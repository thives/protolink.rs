//! Tests for the DMA pump lower layer.
//!
//! The mocks below model a one-shot DMA UART: a transfer is started inside the
//! `read`/`write` future and is lost (receive) or aborted part-way (transmit)
//! when the future is dropped before it completes. That is exactly what ARQ's
//! poll-once-and-drop behaviour would do to a real DMA driver.
#![cfg(feature = "tokio")]

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use embedded_io_async::{Error as _, ErrorKind, ErrorType, Read, Write};
use protolink::link::pump::{DefaultPumpState, PumpError, PumpFailure, PumpState, run};
use protolink::link::{LinkError, StdTimer, reliable_with_timer};

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("timed out")
}

/// Poll a future once and drop it if it is pending, as ARQ does.
fn poll_once<F: Future>(fut: F) -> Option<F::Output> {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

// --- A one-shot DMA UART mock ------------------------------------------------

#[derive(Default)]
struct WireInner {
    buf: VecDeque<u8>,
    waker: Option<Waker>,
}

/// One direction of a serial line.
#[derive(Clone, Default)]
struct Wire(Arc<Mutex<WireInner>>);

impl Wire {
    fn push(&self, data: &[u8]) {
        let waker = {
            let mut w = self.0.lock().unwrap();
            w.buf.extend(data);
            w.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    fn contents(&self) -> Vec<u8> {
        self.0.lock().unwrap().buf.iter().copied().collect()
    }
}

#[derive(Default)]
struct Stats {
    /// Transmit transfers aborted by dropping the future.
    aborted_writes: AtomicUsize,
    /// Received bytes lost because the future was dropped mid-transfer.
    lost_bytes: AtomicUsize,
    /// Calls to `flush`.
    flushes: AtomicUsize,
}

struct DmaRx {
    wire: Wire,
    stats: Arc<Stats>,
}

struct DmaTx {
    wire: Wire,
    stats: Arc<Stats>,
}

impl ErrorType for DmaRx {
    type Error = Infallible;
}

impl ErrorType for DmaTx {
    type Error = Infallible;
}

impl Read for DmaRx {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
        // Waiting for the line to become active is cancel-safe: nothing is
        // taken until there is data.
        let taken: Vec<u8> = poll_fn(|cx| {
            let mut w = self.wire.0.lock().unwrap();
            if w.buf.is_empty() {
                w.waker = Some(cx.waker().clone());
                Poll::Pending
            } else {
                let n = w.buf.len().min(buf.len());
                Poll::Ready(w.buf.drain(..n).collect())
            }
        })
        .await;

        // The DMA transfer is now in flight; dropping the future loses it.
        struct InFlight<'a> {
            bytes: usize,
            stats: &'a Stats,
            done: bool,
        }
        impl Drop for InFlight<'_> {
            fn drop(&mut self) {
                if !self.done {
                    self.stats.lost_bytes.fetch_add(self.bytes, SeqCst);
                }
            }
        }
        let mut guard = InFlight {
            bytes: taken.len(),
            stats: &self.stats,
            done: false,
        };
        tokio::task::yield_now().await;
        guard.done = true;
        buf[..taken.len()].copy_from_slice(&taken);
        Ok(taken.len())
    }
}

impl Write for DmaTx {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
        // Dropping the future aborts the transfer after half of it was shifted
        // out, leaving a truncated frame on the wire.
        struct InFlight<'a> {
            tx: &'a DmaTx,
            buf: &'a [u8],
            done: bool,
        }
        impl Drop for InFlight<'_> {
            fn drop(&mut self) {
                if !self.done {
                    self.tx.wire.push(&self.buf[..self.buf.len() / 2]);
                    self.tx.stats.aborted_writes.fetch_add(1, SeqCst);
                }
            }
        }
        let mut guard = InFlight {
            tx: self,
            buf,
            done: false,
        };
        tokio::task::yield_now().await;
        guard.done = true;
        self.wire.push(buf);
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Infallible> {
        self.stats.flushes.fetch_add(1, SeqCst);
        Ok(())
    }
}

fn dma(rx_wire: &Wire, tx_wire: &Wire, stats: &Arc<Stats>) -> (DmaRx, DmaTx) {
    (
        DmaRx {
            wire: rx_wire.clone(),
            stats: stats.clone(),
        },
        DmaTx {
            wire: tx_wire.clone(),
            stats: stats.clone(),
        },
    )
}

// --- Scripted halves with failures --------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HalError;

impl std::fmt::Display for HalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("hal error")
    }
}

impl std::error::Error for HalError {}

impl embedded_io_async::Error for HalError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::ConnectionReset
    }
}

/// A receive half that replays a script, then reports end of stream.
struct ScriptedRx(VecDeque<Result<Vec<u8>, HalError>>);

impl ErrorType for ScriptedRx {
    type Error = HalError;
}

impl Read for ScriptedRx {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, HalError> {
        tokio::task::yield_now().await;
        match self.0.pop_front() {
            None => Ok(0),
            Some(Err(e)) => Err(e),
            Some(Ok(bytes)) => {
                buf[..bytes.len()].copy_from_slice(&bytes);
                Ok(bytes.len())
            }
        }
    }
}

/// A transmit half that fails on the first write.
struct FailingTx;

impl ErrorType for FailingTx {
    type Error = HalError;
}

impl Write for FailingTx {
    async fn write(&mut self, _: &[u8]) -> Result<usize, HalError> {
        Err(HalError)
    }

    async fn flush(&mut self) -> Result<(), HalError> {
        Ok(())
    }
}

/// A transmit half that never accepts bytes.
struct ZeroTx;

impl ErrorType for ZeroTx {
    type Error = Infallible;
}

impl Write for ZeroTx {
    async fn write(&mut self, _: &[u8]) -> Result<usize, Infallible> {
        Ok(0)
    }

    async fn flush(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}

/// A receive half that never delivers.
struct IdleRx;

impl ErrorType for IdleRx {
    type Error = Infallible;
}

impl Read for IdleRx {
    async fn read(&mut self, _: &mut [u8]) -> Result<usize, Infallible> {
        std::future::pending().await
    }
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 + i / 251) as u8).collect()
}

// --- The mock really is not cancel-safe -----------------------------------------

#[tokio::test]
async fn mock_dma_loses_data_when_dropped() {
    let wire = Wire::default();
    let stats = Arc::new(Stats::default());
    let (mut rx, mut tx) = dma(&wire, &wire, &stats);

    // Dropping a pending transmit aborts it and leaves a truncated frame.
    assert!(poll_once(tx.write(b"abcd")).is_none());
    assert_eq!(stats.aborted_writes.load(SeqCst), 1);
    assert_eq!(wire.contents(), b"ab");

    // Dropping a pending receive loses the bytes it had captured.
    let mut buf = [0u8; 8];
    assert!(poll_once(rx.read(&mut buf)).is_none());
    assert_eq!(stats.lost_bytes.load(SeqCst), 2);
    assert!(wire.contents().is_empty());
}

// --- Handle semantics -----------------------------------------------------------

#[tokio::test]
async fn data_flows_through_the_pump_in_order() {
    // Rings and chunks much smaller than the data exercise wrap-around and
    // backpressure.
    async fn transfer<const RX: usize, const TX: usize, const CHUNK: usize>(len: usize) {
        let ab = Wire::default();
        let ba = Wire::default();
        let stats = Arc::new(Stats::default());
        let mut sa = PumpState::<RX, TX, CHUNK>::new();
        let mut sb = PumpState::<RX, TX, CHUNK>::new();
        let (ra, ta) = dma(&ba, &ab, &stats);
        let (rb, tb) = dma(&ab, &ba, &stats);
        let (mut ha, mut rxa, mut txa) = sa.split(ra, ta);
        let (mut hb, mut rxb, mut txb) = sb.split(rb, tb);
        let data = payload(len);

        let app = async {
            let writer = async {
                ha.write_all(&data).await.unwrap();
                ha.flush().await.unwrap();
            };
            let reader = async {
                let mut got = vec![0u8; len];
                hb.read_exact(&mut got).await.unwrap();
                got
            };
            let ((), got) = tokio::join!(writer, reader);
            got
        };
        let got = tokio::select! {
            r = run(&mut rxa, &mut txa) => panic!("pump a ended: {r:?}"),
            r = run(&mut rxb, &mut txb) => panic!("pump b ended: {r:?}"),
            got = app => got,
        };
        assert_eq!(got, data);
        assert_eq!(stats.aborted_writes.load(SeqCst), 0);
        assert_eq!(stats.lost_bytes.load(SeqCst), 0);
    }

    with_timeout(async {
        transfer::<8, 8, 3>(1000).await;
        transfer::<1, 1, 1>(50).await;
        transfer::<16, 5, 64>(777).await;
        transfer::<520, 520, 64>(5000).await;
    })
    .await;
}

#[tokio::test]
async fn polling_and_dropping_never_cancels_a_transfer() {
    // Drives the handle the way ARQ does: poll once, drop if pending.
    with_timeout(async {
        let ab = Wire::default();
        let ba = Wire::default();
        let stats = Arc::new(Stats::default());
        let mut sa = DefaultPumpState::new();
        let mut sb = DefaultPumpState::new();
        let (ra, ta) = dma(&ba, &ab, &stats);
        let (rb, tb) = dma(&ab, &ba, &stats);
        let (mut ha, mut rxa, mut txa) = sa.split(ra, ta);
        let (mut hb, mut rxb, mut txb) = sb.split(rb, tb);
        let data = payload(4000);

        let app = async {
            let writer = async {
                let mut off = 0;
                while off < data.len() {
                    match poll_once(ha.write(&data[off..])) {
                        Some(n) => off += n.unwrap(),
                        None => tokio::task::yield_now().await,
                    }
                }
                while poll_once(ha.flush()).is_none() {
                    tokio::task::yield_now().await;
                }
            };
            let reader = async {
                let mut got = Vec::new();
                let mut buf = [0u8; 50];
                while got.len() < data.len() {
                    match poll_once(hb.read(&mut buf)) {
                        Some(n) => got.extend_from_slice(&buf[..n.unwrap()]),
                        None => tokio::task::yield_now().await,
                    }
                }
                got
            };
            let ((), got) = tokio::join!(writer, reader);
            got
        };
        let got = tokio::select! {
            r = run(&mut rxa, &mut txa) => panic!("pump a ended: {r:?}"),
            r = run(&mut rxb, &mut txb) => panic!("pump b ended: {r:?}"),
            got = app => got,
        };
        assert_eq!(got, data);
        assert_eq!(stats.aborted_writes.load(SeqCst), 0);
        assert_eq!(stats.lost_bytes.load(SeqCst), 0);
    })
    .await;
}

#[tokio::test]
async fn pending_read_dropped_before_data_loses_nothing() {
    let wire = Wire::default();
    let stats = Arc::new(Stats::default());
    let (rx, tx) = dma(&wire, &Wire::default(), &stats);
    let mut state = DefaultPumpState::new();
    let (mut handle, mut rx_pump, _tx_pump) = state.split(rx, tx);

    let mut buf = [0u8; 16];
    assert!(poll_once(handle.read(&mut buf)).is_none());

    wire.push(b"hello");
    with_timeout(async {
        let read = async {
            let mut got = Vec::new();
            while got.len() < 5 {
                let n = handle.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            got
        };
        tokio::select! {
            r = rx_pump.run() => panic!("rx pump ended: {r:?}"),
            got = read => assert_eq!(got, b"hello"),
        }
    })
    .await;
    assert_eq!(stats.lost_bytes.load(SeqCst), 0);
}

#[tokio::test]
async fn pending_write_on_full_ring_writes_nothing() {
    let wire = Wire::default();
    let stats = Arc::new(Stats::default());
    let (rx, tx) = dma(&Wire::default(), &wire, &stats);
    let mut state = PumpState::<8, 8, 4>::new();
    let (mut handle, _rx_pump, mut tx_pump) = state.split(rx, tx);

    // The ring takes what fits and reports it.
    let data = payload(10);
    assert_eq!(poll_once(handle.write(&data)), Some(Ok(8)));
    // The ring is full: the write stays pending and is dropped.
    assert!(poll_once(handle.write(&data[8..])).is_none());
    assert!(poll_once(handle.write(&data[8..])).is_none());

    // Starting the pump now delivers exactly the accepted bytes.
    let _ = tokio::time::timeout(Duration::from_millis(100), tx_pump.run()).await;
    assert_eq!(wire.contents(), &data[..8]);
    assert_eq!(stats.aborted_writes.load(SeqCst), 0);
}

#[tokio::test]
async fn empty_buffers_complete_immediately() {
    let stats = Arc::new(Stats::default());
    let (rx, tx) = dma(&Wire::default(), &Wire::default(), &stats);
    let mut state = DefaultPumpState::new();
    let (mut handle, _rx_pump, _tx_pump) = state.split(rx, tx);
    assert_eq!(poll_once(handle.read(&mut [])), Some(Ok(0)));
    assert_eq!(poll_once(handle.write(&[])), Some(Ok(0)));
}

#[tokio::test]
async fn flush_waits_for_the_hal() {
    let wire = Wire::default();
    let stats = Arc::new(Stats::default());
    let (rx, tx) = dma(&Wire::default(), &wire, &stats);
    let mut state = DefaultPumpState::new();
    let (mut handle, _rx_pump, mut tx_pump) = state.split(rx, tx);

    // Nothing written: flush is immediate and does not touch the HAL.
    assert_eq!(poll_once(handle.flush()), Some(Ok(())));
    assert_eq!(stats.flushes.load(SeqCst), 0);

    assert_eq!(poll_once(handle.write(b"abc")), Some(Ok(3)));
    // Pending until the pump has written the bytes and flushed the HAL.
    assert!(poll_once(handle.flush()).is_none());
    assert!(wire.contents().is_empty());

    with_timeout(async {
        tokio::select! {
            r = tx_pump.run() => panic!("tx pump ended: {r:?}"),
            r = handle.flush() => r.unwrap(),
        }
    })
    .await;
    assert_eq!(wire.contents(), b"abc");
    assert_eq!(stats.flushes.load(SeqCst), 1);

    // Everything is flushed: another flush is immediate again.
    assert_eq!(poll_once(handle.flush()), Some(Ok(())));
    assert_eq!(stats.flushes.load(SeqCst), 1);
}

// --- Errors ---------------------------------------------------------------------

#[tokio::test]
async fn transmit_failure_fails_the_handle() {
    let mut state = DefaultPumpState::new();
    let (mut handle, _rx_pump, mut tx_pump) = state.split(IdleRx, FailingTx);

    assert_eq!(poll_once(handle.write(b"x")), Some(Ok(1)));
    let err = with_timeout(tx_pump.run()).await.unwrap_err();
    assert_eq!(err, LinkError::Io(HalError));

    let err = poll_once(handle.write(b"y")).unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConnectionReset);
    let err = poll_once(handle.flush()).unwrap().unwrap_err();
    assert_eq!(err, PumpError::new(ErrorKind::ConnectionReset));
}

#[tokio::test]
async fn transmit_accepting_no_bytes_is_write_zero() {
    let mut state = DefaultPumpState::new();
    let (mut handle, _rx_pump, mut tx_pump) = state.split(IdleRx, ZeroTx);
    assert_eq!(poll_once(handle.write(b"x")), Some(Ok(1)));
    let err = with_timeout(tx_pump.run()).await.unwrap_err();
    assert_eq!(err, LinkError::WriteZero);
    let err = poll_once(handle.write(b"y")).unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::WriteZero);
}

#[tokio::test]
async fn receive_failure_is_reported_after_buffered_bytes() {
    let script = VecDeque::from([Ok(b"abc".to_vec()), Err(HalError)]);
    let mut state = DefaultPumpState::new();
    let (mut handle, mut rx_pump, _tx_pump) = state.split(ScriptedRx(script), ZeroTx);

    let err = with_timeout(rx_pump.run()).await.unwrap_err();
    assert_eq!(err, HalError);

    let mut buf = [0u8; 8];
    assert_eq!(poll_once(handle.read(&mut buf)), Some(Ok(3)));
    assert_eq!(&buf[..3], b"abc");
    let err = poll_once(handle.read(&mut buf)).unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConnectionReset);
}

#[tokio::test]
async fn skipped_receive_errors_are_counted_and_end_of_stream_is_eof() {
    let script = VecDeque::from([
        Ok(b"ab".to_vec()),
        Err(HalError),
        Err(HalError),
        Ok(b"cd".to_vec()),
    ]);
    let mut state = DefaultPumpState::new();
    let (mut handle, rx_pump, _tx_pump) = state.split(ScriptedRx(script), ZeroTx);
    let mut rx_pump = rx_pump.skip_errors(true);

    // The script ends with `Ok(0)`: end of stream, not an error.
    with_timeout(rx_pump.run()).await.unwrap();
    assert_eq!(rx_pump.skipped_errors(), 2);

    let mut buf = [0u8; 8];
    assert_eq!(poll_once(handle.read(&mut buf)), Some(Ok(4)));
    assert_eq!(&buf[..4], b"abcd");
    assert_eq!(poll_once(handle.read(&mut buf)), Some(Ok(0)));
}

#[tokio::test]
async fn run_reports_which_half_failed() {
    let mut state = DefaultPumpState::new();
    let (mut handle, mut rx_pump, mut tx_pump) = state.split(IdleRx, FailingTx);
    assert_eq!(poll_once(handle.write(b"x")), Some(Ok(1)));
    match with_timeout(run(&mut rx_pump, &mut tx_pump)).await {
        Err(PumpFailure::Tx(LinkError::Io(HalError))) => {}
        other => panic!("unexpected result: {other:?}"),
    }
    drop((rx_pump, tx_pump));

    let script = VecDeque::from([Err(HalError)]);
    let mut state = DefaultPumpState::new();
    let (_handle, mut rx_pump, mut tx_pump) = state.split(ScriptedRx(script), ZeroTx);
    match with_timeout(run(&mut rx_pump, &mut tx_pump)).await {
        Err(PumpFailure::Rx(HalError)) => {}
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn dropping_a_pump_unblocks_the_handle() {
    let mut state = DefaultPumpState::new();
    let (mut handle, rx_pump, tx_pump) = state.split(IdleRx, ZeroTx);

    let mut buf = [0u8; 4];
    assert!(poll_once(handle.read(&mut buf)).is_none());
    drop(rx_pump);
    let err = poll_once(handle.read(&mut buf)).unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::BrokenPipe);

    assert_eq!(poll_once(handle.write(b"x")), Some(Ok(1)));
    drop(tx_pump);
    let err = poll_once(handle.write(b"x")).unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::BrokenPipe);
    let err = poll_once(handle.flush()).unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::BrokenPipe);
}

#[tokio::test]
async fn state_can_be_split_again() {
    let wire = Wire::default();
    let stats = Arc::new(Stats::default());
    let mut state = DefaultPumpState::new();
    {
        let (rx, tx) = dma(&Wire::default(), &wire, &stats);
        let (mut handle, rx_pump, tx_pump) = state.split(rx, tx);
        assert_eq!(poll_once(handle.write(b"stale")), Some(Ok(5)));
        drop((rx_pump, tx_pump));
    }
    // Splitting again starts from a clean state: no stale bytes, no stale errors.
    let (rx, tx) = dma(&Wire::default(), &wire, &stats);
    let (mut handle, mut rx_pump, mut tx_pump) = state.split(rx, tx);
    assert_eq!(poll_once(handle.write(b"ok")), Some(Ok(2)));
    with_timeout(async {
        tokio::select! {
            r = run(&mut rx_pump, &mut tx_pump) => panic!("pump ended: {r:?}"),
            r = handle.flush() => r.unwrap(),
        }
    })
    .await;
    assert_eq!(wire.contents(), b"ok");
}

// --- Multi-threaded executors ----------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handle_and_pumps_run_on_separate_threads() {
    // `tokio::spawn` needs `Send + 'static`: this test does not compile unless
    // the handle, both pumps and their futures are `Send`.
    with_timeout(async {
        let ab = Wire::default();
        let ba = Wire::default();
        let stats = Arc::new(Stats::default());
        let sa: &'static mut DefaultPumpState = Box::leak(Box::default());
        let sb: &'static mut DefaultPumpState = Box::leak(Box::default());
        let (ra, ta) = dma(&ba, &ab, &stats);
        let (rb, tb) = dma(&ab, &ba, &stats);
        let (mut ha, mut rxa, mut txa) = sa.split(ra, ta);
        let (mut hb, mut rxb, mut txb) = sb.split(rb, tb);
        let data = payload(20_000);

        let pumps = [
            tokio::spawn(async move { panic!("rx a ended: {:?}", rxa.run().await) }),
            tokio::spawn(async move { panic!("tx a ended: {:?}", txa.run().await) }),
            tokio::spawn(async move { panic!("rx b ended: {:?}", rxb.run().await) }),
            tokio::spawn(async move { panic!("tx b ended: {:?}", txb.run().await) }),
        ];
        let writer = tokio::spawn({
            let data = data.clone();
            async move {
                ha.write_all(&data).await.unwrap();
                ha.flush().await.unwrap();
            }
        });
        let reader = tokio::spawn(async move {
            let mut got = vec![0u8; data.len()];
            hb.read_exact(&mut got).await.unwrap();
            got
        });

        writer.await.unwrap();
        assert_eq!(reader.await.unwrap(), payload(20_000));
        for pump in &pumps {
            assert!(!pump.is_finished(), "a pump ended");
        }
        for pump in pumps {
            pump.abort();
        }
        assert_eq!(stats.aborted_writes.load(SeqCst), 0);
        assert_eq!(stats.lost_bytes.load(SeqCst), 0);
    })
    .await;
}

// --- The full reliable stack over a DMA UART --------------------------------------

#[tokio::test]
async fn reliable_link_over_dma_uart() {
    with_timeout(async {
        let ab = Wire::default();
        let ba = Wire::default();
        let stats = Arc::new(Stats::default());
        let mut sa = DefaultPumpState::new();
        let mut sb = DefaultPumpState::new();
        let (ra, ta) = dma(&ba, &ab, &stats);
        let (rb, tb) = dma(&ab, &ba, &stats);
        let (ha, mut rxa, mut txa) = sa.split(ra, ta);
        let (hb, mut rxb, mut txb) = sb.split(rb, tb);
        let mut a = reliable_with_timer(ha, StdTimer::new());
        let mut b = reliable_with_timer(hb, StdTimer::new());

        let request = payload(3000);
        let reply = payload(1500).into_iter().rev().collect::<Vec<_>>();

        let app = async {
            // a -> b
            let writer = async {
                a.write_all(&request).await.unwrap();
                a.flush().await.unwrap();
            };
            let reader = async {
                let mut got = vec![0u8; request.len()];
                b.read_exact(&mut got).await.unwrap();
                got
            };
            let ((), got) = tokio::join!(writer, reader);
            assert_eq!(got, request);

            // b -> a
            let writer = async {
                b.write_all(&reply).await.unwrap();
                b.flush().await.unwrap();
            };
            let reader = async {
                let mut got = vec![0u8; reply.len()];
                a.read_exact(&mut got).await.unwrap();
                got
            };
            let ((), got) = tokio::join!(writer, reader);
            assert_eq!(got, reply);
        };
        tokio::select! {
            r = run(&mut rxa, &mut txa) => panic!("pump a ended: {r:?}"),
            r = run(&mut rxb, &mut txb) => panic!("pump b ended: {r:?}"),
            () = app => {}
        }
        // The DMA hazards never materialised, so ARQ did not have to recover
        // from truncated frames or lost bytes.
        assert_eq!(stats.aborted_writes.load(SeqCst), 0);
        assert_eq!(stats.lost_bytes.load(SeqCst), 0);
    })
    .await;
}

/// Both HAL halves as one `Read + Write` stream, as ARQ would be given them.
struct Duplex(DmaRx, DmaTx);

impl ErrorType for Duplex {
    type Error = Infallible;
}

impl Read for Duplex {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
        self.0.read(buf).await
    }
}

impl Write for Duplex {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
        self.1.write(buf).await
    }

    async fn flush(&mut self) -> Result<(), Infallible> {
        self.1.flush().await
    }
}

/// The reason the pump exists: ARQ polls once and drops, so a one-shot DMA
/// driver used directly has its transfers aborted over and over.
#[tokio::test]
async fn without_the_pump_arq_aborts_dma_transfers() {
    let ab = Wire::default();
    let ba = Wire::default();
    let stats = Arc::new(Stats::default());
    let (ra, ta) = dma(&ba, &ab, &stats);
    let (rb, tb) = dma(&ab, &ba, &stats);
    let mut a = reliable_with_timer(Duplex(ra, ta), StdTimer::new());
    let mut b = reliable_with_timer(Duplex(rb, tb), StdTimer::new());
    let request = payload(3000);

    let transfer = async {
        let writer = async {
            a.write_all(&request).await.unwrap();
            a.flush().await.unwrap();
        };
        let reader = async {
            let mut got = vec![0u8; request.len()];
            b.read_exact(&mut got).await.unwrap();
            got
        };
        tokio::join!(writer, reader).1
    };
    let _ = tokio::time::timeout(Duration::from_millis(300), transfer).await;
    assert!(stats.aborted_writes.load(SeqCst) > 0);
    assert!(stats.lost_bytes.load(SeqCst) > 0);
}
