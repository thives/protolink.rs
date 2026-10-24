//! Tests for the buffered UART facade over the link pump.
//!
//! The HAL halves below model a one-shot DMA UART. A transmit transfer runs
//! inside its `write` future, and dropping that future before it completes
//! aborts the transfer part-way. Receive bytes wait in a FIFO with a finite
//! capacity, and a byte that arrives when the FIFO is full is lost and reported
//! as an overrun once the bytes before it have been read.
#![cfg(feature = "uart")]

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use embedded_io_async::{Error as _, ErrorKind, ErrorType, Read, Write};
use protolink::link::reliable;
use protolink_adaptors::pump::{PumpFailure, WriteError};
use protolink_adaptors::{Binding, UartState, uart};
use static_cell::StaticCell;

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("timed out")
}

/// Poll a future once and drop it if it is pending.
fn poll_once<F: Future>(fut: F) -> Option<F::Output> {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

async fn yield_now() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 + i / 251) as u8).collect()
}

// --- The HAL model ----------------------------------------------------------------

/// A HAL error: an overrun, or a failure of the given kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HalError {
    Overrun,
    Failed(ErrorKind),
}

impl std::fmt::Display for HalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for HalError {}

impl embedded_io_async::Error for HalError {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Overrun => ErrorKind::Other,
            Self::Failed(kind) => *kind,
        }
    }
}

#[derive(Default)]
struct LineState {
    /// Bytes received and not yet read by the HAL.
    fifo: VecDeque<u8>,
    /// The FIFO capacity. Bytes beyond it are lost.
    capacity: usize,
    /// Bytes were lost; reported once the FIFO is empty.
    overrun: bool,
    /// Reported by the receive side once the FIFO is empty.
    failure: Option<ErrorKind>,
    waker: Option<Waker>,
}

/// One direction of a serial line.
#[derive(Clone)]
struct Line(Arc<Mutex<LineState>>);

impl Line {
    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(Mutex::new(LineState {
            capacity,
            ..LineState::default()
        })))
    }

    fn unbounded() -> Self {
        Self::with_capacity(usize::MAX)
    }

    /// Queue bytes at the receiver. Bytes that do not fit are lost.
    fn push(&self, data: &[u8]) {
        let waker = {
            let mut l = self.0.lock().unwrap();
            let room = l.capacity.saturating_sub(l.fifo.len());
            l.fifo.extend(&data[..data.len().min(room)]);
            if data.len() > room {
                l.overrun = true;
            }
            l.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// Fail the receive side once the bytes already queued have been read.
    fn fail(&self, kind: ErrorKind) {
        let waker = {
            let mut l = self.0.lock().unwrap();
            l.failure = Some(kind);
            l.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

#[derive(Default)]
struct Stats {
    /// Receive operations started by the HAL.
    rx_ops: AtomicUsize,
    /// Transmit operations started by the HAL.
    tx_ops: AtomicUsize,
    /// Calls to the HAL `flush`.
    flushes: AtomicUsize,
    /// Transmit transfers aborted by dropping their future.
    aborted_writes: AtomicUsize,
    /// Bytes whose transmit transfer completed, in order.
    sent: Mutex<Vec<u8>>,
}

struct HalRx {
    line: Line,
    /// The most bytes one read returns, to force partial reads.
    max_read: usize,
    stats: Arc<Stats>,
}

impl ErrorType for HalRx {
    type Error = HalError;
}

impl Read for HalRx {
    /// Returns the bytes available, and waits only while there are none.
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, HalError> {
        self.stats.rx_ops.fetch_add(1, SeqCst);
        let line = &self.line;
        let max = self.max_read;
        poll_fn(|cx| {
            let mut l = line.0.lock().unwrap();
            // Bytes that arrived before a fault are delivered first.
            if !l.fifo.is_empty() {
                let n = l.fifo.len().min(buf.len()).min(max);
                for slot in buf.iter_mut().take(n) {
                    *slot = l.fifo.pop_front().expect("length checked");
                }
                return Poll::Ready(Ok(n));
            }
            if l.overrun {
                l.overrun = false;
                return Poll::Ready(Err(HalError::Overrun));
            }
            if let Some(kind) = l.failure {
                return Poll::Ready(Err(HalError::Failed(kind)));
            }
            l.waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

struct HalTx {
    line: Line,
    /// The most bytes one write accepts, to force partial writes.
    max_write: usize,
    /// Pending polls before each write completes.
    write_polls: usize,
    /// Pending polls before each flush completes.
    flush_polls: usize,
    /// Every write fails with this kind.
    write_error: Option<ErrorKind>,
    /// Every flush fails with this kind.
    flush_error: Option<ErrorKind>,
    stats: Arc<Stats>,
}

impl HalTx {
    fn new(line: Line, stats: Arc<Stats>) -> Self {
        Self {
            line,
            max_write: usize::MAX,
            write_polls: 0,
            flush_polls: 0,
            write_error: None,
            flush_error: None,
            stats,
        }
    }
}

impl ErrorType for HalTx {
    type Error = HalError;
}

impl Write for HalTx {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, HalError> {
        self.stats.tx_ops.fetch_add(1, SeqCst);
        if let Some(kind) = self.write_error {
            return Err(HalError::Failed(kind));
        }
        let n = buf.len().min(self.max_write);
        let polls = self.write_polls;

        // The transfer is in flight from here. Dropping the future before it
        // completes aborts it: half of the bytes leave the line, and the rest
        // are lost, as a DMA transfer aborted part-way would.
        struct InFlight<'a> {
            line: &'a Line,
            stats: &'a Stats,
            data: &'a [u8],
            done: bool,
        }
        impl Drop for InFlight<'_> {
            fn drop(&mut self) {
                if !self.done {
                    self.line.push(&self.data[..self.data.len() / 2]);
                    self.stats.aborted_writes.fetch_add(1, SeqCst);
                }
            }
        }
        let mut guard = InFlight {
            line: &self.line,
            stats: &self.stats,
            data: &buf[..n],
            done: false,
        };
        for _ in 0..polls {
            yield_now().await;
        }
        guard.done = true;
        self.line.push(&buf[..n]);
        self.stats.sent.lock().unwrap().extend_from_slice(&buf[..n]);
        Ok(n)
    }

    async fn flush(&mut self) -> Result<(), HalError> {
        self.stats.flushes.fetch_add(1, SeqCst);
        for _ in 0..self.flush_polls {
            yield_now().await;
        }
        match self.flush_error {
            Some(kind) => Err(HalError::Failed(kind)),
            None => Ok(()),
        }
    }
}

/// The receive and transmit halves of one end of a line: `rx_line` carries the
/// peer's bytes to it and `tx_line` carries its bytes to the peer.
fn hal(rx_line: &Line, tx_line: &Line, stats: &Arc<Stats>) -> (HalRx, HalTx) {
    (
        HalRx {
            line: rx_line.clone(),
            max_read: usize::MAX,
            stats: stats.clone(),
        },
        HalTx::new(tx_line.clone(), stats.clone()),
    )
}

// --- Construction and the mock ----------------------------------------------------

#[tokio::test]
async fn construction_performs_no_hardware_operations() {
    let line = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx, tx) = hal(&line, &line, &stats);
    let mut state = UartState::new();

    // Built, then dropped without polling the runner: nothing reaches the HAL.
    let binding = uart(rx, tx).with_storage(&mut state).build();
    drop(binding);

    assert_eq!(stats.rx_ops.load(SeqCst), 0);
    assert_eq!(stats.tx_ops.load(SeqCst), 0);
    assert_eq!(stats.flushes.load(SeqCst), 0);
}

#[tokio::test]
async fn the_mock_transmit_is_not_cancel_safe() {
    // Sanity check of the model: a dropped pending write aborts the transfer.
    let line = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let mut tx = HalTx::new(line.clone(), stats.clone());
    tx.write_polls = 1;

    assert!(poll_once(tx.write(b"abcd")).is_none());
    assert_eq!(stats.aborted_writes.load(SeqCst), 1);
    assert_eq!(line.0.lock().unwrap().fifo.len(), 2);
    assert!(stats.sent.lock().unwrap().is_empty());
}

// --- Data path --------------------------------------------------------------------

#[tokio::test]
async fn bytes_flow_through_partial_reads_and_writes_and_ring_wraparound() {
    // Rings and transfers much smaller than the data force wrap-around,
    // backpressure and partial acceptance on both sides.
    async fn transfer<const N: usize, const CHUNK: usize>(len: usize) {
        let ab = Line::unbounded();
        let ba = Line::unbounded();
        let stats = Arc::new(Stats::default());
        let (rx_a, mut tx_a) = hal(&ba, &ab, &stats);
        let (mut rx_b, tx_b) = hal(&ab, &ba, &stats);
        tx_a.max_write = 3;
        rx_b.max_read = 2;
        let mut state_a = UartState::<N, N, CHUNK>::with_buffers();
        let mut state_b = UartState::<N, N, CHUNK>::with_buffers();
        let Binding {
            port: mut port_a,
            runner: runner_a,
        } = uart(rx_a, tx_a).with_storage(&mut state_a).build();
        let Binding {
            port: mut port_b,
            runner: runner_b,
        } = uart(rx_b, tx_b).with_storage(&mut state_b).build();
        let data = payload(len);

        let app = async {
            let writer = async {
                port_a.write_all(&data).await.unwrap();
                port_a.flush().await.unwrap();
            };
            let reader = async {
                let mut got = vec![0u8; len];
                port_b.read_exact(&mut got).await.unwrap();
                got
            };
            let ((), got) = tokio::join!(writer, reader);
            got
        };
        let got = tokio::select! {
            r = runner_a.run() => panic!("runner a ended: {r:?}"),
            r = runner_b.run() => panic!("runner b ended: {r:?}"),
            got = app => got,
        };
        assert_eq!(got, data);
        assert_eq!(*stats.sent.lock().unwrap(), data);
        assert_eq!(stats.aborted_writes.load(SeqCst), 0);
    }

    with_timeout(async {
        transfer::<8, 3>(1000).await;
        transfer::<1, 1>(50).await;
        transfer::<16, 64>(777).await;
        transfer::<520, 64>(5000).await;
    })
    .await;
}

#[tokio::test]
async fn short_messages_complete_without_another_message() {
    let ab = Line::unbounded();
    let ba = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx_a, tx_a) = hal(&ba, &ab, &stats);
    let (rx_b, tx_b) = hal(&ab, &ba, &stats);
    let mut state_a = UartState::new();
    let mut state_b = UartState::new();
    let Binding {
        port: mut port_a,
        runner: runner_a,
    } = uart(rx_a, tx_a).with_storage(&mut state_a).build();
    let Binding {
        port: mut port_b,
        runner: runner_b,
    } = uart(rx_b, tx_b).with_storage(&mut state_b).build();

    // Five bytes, then nothing: the flush must complete and the read must
    // return without waiting for the buffer to fill.
    with_timeout(async {
        let app = async {
            port_a.write_all(b"hello").await.unwrap();
            port_a.flush().await.unwrap();
            let mut buf = [0u8; 16];
            let n = port_b.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"hello");
        };
        tokio::select! {
            r = runner_a.run() => panic!("runner a ended: {r:?}"),
            r = runner_b.run() => panic!("runner b ended: {r:?}"),
            () = app => {}
        }
    })
    .await;
}

#[tokio::test]
async fn dropping_pending_port_operations_does_not_cancel_hardware_transfers() {
    let ab = Line::unbounded();
    let ba = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx_a, mut tx_a) = hal(&ba, &ab, &stats);
    let (rx_b, tx_b) = hal(&ab, &ba, &stats);
    // Every transfer takes several polls, so the runner is mid-transfer while
    // the port operations below are created and dropped.
    tx_a.write_polls = 3;
    tx_a.max_write = 4;
    let mut state_a = UartState::new();
    let mut state_b = UartState::new();
    let Binding {
        port: mut port_a,
        runner: runner_a,
    } = uart(rx_a, tx_a).with_storage(&mut state_a).build();
    let Binding {
        port: mut port_b,
        runner: runner_b,
    } = uart(rx_b, tx_b).with_storage(&mut state_b).build();
    let data = payload(200);

    let got = with_timeout(async {
        let app = async {
            let mut got = Vec::new();
            let mut buf = [0u8; 8];
            let mut off = 0;
            while off < data.len() {
                // Nothing comes back on this side, so the read stays pending.
                // It is created, polled once and dropped, as ARQ does.
                assert!(poll_once(port_a.read(&mut buf)).is_none());
                // A write dropped while pending must accept nothing.
                if let Some(n) = poll_once(port_a.write(&data[off..])) {
                    off += n.unwrap();
                }
                if let Some(n) = poll_once(port_b.read(&mut buf)) {
                    got.extend_from_slice(&buf[..n.unwrap()]);
                }
                yield_now().await;
            }
            port_a.flush().await.unwrap();
            while got.len() < data.len() {
                let n = port_b.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            got
        };
        tokio::select! {
            r = runner_a.run() => panic!("runner a ended: {r:?}"),
            r = runner_b.run() => panic!("runner b ended: {r:?}"),
            got = app => got,
        }
    })
    .await;

    assert_eq!(got, data);
    assert_eq!(stats.aborted_writes.load(SeqCst), 0);
    assert_eq!(*stats.sent.lock().unwrap(), data);
}

// --- Flush ------------------------------------------------------------------------

#[tokio::test]
async fn pending_hal_flush_holds_the_port_flush() {
    let line = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx, mut tx) = hal(&line, &line, &stats);
    tx.flush_polls = 5;
    let mut state = UartState::new();
    let Binding { mut port, runner } = uart(rx, tx).with_storage(&mut state).build();

    with_timeout(async {
        let app = async {
            port.write_all(b"abc").await.unwrap();
            let mut flush = pin!(port.flush());
            // The flush must not complete while the HAL is still flushing.
            for _ in 0..3 {
                assert!(poll_once(flush.as_mut()).is_none());
                yield_now().await;
            }
            flush.await.unwrap();
            // Only now, after the HAL flush returned, is the flush complete.
            assert_eq!(stats.flushes.load(SeqCst), 1);
            assert_eq!(*stats.sent.lock().unwrap(), b"abc");
        };
        tokio::select! {
            r = runner.run() => panic!("runner ended: {r:?}"),
            () = app => {}
        }
    })
    .await;
}

#[tokio::test]
async fn failing_hal_flush_fails_the_port_flush_and_the_runner() {
    let line = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx, mut tx) = hal(&line, &line, &stats);
    tx.flush_error = Some(ErrorKind::ConnectionReset);
    let mut state = UartState::new();
    let Binding { mut port, runner } = uart(rx, tx).with_storage(&mut state).build();

    let (flush, failure) = with_timeout(async {
        let app = async {
            port.write_all(b"abc").await.unwrap();
            port.flush().await
        };
        tokio::join!(app, runner.run())
    })
    .await;

    let err = flush.expect_err("the flush reports the HAL failure");
    assert_eq!(err.kind(), ErrorKind::ConnectionReset);
    assert_eq!(
        failure.expect_err("the runner ends with the HAL failure"),
        PumpFailure::Tx(WriteError::Io(HalError::Failed(ErrorKind::ConnectionReset)))
    );
}

// --- Errors and teardown ----------------------------------------------------------

#[tokio::test]
async fn transmit_failure_wakes_a_pending_write_and_fails_receive() {
    let line = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx, mut tx) = hal(&line, &line, &stats);
    tx.write_error = Some(ErrorKind::ConnectionAborted);
    // An eight-byte transmit ring: the second write waits for space, which
    // only the runner can make, and the runner fails instead.
    let mut state = UartState::<8, 8, 3>::with_buffers();
    let Binding { mut port, runner } = uart(rx, tx).with_storage(&mut state).build();

    let (write, failure) = with_timeout(async {
        let app = port.write_all(&[1u8; 64]);
        tokio::join!(app, runner.run())
    })
    .await;

    assert_eq!(
        write
            .expect_err("the pending write is woken by the failure")
            .kind(),
        ErrorKind::ConnectionAborted
    );
    assert_eq!(
        failure.expect_err("the runner fails on its first transfer"),
        PumpFailure::Tx(WriteError::Io(HalError::Failed(
            ErrorKind::ConnectionAborted
        )))
    );
    // The transmit half is gone, so the receive side ends too.
    let read = port.read(&mut [0u8; 4]).await;
    assert_eq!(
        read.expect_err("receive has ended").kind(),
        ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn hal_read_failure_is_reported_after_buffered_bytes() {
    let line = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (mut rx, tx) = hal(&line, &line, &stats);
    rx.max_read = 7;
    let mut state = UartState::new();
    let Binding { mut port, runner } = uart(rx, tx).with_storage(&mut state).build();

    // The bytes arrive, then the receive side fails.
    let data = payload(100);
    line.push(&data);
    line.fail(ErrorKind::ConnectionReset);

    let ((got, read_error), failure) = with_timeout(async {
        let app = async {
            let mut got = Vec::new();
            let mut buf = [0u8; 32];
            let err = loop {
                match port.read(&mut buf).await {
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(e) => break e,
                }
            };
            (got, err)
        };
        tokio::join!(app, runner.run())
    })
    .await;

    assert_eq!(got, data, "every buffered byte is read before the error");
    assert_eq!(read_error.kind(), ErrorKind::ConnectionReset);
    assert_eq!(
        failure.expect_err("the runner ends with the receive failure"),
        PumpFailure::Rx(HalError::Failed(ErrorKind::ConnectionReset))
    );
    // With the runner gone, a write fails rather than waiting.
    let write = port.write(b"x").await;
    assert_eq!(
        write.expect_err("the runner has ended").kind(),
        ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn finite_receive_capacity_overrun_is_terminal() {
    // The peer's line holds 16 bytes at the receiver, and the receiver's port
    // holds 8 + 8 more. The application stalls, so the line overruns.
    let ab = Line::with_capacity(16);
    let ba = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx_a, mut tx_a) = hal(&ba, &ab, &stats);
    let (mut rx_b, tx_b) = hal(&ab, &ba, &stats);
    tx_a.max_write = 8;
    rx_b.max_read = 8;
    let mut state_a = UartState::new();
    let mut state_b = UartState::<8, 8, 8>::with_buffers();
    let Binding {
        port: mut port_a,
        runner: runner_a,
    } = uart(rx_a, tx_a).with_storage(&mut state_a).build();
    let Binding {
        port: mut port_b,
        runner: runner_b,
    } = uart(rx_b, tx_b).with_storage(&mut state_b).build();
    let data = payload(200);

    let ((got, read_error), failure) = with_timeout(async {
        let app = async {
            // The sender's writes never block on the line, so they all finish.
            port_a.write_all(&data).await.unwrap();
            port_a.flush().await.unwrap();
            // The receiver stalls first, then reads until the error.
            for _ in 0..200 {
                yield_now().await;
            }
            let mut got = Vec::new();
            let mut buf = [0u8; 8];
            let err = loop {
                match port_b.read(&mut buf).await {
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(e) => break e,
                }
            };
            (got, err)
        };
        tokio::join!(
            async {
                tokio::select! {
                    r = runner_a.run() => panic!("runner a ended: {r:?}"),
                    out = app => out,
                }
            },
            runner_b.run(),
        )
    })
    .await;

    assert!(
        got.len() < data.len(),
        "bytes were lost and are not delivered"
    );
    assert_eq!(
        &got[..],
        &data[..got.len()],
        "what is delivered is in order"
    );
    assert_eq!(
        read_error.kind(),
        ErrorKind::Other,
        "the overrun is an error"
    );
    assert_eq!(
        failure.expect_err("the receiver overran"),
        PumpFailure::Rx(HalError::Overrun)
    );
}

#[tokio::test]
async fn runner_cancellation_wakes_pending_port_operations() {
    let line = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx, tx) = hal(&line, &line, &stats);
    let state: &'static mut UartState = Box::leak(Box::new(UartState::new()));
    let Binding { port, runner } = uart(rx, tx).with_storage(state).build();

    with_timeout(async {
        let runner_task = tokio::spawn(runner.run());
        let reader = tokio::spawn(async move {
            let mut port = port;
            // Pending until the runner is canceled.
            let read = port
                .read(&mut [0u8; 4])
                .await
                .expect_err("woken by teardown");
            let write = port.write(b"x").await.expect_err("woken by teardown");
            (read.kind(), write.kind())
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        runner_task.abort();
        let (read_kind, write_kind) = reader.await.unwrap();
        assert_eq!(read_kind, ErrorKind::BrokenPipe);
        assert_eq!(write_kind, ErrorKind::BrokenPipe);
    })
    .await;
}

// --- Storage, tasks and reuse -----------------------------------------------------

static TASK_STATE_A: StaticCell<UartState> = StaticCell::new();
static TASK_STATE_B: StaticCell<UartState> = StaticCell::new();

#[tokio::test]
async fn static_storage_and_independent_tasks_exchange_data() {
    let ab = Line::unbounded();
    let ba = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx_a, mut tx_a) = hal(&ba, &ab, &stats);
    let (rx_b, tx_b) = hal(&ab, &ba, &stats);
    tx_a.max_write = 5;
    let state_a = TASK_STATE_A.init(UartState::new());
    let state_b = TASK_STATE_B.init(UartState::new());
    let Binding {
        port: port_a,
        runner: runner_a,
    } = uart(rx_a, tx_a).with_storage(state_a).build();
    let Binding {
        port: port_b,
        runner: runner_b,
    } = uart(rx_b, tx_b).with_storage(state_b).build();
    let data = payload(3000);

    let run_a = tokio::spawn(runner_a.run());
    let run_b = tokio::spawn(runner_b.run());
    let sender = {
        let data = data.clone();
        tokio::spawn(async move {
            let mut port = port_a;
            port.write_all(&data).await.unwrap();
            port.flush().await.unwrap();
        })
    };
    let receiver = tokio::spawn(async move {
        let mut port = port_b;
        let mut got = vec![0u8; 3000];
        port.read_exact(&mut got).await.unwrap();
        got
    });

    let got = with_timeout(async { receiver.await.unwrap() }).await;
    with_timeout(async { sender.await.unwrap() }).await;
    assert_eq!(got, data);
    // The runners never ended: they are still driving the line.
    assert!(!run_a.is_finished());
    assert!(!run_b.is_finished());
    run_a.abort();
    run_b.abort();
}

#[tokio::test]
async fn state_can_be_split_again_after_a_session_ends() {
    let line = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let mut state = UartState::new();

    for session in 0..2u8 {
        let (rx, tx) = hal(&line, &line, &stats);
        let Binding { mut port, runner } = uart(rx, tx).with_storage(&mut state).build();
        let msg = [session; 3];
        with_timeout(async {
            let app = async {
                port.write_all(&msg).await.unwrap();
                port.flush().await.unwrap();
            };
            tokio::select! {
                r = runner.run() => panic!("runner ended: {r:?}"),
                () = app => {}
            }
        })
        .await;
    }

    assert_eq!(stats.sent.lock().unwrap().as_slice(), &[0, 0, 0, 1, 1, 1]);
}

// --- The full reliable stack over the facade --------------------------------------

#[tokio::test]
async fn reliable_link_over_buffered_uart() {
    let ab = Line::unbounded();
    let ba = Line::unbounded();
    let stats = Arc::new(Stats::default());
    let (rx_a, mut tx_a) = hal(&ba, &ab, &stats);
    let (rx_b, mut tx_b) = hal(&ab, &ba, &stats);
    // Short transfers in both directions; ARQ must cope with partial HAL I/O.
    tx_a.max_write = 3;
    tx_b.max_write = 3;
    let mut state_a = UartState::new();
    let mut state_b = UartState::new();
    let Binding {
        port: port_a,
        runner: runner_a,
    } = uart(rx_a, tx_a).with_storage(&mut state_a).build();
    let Binding {
        port: port_b,
        runner: runner_b,
    } = uart(rx_b, tx_b).with_storage(&mut state_b).build();
    let data_ab = payload(3000);
    let data_ba = payload(1500);

    with_timeout(async {
        let mut link_a = reliable(port_a).build();
        let mut link_b = reliable(port_b).build();
        let side_a = async {
            link_a.write_all(&data_ab).await.unwrap();
            link_a.flush().await.unwrap();
            let mut got = vec![0u8; data_ba.len()];
            link_a.read_exact(&mut got).await.unwrap();
            got
        };
        let side_b = async {
            link_b.write_all(&data_ba).await.unwrap();
            link_b.flush().await.unwrap();
            let mut got = vec![0u8; data_ab.len()];
            link_b.read_exact(&mut got).await.unwrap();
            got
        };
        let (got_a, got_b) = tokio::select! {
            r = runner_a.run() => panic!("runner a ended: {r:?}"),
            r = runner_b.run() => panic!("runner b ended: {r:?}"),
            both = async { tokio::join!(side_a, side_b) } => both,
        };
        assert_eq!(got_a, data_ba);
        assert_eq!(got_b, data_ab);
    })
    .await;
}

#[test]
fn default_state_keeps_the_pump_buffer_sizes() {
    // The defaults of `UartState` are the pump's: 520-byte rings and a 64-byte
    // staging buffer. A change here changes RAM use.
    let _: UartState<520, 520, 64> = UartState::new();
}
