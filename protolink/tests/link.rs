//! Transport-layer tests for the COBS framing and the ARQ link stack.
#![cfg(feature = "tokio")]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::poll_fn;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use arq_io_async::embedded_io::ReadFrame;
use embedded_io_async::{ErrorType, Read, Write};
use protolink::link::{CobsFramed, Timer, reliable, reliable_with_timer};
use protolink::tokio::FromTokio;

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("timed out")
}

#[tokio::test]
async fn cobs_frames_round_trip() {
    let (a, b) = tokio::io::duplex(64);
    let mut tx = CobsFramed::new(FromTokio::new(a));
    let mut rx = CobsFramed::new(FromTokio::new(b));
    with_timeout(async {
        let writer = async {
            tx.write_all(&[1, 0, 2, 0, 0, 3]).await.unwrap();
            tx.write_all(&[0; 300]).await.unwrap();
            tx.flush().await.unwrap();
        };
        let reader = async {
            let mut got = Vec::new();
            let mut buf = [0u8; 100];
            while got.len() < 306 {
                let n = rx.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            got
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got[..6], &[1, 0, 2, 0, 0, 3]);
        assert!(got[6..].iter().all(|&b| b == 0));
    })
    .await;
}

#[tokio::test]
async fn arq_raw_round_trip() {
    let (a, b) = tokio::io::duplex(4096);
    let layer = arq_io_async::ArqLayer::<8, _, _>::new();
    let mut x = layer.build_with_timer::<16, { arq_io_async::r::<8>() }, _, _>(
        arq_io_async::embedded_io::EiaLower(FromTokio::new(a)),
        arq_io_async::StdTimer::new(),
    );
    let mut y = layer.build_with_timer::<16, { arq_io_async::r::<8>() }, _, _>(
        arq_io_async::embedded_io::EiaLower(FromTokio::new(b)),
        arq_io_async::StdTimer::new(),
    );
    with_timeout(async {
        let writer = async {
            x.write_all(b"hello").await.unwrap();
            x.flush().await.unwrap();
        };
        let reader = async {
            let mut buf = [0u8; 5];
            y.read_exact(&mut buf).await.unwrap();
            buf
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got, b"hello");
    })
    .await;
}

#[tokio::test]
async fn reliable_link_round_trip() {
    let (a, b) = tokio::io::duplex(4096);
    let mut x = reliable(FromTokio::new(a));
    let mut y = reliable(FromTokio::new(b));
    with_timeout(async {
        let writer = async {
            x.write_all(b"hello over cobs + arq").await.unwrap();
            x.flush().await.unwrap();
        };
        let reader = async {
            let mut buf = [0u8; 21];
            y.read_exact(&mut buf).await.unwrap();
            buf
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got, b"hello over cobs + arq");
    })
    .await;
}

#[tokio::test]
async fn reliable_with_timer_round_trip() {
    use protolink::link::{StdTimer, reliable_with_timer};

    let (a, b) = tokio::io::duplex(4096);
    let mut x = reliable_with_timer(FromTokio::new(a), StdTimer::new());
    let mut y = reliable_with_timer(FromTokio::new(b), StdTimer::new());
    with_timeout(async {
        let writer = async {
            x.write_all(b"explicit timer").await.unwrap();
            x.flush().await.unwrap();
        };
        let reader = async {
            let mut buf = [0u8; 14];
            y.read_exact(&mut buf).await.unwrap();
            buf
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got, b"explicit timer");
    })
    .await;
}

#[derive(Clone, Copy, Debug)]
enum Damage {
    Length(u8),
    Payload,
    Type,
    TruncateDat,
    AppendDat,
    AckBit,
    AckUncorrectable,
    TruncateAck,
    AppendAck,
}

#[derive(Default)]
struct Pipe {
    bytes: VecDeque<u8>,
    reader: Option<Waker>,
}

#[derive(Default)]
struct WireStats {
    damaged: usize,
    adjacent_batches: usize,
    data_sequences: Vec<u16>,
    acks: usize,
}

/// A cancel-safe raw wire. It damages decoded frames, then re-encodes them:
/// corruption therefore stays structurally valid COBS, as in the review.
struct FaultWire {
    rx: Rc<RefCell<Pipe>>,
    tx: Rc<RefCell<Pipe>>,
    encoded: Vec<u8>,
    held: Option<Vec<u8>>,
    batch_first_two: bool,
    damage: Option<Damage>,
    stats: Rc<RefCell<WireStats>>,
    read_chunk: usize,
    read_pending: bool,
    write_pending: bool,
}

impl ErrorType for FaultWire {
    type Error = Infallible;
}

impl Read for FaultWire {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        poll_fn(|cx| {
            if self.read_pending {
                self.read_pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let mut pipe = self.rx.borrow_mut();
            if pipe.bytes.is_empty() {
                pipe.reader = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let n = buf.len().min(self.read_chunk).min(pipe.bytes.len());
            for byte in &mut buf[..n] {
                *byte = pipe.bytes.pop_front().unwrap();
            }
            self.read_pending = true;
            Poll::Ready(Ok(n))
        })
        .await
    }
}

async fn encode_frame(frame: &[u8]) -> Vec<u8> {
    let mut encoded = vec![0; cobs_io_async::max_encoding_length(frame.len()) + 2];
    let n = cobs_io_async::embedded::encode_from_slice_including_sentinels_async(
        frame,
        &mut encoded.as_mut_slice(),
    )
    .await
    .unwrap() as usize;
    encoded.truncate(n);
    encoded
}

impl FaultWire {
    async fn emit(&mut self) {
        let mut decoded = [0; 256];
        let n = cobs_io_async::embedded::decode_to_slice_buffered_async(
            &mut self.encoded.as_slice(),
            &mut decoded,
        )
        .await
        .unwrap() as usize;
        self.encoded.clear();
        let mut frame = decoded[..n].to_vec();
        let is_ack = n == 16;
        if is_ack {
            self.stats.borrow_mut().acks += 1;
        } else {
            assert_eq!(n, 130);
            assert_eq!(frame[2], 125);
            self.stats
                .borrow_mut()
                .data_sequences
                .push(u16::from_le_bytes([frame[0], frame[1]]) >> 2);
        }
        if let Some(damage) = self.damage.take() {
            match damage {
                Damage::Length(len) => frame[2] = len,
                Damage::Payload => frame[42] ^= 0x80,
                Damage::Type => frame[0] = (frame[0] & !3) | 1,
                Damage::TruncateDat => frame.truncate(3),
                Damage::AppendDat => frame.push(0x55),
                Damage::AckBit => {
                    assert!(is_ack);
                    frame[0] ^= 1;
                    let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
                    assert!(
                        <arq_io_async::BchAckCodec as arq_io_async::AckCodec<16>>::decode_ack(
                            &crc,
                            frame.as_slice().try_into().unwrap(),
                        )
                        .is_ok()
                    );
                }
                Damage::AckUncorrectable => {
                    assert!(is_ack);
                    frame.fill(0);
                    let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
                    assert!(
                        <arq_io_async::BchAckCodec as arq_io_async::AckCodec<16>>::decode_ack(
                            &crc,
                            frame.as_slice().try_into().unwrap(),
                        )
                        .is_err()
                    );
                }
                Damage::TruncateAck => {
                    assert!(is_ack);
                    frame.truncate(15);
                }
                Damage::AppendAck => {
                    assert!(is_ack);
                    // A valid ACK prefix is not a valid complete ACK frame.
                    frame.push(0x55);
                }
            }
            self.stats.borrow_mut().damaged += 1;
        }
        let mut encoded = encode_frame(&frame).await;
        if self.batch_first_two {
            if let Some(mut first) = self.held.take() {
                first.append(&mut encoded);
                encoded = first;
                self.batch_first_two = false;
                self.stats.borrow_mut().adjacent_batches += 1;
            } else {
                self.held = Some(encoded);
                return;
            }
        }
        let mut pipe = self.tx.borrow_mut();
        pipe.bytes.extend(encoded);
        if let Some(waker) = pipe.reader.take() {
            waker.wake();
        }
    }
}

impl Write for FaultWire {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        poll_fn(|cx| {
            if self.write_pending {
                self.write_pending = false;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                self.write_pending = true;
                Poll::Ready(())
            }
        })
        .await;
        let n = buf.len().min(7);
        for &byte in &buf[..n] {
            if byte == 0 {
                if !self.encoded.is_empty() {
                    self.encoded.push(0);
                    self.emit().await;
                }
            } else {
                self.encoded.push(byte);
            }
        }
        Ok(n)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Default)]
struct VirtualTimer(Option<Pin<Box<tokio::time::Sleep>>>);

impl Timer for VirtualTimer {
    fn start(&mut self, timeout: Duration) {
        self.0 = Some(Box::pin(tokio::time::sleep(timeout)));
    }

    fn stop(&mut self) {
        self.0 = None;
    }

    fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match &mut self.0 {
            Some(sleep) => sleep.as_mut().poll(cx),
            None => Poll::Pending,
        }
    }
}

async fn corruption_round_trip(
    dat_damage: Option<Damage>,
    ack_damage: Option<Damage>,
    chunk: usize,
) {
    let ab = Rc::new(RefCell::new(Pipe::default()));
    let ba = Rc::new(RefCell::new(Pipe::default()));
    let tx_stats = Rc::new(RefCell::new(WireStats::default()));
    let rx_stats = Rc::new(RefCell::new(WireStats::default()));
    let wire = |rx, tx, damage, stats, batch_first_two| FaultWire {
        rx,
        tx,
        encoded: Vec::new(),
        held: None,
        batch_first_two,
        damage,
        stats,
        read_chunk: chunk,
        read_pending: true,
        write_pending: true,
    };
    let mut x = reliable_with_timer(
        wire(ba.clone(), ab.clone(), dat_damage, tx_stats.clone(), true),
        VirtualTimer::default(),
    );
    let mut y = reliable_with_timer(
        wire(ab, ba, ack_damage, rx_stats.clone(), false),
        VirtualTimer::default(),
    );
    let expected: Vec<u8> = (0..375).map(|i| (i * 37) as u8).collect();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let writer = async {
        for block in expected.chunks(125) {
            x.write_all(block).await.unwrap();
        }
        x.flush().await.unwrap();
        done_tx.send(()).unwrap();
    };
    let reader = async {
        let mut got = vec![0; expected.len()];
        y.read_exact(&mut got).await.unwrap();
        assert_eq!(
            got, expected,
            "damage: {dat_damage:?}, {ack_damage:?}, chunk: {chunk}"
        );
        // Keep ARQ alive after delivery: a lost final ACK needs duplicate DATs
        // to be read and acknowledged before the sender can finish flush.
        let drive = async {
            let mut extra = [0; 32];
            loop {
                let n = y.read(&mut extra).await.unwrap();
                assert_eq!(n, 0, "duplicate bytes delivered");
            }
        };
        tokio::select! {
            result = done_rx => result.unwrap(),
            () = drive => unreachable!(),
        }
    };
    with_timeout(async { tokio::join!(writer, reader) }).await;
    let tx = tx_stats.borrow();
    assert_eq!(tx.damaged, usize::from(dat_damage.is_some()));
    assert_eq!(tx.adjacent_batches, 1);
    assert!(tx.data_sequences.contains(&1));
    assert!(tx.data_sequences.contains(&2));
    let retry_ack = matches!(
        ack_damage,
        Some(Damage::AckUncorrectable | Damage::TruncateAck | Damage::AppendAck)
    );
    if dat_damage.is_some() || retry_ack {
        assert!(tx.data_sequences.iter().filter(|&&sn| sn == 0).count() >= 2);
    }
    let rx = rx_stats.borrow();
    assert_eq!(rx.damaged, usize::from(ack_damage.is_some()));
    assert!(rx.acks > 0);
    if retry_ack {
        assert!(
            rx.acks >= 2,
            "invalid ACK was accepted instead of retransmitting"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn corrupted_lengths_discard_whole_frames_and_retransmit() {
    for len in [253, 61, 127] {
        for chunk in [1, 7, usize::MAX] {
            corruption_round_trip(Some(Damage::Length(len)), None, chunk).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn corrupt_payload_type_and_boundaries_recover() {
    for damage in [
        Damage::Payload,
        Damage::Type,
        Damage::TruncateDat,
        Damage::AppendDat,
    ] {
        for chunk in [1, 7, usize::MAX] {
            corruption_round_trip(Some(damage), None, chunk).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn corrupt_ack_codewords_and_boundaries_recover() {
    for damage in [
        Damage::AckBit,
        Damage::AckUncorrectable,
        Damage::TruncateAck,
        Damage::AppendAck,
    ] {
        for chunk in [1, 7, usize::MAX] {
            corruption_round_trip(None, Some(damage), chunk).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn corrupt_data_and_ack_in_same_burst_recover() {
    for len in [253, 61, 127] {
        corruption_round_trip(Some(Damage::Length(len)), Some(Damage::AckUncorrectable), 7).await;
    }
}

#[tokio::test]
async fn generic_cobs_read_preserves_partial_non_arq_payloads() {
    let mut bytes = encode_frame(&[253, 0, 61, 127, 0, 99]).await;
    bytes.extend(encode_frame(&[1, 2, 3]).await);
    let mut rx = CobsFramed::new(bytes.as_slice());
    let mut empty = [];
    assert_eq!(rx.read(&mut empty).await.unwrap(), 0);
    let mut got = Vec::new();
    let mut buf = [0; 2];
    loop {
        let n = rx.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, [253, 0, 61, 127, 0, 99, 1, 2, 3]);
}

#[tokio::test]
async fn oversized_cobs_frame_discards_suffix_until_delimiter() {
    let mut bytes = vec![1; 600];
    // This valid-looking suffix belongs to the oversized damaged frame.
    bytes.extend(encode_frame(&[9, 9, 9]).await.into_iter().skip(1));
    bytes.extend(encode_frame(&[1, 2, 3]).await);
    let mut rx = CobsFramed::new(bytes.as_slice());
    let mut got = [0; 8];
    let n = rx.read(&mut got).await.unwrap();
    assert_eq!(&got[..n], &[1, 2, 3]);
    assert_eq!(rx.read(&mut got).await.unwrap(), 0);
}

#[derive(Default)]
struct ReadyStats {
    reads: usize,
    bytes: usize,
    written: Vec<u8>,
}

/// Unlike FaultWire, this source never suspends while it has queued bytes or
/// noise. Only CobsFramed's own budget can return control to the caller.
struct ReadyWire {
    input: Rc<RefCell<Pipe>>,
    noise: Rc<Cell<Option<u8>>>,
    stats: Rc<RefCell<ReadyStats>>,
    chunk: usize,
}

impl ErrorType for ReadyWire {
    type Error = Infallible;
}

impl Read for ReadyWire {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        poll_fn(|cx| {
            let mut input = self.input.borrow_mut();
            let mut stats = self.stats.borrow_mut();
            stats.reads += 1;
            let n = if !input.bytes.is_empty() {
                let n = buf.len().min(self.chunk).min(input.bytes.len());
                for byte in &mut buf[..n] {
                    *byte = input.bytes.pop_front().unwrap();
                }
                n
            } else if let Some(byte) = self.noise.get() {
                // Make a regression fail rather than hang the runtime forever.
                assert!(stats.reads < 10_000, "receive poll monopolized the runtime");
                let n = buf.len().min(self.chunk);
                buf[..n].fill(byte);
                n
            } else {
                input.reader = Some(cx.waker().clone());
                return Poll::Pending;
            };
            stats.bytes += n;
            Poll::Ready(Ok(n))
        })
        .await
    }
}

impl Write for ReadyWire {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.stats.borrow_mut().written.extend_from_slice(buf);
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Default)]
struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn always_ready_cobs_garbage_yields_and_preserves_following_frame() {
    let raw_capacity = 2 * (cobs_io_async::max_encoding_length(256) + 2);
    let mut oversized = vec![1; raw_capacity * 1024];
    // This valid-looking suffix must remain discarded across budget yields.
    oversized.extend(encode_frame(&[9, 9, 9]).await.into_iter().skip(1));
    let cases = [
        (vec![0; 8192], 1),
        (vec![0; 8192], usize::MAX),
        ([1, 0].repeat(4096), 1), // Structurally valid empty COBS frames.
        ([1, 0].repeat(4096), usize::MAX),
        ([255, 0].repeat(4096), 1), // Truncated COBS blocks.
        ([255, 0].repeat(4096), usize::MAX),
        (oversized.clone(), 7),
        (oversized, usize::MAX),
    ];
    for (garbage, chunk) in cases {
        for framed in [false, true] {
            let mut bytes = garbage.clone();
            bytes.extend(encode_frame(&[3, 0, 7, 8]).await);
            let input = Rc::new(RefCell::new(Pipe {
                bytes: bytes.into(),
                reader: None,
            }));
            let stats = Rc::new(RefCell::new(ReadyStats::default()));
            let mut rx = CobsFramed::new(ReadyWire {
                input,
                noise: Rc::new(Cell::new(None)),
                stats: stats.clone(),
                chunk,
            });
            let wakes = Arc::new(CountWake::default());
            let waker = Waker::from(wakes.clone());
            let mut cx = Context::from_waker(&waker);
            let mut buf = [0; 256];
            let mut delivered = false;
            for poll in 0..100_000 {
                let before_reads = stats.borrow().reads;
                let before_bytes = stats.borrow().bytes;
                let before_wakes = wakes.0.load(Ordering::Relaxed);
                // Recreate the future on every poll, like ARQ. Partial raw
                // input and oversized-frame discard state must survive this.
                let result = if framed {
                    std::pin::pin!(rx.read_frame(&mut buf)).poll(&mut cx)
                } else {
                    std::pin::pin!(rx.read(&mut buf)).poll(&mut cx)
                };
                assert!(stats.borrow().reads - before_reads <= 32);
                assert!(stats.borrow().bytes - before_bytes <= 32 * raw_capacity);
                if poll == 0 {
                    assert!(result.is_pending(), "unbounded garbage scan: chunk {chunk}");
                }
                match result {
                    Poll::Pending => {
                        assert!(wakes.0.load(Ordering::Relaxed) > before_wakes);
                    }
                    Poll::Ready(result) => {
                        let n = result.unwrap();
                        assert_eq!(&buf[..n], &[3, 0, 7, 8]);
                        delivered = true;
                        break;
                    }
                }
            }
            assert!(
                delivered,
                "following frame lost: chunk {chunk}, framed {framed}"
            );
        }
    }
}

#[tokio::test]
async fn cobs_receive_budget_resumes_a_retained_future() {
    for framed in [false, true] {
        let mut bytes = vec![0; 8192];
        bytes.extend(encode_frame(&[4, 5, 6]).await);
        let stats = Rc::new(RefCell::new(ReadyStats::default()));
        let mut rx = CobsFramed::new(ReadyWire {
            input: Rc::new(RefCell::new(Pipe {
                bytes: bytes.into(),
                reader: None,
            })),
            noise: Rc::new(Cell::new(None)),
            stats: stats.clone(),
            chunk: 1,
        });
        let wakes = Arc::new(CountWake::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut buf = [0; 256];
        let mut future: Pin<Box<dyn Future<Output = _>>> = if framed {
            Box::pin(rx.read_frame(&mut buf))
        } else {
            Box::pin(rx.read(&mut buf))
        };
        let mut delivered = false;
        for poll in 0..1024 {
            let before_reads = stats.borrow().reads;
            let before_wakes = wakes.0.load(Ordering::Relaxed);
            let result = future.as_mut().poll(&mut cx);
            assert!(stats.borrow().reads - before_reads <= 32);
            if poll == 0 {
                assert!(result.is_pending());
            }
            match result {
                Poll::Pending => assert!(wakes.0.load(Ordering::Relaxed) > before_wakes),
                Poll::Ready(result) => {
                    assert_eq!(result.unwrap(), 3);
                    delivered = true;
                    break;
                }
            }
        }
        drop(future);
        assert!(delivered);
        assert_eq!(&buf[..3], &[4, 5, 6]);
    }
}

#[tokio::test(start_paused = true)]
async fn always_ready_cobs_noise_allows_runtime_and_retransmission_progress() {
    for byte in [0, 1] {
        let input = Rc::new(RefCell::new(Pipe::default()));
        let noise = Rc::new(Cell::new(Some(byte)));
        let stats = Rc::new(RefCell::new(ReadyStats::default()));
        let mut link = reliable_with_timer(
            ReadyWire {
                input: input.clone(),
                noise: noise.clone(),
                stats: stats.clone(),
                chunk: usize::MAX,
            },
            VirtualTimer::default(),
        );
        let sender = async {
            link.write_all(b"ping").await.unwrap();
            link.flush().await.unwrap();
        };
        let observer = async {
            tokio::task::yield_now().await;
            let first = stats.borrow().written.clone();
            assert!(
                !first.is_empty(),
                "noise prevented the initial transmission"
            );
            // A runnable noise receiver prevents automatic virtual-time
            // advancement. This other task must run to expire the ARQ timer.
            tokio::time::advance(Duration::from_millis(250)).await;
            for _ in 0..100 {
                if stats.borrow().written.len() > first.len() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            let written = stats.borrow().written.clone();
            assert!(written.len() > first.len(), "noise starved retransmission");
            assert_eq!(&written[..first.len()], &written[first.len()..]);
            // End the noise and acknowledge the retransmission. The leading
            // COBS delimiter also ends an oversized delimiter-free frame.
            noise.set(None);
            let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
            let ack = <arq_io_async::BchAckCodec as arq_io_async::AckCodec<16>>::encode_ack(
                arq_io_async::AckFrame::new(&crc, 1).unwrap(),
            )
            .unwrap();
            let encoded = encode_frame(&ack).await;
            input.borrow_mut().bytes.extend(encoded);
            if let Some(waker) = input.borrow_mut().reader.take() {
                waker.wake();
            }
        };
        with_timeout(async { tokio::join!(sender, observer) }).await;
    }
}
