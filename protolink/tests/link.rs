//! Transport-layer tests for the COBS framing and the ARQ link stack.
#![cfg(feature = "tokio")]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use arq_io_async::{ArqError, ArqLayer, Transport};
use cobs_io_async::sync::{decode_to_slice, encode_from_slice_including_sentinels};
use embedded_io_async::{Error as _, ErrorKind, ErrorType, Read, Write};
use protolink::link::{
    CobsFramed, CobsTransport, LinkError, MAX_FRAME_PAYLOAD, ReliableError, StdTimer, Timer,
    reliable, reliable_with_timer,
};
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

/// One end of an in-memory framed link, the lower transport ARQ itself expects:
/// every write is one frame and every read returns one whole frame.
struct FrameEnd {
    rx: Rc<RefCell<FramePipe>>,
    tx: Rc<RefCell<FramePipe>>,
}

#[derive(Default)]
struct FramePipe {
    frames: VecDeque<Vec<u8>>,
    reader: Option<Waker>,
}

fn frame_link() -> (FrameEnd, FrameEnd) {
    let ab = Rc::new(RefCell::new(FramePipe::default()));
    let ba = Rc::new(RefCell::new(FramePipe::default()));
    (
        FrameEnd {
            rx: ba.clone(),
            tx: ab.clone(),
        },
        FrameEnd { rx: ab, tx: ba },
    )
}

impl Transport for FrameEnd {
    type Error = Infallible;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        let mut pipe = self.rx.borrow_mut();
        match pipe.frames.pop_front() {
            Some(frame) => {
                buf[..frame.len()].copy_from_slice(&frame);
                Poll::Ready(Ok(frame.len()))
            }
            None => {
                pipe.reader = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        let mut pipe = self.tx.borrow_mut();
        pipe.frames.push_back(buf.to_vec());
        if let Some(waker) = pipe.reader.take() {
            waker.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}

/// A raw ARQ pair over an in-memory framed fixture. Runs through
/// `poll_fn`, as there is no `embedded_io_async` adapter for ARQ itself.
#[tokio::test]
async fn arq_raw_round_trip() {
    let (a, b) = frame_link();
    let layer = ArqLayer::<8>::new();
    let mut x = layer.build(a, StdTimer::new());
    let mut y = layer.build(b, StdTimer::new());
    with_timeout(async {
        let writer = async {
            let mut data = &b"hello"[..];
            while !data.is_empty() {
                let n = poll_fn(|cx| x.poll_write(cx, data)).await.unwrap();
                data = &data[n..];
            }
            poll_fn(|cx| x.poll_flush(cx)).await.unwrap();
        };
        let reader = async {
            let mut got = Vec::new();
            while got.len() < 5 {
                let mut buf = [0u8; 5];
                let n = poll_fn(|cx| y.poll_read(cx, &mut buf)).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            got
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got, b"hello");
    })
    .await;
}

/// Send `payload` from `tx` and flush, while `rx` reads it back.
async fn transfer<W: Write, R: Read>(tx: &mut W, rx: &mut R, payload: &[u8])
where
    W::Error: std::fmt::Debug,
    R::Error: std::fmt::Debug,
{
    let writer = async {
        tx.write_all(payload).await.unwrap();
        tx.flush().await.unwrap();
    };
    let reader = async {
        let mut got = vec![0u8; payload.len()];
        rx.read_exact(&mut got).await.unwrap();
        got
    };
    let ((), got) = tokio::join!(writer, reader);
    assert_eq!(got, payload);
}

/// One end uses the default timer, the other an explicit one; they must
/// interoperate, in both directions.
#[tokio::test]
async fn reliable_constructors_interoperate() {
    let (a, b) = tokio::io::duplex(4096);
    let mut x = reliable(FromTokio::new(a));
    let mut y = reliable_with_timer(FromTokio::new(b), StdTimer::new());
    with_timeout(async {
        transfer(&mut x, &mut y, b"hello over cobs + arq").await;
        transfer(&mut y, &mut x, b"explicit timer").await;
    })
    .await;
}

/// Payload of a full ARQ frame: `MAX_FRAME` minus the 5-byte DAT header.
const ARQ_PAYLOAD: usize = MAX_FRAME_PAYLOAD - 5;
/// An ACK frame: type byte plus the 16-byte codeword of the default codec.
const ACK_FRAME: usize = 17;

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

fn encode_frame(frame: &[u8]) -> Vec<u8> {
    let mut encoded = vec![0; cobs_io_async::max_encoding_length(frame.len()) + 2];
    let n = encode_from_slice_including_sentinels(frame, &mut encoded).unwrap();
    encoded.truncate(n);
    encoded
}

impl FaultWire {
    fn emit(&mut self) {
        let mut decoded = [0; 256];
        let n = decode_to_slice(&self.encoded, &mut decoded).unwrap().len;
        self.encoded.clear();
        let mut frame = decoded[..n].to_vec();
        // ACK: type byte + 16-byte codeword. DAT: 5-byte header + payload; the
        // test only writes full blocks, so every DAT frame is a full frame.
        let is_ack = n == ACK_FRAME;
        if is_ack {
            self.stats.borrow_mut().acks += 1;
        } else {
            assert_eq!(n, MAX_FRAME_PAYLOAD);
            assert_eq!(frame[2] as usize, ARQ_PAYLOAD);
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
                    // Flip a codeword bit, not the type byte.
                    frame[1] ^= 1;
                    let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
                    assert!(
                        <arq_io_async::BchAckCodec as arq_io_async::AckCodec<16>>::decode_ack(
                            &crc,
                            frame[1..].try_into().unwrap(),
                        )
                        .is_ok()
                    );
                }
                Damage::AckUncorrectable => {
                    assert!(is_ack);
                    frame[1..].fill(0);
                    let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
                    assert!(
                        <arq_io_async::BchAckCodec as arq_io_async::AckCodec<16>>::decode_ack(
                            &crc,
                            frame[1..].try_into().unwrap(),
                        )
                        .is_err()
                    );
                }
                Damage::TruncateAck => {
                    assert!(is_ack);
                    frame.truncate(ACK_FRAME - 1);
                }
                Damage::AppendAck => {
                    assert!(is_ack);
                    // A valid ACK prefix is not a valid complete ACK frame.
                    frame.push(0x55);
                }
            }
            self.stats.borrow_mut().damaged += 1;
        }
        let mut encoded = encode_frame(&frame);
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
                    self.emit();
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
    // ARQ packs writes into one payload, so only full blocks make the frame
    // boundaries (and the three sequence numbers below) independent of timing.
    let expected: Vec<u8> = (0..3 * ARQ_PAYLOAD).map(|i| (i * 37) as u8).collect();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let writer = async {
        for block in expected.chunks(ARQ_PAYLOAD) {
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
    let mut bytes = encode_frame(&[253, 0, 61, 127, 0, 99]);
    bytes.extend(encode_frame(&[1, 2, 3]));
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
    bytes.extend(encode_frame(&[9, 9, 9]).into_iter().skip(1));
    bytes.extend(encode_frame(&[1, 2, 3]));
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
    oversized.extend(encode_frame(&[9, 9, 9]).into_iter().skip(1));
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
            bytes.extend(encode_frame(&[3, 0, 7, 8]));
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
        bytes.extend(encode_frame(&[4, 5, 6]));
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
            let codeword = <arq_io_async::BchAckCodec as arq_io_async::AckCodec<16>>::encode_ack(
                arq_io_async::AckFrame::new(&crc, 1).unwrap(),
            )
            .unwrap();
            let mut ack = vec![0b01];
            ack.extend_from_slice(&codeword);
            let encoded = encode_frame(&ack);
            input.borrow_mut().bytes.extend(encoded);
            if let Some(waker) = input.borrow_mut().reader.take() {
                waker.wake();
            }
        };
        with_timeout(async { tokio::join!(sender, observer) }).await;
    }
}

// ---------------------------------------------------------------------------
// COBS codec edge cases
// ---------------------------------------------------------------------------

async fn sent_bytes(payload: &[u8]) -> Vec<u8> {
    let stats = Rc::new(RefCell::new(ReadyStats::default()));
    let mut tx = CobsFramed::new(ReadyWire {
        input: Rc::default(),
        noise: Rc::new(Cell::new(None)),
        stats: stats.clone(),
        chunk: usize::MAX,
    });
    assert_eq!(tx.write(payload).await.unwrap(), payload.len());
    stats.borrow().written.clone()
}

#[tokio::test]
async fn cobs_block_boundaries_and_maximum_frame_round_trip() {
    for len in [1, 2, 253, 254, 255, MAX_FRAME_PAYLOAD] {
        let patterns: [Vec<u8>; 4] = [
            vec![0xAA; len],
            (0..len).map(|i| (i % 255 + 1) as u8).collect(),
            vec![0; len],
            (0..len)
                .map(|i| if i % 127 == 126 { 0 } else { 7 })
                .collect(),
        ];
        for payload in patterns {
            let wire = sent_bytes(&payload).await;
            assert_eq!(wire, encode_frame(&payload), "len {len}");
            assert_eq!((wire[0], wire[wire.len() - 1]), (0, 0));
            let mut rx = CobsFramed::new(wire.as_slice());
            let mut got = [0; MAX_FRAME_PAYLOAD];
            let n = rx.read_frame(&mut got).await.unwrap();
            assert_eq!(&got[..n], payload, "len {len}");
            assert_eq!(rx.read_frame(&mut got).await.unwrap(), 0);
        }
    }
}

#[tokio::test]
async fn empty_frames_are_skipped_not_mistaken_for_eof() {
    let mut bytes = encode_frame(&[]);
    bytes.extend(encode_frame(&[]));
    bytes.extend(encode_frame(&[5, 0, 6]));
    let mut rx = CobsFramed::new(bytes.as_slice());
    let mut got = [0; MAX_FRAME_PAYLOAD];
    assert_eq!(rx.read_frame(&mut got).await.unwrap(), 3);
    assert_eq!(&got[..3], &[5, 0, 6]);
    assert_eq!(rx.read_frame(&mut got).await.unwrap(), 0);

    let mut bytes = encode_frame(&[]);
    bytes.extend(encode_frame(&[8]));
    let mut rx = CobsFramed::new(bytes.as_slice());
    let mut one = [0; 4];
    assert_eq!(rx.read(&mut one).await.unwrap(), 1);
    assert_eq!(one[0], 8);
}

#[tokio::test]
async fn adjacent_frames_are_not_merged_and_partial_reads_do_not_leak() {
    let mut bytes = Vec::new();
    for frame in [&[1u8, 2, 3, 4][..], &[9], &[0, 0]] {
        bytes.extend(encode_frame(frame));
    }
    let mut rx = CobsFramed::new(bytes.as_slice());
    let mut got = [0; MAX_FRAME_PAYLOAD];
    // A generic partial read leaves a suffix, which is not a frame.
    let mut two = [0; 2];
    assert_eq!(rx.read(&mut two).await.unwrap(), 2);
    assert_eq!(two, [1, 2]);
    let n = rx.read_frame(&mut got).await.unwrap();
    assert_eq!(&got[..n], &[9]);
    let n = rx.read_frame(&mut got).await.unwrap();
    assert_eq!(&got[..n], &[0, 0]);
    assert_eq!(rx.read_frame(&mut got).await.unwrap(), 0);
}

#[tokio::test]
async fn damaged_cobs_frames_are_dropped_and_recovery_is_at_the_next_delimiter() {
    // Truncated block, oversized (> MAX_FRAME_PAYLOAD) frame, delimiter inside a
    // block, and an unterminated tail, each followed by a good frame.
    let oversized = encode_frame(&vec![3; MAX_FRAME_PAYLOAD + 1]);
    let damaged: [Vec<u8>; 4] = [
        vec![0, 5, 1, 2, 0],
        oversized,
        vec![0, 3, 1, 0],
        vec![0, 4, 4, 4, 4, 4, 4, 4],
    ];
    for bad in damaged {
        let mut bytes = bad.clone();
        // An unterminated tail is ended by the next frame's leading delimiter.
        bytes.extend(encode_frame(&[1, 2, 3]));
        let mut rx = CobsFramed::new(bytes.as_slice());
        let mut got = [0; MAX_FRAME_PAYLOAD];
        let n = rx.read_frame(&mut got).await.unwrap();
        assert_eq!(&got[..n], &[1, 2, 3], "bad frame {bad:?}");
        assert_eq!(rx.read_frame(&mut got).await.unwrap(), 0);
    }
}

// ---------------------------------------------------------------------------
// CobsTransport: ARQ's whole-frame lower transport
// ---------------------------------------------------------------------------

/// Raw stream that returns one byte per read, accepts three bytes per write,
/// and stalls (waking itself) on every other call, flush included.
struct Trickle {
    input: VecDeque<u8>,
    written: Rc<RefCell<Vec<u8>>>,
    flushes: Rc<Cell<usize>>,
    stall_next: bool,
}

impl Trickle {
    fn new(input: Vec<u8>) -> Self {
        Self {
            input: input.into(),
            written: Rc::default(),
            flushes: Rc::default(),
            stall_next: false,
        }
    }

    fn stall(&mut self, cx: &mut Context<'_>) -> bool {
        self.stall_next = !self.stall_next;
        if self.stall_next {
            cx.waker().wake_by_ref();
        }
        self.stall_next
    }
}

impl ErrorType for Trickle {
    type Error = Infallible;
}

impl Read for Trickle {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
        poll_fn(|cx| {
            if self.stall(cx) {
                return Poll::Pending;
            }
            match self.input.pop_front() {
                Some(byte) => {
                    buf[0] = byte;
                    Poll::Ready(Ok(1))
                }
                None => Poll::Ready(Ok(0)),
            }
        })
        .await
    }
}

impl Write for Trickle {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
        poll_fn(|cx| {
            if self.stall(cx) {
                return Poll::Pending;
            }
            let n = buf.len().min(3);
            self.written.borrow_mut().extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        })
        .await
    }

    async fn flush(&mut self) -> Result<(), Infallible> {
        poll_fn(|cx| {
            if self.stall(cx) {
                return Poll::Pending;
            }
            self.flushes.set(self.flushes.get() + 1);
            Poll::Ready(Ok(()))
        })
        .await
    }
}

/// Poll `f` with a fresh future each time, as ARQ does, until it is ready.
fn poll_until<T>(mut f: impl FnMut(&mut Context<'_>) -> Poll<T>) -> (T, usize) {
    let mut cx = Context::from_waker(Waker::noop());
    for polls in 1..100_000 {
        if let Poll::Ready(value) = f(&mut cx) {
            return (value, polls);
        }
    }
    panic!("still pending after 100000 polls");
}

#[test]
fn transport_write_reports_a_whole_frame_only_after_partial_and_pending_raw_writes() {
    let wire = Trickle::new(Vec::new());
    let (written, flushes) = (wire.written.clone(), wire.flushes.clone());
    let mut t = CobsTransport::new(wire);
    let frame: Vec<u8> = (0..200u8).collect();
    let (n, polls) = poll_until(|cx| t.poll_write(cx, &frame));
    assert_eq!(n.unwrap(), frame.len());
    assert!(polls > 50, "write finished in {polls} polls");
    assert_eq!(*written.borrow(), encode_frame(&frame));

    // The flush is pending at first and must not emit the frame again.
    let (flushed, polls) = poll_until(|cx| t.poll_flush(cx));
    flushed.unwrap();
    assert!(polls > 1);
    assert_eq!(flushes.get(), 1);
    assert_eq!(*written.borrow(), encode_frame(&frame));

    // The next frame starts cleanly.
    let (n, _) = poll_until(|cx| t.poll_write(cx, &[7, 0, 7]));
    assert_eq!(n.unwrap(), 3);
    let mut all = encode_frame(&frame);
    all.extend(encode_frame(&[7, 0, 7]));
    assert_eq!(*written.borrow(), all);
}

#[test]
fn transport_rejects_frames_it_cannot_carry_instead_of_clipping() {
    let wire = Trickle::new(Vec::new());
    let written = wire.written.clone();
    let mut t = CobsTransport::new(wire);
    let mut cx = Context::from_waker(Waker::noop());
    let big = [1u8; MAX_FRAME_PAYLOAD + 1];
    assert!(matches!(
        t.poll_write(&mut cx, &big),
        Poll::Ready(Err(LinkError::FrameSize))
    ));
    assert!(written.borrow().is_empty());
    assert!(matches!(
        t.poll_read(&mut cx, &mut [0; MAX_FRAME_PAYLOAD - 1]),
        Poll::Ready(Err(LinkError::FrameSize))
    ));
    // The maximum frame is carried whole.
    let (n, _) = poll_until(|cx| t.poll_write(cx, &big[..MAX_FRAME_PAYLOAD]));
    assert_eq!(n.unwrap(), MAX_FRAME_PAYLOAD);
}

#[test]
fn transport_read_returns_one_whole_frame_per_call_and_zero_only_at_eof() {
    let mut bytes = encode_frame(&[1, 2, 3]);
    bytes.extend(encode_frame(&[]));
    bytes.extend([0, 0, 0]);
    bytes.extend(encode_frame(&[0, 0, 9]));
    bytes.extend(encode_frame(&[4]));
    let mut t = CobsTransport::new(Trickle::new(bytes));
    let mut buf = [0u8; MAX_FRAME_PAYLOAD];
    for expected in [&[1u8, 2, 3][..], &[0, 0, 9], &[4]] {
        let (n, _) = poll_until(|cx| t.poll_read(cx, &mut buf));
        assert_eq!(&buf[..n.unwrap()], expected);
    }
    let (n, _) = poll_until(|cx| t.poll_read(cx, &mut buf));
    assert_eq!(n.unwrap(), 0, "EOF is the only zero-length read");
}

// ---------------------------------------------------------------------------
// ReliableLink: upper embedded-io interface and ARQ's terminal semantics
// ---------------------------------------------------------------------------

#[test]
fn reliable_errors_map_to_embedded_io_kinds() {
    use arq_io_async::{AckError, FrameError};
    type E = ReliableError<ErrorKind>;
    let kind = |e: ArqError<LinkError<ErrorKind>>| E::from(e).kind();
    assert_eq!(
        kind(ArqError::Io(LinkError::Io(ErrorKind::ConnectionReset))),
        ErrorKind::ConnectionReset
    );
    assert_eq!(
        kind(ArqError::Io(LinkError::WriteZero)),
        ErrorKind::WriteZero
    );
    assert_eq!(
        kind(ArqError::Io(LinkError::FrameSize)),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        kind(ArqError::Framing(FrameError::Invalid)),
        ErrorKind::InvalidData
    );
    assert_eq!(
        kind(ArqError::InvalidAck(AckError::DecodeError)),
        ErrorKind::InvalidData
    );
    assert_eq!(kind(ArqError::Timeout), ErrorKind::TimedOut);
    assert_eq!(kind(ArqError::Closed), ErrorKind::BrokenPipe);
    assert_eq!(kind(ArqError::WriteLength), ErrorKind::WriteZero);
    assert!(E::from(ArqError::Timeout).to_string().contains("timeout"));
}

/// A scripted raw stream: serves `input`, then reports EOF or stays quiet, and
/// records writes unless `blocked`.
struct ScriptWire {
    input: VecDeque<u8>,
    eof: bool,
    written: Rc<RefCell<Vec<u8>>>,
    blocked: Rc<Cell<bool>>,
    write_waker: Rc<RefCell<Option<Waker>>>,
}

struct Script {
    written: Rc<RefCell<Vec<u8>>>,
    blocked: Rc<Cell<bool>>,
    write_waker: Rc<RefCell<Option<Waker>>>,
}

impl ScriptWire {
    fn new(input: Vec<u8>, eof: bool) -> (Self, Script) {
        let script = Script {
            written: Rc::default(),
            blocked: Rc::default(),
            write_waker: Rc::default(),
        };
        let wire = Self {
            input: input.into(),
            eof,
            written: script.written.clone(),
            blocked: script.blocked.clone(),
            write_waker: script.write_waker.clone(),
        };
        (wire, script)
    }
}

impl ErrorType for ScriptWire {
    type Error = Infallible;
}

impl Read for ScriptWire {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
        poll_fn(|_| {
            if self.input.is_empty() {
                return if self.eof {
                    Poll::Ready(Ok(0))
                } else {
                    Poll::Pending
                };
            }
            let n = buf.len().min(self.input.len());
            for byte in &mut buf[..n] {
                *byte = self.input.pop_front().unwrap();
            }
            Poll::Ready(Ok(n))
        })
        .await
    }
}

impl Write for ScriptWire {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
        poll_fn(|cx| {
            if self.blocked.get() {
                *self.write_waker.borrow_mut() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            self.written.borrow_mut().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        })
        .await
    }

    async fn flush(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}

/// The raw bytes a peer link sends for its first, flushed write.
async fn peer_data_frame(payload: &[u8]) -> Vec<u8> {
    let (wire, script) = ScriptWire::new(Vec::new(), false);
    let mut peer = reliable_with_timer(wire, VirtualTimer::default());
    let mut cx = Context::from_waker(Waker::noop());
    peer.write_all(payload).await.unwrap();
    assert!(pin!(peer.flush()).as_mut().poll(&mut cx).is_pending());
    let bytes = script.written.borrow().clone();
    assert!(!bytes.is_empty(), "peer sent nothing");
    bytes
}

#[tokio::test(start_paused = true)]
async fn retry_exhaustion_is_a_terminal_timeout_after_buffered_data_is_delivered() {
    let (wire, _) = ScriptWire::new(peer_data_frame(b"abc").await, false);
    let mut link = reliable_with_timer(wire, VirtualTimer::default());
    link.write_all(b"ping").await.unwrap();
    // Nobody acknowledges: virtual time runs through every retransmission.
    let err = tokio::time::timeout(Duration::from_secs(600), link.flush())
        .await
        .expect("retransmission never gave up")
        .unwrap_err();
    assert_eq!(err.0, ArqError::Timeout);
    assert_eq!(err.kind(), ErrorKind::TimedOut);

    // The in-order data that arrived before the failure is still readable.
    let mut buf = [0; 8];
    let n = link.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"abc");
    // Afterwards the link is closed for every operation.
    assert_eq!(link.read(&mut buf).await.unwrap_err().0, ArqError::Closed);
    assert_eq!(link.write(b"x").await.unwrap_err().0, ArqError::Closed);
    assert_eq!(link.flush().await.unwrap_err().0, ArqError::Closed);
    assert_eq!(
        link.flush().await.unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
}

#[tokio::test(start_paused = true)]
async fn flush_waits_for_an_owed_ack_to_reach_the_wire() {
    let (wire, script) = ScriptWire::new(peer_data_frame(b"abc").await, false);
    let mut link = reliable_with_timer(wire, VirtualTimer::default());
    script.blocked.set(true);
    let wakes = Arc::new(CountWake::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    // Nothing of ours is unacknowledged, but an ACK is owed to the peer.
    assert!(pin!(link.flush()).as_mut().poll(&mut cx).is_pending());
    assert!(script.written.borrow().is_empty());
    // Dropping and re-creating the flush future strands nothing.
    assert!(pin!(link.flush()).as_mut().poll(&mut cx).is_pending());

    script.blocked.set(false);
    script.write_waker.borrow_mut().take().unwrap().wake();
    assert!(pin!(link.flush()).as_mut().poll(&mut cx).is_ready());
    assert!(!script.written.borrow().is_empty(), "ACK was not written");

    // With nothing owed or outstanding, flush is immediately ready.
    let (idle, _) = ScriptWire::new(Vec::new(), false);
    let mut idle = reliable_with_timer(idle, VirtualTimer::default());
    assert!(pin!(idle.flush()).as_mut().poll(&mut cx).is_ready());
}

#[tokio::test]
async fn lower_eof_does_not_discard_buffered_data() {
    let (wire, script) = ScriptWire::new(peer_data_frame(b"abc").await, true);
    let mut link = reliable_with_timer(wire, VirtualTimer::default());
    let mut buf = [0; 8];
    let n = with_timeout(link.read(&mut buf)).await.unwrap();
    assert_eq!(&buf[..n], b"abc");
    assert!(
        !script.written.borrow().is_empty(),
        "data was not acknowledged"
    );
    // The peer never sent a FIN, so the end of the lower link is an error.
    let err = with_timeout(link.read(&mut buf)).await.unwrap_err();
    assert_eq!(err.0, ArqError::Closed);
}

#[tokio::test]
async fn dropped_upper_read_loses_no_data_and_strands_nothing() {
    let (a, b) = tokio::io::duplex(4096);
    let mut x = reliable(FromTokio::new(a));
    let mut y = reliable(FromTokio::new(b));
    let mut cx = Context::from_waker(Waker::noop());
    let mut buf = [0u8; 8];
    // A pending read that is dropped, as the drivers do when they switch to a
    // write.
    assert!(pin!(y.read(&mut buf)).as_mut().poll(&mut cx).is_pending());
    with_timeout(async {
        let writer = async {
            x.write_all(b"after the drop").await.unwrap();
            x.flush().await.unwrap();
        };
        let reader = async {
            let mut got = [0u8; 14];
            y.read_exact(&mut got).await.unwrap();
            got
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got, b"after the drop");
    })
    .await;
}
