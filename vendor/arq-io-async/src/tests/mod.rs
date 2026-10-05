use core::cell::RefCell;
use core::convert::Infallible;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::rc::Rc;

use crate::Arq;
use crate::ack_codec::BchAckCodec;
use crate::error::{ArqError, FrameError};
use crate::frame::{AckFrame, DatFrame, Frame};
use crate::timer::Timer;
use crate::transport::FrameIo;
use crate::{ArqLayer, Op, OpOut, State, r};

#[cfg(feature = "tokio")]
mod duplex;
mod framed;

type Crc16X25 = ::crc::Crc<u16>;
type TestArq = Arq<4, 16, { r::<4>() }, MockLink, Crc16X25, BchAckCodec, ManualTimer>;

fn crc16() -> Crc16X25 {
    Crc16X25::new(&::crc::CRC_16_IBM_SDLC)
}

fn make_arq() -> TestArq {
    ArqLayer::<4, Crc16X25, BchAckCodec>::new()
        .build_with_timer(MockLink::new(), Clock::default().timer())
}

fn new_arq<L>(link: L) -> Arq<4, 16, { r::<4>() }, L, Crc16X25, BchAckCodec, ManualTimer> {
    ArqLayer::<4, Crc16X25, BchAckCodec>::new().build_with_timer(link, Clock::default().timer())
}

#[derive(Default)]
struct ClockState {
    now: Duration,
    waiters: Vec<Option<(Duration, Waker)>>,
    starts: Vec<Duration>,
}

#[derive(Clone, Default)]
struct Clock(Rc<RefCell<ClockState>>);

impl Clock {
    fn timer(&self) -> ManualTimer {
        let mut s = self.0.borrow_mut();
        s.waiters.push(None);
        ManualTimer {
            clock: self.clone(),
            id: s.waiters.len() - 1,
            deadline: None,
        }
    }

    fn advance(&self, d: Duration) {
        let now = self.0.borrow().now + d;
        self.set(now);
    }

    fn advance_to_next(&self) -> bool {
        let next = self
            .0
            .borrow()
            .waiters
            .iter()
            .flatten()
            .map(|(d, _)| *d)
            .min();
        match next {
            Some(d) => {
                let now = self.0.borrow().now.max(d);
                self.set(now);
                true
            }
            None => false,
        }
    }

    fn set(&self, now: Duration) {
        let mut ready = Vec::new();
        {
            let mut s = self.0.borrow_mut();
            s.now = now;
            for slot in s.waiters.iter_mut() {
                if matches!(slot, Some((d, _)) if *d <= now) {
                    ready.push(slot.take().unwrap().1);
                }
            }
        }
        for w in ready {
            w.wake();
        }
    }

    fn starts(&self) -> Vec<Duration> {
        self.0.borrow().starts.clone()
    }
}

struct ManualTimer {
    clock: Clock,
    id: usize,
    deadline: Option<Duration>,
}

impl Timer for ManualTimer {
    fn start(&mut self, timeout: Duration) {
        let mut s = self.clock.0.borrow_mut();
        self.deadline = Some(s.now + timeout);
        s.waiters[self.id] = None;
        s.starts.push(timeout);
    }

    fn stop(&mut self) {
        self.deadline = None;
        self.clock.0.borrow_mut().waiters[self.id] = None;
    }

    fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(deadline) = self.deadline else {
            return Poll::Pending;
        };
        let mut s = self.clock.0.borrow_mut();
        if s.now >= deadline {
            return Poll::Ready(());
        }
        s.waiters[self.id] = Some((deadline, cx.waker().clone()));
        Poll::Pending
    }
}

fn expire<L>(arq: &Arq<4, 16, { r::<4>() }, L, Crc16X25, BchAckCodec, ManualTimer>) {
    arq.timer.clock.advance(arq.rto);
}

struct MockLink {
    rx: Vec<u8>,
    tx: Vec<u8>,
    eof: bool,
}

impl MockLink {
    fn new() -> Self {
        Self {
            rx: Vec::new(),
            tx: Vec::new(),
            eof: false,
        }
    }
}

impl FrameIo for MockLink {
    type Error = Infallible;

    fn poll_send(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        self.tx.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_recv(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        if self.rx.is_empty() {
            if self.eof {
                Poll::Ready(Ok(0))
            } else {
                Poll::Pending
            }
        } else {
            let n = self.rx.len().min(buf.len());
            buf[..n].copy_from_slice(&self.rx[..n]);
            self.rx.drain(..n);
            Poll::Ready(Ok(n))
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}

struct Peer {
    next: u16,
    seen: Vec<u16>,
    silent: bool,
    tx_to_us: Vec<u8>,
}

impl Peer {
    fn new() -> Self {
        Self {
            next: 0,
            seen: Vec::new(),
            silent: false,
            tx_to_us: Vec::new(),
        }
    }

    fn push(&mut self, bytes: Vec<u8>) {
        self.tx_to_us.extend(bytes);
    }

    fn respond(&mut self, arq: &mut TestArq, offset: &mut usize) {
        let fresh = &arq.channel.tx[*offset..];
        *offset = arq.channel.tx.len();
        if !self.silent {
            for frame in parse_stream(fresh) {
                match frame {
                    Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => {
                        let sn = d.sn();
                        let dup = self.seen.contains(&sn);
                        if !dup {
                            self.seen.push(sn);
                        }
                        if sn == self.next || dup {
                            self.push(wire_ack(sn.wrapping_add(1)));
                            if sn == self.next {
                                self.next = sn.wrapping_add(1);
                            }
                        }
                    }
                    Frame::Ack(_) => {}
                }
            }
        }
        arq.channel.rx.extend(core::mem::take(&mut self.tx_to_us));
    }
}

fn noop_cx() -> Context<'static> {
    static W: std::sync::OnceLock<&'static Waker> = std::sync::OnceLock::new();
    Context::from_waker(W.get_or_init(Waker::noop))
}

fn drive(
    arq: &mut TestArq,
    op: &mut Op,
    peer: &mut Peer,
    offset: &mut usize,
    max_steps: usize,
) -> Option<Result<OpOut, ArqError<Infallible>>> {
    let mut cx = noop_cx();
    for _ in 0..max_steps {
        match arq.poll_op(&mut cx, op) {
            Poll::Ready(r) => return Some(r),
            Poll::Pending => peer.respond(arq, offset),
        }
    }
    None
}

fn drive_out(
    arq: &mut TestArq,
    op: &mut Op,
    peer: &mut Peer,
    offset: &mut usize,
    max_steps: usize,
) -> OpOut {
    match drive(arq, op, peer, offset, max_steps) {
        Some(Ok(out)) => out,
        Some(Err(e)) => panic!("arq error: {e:?}"),
        None => panic!("no progress within {max_steps} steps"),
    }
}

fn encode_frame(frame: &Frame) -> Vec<u8> {
    let mut buf = [0u8; 256];
    let n = frame
        .to_bytes::<BchAckCodec, 16>(&mut buf)
        .expect("frame encode");
    buf[..n].to_vec()
}

fn wire_ack(an: u16) -> Vec<u8> {
    encode_frame(&Frame::Ack(AckFrame::new(&crc16(), an).unwrap()))
}

fn wire_dat(sn: u16, payload: &[u8]) -> Vec<u8> {
    encode_frame(&Frame::Dat(DatFrame::new_dat(&crc16(), sn, payload)))
}

fn wire_dat_ack_req(sn: u16, payload: &[u8]) -> Vec<u8> {
    encode_frame(&Frame::DatAckReq(DatFrame::new_dat_ack_req(
        &crc16(),
        sn,
        payload,
    )))
}

fn wire_fin(sn: u16, payload: &[u8]) -> Vec<u8> {
    encode_frame(&Frame::Fin(DatFrame::new_fin(&crc16(), sn, payload)))
}

fn parse_stream(bytes: &[u8]) -> Vec<Frame> {
    let mut out = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let len = Frame::wire_len::<BchAckCodec, 16, _>(&crc16(), rest).expect("wire len");
        let frame =
            Frame::from_bytes::<BchAckCodec, 16, _>(&crc16(), &rest[..len]).expect("frame decode");
        out.push(frame);
        rest = &rest[len..];
    }
    out
}

fn write_until(arq: &mut TestArq, data: &[u8], peer: &mut Peer, offset: &mut usize) {
    let mut off = 0usize;
    while off < data.len() {
        let mut op = Op::Write { buf: &data[off..] };
        match drive_out(arq, &mut op, peer, offset, 100) {
            OpOut::Write(n) => off += n,
            other => panic!("unexpected write result: {other:?}"),
        }
    }
}

#[test]
fn codec_roundtrip() {
    let crc = crc16();
    let mut buf = [0u8; 256];
    let f = DatFrame::new_dat(&crc, 42, b"hello");
    let n = f.to_bytes(&mut buf);
    let g = DatFrame::from_bytes(&crc, &buf[..n]).unwrap();
    assert_eq!(g.sn(), 42);
    assert_eq!(g.payload(), b"hello");
    assert!(!g.is_fin());
    let f = DatFrame::new_fin(&crc, 7, b"x");
    let n = f.to_bytes(&mut buf);
    let g = DatFrame::from_bytes(&crc, &buf[..n]).unwrap();
    assert_eq!(g.sn(), 7);
    assert_eq!(g.payload(), b"x");
    assert!(g.is_fin());
    assert!(!g.requests_ack());
    let f = DatFrame::new_dat_ack_req(&crc, 300, b"req");
    let n = f.to_bytes(&mut buf);
    assert_eq!(buf[0] & 0b11, 0b00);
    match Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &buf[..n]).unwrap() {
        Frame::DatAckReq(g) => {
            assert_eq!(g.sn(), 300);
            assert_eq!(g.payload(), b"req");
            assert!(g.requests_ack());
            assert!(!g.is_fin());
        }
        other => panic!("unexpected frame: {other:?}"),
    }
    let plain = DatFrame::new_dat(&crc, 300, b"req");
    assert!(!plain.requests_ack());
    let marked = plain.to_ack_req(&crc);
    assert!(marked.requests_ack());
    assert_eq!(encode_frame(&Frame::DatAckReq(marked)), buf[..n].to_vec());
    let a = AckFrame::new(&crc, 99).unwrap();
    let n = Frame::Ack(a).to_bytes::<BchAckCodec, 16>(&mut buf).unwrap();
    match Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &buf[..n]).unwrap() {
        Frame::Ack(a2) => assert_eq!(a2.an(), 99),
        other => panic!("unexpected frame: {other:?}"),
    }
}

#[test]
fn framing_errors() {
    let crc = crc16();
    let mut bytes = wire_dat(0, b"hello");
    bytes[9] ^= 0xFF;
    assert!(Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes).is_err());
    let mut bytes = wire_dat(0, b"hi");
    bytes[0] = 0x01;
    assert!(matches!(
        Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes),
        Err(FrameError::InvalidType(1))
    ));
    let mut bytes = wire_dat(0, b"hi");
    bytes[0] &= !0b11;
    assert!(matches!(
        Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes),
        Err(FrameError::CrcMismatch(..))
    ));
    let bytes = wire_dat(0, b"hello");
    assert!(matches!(
        Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes[..4]),
        Err(FrameError::TooShort(_))
    ));
}

#[test]
fn wire_len_covers_all_data_types() {
    let crc = crc16();
    for bytes in [
        wire_dat(3, b"abc"),
        wire_dat_ack_req(3, b"abc"),
        wire_fin(3, b"abc"),
    ] {
        assert_eq!(
            Frame::wire_len::<BchAckCodec, 16, _>(&crc, &bytes).unwrap(),
            8
        );
    }
    assert_eq!(
        Frame::wire_len::<BchAckCodec, 16, _>(&crc, &wire_ack(3)).unwrap(),
        16
    );
    let mut stream = wire_dat_ack_req(0, b"x");
    stream.extend(wire_dat(1, b"yz"));
    let frames = parse_stream(&stream);
    assert!(matches!(frames[0], Frame::DatAckReq(d) if d.sn() == 0));
    assert!(matches!(frames[1], Frame::Dat(d) if d.sn() == 1));
}

#[test]
fn bch_corruption() {
    let crc = crc16();
    let mut bytes = wire_ack(5);
    bytes[3] ^= 0x01;
    match Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes).unwrap() {
        Frame::Ack(a) => assert_eq!(a.an(), 5),
        other => panic!("unexpected frame: {other:?}"),
    }
    let mut bytes = wire_ack(5);
    for i in 0..3 {
        bytes[i] ^= 0xFF;
    }
    match Frame::from_bytes::<BchAckCodec, 16, _>(&crc, &bytes) {
        Err(_) => {}
        Ok(Frame::Ack(a)) => assert_ne!(a.an(), 5),
        Ok(other) => panic!("unexpected frame: {other:?}"),
    }
}

#[test]
fn bidirectional_transfer() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Shutdown;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    peer.push(wire_dat(0, b"xy"));
    peer.push(wire_fin(1, b"z"));
    peer.respond(&mut arq, &mut off);
    let mut got = Vec::new();
    loop {
        let mut buf = [0u8; 64];
        let mut op = Op::Read { buf: &mut buf };
        match drive_out(&mut arq, &mut op, &mut peer, &mut off, 100) {
            OpOut::Read(0) => break,
            OpOut::Read(n) => got.extend_from_slice(&buf[..n]),
            other => panic!("unexpected read result: {other:?}"),
        }
    }
    assert_eq!(got, b"xyz");
    assert_eq!(arq.state, State::Done);
}

#[test]
fn large_transfer_reassembly() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let data: Vec<u8> = (0..600u16).map(|i| (i % 251) as u8).collect();
    peer.push(wire_dat(0, &data[0..250]));
    peer.push(wire_dat(1, &data[250..500]));
    peer.push(wire_fin(2, &data[500..600]));
    peer.respond(&mut arq, &mut off);
    let mut got = Vec::new();
    loop {
        let mut buf = [0u8; 100];
        let mut op = Op::Read { buf: &mut buf };
        match drive_out(&mut arq, &mut op, &mut peer, &mut off, 100) {
            OpOut::Read(0) => break,
            OpOut::Read(n) => got.extend_from_slice(&buf[..n]),
            other => panic!("unexpected read result: {other:?}"),
        }
    }
    assert_eq!(got, data);
}

#[test]
fn out_of_order_frames() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat(1, b"bc"));
    peer.respond(&mut arq, &mut off);
    let mut buf = [0u8; 4];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 6).is_none());
    peer.push(wire_dat(0, b"a"));
    peer.push(wire_fin(2, b"d"));
    peer.respond(&mut arq, &mut off);
    match drive_out(&mut arq, &mut op, &mut peer, &mut off, 100) {
        OpOut::Read(4) => assert_eq!(&buf[..], b"abcd"),
        other => panic!("unexpected read result: {other:?}"),
    }
}

#[test]
fn retransmit_on_lost_ack() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    peer.silent = true;
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    expire(&arq);
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    let frames = parse_stream(&arq.channel.tx);
    let copies = frames
        .iter()
        .filter(|f| matches!(f, Frame::DatAckReq(d) if d.sn() == 0))
        .count();
    assert_eq!(copies, 2, "expected one retransmission, saw {frames:?}");
    peer.silent = false;
    expire(&arq);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
    assert_eq!(arq.w, 0);
}

#[test]
fn window_full_backpressure() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let data: Vec<u8> = (0..1250u16).map(|i| (i % 252) as u8).collect();
    write_until(&mut arq, &data, &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 200),
        OpOut::Done
    ));
    let frames = parse_stream(&arq.channel.tx);
    let sns: Vec<u16> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::Dat(d) | Frame::DatAckReq(d) => Some(d.sn()),
            _ => None,
        })
        .collect();
    for sn in 0..5u16 {
        assert!(sns.contains(&sn), "missing frame {sn}, saw {sns:?}");
    }
}

#[test]
fn flush_completes() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    write_until(&mut arq, b"0123456789", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    let frames = parse_stream(&arq.channel.tx);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        Frame::DatAckReq(d) => {
            assert_eq!(d.sn(), 0);
            assert_eq!(d.payload(), b"0123456789");
        }
        other => panic!("unexpected frame: {other:?}"),
    }
}

#[test]
fn flush_waits_for_ack() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(
        drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none(),
        "flush completed before the frame was acknowledged"
    );
    peer.push(wire_ack(1));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 50),
        OpOut::Done
    ));
}

#[test]
fn flush_empty_stream_completes() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut op = Op::Flush;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 10),
        OpOut::Done
    ));
}

#[test]
fn flush_does_not_arm_fin() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    write_until(&mut arq, b"ef", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    assert_eq!(arq.state, State::Active);
}

#[test]
fn shutdown_with_no_data_sends_fin() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut op = Op::Shutdown;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    let frames = parse_stream(&arq.channel.tx);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        Frame::Fin(d) => {
            assert_eq!(d.sn(), 0);
            assert_eq!(d.len(), 0);
        }
        other => panic!("unexpected frame: {other:?}"),
    }
}

#[test]
fn retransmit_cycles_window() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let data: Vec<u8> = vec![0u8; 1000];
    write_until(&mut arq, &data, &mut peer, &mut off);
    peer.silent = true;
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    for _ in 0..2 {
        expire(&arq);
        assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    }
    let frames = parse_stream(&arq.channel.tx);
    let sns: Vec<u16> = frames
        .iter()
        .filter_map(|f| match f {
            Frame::Dat(d) | Frame::Fin(d) => Some(d.sn()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sns,
        [0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3],
        "each timeout retransmits the window once, in order"
    );
}

#[test]
fn duplicate_in_order_frame_ignored() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat(0, b"a"));
    peer.push(wire_dat(0, b"a"));
    peer.push(wire_fin(1, b"b"));
    peer.respond(&mut arq, &mut off);
    let mut got = Vec::new();
    loop {
        let mut buf = [0u8; 16];
        let mut op = Op::Read { buf: &mut buf };
        match drive_out(&mut arq, &mut op, &mut peer, &mut off, 100) {
            OpOut::Read(0) => break,
            OpOut::Read(n) => got.extend_from_slice(&buf[..n]),
            other => panic!("unexpected read result: {other:?}"),
        }
    }
    assert_eq!(got, b"ab");
}

#[test]
fn corrupted_dat_is_discarded() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut bytes = wire_dat(0, b"hello world!");
    bytes[9] ^= 0xFF;
    peer.push(bytes);
    peer.respond(&mut arq, &mut off);
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 9).is_none());
    assert_eq!(arq.rx_len, 0, "corrupt frame must be consumed");
    assert_eq!(arq.rn, 0);
}

#[test]
fn corrupt_frame_recovered_by_retransmission() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut bytes = wire_dat(0, b"hello");
    bytes[7] ^= 0x10;
    peer.push(bytes);
    peer.push(wire_dat(1, b" world"));
    peer.push(wire_dat(0, b"hello"));
    peer.push(wire_dat(1, b" world"));
    peer.push(wire_fin(2, b"!"));
    peer.respond(&mut arq, &mut off);
    let mut got = Vec::new();
    loop {
        let mut buf = [0u8; 64];
        let mut op = Op::Read { buf: &mut buf };
        match drive_out(&mut arq, &mut op, &mut peer, &mut off, 100) {
            OpOut::Read(0) => break,
            OpOut::Read(n) => got.extend_from_slice(&buf[..n]),
            other => panic!("unexpected read result: {other:?}"),
        }
    }
    assert_eq!(got, b"hello world!");
}

#[test]
fn corrupt_frame_before_ack_is_discarded() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    let mut bad = wire_dat(5, b"eleven byte");
    bad[6] ^= 0x01;
    peer.push(bad);
    peer.push(wire_ack(1));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
}

#[test]
fn truncated_frame_at_eof_is_closed() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let bytes = wire_dat(0, b"hello world");
    peer.push(bytes[..9].to_vec());
    peer.respond(&mut arq, &mut off);
    arq.channel.eof = true;
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        drive(&mut arq, &mut op, &mut peer, &mut off, 10),
        Some(Err(ArqError::Closed))
    ));
    assert_eq!(arq.rn, 0);
}

#[test]
fn short_unparseable_prefix_waits_for_more_bytes() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(vec![0x01, 0x00, 0x00]);
    peer.respond(&mut arq, &mut off);
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    assert_eq!(
        arq.rx_len, 3,
        "bytes must be kept until the boundary is known"
    );
}

#[test]
fn invalid_type_byte_aborts() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(vec![0x01; 16]);
    peer.respond(&mut arq, &mut off);
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        drive(&mut arq, &mut op, &mut peer, &mut off, 10),
        Some(Err(ArqError::Framing(FrameError::InvalidType(1))))
    ));
}

#[test]
fn oversized_length_aborts() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut bytes = vec![0x02, 0x00, 0xFF];
    bytes.resize(16, 0);
    peer.push(bytes);
    peer.respond(&mut arq, &mut off);
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        drive(&mut arq, &mut op, &mut peer, &mut off, 10),
        Some(Err(ArqError::Framing(FrameError::TooLong(255))))
    ));
}

#[test]
fn late_frame_after_eof_reacks() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat(0, b"a"));
    peer.push(wire_fin(1, b""));
    peer.respond(&mut arq, &mut off);
    let mut got = Vec::new();
    loop {
        let mut buf = [0u8; 16];
        let mut op = Op::Read { buf: &mut buf };
        match drive_out(&mut arq, &mut op, &mut peer, &mut off, 100) {
            OpOut::Read(0) => break,
            OpOut::Read(n) => got.extend_from_slice(&buf[..n]),
            other => panic!("unexpected read result: {other:?}"),
        }
    }
    assert_eq!(got, b"a");
    let acks_before = parse_stream(&arq.channel.tx)
        .iter()
        .filter(|f| matches!(f, Frame::Ack(_)))
        .count();
    peer.push(wire_dat(0, b"a"));
    peer.respond(&mut arq, &mut off);
    let mut op = Op::Flush;
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Done
    ));
    let acks_after = parse_stream(&arq.channel.tx)
        .iter()
        .filter(|f| matches!(f, Frame::Ack(_)))
        .count();
    assert_eq!(acks_after, acks_before + 1);
    let mut buf = [0u8; 16];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 100),
        OpOut::Read(0)
    ));
}

#[test]
fn empty_op_buffers() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    let mut empty: [u8; 0] = [];
    let mut op = Op::Read { buf: &mut empty };
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 1),
        OpOut::Read(0)
    ));
    let mut op = Op::Write { buf: &[] };
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 1),
        OpOut::Write(0)
    ));
}

#[test]
fn push_read_compaction() {
    let mut arq = make_arq();
    let chunk = [0xAAu8; 250];
    while arq.push_read(&chunk) {}
    let rem = r::<4>() - arq.read_tail;
    assert!(arq.push_read(&vec![0xAA; rem]));
    assert_eq!(arq.read_tail, r::<4>());
    assert!(!arq.push_read(&[0u8; 1]));
    let mut buf = vec![0u8; 1000];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(arq.service_op(&mut op), Some(OpOut::Read(1000))));
    assert!(arq.push_read(&[0xBB; 250]));
    let mut buf = vec![0u8; 1258];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(arq.service_op(&mut op), Some(OpOut::Read(1258))));
    assert!(buf[..1008].iter().all(|&b| b == 0xAA));
    assert!(buf[1008..].iter().all(|&b| b == 0xBB));
    let mut buf = [0u8; 250];
    let mut op = Op::Read { buf: &mut buf };
    assert!(arq.service_op(&mut op).is_none());
    assert!(arq.push_read(&[0xCC; 250]));
    assert_eq!(arq.read_head, 0);
    assert_eq!(arq.read_tail, 250);
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(arq.service_op(&mut op), Some(OpOut::Read(250))));
    assert!(buf.iter().all(|&b| b == 0xCC));
}

fn sent_acks(arq: &TestArq) -> Vec<u16> {
    parse_stream(&arq.channel.tx)
        .iter()
        .filter_map(|f| match f {
            Frame::Ack(a) => Some(a.an()),
            _ => None,
        })
        .collect()
}

fn sent_data(arq: &TestArq) -> Vec<Frame> {
    parse_stream(&arq.channel.tx)
        .into_iter()
        .filter(|f| !matches!(f, Frame::Ack(_)))
        .collect()
}

fn sent_sns(arq: &TestArq) -> Vec<u16> {
    sent_data(arq)
        .iter()
        .map(|f| match f {
            Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => d.sn(),
            Frame::Ack(_) => unreachable!(),
        })
        .collect()
}

fn read_some(arq: &mut TestArq, peer: &mut Peer, off: &mut usize) -> Option<Vec<u8>> {
    let mut buf = [0u8; 64];
    let mut op = Op::Read { buf: &mut buf };
    match drive(arq, &mut op, peer, off, 10)? {
        Ok(OpOut::Read(n)) => Some(buf[..n].to_vec()),
        other => panic!("unexpected read result: {other:?}"),
    }
}

#[test]
fn ack_request_is_acked_immediately() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat_ack_req(0, b"x"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"x");
    assert_eq!(sent_acks(&arq), [1]);
}

#[test]
fn bulk_frames_still_batch_acks() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    for sn in 0..3u16 {
        peer.push(wire_dat(sn, &[b'a' + sn as u8]));
    }
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"abc");
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert!(sent_acks(&arq).is_empty(), "ordinary DATs must batch");
    peer.push(wire_dat(3, b"d"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"d");
    assert_eq!(sent_acks(&arq), [4]);
}

#[test]
fn flush_requests_ack_only_on_final_frame() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    let data: Vec<u8> = (0..600u32).map(|i| i as u8).collect();
    write_until(&mut arq, &data, &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    let frames = sent_data(&arq);
    assert!(matches!(frames[0], Frame::Dat(d) if d.sn() == 0));
    assert!(matches!(frames[1], Frame::Dat(d) if d.sn() == 1));
    assert!(matches!(frames[2], Frame::DatAckReq(d) if d.sn() == 2));
    assert_eq!(frames.len(), 3);
    peer.push(wire_ack(3));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
    assert_eq!(arq.state, State::Active);
}

#[test]
fn flush_marks_already_sent_final_frame() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert!(matches!(sent_data(&arq)[..], [Frame::Dat(d)] if d.sn() == 0));
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none());
    let frames = sent_data(&arq);
    assert_eq!(frames.len(), 2, "final frame re-sent once: {frames:?}");
    assert!(matches!(frames[1], Frame::DatAckReq(d) if d.sn() == 0 && d.payload() == b"abcd"));
    assert!(arq.sbuf.get(0).unwrap().requests_ack());
    peer.push(wire_ack(1));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
}

#[test]
fn ack_request_property_survives_retransmission() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    expire(&arq);
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    let frames = sent_data(&arq);
    assert_eq!(frames.len(), 2);
    assert!(
        frames
            .iter()
            .all(|f| matches!(f, Frame::DatAckReq(d) if d.sn() == 0))
    );
}

#[test]
fn duplicate_after_lost_ack_is_reacked() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat_ack_req(0, b"a"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"a");
    assert_eq!(sent_acks(&arq), [1]);
    peer.push(wire_dat(0, b"a"));
    peer.respond(&mut arq, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert_eq!(sent_acks(&arq), [1, 1], "duplicate must be re-ACKed");
    assert_eq!(arq.rn, 1);
    assert_eq!(arq.read_head, arq.read_tail);
    peer.push(wire_fin(1, b"b"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"b");
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"");
}

#[test]
fn duplicate_buffered_out_of_order_is_reacked() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.push(wire_dat(1, b"b"));
    peer.respond(&mut arq, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert!(sent_acks(&arq).is_empty());
    peer.push(wire_dat(1, b"b"));
    peer.respond(&mut arq, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert_eq!(sent_acks(&arq), [0]);
    assert_eq!(arq.rn, 0);
    peer.push(wire_dat(0, b"a"));
    peer.respond(&mut arq, &mut off);
    assert_eq!(read_some(&mut arq, &mut peer, &mut off).unwrap(), b"ab");
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    assert_eq!(sent_acks(&arq), [0, 2]);
    assert_eq!(arq.rn, 2);
}

#[test]
fn no_retransmit_before_deadline() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none());
    assert_eq!(sent_data(&arq).len(), 1);
    arq.timer.clock.advance(arq.rto - Duration::from_millis(1));
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none());
    assert_eq!(sent_data(&arq).len(), 1, "retransmitted before deadline");
    arq.timer.clock.advance(Duration::from_millis(1));
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 50).is_none());
    assert_eq!(sent_data(&arq).len(), 2, "no retransmit after deadline");
}

#[test]
fn retransmit_backs_off_and_ack_resets() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, b"abcd", &mut peer, &mut off);
    let mut op = Op::Flush;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    for _ in 0..6 {
        expire(&arq);
        assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10).is_none());
    }
    let ms = Duration::from_millis;
    assert_eq!(
        arq.timer.clock.starts(),
        [
            ms(250),
            ms(500),
            ms(1000),
            ms(2000),
            ms(4000),
            ms(4000),
            ms(4000)
        ]
    );
    assert_eq!(sent_data(&arq).len(), 7);
    peer.push(wire_ack(1));
    peer.respond(&mut arq, &mut off);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 10),
        OpOut::Done
    ));
    assert_eq!(arq.rto, ms(250));
    assert!(!arq.timer_running);
}

#[test]
fn delayed_peer_writes_do_not_scale_with_polls() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, &[7u8; 1000], &mut peer, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    let first = arq.channel.tx.len();
    assert_eq!(sent_sns(&arq), [0, 1, 2, 3]);
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10_000).is_none());
    assert_eq!(arq.channel.tx.len(), first, "polls alone must not send");
    expire(&arq);
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 10_000).is_none());
    assert_eq!(arq.channel.tx.len(), 2 * first, "one round per timeout");
    peer.silent = false;
    peer.seen = vec![0, 1, 2, 3];
    peer.next = 4;
    expire(&arq);
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 100).is_none());
    assert_eq!(arq.w, 0, "lost ACKs recovered via re-ACKed duplicates");
    assert!(!arq.timer_running);
}

#[test]
fn ack_during_retransmit_round_skips_acked_frames() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    write_until(&mut arq, &[7u8; 1000], &mut peer, &mut off);
    assert!(read_some(&mut arq, &mut peer, &mut off).is_none());
    expire(&arq);
    let mut cx = noop_cx();
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(arq.poll_op(&mut cx, &mut op).is_pending());
    assert_eq!(sent_sns(&arq), [0, 1, 2, 3, 0]);
    arq.channel.rx.extend(wire_ack(2));
    for _ in 0..10 {
        assert!(arq.poll_op(&mut cx, &mut op).is_pending());
    }
    assert_eq!(sent_sns(&arq), [0, 1, 2, 3, 0, 2, 3]);
    assert!(arq.timer_running);
}

#[test]
fn lost_fin_is_retransmitted() {
    let mut arq = make_arq();
    let mut peer = Peer::new();
    let mut off = 0usize;
    peer.silent = true;
    let mut op = Op::Shutdown;
    assert!(drive(&mut arq, &mut op, &mut peer, &mut off, 20).is_none());
    assert!(matches!(sent_data(&arq)[..], [Frame::Fin(d)] if d.sn() == 0));
    peer.silent = false;
    expire(&arq);
    assert!(matches!(
        drive_out(&mut arq, &mut op, &mut peer, &mut off, 20),
        OpOut::Done
    ));
    assert_eq!(sent_sns(&arq), [0, 0]);
}

struct EofLink;

impl FrameIo for EofLink {
    type Error = Infallible;
    fn poll_send(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_recv(
        &mut self,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        Poll::Ready(Ok(0))
    }
    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn channel_eof_is_closed() {
    let mut arq = new_arq(EofLink);
    let mut cx = noop_cx();
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Err(ArqError::Closed))
    ));
}

struct FailLink {
    fail_recv: bool,
    fail_send: bool,
    send_zero: bool,
}

impl FrameIo for FailLink {
    type Error = String;

    fn poll_send(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, String>> {
        if self.fail_send {
            Poll::Ready(Err("send failed".into()))
        } else if self.send_zero {
            Poll::Ready(Ok(0))
        } else {
            Poll::Ready(Ok(buf.len()))
        }
    }

    fn poll_recv(&mut self, _cx: &mut Context<'_>, _buf: &mut [u8]) -> Poll<Result<usize, String>> {
        if self.fail_recv {
            Poll::Ready(Err("recv failed".into()))
        } else {
            Poll::Pending
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), String>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn channel_recv_error_propagates() {
    let mut arq = new_arq(FailLink {
        fail_recv: true,
        fail_send: false,
        send_zero: false,
    });
    let mut cx = noop_cx();
    let mut buf = [0u8; 8];
    let mut op = Op::Read { buf: &mut buf };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Err(ArqError::Io(e))) if e == "recv failed"
    ));
}

#[test]
fn channel_send_error_propagates() {
    let mut arq = new_arq(FailLink {
        fail_recv: false,
        fail_send: true,
        send_zero: false,
    });
    let mut cx = noop_cx();
    let data = [7u8; 10];
    let mut op = Op::Write { buf: &data };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Ok(OpOut::Write(10)))
    ));
    let mut op = Op::Flush;
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Err(ArqError::Io(e))) if e == "send failed"
    ));
}

#[test]
fn channel_send_eof_is_closed() {
    let mut arq = new_arq(FailLink {
        fail_recv: false,
        fail_send: false,
        send_zero: true,
    });
    let mut cx = noop_cx();
    let data = [9u8; 10];
    let mut op = Op::Write { buf: &data };
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Ok(OpOut::Write(10)))
    ));
    let mut op = Op::Flush;
    assert!(matches!(
        arq.poll_op(&mut cx, &mut op),
        Poll::Ready(Err(ArqError::Closed))
    ));
}

struct TrickleLink {
    rx: Vec<u8>,
    tx: Vec<u8>,
}

impl TrickleLink {
    fn new() -> Self {
        Self {
            rx: Vec::new(),
            tx: Vec::new(),
        }
    }

    fn push(&mut self, bytes: Vec<u8>) {
        self.rx.extend(bytes);
    }
}

impl FrameIo for TrickleLink {
    type Error = Infallible;

    fn poll_send(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        self.tx.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_recv(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        if self.rx.is_empty() {
            Poll::Pending
        } else {
            let n = 1.min(self.rx.len()).min(buf.len());
            buf[..n].copy_from_slice(&self.rx[..n]);
            self.rx.drain(..n);
            Poll::Ready(Ok(n))
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn fragmented_channel_reads() {
    let mut link = TrickleLink::new();
    link.push(wire_dat(0, &[1u8; 10]));
    link.push(wire_dat(1, &[2u8; 10]));
    link.push(wire_fin(2, &[3u8; 5]));
    let mut arq = new_arq(link);
    let mut cx = noop_cx();
    let mut got = Vec::new();
    let mut buf = [0u8; 16];
    let mut done = false;
    for _ in 0..100_000 {
        let mut n = 0usize;
        {
            let mut op = Op::Read { buf: &mut buf };
            match arq.poll_op(&mut cx, &mut op) {
                Poll::Ready(Ok(OpOut::Read(0))) => {
                    done = true;
                    break;
                }
                Poll::Ready(Ok(OpOut::Read(k))) => n = k,
                Poll::Ready(Ok(other)) => panic!("unexpected op out: {other:?}"),
                Poll::Ready(Err(e)) => panic!("arq error: {e:?}"),
                Poll::Pending => {}
            }
        }
        got.extend_from_slice(&buf[..n]);
    }
    assert!(done, "read did not reach end of stream");
    let expected: Vec<u8> = [vec![1u8; 10], vec![2u8; 10], vec![3u8; 5]].concat();
    assert_eq!(got, expected);
}

struct PartialLink {
    rx: Vec<u8>,
    tx: Vec<u8>,
    cap: usize,
}

impl PartialLink {
    fn new(cap: usize) -> Self {
        Self {
            rx: Vec::new(),
            tx: Vec::new(),
            cap,
        }
    }
}

impl FrameIo for PartialLink {
    type Error = Infallible;

    fn poll_send(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Infallible>> {
        let space = self.cap - self.tx.len();
        if space == 0 {
            return Poll::Pending;
        }
        let n = space.min(buf.len());
        self.tx.extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_recv(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        if self.rx.is_empty() {
            Poll::Pending
        } else {
            let n = self.rx.len().min(buf.len());
            buf[..n].copy_from_slice(&self.rx[..n]);
            self.rx.drain(..n);
            Poll::Ready(Ok(n))
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
}

struct PartialPeer {
    rest: Vec<u8>,
    next_rx: u16,
    next_tx: u16,
    to_us: Vec<u8>,
}

impl PartialPeer {
    fn new() -> Self {
        Self {
            rest: Vec::new(),
            next_rx: 0,
            next_tx: 0,
            to_us: Vec::new(),
        }
    }

    fn poll(&mut self, wire: &mut Vec<u8>) {
        self.rest.extend(core::mem::take(wire));
        while let Ok(len) = Frame::wire_len::<BchAckCodec, 16, _>(&crc16(), &self.rest) {
            if self.rest.len() < len {
                break;
            }
            let frame = Frame::from_bytes::<BchAckCodec, 16, _>(&crc16(), &self.rest[..len])
                .expect("peer frame decode");
            self.rest.drain(..len);
            if let Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) = frame {
                if d.sn() == self.next_rx {
                    self.to_us.extend(wire_ack(self.next_rx.wrapping_add(1)));
                    self.to_us.extend(wire_dat(self.next_tx, d.payload()));
                    self.next_tx = self.next_tx.wrapping_add(1);
                    self.next_rx = d.sn().wrapping_add(1);
                }
            }
        }
    }

    fn drain(&mut self) -> Vec<u8> {
        core::mem::take(&mut self.to_us)
    }
}

#[test]
fn partial_send_resumes_across_polls() {
    let mut arq = new_arq(PartialLink::new(16));
    let mut peer = PartialPeer::new();
    let mut cx = noop_cx();
    let data: Vec<u8> = (0..600u32).map(|i| (i % 251) as u8).collect();
    let mut off = 0usize;
    for _ in 0..10_000 {
        if off == data.len() {
            break;
        }
        let mut op = Op::Write { buf: &data[off..] };
        match arq.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Write(n))) => off += n,
            Poll::Ready(Ok(other)) => panic!("unexpected op out: {other:?}"),
            Poll::Ready(Err(e)) => panic!("arq error: {e:?}"),
            Poll::Pending => {
                peer.poll(&mut arq.channel.tx);
                arq.channel.rx.extend(peer.drain());
            }
        }
    }
    assert_eq!(off, data.len(), "write did not complete");
    let mut op = Op::Flush;
    let mut flushed = false;
    for _ in 0..10_000 {
        match arq.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Done)) => {
                flushed = true;
                break;
            }
            Poll::Ready(Ok(other)) => panic!("unexpected op out: {other:?}"),
            Poll::Ready(Err(e)) => panic!("arq error: {e:?}"),
            Poll::Pending => {
                peer.poll(&mut arq.channel.tx);
                arq.channel.rx.extend(peer.drain());
            }
        }
    }
    assert!(flushed, "flush did not complete");
    let mut got = Vec::new();
    for _ in 0..10_000 {
        if got.len() == data.len() {
            break;
        }
        let mut buf = [0u8; 64];
        let mut op = Op::Read { buf: &mut buf };
        match arq.poll_op(&mut cx, &mut op) {
            Poll::Ready(Ok(OpOut::Read(n))) => got.extend_from_slice(&buf[..n]),
            Poll::Ready(Ok(other)) => panic!("unexpected op out: {other:?}"),
            Poll::Ready(Err(e)) => panic!("arq error: {e:?}"),
            Poll::Pending => {}
        }
    }
    assert_eq!(got, data);
}

#[test]
#[cfg(feature = "tokio")]
fn tokio_duplex_async_read_write() {
    use core::pin::Pin;
    use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
    type TokArq = Arq<4, 16, { r::<4>() }, DuplexStream, Crc16X25, BchAckCodec, ManualTimer>;
    #[allow(clippy::too_many_arguments)]
    fn step(
        arq: &mut TokArq,
        cx: &mut Context<'_>,
        phase: &mut u8,
        off: &mut usize,
        data: &[u8],
        got: &mut Vec<u8>,
        buf: &mut [u8],
        label: &'static str,
    ) {
        match *phase {
            0 => match AsyncWrite::poll_write(Pin::new(arq), cx, &data[*off..]) {
                Poll::Pending => {}
                Poll::Ready(Ok(n)) => {
                    *off += n;
                    if *off == data.len() {
                        *phase = 1;
                    }
                }
                Poll::Ready(Err(e)) => panic!("{label} write: {e}"),
            },
            1 => match AsyncWrite::poll_flush(Pin::new(arq), cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(())) => *phase = 2,
                Poll::Ready(Err(e)) => panic!("{label} flush: {e}"),
            },
            2 => match AsyncWrite::poll_shutdown(Pin::new(arq), cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(())) => *phase = 3,
                Poll::Ready(Err(e)) => panic!("{label} shutdown: {e}"),
            },
            3 => {
                let mut rb = ReadBuf::new(buf);
                match AsyncRead::poll_read(Pin::new(arq), cx, &mut rb) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(())) => {
                        if rb.filled().is_empty() {
                            *phase = 4;
                        } else {
                            got.extend_from_slice(rb.filled());
                        }
                    }
                    Poll::Ready(Err(e)) => panic!("{label} read: {e}"),
                }
            }
            _ => {}
        }
    }
    let (a_ch, b_ch) = tokio::io::duplex(64);
    let mut a: TokArq = new_arq(a_ch);
    let mut b: TokArq = new_arq(b_ch);
    let mut cx = noop_cx();
    let da: Vec<u8> = (0..2000u32).map(|i| (i % 251) as u8).collect();
    let db: Vec<u8> = (0..2000u32).map(|i| (i * 7 % 251) as u8).collect();
    let mut a_phase = 0u8;
    let mut b_phase = 0u8;
    let mut a_off = 0usize;
    let mut b_off = 0usize;
    let mut a_got = Vec::new();
    let mut b_got = Vec::new();
    let mut a_buf = [0u8; 64];
    let mut b_buf = [0u8; 64];
    for steps in 0..200_000 {
        if a_phase < 4 {
            step(
                &mut a,
                &mut cx,
                &mut a_phase,
                &mut a_off,
                &da,
                &mut a_got,
                &mut a_buf,
                "a",
            );
        }
        if b_phase < 4 {
            step(
                &mut b,
                &mut cx,
                &mut b_phase,
                &mut b_off,
                &db,
                &mut b_got,
                &mut b_buf,
                "b",
            );
        }
        if a_phase == 4 && b_phase == 4 {
            break;
        }
        if steps == 199_999 {
            panic!("no convergence after {steps} steps (a={a_phase}, b={b_phase})");
        }
    }
    assert_eq!(a_got, db);
    assert_eq!(b_got, da);
}
