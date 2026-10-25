//! A bidirectional PTY relay between a client port and a server port.
//!
//! ```text
//! client LinuxUart <-pty-> relay <-pty-> server LinuxUart
//! ```
//!
//! The relay owns the master side of both pseudo-terminals and keeps both
//! alive, even after one peer has hung up, until [`Relay::release`] is called.
//! Dropping the client's port therefore does not hang up the server by itself:
//! the test decides when the line goes away.
//!
//! In [`Mode::Bytes`] the relay copies bytes unchanged. In
//! [`Mode::ReliableFault`] it also splits both directions into zero-delimited
//! COBS frames and inspects the ARQ frames in them, only as far as one
//! fault-injection test needs:
//!
//! 1. It rebuilds the server's byte stream from the in-order DAT and FIN
//!    payloads and finds the HTTP/2 frame that completes the third client
//!    request stream (the `EventSubscribe` call): the trailing `HEADERS` frame
//!    with `END_STREAM`. It rebuilds the client's stream the same way, to find
//!    the request streams.
//! 2. The DAT sequence number that carries the last byte of that frame is the
//!    target.
//! 3. It drops the first client ACK that covers the target, and records the
//!    retransmissions of the target and the ACKs that follow.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arq_io_async::{AckCodec, BchAckCodec};
use protolink_adaptors::uart::linux::LinuxUart;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_serial::SerialStream;

const PREFACE_LEN: usize = 24;
const END_STREAM: u8 = 0x1;
const TYPE_DATA: u8 = 0;
const TYPE_HEADERS: u8 = 1;
const FRAME_ACK: u8 = 0b01;
const SEQ_MASK: u16 = (1 << 14) - 1;

#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// Copy every byte unchanged.
    Bytes,
    /// Inspect ARQ frames and lose the client's ACK of the final response.
    ReliableFault {
        /// Hold the replacement ACK back until this long after the request of
        /// the final call, if set.
        hold_replacement_until: Option<Duration>,
    },
}

/// What the relay has seen. Read it through [`Relay::observed`].
#[derive(Debug, Default, Clone)]
pub struct Observed {
    /// Sequence number of every ARQ data frame from the server, in arrival order.
    pub server_data: Vec<u16>,
    /// The DAT that completes the final call's response stream.
    pub target: Option<u16>,
    /// `an` of every ACK from the client, in arrival order.
    pub client_acks: Vec<u16>,
    /// The `an` of the ACK that was dropped.
    pub dropped_acks: Vec<u16>,
    /// ACKs that covered the target and were forwarded after the drop.
    pub replacement_acks: Vec<u16>,
    /// Request streams opened by the client, in order.
    pub request_streams: Vec<u32>,
    /// When the final call's request reached the relay.
    pub final_request_at: Option<Instant>,
    /// When the replacement ACK was forwarded.
    pub replacement_forwarded_at: Option<Instant>,
    pub client_hung_up: bool,
    pub server_hung_up: bool,
}

impl Observed {
    /// How often the target was sent beyond the first time.
    pub fn target_retransmissions(&self) -> usize {
        let Some(target) = self.target else { return 0 };
        let sent = self.server_data.iter().filter(|s| **s == target).count();
        sent.saturating_sub(1)
    }
}

pub struct Relay {
    state: Arc<Mutex<Observed>>,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Relay {
    /// Create the two lines. Returns the relay, the client's port and the
    /// server's port.
    pub fn start(mode: Mode) -> (Self, LinuxUart, LinuxUart) {
        let (client_master, client_slave) = SerialStream::pair().unwrap();
        let (server_master, server_slave) = SerialStream::pair().unwrap();
        let state = Arc::new(Mutex::new(Observed::default()));
        let (stop, stop_rx) = watch::channel(false);

        let (client_rd, client_wr) = tokio::io::split(client_master);
        let (server_rd, server_wr) = tokio::io::split(server_master);

        let inspector = Arc::new(Mutex::new(Inspector::new(mode)));
        let c2s = tokio::spawn(pump(
            Direction::ClientToServer,
            client_rd,
            server_wr,
            stop_rx.clone(),
            state.clone(),
            inspector.clone(),
        ));
        let s2c = tokio::spawn(pump(
            Direction::ServerToClient,
            server_rd,
            client_wr,
            stop_rx,
            state.clone(),
            inspector,
        ));
        let relay = Self {
            state,
            stop,
            tasks: vec![c2s, s2c],
        };
        (
            relay,
            LinuxUart::from_serial(client_slave),
            LinuxUart::from_serial(server_slave),
        )
    }

    pub fn observed(&self) -> Observed {
        self.state.lock().unwrap().clone()
    }

    /// Stop the relay and close both masters, which hangs up both peers.
    pub async fn release(mut self) {
        let _ = self.stop.send(true);
        for task in std::mem::take(&mut self.tasks) {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("the relay stops when released")
                .unwrap();
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    ClientToServer,
    ServerToClient,
}

enum Verdict {
    Forward,
    Drop,
    ForwardAt(Instant),
}

async fn pump(
    direction: Direction,
    mut rd: ReadHalf<SerialStream>,
    mut wr: WriteHalf<SerialStream>,
    mut stop: watch::Receiver<bool>,
    state: Arc<Mutex<Observed>>,
    inspector: Arc<Mutex<Inspector>>,
) {
    let mut chunk = [0u8; 1024];
    let mut pending: Vec<u8> = Vec::new();
    let mut hung_up = false;
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            read = rd.read(&mut chunk), if !hung_up => {
                let n = match read {
                    Ok(0) | Err(_) => {
                        // Keep both ends alive: only the test releases them.
                        hung_up = true;
                        let mut observed = state.lock().unwrap();
                        match direction {
                            Direction::ClientToServer => observed.client_hung_up = true,
                            Direction::ServerToClient => observed.server_hung_up = true,
                        }
                        continue;
                    }
                    Ok(n) => n,
                };
                let mut out = Vec::new();
                let mut hold = None;
                {
                    let mut inspector = inspector.lock().unwrap();
                    let mut observed = state.lock().unwrap();
                    if !inspector.frames() {
                        out.extend_from_slice(&chunk[..n]);
                    } else {
                        pending.extend_from_slice(&chunk[..n]);
                        while let Some(end) = pending.iter().position(|b| *b == 0) {
                            let frame: Vec<u8> = pending.drain(..=end).collect();
                            let body = &frame[..frame.len() - 1];
                            if body.is_empty() {
                                continue;
                            }
                            match inspector.frame(direction, body, &mut observed) {
                                Verdict::Drop => {}
                                Verdict::Forward => {
                                    out.push(0);
                                    out.extend_from_slice(&frame);
                                }
                                Verdict::ForwardAt(at) => {
                                    hold = Some(at);
                                    out.push(0);
                                    out.extend_from_slice(&frame);
                                }
                            }
                        }
                    }
                }
                if let Some(at) = hold {
                    tokio::time::sleep_until(at).await;
                    state.lock().unwrap().replacement_forwarded_at = Some(Instant::now());
                }
                if !out.is_empty() {
                    // The peer may have gone: a failed write is dropped.
                    let _ = wr.write_all(&out).await;
                }
            }
        }
    }
}

/// An HTTP/2 byte stream cut into frames.
struct H2 {
    skip: usize,
    buf: Vec<u8>,
}

struct H2Frame {
    stream: u32,
    ty: u8,
    flags: u8,
}

impl H2 {
    fn new(preface: bool) -> Self {
        Self {
            skip: if preface { PREFACE_LEN } else { 0 },
            buf: Vec::new(),
        }
    }

    fn push(&mut self, mut bytes: &[u8]) -> Vec<H2Frame> {
        let skipped = self.skip.min(bytes.len());
        self.skip -= skipped;
        bytes = &bytes[skipped..];
        self.buf.extend_from_slice(bytes);

        let mut frames = Vec::new();
        loop {
            if self.buf.len() < 9 {
                break;
            }
            let len = u32::from_be_bytes([0, self.buf[0], self.buf[1], self.buf[2]]) as usize;
            if self.buf.len() < 9 + len {
                break;
            }
            frames.push(H2Frame {
                ty: self.buf[3],
                flags: self.buf[4],
                stream: u32::from_be_bytes(self.buf[5..9].try_into().unwrap()) & 0x7fff_ffff,
            });
            self.buf.drain(..9 + len);
        }
        frames
    }
}

/// Whether `seq` is the next sequence number of its direction. If it is, the
/// expectation advances. A retransmission, or a DAT resent with an ACK request,
/// repeats a sequence number that has been taken already, so it is not stream
/// data again. FIN payload is stream data too: the receiver delivers it.
fn take_in_order(expected: &mut u16, seq: u16) -> bool {
    if seq != *expected {
        return false;
    }
    *expected = (*expected + 1) & SEQ_MASK;
    true
}

struct Inspector {
    mode: Mode,
    crc: crc::Crc<u16>,
    client: H2,
    server: H2,
    /// The next client sequence number, from 0 as the client's ARQ starts.
    client_next: u16,
    /// The next server sequence number, from 0 as the server's ARQ starts.
    server_next: u16,
}

impl Inspector {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            crc: crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC),
            client: H2::new(true),
            server: H2::new(false),
            client_next: 0,
            server_next: 0,
        }
    }

    fn frames(&self) -> bool {
        matches!(self.mode, Mode::ReliableFault { .. })
    }

    /// Decide what to do with one decoded-COBS-frame `body` (without delimiters).
    /// Only inspection depends on the decisions here: the frame is forwarded
    /// unchanged unless it is an ACK the test drops.
    fn frame(&mut self, direction: Direction, body: &[u8], observed: &mut Observed) -> Verdict {
        let mut decoded = [0u8; 256];
        let mut framed = vec![0u8];
        framed.extend_from_slice(body);
        framed.push(0);
        let Ok(frame) = cobs_io_async::sync::decode_to_slice(&framed, &mut decoded) else {
            return Verdict::Forward;
        };
        let bytes = &decoded[..frame.len];
        let Some(first) = bytes.first() else {
            return Verdict::Forward;
        };

        match (direction, first & 0b11) {
            (Direction::ClientToServer, FRAME_ACK) => self.client_ack(bytes, observed),
            (Direction::ClientToServer, _) => {
                self.client_data(bytes, observed);
                Verdict::Forward
            }
            (Direction::ServerToClient, FRAME_ACK) => Verdict::Forward,
            (Direction::ServerToClient, _) => {
                self.server_data(bytes, observed);
                Verdict::Forward
            }
        }
    }

    /// Payload of a DAT, DAT with ACK request or FIN: `[pkt_id:2][len:1][crc:2][payload]`.
    fn payload(bytes: &[u8]) -> Option<(u16, &[u8])> {
        if bytes.len() < 5 {
            return None;
        }
        let pkt_id = u16::from_le_bytes([bytes[0], bytes[1]]);
        let len = bytes[2] as usize;
        (bytes.len() == 5 + len).then(|| (pkt_id >> 2, &bytes[5..]))
    }

    fn client_data(&mut self, bytes: &[u8], observed: &mut Observed) {
        let Some((seq, payload)) = Self::payload(bytes) else {
            return;
        };
        if !take_in_order(&mut self.client_next, seq) {
            return;
        }
        for frame in self.client.push(payload) {
            if frame.ty == TYPE_HEADERS && !observed.request_streams.contains(&frame.stream) {
                observed.request_streams.push(frame.stream);
                if observed.request_streams.len() == 3 {
                    observed.final_request_at = Some(Instant::now());
                }
            }
        }
    }

    fn server_data(&mut self, bytes: &[u8], observed: &mut Observed) {
        let Some((seq, payload)) = Self::payload(bytes) else {
            return;
        };
        observed.server_data.push(seq);
        if !take_in_order(&mut self.server_next, seq) {
            return;
        }

        let final_stream = observed.request_streams.get(2).copied();
        for frame in self.server.push(payload) {
            let completes = Some(frame.stream) == final_stream
                && matches!(frame.ty, TYPE_HEADERS | TYPE_DATA)
                && frame.flags & END_STREAM != 0;
            if completes && observed.target.is_none() {
                observed.target = Some(seq);
            }
        }
    }

    fn client_ack(&mut self, bytes: &[u8], observed: &mut Observed) -> Verdict {
        let Ok(codeword) = <&[u8; 16]>::try_from(&bytes[1..]) else {
            return Verdict::Forward;
        };
        let Ok(ack) = <BchAckCodec as AckCodec<16>>::decode_ack(&self.crc, codeword) else {
            return Verdict::Forward;
        };
        let an = ack.an();
        observed.client_acks.push(an);

        let Some(target) = observed.target else {
            return Verdict::Forward;
        };
        // `an` is the next sequence number expected: it covers `target` when
        // it is past it. The tests stay far from the 14-bit wrap.
        if an <= target {
            return Verdict::Forward;
        }
        if observed.dropped_acks.is_empty() {
            observed.dropped_acks.push(an);
            return Verdict::Drop;
        }
        observed.replacement_acks.push(an);
        let Mode::ReliableFault {
            hold_replacement_until: Some(hold),
        } = self.mode
        else {
            observed.replacement_forwarded_at = Some(Instant::now());
            return Verdict::Forward;
        };
        match observed.final_request_at {
            Some(at) if Instant::now() < at + hold => Verdict::ForwardAt(at + hold),
            _ => {
                observed.replacement_forwarded_at = Some(Instant::now());
                Verdict::Forward
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAT_ACK_REQ: u8 = 0b00;
    const DAT: u8 = 0b10;
    const FIN: u8 = 0b11;
    const END_HEADERS: u8 = 0x4;

    fn inspector() -> Inspector {
        Inspector::new(Mode::ReliableFault {
            hold_replacement_until: None,
        })
    }

    /// An ARQ frame as a sender builds it, before COBS. The inspector does not
    /// check the CRC, so it is left zero.
    fn arq(ty: u8, seq: u16, payload: &[u8]) -> Vec<u8> {
        let pkt_id = (seq << 2) | u16::from(ty);
        let mut frame = pkt_id.to_le_bytes().to_vec();
        frame.extend_from_slice(&[payload.len() as u8, 0, 0]);
        frame.extend_from_slice(payload);
        frame
    }

    fn h2(ty: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let mut frame = (payload.len() as u32).to_be_bytes()[1..].to_vec();
        frame.extend_from_slice(&[ty, flags]);
        frame.extend_from_slice(&stream.to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// The client's HTTP/2 bytes: the preface, then a request `HEADERS` frame
    /// for each stream.
    fn requests(streams: &[u32]) -> Vec<u8> {
        let mut bytes = vec![0; PREFACE_LEN];
        for &stream in streams {
            bytes.extend(h2(TYPE_HEADERS, END_HEADERS, stream, b"request"));
        }
        bytes
    }

    /// Hand one ARQ frame to the inspector as the relay reads it: as an
    /// undelimited COBS body. The frame must be forwarded.
    fn receive(
        inspector: &mut Inspector,
        direction: Direction,
        observed: &mut Observed,
        frame: &[u8],
    ) {
        let mut body = [0u8; 512];
        let len = cobs_io_async::sync::encode_from_slice(frame, &mut body).unwrap();
        assert!(matches!(
            inspector.frame(direction, &body[..len], observed),
            Verdict::Forward
        ));
    }

    /// The HTTP/2 stream was inspected exactly once: nothing is left over, and
    /// the parser is on a frame boundary.
    fn assert_client_consumed(inspector: &Inspector) {
        assert_eq!(inspector.client.skip, 0);
        assert!(
            inspector.client.buf.is_empty(),
            "left over: {:?}",
            inspector.client.buf
        );
    }

    /// A client resends a DAT with an ACK request, as a flush does: the same
    /// sequence number and payload arrive twice, and the next fresh sequence
    /// resumes the stream.
    #[test]
    fn a_resent_client_dat_is_not_inspected_again() {
        let mut inspector = inspector();
        let mut observed = Observed::default();
        let stream = requests(&[1, 3, 5]);
        for (seq, chunk) in stream.chunks(10).enumerate() {
            let seq = seq as u16;
            receive(
                &mut inspector,
                Direction::ClientToServer,
                &mut observed,
                &arq(DAT, seq, chunk),
            );
            if seq == 2 {
                receive(
                    &mut inspector,
                    Direction::ClientToServer,
                    &mut observed,
                    &arq(DAT_ACK_REQ, seq, chunk),
                );
            }
        }
        assert_eq!(observed.request_streams, vec![1, 3, 5]);
        assert_client_consumed(&inspector);
    }

    /// Sequence numbers wrap at `SEQ_MASK`, and a resend just before the wrap
    /// is still recognised.
    #[test]
    fn client_sequence_wraps_past_a_resent_dat() {
        let mut inspector = inspector();
        inspector.client_next = SEQ_MASK - 1;
        let mut observed = Observed::default();
        let stream = requests(&[1, 3, 5]);
        for (i, chunk) in stream.chunks(10).enumerate() {
            let seq = (SEQ_MASK - 1 + i as u16) & SEQ_MASK;
            receive(
                &mut inspector,
                Direction::ClientToServer,
                &mut observed,
                &arq(DAT, seq, chunk),
            );
            if i == 1 {
                receive(
                    &mut inspector,
                    Direction::ClientToServer,
                    &mut observed,
                    &arq(DAT_ACK_REQ, seq, chunk),
                );
            }
        }
        assert_eq!(observed.request_streams, vec![1, 3, 5]);
        assert_eq!(inspector.client_next, 6, "the sequence wrapped to 6");
        assert_client_consumed(&inspector);
    }

    /// FIN payload is stream data, and a resent FIN is not inspected again.
    #[test]
    fn fin_payload_is_stream_data_and_a_resent_fin_is_ignored() {
        let mut inspector = inspector();
        let mut observed = Observed::default();
        let stream = requests(&[1, 3, 5]);
        let (head, tail) = stream.split_at(50);
        receive(
            &mut inspector,
            Direction::ClientToServer,
            &mut observed,
            &arq(DAT, 0, head),
        );
        receive(
            &mut inspector,
            Direction::ClientToServer,
            &mut observed,
            &arq(FIN, 1, tail),
        );
        receive(
            &mut inspector,
            Direction::ClientToServer,
            &mut observed,
            &arq(FIN, 1, tail),
        );
        assert_eq!(observed.request_streams, vec![1, 3, 5]);
        assert_client_consumed(&inspector);
    }

    /// The server's completing frame is found once, and its resend does not
    /// change the target or inspect the bytes again.
    #[test]
    fn a_resent_completing_server_dat_is_inspected_once() {
        let mut inspector = inspector();
        let mut observed = Observed {
            request_streams: vec![1, 3, 5],
            ..Observed::default()
        };
        let response = [
            h2(TYPE_HEADERS, END_HEADERS, 1, b"ok"),
            h2(TYPE_DATA, 0, 5, b"partial"),
            h2(TYPE_DATA, END_STREAM, 5, b"done"),
        ];
        for (seq, payload) in response.iter().enumerate() {
            let seq = seq as u16;
            receive(
                &mut inspector,
                Direction::ServerToClient,
                &mut observed,
                &arq(DAT, seq, payload),
            );
        }
        receive(
            &mut inspector,
            Direction::ServerToClient,
            &mut observed,
            &arq(DAT_ACK_REQ, 2, &response[2]),
        );
        assert_eq!(observed.target, Some(2));
        assert_eq!(observed.server_data, vec![0, 1, 2, 2]);
        assert_eq!(observed.target_retransmissions(), 1);
        assert!(inspector.server.buf.is_empty(), "left over");
    }
}
