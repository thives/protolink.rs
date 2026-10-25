//! Receive-side frame parsing, one piece of input at a time.
//!
//! [`Connection::recv_step`](super::Connection::recv_step) consumes the caller's
//! input without copying it. The frame header and fixed-size fields are collected
//! in small scratch arrays, DATA bodies are lent back as borrowed events while
//! they arrive, and an HPACK fragment is decoded as it arrives. Nothing here keeps
//! a reference to the input between calls. The frame state has a fixed size. A
//! header block keeps its decoded fields, never its encoded octets.

use alloc::borrow::Cow;
use core::ops::Bound::{Excluded, Unbounded};

use zerodds_http2::{
    CLIENT_PREFACE, ErrorCode, Flags, FrameType, Http2Error, Setting, SettingId, Settings, StreamId,
};

use super::headers::Phase;
use super::hpack;
use super::{Connection, Error, Event, FRAME_HEADER_LEN, FlowControl, MAX_WINDOW, PendingBlock};
use zerodds_http2::stream::StreamEvent;

/// Which fields of a frame the parser is consuming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    /// The Pad Length octet of a padded DATA or HEADERS frame.
    PadLength,
    /// The five stream dependency and weight octets of HEADERS, skipped.
    Priority { remaining: usize },
    /// DATA body octets, delivered or discarded according to [`Frame::data`].
    Body { remaining: usize },
    /// HPACK fragment octets of HEADERS or CONTINUATION.
    Fragment { remaining: usize },
    /// A fixed-size field copied into [`Frame::scratch`].
    Capture { want: usize },
    /// Octets nobody needs: trailing padding, unknown payloads, control debris.
    Skip,
}

/// What became of the body of a DATA frame once its stream was checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DataMode {
    /// Body octets are lent to the application, which owns the credit they hold.
    Deliver,
    /// Body octets are consumed and dropped. `publish` applies the frame's
    /// END_STREAM when it completes (the stream is only waiting to flush its
    /// reset). `tail` is the connection credit still owed for the body octets
    /// that had not been delivered when manual flow control switched the frame
    /// to discard: it is snapshotted once and returned once at frame completion,
    /// so a cancelled tail arriving in small pieces cannot multiply WINDOW_UPDATE
    /// output. Automatic mode and frames already credited at admission record
    /// zero.
    Discard { publish: bool, tail: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Data,
    Headers,
    Continuation,
    Priority,
    RstStream,
    Settings,
    Ping,
    GoAway,
    WindowUpdate,
    Skip,
}

/// SETTINGS being staged: applied to the connection only when the frame is
/// complete, so a frame that fails part way publishes nothing.
#[derive(Debug)]
struct Staged {
    candidate: Settings,
    /// `INITIAL_WINDOW_SIZE` before this frame.
    base_window: i64,
    /// Largest growth of `INITIAL_WINDOW_SIZE` over any prefix of the frame.
    peak_growth: i64,
}

impl Staged {
    fn stage(&mut self, setting: Setting) -> Result<(), Http2Error> {
        self.candidate.apply(setting)?;
        let growth = i64::from(self.candidate.initial_window_size) - self.base_window;
        self.peak_growth = self.peak_growth.max(growth);
        Ok(())
    }
}

/// The payload of the frame being received.
#[derive(Debug)]
pub(super) struct Frame {
    kind: Kind,
    flags: Flags,
    id: StreamId,
    /// Declared payload length.
    total: usize,
    /// Payload octets not yet consumed.
    left: usize,
    /// Trailing padding octets, known once the Pad Length octet has been read.
    pad: usize,
    part: Part,
    data: DataMode,
    /// Captured fixed-size field: at most eight octets.
    scratch: [u8; 8],
    filled: usize,
    staged: Option<Staged>,
}

impl Frame {
    fn new(kind: Kind, flags: Flags, id: StreamId, total: usize, part: Part) -> Self {
        Self {
            kind,
            flags,
            id,
            total,
            left: total,
            pad: 0,
            part,
            data: DataMode::Deliver,
            scratch: [0; 8],
            filled: 0,
            staged: None,
        }
    }
}

impl Connection {
    /// Consume `input` from `*pos` until an event is ready or the input is used
    /// up. `Ok(None)` means the input is used up and no event is pending.
    pub(super) fn parse<'a>(
        &mut self,
        input: &'a [u8],
        pos: &mut usize,
    ) -> Result<Option<Event<'a>>, Error> {
        loop {
            if self.awaiting_preface {
                let want = &CLIENT_PREFACE[self.preface_matched..];
                let n = want.len().min(input.len() - *pos);
                if input[*pos..*pos + n] != want[..n] {
                    return Err(self.fail(ErrorCode::ProtocolError, "invalid connection preface"));
                }
                self.preface_matched += n;
                *pos += n;
                if self.preface_matched < CLIENT_PREFACE.len() {
                    return Ok(None);
                }
                self.awaiting_preface = false;
            } else if self.frame.is_some() {
                if let Some(event) = self.payload(input, pos)? {
                    return Ok(Some(event));
                }
                if self.frame.is_some() {
                    return Ok(None);
                }
            } else {
                if *pos == input.len() {
                    return Ok(None);
                }
                let n = (FRAME_HEADER_LEN - self.header_len).min(input.len() - *pos);
                self.header[self.header_len..self.header_len + n]
                    .copy_from_slice(&input[*pos..*pos + n]);
                self.header_len += n;
                *pos += n;
                if self.header_len < FRAME_HEADER_LEN {
                    return Ok(None);
                }
                self.header_len = 0;
                let frame = self.begin()?;
                self.frame = Some(frame);
            }
            if let Some(event) = self.notices.pop_front() {
                return Ok(Some(event));
            }
            self.check_failed()?;
        }
    }

    /// Validate a complete frame header and decide how its payload is handled.
    /// Everything that depends only on the header is checked here, before any
    /// payload octet is consumed.
    fn begin(&mut self) -> Result<Frame, Error> {
        let raw = self.header;
        let len = (usize::from(raw[0]) << 16) | (usize::from(raw[1]) << 8) | usize::from(raw[2]);
        if len > self.local_max_frame_size() as usize {
            return Err(self.fail(
                ErrorCode::FrameSizeError,
                "frame exceeds SETTINGS_MAX_FRAME_SIZE",
            ));
        }
        let flags = Flags(raw[4]);
        let id = u32::from_be_bytes([raw[5] & 0x7f, raw[6], raw[7], raw[8]]);
        // RFC 9113 §3.4: the peer's first frame must be a non-ACK SETTINGS, so
        // this precedes the unknown-frame early return.
        if self.awaiting_peer_settings
            && (raw[3] != FrameType::Settings as u8 || flags.has(Flags::ACK))
        {
            return Err(self.fail(ErrorCode::ProtocolError, "first frame must be SETTINGS"));
        }
        let Ok(frame_type) = FrameType::from_u8(raw[3]) else {
            // RFC 9113 §4.1: unknown frame types MUST be ignored, except in the
            // middle of a header block.
            if self.continuation.is_some() {
                return Err(self.fail(ErrorCode::ProtocolError, "expected CONTINUATION"));
            }
            return Ok(Frame::new(Kind::Skip, flags, id, len, Part::Skip));
        };
        self.awaiting_peer_settings = false;
        if self.continuation.is_some() && frame_type != FrameType::Continuation {
            return Err(self.fail(ErrorCode::ProtocolError, "expected CONTINUATION"));
        }
        match frame_type {
            FrameType::Data => self.begin_data(flags, id, len),
            FrameType::Headers => self.begin_headers(flags, id, len),
            FrameType::Continuation => self.begin_continuation(flags, id, len),
            FrameType::Priority => {
                if id == 0 {
                    return Err(self.fail(ErrorCode::ProtocolError, "PRIORITY on stream 0"));
                }
                if len != 5 {
                    self.reset(id, ErrorCode::FrameSizeError);
                }
                Ok(Frame::new(Kind::Priority, flags, id, len, Part::Skip))
            }
            FrameType::RstStream => {
                if id == 0 {
                    return Err(self.fail(ErrorCode::ProtocolError, "RST_STREAM on stream 0"));
                }
                if len != 4 {
                    return Err(self.fail(ErrorCode::FrameSizeError, "RST_STREAM length"));
                }
                if self.is_idle_stream(id) {
                    return Err(self.fail(ErrorCode::ProtocolError, "RST_STREAM on idle stream"));
                }
                Ok(Frame::new(
                    Kind::RstStream,
                    flags,
                    id,
                    len,
                    Part::Capture { want: 4 },
                ))
            }
            FrameType::Settings => self.begin_settings(flags, id, len),
            FrameType::PushPromise => {
                Err(self.fail(ErrorCode::ProtocolError, "PUSH_PROMISE not supported"))
            }
            FrameType::Ping => {
                if id != 0 {
                    return Err(self.fail(ErrorCode::ProtocolError, "PING on non-zero stream"));
                }
                if len != 8 {
                    return Err(self.fail(ErrorCode::FrameSizeError, "PING length"));
                }
                let part = if flags.has(Flags::ACK) {
                    Part::Skip
                } else {
                    Part::Capture { want: 8 }
                };
                Ok(Frame::new(Kind::Ping, flags, id, len, part))
            }
            FrameType::GoAway => {
                if id != 0 {
                    return Err(self.fail(ErrorCode::ProtocolError, "GOAWAY on non-zero stream"));
                }
                if len < 8 {
                    return Err(self.fail(ErrorCode::FrameSizeError, "GOAWAY length"));
                }
                Ok(Frame::new(
                    Kind::GoAway,
                    flags,
                    id,
                    len,
                    Part::Capture { want: 8 },
                ))
            }
            FrameType::WindowUpdate => {
                if len != 4 {
                    return Err(self.fail(ErrorCode::FrameSizeError, "WINDOW_UPDATE length"));
                }
                if id != 0 && self.is_idle_stream(id) {
                    return Err(self.fail(ErrorCode::ProtocolError, "WINDOW_UPDATE on idle stream"));
                }
                Ok(Frame::new(
                    Kind::WindowUpdate,
                    flags,
                    id,
                    len,
                    Part::Capture { want: 4 },
                ))
            }
        }
    }

    fn begin_data(&mut self, flags: Flags, id: StreamId, len: usize) -> Result<Frame, Error> {
        if id == 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "DATA on stream 0"));
        }
        // Both flow-control modes enforce the same windows; only credit timing
        // differs. The whole declared payload is charged once, here.
        if len as i64 > self.conn_recv_window {
            return Err(self.fail(
                ErrorCode::FlowControlError,
                "connection receive window exceeded",
            ));
        }
        self.conn_recv_window -= len as i64;
        let mut frame = Frame::new(Kind::Data, flags, id, len, Part::Body { remaining: len });
        if flags.has(Flags::PADDED) {
            if len == 0 {
                return Err(self.fail(ErrorCode::ProtocolError, "invalid DATA padding"));
            }
            frame.part = Part::PadLength;
        } else {
            self.admit_data(&mut frame)?;
        }
        Ok(frame)
    }

    fn begin_headers(&mut self, flags: Flags, id: StreamId, len: usize) -> Result<Frame, Error> {
        if id == 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "HEADERS on stream 0"));
        }
        let mut frame = Frame::new(
            Kind::Headers,
            flags,
            id,
            len,
            Part::Fragment { remaining: 0 },
        );
        if flags.has(Flags::PADDED) {
            if len == 0 {
                return Err(self.fail(ErrorCode::ProtocolError, "invalid HEADERS padding"));
            }
            frame.part = Part::PadLength;
        } else {
            self.headers_after_pad(&mut frame)?;
        }
        Ok(frame)
    }

    fn begin_continuation(
        &mut self,
        flags: Flags,
        id: StreamId,
        len: usize,
    ) -> Result<Frame, Error> {
        let Some((pending_id, pending_len)) =
            self.continuation.as_ref().map(|p| (p.stream_id, p.encoded))
        else {
            return Err(self.fail(ErrorCode::ProtocolError, "unexpected CONTINUATION"));
        };
        if pending_id != id {
            return Err(self.fail(ErrorCode::ProtocolError, "CONTINUATION on wrong stream"));
        }
        if len > self.encoded_header_limit().saturating_sub(pending_len) {
            return Err(self.fail(ErrorCode::EnhanceYourCalm, "header block too large"));
        }
        Ok(Frame::new(
            Kind::Continuation,
            flags,
            id,
            len,
            Part::Fragment { remaining: len },
        ))
    }

    fn begin_settings(&mut self, flags: Flags, id: StreamId, len: usize) -> Result<Frame, Error> {
        if id != 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "SETTINGS on non-zero stream"));
        }
        if flags.has(Flags::ACK) {
            if len != 0 {
                return Err(self.fail(ErrorCode::FrameSizeError, "SETTINGS ACK with payload"));
            }
            return Ok(Frame::new(Kind::Settings, flags, id, 0, Part::Skip));
        }
        if !len.is_multiple_of(6) {
            return Err(self.fail(ErrorCode::FrameSizeError, "SETTINGS length"));
        }
        let mut frame = Frame::new(Kind::Settings, flags, id, len, Part::Capture { want: 6 });
        frame.staged = Some(Staged {
            candidate: self.peer.clone(),
            base_window: i64::from(self.peer.initial_window_size),
            peak_growth: 0,
        });
        Ok(frame)
    }

    /// Consume payload octets of the current frame. Returns an event as soon as
    /// one is ready, leaving the frame in progress; `Ok(None)` means the frame
    /// completed or the input is used up.
    fn payload<'a>(
        &mut self,
        input: &'a [u8],
        pos: &mut usize,
    ) -> Result<Option<Event<'a>>, Error> {
        let Some(mut frame) = self.frame.take() else {
            return Ok(None);
        };
        let manual = self.config.flow_control == FlowControl::Manual;
        loop {
            if let Part::Body { remaining: 0 } | Part::Fragment { remaining: 0 } = frame.part {
                frame.part = Part::Skip;
            }
            if frame.left == 0 {
                let event = self.finish(frame)?;
                return Ok(event.or_else(|| self.notices.pop_front()));
            }
            if *pos == input.len() {
                self.frame = Some(frame);
                return Ok(None);
            }
            let avail = input.len() - *pos;
            match frame.part {
                Part::PadLength => {
                    let pad = usize::from(input[*pos]);
                    *pos += 1;
                    frame.left -= 1;
                    if pad > frame.left {
                        let reason = if frame.kind == Kind::Data {
                            "invalid DATA padding"
                        } else {
                            "invalid HEADERS padding"
                        };
                        return Err(self.fail(ErrorCode::ProtocolError, reason));
                    }
                    frame.pad = pad;
                    if frame.kind == Kind::Data {
                        self.admit_data(&mut frame)?;
                    } else {
                        self.headers_after_pad(&mut frame)?;
                    }
                }
                Part::Priority { remaining } => {
                    let n = remaining.min(avail);
                    *pos += n;
                    frame.left -= n;
                    if n == remaining {
                        self.headers_fragment(&mut frame)?;
                    } else {
                        frame.part = Part::Priority {
                            remaining: remaining - n,
                        };
                    }
                }
                Part::Body { remaining } => {
                    let n = remaining.min(avail);
                    if frame.data == DataMode::Deliver && !self.can_deliver(frame.id) {
                        // The application cancelled the stream since the last
                        // chunk: drop the rest. Only a stream still waiting to
                        // flush its reset publishes the frame's END_STREAM.
                        let publish = self
                            .streams
                            .get(&frame.id)
                            .is_some_and(|s| s.reset_after_flush.is_some());
                        frame.data = DataMode::Discard {
                            publish,
                            // Manual flow control withheld credit for the
                            // undelivered body bytes: snapshot the unread tail
                            // once, at its exact `remaining` length. It is
                            // returned once at frame completion (see `finish_data`),
                            // never per chunk. Automatic mode already credited
                            // the whole frame at admission, so it owes nothing
                            // here.
                            tail: if manual { remaining } else { 0 },
                        };
                        continue;
                    }
                    match frame.data {
                        DataMode::Deliver => {
                            let chunk = &input[*pos..*pos + n];
                            *pos += n;
                            frame.left -= n;
                            frame.part = Part::Body {
                                remaining: remaining - n,
                            };
                            // Record ownership before the chunk is exposed, so an
                            // immediate release is valid.
                            if manual && let Some(stream) = self.streams.get_mut(&frame.id) {
                                stream.unreleased += n;
                            }
                            let id = frame.id;
                            self.frame = Some(frame);
                            return Ok(Some(Event::Data {
                                stream_id: id,
                                data: Cow::Borrowed(chunk),
                            }));
                        }
                        DataMode::Discard { .. } => {
                            // Advance the input/parser positions only. The credit
                            // owed for these octets was snapshotted at the
                            // discard transition and is returned once when the
                            // frame completes; releasing it here would emit one
                            // WINDOW_UPDATE per fragment.
                            *pos += n;
                            frame.left -= n;
                            frame.part = Part::Body {
                                remaining: remaining - n,
                            };
                        }
                    }
                }
                Part::Fragment { remaining } => {
                    let n = remaining.min(avail);
                    let Some(pending) = self.continuation.as_mut() else {
                        return Err(self.fail(ErrorCode::ProtocolError, "unexpected CONTINUATION"));
                    };
                    pending.encoded += n;
                    if let Err(e) = self.decoder.push(&input[*pos..*pos + n]) {
                        return Err(self.hpack_failure(e));
                    }
                    *pos += n;
                    frame.left -= n;
                    frame.part = Part::Fragment {
                        remaining: remaining - n,
                    };
                }
                Part::Capture { want } => {
                    let n = (want - frame.filled).min(avail);
                    let at = frame.filled;
                    frame.scratch[at..at + n].copy_from_slice(&input[*pos..*pos + n]);
                    *pos += n;
                    frame.left -= n;
                    frame.filled += n;
                    if frame.filled == want {
                        frame.filled = 0;
                        if frame.kind == Kind::Settings {
                            self.stage_setting(&mut frame)?;
                            frame.part = Part::Capture { want: 6 };
                        } else {
                            frame.part = Part::Skip;
                        }
                    }
                }
                Part::Skip => {
                    let n = frame.left.min(avail);
                    *pos += n;
                    frame.left -= n;
                }
            }
            if let Some(event) = self.notices.pop_front() {
                self.frame = Some(frame);
                return Ok(Some(event));
            }
            self.check_failed()?;
        }
    }

    /// Settle HEADERS once its padding is known: skip the priority fields if
    /// present, then start the header block.
    fn headers_after_pad(&mut self, frame: &mut Frame) -> Result<(), Error> {
        if frame.flags.has(Flags::PRIORITY) {
            if frame.left - frame.pad < 5 {
                return Err(self.fail(ErrorCode::FrameSizeError, "short HEADERS priority"));
            }
            frame.part = Part::Priority { remaining: 5 };
            return Ok(());
        }
        self.headers_fragment(frame)
    }

    /// Start a header block for HEADERS. Its declared size is checked before any
    /// fragment octet is stored, so the block never exceeds the encoded limit.
    fn headers_fragment(&mut self, frame: &mut Frame) -> Result<(), Error> {
        let fragment = frame.left - frame.pad;
        if fragment > self.encoded_header_limit() {
            return Err(self.fail(ErrorCode::EnhanceYourCalm, "header block too large"));
        }
        self.continuation = Some(PendingBlock {
            stream_id: frame.id,
            end_stream: frame.flags.has(Flags::END_STREAM),
            encoded: 0,
        });
        self.decoder
            .begin(self.config.max_header_list_size as usize);
        frame.part = Part::Fragment {
            remaining: fragment,
        };
        Ok(())
    }

    /// Decide what happens to a DATA body, once the stream it belongs to is
    /// known. Resource checks happen here, before any octet is delivered.
    fn admit_data(&mut self, frame: &mut Frame) -> Result<(), Error> {
        let id = frame.id;
        let total = frame.total;
        let body = frame.left - frame.pad;
        let end = frame.flags.has(Flags::END_STREAM);
        frame.part = Part::Body { remaining: body };
        frame.data = DataMode::Discard {
            publish: false,
            tail: 0,
        };
        let Some(stream) = self.streams.get_mut(&id) else {
            if self.is_idle_stream(id) {
                return Err(self.fail(ErrorCode::ProtocolError, "DATA on idle stream"));
            }
            // Closed or reset stream: the body is discarded, so its credit goes back now.
            self.release_connection(total);
            return Ok(());
        };
        let refused = if !stream.can_recv() {
            Some(ErrorCode::StreamClosed)
        } else if total as i64 > stream.recv_window {
            Some(ErrorCode::FlowControlError)
        } else if stream.recv_phase != Phase::Body {
            Some(ErrorCode::ProtocolError)
        } else {
            match stream.recv_body.checked_data(body, end) {
                Ok(next) => {
                    stream.recv_body = next;
                    None
                }
                Err(_) => Some(ErrorCode::ProtocolError),
            }
        };
        if let Some(code) = refused {
            self.release_connection(total);
            self.reset(id, code);
            return Ok(());
        }
        stream.recv_window -= total as i64;
        if stream.reset_after_flush.is_some() {
            // The application has finished with this stream; nobody will release
            // these bytes.
            self.release_connection(total);
            frame.data = DataMode::Discard {
                publish: true,
                tail: 0,
            };
            return Ok(());
        }
        // Credit that goes back at once: the whole frame in automatic mode, but in
        // manual mode only the padding, which is never delivered. The application
        // releases the body bytes it is handed.
        let credit = if self.config.flow_control == FlowControl::Manual {
            total - body
        } else {
            total
        };
        frame.data = DataMode::Deliver;
        if end {
            // The peer's last octets on this stream: it cannot receive once they
            // are in, so only the connection is credited.
            self.release_connection(credit);
        } else {
            self.credit_received(id, credit);
        }
        Ok(())
    }

    /// The connection error for a header block the HPACK decoder rejected.
    fn hpack_failure(&mut self, error: hpack::DecodeError) -> Error {
        match error {
            hpack::DecodeError::Limit => {
                self.fail(ErrorCode::EnhanceYourCalm, "decoded header list too large")
            }
            hpack::DecodeError::Compression => {
                self.fail(ErrorCode::CompressionError, "HPACK decode failed")
            }
        }
    }

    /// Whether body octets of `id` may still be delivered to the application.
    fn can_deliver(&self, id: StreamId) -> bool {
        self.streams
            .get(&id)
            .is_some_and(|s| s.can_recv() && s.reset_after_flush.is_none())
    }

    /// Apply one SETTINGS tuple from the scratch to the staged settings.
    fn stage_setting(&mut self, frame: &mut Frame) -> Result<(), Error> {
        let raw = frame.scratch;
        // Unknown identifiers are ignored (RFC 9113 §6.5.2).
        let Some(id) = SettingId::from_u16(u16::from_be_bytes([raw[0], raw[1]])) else {
            return Ok(());
        };
        let value = u32::from_be_bytes([raw[2], raw[3], raw[4], raw[5]]);
        let Some(staged) = frame.staged.as_mut() else {
            return Ok(());
        };
        if let Err(e) = staged.stage(Setting { id, value }) {
            let code = match e {
                Http2Error::Protocol(c) => c,
                _ => ErrorCode::ProtocolError,
            };
            return Err(self.fail(code, "invalid SETTINGS value"));
        }
        Ok(())
    }

    /// Complete a frame whose payload has been consumed.
    fn finish(&mut self, mut frame: Frame) -> Result<Option<Event<'static>>, Error> {
        match frame.kind {
            Kind::Data => Ok(self.finish_data(&frame)),
            Kind::Headers | Kind::Continuation => {
                if frame.flags.has(Flags::END_HEADERS)
                    && let Some(pending) = self.continuation.take()
                {
                    let headers = match self.decoder.finish() {
                        Ok(headers) => headers,
                        Err(e) => return Err(self.hpack_failure(e)),
                    };
                    self.on_header_block(pending.stream_id, pending.end_stream, headers)?;
                }
                Ok(None)
            }
            Kind::Priority | Kind::Skip => Ok(None),
            Kind::RstStream => {
                let code = ErrorCode::from_u32(u32::from_be_bytes([
                    frame.scratch[0],
                    frame.scratch[1],
                    frame.scratch[2],
                    frame.scratch[3],
                ]));
                if self.remove_stream(frame.id) {
                    self.notices.push_back(Event::Reset {
                        stream_id: frame.id,
                        error_code: code,
                    });
                }
                Ok(None)
            }
            Kind::Settings => {
                if frame.flags.has(Flags::ACK) {
                    self.on_settings_ack();
                } else if let Some(staged) = frame.staged.take() {
                    self.commit_settings(staged)?;
                }
                Ok(None)
            }
            Kind::Ping => {
                if !frame.flags.has(Flags::ACK) {
                    self.write_frame(FrameType::Ping, Flags::ACK, 0, &frame.scratch);
                }
                Ok(None)
            }
            Kind::GoAway => {
                let s = frame.scratch;
                let last = u32::from_be_bytes([s[0], s[1], s[2], s[3]]) & 0x7fff_ffff;
                let code = ErrorCode::from_u32(u32::from_be_bytes([s[4], s[5], s[6], s[7]]));
                self.goaway_received = true;
                self.notices.push_back(Event::GoAway {
                    last_stream_id: last,
                    error_code: code,
                });
                // Locally-initiated streams above `last` were never processed by
                // the peer; they are refused one at a time by `next_refusal`.
                self.refusal = Some((last, 0));
                Ok(None)
            }
            Kind::WindowUpdate => {
                let s = frame.scratch;
                let inc = u32::from_be_bytes([s[0], s[1], s[2], s[3]]) & 0x7fff_ffff;
                self.window_update(frame.id, i64::from(inc))?;
                Ok(None)
            }
        }
    }

    /// Complete a DATA frame: publish END_STREAM once its trailing padding has
    /// been consumed, and report the completion if the body was delivered.
    fn finish_data(&mut self, frame: &Frame) -> Option<Event<'static>> {
        // Return the deferred cancelled-tail credit once, before any early return
        // for a removed stream or a suppressed completion event. This is a
        // connection-level obligation independent of the stream map and of
        // whether END_STREAM/completion will be published; automatic and
        // admission-credited discards record zero, so they emit nothing.
        if let DataMode::Discard { tail, .. } = frame.data {
            self.release_connection(tail);
        }
        let delivered = frame.data == DataMode::Deliver;
        let publish = match frame.data {
            DataMode::Deliver => true,
            DataMode::Discard { publish, .. } => publish,
        };
        if !publish {
            return None;
        }
        let end = frame.flags.has(Flags::END_STREAM);
        let stream = self.streams.get_mut(&frame.id)?;
        if end {
            stream.recv_phase = Phase::End;
            stream.apply(StreamEvent::RecvEndStream);
        }
        self.cleanup(frame.id);
        delivered.then_some(Event::DataEnd {
            stream_id: frame.id,
            end_stream: end,
        })
    }

    /// Apply a complete SETTINGS frame: every live stream is checked against the
    /// largest transient growth before any window changes, so a failure leaves
    /// the connection as it was.
    fn commit_settings(&mut self, staged: Staged) -> Result<(), Error> {
        let total = i64::from(staged.candidate.initial_window_size) - staged.base_window;
        // A transient overshoot is invalid even if a later tuple lowers the
        // window again. Streams that closed while the frame was arriving are not
        // in the map and are not resurrected.
        if self
            .streams
            .values()
            .any(|s| s.send_window + staged.peak_growth > MAX_WINDOW)
        {
            return Err(self.fail(ErrorCode::FlowControlError, "stream window overflow"));
        }
        for stream in self.streams.values_mut() {
            stream.send_window += total;
        }
        self.peer = staged.candidate;
        self.write_frame(FrameType::Settings, Flags::ACK, 0, &[]);
        Ok(())
    }

    fn window_update(&mut self, id: StreamId, inc: i64) -> Result<(), Error> {
        if id == 0 {
            if inc == 0 {
                return Err(self.fail(ErrorCode::ProtocolError, "zero WINDOW_UPDATE"));
            }
            self.conn_send_window += inc;
            if self.conn_send_window > MAX_WINDOW {
                return Err(self.fail(ErrorCode::FlowControlError, "connection window overflow"));
            }
            return Ok(());
        }
        if inc == 0 {
            self.reset(id, ErrorCode::ProtocolError);
            return Ok(());
        }
        if let Some(stream) = self.streams.get_mut(&id) {
            stream.send_window += inc;
            if stream.send_window > MAX_WINDOW {
                self.reset(id, ErrorCode::FlowControlError);
            }
        }
        Ok(())
    }

    /// The next refused stream after a GOAWAY, one per call, so that a GOAWAY
    /// naming many streams queues nothing.
    pub(super) fn next_refusal(&mut self) -> Option<Event<'static>> {
        let (last, after) = self.refusal?;
        let next = self
            .streams
            .range((Excluded(after), Unbounded))
            .map(|(id, _)| *id)
            .find(|id| *id > last && !self.is_peer_initiated(*id));
        let Some(id) = next else {
            self.refusal = None;
            return None;
        };
        self.refusal = Some((last, id));
        self.remove_stream(id);
        Some(Event::Reset {
            stream_id: id,
            error_code: ErrorCode::RefusedStream,
        })
    }
}
