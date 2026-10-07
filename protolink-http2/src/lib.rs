//! # protolink-http2
//!
//! A sans-IO, `no_std + alloc` HTTP/2 connection state machine.
//!
//! Frame encoding/decoding, SETTINGS (de)serialisation, stream state transitions
//! and HPACK come from the `zerodds-http2` and `zerodds-hpack` crates. This crate
//! adds the connection lifecycle that a real HTTP/2 peer (tonic, grpc-go,
//! grpcurl, `h2`, ...) expects:
//!
//! - connection preface and initial SETTINGS exchange,
//! - SETTINGS / PING acknowledgement,
//! - partial-frame buffering across arbitrary read boundaries,
//! - header blocks split over CONTINUATION frames, padding and priority fields,
//! - send-side flow control (connection + stream windows, WINDOW_UPDATE),
//! - receive-side flow control, either replenished automatically or released by
//!   the application ([`FlowControl::Manual`]),
//! - RST_STREAM, GOAWAY and connection error handling.
//!
//! The [`Connection`] never performs I/O. Feed received bytes with
//! [`Connection::recv`], drain produced events with [`Connection::poll_event`]
//! and write [`Connection::pending_output`] to the transport.
//!
//! # Streaming and backpressure
//!
//! Long-lived streams need bounded buffering in both directions:
//!
//! - **Sending.** [`Connection::send_data`] never blocks; data beyond the
//!   peer's flow-control window is queued inside the connection. Use
//!   [`Connection::queued_send_bytes`] / [`Connection::send_capacity`] to apply
//!   a high-water mark, and [`Connection::poll_send_ready`] to learn which
//!   streams made progress after [`Connection::recv`].
//! - **Receiving.** With [`FlowControl::Manual`], received DATA is only credited
//!   back to the peer when the application calls
//!   [`Connection::release_capacity`], so unconsumed data per stream is bounded
//!   by [`Config::initial_window_size`] without stalling other streams.
//! - **Closing.** [`Connection::reset_stream_after_flush`] sends RST_STREAM only
//!   once all queued DATA and trailers have been written, as needed by a server
//!   that finishes before the client half-closes.
//!
//! Server push is not supported (clients advertise `SETTINGS_ENABLE_PUSH = 0`).
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::vec::Vec;
use core::fmt;

mod headers;
mod hpack;
mod huffman;

use headers::Phase;
use hpack::Decoder;
use zerodds_hpack::Encoder;
use zerodds_http2::flow::{decode_window_update, encode_window_update};
use zerodds_http2::settings::{decode_settings, encode_settings};
use zerodds_http2::stream::{StreamEvent, is_client_initiated, is_server_initiated, transition};
use zerodds_http2::{
    CLIENT_PREFACE, Flags, FrameHeader, FrameType, Http2Error, Setting, SettingId, Settings,
    StreamState, decode_frame, encode_frame,
};

pub use zerodds_hpack::HeaderField;
pub use zerodds_http2::{ErrorCode, StreamId};

const FRAME_HEADER_LEN: usize = 9;
const MAX_WINDOW: i64 = 0x7fff_ffff;
const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;
/// Initial flow-control window defined by RFC 9113 §6.9.2.
const DEFAULT_WINDOW: i64 = 65_535;

/// Which side of the connection this endpoint is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Initiates streams (odd stream ids) and sends the connection preface.
    Client,
    /// Accepts streams.
    Server,
}

/// How received DATA is credited back to the peer (receive-side flow control).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlowControl {
    /// Every received DATA frame is acknowledged with WINDOW_UPDATE frames as
    /// soon as it is parsed, whether or not the application has consumed it.
    /// The peer is never throttled.
    #[default]
    Automatic,
    /// Receive windows are tracked per stream and per connection. Bytes
    /// delivered in [`Event::Data`] are only credited back to the peer once
    /// the application calls [`Connection::release_capacity`]. Overrunning a
    /// stream window resets that stream with `FLOW_CONTROL_ERROR`; overrunning
    /// the connection window is a connection error.
    ///
    /// Padding, DATA on closed or reset streams and DATA discarded by
    /// [`Connection::reset_stream_after_flush`] are released automatically, as
    /// is capacity still held when a stream closes or resets, unless opted into
    /// [`Connection::retain_receive_capacity`].
    Manual,
}

/// Local connection limits, advertised to the peer in the initial SETTINGS frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// `SETTINGS_MAX_CONCURRENT_STREAMS` (peer-initiated streams we accept).
    pub max_concurrent_streams: u32,
    /// `SETTINGS_INITIAL_WINDOW_SIZE` for our receive windows.
    pub initial_window_size: u32,
    /// `SETTINGS_MAX_HEADER_LIST_SIZE`. Larger decoded lists terminate the
    /// connection before materializing fields beyond this limit.
    pub max_header_list_size: u32,
    /// Connection-level receive window. HTTP/2 starts every connection at
    /// 65 535 bytes; larger values are announced with a WINDOW_UPDATE right
    /// after our SETTINGS. Smaller values are treated as 65 535.
    ///
    /// With [`FlowControl::Manual`], set this to at least
    /// `max_concurrent_streams * initial_window_size` so that one stream whose
    /// data is not being consumed cannot starve the others.
    pub connection_window_size: u32,
    /// Receive-side flow-control mode.
    pub flow_control: FlowControl,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 8,
            initial_window_size: 65_535,
            max_header_list_size: 8 * 1024,
            connection_window_size: 65_535,
            flow_control: FlowControl::Automatic,
        }
    }
}

/// Something that happened on the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A complete header block (request/response headers or trailers).
    Headers {
        /// Stream the headers belong to.
        stream_id: StreamId,
        /// Decoded header fields, pseudo-headers included.
        headers: Vec<HeaderField>,
        /// The peer closed its side of the stream.
        end_stream: bool,
    },
    /// Body bytes (padding stripped).
    Data {
        /// Stream the data belongs to.
        stream_id: StreamId,
        /// Payload bytes.
        data: Vec<u8>,
        /// The peer closed its side of the stream.
        end_stream: bool,
    },
    /// The stream was reset, by the peer or because of a stream error.
    Reset {
        /// Affected stream.
        stream_id: StreamId,
        /// Reason.
        error_code: ErrorCode,
    },
    /// The peer is shutting the connection down.
    GoAway {
        /// Highest peer-processed stream id.
        last_stream_id: StreamId,
        /// Reason.
        error_code: ErrorCode,
    },
}

/// Connection-level errors and API misuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Fatal protocol violation. A GOAWAY has been queued; write the pending
    /// output and close the transport.
    Connection {
        /// HTTP/2 error code sent in the GOAWAY.
        code: ErrorCode,
        /// Human-readable reason.
        reason: &'static str,
    },
    /// The stream does not exist (never opened, closed or reset).
    UnknownStream(StreamId),
    /// The local side of the stream is already closed.
    StreamClosed(StreamId),
    /// Operation not available for this [`Role`].
    WrongRole,
    /// The connection is shutting down; no new streams can be opened.
    GoingAway,
    /// Retryable: the peer's concurrent-stream limit is currently reached.
    StreamLimit,
    /// All locally initiated 31-bit stream IDs have been used.
    StreamIdExhausted,
    /// Invalid outbound HTTP message ordering or field section.
    InvalidHeaders(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection { code, reason } => write!(f, "connection error {code:?}: {reason}"),
            Self::UnknownStream(id) => write!(f, "unknown stream {id}"),
            Self::StreamClosed(id) => write!(f, "stream {id} is closed for sending"),
            Self::WrongRole => f.write_str("operation not supported for this role"),
            Self::GoingAway => f.write_str("connection is going away"),
            Self::StreamLimit => f.write_str("peer concurrent stream limit reached"),
            Self::StreamIdExhausted => f.write_str("stream IDs exhausted"),
            Self::InvalidHeaders(reason) => write!(f, "invalid HTTP message: {reason}"),
        }
    }
}

impl core::error::Error for Error {}

#[derive(Debug)]
enum Outbound {
    Headers {
        fields: Vec<HeaderField>,
        end_stream: bool,
    },
    Data {
        buf: Vec<u8>,
        offset: usize,
        end_stream: bool,
    },
}

#[derive(Debug)]
struct Stream {
    state: StreamState,
    send_window: i64,
    outbound: VecDeque<Outbound>,
    send_phase: Phase,
    recv_phase: Phase,
    send_body: headers::Body,
    recv_body: headers::Body,
    request_kind: headers::RequestKind,
    retain_credit: bool,
    /// Our receive window as the peer sees it.
    recv_window: i64,
    /// Bytes delivered in [`Event::Data`] but not yet released
    /// ([`FlowControl::Manual`] only).
    unreleased: usize,
    /// Send RST_STREAM with this code once `outbound` has drained.
    reset_after_flush: Option<ErrorCode>,
}

impl Stream {
    fn new(send_window: i64, recv_window: i64) -> Self {
        Self {
            state: StreamState::Idle,
            send_window,
            outbound: VecDeque::new(),
            recv_window,
            send_phase: Phase::Initial,
            recv_phase: Phase::Initial,
            send_body: headers::Body::default(),
            recv_body: headers::Body::default(),
            request_kind: headers::RequestKind::default(),
            retain_credit: false,
            unreleased: 0,
            reset_after_flush: None,
        }
    }

    fn checked_headers(
        &self,
        fields: &[HeaderField],
        request: bool,
        sending: bool,
        end_stream: bool,
    ) -> Result<(headers::Section, headers::Body), &'static str> {
        let (phase, current) = if sending {
            (self.send_phase, self.send_body)
        } else {
            (self.recv_phase, self.recv_body)
        };
        if current.is_tunnel() {
            return Err("HEADERS on an established tunnel");
        }
        let section = headers::validate(fields, request, phase, end_stream)?;
        let body = if phase == Phase::Body || section.phase == Phase::Informational {
            current
        } else {
            headers::Body::for_section(&section, self.request_kind)
        };
        Ok((section, body.checked_data(0, end_stream)?))
    }

    fn commit_headers(&mut self, section: headers::Section, body: headers::Body, sending: bool) {
        if sending {
            self.send_phase = section.phase;
            self.send_body = body;
        } else {
            self.recv_phase = section.phase;
            self.recv_body = body;
        }
        if let Some(method) = section.method {
            self.request_kind = method;
        }
        if self.request_kind == headers::RequestKind::Connect
            && section
                .status
                .is_some_and(|code| (200..300).contains(&code))
        {
            // Successful CONNECT turns both halves into opaque tunnel bytes.
            self.send_body.tunnel();
            self.recv_body.tunnel();
        }
    }

    /// Validate `fields` as the next outbound header block (a request if
    /// `request`) and queue it, changing nothing if it is rejected.
    fn queue_headers(
        &mut self,
        id: StreamId,
        fields: Vec<HeaderField>,
        request: bool,
        end_stream: bool,
    ) -> Result<(), Error> {
        if self.reset_after_flush.is_some()
            || !matches!(
                self.state,
                StreamState::Idle | StreamState::Open | StreamState::HalfClosedRemote
            )
        {
            return Err(Error::StreamClosed(id));
        }
        let (section, body) = self
            .checked_headers(&fields, request, true, end_stream)
            .map_err(Error::InvalidHeaders)?;
        if !self.apply(StreamEvent::SendHeaders) {
            return Err(Error::StreamClosed(id));
        }
        self.commit_headers(section, body, true);
        if end_stream {
            self.apply(StreamEvent::SendEndStream);
        }
        self.outbound
            .push_back(Outbound::Headers { fields, end_stream });
        Ok(())
    }

    fn can_send(&self) -> bool {
        matches!(
            self.state,
            StreamState::Open | StreamState::HalfClosedRemote
        ) && self.reset_after_flush.is_none()
    }

    fn queued_bytes(&self) -> usize {
        self.outbound
            .iter()
            .map(|o| match o {
                Outbound::Data { buf, offset, .. } => buf.len() - offset,
                Outbound::Headers { .. } => 0,
            })
            .sum()
    }

    fn apply(&mut self, ev: StreamEvent) -> bool {
        match transition(self.state, ev) {
            Ok(s) => {
                self.state = s;
                true
            }
            Err(_) => false,
        }
    }

    fn can_recv(&self) -> bool {
        matches!(self.state, StreamState::Open | StreamState::HalfClosedLocal)
    }
}

struct PendingBlock {
    stream_id: StreamId,
    end_stream: bool,
    block: Vec<u8>,
}

/// Sans-IO HTTP/2 connection.
pub struct Connection {
    role: Role,
    config: Config,
    peer: Settings,
    encoder: Encoder,
    encoder_update_pending: bool,
    decoder: Decoder,
    input: Vec<u8>,
    output: Vec<u8>,
    awaiting_preface: bool,
    awaiting_peer_settings: bool,
    streams: BTreeMap<StreamId, Stream>,
    retained_credit: BTreeMap<StreamId, usize>,
    conn_send_window: i64,
    /// Connection receive window as the peer sees it.
    conn_recv_window: i64,
    /// The peer has acknowledged our SETTINGS.
    local_settings_acked: bool,
    /// Streams whose queued send data shrank since last reported.
    send_ready: BTreeSet<StreamId>,
    next_local_id: StreamId,
    last_peer_id: StreamId,
    continuation: Option<PendingBlock>,
    events: VecDeque<Event>,
    goaway_sent: bool,
    /// The last-stream-ID advertised by the first GOAWAY; later ones repeat it.
    goaway_last_stream_id: Option<StreamId>,
    goaway_received: bool,
    failed: Option<Error>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("role", &self.role)
            .field("streams", &self.streams.len())
            .field("pending_output", &self.output.len())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl Connection {
    /// New client connection. The connection preface and SETTINGS are queued
    /// immediately in [`pending_output`](Self::pending_output).
    pub fn client(config: Config) -> Self {
        Self::new(Role::Client, config)
    }

    /// New server connection. Our SETTINGS are queued immediately; the client
    /// preface is expected as the first received bytes.
    pub fn server(config: Config) -> Self {
        Self::new(Role::Server, config)
    }

    /// New connection for `role`.
    pub fn new(role: Role, config: Config) -> Self {
        // A zero-sized dynamic table means we only ever reference the static
        // table, which keeps us correct whatever SETTINGS_HEADER_TABLE_SIZE the
        // peer advertises.
        let encoder = Encoder::with_max_size(0);
        let mut conn = Self {
            role,
            config,
            peer: Settings::default(),
            encoder,
            encoder_update_pending: true,
            decoder: Decoder::new(),
            input: Vec::new(),
            output: Vec::new(),
            awaiting_preface: role == Role::Server,
            awaiting_peer_settings: true,
            streams: BTreeMap::new(),
            retained_credit: BTreeMap::new(),
            conn_send_window: 65_535,
            conn_recv_window: DEFAULT_WINDOW,
            local_settings_acked: false,
            send_ready: BTreeSet::new(),
            next_local_id: if role == Role::Client { 1 } else { 2 },
            last_peer_id: 0,
            continuation: None,
            events: VecDeque::new(),
            goaway_sent: false,
            goaway_last_stream_id: None,
            goaway_received: false,
            failed: None,
        };
        if role == Role::Client {
            conn.output.extend_from_slice(CLIENT_PREFACE);
        }
        let mut settings = alloc::vec![
            Setting {
                id: SettingId::MaxConcurrentStreams,
                value: config.max_concurrent_streams,
            },
            Setting {
                id: SettingId::InitialWindowSize,
                value: config.initial_window_size,
            },
            Setting {
                id: SettingId::MaxHeaderListSize,
                value: config.max_header_list_size,
            },
        ];
        if role == Role::Client {
            settings.push(Setting {
                id: SettingId::EnablePush,
                value: 0,
            });
        }
        conn.write_frame(FrameType::Settings, 0, 0, &encode_settings(&settings));
        let conn_window = i64::from(config.connection_window_size).min(MAX_WINDOW);
        if conn_window > DEFAULT_WINDOW {
            let inc = (conn_window - DEFAULT_WINDOW) as u32;
            conn.write_frame(FrameType::WindowUpdate, 0, 0, &encode_window_update(inc));
            conn.conn_recv_window = conn_window;
        }
        conn
    }

    /// This endpoint's role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// Settings most recently received from the peer.
    pub fn peer_settings(&self) -> &Settings {
        &self.peer
    }

    /// Bytes that must be written to the transport.
    pub fn pending_output(&self) -> &[u8] {
        &self.output
    }

    /// Mark the first `n` bytes of [`pending_output`](Self::pending_output) as written.
    pub fn consume_output(&mut self, n: usize) {
        let n = n.min(self.output.len());
        self.output.drain(..n);
    }

    /// Take all pending output.
    pub fn take_output(&mut self) -> Vec<u8> {
        core::mem::take(&mut self.output)
    }

    /// Whether there is output waiting to be written.
    pub fn has_output(&self) -> bool {
        !self.output.is_empty()
    }

    /// Next connection event, if any.
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// Number of open (not fully closed) streams.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Whether the stream is known and not fully closed.
    pub fn has_stream(&self, id: StreamId) -> bool {
        self.streams.contains_key(&id)
    }

    /// The connection hit a fatal error or finished a graceful shutdown.
    pub fn is_closed(&self) -> bool {
        self.failed.is_some()
            || ((self.goaway_sent || self.goaway_received) && self.streams.is_empty())
    }

    /// Feed bytes received from the transport.
    ///
    /// On `Err(Error::Connection { .. })` a GOAWAY has been queued: write the
    /// remaining output and close the transport.
    pub fn recv(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if let Some(e) = &self.failed {
            return Err(e.clone());
        }
        let mut input = core::mem::take(&mut self.input);
        input.extend_from_slice(bytes);
        let result = self.process_input(&mut input);
        self.input = input;
        result
    }

    fn process_input(&mut self, input: &mut Vec<u8>) -> Result<(), Error> {
        let mut pos = 0;
        if self.awaiting_preface {
            let n = input.len().min(CLIENT_PREFACE.len());
            if input[..n] != CLIENT_PREFACE[..n] {
                return Err(self.fail(ErrorCode::ProtocolError, "invalid connection preface"));
            }
            if n < CLIENT_PREFACE.len() {
                return Ok(());
            }
            self.awaiting_preface = false;
            pos = CLIENT_PREFACE.len();
        }
        let max_frame = self.local_max_frame_size();
        let mut result = Ok(());
        while input.len() - pos >= FRAME_HEADER_LEN {
            let rest = &input[pos..];
            let len =
                (usize::from(rest[0]) << 16) | (usize::from(rest[1]) << 8) | usize::from(rest[2]);
            if len > max_frame as usize {
                result = Err(self.fail(
                    ErrorCode::FrameSizeError,
                    "frame exceeds SETTINGS_MAX_FRAME_SIZE",
                ));
                break;
            }
            if rest.len() < FRAME_HEADER_LEN + len {
                break;
            }
            let frame = &rest[..FRAME_HEADER_LEN + len];
            pos += frame.len();
            if let Err(e) = self.handle_frame(frame, max_frame) {
                result = Err(e);
                break;
            }
        }
        input.drain(..pos);
        if result.is_ok() {
            self.flush_streams();
        }
        result
    }

    fn handle_frame(&mut self, raw: &[u8], max_frame: u32) -> Result<(), Error> {
        // RFC 9113 §3.4: the peer's first frame must be a non-ACK SETTINGS, so
        // this precedes the unknown-frame early return.
        if self.awaiting_peer_settings
            && (raw[3] != FrameType::Settings as u8 || Flags(raw[4]).has(Flags::ACK))
        {
            return Err(self.fail(ErrorCode::ProtocolError, "first frame must be SETTINGS"));
        }
        let Ok(frame_type) = FrameType::from_u8(raw[3]) else {
            // RFC 9113 §4.1: unknown frame types MUST be ignored, except in the
            // middle of a header block.
            if self.continuation.is_some() {
                return Err(self.fail(ErrorCode::ProtocolError, "expected CONTINUATION"));
            }
            return Ok(());
        };
        let (frame, _) = match decode_frame(raw, max_frame) {
            Ok(f) => f,
            Err(_) => return Err(self.fail(ErrorCode::FrameSizeError, "malformed frame")),
        };
        let FrameHeader {
            flags, stream_id, ..
        } = frame.header;
        let payload = frame.payload;

        self.awaiting_peer_settings = false;
        if self.continuation.is_some() && frame_type != FrameType::Continuation {
            return Err(self.fail(ErrorCode::ProtocolError, "expected CONTINUATION"));
        }

        match frame_type {
            FrameType::Data => self.on_data(stream_id, flags, payload),
            FrameType::Headers => self.on_headers(stream_id, flags, payload),
            FrameType::Continuation => self.on_continuation(stream_id, flags, payload),
            FrameType::Priority => {
                if stream_id == 0 {
                    return Err(self.fail(ErrorCode::ProtocolError, "PRIORITY on stream 0"));
                }
                if payload.len() != 5 {
                    self.reset(stream_id, ErrorCode::FrameSizeError);
                }
                Ok(())
            }
            FrameType::RstStream => self.on_rst_stream(stream_id, payload),
            FrameType::Settings => self.on_settings(stream_id, flags, payload),
            FrameType::PushPromise => {
                Err(self.fail(ErrorCode::ProtocolError, "PUSH_PROMISE not supported"))
            }
            FrameType::Ping => self.on_ping(stream_id, flags, payload),
            FrameType::GoAway => self.on_goaway(stream_id, payload),
            FrameType::WindowUpdate => self.on_window_update(stream_id, payload),
        }
    }

    fn on_data(&mut self, id: StreamId, flags: Flags, payload: &[u8]) -> Result<(), Error> {
        if id == 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "DATA on stream 0"));
        }
        // Both modes enforce the same windows; only credit timing differs.
        let flow_len = payload.len();
        if flow_len as i64 > self.conn_recv_window {
            return Err(self.fail(
                ErrorCode::FlowControlError,
                "connection receive window exceeded",
            ));
        }
        self.conn_recv_window -= flow_len as i64;
        let data = match strip_padding(flags, payload) {
            Some(d) => d,
            None => return Err(self.fail(ErrorCode::ProtocolError, "invalid DATA padding")),
        };
        let end_stream = flags.has(Flags::END_STREAM);
        let padding = flow_len - data.len();

        let Some(stream) = self.streams.get_mut(&id) else {
            if self.is_idle_stream(id) {
                return Err(self.fail(ErrorCode::ProtocolError, "DATA on idle stream"));
            }
            // Closed or reset stream: discard.
            self.release_connection(flow_len);
            return Ok(());
        };
        if !stream.can_recv() {
            self.release_connection(flow_len);
            self.reset(id, ErrorCode::StreamClosed);
            return Ok(());
        }
        if flow_len as i64 > stream.recv_window {
            self.release_connection(flow_len);
            self.reset(id, ErrorCode::FlowControlError);
            return Ok(());
        }
        if stream.recv_phase != Phase::Body {
            self.release_connection(flow_len);
            self.reset(id, ErrorCode::ProtocolError);
            return Ok(());
        }
        let Ok(body) = stream.recv_body.checked_data(data.len(), end_stream) else {
            self.release_connection(flow_len);
            self.reset(id, ErrorCode::ProtocolError);
            return Ok(());
        };
        stream.recv_body = body;
        stream.recv_window -= flow_len as i64;
        if end_stream {
            stream.recv_phase = Phase::End;
            stream.apply(StreamEvent::RecvEndStream);
        }
        if stream.reset_after_flush.is_some() {
            // The application has finished with this stream; nobody will
            // release these bytes.
            self.release_connection(flow_len);
            self.cleanup(id);
            return Ok(());
        }
        // Credit that goes back at once: the whole frame in automatic mode, but
        // in manual mode only the padding, which is never delivered. The
        // application releases the rest.
        let credit = if self.config.flow_control == FlowControl::Manual {
            stream.unreleased += data.len();
            padding
        } else {
            flow_len
        };
        self.credit_received(id, credit);
        self.events.push_back(Event::Data {
            stream_id: id,
            data: data.to_vec(),
            end_stream,
        });
        self.cleanup(id);
        Ok(())
    }

    fn on_headers(&mut self, id: StreamId, flags: Flags, payload: &[u8]) -> Result<(), Error> {
        if id == 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "HEADERS on stream 0"));
        }
        let Some(mut block) = strip_padding(flags, payload) else {
            return Err(self.fail(ErrorCode::ProtocolError, "invalid HEADERS padding"));
        };
        if flags.has(Flags::PRIORITY) {
            if block.len() < 5 {
                return Err(self.fail(ErrorCode::FrameSizeError, "short HEADERS priority"));
            }
            block = &block[5..];
        }
        if block.len() > self.encoded_header_limit() {
            return Err(self.fail(ErrorCode::EnhanceYourCalm, "header block too large"));
        }
        let pending = PendingBlock {
            stream_id: id,
            end_stream: flags.has(Flags::END_STREAM),
            block: block.to_vec(),
        };
        if flags.has(Flags::END_HEADERS) {
            self.on_header_block(pending)
        } else {
            self.continuation = Some(pending);
            Ok(())
        }
    }

    fn on_continuation(&mut self, id: StreamId, flags: Flags, payload: &[u8]) -> Result<(), Error> {
        let Some(mut pending) = self.continuation.take() else {
            return Err(self.fail(ErrorCode::ProtocolError, "unexpected CONTINUATION"));
        };
        if pending.stream_id != id {
            return Err(self.fail(ErrorCode::ProtocolError, "CONTINUATION on wrong stream"));
        }
        if payload.len()
            > self
                .encoded_header_limit()
                .saturating_sub(pending.block.len())
        {
            return Err(self.fail(ErrorCode::EnhanceYourCalm, "header block too large"));
        }
        pending.block.extend_from_slice(payload);
        if flags.has(Flags::END_HEADERS) {
            self.on_header_block(pending)
        } else {
            self.continuation = Some(pending);
            Ok(())
        }
    }

    fn on_header_block(&mut self, block: PendingBlock) -> Result<(), Error> {
        let PendingBlock {
            stream_id: id,
            end_stream,
            block,
        } = block;
        // Decode even discarded streams. Overflow terminates the connection,
        // avoiding both unbounded materialization and unsynchronized HPACK state.
        let headers = match self
            .decoder
            .decode(&block, self.config.max_header_list_size as usize)
        {
            Ok(h) => h,
            Err(hpack::DecodeError::Limit) => {
                return Err(self.fail(ErrorCode::EnhanceYourCalm, "decoded header list too large"));
            }
            Err(hpack::DecodeError::Compression) => {
                return Err(self.fail(ErrorCode::CompressionError, "HPACK decode failed"));
            }
        };

        if !self.streams.contains_key(&id) {
            if !self.is_peer_initiated(id) {
                if id >= self.next_local_id {
                    return Err(self.fail(ErrorCode::ProtocolError, "HEADERS on idle local stream"));
                }
                // Locally reset/closed stream: discard.
                return Ok(());
            }
            if self.role == Role::Client {
                return Err(self.fail(ErrorCode::ProtocolError, "server-initiated stream"));
            }
            if id <= self.last_peer_id {
                self.reset(id, ErrorCode::StreamClosed);
                return Ok(());
            }
            self.last_peer_id = id;
            if self.goaway_sent {
                return Ok(());
            }
            // Without server push every open stream is peer-initiated here.
            if self.streams.len() >= self.config.max_concurrent_streams as usize {
                self.reset(id, ErrorCode::RefusedStream);
                return Ok(());
            }
            let stream = self.new_stream();
            self.streams.insert(id, stream);
        }

        // HTTP/API validation is intentionally after complete raw HPACK decoding:
        // even rejected octet strings must remain in the shared dynamic table.
        let Ok(headers) = headers
            .into_iter()
            .map(hpack::RawHeader::into_field)
            .collect::<Result<Vec<_>, _>>()
        else {
            self.reset(id, ErrorCode::ProtocolError);
            return Ok(());
        };
        let Some(stream) = self.streams.get_mut(&id) else {
            return Ok(());
        };
        if !matches!(
            stream.state,
            StreamState::Idle | StreamState::Open | StreamState::HalfClosedLocal
        ) {
            self.reset(id, ErrorCode::StreamClosed);
            return Ok(());
        }
        let Ok((section, body)) =
            stream.checked_headers(&headers, self.role == Role::Server, false, end_stream)
        else {
            self.reset(id, ErrorCode::ProtocolError);
            return Ok(());
        };
        if !stream.apply(StreamEvent::RecvHeaders) {
            self.reset(id, ErrorCode::StreamClosed);
            return Ok(());
        }
        if end_stream {
            stream.apply(StreamEvent::RecvEndStream);
        }
        stream.commit_headers(section, body, false);
        self.events.push_back(Event::Headers {
            stream_id: id,
            headers,
            end_stream,
        });
        self.cleanup(id);
        Ok(())
    }

    fn on_rst_stream(&mut self, id: StreamId, payload: &[u8]) -> Result<(), Error> {
        if id == 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "RST_STREAM on stream 0"));
        }
        if payload.len() != 4 {
            return Err(self.fail(ErrorCode::FrameSizeError, "RST_STREAM length"));
        }
        if self.is_idle_stream(id) {
            return Err(self.fail(ErrorCode::ProtocolError, "RST_STREAM on idle stream"));
        }
        let code = ErrorCode::from_u32(u32::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]));
        if self.remove_stream(id) {
            self.events.push_back(Event::Reset {
                stream_id: id,
                error_code: code,
            });
        }
        Ok(())
    }

    fn on_settings(&mut self, id: StreamId, flags: Flags, payload: &[u8]) -> Result<(), Error> {
        if id != 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "SETTINGS on non-zero stream"));
        }
        if flags.has(Flags::ACK) {
            if !payload.is_empty() {
                return Err(self.fail(ErrorCode::FrameSizeError, "SETTINGS ACK with payload"));
            }
            self.on_settings_ack();
            return Ok(());
        }
        let settings = match decode_settings(payload) {
            Ok(s) => s,
            Err(_) => return Err(self.fail(ErrorCode::FrameSizeError, "SETTINGS length")),
        };
        for s in settings {
            let old_window = i64::from(self.peer.initial_window_size);
            if let Err(e) = self.peer.apply(s) {
                let code = match e {
                    Http2Error::Protocol(c) => c,
                    _ => ErrorCode::ProtocolError,
                };
                return Err(self.fail(code, "invalid SETTINGS value"));
            }
            if s.id == SettingId::InitialWindowSize {
                let delta = i64::from(self.peer.initial_window_size) - old_window;
                for stream in self.streams.values_mut() {
                    stream.send_window += delta;
                    if stream.send_window > MAX_WINDOW {
                        return Err(
                            self.fail(ErrorCode::FlowControlError, "stream window overflow")
                        );
                    }
                }
            }
        }
        self.write_frame(FrameType::Settings, Flags::ACK, 0, &[]);
        Ok(())
    }

    fn on_ping(&mut self, id: StreamId, flags: Flags, payload: &[u8]) -> Result<(), Error> {
        if id != 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "PING on non-zero stream"));
        }
        if payload.len() != 8 {
            return Err(self.fail(ErrorCode::FrameSizeError, "PING length"));
        }
        if !flags.has(Flags::ACK) {
            self.write_frame(FrameType::Ping, Flags::ACK, 0, payload);
        }
        Ok(())
    }

    fn on_goaway(&mut self, id: StreamId, payload: &[u8]) -> Result<(), Error> {
        if id != 0 {
            return Err(self.fail(ErrorCode::ProtocolError, "GOAWAY on non-zero stream"));
        }
        if payload.len() < 8 {
            return Err(self.fail(ErrorCode::FrameSizeError, "GOAWAY length"));
        }
        let last =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
        let code = ErrorCode::from_u32(u32::from_be_bytes([
            payload[4], payload[5], payload[6], payload[7],
        ]));
        self.goaway_received = true;
        self.events.push_back(Event::GoAway {
            last_stream_id: last,
            error_code: code,
        });
        // Locally-initiated streams above `last` were never processed by the peer.
        let refused: Vec<StreamId> = self
            .streams
            .keys()
            .copied()
            .filter(|s| !self.is_peer_initiated(*s) && *s > last)
            .collect();
        for s in refused {
            self.remove_stream(s);
            self.events.push_back(Event::Reset {
                stream_id: s,
                error_code: ErrorCode::RefusedStream,
            });
        }
        Ok(())
    }

    fn on_window_update(&mut self, id: StreamId, payload: &[u8]) -> Result<(), Error> {
        let inc = match decode_window_update(payload) {
            Ok(i) => i64::from(i),
            Err(_) => return Err(self.fail(ErrorCode::FrameSizeError, "WINDOW_UPDATE length")),
        };
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
        if self.is_idle_stream(id) {
            return Err(self.fail(ErrorCode::ProtocolError, "WINDOW_UPDATE on idle stream"));
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

    /// Open a new client stream by sending its request headers.
    pub fn open_stream(
        &mut self,
        headers: Vec<HeaderField>,
        end_stream: bool,
    ) -> Result<StreamId, Error> {
        if self.role != Role::Client {
            return Err(Error::WrongRole);
        }
        if let Some(e) = &self.failed {
            return Err(e.clone());
        }
        if self.goaway_received || self.goaway_sent {
            return Err(Error::GoingAway);
        }
        if self.next_local_id > MAX_WINDOW as u32 {
            return Err(Error::StreamIdExhausted);
        }
        // Without server push every open stream is ours.
        if self.streams.len() >= self.peer.max_concurrent_streams as usize {
            return Err(Error::StreamLimit);
        }
        // Validate on the candidate, so a rejection leaves no trace.
        let mut stream = self.new_stream();
        let id = self.next_local_id;
        stream.queue_headers(id, headers, true, end_stream)?;
        self.next_local_id += 2;
        self.streams.insert(id, stream);
        self.flush_stream(id);
        Ok(id)
    }

    /// Queue a header block (response headers or trailers) on a stream.
    pub fn send_headers(
        &mut self,
        id: StreamId,
        headers: Vec<HeaderField>,
        end_stream: bool,
    ) -> Result<(), Error> {
        let stream = self.streams.get_mut(&id).ok_or(Error::UnknownStream(id))?;
        stream.queue_headers(id, headers, self.role == Role::Client, end_stream)?;
        self.flush_stream(id);
        Ok(())
    }

    /// Queue body bytes on a stream. Data beyond the peer's flow-control
    /// window stays queued until a WINDOW_UPDATE arrives.
    ///
    /// The queue is unbounded: callers producing a stream of messages should
    /// check [`queued_send_bytes`](Self::queued_send_bytes) and stop producing
    /// above their own high-water mark.
    pub fn send_data(
        &mut self,
        id: StreamId,
        data: Vec<u8>,
        end_stream: bool,
    ) -> Result<(), Error> {
        let stream = self.streams.get_mut(&id).ok_or(Error::UnknownStream(id))?;
        if !stream.can_send() {
            return Err(Error::StreamClosed(id));
        }
        if stream.send_phase != Phase::Body {
            return Err(Error::InvalidHeaders("DATA before final headers"));
        }
        let body = stream
            .send_body
            .checked_data(data.len(), end_stream)
            .map_err(Error::InvalidHeaders)?;
        stream.send_body = body;
        if end_stream {
            stream.send_phase = Phase::End;
            stream.apply(StreamEvent::SendEndStream);
        }
        stream.outbound.push_back(Outbound::Data {
            buf: data,
            offset: 0,
            end_stream,
        });
        self.flush_stream(id);
        Ok(())
    }

    /// Abort a stream with RST_STREAM. Queued outbound data is discarded.
    ///
    /// Use [`reset_stream_after_flush`](Self::reset_stream_after_flush) to
    /// deliver queued data and trailers first.
    pub fn reset_stream(&mut self, id: StreamId, code: ErrorCode) -> Result<(), Error> {
        if !self.streams.contains_key(&id) {
            return Err(Error::UnknownStream(id));
        }
        self.reset(id, code);
        Ok(())
    }

    /// Send RST_STREAM once everything already queued on the stream (DATA and
    /// trailers) has been written, e.g. `RST_STREAM(NO_ERROR)` after a server
    /// sends its trailers before the client half-closed (RFC 9113 §8.1).
    ///
    /// If nothing is queued the stream is reset immediately. If the stream
    /// closes normally first (the peer ends its side), no RST_STREAM is sent.
    /// Until then, nothing more can be sent on the stream and further DATA
    /// from the peer is discarded rather than reported. The usual
    /// [`Event::Reset`] is emitted when the reset is finally sent; call
    /// [`reset_stream`](Self::reset_stream) to abort immediately instead.
    pub fn reset_stream_after_flush(&mut self, id: StreamId, code: ErrorCode) -> Result<(), Error> {
        let stream = self.streams.get_mut(&id).ok_or(Error::UnknownStream(id))?;
        stream.reset_after_flush = Some(code);
        self.flush_stream(id);
        Ok(())
    }

    /// Body bytes queued on the stream that have not been written to
    /// [`pending_output`](Self::pending_output) yet, typically because the
    /// peer's flow-control window is exhausted. `None` if the stream is
    /// unknown.
    pub fn queued_send_bytes(&self, id: StreamId) -> Option<usize> {
        self.streams.get(&id).map(Stream::queued_bytes)
    }

    /// How many more body bytes [`send_data`](Self::send_data) could accept on
    /// the stream and write out immediately under the current stream and
    /// connection send windows. The connection window is shared, so this is
    /// an upper bound when several streams are sending. `None` if the stream
    /// is unknown; `Some(0)` if it is closed for sending.
    pub fn send_capacity(&self, id: StreamId) -> Option<usize> {
        let stream = self.streams.get(&id)?;
        if !stream.can_send() {
            return Some(0);
        }
        let window = self.conn_send_window.min(stream.send_window).max(0) as usize;
        Some(window.saturating_sub(stream.queued_bytes()))
    }

    /// Next stream whose queued body data shrank because the peer granted
    /// flow-control credit (WINDOW_UPDATE or SETTINGS) during
    /// [`recv`](Self::recv), and which can still send. Each stream is reported
    /// at most once per call to `recv`; check
    /// [`queued_send_bytes`](Self::queued_send_bytes) to decide whether to
    /// resume a producer.
    pub fn poll_send_ready(&mut self) -> Option<StreamId> {
        while let Some(id) = self.send_ready.pop_first() {
            if self.streams.get(&id).is_some_and(Stream::can_send) {
                return Some(id);
            }
        }
        None
    }

    /// Credit `n` received body bytes on the stream back to the peer, after
    /// the application has consumed data delivered in [`Event::Data`].
    ///
    /// Only meaningful with [`FlowControl::Manual`]; a no-op otherwise. `n` is
    /// clamped to the bytes delivered and not yet released. Releasing on a
    /// stream that has closed or been reset is a no-op unless
    /// [`retain_receive_capacity`](Self::retain_receive_capacity) was enabled:
    /// then it releases retained connection credit, but sends no stream update.
    pub fn release_capacity(&mut self, id: StreamId, n: usize) {
        if self.config.flow_control != FlowControl::Manual || self.failed.is_some() {
            return;
        }
        let Some(stream) = self.streams.get_mut(&id) else {
            if let Some(held) = self.retained_credit.get_mut(&id) {
                let n = n.min(*held);
                *held -= n;
                if *held == 0 {
                    self.retained_credit.remove(&id);
                }
                self.release_connection(n);
            }
            return;
        };
        let n = n.min(stream.unreleased);
        if n == 0 {
            return;
        }
        stream.unreleased -= n;
        self.credit_received(id, n);
    }

    /// Received body bytes delivered on the stream and not yet released with
    /// [`release_capacity`](Self::release_capacity), including retained credit
    /// after removal. Always `Some(0)` for a known automatic-mode stream.
    pub fn unreleased_recv_bytes(&self, id: StreamId) -> Option<usize> {
        self.streams
            .get(&id)
            .map(|s| s.unreleased)
            .or_else(|| self.retained_credit.get(&id).copied())
    }

    /// Keep manual receive-credit ownership beyond protocol stream removal.
    /// Call before the stream can close (typically immediately after opening).
    /// Thereafter every delivered body byte must be released with
    /// [`release_capacity`](Self::release_capacity), even after completion or
    /// reset. Cancellation must release discarded data too. Retained entries
    /// do not count as active streams and disappear when their credit is zero.
    /// A no-op in automatic mode. Credit belongs to this connection and stream ID.
    pub fn retain_receive_capacity(&mut self, id: StreamId) -> Result<(), Error> {
        let stream = self.streams.get_mut(&id).ok_or(Error::UnknownStream(id))?;
        if self.config.flow_control == FlowControl::Manual {
            stream.retain_credit = true;
        }
        Ok(())
    }

    /// Start a graceful shutdown: no new peer streams are accepted, existing
    /// ones may complete.
    pub fn go_away(&mut self, code: ErrorCode) {
        if !self.goaway_sent {
            self.send_goaway(code);
        }
    }

    fn send_goaway(&mut self, code: ErrorCode) {
        self.goaway_sent = true;
        let mut payload = [0u8; 8];
        let last_stream_id = *self.goaway_last_stream_id.get_or_insert(self.last_peer_id);
        payload[..4].copy_from_slice(&last_stream_id.to_be_bytes());
        payload[4..].copy_from_slice(&(code as u32).to_be_bytes());
        self.write_frame(FrameType::GoAway, 0, 0, &payload);
    }

    fn fail(&mut self, code: ErrorCode, reason: &'static str) -> Error {
        let err = Error::Connection { code, reason };
        if self.failed.is_none() {
            self.send_goaway(code);
            self.failed = Some(err.clone());
        }
        err
    }

    fn reset(&mut self, id: StreamId, code: ErrorCode) {
        self.write_frame(FrameType::RstStream, 0, id, &(code as u32).to_be_bytes());
        if self.remove_stream(id) {
            self.events.push_back(Event::Reset {
                stream_id: id,
                error_code: code,
            });
        }
    }

    fn is_peer_initiated(&self, id: StreamId) -> bool {
        match self.role {
            Role::Server => is_client_initiated(id),
            Role::Client => is_server_initiated(id),
        }
    }

    fn is_idle_stream(&self, id: StreamId) -> bool {
        if self.is_peer_initiated(id) {
            id > self.last_peer_id
        } else {
            id >= self.next_local_id
        }
    }

    fn new_stream(&self) -> Stream {
        // Until the peer acknowledges our SETTINGS it still assumes the
        // default initial window; `on_settings_ack` applies the difference.
        let recv_window = if self.local_settings_acked {
            i64::from(self.config.initial_window_size)
        } else {
            DEFAULT_WINDOW
        };
        Stream::new(i64::from(self.peer.initial_window_size), recv_window)
    }

    fn on_settings_ack(&mut self) {
        if self.local_settings_acked {
            return;
        }
        self.local_settings_acked = true;
        let delta = i64::from(self.config.initial_window_size) - DEFAULT_WINDOW;
        for stream in self.streams.values_mut() {
            stream.recv_window += delta;
        }
    }

    /// Credit `n` received bytes back to the peer: on the stream while it can
    /// still receive, then on the connection.
    fn credit_received(&mut self, id: StreamId, n: usize) {
        if n == 0 {
            return;
        }
        if let Some(stream) = self.streams.get_mut(&id)
            && stream.can_recv()
        {
            stream.recv_window += n as i64;
            self.write_frame(
                FrameType::WindowUpdate,
                0,
                id,
                &encode_window_update(n as u32),
            );
        }
        self.release_connection(n);
    }

    /// Credit `n` bytes back to the peer's connection-level send window.
    fn release_connection(&mut self, n: usize) {
        if n == 0 || self.failed.is_some() {
            return;
        }
        self.conn_recv_window += n as i64;
        self.write_frame(
            FrameType::WindowUpdate,
            0,
            0,
            &encode_window_update(n as u32),
        );
    }

    /// Forget protocol state, returning credit unless independently retained.
    fn remove_stream(&mut self, id: StreamId) -> bool {
        let Some(stream) = self.streams.remove(&id) else {
            return false;
        };
        self.send_ready.remove(&id);
        if stream.retain_credit && stream.unreleased > 0 {
            self.retained_credit.insert(id, stream.unreleased);
        } else {
            self.release_connection(stream.unreleased);
        }
        true
    }

    fn cleanup(&mut self, id: StreamId) {
        if let Some(s) = self.streams.get(&id)
            && s.state == StreamState::Closed
            && s.outbound.is_empty()
        {
            self.remove_stream(id);
        }
    }

    fn flush_streams(&mut self) {
        let ids: Vec<(StreamId, usize)> = self
            .streams
            .iter()
            .filter(|(_, s)| !s.outbound.is_empty())
            .map(|(id, s)| (*id, s.queued_bytes()))
            .collect();
        for (id, before) in ids {
            self.flush_stream(id);
            if let Some(s) = self.streams.get(&id)
                && s.can_send()
                && s.queued_bytes() < before
            {
                self.send_ready.insert(id);
            }
        }
    }

    fn flush_stream(&mut self, id: StreamId) {
        let max_frame = self.peer.max_frame_size as usize;
        loop {
            let Some(stream) = self.streams.get_mut(&id) else {
                return;
            };
            let Some(front) = stream.outbound.front_mut() else {
                break;
            };
            match front {
                Outbound::Headers { fields, end_stream } => {
                    let end_stream = *end_stream;
                    let fields = core::mem::take(fields);
                    stream.outbound.pop_front();
                    let mut block = self.encoder.encode(&fields);
                    if self.encoder_update_pending {
                        block.insert(0, 0x20);
                        self.encoder_update_pending = false;
                    }
                    let mut chunks = block.chunks(max_frame).peekable();
                    let first = chunks.next().unwrap_or(&[]);
                    let mut flags = if end_stream { Flags::END_STREAM } else { 0 };
                    if chunks.peek().is_none() {
                        flags |= Flags::END_HEADERS;
                    }
                    self.write_frame(FrameType::Headers, flags, id, first);
                    while let Some(chunk) = chunks.next() {
                        let flags = if chunks.peek().is_none() {
                            Flags::END_HEADERS
                        } else {
                            0
                        };
                        self.write_frame(FrameType::Continuation, flags, id, chunk);
                    }
                }
                Outbound::Data {
                    buf,
                    offset,
                    end_stream,
                } => {
                    let remaining = buf.len() - *offset;
                    let window = self.conn_send_window.min(stream.send_window).max(0) as usize;
                    let n = remaining.min(window).min(max_frame);
                    if n == 0 && remaining > 0 {
                        break;
                    }
                    let chunk = buf[*offset..*offset + n].to_vec();
                    *offset += n;
                    let done = *offset == buf.len();
                    let flags = if done && *end_stream {
                        Flags::END_STREAM
                    } else {
                        0
                    };
                    stream.send_window -= n as i64;
                    if done {
                        stream.outbound.pop_front();
                    }
                    self.conn_send_window -= n as i64;
                    self.write_frame(FrameType::Data, flags, id, &chunk);
                }
            }
        }
        if let Some(s) = self.streams.get(&id)
            && s.outbound.is_empty()
            && s.state != StreamState::Closed
            && let Some(code) = s.reset_after_flush
        {
            self.reset(id, code);
            return;
        }
        self.cleanup(id);
    }

    fn write_frame(
        &mut self,
        frame_type: FrameType,
        flags: u8,
        stream_id: StreamId,
        payload: &[u8],
    ) {
        let header = FrameHeader {
            length: payload.len() as u32,
            frame_type,
            flags: Flags(flags),
            stream_id,
        };
        let start = self.output.len();
        self.output
            .resize(start + FRAME_HEADER_LEN + payload.len(), 0);
        // Payload sizes are bounded by the peer's max frame size by construction.
        let written =
            encode_frame(&header, payload, &mut self.output[start..], u32::MAX).unwrap_or(0);
        self.output.truncate(start + written);
    }

    fn encoded_header_limit(&self) -> usize {
        (self.config.max_header_list_size as usize)
            .saturating_mul(2)
            .saturating_add(1024)
    }

    fn local_max_frame_size(&self) -> u32 {
        DEFAULT_MAX_FRAME_SIZE
    }
}

fn strip_padding(flags: Flags, payload: &[u8]) -> Option<&[u8]> {
    if !flags.has(Flags::PADDED) {
        return Some(payload);
    }
    let (&pad, rest) = payload.split_first()?;
    let pad = usize::from(pad);
    if pad > rest.len() {
        return None;
    }
    Some(&rest[..rest.len() - pad])
}

#[cfg(test)]
mod tests;
