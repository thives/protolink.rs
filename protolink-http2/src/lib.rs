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
//! - receive-side window replenishment,
//! - RST_STREAM, GOAWAY and connection error handling.
//!
//! The [`Connection`] never performs I/O. Feed received bytes with
//! [`Connection::recv`], drain produced events with [`Connection::poll_event`]
//! and write [`Connection::pending_output`] to the transport.
//!
//! Server push is not supported (clients advertise `SETTINGS_ENABLE_PUSH = 0`).
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec::Vec;
use core::fmt;

use zerodds_hpack::{Decoder, Encoder};
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

/// Which side of the connection this endpoint is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Initiates streams (odd stream ids) and sends the connection preface.
    Client,
    /// Accepts streams.
    Server,
}

/// Local connection limits, advertised to the peer in the initial SETTINGS frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// `SETTINGS_MAX_CONCURRENT_STREAMS` (peer-initiated streams we accept).
    pub max_concurrent_streams: u32,
    /// `SETTINGS_INITIAL_WINDOW_SIZE` for our receive windows.
    pub initial_window_size: u32,
    /// `SETTINGS_MAX_HEADER_LIST_SIZE`. Larger header lists reset the stream.
    pub max_header_list_size: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 8,
            initial_window_size: 65_535,
            max_header_list_size: 8 * 1024,
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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection { code, reason } => write!(f, "connection error {code:?}: {reason}"),
            Self::UnknownStream(id) => write!(f, "unknown stream {id}"),
            Self::StreamClosed(id) => write!(f, "stream {id} is closed for sending"),
            Self::WrongRole => f.write_str("operation not supported for this role"),
            Self::GoingAway => f.write_str("connection is going away"),
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
}

impl Stream {
    fn new(send_window: i64) -> Self {
        Self {
            state: StreamState::Idle,
            send_window,
            outbound: VecDeque::new(),
        }
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
    decoder: Decoder,
    input: Vec<u8>,
    output: Vec<u8>,
    awaiting_preface: bool,
    awaiting_peer_settings: bool,
    streams: BTreeMap<StreamId, Stream>,
    conn_send_window: i64,
    next_local_id: StreamId,
    last_peer_id: StreamId,
    continuation: Option<PendingBlock>,
    events: VecDeque<Event>,
    goaway_sent: bool,
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
            decoder: Decoder::new(),
            input: Vec::new(),
            output: Vec::new(),
            awaiting_preface: role == Role::Server,
            awaiting_peer_settings: true,
            streams: BTreeMap::new(),
            conn_send_window: 65_535,
            next_local_id: if role == Role::Client { 1 } else { 2 },
            last_peer_id: 0,
            continuation: None,
            events: VecDeque::new(),
            goaway_sent: false,
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

        if self.awaiting_peer_settings {
            if frame_type != FrameType::Settings || flags.has(Flags::ACK) {
                return Err(self.fail(ErrorCode::ProtocolError, "first frame must be SETTINGS"));
            }
            self.awaiting_peer_settings = false;
        }
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
        let flow_len = payload.len() as u32;
        if flow_len > self.config.initial_window_size.max(65_535) {
            return Err(self.fail(ErrorCode::FlowControlError, "receive window exceeded"));
        }
        // The whole frame (padding included) counts against flow control; we
        // replenish the connection window immediately, so it never runs dry.
        if flow_len > 0 {
            self.write_frame(
                FrameType::WindowUpdate,
                0,
                0,
                &encode_window_update(flow_len),
            );
        }
        let data = match strip_padding(flags, payload) {
            Some(d) => d,
            None => return Err(self.fail(ErrorCode::ProtocolError, "invalid DATA padding")),
        };
        let end_stream = flags.has(Flags::END_STREAM);

        let Some(stream) = self.streams.get_mut(&id) else {
            if self.is_idle_stream(id) {
                return Err(self.fail(ErrorCode::ProtocolError, "DATA on idle stream"));
            }
            // Closed or reset stream: discard.
            return Ok(());
        };
        if !stream.can_recv() {
            self.reset(id, ErrorCode::StreamClosed);
            return Ok(());
        }
        if end_stream {
            stream.apply(StreamEvent::RecvEndStream);
        } else if flow_len > 0 {
            self.write_frame(
                FrameType::WindowUpdate,
                0,
                id,
                &encode_window_update(flow_len),
            );
        }
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
        pending.block.extend_from_slice(payload);
        if pending.block.len() > 2 * self.config.max_header_list_size as usize + 1024 {
            return Err(self.fail(ErrorCode::EnhanceYourCalm, "header block too large"));
        }
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
        // Always decode, even for streams we discard, to keep HPACK state in sync.
        let headers = match self.decoder.decode(&block) {
            Ok(h) => h,
            Err(_) => return Err(self.fail(ErrorCode::CompressionError, "HPACK decode failed")),
        };
        let list_size: usize = headers
            .iter()
            .map(|h| h.name.len() + h.value.len() + 32)
            .sum();

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
            let peer_open = self
                .streams
                .keys()
                .filter(|k| self.is_peer_initiated(**k))
                .count();
            if peer_open >= self.config.max_concurrent_streams as usize {
                self.reset(id, ErrorCode::RefusedStream);
                return Ok(());
            }
            self.streams
                .insert(id, Stream::new(i64::from(self.peer.initial_window_size)));
        }

        if list_size > self.config.max_header_list_size as usize {
            self.reset(id, ErrorCode::ProtocolError);
            return Ok(());
        }
        let Some(stream) = self.streams.get_mut(&id) else {
            return Ok(());
        };
        if !stream.apply(StreamEvent::RecvHeaders) {
            self.reset(id, ErrorCode::StreamClosed);
            return Ok(());
        }
        if end_stream {
            stream.apply(StreamEvent::RecvEndStream);
        }
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
        if self.streams.remove(&id).is_some() {
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
            self.streams.remove(&s);
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
        let id = self.next_local_id;
        self.next_local_id += 2;
        self.streams
            .insert(id, Stream::new(i64::from(self.peer.initial_window_size)));
        self.send_headers(id, headers, end_stream)?;
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
        if !stream.apply(StreamEvent::SendHeaders) {
            return Err(Error::StreamClosed(id));
        }
        if end_stream {
            stream.apply(StreamEvent::SendEndStream);
        }
        stream.outbound.push_back(Outbound::Headers {
            fields: headers,
            end_stream,
        });
        self.flush_stream(id);
        Ok(())
    }

    /// Queue body bytes on a stream. Data beyond the peer's flow-control
    /// window stays queued until a WINDOW_UPDATE arrives.
    pub fn send_data(
        &mut self,
        id: StreamId,
        data: Vec<u8>,
        end_stream: bool,
    ) -> Result<(), Error> {
        let stream = self.streams.get_mut(&id).ok_or(Error::UnknownStream(id))?;
        if !matches!(
            stream.state,
            StreamState::Open | StreamState::HalfClosedRemote
        ) {
            return Err(Error::StreamClosed(id));
        }
        if end_stream {
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
    pub fn reset_stream(&mut self, id: StreamId, code: ErrorCode) -> Result<(), Error> {
        if !self.streams.contains_key(&id) {
            return Err(Error::UnknownStream(id));
        }
        self.reset(id, code);
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
        payload[..4].copy_from_slice(&self.last_peer_id.to_be_bytes());
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
        if self.streams.remove(&id).is_some() {
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

    fn cleanup(&mut self, id: StreamId) {
        if let Some(s) = self.streams.get(&id)
            && s.state == StreamState::Closed
            && s.outbound.is_empty()
        {
            self.streams.remove(&id);
        }
    }

    fn flush_streams(&mut self) {
        let ids: Vec<StreamId> = self
            .streams
            .iter()
            .filter(|(_, s)| !s.outbound.is_empty())
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.flush_stream(id);
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
                    let block = self.encoder.encode(&fields);
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
