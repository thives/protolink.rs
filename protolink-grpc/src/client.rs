use alloc::collections::{BTreeMap, VecDeque};
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

use protolink_http2::{Config, Connection, Error, ErrorCode, Event, FlowControl, HeaderField};

use crate::compression::Compression;
use crate::fields::{field, header};
use crate::status::decode_message;
use crate::{
    CallId, Code, DEFAULT_MAX_MESSAGE_SIZE, Metadata, Next, Response, Status, lpm, timeout,
};

/// Per-call options.
///
/// The default is no timeout (apart from [`ClientConfig::default_timeout`]) and
/// no custom metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CallOptions {
    /// Time the whole call may take, from the moment it is started until its
    /// final status. It is sent to the server as `grpc-timeout`, and the call
    /// fails locally with `DEADLINE_EXCEEDED` when it runs out (see
    /// [`Client::tick`]). Overrides [`ClientConfig::default_timeout`].
    pub timeout: Option<Duration>,
    /// Custom metadata sent with the request headers. A `user-agent` entry
    /// replaces the default `protolink` one.
    pub metadata: Metadata,
}

impl CallOptions {
    /// Options with no timeout of their own and no metadata.
    pub const fn new() -> Self {
        Self {
            timeout: None,
            metadata: Metadata::new(),
        }
    }

    /// Options with a call `timeout`.
    pub const fn timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            metadata: Metadata::new(),
        }
    }

    /// Options with request `metadata`.
    pub const fn metadata(metadata: Metadata) -> Self {
        Self {
            timeout: None,
            metadata,
        }
    }

    /// These options with a call `timeout`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// These options with request `metadata`.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// The `:scheme` pseudo-header sent with every request.
///
/// This only labels requests. Selecting [`Scheme::Https`] does **not**
/// establish TLS: the caller must run the client over a TLS-backed transport.
/// Drivers never infer the scheme from the authority, port or transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// `:scheme: http`, for cleartext transports.
    Http,
    /// `:scheme: https`, for TLS-protected transports.
    Https,
}

impl Scheme {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

/// Client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// HTTP/2 connection limits.
    ///
    /// The default uses [`FlowControl::Manual`], so response bytes are only
    /// credited back to the server once the application has taken the
    /// messages they carry. With several concurrent streaming calls, raise
    /// `connection_window_size` to at least
    /// `calls * initial_window_size` so that one call whose responses are not
    /// being read cannot stall the others.
    pub http2: Config,
    /// Largest accepted response and sent request message, in bytes. Compressed
    /// messages count by their decompressed size.
    pub max_message_size: usize,
    /// Connection-wide retained response budget, including decompressed messages
    /// (with their five-byte prefixes) and partial messages, even after completion.
    /// Exceeding it fails the receiving call with `RESOURCE_EXHAUSTED`.
    /// Each message is charged at least its framed wire size. Decoder scratch
    /// space is additionally bounded by [`lpm::wire_limit`] of `max_message_size`
    /// plus one HTTP/2 DATA frame and one decompressed message. Defaults to 1 MiB.
    pub max_buffered_response_bytes: usize,
    /// Maximum calls retained by the client, including completed unread results
    /// and finished streaming metadata. Consume or cancel them to admit more
    /// calls. Defaults to 64.
    pub max_retained_calls: usize,
    /// `:authority` sent with every request.
    pub authority: String,
    /// `:scheme` sent with every request. Defaults to [`Scheme::Http`]. It does
    /// not enable TLS; see [`Scheme`].
    pub scheme: Scheme,
    /// Message compression. Off by default.
    ///
    /// If [`Compression::send`] is set, every request is compressed with it,
    /// so the server must support that encoding.
    pub compression: Compression,
    /// Timeout of calls that don't set [`CallOptions::timeout`]. `None` (the
    /// default) sends no `grpc-timeout`, so calls never expire by themselves.
    pub default_timeout: Option<Duration>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            http2: Config {
                flow_control: FlowControl::Manual,
                ..Config::default()
            },
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_buffered_response_bytes: 1024 * 1024,
            max_retained_calls: 64,
            authority: "localhost".into(),
            scheme: Scheme::Http,
            compression: Compression::NONE,
            default_timeout: None,
        }
    }
}

#[derive(Debug)]
struct RetainedMessage {
    message: Vec<u8>,
    credit: usize,
    wire_len: usize,
}

impl RetainedMessage {
    fn bytes(&self) -> usize {
        self.message
            .len()
            .saturating_add(lpm::HEADER_LEN)
            .max(self.wire_len)
    }
}

#[derive(Debug)]
struct Completed {
    result: Result<Response<Vec<u8>>, Status>,
    credit: usize,
    bytes: usize,
}

/// Credit ownership outlives HTTP/2 stream state. Decode before retention so
/// compressed responses cannot bypass the connection-wide memory budget.
#[derive(Debug)]
struct ResponseBuffer {
    decoder: lpm::Decoder,
    messages: VecDeque<RetainedMessage>,
    message_bytes: usize,
    credited: usize,
    held: usize,
}

impl ResponseBuffer {
    fn new(max_message_size: usize) -> Self {
        Self {
            decoder: lpm::Decoder::new(max_message_size),
            messages: VecDeque::new(),
            message_bytes: 0,
            credited: 0,
            held: 0,
        }
    }

    fn buffered(&self) -> usize {
        self.message_bytes.saturating_add(self.decoder.buffered())
    }

    fn push(
        &mut self,
        conn: &mut Connection,
        id: CallId,
        data: &[u8],
        limit: usize,
    ) -> Result<(), Status> {
        if data.len() > limit.saturating_sub(self.buffered()) {
            conn.release_capacity(id, data.len());
            return Err(Status::resource_exhausted(
                "response retention budget exceeded",
            ));
        }
        self.held += data.len();
        self.decoder.push(data);
        while let Some(item) = self.decoder.next_framed() {
            let (message, wire_len) = item?;
            let already = wire_len.min(self.credited);
            self.credited -= already;
            let credit = (wire_len - already).min(self.held);
            self.held -= credit;
            let message = RetainedMessage {
                message,
                credit,
                wire_len,
            };
            if message.bytes() > limit.saturating_sub(self.buffered()) {
                conn.release_capacity(id, credit);
                return Err(Status::resource_exhausted(
                    "response retention budget exceeded",
                ));
            }
            self.message_bytes += message.bytes();
            self.messages.push_back(message);
        }
        self.release_partial(conn, id);
        Ok(())
    }

    fn take_message(&mut self) -> Option<RetainedMessage> {
        let message = self.messages.pop_front()?;
        self.message_bytes -= message.bytes();
        Some(message)
    }

    fn release_partial(&mut self, conn: &mut Connection, id: CallId) {
        // A partial message may exceed the stream window. Its early credit is
        // still charged to the retention budget, but never released twice.
        if self.messages.is_empty() && self.held > 0 {
            conn.release_capacity(id, self.held);
            self.held = 0;
            self.credited = self.decoder.buffered();
        }
    }

    fn discard_partial(&mut self, conn: &mut Connection, id: CallId) {
        conn.release_capacity(id, core::mem::take(&mut self.held));
        self.credited = 0;
        self.decoder = lpm::Decoder::new(0);
    }

    fn discard(&mut self, conn: &mut Connection, id: CallId) {
        self.discard_partial(conn, id);
        self.message_bytes = 0;
        for message in self.messages.drain(..) {
            conn.release_capacity(id, message.credit);
        }
    }
}

#[derive(Debug)]
struct Call {
    unary: bool,
    /// When the call runs out of time, on the client's clock (see
    /// [`Client::tick`]).
    deadline: Option<Duration>,
    headers_received: bool,
    /// Metadata of the response headers, once received.
    initial: Option<Metadata>,
    /// Metadata of the response trailers, once received.
    trailers: Metadata,
    inbound: ResponseBuffer,
    /// Used only if the terminal response does not carry a gRPC status.
    fallback: Option<Status>,
    valid_content_type: bool,
    /// Final status, once known. Reported after every received message.
    status: Option<Result<(), Status>>,
    /// The request side was half-closed.
    send_closed: bool,
}

/// Sans-IO gRPC client for one connection.
///
/// Any number of calls may be active at once; each is identified by the
/// [`CallId`] returned when it starts. Write
/// [`pending_output`](Self::pending_output) to the transport and feed received
/// bytes to [`recv`](Self::recv).
///
/// - Unary calls: [`start_unary`](Self::start_unary), then
///   [`take_response`](Self::take_response).
/// - Streaming calls: [`start_streaming`](Self::start_streaming), then
///   [`send_message`](Self::send_message) (while
///   [`can_send`](Self::can_send)), [`close_send`](Self::close_send) and
///   [`try_next`](Self::try_next) until it returns [`Next::Done`].
///
/// Received messages stay buffered until taken, and their bytes are only
/// credited back to the server then (with
/// [`FlowControl::Manual`], the default), so a consumer that stops reading
/// stalls the server instead of growing memory.
///
/// # Deadlines
///
/// The client never reads a clock. The caller reports the time with
/// [`tick`](Self::tick), a monotonic [`Duration`] since any fixed point, and
/// learns when to call it next from [`next_deadline`](Self::next_deadline).
/// A call started with a timeout (see [`CallOptions`]) sends it as
/// `grpc-timeout` and fails with `DEADLINE_EXCEEDED` once a `tick` reaches its
/// deadline. Call `tick` before starting a call, so that its deadline counts
/// from the right moment. A streaming call still returns the messages it had
/// already received before the failure.
#[derive(Debug)]
pub struct Client {
    conn: Connection,
    calls: BTreeMap<CallId, Call>,
    done: BTreeMap<CallId, Completed>,
    max_buffered_response_bytes: usize,
    max_retained_calls: usize,
    /// Response metadata of finished streaming calls, until taken.
    finished_metadata: BTreeMap<CallId, (Option<Metadata>, Metadata)>,
    max_message_size: usize,
    authority: String,
    scheme: Scheme,
    compression: Compression,
    default_timeout: Option<Duration>,
    /// Latest time reported through [`Client::tick`].
    now: Duration,
}

impl Client {
    /// New client connection. The HTTP/2 preface is queued immediately.
    pub fn new(config: ClientConfig) -> Self {
        config.compression.debug_validate();
        Self {
            conn: Connection::client(config.http2),
            calls: BTreeMap::new(),
            done: BTreeMap::new(),
            max_buffered_response_bytes: config.max_buffered_response_bytes,
            max_retained_calls: config.max_retained_calls,
            finished_metadata: BTreeMap::new(),
            max_message_size: config.max_message_size,
            authority: config.authority,
            scheme: config.scheme,
            compression: config.compression,
            default_timeout: config.default_timeout,
            now: Duration::ZERO,
        }
    }

    /// Queue a unary call of `path` with the encoded `request` message.
    pub fn start_unary(&mut self, path: &str, request: &[u8]) -> Result<CallId, Status> {
        self.start_unary_with(path, request, &CallOptions::default())
    }

    /// [`start_unary`](Self::start_unary) with per-call `options`.
    ///
    /// Fails with `DEADLINE_EXCEEDED`, without sending anything, if the
    /// timeout is zero.
    pub fn start_unary_with(
        &mut self,
        path: &str,
        request: &[u8],
        options: &CallOptions,
    ) -> Result<CallId, Status> {
        if request.len() > self.max_message_size {
            return Err(Status::resource_exhausted("request message too large"));
        }
        let frame = self.frame(request)?;
        let id = self.open(path, true, options)?;
        self.conn
            .send_data(id, frame, true)
            .map_err(|_| Status::internal("failed to queue request"))?;
        Ok(id)
    }

    /// Wrap a request message for sending, compressing it if configured.
    fn frame(&self, message: &[u8]) -> Result<Vec<u8>, Status> {
        lpm::frame(message, self.compression.send, self.compression.min_size)
    }

    /// Start a streaming call of `path`. Requests are sent with
    /// [`send_message`](Self::send_message) and responses read with
    /// [`try_next`](Self::try_next).
    pub fn start_streaming(&mut self, path: &str) -> Result<CallId, Status> {
        self.start_streaming_with(path, &CallOptions::default())
    }

    /// [`start_streaming`](Self::start_streaming) with per-call `options`.
    ///
    /// The timeout covers the whole call, not each message. Fails with
    /// `DEADLINE_EXCEEDED`, without sending anything, if it is zero.
    pub fn start_streaming_with(
        &mut self,
        path: &str,
        options: &CallOptions,
    ) -> Result<CallId, Status> {
        self.open(path, false, options)
    }

    fn open(&mut self, path: &str, unary: bool, options: &CallOptions) -> Result<CallId, Status> {
        let timeout = options.timeout.or(self.default_timeout);
        if timeout == Some(Duration::ZERO) {
            return Err(Status::deadline_exceeded("timeout is zero"));
        }
        if self.calls.len() + self.done.len() + self.finished_metadata.len()
            >= self.max_retained_calls
            || self.retained_response_bytes() >= self.max_buffered_response_bytes
        {
            return Err(Status::resource_exhausted(
                "unread response retention limit",
            ));
        }
        let mut headers = vec![
            field(":method", "POST"),
            field(":scheme", self.scheme.as_str()),
            field(":path", path),
            field(":authority", &self.authority),
        ];
        // The spec asks for the timeout right after the pseudo-headers.
        if let Some(timeout) = timeout {
            headers.push(field("grpc-timeout", &timeout::format(timeout)));
        }
        headers.extend([
            field("content-type", "application/grpc"),
            field("te", "trailers"),
        ]);
        if !options.metadata.contains_key("user-agent") {
            headers.push(field("user-agent", "protolink"));
        }
        if let Some(codec) = self.compression.send {
            headers.push(field("grpc-encoding", codec.name()));
        }
        if let Some(accept) = self.compression.accept_header() {
            headers.push(field("grpc-accept-encoding", &accept));
        }
        // Custom metadata goes after the headers gRPC defines.
        options.metadata.append_fields(&mut headers);
        let id = self.conn.open_stream(headers, false).map_err(|e| match e {
            Error::GoingAway => Status::unavailable("connection is going away"),
            _ => Status::unavailable("connection failed"),
        })?;
        self.conn
            .retain_receive_capacity(id)
            .map_err(|_| Status::internal("failed to retain response capacity"))?;
        self.calls.insert(
            id,
            Call {
                unary,
                deadline: timeout.map(|t| self.now.saturating_add(t)),
                headers_received: false,
                initial: None,
                trailers: Metadata::new(),
                inbound: ResponseBuffer::new(self.max_message_size),
                fallback: None,
                valid_content_type: false,
                status: None,
                send_closed: unary,
            },
        );
        Ok(id)
    }

    /// Queue one request message on a streaming call.
    ///
    /// The message is always accepted; check [`can_send`](Self::can_send)
    /// first to keep at most one message per call waiting for the server's
    /// flow-control window. Messages sent after the server finished the call
    /// are discarded (its outcome is reported by
    /// [`try_next`](Self::try_next)).
    pub fn send_message(&mut self, id: CallId, message: &[u8]) -> Result<(), Status> {
        if message.len() > self.max_message_size {
            return Err(Status::resource_exhausted("request message too large"));
        }
        let call = self.calls.get(&id).ok_or_else(inactive)?;
        if call.status.is_some() {
            return Ok(());
        }
        if call.send_closed {
            return Err(Status::failed_precondition("request stream already closed"));
        }
        self.conn
            .send_data(id, self.frame(message)?, false)
            .map_err(|_| Status::internal("failed to queue request"))
    }

    /// Half-close a streaming call: no more request messages. Responses keep
    /// flowing.
    pub fn close_send(&mut self, id: CallId) -> Result<(), Status> {
        let call = self.calls.get_mut(&id).ok_or_else(inactive)?;
        if call.status.is_some() || call.send_closed {
            return Ok(());
        }
        call.send_closed = true;
        self.conn
            .send_data(id, Vec::new(), true)
            .map_err(|_| Status::internal("failed to close request stream"))
    }

    /// Whether [`send_message`](Self::send_message) can be called without
    /// queueing more than one message per call: nothing from earlier
    /// messages still waits for the server's flow-control window, or the
    /// call no longer sends at all.
    pub fn can_send(&self, id: CallId) -> bool {
        match self.calls.get(&id) {
            Some(call) if call.status.is_none() && !call.send_closed => {
                self.conn.queued_send_bytes(id).is_none_or(|n| n == 0)
            }
            _ => true,
        }
    }

    /// Next response of a streaming call: each received message in order,
    /// then [`Next::Done`] with the final status, after which the call is
    /// forgotten. `None` if more input is needed (or the call is unknown).
    pub fn try_next(&mut self, id: CallId) -> Option<Next<Vec<u8>>> {
        let call = self.calls.get_mut(&id)?;
        if call.unary {
            return None;
        }
        if let Some(message) = call.inbound.take_message() {
            self.conn.release_capacity(id, message.credit);
            call.inbound.release_partial(&mut self.conn, id);
            return Some(Next::Message(message.message));
        }
        let status = call.status.take()?;
        self.forget(id);
        Some(Next::Done(status))
    }

    /// Remove a streaming call, keeping its response metadata for
    /// [`take_metadata`](Self::take_metadata).
    fn forget(&mut self, id: CallId) {
        if let Some(call) = self.calls.remove(&id)
            && (call.initial.is_some() || !call.trailers.is_empty())
        {
            self.finished_metadata
                .insert(id, (call.initial, call.trailers));
        }
    }

    /// Metadata of the response headers of an active call, once the server has
    /// sent them. `None` before that, for a trailers-only response, and for
    /// unknown calls.
    pub fn response_headers(&self, id: CallId) -> Option<&Metadata> {
        self.calls.get(&id)?.initial.as_ref()
    }

    /// Response metadata of a streaming call that [`try_next`](Self::try_next)
    /// has reported as done: the headers' (`None` if the server sent none
    /// before the trailers) and the trailers'. Taking it removes it, so call
    /// this once after `Next::Done`; unknown or already taken gives no
    /// metadata.
    pub fn take_metadata(&mut self, id: CallId) -> (Option<Metadata>, Metadata) {
        self.finished_metadata.remove(&id).unwrap_or_default()
    }

    /// Process received bytes.
    ///
    /// On `Err` the connection is unusable and every in-flight call completes
    /// with `UNAVAILABLE`; write the pending output and close the transport.
    pub fn recv(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let result = self.conn.recv(bytes);
        while let Some(event) = self.conn.poll_event() {
            self.on_event(event);
        }
        if result.is_err() {
            self.fail_all(Status::unavailable("connection error"));
        }
        result
    }

    /// Report the current time and fail every call whose deadline has been
    /// reached with `DEADLINE_EXCEEDED`, resetting its stream with `CANCEL`.
    /// Write [`pending_output`](Self::pending_output) afterwards.
    ///
    /// `now` is a monotonic time since any fixed point, in the same unit and
    /// from the same clock on every call. Earlier values than one already
    /// reported are ignored.
    pub fn tick(&mut self, now: Duration) {
        self.now = self.now.max(now);
        let expired: Vec<CallId> = self
            .calls
            .iter()
            .filter(|(_, c)| c.status.is_none() && c.deadline.is_some_and(|d| d <= self.now))
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.terminate(
                id,
                Err(Status::deadline_exceeded("deadline exceeded")),
                Some(ErrorCode::Cancel),
            );
        }
    }

    /// Latest time reported through [`tick`](Self::tick).
    pub fn now(&self) -> Duration {
        self.now
    }

    /// When the earliest deadline of a call still waiting for its final status
    /// is reached, on the clock given to [`tick`](Self::tick). `None` if no
    /// such call has a deadline.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.calls
            .values()
            .filter(|c| c.status.is_none())
            .filter_map(|c| c.deadline)
            .min()
    }

    /// Result of a finished unary call, removing it from the client. A
    /// successful call carries the response metadata; a failed one has its
    /// trailers in [`Status::metadata`].
    pub fn take_response(&mut self, id: CallId) -> Option<Result<Response<Vec<u8>>, Status>> {
        let completed = self.done.remove(&id)?;
        self.conn.release_capacity(id, completed.credit);
        Some(completed.result)
    }

    /// Whether the call is still waiting for its final status.
    pub fn is_pending(&self, id: CallId) -> bool {
        self.calls.get(&id).is_some_and(|c| c.status.is_none())
    }

    /// Abort an in-flight call (RST_STREAM CANCEL), or discard an unread result.
    pub fn cancel(&mut self, id: CallId) {
        if let Some(mut call) = self.calls.remove(&id) {
            call.inbound.discard(&mut self.conn, id);
            let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
        }
        if let Some(completed) = self.done.remove(&id) {
            self.conn.release_capacity(id, completed.credit);
        }
        self.finished_metadata.remove(&id);
    }

    /// Mark every in-flight call as failed with `status`, e.g. when the
    /// transport closed. Streaming calls still return the messages received
    /// so far before the failure.
    pub fn fail_all(&mut self, status: Status) {
        let ids: Vec<CallId> = self.calls.keys().copied().collect();
        for id in ids {
            self.terminate(id, Err(status.clone()), None);
        }
    }

    /// Retention-budget bytes for the call: unread messages (the larger of
    /// decompressed framed size and wire size), plus partial message bytes.
    /// Includes completed unary results. `None` if the call is unknown.
    pub fn buffered_response_bytes(&self, id: CallId) -> Option<usize> {
        self.calls
            .get(&id)
            .map(|c| c.inbound.buffered())
            .or_else(|| self.done.get(&id).map(|c| c.bytes))
    }

    /// Total retention-budget bytes, including completed unread unary results.
    /// See [`buffered_response_bytes`](Self::buffered_response_bytes).
    pub fn retained_response_bytes(&self) -> usize {
        self.calls
            .values()
            .map(|c| c.inbound.buffered())
            .chain(self.done.values().map(|c| c.bytes))
            .fold(0, usize::saturating_add)
    }

    /// Request bytes queued on the call that wait for the server's
    /// flow-control window. `None` if the stream is closed.
    pub fn queued_request_bytes(&self, id: CallId) -> Option<usize> {
        self.conn.queued_send_bytes(id)
    }

    /// Bytes that must be written to the transport.
    pub fn pending_output(&self) -> &[u8] {
        self.conn.pending_output()
    }

    /// Mark `n` bytes of [`pending_output`](Self::pending_output) as written.
    pub fn consume_output(&mut self, n: usize) {
        self.conn.consume_output(n);
    }

    /// Take all pending output.
    pub fn take_output(&mut self) -> Vec<u8> {
        self.conn.take_output()
    }

    /// Whether output is waiting to be written.
    pub fn has_output(&self) -> bool {
        self.conn.has_output()
    }

    /// The connection failed or the server is shutting it down.
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    /// Record the final status of a call. Unary calls move to `done`. If our
    /// side of the stream is still open, it is reset with `reset`.
    fn terminate(&mut self, id: CallId, result: Result<(), Status>, reset: Option<ErrorCode>) {
        let Some(call) = self.calls.get_mut(&id) else {
            return;
        };
        if call.status.is_some() {
            return;
        }
        if let Some(code) = reset {
            let _ = self.conn.reset_stream(id, code);
        }
        if !call.unary {
            call.inbound.discard_partial(&mut self.conn, id);
            call.status = Some(result);
            return;
        }
        let result = result.and_then(|()| {
            call.inbound.decoder.finish()?;
            match call.inbound.messages.len() {
                1 => Ok(call.inbound.take_message().unwrap()),
                0 => Err(Status::internal("missing response message")),
                _ => Err(Status::internal(
                    "more than one response message for unary call",
                )),
            }
        });
        call.inbound.discard(&mut self.conn, id);
        let headers = call.initial.take().unwrap_or_default();
        let trailers = core::mem::take(&mut call.trailers);
        let (credit, bytes) = result.as_ref().map_or((0, 0), |m| (m.credit, m.bytes()));
        let result = result.map(|message| Response {
            message: message.message,
            headers,
            trailers,
        });
        self.calls.remove(&id);
        self.done.insert(
            id,
            Completed {
                result,
                credit,
                bytes,
            },
        );
    }

    fn on_event(&mut self, event: Event) {
        match event {
            Event::Headers {
                stream_id,
                headers,
                end_stream,
            } => {
                let http = header(&headers, ":status").and_then(|s| s.parse::<u16>().ok());
                // HTTP/2 validates informational responses; they do not start
                // the final gRPC response or contribute response metadata.
                if http.is_some_and(|status| (100..200).contains(&status)) {
                    return;
                }
                let Some(call) = self.calls.get_mut(&stream_id) else {
                    return;
                };
                if call.status.is_some() {
                    return;
                }
                let first = !call.headers_received;
                call.headers_received = true;
                if first {
                    call.valid_content_type =
                        header(&headers, "content-type").is_some_and(grpc_content_type);
                    if http != Some(200) {
                        let code = http.map_or(Code::Internal, Code::from_http_status);
                        let msg = alloc::format!("unexpected HTTP status {}", http.unwrap_or(0));
                        call.fallback = Some(Status::new(code, msg));
                    } else if !call.valid_content_type {
                        call.fallback = Some(Status::internal("invalid response content-type"));
                    }
                    if !end_stream {
                        call.initial = Some(Metadata::from_headers_lossy(&headers));
                        if !call.valid_content_type {
                            return;
                        }
                        // The message encoding is announced in the response
                        // headers, before any message.
                        match self
                            .compression
                            .decoder_for(header(&headers, "grpc-encoding"))
                        {
                            Ok(codec) => call.inbound.decoder.set_codec(codec),
                            Err(_) => self.terminate(
                                stream_id,
                                Err(Status::internal("unsupported response grpc-encoding")),
                                Some(ErrorCode::Cancel),
                            ),
                        }
                        return;
                    }
                }
                if !end_stream {
                    self.terminate(
                        stream_id,
                        Err(Status::internal("unexpected header block")),
                        Some(ErrorCode::ProtocolError),
                    );
                    return;
                }
                // Trailers-only responses carry all their metadata here.
                let trailers = Metadata::from_headers_lossy(&headers);
                let result = if header(&headers, "grpc-status").is_some() {
                    trailers_status(&headers).and_then(|()| {
                        if call.valid_content_type {
                            Ok(())
                        } else {
                            Err(Status::internal("invalid response content-type"))
                        }
                    })
                } else {
                    Err(call
                        .fallback
                        .clone()
                        .unwrap_or_else(|| Status::internal("missing grpc-status")))
                };
                let result = result
                    .and_then(|()| call.inbound.decoder.finish())
                    .map_err(|status| status.with_metadata(trailers.clone()));
                call.trailers = trailers;
                self.terminate(stream_id, result, Some(ErrorCode::NoError));
            }
            Event::Data {
                stream_id,
                data,
                end_stream,
            } => {
                let retained = self.retained_response_bytes();
                let Some(call) = self.calls.get_mut(&stream_id) else {
                    self.conn.release_capacity(stream_id, data.len());
                    return;
                };
                if call.status.is_some() {
                    self.conn.release_capacity(stream_id, data.len());
                    return;
                }
                if !call.headers_received {
                    self.conn.release_capacity(stream_id, data.len());
                    self.terminate(
                        stream_id,
                        Err(Status::internal("DATA before response headers")),
                        Some(ErrorCode::ProtocolError),
                    );
                    return;
                }
                if !call.valid_content_type {
                    self.conn.release_capacity(stream_id, data.len());
                    if end_stream {
                        let status = call
                            .fallback
                            .clone()
                            .unwrap_or_else(|| Status::internal("invalid response content-type"));
                        self.terminate(stream_id, Err(status), Some(ErrorCode::ProtocolError));
                    }
                    return;
                }
                let limit = self
                    .max_buffered_response_bytes
                    .saturating_sub(retained.saturating_sub(call.inbound.buffered()));
                if let Err(e) = call.inbound.push(&mut self.conn, stream_id, &data, limit) {
                    self.terminate(stream_id, Err(e), Some(ErrorCode::Cancel));
                } else if call.unary && call.inbound.messages.len() > 1 {
                    self.terminate(
                        stream_id,
                        Err(Status::internal(
                            "more than one response message for unary call",
                        )),
                        Some(ErrorCode::Cancel),
                    );
                } else if end_stream {
                    let status = call
                        .fallback
                        .clone()
                        .unwrap_or_else(|| Status::internal("response ended without trailers"));
                    self.terminate(stream_id, Err(status), Some(ErrorCode::ProtocolError));
                }
            }
            Event::Reset {
                stream_id,
                error_code,
            } => {
                self.terminate(
                    stream_id,
                    Err(Status::new(Code::from_h2(error_code), "stream reset")),
                    None,
                );
            }
            Event::GoAway { .. } => {}
        }
    }
}

fn grpc_content_type(value: &str) -> bool {
    let media = value.split(';').next().unwrap_or("").trim();
    media == "application/grpc"
        || media
            .strip_prefix("application/grpc+")
            .is_some_and(|suffix| {
                !suffix.is_empty()
                    && suffix
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
            })
}

fn inactive() -> Status {
    Status::failed_precondition("call is not active")
}

fn trailers_status(headers: &[HeaderField]) -> Result<(), Status> {
    let mut fields = headers.iter().filter(|h| h.name == "grpc-status");
    let Some(field) = fields.next() else {
        return Err(Status::internal("missing grpc-status"));
    };
    if fields.next().is_some() {
        return Err(Status::internal("duplicate grpc-status"));
    }
    let value = field.value.as_str();
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Status::internal("malformed grpc-status"));
    }
    let code = value.parse::<u8>().map_or(Code::Unknown, Code::from_u8);
    if code == Code::Ok {
        return Ok(());
    }
    let message = header(headers, "grpc-message")
        .map(decode_message)
        .unwrap_or_default();
    Err(Status::new(code, message))
}
