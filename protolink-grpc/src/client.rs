use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

use protolink_http2::{Config, Connection, Error, ErrorCode, Event, FlowControl, HeaderField};

use crate::compression::Compression;
use crate::inbound::Inbound;
use crate::status::decode_message;
use crate::{CallId, Code, DEFAULT_MAX_MESSAGE_SIZE, Next, Status, lpm, timeout};

/// Per-call options.
///
/// The default is no timeout (apart from [`ClientConfig::default_timeout`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CallOptions {
    /// Time the whole call may take, from the moment it is started until its
    /// final status. It is sent to the server as `grpc-timeout`, and the call
    /// fails locally with `DEADLINE_EXCEEDED` when it runs out (see
    /// [`Client::tick`]). Overrides [`ClientConfig::default_timeout`].
    pub timeout: Option<Duration>,
}

impl CallOptions {
    /// Options with no timeout of their own.
    pub const fn new() -> Self {
        Self { timeout: None }
    }

    /// Options with a call `timeout`.
    pub const fn timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
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
    /// `:authority` sent with every request.
    pub authority: String,
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
            authority: "localhost".into(),
            compression: Compression::NONE,
            default_timeout: None,
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
    inbound: Inbound,
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
    done: BTreeMap<CallId, Result<Vec<u8>, Status>>,
    max_message_size: usize,
    authority: String,
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
            max_message_size: config.max_message_size,
            authority: config.authority,
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
        let id = self.open(path, true, options)?;
        self.conn
            .send_data(id, self.frame(request), true)
            .map_err(|_| Status::internal("failed to queue request"))?;
        Ok(id)
    }

    /// Wrap a request message for sending, compressing it if configured.
    fn frame(&self, message: &[u8]) -> Vec<u8> {
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
        let mut headers = vec![
            field(":method", "POST"),
            field(":scheme", "http"),
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
            field("user-agent", "protolink"),
        ]);
        if let Some(codec) = self.compression.send {
            headers.push(field("grpc-encoding", codec.name()));
        }
        if let Some(accept) = self.compression.accept_header() {
            headers.push(field("grpc-accept-encoding", &accept));
        }
        let id = self.conn.open_stream(headers, false).map_err(|e| match e {
            Error::GoingAway => Status::unavailable("connection is going away"),
            _ => Status::unavailable("connection failed"),
        })?;
        self.calls.insert(
            id,
            Call {
                unary,
                deadline: timeout.map(|t| self.now.saturating_add(t)),
                headers_received: false,
                inbound: Inbound::new(self.max_message_size),
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
            .send_data(id, self.frame(message), false)
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
        match call.inbound.next(&mut self.conn, id) {
            Some(Ok(msg)) => return Some(Next::Message(msg)),
            Some(Err(status)) => {
                self.calls.remove(&id);
                // A message that fails to decompress is only found here, with
                // the stream possibly still open.
                if self.conn.has_stream(id) {
                    let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
                }
                return Some(Next::Done(Err(status)));
            }
            None => {}
        }
        let status = call.status.take()?;
        self.calls.remove(&id);
        Some(Next::Done(status))
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

    /// Result of a finished unary call, removing it from the client.
    pub fn take_response(&mut self, id: CallId) -> Option<Result<Vec<u8>, Status>> {
        self.done.remove(&id)
    }

    /// Whether the call is still waiting for its final status.
    pub fn is_pending(&self, id: CallId) -> bool {
        self.calls.get(&id).is_some_and(|c| c.status.is_none())
    }

    /// Abort an in-flight call (RST_STREAM CANCEL) and forget it.
    pub fn cancel(&mut self, id: CallId) {
        if self.calls.remove(&id).is_some() && self.conn.has_stream(id) {
            let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
        }
        self.done.remove(&id);
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

    /// Response bytes buffered for the call (messages not taken yet, plus a
    /// partial message). `None` if the call is unknown.
    pub fn buffered_response_bytes(&self, id: CallId) -> Option<usize> {
        self.calls.get(&id).map(|c| c.inbound.buffered())
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
        if let Some(code) = reset
            && self.conn.has_stream(id)
        {
            let _ = self.conn.reset_stream(id, code);
        }
        if !call.unary {
            call.status = Some(result);
            return;
        }
        let result = result.and_then(|()| {
            call.inbound.finish()?;
            match call.inbound.next(&mut self.conn, id) {
                Some(Ok(msg)) if !call.inbound.has_next() => Ok(msg),
                Some(Ok(_)) => Err(Status::internal(
                    "more than one response message for unary call",
                )),
                Some(Err(e)) => Err(e),
                None => Err(Status::internal("missing response message")),
            }
        });
        self.calls.remove(&id);
        self.done.insert(id, result);
    }

    fn on_event(&mut self, event: Event) {
        match event {
            Event::Headers {
                stream_id,
                headers,
                end_stream,
            } => {
                let Some(call) = self.calls.get_mut(&stream_id) else {
                    return;
                };
                if call.status.is_some() {
                    return;
                }
                let first = !call.headers_received;
                call.headers_received = true;
                if first {
                    let http = header(&headers, ":status").and_then(|s| s.parse::<u16>().ok());
                    if http != Some(200) {
                        let code = http.map_or(Code::Internal, Code::from_http_status);
                        let msg = alloc::format!("unexpected HTTP status {}", http.unwrap_or(0));
                        self.terminate(
                            stream_id,
                            Err(Status::new(code, msg)),
                            Some(ErrorCode::Cancel),
                        );
                        return;
                    }
                    if !end_stream {
                        // The message encoding is announced in the response
                        // headers, before any message.
                        match self
                            .compression
                            .decoder_for(header(&headers, "grpc-encoding"))
                        {
                            Ok(codec) => call.inbound.set_codec(codec),
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
                let result = trailers_status(&headers).and_then(|()| call.inbound.finish());
                self.terminate(stream_id, result, Some(ErrorCode::NoError));
            }
            Event::Data {
                stream_id,
                data,
                end_stream,
            } => {
                let Some(call) = self.calls.get_mut(&stream_id) else {
                    self.conn.release_capacity(stream_id, data.len());
                    return;
                };
                if call.status.is_some() {
                    self.conn.release_capacity(stream_id, data.len());
                    return;
                }
                call.inbound.push(&mut self.conn, stream_id, &data);
                if let Some(e) = call.inbound.error() {
                    let e = e.clone();
                    self.terminate(stream_id, Err(e), Some(ErrorCode::Cancel));
                } else if call.unary && call.inbound.message_count() > 1 {
                    self.terminate(
                        stream_id,
                        Err(Status::internal(
                            "more than one response message for unary call",
                        )),
                        Some(ErrorCode::Cancel),
                    );
                } else if end_stream {
                    self.terminate(
                        stream_id,
                        Err(Status::internal("response ended without trailers")),
                        None,
                    );
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

fn inactive() -> Status {
    Status::failed_precondition("call is not active")
}

fn trailers_status(headers: &[HeaderField]) -> Result<(), Status> {
    let Some(code) = header(headers, "grpc-status") else {
        return Err(Status::internal("missing grpc-status"));
    };
    let code = code.parse::<u8>().map_or(Code::Unknown, Code::from_u8);
    if code == Code::Ok {
        return Ok(());
    }
    let message = header(headers, "grpc-message")
        .map(decode_message)
        .unwrap_or_default();
    Err(Status::new(code, message))
}

fn header<'a>(headers: &'a [HeaderField], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name == name)
        .map(|h| h.value.as_str())
}

fn field(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}
