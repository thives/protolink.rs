use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::task::{Context, Poll};

use protolink_http2::{
    Config, Connection, Error, ErrorCode, Event, FlowControl, HeaderField, StreamId,
};

use crate::inbound::Inbound;
use crate::status::encode_message;
use crate::{CallId, DEFAULT_MAX_MESSAGE_SIZE, Handler, MethodKind, Next, Status, lpm};

/// Server configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConfig {
    /// HTTP/2 connection limits.
    ///
    /// The default uses [`FlowControl::Manual`] with a connection window of
    /// `max_concurrent_streams * initial_window_size`, so a call whose
    /// requests are not being consumed cannot starve the others. Received
    /// data is then bounded by about `connection_window_size` plus one
    /// partial message per call; lower `initial_window_size` or
    /// `max_concurrent_streams` on small targets.
    pub http2: Config,
    /// Largest accepted request and produced response message, in bytes.
    pub max_message_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let http2 = Config::default();
        Self {
            http2: Config {
                flow_control: FlowControl::Manual,
                connection_window_size: http2
                    .max_concurrent_streams
                    .saturating_mul(http2.initial_window_size),
                ..http2
            },
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

#[derive(Debug)]
struct Call {
    path: String,
    kind: MethodKind,
    inbound: Inbound,
    /// The client ended its side of the stream (END_STREAM received).
    half_closed: bool,
    /// `on_half_close` was delivered, after every buffered request message.
    end_delivered: bool,
    /// Request messages delivered to the handler.
    requests: usize,
    /// Response HEADERS were sent.
    headers_sent: bool,
}

/// Sans-IO gRPC server for one connection.
///
/// Feed received bytes and a [`Handler`] to [`recv`](Self::recv): unary
/// requests are dispatched synchronously and request messages of streaming
/// calls are delivered as they arrive. Then call [`poll`](Self::poll) to pull
/// streaming responses, and write [`pending_output`](Self::pending_output) to
/// the transport. Repeat `poll` and the write while `poll` produces output,
/// and call `poll` again whenever a handler wakes the waker passed to it.
///
/// Streaming responses are only pulled while the call has nothing waiting for
/// the client's flow-control window and less than one maximum-size message is
/// waiting in `pending_output`. Request messages of bidirectional calls are
/// delivered from `poll`, one at a time between response pulls, and not at
/// all while the call's responses are backed up; undelivered requests keep
/// their flow-control credit, which stops the client. Memory therefore stays
/// bounded however slowly the client reads.
#[derive(Debug)]
pub struct Server {
    conn: Connection,
    calls: BTreeMap<StreamId, Call>,
    /// Highest stream id that started a call; later HEADERS on lower ids are
    /// trailers or belong to finished calls.
    last_call: StreamId,
    max_message_size: usize,
}

impl Server {
    /// New server connection. Our HTTP/2 SETTINGS are queued immediately.
    pub fn new(config: ServerConfig) -> Self {
        Self {
            conn: Connection::server(config.http2),
            calls: BTreeMap::new(),
            last_call: 0,
            max_message_size: config.max_message_size,
        }
    }

    /// Process received bytes: dispatch complete unary requests and deliver
    /// request messages of server- and client-streaming calls to `handler`.
    /// Bidirectional requests are delivered by [`poll`](Self::poll).
    ///
    /// On `Err` the connection is unusable and every active streaming call
    /// has been cancelled; write the pending output (it contains a GOAWAY)
    /// and close the transport.
    pub fn recv<H: Handler + ?Sized>(
        &mut self,
        bytes: &[u8],
        handler: &mut H,
    ) -> Result<(), Error> {
        let result = self.conn.recv(bytes);
        while let Some(event) = self.conn.poll_event() {
            self.on_event(event, handler);
        }
        if result.is_err() {
            self.cancel_all(handler);
        }
        result
    }

    /// Drive every streaming call: deliver request messages held back by
    /// backpressure and pull responses from `handler` while the client can
    /// accept them. Handlers returning `Poll::Pending` wake `cx`'s waker when
    /// they have more to send.
    pub fn poll<H: Handler + ?Sized>(&mut self, handler: &mut H, cx: &mut Context<'_>) {
        let ids: Vec<StreamId> = self.calls.keys().copied().collect();
        for id in ids {
            self.drive(id, handler, Some(&mut *cx));
        }
    }

    /// Cancel every active call, e.g. because the transport closed. Streams
    /// are reset with `CANCEL` and streaming calls are reported to
    /// [`Handler::on_cancel`].
    pub fn cancel_all<H: Handler + ?Sized>(&mut self, handler: &mut H) {
        for (id, call) in core::mem::take(&mut self.calls) {
            if self.conn.has_stream(id) {
                let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
            }
            if call.kind != MethodKind::Unary {
                handler.on_cancel(&call.path, id);
            }
        }
    }

    /// Number of active calls.
    pub fn active_calls(&self) -> usize {
        self.calls.len()
    }

    /// Received bytes buffered for the call (request messages not delivered
    /// to the handler yet, plus a partial message). `None` if the call is not
    /// active.
    pub fn buffered_request_bytes(&self, id: CallId) -> Option<usize> {
        self.calls.get(&id).map(|c| c.inbound.buffered())
    }

    /// Response bytes queued on the call that wait for the client's
    /// flow-control window. `None` if the stream is closed.
    pub fn queued_response_bytes(&self, id: CallId) -> Option<usize> {
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

    /// Start a graceful shutdown (GOAWAY). In-flight calls still complete.
    pub fn shutdown(&mut self) {
        self.conn.go_away(ErrorCode::NoError);
    }

    /// The connection failed or finished shutting down.
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    fn on_event<H: Handler + ?Sized>(&mut self, event: Event, handler: &mut H) {
        match event {
            Event::Headers {
                stream_id,
                headers,
                end_stream,
            } => {
                if let Some(call) = self.calls.get_mut(&stream_id) {
                    // Request trailers: only END_STREAM matters.
                    call.half_closed |= end_stream;
                } else if stream_id > self.last_call {
                    self.last_call = stream_id;
                    match validate_request(&headers) {
                        Ok(path) => {
                            let kind = handler.method_kind(&path).unwrap_or(MethodKind::Unary);
                            self.calls.insert(
                                stream_id,
                                Call {
                                    path,
                                    kind,
                                    inbound: Inbound::new(self.max_message_size),
                                    half_closed: end_stream,
                                    end_delivered: false,
                                    requests: 0,
                                    headers_sent: false,
                                },
                            );
                        }
                        Err(http_status) => {
                            let _ = self.conn.send_headers(
                                stream_id,
                                vec![field(":status", http_status)],
                                true,
                            );
                            if !end_stream {
                                let _ = self
                                    .conn
                                    .reset_stream_after_flush(stream_id, ErrorCode::NoError);
                            }
                            return;
                        }
                    }
                } else {
                    return;
                }
                self.drive(stream_id, handler, None);
            }
            Event::Data {
                stream_id,
                data,
                end_stream,
            } => {
                let Some(call) = self.calls.get_mut(&stream_id) else {
                    // Finished call: nobody will consume this.
                    self.conn.release_capacity(stream_id, data.len());
                    return;
                };
                call.inbound.push(&mut self.conn, stream_id, &data);
                call.half_closed |= end_stream;
                self.drive(stream_id, handler, None);
            }
            Event::Reset { stream_id, .. } => {
                if let Some(call) = self.calls.remove(&stream_id)
                    && call.kind != MethodKind::Unary
                {
                    handler.on_cancel(&call.path, stream_id);
                }
            }
            Event::GoAway { .. } => {}
        }
    }

    /// Deliver what can be delivered and, with a context, pull responses,
    /// until neither makes progress or the call ends.
    ///
    /// Bidirectional calls alternate between delivering one request and
    /// pulling responses, and only while their response side is not backed
    /// up. A handler that answers each request therefore never holds more
    /// than about one request's worth of pending work, however fast the
    /// client sends; unconsumed requests stay in the call's buffer, where
    /// they hold back flow-control credit.
    fn drive<H: Handler + ?Sized>(
        &mut self,
        id: StreamId,
        handler: &mut H,
        mut cx: Option<&mut Context<'_>>,
    ) {
        loop {
            let Some(kind) = self.calls.get(&id).map(|c| c.kind) else {
                return;
            };
            let progressed = match kind {
                MethodKind::Unary => {
                    self.drive_unary(id, handler);
                    return;
                }
                MethodKind::BidiStreaming => {
                    // Requests are delivered from `poll` only, interleaved
                    // with responses.
                    let Some(cx) = cx.as_deref_mut() else {
                        return;
                    };
                    if self.backed_up(id) {
                        return;
                    }
                    let delivered = self.deliver(id, handler, 1);
                    let polled = self.poll_call(id, handler, cx);
                    delivered || polled
                }
                MethodKind::ServerStreaming | MethodKind::ClientStreaming => {
                    let delivered = self.deliver(id, handler, usize::MAX);
                    let polled = match cx.as_deref_mut() {
                        Some(cx) => self.poll_call(id, handler, cx),
                        None => false,
                    };
                    delivered || polled
                }
            };
            if !progressed {
                return;
            }
        }
    }

    /// Responses of the call wait for the client's window, or enough output
    /// waits for the transport.
    fn backed_up(&self, id: StreamId) -> bool {
        self.conn.queued_send_bytes(id).unwrap_or(0) > 0
            || self.conn.pending_output().len() > self.max_message_size + lpm::HEADER_LEN
    }

    fn drive_unary<H: Handler + ?Sized>(&mut self, id: StreamId, handler: &mut H) {
        let Some(call) = self.calls.get_mut(&id) else {
            return;
        };
        if let Some(e) = call.inbound.error() {
            let e = e.clone();
            self.finish(id, Err(e));
            return;
        }
        if call.inbound.message_count() > 1 {
            self.finish(
                id,
                Err(Status::internal(
                    "more than one request message for unary call",
                )),
            );
            return;
        }
        if !call.half_closed {
            return;
        }
        let truncated = call.inbound.finish();
        let message = call.inbound.next(&mut self.conn, id);
        let path = call.path.clone();
        let result = truncated.and_then(|()| match message {
            Some(Ok(msg)) => handler.call(&path, &msg).unwrap_or_else(|| {
                Err(Status::unimplemented(alloc::format!(
                    "unknown method {path}"
                )))
            }),
            Some(Err(e)) => Err(e),
            None => Err(Status::internal("missing request message")),
        });
        match result {
            Ok(reply) if reply.len() > self.max_message_size => {
                self.finish(
                    id,
                    Err(Status::resource_exhausted("response message too large")),
                );
            }
            Ok(reply) => {
                if self.send_message(id, &reply) {
                    self.finish(id, Ok(()));
                }
            }
            Err(status) => self.finish(id, Err(status)),
        }
    }

    /// Deliver up to `limit` buffered request messages (or the half-close)
    /// of a streaming call. Returns whether anything was delivered.
    fn deliver<H: Handler + ?Sized>(
        &mut self,
        id: StreamId,
        handler: &mut H,
        limit: usize,
    ) -> bool {
        let mut delivered = 0;
        loop {
            let Some(call) = self.calls.get_mut(&id) else {
                return delivered > 0;
            };
            if delivered >= limit {
                return true;
            }
            match call.inbound.next(&mut self.conn, id) {
                Some(Ok(msg)) => {
                    delivered += 1;
                    if call.kind == MethodKind::ServerStreaming && call.requests > 0 {
                        self.abort(
                            id,
                            Status::internal(
                                "more than one request message for server streaming call",
                            ),
                            handler,
                        );
                        return true;
                    }
                    call.requests += 1;
                    let path = call.path.clone();
                    if let Err(status) = handler.on_message(&path, id, &msg) {
                        self.finish(id, Err(status));
                        return true;
                    }
                }
                Some(Err(status)) => {
                    self.abort(id, status, handler);
                    return true;
                }
                None => {
                    if !call.half_closed || call.end_delivered {
                        return delivered > 0;
                    }
                    if let Err(status) = call.inbound.finish() {
                        self.abort(id, status, handler);
                        return true;
                    }
                    if call.kind == MethodKind::ServerStreaming && call.requests == 0 {
                        self.abort(id, Status::internal("missing request message"), handler);
                        return true;
                    }
                    call.end_delivered = true;
                    let path = call.path.clone();
                    if let Err(status) = handler.on_half_close(&path, id) {
                        self.finish(id, Err(status));
                    }
                    return true;
                }
            }
        }
    }

    /// Pull responses of a streaming call while the client can take them.
    /// Returns whether the handler produced anything.
    fn poll_call<H: Handler + ?Sized>(
        &mut self,
        id: StreamId,
        handler: &mut H,
        cx: &mut Context<'_>,
    ) -> bool {
        let mut progressed = false;
        loop {
            let Some(call) = self.calls.get(&id) else {
                return progressed;
            };
            let kind = call.kind;
            if kind != MethodKind::BidiStreaming && !call.end_delivered {
                return progressed;
            }
            if self.backed_up(id) {
                return progressed;
            }
            let path = call.path.clone();
            let Poll::Ready(next) = handler.poll_response(&path, id, cx) else {
                // The handler is working on the call: send the response
                // headers now, so that peers which wait for them before
                // streaming (or to see the call accepted) are not stalled.
                // Calls that fail immediately still get a trailers-only
                // response.
                if !self.send_headers(id) {
                    handler.on_cancel(&path, id);
                    return true;
                }
                return progressed;
            };
            progressed = true;
            match next {
                Next::Message(msg) => {
                    if msg.len() > self.max_message_size {
                        self.abort(
                            id,
                            Status::resource_exhausted("response message too large"),
                            handler,
                        );
                        return true;
                    }
                    if !self.send_message(id, &msg) {
                        handler.on_cancel(&path, id);
                        return true;
                    }
                    if kind == MethodKind::ClientStreaming {
                        self.finish(id, Ok(()));
                        return true;
                    }
                }
                Next::Done(Ok(())) if kind == MethodKind::ClientStreaming => {
                    self.finish(
                        id,
                        Err(Status::internal(
                            "client streaming call ended without a response",
                        )),
                    );
                    return true;
                }
                Next::Done(result) => {
                    self.finish(id, result);
                    return true;
                }
            }
        }
    }

    /// Queue one response message, preceded by the response headers if they
    /// were not sent yet. On failure the stream is gone and the call is
    /// removed.
    fn send_message(&mut self, id: StreamId, msg: &[u8]) -> bool {
        if !self.send_headers(id) {
            return false;
        }
        if self.conn.send_data(id, lpm::encode(msg), false).is_err() {
            self.calls.remove(&id);
            let _ = self.conn.reset_stream(id, ErrorCode::InternalError);
            return false;
        }
        true
    }

    /// Queue the response headers unless they were sent already. On failure
    /// the stream is gone and the call is removed.
    fn send_headers(&mut self, id: StreamId) -> bool {
        let Some(call) = self.calls.get_mut(&id) else {
            return false;
        };
        if call.headers_sent {
            return true;
        }
        call.headers_sent = true;
        let headers = vec![
            field(":status", "200"),
            field("content-type", "application/grpc"),
        ];
        if self.conn.send_headers(id, headers, false).is_err() {
            self.calls.remove(&id);
            let _ = self.conn.reset_stream(id, ErrorCode::InternalError);
            return false;
        }
        true
    }

    /// End the call with its final status: trailers after a response, or a
    /// trailers-only response. Queued messages are delivered first; if the
    /// client is still sending, the stream is then reset with `NO_ERROR`.
    fn finish(&mut self, id: StreamId, result: Result<(), Status>) {
        let Some(call) = self.calls.remove(&id) else {
            return;
        };
        let mut fields = Vec::new();
        if !call.headers_sent {
            fields.push(field(":status", "200"));
            fields.push(field("content-type", "application/grpc"));
        }
        match result {
            Ok(()) => fields.push(field("grpc-status", "0")),
            Err(status) => {
                fields.push(field("grpc-status", &status.code.as_u8().to_string()));
                if !status.message.is_empty() {
                    fields.push(field("grpc-message", &encode_message(&status.message)));
                }
            }
        }
        let _ = self.conn.send_headers(id, fields, true);
        if !call.half_closed {
            let _ = self.conn.reset_stream_after_flush(id, ErrorCode::NoError);
        }
    }

    /// End a streaming call the handler did not finish itself.
    fn abort<H: Handler + ?Sized>(&mut self, id: StreamId, status: Status, handler: &mut H) {
        let Some(path) = self.calls.get(&id).map(|c| c.path.clone()) else {
            return;
        };
        self.finish(id, Err(status));
        handler.on_cancel(&path, id);
    }
}

/// Validate request headers; returns the `:path` or an HTTP status to reply with.
fn validate_request(headers: &[HeaderField]) -> Result<String, &'static str> {
    let get = |name: &str| {
        headers
            .iter()
            .find(|h| h.name == name)
            .map(|h| h.value.as_str())
    };
    if get(":method") != Some("POST") {
        return Err("405");
    }
    let content_type = get("content-type").unwrap_or("");
    let grpc = content_type == "application/grpc"
        || content_type.starts_with("application/grpc+")
        || content_type.starts_with("application/grpc;");
    if !grpc {
        return Err("415");
    }
    match get(":path") {
        Some(p) if p.starts_with('/') => Ok(p.into()),
        _ => Err("400"),
    }
}

fn field(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}
