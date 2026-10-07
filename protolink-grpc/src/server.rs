use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::task::{Context, Poll};
use core::time::Duration;

use protolink_http2::{
    Config, Connection, Error, ErrorCode, Event, FlowControl, HeaderField, StreamId,
};

use crate::compression::{Codec, Compression};
use crate::fields::{field, header};
use crate::handler::unimplemented;
use crate::inbound::Inbound;
use crate::status::encode_message;
use crate::{
    CallContext, CallId, DEFAULT_MAX_MESSAGE_SIZE, Handler, Metadata, MethodKind, Next,
    ResponseMetadata, Status, lpm, timeout,
};

/// Maximum drive iterations per call and polling pass. Each iteration pulls
/// at most one response and delivers at most one buffered streaming request.
const CALL_QUANTUM: usize = 16;

/// Server configuration.
#[derive(Debug, Clone, Copy)]
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
    /// Compressed messages count by their decompressed size. Output-budget
    /// arithmetic saturates for limits near `usize::MAX`.
    pub max_message_size: usize,
    /// Message compression. Off by default.
    ///
    /// Requests in any [`Compression::accept`] encoding are decoded. Responses
    /// are compressed with [`Compression::send`] when the client accepts it.
    pub compression: Compression,
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
            compression: Compression::NONE,
        }
    }
}

#[derive(Debug)]
struct Call {
    path: String,
    kind: MethodKind,
    /// When the call runs out of time, on the server's clock (see
    /// [`Server::tick`]), from the request's `grpc-timeout`.
    deadline: Option<Duration>,
    inbound: Inbound,
    /// The client ended its side of the stream (END_STREAM received).
    half_closed: bool,
    /// `on_half_close` was delivered, after every buffered request message.
    end_delivered: bool,
    /// Request messages delivered to the handler.
    requests: usize,
    /// Encoding of the response messages, negotiated from the request.
    response_codec: Option<&'static dyn Codec>,
    /// Custom metadata of the request.
    request_metadata: Metadata,
    /// Response metadata set by the handler.
    response: ResponseMetadata,
}

impl Call {
    /// What handlers are told about this call.
    fn ctx(&mut self, id: StreamId) -> CallContext<'_> {
        CallContext::new(
            &self.path,
            id,
            self.deadline,
            &self.request_metadata,
            &mut self.response,
        )
    }
}

/// Sans-IO gRPC server for one connection.
///
/// Feed received bytes and a [`Handler`] to [`recv`](Self::recv): unary
/// requests are dispatched synchronously and request messages of streaming
/// calls are delivered as they arrive. Then call [`poll`](Self::poll) to pull
/// streaming responses, and write [`pending_output`](Self::pending_output) to
/// the transport. After consuming all written bytes and successfully flushing
/// the transport, acknowledge delivery with [`output_flushed`](Self::output_flushed).
/// Repeat `poll` and the write while `poll` produces output, and call `poll`
/// again whenever a handler wakes the waker passed to it.
///
/// Streaming responses are only pulled while the call has nothing waiting for
/// the client's flow-control window and less than one maximum-size message is
/// waiting in `pending_output`. Request messages of bidirectional calls are
/// delivered from `poll`, one at a time between response pulls, and not at
/// all while the call's responses are backed up; undelivered requests keep
/// their flow-control credit, which stops the client. Memory therefore stays
/// bounded however slowly the client reads.
///
/// # Deadlines
///
/// The server never reads a clock. The caller reports the time with
/// [`tick`](Self::tick), a monotonic [`Duration`] since any fixed point, and
/// learns when to call it next from [`next_deadline`](Self::next_deadline).
/// A request's `grpc-timeout` is a relative budget that starts when its
/// headers are received, at the time last reported to `tick`, so call `tick`
/// before [`recv`](Self::recv). Once a `tick` reaches a call's deadline, the
/// call ends with `DEADLINE_EXCEEDED` if its output can drain; otherwise an
/// immediate `CANCEL` reset discards flow-control-blocked output. Unfinished
/// streaming handlers are reported to [`Handler::on_cancel`]. Timeout trailers
/// newly generated by active expiry are delivered best-effort, without the old
/// deadline: the call can no longer succeed and its handler resources are freed.
/// Responses completed before expiry keep their deadlines armed until transport
/// delivery is acknowledged. Already-serialized bytes cannot safely be discarded
/// on expiry, so their deadline remains armed for stalled writes and flushes.
/// A call that is already expired when it would be
/// dispatched never reaches the handler. A unary handler that is running
/// can't be interrupted. A request without `grpc-timeout` has no deadline, and
/// a malformed one is answered with `INVALID_ARGUMENT`.
#[derive(Debug)]
pub struct Server {
    conn: Connection,
    calls: BTreeMap<StreamId, Call>,
    /// Handler-complete responses whose transport delivery is not acknowledged.
    draining: BTreeMap<StreamId, Option<Duration>>,
    /// First call visited in the previous polling pass.
    poll_cursor: StreamId,
    /// Highest stream id that started a call; later HEADERS on lower ids are
    /// trailers or belong to finished calls.
    last_call: StreamId,
    max_message_size: usize,
    compression: Compression,
    /// Latest time reported through [`Server::tick`].
    now: Duration,
}

impl Server {
    /// New server connection. Our HTTP/2 SETTINGS are queued immediately.
    pub fn new(config: ServerConfig) -> Self {
        config.compression.debug_validate();
        Self {
            conn: Connection::server(config.http2),
            calls: BTreeMap::new(),
            draining: BTreeMap::new(),
            poll_cursor: 0,
            last_call: 0,
            max_message_size: config.max_message_size,
            compression: config.compression,
            now: Duration::ZERO,
        }
    }

    /// Report the current time and end every call whose deadline has been
    /// reached with `DEADLINE_EXCEEDED`, or an immediate `CANCEL` reset if
    /// flow-controlled output is blocked. Unfinished streaming handlers are
    /// reported to [`Handler::on_cancel`]. Newly generated timeout trailers are
    /// best-effort and do not retain the expired deadline; previously completed
    /// responses still await delivery acknowledgment with their deadlines armed.
    /// Write [`pending_output`](Self::pending_output) afterwards.
    ///
    /// `now` is a monotonic time since any fixed point, in the same unit and
    /// from the same clock on every call. Earlier values than one already
    /// reported are ignored.
    pub fn tick<H: Handler + ?Sized>(&mut self, now: Duration, handler: &mut H) {
        self.now = self.now.max(now);
        let expired: Vec<StreamId> = self
            .calls
            .iter()
            .filter(|(_, c)| c.deadline.is_some_and(|d| d <= self.now))
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.expire(id, handler);
        }
        let expired: Vec<StreamId> = self
            .draining
            .iter()
            .filter(|(_, deadline)| deadline.is_some_and(|d| d <= self.now))
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if self
                .conn
                .queued_send_bytes(id)
                .is_some_and(|bytes| bytes > 0)
            {
                self.draining.remove(&id);
                let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
            }
            // Fully framed output may still be in a stalled write or flush.
            // Keep its expired obligation armed until delivery is acknowledged
            // or the driver cancels the connection.
        }
    }

    /// Latest time reported through [`tick`](Self::tick).
    pub fn now(&self) -> Duration {
        self.now
    }

    /// Earliest deadline of a handler-active or output-draining call, on the
    /// clock given to [`tick`](Self::tick). `None` if neither has a deadline.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.calls
            .values()
            .filter_map(|c| c.deadline)
            .chain(self.draining.values().filter_map(|deadline| *deadline))
            .min()
    }

    /// End the call with `DEADLINE_EXCEEDED`.
    fn expire<H: Handler + ?Sized>(&mut self, id: StreamId, handler: &mut H) {
        let Some(kind) = self.calls.get(&id).map(|c| c.kind) else {
            return;
        };
        // Trailers (and deferred resets) cannot pass flow-controlled DATA.
        // Discard the blocked remainder rather than retaining it indefinitely.
        if self.conn.queued_send_bytes(id).unwrap_or(0) > 0 {
            let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
            self.teardown(id, handler);
            return;
        }
        let status = Status::deadline_exceeded("deadline exceeded");
        if kind == MethodKind::Unary {
            self.finish(id, Err(status));
        } else {
            self.abort(id, status, handler);
        }
        // This call can no longer succeed; its new timeout trailers are
        // best-effort. Clear the output deadline only after cancellation so
        // the handler still observes the original deadline in its context.
        if let Some(deadline) = self.draining.get_mut(&id) {
            *deadline = None;
        }
    }

    fn is_expired(&self, id: StreamId) -> bool {
        self.calls
            .get(&id)
            .and_then(|c| c.deadline)
            .is_some_and(|d| d <= self.now)
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
    /// they have more to send. The starting call rotates each pass, and calls
    /// yield after a bounded quantum, waking `cx` if runnable work remains.
    pub fn poll<H: Handler + ?Sized>(&mut self, handler: &mut H, cx: &mut Context<'_>) {
        let mut ids: Vec<StreamId> = self.calls.keys().copied().collect();
        let start = ids.partition_point(|id| *id <= self.poll_cursor);
        ids.rotate_left(start);
        if let Some(id) = ids.first() {
            self.poll_cursor = *id;
        }
        for id in ids {
            self.drive(id, handler, Some(&mut *cx));
        }
    }

    /// Cancel every active call, e.g. because the transport closed. Streams
    /// are reset with `CANCEL` and streaming calls are reported to
    /// [`Handler::on_cancel`]. Handler-complete draining streams are also
    /// reset, without notifying their already-completed handlers again.
    pub fn cancel_all<H: Handler + ?Sized>(&mut self, handler: &mut H) {
        for (id, _) in core::mem::take(&mut self.draining) {
            let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
        }
        for (id, mut call) in core::mem::take(&mut self.calls) {
            if self.conn.has_stream(id) {
                let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
            }
            if call.kind != MethodKind::Unary {
                handler.on_cancel(&mut call.ctx(id));
            }
        }
    }

    /// Number of handler-active calls, excluding handler-complete streams
    /// awaiting transport delivery acknowledgment (whose deadlines remain armed).
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
    /// This does not acknowledge transport flushing or disarm response deadlines.
    pub fn consume_output(&mut self, n: usize) {
        self.conn.consume_output(n);
    }

    /// Acknowledge successful transport delivery after all pending output was
    /// written and the transport's flush completed successfully.
    ///
    /// Retires handler-complete responses only once their HTTP/2 streams have
    /// finished draining. Flow-control-blocked streams keep their deadlines.
    /// Never call this for a failed or pending flush. If output is still waiting
    /// to be written, no responses are retired.
    pub fn output_flushed(&mut self) {
        if !self.conn.has_output() {
            self.draining.retain(|id, _| self.conn.has_stream(*id));
        }
    }

    /// Take all pending output, transferring delivery responsibility to the caller.
    ///
    /// For sans-I/O use, this also acknowledges delivery of fully serialized
    /// responses, like [`output_flushed`](Self::output_flushed). The caller is
    /// responsible for transport stalls and failures after taking these bytes.
    /// Drivers that need the server's deadlines to cover writes and flushes must
    /// instead use [`pending_output`](Self::pending_output),
    /// [`consume_output`](Self::consume_output), and `output_flushed` after a
    /// successful flush.
    pub fn take_output(&mut self) -> Vec<u8> {
        let output = self.conn.take_output();
        self.output_flushed();
        output
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
                        Ok(path) if handler.is_unknown_method(&path) => {
                            // Nothing will serve this call: answer now
                            // instead of waiting for the client to finish
                            // sending.
                            self.send_status(stream_id, end_stream, Err(unimplemented(&path)));
                            return;
                        }
                        Ok(path) => {
                            // A malformed timeout can't be honored, and
                            // ignoring it would silently run the call
                            // without the deadline the client asked for.
                            let timeout = match header(&headers, "grpc-timeout") {
                                None => None,
                                Some(value) => match timeout::parse(value) {
                                    Some(timeout) => Some(timeout),
                                    None => {
                                        self.send_status(
                                            stream_id,
                                            end_stream,
                                            Err(Status::invalid_argument("malformed grpc-timeout")),
                                        );
                                        return;
                                    }
                                },
                            };
                            // Metadata that breaks the rules can't be handed to
                            // the handler as it was sent.
                            let request_metadata = match Metadata::from_headers(&headers) {
                                Ok(metadata) => metadata,
                                Err(e) => {
                                    self.send_status(
                                        stream_id,
                                        end_stream,
                                        Err(Status::invalid_argument(alloc::format!(
                                            "malformed request metadata: {e}"
                                        ))),
                                    );
                                    return;
                                }
                            };
                            // The request's encoding must be one we can
                            // decode; the client is told which ones we can.
                            let Ok(request_codec) = self
                                .compression
                                .decoder_for(header(&headers, "grpc-encoding"))
                            else {
                                let mut extra = Vec::new();
                                if let Some(accept) = self.compression.accept_header() {
                                    extra.push(field("grpc-accept-encoding", &accept));
                                }
                                self.send_status_with(
                                    stream_id,
                                    end_stream,
                                    Err(Status::unimplemented("unsupported grpc-encoding")),
                                    extra,
                                    ResponseMetadata::default(),
                                );
                                return;
                            };
                            let response_codec = self
                                .compression
                                .encoder_for(header(&headers, "grpc-accept-encoding"));
                            let mut inbound = Inbound::new(self.max_message_size);
                            inbound.set_codec(request_codec);
                            let kind = handler.method_kind(&path).unwrap_or(MethodKind::Unary);
                            self.calls.insert(
                                stream_id,
                                Call {
                                    path,
                                    kind,
                                    deadline: timeout.map(|t| self.now.saturating_add(t)),
                                    inbound,
                                    half_closed: end_stream,
                                    end_delivered: false,
                                    requests: 0,
                                    response_codec,
                                    request_metadata,
                                    response: ResponseMetadata::default(),
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
                // Locally deferred resets also produce this event, before
                // their serialized output has been flushed. Draining records
                // are retired by output_flushed, not protocol stream closure.
                if let Some(mut call) = self.calls.remove(&stream_id)
                    && call.kind != MethodKind::Unary
                {
                    handler.on_cancel(&mut call.ctx(stream_id));
                }
            }
            Event::GoAway { .. } => {}
        }
    }

    /// Deliver what can be delivered and, with a context, pull responses,
    /// until neither makes progress, the call ends, or its quantum is spent.
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
        for _ in 0..CALL_QUANTUM {
            let Some(kind) = self.calls.get(&id).map(|c| c.kind) else {
                return;
            };
            if self.is_expired(id) {
                self.expire(id, handler);
                return;
            }
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
                    // `recv` has no waker and must deliver all available
                    // non-bidi requests. Polling passes bound request work too.
                    let limit = if cx.is_some() { 1 } else { usize::MAX };
                    let delivered = self.deliver(id, handler, limit);
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
        if self.calls.contains_key(&id)
            && !self.backed_up(id)
            && let Some(cx) = cx
        {
            cx.waker().wake_by_ref();
        }
    }

    /// Responses of the call wait for the client's window, or enough output
    /// waits for the transport.
    fn backed_up(&self, id: StreamId) -> bool {
        self.conn.queued_send_bytes(id).unwrap_or(0) > 0
            || self.conn.pending_output().len()
                > self.max_wire_message().saturating_add(lpm::HEADER_LEN)
    }

    /// Largest response message on the wire, prefix excluded.
    fn max_wire_message(&self) -> usize {
        if self.compression.send.is_some() {
            lpm::wire_limit(self.max_message_size)
        } else {
            self.max_message_size
        }
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
        let mut ctx = call.ctx(id);
        let result = truncated.and_then(|()| match message {
            Some(Ok(msg)) => handler
                .call(&mut ctx, &msg)
                .unwrap_or_else(|| Err(unimplemented(&path))),
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
                if self.send_message(id, &reply, handler) {
                    self.finish(id, Ok(()));
                } else {
                    self.teardown(id, handler);
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
                    if let Err(status) = handler.on_message(&mut call.ctx(id), &msg) {
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
                    if let Err(status) = handler.on_half_close(&mut call.ctx(id)) {
                        self.finish(id, Err(status));
                    }
                    return true;
                }
            }
        }
    }

    /// Pull at most one response while the client can take it.
    /// Returns whether the handler produced anything.
    fn poll_call<H: Handler + ?Sized>(
        &mut self,
        id: StreamId,
        handler: &mut H,
        cx: &mut Context<'_>,
    ) -> bool {
        let Some(call) = self.calls.get(&id) else {
            return false;
        };
        let kind = call.kind;
        if kind != MethodKind::BidiStreaming && !call.end_delivered {
            return false;
        }
        if self.backed_up(id) {
            return false;
        }
        let polled = self
            .calls
            .get_mut(&id)
            .map(|call| handler.poll_response(&mut call.ctx(id), cx));
        let Some(Poll::Ready(next)) = polled else {
            // Send headers for pending handlers so peers waiting for call
            // acceptance can start streaming. Immediate failures remain
            // trailers-only responses.
            if !self.send_headers(id) {
                self.teardown(id, handler);
                return true;
            }
            return false;
        };
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
                if !self.send_message(id, &msg, handler) {
                    self.teardown(id, handler);
                    return true;
                }
                if kind == MethodKind::ClientStreaming {
                    self.finish(id, Ok(()));
                }
            }
            Next::Done(Ok(())) if kind == MethodKind::ClientStreaming => {
                self.finish(
                    id,
                    Err(Status::internal(
                        "client streaming call ended without a response",
                    )),
                );
            }
            Next::Done(result) => self.finish(id, result),
        }
        true
    }

    /// Queue one response message, preceded by the response headers if they
    /// were not sent yet. Framing errors end the call with a gRPC status;
    /// transport errors reset it for the caller to tear down.
    fn send_message<H: Handler + ?Sized>(
        &mut self,
        id: StreamId,
        msg: &[u8],
        handler: &mut H,
    ) -> bool {
        let codec = self.calls.get(&id).and_then(|call| call.response_codec);
        let framed = match lpm::frame(msg, codec, self.compression.min_size) {
            Ok(framed) => framed,
            Err(status) => {
                if self
                    .calls
                    .get(&id)
                    .is_some_and(|call| call.kind == MethodKind::Unary)
                {
                    self.finish(id, Err(status));
                } else {
                    self.abort(id, status, handler);
                }
                return false;
            }
        };
        if !self.send_headers(id) {
            return false;
        }
        if self.conn.send_data(id, framed, false).is_err() {
            let _ = self.conn.reset_stream(id, ErrorCode::InternalError);
            return false;
        }
        true
    }

    /// Queue the response headers unless they were sent already. On failure
    /// the stream is reset; the caller removes the call with
    /// [`teardown`](Self::teardown).
    fn send_headers(&mut self, id: StreamId) -> bool {
        let Some(call) = self.calls.get_mut(&id) else {
            return false;
        };
        // From here on the handler can no longer add initial metadata, which
        // is also how a call remembers that its headers went out.
        let Some(initial) = call.response.initial.take() else {
            return true;
        };
        let mut headers = vec![
            field(":status", "200"),
            field("content-type", "application/grpc"),
        ];
        if let Some(codec) = call.response_codec {
            headers.push(field("grpc-encoding", codec.name()));
        }
        if let Some(accept) = self.compression.accept_header() {
            headers.push(field("grpc-accept-encoding", &accept));
        }
        initial.append_fields(&mut headers);
        if self.conn.send_headers(id, headers, false).is_err() {
            let _ = self.conn.reset_stream(id, ErrorCode::InternalError);
            return false;
        }
        true
    }

    /// Forget a call whose stream failed. Streaming handlers are told.
    fn teardown<H: Handler + ?Sized>(&mut self, id: StreamId, handler: &mut H) {
        if let Some(mut call) = self.calls.remove(&id)
            && call.kind != MethodKind::Unary
        {
            handler.on_cancel(&mut call.ctx(id));
        }
    }

    /// End the call with its final status: trailers after a response, or a
    /// trailers-only response. Queued messages are delivered first; if the
    /// client is still sending, the stream is then reset with `NO_ERROR`.
    fn finish(&mut self, id: StreamId, result: Result<(), Status>) {
        self.end_call(id, result);
    }

    /// End a streaming call the handler did not finish itself.
    fn abort<H: Handler + ?Sized>(&mut self, id: StreamId, status: Status, handler: &mut H) {
        // The status and metadata go out first; the handler is told after,
        // with the call's original deadline in its context.
        if let Some(mut call) = self.end_call(id, Err(status)) {
            handler.on_cancel(&mut call.ctx(id));
        }
    }

    /// Remove the call, send its final status with the metadata its handler
    /// set, and keep its response draining until delivery is acknowledged.
    fn end_call(&mut self, id: StreamId, result: Result<(), Status>) -> Option<Call> {
        let mut call = self.calls.remove(&id)?;
        let response = core::mem::take(&mut call.response);
        self.send_status_with(id, call.half_closed, result, Vec::new(), response);
        self.draining.insert(id, call.deadline);
        Some(call)
    }

    /// Send the final status of a stream that never became a call: a
    /// trailers-only response. If the client is still sending, the stream is
    /// reset with `NO_ERROR` after the output flushed.
    fn send_status(&mut self, id: StreamId, half_closed: bool, result: Result<(), Status>) {
        self.send_status_with(
            id,
            half_closed,
            result,
            Vec::new(),
            ResponseMetadata::default(),
        );
    }

    /// [`send_status`](Self::send_status) with `extra` header fields in a
    /// trailers-only response (ignored if the headers were already sent), and
    /// the metadata the handler set. Initial metadata is only sent in a
    /// trailers-only response; otherwise it went out with the headers, and
    /// `response.initial` is `None`.
    fn send_status_with(
        &mut self,
        id: StreamId,
        half_closed: bool,
        result: Result<(), Status>,
        extra: Vec<HeaderField>,
        response: ResponseMetadata,
    ) {
        let mut fields = Vec::new();
        if let Some(initial) = &response.initial {
            fields.push(field(":status", "200"));
            fields.push(field("content-type", "application/grpc"));
            fields.extend(extra);
            initial.append_fields(&mut fields);
        }
        let status_metadata = match result {
            Ok(()) => {
                fields.push(field("grpc-status", "0"));
                None
            }
            Err(status) => {
                fields.push(field("grpc-status", &status.code.as_u8().to_string()));
                if !status.message.is_empty() {
                    fields.push(field("grpc-message", &encode_message(&status.message)));
                }
                Some(status.metadata)
            }
        };
        response.trailing.append_fields(&mut fields);
        if let Some(metadata) = status_metadata {
            metadata.append_fields(&mut fields);
        }
        if self.conn.send_headers(id, fields, true).is_err() {
            let _ = self.conn.reset_stream(id, ErrorCode::InternalError);
            return;
        }
        if !half_closed {
            let _ = self.conn.reset_stream_after_flush(id, ErrorCode::NoError);
        }
    }
}

#[cfg(test)]
#[path = "tests_server.rs"]
mod tests_server;

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
