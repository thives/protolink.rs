//! The sans-IO call lifecycle of a server connection, without handlers.
//!
//! [`Lifecycle`] speaks the gRPC server protocol for every call of one HTTP/2
//! connection and leaves what happens to each call to its caller:
//!
//! - a call is reported as [`CallEvent::Started`] once its request headers have
//!   been validated and the caller has resolved its path to a key `K`;
//! - request messages are released only when the caller takes them with
//!   [`take_request`](Lifecycle::take_request), so flow-control credit is
//!   returned only as the caller accepts them;
//! - response headers, messages and the final status are sent when the caller
//!   asks for them, so a caller can wait for a downstream before answering.
//!
//! [`Server`](crate::Server) is the [`Handler`](crate::Handler) adapter over this
//! type. Routers and relays drive it directly.
//!
//! # Events
//!
//! [`poll_event`](Lifecycle::poll_event) reports calls that started and calls
//! that ended without the caller finishing them:
//!
//! - [`CallEvent::Expired`]: the call's deadline passed. The protocol does not
//!   send the status itself, so that the caller can attach its trailing
//!   metadata; it must end the call with [`finish`](Lifecycle::finish).
//! - [`CallEvent::Cancelled`]: the call is gone. It was reset by the peer, its
//!   stream failed, or the connection closed. Nothing more is sent for it.
//!
//! Calls the caller finishes itself produce no event.

use alloc::collections::{BTreeMap, VecDeque};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::mem;
use core::time::Duration;

use protolink_http2::{Connection, Error, ErrorCode, Event, HeaderField, Step, StreamId};

use crate::compression::{Codec, Compression};
use crate::fields::{field, header};
use crate::handler::ResponseMetadata;
use crate::inbound::Inbound;
use crate::status::encode_message;
use crate::{CallId, Metadata, ServerConfig, Status, lpm, timeout};

/// The status of a call that a connection failure ended. An output limit is
/// an overload to act on, so it names the setting that raises it.
pub fn failure_status(error: &Error) -> Status {
    match error {
        Error::OutputLimitExceeded { limit, .. } => Status::unavailable(format!(
            "HTTP/2 output limit of {limit} bytes exceeded; raise http2.max_pending_output_bytes"
        )),
        _ => Status::unavailable("connection error"),
    }
}

/// Something that happened to a call which the caller must act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallEvent<K> {
    /// The request headers were accepted and their path resolved to `key`.
    /// The request messages, if any, are still to be taken.
    Started {
        /// The call's id.
        id: CallId,
        /// What the caller's resolver returned for the path.
        key: K,
        /// The method path, `/package.Service/Method`.
        path: String,
        /// The request's custom metadata.
        metadata: Metadata,
        /// When the call expires, on the clock given to [`Lifecycle::tick`].
        deadline: Option<Duration>,
    },
    /// The call's deadline passed. The caller must finish it with
    /// [`Lifecycle::finish`]; the protocol has not ended it.
    Expired {
        /// The call's id.
        id: CallId,
    },
    /// The call ended without the caller finishing it, and nothing more is
    /// sent for it: the peer reset it, its stream failed, the connection closed,
    /// or its output was blocked past its deadline.
    Cancelled {
        /// The call's id.
        id: CallId,
    },
}

/// State of one active call, as the lifecycle knows it.
#[derive(Debug)]
struct Call {
    /// When the call expires, on the server's clock.
    deadline: Option<Duration>,
    inbound: Inbound,
    /// The client ended its side of the stream (END_STREAM received).
    half_closed: bool,
    /// [`CallEvent::Expired`] was reported and the caller has not finished the
    /// call yet.
    expired: bool,
    /// Encoding of the response messages, negotiated from the request.
    response_codec: Option<&'static dyn Codec>,
    /// The response headers have been sent.
    headers_sent: bool,
}

/// Sans-IO gRPC server protocol for one connection, with the calls' routing
/// keys of type `K`.
///
/// Feed received bytes to [`recv`](Self::recv) with a resolver from request
/// paths to keys, then drain [`poll_event`](Self::poll_event). For each started
/// call, pull request messages with [`take_request`](Self::take_request) while
/// [`can_send`](Self::can_send) allows, send the response with
/// [`send_headers`](Self::send_headers) and [`send_message`](Self::send_message),
/// and end it with [`finish`](Self::finish). Write
/// [`pending_output`](Self::pending_output) to the transport as for
/// [`Server`](crate::Server).
///
/// Deadlines work as on [`Server`](crate::Server): the caller reports the time
/// with [`tick`](Self::tick) and learns the next one from
/// [`next_deadline`](Self::next_deadline).
#[derive(Debug)]
pub struct Lifecycle<K> {
    conn: Connection,
    calls: BTreeMap<StreamId, Call>,
    /// Calls finished by the caller whose output awaits transport delivery, with
    /// the deadline they keep armed until then.
    draining: BTreeMap<StreamId, Option<Duration>>,
    /// Highest stream id that started a call; later HEADERS on lower ids are
    /// trailers or belong to finished calls.
    last_call: StreamId,
    max_message_size: usize,
    compression: Compression,
    /// Latest time reported through [`tick`](Self::tick).
    now: Duration,
    events: VecDeque<CallEvent<K>>,
}

impl<K: Copy> Lifecycle<K> {
    /// New connection. Our HTTP/2 SETTINGS are queued immediately.
    pub fn new(config: ServerConfig) -> Self {
        config.compression.debug_validate();
        Self {
            conn: Connection::server(config.http2),
            calls: BTreeMap::new(),
            draining: BTreeMap::new(),
            last_call: 0,
            max_message_size: config.max_message_size,
            compression: config.compression,
            now: Duration::ZERO,
            events: VecDeque::new(),
        }
    }

    /// Process received bytes. Each request whose headers pass validation is
    /// resolved with `resolve`: `None` answers it `UNIMPLEMENTED` at once, and
    /// `Some(key)` starts a call, reported as [`CallEvent::Started`].
    ///
    /// On `Err` the connection is unusable and every active call has been
    /// cancelled; write the pending output and close the transport.
    pub fn recv(
        &mut self,
        bytes: &[u8],
        mut resolve: impl FnMut(&str) -> Option<K>,
    ) -> Result<(), Error> {
        let result = self.feed(bytes, &mut resolve);
        // Handling the events can latch a failure too, such as a credit release
        // that does not fit. The input is not successful then.
        let result = match (result, self.conn.failure()) {
            (Ok(()), Some(error)) => Err(error.clone()),
            (result, _) => result,
        };
        if result.is_err() {
            self.cancel_all();
        }
        result
    }

    /// Hand each event of `input` to the calls as soon as it is produced. Body
    /// bytes are borrowed from `input` and go straight to the call's decoder.
    fn feed(
        &mut self,
        mut input: &[u8],
        resolve: &mut impl FnMut(&str) -> Option<K>,
    ) -> Result<(), Error> {
        loop {
            let Step { consumed, event } = self.conn.recv_step(input)?;
            input = &input[consumed..];
            let Some(event) = event else {
                return Ok(());
            };
            self.on_event(event, resolve);
        }
    }

    /// The connection's terminal failure, if it has one: a protocol error or
    /// an output limit. Once set, every call has been cancelled and nothing
    /// more is serialized; retire the connection after its output is handled.
    pub fn failure(&self) -> Option<&Error> {
        self.conn.failure()
    }

    /// Adopt a failure that the connection latched while this lifecycle did
    /// work for it, such as queueing a response. Every call is cancelled once,
    /// as for a failed [`recv`](Self::recv), and the failure is returned.
    fn adopt_failure(&mut self) -> Option<Error> {
        let error = self.conn.failure()?.clone();
        self.cancel_all();
        Some(error)
    }

    /// Next event, in the order it happened.
    pub fn poll_event(&mut self) -> Option<CallEvent<K>> {
        self.events.pop_front()
    }

    /// Report the current time. Calls whose deadline has been reached get
    /// [`CallEvent::Expired`]; those whose output is blocked by flow control are
    /// reset at once and get [`CallEvent::Cancelled`] instead. Completed calls
    /// awaiting delivery are reset if their output is blocked.
    ///
    /// `now` is a monotonic time since any fixed point, in the same unit and
    /// from the same clock on every call. Earlier values than one already
    /// reported are ignored.
    pub fn tick(&mut self, now: Duration) {
        self.now = self.now.max(now);
        let due: Vec<StreamId> = self
            .calls
            .iter()
            .filter(|(_, c)| !c.expired && self.is_due(c.deadline))
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            self.expire(id);
        }
        let due: Vec<StreamId> = self
            .draining
            .iter()
            .filter(|(_, deadline)| self.is_due(**deadline))
            .map(|(id, _)| *id)
            .collect();
        for id in due {
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
        self.adopt_failure();
    }

    /// Whether `deadline` has been reached at the current time.
    fn is_due(&self, deadline: Option<Duration>) -> bool {
        deadline.is_some_and(|d| d <= self.now)
    }

    /// Latest time reported through [`tick`](Self::tick).
    pub fn now(&self) -> Duration {
        self.now
    }

    /// Largest request and response message, in bytes.
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    #[cfg(test)]
    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }

    #[cfg(test)]
    pub(crate) fn draining(&self) -> &BTreeMap<StreamId, Option<Duration>> {
        &self.draining
    }

    /// Earliest deadline of a call still running or of output awaiting
    /// delivery, on the clock given to [`tick`](Self::tick). Calls that have
    /// already expired are not included.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.calls
            .values()
            .filter(|c| !c.expired)
            .filter_map(|c| c.deadline)
            .chain(self.draining.values().filter_map(|deadline| *deadline))
            .min()
    }

    /// If the call's deadline has been reached, expires it as [`tick`](Self::tick)
    /// does, and returns `true`. A call that has already expired also returns
    /// `true`. Unknown calls return `false`.
    pub fn expire_if_due(&mut self, id: StreamId) -> bool {
        let Some(call) = self.calls.get(&id) else {
            return false;
        };
        if call.expired {
            return true;
        }
        if !self.is_due(call.deadline) {
            return false;
        }
        self.expire(id);
        self.adopt_failure();
        true
    }

    fn expire(&mut self, id: StreamId) {
        // Trailers (and deferred resets) cannot pass flow-controlled DATA.
        // Discard the blocked remainder rather than retaining it indefinitely.
        if self.conn.queued_send_bytes(id).unwrap_or(0) > 0 {
            let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
            self.calls.remove(&id);
            self.events.push_back(CallEvent::Cancelled { id });
            return;
        }
        if let Some(call) = self.calls.get_mut(&id) {
            call.expired = true;
        }
        self.events.push_back(CallEvent::Expired { id });
    }

    /// Whether the call is active: started, and not yet finished, cancelled or
    /// reset.
    pub fn has_call(&self, id: CallId) -> bool {
        self.calls.contains_key(&id)
    }

    /// Number of active calls.
    pub fn active_calls(&self) -> usize {
        self.calls.len()
    }

    /// When the call expires, on the clock given to [`tick`](Self::tick). `None`
    /// for calls without a deadline and unknown calls.
    pub fn deadline(&self, id: CallId) -> Option<Duration> {
        self.calls.get(&id)?.deadline
    }

    /// Whether the client has half-closed the request side of the call.
    pub fn is_half_closed(&self, id: CallId) -> bool {
        self.calls.get(&id).is_some_and(|c| c.half_closed)
    }

    /// Complete request messages buffered for the call and not taken yet.
    pub fn request_count(&self, id: CallId) -> usize {
        self.calls.get(&id).map_or(0, |c| c.inbound.message_count())
    }

    /// The decoding error of the call's request, if it has one.
    pub fn request_error(&self, id: CallId) -> Option<Status> {
        self.calls.get(&id)?.inbound.error().cloned()
    }

    /// Checks that the client's request was not cut short: fails with `INTERNAL`
    /// if a partial message was left behind, or with the decoding error. Call it
    /// once the request is half-closed.
    pub fn request_finish(&self, id: CallId) -> Result<(), Status> {
        self.calls.get(&id).map_or(Ok(()), |c| c.inbound.finish())
    }

    /// Take the next request message of the call, returning its credit to the
    /// peer. `Ok(None)` when no complete message is buffered (more may still
    /// arrive, unless [`is_half_closed`](Self::is_half_closed)). `Err` for a
    /// message that cannot be decoded, which ends the call.
    pub fn take_request(&mut self, id: CallId) -> Result<Option<Vec<u8>>, Status> {
        let Some(call) = self.calls.get_mut(&id) else {
            return Ok(None);
        };
        let taken = match call.inbound.next(&mut self.conn, id) {
            Some(Ok(message)) => Ok(Some(message)),
            Some(Err(status)) => Err(status),
            None => Ok(None),
        };
        // Releasing the credit of the message can fail the connection, in which
        // case the call is over whatever the message was.
        match self.adopt_failure() {
            Some(error) => Err(failure_status(&error)),
            None => taken,
        }
    }

    /// Whether the call can take more response data without queueing it behind
    /// flow control or past the transport's buffering limit.
    pub fn can_send(&self, id: CallId) -> bool {
        self.conn.queued_send_bytes(id).unwrap_or(0) == 0
            && self.conn.pending_output().len()
                <= self.max_wire_message().saturating_add(lpm::HEADER_LEN)
    }

    /// Largest response message on the wire, prefix excluded.
    fn max_wire_message(&self) -> usize {
        if self.compression.send.is_some() {
            lpm::wire_limit(self.max_message_size)
        } else {
            self.max_message_size
        }
    }

    /// Send the response headers of the call, with `initial` as its metadata.
    /// A no-op once they have been sent. Returns `false` if the stream failed;
    /// the call is then gone and a [`CallEvent::Cancelled`] is queued.
    pub fn send_headers(&mut self, id: CallId, initial: Metadata) -> bool {
        let Some(call) = self.calls.get_mut(&id) else {
            return false;
        };
        if call.headers_sent {
            return true;
        }
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
        call.headers_sent = true;
        if self.conn.send_headers(id, headers, false).is_err() {
            self.fail_stream(id);
            self.adopt_failure();
            return false;
        }
        true
    }

    /// Queue one response message. The response headers must have been sent.
    ///
    /// The message is limited by its uncompressed size, as the caller sees it,
    /// so a compressible message cannot exceed
    /// [`max_message_size`](Self::max_message_size) on the wire either. Fails
    /// with the status to end the call with if the message is too large or
    /// cannot be encoded; the call is still active then, and the caller must end
    /// it. If the stream failed, the call is gone and a [`CallEvent::Cancelled`]
    /// is queued.
    pub fn send_message(&mut self, id: CallId, message: &[u8]) -> Result<(), Status> {
        let Some(call) = self.calls.get(&id) else {
            return Err(Status::failed_precondition("call is not active"));
        };
        if !call.headers_sent {
            return Err(Status::failed_precondition(
                "response headers not sent before the message",
            ));
        }
        if message.len() > self.max_message_size {
            return Err(Status::resource_exhausted("response message too large"));
        }
        let framed = lpm::frame(message, call.response_codec, self.compression.min_size)?;
        if self.conn.send_data(id, framed, false).is_err() {
            self.fail_stream(id);
            return Err(match self.adopt_failure() {
                Some(error) => failure_status(&error),
                None => Status::internal("failed to queue response message"),
            });
        }
        Ok(())
    }

    /// End the call with `result`. `response` carries the metadata to send: its
    /// initial metadata goes out only if the headers have not been sent (then
    /// the response is trailers-only), and its trailing metadata always goes
    /// out with the status. Queued messages are delivered first. Does nothing
    /// for unknown calls.
    pub fn finish(
        &mut self,
        id: CallId,
        result: Result<(), Status>,
        mut response: ResponseMetadata,
    ) {
        let Some(call) = self.calls.remove(&id) else {
            return;
        };
        // Sent headers can't be sent again: the status is trailers only.
        if call.headers_sent {
            response.initial = None;
        }
        // A call that expired may not keep its deadline armed for delivery:
        // the status it carries is already the deadline's.
        let deadline = if call.expired { None } else { call.deadline };
        self.send_status(id, call.half_closed, result, Vec::new(), response);
        // Output that can never be delivered is not waited for.
        if self.conn.failure().is_none() {
            self.draining.insert(id, deadline);
        }
        self.adopt_failure();
    }

    /// Cancel every active call, e.g. because the transport closed: streams are
    /// reset with `CANCEL` and each call gets [`CallEvent::Cancelled`].
    /// Completed calls awaiting delivery are reset too, without an event.
    pub fn cancel_all(&mut self) {
        for (id, _) in mem::take(&mut self.draining) {
            let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
        }
        for (id, _) in mem::take(&mut self.calls) {
            if self.conn.has_stream(id) {
                let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
            }
            self.events.push_back(CallEvent::Cancelled { id });
        }
    }

    /// Received bytes buffered for the call (request messages not taken yet,
    /// plus a partial message). `None` if the call is not active.
    pub fn buffered_request_bytes(&self, id: CallId) -> Option<usize> {
        self.calls.get(&id).map(|c| c.inbound.buffered())
    }

    /// Response bytes queued on the stream that wait for the client's
    /// flow-control window. `None` if the stream is closed.
    pub fn queued_response_bytes(&self, id: CallId) -> Option<usize> {
        self.conn.queued_send_bytes(id)
    }

    /// Bytes that must be written to the transport.
    pub fn pending_output(&self) -> &[u8] {
        self.conn.pending_output()
    }

    /// Mark `n` bytes of [`pending_output`](Self::pending_output) as written.
    /// This does not acknowledge transport flushing.
    pub fn consume_output(&mut self, n: usize) {
        self.conn.consume_output(n);
    }

    /// Acknowledge successful transport delivery after all pending output was
    /// written and the transport's flush completed successfully. Retires
    /// finished calls whose streams have drained. Never call it for a failed or
    /// pending flush.
    pub fn output_flushed(&mut self) {
        if !self.conn.has_output() {
            self.draining.retain(|id, _| self.conn.has_stream(*id));
        }
    }

    /// Take all pending output, transferring delivery responsibility to the
    /// caller. Acknowledges delivery like [`output_flushed`](Self::output_flushed).
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
        self.adopt_failure();
    }

    /// The connection failed or finished shutting down.
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    fn on_event(&mut self, event: Event<'_>, resolve: &mut impl FnMut(&str) -> Option<K>) {
        match event {
            Event::Headers {
                stream_id,
                headers,
                end_stream,
            } => self.on_headers(stream_id, headers, end_stream, resolve),
            Event::Data { stream_id, data } => {
                let Some(call) = self.calls.get_mut(&stream_id) else {
                    // Finished call: nobody will consume this.
                    self.conn.release_capacity(stream_id, data.len());
                    return;
                };
                call.inbound.push(&data);
            }
            Event::DataEnd {
                stream_id,
                end_stream,
            } => {
                if let Some(call) = self.calls.get_mut(&stream_id) {
                    call.inbound.finish_frame(&mut self.conn, stream_id);
                    call.half_closed |= end_stream;
                }
            }
            Event::Reset { stream_id, .. } => {
                // Locally deferred resets also produce this event, before
                // their serialized output has been flushed. Draining records
                // are retired by output_flushed, not protocol stream closure.
                if self.calls.remove(&stream_id).is_some() {
                    self.events
                        .push_back(CallEvent::Cancelled { id: stream_id });
                }
            }
            Event::GoAway { .. } => {}
        }
    }

    fn on_headers(
        &mut self,
        stream_id: StreamId,
        headers: Vec<HeaderField>,
        end_stream: bool,
        resolve: &mut impl FnMut(&str) -> Option<K>,
    ) {
        // A call that starts on a failed connection would never be answered.
        if self.conn.failure().is_some() {
            return;
        }
        if let Some(call) = self.calls.get_mut(&stream_id) {
            // Request trailers: only END_STREAM matters.
            call.half_closed |= end_stream;
            return;
        }
        if stream_id <= self.last_call {
            return;
        }
        self.last_call = stream_id;
        let path = match validate_request(&headers) {
            Ok(path) => path,
            Err(http_status) => {
                let _ =
                    self.conn
                        .send_headers(stream_id, vec![field(":status", http_status)], true);
                if !end_stream {
                    let _ = self
                        .conn
                        .reset_stream_after_flush(stream_id, ErrorCode::NoError);
                }
                return;
            }
        };
        // Nothing will serve the call: answer now instead of waiting for the
        // client to finish sending.
        let Some(key) = resolve(&path) else {
            let status = Status::unimplemented(format!("unknown method {path}"));
            self.send_status(
                stream_id,
                end_stream,
                Err(status),
                Vec::new(),
                ResponseMetadata::default(),
            );
            return;
        };
        // A malformed timeout can't be honored, and ignoring it would silently
        // run the call without the deadline the client asked for.
        let timeout = match header(&headers, "grpc-timeout") {
            None => None,
            Some(value) => match timeout::parse(value) {
                Some(timeout) => Some(timeout),
                None => {
                    self.send_status(
                        stream_id,
                        end_stream,
                        Err(Status::invalid_argument("malformed grpc-timeout")),
                        Vec::new(),
                        ResponseMetadata::default(),
                    );
                    return;
                }
            },
        };
        // Metadata that breaks the rules can't be handed to the caller as it was
        // sent.
        let metadata = match Metadata::from_headers(&headers) {
            Ok(metadata) => metadata,
            Err(e) => {
                self.send_status(
                    stream_id,
                    end_stream,
                    Err(Status::invalid_argument(format!(
                        "malformed request metadata: {e}"
                    ))),
                    Vec::new(),
                    ResponseMetadata::default(),
                );
                return;
            }
        };
        // The request's encoding must be one we can decode; the client is told
        // which ones we can.
        let Ok(request_codec) = self
            .compression
            .decoder_for(header(&headers, "grpc-encoding"))
        else {
            let mut extra = Vec::new();
            if let Some(accept) = self.compression.accept_header() {
                extra.push(field("grpc-accept-encoding", &accept));
            }
            self.send_status(
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
        let deadline = timeout.map(|t| self.now.saturating_add(t));
        self.calls.insert(
            stream_id,
            Call {
                deadline,
                inbound,
                half_closed: end_stream,
                expired: false,
                response_codec,
                headers_sent: false,
            },
        );
        self.events.push_back(CallEvent::Started {
            id: stream_id,
            key,
            path,
            metadata,
            deadline,
        });
    }

    /// A stream whose output could not be queued is gone: reset it, and report
    /// the call as cancelled if it was still active.
    fn fail_stream(&mut self, id: StreamId) {
        let _ = self.conn.reset_stream(id, ErrorCode::InternalError);
        if self.calls.remove(&id).is_some() {
            self.events.push_back(CallEvent::Cancelled { id });
        }
    }

    /// Queue the final status of a stream: trailers, or a trailers-only
    /// response if the response headers were never sent. If the client is still
    /// sending, the stream is then reset with `NO_ERROR`.
    fn send_status(
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
