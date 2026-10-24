//! Sans-IO relay of one incoming call to one downstream call.
//!
//! A [`Relay`] owns no connection. Each [`pump`](Relay::pump) moves whatever can
//! move between an incoming [`Lifecycle`] call and a downstream [`Client`]
//! call, in both directions, and reports whether anything moved. The caller
//! owns both endpoints, drives their I/O, and pumps the relay after each change
//! to either side.
//!
//! Rules the relay keeps:
//!
//! - Request messages are taken from the incoming call only while the downstream
//!   call is pending and can queue them, so memory stays bounded by one message
//!   per call, and the half-close follows the last message it covers. Once the
//!   downstream has finished, no request is taken, so a request error cannot
//!   replace its outcome.
//! - Response headers are forwarded as soon as the downstream has them, before
//!   any message, and response messages only while the incoming call can take
//!   them.
//! - The downstream's final status and trailers are forwarded as they are; a
//!   trailers-only response stays trailers-only.
//! - The downstream call gets the remaining deadline budget, not the original
//!   timeout.
//! - Each hop negotiates compression on its own. Messages are relayed as the
//!   bytes of their protobuf payload, which the relay never decodes.
//! - Shape rules follow the method kind, as they do for local calls: a unary or
//!   server-streaming request has exactly one message, and a unary or
//!   client-streaming response has exactly one message on success.

use alloc::string::String;
use core::mem;

use crate::client::{CallOptions, Client};
use crate::lifecycle::Lifecycle;
use crate::{CallId, Metadata, MethodKind, Next, ResponseMetadata, Status};

/// Relays one incoming call to one downstream call. See the [module](self)
/// documentation for the rules.
#[derive(Debug)]
pub struct Relay {
    upstream: CallId,
    path: String,
    kind: MethodKind,
    /// Request metadata, forwarded when the downstream call starts.
    metadata: Metadata,
    downstream: Option<CallId>,
    /// Request messages forwarded so far.
    requests: usize,
    /// The request half-close was forwarded.
    close_sent: bool,
    /// Response messages forwarded so far.
    responses: usize,
    /// The response headers were forwarded to the incoming call.
    headers_sent: bool,
    /// The incoming call has been ended, or cancelled, and the relay is spent.
    done: bool,
}

impl Relay {
    /// A relay of the incoming call `upstream` (its id in the incoming
    /// [`Lifecycle`]) to a downstream call of `path` with the call's shape and
    /// request metadata. Nothing is started until the first
    /// [`pump`](Self::pump).
    pub fn new(upstream: CallId, path: String, kind: MethodKind, metadata: Metadata) -> Self {
        Self {
            upstream,
            path,
            kind,
            metadata,
            downstream: None,
            requests: 0,
            close_sent: false,
            responses: 0,
            headers_sent: false,
            done: false,
        }
    }

    /// The incoming call's id.
    pub fn upstream(&self) -> CallId {
        self.upstream
    }

    /// The downstream call's id, once started.
    pub fn downstream(&self) -> Option<CallId> {
        self.downstream
    }

    /// The incoming call has ended (finished, cancelled or expired) and the
    /// downstream call is no longer relayed. A done relay does nothing more.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Move everything that can move between the two calls. Returns whether
    /// anything did.
    ///
    /// Starts the downstream call on the first pump, with the incoming call's
    /// remaining deadline budget. The incoming call's clock must be the one the
    /// downstream client is ticked with.
    pub fn pump<K: Copy>(&mut self, incoming: &mut Lifecycle<K>, downstream: &mut Client) -> bool {
        if self.done {
            return false;
        }
        if !incoming.has_call(self.upstream) {
            // Cancelled by the peer, or its connection failed: nothing to send.
            self.cancel_downstream(downstream);
            self.done = true;
            return true;
        }
        if incoming.expire_if_due(self.upstream) {
            self.end(
                incoming,
                downstream,
                Err(Status::deadline_exceeded("deadline exceeded")),
                Metadata::new(),
            );
            return true;
        }
        let mut progress = false;
        let id = match self.downstream {
            Some(id) => id,
            None => match self.start(incoming, downstream) {
                Ok(id) => {
                    progress = true;
                    id
                }
                Err(status) => {
                    self.end(incoming, downstream, Err(status), Metadata::new());
                    return true;
                }
            },
        };
        progress |= self.forward_requests(incoming, downstream, id);
        if self.done {
            return true;
        }
        progress |= self.forward_responses(incoming, downstream, id);
        progress
    }

    /// The incoming call's deadline passed. Ends the call with
    /// `DEADLINE_EXCEEDED` and cancels the downstream call, unless it is done
    /// already. [`pump`](Self::pump) does the same on its own; this is for a
    /// caller that wants to act on the lifecycle's `Expired` event directly.
    pub fn expired<K: Copy>(&mut self, incoming: &mut Lifecycle<K>, downstream: &mut Client) {
        if !self.done {
            self.end(
                incoming,
                downstream,
                Err(Status::deadline_exceeded("deadline exceeded")),
                Metadata::new(),
            );
        }
    }

    fn start<K: Copy>(
        &mut self,
        incoming: &Lifecycle<K>,
        downstream: &mut Client,
    ) -> Result<CallId, Status> {
        let mut options = CallOptions::metadata(mem::take(&mut self.metadata));
        if let Some(deadline) = incoming.deadline(self.upstream) {
            options = options.with_timeout(deadline.saturating_sub(incoming.now()));
        }
        let id = downstream.start_streaming_with(&self.path, &options)?;
        self.downstream = Some(id);
        Ok(id)
    }

    /// Forward buffered request messages while the downstream call can queue
    /// them, and the half-close once every message is forwarded.
    ///
    /// Nothing is taken once the downstream call has finished: its outcome is
    /// already known, and a request that could not be sent (for example one
    /// larger than the downstream's limit) must not replace it.
    fn forward_requests<K: Copy>(
        &mut self,
        incoming: &mut Lifecycle<K>,
        downstream: &mut Client,
        id: CallId,
    ) -> bool {
        let mut progress = false;
        while !self.close_sent && downstream.is_pending(id) && downstream.can_send(id) {
            match incoming.take_request(self.upstream) {
                Err(status) => {
                    self.end(incoming, downstream, Err(status), Metadata::new());
                    return true;
                }
                Ok(Some(message)) => {
                    if self.single_request() && self.requests > 0 {
                        let status = Status::internal(match self.kind {
                            MethodKind::Unary => "more than one request message for unary call",
                            _ => "more than one request message for server streaming call",
                        });
                        self.end(incoming, downstream, Err(status), Metadata::new());
                        return true;
                    }
                    self.requests += 1;
                    if let Err(status) = downstream.send_message(id, &message) {
                        self.end(incoming, downstream, Err(status), Metadata::new());
                        return true;
                    }
                    progress = true;
                }
                Ok(None) => {
                    if !incoming.is_half_closed(self.upstream) {
                        break;
                    }
                    if let Err(status) = incoming.request_finish(self.upstream) {
                        self.end(incoming, downstream, Err(status), Metadata::new());
                        return true;
                    }
                    if self.single_request() && self.requests == 0 {
                        let status = Status::internal("missing request message");
                        self.end(incoming, downstream, Err(status), Metadata::new());
                        return true;
                    }
                    if let Err(status) = downstream.close_send(id) {
                        self.end(incoming, downstream, Err(status), Metadata::new());
                        return true;
                    }
                    self.close_sent = true;
                    progress = true;
                    break;
                }
            }
        }
        progress
    }

    /// Forward the response headers, then messages, then the final status.
    fn forward_responses<K: Copy>(
        &mut self,
        incoming: &mut Lifecycle<K>,
        downstream: &mut Client,
        id: CallId,
    ) -> bool {
        let mut progress = self.forward_headers(incoming, downstream, id);
        while !self.done && incoming.can_send(self.upstream) {
            match downstream.try_next(id) {
                Some(Next::Message(message)) => {
                    if self.single_response() && self.responses > 0 {
                        let status = Status::internal(match self.kind {
                            MethodKind::Unary => "more than one response message for unary call",
                            _ => "more than one response message for client streaming call",
                        });
                        self.end(incoming, downstream, Err(status), Metadata::new());
                        return true;
                    }
                    if !self.headers_sent {
                        // Headers precede the first message on the downstream
                        // stream, so they have arrived: they were forwarded above
                        // unless the incoming call was gone.
                        self.end(
                            incoming,
                            downstream,
                            Err(Status::internal("response message before headers")),
                            Metadata::new(),
                        );
                        return true;
                    }
                    if let Err(status) = incoming.send_message(self.upstream, &message) {
                        // A transport failure already removed the call; a
                        // framing error did not, and ends it here.
                        if incoming.has_call(self.upstream) {
                            self.end(incoming, downstream, Err(status), Metadata::new());
                        } else {
                            self.done = true;
                            self.cancel_downstream(downstream);
                        }
                        return true;
                    }
                    self.responses += 1;
                    progress = true;
                }
                Some(Next::Done(result)) => {
                    // Headers that arrived with the status were forwarded above.
                    let (_, trailers) = downstream.take_metadata(id);
                    let (result, trailers) = match result {
                        // The trailers of an OK status still go out with the
                        // missing-response error, which carries none of its own.
                        Ok(()) if self.single_response() && self.responses == 0 => {
                            (Err(self.missing_response()), trailers)
                        }
                        Ok(()) => (Ok(()), trailers),
                        // A failed status carries its trailers in `Status::metadata`.
                        Err(status) => (Err(status), Metadata::new()),
                    };
                    self.end(incoming, downstream, result, trailers);
                    return true;
                }
                None => break,
            }
        }
        progress
    }

    /// Forward the downstream's response headers once they have arrived.
    fn forward_headers<K: Copy>(
        &mut self,
        incoming: &mut Lifecycle<K>,
        downstream: &mut Client,
        id: CallId,
    ) -> bool {
        if self.headers_sent {
            return false;
        }
        let Some(headers) = downstream.response_headers(id).cloned() else {
            return false;
        };
        self.headers_sent = incoming.send_headers(self.upstream, headers);
        if !self.headers_sent {
            self.cancel_downstream(downstream);
            self.done = true;
        }
        true
    }

    /// End the incoming call with `result`, and cancel the downstream call if
    /// it is still running. The relay is spent afterwards.
    fn end<K: Copy>(
        &mut self,
        incoming: &mut Lifecycle<K>,
        downstream: &mut Client,
        result: Result<(), Status>,
        trailers: Metadata,
    ) {
        self.cancel_downstream(downstream);
        let response = ResponseMetadata {
            initial: if self.headers_sent {
                None
            } else {
                Some(Metadata::new())
            },
            trailing: trailers,
        };
        incoming.finish(self.upstream, result, response);
        self.done = true;
    }

    fn cancel_downstream(&self, downstream: &mut Client) {
        if let Some(id) = self.downstream {
            downstream.cancel(id);
        }
    }

    /// The kinds that take one request message.
    fn single_request(&self) -> bool {
        matches!(self.kind, MethodKind::Unary | MethodKind::ServerStreaming)
    }

    /// The kinds that answer with one response message.
    fn single_response(&self) -> bool {
        matches!(self.kind, MethodKind::Unary | MethodKind::ClientStreaming)
    }

    fn missing_response(&self) -> Status {
        Status::internal(match self.kind {
            MethodKind::Unary => "missing response message",
            _ => "client streaming call ended without a response",
        })
    }
}
