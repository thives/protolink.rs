//! Drives the [`Handler`] of each local call of a connection.
//!
//! [`LocalCalls`] adapts a [`Handler`] to the [`Lifecycle`]: it keeps the state
//! of each call the handler serves, pulls request messages into the handler,
//! and pulls responses out of it, as far as the lifecycle allows. The owner of
//! the lifecycle decides when each call is driven and applies its events.
//! [`Server`](crate::Server) and [`Router`](crate::Router) are built on it.

use alloc::collections::BTreeMap;
use alloc::string::String;
use core::mem;
use core::ops::Bound;
use core::task::{Context, Poll};
use core::time::Duration;

use protolink_http2::StreamId;

use crate::handler::ResponseMetadata;
use crate::lifecycle::Lifecycle;
use crate::{CallContext, Handler, Metadata, MethodKind, Next, Route, Status};

/// Maximum drive iterations per call and polling pass. Each iteration pulls
/// at most one response and delivers at most one buffered streaming request.
pub(crate) const CALL_QUANTUM: usize = 16;

/// A call as the [`Handler`] sees it: what the handler was told when the call
/// started, and the response metadata it has set so far.
#[derive(Debug)]
struct Call {
    path: String,
    /// Selected by [`Handler::route`] when the call started; every later
    /// event of the call is delivered with it.
    route: Route,
    /// When the call runs out of time, on the server's clock (see
    /// [`Server::tick`]), from the request's `grpc-timeout`.
    deadline: Option<Duration>,
    /// Custom metadata of the request.
    request_metadata: Metadata,
    /// Response metadata set by the handler.
    response: ResponseMetadata,
    /// Request messages delivered to the handler.
    requests: usize,
    /// `on_half_close` was delivered, after every buffered request message.
    end_delivered: bool,
}

impl Call {
    /// What handlers are told about this call.
    fn ctx(&mut self, id: StreamId) -> CallContext<'_> {
        CallContext::new(
            &self.path,
            self.route,
            id,
            self.deadline,
            &self.request_metadata,
            &mut self.response,
        )
    }
}

/// A walk over the local calls in id order, advanced by
/// [`LocalCalls::next_id`].
///
/// A receive walk visits every id in ascending order. A polling walk visits
/// the ids after its split, the cursor when it started, and then the ids up to
/// and including it, so each polling pass starts one call later than the last.
///
/// The walk holds no borrow of the call map and no storage, which is why it is
/// `Copy`. Each step is a short-lived range lookup for the next id, so the
/// caller can drive calls and apply lifecycle events between steps. Bounds are
/// ids rather than positions: removing a call skips only that call, a call that
/// is still present is reached exactly once, and the walk wraps at most once,
/// even if the split call itself is removed.
///
/// Insertion policy: a call added during a walk is visited by it only if it is
/// ahead of the walk, meaning its id sorts after the last id visited in the run
/// in progress. The driving loops add no calls mid-walk. Calls are admitted only
/// by [`Lifecycle::recv`], which those loops never call, so the walk sees a
/// fixed set of calls apart from removals.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Traversal {
    /// The cursor when a polling walk started. `None` for a receive walk.
    split: Option<StreamId>,
    /// The last id yielded in the run in progress.
    last: Option<StreamId>,
    /// Whether the run over the ids up to `split` is in progress.
    wrapped: bool,
    /// Whether any id has been yielded yet.
    started: bool,
}

impl Traversal {
    fn new(split: Option<StreamId>) -> Self {
        Self {
            split,
            last: None,
            wrapped: false,
            started: false,
        }
    }

    /// The next id of the run in progress, or the first id of the wrapped run
    /// once the first run is exhausted.
    fn advance(&mut self, calls: &BTreeMap<StreamId, Call>) -> Option<StreamId> {
        loop {
            if let Some((&id, _)) = calls.range(self.bounds()).next() {
                self.last = Some(id);
                return Some(id);
            }
            if self.wrapped || self.split.is_none() {
                return None;
            }
            self.wrapped = true;
            self.last = None;
        }
    }

    /// The bounds of the next lookup in the run in progress. Both bounds are
    /// ids, so a removed call neither moves them nor leaves them dangling.
    fn bounds(&self) -> (Bound<StreamId>, Bound<StreamId>) {
        let from = match (self.last, self.split) {
            (Some(last), _) => Bound::Excluded(last),
            (None, Some(split)) if !self.wrapped => Bound::Excluded(split),
            _ => Bound::Unbounded,
        };
        let to = match self.split {
            Some(split) if self.wrapped => Bound::Included(split),
            _ => Bound::Unbounded,
        };
        (from, to)
    }
}

#[derive(Debug, Default)]
pub(crate) struct LocalCalls {
    /// Calls the handler has been told about and has not finished.
    calls: BTreeMap<StreamId, Call>,
    /// First call visited in the previous polling pass.
    poll_cursor: StreamId,
}

impl LocalCalls {
    /// A walk in ascending id order. It leaves the polling rotation unchanged.
    pub(crate) fn receive_traversal(&self) -> Traversal {
        Traversal::new(None)
    }

    /// A polling walk: it starts after the call that the previous pass visited
    /// first, so that call moves to the back of the rotation.
    pub(crate) fn poll_traversal(&self) -> Traversal {
        Traversal::new(Some(self.poll_cursor))
    }

    /// Advance `traversal` to the next local call and return its id. The first
    /// id a polling walk yields becomes the cursor of the next polling walk.
    pub(crate) fn next_id(&mut self, traversal: &mut Traversal) -> Option<StreamId> {
        let id = traversal.advance(&self.calls)?;
        if !traversal.started {
            traversal.started = true;
            if traversal.split.is_some() {
                self.poll_cursor = id;
            }
        }
        Some(id)
    }

    /// Whether `id` is a local call.
    pub(crate) fn contains(&self, id: StreamId) -> bool {
        self.calls.contains_key(&id)
    }

    /// Number of local calls.
    pub(crate) fn len(&self) -> usize {
        self.calls.len()
    }

    /// A call started: its path was resolved to `route`.
    pub(crate) fn accept(
        &mut self,
        id: StreamId,
        route: Route,
        path: String,
        metadata: Metadata,
        deadline: Option<Duration>,
    ) {
        self.calls.insert(
            id,
            Call {
                path,
                route,
                deadline,
                request_metadata: metadata,
                response: ResponseMetadata::default(),
                requests: 0,
                end_delivered: false,
            },
        );
    }

    /// The call's deadline passed: end it with `DEADLINE_EXCEEDED`. Unknown
    /// calls are ignored.
    pub(crate) fn expire<H: Handler + ?Sized>(
        &mut self,
        lifecycle: &mut Lifecycle<Route>,
        id: StreamId,
        handler: &mut H,
    ) {
        self.abort(
            lifecycle,
            id,
            Status::deadline_exceeded("deadline exceeded"),
            handler,
        );
    }

    /// The call is gone (reset, failed or cancelled): streaming handlers are
    /// told. Unknown calls are ignored.
    pub(crate) fn cancel<H: Handler + ?Sized>(&mut self, id: StreamId, handler: &mut H) {
        if let Some(mut call) = self.calls.remove(&id)
            && call.route.kind != MethodKind::Unary
        {
            handler.on_cancel(&mut call.ctx(id));
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
    pub(crate) fn drive<H: Handler + ?Sized>(
        &mut self,
        lifecycle: &mut Lifecycle<Route>,
        id: StreamId,
        handler: &mut H,
        mut cx: Option<&mut Context<'_>>,
    ) {
        for _ in 0..CALL_QUANTUM {
            let Some(kind) = self.calls.get(&id).map(|c| c.route.kind) else {
                return;
            };
            if lifecycle.expire_if_due(id) || !lifecycle.has_call(id) {
                return;
            }
            let progressed = match kind {
                MethodKind::Unary => {
                    self.drive_unary(lifecycle, id, handler);
                    return;
                }
                MethodKind::BidiStreaming => {
                    // Requests are delivered from `poll` only, interleaved
                    // with responses.
                    let Some(cx) = cx.as_deref_mut() else {
                        return;
                    };
                    if !lifecycle.can_send(id) {
                        return;
                    }
                    let delivered = self.deliver(lifecycle, id, handler, 1);
                    let polled = self.poll_call(lifecycle, id, handler, cx);
                    delivered || polled
                }
                MethodKind::ServerStreaming | MethodKind::ClientStreaming => {
                    // `recv` has no waker and must deliver all available
                    // non-bidi requests. Polling passes bound request work too.
                    let limit = if cx.is_some() { 1 } else { usize::MAX };
                    let delivered = self.deliver(lifecycle, id, handler, limit);
                    let polled = match cx.as_deref_mut() {
                        Some(cx) => self.poll_call(lifecycle, id, handler, cx),
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
            && lifecycle.can_send(id)
            && let Some(cx) = cx
        {
            cx.waker().wake_by_ref();
        }
    }

    fn drive_unary<H: Handler + ?Sized>(
        &mut self,
        lifecycle: &mut Lifecycle<Route>,
        id: StreamId,
        handler: &mut H,
    ) {
        if let Some(e) = lifecycle.request_error(id) {
            self.finish(lifecycle, id, Err(e));
            return;
        }
        if lifecycle.request_count(id) > 1 {
            self.finish(
                lifecycle,
                id,
                Err(Status::internal(
                    "more than one request message for unary call",
                )),
            );
            return;
        }
        if !lifecycle.is_half_closed(id) {
            return;
        }
        let truncated = lifecycle.request_finish(id);
        let message = lifecycle.take_request(id);
        let Some(call) = self.calls.get_mut(&id) else {
            return;
        };
        let mut ctx = call.ctx(id);
        let result = truncated.and_then(|()| match message {
            Ok(Some(msg)) => handler.call(&mut ctx, &msg),
            Err(e) => Err(e),
            Ok(None) => Err(Status::internal("missing request message")),
        });
        match result {
            Ok(reply) if reply.len() > lifecycle.max_message_size() => {
                self.finish(
                    lifecycle,
                    id,
                    Err(Status::resource_exhausted("response message too large")),
                );
            }
            Ok(reply) => {
                if self.send_message(lifecycle, id, &reply, handler) {
                    self.finish(lifecycle, id, Ok(()));
                }
            }
            Err(status) => self.finish(lifecycle, id, Err(status)),
        }
    }

    /// Deliver up to `limit` buffered request messages (or the half-close)
    /// of a streaming call. Returns whether anything was delivered.
    fn deliver<H: Handler + ?Sized>(
        &mut self,
        lifecycle: &mut Lifecycle<Route>,
        id: StreamId,
        handler: &mut H,
        limit: usize,
    ) -> bool {
        let mut delivered = 0;
        loop {
            let Some(kind) = self.calls.get(&id).map(|c| c.route.kind) else {
                return delivered > 0;
            };
            if delivered >= limit {
                return true;
            }
            match lifecycle.take_request(id) {
                Ok(Some(message)) => {
                    delivered += 1;
                    let Some(call) = self.calls.get_mut(&id) else {
                        return true;
                    };
                    if kind == MethodKind::ServerStreaming && call.requests > 0 {
                        self.abort(
                            lifecycle,
                            id,
                            Status::internal(
                                "more than one request message for server streaming call",
                            ),
                            handler,
                        );
                        return true;
                    }
                    call.requests += 1;
                    if let Err(status) = handler.on_message(&mut call.ctx(id), &message) {
                        self.finish(lifecycle, id, Err(status));
                        return true;
                    }
                }
                Err(status) => {
                    self.abort(lifecycle, id, status, handler);
                    return true;
                }
                Ok(None) => {
                    let Some(call) = self.calls.get_mut(&id) else {
                        return delivered > 0;
                    };
                    if !lifecycle.is_half_closed(id) || call.end_delivered {
                        return delivered > 0;
                    }
                    if let Err(status) = lifecycle.request_finish(id) {
                        self.abort(lifecycle, id, status, handler);
                        return true;
                    }
                    if kind == MethodKind::ServerStreaming && call.requests == 0 {
                        self.abort(
                            lifecycle,
                            id,
                            Status::internal("missing request message"),
                            handler,
                        );
                        return true;
                    }
                    call.end_delivered = true;
                    if let Err(status) = handler.on_half_close(&mut call.ctx(id)) {
                        self.finish(lifecycle, id, Err(status));
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
        lifecycle: &mut Lifecycle<Route>,
        id: StreamId,
        handler: &mut H,
        cx: &mut Context<'_>,
    ) -> bool {
        let Some(call) = self.calls.get_mut(&id) else {
            return false;
        };
        let kind = call.route.kind;
        if kind != MethodKind::BidiStreaming && !call.end_delivered {
            return false;
        }
        if !lifecycle.can_send(id) {
            return false;
        }
        let next = match handler.poll_response(&mut call.ctx(id), cx) {
            Poll::Ready(next) => next,
            Poll::Pending => {
                // Send headers for pending handlers so peers waiting for call
                // acceptance can start streaming. Immediate failures remain
                // trailers-only responses.
                return !self.ensure_headers(lifecycle, id);
            }
        };
        match next {
            Next::Message(msg) => {
                if msg.len() > lifecycle.max_message_size() {
                    self.abort(
                        lifecycle,
                        id,
                        Status::resource_exhausted("response message too large"),
                        handler,
                    );
                    return true;
                }
                if !self.send_message(lifecycle, id, &msg, handler) {
                    return true;
                }
                if kind == MethodKind::ClientStreaming {
                    self.finish(lifecycle, id, Ok(()));
                }
            }
            Next::Done(Ok(())) if kind == MethodKind::ClientStreaming => {
                self.finish(
                    lifecycle,
                    id,
                    Err(Status::internal(
                        "client streaming call ended without a response",
                    )),
                );
            }
            Next::Done(result) => self.finish(lifecycle, id, result),
        }
        true
    }

    /// Queue one response message, preceded by the response headers if they
    /// were not sent yet. Framing errors end the call with a gRPC status;
    /// transport errors reset it. Returns whether the message was queued.
    fn send_message<H: Handler + ?Sized>(
        &mut self,
        lifecycle: &mut Lifecycle<Route>,
        id: StreamId,
        msg: &[u8],
        handler: &mut H,
    ) -> bool {
        if !self.ensure_headers(lifecycle, id) {
            return false;
        }
        match lifecycle.send_message(id, msg) {
            Ok(()) => true,
            Err(status) => {
                // A transport failure has already removed the call, and its
                // cancellation is reported as an event.
                if lifecycle.has_call(id) {
                    self.abort(lifecycle, id, status, handler);
                }
                false
            }
        }
    }

    /// Send the response headers unless they were sent already, with the
    /// metadata the handler set. Returns whether the call is still active.
    fn ensure_headers(&mut self, lifecycle: &mut Lifecycle<Route>, id: StreamId) -> bool {
        let Some(call) = self.calls.get_mut(&id) else {
            return false;
        };
        // From here on the handler can no longer add initial metadata, which
        // is also how a call remembers that its headers went out.
        let Some(initial) = call.response.initial.take() else {
            return true;
        };
        lifecycle.send_headers(id, initial)
    }

    /// End a call whose handler has finished it, with its final status and
    /// the metadata the handler set.
    pub(crate) fn finish(
        &mut self,
        lifecycle: &mut Lifecycle<Route>,
        id: StreamId,
        result: Result<(), Status>,
    ) {
        let response = self
            .calls
            .remove(&id)
            .map(|mut call| mem::take(&mut call.response))
            .unwrap_or_default();
        lifecycle.finish(id, result, response);
    }

    /// End a call the handler did not finish itself. Streaming handlers are
    /// told.
    fn abort<H: Handler + ?Sized>(
        &mut self,
        lifecycle: &mut Lifecycle<Route>,
        id: StreamId,
        status: Status,
        handler: &mut H,
    ) {
        // The status and metadata go out first; the handler is told after,
        // with the call's original deadline in its context.
        let Some(mut call) = self.calls.remove(&id) else {
            lifecycle.finish(id, Err(status), ResponseMetadata::default());
            return;
        };
        lifecycle.finish(id, Err(status), mem::take(&mut call.response));
        if call.route.kind != MethodKind::Unary {
            handler.on_cancel(&mut call.ctx(id));
        }
    }
}

#[cfg(test)]
#[path = "tests_local.rs"]
mod tests_local;
