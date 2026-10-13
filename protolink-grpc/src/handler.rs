use alloc::boxed::Box;
use alloc::vec::Vec;
use core::task::{Context, Poll};
use core::time::Duration;

use crate::{CallId, Metadata, MethodKind, Next, Status};

/// The response metadata a [`Handler`] has set on a call so far.
///
/// The server keeps one per call; handlers reach it through
/// [`CallContext`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseMetadata {
    /// Metadata for the response headers. `None` once the headers have been
    /// sent (with the first response message, or when `poll_response` returned
    /// `Poll::Pending`): later changes could no longer reach the client.
    pub initial: Option<Metadata>,
    /// Metadata for the response trailers, sent with the final status. It is
    /// sent whatever the status is, and also if the call ends some other way
    /// (for example `DEADLINE_EXCEEDED`).
    pub trailing: Metadata,
}

impl Default for ResponseMetadata {
    fn default() -> Self {
        Self {
            initial: Some(Metadata::new()),
            trailing: Metadata::new(),
        }
    }
}

/// The method selected for a call: its shape and an identifier that is
/// meaningful to the [`Handler`] that selected it.
///
/// [`Handler::route`] resolves a call's path to a route once, when the request
/// headers arrive. The server keeps it with the call and hands it back in
/// [`CallContext::route`] for every later lifecycle event, so a handler never
/// re-resolves the path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Route {
    /// The shape of the method. Decides which [`Handler`] methods the call
    /// is driven through.
    pub kind: MethodKind,
    /// Identifies the method within the handler that resolved it, for
    /// example its index in a [`MethodDescriptor`] table.
    pub method: u32,
}

impl Route {
    /// A route to `method` of shape `kind`.
    pub const fn new(kind: MethodKind, method: u32) -> Self {
        Self { kind, method }
    }

    /// The route of the method at `path` in `methods`, if any.
    pub fn find(methods: &[MethodDescriptor], path: &str) -> Option<Self> {
        methods.iter().find(|m| m.path == path).map(|m| m.route())
    }
}

/// A statically known method: its path, shape and identifier.
///
/// Generated services describe their methods with a `METHODS` table of these;
/// [`Registry`](crate::Registry) and [`MultiDispatch`](crate::MultiDispatch)
/// merge such tables and reject duplicate paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MethodDescriptor {
    /// The method path, `/package.Service/Method`.
    pub path: &'static str,
    /// The shape of the method.
    pub kind: MethodKind,
    /// Identifies the method within its service, see [`Route::method`].
    pub method: u32,
}

impl MethodDescriptor {
    /// A descriptor of `method` at `path`, of shape `kind`.
    pub const fn new(path: &'static str, kind: MethodKind, method: u32) -> Self {
        Self { path, kind, method }
    }

    /// The route that selects this method.
    pub const fn route(&self) -> Route {
        Route::new(self.kind, self.method)
    }
}

/// What a [`Handler`] is told about the call it is working on, and where it
/// sets the call's response metadata.
///
/// Passed to every call-specific [`Handler`] method.
#[derive(Debug)]
pub struct CallContext<'a> {
    /// The method path, `/package.Service/Method`.
    pub path: &'a str,
    /// The route [`Handler::route`] selected for this call. Composite
    /// handlers such as [`Registry`](crate::Registry) replace it with the
    /// inner handler's route while they delegate.
    pub route: Route,
    /// The call's id, unique per connection. Streaming handlers key their
    /// per-call state by it.
    pub id: CallId,
    /// When the call runs out of time, if the client sent a `grpc-timeout`:
    /// the deadline on the clock given to [`Server::tick`](crate::Server::tick).
    ///
    /// Use [`remaining`](Self::remaining) to turn it into a budget for work
    /// the handler starts, so the deadline propagates downstream.
    pub deadline: Option<Duration>,
    metadata: &'a Metadata,
    response: &'a mut ResponseMetadata,
}

impl<'a> CallContext<'a> {
    /// A context for a call of `path`, for the server and for testing
    /// handlers.
    pub fn new(
        path: &'a str,
        route: Route,
        id: CallId,
        deadline: Option<Duration>,
        metadata: &'a Metadata,
        response: &'a mut ResponseMetadata,
    ) -> Self {
        Self {
            path,
            route,
            id,
            deadline,
            metadata,
            response,
        }
    }

    /// Time left until the deadline at `now`, on the same clock as
    /// [`deadline`](Self::deadline). Zero once it has passed, `None` if the
    /// call has no deadline.
    pub fn remaining(&self, now: Duration) -> Option<Duration> {
        self.deadline.map(|d| d.saturating_sub(now))
    }

    /// The custom metadata the client sent with the request.
    pub fn metadata(&self) -> &Metadata {
        self.metadata
    }

    /// The metadata for the response headers, or `None` if the headers have
    /// been sent already.
    pub fn initial_metadata_mut(&mut self) -> Option<&mut Metadata> {
        self.response.initial.as_mut()
    }

    /// The metadata for the response trailers. It can be changed until the call
    /// ends.
    pub fn trailing_metadata_mut(&mut self) -> &mut Metadata {
        &mut self.response.trailing
    }

    /// Run `f` with [`route`](Self::route) temporarily replaced by `route`.
    pub(crate) fn with_route<R>(&mut self, route: Route, f: impl FnOnce(&mut Self) -> R) -> R {
        let outer = core::mem::replace(&mut self.route, route);
        let result = f(self);
        self.route = outer;
        result
    }
}

/// The server-side contract between a connection and the application.
///
/// Generated `<Service>Dispatch` wrappers, [`Registry`](crate::Registry) and
/// [`MultiDispatch`](crate::MultiDispatch) implement it. Implementing it by
/// hand is the low-level, dynamic facility: the handler decides at runtime
/// which paths it serves (see [`FnHandler`]).
///
/// # Routing
///
/// When a call's request headers arrive the server asks
/// [`route`](Self::route) once which method serves the path. `None` answers
/// `UNIMPLEMENTED` at once. Otherwise the returned [`Route`] is stored with
/// the call, decides by its [`kind`](Route::kind) which methods below drive
/// the call, and is passed back in [`CallContext::route`] to every one of
/// them. A handler therefore resolves each path exactly once per call; this
/// is the only routing authority.
///
/// # Unary calls
///
/// [`call`](Self::call) receives the request message once the client has
/// half-closed, and returns the response or a status.
///
/// # Streaming calls
///
/// Calls of streaming routes are driven through the remaining methods,
/// identified by the [`CallId`] in their [`CallContext`], which is unique per
/// connection. Per-call state is kept by the handler, typically in a map keyed
/// by `CallId`. The default implementations reject every call with
/// `UNIMPLEMENTED`, so handlers only implement what their routes need.
///
/// For each call, the server invokes:
///
/// 1. [`on_message`](Self::on_message) once per request message, in order.
///    A server-streaming call receives exactly one; a missing or extra
///    request fails the call with `INTERNAL` before the handler sees it.
/// 2. [`on_half_close`](Self::on_half_close) once the client has sent its
///    last message.
/// 3. [`poll_response`](Self::poll_response) to pull response messages while
///    the client can accept them. Bidirectional calls are polled from the
///    start; server- and client-streaming calls once the request side has
///    ended. A client-streaming call completes successfully after its first
///    [`Next::Message`].
///
/// The call ends when the handler returns an error from `on_message` or
/// `on_half_close`, or [`Next::Done`] from `poll_response`. If it ends any
/// other way (the client cancels or resets the stream, the request is
/// malformed or too large, the call's deadline passes, the connection fails
/// or is closed), the server calls [`on_cancel`](Self::on_cancel) instead, so
/// the handler can release the call's state. `on_cancel` may be called for a
/// call the handler has not seen yet.
///
/// # Connections
///
/// A driver borrows the handler for the lifetime of one connection, so call
/// state keyed by [`CallId`] is connection-local; separate connections use
/// separate handlers (with business state shared through handles the
/// application chooses, if needed).
///
/// # Deadlines
///
/// Every method receives a [`CallContext`] with the call's deadline, if the
/// client set one. The server ends the call with `DEADLINE_EXCEEDED` when it
/// passes (and reports streaming calls to `on_cancel`), but it can't
/// interrupt a unary handler that is running: a handler that does slow work
/// should consult [`CallContext::remaining`] and stop early.
///
/// The response headers are sent with the first response message, or as soon
/// as `poll_response` returns `Poll::Pending`. A call that ends before either
/// gets a trailers-only response.
///
/// # Metadata
///
/// [`CallContext::metadata`] holds the request's custom metadata. A handler
/// sets the response's headers with [`CallContext::initial_metadata_mut`], which
/// works until the response headers are sent, and its trailers with
/// [`CallContext::trailing_metadata_mut`]. A trailers-only response carries
/// both in its single header block. A [`Status`] returned by the handler adds
/// its own [`Status::metadata`] to the trailers.
///
/// # Waking
///
/// `poll_response` follows the usual [`Future`](core::future::Future)
/// contract: on `Poll::Pending`, wake `cx.waker()` once a response may be
/// ready. The server also polls every active call after delivering received
/// messages, so readiness caused by `on_message` or `on_half_close` needs no
/// wake-up; only external events (a timer, an interrupt, another task) do.
/// Spurious polls are allowed.
pub trait Handler {
    /// Select the method that serves `path`, or `None` if nothing does.
    /// Called once per call, when its request headers arrive.
    fn route(&self, path: &str) -> Option<Route>;

    /// Handle the unary call `ctx.path` with the encoded `request` message.
    fn call(&mut self, ctx: &mut CallContext<'_>, _request: &[u8]) -> Result<Vec<u8>, Status> {
        Err(unimplemented(ctx.path))
    }

    /// One request message of the streaming call `ctx.id` on `ctx.path`.
    fn on_message(&mut self, ctx: &mut CallContext<'_>, _message: &[u8]) -> Result<(), Status> {
        Err(unimplemented(ctx.path))
    }

    /// The client half-closed the streaming call: no more request messages.
    fn on_half_close(&mut self, _ctx: &mut CallContext<'_>) -> Result<(), Status> {
        Ok(())
    }

    /// Next response of the streaming call `ctx.id` on `ctx.path`.
    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _cx: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        Poll::Ready(Next::Done(Err(unimplemented(ctx.path))))
    }

    /// The streaming call ended without the handler finishing it.
    fn on_cancel(&mut self, _ctx: &mut CallContext<'_>) {}
}

pub(crate) fn unimplemented(path: &str) -> Status {
    Status::unimplemented(alloc::format!("unknown method {path}"))
}

macro_rules! forward_handler {
    () => {
        fn route(&self, path: &str) -> Option<Route> {
            (**self).route(path)
        }

        fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
            (**self).call(ctx, request)
        }

        fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
            (**self).on_message(ctx, message)
        }

        fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
            (**self).on_half_close(ctx)
        }

        fn poll_response(
            &mut self,
            ctx: &mut CallContext<'_>,
            cx: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            (**self).poll_response(ctx, cx)
        }

        fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
            (**self).on_cancel(ctx)
        }
    };
}

impl<H: Handler + ?Sized> Handler for &mut H {
    forward_handler!();
}

impl<H: Handler + ?Sized> Handler for Box<H> {
    forward_handler!();
}

/// [`Handler`] backed by a closure: the dynamic, unary-only facility.
///
/// Every path is routed to the closure as a unary method; it returns `None`
/// for paths it does not serve, which are answered with `UNIMPLEMENTED` once
/// the request has arrived.
#[derive(Debug, Clone)]
pub struct FnHandler<F>(pub F);

impl<F> Handler for FnHandler<F>
where
    F: FnMut(&str, &[u8]) -> Option<Result<Vec<u8>, Status>>,
{
    fn route(&self, _: &str) -> Option<Route> {
        Some(Route::new(MethodKind::Unary, 0))
    }

    fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        (self.0)(ctx.path, request).unwrap_or_else(|| Err(unimplemented(ctx.path)))
    }
}
