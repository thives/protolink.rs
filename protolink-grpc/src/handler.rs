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

/// What a [`Handler`] is told about the call it is working on, and where it
/// sets the call's response metadata.
///
/// Passed to every call-specific [`Handler`] method.
#[derive(Debug)]
pub struct CallContext<'a> {
    /// The method path, `/package.Service/Method`.
    pub path: &'a str,
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
    /// A context for `path`, for the server and for testing handlers.
    pub fn new(
        path: &'a str,
        id: CallId,
        deadline: Option<Duration>,
        metadata: &'a Metadata,
        response: &'a mut ResponseMetadata,
    ) -> Self {
        Self {
            path,
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
}

/// Server-side request handler. Generated `*Server` wrappers implement this.
///
/// # Unary calls
///
/// `call` returns `None` when the path is not handled, which lets handlers be
/// combined: a tuple `(A, B, ...)` of handlers tries each in order. The server
/// answers unhandled paths with `UNIMPLEMENTED` once the client has sent its
/// request, unless the handler recognizes the path as unknown up front (see
/// [`is_unknown_method`](Self::is_unknown_method)), in which case the answer
/// is sent as soon as the request headers arrive.
///
/// # Streaming calls
///
/// A handler declares its streaming methods through
/// [`method_kind`](Self::method_kind). Calls of those paths are driven through
/// the remaining methods instead of `call`, identified by the [`CallId`] in
/// their [`CallContext`], which is unique per connection. Per-call state is kept by the handler, typically in
/// a map keyed by `CallId`. The default implementations reject every streaming
/// call with `UNIMPLEMENTED`, so unary-only handlers need not implement them.
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
/// or is closed), the server
/// calls [`on_cancel`](Self::on_cancel) instead, so the handler can release
/// the call's state. `on_cancel` may be called for a call the handler has not
/// seen yet.
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
    /// Handle the unary call `ctx.path` with the encoded `request` message.
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>>;

    /// Whether this handler knows that it serves nothing at `path`, neither
    /// unary nor streaming. The server then answers `UNIMPLEMENTED` as soon
    /// as the request headers arrive, so a streaming client learns about it
    /// without having to half-close first.
    ///
    /// The default is `false`: the path may still be served dynamically
    /// through [`call`](Self::call), so the server waits for the request to
    /// end. Return `true` only if [`call`](Self::call) would return `None` and
    /// [`method_kind`](Self::method_kind) would return `None` for `path`.
    /// Generated `*Server` wrappers implement this; a tuple of handlers
    /// reports `true` only if every member does.
    fn is_unknown_method(&self, path: &str) -> bool {
        let _ = path;
        false
    }

    /// The kind of method served at `path`, or `None` if it is not handled
    /// here. Paths reported as `None` or [`MethodKind::Unary`] are served
    /// through [`call`](Self::call).
    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        let _ = path;
        None
    }

    /// One request message of the streaming call `ctx.id` on `ctx.path`.
    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        let _ = message;
        Err(unimplemented(ctx.path))
    }

    /// The client half-closed the streaming call: no more request messages.
    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        let _ = ctx;
        Ok(())
    }

    /// Next response of the streaming call `ctx.id` on `ctx.path`.
    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        let _ = cx;
        Poll::Ready(Next::Done(Err(unimplemented(ctx.path))))
    }

    /// The streaming call ended without the handler finishing it.
    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        let _ = ctx;
    }
}

pub(crate) fn unimplemented(path: &str) -> Status {
    Status::unimplemented(alloc::format!("unknown method {path}"))
}

impl<H: Handler + ?Sized> Handler for &mut H {
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>> {
        (**self).call(ctx, request)
    }

    fn is_unknown_method(&self, path: &str) -> bool {
        (**self).is_unknown_method(path)
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        (**self).method_kind(path)
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
}

/// [`Handler`] backed by a closure (unary calls only).
#[derive(Debug, Clone)]
pub struct FnHandler<F>(pub F);

impl<F> Handler for FnHandler<F>
where
    F: FnMut(&str, &[u8]) -> Option<Result<Vec<u8>, Status>>,
{
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>> {
        (self.0)(ctx.path, request)
    }
}

macro_rules! tuple_handler {
    ($($name:ident),+) => {
        /// Unary calls try each handler in order. Streaming calls go to the
        /// first handler whose [`method_kind`](Handler::method_kind) knows
        /// the path. A path is unknown up front only if every handler says so.
        impl<$($name: Handler),+> Handler for ($($name,)+) {
            #[allow(non_snake_case)]
            fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {
                let ($($name,)+) = self;
                $(
                    if let Some(r) = $name.call(ctx, request) {
                        return Some(r);
                    }
                )+
                None
            }

            #[allow(non_snake_case)]
            fn is_unknown_method(&self, path: &str) -> bool {
                let ($($name,)+) = self;
                true $(&& $name.is_unknown_method(path))+
            }

            #[allow(non_snake_case)]
            fn method_kind(&self, path: &str) -> Option<MethodKind> {
                let ($($name,)+) = self;
                $(
                    if let Some(k) = $name.method_kind(path) {
                        return Some(k);
                    }
                )+
                None
            }

            #[allow(non_snake_case)]
            fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
                let ($($name,)+) = self;
                $(
                    if $name.method_kind(ctx.path).is_some() {
                        return $name.on_message(ctx, message);
                    }
                )+
                Err(unimplemented(ctx.path))
            }

            #[allow(non_snake_case)]
            fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
                let ($($name,)+) = self;
                $(
                    if $name.method_kind(ctx.path).is_some() {
                        return $name.on_half_close(ctx);
                    }
                )+
                Ok(())
            }

            #[allow(non_snake_case)]
            fn poll_response(
                &mut self,
                ctx: &mut CallContext<'_>,
                cx: &mut Context<'_>,
            ) -> Poll<Next<Vec<u8>>> {
                let ($($name,)+) = self;
                $(
                    if $name.method_kind(ctx.path).is_some() {
                        return $name.poll_response(ctx, cx);
                    }
                )+
                Poll::Ready(Next::Done(Err(unimplemented(ctx.path))))
            }

            #[allow(non_snake_case)]
            fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
                let ($($name,)+) = self;
                $(
                    if $name.method_kind(ctx.path).is_some() {
                        return $name.on_cancel(ctx);
                    }
                )+
            }
        }
    };
}

tuple_handler!(A);
tuple_handler!(A, B);
tuple_handler!(A, B, C);
tuple_handler!(A, B, C, D);
tuple_handler!(A, B, C, D, E);
tuple_handler!(A, B, C, D, E, F);
tuple_handler!(A, B, C, D, E, F, G);
tuple_handler!(A, B, C, D, E, F, G, H);
