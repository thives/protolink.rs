//! Async drivers over `embedded_io_async`.

use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Poll, Waker};

use embedded_io_async::{Read, Write};

use crate::grpc::{
    self, CallId, CallOptions, ClientConfig, Handler, Metadata, Next, Response, ServerConfig,
    Status, StreamingCall, StreamingTransport, UnaryTransport,
};
use crate::shared::Shared;
use crate::timer::{NoTimer, Timer};
use crate::{Error, READ_CHUNK};

/// Serve gRPC on one connection until the peer disconnects or the connection
/// fails.
///
/// Requests are dispatched to `handler` (e.g. a generated `<Service>Server`, or
/// a tuple of them). Writes are cancel-safe if the transport's `write` is, so
/// the future may be dropped (e.g. on shutdown) without corrupting state.
///
/// Streaming handlers are polled with this future's waker: when one returns
/// `Poll::Pending` and later wakes the waker, the pending transport `read` is
/// dropped so the new responses can be written. Serving streaming methods
/// whose handlers return `Pending` therefore requires a cancel-safe `read`
/// (as tokio streams, [`CobsFramed`](crate::link::CobsFramed) and the
/// [`link`](crate::link) stack are); unary-only servers never drop a read.
///
/// When the connection ends, every active streaming call is reported to
/// [`Handler::on_cancel`].
///
/// `serve` has no clock, so it doesn't enforce `grpc-timeout` deadlines; use
/// [`serve_with_timer`] for that.
pub async fn serve<IO, H>(
    io: IO,
    handler: &mut H,
    config: ServerConfig,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
{
    serve_with_timer(io, handler, config, NoTimer).await
}

/// Like [`serve`], and enforces the `grpc-timeout` deadline of each call using
/// `timer`.
///
/// A call's deadline starts when its request headers arrive. When it is
/// reached the call is ended with `DEADLINE_EXCEEDED` (streaming handlers are
/// told through [`Handler::on_cancel`]). A unary handler that is running when
/// its deadline passes can't be preempted; it is [`CallContext`](crate::CallContext)'s
/// `deadline` that lets it give up early.
///
/// Waiting for a deadline drops the pending transport `read` when the timer
/// fires, exactly like waiting for a streaming handler does, so the transport's
/// `read` must be cancel-safe (see [`serve`]), also for unary-only servers.
pub async fn serve_with_timer<IO, H, T>(
    mut io: IO,
    handler: &mut H,
    config: ServerConfig,
    timer: T,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
    T: Timer,
{
    let mut server = grpc::Server::new(config);
    let result = serve_connection(&mut io, &mut server, handler, &timer).await;
    server.cancel_all(handler);
    result
}

async fn serve_connection<IO, H, T>(
    io: &mut IO,
    server: &mut grpc::Server,
    handler: &mut H,
    timer: &T,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
    T: Timer,
{
    let mut buf = [0u8; READ_CHUNK];
    loop {
        server.tick(timer.now(), &mut *handler);
        while server.has_output() {
            let n = io.write(server.pending_output()).await.map_err(Error::Io)?;
            server.consume_output(n);
        }
        io.flush().await.map_err(Error::Io)?;
        if server.is_closed() {
            return Ok(());
        }
        // Wait for input, for a streaming handler to produce output, or for
        // a deadline.
        let read = {
            let mut read = pin!(io.read(&mut buf));
            let mut sleep = pin!(server.next_deadline().map(|d| timer.sleep_until(d)));
            poll_fn(|cx| {
                server.poll(&mut *handler, cx);
                if server.has_output() {
                    return Poll::Ready(None);
                }
                if let Some(sleep) = sleep.as_mut().as_pin_mut()
                    && sleep.poll(cx).is_ready()
                {
                    return Poll::Ready(None);
                }
                read.as_mut().poll(cx).map(Some)
            })
            .await
        };
        let Some(read) = read else {
            continue;
        };
        let n = read.map_err(Error::Io)?;
        if n == 0 {
            return Ok(());
        }
        // Calls that start with this input get their deadline from now.
        server.tick(timer.now(), &mut *handler);
        if let Err(e) = server.recv(&buf[..n], handler) {
            // Best effort: deliver the GOAWAY before giving up.
            let _ = io.write_all(server.pending_output()).await;
            let _ = io.flush().await;
            return Err(Error::Protocol(e));
        }
    }
}

/// Async gRPC client on one connection.
///
/// Implements [`UnaryTransport`] and [`StreamingTransport`], so it plugs
/// straight into generated `<Service>Client`s.
///
/// Several streaming [`Call`]s can be active on one connection, and their
/// operations can run concurrently (for example `join!`ed, or on separate
/// tasks with `std`). Unary calls take `&mut self`, so they can't run while a
/// [`Call`] exists.
///
/// The transport is used by one operation at a time. It is checked out of the
/// client for each I/O step and returned when the step completes or its
/// future is dropped. A pending read doesn't hold up the other calls: when
/// another call has something to write (a request, a half-close, or the
/// cancellation of a dropped call), the read is **dropped** so the transport
/// can be used, and started again afterwards. This requires a cancel-safe
/// `read`, which tokio streams, [`CobsFramed`](crate::link::CobsFramed) and
/// the [`link`](crate::link) stack are (the same requirement as
/// [`serve`] with streaming handlers). It only matters when more than one call
/// is active; a client with one call never drops a read. For transports
/// without a cancel-safe `read` (for example a one-shot DMA UART), put them
/// behind [`link::pump`](crate::link::pump) or keep to one call at a time.
///
/// A blocked transport `write` still stops everything until it completes; see
/// [`Call`].
///
/// # Deadlines
///
/// Calls can have a timeout ([`CallOptions`], [`ClientConfig::default_timeout`]),
/// which is sent to the server as `grpc-timeout`. A client built with
/// [`with_timer`](Self::with_timer) also enforces it: when the call's time is
/// up it fails with `DEADLINE_EXCEEDED` and the stream is reset. Messages
/// that were already received are still delivered first. A client built with
/// [`new`](Self::new) has no clock and leaves enforcement to the server.
///
/// Deadlines are only noticed while one of the client's operations is being
/// polled. Waiting for a deadline drops the pending read when the timer fires,
/// so with a timer, `read` must be cancel-safe even when only one call is
/// active. The reset of a call that expired is written by the next operation
/// on the client.
#[derive(Debug)]
pub struct Client<IO, T = NoTimer> {
    shared: Shared<State<IO>>,
    timer: T,
}

#[derive(Debug)]
struct State<IO> {
    inner: grpc::Client,
    /// The transport, or `None` while an operation has it checked out.
    io: Option<IO>,
    /// Tasks waiting for the transport to be checked back in.
    waiters: Vec<Waker>,
    /// A unary call whose future was dropped before it completed.
    stale_unary: Option<CallId>,
    /// Some call has output to write while the transport is busy: a pending
    /// read should be dropped.
    want_io: bool,
    /// Waker of the operation that is reading, so that it can be asked to
    /// give up the transport.
    holder: Option<Waker>,
}

/// The transport, checked out of a [`Client`]. Dropping the guard checks it
/// back in and wakes the tasks waiting for it.
struct IoGuard<'a, IO, T> {
    client: &'a Client<IO, T>,
    io: Option<IO>,
}

impl<IO, T> IoGuard<'_, IO, T> {
    fn io(&mut self) -> &mut IO {
        self.io
            .as_mut()
            .expect("the transport is held until the guard is dropped")
    }
}

impl<IO, T> Drop for IoGuard<'_, IO, T> {
    fn drop(&mut self) {
        let io = self.io.take();
        let waiters = self.client.shared.with(|s| {
            s.io = io;
            s.holder = None;
            core::mem::take(&mut s.waiters)
        });
        for waiter in waiters {
            waiter.wake();
        }
    }
}

impl<IO: Read + Write> Client<IO> {
    /// Create a client without a clock; the HTTP/2 preface is sent with the
    /// first call. Timeouts are sent to the server but not enforced locally;
    /// see [`with_timer`](Self::with_timer).
    pub fn new(io: IO, config: ClientConfig) -> Self {
        Self::with_timer(io, config, NoTimer)
    }
}

impl<IO: Read + Write, T: Timer> Client<IO, T> {
    /// Create a client that enforces call timeouts using `timer`; the HTTP/2
    /// preface is sent with the first call.
    pub fn with_timer(io: IO, config: ClientConfig, timer: T) -> Self {
        Self {
            shared: Shared::new(State {
                inner: grpc::Client::new(config),
                io: Some(io),
                waiters: Vec::new(),
                stale_unary: None,
                want_io: false,
                holder: None,
            }),
            timer,
        }
    }

    /// Perform one unary call.
    pub async fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        self.unary_with(path, request, CallOptions::default())
            .await
            .map(Response::into_message)
    }

    /// Perform one unary call with per-call `options`, for example a timeout
    /// or request metadata. The response carries the metadata that came with
    /// it.
    pub async fn unary_with(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> Result<Response<Vec<u8>>, Status> {
        let now = self.timer.now();
        let id = self.shared.with(|s| {
            s.inner.tick(now);
            cancel_stale_unary(s);
            let id = s.inner.start_unary_with(path, request, &options)?;
            s.stale_unary = Some(id);
            Ok::<_, Status>(id)
        })?;
        let result = self
            .drive(|c| match c.take_response(id) {
                Some(result) => Some(result),
                None if c.is_pending(id) => None,
                None => Some(Err(Status::cancelled("call is not active"))),
            })
            .await;
        self.shared.with(|s| s.stale_unary = None);
        result
    }

    /// Start a streaming call of `path` (`/package.Service/Method`).
    ///
    /// The request headers are sent with the first operation on the
    /// returned [`Call`]. Dropping the call before it completes cancels it.
    pub fn streaming(&self, path: &str) -> Result<Call<'_, IO, T>, Status> {
        self.streaming_with(path, CallOptions::default())
    }

    /// [`streaming`](Self::streaming) with per-call `options`. The timeout
    /// covers the whole call, not each message.
    pub fn streaming_with(
        &self,
        path: &str,
        options: CallOptions,
    ) -> Result<Call<'_, IO, T>, Status> {
        let now = self.timer.now();
        let id = self.shared.with(|s| {
            s.inner.tick(now);
            cancel_stale_unary(s);
            s.inner.start_streaming_with(path, &options)
        })?;
        Ok(Call {
            client: self,
            id,
            finished: None,
            metadata: (None, Metadata::new()),
        })
    }

    /// Run `f` with the sans-IO client state.
    ///
    /// `f` must not use this client or drop its [`Call`]s.
    pub fn with_inner<R>(&self, f: impl FnOnce(&grpc::Client) -> R) -> R {
        self.shared.with(|s| f(&s.inner))
    }

    /// Unwrap the transport.
    pub fn into_io(self) -> IO {
        self.shared
            .into_inner()
            .io
            .expect("the transport is checked in when no operation is running")
    }

    /// Check the transport out, waiting while another operation has it.
    /// `done` is evaluated first, and whenever the client state may have
    /// changed; if it returns a value, that is returned instead of the
    /// transport.
    async fn acquire<R>(
        &self,
        done: &mut impl FnMut(&mut grpc::Client) -> Option<R>,
    ) -> Result<IoGuard<'_, IO, T>, R> {
        let acquired = poll_fn(|cx| {
            let (poll, wake) = self.shared.with(|s| {
                if let Some(value) = done(&mut s.inner) {
                    return (Poll::Ready(Err(value)), None);
                }
                match s.io.take() {
                    Some(io) => {
                        s.want_io = false;
                        (Poll::Ready(Ok(io)), None)
                    }
                    None => {
                        if !s.waiters.iter().any(|w| w.will_wake(cx.waker())) {
                            s.waiters.push(cx.waker().clone());
                        }
                        // Output is waiting but the transport is busy,
                        // probably in a read: ask it to give way.
                        let wake = if s.inner.has_output() {
                            s.want_io = true;
                            s.holder.take()
                        } else {
                            None
                        };
                        (Poll::Pending, wake)
                    }
                }
            });
            if let Some(waker) = wake {
                waker.wake();
            }
            poll
        })
        .await;
        acquired.map(|io| IoGuard {
            client: self,
            io: Some(io),
        })
    }

    /// Move the connection along (write pending output, read input) until
    /// `step` returns a value.
    async fn drive<R>(&self, mut step: impl FnMut(&mut grpc::Client) -> Option<R>) -> R {
        loop {
            let now = self.timer.now();
            self.shared.with(|s| s.inner.tick(now));
            let mut guard = match self.acquire(&mut step).await {
                Ok(guard) => guard,
                Err(value) => return value,
            };
            if self.write_all(guard.io()).await {
                if let Some(value) = self.shared.with(|s| step(&mut s.inner)) {
                    return value;
                }
                self.read_once(guard.io()).await;
            }
        }
    }

    /// Write all pending output.
    async fn flush(&self) {
        let mut done = |c: &mut grpc::Client| (!c.has_output()).then_some(());
        if let Ok(mut guard) = self.acquire(&mut done).await {
            self.write_all(guard.io()).await;
        }
    }

    /// Write all pending output and flush the transport. Transport failures
    /// fail every call and return `false`.
    ///
    /// Output is copied out in chunks and only consumed once the transport
    /// accepted it, so no borrow of the client state is held while the
    /// transport is awaited.
    async fn write_all(&self, io: &mut IO) -> bool {
        let mut buf = [0u8; READ_CHUNK];
        loop {
            let n = self.shared.with(|s| {
                let pending = s.inner.pending_output();
                let n = pending.len().min(buf.len());
                buf[..n].copy_from_slice(&pending[..n]);
                n
            });
            if n == 0 {
                break;
            }
            match io.write(&buf[..n]).await {
                Ok(0) | Err(_) => {
                    self.fail_all("transport write failed");
                    return false;
                }
                Ok(written) => self.shared.with(|s| s.inner.consume_output(written)),
            }
        }
        if io.flush().await.is_err() {
            self.fail_all("transport flush failed");
            return false;
        }
        true
    }

    /// Read once from the transport. Transport failures fail every call.
    ///
    /// If another call wants to write while the read is pending, or the
    /// earliest deadline is reached, the read is dropped (it must be
    /// cancel-safe) and nothing is read.
    async fn read_once(&self, io: &mut IO) {
        let mut buf = [0u8; READ_CHUNK];
        let deadline = self.shared.with(|s| s.inner.next_deadline());
        let mut sleep = pin!(deadline.map(|d| self.timer.sleep_until(d)));
        let result = {
            let mut read = pin!(io.read(&mut buf));
            poll_fn(|cx| {
                if let Poll::Ready(result) = read.as_mut().poll(cx) {
                    return Poll::Ready(Some(result));
                }
                if let Some(sleep) = sleep.as_mut().as_pin_mut()
                    && sleep.poll(cx).is_ready()
                {
                    return Poll::Ready(None);
                }
                let yield_now = self.shared.with(|s| {
                    if !s.want_io && !s.holder.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                        s.holder = Some(cx.waker().clone());
                    }
                    s.want_io
                });
                if yield_now {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            })
            .await
        };
        let Some(result) = result else {
            return;
        };
        self.shared.with(|s| match result {
            Ok(0) => s.inner.fail_all(Status::unavailable("connection closed")),
            Ok(n) => {
                // Errors fail all pending calls inside the client.
                let _ = s.inner.recv(&buf[..n]);
            }
            Err(_) => s
                .inner
                .fail_all(Status::unavailable("transport read failed")),
        });
    }

    fn fail_all(&self, message: &'static str) {
        self.shared
            .with(|s| s.inner.fail_all(Status::unavailable(message)));
    }
}

/// Cancel a unary call whose future was dropped.
fn cancel_stale_unary<IO>(state: &mut State<IO>) {
    if let Some(stale) = state.stale_unary.take() {
        state.inner.cancel(stale);
    }
}

impl<IO: Read + Write, T: Timer> UnaryTransport for Client<IO, T> {
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> impl Future<Output = Result<Response<Vec<u8>>, Status>> {
        Client::unary_with(self, path, request, options)
    }
}

impl<IO: Read + Write, T: Timer> StreamingTransport for Client<IO, T> {
    type Call<'a>
        = Call<'a, IO, T>
    where
        Self: 'a;

    fn start(
        &self,
        path: &str,
        options: CallOptions,
    ) -> impl Future<Output = Result<Self::Call<'_>, Status>> {
        core::future::ready(self.streaming_with(path, options))
    }
}

/// An active streaming call on an async [`Client`].
///
/// Requests are sent with [`send`](Self::send) and
/// [`close_send`](Self::close_send); responses are read with
/// [`message`](Self::message). While waiting to send, received responses are
/// buffered up to the call's flow-control window, after which the server is
/// stalled; read them to let it continue. Dropping the call before
/// `message` reported the end cancels it (RST_STREAM CANCEL).
///
/// The drivers do not read while a transport write is blocked. On a
/// transport that buffers less than the data in flight (a small pipe or
/// UART buffer without a reader task), a bidirectional call that sends many
/// requests without reading the responses can block both peers in `write`.
/// Interleave `message` with `send`, or give the transport enough buffering.
#[derive(Debug)]
pub struct Call<'a, IO, T = NoTimer> {
    client: &'a Client<IO, T>,
    id: CallId,
    finished: Option<Result<(), Status>>,
    /// Response headers and trailers, taken from the client when the call
    /// finished.
    metadata: (Option<Metadata>, Metadata),
}

impl<IO: Read + Write, T: Timer> Call<'_, IO, T> {
    /// The call's id on the connection.
    pub fn id(&self) -> CallId {
        self.id
    }

    /// Send one request message, waiting while earlier messages still wait
    /// for the server's flow-control window. Messages sent after the server
    /// finished the call are discarded.
    pub async fn send(&mut self, message: &[u8]) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let client = self.client;
        let id = self.id;
        client
            .drive(|c| (c.can_send(id) || !c.is_pending(id)).then_some(()))
            .await;
        let queued = client.shared.with(|s| {
            if !s.inner.is_pending(id) {
                // Finished while waiting; the outcome is reported by `message`.
                return Ok(false);
            }
            s.inner.send_message(id, message).map(|()| true)
        })?;
        if queued {
            client.flush().await;
        }
        Ok(())
    }

    /// Half-close: no more request messages. Responses keep flowing.
    pub async fn close_send(&mut self) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let client = self.client;
        let id = self.id;
        let queued = client.shared.with(|s| {
            if !s.inner.is_pending(id) {
                return Ok(false);
            }
            s.inner.close_send(id).map(|()| true)
        })?;
        if queued {
            client.flush().await;
        }
        Ok(())
    }

    /// Next response message; `Ok(None)` once the call completed with
    /// `grpc-status: 0`, `Err` if it failed. Messages received before a
    /// failure are returned first; the outcome is repeated on later calls.
    pub async fn message(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if let Some(result) = &self.finished {
            return result.clone().map(|()| None);
        }
        let id = self.id;
        let next = self
            .client
            .drive(|c| match c.try_next(id) {
                Some(next) => Some(next),
                None if !c.is_pending(id) => {
                    Some(Next::Done(Err(Status::cancelled("call is not active"))))
                }
                None => None,
            })
            .await;
        match next {
            Next::Message(m) => Ok(Some(m)),
            Next::Done(result) => {
                self.metadata = client_take_metadata(self.client, id);
                self.finished = Some(result.clone());
                result.map(|()| None)
            }
        }
    }

    /// Metadata of the response headers, once the server has sent them. `None`
    /// before that, and for a trailers-only response.
    pub fn headers(&self) -> Option<Metadata> {
        if self.finished.is_some() {
            return self.metadata.0.clone();
        }
        self.client
            .shared
            .with(|s| s.inner.response_headers(self.id).cloned())
    }

    /// Metadata of the response trailers. `None` until the call has completed
    /// ([`message`](Self::message) returned `Ok(None)` or an error).
    pub fn trailers(&self) -> Option<Metadata> {
        self.finished.as_ref().map(|_| self.metadata.1.clone())
    }
}

fn client_take_metadata<IO, T>(client: &Client<IO, T>, id: CallId) -> (Option<Metadata>, Metadata) {
    client.shared.with(|s| s.inner.take_metadata(id))
}

impl<IO, T> Drop for Call<'_, IO, T> {
    fn drop(&mut self) {
        if self.finished.is_some() {
            return;
        }
        let id = self.id;
        let wake = self.client.shared.with(|s| {
            s.inner.cancel(id);
            // The cancellation is written by whichever operation uses the
            // transport next. If one is reading right now, ask it to flush.
            if s.io.is_none() && s.inner.has_output() {
                s.want_io = true;
                s.holder.take()
            } else {
                None
            }
        });
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

impl<IO: Read + Write, T: Timer> StreamingCall for Call<'_, IO, T> {
    fn send(&mut self, message: &[u8]) -> impl Future<Output = Result<(), Status>> {
        Call::send(self, message)
    }

    fn close_send(&mut self) -> impl Future<Output = Result<(), Status>> {
        Call::close_send(self)
    }

    fn message(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, Status>> {
        Call::message(self)
    }

    fn headers(&self) -> Option<Metadata> {
        Call::headers(self)
    }

    fn trailers(&self) -> Option<Metadata> {
        Call::trailers(self)
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use alloc::collections::{BTreeSet, VecDeque};
    use alloc::rc::Rc;
    use alloc::sync::Arc;
    use alloc::task::Wake;
    use core::cell::RefCell;
    use core::convert::Infallible;
    use core::pin::{Pin, pin};
    use core::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use core::task::Context;

    use embedded_io_async::ErrorType;

    use super::*;
    use crate::grpc::{CallContext, MethodKind};

    const GATE: &str = "/t.T/Gate";
    const OPEN: &str = "/t.T/Open";
    const QUIET: &str = "/t.T/Quiet";

    /// `Open` calls open the gate; `Gate` calls answer once it is open;
    /// `Quiet` calls never answer. Every call ends when the client half-closes.
    #[derive(Default)]
    struct Script {
        open: bool,
        sent: BTreeSet<CallId>,
        half_closed: BTreeSet<CallId>,
        cancelled: usize,
    }

    impl Handler for Script {
        fn call(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
            None
        }

        fn method_kind(&self, path: &str) -> Option<MethodKind> {
            matches!(path, GATE | OPEN | QUIET).then_some(MethodKind::BidiStreaming)
        }

        fn on_message(&mut self, ctx: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
            let path = ctx.path;
            if path == OPEN {
                self.open = true;
            }
            Ok(())
        }

        fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
            let call = ctx.id;
            self.half_closed.insert(call);
            Ok(())
        }

        fn poll_response(
            &mut self,
            ctx: &mut CallContext<'_>,
            _: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            let path = ctx.path;
            let call = ctx.id;
            if self.half_closed.remove(&call) {
                return Poll::Ready(Next::Done(Ok(())));
            }
            if path == GATE && self.open && self.sent.insert(call) {
                return Poll::Ready(Next::Message(b"open".to_vec()));
            }
            Poll::Pending
        }

        fn on_cancel(&mut self, _: &mut CallContext<'_>) {
            self.cancelled += 1;
        }
    }

    /// An in-memory peer: whatever is written is processed by a sans-IO
    /// server at once, and its output becomes readable.
    struct Peer {
        server: grpc::Server,
        script: Script,
        rx: VecDeque<u8>,
        rx_waker: Option<Waker>,
        reads_dropped: usize,
    }

    impl Peer {
        fn new() -> Rc<RefCell<Self>> {
            Rc::new(RefCell::new(Self {
                server: grpc::Server::new(ServerConfig::default()),
                script: Script::default(),
                rx: VecDeque::new(),
                rx_waker: None,
                reads_dropped: 0,
            }))
        }

        /// Run the server until it is idle (as `serve` does: delivering a
        /// request can enable a response of a call polled earlier) and make
        /// its output readable.
        fn pump(&mut self) {
            let mut cx = Context::from_waker(Waker::noop());
            let mut idle_rounds = 0;
            while idle_rounds < 2 {
                self.server.poll(&mut self.script, &mut cx);
                let n = self.server.pending_output().len();
                idle_rounds = if n == 0 { idle_rounds + 1 } else { 0 };
                self.rx.extend(self.server.pending_output());
                self.server.consume_output(n);
            }
            if !self.rx.is_empty()
                && let Some(waker) = self.rx_waker.take()
            {
                waker.wake();
            }
        }
    }

    /// The client's transport. Its `read` is cancel-safe: bytes are only taken
    /// when the read completes.
    struct Loopback(Rc<RefCell<Peer>>);

    impl ErrorType for Loopback {
        type Error = Infallible;
    }

    impl Read for Loopback {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            struct CountDrop<'a>(&'a RefCell<Peer>, bool);
            impl Drop for CountDrop<'_> {
                fn drop(&mut self) {
                    if !self.1 {
                        self.0.borrow_mut().reads_dropped += 1;
                    }
                }
            }
            let peer = &*self.0;
            let mut tracker = CountDrop(peer, false);
            let n = poll_fn(|cx| {
                let mut p = peer.borrow_mut();
                if p.rx.is_empty() {
                    p.rx_waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                let n = buf.len().min(p.rx.len());
                for byte in &mut buf[..n] {
                    *byte = p.rx.pop_front().expect("checked length");
                }
                Poll::Ready(n)
            })
            .await;
            tracker.1 = true;
            Ok(n)
        }
    }

    impl Write for Loopback {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            let mut p = self.0.borrow_mut();
            let Peer { server, script, .. } = &mut *p;
            server
                .recv(buf, script)
                .expect("valid HTTP/2 from the client");
            p.pump();
            Ok(buf.len())
        }

        async fn flush(&mut self) -> Result<(), Infallible> {
            Ok(())
        }
    }

    /// A waker that counts how often it was woken.
    #[derive(Default)]
    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, SeqCst);
        }
    }

    impl Wakes {
        fn waker() -> (Waker, Arc<Self>) {
            let wakes = Arc::new(Self::default());
            (Waker::from(wakes.clone()), wakes)
        }

        fn count(&self) -> usize {
            self.0.load(SeqCst)
        }
    }

    fn poll_once<F: Future>(f: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        f.poll(&mut Context::from_waker(waker))
    }

    /// Drive a future that only waits on the in-memory peer.
    fn run<F: Future>(f: F) -> F::Output {
        let mut f = pin!(f);
        for _ in 0..1000 {
            if let Poll::Ready(v) = poll_once(f.as_mut(), Waker::noop()) {
                return v;
            }
        }
        panic!("future did not complete");
    }

    fn client(peer: &Rc<RefCell<Peer>>) -> Client<Loopback> {
        Client::new(Loopback(peer.clone()), ClientConfig::default())
    }

    /// Call A waits for a reply that only arrives after call B sent a
    /// request. A holds the transport in a pending read; B must get the
    /// transport to write, so A has to yield.
    #[test]
    fn concurrent_reader_yields_to_a_writer_on_another_call() {
        let peer = Peer::new();
        let client = client(&peer);
        let mut a = client.streaming(GATE).unwrap();
        let mut b = client.streaming(OPEN).unwrap();
        let (wake_a, a_wakes) = Wakes::waker();
        let (wake_b, b_wakes) = Wakes::waker();

        {
            let mut a_message = pin!(a.message());
            assert!(poll_once(a_message.as_mut(), &wake_a).is_pending());
            assert_eq!(peer.borrow().reads_dropped, 0);
            assert_eq!(a_wakes.count(), 0);

            // B queues its request, but the transport is busy with A's read.
            let mut b_send = pin!(b.send(b"go"));
            assert!(poll_once(b_send.as_mut(), &wake_b).is_pending());
            assert_eq!(a_wakes.count(), 1, "A is asked to give up the transport");

            // A yields its read, which writes B's request. That opens the gate,
            // so A's reply is already there and A completes.
            let Poll::Ready(reply) = poll_once(a_message.as_mut(), &wake_a) else {
                panic!("A should complete once B's request was written");
            };
            assert_eq!(reply.unwrap().unwrap(), b"open");
            assert_eq!(peer.borrow().reads_dropped, 1);

            // B's request went out with A's turn; B is woken and completes.
            assert!(b_wakes.count() >= 1);
            assert!(matches!(
                poll_once(b_send.as_mut(), &wake_b),
                Poll::Ready(Ok(()))
            ));
        }

        // Both calls end cleanly.
        run(a.close_send()).unwrap();
        run(b.close_send()).unwrap();
        assert!(run(a.message()).unwrap().is_none());
        assert!(run(b.message()).unwrap().is_none());
    }

    /// A reader that receives data for another call wakes it.
    #[test]
    fn concurrent_waiting_call_is_woken_by_data_read_for_it() {
        let peer = Peer::new();
        let client = client(&peer);
        let mut quiet = client.streaming(QUIET).unwrap();
        let mut gate = client.streaming(GATE).unwrap();
        let (wake_q, q_wakes) = Wakes::waker();
        let (wake_g, g_wakes) = Wakes::waker();

        let mut quiet_message = pin!(quiet.message());
        assert!(poll_once(quiet_message.as_mut(), &wake_q).is_pending());

        // The gate call has nothing to write and the transport is busy: it
        // waits without asking the reader to yield.
        let mut gate_message = pin!(gate.message());
        assert!(poll_once(gate_message.as_mut(), &wake_g).is_pending());
        assert_eq!(q_wakes.count(), 0);

        // The reply for the gate call arrives. Only the reader is woken by
        // the transport; it reads the data and passes the turn on.
        {
            let mut p = peer.borrow_mut();
            p.script.open = true;
            p.pump();
        }
        assert!(poll_once(quiet_message.as_mut(), &wake_q).is_pending());
        assert!(g_wakes.count() >= 1, "the waiting call is woken");
        let Poll::Ready(reply) = poll_once(gate_message.as_mut(), &wake_g) else {
            panic!("the gate call's reply was read by the other call");
        };
        assert_eq!(reply.unwrap().unwrap(), b"open");
        assert_eq!(peer.borrow().reads_dropped, 0, "nobody had to yield");
    }

    /// Dropping a call while another call is reading gets the cancellation
    /// written promptly.
    #[test]
    fn concurrent_dropping_a_call_makes_the_reader_flush_the_cancel() {
        let peer = Peer::new();
        let client = client(&peer);
        let mut quiet = client.streaming(QUIET).unwrap();
        let mut other = client.streaming(QUIET).unwrap();
        let (wake, wakes) = Wakes::waker();

        run(other.send(b"hello")).unwrap();
        let mut quiet_message = pin!(quiet.message());
        assert!(poll_once(quiet_message.as_mut(), &wake).is_pending());
        assert_eq!(peer.borrow().script.cancelled, 0);

        drop(other);
        assert_eq!(wakes.count(), 1, "the reader is asked to yield");
        assert!(poll_once(quiet_message.as_mut(), &wake).is_pending());
        assert_eq!(peer.borrow().script.cancelled, 1);
        assert_eq!(peer.borrow().reads_dropped, 1);
    }

    /// A call that gave up waiting for the transport doesn't disturb the
    /// others.
    #[test]
    fn concurrent_abandoned_waiter_does_not_block_the_others() {
        let peer = Peer::new();
        let client = client(&peer);
        let mut gate = client.streaming(GATE).unwrap();
        let mut open = client.streaming(OPEN).unwrap();
        let (wake_g, _) = Wakes::waker();
        let (wake_o, _) = Wakes::waker();

        let mut gate_message = pin!(gate.message());
        assert!(poll_once(gate_message.as_mut(), &wake_g).is_pending());
        {
            let mut parked = pin!(open.send(b"go"));
            assert!(poll_once(parked.as_mut(), &wake_o).is_pending());
        } // the parked send is dropped without ever finishing

        // The request was queued before the send parked, so the reader still
        // flushes it and completes.
        let Poll::Ready(reply) = poll_once(gate_message.as_mut(), &wake_g) else {
            panic!("the reader should complete");
        };
        assert_eq!(reply.unwrap().unwrap(), b"open");
        // And the abandoned call can be used again.
        run(open.close_send()).unwrap();
        assert!(run(open.message()).unwrap().is_none());
    }
}
