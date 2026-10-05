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
/// a tuple of them). Dropping this future closes its owned transport and
/// cancels active streaming handlers, including while suspended in I/O.
///
/// Streaming handlers are polled with this future's waker: when one returns
/// `Poll::Pending` and later wakes the waker, the pending transport `read` is
/// dropped so the new responses can be written. Serving streaming methods
/// whose handlers return `Pending` therefore requires a cancel-safe `read`
/// (as tokio streams, [`CobsFramed`](crate::link::CobsFramed) and the
/// [`link`](crate::link) stack are); unary-only servers never drop a read.
///
/// When the connection ends, every active streaming call is reported to
/// [`Handler::on_cancel`], including when this future is dropped.
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
/// Writes and flushes are raced too; expiry during output closes the connection
/// with [`Error::OutputDeadline`] rather than reusing indeterminate wire state.
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
    let mut serving = Serving {
        server: grpc::Server::new(config),
        handler,
    };
    serve_connection(&mut io, &mut serving.server, &mut *serving.handler, &timer).await
}

struct Serving<'a, H: Handler + ?Sized> {
    server: grpc::Server,
    handler: &'a mut H,
}

impl<H: Handler + ?Sized> Drop for Serving<'_, H> {
    fn drop(&mut self) {
        self.server.cancel_all(self.handler);
    }
}

/// Output cancellation can leave an indeterminate prefix on the wire. The
/// caller must close/retire the connection if this returns `None`.
async fn output_until<T: Timer, F: Future>(
    timer: &T,
    deadline: Option<core::time::Duration>,
    output: F,
) -> Option<F::Output> {
    let mut output = pin!(output);
    let mut sleep = pin!(deadline.map(|d| timer.sleep_until(d)));
    poll_fn(|cx| {
        if deadline.is_some_and(|d| timer.now() >= d)
            || sleep
                .as_mut()
                .as_pin_mut()
                .is_some_and(|s| s.poll(cx).is_ready())
        {
            return Poll::Ready(None);
        }
        let result = output.as_mut().poll(cx);
        if deadline.is_some_and(|d| timer.now() >= d) {
            Poll::Ready(None)
        } else {
            result.map(Some)
        }
    })
    .await
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
            let n = output_until(
                timer,
                server.next_deadline(),
                io.write(server.pending_output()),
            )
            .await
            .ok_or(Error::OutputDeadline)?
            .map_err(Error::Io)?;
            if n == 0 {
                return Err(Error::WriteZero);
            }
            server.consume_output(n);
            server.tick(timer.now(), &mut *handler);
        }
        output_until(timer, server.next_deadline(), io.flush())
            .await
            .ok_or(Error::OutputDeadline)?
            .map_err(Error::Io)?;
        server.output_flushed();
        if server.is_closed() {
            return Ok(());
        }
        // Wait for input, for a streaming handler to produce output, or for
        // a deadline.
        let read = {
            let mut read = pin!(io.read(&mut buf));
            let mut sleep = pin!(server.next_deadline().map(|d| timer.sleep_until(d)));
            poll_fn(|cx| {
                server.tick(timer.now(), &mut *handler);
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
            let _ = output_until(timer, server.next_deadline(), async {
                io.write_all(server.pending_output()).await?;
                server.consume_output(server.pending_output().len());
                io.flush().await?;
                server.output_flushed();
                Ok::<(), IO::Error>(())
            })
            .await;
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
/// Without a deadline a blocked transport `write` or `flush` stops progress.
/// With a timer, expiry during output retires the whole connection because
/// dropping an output operation can leave indeterminate bytes on the wire.
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
/// on the client, except that an output deadline retires the connection and
/// no further transport I/O is attempted. Transport/protocol errors likewise
/// permanently retire the connection; buffered messages remain retrievable.
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
    waiters: Vec<(usize, Waker)>,
    next_waiter: usize,
    /// Once retired, queued bytes must never be sent again.
    terminal: Option<Status>,
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
    output_in_flight: bool,
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
        let (waiters, holder) = self.client.shared.with(|s| {
            if self.output_in_flight {
                retire(s, Status::unavailable("transport output cancelled"));
            }
            s.io = io;
            (core::mem::take(&mut s.waiters), s.holder.take())
        });
        drop(holder);
        for (_, waiter) in waiters {
            waiter.wake();
        }
    }
}

/// Owns one waiter registration even when an acquisition is cancelled.
struct Registration<'a, IO, T> {
    client: &'a Client<IO, T>,
    id: Option<usize>,
}

impl<IO, T> Drop for Registration<'_, IO, T> {
    fn drop(&mut self) {
        let removed = self.client.shared.with(|s| {
            self.id.and_then(|id| {
                s.waiters
                    .iter()
                    .position(|(key, _)| *key == id)
                    .map(|index| s.waiters.swap_remove(index))
            })
        });
        drop(removed);
    }
}

fn retire<IO>(state: &mut State<IO>, status: Status) {
    if state.terminal.is_none() {
        state.inner.fail_all(status.clone());
        state.terminal = Some(status);
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
                next_waiter: 0,
                terminal: None,
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
            if let Some(status) = &s.terminal {
                return Err(status.clone());
            }
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
            if let Some(status) = &s.terminal {
                return Err(status.clone());
            }
            s.inner.start_streaming_with(path, &options)
        })?;
        let wake = self.shared.with(|s| {
            if s.io.is_none() {
                s.want_io = true;
                s.holder.take()
            } else {
                None
            }
        });
        if let Some(waker) = wake {
            waker.wake();
        }
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
        let mut registration = Registration {
            client: self,
            id: None,
        };
        let acquired = poll_fn(|cx| {
            let (poll, wake, discarded) = self.shared.with(|s| {
                s.inner.tick(self.timer.now());
                if let Some(value) = done(&mut s.inner) {
                    return (Poll::Ready(Err(value)), None, None);
                }
                match s.io.take() {
                    Some(io) => {
                        s.want_io = false;
                        (Poll::Ready(Ok(io)), None, None)
                    }
                    None => {
                        let id = *registration.id.get_or_insert_with(|| {
                            while s.waiters.iter().any(|(id, _)| *id == s.next_waiter) {
                                s.next_waiter = s.next_waiter.wrapping_add(1);
                            }
                            let id = s.next_waiter;
                            s.next_waiter = s.next_waiter.wrapping_add(1);
                            id
                        });
                        let discarded = if let Some((_, waker)) =
                            s.waiters.iter_mut().find(|(key, _)| *key == id)
                        {
                            if waker.will_wake(cx.waker()) {
                                None
                            } else {
                                Some(core::mem::replace(waker, cx.waker().clone()))
                            }
                        } else {
                            s.waiters.push((id, cx.waker().clone()));
                            None
                        };
                        // Output is waiting but the transport is busy,
                        // probably in a read: ask it to give way.
                        let wake = if s.inner.has_output() {
                            s.want_io = true;
                            s.holder.take()
                        } else {
                            None
                        };
                        (Poll::Pending, wake, discarded)
                    }
                }
            });
            drop(discarded);
            if let Some(waker) = wake {
                waker.wake();
            }
            poll
        })
        .await;
        acquired.map(|io| IoGuard {
            client: self,
            io: Some(io),
            output_in_flight: false,
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
            if self.write_all(&mut guard).await {
                if let Some(value) = self.shared.with(|s| step(&mut s.inner)) {
                    return value;
                }
                self.read_once(guard.io()).await;
            }
        }
    }

    /// Write all pending output.
    async fn flush(&self) {
        if self.shared.with(|s| s.terminal.is_some()) {
            return;
        }
        let mut done = |c: &mut grpc::Client| (!c.has_output()).then_some(());
        if let Ok(mut guard) = self.acquire(&mut done).await {
            self.write_all(&mut guard).await;
        }
    }

    /// Write all pending output and flush the transport. Transport failures
    /// fail every call and return `false`.
    ///
    /// Output is copied out in chunks and only consumed once the transport
    /// accepted it, so no borrow of the client state is held while the
    /// transport is awaited.
    async fn write_all(&self, guard: &mut IoGuard<'_, IO, T>) -> bool {
        if self.shared.with(|s| s.terminal.is_some()) {
            return false;
        }
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
            guard.output_in_flight = true;
            let result = self.output(guard.io().write(&buf[..n])).await;
            guard.output_in_flight = false;
            match result {
                None => return false,
                Some(Ok(0) | Err(_)) => {
                    self.fail_all("transport write failed");
                    return false;
                }
                Some(Ok(written)) => self.shared.with(|s| s.inner.consume_output(written)),
            }
        }
        guard.output_in_flight = true;
        let result = self.output(guard.io().flush()).await;
        guard.output_in_flight = false;
        match result {
            None => false,
            Some(Err(_)) => {
                self.fail_all("transport flush failed");
                false
            }
            Some(Ok(())) => true,
        }
    }

    async fn output<F: Future>(&self, output: F) -> Option<F::Output> {
        let mut output = pin!(output);
        // This obligation belongs to the in-flight output, not to the core's
        // mutable call set. A concurrent tick/cancellation may remove a call,
        // but cannot make potentially non-cancel-safe output safe to reuse.
        let mut deadline = self.shared.with(|s| s.inner.next_deadline());
        let mut sleep = pin!(deadline.map(|d| self.timer.sleep_until(d)));
        poll_fn(|cx| {
            if deadline.is_some_and(|d| self.timer.now() >= d)
                || sleep
                    .as_mut()
                    .as_pin_mut()
                    .is_some_and(|s| s.poll(cx).is_ready())
            {
                self.output_expired();
                return Poll::Ready(None);
            }
            let (next, discarded) = self.shared.with(|s| {
                (
                    s.inner.next_deadline(),
                    s.holder.replace(cx.waker().clone()),
                )
            });
            drop(discarded);
            if next.is_some_and(|next| deadline.is_none_or(|armed| next < armed)) {
                deadline = next;
                sleep.set(deadline.map(|d| self.timer.sleep_until(d)));
            }
            if deadline.is_some_and(|d| self.timer.now() >= d)
                || sleep
                    .as_mut()
                    .as_pin_mut()
                    .is_some_and(|s| s.poll(cx).is_ready())
            {
                self.output_expired();
                return Poll::Ready(None);
            }
            let result = output.as_mut().poll(cx);
            // A ready transport may itself advance a platform clock.
            if deadline.is_some_and(|d| self.timer.now() >= d) {
                self.output_expired();
                Poll::Ready(None)
            } else {
                result.map(Some)
            }
        })
        .await
    }

    fn output_expired(&self) {
        self.shared.with(|s| {
            // Preserve DEADLINE_EXCEEDED for expired calls before failing peers.
            s.inner.tick(self.timer.now());
            retire(s, Status::unavailable("transport output deadline exceeded"));
        });
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
                let (yield_now, discarded) = self.shared.with(|s| {
                    let discarded = if !s.want_io
                        && !s.holder.as_ref().is_some_and(|w| w.will_wake(cx.waker()))
                    {
                        s.holder.replace(cx.waker().clone())
                    } else {
                        None
                    };
                    (s.want_io, discarded)
                });
                drop(discarded);
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
        self.shared.with(|s| {
            s.inner.tick(self.timer.now());
            match result {
                Ok(0) => retire(s, Status::unavailable("connection closed")),
                Ok(n) => {
                    if s.inner.recv(&buf[..n]).is_err() {
                        retire(s, Status::unavailable("connection protocol error"));
                    }
                }
                Err(_) => retire(s, Status::unavailable("transport read failed")),
            }
        });
    }

    fn fail_all(&self, message: &'static str) {
        self.shared
            .with(|s| retire(s, Status::unavailable(message)));
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

    #[test]
    fn cancelled_acquisitions_release_and_replace_wakers() {
        let peer = Peer::new();
        let client = client(&peer);
        let mut quiet = client.streaming(QUIET).unwrap();
        let mut other = client.streaming(QUIET).unwrap();
        let mut reader = pin!(quiet.message());
        assert!(poll_once(reader.as_mut(), Waker::noop()).is_pending());
        for _ in 0..100 {
            let (first, first_count) = Wakes::waker();
            let (second, second_count) = Wakes::waker();
            {
                let mut waiter = pin!(other.message());
                assert!(poll_once(waiter.as_mut(), &first).is_pending());
                assert_eq!(client.shared.with(|s| s.waiters.len()), 1);
                assert_eq!(Arc::strong_count(&first_count), 3);
                assert!(poll_once(waiter.as_mut(), &second).is_pending());
                assert_eq!(Arc::strong_count(&first_count), 2);
                assert_eq!(client.shared.with(|s| s.waiters.len()), 1);
            }
            assert_eq!(client.shared.with(|s| s.waiters.len()), 0);
            assert_eq!(Arc::strong_count(&second_count), 2);
        }
    }

    #[derive(Clone)]
    struct ManualTimer(Rc<core::cell::Cell<core::time::Duration>>);

    impl crate::Clock for ManualTimer {
        fn now(&self) -> core::time::Duration {
            self.0.get()
        }
    }

    impl Timer for ManualTimer {
        fn sleep_until(&self, deadline: core::time::Duration) -> impl Future<Output = ()> {
            poll_fn(move |_| {
                if self.0.get() >= deadline {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
        }
    }

    #[derive(Clone, Copy)]
    enum Fault {
        Partial,
        Flush,
        Read,
        Eof,
        Protocol,
        WritePending,
        FlushPending,
        FlushWaking,
    }

    struct FaultIo {
        fault: Fault,
        activity: Rc<core::cell::Cell<usize>>,
        writes: usize,
    }

    impl ErrorType for FaultIo {
        type Error = embedded_io_async::ErrorKind;
    }

    impl Write for FaultIo {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
            self.activity.set(self.activity.get() + 1);
            self.writes += 1;
            match self.fault {
                Fault::Partial if self.writes == 1 => Ok(1),
                Fault::Partial => Err(embedded_io_async::ErrorKind::BrokenPipe),
                Fault::WritePending => {
                    poll_fn(|_| {
                        self.activity.set(self.activity.get() + 1);
                        Poll::Pending
                    })
                    .await
                }
                _ => Ok(buf.len()),
            }
        }
        async fn flush(&mut self) -> Result<(), Self::Error> {
            self.activity.set(self.activity.get() + 1);
            match self.fault {
                Fault::Flush => Err(embedded_io_async::ErrorKind::BrokenPipe),
                Fault::FlushPending => {
                    poll_fn(|_| {
                        self.activity.set(self.activity.get() + 1);
                        Poll::Pending
                    })
                    .await
                }
                Fault::FlushWaking => {
                    poll_fn(|cx| {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    })
                    .await
                }
                _ => Ok(()),
            }
        }
    }

    impl Read for FaultIo {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            self.activity.set(self.activity.get() + 1);
            match self.fault {
                Fault::Read => Err(embedded_io_async::ErrorKind::BrokenPipe),
                Fault::Protocol => {
                    // Invalid SETTINGS frame on stream 1.
                    buf[..9].copy_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 1]);
                    Ok(9)
                }
                _ => Ok(0),
            }
        }
    }

    fn fault_io(fault: Fault) -> (FaultIo, Rc<core::cell::Cell<usize>>) {
        let activity = Rc::new(core::cell::Cell::new(0));
        (
            FaultIo {
                fault,
                activity: activity.clone(),
                writes: 0,
            },
            activity,
        )
    }

    #[test]
    fn transport_failures_permanently_retire_async_clients() {
        for fault in [
            Fault::Partial,
            Fault::Flush,
            Fault::Read,
            Fault::Eof,
            Fault::Protocol,
        ] {
            let (io, activity) = fault_io(fault);
            let mut client = Client::new(io, ClientConfig::default());
            assert_eq!(
                run(client.unary("/t.T/Echo", b"x")).unwrap_err().code,
                grpc::Code::Unavailable
            );
            let count = activity.get();
            assert_eq!(
                run(client.unary("/t.T/Echo", b"again")).unwrap_err().code,
                grpc::Code::Unavailable
            );
            assert!(client.streaming(QUIET).is_err());
            run(client.flush());
            assert_eq!(activity.get(), count, "retired connection performed I/O");
        }
    }

    #[test]
    fn output_deadlines_retire_pending_writes_and_flushes() {
        use core::time::Duration;
        for fault in [Fault::WritePending, Fault::FlushPending, Fault::FlushWaking] {
            let (io, activity) = fault_io(fault);
            let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
            let client = Client::with_timer(io, ClientConfig::default(), ManualTimer(now.clone()));
            let mut expired = client
                .streaming_with(QUIET, CallOptions::timeout(Duration::from_millis(100)))
                .unwrap();
            let mut other = client.streaming(QUIET).unwrap();
            {
                let mut message = pin!(expired.message());
                assert!(poll_once(message.as_mut(), Waker::noop()).is_pending());
                for _ in 0..4 {
                    assert!(poll_once(message.as_mut(), Waker::noop()).is_pending());
                }
                now.set(Duration::from_millis(100));
                let Poll::Ready(Err(status)) = poll_once(message.as_mut(), Waker::noop()) else {
                    panic!("output deadline did not finish the call");
                };
                assert_eq!(status.code, grpc::Code::DeadlineExceeded);
            }
            let count = activity.get();
            assert_eq!(
                run(other.message()).unwrap_err().code,
                grpc::Code::Unavailable
            );
            assert_eq!(
                run(expired.message()).unwrap_err().code,
                grpc::Code::DeadlineExceeded
            );
            assert!(client.streaming(QUIET).is_err());
            assert_eq!(activity.get(), count);
        }
    }

    #[test]
    fn concurrent_tick_cannot_disarm_expired_output_deadline() {
        use core::time::Duration;
        for fault in [Fault::WritePending, Fault::FlushPending] {
            for later_timeout in [None, Some(Duration::from_secs(10))] {
                let (io, activity) = fault_io(fault);
                let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
                let client =
                    Client::with_timer(io, ClientConfig::default(), ManualTimer(now.clone()));
                let mut first = client
                    .streaming_with(QUIET, CallOptions::timeout(Duration::from_millis(100)))
                    .unwrap();
                let mut second;
                let count;
                {
                    let mut pending = pin!(first.message());
                    assert!(poll_once(pending.as_mut(), Waker::noop()).is_pending());
                    now.set(Duration::from_millis(100));
                    let mut options = CallOptions::default();
                    options.timeout = later_timeout;
                    // Starting another call ticks the core and removes the
                    // expired first call from next_deadline().
                    second = client.streaming_with(QUIET, options).unwrap();
                    assert_eq!(
                        client.with_inner(|c| c.next_deadline()),
                        later_timeout.map(|d| d + now.get())
                    );
                    count = activity.get();
                    let Poll::Ready(Err(status)) = poll_once(pending.as_mut(), Waker::noop())
                    else {
                        panic!("concurrent tick disarmed an expired output deadline");
                    };
                    assert_eq!(status.code, grpc::Code::DeadlineExceeded);
                    assert!(client.shared.with(|s| s.terminal.is_some()));
                }
                assert_eq!(
                    run(second.message()).unwrap_err().code,
                    grpc::Code::Unavailable
                );
                assert_eq!(
                    run(first.message()).unwrap_err().code,
                    grpc::Code::DeadlineExceeded
                );
                assert!(client.streaming(QUIET).is_err());
                run(client.flush());
                assert_eq!(
                    activity.get(),
                    count,
                    "retirement must not perform more I/O"
                );
            }
        }
    }

    #[test]
    fn cancellation_cannot_disarm_an_armed_output_deadline() {
        use core::time::Duration;
        for fault in [Fault::WritePending, Fault::FlushPending] {
            let (io, activity) = fault_io(fault);
            let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
            let client = Client::with_timer(io, ClientConfig::default(), ManualTimer(now.clone()));
            let mut first = client.streaming(QUIET).unwrap();
            let cancelled = client
                .streaming_with(QUIET, CallOptions::timeout(Duration::from_millis(100)))
                .unwrap();
            let count;
            {
                let mut pending = pin!(first.message());
                assert!(poll_once(pending.as_mut(), Waker::noop()).is_pending());
                drop(cancelled);
                assert_eq!(client.with_inner(|c| c.next_deadline()), None);
                now.set(Duration::from_millis(50));
                assert!(poll_once(pending.as_mut(), Waker::noop()).is_pending());
                count = activity.get();
                now.set(Duration::from_millis(100));
                let Poll::Ready(Err(status)) = poll_once(pending.as_mut(), Waker::noop()) else {
                    panic!("cancellation disarmed in-flight output");
                };
                assert_eq!(status.code, grpc::Code::Unavailable);
            }
            assert!(client.streaming(QUIET).is_err());
            run(client.flush());
            assert_eq!(activity.get(), count);
        }
    }

    #[test]
    fn dropping_pending_output_retires_the_client() {
        for fault in [Fault::WritePending, Fault::FlushPending] {
            let (io, activity) = fault_io(fault);
            let mut client = Client::new(io, ClientConfig::default());
            {
                let mut call = pin!(client.unary("/t.T/Echo", b"x"));
                assert!(poll_once(call.as_mut(), Waker::noop()).is_pending());
            }
            let count = activity.get();
            assert_eq!(
                run(client.unary("/t.T/Echo", b"again")).unwrap_err().code,
                grpc::Code::Unavailable
            );
            assert_eq!(activity.get(), count);
        }
    }

    struct ServerIo {
        request: Option<Vec<u8>>,
        input_seen: bool,
        stall: Fault,
    }

    impl ErrorType for ServerIo {
        type Error = Infallible;
    }
    impl Read for ServerIo {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            if let Some(bytes) = self.request.take() {
                buf[..bytes.len()].copy_from_slice(&bytes);
                self.input_seen = true;
                Ok(bytes.len())
            } else {
                core::future::pending().await
            }
        }
    }
    impl Write for ServerIo {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            if self.input_seen && matches!(self.stall, Fault::WritePending) {
                core::future::pending().await
            } else {
                Ok(buf.len())
            }
        }
        async fn flush(&mut self) -> Result<(), Infallible> {
            if self.input_seen && matches!(self.stall, Fault::FlushPending) {
                core::future::pending().await
            } else {
                Ok(())
            }
        }
    }

    fn server_io(stall: Fault, timeout: Option<core::time::Duration>) -> ServerIo {
        let mut client = grpc::Client::new(ClientConfig::default());
        let mut options = CallOptions::default();
        options.timeout = timeout;
        let id = client.start_streaming_with(QUIET, &options).unwrap();
        client.send_message(id, b"request").unwrap();
        ServerIo {
            request: Some(client.take_output()),
            input_seen: false,
            stall,
        }
    }

    #[test]
    fn dropping_server_in_read_write_or_flush_cancels_once() {
        for stall in [Fault::Read, Fault::WritePending, Fault::FlushPending] {
            let mut handler = Script::default();
            {
                let mut server = pin!(serve(
                    server_io(stall, None),
                    &mut handler,
                    ServerConfig::default()
                ));
                assert!(poll_once(server.as_mut(), Waker::noop()).is_pending());
            }
            assert_eq!(handler.cancelled, 1);
        }
    }

    #[test]
    fn server_output_deadline_cancels_once() {
        use core::time::Duration;
        for stall in [Fault::WritePending, Fault::FlushPending] {
            let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
            let mut handler = Script::default();
            {
                let mut server = pin!(serve_with_timer(
                    server_io(stall, Some(Duration::from_millis(100))),
                    &mut handler,
                    ServerConfig::default(),
                    ManualTimer(now.clone()),
                ));
                assert!(poll_once(server.as_mut(), Waker::noop()).is_pending());
                now.set(Duration::from_millis(100));
                assert!(matches!(
                    poll_once(server.as_mut(), Waker::noop()),
                    Poll::Ready(Err(Error::OutputDeadline))
                ));
            }
            assert_eq!(handler.cancelled, 1);
        }
    }

    struct CompletedHandler {
        unary: bool,
        completed: Rc<core::cell::Cell<usize>>,
        cancelled: Rc<core::cell::Cell<usize>>,
    }

    impl Handler for CompletedHandler {
        fn call(&mut self, _: &mut CallContext<'_>, req: &[u8]) -> Option<Result<Vec<u8>, Status>> {
            assert!(self.unary);
            self.completed.set(self.completed.get() + 1);
            Some(Ok(req.to_vec()))
        }
        fn method_kind(&self, _: &str) -> Option<MethodKind> {
            (!self.unary).then_some(MethodKind::ServerStreaming)
        }
        fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
            Ok(())
        }
        fn poll_response(
            &mut self,
            _: &mut CallContext<'_>,
            _: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            self.completed.set(self.completed.get() + 1);
            Poll::Ready(Next::Done(Ok(())))
        }
        fn on_cancel(&mut self, _: &mut CallContext<'_>) {
            self.cancelled.set(self.cancelled.get() + 1);
        }
    }

    struct CompletedOutputIo {
        input: ServerIo,
        completed: Rc<core::cell::Cell<usize>>,
        written: Rc<RefCell<Vec<u8>>>,
        activity: Rc<core::cell::Cell<usize>>,
    }
    impl ErrorType for CompletedOutputIo {
        type Error = Infallible;
    }
    impl Read for CompletedOutputIo {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            self.activity.set(self.activity.get() + 1);
            self.input.read(buf).await
        }
    }
    impl Write for CompletedOutputIo {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            if self.completed.get() > 0 && matches!(self.input.stall, Fault::WritePending) {
                poll_fn(|_| {
                    self.activity.set(self.activity.get() + 1);
                    Poll::<()>::Pending
                })
                .await;
            }
            self.activity.set(self.activity.get() + 1);
            self.written.borrow_mut().extend_from_slice(buf);
            Ok(buf.len())
        }
        async fn flush(&mut self) -> Result<(), Infallible> {
            if self.completed.get() > 0 && matches!(self.input.stall, Fault::FlushPending) {
                poll_fn(|_| {
                    self.activity.set(self.activity.get() + 1);
                    Poll::<()>::Pending
                })
                .await;
            }
            self.activity.set(self.activity.get() + 1);
            Ok(())
        }
    }

    #[test]
    fn completed_server_responses_keep_deadlines_through_write_and_flush() {
        use core::time::Duration;
        for unary in [true, false] {
            for stall in [Fault::WritePending, Fault::FlushPending, Fault::Read] {
                let mut client = grpc::Client::new(ClientConfig::default());
                let options = CallOptions::timeout(Duration::from_millis(100));
                let id = if unary {
                    client
                        .start_unary_with("/t.T/Echo", b"request", &options)
                        .unwrap()
                } else {
                    let id = client.start_streaming_with(QUIET, &options).unwrap();
                    client.send_message(id, b"request").unwrap();
                    client.close_send(id).unwrap();
                    id
                };
                let completed = Rc::new(core::cell::Cell::new(0));
                let cancelled = Rc::new(core::cell::Cell::new(0));
                let written = Rc::new(RefCell::new(Vec::new()));
                let activity = Rc::new(core::cell::Cell::new(0));
                let io = CompletedOutputIo {
                    input: ServerIo {
                        request: Some(client.take_output()),
                        input_seen: false,
                        stall,
                    },
                    completed: completed.clone(),
                    written: written.clone(),
                    activity: activity.clone(),
                };
                let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
                let mut handler = CompletedHandler {
                    unary,
                    completed: completed.clone(),
                    cancelled: cancelled.clone(),
                };
                {
                    let mut server = pin!(serve_with_timer(
                        io,
                        &mut handler,
                        ServerConfig::default(),
                        ManualTimer(now.clone())
                    ));
                    assert!(poll_once(server.as_mut(), Waker::noop()).is_pending());
                    assert_eq!(
                        completed.get(),
                        1,
                        "handler must finish before output stalls"
                    );
                    if !matches!(stall, Fault::WritePending) {
                        client.recv(&written.borrow()).unwrap();
                        if unary {
                            assert_eq!(
                                client.take_response(id).unwrap().unwrap().message,
                                b"request"
                            );
                        } else {
                            assert!(matches!(client.try_next(id), Some(Next::Done(Ok(())))));
                        }
                    }
                    let count = activity.get();
                    now.set(Duration::from_millis(100));
                    let result = poll_once(server.as_mut(), Waker::noop());
                    if matches!(stall, Fault::Read) {
                        // A successful flush must acknowledge completion and
                        // disarm the deadline before the next pending read.
                        assert!(result.is_pending());
                    } else {
                        assert!(matches!(result, Poll::Ready(Err(Error::OutputDeadline))));
                        assert_eq!(
                            activity.get(),
                            count,
                            "expired output must not be polled again"
                        );
                    }
                }
                assert_eq!(completed.get(), 1);
                assert_eq!(
                    cancelled.get(),
                    0,
                    "finished handlers must not be cancelled again"
                );
            }
        }
    }

    struct LateInput {
        server: grpc::Server,
        now: ManualTimer,
    }
    impl ErrorType for LateInput {
        type Error = Infallible;
    }
    impl Write for LateInput {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            self.server
                .recv(
                    buf,
                    &mut grpc::FnHandler(|_: &str, req: &[u8]| Some(Ok(req.to_vec()))),
                )
                .unwrap();
            Ok(buf.len())
        }
        async fn flush(&mut self) -> Result<(), Infallible> {
            Ok(())
        }
    }
    impl Read for LateInput {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            self.now.0.set(core::time::Duration::from_millis(101));
            let n = self.server.pending_output().len();
            buf[..n].copy_from_slice(self.server.pending_output());
            self.server.consume_output(n);
            Ok(n)
        }
    }

    #[test]
    fn fresh_clock_rejects_a_late_ready_response() {
        use core::time::Duration;
        let timer = ManualTimer(Rc::new(core::cell::Cell::new(Duration::ZERO)));
        let io = LateInput {
            server: grpc::Server::new(ServerConfig::default()),
            now: timer.clone(),
        };
        let mut client = Client::with_timer(io, ClientConfig::default(), timer);
        let status = run(client.unary_with(
            "/t.T/Echo",
            b"x",
            CallOptions::timeout(Duration::from_millis(100)),
        ))
        .unwrap_err();
        assert_eq!(status.code, grpc::Code::DeadlineExceeded);
    }

    #[test]
    fn retirement_keeps_buffered_messages_before_terminal_status() {
        let peer = Peer::new();
        peer.borrow_mut().script.open = true;
        let client = client(&peer);
        let mut call = client.streaming(GATE).unwrap();
        run(call.send(b"x")).unwrap();
        let bytes: Vec<u8> = peer.borrow_mut().rx.drain(..).collect();
        client.shared.with(|s| s.inner.recv(&bytes).unwrap());
        client.fail_all("scripted transport failure");
        assert_eq!(run(call.message()).unwrap().unwrap(), b"open");
        assert_eq!(
            run(call.message()).unwrap_err().code,
            grpc::Code::Unavailable
        );
        assert_eq!(
            run(call.message()).unwrap_err().code,
            grpc::Code::Unavailable
        );
    }

    #[test]
    fn earlier_deadline_rearms_pending_output() {
        use core::time::Duration;
        let (io, _) = fault_io(Fault::FlushPending);
        let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
        let client = Client::with_timer(io, ClientConfig::default(), ManualTimer(now.clone()));
        let mut first = client
            .streaming_with(QUIET, CallOptions::timeout(Duration::from_secs(10)))
            .unwrap();
        let (wake, wakes) = Wakes::waker();
        let mut pending = pin!(first.message());
        assert!(poll_once(pending.as_mut(), &wake).is_pending());
        let mut earlier = client
            .streaming_with(QUIET, CallOptions::timeout(Duration::from_millis(100)))
            .unwrap();
        assert_eq!(wakes.count(), 1, "new deadline wakes the output holder");
        assert!(poll_once(pending.as_mut(), &wake).is_pending());
        now.set(Duration::from_millis(100));
        let Poll::Ready(Err(status)) = poll_once(pending.as_mut(), &wake) else {
            panic!("connection not retired");
        };
        assert_eq!(status.code, grpc::Code::Unavailable);
        assert_eq!(
            run(earlier.message()).unwrap_err().code,
            grpc::Code::DeadlineExceeded
        );
    }

    struct ReadyAfterDeadline {
        timer: ManualTimer,
        late_polls: Rc<core::cell::Cell<usize>>,
        cancellations: Rc<core::cell::Cell<usize>>,
    }
    impl Handler for ReadyAfterDeadline {
        fn call(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
            None
        }
        fn method_kind(&self, _: &str) -> Option<MethodKind> {
            Some(MethodKind::BidiStreaming)
        }
        fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
            Ok(())
        }
        fn poll_response(
            &mut self,
            _: &mut CallContext<'_>,
            _: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            if self.timer.0.get() > core::time::Duration::from_millis(100) {
                self.late_polls.set(self.late_polls.get() + 1);
                Poll::Ready(Next::Done(Ok(())))
            } else {
                Poll::Pending
            }
        }
        fn on_cancel(&mut self, _: &mut CallContext<'_>) {
            self.cancellations.set(self.cancellations.get() + 1);
        }
    }

    #[test]
    fn server_expires_before_polling_a_newly_ready_handler() {
        use core::time::Duration;
        let timer = ManualTimer(Rc::new(core::cell::Cell::new(Duration::ZERO)));
        let late_polls = Rc::new(core::cell::Cell::new(0));
        let cancellations = Rc::new(core::cell::Cell::new(0));
        let mut handler = ReadyAfterDeadline {
            timer: timer.clone(),
            late_polls: late_polls.clone(),
            cancellations: cancellations.clone(),
        };
        {
            let mut server = pin!(serve_with_timer(
                server_io(Fault::Read, Some(Duration::from_millis(100))),
                &mut handler,
                ServerConfig::default(),
                timer.clone()
            ));
            assert!(poll_once(server.as_mut(), Waker::noop()).is_pending());
            timer.0.set(Duration::from_millis(101));
            assert!(poll_once(server.as_mut(), Waker::noop()).is_pending());
            assert_eq!(cancellations.get(), 1);
            assert_eq!(late_polls.get(), 0);
        }
        assert_eq!(cancellations.get(), 1);
    }
}
