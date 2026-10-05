//! Blocking drivers over `embedded_io` (feature `blocking`).
//!
//! Same behaviour as the async [`serve`](crate::serve) and
//! [`Client`](crate::Client), for transports without an executor.

use alloc::vec::Vec;
use core::cell::RefCell;
use core::task::{Context, Waker};
use core::time::Duration;

use embedded_io::{Error as _, ErrorKind, Read, Write};

use crate::grpc::{
    self, BlockingStreamingCall, BlockingStreamingTransport, BlockingUnaryTransport, CallId,
    CallOptions, ClientConfig, Handler, Metadata, Next, Response, ServerConfig, Status,
};
use crate::timer::{Clock, NoTimer};
use crate::{Error, READ_CHUNK};

/// A blocking transport whose reads can be given a timeout.
///
/// Needed to enforce deadlines with a blocking driver: the driver sets the
/// timeout to the time left until the earliest deadline before each read, so
/// the read returns in time. A read that times out must fail with
/// [`ErrorKind::TimedOut`] (or [`ErrorKind::Interrupted`]). Note that `std`
/// sockets report timeouts as `WouldBlock` on Unix, which `embedded_io` maps to
/// [`ErrorKind::Other`]; map it to `TimedOut` in the transport adapter.
pub trait ReadTimeout {
    /// Make later reads fail after `timeout` without data, or block without
    /// limit for `None`. The timeout applies to every following read until it
    /// is changed.
    fn set_read_timeout(&mut self, timeout: Option<Duration>);
}

/// Optional output timeout capabilities for whole-call deadline enforcement.
///
/// Each hook must bound the next `write` or `flush`, including buffered output
/// and acknowledgment waits. `Some(Duration::ZERO)` must not block; `None`
/// removes the bound. Timeout errors must use `ErrorKind::TimedOut`. A driver
/// retires the connection after any output error because accepted bytes may
/// be indeterminate. Read timeouts alone cannot bound output stalls.
pub trait OutputTimeout {
    /// Bound each following write until changed.
    fn set_write_timeout(&mut self, timeout: Option<Duration>);
    /// Bound each following flush, including acknowledgment waits, until changed.
    fn set_flush_timeout(&mut self, timeout: Option<Duration>);
}

fn set_output_timeout<IO: OutputTimeout>(io: &mut IO, timeout: Option<Duration>, flush: bool) {
    if flush {
        io.set_flush_timeout(timeout);
    } else {
        io.set_write_timeout(timeout);
    }
}

fn timed_out(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::TimedOut | ErrorKind::Interrupted)
}

fn remaining(next: Option<Duration>, now: Duration) -> Option<Duration> {
    next.map(|d| d.saturating_sub(now))
}

/// Serve gRPC on one connection until the peer disconnects or the connection fails.
///
/// Streaming handlers are polled with a no-op waker before every read, so a
/// handler that returns `Poll::Pending` is polled again only once the read
/// returns. To keep such streams moving without client traffic, give the
/// transport a read timeout that surfaces as [`ErrorKind::TimedOut`] (or
/// [`ErrorKind::Interrupted`]): those errors are treated as an idle tick, not
/// a failure. Note that `std` sockets report timeouts as `WouldBlock` on Unix,
/// which `embedded_io` maps to [`ErrorKind::Other`]; map it to `TimedOut` in
/// the transport adapter.
///
/// A transport that can wait for input *or* a wake-up avoids the timeout: see
/// [`WakeableRead`] and [`serve_wakeable`].
///
/// When the connection ends, every active streaming call is reported to
/// [`Handler::on_cancel`].
///
/// `serve` has no clock, so it doesn't enforce `grpc-timeout` deadlines; use
/// [`serve_with_clock`] for that.
pub fn serve<IO, H>(
    mut io: IO,
    handler: &mut H,
    config: ServerConfig,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
{
    let mut server = grpc::Server::new(config);
    let mut cx = Context::from_waker(Waker::noop());
    let result = serve_connection(
        &mut io,
        &mut server,
        handler,
        &mut cx,
        &NoTimer,
        |_, _, _| {},
        |io, buf, _| io.read(buf).map(Some),
    );
    server.cancel_all(handler);
    result
}

/// Serve with deadline-aware reads, writes and flushes.
///
/// The transport must implement both timeout capabilities honestly, including
/// flush acknowledgment waits. Any output error closes the connection and
/// cancels active handlers. Synchronous handler execution cannot be preempted.
pub fn serve_with_io_timeouts<IO, H, C>(
    mut io: IO,
    handler: &mut H,
    config: ServerConfig,
    clock: C,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write + ReadTimeout + OutputTimeout,
    H: Handler + ?Sized,
    C: Clock,
{
    let mut server = grpc::Server::new(config);
    let mut cx = Context::from_waker(Waker::noop());
    let result = serve_connection(
        &mut io,
        &mut server,
        handler,
        &mut cx,
        &clock,
        set_output_timeout::<IO>,
        |io, buf, timeout| {
            io.set_read_timeout(timeout);
            io.read(buf).map(Some)
        },
    );
    server.cancel_all(handler);
    result
}

/// Like [`serve`], and enforces the `grpc-timeout` deadline of each call using
/// `clock`.
///
/// A call's deadline starts when its request headers arrive. When it is
/// reached the call is ended with `DEADLINE_EXCEEDED` (streaming handlers are
/// told through [`Handler::on_cancel`]). A unary handler that is running when
/// its deadline passes can't be preempted.
///
/// Before each read the transport's read timeout is set to the time left until
/// the earliest deadline (see [`ReadTimeout`]); the resulting
/// [`ErrorKind::TimedOut`] is an idle tick, as in [`serve`]. Writes and flushes
/// are not bounded; use [`serve_with_io_timeouts`] to bound output too.
pub fn serve_with_clock<IO, H, C>(
    mut io: IO,
    handler: &mut H,
    config: ServerConfig,
    clock: C,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write + ReadTimeout,
    H: Handler + ?Sized,
    C: Clock,
{
    let mut server = grpc::Server::new(config);
    let mut cx = Context::from_waker(Waker::noop());
    let result = serve_connection(
        &mut io,
        &mut server,
        handler,
        &mut cx,
        &clock,
        |_, _, _| {},
        |io, buf, timeout| {
            io.set_read_timeout(timeout);
            io.read(buf).map(Some)
        },
    );
    server.cancel_all(handler);
    result
}

/// A handle that wakes a [`WakeableRead`] transport.
///
/// [`serve_wakeable`] turns it into the [`Waker`] handlers receive in
/// `poll_response`. Calling [`wake`](Self::wake) must make the transport's
/// in-progress [`read_or_wake`](WakeableRead::read_or_wake) return `Ok(None)`,
/// or the next one if none is in progress. It may be called from any thread
/// or from an interrupt handler, so it must not block.
#[cfg(target_has_atomic = "ptr")]
#[derive(Clone)]
pub struct WakeHandle(alloc::sync::Arc<dyn Fn() + Send + Sync>);

#[cfg(target_has_atomic = "ptr")]
impl WakeHandle {
    /// Create a handle that calls `wake`.
    pub fn new(wake: impl Fn() + Send + Sync + 'static) -> Self {
        Self(alloc::sync::Arc::new(wake))
    }

    /// Wake the transport.
    pub fn wake(&self) {
        (self.0)();
    }

    /// Convert into a [`Waker`] that calls [`wake`](Self::wake).
    pub fn into_waker(self) -> Waker {
        Waker::from(alloc::sync::Arc::new(self))
    }
}

#[cfg(target_has_atomic = "ptr")]
impl alloc::task::Wake for WakeHandle {
    fn wake(self: alloc::sync::Arc<Self>) {
        (self.0)();
    }

    fn wake_by_ref(self: &alloc::sync::Arc<Self>) {
        (self.0)();
    }
}

#[cfg(target_has_atomic = "ptr")]
impl core::fmt::Debug for WakeHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("WakeHandle")
    }
}

/// A blocking transport that can wait for input *or* a wake-up.
///
/// A plain [`Read`] can't be interrupted, so [`serve`] notices that a
/// handler became ready only when a read returns or times out. Implementing
/// this trait lets [`serve_wakeable`] react to a handler's wake immediately.
/// How to wait for both is up to the transport (a condition variable, an
/// event flag with `WFE`/`SEV`, an interrupt that also ends the UART read, a
/// self-pipe, ...).
///
/// # Contract
///
/// - Every call of the [`WakeHandle`] returned by
///   [`wake_handle`](Self::wake_handle) is *latched*: the next
///   [`read_or_wake`](Self::read_or_wake) that has not consumed it returns
///   `Ok(None)` without blocking. A wake that arrives while the server is
///   polling handlers or writing, i.e. while no read is in progress, must not
///   be lost.
/// - Wakes may be coalesced, and spurious `Ok(None)` results are fine.
/// - A wake is never reported as end of stream: `Ok(Some(0))` means the peer
///   closed the connection, as for [`Read::read`].
#[cfg(target_has_atomic = "ptr")]
pub trait WakeableRead: Read {
    /// A handle that wakes [`read_or_wake`](Self::read_or_wake). It is
    /// requested once per [`serve_wakeable`] call.
    fn wake_handle(&self) -> WakeHandle;

    /// Like [`Read::read`], but returns `Ok(None)` when woken through a
    /// [`WakeHandle`] before data arrived.
    fn read_or_wake(&mut self, buf: &mut [u8]) -> Result<Option<usize>, Self::Error>;

    /// [`read_or_wake`](Self::read_or_wake) that also gives up after `timeout`
    /// (`None`: no limit), either by returning `Ok(None)` or by failing with
    /// [`ErrorKind::TimedOut`]. Used by [`serve_wakeable_with_clock`] so that
    /// the wait ends when the earliest deadline is reached.
    ///
    /// The default ignores `timeout`; deadlines are then only enforced when a
    /// read returns for another reason.
    fn read_or_wake_timeout(
        &mut self,
        buf: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<Option<usize>, Self::Error> {
        let _ = timeout;
        self.read_or_wake(buf)
    }
}

/// Like [`serve`], for a transport that can wait for input or a wake-up.
///
/// Handlers receive a waker backed by [`WakeableRead::wake_handle`]. Waking it
/// ends the wait for input, so a streaming handler that returned
/// `Poll::Pending` and is woken later (from a timer, an interrupt or another
/// thread) is polled again, and its output written, without any peer traffic
/// and without a transport read timeout.
///
/// Read errors of kind [`ErrorKind::TimedOut`] or [`ErrorKind::Interrupted`]
/// are still treated as idle ticks, as in [`serve`].
///
/// When the connection ends, every active streaming call is reported to
/// [`Handler::on_cancel`].
#[cfg(target_has_atomic = "ptr")]
pub fn serve_wakeable<IO, H>(
    mut io: IO,
    handler: &mut H,
    config: ServerConfig,
) -> Result<(), Error<IO::Error>>
where
    IO: WakeableRead + Write,
    H: Handler + ?Sized,
{
    let mut server = grpc::Server::new(config);
    let waker = io.wake_handle().into_waker();
    let mut cx = Context::from_waker(&waker);
    let result = serve_connection(
        &mut io,
        &mut server,
        handler,
        &mut cx,
        &NoTimer,
        |_, _, _| {},
        |io, buf, _| io.read_or_wake(buf),
    );
    server.cancel_all(handler);
    result
}

/// [`serve_wakeable`] that also enforces `grpc-timeout` deadlines using
/// `clock`, like [`serve_with_clock`].
///
/// The time left until the earliest deadline is passed to
/// [`WakeableRead::read_or_wake_timeout`].
#[cfg(target_has_atomic = "ptr")]
pub fn serve_wakeable_with_clock<IO, H, C>(
    mut io: IO,
    handler: &mut H,
    config: ServerConfig,
    clock: C,
) -> Result<(), Error<IO::Error>>
where
    IO: WakeableRead + Write,
    H: Handler + ?Sized,
    C: Clock,
{
    let mut server = grpc::Server::new(config);
    let waker = io.wake_handle().into_waker();
    let mut cx = Context::from_waker(&waker);
    let result = serve_connection(
        &mut io,
        &mut server,
        handler,
        &mut cx,
        &clock,
        |_, _, _| {},
        |io, buf, timeout| io.read_or_wake_timeout(buf, timeout),
    );
    server.cancel_all(handler);
    result
}

/// The serving loop shared by [`serve`] and [`serve_wakeable`] and their
/// clock variants.
///
/// `read` waits for input, at most for the timeout it is given (the time left
/// until the earliest deadline; it may ignore it). `Ok(None)` means it was
/// woken: the handlers are polled again. Wakes that arrive before `read` is
/// entered must make it return `Ok(None)` right away; that is the transport's
/// job.
fn serve_connection<IO, H, C, R>(
    io: &mut IO,
    server: &mut grpc::Server,
    handler: &mut H,
    cx: &mut Context<'_>,
    clock: &C,
    set_output: fn(&mut IO, Option<Duration>, bool),
    mut read: R,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
    C: Clock,
    R: FnMut(&mut IO, &mut [u8], Option<Duration>) -> Result<Option<usize>, IO::Error>,
{
    let mut buf = [0u8; READ_CHUNK];
    loop {
        server.tick(clock.now(), &mut *handler);
        // Producing output can make room for more, so poll until idle.
        loop {
            server.tick(clock.now(), &mut *handler);
            server.poll(&mut *handler, cx);
            if !server.has_output() {
                break;
            }
            set_output(io, remaining(server.next_deadline(), clock.now()), false);
            let n = io.write(server.pending_output()).map_err(Error::Io)?;
            if n == 0 {
                return Err(Error::WriteZero);
            }
            server.consume_output(n);
        }
        set_output(io, remaining(server.next_deadline(), clock.now()), true);
        io.flush().map_err(Error::Io)?;
        server.tick(clock.now(), &mut *handler);
        server.output_flushed();
        if server.has_output() {
            continue;
        }
        if server.is_closed() {
            return Ok(());
        }
        let timeout = remaining(server.next_deadline(), clock.now());
        let n = match read(io, &mut buf, timeout) {
            Ok(None) => continue,
            Ok(Some(0)) => return Ok(()),
            Ok(Some(n)) => n,
            Err(e) if timed_out(e.kind()) => continue,
            Err(e) => return Err(Error::Io(e)),
        };
        // Calls that start with this input get their deadline from now.
        server.tick(clock.now(), &mut *handler);
        if let Err(e) = server.recv(&buf[..n], handler) {
            if io.write_all(server.pending_output()).is_ok() {
                server.consume_output(server.pending_output().len());
                if io.flush().is_ok() {
                    server.output_flushed();
                }
            }
            return Err(Error::Protocol(e));
        }
    }
}

/// Blocking gRPC client on one connection.
///
/// Implements [`BlockingUnaryTransport`] and [`BlockingStreamingTransport`]
/// for generated `<Service>BlockingClient`s.
///
/// Several streaming [`Call`]s can be active on one connection; drive them
/// from one thread by interleaving their operations. A blocking read can't be
/// interrupted, so a `message()` waits until its own call has something to
/// return, even if another call needs to send first. Send on every call that
/// the peer waits for before blocking on a response. Unary calls take
/// `&mut self`, so they can't run while a [`Call`] exists. The client is not
/// `Sync`.
///
/// # Deadlines
///
/// Calls can have a timeout ([`CallOptions`], [`ClientConfig::default_timeout`]),
/// which is sent to the server as `grpc-timeout`. A client built with
/// [`with_clock`](Self::with_clock) also enforces it: when the call's time is
/// up it fails with `DEADLINE_EXCEEDED` and the stream is reset. Messages
/// that were already received are still delivered first. The transport's read
/// timeout is set to the time left, so a read that finds no data returns in
/// time; see [`ReadTimeout`]. A client built with [`new`](Self::new) has no
/// clock and leaves enforcement to the server.
///
/// `with_clock` only bounds reads: a blocked write or flush can postpone
/// expiry indefinitely. Use [`with_io_timeouts`](Self::with_io_timeouts) for
/// deadline-aware output too. Transport/protocol failures permanently retire
/// the connection; new calls are rejected and no further I/O is attempted,
/// but buffered messages and terminal statuses remain retrievable.
#[derive(Debug)]
pub struct Client<IO, C = NoTimer> {
    state: RefCell<State<IO>>,
    clock: C,
    /// Sets the transport's read timeout; a no-op without a clock.
    set_timeout: fn(&mut IO, Option<Duration>),
    set_output_timeout: Option<fn(&mut IO, Option<Duration>, bool)>,
    /// Read timeouts are idle ticks, not failures.
    idle_timeouts: bool,
}

#[derive(Debug)]
struct State<IO> {
    io: IO,
    inner: grpc::Client,
    /// The read timeout last set on the transport.
    timeout: Option<Duration>,
    terminal: Option<Status>,
}

impl<IO: Read + Write> Client<IO> {
    /// Create a client without a clock; the HTTP/2 preface is sent with the
    /// first call. Timeouts are sent to the server but not enforced locally;
    /// see [`with_clock`](Self::with_clock).
    pub fn new(io: IO, config: ClientConfig) -> Self {
        Self::build(io, config, NoTimer, |_, _| {}, false)
    }
}

impl<IO: Read + Write + ReadTimeout, C: Clock> Client<IO, C> {
    /// Create a client that enforces call timeouts using `clock`; the HTTP/2
    /// preface is sent with the first call. Only reads are bounded; writes and
    /// flushes can stall indefinitely. See [`with_io_timeouts`](Self::with_io_timeouts).
    ///
    /// Read errors of kind [`ErrorKind::TimedOut`] or [`ErrorKind::Interrupted`]
    /// are treated as idle ticks, not as transport failures.
    pub fn with_clock(io: IO, config: ClientConfig, clock: C) -> Self {
        Self::build(io, config, clock, IO::set_read_timeout, true)
    }
}

impl<IO: Read + Write + ReadTimeout + OutputTimeout, C: Clock> Client<IO, C> {
    /// Enforce deadlines across reads, writes and flushes. Unlike
    /// [`with_clock`](Self::with_clock), output stalls are bounded too.
    /// Any output timeout retires this connection; expired calls retain
    /// `DEADLINE_EXCEEDED`, other calls fail with `UNAVAILABLE`.
    pub fn with_io_timeouts(io: IO, config: ClientConfig, clock: C) -> Self {
        let mut client = Self::with_clock(io, config, clock);
        client.set_output_timeout = Some(set_output_timeout::<IO>);
        client
    }
}

fn retire<IO>(state: &mut State<IO>, status: Status) {
    if state.terminal.is_none() {
        state.inner.fail_all(status.clone());
        state.terminal = Some(status);
    }
}

impl<IO: Read + Write, C: Clock> Client<IO, C> {
    fn build(
        io: IO,
        config: ClientConfig,
        clock: C,
        set_timeout: fn(&mut IO, Option<Duration>),
        idle_timeouts: bool,
    ) -> Self {
        Self {
            state: RefCell::new(State {
                io,
                inner: grpc::Client::new(config),
                timeout: None,
                terminal: None,
            }),
            clock,
            set_timeout,
            set_output_timeout: None,
            idle_timeouts,
        }
    }

    /// Report the current time to the call state, expiring calls.
    fn tick(&self) {
        let now = self.clock.now();
        self.with(|s| s.inner.tick(now));
    }

    /// Perform one unary call.
    pub fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        self.unary_with(path, request, CallOptions::default())
            .map(Response::into_message)
    }

    /// Perform one unary call with per-call `options`, for example a timeout
    /// or request metadata. The response carries the metadata that came with
    /// it.
    pub fn unary_with(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> Result<Response<Vec<u8>>, Status> {
        self.tick();
        let id = self.with(|s| {
            if let Some(status) = &s.terminal {
                return Err(status.clone());
            }
            s.inner.start_unary_with(path, request, &options)
        })?;
        loop {
            self.tick();
            if let Some(result) = self.with(|s| s.inner.take_response(id)) {
                return result;
            }
            if self.write_output() && self.with(|s| s.inner.is_pending(id)) {
                self.read_input(id);
            }
        }
    }

    /// Start a streaming call of `path` (`/package.Service/Method`).
    ///
    /// The request headers are sent with the first operation on the
    /// returned [`Call`]. Dropping the call before it completes cancels it.
    pub fn streaming(&self, path: &str) -> Result<Call<'_, IO, C>, Status> {
        self.streaming_with(path, CallOptions::default())
    }

    /// [`streaming`](Self::streaming) with per-call `options`. The timeout
    /// covers the whole call, not each message.
    pub fn streaming_with(
        &self,
        path: &str,
        options: CallOptions,
    ) -> Result<Call<'_, IO, C>, Status> {
        self.tick();
        let id = self.with(|s| {
            if let Some(status) = &s.terminal {
                return Err(status.clone());
            }
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
        self.with(|s| f(&s.inner))
    }

    /// Unwrap the transport.
    pub fn into_io(self) -> IO {
        self.state.into_inner().io
    }

    fn with<R>(&self, f: impl FnOnce(&mut State<IO>) -> R) -> R {
        f(&mut self.state.borrow_mut())
    }

    /// Write all pending output; `false` (and every call failed) if the
    /// transport failed.
    fn write_output(&self) -> bool {
        self.with(|s| {
            if s.terminal.is_some() {
                return false;
            }
            s.inner.tick(self.clock.now());
            while s.inner.has_output() {
                let deadline = s.inner.next_deadline();
                if let Some(set) = self.set_output_timeout {
                    set(&mut s.io, remaining(deadline, self.clock.now()), false);
                }
                let result = s.io.write(s.inner.pending_output());
                match result {
                    Ok(n) if n > 0 => s.inner.consume_output(n),
                    _ => {
                        s.inner.tick(self.clock.now());
                        retire(s, Status::unavailable("transport write failed"));
                        return false;
                    }
                }
                s.inner.tick(self.clock.now());
                if self.set_output_timeout.is_some()
                    && deadline.is_some_and(|d| self.clock.now() >= d)
                {
                    retire(s, Status::unavailable("transport output deadline exceeded"));
                    return false;
                }
            }
            let deadline = s.inner.next_deadline();
            if let Some(set) = self.set_output_timeout {
                set(&mut s.io, remaining(deadline, self.clock.now()), true);
            }
            let result = s.io.flush();
            s.inner.tick(self.clock.now());
            if result.is_err() {
                retire(s, Status::unavailable("transport flush failed"));
                return false;
            }
            if self.set_output_timeout.is_some() && deadline.is_some_and(|d| self.clock.now() >= d)
            {
                retire(s, Status::unavailable("transport output deadline exceeded"));
                return false;
            }
            true
        })
    }

    fn read_input(&self, id: CallId) {
        let mut buf = [0u8; READ_CHUNK];
        let idle_timeouts = self.idle_timeouts;
        self.with(|s| {
            if s.terminal.is_some() {
                return;
            }
            s.inner.tick(self.clock.now());
            // Output/expiry may have completed the last call. Never start an
            // unbounded read after its deadline has already elapsed.
            if !s.inner.is_pending(id) {
                return;
            }
            let timeout = remaining(s.inner.next_deadline(), self.clock.now());
            if timeout != s.timeout {
                (self.set_timeout)(&mut s.io, timeout);
                s.timeout = timeout;
            }
            let result = s.io.read(&mut buf);
            s.inner.tick(self.clock.now());
            match result {
                Ok(0) => retire(s, Status::unavailable("connection closed")),
                Ok(n) => {
                    if s.inner.recv(&buf[..n]).is_err() {
                        retire(s, Status::unavailable("connection protocol error"));
                    }
                }
                Err(e) if idle_timeouts && timed_out(e.kind()) => {}
                Err(_) => retire(s, Status::unavailable("transport read failed")),
            }
        });
    }
}

impl<IO: Read + Write, C: Clock> BlockingUnaryTransport for Client<IO, C> {
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> Result<Response<Vec<u8>>, Status> {
        Client::unary_with(self, path, request, options)
    }
}

impl<IO: Read + Write, C: Clock> BlockingStreamingTransport for Client<IO, C> {
    type Call<'a>
        = Call<'a, IO, C>
    where
        Self: 'a;

    fn start(&self, path: &str, options: CallOptions) -> Result<Self::Call<'_>, Status> {
        self.streaming_with(path, options)
    }
}

/// An active streaming call on a blocking [`Client`].
///
/// See the async [`Call`](crate::Call) for the semantics. Dropping the call
/// before [`message`](Self::message) reported the end cancels it.
#[derive(Debug)]
pub struct Call<'a, IO, C = NoTimer> {
    client: &'a Client<IO, C>,
    id: CallId,
    finished: Option<Result<(), Status>>,
    /// Response headers and trailers, taken from the client when the call
    /// finished.
    metadata: (Option<Metadata>, Metadata),
}

impl<IO: Read + Write, C: Clock> Call<'_, IO, C> {
    /// The call's id on the connection.
    pub fn id(&self) -> CallId {
        self.id
    }

    /// Send one request message, blocking while earlier messages still wait
    /// for the server's flow-control window.
    pub fn send(&mut self, message: &[u8]) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let client = self.client;
        let id = self.id;
        loop {
            client.tick();
            if client.with(|s| s.inner.can_send(id) || !s.inner.is_pending(id)) {
                break;
            }
            if client.write_output() && !client.with(|s| s.inner.can_send(id)) {
                client.read_input(id);
            }
        }
        let queued = client.with(|s| {
            if !s.inner.is_pending(id) {
                return Ok(false);
            }
            s.inner.send_message(id, message).map(|()| true)
        })?;
        if queued {
            client.write_output();
        }
        Ok(())
    }

    /// Half-close: no more request messages. Responses keep flowing.
    pub fn close_send(&mut self) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let client = self.client;
        let id = self.id;
        let queued = client.with(|s| {
            if !s.inner.is_pending(id) {
                return Ok(false);
            }
            s.inner.close_send(id).map(|()| true)
        })?;
        if queued {
            client.write_output();
        }
        Ok(())
    }

    /// Next response message; `Ok(None)` once the call completed with
    /// `grpc-status: 0`, `Err` if it failed.
    pub fn message(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if let Some(result) = &self.finished {
            return result.clone().map(|()| None);
        }
        let client = self.client;
        let id = self.id;
        loop {
            client.tick();
            match client.with(|s| s.inner.try_next(id)) {
                Some(Next::Message(m)) => return Ok(Some(m)),
                Some(Next::Done(result)) => {
                    self.metadata = client.with(|s| s.inner.take_metadata(id));
                    self.finished = Some(result.clone());
                    return result.map(|()| None);
                }
                None if !client.with(|s| s.inner.is_pending(id)) => {
                    let status = Status::cancelled("call is not active");
                    self.finished = Some(Err(status.clone()));
                    return Err(status);
                }
                None => {}
            }
            if client.write_output() && client.with(|s| s.inner.is_pending(id)) {
                client.read_input(id);
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
            .with(|s| s.inner.response_headers(self.id).cloned())
    }

    /// Metadata of the response trailers. `None` until the call has completed
    /// ([`message`](Self::message) returned `Ok(None)` or an error).
    pub fn trailers(&self) -> Option<Metadata> {
        self.finished.as_ref().map(|_| self.metadata.1.clone())
    }
}

impl<IO, C> Drop for Call<'_, IO, C> {
    fn drop(&mut self) {
        if self.finished.is_some() {
            return;
        }
        // Never panic in `drop`: the state is only borrowed here if a
        // `with_inner` closure drops a call, which is documented as not
        // allowed.
        if let Ok(mut s) = self.client.state.try_borrow_mut() {
            s.inner.cancel(self.id);
        }
    }
}

impl<IO: Read + Write, C: Clock> BlockingStreamingCall for Call<'_, IO, C> {
    fn send(&mut self, message: &[u8]) -> Result<(), Status> {
        Call::send(self, message)
    }

    fn close_send(&mut self) -> Result<(), Status> {
        Call::close_send(self)
    }

    fn message(&mut self) -> Result<Option<Vec<u8>>, Status> {
        Call::message(self)
    }

    fn headers(&self) -> Option<Metadata> {
        Call::headers(self)
    }

    fn trailers(&self) -> Option<Metadata> {
        Call::trailers(self)
    }
}
