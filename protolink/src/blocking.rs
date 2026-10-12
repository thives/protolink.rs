//! Blocking drivers over `embedded_io` (feature `blocking`).
//!
//! Same behaviour as the async [`server`](crate::server) and
//! [`client`](crate::client) factories, for transports without an executor:
//! [`server`] builds a [`Server`] and [`client`] a [`Client`].
//!
//! Blocking factories have no default deadline policy: choose
//! `.with_deadline_clock(clock)` (which needs a transport that can bound its
//! reads, [`ReadTimeout`]) or `.without_deadlines()`. Further capabilities,
//! [`with_output_timeouts`](ServerBuilder::with_output_timeouts) and
//! [`with_wakeable_reads`](ServerBuilder::with_wakeable_reads), are only
//! available when the transport implements the matching trait.

use alloc::vec::Vec;
use core::cell::RefCell;
use core::task::{Context, Waker};
use core::time::Duration;

use embedded_io::{Error as _, ErrorKind, Read, Write};

use crate::builder::{DeadlinesUnspecified, client_config_methods, server_config_methods};
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

/// A handle that wakes a [`WakeableRead`] transport.
///
/// A server built [`with_wakeable_reads`](ServerBuilder::with_wakeable_reads)
/// turns it into the [`Waker`] handlers receive in
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
/// A plain [`Read`] can't be interrupted, so a server notices that a
/// handler became ready only when a read returns or times out. Implementing
/// this trait lets a server built
/// [`with_wakeable_reads`](ServerBuilder::with_wakeable_reads) react to a
/// handler's wake immediately.
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
    /// requested once, when [`Server::serve`] starts.
    fn wake_handle(&self) -> WakeHandle;

    /// Like [`Read::read`], but returns `Ok(None)` when woken through a
    /// [`WakeHandle`] before data arrived.
    fn read_or_wake(&mut self, buf: &mut [u8]) -> Result<Option<usize>, Self::Error>;
}

/// A [`WakeableRead`] transport whose wait can also be bounded by a timeout.
///
/// Required to combine
/// [`with_wakeable_reads`](ServerBuilder::with_wakeable_reads) with
/// [`with_deadline_clock`](ServerBuilder::with_deadline_clock): the server
/// passes the time left until the earliest deadline, so that the wait ends
/// when it is reached. There is deliberately no default implementation; one
/// that ignored `timeout` would silently disable deadline enforcement.
#[cfg(target_has_atomic = "ptr")]
pub trait WakeableReadTimeout: WakeableRead {
    /// [`read_or_wake`](WakeableRead::read_or_wake) that also gives up after
    /// `timeout` (`None`: no limit), either by returning `Ok(None)` or by
    /// failing with [`ErrorKind::TimedOut`].
    fn read_or_wake_timeout(
        &mut self,
        buf: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<Option<usize>, Self::Error>;
}

/// Start building a blocking gRPC server on one connection.
///
/// Configure the factory with `.with_*` methods, choose a deadline policy and
/// the transport capabilities to use, then [`build`](ServerBuilder::build) it
/// and [`serve`](Server::serve) the application's services:
///
/// | Factory method | Transport must implement | Effect |
/// |---|---|---|
/// | [`without_deadlines`](ServerBuilder::without_deadlines) | `Read + Write` | `grpc-timeout` is not enforced |
/// | [`with_deadline_clock`](ServerBuilder::with_deadline_clock) | [`ReadTimeout`] | Reads are bounded by the earliest deadline |
/// | [`with_output_timeouts`](ServerBuilder::with_output_timeouts) | [`OutputTimeout`] | Writes and flushes are bounded too |
/// | [`with_wakeable_reads`](ServerBuilder::with_wakeable_reads) | [`WakeableRead`] | Streaming handlers wake the read |
/// | `with_wakeable_reads` + `with_deadline_clock` | [`WakeableReadTimeout`] | Wakeable reads bounded by deadlines |
///
/// ```no_run
/// # fn demo<IO, C, H>(io: IO, clock: C, services: &mut H)
/// # where IO: embedded_io::Read + embedded_io::Write + protolink::blocking::ReadTimeout,
/// #       C: protolink::Clock, H: protolink::Handler {
/// let server = protolink::blocking::server(io)
///     .with_deadline_clock(clock)
///     .build();
/// server.serve(services).ok();
/// # }
/// ```
///
/// Building performs no I/O. A deadline policy is required, and each
/// capability is only available when the transport implements it:
///
/// ```compile_fail
/// # fn demo<IO: embedded_io::Read + embedded_io::Write>(io: IO) {
/// // No deadline policy: `build` is not available.
/// let server = protolink::blocking::server(io).build();
/// # }
/// ```
///
/// ```compile_fail
/// # fn demo<IO: embedded_io::Read + embedded_io::Write, C: protolink::Clock>(io: IO, clock: C) {
/// // A deadline clock needs a transport that implements `ReadTimeout`.
/// let server = protolink::blocking::server(io).with_deadline_clock(clock).build();
/// # }
/// ```
///
/// ```compile_fail
/// # use protolink::blocking::ReadTimeout;
/// # fn demo<IO: embedded_io::Read + embedded_io::Write + ReadTimeout, C: protolink::Clock>(io: IO, clock: C) {
/// // Output timeouts need a transport that implements `OutputTimeout`.
/// let server = protolink::blocking::server(io)
///     .with_deadline_clock(clock)
///     .with_output_timeouts()
///     .build();
/// # }
/// ```
///
/// ```compile_fail
/// # use protolink::blocking::{ReadTimeout, WakeableRead};
/// # fn demo<IO: WakeableRead + embedded_io::Write + ReadTimeout, C: protolink::Clock>(io: IO, clock: C) {
/// // Wakeable reads bounded by deadlines need `WakeableReadTimeout`.
/// let server = protolink::blocking::server(io)
///     .with_wakeable_reads()
///     .with_deadline_clock(clock)
///     .build();
/// # }
/// ```
pub fn server<IO: Read + Write>(io: IO) -> ServerBuilder<IO> {
    ServerBuilder {
        io,
        config: ServerConfig::default(),
        deadlines: DeadlinesUnspecified,
        reads: PlainReads,
        set_output: no_output_timeout::<IO>,
    }
}

/// Deadline policy of a blocking factory: enforce `grpc-timeout` with the
/// clock `C`. Set by `with_deadline_clock`.
#[derive(Debug, Clone, Copy)]
pub struct DeadlineClock<C>(C);

/// Deadline policy of a blocking factory: no local enforcement. Set by
/// `without_deadlines`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoDeadlines;

/// Read mode of a blocking server: plain [`Read::read`], the default.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlainReads;

/// Read mode of a blocking server: [`WakeableRead::read_or_wake`]. Set by
/// [`ServerBuilder::with_wakeable_reads`].
#[cfg(target_has_atomic = "ptr")]
#[derive(Debug, Clone, Copy, Default)]
pub struct WakeableReads;

type OutputFn<IO> = fn(&mut IO, Option<Duration>, bool);

fn no_output_timeout<IO>(_: &mut IO, _: Option<Duration>, _: bool) {}

/// Factory of a blocking [`Server`], created by [`server`].
///
/// `D` is the deadline policy ([`DeadlinesUnspecified`], [`NoDeadlines`] or
/// [`DeadlineClock`]) and `R` the read mode ([`PlainReads`] or
/// `WakeableReads`).
pub struct ServerBuilder<IO, D = DeadlinesUnspecified, R = PlainReads> {
    io: IO,
    config: ServerConfig,
    deadlines: D,
    reads: R,
    set_output: OutputFn<IO>,
}

impl<IO: core::fmt::Debug, D: core::fmt::Debug, R: core::fmt::Debug> core::fmt::Debug
    for ServerBuilder<IO, D, R>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ServerBuilder")
            .field("io", &self.io)
            .field("config", &self.config)
            .field("deadlines", &self.deadlines)
            .field("reads", &self.reads)
            .finish_non_exhaustive()
    }
}

impl<IO, D, R> ServerBuilder<IO, D, R> {
    /// Enforce each call's `grpc-timeout` with `clock`.
    ///
    /// A call's deadline starts when its request headers arrive. When it is
    /// reached the call ends with `DEADLINE_EXCEEDED` (streaming handlers are
    /// told through [`Handler::on_cancel`]). A unary handler that is running
    /// when its deadline passes can't be preempted.
    ///
    /// Before each read the transport's read timeout is set to the time left
    /// until the earliest deadline, so the transport must implement
    /// [`ReadTimeout`] (or [`WakeableReadTimeout`] with
    /// [`with_wakeable_reads`](Self::with_wakeable_reads)); `build` requires
    /// it. Writes and flushes stay unbounded unless
    /// [`with_output_timeouts`](ServerBuilder::with_output_timeouts) is chosen
    /// too.
    pub fn with_deadline_clock<C: Clock>(self, clock: C) -> ServerBuilder<IO, DeadlineClock<C>, R> {
        ServerBuilder {
            io: self.io,
            config: self.config,
            deadlines: DeadlineClock(clock),
            reads: self.reads,
            set_output: self.set_output,
        }
    }

    /// Do not enforce deadlines locally. Calls still receive their
    /// `grpc-timeout` in [`CallContext::deadline`](crate::CallContext::deadline),
    /// on a clock that never advances. Clears
    /// [`with_output_timeouts`](ServerBuilder::with_output_timeouts).
    pub fn without_deadlines(self) -> ServerBuilder<IO, NoDeadlines, R> {
        ServerBuilder {
            io: self.io,
            config: self.config,
            deadlines: NoDeadlines,
            reads: self.reads,
            set_output: no_output_timeout::<IO>,
        }
    }

    /// Wait for input *or* a wake-up of a streaming handler, see
    /// [`WakeableRead`].
    ///
    /// Handlers receive a waker backed by [`WakeableRead::wake_handle`].
    /// Waking it ends the wait for input, so a streaming handler that returned
    /// `Poll::Pending` and is woken later (from a timer, an interrupt or
    /// another thread) is polled again, and its output written, without any
    /// peer traffic and without a transport read timeout. With a deadline
    /// clock the transport must implement [`WakeableReadTimeout`].
    #[cfg(target_has_atomic = "ptr")]
    pub fn with_wakeable_reads(self) -> ServerBuilder<IO, D, WakeableReads>
    where
        IO: WakeableRead,
    {
        ServerBuilder {
            io: self.io,
            config: self.config,
            deadlines: self.deadlines,
            reads: WakeableReads,
            set_output: self.set_output,
        }
    }

    server_config_methods!();
}

impl<IO, C, R> ServerBuilder<IO, DeadlineClock<C>, R> {
    /// Also bound writes and flushes by the earliest deadline, through the
    /// transport's [`OutputTimeout`].
    ///
    /// The transport must implement both timeout capabilities honestly,
    /// including flush acknowledgment waits. Any output error closes the
    /// connection and cancels active handlers.
    pub fn with_output_timeouts(mut self) -> Self
    where
        IO: OutputTimeout,
    {
        self.set_output = set_output_timeout::<IO>;
        self
    }
}

impl<IO: Read + Write> ServerBuilder<IO, NoDeadlines, PlainReads> {
    /// Construct the server. Performs no I/O.
    pub fn build(self) -> Server<IO, NoTimer> {
        Server::from_parts(
            self.io,
            self.config,
            NoTimer,
            |io, buf, _| io.read(buf).map(Some),
            None,
            self.set_output,
        )
    }
}

impl<IO: Read + Write + ReadTimeout, C: Clock> ServerBuilder<IO, DeadlineClock<C>, PlainReads> {
    /// Construct the server. Performs no I/O.
    pub fn build(self) -> Server<IO, C> {
        Server::from_parts(
            self.io,
            self.config,
            self.deadlines.0,
            |io, buf, timeout| {
                io.set_read_timeout(timeout);
                io.read(buf).map(Some)
            },
            None,
            self.set_output,
        )
    }
}

#[cfg(target_has_atomic = "ptr")]
impl<IO: WakeableRead + Write> ServerBuilder<IO, NoDeadlines, WakeableReads> {
    /// Construct the server. Performs no I/O; the wake handle is requested
    /// when [`serve`](Server::serve) starts.
    pub fn build(self) -> Server<IO, NoTimer> {
        Server::from_parts(
            self.io,
            self.config,
            NoTimer,
            |io, buf, _| io.read_or_wake(buf),
            Some(IO::wake_handle),
            self.set_output,
        )
    }
}

#[cfg(target_has_atomic = "ptr")]
impl<IO: WakeableReadTimeout + Write, C: Clock> ServerBuilder<IO, DeadlineClock<C>, WakeableReads> {
    /// Construct the server. Performs no I/O; the wake handle is requested
    /// when [`serve`](Server::serve) starts.
    pub fn build(self) -> Server<IO, C> {
        Server::from_parts(
            self.io,
            self.config,
            self.deadlines.0,
            |io, buf, timeout| io.read_or_wake_timeout(buf, timeout),
            Some(IO::wake_handle),
            self.set_output,
        )
    }
}

type ReadFn<IO> = fn(
    &mut IO,
    &mut [u8],
    Option<Duration>,
) -> Result<Option<usize>, <IO as embedded_io::ErrorType>::Error>;

#[cfg(target_has_atomic = "ptr")]
type WakeFn<IO> = fn(&IO) -> WakeHandle;
#[cfg(not(target_has_atomic = "ptr"))]
type WakeFn<IO> = fn(&IO) -> core::convert::Infallible;

/// A blocking gRPC server driver for one connection, built by [`server`].
///
/// It owns the transport, the configuration and the deadline clock. The
/// application's services are only joined to it by [`serve`](Self::serve).
pub struct Server<IO: embedded_io::ErrorType, C = NoTimer> {
    io: IO,
    config: ServerConfig,
    clock: C,
    read: ReadFn<IO>,
    wake: Option<WakeFn<IO>>,
    set_output: OutputFn<IO>,
}

impl<IO: embedded_io::ErrorType + core::fmt::Debug, C: core::fmt::Debug> core::fmt::Debug
    for Server<IO, C>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Server")
            .field("io", &self.io)
            .field("config", &self.config)
            .field("clock", &self.clock)
            .field("wakeable", &self.wake.is_some())
            .finish_non_exhaustive()
    }
}

impl<IO: Read + Write, C: Clock> Server<IO, C> {
    fn from_parts(
        io: IO,
        config: ServerConfig,
        clock: C,
        read: ReadFn<IO>,
        wake: Option<WakeFn<IO>>,
        set_output: OutputFn<IO>,
    ) -> Self {
        Self {
            io,
            config,
            clock,
            read,
            wake,
            set_output,
        }
    }

    /// Serve gRPC on this connection until the peer disconnects or the
    /// connection fails.
    ///
    /// `services` (a generated `<Service>Dispatch`, a
    /// [`Registry`](crate::Registry), or any [`Handler`]) is borrowed
    /// for the whole connection and receives every call on it; call state
    /// keyed by [`CallId`] is therefore connection-local.
    ///
    /// With plain reads, streaming handlers are polled with a no-op waker
    /// before every read, so a handler that returns `Poll::Pending` is polled
    /// again only once the read returns. To keep such streams moving without
    /// client traffic, give the transport a read timeout that surfaces as
    /// [`ErrorKind::TimedOut`] (or [`ErrorKind::Interrupted`]): those errors
    /// are treated as an idle tick, not a failure. Note that `std` sockets
    /// report timeouts as `WouldBlock` on Unix, which `embedded_io` maps to
    /// [`ErrorKind::Other`]; map it to `TimedOut` in the transport adapter.
    /// [`with_wakeable_reads`](ServerBuilder::with_wakeable_reads) avoids the
    /// timeout.
    ///
    /// When the connection ends, normally or with an error, every active
    /// streaming call is reported to [`Handler::on_cancel`] before this
    /// returns. If a handler panics, no cleanup is attempted.
    pub fn serve<H: Handler + ?Sized>(self, services: &mut H) -> Result<(), Error<IO::Error>> {
        let Self {
            mut io,
            config,
            clock,
            read,
            wake,
            set_output,
        } = self;
        let mut server = grpc::Server::new(config);
        #[cfg(target_has_atomic = "ptr")]
        let waker = match wake {
            Some(wake_handle) => wake_handle(&io).into_waker(),
            None => Waker::noop().clone(),
        };
        #[cfg(not(target_has_atomic = "ptr"))]
        let waker = {
            let _ = wake;
            Waker::noop().clone()
        };
        let mut cx = Context::from_waker(&waker);
        let result = serve_connection(
            &mut io,
            &mut server,
            services,
            &mut cx,
            &clock,
            set_output,
            read,
        );
        server.cancel_all(services);
        result
    }
}

/// The serving loop of every [`Server`] configuration.
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
/// which is sent to the server as `grpc-timeout`. A client built
/// [`with_deadline_clock`](ClientBuilder::with_deadline_clock) also enforces
/// it: when the call's time is up it fails with `DEADLINE_EXCEEDED` and the
/// stream is reset. Messages that were already received are still delivered
/// first. The transport's read timeout is set to the time left, so a read that
/// finds no data returns in time; see [`ReadTimeout`]. A client built
/// [`without_deadlines`](ClientBuilder::without_deadlines) has no clock and
/// leaves enforcement to the server.
///
/// A deadline clock only bounds reads: a blocked write or flush can postpone
/// expiry indefinitely. Add
/// [`with_output_timeouts`](ClientBuilder::with_output_timeouts) for
/// deadline-aware output too. Transport/protocol failures permanently retire
/// the connection; new calls are rejected and no further I/O is attempted,
/// but buffered messages and terminal statuses remain retrievable.
#[derive(Debug)]
pub struct Client<IO, C = NoTimer> {
    state: RefCell<State<IO>>,
    clock: C,
    /// Sets the transport's read timeout; a no-op without a clock.
    set_timeout: fn(&mut IO, Option<Duration>),
    set_output_timeout: Option<OutputFn<IO>>,
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

/// Start building a blocking gRPC client on one connection.
///
/// ```no_run
/// # fn demo<IO, C>(io: IO, clock: C)
/// # where IO: embedded_io::Read + embedded_io::Write + protolink::blocking::ReadTimeout,
/// #       C: protolink::Clock {
/// let transport = protolink::blocking::client(io)
///     .with_deadline_clock(clock)
///     .build();
/// // Hand `transport` to a generated `<Service>BlockingClient::new`.
/// # let _ = transport;
/// # }
/// ```
///
/// Building performs no I/O: the HTTP/2 preface is sent with the first call.
/// A deadline policy is required, as for [`server`], and a deadline clock
/// needs a transport that can bound its reads:
///
/// ```compile_fail
/// # fn demo<IO: embedded_io::Read + embedded_io::Write, C: protolink::Clock>(io: IO, clock: C) {
/// let transport = protolink::blocking::client(io).with_deadline_clock(clock).build();
/// # }
/// ```
pub fn client<IO: Read + Write>(io: IO) -> ClientBuilder<IO> {
    ClientBuilder {
        io,
        config: ClientConfig::default(),
        deadlines: DeadlinesUnspecified,
        set_output: None,
    }
}

/// Factory of a blocking [`Client`], created by [`client`].
///
/// `D` is the deadline policy: [`DeadlinesUnspecified`], [`NoDeadlines`] or
/// [`DeadlineClock`].
pub struct ClientBuilder<IO, D = DeadlinesUnspecified> {
    io: IO,
    config: ClientConfig,
    deadlines: D,
    set_output: Option<OutputFn<IO>>,
}

impl<IO: core::fmt::Debug, D: core::fmt::Debug> core::fmt::Debug for ClientBuilder<IO, D> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("io", &self.io)
            .field("config", &self.config)
            .field("deadlines", &self.deadlines)
            .field("output_timeouts", &self.set_output.is_some())
            .finish()
    }
}

impl<IO, D> ClientBuilder<IO, D> {
    /// Enforce call timeouts locally with `clock`, see
    /// [Deadlines](Client#deadlines). The transport must implement
    /// [`ReadTimeout`]; `build` requires it. Read errors of kind
    /// [`ErrorKind::TimedOut`] or [`ErrorKind::Interrupted`] are then treated
    /// as idle ticks, not as transport failures.
    pub fn with_deadline_clock<C: Clock>(self, clock: C) -> ClientBuilder<IO, DeadlineClock<C>> {
        ClientBuilder {
            io: self.io,
            config: self.config,
            deadlines: DeadlineClock(clock),
            set_output: self.set_output,
        }
    }

    /// Do not enforce timeouts locally. They are still sent as
    /// `grpc-timeout`, so a server with a clock enforces them. Clears
    /// [`with_output_timeouts`](ClientBuilder::with_output_timeouts).
    pub fn without_deadlines(self) -> ClientBuilder<IO, NoDeadlines> {
        ClientBuilder {
            io: self.io,
            config: self.config,
            deadlines: NoDeadlines,
            set_output: None,
        }
    }

    client_config_methods!();
}

impl<IO, C> ClientBuilder<IO, DeadlineClock<C>> {
    /// Also bound writes and flushes by the earliest deadline, through the
    /// transport's [`OutputTimeout`]. Any output timeout retires the
    /// connection; expired calls retain `DEADLINE_EXCEEDED`, other calls fail
    /// with `UNAVAILABLE`.
    pub fn with_output_timeouts(mut self) -> Self
    where
        IO: OutputTimeout,
    {
        self.set_output = Some(set_output_timeout::<IO>);
        self
    }
}

impl<IO: Read + Write> ClientBuilder<IO, NoDeadlines> {
    /// Construct the client. Performs no I/O: the HTTP/2 preface is sent with
    /// the first call.
    pub fn build(self) -> Client<IO, NoTimer> {
        Client::from_parts(self.io, self.config, NoTimer, |_, _| {}, None, false)
    }
}

impl<IO: Read + Write + ReadTimeout, C: Clock> ClientBuilder<IO, DeadlineClock<C>> {
    /// Construct the client. Performs no I/O: the HTTP/2 preface is sent with
    /// the first call.
    pub fn build(self) -> Client<IO, C> {
        Client::from_parts(
            self.io,
            self.config,
            self.deadlines.0,
            IO::set_read_timeout,
            self.set_output,
            true,
        )
    }
}

fn retire<IO>(state: &mut State<IO>, status: Status) {
    if state.terminal.is_none() {
        state.inner.fail_all(status.clone());
        state.terminal = Some(status);
    }
}

impl<IO: Read + Write, C: Clock> Client<IO, C> {
    fn from_parts(
        io: IO,
        config: ClientConfig,
        clock: C,
        set_timeout: fn(&mut IO, Option<Duration>),
        set_output_timeout: Option<OutputFn<IO>>,
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
            set_output_timeout,
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
