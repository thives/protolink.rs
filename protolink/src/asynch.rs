//! Async drivers over `embedded_io_async`.
//!
//! A driver is built over a [`Mode`]: [`Serial`] for one transport read and
//! written in turn, [`Duplex`] for independent halves, or
//! [`CoordinatedReliable`] for a reliable link. The same protocol code runs in
//! every mode; only the workers that move bytes differ.

use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Poll, Waker};

use embedded_io_async::{Read, Write};

use crate::Error;
use crate::builder::{DeadlinesUnspecified, client_config_methods, server_config_methods};
use crate::coordinator::{self, Delivery, Halt, Lane, Outbound, RouterNode, ServerNode, earliest};
use crate::driver::{self, Port};
use crate::error::client_failure_status;
use crate::grpc::{
    self, CallId, CallOptions, ClientConfig, Handler, Metadata, Next, Response, Router,
    ServerConfig, Status, StreamingCall, StreamingTransport, UnaryTransport,
};
use crate::link::{ReliableLink, Timer as LinkTimer};
use crate::mode::{CoordinatedReliable, Duplex, Mode, Serial};
use crate::shared::Shared;
use crate::timer::{NoTimer, Timer};

/// Start building an async gRPC server on one connection, served in serial
/// mode (see [`Serial`]).
///
/// Configure the factory with `.with_*` methods and choose a deadline policy,
/// then [`build`](ServerBuilder::build) it and
/// [`serve`](Server::serve) the application's services:
///
/// ```no_run
/// # async fn demo<IO, T, H>(io: IO, timer: T, services: &mut H)
/// # where IO: embedded_io_async::Read + embedded_io_async::Write, T: protolink::Timer, H: protolink::Handler {
/// let server = protolink::server(io)
///     .with_deadline_timer(timer)
///     .with_max_message_size(4096)
///     .build();
/// server.serve(services).await.ok();
/// # }
/// ```
///
/// Building performs no I/O. A deadline policy is required: with
/// [`with_deadline_timer`](ServerBuilder::with_deadline_timer) the server
/// enforces `grpc-timeout`, with
/// [`without_deadlines`](ServerBuilder::without_deadlines) it does not.
/// Without either, there is no `build`:
///
/// ```compile_fail
/// # async fn demo<IO: embedded_io_async::Read + embedded_io_async::Write, H: protolink::Handler>(io: IO, services: &mut H) {
/// let server = protolink::server(io).build();
/// # }
/// ```
///
/// On tokio, [`tokio::server`](crate::tokio::server) supplies the timer.
pub fn server<IO: Read + Write>(io: IO) -> ServerBuilder<Serial<IO>> {
    ServerBuilder {
        mode: Serial::new(io),
        config: ServerConfig::default(),
        timer: DeadlinesUnspecified,
    }
}

/// Start building an async gRPC server whose read and write halves are
/// independent, served in duplex mode (see [`Duplex`]).
///
/// Input is read while output is blocked on the same connection, so a
/// bidirectional exchange does not stall when the transport buffers less than
/// the data in flight.
pub fn server_split<R, W>(reader: R, writer: W) -> ServerBuilder<Duplex<R, W>>
where
    R: Read,
    W: Write<Error = R::Error>,
{
    ServerBuilder {
        mode: Duplex::new(reader, writer),
        config: ServerConfig::default(),
        timer: DeadlinesUnspecified,
    }
}

/// Start building an async gRPC server on a built reliable link, served as one
/// coordinated engine (see [`CoordinatedReliable`]).
pub fn server_reliable<S, L>(link: ReliableLink<S, L>) -> ServerBuilder<CoordinatedReliable<S, L>>
where
    S: Read + Write,
    L: LinkTimer,
{
    ServerBuilder {
        mode: CoordinatedReliable::new(link),
        config: ServerConfig::default(),
        timer: DeadlinesUnspecified,
    }
}

/// Factory of an async [`Server`], created by [`server`], [`server_split`] or
/// [`server_reliable`].
///
/// `M` is the [`Mode`] the transport is served in, and `T` the deadline policy:
/// [`DeadlinesUnspecified`] until one is chosen, then the [`Timer`] (or
/// [`NoTimer`]) the server will use.
#[derive(Debug)]
pub struct ServerBuilder<M, T = DeadlinesUnspecified> {
    mode: M,
    config: ServerConfig,
    timer: T,
}

impl<M, T> ServerBuilder<M, T> {
    /// Enforce each call's `grpc-timeout` with `timer`.
    ///
    /// A call's deadline starts when its request headers arrive. When it is
    /// reached the call ends with `DEADLINE_EXCEEDED` (streaming handlers are
    /// told through [`Handler::on_cancel`]). A unary handler that is running
    /// when its deadline passes can't be preempted; it can consult
    /// [`CallContext::remaining`](crate::CallContext::remaining).
    ///
    /// Waiting for a deadline drops the pending transport `read` when the
    /// timer fires, so the transport's `read` must be cancel-safe, also for
    /// unary-only servers. Writes and flushes are raced too; expiry during
    /// output closes the connection with [`Error::OutputDeadline`] rather than
    /// reusing indeterminate wire state.
    pub fn with_deadline_timer<U: Timer>(self, timer: U) -> ServerBuilder<M, U> {
        ServerBuilder {
            mode: self.mode,
            config: self.config,
            timer,
        }
    }

    /// Do not enforce deadlines locally. Calls still receive their
    /// `grpc-timeout` in [`CallContext::deadline`](crate::CallContext::deadline),
    /// on a clock that never advances.
    pub fn without_deadlines(self) -> ServerBuilder<M, NoTimer> {
        self.with_deadline_timer(NoTimer)
    }

    server_config_methods!();
}

impl<M: Mode, T: Timer> ServerBuilder<M, T> {
    /// Construct the server. Performs no I/O; the HTTP/2 SETTINGS are sent
    /// once [`serve`](Server::serve) runs.
    pub fn build(self) -> Server<M, T> {
        Server {
            mode: self.mode,
            config: self.config,
            timer: self.timer,
        }
    }
}

/// An async gRPC server driver for one connection, built by [`server`],
/// [`server_split`] or [`server_reliable`].
///
/// It owns the transport, the configuration and the deadline timer. The
/// application's services are only joined to it by [`serve`](Self::serve).
#[derive(Debug)]
pub struct Server<M, T = NoTimer> {
    mode: M,
    config: ServerConfig,
    timer: T,
}

impl<M: Mode, T: Timer> Server<M, T> {
    /// Serve gRPC on this connection until the peer disconnects or the
    /// connection fails.
    ///
    /// `services` (a generated `<Service>Dispatch`, a
    /// [`Registry`](crate::Registry), or any [`Handler`]) is borrowed
    /// for the whole connection and receives every call on it; call state
    /// keyed by [`CallId`] is therefore connection-local.
    ///
    /// Streaming handlers are polled with this future's waker. When the
    /// connection needs the transport for output, a pending read is dropped
    /// (serial mode), or continues in the background (duplex and reliable
    /// modes); see [`Mode`].
    ///
    /// When the connection ends, normally or with an error, every active
    /// streaming call is reported to [`Handler::on_cancel`] before this
    /// returns. Dropping the future before it completes drops the transport
    /// and makes the same `on_cancel` calls from the destructor; nothing is
    /// written to the peer in that case.
    pub async fn serve<H: Handler + ?Sized>(self, services: &mut H) -> Result<(), Error<M::Error>> {
        let Self {
            mut mode,
            config,
            timer,
        } = self;
        let mut serving = Serving {
            server: grpc::Server::new(config),
            handler: services,
        };
        let port = Shared::new(Port::new());
        let outcome = {
            let mut node = ServerNode {
                server: &mut serving.server,
                handler: &mut *serving.handler,
            };
            let mut gateway = Lane::new(&port, mode.worker(&port));
            let mut no_lanes: [Lane<'_, core::future::Ready<()>, _>; 0] = [];
            coordinator::drive(&mut node, &mut gateway, &mut no_lanes, &timer).await
        };
        finish_gateway(&mut mode, &port, &mut serving.server, &timer, outcome).await
    }

    /// Serve gRPC on this connection with `router`, which serves its local
    /// services and forwards the rest to `downstream` connections.
    ///
    /// Downstream `i` is `downstream[i]`, in the order the router's routes
    /// were added with [`with_route`](grpc::RouterBuilder::with_route). The
    /// gateway connection and every downstream are driven by this one future.
    /// Each transport has its own operations: a write or flush that stays
    /// pending on one of them does not hold up the others, the local services,
    /// or the deadlines. A downstream is served in serial mode, so its read is
    /// dropped when it has output to write; use
    /// [`serve_router_split`](Self::serve_router_split) for duplex downstreams.
    ///
    /// A downstream whose transport fails (read, write, flush or protocol error),
    /// or whose output stays blocked past the deadline of its calls, is marked
    /// failed: its calls end with `UNAVAILABLE`, and calls routed to it
    /// afterwards do too. The gateway connection keeps serving the others. A
    /// failure of the gateway connection, or output blocked past the deadline
    /// of its calls, ends this future, as for `serve`. Once the router is shut
    /// down, the future completes when the gateway's output has been delivered,
    /// without waiting for the caller to hang up.
    ///
    /// When the future completes or is dropped, the router cancels every call:
    /// its forwarded calls cancel their downstream calls, which are written only
    /// if the connection is still up at that point (a dropped future writes
    /// nothing).
    ///
    /// The router's own [`ServerConfig`] applies to the gateway connection;
    /// the message limits of this server's builder are not used.
    ///
    /// # Panics
    ///
    /// If `downstream` does not have one transport per downstream connection of
    /// `router`: a route without its transport would never be driven.
    pub async fn serve_router<B>(
        self,
        router: &mut Router,
        downstream: &mut [B],
    ) -> Result<(), Error<M::Error>>
    where
        B: Read + Write<Error = M::Error>,
    {
        assert_backends(router, downstream.len());
        let Self {
            mut mode, timer, ..
        } = self;
        let ports = coordinator::ports::<M::Error>(downstream.len());
        let mut lanes: Vec<_> = downstream
            .iter_mut()
            .zip(&ports)
            .map(|(transport, port)| Lane::new(port, driver::run_serial(transport, port)))
            .collect();
        serve_routed(&mut mode, &timer, router, &mut lanes).await
    }

    /// [`serve_router`](Self::serve_router) with duplex downstreams: each one's
    /// read and write halves are polled independently, so a downstream whose
    /// output is blocked keeps delivering its responses.
    ///
    /// # Panics
    ///
    /// As [`serve_router`](Self::serve_router).
    pub async fn serve_router_split<R, W>(
        self,
        router: &mut Router,
        downstream: &mut [Duplex<R, W>],
    ) -> Result<(), Error<M::Error>>
    where
        R: Read,
        W: Write<Error = R::Error>,
        M: Mode<Error = R::Error>,
    {
        assert_backends(router, downstream.len());
        let Self {
            mut mode, timer, ..
        } = self;
        let ports = coordinator::ports::<R::Error>(downstream.len());
        let mut lanes: Vec<_> = downstream
            .iter_mut()
            .zip(&ports)
            .map(|(transport, port)| Lane::new(port, transport.worker(port)))
            .collect();
        serve_routed(&mut mode, &timer, router, &mut lanes).await
    }

    /// [`serve_router`](Self::serve_router) with coordinated reliable
    /// downstreams, each polled as one engine.
    ///
    /// # Panics
    ///
    /// As [`serve_router`](Self::serve_router).
    pub async fn serve_router_reliable<S, L>(
        self,
        router: &mut Router,
        downstream: &mut [CoordinatedReliable<S, L>],
    ) -> Result<(), Error<M::Error>>
    where
        S: Read + Write,
        L: LinkTimer,
        M: Mode<Error = crate::link::ReliableError<S::Error>>,
    {
        assert_backends(router, downstream.len());
        let Self {
            mut mode, timer, ..
        } = self;
        let ports = coordinator::ports::<M::Error>(downstream.len());
        let mut lanes: Vec<_> = downstream
            .iter_mut()
            .zip(&ports)
            .map(|(transport, port)| Lane::new(port, transport.worker(port)))
            .collect();
        serve_routed(&mut mode, &timer, router, &mut lanes).await
    }
}

fn assert_backends(router: &Router, transports: usize) {
    assert_eq!(
        router.backend_count(),
        transports,
        "serve_router needs one transport per downstream of the router"
    );
}

/// Drive a router whose gateway is served in `mode`, with its downstream lanes.
/// The router cancels its calls when the future ends or is dropped.
async fn serve_routed<M: Mode, T: Timer, G: Future<Output = ()>>(
    mode: &mut M,
    timer: &T,
    router: &mut Router,
    lanes: &mut [Lane<'_, G, M::Error>],
) -> Result<(), Error<M::Error>> {
    let routing = RouterGuard(router);
    let port = Shared::new(Port::new());
    let outcome = {
        let mut node = RouterNode {
            router: &mut *routing.0,
        };
        let mut gateway = Lane::new(&port, mode.worker(&port));
        coordinator::drive(&mut node, &mut gateway, lanes, timer).await
    };
    finish_gateway(mode, &port, &mut *routing.0, timer, outcome).await
}

/// The router, cancelled when dropped.
struct RouterGuard<'a>(&'a mut Router);

impl Drop for RouterGuard<'_> {
    fn drop(&mut self) {
        self.0.cancel_all();
    }
}

/// Finish a connection whose coordinator stopped. A protocol error is answered
/// with a GOAWAY, best effort, when the gateway's output allows it.
async fn finish_gateway<M, T, O, E>(
    mode: &mut M,
    port: &Shared<Port<E>>,
    out: &mut O,
    timer: &T,
    outcome: Result<(), Halt<E>>,
) -> Result<(), Error<E>>
where
    M: Mode<Error = E>,
    T: Timer,
    O: Outbound,
{
    match outcome {
        Ok(()) => Ok(()),
        Err(Halt::Error(error)) => Err(error),
        Err(Halt::Protocol { error, goaway }) => {
            // An overload writes nothing more: the gateway's output is what is full.
            let overload = matches!(error, grpc::http2::Error::OutputLimitExceeded { .. });
            if goaway && !overload {
                // The coordinator's worker is gone, so nothing else is writing.
                let mut lane = Lane::new(port, mode.worker(port));
                coordinator::deliver(&mut lane, out, timer).await;
            }
            Err(Error::from_connection(error))
        }
    }
}

/// The server and its handler for one connection. Dropping it cancels every call.
struct Serving<'a, H: Handler + ?Sized> {
    server: grpc::Server,
    handler: &'a mut H,
}

impl<H: Handler + ?Sized> Drop for Serving<'_, H> {
    fn drop(&mut self) {
        self.server.cancel_all(self.handler);
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
/// [`Server::serve`] with streaming handlers). It only matters when more than one call
/// is active; a client with one call never drops a read. For transports
/// without a cancel-safe `read` (for example a one-shot DMA UART), put them
/// behind the `pump` module of `protolink-adaptors` or keep to one call at a time.
///
/// Without a deadline a blocked transport `write` or `flush` stops progress.
/// With a timer, expiry during output retires the whole connection because
/// dropping an output operation can leave indeterminate bytes on the wire.
///
/// # Deadlines
///
/// Calls can have a timeout ([`CallOptions`], [`ClientConfig::default_timeout`]),
/// which is sent to the server as `grpc-timeout`. A client built with
/// [`with_deadline_timer`](ClientBuilder::with_deadline_timer) also enforces
/// it: when the call's time is up it fails with `DEADLINE_EXCEEDED` and the
/// stream is reset. Messages that were already received are still delivered
/// first. A client built
/// [`without_deadlines`](ClientBuilder::without_deadlines) has no clock and
/// leaves enforcement to the server.
///
/// Deadlines are only noticed while one of the client's operations is being
/// polled. Waiting for a deadline drops the pending read when the timer fires,
/// so with a timer, `read` must be cancel-safe even when only one call is
/// active. The reset of a call that expired is written by the next operation
/// on the client, except that an output deadline retires the connection and
/// no further transport I/O is attempted. Transport/protocol errors likewise
/// permanently retire the connection; buffered messages remain retrievable.
#[derive(Debug)]
pub struct Client<M, T = NoTimer> {
    shared: Shared<State<M>>,
    timer: T,
}

#[derive(Debug)]
struct State<M> {
    inner: grpc::Client,
    /// The transport, or `None` while an operation has it checked out.
    io: Option<M>,
    /// Tasks waiting for the transport to be checked back in.
    waiters: Vec<(usize, Waker)>,
    next_waiter: usize,
    /// Once retired, queued bytes must never be sent again.
    terminal: Option<Status>,
    /// A unary call whose future was dropped before it completed.
    stale_unary: Option<CallId>,
    /// Some operation waits for the transport. The owner gives it up at its
    /// next point where nothing is in flight.
    handoff: bool,
    /// Waker of the operation that owns the transport, so that it is woken when
    /// the state it waits for changes.
    owner: Option<Waker>,
    /// The delivery obligation of the transport, kept across leases so that
    /// neither the flush nor the deadline of unacknowledged output is forgotten
    /// by a handoff.
    delivery: Delivery,
    /// The owner must act: output was queued, credit was released, or a call
    /// came due and so queued output. Taken and woken by [`State::take_wakers`].
    dirty_owner: bool,
    /// Waiters must check their completion again: a call ended, input was
    /// applied, or the client was retired.
    dirty_waiters: bool,
}

/// The transport, checked out of a [`Client`]. Dropping the guard checks it
/// back in and wakes the tasks waiting for it.
struct IoGuard<'a, M, T> {
    client: &'a Client<M, T>,
    io: Option<M>,
    /// An output operation is in flight, so its outcome is unknown if the owner
    /// is dropped now.
    in_flight: bool,
}

impl<M, T> Drop for IoGuard<'_, M, T> {
    fn drop(&mut self) {
        let io = self.io.take();
        let wakers = self.client.shared.with(|s| {
            if self.in_flight {
                retire(s, Status::unavailable("transport output cancelled"));
            }
            s.io = io;
            s.owner = None;
            s.dirty_waiters = true;
            s.take_wakers()
        });
        wake_all(wakers);
    }
}

/// Owns one waiter registration even when an acquisition is cancelled.
struct Registration<'a, M, T> {
    client: &'a Client<M, T>,
    id: Option<usize>,
}

impl<M, T> Drop for Registration<'_, M, T> {
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

fn retire<M>(state: &mut State<M>, status: Status) {
    if state.terminal.is_none() {
        state.inner.fail_all(status.clone());
        state.terminal = Some(status);
        state.dirty_owner = true;
        state.dirty_waiters = true;
    }
}

/// Retire the client if its connection failed on its own, as a call's work
/// can latch an output limit (for example a credit release that does not fit).
fn adopt_connection_failure<M>(state: &mut State<M>) {
    if let Some(status) = state.inner.failure().map(client_failure_status) {
        retire(state, status);
    }
}

impl<M> State<M> {
    /// Output was queued, or credit released: the owner has to write it.
    fn queued(&mut self) {
        self.dirty_owner = true;
    }

    /// Tick the core at `now`. Only a call that came due changes anything: it
    /// queues output for the owner and ends its waiting task, so an unchanged
    /// tick notifies nobody.
    fn tick(&mut self, now: core::time::Duration) {
        let due = self.inner.next_deadline().is_some_and(|d| d <= now);
        self.inner.tick(now);
        if due {
            self.dirty_owner = true;
            self.dirty_waiters = true;
        }
    }

    /// The wakers that the changes since the last call make due. They are woken
    /// after the state's lock is released, by [`wake_all`].
    fn take_wakers(&mut self) -> Vec<Waker> {
        let mut wakers = Vec::new();
        if core::mem::take(&mut self.dirty_owner) {
            wakers.extend(self.owner.take());
        }
        if core::mem::take(&mut self.dirty_waiters) {
            wakers.extend(self.waiters.drain(..).map(|(_, waiter)| waiter));
        }
        wakers
    }
}

/// Wake tasks collected under the state's lock, now that it is released.
fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

/// Start building an async gRPC client on one connection.
///
/// ```no_run
/// # fn demo<IO, T>(io: IO, timer: T)
/// # where IO: embedded_io_async::Read + embedded_io_async::Write, T: protolink::Timer {
/// use core::time::Duration;
///
/// let transport = protolink::client(io)
///     .with_deadline_timer(timer)
///     .with_default_timeout(Duration::from_secs(5))
///     .build();
/// // Hand `transport` to a generated `<Service>Client::new`.
/// # let _ = transport;
/// # }
/// ```
///
/// Building performs no I/O: the HTTP/2 preface is sent with the first call.
/// As for [`server`], a deadline policy must be chosen before `build`; on
/// tokio, [`tokio::client`](crate::tokio::client) supplies the timer.
///
/// ```compile_fail
/// # fn demo<IO: embedded_io_async::Read + embedded_io_async::Write>(io: IO) {
/// let transport = protolink::client(io).build();
/// # }
/// ```
pub fn client<IO: Read + Write>(io: IO) -> ClientBuilder<Serial<IO>> {
    ClientBuilder {
        mode: Serial::new(io),
        config: ClientConfig::default(),
        timer: DeadlinesUnspecified,
    }
}

/// Start building an async gRPC client whose read and write halves are
/// independent, served in duplex mode (see [`Duplex`]).
pub fn client_split<R, W>(reader: R, writer: W) -> ClientBuilder<Duplex<R, W>>
where
    R: Read,
    W: Write<Error = R::Error>,
{
    ClientBuilder {
        mode: Duplex::new(reader, writer),
        config: ClientConfig::default(),
        timer: DeadlinesUnspecified,
    }
}

/// Start building an async gRPC client on a built reliable link, served as one
/// coordinated engine (see [`CoordinatedReliable`]).
pub fn client_reliable<S, L>(link: ReliableLink<S, L>) -> ClientBuilder<CoordinatedReliable<S, L>>
where
    S: Read + Write,
    L: LinkTimer,
{
    ClientBuilder {
        mode: CoordinatedReliable::new(link),
        config: ClientConfig::default(),
        timer: DeadlinesUnspecified,
    }
}

/// Factory of an async [`Client`], created by [`client`], [`client_split`] or
/// [`client_reliable`].
///
/// `T` is the deadline policy: [`DeadlinesUnspecified`] until one is chosen,
/// then the [`Timer`] (or [`NoTimer`]) the client will use.
#[derive(Debug)]
pub struct ClientBuilder<M, T = DeadlinesUnspecified> {
    mode: M,
    config: ClientConfig,
    timer: T,
}

impl<M, T> ClientBuilder<M, T> {
    /// Enforce call timeouts locally with `timer`, see
    /// [Deadlines](Client#deadlines). The transport's `read` must then be
    /// cancel-safe even with a single call.
    pub fn with_deadline_timer<U: Timer>(self, timer: U) -> ClientBuilder<M, U> {
        ClientBuilder {
            mode: self.mode,
            config: self.config,
            timer,
        }
    }

    /// Do not enforce timeouts locally. They are still sent as
    /// `grpc-timeout`, so a server with a timer enforces them.
    pub fn without_deadlines(self) -> ClientBuilder<M, NoTimer> {
        self.with_deadline_timer(NoTimer)
    }

    client_config_methods!();
}

impl<M: Mode, T: Timer> ClientBuilder<M, T> {
    /// Construct the client. Performs no I/O: the HTTP/2 preface is sent with
    /// the first call.
    pub fn build(self) -> Client<M, T> {
        Client::from_parts(self.mode, self.config, self.timer)
    }
}

impl<IO, T> Client<Serial<IO>, T> {
    /// Unwrap the transport of a serial client.
    pub fn into_io(self) -> IO {
        self.shared
            .into_inner()
            .io
            .expect("the transport is checked in when no operation is running")
            .into_inner()
    }
}

impl<R: Read, W: Write<Error = R::Error>, T> Client<Duplex<R, W>, T> {
    /// Unwrap the two halves of a duplex client. They are not reunited.
    pub fn into_parts(self) -> (R, W) {
        self.shared
            .into_inner()
            .io
            .expect("the transport is checked in when no operation is running")
            .into_parts()
    }
}

impl<S: Read + Write, L: LinkTimer, T> Client<CoordinatedReliable<S, L>, T> {
    /// Unwrap the reliable link of a coordinated client.
    pub fn into_link(self) -> ReliableLink<S, L> {
        self.shared
            .into_inner()
            .io
            .expect("the transport is checked in when no operation is running")
            .into_link()
    }
}

impl<M: Mode, T: Timer> Client<M, T> {
    fn from_parts(mode: M, config: ClientConfig, timer: T) -> Self {
        Self {
            shared: Shared::new(State {
                inner: grpc::Client::new(config),
                io: Some(mode),
                waiters: Vec::new(),
                next_waiter: 0,
                terminal: None,
                stale_unary: None,
                handoff: false,
                owner: None,
                delivery: Delivery::default(),
                dirty_owner: false,
                dirty_waiters: false,
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
        let (started, wakers) = self.shared.with(|s| {
            s.tick(now);
            cancel_stale_unary(s);
            let started = match &s.terminal {
                Some(status) => Err(status.clone()),
                None => s.inner.start_unary_with(path, request, &options),
            };
            adopt_connection_failure(s);
            if started.is_ok() {
                s.queued();
            }
            (started, s.take_wakers())
        });
        wake_all(wakers);
        let id = started?;
        self.shared.with(|s| s.stale_unary = Some(id));
        let result = self
            .drive(|c, _| match c.take_response(id) {
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
    pub fn streaming(&self, path: &str) -> Result<Call<'_, M, T>, Status> {
        self.streaming_with(path, CallOptions::default())
    }

    /// [`streaming`](Self::streaming) with per-call `options`. The timeout
    /// covers the whole call, not each message.
    pub fn streaming_with(
        &self,
        path: &str,
        options: CallOptions,
    ) -> Result<Call<'_, M, T>, Status> {
        let now = self.timer.now();
        let (started, wakers) = self.shared.with(|s| {
            s.tick(now);
            cancel_stale_unary(s);
            let started = match &s.terminal {
                Some(status) => Err(status.clone()),
                None => s.inner.start_streaming_with(path, &options),
            };
            adopt_connection_failure(s);
            if started.is_ok() {
                s.queued();
            }
            (started, s.take_wakers())
        });
        wake_all(wakers);
        let id = started?;
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

    /// Check the transport out, waiting while another operation has it.
    /// `done` is evaluated first, and whenever the client state may have
    /// changed; if it returns a value, that is returned instead of the
    /// transport.
    async fn acquire<R>(
        &self,
        done: &mut impl FnMut(&mut grpc::Client, bool) -> Option<R>,
    ) -> Result<IoGuard<'_, M, T>, R> {
        let mut registration = Registration {
            client: self,
            id: None,
        };
        let acquired = poll_fn(|cx| {
            let (poll, wakers) = self.shared.with(|s| {
                s.tick(self.timer.now());
                adopt_connection_failure(s);
                if let Some(value) = done(&mut s.inner, s.delivery.unflushed) {
                    // Consuming a result may release credit, which the owner writes,
                    // and whose output may not fit. The result itself is kept.
                    s.queued();
                    adopt_connection_failure(s);
                    return (Poll::Ready(Err(value)), s.take_wakers());
                }
                match s.io.take() {
                    Some(io) => {
                        s.handoff = false;
                        (Poll::Ready(Ok(io)), Vec::new())
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
                        drop(discarded);
                        // Only output needs the transport from another task. Input
                        // is read by the owner, which feeds the state this waits on.
                        // The owner gives the transport up at its next safe point.
                        if s.inner.has_output() {
                            s.handoff = true;
                            s.dirty_owner = true;
                            (Poll::Pending, s.take_wakers())
                        } else {
                            (Poll::Pending, Vec::new())
                        }
                    }
                }
            });
            wake_all(wakers);
            poll
        })
        .await;
        acquired.map(|io| IoGuard {
            client: self,
            io: Some(io),
            in_flight: false,
        })
    }

    /// Run the connection, as its owner, until `step` returns a value. Each
    /// operation holds the transport only as long as it needs it; see [`pump`].
    async fn drive<R>(&self, mut step: impl FnMut(&mut grpc::Client, bool) -> Option<R>) -> R {
        loop {
            let mut guard = match self.acquire(&mut step).await {
                Ok(guard) => guard,
                Err(value) => return value,
            };
            match self.pump(&mut guard, &mut step).await {
                Pumped::Value(value) => return value,
                Pumped::Retired => {}
                Pumped::Handoff => {
                    drop(guard);
                    yield_now().await;
                }
            }
        }
    }

    /// Write all pending output and flush the transport, as the owner of the
    /// transport. Transport failures fail every call.
    async fn flush(&self) {
        loop {
            if self.shared.with(|s| s.terminal.is_some()) {
                return;
            }
            let mut done = |c: &mut grpc::Client, unflushed: bool| {
                (!c.has_output() && !unflushed).then_some(())
            };
            let mut guard = match self.acquire(&mut done).await {
                Ok(guard) => guard,
                Err(()) => return,
            };
            match self.pump(&mut guard, &mut done).await {
                Pumped::Value(()) | Pumped::Retired => return,
                Pumped::Handoff => {
                    drop(guard);
                    yield_now().await;
                }
            }
        }
    }

    /// Move the connection along while this task owns the transport: write what
    /// is pending, flush what was written, and read input into the core, until
    /// `step` returns a value. `step` is only evaluated when no operation is in
    /// flight, so a result is consumed only at a safe point. Input is read while
    /// an output operation is pending on the same transport, see [`Mode`].
    ///
    /// The transport is only given up when nothing is in flight. Other
    /// operations that wait for it request that, and the owner yields at the
    /// first such point.
    async fn pump<R>(
        &self,
        guard: &mut IoGuard<'_, M, T>,
        step: &mut impl FnMut(&mut grpc::Client, bool) -> Option<R>,
    ) -> Pumped<R> {
        let port = Shared::new(driver::Port::new().write_first());
        let io = guard
            .io
            .as_mut()
            .expect("the transport is held until the guard is dropped");
        let mut lane = Lane::new(&port, io.worker(&port));
        lane.set_delivery(self.shared.with(|s| s.delivery));
        loop {
            let now = self.timer.now();
            let settled = self.shared.with(|s| {
                // The obligations of output in flight are latched before the calls
                // that are due are expired, so that the deadline they set is kept.
                lane.arm(&s.inner);
                s.tick(now);
                adopt_connection_failure(s);
                if s.terminal.is_some() {
                    lane.retire();
                    return Settled::Retired;
                }
                // Output past its deadline retires the client before a completion
                // is taken: taking it would otherwise erase the obligation.
                if lane.overdue(now) {
                    // Expired calls end first, so that the retirement keeps their status.
                    retire(s, Status::unavailable("transport output deadline exceeded"));
                    lane.retire();
                    return Settled::Retired;
                }
                // The reply to an operation ends it. A failed one retires the client.
                let flushing = lane.flushing();
                if lane.reap(&mut s.inner).is_some() {
                    let message = if flushing {
                        "transport flush failed"
                    } else {
                        "transport write failed"
                    };
                    retire(s, Status::unavailable(message));
                    lane.retire();
                    return Settled::Retired;
                }
                // Input: one read outcome, fed to the core without handler work.
                if let Some(read) = lane.take_input() {
                    match read {
                        Ok(0) => retire(s, Status::unavailable("connection closed")),
                        Ok(n) => match lane.with_input(n, |bytes| s.inner.recv(bytes)) {
                            Ok(()) => {
                                s.dirty_waiters = true;
                            }
                            Err(error) => retire(s, client_failure_status(&error)),
                        },
                        Err(_) => retire(s, Status::unavailable("transport read failed")),
                    }
                    if s.terminal.is_some() {
                        lane.retire();
                        return Settled::Retired;
                    }
                }
                // Output: the write or flush that is due, once nothing is in flight.
                lane.issue(&s.inner);
                if !lane.busy() {
                    if let Some(value) = step(&mut s.inner, lane.unflushed()) {
                        // The result was consumed, and its credit may not fit.
                        adopt_connection_failure(s);
                        return Settled::Value(value);
                    }
                    if s.handoff {
                        return Settled::Handoff;
                    }
                }
                Settled::Pending
            });
            // An operation in flight is what the guard must not lose: see IoGuard.
            guard.in_flight = lane.busy();
            // Input changed the calls, or the client ended: the tasks waiting for the
            // transport may now be able to finish. The owner, which is this task, needs
            // no wake of its own.
            let wakers = self.shared.with(|s| {
                s.dirty_owner = false;
                s.take_wakers()
            });
            wake_all(wakers);
            match settled {
                Settled::Value(value) => return leave(self, &lane, Pumped::Value(value)),
                Settled::Retired => return leave(self, &lane, Pumped::Retired),
                Settled::Handoff => return leave(self, &lane, Pumped::Handoff),
                Settled::Pending => {}
            }
            wait_for_pump(self, &mut lane).await;
        }
    }
}

/// What a pump of the transport ended with.
enum Pumped<R> {
    /// `step` returned its value, at a safe point.
    Value(R),
    /// The client was retired; its calls have their statuses.
    Retired,
    /// Another operation waits for the transport, and nothing is in flight.
    Handoff,
}

/// The outcome of one settling pass of a pump, taken under the state's lock.
enum Settled<R> {
    Value(R),
    Retired,
    Handoff,
    Pending,
}

/// Record the flush obligation of a lane that is leaving the transport, so that
/// the next owner inherits it.
fn leave<M: Mode, T: Timer, F: Future<Output = ()>, R>(
    client: &Client<M, T>,
    lane: &Lane<'_, F, M::Error>,
    pumped: Pumped<R>,
) -> Pumped<R> {
    client.shared.with(|s| s.delivery = lane.delivery());
    pumped
}

/// Wait until the pump has something to do: its worker has an outcome, a
/// deadline is due, or the state changed in a way the owner acts on. The owner's
/// waker is registered under the state's lock, together with the readiness
/// check, so that a change cannot slip in unnoticed.
async fn wait_for_pump<M: Mode, T: Timer, F: Future<Output = ()>>(
    client: &Client<M, T>,
    lane: &mut Lane<'_, F, M::Error>,
) {
    let mut deadline = client
        .shared
        .with(|s| earliest([s.inner.next_deadline(), lane.deadline()]));
    let mut sleep = pin!(deadline.map(|d| client.timer.sleep_until(d)));
    poll_fn(|cx| {
        // Register and inspect the state first, under one lock: a change made after
        // this point wakes the owner, and one made before it is seen here. A
        // terminal client needs no transport I/O, even with a write still pending.
        let (previous, terminal, next) = client.shared.with(|s| {
            let previous = s.owner.replace(cx.waker().clone());
            let terminal = s.terminal.is_some();
            (
                previous,
                terminal,
                earliest([s.inner.next_deadline(), lane.deadline()]),
            )
        });
        drop(previous);
        // A call that started meanwhile may have brought an earlier deadline.
        if next != deadline {
            deadline = next;
            sleep.set(deadline.map(|d| client.timer.sleep_until(d)));
        }
        // A due deadline returns to the pump before the worker is polled, so that
        // no transport operation starts once output has expired.
        if terminal
            || deadline.is_some_and(|d| client.timer.now() >= d)
            || sleep
                .as_mut()
                .as_pin_mut()
                .is_some_and(|sleep| sleep.poll(cx).is_ready())
        {
            return Poll::Ready(());
        }
        lane.poll_worker(cx);
        let ready = client.shared.with(|s| {
            lane.has_event() || (!lane.busy() && (lane.wants_issue(&s.inner) || s.handoff))
        });
        if ready
            || deadline.is_some_and(|d| client.timer.now() >= d)
            || sleep
                .as_mut()
                .as_pin_mut()
                .is_some_and(|sleep| sleep.poll(cx).is_ready())
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Give other tasks a turn: returns `Pending` once, waking itself, then `Ready`.
/// It does not depend on which executor polls the future.
async fn yield_now() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// Cancel a unary call whose future was dropped.
fn cancel_stale_unary<M>(state: &mut State<M>) {
    if let Some(stale) = state.stale_unary.take() {
        state.inner.cancel(stale);
    }
}

impl<M: Mode, T: Timer> UnaryTransport for Client<M, T> {
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> impl Future<Output = Result<Response<Vec<u8>>, Status>> {
        Client::unary_with(self, path, request, options)
    }
}

impl<M: Mode, T: Timer> StreamingTransport for Client<M, T> {
    type Call<'a>
        = Call<'a, M, T>
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
pub struct Call<'a, M, T = NoTimer> {
    client: &'a Client<M, T>,
    id: CallId,
    finished: Option<Result<(), Status>>,
    /// Response headers and trailers, taken from the client when the call
    /// finished.
    metadata: (Option<Metadata>, Metadata),
}

impl<M: Mode, T: Timer> Call<'_, M, T> {
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
            .drive(|c, _| (c.can_send(id) || !c.is_pending(id)).then_some(()))
            .await;
        let (queued, wakers) = client.shared.with(|s| {
            let queued = if s.inner.is_pending(id) {
                let queued = s.inner.send_message(id, message);
                adopt_connection_failure(s);
                queued.map(|()| true)
            } else {
                // Finished while waiting; the outcome is reported by `message`.
                Ok(false)
            };
            if let Ok(true) = queued {
                s.queued();
            }
            (queued, s.take_wakers())
        });
        wake_all(wakers);
        let queued = queued?;
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
        let (queued, wakers) = client.shared.with(|s| {
            let queued = if s.inner.is_pending(id) {
                let queued = s.inner.close_send(id);
                adopt_connection_failure(s);
                queued.map(|()| true)
            } else {
                Ok(false)
            };
            if let Ok(true) = queued {
                s.queued();
            }
            (queued, s.take_wakers())
        });
        wake_all(wakers);
        let queued = queued?;
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
            .drive(|c, _| match c.try_next(id) {
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

fn client_take_metadata<M, T>(client: &Client<M, T>, id: CallId) -> (Option<Metadata>, Metadata) {
    client.shared.with(|s| s.inner.take_metadata(id))
}

impl<M, T> Drop for Call<'_, M, T> {
    fn drop(&mut self) {
        if self.finished.is_some() {
            return;
        }
        let id = self.id;
        let wakers = self.client.shared.with(|s| {
            s.inner.cancel(id);
            adopt_connection_failure(s);
            // The cancellation is written by the owner of the transport, which
            // is woken to do so.
            s.queued();
            s.take_wakers()
        });
        wake_all(wakers);
    }
}

impl<M: Mode, T: Timer> StreamingCall for Call<'_, M, T> {
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
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
    use core::task::Context;

    use embedded_io_async::ErrorType;

    use super::*;
    use crate::grpc::{
        CallContext, Code, Dispatcher, FnHandler, MethodDescriptor, MethodKind, Route,
    };

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
        fn route(&self, path: &str) -> Option<Route> {
            matches!(path, GATE | OPEN | QUIET).then_some(Route::new(MethodKind::BidiStreaming, 0))
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

    fn loopback_client(peer: &Rc<RefCell<Peer>>) -> Client<Serial<Loopback>> {
        client(Loopback(peer.clone())).without_deadlines().build()
    }

    /// Call A waits for a reply that only arrives after call B sent a
    /// request. A holds the transport in a pending read; B must get the
    /// transport to write, so A has to yield.
    #[test]
    fn concurrent_reader_yields_to_a_writer_on_another_call() {
        let peer = Peer::new();
        let client = loopback_client(&peer);
        let mut a = client.streaming(GATE).unwrap();
        let mut b = client.streaming(OPEN).unwrap();
        let (wake_a, a_wakes) = Wakes::waker();
        let (wake_b, b_wakes) = Wakes::waker();

        {
            let mut a_message = pin!(a.message());
            assert!(poll_once(a_message.as_mut(), &wake_a).is_pending());
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
        let client = loopback_client(&peer);
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
    }

    /// Dropping a call while another call is reading gets the cancellation
    /// written promptly.
    #[test]
    fn concurrent_dropping_a_call_makes_the_reader_flush_the_cancel() {
        let peer = Peer::new();
        let client = loopback_client(&peer);
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
        // The cancel took the reader's read away; leases may drop reads too.
        assert!(peer.borrow().reads_dropped >= 1);
    }

    /// A call that gave up waiting for the transport doesn't disturb the
    /// others.
    #[test]
    fn concurrent_abandoned_waiter_does_not_block_the_others() {
        let peer = Peer::new();
        let client = loopback_client(&peer);
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
        let client = loopback_client(&peer);
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
                // A stalled transport has no input to give, rather than an end.
                Fault::WritePending | Fault::FlushPending | Fault::FlushWaking => {
                    core::future::pending().await
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
            let mut client = client(io).without_deadlines().build();
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
            let client = client(io)
                .with_deadline_timer(ManualTimer(now.clone()))
                .build();
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
                let client = client(io)
                    .with_deadline_timer(ManualTimer(now.clone()))
                    .build();
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
            let client = client(io)
                .with_deadline_timer(ManualTimer(now.clone()))
                .build();
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
            let mut client = client(io).without_deadlines().build();
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
                let mut server = pin!(
                    server(server_io(stall, None))
                        .without_deadlines()
                        .build()
                        .serve(&mut handler)
                );
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
                let mut server = pin!(
                    server(server_io(stall, Some(Duration::from_millis(100))))
                        .with_deadline_timer(ManualTimer(now.clone()))
                        .build()
                        .serve(&mut handler)
                );
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
        fn route(&self, _: &str) -> Option<Route> {
            let kind = if self.unary {
                MethodKind::Unary
            } else {
                MethodKind::ServerStreaming
            };
            Some(Route::new(kind, 0))
        }
        fn call(&mut self, _: &mut CallContext<'_>, req: &[u8]) -> Result<Vec<u8>, Status> {
            assert!(self.unary);
            self.completed.set(self.completed.get() + 1);
            Ok(req.to_vec())
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
                    let mut server = pin!(
                        server(io)
                            .with_deadline_timer(ManualTimer(now.clone()))
                            .build()
                            .serve(&mut handler)
                    );
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
        let mut client = client(io).with_deadline_timer(timer).build();
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
        let client = loopback_client(&peer);
        let mut call = client.streaming(GATE).unwrap();
        run(call.send(b"x")).unwrap();
        let bytes: Vec<u8> = peer.borrow_mut().rx.drain(..).collect();
        client.shared.with(|s| s.inner.recv(&bytes).unwrap());
        client
            .shared
            .with(|s| retire(s, Status::unavailable("scripted transport failure")));
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
        let client = client(io)
            .with_deadline_timer(ManualTimer(now.clone()))
            .build();
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
        fn route(&self, _: &str) -> Option<Route> {
            Some(Route::new(MethodKind::BidiStreaming, 0))
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
            let mut server = pin!(
                server(server_io(Fault::Read, Some(Duration::from_millis(100))))
                    .with_deadline_timer(timer.clone())
                    .build()
                    .serve(&mut handler)
            );
            assert!(poll_once(server.as_mut(), Waker::noop()).is_pending());
            timer.0.set(Duration::from_millis(101));
            assert!(poll_once(server.as_mut(), Waker::noop()).is_pending());
            assert_eq!(cancellations.get(), 1);
            assert_eq!(late_polls.get(), 0);
        }
        assert_eq!(cancellations.get(), 1);
    }

    // ---------------------------------------------------------------------
    // The router's coordinator: deadlines before progress, and output safety
    // ---------------------------------------------------------------------

    const GATE_PATH: &str = "/t.R/Gate";
    const QUICK_PATH: &str = "/t.R/Quick";
    const ROUTED: &[MethodDescriptor] = &[
        MethodDescriptor::new(GATE_PATH, MethodKind::BidiStreaming, 0),
        MethodDescriptor::new(QUICK_PATH, MethodKind::BidiStreaming, 1),
    ];

    /// Not a connection preface, so the gateway refuses it as a protocol error.
    const GARBAGE: &[u8] = b"GET / HTTP/1.1\r\nHost: gateway.test\r\n\r\n";

    /// The gateway's local service. `Quick` answers at once. `Gate` answers once
    /// `open` is set, so the test decides exactly when its handler is ready.
    #[derive(Clone, Default)]
    struct Local {
        open: Arc<AtomicBool>,
        /// Polls of `Gate` that found it open.
        open_polls: Arc<AtomicUsize>,
        cancelled: Arc<AtomicUsize>,
        answered: BTreeSet<CallId>,
    }

    impl Handler for Local {
        fn route(&self, path: &str) -> Option<Route> {
            Route::find(ROUTED, path)
        }

        fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
            Ok(())
        }

        fn poll_response(
            &mut self,
            ctx: &mut CallContext<'_>,
            _: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            if ctx.path == GATE_PATH {
                if !self.open.load(SeqCst) {
                    return Poll::Pending;
                }
                self.open_polls.fetch_add(1, SeqCst);
            }
            if self.answered.insert(ctx.id) {
                Poll::Ready(Next::Message(b"answer".to_vec()))
            } else {
                Poll::Ready(Next::Done(Ok(())))
            }
        }

        fn on_cancel(&mut self, _: &mut CallContext<'_>) {
            self.cancelled.fetch_add(1, SeqCst);
        }
    }

    impl Dispatcher for Local {
        fn methods(&self) -> &[MethodDescriptor] {
            ROUTED
        }
    }

    /// The scripted gateway transport, and the state the test sees through it.
    #[derive(Default)]
    struct Wire {
        /// The caller's bytes, read in order.
        inbound: VecDeque<u8>,
        /// Reads return nothing until this many polls of reads have been made.
        read_ready_after: usize,
        read_polls: usize,
        /// Bytes accepted by completed writes, in order.
        written: Vec<u8>,
        /// Largest number of bytes one write accepts; `0` for no limit.
        max_write: usize,
        /// While set, a write accepts its first `emit` bytes and then stays
        /// pending. Such a write is never released in these tests.
        stall_writes: bool,
        emit: usize,
        /// While set, a flush stays pending.
        stall_flushes: bool,
        /// Writes and flushes started, and polls of writes.
        writes: usize,
        flushes: usize,
        write_polls: usize,
        /// Writes and flushes dropped before they completed.
        cancelled: usize,
    }

    /// The gateway's transport. The test keeps a clone to script it.
    #[derive(Clone, Default)]
    struct GatewayIo(Rc<RefCell<Wire>>);

    impl GatewayIo {
        fn with_inbound(bytes: &[u8]) -> Self {
            let io = Self::default();
            io.0.borrow_mut().inbound.extend(bytes);
            io
        }
    }

    /// Counts an operation as cancelled when it is dropped before it completes.
    struct Unfinished(Option<Rc<RefCell<Wire>>>);

    impl Unfinished {
        fn finish(mut self) {
            self.0 = None;
        }
    }

    impl Drop for Unfinished {
        fn drop(&mut self) {
            if let Some(wire) = self.0.take() {
                wire.borrow_mut().cancelled += 1;
            }
        }
    }

    impl ErrorType for GatewayIo {
        type Error = Infallible;
    }

    impl Read for GatewayIo {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            let wire = self.0.clone();
            poll_fn(|_| {
                let mut w = wire.borrow_mut();
                w.read_polls += 1;
                if w.read_polls <= w.read_ready_after || w.inbound.is_empty() {
                    return Poll::Pending;
                }
                let n = buf.len().min(w.inbound.len());
                let bytes: Vec<u8> = w.inbound.drain(..n).collect();
                buf[..n].copy_from_slice(&bytes);
                Poll::Ready(Ok(n))
            })
            .await
        }
    }

    impl Write for GatewayIo {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            let wire = self.0.clone();
            wire.borrow_mut().writes += 1;
            let op = Unfinished(Some(wire.clone()));
            let mut emitted = false;
            let n = poll_fn(|_| {
                let mut w = wire.borrow_mut();
                w.write_polls += 1;
                if w.stall_writes {
                    if !emitted {
                        emitted = true;
                        let n = w.emit.min(buf.len());
                        w.written.extend_from_slice(&buf[..n]);
                    }
                    return Poll::Pending;
                }
                let n = match w.max_write {
                    0 => buf.len(),
                    limit => buf.len().min(limit),
                };
                w.written.extend_from_slice(&buf[..n]);
                Poll::Ready(n)
            })
            .await;
            op.finish();
            Ok(n)
        }

        async fn flush(&mut self) -> Result<(), Infallible> {
            let wire = self.0.clone();
            wire.borrow_mut().flushes += 1;
            let op = Unfinished(Some(wire.clone()));
            poll_fn(|_| {
                if wire.borrow().stall_flushes {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            op.finish();
            Ok(())
        }
    }

    /// Serve `router` over `io` on the manual clock, with no downstream.
    fn serve_router_alone<'a>(
        io: GatewayIo,
        clock: &ManualTimer,
        router: &'a mut Router,
    ) -> impl Future<Output = Result<(), Error<Infallible>>> + 'a {
        let none: &'a mut [GatewayIo] = &mut [];
        server(io)
            .with_deadline_timer(clock.clone())
            .build()
            .serve_router(router, none)
    }

    /// Start a call on `caller`, with one request message and `timeout`.
    fn start_call(
        caller: &mut grpc::Client,
        path: &str,
        timeout: Option<core::time::Duration>,
    ) -> CallId {
        let options = timeout.map_or_else(CallOptions::default, CallOptions::timeout);
        let id = caller.start_streaming_with(path, &options).unwrap();
        caller.send_message(id, b"request").unwrap();
        id
    }

    /// Feed the gateway's output so far to `caller`, and return what call `id`
    /// has received.
    fn received(caller: &mut grpc::Client, id: CallId, io: &GatewayIo) -> Vec<Next<Vec<u8>>> {
        let bytes = core::mem::take(&mut io.0.borrow_mut().written);
        caller.recv(&bytes).unwrap();
        core::iter::from_fn(|| caller.try_next(id)).collect()
    }

    /// The gateway's output before any input, and the output once `GARBAGE` is
    /// refused: the GOAWAY is what follows the first.
    fn output_around_protocol_error() -> (Vec<u8>, Vec<u8>) {
        let mut reference = Router::builder()
            .with_service(Local::default())
            .build()
            .unwrap();
        let before = reference.take_output();
        assert!(reference.recv(GARBAGE).is_err());
        (before, reference.take_output())
    }

    #[test]
    fn gateway_expires_a_call_before_polling_a_handler_made_ready_at_its_deadline() {
        use core::time::Duration;
        for millis in [100, 101] {
            let clock = ManualTimer(Rc::new(core::cell::Cell::new(Duration::ZERO)));
            let service = Local::default();
            let mut router = Router::builder()
                .with_service(service.clone())
                .build()
                .unwrap();
            let mut caller = grpc::Client::new(ClientConfig::default());
            let id = start_call(&mut caller, GATE_PATH, Some(Duration::from_millis(100)));
            let io = GatewayIo::with_inbound(&caller.take_output());
            let mut serving = pin!(serve_router_alone(io.clone(), &clock, &mut router));
            assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());

            // The handler becomes ready in the same step as the deadline.
            service.open.store(true, SeqCst);
            clock.0.set(Duration::from_millis(millis));
            assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());
            assert_eq!(
                service.open_polls.load(SeqCst),
                0,
                "an expired handler was polled at {millis}ms"
            );
            assert_eq!(service.cancelled.load(SeqCst), 1);
            let events = received(&mut caller, id, &io);
            assert!(
                matches!(events.as_slice(), [Next::Done(Err(status))] if status.code == grpc::Code::DeadlineExceeded),
                "the expired call was not ended with DEADLINE_EXCEEDED at {millis}ms"
            );
        }
    }

    #[test]
    fn gateway_answers_a_handler_made_ready_before_its_deadline() {
        use core::time::Duration;
        let clock = ManualTimer(Rc::new(core::cell::Cell::new(Duration::ZERO)));
        let service = Local::default();
        let mut router = Router::builder()
            .with_service(service.clone())
            .build()
            .unwrap();
        let mut caller = grpc::Client::new(ClientConfig::default());
        let id = start_call(&mut caller, GATE_PATH, Some(Duration::from_millis(100)));
        let io = GatewayIo::with_inbound(&caller.take_output());
        let mut serving = pin!(serve_router_alone(io.clone(), &clock, &mut router));
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());

        service.open.store(true, SeqCst);
        clock.0.set(Duration::from_millis(99));
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());
        let events = received(&mut caller, id, &io);
        assert!(
            matches!(events.as_slice(), [Next::Message(m), Next::Done(Ok(()))] if m == b"answer"),
            "the call was not answered"
        );
        assert_eq!(service.cancelled.load(SeqCst), 0);
    }

    #[test]
    fn gateway_keeps_serving_calls_after_one_expires() {
        use core::time::Duration;
        let clock = ManualTimer(Rc::new(core::cell::Cell::new(Duration::ZERO)));
        let service = Local::default();
        let mut router = Router::builder()
            .with_service(service.clone())
            .build()
            .unwrap();
        let mut caller = grpc::Client::new(ClientConfig::default());
        let gate = start_call(&mut caller, GATE_PATH, Some(Duration::from_millis(100)));
        let io = GatewayIo::with_inbound(&caller.take_output());
        let mut serving = pin!(serve_router_alone(io.clone(), &clock, &mut router));
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());
        service.open.store(true, SeqCst);
        clock.0.set(Duration::from_millis(101));
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());
        let events = received(&mut caller, gate, &io);
        assert!(matches!(events.as_slice(), [Next::Done(Err(_))]));

        let quick = start_call(&mut caller, QUICK_PATH, None);
        io.0.borrow_mut().inbound.extend(caller.take_output());
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());
        let events = received(&mut caller, quick, &io);
        assert!(
            matches!(events.as_slice(), [Next::Message(m), Next::Done(Ok(()))] if m == b"answer"),
            "the gateway stopped serving after a call expired"
        );
    }

    #[test]
    fn overdue_output_retires_the_gateway_without_polling_its_worker() {
        use core::time::Duration;
        let clock = ManualTimer(Rc::new(core::cell::Cell::new(Duration::ZERO)));
        let mut router = Router::builder()
            .with_service(Local::default())
            .build()
            .unwrap();
        let mut caller = grpc::Client::new(ClientConfig::default());
        start_call(&mut caller, GATE_PATH, Some(Duration::from_millis(100)));
        let io = GatewayIo::with_inbound(&caller.take_output());
        io.0.borrow_mut().stall_writes = true;
        let mut serving = pin!(serve_router_alone(io.clone(), &clock, &mut router));
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());
        let polls = io.0.borrow().write_polls;

        // The write is released in the same step as its deadline passes.
        io.0.borrow_mut().stall_writes = false;
        clock.0.set(Duration::from_millis(100));
        assert!(matches!(
            poll_once(serving.as_mut(), Waker::noop()),
            Poll::Ready(Err(Error::OutputDeadline))
        ));
        assert_eq!(
            io.0.borrow().write_polls,
            polls,
            "overdue output was polled before its transport was retired"
        );
    }

    #[test]
    fn protocol_error_during_a_pending_write_writes_nothing_more() {
        let (before, _) = output_around_protocol_error();
        // The first write accepts three bytes, then stays pending. The input is
        // read first, so the protocol error arrives while that write is in flight.
        let io = GatewayIo::with_inbound(GARBAGE);
        {
            let mut wire = io.0.borrow_mut();
            wire.stall_writes = true;
            wire.emit = 3;
        }
        let clock = ManualTimer(Rc::new(core::cell::Cell::new(core::time::Duration::ZERO)));
        let mut router = Router::builder()
            .with_service(Local::default())
            .build()
            .unwrap();
        let mut serving = pin!(serve_router_alone(io.clone(), &clock, &mut router));
        assert!(matches!(
            poll_once(serving.as_mut(), Waker::noop()),
            Poll::Ready(Err(Error::Protocol(_)))
        ));
        let wire = io.0.borrow();
        assert_eq!(wire.written, before[..3]);
        assert_eq!((wire.writes, wire.flushes, wire.cancelled), (1, 0, 1));
    }

    #[test]
    fn protocol_error_during_a_pending_flush_writes_nothing_more() {
        let (before, _) = output_around_protocol_error();
        let io = GatewayIo::with_inbound(GARBAGE);
        {
            let mut wire = io.0.borrow_mut();
            // The worker polls a read before each request it takes, and a read
            // dropped for a request is polled again after it. The input becomes
            // readable on the third poll, which is the one that starts the flush
            // the coordinator asks for once the write is reaped.
            wire.read_ready_after = 2;
            wire.stall_flushes = true;
        }
        let clock = ManualTimer(Rc::new(core::cell::Cell::new(core::time::Duration::ZERO)));
        let mut router = Router::builder()
            .with_service(Local::default())
            .build()
            .unwrap();
        let mut serving = pin!(serve_router_alone(io.clone(), &clock, &mut router));
        assert!(matches!(
            poll_once(serving.as_mut(), Waker::noop()),
            Poll::Ready(Err(Error::Protocol(_)))
        ));
        let wire = io.0.borrow();
        assert_eq!(wire.written, before);
        assert_eq!((wire.writes, wire.flushes, wire.cancelled), (1, 1, 1));
    }

    #[test]
    fn protocol_error_after_a_short_write_sends_only_the_rest_and_the_goaway() {
        let (before, goaway) = output_around_protocol_error();
        let io = GatewayIo::with_inbound(GARBAGE);
        io.0.borrow_mut().max_write = 1;
        let clock = ManualTimer(Rc::new(core::cell::Cell::new(core::time::Duration::ZERO)));
        let mut router = Router::builder()
            .with_service(Local::default())
            .build()
            .unwrap();
        let mut serving = pin!(serve_router_alone(io.clone(), &clock, &mut router));
        assert!(matches!(
            poll_once(serving.as_mut(), Waker::noop()),
            Poll::Ready(Err(Error::Protocol(_)))
        ));
        assert_eq!(io.0.borrow().written, [&before[..], &goaway[..]].concat());
    }

    #[test]
    fn goaway_is_written_on_a_protocol_error_with_no_output_in_flight() {
        let (before, goaway) = output_around_protocol_error();
        let io = GatewayIo::default();
        let clock = ManualTimer(Rc::new(core::cell::Cell::new(core::time::Duration::ZERO)));
        let mut router = Router::builder()
            .with_service(Local::default())
            .build()
            .unwrap();
        let mut serving = pin!(serve_router_alone(io.clone(), &clock, &mut router));
        // The initial output is written and flushed, and a read is left pending.
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());
        assert_eq!(io.0.borrow().written, before);

        io.0.borrow_mut().inbound.extend(GARBAGE);
        assert!(matches!(
            poll_once(serving.as_mut(), Waker::noop()),
            Poll::Ready(Err(Error::Protocol(_)))
        ));
        assert_eq!(io.0.borrow().written, [&before[..], &goaway[..]].concat());
    }

    /// A transport that accepts every write, never completes a read, and keeps
    /// what was written.
    #[derive(Clone, Default)]
    struct Sink(Rc<RefCell<Vec<u8>>>);

    impl embedded_io_async::ErrorType for Sink {
        type Error = Infallible;
    }

    impl Read for Sink {
        async fn read(&mut self, _: &mut [u8]) -> Result<usize, Infallible> {
            core::future::pending().await
        }
    }

    impl Write for Sink {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            self.0.borrow_mut().extend_from_slice(buf);
            Ok(buf.len())
        }

        async fn flush(&mut self) -> Result<(), Infallible> {
            Ok(())
        }
    }

    #[test]
    fn a_connection_whose_handshake_exceeds_the_output_limit_writes_nothing() {
        let limit = 10;
        let io = Sink::default();
        let server = server(io.clone())
            .without_deadlines()
            .with_http2(crate::grpc::http2::Config {
                max_pending_output_bytes: limit,
                ..Default::default()
            })
            .build();
        let mut services = FnHandler(|_: &str, req: &[u8]| Some(Ok(req.to_vec())));
        let result = run(server.serve(&mut services));
        assert!(matches!(
            result,
            Err(Error::OutputLimitExceeded { limit: l, pending: 0, .. }) if l == limit
        ));
        assert!(io.0.borrow().is_empty(), "no partial handshake is written");
    }

    #[test]
    fn a_client_whose_handshake_exceeds_the_output_limit_fails_with_an_overload() {
        let mut client = client(Sink::default())
            .without_deadlines()
            .with_http2(crate::grpc::http2::Config {
                max_pending_output_bytes: 10,
                ..Default::default()
            })
            .build();
        let status = run(client.unary("/test/Echo", b"x")).unwrap_err();
        assert_eq!(status.code, Code::Unavailable);
        assert!(
            status.message.contains("output limit"),
            "{}",
            status.message
        );
    }

    /// The input half of a split transport: chunks the test pushes, waking the
    /// reader that waits for them.
    #[derive(Clone, Default)]
    struct SplitIn(Rc<RefCell<SplitInState>>);

    #[derive(Default)]
    struct SplitInState {
        chunks: VecDeque<Vec<u8>>,
        waker: Option<Waker>,
    }

    impl SplitIn {
        fn push(&self, bytes: Vec<u8>) {
            let mut state = self.0.borrow_mut();
            state.chunks.push_back(bytes);
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        }
    }

    impl embedded_io_async::ErrorType for SplitIn {
        type Error = Infallible;
    }

    impl Read for SplitIn {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            poll_fn(|cx| {
                let mut state = self.0.borrow_mut();
                match state.chunks.pop_front() {
                    Some(chunk) => {
                        let n = chunk.len().min(buf.len());
                        buf[..n].copy_from_slice(&chunk[..n]);
                        // What the buffer could not take is read next.
                        if n < chunk.len() {
                            state.chunks.push_front(chunk[n..].to_vec());
                        }
                        Poll::Ready(Ok(n))
                    }
                    None => {
                        state.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            })
            .await
        }
    }

    /// The output half of a split transport: what it accepted, and whether its
    /// writes are stalled.
    #[derive(Clone, Default)]
    struct SplitOut {
        written: Rc<RefCell<Vec<u8>>>,
        stalled: Rc<core::cell::Cell<bool>>,
    }

    impl embedded_io_async::ErrorType for SplitOut {
        type Error = Infallible;
    }

    impl Write for SplitOut {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            poll_fn(|_| {
                if self.stalled.get() {
                    return Poll::Pending;
                }
                self.written.borrow_mut().extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            })
            .await
        }

        async fn flush(&mut self) -> Result<(), Infallible> {
            Ok(())
        }
    }

    /// A response that arrives on the input half while the owner's write is
    /// blocked completes its waiting call, without the write being finished.
    #[test]
    fn response_waiter_completes_while_owner_tx_is_pending() {
        use crate::grpc::http2::{Config as H2, Connection as Peer, HeaderField};
        let input = SplitIn::default();
        let output = SplitOut::default();
        let client = client_split(input.clone(), output.clone())
            .without_deadlines()
            .build();
        let mut b = client.streaming(QUIET).unwrap();
        run(b.send(b"hi")).unwrap();

        // The peer answers the call B has started, and nothing else.
        let mut peer = Peer::server(H2::default());
        peer.recv(&output.written.borrow()).unwrap();
        let field = |name: &str, value: &str| HeaderField {
            name: name.into(),
            value: value.into(),
        };
        let b_id = b.id();
        peer.send_headers(
            b_id,
            alloc::vec![
                field(":status", "200"),
                field("content-type", "application/grpc"),
            ],
            false,
        )
        .unwrap();
        peer.send_data(b_id, crate::grpc::lpm::encode(b"reply").unwrap(), false)
            .unwrap();

        // The owner's write is now blocked, so the owner is A.
        output.stalled.set(true);
        let (wake_a, _) = Wakes::waker();
        let (wake_b, _) = Wakes::waker();
        let mut a = client.streaming(QUIET).unwrap();
        let mut a_send = pin!(a.send(&[7; 2000]));
        assert!(poll_once(a_send.as_mut(), &wake_a).is_pending());

        // B waits for the transport, since it holds no result yet.
        let mut b_message = pin!(b.message());
        assert!(poll_once(b_message.as_mut(), &wake_b).is_pending());

        // The response reaches the reader. A reads it while its write stays blocked, and
        // B returns the message without taking the transport.
        input.push(peer.take_output());
        assert!(poll_once(a_send.as_mut(), &wake_a).is_pending());
        let Poll::Ready(Ok(Some(message))) = poll_once(b_message.as_mut(), &wake_b) else {
            panic!("the response waiter did not complete while the write was blocked");
        };
        assert_eq!(message, b"reply");
        assert!(
            poll_once(a_send.as_mut(), &wake_a).is_pending(),
            "the owner's write is still blocked"
        );
    }

    /// Bytes a peer sends to answer `call` with one message.
    fn peer_reply(
        peer: &mut crate::grpc::http2::Connection,
        call: CallId,
        message: &[u8],
    ) -> Vec<u8> {
        use crate::grpc::http2::HeaderField;
        let field = |name: &str, value: &str| HeaderField {
            name: name.into(),
            value: value.into(),
        };
        peer.send_headers(
            call,
            alloc::vec![
                field(":status", "200"),
                field("content-type", "application/grpc"),
            ],
            false,
        )
        .unwrap();
        peer.send_data(call, crate::grpc::lpm::encode(message).unwrap(), false)
            .unwrap();
        peer.take_output()
    }

    /// A waiter that consumes its response releases the credit it occupied, and
    /// the owner, whose write is blocked, is woken to write that credit.
    #[test]
    fn consuming_waiter_response_wakes_owner_for_credit() {
        use crate::grpc::http2::{Config as H2, Connection as Peer};
        let input = SplitIn::default();
        let output = SplitOut::default();
        let config = crate::grpc::ClientConfig {
            max_message_size: 64 * 1024,
            ..crate::grpc::ClientConfig::default()
        };
        let client = client_split(input.clone(), output.clone())
            .with_config(config)
            .without_deadlines()
            .build();
        let mut b = client.streaming(QUIET).unwrap();
        run(b.send(b"hi")).unwrap();
        let mut peer = Peer::server(H2::default());
        peer.recv(&output.written.borrow()).unwrap();
        // Large enough that consuming it passes the window update threshold.
        let reply = peer_reply(&mut peer, b.id(), &[9; 40_000]);

        output.stalled.set(true);
        let (wake_a, counted_a) = Wakes::waker();
        let (wake_b, _) = Wakes::waker();
        let mut a = client.streaming(QUIET).unwrap();
        let mut a_send = pin!(a.send(&[7; 2000]));
        assert!(poll_once(a_send.as_mut(), &wake_a).is_pending());

        let mut b_message = pin!(b.message());
        assert!(poll_once(b_message.as_mut(), &wake_b).is_pending());
        input.push(reply);
        // The owner reads the response in as many polls as its read buffer needs,
        // while its own write stays blocked.
        let before = counted_a.count();
        let mut result = None;
        for _ in 0..1_000 {
            assert!(poll_once(a_send.as_mut(), &wake_a).is_pending());
            if let Poll::Ready(outcome) = poll_once(b_message.as_mut(), &wake_b) {
                result = Some(outcome);
                break;
            }
        }
        let Some(Ok(Some(message))) = result else {
            panic!("the waiter did not receive its response: {result:?}");
        };
        assert_eq!(message.len(), 40_000);
        assert!(
            counted_a.count() > before,
            "consuming the response did not wake the owner for the credit it released"
        );
    }

    /// The owner's own reply is read while its write is blocked. It stays
    /// buffered until the write completes, and is then returned.
    #[test]
    fn owner_result_stays_buffered_until_safe_return() {
        use crate::grpc::http2::{Config as H2, Connection as Peer};
        let input = SplitIn::default();
        let output = SplitOut::default();
        let client = client_split(input.clone(), output.clone())
            .without_deadlines()
            .build();
        let mut x = client.streaming(QUIET).unwrap();
        run(x.send(b"hi")).unwrap();
        let mut peer = Peer::server(H2::default());
        peer.recv(&output.written.borrow()).unwrap();
        input.push(peer_reply(&mut peer, x.id(), b"reply"));

        output.stalled.set(true);
        let (wake_x, _) = Wakes::waker();
        {
            let mut send = pin!(x.send(&[7; 2000]));
            // The owner reads the reply while its write is blocked, and keeps it.
            assert!(poll_once(send.as_mut(), &wake_x).is_pending());
            assert!(poll_once(send.as_mut(), &wake_x).is_pending());

            output.stalled.set(false);
            assert!(matches!(
                poll_once(send.as_mut(), &wake_x),
                Poll::Ready(Ok(()))
            ));
        }
        let message = run(x.message()).unwrap();
        assert_eq!(message, Some(b"reply".to_vec()));
    }

    /// The owner's write is blocked, and a contender waits for the transport.
    /// When the owner's write completes, it gives the transport up at that
    /// safe point and wakes the contender, which then writes.
    #[test]
    fn safe_handoff_gives_contender_a_turn() {
        let input = SplitIn::default();
        let output = SplitOut::default();
        let client = client_split(input.clone(), output.clone())
            .without_deadlines()
            .build();
        let mut a = client.streaming(QUIET).unwrap();
        let mut b = client.streaming(QUIET).unwrap();

        output.stalled.set(true);
        let (wake_a, _) = Wakes::waker();
        let (wake_b, counted_b) = Wakes::waker();
        let mut a_send = pin!(a.send(&[7; 2000]));
        assert!(poll_once(a_send.as_mut(), &wake_a).is_pending());
        let mut b_send = pin!(b.send(b"hi"));
        assert!(poll_once(b_send.as_mut(), &wake_b).is_pending());

        output.stalled.set(false);
        let before = counted_b.count();
        assert!(matches!(
            poll_once(a_send.as_mut(), &wake_a),
            Poll::Ready(Ok(()))
        ));
        assert!(
            counted_b.count() > before,
            "the contender was not woken at the handoff"
        );
        assert!(matches!(
            poll_once(b_send.as_mut(), &wake_b),
            Poll::Ready(Ok(()))
        ));
        assert!(output.written.borrow().ends_with(b"hi"));
    }

    /// A waiter cancelled while the owner's write is blocked leaves no
    /// registration behind, so the owner's progress does not wake its waker.
    #[test]
    fn cancelled_registration_is_removed_during_duplex_tx() {
        let input = SplitIn::default();
        let output = SplitOut::default();
        let client = client_split(input.clone(), output.clone())
            .without_deadlines()
            .build();
        let mut a = client.streaming(QUIET).unwrap();
        let mut b = client.streaming(QUIET).unwrap();

        output.stalled.set(true);
        let (wake_a, _) = Wakes::waker();
        let (wake_b, counted_b) = Wakes::waker();
        let mut a_send = pin!(a.send(&[7; 2000]));
        assert!(poll_once(a_send.as_mut(), &wake_a).is_pending());
        {
            let mut b_message = pin!(b.message());
            assert!(poll_once(b_message.as_mut(), &wake_b).is_pending());
            assert_eq!(client.shared.with(|s| s.waiters.len()), 1);
        }
        assert_eq!(client.shared.with(|s| s.waiters.len()), 0);

        output.stalled.set(false);
        assert!(matches!(
            poll_once(a_send.as_mut(), &wake_a),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(counted_b.count(), 0, "the cancelled waiter was woken");
    }

    /// A transport whose writes accept only the bytes the test grants, and
    /// whose flushes stall while the test says so. Its input is fed by the test.
    #[derive(Clone, Default)]
    struct Gated(Rc<RefCell<GatedState>>);

    #[derive(Default)]
    struct GatedState {
        inbound: VecDeque<Vec<u8>>,
        reader: Option<Waker>,
        grant: usize,
        flushes_stalled: bool,
        written: Vec<u8>,
        /// Writes and flushes started.
        writes_started: usize,
        flushes_started: usize,
    }

    impl Gated {
        fn feed(&self, bytes: Vec<u8>) {
            let mut state = self.0.borrow_mut();
            state.inbound.push_back(bytes);
            if let Some(waker) = state.reader.take() {
                waker.wake();
            }
        }

        fn grant(&self, bytes: usize) {
            self.0.borrow_mut().grant = bytes;
        }
    }

    impl embedded_io_async::ErrorType for Gated {
        type Error = Infallible;
    }

    impl Read for Gated {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            poll_fn(|cx| {
                let mut state = self.0.borrow_mut();
                match state.inbound.pop_front() {
                    Some(chunk) => {
                        let n = chunk.len().min(buf.len());
                        buf[..n].copy_from_slice(&chunk[..n]);
                        Poll::Ready(Ok(n))
                    }
                    None => {
                        state.reader = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            })
            .await
        }
    }

    impl Write for Gated {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            self.0.borrow_mut().writes_started += 1;
            poll_fn(|_| {
                let mut state = self.0.borrow_mut();
                if state.grant == 0 {
                    return Poll::Pending;
                }
                let n = buf.len().min(state.grant);
                state.grant -= n;
                state.written.extend_from_slice(&buf[..n]);
                Poll::Ready(Ok(n))
            })
            .await
        }

        async fn flush(&mut self) -> Result<(), Infallible> {
            self.0.borrow_mut().flushes_started += 1;
            poll_fn(|_| {
                if self.0.borrow().flushes_stalled {
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                }
            })
            .await
        }
    }

    /// Answers every call with messages until the call is cancelled.
    struct Endless;

    impl Handler for Endless {
        fn route(&self, _: &str) -> Option<Route> {
            Some(Route::new(MethodKind::ServerStreaming, 0))
        }

        fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
            Ok(())
        }

        fn poll_response(
            &mut self,
            _: &mut CallContext<'_>,
            _: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            Poll::Ready(Next::Message(alloc::vec![7; 300]))
        }
    }

    /// The server's stream of output has been partly written, and the call that
    /// produced it is then reset by its client. The rest of the output still has
    /// to be delivered by the call's deadline, which must not be forgotten when
    /// the call goes: `variant` decides whether the next operation is a write or
    /// the flush.
    fn reset_call_output_expires(flush_next: bool) {
        use core::time::Duration;
        let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
        let io = Gated::default();
        let server = server(io.clone())
            .with_deadline_timer(ManualTimer(now.clone()))
            .build();
        let mut caller = crate::grpc::Client::new(crate::grpc::ClientConfig::default());
        let id = caller
            .start_streaming_with(
                "/test/Stream",
                &crate::grpc::CallOptions::timeout(Duration::from_millis(100)),
            )
            .unwrap();
        caller.send_message(id, b"request").unwrap();
        caller.close_send(id).unwrap();
        io.feed(caller.take_output());

        let mut handler = Endless;
        let mut serving = pin!(server.serve(&mut handler));
        // Output is produced, and no write can complete yet.
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());

        now.set(Duration::from_millis(10));
        caller.cancel(id);
        io.feed(caller.take_output());
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());

        // One byte is accepted, and the output after it stays blocked.
        now.set(Duration::from_millis(20));
        if flush_next {
            // Everything is accepted, and the flush that follows stalls.
            io.0.borrow_mut().flushes_stalled = true;
            io.grant(usize::MAX);
        } else {
            io.grant(1);
        }
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());

        // The call's deadline has passed: the connection's output is retired.
        now.set(Duration::from_millis(100));
        let Poll::Ready(Err(error)) = poll_once(serving.as_mut(), Waker::noop()) else {
            panic!("the reset call's output was not held to its deadline");
        };
        assert!(matches!(error, Error::OutputDeadline));
    }

    #[test]
    fn delivery_deadline_survives_reset_and_short_write() {
        reset_call_output_expires(false);
    }

    #[test]
    fn delivery_deadline_survives_reset_and_completed_writes_before_flush() {
        reset_call_output_expires(true);
    }

    /// An unflushed delivery carried into a new owner keeps its deadline, though
    /// the call that wrote the output is no longer part of the client's state.
    #[test]
    fn client_delivery_deadline_survives_owner_handoff() {
        use core::time::Duration;
        let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
        let io = Gated::default();
        {
            let mut wire = io.0.borrow_mut();
            wire.flushes_stalled = true;
        }
        io.grant(usize::MAX);
        let client = client(io.clone())
            .with_deadline_timer(ManualTimer(now.clone()))
            .build();
        // A previous owner left written output that is not flushed yet.
        client.shared.with(|s| {
            s.delivery = Delivery {
                unflushed: true,
                deadline: Some(Duration::from_millis(100)),
            };
        });
        let mut call = client.streaming(QUIET).unwrap();
        let mut message = pin!(call.message());
        assert!(poll_once(message.as_mut(), Waker::noop()).is_pending());

        now.set(Duration::from_millis(100));
        let Poll::Ready(Err(status)) = poll_once(message.as_mut(), Waker::noop()) else {
            panic!("the carried delivery deadline was disarmed");
        };
        assert_eq!(status.code, grpc::Code::Unavailable);
        assert!(
            status.message.contains("output deadline"),
            "{}",
            status.message
        );
    }

    /// Output that becomes ready at the instant its deadline is due is not
    /// progress: the expiry is processed first, and nothing is written after it.
    #[test]
    fn ready_tx_at_expired_deadline_cannot_disarm_retirement() {
        use core::time::Duration;
        let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
        let io = Gated::default();
        let mut client = client(io.clone())
            .with_deadline_timer(ManualTimer(now.clone()))
            .build();
        {
            let mut call = client
                .streaming_with(QUIET, CallOptions::timeout(Duration::from_millis(100)))
                .unwrap();
            let mut message = pin!(call.message());
            assert!(poll_once(message.as_mut(), Waker::noop()).is_pending());

            now.set(Duration::from_millis(100));
            io.grant(usize::MAX);
            let Poll::Ready(result) = poll_once(message.as_mut(), Waker::noop()) else {
                panic!("the due output did not end the call");
            };
            assert_eq!(result.unwrap_err().code, grpc::Code::DeadlineExceeded);
        }
        assert!(
            io.0.borrow().written.is_empty(),
            "the due write was not performed"
        );
        let status = run(client.unary("/t.T/Echo", b"x")).unwrap_err();
        assert_eq!(status.code, grpc::Code::Unavailable);
        assert!(
            status.message.contains("output deadline"),
            "{}",
            status.message
        );
    }

    /// Answers a call with one `first` message, then, once released, with a
    /// message of `second` bytes. Until then it is pending, and it wakes the
    /// task that polled it when released.
    struct Released {
        released: Rc<core::cell::Cell<bool>>,
        waker: Rc<RefCell<Option<Waker>>>,
        first: usize,
        second: usize,
        answered: usize,
        cancelled: Rc<core::cell::Cell<usize>>,
    }

    impl Handler for Released {
        fn route(&self, _: &str) -> Option<Route> {
            Some(Route::new(MethodKind::ServerStreaming, 0))
        }

        fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
            Ok(())
        }

        fn poll_response(
            &mut self,
            _: &mut CallContext<'_>,
            cx: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            self.answered += 1;
            if self.answered == 1 {
                return Poll::Ready(Next::Message(alloc::vec![1; self.first]));
            }
            if !self.released.get() {
                *self.waker.borrow_mut() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(Next::Message(alloc::vec![2; self.second]))
        }

        fn on_cancel(&mut self, _: &mut CallContext<'_>) {
            self.cancelled.set(self.cancelled.get() + 1);
        }
    }

    /// A handler's output that does not fit, produced while the server's output
    /// is blocked: the connection ends at once, with no further transport
    /// operation and with the call cancelled once. `flush_blocked` says whether
    /// the blocked operation is the flush, after the first message was written.
    fn handler_output_limit_during_pending_tx(flush_blocked: bool) {
        use core::time::Duration;
        let limit = 1024;
        let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
        let io = Gated::default();
        let server = server(io.clone())
            .with_deadline_timer(ManualTimer(now.clone()))
            .with_http2(crate::grpc::http2::Config {
                max_pending_output_bytes: limit,
                ..Default::default()
            })
            .build();
        let mut caller = crate::grpc::Client::new(crate::grpc::ClientConfig::default());
        let id = caller.start_streaming("/test/Released").unwrap();
        caller.send_message(id, b"request").unwrap();
        caller.close_send(id).unwrap();
        io.feed(caller.take_output());

        let released = Rc::new(core::cell::Cell::new(false));
        let waker = Rc::new(RefCell::new(None));
        let cancelled = Rc::new(core::cell::Cell::new(0));
        let mut handler = Released {
            released: released.clone(),
            waker: waker.clone(),
            first: 100,
            second: 2048,
            answered: 0,
            cancelled: cancelled.clone(),
        };
        if flush_blocked {
            // The first message is accepted, and its flush stalls.
            io.0.borrow_mut().flushes_stalled = true;
            io.grant(usize::MAX);
        }
        // Boxed, so that dropping it ends the connection's future.
        let mut serving = alloc::boxed::Box::pin(server.serve(&mut handler));
        assert!(poll_once(serving.as_mut(), Waker::noop()).is_pending());
        let (writes, flushes) = {
            let wire = io.0.borrow();
            (wire.writes_started, wire.flushes_started)
        };

        // The handler is released, and its output does not fit the cap.
        released.set(true);
        let wake = waker.borrow_mut().take().expect("the handler is pending");
        wake.wake();
        let Poll::Ready(result) = poll_once(serving.as_mut(), Waker::noop()) else {
            panic!("the output limit did not end the connection while the transport was blocked");
        };
        assert!(matches!(
            result,
            Err(Error::OutputLimitExceeded { limit: l, .. }) if l == limit
        ));
        assert_eq!(cancelled.get(), 1, "the call was cancelled once");
        drop(serving);
        assert_eq!(cancelled.get(), 1, "the call is not cancelled again");
        let wire = io.0.borrow();
        assert_eq!(
            (wire.writes_started, wire.flushes_started),
            (writes, flushes)
        );
        let _ = id;
    }

    #[test]
    fn handler_output_limit_ends_server_during_pending_tx() {
        handler_output_limit_during_pending_tx(false);
    }

    #[test]
    fn handler_output_limit_ends_server_during_pending_flush() {
        handler_output_limit_during_pending_tx(true);
    }

    /// A call that cannot be queued ends the connection for every call: the
    /// owner, blocked on a write, and the task waiting for the transport are both
    /// told, and the owner retires without writing again.
    #[test]
    fn external_output_limit_wakes_and_retires_busy_owner() {
        let io = Gated::default();
        let client = client(io.clone())
            .without_deadlines()
            .with_http2(crate::grpc::http2::Config {
                max_pending_output_bytes: 1024,
                ..Default::default()
            })
            .build();
        let (wake_a, a_wakes) = Wakes::waker();
        let (wake_c, c_wakes) = Wakes::waker();
        let mut a = client.streaming(QUIET).unwrap();
        let mut a_message = pin!(a.message());
        assert!(poll_once(a_message.as_mut(), &wake_a).is_pending());
        let mut c = client.streaming(QUIET).unwrap();
        let mut c_message = pin!(c.message());
        assert!(poll_once(c_message.as_mut(), &wake_c).is_pending());
        // Starting C woke the owner, which registers again when it is polled.
        assert!(poll_once(a_message.as_mut(), &wake_a).is_pending());
        let (a_before, c_before) = (a_wakes.count(), c_wakes.count());

        // The metadata of this call does not fit the cap, so it cannot start.
        let mut metadata = crate::grpc::Metadata::new();
        metadata.insert("x-big", &"a".repeat(2000)).unwrap();
        let Err(status) = client.streaming_with(
            QUIET,
            crate::grpc::CallOptions::default().with_metadata(metadata),
        ) else {
            panic!("the call started with a header block over the cap");
        };
        assert_eq!(status.code, grpc::Code::Unavailable);
        assert!(
            status.message.contains("output limit"),
            "{}",
            status.message
        );
        assert!(a_wakes.count() > a_before, "the owner is woken to retire");
        assert!(c_wakes.count() > c_before, "the waiting call is notified");

        // The owner retires without its blocked write completing, and the waiter
        // observes the same failure.
        let Poll::Ready(Err(status)) = poll_once(a_message.as_mut(), &wake_a) else {
            panic!("the owner did not retire while its write was blocked");
        };
        assert_eq!(status.code, grpc::Code::Unavailable);
        let Poll::Ready(Err(status)) = poll_once(c_message.as_mut(), &wake_c) else {
            panic!("the waiting call did not observe the failure");
        };
        assert_eq!(status.code, grpc::Code::Unavailable);
        assert!(
            io.0.borrow().written.is_empty(),
            "nothing was written after the failure"
        );
    }

    /// A terminal owner leaves its wait loop even while its write is still
    /// pending, and without being woken: the state it waits on is already final.
    #[test]
    fn terminal_owner_readiness_does_not_depend_on_tx_completion() {
        let io = Gated::default();
        let client = client(io.clone()).without_deadlines().build();
        let (wake, _) = Wakes::waker();
        let mut a = client.streaming(QUIET).unwrap();
        let mut a_message = pin!(a.message());
        assert!(poll_once(a_message.as_mut(), &wake).is_pending());

        // Retired by something other than this owner, with no notification.
        client
            .shared
            .with(|s| retire(s, Status::unavailable("external failure")));
        let Poll::Ready(Err(status)) = poll_once(a_message.as_mut(), &wake) else {
            panic!("a terminal owner waited on its pending write");
        };
        assert_eq!(status.code, grpc::Code::Unavailable);
        assert!(io.0.borrow().written.is_empty());
    }

    /// The reset written when a call expires is blocked, but the expiry ends the
    /// waiting call at once: its task is notified, and it returns its deadline
    /// status without the transport being released.
    #[test]
    fn deadline_tick_notifies_waiter_while_reset_write_is_blocked() {
        use core::time::Duration;
        let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
        let io = Gated::default();
        // The first output is written, so the owner is idle with a pending read.
        io.grant(usize::MAX);
        let client = client(io.clone())
            .with_deadline_timer(ManualTimer(now.clone()))
            .build();
        let (wake_a, _) = Wakes::waker();
        let (wake_b, b_wakes) = Wakes::waker();
        let mut a = client.streaming(QUIET).unwrap();
        let mut b = client
            .streaming_with(QUIET, CallOptions::timeout(Duration::from_millis(100)))
            .unwrap();
        let mut a_message = pin!(a.message());
        assert!(poll_once(a_message.as_mut(), &wake_a).is_pending());
        let mut b_message = pin!(b.message());
        assert!(poll_once(b_message.as_mut(), &wake_b).is_pending());

        // From now on writes stall, so the reset written at expiry is blocked.
        io.grant(0);
        let written = io.0.borrow().written.len();
        now.set(Duration::from_millis(100));
        assert!(poll_once(a_message.as_mut(), &wake_a).is_pending());
        assert!(b_wakes.count() >= 1, "the expired call's task is notified");
        let Poll::Ready(Err(status)) = poll_once(b_message.as_mut(), &wake_b) else {
            panic!("the expired call did not finish");
        };
        assert_eq!(status.code, grpc::Code::DeadlineExceeded);
        assert_eq!(
            io.0.borrow().written.len(),
            written,
            "the transport was not released"
        );
    }

    /// A clock reading that changes nothing notifies nobody: waiters keep their
    /// wait, and the owner is not woken.
    #[test]
    fn unchanged_tick_does_not_wake_waiters() {
        use core::time::Duration;
        let now = Rc::new(core::cell::Cell::new(Duration::ZERO));
        let io = Gated::default();
        io.grant(usize::MAX);
        let client = client(io.clone())
            .with_deadline_timer(ManualTimer(now.clone()))
            .build();
        let (wake_a, a_wakes) = Wakes::waker();
        let (wake_b, b_wakes) = Wakes::waker();
        let mut a = client
            .streaming_with(QUIET, CallOptions::timeout(Duration::from_millis(100)))
            .unwrap();
        let mut b = client.streaming(QUIET).unwrap();
        let mut a_message = pin!(a.message());
        assert!(poll_once(a_message.as_mut(), &wake_a).is_pending());
        let mut b_message = pin!(b.message());
        assert!(poll_once(b_message.as_mut(), &wake_b).is_pending());
        let (a_before, b_before) = (a_wakes.count(), b_wakes.count());

        for step in [10, 20, 30] {
            now.set(Duration::from_millis(step));
            assert!(poll_once(a_message.as_mut(), &wake_a).is_pending());
            assert!(poll_once(b_message.as_mut(), &wake_b).is_pending());
        }
        assert_eq!(
            b_wakes.count(),
            b_before,
            "a waiter was woken by an unchanged tick"
        );
        assert_eq!(
            a_wakes.count(),
            a_before,
            "the owner was woken by an unchanged tick"
        );
    }
}
