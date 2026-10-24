//! Async drivers over `embedded_io_async`.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use embedded_io_async::{Read, Write};

use crate::builder::{DeadlinesUnspecified, client_config_methods, server_config_methods};
use crate::driver;
use crate::grpc::{
    self, CallId, CallOptions, ClientConfig, Handler, Metadata, Next, Response, Router,
    ServerConfig, Status, StreamingCall, StreamingTransport, UnaryTransport,
};
use crate::shared::Shared;
use crate::timer::{NoTimer, Timer};
use crate::{Error, READ_CHUNK};

/// Start building an async gRPC server on one connection.
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
pub fn server<IO: Read + Write>(io: IO) -> ServerBuilder<IO> {
    ServerBuilder {
        io,
        config: ServerConfig::default(),
        timer: DeadlinesUnspecified,
    }
}

/// Factory of an async [`Server`], created by [`server`].
///
/// `T` is the deadline policy: [`DeadlinesUnspecified`] until one is chosen,
/// then the [`Timer`] (or [`NoTimer`]) the server will use.
#[derive(Debug)]
pub struct ServerBuilder<IO, T = DeadlinesUnspecified> {
    io: IO,
    config: ServerConfig,
    timer: T,
}

impl<IO, T> ServerBuilder<IO, T> {
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
    pub fn with_deadline_timer<U: Timer>(self, timer: U) -> ServerBuilder<IO, U> {
        ServerBuilder {
            io: self.io,
            config: self.config,
            timer,
        }
    }

    /// Do not enforce deadlines locally. Calls still receive their
    /// `grpc-timeout` in [`CallContext::deadline`](crate::CallContext::deadline),
    /// on a clock that never advances.
    pub fn without_deadlines(self) -> ServerBuilder<IO, NoTimer> {
        self.with_deadline_timer(NoTimer)
    }

    server_config_methods!();
}

impl<IO: Read + Write, T: Timer> ServerBuilder<IO, T> {
    /// Construct the server. Performs no I/O; the HTTP/2 SETTINGS are sent
    /// once [`serve`](Server::serve) runs.
    pub fn build(self) -> Server<IO, T> {
        Server {
            io: self.io,
            config: self.config,
            timer: self.timer,
        }
    }
}

/// An async gRPC server driver for one connection, built by [`server`].
///
/// It owns the transport, the configuration and the deadline timer. The
/// application's services are only joined to it by [`serve`](Self::serve).
#[derive(Debug)]
pub struct Server<IO, T = NoTimer> {
    io: IO,
    config: ServerConfig,
    timer: T,
}

impl<IO: Read + Write, T: Timer> Server<IO, T> {
    /// Serve gRPC on this connection until the peer disconnects or the
    /// connection fails.
    ///
    /// `services` (a generated `<Service>Dispatch`, a
    /// [`Registry`](crate::Registry), or any [`Handler`]) is borrowed
    /// for the whole connection and receives every call on it; call state
    /// keyed by [`CallId`] is therefore connection-local.
    ///
    /// Streaming handlers are polled with this future's waker: when one returns
    /// `Poll::Pending` and later wakes the waker, the pending transport `read`
    /// is dropped so the new responses can be written. Serving streaming
    /// methods whose handlers return `Pending` therefore requires a
    /// cancel-safe `read` (as tokio streams,
    /// [`CobsFramed`](crate::link::CobsFramed) and the [`link`](crate::link)
    /// stack are); unary-only servers without deadlines never drop a read.
    ///
    /// When the connection ends, normally or with an error, every active
    /// streaming call is reported to [`Handler::on_cancel`] before this
    /// returns. Dropping the future before it completes drops the transport
    /// and makes the same `on_cancel` calls from the destructor; nothing is
    /// written to the peer in that case.
    pub async fn serve<H: Handler + ?Sized>(
        self,
        services: &mut H,
    ) -> Result<(), Error<IO::Error>> {
        let Self {
            mut io,
            config,
            timer,
        } = self;
        let mut serving = Serving {
            server: grpc::Server::new(config),
            handler: services,
        };
        serve_connection(&mut io, &mut serving.server, &mut *serving.handler, &timer).await
    }

    /// Serve gRPC on this connection with `router`, which serves its local
    /// services and forwards the rest to `downstream` connections.
    ///
    /// Downstream `i` is `downstream[i]`, in the order the router's routes
    /// were added with [`with_route`](grpc::RouterBuilder::with_route). The
    /// gateway connection and every downstream are driven by this one future.
    /// Each transport has its own operations: a write or flush that stays
    /// pending on one of them does not hold up the others, the local services,
    /// or the deadlines. A transport still runs one operation at a time, so its
    /// read is dropped when that transport has output to write, and reads must
    /// be cancel-safe as for [`serve`](Self::serve). Deadlines come from the
    /// timer of this server, and from the router's calls.
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
    ) -> Result<(), Error<IO::Error>>
    where
        B: Read + Write<Error = IO::Error>,
    {
        assert_eq!(
            router.backend_count(),
            downstream.len(),
            "serve_router needs one transport per downstream of the router"
        );
        let Self { mut io, timer, .. } = self;
        let mut routing = Routing { router, downstream };
        serve_router_connection(&mut io, &mut routing, &timer).await
    }
}

/// The router and its downstream transports for one `serve_router`. Dropping
/// it cancels every call.
struct Routing<'a, B> {
    router: &'a mut Router,
    downstream: &'a mut [B],
}

impl<B> Drop for Routing<'_, B> {
    fn drop(&mut self) {
        self.router.cancel_all();
    }
}

/// Output of one connection: the gateway's router or a downstream client.
trait Outbound {
    fn pending_output(&self) -> &[u8];
    fn consume_output(&mut self, n: usize);
    fn has_output(&self) -> bool;
    /// When output still blocked at this time retires the connection: the
    /// deadline of its own calls and of output awaiting delivery.
    fn output_deadline(&self) -> Option<Duration>;
    /// Called after a successful flush. Clients keep no flush state.
    fn output_flushed(&mut self);
}

impl Outbound for Router {
    fn pending_output(&self) -> &[u8] {
        Router::pending_output(self)
    }

    fn consume_output(&mut self, n: usize) {
        Router::consume_output(self, n);
    }

    fn has_output(&self) -> bool {
        Router::has_output(self)
    }

    fn output_deadline(&self) -> Option<Duration> {
        Router::gateway_next_deadline(self)
    }

    fn output_flushed(&mut self) {
        Router::output_flushed(self);
    }
}

impl Outbound for grpc::Client {
    fn pending_output(&self) -> &[u8] {
        grpc::Client::pending_output(self)
    }

    fn consume_output(&mut self, n: usize) {
        grpc::Client::consume_output(self, n);
    }

    fn has_output(&self) -> bool {
        grpc::Client::has_output(self)
    }

    fn output_deadline(&self) -> Option<Duration> {
        grpc::Client::next_deadline(self)
    }

    fn output_flushed(&mut self) {}
}

/// Write everything `out` has pending, then flush, as `serve_connection` does.
/// An output deadline or a failed write is an error for this connection.
async fn write_pending<S: Write, O: Outbound, T: Timer>(
    io: &mut S,
    out: &mut O,
    timer: &T,
) -> Result<(), Error<S::Error>> {
    while out.has_output() {
        let n = output_until(timer, out.output_deadline(), io.write(out.pending_output()))
            .await
            .ok_or(Error::OutputDeadline)?
            .map_err(Error::Io)?;
        if n == 0 {
            return Err(Error::WriteZero);
        }
        out.consume_output(n);
    }
    output_until(timer, out.output_deadline(), io.flush())
        .await
        .ok_or(Error::OutputDeadline)?
        .map_err(Error::Io)?;
    out.output_flushed();
    Ok(())
}

/// What went wrong with one transport operation.
enum Failure<E> {
    Io(E),
    WriteZero,
}

/// The operation a lane has in flight.
#[derive(Clone, Copy)]
struct Op {
    /// Bytes requested by a write; `0` for a flush.
    len: usize,
    /// The earliest deadline of the connection's output seen while the operation
    /// is in flight. It only moves earlier, so the operation stays armed after the
    /// call that caused it has ended.
    deadline: Option<Duration>,
}

/// The coordinator's side of one transport: the port its worker reports
/// through, the worker, and the operation in flight.
struct Lane<'p, F, E> {
    port: &'p Shared<driver::Port<E>>,
    /// `None` once the transport has been retired. Its worker is dropped then,
    /// so nothing is written to it again.
    worker: Option<Pin<Box<F>>>,
    op: Option<Op>,
    /// Bytes were written since the transport was last flushed.
    unflushed: bool,
}

impl<'p, F: Future<Output = ()>, E> Lane<'p, F, E> {
    fn new(port: &'p Shared<driver::Port<E>>, worker: Pin<Box<F>>) -> Self {
        Self {
            port,
            worker: Some(worker),
            op: None,
            unflushed: false,
        }
    }

    fn live(&self) -> bool {
        self.worker.is_some()
    }

    /// Poll the worker, which never completes: polling it lets it make progress.
    fn poll_worker(&mut self, cx: &mut Context<'_>) {
        if let Some(worker) = self.worker.as_mut() {
            let _ = worker.as_mut().poll(cx);
        }
    }

    /// Whether a reply or input is waiting to be taken.
    fn has_event(&self) -> bool {
        self.live() && self.port.with(|p| p.has_event())
    }

    /// Whether [`issue`](Self::issue) would start an operation.
    fn wants_issue<O: Outbound>(&self, out: &O) -> bool {
        self.live() && self.op.is_none() && (out.has_output() || self.unflushed)
    }

    /// Start a write of the output, or a flush once the output is all written.
    /// The bytes are copied now; the core keeps them until the reply says how
    /// many were accepted.
    fn issue<O: Outbound>(&mut self, out: &O) {
        if !self.wants_issue(out) {
            return;
        }
        let len = if out.has_output() {
            self.port.with(|p| p.request_write(out.pending_output()))
        } else {
            self.port.with(|p| p.request_flush());
            0
        };
        self.op = Some(Op {
            len,
            deadline: out.output_deadline(),
        });
    }

    /// Take the reply of the operation, if it has come back, and apply it to
    /// `out`. Returns the failure if the operation failed.
    fn reap<O: Outbound>(&mut self, out: &mut O) -> Option<Failure<E>> {
        let op = self.op?;
        let reply = self.port.with(|p| p.take_reply())?;
        self.op = None;
        match reply {
            driver::Reply::Written(Ok(n)) => match n.min(op.len) {
                0 => Some(Failure::WriteZero),
                n => {
                    out.consume_output(n);
                    self.unflushed = true;
                    None
                }
            },
            driver::Reply::Flushed(Ok(())) => {
                out.output_flushed();
                self.unflushed = false;
                None
            }
            driver::Reply::Written(Err(e)) | driver::Reply::Flushed(Err(e)) => Some(Failure::Io(e)),
        }
    }

    /// Take the outcome of the last read, if it has come back. For `Ok(n)` the
    /// bytes are in the port's inbox, until the next read starts.
    fn take_input(&mut self) -> Option<Result<usize, E>> {
        self.port.with(|p| p.take_read())
    }

    /// Run `f` on the bytes of the last read, `n` of them.
    fn with_input<R>(&self, n: usize, f: impl FnOnce(&[u8]) -> R) -> R {
        self.port.with(|p| f(&p.inbox()[..n]))
    }

    /// Keep the operation's deadline no later than the connection's current one.
    fn arm<O: Outbound>(&mut self, out: &O) {
        if let Some(op) = self.op.as_mut() {
            op.deadline = earliest([op.deadline, out.output_deadline()]);
        }
    }

    /// The deadline of the operation in flight.
    fn deadline(&self) -> Option<Duration> {
        self.op.and_then(|op| op.deadline)
    }

    /// Whether the operation in flight has passed its deadline.
    fn overdue(&self, now: Duration) -> bool {
        self.deadline().is_some_and(|d| now >= d)
    }

    /// Drop the worker, abandoning any operation it has in flight.
    fn retire(&mut self) {
        self.worker = None;
        self.op = None;
        self.unflushed = false;
    }
}

/// The earliest of the deadlines that are set.
fn earliest(deadlines: impl IntoIterator<Item = Option<Duration>>) -> Option<Duration> {
    deadlines.into_iter().flatten().min()
}

/// Retire downstream `backend`: its transport can no longer be used, and its
/// calls end with `status`. Only that downstream is affected.
fn retire_backend<F, E>(
    lane: &mut Lane<'_, F, E>,
    router: &mut Router,
    backend: usize,
    status: Status,
) where
    F: Future<Output = ()>,
{
    lane.retire();
    router.backend_failed(backend, status);
}

/// Why [`drive_router`] stopped with an error.
enum Halt<E> {
    /// Reported to the caller as it is.
    Error(Error<E>),
    /// The gateway received bytes that break the protocol. `goaway` says whether
    /// the gateway's output is in a state that allows a GOAWAY to be written.
    Protocol {
        error: grpc::http2::Error,
        goaway: bool,
    },
}

/// Serve a router's connection. A protocol error on the gateway is the one failure
/// answered on the wire, and only as far as [`Halt::Protocol`] allows: the GOAWAY
/// is best effort, and is written only when no output operation of the gateway was
/// in flight. Failures of the transports, and deadlines, never write again.
async fn serve_router_connection<IO, B, T>(
    io: &mut IO,
    routing: &mut Routing<'_, B>,
    timer: &T,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    B: Read + Write<Error = IO::Error>,
    T: Timer,
{
    match drive_router(&mut *io, routing, timer).await {
        Ok(()) => Ok(()),
        Err(Halt::Error(error)) => Err(error),
        Err(Halt::Protocol { error, goaway }) => {
            if goaway {
                // The gateway's worker has been dropped, so nothing else is writing.
                let _ = write_pending(io, &mut *routing.router, timer).await;
            }
            Err(Error::Protocol(error))
        }
    }
}

/// Drive a router's connections: the gateway's transport and each downstream
/// transport, all through one future.
///
/// The coordinator (this function) owns the router, the handlers and the clock.
/// Each transport has a worker that borrows it and performs one write, flush or
/// read at a time; the coordinator asks for operations and collects their
/// results through the transport's port. A slow transport therefore delays only
/// its own operations. Reads of a transport are dropped only when its output
/// needs the transport, and writes are never restarted.
///
/// Deadlines come before progress. Each pass reads the clock once, keeps the
/// deadline of every output in flight, and ticks the router with that reading;
/// output still past its deadline then retires its connection, before any worker
/// is polled again. The wait ends as soon as a deadline is due, before the router
/// or a worker is polled, so no handler runs on a clock that has passed the
/// deadline of its call.
async fn drive_router<IO, B, T>(
    io: &mut IO,
    routing: &mut Routing<'_, B>,
    timer: &T,
) -> Result<(), Halt<IO::Error>>
where
    IO: Read + Write,
    B: Read + Write<Error = IO::Error>,
    T: Timer,
{
    let router = &mut *routing.router;
    let downstream = &mut *routing.downstream;
    let count = downstream.len();
    let gateway_port = Shared::new(driver::Port::new());
    let ports: Vec<Shared<driver::Port<IO::Error>>> = (0..count)
        .map(|_| Shared::new(driver::Port::new()))
        .collect();
    let mut gateway = Lane::new(&gateway_port, Box::pin(driver::run(io, &gateway_port)));
    let mut lanes: Vec<_> = downstream
        .iter_mut()
        .zip(&ports)
        .map(|(transport, port)| Lane::new(port, Box::pin(driver::run(transport, port))))
        .collect();
    // The downstream lanes are visited in turn, from a rotating start.
    let mut turn = 0;
    loop {
        // One reading of the clock decides the whole pass, and the router is
        // ticked with that same reading.
        let now = timer.now();
        // Output in flight keeps the deadline it was issued with, even when the
        // tick below ends the call that caused it.
        gateway.arm(router);
        for (backend, lane) in lanes.iter_mut().enumerate() {
            if let Some(client) = router.backend(backend) {
                lane.arm(&*client);
            }
        }
        // Calls that are due end first, with their own status, so that retiring
        // the output they were waiting on does not replace it.
        router.tick(now);
        // Output that is still in flight past its deadline retires its
        // connection. For a downstream only that downstream is failed.
        if gateway.overdue(now) {
            return Err(Halt::Error(Error::OutputDeadline));
        }
        for step in 0..count {
            let backend = (turn + step) % count;
            if lanes[backend].overdue(now) {
                retire_backend(
                    &mut lanes[backend],
                    router,
                    backend,
                    Status::unavailable("downstream output deadline exceeded"),
                );
            }
        }

        // Replies to the operations in flight.
        if let Some(failure) = gateway.reap(router) {
            return Err(Halt::Error(match failure {
                Failure::Io(e) => Error::Io(e),
                Failure::WriteZero => Error::WriteZero,
            }));
        }
        for step in 0..count {
            let backend = (turn + step) % count;
            let lane = &mut lanes[backend];
            if !lane.live() {
                continue;
            }
            let client = router.backend(backend).expect("a lane per downstream");
            if lane.reap(client).is_some() {
                retire_backend(
                    lane,
                    router,
                    backend,
                    Status::unavailable("downstream write failed"),
                );
            }
        }

        // Input. A read that ended the transport retires it; the gateway's
        // ends the serving.
        match gateway.take_input() {
            Some(Err(e)) => return Err(Halt::Error(Error::Io(e))),
            Some(Ok(0)) => return Ok(()),
            Some(Ok(n)) => {
                if let Err(error) = gateway.with_input(n, |bytes| router.recv(bytes)) {
                    // The reply of any operation has been taken above. One still
                    // outstanding (queued, pending or partly performed) leaves
                    // its bytes unknown, so it is not written on.
                    let goaway = gateway.op.is_none();
                    return Err(Halt::Protocol { error, goaway });
                }
            }
            None => {}
        }
        for step in 0..count {
            let backend = (turn + step) % count;
            let lane = &mut lanes[backend];
            let Some(read) = lane.take_input() else {
                continue;
            };
            let delivered = match read {
                Ok(0) => Err("downstream closed"),
                Ok(n) => lane
                    .with_input(n, |bytes| router.backend_recv(backend, bytes))
                    .map_err(|_| "downstream protocol error"),
                Err(_) => Err("downstream read failed"),
            };
            if let Err(reason) = delivered {
                retire_backend(lane, router, backend, Status::unavailable(reason));
            }
        }

        // Output: start what each idle transport needs.
        gateway.issue(router);
        for step in 0..count {
            let backend = (turn + step) % count;
            if let Some(client) = router.backend(backend) {
                lanes[backend].issue(&*client);
            }
        }
        turn = (turn + 1) % count.max(1);

        // Closed, with everything delivered: the gateway is finished. Its
        // peer's hang-up or any downstream's output does not matter.
        if router.is_closed() && gateway.op.is_none() && !gateway.unflushed && !router.has_output()
        {
            return Ok(());
        }

        // Wait until a worker has a result, a transport can take output, or a
        // deadline is due.
        gateway.arm(router);
        for (backend, lane) in lanes.iter_mut().enumerate() {
            if let Some(client) = router.backend(backend) {
                lane.arm(&*client);
            }
        }
        let wake = earliest(
            core::iter::once(router.next_deadline())
                .chain(core::iter::once(gateway.deadline()))
                .chain(lanes.iter().map(Lane::deadline)),
        );
        let mut sleep = pin!(wake.map(|d| timer.sleep_until(d)));
        let due = || wake.is_some_and(|d| timer.now() >= d);
        poll_fn(|cx| {
            // A due deadline returns to the loop before anything is polled. The
            // loop ticks the router with the clock, so a handler is never polled
            // with a clock that has passed its call's deadline.
            if due() {
                return Poll::Ready(());
            }
            if let Some(sleep) = sleep.as_mut().as_pin_mut()
                && sleep.poll(cx).is_ready()
            {
                return Poll::Ready(());
            }
            router.poll(cx);
            // Handlers run synchronously, and may have moved the clock on.
            if due() {
                return Poll::Ready(());
            }
            gateway.poll_worker(cx);
            for lane in lanes.iter_mut() {
                lane.poll_worker(cx);
            }
            let output = gateway.wants_issue(router)
                || (0..count).any(|backend| {
                    router
                        .backend(backend)
                        .is_some_and(|client| lanes[backend].wants_issue(&*client))
                });
            let event = gateway.has_event() || lanes.iter().any(Lane::has_event);
            if output || event {
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await;
    }
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
pub fn client<IO: Read + Write>(io: IO) -> ClientBuilder<IO> {
    ClientBuilder {
        io,
        config: ClientConfig::default(),
        timer: DeadlinesUnspecified,
    }
}

/// Factory of an async [`Client`], created by [`client`].
///
/// `T` is the deadline policy: [`DeadlinesUnspecified`] until one is chosen,
/// then the [`Timer`] (or [`NoTimer`]) the client will use.
#[derive(Debug)]
pub struct ClientBuilder<IO, T = DeadlinesUnspecified> {
    io: IO,
    config: ClientConfig,
    timer: T,
}

impl<IO, T> ClientBuilder<IO, T> {
    /// Enforce call timeouts locally with `timer`, see
    /// [Deadlines](Client#deadlines). The transport's `read` must then be
    /// cancel-safe even with a single call.
    pub fn with_deadline_timer<U: Timer>(self, timer: U) -> ClientBuilder<IO, U> {
        ClientBuilder {
            io: self.io,
            config: self.config,
            timer,
        }
    }

    /// Do not enforce timeouts locally. They are still sent as
    /// `grpc-timeout`, so a server with a timer enforces them.
    pub fn without_deadlines(self) -> ClientBuilder<IO, NoTimer> {
        self.with_deadline_timer(NoTimer)
    }

    client_config_methods!();
}

impl<IO: Read + Write, T: Timer> ClientBuilder<IO, T> {
    /// Construct the client. Performs no I/O: the HTTP/2 preface is sent with
    /// the first call.
    pub fn build(self) -> Client<IO, T> {
        Client::from_parts(self.io, self.config, self.timer)
    }
}

impl<IO: Read + Write, T: Timer> Client<IO, T> {
    fn from_parts(io: IO, config: ClientConfig, timer: T) -> Self {
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
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
    use core::task::Context;

    use embedded_io_async::ErrorType;

    use super::*;
    use crate::grpc::{CallContext, Dispatcher, MethodDescriptor, MethodKind, Route};

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

    fn loopback_client(peer: &Rc<RefCell<Peer>>) -> Client<Loopback> {
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
        assert_eq!(peer.borrow().reads_dropped, 0, "nobody had to yield");
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
        assert_eq!(peer.borrow().reads_dropped, 1);
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
}
