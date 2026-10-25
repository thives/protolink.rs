//! The coordinator of the async server and router drivers (crate-private).
//!
//! One future drives a protocol [`Node`] and the transports that serve it: a
//! gateway connection, and for a router, one connection per downstream. The
//! coordinator alone runs the protocol and the clock. Each transport has a
//! [`Lane`]: its worker (see [`crate::driver`]) and the operation it has in
//! flight. A pass does, in order:
//!
//! 1. reads the clock once, ticks the node, and retires connections whose
//!    output is past its deadline or that have already failed;
//! 2. reaps at most one write or flush completion per lane;
//! 3. takes at most one read outcome per lane and feeds it to the node;
//! 4. retires any connection that a step above left failed;
//! 5. starts one write, or the flush that is due, on each idle lane;
//! 6. polls the workers and the node with the coordinator's waker, and waits
//!    for a result, a due deadline or work that is ready now.
//!
//! Handlers and protocol state are only touched by the coordinator, never by a
//! worker. A pass that found work ready runs again, a bounded number of times
//! before it yields to the executor, and an idle connection never wakes itself.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use core::time::Duration;

use crate::Error;
use crate::driver::{Port, Reply};
use crate::grpc::{self, Handler, Router, Status, http2};
use crate::shared::Shared;
use crate::timer::Timer;

/// Ready passes the coordinator runs before it yields to the executor.
const READY_PASSES: usize = 16;

/// Output of one connection: the gateway's protocol or a downstream client.
pub(crate) trait Outbound {
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

impl Outbound for grpc::Server {
    fn pending_output(&self) -> &[u8] {
        grpc::Server::pending_output(self)
    }

    fn consume_output(&mut self, n: usize) {
        grpc::Server::consume_output(self, n);
    }

    fn has_output(&self) -> bool {
        grpc::Server::has_output(self)
    }

    fn output_deadline(&self) -> Option<Duration> {
        grpc::Server::next_deadline(self)
    }

    fn output_flushed(&mut self) {
        grpc::Server::output_flushed(self);
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

/// What went wrong with one transport operation.
pub(crate) enum Failure<E> {
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

/// The obligation to deliver what was written to a transport: it outlives the
/// operations that carry it, and it is what a lease hands on to the next owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Delivery {
    /// Bytes were written since the transport was last flushed.
    pub(crate) unflushed: bool,
    /// The earliest deadline of the output that is not yet acknowledged by a
    /// flush. A completed write does not clear it, and nor does a reset of the
    /// call that produced the output: that output still has to be delivered.
    /// It is cleared only by a successful flush that leaves the queue empty.
    pub(crate) deadline: Option<Duration>,
}

/// The coordinator's side of one transport: the port its worker reports
/// through, the worker, the operation in flight, and the delivery obligation.
pub(crate) struct Lane<'p, F, E> {
    port: &'p Shared<Port<E>>,
    /// `None` once the transport has been retired. Its worker is dropped then,
    /// so nothing is written to it again.
    worker: Option<Pin<Box<F>>>,
    op: Option<Op>,
    delivery: Delivery,
}

impl<'p, F: Future<Output = ()>, E> Lane<'p, F, E> {
    /// A lane whose transport is served by `worker` through `port`.
    pub(crate) fn new(port: &'p Shared<Port<E>>, worker: F) -> Self {
        Self {
            port,
            worker: Some(Box::pin(worker)),
            op: None,
            delivery: Delivery::default(),
        }
    }

    pub(crate) fn live(&self) -> bool {
        self.worker.is_some()
    }

    /// Poll the worker, which never completes: polling it lets it make progress.
    pub(crate) fn poll_worker(&mut self, cx: &mut Context<'_>) {
        if let Some(worker) = self.worker.as_mut() {
            let _ = worker.as_mut().poll(cx);
        }
    }

    /// Whether a reply or input is waiting to be taken.
    pub(crate) fn has_event(&self) -> bool {
        self.live() && self.port.with(|p| p.has_event())
    }

    /// Whether the reply of the operation in flight is waiting to be taken.
    pub(crate) fn has_reply(&self) -> bool {
        self.live() && self.port.with(|p| p.has_reply())
    }

    /// Whether [`issue`](Self::issue) would start an operation.
    pub(crate) fn wants_issue<O: Outbound>(&self, out: &O) -> bool {
        self.live() && self.op.is_none() && (out.has_output() || self.delivery.unflushed)
    }

    /// Start a write of the output, or a flush once the output is all written.
    /// The bytes are copied now; the core keeps them until the reply says how
    /// many were accepted.
    pub(crate) fn issue<O: Outbound>(&mut self, out: &O) {
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
    pub(crate) fn reap<O: Outbound>(&mut self, out: &mut O) -> Option<Failure<E>> {
        let op = self.op?;
        let reply = self.port.with(|p| p.take_reply())?;
        self.op = None;
        match reply {
            Reply::Written(Ok(n)) => match n.min(op.len) {
                0 => Some(Failure::WriteZero),
                n => {
                    out.consume_output(n);
                    // The write's deadline is part of the obligation now, so a
                    // short write leaves it armed for the rest of the output.
                    self.delivery.unflushed = true;
                    self.delivery.deadline = earliest([self.delivery.deadline, op.deadline]);
                    None
                }
            },
            Reply::Flushed(Ok(())) => {
                out.output_flushed();
                self.delivery.unflushed = false;
                // Output that arrived during the flush is not acknowledged by it,
                // so the obligation stays until a flush leaves the queue empty.
                if !out.has_output() {
                    self.delivery.deadline = None;
                }
                None
            }
            Reply::Written(Err(e)) | Reply::Flushed(Err(e)) => Some(Failure::Io(e)),
        }
    }

    /// Take the outcome of the last read, if it has come back. For `Ok(n)` the
    /// bytes are in the port's inbox, until the next read starts.
    pub(crate) fn take_input(&mut self) -> Option<Result<usize, E>> {
        self.port.with(|p| p.take_read())
    }

    /// Run `f` on the bytes of the last read, `n` of them.
    pub(crate) fn with_input<R>(&self, n: usize, f: impl FnOnce(&[u8]) -> R) -> R {
        self.port.with(|p| f(&p.inbox()[..n]))
    }

    /// Keep the obligations no later than the connection's current deadlines:
    /// those of the operation in flight, and of output not yet flushed.
    pub(crate) fn arm<O: Outbound>(&mut self, out: &O) {
        if let Some(op) = self.op.as_mut() {
            op.deadline = earliest([op.deadline, out.output_deadline()]);
        }
        if self.delivery.unflushed {
            self.delivery.deadline = earliest([self.delivery.deadline, out.output_deadline()]);
        }
    }

    /// The earliest deadline of the operation in flight and of the output not
    /// yet delivered. It holds between operations too.
    pub(crate) fn deadline(&self) -> Option<Duration> {
        earliest([self.op.and_then(|op| op.deadline), self.delivery.deadline])
    }

    /// Whether the earliest obligation has passed its deadline.
    pub(crate) fn overdue(&self, now: Duration) -> bool {
        self.deadline().is_some_and(|d| now >= d)
    }

    /// Whether the operation in flight is a flush rather than a write.
    pub(crate) fn flushing(&self) -> bool {
        self.op.is_some_and(|op| op.len == 0)
    }

    /// Whether an operation is in flight: it must be reaped before the lane
    /// may be dropped, since its outcome is then unknown.
    pub(crate) fn busy(&self) -> bool {
        self.op.is_some()
    }

    /// The delivery obligation, for a lease to hand on to the next owner.
    pub(crate) fn delivery(&self) -> Delivery {
        self.delivery
    }

    /// Restore the delivery obligation of a previous lease of the same transport.
    pub(crate) fn set_delivery(&mut self, delivery: Delivery) {
        self.delivery = delivery;
    }

    /// Whether bytes were written since the transport was last flushed.
    pub(crate) fn unflushed(&self) -> bool {
        self.delivery.unflushed
    }

    /// Drop the worker, abandoning any operation it has in flight. Nothing is
    /// delivered after this, so the obligation is dropped too.
    pub(crate) fn retire(&mut self) {
        self.worker = None;
        self.op = None;
        self.delivery = Delivery::default();
    }
}

/// The earliest of the deadlines that are set.
pub(crate) fn earliest(deadlines: impl IntoIterator<Item = Option<Duration>>) -> Option<Duration> {
    deadlines.into_iter().flatten().min()
}

/// Why [`drive`] stopped with an error.
pub(crate) enum Halt<E> {
    /// Reported to the caller as it is.
    Error(Error<E>),
    /// A connection failed on the protocol side. `goaway` says whether the
    /// gateway's output is in a state that allows a GOAWAY to be written.
    Protocol { error: http2::Error, goaway: bool },
}

/// The protocol side of a coordinated connection: a gateway connection, and the
/// downstream connections of a router. The coordinator drives it; it never
/// touches a transport.
pub(crate) trait Node {
    /// The gateway's output.
    type Gateway: Outbound;
    /// A downstream's output.
    type Downstream: Outbound;

    fn gateway(&mut self) -> &mut Self::Gateway;
    /// Report the time to the node, and end what has come due.
    fn gateway_tick(&mut self, now: Duration);
    fn gateway_recv(&mut self, bytes: &[u8]) -> Result<(), http2::Error>;
    fn gateway_poll(&mut self, cx: &mut Context<'_>);
    /// The gateway's terminal failure, if it has one.
    fn gateway_failure(&self) -> Option<http2::Error>;
    /// Whether the gateway is finished: shut down, with nothing left to do.
    fn is_closed(&self) -> bool;
    /// The earliest deadline the node needs to be woken for.
    fn next_deadline(&self) -> Option<Duration>;

    fn downstream_count(&self) -> usize;
    fn downstream(&mut self, index: usize) -> Option<&mut Self::Downstream>;
    fn downstream_recv(&mut self, index: usize, bytes: &[u8]) -> Result<(), http2::Error>;
    fn downstream_failed(&mut self, index: usize, status: Status);
    /// The terminal failure of downstream `index`'s own connection, if it has one.
    fn downstream_failure(&self, index: usize) -> Option<http2::Error>;
}

/// The node of a server: its protocol, and its handler for the calls.
pub(crate) struct ServerNode<'a, H: Handler + ?Sized> {
    pub(crate) server: &'a mut grpc::Server,
    pub(crate) handler: &'a mut H,
}

impl<H: Handler + ?Sized> Node for ServerNode<'_, H> {
    type Gateway = grpc::Server;
    type Downstream = grpc::Client;

    fn gateway(&mut self) -> &mut grpc::Server {
        self.server
    }

    fn gateway_tick(&mut self, now: Duration) {
        self.server.tick(now, &mut *self.handler);
    }

    fn gateway_recv(&mut self, bytes: &[u8]) -> Result<(), http2::Error> {
        self.server.recv(bytes, &mut *self.handler)
    }

    fn gateway_poll(&mut self, cx: &mut Context<'_>) {
        self.server.poll(&mut *self.handler, cx);
    }

    fn gateway_failure(&self) -> Option<http2::Error> {
        self.server.failure().cloned()
    }

    fn is_closed(&self) -> bool {
        self.server.is_closed()
    }

    fn next_deadline(&self) -> Option<Duration> {
        self.server.next_deadline()
    }

    fn downstream_count(&self) -> usize {
        0
    }

    fn downstream(&mut self, _: usize) -> Option<&mut grpc::Client> {
        None
    }

    fn downstream_recv(&mut self, _: usize, _: &[u8]) -> Result<(), http2::Error> {
        Ok(())
    }

    fn downstream_failed(&mut self, _: usize, _: Status) {}

    fn downstream_failure(&self, _: usize) -> Option<http2::Error> {
        None
    }
}

/// The node of a router: its gateway, and its downstream connections.
pub(crate) struct RouterNode<'a> {
    pub(crate) router: &'a mut Router,
}

impl Node for RouterNode<'_> {
    type Gateway = Router;
    type Downstream = grpc::Client;

    fn gateway(&mut self) -> &mut Router {
        self.router
    }

    fn gateway_tick(&mut self, now: Duration) {
        self.router.tick(now);
    }

    fn gateway_recv(&mut self, bytes: &[u8]) -> Result<(), http2::Error> {
        self.router.recv(bytes)
    }

    fn gateway_poll(&mut self, cx: &mut Context<'_>) {
        self.router.poll(cx);
    }

    fn gateway_failure(&self) -> Option<http2::Error> {
        self.router.gateway_failure().cloned()
    }

    fn is_closed(&self) -> bool {
        self.router.is_closed()
    }

    fn next_deadline(&self) -> Option<Duration> {
        self.router.next_deadline()
    }

    fn downstream_count(&self) -> usize {
        self.router.backend_count()
    }

    fn downstream(&mut self, index: usize) -> Option<&mut grpc::Client> {
        self.router.backend(index)
    }

    fn downstream_recv(&mut self, index: usize, bytes: &[u8]) -> Result<(), http2::Error> {
        self.router.backend_recv(index, bytes)
    }

    fn downstream_failed(&mut self, index: usize, status: Status) {
        self.router.backend_failed(index, status);
    }

    fn downstream_failure(&self, index: usize) -> Option<http2::Error> {
        self.router.backend_failure(index).cloned()
    }
}

/// The status of calls on a downstream whose own connection failed.
pub(crate) fn downstream_failure_status(error: &http2::Error) -> Status {
    match error {
        http2::Error::OutputLimitExceeded { .. } => grpc::lifecycle::failure_status(error),
        _ => Status::unavailable("downstream protocol error"),
    }
}

/// Retire downstream `index`: its transport can no longer be used, and its
/// calls end with `status`. Only that downstream is affected.
fn retire_downstream<N: Node, F: Future<Output = ()>, E>(
    lane: &mut Lane<'_, F, E>,
    node: &mut N,
    index: usize,
    status: Status,
) {
    lane.retire();
    node.downstream_failed(index, status);
}

/// Drive a node and its transports until the connection ends.
///
/// `lanes` are the downstream transports, one per downstream of the node, in
/// order. The gateway's lane is `gateway`. The future completes when the
/// gateway's input ends, or when the node is closed and everything it wrote has
/// been delivered; errors are returned as [`Halt`].
pub(crate) async fn drive<N, T, E, F, G>(
    node: &mut N,
    gateway: &mut Lane<'_, F, E>,
    lanes: &mut [Lane<'_, G, E>],
    timer: &T,
) -> Result<(), Halt<E>>
where
    N: Node,
    T: Timer,
    F: Future<Output = ()>,
    G: Future<Output = ()>,
{
    let count = lanes.len();
    debug_assert_eq!(count, node.downstream_count());
    let mut turn = 0;
    let mut budget = READY_PASSES;
    loop {
        // One reading of the clock decides the whole pass, and the node is ticked
        // with that same reading. Output in flight keeps the deadline it was
        // issued with, even when the tick ends the call that caused it.
        let now = timer.now();
        gateway.arm(node.gateway());
        for (index, lane) in lanes.iter_mut().enumerate() {
            if let Some(client) = node.downstream(index) {
                lane.arm(client);
            }
        }
        // Calls that are due end first, with their own status, so that retiring
        // the output they were waiting on does not replace it.
        node.gateway_tick(now);
        if let Some(error) = node.gateway_failure() {
            return Err(Halt::Protocol {
                error,
                goaway: false,
            });
        }
        // Output in flight past its deadline retires its connection. For a
        // downstream only that downstream is failed.
        if gateway.overdue(now) {
            return Err(Halt::Error(Error::OutputDeadline));
        }
        for step in 0..count {
            let index = (turn + step) % count;
            if lanes[index].overdue(now) {
                retire_downstream(
                    &mut lanes[index],
                    node,
                    index,
                    Status::unavailable("downstream output deadline exceeded"),
                );
            }
        }

        // At most one reply per lane.
        if let Some(failure) = gateway.reap(node.gateway()) {
            return Err(Halt::Error(match failure {
                Failure::Io(e) => Error::Io(e),
                Failure::WriteZero => Error::WriteZero,
            }));
        }
        for step in 0..count {
            let index = (turn + step) % count;
            let lane = &mut lanes[index];
            if !lane.live() {
                continue;
            }
            let Some(client) = node.downstream(index) else {
                continue;
            };
            if lane.reap(client).is_some() {
                retire_downstream(
                    lane,
                    node,
                    index,
                    Status::unavailable("downstream write failed"),
                );
            }
        }

        // At most one read outcome per lane. A gateway read that ended the
        // transport ends the connection; a downstream's ends only itself.
        match gateway.take_input() {
            Some(Err(e)) => return Err(Halt::Error(Error::Io(e))),
            Some(Ok(0)) => return Ok(()),
            Some(Ok(n)) => {
                if let Err(error) = gateway.with_input(n, |bytes| node.gateway_recv(bytes)) {
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
            let index = (turn + step) % count;
            let lane = &mut lanes[index];
            let Some(read) = lane.take_input() else {
                continue;
            };
            let delivered = match read {
                Ok(0) => Err(Status::unavailable("downstream closed")),
                Ok(n) => lane
                    .with_input(n, |bytes| node.downstream_recv(index, bytes))
                    .map_err(|e| downstream_failure_status(&e)),
                Err(_) => Err(Status::unavailable("downstream read failed")),
            };
            if let Err(status) = delivered {
                retire_downstream(lane, node, index, status);
            }
        }

        // Failures latched by this pass's work, whether by the node's own calls,
        // relayed responses or the tick above, are adopted before any output is
        // started. A gateway failure ends the connection; a downstream's own
        // failure retires only that downstream.
        if let Some(error) = node.gateway_failure() {
            return Err(Halt::Protocol {
                error,
                goaway: false,
            });
        }
        for (index, lane) in lanes.iter_mut().enumerate() {
            if !lane.live() {
                continue;
            }
            if let Some(error) = node.downstream_failure(index) {
                retire_downstream(lane, node, index, downstream_failure_status(&error));
                // Retiring a downstream pumps the relays, which can queue gateway
                // output that does not fit: that failure is recognised before any
                // output is started.
                if let Some(error) = node.gateway_failure() {
                    return Err(Halt::Protocol {
                        error,
                        goaway: false,
                    });
                }
            }
        }

        // Handler and relay work is synchronous, and may have moved the clock on.
        // Calls that came due meanwhile are ended, and output that is due is
        // retired, before any output is started. Only a later reading can make
        // something due, so this does not spin on an unchanged clock.
        let fresh = timer.now();
        if fresh != now {
            node.gateway_tick(fresh);
            if let Some(error) = node.gateway_failure() {
                return Err(Halt::Protocol {
                    error,
                    goaway: false,
                });
            }
            if gateway.overdue(fresh) {
                return Err(Halt::Error(Error::OutputDeadline));
            }
            for (index, lane) in lanes.iter_mut().enumerate() {
                if lane.overdue(fresh) {
                    retire_downstream(
                        lane,
                        node,
                        index,
                        Status::unavailable("downstream output deadline exceeded"),
                    );
                }
            }
        }

        // Output: start what each idle transport needs.
        gateway.issue(node.gateway());
        for step in 0..count {
            let index = (turn + step) % count;
            if let Some(client) = node.downstream(index) {
                lanes[index].issue(client);
            }
        }
        turn = (turn + 1) % count.max(1);

        // Closed, with everything delivered: the gateway is finished. Its
        // peer's hang-up or any downstream's output does not matter.
        if node.is_closed()
            && gateway.op.is_none()
            && !gateway.delivery.unflushed
            && !node.gateway().has_output()
        {
            return Ok(());
        }

        // Wait until a worker has a result, a transport can take output, or a
        // deadline is due.
        gateway.arm(node.gateway());
        for (index, lane) in lanes.iter_mut().enumerate() {
            if let Some(client) = node.downstream(index) {
                lane.arm(client);
            }
        }
        let wake = earliest(
            core::iter::once(node.next_deadline())
                .chain(core::iter::once(gateway.deadline()))
                .chain(lanes.iter().map(Lane::deadline)),
        );
        let mut sleep = pin!(wake.map(|d| timer.sleep_until(d)));
        let due = || wake.is_some_and(|d| timer.now() >= d);
        poll_fn(|cx| {
            // A due deadline returns to the loop before anything is polled. The
            // loop ticks the node with the clock, so a handler is never polled
            // with a clock that has passed its call's deadline.
            if due() {
                return Poll::Ready(());
            }
            if let Some(sleep) = sleep.as_mut().as_pin_mut()
                && sleep.poll(cx).is_ready()
            {
                return Poll::Ready(());
            }
            node.gateway_poll(cx);
            // A failure that the node's work just latched ends the connection now,
            // before any worker is polled. It must not wait for transport I/O, which
            // may be blocked indefinitely. A retired downstream's failure was adopted
            // already, and stays set: it is not new work, so it must not end the pass.
            if node.gateway_failure().is_some()
                || (0..count)
                    .any(|index| lanes[index].live() && node.downstream_failure(index).is_some())
            {
                return Poll::Ready(());
            }
            // Handlers run synchronously, and may have moved the clock on.
            if due() {
                return Poll::Ready(());
            }
            gateway.poll_worker(cx);
            for lane in lanes.iter_mut() {
                lane.poll_worker(cx);
            }
            let output = gateway.wants_issue(node.gateway())
                || (0..count).any(|index| {
                    node.downstream(index)
                        .is_some_and(|client| lanes[index].wants_issue(client))
                });
            let event = gateway.has_event() || lanes.iter().any(Lane::has_event);
            if output || event {
                // Work is ready, so the next pass runs at once, until the budget
                // is spent. Then the executor gets a turn, and the pass runs
                // again when it is woken.
                if budget > 0 {
                    budget -= 1;
                    return Poll::Ready(());
                }
                budget = READY_PASSES;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            budget = READY_PASSES;
            Poll::Pending
        })
        .await;
    }
}

/// Write the gateway's pending output through `lane` as far as it goes, so that
/// a GOAWAY written just before it reaches the peer. Best effort: it stops at
/// the first failure or deadline. The caller must not use `lane` afterwards.
pub(crate) async fn deliver<F, E, O, T>(lane: &mut Lane<'_, F, E>, out: &mut O, timer: &T)
where
    F: Future<Output = ()>,
    O: Outbound,
    T: Timer,
{
    while out.has_output() || lane.delivery.unflushed {
        lane.arm(out);
        if lane.overdue(timer.now()) {
            return;
        }
        lane.issue(out);
        if lane.op.is_none() {
            return;
        }
        let mut sleep = pin!(lane.deadline().map(|d| timer.sleep_until(d)));
        poll_fn(|cx| {
            lane.poll_worker(cx);
            if lane.has_reply() {
                return Poll::Ready(());
            }
            if let Some(sleep) = sleep.as_mut().as_pin_mut()
                && sleep.poll(cx).is_ready()
            {
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await;
        if !lane.has_reply() || lane.reap(out).is_some() {
            return;
        }
    }
}

/// Ports for `count` transports, each its own.
pub(crate) fn ports<E>(count: usize) -> Vec<Shared<Port<E>>> {
    (0..count).map(|_| Shared::new(Port::new())).collect()
}

#[cfg(test)]
mod tests {
    use alloc::vec;
    use alloc::vec::Vec;
    use core::future::{Pending, pending};
    use core::time::Duration;

    use super::*;

    /// Output whose bytes and deadline the test sets, and that counts the
    /// flushes it has acknowledged.
    #[derive(Default)]
    struct FakeOut {
        bytes: Vec<u8>,
        deadline: Option<Duration>,
        flushed: usize,
    }

    impl Outbound for FakeOut {
        fn pending_output(&self) -> &[u8] {
            &self.bytes
        }

        fn consume_output(&mut self, n: usize) {
            self.bytes.drain(..n);
        }

        fn has_output(&self) -> bool {
            !self.bytes.is_empty()
        }

        fn output_deadline(&self) -> Option<Duration> {
            self.deadline
        }

        fn output_flushed(&mut self) {
            self.flushed += 1;
        }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// A lane whose worker is never polled: the test completes its operations.
    fn lane(port: &Shared<Port<()>>) -> Lane<'_, Pending<()>, ()> {
        Lane::new(port, pending())
    }

    /// The flush of a batch is acknowledged while output that arrived during it
    /// is still queued: the batch's deadline must survive until the flush that
    /// leaves the queue empty.
    #[test]
    fn flush_with_appended_output_keeps_delivery_obligation() {
        let port = Shared::new(Port::new());
        let mut lane = lane(&port);
        let mut out = FakeOut {
            bytes: vec![1; 10],
            deadline: Some(ms(100)),
            ..FakeOut::default()
        };

        // Batch A is written.
        lane.issue(&out);
        port.with(|p| p.complete(Reply::Written(Ok(10))));
        assert!(lane.reap(&mut out).is_none());
        assert_eq!(
            lane.delivery(),
            Delivery {
                unflushed: true,
                deadline: Some(ms(100)),
            }
        );

        // Its flush starts, and input appends B while it is in flight.
        out.deadline = Some(ms(200));
        lane.arm(&out);
        lane.issue(&out);
        out.bytes.push(2);
        port.with(|p| p.complete(Reply::Flushed(Ok(()))));
        assert!(lane.reap(&mut out).is_none());
        assert_eq!(out.flushed, 1);
        assert!(!lane.delivery().unflushed);
        assert_eq!(
            lane.delivery().deadline,
            Some(ms(100)),
            "B is not acknowledged, so A's obligation is still armed"
        );

        // B is written and flushed with an empty queue: that barrier retires it.
        lane.issue(&out);
        port.with(|p| p.complete(Reply::Written(Ok(1))));
        assert!(lane.reap(&mut out).is_none());
        lane.issue(&out);
        port.with(|p| p.complete(Reply::Flushed(Ok(()))));
        assert!(lane.reap(&mut out).is_none());
        assert_eq!(out.flushed, 2);
        assert_eq!(lane.delivery(), Delivery::default());
        assert!(!lane.overdue(ms(1_000)));
    }

    /// A flush whose reply has come back is reaped before the next write starts.
    /// Until then the lane holds its operation, even with output queued, so the
    /// reply is never mixed with a write that follows it.
    #[test]
    fn flush_reply_is_reaped_before_next_write() {
        let port = Shared::new(Port::new());
        let mut lane = lane(&port);
        let mut out = FakeOut {
            bytes: vec![1; 4],
            ..FakeOut::default()
        };
        lane.issue(&out);
        port.with(|p| p.complete(Reply::Written(Ok(4))));
        assert!(lane.reap(&mut out).is_none());

        // The flush starts, and output arrives while it is in flight.
        lane.issue(&out);
        out.bytes.push(2);
        port.with(|p| p.complete(Reply::Flushed(Ok(()))));
        assert!(lane.has_reply());
        assert!(
            !lane.wants_issue(&out),
            "a write may start before the flush reply is reaped"
        );

        assert!(lane.reap(&mut out).is_none());
        assert_eq!(out.flushed, 1);
        assert!(lane.wants_issue(&out));
    }

    /// Between operations the obligation is still a deadline: an overdue batch
    /// is seen without any operation in flight.
    #[test]
    fn written_output_stays_overdue_between_operations() {
        let port = Shared::new(Port::new());
        let mut lane = lane(&port);
        let mut out = FakeOut {
            bytes: vec![1; 4],
            deadline: Some(ms(50)),
            ..FakeOut::default()
        };
        lane.issue(&out);
        port.with(|p| p.complete(Reply::Written(Ok(4))));
        assert!(lane.reap(&mut out).is_none());
        // The call that produced the output is gone, and nothing is in flight.
        out.deadline = None;
        lane.arm(&out);
        assert!(!lane.busy());
        assert!(lane.overdue(ms(50)));
        assert!(!lane.overdue(ms(49)));
    }

    /// A transport retired with an obligation drops it: nothing is delivered
    /// after retirement, so it is not carried anywhere.
    #[test]
    fn retirement_clears_the_delivery_obligation() {
        let port = Shared::new(Port::new());
        let mut lane = lane(&port);
        lane.set_delivery(Delivery {
            unflushed: true,
            deadline: Some(ms(10)),
        });
        lane.retire();
        assert_eq!(lane.delivery(), Delivery::default());
        assert!(!lane.live());
    }
}
