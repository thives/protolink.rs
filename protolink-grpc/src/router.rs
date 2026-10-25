//! Serves the calls of one connection from local services and from downstream
//! connections.
//!
//! A [`Router`] is the composition layer above [`Dispatcher`]s. Local services
//! are added with [`with_service`](RouterBuilder::with_service), as many as
//! needed; forwarded methods are added with
//! [`with_route`](RouterBuilder::with_route), giving the methods' descriptors
//! and the downstream [`Client`] that serves them. Both contribute to one
//! exact-path table: a path served twice is a [`RouteConflict`] at build time,
//! whatever the order of registration. A path no route serves is answered
//! `UNIMPLEMENTED`.
//!
//! A local call is driven by its service exactly as a [`Server`](crate::Server)
//! would drive it. A forwarded call is relayed to the downstream it was routed
//! to when its headers arrived, and stays with that downstream until it ends,
//! see [`Relay`]. The router owns no I/O: the driver writes and reads the
//! gateway's bytes with [`pending_output`](Router::pending_output) and
//! [`recv`](Router::recv), and each downstream's with [`backend`](Router::backend).
//!
//! ```ignore
//! let mut router = Router::builder()
//!     .with_service(MultiDispatch::builder(&mut app)
//!         .with_service(DeviceMethods)
//!         .build()?)
//!     .with_route(remote::METHODS, Client::new(ClientConfig::default()))
//!     .build()?;
//! ```

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;
use core::task::{Context, Poll};
use core::time::Duration;

use protolink_http2::Error;

use crate::client::Client;
use crate::dispatch::{Dispatcher, RouteConflict, RouteTable};
use crate::lifecycle::{CallEvent, Lifecycle};
use crate::local::LocalCalls;
use crate::relay::Relay;
use crate::{
    CallContext, Handler, MethodDescriptor, Next, ResponseMetadata, Route, ServerConfig, Status,
};

/// Passes over the relays in one pump before control returns to the driver.
/// Each pass moves whatever it can, so this only bounds a long chain of
/// dependent steps; the driver pumps again when it next wakes.
const RELAY_PASSES: usize = 64;

/// Routes the calls of one connection between local services and forwarded
/// routes, through one exact-path table.
pub struct Router {
    lifecycle: Lifecycle<Route>,
    /// Local calls, driven by [`services`](Self::services).
    locals: LocalCalls,
    services: LocalSet,
    /// Forwarded calls, in the order they started.
    forwards: Vec<Forward>,
    /// Downstream connections, indexed by backend number.
    backends: Vec<Client>,
    /// Downstreams whose transport failed. Calls are not routed to them.
    failed: Vec<bool>,
    /// The forwarded routes, indexed by pool number.
    pools: Vec<Pool>,
}

/// The downstreams that serve one forwarded route, chosen from in turn.
#[derive(Debug)]
struct Pool {
    /// Backend numbers of the members.
    members: Vec<usize>,
    /// Member to try first for the next call.
    next: usize,
    /// Calls that may be forwarded at once.
    max_active_calls: usize,
    /// Calls forwarded and not finished.
    active: usize,
}

impl fmt::Debug for Router {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Router")
            .field("local_calls", &self.locals.len())
            .field("forwarded_calls", &self.forwards.len())
            .field("backends", &self.backends.len())
            .finish_non_exhaustive()
    }
}

/// A forwarded call, the downstream it was routed to, and its pool.
#[derive(Debug)]
struct Forward {
    relay: Relay,
    backend: usize,
    pool: usize,
}

/// Where a route's method is served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dest {
    /// Local service number.
    Local(usize),
    /// Pool number.
    Pool(usize),
}

/// The local services of a router, as one handler: each call is delegated to
/// the service that declared its method.
struct LocalSet {
    services: Vec<Box<dyn Dispatcher + Send>>,
    /// Every route of the router. Members `0..services.len()` are the local
    /// services, and member `services.len() + p` is pool `p`.
    table: RouteTable,
}

impl LocalSet {
    fn dest(&self, route: Route) -> Option<Dest> {
        let (member, _) = self.table.target(route)?;
        Some(match member.checked_sub(self.services.len()) {
            Some(pool) => Dest::Pool(pool),
            None => Dest::Local(member),
        })
    }

    /// Deliver an event of a local call to the service that serves it, with
    /// that service's route.
    fn delegate<R>(
        &mut self,
        ctx: &mut CallContext<'_>,
        f: impl FnOnce(&mut dyn Dispatcher, &mut CallContext<'_>) -> R,
    ) -> Option<R> {
        let (member, route) = self.table.target(ctx.route)?;
        let service = self.services.get_mut(member)?;
        Some(ctx.with_route(route, |ctx| f(&mut **service, ctx)))
    }
}

impl Handler for LocalSet {
    fn route(&self, path: &str) -> Option<Route> {
        self.table.route(path)
    }

    fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        self.delegate(ctx, |service, ctx| service.call(ctx, request))
            .unwrap_or_else(|| Err(crate::handler::unimplemented(ctx.path)))
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        self.delegate(ctx, |service, ctx| service.on_message(ctx, message))
            .unwrap_or_else(|| Err(crate::handler::unimplemented(ctx.path)))
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        self.delegate(ctx, |service, ctx| service.on_half_close(ctx))
            .unwrap_or(Ok(()))
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        self.delegate(ctx, |service, ctx| service.poll_response(ctx, cx))
            .unwrap_or_else(|| {
                Poll::Ready(Next::Done(Err(crate::handler::unimplemented(ctx.path))))
            })
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.delegate(ctx, |service, ctx| service.on_cancel(ctx));
    }
}

/// Factory of a [`Router`], created by [`Router::builder`].
pub struct RouterBuilder {
    config: ServerConfig,
    services: Vec<Box<dyn Dispatcher + Send>>,
    routes: Vec<(&'static [MethodDescriptor], Vec<Client>, usize)>,
}

impl fmt::Debug for RouterBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterBuilder")
            .field("services", &self.services.len())
            .field("routes", &self.routes.len())
            .finish_non_exhaustive()
    }
}

impl Router {
    /// Start building a router with the default [`ServerConfig`].
    pub fn builder() -> RouterBuilder {
        RouterBuilder {
            config: ServerConfig::default(),
            services: Vec::new(),
            routes: Vec::new(),
        }
    }

    /// Process received bytes from the gateway's transport, starting new calls
    /// and delivering request messages to local services. Forwarded calls are
    /// relayed as far as they can go.
    ///
    /// On `Err` the gateway connection is unusable and every call on it has
    /// been cancelled; write the pending output and close the transport.
    pub fn recv(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let table = &self.services.table;
        let result = self.lifecycle.recv(bytes, |path| table.route(path));
        self.flush();
        self.drive_locals(None);
        self.pump_relays();
        // Local and relayed work can latch the gateway's failure after the input
        // itself was accepted: that failure is this call's result too.
        match result {
            Err(error) => Err(error),
            Ok(()) => self
                .lifecycle
                .failure()
                .map_or(Ok(()), |error| Err(error.clone())),
        }
    }

    /// Process received bytes from downstream `backend`, relaying the responses
    /// of its calls. On `Err` the downstream connection is unusable and its
    /// calls have failed with `UNAVAILABLE`.
    ///
    /// # Panics
    ///
    /// If `backend` is not a downstream of this router.
    pub fn backend_recv(
        &mut self,
        backend: usize,
        bytes: &[u8],
    ) -> Result<(), protolink_http2::Error> {
        let result = self.backends[backend].recv(bytes);
        self.pump_relays();
        result
    }

    /// The downstream connection with number `backend`, for the driver to move
    /// its bytes. `None` if there is no such downstream. Bytes received from it
    /// go through [`backend_recv`](Self::backend_recv).
    pub fn backend(&mut self, backend: usize) -> Option<&mut Client> {
        self.backends.get_mut(backend)
    }

    /// Number of downstream connections.
    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    /// Record that downstream `backend` can no longer be used: its transport
    /// failed. Its calls end with `status`, and new calls are routed to the
    /// other members of their pool, if any.
    ///
    /// # Panics
    ///
    /// If `backend` is not a downstream of this router.
    pub fn backend_failed(&mut self, backend: usize, status: Status) {
        self.failed[backend] = true;
        self.backends[backend].fail_all(status);
        self.pump_relays();
    }

    /// Whether downstream `backend` was marked failed by
    /// [`backend_failed`](Self::backend_failed).
    pub fn backend_is_failed(&self, backend: usize) -> bool {
        self.failed.get(backend).copied().unwrap_or(true)
    }

    /// Drive every local call that can progress: deliver request messages,
    /// pull responses while the peer can take them, and relay what is waiting.
    /// Local handlers that return `Poll::Pending` wake `cx`.
    pub fn poll(&mut self, cx: &mut Context<'_>) {
        self.drive_locals(Some(cx));
        self.pump_relays();
    }

    /// Report the current time to the gateway and to every downstream, ending
    /// the calls whose deadline has passed. Calls ended by the gateway's
    /// deadline are finished on the next [`recv`](Self::recv), [`poll`](Self::poll)
    /// or this call.
    pub fn tick(&mut self, now: Duration) {
        self.lifecycle.tick(now);
        for backend in &mut self.backends {
            backend.tick(now);
        }
        self.flush();
        self.pump_relays();
    }

    /// The gateway's current time, as last reported to [`tick`](Self::tick).
    pub fn now(&self) -> Duration {
        self.lifecycle.now()
    }

    /// Earliest deadline of any call, gateway or downstream, on the clock given
    /// to [`tick`](Self::tick).
    pub fn next_deadline(&self) -> Option<Duration> {
        self.backends
            .iter()
            .filter_map(Client::next_deadline)
            .chain(self.lifecycle.next_deadline())
            .min()
    }

    /// Earliest deadline of the gateway's own calls and of its output awaiting
    /// delivery, on the clock given to [`tick`](Self::tick). Downstream deadlines
    /// are left out: output blocked past this one retires the gateway connection,
    /// and a slow downstream must not do that.
    pub fn gateway_next_deadline(&self) -> Option<Duration> {
        self.lifecycle.next_deadline()
    }

    /// Bytes that must be written to the gateway's transport.
    pub fn pending_output(&self) -> &[u8] {
        self.lifecycle.pending_output()
    }

    /// Mark `n` bytes of [`pending_output`](Self::pending_output) as written.
    pub fn consume_output(&mut self, n: usize) {
        self.lifecycle.consume_output(n);
    }

    /// Whether gateway output is waiting to be written.
    pub fn has_output(&self) -> bool {
        self.lifecycle.has_output()
    }

    /// Take all gateway output. See [`Server::take_output`](crate::Server::take_output).
    pub fn take_output(&mut self) -> Vec<u8> {
        self.lifecycle.take_output()
    }

    /// Acknowledge successful delivery of the gateway output.
    pub fn output_flushed(&mut self) {
        self.lifecycle.output_flushed();
    }

    /// The gateway connection failed or finished shutting down.
    pub fn is_closed(&self) -> bool {
        self.lifecycle.is_closed()
    }

    /// The gateway connection's terminal failure, if it has one. It can be set
    /// by a gateway call, or by relayed work done for a downstream response.
    pub fn gateway_failure(&self) -> Option<&Error> {
        self.lifecycle.failure()
    }

    /// The terminal failure of downstream `backend`'s own connection, if it has
    /// one. Only that downstream is affected: its calls have ended, and the
    /// driver retires its transport. `None` for an unknown backend.
    pub fn backend_failure(&self, backend: usize) -> Option<&Error> {
        self.backends.get(backend)?.failure()
    }

    /// Start a graceful shutdown of the gateway connection (GOAWAY).
    pub fn shutdown(&mut self) {
        self.lifecycle.shutdown();
    }

    /// Cancel every call on the gateway connection, e.g. because its transport
    /// closed. Forwarded calls cancel their downstream calls on the next pump.
    pub fn cancel_all(&mut self) {
        self.lifecycle.cancel_all();
        self.flush();
        self.pump_relays();
    }

    /// Number of calls in progress, local and forwarded.
    pub fn active_calls(&self) -> usize {
        self.locals.len() + self.forwards.len()
    }

    /// Drive every local call once, or until its quantum is spent when there
    /// is a context to wake.
    fn drive_locals(&mut self, mut cx: Option<&mut Context<'_>>) {
        let mut traversal = if cx.is_some() {
            self.locals.poll_traversal()
        } else {
            self.locals.receive_traversal()
        };
        while let Some(id) = self.locals.next_id(&mut traversal) {
            // A failed gateway runs no more local handlers; its calls were cancelled.
            if self.lifecycle.failure().is_some() {
                break;
            }
            self.locals.drive(
                &mut self.lifecycle,
                id,
                &mut self.services,
                cx.as_deref_mut(),
            );
            self.flush();
        }
    }

    /// Apply the lifecycle's events: calls start on the local service or relay
    /// their route, and local calls that end are reported to their service.
    /// Relayed calls need no event: their relays see the end themselves.
    fn flush(&mut self) {
        while let Some(event) = self.lifecycle.poll_event() {
            match event {
                CallEvent::Started {
                    id,
                    key,
                    path,
                    metadata,
                    deadline,
                } => match self.services.dest(key) {
                    Some(Dest::Pool(pool)) => match self.select(pool) {
                        Ok(backend) => {
                            self.pools[pool].active += 1;
                            self.forwards.push(Forward {
                                relay: Relay::new(id, path, key.kind, metadata),
                                backend,
                                pool,
                            });
                        }
                        Err(status) => {
                            self.lifecycle
                                .finish(id, Err(status), ResponseMetadata::default());
                        }
                    },
                    _ => self.locals.accept(id, key, path, metadata, deadline),
                },
                CallEvent::Expired { id } => {
                    if self.locals.contains(id) {
                        self.locals
                            .expire(&mut self.lifecycle, id, &mut self.services);
                    }
                }
                CallEvent::Cancelled { id } => self.locals.cancel(id, &mut self.services),
            }
        }
    }

    /// The member of `pool` that the next call goes to: the first usable one
    /// from the round-robin position.
    fn select(&mut self, pool: usize) -> Result<usize, Status> {
        let pool = &mut self.pools[pool];
        let members = pool.members.len();
        let usable = |backend: usize| !self.failed[backend] && !self.backends[backend].is_closed();
        let Some(step) =
            (0..members).find(|step| usable(pool.members[(pool.next + step) % members]))
        else {
            return Err(Status::unavailable("no downstream is available"));
        };
        if pool.active >= pool.max_active_calls {
            return Err(Status::resource_exhausted("too many calls for this method"));
        }
        let slot = (pool.next + step) % members;
        pool.next = (slot + 1) % members;
        Ok(pool.members[slot])
    }

    /// Move forwarded calls as far as they go, and drop the finished ones.
    fn pump_relays(&mut self) {
        for _ in 0..RELAY_PASSES {
            let mut progress = false;
            for forward in &mut self.forwards {
                let backend = &mut self.backends[forward.backend];
                progress |= forward.relay.pump(&mut self.lifecycle, backend);
            }
            let pools = &mut self.pools;
            self.forwards.retain(|forward| {
                let done = forward.relay.is_done();
                if done {
                    pools[forward.pool].active -= 1;
                }
                !done
            });
            if !progress {
                break;
            }
        }
        // A relayed response can fail the gateway, cancelling every call on it.
        // The local calls are told now, whatever the caller does next, since
        // the local-call loop does not run once the gateway has failed.
        self.flush();
    }
}

impl RouterBuilder {
    /// Use `config` for the gateway connection instead of the default.
    pub fn with_config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    /// Serve the methods of a local service (any [`Dispatcher`], such as a
    /// generated `<Service>Dispatch`, a [`Registry`](crate::Registry) or a
    /// [`MultiDispatch`](crate::MultiDispatch)).
    pub fn with_service<D: Dispatcher + Send + 'static>(mut self, service: D) -> Self {
        self.services.push(Box::new(service));
        self
    }

    /// Forward the methods described by `methods` to the downstream connection
    /// `backend`.
    pub fn with_route(self, methods: &'static [MethodDescriptor], backend: Client) -> Self {
        self.with_pool(methods, alloc::vec![backend], usize::MAX)
    }

    /// Forward the methods described by `methods` to a pool of interchangeable
    /// downstreams, each of which serves the same service. New calls go to the
    /// members in turn, skipping any whose transport failed or is closing. A
    /// call is failed `UNAVAILABLE` when no member is usable, and
    /// `RESOURCE_EXHAUSTED` when `max_active_calls` calls are already
    /// forwarded.
    ///
    /// Do not pool downstreams that expose the same service for different
    /// devices: their calls would be spread over all of them. Each call stays on
    /// the member it started on.
    ///
    /// # Panics
    ///
    /// If `members` is empty.
    pub fn with_pool(
        mut self,
        methods: &'static [MethodDescriptor],
        members: Vec<Client>,
        max_active_calls: usize,
    ) -> Self {
        assert!(!members.is_empty(), "a pool needs at least one downstream");
        self.routes.push((methods, members, max_active_calls));
        self
    }

    /// Construct the router, merging the methods of every service and route.
    ///
    /// Fails if two of them serve the same path.
    pub fn build(self) -> Result<Router, RouteConflict> {
        let locals = self.services.len();
        let mut table = RouteTable::default();
        for (index, service) in self.services.iter().enumerate() {
            table.add(index, service.methods());
        }
        let mut backends = Vec::new();
        let mut pools = Vec::with_capacity(self.routes.len());
        for (index, (methods, members, max_active_calls)) in self.routes.into_iter().enumerate() {
            table.add(locals + index, methods);
            let first = backends.len();
            pools.push(Pool {
                members: (first..first + members.len()).collect(),
                next: 0,
                max_active_calls,
                active: 0,
            });
            backends.extend(members);
        }
        Ok(Router {
            lifecycle: Lifecycle::new(self.config),
            locals: LocalCalls::default(),
            services: LocalSet {
                services: self.services,
                table: table.finish()?,
            },
            forwards: Vec::new(),
            failed: alloc::vec![false; backends.len()],
            backends,
            pools,
        })
    }
}
