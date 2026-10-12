//! Static method tables and the dispatchers composed from them.
//!
//! A generated `<Service>Dispatch<S>` serves one service implemented by `S`.
//! Several services are served on one connection either by
//!
//! - a [`Registry`] of independent dispatchers, each owning (or borrowing) its
//!   own application object, or
//! - a [`MultiDispatch`] holding one application object that implements
//!   several generated service traits, with the stateless generated
//!   `<Service>Methods` borrowing it for the duration of each dispatch.
//!
//! Both merge the services' [`MethodDescriptor`] tables when they are built
//! and reject duplicate paths with a [`RouteConflict`]. A route is resolved
//! once per call; every later event of the call goes to the same service.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;
use core::task::{Context, Poll};

use crate::handler::unimplemented;
use crate::{CallContext, Handler, MethodDescriptor, Next, Route, Status};

/// A [`Handler`] that serves a static table of methods, such as a generated
/// `<Service>Dispatch`, a [`Registry`] or a [`MultiDispatch`].
///
/// [`route`](Handler::route) must resolve exactly the paths of
/// [`methods`](Self::methods), each to its descriptor's
/// [`route`](MethodDescriptor::route): composite dispatchers use the table to
/// detect conflicts and to route without asking the member again.
pub trait Dispatcher: Handler {
    /// The methods served, with the routes they resolve to.
    fn methods(&self) -> &[MethodDescriptor];
}

impl<D: Dispatcher + ?Sized> Dispatcher for &mut D {
    fn methods(&self) -> &[MethodDescriptor] {
        (**self).methods()
    }
}

impl<D: Dispatcher + ?Sized> Dispatcher for Box<D> {
    fn methods(&self) -> &[MethodDescriptor] {
        (**self).methods()
    }
}

/// The methods of one generated service, applied to an implementation `S`
/// that is borrowed for each call.
///
/// Generated as the stateless `<Service>Methods` type, for every `S`
/// implementing the service trait. It is what lets a [`MultiDispatch`] serve
/// several services from one application object. [`CallContext::route`]
/// carries the [`MethodDescriptor::method`] of the selected method.
///
/// The defaults answer `UNIMPLEMENTED`, as [`Handler`]'s do.
pub trait ServiceMethods<S: ?Sized> {
    /// The service's method table.
    fn methods(&self) -> &'static [MethodDescriptor];

    /// See [`Handler::call`].
    fn call(
        &self,
        service: &mut S,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Result<Vec<u8>, Status> {
        let _ = (service, request);
        Err(unimplemented(ctx.path))
    }

    /// See [`Handler::on_message`].
    fn on_message(
        &self,
        service: &mut S,
        ctx: &mut CallContext<'_>,
        message: &[u8],
    ) -> Result<(), Status> {
        let _ = (service, message);
        Err(unimplemented(ctx.path))
    }

    /// See [`Handler::on_half_close`].
    fn on_half_close(&self, service: &mut S, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        let _ = (service, ctx);
        Ok(())
    }

    /// See [`Handler::poll_response`].
    fn poll_response(
        &self,
        service: &mut S,
        ctx: &mut CallContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        let _ = (service, cx);
        Poll::Ready(Next::Done(Err(unimplemented(ctx.path))))
    }

    /// See [`Handler::on_cancel`].
    fn on_cancel(&self, service: &mut S, ctx: &mut CallContext<'_>) {
        let _ = (service, ctx);
    }
}

/// Two services of a [`Registry`] or [`MultiDispatch`] serve the same path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteConflict {
    /// The path served twice.
    pub path: &'static str,
}

impl fmt::Display for RouteConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "more than one service serves {}", self.path)
    }
}

impl core::error::Error for RouteConflict {}

/// The merged method table of a composite dispatcher.
#[derive(Debug, Default)]
struct RouteTable {
    /// Every method, with its index as the composite route's method.
    methods: Vec<MethodDescriptor>,
    /// For each method: the member that serves it, and the member's route.
    targets: Vec<(usize, Route)>,
    /// Indices into `methods`, sorted by path.
    by_path: Vec<u32>,
}

impl RouteTable {
    fn add(&mut self, member: usize, methods: &[MethodDescriptor]) {
        for descriptor in methods {
            let method = u32::try_from(self.methods.len()).expect("fewer than 2^32 methods");
            self.methods.push(MethodDescriptor::new(
                descriptor.path,
                descriptor.kind,
                method,
            ));
            self.targets.push((member, descriptor.route()));
        }
    }

    fn finish(mut self) -> Result<Self, RouteConflict> {
        let methods = &self.methods;
        self.by_path = (0..methods.len() as u32).collect();
        self.by_path.sort_by_key(|&i| methods[i as usize].path);
        for pair in self.by_path.windows(2) {
            let path = methods[pair[0] as usize].path;
            if path == methods[pair[1] as usize].path {
                return Err(RouteConflict { path });
            }
        }
        Ok(self)
    }

    fn route(&self, path: &str) -> Option<Route> {
        let i = self
            .by_path
            .binary_search_by(|&i| self.methods[i as usize].path.cmp(path))
            .ok()?;
        Some(self.methods[self.by_path[i] as usize].route())
    }

    fn target(&self, route: Route) -> Option<(usize, Route)> {
        self.targets.get(route.method as usize).copied()
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for () {}
    impl<L: Sealed, D> Sealed for (L, D) {}
}

/// The services of a [`Registry`]: `()` and `(L, D)` for a list `L` extended
/// by one more [`Dispatcher`] `D`, as built by
/// [`RegistryBuilder::with_service`]. The services are stored by value, so a
/// registry is `Send` when its services are.
pub trait ServiceList: sealed::Sealed {
    #[doc(hidden)]
    const LEN: usize;

    #[doc(hidden)]
    fn member(&self, index: usize) -> Option<&dyn Dispatcher>;

    #[doc(hidden)]
    fn member_mut(&mut self, index: usize) -> Option<&mut dyn Dispatcher>;
}

impl ServiceList for () {
    const LEN: usize = 0;

    fn member(&self, _: usize) -> Option<&dyn Dispatcher> {
        None
    }

    fn member_mut(&mut self, _: usize) -> Option<&mut dyn Dispatcher> {
        None
    }
}

impl<L: ServiceList, D: Dispatcher> ServiceList for (L, D) {
    const LEN: usize = L::LEN + 1;

    fn member(&self, index: usize) -> Option<&dyn Dispatcher> {
        if index == L::LEN {
            Some(&self.1)
        } else {
            self.0.member(index)
        }
    }

    fn member_mut(&mut self, index: usize) -> Option<&mut dyn Dispatcher> {
        if index == L::LEN {
            Some(&mut self.1)
        } else {
            self.0.member_mut(index)
        }
    }
}

/// Several independent dispatchers served together on one connection.
///
/// Each registered service is a [`Dispatcher`] that owns or borrows its own
/// application object, typically a generated `<Service>Dispatch`:
///
/// ```ignore
/// let mut services = Registry::builder()
///     .with_service(DeviceDispatch::new(&mut device))
///     .with_service(LogDispatch::new(log))
///     .build()?;
/// server.serve(&mut services).await?;
/// ```
///
/// A call is routed to the service that declared its path, once, and every
/// event of the call is delivered to that service with the service's own
/// [`Route`] in [`CallContext::route`]. Unknown paths are answered with
/// `UNIMPLEMENTED` as soon as their headers arrive.
#[derive(Debug)]
pub struct Registry<L> {
    services: L,
    table: RouteTable,
}

/// Factory of a [`Registry`], created by [`Registry::builder`].
#[derive(Debug)]
pub struct RegistryBuilder<L = ()> {
    services: L,
}

impl Registry<()> {
    /// Start building a registry without services.
    pub fn builder() -> RegistryBuilder {
        RegistryBuilder { services: () }
    }
}

impl<L: ServiceList> RegistryBuilder<L> {
    /// Add a service. Ownership follows `service`: pass a dispatcher that
    /// borrows its application object (`<Service>Dispatch::new(&mut app)`) to
    /// use the object again once the registry is dropped.
    pub fn with_service<D: Dispatcher>(self, service: D) -> RegistryBuilder<(L, D)> {
        RegistryBuilder {
            services: (self.services, service),
        }
    }

    /// Construct the registry, merging the services' method tables.
    ///
    /// Fails if two services declare the same path.
    pub fn build(self) -> Result<Registry<L>, RouteConflict> {
        let mut table = RouteTable::default();
        for index in 0..L::LEN {
            if let Some(member) = self.services.member(index) {
                table.add(index, member.methods());
            }
        }
        Ok(Registry {
            services: self.services,
            table: table.finish()?,
        })
    }
}

impl<L> Registry<L> {
    /// Unwrap the services, as nested pairs in registration order:
    /// `(((), first), second)`.
    pub fn into_inner(self) -> L {
        self.services
    }
}

impl<L: ServiceList> Registry<L> {
    /// Deliver an event of the call in `ctx` to the service that serves it,
    /// with that service's route.
    fn delegate<R>(
        &mut self,
        ctx: &mut CallContext<'_>,
        f: impl FnOnce(&mut dyn Dispatcher, &mut CallContext<'_>) -> R,
    ) -> Option<R> {
        let (index, route) = self.table.target(ctx.route)?;
        let member = self.services.member_mut(index)?;
        Some(ctx.with_route(route, |ctx| f(member, ctx)))
    }
}

impl<L: ServiceList> Handler for Registry<L> {
    fn route(&self, path: &str) -> Option<Route> {
        self.table.route(path)
    }

    fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        self.delegate(ctx, |member, ctx| member.call(ctx, request))
            .unwrap_or_else(|| Err(unimplemented(ctx.path)))
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        self.delegate(ctx, |member, ctx| member.on_message(ctx, message))
            .unwrap_or_else(|| Err(unimplemented(ctx.path)))
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        self.delegate(ctx, |member, ctx| member.on_half_close(ctx))
            .unwrap_or(Ok(()))
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        self.delegate(ctx, |member, ctx| member.poll_response(ctx, cx))
            .unwrap_or_else(|| Poll::Ready(Next::Done(Err(unimplemented(ctx.path)))))
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.delegate(ctx, |member, ctx| member.on_cancel(ctx));
    }
}

impl<L: ServiceList> Dispatcher for Registry<L> {
    fn methods(&self) -> &[MethodDescriptor] {
        &self.table.methods
    }
}

type Members<S> = Vec<Box<dyn ServiceMethods<S> + Send + Sync>>;

/// One application object serving several generated services.
///
/// The object is held once; for each call event the generated, stateless
/// `<Service>Methods` of the selected service borrows it mutably for the
/// duration of the dispatch. No cloning or locking is involved:
///
/// ```ignore
/// let mut services = MultiDispatch::builder(&mut device)
///     .with_service(DeviceMethods)
///     .with_service(DiagnosticsMethods)
///     .build()?;
/// server.serve(&mut services).await?;
/// ```
///
/// `S` is the object itself or a `&mut` borrow of it (generated service
/// traits are implemented for `&mut S` too).
pub struct MultiDispatch<S> {
    service: S,
    members: Members<S>,
    table: RouteTable,
}

impl<S: fmt::Debug> fmt::Debug for MultiDispatch<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiDispatch")
            .field("service", &self.service)
            .field("methods", &self.table.methods)
            .finish()
    }
}

/// Factory of a [`MultiDispatch`], created by [`MultiDispatch::builder`].
pub struct MultiDispatchBuilder<S> {
    service: S,
    members: Members<S>,
}

impl<S: fmt::Debug> fmt::Debug for MultiDispatchBuilder<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiDispatchBuilder")
            .field("service", &self.service)
            .field("services", &self.members.len())
            .finish()
    }
}

impl<S> MultiDispatch<S> {
    /// Start building a dispatcher for `service`, which will serve every
    /// service added with [`with_service`](MultiDispatchBuilder::with_service).
    pub fn builder(service: S) -> MultiDispatchBuilder<S> {
        MultiDispatchBuilder {
            service,
            members: Vec::new(),
        }
    }

    /// The application object.
    pub fn get_ref(&self) -> &S {
        &self.service
    }

    /// The application object, mutably.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.service
    }

    /// Unwrap the application object.
    pub fn into_inner(self) -> S {
        self.service
    }

    fn delegate<R>(
        &mut self,
        ctx: &mut CallContext<'_>,
        f: impl FnOnce(&dyn ServiceMethods<S>, &mut S, &mut CallContext<'_>) -> R,
    ) -> Option<R> {
        let (index, route) = self.table.target(ctx.route)?;
        let member = self.members.get(index)?;
        let service = &mut self.service;
        Some(ctx.with_route(route, |ctx| f(&**member, service, ctx)))
    }
}

impl<S> MultiDispatchBuilder<S> {
    /// Serve the service described by `methods` (a generated
    /// `<Service>Methods`) from the application object.
    pub fn with_service(mut self, methods: impl ServiceMethods<S> + Send + Sync + 'static) -> Self {
        self.members.push(Box::new(methods));
        self
    }

    /// Construct the dispatcher, merging the services' method tables.
    ///
    /// Fails if two services declare the same path.
    pub fn build(self) -> Result<MultiDispatch<S>, RouteConflict> {
        let mut table = RouteTable::default();
        for (index, member) in self.members.iter().enumerate() {
            table.add(index, member.methods());
        }
        Ok(MultiDispatch {
            service: self.service,
            members: self.members,
            table: table.finish()?,
        })
    }
}

impl<S> Handler for MultiDispatch<S> {
    fn route(&self, path: &str) -> Option<Route> {
        self.table.route(path)
    }

    fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        self.delegate(ctx, |m, service, ctx| m.call(service, ctx, request))
            .unwrap_or_else(|| Err(unimplemented(ctx.path)))
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        self.delegate(ctx, |m, service, ctx| m.on_message(service, ctx, message))
            .unwrap_or_else(|| Err(unimplemented(ctx.path)))
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        self.delegate(ctx, |m, service, ctx| m.on_half_close(service, ctx))
            .unwrap_or(Ok(()))
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        self.delegate(ctx, |m, service, ctx| m.poll_response(service, ctx, cx))
            .unwrap_or_else(|| Poll::Ready(Next::Done(Err(unimplemented(ctx.path)))))
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.delegate(ctx, |m, service, ctx| m.on_cancel(service, ctx));
    }
}

impl<S> Dispatcher for MultiDispatch<S> {
    fn methods(&self) -> &[MethodDescriptor] {
        &self.table.methods
    }
}
