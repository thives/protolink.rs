//! Service registration: [`Registry`] and [`MultiDispatch`] merge method
//! tables, reject duplicate paths, and route every event of a call to the one
//! service selected when the call started.

extern crate std;

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::vec::Vec;
use core::task::{Context, Poll};

use super::*;
use crate::test_support::{drain, pump};

const ALPHA: &[MethodDescriptor] = &[
    MethodDescriptor::new("/a.A/Unary", MethodKind::Unary, 0),
    MethodDescriptor::new("/a.A/Chat", MethodKind::BidiStreaming, 1),
];

const BETA: &[MethodDescriptor] = &[
    MethodDescriptor::new("/b.B/Unary", MethodKind::Unary, 0),
    MethodDescriptor::new("/b.B/Chat", MethodKind::BidiStreaming, 1),
];

/// A static dispatcher, as generated code is, that records each event with
/// the route it was delivered with. Chat calls echo their requests and end
/// once the client half-closes.
#[derive(Debug)]
struct Service {
    name: &'static str,
    methods: &'static [MethodDescriptor],
    events: Vec<(&'static str, CallId, Route)>,
    queues: BTreeMap<CallId, VecDeque<Vec<u8>>>,
    ended: BTreeSet<CallId>,
}

impl Service {
    fn new(name: &'static str, methods: &'static [MethodDescriptor]) -> Self {
        Self {
            name,
            methods,
            events: Vec::new(),
            queues: BTreeMap::new(),
            ended: BTreeSet::new(),
        }
    }

    fn record(&mut self, event: &'static str, ctx: &CallContext<'_>) {
        assert!(
            self.methods.iter().any(|m| m.path == ctx.path),
            "{} received {event} for {}",
            self.name,
            ctx.path
        );
        assert_eq!(
            Some(ctx.route),
            Route::find(self.methods, ctx.path),
            "{} received the route of another service",
            self.name
        );
        if self.events.last() != Some(&(event, ctx.id, ctx.route)) {
            self.events.push((event, ctx.id, ctx.route));
        }
    }
}

impl Handler for Service {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(self.methods, path)
    }

    fn call(&mut self, ctx: &mut CallContext<'_>, _: &[u8]) -> Result<Vec<u8>, Status> {
        self.record("call", ctx);
        Ok(self.name.as_bytes().to_vec())
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        self.record("message", ctx);
        self.queues
            .entry(ctx.id)
            .or_default()
            .push_back(message.to_vec());
        Ok(())
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        self.record("half_close", ctx);
        self.ended.insert(ctx.id);
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        self.record("poll", ctx);
        if let Some(message) = self.queues.get_mut(&ctx.id).and_then(VecDeque::pop_front) {
            return Poll::Ready(Next::Message(message));
        }
        if self.ended.remove(&ctx.id) {
            return Poll::Ready(Next::Done(Ok(())));
        }
        Poll::Pending
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.record("cancel", ctx);
        self.queues.remove(&ctx.id);
    }
}

impl Dispatcher for Service {
    fn methods(&self) -> &[MethodDescriptor] {
        self.methods
    }
}

fn kinds(events: &[(&'static str, CallId, Route)], id: CallId) -> Vec<&'static str> {
    events
        .iter()
        .filter(|(_, call, _)| *call == id)
        .map(|(event, _, _)| *event)
        .collect()
}

#[test]
fn registry_rejects_duplicate_paths() {
    let err = Registry::builder()
        .with_service(Service::new("alpha", ALPHA))
        .with_service(Service::new("beta", BETA))
        .with_service(Service::new("again", ALPHA))
        .build()
        .unwrap_err();
    assert_eq!(err, RouteConflict { path: "/a.A/Chat" });
    assert!(alloc::format!("{err}").contains("/a.A/Chat"));
}

#[test]
fn multi_dispatch_rejects_duplicate_paths() {
    let err = MultiDispatch::builder(App::default())
        .with_service(AlphaMethods)
        .with_service(AlphaMethods)
        .build()
        .unwrap_err();
    assert_eq!(err.path, "/a.A/Chat");
}

#[test]
fn registry_merges_tables() {
    let registry = Registry::builder()
        .with_service(Service::new("alpha", ALPHA))
        .with_service(Service::new("beta", BETA))
        .build()
        .unwrap();
    let paths: Vec<_> = registry.methods().iter().map(|m| m.path).collect();
    assert_eq!(
        paths,
        ["/a.A/Unary", "/a.A/Chat", "/b.B/Unary", "/b.B/Chat"]
    );
    for (index, method) in registry.methods().iter().enumerate() {
        assert_eq!(method.method as usize, index);
        assert_eq!(registry.route(method.path), Some(method.route()));
    }
    assert_eq!(registry.route("/c.C/X"), None);
}

#[test]
fn every_event_of_a_call_reaches_the_selected_service() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut alpha = Service::new("alpha", ALPHA);
    let mut beta = Service::new("beta", BETA);
    let mut registry = Registry::builder()
        .with_service(&mut alpha)
        .with_service(&mut beta)
        .build()
        .unwrap();

    let unary = client.start_unary("/b.B/Unary", b"").unwrap();
    let chat = client.start_streaming("/b.B/Chat").unwrap();
    client.send_message(chat, b"hi").unwrap();
    let cancelled = client.start_streaming("/a.A/Chat").unwrap();
    client.send_message(cancelled, b"bye").unwrap();
    pump(&mut client, &mut server, &mut registry);
    client.cancel(cancelled);
    client.close_send(chat).unwrap();
    pump(&mut client, &mut server, &mut registry);

    assert_eq!(
        client.take_response(unary).unwrap().unwrap().message,
        b"beta"
    );
    assert_eq!(
        drain(&mut client, chat),
        (alloc::vec![b"hi".to_vec()], Some(Ok(())))
    );
    drop(registry);

    // The members can be used again once the registry is gone.
    assert_eq!(kinds(&beta.events, unary), ["call"]);
    assert_eq!(
        kinds(&beta.events, chat),
        ["message", "poll", "half_close", "poll"]
    );
    assert_eq!(
        kinds(&alpha.events, cancelled),
        ["message", "poll", "cancel"]
    );
    assert!(kinds(&alpha.events, chat).is_empty());
    // Each service sees its own route, not the registry's.
    assert!(
        beta.events
            .iter()
            .all(|(_, id, route)| *route == if *id == unary { BETA[0] } else { BETA[1] }.route())
    );
}

#[test]
fn unknown_paths_are_unimplemented_before_the_request_ends() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut registry = Registry::builder()
        .with_service(Service::new("alpha", ALPHA))
        .build()
        .unwrap();
    let id = client.start_streaming("/c.C/Chat").unwrap();
    client.send_message(id, b"x").unwrap();
    pump(&mut client, &mut server, &mut registry);
    let Some(Next::Done(Err(status))) = client.try_next(id) else {
        panic!("the call should have been rejected");
    };
    assert_eq!(status.code, Code::Unimplemented);
    let ((), alpha) = registry.into_inner();
    assert!(alpha.events.is_empty());
}

#[test]
fn registries_nest() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let inner = Registry::builder()
        .with_service(Service::new("beta", BETA))
        .build()
        .unwrap();
    let mut outer = Registry::builder()
        .with_service(Service::new("alpha", ALPHA))
        .with_service(inner)
        .build()
        .unwrap();
    let a = client.start_unary("/a.A/Unary", b"").unwrap();
    let b = client.start_unary("/b.B/Unary", b"").unwrap();
    pump(&mut client, &mut server, &mut outer);
    assert_eq!(client.take_response(a).unwrap().unwrap().message, b"alpha");
    assert_eq!(client.take_response(b).unwrap().unwrap().message, b"beta");
}

/// One application object behind two services.
#[derive(Debug, Default)]
struct App {
    log: Vec<(&'static str, &'static str, Route)>,
    queued: BTreeMap<CallId, Vec<u8>>,
}

struct AlphaMethods;
struct BetaMethods;

impl ServiceMethods<App> for AlphaMethods {
    fn methods(&self) -> &'static [MethodDescriptor] {
        ALPHA
    }

    fn call(&self, app: &mut App, ctx: &mut CallContext<'_>, _: &[u8]) -> Result<Vec<u8>, Status> {
        app.log.push(("alpha", "call", ctx.route));
        Ok(alloc::format!("alpha after {}", app.log.len() - 1).into_bytes())
    }

    fn on_message(
        &self,
        app: &mut App,
        ctx: &mut CallContext<'_>,
        message: &[u8],
    ) -> Result<(), Status> {
        app.log.push(("alpha", "message", ctx.route));
        app.queued.insert(ctx.id, message.to_vec());
        Ok(())
    }

    fn poll_response(
        &self,
        app: &mut App,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        Poll::Ready(match app.queued.remove(&ctx.id) {
            Some(message) => Next::Message(message),
            None => Next::Done(Ok(())),
        })
    }
}

impl ServiceMethods<App> for BetaMethods {
    fn methods(&self) -> &'static [MethodDescriptor] {
        BETA
    }

    fn call(&self, app: &mut App, ctx: &mut CallContext<'_>, _: &[u8]) -> Result<Vec<u8>, Status> {
        app.log.push(("beta", "call", ctx.route));
        Ok(alloc::format!("beta after {}", app.log.len() - 1).into_bytes())
    }
}

#[test]
fn one_object_serves_two_services() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut services = MultiDispatch::builder(App::default())
        .with_service(AlphaMethods)
        .with_service(BetaMethods)
        .build()
        .unwrap();

    let a = client.start_unary("/a.A/Unary", b"").unwrap();
    pump(&mut client, &mut server, &mut services);
    let b = client.start_unary("/b.B/Unary", b"").unwrap();
    let chat = client.start_streaming("/a.A/Chat").unwrap();
    client.send_message(chat, b"echo").unwrap();
    client.close_send(chat).unwrap();
    pump(&mut client, &mut server, &mut services);

    // Both services changed the same object, in call order.
    assert_eq!(
        client.take_response(a).unwrap().unwrap().message,
        b"alpha after 0"
    );
    assert_eq!(
        client.take_response(b).unwrap().unwrap().message,
        b"beta after 1"
    );
    assert_eq!(
        drain(&mut client, chat),
        (alloc::vec![b"echo".to_vec()], Some(Ok(())))
    );
    // `BetaMethods` has no streaming methods: its defaults are unimplemented.
    let unimplemented = client.start_streaming("/b.B/Chat").unwrap();
    client.close_send(unimplemented).unwrap();
    pump(&mut client, &mut server, &mut services);
    assert_eq!(
        drain(&mut client, unimplemented)
            .1
            .unwrap()
            .unwrap_err()
            .code,
        Code::Unimplemented
    );

    let app = services.into_inner();
    assert_eq!(
        app.log,
        [
            ("alpha", "call", ALPHA[0].route()),
            ("beta", "call", BETA[0].route()),
            ("alpha", "message", ALPHA[1].route()),
        ]
    );
}

#[test]
fn composite_dispatchers_are_send_when_their_services_are() {
    fn send<T: Send>(_: &T) {}
    let registry = Registry::builder()
        .with_service(Service::new("alpha", ALPHA))
        .build()
        .unwrap();
    send(&registry);
    let multi = MultiDispatch::builder(App::default())
        .with_service(AlphaMethods)
        .build()
        .unwrap();
    send(&multi);
}
