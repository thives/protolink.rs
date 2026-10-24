//! The gateway router over the tokio driver: one caller connection served by a
//! local service and by a downstream service on its own connection.
#![cfg(feature = "tokio")]

use std::collections::{BTreeMap, VecDeque};
use std::task::{Context, Poll};

use protolink::grpc::{
    CallContext, CallId, Client as GrpcClient, ClientConfig as GrpcClientConfig, Code, Dispatcher,
    Handler, MethodDescriptor, MethodKind, Next, Route, Router, Status,
};
use protolink::tokio::{client, compat, server};
use tokio::io::duplex;

const LOCAL: &[MethodDescriptor] = &[MethodDescriptor::new(
    "/t.Local/Upper",
    MethodKind::Unary,
    0,
)];

const REMOTE: &[MethodDescriptor] = &[
    MethodDescriptor::new("/t.Remote/Unary", MethodKind::Unary, 0),
    MethodDescriptor::new("/t.Remote/Count", MethodKind::ServerStreaming, 1),
    MethodDescriptor::new("/t.Remote/Sum", MethodKind::ClientStreaming, 2),
    MethodDescriptor::new("/t.Remote/Echo", MethodKind::BidiStreaming, 3),
];

/// The gateway's own service.
struct Local;

impl Handler for Local {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(LOCAL, path)
    }

    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        Ok(request.to_ascii_uppercase())
    }
}

impl Dispatcher for Local {
    fn methods(&self) -> &[MethodDescriptor] {
        LOCAL
    }
}

/// The downstream service: one answer per shape. Its unary answers start with
/// `name`, so that a pool's members can be told apart.
#[derive(Default)]
struct Backend {
    name: &'static str,
    calls: BTreeMap<CallId, Pending>,
}

impl Backend {
    fn named(name: &'static str) -> Self {
        Self {
            name,
            calls: BTreeMap::new(),
        }
    }
}

#[derive(Default)]
struct Pending {
    replies: VecDeque<Vec<u8>>,
    /// The last request message, for `Count`, which answers at the half-close.
    last: Option<Vec<u8>>,
    half_closed: bool,
    total: usize,
    answered: bool,
}

impl Handler for Backend {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(REMOTE, path)
    }

    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        Ok([self.name.as_bytes(), b"pong:", request].concat())
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        let call = self.calls.entry(ctx.id).or_default();
        match ctx.route.kind {
            MethodKind::ServerStreaming => call.last = Some(message.to_vec()),
            MethodKind::ClientStreaming => call.total += message.len(),
            _ => call.replies.push_back(message.to_vec()),
        }
        Ok(())
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        let call = self.calls.entry(ctx.id).or_default();
        call.half_closed = true;
        if let Some(last) = call.last.take() {
            call.replies.extend(std::iter::repeat_n(last, 3));
        }
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        let call = self.calls.entry(ctx.id).or_default();
        if let Some(reply) = call.replies.pop_front() {
            return Poll::Ready(Next::Message(reply));
        }
        if !call.half_closed {
            return Poll::Pending;
        }
        if ctx.route.kind == MethodKind::ClientStreaming && !call.answered {
            call.answered = true;
            return Poll::Ready(Next::Message(call.total.to_string().into_bytes()));
        }
        self.calls.remove(&ctx.id);
        Poll::Ready(Next::Done(Ok(())))
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.calls.remove(&ctx.id);
    }
}

fn gateway_router() -> Router {
    Router::builder()
        .with_service(Local)
        .with_route(REMOTE, GrpcClient::new(GrpcClientConfig::default()))
        .build()
        .expect("routes do not clash")
}

#[tokio::test]
async fn local_and_forwarded_calls_share_a_gateway_connection() {
    let (caller_io, gateway_io) = duplex(64 * 1024);
    let (gateway_down, backend_io) = duplex(64 * 1024);
    let mut router = gateway_router();
    let gateway = async move {
        let mut downstream = [compat(gateway_down)];
        server(gateway_io)
            .build()
            .serve_router(&mut router, &mut downstream)
            .await
    };
    let backend = async move {
        let mut service = Backend::default();
        server(backend_io).build().serve(&mut service).await
    };
    let caller = async move {
        let mut client = client(caller_io).build();
        assert_eq!(client.unary("/t.Local/Upper", b"hi").await.unwrap(), b"HI");
        assert_eq!(
            client.unary("/t.Remote/Unary", b"ping").await.unwrap(),
            b"pong:ping"
        );
        assert_eq!(
            client.unary("/t.Local/Upper", b"again").await.unwrap(),
            b"AGAIN"
        );
    };
    let (gateway, _backend, ()) = tokio::join!(gateway, backend, caller);
    gateway.expect("gateway ends cleanly when the caller hangs up");
}

/// Serve `router` on one side of a fresh duplex, with `backend_io` as its
/// downstream, until the caller hangs up.
async fn gateway_on(
    gateway_io: tokio::io::DuplexStream,
    gateway_down: tokio::io::DuplexStream,
    router: &mut Router,
) -> Result<(), protolink::Error<std::io::Error>> {
    let mut downstream = [compat(gateway_down)];
    server(gateway_io)
        .build()
        .serve_router(router, &mut downstream)
        .await
}

#[tokio::test]
async fn every_shape_is_forwarded_with_its_own_message_rules() {
    let (caller_io, gateway_io) = duplex(64 * 1024);
    let (gateway_down, backend_io) = duplex(64 * 1024);
    let mut router = gateway_router();
    let gateway = gateway_on(gateway_io, gateway_down, &mut router);
    let backend = async move {
        let mut service = Backend::default();
        server(backend_io).build().serve(&mut service).await
    };
    let caller = async move {
        let client = client(caller_io).build();

        // Server streaming: one request, three replies.
        let mut call = client.streaming("/t.Remote/Count").unwrap();
        call.send(b"x").await.unwrap();
        call.close_send().await.unwrap();
        let mut replies = Vec::new();
        while let Some(message) = call.message().await.unwrap() {
            replies.push(message);
        }
        assert_eq!(replies, vec![b"x".to_vec(); 3]);

        // Client streaming: the downstream sums the request sizes.
        let mut call = client.streaming("/t.Remote/Sum").unwrap();
        call.send(b"ab").await.unwrap();
        call.send(b"cde").await.unwrap();
        call.close_send().await.unwrap();
        assert_eq!(call.message().await.unwrap(), Some(b"5".to_vec()));
        assert_eq!(call.message().await.unwrap(), None);

        // Bidirectional: echoes in order.
        let mut call = client.streaming("/t.Remote/Echo").unwrap();
        call.send(b"m1").await.unwrap();
        call.send(b"m2").await.unwrap();
        call.close_send().await.unwrap();
        assert_eq!(call.message().await.unwrap(), Some(b"m1".to_vec()));
        assert_eq!(call.message().await.unwrap(), Some(b"m2".to_vec()));
        assert_eq!(call.message().await.unwrap(), None);

        // Empty request streams: a client stream and a bidi stream that send
        // nothing but the half-close.
        let mut call = client.streaming("/t.Remote/Sum").unwrap();
        call.close_send().await.unwrap();
        assert_eq!(call.message().await.unwrap(), Some(b"0".to_vec()));
        assert_eq!(call.message().await.unwrap(), None);
        let mut call = client.streaming("/t.Remote/Echo").unwrap();
        call.close_send().await.unwrap();
        assert_eq!(call.message().await.unwrap(), None);
    };
    let (gateway, _backend, ()) = tokio::join!(gateway, backend, caller);
    gateway.expect("gateway ends cleanly");
}

#[tokio::test]
async fn an_unknown_method_is_unimplemented_on_the_wire() {
    let (caller_io, gateway_io) = duplex(64 * 1024);
    let (gateway_down, backend_io) = duplex(64 * 1024);
    let mut router = gateway_router();
    let gateway = gateway_on(gateway_io, gateway_down, &mut router);
    let backend = async move {
        let mut service = Backend::default();
        server(backend_io).build().serve(&mut service).await
    };
    let caller = async move {
        let mut client = client(caller_io).build();
        let status = client.unary("/t.Nobody/Here", b"x").await.unwrap_err();
        assert_eq!(status.code, Code::Unimplemented);
    };
    let (gateway, _backend, ()) = tokio::join!(gateway, backend, caller);
    gateway.expect("gateway ends cleanly");
}

#[tokio::test]
async fn a_dead_downstream_fails_its_routes_and_nothing_else() {
    let (caller_io, gateway_io) = duplex(64 * 1024);
    let (gateway_down, backend_io) = duplex(64 * 1024);
    // The downstream is gone before the first call.
    drop(backend_io);
    let mut router = gateway_router();
    let gateway = gateway_on(gateway_io, gateway_down, &mut router);
    let caller = async move {
        let mut client = client(caller_io).build();
        assert_eq!(client.unary("/t.Local/Upper", b"ok").await.unwrap(), b"OK");
        let status = client.unary("/t.Remote/Unary", b"x").await.unwrap_err();
        assert_eq!(status.code, Code::Unavailable);
        assert_eq!(client.unary("/t.Local/Upper", b"ok").await.unwrap(), b"OK");
    };
    let (gateway, ()) = tokio::join!(gateway, caller);
    gateway.expect("gateway ends cleanly");
}

#[tokio::test]
async fn concurrent_gateway_connections_are_independent() {
    async fn one_caller(name: &'static [u8]) {
        let (caller_io, gateway_io) = duplex(64 * 1024);
        let (gateway_down, backend_io) = duplex(64 * 1024);
        let mut router = gateway_router();
        let gateway = gateway_on(gateway_io, gateway_down, &mut router);
        let backend = async move {
            let mut service = Backend::default();
            server(backend_io).build().serve(&mut service).await
        };
        let caller = async move {
            let mut client = client(caller_io).build();
            for _ in 0..8 {
                let reply = client.unary("/t.Remote/Unary", name).await.unwrap();
                assert_eq!(reply, [b"pong:".as_slice(), name].concat());
            }
        };
        let (gateway, _backend, ()) = tokio::join!(gateway, backend, caller);
        gateway.expect("gateway ends cleanly");
    }
    tokio::join!(one_caller(b"a"), one_caller(b"b"));
}

#[tokio::test]
async fn a_stuck_downstream_call_does_not_block_other_calls() {
    let (caller_io, gateway_io) = duplex(64 * 1024);
    let (gateway_down, backend_io) = duplex(64 * 1024);
    let mut router = gateway_router();
    let gateway = gateway_on(gateway_io, gateway_down, &mut router);
    let backend = async move {
        let mut service = Backend::default();
        server(backend_io).build().serve(&mut service).await
    };
    let caller = async move {
        let client = client(caller_io).build();
        // `Count` answers only after its half-close, so this call is stuck.
        let mut stuck = client.streaming("/t.Remote/Count").unwrap();
        stuck.send(b"x").await.unwrap();
        // Another call on the same connection still runs to completion.
        let mut echo = client.streaming("/t.Remote/Echo").unwrap();
        echo.send(b"hi").await.unwrap();
        echo.close_send().await.unwrap();
        assert_eq!(echo.message().await.unwrap(), Some(b"hi".to_vec()));
        assert_eq!(echo.message().await.unwrap(), None);
        stuck.close_send().await.unwrap();
        let mut replies = Vec::new();
        while let Some(message) = stuck.message().await.unwrap() {
            replies.push(message);
        }
        assert_eq!(replies, vec![b"x".to_vec(); 3]);
    };
    let (gateway, _backend, ()) = tokio::join!(gateway, backend, caller);
    gateway.expect("gateway ends cleanly");
}

/// A gateway serving `router` on `gateway_io`, with one downstream per entry of
/// `downstream`, until the caller hangs up.
async fn gateway_with(
    gateway_io: tokio::io::DuplexStream,
    downstream: Vec<tokio::io::DuplexStream>,
    router: &mut Router,
) -> Result<(), protolink::Error<std::io::Error>> {
    let mut downstream: Vec<_> = downstream.into_iter().map(compat).collect();
    server(gateway_io)
        .build()
        .serve_router(router, &mut downstream)
        .await
}

fn three_members() -> Vec<GrpcClient> {
    (0..3)
        .map(|_| GrpcClient::new(GrpcClientConfig::default()))
        .collect()
}

#[tokio::test]
async fn a_pool_round_robins_new_calls_and_skips_dead_members() {
    let (caller_io, gateway_io) = duplex(64 * 1024);
    let (one_gw, one) = duplex(64 * 1024);
    let (two_gw, two) = duplex(64 * 1024);
    let (three_gw, three) = duplex(64 * 1024);
    // The middle member is gone before the first call.
    drop(two);
    let mut router = Router::builder()
        .with_service(Local)
        .with_pool(REMOTE, three_members(), usize::MAX)
        .build()
        .unwrap();
    let gateway = gateway_with(gateway_io, vec![one_gw, two_gw, three_gw], &mut router);
    let one_service = async move {
        let mut service = Backend::named("one/");
        server(one).build().serve(&mut service).await
    };
    let three_service = async move {
        let mut service = Backend::named("three/");
        server(three).build().serve(&mut service).await
    };
    let caller = async move {
        let mut client = client(caller_io).build();
        let mut replies = Vec::new();
        for _ in 0..4 {
            replies.push(client.unary("/t.Remote/Unary", b"x").await.unwrap());
        }
        assert_eq!(
            replies,
            [
                b"one/pong:x".to_vec(),
                b"three/pong:x".to_vec(),
                b"one/pong:x".to_vec(),
                b"three/pong:x".to_vec(),
            ]
        );
    };
    let (gateway, _one, _three, ()) = tokio::join!(gateway, one_service, three_service, caller);
    gateway.expect("gateway ends cleanly");
}

#[tokio::test]
async fn a_pool_with_no_usable_member_is_unavailable() {
    let (caller_io, gateway_io) = duplex(64 * 1024);
    let (one_gw, one) = duplex(64 * 1024);
    let (two_gw, two) = duplex(64 * 1024);
    drop(one);
    drop(two);
    let mut router = Router::builder()
        .with_service(Local)
        .with_pool(
            REMOTE,
            three_members().into_iter().take(2).collect(),
            usize::MAX,
        )
        .build()
        .unwrap();
    let gateway = gateway_with(gateway_io, vec![one_gw, two_gw], &mut router);
    let caller = async move {
        let mut client = client(caller_io).build();
        let status = client.unary("/t.Remote/Unary", b"x").await.unwrap_err();
        assert_eq!(status.code, Code::Unavailable);
        assert_eq!(client.unary("/t.Local/Upper", b"ok").await.unwrap(), b"OK");
    };
    let (gateway, ()) = tokio::join!(gateway, caller);
    gateway.expect("gateway ends cleanly");
}

#[tokio::test]
async fn a_pool_at_its_call_limit_refuses_the_next_call() {
    let (caller_io, gateway_io) = duplex(64 * 1024);
    let (member_gw, member) = duplex(64 * 1024);
    let mut router = Router::builder()
        .with_service(Local)
        .with_pool(
            REMOTE,
            vec![GrpcClient::new(GrpcClientConfig::default())],
            1,
        )
        .build()
        .unwrap();
    let gateway = gateway_with(gateway_io, vec![member_gw], &mut router);
    let service = async move {
        let mut service = Backend::named("");
        server(member).build().serve(&mut service).await
    };
    let caller = async move {
        let client = client(caller_io).build();
        // `Count` answers only after its half-close, so it holds the one slot.
        let mut held = client.streaming("/t.Remote/Count").unwrap();
        held.send(b"x").await.unwrap();
        let mut refused = client.streaming("/t.Remote/Echo").unwrap();
        refused.send(b"y").await.unwrap();
        refused.close_send().await.unwrap();
        let status = refused.message().await.unwrap_err();
        assert_eq!(status.code, Code::ResourceExhausted);

        held.close_send().await.unwrap();
        let mut replies = Vec::new();
        while let Some(message) = held.message().await.unwrap() {
            replies.push(message);
        }
        assert_eq!(replies, vec![b"x".to_vec(); 3]);
        // The slot is free again once the held call has finished.
        let mut echo = client.streaming("/t.Remote/Echo").unwrap();
        echo.send(b"z").await.unwrap();
        echo.close_send().await.unwrap();
        assert_eq!(echo.message().await.unwrap(), Some(b"z".to_vec()));
    };
    let (gateway, _service, ()) = tokio::join!(gateway, service, caller);
    gateway.expect("gateway ends cleanly");
}
