//! Deterministic tests of a router serving one gateway connection from a local
//! service and from one downstream service. The caller, gateway and downstream
//! exchange bytes in memory.

extern crate std;

use alloc::collections::BTreeSet;
use alloc::vec::Vec;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use super::*;
use crate::dispatch::Dispatcher;
use crate::local::CALL_QUANTUM;
use crate::test_support::drain;
use crate::tests_relay::{
    BIDI, DENIED, METHODS as REMOTE, RawBackend, Script, Service, metadata, script,
};
use protolink_http2::StreamId;

const LOCAL: &[MethodDescriptor] = &[
    MethodDescriptor::new("/test.Local/Upper", MethodKind::Unary, 0),
    MethodDescriptor::new("/test.Local/Echo", MethodKind::BidiStreaming, 1),
];

const REMOTE_UNARY: &str = "/test.Relay/Unary";
const REMOTE_STREAM: &str = "/test.Relay/Server";

/// A local service: `Upper` uppercases its request, `Echo` answers each
/// request message as it arrives.
#[derive(Default)]
struct Local {
    pending: Vec<Vec<u8>>,
    half_closed: bool,
    cancelled: usize,
}

impl Handler for Local {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(LOCAL, path)
    }

    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        Ok(request.to_ascii_uppercase())
    }

    fn on_message(&mut self, _: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        self.pending.push(message.to_vec());
        Ok(())
    }

    fn on_half_close(&mut self, _: &mut CallContext<'_>) -> Result<(), Status> {
        self.half_closed = true;
        Ok(())
    }

    fn poll_response(
        &mut self,
        _: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        if !self.pending.is_empty() {
            return Poll::Ready(Next::Message(self.pending.remove(0)));
        }
        if self.half_closed {
            Poll::Ready(Next::Done(Ok(())))
        } else {
            Poll::Pending
        }
    }

    fn on_cancel(&mut self, _: &mut CallContext<'_>) {
        self.cancelled += 1;
    }
}

impl Dispatcher for Local {
    fn methods(&self) -> &[MethodDescriptor] {
        LOCAL
    }
}

struct Gateway {
    caller: Client,
    router: Router,
    backend: Server,
    service: Service,
}

impl Gateway {
    fn new(script: Script) -> Self {
        let service = Service {
            script,
            ..Service::default()
        };
        Self {
            caller: Client::new(ClientConfig::default()),
            router: Router::builder()
                .with_service(Local::default())
                .with_route(REMOTE, Client::new(ClientConfig::default()))
                .build()
                .unwrap(),
            backend: Server::new(ServerConfig::default()),
            service,
        }
    }

    /// Move bytes between the caller, the router and the downstream until
    /// nothing moves.
    fn settle(&mut self) {
        for _ in 0..256 {
            let mut progress = false;
            let out = self.caller.take_output();
            if !out.is_empty() {
                progress = true;
                self.router.recv(&out).unwrap();
            }
            self.router.poll(&mut Context::from_waker(Waker::noop()));
            let out = self.router.take_output();
            if !out.is_empty() {
                progress = true;
                self.caller.recv(&out).unwrap();
            }
            let out = self.router.backend(0).unwrap().take_output();
            if !out.is_empty() {
                progress = true;
                self.backend.recv(&out, &mut self.service).unwrap();
            }
            self.backend
                .poll(&mut self.service, &mut Context::from_waker(Waker::noop()));
            let out = self.backend.take_output();
            if !out.is_empty() {
                progress = true;
                self.router.backend_recv(0, &out).unwrap();
            }
            if !progress {
                return;
            }
        }
        panic!("gateway did not settle");
    }
}

fn outcome(gateway: &mut Gateway, id: CallId) -> (Vec<Vec<u8>>, Option<Result<(), Status>>) {
    drain(&mut gateway.caller, id)
}

#[test]
fn local_and_forwarded_unary_calls_share_one_connection() {
    let mut g = Gateway::new(Script {
        replies: alloc::vec![b"remote".to_vec()],
        ..script()
    });
    let local = g
        .caller
        .start_unary_with("/test.Local/Upper", b"hi", &CallOptions::default())
        .unwrap();
    let remote = g
        .caller
        .start_unary_with(REMOTE_UNARY, b"ping", &CallOptions::default())
        .unwrap();
    g.settle();
    let response = g.caller.take_response(local).unwrap().unwrap();
    assert_eq!(response.message, b"HI");
    let response = g.caller.take_response(remote).unwrap().unwrap();
    assert_eq!(response.message, b"remote");
    assert_eq!(g.router.active_calls(), 0);
}

#[test]
fn an_unknown_method_is_unimplemented() {
    let mut g = Gateway::new(script());
    let id = g
        .caller
        .start_unary_with("/test.Nobody/Here", b"x", &CallOptions::default())
        .unwrap();
    g.settle();
    let status = g.caller.take_response(id).unwrap().unwrap_err();
    assert_eq!(status.code, Code::Unimplemented);
}

#[test]
fn a_path_served_twice_fails_the_build_in_either_order() {
    let local_first = Router::builder()
        .with_service(Local::default())
        .with_route(LOCAL_CLASH, Client::new(ClientConfig::default()))
        .build();
    assert_eq!(
        local_first.err(),
        Some(RouteConflict {
            path: "/test.Local/Upper"
        })
    );
    let route_first = Router::builder()
        .with_route(LOCAL_CLASH, Client::new(ClientConfig::default()))
        .with_service(Local::default())
        .build();
    assert_eq!(
        route_first.err(),
        Some(RouteConflict {
            path: "/test.Local/Upper"
        })
    );
    let two_routes = Router::builder()
        .with_route(REMOTE, Client::new(ClientConfig::default()))
        .with_route(REMOTE, Client::new(ClientConfig::default()))
        .build();
    assert!(two_routes.is_err());
}

/// A forwarded method that clashes with [`LOCAL`]'s `Upper`.
const LOCAL_CLASH: &[MethodDescriptor] = &[MethodDescriptor::new(
    "/test.Local/Upper",
    MethodKind::Unary,
    0,
)];

#[test]
fn downstream_status_and_metadata_reach_the_caller() {
    let status =
        Status::new(Code::PermissionDenied, "no").with_metadata(metadata(&[("x-why", "policy")]));
    let mut g = Gateway::new(Script {
        status: Err(status),
        ..script()
    });
    let id = g
        .caller
        .start_streaming_with(REMOTE_STREAM, &CallOptions::default())
        .unwrap();
    g.caller.send_message(id, b"req").unwrap();
    g.caller.close_send(id).unwrap();
    g.settle();
    let (messages, done) = outcome(&mut g, id);
    assert!(messages.is_empty());
    let status = done.unwrap().unwrap_err();
    assert_eq!(status.code, Code::PermissionDenied);
    assert_eq!(status.metadata.get("x-why"), Some("policy"));
}

#[test]
fn local_bidi_and_forwarded_streams_interleave_on_one_connection() {
    let mut g = Gateway::new(Script {
        replies: alloc::vec![b"r1".to_vec(), b"r2".to_vec()],
        ..script()
    });
    let echo = g
        .caller
        .start_streaming_with("/test.Local/Echo", &CallOptions::default())
        .unwrap();
    let stream = g
        .caller
        .start_streaming_with(REMOTE_STREAM, &CallOptions::default())
        .unwrap();
    g.caller.send_message(echo, b"a").unwrap();
    g.caller.send_message(stream, b"req").unwrap();
    g.caller.close_send(stream).unwrap();
    g.settle();
    g.caller.send_message(echo, b"b").unwrap();
    g.caller.close_send(echo).unwrap();
    g.settle();
    let (echoed, done) = outcome(&mut g, echo);
    assert_eq!(echoed, [b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(done, Some(Ok(())));
    let (replies, done) = outcome(&mut g, stream);
    assert_eq!(replies, [b"r1".to_vec(), b"r2".to_vec()]);
    assert_eq!(done, Some(Ok(())));
}

#[test]
fn gateway_cancellation_reaches_the_downstream_call() {
    let mut g = Gateway::new(Script {
        hold: true,
        ..script()
    });
    let id = g
        .caller
        .start_streaming_with(REMOTE_STREAM, &CallOptions::default())
        .unwrap();
    g.caller.send_message(id, b"req").unwrap();
    g.caller.close_send(id).unwrap();
    g.settle();
    g.caller.cancel(id);
    g.settle();
    assert_eq!(g.service.cancelled, 1);
    assert_eq!(g.router.active_calls(), 0);
}

#[test]
fn downstream_failure_ends_forwarded_calls_unavailable() {
    let mut g = Gateway::new(Script {
        hold: true,
        ..script()
    });
    let id = g
        .caller
        .start_streaming_with(REMOTE_STREAM, &CallOptions::default())
        .unwrap();
    g.caller.send_message(id, b"req").unwrap();
    g.caller.close_send(id).unwrap();
    g.settle();
    g.router
        .backend(0)
        .unwrap()
        .fail_all(Status::unavailable("gone"));
    g.settle();
    let (_, done) = outcome(&mut g, id);
    assert_eq!(done.unwrap().unwrap_err().code, Code::Unavailable);
}

#[test]
fn forwarded_call_keeps_its_deadline_budget() {
    let mut g = Gateway::new(Script {
        replies: alloc::vec![b"ok".to_vec()],
        ..script()
    });
    g.router.tick(Duration::from_millis(100));
    g.caller.tick(Duration::from_millis(100));
    g.backend.tick(Duration::from_millis(100), &mut g.service);
    let id = g
        .caller
        .start_streaming_with(
            REMOTE_STREAM,
            &CallOptions::timeout(Duration::from_millis(900)),
        )
        .unwrap();
    g.caller.send_message(id, b"req").unwrap();
    g.caller.close_send(id).unwrap();
    g.settle();
    assert_eq!(
        g.service.deadlines.first().copied().flatten(),
        Some(Duration::from_millis(1000))
    );
    let _ = outcome(&mut g, id);
}

/// A gateway whose forwarded route goes to a hand-written downstream.
struct RawGateway {
    caller: Client,
    router: Router,
    backend: RawBackend,
}

impl RawGateway {
    fn new(downstream_limit: usize) -> Self {
        let downstream = Client::new(ClientConfig {
            max_message_size: downstream_limit,
            ..ClientConfig::default()
        });
        let limit = 64 * 1024;
        Self {
            caller: Client::new(ClientConfig {
                max_message_size: limit,
                ..ClientConfig::default()
            }),
            router: Router::builder()
                .with_config(ServerConfig {
                    max_message_size: limit,
                    ..ServerConfig::default()
                })
                .with_service(Local::default())
                .with_route(REMOTE, downstream)
                .build()
                .unwrap(),
            backend: RawBackend::new(16 * 1024),
        }
    }

    /// Move bytes between the caller, the router and the backend until nothing
    /// moves.
    fn settle(&mut self) {
        for _ in 0..256 {
            let mut progress = false;
            let out = self.caller.take_output();
            if !out.is_empty() {
                progress = true;
                self.router.recv(&out).unwrap();
            }
            self.router.poll(&mut Context::from_waker(Waker::noop()));
            let out = self.router.take_output();
            if !out.is_empty() {
                progress = true;
                self.caller.recv(&out).unwrap();
            }
            let out = self.router.backend(0).unwrap().take_output();
            if !out.is_empty() {
                progress = true;
                self.backend.recv(&out);
            }
            let out = self.backend.take_output();
            if !out.is_empty() {
                progress = true;
                self.router.backend_recv(0, &out).unwrap();
            }
            if !progress {
                return;
            }
        }
        panic!("gateway did not settle");
    }
}

#[test]
fn a_held_request_cannot_replace_the_backend_status_through_the_router() {
    let mut g = RawGateway::new(16 * 1024);
    g.settle();
    let id = g
        .caller
        .start_streaming_with(BIDI, &CallOptions::default())
        .unwrap();
    // The backend's window holds 16 KiB, and the last request is larger than
    // the downstream client accepts.
    for size in [12 * 1024, 12 * 1024, 20 * 1024] {
        g.caller.send_message(id, &alloc::vec![7u8; size]).unwrap();
    }
    g.settle();
    g.backend.answer_head();
    g.backend.answer_end(DENIED);
    g.settle();
    let (messages, result) = drain(&mut g.caller, id);
    assert!(messages.is_empty());
    let status = result.unwrap().unwrap_err();
    assert_eq!(status.code, Code::PermissionDenied);
    assert_eq!(status.message, "denied");
    assert_eq!(status.metadata.get("x-trail"), Some("t"));
    assert_eq!(g.router.active_calls(), 0);
}

/// A gateway whose forwarded route is a pool of two downstreams: member `0`
/// has `flooded_limit` bytes of output and is driven by hand, member `1` is a
/// real server.
struct PoolGateway {
    caller: Client,
    router: Router,
    backend: Server,
    service: Service,
}

impl PoolGateway {
    fn new(script: Script, flooded_limit: usize) -> Self {
        let flooded = Client::new(ClientConfig {
            http2: crate::http2::Config {
                max_pending_output_bytes: flooded_limit,
                ..ClientConfig::default().http2
            },
            ..ClientConfig::default()
        });
        let members = alloc::vec![flooded, Client::new(ClientConfig::default())];
        Self {
            caller: Client::new(ClientConfig::default()),
            router: Router::builder()
                .with_service(Local::default())
                .with_pool(REMOTE, members, usize::MAX)
                .build()
                .unwrap(),
            backend: Server::new(ServerConfig::default()),
            service: Service {
                script,
                ..Service::default()
            },
        }
    }

    /// Move bytes between the caller, the router and member `1` until nothing
    /// moves. Member `0` is not served.
    fn settle(&mut self) {
        for _ in 0..256 {
            let mut progress = false;
            let out = self.caller.take_output();
            if !out.is_empty() {
                progress = true;
                self.router.recv(&out).unwrap();
            }
            self.router.poll(&mut Context::from_waker(Waker::noop()));
            let out = self.router.take_output();
            if !out.is_empty() {
                progress = true;
                self.caller.recv(&out).unwrap();
            }
            let out = self.router.backend(1).unwrap().take_output();
            if !out.is_empty() {
                progress = true;
                self.backend.recv(&out, &mut self.service).unwrap();
            }
            self.backend
                .poll(&mut self.service, &mut Context::from_waker(Waker::noop()));
            let out = self.backend.take_output();
            if !out.is_empty() {
                progress = true;
                self.router.backend_recv(1, &out).unwrap();
            }
            if !progress {
                return;
            }
        }
        panic!("gateway did not settle");
    }
}

#[test]
fn output_limit_backend_failure_is_isolated() {
    // Member 0 answers a SETTINGS frame and then PINGs, one 17-byte ACK each,
    // into an output that has room for three of them.
    let handshake = Client::new(ClientConfig::default()).pending_output().len();
    let limit = handshake + 9 + 3 * 17 + 10;
    let mut g = PoolGateway::new(
        Script {
            replies: alloc::vec![b"remote".to_vec()],
            ..script()
        },
        limit,
    );
    let ping = |n: u8| [0, 0, 8, 6, 0, 0, 0, 0, 0, n, n, n, n, n, n, n, n];
    g.router
        .backend_recv(0, &[0, 0, 0, 4, 0, 0, 0, 0, 0])
        .unwrap();
    for n in 0..3 {
        g.router.backend_recv(0, &ping(n)).unwrap();
    }
    let refused = g.router.backend_recv(0, &ping(3));
    assert!(matches!(
        refused,
        Err(crate::http2::Error::OutputLimitExceeded { limit: l, .. }) if l == limit
    ));
    // The driver retires the member it failed on, and only that one.
    g.router
        .backend_failed(0, Status::unavailable("downstream output limit exceeded"));
    assert!(g.router.backend_failure(0).is_some());
    assert!(g.router.backend_failure(1).is_none());
    assert!(g.router.gateway_failure().is_none());
    assert!(!g.router.is_closed());

    // A new call goes to the other member, and succeeds.
    let id = g
        .caller
        .start_unary_with(REMOTE_UNARY, b"ping", &CallOptions::default())
        .unwrap();
    g.settle();
    let response = g.caller.take_response(id).unwrap().unwrap();
    assert_eq!(response.message, b"remote");
}

#[test]
fn router_recv_returns_local_output_limit() {
    // A local unary call answers with a message that does not fit the gateway's cap.
    let limit = 1024;
    let mut caller = Client::new(ClientConfig::default());
    let mut router = Router::builder()
        .with_config(ServerConfig {
            http2: crate::http2::Config {
                max_pending_output_bytes: limit,
                ..ServerConfig::default().http2
            },
            ..ServerConfig::default()
        })
        .with_service(Local::default())
        .build()
        .unwrap();
    caller
        .start_unary_with("/test.Local/Upper", &[b'a'; 2000], &CallOptions::default())
        .unwrap();
    let result = router.recv(&caller.take_output());
    assert!(matches!(
        result,
        Err(crate::http2::Error::OutputLimitExceeded { limit: l, .. }) if l == limit
    ));
    assert!(router.gateway_failure().is_some());
    assert!(router.is_closed());
}

#[test]
fn relay_poll_output_limit_retires_only_failed_lane() {
    // Member 0 has room for the forwarded call's headers but not its body, and
    // its output is never drained. The relay's failed send ends that call, and
    // only member 0 is retired: a later call goes to member 1, and local
    // services keep working.
    let handshake = Client::new(ClientConfig::default()).pending_output().len();
    let mut g = PoolGateway::new(
        Script {
            replies: alloc::vec![b"remote".to_vec()],
            ..script()
        },
        handshake + 400,
    );
    let forwarded = g
        .caller
        .start_unary_with(REMOTE_UNARY, &[7; 2000], &CallOptions::default())
        .unwrap();
    g.settle();
    let status = g.caller.take_response(forwarded).unwrap().unwrap_err();
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("output limit"),
        "{}",
        status.message
    );

    assert!(g.router.backend_failure(0).is_some());
    assert!(g.router.backend_failure(1).is_none());
    assert!(g.router.gateway_failure().is_none());
    assert!(!g.router.is_closed());

    let later = g
        .caller
        .start_unary_with(REMOTE_UNARY, b"ping", &CallOptions::default())
        .unwrap();
    let local = g
        .caller
        .start_unary_with("/test.Local/Upper", b"hi", &CallOptions::default())
        .unwrap();
    g.settle();
    assert_eq!(
        g.caller.take_response(later).unwrap().unwrap().message,
        b"remote"
    );
    assert_eq!(
        g.caller.take_response(local).unwrap().unwrap().message,
        b"HI"
    );
}

/// A local service whose streaming call never answers. It counts the
/// cancellations of its calls in a counter the test keeps.
struct Hold {
    cancelled: Arc<AtomicUsize>,
}

const HOLD: &[MethodDescriptor] = &[MethodDescriptor::new(
    "/test.Local/Hold",
    MethodKind::BidiStreaming,
    0,
)];

impl Handler for Hold {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(HOLD, path)
    }

    fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
        Ok(())
    }

    fn poll_response(
        &mut self,
        _: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        Poll::Pending
    }

    fn on_cancel(&mut self, _: &mut CallContext<'_>) {
        self.cancelled.fetch_add(1, SeqCst);
    }
}

impl Dispatcher for Hold {
    fn methods(&self) -> &[MethodDescriptor] {
        HOLD
    }
}

/// A gateway that serves `Local` and `Hold` itself, forwards `REMOTE` to a
/// downstream that answers with `reply`, and caps its own output at 1 KiB.
fn capped_gateway(cancelled: Arc<AtomicUsize>, reply: Vec<u8>) -> Gateway {
    let config = ServerConfig {
        http2: crate::http2::Config {
            max_pending_output_bytes: 1024,
            ..ServerConfig::default().http2
        },
        ..ServerConfig::default()
    };
    Gateway {
        caller: Client::new(ClientConfig::default()),
        router: Router::builder()
            .with_config(config)
            .with_service(Local::default())
            .with_service(Hold { cancelled })
            .with_route(REMOTE, Client::new(ClientConfig::default()))
            .build()
            .unwrap(),
        backend: Server::new(ServerConfig::default()),
        service: Service {
            script: Script {
                replies: alloc::vec![reply],
                ..script()
            },
            ..Service::default()
        },
    }
}

/// Move bytes between the caller, the router and the downstream until the
/// gateway fails. Nothing more is given to the gateway after that, so the
/// test sees what the failing operation itself left behind.
fn run_until_gateway_fails(g: &mut Gateway) {
    for _ in 0..256 {
        let out = g.caller.take_output();
        if !out.is_empty() {
            g.router.recv(&out).unwrap();
        }
        g.router.poll(&mut Context::from_waker(Waker::noop()));
        let out = g.router.take_output();
        if !out.is_empty() {
            g.caller.recv(&out).unwrap();
        }
        let out = g.router.backend(0).unwrap().take_output();
        if !out.is_empty() {
            g.backend.recv(&out, &mut g.service).unwrap();
        }
        g.backend
            .poll(&mut g.service, &mut Context::from_waker(Waker::noop()));
        let out = g.backend.take_output();
        if !out.is_empty() {
            g.router.backend_recv(0, &out).unwrap();
            if g.router.gateway_failure().is_some() {
                return;
            }
        }
    }
    panic!("the gateway did not fail");
}

#[test]
fn relay_gateway_overflow_cancels_local_calls() {
    // The downstream's 2,000-byte answer does not fit the gateway's cap, and
    // the relay that writes it fails the gateway while a local call is active.
    let cancelled = Arc::default();
    let mut g = capped_gateway(Arc::clone(&cancelled), alloc::vec![b'r'; 2000]);
    let _hold = g.caller.start_streaming(HOLD[0].path).unwrap();
    g.caller
        .start_unary_with(REMOTE_UNARY, b"ping", &CallOptions::default())
        .unwrap();
    run_until_gateway_fails(&mut g);

    assert!(g.router.gateway_failure().is_some());
    assert_eq!(
        cancelled.load(SeqCst),
        1,
        "the local call was cancelled once, without a poll or a tick"
    );
    assert_eq!(g.router.active_calls(), 0, "no call is stranded");

    // Later polls and cancellation do not cancel it again.
    g.router.poll(&mut Context::from_waker(Waker::noop()));
    g.router.cancel_all();
    assert_eq!(cancelled.load(SeqCst), 1);
}

#[test]
fn local_overflow_during_poll_cancels_every_local_call() {
    // A local answer that does not fit the cap is produced by the poll, which
    // cancels the other local call too.
    let cancelled = Arc::default();
    let mut g = capped_gateway(Arc::clone(&cancelled), Vec::new());
    let hold = g.caller.start_streaming(HOLD[0].path).unwrap();
    g.caller.send_message(hold, b"hold").unwrap();
    let echo = g.caller.start_streaming(LOCAL[1].path).unwrap();
    g.caller.send_message(echo, &[7; 2000]).unwrap();
    g.router.recv(&g.caller.take_output()).unwrap();
    assert!(g.router.gateway_failure().is_none());

    g.router.poll(&mut Context::from_waker(Waker::noop()));
    assert!(g.router.gateway_failure().is_some());
    assert_eq!(cancelled.load(SeqCst), 1);
    assert_eq!(g.router.active_calls(), 0);
}

const SPIN: &str = "/test.Probe/Spin";
const STOP: &str = "/test.Probe/Stop";
const PROBE: &[MethodDescriptor] = &[
    MethodDescriptor::new(SPIN, MethodKind::BidiStreaming, 0),
    MethodDescriptor::new(STOP, MethodKind::BidiStreaming, 1),
];

/// A local service whose calls always have a response ready. `Spin` never
/// ends, and `Stop` answers once and then ends. Every poll is logged by call.
#[derive(Default)]
struct Probe {
    polled: Arc<Mutex<Vec<StreamId>>>,
    answered: BTreeSet<StreamId>,
}

impl Handler for Probe {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(PROBE, path)
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        self.polled.lock().unwrap().push(ctx.id);
        if ctx.path == STOP && !self.answered.insert(ctx.id) {
            return Poll::Ready(Next::Done(Ok(())));
        }
        Poll::Ready(Next::Message(b"x".to_vec()))
    }
}

impl Dispatcher for Probe {
    fn methods(&self) -> &[MethodDescriptor] {
        PROBE
    }
}

#[test]
fn local_poll_passes_rotate_their_starting_call_like_a_server() {
    let probe = Probe::default();
    let polled = Arc::clone(&probe.polled);
    let mut router = Router::builder().with_service(probe).build().unwrap();
    let mut caller = Client::new(ClientConfig::default());
    for _ in 0..3 {
        caller.start_streaming(SPIN).unwrap();
    }
    router.recv(&caller.take_output()).unwrap();

    let mut ring: Vec<StreamId> = Vec::new();
    for pass in 0..4 {
        polled.lock().unwrap().clear();
        router.poll(&mut Context::from_waker(Waker::noop()));
        caller.recv(&router.take_output()).unwrap();
        let mut order = polled.lock().unwrap().clone();
        order.dedup();
        assert_eq!(order.len(), 3, "pass {pass}");
        // The first pass runs in id order; each later pass starts one call on.
        if ring.is_empty() {
            ring = order.clone();
            ring.sort_unstable();
        }
        let expected: Vec<StreamId> = ring
            .iter()
            .cycle()
            .skip(pass % ring.len())
            .take(ring.len())
            .copied()
            .collect();
        assert_eq!(order, expected, "pass {pass}");
    }
    assert_eq!(router.active_calls(), 3);
}

#[test]
fn local_calls_finishing_mid_pass_leave_the_rest_of_the_pass_intact() {
    let probe = Probe::default();
    let polled = Arc::clone(&probe.polled);
    let mut router = Router::builder().with_service(probe).build().unwrap();
    let mut caller = Client::new(ClientConfig::default());
    let first = caller.start_streaming(STOP).unwrap();
    let busy = caller.start_streaming(SPIN).unwrap();
    let last = caller.start_streaming(STOP).unwrap();
    router.recv(&caller.take_output()).unwrap();

    // Each `Stop` finishes as the pass reaches it, and the `Spin` call in
    // between still gets its whole quantum.
    router.poll(&mut Context::from_waker(Waker::noop()));
    caller.recv(&router.take_output()).unwrap();
    assert_eq!(router.active_calls(), 1);
    for id in [first, last] {
        let (messages, status) = drain(&mut caller, id);
        assert_eq!(messages, [b"x".to_vec()]);
        assert_eq!(status, Some(Ok(())));
    }

    polled.lock().unwrap().clear();
    router.poll(&mut Context::from_waker(Waker::noop()));
    assert_eq!(polled.lock().unwrap().len(), CALL_QUANTUM);
    caller.recv(&router.take_output()).unwrap();
    assert!(caller.is_pending(busy));
    assert_eq!(router.active_calls(), 1);
}
