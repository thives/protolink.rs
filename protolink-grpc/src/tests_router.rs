//! Deterministic tests of a router serving one gateway connection from a local
//! service and from one downstream service. The caller, gateway and downstream
//! exchange bytes in memory.

extern crate std;

use alloc::vec::Vec;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use super::*;
use crate::dispatch::Dispatcher;
use crate::test_support::drain;
use crate::tests_relay::{
    BIDI, DENIED, METHODS as REMOTE, RawBackend, Script, Service, metadata, script,
};

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
