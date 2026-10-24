//! Deterministic tests of one relay between a caller, a gateway and one
//! downstream service. All three peers are sans-IO and exchange bytes in
//! memory, so every ordering below is reproducible.

extern crate std;

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use super::*;
use crate::lifecycle::{CallEvent, Lifecycle};
use crate::relay::Relay;
use crate::test_support::{drain, events, hf};
use protolink_http2::{Config, Connection, Event, FlowControl, StreamId};

const UNARY: &str = "/test.Relay/Unary";
const SERVER: &str = "/test.Relay/Server";
const CLIENT: &str = "/test.Relay/Client";
pub(crate) const BIDI: &str = "/test.Relay/Bidi";

pub(crate) const METHODS: &[MethodDescriptor] = &[
    MethodDescriptor::new(UNARY, MethodKind::Unary, 0),
    MethodDescriptor::new(SERVER, MethodKind::ServerStreaming, 1),
    MethodDescriptor::new(CLIENT, MethodKind::ClientStreaming, 2),
    MethodDescriptor::new(BIDI, MethodKind::BidiStreaming, 3),
];

/// How the downstream service answers.
#[derive(Clone)]
pub(crate) struct Script {
    /// Response headers.
    pub(crate) initial: Metadata,
    /// Response trailers, sent with an OK status.
    pub(crate) trailing: Metadata,
    /// Response messages, in order.
    pub(crate) replies: Vec<Vec<u8>>,
    /// Final status.
    pub(crate) status: Result<(), Status>,
    /// Bidirectional: answer every request message with itself.
    pub(crate) echo: bool,
    /// Pend (after sending headers) until [`Service::released`] is set.
    pub(crate) hold: bool,
}

impl Default for Script {
    fn default() -> Self {
        Self {
            initial: Metadata::new(),
            trailing: Metadata::new(),
            replies: Vec::new(),
            status: Ok(()),
            echo: false,
            hold: false,
        }
    }
}

/// The downstream service, and what it saw.
#[derive(Default)]
pub(crate) struct Service {
    pub(crate) script: Script,
    pub(crate) released: bool,
    pub(crate) started: bool,
    pub(crate) half_closed: bool,
    pub(crate) requests: Vec<Vec<u8>>,
    /// The deadline each request arrived with.
    pub(crate) deadlines: Vec<Option<Duration>>,
    pub(crate) cancelled: usize,
    pub(crate) pending: VecDeque<Vec<u8>>,
}

impl Handler for Service {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(METHODS, path)
    }

    fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        self.requests.push(request.to_vec());
        self.deadlines.push(ctx.deadline);
        if let Some(initial) = ctx.initial_metadata_mut() {
            *initial = self.script.initial.clone();
        }
        *ctx.trailing_metadata_mut() = self.script.trailing.clone();
        match &self.script.status {
            Ok(()) => Ok(self.script.replies.first().cloned().unwrap_or_default()),
            Err(status) => Err(status.clone()),
        }
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        self.requests.push(message.to_vec());
        self.deadlines.push(ctx.deadline);
        if self.script.echo {
            self.pending.push_back(message.to_vec());
        }
        Ok(())
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        self.half_closed = true;
        if ctx.route.kind == MethodKind::ClientStreaming {
            self.pending.extend(self.script.replies.clone());
        }
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        if !self.started {
            self.started = true;
            if let Some(initial) = ctx.initial_metadata_mut() {
                *initial = self.script.initial.clone();
            }
            if ctx.route.kind == MethodKind::ServerStreaming {
                self.pending.extend(self.script.replies.clone());
            }
        }
        if self.script.hold && !self.released {
            return Poll::Pending;
        }
        if let Some(message) = self.pending.pop_front() {
            return Poll::Ready(Next::Message(message));
        }
        if !self.half_closed {
            return Poll::Pending;
        }
        *ctx.trailing_metadata_mut() = self.script.trailing.clone();
        Poll::Ready(Next::Done(self.script.status.clone()))
    }

    fn on_cancel(&mut self, _: &mut CallContext<'_>) {
        self.cancelled += 1;
    }
}

fn route(path: &str) -> Option<Route> {
    Route::find(METHODS, path)
}

/// Caller, gateway and downstream, with the relays between the gateway and the
/// downstream.
struct Harness {
    caller: Client,
    gateway: Lifecycle<Route>,
    relays: Vec<Relay>,
    downstream: Client,
    backend: Server,
    service: Service,
    now: Duration,
}

impl Harness {
    fn new(script: Script) -> Self {
        let config = ClientConfig {
            max_message_size: 64 * 1024,
            ..ClientConfig::default()
        };
        Self {
            caller: Client::new(config.clone()),
            gateway: Lifecycle::new(ServerConfig {
                max_message_size: 64 * 1024,
                ..ServerConfig::default()
            }),
            relays: Vec::new(),
            downstream: Client::new(config),
            backend: Server::new(ServerConfig {
                max_message_size: 64 * 1024,
                http2: Config {
                    flow_control: FlowControl::Manual,
                    initial_window_size: 16 * 1024,
                    connection_window_size: 16 * 1024,
                    ..Config::default()
                },
                ..ServerConfig::default()
            }),
            service: Service {
                script,
                ..Service::default()
            },
            now: Duration::ZERO,
        }
    }

    /// Move bytes and pump the relays until nothing moves.
    fn settle(&mut self) {
        for _ in 0..256 {
            let mut progress = false;
            let out = self.caller.take_output();
            if !out.is_empty() {
                progress = true;
                self.gateway.recv(&out, route).unwrap();
            }
            while let Some(event) = self.gateway.poll_event() {
                if let CallEvent::Started {
                    id,
                    key,
                    path,
                    metadata,
                    ..
                } = event
                {
                    self.relays.push(Relay::new(id, path, key.kind, metadata));
                }
            }
            for relay in &mut self.relays {
                progress |= relay.pump(&mut self.gateway, &mut self.downstream);
            }
            self.relays.retain(|relay| !relay.is_done());
            let out = self.gateway.take_output();
            if !out.is_empty() {
                progress = true;
                self.caller.recv(&out).unwrap();
            }
            let out = self.downstream.take_output();
            if !out.is_empty() {
                progress = true;
                self.backend.recv(&out, &mut self.service).unwrap();
            }
            self.backend
                .poll(&mut self.service, &mut Context::from_waker(Waker::noop()));
            let out = self.backend.take_output();
            if !out.is_empty() {
                progress = true;
                self.downstream.recv(&out).unwrap();
            }
            if !progress {
                return;
            }
        }
        panic!("relay did not settle");
    }

    /// Exchange the downstream connection's preface and SETTINGS, so that the
    /// downstream client knows the backend's flow-control window.
    fn exchange_downstream(&mut self) {
        for _ in 0..4 {
            let out = self.downstream.take_output();
            if !out.is_empty() {
                self.backend.recv(&out, &mut self.service).unwrap();
            }
            let out = self.backend.take_output();
            if !out.is_empty() {
                self.downstream.recv(&out).unwrap();
            }
        }
    }

    /// Advance the gateway and downstream clocks (not the caller's) and settle.
    fn tick_gateway(&mut self, now: Duration) {
        self.now = now;
        self.gateway.tick(now);
        self.downstream.tick(now);
        self.backend.tick(now, &mut self.service);
        self.settle();
    }

    fn start_streaming(&mut self, path: &str, options: CallOptions) -> CallId {
        let id = self.caller.start_streaming_with(path, &options).unwrap();
        self.settle();
        id
    }

    /// A server-streaming call with its one request message, half-closed.
    fn start_server_stream(&mut self, options: CallOptions) -> CallId {
        let id = self.start_streaming(SERVER, options);
        self.caller.send_message(id, b"req").unwrap();
        self.caller.close_send(id).unwrap();
        self.settle();
        id
    }

    /// Every call the gateway and downstream still track has ended.
    fn assert_released(&self) {
        assert_eq!(self.gateway.active_calls(), 0, "gateway call state leaked");
        assert!(self.relays.is_empty(), "relay outlived its call");
        assert_eq!(self.backend.active_calls(), 0, "backend call state leaked");
    }
}

pub(crate) fn metadata(pairs: &[(&str, &str)]) -> Metadata {
    let mut metadata = Metadata::new();
    for (key, value) in pairs {
        metadata.insert(key, value).unwrap();
    }
    metadata
}

pub(crate) fn script() -> Script {
    Script::default()
}

#[test]
fn unary_call_forwards_request_response_headers_and_trailers() {
    let mut h = Harness::new(Script {
        initial: metadata(&[("x-head", "h")]),
        trailing: metadata(&[("x-tail", "t")]),
        replies: alloc::vec![b"pong".to_vec()],
        ..script()
    });
    let id = h
        .caller
        .start_unary_with(
            UNARY,
            b"ping",
            &CallOptions::metadata(metadata(&[("x-req", "r")])),
        )
        .unwrap();
    h.settle();
    let response = h.caller.take_response(id).unwrap().unwrap();
    assert_eq!(response.message, b"pong");
    assert_eq!(response.headers.get("x-head"), Some("h"));
    assert_eq!(response.trailers.get("x-tail"), Some("t"));
    assert_eq!(h.service.requests, [b"ping".to_vec()]);
    h.assert_released();
}

#[test]
fn server_streaming_forwards_messages_in_order_then_status() {
    let mut h = Harness::new(Script {
        replies: alloc::vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        ..script()
    });
    let id = h.start_streaming(SERVER, CallOptions::default());
    h.caller.send_message(id, b"req").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    let (messages, done) = drain(&mut h.caller, id);
    assert_eq!(messages, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(done, Some(Ok(())));
    h.assert_released();
}

#[test]
fn client_streaming_forwards_requests_in_order_and_single_response() {
    let mut h = Harness::new(Script {
        replies: alloc::vec![b"sum".to_vec()],
        ..script()
    });
    let id = h.start_streaming(CLIENT, CallOptions::default());
    for message in [b"1", b"2", b"3"] {
        h.caller.send_message(id, message).unwrap();
    }
    h.caller.close_send(id).unwrap();
    h.settle();
    assert_eq!(
        h.service.requests,
        [b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]
    );
    let (messages, done) = drain(&mut h.caller, id);
    assert_eq!(messages, [b"sum".to_vec()]);
    assert_eq!(done, Some(Ok(())));
    h.assert_released();
}

#[test]
fn bidi_call_echoes_in_order_across_settles() {
    let mut h = Harness::new(Script {
        echo: true,
        ..script()
    });
    let id = h.start_streaming(BIDI, CallOptions::default());
    h.caller.send_message(id, b"m1").unwrap();
    h.settle();
    h.caller.send_message(id, b"m2").unwrap();
    h.caller.send_message(id, b"m3").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    let (messages, done) = drain(&mut h.caller, id);
    assert_eq!(messages, [b"m1".to_vec(), b"m2".to_vec(), b"m3".to_vec()]);
    assert_eq!(done, Some(Ok(())));
    h.assert_released();
}

#[test]
fn empty_request_streams_are_half_closed_without_messages() {
    let mut h = Harness::new(Script {
        replies: alloc::vec![b"empty".to_vec()],
        ..script()
    });
    let client = h.start_streaming(CLIENT, CallOptions::default());
    h.caller.close_send(client).unwrap();
    h.settle();
    assert_eq!(
        drain(&mut h.caller, client),
        (alloc::vec![b"empty".to_vec()], Some(Ok(())))
    );

    let bidi = h.start_streaming(BIDI, CallOptions::default());
    h.caller.close_send(bidi).unwrap();
    h.settle();
    assert_eq!(drain(&mut h.caller, bidi), (Vec::new(), Some(Ok(()))));
    h.assert_released();
}

#[test]
fn response_headers_arrive_before_the_first_message() {
    let mut h = Harness::new(Script {
        initial: metadata(&[("x-head", "early")]),
        replies: alloc::vec![b"late".to_vec()],
        hold: true,
        ..script()
    });
    let id = h.start_streaming(SERVER, CallOptions::default());
    h.caller.send_message(id, b"req").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    assert_eq!(
        h.caller.response_headers(id).and_then(|m| m.get("x-head")),
        Some("early")
    );
    assert_eq!(h.caller.try_next(id), None, "no message before release");
    h.service.released = true;
    h.settle();
    let (messages, done) = drain(&mut h.caller, id);
    assert_eq!(messages, [b"late".to_vec()]);
    assert_eq!(done, Some(Ok(())));
    h.assert_released();
}

#[test]
fn messages_before_a_failing_status_are_all_forwarded() {
    let status = Status::new(Code::Aborted, "boom").with_metadata(metadata(&[("x-err", "e")]));
    let mut h = Harness::new(Script {
        replies: alloc::vec![b"a".to_vec(), b"b".to_vec()],
        status: Err(status),
        ..script()
    });
    let id = h.start_streaming(SERVER, CallOptions::default());
    h.caller.send_message(id, b"req").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    let (messages, done) = drain(&mut h.caller, id);
    assert_eq!(messages, [b"a".to_vec(), b"b".to_vec()]);
    let status = done.unwrap().unwrap_err();
    assert_eq!(status.code, Code::Aborted);
    assert_eq!(status.message, "boom");
    assert_eq!(status.metadata.get("x-err"), Some("e"));
    h.assert_released();
}

#[test]
fn trailers_only_error_stays_trailers_only() {
    let status = Status::new(Code::NotFound, "nope").with_metadata(metadata(&[("x-tail", "t")]));
    let mut h = Harness::new(Script {
        status: Err(status),
        ..script()
    });
    let id = h.start_streaming(SERVER, CallOptions::default());
    h.caller.send_message(id, b"req").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    assert_eq!(h.caller.response_headers(id), None);
    let (messages, done) = drain(&mut h.caller, id);
    assert!(messages.is_empty());
    let status = done.unwrap().unwrap_err();
    assert_eq!(status.code, Code::NotFound);
    assert_eq!(status.metadata.get("x-tail"), Some("t"));
    h.assert_released();
}

#[test]
fn binary_metadata_survives_both_hops() {
    let mut trailing = Metadata::new();
    trailing.insert_bin("x-trail-bin", &[1, 2, 3]).unwrap();
    let mut h = Harness::new(Script {
        trailing,
        replies: alloc::vec![b"x".to_vec()],
        ..script()
    });
    let mut request = Metadata::new();
    request.insert_bin("x-req-bin", &[9, 8]).unwrap();
    let id = h.start_server_stream(CallOptions::metadata(request));
    let _ = drain(&mut h.caller, id);
    let (_, trailers) = h.caller.take_metadata(id);
    assert_eq!(trailers.get_bin("x-trail-bin"), Some(&[1u8, 2, 3][..]));
    h.assert_released();
}

#[test]
fn downstream_receives_the_remaining_deadline_budget() {
    let mut h = Harness::new(Script {
        replies: alloc::vec![b"ok".to_vec()],
        ..script()
    });
    h.tick_gateway(Duration::from_millis(300));
    h.caller.tick(Duration::from_millis(300));
    let id = h.start_server_stream(CallOptions::timeout(Duration::from_secs(1)));
    // Caller deadline 1.3 s on its clock. Forwarded with the same absolute
    // deadline, not the original one-second timeout restarted at relay time.
    assert_eq!(
        h.service.deadlines.first().copied().flatten(),
        Some(Duration::from_millis(1300))
    );
    let _ = drain(&mut h.caller, id);
    h.assert_released();
}

#[test]
fn deadline_expiry_while_waiting_for_downstream_ends_both_calls() {
    let mut h = Harness::new(Script {
        hold: true,
        ..script()
    });
    h.caller.tick(Duration::ZERO);
    let id = h.start_server_stream(CallOptions::timeout(Duration::from_millis(200)));
    // The caller's own clock is not advanced, so the gateway expires the call.
    h.tick_gateway(Duration::from_millis(250));
    let (messages, done) = drain(&mut h.caller, id);
    assert!(messages.is_empty());
    assert_eq!(done.unwrap().unwrap_err().code, Code::DeadlineExceeded);
    assert_eq!(h.service.cancelled, 1, "downstream was not cancelled");
    assert_eq!(h.service.deadlines, [Some(Duration::from_millis(200))]);
    h.assert_released();
}

#[test]
fn caller_cancellation_cancels_the_downstream_call_once() {
    let mut h = Harness::new(Script {
        hold: true,
        ..script()
    });
    let id = h.start_server_stream(CallOptions::default());
    h.caller.cancel(id);
    h.settle();
    assert_eq!(h.service.cancelled, 1);
    h.assert_released();
}

#[test]
fn downstream_failure_ends_the_incoming_call_with_its_status() {
    let mut h = Harness::new(Script {
        hold: true,
        ..script()
    });
    let id = h.start_server_stream(CallOptions::default());
    h.downstream
        .fail_all(Status::unavailable("backend went away"));
    h.settle();
    // The transport failed: the backend's side of the stream goes with it.
    h.backend.cancel_all(&mut h.service);
    assert_eq!(h.service.cancelled, 1);
    let (_, done) = drain(&mut h.caller, id);
    let status = done.unwrap().unwrap_err();
    assert_eq!(status.code, Code::Unavailable);
    assert_eq!(status.message, "backend went away");
    h.assert_released();
}

#[test]
fn a_busy_downstream_leaves_later_requests_buffered_in_the_gateway() {
    let mut h = Harness::new(Script {
        hold: true,
        ..script()
    });
    h.exchange_downstream();
    let id = h
        .caller
        .start_streaming_with(CLIENT, &CallOptions::default())
        .unwrap();
    // Four messages fit in the gateway's window, so they all arrive whole, but
    // not in the backend's 16 KiB stream window.
    let big = alloc::vec![7u8; 12 * 1024];
    for _ in 0..4 {
        h.caller.send_message(id, &big).unwrap();
    }
    h.caller.close_send(id).unwrap();
    // Only the caller's bytes reach the gateway; the downstream is not flushed.
    let out = h.caller.take_output();
    h.gateway.recv(&out, route).unwrap();
    while let Some(event) = h.gateway.poll_event() {
        if let CallEvent::Started {
            id,
            key,
            path,
            metadata,
            ..
        } = event
        {
            h.relays.push(Relay::new(id, path, key.kind, metadata));
        }
    }
    let relay = &mut h.relays[0];
    relay.pump(&mut h.gateway, &mut h.downstream);
    let downstream = relay.downstream().unwrap();
    // The first message is queued downstream, so no more are taken from the
    // gateway; they stay buffered there and hold back the caller's window.
    assert!(h.downstream.queued_request_bytes(downstream).unwrap() > 0);
    // Two messages went to the downstream (the second only partly fits its
    // window), and the last two still wait in the gateway.
    let buffered = h.gateway.buffered_request_bytes(id).unwrap();
    assert!(
        buffered >= 2 * big.len() && buffered < 4 * big.len(),
        "later messages must stay unconsumed while downstream is busy, got {buffered}"
    );
    h.service.released = true;
    h.settle();
    assert_eq!(h.service.requests.len(), 4);
    h.assert_released();
}

#[test]
fn unary_method_with_two_requests_fails_as_it_does_locally() {
    let mut h = Harness::new(script());
    let id = h.start_streaming(UNARY, CallOptions::default());
    h.caller.send_message(id, b"a").unwrap();
    h.caller.send_message(id, b"b").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    let (_, done) = drain(&mut h.caller, id);
    let status = done.unwrap().unwrap_err();
    assert_eq!(status.code, Code::Internal);
    assert_eq!(
        status.message,
        "more than one request message for unary call"
    );
    assert!(h.service.requests.is_empty());
    h.assert_released();
}

#[test]
fn server_streaming_method_without_a_request_fails() {
    let mut h = Harness::new(script());
    let id = h.start_streaming(SERVER, CallOptions::default());
    h.caller.close_send(id).unwrap();
    h.settle();
    let (_, done) = drain(&mut h.caller, id);
    let status = done.unwrap().unwrap_err();
    assert_eq!(status.code, Code::Internal);
    assert_eq!(status.message, "missing request message");
    h.assert_released();
}

#[test]
fn sequential_calls_release_all_state() {
    let mut h = Harness::new(Script {
        replies: alloc::vec![b"one".to_vec()],
        ..script()
    });
    for _ in 0..2 {
        h.service = Service {
            script: Script {
                replies: alloc::vec![b"one".to_vec()],
                ..script()
            },
            ..Service::default()
        };
        let id = h.start_server_stream(CallOptions::default());
        assert_eq!(drain(&mut h.caller, id).0, [b"one".to_vec()]);
    }
    h.assert_released();
}

// ---------------------------------------------------------------------------
// A hand-written downstream: the test decides what the backend answers and
// when it releases the request bytes it has received.
// ---------------------------------------------------------------------------

/// A downstream service written frame by frame, with manual flow control: it
/// answers only what the test tells it to, and releases request bytes only
/// when asked (it never does by itself).
pub(crate) struct RawBackend {
    conn: Connection,
    /// The stream of the first call, once its request headers arrived.
    call: Option<StreamId>,
}

impl RawBackend {
    /// A backend whose stream and connection windows admit `window` bytes.
    pub(crate) fn new(window: u32) -> Self {
        Self {
            conn: Connection::server(Config {
                flow_control: FlowControl::Manual,
                initial_window_size: window,
                connection_window_size: window,
                ..Config::default()
            }),
            call: None,
        }
    }

    /// Bytes from the downstream client.
    pub(crate) fn recv(&mut self, bytes: &[u8]) {
        self.conn.recv(bytes).unwrap();
        for event in events(&mut self.conn) {
            if let Event::Headers { stream_id, .. } = event {
                self.call.get_or_insert(stream_id);
            }
        }
    }

    /// Bytes for the downstream client.
    pub(crate) fn take_output(&mut self) -> Vec<u8> {
        self.conn.take_output()
    }

    /// The backend's response headers.
    pub(crate) fn answer_head(&mut self) {
        let call = self.call.expect("the backend saw the call");
        let fields = [hf(":status", "200"), hf("content-type", "application/grpc")];
        self.conn
            .send_headers(call, fields.to_vec(), false)
            .unwrap();
    }

    /// One response message of the backend.
    pub(crate) fn answer_message(&mut self, message: &[u8]) {
        let call = self.call.expect("the backend saw the call");
        let framed = crate::lpm::encode(message).unwrap();
        self.conn.send_data(call, framed, false).unwrap();
    }

    /// The backend's trailers, which end its response.
    pub(crate) fn answer_end(&mut self, trailers: &[(&str, &str)]) {
        let call = self.call.expect("the backend saw the call");
        let fields: Vec<_> = trailers.iter().map(|(k, v)| hf(k, v)).collect();
        self.conn.send_headers(call, fields, true).unwrap();
    }
}

/// Caller, gateway and a downstream `Client` talking to a [`RawBackend`].
struct Raw {
    caller: Client,
    gateway: Lifecycle<Route>,
    relays: Vec<Relay>,
    downstream: Client,
    backend: RawBackend,
}

impl Raw {
    /// `caller_window` sets the caller's flow-control window, so that responses
    /// the gateway sends it can back up. `gateway_limit` and `downstream_limit`
    /// are the two message limits.
    fn new(caller_window: Option<u32>, gateway_limit: usize, downstream_limit: usize) -> Self {
        let mut caller = ClientConfig {
            max_message_size: 64 * 1024,
            ..ClientConfig::default()
        };
        if let Some(window) = caller_window {
            caller.http2 = Config {
                flow_control: FlowControl::Manual,
                initial_window_size: window,
                connection_window_size: window,
                ..Config::default()
            };
        }
        let mut raw = Self {
            caller: Client::new(caller),
            gateway: Lifecycle::new(ServerConfig {
                max_message_size: gateway_limit,
                ..ServerConfig::default()
            }),
            relays: Vec::new(),
            downstream: Client::new(ClientConfig {
                max_message_size: downstream_limit,
                ..ClientConfig::default()
            }),
            backend: RawBackend::new(16 * 1024),
        };
        // The backend's window applies only once its SETTINGS have arrived.
        for _ in 0..4 {
            let out = raw.downstream.take_output();
            raw.backend.recv(&out);
            let out = raw.backend.take_output();
            raw.downstream.recv(&out).unwrap();
        }
        raw
    }

    /// Move bytes and pump the relay until nothing moves. The backend records
    /// what it receives and never releases flow-control credit by itself.
    fn settle(&mut self) {
        for _ in 0..256 {
            let mut progress = false;
            let out = self.caller.take_output();
            if !out.is_empty() {
                progress = true;
                self.gateway.recv(&out, route).unwrap();
            }
            while let Some(event) = self.gateway.poll_event() {
                if let CallEvent::Started {
                    id,
                    key,
                    path,
                    metadata,
                    ..
                } = event
                {
                    self.relays.push(Relay::new(id, path, key.kind, metadata));
                }
            }
            for relay in &mut self.relays {
                progress |= relay.pump(&mut self.gateway, &mut self.downstream);
            }
            self.relays.retain(|relay| !relay.is_done());
            let out = self.gateway.take_output();
            if !out.is_empty() {
                progress = true;
                self.caller.recv(&out).unwrap();
            }
            let out = self.downstream.take_output();
            if !out.is_empty() {
                progress = true;
                self.backend.recv(&out);
            }
            let out = self.backend.take_output();
            if !out.is_empty() {
                progress = true;
                self.downstream.recv(&out).unwrap();
            }
            if !progress {
                return;
            }
        }
        panic!("relay did not settle");
    }

    fn answer_head(&mut self) {
        self.backend.answer_head();
    }

    fn answer_message(&mut self, message: &[u8]) {
        self.backend.answer_message(message);
    }

    fn answer_end(&mut self, trailers: &[(&str, &str)]) {
        self.backend.answer_end(trailers);
    }

    /// Everything the caller receives for `id`, settling between takes so the
    /// gateway can send what the caller's window admits.
    fn collect(&mut self, id: CallId) -> (Vec<Vec<u8>>, Result<(), Status>) {
        let mut messages = Vec::new();
        for _ in 0..64 {
            self.settle();
            match self.caller.try_next(id) {
                Some(Next::Message(message)) => messages.push(message),
                Some(Next::Done(result)) => return (messages, result),
                None => {}
            }
        }
        panic!("the caller never saw the end of the call");
    }

    /// Start a bidirectional call, send `messages`, and settle.
    fn start_bidi(&mut self, messages: &[usize]) -> CallId {
        let id = self
            .caller
            .start_streaming_with(BIDI, &CallOptions::default())
            .unwrap();
        for &size in messages {
            self.caller
                .send_message(id, &alloc::vec![7u8; size])
                .unwrap();
        }
        self.settle();
        id
    }
}

/// Trailers of a failed call that carry metadata.
pub(crate) const DENIED: &[(&str, &str)] = &[
    ("grpc-status", "7"),
    ("grpc-message", "denied"),
    ("x-trail", "t"),
];

#[test]
fn a_request_held_behind_flow_control_does_not_replace_the_backend_status() {
    // The backend's window holds the first 16 KiB; the second request waits
    // in the downstream, and the third (larger than the downstream accepts)
    // waits in the gateway.
    let mut h = Raw::new(None, 64 * 1024, 16 * 1024);
    let id = h.start_bidi(&[12 * 1024, 12 * 1024, 20 * 1024]);
    assert!(
        h.downstream
            .queued_request_bytes(h.relays[0].downstream().unwrap())
            .unwrap()
            > 0
    );
    h.answer_head();
    h.answer_end(DENIED);
    let (messages, result) = h.collect(id);
    assert!(messages.is_empty());
    let status = result.unwrap_err();
    assert_eq!(status.code, Code::PermissionDenied);
    assert_eq!(status.message, "denied");
    assert_eq!(status.metadata.get("x-trail"), Some("t"));
    assert_eq!(h.gateway.active_calls(), 0);
    assert!(h.relays.is_empty());
}

#[test]
fn a_held_request_cannot_replace_a_status_while_responses_are_backpressured() {
    // The caller's window admits 8 KiB, so the second response waits in the
    // gateway, and the status waits behind it. The request is still buffered.
    let mut h = Raw::new(Some(8 * 1024), 64 * 1024, 16 * 1024);
    let id = h.start_bidi(&[12 * 1024, 12 * 1024, 20 * 1024]);
    h.answer_head();
    h.answer_message(&[1; 6 * 1024]);
    h.answer_message(&[2; 6 * 1024]);
    h.answer_end(DENIED);
    h.settle();
    let upstream = h.relays[0].upstream();
    assert!(h.gateway.buffered_request_bytes(upstream).unwrap() >= 20 * 1024);
    let (messages, result) = h.collect(id);
    assert_eq!(
        messages,
        [[1u8; 6 * 1024].to_vec(), [2u8; 6 * 1024].to_vec()]
    );
    let status = result.unwrap_err();
    assert_eq!(status.code, Code::PermissionDenied);
    assert_eq!(status.metadata.get("x-trail"), Some("t"));
    assert_eq!(h.gateway.active_calls(), 0);
}

#[test]
fn responses_before_a_completed_status_arrive_in_order_then_the_status() {
    let mut h = Raw::new(None, 64 * 1024, 64 * 1024);
    let id = h
        .caller
        .start_streaming_with(SERVER, &CallOptions::default())
        .unwrap();
    h.caller.send_message(id, b"req").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    h.answer_head();
    h.answer_message(b"one");
    h.answer_message(b"two");
    h.answer_end(&[("grpc-status", "0"), ("x-trail", "t")]);
    let (messages, result) = h.collect(id);
    assert_eq!(messages, [b"one".to_vec(), b"two".to_vec()]);
    assert!(result.is_ok());
    let (_, trailers) = h.caller.take_metadata(id);
    assert_eq!(trailers.get("x-trail"), Some("t"));
}

#[test]
fn a_successful_call_without_its_response_keeps_its_trailers() {
    for (method, message) in [
        (UNARY, "missing response message"),
        (CLIENT, "client streaming call ended without a response"),
    ] {
        let mut h = Raw::new(None, 64 * 1024, 64 * 1024);
        let id = h
            .caller
            .start_streaming_with(method, &CallOptions::default())
            .unwrap();
        h.caller.send_message(id, b"req").unwrap();
        h.caller.close_send(id).unwrap();
        h.settle();
        h.answer_head();
        h.answer_end(&[("grpc-status", "0"), ("x-trail", "t")]);
        let (messages, result) = h.collect(id);
        assert!(messages.is_empty());
        let status = result.unwrap_err();
        assert_eq!(status.code, Code::Internal);
        assert_eq!(status.message, message);
        // Sent once, with the status: the trailers of the OK are not lost.
        assert_eq!(status.metadata.get("x-trail"), Some("t"));
        assert_eq!(h.gateway.active_calls(), 0);
    }
}

#[test]
fn a_unary_call_with_two_responses_fails_after_the_first() {
    let mut h = Raw::new(None, 64 * 1024, 64 * 1024);
    let id = h
        .caller
        .start_streaming_with(UNARY, &CallOptions::default())
        .unwrap();
    h.caller.send_message(id, b"req").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    h.answer_head();
    h.answer_message(b"one");
    h.answer_message(b"two");
    h.answer_end(&[("grpc-status", "0")]);
    let (messages, result) = h.collect(id);
    assert_eq!(messages, [b"one".to_vec()]);
    let status = result.unwrap_err();
    assert_eq!(status.code, Code::Internal);
    assert_eq!(
        status.message,
        "more than one response message for unary call"
    );
    assert_eq!(h.gateway.active_calls(), 0);
}

#[test]
fn a_forwarded_response_is_limited_by_the_gateway_not_the_downstream() {
    // The downstream accepts 64 KiB, the gateway only 1000 bytes.
    let mut h = Raw::new(None, 1000, 64 * 1024);
    let id = h
        .caller
        .start_streaming_with(SERVER, &CallOptions::default())
        .unwrap();
    h.caller.send_message(id, b"req").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    h.answer_head();
    h.answer_message(&[3; 2000]);
    h.answer_end(&[("grpc-status", "0")]);
    let (messages, result) = h.collect(id);
    assert!(messages.is_empty());
    assert_eq!(result.unwrap_err().code, Code::ResourceExhausted);
    assert_eq!(h.gateway.active_calls(), 0);
    assert!(h.relays.is_empty());
}

#[test]
fn a_forwarded_response_at_the_gateway_limit_is_delivered() {
    let mut h = Raw::new(None, 1000, 64 * 1024);
    let id = h
        .caller
        .start_streaming_with(SERVER, &CallOptions::default())
        .unwrap();
    h.caller.send_message(id, b"req").unwrap();
    h.caller.close_send(id).unwrap();
    h.settle();
    h.answer_head();
    h.answer_message(&[3; 1000]);
    h.answer_end(&[("grpc-status", "0")]);
    let (messages, result) = h.collect(id);
    assert_eq!(messages, [[3u8; 1000].to_vec()]);
    assert!(result.is_ok());
}
