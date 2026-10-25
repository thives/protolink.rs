//! Server-only deadline, scheduling, and output-budget regressions.

extern crate std;

use super::*;
use crate::compression::Codec;
use crate::fields::{field, header};
use crate::local::CALL_QUANTUM;
use crate::lpm;
use crate::{CallContext, MethodKind, Next, Status};
use alloc::collections::{BTreeMap, VecDeque};
use alloc::sync::Arc;
use alloc::vec;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::Poll;
use core::task::Waker;
use protolink_http2::{Connection, ErrorCode, Event};
use std::task::Wake;

#[derive(Default)]
struct HandlerState {
    replies: BTreeMap<StreamId, usize>,
    requests: BTreeMap<StreamId, VecDeque<Vec<u8>>>,
    cancelled: Vec<StreamId>,
    cancellation_deadlines: Vec<Option<Duration>>,
    response_size: usize,
}

impl Handler for HandlerState {
    fn route(&self, path: &str) -> Option<Route> {
        let kind = match path {
            "/test/Unary" => MethodKind::Unary,
            "/test/Client" => MethodKind::ClientStreaming,
            "/test/Bidi" => MethodKind::BidiStreaming,
            _ => MethodKind::ServerStreaming,
        };
        Some(Route::new(kind, 0))
    }

    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        Ok(request.to_vec())
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, msg: &[u8]) -> Result<(), Status> {
        self.requests
            .entry(ctx.id)
            .or_default()
            .push_back(msg.to_vec());
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        let replies = self.replies.entry(ctx.id).or_default();
        if ctx.path == "/test/Bidi" {
            return match self.requests.entry(ctx.id).or_default().pop_front() {
                Some(msg) => {
                    *replies += 1;
                    Poll::Ready(Next::Message(msg))
                }
                None => Poll::Pending,
            };
        }
        if ctx.path == "/test/Finite" && *replies > 0 {
            return Poll::Ready(Next::Done(Ok(())));
        }
        *replies += 1;
        Poll::Ready(Next::Message(vec![7; self.response_size]))
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.cancelled.push(ctx.id);
        self.cancellation_deadlines.push(ctx.deadline);
    }
}

struct Peer {
    conn: Connection,
    server: Server,
    handler: HandlerState,
    events: Vec<Event>,
}

impl Peer {
    fn new(window: u32, max_message_size: usize) -> Self {
        let mut peer = Self {
            conn: Connection::client(Config {
                initial_window_size: window,
                flow_control: FlowControl::Manual,
                ..Config::default()
            }),
            server: Server::new(ServerConfig {
                max_message_size,
                ..ServerConfig::default()
            }),
            handler: HandlerState {
                response_size: 20,
                ..HandlerState::default()
            },
            events: Vec::new(),
        };
        peer.exchange();
        peer
    }

    fn open(&mut self, path: &str, body: &[u8], end_stream: bool, deadline: bool) -> StreamId {
        let mut headers = vec![
            field(":method", "POST"),
            field(":scheme", "http"),
            field(":path", path),
            field("content-type", "application/grpc"),
        ];
        if deadline {
            headers.push(field("grpc-timeout", "100m"));
        }
        let id = self.conn.open_stream(headers, false).unwrap();
        self.conn
            .send_data(id, lpm::encode(body).unwrap(), end_stream)
            .unwrap();
        id
    }

    fn recv_requests(&mut self) {
        self.server
            .recv(&self.conn.take_output(), &mut self.handler)
            .unwrap();
    }

    fn poll(&mut self) {
        self.server
            .poll(&mut self.handler, &mut Context::from_waker(Waker::noop()));
    }

    fn recv_responses(&mut self) {
        self.conn.recv(&self.server.take_output()).unwrap();
        self.events
            .extend(core::iter::from_fn(|| self.conn.poll_event()));
    }

    fn exchange(&mut self) {
        for _ in 0..4 {
            self.recv_requests();
            self.poll();
            self.recv_responses();
        }
    }

    fn data_len(&self, id: StreamId) -> usize {
        self.events
            .iter()
            .filter_map(|event| match event {
                Event::Data {
                    stream_id, data, ..
                } if *stream_id == id => Some(data.len()),
                _ => None,
            })
            .sum()
    }

    fn has_trailers(&self, id: StreamId) -> bool {
        self.events.iter().any(|event| {
            matches!(event,
            Event::Headers { stream_id, end_stream: true, .. } if *stream_id == id)
        })
    }
}

struct InvalidResponseMetadata {
    initial: bool,
    value: &'static str,
    reply: bool,
}

impl InvalidResponseMetadata {
    fn set_metadata(&self, ctx: &mut CallContext<'_>) {
        let metadata = if self.initial {
            ctx.initial_metadata_mut().unwrap()
        } else {
            ctx.trailing_metadata_mut()
        };
        // Public insertion refuses boundary whitespace, so bypass it to reach
        // the defensive outbound HTTP/2 validation.
        assert_eq!(
            metadata.insert("x-test", self.value),
            Err(crate::InvalidMetadata::Value)
        );
        metadata.insert_unchecked_for_test("x-test", self.value);
    }
}

impl Handler for InvalidResponseMetadata {
    fn route(&self, path: &str) -> Option<Route> {
        let kind = if path == "/test/EarlyInvalid" {
            MethodKind::BidiStreaming
        } else {
            MethodKind::Unary
        };
        Some(Route::new(kind, 0))
    }

    fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        if ctx.path == "/test/Valid" {
            return Ok(request.to_vec());
        }
        self.set_metadata(ctx);
        if self.reply {
            Ok(request.to_vec())
        } else {
            Err(Status::permission_denied("handler rejected request"))
        }
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        self.set_metadata(ctx);
        Poll::Ready(Next::Done(Err(Status::permission_denied(
            "handler rejected request",
        ))))
    }
}

#[test]
fn invalid_terminal_metadata_resets_immediately_even_with_zero_window_or_open_request() {
    for initial in [true, false] {
        for value in [" value", "value "] {
            for early in [false, true] {
                let mut handler = InvalidResponseMetadata {
                    initial,
                    value,
                    reply: !initial,
                };
                let mut conn = Connection::client(Config {
                    initial_window_size: 0,
                    flow_control: FlowControl::Manual,
                    ..Config::default()
                });
                let mut server = Server::new(ServerConfig::default());
                let path = if early {
                    "/test/EarlyInvalid"
                } else {
                    "/test/Invalid"
                };
                let id = conn
                    .open_stream(
                        vec![
                            field(":method", "POST"),
                            field(":scheme", "http"),
                            field(":path", path),
                            field("content-type", "application/grpc"),
                        ],
                        false,
                    )
                    .unwrap();
                if !early {
                    conn.send_data(id, lpm::encode(b"response").unwrap(), true)
                        .unwrap();
                }
                server.recv(&conn.take_output(), &mut handler).unwrap();
                server.poll(&mut handler, &mut Context::from_waker(Waker::noop()));
                assert_eq!(server.active_calls(), 0);
                assert_eq!(
                    server.queued_response_bytes(id),
                    None,
                    "rejected trailers must discard blocked DATA"
                );
                assert!(!server.core.conn().has_stream(id));
                conn.recv(&server.take_output()).unwrap();
                let events: Vec<_> = core::iter::from_fn(|| conn.poll_event()).collect();
                let resets: Vec<_> = events
                    .iter()
                    .filter_map(|event| match event {
                        Event::Reset {
                            stream_id,
                            error_code,
                        } if *stream_id == id => Some(*error_code),
                        _ => None,
                    })
                    .collect();
                assert_eq!(resets, [ErrorCode::InternalError]);
                assert!(!events.iter().any(|event| matches!(
                    event,
                    Event::Data { .. }
                        | Event::Headers {
                            end_stream: true,
                            ..
                        }
                )));
                assert_eq!(conn.stream_count(), 0);
                assert!(server.core.draining().is_empty());
                assert_eq!(server.next_deadline(), None);
            }
        }
    }
}

#[test]
fn invalid_response_metadata_completes_the_client_with_error_and_other_streams_succeed() {
    use crate::{Client, ClientConfig, Code};

    // `reply` only matters for unary calls: the early streaming call always
    // fails from `poll_response`.
    let cases = [
        ("unary success", false, true),
        ("unary handler error", false, false),
        ("early streaming error", true, false),
    ];
    for initial in [true, false] {
        for value in [" value", "value "] {
            for (name, early, reply) in cases {
                let mut handler = InvalidResponseMetadata {
                    initial,
                    value,
                    reply,
                };
                let mut client = Client::new(ClientConfig::default());
                let mut server = Server::new(ServerConfig::default());
                let bad = if early {
                    // Keep the request open to check that rejected terminal
                    // headers never fall through to a deferred NO_ERROR reset.
                    client.start_streaming("/test/EarlyInvalid").unwrap()
                } else {
                    client.start_unary("/test/Invalid", b"request").unwrap()
                };
                let good = client.start_unary("/test/Valid", b"valid").unwrap();
                for _ in 0..8 {
                    server.recv(&client.take_output(), &mut handler).unwrap();
                    server.poll(&mut handler, &mut Context::from_waker(Waker::noop()));
                    client.recv(&server.take_output()).unwrap();
                }
                let status = if early {
                    match client.try_next(bad) {
                        Some(Next::Done(Err(status))) => status,
                        other => {
                            panic!("invalid metadata must terminate streaming client: {other:?}")
                        }
                    }
                } else {
                    client
                        .take_response(bad)
                        .expect("invalid metadata must terminate unary client")
                        .unwrap_err()
                };
                assert_eq!(status.code, Code::Internal, "{name}");
                assert_eq!(
                    client.take_response(good).unwrap().unwrap().message,
                    b"valid"
                );
                assert_eq!(server.active_calls(), 0);
                assert!(server.core.draining().is_empty());
                assert_eq!(server.core.conn().stream_count(), 0);
                assert_eq!(server.next_deadline(), None);
                assert!(
                    !server.is_closed(),
                    "metadata rejection must not fail the connection"
                );
            }
        }
    }
}

#[test]
fn active_expiry_delivers_timeout_trailers_best_effort_without_the_old_output_deadline() {
    for path in ["/test/Unary", "/test/Finite", "/test/Bidi"] {
        let mut peer = Peer::new(65_535, 1024);
        let id = peer.open(path, b"request", false, true);
        peer.recv_requests();
        assert_eq!(peer.server.active_calls(), 1);
        peer.server
            .tick(Duration::from_millis(99), &mut peer.handler);
        assert_eq!(
            peer.server.next_deadline(),
            Some(Duration::from_millis(100))
        );
        peer.server
            .tick(Duration::from_millis(100), &mut peer.handler);
        assert_eq!(peer.server.active_calls(), 0);
        assert!(peer.server.has_output(), "timeout trailers await delivery");
        assert_eq!(peer.server.core.draining().get(&id), Some(&None));
        assert_eq!(peer.server.next_deadline(), None);
        if path == "/test/Unary" {
            assert!(peer.handler.cancelled.is_empty());
        } else {
            assert_eq!(peer.handler.cancelled, [id]);
            assert_eq!(
                peer.handler.cancellation_deadlines,
                [Some(Duration::from_millis(100))]
            );
        }

        // Neither expiry processing nor a stalled error-trailer flush should
        // recreate the old output deadline or notify the handler twice.
        peer.poll();
        peer.server.recv(&[], &mut peer.handler).unwrap();
        peer.server
            .tick(Duration::from_millis(200), &mut peer.handler);
        assert_eq!(peer.server.core.draining().get(&id), Some(&None));
        assert_eq!(peer.server.next_deadline(), None);
        let output = peer.server.pending_output().to_vec();
        peer.conn.recv(&output).unwrap();
        peer.events
            .extend(core::iter::from_fn(|| peer.conn.poll_event()));
        assert!(peer.events.iter().any(|event| matches!(event,
            Event::Headers { stream_id, headers, end_stream: true }
                if *stream_id == id && header(headers, "grpc-status") == Some("4"))));
        assert_eq!(peer.data_len(id), 0);
        peer.server.consume_output(output.len());
        assert_eq!(peer.server.core.draining().get(&id), Some(&None));
        assert_eq!(peer.server.next_deadline(), None);
        peer.server.output_flushed();
        assert!(!peer.server.core.draining().contains_key(&id));
        assert_eq!(
            peer.handler.cancelled.len(),
            usize::from(path != "/test/Unary")
        );
    }
}

#[test]
fn active_expiry_clears_only_its_new_timeout_trailers_deadline() {
    let (mut peer, success) = fully_serialized_response("/test/Unary");
    let expired = peer.open("/test/Finite", b"request", false, true);
    peer.recv_requests();
    peer.server
        .tick(Duration::from_millis(100), &mut peer.handler);
    assert_eq!(peer.server.active_calls(), 0);
    assert_eq!(peer.server.core.draining().get(&expired), Some(&None));
    assert_eq!(
        peer.server.core.draining().get(&success),
        Some(&Some(Duration::from_millis(100)))
    );
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    assert_eq!(peer.handler.cancelled, [expired]);
    assert_eq!(
        peer.handler.cancellation_deadlines,
        [Some(Duration::from_millis(100))]
    );
    let bytes = peer.server.pending_output().len();
    peer.server.consume_output(bytes);
    peer.server
        .tick(Duration::from_millis(200), &mut peer.handler);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    peer.server.output_flushed();
    assert_eq!(peer.server.next_deadline(), None);
}

fn fully_serialized_response(path: &str) -> (Peer, StreamId) {
    let mut peer = Peer::new(65_535, 1024);
    peer.handler.response_size = 130;
    let id = peer.open(path, &[7; 130], true, true);
    peer.recv_requests();
    peer.poll();
    assert_eq!(peer.server.active_calls(), 0);
    assert_eq!(peer.server.queued_response_bytes(id), None);
    assert!(!peer.server.core.conn().has_stream(id));
    assert!(peer.server.pending_output().len() > 130);
    assert_eq!(peer.server.core.draining().len(), 1);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    (peer, id)
}

fn assert_serialized_deadline_survives_stalled_write_and_flush(path: &str) {
    let (mut peer, id) = fully_serialized_response(path);
    // A partial write must not retire the call, even though HTTP/2 already
    // removed its stream after framing the terminal response.
    peer.server.consume_output(1);
    peer.poll();
    peer.server.recv(&[], &mut peer.handler).unwrap();
    peer.server
        .tick(Duration::from_millis(99), &mut peer.handler);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );

    // All bytes accepted by write, but the transport's flush is still pending.
    let remaining = peer.server.pending_output().len();
    peer.server.consume_output(remaining);
    assert!(!peer.server.has_output());
    for now in [100, 101, 200] {
        peer.server
            .tick(Duration::from_millis(now), &mut peer.handler);
        peer.poll();
        peer.server.recv(&[], &mut peer.handler).unwrap();
        assert_eq!(peer.server.core.draining().len(), 1);
        assert_eq!(
            peer.server.next_deadline(),
            Some(Duration::from_millis(100))
        );
        assert_eq!(peer.server.queued_response_bytes(id), None);
        assert!(
            !peer.server.has_output(),
            "already-framed output cannot be replaced by a reset"
        );
    }
    assert!(peer.handler.cancelled.is_empty());
    peer.server.output_flushed();
    assert_eq!(peer.server.core.draining().len(), 0);
    assert_eq!(peer.server.next_deadline(), None);
}

#[test]
fn fully_serialized_unary_deadline_survives_consumed_output_and_stalled_flush() {
    assert_serialized_deadline_survives_stalled_write_and_flush("/test/Unary");
}

#[test]
fn fully_serialized_finite_stream_deadline_survives_consumed_output_and_stalled_flush() {
    assert_serialized_deadline_survives_stalled_write_and_flush("/test/Finite");
}

#[test]
fn successful_flush_acknowledgment_retires_serialized_unary_and_finite_responses() {
    for path in ["/test/Unary", "/test/Finite"] {
        let (mut peer, _) = fully_serialized_response(path);
        peer.server.output_flushed();
        assert_eq!(
            peer.server.core.draining().len(),
            1,
            "unwritten output cannot be acknowledged"
        );
        let bytes = peer.server.pending_output().len();
        peer.server.consume_output(bytes);
        peer.server
            .tick(Duration::from_millis(99), &mut peer.handler);
        assert_eq!(
            peer.server.next_deadline(),
            Some(Duration::from_millis(100))
        );
        peer.server.output_flushed();
        assert_eq!(peer.server.core.draining().len(), 0);
        assert_eq!(peer.server.next_deadline(), None);
        peer.server.tick(Duration::MAX, &mut peer.handler);
        assert!(!peer.server.has_output());
        assert!(peer.handler.cancelled.is_empty());
    }
}

#[test]
fn taking_serialized_output_transfers_its_delivery_obligation() {
    for path in ["/test/Unary", "/test/Finite"] {
        let (mut peer, id) = fully_serialized_response(path);
        peer.recv_responses();
        assert_eq!(peer.data_len(id), lpm::HEADER_LEN + 130);
        assert!(peer.has_trailers(id));
        assert_eq!(peer.server.core.draining().len(), 0);
        assert_eq!(peer.server.next_deadline(), None);
    }
}

#[test]
fn connection_cancellation_retires_fully_serialized_unacknowledged_responses() {
    for path in ["/test/Unary", "/test/Finite"] {
        let (mut peer, _) = fully_serialized_response(path);
        let bytes = peer.server.pending_output().len();
        peer.server.consume_output(bytes);
        peer.server.cancel_all(&mut peer.handler);
        assert_eq!(peer.server.core.draining().len(), 0);
        assert_eq!(peer.server.next_deadline(), None);
        assert!(peer.handler.cancelled.is_empty());
    }
}

#[test]
fn unblocked_response_keeps_its_deadline_until_the_serialized_remainder_is_flushed() {
    let mut peer = Peer::new(7, 1024);
    let id = peer.open("/test/Client", b"request", true, true);
    peer.exchange();
    assert_eq!(peer.server.queued_response_bytes(id), Some(18));
    peer.server.output_flushed();
    assert_eq!(
        peer.server.core.draining().len(),
        1,
        "a flush cannot retire blocked DATA"
    );
    peer.server
        .recv(&window_update(id, 100), &mut peer.handler)
        .unwrap();
    assert_eq!(peer.server.queued_response_bytes(id), None);
    assert!(peer.server.has_output());
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    peer.server
        .tick(Duration::from_millis(100), &mut peer.handler);
    assert_eq!(peer.server.core.draining().len(), 1);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    let bytes = peer.server.pending_output().len();
    peer.server.consume_output(bytes);
    peer.server.output_flushed();
    assert_eq!(peer.server.next_deadline(), None);
}

#[test]
fn local_deferred_reset_and_graceful_shutdown_do_not_acknowledge_transport_delivery() {
    let mut peer = Peer::new(0, 1024);
    let id = peer.open("/test/Bidi", b"request", false, true);
    peer.exchange();
    peer.server.finish(id, Ok(()));
    peer.server
        .recv(&window_update(id, 100), &mut peer.handler)
        .unwrap();
    // The local NO_ERROR reset event is processed by recv, but its bytes are
    // merely serialized, not transport-flushed.
    assert!(!peer.server.core.conn().has_stream(id));
    assert_eq!(peer.server.core.draining().len(), 1);
    peer.server.shutdown();
    assert!(peer.server.is_closed());
    peer.server.recv(&[], &mut peer.handler).unwrap();
    assert_eq!(peer.server.core.draining().len(), 1);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    let bytes = peer.server.pending_output().len();
    peer.server.consume_output(bytes);
    peer.server
        .tick(Duration::from_millis(100), &mut peer.handler);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    peer.server.output_flushed();
    assert_eq!(peer.server.next_deadline(), None);
    assert!(peer.handler.cancelled.is_empty());
}

fn window_update(id: StreamId, credit: u32) -> Vec<u8> {
    let mut bytes = vec![0, 0, 4, 8, 0];
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend_from_slice(&credit.to_be_bytes());
    bytes
}

#[test]
fn completed_unary_and_client_streaming_responses_keep_their_draining_deadlines() {
    for path in ["/test/Unary", "/test/Client"] {
        for window in [0, 7] {
            let mut peer = Peer::new(window, 1024);
            let id = peer.open(path, &[7; 20], true, true);
            peer.exchange();
            assert_eq!(peer.server.active_calls(), 0, "{path}, window {window}");
            assert_eq!(peer.server.core.draining().len(), 1);
            assert_eq!(
                peer.server.next_deadline(),
                Some(Duration::from_millis(100))
            );
            assert_eq!(
                peer.server.queued_response_bytes(id),
                Some(25 - window as usize)
            );
            assert_eq!(peer.data_len(id), window as usize);
            assert!(!peer.has_trailers(id));

            peer.server
                .tick(Duration::from_millis(99), &mut peer.handler);
            assert_eq!(peer.server.core.draining().len(), 1);
            peer.server
                .tick(Duration::from_millis(100), &mut peer.handler);
            assert_eq!(peer.server.core.draining().len(), 0);
            assert_eq!(peer.server.next_deadline(), None);
            assert_eq!(peer.server.queued_response_bytes(id), None);
            peer.recv_responses();
            assert!(peer.events.iter().any(|event| matches!(event,
                Event::Reset { stream_id, error_code: ErrorCode::Cancel } if *stream_id == id)));
            assert!(
                peer.handler.cancelled.is_empty(),
                "completed handlers must not be cancelled"
            );

            // A late stream/connection WINDOW_UPDATE must not revive discarded DATA.
            let mut updates = window_update(id, 100);
            updates.extend(window_update(0, 100));
            peer.server.recv(&updates, &mut peer.handler).unwrap();
            peer.poll();
            peer.recv_responses();
            assert_eq!(peer.data_len(id), window as usize);
            assert!(!peer.has_trailers(id));
            peer.server.cancel_all(&mut peer.handler);
            assert!(peer.handler.cancelled.is_empty());
        }
    }
}

#[test]
fn active_streaming_deadlines_reset_zero_window_and_partially_sent_output() {
    for path in ["/test/Infinite", "/test/Bidi"] {
        for window in [0, 7] {
            let mut peer = Peer::new(window, 1024);
            let id = peer.open(path, &[7; 20], path != "/test/Bidi", true);
            peer.exchange();
            assert_eq!(peer.server.active_calls(), 1);
            assert!(peer.server.queued_response_bytes(id).unwrap() > 0);
            peer.server
                .tick(Duration::from_millis(100), &mut peer.handler);
            assert_eq!(peer.server.active_calls(), 0);
            assert_eq!(peer.server.next_deadline(), None);
            assert_eq!(peer.server.queued_response_bytes(id), None);
            assert_eq!(peer.handler.cancelled, [id]);
            peer.recv_responses();
            assert!(peer.events.iter().any(|event| matches!(event,
                Event::Reset { stream_id, error_code: ErrorCode::Cancel } if *stream_id == id)));
            let before = peer.data_len(id);
            peer.server
                .recv(&window_update(id, 100), &mut peer.handler)
                .unwrap();
            peer.poll();
            peer.recv_responses();
            assert_eq!(peer.data_len(id), before);
            peer.server.cancel_all(&mut peer.handler);
            peer.server.tick(Duration::MAX, &mut peer.handler);
            assert_eq!(peer.handler.cancelled, [id]);
        }
    }
}

#[test]
fn connection_cancellation_reclaims_draining_streams_without_duplicate_callbacks() {
    let mut peer = Peer::new(0, 1024);
    let unary = peer.open("/test/Unary", b"queued", true, true);
    let client = peer.open("/test/Client", b"queued", true, true);
    let active = peer.open("/test/Infinite", b"queued", true, true);
    peer.exchange();
    assert_eq!(peer.server.core.draining().len(), 2);
    peer.server.cancel_all(&mut peer.handler);
    assert_eq!(peer.server.core.draining().len(), 0);
    assert_eq!(peer.server.active_calls(), 0);
    assert_eq!(peer.server.next_deadline(), None);
    for id in [unary, client, active] {
        assert_eq!(peer.server.queued_response_bytes(id), None);
    }
    assert_eq!(peer.handler.cancelled, [active]);
    peer.server.cancel_all(&mut peer.handler);
    peer.server.tick(Duration::MAX, &mut peer.handler);
    assert_eq!(peer.handler.cancelled, [active]);
}

#[test]
fn draining_records_disappear_when_window_updates_finish_the_response() {
    let mut peer = Peer::new(7, 1024);
    let id = peer.open("/test/Client", b"request", true, true);
    peer.exchange();
    assert_eq!(peer.server.core.draining().len(), 1);
    for _ in 0..4 {
        let held = peer.conn.unreleased_recv_bytes(id).unwrap_or(0);
        peer.conn.release_capacity(id, held);
        peer.exchange();
    }
    assert_eq!(peer.data_len(id), 25);
    assert!(peer.has_trailers(id));
    assert_eq!(peer.server.core.draining().len(), 0);
    assert_eq!(peer.server.next_deadline(), None);
    peer.server.tick(Duration::MAX, &mut peer.handler);
    assert!(!peer.server.has_output());
    assert!(peer.handler.cancelled.is_empty());
}

#[test]
fn completed_early_response_drains_then_expires_without_a_second_handler_callback() {
    let mut peer = Peer::new(0, 1024);
    let id = peer.open("/test/Bidi", b"request", false, true);
    peer.exchange();
    // A handler can terminate with an error during request delivery. Its
    // terminal status and deferred NO_ERROR reset must not hide its deadline.
    peer.server.finish(id, Err(Status::aborted("finished")));
    assert_eq!(peer.server.core.draining().len(), 1);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    peer.server
        .tick(Duration::from_millis(100), &mut peer.handler);
    assert_eq!(peer.server.queued_response_bytes(id), None);
    assert_eq!(peer.server.core.draining().len(), 0);
    peer.recv_responses();
    assert!(!peer.has_trailers(id));
    assert!(peer.events.iter().any(|event| matches!(event,
        Event::Reset { stream_id, error_code: ErrorCode::Cancel } if *stream_id == id)));
    assert!(peer.handler.cancelled.is_empty());
}

#[test]
fn connection_failure_cancels_active_and_draining_streams() {
    let mut peer = Peer::new(0, 1024);
    let draining = peer.open("/test/Client", b"queued", true, true);
    let active = peer.open("/test/Infinite", b"queued", true, true);
    peer.exchange();
    assert_eq!(peer.server.core.draining().len(), 1);
    // DATA on stream zero is a fatal HTTP/2 protocol error.
    assert!(peer.server.recv(&[0; 9], &mut peer.handler).is_err());
    assert!(peer.server.is_closed());
    assert_eq!(peer.server.core.draining().len(), 0);
    assert_eq!(peer.server.active_calls(), 0);
    assert_eq!(peer.server.next_deadline(), None);
    assert_eq!(peer.server.queued_response_bytes(draining), None);
    assert_eq!(peer.server.queued_response_bytes(active), None);
    assert_eq!(peer.handler.cancelled, [active]);
}

#[test]
fn peer_reset_reclaims_a_completed_draining_stream() {
    let mut peer = Peer::new(0, 1024);
    let id = peer.open("/test/Client", b"queued", true, true);
    peer.exchange();
    assert_eq!(peer.server.core.draining().len(), 1);
    peer.conn.reset_stream(id, ErrorCode::Cancel).unwrap();
    peer.recv_requests();
    assert_eq!(peer.server.core.draining().len(), 1);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    peer.recv_responses();
    assert_eq!(peer.server.core.draining().len(), 0);
    assert_eq!(peer.server.next_deadline(), None);
    assert_eq!(peer.server.queued_response_bytes(id), None);
    assert!(peer.handler.cancelled.is_empty());
}

#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn flow_control_blocked_calls_do_not_self_wake_or_keep_polling_the_handler() {
    let mut peer = Peer::new(0, usize::MAX);
    let id = peer.open("/test/Infinite", b"", true, true);
    peer.exchange();
    assert_eq!(peer.handler.replies[&id], 1);
    let counter = Arc::new(CountingWaker::default());
    let waker = Waker::from(counter.clone());
    for _ in 0..4 {
        peer.server
            .poll(&mut peer.handler, &mut Context::from_waker(&waker));
    }
    assert_eq!(peer.handler.replies[&id], 1);
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
}

#[test]
fn productive_calls_have_a_bounded_quantum_and_schedule_their_next_poll() {
    let mut peer = Peer::new(65_535, usize::MAX);
    peer.handler.response_size = 1;
    let first = peer.open("/test/Infinite", b"", true, false);
    let second = peer.open("/test/Infinite", b"", true, false);
    peer.recv_requests();
    let counter = Arc::new(CountingWaker::default());
    let waker = Waker::from(counter.clone());
    peer.server
        .poll(&mut peer.handler, &mut Context::from_waker(&waker));
    assert_eq!(peer.handler.replies[&first], CALL_QUANTUM);
    assert_eq!(peer.handler.replies[&second], CALL_QUANTUM);
    assert_eq!(counter.0.load(Ordering::SeqCst), 2);
    peer.recv_responses();
    peer.server
        .poll(&mut peer.handler, &mut Context::from_waker(&waker));
    assert_eq!(peer.handler.replies[&first], 2 * CALL_QUANTUM);
    assert_eq!(peer.handler.replies[&second], 2 * CALL_QUANTUM);
    assert_eq!(counter.0.load(Ordering::SeqCst), 4);
}

#[test]
fn rotating_polls_progress_later_finite_and_bidi_calls_under_a_tight_output_budget() {
    let mut peer = Peer::new(65_535, 1000);
    peer.handler.response_size = 1000;
    let busy = peer.open("/test/Infinite", b"", true, false);
    let also_busy = peer.open("/test/Infinite", b"", true, false);
    let finite = peer.open("/test/Finite", b"", true, false);
    let bidi = peer.open("/test/Bidi", b"echo", false, false);
    peer.recv_requests();
    // One rotation to send a maximum-sized finite response, a second to
    // observe Done after that response's output budget has been consumed.
    for round in 0..8 {
        peer.poll();
        peer.recv_responses();
        if round == 3 {
            assert!(peer.data_len(finite) > 0, "finite response starved");
            assert_eq!(
                peer.handler.replies[&bidi], 1,
                "bidi request delivery starved"
            );
        }
        // Continuously drain the productive low-ID calls, not just the later ones.
        for id in [busy, also_busy, finite, bidi] {
            let held = peer.conn.unreleased_recv_bytes(id).unwrap_or(0);
            peer.conn.release_capacity(id, held);
        }
        peer.recv_requests();
    }
    assert!(peer.handler.replies[&busy] > 0);
    assert!(peer.handler.replies[&also_busy] > 0);
    assert!(peer.has_trailers(finite), "finite completion starved");
    assert_eq!(
        peer.handler.replies[&bidi], 1,
        "bidi request delivery starved"
    );
    assert_eq!(peer.data_len(bidi), lpm::HEADER_LEN + 4);
}

#[test]
fn buffered_bidi_request_delivery_is_bounded_and_wakes_for_remaining_work() {
    let mut peer = Peer::new(65_535, usize::MAX);
    let id = peer.open("/test/Bidi", b"", false, false);
    for _ in 1..CALL_QUANTUM + 3 {
        peer.conn
            .send_data(id, lpm::encode(b"").unwrap(), false)
            .unwrap();
    }
    peer.recv_requests();
    let counter = Arc::new(CountingWaker::default());
    let waker = Waker::from(counter.clone());
    peer.server
        .poll(&mut peer.handler, &mut Context::from_waker(&waker));
    assert_eq!(peer.handler.replies[&id], CALL_QUANTUM);
    assert!(peer.server.buffered_request_bytes(id).unwrap() > 0);
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    peer.recv_responses();
    peer.server
        .poll(&mut peer.handler, &mut Context::from_waker(&waker));
    assert_eq!(peer.handler.replies[&id], CALL_QUANTUM + 3);
    assert_eq!(peer.server.buffered_request_bytes(id), Some(0));
}

#[derive(Debug)]
struct NoCompression;

impl Codec for NoCompression {
    fn name(&self) -> &'static str {
        "test"
    }
    fn compress(&self, _: &[u8], _: &mut Vec<u8>) -> Result<(), crate::compression::CodecError> {
        Err(crate::compression::CodecError::Unsupported)
    }
    fn decompress(
        &self,
        _: &[u8],
        _: &mut Vec<u8>,
        _: usize,
    ) -> Result<(), crate::compression::CodecError> {
        Err(crate::compression::CodecError::Unsupported)
    }
}

#[test]
fn output_budget_boundaries_saturate_without_large_allocations() {
    static CODEC: NoCompression = NoCompression;
    let settings_bytes = Server::new(ServerConfig::default()).pending_output().len();
    let with_limit = |max_message_size| {
        Server::new(ServerConfig {
            max_message_size,
            ..ServerConfig::default()
        })
    };
    assert!(
        with_limit(settings_bytes - lpm::HEADER_LEN)
            .core
            .can_send(1),
        "exact output budget should still permit a pull"
    );
    assert!(
        !with_limit(settings_bytes - lpm::HEADER_LEN - 1)
            .core
            .can_send(1),
        "one byte over budget must stop pulling"
    );
    for limit in [
        0,
        1,
        usize::MAX - lpm::HEADER_LEN,
        usize::MAX - 1,
        usize::MAX,
    ] {
        for send in [None, Some(&CODEC as &'static dyn Codec)] {
            let mut server = Server::new(ServerConfig {
                max_message_size: limit,
                compression: Compression {
                    send,
                    ..Compression::NONE
                },
                ..ServerConfig::default()
            });
            let max_wire = if send.is_some() {
                lpm::wire_limit(limit)
            } else {
                limit
            };
            // Only SETTINGS are allocated, even for a usize::MAX size limit.
            let budget = max_wire.saturating_add(lpm::HEADER_LEN);
            assert_eq!(
                !server.core.can_send(1),
                server.pending_output().len() > budget
            );
            server.take_output();
            assert!(server.core.can_send(1));
        }
    }
}

#[test]
fn reset_in_the_same_read_as_a_request_cancels_before_delivery() {
    // The request message and the reset arrive in one read. The call is
    // cancelled as a whole: its handler is told about the cancellation, and the
    // message is not delivered to it.
    let mut conn = Connection::client(Default::default());
    let headers = vec![
        field(":method", "POST"),
        field(":scheme", "http"),
        field(":path", "/test/Bidi"),
        field("content-type", "application/grpc"),
    ];
    let id = conn.open_stream(headers, false).unwrap();
    conn.send_data(id, lpm::encode(b"m").unwrap(), false)
        .unwrap();
    conn.reset_stream(id, ErrorCode::Cancel).unwrap();
    let bytes = conn.take_output();
    let mut server = Server::new(ServerConfig::default());
    let mut handler = HandlerState::default();
    server.recv(&bytes, &mut handler).unwrap();
    assert!(handler.requests.is_empty());
    assert_eq!(handler.cancelled, vec![id]);
    assert_eq!(server.active_calls(), 0);
}

/// A reset arrives while the server's output is pending: the call it names is
/// cancelled once, and the pending reply of another call is kept for the
/// transport. Polling again does not cancel it a second time.
#[test]
fn rx_reset_cancels_once_during_tx() {
    let mut conn = Connection::client(Default::default());
    let open = |conn: &mut Connection, path: &str| {
        conn.open_stream(
            vec![
                field(":method", "POST"),
                field(":scheme", "http"),
                field(":path", path),
                field("content-type", "application/grpc"),
            ],
            false,
        )
        .unwrap()
    };
    let bidi = open(&mut conn, "/test/Bidi");
    conn.send_data(bidi, lpm::encode(b"m").unwrap(), false)
        .unwrap();
    let unary = open(&mut conn, "/test/Unary");
    conn.send_data(unary, lpm::encode(b"u").unwrap(), true)
        .unwrap();
    let mut server = Server::new(ServerConfig::default());
    let mut handler = HandlerState::default();
    server.recv(&conn.take_output(), &mut handler).unwrap();
    // The unary reply is queued and not written: the transmit is pending.
    assert!(!server.pending_output().is_empty());
    assert!(handler.cancelled.is_empty());

    conn.reset_stream(bidi, ErrorCode::Cancel).unwrap();
    server.recv(&conn.take_output(), &mut handler).unwrap();
    assert_eq!(handler.cancelled, vec![bidi]);
    assert!(
        !server.pending_output().is_empty(),
        "the pending reply was dropped by the reset"
    );

    let waker = core::task::Waker::noop();
    let mut cx = core::task::Context::from_waker(waker);
    server.poll(&mut handler, &mut cx);
    assert_eq!(
        handler.cancelled,
        vec![bidi],
        "the call was cancelled twice"
    );
}

/// A graceful shutdown queues its GOAWAY, and input that arrives afterwards
/// appends a PING acknowledgement. The connection is closed, but the drivers
/// keep writing until none of those bytes is left: `has_output` stays true
/// until the transport has taken them and the flush is acknowledged.
#[test]
fn shutdown_flushes_output_appended_by_rx() {
    let mut conn = Connection::client(Default::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = HandlerState::default();
    server.recv(&conn.take_output(), &mut handler).unwrap();
    // The server's own handshake is written first, so only the bytes below remain.
    let _ = server.take_output();

    server.shutdown();
    assert!(server.is_closed());
    assert!(server.has_output(), "the GOAWAY is still to be written");
    // A PING arrives after the shutdown began: its acknowledgement is output too.
    let ping = [0, 0, 8, 6, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8];
    server.recv(&ping, &mut handler).unwrap();
    let pending = server.pending_output().to_vec();
    assert_eq!(pending.len(), 17 + 17);
    assert_eq!(pending[3], 7, "the GOAWAY comes first");
    assert_eq!(
        (pending[20], pending[21]),
        (6, 1),
        "then the PING acknowledgement"
    );

    server.consume_output(17);
    assert!(
        server.has_output(),
        "the PING acknowledgement is not written yet"
    );
    server.consume_output(17);
    server.output_flushed();
    assert!(!server.has_output());
}

/// A peer whose server limits its pending output to `limit` bytes.
fn limited_peer(window: u32, limit: usize) -> Peer {
    let mut peer = Peer {
        conn: Connection::client(Config {
            initial_window_size: window,
            flow_control: FlowControl::Manual,
            ..Config::default()
        }),
        server: Server::new(ServerConfig {
            http2: Config {
                max_pending_output_bytes: limit,
                ..ServerConfig::default().http2
            },
            ..ServerConfig::default()
        }),
        handler: HandlerState {
            response_size: 20,
            ..HandlerState::default()
        },
        events: Vec::new(),
    };
    peer.exchange();
    peer
}

#[test]
fn output_limit_local_handler_poll_cancels_once() {
    // Responses are not read, so a handler's output builds up until the limit
    // refuses one of them, in `poll`, not in `recv`.
    let limit = 1024;
    let mut peer = limited_peer(1 << 16, limit);
    let id = peer.open("/test/Infinite", b"request", true, false);
    peer.recv_requests();
    for _ in 0..200 {
        peer.poll();
        if peer.server.is_closed() {
            break;
        }
    }
    assert!(matches!(
        peer.server.failure(),
        Some(Error::OutputLimitExceeded { limit: l, .. }) if *l == limit
    ));
    assert!(peer.server.is_closed());
    assert_eq!(peer.handler.cancelled, [id]);
    assert!(peer.server.pending_output().len() <= limit);

    // Later work on the dead connection neither serializes nor cancels again.
    let pending = peer.server.pending_output().len();
    peer.poll();
    peer.server.tick(Duration::from_secs(1), &mut peer.handler);
    assert!(peer.server.recv(&[], &mut peer.handler).is_err());
    assert_eq!(peer.server.pending_output().len(), pending);
    assert_eq!(peer.handler.cancelled, [id]);
    assert_eq!(peer.server.active_calls(), 0);
}

#[test]
fn server_recv_returns_handler_generated_output_limit() {
    // A unary call answers with a message that does not fit the cap, and the
    // streaming call open in the same input is cancelled once, by that failure.
    let limit = 1024;
    let mut peer = limited_peer(1 << 16, limit);
    let streaming = peer.open("/test/Infinite", b"request", true, false);
    let _unary = peer.open("/test/Unary", &[7; 2000], true, false);
    let input = peer.conn.take_output();
    let result = peer.server.recv(&input, &mut peer.handler);
    assert!(matches!(
        result,
        Err(Error::OutputLimitExceeded { limit: l, .. }) if l == limit
    ));
    assert_eq!(peer.handler.cancelled, [streaming]);
    assert!(peer.server.is_closed());
    assert!(peer.server.pending_output().len() <= limit);
}
