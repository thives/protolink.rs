//! Server-only deadline, scheduling, and output-budget regressions.

extern crate std;

use super::*;
use alloc::collections::{BTreeMap, VecDeque};
use alloc::sync::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::Waker;
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
    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        Some(Ok(request.to_vec()))
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        Some(match path {
            "/test/Unary" => MethodKind::Unary,
            "/test/Client" => MethodKind::ClientStreaming,
            "/test/Bidi" => MethodKind::BidiStreaming,
            _ => MethodKind::ServerStreaming,
        })
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
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>> {
        if ctx.path == "/test/Valid" {
            return Some(Ok(request.to_vec()));
        }
        self.set_metadata(ctx);
        Some(if self.reply {
            Ok(request.to_vec())
        } else {
            Err(Status::permission_denied("handler rejected request"))
        })
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        Some(if path == "/test/EarlyInvalid" {
            MethodKind::BidiStreaming
        } else {
            MethodKind::Unary
        })
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
                assert!(!server.conn.has_stream(id));
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
                assert!(server.draining.is_empty());
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
                assert!(server.draining.is_empty());
                assert_eq!(server.conn.stream_count(), 0);
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
        assert_eq!(peer.server.draining.get(&id), Some(&None));
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
        assert_eq!(peer.server.draining.get(&id), Some(&None));
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
        assert_eq!(peer.server.draining.get(&id), Some(&None));
        assert_eq!(peer.server.next_deadline(), None);
        peer.server.output_flushed();
        assert!(!peer.server.draining.contains_key(&id));
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
    assert_eq!(peer.server.draining.get(&expired), Some(&None));
    assert_eq!(
        peer.server.draining.get(&success),
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
    assert!(!peer.server.conn.has_stream(id));
    assert!(peer.server.pending_output().len() > 130);
    assert_eq!(peer.server.draining.len(), 1);
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
        assert_eq!(peer.server.draining.len(), 1);
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
    assert_eq!(peer.server.draining.len(), 0);
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
            peer.server.draining.len(),
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
        assert_eq!(peer.server.draining.len(), 0);
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
        assert_eq!(peer.server.draining.len(), 0);
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
        assert_eq!(peer.server.draining.len(), 0);
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
        peer.server.draining.len(),
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
    assert_eq!(peer.server.draining.len(), 1);
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
    assert!(!peer.server.conn.has_stream(id));
    assert_eq!(peer.server.draining.len(), 1);
    peer.server.shutdown();
    assert!(peer.server.is_closed());
    peer.server.recv(&[], &mut peer.handler).unwrap();
    assert_eq!(peer.server.draining.len(), 1);
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
            assert_eq!(peer.server.draining.len(), 1);
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
            assert_eq!(peer.server.draining.len(), 1);
            peer.server
                .tick(Duration::from_millis(100), &mut peer.handler);
            assert_eq!(peer.server.draining.len(), 0);
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
    assert_eq!(peer.server.draining.len(), 2);
    peer.server.cancel_all(&mut peer.handler);
    assert_eq!(peer.server.draining.len(), 0);
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
    assert_eq!(peer.server.draining.len(), 1);
    for _ in 0..4 {
        let held = peer.conn.unreleased_recv_bytes(id).unwrap_or(0);
        peer.conn.release_capacity(id, held);
        peer.exchange();
    }
    assert_eq!(peer.data_len(id), 25);
    assert!(peer.has_trailers(id));
    assert_eq!(peer.server.draining.len(), 0);
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
    assert_eq!(peer.server.draining.len(), 1);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    peer.server
        .tick(Duration::from_millis(100), &mut peer.handler);
    assert_eq!(peer.server.queued_response_bytes(id), None);
    assert_eq!(peer.server.draining.len(), 0);
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
    assert_eq!(peer.server.draining.len(), 1);
    // DATA on stream zero is a fatal HTTP/2 protocol error.
    assert!(peer.server.recv(&[0; 9], &mut peer.handler).is_err());
    assert!(peer.server.is_closed());
    assert_eq!(peer.server.draining.len(), 0);
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
    assert_eq!(peer.server.draining.len(), 1);
    peer.conn.reset_stream(id, ErrorCode::Cancel).unwrap();
    peer.recv_requests();
    assert_eq!(peer.server.draining.len(), 1);
    assert_eq!(
        peer.server.next_deadline(),
        Some(Duration::from_millis(100))
    );
    peer.recv_responses();
    assert_eq!(peer.server.draining.len(), 0);
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
    let mut exact = Server::new(ServerConfig::default());
    let settings_bytes = exact.pending_output().len();
    exact.max_message_size = settings_bytes - lpm::HEADER_LEN;
    assert!(
        !exact.backed_up(1),
        "exact output budget should still permit a pull"
    );
    exact.max_message_size -= 1;
    assert!(exact.backed_up(1), "one byte over budget must stop pulling");
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
            // Only SETTINGS are allocated, even for a usize::MAX size limit.
            let budget = server.max_wire_message().saturating_add(lpm::HEADER_LEN);
            assert_eq!(server.backed_up(1), server.pending_output().len() > budget);
            server.take_output();
            assert!(!server.backed_up(1));
        }
    }
}
