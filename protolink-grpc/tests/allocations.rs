//! Allocation measurements on the gRPC message path, the server and router
//! receive and poll paths, and the local-call traversal. Each measurement runs
//! inside a `Window` on this thread. The tracker and its accounting contract
//! live in `protolink-alloc-track`.

use std::task::{Context, Poll, Waker};

use protolink_alloc_track::{CountingAllocator, Window};
use protolink_grpc::http2::{Config, Connection, HeaderField};
use protolink_grpc::lpm::{self, Decoder};
use protolink_grpc::{
    CallContext, CallId, Client, ClientConfig, Dispatcher, Handler, MethodDescriptor, MethodKind,
    Next, Route, Router, Server, ServerConfig,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| i as u8).collect()
}

fn push_chunks(decoder: &mut Decoder, framed: &[u8], chunk: usize) {
    for piece in framed.chunks(chunk) {
        decoder.push(piece);
    }
}

#[test]
fn eligible_extraction_reuses_the_input_allocation() {
    for payload_len in [1024, 4096] {
        let payload = pattern(payload_len);
        let framed = lpm::encode(&payload).unwrap();

        let window = Window::open();
        let mut decoder = Decoder::new(payload_len);
        decoder.push(&framed);
        let pushed = window.checkpoint();
        let message = decoder.next().unwrap().unwrap();
        let extracted = window.checkpoint();
        eprintln!("eligible {payload_len}-byte: pushed {pushed:?}, extracted {extracted:?}");

        assert_eq!(message, payload, "payload {payload_len}");
        assert_eq!(message.capacity(), framed.len());
        assert_eq!(pushed.allocations, 1);
        assert_eq!(pushed.largest, framed.len());
        assert_eq!(pushed.live, framed.len());
        assert_eq!(extracted.allocations, 0, "payload {payload_len}");
        assert_eq!(extracted.reallocations, 0, "payload {payload_len}");
        assert_eq!(extracted.peak, framed.len(), "no second payload buffer");
        assert_eq!(extracted.live, framed.len(), "decoder retains no storage");
        assert_eq!(decoder.buffered(), 0);
    }
}

#[test]
fn unary_body_decode_allocates_one_buffer_for_an_exact_message() {
    for payload_len in [1024, 4096] {
        let payload = pattern(payload_len);
        let body = lpm::encode(&payload).unwrap();

        let window = Window::open();
        let message = lpm::decode_unary(&body, payload_len, None).unwrap();
        let decoded = window.checkpoint();
        eprintln!("decode_unary {payload_len}-byte: {decoded:?}");

        assert_eq!(message, payload, "payload {payload_len}");
        assert_eq!(decoded.allocations, 1, "payload {payload_len}");
        assert_eq!(decoded.peak, body.len(), "payload {payload_len}");
        assert_eq!(decoded.live, body.len(), "payload {payload_len}");
    }
}

#[test]
fn fragmented_input_copies_out_and_releases_its_oversized_buffer() {
    let payload = pattern(4096);
    let framed = lpm::encode(&payload).unwrap();

    let window = Window::open();
    let mut decoder = Decoder::new(payload.len());
    push_chunks(&mut decoder, &framed, 512);
    let pushed = window.checkpoint();
    let message = decoder.next().unwrap().unwrap();
    let extracted = window.checkpoint();

    assert_eq!(message, payload);
    assert!(
        pushed.live > framed.len(),
        "geometric growth over-allocates"
    );
    assert_eq!(message.capacity(), payload.len());
    assert_eq!(extracted.allocations, 1, "the payload copy");
    assert_eq!(extracted.peak, pushed.live + payload.len());
    assert_eq!(extracted.live, payload.len(), "oversized buffer released");
    assert_eq!(decoder.buffered(), 0);
}

#[test]
fn small_message_in_a_rounded_up_buffer_copies_its_payload() {
    // The six-byte frame sits in a larger allocation, so it does not transfer.
    let framed = lpm::encode(&[7]).unwrap();

    let window = Window::open();
    let mut decoder = Decoder::new(1);
    decoder.push(&framed);
    let pushed = window.checkpoint();
    let message = decoder.next().unwrap().unwrap();
    let extracted = window.checkpoint();

    assert_eq!(message, [7]);
    assert!(pushed.live > framed.len());
    assert_eq!(extracted.allocations, 1);
    assert_eq!(extracted.peak, pushed.live + 1);
    assert_eq!(extracted.live, 1);
}

#[test]
fn oversized_buffer_is_released_before_a_later_tiny_message() {
    let payload = pattern(4096);
    let large = lpm::encode(&payload).unwrap();
    let tiny = lpm::encode(b"ok").unwrap();

    let window = Window::open();
    let mut decoder = Decoder::new(payload.len());
    push_chunks(&mut decoder, &large, 512);
    let large_message = decoder.next().unwrap().unwrap();
    let taken = window.checkpoint();
    assert_eq!(large_message, payload);
    assert_eq!(
        taken.live,
        payload.len(),
        "no storage held for the tiny message"
    );

    decoder.push(&tiny);
    let tiny_message = decoder.next().unwrap().unwrap();
    let tiny_taken = window.checkpoint();
    assert_eq!(tiny_message, b"ok");
    assert_eq!(
        tiny_taken.live,
        payload.len() + tiny_message.capacity(),
        "decoder retains no storage after the tiny message"
    );
    assert_eq!(decoder.buffered(), 0);
}

/// Feed `pieces` to one decoder and count the messages returned without
/// allocating. Reported rather than asserted: a missed transfer is a copy, not a
/// correctness failure.
fn hit_rate(pieces: &[Vec<u8>], expected: &[Vec<u8>]) -> usize {
    let mut decoder = Decoder::new(4096);
    let window = Window::open();
    let (mut hits, mut returned) = (0, 0);
    for piece in pieces {
        decoder.push(piece);
        while decoder.has_next() {
            let _pushed = window.checkpoint();
            let message = decoder.next().unwrap().unwrap();
            let taken = window.checkpoint();
            if taken.allocations == 0 && taken.reallocations == 0 {
                hits += 1;
            }
            assert_eq!(message, expected[returned]);
            returned += 1;
        }
    }
    assert_eq!(returned, expected.len());
    hits
}

#[test]
fn ordinary_delivery_hit_rate_is_reported() {
    let sizes = [64, 1024, 4096];
    let payloads: Vec<Vec<u8>> = (0..48).map(|i| pattern(sizes[i % sizes.len()])).collect();
    let framed: Vec<Vec<u8>> = payloads
        .iter()
        .map(|payload| lpm::encode(payload).unwrap())
        .collect();
    let segmented: Vec<Vec<u8>> = framed.concat().chunks(1460).map(<[u8]>::to_vec).collect();
    for (label, pieces) in [
        ("one DATA frame per message", framed),
        ("1460-byte segments", segmented),
    ] {
        let hits = hit_rate(&pieces, &payloads);
        eprintln!(
            "{label}: {hits}/{} messages returned without allocation",
            payloads.len()
        );
    }
}

fn field(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}

fn exchange(client: &mut Client, peer: &mut Connection) {
    for _ in 0..16 {
        if !client.has_output() && !peer.has_output() {
            return;
        }
        let out = client.take_output();
        if !out.is_empty() {
            peer.recv(&out).unwrap();
        }
        let out = peer.take_output();
        if !out.is_empty() {
            client.recv(&out).unwrap();
        }
    }
    panic!("exchange did not quiesce");
}

/// A unary call whose request has reached the raw peer.
fn unary_call() -> (Client, Connection, CallId) {
    let mut client = Client::new(ClientConfig::default());
    let mut peer = Connection::server(Config::default());
    exchange(&mut client, &mut peer);
    let id = client.start_unary("/test.Client/Unary", b"").unwrap();
    exchange(&mut client, &mut peer);
    while peer.poll_event().is_some() {}
    (client, peer, id)
}

/// The real client receive path. Only the call under test is measured: the
/// connection's own events, headers, and the retained call state also allocate,
/// so this reports the phase totals rather than asserting that nothing does.
#[test]
fn client_unary_response_is_taken_into_its_received_buffer() {
    for payload_len in [1024, 4096] {
        let payload = pattern(payload_len);
        let framed = lpm::encode(&payload).unwrap();
        let (mut client, mut peer, id) = unary_call();
        peer.send_headers(
            id,
            vec![
                field(":status", "200"),
                field("content-type", "application/grpc"),
            ],
            false,
        )
        .unwrap();
        peer.send_data(id, framed.clone(), false).unwrap();
        peer.send_headers(id, vec![field("grpc-status", "0")], true)
            .unwrap();
        let bytes = peer.take_output();

        let window = Window::open();
        client.recv(&bytes).unwrap();
        let response = client.take_response(id).unwrap().unwrap();
        let usage = window.checkpoint();
        eprintln!(
            "client unary {payload_len}-byte response: {usage:?}, message capacity {}",
            response.message.capacity()
        );

        assert_eq!(response.message, payload);
        assert!(
            response.message.capacity() <= framed.len(),
            "retained capacity is covered by the framed-byte charge"
        );
    }
}

const QUIET: &str = "/test.Quiet/Bidi";
const QUIET_METHODS: &[MethodDescriptor] =
    &[MethodDescriptor::new(QUIET, MethodKind::BidiStreaming, 0)];

/// A handler with nothing to send. Its calls stay open and quiescent, so a pass
/// over them is only the traversal, with no output and no handler allocation.
struct Quiet;

impl Handler for Quiet {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(QUIET_METHODS, path)
    }

    fn poll_response(
        &mut self,
        _: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        Poll::Pending
    }
}

impl Dispatcher for Quiet {
    fn methods(&self) -> &[MethodDescriptor] {
        QUIET_METHODS
    }
}

/// `calls` open calls, past their response headers. The setup poll sends those
/// headers and drains the output, so the measurements see only the traversal.
fn quiet_server(calls: usize) -> Server {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    for _ in 0..calls {
        client.start_streaming(QUIET).unwrap();
    }
    server.recv(&client.take_output(), &mut Quiet).unwrap();
    server.poll(&mut Quiet, &mut Context::from_waker(Waker::noop()));
    let _ = server.take_output();
    server
}

fn quiet_router(calls: usize) -> Router {
    let mut client = Client::new(ClientConfig::default());
    let mut router = Router::builder().with_service(Quiet).build().unwrap();
    for _ in 0..calls {
        client.start_streaming(QUIET).unwrap();
    }
    router.recv(&client.take_output()).unwrap();
    router.poll(&mut Context::from_waker(Waker::noop()));
    let _ = router.take_output();
    router
}

/// Asserts that the phase since the previous checkpoint allocated nothing and
/// reallocated nothing. The message is a literal: formatting it would allocate.
fn assert_no_allocation(window: &Window, pass: &str) {
    let phase = window.checkpoint();
    assert_eq!(
        (phase.allocations, phase.reallocations),
        (0, 0),
        "{pass}: {phase:?}"
    );
}

#[test]
fn server_poll_passes_allocate_nothing_across_active_calls() {
    let mut server = quiet_server(8);
    let mut cx = Context::from_waker(Waker::noop());

    let window = Window::open();
    for _ in 0..4 {
        server.poll(&mut Quiet, &mut cx);
        assert_no_allocation(&window, "server poll");
    }
    assert_eq!(server.active_calls(), 8);
}

#[test]
fn server_empty_receive_passes_allocate_nothing_across_active_calls() {
    let mut server = quiet_server(8);

    let window = Window::open();
    for _ in 0..4 {
        server.recv(&[], &mut Quiet).unwrap();
        assert_no_allocation(&window, "server receive");
    }
    assert_eq!(server.active_calls(), 8);
}

/// An empty receive after admission and before the first poll. The admitting
/// `recv` has already walked these calls, so this is a later receive, not the
/// first traversal of them. The cold traversal is measured in `tests_local`.
#[test]
fn empty_receive_before_first_poll_allocates_nothing() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    for _ in 0..8 {
        client.start_streaming(QUIET).unwrap();
    }
    server.recv(&client.take_output(), &mut Quiet).unwrap();
    let _ = server.take_output();

    let window = Window::open();
    server.recv(&[], &mut Quiet).unwrap();
    assert_no_allocation(&window, "empty receive before first poll");
    assert_eq!(server.active_calls(), 8);
}

#[test]
fn router_passes_allocate_nothing_across_active_local_calls() {
    let mut router = quiet_router(8);
    let mut cx = Context::from_waker(Waker::noop());

    let window = Window::open();
    for _ in 0..4 {
        router.poll(&mut cx);
        assert_no_allocation(&window, "router poll");
    }
    for _ in 0..4 {
        router.recv(&[]).unwrap();
        assert_no_allocation(&window, "router receive");
    }
    assert_eq!(router.active_calls(), 8);
}
