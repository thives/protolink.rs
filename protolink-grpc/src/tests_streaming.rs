//! Streaming tests for the sans-IO client and server cores.

extern crate std;

use alloc::collections::{BTreeMap, VecDeque};
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use std::task::Wake;

use super::*;
use protolink_http2::{Connection, ErrorCode, Event, HeaderField};

// ---------------------------------------------------------------------------
// Length-prefixed message decoder
// ---------------------------------------------------------------------------

fn encoded(messages: &[&[u8]]) -> Vec<u8> {
    messages.iter().flat_map(|m| lpm::encode(m)).collect()
}

fn decode_all(d: &mut lpm::Decoder) -> Vec<Vec<u8>> {
    core::iter::from_fn(|| d.next().map(Result::unwrap)).collect()
}

#[test]
fn decoder_handles_every_split_point() {
    let messages: [&[u8]; 4] = [b"first", b"", b"third message", &[9; 300]];
    let bytes = encoded(&messages);
    for split in 0..=bytes.len() {
        let mut d = lpm::Decoder::new(1024);
        d.push(&bytes[..split]);
        let mut got = decode_all(&mut d);
        d.push(&bytes[split..]);
        got.extend(decode_all(&mut d));
        assert_eq!(got, messages.map(<[u8]>::to_vec), "split at {split}");
        assert_eq!(d.finish(), Ok(()));
        assert_eq!(d.buffered(), 0);
    }
}

#[test]
fn decoder_handles_byte_by_byte_and_batched_input() {
    let messages: [&[u8]; 3] = [b"a", b"bb", b"ccc"];
    let bytes = encoded(&messages);
    let mut d = lpm::Decoder::new(16);
    let mut got = Vec::new();
    for b in &bytes {
        d.push(core::slice::from_ref(b));
        got.extend(decode_all(&mut d));
    }
    assert_eq!(got, messages.map(<[u8]>::to_vec));

    // All three in one push.
    let mut d = lpm::Decoder::new(16);
    d.push(&bytes);
    assert_eq!(d.message_count(), 3);
    assert_eq!(d.complete_len(), bytes.len());
    assert_eq!(decode_all(&mut d), messages.map(<[u8]>::to_vec));
}

#[test]
fn decoder_detects_truncation_at_end_of_stream() {
    let bytes = encoded(&[b"hello"]);
    let mut d = lpm::Decoder::new(16);
    d.push(&bytes[..3]);
    assert_eq!(d.finish().unwrap_err().code, Code::Internal);
    let mut d = lpm::Decoder::new(16);
    d.push(&bytes[..7]);
    assert!(d.next().is_none());
    assert_eq!(d.finish().unwrap_err().code, Code::Internal);
}

#[test]
fn decoder_rejects_oversized_message_from_its_prefix() {
    let mut d = lpm::Decoder::new(8);
    let mut bytes = encoded(&[b"ok"]);
    bytes.extend_from_slice(&[0, 0, 0, 0, 9]);
    d.push(&bytes);
    // The valid message comes first, then the error; no payload was needed.
    assert_eq!(d.next(), Some(Ok(b"ok".to_vec())));
    assert_eq!(d.next().unwrap().unwrap_err().code, Code::ResourceExhausted);
    assert_eq!(d.buffered(), 0);
    d.push(&[0; 100]);
    assert_eq!(d.buffered(), 0, "input after an error is ignored");
}

#[test]
fn decoder_rejects_compressed_messages_without_a_codec() {
    let mut d = lpm::Decoder::new(8);
    d.push(&[1, 0, 0, 0, 1, 0]);
    assert_eq!(d.next().unwrap().unwrap_err().code, Code::Internal);
}

// ---------------------------------------------------------------------------
// Scripted streaming handler
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct CallState {
    queue: VecDeque<Vec<u8>>,
    requests: u32,
    sum: u32,
    target: u32,
    produced: u32,
    ended: bool,
}

/// Paths:
/// - `/s.S/Unary`: unary echo.
/// - `/s.S/Count`: server-streaming; request `[n]` yields `[1]..[n]`.
/// - `/s.S/FailAfter`: server-streaming; two messages, then `ABORTED`.
/// - `/s.S/FailBefore`: server-streaming; `PERMISSION_DENIED` at once.
/// - `/s.S/Infinite`: server-streaming; endless 1000-byte messages.
/// - `/s.S/Big`: server-streaming; one message, then an oversized one.
/// - `/s.S/Wait`: server-streaming; pending until `ready`, then one message.
/// - `/s.S/Sum`: client-streaming; replies `[count, sum of bytes]`.
/// - `/s.S/Reject`: client-streaming; fails on request `bad`.
/// - `/s.S/NoReply`: client-streaming; ends without a response.
/// - `/s.S/Echo`: bidi echo, done after the client half-closes.
/// - `/s.S/EarlyDone`: bidi; echoes the first request and finishes.
#[derive(Debug, Default)]
struct Scripted {
    calls: BTreeMap<CallId, CallState>,
    cancelled: Vec<CallId>,
    half_closed: Vec<CallId>,
    polls: usize,
    ready: bool,
    waker: Option<Waker>,
}

impl Handler for Scripted {
    fn call(&mut self, path: &str, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        (path == "/s.S/Unary").then(|| Ok(request.to_vec()))
    }

    fn is_unknown_method(&self, path: &str) -> bool {
        path == "/s.S/Missing"
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        Some(match path {
            "/s.S/Unary" => MethodKind::Unary,
            "/s.S/Count" | "/s.S/FailAfter" | "/s.S/FailBefore" | "/s.S/Infinite" | "/s.S/Big"
            | "/s.S/Wait" => MethodKind::ServerStreaming,
            "/s.S/Sum" | "/s.S/Reject" | "/s.S/NoReply" => MethodKind::ClientStreaming,
            "/s.S/Echo" | "/s.S/EarlyDone" => MethodKind::BidiStreaming,
            _ => return None,
        })
    }

    fn on_message(&mut self, path: &str, call: CallId, message: &[u8]) -> Result<(), Status> {
        let st = self.calls.entry(call).or_default();
        st.requests += 1;
        match path {
            "/s.S/Count" => st.target = u32::from(message.first().copied().unwrap_or(0)),
            "/s.S/Sum" => st.sum += message.iter().map(|&b| u32::from(b)).sum::<u32>(),
            "/s.S/Reject" if message == b"bad" => {
                return Err(Status::invalid_argument("bad request"));
            }
            "/s.S/Echo" | "/s.S/EarlyDone" => st.queue.push_back(message.to_vec()),
            _ => {}
        }
        Ok(())
    }

    fn on_half_close(&mut self, _path: &str, call: CallId) -> Result<(), Status> {
        self.half_closed.push(call);
        self.calls.entry(call).or_default().ended = true;
        Ok(())
    }

    fn poll_response(
        &mut self,
        path: &str,
        call: CallId,
        cx: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        self.polls += 1;
        let st = self.calls.entry(call).or_default();
        let next = match path {
            "/s.S/Count" if st.produced < st.target => {
                st.produced += 1;
                Next::Message(vec![st.produced as u8])
            }
            "/s.S/Count" => Next::Done(Ok(())),
            "/s.S/FailAfter" if st.produced < 2 => {
                st.produced += 1;
                Next::Message(vec![st.produced as u8])
            }
            "/s.S/FailAfter" => Next::Done(Err(Status::aborted("after two"))),
            "/s.S/FailBefore" => Next::Done(Err(Status::permission_denied("denied"))),
            "/s.S/Infinite" => {
                st.produced += 1;
                Next::Message(vec![7; 1000])
            }
            "/s.S/Big" if st.produced == 0 => {
                st.produced += 1;
                Next::Message(vec![1])
            }
            "/s.S/Big" => Next::Message(vec![0; 10_000]),
            "/s.S/Wait" if !self.ready => {
                self.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            "/s.S/Wait" if st.produced == 0 => {
                st.produced += 1;
                Next::Message(b"ready".to_vec())
            }
            "/s.S/Wait" => Next::Done(Ok(())),
            "/s.S/Sum" => Next::Message(vec![st.requests as u8, st.sum as u8]),
            "/s.S/NoReply" => Next::Done(Ok(())),
            "/s.S/Echo" => match st.queue.pop_front() {
                Some(m) => Next::Message(m),
                None if st.ended => Next::Done(Ok(())),
                None => return Poll::Pending,
            },
            "/s.S/EarlyDone" => match st.queue.pop_front() {
                Some(m) => Next::Message(m),
                None if st.requests > 0 => Next::Done(Ok(())),
                None => return Poll::Pending,
            },
            _ => Next::Done(Err(Status::unimplemented("unknown"))),
        };
        if matches!(next, Next::Done(_)) || path == "/s.S/Sum" {
            self.calls.remove(&call);
        }
        Poll::Ready(next)
    }

    fn on_cancel(&mut self, _path: &str, call: CallId) {
        self.cancelled.push(call);
        self.calls.remove(&call);
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn poll_server(server: &mut Server, handler: &mut impl Handler) {
    server.poll(handler, &mut Context::from_waker(Waker::noop()));
}

/// Exchange bytes until both sides are idle, splitting every transfer into
/// `chunk`-byte pieces.
fn pump_chunked(
    client: &mut Client,
    server: &mut Server,
    handler: &mut impl Handler,
    chunk: usize,
) {
    for _ in 0..256 {
        poll_server(server, handler);
        if !client.has_output() && !server.has_output() {
            return;
        }
        for piece in client.take_output().chunks(chunk) {
            server.recv(piece, handler).unwrap();
        }
        poll_server(server, handler);
        for piece in server.take_output().chunks(chunk) {
            client.recv(piece).unwrap();
        }
    }
    panic!("connection did not settle");
}

fn pump(client: &mut Client, server: &mut Server, handler: &mut impl Handler) {
    pump_chunked(client, server, handler, usize::MAX);
}

/// Take every available response; the final status if the call completed.
fn drain(client: &mut Client, id: CallId) -> (Vec<Vec<u8>>, Option<Result<(), Status>>) {
    let mut messages = Vec::new();
    while let Some(next) = client.try_next(id) {
        match next {
            Next::Message(m) => messages.push(m),
            Next::Done(r) => return (messages, Some(r)),
        }
    }
    (messages, None)
}

struct Setup {
    client: Client,
    server: Server,
    handler: Scripted,
}

fn setup() -> Setup {
    Setup {
        client: Client::new(ClientConfig::default()),
        server: Server::new(ServerConfig::default()),
        handler: Scripted::default(),
    }
}

impl Setup {
    fn pump(&mut self) {
        pump(&mut self.client, &mut self.server, &mut self.handler);
    }

    /// Run a whole call: send `requests`, half-close, collect the responses.
    fn call(&mut self, path: &str, requests: &[&[u8]]) -> (Vec<Vec<u8>>, Result<(), Status>) {
        let id = self.client.start_streaming(path).unwrap();
        for r in requests {
            self.client.send_message(id, r).unwrap();
        }
        self.client.close_send(id).unwrap();
        self.pump();
        let (messages, status) = drain(&mut self.client, id);
        (messages, status.expect("call finished"))
    }
}

fn msgs(items: &[&[u8]]) -> Vec<Vec<u8>> {
    items.iter().map(|m| m.to_vec()).collect()
}

// ---------------------------------------------------------------------------
// RPC shapes
// ---------------------------------------------------------------------------

#[test]
fn server_streaming_multiple_messages() {
    let mut s = setup();
    let (messages, status) = s.call("/s.S/Count", &[&[5]]);
    assert_eq!(messages, msgs(&[&[1], &[2], &[3], &[4], &[5]]));
    assert_eq!(status, Ok(()));
    assert!(s.handler.cancelled.is_empty());
    assert_eq!(s.server.active_calls(), 0);
}

#[test]
fn server_streaming_empty_stream_is_ok() {
    let mut s = setup();
    assert_eq!(s.call("/s.S/Count", &[&[0]]), (Vec::new(), Ok(())));
}

#[test]
fn client_streaming_with_zero_and_many_requests() {
    let mut s = setup();
    assert_eq!(s.call("/s.S/Sum", &[]), (msgs(&[&[0, 0]]), Ok(())));
    let (messages, status) = s.call("/s.S/Sum", &[&[1, 2], &[3], b"", &[4]]);
    assert_eq!(messages, msgs(&[&[4, 10]]));
    assert_eq!(status, Ok(()));
}

#[test]
fn bidi_messages_flow_independently() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Echo").unwrap();
    s.pump();
    assert_eq!(drain(&mut s.client, id), (Vec::new(), None));

    s.client.send_message(id, b"one").unwrap();
    s.pump();
    assert_eq!(drain(&mut s.client, id), (msgs(&[b"one"]), None));

    s.client.send_message(id, b"two").unwrap();
    s.client.send_message(id, b"three").unwrap();
    s.pump();
    assert_eq!(drain(&mut s.client, id), (msgs(&[b"two", b"three"]), None));

    // Half-close is not cancellation: the server finishes normally.
    s.client.close_send(id).unwrap();
    s.pump();
    assert_eq!(drain(&mut s.client, id), (Vec::new(), Some(Ok(()))));
    assert_eq!(s.handler.half_closed, [id]);
    assert!(s.handler.cancelled.is_empty());
}

#[test]
fn bidi_empty_in_both_directions() {
    let mut s = setup();
    assert_eq!(s.call("/s.S/Echo", &[]), (Vec::new(), Ok(())));
}

#[test]
fn unary_and_streaming_share_a_connection() {
    let mut s = setup();
    let unary = s.client.start_unary("/s.S/Unary", b"u").unwrap();
    let count = s.client.start_streaming("/s.S/Count").unwrap();
    s.client.send_message(count, &[2]).unwrap();
    s.client.close_send(count).unwrap();
    s.pump();
    assert_eq!(s.client.take_response(unary), Some(Ok(b"u".to_vec())));
    assert_eq!(
        drain(&mut s.client, count),
        (msgs(&[&[1], &[2]]), Some(Ok(())))
    );
}

#[test]
fn messages_split_across_and_batched_in_data_events() {
    for chunk in [1, 2, 3, 7, 13, 64] {
        let mut s = setup();
        let id = s.client.start_streaming("/s.S/Echo").unwrap();
        let requests: Vec<Vec<u8>> = (0..6u8).map(|i| vec![i; usize::from(i) * 11]).collect();
        for r in &requests {
            s.client.send_message(id, r).unwrap();
        }
        s.client.close_send(id).unwrap();
        pump_chunked(&mut s.client, &mut s.server, &mut s.handler, chunk);
        assert_eq!(
            drain(&mut s.client, id),
            (requests, Some(Ok(()))),
            "chunk size {chunk}"
        );
    }
}

#[test]
fn concurrent_streaming_calls() {
    let mut s = setup();
    let a = s.client.start_streaming("/s.S/Echo").unwrap();
    let b = s.client.start_streaming("/s.S/Count").unwrap();
    let c = s.client.start_streaming("/s.S/Echo").unwrap();
    s.client.send_message(b, &[3]).unwrap();
    s.client.close_send(b).unwrap();
    for round in 0..3u8 {
        s.client.send_message(a, &[b'a', round]).unwrap();
        s.client.send_message(c, &[b'c', round]).unwrap();
        s.pump();
        assert_eq!(drain(&mut s.client, a).0, [vec![b'a', round]]);
        assert_eq!(drain(&mut s.client, c).0, [vec![b'c', round]]);
    }
    assert_eq!(
        drain(&mut s.client, b),
        (msgs(&[&[1], &[2], &[3]]), Some(Ok(())))
    );
    s.client.close_send(a).unwrap();
    s.client.close_send(c).unwrap();
    s.pump();
    assert_eq!(drain(&mut s.client, a), (Vec::new(), Some(Ok(()))));
    assert_eq!(drain(&mut s.client, c), (Vec::new(), Some(Ok(()))));
    assert_eq!(s.server.active_calls(), 0);
}

// ---------------------------------------------------------------------------
// Status and errors
// ---------------------------------------------------------------------------

#[test]
fn error_after_messages_keeps_the_messages() {
    let mut s = setup();
    let (messages, status) = s.call("/s.S/FailAfter", &[b""]);
    assert_eq!(messages, msgs(&[&[1], &[2]]));
    assert_eq!(status, Err(Status::aborted("after two")));
}

#[test]
fn error_before_messages() {
    let mut s = setup();
    let (messages, status) = s.call("/s.S/FailBefore", &[b""]);
    assert!(messages.is_empty());
    assert_eq!(status, Err(Status::permission_denied("denied")));
}

#[test]
fn handler_error_on_request_ends_the_call() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Reject").unwrap();
    s.client.send_message(id, b"fine").unwrap();
    s.client.send_message(id, b"bad").unwrap();
    s.pump();
    assert_eq!(
        drain(&mut s.client, id),
        (
            Vec::new(),
            Some(Err(Status::invalid_argument("bad request")))
        )
    );
    // The handler ended the call itself: no cancellation.
    assert!(s.handler.cancelled.is_empty());
    assert_eq!(s.server.active_calls(), 0);
}

#[test]
fn sends_after_the_server_finished_are_discarded() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Reject").unwrap();
    s.client.send_message(id, b"bad").unwrap();
    s.pump();
    assert!(!s.client.is_pending(id));
    assert_eq!(s.client.send_message(id, b"more"), Ok(()));
    assert_eq!(s.client.close_send(id), Ok(()));
    assert_eq!(
        s.client.try_next(id),
        Some(Next::Done(Err(Status::invalid_argument("bad request"))))
    );
    assert!(
        s.client.send_message(id, b"x").is_err(),
        "call is forgotten"
    );
}

#[test]
fn client_streaming_without_response_is_internal() {
    let mut s = setup();
    let (messages, status) = s.call("/s.S/NoReply", &[b"x"]);
    assert!(messages.is_empty());
    assert_eq!(status.unwrap_err().code, Code::Internal);
}

#[test]
fn server_streaming_requires_exactly_one_request() {
    let mut s = setup();
    assert_eq!(
        s.call("/s.S/Count", &[]).1.unwrap_err().code,
        Code::Internal
    );
    assert_eq!(
        s.call("/s.S/Count", &[&[1], &[1]]).1.unwrap_err().code,
        Code::Internal
    );
    // Protocol violations end the call without the handler: it is cancelled.
    assert_eq!(s.handler.cancelled.len(), 2);
}

#[test]
fn unary_with_two_requests_is_internal() {
    let mut s = setup();
    assert_eq!(
        s.call("/s.S/Unary", &[b"a", b"b"]).1.unwrap_err().code,
        Code::Internal
    );
}

#[test]
fn unknown_streaming_method_is_unimplemented() {
    let mut s = setup();
    assert_eq!(
        s.call("/s.S/Missing", &[b"a"]).1.unwrap_err().code,
        Code::Unimplemented
    );
    // Not recognized up front: answered once the request ended.
    assert_eq!(
        s.call("/s.S/Dynamic", &[b"a"]).1.unwrap_err().code,
        Code::Unimplemented
    );
}

#[test]
fn unknown_method_is_answered_before_half_close() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Missing").unwrap();
    s.client.send_message(id, b"a").unwrap();
    s.pump();
    assert!(!s.client.is_pending(id));
    assert_eq!(s.server.active_calls(), 0);
    assert!(s.handler.cancelled.is_empty());
    // Later sends and the half-close are discarded.
    assert_eq!(s.client.send_message(id, b"more"), Ok(()));
    assert_eq!(s.client.close_send(id), Ok(()));
    assert_eq!(
        drain(&mut s.client, id).1.unwrap().unwrap_err().code,
        Code::Unimplemented
    );
    s.pump();
    // The connection stays usable.
    assert_eq!(s.call("/s.S/Count", &[&[2]]), (msgs(&[&[1], &[2]]), Ok(())));
}

#[test]
fn unclassified_path_waits_for_half_close() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Dynamic").unwrap();
    s.client.send_message(id, b"a").unwrap();
    s.pump();
    assert!(s.client.is_pending(id));
    assert_eq!(drain(&mut s.client, id), (Vec::new(), None));
    s.client.close_send(id).unwrap();
    s.pump();
    assert_eq!(
        drain(&mut s.client, id).1.unwrap().unwrap_err().code,
        Code::Unimplemented
    );
}

#[test]
fn unknown_method_is_a_trailers_only_response() {
    let (mut conn, mut server, mut handler) = raw_setup();
    let id = conn
        .open_stream(request_headers("/s.S/Missing"), false)
        .unwrap();
    conn.send_data(id, lpm::encode(b"x"), false).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let ev = raw_events(&mut conn);
    assert!(
        matches!(&ev[0], Event::Headers { headers, end_stream: true, .. }
            if headers.iter().any(|h| h.name == "grpc-status" && h.value == "12")),
        "{ev:?}"
    );
    assert_eq!(server.active_calls(), 0);
}

#[test]
fn unknown_unary_request_in_one_chunk_is_unimplemented() {
    let (mut conn, mut server, mut handler) = raw_setup();
    let id = conn
        .open_stream(request_headers("/s.S/Missing"), false)
        .unwrap();
    conn.send_data(id, lpm::encode(b"x"), true).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let ev = raw_events(&mut conn);
    assert!(
        matches!(&ev[0], Event::Headers { headers, end_stream: true, .. }
            if headers.iter().any(|h| h.name == "grpc-status" && h.value == "12")),
        "{ev:?}"
    );
}

#[test]
fn oversized_streaming_messages_are_rejected() {
    let mut s = setup();
    // Request above the server's limit, on a separate connection.
    let mut server = Server::new(ServerConfig::default());
    let mut client = Client::new(ClientConfig {
        max_message_size: 100_000,
        ..ClientConfig::default()
    });
    let id = client.start_streaming("/s.S/Echo").unwrap();
    client.send_message(id, b"ok").unwrap();
    client.send_message(id, &vec![0; 10_000]).unwrap();
    pump(&mut client, &mut server, &mut s.handler);
    let (messages, status) = drain(&mut client, id);
    assert!(messages.len() <= 1);
    assert_eq!(status.unwrap().unwrap_err().code, Code::ResourceExhausted);
    assert_eq!(s.handler.cancelled, [id]);

    // Response above the server's limit, after a valid one.
    let (messages, status) = s.call("/s.S/Big", &[b""]);
    assert_eq!(messages, msgs(&[&[1]]));
    assert_eq!(status.unwrap_err().code, Code::ResourceExhausted);

    // Request above the client's own limit is refused locally.
    let id = s.client.start_streaming("/s.S/Echo").unwrap();
    assert_eq!(
        s.client
            .send_message(id, &vec![0; 10_000])
            .unwrap_err()
            .code,
        Code::ResourceExhausted
    );
}

#[test]
fn oversized_response_is_rejected_by_client() {
    let mut s = setup();
    s.server = Server::new(ServerConfig {
        max_message_size: 100_000,
        ..ServerConfig::default()
    });
    let (messages, status) = s.call("/s.S/Big", &[b""]);
    assert_eq!(messages, msgs(&[&[1]]));
    assert_eq!(status.unwrap_err().code, Code::ResourceExhausted);
    // The client reset the stream; the server cancelled the handler.
    assert_eq!(s.handler.cancelled.len(), 1);
}

// ---------------------------------------------------------------------------
// Cancellation, resets and connection failure
// ---------------------------------------------------------------------------

#[test]
fn client_cancel_reaches_the_handler() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Echo").unwrap();
    s.client.send_message(id, b"x").unwrap();
    s.pump();
    s.client.cancel(id);
    assert_eq!(s.client.try_next(id), None);
    s.pump();
    assert_eq!(s.handler.cancelled, [id]);
    assert!(s.handler.half_closed.is_empty());
    assert_eq!(s.server.active_calls(), 0);
}

#[test]
fn server_cancel_all_resets_streams_and_cancels_handlers() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Wait").unwrap();
    s.client.send_message(id, b"").unwrap();
    s.client.close_send(id).unwrap();
    s.pump();
    s.server.cancel_all(&mut s.handler);
    assert_eq!(s.handler.cancelled, [id]);
    s.pump();
    assert_eq!(
        drain(&mut s.client, id).1.unwrap().unwrap_err().code,
        Code::Cancelled
    );
}

#[test]
fn peer_reset_cancels_the_handler_call() {
    let (mut conn, mut server, mut handler) = raw_setup();
    let id = conn
        .open_stream(request_headers("/s.S/Infinite"), false)
        .unwrap();
    conn.send_data(id, lpm::encode(b""), true).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    assert!(handler.polls > 0);
    conn.reset_stream(id, ErrorCode::Cancel).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    assert_eq!(handler.cancelled, [id]);
    assert_eq!(server.active_calls(), 0);
}

#[test]
fn connection_failure_cancels_streaming_calls() {
    let mut s = setup();
    let a = s.client.start_streaming("/s.S/Echo").unwrap();
    let b = s.client.start_streaming("/s.S/Echo").unwrap();
    s.client.send_message(a, b"queued").unwrap();
    s.pump();
    // DATA on stream 0 is a connection error.
    assert!(
        s.server
            .recv(&[0, 0, 1, 0, 0, 0, 0, 0, 0, 0], &mut s.handler)
            .is_err()
    );
    assert_eq!(s.handler.cancelled, [a, b]);
    assert_eq!(s.server.active_calls(), 0);

    // On the client, messages received before the failure come first.
    assert!(s.client.recv(&[0, 0, 1, 0, 0, 0, 0, 0, 0, 0]).is_err());
    let (messages, status) = drain(&mut s.client, a);
    assert_eq!(messages, msgs(&[b"queued"]));
    assert_eq!(status.unwrap().unwrap_err().code, Code::Unavailable);
    assert_eq!(
        drain(&mut s.client, b).1.unwrap().unwrap_err().code,
        Code::Unavailable
    );
}

#[test]
fn transport_close_cancels_through_fail_all() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Echo").unwrap();
    s.pump();
    s.client.fail_all(Status::unavailable("connection closed"));
    assert_eq!(
        drain(&mut s.client, id),
        (
            Vec::new(),
            Some(Err(Status::unavailable("connection closed")))
        )
    );
}

// ---------------------------------------------------------------------------
// Wake-ups
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn pending_handler_is_polled_again_after_waking() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Wait").unwrap();
    s.client.send_message(id, b"").unwrap();
    s.client.close_send(id).unwrap();
    s.pump();
    assert_eq!(drain(&mut s.client, id), (Vec::new(), None));

    let counter = Arc::new(CountingWaker::default());
    let waker = Waker::from(counter.clone());
    s.server
        .poll(&mut s.handler, &mut Context::from_waker(&waker));
    assert!(!s.server.has_output());
    // The handler registered the driver's waker.
    let stored = s.handler.waker.take().expect("waker registered");
    assert!(stored.will_wake(&waker));

    // An external event makes a response ready and wakes the driver.
    s.handler.ready = true;
    stored.wake();
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    s.server
        .poll(&mut s.handler, &mut Context::from_waker(&waker));
    assert!(s.server.has_output());
    s.pump();
    assert_eq!(drain(&mut s.client, id), (msgs(&[b"ready"]), Some(Ok(()))));
}

// ---------------------------------------------------------------------------
// Flow control and bounded memory
// ---------------------------------------------------------------------------

const WINDOW: usize = 65_535;

#[test]
fn consumer_that_stops_reading_bounds_memory() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Infinite").unwrap();
    s.client.send_message(id, b"").unwrap();
    s.client.close_send(id).unwrap();
    for _ in 0..8 {
        s.pump();
    }
    // The client never took a message: the server is stalled by the
    // client's receive window, with at most one message queued.
    let buffered = s.client.buffered_response_bytes(id).unwrap();
    assert!(
        buffered <= WINDOW + DEFAULT_MAX_MESSAGE_SIZE + 5,
        "{buffered}"
    );
    assert!(buffered >= WINDOW - 1005, "{buffered}");
    assert!(s.server.queued_response_bytes(id).unwrap() <= 1005);
    let polls = s.handler.polls;
    assert!(polls <= WINDOW / 1005 + 3, "{polls} polls");

    // Pumping more does not make the handler produce more.
    s.pump();
    assert_eq!(s.handler.polls, polls);

    // Consuming messages credits the window back and lets the server go on.
    for _ in 0..20 {
        assert!(matches!(s.client.try_next(id), Some(Next::Message(m)) if m == [7; 1000]));
    }
    s.pump();
    assert!(s.handler.polls > polls);
    assert!(s.client.buffered_response_bytes(id).unwrap() <= WINDOW + 1005);

    s.client.cancel(id);
    s.pump();
    assert_eq!(s.handler.cancelled, [id]);
}

#[test]
fn peer_withholding_window_updates_stalls_the_producer() {
    let (mut conn, mut server, mut handler) = raw_setup();
    let id = conn
        .open_stream(request_headers("/s.S/Infinite"), false)
        .unwrap();
    conn.send_data(id, lpm::encode(b""), true).unwrap();
    for _ in 0..8 {
        raw_exchange(&mut conn, &mut server, &mut handler);
    }
    // `conn` uses manual flow control and never releases capacity.
    let received: usize = core::iter::from_fn(|| conn.poll_event())
        .map(|e| match e {
            Event::Data { data, .. } => data.len(),
            _ => 0,
        })
        .sum();
    assert!(received <= WINDOW, "{received}");
    assert!(server.queued_response_bytes(id).unwrap() <= 1005);
    assert!(
        handler.polls <= WINDOW / 1005 + 3,
        "{} polls",
        handler.polls
    );
}

#[test]
fn bidi_flood_without_reading_is_bounded_on_both_sides() {
    let mut s = setup();
    let id = s.client.start_streaming("/s.S/Echo").unwrap();
    let mut sent = 0usize;
    for _ in 0..64 {
        while s.client.can_send(id) && sent < 10_000 {
            s.client
                .send_message(id, &[(sent % 251) as u8; 1000])
                .unwrap();
            sent += 1;
        }
        s.pump();
    }
    // Responses back up in the client's window; the server then stops
    // delivering requests and withholds their credit, which stops the client.
    assert!(sent < 10_000, "client was never blocked");
    assert!(!s.client.can_send(id));
    assert!(s.client.queued_request_bytes(id).unwrap() <= 1005);
    let server_buffered = s.server.buffered_request_bytes(id).unwrap();
    assert!(server_buffered <= WINDOW + 1005, "{server_buffered}");
    let handler_queued: usize = s.handler.calls[&id].queue.iter().map(Vec::len).sum();
    assert!(handler_queued <= WINDOW + 1005, "{handler_queued}");

    // Reading everything lets the call complete, losslessly and in order.
    let mut received = 0usize;
    let mut closed = false;
    for _ in 0..10_000 {
        while let Some(next) = s.client.try_next(id) {
            match next {
                Next::Message(m) => {
                    assert_eq!(m, [(received % 251) as u8; 1000]);
                    received += 1;
                }
                Next::Done(r) => {
                    assert_eq!(r, Ok(()));
                    assert_eq!(received, sent);
                    return;
                }
            }
        }
        if !closed && s.client.can_send(id) {
            s.client.close_send(id).unwrap();
            closed = true;
        }
        s.pump();
    }
    panic!("call did not complete: {received}/{sent}");
}

// ---------------------------------------------------------------------------
// Raw HTTP/2 peer
// ---------------------------------------------------------------------------

fn hf(n: &str, v: &str) -> HeaderField {
    HeaderField {
        name: n.into(),
        value: v.into(),
    }
}

fn request_headers(path: &str) -> Vec<HeaderField> {
    vec![
        hf(":method", "POST"),
        hf(":scheme", "http"),
        hf(":path", path),
        hf(":authority", "localhost"),
        hf("content-type", "application/grpc"),
        hf("te", "trailers"),
    ]
}

fn raw_setup() -> (Connection, Server, Scripted) {
    let conn = Connection::client(protolink_http2::Config {
        flow_control: protolink_http2::FlowControl::Manual,
        ..Default::default()
    });
    (
        conn,
        Server::new(ServerConfig::default()),
        Scripted::default(),
    )
}

/// Exchange bytes without consuming the raw peer's events.
fn raw_exchange(conn: &mut Connection, server: &mut Server, handler: &mut Scripted) {
    for _ in 0..64 {
        poll_server(server, handler);
        if !conn.has_output() && !server.has_output() {
            return;
        }
        server.recv(&conn.take_output(), handler).unwrap();
        poll_server(server, handler);
        conn.recv(&server.take_output()).unwrap();
    }
}

fn raw_events(conn: &mut Connection) -> Vec<Event> {
    core::iter::from_fn(|| conn.poll_event()).collect()
}

fn grpc_status(headers: &[HeaderField]) -> Option<&str> {
    headers
        .iter()
        .find(|h| h.name == "grpc-status")
        .map(|h| h.value.as_str())
}

#[test]
fn error_before_messages_is_trailers_only() {
    let (mut conn, mut server, mut handler) = raw_setup();
    let id = conn
        .open_stream(request_headers("/s.S/FailBefore"), false)
        .unwrap();
    conn.send_data(id, lpm::encode(b""), true).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let ev = raw_events(&mut conn);
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert!(
        matches!(&ev[0], Event::Headers { headers, end_stream: true, .. }
        if headers[0].value == "200" && grpc_status(headers) == Some("7"))
    );
}

#[test]
fn pending_handler_sends_response_headers() {
    // Peers may wait for the response headers before streaming requests (as
    // h2 and tonic-style bidi clients do), so a waiting handler must not
    // withhold them.
    let (mut conn, mut server, mut handler) = raw_setup();
    let id = conn
        .open_stream(request_headers("/s.S/Echo"), false)
        .unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let ev = raw_events(&mut conn);
    assert!(
        matches!(&ev[..], [Event::Headers { stream_id, headers, end_stream: false }]
            if *stream_id == id && headers[0].value == "200" && grpc_status(headers).is_none()),
        "{ev:?}"
    );

    // So does a server-streaming call waiting for an external event.
    let wait = conn
        .open_stream(request_headers("/s.S/Wait"), false)
        .unwrap();
    conn.send_data(wait, lpm::encode(b""), true).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let ev = raw_events(&mut conn);
    assert!(
        matches!(&ev[..], [Event::Headers { stream_id, end_stream: false, .. }] if *stream_id == wait),
        "{ev:?}"
    );
    conn.reset_stream(id, ErrorCode::Cancel).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    assert_eq!(handler.cancelled, [id]);
}

#[test]
fn error_after_messages_uses_trailers() {
    let (mut conn, mut server, mut handler) = raw_setup();
    let id = conn
        .open_stream(request_headers("/s.S/FailAfter"), false)
        .unwrap();
    conn.send_data(id, lpm::encode(b""), true).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let ev = raw_events(&mut conn);
    assert!(
        matches!(&ev[0], Event::Headers { headers, end_stream: false, .. }
            if grpc_status(headers).is_none()),
        "{ev:?}"
    );
    let data: Vec<u8> = ev
        .iter()
        .filter_map(|e| match e {
            Event::Data { data, .. } => Some(data.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(data, encoded(&[&[1], &[2]]));
    // Exactly one final status.
    let statuses: Vec<_> = ev
        .iter()
        .filter_map(|e| match e {
            Event::Headers { headers, .. } => grpc_status(headers),
            _ => None,
        })
        .collect();
    assert_eq!(statuses, ["10"]);
    assert!(matches!(
        ev.last(),
        Some(Event::Headers {
            end_stream: true,
            ..
        })
    ));
}

#[test]
fn early_finish_delivers_trailers_before_reset() {
    let (mut conn, mut server, mut handler) = raw_setup();
    let id = conn
        .open_stream(request_headers("/s.S/EarlyDone"), false)
        .unwrap();
    // The client keeps its side open.
    conn.send_data(id, lpm::encode(b"hi"), false).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let ev = raw_events(&mut conn);
    let kinds: Vec<&str> = ev
        .iter()
        .map(|e| match e {
            Event::Headers {
                end_stream: false, ..
            } => "headers",
            Event::Headers {
                end_stream: true, ..
            } => "trailers",
            Event::Data { .. } => "data",
            Event::Reset {
                error_code: ErrorCode::NoError,
                ..
            } => "reset",
            _ => "other",
        })
        .collect();
    assert_eq!(kinds, ["headers", "data", "trailers", "reset"], "{ev:?}");
    assert!(handler.cancelled.is_empty());
    assert_eq!(server.active_calls(), 0);
}

#[test]
fn early_finish_waits_for_window_before_reset() {
    let (mut conn, mut server, mut handler) = raw_setup();
    // Fill the client's window with an infinite producer on another stream
    // first, so the early-finishing call's response must wait.
    let busy = conn
        .open_stream(request_headers("/s.S/Infinite"), false)
        .unwrap();
    conn.send_data(busy, lpm::encode(b""), true).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let id = conn
        .open_stream(request_headers("/s.S/EarlyDone"), false)
        .unwrap();
    conn.send_data(id, lpm::encode(&[5; 100]), false).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    // Connection window exhausted: the response headers went out (they are
    // not flow-controlled) but the echo is queued, with no trailers yet.
    let ev = raw_events(&mut conn);
    assert!(
        !ev.iter().any(|e| matches!(e,
            Event::Data { stream_id, .. } | Event::Headers { stream_id, end_stream: true, .. }
                if *stream_id == id)),
        "{ev:?}"
    );
    assert!(server.queued_response_bytes(id).unwrap() > 0);
    // Release the busy stream's data; the queued message, trailers, then
    // the reset follow, in that order.
    let held = conn.unreleased_recv_bytes(busy).unwrap();
    conn.release_capacity(busy, held);
    conn.reset_stream(busy, ErrorCode::Cancel).unwrap();
    raw_exchange(&mut conn, &mut server, &mut handler);
    let ev: Vec<Event> = raw_events(&mut conn)
        .into_iter()
        .filter(|e| match e {
            Event::Headers { stream_id, .. }
            | Event::Data { stream_id, .. }
            | Event::Reset { stream_id, .. } => *stream_id == id,
            Event::GoAway { .. } => true,
        })
        .collect();
    assert!(
        matches!(&ev[0], Event::Data { data, .. } if *data == lpm::encode(&[5; 100])),
        "{ev:?}"
    );
    assert!(
        matches!(&ev[1], Event::Headers { headers, end_stream: true, .. }
        if grpc_status(headers) == Some("0"))
    );
    assert!(matches!(
        &ev[2],
        Event::Reset {
            error_code: ErrorCode::NoError,
            ..
        }
    ));
    assert_eq!(ev.len(), 3);
}
