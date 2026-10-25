//! Tests of the lifecycle's response rules, driven directly: a plain [`Client`]
//! is the caller, and the test answers each started call by hand.

use alloc::vec;
use alloc::vec::Vec;

use super::*;
use crate::compression::{Codec, Compression};
use crate::fields::field as hf;
use crate::lifecycle::{CallEvent, Lifecycle};
use crate::test_support::drain;
use crate::tests_compression::{ACCEPT_RLE, RLE};
use protolink_http2::{Connection, Event, HeaderField};

const PATH: &str = "/test.Life/Echo";
const LIMIT: usize = 1000;

/// A caller and a lifecycle, exchanging bytes in memory.
struct Peers {
    caller: Client,
    gateway: Lifecycle<()>,
}

/// What the caller saw of one call: its messages, final status and metadata.
struct Outcome {
    messages: Vec<Vec<u8>>,
    status: Result<(), Status>,
    initial: Option<Metadata>,
    trailing: Metadata,
}

impl Peers {
    fn new(compression: Compression) -> Self {
        Self {
            caller: Client::new(ClientConfig {
                compression: Compression::new(&ACCEPT_RLE),
                ..ClientConfig::default()
            }),
            gateway: Lifecycle::new(ServerConfig {
                max_message_size: LIMIT,
                compression,
                ..ServerConfig::default()
            }),
        }
    }

    /// Move bytes both ways until neither side has output.
    fn exchange(&mut self) {
        for _ in 0..8 {
            let up = self.caller.take_output();
            if !up.is_empty() {
                self.gateway
                    .recv(&up, |path| (path == PATH).then_some(()))
                    .unwrap();
            }
            let down = self.gateway.take_output();
            if !down.is_empty() {
                self.caller.recv(&down).unwrap();
            }
        }
    }

    /// A streaming call from the caller with one request, half-closed, started
    /// by the lifecycle. Returns the caller's id and the lifecycle's.
    fn start(&mut self) -> (CallId, CallId) {
        let caller_id = self.caller.start_streaming(PATH).unwrap();
        self.caller.send_message(caller_id, b"request").unwrap();
        self.caller.close_send(caller_id).unwrap();
        self.exchange();
        let Some(CallEvent::Started { id, .. }) = self.gateway.poll_event() else {
            panic!("call did not start");
        };
        (caller_id, id)
    }

    /// Everything the caller received for the call, once it has completed.
    fn outcome(&mut self, id: CallId) -> Outcome {
        self.exchange();
        let (messages, status) = drain(&mut self.caller, id);
        let (initial, trailing) = self.caller.take_metadata(id);
        Outcome {
            messages,
            status: status.expect("the call completed"),
            initial,
            trailing,
        }
    }
}

fn metadata(pairs: &[(&str, &str)]) -> Metadata {
    let mut metadata = Metadata::new();
    for (key, value) in pairs {
        metadata.insert(key, value).unwrap();
    }
    metadata
}

#[test]
fn a_message_at_the_limit_is_sent_and_one_byte_more_is_refused() {
    let mut peers = Peers::new(Compression::NONE);
    let (caller_id, id) = peers.start();
    peers.gateway.send_headers(id, Metadata::new());
    peers.gateway.send_message(id, &vec![1; LIMIT]).unwrap();
    peers
        .gateway
        .finish(id, Ok(()), ResponseMetadata::default());
    let outcome = peers.outcome(caller_id);
    assert_eq!(outcome.messages, [vec![1; LIMIT]]);
    assert!(outcome.status.is_ok());

    let mut peers = Peers::new(Compression::NONE);
    let (caller_id, id) = peers.start();
    peers.gateway.send_headers(id, Metadata::new());
    let status = peers
        .gateway
        .send_message(id, &vec![1; LIMIT + 1])
        .unwrap_err();
    assert_eq!(status.code, Code::ResourceExhausted);
    // Nothing was queued, and the call is still the caller's to end.
    assert!(peers.gateway.has_call(id));
    peers
        .gateway
        .finish(id, Err(status), ResponseMetadata::default());
    let outcome = peers.outcome(caller_id);
    assert!(outcome.messages.is_empty());
    assert_eq!(outcome.status.unwrap_err().code, Code::ResourceExhausted);
}

#[test]
fn a_compressible_message_over_the_limit_is_still_refused() {
    static CODECS: [&dyn Codec; 1] = [&RLE];
    let compression = Compression::new(&CODECS).with_send(&RLE).with_min_size(8);
    let mut peers = Peers::new(compression);
    let (caller_id, id) = peers.start();
    peers.gateway.send_headers(id, Metadata::new());
    // A run of one byte shrinks to a few bytes on the wire, but the message is
    // still counted at its uncompressed size, one byte over the limit.
    let status = peers
        .gateway
        .send_message(id, &vec![7; LIMIT + 1])
        .unwrap_err();
    assert_eq!(status.code, Code::ResourceExhausted);
    peers
        .gateway
        .finish(id, Err(status), ResponseMetadata::default());
    assert_eq!(
        peers.outcome(caller_id).status.unwrap_err().code,
        Code::ResourceExhausted
    );

    // At the limit the same message goes out compressed and arrives whole.
    let mut peers = Peers::new(compression);
    let (caller_id, id) = peers.start();
    peers.gateway.send_headers(id, Metadata::new());
    peers.gateway.send_message(id, &vec![7; LIMIT]).unwrap();
    peers
        .gateway
        .finish(id, Ok(()), ResponseMetadata::default());
    let outcome = peers.outcome(caller_id);
    assert_eq!(outcome.messages, [vec![7; LIMIT]]);
    assert!(outcome.status.is_ok());
}

#[test]
fn headers_then_message_then_default_finish_is_valid() {
    let mut peers = Peers::new(Compression::NONE);
    let (caller_id, id) = peers.start();
    peers
        .gateway
        .send_headers(id, metadata(&[("x-init", "head")]));
    peers.gateway.send_message(id, b"body").unwrap();
    peers
        .gateway
        .finish(id, Ok(()), ResponseMetadata::default());
    let outcome = peers.outcome(caller_id);
    assert_eq!(outcome.messages, [b"body".to_vec()]);
    assert!(outcome.status.is_ok());
    assert_eq!(outcome.initial.unwrap().get("x-init"), Some("head"));
    assert!(outcome.trailing.is_empty());
}

#[test]
fn headers_and_default_finish_without_messages_is_a_valid_empty_stream() {
    let mut peers = Peers::new(Compression::NONE);
    let (caller_id, id) = peers.start();
    peers
        .gateway
        .send_headers(id, metadata(&[("x-init", "head")]));
    peers
        .gateway
        .finish(id, Ok(()), ResponseMetadata::default());
    let outcome = peers.outcome(caller_id);
    assert!(outcome.messages.is_empty());
    assert!(outcome.status.is_ok());
    assert_eq!(outcome.initial.unwrap().get("x-init"), Some("head"));
}

#[test]
fn headers_then_error_with_trailers_keeps_the_status_and_metadata() {
    let mut peers = Peers::new(Compression::NONE);
    let (caller_id, id) = peers.start();
    peers
        .gateway
        .send_headers(id, metadata(&[("x-init", "head")]));
    let mut status = Status::new(Code::PermissionDenied, "denied");
    status.metadata = metadata(&[("x-status", "error")]);
    peers
        .gateway
        .finish(id, Err(status), ResponseMetadata::default());
    let outcome = peers.outcome(caller_id);
    let status = outcome.status.unwrap_err();
    assert_eq!(status.code, Code::PermissionDenied);
    assert_eq!(status.message, "denied");
    assert_eq!(status.metadata.get("x-status"), Some("error"));
}

#[test]
fn trailers_only_success_and_error_carry_their_metadata() {
    let mut peers = Peers::new(Compression::NONE);
    let (caller_id, id) = peers.start();
    peers.gateway.finish(
        id,
        Ok(()),
        ResponseMetadata {
            initial: Some(metadata(&[("x-init", "only")])),
            trailing: metadata(&[("x-trail", "only")]),
        },
    );
    let outcome = peers.outcome(caller_id);
    assert!(outcome.messages.is_empty());
    assert!(outcome.status.is_ok());
    // One field section carries both, so the caller sees no separate headers.
    assert!(outcome.initial.is_none());
    assert_eq!(outcome.trailing.get("x-init"), Some("only"));
    assert_eq!(outcome.trailing.get("x-trail"), Some("only"));

    let mut peers = Peers::new(Compression::NONE);
    let (caller_id, id) = peers.start();
    let mut status = Status::new(Code::PermissionDenied, "denied");
    status.metadata = metadata(&[("x-status", "error")]);
    peers
        .gateway
        .finish(id, Err(status), ResponseMetadata::default());
    let status = peers.outcome(caller_id).status.unwrap_err();
    assert_eq!(status.code, Code::PermissionDenied);
    assert_eq!(status.message, "denied");
    assert_eq!(status.metadata.get("x-status"), Some("error"));
}

/// A PING from the peer, which the lifecycle answers with a 17-byte frame.
const PING: [u8; 17] = [0, 0, 8, 6, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8];

#[test]
fn flush_a_append_b_complete_a_defers_retirement() {
    let mut peers = Peers::new(Compression::NONE);
    let (_, id) = peers.start();
    peers
        .gateway
        .finish(id, Ok(()), ResponseMetadata::default());
    assert_eq!(peers.gateway.draining().len(), 1);

    // Output A is written, and its flush begins.
    let a = peers.gateway.pending_output().len();
    assert!(a > 0);
    peers.gateway.consume_output(a);

    // Input appends B while that flush is pending.
    peers.gateway.recv(&PING, |_| None).unwrap();
    let b = peers.gateway.pending_output().len();
    assert_eq!(b, 17);

    // A's flush completes, but B is still unwritten: nothing is acknowledged,
    // so the call's output stays accounted for.
    peers.gateway.output_flushed();
    assert_eq!(peers.gateway.draining().len(), 1);

    // Flushing B completes the retirement.
    peers.gateway.consume_output(b);
    peers.gateway.output_flushed();
    assert_eq!(peers.gateway.draining().len(), 0);
}

#[test]
fn output_limit_during_finish_cancels_calls_and_keeps_no_draining() {
    // Room for the server's handshake and the ACK of the caller's SETTINGS, and
    // nothing more: a response cannot be queued once the call finishes.
    let handshake = Lifecycle::<()>::new(ServerConfig::default())
        .pending_output()
        .len();
    let limit = handshake + 9;
    let mut peers = Peers {
        caller: Client::new(ClientConfig {
            compression: Compression::new(&ACCEPT_RLE),
            ..ClientConfig::default()
        }),
        gateway: Lifecycle::new(ServerConfig {
            max_message_size: LIMIT,
            http2: protolink_http2::Config {
                max_pending_output_bytes: limit,
                ..ServerConfig::default().http2
            },
            ..ServerConfig::default()
        }),
    };
    let caller_id = peers.caller.start_streaming(PATH).unwrap();
    peers.caller.close_send(caller_id).unwrap();
    peers.exchange();
    assert!(peers.gateway.failure().is_none());
    let Some(CallEvent::Started { id, .. }) = peers.gateway.poll_event() else {
        panic!("call did not start");
    };

    peers
        .gateway
        .finish(id, Ok(()), ResponseMetadata::default());
    assert!(matches!(
        peers.gateway.failure(),
        Some(protolink_http2::Error::OutputLimitExceeded { limit: l, .. }) if *l == limit
    ));
    assert!(
        peers.gateway.draining().is_empty(),
        "no delivery is awaited"
    );
    assert!(peers.gateway.is_closed());
    assert!(peers.gateway.pending_output().len() <= limit);
    assert!(!peers.gateway.has_call(id));
}

/// Four-byte control payloads for one frame type and stream, in wire order.
fn control_values(output: &[u8], kind: u8, stream: CallId) -> Vec<u32> {
    let mut out: Vec<u32> = Vec::new();
    let mut rest = output;
    while !rest.is_empty() {
        assert!(rest.len() >= 9, "truncated frame header");
        let len = (usize::from(rest[0]) << 16) | (usize::from(rest[1]) << 8) | usize::from(rest[2]);
        assert!(rest.len() >= 9 + len, "truncated frame payload");
        let id = u32::from_be_bytes([rest[5] & 0x7f, rest[6], rest[7], rest[8]]);
        if rest[3] == kind {
            assert_eq!(len, 4, "control frame payload length");
            if id == stream {
                out.push(u32::from_be_bytes(rest[9..13].try_into().unwrap()));
            }
        }
        rest = &rest[9 + len..];
    }
    out
}

fn updates(output: &[u8], stream: CallId) -> Vec<u32> {
    control_values(output, 0x8, stream)
        .into_iter()
        .map(|value| value & 0x7fff_ffff)
        .collect()
}

fn resets(output: &[u8], stream: CallId) -> usize {
    control_values(output, 0x3, stream).len()
}

/// A request from a raw HTTP/2 peer for a call this lifecycle serves.
fn request() -> Vec<HeaderField> {
    vec![
        hf(":method", "POST"),
        hf(":scheme", "http"),
        hf(":path", PATH),
        hf(":authority", "localhost"),
        hf("content-type", "application/grpc"),
    ]
}

/// Only [`PATH`] is served.
fn resolve(path: &str) -> Option<()> {
    (path == PATH).then_some(())
}

#[test]
fn cancelled_request_tail_does_not_cancel_other_lifecycle_calls() {
    // A raw peer lets us stop mid-frame. Per-octet credit would exhaust 1 KiB
    // of blocked output and prevent the surviving call from completing.
    const OUTPUT_LIMIT: usize = 1024;
    let mut peer = Connection::client(protolink_http2::Config::default());
    let mut gateway = Lifecycle::<()>::new(ServerConfig {
        http2: protolink_http2::Config {
            max_pending_output_bytes: OUTPUT_LIMIT,
            ..ServerConfig::default().http2
        },
        ..ServerConfig::default()
    });
    gateway.recv(&peer.take_output(), resolve).unwrap();
    // The lifecycle's handshake (SETTINGS, its connection-window widening and
    // the ACK of the peer's SETTINGS) goes to the peer, so every later phase
    // measures only what that phase owes.
    peer.recv(&gateway.take_output()).unwrap();

    let a = peer.open_stream(request(), false).unwrap();
    let b = peer.open_stream(request(), false).unwrap();
    gateway.recv(&peer.take_output(), resolve).unwrap();
    peer.recv(&gateway.take_output()).unwrap();
    for (label, expected) in [("A", a), ("B", b)] {
        let Some(CallEvent::Started { id, .. }) = gateway.poll_event() else {
            panic!("{label} did not start");
        };
        assert_eq!(id, expected, "{label} is the call the peer opened");
    }
    assert!(gateway.poll_event().is_none(), "both calls are active");

    // One DATA frame on A, of which only the first octet is delivered: no
    // complete request message can be decoded, so nothing handler-visible can
    // happen early.
    let body = crate::lpm::encode(&vec![0x21; 295]).unwrap();
    let frame_size = body.len();
    peer.send_data(a, body, false).unwrap();
    let wire = peer.take_output();
    assert_eq!(wire.len(), 9 + frame_size, "exactly one DATA frame");

    gateway.recv(&wire[..10], resolve).unwrap();
    assert!(gateway.pending_output().is_empty(), "no mid-frame output");
    assert_eq!(
        gateway.buffered_request_bytes(a),
        Some(1),
        "one octet is buffered"
    );
    assert_eq!(
        gateway.request_count(a),
        0,
        "no request message is complete"
    );
    // The handler ends A while its request frame is still on the wire.
    gateway.finish(a, Ok(()), ResponseMetadata::default());
    let out = gateway.pending_output();
    assert!(!gateway.has_call(a), "A is over");
    assert!(gateway.has_call(b), "B is untouched");
    assert_eq!(resets(out, a), 1, "A's request side ends with one reset");
    assert_eq!(resets(out, b), 0, "B is never reset");
    assert_eq!(
        updates(out, 0),
        vec![1u32],
        "the delivered octet returns once"
    );
    assert!(
        updates(out, a).is_empty(),
        "its stream is gone, so only the connection owes it"
    );
    let blocked = out.len();
    assert!(blocked > 0 && blocked <= OUTPUT_LIMIT);
    assert!(
        gateway.poll_event().is_none(),
        "finishing a call is not a cancellation"
    );
    gateway.finish(a, Ok(()), ResponseMetadata::default());
    assert_eq!(gateway.pending_output().len(), blocked, "A ends only once");

    // Leave A's response/reset queued throughout the bytewise tail. No tail
    // credit is owed until the final frame byte arrives.
    for piece in wire[10..wire.len() - 1].chunks(1) {
        gateway.recv(piece, resolve).unwrap();
        assert_eq!(gateway.pending_output().len(), blocked, "tail incomplete");
    }
    gateway.recv(&wire[wire.len() - 1..], resolve).unwrap();
    let out = gateway.take_output();
    assert_eq!(out.len(), blocked + 13, "one tail WINDOW_UPDATE");
    assert!(out.len() <= OUTPUT_LIMIT, "bounded blocked output");
    assert_eq!(
        updates(&out, 0),
        vec![1u32, (frame_size - 1) as u32],
        "prefix and tail each return connection credit exactly once"
    );
    assert!(
        updates(&out, a).is_empty(),
        "closed A gets no stream credit"
    );
    assert!(updates(&out, b).is_empty(), "B owes no credit yet");
    assert_eq!(resets(&out, a), 1, "no duplicate reset during the tail");
    assert_eq!(resets(&out, b), 0, "B is never reset");
    assert!(!gateway.has_call(a));
    assert!(gateway.has_call(b));
    assert!(
        gateway.poll_event().is_none(),
        "discarded bytes report nothing"
    );
    assert!(
        gateway.failure().is_none(),
        "a mid-frame cancellation is not a connection failure"
    );
    peer.recv(&out).unwrap();

    // B is a real call: its request arrives without ending the stream, the handler
    // takes it, and a full response goes out and arrives intact. That is only
    // meaningful if cancelling A never disturbed this stream.
    peer.send_data(b, lpm::encode(b"ping").unwrap(), false)
        .unwrap();
    gateway.recv(&peer.take_output(), resolve).unwrap();
    assert!(
        updates(&gateway.take_output(), 0).is_empty(),
        "a complete message is withheld until the handler takes it"
    );
    assert_eq!(
        gateway.take_request(b),
        Ok(Some(b"ping".to_vec())),
        "B still takes its request message"
    );
    let out = gateway.take_output();
    assert_eq!(
        updates(&out, 0),
        vec![9u32],
        "the taken message is credited once"
    );
    assert_eq!(
        updates(&out, b),
        vec![9u32],
        "and its stream can still receive"
    );
    peer.recv(&out).unwrap();

    // The peer ends B's request side, so the handler can close the call normally
    // rather than needing a reset the way A did.
    peer.send_data(b, Vec::new(), true).unwrap();
    gateway.recv(&peer.take_output(), resolve).unwrap();
    assert!(gateway.is_half_closed(b), "B's request side has ended");
    assert!(
        updates(&gateway.take_output(), 0).is_empty(),
        "an empty frame owes nothing"
    );

    assert!(
        gateway.send_headers(b, Metadata::new()),
        "B can still send headers"
    );
    gateway.send_message(b, b"response").unwrap();
    gateway.finish(b, Ok(()), ResponseMetadata::default());
    assert!(
        gateway.failure().is_none(),
        "the connection stayed usable throughout"
    );

    // B's small response fits the peer's window, so one transfer delivers it.
    peer.recv(&gateway.take_output()).unwrap();
    assert!(!gateway.has_output());
    let mut payloads = Vec::new();
    let mut trailers = Vec::new();
    let mut resets = Vec::new();
    let mut ended = Vec::new();
    while let Some(event) = peer.poll_event() {
        match event {
            Event::Data { stream_id, data } if stream_id == b => payloads.push(data.to_vec()),
            Event::Headers {
                stream_id,
                headers,
                end_stream: true,
            } => {
                ended.push(stream_id);
                assert_eq!(crate::fields::header(&headers, "grpc-status"), Some("0"));
                if stream_id == b {
                    trailers.push(headers);
                }
            }
            Event::DataEnd {
                stream_id,
                end_stream: true,
            } => ended.push(stream_id),
            Event::Reset { stream_id, .. } => resets.push(stream_id),
            _ => {}
        }
    }
    assert_eq!(
        payloads,
        [lpm::encode(b"response").unwrap()],
        "B's message arrives framed and intact"
    );
    assert!(!peer.has_stream(b), "B closed cleanly on the peer as well");
    assert_eq!(resets, [a], "only A was reset, exactly once");
    assert_eq!(ended, [a, b], "both calls ended exactly once");
    assert_eq!(
        trailers.len(),
        1,
        "B received exactly one set of success trailers"
    );
    assert!(
        gateway.draining().is_empty(),
        "B's output was fully delivered"
    );
    assert_eq!(gateway.active_calls(), 0, "no call is left behind");
    assert!(gateway.poll_event().is_none(), "no spurious cancellation");
}
