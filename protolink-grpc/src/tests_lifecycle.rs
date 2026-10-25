//! Tests of the lifecycle's response rules, driven directly: a plain [`Client`]
//! is the caller, and the test answers each started call by hand.

use alloc::vec;
use alloc::vec::Vec;

use super::*;
use crate::compression::{Codec, Compression};
use crate::lifecycle::{CallEvent, Lifecycle};
use crate::test_support::drain;
use crate::tests_compression::{ACCEPT_RLE, RLE};

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
