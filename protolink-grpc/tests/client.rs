//! Client-only raw-peer regressions for response lifetime and validation.

use core::time::Duration;

use protolink_grpc::compression::{Codec, CodecError};
use protolink_grpc::http2::{Config, Connection, ErrorCode, Event, HeaderField};
use protolink_grpc::{
    CallId, CallOptions, Client, ClientConfig, Code, Compression, Next, Status, lpm,
};

fn field(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}

fn encoded(message: &[u8]) -> Vec<u8> {
    lpm::encode(message).unwrap()
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

fn setup(config: ClientConfig) -> (Client, Connection) {
    let mut client = Client::new(config);
    let mut peer = Connection::server(Config::default());
    exchange(&mut client, &mut peer);
    (client, peer)
}

fn open(client: &mut Client, peer: &mut Connection, unary: bool) -> CallId {
    open_with(client, peer, unary, &CallOptions::new())
}

fn open_with(
    client: &mut Client,
    peer: &mut Connection,
    unary: bool,
    options: &CallOptions,
) -> CallId {
    let id = if unary {
        client
            .start_unary_with("/test.Client/Unary", b"", options)
            .unwrap()
    } else {
        let id = client
            .start_streaming_with("/test.Client/Stream", options)
            .unwrap();
        client.close_send(id).unwrap();
        id
    };
    exchange(client, peer);
    while peer.poll_event().is_some() {}
    id
}

fn headers(peer: &mut Connection, id: CallId) {
    peer.send_headers(
        id,
        vec![
            field(":status", "200"),
            field("content-type", "application/grpc"),
        ],
        false,
    )
    .unwrap();
}

fn finish(peer: &mut Connection, id: CallId) {
    peer.send_headers(id, vec![field("grpc-status", "0")], true)
        .unwrap();
}

fn deliver(client: &mut Client, peer: &mut Connection) {
    client.recv(&peer.take_output()).unwrap();
}

/// Connection WINDOW_UPDATE increments in already-handshaken output.
fn connection_credit(bytes: &[u8]) -> usize {
    credit(bytes).0
}

fn informational_headers_then_grpc_response(unary: bool) {
    for statuses in [&["100"][..], &["103"][..], &["100", "103"][..]] {
        let (mut client, mut peer) = setup(ClientConfig::default());
        let id = open(&mut client, &mut peer, unary);
        for status in statuses {
            peer.send_headers(
                id,
                vec![
                    field(":status", status),
                    field("x-informational", "must not retain"),
                    field("x-initial", "informational"),
                ],
                false,
            )
            .unwrap();
            deliver(&mut client, &mut peer);
            assert!(client.is_pending(id));
            assert!(
                client.response_headers(id).is_none(),
                "informational metadata is not initial response metadata"
            );
            assert_eq!(client.retained_response_bytes(), 0);
            assert!(client.take_response(id).is_none());
            assert!(client.try_next(id).is_none());
        }
        peer.send_headers(
            id,
            vec![
                field(":status", "200"),
                field("content-type", "application/grpc"),
                field("x-initial", "final"),
            ],
            false,
        )
        .unwrap();
        deliver(&mut client, &mut peer);
        let initial = client.response_headers(id).unwrap();
        assert_eq!(initial.get("x-initial"), Some("final"));
        assert_eq!(initial.get("x-informational"), None);
        peer.send_data(id, encoded(b"response"), false).unwrap();
        peer.send_headers(
            id,
            vec![field("grpc-status", "0"), field("x-trailer", "terminal")],
            true,
        )
        .unwrap();
        deliver(&mut client, &mut peer);
        let (initial, trailers) = if unary {
            let response = client.take_response(id).unwrap().unwrap();
            assert_eq!(response.message, b"response");
            (response.headers, response.trailers)
        } else {
            assert_eq!(
                client.try_next(id),
                Some(Next::Message(b"response".to_vec()))
            );
            assert_eq!(client.try_next(id), Some(Next::Done(Ok(()))));
            let (initial, trailers) = client.take_metadata(id);
            (initial.unwrap(), trailers)
        };
        assert_eq!(initial.get("x-initial"), Some("final"));
        assert_eq!(initial.get("x-informational"), None);
        assert_eq!(trailers.get("x-trailer"), Some("terminal"));
        assert_eq!(trailers.get("x-informational"), None);
        assert_eq!(client.retained_response_bytes(), 0);
    }
}

#[test]
fn unary_ignores_informational_headers_before_final_response() {
    informational_headers_then_grpc_response(true);
}

#[test]
fn streaming_ignores_informational_headers_before_final_response() {
    informational_headers_then_grpc_response(false);
}

#[test]
fn repeated_completions_fill_byte_budget_until_consumed_or_discarded() {
    for unary in [true, false] {
        let (mut client, mut peer) = setup(ClientConfig {
            max_buffered_response_bytes: 8 * 1005,
            ..ClientConfig::default()
        });
        let mut ids = Vec::new();
        for _ in 0..8 {
            let id = open(&mut client, &mut peer, unary);
            headers(&mut peer, id);
            peer.send_data(id, encoded(&[42; 1000]), false).unwrap();
            finish(&mut peer, id);
            exchange(&mut client, &mut peer);
            assert!(!client.is_pending(id));
            ids.push(id);
        }
        assert_eq!(client.retained_response_bytes(), 8 * 1005);
        assert_eq!(
            client
                .start_streaming("/test.Client/Stream")
                .unwrap_err()
                .code,
            Code::ResourceExhausted
        );
        assert!(!client.has_output());
        if unary {
            assert_eq!(
                client.take_response(ids[0]).unwrap().unwrap().message,
                [42; 1000]
            );
        } else {
            assert_eq!(client.try_next(ids[0]), Some(Next::Message(vec![42; 1000])));
            assert_eq!(client.try_next(ids[0]), Some(Next::Done(Ok(()))));
            client.take_metadata(ids[0]);
        }
        assert_eq!(connection_credit(&client.take_output()), 1005);
        let id = open(&mut client, &mut peer, unary);
        headers(&mut peer, id);
        peer.send_data(id, encoded(&[42; 1000]), false).unwrap();
        finish(&mut peer, id);
        exchange(&mut client, &mut peer);
        assert_eq!(client.retained_response_bytes(), 8 * 1005);
        ids.push(id);
        for id in ids {
            client.cancel(id);
            client.cancel(id);
        }
        assert_eq!(connection_credit(&client.take_output()), 8 * 1005);
        assert_eq!(client.retained_response_bytes(), 0);
        client.start_streaming("/test.Client/Stream").unwrap();
    }
}

#[test]
fn completed_unread_stream_still_backpressures_next_stream() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let a = open(&mut client, &mut peer, false);
    headers(&mut peer, a);
    let body: Vec<u8> = (0..65).flat_map(|_| encoded(&[42; 1000])).collect();
    peer.send_data(a, body.clone(), false).unwrap();
    finish(&mut peer, a);
    exchange(&mut client, &mut peer);
    assert!(!client.is_pending(a));
    assert_eq!(client.buffered_response_bytes(a), Some(65 * 1005));
    let b = open(&mut client, &mut peer, false);
    headers(&mut peer, b);
    peer.send_data(b, body, false).unwrap();
    finish(&mut peer, b);
    exchange(&mut client, &mut peer);
    assert!(
        client.is_pending(b),
        "unread completion must not reopen the connection window"
    );
    assert!(peer.queued_send_bytes(b).is_some_and(|n| n > 0));
    assert!(client.retained_response_bytes() < 2 * 65 * 1005);
    client.cancel(a);
    exchange(&mut client, &mut peer);
    assert!(
        !client.is_pending(b),
        "discarding the first result must resume the peer"
    );
    assert_eq!(client.buffered_response_bytes(b), Some(65 * 1005));
    for _ in 0..65 {
        assert_eq!(client.try_next(b), Some(Next::Message(vec![42; 1000])));
    }
    assert_eq!(client.try_next(b), Some(Next::Done(Ok(()))));
    assert_eq!(client.retained_response_bytes(), 0);
}

#[test]
fn explicit_success_status_overrides_non_200_http_status() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, true);
    peer.send_headers(
        id,
        vec![
            field(":status", "503"),
            field("content-type", "application/grpc"),
        ],
        false,
    )
    .unwrap();
    peer.send_data(id, encoded(b"success"), false).unwrap();
    finish(&mut peer, id);
    deliver(&mut client, &mut peer);
    assert_eq!(
        client.take_response(id).unwrap().unwrap().message,
        b"success"
    );
}

#[test]
fn decoder_failure_retains_valid_messages_and_releases_discarded_tail_once() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, false);
    headers(&mut peer, id);
    let mut body = encoded(b"one");
    body.extend_from_slice(&[2, 0, 0, 0, 0]);
    peer.send_data(id, body, false).unwrap();
    deliver(&mut client, &mut peer);
    assert_eq!(connection_credit(&client.take_output()), 5);
    assert_eq!(client.retained_response_bytes(), 8);
    assert_eq!(client.try_next(id), Some(Next::Message(b"one".to_vec())));
    assert_eq!(connection_credit(&client.take_output()), 8);
    let Some(Next::Done(Err(status))) = client.try_next(id) else {
        panic!("missing decoder failure");
    };
    assert_eq!(status.code, Code::Internal);
    client.cancel(id);
    assert_eq!(connection_credit(&client.take_output()), 0);
}

#[test]
fn completed_unary_holds_credit_until_consumed_once() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, true);
    headers(&mut peer, id);
    peer.send_data(id, encoded(b"hello"), false).unwrap();
    finish(&mut peer, id);
    deliver(&mut client, &mut peer);
    assert!(!client.is_pending(id));
    assert_eq!(client.buffered_response_bytes(id), Some(10));
    assert_eq!(client.retained_response_bytes(), 10);
    assert_eq!(
        connection_credit(&client.take_output()),
        0,
        "completion must not return retained credit"
    );
    assert_eq!(client.take_response(id).unwrap().unwrap().message, b"hello");
    assert_eq!(connection_credit(&client.take_output()), 10);
    assert_eq!(client.retained_response_bytes(), 0);
    assert!(client.take_response(id).is_none());
    client.cancel(id);
    assert_eq!(connection_credit(&client.take_output()), 0);
}

/// A response that is transferred rather than copied keeps the capacity of its
/// frame, and its retention charge must still cover that capacity exactly.
#[test]
fn eligible_response_is_charged_its_framed_bytes_and_budget_is_exact() {
    let body = encoded(&[42; 1000]);
    let framed = body.len();

    let (mut client, mut peer) = setup(ClientConfig {
        max_buffered_response_bytes: framed,
        ..ClientConfig::default()
    });
    let id = open(&mut client, &mut peer, true);
    headers(&mut peer, id);
    peer.send_data(id, body.clone(), false).unwrap();
    finish(&mut peer, id);
    deliver(&mut client, &mut peer);
    assert_eq!(client.buffered_response_bytes(id), Some(framed));
    assert_eq!(client.retained_response_bytes(), framed);
    let response = client.take_response(id).unwrap().unwrap();
    assert_eq!(response.message, [42; 1000]);
    assert!(response.message.capacity() <= framed);
    assert_eq!(client.retained_response_bytes(), 0);
    assert_eq!(connection_credit(&client.take_output()), framed);

    let (mut client, mut peer) = setup(ClientConfig {
        max_buffered_response_bytes: framed - 1,
        ..ClientConfig::default()
    });
    let id = open(&mut client, &mut peer, true);
    headers(&mut peer, id);
    peer.send_data(id, body, false).unwrap();
    finish(&mut peer, id);
    deliver(&mut client, &mut peer);
    assert_eq!(
        client.take_response(id).unwrap().unwrap_err().code,
        Code::ResourceExhausted
    );
}

#[test]
fn completed_stream_holds_credit_until_messages_consumed_once() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, false);
    headers(&mut peer, id);
    let mut body = encoded(b"one");
    body.extend(encoded(b"two"));
    peer.send_data(id, body, false).unwrap();
    finish(&mut peer, id);
    deliver(&mut client, &mut peer);
    assert_eq!(connection_credit(&client.take_output()), 0);
    assert_eq!(client.try_next(id), Some(Next::Message(b"one".to_vec())));
    assert_eq!(connection_credit(&client.take_output()), 8);
    assert_eq!(client.try_next(id), Some(Next::Message(b"two".to_vec())));
    assert_eq!(connection_credit(&client.take_output()), 8);
    assert_eq!(client.try_next(id), Some(Next::Done(Ok(()))));
    assert_eq!(client.try_next(id), None);
    client.cancel(id);
    assert_eq!(connection_credit(&client.take_output()), 0);
}

#[test]
fn cancellation_discards_completed_unary_and_stream_credit_once() {
    for unary in [true, false] {
        let (mut client, mut peer) = setup(ClientConfig::default());
        let id = open(&mut client, &mut peer, unary);
        headers(&mut peer, id);
        peer.send_data(id, encoded(b"retained"), false).unwrap();
        finish(&mut peer, id);
        deliver(&mut client, &mut peer);
        assert_eq!(connection_credit(&client.take_output()), 0);
        client.cancel(id);
        assert_eq!(client.retained_response_bytes(), 0);
        assert_eq!(connection_credit(&client.take_output()), 13);
        client.cancel(id);
        assert_eq!(connection_credit(&client.take_output()), 0);
        assert!(client.take_response(id).is_none());
        assert!(client.try_next(id).is_none());
    }
}

#[test]
fn unary_failure_discards_body_credit_once() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, true);
    headers(&mut peer, id);
    peer.send_data(id, encoded(b"discard"), false).unwrap();
    peer.send_headers(id, vec![field("grpc-status", "7")], true)
        .unwrap();
    deliver(&mut client, &mut peer);
    assert_eq!(connection_credit(&client.take_output()), 12);
    assert_eq!(
        client.take_response(id).unwrap().unwrap_err().code,
        Code::PermissionDenied
    );
    client.cancel(id);
    assert_eq!(connection_credit(&client.take_output()), 0);
}

#[test]
fn partial_message_credit_is_not_released_twice_on_completion() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, true);
    headers(&mut peer, id);
    let body = encoded(b"hello");
    peer.send_data(id, body[..7].to_vec(), false).unwrap();
    deliver(&mut client, &mut peer);
    assert_eq!(client.retained_response_bytes(), 7);
    assert_eq!(connection_credit(&client.take_output()), 7);
    peer.send_data(id, body[7..].to_vec(), false).unwrap();
    finish(&mut peer, id);
    deliver(&mut client, &mut peer);
    assert_eq!(connection_credit(&client.take_output()), 0);
    assert_eq!(client.take_response(id).unwrap().unwrap().message, b"hello");
    assert_eq!(connection_credit(&client.take_output()), 3);
    client.cancel(id);
    assert_eq!(connection_credit(&client.take_output()), 0);
}

#[test]
fn partial_message_cancellation_releases_only_uncredited_tail() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, false);
    headers(&mut peer, id);
    let mut body = encoded(b"ok");
    body.extend_from_slice(&encoded(b"unfinished")[..6]);
    peer.send_data(id, body, false).unwrap();
    deliver(&mut client, &mut peer);
    assert_eq!(connection_credit(&client.take_output()), 0);
    client.cancel(id);
    assert_eq!(connection_credit(&client.take_output()), 13);
    client.cancel(id);
    assert_eq!(connection_credit(&client.take_output()), 0);
}

#[test]
fn retained_results_bound_admission_and_consumption_reopens_it() {
    for unary in [true, false] {
        let (mut client, mut peer) = setup(ClientConfig {
            max_retained_calls: 2,
            ..ClientConfig::default()
        });
        let mut ids = Vec::new();
        for _ in 0..2 {
            let id = open(&mut client, &mut peer, unary);
            headers(&mut peer, id);
            peer.send_data(id, encoded(b"retained"), false).unwrap();
            finish(&mut peer, id);
            exchange(&mut client, &mut peer);
            ids.push(id);
        }
        assert_eq!(client.retained_response_bytes(), 26);
        assert_eq!(
            client
                .start_streaming("/test.Client/Stream")
                .unwrap_err()
                .code,
            Code::ResourceExhausted
        );
        assert!(
            !client.has_output(),
            "failed admission must not send headers"
        );
        if unary {
            client.take_response(ids[0]).unwrap().unwrap();
        } else {
            assert!(matches!(client.try_next(ids[0]), Some(Next::Message(_))));
            assert_eq!(client.try_next(ids[0]), Some(Next::Done(Ok(()))));
            // Finished metadata also occupies an admission slot until taken.
            assert!(client.start_streaming("/test.Client/Stream").is_err());
            client.take_metadata(ids[0]);
        }
        assert_eq!(client.retained_response_bytes(), 13);
        let id = client.start_streaming("/test.Client/Stream").unwrap();
        client.cancel(id);
        client.cancel(ids[1]);
        assert_eq!(client.retained_response_bytes(), 0);
    }
}

#[derive(Debug)]
struct Expand;
impl Codec for Expand {
    fn name(&self) -> &'static str {
        "expand"
    }
    fn compress(&self, _: &[u8], _: &mut Vec<u8>) -> Result<(), CodecError> {
        Err(CodecError::Unsupported)
    }
    fn decompress(&self, _: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError> {
        if limit < 100 {
            return Err(CodecError::TooLarge);
        }
        out.extend_from_slice(&[42; 100]);
        Ok(())
    }
}
static EXPAND: Expand = Expand;
static ACCEPT: [&dyn Codec; 1] = [&EXPAND];

#[test]
fn decompressed_results_and_partial_messages_share_retention_budget() {
    for unary in [true, false] {
        let (mut client, mut peer) = setup(ClientConfig {
            compression: Compression::new(&ACCEPT),
            max_buffered_response_bytes: 110,
            ..ClientConfig::default()
        });
        let a = open(&mut client, &mut peer, unary);
        let b = open(&mut client, &mut peer, unary);
        for id in [a, b] {
            peer.send_headers(
                id,
                vec![
                    field(":status", "200"),
                    field("content-type", "application/grpc"),
                    field("grpc-encoding", "expand"),
                ],
                false,
            )
            .unwrap();
        }
        peer.send_data(a, vec![1, 0, 0, 0, 1, 9], false).unwrap();
        finish(&mut peer, a);
        deliver(&mut client, &mut peer);
        assert_eq!(client.retained_response_bytes(), 105);
        // A five-byte partial prefix fits, even though it has already returned
        // wire credit. The next byte cannot bypass the global budget.
        peer.send_data(b, vec![1, 0, 0, 0, 1], false).unwrap();
        deliver(&mut client, &mut peer);
        assert_eq!(client.retained_response_bytes(), 110);
        assert_eq!(
            client
                .start_streaming("/test.Client/Stream")
                .unwrap_err()
                .code,
            Code::ResourceExhausted
        );
        peer.send_data(b, vec![9], false).unwrap();
        deliver(&mut client, &mut peer);
        assert_eq!(client.retained_response_bytes(), 105);
        let code = if unary {
            client.take_response(b).unwrap().unwrap_err().code
        } else {
            let Some(Next::Done(Err(status))) = client.try_next(b) else {
                panic!("missing budget failure");
            };
            status.code
        };
        assert_eq!(code, Code::ResourceExhausted);
        client.cancel(a);
        assert_eq!(client.retained_response_bytes(), 0);
        client.start_streaming("/test.Client/Stream").unwrap();
    }
}

#[test]
fn compressed_message_expansion_cannot_exceed_budget() {
    let (mut client, mut peer) = setup(ClientConfig {
        compression: Compression::new(&ACCEPT),
        max_buffered_response_bytes: 100,
        ..ClientConfig::default()
    });
    let id = open(&mut client, &mut peer, true);
    peer.send_headers(
        id,
        vec![
            field(":status", "200"),
            field("content-type", "application/grpc"),
            field("grpc-encoding", "expand"),
        ],
        false,
    )
    .unwrap();
    peer.send_data(id, vec![1, 0, 0, 0, 1, 9], false).unwrap();
    deliver(&mut client, &mut peer);
    assert_eq!(
        client.take_response(id).unwrap().unwrap_err().code,
        Code::ResourceExhausted
    );
    assert_eq!(client.retained_response_bytes(), 0);
    assert_eq!(connection_credit(&client.take_output()), 6);
}

#[test]
fn explicit_terminal_status_overrides_http_fallback_and_preserves_metadata() {
    for trailers_only in [true, false] {
        for unary in [true, false] {
            let (mut client, mut peer) = setup(ClientConfig::default());
            let id = open(&mut client, &mut peer, unary);
            let mut initial = vec![
                field(":status", "503"),
                field("content-type", "application/grpc"),
            ];
            let terminal = vec![
                field("grpc-status", "7"),
                field("grpc-message", "denied%20here"),
                field("x-detail", "kept"),
            ];
            if trailers_only {
                initial.extend(terminal);
                peer.send_headers(id, initial, true).unwrap();
            } else {
                peer.send_headers(id, initial, false).unwrap();
                deliver(&mut client, &mut peer);
                assert!(client.is_pending(id), "HTTP fallback is not terminal yet");
                peer.send_headers(id, terminal, true).unwrap();
            }
            deliver(&mut client, &mut peer);
            let status = if unary {
                client.take_response(id).unwrap().unwrap_err()
            } else {
                let Some(Next::Done(Err(status))) = client.try_next(id) else {
                    panic!("missing status");
                };
                status
            };
            assert_eq!(status.code, Code::PermissionDenied);
            assert_eq!(status.message, "denied here");
            assert_eq!(status.metadata.get("x-detail"), Some("kept"));
        }
    }
}

#[test]
fn explicit_error_status_survives_non_grpc_content_type() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, true);
    peer.send_headers(
        id,
        vec![
            field(":status", "503"),
            field("content-type", "application/json"),
            field("grpc-status", "7"),
            field("grpc-message", "authoritative"),
        ],
        true,
    )
    .unwrap();
    deliver(&mut client, &mut peer);
    let status = client.take_response(id).unwrap().unwrap_err();
    assert_eq!(status.code, Code::PermissionDenied);
    assert_eq!(status.message, "authoritative");
}

#[test]
fn absent_grpc_status_uses_http_fallback_only_at_terminal() {
    for trailers_only in [true, false] {
        let (mut client, mut peer) = setup(ClientConfig::default());
        let id = open(&mut client, &mut peer, true);
        peer.send_headers(
            id,
            vec![
                field(":status", "503"),
                field("content-type", "application/json"),
            ],
            trailers_only,
        )
        .unwrap();
        deliver(&mut client, &mut peer);
        if !trailers_only {
            assert!(client.is_pending(id));
            peer.send_data(id, b"not grpc".to_vec(), false).unwrap();
            peer.send_headers(id, vec![field("x-detail", "fallback")], true)
                .unwrap();
            deliver(&mut client, &mut peer);
        }
        let status = client.take_response(id).unwrap().unwrap_err();
        assert_eq!(status.code, Code::Unavailable);
        if !trailers_only {
            assert_eq!(status.metadata.get("x-detail"), Some("fallback"));
        }
        assert_eq!(client.retained_response_bytes(), 0);
    }
}

#[test]
fn missing_or_wrong_content_type_never_accepts_grpc_body() {
    for content_type in [
        None,
        Some("application/json"),
        Some("application/grpc-web"),
        Some("application/grpc+"),
    ] {
        for unary in [true, false] {
            let (mut client, mut peer) = setup(ClientConfig::default());
            let id = open(&mut client, &mut peer, unary);
            let mut initial = vec![field(":status", "200")];
            if let Some(value) = content_type {
                initial.push(field("content-type", value));
            }
            peer.send_headers(id, initial, false).unwrap();
            peer.send_data(id, encoded(b"must not deliver"), false)
                .unwrap();
            finish(&mut peer, id);
            deliver(&mut client, &mut peer);
            let status = if unary {
                client.take_response(id).unwrap().unwrap_err()
            } else {
                let Some(Next::Done(Err(status))) = client.try_next(id) else {
                    panic!("accepted non-gRPC body");
                };
                status
            };
            assert_eq!(status.code, Code::Internal);
            assert!(status.message.contains("content-type"));
            assert_eq!(client.retained_response_bytes(), 0);
        }
    }
}

#[test]
fn grpc_content_type_suffix_and_parameters_are_accepted() {
    for value in [
        "application/grpc",
        "application/grpc+proto",
        "application/grpc; charset=utf-8",
        "application/grpc+proto; charset=utf-8",
    ] {
        let (mut client, mut peer) = setup(ClientConfig::default());
        let id = open(&mut client, &mut peer, true);
        peer.send_headers(
            id,
            vec![field(":status", "200"), field("content-type", value)],
            false,
        )
        .unwrap();
        peer.send_data(id, encoded(b"ok"), false).unwrap();
        finish(&mut peer, id);
        deliver(&mut client, &mut peer);
        assert_eq!(client.take_response(id).unwrap().unwrap().message, b"ok");
    }
}

#[test]
fn data_end_stream_resets_open_request_half_and_allows_stream_reuse() {
    let mut client = Client::new(ClientConfig::default());
    let mut peer = Connection::server(Config {
        max_concurrent_streams: 1,
        initial_window_size: 0,
        ..Config::default()
    });
    exchange(&mut client, &mut peer);
    let id = client.start_streaming("/test.Client/Bidi").unwrap();
    exchange(&mut client, &mut peer);
    while peer.poll_event().is_some() {}
    client.send_message(id, b"queued request").unwrap();
    assert_eq!(client.queued_request_bytes(id), Some(19));
    headers(&mut peer, id);
    peer.send_data(id, encoded(b"last response"), true).unwrap();
    deliver(&mut client, &mut peer);
    assert_eq!(
        client.queued_request_bytes(id),
        None,
        "malformed response must reclaim the request half"
    );
    assert_eq!(
        client.try_next(id),
        Some(Next::Message(b"last response".to_vec()))
    );
    let Some(Next::Done(Err(status))) = client.try_next(id) else {
        panic!("missing malformed-response status");
    };
    assert_eq!(status.code, Code::Internal);
    exchange(&mut client, &mut peer);
    assert!(core::iter::from_fn(|| peer.poll_event()).any(|e| matches!(e, Event::Reset { stream_id, error_code: ErrorCode::ProtocolError } if stream_id == id)));
    let next = client.start_streaming("/test.Client/Bidi").unwrap();
    exchange(&mut client, &mut peer);
    assert!(
        core::iter::from_fn(|| peer.poll_event())
            .any(|e| matches!(e, Event::Headers { stream_id, .. } if stream_id == next))
    );
    assert!(client.is_pending(next));
}

#[test]
fn terminal_failure_keeps_stream_messages_but_drops_partial_tail() {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let id = open(&mut client, &mut peer, false);
    headers(&mut peer, id);
    let mut body = encoded(b"kept");
    body.extend_from_slice(&[0, 0, 0]);
    peer.send_data(id, body, false).unwrap();
    finish(&mut peer, id);
    deliver(&mut client, &mut peer);
    assert_eq!(client.retained_response_bytes(), 9);
    assert_eq!(connection_credit(&client.take_output()), 3);
    assert_eq!(client.try_next(id), Some(Next::Message(b"kept".to_vec())));
    assert_eq!(connection_credit(&client.take_output()), 9);
    let Some(Next::Done(Err(status))) = client.try_next(id) else {
        panic!("missing truncation status");
    };
    assert_eq!(status.code, Code::Internal);
    assert_eq!(client.retained_response_bytes(), 0);
}

/// A client with `limit` bytes of pending output, with one unary call whose
/// response has been received but not taken, and one streaming call whose
/// request message is queued but not drained. Returns both calls' ids.
fn retained_result_scenario(limit: usize) -> (Client, Connection, CallId, CallId) {
    let config = ClientConfig {
        http2: Config {
            max_pending_output_bytes: limit,
            ..ClientConfig::default().http2
        },
        ..ClientConfig::default()
    };
    let (mut client, mut peer) = setup(config);
    let unary = open(&mut client, &mut peer, true);
    headers(&mut peer, unary);
    peer.send_data(unary, encoded(b"reply"), false).unwrap();
    finish(&mut peer, unary);
    deliver(&mut client, &mut peer);
    let streaming = client.start_streaming("/test.Client/Stream").unwrap();
    client.send_message(streaming, &[1; 3000]).unwrap();
    (client, peer, unary, streaming)
}

#[test]
fn output_limit_result_consumption_retires_client() {
    // Measure the output of the scenario, then set the limit so that it just
    // fits: taking the unary result must then send a credit update, which does
    // not. Data is admitted at its exact size, so the limit can be that tight.
    let (measured, _, _, _) = retained_result_scenario(usize::MAX);
    let pending = measured.pending_output().len();
    let (mut client, _, unary, streaming) = retained_result_scenario(pending + 12);
    assert_eq!(client.pending_output().len(), pending);
    assert!(client.failure().is_none());

    let result = client
        .take_response(unary)
        .expect("the response was received");
    assert_eq!(result.unwrap().message, b"reply");
    assert!(matches!(
        client.failure(),
        Some(protolink_grpc::http2::Error::OutputLimitExceeded { limit, .. }) if *limit == pending + 12
    ));
    assert!(client.is_closed());
    assert_eq!(
        client.pending_output().len(),
        pending,
        "no credit update was written"
    );

    // The other call ends as an overload, not a protocol error.
    let Some(protolink_grpc::Next::Done(Err(status))) = client.try_next(streaming) else {
        panic!("the call did not end with a status");
    };
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("output limit"),
        "{}",
        status.message
    );
    assert!(client.send_message(streaming, b"late").is_err());
}

#[test]
fn unary_body_overflow_adopts_failure_for_other_calls() {
    let limit = 1024;
    let config = ClientConfig {
        http2: Config {
            max_pending_output_bytes: limit,
            ..ClientConfig::default().http2
        },
        ..ClientConfig::default()
    };
    let (mut client, _peer) = setup(config);
    let streaming = client.start_streaming("/test.Client/Stream").unwrap();

    // The headers of this unary call fit; its body does not, so the call fails
    // to start.
    let status = client
        .start_unary_with(
            "/test.Client/Unary",
            &[7; 2000],
            &protolink_grpc::CallOptions::default(),
        )
        .unwrap_err();
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("output limit"),
        "{}",
        status.message
    );
    assert!(matches!(
        client.failure(),
        Some(protolink_grpc::http2::Error::OutputLimitExceeded { limit: l, .. }) if *l == limit
    ));
    // The call that never started is not left behind, active or retained.
    assert!(!client.is_pending(streaming + 2));
    assert!(client.take_response(streaming + 2).is_none());
    // The call that was already open learns of the failure without a tick.
    let Some(protolink_grpc::Next::Done(Err(status))) = client.try_next(streaming) else {
        panic!("the open call did not end with the connection's failure");
    };
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("output limit"),
        "{}",
        status.message
    );
}

#[test]
fn fragmented_non_grpc_error_body_keeps_credit_bounded_and_peer_alive() {
    // Keep client TX blocked while fragmenting A's terminal ignored body.
    // Control output must stay bounded and the unrelated call B must survive.
    let body = vec![b'x'; 16_384];
    for small_budget in [false, true] {
        let mut config = ClientConfig::default();
        if small_budget {
            // A deliberately tight buffer that still fits the setup.
            config.http2.max_pending_output_bytes = 1024;
        }
        for chunk in [16_384, 512, 1] {
            let (mut client, mut peer) = setup(config.clone());
            let a = open(&mut client, &mut peer, true);
            let b = open(&mut client, &mut peer, true);

            peer.send_headers(
                a,
                vec![field(":status", "503"), field("content-type", "text/html")],
                false,
            )
            .unwrap();
            peer.send_data(a, body.clone(), true).unwrap();
            let response = peer.take_output();

            client.take_output();
            for piece in response.chunks(chunk) {
                client.recv(piece).unwrap_or_else(|error| {
                    panic!("chunk={chunk} small_budget={small_budget}: {error:?}")
                });
            }
            assert!(client.failure().is_none());
            let frames = decoded_frames(&client.take_output()).len();
            assert!(frames <= 2, "flooded {frames} frames at chunk={chunk}");

            let status = client.take_response(a).unwrap().unwrap_err();
            assert_eq!(status.code, Code::Unavailable);
            assert_eq!(client.retained_response_bytes(), 0);

            complete_unrelated_call(&mut client, &mut peer, b);
        }
    }
}

const DATA_FRAME: u8 = 0;
const RST_STREAM_FRAME: u8 = 3;
const WINDOW_UPDATE_FRAME: u8 = 8;
const WINDOW: usize = 16_384;

/// Whole, already-handshaken frames as `(type, stream, payload)`; reject truncation.
fn decoded_frames(mut bytes: &[u8]) -> Vec<(u8, u32, &[u8])> {
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        assert!(bytes.len() >= 9, "truncated frame header");
        let len =
            (usize::from(bytes[0]) << 16) | (usize::from(bytes[1]) << 8) | usize::from(bytes[2]);
        assert!(bytes.len() - 9 >= len, "truncated {len}-byte payload");
        let id = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) & 0x7fff_ffff;
        frames.push((bytes[3], id, &bytes[9..9 + len]));
        bytes = &bytes[9 + len..];
    }
    frames
}

/// `(connection_bytes, connection_frames, stream_bytes, stream_frames)`.
/// Check both amounts and counts: totals alone miss floods; counts alone miss lost credit.
fn credit(bytes: &[u8]) -> (usize, usize, usize, usize) {
    let mut credit = (0, 0, 0, 0);
    for (kind, id, payload) in decoded_frames(bytes) {
        if kind == WINDOW_UPDATE_FRAME {
            let increment = (u32::from_be_bytes(payload.try_into().expect("WINDOW_UPDATE payload"))
                & 0x7fff_ffff) as usize;
            if id == 0 {
                credit.0 += increment;
                credit.1 += 1;
            } else {
                credit.2 += increment;
                credit.3 += 1;
            }
        }
    }
    credit
}

/// DATA payload byte counts in `bytes` as `(stream, payload_len)` pairs.
fn data_frames(bytes: &[u8]) -> Vec<(u32, usize)> {
    decoded_frames(bytes)
        .iter()
        .filter(|frame| frame.0 == DATA_FRAME)
        .map(|frame| (frame.1, frame.2.len()))
        .collect()
}

#[test]
fn ignored_body_returns_credit_before_terminal_and_unblocks_next_frame() {
    // Six nonterminal frames exceed both flow-control windows. Terminal cleanup
    // cannot stand in for frame credit: each release must unblock the real peer.
    for unary in [true, false] {
        // Zero selects a deterministic irregular cycle; other values are fixed
        // transport chunks, with MAX feeding the entire prefix at once.
        for chunk in [1, 2, 32, 64, 128, 512, usize::MAX, 0] {
            ignored_body_credit_liveness(unary, chunk);
        }
    }
}

fn ignored_body_credit_liveness(unary: bool, chunk: usize) {
    let (mut client, mut peer) = setup(small_window_config());
    let a = open(&mut client, &mut peer, unary);
    let b = open(&mut client, &mut peer, true);
    assert_eq!(peer.send_capacity(a), Some(WINDOW));
    ignored_headers(&mut client, &mut peer, a, html_error_headers());
    assert!(client.is_pending(a));
    assert!(client.take_response(a).is_none());
    assert!(client.try_next(a).is_none());
    assert_eq!(client.retained_response_bytes(), 0);

    let body = vec![b'x'; WINDOW];
    peer.send_data(a, body.clone(), false).unwrap();
    assert_eq!(peer.queued_send_bytes(a), Some(0));
    let mut wire = peer.take_output();
    assert_eq!(data_frames(&wire), [(a, WINDOW)]);

    let mut total = (0, 0);
    for frame in 0..6 {
        // Queue the next frame now; only forwarding this frame's credit may unblock it.
        if frame < 5 {
            peer.send_data(a, body.clone(), false).unwrap();
            assert_eq!(peer.queued_send_bytes(a), Some(WINDOW));
            assert!(peer.pending_output().is_empty());
        }
        let last = wire.len() - 1;
        let mut at = 0;
        let mut piece = 0;
        while at < last {
            let size = if chunk == 0 {
                [7, 2, 13, 1, 29, 4][piece % 6]
            } else {
                chunk
            };
            let end = at + size.min(last - at);
            client.recv(&wire[at..end]).unwrap();
            at = end;
            piece += 1;
        }
        assert!(
            client.pending_output().is_empty(),
            "frame {frame} released credit before completion",
        );
        client.recv(&wire[last..]).unwrap();

        let queued = client.take_output();
        let released = credit(&queued);
        assert_eq!(
            released,
            (WINDOW, 1, WINDOW, 1),
            "unary={unary} chunk={chunk} frame={frame}: boundary credit",
        );
        assert!(client.failure().is_none(), "frame {frame}: output limit");
        assert_eq!(client.retained_response_bytes(), 0, "frame {frame}");
        assert!(client.is_pending(a), "frame {frame} ended the call early");
        total.0 += released.0;
        total.1 += released.2;

        // Still blocked at the boundary, even though the client has queued credit.
        if frame < 5 {
            assert_eq!(peer.queued_send_bytes(a), Some(WINDOW));
            assert!(data_frames(peer.pending_output()).is_empty());
        }
        peer.recv(&queued).unwrap();
        assert!(peer.failure().is_none());
        if frame < 5 {
            assert_eq!(peer.queued_send_bytes(a), Some(0));
            wire = peer.take_output();
            assert_eq!(data_frames(&wire), [(a, WINDOW)], "after frame {frame}");
        } else {
            assert!(peer.take_output().is_empty());
        }
    }
    assert_eq!(total, (6 * WINDOW, 6 * WINDOW));
    assert!(total.0 > 65_535 && total.1 > WINDOW);

    peer.send_headers(a, vec![field("x-detail", "after six frames")], true)
        .unwrap();
    deliver(&mut client, &mut peer);
    let status = ended_status(&mut client, a, unary);
    assert_eq!(status.code, Code::Unavailable);
    assert_eq!(status.metadata.get("x-detail"), Some("after six frames"));
    complete_unrelated_call(&mut client, &mut peer, b);
}

#[test]
fn interrupted_ignored_body_releases_delivered_credit_once() {
    // Cleanup owns the delivered prefix; the parser owns the unread tail.
    // Neither may credit the other's bytes, regardless of how the call ends.
    for unary in [true, false] {
        for prefix in [1usize, 8_192usize] {
            cancelled_ignored_body(unary, prefix);
            expired_ignored_body(unary, prefix);
            failed_over_ignored_body(unary, prefix);
        }
    }
}

fn small_window_config() -> ClientConfig {
    ClientConfig {
        http2: Config {
            initial_window_size: WINDOW as u32,
            max_pending_output_bytes: 1024,
            ..ClientConfig::default().http2
        },
        ..ClientConfig::default()
    }
}

fn html_error_headers() -> Vec<HeaderField> {
    vec![field(":status", "503"), field("content-type", "text/html")]
}

/// Deliver initial headers and clear both output buffers before measuring DATA credit.
fn ignored_headers(
    client: &mut Client,
    peer: &mut Connection,
    id: CallId,
    initial: Vec<HeaderField>,
) {
    peer.send_headers(id, initial, false).unwrap();
    deliver(client, peer);
    client.take_output();
    peer.take_output();
}

fn complete_unrelated_call(client: &mut Client, peer: &mut Connection, id: CallId) {
    headers(peer, id);
    peer.send_data(id, encoded(b"ok"), false).unwrap();
    finish(peer, id);
    exchange(client, peer);
    assert_eq!(client.take_response(id).unwrap().unwrap().message, b"ok");
}

/// Deliver only a nonterminal frame's prefix; return the wire bytes for its tail.
fn ignored_prefix(
    unary: bool,
    timeout: bool,
    prefix: usize,
) -> (Client, Connection, CallId, CallId, Vec<u8>) {
    let (mut client, mut peer) = setup(small_window_config());
    let options = if timeout {
        CallOptions::timeout(Duration::from_secs(1))
    } else {
        CallOptions::new()
    };
    let a = open_with(&mut client, &mut peer, unary, &options);
    let b = open(&mut client, &mut peer, true);
    ignored_headers(&mut client, &mut peer, a, html_error_headers());

    peer.send_data(a, vec![b'y'; WINDOW], false).unwrap();
    let frame = peer.take_output();
    assert_eq!(data_frames(&frame), [(a, WINDOW)]);
    client.recv(&frame[..9]).unwrap();
    for piece in frame[9..9 + prefix].chunks(1) {
        client.recv(piece).unwrap();
    }
    assert!(
        client.pending_output().is_empty(),
        "unary={unary} prefix={prefix}"
    );
    assert!(client.is_pending(a));
    assert_eq!(client.retained_response_bytes(), 0);
    (client, peer, a, b, frame)
}

/// RST_STREAM frames for `id` in `bytes`.
fn resets_for(bytes: &[u8], id: CallId) -> usize {
    decoded_frames(bytes)
        .iter()
        .filter(|frame| frame.0 == RST_STREAM_FRAME && frame.1 == id)
        .count()
}

fn cancelled_ignored_body(unary: bool, prefix: usize) {
    let (mut client, mut peer, a, b, frame) = ignored_prefix(unary, false, prefix);

    client.cancel(a);
    client.cancel(a);
    let output = client.take_output();
    assert_eq!(credit(&output), (prefix, 1, prefix, 1), "unary={unary}");
    assert_eq!(resets_for(&output, a), 1);

    // After cancellation, the parser owes the tail at connection level only.
    for piece in frame[9 + prefix..].chunks(1) {
        client.recv(piece).unwrap();
    }
    assert_eq!(credit(&client.take_output()), (WINDOW - prefix, 1, 0, 0));
    assert!(client.failure().is_none());
    assert!(!client.is_pending(a));
    assert!(client.take_response(a).is_none());
    assert!(client.try_next(a).is_none());
    client.cancel(a);
    assert_eq!(credit(&client.take_output()), (0, 0, 0, 0));
    assert_eq!(client.retained_response_bytes(), 0);
    complete_unrelated_call(&mut client, &mut peer, b);
}

fn expired_ignored_body(unary: bool, prefix: usize) {
    let (mut client, mut peer, a, b, frame) = ignored_prefix(unary, true, prefix);

    assert_eq!(client.next_deadline(), Some(Duration::from_secs(1)));
    client.tick(Duration::from_secs(1));
    let output = client.take_output();
    // Deadline termination resets before discarding: only connection credit remains.
    assert_eq!(credit(&output), (prefix, 1, 0, 0), "unary={unary}");
    assert_eq!(resets_for(&output, a), 1);
    assert!(client.next_deadline().is_none());

    let status = ended_status(&mut client, a, unary);
    assert_eq!(status.code, Code::DeadlineExceeded);
    assert_eq!(status.message, "deadline exceeded");
    client.cancel(a);
    assert_eq!(credit(&client.take_output()), (0, 0, 0, 0));

    for piece in frame[9 + prefix..].chunks(1) {
        client.recv(piece).unwrap();
    }
    assert_eq!(credit(&client.take_output()), (WINDOW - prefix, 1, 0, 0));
    assert!(client.failure().is_none());
    assert_eq!(client.retained_response_bytes(), 0);
    complete_unrelated_call(&mut client, &mut peer, b);
}

fn failed_over_ignored_body(unary: bool, prefix: usize) {
    let (mut client, _peer, a, b, _frame) = ignored_prefix(unary, false, prefix);

    let known = Status::new(Code::Unavailable, "transport retired");
    client.fail_all(known.clone());
    let output = client.take_output();
    assert_eq!(credit(&output), (prefix, 1, prefix, 1), "unary={unary}");
    assert_eq!(resets_for(&output, a), 0);
    client.fail_all(known);
    assert_eq!(credit(&client.take_output()), (0, 0, 0, 0));

    // Both calls learn the failure. Do not resume the retired transport's unread tail.
    for (id, unary) in [(a, unary), (b, true)] {
        let status = ended_status(&mut client, id, unary);
        assert_eq!(status.code, Code::Unavailable);
        assert_eq!(status.message, "transport retired");
    }
}

/// Consume one error outcome, release streaming metadata, and check cleanup.
#[track_caller]
fn ended_status(client: &mut Client, id: CallId, unary: bool) -> Status {
    let status = if unary {
        client.take_response(id).unwrap().unwrap_err()
    } else {
        match client.try_next(id) {
            Some(Next::Done(Err(status))) => status,
            next => panic!("call {id}: expected an error outcome, got {next:?}"),
        }
    };
    assert!(
        client.take_response(id).is_none(),
        "call {id}: duplicate result"
    );
    assert!(
        client.try_next(id).is_none(),
        "call {id}: duplicate outcome"
    );
    if !unary {
        client.take_metadata(id);
    }
    assert_eq!(client.retained_response_bytes(), 0);
    assert!(client.failure().is_none());
    status
}

#[test]
fn fragmented_ignored_body_preserves_terminal_status_and_metadata() {
    // Explicit error trailers override the fallback; missing status keeps it.
    // Success trailers cannot rehabilitate an invalid content type.
    for unary in [true, false] {
        for chunk in [1, 40] {
            for (initial, code, message) in [
                (
                    html_error_headers(),
                    Code::Unavailable,
                    "unexpected HTTP status 503",
                ),
                (
                    vec![
                        field(":status", "200"),
                        field("content-type", "application/grpc-web"),
                    ],
                    Code::Internal,
                    "invalid response content-type",
                ),
                (
                    vec![field(":status", "200")],
                    Code::Internal,
                    "invalid response content-type",
                ),
            ] {
                ignored_trailers_decide_status(unary, chunk, initial.clone(), code, message);
                ignored_terminal_data_decides_status(unary, chunk, initial.clone(), code, message);
                ignored_padded_frame_credits_padding_separately(
                    unary, chunk, initial, code, message,
                );
            }
        }
    }
}

/// Check an unpadded frame immediately before and after its final byte.
fn ignored_body_opened(
    unary: bool,
    initial: Vec<HeaderField>,
    body: usize,
    end_stream: bool,
    chunk: usize,
) -> (Client, Connection, CallId) {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let a = open(&mut client, &mut peer, unary);
    ignored_headers(&mut client, &mut peer, a, initial);

    peer.send_data(a, vec![b'x'; body], end_stream).unwrap();
    let wire = peer.take_output();
    assert_eq!(data_frames(&wire), [(a, body)]);

    let last = wire.len() - 1;
    for piece in wire[..last].chunks(chunk) {
        client.recv(piece).unwrap();
    }
    assert!(
        client.is_pending(a),
        "unary={unary} end_stream={end_stream}"
    );
    assert!(client.take_response(a).is_none());
    assert!(client.try_next(a).is_none());
    assert_eq!(client.retained_response_bytes(), 0);
    assert_eq!(credit(&client.take_output()), (0, 0, 0, 0));
    client.recv(&wire[last..]).unwrap();

    // END_STREAM closes the receive half, so only the connection is owed credit.
    let frames = usize::from(body != 0);
    let expected = if end_stream {
        (body, frames, 0, 0)
    } else {
        (body, frames, body, frames)
    };
    assert_eq!(
        credit(&client.take_output()),
        expected,
        "body={body} end_stream={end_stream} chunk={chunk} unary={unary}",
    );
    (client, peer, a)
}

fn ignored_trailers_decide_status(
    unary: bool,
    chunk: usize,
    initial: Vec<HeaderField>,
    code: Code,
    message: &str,
) {
    let shapes = [
        (
            "error trailers override the fallback",
            vec![
                field("grpc-status", "7"),
                field("grpc-message", "denied%20here"),
                field("x-detail", "kept"),
            ],
            Code::PermissionDenied,
            "denied here",
        ),
        (
            "statusless trailers keep the fallback",
            vec![field("x-detail", "kept")],
            code,
            message,
        ),
        (
            "success trailers stay rejected",
            vec![field("grpc-status", "0"), field("x-detail", "kept")],
            Code::Internal,
            "invalid response content-type",
        ),
    ];
    for (shape, trailers, expected_code, expected_message) in shapes {
        let (mut client, mut peer, a) =
            ignored_body_opened(unary, initial.clone(), 64, false, chunk);
        peer.send_headers(a, trailers, true).unwrap();
        deliver(&mut client, &mut peer);
        assert_eq!(credit(&client.take_output()), (0, 0, 0, 0), "{shape}");
        let status = ended_status(&mut client, a, unary);
        assert_eq!(
            (status.code, status.message.as_str()),
            (expected_code, expected_message),
            "{shape} unary={unary} chunk={chunk}",
        );
        assert_eq!(status.metadata.get("x-detail"), Some("kept"));
    }
}

/// Terminal body, empty DATA, and padding-only DATA all keep the fallback.
fn ignored_terminal_data_decides_status(
    unary: bool,
    chunk: usize,
    initial: Vec<HeaderField>,
    code: Code,
    message: &str,
) {
    for body in [64, 0] {
        let (mut client, _peer, a) = ignored_body_opened(unary, initial.clone(), body, true, chunk);
        let status = ended_status(&mut client, a, unary);
        assert_eq!(
            (status.code, status.message.as_str()),
            (code, message),
            "terminal DATA body={body} unary={unary} chunk={chunk}",
        );
        assert!(status.metadata.is_empty());
    }

    for pad in [2, 255] {
        let (mut client, _peer, a) = ignored_raw_body(unary, initial.clone(), 0, pad, true, chunk);
        assert_eq!(credit(&client.take_output()), (1 + pad, 1, 0, 0));
        let status = ended_status(&mut client, a, unary);
        assert_eq!(
            (status.code, status.message.as_str()),
            (code, message),
            "padding-only pad={pad} unary={unary} chunk={chunk}",
        );
        assert!(status.metadata.is_empty());
    }
}

/// Padding is credited at admission, body at completion: two updates per window.
fn ignored_padded_frame_credits_padding_separately(
    unary: bool,
    chunk: usize,
    initial: Vec<HeaderField>,
    code: Code,
    message: &str,
) {
    for pad in [7, 255] {
        let (mut client, mut peer, a) =
            ignored_raw_body(unary, initial.clone(), 64, pad, false, chunk);
        assert_eq!(
            credit(&client.take_output()),
            (1 + pad + 64, 2, 1 + pad + 64, 2),
            "padded pad={pad} unary={unary} chunk={chunk}",
        );
        peer.send_headers(a, vec![field("x-detail", "kept")], true)
            .unwrap();
        deliver(&mut client, &mut peer);
        assert_eq!(credit(&client.take_output()), (0, 0, 0, 0));
        let status = ended_status(&mut client, a, unary);
        assert_eq!((status.code, status.message.as_str()), (code, message));
        assert_eq!(status.metadata.get("x-detail"), Some("kept"));
    }
}

/// The peer serializer never pads, so inject a padded frame directly.
fn ignored_raw_body(
    unary: bool,
    initial: Vec<HeaderField>,
    body: usize,
    pad: usize,
    end_stream: bool,
    chunk: usize,
) -> (Client, Connection, CallId) {
    let (mut client, mut peer) = setup(ClientConfig::default());
    let a = open(&mut client, &mut peer, unary);
    ignored_headers(&mut client, &mut peer, a, initial);

    let frame = raw_data(a, body, pad, end_stream);
    for piece in frame.chunks(chunk) {
        client.recv(piece).unwrap();
    }
    assert_eq!(client.retained_response_bytes(), 0);
    (client, peer, a)
}

/// Padded DATA: header, pad length, body, then padding.
fn raw_data(id: CallId, body: usize, pad: usize, end_stream: bool) -> Vec<u8> {
    assert!(pad <= 255, "the pad-length byte holds at most 255");
    let len = 1 + pad + body;
    let mut frame = vec![
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        DATA_FRAME,
        if end_stream { 0x09 } else { 0x08 },
    ];
    frame.extend_from_slice(&id.to_be_bytes());
    frame.push(pad as u8);
    frame.extend(std::iter::repeat_n(b'x', body));
    frame.extend(std::iter::repeat_n(0x20, pad));
    frame
}
