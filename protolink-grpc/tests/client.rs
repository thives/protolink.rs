//! Client-only raw-peer regressions for response lifetime and validation.

use protolink_grpc::compression::{Codec, CodecError};
use protolink_grpc::http2::{Config, Connection, ErrorCode, Event, HeaderField};
use protolink_grpc::{CallId, Client, ClientConfig, Code, Compression, Next, lpm};

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
    let id = if unary {
        client.start_unary("/test.Client/Unary", b"").unwrap()
    } else {
        let id = client.start_streaming("/test.Client/Stream").unwrap();
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
    let mut pos = 0;
    let mut credit = 0;
    while pos < bytes.len() {
        assert!(bytes.len() - pos >= 9);
        let len = (usize::from(bytes[pos]) << 16)
            | (usize::from(bytes[pos + 1]) << 8)
            | usize::from(bytes[pos + 2]);
        let id = u32::from_be_bytes(bytes[pos + 5..pos + 9].try_into().unwrap()) & 0x7fff_ffff;
        if bytes[pos + 3] == 8 && id == 0 {
            assert_eq!(len, 4);
            credit += (u32::from_be_bytes(bytes[pos + 9..pos + 13].try_into().unwrap())
                & 0x7fff_ffff) as usize;
        }
        pos += 9 + len;
    }
    assert_eq!(pos, bytes.len());
    credit
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
