//! Metadata tests for the sans-IO client and server cores.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use super::*;
use protolink_http2::{Connection, Event, HeaderField};

/// Paths:
/// - `/m.M/Echo`: unary; replies with the request's `x-req` values joined by
///   commas, echoes `x-bin` in the response headers, sets `x-init` (headers)
///   and `x-trail` (trailers).
/// - `/m.M/Fail`: unary; sets `x-init` and `x-trail`, fails `ABORTED` with
///   `x-status` in the status.
/// - `/m.M/UserAgent`: unary; replies with the request's `user-agent` values.
/// - `/m.M/Count`: server-streaming; request `[n]` yields `[1]..[n]`. Sets
///   `x-start` in the headers and `x-done` in the trailers.
/// - `/m.M/FailStream`: server-streaming; sets `x-start`, then fails
///   `NOT_FOUND` with `x-err` in the status.
/// - `/m.M/Quiet`: server-streaming; sets `x-quiet` in the trailers and never
///   answers.
#[derive(Debug, Default)]
struct Meta {
    counts: BTreeMap<CallId, (u8, u8)>,
    /// Whether the headers could still be changed, at every `Count` poll.
    open_at_poll: Vec<bool>,
}

impl Handler for Meta {
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        _request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>> {
        match ctx.path {
            "/m.M/Echo" => {
                let reply = ctx
                    .metadata()
                    .get_all("x-req")
                    .collect::<Vec<_>>()
                    .join(",")
                    .into_bytes();
                let bin = ctx.metadata().get_bin("x-bin").map(<[u8]>::to_vec);
                let initial = ctx.initial_metadata_mut().unwrap();
                initial.insert("x-init", "i").unwrap();
                if let Some(bin) = bin {
                    initial.insert_bin("x-bin", &bin).unwrap();
                }
                ctx.trailing_metadata_mut().insert("x-trail", "t").unwrap();
                Some(Ok(reply))
            }
            "/m.M/Fail" => {
                ctx.initial_metadata_mut()
                    .unwrap()
                    .insert("x-init", "i")
                    .unwrap();
                ctx.trailing_metadata_mut().insert("x-trail", "t").unwrap();
                let mut md = Metadata::new();
                md.insert("x-status", "s").unwrap();
                Some(Err(Status::aborted("no").with_metadata(md)))
            }
            "/m.M/BinaryEcho" => {
                let values: Vec<Vec<u8>> = ctx
                    .metadata()
                    .get_all_bin("trace-bin")
                    .map(<[u8]>::to_vec)
                    .collect();
                for value in values {
                    ctx.initial_metadata_mut()
                        .unwrap()
                        .insert_bin("trace-bin", &value)
                        .unwrap();
                    ctx.trailing_metadata_mut()
                        .insert_bin("trace-bin", &value)
                        .unwrap();
                }
                Some(Ok(Vec::new()))
            }
            "/m.M/UserAgent" => Some(Ok(ctx
                .metadata()
                .get_all("user-agent")
                .collect::<Vec<_>>()
                .join(",")
                .into_bytes())),
            _ => None,
        }
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        matches!(path, "/m.M/Count" | "/m.M/FailStream" | "/m.M/Quiet")
            .then_some(MethodKind::ServerStreaming)
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        match ctx.path {
            "/m.M/Count" => {
                ctx.initial_metadata_mut()
                    .unwrap()
                    .insert("x-start", "s")
                    .unwrap();
                self.counts.insert(ctx.id, (message[0], 0));
            }
            "/m.M/FailStream" => {
                ctx.initial_metadata_mut()
                    .unwrap()
                    .insert("x-start", "s")
                    .unwrap();
            }
            _ => {
                ctx.trailing_metadata_mut().insert("x-quiet", "q").unwrap();
            }
        }
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        match ctx.path {
            "/m.M/Count" => {
                self.open_at_poll.push(ctx.initial_metadata_mut().is_some());
                let (target, produced) = self.counts.get_mut(&ctx.id).unwrap();
                if *produced < *target {
                    *produced += 1;
                    return Poll::Ready(Next::Message(vec![*produced]));
                }
                ctx.trailing_metadata_mut().insert("x-done", "d").unwrap();
                Poll::Ready(Next::Done(Ok(())))
            }
            "/m.M/FailStream" => {
                let mut md = Metadata::new();
                md.insert("x-err", "e").unwrap();
                Poll::Ready(Next::Done(Err(Status::not_found("nf").with_metadata(md))))
            }
            _ => Poll::Pending,
        }
    }
}

fn pump(client: &mut Client, server: &mut Server, handler: &mut Meta) {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..16 {
        server.poll(handler, &mut cx);
        let out = client.take_output();
        if !out.is_empty() {
            server.recv(&out, handler).unwrap();
        }
        server.poll(handler, &mut cx);
        let out = server.take_output();
        if !out.is_empty() {
            client.recv(&out).unwrap();
        }
    }
}

fn pair() -> (Client, Server, Meta) {
    (
        Client::new(ClientConfig::default()),
        Server::new(ServerConfig::default()),
        Meta::default(),
    )
}

fn drain(client: &mut Client, id: CallId) -> (Vec<Vec<u8>>, Result<(), Status>) {
    let mut messages = Vec::new();
    loop {
        match client.try_next(id) {
            Some(Next::Message(m)) => messages.push(m),
            Some(Next::Done(result)) => return (messages, result),
            None => panic!("call has not finished"),
        }
    }
}

fn md(entries: &[(&str, &str)]) -> Metadata {
    let mut md = Metadata::new();
    for (k, v) in entries {
        md.insert(k, v).unwrap();
    }
    md
}

#[test]
fn unary_metadata_travels_both_ways() {
    let (mut client, mut server, mut handler) = pair();
    let mut request = md(&[("x-req", "a"), ("x-req", "b")]);
    request.insert_bin("x-bin", &[0, 255, 7]).unwrap();
    let id = client
        .start_unary_with("/m.M/Echo", b"", &CallOptions::metadata(request))
        .unwrap();
    pump(&mut client, &mut server, &mut handler);

    let response = client.take_response(id).unwrap().unwrap();
    assert_eq!(response.message, b"a,b");
    assert_eq!(response.headers.get("x-init"), Some("i"));
    assert_eq!(response.headers.get_bin("x-bin"), Some(&[0, 255, 7][..]));
    assert_eq!(
        response.headers.len(),
        2,
        "protocol headers are not metadata"
    );
    assert_eq!(response.trailers.get("x-trail"), Some("t"));
    assert_eq!(response.trailers.len(), 1);
}

#[test]
fn calls_without_metadata_have_none() {
    let (mut client, mut server, mut handler) = pair();
    let id = client.start_unary("/m.M/UserAgent", b"").unwrap();
    pump(&mut client, &mut server, &mut handler);
    let response = client.take_response(id).unwrap().unwrap();
    assert!(response.headers.is_empty());
    assert!(response.trailers.is_empty());
}

#[test]
fn user_agent_defaults_and_can_be_overridden() {
    let (mut client, mut server, mut handler) = pair();
    let default = client.start_unary("/m.M/UserAgent", b"").unwrap();
    let custom = client
        .start_unary_with(
            "/m.M/UserAgent",
            b"",
            &CallOptions::metadata(md(&[("user-agent", "mine/1")])),
        )
        .unwrap();
    pump(&mut client, &mut server, &mut handler);
    assert_eq!(
        client.take_response(default).unwrap().unwrap().message,
        b"protolink"
    );
    assert_eq!(
        client.take_response(custom).unwrap().unwrap().message,
        b"mine/1"
    );
}

#[test]
fn failed_unary_call_carries_all_metadata_in_the_status() {
    let (mut client, mut server, mut handler) = pair();
    let id = client.start_unary("/m.M/Fail", b"").unwrap();
    pump(&mut client, &mut server, &mut handler);
    let status = client.take_response(id).unwrap().unwrap_err();
    assert_eq!(status.code, Code::Aborted);
    assert_eq!(status.message, "no");
    // A trailers-only response has a single header block: everything is
    // trailing metadata.
    assert_eq!(status.metadata.get("x-init"), Some("i"));
    assert_eq!(status.metadata.get("x-trail"), Some("t"));
    assert_eq!(status.metadata.get("x-status"), Some("s"));
}

#[test]
fn streaming_headers_are_available_before_the_call_ends() {
    let (mut client, mut server, mut handler) = pair();
    let id = client
        .start_streaming_with("/m.M/Count", &CallOptions::default())
        .unwrap();
    assert!(client.response_headers(id).is_none());
    client.send_message(id, &[3]).unwrap();
    client.close_send(id).unwrap();
    pump(&mut client, &mut server, &mut handler);

    assert_eq!(
        client.response_headers(id).unwrap().get("x-start"),
        Some("s")
    );
    let (messages, result) = drain(&mut client, id);
    assert_eq!(messages, [[1], [2], [3]]);
    assert_eq!(result, Ok(()));

    let (headers, trailers) = client.take_metadata(id);
    assert_eq!(headers.unwrap().get("x-start"), Some("s"));
    assert_eq!(trailers.get("x-done"), Some("d"));
    assert!(!trailers.contains_key("x-start"));
    // Taking removes it.
    assert_eq!(client.take_metadata(id), (None, Metadata::new()));
}

#[test]
fn initial_metadata_is_locked_once_the_headers_are_sent() {
    let (mut client, mut server, mut handler) = pair();
    let id = client.start_streaming("/m.M/Count").unwrap();
    client.send_message(id, &[3]).unwrap();
    client.close_send(id).unwrap();
    pump(&mut client, &mut server, &mut handler);
    let _ = drain(&mut client, id);
    // Polled for `[1]`, `[2]`, `[3]` and the end; the headers went out with `[1]`.
    assert_eq!(handler.open_at_poll, [true, false, false, false]);
}

#[test]
fn streaming_call_that_fails_at_once_is_trailers_only() {
    let (mut client, mut server, mut handler) = pair();
    let id = client.start_streaming("/m.M/FailStream").unwrap();
    client.send_message(id, b"x").unwrap();
    client.close_send(id).unwrap();
    pump(&mut client, &mut server, &mut handler);

    let (messages, result) = drain(&mut client, id);
    assert!(messages.is_empty());
    let status = result.unwrap_err();
    assert_eq!(status.code, Code::NotFound);
    assert_eq!(status.metadata.get("x-start"), Some("s"));
    assert_eq!(status.metadata.get("x-err"), Some("e"));

    let (headers, trailers) = client.take_metadata(id);
    assert_eq!(headers, None);
    assert_eq!(trailers.get("x-start"), Some("s"));
}

#[test]
fn cancel_discards_the_metadata_of_a_finished_call() {
    let (mut client, mut server, mut handler) = pair();
    let id = client.start_streaming("/m.M/Count").unwrap();
    client.send_message(id, &[1]).unwrap();
    client.close_send(id).unwrap();
    pump(&mut client, &mut server, &mut handler);
    let _ = drain(&mut client, id);
    client.cancel(id);
    assert_eq!(client.take_metadata(id), (None, Metadata::new()));
}

#[test]
fn trailing_metadata_is_sent_when_the_deadline_ends_the_call() {
    let (mut client, mut server, mut handler) = pair();
    let id = client
        .start_streaming_with("/m.M/Quiet", &CallOptions::timeout(Duration::from_secs(1)))
        .unwrap();
    client.send_message(id, b"x").unwrap();
    client.close_send(id).unwrap();
    pump(&mut client, &mut server, &mut handler);
    server.tick(Duration::from_secs(2), &mut handler);
    pump(&mut client, &mut server, &mut handler);

    let (_, result) = drain(&mut client, id);
    let status = result.unwrap_err();
    assert_eq!(status.code, Code::DeadlineExceeded);
    assert_eq!(status.metadata.get("x-quiet"), Some("q"));
}

/// A raw request on a fresh connection; returns what the server answered.
fn raw_request(extra: &[(&str, &str)]) -> Vec<Event> {
    raw_request_at("/m.M/UserAgent", extra)
}

fn raw_request_at(path: &str, extra: &[(&str, &str)]) -> Vec<Event> {
    let hf = |n: &str, v: &str| HeaderField {
        name: n.into(),
        value: v.into(),
    };
    let mut headers = vec![
        hf(":method", "POST"),
        hf(":scheme", "http"),
        hf(":path", path),
        hf("content-type", "application/grpc"),
    ];
    headers.extend(extra.iter().map(|(n, v)| hf(n, v)));
    let mut conn = Connection::client(Default::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = Meta::default();
    let id = conn.open_stream(headers, false).unwrap();
    conn.send_data(id, lpm::encode(b"").unwrap(), true).unwrap();
    for _ in 0..8 {
        server.recv(&conn.take_output(), &mut handler).unwrap();
        conn.recv(&server.take_output()).unwrap();
    }
    core::iter::from_fn(|| conn.poll_event()).collect()
}

fn grpc_status(events: &[Event]) -> Option<String> {
    events.iter().find_map(|e| match e {
        Event::Headers { headers, .. } => headers
            .iter()
            .find(|h| h.name == "grpc-status")
            .map(|h| h.value.clone()),
        _ => None,
    })
}

#[test]
fn malformed_binary_request_metadata_is_invalid_argument() {
    let events = raw_request(&[("x-bad-bin", "T!")]);
    assert_eq!(grpc_status(&events).as_deref(), Some("3"), "{events:?}");
}

#[test]
fn request_metadata_with_control_characters_is_invalid_argument() {
    // Internal HTAB is legal in HTTP fields, but not in gRPC ASCII metadata.
    // Other controls are now rejected by HTTP/2 before reaching the gRPC parser.
    let events = raw_request(&[("x-bad", "a\tb")]);
    assert_eq!(grpc_status(&events).as_deref(), Some("3"), "{events:?}");
}

#[test]
fn well_formed_raw_metadata_is_accepted() {
    // Padded base64 is accepted too.
    let events = raw_request(&[("x-ok-bin", "TWE="), ("x-ok", "v")]);
    assert_eq!(grpc_status(&events).as_deref(), Some("0"), "{events:?}");
}

#[test]
fn combined_and_repeated_binary_request_headers_reach_the_handler_in_order() {
    let events = raw_request_at(
        "/m.M/BinaryEcho",
        &[("trace-bin", "AQ== ,\tAg\t,"), ("trace-bin", "Aw==,, BA")],
    );
    assert_eq!(grpc_status(&events).as_deref(), Some("0"), "{events:?}");
    let metadata: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Headers { headers, .. } => Some(Metadata::from_headers(headers).unwrap()),
            _ => None,
        })
        .collect();
    assert_eq!(metadata.len(), 2, "{events:?}");
    for fields in metadata {
        assert_eq!(
            fields.get_all_bin("trace-bin").collect::<Vec<_>>(),
            [&[1][..], &[2][..], &[], &[3][..], &[], &[4][..]]
        );
    }
}

#[test]
fn malformed_combined_request_element_rejects_the_whole_call() {
    for value in ["!,AQ==,Ag", "AQ==,!,Ag", "AQ==,Ag,!"] {
        let events = raw_request_at("/m.M/BinaryEcho", &[("trace-bin", value)]);
        assert_eq!(grpc_status(&events).as_deref(), Some("3"), "{events:?}");
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Event::Data { .. }))
        );
    }
}

fn raw_response(
    initial: &[(&str, &str)],
    trailing: &[(&str, &str)],
    status: &str,
) -> Result<Response<Vec<u8>>, Status> {
    let hf = |name: &str, value: &str| HeaderField {
        name: name.into(),
        value: value.into(),
    };
    let mut client = Client::new(ClientConfig::default());
    let mut peer = Connection::server(Default::default());
    let id = client.start_unary("/m.M/Echo", b"").unwrap();
    peer.recv(&client.take_output()).unwrap();
    let mut headers = vec![hf(":status", "200"), hf("content-type", "application/grpc")];
    headers.extend(initial.iter().map(|(name, value)| hf(name, value)));
    peer.send_headers(id, headers, false).unwrap();
    peer.send_data(id, lpm::encode(b"").unwrap(), false)
        .unwrap();
    let mut trailers = vec![hf("grpc-status", status)];
    trailers.extend(trailing.iter().map(|(name, value)| hf(name, value)));
    peer.send_headers(id, trailers, true).unwrap();
    client.recv(&peer.take_output()).unwrap();
    client.take_response(id).unwrap()
}

#[test]
fn combined_binary_response_headers_and_trailers_preserve_valid_siblings() {
    for first in ["AQ== ,\tAg\t,", "AQ==,!,Ag,"] {
        let fields = [("trace-bin", first), ("trace-bin", "Aw==,, BA")];
        let response = raw_response(&fields, &fields, "0").unwrap();
        for metadata in [response.headers, response.trailers] {
            assert_eq!(
                metadata.get_all_bin("trace-bin").collect::<Vec<_>>(),
                [&[1][..], &[2][..], &[], &[3][..], &[], &[4][..]]
            );
        }
    }
}

#[test]
fn combined_binary_error_trailers_preserve_valid_siblings() {
    let fields = [("trace-bin", "AQ==,!,Ag"), ("trace-bin", "Aw==,, BA")];
    let status = raw_response(&[], &fields, "7").unwrap_err();
    assert_eq!(status.code, Code::PermissionDenied);
    assert_eq!(
        status.metadata.get_all_bin("trace-bin").collect::<Vec<_>>(),
        [&[1][..], &[2][..], &[3][..], &[], &[4][..]]
    );
}

#[test]
fn handlers_can_be_tested_with_a_hand_built_context() {
    let request = md(&[("k", "v")]);
    let mut response = ResponseMetadata::default();
    {
        let mut ctx = CallContext::new("/m.M/Echo", 1, None, &request, &mut response);
        assert_eq!(ctx.metadata().get("k"), Some("v"));
        ctx.initial_metadata_mut()
            .unwrap()
            .insert("a", "1")
            .unwrap();
        ctx.trailing_metadata_mut().insert("b", "2").unwrap();
    }
    assert_eq!(response.initial.as_ref().unwrap().get("a"), Some("1"));
    assert_eq!(response.trailing.get("b"), Some("2"));

    response.initial = None;
    let mut ctx = CallContext::new("/m.M/Echo", 1, None, &request, &mut response);
    assert!(ctx.initial_metadata_mut().is_none());
}

#[test]
fn tuple_and_reference_handlers_pass_the_context_through() {
    let mut handler = (Meta::default(), Meta::default());
    let request = md(&[("x-req", "q")]);
    let mut response = ResponseMetadata::default();
    let mut ctx = CallContext::new("/m.M/Echo", 1, None, &request, &mut response);
    let mut by_ref = &mut handler;
    let reply = <&mut (Meta, Meta) as Handler>::call(&mut by_ref, &mut ctx, b"");
    assert_eq!(reply, Some(Ok(b"q".to_vec())));
    assert_eq!(
        response.initial.unwrap().get("x-init"),
        Some("i"),
        "the first handler wrote through the shared context"
    );
}
