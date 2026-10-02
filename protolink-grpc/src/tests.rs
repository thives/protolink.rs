extern crate std;

use super::*;
use alloc::vec;
use alloc::vec::Vec;
use protolink_http2::{Connection, Event, HeaderField};

fn echo(path: &str, req: &[u8]) -> Option<Result<Vec<u8>, Status>> {
    match path {
        "/test.Echo/Echo" => Some(Ok(req.to_vec())),
        "/test.Echo/Fail" => Some(Err(Status::failed_precondition("nope: æ 100%"))),
        "/test.Echo/Big" => Some(Ok(vec![1; 10_000])),
        _ => None,
    }
}

fn run(client: &mut Client, server: &mut Server, handler: &mut impl Handler) {
    for _ in 0..16 {
        if !client.has_output() && !server.has_output() {
            return;
        }
        let out = client.take_output();
        server.recv(&out, handler).unwrap();
        let out = server.take_output();
        client.recv(&out).unwrap();
    }
}

fn call(path: &str, req: &[u8]) -> Result<Vec<u8>, Status> {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = FnHandler(echo);
    let id = client.start_unary(path, req).unwrap();
    run(&mut client, &mut server, &mut handler);
    client.take_response(id).expect("call finished")
}

#[test]
fn unary_ok() {
    assert_eq!(call("/test.Echo/Echo", b"ping"), Ok(b"ping".to_vec()));
}

#[test]
fn empty_message_round_trips() {
    assert_eq!(call("/test.Echo/Echo", b""), Ok(Vec::new()));
}

#[test]
fn error_status_and_message() {
    let err = call("/test.Echo/Fail", b"").unwrap_err();
    assert_eq!(err, Status::failed_precondition("nope: æ 100%"));
}

#[test]
fn unknown_method_is_unimplemented() {
    let err = call("/test.Echo/Nope", b"").unwrap_err();
    assert_eq!(err.code, Code::Unimplemented);
}

/// Handler that reports exactly one path as known.
struct Knows(&'static str);

impl Handler for Knows {
    fn call(&mut self, _: &str, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        None
    }

    fn is_unknown_method(&self, path: &str) -> bool {
        path != self.0
    }
}

#[test]
fn tuple_handler_is_unknown_only_if_every_member_says_so() {
    let pair = (Knows("/a"), Knows("/b"));
    assert!(!pair.is_unknown_method("/a"));
    assert!(!pair.is_unknown_method("/b"));
    assert!(pair.is_unknown_method("/c"));

    // A closure handler can't enumerate its paths.
    let dynamic = (Knows("/a"), FnHandler(echo));
    assert!(!dynamic.is_unknown_method("/c"));

    // The `&mut H` impl forwards.
    let mut knows = Knows("/a");
    let forwarded = &mut knows;
    assert!(!<&mut Knows as Handler>::is_unknown_method(
        &forwarded, "/a"
    ));
    assert!(<&mut Knows as Handler>::is_unknown_method(&forwarded, "/c"));
}

#[test]
fn oversized_response_is_resource_exhausted() {
    let err = call("/test.Echo/Big", b"").unwrap_err();
    assert_eq!(err.code, Code::ResourceExhausted);
}

#[test]
fn oversized_request_is_rejected() {
    let mut client = Client::new(ClientConfig {
        max_message_size: 100_000,
        ..ClientConfig::default()
    });
    let mut server = Server::new(ServerConfig::default());
    let mut handler = FnHandler(echo);
    let id = client
        .start_unary("/test.Echo/Echo", &vec![0; 50_000])
        .unwrap();
    run(&mut client, &mut server, &mut handler);
    assert_eq!(
        client.take_response(id).unwrap().unwrap_err().code,
        Code::ResourceExhausted
    );
}

#[test]
fn multiple_calls_on_one_connection() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = FnHandler(echo);
    let a = client.start_unary("/test.Echo/Echo", b"a").unwrap();
    let b = client.start_unary("/test.Echo/Echo", b"b").unwrap();
    run(&mut client, &mut server, &mut handler);
    assert_eq!(client.take_response(a), Some(Ok(b"a".to_vec())));
    assert_eq!(client.take_response(b), Some(Ok(b"b".to_vec())));
    let c = client.start_unary("/test.Echo/Echo", b"c").unwrap();
    run(&mut client, &mut server, &mut handler);
    assert_eq!(client.take_response(c), Some(Ok(b"c".to_vec())));
}

#[test]
fn tuple_handlers_route_in_order() {
    let mut handler = (
        FnHandler(|p: &str, _: &[u8]| (p == "/a.A/X").then(|| Ok(b"a".to_vec()))),
        FnHandler(|p: &str, _: &[u8]| (p == "/b.B/X").then(|| Ok(b"b".to_vec()))),
    );
    assert_eq!(handler.call("/b.B/X", b""), Some(Ok(b"b".to_vec())));
    assert_eq!(handler.call("/c.C/X", b""), None);
}

/// Drive the server with a raw HTTP/2 client to check non-gRPC requests.
fn raw_request(headers: Vec<HeaderField>, body: &[u8]) -> Vec<Event> {
    let mut conn = Connection::client(Default::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = FnHandler(echo);
    let id = conn.open_stream(headers, false).unwrap();
    conn.send_data(id, body.to_vec(), true).unwrap();
    for _ in 0..8 {
        server.recv(&conn.take_output(), &mut handler).unwrap();
        conn.recv(&server.take_output()).unwrap();
    }
    core::iter::from_fn(|| conn.poll_event()).collect()
}

fn hf(n: &str, v: &str) -> HeaderField {
    HeaderField {
        name: n.into(),
        value: v.into(),
    }
}

#[test]
fn wrong_content_type_is_415() {
    let ev = raw_request(
        vec![
            hf(":method", "POST"),
            hf(":scheme", "http"),
            hf(":path", "/test.Echo/Echo"),
            hf("content-type", "application/json"),
        ],
        b"{}",
    );
    assert!(
        matches!(&ev[0], Event::Headers { headers, .. } if headers[0].value == "415"),
        "{ev:?}"
    );
}

#[test]
fn compressed_message_is_unimplemented() {
    let ev = raw_request(
        vec![
            hf(":method", "POST"),
            hf(":scheme", "http"),
            hf(":path", "/test.Echo/Echo"),
            hf("content-type", "application/grpc"),
        ],
        &[1, 0, 0, 0, 0],
    );
    assert!(
        matches!(&ev[0], Event::Headers { headers, end_stream: true, .. }
            if headers.iter().any(|h| h.name == "grpc-status" && h.value == "12")),
        "{ev:?}"
    );
}

#[test]
fn grpc_message_percent_encoding_round_trips() {
    let s = "a%b\n\u{e6}";
    assert_eq!(status::decode_message(&status::encode_message(s)), s);
    assert_eq!(status::encode_message("100%"), "100%25");
}
