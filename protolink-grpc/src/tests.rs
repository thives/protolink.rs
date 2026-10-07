extern crate std;

use super::*;
use crate::test_support::{hf, pump, send_raw};
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use protolink_http2::{Event, HeaderField};

fn echo(path: &str, req: &[u8]) -> Option<Result<Vec<u8>, Status>> {
    match path {
        "/test.Echo/Echo" => Some(Ok(req.to_vec())),
        "/test.Echo/Fail" => Some(Err(Status::failed_precondition("nope: æ 100%"))),
        "/test.Echo/Big" => Some(Ok(vec![1; 10_000])),
        _ => None,
    }
}

fn call(path: &str, req: &[u8]) -> Result<Vec<u8>, Status> {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = FnHandler(echo);
    let id = client.start_unary(path, req).unwrap();
    pump(&mut client, &mut server, &mut handler);
    client
        .take_response(id)
        .expect("call finished")
        .map(Response::into_message)
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
    fn call(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
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
    pump(&mut client, &mut server, &mut handler);
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
    pump(&mut client, &mut server, &mut handler);
    let body =
        |r: Option<Result<Response<Vec<u8>>, Status>>| r.map(|r| r.map(Response::into_message));
    assert_eq!(body(client.take_response(a)), Some(Ok(b"a".to_vec())));
    assert_eq!(body(client.take_response(b)), Some(Ok(b"b".to_vec())));
    let c = client.start_unary("/test.Echo/Echo", b"c").unwrap();
    pump(&mut client, &mut server, &mut handler);
    assert_eq!(body(client.take_response(c)), Some(Ok(b"c".to_vec())));
}

#[test]
fn tuple_handlers_route_in_order() {
    let mut handler = (
        FnHandler(|p: &str, _: &[u8]| (p == "/a.A/X").then(|| Ok(b"a".to_vec()))),
        FnHandler(|p: &str, _: &[u8]| (p == "/b.B/X").then(|| Ok(b"b".to_vec()))),
    );
    let md = Metadata::new();
    let mut response = ResponseMetadata::default();
    let mut ctx = CallContext::new("/b.B/X", 1, None, &md, &mut response);
    assert_eq!(handler.call(&mut ctx, b""), Some(Ok(b"b".to_vec())));
    ctx.path = "/c.C/X";
    assert_eq!(handler.call(&mut ctx, b""), None);
}

/// Drive the server with a raw HTTP/2 client to check non-gRPC requests.
fn raw_request(headers: Vec<HeaderField>, body: &[u8]) -> Vec<Event> {
    let mut server = Server::new(ServerConfig::default());
    send_raw(&mut server, &mut FnHandler(echo), headers, body)
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
fn grpc_message_percent_encoding_round_trips() {
    let s = "a%b\n\u{e6}";
    assert_eq!(status::decode_message(&status::encode_message(s)), s);
    for (input, encoded) in [
        ("", ""),
        (" denied", "%20denied"),
        ("denied ", "denied%20"),
        ("   ", "%20%20%20"),
        ("access denied", "access%20denied"),
        ("100%", "100%25"),
    ] {
        assert_eq!(status::encode_message(input), encoded);
        assert_eq!(status::decode_message(encoded), input);
    }
    for input in ["a\tb", "line\nbreak\r\n", "caf\u{e9} \u{1f600}", "\t \n"] {
        assert_eq!(
            status::decode_message(&status::encode_message(input)),
            input
        );
    }
}

/// The `:scheme` of the request headers a bare HTTP/2 server receives.
fn request_scheme(config: ClientConfig, streaming: bool) -> String {
    let mut client = Client::new(config);
    let mut server = protolink_http2::Connection::server(protolink_http2::Config::default());
    if streaming {
        client.start_streaming("/test.Echo/Echo").unwrap();
    } else {
        client.start_unary("/test.Echo/Echo", b"x").unwrap();
    }
    server.recv(&client.take_output()).unwrap();
    core::iter::from_fn(|| server.poll_event())
        .find_map(|e| match e {
            Event::Headers { headers, .. } => headers
                .into_iter()
                .find(|h| h.name == ":scheme")
                .map(|h| h.value),
            _ => None,
        })
        .expect("request headers with :scheme")
}

#[test]
fn request_scheme_is_configurable() {
    assert_eq!(ClientConfig::default().scheme, Scheme::Http);
    for streaming in [false, true] {
        assert_eq!(request_scheme(ClientConfig::default(), streaming), "http");
        let https = ClientConfig {
            scheme: Scheme::Https,
            ..ClientConfig::default()
        };
        assert_eq!(request_scheme(https, streaming), "https");
    }
}
