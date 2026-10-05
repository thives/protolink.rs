//! Deadline tests for the sans-IO client and server cores.

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use super::*;
use protolink_http2::{Connection, ErrorCode, Event, HeaderField};

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn header<'a>(headers: &'a [HeaderField], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name == name)
        .map(|h| h.value.as_str())
}

/// An HTTP/2 server connection that only records what the client sent.
struct Peek(Connection);

impl Peek {
    fn new() -> Self {
        Self(Connection::server(Default::default()))
    }

    fn events(&mut self, client: &mut Client) -> Vec<Event> {
        self.0.recv(&client.take_output()).unwrap();
        core::iter::from_fn(|| self.0.poll_event()).collect()
    }

    fn request_headers(&mut self, client: &mut Client) -> Vec<HeaderField> {
        self.events(client)
            .into_iter()
            .find_map(|e| match e {
                Event::Headers { headers, .. } => Some(headers),
                _ => None,
            })
            .expect("request headers")
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

#[test]
fn timeout_is_sent_right_after_the_pseudo_headers() {
    let mut client = Client::new(ClientConfig::default());
    client
        .start_unary_with(
            "/t.T/M",
            b"x",
            &CallOptions::timeout(Duration::from_secs(1)),
        )
        .unwrap();
    let headers = Peek::new().request_headers(&mut client);
    let names: Vec<&str> = headers.iter().map(|h| h.name.as_str()).collect();
    assert_eq!(
        &names[..5],
        [":method", ":scheme", ":path", ":authority", "grpc-timeout"]
    );
    assert_eq!(header(&headers, "grpc-timeout"), Some("1000000u"));
}

#[test]
fn no_timeout_header_by_default() {
    let mut client = Client::new(ClientConfig::default());
    client.start_unary("/t.T/M", b"x").unwrap();
    client.start_streaming("/t.T/S").unwrap();
    let mut peek = Peek::new();
    let events = peek.events(&mut client);
    let mut seen = 0;
    for e in events {
        if let Event::Headers { headers, .. } = e {
            assert_eq!(header(&headers, "grpc-timeout"), None);
            seen += 1;
        }
    }
    assert_eq!(seen, 2);
    assert_eq!(client.next_deadline(), None);
}

#[test]
fn default_timeout_applies_and_options_override_it() {
    let mut client = Client::new(ClientConfig {
        default_timeout: Some(Duration::from_secs(5)),
        ..ClientConfig::default()
    });
    client.start_unary("/t.T/A", b"").unwrap();
    client
        .start_unary_with("/t.T/B", b"", &CallOptions::timeout(Duration::from_secs(2)))
        .unwrap();
    let mut peek = Peek::new();
    let timeouts: Vec<Option<alloc::string::String>> = peek
        .events(&mut client)
        .into_iter()
        .filter_map(|e| match e {
            Event::Headers { headers, .. } => {
                Some(header(&headers, "grpc-timeout").map(Into::into))
            }
            _ => None,
        })
        .collect();
    assert_eq!(timeouts, [Some("5000000u".into()), Some("2000000u".into())]);
    assert_eq!(client.next_deadline(), Some(Duration::from_secs(2)));
}

#[test]
fn zero_timeout_fails_without_sending() {
    let mut client = Client::new(ClientConfig::default());
    client.take_output();
    let err = client
        .start_unary_with("/t.T/M", b"x", &CallOptions::timeout(Duration::ZERO))
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
    let err = client
        .start_streaming_with("/t.T/S", &CallOptions::timeout(Duration::ZERO))
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
    assert!(!client.has_output());
}

#[test]
fn unary_call_expires_and_cancels_its_stream() {
    let mut client = Client::new(ClientConfig::default());
    let mut peek = Peek::new();
    let id = client
        .start_unary_with("/t.T/M", b"x", &CallOptions::timeout(ms(100)))
        .unwrap();
    peek.events(&mut client);

    client.tick(ms(99));
    assert!(client.is_pending(id));
    assert_eq!(client.take_response(id), None);
    assert_eq!(client.next_deadline(), Some(ms(100)));

    client.tick(ms(100));
    assert!(!client.is_pending(id));
    let err = client.take_response(id).unwrap().unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
    assert_eq!(client.next_deadline(), None);
    assert!(
        peek.events(&mut client).contains(&Event::Reset {
            stream_id: id,
            error_code: ErrorCode::Cancel,
        }),
        "the stream is reset with CANCEL"
    );
}

#[test]
fn deadline_counts_from_the_time_the_call_started() {
    let mut client = Client::new(ClientConfig::default());
    client.tick(Duration::from_secs(10));
    let id = client
        .start_unary_with(
            "/t.T/M",
            b"x",
            &CallOptions::timeout(Duration::from_secs(1)),
        )
        .unwrap();
    assert_eq!(client.next_deadline(), Some(Duration::from_secs(11)));
    client.tick(Duration::from_millis(10_999));
    assert!(client.is_pending(id));
    client.tick(Duration::from_secs(11));
    assert!(!client.is_pending(id));
}

#[test]
fn tick_ignores_time_going_backwards() {
    let mut client = Client::new(ClientConfig::default());
    client.tick(Duration::from_secs(10));
    client.tick(Duration::from_secs(5));
    assert_eq!(client.now(), Duration::from_secs(10));
}

#[test]
fn next_deadline_is_the_earliest_of_the_pending_calls() {
    let mut client = Client::new(ClientConfig::default());
    client
        .start_unary_with("/t.T/A", b"", &CallOptions::timeout(ms(300)))
        .unwrap();
    let b = client
        .start_unary_with("/t.T/B", b"", &CallOptions::timeout(ms(100)))
        .unwrap();
    client.start_unary("/t.T/C", b"").unwrap();
    assert_eq!(client.next_deadline(), Some(ms(100)));
    client.cancel(b);
    assert_eq!(client.next_deadline(), Some(ms(300)));
}

#[test]
fn only_expired_calls_fail() {
    let mut client = Client::new(ClientConfig::default());
    let short = client
        .start_unary_with("/t.T/A", b"", &CallOptions::timeout(ms(100)))
        .unwrap();
    let long = client
        .start_unary_with("/t.T/B", b"", &CallOptions::timeout(ms(500)))
        .unwrap();
    let none = client.start_unary("/t.T/C", b"").unwrap();
    client.tick(ms(200));
    assert_eq!(
        client.take_response(short).unwrap().unwrap_err().code,
        Code::DeadlineExceeded
    );
    assert!(client.is_pending(long));
    assert!(client.is_pending(none));
    client.tick(Duration::from_secs(3600));
    assert!(!client.is_pending(long));
    assert!(
        client.is_pending(none),
        "a call without a timeout never expires"
    );
}

fn echo(path: &str, req: &[u8]) -> Option<Result<Vec<u8>, Status>> {
    (path == "/t.T/Echo").then(|| Ok(req.to_vec()))
}

fn pump(client: &mut Client, server: &mut Server, handler: &mut impl Handler) {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..16 {
        server.recv(&client.take_output(), handler).unwrap();
        server.poll(handler, &mut cx);
        client.recv(&server.take_output()).unwrap();
    }
}

#[test]
fn a_call_that_completes_in_time_is_unaffected() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = FnHandler(echo);
    let id = client
        .start_unary_with(
            "/t.T/Echo",
            b"ping",
            &CallOptions::timeout(Duration::from_secs(1)),
        )
        .unwrap();
    pump(&mut client, &mut server, &mut handler);
    client.tick(ms(500));
    assert_eq!(
        client
            .take_response(id)
            .map(|r| r.map(Response::into_message)),
        Some(Ok(b"ping".to_vec()))
    );
    assert_eq!(client.next_deadline(), None);
    client.tick(Duration::from_secs(3600));
    assert!(client.take_output().is_empty());
}

/// Server-streaming method that sends one message per call and then stays
/// pending.
#[derive(Default)]
struct OneThenWait {
    sent: Vec<CallId>,
}

impl Handler for OneThenWait {
    fn call(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        None
    }

    fn method_kind(&self, _: &str) -> Option<MethodKind> {
        Some(MethodKind::ServerStreaming)
    }

    fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        let call = ctx.id;
        if self.sent.contains(&call) {
            Poll::Pending
        } else {
            self.sent.push(call);
            Poll::Ready(Next::Message(b"hi".to_vec()))
        }
    }
}

#[test]
fn streaming_call_delivers_received_messages_before_the_deadline_status() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = OneThenWait::default();
    let id = client
        .start_streaming_with("/t.T/Watch", &CallOptions::timeout(ms(100)))
        .unwrap();
    client.send_message(id, b"go").unwrap();
    client.close_send(id).unwrap();
    pump(&mut client, &mut server, &mut handler);
    assert_eq!(client.try_next(id), Some(Next::Message(b"hi".to_vec())));
    assert_eq!(client.try_next(id), None, "still waiting for the server");

    // A message that arrived but was not taken yet survives the deadline.
    let id2 = client
        .start_streaming_with("/t.T/Watch", &CallOptions::timeout(ms(100)))
        .unwrap();
    client.send_message(id2, b"go").unwrap();
    client.close_send(id2).unwrap();
    pump(&mut client, &mut server, &mut handler);

    client.tick(ms(100));
    match client.try_next(id2) {
        Some(Next::Message(m)) => assert_eq!(m, b"hi"),
        other => panic!("expected the buffered message, got {other:?}"),
    }
    match client.try_next(id2) {
        Some(Next::Done(Err(status))) => assert_eq!(status.code, Code::DeadlineExceeded),
        other => panic!("expected the deadline status, got {other:?}"),
    }
    match client.try_next(id) {
        Some(Next::Done(Err(status))) => assert_eq!(status.code, Code::DeadlineExceeded),
        other => panic!("expected the deadline status, got {other:?}"),
    }
    assert_eq!(client.next_deadline(), None);
}

#[test]
fn late_response_after_the_deadline_is_ignored() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = FnHandler(echo);
    let id = client
        .start_unary_with("/t.T/Echo", b"late", &CallOptions::timeout(ms(10)))
        .unwrap();
    client.tick(ms(10));
    // The server only sees the request after the client gave up.
    pump(&mut client, &mut server, &mut handler);
    assert_eq!(
        client.take_response(id).unwrap().unwrap_err().code,
        Code::DeadlineExceeded
    );
    assert_eq!(client.take_response(id), None);
    assert_eq!(server.active_calls(), 0);
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

fn hf(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}

/// A raw HTTP/2 client talking to a [`Server`], so that requests the real
/// client would never send can be made.
struct Raw {
    conn: Connection,
    server: Server,
    events: Vec<Event>,
}

impl Raw {
    fn new() -> Self {
        Self {
            conn: Connection::client(Default::default()),
            server: Server::new(ServerConfig::default()),
            events: Vec::new(),
        }
    }

    /// Open a call of `/t.T/Echo`, optionally with a `grpc-timeout`, and
    /// optionally send `body` and half-close.
    fn open(&mut self, timeout: Option<&str>, body: Option<&[u8]>, h: &mut impl Handler) -> u32 {
        let mut headers = vec![
            hf(":method", "POST"),
            hf(":scheme", "http"),
            hf(":path", "/t.T/Echo"),
        ];
        headers.extend(timeout.map(|t| hf("grpc-timeout", t)));
        headers.push(hf("content-type", "application/grpc"));
        let id = self.conn.open_stream(headers, false).unwrap();
        if let Some(body) = body {
            self.conn.send_data(id, lpm::encode(body), true).unwrap();
        }
        self.exchange(h);
        id
    }

    /// Move bytes both ways until nothing is left, remembering the events.
    fn exchange(&mut self, handler: &mut impl Handler) {
        for _ in 0..4 {
            self.server.recv(&self.conn.take_output(), handler).unwrap();
            self.conn.recv(&self.server.take_output()).unwrap();
            self.events
                .extend(core::iter::from_fn(|| self.conn.poll_event()));
        }
    }

    /// The `grpc-status` the server ended the call with, if it did.
    fn status(&self, id: u32) -> Option<String> {
        self.events.iter().find_map(|e| match e {
            Event::Headers {
                stream_id,
                headers,
                end_stream: true,
            } if *stream_id == id => header(headers, "grpc-status").map(Into::into),
            _ => None,
        })
    }
}

/// Counts the unary calls it is asked to handle.
struct Counting(usize);

impl Handler for Counting {
    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        self.0 += 1;
        Some(Ok(request.to_vec()))
    }
}

#[test]
fn malformed_timeout_is_answered_with_invalid_argument() {
    for bad in [
        "",
        "S",
        "1",
        "123456789S",
        "1s",
        "1x",
        "-1S",
        "1 S",
        "1.5S",
        "S1",
    ] {
        let mut raw = Raw::new();
        let mut handler = Counting(0);
        let id = raw.open(Some(bad), Some(b"x"), &mut handler);
        assert_eq!(raw.status(id).as_deref(), Some("3"), "{bad:?}");
        assert_eq!(handler.0, 0, "{bad:?} reached the handler");
        assert_eq!(raw.server.active_calls(), 0, "{bad:?}");
        assert_eq!(raw.server.next_deadline(), None, "{bad:?}");
    }
}

#[test]
fn well_formed_timeouts_are_accepted() {
    for good in ["1n", "5u", "5m", "5S", "5M", "5H", "00000010S", "99999999H"] {
        let mut raw = Raw::new();
        let mut handler = Counting(0);
        let id = raw.open(Some(good), Some(b"x"), &mut handler);
        assert_eq!(raw.status(id).as_deref(), Some("0"), "{good:?}");
        assert_eq!(handler.0, 1, "{good:?}");
    }
}

#[test]
fn expired_request_never_reaches_the_handler() {
    let mut raw = Raw::new();
    let mut handler = Counting(0);
    // Zero means the deadline has already passed.
    let id = raw.open(Some("0n"), Some(b"x"), &mut handler);
    assert_eq!(raw.status(id).as_deref(), Some("4"));
    assert_eq!(handler.0, 0);
    assert_eq!(raw.server.active_calls(), 0);
}

#[test]
fn unary_call_waiting_for_its_request_expires() {
    let mut raw = Raw::new();
    let mut handler = Counting(0);
    let id = raw.open(Some("100m"), None, &mut handler);
    assert_eq!(raw.server.active_calls(), 1);
    assert_eq!(raw.server.next_deadline(), Some(ms(100)));

    raw.server.tick(ms(99), &mut handler);
    assert_eq!(raw.server.active_calls(), 1);
    raw.server.tick(ms(100), &mut handler);
    assert_eq!(raw.server.active_calls(), 0);
    assert_eq!(raw.server.next_deadline(), None);

    raw.exchange(&mut handler);
    assert_eq!(raw.status(id).as_deref(), Some("4"));
    assert_eq!(handler.0, 0);
}

#[test]
fn deadline_counts_from_the_time_the_request_arrived() {
    let mut raw = Raw::new();
    let mut handler = Counting(0);
    raw.server.tick(Duration::from_secs(10), &mut handler);
    raw.open(Some("1S"), None, &mut handler);
    assert_eq!(raw.server.next_deadline(), Some(Duration::from_secs(11)));
    raw.server.tick(Duration::from_secs(5), &mut handler);
    assert_eq!(
        raw.server.now(),
        Duration::from_secs(10),
        "time never goes back"
    );
}

#[test]
fn request_without_a_timeout_has_no_deadline() {
    let mut raw = Raw::new();
    let mut handler = Counting(0);
    raw.open(None, None, &mut handler);
    assert_eq!(raw.server.next_deadline(), None);
    raw.server
        .tick(Duration::from_secs(1_000_000), &mut handler);
    assert_eq!(raw.server.active_calls(), 1);
}

#[test]
fn a_call_that_finishes_in_time_leaves_no_deadline() {
    let mut raw = Raw::new();
    let mut handler = Counting(0);
    let id = raw.open(Some("1S"), Some(b"ok"), &mut handler);
    assert_eq!(raw.status(id).as_deref(), Some("0"));
    assert_eq!(handler.0, 1);
    assert_eq!(raw.server.active_calls(), 0);
    assert_eq!(raw.server.next_deadline(), None);
    raw.server.tick(Duration::from_secs(60), &mut handler);
    assert!(!raw.server.has_output());
}

#[test]
fn next_deadline_is_the_earliest_of_the_active_calls() {
    let mut raw = Raw::new();
    let mut handler = Counting(0);
    raw.open(Some("300m"), None, &mut handler);
    raw.open(Some("100m"), None, &mut handler);
    raw.open(None, None, &mut handler);
    assert_eq!(raw.server.next_deadline(), Some(ms(100)));
    raw.server.tick(ms(100), &mut handler);
    assert_eq!(raw.server.next_deadline(), Some(ms(300)));
    assert_eq!(raw.server.active_calls(), 2);
}

/// Server-streaming method that never answers and records cancellations.
#[derive(Default)]
struct Stuck {
    cancelled: Vec<CallId>,
}

impl Handler for Stuck {
    fn call(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        None
    }

    fn method_kind(&self, _: &str) -> Option<MethodKind> {
        Some(MethodKind::ServerStreaming)
    }

    fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
        Ok(())
    }

    fn poll_response(
        &mut self,
        _: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        Poll::Pending
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        let call = ctx.id;
        self.cancelled.push(call);
    }
}

#[test]
fn streaming_call_expires_and_is_cancelled_exactly_once() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = Stuck::default();
    // `Client::tick` is never called, so only the server's deadline can fire.
    let id = client
        .start_streaming_with("/t.T/Watch", &CallOptions::timeout(ms(100)))
        .unwrap();
    client.send_message(id, b"go").unwrap();
    client.close_send(id).unwrap();
    pump(&mut client, &mut server, &mut handler);
    assert_eq!(server.active_calls(), 1);
    assert_eq!(server.next_deadline(), Some(ms(100)));

    server.tick(ms(99), &mut handler);
    assert_eq!(server.active_calls(), 1);
    assert!(handler.cancelled.is_empty());

    server.tick(ms(100), &mut handler);
    assert_eq!(server.active_calls(), 0);
    assert_eq!(handler.cancelled, [id]);
    client.recv(&server.take_output()).unwrap();
    assert_eq!(client.now(), Duration::ZERO);
    match client.try_next(id) {
        Some(Next::Done(Err(status))) => assert_eq!(status.code, Code::DeadlineExceeded),
        other => panic!("expected the server's deadline status, got {other:?}"),
    }

    server.tick(Duration::from_secs(10), &mut handler);
    assert_eq!(handler.cancelled, [id], "cancelled only once");
}

#[test]
fn streaming_call_expired_on_arrival_is_cancelled_without_a_response() {
    let mut raw = Raw::new();
    let mut handler = Stuck::default();
    let id = raw.open(Some("0n"), Some(b"go"), &mut handler);
    assert_eq!(raw.status(id).as_deref(), Some("4"));
    assert_eq!(handler.cancelled, [id]);
    assert_eq!(raw.server.active_calls(), 0);
}

// ---------------------------------------------------------------------------
// Handler context
// ---------------------------------------------------------------------------

/// Records the context of every call it is given, for unary and streaming
/// methods alike.
#[derive(Default)]
struct Recorder {
    seen: Vec<(&'static str, CallId, Option<Duration>)>,
}

impl Handler for Recorder {
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>> {
        self.seen.push(("call", ctx.id, ctx.deadline));
        Some(Ok(request.to_vec()))
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        (path == "/t.T/Watch").then_some(MethodKind::ServerStreaming)
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
        assert_eq!(ctx.path, "/t.T/Watch");
        self.seen.push(("on_message", ctx.id, ctx.deadline));
        Ok(())
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        self.seen.push(("on_half_close", ctx.id, ctx.deadline));
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        self.seen.push(("poll_response", ctx.id, ctx.deadline));
        Poll::Pending
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.seen.push(("on_cancel", ctx.id, ctx.deadline));
    }
}

#[test]
fn unary_handler_sees_the_deadline() {
    let mut raw = Raw::new();
    let mut handler = Recorder::default();
    raw.server.tick(Duration::from_secs(10), &mut handler);
    let id = raw.open(Some("2S"), Some(b"x"), &mut handler);
    assert_eq!(
        handler.seen,
        [("call", id, Some(Duration::from_secs(12)))],
        "the deadline is on the server's clock"
    );
}

#[test]
fn unary_handler_sees_no_deadline_without_a_timeout() {
    let mut raw = Raw::new();
    let mut handler = Recorder::default();
    let id = raw.open(None, Some(b"x"), &mut handler);
    assert_eq!(handler.seen, [("call", id, None)]);
}

#[test]
fn every_streaming_method_sees_the_deadline() {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = Recorder::default();
    server.tick(ms(500), &mut handler);
    let id = client
        .start_streaming_with("/t.T/Watch", &CallOptions::timeout(ms(100)))
        .unwrap();
    client.send_message(id, b"go").unwrap();
    client.close_send(id).unwrap();
    pump(&mut client, &mut server, &mut handler);
    server.tick(ms(600), &mut handler);

    let expected = Some(ms(600));
    for name in ["on_message", "on_half_close", "poll_response", "on_cancel"] {
        assert!(
            handler.seen.contains(&(name, id, expected)),
            "{name} did not see {expected:?}: {:?}",
            handler.seen
        );
    }
    assert!(handler.seen.iter().all(|(_, _, d)| *d == expected));
}

#[test]
fn a_tuple_of_handlers_passes_the_context_on() {
    let mut raw = Raw::new();
    let mut handler = (Recorder::default(), Counting(0));
    let id = raw.open(Some("1S"), Some(b"x"), &mut handler);
    assert_eq!(handler.0.seen, [("call", id, Some(Duration::from_secs(1)))]);
}

#[test]
fn remaining_counts_down_and_saturates() {
    let md = Metadata::new();
    let mut response = ResponseMetadata::default();
    let mut ctx = CallContext::new(
        "/t.T/M",
        1,
        Some(Duration::from_secs(10)),
        &md,
        &mut response,
    );
    assert_eq!(
        ctx.remaining(Duration::from_secs(4)),
        Some(Duration::from_secs(6))
    );
    assert_eq!(ctx.remaining(Duration::from_secs(10)), Some(Duration::ZERO));
    assert_eq!(ctx.remaining(Duration::from_secs(99)), Some(Duration::ZERO));
    ctx.deadline = None;
    assert_eq!(ctx.remaining(Duration::from_secs(4)), None);
}
