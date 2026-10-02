//! Interop with the `h2` crate (the HTTP/2 stack under hyper and tonic).
#![cfg(feature = "tokio")]

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Request, Response};
use protolink::grpc::{Code, FnHandler, Status};
use protolink::{ClientConfig, ServerConfig};

fn lpm(payload: &[u8]) -> Bytes {
    let mut v = vec![0];
    v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    v.extend_from_slice(payload);
    v.into()
}

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("timed out")
}

fn handler(path: &str, req: &[u8]) -> Option<Result<Vec<u8>, Status>> {
    match path {
        "/echo.Echo/Echo" => Some(Ok(req.to_vec())),
        "/echo.Echo/Fail" => Some(Err(Status::not_found("no such thing"))),
        _ => None,
    }
}

/// Send one gRPC request with an h2 client; returns (status, body, trailers).
async fn h2_call(
    sender: &mut h2::client::SendRequest<Bytes>,
    path: &str,
    body: Bytes,
) -> (http::StatusCode, Vec<u8>, HeaderMap) {
    h2_call_with(sender, path, body, &[]).await
}

/// [`h2_call`] with extra request headers.
async fn h2_call_with(
    sender: &mut h2::client::SendRequest<Bytes>,
    path: &str,
    body: Bytes,
    extra: &[(&str, &str)],
) -> (http::StatusCode, Vec<u8>, HeaderMap) {
    let mut req = Request::post(format!("http://localhost{path}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .unwrap();
    for (name, value) in extra {
        req.headers_mut().insert(
            http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    let (resp, mut stream) = sender
        .clone()
        .ready()
        .await
        .unwrap()
        .send_request(req, false)
        .unwrap();
    stream.send_data(body, true).unwrap();
    let resp = resp.await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let mut recv = resp.into_body();
    let mut data = Vec::new();
    while let Some(chunk) = recv.data().await {
        let chunk = chunk.unwrap();
        recv.flow_control().release_capacity(chunk.len()).unwrap();
        data.extend_from_slice(&chunk);
    }
    // Trailers-only responses carry grpc-status in the initial headers.
    let trailers = recv.trailers().await.unwrap().unwrap_or(headers);
    (status, data, trailers)
}

#[tokio::test]
async fn h2_client_against_protolink_server() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut h = FnHandler(handler);
        protolink::tokio::serve(a, &mut h, ServerConfig::default())
            .await
            .unwrap();
    });
    with_timeout(async {
        let (mut sender, conn) = h2::client::handshake(b).await.unwrap();
        tokio::spawn(async move { conn.await.unwrap() });

        let (status, body, trailers) = h2_call(&mut sender, "/echo.Echo/Echo", lpm(b"hello")).await;
        assert_eq!(status, 200);
        assert_eq!(body, lpm(b"hello"));
        assert_eq!(trailers["grpc-status"], "0");

        let (_, body, trailers) = h2_call(&mut sender, "/echo.Echo/Fail", lpm(b"")).await;
        assert!(body.is_empty());
        assert_eq!(trailers["grpc-status"], "5");
        assert_eq!(trailers["grpc-message"], "no such thing");

        let (_, _, trailers) = h2_call(&mut sender, "/echo.Echo/Missing", lpm(b"")).await;
        assert_eq!(trailers["grpc-status"], "12");

        // Several concurrent streams on one connection.
        let tasks: Vec<_> = (0..4u8)
            .map(|i| {
                let mut s = sender.clone();
                tokio::spawn(async move { h2_call(&mut s, "/echo.Echo/Echo", lpm(&[i; 3])).await })
            })
            .collect();
        for (i, task) in tasks.into_iter().enumerate() {
            let (_, body, _) = task.await.unwrap();
            assert_eq!(body, lpm(&[i as u8; 3]));
        }
    })
    .await;
}

#[tokio::test]
async fn protolink_client_against_h2_server() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut conn = h2::server::handshake(a).await.unwrap();
        while let Some(req) = conn.accept().await {
            let (req, mut respond) = req.unwrap();
            tokio::spawn(async move {
                let path = req.uri().path().to_owned();
                assert_eq!(req.headers()["content-type"], "application/grpc");
                let mut body = req.into_body();
                let mut data = Vec::new();
                while let Some(chunk) = body.data().await {
                    let chunk = chunk.unwrap();
                    body.flow_control().release_capacity(chunk.len()).unwrap();
                    data.extend_from_slice(&chunk);
                }
                let resp = Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(())
                    .unwrap();
                if path == "/echo.Echo/Fail" {
                    let mut resp = resp;
                    resp.headers_mut()
                        .insert("grpc-status", "7".parse().unwrap());
                    resp.headers_mut()
                        .insert("grpc-message", "denied%20%C3%A6".parse().unwrap());
                    respond.send_response(resp, true).unwrap();
                    return;
                }
                let mut send = respond.send_response(resp, false).unwrap();
                send.send_data(data.into(), false).unwrap();
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                send.send_trailers(trailers).unwrap();
            });
        }
    });
    with_timeout(async {
        let mut client = protolink::tokio::client(b, ClientConfig::default());
        assert_eq!(
            client.unary("/echo.Echo/Echo", b"abc").await.unwrap(),
            b"abc"
        );
        let big = vec![0x5a; 4000];
        assert_eq!(client.unary("/echo.Echo/Echo", &big).await.unwrap(), big);
        let err = client.unary("/echo.Echo/Fail", b"").await.unwrap_err();
        assert_eq!(err.code, Code::PermissionDenied);
        assert_eq!(err.message, "denied \u{e6}");
    })
    .await;
}

// --- Streaming ---------------------------------------------------------------

mod streaming {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use bytes::Bytes;
    use http::{HeaderMap, Request, Response};
    use protolink::grpc::{
        CallContext, CallId, Code, FnHandler, Handler, MethodKind, Next, Status,
    };
    use protolink::{ClientConfig, ServerConfig};

    use super::{handler, lpm, with_timeout};

    /// Split a body into its length-prefixed messages.
    fn messages(mut body: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while !body.is_empty() {
            assert_eq!(body[0], 0, "uncompressed");
            let len = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
            out.push(body[5..5 + len].to_vec());
            body = &body[5 + len..];
        }
        out
    }

    /// Externally driven responses of `/echo.Echo/Wait`.
    #[derive(Default)]
    struct Feed {
        items: VecDeque<Vec<u8>>,
        done: bool,
        waker: Option<Waker>,
    }

    #[derive(Default)]
    struct CallState {
        queue: VecDeque<Vec<u8>>,
        acc: Vec<u8>,
        end: Option<Result<(), Status>>,
    }

    /// Raw streaming handler:
    /// - `Repeat` (server streaming): request `[n, fail]` yields `[0]..[n-1]`,
    ///   then OK, or ABORTED if `fail == 1`.
    /// - `Concat` (client streaming): concatenates all requests.
    /// - `Chat` (bidi): echoes every request as it arrives.
    /// - `Wait` (server streaming): yields what `Feed` provides.
    #[derive(Default, Clone)]
    struct Streams {
        calls: Arc<Mutex<BTreeMap<CallId, CallState>>>,
        feed: Arc<Mutex<Feed>>,
        cancelled: Arc<Mutex<Vec<String>>>,
    }

    impl Handler for Streams {
        fn call(&mut self, _: &CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
            None
        }

        fn method_kind(&self, path: &str) -> Option<MethodKind> {
            match path {
                "/echo.Echo/Repeat" | "/echo.Echo/Wait" => Some(MethodKind::ServerStreaming),
                "/echo.Echo/Concat" => Some(MethodKind::ClientStreaming),
                "/echo.Echo/Chat" => Some(MethodKind::BidiStreaming),
                _ => None,
            }
        }

        fn on_message(&mut self, ctx: &CallContext<'_>, msg: &[u8]) -> Result<(), Status> {
            let path = ctx.path;
            let call = ctx.id;
            let mut calls = self.calls.lock().unwrap();
            let state = calls.entry(call).or_default();
            match path {
                "/echo.Echo/Repeat" => {
                    let n = *msg.first().unwrap_or(&0);
                    state.queue = (0..n).map(|i| vec![i]).collect();
                    state.end = Some(match msg.get(1) {
                        Some(1) => Err(Status::new(Code::Aborted, "stopped")),
                        _ => Ok(()),
                    });
                }
                "/echo.Echo/Concat" => state.acc.extend_from_slice(msg),
                "/echo.Echo/Chat" => state.queue.push_back(msg.to_vec()),
                _ => {}
            }
            Ok(())
        }

        fn on_half_close(&mut self, ctx: &CallContext<'_>) -> Result<(), Status> {
            let path = ctx.path;
            let call = ctx.id;
            let mut calls = self.calls.lock().unwrap();
            let state = calls.entry(call).or_default();
            match path {
                "/echo.Echo/Concat" => {
                    let acc = std::mem::take(&mut state.acc);
                    state.queue.push_back(acc);
                }
                "/echo.Echo/Chat" => state.end = Some(Ok(())),
                _ => {}
            }
            Ok(())
        }

        fn poll_response(
            &mut self,
            ctx: &CallContext<'_>,
            cx: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            let path = ctx.path;
            let call = ctx.id;
            if path == "/echo.Echo/Wait" {
                let mut feed = self.feed.lock().unwrap();
                if let Some(item) = feed.items.pop_front() {
                    return Poll::Ready(Next::Message(item));
                }
                if feed.done {
                    return Poll::Ready(Next::Done(Ok(())));
                }
                feed.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let mut calls = self.calls.lock().unwrap();
            let Some(state) = calls.get_mut(&call) else {
                return Poll::Pending;
            };
            if let Some(msg) = state.queue.pop_front() {
                return Poll::Ready(Next::Message(msg));
            }
            match state.end.take() {
                Some(end) => {
                    calls.remove(&call);
                    Poll::Ready(Next::Done(end))
                }
                None => Poll::Pending,
            }
        }

        fn on_cancel(&mut self, ctx: &CallContext<'_>) {
            let path = ctx.path;
            let call = ctx.id;
            self.calls.lock().unwrap().remove(&call);
            self.cancelled.lock().unwrap().push(path.to_owned());
        }
    }

    fn grpc_request(path: &str) -> Request<()> {
        Request::post(format!("http://localhost{path}"))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(())
            .unwrap()
    }

    /// Serve `Streams` (plus the unary echo handler) on a duplex; returns the
    /// h2 client sender, the handler state and the server task.
    async fn start_server() -> (
        h2::client::SendRequest<Bytes>,
        Streams,
        tokio::task::JoinHandle<()>,
    ) {
        let (a, b) = tokio::io::duplex(16 * 1024);
        let streams = Streams::default();
        let server_streams = streams.clone();
        let server = tokio::spawn(async move {
            let mut h = (FnHandler(handler), server_streams);
            protolink::tokio::serve(a, &mut h, ServerConfig::default())
                .await
                .unwrap();
        });
        let (sender, conn) = h2::client::handshake(b).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        (sender, streams, server)
    }

    /// Read a whole response: (messages, grpc-status, grpc-message).
    async fn read_all(resp: h2::client::ResponseFuture) -> (Vec<Vec<u8>>, String, Option<String>) {
        let resp = resp.await.unwrap();
        assert_eq!(resp.status(), 200);
        let headers = resp.headers().clone();
        let mut body = resp.into_body();
        let mut data = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk.unwrap();
            body.flow_control().release_capacity(chunk.len()).unwrap();
            data.extend_from_slice(&chunk);
        }
        let trailers = body.trailers().await.unwrap().unwrap_or(headers);
        let status = trailers["grpc-status"].to_str().unwrap().to_owned();
        let message = trailers
            .get("grpc-message")
            .map(|m| m.to_str().unwrap().to_owned());
        (messages(&data), status, message)
    }

    /// Read from `body` until one more complete message is available.
    async fn next_message(body: &mut h2::RecvStream, buf: &mut Vec<u8>) -> Vec<u8> {
        loop {
            if buf.len() >= 5 {
                let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
                if buf.len() >= 5 + len {
                    let msg = buf[5..5 + len].to_vec();
                    buf.drain(..5 + len);
                    return msg;
                }
            }
            let chunk = body.data().await.expect("stream ended").unwrap();
            body.flow_control().release_capacity(chunk.len()).unwrap();
            buf.extend_from_slice(&chunk);
        }
    }

    #[tokio::test]
    async fn h2_client_server_streaming() {
        let (sender, _, _) = start_server().await;
        with_timeout(async {
            let call = |body: Bytes| {
                let sender = sender.clone();
                async move {
                    let mut sender = sender.ready().await.unwrap();
                    let (resp, mut stream) = sender
                        .send_request(grpc_request("/echo.Echo/Repeat"), false)
                        .unwrap();
                    stream.send_data(body, true).unwrap();
                    read_all(resp).await
                }
            };

            let (msgs, status, _) = call(lpm(&[3])).await;
            assert_eq!(msgs, [vec![0], vec![1], vec![2]]);
            assert_eq!(status, "0");

            // Empty stream: a trailers-only OK.
            let (msgs, status, _) = call(lpm(&[0])).await;
            assert!(msgs.is_empty());
            assert_eq!(status, "0");

            // Error after messages: the messages are kept, then the status.
            let (msgs, status, message) = call(lpm(&[2, 1])).await;
            assert_eq!(msgs, [vec![0], vec![1]]);
            assert_eq!(status, (Code::Aborted as u8).to_string());
            assert_eq!(message.as_deref(), Some("stopped"));

            // No request message: INTERNAL.
            let (msgs, status, _) = call(Bytes::new()).await;
            assert!(msgs.is_empty());
            assert_eq!(status, (Code::Internal as u8).to_string());

            // Two request messages: INTERNAL.
            let two: Vec<u8> = [lpm(&[1]), lpm(&[1])].concat();
            let (_, status, _) = call(two.into()).await;
            assert_eq!(status, (Code::Internal as u8).to_string());

            // Unary calls still work on the same connection.
            let (status, body, trailers) =
                super::h2_call(&mut sender.clone(), "/echo.Echo/Echo", lpm(b"hi")).await;
            assert_eq!(status, 200);
            assert_eq!(body, lpm(b"hi"));
            assert_eq!(trailers["grpc-status"], "0");
        })
        .await;
    }

    #[tokio::test]
    async fn h2_client_client_streaming() {
        let (sender, _, _) = start_server().await;
        with_timeout(async {
            let mut sender = sender.ready().await.unwrap();
            let (resp, mut stream) = sender
                .send_request(grpc_request("/echo.Echo/Concat"), false)
                .unwrap();
            // Two messages in one DATA frame, then one split byte by byte.
            let two: Vec<u8> = [lpm(b"ab"), lpm(b"cd")].concat();
            stream.send_data(two.into(), false).unwrap();
            for byte in lpm(b"ef").iter() {
                stream
                    .send_data(Bytes::copy_from_slice(&[*byte]), false)
                    .unwrap();
            }
            stream.send_data(Bytes::new(), true).unwrap();
            let (msgs, status, _) = read_all(resp).await;
            assert_eq!(msgs, [b"abcdef".to_vec()]);
            assert_eq!(status, "0");

            // Empty client stream.
            let (resp, mut stream) = sender
                .send_request(grpc_request("/echo.Echo/Concat"), false)
                .unwrap();
            stream.send_data(Bytes::new(), true).unwrap();
            let (msgs, status, _) = read_all(resp).await;
            assert_eq!(msgs, [Vec::<u8>::new()]);
            assert_eq!(status, "0");
        })
        .await;
    }

    #[tokio::test]
    async fn h2_client_bidi_streaming() {
        let (sender, _, _) = start_server().await;
        with_timeout(async {
            // Several concurrent ping-pong calls on one connection: every
            // reply arrives before the client half-closes.
            let tasks: Vec<_> = (0..4u8)
                .map(|t| {
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        let mut sender = sender.ready().await.unwrap();
                        let (resp, mut stream) = sender
                            .send_request(grpc_request("/echo.Echo/Chat"), false)
                            .unwrap();
                        let resp = resp.await.unwrap();
                        assert_eq!(resp.status(), 200);
                        let mut body = resp.into_body();
                        let mut buf = Vec::new();
                        for i in 0..3u8 {
                            stream.send_data(lpm(&[t, i]), false).unwrap();
                            assert_eq!(next_message(&mut body, &mut buf).await, [t, i]);
                        }
                        stream.send_data(Bytes::new(), true).unwrap();
                        assert!(body.data().await.is_none());
                        let trailers = body.trailers().await.unwrap().unwrap();
                        assert_eq!(trailers["grpc-status"], "0");
                    })
                })
                .collect();
            for task in tasks {
                task.await.unwrap();
            }

            // Empty bidi call.
            let mut sender = sender.ready().await.unwrap();
            let (resp, mut stream) = sender
                .send_request(grpc_request("/echo.Echo/Chat"), false)
                .unwrap();
            stream.send_data(Bytes::new(), true).unwrap();
            let (msgs, status, _) = read_all(resp).await;
            assert!(msgs.is_empty());
            assert_eq!(status, "0");
        })
        .await;
    }

    #[tokio::test]
    async fn h2_client_reset_and_disconnect_cancel_calls() {
        let (sender, streams, server) = start_server().await;
        with_timeout(async {
            let mut sender = sender.clone().ready().await.unwrap();
            let (resp, mut stream) = sender
                .send_request(grpc_request("/echo.Echo/Chat"), false)
                .unwrap();
            stream.send_data(lpm(b"x"), false).unwrap();
            let mut body = resp.await.unwrap().into_body();
            assert_eq!(next_message(&mut body, &mut Vec::new()).await, b"x");
            stream.send_reset(h2::Reason::CANCEL);
            while streams.cancelled.lock().unwrap().is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            assert_eq!(*streams.cancelled.lock().unwrap(), ["/echo.Echo/Chat"]);
            assert!(streams.calls.lock().unwrap().is_empty());

            // A call still open when the connection drops is cancelled too.
            let (resp, mut stream) = sender
                .send_request(grpc_request("/echo.Echo/Chat"), false)
                .unwrap();
            stream.send_data(lpm(b"y"), false).unwrap();
            let mut body = resp.await.unwrap().into_body();
            assert_eq!(next_message(&mut body, &mut Vec::new()).await, b"y");
        })
        .await;
        drop(sender);
        // Dropping every handle closes the h2 connection and the transport.
        with_timeout(server).await.unwrap();
        assert_eq!(
            *streams.cancelled.lock().unwrap(),
            ["/echo.Echo/Chat", "/echo.Echo/Chat"]
        );
        assert!(streams.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn handler_wakes_server_from_another_task() {
        let (sender, streams, _) = start_server().await;
        let feed = streams.feed.clone();
        with_timeout(async {
            let mut sender = sender.ready().await.unwrap();
            let (resp, mut stream) = sender
                .send_request(grpc_request("/echo.Echo/Wait"), false)
                .unwrap();
            stream.send_data(lpm(b""), true).unwrap();
            let mut body = resp.await.unwrap().into_body();

            // Produce responses from another task, waking the server.
            tokio::spawn(async move {
                for i in 0..3u8 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    let mut f = feed.lock().unwrap();
                    f.items.push_back(vec![i]);
                    f.done = i == 2;
                    if let Some(w) = f.waker.take() {
                        w.wake();
                    }
                }
            });
            let mut buf = Vec::new();
            for i in 0..3u8 {
                assert_eq!(next_message(&mut body, &mut buf).await, [i]);
            }
            assert!(body.data().await.is_none());
            let trailers = body.trailers().await.unwrap().unwrap();
            assert_eq!(trailers["grpc-status"], "0");
        })
        .await;
    }

    /// h2 server with streaming endpoints:
    /// - `Chat` echoes the request bytes back one byte per DATA frame as they
    ///   arrive, then OK;
    /// - `Many` sends three messages in one DATA frame, then OK;
    /// - `Empty` answers trailers-only OK;
    /// - `FailLate` sends one message, then `grpc-status: 13`;
    /// - `Reset` sends one message, then resets the stream (CANCEL);
    /// - `Drop` sends one message, then drops the whole connection.
    fn spawn_h2_server(io: tokio::io::DuplexStream) {
        tokio::spawn(async move {
            let mut conn = h2::server::handshake(io).await.unwrap();
            while let Some(req) = conn.accept().await {
                let (req, mut respond) = req.unwrap();
                let path = req.uri().path().to_owned();
                let ok = || {
                    Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .body(())
                        .unwrap()
                };
                let status = |code: &str| {
                    let mut t = HeaderMap::new();
                    t.insert("grpc-status", code.parse().unwrap());
                    t
                };
                if path == "/echo.Echo/Drop" {
                    let mut send = respond.send_response(ok(), false).unwrap();
                    send.send_data(lpm(b"bye"), false).unwrap();
                    // Flush, then drop the connection.
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_millis(50), conn.accept())
                            .await;
                    return;
                }
                let mut body = req.into_body();
                tokio::spawn(async move {
                    match path.as_str() {
                        "/echo.Echo/Chat" => {
                            let mut send = respond.send_response(ok(), false).unwrap();
                            while let Some(chunk) = body.data().await {
                                let chunk = chunk.unwrap();
                                body.flow_control().release_capacity(chunk.len()).unwrap();
                                for b in chunk.iter() {
                                    send.send_data(Bytes::copy_from_slice(&[*b]), false)
                                        .unwrap();
                                }
                            }
                            send.send_trailers(status("0")).unwrap();
                        }
                        "/echo.Echo/Many" => {
                            while body.data().await.is_some() {}
                            let mut send = respond.send_response(ok(), false).unwrap();
                            let all: Vec<u8> = [lpm(b"1"), lpm(b""), lpm(b"333")].concat();
                            send.send_data(all.into(), false).unwrap();
                            send.send_trailers(status("0")).unwrap();
                        }
                        "/echo.Echo/Empty" => {
                            while body.data().await.is_some() {}
                            let mut resp = ok();
                            resp.headers_mut().extend(status("0"));
                            respond.send_response(resp, true).unwrap();
                        }
                        "/echo.Echo/FailLate" => {
                            let mut send = respond.send_response(ok(), false).unwrap();
                            send.send_data(lpm(b"partial"), false).unwrap();
                            send.send_trailers(status("13")).unwrap();
                        }
                        "/echo.Echo/Reset" => {
                            let mut send = respond.send_response(ok(), false).unwrap();
                            send.send_data(lpm(b"partial"), false).unwrap();
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            send.send_reset(h2::Reason::CANCEL);
                        }
                        _ => panic!("unexpected path {path}"),
                    }
                });
            }
        });
    }

    #[tokio::test]
    async fn protolink_streaming_client_against_h2_server() {
        let (a, b) = tokio::io::duplex(16 * 1024);
        spawn_h2_server(a);
        with_timeout(async {
            let client = protolink::tokio::client(b, ClientConfig::default());

            // Bidi: replies arrive byte by byte, before the half-close.
            let mut call = client.streaming("/echo.Echo/Chat").unwrap();
            for msg in [&b"hello"[..], b"", b"world"] {
                call.send(msg).await.unwrap();
                assert_eq!(call.message().await.unwrap().as_deref(), Some(msg));
            }
            // Several requests before reading.
            for i in 0..5u8 {
                call.send(&[i; 100]).await.unwrap();
            }
            call.close_send().await.unwrap();
            for i in 0..5u8 {
                assert_eq!(call.message().await.unwrap(), Some(vec![i; 100]));
            }
            assert_eq!(call.message().await.unwrap(), None);
            assert_eq!(call.message().await.unwrap(), None, "end is sticky");
            drop(call);

            // Server streaming: three messages in one DATA frame.
            let mut call = client.streaming("/echo.Echo/Many").unwrap();
            call.send(b"req").await.unwrap();
            call.close_send().await.unwrap();
            let mut got = Vec::new();
            while let Some(m) = call.message().await.unwrap() {
                got.push(m);
            }
            assert_eq!(got, [b"1".to_vec(), Vec::new(), b"333".to_vec()]);
            drop(call);

            // Empty, trailers-only stream.
            let mut call = client.streaming("/echo.Echo/Empty").unwrap();
            call.close_send().await.unwrap();
            assert_eq!(call.message().await.unwrap(), None);
            drop(call);

            // Error after a message: the message, then the status.
            let mut call = client.streaming("/echo.Echo/FailLate").unwrap();
            call.close_send().await.unwrap();
            assert_eq!(
                call.message().await.unwrap().as_deref(),
                Some(&b"partial"[..])
            );
            let err = call.message().await.unwrap_err();
            assert_eq!(err.code, Code::Internal);
            drop(call);

            // Peer reset after a message.
            let mut call = client.streaming("/echo.Echo/Reset").unwrap();
            call.close_send().await.unwrap();
            assert_eq!(
                call.message().await.unwrap().as_deref(),
                Some(&b"partial"[..])
            );
            let err = call.message().await.unwrap_err();
            assert_eq!(err.code, Code::Cancelled);
            drop(call);

            // Cancel by drop, then reuse the connection.
            let mut call = client.streaming("/echo.Echo/Chat").unwrap();
            call.send(b"abandoned").await.unwrap();
            drop(call);
            let mut call = client.streaming("/echo.Echo/Chat").unwrap();
            call.send(b"again").await.unwrap();
            assert_eq!(
                call.message().await.unwrap().as_deref(),
                Some(&b"again"[..])
            );
            drop(call);

            // Connection failure mid-stream.
            let mut call = client.streaming("/echo.Echo/Drop").unwrap();
            call.close_send().await.unwrap();
            assert_eq!(call.message().await.unwrap().as_deref(), Some(&b"bye"[..]));
            let err = call.message().await.unwrap_err();
            assert_eq!(err.code, Code::Unavailable);
        })
        .await;
    }
}

// --- Deadlines ---------------------------------------------------------------

mod deadlines {
    use std::task::{Context, Poll};
    use std::time::Duration;

    use bytes::Bytes;
    use http::{HeaderMap, Response};
    use protolink::grpc::{CallContext, Code, Handler, MethodKind, Next, Status};
    use protolink::{CallOptions, ClientConfig, ServerConfig};

    use super::{h2_call_with, handler, lpm, with_timeout};

    const QUIET: &str = "/t.T/Quiet";

    /// A server-streaming method that never answers, plus the echo methods.
    struct QuietEcho;

    impl Handler for QuietEcho {
        fn call(&mut self, ctx: &CallContext<'_>, req: &[u8]) -> Option<Result<Vec<u8>, Status>> {
            handler(ctx.path, req)
        }

        fn method_kind(&self, path: &str) -> Option<MethodKind> {
            (path == QUIET).then_some(MethodKind::ServerStreaming)
        }

        fn on_message(&mut self, _: &CallContext<'_>, _: &[u8]) -> Result<(), Status> {
            Ok(())
        }

        fn poll_response(
            &mut self,
            _: &CallContext<'_>,
            _: &mut Context<'_>,
        ) -> Poll<Next<Vec<u8>>> {
            Poll::Pending
        }
    }

    /// An h2 client connected to a protolink server (with a timer).
    async fn h2_client() -> h2::client::SendRequest<Bytes> {
        let (a, b) = tokio::io::duplex(16 * 1024);
        tokio::spawn(async move {
            let _ = protolink::tokio::serve(a, &mut QuietEcho, ServerConfig::default()).await;
        });
        let (sender, conn) = h2::client::handshake(b).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        sender
    }

    #[tokio::test(start_paused = true)]
    async fn h2_client_timeout_expires_a_quiet_call() {
        with_timeout(async {
            let mut sender = h2_client().await;
            let (status, body, trailers) =
                h2_call_with(&mut sender, QUIET, lpm(b"x"), &[("grpc-timeout", "100m")]).await;
            assert_eq!(status, 200);
            assert!(body.is_empty());
            assert_eq!(trailers["grpc-status"], "4", "DEADLINE_EXCEEDED");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn h2_client_zero_timeout_is_already_expired() {
        with_timeout(async {
            let mut sender = h2_client().await;
            let (_, _, trailers) =
                h2_call_with(&mut sender, QUIET, lpm(b"x"), &[("grpc-timeout", "0n")]).await;
            assert_eq!(trailers["grpc-status"], "4");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn h2_client_malformed_timeout_is_rejected() {
        with_timeout(async {
            let mut sender = h2_client().await;
            for bad in ["10x", "10", "S", "123456789S", "-5S", "5 S"] {
                let (_, body, trailers) = h2_call_with(
                    &mut sender,
                    "/echo.Echo/Echo",
                    lpm(b"x"),
                    &[("grpc-timeout", bad)],
                )
                .await;
                assert!(body.is_empty(), "{bad}");
                assert_eq!(trailers["grpc-status"], "3", "INVALID_ARGUMENT for {bad:?}");
            }
            // The connection is still fine.
            let (_, body, trailers) =
                h2_call_with(&mut sender, "/echo.Echo/Echo", lpm(b"ok"), &[]).await;
            assert_eq!(body, lpm(b"ok"));
            assert_eq!(trailers["grpc-status"], "0");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn h2_client_generous_timeout_does_not_disturb_a_call() {
        with_timeout(async {
            let mut sender = h2_client().await;
            for timeout in ["5S", "1H", "99999999n", "1m"] {
                let (_, body, trailers) = h2_call_with(
                    &mut sender,
                    "/echo.Echo/Echo",
                    lpm(b"fast"),
                    &[("grpc-timeout", timeout)],
                )
                .await;
                assert_eq!(body, lpm(b"fast"), "{timeout}");
                assert_eq!(trailers["grpc-status"], "0", "{timeout}");
            }
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn protolink_client_sends_grpc_timeout_and_resets_on_expiry() {
        let (a, b) = tokio::io::duplex(16 * 1024);
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reset_tx, mut reset_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut conn = h2::server::handshake(a).await.unwrap();
            while let Some(req) = conn.accept().await {
                let (req, mut respond) = req.unwrap();
                let path = req.uri().path().to_owned();
                let timeout = req
                    .headers()
                    .get("grpc-timeout")
                    .map(|v| v.to_str().unwrap().to_owned());
                seen_tx.send((path.clone(), timeout)).unwrap();
                let reset_tx = reset_tx.clone();
                tokio::spawn(async move {
                    if path == "/echo.Echo/Hang" {
                        // Never answer; report how the client ends the stream.
                        let reason = std::future::poll_fn(|cx| respond.poll_reset(cx))
                            .await
                            .unwrap();
                        reset_tx.send(reason).unwrap();
                        return;
                    }
                    let mut body = req.into_body();
                    while let Some(chunk) = body.data().await {
                        let chunk = chunk.unwrap();
                        body.flow_control().release_capacity(chunk.len()).unwrap();
                    }
                    let resp = Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .body(())
                        .unwrap();
                    let mut send = respond.send_response(resp, false).unwrap();
                    send.send_data(lpm(b"ok"), false).unwrap();
                    let mut trailers = HeaderMap::new();
                    trailers.insert("grpc-status", "0".parse().unwrap());
                    send.send_trailers(trailers).unwrap();
                });
            }
        });
        with_timeout(async {
            let mut client = protolink::tokio::client(b, ClientConfig::default());
            let err = client
                .unary_with(
                    "/echo.Echo/Hang",
                    b"",
                    CallOptions::timeout(Duration::from_millis(300)),
                )
                .await
                .unwrap_err();
            assert_eq!(err.code, Code::DeadlineExceeded);
            // The next operation writes the reset of the expired call.
            assert_eq!(client.unary("/echo.Echo/Echo", b"").await.unwrap(), b"ok");

            let (path, timeout) = seen_rx.recv().await.unwrap();
            assert_eq!(path, "/echo.Echo/Hang");
            assert_eq!(timeout.as_deref(), Some("300000u"));
            let (path, timeout) = seen_rx.recv().await.unwrap();
            assert_eq!(path, "/echo.Echo/Echo");
            assert_eq!(timeout, None, "no timeout, no header");
            assert_eq!(reset_rx.recv().await.unwrap(), h2::Reason::CANCEL);
        })
        .await;
    }
}
