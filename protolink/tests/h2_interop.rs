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
    let req = Request::post(format!("http://localhost{path}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .unwrap();
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
