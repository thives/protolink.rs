//! gzip message compression through the I/O drivers, against the `h2` crate
//! (the HTTP/2 stack under hyper and tonic) and `flate2`, which are
//! independent implementations of the wire format.
#![cfg(all(feature = "tokio", feature = "miniz-oxide"))]

use std::io::{Read, Write};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Request, Response};
use protolink::grpc::{FnHandler, Status};
use protolink::{ClientConfig, Compression, ServerConfig};

/// Compressible, and longer than the default `min_size`.
fn text() -> Vec<u8> {
    b"protolink compresses long, repetitive messages. "
        .iter()
        .copied()
        .cycle()
        .take(3000)
        .collect()
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn gunzip(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(data)
        .read_to_end(&mut out)
        .unwrap();
    out
}

/// A length-prefixed message; `compressed` payloads are already gzip.
fn framed(payload: &[u8], compressed: bool) -> Bytes {
    let mut v = vec![u8::from(compressed)];
    v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    v.extend_from_slice(payload);
    v.into()
}

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("timed out")
}

fn echo(path: &str, req: &[u8]) -> Option<Result<Vec<u8>, Status>> {
    (path == "/echo.Echo/Echo").then(|| Ok(req.to_vec()))
}

struct Reply {
    status: http::StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
    trailers: HeaderMap,
}

/// One request from an `h2` client with the given extra headers.
async fn h2_call(
    sender: &mut h2::client::SendRequest<Bytes>,
    headers: &[(&str, &str)],
    body: Bytes,
) -> Reply {
    let mut req = Request::post("http://localhost/echo.Echo/Echo")
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    let (resp, mut stream) = sender
        .clone()
        .ready()
        .await
        .unwrap()
        .send_request(req.body(()).unwrap(), false)
        .unwrap();
    stream.send_data(body, true).unwrap();
    let resp = resp.await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let mut recv = resp.into_body();
    let mut body = Vec::new();
    while let Some(chunk) = recv.data().await {
        let chunk = chunk.unwrap();
        recv.flow_control().release_capacity(chunk.len()).unwrap();
        body.extend_from_slice(&chunk);
    }
    // Trailers-only responses carry grpc-status in the initial headers.
    let trailers = recv
        .trailers()
        .await
        .unwrap()
        .unwrap_or_else(|| headers.clone());
    Reply {
        status,
        headers,
        body,
        trailers,
    }
}

/// Serve with the given compression; the client end of the connection.
fn serve(server: Compression) -> tokio::io::DuplexStream {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut h = FnHandler(echo);
        let config = ServerConfig {
            compression: server,
            ..ServerConfig::default()
        };
        protolink::tokio::serve(a, &mut h, config).await.unwrap();
    });
    b
}

#[tokio::test]
async fn h2_client_gets_gzip_from_the_server() {
    let io = serve(Compression::gzip());
    with_timeout(async {
        let (mut sender, conn) = h2::client::handshake(io).await.unwrap();
        tokio::spawn(async move { conn.await.unwrap() });
        let data = text();

        // A gzip request written by flate2, answered in gzip.
        let reply = h2_call(
            &mut sender,
            &[("grpc-encoding", "gzip"), ("grpc-accept-encoding", "gzip")],
            framed(&gzip(&data), true),
        )
        .await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.trailers["grpc-status"], "0");
        assert_eq!(reply.headers["grpc-encoding"], "gzip");
        assert_eq!(reply.headers["grpc-accept-encoding"], "gzip,identity");
        assert_eq!(reply.body[0], 1, "response not flagged as compressed");
        assert!(reply.body.len() < data.len() / 4);
        assert_eq!(gunzip(&reply.body[5..]), data);

        // Plain request, client that cannot read gzip: plain response.
        let reply = h2_call(&mut sender, &[], framed(&data, false)).await;
        assert_eq!(reply.trailers["grpc-status"], "0");
        assert!(!reply.headers.contains_key("grpc-encoding"));
        assert_eq!(reply.body, framed(&data, false));

        // Explicit identity, and a plain message under a gzip encoding.
        for headers in [
            &[("grpc-encoding", "identity")][..],
            &[
                ("grpc-encoding", "gzip"),
                ("grpc-accept-encoding", "identity"),
            ],
        ] {
            let reply = h2_call(&mut sender, headers, framed(&data, false)).await;
            assert_eq!(reply.trailers["grpc-status"], "0", "{headers:?}");
            assert_eq!(reply.body, framed(&data, false));
        }

        // An encoding the server does not have.
        let reply = h2_call(
            &mut sender,
            &[("grpc-encoding", "zstd")],
            framed(b"whatever", true),
        )
        .await;
        assert_eq!(reply.trailers["grpc-status"], "12");
        assert_eq!(reply.trailers["grpc-accept-encoding"], "gzip,identity");

        // Corrupt gzip data fails the call without taking the connection down.
        let mut broken = gzip(&data);
        let n = broken.len();
        broken[n - 6] ^= 0xff;
        let reply = h2_call(
            &mut sender,
            &[("grpc-encoding", "gzip")],
            framed(&broken, true),
        )
        .await;
        assert_eq!(reply.trailers["grpc-status"], "13");
        let reply = h2_call(&mut sender, &[], framed(b"still alive", false)).await;
        assert_eq!(reply.trailers["grpc-status"], "0");
    })
    .await;
}

#[tokio::test]
async fn server_without_compression_rejects_gzip() {
    let io = serve(Compression::NONE);
    with_timeout(async {
        let (mut sender, conn) = h2::client::handshake(io).await.unwrap();
        tokio::spawn(async move { conn.await.unwrap() });
        let reply = h2_call(
            &mut sender,
            &[("grpc-encoding", "gzip")],
            framed(&gzip(&text()), true),
        )
        .await;
        assert_eq!(reply.trailers["grpc-status"], "12");
    })
    .await;
}

/// An `h2` server that checks the request's gzip and answers in gzip.
async fn gzip_echo_h2_server(io: tokio::io::DuplexStream) {
    let mut conn = h2::server::handshake(io).await.unwrap();
    while let Some(req) = conn.accept().await {
        let (req, mut respond) = req.unwrap();
        tokio::spawn(async move {
            assert_eq!(req.headers()["grpc-encoding"], "gzip");
            assert!(
                req.headers()["grpc-accept-encoding"]
                    .to_str()
                    .unwrap()
                    .contains("gzip")
            );
            let mut body = req.into_body();
            let mut data = Vec::new();
            while let Some(chunk) = body.data().await {
                let chunk = chunk.unwrap();
                body.flow_control().release_capacity(chunk.len()).unwrap();
                data.extend_from_slice(&chunk);
            }
            assert_eq!(data[0], 1, "request not flagged as compressed");
            let message = gunzip(&data[5..]);
            let resp = Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .header("grpc-encoding", "gzip")
                .body(())
                .unwrap();
            let mut send = respond.send_response(resp, false).unwrap();
            send.send_data(framed(&gzip(&message), true), false)
                .unwrap();
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            send.send_trailers(trailers).unwrap();
        });
    }
}

#[tokio::test]
async fn protolink_client_sends_and_reads_gzip() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(gzip_echo_h2_server(a));
    with_timeout(async {
        let mut client = protolink::tokio::client(
            b,
            ClientConfig {
                compression: Compression::gzip(),
                ..ClientConfig::default()
            },
        );
        let data = text();
        assert_eq!(client.unary("/echo.Echo/Echo", &data).await.unwrap(), data);
        // The encoding stays in effect for later calls on the connection.
        assert_eq!(client.unary("/echo.Echo/Echo", &data).await.unwrap(), data);
    })
    .await;
}

#[tokio::test]
async fn protolink_client_and_server_agree_on_gzip() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut h = FnHandler(echo);
        let config = ServerConfig {
            compression: Compression::gzip(),
            ..ServerConfig::default()
        };
        protolink::tokio::serve(a, &mut h, config).await.unwrap();
    });
    with_timeout(async {
        let mut client = protolink::tokio::client(
            b,
            ClientConfig {
                compression: Compression::gzip(),
                ..ClientConfig::default()
            },
        );
        let data = text();
        for message in [data.clone(), b"short".to_vec(), Vec::new(), data] {
            assert_eq!(
                client.unary("/echo.Echo/Echo", &message).await.unwrap(),
                message
            );
        }
    })
    .await;
}
