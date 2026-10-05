//! Custom metadata over the drivers, interoperating with the `h2` crate (the
//! HTTP/2 stack under hyper and tonic).
#![cfg(feature = "tokio")]

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Request, Response};
use protolink::{
    CallContext, CallOptions, ClientConfig, Code, Handler, Metadata, ServerConfig, Status,
};

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

/// Unary echo that reflects request metadata into the response:
/// `x-in` becomes `x-out` (headers), `x-bin-bin` becomes `x-bin-echo-bin`
/// (headers) and `x-trail: done` is always sent (trailers).
struct Reflect;

impl Handler for Reflect {
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>> {
        let text = ctx.metadata().get("x-in").map(str::to_owned);
        let bin = ctx.metadata().get_bin("x-bin-bin").map(<[u8]>::to_vec);
        let headers = ctx.initial_metadata_mut().unwrap();
        if let Some(text) = text {
            headers.insert("x-out", &text).unwrap();
        }
        if let Some(bin) = bin {
            headers.insert_bin("x-bin-echo-bin", &bin).unwrap();
        }
        ctx.trailing_metadata_mut()
            .insert("x-trail", "done")
            .unwrap();
        Some(Ok(request.to_vec()))
    }
}

/// One request with an h2 client; returns (response headers, body, trailers).
async fn h2_call(
    sender: &mut h2::client::SendRequest<Bytes>,
    path: &str,
    extra: &[(&str, &str)],
) -> (HeaderMap, Vec<u8>, HeaderMap) {
    let mut req = Request::post(format!("http://localhost{path}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .unwrap();
    for (name, value) in extra {
        req.headers_mut().append(
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
    stream.send_data(lpm(b"hi"), true).unwrap();
    let resp = resp.await.unwrap();
    let headers = resp.headers().clone();
    let mut recv = resp.into_body();
    let mut data = Vec::new();
    while let Some(chunk) = recv.data().await {
        let chunk = chunk.unwrap();
        recv.flow_control().release_capacity(chunk.len()).unwrap();
        data.extend_from_slice(&chunk);
    }
    // Trailers-only responses carry grpc-status in the initial headers.
    let trailers = recv
        .trailers()
        .await
        .unwrap()
        .unwrap_or_else(|| headers.clone());
    (headers, data, trailers)
}

#[tokio::test]
async fn h2_client_metadata_reaches_the_handler_and_comes_back() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut handler = Reflect;
        protolink::tokio::serve(a, &mut handler, ServerConfig::default())
            .await
            .unwrap();
    });
    with_timeout(async {
        let (mut sender, conn) = h2::client::handshake(b).await.unwrap();
        tokio::spawn(async move { conn.await.unwrap() });

        // A padded binary value is accepted and answered unpadded.
        let (headers, body, trailers) = h2_call(
            &mut sender,
            "/m.M/Reflect",
            &[
                ("x-in", "hello"),
                ("x-bin-bin", "AQI="),
                ("x-other", "ignored"),
            ],
        )
        .await;
        assert_eq!(body, lpm(b"hi"));
        assert_eq!(headers["x-out"], "hello");
        assert_eq!(headers["x-bin-echo-bin"], "AQI");
        assert!(headers.get("x-other").is_none());
        assert_eq!(trailers["x-trail"], "done");
        assert_eq!(trailers["grpc-status"], "0");

        // Without request metadata, only the handler's own is sent.
        let (headers, _, trailers) = h2_call(&mut sender, "/m.M/Reflect", &[]).await;
        assert!(headers.get("x-out").is_none());
        assert_eq!(trailers["x-trail"], "done");

        // A malformed binary value is the client's mistake.
        let (_, body, trailers) =
            h2_call(&mut sender, "/m.M/Reflect", &[("x-bin-bin", "not base64!")]).await;
        assert!(body.is_empty());
        assert_eq!(trailers["grpc-status"], "3");
    })
    .await;
}

#[tokio::test]
async fn protolink_client_metadata_against_an_h2_server() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut conn = h2::server::handshake(a).await.unwrap();
        while let Some(req) = conn.accept().await {
            let (req, mut respond) = req.unwrap();
            tokio::spawn(async move {
                let path = req.uri().path().to_owned();
                let headers = req.headers().clone();
                // Custom metadata arrives as plain headers, binary ones as
                // base64.
                assert_eq!(headers["x-req"], "r");
                assert_eq!(headers["x-req-bin"], "AQI");
                // Exactly one `user-agent`, the caller's if it set one.
                let agents: Vec<_> = headers.get_all("user-agent").iter().collect();
                assert_eq!(agents.len(), 1);
                if path == "/m.M/CustomAgent" {
                    assert_eq!(agents[0], "custom/9");
                } else {
                    assert_eq!(agents[0], "protolink");
                }
                let mut body = req.into_body();
                let mut data = Vec::new();
                while let Some(chunk) = body.data().await {
                    let chunk = chunk.unwrap();
                    body.flow_control().release_capacity(chunk.len()).unwrap();
                    data.extend_from_slice(&chunk);
                }
                let mut resp = Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("x-h", "head")
                    // Padded base64 from a peer that pads.
                    .header("x-b-bin", "AQI=")
                    .body(())
                    .unwrap();
                if path == "/m.M/Fail" {
                    resp.headers_mut()
                        .insert("grpc-status", "9".parse().unwrap());
                    resp.headers_mut().insert("x-t", "tail".parse().unwrap());
                    respond.send_response(resp, true).unwrap();
                    return;
                }
                let mut send = respond.send_response(resp, false).unwrap();
                send.send_data(data.into(), false).unwrap();
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                trailers.insert("x-t", "tail".parse().unwrap());
                send.send_trailers(trailers).unwrap();
            });
        }
    });
    with_timeout(async {
        let mut client = protolink::tokio::client(b, ClientConfig::default());
        let options = || {
            let mut md = Metadata::new();
            md.insert("x-req", "r").unwrap();
            md.insert_bin("x-req-bin", &[1, 2]).unwrap();
            CallOptions::metadata(md)
        };

        let reply = client
            .unary_with("/m.M/Echo", b"abc", options())
            .await
            .unwrap();
        assert_eq!(reply.message, b"abc");
        assert_eq!(reply.headers.get("x-h"), Some("head"));
        assert_eq!(reply.headers.get_bin("x-b-bin"), Some(&[1, 2][..]));
        assert_eq!(reply.headers.len(), 2, "protocol headers are not metadata");
        assert_eq!(reply.trailers.get("x-t"), Some("tail"));
        assert_eq!(reply.trailers.len(), 1);

        let mut custom_agent = options();
        custom_agent
            .metadata
            .insert("user-agent", "custom/9")
            .unwrap();
        client
            .unary_with("/m.M/CustomAgent", b"", custom_agent)
            .await
            .unwrap();

        // The metadata of a failed call is in its status.
        let err = client
            .unary_with("/m.M/Fail", b"", options())
            .await
            .unwrap_err();
        assert_eq!(err.code, Code::FailedPrecondition);
        assert_eq!(err.metadata.get("x-t"), Some("tail"));
        assert_eq!(err.metadata.get("x-h"), Some("head"));
    })
    .await;
}
