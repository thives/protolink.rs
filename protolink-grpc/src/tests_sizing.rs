//! Output sizing of the HTTP/2 output limit. These measure the serialized bytes
//! that gRPC traffic leaves waiting while the transport stops draining, which
//! is what [`protolink_http2::DEFAULT_MAX_PENDING_OUTPUT_BYTES`] must cover.

use alloc::vec::Vec;
use core::task::{Context, Poll, Waker};

use super::*;
use crate::{CallContext, Handler, MethodKind, Next, Route, Status};

/// Every call answers with messages of `size` bytes, until it is cancelled.
struct Endless {
    size: usize,
}

impl Handler for Endless {
    fn route(&self, _: &str) -> Option<Route> {
        Some(Route::new(MethodKind::ServerStreaming, 0))
    }

    fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
        Ok(())
    }

    fn poll_response(
        &mut self,
        _: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        Poll::Ready(Next::Message(alloc::vec![7; self.size]))
    }
}

/// `calls` server-streaming calls from a default client, whose requests reach a
/// default server that never drains its output. Returns the largest output the
/// server holds while it is polled to quiescence.
fn stalled_server_high_water(calls: usize, size: usize) -> usize {
    let mut client = Client::new(ClientConfig::default());
    let mut server = Server::new(ServerConfig::default());
    let mut handler = Endless { size };
    for _ in 0..calls {
        let id = client.start_streaming("/test/Endless").unwrap();
        client.send_message(id, b"request").unwrap();
        client.close_send(id).unwrap();
    }
    server.recv(&client.take_output(), &mut handler).unwrap();
    let mut high = 0;
    for _ in 0..64 * calls {
        server.poll(&mut handler, &mut Context::from_waker(Waker::noop()));
        high = high.max(server.pending_output().len());
    }
    high
}

/// `calls` streaming calls from a default client, each sending one message of
/// `size` bytes, while no output reaches the server. Returns the largest output
/// the client holds.
fn stalled_client_high_water(calls: usize, size: usize, metadata: usize) -> usize {
    let mut client = Client::new(ClientConfig::default());
    let mut high = client.pending_output().len();
    for _ in 0..calls {
        let mut headers = Metadata::new();
        if metadata > 0 {
            headers.insert("x-padding", &"p".repeat(metadata)).unwrap();
        }
        let options = CallOptions::default().with_metadata(headers);
        let id = client
            .start_streaming_with("/test/Upload", &options)
            .unwrap();
        client.send_message(id, &alloc::vec![1; size]).unwrap();
        high = high.max(client.pending_output().len());
    }
    high
}

#[test]
fn default_server_output_stays_far_below_the_default_limit_while_stalled() {
    // Measured: about 4.2 to 4.3 KiB. The flow gate admits a response message
    // only while less than one maximum-size wire message is pending.
    let limit = protolink_http2::DEFAULT_MAX_PENDING_OUTPUT_BYTES;
    for (calls, size) in [(1, 4096), (8, 4096), (8, 1024)] {
        let high = stalled_server_high_water(calls, size);
        assert!(
            high * 4 <= limit,
            "{calls} calls of {size} bytes held {high} bytes"
        );
        assert!(high > size, "the measurement saw a response");
    }
}

#[test]
fn default_client_output_stays_within_half_the_default_limit_while_stalled() {
    // Eight calls (the most a default server admits at once), each sending one
    // message of the largest size, with no metadata, 1 KiB and 8 KiB of it.
    // Measured: about 34, 42 and 99 KiB. The last is the worst case: every call
    // carries a whole header list.
    let limit = protolink_http2::DEFAULT_MAX_PENDING_OUTPUT_BYTES;
    for (metadata, measured) in [(0, 34 << 10), (1024, 42 << 10), (8 * 1024, 100 << 10)] {
        let high = stalled_client_high_water(8, 4096, metadata);
        assert!(
            high <= limit / 2,
            "eight calls with {metadata} bytes of metadata held {high} bytes"
        );
        assert!(high <= measured, "the measured peak grew: {high} bytes");
    }
}

#[test]
fn a_default_connection_handshake_is_small() {
    let client = Client::new(ClientConfig::default());
    let server = Server::new(ServerConfig::default());
    assert!(client.pending_output().len() < 128);
    assert!(server.pending_output().len() < 128);
}

#[test]
fn large_windows_and_messages_must_raise_the_limit() {
    // A peer that advertises a 1 MiB window receives a 1 MiB message in one
    // burst: the default limit refuses it, and a raised limit admits it.
    let mib: usize = 1 << 20;
    let peer = ClientConfig {
        http2: protolink_http2::Config {
            initial_window_size: mib as u32,
            connection_window_size: mib as u32,
            flow_control: protolink_http2::FlowControl::Manual,
            ..protolink_http2::Config::default()
        },
        max_message_size: 2 * mib,
        ..ClientConfig::default()
    };
    let serve = |limit: usize| {
        let mut client = Client::new(peer.clone());
        let mut server = Server::new(ServerConfig {
            max_message_size: 2 * mib,
            http2: protolink_http2::Config {
                max_pending_output_bytes: limit,
                ..ServerConfig::default().http2
            },
            ..ServerConfig::default()
        });
        let mut handler = Endless { size: mib };
        let id = client.start_streaming("/test/Endless").unwrap();
        client.send_message(id, b"request").unwrap();
        client.close_send(id).unwrap();
        server.recv(&client.take_output(), &mut handler).unwrap();
        let mut result = Ok(());
        for _ in 0..16 {
            server.poll(&mut handler, &mut Context::from_waker(Waker::noop()));
            if server.failure().is_some() {
                result = Err(());
                break;
            }
        }
        (result, server.failure().cloned())
    };

    let (result, failure) = serve(protolink_http2::DEFAULT_MAX_PENDING_OUTPUT_BYTES);
    assert!(result.is_err());
    assert!(matches!(
        failure,
        Some(protolink_http2::Error::OutputLimitExceeded { .. })
    ));
    let (result, _) = serve(4 * mib);
    assert!(result.is_ok());
}
