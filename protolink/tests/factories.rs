//! The client, server and link factories: building performs no I/O, the
//! configuration setters reach the drivers, and the tokio factories behave like
//! the generic async ones with a `TokioTimer`.
#![cfg(all(feature = "tokio", feature = "blocking"))]

use std::cell::Cell;
use std::convert::Infallible;
use std::rc::Rc;
use std::time::Duration;

use protolink::grpc::FnHandler;
use protolink::tokio::{FromTokio, TokioTimer};
use protolink::{CallOptions, Code};

/// A transport that records every operation and never completes one.
#[derive(Clone, Default)]
struct Untouched(Rc<Cell<usize>>);

impl Untouched {
    fn touched(&self) -> usize {
        self.0.get()
    }
}

impl embedded_io_async::ErrorType for Untouched {
    type Error = Infallible;
}

impl embedded_io_async::Read for Untouched {
    async fn read(&mut self, _: &mut [u8]) -> Result<usize, Infallible> {
        self.0.set(self.0.get() + 1);
        core::future::pending().await
    }
}

impl embedded_io_async::Write for Untouched {
    async fn write(&mut self, _: &[u8]) -> Result<usize, Infallible> {
        self.0.set(self.0.get() + 1);
        core::future::pending().await
    }

    async fn flush(&mut self) -> Result<(), Infallible> {
        self.0.set(self.0.get() + 1);
        core::future::pending().await
    }
}

impl embedded_io::Read for Untouched {
    fn read(&mut self, _: &mut [u8]) -> Result<usize, Infallible> {
        panic!("blocking construction must not read");
    }
}

impl embedded_io::Write for Untouched {
    fn write(&mut self, _: &[u8]) -> Result<usize, Infallible> {
        panic!("blocking construction must not write");
    }

    fn flush(&mut self) -> Result<(), Infallible> {
        panic!("blocking construction must not flush");
    }
}

impl protolink::blocking::ReadTimeout for Untouched {
    fn set_read_timeout(&mut self, _: Option<Duration>) {
        panic!("blocking construction must not configure the transport");
    }
}

impl protolink::blocking::OutputTimeout for Untouched {
    fn set_write_timeout(&mut self, _: Option<Duration>) {
        panic!("blocking construction must not configure the transport");
    }

    fn set_flush_timeout(&mut self, _: Option<Duration>) {
        panic!("blocking construction must not configure the transport");
    }
}

#[test]
fn building_performs_no_io() {
    let io = Untouched::default();

    let server = protolink::server(io.clone())
        .with_max_message_size(128)
        .without_deadlines()
        .build();
    let client = protolink::client(io.clone())
        .with_default_timeout(Duration::from_secs(1))
        .with_deadline_timer(protolink::NoTimer)
        .build();
    let link = protolink::link::reliable(io.clone()).build();
    let blocking_server = protolink::blocking::server(io.clone())
        .with_deadline_clock(protolink::NoTimer)
        .with_output_timeouts()
        .build();
    let blocking_client = protolink::blocking::client(io.clone())
        .with_deadline_clock(protolink::NoTimer)
        .with_output_timeouts()
        .build();

    // Starting a call only queues bytes in memory; the preface goes out with
    // the first operation that is polled.
    let call = client.streaming("/t.T/Chat").unwrap();
    assert!(client.with_inner(|c| c.has_output()));
    drop(call);
    assert_eq!(io.touched(), 0);
    drop((server, client, link, blocking_server, blocking_client));
    assert_eq!(io.touched(), 0);
}

#[tokio::test]
async fn tokio_factories_build_without_io() {
    let (a, mut b) = tokio::io::duplex(1024);
    let server = protolink::tokio::server_serial(a).build();
    let (c, mut d) = tokio::io::duplex(1024);
    let client = protolink::tokio::client_serial(c).build();
    let mut buf = [0u8; 1];
    for peer in [&mut b, &mut d] {
        let read = tokio::time::timeout(
            Duration::from_millis(20),
            tokio::io::AsyncReadExt::read(peer, &mut buf),
        );
        assert!(
            read.await.is_err(),
            "nothing must be written before serving"
        );
    }
    drop((server, client));
}

fn echo(_: &str, req: &[u8]) -> Option<Result<Vec<u8>, protolink::Status>> {
    Some(Ok(req.to_vec()))
}

#[tokio::test]
async fn configuration_setters_reach_the_server() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut services = FnHandler(echo);
        let server = protolink::tokio::server_serial(a)
            .with_max_message_size(8)
            .build();
        let _ = server.serve(&mut services).await;
    });
    let mut client = protolink::tokio::client_serial(b).build();
    assert_eq!(client.unary("/t.T/Echo", b"small").await.unwrap(), b"small");
    let err = client
        .unary("/t.T/Echo", b"far too large")
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::ResourceExhausted, "{err:?}");
}

#[tokio::test]
async fn configuration_setters_reach_the_client() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut services = FnHandler(echo);
        let _ = protolink::tokio::server_serial(a)
            .build()
            .serve(&mut services)
            .await;
    });
    let mut client = protolink::tokio::client_serial(b)
        .with_max_message_size(8)
        .build();
    let err = client
        .unary("/t.T/Echo", b"far too large")
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::ResourceExhausted, "{err:?}");
}

/// A peer that accepts the connection but never answers.
fn silent_peer() -> tokio::io::DuplexStream {
    let (a, b) = tokio::io::duplex(16 * 1024);
    tokio::spawn(async move {
        let mut a = a;
        let mut sink = Vec::new();
        let _ = tokio::io::AsyncReadExt::read_to_end(&mut a, &mut sink).await;
    });
    b
}

#[tokio::test(start_paused = true)]
async fn tokio_client_matches_the_async_client_with_a_tokio_timer() {
    let options = || CallOptions::timeout(Duration::from_millis(100));

    let mut tokio_client = protolink::tokio::client_serial(silent_peer()).build();
    let start = tokio::time::Instant::now();
    let tokio_err = tokio_client
        .unary_with("/t.T/Echo", b"x", options())
        .await
        .unwrap_err();
    let tokio_elapsed = start.elapsed();

    let mut async_client = protolink::client(FromTokio::new(silent_peer()))
        .with_deadline_timer(TokioTimer::new())
        .build();
    let start = tokio::time::Instant::now();
    let async_err = async_client
        .unary_with("/t.T/Echo", b"x", options())
        .await
        .unwrap_err();
    let async_elapsed = start.elapsed();

    assert_eq!(tokio_err.code, Code::DeadlineExceeded);
    assert_eq!(async_err.code, tokio_err.code);
    assert_eq!(async_elapsed, tokio_elapsed);
    assert_eq!(tokio_elapsed, Duration::from_millis(100));
}

#[tokio::test(start_paused = true)]
async fn a_tokio_client_without_deadlines_leaves_enforcement_to_the_server() {
    let mut client = protolink::tokio::client_serial(silent_peer())
        .without_deadlines()
        .build();
    let call = client.unary_with(
        "/t.T/Echo",
        b"x",
        CallOptions::timeout(Duration::from_millis(100)),
    );
    let waited = tokio::time::timeout(Duration::from_secs(5), call).await;
    assert!(
        waited.is_err(),
        "the client must not expire the call itself"
    );
}
