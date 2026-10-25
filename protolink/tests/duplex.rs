//! Same-connection progress of the duplex mode, over tokio pipes whose output
//! side is small and not drained by the peer. A request sent while the server's
//! response is blocked must still reach the server's handler. Serial mode does
//! not give that, and is the control of the test.
//!
//! The peer never reads what the server writes in these tests: reading it would
//! unblock the server.
#![cfg(all(feature = "tokio", feature = "std"))]

use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use protolink::grpc::{Client, ClientConfig, FnHandler};
use protolink::tokio::FromTokio;
use tokio::io::{AsyncWrite, AsyncWriteExt};

const PATH: &str = "/test.Echo/Echo";
/// Capacity of the server's output: smaller than a reply, so writing one blocks.
const OUTPUT: usize = 64;

/// The peer's end of a connection: a sans-IO gRPC client and the stream it
/// writes requests to.
struct Peer<W> {
    client: Client,
    writer: W,
}

impl<W: AsyncWrite + Unpin> Peer<W> {
    /// Start a unary call of `body` and send the client's output.
    async fn send(&mut self, body: &[u8]) {
        self.client.start_unary(PATH, body).unwrap();
        let out = self.client.take_output();
        self.writer.write_all(&out).await.unwrap();
    }
}

/// Sleep, without reading anything the server writes, until the handler has
/// seen `count` requests, or give up.
async fn wait_until(handled: &AtomicUsize, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while handled.load(Ordering::SeqCst) < count && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn duplex_server_handles_a_request_while_its_response_is_blocked() {
    let handled = Arc::new(AtomicUsize::new(0));
    // Requests reach the server on one pipe with room for everything; responses
    // leave on another, too small for the first reply.
    let (client_writes, server_reads) = tokio::io::duplex(64 * 1024);
    let (server_writes, _client_reads) = tokio::io::duplex(OUTPUT);
    let mut peer = Peer {
        client: Client::new(ClientConfig::default()),
        writer: client_writes,
    };
    let counted = handled.clone();
    let mut services = FnHandler(move |_: &str, req: &[u8]| {
        counted.fetch_add(1, Ordering::SeqCst);
        Some(Ok(req.to_vec()))
    });
    let server = tokio::spawn(async move {
        let _ =
            protolink::server_split(FromTokio::new(server_reads), FromTokio::new(server_writes))
                .without_deadlines()
                .build()
                .serve(&mut services)
                .await;
    });

    // The first reply is larger than the output pipe: writing it blocks.
    peer.send(&[7; 2000]).await;
    wait_until(&handled, 1).await;
    assert_eq!(handled.load(Ordering::SeqCst), 1);

    // The next request, while the reply is still blocked, reaches the handler.
    peer.send(&[8; 16]).await;
    wait_until(&handled, 2).await;
    assert_eq!(
        handled.load(Ordering::SeqCst),
        2,
        "a request was not read while the response was blocked"
    );
    server.abort();
}

#[tokio::test]
async fn serial_server_cannot_read_while_its_response_is_blocked() {
    // The control: one pipe served in turn. The reply blocks the server, so the
    // next request waits in the pipe, unread.
    let handled = Arc::new(AtomicUsize::new(0));
    let (client_io, server_io) = tokio::io::duplex(OUTPUT);
    let (_client_reads, client_writes) = tokio::io::split(client_io);
    let mut peer = Peer {
        client: Client::new(ClientConfig::default()),
        writer: client_writes,
    };
    let counted = handled.clone();
    let mut services = FnHandler(move |_: &str, req: &[u8]| {
        counted.fetch_add(1, Ordering::SeqCst);
        Some(Ok(req.to_vec()))
    });
    let server = tokio::spawn(async move {
        let _ = protolink::server(FromTokio::new(server_io))
            .without_deadlines()
            .build()
            .serve(&mut services)
            .await;
    });

    peer.send(&[7; 2000]).await;
    wait_until(&handled, 1).await;
    // The request does not fit in the pipe, and the server does not read it:
    // the peer's write waits, and the request stays unread.
    let _ = tokio::time::timeout(Duration::from_millis(200), peer.send(&[8; 16])).await;
    assert_eq!(
        handled.load(Ordering::SeqCst),
        1,
        "the serial server read while it was blocked writing"
    );
    server.abort();
}

/// A reader that is ready on every poll, with the request bytes queued in
/// chunks. Only the server's own work budget can give the executor a turn.
struct ReadyReader {
    bytes: std::collections::VecDeque<u8>,
}

impl embedded_io_async::ErrorType for ReadyReader {
    type Error = std::convert::Infallible;
}

impl embedded_io_async::Read for ReadyReader {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let n = buf.len().min(64).min(self.bytes.len());
        for slot in &mut buf[..n] {
            *slot = self.bytes.pop_front().unwrap();
        }
        Ok(n)
    }
}

/// A writer that accepts everything at once.
struct ReadySink;

impl embedded_io_async::ErrorType for ReadySink {
    type Error = std::convert::Infallible;
}

impl embedded_io_async::Write for ReadySink {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Counts the wakes it receives.
#[derive(Default)]
struct CountWake(std::sync::atomic::AtomicUsize);

impl std::task::Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Input that is always ready cannot monopolize the executor: the server takes
/// a bounded amount of work per poll, yields, and wakes itself to continue.
#[test]
fn ready_input_obeys_work_budget() {
    let mut client = protolink::grpc::Client::new(protolink::grpc::ClientConfig::default());
    client.start_unary(PATH, &[7; 4000]).unwrap();
    let reader = ReadyReader {
        bytes: client.take_output().into(),
    };
    let mut handler = FnHandler(|_: &str, req: &[u8]| Some(Ok(req.to_vec())));
    let mut serving = std::pin::pin!(
        protolink::server_split(reader, ReadySink)
            .without_deadlines()
            .build()
            .serve(&mut handler)
    );
    let wakes = Arc::new(CountWake::default());
    let waker = std::task::Waker::from(wakes.clone());
    let mut cx = std::task::Context::from_waker(&waker);

    assert!(
        serving.as_mut().poll(&mut cx).is_pending(),
        "one poll drained all the ready input"
    );
    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "the yield did not wake the executor"
    );
    loop {
        if let std::task::Poll::Ready(result) = serving.as_mut().poll(&mut cx) {
            result.unwrap();
            break;
        }
    }
}
