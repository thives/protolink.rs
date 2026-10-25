//! Finite transfers over the coordinated reliable mode: a client and a server,
//! each on a reliable link (ARQ over COBS) over a tokio pipe. Each direction
//! carries more data than the link's receive storage, so the transfer needs
//! the acknowledgements and the window to move along.
#![cfg(all(feature = "tokio", feature = "std"))]

use protolink::grpc::FnHandler;
use protolink::tokio::FromTokio;

const ECHO: &str = "/test.Reliable/Echo";
/// Bytes per call in each direction.
const CHUNK: usize = 4096;
/// Calls per transfer: 64 KiB each way.
const CALLS: usize = 16;

#[tokio::test]
async fn reliable_client_and_server_transfer_more_than_the_link_buffers() {
    let (client_side, server_side) = tokio::io::duplex(1024);
    let server_link = protolink::link::reliable(FromTokio::new(server_side)).build();
    let client_link = protolink::link::reliable(FromTokio::new(client_side)).build();

    let server = tokio::spawn(async move {
        let mut services = FnHandler(|path: &str, request: &[u8]| {
            assert_eq!(path, ECHO);
            Some(Ok(request.to_vec()))
        });
        protolink::server_reliable(server_link)
            .without_deadlines()
            .build()
            .serve(&mut services)
            .await
    });

    let mut client = protolink::client_reliable(client_link)
        .without_deadlines()
        .build();
    for call in 0..CALLS {
        let payload: Vec<u8> = (0..CHUNK).map(|i| (i + call) as u8).collect();
        let reply = client
            .unary(ECHO, &payload)
            .await
            .expect("the call completes");
        assert_eq!(reply, payload, "call {call} echoed its payload");
    }
    drop(client);
    let _ = server.await;
}

/// A transport error that is not `Clone`: the engine must pass the original on
/// by ownership.
#[derive(Debug)]
struct Broken(u8);

impl std::fmt::Display for Broken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "broken transport {}", self.0)
    }
}

impl std::error::Error for Broken {}

impl embedded_io_async::Error for Broken {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        embedded_io_async::ErrorKind::Other
    }
}

/// A retransmission timer that never fires.
struct Idle;

impl protolink::link::Timer for Idle {
    fn start(&mut self, _: std::time::Duration) {}

    fn stop(&mut self) {}

    fn poll_expired(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        std::task::Poll::Pending
    }
}

/// A transport whose reads fail at once, and whose writes are accepted.
struct FailingReads;

impl embedded_io_async::ErrorType for FailingReads {
    type Error = Broken;
}

impl embedded_io_async::Read for FailingReads {
    async fn read(&mut self, _: &mut [u8]) -> Result<usize, Broken> {
        Err(Broken(7))
    }
}

impl embedded_io_async::Write for FailingReads {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Broken> {
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Broken> {
        Ok(())
    }
}

/// The handshake is queued when the read fails. The engine reports the read's
/// error once and `Closed` afterwards, so the worker must not poll it for the
/// write: the caller sees the original cause.
#[tokio::test]
async fn reliable_rx_error_is_not_replaced_by_closed() {
    let link = protolink::link::reliable(FailingReads)
        .with_retransmission_timer(Idle)
        .build();
    let result = protolink::server_reliable(link)
        .without_deadlines()
        .build()
        .serve(&mut FnHandler(|_: &str, request: &[u8]| {
            Some(Ok(request.to_vec()))
        }))
        .await;
    assert!(
        matches!(
            result,
            Err(protolink::Error::Io(protolink::link::ReliableError(
                protolink::link::ArqError::Io(protolink::link::LinkError::Io(Broken(7)))
            )))
        ),
        "{result:?}"
    );
}

/// A transport at the end of its stream, whose writes are accepted.
struct EndOfStream;

impl embedded_io_async::ErrorType for EndOfStream {
    type Error = Broken;
}

impl embedded_io_async::Read for EndOfStream {
    async fn read(&mut self, _: &mut [u8]) -> Result<usize, Broken> {
        Ok(0)
    }
}

impl embedded_io_async::Write for EndOfStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Broken> {
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Broken> {
        Ok(())
    }
}

/// The end of the raw stream is reported as `Closed`, as the link documents.
/// This is the control for the transport errors above: it is not replaced by
/// anything else.
#[tokio::test]
async fn reliable_end_of_stream_is_reported_as_closed() {
    let link = protolink::link::reliable(EndOfStream)
        .with_retransmission_timer(Idle)
        .build();
    let result = protolink::server_reliable(link)
        .without_deadlines()
        .build()
        .serve(&mut FnHandler(|_: &str, request: &[u8]| {
            Some(Ok(request.to_vec()))
        }))
        .await;
    assert!(
        matches!(
            result,
            Err(protolink::Error::Io(protolink::link::ReliableError(
                protolink::link::ArqError::Closed
            )))
        ),
        "{result:?}"
    );
}
