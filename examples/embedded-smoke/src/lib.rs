//! Feature-isolated no_std consumer of generated messages, server and clients.
#![no_std]

extern crate alloc;

use core::hint::black_box;
use core::task::{Context, Poll};
use protolink::{CallContext, Next, Status};

pub mod proto {
    #![allow(
        clippy::all,
        missing_docs,
        non_snake_case,
        non_camel_case_types,
        unused,
        unused_parens
    )]
    include!(concat!(env!("OUT_DIR"), "/smoke.rs"));
    include!(concat!(env!("OUT_DIR"), "/smoke_grpc.rs"));
}

use proto::embedded_::smoke_::Payload;
use proto::smoke::{Smoke, SmokeServer};

struct Echo;

impl Smoke for Echo {
    fn unary(&mut self, _ctx: &mut CallContext<'_>, request: Payload) -> Result<Payload, Status> {
        Ok(request)
    }

    fn server_stream(
        &mut self,
        _ctx: &mut CallContext<'_>,
        _request: Payload,
    ) -> Result<(), Status> {
        Ok(())
    }

    fn poll_server_stream(
        &mut self,
        _ctx: &mut CallContext<'_>,
        _cx: &mut Context<'_>,
    ) -> Poll<Next<Payload>> {
        Poll::Ready(Next::Done(Ok(())))
    }

    fn client_stream(
        &mut self,
        _ctx: &mut CallContext<'_>,
        _request: Payload,
    ) -> Result<(), Status> {
        Ok(())
    }

    fn poll_client_stream(
        &mut self,
        _ctx: &mut CallContext<'_>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Payload, Status>> {
        Poll::Ready(Ok(Payload::default()))
    }

    fn bidi(&mut self, _ctx: &mut CallContext<'_>, _request: Payload) -> Result<(), Status> {
        Ok(())
    }

    fn end_bidi(&mut self, _ctx: &mut CallContext<'_>) -> Result<(), Status> {
        Ok(())
    }

    fn poll_bidi(
        &mut self,
        _ctx: &mut CallContext<'_>,
        _cx: &mut Context<'_>,
    ) -> Poll<Next<Payload>> {
        Poll::Ready(Next::Done(Ok(())))
    }
}

// EOF/error-only I/O: retain concrete driver paths, without pretending to be a
// UART or a peer. black_box prevents constant-folding away their link coverage.
struct SmokeIo {
    eof: bool,
}

impl SmokeIo {
    fn new() -> Self {
        black_box(Self { eof: true })
    }

    fn read_result(&self) -> Result<usize, embedded_io::ErrorKind> {
        if self.eof {
            Ok(0)
        } else {
            Err(embedded_io::ErrorKind::Other)
        }
    }
}

impl embedded_io::ErrorType for SmokeIo {
    type Error = embedded_io::ErrorKind;
}

#[cfg(feature = "blocking")]
impl embedded_io::Read for SmokeIo {
    fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.read_result()
    }
}

#[cfg(feature = "blocking")]
impl embedded_io::Write for SmokeIo {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(feature = "async")]
impl embedded_io_async::Read for SmokeIo {
    async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.read_result()
    }
}

#[cfg(feature = "async")]
impl embedded_io_async::Write for SmokeIo {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn request() -> Payload {
    Payload {
        value: black_box(7),
        label: "no_std".into(),
        samples: alloc::vec![1, 2, 3],
    }
}

/// Monomorphize all generated blocking RPC shapes and the server driver.
#[cfg(feature = "blocking")]
pub fn blocking_smoke() {
    use proto::smoke::SmokeBlockingClient;
    let request = request();
    let mut client = SmokeBlockingClient::new(protolink::blocking::Client::new(
        SmokeIo::new(),
        black_box(protolink::ClientConfig::default()),
    ));
    let _ = black_box(client.unary(&request));
    if let Ok(mut stream) = client.server_stream(&request) {
        let _ = black_box(stream.message());
    }
    if let Ok(mut stream) = client.client_stream() {
        let _ = black_box(stream.send(&request));
        let _ = black_box(stream.finish());
    }
    if let Ok(mut stream) = client.bidi() {
        let _ = black_box(stream.send(&request));
        let _ = black_box(stream.close_send());
        let _ = black_box(stream.message());
    }
    let mut handler = SmokeServer::new(Echo);
    let _ = black_box(protolink::blocking::serve(
        SmokeIo::new(),
        &mut handler,
        black_box(protolink::ServerConfig::default()),
    ));
}

/// Monomorphize all generated async RPC shapes, server, and CAS-backed wakers.
#[cfg(feature = "async")]
pub async fn async_smoke() {
    use proto::smoke::SmokeClient;
    use protolink::link::ring::RingBuffer;

    let mut ring = RingBuffer::<u8, 4>::new();
    let (mut tx, mut rx) = ring.split();
    let mut cx = Context::from_waker(core::task::Waker::noop());
    let _ = black_box(rx.poll_readable(&mut cx));
    tx.push(black_box(7)).await;
    black_box(rx.pop().await);

    let request = request();
    let mut client = SmokeClient::new(protolink::Client::new(
        SmokeIo::new(),
        black_box(protolink::ClientConfig::default()),
    ));
    let _ = black_box(client.unary(&request).await);
    if let Ok(mut stream) = client.server_stream(&request).await {
        let _ = black_box(stream.message().await);
    }
    if let Ok(mut stream) = client.client_stream().await {
        let _ = black_box(stream.send(&request).await);
        let _ = black_box(stream.finish().await);
    }
    if let Ok(mut stream) = client.bidi().await {
        let _ = black_box(stream.send(&request).await);
        let _ = black_box(stream.close_send().await);
        let _ = black_box(stream.message().await);
    }
    let mut handler = SmokeServer::new(Echo);
    let _ = black_box(
        protolink::serve(
            SmokeIo::new(),
            &mut handler,
            black_box(protolink::ServerConfig::default()),
        )
        .await,
    );
}

/// Keep the optional gzip backend in the linked image without a real peer.
#[cfg(feature = "compression")]
pub fn compression_smoke() {
    use protolink::compression::{Codec, GZIP};
    let mut out = alloc::vec::Vec::new();
    // A single empty gzip member; exercise decoding, not the large compressor.
    let input = [
        0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    let _ = black_box(GZIP.decompress(black_box(&input), &mut out, 64));
    black_box(out);
}
