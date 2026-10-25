//! Tokio integration (feature `tokio`).
//!
//! Tokio I/O types are adapted to `embedded_io_async` with
//! [`embedded_io_adapters::tokio_1::FromTokio`], so the same drivers and link
//! stack run on host and embedded targets.
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use protolink::grpc::FnHandler;
//!
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:50051").await?;
//! let (stream, _) = listener.accept().await?;
//! let mut services = FnHandler(|_path: &str, req: &[u8]| Some(Ok(req.to_vec())));
//! let server = protolink::tokio::server(stream).build();
//! server.serve(&mut services).await.ok();
//! # Ok(()) }
//! ```
//!
//! [`server`] and [`client`] are the async factories with a [`TokioTimer`]
//! already chosen as the deadline timer, so they enforce `grpc-timeout` and
//! work with tokio's paused test clock too.

use core::time::Duration;

use ::tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf, split};
use ::tokio::time::Instant;
pub use embedded_io_adapters::tokio_1::FromTokio;

use crate::{ClientBuilder, Clock, Duplex, Serial, ServerBuilder, Timer};

/// A [`Timer`] on tokio's clock.
///
/// Time is measured from the moment the timer was created. It follows
/// `tokio::time::pause` in tests (tokio's `test-util` feature).
#[derive(Debug, Clone, Copy)]
pub struct TokioTimer {
    start: Instant,
}

impl TokioTimer {
    /// A timer whose origin is now.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for TokioTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TokioTimer {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Timer for TokioTimer {
    async fn sleep_until(&self, deadline: Duration) {
        // Keep both Instant arithmetic and Tokio's timer-wheel horizon bounded.
        const CHUNK: Duration = Duration::from_secs(24 * 60 * 60);
        loop {
            let remaining = deadline.saturating_sub(self.now());
            if remaining.is_zero() {
                return;
            }
            let now = Instant::now();
            let wake = now
                .checked_add(remaining.min(CHUNK))
                .expect("a one-day timer interval fits Instant");
            ::tokio::time::sleep_until(wake).await;
        }
    }
}

/// Adapt a tokio stream to `embedded_io_async`.
pub fn compat<T: AsyncRead + AsyncWrite + Unpin>(io: T) -> FromTokio<T> {
    FromTokio::new(io)
}

/// The halves of a tokio stream, adapted to `embedded_io_async`, as a duplex
/// transport.
pub type TokioDuplex<T> = Duplex<FromTokio<ReadHalf<T>>, FromTokio<WriteHalf<T>>>;

/// Adapt a tokio stream to be served in duplex mode: the stream is split into
/// its read and write halves, each adapted to `embedded_io_async`.
///
/// Use it with [`protolink::server_split`](crate::server_split) or
/// [`protolink::client_split`](crate::client_split) when the timer or
/// configuration is chosen separately, or as a router downstream with
/// [`Server::serve_router_split`](crate::Server::serve_router_split).
pub fn duplex<T: AsyncRead + AsyncWrite + Unpin>(io: T) -> TokioDuplex<T> {
    let (reader, writer) = split(io);
    Duplex::new(FromTokio::new(reader), FromTokio::new(writer))
}

/// Start building a server on a tokio stream, with a [`TokioTimer`] as the
/// deadline timer.
///
/// The stream is split into its halves, so that input is read while output is
/// blocked. This is the duplex mode (see [`Mode`](crate::Mode)); the
/// [`ServerBuilder`] is the same as [`crate::server_split`]. The timer can still
/// be replaced with [`with_deadline_timer`](ServerBuilder::with_deadline_timer)
/// or removed with [`without_deadlines`](ServerBuilder::without_deadlines).
pub fn server<T: AsyncRead + AsyncWrite + Unpin>(
    io: T,
) -> ServerBuilder<TokioDuplex<T>, TokioTimer> {
    let (reader, writer) = duplex(io).into_parts();
    crate::server_split(reader, writer).with_deadline_timer(TokioTimer::new())
}

/// Start building a server on a tokio stream served in serial mode: the stream
/// is read and written in turn, without splitting it. Use it when the stream
/// can't be split, or to keep the serial behaviour.
pub fn server_serial<T: AsyncRead + AsyncWrite + Unpin>(
    io: T,
) -> ServerBuilder<Serial<FromTokio<T>>, TokioTimer> {
    crate::server(FromTokio::new(io)).with_deadline_timer(TokioTimer::new())
}

/// Start building a client on a tokio stream, with a [`TokioTimer`] as the
/// deadline timer.
///
/// ```no_run
/// # async fn demo(stream: tokio::net::TcpStream) {
/// use core::time::Duration;
///
/// let transport = protolink::tokio::client(stream)
///     .with_default_timeout(Duration::from_secs(5))
///     .build();
/// // Hand `transport` to a generated `<Service>Client::new`.
/// # let _ = transport;
/// # }
/// ```
///
/// Like [`server`], the client is served in duplex mode.
pub fn client<T: AsyncRead + AsyncWrite + Unpin>(
    io: T,
) -> ClientBuilder<TokioDuplex<T>, TokioTimer> {
    let (reader, writer) = duplex(io).into_parts();
    crate::client_split(reader, writer).with_deadline_timer(TokioTimer::new())
}

/// Start building a client on a tokio stream served in serial mode, see
/// [`server_serial`].
pub fn client_serial<T: AsyncRead + AsyncWrite + Unpin>(
    io: T,
) -> ClientBuilder<Serial<FromTokio<T>>, TokioTimer> {
    crate::client(FromTokio::new(io)).with_deadline_timer(TokioTimer::new())
}
