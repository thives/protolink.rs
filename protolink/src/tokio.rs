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

use ::tokio::io::{AsyncRead, AsyncWrite};
use ::tokio::time::Instant;
pub use embedded_io_adapters::tokio_1::FromTokio;

use crate::{ClientBuilder, Clock, ServerBuilder, Timer};

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

/// Start building a server on a tokio stream, with a [`TokioTimer`] as the
/// deadline timer.
///
/// The same [`ServerBuilder`] as [`crate::server`]; the timer can still be
/// replaced with [`with_deadline_timer`](ServerBuilder::with_deadline_timer)
/// or removed with [`without_deadlines`](ServerBuilder::without_deadlines).
pub fn server<T: AsyncRead + AsyncWrite + Unpin>(io: T) -> ServerBuilder<FromTokio<T>, TokioTimer> {
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
pub fn client<T: AsyncRead + AsyncWrite + Unpin>(io: T) -> ClientBuilder<FromTokio<T>, TokioTimer> {
    crate::client(FromTokio::new(io)).with_deadline_timer(TokioTimer::new())
}
