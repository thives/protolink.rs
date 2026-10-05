//! Tokio integration (feature `tokio`).
//!
//! Tokio I/O types are adapted to `embedded_io_async` with
//! [`embedded_io_adapters::tokio_1::FromTokio`], so the same drivers and link
//! stack run on host and embedded targets.
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use protolink::grpc::{FnHandler, ServerConfig};
//!
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:50051").await?;
//! let (stream, _) = listener.accept().await?;
//! let mut handler = FnHandler(|_path: &str, req: &[u8]| Some(Ok(req.to_vec())));
//! protolink::tokio::serve(stream, &mut handler, ServerConfig::default()).await.ok();
//! # Ok(()) }
//! ```
//!
//! [`serve`] and [`client`] enforce deadlines (`grpc-timeout`) with a
//! [`TokioTimer`], so they work with tokio's paused test clock too.

use core::time::Duration;

use ::tokio::io::{AsyncRead, AsyncWrite};
use ::tokio::time::Instant;
pub use embedded_io_adapters::tokio_1::FromTokio;

use crate::grpc::{ClientConfig, Handler, ServerConfig};
use crate::{Client, Clock, Error, Timer};

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

/// [`serve_with_timer`](crate::serve_with_timer) on a tokio stream, with a
/// [`TokioTimer`].
pub async fn serve<T, H>(
    io: T,
    handler: &mut H,
    config: ServerConfig,
) -> Result<(), Error<std::io::Error>>
where
    T: AsyncRead + AsyncWrite + Unpin,
    H: Handler + ?Sized,
{
    crate::serve_with_timer(FromTokio::new(io), handler, config, TokioTimer::new()).await
}

/// A [`Client`] on a tokio stream, with a [`TokioTimer`].
pub fn client<T: AsyncRead + AsyncWrite + Unpin>(
    io: T,
    config: ClientConfig,
) -> Client<FromTokio<T>, TokioTimer> {
    Client::with_timer(FromTokio::new(io), config, TokioTimer::new())
}
