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

use ::tokio::io::{AsyncRead, AsyncWrite};
pub use embedded_io_adapters::tokio_1::FromTokio;

use crate::grpc::{ClientConfig, Handler, ServerConfig};
use crate::{Client, Error};

/// Adapt a tokio stream to `embedded_io_async`.
pub fn compat<T: AsyncRead + AsyncWrite + Unpin>(io: T) -> FromTokio<T> {
    FromTokio::new(io)
}

/// [`serve`](crate::serve) on a tokio stream.
pub async fn serve<T, H>(
    io: T,
    handler: &mut H,
    config: ServerConfig,
) -> Result<(), Error<std::io::Error>>
where
    T: AsyncRead + AsyncWrite + Unpin,
    H: Handler + ?Sized,
{
    crate::serve(FromTokio::new(io), handler, config).await
}

/// A [`Client`] on a tokio stream.
pub fn client<T: AsyncRead + AsyncWrite + Unpin>(
    io: T,
    config: ClientConfig,
) -> Client<FromTokio<T>> {
    Client::new(FromTokio::new(io), config)
}
