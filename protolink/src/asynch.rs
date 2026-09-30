//! Async drivers over `embedded_io_async`.

use alloc::vec::Vec;

use embedded_io_async::{Read, Write};

use crate::grpc::{self, CallId, ClientConfig, Handler, ServerConfig, Status, UnaryTransport};
use crate::{Error, READ_CHUNK};

/// Serve gRPC on one connection until the peer disconnects or the connection
/// fails.
///
/// Requests are dispatched to `handler` (e.g. a generated `<Service>Server`, or
/// a tuple of them). Writes are cancel-safe if the transport's `write` is, so
/// the future may be dropped (e.g. on shutdown) without corrupting state.
pub async fn serve<IO, H>(
    mut io: IO,
    handler: &mut H,
    config: ServerConfig,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
{
    let mut server = grpc::Server::new(config);
    let mut buf = [0u8; READ_CHUNK];
    loop {
        while server.has_output() {
            let n = io.write(server.pending_output()).await.map_err(Error::Io)?;
            server.consume_output(n);
        }
        io.flush().await.map_err(Error::Io)?;
        if server.is_closed() {
            return Ok(());
        }
        let n = io.read(&mut buf).await.map_err(Error::Io)?;
        if n == 0 {
            return Ok(());
        }
        if let Err(e) = server.recv(&buf[..n], handler) {
            // Best effort: deliver the GOAWAY before giving up.
            let _ = io.write_all(server.pending_output()).await;
            let _ = io.flush().await;
            return Err(Error::Protocol(e));
        }
    }
}

/// Async unary gRPC client on one connection.
///
/// Implements [`UnaryTransport`], so it plugs straight into generated
/// `<Service>Client`s. Calls are issued one at a time.
#[derive(Debug)]
pub struct Client<IO> {
    io: IO,
    inner: grpc::Client,
    current: Option<CallId>,
}

impl<IO: Read + Write> Client<IO> {
    /// Create a client; the HTTP/2 preface is sent with the first call.
    pub fn new(io: IO, config: ClientConfig) -> Self {
        Self {
            io,
            inner: grpc::Client::new(config),
            current: None,
        }
    }

    /// Perform one unary call.
    pub async fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        // A previous call whose future was dropped is cancelled.
        if let Some(stale) = self.current.take() {
            self.inner.cancel(stale);
        }
        let id = self.inner.start_unary(path, request)?;
        self.current = Some(id);
        let mut buf = [0u8; READ_CHUNK];
        loop {
            if let Some(result) = self.inner.take_response(id) {
                self.current = None;
                return result;
            }
            while self.inner.has_output() {
                match self.io.write(self.inner.pending_output()).await {
                    Ok(0) | Err(_) => {
                        self.inner
                            .fail_all(Status::unavailable("transport write failed"));
                        break;
                    }
                    Ok(n) => self.inner.consume_output(n),
                }
            }
            if self.inner.is_pending(id) && self.io.flush().await.is_err() {
                self.inner
                    .fail_all(Status::unavailable("transport flush failed"));
            }
            if !self.inner.is_pending(id) {
                continue;
            }
            match self.io.read(&mut buf).await {
                Ok(0) => self
                    .inner
                    .fail_all(Status::unavailable("connection closed")),
                Ok(n) => {
                    // Errors fail all pending calls inside the client.
                    let _ = self.inner.recv(&buf[..n]);
                }
                Err(_) => self
                    .inner
                    .fail_all(Status::unavailable("transport read failed")),
            }
        }
    }

    /// Access the sans-IO client state.
    pub fn inner(&self) -> &grpc::Client {
        &self.inner
    }

    /// Unwrap the transport.
    pub fn into_io(self) -> IO {
        self.io
    }
}

impl<IO: Read + Write> UnaryTransport for Client<IO> {
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, Status>> {
        Client::unary(self, path, request)
    }
}
