//! Blocking drivers over `embedded_io` (feature `blocking`).
//!
//! Same behaviour as the async [`serve`](crate::serve) and
//! [`Client`](crate::Client), for transports without an executor.

use alloc::vec::Vec;

use embedded_io::{Read, Write};

use crate::grpc::{self, BlockingUnaryTransport, ClientConfig, Handler, ServerConfig, Status};
use crate::{Error, READ_CHUNK};

/// Serve gRPC on one connection until the peer disconnects or the connection fails.
pub fn serve<IO, H>(
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
        io.write_all(server.pending_output()).map_err(Error::Io)?;
        server.consume_output(server.pending_output().len());
        io.flush().map_err(Error::Io)?;
        if server.is_closed() {
            return Ok(());
        }
        let n = io.read(&mut buf).map_err(Error::Io)?;
        if n == 0 {
            return Ok(());
        }
        if let Err(e) = server.recv(&buf[..n], handler) {
            let _ = io.write_all(server.pending_output());
            let _ = io.flush();
            return Err(Error::Protocol(e));
        }
    }
}

/// Blocking unary gRPC client on one connection.
///
/// Implements [`BlockingUnaryTransport`] for generated `<Service>BlockingClient`s.
#[derive(Debug)]
pub struct Client<IO> {
    io: IO,
    inner: grpc::Client,
}

impl<IO: Read + Write> Client<IO> {
    /// Create a client; the HTTP/2 preface is sent with the first call.
    pub fn new(io: IO, config: ClientConfig) -> Self {
        Self {
            io,
            inner: grpc::Client::new(config),
        }
    }

    /// Perform one unary call.
    pub fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        let id = self.inner.start_unary(path, request)?;
        let mut buf = [0u8; READ_CHUNK];
        loop {
            if let Some(result) = self.inner.take_response(id) {
                return result;
            }
            if self.io.write_all(self.inner.pending_output()).is_err() || self.io.flush().is_err() {
                self.inner
                    .fail_all(Status::unavailable("transport write failed"));
                continue;
            }
            self.inner.consume_output(self.inner.pending_output().len());
            match self.io.read(&mut buf) {
                Ok(0) => self
                    .inner
                    .fail_all(Status::unavailable("connection closed")),
                Ok(n) => {
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

impl<IO: Read + Write> BlockingUnaryTransport for Client<IO> {
    fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        Client::unary(self, path, request)
    }
}
