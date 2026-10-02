//! Blocking drivers over `embedded_io` (feature `blocking`).
//!
//! Same behaviour as the async [`serve`](crate::serve) and
//! [`Client`](crate::Client), for transports without an executor.

use alloc::vec::Vec;
use core::task::{Context, Waker};

use embedded_io::{Error as _, ErrorKind, Read, Write};

use crate::grpc::{
    self, BlockingStreamingCall, BlockingStreamingTransport, BlockingUnaryTransport, CallId,
    ClientConfig, Handler, Next, ServerConfig, Status,
};
use crate::{Error, READ_CHUNK};

/// Serve gRPC on one connection until the peer disconnects or the connection fails.
///
/// Streaming handlers are polled with a no-op waker before every read, so a
/// handler that returns `Poll::Pending` is polled again only once the read
/// returns. To keep such streams moving without client traffic, give the
/// transport a read timeout that surfaces as [`ErrorKind::TimedOut`] (or
/// [`ErrorKind::Interrupted`]): those errors are treated as an idle tick, not
/// a failure. Note that `std` sockets report timeouts as `WouldBlock` on Unix,
/// which `embedded_io` maps to [`ErrorKind::Other`]; map it to `TimedOut` in
/// the transport adapter.
///
/// When the connection ends, every active streaming call is reported to
/// [`Handler::on_cancel`].
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
    let result = serve_connection(&mut io, &mut server, handler);
    server.cancel_all(handler);
    result
}

fn serve_connection<IO, H>(
    io: &mut IO,
    server: &mut grpc::Server,
    handler: &mut H,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
{
    let mut buf = [0u8; READ_CHUNK];
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        // Producing output can make room for more, so poll until idle.
        loop {
            server.poll(&mut *handler, &mut cx);
            if !server.has_output() {
                break;
            }
            io.write_all(server.pending_output()).map_err(Error::Io)?;
            server.consume_output(server.pending_output().len());
        }
        io.flush().map_err(Error::Io)?;
        if server.is_closed() {
            return Ok(());
        }
        let n = match io.read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) if matches!(e.kind(), ErrorKind::TimedOut | ErrorKind::Interrupted) => {
                continue;
            }
            Err(e) => return Err(Error::Io(e)),
        };
        if let Err(e) = server.recv(&buf[..n], handler) {
            let _ = io.write_all(server.pending_output());
            let _ = io.flush();
            return Err(Error::Protocol(e));
        }
    }
}

/// Blocking gRPC client on one connection.
///
/// Implements [`BlockingUnaryTransport`] and [`BlockingStreamingTransport`]
/// for generated `<Service>BlockingClient`s. Calls are issued one at a time:
/// a streaming [`Call`] borrows the client until it is dropped.
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
        loop {
            if let Some(result) = self.inner.take_response(id) {
                return result;
            }
            if self.write_output() {
                self.read_input();
            }
        }
    }

    /// Start a streaming call of `path` (`/package.Service/Method`).
    ///
    /// The request headers are sent with the first operation on the
    /// returned [`Call`]. Dropping the call before it completes cancels it.
    pub fn streaming(&mut self, path: &str) -> Result<Call<'_, IO>, Status> {
        let id = self.inner.start_streaming(path)?;
        Ok(Call {
            client: self,
            id,
            finished: None,
        })
    }

    /// Access the sans-IO client state.
    pub fn inner(&self) -> &grpc::Client {
        &self.inner
    }

    /// Unwrap the transport.
    pub fn into_io(self) -> IO {
        self.io
    }

    /// Write all pending output; `false` (and every call failed) if the
    /// transport failed.
    fn write_output(&mut self) -> bool {
        if self.io.write_all(self.inner.pending_output()).is_err() || self.io.flush().is_err() {
            self.inner
                .fail_all(Status::unavailable("transport write failed"));
            return false;
        }
        self.inner.consume_output(self.inner.pending_output().len());
        true
    }

    fn read_input(&mut self) {
        let mut buf = [0u8; READ_CHUNK];
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

impl<IO: Read + Write> BlockingUnaryTransport for Client<IO> {
    fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        Client::unary(self, path, request)
    }
}

impl<IO: Read + Write> BlockingStreamingTransport for Client<IO> {
    type Call<'a>
        = Call<'a, IO>
    where
        Self: 'a;

    fn start(&mut self, path: &str) -> Result<Self::Call<'_>, Status> {
        self.streaming(path)
    }
}

/// An active streaming call on a blocking [`Client`].
///
/// See the async [`Call`](crate::Call) for the semantics. Dropping the call
/// before [`message`](Self::message) reported the end cancels it.
#[derive(Debug)]
pub struct Call<'a, IO> {
    client: &'a mut Client<IO>,
    id: CallId,
    finished: Option<Result<(), Status>>,
}

impl<IO: Read + Write> Call<'_, IO> {
    /// The call's id on the connection.
    pub fn id(&self) -> CallId {
        self.id
    }

    /// Send one request message, blocking while earlier messages still wait
    /// for the server's flow-control window.
    pub fn send(&mut self, message: &[u8]) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let id = self.id;
        while !self.client.inner.can_send(id) {
            if self.client.write_output() && !self.client.inner.can_send(id) {
                self.client.read_input();
            }
        }
        if !self.client.inner.is_pending(id) {
            return Ok(());
        }
        self.client.inner.send_message(id, message)?;
        self.client.write_output();
        Ok(())
    }

    /// Half-close: no more request messages. Responses keep flowing.
    pub fn close_send(&mut self) -> Result<(), Status> {
        if self.finished.is_some() || !self.client.inner.is_pending(self.id) {
            return Ok(());
        }
        self.client.inner.close_send(self.id)?;
        self.client.write_output();
        Ok(())
    }

    /// Next response message; `Ok(None)` once the call completed with
    /// `grpc-status: 0`, `Err` if it failed.
    pub fn message(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if let Some(result) = &self.finished {
            return result.clone().map(|()| None);
        }
        let id = self.id;
        loop {
            match self.client.inner.try_next(id) {
                Some(Next::Message(m)) => return Ok(Some(m)),
                Some(Next::Done(result)) => {
                    self.finished = Some(result.clone());
                    return result.map(|()| None);
                }
                None if !self.client.inner.is_pending(id) => {
                    let status = Status::cancelled("call is not active");
                    self.finished = Some(Err(status.clone()));
                    return Err(status);
                }
                None => {}
            }
            if self.client.write_output() && self.client.inner.is_pending(id) {
                self.client.read_input();
            }
        }
    }
}

impl<IO> Drop for Call<'_, IO> {
    fn drop(&mut self) {
        if self.finished.is_none() {
            self.client.inner.cancel(self.id);
        }
    }
}

impl<IO: Read + Write> BlockingStreamingCall for Call<'_, IO> {
    fn send(&mut self, message: &[u8]) -> Result<(), Status> {
        Call::send(self, message)
    }

    fn close_send(&mut self) -> Result<(), Status> {
        Call::close_send(self)
    }

    fn message(&mut self) -> Result<Option<Vec<u8>>, Status> {
        Call::message(self)
    }
}
