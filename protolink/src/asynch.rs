//! Async drivers over `embedded_io_async`.

use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;

use embedded_io_async::{Read, Write};

use crate::grpc::{
    self, CallId, ClientConfig, Handler, Next, ServerConfig, Status, StreamingCall,
    StreamingTransport, UnaryTransport,
};
use crate::{Error, READ_CHUNK};

/// Serve gRPC on one connection until the peer disconnects or the connection
/// fails.
///
/// Requests are dispatched to `handler` (e.g. a generated `<Service>Server`, or
/// a tuple of them). Writes are cancel-safe if the transport's `write` is, so
/// the future may be dropped (e.g. on shutdown) without corrupting state.
///
/// Streaming handlers are polled with this future's waker: when one returns
/// `Poll::Pending` and later wakes the waker, the pending transport `read` is
/// dropped so the new responses can be written. Serving streaming methods
/// whose handlers return `Pending` therefore requires a cancel-safe `read`
/// (as tokio streams, [`CobsFramed`](crate::link::CobsFramed) and the
/// [`link`](crate::link) stack are); unary-only servers never drop a read.
///
/// When the connection ends, every active streaming call is reported to
/// [`Handler::on_cancel`].
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
    let result = serve_connection(&mut io, &mut server, handler).await;
    server.cancel_all(handler);
    result
}

async fn serve_connection<IO, H>(
    io: &mut IO,
    server: &mut grpc::Server,
    handler: &mut H,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
{
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
        // Wait for input, or for a streaming handler to produce output.
        let read = {
            let mut read = pin!(io.read(&mut buf));
            poll_fn(|cx| {
                server.poll(&mut *handler, cx);
                if server.has_output() {
                    return Poll::Ready(None);
                }
                read.as_mut().poll(cx).map(Some)
            })
            .await
        };
        let Some(read) = read else {
            continue;
        };
        let n = read.map_err(Error::Io)?;
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

/// Async gRPC client on one connection.
///
/// Implements [`UnaryTransport`] and [`StreamingTransport`], so it plugs
/// straight into generated `<Service>Client`s. Calls are issued one at a time:
/// a streaming [`Call`] borrows the client until it is dropped. Use the
/// sans-IO [`grpc::Client`] directly to run calls concurrently on one
/// connection.
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
        loop {
            if let Some(result) = self.inner.take_response(id) {
                self.current = None;
                return result;
            }
            self.write_output().await;
            if self.inner.is_pending(id) {
                self.read_input().await;
            }
        }
    }

    /// Start a streaming call of `path` (`/package.Service/Method`).
    ///
    /// The request headers are sent with the first operation on the
    /// returned [`Call`]. Dropping the call before it completes cancels it.
    pub fn streaming(&mut self, path: &str) -> Result<Call<'_, IO>, Status> {
        if let Some(stale) = self.current.take() {
            self.inner.cancel(stale);
        }
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

    /// Write all pending output. Transport failures fail every call.
    async fn write_output(&mut self) {
        while self.inner.has_output() {
            match self.io.write(self.inner.pending_output()).await {
                Ok(0) | Err(_) => {
                    self.inner
                        .fail_all(Status::unavailable("transport write failed"));
                    return;
                }
                Ok(n) => self.inner.consume_output(n),
            }
        }
        if self.io.flush().await.is_err() {
            self.inner
                .fail_all(Status::unavailable("transport flush failed"));
        }
    }

    /// Read once from the transport. Transport failures fail every call.
    async fn read_input(&mut self) {
        let mut buf = [0u8; READ_CHUNK];
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

impl<IO: Read + Write> UnaryTransport for Client<IO> {
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, Status>> {
        Client::unary(self, path, request)
    }
}

impl<IO: Read + Write> StreamingTransport for Client<IO> {
    type Call<'a>
        = Call<'a, IO>
    where
        Self: 'a;

    fn start(&mut self, path: &str) -> impl Future<Output = Result<Self::Call<'_>, Status>> {
        core::future::ready(self.streaming(path))
    }
}

/// An active streaming call on an async [`Client`].
///
/// Requests are sent with [`send`](Self::send) and
/// [`close_send`](Self::close_send); responses are read with
/// [`message`](Self::message). While waiting to send, received responses are
/// buffered up to the call's flow-control window, after which the server is
/// stalled; read them to let it continue. Dropping the call before
/// `message` reported the end cancels it (RST_STREAM CANCEL).
///
/// The drivers do not read while a transport write is blocked. On a
/// transport that buffers less than the data in flight (a small pipe or
/// UART buffer without a reader task), a bidirectional call that sends many
/// requests without reading the responses can block both peers in `write`.
/// Interleave `message` with `send`, or give the transport enough buffering.
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

    /// Send one request message, waiting while earlier messages still wait
    /// for the server's flow-control window. Messages sent after the server
    /// finished the call are discarded.
    pub async fn send(&mut self, message: &[u8]) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let id = self.id;
        while !self.client.inner.can_send(id) {
            self.client.write_output().await;
            if self.client.inner.can_send(id) {
                break;
            }
            self.client.read_input().await;
        }
        if !self.client.inner.is_pending(id) {
            // Finished while waiting; the outcome is reported by `message`.
            return Ok(());
        }
        self.client.inner.send_message(id, message)?;
        self.client.write_output().await;
        Ok(())
    }

    /// Half-close: no more request messages. Responses keep flowing.
    pub async fn close_send(&mut self) -> Result<(), Status> {
        if self.finished.is_some() || !self.client.inner.is_pending(self.id) {
            return Ok(());
        }
        self.client.inner.close_send(self.id)?;
        self.client.write_output().await;
        Ok(())
    }

    /// Next response message; `Ok(None)` once the call completed with
    /// `grpc-status: 0`, `Err` if it failed. Messages received before a
    /// failure are returned first; the outcome is repeated on later calls.
    pub async fn message(&mut self) -> Result<Option<Vec<u8>>, Status> {
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
            self.client.write_output().await;
            if self.client.inner.is_pending(id) {
                self.client.read_input().await;
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

impl<IO: Read + Write> StreamingCall for Call<'_, IO> {
    fn send(&mut self, message: &[u8]) -> impl Future<Output = Result<(), Status>> {
        Call::send(self, message)
    }

    fn close_send(&mut self) -> impl Future<Output = Result<(), Status>> {
        Call::close_send(self)
    }

    fn message(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, Status>> {
        Call::message(self)
    }
}
