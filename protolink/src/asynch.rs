//! Async drivers over `embedded_io_async`.

use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Poll, Waker};

use embedded_io_async::{Read, Write};

use crate::grpc::{
    self, CallId, ClientConfig, Handler, Next, ServerConfig, Status, StreamingCall,
    StreamingTransport, UnaryTransport,
};
use crate::shared::Shared;
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
/// straight into generated `<Service>Client`s.
///
/// One streaming [`Call`] can be active at a time: starting another one while
/// a call still exists fails with `FAILED_PRECONDITION`. Unary calls take
/// `&mut self`, so they can't run while a [`Call`] exists. Use the sans-IO
/// [`grpc::Client`] directly to run calls concurrently on one connection.
///
/// The transport is only checked out of the client while an operation uses
/// it, and is returned when that operation completes or its future is
/// dropped.
#[derive(Debug)]
pub struct Client<IO> {
    shared: Shared<State<IO>>,
}

#[derive(Debug)]
struct State<IO> {
    inner: grpc::Client,
    /// The transport, or `None` while an operation has it checked out.
    io: Option<IO>,
    /// Tasks waiting for the transport to be checked back in.
    waiters: Vec<Waker>,
    /// A unary call whose future was dropped before it completed.
    stale_unary: Option<CallId>,
    /// Number of existing [`Call`]s.
    active_calls: usize,
}

/// The transport, checked out of a [`Client`]. Dropping the guard checks it
/// back in and wakes the tasks waiting for it.
struct IoGuard<'a, IO> {
    client: &'a Client<IO>,
    io: Option<IO>,
}

impl<IO> IoGuard<'_, IO> {
    fn io(&mut self) -> &mut IO {
        self.io
            .as_mut()
            .expect("the transport is held until the guard is dropped")
    }
}

impl<IO> Drop for IoGuard<'_, IO> {
    fn drop(&mut self) {
        let io = self.io.take();
        let waiters = self.client.shared.with(|s| {
            s.io = io;
            core::mem::take(&mut s.waiters)
        });
        for waiter in waiters {
            waiter.wake();
        }
    }
}

impl<IO: Read + Write> Client<IO> {
    /// Create a client; the HTTP/2 preface is sent with the first call.
    pub fn new(io: IO, config: ClientConfig) -> Self {
        Self {
            shared: Shared::new(State {
                inner: grpc::Client::new(config),
                io: Some(io),
                waiters: Vec::new(),
                stale_unary: None,
                active_calls: 0,
            }),
        }
    }

    /// Perform one unary call.
    pub async fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        let id = self.shared.with(|s| {
            cancel_stale_unary(s);
            let id = s.inner.start_unary(path, request)?;
            s.stale_unary = Some(id);
            Ok::<_, Status>(id)
        })?;
        let result = self
            .drive(|c| match c.take_response(id) {
                Some(result) => Some(result),
                None if c.is_pending(id) => None,
                None => Some(Err(Status::cancelled("call is not active"))),
            })
            .await;
        self.shared.with(|s| s.stale_unary = None);
        result
    }

    /// Start a streaming call of `path` (`/package.Service/Method`).
    ///
    /// The request headers are sent with the first operation on the
    /// returned [`Call`]. Dropping the call before it completes cancels it.
    /// Fails with `FAILED_PRECONDITION` while another [`Call`] exists.
    pub fn streaming(&self, path: &str) -> Result<Call<'_, IO>, Status> {
        let id = self.shared.with(|s| {
            if s.active_calls > 0 {
                return Err(Status::failed_precondition(
                    "another streaming call is active on this client",
                ));
            }
            cancel_stale_unary(s);
            let id = s.inner.start_streaming(path)?;
            s.active_calls += 1;
            Ok(id)
        })?;
        Ok(Call {
            client: self,
            id,
            finished: None,
        })
    }

    /// Run `f` with the sans-IO client state.
    ///
    /// `f` must not use this client or drop its [`Call`]s.
    pub fn with_inner<R>(&self, f: impl FnOnce(&grpc::Client) -> R) -> R {
        self.shared.with(|s| f(&s.inner))
    }

    /// Unwrap the transport.
    pub fn into_io(self) -> IO {
        self.shared
            .into_inner()
            .io
            .expect("the transport is checked in when no operation is running")
    }

    /// Check the transport out, waiting while another operation has it.
    /// `done` is evaluated first, and whenever the client state may have
    /// changed; if it returns a value, that is returned instead of the
    /// transport.
    async fn acquire<T>(
        &self,
        done: &mut impl FnMut(&mut grpc::Client) -> Option<T>,
    ) -> Result<IoGuard<'_, IO>, T> {
        let acquired = poll_fn(|cx| {
            self.shared.with(|s| {
                if let Some(value) = done(&mut s.inner) {
                    return Poll::Ready(Err(value));
                }
                match s.io.take() {
                    Some(io) => Poll::Ready(Ok(io)),
                    None => {
                        if !s.waiters.iter().any(|w| w.will_wake(cx.waker())) {
                            s.waiters.push(cx.waker().clone());
                        }
                        Poll::Pending
                    }
                }
            })
        })
        .await;
        acquired.map(|io| IoGuard {
            client: self,
            io: Some(io),
        })
    }

    /// Move the connection along (write pending output, read input) until
    /// `step` returns a value.
    async fn drive<T>(&self, mut step: impl FnMut(&mut grpc::Client) -> Option<T>) -> T {
        loop {
            let mut guard = match self.acquire(&mut step).await {
                Ok(guard) => guard,
                Err(value) => return value,
            };
            if self.write_all(guard.io()).await {
                if let Some(value) = self.shared.with(|s| step(&mut s.inner)) {
                    return value;
                }
                self.read_once(guard.io()).await;
            }
        }
    }

    /// Write all pending output.
    async fn flush(&self) {
        let mut done = |c: &mut grpc::Client| (!c.has_output()).then_some(());
        if let Ok(mut guard) = self.acquire(&mut done).await {
            self.write_all(guard.io()).await;
        }
    }

    /// Write all pending output and flush the transport. Transport failures
    /// fail every call and return `false`.
    ///
    /// Output is copied out in chunks and only consumed once the transport
    /// accepted it, so no borrow of the client state is held while the
    /// transport is awaited.
    async fn write_all(&self, io: &mut IO) -> bool {
        let mut buf = [0u8; READ_CHUNK];
        loop {
            let n = self.shared.with(|s| {
                let pending = s.inner.pending_output();
                let n = pending.len().min(buf.len());
                buf[..n].copy_from_slice(&pending[..n]);
                n
            });
            if n == 0 {
                break;
            }
            match io.write(&buf[..n]).await {
                Ok(0) | Err(_) => {
                    self.fail_all("transport write failed");
                    return false;
                }
                Ok(written) => self.shared.with(|s| s.inner.consume_output(written)),
            }
        }
        if io.flush().await.is_err() {
            self.fail_all("transport flush failed");
            return false;
        }
        true
    }

    /// Read once from the transport. Transport failures fail every call.
    async fn read_once(&self, io: &mut IO) {
        let mut buf = [0u8; READ_CHUNK];
        let result = io.read(&mut buf).await;
        self.shared.with(|s| match result {
            Ok(0) => s.inner.fail_all(Status::unavailable("connection closed")),
            Ok(n) => {
                // Errors fail all pending calls inside the client.
                let _ = s.inner.recv(&buf[..n]);
            }
            Err(_) => s
                .inner
                .fail_all(Status::unavailable("transport read failed")),
        });
    }

    fn fail_all(&self, message: &'static str) {
        self.shared
            .with(|s| s.inner.fail_all(Status::unavailable(message)));
    }
}

/// Cancel a unary call whose future was dropped.
fn cancel_stale_unary<IO>(state: &mut State<IO>) {
    if let Some(stale) = state.stale_unary.take() {
        state.inner.cancel(stale);
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

    fn start(&self, path: &str) -> impl Future<Output = Result<Self::Call<'_>, Status>> {
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
    client: &'a Client<IO>,
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
        let client = self.client;
        let id = self.id;
        client
            .drive(|c| (c.can_send(id) || !c.is_pending(id)).then_some(()))
            .await;
        let queued = client.shared.with(|s| {
            if !s.inner.is_pending(id) {
                // Finished while waiting; the outcome is reported by `message`.
                return Ok(false);
            }
            s.inner.send_message(id, message).map(|()| true)
        })?;
        if queued {
            client.flush().await;
        }
        Ok(())
    }

    /// Half-close: no more request messages. Responses keep flowing.
    pub async fn close_send(&mut self) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let client = self.client;
        let id = self.id;
        let queued = client.shared.with(|s| {
            if !s.inner.is_pending(id) {
                return Ok(false);
            }
            s.inner.close_send(id).map(|()| true)
        })?;
        if queued {
            client.flush().await;
        }
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
        let next = self
            .client
            .drive(|c| match c.try_next(id) {
                Some(next) => Some(next),
                None if !c.is_pending(id) => {
                    Some(Next::Done(Err(Status::cancelled("call is not active"))))
                }
                None => None,
            })
            .await;
        match next {
            Next::Message(m) => Ok(Some(m)),
            Next::Done(result) => {
                self.finished = Some(result.clone());
                result.map(|()| None)
            }
        }
    }
}

impl<IO> Drop for Call<'_, IO> {
    fn drop(&mut self) {
        let id = self.id;
        let unfinished = self.finished.is_none();
        self.client.shared.with(|s| {
            if unfinished {
                s.inner.cancel(id);
            }
            s.active_calls = s.active_calls.saturating_sub(1);
        });
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
