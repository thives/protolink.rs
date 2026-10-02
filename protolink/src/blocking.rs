//! Blocking drivers over `embedded_io` (feature `blocking`).
//!
//! Same behaviour as the async [`serve`](crate::serve) and
//! [`Client`](crate::Client), for transports without an executor.

use alloc::vec::Vec;
use core::cell::RefCell;
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
/// A transport that can wait for input *or* a wake-up avoids the timeout: see
/// [`WakeableRead`] and [`serve_wakeable`].
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
    let mut cx = Context::from_waker(Waker::noop());
    let result = serve_connection(&mut io, &mut server, handler, &mut cx, |io, buf| {
        io.read(buf).map(Some)
    });
    server.cancel_all(handler);
    result
}

/// A handle that wakes a [`WakeableRead`] transport.
///
/// [`serve_wakeable`] turns it into the [`Waker`] handlers receive in
/// `poll_response`. Calling [`wake`](Self::wake) must make the transport's
/// in-progress [`read_or_wake`](WakeableRead::read_or_wake) return `Ok(None)`,
/// or the next one if none is in progress. It may be called from any thread
/// or from an interrupt handler, so it must not block.
#[cfg(target_has_atomic = "ptr")]
#[derive(Clone)]
pub struct WakeHandle(alloc::sync::Arc<dyn Fn() + Send + Sync>);

#[cfg(target_has_atomic = "ptr")]
impl WakeHandle {
    /// Create a handle that calls `wake`.
    pub fn new(wake: impl Fn() + Send + Sync + 'static) -> Self {
        Self(alloc::sync::Arc::new(wake))
    }

    /// Wake the transport.
    pub fn wake(&self) {
        (self.0)();
    }

    /// Convert into a [`Waker`] that calls [`wake`](Self::wake).
    pub fn into_waker(self) -> Waker {
        Waker::from(alloc::sync::Arc::new(self))
    }
}

#[cfg(target_has_atomic = "ptr")]
impl alloc::task::Wake for WakeHandle {
    fn wake(self: alloc::sync::Arc<Self>) {
        (self.0)();
    }

    fn wake_by_ref(self: &alloc::sync::Arc<Self>) {
        (self.0)();
    }
}

#[cfg(target_has_atomic = "ptr")]
impl core::fmt::Debug for WakeHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("WakeHandle")
    }
}

/// A blocking transport that can wait for input *or* a wake-up.
///
/// A plain [`Read`] can't be interrupted, so [`serve`] notices that a
/// handler became ready only when a read returns or times out. Implementing
/// this trait lets [`serve_wakeable`] react to a handler's wake immediately.
/// How to wait for both is up to the transport (a condition variable, an
/// event flag with `WFE`/`SEV`, an interrupt that also ends the UART read, a
/// self-pipe, ...).
///
/// # Contract
///
/// - Every call of the [`WakeHandle`] returned by
///   [`wake_handle`](Self::wake_handle) is *latched*: the next
///   [`read_or_wake`](Self::read_or_wake) that has not consumed it returns
///   `Ok(None)` without blocking. A wake that arrives while the server is
///   polling handlers or writing, i.e. while no read is in progress, must not
///   be lost.
/// - Wakes may be coalesced, and spurious `Ok(None)` results are fine.
/// - A wake is never reported as end of stream: `Ok(Some(0))` means the peer
///   closed the connection, as for [`Read::read`].
#[cfg(target_has_atomic = "ptr")]
pub trait WakeableRead: Read {
    /// A handle that wakes [`read_or_wake`](Self::read_or_wake). It is
    /// requested once per [`serve_wakeable`] call.
    fn wake_handle(&self) -> WakeHandle;

    /// Like [`Read::read`], but returns `Ok(None)` when woken through a
    /// [`WakeHandle`] before data arrived.
    fn read_or_wake(&mut self, buf: &mut [u8]) -> Result<Option<usize>, Self::Error>;
}

/// Like [`serve`], for a transport that can wait for input or a wake-up.
///
/// Handlers receive a waker backed by [`WakeableRead::wake_handle`]. Waking it
/// ends the wait for input, so a streaming handler that returned
/// `Poll::Pending` and is woken later (from a timer, an interrupt or another
/// thread) is polled again, and its output written, without any peer traffic
/// and without a transport read timeout.
///
/// Read errors of kind [`ErrorKind::TimedOut`] or [`ErrorKind::Interrupted`]
/// are still treated as idle ticks, as in [`serve`].
///
/// When the connection ends, every active streaming call is reported to
/// [`Handler::on_cancel`].
#[cfg(target_has_atomic = "ptr")]
pub fn serve_wakeable<IO, H>(
    mut io: IO,
    handler: &mut H,
    config: ServerConfig,
) -> Result<(), Error<IO::Error>>
where
    IO: WakeableRead + Write,
    H: Handler + ?Sized,
{
    let mut server = grpc::Server::new(config);
    let waker = io.wake_handle().into_waker();
    let mut cx = Context::from_waker(&waker);
    let result = serve_connection(&mut io, &mut server, handler, &mut cx, |io, buf| {
        io.read_or_wake(buf)
    });
    server.cancel_all(handler);
    result
}

/// The serving loop shared by [`serve`] and [`serve_wakeable`].
///
/// `read` waits for input. `Ok(None)` means it was woken: the handlers are
/// polled again. Wakes that arrive before `read` is entered must make it
/// return `Ok(None)` right away; that is the transport's job.
fn serve_connection<IO, H, R>(
    io: &mut IO,
    server: &mut grpc::Server,
    handler: &mut H,
    cx: &mut Context<'_>,
    mut read: R,
) -> Result<(), Error<IO::Error>>
where
    IO: Read + Write,
    H: Handler + ?Sized,
    R: FnMut(&mut IO, &mut [u8]) -> Result<Option<usize>, IO::Error>,
{
    let mut buf = [0u8; READ_CHUNK];
    loop {
        // Producing output can make room for more, so poll until idle.
        loop {
            server.poll(&mut *handler, cx);
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
        let n = match read(io, &mut buf) {
            Ok(None) => continue,
            Ok(Some(0)) => return Ok(()),
            Ok(Some(n)) => n,
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
/// for generated `<Service>BlockingClient`s.
///
/// Several streaming [`Call`]s can be active on one connection; drive them
/// from one thread by interleaving their operations. A blocking read can't be
/// interrupted, so a `message()` waits until its own call has something to
/// return, even if another call needs to send first. Send on every call that
/// the peer waits for before blocking on a response. Unary calls take
/// `&mut self`, so they can't run while a [`Call`] exists. The client is not
/// `Sync`.
#[derive(Debug)]
pub struct Client<IO> {
    state: RefCell<State<IO>>,
}

#[derive(Debug)]
struct State<IO> {
    io: IO,
    inner: grpc::Client,
}

impl<IO: Read + Write> Client<IO> {
    /// Create a client; the HTTP/2 preface is sent with the first call.
    pub fn new(io: IO, config: ClientConfig) -> Self {
        Self {
            state: RefCell::new(State {
                io,
                inner: grpc::Client::new(config),
            }),
        }
    }

    /// Perform one unary call.
    pub fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        let id = self.with(|s| s.inner.start_unary(path, request))?;
        loop {
            if let Some(result) = self.with(|s| s.inner.take_response(id)) {
                return result;
            }
            if self.write_output() && self.with(|s| s.inner.is_pending(id)) {
                self.read_input();
            }
        }
    }

    /// Start a streaming call of `path` (`/package.Service/Method`).
    ///
    /// The request headers are sent with the first operation on the
    /// returned [`Call`]. Dropping the call before it completes cancels it.
    pub fn streaming(&self, path: &str) -> Result<Call<'_, IO>, Status> {
        let id = self.with(|s| s.inner.start_streaming(path))?;
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
        self.with(|s| f(&s.inner))
    }

    /// Unwrap the transport.
    pub fn into_io(self) -> IO {
        self.state.into_inner().io
    }

    fn with<R>(&self, f: impl FnOnce(&mut State<IO>) -> R) -> R {
        f(&mut self.state.borrow_mut())
    }

    /// Write all pending output; `false` (and every call failed) if the
    /// transport failed.
    fn write_output(&self) -> bool {
        self.with(|s| {
            let State { io, inner, .. } = s;
            let pending = inner.pending_output().len();
            if io.write_all(inner.pending_output()).is_err() || io.flush().is_err() {
                inner.fail_all(Status::unavailable("transport write failed"));
                return false;
            }
            inner.consume_output(pending);
            true
        })
    }

    fn read_input(&self) {
        let mut buf = [0u8; READ_CHUNK];
        self.with(|s| match s.io.read(&mut buf) {
            Ok(0) => s.inner.fail_all(Status::unavailable("connection closed")),
            Ok(n) => {
                let _ = s.inner.recv(&buf[..n]);
            }
            Err(_) => s
                .inner
                .fail_all(Status::unavailable("transport read failed")),
        });
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

    fn start(&self, path: &str) -> Result<Self::Call<'_>, Status> {
        self.streaming(path)
    }
}

/// An active streaming call on a blocking [`Client`].
///
/// See the async [`Call`](crate::Call) for the semantics. Dropping the call
/// before [`message`](Self::message) reported the end cancels it.
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

    /// Send one request message, blocking while earlier messages still wait
    /// for the server's flow-control window.
    pub fn send(&mut self, message: &[u8]) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let client = self.client;
        let id = self.id;
        while !client.with(|s| s.inner.can_send(id)) {
            if client.write_output() && !client.with(|s| s.inner.can_send(id)) {
                client.read_input();
            }
        }
        let queued = client.with(|s| {
            if !s.inner.is_pending(id) {
                return Ok(false);
            }
            s.inner.send_message(id, message).map(|()| true)
        })?;
        if queued {
            client.write_output();
        }
        Ok(())
    }

    /// Half-close: no more request messages. Responses keep flowing.
    pub fn close_send(&mut self) -> Result<(), Status> {
        if self.finished.is_some() {
            return Ok(());
        }
        let client = self.client;
        let id = self.id;
        let queued = client.with(|s| {
            if !s.inner.is_pending(id) {
                return Ok(false);
            }
            s.inner.close_send(id).map(|()| true)
        })?;
        if queued {
            client.write_output();
        }
        Ok(())
    }

    /// Next response message; `Ok(None)` once the call completed with
    /// `grpc-status: 0`, `Err` if it failed.
    pub fn message(&mut self) -> Result<Option<Vec<u8>>, Status> {
        if let Some(result) = &self.finished {
            return result.clone().map(|()| None);
        }
        let client = self.client;
        let id = self.id;
        loop {
            match client.with(|s| s.inner.try_next(id)) {
                Some(Next::Message(m)) => return Ok(Some(m)),
                Some(Next::Done(result)) => {
                    self.finished = Some(result.clone());
                    return result.map(|()| None);
                }
                None if !client.with(|s| s.inner.is_pending(id)) => {
                    let status = Status::cancelled("call is not active");
                    self.finished = Some(Err(status.clone()));
                    return Err(status);
                }
                None => {}
            }
            if client.write_output() && client.with(|s| s.inner.is_pending(id)) {
                client.read_input();
            }
        }
    }
}

impl<IO> Drop for Call<'_, IO> {
    fn drop(&mut self) {
        if self.finished.is_some() {
            return;
        }
        // Never panic in `drop`: the state is only borrowed here if a
        // `with_inner` closure drops a call, which is documented as not
        // allowed.
        if let Ok(mut s) = self.client.state.try_borrow_mut() {
            s.inner.cancel(self.id);
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
