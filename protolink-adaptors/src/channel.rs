//! The buffered channel between an application port and a bus runner.
//!
//! A [`Channel`] is the storage of one route: a receive ring (runner to port),
//! a send ring (port to runner) and a small [`Control`] block for terminal
//! failure. The port side is cancel-safe by construction: every operation
//! either copies bytes synchronously and completes, or registers a waker and
//! stays pending without having done anything. No hardware operation is ever
//! started, and so none can be canceled, by a port.

use core::future::poll_fn;
#[cfg(not(feature = "portable-atomic"))]
use core::sync::atomic::{AtomicU8, Ordering};
use core::task::{Context, Poll};
#[cfg(feature = "portable-atomic")]
use portable_atomic::{AtomicU8, Ordering};

use atomic_waker::AtomicWaker;
use embedded_io_async::{Error as IoError, ErrorKind, ErrorType, Read, Write};

use crate::ring::{RingBuffer, RingRx, RingTx};

/// Default size, in bytes, of each of a port's two buffers.
pub const DEFAULT_BUFFER_SIZE: usize = 64;

/// Why a [`Port`] operation failed.
///
/// A port fails when its runner fails or is dropped. Bytes already received
/// are still delivered by `read` before the error is reported. The concrete
/// hardware error is returned by the runner, not by the port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortError {
    /// The runner stopped because the bus hardware failed.
    Hardware,
    /// The runner stopped because the peer violated the mailbox protocol.
    Protocol,
    /// The runner was dropped or its future canceled.
    Closed,
}

impl core::fmt::Display for PortError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Hardware => "the bus runner stopped on a hardware failure",
            Self::Protocol => "the bus runner stopped on a protocol violation",
            Self::Closed => "the bus runner was dropped",
        })
    }
}

impl core::error::Error for PortError {}

impl IoError for PortError {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Hardware => ErrorKind::Other,
            Self::Protocol => ErrorKind::InvalidData,
            Self::Closed => ErrorKind::BrokenPipe,
        }
    }
}

/// What the port and the runner share besides the rings.
///
/// `end` is written only from the runner side, first failure wins. Setting it
/// wakes both port wakers, and ports register their waker before reading it,
/// so a failure cannot be missed.
pub(crate) struct Control {
    /// `0` is open; `1 + PortError` once failed.
    end: AtomicU8,
    /// The port waiting in `read`.
    read_waker: AtomicWaker,
    /// The port waiting in `write` or `flush`.
    write_waker: AtomicWaker,
}

impl Control {
    const fn new() -> Self {
        Self {
            end: AtomicU8::new(0),
            read_waker: AtomicWaker::new(),
            write_waker: AtomicWaker::new(),
        }
    }

    fn end(&self) -> Option<PortError> {
        match self.end.load(Ordering::Acquire) {
            0 => None,
            1 => Some(PortError::Hardware),
            2 => Some(PortError::Protocol),
            _ => Some(PortError::Closed),
        }
    }

    /// Fails the channel unless it already failed, and wakes the port.
    pub(crate) fn fail(&self, error: PortError) {
        if self.end().is_some() {
            return;
        }
        let code = match error {
            PortError::Hardware => 1,
            PortError::Protocol => 2,
            PortError::Closed => 3,
        };
        self.end.store(code, Ordering::Release);
        self.read_waker.wake();
        self.write_waker.wake();
    }
}

/// The storage of one route.
pub(crate) struct Channel<const RX: usize, const TX: usize> {
    /// Bytes from the runner to the port.
    recv: RingBuffer<u8, RX>,
    /// Bytes from the port to the runner.
    send: RingBuffer<u8, TX>,
    ctl: Control,
}

impl<const RX: usize, const TX: usize> Channel<RX, TX> {
    pub(crate) const fn new() -> Self {
        Self {
            recv: RingBuffer::new(),
            send: RingBuffer::new(),
            ctl: Control::new(),
        }
    }

    /// Resets the channel and splits it into the application's [`Port`] and
    /// the runner's [`Ends`]. The `&mut` borrow means this can only happen
    /// again after both are dropped.
    pub(crate) fn split(&mut self) -> (Port<'_, RX, TX>, Ends<'_, RX, TX>) {
        self.recv.clear();
        self.send.clear();
        self.ctl = Control::new();
        let (recv_tx, recv_rx) = self.recv.split();
        let (send_tx, send_rx) = self.send.split();
        let ctl = &self.ctl;
        (
            Port {
                reader: PortReader { recv: recv_rx, ctl },
                writer: PortWriter { send: send_tx, ctl },
            },
            Ends {
                recv: recv_tx,
                send: send_rx,
                ctl,
            },
        )
    }
}

/// The runner's ends of one channel. Dropping them closes the channel, so a
/// port never waits for a runner that is gone.
pub(crate) struct Ends<'a, const RX: usize, const TX: usize> {
    recv: RingTx<'a, u8, RX>,
    send: RingRx<'a, u8, TX>,
    ctl: &'a Control,
}

impl<const RX: usize, const TX: usize> Ends<'_, RX, TX> {
    pub(crate) fn fail(&self, error: PortError) {
        self.ctl.fail(error);
    }

    /// Free room in the receive buffer.
    pub(crate) fn recv_free(&self) -> usize {
        self.recv.remaining_capacity()
    }

    /// Queues received bytes and returns how many fit.
    pub(crate) fn push_recv(&mut self, data: &[u8]) -> usize {
        self.recv.push_slice(data)
    }

    /// Bytes waiting to be sent.
    pub(crate) fn send_len(&self) -> usize {
        self.send.len()
    }

    /// Copies up to `out.len()` queued bytes without removing them, so that
    /// they are only consumed once the transfer succeeded.
    pub(crate) fn peek_send(&self, out: &mut [u8]) -> usize {
        let (a, b) = self.send.as_slices();
        let n = a.len().min(out.len());
        out[..n].copy_from_slice(&a[..n]);
        let m = b.len().min(out.len() - n);
        out[n..n + m].copy_from_slice(&b[..m]);
        n + m
    }

    /// Removes `n` bytes after their transfer completed. This also lets a
    /// pending `flush` finish.
    pub(crate) fn consume_send(&mut self, n: usize) {
        self.send.consume(n);
    }

    /// Registers `cx` to be woken when there are bytes to send.
    pub(crate) fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<usize> {
        self.send.poll_readable(cx)
    }

    /// Registers `cx` to be woken when the receive buffer has room for `n`
    /// bytes.
    pub(crate) fn poll_recv_space(&mut self, cx: &mut Context<'_>, n: usize) -> Poll<usize> {
        self.recv.poll_writable_at_least(cx, n)
    }
}

impl<const RX: usize, const TX: usize> Drop for Ends<'_, RX, TX> {
    fn drop(&mut self) {
        self.ctl.fail(PortError::Closed);
    }
}

/// The application's end of a bus route: a buffered byte stream.
///
/// It implements [`embedded_io_async::Read`] and [`Write`], so it plugs into
/// `protolink::link::reliable`, `protolink::client` and `protolink::server`
/// like any other stream. Reliability is never added implicitly: layer it with
/// `protolink::link::reliable(port)` when wanted. It only moves bytes in and out
/// of two ring buffers; the bus is driven by the runner, which must be polled
/// concurrently.
///
/// - `read` returns the bytes available. An empty mailbox on the bus is
///   *idle*, not end of stream, so `read` waits rather than returning `0`.
/// - `write` accepts a prefix when there is room, and waits when there is none.
/// - `flush` completes once every byte accepted so far has completed its bus
///   transfer. This is a transport guarantee only: reliable-link `flush`
///   (peer acknowledgment) is a separate, stronger one.
/// - Dropping a pending operation loses nothing that was accepted and starts
///   or cancels no hardware operation.
/// - When the runner fails or is dropped, buffered received bytes are still
///   read; after that, and for writes immediately, operations fail with a
///   [`PortError`] instead of hanging.
///
/// The default buffer sizes are [`DEFAULT_BUFFER_SIZE`].
pub struct Port<'a, const RX: usize = DEFAULT_BUFFER_SIZE, const TX: usize = DEFAULT_BUFFER_SIZE> {
    reader: PortReader<'a, RX>,
    writer: PortWriter<'a, TX>,
}

/// The read half of a [`Port`].
pub struct PortReader<'a, const RX: usize = DEFAULT_BUFFER_SIZE> {
    recv: RingRx<'a, u8, RX>,
    ctl: &'a Control,
}

/// The write half of a [`Port`].
pub struct PortWriter<'a, const TX: usize = DEFAULT_BUFFER_SIZE> {
    send: RingTx<'a, u8, TX>,
    ctl: &'a Control,
}

impl<'a, const RX: usize, const TX: usize> Port<'a, RX, TX> {
    /// Splits the port into independently owned read and write halves.
    pub fn into_split(self) -> (PortReader<'a, RX>, PortWriter<'a, TX>) {
        (self.reader, self.writer)
    }
}

impl<const RX: usize> PortReader<'_, RX> {
    async fn read_bytes(&mut self, buf: &mut [u8]) -> Result<usize, PortError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let ctl = self.ctl;
        let recv = &mut self.recv;
        poll_fn(|cx| {
            ctl.read_waker.register(cx.waker());
            // Look at the end before the ring: the runner pushes its last
            // bytes before it fails the channel, so they are read first.
            let end = ctl.end();
            if recv.poll_readable(cx).is_ready() {
                return Poll::Ready(Ok(recv.pop_slice(buf)));
            }
            match end {
                Some(error) => Poll::Ready(Err(error)),
                None => Poll::Pending,
            }
        })
        .await
    }
}

impl<const TX: usize> PortWriter<'_, TX> {
    async fn write_bytes(&mut self, buf: &[u8]) -> Result<usize, PortError> {
        let ctl = self.ctl;
        let send = &mut self.send;
        poll_fn(|cx| {
            ctl.write_waker.register(cx.waker());
            if let Some(error) = ctl.end() {
                return Poll::Ready(Err(error));
            }
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }
            send.poll_writable(cx).map(|_| Ok(send.push_slice(buf)))
        })
        .await
    }

    async fn flush_bytes(&mut self) -> Result<(), PortError> {
        let ctl = self.ctl;
        let send = &mut self.send;
        poll_fn(|cx| {
            ctl.write_waker.register(cx.waker());
            // The runner consumes bytes only after their transfer succeeded,
            // so an empty ring means everything accepted was transferred.
            if send.poll_writable_at_least(cx, TX).is_ready() {
                return Poll::Ready(Ok(()));
            }
            match ctl.end() {
                Some(error) => Poll::Ready(Err(error)),
                None => Poll::Pending,
            }
        })
        .await
    }
}

impl<const RX: usize, const TX: usize> ErrorType for Port<'_, RX, TX> {
    type Error = PortError;
}

impl<const RX: usize, const TX: usize> Read for Port<'_, RX, TX> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, PortError> {
        self.reader.read_bytes(buf).await
    }
}

impl<const RX: usize, const TX: usize> Write for Port<'_, RX, TX> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, PortError> {
        self.writer.write_bytes(buf).await
    }

    async fn flush(&mut self) -> Result<(), PortError> {
        self.writer.flush_bytes().await
    }
}

impl<const RX: usize> ErrorType for PortReader<'_, RX> {
    type Error = PortError;
}

impl<const RX: usize> Read for PortReader<'_, RX> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, PortError> {
        self.read_bytes(buf).await
    }
}

impl<const TX: usize> ErrorType for PortWriter<'_, TX> {
    type Error = PortError;
}

impl<const TX: usize> Write for PortWriter<'_, TX> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, PortError> {
        self.write_bytes(buf).await
    }

    async fn flush(&mut self) -> Result<(), PortError> {
        self.flush_bytes().await
    }
}
