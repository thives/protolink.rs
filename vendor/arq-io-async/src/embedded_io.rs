//! The `embedded_io_async` interface for [`Arq`].
//!
//! `Arq` implements [`embedded_io_async::Read`] and
//! [`embedded_io_async::Write`], and [`EiaLower`] adapts an
//! `embedded_io_async` stream to the channel interface.
//!
//! Without the `std` feature, build instances with
//! [`ArqLayer::build_with_timer`](crate::ArqLayer::build_with_timer) and a
//! [`Timer`] for your platform.
//!
//! [`EiaLower`] requires the stream's operations to be cancel-safe; see its
//! documentation.

use core::pin::Pin;
use core::task::{Context, Poll};

use crate::Arq;
use crate::ack_codec::AckCodec;
use crate::crc::Crc16;
use crate::error::ArqError;
use crate::timer::Timer;
use crate::transport::FrameIo;
use crate::{Op, OpOut};

/// Adapts an `embedded_io_async` stream to the channel interface required by
/// [`Arq`].
///
/// The inner stream must implement both [`embedded_io_async::Read`] and
/// [`embedded_io_async::Write`].
///
/// # Cancellation requirement
///
/// `Arq` is a polled state machine, so `EiaLower` cannot keep an `async`
/// operation alive between polls. On every poll it creates a new `read`,
/// `write`, or `flush` future on the inner stream, polls it once, and drops it
/// if it returns `Pending`. The inner stream's operations must therefore be
/// cancel-safe:
///
/// - Dropping a pending `read` must not lose bytes that were already received;
///   they must be returned by a later `read`.
/// - Dropping a pending `write` must mean nothing was written. Bytes that were
///   accepted must be reported by a completed `write`.
/// - Dropping a pending `flush` must leave the stream usable, so that a later
///   `flush` can complete.
///
/// Streams backed by a buffer that is filled or drained in the background,
/// such as interrupt-driven or ring-buffered UARTs, usually meet these
/// requirements. Drivers that start a transfer inside the future and abort or
/// lose it when the future is dropped do not, for example some DMA UART
/// drivers. With such drivers data is lost or corrupted.
///
/// This adapter does not support such drivers. Supporting them would need a
/// separate poll-based lower-layer interface that keeps the in-progress
/// operation across polls; this crate does not currently provide one.
pub struct EiaLower<S>(
    /// The wrapped stream.
    pub S,
);

impl<S> FrameIo for EiaLower<S>
where
    S: embedded_io_async::Read + embedded_io_async::Write,
{
    type Error = S::Error;

    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.write(buf);
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.read(buf);
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut fut = self.0.flush();
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }
}

/// Receives complete, bounded frames from a framing transport.
///
/// Each call returns one nonempty frame in `buf`, or zero at end-of-stream.
/// Empty frames must be skipped. Frames must never be truncated, split, or
/// joined. Like [`EiaLower`], the operation must be cancel-safe.
#[allow(async_fn_in_trait)]
pub trait ReadFrame: embedded_io_async::ErrorType {
    /// Read the next complete frame (at most [`crate::MAX_FRAME`] bytes).
    async fn read_frame(&mut self, buf: &mut [u8; crate::MAX_FRAME]) -> Result<usize, Self::Error>;
}

/// Adapts a frame-oriented transport to [`Arq`].
///
/// Unlike [`EiaLower`], receive boundaries are authoritative: ARQ validates
/// exact lengths, types, CRCs and ACK codewords within each frame, discarding
/// the whole frame on failure. Flushes have the same cancellation requirements
/// as [`EiaLower`]. The inner writer must accept a whole ARQ frame on each
/// successful nonempty write. It may suspend while transmitting fragments,
/// but retries after cancellation must finish that same frame and report its
/// whole length, rather than accepting a prefix as a separate frame.
pub struct EiaFramed<S>(
    /// The wrapped framing transport.
    pub S,
);

impl<S: ReadFrame + embedded_io_async::Write> FrameIo for EiaFramed<S> {
    type Error = S::Error;
    const FRAMED_RECV: bool = true;

    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.write(buf);
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.read_frame(buf.try_into().expect("ARQ frame buffer"));
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut fut = self.0.flush();
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }
}

/// The error type of [`Arq`] under the `embedded_io_async` interface.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr, E>
    embedded_io_async::ErrorType for Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    Tmr: Timer,
    E: embedded_io_async::Error,
{
    type Error = ArqError<E>;
}

/// Reads in-order data from the peer through the ARQ layer.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr, E>
    embedded_io_async::Read for Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    Tmr: Timer,
    E: embedded_io_async::Error,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        match (ReadDrive { arq: self, buf }).await {
            Ok(OpOut::Read(n)) => Ok(n),
            Ok(_) => unreachable!(),
            Err(e) => Err(e),
        }
    }
}

/// Writes data to the peer through the ARQ layer.
#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr, E>
    embedded_io_async::Write for Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo<Error = E>,
    Tmr: Timer,
    E: embedded_io_async::Error,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        match (WriteDrive { arq: self, buf }).await {
            Ok(OpOut::Write(n)) => Ok(n),
            Ok(_) => unreachable!(),
            Err(e) => Err(e),
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        match (FlushDrive { arq: self }).await {
            Ok(OpOut::Done) => Ok(()),
            Ok(_) => unreachable!(),
            Err(e) => Err(e),
        }
    }
}

#[allow(private_bounds)]
struct ReadDrive<
    'a,
    Channel,
    Crc,
    AckCodecType: AckCodec<M>,
    Tmr,
    const N: usize,
    const M: usize,
    const R: usize,
> {
    arq: &'a mut Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>,
    buf: &'a mut [u8],
}

impl<Channel, Crc, AckCodecType, Tmr, const N: usize, const M: usize, const R: usize>
    core::future::Future for ReadDrive<'_, Channel, Crc, AckCodecType, Tmr, N, M, R>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo,
    Tmr: Timer,
{
    type Output = Result<OpOut, ArqError<Channel::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut op = Op::Read { buf: this.buf };
        match this.arq.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res),
        }
    }
}

#[allow(private_bounds)]
struct WriteDrive<
    'a,
    Channel,
    Crc,
    AckCodecType: AckCodec<M>,
    Tmr,
    const N: usize,
    const M: usize,
    const R: usize,
> {
    arq: &'a mut Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>,
    buf: &'a [u8],
}

impl<Channel, Crc, AckCodecType, Tmr, const N: usize, const M: usize, const R: usize>
    core::future::Future for WriteDrive<'_, Channel, Crc, AckCodecType, Tmr, N, M, R>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo,
    Tmr: Timer,
{
    type Output = Result<OpOut, ArqError<Channel::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut op = Op::Write { buf: this.buf };
        match this.arq.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res),
        }
    }
}

#[allow(private_bounds)]
struct FlushDrive<
    'a,
    Channel,
    Crc,
    AckCodecType: AckCodec<M>,
    Tmr,
    const N: usize,
    const M: usize,
    const R: usize,
> {
    arq: &'a mut Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>,
}

impl<Channel, Crc, AckCodecType, Tmr, const N: usize, const M: usize, const R: usize>
    core::future::Future for FlushDrive<'_, Channel, Crc, AckCodecType, Tmr, N, M, R>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo,
    Tmr: Timer,
{
    type Output = Result<OpOut, ArqError<Channel::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut op = Op::Flush;
        match this.arq.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => Poll::Ready(res),
        }
    }
}

/// Maps [`ArqError`] variants to `embedded_io_async::ErrorKind`.
impl<E: embedded_io_async::Error> embedded_io_async::Error for ArqError<E> {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        match self {
            ArqError::Io(e) => e.kind(),
            ArqError::Framing(_) | ArqError::InvalidAck(_) => {
                embedded_io_async::ErrorKind::InvalidData
            }
            ArqError::Timeout => embedded_io_async::ErrorKind::TimedOut,
            ArqError::Closed => embedded_io_async::ErrorKind::BrokenPipe,
        }
    }
}
