use core::pin::Pin;
use core::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::Arq;
use crate::ack_codec::AckCodec;
use crate::crc::Crc16;
use crate::error::ArqError;
use crate::timer::Timer;
use crate::transport::FrameIo;
use crate::{Op, OpOut};

impl<A> FrameIo for A
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    type Error = std::io::Error;

    fn poll_send(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        <Self as AsyncWrite>::poll_write(Pin::new(self), cx, buf)
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let mut rb = ReadBuf::new(buf);
        match <Self as AsyncRead>::poll_read(Pin::new(self), cx, &mut rb) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(rb.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        <Self as AsyncWrite>::poll_flush(Pin::new(self), cx)
    }
}

fn into_io<E>(e: ArqError<E>) -> std::io::Error
where
    ArqError<E>: core::error::Error + Send + Sync + 'static,
{
    std::io::Error::other(e)
}

/// Reads in-order data from the peer through the ARQ layer.
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr> AsyncRead
    for Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16 + Unpin,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo + Unpin,
    Tmr: Timer + Unpin,
    ArqError<Channel::Error>: core::error::Error + Send + Sync + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.read_waker.register(cx.waker());
        let slice = buf.initialize_unfilled();
        if slice.is_empty() {
            return Poll::Ready(Ok(()));
        }
        let mut op = Op::Read { buf: slice };
        match this.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(OpOut::Read(n))) => {
                buf.set_filled(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(_)) => unreachable!(),
            Poll::Ready(Err(e)) => Poll::Ready(Err(into_io(e))),
        }
    }
}

/// Writes data to the peer through the ARQ layer. `poll_shutdown` closes the
/// link towards the peer.
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType, Tmr> AsyncWrite
    for Arq<N, M, R, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16 + Unpin,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo + Unpin,
    Tmr: Timer + Unpin,
    ArqError<Channel::Error>: core::error::Error + Send + Sync + 'static,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.write_waker.register(cx.waker());
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut op = Op::Write { buf };
        match this.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(OpOut::Write(n))) => Poll::Ready(Ok(n)),
            Poll::Ready(Ok(_)) => unreachable!(),
            Poll::Ready(Err(e)) => Poll::Ready(Err(into_io(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.write_waker.register(cx.waker());
        let mut op = Op::Flush;
        match this.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(OpOut::Done)) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(_)) => unreachable!(),
            Poll::Ready(Err(e)) => Poll::Ready(Err(into_io(e))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.write_waker.register(cx.waker());
        let mut op = Op::Shutdown;
        match this.poll_op(cx, &mut op) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(OpOut::Done)) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(_)) => unreachable!(),
            Poll::Ready(Err(e)) => Poll::Ready(Err(into_io(e))),
        }
    }
}
