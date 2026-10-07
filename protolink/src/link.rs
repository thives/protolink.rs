//! Framing and reliability for raw byte links (UART, USB CDC, RS-485, ...).
//!
//! ```text
//! HTTP/2 bytes ─► ARQ (retransmission, ordering, CRC) ─► COBS (frame delimiting) ─► raw link
//! ```
//!
//! [`reliable_with_timer`] builds the full stack; the result is a
//! [`ReliableLink`], which implements `embedded_io_async::{Read, Write}` and can
//! be handed to [`serve`](crate::serve) or [`Client`](crate::Client). Both ends
//! of the link must use the same stack, and the same `arq-io-async` wire format.
//!
//! ARQ (`arq-io-async`) is a poll-based state machine over a *framed* lower
//! [`Transport`]. protolink supplies the glue on both sides: [`CobsTransport`]
//! turns a raw byte stream into that framed lower transport (one COBS frame per
//! ARQ frame), and [`ReliableLink`] drives ARQ from `async` `read`, `write` and
//! `flush`. COBS (`cobs-io-async`) is used only for its in-memory codec; ARQ
//! alone provides acknowledgement and retransmission.
//!
//! # Retransmission timer
//!
//! ARQ retransmits unacknowledged frames when a timeout expires, so it needs a
//! time source. It is supplied through the [`Timer`] trait:
//!
//! - On `no_std` targets, implement [`Timer`] on top of the platform timer
//!   (for example `embassy-time`) and call [`reliable_with_timer`]. Each link
//!   needs its own timer.
//! - With the `std` feature, [`reliable`] is available as a shortcut that uses
//!   [`StdTimer`], which works with any executor (including Tokio). `StdTimer`
//!   is owned by protolink: one reusable helper thread per timer.
//!
//! # Requirements on the raw stream
//!
//! The raw stream is typically a HAL UART or USB CDC driver. ARQ polls the
//! stack by hand: on every poll [`CobsTransport`] creates a fresh `read_frame`,
//! `write` or `flush` future on [`CobsFramed`], polls it once and drops it if it
//! is pending. [`CobsFramed`] keeps all of its state in the struct, so it
//! tolerates this, but the raw stream must too:
//!
//! - **Cancel-safe `read`:** dropping a pending `read` must not lose bytes that
//!   were already received.
//! - **Cancel-safe `write`:** dropping a pending `write` must mean nothing was
//!   written; accepted bytes are reported by a completed `write`.
//! - **Prompt `read`:** it must return as soon as at least one byte is
//!   available, not wait to fill the whole buffer.
//!
//! Interrupt-driven or ring-buffered drivers (for example a buffered UART)
//! meet these requirements. Drivers that start a DMA transfer inside the
//! future and abort it on drop do not; put them behind the [`pump`] module,
//! which owns the driver, completes every transfer, and hands the link a
//! cancel-safe stream.
//!
//! Errors from the raw stream are returned to the caller as-is. An overrun or
//! framing error reported by a HAL therefore ends the operation; a HAL wrapper
//! that wants the link to ride them out should swallow them, because corrupt
//! frames are already dropped and retransmitted.
//!
//! # Corruption recovery
//!
//! The reliable stack gives ARQ one complete decoded COBS frame at a time.
//! DAT/FIN frames must have exactly the declared length and a valid type and
//! CRC; ACKs must be exactly one BCH codeword with a valid reconstructed type
//! and CRC. Invalid frames are discarded in full, including corrupt length
//! fields: no suffix is retained and no bytes from the next frame are consumed.
//! Uncorrectable ACKs and lost frames recover through timed retransmission.
//!
//! Standalone [`CobsFramed::read`](Read::read) remains a generic byte stream:
//! it validates COBS structure, not ARQ integrity, and supports partial reads.
//! [`CobsFramed::read_frame`] is the whole-frame operation behind
//! [`CobsTransport`].
//!
//! # Failure and shutdown
//!
//! ARQ gives up after 16 retransmission rounds without acknowledgement
//! progress (250 ms doubling to 4 s, roughly a minute for a silent peer). The
//! link then fails with [`ArqError::Timeout`], wrapped in [`ReliableError`]
//! (`ErrorKind::TimedOut`). This is terminal: in-order data that had already
//! arrived can still be read, then every operation fails with
//! [`ArqError::Closed`] (`ErrorKind::BrokenPipe`). The same holds for errors of
//! the raw stream, and the end of the raw stream is reported as `Closed`.
//! `flush` completes only when the peer has acknowledged everything written
//! and every ACK owed to the peer has been written to the raw stream.
//!
//! # Memory
//!
//! [`CobsFramed`] is about 1.1 KiB. The full [`reliable_with_timer`] stack is
//! about 10.5 KiB (measured on x86-64), dominated by the ARQ window buffers.
//! Prefer constructing it in place (a `static` or an executor task) over moving
//! it through small stacks.
//!
//! Over transports that are already reliable and ordered (TCP, USB bulk), use
//! the drivers directly without this module.

pub mod pump;
pub mod ring;
#[cfg(feature = "std")]
mod timer;

use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Context, Poll};

use embedded_io_async::{ErrorKind, ErrorType, Read, Write};

use arq_io_async::{Arq, ArqLayer, BchAckCodec};
use cobs_io_async::max_encoding_length;
use cobs_io_async::sync::{decode_to_slice, encode_from_slice_including_sentinels};

pub use arq_io_async::{ArqError, Timer, Transport};
#[cfg(feature = "std")]
pub use timer::StdTimer;

/// Largest payload carried in one COBS frame. Matches the ARQ frame size.
pub const MAX_FRAME_PAYLOAD: usize = arq_io_async::MAX_FRAME;
const ENCODED_FRAME: usize = max_encoding_length(MAX_FRAME_PAYLOAD) + 2;
const RX_RAW: usize = 2 * ENCODED_FRAME;
// Each step scans at most RX_RAW bytes, then consumes one segment or polls
// one raw read. Bound both garbage processing and always-ready raw reads.
const RX_WORK_BUDGET: usize = 32;

/// ARQ retransmission window, in frames.
pub const ARQ_WINDOW: usize = 8;
/// Codeword length of the default ARQ ACK codec.
pub const ARQ_ACK_LEN: usize = 16;

/// Error of a [`CobsFramed`] link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkError<E> {
    /// The underlying transport failed.
    Io(E),
    /// The underlying transport accepted zero bytes.
    WriteZero,
    /// A frame or buffer handed to the framed transport did not fit
    /// [`MAX_FRAME_PAYLOAD`].
    FrameSize,
}

impl<E: core::fmt::Debug> core::fmt::Display for LinkError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "link i/o error: {e:?}"),
            Self::WriteZero => f.write_str("link accepted zero bytes"),
            Self::FrameSize => f.write_str("frame does not fit the link frame size"),
        }
    }
}

impl<E: core::fmt::Debug> core::error::Error for LinkError<E> {}

impl<E: embedded_io_async::Error> embedded_io_async::Error for LinkError<E> {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Io(e) => e.kind(),
            Self::WriteZero => ErrorKind::WriteZero,
            Self::FrameSize => ErrorKind::InvalidInput,
        }
    }
}

/// COBS framing over a raw byte stream.
///
/// Each `write` call (up to [`MAX_FRAME_PAYLOAD`] bytes) becomes one
/// zero-delimited COBS frame; `read` returns decoded frame payloads. Corrupt
/// frames are dropped, and the stream resynchronises on the next delimiter.
/// Receive work yields cooperatively after 32 bounded steps, even when the
/// raw stream continuously returns empty/malformed frames or oversized garbage.
///
/// All state lives in the struct, so the `read`/`write` futures may be dropped
/// and re-created at any await point, as ARQ does. If a `write` future is
/// dropped while the frame is half sent, the next `write` finishes that frame
/// first and reports the length accepted for it. Callers must therefore retry
/// with the same data, as ARQ does.
pub struct CobsFramed<S> {
    inner: S,
    rx_raw: [u8; RX_RAW],
    rx_len: usize,
    rx_discarding: bool,
    frame: [u8; MAX_FRAME_PAYLOAD],
    frame_len: usize,
    frame_pos: usize,
    tx: [u8; ENCODED_FRAME],
    tx_len: usize,
    tx_pos: usize,
    tx_accepted: usize,
}

impl<S> CobsFramed<S> {
    /// Wrap a raw byte stream.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            rx_raw: [0; RX_RAW],
            rx_len: 0,
            rx_discarding: false,
            frame: [0; MAX_FRAME_PAYLOAD],
            frame_len: 0,
            frame_pos: 0,
            tx: [0; ENCODED_FRAME],
            tx_len: 0,
            tx_pos: 0,
            tx_accepted: 0,
        }
    }

    /// Unwrap the raw stream.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: ErrorType> ErrorType for CobsFramed<S> {
    type Error = LinkError<S::Error>;
}

impl<S: Read> CobsFramed<S> {
    async fn receive_frame(&mut self) -> Result<bool, LinkError<S::Error>> {
        let mut work = 0;
        loop {
            if self.frame_pos < self.frame_len {
                return Ok(true);
            }
            if work == RX_WORK_BUDGET {
                // Framing state is already in self. Yield once and arrange a
                // retry even if the always-ready transport never registers a
                // waker; dropping this future must not drop buffered input.
                let mut yielded = false;
                poll_fn(|cx| {
                    if yielded {
                        Poll::Ready(())
                    } else {
                        yielded = true;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
                work = 0;
            }
            work += 1;
            if let Some(end) = self.rx_raw[..self.rx_len].iter().position(|&b| b == 0) {
                if end > 0 && !self.rx_discarding {
                    // The segment is a complete delimiter-terminated frame, so
                    // the decoder never sees unterminated input. A failed
                    // decode may have scribbled on `frame`; nothing of it is
                    // unread, and the whole segment is dropped below.
                    self.frame_len = 0;
                    self.frame_pos = 0;
                    if let Ok(decoded) = decode_to_slice(&self.rx_raw[..=end], &mut self.frame) {
                        self.frame_len = decoded.len;
                    }
                }
                self.rx_discarding = false;
                self.rx_raw.copy_within(end + 1..self.rx_len, 0);
                self.rx_len -= end + 1;
                continue;
            }
            if self.rx_len == self.rx_raw.len() {
                // Discard the whole oversized frame, not just this buffer:
                // its suffix must not become a new frame before the delimiter.
                self.rx_len = 0;
                self.rx_discarding = true;
            }
            let n = self
                .inner
                .read(&mut self.rx_raw[self.rx_len..])
                .await
                .map_err(LinkError::Io)?;
            if n == 0 {
                return Ok(false);
            }
            self.rx_len += n;
        }
    }
}

impl<S: Read> Read for CobsFramed<S> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() || !self.receive_frame().await? {
            return Ok(0);
        }
        let n = buf.len().min(self.frame_len - self.frame_pos);
        buf[..n].copy_from_slice(&self.frame[self.frame_pos..self.frame_pos + n]);
        self.frame_pos += n;
        Ok(n)
    }
}

impl<S: Read> CobsFramed<S> {
    /// Read one complete decoded frame into `buf`, returning its length.
    ///
    /// Unlike [`Read::read`], which hands out a frame in pieces, this returns
    /// a whole frame or nothing: empty and corrupt frames are skipped, and `0`
    /// means the raw stream ended. A remainder left by an earlier
    /// [`Read::read`] is discarded rather than reported as a frame.
    ///
    /// Like `read`, the future may be dropped and re-created at any await
    /// point without losing framing state.
    pub async fn read_frame(
        &mut self,
        buf: &mut [u8; MAX_FRAME_PAYLOAD],
    ) -> Result<usize, LinkError<S::Error>> {
        self.frame_pos = self.frame_len;
        if !self.receive_frame().await? {
            return Ok(0);
        }
        let n = self.frame_len;
        buf[..n].copy_from_slice(&self.frame[..n]);
        self.frame_pos = n;
        Ok(n)
    }
}

impl<S: Write> CobsFramed<S> {
    async fn drain(&mut self) -> Result<(), LinkError<S::Error>> {
        while self.tx_pos < self.tx_len {
            let n = self
                .inner
                .write(&self.tx[self.tx_pos..self.tx_len])
                .await
                .map_err(LinkError::Io)?;
            if n == 0 {
                return Err(LinkError::WriteZero);
            }
            self.tx_pos += n;
        }
        Ok(())
    }
}

impl<S: Write> Write for CobsFramed<S> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        if self.tx_pos >= self.tx_len {
            if buf.is_empty() {
                return Ok(0);
            }
            let n = buf.len().min(MAX_FRAME_PAYLOAD);
            // Encoding into memory cannot overflow `tx`.
            let len = encode_from_slice_including_sentinels(&buf[..n], &mut self.tx)
                .map_err(|_| LinkError::WriteZero)?;
            self.tx_len = len;
            self.tx_pos = 0;
            self.tx_accepted = n;
        }
        self.drain().await?;
        self.tx_len = 0;
        self.tx_pos = 0;
        Ok(self.tx_accepted)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.drain().await?;
        self.tx_len = 0;
        self.tx_pos = 0;
        self.inner.flush().await.map_err(LinkError::Io)
    }
}

/// [`CobsFramed`] as the framed lower transport of ARQ.
///
/// ARQ polls its lower layer by hand and relies on frame boundaries: a read
/// returns exactly one decoded COBS frame, and a write carries exactly one
/// ARQ frame. This adapter provides that contract on top of a raw
/// `embedded_io_async` stream.
///
/// On every poll it creates a fresh `read_frame`, `write` or `flush` future on
/// the [`CobsFramed`], polls it once, and drops it if it is pending. All
/// progress lives in the [`CobsFramed`], so nothing is lost, but the raw
/// stream must be cancel-safe (see the [module docs](self)); put DMA drivers
/// behind [`pump`].
///
/// - `poll_read` returns one whole frame, or `0` once the raw stream ended.
/// - `poll_write` accepts a whole frame of at most [`MAX_FRAME_PAYLOAD`]
///   bytes and reports its full length only after every encoded byte has been
///   written, however many partial raw writes and polls that takes. A larger
///   frame fails with [`LinkError::FrameSize`] rather than being clipped.
/// - `poll_flush` drains any encoded output, then flushes the raw stream.
pub struct CobsTransport<S>(CobsFramed<S>);

impl<S> CobsTransport<S> {
    /// Wrap a raw byte stream.
    pub fn new(inner: S) -> Self {
        Self(CobsFramed::new(inner))
    }

    /// Unwrap the raw stream.
    pub fn into_inner(self) -> S {
        self.0.into_inner()
    }
}

impl<S: Read + Write> Transport for CobsTransport<S> {
    type Error = LinkError<S::Error>;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let Some(frame) = buf.first_chunk_mut::<MAX_FRAME_PAYLOAD>() else {
            return Poll::Ready(Err(LinkError::FrameSize));
        };
        pin!(self.0.read_frame(frame)).as_mut().poll(cx)
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        if buf.len() > MAX_FRAME_PAYLOAD {
            return Poll::Ready(Err(LinkError::FrameSize));
        }
        pin!(self.0.write(buf)).as_mut().poll(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        pin!(self.0.flush()).as_mut().poll(cx)
    }
}

/// Error of a [`ReliableLink`].
///
/// Wraps the ARQ error so that it implements `embedded_io_async::Error`.
/// Timeout, lower I/O and write failures are terminal: the link reports the
/// original error once (after any buffered in-order data has been read) and
/// fails every later operation with [`ArqError::Closed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReliableError<E>(pub ArqError<LinkError<E>>);

impl<E> From<ArqError<LinkError<E>>> for ReliableError<E> {
    fn from(error: ArqError<LinkError<E>>) -> Self {
        Self(error)
    }
}

impl<E: core::fmt::Debug> core::fmt::Display for ReliableError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(&self.0, f)
    }
}

impl<E: core::fmt::Debug> core::error::Error for ReliableError<E> {}

impl<E: embedded_io_async::Error> embedded_io_async::Error for ReliableError<E> {
    fn kind(&self) -> ErrorKind {
        match &self.0 {
            ArqError::Io(e) => e.kind(),
            ArqError::Framing(_) | ArqError::InvalidAck(_) => ErrorKind::InvalidData,
            ArqError::Timeout => ErrorKind::TimedOut,
            ArqError::Closed => ErrorKind::BrokenPipe,
            ArqError::WriteLength => ErrorKind::WriteZero,
        }
    }
}

type Engine<S, Tmr> =
    Arq<ARQ_WINDOW, ARQ_ACK_LEN, CobsTransport<S>, ::crc::Crc<u16>, BchAckCodec, Tmr>;

/// The reliable link stack built by [`reliable_with_timer`]: ARQ over COBS
/// over `S`, retransmitting with the timer `Tmr`.
///
/// Implements `embedded_io_async::{Read, Write}`, with [`ReliableError`] as
/// its error. ARQ itself is poll based; this type drives it from `async`
/// methods. Dropping a `read`, `write` or `flush` future never loses accepted
/// data, because ARQ keeps all protocol state in the link.
pub struct ReliableLink<S: Read + Write, Tmr> {
    arq: Engine<S, Tmr>,
}

/// The reliable link stack built by [`reliable`], using [`StdTimer`].
#[cfg(feature = "std")]
pub type StdReliableLink<S> = ReliableLink<S, StdTimer>;

impl<S: Read + Write, Tmr> ErrorType for ReliableLink<S, Tmr> {
    type Error = ReliableError<S::Error>;
}

impl<S: Read + Write, Tmr: Timer> Read for ReliableLink<S, Tmr> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        poll_fn(|cx| self.arq.poll_read(cx, buf))
            .await
            .map_err(ReliableError)
    }
}

impl<S: Read + Write, Tmr: Timer> Write for ReliableLink<S, Tmr> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        poll_fn(|cx| self.arq.poll_write(cx, buf))
            .await
            .map_err(ReliableError)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        poll_fn(|cx| self.arq.poll_flush(cx))
            .await
            .map_err(ReliableError)
    }
}

/// Build the reliable link stack (ARQ over COBS) on a raw byte stream, using
/// `timer` to schedule retransmissions. Receive validation uses complete COBS
/// boundaries, so even corrupt ARQ length/type fields are discarded safely.
///
/// Uses an ARQ window of [`ARQ_WINDOW`] frames with the default CRC-16 and
/// error-correcting ACK codec, and ARQ's default retransmission policy: 250 ms
/// doubling to 4 s, and [`Timeout`](ArqError::Timeout) after 16 rounds without
/// acknowledgement progress. Needs about 10.5 KiB of RAM (see the
/// [module docs](self)). This is the constructor for `no_std` targets; see the
/// module docs for the timer requirements.
pub fn reliable_with_timer<S, Tmr>(raw: S, timer: Tmr) -> ReliableLink<S, Tmr>
where
    S: Read + Write,
    Tmr: Timer,
{
    ReliableLink {
        arq: ArqLayer::<ARQ_WINDOW>::new().build(CobsTransport::new(raw), timer),
    }
}

/// Build the reliable link stack on a raw byte stream, using a [`StdTimer`].
///
/// Shortcut for [`reliable_with_timer`]; requires the `std` feature.
#[cfg(feature = "std")]
pub fn reliable<S>(raw: S) -> StdReliableLink<S>
where
    S: Read + Write,
{
    reliable_with_timer(raw, StdTimer::new())
}
