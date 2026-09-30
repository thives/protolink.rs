//! Framing and reliability for raw byte links (UART, USB CDC, RS-485, ...).
//!
//! ```text
//! HTTP/2 bytes ─► ARQ (retransmission, ordering, CRC) ─► COBS (frame delimiting) ─► raw link
//! ```
//!
//! [`reliable_with_timer`] builds the full stack; the result implements
//! `embedded_io_async::{Read, Write}` and can be handed to [`serve`](crate::serve)
//! or [`Client`](crate::Client). Both ends of the link must use the same stack.
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
//!   [`StdTimer`], which works with any executor (including Tokio).
//!
//! # Requirements on the raw stream
//!
//! The raw stream is typically a HAL UART or USB CDC driver. ARQ polls the
//! stack by hand: on every poll it creates a fresh `read`, `write` or `flush`
//! future on [`CobsFramed`], polls it once and drops it if it is pending.
//! [`CobsFramed`] keeps all of its state in the struct, so it tolerates this,
//! but the raw stream must too:
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

use embedded_io_async::{ErrorKind, ErrorType, Read, Write};

use arq_io_async::embedded_io::EiaLower;
use arq_io_async::{Arq, ArqLayer, BchAckCodec, r};

#[cfg(feature = "std")]
pub use arq_io_async::StdTimer;
pub use arq_io_async::Timer;
use cobs_io_async::embedded::{
    decode_to_slice_buffered_async, encode_from_slice_including_sentinels_async,
};
use cobs_io_async::max_encoding_length;

/// Largest payload carried in one COBS frame. Matches the ARQ frame size.
pub const MAX_FRAME_PAYLOAD: usize = 256;
const ENCODED_FRAME: usize = max_encoding_length(MAX_FRAME_PAYLOAD) + 2;
const RX_RAW: usize = 2 * ENCODED_FRAME;

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
}

impl<E: core::fmt::Debug> core::fmt::Display for LinkError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "link i/o error: {e:?}"),
            Self::WriteZero => f.write_str("link accepted zero bytes"),
        }
    }
}

impl<E: core::fmt::Debug> core::error::Error for LinkError<E> {}

impl<E: embedded_io_async::Error> embedded_io_async::Error for LinkError<E> {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Io(e) => e.kind(),
            Self::WriteZero => ErrorKind::WriteZero,
        }
    }
}

/// COBS framing over a raw byte stream.
///
/// Each `write` call (up to [`MAX_FRAME_PAYLOAD`] bytes) becomes one
/// zero-delimited COBS frame; `read` returns decoded frame payloads. Corrupt
/// frames are dropped, and the stream resynchronises on the next delimiter.
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

impl<S: Read> Read for CobsFramed<S> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            if self.frame_pos < self.frame_len {
                let n = buf.len().min(self.frame_len - self.frame_pos);
                buf[..n].copy_from_slice(&self.frame[self.frame_pos..self.frame_pos + n]);
                self.frame_pos += n;
                return Ok(n);
            }
            if let Some(end) = self.rx_raw[..self.rx_len].iter().position(|&b| b == 0) {
                if end > 0 {
                    let mut segment = &self.rx_raw[..=end];
                    // `&[u8]` is a `BufRead`, so the whole segment is decoded in
                    // one batch. Decoding from memory never suspends.
                    if let Ok(len) =
                        decode_to_slice_buffered_async(&mut segment, &mut self.frame).await
                    {
                        self.frame_len = len as usize;
                        self.frame_pos = 0;
                    }
                }
                self.rx_raw.copy_within(end + 1..self.rx_len, 0);
                self.rx_len -= end + 1;
                continue;
            }
            if self.rx_len == self.rx_raw.len() {
                // No delimiter in a full buffer: garbage, drop it.
                self.rx_len = 0;
            }
            let n = self
                .inner
                .read(&mut self.rx_raw[self.rx_len..])
                .await
                .map_err(LinkError::Io)?;
            if n == 0 {
                return Ok(0);
            }
            self.rx_len += n;
        }
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
            let mut dest = &mut self.tx[..];
            // Encoding into memory never suspends and cannot overflow.
            let len = encode_from_slice_including_sentinels_async(&buf[..n], &mut dest)
                .await
                .map_err(|_| LinkError::WriteZero)?;
            self.tx_len = len as usize;
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

/// The reliable link stack built by [`reliable_with_timer`]: ARQ over COBS
/// over `S`, retransmitting with the timer `Tmr`.
///
/// Implements `embedded_io_async::{Read, Write}`.
pub type ReliableLink<S, Tmr> = Arq<
    ARQ_WINDOW,
    ARQ_ACK_LEN,
    { r::<ARQ_WINDOW>() },
    EiaLower<CobsFramed<S>>,
    ::crc::Crc<u16>,
    BchAckCodec,
    Tmr,
>;

/// The reliable link stack built by [`reliable`], using [`StdTimer`].
#[cfg(feature = "std")]
pub type StdReliableLink<S> = ReliableLink<S, StdTimer>;

/// Build the reliable link stack (ARQ over COBS) on a raw byte stream, using
/// `timer` to schedule retransmissions.
///
/// Uses an ARQ window of [`ARQ_WINDOW`] frames with the default CRC-16 and
/// error-correcting ACK codec. Needs about 10.5 KiB of RAM (see the
/// [module docs](self)). This is the constructor for `no_std` targets; see the
/// module docs for the timer requirements.
pub fn reliable_with_timer<S, Tmr>(raw: S, timer: Tmr) -> ReliableLink<S, Tmr>
where
    S: Read + Write,
    Tmr: Timer,
{
    ArqLayer::<ARQ_WINDOW, _, _>::new()
        .build_with_timer::<ARQ_ACK_LEN, { r::<ARQ_WINDOW>() }, _, _>(
            EiaLower(CobsFramed::new(raw)),
            timer,
        )
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
