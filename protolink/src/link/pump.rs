//! A cancel-safe lower layer for drivers whose operations are not, such as DMA
//! UARTs.
//!
//! ```text
//!  ARQ / COBS ◄──► PumpHandle ◄── ring buffers ──► RxPump / TxPump ◄──► DMA UART
//!  (polls once,    (copies bytes,                   (own the HAL halves,
//!   drops futures)  never starts I/O)                drive every transfer to the end)
//! ```
//!
//! # Why
//!
//! ARQ polls its lower layer by hand. On every poll it creates a fresh `read`,
//! `write` or `flush` future, polls it once, and drops it if it is pending (see
//! [the requirements](super#requirements-on-the-raw-stream)). That is harmless
//! for a driver backed by a buffer that is filled and drained in the background
//! (an interrupt-driven or ring-buffered UART). It breaks a driver that starts a
//! DMA transfer *inside* the future and aborts it when the future is dropped:
//!
//! - a dropped `read` loses the bytes the transfer had already captured;
//! - a dropped `write` is aborted part-way, so a truncated frame is sent, and
//!   because ARQ starts it again on the next poll it may never complete.
//!
//! The pump removes the problem instead of working around it. The HAL halves are
//! owned by [`RxPump`] and [`TxPump`], whose `run` futures start a transfer and
//! always wait for it to finish. The link talks to a [`PumpHandle`] instead,
//! which only moves bytes in and out of two small ring buffers. Every handle
//! operation is cancel-safe by construction: it either copies bytes
//! synchronously and completes, or it registers a waker and stays pending
//! without having done anything.
//!
//! # When to use it
//!
//! | Raw stream | Use the pump? |
//! |---|---|
//! | Interrupt-driven or ring-buffered UART, USB CDC with a background buffer | No: already cancel-safe, use it directly. |
//! | Circular DMA receive with idle-line detection | Not for receive; it is already cancel-safe. |
//! | One-shot DMA transmit | **Yes.** Without it transmit can stall or send truncated frames. |
//! | One-shot DMA receive | **Yes**, but read the caveats below. |
//! | Tokio / TCP / anything from `std` | No. |
//!
//! The pump is optional and costs two ring buffers plus one staging buffer (see
//! [Memory](#memory)). Use it only when the driver needs it.
//!
//! # Usage
//!
//! Keep a [`PumpState`] somewhere that outlives the link (a local in the task
//! that owns the link, or a `static_cell::StaticCell` on `no_std`), then split
//! it. The handle goes into the link as its raw stream, and the two pumps must
//! be polled for as long as the link is used, either next to the application
//! (for example with `embassy_futures::select`) or on their own tasks:
//!
//! ```no_run
//! use embedded_io_async::{Read, Write};
//! use protolink::link::pump::{DefaultPumpState, run};
//! use protolink::link::{Timer, reliable_with_timer};
//!
//! # async fn demo<R: Read, W: Write, T: Timer>(uart_rx: R, uart_tx: W, timer: T) {
//! let mut state = DefaultPumpState::new();
//! let (handle, mut rx, mut tx) = state.split(uart_rx, uart_tx);
//! let mut link = reliable_with_timer(handle, timer);
//!
//! // Poll `run(&mut rx, &mut tx)` next to the code that uses `link`, e.g.
//! //     select(run(&mut rx, &mut tx), application(&mut link)).await;
//! // It only returns if the UART fails.
//! # let _ = (&mut link, &mut rx, &mut tx);
//! # }
//! ```
//!
//! [`run`] drives both directions in one future. To run them as separate tasks
//! call [`RxPump::run`] and [`TxPump::run`] directly.
//!
//! # Threads
//!
//! The handle and the two pumps share nothing but the ring buffers and a few
//! atomics, so each of them is `Send` (given `Send` HAL halves) and may run on
//! its own task, executor or thread, including on a multi-threaded executor
//! such as Tokio's. None of them is `Sync`: each is used from one place at a
//! time. To move them into spawned tasks, split a `'static` state, for example
//! one from a `static_cell::StaticCell`.
//!
//! # Behaviour
//!
//! - **Receive:** [`RxPump::run`] reads into a staging buffer and moves the
//!   bytes into the receive ring. When the ring is full it stops reading until
//!   the link has drained it (backpressure), so the pump never discards bytes
//!   itself.
//! - **Transmit:** [`PumpHandle::write`](embedded_io_async::Write::write)
//!   accepts as many bytes as fit in the transmit ring and may therefore accept
//!   fewer than it was given. [`TxPump::run`] writes them out directly from the
//!   ring in chunks of up to `CHUNK` bytes; bytes leave the ring only once the
//!   HAL has written them.
//! - **Flush:** [`PumpHandle::flush`](embedded_io_async::Write::flush) completes
//!   once every accepted byte has been written *and* the HAL's own `flush` has
//!   returned. It does not call the HAL's `flush` if nothing was written.
//! - **Errors:** a HAL failure ends the `run` future with that error and is
//!   reported to the handle as a [`PumpError`] carrying the
//!   [`ErrorKind`]. Receive errors are reported
//!   after the bytes already buffered have been read. If a pump is dropped, the
//!   matching direction fails with `BrokenPipe`, so the link never waits for a
//!   pump that is gone. A receive `Ok(0)` is treated as end of stream.
//!   [`RxPump::skip_errors`] makes receive errors such as UART overruns non-fatal;
//!   corrupt frames are already dropped by COBS and retransmitted by ARQ.
//! - **Restarting:** `run` may be called again after it returns an error, once
//!   the HAL has recovered. A transmit chunk that failed is still in the ring
//!   and is written again.
//!
//! # Caveats
//!
//! - **Receive gaps.** With one-shot DMA there is a gap between one transfer
//!   completing and the next starting. Bytes that arrive in the gap overrun the
//!   UART. COBS drops the damaged frame and ARQ retransmits, so the link stays
//!   correct, but throughput drops on a busy line. Prefer circular DMA with
//!   idle-line detection for receive where the part supports it.
//! - **Read length.** [`RxPump`] asks the HAL for up to `CHUNK` bytes at a time.
//!   A HAL `read` that waits to fill the *whole* buffer delays everything by up
//!   to `CHUNK` bytes, so wrap such a driver so that it returns on idle-line, or
//!   choose a small `CHUNK`.
//! - **Dropping a `run` future.** Dropping it while a transfer is in flight is
//!   handled by the HAL like any dropped future: an interrupted receive loses
//!   at most one chunk, and an interrupted transmit is written again from the
//!   start of its chunk when `run` is next polled, so part of it may be sent
//!   twice. ARQ recovers, but do not do this in normal operation.
//!
//! # Memory
//!
//! A [`PumpState<RX, TX, CHUNK>`](PumpState) holds a receive ring of `RX` bytes
//! and a transmit ring of `TX` bytes; [`RxPump`] holds a `CHUNK`-byte staging
//! buffer. The defaults ([`RING_SIZE`], [`CHUNK_SIZE`]) hold two COBS frames per
//! ring and use about 1.2 KiB in total. Size each ring to at least one encoded
//! frame (about 260 bytes); larger rings absorb scheduling jitter at the cost
//! of RAM.

use core::convert::Infallible;
use core::future::poll_fn;
use core::pin::pin;
#[cfg(not(feature = "portable-atomic"))]
use core::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use core::task::Poll;
#[cfg(feature = "portable-atomic")]
use portable_atomic::{AtomicU8, AtomicUsize, Ordering};

use atomic_waker::AtomicWaker;
use embedded_io_async::{Error as IoError, ErrorKind, ErrorType, Read, Write};

use super::ring::{RingBuffer, RingRx, RingTx};
use super::{ENCODED_FRAME, LinkError};

/// Default size of each ring buffer: two encoded COBS frames.
pub const RING_SIZE: usize = 2 * ENCODED_FRAME;

/// Default size of the receive staging buffer, which is also the largest
/// single HAL transfer.
pub const CHUNK_SIZE: usize = 64;

/// A [`PumpState`] with the default buffer sizes.
pub type DefaultPumpState = PumpState<RING_SIZE, RING_SIZE, CHUNK_SIZE>;

/// Error of a [`PumpHandle`]: the failure of the matching pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PumpError {
    kind: ErrorKind,
}

impl PumpError {
    /// An error of the given kind.
    pub const fn new(kind: ErrorKind) -> Self {
        Self { kind }
    }
}

impl core::fmt::Display for PumpError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "pump stream failed: {:?}", self.kind)
    }
}

impl core::error::Error for PumpError {}

impl IoError for PumpError {
    fn kind(&self) -> ErrorKind {
        self.kind
    }
}

/// Why [`run`] returned: the failure of one of the two HAL halves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpFailure<RE, WE> {
    /// The receive half failed.
    Rx(RE),
    /// The transmit half failed.
    Tx(LinkError<WE>),
}

impl<RE: core::fmt::Debug, WE: core::fmt::Debug> core::fmt::Display for PumpFailure<RE, WE> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Rx(e) => write!(f, "pump receive failed: {e:?}"),
            Self::Tx(e) => write!(f, "pump transmit failed: {e}"),
        }
    }
}

impl<RE: core::fmt::Debug, WE: core::fmt::Debug> core::error::Error for PumpFailure<RE, WE> {}

/// How one direction of the pumped stream ended.
#[derive(Clone, Copy)]
enum End {
    Eof,
    Error(ErrorKind),
}

/// The error kinds an [`AtomicEnd`] can hold. `ErrorKind` is non-exhaustive;
/// kinds added later are reported as `Other`.
const KINDS: [ErrorKind; 18] = [
    ErrorKind::Other,
    ErrorKind::NotFound,
    ErrorKind::PermissionDenied,
    ErrorKind::ConnectionRefused,
    ErrorKind::ConnectionReset,
    ErrorKind::ConnectionAborted,
    ErrorKind::NotConnected,
    ErrorKind::AddrInUse,
    ErrorKind::AddrNotAvailable,
    ErrorKind::BrokenPipe,
    ErrorKind::AlreadyExists,
    ErrorKind::InvalidInput,
    ErrorKind::InvalidData,
    ErrorKind::TimedOut,
    ErrorKind::Interrupted,
    ErrorKind::Unsupported,
    ErrorKind::OutOfMemory,
    ErrorKind::WriteZero,
];

/// An `Option<End>` in an atomic: `0` is `None`, `1` is `Eof` and `2 + i` is
/// `Error(KINDS[i])`.
struct AtomicEnd(AtomicU8);

impl AtomicEnd {
    const fn new() -> Self {
        Self(AtomicU8::new(0))
    }

    fn get(&self) -> Option<End> {
        match self.0.load(Ordering::Acquire) {
            0 => None,
            1 => Some(End::Eof),
            n => Some(End::Error(KINDS[usize::from(n - 2)])),
        }
    }

    fn set(&self, end: Option<End>) {
        let n = match end {
            None => 0,
            Some(End::Eof) => 1,
            Some(End::Error(kind)) => 2 + KINDS.iter().position(|&k| k == kind).unwrap_or(0) as u8,
        };
        self.0.store(n, Ordering::Release);
    }

    fn error(&self) -> Option<ErrorKind> {
        match self.get() {
            Some(End::Error(kind)) => Some(kind),
            _ => None,
        }
    }
}

/// What the handle and the pumps share besides the rings.
///
/// Each field is written from one side only. Flushing is tracked with counts
/// of accepted bytes (wrapping): the handle asks for everything it has
/// accepted so far to be flushed by storing its count in `flush_req`, and the
/// transmit pump stores a count in `flushed` once those bytes have been
/// written and the HAL flushed.
struct Control {
    /// How the receive side ended. Written by the [`RxPump`].
    rx_end: AtomicEnd,
    /// Why the transmit side failed. Written by the [`TxPump`].
    tx_end: AtomicEnd,
    /// The accepted count the handle wants flushed. Written by the handle.
    flush_req: AtomicUsize,
    /// The accepted count flushed to the HAL. Written by the [`TxPump`].
    flushed: AtomicUsize,
    /// The handle waiting in `read` for `rx_end`.
    read_waker: AtomicWaker,
    /// The handle waiting in `write`/`flush` for `tx_end` or `flushed`.
    write_waker: AtomicWaker,
    /// The transmit pump waiting for `flush_req`.
    flush_waker: AtomicWaker,
}

impl Control {
    const fn new() -> Self {
        Self {
            rx_end: AtomicEnd::new(),
            tx_end: AtomicEnd::new(),
            flush_req: AtomicUsize::new(0),
            flushed: AtomicUsize::new(0),
            read_waker: AtomicWaker::new(),
            write_waker: AtomicWaker::new(),
            flush_waker: AtomicWaker::new(),
        }
    }
}

async fn yield_now() {
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
}

/// Shared state of a pumped stream: the two ring buffers and the wakers.
///
/// `RX` and `TX` are the sizes of the receive and transmit rings and `CHUNK` is
/// the largest single HAL transfer. See the [module docs](self).
pub struct PumpState<const RX: usize, const TX: usize, const CHUNK: usize> {
    rx: RingBuffer<u8, RX>,
    tx: RingBuffer<u8, TX>,
    ctl: Control,
}

impl<const RX: usize, const TX: usize, const CHUNK: usize> PumpState<RX, TX, CHUNK> {
    /// Create empty state. `RX`, `TX` and `CHUNK` must be non-zero.
    pub const fn new() -> Self {
        const {
            assert!(
                RX > 0 && TX > 0 && CHUNK > 0,
                "pump buffer sizes must be non-zero"
            );
        }
        Self {
            rx: RingBuffer::new(),
            tx: RingBuffer::new(),
            ctl: Control::new(),
        }
    }

    /// Reset the state and split it around the two halves of a HAL stream.
    ///
    /// Returns the handle to give to the link, and the two pumps to poll. The
    /// borrow ends, and the state can be split again, once all three are
    /// dropped.
    pub fn split<R: Read, W: Write>(
        &mut self,
        rx: R,
        tx: W,
    ) -> (
        PumpHandle<'_, RX, TX>,
        RxPump<'_, R, RX, CHUNK>,
        TxPump<'_, W, TX, CHUNK>,
    ) {
        self.rx.clear();
        self.tx.clear();
        self.ctl = Control::new();
        let (rx_in, rx_out) = self.rx.split();
        let (tx_in, tx_out) = self.tx.split();
        let ctl = &self.ctl;
        (
            PumpHandle {
                rx: rx_out,
                tx: tx_in,
                ctl,
                accepted: 0,
            },
            RxPump {
                ring: rx_in,
                ctl,
                raw: rx,
                stage: [0; CHUNK],
                skip_errors: false,
                skipped: 0,
            },
            TxPump {
                ring: tx_out,
                ctl,
                raw: tx,
            },
        )
    }
}

impl<const RX: usize, const TX: usize, const CHUNK: usize> Default for PumpState<RX, TX, CHUNK> {
    fn default() -> Self {
        Self::new()
    }
}

/// The link side of a pumped stream.
///
/// Implements `embedded_io_async::{Read, Write}` with operations that are
/// cancel-safe: they only move bytes to or from the ring buffers. Give it to
/// [`reliable_with_timer`](super::reliable_with_timer) or
/// [`CobsFramed::new`](super::CobsFramed::new).
pub struct PumpHandle<'a, const RX: usize, const TX: usize> {
    rx: RingRx<'a, u8, RX>,
    tx: RingTx<'a, u8, TX>,
    ctl: &'a Control,
    /// Bytes accepted by `write` so far (wrapping).
    accepted: usize,
}

impl<const RX: usize, const TX: usize> ErrorType for PumpHandle<'_, RX, TX> {
    type Error = PumpError;
}

impl<const RX: usize, const TX: usize> Read for PumpHandle<'_, RX, TX> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        let ctl = self.ctl;
        let rx = &mut self.rx;
        poll_fn(|cx| {
            ctl.read_waker.register(cx.waker());
            // Look at the end before the ring: the pump pushes its last bytes
            // before it sets the end, so they are seen, and read, first.
            let end = ctl.rx_end.get();
            if rx.poll_readable(cx).is_ready() {
                return Poll::Ready(Ok(rx.pop_slice(buf)));
            }
            match end {
                Some(End::Eof) => Poll::Ready(Ok(0)),
                Some(End::Error(kind)) => Poll::Ready(Err(PumpError::new(kind))),
                None => Poll::Pending,
            }
        })
        .await
    }
}

impl<const RX: usize, const TX: usize> Write for PumpHandle<'_, RX, TX> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        let ctl = self.ctl;
        let tx = &mut self.tx;
        let accepted = &mut self.accepted;
        poll_fn(|cx| {
            ctl.write_waker.register(cx.waker());
            if let Some(kind) = ctl.tx_end.error() {
                return Poll::Ready(Err(PumpError::new(kind)));
            }
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }
            tx.poll_writable(cx).map(|_| {
                let n = tx.push_slice(buf);
                *accepted = accepted.wrapping_add(n);
                Ok(n)
            })
        })
        .await
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        let ctl = self.ctl;
        let accepted = self.accepted;
        poll_fn(|cx| {
            ctl.write_waker.register(cx.waker());
            if let Some(kind) = ctl.tx_end.error() {
                return Poll::Ready(Err(PumpError::new(kind)));
            }
            if ctl.flushed.load(Ordering::Acquire) == accepted {
                return Poll::Ready(Ok(()));
            }
            if ctl.flush_req.load(Ordering::Relaxed) != accepted {
                // Release: the transmit pump sees the bytes pushed so far.
                ctl.flush_req.store(accepted, Ordering::Release);
                ctl.flush_waker.wake();
            }
            Poll::Pending
        })
        .await
    }
}

/// The receive half of a pumped stream. Owns the HAL's receive half.
///
/// Created by [`PumpState::split`]. Poll [`run`](Self::run) for as long as the
/// link is used.
pub struct RxPump<'a, R, const RX: usize, const CHUNK: usize> {
    ring: RingTx<'a, u8, RX>,
    ctl: &'a Control,
    raw: R,
    stage: [u8; CHUNK],
    skip_errors: bool,
    skipped: u32,
}

impl<R, const RX: usize, const CHUNK: usize> RxPump<'_, R, RX, CHUNK> {
    /// Treat receive errors (for example UART overrun, framing or noise) as
    /// non-fatal: count them and keep reading instead of ending
    /// [`run`](Self::run). Off by default.
    ///
    /// The damaged bytes are not delivered as such; COBS drops the corrupt
    /// frame and ARQ retransmits it.
    pub fn skip_errors(mut self, skip: bool) -> Self {
        self.skip_errors = skip;
        self
    }

    /// Number of receive errors skipped so far, see [`skip_errors`](Self::skip_errors).
    pub fn skipped_errors(&self) -> u32 {
        self.skipped
    }

    fn end(&self, end: End) {
        self.ctl.rx_end.set(Some(end));
        self.ctl.read_waker.wake();
    }
}

impl<R: Read, const RX: usize, const CHUNK: usize> RxPump<'_, R, RX, CHUNK> {
    /// Move received bytes from the HAL into the receive ring.
    ///
    /// Returns `Ok(())` if the HAL reports end of stream, or its error. While
    /// the ring is full it waits for the link to read.
    pub async fn run(&mut self) -> Result<(), R::Error> {
        self.ctl.rx_end.set(None);
        loop {
            let free = poll_fn(|cx| self.ring.poll_writable(cx)).await;
            let want = free.min(CHUNK);
            match self.raw.read(&mut self.stage[..want]).await {
                Ok(0) => {
                    self.end(End::Eof);
                    return Ok(());
                }
                Ok(n) => {
                    // Only this pump pushes, so the free space cannot shrink.
                    let pushed = self.ring.push_slice(&self.stage[..n]);
                    debug_assert_eq!(pushed, n);
                }
                Err(_) if self.skip_errors => {
                    self.skipped = self.skipped.saturating_add(1);
                    // A HAL that fails immediately and repeatedly must not
                    // starve the executor.
                    yield_now().await;
                }
                Err(e) => {
                    self.end(End::Error(e.kind()));
                    return Err(e);
                }
            }
        }
    }
}

impl<R, const RX: usize, const CHUNK: usize> Drop for RxPump<'_, R, RX, CHUNK> {
    fn drop(&mut self) {
        if self.ctl.rx_end.get().is_none() {
            self.end(End::Error(ErrorKind::BrokenPipe));
        }
    }
}

/// The transmit half of a pumped stream. Owns the HAL's transmit half.
///
/// Created by [`PumpState::split`]. Poll [`run`](Self::run) for as long as the
/// link is used.
pub struct TxPump<'a, W, const TX: usize, const CHUNK: usize> {
    ring: RingRx<'a, u8, TX>,
    ctl: &'a Control,
    raw: W,
}

enum Work {
    Data,
    /// Flush the HAL, then report this accepted count as flushed.
    Flush(usize),
}

impl<W: Write, const TX: usize, const CHUNK: usize> TxPump<'_, W, TX, CHUNK> {
    /// Move bytes from the transmit ring to the HAL, and flush the HAL when the
    /// handle asks for it.
    ///
    /// Never returns `Ok`; it ends with the HAL's error, or
    /// [`LinkError::WriteZero`] if the HAL accepts no bytes.
    pub async fn run(&mut self) -> Result<Infallible, LinkError<W::Error>> {
        self.ctl.tx_end.set(None);
        loop {
            let ctl = self.ctl;
            let ring = &mut self.ring;
            let work = poll_fn(|cx| {
                ctl.flush_waker.register(cx.waker());
                // Load the request before looking at the ring: the bytes it
                // covers were pushed before it, so an empty ring means they
                // have all been written.
                let req = ctl.flush_req.load(Ordering::Acquire);
                if ring.poll_readable(cx).is_ready() {
                    Poll::Ready(Work::Data)
                } else if req != ctl.flushed.load(Ordering::Relaxed) {
                    Poll::Ready(Work::Flush(req))
                } else {
                    Poll::Pending
                }
            })
            .await;

            match work {
                Work::Data => {
                    let (chunk, _) = self.ring.as_slices();
                    let chunk = &chunk[..chunk.len().min(CHUNK)];
                    match self.raw.write(chunk).await {
                        Ok(0) => return Err(self.fail(LinkError::WriteZero)),
                        // The bytes leave the ring only now that they are
                        // written, which frees space for the handle.
                        Ok(n) => {
                            let n = n.min(chunk.len());
                            self.ring.consume(n);
                        }
                        Err(e) => return Err(self.fail(LinkError::Io(e))),
                    }
                }
                Work::Flush(req) => {
                    if let Err(e) = self.raw.flush().await {
                        return Err(self.fail(LinkError::Io(e)));
                    }
                    self.ctl.flushed.store(req, Ordering::Release);
                    self.ctl.write_waker.wake();
                }
            }
        }
    }

    fn fail(&self, err: LinkError<W::Error>) -> LinkError<W::Error> {
        self.ctl.tx_end.set(Some(End::Error(err.kind())));
        self.ctl.write_waker.wake();
        err
    }
}

impl<W, const TX: usize, const CHUNK: usize> Drop for TxPump<'_, W, TX, CHUNK> {
    fn drop(&mut self) {
        if self.ctl.tx_end.get().is_none() {
            self.ctl.tx_end.set(Some(End::Error(ErrorKind::BrokenPipe)));
            self.ctl.write_waker.wake();
        }
    }
}

/// Drive both pumps in one future.
///
/// Returns when either HAL half fails; it never completes otherwise. If the
/// receive half reports end of stream, the transmit half keeps running.
///
/// To run the halves as separate tasks, call [`RxPump::run`] and
/// [`TxPump::run`] instead.
pub async fn run<R, W, const RX: usize, const TX: usize, const CHUNK: usize>(
    rx: &mut RxPump<'_, R, RX, CHUNK>,
    tx: &mut TxPump<'_, W, TX, CHUNK>,
) -> Result<Infallible, PumpFailure<R::Error, W::Error>>
where
    R: Read,
    W: Write,
{
    let mut rx_run = pin!(rx.run());
    let mut tx_run = pin!(tx.run());
    let mut rx_done = false;
    poll_fn(|cx| {
        if !rx_done && let Poll::Ready(result) = rx_run.as_mut().poll(cx) {
            match result {
                Ok(()) => rx_done = true,
                Err(e) => return Poll::Ready(Err(PumpFailure::Rx(e))),
            }
        }
        match tx_run.as_mut().poll(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(PumpFailure::Tx(e))),
            Poll::Ready(Ok(never)) => match never {},
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}
