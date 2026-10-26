//! The buffered channel between an application port and a bus runner.
//!
//! A [`Channel`] is the storage of one connection generation: a receive ring
//! (runner to application), a send ring (application to runner) and a small
//! [`Control`] block that holds the generation's state. The runner keeps its
//! own ends for as long as it lives. Application halves are claimed at
//! runtime, so a port can be detached and attached again without rebuilding
//! the bus; at most one port half of a channel is attached at a time.
//!
//! Admission is one atomic decision. The lifecycle flags and the application
//! roles share a single word, so an attachment cannot be admitted between the
//! runner's finalization steps, and a finalization cannot start while a role
//! is held. See [`Ends::try_finalize`] for the order in which a generation is
//! finished and published.
//!
//! The port side is cancel-safe by construction: every operation either copies
//! bytes synchronously and completes, or registers a waker and stays pending
//! without having done anything. No hardware operation is ever started, and so
//! none can be canceled, by a port.
//!
//! The runner finishes a generation only once no application half is attached:
//! buffers are cleared at that point, so bytes of an old generation can never
//! reach the next one. The same moment decides an abort: the snapshot taken when
//! finalization starts holds every abort issued before it, and no later one can
//! be issued. See [`Ends::try_finalize`].

use core::future::poll_fn;
#[cfg(not(feature = "portable-atomic"))]
use core::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use core::task::{Context, Poll};
#[cfg(feature = "portable-atomic")]
use portable_atomic::{AtomicBool, AtomicU16, Ordering};

use atomic_waker::AtomicWaker;
use embedded_io_async::{Error as IoError, ErrorKind, ErrorType, Read, Write};

use crate::ring::{RingBuffer, RingRx, RingTx};

/// Default size, in bytes, of each of a port's two buffers.
pub const DEFAULT_BUFFER_SIZE: usize = 64;

/// Why a [`Port`] operation failed.
///
/// Bytes already received are still delivered by `read` before a terminal
/// error is reported, except for [`Reset`](Self::Reset), which discards them.
/// The concrete hardware error is reported by the runner's event callback, not
/// by the port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortError {
    /// The bus failed, or the outcome of a transfer is unknown. The connection
    /// generation ended, and bytes not yet transferred were discarded, never
    /// replayed.
    Hardware,
    /// The peer broke the mailbox protocol, or exceeded the credit it
    /// advertised. The generation ended.
    Protocol,
    /// The runner was dropped or its future canceled.
    Closed,
    /// The connection was replaced, retired, reset by the peer, or aborted
    /// locally. This port is stale: bytes of the old connection were discarded.
    /// Attach again for the new one.
    Reset,
}

impl core::fmt::Display for PortError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Hardware => "the bus failed or a transfer's outcome is unknown",
            Self::Protocol => "the peer violated the mailbox protocol",
            Self::Closed => "the bus runner was dropped",
            Self::Reset => "the connection was replaced, retired or aborted",
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
            Self::Reset => ErrorKind::ConnectionReset,
        }
    }
}

/// Why an application could not attach a port half to a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachError {
    /// Another port holds this half, or another [`Attachment::attach`] is
    /// already waiting on the channel. Drop the holder, or wait for the waiter,
    /// first.
    Busy,
    /// The connection is ending. The runner finishes it once no half is
    /// attached, after which attaching works again; [`Attachment::attach`]
    /// waits for that.
    Ending,
    /// The runner was dropped, so the channel can never be attached again
    /// until the storage is built afresh.
    Closed,
}

impl core::fmt::Display for AttachError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Busy => "the port half is still attached, or another attachment waits",
            Self::Ending => "the connection is ending",
            Self::Closed => "the bus runner was dropped",
        })
    }
}

impl core::error::Error for AttachError {}

/// Bits of [`Control::word`].
///
/// The low bits hold the terminal cause of the generation, `0` while it is
/// live, and the other bits are flags. One atomic word lets the admission of an
/// attachment see all of them at once.
const CAUSE_MASK: u16 = 0b111;
/// The generation is being replaced, retired or aborted: attached halves fail
/// with `Reset` and their buffered bytes are discarded.
const CLOSING: u16 = 1 << 3;
/// [`Port::abort`] was called, so the runner must retire the connection on the
/// wire.
const ABORTED: u16 = 1 << 4;
/// The runner is finishing the generation. No half may attach until it is
/// published.
const FINALIZING: u16 = 1 << 5;
/// The runner was dropped. Attachments then report `Closed`, even when the
/// generation had already ended with another cause.
const CLOSED: u16 = 1 << 6;
/// An [`Attachment::attach`] future owns the single waiter slot.
const WAITER: u16 = 1 << 7;
/// The application's read half is attached.
const READER: u16 = 1 << 8;
/// The application's write half is attached.
const WRITER: u16 = 1 << 9;

/// Flags that keep attachments out until the runner publishes the generation.
const NOT_ATTACHABLE: u16 = CAUSE_MASK | CLOSING | FINALIZING;

/// The cause code of a live generation.
const LIVE: u16 = 0;

fn code(error: PortError) -> u16 {
    match error {
        PortError::Hardware => 1,
        PortError::Protocol => 2,
        PortError::Closed => 3,
        PortError::Reset => 4,
    }
}

fn cause(word: u16) -> Option<PortError> {
    match word & CAUSE_MASK {
        1 => Some(PortError::Hardware),
        2 => Some(PortError::Protocol),
        3 => Some(PortError::Closed),
        4 => Some(PortError::Reset),
        _ => None,
    }
}

/// Whether a generation with `word` discards its buffered receive bytes: it is
/// replaced, retired or aborted, so the bytes belong to a connection that is
/// over. A hardware or protocol failure does not discard them.
fn reset_pending(word: u16) -> bool {
    word & CLOSING != 0 || cause(word) == Some(PortError::Reset)
}

/// The generation as the runner's exclusive finalization found it.
///
/// It is read in the same atomic transition that starts finalization. Once that
/// transition succeeds, no application role is held and none can be admitted,
/// so no abort can follow it. The snapshot is therefore the decision boundary:
/// it holds every abort issued before the last role was released.
#[derive(Clone, Copy)]
pub(crate) struct Exclusive {
    /// The application aborted the generation before finalization began.
    pub(crate) aborted: bool,
}

/// The state that the application and the runner share besides the rings.
pub(crate) struct Control {
    /// The lifecycle and the application roles, see the flags above.
    word: AtomicU16,
    /// The application waiting in `read`.
    read_waker: AtomicWaker,
    /// The application waiting in `write` or `flush`.
    write_waker: AtomicWaker,
    /// The attachment waiting for the ending connection to finish.
    attach_waker: AtomicWaker,
    /// The runner, waiting for an application event such as an abort, an attach
    /// or a detach.
    runner_waker: AtomicWaker,
    /// An application event happened since the runner last looked.
    runner_event: AtomicBool,
}

impl Control {
    const fn new() -> Self {
        Self {
            word: AtomicU16::new(LIVE),
            read_waker: AtomicWaker::new(),
            write_waker: AtomicWaker::new(),
            attach_waker: AtomicWaker::new(),
            runner_waker: AtomicWaker::new(),
            runner_event: AtomicBool::new(false),
        }
    }

    fn load(&self) -> u16 {
        self.word.load(Ordering::Acquire)
    }

    fn set(&self, flags: u16) {
        self.word.fetch_or(flags, Ordering::AcqRel);
    }

    fn clear(&self, flags: u16) {
        self.word.fetch_and(!flags, Ordering::AcqRel);
    }

    /// Tells the runner that the application did something it may act on.
    fn notify_runner(&self) {
        self.runner_event.store(true, Ordering::Release);
        self.runner_waker.wake();
    }

    /// What a write fails with now, if anything. A generation that already
    /// ended keeps its cause; one that is being replaced reports `Reset`.
    fn terminal(&self) -> Option<PortError> {
        let word = self.load();
        cause(word).or((word & CLOSING != 0).then_some(PortError::Reset))
    }

    /// Ends the generation unless it already ended, and wakes the application.
    fn fail(&self, error: PortError) {
        let _ = self
            .word
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
                cause(word).is_none().then_some(word | code(error))
            });
        self.read_waker.wake();
        self.write_waker.wake();
        self.attach_waker.wake();
        self.notify_runner();
    }

    /// Starts retiring the connection on behalf of the application.
    fn abort(&self) {
        self.set(ABORTED | CLOSING);
        self.read_waker.wake();
        self.write_waker.wake();
        self.notify_runner();
    }

    /// Admits an application half in `role`, or says why not. Admission and
    /// the role claim are one compare-and-swap, so no check can be overtaken.
    fn admit(&self, role: u16) -> Result<(), AttachError> {
        let mut word = self.load();
        loop {
            if word & CLOSED != 0 {
                return Err(AttachError::Closed);
            }
            if word & NOT_ATTACHABLE != 0 {
                return Err(AttachError::Ending);
            }
            if word & role != 0 {
                return Err(AttachError::Busy);
            }
            match self.word.compare_exchange_weak(
                word,
                word | role,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => word = current,
            }
        }
    }

    /// Admits a full port by reserving both roles in one compare-and-swap.
    ///
    /// A held role refuses it as `Busy` even while the connection is ending:
    /// the holder must go before any waiting could succeed. Otherwise a
    /// generation that is ending or finalizing refuses it as `Ending`.
    fn admit_both(&self) -> Result<(), AttachError> {
        let mut word = self.load();
        loop {
            if word & CLOSED != 0 {
                return Err(AttachError::Closed);
            }
            if word & (READER | WRITER) != 0 {
                return Err(AttachError::Busy);
            }
            if word & NOT_ATTACHABLE != 0 {
                return Err(AttachError::Ending);
            }
            match self.word.compare_exchange_weak(
                word,
                word | READER | WRITER,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => word = current,
            }
        }
    }

    /// Gives up an application role. The caller has already released the
    /// ring claim, so the runner can take it as soon as it sees the role gone.
    fn release(&self, role: u16) {
        self.clear(role);
        self.notify_runner();
    }

    /// Claims the waiter slot, or reports that another attachment owns it.
    fn claim_waiter(&self) -> Result<(), AttachError> {
        if self.word.fetch_or(WAITER, Ordering::AcqRel) & WAITER != 0 {
            Err(AttachError::Busy)
        } else {
            Ok(())
        }
    }

    /// Starts finalization if no application role is held and nothing else is
    /// finalizing the generation. The snapshot is the word the transition
    /// replaced, so it cannot disagree with the transition about an abort.
    fn begin_finalize(&self) -> Option<Exclusive> {
        self.word
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
                (word & (READER | WRITER | FINALIZING) == 0).then_some(word | FINALIZING)
            })
            .ok()
            .map(|word| Exclusive {
                aborted: word & ABORTED != 0,
            })
    }

    /// Publishes a finalized generation: it is live again and attachable.
    fn publish(&self) {
        self.clear(CAUSE_MASK | CLOSING | ABORTED | FINALIZING);
        self.attach_waker.wake();
    }
}

/// The storage of one channel.
pub(crate) struct Channel<const RX: usize, const TX: usize> {
    /// Bytes from the runner to the application.
    recv: RingBuffer<u8, RX>,
    /// Bytes from the application to the runner.
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

    /// Discards everything, as building does. It needs `&mut`, so no port or
    /// runner from an earlier build can still refer to the channel.
    pub(crate) fn reset(&mut self) {
        self.recv.clear();
        self.send.clear();
        self.recv.release_claims();
        self.send.release_claims();
        self.ctl = Control::new();
    }

    /// A handle to attach the application's halves, see [`Attachment`].
    pub(crate) fn attachment(&self) -> Attachment<'_, RX, TX> {
        Attachment { ch: self }
    }

    /// Takes the runner's ends. They are free, since the previous runner, if
    /// any, has been dropped.
    pub(crate) fn ends(&self) -> Ends<'_, RX, TX> {
        Ends {
            recv: self
                .recv
                .claim_tx()
                .expect("the runner's ends are free once the previous runner is gone"),
            send: self
                .send
                .claim_rx()
                .expect("the runner's ends are free once the previous runner is gone"),
            ch: self,
        }
    }
}

/// The runner's ends of one channel. Dropping them closes the channel, so a
/// port never waits for a runner that is gone.
pub(crate) struct Ends<'a, const RX: usize, const TX: usize> {
    /// Writes to the receive ring.
    recv: RingTx<'a, u8, RX>,
    /// Reads from the send ring.
    send: RingRx<'a, u8, TX>,
    ch: &'a Channel<RX, TX>,
}

impl<const RX: usize, const TX: usize> Ends<'_, RX, TX> {
    /// Ends the generation with `error`, wakes the application and discards the
    /// bytes not yet transferred. Their transfers are never replayed.
    pub(crate) fn fail(&mut self, error: PortError) {
        self.ch.ctl.fail(error);
        self.discard_send();
    }

    fn discard_send(&mut self) {
        let n = self.send.len();
        self.send.consume(n);
    }

    /// Whether the generation has ended, by failure or by a replacement.
    pub(crate) fn is_ending(&self) -> bool {
        self.ch.ctl.load() & (CAUSE_MASK | CLOSING) != 0
    }

    /// Whether the application aborted the connection, and the runner has not
    /// yet retired it on the wire.
    pub(crate) fn aborted(&self) -> bool {
        self.ch.ctl.load() & ABORTED != 0
    }

    /// Whether any application half is attached.
    pub(crate) fn is_attached(&self) -> bool {
        self.ch.ctl.load() & (READER | WRITER) != 0
    }

    /// Marks the connection as being replaced or retired. Attached halves fail
    /// with [`PortError::Reset`] from now on, and the runner finishes the
    /// generation at the next [`try_finalize`](Self::try_finalize) that finds no
    /// half attached.
    ///
    /// A generation that already failed keeps its cause, which reads report
    /// after the buffered bytes. Only a replacement or a retirement resets them,
    /// so the flag is not set on a failed generation.
    pub(crate) fn set_closing(&mut self) {
        let _ = self
            .ch
            .ctl
            .word
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
                cause(word).is_none().then_some(word | CLOSING)
            });
        self.ch.ctl.read_waker.wake();
        self.ch.ctl.write_waker.wake();
    }

    /// Finishes an ending generation if no application half is attached, and
    /// returns whether it did.
    ///
    /// The order matters, and every step is inside the finalization:
    ///
    /// 1. Finalization starts only when no application role is held. The
    ///    transition takes the [`Exclusive`] snapshot, which is the abort
    ///    decision boundary: an abort is in it if and only if it was issued
    ///    before the last role was released. Until the generation is published,
    ///    attachments are refused as [`AttachError::Ending`].
    /// 2. The buffers of the old generation are cleared.
    /// 3. `commit` makes the runner's own state the next generation's, and
    ///    receives the snapshot.
    /// 4. The temporary ring claim is released.
    /// 5. The generation is published as live and attachable.
    /// 6. A waiting [`Attachment::attach`] is woken.
    ///
    /// `commit` must not wake or touch the application: it runs before the
    /// generation is visible to attachments.
    pub(crate) fn try_finalize(&mut self, commit: impl FnOnce(Exclusive)) -> bool {
        let Some(exclusive) = self.ch.ctl.begin_finalize() else {
            return false;
        };
        let mut recv_rx = self
            .ch
            .recv
            .claim_rx()
            .expect("no application half is attached while finalizing");
        let n = recv_rx.len();
        recv_rx.consume(n);
        self.discard_send();
        commit(exclusive);
        drop(recv_rx);
        self.ch.ctl.publish();
        true
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

    /// Ready once the application has aborted, attached or detached, or ended
    /// the connection, since the last call. Otherwise registers `cx` to be
    /// woken.
    ///
    /// It takes `&mut self` so that a future holding it captures the runner's
    /// end uniquely, which keeps that future `Send` even though the ends are not
    /// `Sync`.
    pub(crate) fn poll_app_event(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.ch.ctl.runner_waker.register(cx.waker());
        if self.ch.ctl.runner_event.swap(false, Ordering::AcqRel) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<const RX: usize, const TX: usize> Drop for Ends<'_, RX, TX> {
    fn drop(&mut self) {
        self.ch.ctl.set(CLOSED);
        self.ch.ctl.fail(PortError::Closed);
    }
}

/// A handle to attach application ports to one channel, after the port it came
/// from is gone.
///
/// Each attached port is one handle to the connection at that time. After a
/// [`Reset`](PortError::Reset) the old port stays stale. Drop it, and attach
/// again for the new connection. Attachments are cheap to copy.
#[derive(Clone, Copy)]
pub struct Attachment<
    'a,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    ch: &'a Channel<RX, TX>,
}

impl<'a, const RX: usize, const TX: usize> Attachment<'a, RX, TX> {
    /// Attaches both halves of a port, or fails without attaching either.
    ///
    /// Both application roles are reserved in one step, so a half that is held
    /// refuses the whole port as [`AttachError::Busy`], even during an ending
    /// generation. Half attachments are admitted one role at a time.
    pub fn try_attach(&self) -> Result<Port<'a, RX, TX>, AttachError> {
        self.ch.ctl.admit_both()?;
        let recv = self
            .ch
            .recv
            .claim_rx()
            .expect("an admitted reader finds its ring role free");
        let send = self
            .ch
            .send
            .claim_tx()
            .expect("an admitted writer finds its ring role free");
        self.ch.ctl.notify_runner();
        Ok(Port {
            reader: PortReader {
                recv: Some(recv),
                ctl: &self.ch.ctl,
            },
            writer: PortWriter {
                send: Some(send),
                ctl: &self.ch.ctl,
            },
            ch: self.ch,
        })
    }

    /// Attaches the read half only.
    pub fn try_attach_reader(&self) -> Result<PortReader<'a, RX>, AttachError> {
        self.ch.ctl.admit(READER)?;
        let recv = self
            .ch
            .recv
            .claim_rx()
            .expect("an admitted reader finds its ring role free");
        self.ch.ctl.notify_runner();
        Ok(PortReader {
            recv: Some(recv),
            ctl: &self.ch.ctl,
        })
    }

    /// Attaches the write half only.
    pub fn try_attach_writer(&self) -> Result<PortWriter<'a, TX>, AttachError> {
        self.ch.ctl.admit(WRITER)?;
        let send = self
            .ch
            .send
            .claim_tx()
            .expect("an admitted writer finds its ring role free");
        self.ch.ctl.notify_runner();
        Ok(PortWriter {
            send: Some(send),
            ctl: &self.ch.ctl,
        })
    }

    /// Attaches a port, waiting while the connection is ending.
    ///
    /// Only one `attach` waits on a channel at a time. A second one fails at
    /// once with [`AttachError::Busy`], since two waiters could not both be
    /// woken. The waiter's ownership ends with the future, so dropping a pending
    /// `attach` lets the next one wait. It also fails at once with
    /// [`AttachError::Busy`] when a half is still attached, since waiting could
    /// then never succeed.
    pub async fn attach(&self) -> Result<Port<'a, RX, TX>, AttachError> {
        let _slot = WaiterSlot::claim(&self.ch.ctl)?;
        poll_fn(|cx| {
            self.ch.ctl.attach_waker.register(cx.waker());
            match self.try_attach() {
                Err(AttachError::Ending) => Poll::Pending,
                other => Poll::Ready(other),
            }
        })
        .await
    }
}

/// The single waiter slot of a channel, held by one pending `attach`.
struct WaiterSlot<'a> {
    ctl: &'a Control,
}

impl<'a> WaiterSlot<'a> {
    fn claim(ctl: &'a Control) -> Result<Self, AttachError> {
        ctl.claim_waiter()?;
        Ok(Self { ctl })
    }
}

impl Drop for WaiterSlot<'_> {
    fn drop(&mut self) {
        self.ctl.clear(WAITER);
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
///   transfer. It fails when the connection ended first. This is a transport
///   guarantee only: reliable-link `flush` (peer acknowledgment) is a separate,
///   stronger one.
/// - Dropping a pending operation loses nothing that was accepted and starts
///   or cancels no hardware operation.
/// - Dropping the port, or one of its halves, never ends the connection: the
///   other half keeps working, and a later attachment continues the same
///   connection. [`abort`](Self::abort) is the explicit way to end it.
/// - When the runner fails or is dropped, buffered received bytes are still
///   read; after that, and for writes immediately, operations fail with a
///   [`PortError`] instead of hanging.
///
/// The default buffer sizes are [`DEFAULT_BUFFER_SIZE`].
pub struct Port<'a, const RX: usize = DEFAULT_BUFFER_SIZE, const TX: usize = DEFAULT_BUFFER_SIZE> {
    reader: PortReader<'a, RX>,
    writer: PortWriter<'a, TX>,
    ch: &'a Channel<RX, TX>,
}

/// The read half of a [`Port`].
pub struct PortReader<'a, const RX: usize = DEFAULT_BUFFER_SIZE> {
    /// Held until the drop, which releases the ring role before the role bit.
    recv: Option<RingRx<'a, u8, RX>>,
    ctl: &'a Control,
}

/// The write half of a [`Port`].
pub struct PortWriter<'a, const TX: usize = DEFAULT_BUFFER_SIZE> {
    /// Held until the drop, which releases the ring role before the role bit.
    send: Option<RingTx<'a, u8, TX>>,
    ctl: &'a Control,
}

impl<const RX: usize> Drop for PortReader<'_, RX> {
    fn drop(&mut self) {
        // The ring role goes first, so that the runner, woken by the release,
        // finds it free.
        drop(self.recv.take());
        self.ctl.release(READER);
    }
}

impl<const TX: usize> Drop for PortWriter<'_, TX> {
    fn drop(&mut self) {
        drop(self.send.take());
        self.ctl.release(WRITER);
    }
}

impl<'a, const RX: usize, const TX: usize> Port<'a, RX, TX> {
    /// Splits the port into independently owned read and write halves.
    pub fn into_split(self) -> (PortReader<'a, RX>, PortWriter<'a, TX>) {
        (self.reader, self.writer)
    }

    /// A handle to attach this connection again later.
    pub fn attachment(&self) -> Attachment<'a, RX, TX> {
        Attachment { ch: self.ch }
    }

    /// Ends the connection explicitly, from this side.
    ///
    /// This port and the other halves fail with [`PortError::Reset`] from now
    /// on, and buffered bytes of the connection are discarded. The runner
    /// retires the connection on the wire, with one best-effort attempt. It
    /// finishes once this port is dropped, after which the connection can be
    /// attached again as a new one.
    pub fn abort(&mut self) {
        self.reader.ctl.abort();
    }
}

impl<const RX: usize> PortReader<'_, RX> {
    async fn read_bytes(&mut self, buf: &mut [u8]) -> Result<usize, PortError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let ctl = self.ctl;
        let recv = self
            .recv
            .as_mut()
            .expect("held until the reader is dropped");
        poll_fn(|cx| {
            ctl.read_waker.register(cx.waker());
            // Look at the state before the ring: the runner pushes its last
            // bytes before it fails the channel, so they are read first.
            let word = ctl.load();
            if reset_pending(word) {
                // Replaced, retired, aborted or reset: the buffered bytes belong
                // to a connection that is over, so they are discarded. This
                // holds whether or not the runner has published the cause yet.
                let n = recv.len();
                recv.consume(n);
                return Poll::Ready(Err(PortError::Reset));
            }
            if recv.poll_readable(cx).is_ready() {
                return Poll::Ready(Ok(recv.pop_slice(buf)));
            }
            match cause(word) {
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
        let send = self
            .send
            .as_mut()
            .expect("held until the writer is dropped");
        poll_fn(|cx| {
            ctl.write_waker.register(cx.waker());
            if let Some(error) = ctl.terminal() {
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
        self.flush_observed(|| {}).await
    }

    /// The flush, with `between` run on every poll after the first terminal
    /// check and before the send ring is observed. Production passes a no-op;
    /// the tests use it to interleave another thread at exactly that point.
    async fn flush_observed(&mut self, mut between: impl FnMut()) -> Result<(), PortError> {
        let ctl = self.ctl;
        let send = self
            .send
            .as_mut()
            .expect("held until the writer is dropped");
        poll_fn(|cx| {
            ctl.write_waker.register(cx.waker());
            // Checked before the ring: a failed generation discards its unsent
            // bytes, so an empty ring after a failure proves nothing.
            if let Some(error) = ctl.terminal() {
                return Poll::Ready(Err(error));
            }
            between();
            // The runner consumes bytes only after their transfer succeeded,
            // so an empty ring means everything accepted was transferred. A
            // failure discards the queue after it has ended the generation, so
            // an empty ring is read again against the terminal state: success
            // needs both to permit it.
            if send.poll_writable_at_least(cx, TX).is_ready() {
                return Poll::Ready(ctl.terminal().map_or(Ok(()), Err));
            }
            Poll::Pending
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

#[cfg(test)]
mod tests {
    extern crate std;

    use core::future::Future;
    use core::pin::pin;
    use core::task::Waker;
    use std::boxed::Box;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn poll_once<F: Future>(fut: F) -> Poll<F::Output> {
        let mut fut = pin!(fut);
        fut.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    }

    /// Counts the wake-ups it receives, and unparks the thread that waits for
    /// them.
    struct CountingWaker {
        wakes: Arc<AtomicUsize>,
        thread: std::thread::Thread,
    }

    impl std::task::Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }
    }

    #[test]
    fn a_waiting_attach_is_notified_only_after_the_commit_and_then_attaches_at_once() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        ends.fail(PortError::Hardware);
        let wakes = Arc::new(AtomicUsize::new(0));
        let committed = AtomicBool::new(false);
        let (parked, is_parked) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let waker = Waker::from(Arc::new(CountingWaker {
                    wakes: wakes.clone(),
                    thread: std::thread::current(),
                }));
                let mut cx = Context::from_waker(&waker);
                let mut attach = pin!(attachment.attach());
                assert!(attach.as_mut().poll(&mut cx).is_pending());
                parked.send(()).unwrap();
                // The waiter polls again only after a notification. Polling
                // earlier could attach before any notification, which is correct
                // for the channel but would leave this test nothing to observe.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                let mut notified = 0;
                let attached = loop {
                    while wakes.load(Ordering::SeqCst) == notified {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "the publication never notified the waiter"
                        );
                        std::thread::park_timeout(std::time::Duration::from_millis(100));
                    }
                    notified = wakes.load(Ordering::SeqCst);
                    if let Poll::Ready(attached) = attach.as_mut().poll(&mut cx) {
                        break attached;
                    }
                };
                // Attached at once: no transient Busy, and the commit is visible.
                let _port = attached.expect("the waiter attaches, not Busy or Ending");
                assert!(committed.load(Ordering::Acquire));
            });
            is_parked.recv().unwrap();
            let finalized = ends.try_finalize(|_| {
                // Nothing has notified the waiter yet, and nothing attaches
                // mid-finalization.
                assert_eq!(wakes.load(Ordering::SeqCst), 0);
                assert_eq!(
                    attachment.try_attach_reader().err(),
                    Some(AttachError::Ending)
                );
                committed.store(true, Ordering::Release);
            });
            assert!(finalized);
            // Publication, after the commit and the release of the runner's
            // claims, is the one notification.
            assert_eq!(wakes.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn a_second_waiter_is_refused_busy_until_the_first_one_is_gone() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        ends.fail(PortError::Reset);
        let mut first = Box::pin(attachment.attach());
        assert!(poll_once(first.as_mut()).is_pending());
        let mut second = Box::pin(attachment.attach());
        assert!(matches!(
            poll_once(second.as_mut()),
            Poll::Ready(Err(AttachError::Busy))
        ));
        // Cancelling the first waiter releases its ownership.
        drop(first);
        let mut third = Box::pin(attachment.attach());
        assert!(poll_once(third.as_mut()).is_pending());
    }

    #[test]
    fn a_reset_discards_buffered_bytes_even_after_the_runner_published_it() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        let mut reader = attachment.try_attach_reader().unwrap();
        ends.push_recv(b"abc");
        ends.fail(PortError::Reset);
        let mut buf = [0u8; 8];
        assert_eq!(
            poll_once(reader.read(&mut buf)),
            Poll::Ready(Err(PortError::Reset))
        );
        assert_eq!(
            reader.recv.as_ref().unwrap().len(),
            0,
            "the discarded bytes are gone"
        );
    }

    #[test]
    fn dropping_the_runner_wakes_a_waiting_attachment_with_closed() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        ends.fail(PortError::Hardware);
        let mut waiter = Box::pin(attachment.attach());
        assert!(poll_once(waiter.as_mut()).is_pending());
        drop(ends);
        assert!(matches!(
            poll_once(waiter.as_mut()),
            Poll::Ready(Err(AttachError::Closed))
        ));
    }

    #[test]
    fn a_partly_failed_attach_releases_the_half_it_claimed() {
        let channel = Channel::<8, 8>::new();
        let _ends = channel.ends();
        let attachment = channel.attachment();
        let _writer = attachment.try_attach_writer().unwrap();
        assert_eq!(attachment.try_attach().err(), Some(AttachError::Busy));
        assert!(attachment.try_attach_reader().is_ok());
    }

    /// Queues three bytes, then polls one flush that pauses after its first
    /// terminal check. `during` runs on another thread inside that pause, with
    /// the runner's end, and the flush is resumed once it has returned.
    fn flush_paused_by(
        during: impl FnOnce(&mut Ends<'_, 8, 8>) + Send,
    ) -> Poll<Result<(), PortError>> {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let mut writer = channel.attachment().try_attach_writer().unwrap();
        assert!(matches!(
            poll_once(writer.write(b"abc")),
            Poll::Ready(Ok(3))
        ));
        let mut during = Some(during);
        poll_once(writer.flush_observed(|| {
            if let Some(during) = during.take() {
                std::thread::scope(|scope| {
                    scope.spawn(|| during(&mut ends));
                });
            }
        }))
    }

    #[test]
    fn a_failure_that_discards_the_queue_during_a_flush_pause_fails_the_flush() {
        for error in [PortError::Hardware, PortError::Protocol, PortError::Reset] {
            assert_eq!(
                flush_paused_by(move |ends| ends.fail(error)),
                Poll::Ready(Err(error)),
                "a flush must not succeed for a queue a {error:?} discarded"
            );
        }
    }

    #[test]
    fn a_flush_pause_during_which_the_runner_consumes_the_bytes_completes() {
        assert_eq!(
            flush_paused_by(|ends| ends.consume_send(3)),
            Poll::Ready(Ok(()))
        );
    }

    #[test]
    fn a_full_port_is_refused_busy_while_any_half_is_held_in_a_live_generation() {
        let channel = Channel::<8, 8>::new();
        let _ends = channel.ends();
        let attachment = channel.attachment();
        let reader = attachment.try_attach_reader().unwrap();
        assert_eq!(attachment.try_attach().err(), Some(AttachError::Busy));
        drop(reader);
        let writer = attachment.try_attach_writer().unwrap();
        assert_eq!(attachment.try_attach().err(), Some(AttachError::Busy));
        drop(writer);
        let port = attachment.try_attach().unwrap();
        assert_eq!(attachment.try_attach().err(), Some(AttachError::Busy));
        drop(port);
    }

    /// A holder taken before the generation ends keeps the full port refused as
    /// `Busy`, not `Ending`, until it is dropped.
    fn busy_while_held_then_ending<'a, H>(
        channel: &'a Channel<8, 8>,
        hold: impl FnOnce(Attachment<'a, 8, 8>) -> Result<H, AttachError>,
    ) {
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        let held = hold(attachment).unwrap();
        ends.fail(PortError::Hardware);
        assert_eq!(attachment.try_attach().err(), Some(AttachError::Busy));
        drop(held);
        // Nothing holds a role now, so the ending generation refuses it as Ending.
        assert_eq!(attachment.try_attach().err(), Some(AttachError::Ending));
    }

    #[test]
    fn a_full_port_is_refused_busy_while_any_half_is_held_in_an_ending_generation() {
        busy_while_held_then_ending(&Channel::new(), |a| a.try_attach_reader());
        busy_while_held_then_ending(&Channel::new(), |a| a.try_attach_writer());
        busy_while_held_then_ending(&Channel::new(), |a| a.try_attach());
    }

    #[test]
    fn a_full_port_waits_out_an_ending_generation_with_no_holder() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        ends.fail(PortError::Reset);
        assert_eq!(attachment.try_attach().err(), Some(AttachError::Ending));
        assert_eq!(
            attachment.try_attach_reader().err(),
            Some(AttachError::Ending)
        );
        assert!(ends.try_finalize(|_| {}));
        assert!(attachment.try_attach().is_ok());
    }

    #[test]
    fn a_finalizing_generation_refuses_full_ports_as_ending_not_busy() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        ends.fail(PortError::Hardware);
        assert!(ends.try_finalize(|_| {
            assert_eq!(attachment.try_attach().err(), Some(AttachError::Ending));
        }));
    }

    #[test]
    fn a_refused_full_attach_retains_neither_role() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        let reader = attachment.try_attach_reader().unwrap();
        ends.fail(PortError::Hardware);
        assert_eq!(attachment.try_attach().err(), Some(AttachError::Busy));
        drop(reader);
        // The refused full attach kept no writer: the half is merely ending.
        assert_eq!(
            attachment.try_attach_writer().err(),
            Some(AttachError::Ending)
        );
    }

    #[test]
    fn an_abort_issued_before_the_last_role_is_released_is_in_the_snapshot() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let mut port = channel.attachment().try_attach().unwrap();
        port.abort();
        drop(port);
        let mut seen = None;
        assert!(ends.try_finalize(|exclusive| seen = Some(exclusive.aborted)));
        assert_eq!(seen, Some(true));
    }

    #[test]
    fn a_failed_finalization_keeps_the_abort_for_the_next_one() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        let mut port = attachment.try_attach().unwrap();
        port.abort();
        let mut committed = false;
        assert!(!ends.try_finalize(|_| committed = true));
        assert!(!committed, "commit does not run while a half is attached");
        drop(port);
        let mut seen = None;
        assert!(ends.try_finalize(|exclusive| seen = Some(exclusive.aborted)));
        assert_eq!(seen, Some(true));
    }

    #[test]
    fn each_generation_snapshots_only_its_own_abort() {
        let channel = Channel::<8, 8>::new();
        let mut ends = channel.ends();
        let attachment = channel.attachment();
        let mut port = attachment.try_attach().unwrap();
        port.abort();
        drop(port);
        let mut first = None;
        assert!(ends.try_finalize(|exclusive| first = Some(exclusive.aborted)));
        ends.fail(PortError::Hardware);
        let mut second = None;
        assert!(ends.try_finalize(|exclusive| second = Some(exclusive.aborted)));
        assert_eq!((first, second), (Some(true), Some(false)));
    }

    #[test]
    fn full_and_half_attachments_racing_on_threads_never_hold_a_role_twice() {
        const ROUNDS: usize = 2_000;
        let channel = Channel::<8, 8>::new();
        let _ends = channel.ends();
        let attachment = channel.attachment();
        let readers = AtomicUsize::new(0);
        let writers = AtomicUsize::new(0);
        // Records a role as held for the time it takes to yield, so that two
        // holders of one role would overlap.
        let hold = |counter: &AtomicUsize| {
            assert_eq!(
                counter.fetch_add(1, Ordering::SeqCst),
                0,
                "a duplicate role"
            );
            std::thread::yield_now();
            counter.fetch_sub(1, Ordering::SeqCst);
        };
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..ROUNDS {
                    if let Ok(port) = attachment.try_attach() {
                        hold(&readers);
                        hold(&writers);
                        drop(port);
                    }
                }
            });
            scope.spawn(|| {
                for _ in 0..ROUNDS {
                    if let Ok(reader) = attachment.try_attach_reader() {
                        hold(&readers);
                        drop(reader);
                    }
                }
            });
            scope.spawn(|| {
                for _ in 0..ROUNDS {
                    if let Ok(writer) = attachment.try_attach_writer() {
                        hold(&writers);
                        drop(writer);
                    }
                }
            });
        });
    }
}
