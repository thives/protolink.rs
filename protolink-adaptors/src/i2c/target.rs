use core::convert::Infallible;
use core::future::poll_fn;
use core::pin::pin;
use core::task::Poll;

use protolink_i2c::mailbox::{
    self, ConnectionId, Frame, MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE, MailboxError, Reply, Request,
};
use protolink_i2c::target::{Command, I2cTarget, ReadStatus};

use crate::Binding;
use crate::NoStorage;
use crate::channel::{Channel, DEFAULT_BUFFER_SIZE, Ends, Port, PortError};

/// Why a [`TargetRunner`] stopped.
///
/// Only a failed peripheral stops the runner, and the concrete HAL error is
/// preserved. Every port fails with [`PortError::Hardware`] once its buffered
/// data is read. Failures of the connection itself end its generation instead,
/// and are reported to the observer of [`TargetRunner::run_observed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetError<E> {
    /// The target peripheral failed. The driver has been disarmed.
    Hardware(E),
}

impl<E: core::fmt::Debug> core::fmt::Display for TargetError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Hardware(e) => write!(f, "I2C target hardware failed: {e:?}"),
        }
    }
}

impl<E: core::fmt::Debug> core::error::Error for TargetError<E> {}

/// Why the connection of a target ended, as the observer of
/// [`TargetRunner::run_observed`] is told.
///
/// The port fails with the matching [`PortError`]: [`PortError::Protocol`]
/// for the first three, [`Hardware`](PortError::Hardware) for
/// [`ReadStatus`](TargetFault::ReadStatus).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetFault {
    /// The controller wrote a frame that does not follow the wire format.
    Mailbox(MailboxError),
    /// The controller wrote more bytes than the credit it had been given.
    ReceiveOverflow,
    /// The peripheral reported a write longer than the mailbox buffer.
    OversizedWrite(usize),
    /// The controller did not read exactly one reply. The bytes of that reply
    /// may or may not have been received, so they are discarded, not replayed.
    ReadStatus(ReadStatus),
}

impl core::fmt::Display for TargetFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Mailbox(error) => write!(f, "controller sent a bad mailbox write: {error}"),
            Self::ReceiveOverflow => f.write_str("controller wrote beyond the advertised credit"),
            Self::OversizedWrite(len) => write!(f, "controller write of {len} bytes is too long"),
            Self::ReadStatus(status) => {
                write!(f, "controller did not read one whole reply: {status:?}")
            }
        }
    }
}

impl core::error::Error for TargetFault {}

/// Storage for one I2C target adaptor.
///
/// `RX` and `TX` are the receive and send buffer sizes and default to
/// [`DEFAULT_BUFFER_SIZE`]. The state holds no HAL driver. A built target
/// borrows it for as long as its port, its runner and any attachment to the
/// port are alive. Building again clears its queues, so no byte of an earlier
/// build reaches a later one.
pub struct TargetState<const RX: usize = DEFAULT_BUFFER_SIZE, const TX: usize = DEFAULT_BUFFER_SIZE>
{
    channel: Channel<RX, TX>,
}

impl TargetState {
    /// State with the default buffer sizes.
    pub const fn new() -> Self {
        Self::with_buffers()
    }
}

impl Default for TargetState {
    fn default() -> Self {
        Self::new()
    }
}

impl<const RX: usize, const TX: usize> TargetState<RX, TX> {
    /// State with `RX` receive and `TX` send bytes.
    pub const fn with_buffers() -> Self {
        const {
            assert!(RX > 0, "the receive buffer must not be empty");
            assert!(TX > 0, "the send buffer must not be empty");
        }
        Self {
            channel: Channel::new(),
        }
    }
}

/// Starts building an I2C target adaptor on `driver`.
///
/// `boot` identifies this boot of the target. It must differ from the boot of
/// any earlier run of the same target, and the controller compares it to
/// detect a reboot, so the board should draw it at random on each start. The
/// target's own address is configured on `driver` by the board, not here.
///
/// ```no_run
/// use protolink_adaptors::{Binding, TargetState, i2c_target};
/// use protolink_i2c::target::I2cTarget;
///
/// async fn demo<T: I2cTarget>(driver: T) {
///     let mut state = TargetState::new();
///     let Binding { port, runner } = i2c_target(driver, 0x2A17)
///         .with_storage(&mut state)
///         .build();
///     // Drive the runner concurrently with the code using the port.
///     # let _ = (port, runner);
/// }
/// ```
pub fn i2c_target<T: I2cTarget>(driver: T, boot: u16) -> TargetBuilder<T> {
    TargetBuilder {
        driver,
        boot,
        storage: NoStorage,
    }
}

/// Factory of an I2C target adaptor, created by [`i2c_target`].
///
/// `S` is [`NoStorage`] or the attached `&mut` [`TargetState`].
pub struct TargetBuilder<T, S = NoStorage> {
    driver: T,
    boot: u16,
    storage: S,
}

impl<T: I2cTarget, S> TargetBuilder<T, S> {
    /// Attach the caller-owned storage, which the port and the runner borrow.
    pub fn with_storage<const RX: usize, const TX: usize>(
        self,
        state: &mut TargetState<RX, TX>,
    ) -> TargetBuilder<T, &mut TargetState<RX, TX>> {
        TargetBuilder {
            driver: self.driver,
            boot: self.boot,
            storage: state,
        }
    }
}

impl<'a, T: I2cTarget, const RX: usize, const TX: usize>
    TargetBuilder<T, &'a mut TargetState<RX, TX>>
{
    /// Construct the port and the runner.
    ///
    /// There is nothing to validate, so this cannot fail, and no hardware is
    /// touched until the runner is driven. The port starts unbound: it can
    /// write, and the bytes wait for the controller's first open.
    pub fn build(self) -> Binding<Port<'a, RX, TX>, TargetRunner<'a, T, RX, TX>> {
        let state: &'a mut TargetState<RX, TX> = self.storage;
        state.channel.reset();
        let state: &'a TargetState<RX, TX> = state;
        let ends = state.channel.ends();
        let port = state
            .channel
            .attachment()
            .try_attach()
            .expect("a freshly reset channel has no attached half");
        Binding {
            port,
            runner: TargetRunner {
                driver: self.driver,
                conn: Connection {
                    ends,
                    boot: self.boot,
                    epoch: 0,
                    serving: false,
                    faulted: 0,
                    next: None,
                    abort_seen: false,
                },
            },
        }
    }
}

/// What the target does once no application half is attached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    /// Serve this connection, replacing the one before.
    Open(u16),
    /// Serve nothing until the controller opens a connection.
    Unbind,
}

/// The connection of a target: which identity it serves, and what waits to
/// change it.
///
/// It is kept apart from the driver, so that local events can be processed
/// while a listen operation is pending, see [`listen`].
struct Connection<'a, const RX: usize, const TX: usize> {
    ends: Ends<'a, RX, TX>,
    /// The boot identifier of this target.
    boot: u16,
    /// The epoch that replies name: the one served, or the one being left.
    epoch: u16,
    /// Whether `epoch` accepts writes and carries payload.
    serving: bool,
    /// The epoch of the last connection that ended with a fault. It is never
    /// served again: the controller's bytes for it were partly lost, so it
    /// must open a new epoch to continue. Zero means none.
    faulted: u16,
    /// The request to apply once no application half is attached.
    next: Option<Next>,
    /// The abort of the current generation has been observed, and so has
    /// turned into [`Next::Unbind`]. Cleared when the next generation is
    /// committed, so that each generation's abort is observed once.
    abort_seen: bool,
}

impl<const RX: usize, const TX: usize> Connection<'_, RX, TX> {
    /// Whether the generation is serving traffic now: a connection is served
    /// and it is not ending.
    fn serving_now(&self) -> bool {
        self.serving && !self.ends.is_ending()
    }

    /// Takes the application's abort of this generation, once. The connection
    /// is retired, so the pending action becomes [`Next::Unbind`], whatever it
    /// was. The channel's flag stays set, so replies keep asking for retirement
    /// until the generation is finalized.
    fn observe_abort(&mut self) {
        if !self.abort_seen && self.ends.aborted() {
            self.abort_seen = true;
            self.next = Some(Next::Unbind);
        }
    }

    /// Accepts one controller write, if it names the connection served.
    fn receive(&mut self, bytes: &[u8], observer: &mut impl FnMut(TargetFault)) {
        // An abort not yet observed is applied first, so that a request received
        // after it replaces its `Unbind`, rather than being replaced by it.
        self.observe_abort();
        let (connection, request) = match mailbox::decode_request(bytes) {
            Ok(decoded) => decoded,
            Err(error) => {
                // A write this target cannot read is a fault only on the
                // connection it served.
                if self.serving_now() {
                    self.fault(TargetFault::Mailbox(error), observer);
                }
                return;
            }
        };
        if connection.boot() != self.boot {
            return;
        }
        match request {
            Request::Open => self.open(connection),
            Request::Retire => {
                // The advertised connection is retired even while a replacement
                // waits for its last half: the retirement takes its place.
                if self.serving && connection.epoch() == self.epoch {
                    self.next = Some(Next::Unbind);
                }
            }
            Request::Data(payload) => {
                if self.serving_now() && connection.epoch() == self.epoch {
                    // A write that does not fit is refused whole, so buffered
                    // data keeps the boundaries of the writes it came from.
                    if payload.len() > self.ends.recv_free() {
                        self.fault(TargetFault::ReceiveOverflow, observer);
                    } else {
                        self.ends.push_recv(payload);
                    }
                }
            }
        }
    }

    /// Adopts `connection` as the one served, if it is not already.
    fn open(&mut self, connection: ConnectionId) {
        let epoch = connection.epoch();
        if self.faulted == epoch {
            return;
        }
        if self.serving_now() && self.epoch == epoch && self.next.is_none() {
            return;
        }
        if !self.serving && !self.ends.is_ending() && self.next.is_none() {
            // The first establishment discards nothing, so an attached port
            // keeps the bytes it wrote before the controller opened.
            self.epoch = epoch;
            self.serving = true;
            return;
        }
        self.next = Some(Next::Open(epoch));
    }

    /// Ends the generation with `fault` and reports it.
    fn fault(&mut self, fault: TargetFault, observer: &mut impl FnMut(TargetFault)) {
        if self.serving {
            self.faulted = self.epoch;
        }
        observer(fault);
        let error = match fault {
            TargetFault::ReadStatus(_) => PortError::Hardware,
            TargetFault::Mailbox(_)
            | TargetFault::ReceiveOverflow
            | TargetFault::OversizedWrite(_) => PortError::Protocol,
        };
        self.ends.fail(error);
    }

    /// Finishes a pending replacement or retirement once no application half
    /// is attached, and marks an attached half stale in the meantime.
    ///
    /// The runner's state is committed inside the channel's finalization, so
    /// attachments see the new connection only once it is served. The action
    /// is chosen there, from the [`Exclusive`] snapshot:
    ///
    /// 1. An abort that no earlier step observed retires the generation, so the
    ///    pending action becomes [`Next::Unbind`]. Any abort before the last
    ///    half was released is in the snapshot, so a late abort cannot be
    ///    replaced by the captured replacement.
    /// 2. A pending open that names the epoch a fault has ended is replaced by
    ///    [`Next::Unbind`]: that epoch is never served again.
    /// 3. Otherwise the pending action is taken.
    fn settle(&mut self) {
        self.settle_between(|| {});
    }

    /// As [`settle`](Self::settle), with `between` run after the pending action
    /// is chosen and before finalization is attempted. Production passes a
    /// no-op; the tests use it to abort in exactly that window.
    fn settle_between(&mut self, between: impl FnOnce()) {
        self.observe_abort();
        if self.ends.is_ending() && self.next.is_none() {
            self.next = Some(Next::Unbind);
        }
        let Some(next) = self.next else {
            return;
        };
        if self.ends.is_attached() {
            self.ends.set_closing();
        }
        between();
        let faulted = self.faulted;
        let (epoch, serving, abort_seen) =
            (&mut self.epoch, &mut self.serving, &mut self.abort_seen);
        let finished = self.ends.try_finalize(|exclusive| {
            let unobserved_abort = exclusive.aborted && !*abort_seen;
            let action = if unobserved_abort || next == Next::Open(faulted) {
                Next::Unbind
            } else {
                next
            };
            // Reset only once the next state is chosen: the old generation's
            // abort was judged above. The next generation's abort is its own,
            // observed when it happens.
            *abort_seen = false;
            match action {
                Next::Open(connection) => {
                    *epoch = connection;
                    *serving = true;
                }
                Next::Unbind => {
                    *epoch = 0;
                    *serving = false;
                }
            }
        });
        if finished {
            self.next = None;
        }
    }

    /// The reply to a controller read, and the payload length it carries. The
    /// payload stays queued until the read is known to have completed.
    fn reply(&self) -> (Frame, usize) {
        let serving = self.serving_now();
        let mut payload = [0u8; MAILBOX_PAYLOAD_MAX];
        let len = if serving {
            self.ends.peek_send(&mut payload)
        } else {
            0
        };
        let reply = Reply {
            payload: &payload[..len],
            rx_credit: if serving {
                mailbox::rx_credit(self.ends.recv_free())
            } else {
                0
            },
            connection: ConnectionId::new(self.boot, self.epoch),
            serving,
            retire: self.ends.aborted() && self.epoch != 0,
        };
        let mut frame = [0u8; MAILBOX_SIZE];
        mailbox::encode_reply(&reply, &mut frame)
            .expect("the payload buffer is at most one mailbox payload");
        (frame, len)
    }

    /// Takes the outcome of a read answered with a reply of `len` payload bytes.
    fn replied(&mut self, len: usize, status: ReadStatus, observer: &mut impl FnMut(TargetFault)) {
        match status {
            // Only a completed transaction removes the payload.
            ReadStatus::Done => {
                if len > 0 {
                    self.ends.consume_send(len);
                }
            }
            status => self.fault(TargetFault::ReadStatus(status), observer),
        }
    }
}

/// Services the controller's transactions for one target.
///
/// [`run`](Self::run) answers every read with one whole reply, and queues
/// every accepted write. Outgoing bytes leave the send buffer only after the
/// peripheral reports the read complete. Reads are answered even when the
/// receive buffer is full: the reply then advertises zero credit.
///
/// The runner tracks which connection it serves. A controller's first `OPEN`
/// establishes it without touching the buffers. A replacement or retirement
/// finishes once the port is dropped, and clears what was buffered for the old
/// connection. It finishes at that moment, even while the runner waits for the
/// controller's next transaction, so attaching never needs bus traffic. Failures
/// of the connection end its generation, and the runner keeps serving the bus.
///
/// The runner owns the driver. When it is dropped, finished or canceled, the
/// driver is [disarmed](I2cTarget::disarm) before the buffers are released.
pub struct TargetRunner<
    'a,
    T: I2cTarget,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    driver: T,
    conn: Connection<'a, RX, TX>,
}

impl<T: I2cTarget, const RX: usize, const TX: usize> TargetRunner<'_, T, RX, TX> {
    /// Services controller transactions until the peripheral fails.
    ///
    /// It never returns `Ok`. Connection faults are not returned: they are
    /// reported to the observer of [`run_observed`](Self::run_observed) and end
    /// the generation.
    ///
    /// The runner is consumed, so canceling (dropping) this future disarms the
    /// driver and ends the channel: the ports then fail with
    /// [`PortError::Closed`]. A transaction interrupted this way is not resumed.
    pub async fn run(self) -> Result<Infallible, TargetError<T::Error>> {
        self.run_observed(|_| {}).await
    }

    /// As [`run`](Self::run), with `observer` told of every connection fault.
    pub async fn run_observed(
        mut self,
        mut observer: impl FnMut(TargetFault),
    ) -> Result<Infallible, TargetError<T::Error>> {
        loop {
            // Before each transaction, so that its reply reflects every local
            // event that happened since the last one.
            self.conn.settle();
            if let Err(error) = self.transact(&mut observer).await {
                self.conn.ends.fail(PortError::Hardware);
                return Err(error);
            }
        }
    }

    async fn transact(
        &mut self,
        observer: &mut impl FnMut(TargetFault),
    ) -> Result<(), TargetError<T::Error>> {
        let mut frame = [0u8; MAILBOX_SIZE];
        let command = listen(&mut self.driver, &mut self.conn, &mut frame)
            .await
            .map_err(TargetError::Hardware)?;
        // A local event may have happened while the controller's transaction was
        // in flight. Settle again, so that the reply reflects it.
        self.conn.settle();
        match command {
            Command::Read => {
                let (reply, len) = self.conn.reply();
                // Nothing settles while the reply is in flight, so the payload
                // it carries is still queued when the read completes.
                let status = self
                    .driver
                    .respond_to_read(&reply)
                    .await
                    .map_err(TargetError::Hardware)?;
                self.conn.replied(len, status, observer);
            }
            Command::Write(len) => match frame.get(..len) {
                Some(bytes) => self.conn.receive(bytes, observer),
                None => self.conn.fault(TargetFault::OversizedWrite(len), observer),
            },
        }
        Ok(())
    }
}

/// Waits for the controller's next transaction.
///
/// The listen operation is started once and kept pending. A local event, such
/// as the last application half being dropped, settles the connection in
/// place: the operation itself is never dropped or started again for it.
async fn listen<T: I2cTarget, const RX: usize, const TX: usize>(
    driver: &mut T,
    conn: &mut Connection<'_, RX, TX>,
    frame: &mut [u8],
) -> Result<Command, T::Error> {
    let mut pending = pin!(driver.listen(frame));
    loop {
        let result = poll_fn(|cx| {
            if let Poll::Ready(result) = pending.as_mut().poll(cx) {
                return Poll::Ready(Some(result));
            }
            if conn.ends.poll_app_event(cx).is_ready() {
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await;
        match result {
            Some(result) => return result,
            None => conn.settle(),
        }
    }
}

impl<T: I2cTarget, const RX: usize, const TX: usize> Drop for TargetRunner<'_, T, RX, TX> {
    fn drop(&mut self) {
        // Runs before the fields drop: the hardware is disarmed first, then
        // the channel closes and the buffers can be released.
        self.driver.disarm();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A connection that serves `epoch` of boot `1`, with nothing pending.
    fn served(channel: &Channel<8, 8>, epoch: u16) -> Connection<'_, 8, 8> {
        Connection {
            ends: channel.ends(),
            boot: 1,
            epoch,
            serving: true,
            faulted: 0,
            next: None,
            abort_seen: false,
        }
    }

    #[test]
    fn an_abort_after_the_pending_action_is_chosen_retires_the_generation() {
        let channel = Channel::<8, 8>::new();
        let mut conn = served(&channel, 1);
        let mut port = channel.attachment().try_attach().unwrap();
        // Nothing has observed an abort, and the replacement is pending.
        conn.next = Some(Next::Open(2));
        // The abort and the last half's drop land in the window before the
        // finalization is acquired.
        conn.settle_between(move || {
            port.abort();
            drop(port);
        });
        assert_eq!(
            (conn.serving, conn.epoch),
            (false, 0),
            "unbound, not replaced"
        );
        assert_eq!(conn.next, None);
        assert!(!conn.abort_seen);
        assert!(
            !conn.ends.aborted(),
            "the abort is published with the generation"
        );
    }

    #[test]
    fn a_failed_finalization_keeps_the_abort_for_the_half_still_attached() {
        let channel = Channel::<8, 8>::new();
        let mut conn = served(&channel, 1);
        let mut port = channel.attachment().try_attach().unwrap();
        conn.next = Some(Next::Open(2));
        // The abort lands after the pending action is chosen, while the half is
        // still attached, so finalization is refused.
        conn.settle_between(|| port.abort());
        assert_eq!(
            (conn.serving, conn.epoch),
            (true, 1),
            "still held by the half"
        );
        assert!(conn.ends.aborted(), "the abort is not lost");
        drop(port);
        conn.settle();
        assert_eq!((conn.serving, conn.epoch), (false, 0));
        assert_eq!(conn.next, None);
    }

    #[test]
    fn a_later_open_replaces_the_unbinding_of_an_abort_already_observed() {
        let channel = Channel::<8, 8>::new();
        let mut conn = served(&channel, 1);
        let mut port = channel.attachment().try_attach().unwrap();
        port.abort();
        conn.settle();
        assert_eq!(conn.next, Some(Next::Unbind));
        assert!(conn.abort_seen);
        conn.open(ConnectionId::new(1, 2));
        drop(port);
        conn.settle();
        assert_eq!((conn.serving, conn.epoch), (true, 2));
        assert!(!conn.abort_seen);
    }
}
