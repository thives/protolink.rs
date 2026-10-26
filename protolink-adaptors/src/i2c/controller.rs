use core::convert::Infallible;
use core::future::poll_fn;
use core::pin::pin;
use core::task::Poll;
use core::time::Duration;

use embedded_hal_async::i2c::I2c;
use protolink_i2c::controller::{read_mailbox, write_transaction};
use protolink_i2c::mailbox::{
    self, ConnectionId, MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE, MailboxError, Reply, Request,
};
use protolink_i2c::{Address, InvalidAddress};

use crate::channel::{Channel, DEFAULT_BUFFER_SIZE, Ends, Port, PortError};
use crate::timer::{Clock, Timer};
use crate::{Binding, NoStorage};

/// Default pause when no route has work: one millisecond.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Default wait before a route tries again after a failed discovery read or an
/// `OPEN` that was not confirmed: one millisecond.
pub const DEFAULT_OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(1);

/// Default number of opening operations a route makes before its connection is
/// given up.
pub const DEFAULT_OPEN_ATTEMPTS: u32 = 64;

/// Addresses are 7-bit, so the epoch history has one slot per address.
const ADDRESS_SLOTS: usize = 128;

/// Why a controller could not be built or a route could not be opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildError {
    /// The address is not a usable 7-bit address (`0x08..=0x77`).
    InvalidAddress(u8),
    /// A route to this address is already open on the controller.
    DuplicateAddress(u8),
    /// Every route of the [`ControllerState`] is already open.
    CapacityExhausted,
    /// The open budget is zero, so no connection could ever open.
    NoOpenAttempts,
}

impl From<InvalidAddress> for BuildError {
    fn from(InvalidAddress(address): InvalidAddress) -> Self {
        Self::InvalidAddress(address)
    }
}

impl core::fmt::Display for BuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidAddress(a) => write!(f, "{a:#04x} is not a usable 7-bit I2C address"),
            Self::DuplicateAddress(a) => write!(f, "a route to {a:#04x} is already open"),
            Self::CapacityExhausted => f.write_str("all routes of the state are already open"),
            Self::NoOpenAttempts => f.write_str("the open budget must allow at least one attempt"),
        }
    }
}

impl core::error::Error for BuildError {}

/// Why a route's connection ended, or a single opening attempt failed.
///
/// The port of the route fails with [`PortError::Hardware`] for `Bus`,
/// [`PortError::Protocol`] for `Mailbox`, and [`PortError::Reset`] for the
/// rest. `OpenFailed` is reported without ending the route: the open is retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteFault<E> {
    /// A transaction with the target failed, or its outcome is unknown. Bytes
    /// not yet transferred were discarded, never replayed.
    Bus(E),
    /// An opening operation failed on the bus: a discovery read, or an `OPEN`.
    /// The route tries again at its retry deadline, while its budget lasts.
    OpenFailed(E),
    /// The target did not accept the connection within the open budget.
    OpenExhausted,
    /// The target sent a malformed mailbox reply.
    Mailbox(MailboxError),
    /// The target's boot identifier changed mid-connection, so it rebooted.
    Rebooted {
        /// The boot identifier of the connection.
        expected: u16,
        /// The boot identifier now reported.
        found: u16,
    },
    /// The target retired the connection.
    Retired,
    /// The target no longer serves the connection, without retiring it.
    NotServing,
    /// The target serves a different connection.
    Mismatch(ConnectionId),
}

/// A fault of one route, reported to the observer of
/// [`ControllerRunner::run_observed`]. Other routes are unaffected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteEvent<E> {
    /// The target address of the route.
    pub address: u8,
    /// What happened.
    pub fault: RouteFault<E>,
}

/// Storage for the routes of one I2C controller bus.
///
/// `N` is the number of peers (routes). `RX` and `TX` are the sizes of each
/// port's receive and send buffers and default to [`DEFAULT_BUFFER_SIZE`];
/// use [`with_buffers`](Self::with_buffers) to choose others. The state holds
/// no HAL driver, so it can sit in a `static` without naming one.
///
/// A built controller borrows the state for as long as any of its ports,
/// attachments or its runner is alive. Building again clears every route's
/// queues, but keeps the epoch history: for each target address, the state
/// remembers the last epoch its routes allocated, so a rebuilt controller never
/// reuses an epoch of that target. The history is per address, so it does not
/// depend on the order in which the routes are opened. It costs 256 bytes, one
/// slot per 7-bit address. Each address advances from its own last epoch, so a
/// rebuilt controller continues the sequence rather than starting over; epochs
/// wrap modulo 65 535, and an advertised epoch is skipped.
///
/// A fresh state has no history. See [`with_epoch_seed`](Self::with_epoch_seed)
/// for restarts.
///
/// `RX` must hold a full mailbox payload ([`MAILBOX_PAYLOAD_MAX`] bytes),
/// otherwise the controller could never read.
pub struct ControllerState<
    const N: usize,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    channels: [Channel<RX, TX>; N],
    /// The last epoch allocated for each target address, zero when none.
    last_epochs: [u16; ADDRESS_SLOTS],
    /// The baseline of an address with no allocated epoch. See
    /// [`with_epoch_seed`].
    ///
    /// [`with_epoch_seed`]: Self::with_epoch_seed
    epoch_seed: u16,
}

impl<const N: usize> ControllerState<N> {
    /// State for `N` routes with the default buffer sizes.
    pub const fn new() -> Self {
        Self::with_buffers()
    }
}

impl<const N: usize> Default for ControllerState<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize, const RX: usize, const TX: usize> ControllerState<N, RX, TX> {
    /// State for `N` routes with `RX` receive and `TX` send bytes per route.
    pub const fn with_buffers() -> Self {
        const {
            assert!(N > 0, "a controller needs at least one route");
            assert!(
                RX >= MAILBOX_PAYLOAD_MAX,
                "the receive buffer must hold a full mailbox payload"
            );
            assert!(TX > 0, "the send buffer must not be empty");
        }
        Self {
            channels: [const { Channel::new() }; N],
            last_epochs: [0; ADDRESS_SLOTS],
            epoch_seed: 0,
        }
    }

    /// Seeds the epoch baseline of every address that has no allocated epoch.
    ///
    /// The first epoch allocated for such an address is the one after `seed`,
    /// skipping zero. An address that already has a history keeps it, so the
    /// seed never rewinds it. Epochs advance modulo 65 535 and skip zero, so
    /// after the wraparound an address continues from its own last epoch: the
    /// seed is a starting point, not a lower bound that lasts.
    ///
    /// Seed a fresh state with an epoch the previous controller allocated, which
    /// the application must persist. Without a seed, a controller that restarts
    /// can only avoid the epoch a target advertises; an unadvertised pending
    /// replacement can still collide. No isolation is promised across arbitrary
    /// state loss.
    pub const fn with_epoch_seed(mut self, seed: u16) -> Self {
        self.epoch_seed = seed;
        self
    }
}

/// The timing and budget of a controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Settings {
    poll_interval: Duration,
    open_retry_interval: Duration,
    open_attempts: u32,
}

/// Starts building an I2C controller adaptor on `i2c`.
///
/// `timer` is the clock of the runner: it paces polling when no peer has work,
/// and it times the retries of each route. It is unrelated to the ARQ
/// retransmission timer and to RPC deadlines.
///
/// Configure the factory with `with_*` methods, attach the caller-owned
/// storage with [`with_storage`](ControllerBuilder::with_storage), then
/// [`build`](ControllerBuilder::build) it. For one peer, choose its address
/// with [`with_address`](ControllerBuilder::with_address) and `build` returns
/// the port and the runner:
///
/// ```no_run
/// use protolink_adaptors::{Binding, ControllerState, Timer, i2c_controller};
/// use embedded_hal_async::i2c::I2c;
///
/// async fn demo<I: I2c, T: Timer>(i2c: I, timer: T) {
///     let mut state = ControllerState::<1>::new();
///     let Binding { port, runner } = i2c_controller(i2c, timer)
///         .with_address(0x42)
///         .with_storage(&mut state)
///         .build()
///         .unwrap();
///     // Drive the runner concurrently with the code using the port,
///     // for example: select(runner.run(), application(port)).await;
///     # let _ = (port, runner);
/// }
/// ```
///
/// Without an address, `build` returns a [`Controller`] to
/// [`open`](Controller::open) several peers on. Building validates the
/// configuration and performs no I2C traffic. Storage is required:
///
/// ```compile_fail
/// # use embedded_hal_async::i2c::I2c;
/// # use protolink_adaptors::Timer;
/// # fn demo<I: I2c, T: Timer>(i2c: I, timer: T) {
/// let binding = protolink_adaptors::i2c_controller(i2c, timer)
///     .with_address(0x42)
///     .build();
/// # }
/// ```
pub fn i2c_controller<I: I2c, T: Timer>(i2c: I, timer: T) -> ControllerBuilder<I, T> {
    ControllerBuilder {
        i2c,
        timer,
        settings: Settings {
            poll_interval: DEFAULT_POLL_INTERVAL,
            open_retry_interval: DEFAULT_OPEN_RETRY_INTERVAL,
            open_attempts: DEFAULT_OPEN_ATTEMPTS,
        },
        peers: AnyPeers,
        storage: NoStorage,
    }
}

/// Peers of a [`ControllerBuilder`] without an address: `build` returns a
/// [`Controller`] to open peers on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AnyPeers;

/// Peers of a [`ControllerBuilder`] with an address, set by
/// [`with_address`](ControllerBuilder::with_address): `build` opens that one
/// peer and returns its port and the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinglePeer {
    address: u8,
}

/// Factory of an I2C controller adaptor, created by [`i2c_controller`].
///
/// `P` is [`AnyPeers`] or [`SinglePeer`], `S` is [`NoStorage`] or the
/// attached `&mut` [`ControllerState`].
pub struct ControllerBuilder<I, T, P = AnyPeers, S = NoStorage> {
    i2c: I,
    timer: T,
    settings: Settings,
    peers: P,
    storage: S,
}

impl<I: I2c, T: Timer, P, S> ControllerBuilder<I, T, P, S> {
    /// How long the runner sleeps when no peer made progress on a full cycle.
    /// Local outgoing data wakes it early. Defaults to one millisecond.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.settings.poll_interval = interval;
        self
    }

    /// How long a route waits before it tries again, after a failed discovery
    /// read or an `OPEN` that was not confirmed. The wait starts when that
    /// transaction completes, so a slow transaction does not shorten it. Only
    /// that route waits: the others keep their turns, and their traffic does
    /// not shorten its wait. Zero means the next attempt is due at once.
    /// Defaults to [`DEFAULT_OPEN_RETRY_INTERVAL`].
    pub fn with_open_retry_interval(mut self, interval: Duration) -> Self {
        self.settings.open_retry_interval = interval;
        self
    }

    /// How many opening operations a route makes before its connection is given
    /// up with [`RouteFault::OpenExhausted`]. Every failed discovery read counts,
    /// and so does every `OPEN` write, whether it succeeded or failed. The final
    /// permitted `OPEN` may still be confirmed. Defaults to
    /// [`DEFAULT_OPEN_ATTEMPTS`]. Must be at least one.
    pub fn with_open_attempts(mut self, attempts: u32) -> Self {
        self.settings.open_attempts = attempts;
        self
    }

    /// Talk to a single peer at `address`; `build` then returns its port and
    /// the runner. Validated by `build`.
    pub fn with_address(self, address: u8) -> ControllerBuilder<I, T, SinglePeer, S> {
        ControllerBuilder {
            i2c: self.i2c,
            timer: self.timer,
            settings: self.settings,
            peers: SinglePeer { address },
            storage: self.storage,
        }
    }

    /// Attach the caller-owned storage, which the ports and the runner borrow.
    pub fn with_storage<const N: usize, const RX: usize, const TX: usize>(
        self,
        state: &mut ControllerState<N, RX, TX>,
    ) -> ControllerBuilder<I, T, P, &mut ControllerState<N, RX, TX>> {
        ControllerBuilder {
            i2c: self.i2c,
            timer: self.timer,
            settings: self.settings,
            peers: self.peers,
            storage: state,
        }
    }

    fn controller<'a, const N: usize, const RX: usize, const TX: usize>(
        i2c: I,
        timer: T,
        settings: Settings,
        state: &'a mut ControllerState<N, RX, TX>,
    ) -> Result<Controller<'a, I, T, N, RX, TX>, BuildError> {
        if settings.open_attempts == 0 {
            return Err(BuildError::NoOpenAttempts);
        }
        let ControllerState {
            channels,
            last_epochs,
            epoch_seed,
        } = state;
        for channel in channels.iter_mut() {
            channel.reset();
        }
        let channels: &'a [Channel<RX, TX>; N] = channels;
        let mut ends = [const { None }; N];
        for (slot, channel) in ends.iter_mut().zip(channels.iter()) {
            *slot = Some(channel.ends());
        }
        Ok(Controller {
            i2c,
            timer,
            settings,
            channels,
            epochs: Epochs {
                last: last_epochs,
                seed: *epoch_seed,
            },
            unopened: ends,
            routes: [const { None }; N],
            used: 0,
        })
    }
}

impl<'a, I: I2c, T: Timer, const N: usize, const RX: usize, const TX: usize>
    ControllerBuilder<I, T, AnyPeers, &'a mut ControllerState<N, RX, TX>>
{
    /// Construct the [`Controller`] to open peers on.
    ///
    /// This validates the configuration and starts no I2C traffic;
    /// communication begins once the runner is driven.
    pub fn build(self) -> Result<Controller<'a, I, T, N, RX, TX>, BuildError> {
        Self::controller(self.i2c, self.timer, self.settings, self.storage)
    }
}

impl<'a, I: I2c, T: Timer, const N: usize, const RX: usize, const TX: usize>
    ControllerBuilder<I, T, SinglePeer, &'a mut ControllerState<N, RX, TX>>
{
    /// Construct the controller, open the peer on its first route and return
    /// the port and the runner.
    ///
    /// This validates the configuration and the address and starts no I2C
    /// traffic; communication begins once the runner is driven.
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
    ) -> Result<Binding<Port<'a, RX, TX>, ControllerRunner<'a, I, T, N, RX, TX>>, BuildError> {
        let address = self.peers.address;
        let mut controller = Self::controller(self.i2c, self.timer, self.settings, self.storage)?;
        let port = controller.open(address)?;
        Ok(Binding {
            port,
            runner: controller.into_runner(),
        })
    }
}

/// The routes of one controller bus while they are being opened.
///
/// [`open`](Self::open) peers, then turn the controller into its single
/// [`runner`](Self::into_runner). Ports borrow the [`ControllerState`], not
/// this value, so they outlive it.
pub struct Controller<
    'a,
    I,
    T,
    const N: usize,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    i2c: I,
    timer: T,
    settings: Settings,
    channels: &'a [Channel<RX, TX>; N],
    epochs: Epochs<'a>,
    unopened: [Option<Ends<'a, RX, TX>>; N],
    routes: [Option<Peer<'a, RX, TX>>; N],
    used: usize,
}

impl<'a, I: I2c, T: Timer, const N: usize, const RX: usize, const TX: usize>
    Controller<'a, I, T, N, RX, TX>
{
    /// Opens a route to the peer at `address`, and returns its port.
    ///
    /// The first connection of a route is opened by the runner as soon as the
    /// port is attached, so bytes written before that wait for it.
    pub fn open(&mut self, address: u8) -> Result<Port<'a, RX, TX>, BuildError> {
        let address = Address::new(address)?;
        if self.routes[..self.used]
            .iter()
            .flatten()
            .any(|route| route.address == address)
        {
            return Err(BuildError::DuplicateAddress(address.get()));
        }
        if self.used == N {
            return Err(BuildError::CapacityExhausted);
        }
        let ends = self.unopened[self.used]
            .take()
            .expect("each slot is opened once, in order");
        let channels: &'a [Channel<RX, TX>; N] = self.channels;
        // The channel is fresh, so its first attachment cannot be refused.
        let port = channels[self.used]
            .attachment()
            .try_attach()
            .expect("a fresh route has no attached half");
        self.routes[self.used] = Some(Peer::new(address, ends));
        self.used += 1;
        Ok(port)
    }

    /// Consumes the controller and returns the one runner of the bus.
    pub fn into_runner(self) -> ControllerRunner<'a, I, T, N, RX, TX> {
        ControllerRunner {
            i2c: self.i2c,
            timer: self.timer,
            settings: self.settings,
            epochs: self.epochs,
            routes: self.routes,
            cursor: 0,
            idle_turns: 0,
        }
    }
}

/// The epoch history of a controller state, and the floor it starts from.
struct Epochs<'a> {
    last: &'a mut [u16; ADDRESS_SLOTS],
    seed: u16,
}

impl Epochs<'_> {
    /// The next epoch for a connection to `address`.
    ///
    /// The baseline is the address's last epoch, or the seed when the address
    /// has never allocated one. Zero marks "never allocated", and an allocated
    /// epoch is never zero, so no separate flag is needed. The candidate is the
    /// next nonzero epoch after the baseline, so epochs advance modulo 65 535.
    ///
    /// It is never the epoch the target advertised in discovery, which the
    /// target may still serve: that one is skipped as well.
    fn allocate(&mut self, address: Address, advertised: u16) -> u16 {
        let last = &mut self.last[usize::from(address.get())];
        let baseline = if *last == 0 { self.seed } else { *last };
        let mut epoch = ConnectionId::next_epoch(baseline);
        if epoch == advertised {
            epoch = ConnectionId::next_epoch(epoch);
        }
        *last = epoch;
        epoch
    }
}

/// Where a route is in its connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// No connection is wanted. Attaching a port starts one.
    Idle,
    /// The target's identity is not known yet. Its first reply supplies it, and
    /// nothing else of that reply is used.
    Discovering,
    /// The connection is being opened: an `OPEN` was attempted, and only a reply
    /// that serves this connection confirms it.
    Opening,
    /// The target serves the connection, and data flows.
    Serving,
    /// The connection ended. The route waits for its port to be dropped, then
    /// becomes idle again.
    Ended,
}

/// What one turn of a route did. Pacing depends on it, so each variant means
/// exactly one thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Nothing happened: no bus operation and no change of stage.
    Idle,
    /// A bus operation was attempted and went unanswered or failed.
    Attempted,
    /// Payload moved in either direction.
    Transferred,
    /// The route changed stage without moving payload.
    Transitioned,
    /// The route ended.
    Ended,
}

/// An open route as seen by the runner.
struct Peer<'a, const RX: usize, const TX: usize> {
    address: Address,
    ends: Ends<'a, RX, TX>,
    stage: Stage,
    /// The target's boot identifier, learned from its first reply.
    boot: u16,
    /// The epoch of the current connection, or of the last one.
    epoch: u16,
    /// The receive credit the target last advertised, less what was written
    /// since.
    credit: u8,
    /// The last turn skipped the read because a full payload did not fit.
    recv_blocked: bool,
    /// Opening operations spent in the current opening: failed reads and `OPEN`
    /// writes.
    attempts: u32,
    /// The final permitted `OPEN` was written, so one confirming read is still
    /// allowed before the opening is given up.
    confirm_owed: bool,
    /// The earliest moment the route may act again, after an attempt that went
    /// unanswered. `None` when it may act at once.
    retry_at: Option<Duration>,
}

impl<'a, const RX: usize, const TX: usize> Peer<'a, RX, TX> {
    fn new(address: Address, ends: Ends<'a, RX, TX>) -> Self {
        Self {
            address,
            ends,
            stage: Stage::Idle,
            boot: 0,
            epoch: 0,
            credit: 0,
            recv_blocked: false,
            attempts: 0,
            confirm_owed: false,
            retry_at: None,
        }
    }

    /// Whether the route may act at `now`. Local abort work is not held back by
    /// a retry deadline: ending the connection needs no bus traffic.
    fn due(&self, now: Duration) -> bool {
        self.ends.aborted() || self.retry_at.is_none_or(|retry| retry <= now)
    }

    /// Ends the connection with `fault`, reports it, and fails the port.
    fn end<E>(&mut self, fault: RouteFault<E>, observer: &mut impl FnMut(RouteEvent<E>)) {
        let error = match fault {
            RouteFault::Bus(_) => PortError::Hardware,
            RouteFault::Mailbox(_) => PortError::Protocol,
            _ => PortError::Reset,
        };
        observer(RouteEvent {
            address: self.address.get(),
            fault,
        });
        self.ends.fail(error);
        self.stage = Stage::Ended;
        self.retry_at = None;
    }
}

/// The one owner of an I2C controller bus.
///
/// [`run`](Self::run) polls every route round-robin. A route whose port is
/// attached discovers its target, opens its connection, then reads one mailbox
/// when it has room for a full payload, queues the payload, and writes at most
/// one chunk allowed by the target's credit. Outgoing bytes leave the send buffer
/// only after their transaction succeeded.
///
/// A fault ends one route and leaves the others running. The runner itself
/// never stops on a route fault; see [`run_observed`](Self::run_observed).
///
/// A route that waits for its target does not hold up the others: it is skipped
/// until its own retry deadline, and the runner sleeps only when no route is due.
///
/// Construct it with [`i2c_controller`]. Nothing happens until `run` is
/// polled.
pub struct ControllerRunner<
    'a,
    I,
    T,
    const N: usize,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    i2c: I,
    timer: T,
    settings: Settings,
    epochs: Epochs<'a>,
    routes: [Option<Peer<'a, RX, TX>>; N],
    cursor: usize,
    /// Consecutive turns that moved no data.
    idle_turns: usize,
}

impl<I: I2c, T: Timer, const N: usize, const RX: usize, const TX: usize>
    ControllerRunner<'_, I, T, N, RX, TX>
{
    /// Drives the bus until the future is canceled.
    ///
    /// It never returns. Route faults are not returned: their ports fail with
    /// a matching [`PortError`], and the runner keeps serving the other routes.
    /// Canceling (dropping) this future tears the runner down: the ports then
    /// fail with [`PortError::Closed`]. A transaction interrupted this way is
    /// not resumed; start over by building again.
    pub async fn run(self) -> Infallible {
        self.run_observed(|_| {}).await
    }

    /// As [`run`](Self::run), with `observer` told of every route fault as it
    /// happens, with the concrete HAL error where there is one.
    pub async fn run_observed(
        mut self,
        mut observer: impl FnMut(RouteEvent<I::Error>),
    ) -> Infallible {
        loop {
            self.turn(&mut observer).await;
        }
    }

    /// Services the next due route, or waits when none is due.
    ///
    /// Routes that were quiet for a full cycle make the runner wait as well.
    async fn turn(&mut self, observer: &mut impl FnMut(RouteEvent<I::Error>)) {
        let now = self.timer.now();
        let Some(index) = self.next_route(now) else {
            self.idle_turns = 0;
            self.idle().await;
            return;
        };
        let peer = self.routes[index]
            .as_mut()
            .expect("scheduled route is open");
        let step = service(
            &mut self.i2c,
            peer,
            &mut self.epochs,
            self.settings,
            &self.timer,
            observer,
        )
        .await;
        match step {
            Step::Transferred | Step::Transitioned | Step::Ended => self.idle_turns = 0,
            Step::Idle | Step::Attempted => {
                self.idle_turns += 1;
                if self.idle_turns >= self.open_routes() {
                    self.idle_turns = 0;
                    self.idle().await;
                }
            }
        }
    }

    /// Picks the next open route that is due, in round-robin order.
    fn next_route(&mut self, now: Duration) -> Option<usize> {
        for offset in 0..N {
            let index = (self.cursor + offset) % N;
            if self.routes[index]
                .as_ref()
                .is_some_and(|peer| peer.due(now))
            {
                self.cursor = (index + 1) % N;
                return Some(index);
            }
        }
        None
    }

    fn open_routes(&self) -> usize {
        self.routes.iter().flatten().count()
    }

    /// Waits until the poll interval has passed, the earliest retry deadline is
    /// reached, or local work makes the next turn useful: bytes to send on a
    /// route with credit, room to receive on a route whose last read was skipped
    /// for lack of room, or an application event such as an attach or an abort.
    /// Blocked state is remembered rather than re-derived, so room that appears
    /// between the wakeup and the poll still counts.
    ///
    /// A deadline that has already passed, such as a zero interval, only yields.
    async fn idle(&mut self) {
        let now = self.timer.now();
        let mut deadline = now + self.settings.poll_interval;
        for retry in self
            .routes
            .iter()
            .flatten()
            .filter_map(|peer| peer.retry_at)
        {
            deadline = deadline.min(retry);
        }
        if deadline <= now {
            yield_now().await;
            return;
        }
        let mut sleep = pin!(self.timer.sleep_until(deadline));
        let routes = &mut self.routes;
        poll_fn(|cx| {
            if sleep.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
            for peer in routes.iter_mut().flatten() {
                if peer.ends.poll_app_event(cx).is_ready() {
                    return Poll::Ready(());
                }
                if peer.credit > 0 && peer.ends.poll_send(cx).is_ready() {
                    return Poll::Ready(());
                }
                if peer.recv_blocked
                    && peer
                        .ends
                        .poll_recv_space(cx, MAILBOX_PAYLOAD_MAX)
                        .is_ready()
                {
                    return Poll::Ready(());
                }
            }
            Poll::Pending
        })
        .await
    }
}

/// Yields to the executor once, without waiting for anything.
async fn yield_now() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

/// Services one route for one turn. `timer` is read again after each opening
/// transaction, since a retry counts from when that transaction completed.
async fn service<I: I2c, T: Clock, const RX: usize, const TX: usize>(
    i2c: &mut I,
    peer: &mut Peer<'_, RX, TX>,
    epochs: &mut Epochs<'_>,
    settings: Settings,
    timer: &T,
    observer: &mut impl FnMut(RouteEvent<I::Error>),
) -> Step {
    match peer.stage {
        Stage::Idle => start(peer),
        Stage::Ended => finish(peer),
        _ if peer.ends.aborted() => abort(i2c, peer).await,
        Stage::Discovering | Stage::Opening => {
            connect(i2c, peer, epochs, settings, timer, observer).await
        }
        Stage::Serving => serve(i2c, peer, observer).await,
    }
}

/// An idle route whose port is attached starts a generation, which begins by
/// discovering its target.
///
/// An abort seen while idle ends the generation here, with no bus traffic. An
/// idle route has no discovered connection, so nothing on the wire is retired.
/// The route ends even while no port is attached: the aborted generation must
/// still finish, or every later attachment would wait for it.
fn start<const RX: usize, const TX: usize>(peer: &mut Peer<'_, RX, TX>) -> Step {
    if peer.ends.aborted() {
        peer.ends.fail(PortError::Reset);
        peer.stage = Stage::Ended;
        peer.retry_at = None;
        return Step::Ended;
    }
    if !peer.ends.is_attached() {
        return Step::Idle;
    }
    peer.stage = Stage::Discovering;
    peer.attempts = 0;
    peer.confirm_owed = false;
    peer.retry_at = None;
    peer.credit = 0;
    peer.recv_blocked = false;
    Step::Transitioned
}

/// An ended route becomes idle once its port is dropped, which finishes the
/// generation.
fn finish<const RX: usize, const TX: usize>(peer: &mut Peer<'_, RX, TX>) -> Step {
    let stage = &mut peer.stage;
    if peer.ends.try_finalize(|_| *stage = Stage::Idle) {
        Step::Transitioned
    } else {
        Step::Idle
    }
}

/// The port was aborted: the route ends at once, and its connection is retired
/// once, best effort.
async fn abort<I: I2c, const RX: usize, const TX: usize>(
    i2c: &mut I,
    peer: &mut Peer<'_, RX, TX>,
) -> Step {
    // The terminal state is published first: the retirement is optional cleanup
    // and must not delay it. Before discovery there is no connection to retire.
    let connection =
        (peer.stage != Stage::Discovering).then(|| ConnectionId::new(peer.boot, peer.epoch));
    peer.ends.fail(PortError::Reset);
    peer.stage = Stage::Ended;
    peer.retry_at = None;
    if let Some(connection) = connection {
        retire_best_effort(i2c, peer.address, connection).await;
    }
    Step::Ended
}

/// One turn of an opening route: read the target, then send `OPEN` unless the
/// reply confirms the connection.
async fn connect<I: I2c, T: Clock, const RX: usize, const TX: usize>(
    i2c: &mut I,
    peer: &mut Peer<'_, RX, TX>,
    epochs: &mut Epochs<'_>,
    settings: Settings,
    timer: &T,
    observer: &mut impl FnMut(RouteEvent<I::Error>),
) -> Step {
    // A failed read spends the budget, so a read is due only while budget
    // remains, or once more to confirm the final permitted `OPEN`.
    if peer.attempts >= settings.open_attempts && !peer.confirm_owed {
        peer.end(RouteFault::OpenExhausted, observer);
        return Step::Ended;
    }
    peer.confirm_owed = false;
    let frame = match read_mailbox(i2c, peer.address).await {
        Ok(frame) => frame,
        Err(error) => {
            peer.attempts += 1;
            observer(RouteEvent {
                address: peer.address.get(),
                fault: RouteFault::OpenFailed(error),
            });
            peer.retry_at = Some(timer.now() + settings.open_retry_interval);
            return Step::Attempted;
        }
    };
    let reply = match mailbox::decode_reply(&frame) {
        Ok(reply) => reply,
        Err(error) => {
            peer.end(RouteFault::Mailbox(error), observer);
            return Step::Ended;
        }
    };
    if peer.stage == Stage::Discovering {
        // Discovery supplies identity only. The reply's payload and credit belong
        // to the target's previous connection, so they are dropped.
        peer.boot = reply.connection.boot();
        peer.epoch = epochs.allocate(peer.address, reply.connection.epoch());
        peer.stage = Stage::Opening;
        return match open(i2c, peer, settings, timer, observer).await {
            Step::Ended => Step::Ended,
            _ => Step::Transitioned,
        };
    }
    if reply.connection.boot() != peer.boot {
        let fault = RouteFault::Rebooted {
            expected: peer.boot,
            found: reply.connection.boot(),
        };
        peer.end(fault, observer);
        return Step::Ended;
    }
    let ours = ConnectionId::new(peer.boot, peer.epoch);
    // A retirement of our connection ends it before its payload or credit is
    // taken, even when the reply also says the target serves it.
    if reply.retire && reply.connection == ours {
        peer.end(RouteFault::Retired, observer);
        return Step::Ended;
    }
    if reply.serving && reply.connection == ours {
        peer.stage = Stage::Serving;
        peer.retry_at = None;
        peer.credit = reply.rx_credit;
        let pushed = peer.ends.push_recv(reply.payload);
        debug_assert_eq!(pushed, reply.payload.len());
        return if reply.payload.is_empty() {
            Step::Transitioned
        } else {
            Step::Transferred
        };
    }
    open(i2c, peer, settings, timer, observer).await
}

/// Writes one `OPEN` for the route's connection, unless the budget is spent.
///
/// Every write spends budget, whether it succeeds or not. An `OPEN` that is not
/// confirmed schedules the route's next attempt, which counts from the write's
/// completion.
async fn open<I: I2c, T: Clock, const RX: usize, const TX: usize>(
    i2c: &mut I,
    peer: &mut Peer<'_, RX, TX>,
    settings: Settings,
    timer: &T,
    observer: &mut impl FnMut(RouteEvent<I::Error>),
) -> Step {
    if peer.attempts >= settings.open_attempts {
        peer.end(RouteFault::OpenExhausted, observer);
        return Step::Ended;
    }
    peer.attempts += 1;
    // The next read may confirm this, the final permitted, `OPEN`.
    peer.confirm_owed = peer.attempts == settings.open_attempts;
    let mut frame = [0u8; MAILBOX_SIZE];
    let len = mailbox::encode_request(
        ConnectionId::new(peer.boot, peer.epoch),
        Request::Open,
        &mut frame,
    )
    .expect("epochs are never zero");
    if let Err(error) = write_transaction(i2c, peer.address, &frame[..len]).await {
        observer(RouteEvent {
            address: peer.address.get(),
            fault: RouteFault::OpenFailed(error),
        });
    }
    peer.retry_at = Some(timer.now() + settings.open_retry_interval);
    Step::Attempted
}

/// One turn of a serving route: take one reply if there is room for a whole
/// payload, then write as much queued data as the target's credit allows.
async fn serve<I: I2c, const RX: usize, const TX: usize>(
    i2c: &mut I,
    peer: &mut Peer<'_, RX, TX>,
    observer: &mut impl FnMut(RouteEvent<I::Error>),
) -> Step {
    let mut step = Step::Idle;
    peer.recv_blocked = peer.ends.recv_free() < MAILBOX_PAYLOAD_MAX;
    if !peer.recv_blocked {
        let frame = match read_mailbox(i2c, peer.address).await {
            Ok(frame) => frame,
            Err(error) => {
                peer.end(RouteFault::Bus(error), observer);
                return Step::Ended;
            }
        };
        let reply = match mailbox::decode_reply(&frame) {
            Ok(reply) => reply,
            Err(error) => {
                peer.end(RouteFault::Mailbox(error), observer);
                return Step::Ended;
            }
        };
        if !absorb(peer, &reply, observer) {
            return Step::Ended;
        }
        if !reply.payload.is_empty() {
            step = Step::Transferred;
        }
    }

    if peer.credit > 0 && peer.ends.send_len() > 0 {
        let len = mailbox::write_len(peer.credit, peer.ends.send_len());
        let mut chunk = [0u8; MAILBOX_PAYLOAD_MAX];
        peer.ends.peek_send(&mut chunk[..len]);
        let mut frame = [0u8; MAILBOX_SIZE];
        let transaction = mailbox::encode_request(
            ConnectionId::new(peer.boot, peer.epoch),
            Request::Data(&chunk[..len]),
            &mut frame,
        )
        .expect("a chunk is at most one mailbox payload");
        match write_transaction(i2c, peer.address, &frame[..transaction]).await {
            // Only a completed transaction removes the bytes.
            Ok(()) => {
                peer.credit -= len as u8;
                peer.ends.consume_send(len);
                step = Step::Transferred;
            }
            Err(error) => {
                peer.end(RouteFault::Bus(error), observer);
                return Step::Ended;
            }
        }
    }
    step
}

/// Takes a reply read by a serving route. Returns `false` when the reply ended
/// the connection, in which case neither its payload nor its credit is taken.
fn absorb<E, const RX: usize, const TX: usize>(
    peer: &mut Peer<'_, RX, TX>,
    reply: &Reply<'_>,
    observer: &mut impl FnMut(RouteEvent<E>),
) -> bool {
    if reply.connection.boot() != peer.boot {
        let fault = RouteFault::Rebooted {
            expected: peer.boot,
            found: reply.connection.boot(),
        };
        peer.end(fault, observer);
        return false;
    }
    let ours = ConnectionId::new(peer.boot, peer.epoch);
    if reply.retire && reply.connection == ours {
        peer.end(RouteFault::Retired, observer);
        return false;
    }
    if reply.serving && reply.connection == ours {
        peer.credit = reply.rx_credit;
        let pushed = peer.ends.push_recv(reply.payload);
        debug_assert_eq!(pushed, reply.payload.len());
        return true;
    }
    let fault = if reply.connection.epoch() != peer.epoch {
        RouteFault::Mismatch(reply.connection)
    } else {
        RouteFault::NotServing
    };
    peer.end(fault, observer);
    false
}

/// Asks the target to retire the connection once, ignoring the outcome. The
/// retirement is optional: the target also learns of the end when the route
/// is replaced or its boot changes.
async fn retire_best_effort<I: I2c>(i2c: &mut I, address: Address, connection: ConnectionId) {
    let mut frame = [0u8; MAILBOX_SIZE];
    if let Ok(len) = mailbox::encode_request(connection, Request::Retire, &mut frame) {
        let _ = write_transaction(i2c, address, &frame[..len]).await;
    }
}
