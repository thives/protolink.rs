use core::convert::Infallible;
use core::future::poll_fn;
use core::pin::pin;
use core::task::Poll;
use core::time::Duration;

use embedded_hal_async::delay::DelayNs;
use embedded_hal_async::i2c::I2c;
use protolink_i2c::controller::{read_mailbox, write_payload};
use protolink_i2c::mailbox::{self, MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE, MailboxError};
use protolink_i2c::{Address, InvalidAddress};

use crate::channel::{Channel, DEFAULT_BUFFER_SIZE, Ends, Port, PortError};
use crate::{Binding, NoStorage};

/// Default pause when no route has work: one millisecond.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Why a controller could not be built or a route could not be opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildError {
    /// The address is not a usable 7-bit address (`0x08..=0x77`).
    InvalidAddress(u8),
    /// A route to this address is already open on the controller.
    DuplicateAddress(u8),
    /// Every route of the [`ControllerState`] is already open.
    CapacityExhausted,
    /// The poll interval does not fit the delay driver's `u32` microseconds.
    PollIntervalTooLong,
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
            Self::PollIntervalTooLong => f.write_str("the poll interval is too long"),
        }
    }
}

impl core::error::Error for BuildError {}

/// Why a [`ControllerRunner`] stopped.
///
/// The concrete HAL error is preserved. Every port of the controller fails
/// with the matching [`PortError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerError<E> {
    /// An I2C transaction with the target at `address` failed.
    Bus {
        /// The target address.
        address: u8,
        /// The HAL error.
        error: E,
    },
    /// The target at `address` answered with a malformed mailbox frame.
    Mailbox {
        /// The target address.
        address: u8,
        /// What was wrong with the frame.
        error: MailboxError,
    },
}

impl<E: core::fmt::Debug> core::fmt::Display for ControllerError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Bus { address, error } => {
                write!(f, "I2C transaction with {address:#04x} failed: {error:?}")
            }
            Self::Mailbox { address, error } => {
                write!(f, "target {address:#04x} sent a bad mailbox: {error}")
            }
        }
    }
}

impl<E: core::fmt::Debug> core::error::Error for ControllerError<E> {}

/// Storage for the routes of one I2C controller bus.
///
/// `N` is the number of peers (routes). `RX` and `TX` are the sizes of each
/// port's receive and send buffers and default to [`DEFAULT_BUFFER_SIZE`];
/// use [`with_buffers`](Self::with_buffers) to choose others. The state holds
/// no HAL driver, so it can sit in a `static` without naming one.
///
/// A built controller borrows the state mutably for as long as any of its
/// ports or its runner is alive. Afterwards the state can be used for a new
/// build; queued bytes are discarded at that point.
///
/// `RX` must hold a full mailbox payload ([`MAILBOX_PAYLOAD_MAX`] bytes),
/// otherwise the controller could never read.
pub struct ControllerState<
    const N: usize,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    channels: [Channel<RX, TX>; N],
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
        }
    }
}

/// Starts building an I2C controller adaptor on `i2c`.
///
/// `delay` paces polling when no peer has work. It is unrelated to the ARQ
/// retransmission timer and to RPC deadlines.
///
/// Configure the factory with `with_*` methods, attach the caller-owned
/// storage with [`with_storage`](ControllerBuilder::with_storage), then
/// [`build`](ControllerBuilder::build) it. For one peer, choose its address
/// with [`with_address`](ControllerBuilder::with_address) and `build` returns
/// the port and the runner:
///
/// ```no_run
/// use protolink_adaptors::{Binding, ControllerState, i2c_controller};
/// use embedded_hal_async::{delay::DelayNs, i2c::I2c};
///
/// async fn demo<I: I2c, D: DelayNs>(i2c: I, delay: D) {
///     let mut state = ControllerState::<1>::new();
///     let Binding { port, runner } = i2c_controller(i2c, delay)
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
/// # use embedded_hal_async::{delay::DelayNs, i2c::I2c};
/// # fn demo<I: I2c, D: DelayNs>(i2c: I, delay: D) {
/// let binding = protolink_adaptors::i2c_controller(i2c, delay)
///     .with_address(0x42)
///     .build();
/// # }
/// ```
pub fn i2c_controller<I: I2c, D: DelayNs>(i2c: I, delay: D) -> ControllerBuilder<I, D> {
    ControllerBuilder {
        i2c,
        delay,
        poll_interval: DEFAULT_POLL_INTERVAL,
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
pub struct ControllerBuilder<I, D, P = AnyPeers, S = NoStorage> {
    i2c: I,
    delay: D,
    poll_interval: Duration,
    peers: P,
    storage: S,
}

impl<I: I2c, D: DelayNs, P, S> ControllerBuilder<I, D, P, S> {
    /// How long the runner sleeps when no peer made progress on a full cycle.
    /// Local outgoing data wakes it early. Defaults to one millisecond.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Talk to a single peer at `address`; `build` then returns its port and
    /// the runner. Validated by `build`.
    pub fn with_address(self, address: u8) -> ControllerBuilder<I, D, SinglePeer, S> {
        ControllerBuilder {
            i2c: self.i2c,
            delay: self.delay,
            poll_interval: self.poll_interval,
            peers: SinglePeer { address },
            storage: self.storage,
        }
    }

    /// Attach the caller-owned storage, which the ports and the runner borrow.
    pub fn with_storage<const N: usize, const RX: usize, const TX: usize>(
        self,
        state: &mut ControllerState<N, RX, TX>,
    ) -> ControllerBuilder<I, D, P, &mut ControllerState<N, RX, TX>> {
        ControllerBuilder {
            i2c: self.i2c,
            delay: self.delay,
            poll_interval: self.poll_interval,
            peers: self.peers,
            storage: state,
        }
    }

    fn controller<'a, const N: usize, const RX: usize, const TX: usize>(
        i2c: I,
        delay: D,
        poll_interval: Duration,
        state: &'a mut ControllerState<N, RX, TX>,
    ) -> Result<Controller<'a, I, D, N, RX, TX>, BuildError> {
        let poll_us = u32::try_from(poll_interval.as_micros())
            .map_err(|_| BuildError::PollIntervalTooLong)?;
        let mut unopened = [const { None }; N];
        for (channel, slot) in state.channels.iter_mut().zip(unopened.iter_mut()) {
            *slot = Some(channel.split());
        }
        Ok(Controller {
            i2c,
            delay,
            poll_us,
            unopened,
            routes: [const { None }; N],
            used: 0,
        })
    }
}

impl<'a, I: I2c, D: DelayNs, const N: usize, const RX: usize, const TX: usize>
    ControllerBuilder<I, D, AnyPeers, &'a mut ControllerState<N, RX, TX>>
{
    /// Construct the [`Controller`] to open peers on.
    ///
    /// This validates the configuration and starts no I2C traffic;
    /// communication begins once the runner is driven.
    pub fn build(self) -> Result<Controller<'a, I, D, N, RX, TX>, BuildError> {
        Self::controller(self.i2c, self.delay, self.poll_interval, self.storage)
    }
}

impl<'a, I: I2c, D: DelayNs, const N: usize, const RX: usize, const TX: usize>
    ControllerBuilder<I, D, SinglePeer, &'a mut ControllerState<N, RX, TX>>
{
    /// Construct the controller, open the peer on its first route and return
    /// the port and the runner.
    ///
    /// This validates the configuration and the address and starts no I2C
    /// traffic; communication begins once the runner is driven.
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
    ) -> Result<Binding<Port<'a, RX, TX>, ControllerRunner<'a, I, D, N, RX, TX>>, BuildError> {
        let mut controller =
            Self::controller(self.i2c, self.delay, self.poll_interval, self.storage)?;
        let port = controller.open(self.peers.address)?;
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
    D,
    const N: usize,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    i2c: I,
    delay: D,
    poll_us: u32,
    unopened: [Option<(Port<'a, RX, TX>, Ends<'a, RX, TX>)>; N],
    routes: [Option<Route<'a, RX, TX>>; N],
    used: usize,
}

impl<'a, I: I2c, D: DelayNs, const N: usize, const RX: usize, const TX: usize>
    Controller<'a, I, D, N, RX, TX>
{
    /// Opens a route to the peer at `address`.
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
        let (port, ends) = self.unopened[self.used]
            .take()
            .expect("each slot is opened once, in order");
        self.routes[self.used] = Some(Route {
            address,
            ends,
            credit: 0,
            recv_blocked: false,
        });
        self.used += 1;
        Ok(port)
    }

    /// Consumes the controller and returns the one runner of the bus.
    pub fn into_runner(self) -> ControllerRunner<'a, I, D, N, RX, TX> {
        ControllerRunner {
            i2c: self.i2c,
            delay: self.delay,
            poll_us: self.poll_us,
            routes: self.routes,
            cursor: 0,
            idle_turns: 0,
        }
    }
}

/// An open route as seen by the runner.
struct Route<'a, const RX: usize, const TX: usize> {
    address: Address,
    ends: Ends<'a, RX, TX>,
    /// The receive credit the target last advertised, less what was written
    /// since.
    credit: u8,
    /// The last turn skipped the read because a full payload did not fit.
    recv_blocked: bool,
}

/// The one owner of an I2C controller bus.
///
/// [`run`](Self::run) polls every opened peer round-robin: it reads one
/// mailbox when the route has room for a full payload, queues the payload,
/// then writes at most one chunk allowed by the target's credit. Routes take
/// turns, so sustained traffic or backpressure on one cannot starve another.
/// Outgoing bytes leave the send buffer only after their transaction succeeded.
///
/// Construct it with [`i2c_controller`]. Nothing happens until `run` is
/// polled.
pub struct ControllerRunner<
    'a,
    I,
    D,
    const N: usize,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    i2c: I,
    delay: D,
    poll_us: u32,
    routes: [Option<Route<'a, RX, TX>>; N],
    cursor: usize,
    /// Consecutive turns that moved no data.
    idle_turns: usize,
}

impl<I: I2c, D: DelayNs, const N: usize, const RX: usize, const TX: usize>
    ControllerRunner<'_, I, D, N, RX, TX>
{
    /// Drives the bus until it fails.
    ///
    /// It never returns `Ok`. A failed or malformed transaction is terminal:
    /// the concrete error is returned and every port fails with a matching
    /// [`PortError`] once its buffered data is read. Nothing is retried.
    ///
    /// The runner is consumed, so canceling (dropping) this future tears the
    /// runner down: the ports then fail with [`PortError::Closed`]. A
    /// transaction interrupted this way is not resumed; start over by building
    /// again.
    pub async fn run(mut self) -> Result<Infallible, ControllerError<I::Error>> {
        loop {
            match self.turn().await {
                Ok(true) => self.idle().await,
                Ok(false) => {}
                Err(error) => {
                    let kind = match error {
                        ControllerError::Bus { .. } => PortError::Hardware,
                        ControllerError::Mailbox { .. } => PortError::Protocol,
                    };
                    for route in self.routes.iter().flatten() {
                        route.ends.fail(kind);
                    }
                    return Err(error);
                }
            }
        }
    }

    /// Services the next route. Returns `true` when every route went a full
    /// cycle without progress and the runner should wait.
    async fn turn(&mut self) -> Result<bool, ControllerError<I::Error>> {
        let Some(index) = self.next_route() else {
            return Ok(true);
        };
        let route = self.routes[index]
            .as_mut()
            .expect("scheduled route is open");
        let address = route.address;
        let mut progressed = false;

        // A read is only issued when the route can take a full payload.
        route.recv_blocked = route.ends.recv_free() < MAILBOX_PAYLOAD_MAX;
        if !route.recv_blocked {
            let frame = read_mailbox(&mut self.i2c, address)
                .await
                .map_err(|error| ControllerError::Bus {
                    address: address.get(),
                    error,
                })?;
            let response =
                mailbox::decode_response(&frame).map_err(|error| ControllerError::Mailbox {
                    address: address.get(),
                    error,
                })?;
            route.credit = response.rx_credit;
            if !response.payload.is_empty() {
                let pushed = route.ends.push_recv(response.payload);
                debug_assert_eq!(pushed, response.payload.len());
                progressed = true;
            }
        }

        let len = mailbox::write_len(route.credit, route.ends.send_len());
        if len > 0 {
            let mut chunk = [0u8; MAILBOX_SIZE];
            route.ends.peek_send(&mut chunk[..len]);
            write_payload(&mut self.i2c, address, &chunk[..len])
                .await
                .map_err(|error| ControllerError::Bus {
                    address: address.get(),
                    error,
                })?;
            // Only a completed transaction removes the bytes.
            route.credit -= len as u8;
            route.ends.consume_send(len);
            progressed = true;
        }

        if progressed {
            self.idle_turns = 0;
            return Ok(false);
        }
        self.idle_turns += 1;
        if self.idle_turns >= self.open_routes() {
            self.idle_turns = 0;
            return Ok(true);
        }
        Ok(false)
    }

    /// Picks the next open route in round-robin order.
    fn next_route(&mut self) -> Option<usize> {
        for offset in 0..N {
            let index = (self.cursor + offset) % N;
            if self.routes[index].is_some() {
                self.cursor = (index + 1) % N;
                return Some(index);
            }
        }
        None
    }

    fn open_routes(&self) -> usize {
        self.routes.iter().flatten().count()
    }

    /// Sleeps for the poll interval, or until local work makes the next turn
    /// useful: bytes to send on a route with credit, or room to receive on a
    /// route whose last read was skipped for lack of room. The blocked state
    /// is remembered rather than re-derived, so room that appears between the
    /// wakeup and the poll still counts.
    async fn idle(&mut self) {
        let mut sleep = pin!(self.delay.delay_us(self.poll_us));
        let routes = &mut self.routes;
        poll_fn(|cx| {
            if sleep.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
            for route in routes.iter_mut().flatten() {
                if route.credit > 0 && route.ends.poll_send(cx).is_ready() {
                    return Poll::Ready(());
                }
                if route.recv_blocked
                    && route
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
