//! A simulated I2C bus: a controller-side `I2c`, a target-side `I2cTarget`,
//! and a scripted controller-side bus, all single threaded.
#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use embedded_hal::i2c::{ErrorKind, ErrorType, Operation};
use embedded_hal_async::i2c::I2c;
use protolink_adaptors::{Clock, Timer};
use protolink_i2c::mailbox::{
    self, ConnectionId, Frame, MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE, Reply, Request,
};
use protolink_i2c::target::{Command, I2cTarget, ReadStatus};

/// The target address used by the simulated bus.
pub const ADDRESS: u8 = 0x42;

/// A bus failure injected by a test or raised by a fake driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BusFault;

impl embedded_hal::i2c::Error for BusFault {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Bus
    }
}

/// Fails the test if `f` takes longer than ten seconds.
pub async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("test timed out")
}

/// A future that is pending once and wakes itself, so other tasks run.
#[derive(Default)]
pub struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A timer on virtual time. Waiting moves the clock to the deadline, unless
/// the clock is already past it, and records the deadline. The clock is shared,
/// so a test can read it.
#[derive(Clone, Default)]
pub struct VirtualTimer {
    pub now: Rc<Cell<Duration>>,
    pub deadlines: Rc<RefCell<Vec<Duration>>>,
}

impl Clock for VirtualTimer {
    fn now(&self) -> Duration {
        self.now.get()
    }
}

impl Timer for VirtualTimer {
    fn sleep_until(&self, deadline: Duration) -> impl Future<Output = ()> {
        Wait {
            timer: self.clone(),
            deadline,
            started: false,
        }
    }
}

/// The wait of a [`VirtualTimer`]: it moves the clock on its first poll, then
/// yields once so that other tasks run.
struct Wait {
    timer: VirtualTimer,
    deadline: Duration,
    started: bool,
}

impl Future for Wait {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.started {
            return Poll::Ready(());
        }
        self.started = true;
        let deadline = self.deadline;
        self.timer.deadlines.borrow_mut().push(deadline);
        if deadline > self.timer.now.get() {
            self.timer.now.set(deadline);
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A timer whose clock never moves and whose waits never end: only an early
/// wakeup can make the runner progress.
#[derive(Clone, Copy, Default)]
pub struct NeverTimer;

impl Clock for NeverTimer {
    fn now(&self) -> Duration {
        Duration::ZERO
    }
}

impl Timer for NeverTimer {
    fn sleep_until(&self, _deadline: Duration) -> impl Future<Output = ()> {
        std::future::pending::<()>()
    }
}

/// A real-time timer for tests that run protolink's timers too.
#[derive(Clone, Copy)]
pub struct TokioTimer {
    origin: std::time::Instant,
}

impl Default for TokioTimer {
    fn default() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

impl Clock for TokioTimer {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

impl Timer for TokioTimer {
    fn sleep_until(&self, deadline: Duration) -> impl Future<Output = ()> {
        let wait = deadline.saturating_sub(self.now());
        async move { tokio::time::sleep(wait).await }
    }
}

/// A deterministic byte pattern, so ordering and content can be checked.
pub fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

// --- A controller and a target connected by a simulated bus ----------------

#[derive(Debug)]
enum BusRequest {
    Read,
    Write(Vec<u8>),
}

/// How the next `respond_to_read` ends, instead of `Done`.
#[derive(Clone, Copy)]
pub enum RespondFault {
    Short,
    Leftover,
}

#[derive(Default)]
struct Bus {
    request: Option<BusRequest>,
    response: Option<Frame>,
    listen_fault: bool,
    respond_fault: Option<RespondFault>,
    listen_waker: Option<Waker>,
    controller_waker: Option<Waker>,
    target_waker: Option<Waker>,
    disarmed: usize,
}

#[derive(Clone, Default)]
pub struct SharedBus(Rc<RefCell<Bus>>);

fn wake(slot: &mut Option<Waker>) {
    if let Some(waker) = slot.take() {
        waker.wake();
    }
}

impl SharedBus {
    pub fn inject_listen_fault(&self) {
        let mut bus = self.0.borrow_mut();
        bus.listen_fault = true;
        wake(&mut bus.listen_waker);
    }

    pub fn respond_with(&self, fault: RespondFault) {
        self.0.borrow_mut().respond_fault = Some(fault);
    }

    /// How many times the target was disarmed.
    pub fn disarmed(&self) -> usize {
        self.0.borrow().disarmed
    }
}

/// The target end of the bus.
pub struct SimTarget(pub SharedBus);

impl I2cTarget for SimTarget {
    type Error = BusFault;

    fn disarm(&mut self) {
        self.0.0.borrow_mut().disarmed += 1;
    }

    async fn listen(&mut self, buffer: &mut [u8]) -> Result<Command, BusFault> {
        poll_fn(|cx| {
            let mut bus = self.0.0.borrow_mut();
            if std::mem::take(&mut bus.listen_fault) {
                return Poll::Ready(Err(BusFault));
            }
            match bus.request.take() {
                Some(request) => {
                    wake(&mut bus.controller_waker);
                    Poll::Ready(Ok(match request {
                        BusRequest::Read => Command::Read,
                        BusRequest::Write(data) => {
                            buffer[..data.len()].copy_from_slice(&data);
                            Command::Write(data.len())
                        }
                    }))
                }
                None => {
                    bus.listen_waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await
    }

    async fn respond_to_read(&mut self, response: &[u8]) -> Result<ReadStatus, BusFault> {
        let mut frame = [0u8; MAILBOX_SIZE];
        frame[..response.len()].copy_from_slice(response);
        let mut status = Some({
            let mut bus = self.0.0.borrow_mut();
            bus.response = Some(frame);
            wake(&mut bus.controller_waker);
            match bus.respond_fault.take() {
                Some(RespondFault::Short) => ReadStatus::NeedMoreBytes,
                Some(RespondFault::Leftover) => ReadStatus::LeftoverBytes(1),
                None => ReadStatus::Done,
            }
        });
        poll_fn(|cx| {
            let mut bus = self.0.0.borrow_mut();
            if bus.response.is_none() {
                return Poll::Ready(Ok(status.take().expect("polled after completion")));
            }
            bus.target_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

/// The controller end of the bus, as a raw I2C controller.
pub struct SimController(pub SharedBus);

impl ErrorType for SimController {
    type Error = BusFault;
}

impl I2c for SimController {
    async fn transaction(
        &mut self,
        address: u8,
        operations: &mut [Operation<'_>],
    ) -> Result<(), BusFault> {
        assert_eq!(address, ADDRESS, "the simulated bus has one target");
        for operation in operations.iter_mut() {
            match operation {
                Operation::Read(buffer) => {
                    {
                        let mut bus = self.0.0.borrow_mut();
                        bus.request = Some(BusRequest::Read);
                        wake(&mut bus.listen_waker);
                    }
                    let frame = poll_fn(|cx| {
                        let mut bus = self.0.0.borrow_mut();
                        match bus.response.take() {
                            Some(frame) => {
                                wake(&mut bus.target_waker);
                                Poll::Ready(frame)
                            }
                            None => {
                                bus.controller_waker = Some(cx.waker().clone());
                                Poll::Pending
                            }
                        }
                    })
                    .await;
                    buffer.copy_from_slice(&frame[..buffer.len()]);
                }
                Operation::Write(data) => {
                    {
                        let mut bus = self.0.0.borrow_mut();
                        bus.request = Some(BusRequest::Write(data.to_vec()));
                        wake(&mut bus.listen_waker);
                    }
                    poll_fn(|cx| {
                        let mut bus = self.0.0.borrow_mut();
                        if bus.request.is_none() {
                            return Poll::Ready(());
                        }
                        bus.controller_waker = Some(cx.waker().clone());
                        Poll::Pending
                    })
                    .await;
                }
            }
        }
        Ok(())
    }
}

// --- A simulated target that speaks the mailbox protocol ------------------

/// The boot identifier the simulated targets start with.
pub const BOOT: u16 = 0x2A17;

/// One request a controller wrote to a simulated target, decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sent {
    Open(ConnectionId),
    Data(ConnectionId, usize),
    Retire(ConnectionId),
}

/// A simulated target with the mailbox protocol built in: it adopts an `OPEN`
/// at once, accepts data of the connection it serves, and answers reads with
/// encoded replies. Tests steer it with the fields and helpers below.
pub struct Wire {
    pub boot: u16,
    /// The connection the target serves, if any.
    pub served: Option<ConnectionId>,
    /// The target is retiring its connection: replies carry no serving flag and
    /// ask for retirement.
    pub closing: bool,
    /// Credit advertised in replies.
    pub credit: u8,
    /// `OPEN`s to ignore before adopting one.
    pub refuse_open: usize,
    /// Every request the target decoded, in order.
    pub sent: Vec<Sent>,
    /// Data bytes accepted, in order.
    pub received: Vec<u8>,
    /// Bytes to deliver in replies to serving reads.
    pub outgoing: VecDeque<u8>,
    /// Frames returned verbatim by the next reads, before any encoded reply.
    pub raw_replies: VecDeque<Frame>,
    /// Reads performed so far.
    pub reads: usize,
    /// Reads from this (0-based) index on fail.
    pub fail_reads_from: Option<usize>,
    /// Writes fail; their bytes are not accepted.
    pub fail_writes: bool,
    /// Data writes fail, while `OPEN` and `RETIRE` still succeed.
    pub fail_data_writes: bool,
}

impl Wire {
    fn new() -> Self {
        Self {
            boot: BOOT,
            served: None,
            closing: false,
            credit: u8::MAX,
            refuse_open: 0,
            sent: Vec::new(),
            received: Vec::new(),
            outgoing: VecDeque::new(),
            raw_replies: VecDeque::new(),
            reads: 0,
            fail_reads_from: None,
            fail_writes: false,
            fail_data_writes: false,
        }
    }

    /// The target serves `epoch` from now on, as if it had been opened already.
    pub fn pre_serve(&mut self, epoch: u16) {
        self.served = Some(ConnectionId::new(self.boot, epoch));
    }

    /// Asks the controller to retire the connection, as an aborting target does.
    pub fn request_retire(&mut self) {
        self.closing = true;
    }

    fn reply(&mut self) -> Frame {
        let serving = self.served.is_some() && !self.closing;
        let take = if serving {
            self.outgoing.len().min(MAILBOX_PAYLOAD_MAX)
        } else {
            0
        };
        let payload: Vec<u8> = self.outgoing.drain(..take).collect();
        let connection = ConnectionId::new(
            self.boot,
            self.served.map_or(0, |connection| connection.epoch()),
        );
        let mut frame = [0u8; MAILBOX_SIZE];
        mailbox::encode_reply(
            &Reply {
                payload: &payload,
                rx_credit: self.credit,
                connection,
                serving,
                retire: self.closing && self.served.is_some(),
            },
            &mut frame,
        )
        .expect("a reply payload fits one frame");
        frame
    }

    fn read(&mut self, buffer: &mut [u8]) -> Result<(), BusFault> {
        let index = self.reads;
        self.reads += 1;
        if self.fail_reads_from.is_some_and(|from| index >= from) {
            return Err(BusFault);
        }
        let frame = match self.raw_replies.pop_front() {
            Some(frame) => frame,
            None => self.reply(),
        };
        buffer.copy_from_slice(&frame[..buffer.len()]);
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> Result<(), BusFault> {
        if self.fail_writes {
            return Err(BusFault);
        }
        let (connection, request) = mailbox::decode_request(data).map_err(|_| BusFault)?;
        match request {
            Request::Open => {
                if self.refuse_open > 0 {
                    self.refuse_open -= 1;
                } else if connection.boot() == self.boot {
                    self.served = Some(connection);
                    self.closing = false;
                }
                self.sent.push(Sent::Open(connection));
            }
            Request::Data(payload) => {
                if self.fail_data_writes {
                    return Err(BusFault);
                }
                if self.served == Some(connection) && !self.closing {
                    self.received.extend_from_slice(payload);
                }
                self.sent.push(Sent::Data(connection, payload.len()));
            }
            Request::Retire => {
                if self.served == Some(connection) {
                    self.served = None;
                }
                self.sent.push(Sent::Retire(connection));
            }
        }
        Ok(())
    }
}

/// A bus of simulated targets, keyed by address. `Send`, so that it can drive a
/// runner spawned on a multi-threaded executor.
#[derive(Clone, Default)]
pub struct WireBus(pub std::sync::Arc<std::sync::Mutex<HashMap<u8, Wire>>>);

/// A borrow of one simulated target. Do not hold it across an await.
pub struct WireGuard<'a> {
    guard: std::sync::MutexGuard<'a, HashMap<u8, Wire>>,
    address: u8,
}

impl std::ops::Deref for WireGuard<'_> {
    type Target = Wire;

    fn deref(&self) -> &Wire {
        &self.guard[&self.address]
    }
}

impl std::ops::DerefMut for WireGuard<'_> {
    fn deref_mut(&mut self) -> &mut Wire {
        self.guard
            .get_mut(&self.address)
            .expect("the peer was added")
    }
}

impl WireBus {
    /// Adds a target at `address`, with [`BOOT`] as its boot identifier.
    pub fn add(&self, address: u8) {
        self.0.lock().unwrap().insert(address, Wire::new());
    }

    /// The target at `address`.
    pub fn wire(&self, address: u8) -> WireGuard<'_> {
        let guard = self.0.lock().unwrap();
        assert!(guard.contains_key(&address), "the peer was added");
        WireGuard { guard, address }
    }
}

impl ErrorType for WireBus {
    type Error = BusFault;
}

impl I2c for WireBus {
    async fn transaction(
        &mut self,
        address: u8,
        operations: &mut [Operation<'_>],
    ) -> Result<(), BusFault> {
        YieldOnce::default().await;
        let mut wires = self.0.lock().unwrap();
        let wire = wires.get_mut(&address).ok_or(BusFault)?;
        for operation in operations.iter_mut() {
            match operation {
                Operation::Read(buffer) => wire.read(buffer)?,
                Operation::Write(data) => wire.write(data)?,
            }
        }
        Ok(())
    }
}

/// An encoded reply of a target that serves nothing: unbound, no flags.
pub fn unbound_frame(boot: u16) -> Frame {
    let mut frame = [0u8; MAILBOX_SIZE];
    mailbox::encode_reply(
        &Reply {
            payload: &[],
            rx_credit: u8::MAX,
            connection: ConnectionId::new(boot, 0),
            serving: false,
            retire: false,
        },
        &mut frame,
    )
    .expect("an empty reply fits");
    frame
}

/// An I2C controller that is always idle and never fails, and is `Send`.
pub struct NullI2c;

impl ErrorType for NullI2c {
    type Error = BusFault;
}

impl I2c for NullI2c {
    async fn transaction(
        &mut self,
        _: u8,
        operations: &mut [Operation<'_>],
    ) -> Result<(), BusFault> {
        for operation in operations.iter_mut() {
            if let Operation::Read(buffer) = operation {
                buffer.copy_from_slice(&unbound_frame(BOOT)[..buffer.len()]);
            }
        }
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// An I2C target that never sees a transaction, and is `Send`.
pub struct NullTarget(pub std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl I2cTarget for NullTarget {
    type Error = BusFault;

    fn disarm(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    async fn listen(&mut self, _: &mut [u8]) -> Result<Command, BusFault> {
        std::future::pending().await
    }

    async fn respond_to_read(&mut self, _: &[u8]) -> Result<ReadStatus, BusFault> {
        std::future::pending().await
    }
}
