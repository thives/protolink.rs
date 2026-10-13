//! A simulated I2C bus: a controller-side `I2c`, a target-side `I2cTarget`,
//! and a scripted controller-side bus, all single threaded.
#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use embedded_hal::i2c::{ErrorKind, ErrorType, Operation};
use embedded_hal_async::delay::DelayNs;
use embedded_hal_async::i2c::I2c;
use protolink_i2c::mailbox::{Frame, MAILBOX_SIZE};
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

/// A delay that records each requested wait and yields once.
#[derive(Clone, Default)]
pub struct RecordingDelay {
    pub waits_us: Rc<RefCell<Vec<u32>>>,
}

impl DelayNs for RecordingDelay {
    async fn delay_ns(&mut self, ns: u32) {
        self.waits_us.borrow_mut().push(ns / 1000);
        YieldOnce::default().await;
    }
}

/// A delay that never completes: only early wakeups can make progress.
#[derive(Clone, Copy, Default)]
pub struct NeverDelay;

impl DelayNs for NeverDelay {
    async fn delay_ns(&mut self, _ns: u32) {
        std::future::pending::<()>().await;
    }
}

/// A real-time delay for tests that run protolink's timers too.
#[derive(Clone, Copy, Default)]
pub struct TokioDelay;

impl DelayNs for TokioDelay {
    async fn delay_ns(&mut self, ns: u32) {
        tokio::time::sleep(std::time::Duration::from_nanos(ns.into())).await;
    }
}

/// A frame that carries no payload and advertises `credit`.
pub fn idle_frame(credit: u8) -> Frame {
    let mut frame = [0u8; MAILBOX_SIZE];
    frame[1] = credit;
    frame
}

/// A frame carrying `payload` and advertising `credit`.
pub fn payload_frame(payload: &[u8], credit: u8) -> Frame {
    let mut frame = idle_frame(credit);
    frame[0] = payload.len() as u8;
    frame[2..2 + payload.len()].copy_from_slice(payload);
    frame
}

/// A deterministic byte pattern, so ordering and content can be checked.
pub fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

// --- A controller and a target connected by a simulated bus ----------------

#[derive(Debug)]
enum Request {
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
    request: Option<Request>,
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
                        Request::Read => Command::Read,
                        Request::Write(data) => {
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
                        bus.request = Some(Request::Read);
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
                        bus.request = Some(Request::Write(data.to_vec()));
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

// --- A scripted controller-side bus ----------------------------------------

#[derive(Default)]
struct Script {
    responses: HashMap<u8, VecDeque<Frame>>,
    reads: Vec<u8>,
    writes: Vec<(u8, Vec<u8>)>,
    fail_reads: bool,
    fail_writes: bool,
    /// Reads from this (0-based) read index on fail.
    fail_reads_from: Option<usize>,
}

/// A bus whose read responses are queued per address. Reads with no queued
/// response see an idle target advertising full credit.
#[derive(Clone, Default)]
pub struct Scripted(Rc<RefCell<Script>>);

impl Scripted {
    pub fn respond(&self, address: u8, frame: Frame) {
        self.0
            .borrow_mut()
            .responses
            .entry(address)
            .or_default()
            .push_back(frame);
    }

    pub fn set_fail_reads(&self, fail: bool) {
        self.0.borrow_mut().fail_reads = fail;
    }

    /// Makes every read fail once `n` reads have succeeded.
    pub fn fail_reads_after(&self, n: usize) {
        self.0.borrow_mut().fail_reads_from = Some(n);
    }

    pub fn set_fail_writes(&self, fail: bool) {
        self.0.borrow_mut().fail_writes = fail;
    }

    /// The address of every read, in order.
    pub fn reads(&self) -> Vec<u8> {
        self.0.borrow().reads.clone()
    }

    /// Every successful write, in order.
    pub fn writes(&self) -> Vec<(u8, Vec<u8>)> {
        self.0.borrow().writes.clone()
    }
}

impl ErrorType for Scripted {
    type Error = BusFault;
}

impl I2c for Scripted {
    async fn transaction(
        &mut self,
        address: u8,
        operations: &mut [Operation<'_>],
    ) -> Result<(), BusFault> {
        YieldOnce::default().await;
        let mut script = self.0.borrow_mut();
        for operation in operations.iter_mut() {
            match operation {
                Operation::Read(buffer) => {
                    let index = script.reads.len();
                    script.reads.push(address);
                    if script.fail_reads || script.fail_reads_from.is_some_and(|n| index >= n) {
                        return Err(BusFault);
                    }
                    let frame = script
                        .responses
                        .get_mut(&address)
                        .and_then(VecDeque::pop_front)
                        .unwrap_or(idle_frame(255));
                    buffer.copy_from_slice(&frame[..buffer.len()]);
                }
                Operation::Write(data) => {
                    if script.fail_writes {
                        return Err(BusFault);
                    }
                    script.writes.push((address, data.to_vec()));
                }
            }
        }
        Ok(())
    }
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
                buffer.copy_from_slice(&idle_frame(255)[..buffer.len()]);
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
