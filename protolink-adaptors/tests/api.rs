//! The public construction paths, compiled and run: single peer, target and
//! several peers, with local and `static` storage, and ports and runners moved
//! into separate tasks.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use common::*;
use embedded_io_async::{Read, Write};
use protolink_adaptors::{
    Attachment, Binding, BuildError, ControllerRunner, ControllerState, DEFAULT_BUFFER_SIZE, Port,
    PortError, PortReader, PortWriter, TargetRunner, TargetState, i2c_controller, i2c_target,
};
use static_cell::StaticCell;

#[tokio::test]
async fn single_peer_controller_with_local_storage() {
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(NullI2c, TokioTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    // Constructing starts nothing; the port is idle until the runner is driven.
    let mut buf = [0u8; 4];
    let idle = tokio::time::timeout(Duration::from_millis(20), port.read(&mut buf)).await;
    assert!(idle.is_err(), "an empty mailbox is idle, not EOF");
    drop(runner);
    // With the runner gone the port fails instead of hanging.
    assert_eq!(port.read(&mut buf).await, Err(PortError::Closed));
    assert_eq!(port.write(b"x").await, Err(PortError::Closed));
}

#[tokio::test]
async fn target_with_local_storage() {
    let disarmed = Arc::new(AtomicUsize::new(0));
    let mut state = TargetState::new();
    let Binding { port, runner } = i2c_target(NullTarget(disarmed.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    drop(runner);
    assert_eq!(disarmed.load(SeqCst), 1, "dropping the runner disarms");
    let _ = port;
}

static CONTROLLER_STATE: StaticCell<ControllerState<2>> = StaticCell::new();
static TARGET_STATE: StaticCell<TargetState> = StaticCell::new();

async fn application(mut port: Port<'static>) -> Result<(), PortError> {
    port.write(b"ping").await?;
    port.flush().await
}

#[tokio::test]
async fn static_storage_and_separate_tasks() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.add(0x43);
    let state = CONTROLLER_STATE.init(ControllerState::new());
    let mut controller = i2c_controller(bus.clone(), TokioTimer::default())
        .with_storage(state)
        .build()
        .unwrap();
    let port = controller.open(0x42).unwrap();
    let _second = controller.open(0x43).unwrap();
    let runner = controller.into_runner();

    // Ports and runners are `Send + 'static`, so each can be its own task.
    let runner_task = tokio::spawn(runner.run());
    let app_task = tokio::spawn(application(port));
    app_task.await.unwrap().unwrap();
    runner_task.abort();

    let disarmed = Arc::new(AtomicUsize::new(0));
    let state = TARGET_STATE.init(TargetState::new());
    let Binding { port, runner } = i2c_target(NullTarget(disarmed.clone()), BOOT)
        .with_storage(state)
        .build();
    let runner_task = tokio::spawn(runner.run());
    tokio::task::yield_now().await;
    runner_task.abort();
    let _ = runner_task.await;
    assert_eq!(disarmed.load(SeqCst), 1, "canceling the task disarms");
    let _ = port;
}

#[test]
fn bind_validates_addresses_duplicates_and_capacity() {
    let mut state = ControllerState::<2>::new();
    let mut controller = i2c_controller(NullI2c, TokioTimer::default())
        .with_storage(&mut state)
        .build()
        .unwrap();
    assert!(matches!(
        controller.open(0x00),
        Err(BuildError::InvalidAddress(0x00))
    ));
    assert!(matches!(
        controller.open(0x78),
        Err(BuildError::InvalidAddress(0x78))
    ));
    let _a = controller.open(0x42).unwrap();
    assert!(matches!(
        controller.open(0x42),
        Err(BuildError::DuplicateAddress(0x42))
    ));
    let _b = controller.open(0x43).unwrap();
    assert!(matches!(
        controller.open(0x44),
        Err(BuildError::CapacityExhausted)
    ));

    // The single-peer path validates too.
    let mut state = ControllerState::<1>::new();
    assert!(matches!(
        i2c_controller(NullI2c, TokioTimer::default())
            .with_address(0x01)
            .with_storage(&mut state)
            .build(),
        Err(BuildError::InvalidAddress(0x01))
    ));
}

#[test]
fn state_is_reusable_once_everything_borrowed_is_dropped() {
    let mut state = ControllerState::<1>::new();
    for _ in 0..2 {
        let Binding { port, runner } = i2c_controller(NullI2c, TokioTimer::default())
            .with_address(0x42)
            .with_storage(&mut state)
            .build()
            .unwrap();
        drop((port, runner));
    }
    let mut target = TargetState::new();
    for _ in 0..2 {
        let binding = i2c_target(NullTarget(Arc::default()), BOOT)
            .with_storage(&mut target)
            .build();
        drop(binding);
    }
}

#[test]
fn advanced_buffer_sizes_are_explicit() {
    let mut state = ControllerState::<1, 128, 32>::with_buffers();
    let Binding { port, runner } = i2c_controller(NullI2c, TokioTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let _: &Port<'_, 128, 32> = &port;
    drop(runner);
    assert_eq!(DEFAULT_BUFFER_SIZE, 64);
}

/// Hardware that must not be used: building only wires up storage.
struct Untouchable;

impl embedded_hal_async::i2c::ErrorType for Untouchable {
    type Error = BusFault;
}

impl embedded_hal_async::i2c::I2c for Untouchable {
    async fn transaction(
        &mut self,
        _: u8,
        _: &mut [embedded_hal_async::i2c::Operation<'_>],
    ) -> Result<(), BusFault> {
        panic!("building must not start an I2C transaction");
    }
}

impl protolink_adaptors::Clock for Untouchable {
    fn now(&self) -> Duration {
        panic!("building must not read the clock");
    }
}

impl protolink_adaptors::Timer for Untouchable {
    async fn sleep_until(&self, _: Duration) {
        panic!("building must not wait")
    }
}

impl protolink_i2c::target::I2cTarget for Untouchable {
    type Error = BusFault;

    fn disarm(&mut self) {}

    async fn listen(&mut self, _: &mut [u8]) -> Result<protolink_i2c::target::Command, BusFault> {
        panic!("building must not listen");
    }

    async fn respond_to_read(
        &mut self,
        _: &[u8],
    ) -> Result<protolink_i2c::target::ReadStatus, BusFault> {
        panic!("building must not respond");
    }
}

#[test]
fn building_touches_no_hardware() {
    let mut single = ControllerState::<1>::new();
    let binding = i2c_controller(Untouchable, Untouchable)
        .with_address(0x42)
        .with_poll_interval(Duration::from_millis(5))
        .with_storage(&mut single)
        .build()
        .unwrap();
    drop(binding);

    let mut several = ControllerState::<2>::new();
    let mut controller = i2c_controller(Untouchable, Untouchable)
        .with_storage(&mut several)
        .build()
        .unwrap();
    let ports = (
        controller.open(0x42).unwrap(),
        controller.open(0x43).unwrap(),
    );
    drop((ports, controller.into_runner()));

    let mut target = TargetState::new();
    drop(
        i2c_target(Untouchable, BOOT)
            .with_storage(&mut target)
            .build(),
    );
}

fn assert_send<T: Send>() {}

/// Ports, attachments and both runners can move to other tasks and threads, as
/// the buses they drive allow: the drivers here are `Send`.
#[test]
fn ports_attachments_and_runners_stay_send() {
    assert_send::<Port<'static>>();
    assert_send::<PortReader<'static>>();
    assert_send::<PortWriter<'static>>();
    assert_send::<Attachment<'static>>();
    assert_send::<ControllerRunner<'static, WireBus, TokioTimer, 1>>();
    assert_send::<TargetRunner<'static, NullTarget>>();
}
