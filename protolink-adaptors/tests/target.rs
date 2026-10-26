//! Target runner behaviour, with a raw simulated controller on the other end.

mod common;

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use common::*;
use embedded_hal_async::i2c::I2c;
use embedded_io_async::{Read, Write};
use protolink_adaptors::{
    AttachError, Binding, PortError, TargetError, TargetFault, TargetState, i2c_target,
};
use protolink_i2c::mailbox::{
    self, ConnectionId, MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE, MailboxError, Request,
};
use protolink_i2c::target::{Command, I2cTarget, ReadStatus};

/// A reply as the controller sees it, owned so that it outlives the frame.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    payload: Vec<u8>,
    credit: u8,
    connection: ConnectionId,
    serving: bool,
    retire: bool,
}

async fn read_reply(controller: &mut SimController) -> Seen {
    let mut frame = [0u8; MAILBOX_SIZE];
    controller.read(ADDRESS, &mut frame).await.unwrap();
    let reply = mailbox::decode_reply(&frame).unwrap();
    Seen {
        payload: reply.payload.to_vec(),
        credit: reply.rx_credit,
        connection: reply.connection,
        serving: reply.serving,
        retire: reply.retire,
    }
}

async fn send(controller: &mut SimController, connection: ConnectionId, request: Request<'_>) {
    let mut frame = [0u8; MAILBOX_SIZE];
    let len = mailbox::encode_request(connection, request, &mut frame).unwrap();
    controller.write(ADDRESS, &frame[..len]).await.unwrap();
}

fn connection(epoch: u16) -> ConnectionId {
    ConnectionId::new(BOOT, epoch)
}

/// Runs `runner` until `app` finishes. The runner never returns on its own.
macro_rules! run_until {
    ($runner:expr, $app:expr) => {{
        let runner = $runner;
        tokio::select! {
            never = runner.run_observed(|_| {}) => panic!("runner stopped: {never:?}"),
            out = with_timeout($app) => out,
        }
    }};
}

/// Collects the faults the target observes.
#[derive(Clone, Default)]
struct Faults(Rc<RefCell<Vec<TargetFault>>>);

impl Faults {
    fn observer(&self) -> impl FnMut(TargetFault) + use<> {
        let faults = self.0.clone();
        move |fault| faults.borrow_mut().push(fault)
    }

    fn get(&self) -> Vec<TargetFault> {
        self.0.borrow().clone()
    }
}

#[tokio::test]
async fn an_unbound_target_answers_without_serving_and_keeps_early_bytes() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        // Written before any open: the bytes wait for the controller.
        port.write_all(b"early").await.unwrap();
        let before = read_reply(&mut controller).await;
        assert_eq!(
            before,
            Seen {
                payload: vec![],
                credit: 0,
                connection: ConnectionId::new(BOOT, 0),
                serving: false,
                retire: false,
            }
        );
        send(&mut controller, connection(1), Request::Open).await;
        let opened = read_reply(&mut controller).await;
        assert!(opened.serving);
        assert_eq!(opened.connection, connection(1));
        assert_eq!(
            opened.payload, b"early",
            "first establishment keeps the bytes"
        );
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn a_read_gets_one_whole_mailbox_and_consumes_only_when_done() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        // An empty mailbox is idle and advertises the free receive space.
        let idle = read_reply(&mut controller).await;
        assert!(idle.payload.is_empty());
        assert_eq!(idle.credit, 64);

        // 40 bytes need two mailboxes of at most 24.
        let data = pattern(40, 9);
        port.write_all(&data).await.unwrap();
        let first = read_reply(&mut controller).await;
        assert_eq!(first.payload, &data[..MAILBOX_PAYLOAD_MAX]);
        let second = read_reply(&mut controller).await;
        assert_eq!(second.payload, &data[MAILBOX_PAYLOAD_MAX..]);
        port.flush().await.unwrap();

        // Writes go to the port; the credit shrinks accordingly.
        send(&mut controller, connection(1), Request::Data(&[1, 2, 3])).await;
        let reply = read_reply(&mut controller).await;
        assert_eq!(reply.credit, 61);
        let mut buf = [0u8; 8];
        let n = port.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], &[1, 2, 3]);
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn reads_are_still_answered_when_the_receive_queue_is_full() {
    let bus = SharedBus::default();
    let mut state = TargetState::<64, 64>::with_buffers();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        send(&mut controller, connection(1), Request::Data(&[0xAB; 24])).await;
        send(&mut controller, connection(1), Request::Data(&[0xAB; 24])).await;
        send(&mut controller, connection(1), Request::Data(&[0xAB; 16])).await;
        port.write_all(b"reply").await.unwrap();
        let reply = read_reply(&mut controller).await;
        assert_eq!(reply.credit, 0, "the queue is full");
        assert_eq!(reply.payload, b"reply");
        assert!(reply.serving);
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn a_write_beyond_the_advertised_credit_ends_the_generation_and_keeps_buffered_data() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let faults = Faults::default();
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        // The queue holds 64 bytes: two 24-byte writes fit, the third does not.
        send(&mut controller, connection(1), Request::Data(&[0xAB; 24])).await;
        send(&mut controller, connection(1), Request::Data(&[0xAB; 24])).await;
        send(&mut controller, connection(1), Request::Data(&[0xCD; 24])).await;
        let after = read_reply(&mut controller).await;
        assert!(!after.serving, "the generation ended");
        assert_eq!(after.connection, connection(1));

        // The bytes that were accepted are still delivered, then the fault.
        let mut buf = [0u8; 256];
        let mut got = 0;
        loop {
            match port.read(&mut buf[got..]).await {
                Ok(n) => got += n,
                Err(error) => {
                    assert_eq!(error, PortError::Protocol);
                    break;
                }
            }
        }
        assert_eq!(got, 2 * MAILBOX_PAYLOAD_MAX);
        assert!(buf[..got].iter().all(|&b| b == 0xAB));
    };
    let runner_faults = faults.clone();
    tokio::select! {
        never = runner.run_observed(runner_faults.observer()) => panic!("runner stopped: {never:?}"),
        () = with_timeout(app) => {}
    }
    assert_eq!(faults.get(), vec![TargetFault::ReceiveOverflow]);
    assert_eq!(bus.disarmed(), 1, "only the drop of the runner disarms");
}

#[tokio::test]
async fn an_incomplete_read_ends_the_generation_and_never_replays_its_payload() {
    for (fault, status) in [
        (RespondFault::Short, ReadStatus::NeedMoreBytes),
        (RespondFault::Leftover, ReadStatus::LeftoverBytes(1)),
    ] {
        let bus = SharedBus::default();
        bus.respond_with(fault);
        let mut state = TargetState::new();
        let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
            .with_storage(&mut state)
            .build();
        let mut controller = SimController(bus.clone());
        let faults = Faults::default();
        let app = async {
            send(&mut controller, connection(1), Request::Open).await;
            port.write_all(b"keep").await.unwrap();
            // The first read after the open carries the fault.
            let _ = read_reply(&mut controller).await;
            // The bytes of the failed reply are gone: the flush fails.
            port.flush().await
        };
        let flush = with_timeout(async {
            tokio::select! {
                never = runner.run_observed(faults.observer()) => panic!("runner stopped: {never:?}"),
                flush = app => flush,
            }
        })
        .await;
        assert_eq!(flush, Err(PortError::Hardware));
        assert_eq!(faults.get(), vec![TargetFault::ReadStatus(status)]);
        // Only the drop of the runner disarms: the fault did not stop it.
        assert_eq!(bus.disarmed(), 1);
    }
}

#[tokio::test]
async fn hardware_failure_wakes_pending_operations_and_preserves_the_error() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    bus.inject_listen_fault();
    let mut buf = [0u8; 8];
    // The read is polled first, so it is pending when the runner fails.
    let (read, result) =
        with_timeout(async { tokio::join!(port.read(&mut buf), runner.run()) }).await;
    assert_eq!(result.unwrap_err(), TargetError::Hardware(BusFault));
    assert_eq!(read, Err(PortError::Hardware));
    assert_eq!(bus.disarmed(), 1);
}

#[tokio::test]
async fn teardown_disarms_before_the_buffers_are_released() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    {
        let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
            .with_storage(&mut state)
            .build();
        let mut buf = [0u8; 1];
        tokio::select! {
            never = runner.run() => panic!("runner stopped: {never:?}"),
            () = tokio::task::yield_now() => (),
        }
        // The canceled run future dropped the runner.
        assert_eq!(bus.disarmed(), 1);
        assert_eq!(port.read(&mut buf).await, Err(PortError::Closed));
    }
    // The state is reusable afterwards and starts empty.
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    drop(runner);
    assert_eq!(bus.disarmed(), 2);
    let _ = port;
}

/// A target that reports one write longer than the mailbox buffer, then never
/// sees another transaction.
struct OversizedWrite {
    disarmed: Rc<Cell<usize>>,
    reported: bool,
}

impl I2cTarget for OversizedWrite {
    type Error = BusFault;

    fn disarm(&mut self) {
        self.disarmed.set(self.disarmed.get() + 1);
    }

    async fn listen(&mut self, _: &mut [u8]) -> Result<Command, BusFault> {
        if std::mem::replace(&mut self.reported, true) {
            return std::future::pending().await;
        }
        Ok(Command::Write(MAILBOX_SIZE + 1))
    }

    async fn respond_to_read(&mut self, _: &[u8]) -> Result<ReadStatus, BusFault> {
        unreachable!("an oversized write is never answered")
    }
}

#[tokio::test]
async fn an_oversized_write_ends_the_generation_and_is_not_clipped() {
    let disarmed = Rc::new(Cell::new(0));
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(
        OversizedWrite {
            disarmed: disarmed.clone(),
            reported: false,
        },
        BOOT,
    )
    .with_storage(&mut state)
    .build();
    let faults = Faults::default();
    let app = async {
        // Serve the connection first, so that the write is judged against it.
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = with_timeout(async {
        tokio::select! {
            never = runner.run_observed(faults.observer()) => panic!("runner stopped: {never:?}"),
            read = app => read,
        }
    })
    .await;
    assert_eq!(read, Err(PortError::Protocol));
    assert_eq!(
        faults.get(),
        vec![TargetFault::OversizedWrite(MAILBOX_SIZE + 1)]
    );
    // Only the drop of the runner disarms: the fault did not stop it.
    assert_eq!(disarmed.get(), 1);
}

#[tokio::test]
async fn a_repeated_open_of_the_served_identity_changes_nothing() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.write_all(b"keep").await.unwrap();
        send(&mut controller, connection(1), Request::Open).await;
        let reply = read_reply(&mut controller).await;
        assert!(reply.serving);
        assert_eq!(reply.payload, b"keep");
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn writes_of_another_identity_are_ignored() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        send(&mut controller, connection(2), Request::Data(b"x")).await;
        send(
            &mut controller,
            ConnectionId::new(BOOT ^ 1, 1),
            Request::Data(b"y"),
        )
        .await;
        let mut buf = [0u8; 4];
        assert!(poll_once_pending(port.read(&mut buf)));
        send(&mut controller, connection(1), Request::Data(b"z")).await;
        let n = port.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"z");
        assert!(read_reply(&mut controller).await.serving);
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn a_zero_epoch_write_on_the_served_connection_is_a_protocol_failure() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let faults = Faults::default();
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        // Epoch zero is not a connection, so the frame is malformed.
        let mut frame = [0u8; MAILBOX_SIZE];
        frame[0] = 1;
        frame[1] = mailbox::OP_DATA;
        frame[4..6].copy_from_slice(&BOOT.to_le_bytes());
        controller
            .write(ADDRESS, &frame[..mailbox::HEADER_LEN + 1])
            .await
            .unwrap();
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = with_timeout(async {
        tokio::select! {
            never = runner.run_observed(faults.observer()) => panic!("runner stopped: {never:?}"),
            read = app => read,
        }
    })
    .await;
    assert_eq!(read, Err(PortError::Protocol));
    assert_eq!(
        faults.get(),
        vec![TargetFault::Mailbox(MailboxError::ZeroEpoch)]
    );
}

#[tokio::test]
async fn a_replacement_waits_for_the_port_to_be_dropped_and_discards_the_old_bytes() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let attachment = port.attachment();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.write_all(b"old").await.unwrap();
        // The controller replaces the connection while the port is attached.
        send(&mut controller, connection(2), Request::Open).await;
        let stale = read_reply(&mut controller).await;
        assert!(!stale.serving, "the old connection stops serving at once");
        assert_eq!(stale.connection, connection(1));

        let mut buf = [0u8; 4];
        assert_eq!(port.read(&mut buf).await, Err(PortError::Reset));
        assert_eq!(port.write(b"late").await, Err(PortError::Reset));
        drop(port);

        // Once the port is gone, the replacement is served with empty buffers.
        let fresh = read_reply(&mut controller).await;
        assert!(fresh.serving);
        assert_eq!(fresh.connection, connection(2));
        assert!(fresh.payload.is_empty(), "the old bytes never cross over");

        let mut port = attachment.try_attach().unwrap();
        port.write_all(b"new").await.unwrap();
        let sent = read_reply(&mut controller).await;
        assert_eq!(sent.payload, b"new");
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn a_retirement_unbinds_the_target_once_the_port_is_dropped() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        drop(port);
        send(&mut controller, connection(1), Request::Retire).await;
        let reply = read_reply(&mut controller).await;
        assert!(!reply.serving);
        assert_eq!(reply.connection, ConnectionId::new(BOOT, 0));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn an_aborting_port_asks_the_controller_to_retire_the_connection() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.abort();
        let reply = read_reply(&mut controller).await;
        assert!(!reply.serving);
        assert!(reply.retire, "the controller is asked to retire it");
        assert_eq!(reply.connection, connection(1));
        let mut buf = [0u8; 4];
        assert_eq!(port.read(&mut buf).await, Err(PortError::Reset));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn dropping_halves_or_the_whole_port_never_ends_the_connection() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let attachment = port.attachment();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        let (reader, mut writer) = port.into_split();
        // Dropping the reader leaves the writer working.
        drop(reader);
        writer.write_all(b"out").await.unwrap();
        assert_eq!(read_reply(&mut controller).await.payload, b"out");
        // Dropping both halves keeps the connection and its buffered bytes.
        drop(writer);
        send(&mut controller, connection(1), Request::Data(b"in")).await;
        let reply = read_reply(&mut controller).await;
        assert!(reply.serving, "the connection outlives its ports");
        let mut port = attachment.try_attach().unwrap();
        let mut buf = [0u8; 4];
        let n = port.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"in");
    };
    run_until!(runner, app);
}

/// Polls a future once and reports whether it is still pending.
fn poll_once_pending<F: Future>(fut: F) -> bool {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    fut.as_mut().poll(&mut cx).is_pending()
}

/// A driver that counts the `listen` operations it is asked to start.
struct CountingTarget {
    inner: SimTarget,
    listens: Rc<Cell<usize>>,
}

impl I2cTarget for CountingTarget {
    type Error = common::BusFault;

    fn disarm(&mut self) {
        self.inner.disarm();
    }

    fn listen(
        &mut self,
        buffer: &mut [u8],
    ) -> impl Future<Output = Result<Command, common::BusFault>> {
        self.listens.set(self.listens.get() + 1);
        self.inner.listen(buffer)
    }

    fn respond_to_read(
        &mut self,
        response: &[u8],
    ) -> impl Future<Output = Result<ReadStatus, common::BusFault>> {
        self.inner.respond_to_read(response)
    }
}

#[tokio::test]
async fn a_replacement_is_finished_by_the_last_half_dropped_without_bus_traffic() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let attachment = port.attachment();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.write_all(b"old").await.unwrap();
        send(&mut controller, connection(2), Request::Open).await;
        let stale = read_reply(&mut controller).await;
        assert!(!stale.serving);
        drop(port);
        // No bus transaction follows: the runner must finish the replacement by
        // itself, so that the attachment can proceed.
        let port = attachment.attach().await.unwrap();
        drop(port);
        let fresh = read_reply(&mut controller).await;
        assert!(fresh.serving);
        assert_eq!(fresh.connection, connection(2));
        assert!(fresh.payload.is_empty(), "the old bytes never cross over");
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn dropping_one_half_leaves_a_replacement_pending_until_the_other_is_dropped() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let attachment = port.attachment();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        let (reader, writer) = port.into_split();
        send(&mut controller, connection(2), Request::Open).await;
        let stale = read_reply(&mut controller).await;
        assert!(!stale.serving);
        drop(writer);
        assert_eq!(
            attachment.try_attach_writer().err(),
            Some(AttachError::Ending),
            "the reader still holds the old connection"
        );
        drop(reader);
        let port = attachment.attach().await.unwrap();
        drop(port);
        let fresh = read_reply(&mut controller).await;
        assert_eq!(fresh.connection, connection(2));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn local_events_never_restart_the_listen_operation() {
    let bus = SharedBus::default();
    let listens = Rc::new(Cell::new(0));
    let mut state = TargetState::new();
    let driver = CountingTarget {
        inner: SimTarget(bus.clone()),
        listens: listens.clone(),
    };
    let Binding { port, runner } = i2c_target(driver, BOOT).with_storage(&mut state).build();
    let attachment = port.attachment();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        let port = port;
        send(&mut controller, connection(2), Request::Open).await;
        let stale = read_reply(&mut controller).await;
        assert!(!stale.serving);
        // Let the runner settle into its next, pending, listen operation.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        // Local events while the listen operation is pending: a drop, which
        // finishes the replacement, and an attachment.
        let before = listens.get();
        drop(port);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let port = attachment.attach().await.unwrap();
        assert_eq!(listens.get(), before, "no new listen was started");
        drop(port);
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn retiring_the_advertised_connection_while_a_replacement_waits_unbinds_the_target() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        let mut port = port;
        port.write_all(b"old").await.unwrap();
        send(&mut controller, connection(2), Request::Open).await;
        let stale = read_reply(&mut controller).await;
        assert_eq!(
            stale.connection,
            connection(1),
            "still advertising the old one"
        );
        // The controller retires what the target still advertises. The pending
        // replacement gives way to the retirement.
        send(&mut controller, connection(1), Request::Retire).await;
        drop(port);
        let reply = read_reply(&mut controller).await;
        assert!(!reply.serving, "the target is unbound, not serving epoch 2");
        assert_eq!(reply.connection, ConnectionId::new(BOOT, 0));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn retiring_another_connection_while_a_replacement_waits_changes_nothing() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        let mut port = port;
        port.write_all(b"old").await.unwrap();
        send(&mut controller, connection(2), Request::Open).await;
        read_reply(&mut controller).await;
        send(&mut controller, connection(9), Request::Retire).await;
        drop(port);
        let reply = read_reply(&mut controller).await;
        assert!(reply.serving);
        assert_eq!(reply.connection, connection(2));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn a_reply_in_flight_keeps_its_payload_until_the_read_completes() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let attachment = port.attachment();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.write_all(b"abc").await.unwrap();
        // The read is answered with "abc", and the answer stays in flight until
        // the controller takes it. The port is dropped meanwhile: that must not
        // touch the payload the answer carries.
        let mut frame = [0u8; MAILBOX_SIZE];
        let read = controller.read(ADDRESS, &mut frame);
        let detach = async { drop(port) };
        let (read, ()) = tokio::join!(read, detach);
        read.unwrap();
        let reply = mailbox::decode_reply(&frame).unwrap();
        assert_eq!(reply.payload, b"abc");
        // The payload left the queue once, when the read completed.
        let next = read_reply(&mut controller).await;
        assert!(next.serving, "the drop never ends the connection");
        assert!(next.payload.is_empty());
        let _ = attachment.try_attach().unwrap();
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn an_abort_supersedes_a_pending_replacement() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        send(&mut controller, connection(2), Request::Open).await;
        let stale = read_reply(&mut controller).await;
        assert_eq!(
            stale.connection,
            connection(1),
            "still advertising the old one"
        );
        port.abort();
        drop(port);
        // The abort retires the old connection, so the replacement never takes effect.
        let reply = read_reply(&mut controller).await;
        assert!(!reply.serving, "the target is unbound, not serving epoch 2");
        assert_eq!(reply.connection, ConnectionId::new(BOOT, 0));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn an_open_after_an_observed_abort_replaces_the_unbinding() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.abort();
        let reply = read_reply(&mut controller).await;
        assert!(reply.retire && !reply.serving, "the abort is observed");
        send(&mut controller, connection(2), Request::Open).await;
        drop(port);
        let fresh = read_reply(&mut controller).await;
        assert!(fresh.serving);
        assert_eq!(fresh.connection, connection(2));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn repeated_settling_does_not_replay_an_observed_abort() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.abort();
        read_reply(&mut controller).await;
        send(&mut controller, connection(2), Request::Open).await;
        // Another transaction settles again while the port is still attached and
        // the abort flag is still set. The newer OPEN must survive it.
        let during = read_reply(&mut controller).await;
        assert_eq!(during.connection, connection(1));
        drop(port);
        let fresh = read_reply(&mut controller).await;
        assert!(
            fresh.serving,
            "the newer OPEN was not overwritten by the abort"
        );
        assert_eq!(fresh.connection, connection(2));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn a_later_generation_observes_its_own_abort() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let attachment = port.attachment();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.abort();
        read_reply(&mut controller).await;
        drop(port);
        let reply = read_reply(&mut controller).await;
        assert!(!reply.serving, "the first generation unbinds");

        // A second generation with a pending replacement, aborted while attached.
        let mut port = attachment.try_attach().unwrap();
        send(&mut controller, connection(2), Request::Open).await;
        send(&mut controller, connection(3), Request::Open).await;
        let stale = read_reply(&mut controller).await;
        assert_eq!(stale.connection, connection(2));
        port.abort();
        drop(port);
        let reply = read_reply(&mut controller).await;
        assert!(!reply.serving, "the second abort is observed, not stale");
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn a_fault_on_the_served_epoch_cannot_resurrect_it_through_a_pending_open() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        let port = port;
        send(&mut controller, connection(2), Request::Open).await;
        // The controller asks for epoch one again while it is being left.
        send(&mut controller, connection(1), Request::Open).await;
        // The read of epoch one's reply fails, so epoch one is faulted.
        bus.respond_with(RespondFault::Short);
        read_reply(&mut controller).await;
        drop(port);
        let fresh = read_reply(&mut controller).await;
        assert!(!fresh.serving, "a faulted epoch is never served again");
        assert_eq!(fresh.connection, ConnectionId::new(BOOT, 0));
    };
    run_until!(runner, app);
}

#[tokio::test]
async fn a_fault_on_the_served_epoch_keeps_a_valid_pending_replacement() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()), BOOT)
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        let port = port;
        send(&mut controller, connection(2), Request::Open).await;
        bus.respond_with(RespondFault::Short);
        read_reply(&mut controller).await;
        drop(port);
        let fresh = read_reply(&mut controller).await;
        assert!(fresh.serving, "epoch two is still a valid replacement");
        assert_eq!(fresh.connection, connection(2));
    };
    run_until!(runner, app);
}

/// A target whose answers to reads complete only once the test opens `gate`.
/// Each answer is announced on `at_gate` once the controller has taken it, when
/// the answer has reached the completion gate.
struct GatedTarget {
    inner: SimTarget,
    at_gate: tokio::sync::mpsc::UnboundedSender<()>,
    gate: Arc<tokio::sync::Notify>,
}

impl I2cTarget for GatedTarget {
    type Error = BusFault;

    fn disarm(&mut self) {
        self.inner.disarm();
    }

    fn listen(&mut self, buffer: &mut [u8]) -> impl Future<Output = Result<Command, BusFault>> {
        self.inner.listen(buffer)
    }

    async fn respond_to_read(&mut self, response: &[u8]) -> Result<ReadStatus, BusFault> {
        let status = self.inner.respond_to_read(response).await?;
        // The controller has the answer, and its completion is held back here.
        self.at_gate.send(()).unwrap();
        self.gate.notified().await;
        Ok(status)
    }
}

#[tokio::test]
async fn an_in_flight_answer_holds_off_finalization_until_it_completes() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let (at_gate_tx, mut at_gate) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(tokio::sync::Notify::new());
    let driver = GatedTarget {
        inner: SimTarget(bus.clone()),
        at_gate: at_gate_tx,
        gate: gate.clone(),
    };
    let Binding { mut port, runner } = i2c_target(driver, BOOT).with_storage(&mut state).build();
    let attachment = port.attachment();
    // The executor polls the runner, and the test polls it by hand as well, so
    // it is shared through a cell. Neither poll is ever inside the other.
    let runner = RefCell::new(Box::pin(runner.run_observed(|_| {})));
    let mut controller = SimController(bus.clone());
    let app = async {
        send(&mut controller, connection(1), Request::Open).await;
        port.write_all(b"abc").await.unwrap();
        let mut frame = [0u8; MAILBOX_SIZE];
        let read = controller.read(ADDRESS, &mut frame);
        let control = async {
            // The answer has been taken, and the runner is held at the gate.
            at_gate.recv().await.unwrap();
            port.abort();
            drop(port);
            // Poll the runner while the gate is still closed. The abort and the
            // drop are local events, which it must not finish the generation on
            // while its answer is in flight.
            let polled = poll_fn(|cx| Poll::Ready(runner.borrow_mut().as_mut().poll(cx))).await;
            assert!(polled.is_pending(), "the runner is still answering");
            // Finalization waits for the runner, and the runner waits for the answer.
            assert_eq!(attachment.try_attach().err(), Some(AttachError::Ending));
            assert!(poll_once_pending(attachment.attach()));
            gate.notify_one();
        };
        let (read, ()) = tokio::join!(read, control);
        read.unwrap();
        let reply = mailbox::decode_reply(&frame).unwrap();
        assert_eq!(reply.payload, b"abc", "the payload crossed with the answer");
        // The completed answer removed the payload once, and the aborted
        // connection is then finished and unbound.
        let next = read_reply(&mut controller).await;
        assert!(next.payload.is_empty(), "the payload is not replayed");
        assert!(!next.serving);
        assert_eq!(next.connection, ConnectionId::new(BOOT, 0));
        attachment.attach().await.unwrap();
    };
    tokio::select! {
        never = poll_fn(|cx| runner.borrow_mut().as_mut().poll(cx)) => {
            panic!("runner stopped: {never:?}")
        }
        out = with_timeout(app) => out,
    }
}
