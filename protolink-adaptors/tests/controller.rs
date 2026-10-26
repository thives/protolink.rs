//! Controller runner behaviour against simulated targets that speak the mailbox
//! protocol.

mod common;

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use common::*;
use embedded_hal::i2c::{ErrorType, Operation};
use embedded_hal_async::i2c::I2c;
use embedded_io_async::{Read, Write};
use protolink_adaptors::Clock;
use protolink_adaptors::{
    AttachError, Binding, ControllerState, PortError, RouteEvent, RouteFault, i2c_controller,
};
use protolink_i2c::mailbox::{
    self, ConnectionId, MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE, MailboxError, Reply,
};

/// Polls a future once and drops it if it is pending, as ARQ does.
fn poll_once<F: Future>(fut: F) -> Option<F::Output> {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// The route events of one run, as the observer records them.
#[derive(Clone, Default)]
struct Events(Rc<RefCell<Vec<RouteEvent<BusFault>>>>);

impl Events {
    fn observer(&self) -> impl FnMut(RouteEvent<BusFault>) + use<> {
        let events = self.0.clone();
        move |event| events.borrow_mut().push(event)
    }

    fn faults(&self) -> Vec<RouteFault<BusFault>> {
        self.0.borrow().iter().map(|event| event.fault).collect()
    }
}

/// Runs `runner` until `app` finishes, failing if it takes too long. The
/// runner never returns, so its completion is unreachable.
macro_rules! run_until {
    ($runner:expr, $observer:expr, $app:expr) => {{
        let runner = $runner;
        tokio::select! {
            never = runner.run_observed($observer) => match never {},
            out = with_timeout($app) => out,
        }
    }};
}

#[tokio::test]
async fn a_route_opens_its_connection_before_it_moves_data() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    bus.wire(0x42).outgoing.extend(b"world");
    let events = Events::default();
    let app = async {
        port.write_all(b"hello").await.unwrap();
        port.flush().await.unwrap();
        let mut buf = [0u8; 8];
        let n = port.read(&mut buf).await.unwrap();
        buf[..n].to_vec()
    };
    let got = run_until!(runner, events.observer(), app);
    assert_eq!(got, b"world");
    let wire = bus.wire(0x42);
    assert_eq!(
        wire.sent.first(),
        Some(&Sent::Open(ConnectionId::new(BOOT, 1))),
        "the first request opens the connection"
    );
    assert_eq!(wire.received, b"hello");
    assert!(events.faults().is_empty());
}

#[tokio::test]
async fn the_open_is_retried_while_the_target_refuses_it() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).refuse_open = 3;
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let events = Events::default();
    let app = async {
        port.write_all(b"late").await.unwrap();
        port.flush().await.unwrap();
    };
    run_until!(runner, events.observer(), app);
    let opens = bus
        .wire(0x42)
        .sent
        .iter()
        .filter(|sent| matches!(sent, Sent::Open(_)))
        .count();
    assert_eq!(opens, 4, "three refusals, then the accepted open");
    assert_eq!(bus.wire(0x42).received, b"late");
    assert!(events.faults().is_empty(), "a refused open is not a fault");
}

#[tokio::test]
async fn an_open_that_is_never_accepted_ends_the_route_within_its_budget() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).refuse_open = usize::MAX;
    let mut state = ControllerState::<1>::new();
    let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_open_attempts(3)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let runner = controller.into_runner();
    let events = Events::default();
    let app = async {
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let result = run_until!(runner, events.observer(), app);
    assert_eq!(result, Err(PortError::Reset));
    assert_eq!(
        bus.wire(0x42)
            .sent
            .iter()
            .filter(|sent| matches!(sent, Sent::Open(_)))
            .count(),
        3
    );
    assert_eq!(events.faults(), vec![RouteFault::OpenExhausted]);
}

#[tokio::test]
async fn an_ambiguous_data_write_ends_the_route_and_is_never_replayed() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).fail_data_writes = true;
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let events = Events::default();
    let app = async {
        port.write_all(b"abc").await.unwrap();
        let flush = port.flush().await;
        // The route is over, so later operations fail too.
        let write = port.write(b"x").await;
        (flush, write)
    };
    let (flush, write) = run_until!(runner, events.observer(), app);
    assert_eq!(flush, Err(PortError::Hardware));
    assert_eq!(write, Err(PortError::Hardware));
    assert_eq!(events.faults(), vec![RouteFault::Bus(BusFault)]);
    assert!(bus.wire(0x42).received.is_empty());
}

#[tokio::test]
async fn bytes_received_before_a_failed_read_are_delivered_first() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).outgoing.extend(b"last");
    // Read 0 is the unbound reply, read 1 the first payload, read 2 fails.
    bus.wire(0x42).fail_reads_from = Some(2);
    let mut state = ControllerState::<1>::new();
    let Binding { port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let (mut reader, mut writer) = port.into_split();
    let events = Events::default();
    let app = async {
        let mut buf = [0u8; 16];
        let n = reader.read(&mut buf).await.unwrap();
        let read = reader.read(&mut buf).await;
        let write = writer.write(b"x").await;
        let flush = writer.flush().await;
        (buf[..n].to_vec(), read, write, flush)
    };
    let (got, read, write, flush) = run_until!(runner, events.observer(), app);
    assert_eq!(got, b"last");
    assert_eq!(read, Err(PortError::Hardware));
    assert_eq!(write, Err(PortError::Hardware));
    assert_eq!(
        flush,
        Err(PortError::Hardware),
        "a failed generation never flushes"
    );
    assert_eq!(events.faults(), vec![RouteFault::Bus(BusFault)]);
}

#[tokio::test]
async fn a_malformed_reply_is_a_protocol_failure() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut bad = unbound_frame(BOOT);
    bad[0] = 31;
    bus.wire(0x42).raw_replies.push_back(bad);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus, VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let events = Events::default();
    let app = async {
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(read, Err(PortError::Protocol));
    assert_eq!(
        events.faults(),
        vec![RouteFault::Mailbox(MailboxError::InvalidPayloadLength(31))]
    );
}

#[tokio::test]
async fn a_rebooted_target_ends_its_route_and_leaves_the_others_running() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.add(0x43);
    let mut state = ControllerState::<2>::new();
    let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut rebooting = controller.open(0x42).unwrap();
    let mut steady = controller.open(0x43).unwrap();
    let runner = controller.into_runner();
    let data = pattern(400, 3);
    bus.wire(0x43).outgoing.extend(&data);
    let events = Events::default();
    let app = async {
        // Let both routes come up, then reboot the first target.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        bus.wire(0x42).boot = 0x1111;
        let mut buf = [0u8; 8];
        let rebooted = rebooting.read(&mut buf).await;
        let mut got = Vec::new();
        let mut chunk = [0u8; 64];
        while got.len() < data.len() {
            let n = steady.read(&mut chunk).await.unwrap();
            got.extend_from_slice(&chunk[..n]);
        }
        steady.write_all(b"ok").await.unwrap();
        steady.flush().await.unwrap();
        (rebooted, got)
    };
    let (rebooted, got) = run_until!(runner, events.observer(), app);
    assert_eq!(rebooted, Err(PortError::Reset));
    assert_eq!(got, data, "the healthy route kept transferring");
    assert_eq!(bus.wire(0x43).received, b"ok");
    assert_eq!(
        events.faults(),
        vec![RouteFault::Rebooted {
            expected: BOOT,
            found: 0x1111,
        }]
    );
}

#[tokio::test]
async fn a_target_that_retires_its_connection_resets_the_port() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let events = Events::default();
    let app = async {
        port.write_all(b"ping").await.unwrap();
        port.flush().await.unwrap();
        bus.wire(0x42).request_retire();
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    assert_eq!(events.faults(), vec![RouteFault::Retired]);
}

#[tokio::test]
async fn abort_resets_the_port_and_retires_the_connection_once() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let events = Events::default();
    let app = async {
        port.write_all(b"bye").await.unwrap();
        port.flush().await.unwrap();
        port.abort();
        let mut buf = [0u8; 4];
        let read = port.read(&mut buf).await;
        // The retirement goes out on the runner's next turns.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        read
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    let retires: Vec<Sent> = bus
        .wire(0x42)
        .sent
        .iter()
        .copied()
        .filter(|sent| matches!(sent, Sent::Retire(_)))
        .collect();
    assert_eq!(retires, vec![Sent::Retire(ConnectionId::new(BOOT, 1))]);
    assert!(events.faults().is_empty(), "an abort is local, not a fault");
}

#[tokio::test]
async fn an_attachment_opens_a_new_epoch_after_a_reset() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let attachment = port.attachment();
    let events = Events::default();
    let app = async {
        port.write_all(b"old").await.unwrap();
        port.flush().await.unwrap();
        port.abort();
        drop(port);
        // The attachment waits until the aborted connection has finished.
        let mut port = attachment.attach().await.unwrap();
        port.write_all(b"new").await.unwrap();
        port.flush().await.unwrap();
    };
    run_until!(runner, events.observer(), app);
    let wire = bus.wire(0x42);
    assert_eq!(
        wire.sent
            .iter()
            .filter(|sent| matches!(sent, Sent::Open(_)))
            .copied()
            .collect::<Vec<_>>(),
        vec![
            Sent::Open(ConnectionId::new(BOOT, 1)),
            Sent::Open(ConnectionId::new(BOOT, 2)),
        ]
    );
    assert_eq!(
        wire.received, b"oldnew",
        "both were sent on their own epochs"
    );
}

#[tokio::test]
async fn routes_are_served_round_robin_under_sustained_traffic() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.add(0x43);
    let mut state = ControllerState::<2>::new();
    let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut a = controller.open(0x42).unwrap();
    let mut b = controller.open(0x43).unwrap();
    let runner = controller.into_runner();
    let (data_a, data_b) = (pattern(400, 1), pattern(400, 2));

    let app = async {
        let send_a = async {
            a.write_all(&data_a).await.unwrap();
            a.flush().await.unwrap();
        };
        let send_b = async {
            b.write_all(&data_b).await.unwrap();
            b.flush().await.unwrap();
        };
        tokio::join!(send_a, send_b);
    };
    let events = Events::default();
    run_until!(runner, events.observer(), app);
    assert_eq!(bus.wire(0x42).received, data_a);
    assert_eq!(bus.wire(0x43).received, data_b);
    assert!(events.faults().is_empty());
}

#[tokio::test]
async fn local_data_wakes_an_idle_runner_early() {
    // The poll delay never completes, so only an early wakeup can send.
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).pre_serve(1);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), NeverTimer)
        .with_open_retry_interval(Duration::ZERO)
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let app = async {
        for _ in 0..10 {
            tokio::task::yield_now().await; // let the runner go idle
        }
        port.write_all(b"wake").await.unwrap();
        port.flush().await.unwrap();
    };
    run_until!(runner, Events::default().observer(), app);
    assert_eq!(bus.wire(0x42).received, b"wake");
}

#[tokio::test]
async fn idle_runner_sleeps_for_the_poll_interval() {
    let timer = VirtualTimer::default();
    let deadlines = timer.deadlines.clone();
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    let Binding { port, runner } = i2c_controller(bus, timer)
        .with_poll_interval(Duration::from_micros(2500))
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    // Without a port, no route has work, so the only waits are the poll ones.
    drop(port);
    let stop = async {
        while deadlines.borrow().len() < 3 {
            tokio::task::yield_now().await;
        }
    };
    run_until!(runner, Events::default().observer(), stop);
    let deadlines = deadlines.borrow();
    assert_eq!(deadlines[0], Duration::from_micros(2500));
    for pair in deadlines.windows(2) {
        assert_eq!(pair[1] - pair[0], Duration::from_micros(2500));
    }
}

#[tokio::test]
async fn a_full_receive_buffer_stops_reads_until_a_whole_payload_fits() {
    let bus = WireBus::default();
    bus.add(0x42);
    // Unbound at first: the frames are all the new connection's, and the first
    // one is carried by the confirming read.
    let frames: Vec<Vec<u8>> = (0..6).map(|i| pattern(MAILBOX_PAYLOAD_MAX, i)).collect();
    bus.wire(0x42).outgoing.extend(frames.concat());
    // 32 bytes: after one payload only 8 are free, less than a payload.
    let mut state = ControllerState::<1, 32, 32>::with_buffers();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), NeverTimer)
        .with_open_retry_interval(Duration::ZERO)
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let app = async {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        // The discovery and confirming reads, which carry the first payload.
        let blocked = bus.wire(0x42).reads;
        assert_eq!(blocked, 2);
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            bus.wire(0x42).reads,
            blocked,
            "no read without room for a payload"
        );
        // Reading frees room, which wakes the idle runner without any delay.
        let mut got = Vec::new();
        let mut buf = [0u8; 7];
        while got.len() < 6 * MAILBOX_PAYLOAD_MAX {
            let n = port.read(&mut buf).await.unwrap();
            got.extend_from_slice(&buf[..n]);
        }
        got
    };
    let got = run_until!(runner, Events::default().observer(), app);
    // 24-byte payloads in a 32-byte ring wrap around constantly.
    assert_eq!(got, frames.concat());
}

#[tokio::test]
async fn a_blocked_route_does_not_starve_the_others() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.add(0x43);
    bus.wire(0x42).pre_serve(1);
    bus.wire(0x43).pre_serve(1);
    for i in 0..50 {
        bus.wire(0x42)
            .outgoing
            .extend(pattern(MAILBOX_PAYLOAD_MAX, i));
        bus.wire(0x43)
            .outgoing
            .extend(pattern(MAILBOX_PAYLOAD_MAX, i));
    }
    let mut state = ControllerState::<2>::new();
    let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_storage(&mut state)
        .build()
        .unwrap();
    let _blocked = controller.open(0x42).unwrap(); // never read: backpressure
    let mut flowing = controller.open(0x43).unwrap();
    let runner = controller.into_runner();
    let app = async {
        let mut total = 0;
        let mut buf = [0u8; 64];
        while total < 20 * MAILBOX_PAYLOAD_MAX {
            total += flowing.read(&mut buf).await.unwrap();
        }
    };
    run_until!(runner, Events::default().observer(), app);
}

#[tokio::test]
async fn bytes_stay_queued_until_their_transaction_succeeds_or_fail_the_flush() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).fail_data_writes = true;
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    port.write_all(b"abc").await.unwrap();
    let flush = run_until!(runner, Events::default().observer(), port.flush());
    assert_eq!(flush, Err(PortError::Hardware));
    assert!(bus.wire(0x42).received.is_empty());
}

#[tokio::test]
async fn canceled_port_operations_lose_nothing_and_can_be_repolled() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).pre_serve(1);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();

    // Nothing to read yet: a canceled read took nothing.
    let mut buf = [0u8; 8];
    assert!(poll_once(port.read(&mut buf)).is_none());

    // Fill the send buffer. A write that cannot be accepted is canceled
    // without accepting anything.
    assert_eq!(port.write(&[7u8; 64]).await, Ok(64));
    assert!(poll_once(port.write(b"rejected")).is_none());
    // A canceled flush does not complete or discard the bytes either.
    assert!(poll_once(port.flush()).is_none());

    // Once the runner is driven, exactly the accepted bytes cross the bus.
    let app = async {
        port.flush().await.unwrap();
        // Bytes queued before the route serves belong to the target's old
        // connection, so the target sends the new bytes only now.
        bus.wire(0x42).outgoing.extend(b"late");
        let n = port.read(&mut buf).await.unwrap();
        (n, buf)
    };
    let (n, buf) = run_until!(runner, Events::default().observer(), app);
    assert_eq!(&buf[..n], b"late");
    assert_eq!(bus.wire(0x42).received, vec![7u8; 64]);
}

#[tokio::test]
async fn pending_operations_are_woken_when_the_runner_is_dropped() {
    let mut state = ControllerState::<1>::new();
    let Binding { port, runner } = i2c_controller(WireBus::default(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let (mut reader, mut writer) = port.into_split();
    // Fill the send buffer so `write` and `flush` both have to wait.
    let filled = writer.write(&[0u8; 64]).await.unwrap();
    assert_eq!(filled, 64);
    let mut buf = [0u8; 4];
    let read = reader.read(&mut buf);
    let write = writer.write(b"more");
    let tear_down = async {
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        drop(runner);
    };
    let (read, write, ()) = with_timeout(async { tokio::join!(read, write, tear_down) }).await;
    assert_eq!(read, Err(PortError::Closed));
    assert_eq!(write, Err(PortError::Closed));
    assert_eq!(writer.flush().await, Err(PortError::Closed));
}

#[tokio::test]
async fn canceling_the_run_future_tears_the_runner_down() {
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(WireBus::default(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    {
        let run = runner.run();
        let mut run = pin!(run);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(run.as_mut().poll(&mut cx).is_pending());
    } // dropped while pending
    let mut buf = [0u8; 1];
    assert_eq!(port.read(&mut buf).await, Err(PortError::Closed));
}

#[tokio::test]
async fn write_accepts_a_prefix_when_space_is_short() {
    let mut state = ControllerState::<1, 64, 8>::with_buffers();
    let Binding { mut port, runner } = i2c_controller(WireBus::default(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    assert_eq!(port.write(&[1u8; 20]).await, Ok(8));
    drop(runner);
}

#[tokio::test]
async fn a_frame_with_an_impossible_length_is_a_protocol_failure() {
    // A reply of all 0xFF declares a payload longer than a frame holds.
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).raw_replies.push_back([0xFF; MAILBOX_SIZE]);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus, VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut buf = [0u8; 4];
    let read = run_until!(runner, Events::default().observer(), port.read(&mut buf));
    assert_eq!(read, Err(PortError::Protocol));
}

/// The epochs of every `OPEN` a target received, in order.
fn opened_epochs(bus: &WireBus, address: u8) -> Vec<u16> {
    bus.wire(address)
        .sent
        .iter()
        .filter_map(|sent| match sent {
            Sent::Open(connection) => Some(connection.epoch()),
            _ => None,
        })
        .collect()
}

/// Asserts that no epoch appears twice in `epochs`.
fn assert_fresh_epochs(epochs: &[u16]) {
    let mut sorted = epochs.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        epochs.len(),
        "an epoch was opened twice: {epochs:?}"
    );
}

/// The route faults of a run, each with the virtual time it happened at.
type TimedLog = Rc<RefCell<Vec<(RouteFault<BusFault>, Duration)>>>;

/// Records each route event with the virtual time it happened at.
#[derive(Clone)]
struct TimedEvents {
    log: TimedLog,
    clock: VirtualTimer,
}

impl TimedEvents {
    fn new(clock: &VirtualTimer) -> Self {
        Self {
            log: Rc::default(),
            clock: clock.clone(),
        }
    }

    fn observer(&self) -> impl FnMut(RouteEvent<BusFault>) + use<> {
        let log = self.log.clone();
        let clock = self.clock.clone();
        move |event| log.borrow_mut().push((event.fault, clock.now.get()))
    }
}

#[tokio::test]
async fn a_rebuilt_controller_never_reuses_an_epoch_it_gave_out() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    for round in 0..2u8 {
        let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
            .with_address(0x42)
            .with_storage(&mut state)
            .build()
            .unwrap();
        let app = async {
            port.write_all(&[round]).await.unwrap();
            port.flush().await.unwrap();
        };
        run_until!(runner, Events::default().observer(), app);
    }
    assert_eq!(opened_epochs(&bus, 0x42), vec![1, 2]);
}

#[tokio::test]
async fn a_rebuild_after_an_abort_never_reuses_the_aborted_epoch() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    for round in 0..2u8 {
        let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
            .with_address(0x42)
            .with_storage(&mut state)
            .build()
            .unwrap();
        let app = async {
            port.write_all(&[round]).await.unwrap();
            port.flush().await.unwrap();
            if round == 0 {
                port.abort();
                // The retirement goes out on the runner's next turns.
                for _ in 0..20 {
                    tokio::task::yield_now().await;
                }
            }
        };
        run_until!(runner, Events::default().observer(), app);
    }
    assert_eq!(opened_epochs(&bus, 0x42), vec![1, 2]);
}

#[tokio::test]
async fn a_rebuild_after_an_ambiguous_transfer_never_reuses_the_epoch() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    for round in 0..2u8 {
        bus.wire(0x42).fail_data_writes = round == 0;
        let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
            .with_address(0x42)
            .with_storage(&mut state)
            .build()
            .unwrap();
        let app = async {
            port.write_all(b"abc").await.unwrap();
            let _ = port.flush().await;
        };
        run_until!(runner, Events::default().observer(), app);
    }
    assert_eq!(opened_epochs(&bus, 0x42), vec![1, 2]);
}

#[tokio::test]
async fn a_route_keeps_its_epoch_history_when_the_routes_are_opened_in_another_order() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.add(0x43);
    let mut state = ControllerState::<2>::new();
    {
        // The first build gives 0x42 two epochs, and 0x43 one.
        let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
            .with_storage(&mut state)
            .build()
            .unwrap();
        let mut a = controller.open(0x42).unwrap();
        let _b = controller.open(0x43).unwrap();
        let runner = controller.into_runner();
        let attachment = a.attachment();
        let app = async {
            a.write_all(b"one").await.unwrap();
            a.flush().await.unwrap();
            a.abort();
            drop(a);
            let mut a = attachment.attach().await.unwrap();
            a.write_all(b"two").await.unwrap();
            a.flush().await.unwrap();
        };
        run_until!(runner, Events::default().observer(), app);
    }
    {
        // The second build opens 0x43 first, so the routes trade places.
        let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
            .with_storage(&mut state)
            .build()
            .unwrap();
        let mut b = controller.open(0x43).unwrap();
        let mut a = controller.open(0x42).unwrap();
        let runner = controller.into_runner();
        let app = async {
            b.write_all(b"b").await.unwrap();
            b.flush().await.unwrap();
            a.write_all(b"a").await.unwrap();
            a.flush().await.unwrap();
        };
        run_until!(runner, Events::default().observer(), app);
    }
    assert_eq!(opened_epochs(&bus, 0x42).len(), 3);
    assert_fresh_epochs(&opened_epochs(&bus, 0x42));
    assert_fresh_epochs(&opened_epochs(&bus, 0x43));
}

#[tokio::test]
async fn a_target_already_serving_the_first_candidate_is_not_adopted() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).pre_serve(1);
    bus.wire(0x42).outgoing.extend(b"stale");
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let events = Events::default();
    let app = async {
        // The stale bytes belong to epoch 1, which the controller must not
        // adopt. Wait until a read has handed them over, then send fresh bytes.
        while !bus.wire(0x42).outgoing.is_empty() {
            tokio::task::yield_now().await;
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        bus.wire(0x42).outgoing.extend(b"fresh");
        let mut buf = [0u8; 16];
        let n = port.read(&mut buf).await.unwrap();
        buf[..n].to_vec()
    };
    let got = run_until!(runner, events.observer(), app);
    assert_eq!(got, b"fresh", "the stale bytes never reach the port");
    assert_eq!(opened_epochs(&bus, 0x42), vec![2]);
    assert!(events.faults().is_empty());
}

/// Builds a controller with one route to `address` on `state`, writes one byte
/// and flushes it, so that one connection is opened and the build is dropped.
async fn open_once(bus: &WireBus, state: &mut ControllerState<2>, address: u8) {
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(address)
        .with_storage(state)
        .build()
        .unwrap();
    let app = async {
        port.write_all(&[0]).await.unwrap();
        port.flush().await.unwrap();
    };
    run_until!(runner, Events::default().observer(), app);
}

#[tokio::test]
async fn a_seed_at_the_highest_epoch_wraps_past_zero_without_repeating() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<2>::new().with_epoch_seed(u16::MAX);
    for _ in 0..3 {
        open_once(&bus, &mut state, 0x42).await;
    }
    assert_eq!(opened_epochs(&bus, 0x42), vec![1, 2, 3]);
}

#[tokio::test]
async fn a_seed_just_below_the_highest_epoch_skips_zero_after_the_wrap() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<2>::new().with_epoch_seed(u16::MAX - 1);
    for _ in 0..3 {
        open_once(&bus, &mut state, 0x42).await;
    }
    assert_eq!(opened_epochs(&bus, 0x42), vec![u16::MAX, 1, 2]);
}

#[tokio::test]
async fn an_advertised_epoch_is_skipped_at_the_wrap() {
    let bus = WireBus::default();
    bus.add(0x42);
    // The target still serves epoch 1, which the candidate after the wrap is.
    bus.wire(0x42).pre_serve(1);
    let mut state = ControllerState::<2>::new().with_epoch_seed(u16::MAX);
    open_once(&bus, &mut state, 0x42).await;
    assert_eq!(opened_epochs(&bus, 0x42), vec![2]);
}

#[tokio::test]
async fn an_advertised_epoch_is_skipped_just_before_the_wrap() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).pre_serve(u16::MAX);
    let mut state = ControllerState::<2>::new().with_epoch_seed(u16::MAX - 1);
    open_once(&bus, &mut state, 0x42).await;
    assert_eq!(opened_epochs(&bus, 0x42), vec![1]);
}

#[tokio::test]
async fn a_seed_set_on_used_state_keeps_used_histories_and_seeds_unused_addresses() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.add(0x44);
    let mut state = ControllerState::<2>::new();
    open_once(&bus, &mut state, 0x42).await;
    let mut state = state.with_epoch_seed(500);
    // 0x42 has a history, so the seed does not rewind it; 0x44 never allocated.
    open_once(&bus, &mut state, 0x42).await;
    open_once(&bus, &mut state, 0x44).await;
    assert_eq!(opened_epochs(&bus, 0x42), vec![1, 2]);
    assert_eq!(opened_epochs(&bus, 0x44), vec![501]);
}

#[tokio::test]
async fn bytes_an_unbound_target_wrote_before_the_first_open_reach_the_port() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).outgoing.extend(b"early");
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let app = async {
        let mut buf = [0u8; 16];
        let n = port.read(&mut buf).await.unwrap();
        buf[..n].to_vec()
    };
    let got = run_until!(runner, Events::default().observer(), app);
    assert_eq!(got, b"early");
    assert_eq!(opened_epochs(&bus, 0x42), vec![1]);
}

#[tokio::test]
async fn failed_discovery_reads_spend_the_budget_before_the_boot_is_known() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).fail_reads_from = Some(0);
    let mut state = ControllerState::<1>::new();
    let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_open_attempts(3)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let runner = controller.into_runner();
    let events = Events::default();
    let app = async {
        let mut buf = [0u8; 4];
        let read = port.read(&mut buf).await;
        // Once the budget is spent, nothing else is attempted on the bus.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        read
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    assert_eq!(bus.wire(0x42).reads, 3);
    assert_eq!(
        events.faults(),
        vec![
            RouteFault::OpenFailed(BusFault),
            RouteFault::OpenFailed(BusFault),
            RouteFault::OpenFailed(BusFault),
            RouteFault::OpenExhausted,
        ]
    );
}

#[tokio::test]
async fn refused_and_failed_opens_spend_the_same_budget() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).fail_writes = true;
    let mut state = ControllerState::<1>::new();
    let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_open_attempts(3)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let runner = controller.into_runner();
    let events = Events::default();
    let app = async {
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    assert_eq!(
        events.faults(),
        vec![
            RouteFault::OpenFailed(BusFault),
            RouteFault::OpenFailed(BusFault),
            RouteFault::OpenFailed(BusFault),
            RouteFault::OpenExhausted,
        ]
    );
}

#[tokio::test]
async fn the_last_permitted_open_is_still_confirmed() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).refuse_open = 2;
    let mut state = ControllerState::<1>::new();
    let mut controller = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_open_attempts(3)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let runner = controller.into_runner();
    let events = Events::default();
    let app = async {
        port.write_all(b"late").await.unwrap();
        port.flush().await.unwrap();
    };
    run_until!(runner, events.observer(), app);
    assert_eq!(opened_epochs(&bus, 0x42).len(), 3);
    assert_eq!(bus.wire(0x42).received, b"late");
    assert!(events.faults().is_empty());
}

#[tokio::test]
async fn an_unanswered_open_waits_for_its_retry_deadline() {
    let retry = Duration::from_millis(10);
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).refuse_open = usize::MAX;
    let timer = VirtualTimer::default();
    let events = TimedEvents::new(&timer);
    let mut state = ControllerState::<1>::new();
    let mut controller = i2c_controller(bus.clone(), timer)
        .with_open_attempts(4)
        .with_open_retry_interval(retry)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let runner = controller.into_runner();
    let app = async {
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    // Four opens, each one retry interval after the previous: the budget is
    // spent at four intervals, not at the poll interval.
    assert_eq!(
        *events.log.borrow(),
        vec![(RouteFault::OpenExhausted, retry * 4)]
    );
    assert_eq!(opened_epochs(&bus, 0x42).len(), 4);
}

#[tokio::test]
async fn a_transferring_route_does_not_accelerate_another_routes_retries() {
    let retry = Duration::from_millis(10);
    let bus = WireBus::default();
    bus.add(0x42);
    bus.add(0x43);
    bus.wire(0x42).refuse_open = usize::MAX;
    bus.wire(0x43).pre_serve(1);
    let timer = VirtualTimer::default();
    let events = TimedEvents::new(&timer);
    let mut state = ControllerState::<2>::new();
    let mut controller = i2c_controller(bus.clone(), timer)
        .with_open_attempts(4)
        .with_open_retry_interval(retry)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut stuck = controller.open(0x42).unwrap();
    let mut healthy = controller.open(0x43).unwrap();
    let runner = controller.into_runner();
    let data = pattern(400, 9);
    let app = async {
        healthy.write_all(&data).await.unwrap();
        healthy.flush().await.unwrap();
        let mut buf = [0u8; 8];
        stuck.read(&mut buf).await
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    assert_eq!(
        bus.wire(0x43).received,
        data,
        "the healthy route kept moving"
    );
    assert_eq!(
        *events.log.borrow(),
        vec![(RouteFault::OpenExhausted, retry * 4)],
        "the stuck route kept its own pace"
    );
}

#[tokio::test]
async fn zero_intervals_retry_at_once_and_never_wait() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).refuse_open = 2;
    let timer = VirtualTimer::default();
    let clock = timer.clone();
    let events = Events::default();
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), timer)
        .with_poll_interval(Duration::ZERO)
        .with_open_retry_interval(Duration::ZERO)
        .with_open_attempts(5)
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let app = async {
        port.write_all(b"zero").await.unwrap();
        port.flush().await.unwrap();
    };
    run_until!(runner, events.observer(), app);
    assert_eq!(bus.wire(0x42).received, b"zero");
    assert_eq!(opened_epochs(&bus, 0x42).len(), 3);
    assert!(events.faults().is_empty());
    assert_eq!(clock.now(), Duration::ZERO, "no wait moved the clock");
}

#[tokio::test]
async fn an_abort_discards_received_bytes_even_after_the_runner_processed_it() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).pre_serve(1);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let events = Events::default();
    let app = async {
        port.write_all(b"x").await.unwrap();
        port.flush().await.unwrap();
        bus.wire(0x42).outgoing.extend(b"stale");
        // Wait until the runner has taken the bytes into the port's buffer.
        while !bus.wire(0x42).outgoing.is_empty() {
            tokio::task::yield_now().await;
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        port.abort();
        // The runner retires the connection on its next turns: let it.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let mut buf = [0u8; 16];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(
        read,
        Err(PortError::Reset),
        "the buffered bytes are discarded"
    );
    assert!(events.faults().is_empty());
}

#[tokio::test]
async fn a_retirement_flag_on_a_serving_reply_ends_the_route_before_its_payload_is_taken() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let events = Events::default();
    let app = async {
        port.write_all(b"ping").await.unwrap();
        port.flush().await.unwrap();
        // A serving reply that also asks for retirement: its payload is refused.
        let mut frame = [0u8; MAILBOX_SIZE];
        mailbox::encode_reply(
            &Reply {
                payload: b"bad",
                rx_credit: u8::MAX,
                connection: ConnectionId::new(BOOT, 1),
                serving: true,
                retire: true,
            },
            &mut frame,
        )
        .unwrap();
        bus.wire(0x42).raw_replies.push_back(frame);
        let mut buf = [0u8; 8];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, events.observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    assert_eq!(events.faults(), vec![RouteFault::Retired]);
}

/// A simulated bus on which every transaction takes `cost` of virtual time. It
/// records when each transaction started, and reports each completion on
/// `done`.
struct TracedBus {
    bus: WireBus,
    clock: VirtualTimer,
    cost: Duration,
    starts: Rc<RefCell<Vec<Duration>>>,
    done: tokio::sync::mpsc::UnboundedSender<()>,
}

impl ErrorType for TracedBus {
    type Error = BusFault;
}

impl I2c for TracedBus {
    async fn transaction(
        &mut self,
        address: u8,
        operations: &mut [Operation<'_>],
    ) -> Result<(), BusFault> {
        self.starts.borrow_mut().push(self.clock.now());
        let result = I2c::transaction(&mut self.bus, address, operations).await;
        self.clock.now.set(self.clock.now.get() + self.cost);
        let _ = self.done.send(());
        result
    }
}

#[tokio::test]
async fn a_failed_read_schedules_its_retry_from_when_it_failed() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).fail_reads_from = Some(0);
    let clock = VirtualTimer::default();
    let (done, _completions) = tokio::sync::mpsc::unbounded_channel();
    let starts = Rc::default();
    let traced = TracedBus {
        bus: bus.clone(),
        clock: clock.clone(),
        cost: Duration::from_millis(20),
        starts: Rc::clone(&starts),
        done,
    };
    let mut state = ControllerState::<1>::new();
    let mut controller = i2c_controller(traced, clock)
        .with_open_attempts(2)
        .with_open_retry_interval(Duration::from_millis(10))
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let runner = controller.into_runner();
    let app = async {
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, Events::default().observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    // The first read ends at 20 ms, so its retry is due 10 ms later, at 30 ms.
    assert_eq!(
        *starts.borrow(),
        vec![Duration::ZERO, Duration::from_millis(30)]
    );
}

#[tokio::test]
async fn a_failed_open_schedules_its_retry_from_when_the_write_failed() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).fail_writes = true;
    let clock = VirtualTimer::default();
    let (done, _completions) = tokio::sync::mpsc::unbounded_channel();
    let starts = Rc::default();
    let traced = TracedBus {
        bus: bus.clone(),
        clock: clock.clone(),
        cost: Duration::from_millis(20),
        starts: Rc::clone(&starts),
        done,
    };
    let mut state = ControllerState::<1>::new();
    let mut controller = i2c_controller(traced, clock)
        .with_open_attempts(2)
        .with_open_retry_interval(Duration::from_millis(10))
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let runner = controller.into_runner();
    let app = async {
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, Events::default().observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    // The OPEN starts at 20 ms and fails at 40 ms, so its retry is due at 50 ms.
    let starts = starts.borrow();
    assert_eq!(
        starts[..3],
        [
            Duration::ZERO,
            Duration::from_millis(20),
            Duration::from_millis(50)
        ]
    );
}

#[tokio::test]
async fn an_unconfirmed_open_schedules_its_retry_from_when_the_write_completed() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).refuse_open = usize::MAX;
    let clock = VirtualTimer::default();
    let (done, _completions) = tokio::sync::mpsc::unbounded_channel();
    let starts = Rc::default();
    let traced = TracedBus {
        bus: bus.clone(),
        clock: clock.clone(),
        cost: Duration::from_millis(20),
        starts: Rc::clone(&starts),
        done,
    };
    let mut state = ControllerState::<1>::new();
    let mut controller = i2c_controller(traced, clock)
        .with_open_attempts(3)
        .with_open_retry_interval(Duration::from_millis(10))
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let runner = controller.into_runner();
    let app = async {
        let mut buf = [0u8; 4];
        port.read(&mut buf).await
    };
    let read = run_until!(runner, Events::default().observer(), app);
    assert_eq!(read, Err(PortError::Reset));
    // The refused OPEN is written from 20 ms to 40 ms, so the next read is due
    // at 50 ms, not 10 ms after the turn that started the read.
    let starts = starts.borrow();
    assert_eq!(
        starts[..3],
        [
            Duration::ZERO,
            Duration::from_millis(20),
            Duration::from_millis(50)
        ]
    );
}

#[tokio::test]
async fn an_abort_during_a_discovery_cooldown_finishes_without_another_read_or_the_clock() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).fail_reads_from = Some(0);
    let (failed, mut first_failure) = tokio::sync::mpsc::unbounded_channel();
    let mut state = ControllerState::<1>::new();
    // The clock is held: it never moves, and no wait completes.
    let mut controller = i2c_controller(bus.clone(), NeverTimer)
        .with_open_retry_interval(Duration::from_millis(10))
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let attachment = port.attachment();
    let runner = controller.into_runner();
    let observer = move |_: RouteEvent<BusFault>| {
        let _ = failed.send(());
    };
    let app = async {
        // The failed read starts the cooldown, which the held clock never ends.
        first_failure.recv().await.unwrap();
        port.abort();
        drop(port);
        let port = attachment.attach().await.unwrap();
        drop(port);
    };
    run_until!(runner, observer, app);
    assert_eq!(bus.wire(0x42).reads, 1, "no read after the abort");
}

#[tokio::test]
async fn an_abort_during_an_open_cooldown_retires_the_identity_once_and_never_waits() {
    let bus = WireBus::default();
    bus.add(0x42);
    bus.wire(0x42).refuse_open = usize::MAX;
    let (done, mut completions) = tokio::sync::mpsc::unbounded_channel();
    let traced = TracedBus {
        bus: bus.clone(),
        clock: VirtualTimer::default(),
        cost: Duration::ZERO,
        starts: Rc::default(),
        done,
    };
    let mut state = ControllerState::<1>::new();
    // The clock is held, as above.
    let mut controller = i2c_controller(traced, NeverTimer)
        .with_open_retry_interval(Duration::from_millis(10))
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut port = controller.open(0x42).unwrap();
    let attachment = port.attachment();
    let runner = controller.into_runner();
    let app = async {
        // The discovery read and the refused OPEN complete. The retry is then
        // due at 10 ms, which the held clock never reaches.
        completions.recv().await.unwrap();
        completions.recv().await.unwrap();
        port.abort();
        drop(port);
        let port = attachment.attach().await.unwrap();
        drop(port);
    };
    run_until!(runner, Events::default().observer(), app);
    let (reads, retires) = {
        let wire = bus.wire(0x42);
        let retires = wire
            .sent
            .iter()
            .filter(|sent| matches!(sent, Sent::Retire(_)))
            .count();
        (wire.reads, retires)
    };
    assert_eq!(reads, 1, "no discovery read after the abort");
    assert_eq!(
        opened_epochs(&bus, 0x42).len(),
        1,
        "no OPEN after the abort"
    );
    assert_eq!(retires, 1, "the identity is retired once");
}

/// A bus that refuses every transaction at once, and counts them. Any bus
/// traffic is therefore refused, and visible to the test.
#[derive(Clone, Default)]
struct RefusingBus(Rc<Cell<usize>>);

impl RefusingBus {
    fn transactions(&self) -> usize {
        self.0.get()
    }
}

impl ErrorType for RefusingBus {
    type Error = BusFault;
}

impl I2c for RefusingBus {
    async fn transaction(&mut self, _: u8, _: &mut [Operation<'_>]) -> Result<(), BusFault> {
        self.0.set(self.0.get() + 1);
        Err(BusFault)
    }
}

/// Polls `runner` once, with a waker that does nothing, and asserts that it is
/// still running. Unlike [`poll_once`], it keeps the runner.
fn assert_running(mut runner: Pin<&mut impl Future>) {
    let mut cx = Context::from_waker(Waker::noop());
    assert!(runner.as_mut().poll(&mut cx).is_pending());
}

#[test]
fn an_aborted_idle_generation_finishes_on_the_first_runner_poll() {
    let bus = RefusingBus::default();
    let mut state = ControllerState::<1>::new();
    // The clock never moves, so only a poll of the runner can finish anything.
    let Binding { mut port, runner } = i2c_controller(bus.clone(), NeverTimer)
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let attachment = port.attachment();
    port.abort();
    drop(port);
    let mut runner = pin!(runner.run_observed(|_| {}));
    assert_running(runner.as_mut());
    assert!(
        attachment.try_attach().is_ok(),
        "the first poll finished the generation"
    );
    assert_eq!(bus.transactions(), 0, "an idle abort sends no bus traffic");
}

#[test]
fn an_aborted_idle_generation_with_its_port_retained_finishes_once_the_port_drops() {
    let bus = RefusingBus::default();
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), NeverTimer)
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let attachment = port.attachment();
    port.abort();
    let mut runner = pin!(runner.run_observed(|_| {}));
    assert_running(runner.as_mut());
    assert_eq!(
        attachment.try_attach().err(),
        Some(AttachError::Busy),
        "the held port keeps the generation"
    );
    drop(port);
    assert_running(runner.as_mut());
    assert!(attachment.try_attach().is_ok());
    assert_eq!(bus.transactions(), 0, "an idle abort sends no bus traffic");
}

#[tokio::test]
async fn an_idle_abort_sends_nothing_and_the_next_attachment_transfers() {
    let bus = WireBus::default();
    bus.add(0x42);
    let mut state = ControllerState::<1>::new();
    // An earlier generation is confirmed only once its retry deadline passes, so
    // the clock must move here. The idle abort itself never waits for it.
    let Binding { mut port, runner } = i2c_controller(bus.clone(), VirtualTimer::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let attachment = port.attachment();
    // Reads and requests the target has received so far.
    let traffic = || {
        let wire = bus.wire(0x42);
        (wire.reads, wire.sent.len())
    };
    let app = async {
        // The first generation identifies its target, is served, and then ends.
        port.write_all(b"one").await.unwrap();
        port.flush().await.unwrap();
        port.abort();
        drop(port);
        // Finishing the generation is what makes the route idle again.
        let mut port = attachment.attach().await.unwrap();
        let before = traffic();
        // Aborted while idle, before the runner has noticed this attachment.
        port.abort();
        drop(port);
        let mut port = attachment.attach().await.unwrap();
        assert_eq!(
            traffic(),
            before,
            "no discovery read, OPEN or RETIRE for the aborted idle generation"
        );
        // The fresh attachment establishes and transfers normally. The clock never
        // moves, so the write, a local event, is what makes the runner read the reply.
        bus.wire(0x42).outgoing.extend(b"ok");
        port.write_all(b"two").await.unwrap();
        port.flush().await.unwrap();
        let mut reply = [0u8; 2];
        port.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"ok");
        assert_eq!(bus.wire(0x42).received, b"onetwo");
        assert_eq!(opened_epochs(&bus, 0x42).len(), 2, "a new epoch was opened");
    };
    run_until!(runner, Events::default().observer(), app);
}
