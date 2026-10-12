//! Controller runner behaviour against a scripted bus.

mod common;

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use common::*;
use embedded_io_async::{Read, Write};
use protolink_adaptors::{Binding, ControllerError, ControllerState, PortError, i2c_controller};
use protolink_i2c::mailbox::MAILBOX_PAYLOAD_MAX;
use protolink_i2c::mailbox::MailboxError;

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("test timed out")
}

/// Polls a future once and drops it if it is pending, as ARQ does.
fn poll_once<F: Future>(fut: F) -> Option<F::Output> {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

#[tokio::test]
async fn routes_are_served_round_robin_under_sustained_traffic() {
    let bus = Scripted::default();
    let mut state = ControllerState::<2>::new();
    let mut controller = i2c_controller(bus.clone(), RecordingDelay::default())
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
    tokio::select! {
        r = runner.run() => panic!("runner stopped: {r:?}"),
        () = with_timeout(app) => {}
    }

    let reads = bus.reads();
    assert!(reads.len() >= 20);
    assert!(
        reads.windows(2).all(|w| w[0] != w[1]),
        "routes must alternate: {reads:?}"
    );
    let collect = |address| -> Vec<u8> {
        bus.writes()
            .into_iter()
            .filter(|(a, _)| *a == address)
            .flat_map(|(_, d)| d)
            .collect()
    };
    assert_eq!(collect(0x42), data_a);
    assert_eq!(collect(0x43), data_b);
}

#[tokio::test]
async fn local_data_wakes_an_idle_runner_early() {
    // The poll delay never completes, so only an early wakeup can send.
    let bus = Scripted::default();
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), NeverDelay)
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
    tokio::select! {
        r = runner.run() => panic!("runner stopped: {r:?}"),
        () = with_timeout(app) => {}
    }
    assert_eq!(bus.writes(), vec![(0x42, b"wake".to_vec())]);
}

#[tokio::test]
async fn idle_runner_sleeps_for_the_poll_interval() {
    let delay = RecordingDelay::default();
    let waits = delay.waits_us.clone();
    let mut state = ControllerState::<1>::new();
    let Binding { port, runner } = i2c_controller(Scripted::default(), delay)
        .with_poll_interval(Duration::from_micros(2500))
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let _port = port;
    let stop = async {
        while waits.borrow().len() < 3 {
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        r = runner.run() => panic!("runner stopped: {r:?}"),
        () = with_timeout(stop) => {}
    }
    assert!(waits.borrow().iter().all(|&w| w == 2500));
}

#[tokio::test]
async fn bytes_stay_queued_until_their_transaction_succeeds() {
    let bus = Scripted::default();
    bus.set_fail_writes(true);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), RecordingDelay::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    port.write_all(b"abc").await.unwrap();
    let (flush, result) = tokio::join!(port.flush(), runner.run());
    // The concrete HAL error is preserved.
    assert_eq!(
        result.unwrap_err(),
        ControllerError::Bus {
            address: 0x42,
            error: BusFault
        }
    );
    // The flush failed because the bytes were never consumed.
    assert_eq!(flush, Err(PortError::Hardware));
    assert!(bus.writes().is_empty());
}

#[tokio::test]
async fn a_full_receive_buffer_stops_reads_until_a_whole_payload_fits() {
    let bus = Scripted::default();
    let frames: Vec<Vec<u8>> = (0..6).map(|i| pattern(MAILBOX_PAYLOAD_MAX, i)).collect();
    for frame in &frames {
        bus.respond(0x42, payload_frame(frame, 255));
    }
    // 32 bytes: after one payload only 2 are free, less than a payload.
    let mut state = ControllerState::<1, 32, 32>::with_buffers();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), NeverDelay)
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let app = async {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(bus.reads().len(), 1, "no read without room for a payload");
        // Reading frees room, which wakes the idle runner without any delay.
        let mut got = Vec::new();
        let mut buf = [0u8; 7];
        while got.len() < 6 * MAILBOX_PAYLOAD_MAX {
            let n = port.read(&mut buf).await.unwrap();
            got.extend_from_slice(&buf[..n]);
        }
        got
    };
    let got = tokio::select! {
        r = runner.run() => panic!("runner stopped: {r:?}"),
        got = with_timeout(app) => got,
    };
    // 30-byte payloads in a 32-byte ring wrap around constantly.
    assert_eq!(got, frames.concat());
}

#[tokio::test]
async fn a_blocked_route_does_not_starve_the_others() {
    let bus = Scripted::default();
    for i in 0..50 {
        bus.respond(0x42, payload_frame(&pattern(MAILBOX_PAYLOAD_MAX, i), 255));
        bus.respond(0x43, payload_frame(&pattern(MAILBOX_PAYLOAD_MAX, i), 255));
    }
    let mut state = ControllerState::<2>::new();
    let mut controller = i2c_controller(bus.clone(), RecordingDelay::default())
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
    tokio::select! {
        r = runner.run() => panic!("runner stopped: {r:?}"),
        () = with_timeout(app) => {}
    }
}

#[tokio::test]
async fn a_malformed_mailbox_is_a_protocol_failure() {
    let bus = Scripted::default();
    let mut bad = idle_frame(255);
    bad[0] = 31;
    bus.respond(0x42, bad);
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus, RecordingDelay::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let mut buf = [0u8; 4];
    let (read, result) = tokio::join!(port.read(&mut buf), runner.run());
    assert_eq!(
        result.unwrap_err(),
        ControllerError::Mailbox {
            address: 0x42,
            error: MailboxError::InvalidPayloadLength(31)
        }
    );
    assert_eq!(read, Err(PortError::Protocol));
}

#[tokio::test]
async fn failure_delivers_buffered_data_then_wakes_and_fails_pending_operations() {
    let bus = Scripted::default();
    bus.respond(0x42, payload_frame(b"last", 255));
    bus.fail_reads_after(1);
    let mut state = ControllerState::<1>::new();
    let Binding { port, runner } = i2c_controller(bus, RecordingDelay::default())
        .with_address(0x42)
        .with_storage(&mut state)
        .build()
        .unwrap();
    let (mut reader, mut writer) = port.into_split();
    // Fill the send buffer (no credit is ever used up because of the failure).
    let result = runner.run().await;
    assert!(matches!(result, Err(ControllerError::Bus { .. })));
    let mut buf = [0u8; 16];
    // Bytes received before the failure are still delivered.
    assert_eq!(reader.read(&mut buf).await, Ok(4));
    assert_eq!(&buf[..4], b"last");
    assert_eq!(reader.read(&mut buf).await, Err(PortError::Hardware));
    assert_eq!(writer.write(b"x").await, Err(PortError::Hardware));
    assert_eq!(writer.flush().await, Ok(()), "nothing is left to flush");
}

#[tokio::test]
async fn pending_operations_are_woken_when_the_runner_is_dropped() {
    let mut state = ControllerState::<1>::new();
    let Binding { port, runner } = i2c_controller(Scripted::default(), RecordingDelay::default())
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
    let Binding { mut port, runner } =
        i2c_controller(Scripted::default(), RecordingDelay::default())
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
async fn canceled_port_operations_lose_nothing_and_can_be_repolled() {
    let bus = Scripted::default();
    let mut state = ControllerState::<1>::new();
    let Binding { mut port, runner } = i2c_controller(bus.clone(), RecordingDelay::default())
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
    bus.respond(0x42, payload_frame(b"late", 255));
    let app = async {
        port.flush().await.unwrap();
        let n = port.read(&mut buf).await.unwrap();
        (n, buf)
    };
    let (n, buf) = tokio::select! {
        r = runner.run() => panic!("runner stopped: {r:?}"),
        out = with_timeout(app) => out,
    };
    assert_eq!(&buf[..n], b"late");
    let sent: Vec<u8> = bus.writes().into_iter().flat_map(|(_, d)| d).collect();
    assert_eq!(sent, vec![7u8; 64]);
}

#[tokio::test]
async fn write_accepts_a_prefix_when_space_is_short() {
    let mut state = ControllerState::<1, 64, 8>::with_buffers();
    let Binding { mut port, runner } =
        i2c_controller(Scripted::default(), RecordingDelay::default())
            .with_address(0x42)
            .with_storage(&mut state)
            .build()
            .unwrap();
    assert_eq!(port.write(&[1u8; 20]).await, Ok(8));
    drop(runner);
}

// Keep the helper used even if a future test drops it.
#[allow(dead_code)]
async fn _poll_fn_is_available() {
    poll_fn(|_| Poll::Ready(())).await;
}
