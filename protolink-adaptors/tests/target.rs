//! Target runner behaviour, with a raw simulated controller on the other end.

mod common;

use common::*;
use embedded_hal_async::i2c::I2c;
use embedded_io_async::{Read, Write};
use protolink_adaptors::{Binding, PortError, TargetError, TargetState, i2c_target};
use protolink_i2c::mailbox::{MAILBOX_SIZE, decode_response};
use protolink_i2c::target::{Command, I2cTarget, ReadStatus};

async fn read_frame(controller: &mut SimController) -> [u8; MAILBOX_SIZE] {
    let mut frame = [0u8; MAILBOX_SIZE];
    controller.read(ADDRESS, &mut frame).await.unwrap();
    frame
}

#[tokio::test]
async fn a_read_gets_one_whole_mailbox_and_consumes_only_when_done() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()))
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());

    let app = async {
        // An empty mailbox is idle and advertises the free receive space.
        let frame = read_frame(&mut controller).await;
        let response = decode_response(&frame).unwrap();
        assert!(response.payload.is_empty());
        assert_eq!(response.rx_credit, 64);

        // 40 bytes need two mailboxes of at most 30.
        let data = pattern(40, 9);
        port.write_all(&data).await.unwrap();
        let first = read_frame(&mut controller).await;
        let first = decode_response(&first).unwrap();
        assert_eq!(first.payload, &data[..30]);
        let second = read_frame(&mut controller).await;
        assert_eq!(decode_response(&second).unwrap().payload, &data[30..]);
        port.flush().await.unwrap();

        // Writes go to the port; the credit shrinks accordingly.
        controller.write(ADDRESS, &[1, 2, 3]).await.unwrap();
        let frame = read_frame(&mut controller).await;
        assert_eq!(decode_response(&frame).unwrap().rx_credit, 61);
        let mut buf = [0u8; 8];
        let n = port.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], &[1, 2, 3]);
    };
    tokio::select! {
        r = runner.run() => panic!("runner stopped: {r:?}"),
        () = with_timeout(app) => {}
    }
}

#[tokio::test]
async fn reads_are_still_answered_when_the_receive_queue_is_full() {
    let bus = SharedBus::default();
    let mut state = TargetState::<64, 64>::with_buffers();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()))
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let app = async {
        let chunk = [0xAB; MAILBOX_SIZE];
        controller.write(ADDRESS, &chunk).await.unwrap();
        controller.write(ADDRESS, &chunk).await.unwrap();
        port.write_all(b"reply").await.unwrap();
        let frame = read_frame(&mut controller).await;
        let response = decode_response(&frame).unwrap();
        assert_eq!(response.rx_credit, 0, "the queue is full");
        assert_eq!(response.payload, b"reply");
    };
    tokio::select! {
        r = runner.run() => panic!("runner stopped: {r:?}"),
        () = with_timeout(app) => {}
    }
}

#[tokio::test]
async fn a_write_beyond_the_advertised_credit_is_terminal_and_buffered_data_survives() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()))
        .with_storage(&mut state)
        .build();
    let mut controller = SimController(bus.clone());
    let chunk = [0xAB; MAILBOX_SIZE];
    let writer = async {
        // The queue holds two chunks, so the third write overflows.
        for _ in 0..3 {
            controller.write(ADDRESS, &chunk).await.unwrap();
        }
    };
    let (result, ()) = with_timeout(async { tokio::join!(runner.run(), writer) }).await;
    assert_eq!(result.unwrap_err(), TargetError::ReceiveOverflow);
    assert_eq!(bus.disarmed(), 1, "a failed target is disarmed");

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
    assert_eq!(got, 2 * MAILBOX_SIZE);
    assert!(buf[..got].iter().all(|&b| b == 0xAB));
}

#[tokio::test]
async fn an_incomplete_read_keeps_the_payload_queued() {
    for fault in [RespondFault::Short, RespondFault::Leftover] {
        let bus = SharedBus::default();
        bus.respond_with(fault);
        let mut state = TargetState::new();
        let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()))
            .with_storage(&mut state)
            .build();
        let mut controller = SimController(bus.clone());
        port.write_all(b"keep").await.unwrap();
        let reader = async { read_frame(&mut controller).await };
        let (result, _) = with_timeout(async { tokio::join!(runner.run(), reader) }).await;
        let expected = match fault {
            RespondFault::Short => ReadStatus::NeedMoreBytes,
            RespondFault::Leftover => ReadStatus::LeftoverBytes(1),
        };
        assert_eq!(result.unwrap_err(), TargetError::ReadStatus(expected));
        assert_eq!(bus.disarmed(), 1);
        // Nothing was consumed, so the flush cannot complete.
        assert_eq!(port.flush().await, Err(PortError::Protocol));
    }
}

#[tokio::test]
async fn hardware_failure_wakes_pending_operations_and_preserves_the_error() {
    let bus = SharedBus::default();
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()))
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
        let Binding { mut port, runner } = i2c_target(SimTarget(bus.clone()))
            .with_storage(&mut state)
            .build();
        let mut buf = [0u8; 1];
        tokio::select! {
            r = runner.run() => panic!("runner stopped: {r:?}"),
            () = tokio::task::yield_now() => (),
        }
        // The canceled run future dropped the runner.
        assert_eq!(bus.disarmed(), 1);
        assert_eq!(port.read(&mut buf).await, Err(PortError::Closed));
    }
    // The state is reusable afterwards and starts empty.
    let Binding { port, runner } = i2c_target(SimTarget(bus.clone()))
        .with_storage(&mut state)
        .build();
    drop(runner);
    assert_eq!(bus.disarmed(), 2);
    let _ = port;
}

/// A target that reports a write longer than the mailbox buffer.
struct OversizedWrite(std::rc::Rc<std::cell::Cell<usize>>);

impl I2cTarget for OversizedWrite {
    type Error = BusFault;

    fn disarm(&mut self) {
        self.0.set(self.0.get() + 1);
    }

    async fn listen(&mut self, _: &mut [u8]) -> Result<Command, BusFault> {
        Ok(Command::Write(MAILBOX_SIZE + 1))
    }

    async fn respond_to_read(&mut self, _: &[u8]) -> Result<ReadStatus, BusFault> {
        unreachable!()
    }
}

#[tokio::test]
async fn an_oversized_write_is_rejected_not_clipped() {
    let disarmed = std::rc::Rc::new(std::cell::Cell::new(0));
    let mut state = TargetState::new();
    let Binding { mut port, runner } = i2c_target(OversizedWrite(disarmed.clone()))
        .with_storage(&mut state)
        .build();
    let result = runner.run().await;
    assert_eq!(
        result.unwrap_err(),
        TargetError::InvalidWrite(MAILBOX_SIZE + 1)
    );
    assert_eq!(disarmed.get(), 1);
    let mut buf = [0u8; 4];
    assert_eq!(port.read(&mut buf).await, Err(PortError::Protocol));
}
