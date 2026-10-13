//! The device service over a Linux serial port, through the same functions the
//! binaries use. A PTY pair stands in for the line: it verifies the RPC path
//! and the transports, not baud timing or physical transmit completion.
#![cfg(all(target_os = "linux", feature = "uart-linux"))]

#[path = "uart_linux/relay.rs"]
mod relay;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::{Duration, Instant};

use embedded_device_example::uart_link::{self, Options, Session, Transport};
use embedded_device_example::{Device, EVENT_OUTPUT_CHANGED, ServiceDispatch};
use protolink_adaptors::uart::linux::LinuxUart;
use relay::{Mode, Relay};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tokio_serial::SerialStream;

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), f)
        .await
        .expect("test timed out")
}

/// A pseudo-terminal pair as two serial ports: the server's end and the client's.
fn line() -> (LinuxUart, LinuxUart) {
    let (server, client) = SerialStream::pair().unwrap();
    (
        LinuxUart::from_serial(server),
        LinuxUart::from_serial(client),
    )
}

/// Serve one device over `uart`, and hand the device back once the link ends.
async fn serve_device(
    uart: LinuxUart,
    transport: Transport,
    device: Device,
) -> (Result<(), String>, Device) {
    let mut handler = ServiceDispatch::new(device);
    let result = uart_link::serve(uart, transport, &mut handler).await;
    (result, handler.into_inner())
}

/// Serve `device` on a task, keeping the device for the end.
fn spawn_server(
    uart: LinuxUart,
    transport: Transport,
    device: Device,
) -> JoinHandle<(Result<(), String>, Device)> {
    tokio::spawn(serve_device(uart, transport, device))
}

/// Wait for a server task to end, within a bound.
async fn finish<T>(server: JoinHandle<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("the server ended after the line went away")
        .unwrap()
}

async fn round_trip(transport: Transport) {
    // The relay keeps both ends of the line alive after the client returns, so
    // the client dropping its port does not end the server by itself.
    let (relay, client_end, server_end) = Relay::start(Mode::Bytes);
    let device = Device {
        uptime_ms: 12_345,
        ..Device::default()
    };

    with_timeout(async {
        let server = spawn_server(server_end, transport, device);
        let session = uart_link::call(client_end, transport, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            session,
            Session {
                uptime_ms: 12_345,
                output_enabled: true,
                events: vec![EVENT_OUTPUT_CHANGED],
            }
        );
        assert!(
            !server.is_finished(),
            "the server keeps serving while the line is up"
        );

        // Now the line goes away. Closing a pseudo-terminal is a hangup, not a
        // graceful HTTP/2 shutdown, so only the end of the server is expected.
        relay.release().await;
        let (_result, device) = finish(server).await;
        assert!(
            device.output_states[0],
            "the unary SetOutput reached the device"
        );
    })
    .await;
}

#[tokio::test]
async fn raw_transport_serves_unary_and_streaming_calls() {
    round_trip(Transport::Raw).await;
}

#[tokio::test]
async fn cobs_transport_serves_unary_and_streaming_calls() {
    round_trip(Transport::Cobs).await;
}

#[tokio::test]
async fn reliable_transport_serves_unary_and_streaming_calls() {
    round_trip(Transport::Reliable).await;
}

#[tokio::test]
async fn deadline_expires_while_other_tasks_keep_running() {
    // No server answers, so every call waits for its deadline. The line stays
    // open, so the failure is a deadline and not a hangup.
    let (_server_end, client_end) = line();
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker = {
        let ticks = ticks.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                ticks.fetch_add(1, SeqCst);
            }
        })
    };

    let started = Instant::now();
    let err = with_timeout(uart_link::call(
        client_end,
        Transport::Reliable,
        Duration::from_millis(200),
    ))
    .await
    .expect_err("the call cannot succeed without a server");
    let elapsed = started.elapsed();
    ticker.abort();

    assert!(err.contains("DeadlineExceeded"), "got {err}");
    // A failed call returns its own error at once: it does not linger on the link.
    assert!(
        elapsed < uart_link::RELIABLE_LINGER,
        "the failed call lingered: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(200),
        "waited for the deadline"
    );
    assert!(
        ticks.load(SeqCst) >= 50,
        "the executor kept running while the call waited"
    );
}

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

#[test]
fn options_name_the_device_baud_transport_and_deadline() {
    let options = uart_link::parse_options(args(&[
        "--device",
        "/dev/ttyUSB0",
        "--transport",
        "cobs",
        "--baud",
        "9600",
        "--deadline-ms",
        "250",
    ]))
    .unwrap();
    assert_eq!(
        options,
        Options {
            device: "/dev/ttyUSB0".into(),
            baud: 9600,
            transport: Transport::Cobs,
            deadline: Duration::from_millis(250),
        }
    );
}

#[test]
fn options_have_defaults_only_for_baud_and_deadline() {
    let options =
        uart_link::parse_options(args(&["--device", "/dev/ttyS1", "--transport", "raw"])).unwrap();
    assert_eq!(options.baud, 115_200);
    assert_eq!(options.deadline, Duration::from_millis(1000));
}

#[test]
fn options_reject_a_missing_or_unknown_transport() {
    let missing = uart_link::parse_options(args(&["--device", "/dev/ttyS1"])).unwrap_err();
    assert!(missing.contains("--transport is required"), "got {missing}");

    let unknown = uart_link::parse_options(args(&["--device", "/dev/ttyS1", "--transport", "tcp"]))
        .unwrap_err();
    assert!(unknown.contains("unknown transport `tcp`"), "got {unknown}");
}

#[test]
fn options_reject_a_missing_device_a_zero_deadline_and_unknown_flags() {
    let no_device = uart_link::parse_options(args(&["--transport", "raw"])).unwrap_err();
    assert!(
        no_device.contains("--device is required"),
        "got {no_device}"
    );

    let zero = uart_link::parse_options(args(&[
        "--device",
        "/dev/ttyS1",
        "--transport",
        "raw",
        "--deadline-ms",
        "0",
    ]))
    .unwrap_err();
    assert!(zero.contains("at least 1"), "got {zero}");

    let flag = uart_link::parse_options(args(&["--reconnect"])).unwrap_err();
    assert!(flag.contains("unknown option"), "got {flag}");
}

// --- Loss of the final acknowledgement ----------------------------------------------

/// The RPC deadline of the fault tests: enough for setup under load, and longer
/// than the first retransmission timeout (250 ms) by a wide margin.
const FAULT_DEADLINE: Duration = Duration::from_secs(2);

struct Outcome {
    session: Result<Session, String>,
    server: JoinHandle<(Result<(), String>, Device)>,
    relay: Relay,
}

async fn run_with_lost_final_ack(mode: Mode, deadline: Duration, grace: Duration) -> Outcome {
    let (relay, client_end, server_end) = Relay::start(mode);
    let device = Device {
        uptime_ms: 777,
        ..Device::default()
    };
    let server = spawn_server(server_end, Transport::Reliable, device);
    let session =
        uart_link::call_with_linger(client_end, Transport::Reliable, deadline, grace).await;
    Outcome {
        session,
        server,
        relay,
    }
}

fn expected_session() -> Session {
    Session {
        uptime_ms: 777,
        output_enabled: true,
        events: vec![EVENT_OUTPUT_CHANGED],
    }
}

/// Sleep until `at` plus a margin.
async fn sleep_past(at: TokioInstant, margin: Duration) {
    tokio::time::sleep_until(at + margin).await;
}

fn fault(hold_replacement_until: Option<Duration>) -> Mode {
    Mode::ReliableFault {
        hold_replacement_until,
    }
}

/// Check the session and that the relay found the frame that completes it.
fn assert_session_and_target(outcome: &Outcome) -> relay::Observed {
    assert_eq!(outcome.session.as_ref().unwrap(), &expected_session());
    let seen = outcome.relay.observed();
    assert!(seen.target.is_some(), "no completing DAT found: {seen:#?}");
    assert_eq!(seen.request_streams.len(), 3, "{seen:#?}");
    seen
}

/// Check that the ACK covering the target was dropped once and the target was
/// sent again. A lingering client always sends that ACK, so this holds for it.
fn assert_ack_lost(outcome: &Outcome) -> relay::Observed {
    let seen = assert_session_and_target(outcome);
    assert_eq!(seen.dropped_acks.len(), 1, "{seen:#?}");
    assert!(seen.dropped_acks[0] > seen.target.unwrap(), "{seen:#?}");
    assert!(
        seen.target_retransmissions() >= 1,
        "the target was retransmitted: {seen:#?}"
    );
    seen
}

#[tokio::test]
async fn lingering_client_recovers_a_lost_final_acknowledgement() {
    let outcome =
        run_with_lost_final_ack(fault(None), FAULT_DEADLINE, uart_link::RELIABLE_LINGER).await;
    let seen = assert_ack_lost(&outcome);
    let requested = seen.final_request_at.unwrap();

    // The replacement ACK was generated while the client lingered, and it
    // arrived while the final call's deadline was still running.
    assert!(!seen.replacement_acks.is_empty(), "{seen:#?}");
    assert!(seen.replacement_acks[0] > seen.target.unwrap(), "{seen:#?}");
    let forwarded = seen
        .replacement_forwarded_at
        .expect("the ACK was forwarded");
    assert!(forwarded < requested + FAULT_DEADLINE, "{seen:#?}");

    // The relay still holds the line, so the server's end is intact. Past the
    // final call's original deadline it is still running: its pending response
    // output completed instead of timing out.
    sleep_past(requested, FAULT_DEADLINE + Duration::from_millis(500)).await;
    assert!(
        !outcome.server.is_finished(),
        "the server ended before the line was released: {seen:#?}"
    );

    outcome.relay.release().await;
    let (result, device) = finish(outcome.server).await;
    let err = result.expect_err("the hangup ends the server with an error");
    assert!(err.contains("Closed"), "got {err}");
    assert!(device.output_states[0]);
}

#[tokio::test]
async fn without_linger_the_lost_final_acknowledgement_is_not_recovered() {
    // The same relay and the same retained endpoints; only the linger differs.
    let outcome = run_with_lost_final_ack(fault(None), FAULT_DEADLINE, Duration::ZERO).await;
    let seen = assert_session_and_target(&outcome);
    let requested = seen.final_request_at.unwrap();

    // Nobody answers the retransmissions, so the server's output stays pending
    // until the final call's deadline interrupts it.
    sleep_past(requested, FAULT_DEADLINE + Duration::from_millis(500)).await;
    let seen = outcome.relay.observed();
    assert!(seen.replacement_acks.is_empty(), "{seen:#?}");
    assert!(
        seen.target_retransmissions() >= 1,
        "the server kept retransmitting: {seen:#?}"
    );
    assert!(
        outcome.server.is_finished(),
        "the server was still running: {seen:#?}"
    );

    outcome.relay.release().await;
    let (result, _device) = finish(outcome.server).await;
    let err = result.expect_err("the deadline ended the server");
    assert!(err.contains("deadline"), "got {err}");
}

#[tokio::test]
async fn a_recovery_slower_than_the_server_deadline_is_too_late() {
    // The replacement ACK is held back until after the final call's deadline,
    // as if the server's remaining deadline were shorter than the time the
    // retransmission needed. The client lingers throughout and the server
    // still fails: linger is a chance for recovery, not a guarantee.
    let deadline = Duration::from_secs(1);
    let hold = deadline + Duration::from_millis(500);
    let outcome =
        run_with_lost_final_ack(fault(Some(hold)), deadline, Duration::from_secs(3)).await;
    let seen = assert_ack_lost(&outcome);
    let requested = seen.final_request_at.unwrap();

    sleep_past(requested, hold + Duration::from_millis(300)).await;
    let seen = outcome.relay.observed();
    assert!(!seen.replacement_acks.is_empty(), "{seen:#?}");
    let forwarded = seen
        .replacement_forwarded_at
        .expect("the ACK was forwarded");
    assert!(forwarded >= requested + hold, "{seen:#?}");
    assert!(outcome.server.is_finished(), "{seen:#?}");

    outcome.relay.release().await;
    let (result, _device) = finish(outcome.server).await;
    let err = result.expect_err("the deadline ended the server");
    assert!(err.contains("deadline"), "got {err}");
}
