//! Tests of the Linux serial wrapper over pseudo-terminals.
//!
//! No physical device is needed. A PTY pair verifies the API behaviour and the
//! software integration. It cannot verify baud timing, physical transmit
//! completion, or hardware RTS/CTS; those need a real port and a board.
#![cfg(all(target_os = "linux", feature = "uart-linux"))]

use std::future::Future;
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use protolink_adaptors::uart::linux::LinuxUart;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serial::SerialStream;

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("timed out")
}

/// Poll a future once and drop it if it is pending.
fn poll_once<F: Future>(fut: F) -> Option<F::Output> {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// A pseudo-terminal pair: the peer (master) and this end (slave), wrapped.
fn pair() -> (SerialStream, LinuxUart) {
    let (master, slave) = SerialStream::pair().unwrap();
    (master, LinuxUart::from_serial(slave))
}

/// The device node of a PTY slave, so the production open path can reach it.
fn slave_path(port: &SerialStream) -> String {
    let fd = port.as_raw_fd();
    std::fs::read_link(format!("/proc/self/fd/{fd}"))
        .expect("a pty slave has a device node")
        .to_string_lossy()
        .into_owned()
}

/// Every byte value, repeated: includes `0x00`, newline and carriage return,
/// XON (`0x11`), XOFF (`0x13`) and the other control characters a line
/// discipline could act on.
fn all_bytes(len: usize) -> Vec<u8> {
    (0..=255u8).cycle().take(len).collect()
}

/// Fill the output queue of `uart` while its peer reads nothing, and return the
/// number of bytes accepted.
///
/// A single `Pending` proves little: it can be only the registration of
/// readiness interest. The queue counts as full once a write has stayed pending
/// through several waits, each long enough for a readiness event to arrive. A
/// write that times out is dropped, which writes nothing, so the returned count
/// is exactly what the peer will find.
async fn fill_output(uart: &mut LinuxUart, data: &[u8]) -> usize {
    let mut sent = 0;
    let mut stalls = 0;
    with_timeout(async {
        while stalls < 3 {
            assert!(
                sent < data.len(),
                "the output queue never filled: {sent} bytes accepted"
            );
            match tokio::time::timeout(Duration::from_millis(50), uart.write(&data[sent..])).await {
                Ok(n) => {
                    sent += n.unwrap();
                    stalls = 0;
                }
                Err(_) => stalls += 1,
            }
        }
    })
    .await;
    assert!(sent > 0, "some bytes were accepted before the queue filled");
    sent
}

/// Read `expected.len()` bytes from `master`, compare them, and check that no
/// further byte follows.
async fn assert_drained(master: &mut SerialStream, expected: &[u8]) {
    let got = with_timeout(async {
        let mut got = vec![0u8; expected.len()];
        master.read_exact(&mut got).await.unwrap();
        got
    })
    .await;
    assert!(
        got == expected,
        "the peer received exactly the accepted bytes"
    );
    let extra = tokio::time::timeout(Duration::from_millis(100), master.read(&mut [0u8; 16])).await;
    assert!(
        extra.is_err(),
        "no byte beyond the accepted ones: {extra:?}"
    );
}

// --- Data path --------------------------------------------------------------------

#[tokio::test]
async fn binary_bytes_pass_through_unchanged_in_both_directions() {
    let (mut master, mut uart) = pair();
    let data = all_bytes(4096);

    with_timeout(async {
        let to_uart = async { master.write_all(&data).await.unwrap() };
        let from_uart = async {
            let mut got = vec![0u8; data.len()];
            uart.read_exact(&mut got).await.unwrap();
            got
        };
        let ((), got) = tokio::join!(to_uart, from_uart);
        assert_eq!(got, data);

        let to_master = async { uart.write_all(&data).await.unwrap() };
        let from_master = async {
            let mut got = vec![0u8; data.len()];
            master.read_exact(&mut got).await.unwrap();
            got
        };
        let ((), got) = tokio::join!(to_master, from_master);
        assert_eq!(got, data);
    })
    .await;
}

#[tokio::test]
async fn fragmented_reads_return_every_byte_in_order() {
    let (mut master, mut uart) = pair();
    let data = all_bytes(300);

    with_timeout(async {
        let writer = async {
            for chunk in data.chunks(7) {
                master.write_all(chunk).await.unwrap();
                tokio::task::yield_now().await;
            }
        };
        let reader = async {
            // One byte per read: every read is a short read.
            let mut got = Vec::new();
            let mut byte = [0u8; 1];
            while got.len() < data.len() {
                let n = uart.read(&mut byte).await.unwrap();
                assert_eq!(n, 1, "a one-byte buffer yields one byte");
                got.push(byte[0]);
            }
            got
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(got, data);
    })
    .await;
}

#[tokio::test]
async fn idle_read_is_pending_not_end_of_stream() {
    let (mut master, mut uart) = pair();
    let mut buf = [0u8; 16];

    // Nothing has arrived: the read must wait, not return `Ok(0)`.
    let idle = with_timeout(async {
        tokio::time::timeout(Duration::from_millis(50), uart.read(&mut buf)).await
    })
    .await;
    assert!(idle.is_err(), "an idle line keeps the read pending");

    with_timeout(async {
        master.write_all(b"hello").await.unwrap();
        let mut got = [0u8; 5];
        uart.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello");
    })
    .await;
}

#[tokio::test]
async fn cancelling_a_pending_read_loses_nothing() {
    let (mut master, mut uart) = pair();
    let mut buf = [0u8; 16];

    // A read that is polled once and dropped, as ARQ does, repeatedly.
    for _ in 0..3 {
        assert!(poll_once(uart.read(&mut buf)).is_none());
    }

    with_timeout(async {
        master.write_all(b"data").await.unwrap();
        let mut got = [0u8; 4];
        uart.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"data");
    })
    .await;
}

#[tokio::test]
async fn pending_operations_do_not_block_the_executor() {
    let (mut master, mut uart) = pair();
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

    with_timeout(async {
        // An idle read waits for 100 ms. The ticker keeps running meanwhile.
        let before = ticks.load(SeqCst);
        let _ = tokio::time::timeout(Duration::from_millis(100), uart.read(&mut [0u8; 4])).await;
        assert!(
            ticks.load(SeqCst) - before >= 20,
            "the executor kept running while the read waited"
        );
        master.write_all(b"x").await.unwrap();
        assert_eq!(uart.read(&mut [0u8; 4]).await.unwrap(), 1);
    })
    .await;
    ticker.abort();
}

// --- Writes, flush and shutdown -----------------------------------------------------

#[tokio::test]
async fn backpressured_writes_wait_and_then_complete() {
    let (mut master, mut uart) = pair();
    let data = all_bytes(1 << 20);

    // Without a reader the kernel buffer fills, and a write then waits.
    let sent = fill_output(&mut uart, &data).await;
    assert!(sent < data.len(), "the output buffer filled up");

    let got = with_timeout(async {
        let drain = async {
            let mut got = Vec::new();
            let mut buf = [0u8; 4096];
            while got.len() < data.len() {
                let n = master.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            got
        };
        let rest = async { uart.write_all(&data[sent..]).await.unwrap() };
        let (got, ()) = tokio::join!(drain, rest);
        got
    })
    .await;
    assert_eq!(got, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_and_shutdown_complete_without_waiting_for_the_peer() {
    // The peer does not read yet, so the accepted bytes are still queued below
    // the wrapper. Flush must return at once, and must not discard them: the
    // peer reads every accepted byte afterwards.
    //
    // A PTY does not model baud rate, so it cannot show a drain taking long.
    // The multi-threaded runtime makes a blocking flush fail the timeout
    // instead of hanging the test.
    let (mut master, mut uart) = pair();
    let mut sent = 0;
    let data = all_bytes(1 << 16);
    while let Some(n) = poll_once(uart.write(&data[sent..])) {
        sent += n.unwrap();
    }
    assert!(sent > 0, "some bytes were accepted");

    with_timeout(async {
        uart.flush().await.unwrap();
        uart.shutdown().await.unwrap();
    })
    .await;

    let got = with_timeout(async {
        let mut got = vec![0u8; sent];
        master.read_exact(&mut got).await.unwrap();
        got
    })
    .await;
    assert_eq!(got, data[..sent]);
}

#[tokio::test]
async fn write_is_cancel_safe_when_dropped_while_pending() {
    let (mut master, mut uart) = pair();
    let data = all_bytes(1 << 20);

    // Fill the output buffer, then drop pending writes without retrying them.
    let sent = fill_output(&mut uart, &data).await;
    assert!(poll_once(uart.write(&data[sent..])).is_none());
    let late = tokio::time::timeout(Duration::from_millis(50), uart.write(&data[sent..])).await;
    assert!(late.is_err(), "the queue is still full");

    // The bytes accepted so far are exactly the bytes the peer receives.
    assert_drained(&mut master, &data[..sent]).await;
}

#[tokio::test]
async fn empty_write_completes_at_once_under_backpressure() {
    let (mut master, mut uart) = pair();
    let data = all_bytes(1 << 20);
    let sent = fill_output(&mut uart, &data).await;

    // A nonempty write stays pending: the queue is full, not merely unregistered.
    assert!(poll_once(uart.write(&data[sent..])).is_none());
    let nonempty = tokio::time::timeout(Duration::from_millis(100), uart.write(b"x")).await;
    assert!(nonempty.is_err(), "a nonempty write waits for the queue");

    // An empty write has nothing to wait for. It must not wait for writability,
    // neither directly nor through the protolink adapter.
    let direct = poll_once(uart.write(&[])).expect("an empty write completes on its first poll");
    assert_eq!(direct.unwrap(), 0);
    let mut io = protolink::tokio::compat(&mut uart);
    let adapted = poll_once(embedded_io_async::Write::write(&mut io, &[]))
        .expect("an empty write through the adapter completes on its first poll");
    assert_eq!(adapted.unwrap(), 0);

    // It neither wrote nor discarded anything.
    assert_drained(&mut master, &data[..sent]).await;
}

// --- Errors and teardown ----------------------------------------------------------

#[tokio::test]
async fn peer_hangup_ends_operations_instead_of_waiting() {
    let (master, mut uart) = pair();
    drop(master);

    // A hangup is not idleness: the read ends with end of stream, and the write
    // fails with the kernel's I/O error. Both come through unchanged.
    let read = with_timeout(uart.read(&mut [0u8; 8])).await.unwrap();
    assert_eq!(read, 0, "end of stream after the peer hangs up");

    let write = with_timeout(uart.write(b"x")).await;
    let err = write.expect_err("writing to a hung-up peer fails");
    // EIO (5 on Linux): the OS error is kept, not reported as idle.
    assert_eq!(err.raw_os_error(), Some(5), "got {err}");
}

#[tokio::test]
async fn open_missing_device_reports_the_error() {
    let err = LinuxUart::open("/dev/protolink-no-such-device", 115_200)
        .expect_err("a missing device cannot be opened");
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn production_open_path_reaches_the_slave_and_can_reopen() {
    // The pair's slave is reached through the same open the application uses,
    // and the device works again after it has been closed and reopened.
    let (mut master, slave) = SerialStream::pair().unwrap();
    let path = slave_path(&slave);
    drop(slave);

    for round in 0..2u8 {
        let mut uart = LinuxUart::open(&path, 115_200).unwrap();
        let msg = [round, 0x00, b'\n', 0x11, 0x13];
        with_timeout(async {
            master.write_all(&msg).await.unwrap();
            let mut got = [0u8; 5];
            uart.read_exact(&mut got).await.unwrap();
            assert_eq!(got, msg);

            uart.write_all(&msg).await.unwrap();
            uart.flush().await.unwrap();
            let mut back = [0u8; 5];
            master.read_exact(&mut back).await.unwrap();
            assert_eq!(back, msg);
        })
        .await;
    }
}

#[tokio::test]
async fn second_open_fails_while_the_first_handle_is_alive() {
    // Contention is reported by the dependency, and the error kind it chooses is
    // not part of the wrapper's contract; only the failure is.
    let (_master, slave) = SerialStream::pair().unwrap();
    let path = slave_path(&slave);
    drop(slave);

    let first = LinuxUart::open(&path, 115_200).unwrap();
    let second = LinuxUart::open(&path, 115_200);
    assert!(
        second.is_err(),
        "a second opener fails while the port is held"
    );

    drop(first);
    LinuxUart::open(&path, 115_200).expect("the port can be opened again after the drop");
}
