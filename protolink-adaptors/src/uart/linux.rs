//! A Linux serial port for the Tokio runtime (feature `uart-linux`).
//!
//! [`LinuxUart`] wraps a [`tokio_serial::SerialStream`] and implements Tokio's
//! `AsyncRead` and `AsyncWrite`. It is nonblocking: reads and writes are the
//! stream's own readiness-driven operations, and `flush` never waits for the
//! device to drain.
//!
//! The wrapper adds no framing and no reliability. Give it to protolink through
//! `protolink::tokio::compat`, then choose the stack explicitly:
//!
//! ```no_run
//! use protolink_adaptors::uart::linux::LinuxUart;
//!
//! # async fn demo() -> std::io::Result<()> {
//! let raw = LinuxUart::open("/dev/ttyUSB0", 115_200)?;
//!
//! // Raw: HTTP/2 over the bytes as they are. Only for an error-free line.
//! // let io = protolink::tokio::compat(raw);
//! // COBS framing: `protolink::link::CobsFramed::new(protolink::tokio::compat(raw))`.
//! // Reliable: COBS plus ARQ, with the ARQ timer defaulting to `StdTimer`.
//! let io = protolink::link::reliable(protolink::tokio::compat(raw)).build();
//! let client = protolink::client(io)
//!     .with_deadline_timer(protolink::tokio::TokioTimer::new())
//!     .build();
//! # let _ = client;
//! # Ok(()) }
//! ```
//!
//! # Opening and configuration
//!
//! [`open`](LinuxUart::open) performs I/O: it opens the device node and applies
//! the line settings. It is a separate step from building a protocol stack, and
//! the stack's `build` never opens a device.
//!
//! - **Line settings:** 8 data bits, no parity, one stop bit, no hardware or
//!   software flow control. The port is in raw mode, so `0x00`, newline, `XON`
//!   and `XOFF` pass through unchanged. Both ends of the line must use the same
//!   settings and baud rate. Software flow control (`XON`/`XOFF`) is never
//!   enabled.
//! - **Exclusivity:** `serialport` opens every port in exclusive mode (`TIOCEXCL`
//!   plus an advisory lock), so a second opener fails until this one is dropped.
//!   The kind of that error is the dependency's, and is not promised.
//! - **Permissions:** the process needs read and write access to the device node,
//!   usually through the group that owns it (for example `dialout`).
//! - **DTR:** opening some USB-serial adapters asserts DTR, which resets boards
//!   that use it for auto-reset. The wrapper does not change DTR or RTS.
//! - **Other settings** (a non-standard baud rate, parity, stop bits, flow
//!   control) need a configured [`SerialStream`] given to
//!   [`from_serial`](LinuxUart::from_serial).
//!
//! # Semantics
//!
//! - **Read:** returns the bytes available. An idle line is `Pending`, never
//!   `Ok(0)`. `Ok(0)` from the device is end of stream, and it is passed on
//!   unchanged, as are errors such as a hangup or an I/O error.
//! - **Write:** accepts a prefix, as the kernel does. Dropping a pending write
//!   writes nothing. An empty write completes at once with `Ok(0)`, even while
//!   the output queue is full.
//! - **Flush:** nothing in the wrapper is buffered, so an accepted write is
//!   already with the operating system's driver. `flush` therefore completes at
//!   once. It does **not** call `tcdrain`, and it does not mean that the bytes
//!   have left the transceiver: that is physical wire-idle, which the wrapper does
//!   not provide.
//! - **Shutdown:** the UART has no half-close, so shutdown is the same as
//!   `flush`. It completes at once, does not close the device (that happens when
//!   the wrapper is dropped), and does not show that the peer received the queued
//!   bytes.
//! - **Timeouts:** the stream's `set_timeout` is a no-op in the audited
//!   `tokio-serial` version. Use protolink or Tokio deadlines instead, such as
//!   `protolink::tokio::TokioTimer`.
//!
//! # Why not upstream `poll_flush`
//!
//! `tokio-serial`'s `poll_flush` calls `tcdrain` on the executor thread. That
//! call blocks until the device has transmitted all queued output, which can take
//! seconds at a low baud rate or while flow control holds the line, and it cannot
//! be cancelled. The upstream `poll_flush` also discards the result of the call
//! (`Ok(_)`), so a failed drain is reported as success. The wrapper never calls
//! it. Draining the output to the wire, if an application needs it (for example
//! before a baud-rate change or before switching a transceiver), needs a
//! separate, explicitly designed operation.

extern crate std;

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPortBuilderExt, SerialStream, StopBits};

/// A Linux serial port with nonblocking reads and writes and a `flush` that
/// does not wait for the device. See the [module docs](self).
#[derive(Debug)]
pub struct LinuxUart {
    port: SerialStream,
}

impl LinuxUart {
    /// Open `path` (for example `/dev/ttyUSB0` or `/dev/serial/by-id/...`) at
    /// `baud`, with 8N1 line settings, no flow control and raw mode.
    ///
    /// This performs I/O, and fails with the error the dependency
    /// (`tokio-serial` and `serialport`) reports. The wrapper does not map or
    /// classify it: contention with another holder of the port can appear as
    /// `NotFound`, for example, and errors while opening or configuring the
    /// port may have lost their numeric `errno`. It is not part of `build`.
    pub fn open(path: &str, baud: u32) -> io::Result<Self> {
        let port = tokio_serial::new(path, baud)
            .data_bits(DataBits::Eight)
            .parity(Parity::None)
            .stop_bits(StopBits::One)
            .flow_control(FlowControl::None)
            .open_native_async()?;
        Ok(Self::from_serial(port))
    }

    /// Wrap a port that was opened and configured elsewhere.
    ///
    /// The caller is responsible for raw mode and for line settings that match
    /// the other end.
    pub fn from_serial(port: SerialStream) -> Self {
        Self { port }
    }

    /// The underlying stream, for line control such as `name` or `baud_rate`.
    pub fn get_ref(&self) -> &SerialStream {
        &self.port
    }

    /// The underlying stream, mutably.
    pub fn get_mut(&mut self) -> &mut SerialStream {
        &mut self.port
    }

    /// Give back the underlying stream. Dropping it closes the device.
    pub fn into_inner(self) -> SerialStream {
        self.port
    }
}

impl AsyncRead for LinuxUart {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.port).poll_read(cx, buf)
    }
}

impl AsyncWrite for LinuxUart {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // An empty write completes at once. The stream would otherwise wait for
        // writability, so an empty write could stay pending under backpressure.
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        Pin::new(&mut self.port).poll_write(cx, buf)
    }

    /// Completes at once: the wrapper buffers nothing, so every accepted byte is
    /// already with the operating system's driver. This is not wire-idle.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Completes at once, like [`poll_flush`](Self::poll_flush). The device stays
    /// open until the wrapper is dropped, and the peer's receipt is not implied.
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
