//! UART adaptors: the buffered [`uart()`] factory and, on Linux, the serial port
//! wrapper `linux::LinuxUart`.
//!
//! A UART is a full-duplex, point-to-point byte stream. There are three
//! supported paths, and none of them adds COBS or ARQ:
//!
//! | Path | Use it when | Assembly |
//! |---|---|---|
//! | Direct embedded UART | the HAL driver is a cancel-safe duplex `Read + Write` | `protolink::link::reliable(uart)` |
//! | Buffered embedded UART | the RX and TX halves are split, and a transfer may be aborted when its future is dropped (DMA) | `uart(rx, tx).with_storage(&mut state).build()` |
//! | Linux UART | a configured serial device under Tokio | `LinuxUart::open(path, baud)` |
//!
//! The buffered path is a facade over [`protolink::link::pump`]: it adds the
//! construction pattern and buffer defaults, not a second buffer
//! implementation. The UART contract (reads, writes, cancellation, errors,
//! flush meanings) is in `docs/UART.md`.
//!
//! ```no_run
//! use embedded_io_async::{Read, Write};
//! use protolink_adaptors::{Binding, UartState, uart};
//!
//! async fn demo<R: Read, W: Write>(uart_rx: R, uart_tx: W) {
//!     let mut state = UartState::new();
//!     let Binding { port, runner } = uart(uart_rx, uart_tx)
//!         .with_storage(&mut state)
//!         .build();
//!
//!     // Poll `runner.run()` next to the application that uses `port`.
//!     # let _ = (port, runner);
//! }
//! ```

mod buffered;
#[cfg(all(feature = "uart-linux", target_os = "linux"))]
pub mod linux;

pub use buffered::{UartBuilder, UartRunner, UartState, uart};
pub use protolink::link::pump::PumpFailure;
