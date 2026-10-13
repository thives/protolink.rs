//! Buffered ports and background runners that connect protolink to hardware
//! buses.
//!
//! The vocabulary is the same as for the rest of protolink: **create a factory
//! → configure it with `with_*` → `build` → drive the runner with `run`**:
//!
//! - an *adaptor factory* such as [`i2c_controller`] or [`i2c_target`] selects
//!   the hardware role;
//! - `with_*` methods configure it, and
//!   [`with_storage`](ControllerBuilder::with_storage) attaches the
//!   caller-owned *storage* ([`ControllerState`], [`TargetState`]), which
//!   holds the buffers, lives in a local or a `static`, and names no HAL type;
//! - `build` validates and constructs, without any bus traffic, and returns
//!   a [`Binding`] of a port and a runner (or, for a controller of several
//!   peers, a [`Controller`] to open them on);
//! - the [`Port`] is the application's byte stream and implements
//!   [`embedded_io_async::Read`] and [`Write`](embedded_io_async::Write);
//! - the *runner* owns the hardware and moves the bytes. **Nothing happens
//!   until its `run` future is polled**, so drive it concurrently with the
//!   code using the port: no task is ever spawned for you.
//!
//! ```no_run
//! use core::future::Future;
//! use embedded_hal_async::{delay::DelayNs, i2c::I2c};
//! use embedded_io_async::{Read, Write};
//! use protolink_adaptors::{Binding, ControllerState, i2c_controller};
//!
//! async fn demo<I: I2c, D: DelayNs>(i2c: I, delay: D) {
//!     let mut state = ControllerState::<1>::new();
//!     let Binding { mut port, runner } = i2c_controller(i2c, delay)
//!         .with_address(0x42)
//!         .with_storage(&mut state)
//!         .build()
//!         .unwrap();
//!
//!     let application = async {
//!         port.write(b"hello").await.unwrap();
//!         port.flush().await.unwrap();
//!         let mut buf = [0u8; 16];
//!         let n = port.read(&mut buf).await.unwrap();
//!         # let _ = n;
//!     };
//!     // Poll both together, e.g. with `embassy_futures::select` or
//!     // `futures::future::select`. The runner only returns on failure.
//!     # async fn either<A: Future, B: Future>(_: A, _: B) {}
//!     either(runner.run(), application).await;
//! }
//! ```
//!
//! Hand the port to protolink like any stream: `protolink::client(port)`,
//! `protolink::server(port)`, or first `protolink::link::reliable(port)` for
//! COBS framing and ARQ. The adaptors never add COBS or ARQ themselves, since
//! that would be an invisible wire-format and timer choice.
//!
//! # Three different timers
//!
//! - the controller *polling delay* (`DelayNs`, see
//!   [`ControllerBuilder::with_poll_interval`]);
//! - the ARQ *retransmission timer* of the reliable link;
//! - the RPC *deadline timer* of the protolink client and server.
//!
//! They are not interchangeable.
//!
//! # UART
//!
//! [`uart()`] is the buffered factory for a split, full-duplex UART. It is a thin
//! facade over [`protolink::link::pump`], and the runner drives both halves.
//! Drivers that are already cancel-safe need no adaptor and are passed to
//! protolink directly. See `docs/UART.md` for the contract.
//!
//! # Features
//!
//! - `i2c` (default): the I2C adaptors.
//! - `uart`: the buffered UART factory, [`uart()`] and [`UartState`].
//! - `uart-linux`: the Linux serial port wrapper, `uart::linux::LinuxUart`,
//!   over `tokio-serial`. Compiled on Linux only; other targets ignore it.
//! - `portable-atomic`, `portable-atomic-critical-section`: forwarded to
//!   protolink, for targets without native atomics.
#![no_std]
#![warn(missing_docs)]
#![forbid(unsafe_code)]

// Without a bus feature nothing constructs a channel.
#[cfg_attr(not(feature = "i2c"), allow(dead_code))]
mod channel;
pub use channel::{DEFAULT_BUFFER_SIZE, Port, PortError, PortReader, PortWriter};

/// A port and the runner that drives it, as returned by `build`.
///
/// The fields are named so that the ownership split is obvious: the `port`
/// goes to the application, the `runner` owns the hardware and must be driven
/// concurrently.
pub struct Binding<P, R> {
    /// The application-facing byte stream.
    pub port: P,
    /// Owns the hardware and drives it. Must be polled for the port to work.
    pub runner: R,
}

/// Storage of a factory that has none attached yet; `build` needs it.
///
/// Shared by the I2C and UART factories.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoStorage;

#[cfg(feature = "i2c")]
mod i2c;
#[cfg(feature = "i2c")]
pub use i2c::{
    AnyPeers, BuildError, Controller, ControllerBuilder, ControllerError, ControllerRunner,
    ControllerState, SinglePeer, TargetBuilder, TargetError, TargetRunner, TargetState,
    i2c_controller, i2c_target,
};

#[cfg(feature = "uart")]
pub mod uart;
#[cfg(feature = "uart")]
pub use uart::{UartBuilder, UartRunner, UartState, uart};
