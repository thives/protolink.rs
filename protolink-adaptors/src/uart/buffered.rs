//! The buffered UART facade: a [`PumpState`] around two separately owned HAL
//! halves.
//!
//! The facade adds no buffering of its own. The rings, the staging buffer, the
//! flush protocol and the error propagation are all in [`crate::pump`]; this
//! module provides the adaptor vocabulary (`uart(..).with_storage(..)
//! .build()`), buffer defaults sized for the pump, and a runner that owns both
//! halves.

use core::convert::Infallible;

use embedded_io_async::{Read, Write};

use crate::pump::{
    self, CHUNK_SIZE, PumpFailure, PumpHandle, PumpState, RING_SIZE, RxPump, TxPump,
};
use crate::{Binding, NoStorage};

/// Storage for a buffered UART: the receive and transmit rings and the receive
/// staging buffer of [`PumpState`].
///
/// `RX` and `TX` are the ring sizes and `CHUNK` the largest single HAL
/// transfer. The defaults are [`RING_SIZE`] and [`CHUNK_SIZE`], which hold two
/// encoded COBS frames per ring. They are not the 64-byte defaults of the I2C
/// channel. The state holds no HAL driver and no heap, so it can live in a
/// local or a `static`.
pub struct UartState<
    const RX: usize = RING_SIZE,
    const TX: usize = RING_SIZE,
    const CHUNK: usize = CHUNK_SIZE,
> {
    pump: PumpState<RX, TX, CHUNK>,
}

impl UartState {
    /// State with the default buffer sizes.
    pub const fn new() -> Self {
        Self::with_buffers()
    }
}

impl Default for UartState {
    fn default() -> Self {
        Self::new()
    }
}

impl<const RX: usize, const TX: usize, const CHUNK: usize> UartState<RX, TX, CHUNK> {
    /// State with `RX` receive and `TX` transmit ring bytes, and `CHUNK`-byte
    /// HAL transfers. Every size must be non-zero.
    pub const fn with_buffers() -> Self {
        Self {
            pump: PumpState::new(),
        }
    }
}

/// Starts building a buffered UART on the two halves of a split HAL UART.
///
/// `rx` and `tx` are the receive and transmit halves of one UART and must be
/// independently owned: the runner drives each from its own transfer. A driver
/// that cannot be split must be wrapped first, or used directly if it is already
/// cancel-safe.
///
/// ```no_run
/// use protolink_adaptors::{Binding, UartState, uart};
///
/// async fn demo<R, W>(uart_rx: R, uart_tx: W)
/// where
///     R: embedded_io_async::Read,
///     W: embedded_io_async::Write,
/// {
///     let mut state = UartState::new();
///     let Binding { port, runner } = uart(uart_rx, uart_tx).with_storage(&mut state).build();
///     // Drive the runner concurrently with the code using the port.
///     # let _ = (port, runner);
/// }
/// ```
pub fn uart<R: Read, W: Write>(rx: R, tx: W) -> UartBuilder<R, W> {
    UartBuilder {
        rx,
        tx,
        storage: NoStorage,
    }
}

/// Factory of a buffered UART, created by [`uart()`].
///
/// `S` is [`NoStorage`] or the attached `&mut` [`UartState`]. Only the latter
/// can be built.
pub struct UartBuilder<R, W, S = NoStorage> {
    rx: R,
    tx: W,
    storage: S,
}

impl<R, W, S> UartBuilder<R, W, S> {
    /// Attach the caller-owned storage, which the port and the runner borrow.
    pub fn with_storage<const RX: usize, const TX: usize, const CHUNK: usize>(
        self,
        state: &mut UartState<RX, TX, CHUNK>,
    ) -> UartBuilder<R, W, &mut UartState<RX, TX, CHUNK>> {
        UartBuilder {
            rx: self.rx,
            tx: self.tx,
            storage: state,
        }
    }
}

impl<'a, R: Read, W: Write, const RX: usize, const TX: usize, const CHUNK: usize>
    UartBuilder<R, W, &'a mut UartState<RX, TX, CHUNK>>
{
    /// Construct the port and the runner.
    ///
    /// There is nothing to validate, so this cannot fail. It performs no
    /// hardware operation and spawns nothing: the HAL halves are first touched
    /// when the runner is polled.
    pub fn build(self) -> Binding<PumpHandle<'a, RX, TX>, UartRunner<'a, R, W, RX, TX, CHUNK>> {
        let state = self.storage;
        let (port, rx, tx) = state.pump.split(self.rx, self.tx);
        Binding {
            port,
            runner: UartRunner { rx, tx },
        }
    }
}

/// Owns both HAL halves of a buffered UART and moves the bytes in and out of
/// the rings.
///
/// Created by [`UartBuilder::build`]. [`run`](Self::run) must be polled for the
/// port to make progress. It consumes the runner, so both halves are dropped
/// when it returns or is dropped. The port then fails instead of waiting for a
/// runner that is gone. A cancelled runner fails both directions with
/// `BrokenPipe`. After a HAL failure the failing direction reports the HAL's
/// error kind, and the other direction reports `BrokenPipe`. Bytes already
/// received are read first.
pub struct UartRunner<'a, R, W, const RX: usize, const TX: usize, const CHUNK: usize> {
    rx: RxPump<'a, R, RX, CHUNK>,
    tx: TxPump<'a, W, TX, CHUNK>,
}

impl<R: Read, W: Write, const RX: usize, const TX: usize, const CHUNK: usize>
    UartRunner<'_, R, W, RX, TX, CHUNK>
{
    /// Drives both directions until one HAL half fails.
    ///
    /// It never returns `Ok`. A failure of either half is returned as
    /// [`PumpFailure`], which keeps the concrete HAL error (`Rx` or `Tx`). If
    /// the receive half reaches end of stream, transmit keeps running. The
    /// runner's halves are dropped on return, so the port reports the failure
    /// after its buffered receive data.
    ///
    /// Treat a returned failure as the end of this session. Recovery means
    /// building the UART, link and RPC stack again; restarting the runner does
    /// not revive a terminal ARQ or RPC state.
    pub async fn run(mut self) -> Result<Infallible, PumpFailure<R::Error, W::Error>> {
        pump::run(&mut self.rx, &mut self.tx).await
    }
}
