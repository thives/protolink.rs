use core::convert::Infallible;

use protolink_i2c::mailbox::{self, MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE};
use protolink_i2c::target::{Command, I2cTarget, ReadStatus};

use crate::channel::{Channel, DEFAULT_BUFFER_SIZE, Ends, Port, PortError};
use crate::{Binding, NoStorage};

/// Why a [`TargetRunner`] stopped.
///
/// The concrete HAL error is preserved. Every port of the target fails with
/// the matching [`PortError`] once its buffered data is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetError<E> {
    /// The target peripheral failed.
    Hardware(E),
    /// The controller wrote more than the receive credit this target
    /// advertised.
    ReceiveOverflow,
    /// The peripheral reported a write longer than the mailbox buffer.
    InvalidWrite(usize),
    /// The controller did not read exactly one mailbox frame.
    ReadStatus(ReadStatus),
}

impl<E: core::fmt::Debug> core::fmt::Display for TargetError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Hardware(e) => write!(f, "I2C target hardware failed: {e:?}"),
            Self::ReceiveOverflow => f.write_str("controller wrote beyond the advertised credit"),
            Self::InvalidWrite(len) => write!(f, "controller write of {len} bytes is too long"),
            Self::ReadStatus(status) => {
                write!(f, "controller did not read one whole mailbox: {status:?}")
            }
        }
    }
}

impl<E: core::fmt::Debug> core::error::Error for TargetError<E> {}

/// Storage for one I2C target adaptor.
///
/// `RX` and `TX` are the receive and send buffer sizes and default to
/// [`DEFAULT_BUFFER_SIZE`]. The state holds no HAL driver. A built target
/// borrows it mutably until the port and runner are dropped; it can then be
/// used for a new build.
pub struct TargetState<const RX: usize = DEFAULT_BUFFER_SIZE, const TX: usize = DEFAULT_BUFFER_SIZE>
{
    channel: Channel<RX, TX>,
}

impl TargetState {
    /// State with the default buffer sizes.
    pub const fn new() -> Self {
        Self::with_buffers()
    }
}

impl Default for TargetState {
    fn default() -> Self {
        Self::new()
    }
}

impl<const RX: usize, const TX: usize> TargetState<RX, TX> {
    /// State with `RX` receive and `TX` send bytes.
    pub const fn with_buffers() -> Self {
        const {
            assert!(RX > 0, "the receive buffer must not be empty");
            assert!(TX > 0, "the send buffer must not be empty");
        }
        Self {
            channel: Channel::new(),
        }
    }
}

/// Starts building an I2C target adaptor on `driver`.
///
/// The target's own address is configured on `driver` by the board, not here.
///
/// ```no_run
/// use protolink_adaptors::{Binding, TargetState, i2c_target};
/// use protolink_i2c::target::I2cTarget;
///
/// async fn demo<T: I2cTarget>(driver: T) {
///     let mut state = TargetState::new();
///     let Binding { port, runner } = i2c_target(driver).with_storage(&mut state).build();
///     // Drive the runner concurrently with the code using the port.
///     # let _ = (port, runner);
/// }
/// ```
pub fn i2c_target<T: I2cTarget>(driver: T) -> TargetBuilder<T> {
    TargetBuilder {
        driver,
        storage: NoStorage,
    }
}

/// Factory of an I2C target adaptor, created by [`i2c_target`].
///
/// `S` is [`NoStorage`] or the attached `&mut` [`TargetState`].
pub struct TargetBuilder<T, S = NoStorage> {
    driver: T,
    storage: S,
}

impl<T: I2cTarget, S> TargetBuilder<T, S> {
    /// Attach the caller-owned storage, which the port and the runner borrow.
    pub fn with_storage<const RX: usize, const TX: usize>(
        self,
        state: &mut TargetState<RX, TX>,
    ) -> TargetBuilder<T, &mut TargetState<RX, TX>> {
        TargetBuilder {
            driver: self.driver,
            storage: state,
        }
    }
}

impl<'a, T: I2cTarget, const RX: usize, const TX: usize>
    TargetBuilder<T, &'a mut TargetState<RX, TX>>
{
    /// Construct the port and the runner.
    ///
    /// There is nothing to validate, so this cannot fail, and no hardware is
    /// touched until the runner is driven.
    pub fn build(self) -> Binding<Port<'a, RX, TX>, TargetRunner<'a, T, RX, TX>> {
        let (port, ends) = self.storage.channel.split();
        Binding {
            port,
            runner: TargetRunner {
                driver: self.driver,
                ends,
            },
        }
    }
}

/// Services the controller's transactions for one target.
///
/// [`run`](Self::run) answers every read with one whole mailbox and queues
/// every write. Outgoing bytes leave the send buffer only after the
/// peripheral reports the read complete. Reads are answered even when the
/// receive buffer is full: the mailbox then advertises zero credit.
///
/// The runner owns the driver. When it is dropped, finished or canceled, the
/// driver is [disarmed](I2cTarget::disarm) before the buffers are released.
pub struct TargetRunner<
    'a,
    T: I2cTarget,
    const RX: usize = DEFAULT_BUFFER_SIZE,
    const TX: usize = DEFAULT_BUFFER_SIZE,
> {
    driver: T,
    ends: Ends<'a, RX, TX>,
}

impl<T: I2cTarget, const RX: usize, const TX: usize> TargetRunner<'_, T, RX, TX> {
    /// Services controller transactions until the first failure.
    ///
    /// It never returns `Ok`. A hardware error, a write beyond the advertised
    /// credit, an oversized write, or a controller read that is not exactly one
    /// mailbox is terminal: the driver is disarmed, the error is returned and
    /// every port fails with a matching [`PortError`] once its buffered data
    /// is read. Canceling the future disarms the driver too, and the ports
    /// fail with [`PortError::Closed`]. A transaction interrupted this way is
    /// never resumed.
    pub async fn run(mut self) -> Result<Infallible, TargetError<T::Error>> {
        loop {
            if let Err(error) = self.transact().await {
                let kind = match error {
                    TargetError::Hardware(_) => PortError::Hardware,
                    _ => PortError::Protocol,
                };
                self.ends.fail(kind);
                return Err(error);
            }
        }
    }

    async fn transact(&mut self) -> Result<(), TargetError<T::Error>> {
        let mut frame = [0u8; MAILBOX_SIZE];
        match self
            .driver
            .listen(&mut frame)
            .await
            .map_err(TargetError::Hardware)?
        {
            Command::Read => self.respond().await,
            Command::Write(len) => {
                let data = frame.get(..len).ok_or(TargetError::InvalidWrite(len))?;
                if self.ends.push_recv(data) != len {
                    return Err(TargetError::ReceiveOverflow);
                }
                Ok(())
            }
        }
    }

    async fn respond(&mut self) -> Result<(), TargetError<T::Error>> {
        let mut payload = [0u8; MAILBOX_PAYLOAD_MAX];
        let len = self.ends.peek_send(&mut payload);
        let mut frame = [0u8; MAILBOX_SIZE];
        mailbox::encode_response(&payload[..len], self.ends.recv_free(), &mut frame)
            .expect("the payload buffer is at most one mailbox payload");
        match self
            .driver
            .respond_to_read(&frame)
            .await
            .map_err(TargetError::Hardware)?
        {
            // Only a completed transaction removes the payload.
            ReadStatus::Done => {
                self.ends.consume_send(len);
                Ok(())
            }
            status => Err(TargetError::ReadStatus(status)),
        }
    }
}

impl<T: I2cTarget, const RX: usize, const TX: usize> Drop for TargetRunner<'_, T, RX, TX> {
    fn drop(&mut self) {
        // Runs before the fields drop: the hardware is disarmed first, then
        // the channel closes and the buffers can be released.
        self.driver.disarm();
    }
}
