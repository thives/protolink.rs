//! The contract between a target-mode I2C peripheral and the mailbox runner.
//!
//! `embedded-hal` defines no target (slave) role, so a board implements
//! [`I2cTarget`] for its peripheral. The shape mirrors the target drivers of
//! common HALs: wait for the controller's next transaction, then either take
//! its write or answer its read.

use core::fmt::Debug;
use core::future::Future;

/// What the controller asked for, as reported by [`I2cTarget::listen`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// The controller wants to read; answer with
    /// [`respond_to_read`](I2cTarget::respond_to_read).
    Read,
    /// The controller wrote this many bytes, which are now at the start of the
    /// buffer passed to [`listen`](I2cTarget::listen). The bytes are one whole mailbox transaction.
    Write(usize),
}

/// How a response to a controller read ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStatus {
    /// The controller read exactly the response and ended the transaction.
    Done,
    /// The controller read more bytes than the response held.
    NeedMoreBytes,
    /// The controller ended the transaction with this many response bytes
    /// unread.
    LeftoverBytes(u16),
}

/// An I2C peripheral in target mode.
///
/// # Contract
///
/// - The target address is part of the peripheral's own setup. The adaptor
///   does not configure it.
/// - [`listen`](Self::listen) and [`respond_to_read`](Self::respond_to_read)
///   are driven to completion by the runner. If either future is dropped, or
///   fails, the hardware transaction is in an unspecified state: the runner
///   does not try to resume it. It calls [`disarm`](Self::disarm) instead, and
///   the next call to `listen` must arm the peripheral afresh.
/// - `respond_to_read` may only report [`ReadStatus::Done`] when the
///   controller really received the whole response. The runner removes the
///   queued payload only on `Done`. On any other status the connection ends,
///   and the uncertain payload is discarded: it is never replayed.
/// - [`disarm`](Self::disarm) stops servicing the bus and releases SCL (no
///   clock stretching). It must not wait for the bus to go idle, must be safe
///   to call at any point and more than once, and is called before the
///   runner's buffers are released, on failure and on teardown.
pub trait I2cTarget {
    /// The hardware error. It is preserved in the runner's result.
    type Error: Debug;

    /// Waits for the next controller transaction.
    ///
    /// For a write, the received bytes are stored at the start of `buffer`;
    /// the buffer is [`MAILBOX_SIZE`](crate::MAILBOX_SIZE) bytes.
    fn listen(&mut self, buffer: &mut [u8]) -> impl Future<Output = Result<Command, Self::Error>>;

    /// Answers a controller read with `response` and waits for the
    /// transaction to end. A status other than [`ReadStatus::Done`] means the
    /// controller did not take the whole response, so the runner treats the
    /// response's payload as uncertain.
    fn respond_to_read(
        &mut self,
        response: &[u8],
    ) -> impl Future<Output = Result<ReadStatus, Self::Error>>;

    /// Stops servicing the bus. Must not wait for the bus to go idle.
    fn disarm(&mut self);
}

impl<T: I2cTarget + ?Sized> I2cTarget for &mut T {
    type Error = T::Error;

    fn listen(&mut self, buffer: &mut [u8]) -> impl Future<Output = Result<Command, Self::Error>> {
        (**self).listen(buffer)
    }

    fn respond_to_read(
        &mut self,
        response: &[u8],
    ) -> impl Future<Output = Result<ReadStatus, Self::Error>> {
        (**self).respond_to_read(response)
    }

    fn disarm(&mut self) {
        (**self).disarm();
    }
}
