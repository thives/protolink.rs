//! Controller-side transaction primitives.
//!
//! These perform exactly one I2C transaction and own no queues, so a caller decides when to consume its outgoing bytes:
//! only after the transaction returned `Ok`, and in any case without resending them blindly.

use embedded_hal_async::i2c::I2c;

use crate::Address;
use crate::mailbox::{Frame, MAILBOX_SIZE};

/// Reads one mailbox frame from the target at `address`.
///
/// The frame is not validated; pass it to [`decode_reply`](crate::mailbox::decode_reply).
pub async fn read_mailbox<I: I2c>(i2c: &mut I, address: Address) -> Result<Frame, I::Error> {
    let mut frame = [0u8; MAILBOX_SIZE];
    i2c.read(address.get(), &mut frame).await?;
    Ok(frame)
}

/// Writes one encoded request transaction to the target at `address`.
///
/// # Panics
///
/// If `transaction` is longer than [`MAILBOX_SIZE`]; the target cannot accept more in one transaction.
pub async fn write_transaction<I: I2c>(
    i2c: &mut I,
    address: Address,
    transaction: &[u8],
) -> Result<(), I::Error> {
    assert!(
        transaction.len() <= MAILBOX_SIZE,
        "a mailbox transaction carries at most MAILBOX_SIZE bytes"
    );
    i2c.write(address.get(), transaction).await
}
