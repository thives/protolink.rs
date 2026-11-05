//! Byte encoding for `lua.execution.v1` messages.

use alloc::vec::Vec;
use core::convert::Infallible;

use micropb::{MessageDecode, MessageEncode, PbEncoder};

/// Failure to decode a message from bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeFailed;

impl core::fmt::Display for DecodeFailed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("malformed lua.execution.v1 message")
    }
}

/// Encodes `message` to its protobuf bytes.
pub fn encode<M: MessageEncode>(message: &M) -> Vec<u8> {
    let mut encoder = PbEncoder::new(Vec::with_capacity(message.compute_size()));
    // Writing to a Vec cannot fail.
    message
        .encode(&mut encoder)
        .unwrap_or_else(|never: Infallible| match never {});
    encoder.into_writer()
}

/// Decodes `message` from `bytes`, merging into a default value.
pub fn decode<M: MessageDecode + Default>(bytes: &[u8]) -> Result<M, DecodeFailed> {
    let mut message = M::default();
    message.decode_from_bytes(bytes).map_err(|_| DecodeFailed)?;
    Ok(message)
}
