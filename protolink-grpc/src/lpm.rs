//! gRPC length-prefixed messages (LPM): 1-byte compressed flag, 4-byte
//! big-endian length, payload.

use alloc::vec::Vec;

use crate::Status;

/// Size of the LPM prefix.
pub const HEADER_LEN: usize = 5;

/// Wrap `payload` as one uncompressed length-prefixed message.
pub fn encode(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(0);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Extract the single message of a complete unary request/response body.
///
/// Errors follow the reduced compatibility profile:
/// - no message → `INTERNAL`
/// - compressed message → `UNIMPLEMENTED`
/// - message larger than `max_size` → `RESOURCE_EXHAUSTED`
/// - truncated message → `INTERNAL`
/// - more than one message → `UNIMPLEMENTED` (streaming)
pub fn decode_unary(body: &[u8], max_size: usize) -> Result<&[u8], Status> {
    if body.is_empty() {
        return Err(Status::internal("missing message"));
    }
    if body.len() < HEADER_LEN {
        return Err(Status::internal("truncated message prefix"));
    }
    match body[0] {
        0 => {}
        1 => {
            return Err(Status::unimplemented(
                "message compression is not supported",
            ));
        }
        _ => return Err(Status::internal("invalid message flags")),
    }
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    if len > max_size {
        return Err(Status::resource_exhausted("message exceeds maximum size"));
    }
    let end = HEADER_LEN + len;
    if body.len() < end {
        return Err(Status::internal("truncated message"));
    }
    if body.len() > end {
        return Err(Status::unimplemented(
            "streaming is not supported (more than one message)",
        ));
    }
    Ok(&body[HEADER_LEN..end])
}
