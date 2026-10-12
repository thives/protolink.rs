//! Wire format of the I2C mailbox.
//!
//! A controller read always requests [`MAILBOX_SIZE`] bytes, and the target
//! answers with one fixed-size frame:
//!
//! | Offset | Field |
//! |---|---|
//! | `0` | Payload length, `0..=MAILBOX_PAYLOAD_MAX`. Zero means idle, not EOF. |
//! | `1` | Target receive credit: free receive-buffer bytes, saturated to 255. |
//! | `2..` | Payload, followed by ignored padding. |
//!
//! A controller write carries raw payload bytes only, at most
//! [`MAILBOX_SIZE`] of them and never more than the advertised credit. Its
//! length is the length of the write itself, so it needs no header.

use core::cmp::min;

/// Size of one mailbox frame, and the largest controller write.
pub const MAILBOX_SIZE: usize = 32;

/// Length of the frame header: payload length and receive credit.
pub const HEADER_LEN: usize = 2;

/// Largest payload one read response can carry.
pub const MAILBOX_PAYLOAD_MAX: usize = MAILBOX_SIZE - HEADER_LEN;

/// One mailbox frame.
pub type Frame = [u8; MAILBOX_SIZE];

/// A frame that does not follow the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailboxError {
    /// The payload length byte exceeds [`MAILBOX_PAYLOAD_MAX`].
    InvalidPayloadLength(u8),
    /// The payload passed to [`encode_response`] does not fit one frame.
    PayloadTooLarge(usize),
}

impl core::fmt::Display for MailboxError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidPayloadLength(len) => {
                write!(
                    f,
                    "mailbox payload length {len} exceeds {MAILBOX_PAYLOAD_MAX}"
                )
            }
            Self::PayloadTooLarge(len) => {
                write!(f, "payload of {len} bytes does not fit a mailbox frame")
            }
        }
    }
}

impl core::error::Error for MailboxError {}

/// A decoded read response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Response<'a> {
    /// The valid payload. Padding is never included.
    pub payload: &'a [u8],
    /// The target's free receive-buffer bytes at the time of the read,
    /// saturated to 255.
    pub rx_credit: u8,
}

/// Credit advertised for `free` bytes of receive buffer.
pub fn rx_credit(free: usize) -> u8 {
    min(free, u8::MAX as usize) as u8
}

/// The number of bytes a controller may write in one transaction.
///
/// `credit` is the target's last advertised receive credit, minus what the
/// controller has written since. `queued` is the number of bytes waiting to be
/// sent.
pub fn write_len(credit: u8, queued: usize) -> usize {
    min(min(MAILBOX_SIZE, credit as usize), queued)
}

/// Encodes a target read response into `frame`.
///
/// The padding after the payload is zeroed, so the frame is deterministic.
pub fn encode_response(
    payload: &[u8],
    free_rx: usize,
    frame: &mut Frame,
) -> Result<(), MailboxError> {
    if payload.len() > MAILBOX_PAYLOAD_MAX {
        return Err(MailboxError::PayloadTooLarge(payload.len()));
    }
    frame[0] = payload.len() as u8;
    frame[1] = rx_credit(free_rx);
    frame[HEADER_LEN..HEADER_LEN + payload.len()].copy_from_slice(payload);
    frame[HEADER_LEN + payload.len()..].fill(0);
    Ok(())
}

/// Decodes a controller read response.
///
/// Padding is ignored, so this accepts any bytes after the declared payload.
pub fn decode_response(frame: &Frame) -> Result<Response<'_>, MailboxError> {
    let len = frame[0] as usize;
    if len > MAILBOX_PAYLOAD_MAX {
        return Err(MailboxError::InvalidPayloadLength(frame[0]));
    }
    Ok(Response {
        payload: &frame[HEADER_LEN..HEADER_LEN + len],
        rx_credit: frame[1],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame_with(len: u8, credit: u8, payload: &[u8]) -> Frame {
        let mut frame = [0xAA; MAILBOX_SIZE];
        frame[0] = len;
        frame[1] = credit;
        frame[HEADER_LEN..HEADER_LEN + payload.len()].copy_from_slice(payload);
        frame
    }

    #[test]
    fn empty_response_is_idle_not_eof() {
        let frame = frame_with(0, 40, &[]);
        let response = decode_response(&frame).unwrap();
        assert!(response.payload.is_empty());
        assert_eq!(response.rx_credit, 40);
    }

    #[test]
    fn partial_response_yields_only_the_declared_payload() {
        let frame = frame_with(3, 9, &[1, 2, 3]);
        let response = decode_response(&frame).unwrap();
        assert_eq!(response.payload, &[1, 2, 3]);
        assert_eq!(response.rx_credit, 9);
    }

    #[test]
    fn full_response_carries_the_maximum_payload() {
        let payload: [u8; MAILBOX_PAYLOAD_MAX] = core::array::from_fn(|i| i as u8);
        let frame = frame_with(MAILBOX_PAYLOAD_MAX as u8, 255, &payload);
        let response = decode_response(&frame).unwrap();
        assert_eq!(response.payload, &payload[..]);
        assert_eq!(response.rx_credit, 255);
    }

    #[test]
    fn payload_length_beyond_the_maximum_is_rejected() {
        let len = (MAILBOX_PAYLOAD_MAX + 1) as u8;
        assert_eq!(
            decode_response(&frame_with(len, 0, &[])),
            Err(MailboxError::InvalidPayloadLength(len))
        );
        assert_eq!(
            decode_response(&frame_with(255, 0, &[])),
            Err(MailboxError::InvalidPayloadLength(255))
        );
    }

    #[test]
    fn padding_is_ignored_even_when_it_looks_like_data() {
        let frame = frame_with(2, 1, &[7, 8]);
        assert_eq!(decode_response(&frame).unwrap().payload, &[7, 8]);
    }

    #[test]
    fn encode_zeroes_padding_and_round_trips() {
        let mut frame = [0xFF; MAILBOX_SIZE];
        encode_response(&[4, 5, 6], 300, &mut frame).unwrap();
        assert_eq!(frame[0], 3);
        assert_eq!(frame[1], 255, "credit saturates");
        assert_eq!(&frame[HEADER_LEN..HEADER_LEN + 3], &[4, 5, 6]);
        assert!(frame[HEADER_LEN + 3..].iter().all(|&b| b == 0));
        assert_eq!(decode_response(&frame).unwrap().payload, &[4, 5, 6]);
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        let mut frame = [0u8; MAILBOX_SIZE];
        let payload = [0u8; MAILBOX_PAYLOAD_MAX + 1];
        assert_eq!(
            encode_response(&payload, 0, &mut frame),
            Err(MailboxError::PayloadTooLarge(MAILBOX_PAYLOAD_MAX + 1))
        );
    }

    #[test]
    fn credit_is_saturated_to_one_byte() {
        assert_eq!(rx_credit(0), 0);
        assert_eq!(rx_credit(7), 7);
        assert_eq!(rx_credit(255), 255);
        assert_eq!(rx_credit(1024), 255);
    }

    #[test]
    fn write_length_is_limited_by_mailbox_credit_and_queue() {
        assert_eq!(write_len(0, 100), 0, "no credit means no write");
        assert_eq!(write_len(5, 100), 5, "credit limits");
        assert_eq!(write_len(255, 3), 3, "queue limits");
        assert_eq!(write_len(255, 1000), MAILBOX_SIZE, "mailbox limits");
    }
}
