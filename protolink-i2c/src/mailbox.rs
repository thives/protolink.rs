//! Wire format of the I2C mailbox.
//!
//! Every transaction is one frame of [`MAILBOX_SIZE`] bytes, a fixed header followed by payload:
//!
//! | Offset | Read reply (target to controller) | Write request (controller to target) |
//! |---|---|---|
//! | `0` | Payload length, `0..=MAILBOX_PAYLOAD_MAX`. Zero means idle, not EOF. | Payload length, `0..=MAILBOX_PAYLOAD_MAX`. |
//! | `1` | Flags: [`FLAG_SERVING`], [`FLAG_RETIRE`]. | Operation: [`OP_DATA`], [`OP_OPEN`] or [`OP_RETIRE`]. |
//! | `2` | Target receive credit: free receive-buffer bytes, saturated to 255. | Zero. |
//! | `3` | Zero. | Zero. |
//! | `4..6` | Target boot identifier, little endian. | Target boot identifier, little endian. |
//! | `6..8` | Connection epoch the target serves or retires, little endian; zero if none. | Connection epoch the controller addresses, little endian. |
//! | `8..` | Payload, followed by ignored padding. | Payload. |
//!
//! A write is a whole transaction of `HEADER_LEN + payload` bytes, so the header travels with its
//! payload and the target can reject bytes that belong to a connection it no longer serves.
//! Everything in this module is pure encoding; the connection rules live in `protolink-adaptors`.

use core::cmp::min;

/// Size of one mailbox frame, and the largest transaction.
pub const MAILBOX_SIZE: usize = 32;

/// Length of the frame header.
pub const HEADER_LEN: usize = 8;

/// Largest payload one frame can carry.
pub const MAILBOX_PAYLOAD_MAX: usize = MAILBOX_SIZE - HEADER_LEN;

/// One mailbox frame.
pub type Frame = [u8; MAILBOX_SIZE];

/// Read reply flag: the target serves the connection named in the reply and accepts its writes.
pub const FLAG_SERVING: u8 = 0b01;

/// Read reply flag: the target asks the controller to retire the connection named in the reply.
pub const FLAG_RETIRE: u8 = 0b10;

const FLAGS_KNOWN: u8 = FLAG_SERVING | FLAG_RETIRE;

/// Write operation: payload bytes of the connection.
pub const OP_DATA: u8 = 1;

/// Write operation: open the connection named in the header, which replaces the one the target serves.
pub const OP_OPEN: u8 = 2;

/// Write operation: retire the connection named in the header.
pub const OP_RETIRE: u8 = 3;

/// Identity of one connection: the target's boot identifier and the connection epoch.
///
/// The boot identifier must be random per boot of the target, so that a restarted target never matches a connection
/// from its previous life. The epoch is chosen by the controller and is zero only when no connection exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ConnectionId {
    boot: u16,
    epoch: u16,
}

impl ConnectionId {
    /// Identifies connection `epoch` of the target that booted with `boot`.
    pub const fn new(boot: u16, epoch: u16) -> Self {
        Self { boot, epoch }
    }

    /// The target's boot identifier.
    pub const fn boot(self) -> u16 {
        self.boot
    }

    /// The connection epoch. Zero means no connection.
    pub const fn epoch(self) -> u16 {
        self.epoch
    }

    /// The epoch that follows `epoch`, skipping zero, which means no connection.
    pub const fn next_epoch(epoch: u16) -> u16 {
        match epoch.wrapping_add(1) {
            0 => 1,
            next => next,
        }
    }
}

/// A frame that does not follow the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailboxError {
    /// The payload length byte exceeds [`MAILBOX_PAYLOAD_MAX`].
    InvalidPayloadLength(u8),
    /// The payload passed to an encoder does not fit one frame.
    PayloadTooLarge(usize),
    /// A read reply sets flags that this version does not know.
    InvalidFlags(u8),
    /// A write is shorter than its header.
    Truncated(usize),
    /// A write names an operation that this version does not know.
    UnknownOperation(u8),
    /// An open or retire write carries payload bytes.
    UnexpectedPayload(u8),
    /// A connection-bearing request or reply names epoch zero, which means no connection.
    ZeroEpoch,
    /// A reply sets `SERVING` or `RETIRE` but names no connection.
    FlagsWithoutConnection(u8),
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
            Self::InvalidFlags(flags) => write!(f, "mailbox flags {flags:#04x} are unknown"),
            Self::Truncated(len) => write!(f, "mailbox write of {len} bytes lacks its header"),
            Self::UnknownOperation(op) => write!(f, "mailbox operation {op:#04x} is unknown"),
            Self::UnexpectedPayload(op) => {
                write!(f, "mailbox operation {op:#04x} carries no payload")
            }
            Self::ZeroEpoch => f.write_str("mailbox connection epoch zero names no connection"),
            Self::FlagsWithoutConnection(flags) => {
                write!(f, "mailbox flags {flags:#04x} are set without a connection")
            }
        }
    }
}

impl core::error::Error for MailboxError {}

/// A read reply, as the target builds it or the controller decodes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reply<'a> {
    /// The valid payload. Padding is never included.
    pub payload: &'a [u8],
    /// The target's free receive-buffer bytes at the time of the read, saturated to 255.
    pub rx_credit: u8,
    /// The target's boot identifier and the connection epoch it serves or retires.
    pub connection: ConnectionId,
    /// The target serves `connection` and accepts its writes.
    pub serving: bool,
    /// The target asks for `connection` to be retired.
    pub retire: bool,
}

/// A controller write, as the controller encodes it or the target decodes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request<'a> {
    /// Payload bytes of the connection.
    Data(&'a [u8]),
    /// Make this connection the one the target serves.
    Open,
    /// Retire this connection.
    Retire,
}

/// Credit advertised for `free` bytes of receive buffer.
pub fn rx_credit(free: usize) -> u8 {
    min(free, u8::MAX as usize) as u8
}

/// The number of payload bytes a controller may write in one transaction.
///
/// `credit` is the target's last advertised receive credit, minus what the controller has written since.
/// `queued` is the number of bytes waiting to be sent.
pub fn write_len(credit: u8, queued: usize) -> usize {
    min(min(MAILBOX_PAYLOAD_MAX, credit as usize), queued)
}

fn write_header(
    frame: &mut Frame,
    len: usize,
    flags_or_op: u8,
    rx_credit: u8,
    connection: ConnectionId,
) {
    frame[0] = len as u8;
    frame[1] = flags_or_op;
    frame[2] = rx_credit;
    frame[3] = 0;
    frame[4..6].copy_from_slice(&connection.boot.to_le_bytes());
    frame[6..8].copy_from_slice(&connection.epoch.to_le_bytes());
}

/// Encodes a read reply into `frame`.
///
/// The padding after the payload is zeroed, so the frame is deterministic.
pub fn encode_reply(reply: &Reply<'_>, frame: &mut Frame) -> Result<(), MailboxError> {
    if reply.payload.len() > MAILBOX_PAYLOAD_MAX {
        return Err(MailboxError::PayloadTooLarge(reply.payload.len()));
    }
    let mut flags = 0;
    if reply.serving {
        flags |= FLAG_SERVING;
    }
    if reply.retire {
        flags |= FLAG_RETIRE;
    }
    check_reply_flags(flags, reply.connection)?;
    let len = reply.payload.len();
    write_header(frame, len, flags, reply.rx_credit, reply.connection);
    frame[HEADER_LEN..HEADER_LEN + len].copy_from_slice(reply.payload);
    frame[HEADER_LEN + len..].fill(0);
    Ok(())
}

/// Decodes a read reply.
///
/// Padding is ignored, so this accepts any bytes after the declared payload.
pub fn decode_reply(frame: &Frame) -> Result<Reply<'_>, MailboxError> {
    let len = frame[0] as usize;
    if len > MAILBOX_PAYLOAD_MAX {
        return Err(MailboxError::InvalidPayloadLength(frame[0]));
    }
    let flags = frame[1];
    if flags & !FLAGS_KNOWN != 0 {
        return Err(MailboxError::InvalidFlags(flags));
    }
    let connection = connection_from([frame[4], frame[5]], [frame[6], frame[7]]);
    check_reply_flags(flags, connection)?;
    Ok(Reply {
        payload: &frame[HEADER_LEN..HEADER_LEN + len],
        rx_credit: frame[2],
        connection,
        serving: flags & FLAG_SERVING != 0,
        retire: flags & FLAG_RETIRE != 0,
    })
}

/// A reply that serves or retires a connection must name one.
fn check_reply_flags(flags: u8, connection: ConnectionId) -> Result<(), MailboxError> {
    if flags & (FLAG_SERVING | FLAG_RETIRE) != 0 && connection.epoch == 0 {
        return Err(MailboxError::FlagsWithoutConnection(flags));
    }
    Ok(())
}

/// Encodes a controller write into `frame` and returns the transaction length.
///
/// Every request names a connection, so epoch zero is rejected.
pub fn encode_request(
    connection: ConnectionId,
    request: Request<'_>,
    frame: &mut Frame,
) -> Result<usize, MailboxError> {
    if connection.epoch == 0 {
        return Err(MailboxError::ZeroEpoch);
    }
    let (op, payload): (u8, &[u8]) = match request {
        Request::Data(payload) => (OP_DATA, payload),
        Request::Open => (OP_OPEN, &[]),
        Request::Retire => (OP_RETIRE, &[]),
    };
    if payload.len() > MAILBOX_PAYLOAD_MAX {
        return Err(MailboxError::PayloadTooLarge(payload.len()));
    }
    let len = payload.len();
    write_header(frame, len, op, 0, connection);
    frame[HEADER_LEN..HEADER_LEN + len].copy_from_slice(payload);
    Ok(HEADER_LEN + len)
}

/// Decodes a controller write. `bytes` is the whole transaction, as the target received it.
pub fn decode_request(bytes: &[u8]) -> Result<(ConnectionId, Request<'_>), MailboxError> {
    if bytes.len() < HEADER_LEN {
        return Err(MailboxError::Truncated(bytes.len()));
    }
    let len = bytes[0] as usize;
    if len > MAILBOX_PAYLOAD_MAX || bytes.len() != HEADER_LEN + len {
        return Err(MailboxError::InvalidPayloadLength(bytes[0]));
    }
    let connection = connection_from([bytes[4], bytes[5]], [bytes[6], bytes[7]]);
    if connection.epoch == 0 {
        return Err(MailboxError::ZeroEpoch);
    }
    let payload = &bytes[HEADER_LEN..];
    let request = match bytes[1] {
        OP_DATA => Request::Data(payload),
        OP_OPEN | OP_RETIRE if !payload.is_empty() => {
            return Err(MailboxError::UnexpectedPayload(bytes[1]));
        }
        OP_OPEN => Request::Open,
        OP_RETIRE => Request::Retire,
        op => return Err(MailboxError::UnknownOperation(op)),
    };
    Ok((connection, request))
}

fn connection_from(boot: [u8; 2], epoch: [u8; 2]) -> ConnectionId {
    ConnectionId::new(u16::from_le_bytes(boot), u16::from_le_bytes(epoch))
}

/// The connection named in a frame's header, whatever the frame carries.
///
/// `None` when `frame` is too short to hold a header. The result is not
/// validated: epoch zero means no connection.
pub fn connection_of(frame: &[u8]) -> Option<ConnectionId> {
    let fields = frame.get(4..HEADER_LEN)?;
    Some(connection_from(
        [fields[0], fields[1]],
        [fields[2], fields[3]],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply_frame(len: u8, flags: u8, credit: u8, boot: u16, epoch: u16, payload: &[u8]) -> Frame {
        let mut frame = [0xAA; MAILBOX_SIZE];
        frame[0] = len;
        frame[1] = flags;
        frame[2] = credit;
        frame[3] = 0;
        frame[4..6].copy_from_slice(&boot.to_le_bytes());
        frame[6..8].copy_from_slice(&epoch.to_le_bytes());
        frame[HEADER_LEN..HEADER_LEN + payload.len()].copy_from_slice(payload);
        frame
    }

    #[test]
    fn idle_reply_is_idle_not_eof_and_carries_its_connection() {
        let frame = reply_frame(0, FLAG_SERVING, 40, 7, 3, &[]);
        let reply = decode_reply(&frame).unwrap();
        assert!(reply.payload.is_empty());
        assert_eq!(reply.rx_credit, 40);
        assert_eq!(reply.connection, ConnectionId::new(7, 3));
        assert!(reply.serving);
        assert!(!reply.retire);
    }

    #[test]
    fn partial_reply_yields_only_the_declared_payload() {
        let frame = reply_frame(3, 0, 9, 1, 1, &[1, 2, 3]);
        assert_eq!(decode_reply(&frame).unwrap().payload, &[1, 2, 3]);
    }

    #[test]
    fn full_reply_carries_the_maximum_payload() {
        let payload: [u8; MAILBOX_PAYLOAD_MAX] = core::array::from_fn(|i| i as u8);
        let frame = reply_frame(MAILBOX_PAYLOAD_MAX as u8, FLAG_SERVING, 255, 0, 1, &payload);
        assert_eq!(decode_reply(&frame).unwrap().payload, &payload[..]);
    }

    #[test]
    fn payload_length_beyond_the_maximum_is_rejected() {
        let len = (MAILBOX_PAYLOAD_MAX + 1) as u8;
        assert_eq!(
            decode_reply(&reply_frame(len, 0, 0, 0, 0, &[])),
            Err(MailboxError::InvalidPayloadLength(len))
        );
        assert_eq!(
            decode_reply(&reply_frame(255, 0, 0, 0, 0, &[])),
            Err(MailboxError::InvalidPayloadLength(255))
        );
    }

    #[test]
    fn unknown_reply_flags_are_rejected() {
        assert_eq!(
            decode_reply(&reply_frame(0, 0b100, 0, 0, 0, &[])),
            Err(MailboxError::InvalidFlags(0b100))
        );
    }

    #[test]
    fn padding_is_ignored_even_when_it_looks_like_data() {
        let frame = reply_frame(2, 0, 1, 0, 0, &[7, 8]);
        assert_eq!(decode_reply(&frame).unwrap().payload, &[7, 8]);
    }

    #[test]
    fn encode_zeroes_padding_and_round_trips() {
        let mut frame = [0xFF; MAILBOX_SIZE];
        let reply = Reply {
            payload: &[4, 5, 6],
            rx_credit: rx_credit(300),
            connection: ConnectionId::new(9, 2),
            serving: true,
            retire: true,
        };
        encode_reply(&reply, &mut frame).unwrap();
        assert_eq!(frame[0], 3);
        assert_eq!(frame[2], 255, "credit saturates");
        assert_eq!(&frame[HEADER_LEN..HEADER_LEN + 3], &[4, 5, 6]);
        assert!(frame[HEADER_LEN + 3..].iter().all(|&b| b == 0));
        assert_eq!(decode_reply(&frame).unwrap(), reply);
    }

    #[test]
    fn encode_rejects_oversized_reply_payload() {
        let mut frame = [0u8; MAILBOX_SIZE];
        let payload = [0u8; MAILBOX_PAYLOAD_MAX + 1];
        let reply = Reply {
            payload: &payload,
            rx_credit: 0,
            connection: ConnectionId::default(),
            serving: false,
            retire: false,
        };
        assert_eq!(
            encode_reply(&reply, &mut frame),
            Err(MailboxError::PayloadTooLarge(MAILBOX_PAYLOAD_MAX + 1))
        );
    }

    #[test]
    fn requests_round_trip_with_their_connection() {
        let mut frame = [0u8; MAILBOX_SIZE];
        let connection = ConnectionId::new(0xBEEF, 0x0102);
        let len = encode_request(connection, Request::Data(&[1, 2, 3]), &mut frame).unwrap();
        assert_eq!(len, HEADER_LEN + 3);
        assert_eq!(
            decode_request(&frame[..len]),
            Ok((connection, Request::Data(&[1, 2, 3])))
        );

        let len = encode_request(connection, Request::Open, &mut frame).unwrap();
        assert_eq!(len, HEADER_LEN);
        assert_eq!(
            decode_request(&frame[..len]),
            Ok((connection, Request::Open))
        );

        let len = encode_request(connection, Request::Retire, &mut frame).unwrap();
        assert_eq!(
            decode_request(&frame[..len]),
            Ok((connection, Request::Retire))
        );
    }

    #[test]
    fn malformed_requests_are_rejected() {
        assert_eq!(decode_request(&[0; 3]), Err(MailboxError::Truncated(3)));
        let mut frame = [0u8; MAILBOX_SIZE];
        frame[0] = 2;
        frame[1] = OP_DATA;
        frame[6] = 1; // a real connection, so only the length is wrong
        assert_eq!(
            decode_request(&frame[..HEADER_LEN]),
            Err(MailboxError::InvalidPayloadLength(2))
        );
        frame[0] = 0;
        frame[1] = 9;
        assert_eq!(
            decode_request(&frame[..HEADER_LEN]),
            Err(MailboxError::UnknownOperation(9))
        );
        frame[0] = 1;
        frame[1] = OP_OPEN;
        assert_eq!(
            decode_request(&frame[..HEADER_LEN + 1]),
            Err(MailboxError::UnexpectedPayload(OP_OPEN))
        );
    }

    #[test]
    fn write_length_is_limited_by_mailbox_credit_and_queue() {
        assert_eq!(write_len(0, 100), 0, "no credit means no write");
        assert_eq!(write_len(5, 100), 5, "credit limits");
        assert_eq!(write_len(255, 3), 3, "queue limits");
        assert_eq!(write_len(255, 1000), MAILBOX_PAYLOAD_MAX, "mailbox limits");
    }

    #[test]
    fn frames_have_the_exact_bytes_on_the_wire() {
        // A write: header, then payload, then nothing.
        let mut frame = [0xEE; MAILBOX_SIZE];
        let len = encode_request(
            ConnectionId::new(0xBEEF, 0x0102),
            Request::Data(&[1, 2, 3]),
            &mut frame,
        )
        .unwrap();
        assert_eq!(
            &frame[..len],
            &[3, OP_DATA, 0, 0, 0xEF, 0xBE, 0x02, 0x01, 1, 2, 3]
        );

        let len = encode_request(ConnectionId::new(7, 9), Request::Open, &mut frame).unwrap();
        assert_eq!(&frame[..len], &[0, OP_OPEN, 0, 0, 7, 0, 9, 0]);

        // A reply: serving, credit 40, payload, zeroed padding.
        let reply = Reply {
            payload: &[0xA1],
            rx_credit: 40,
            connection: ConnectionId::new(0x0300, 0x0004),
            serving: true,
            retire: false,
        };
        let mut frame = [0xEE; MAILBOX_SIZE];
        encode_reply(&reply, &mut frame).unwrap();
        assert_eq!(
            &frame[..9],
            &[1, FLAG_SERVING, 40, 0, 0x00, 0x03, 0x04, 0x00, 0xA1]
        );
        assert!(frame[9..].iter().all(|&b| b == 0));
    }

    #[test]
    fn requests_name_a_connection() {
        let mut frame = [0u8; MAILBOX_SIZE];
        assert_eq!(
            encode_request(ConnectionId::new(5, 0), Request::Open, &mut frame),
            Err(MailboxError::ZeroEpoch)
        );
        frame[0] = 0;
        frame[1] = OP_OPEN;
        frame[6] = 0;
        frame[7] = 0;
        assert_eq!(
            decode_request(&frame[..HEADER_LEN]),
            Err(MailboxError::ZeroEpoch)
        );
    }

    #[test]
    fn replies_that_serve_or_retire_must_name_a_connection() {
        for flags in [FLAG_SERVING, FLAG_RETIRE, FLAG_SERVING | FLAG_RETIRE] {
            assert_eq!(
                decode_reply(&reply_frame(0, flags, 0, 4, 0, &[])),
                Err(MailboxError::FlagsWithoutConnection(flags))
            );
        }
        let reply = Reply {
            payload: &[],
            rx_credit: 0,
            connection: ConnectionId::new(4, 0),
            serving: true,
            retire: false,
        };
        let mut frame = [0u8; MAILBOX_SIZE];
        assert_eq!(
            encode_reply(&reply, &mut frame),
            Err(MailboxError::FlagsWithoutConnection(FLAG_SERVING))
        );
        // An unbound target still names itself, with no flags.
        assert_eq!(
            decode_reply(&reply_frame(0, 0, 0, 4, 0, &[]))
                .unwrap()
                .connection,
            ConnectionId::new(4, 0)
        );
    }

    #[test]
    fn connection_of_is_length_safe() {
        let frame = reply_frame(0, 0, 0, 0x0102, 0x0304, &[]);
        assert_eq!(
            connection_of(&frame),
            Some(ConnectionId::new(0x0102, 0x0304))
        );
        assert_eq!(
            connection_of(&frame[..HEADER_LEN]),
            Some(ConnectionId::new(0x0102, 0x0304))
        );
        assert_eq!(connection_of(&frame[..HEADER_LEN - 1]), None);
        assert_eq!(connection_of(&[]), None);
    }

    #[test]
    fn epochs_advance_and_skip_no_connection() {
        assert_eq!(ConnectionId::next_epoch(0), 1);
        assert_eq!(ConnectionId::next_epoch(41), 42);
        assert_eq!(ConnectionId::next_epoch(u16::MAX), 1);
    }

    #[test]
    fn credit_is_saturated_to_one_byte() {
        assert_eq!(rx_credit(0), 0);
        assert_eq!(rx_credit(7), 7);
        assert_eq!(rx_credit(255), 255);
        assert_eq!(rx_credit(1024), 255);
    }
}
