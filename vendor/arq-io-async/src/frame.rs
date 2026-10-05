use crate::{
    Crc16, MAX_FRAME,
    ack_codec::AckCodec,
    error::{AckError, FrameError},
};

/// The maximum frame sequence number.
///
/// Sequence numbers are 14 bits and wrap around at this value.
pub const MAX_SEQ: u16 = 1 << 14;

// Wire format (all multi-byte fields are big-endian):
//
// ACK:
//   [0..M]  BCH codeword (M bytes)
//
// DAT / DAT+ACK-request / FIN:
//   [0-1]  an     u16  frame sequence number - 2 bits for type on the low end of the LE u16
//   [2]    len    u8   payload length
//   [3-4]  crc16  u16  over bytes [0..4] + payload
//   [5..]  payload

pub(crate) const TYPE_DAT_ACK_REQ: u8 = 0b00;
pub(crate) const TYPE_ACK: u8 = 0b01;
pub(crate) const TYPE_DAT: u8 = 0b10;
pub(crate) const TYPE_FIN: u8 = 0b11;

pub(crate) const MAX_PAYLOAD: usize = MAX_FRAME - 5;

/// A decoded and validated ACK frame.
#[derive(Debug, Clone, Copy)]
pub struct AckFrame {
    pkt_id: u16,
    crc: u16,
}

impl AckFrame {
    /// Builds an ACK frame acknowledging all frames below `an`.
    ///
    /// Returns [`FrameError::InvalidSeq`] if `an >= MAX_SEQ`.
    pub fn new<C: Crc16>(crc: &C, an: u16) -> Result<Self, FrameError> {
        if an >= MAX_SEQ {
            return Err(FrameError::InvalidSeq(an, MAX_SEQ));
        }
        let pkt_id = TYPE_ACK as u16 | (an << 2);
        let v = crc.checksum_concat([pkt_id.to_le_bytes().as_slice()]);
        Ok(Self { pkt_id, crc: v })
    }

    pub(crate) fn from_parts<C: Crc16>(
        crc: &C,
        pkt_id: u16,
        crc_recv: u16,
    ) -> Result<Self, FrameError> {
        let v = crc.checksum_concat([pkt_id.to_le_bytes().as_slice()]);
        if crc_recv != v {
            return Err(FrameError::CrcMismatch(crc_recv, v));
        }
        if pkt_id & 0b11 != TYPE_ACK as u16 {
            return Err(FrameError::TypeMismatch);
        }
        Ok(Self { pkt_id, crc: v })
    }

    /// The raw 16-bit packet identifier: 2 type bits plus the 14-bit sequence
    /// number.
    pub fn pkt_id(&self) -> u16 {
        self.pkt_id
    }

    /// The sequence number carried by the frame, i.e. the next expected
    /// sequence number.
    pub fn an(&self) -> u16 {
        self.pkt_id >> 2
    }

    /// The CRC-16 over the frame's packet identifier.
    pub fn crc(&self) -> u16 {
        self.crc
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DatFrame {
    pkt_id: u16,
    len: u8,
    crc: u16,
    payload: [u8; MAX_PAYLOAD],
}

impl DatFrame {
    pub(crate) fn new_dat<C: Crc16>(crc: &C, an: u16, payload: &[u8]) -> Self {
        Self::new(crc, an, TYPE_DAT, payload)
    }

    pub(crate) fn new_dat_ack_req<C: Crc16>(crc: &C, an: u16, payload: &[u8]) -> Self {
        Self::new(crc, an, TYPE_DAT_ACK_REQ, payload)
    }

    pub(crate) fn new_fin<C: Crc16>(crc: &C, an: u16, payload: &[u8]) -> Self {
        Self::new(crc, an, TYPE_FIN, payload)
    }

    pub(crate) fn to_ack_req<C: Crc16>(self, crc: &C) -> Self {
        debug_assert!(!self.is_fin());
        Self::new_dat_ack_req(crc, self.sn(), self.payload())
    }

    fn new<C: Crc16>(crc: &C, an: u16, ty: u8, payload: &[u8]) -> Self {
        assert!(an < MAX_SEQ);
        assert!(payload.len() <= MAX_PAYLOAD);
        let len = payload.len() as u8;
        let pkt_id = ty as u16 | (an << 2);
        let pkt_id_bytes = pkt_id.to_le_bytes();
        let v = crc.checksum_concat([pkt_id_bytes.as_slice(), [len].as_slice(), payload]);
        let mut this = Self {
            pkt_id,
            len,
            crc: v,
            payload: [0; MAX_PAYLOAD],
        };
        this.payload[..len as usize].copy_from_slice(payload);
        this
    }

    pub(crate) fn from_bytes<C: Crc16>(crc: &C, bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < 5 {
            return Err(FrameError::TooShort(bytes.len()));
        }
        let pkt_id = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
        let ty = (pkt_id & 0b11) as u8;
        if ty == TYPE_ACK {
            return Err(FrameError::TypeMismatch);
        }
        let len = bytes[2] as usize;
        if bytes.len() != 5 + len {
            return Err(FrameError::LengthMismatch(len, bytes.len()));
        }
        if len > MAX_PAYLOAD {
            return Err(FrameError::TooLong(len));
        }
        let crc_recv = u16::from_le_bytes(bytes[3..5].try_into().unwrap());
        let v = crc.checksum_concat([&bytes[0..3], &bytes[5..]]);
        if crc_recv != v {
            return Err(FrameError::CrcMismatch(crc_recv, v));
        }
        let mut this = Self {
            pkt_id,
            len: bytes[2],
            crc: v,
            payload: [0; MAX_PAYLOAD],
        };
        this.payload[..len].copy_from_slice(&bytes[5..]);
        Ok(this)
    }

    pub(crate) fn to_bytes(self, buf: &mut [u8]) -> usize {
        buf[0..2].copy_from_slice(&self.pkt_id.to_le_bytes());
        buf[2] = self.len;
        buf[3..5].copy_from_slice(&self.crc.to_le_bytes());
        buf[5..5 + self.len as usize].copy_from_slice(&self.payload[..self.len as usize]);
        5 + self.len as usize
    }

    pub(crate) fn sn(&self) -> u16 {
        self.pkt_id >> 2
    }

    pub(crate) fn len(&self) -> usize {
        self.len as usize
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload[..self.len()]
    }

    pub(crate) fn is_fin(&self) -> bool {
        self.pkt_id & 0b11 == TYPE_FIN as u16
    }

    pub(crate) fn requests_ack(&self) -> bool {
        self.pkt_id & 0b11 == TYPE_DAT_ACK_REQ as u16
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Frame {
    Ack(AckFrame),
    Dat(DatFrame),
    DatAckReq(DatFrame),
    Fin(DatFrame),
}

impl Frame {
    pub(crate) fn from_dat(d: DatFrame) -> Self {
        if d.is_fin() {
            Frame::Fin(d)
        } else if d.requests_ack() {
            Frame::DatAckReq(d)
        } else {
            Frame::Dat(d)
        }
    }
    pub(crate) fn wire_len<B: AckCodec<M>, const M: usize, C: Crc16>(
        crc: &C,
        bytes: &[u8],
    ) -> Result<usize, FrameError> {
        let cw: Option<[u8; M]> = bytes.get(0..M).and_then(|s| s.try_into().ok());
        if let Some(cw) = &cw {
            if B::decode_ack(crc, cw).is_ok() {
                return Ok(M);
            }
        }
        match bytes.first().copied().map(|b| b & 0b11) {
            None => Err(FrameError::TooShort(0)),
            Some(TYPE_DAT) | Some(TYPE_DAT_ACK_REQ) | Some(TYPE_FIN) => {
                if bytes.len() < 3 {
                    return Err(FrameError::TooShort(bytes.len()));
                }
                let len = bytes[2] as usize;
                if len > MAX_PAYLOAD {
                    return Err(FrameError::TooLong(len));
                }
                Ok(5 + len)
            }
            Some(t) => Err(FrameError::InvalidType(t)),
        }
    }

    pub(crate) fn from_bytes<B: AckCodec<M>, const M: usize, C: Crc16>(
        crc: &C,
        bytes: &[u8],
    ) -> Result<Self, FrameError> {
        let cw: Option<[u8; M]> = bytes.get(0..M).and_then(|s| s.try_into().ok());
        if let Some(cw) = &cw {
            if let Ok(ack) = B::decode_ack(crc, cw) {
                return Ok(Frame::Ack(ack));
            }
        }
        match bytes.first().copied().ok_or(FrameError::TooShort(0))? & 0b11 {
            TYPE_DAT | TYPE_DAT_ACK_REQ | TYPE_FIN => {
                Ok(Frame::from_dat(DatFrame::from_bytes(crc, bytes)?))
            }
            t => Err(FrameError::InvalidType(t)),
        }
    }

    /// Decode an authoritative frame boundary, never accepting an ACK prefix
    /// or trusting a DAT length to select bytes from another frame.
    pub(crate) fn from_complete_bytes<B: AckCodec<M>, const M: usize, C: Crc16>(
        crc: &C,
        bytes: &[u8],
    ) -> Result<Self, FrameError> {
        if let Ok(cw) = <&[u8; M]>::try_from(bytes) {
            if let Ok(ack) = B::decode_ack(crc, cw) {
                return Ok(Frame::Ack(ack));
            }
        }
        Ok(Frame::from_dat(DatFrame::from_bytes(crc, bytes)?))
    }

    pub(crate) fn to_bytes<B: AckCodec<M>, const M: usize>(
        self,
        buf: &mut [u8],
    ) -> Result<usize, AckError> {
        match self {
            Frame::Ack(ack) => {
                let cw = B::encode_ack(ack)?;
                buf[0..M].copy_from_slice(&cw);
                Ok(M)
            }
            Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => Ok(d.to_bytes(buf)),
        }
    }
}
