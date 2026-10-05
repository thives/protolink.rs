use crate::bch::{decode, encode};
use crate::crc::Crc16;
use crate::error::AckError;
use crate::frame::AckFrame;

/// Encodes and decodes the error-correcting codeword that carries ACK frames.
///
/// The codeword is `ACK_CODEWORD_LEN` bytes long.
pub trait AckCodec<const ACK_CODEWORD_LEN: usize> {
    /// Encodes `frame` into the codeword.
    fn encode_ack(frame: AckFrame) -> Result<[u8; ACK_CODEWORD_LEN], AckError>;
    /// Decodes the codeword into an [`AckFrame`], validating the frame with
    /// `crc`.
    fn decode_ack<C: Crc16>(
        crc: &C,
        codeword: &[u8; ACK_CODEWORD_LEN],
    ) -> Result<AckFrame, AckError>;
}

/// A BCH error-correcting [`AckCodec`] for 16-byte codewords.
///
/// The codeword carries two 8-byte BCH codewords, each protecting a 16-bit
/// value and correcting up to 11 bit errors: the frame's packet identifier
/// in the first half and the frame CRC in the second half.
///
/// Decoding fails with [`AckError::DecodeError`] when a half has more bit
/// errors than it can correct, and with [`AckError::FrameError`] when the
/// reconstructed frame fails validation.
#[derive(Debug, Default, Clone, Copy)]
pub struct BchAckCodec;

/// Implements [`AckCodec`] for 16-byte codewords.
impl AckCodec<16> for BchAckCodec {
    fn encode_ack(frame: AckFrame) -> Result<[u8; 16], AckError> {
        let mut result = [0u8; 16];
        result[..8].copy_from_slice(encode(frame.pkt_id()).to_le_bytes().as_slice());
        result[8..].copy_from_slice(encode(frame.crc()).to_le_bytes().as_slice());
        Ok(result)
    }

    fn decode_ack<C: Crc16>(crc: &C, codeword: &[u8; 16]) -> Result<AckFrame, AckError> {
        let (pkt_id, _) = decode(u64::from_le_bytes(codeword[..8].try_into().unwrap()))
            .ok_or(AckError::DecodeError)?;
        let (crc2, _) = decode(u64::from_le_bytes(codeword[8..].try_into().unwrap()))
            .ok_or(AckError::DecodeError)?;
        Ok(AckFrame::from_parts(crc, pkt_id, crc2)?)
    }
}
