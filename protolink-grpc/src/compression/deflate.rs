//! Raw DEFLATE backends and the gzip container around them.

use alloc::vec::Vec;
use core::fmt;

use super::{Codec, CodecError};

/// A raw DEFLATE (RFC 1951) implementation.
///
/// This is what miniz, zlib and most hardware or ROM libraries offer. Wrap it
/// in [`Gzip`] to get the `gzip` [`Codec`]; the container (header, CRC-32 and
/// size trailer) is handled there, so a backend only deals with the bare
/// DEFLATE stream.
///
/// A backend that cannot do both directions (some ROMs ship only a
/// decompressor) returns [`CodecError::Unsupported`] from the other method.
/// Backends are shared between calls and threads, hence `Sync`.
pub trait Deflate: Sync + fmt::Debug {
    /// Append the raw DEFLATE stream of `input` to `out`.
    ///
    /// On error the contents of `out` are unspecified and discarded.
    fn deflate(&self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError>;

    /// Append the data of the raw DEFLATE stream `input` (no gzip or zlib
    /// header or trailer) to `out`.
    ///
    /// Must not append more than `limit` bytes: return
    /// [`CodecError::TooLarge`] as soon as the result would be larger. On
    /// error the contents of `out` are unspecified.
    ///
    /// Success requires a complete, terminated DEFLATE stream consuming all
    /// of `input` (apart from unused bits in its final byte). Truncated streams,
    /// trailing bytes and concatenated streams must return
    /// [`CodecError::Corrupt`], not a successfully decoded prefix. This is
    /// necessary for [`Gzip`] to validate exactly one member.
    fn inflate(&self, input: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError>;

    /// CRC-32 (IEEE 802.3, as used by gzip and zlib) of `data`, continuing
    /// from `crc`, with the same convention as zlib's `crc32(crc, data, len)`:
    /// start with `0`, pass the previous result to continue.
    ///
    /// The default is a small table-driven software implementation. Override
    /// it to use a hardware or ROM routine.
    fn crc32(&self, crc: u32, data: &[u8]) -> u32 {
        crc32(crc, data)
    }
}

/// The `gzip` [`Codec`] (RFC 1952) on top of a [`Deflate`] backend.
///
/// One gzip member is written per message, which is what gRPC peers expect.
/// Decoding verifies the CRC-32 and size trailer, and accepts the optional
/// header fields other implementations may add. Concatenated members are not
/// supported and are rejected, as are bytes after the DEFLATE stream.
#[derive(Debug, Clone, Copy, Default)]
pub struct Gzip<D>(D);

impl<D> Gzip<D> {
    /// Wrap `backend`.
    pub const fn new(backend: D) -> Self {
        Self(backend)
    }

    /// The DEFLATE backend.
    pub const fn backend(&self) -> &D {
        &self.0
    }
}

/// gzip header: magic, deflate, no flags, no timestamp, no extra flags,
/// unknown OS.
const HEADER: [u8; 10] = [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];
/// CRC-32 and size.
const TRAILER_LEN: usize = 8;

const FHCRC: u8 = 1 << 1;
const FEXTRA: u8 = 1 << 2;
const FNAME: u8 = 1 << 3;
const FCOMMENT: u8 = 1 << 4;
const RESERVED: u8 = 0xe0;

impl<D: Deflate> Codec for Gzip<D> {
    fn name(&self) -> &'static str {
        "gzip"
    }

    fn compress(&self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
        out.extend_from_slice(&HEADER);
        self.0.deflate(input, out)?;
        out.extend_from_slice(&self.0.crc32(0, input).to_le_bytes());
        out.extend_from_slice(&(input.len() as u32).to_le_bytes());
        Ok(())
    }

    fn decompress(&self, input: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError> {
        let header_len = header_len(input, |data| self.0.crc32(0, data))?;
        let body_end = input
            .len()
            .checked_sub(TRAILER_LEN)
            .filter(|&end| end >= header_len)
            .ok_or(CodecError::Corrupt)?;
        let (body, trailer) = input[header_len..].split_at(body_end - header_len);

        let start = out.len();
        self.0.inflate(body, out, limit)?;
        let data = &out[start..];
        if data.len() > limit {
            return Err(CodecError::TooLarge);
        }
        let crc = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
        let size = u32::from_le_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]);
        if self.0.crc32(0, data) != crc || data.len() as u32 != size {
            return Err(CodecError::Corrupt);
        }
        Ok(())
    }
}

/// Length of the gzip header at the start of `input`, optional fields
/// included.
fn header_len(input: &[u8], crc32: impl Fn(&[u8]) -> u32) -> Result<usize, CodecError> {
    if input.len() < HEADER.len() || input[..3] != [0x1f, 0x8b, 8] {
        return Err(CodecError::Corrupt);
    }
    let flags = input[3];
    if flags & RESERVED != 0 {
        return Err(CodecError::Corrupt);
    }
    let mut pos = HEADER.len();
    if flags & FEXTRA != 0 {
        let len = input.get(pos..pos + 2).ok_or(CodecError::Corrupt)?;
        pos += 2 + usize::from(u16::from_le_bytes([len[0], len[1]]));
    }
    for flag in [FNAME, FCOMMENT] {
        if flags & flag != 0 {
            let rest = input.get(pos..).ok_or(CodecError::Corrupt)?;
            pos += rest
                .iter()
                .position(|&b| b == 0)
                .ok_or(CodecError::Corrupt)?
                + 1;
        }
    }
    if flags & FHCRC != 0 {
        let stored = input.get(pos..pos + 2).ok_or(CodecError::Corrupt)?;
        let stored = u16::from_le_bytes([stored[0], stored[1]]);
        if crc32(&input[..pos]) as u16 != stored {
            return Err(CodecError::Corrupt);
        }
        pos += 2;
    }
    if pos > input.len() {
        return Err(CodecError::Corrupt);
    }
    Ok(pos)
}

/// Table for processing a CRC-32 four bits at a time.
const CRC_NIBBLES: [u32; 16] = {
    let mut table = [0; 16];
    let mut i = 0;
    while i < 16 {
        let mut c = i as u32;
        let mut bit = 0;
        while bit < 4 {
            c = if c & 1 == 1 {
                (c >> 1) ^ 0xedb8_8320
            } else {
                c >> 1
            };
            bit += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
};

/// Software CRC-32 with zlib's calling convention.
pub(crate) fn crc32(crc: u32, data: &[u8]) -> u32 {
    let mut c = !crc;
    for &byte in data {
        c = CRC_NIBBLES[((c ^ u32::from(byte)) & 0xf) as usize] ^ (c >> 4);
        c = CRC_NIBBLES[((c ^ u32::from(byte >> 4)) & 0xf) as usize] ^ (c >> 4);
    }
    !c
}
