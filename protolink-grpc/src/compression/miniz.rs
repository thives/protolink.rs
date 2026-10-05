//! The stock DEFLATE backend, built on `miniz_oxide`.

use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;

use miniz_oxide::deflate::compress_to_vec;
use miniz_oxide::inflate::TINFLStatus;
use miniz_oxide::inflate::core::{
    DecompressorOxide, decompress, inflate_flags::TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF,
};

use super::{Codec, CodecError, Compression, Deflate, Gzip};

/// DEFLATE through the pure-Rust `miniz_oxide` crate (feature `miniz-oxide`).
///
/// The compressor allocates a considerable amount of working memory per call.
/// On small targets prefer a backend over a smaller or ROM-resident
/// implementation, or accept compressed requests without compressing
/// responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinizOxide {
    level: u8,
}

impl MinizOxide {
    /// Backend compressing at `level`, from 0 (stored) to 10 (smallest, slowest).
    /// Higher values are treated as 10.
    pub const fn new(level: u8) -> Self {
        Self {
            level: if level > 10 { 10 } else { level },
        }
    }

    /// The compression level.
    pub const fn level(&self) -> u8 {
        self.level
    }
}

impl Default for MinizOxide {
    fn default() -> Self {
        Self::new(6)
    }
}

impl Deflate for MinizOxide {
    fn deflate(&self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
        let compressed = compress_to_vec(input, self.level);
        if out.is_empty() {
            *out = compressed;
        } else {
            out.extend_from_slice(&compressed);
        }
        Ok(())
    }

    fn inflate(&self, input: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError> {
        let mut input = input;
        let mut data = vec![0; input.len().saturating_mul(2).max(1).min(limit)];
        let mut state = Box::<DecompressorOxide>::default();
        let mut written = 0;
        loop {
            let (status, consumed, produced) = decompress(
                &mut state,
                input,
                &mut data,
                written,
                TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF,
            );
            written += produced;
            input = input.get(consumed..).ok_or(CodecError::Corrupt)?;
            match status {
                TINFLStatus::Done if input.is_empty() => {
                    data.truncate(written);
                    break;
                }
                TINFLStatus::HasMoreOutput if data.len() < limit => {
                    data.resize(data.len().saturating_mul(2).max(1).min(limit), 0);
                }
                TINFLStatus::HasMoreOutput => return Err(CodecError::TooLarge),
                _ => return Err(CodecError::Corrupt),
            }
        }
        if out.is_empty() {
            *out = data;
        } else {
            out.extend_from_slice(&data);
        }
        Ok(())
    }
}

/// The `gzip` codec on [`MinizOxide`] at the default compression level.
pub static GZIP: Gzip<MinizOxide> = Gzip::new(MinizOxide::new(6));

static GZIP_ACCEPT: [&dyn Codec; 1] = [&GZIP];

impl Compression {
    /// Accept and send `gzip`, using [`GZIP`]. Needs the `miniz-oxide`
    /// feature.
    pub fn gzip() -> Self {
        Self::new(&GZIP_ACCEPT).send(&GZIP)
    }
}
