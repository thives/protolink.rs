//! Pluggable gRPC message compression.
//!
//! Compression is negotiated per call with the `grpc-encoding` and
//! `grpc-accept-encoding` headers and applied per message (the flag byte of
//! the [length-prefixed message](crate::lpm)). It is off by default: enable it
//! with [`ClientConfig::compression`](crate::ClientConfig::compression) and
//! [`ServerConfig::compression`](crate::ServerConfig::compression).
//!
//! The algorithm is behind two traits, so a target can use whatever it has:
//!
//! - [`Codec`] is one gRPC message encoding (one `grpc-encoding` value). Use
//!   it to plug in any algorithm.
//! - [`Deflate`] is a raw DEFLATE (RFC 1951) backend. [`Gzip`] turns any
//!   `Deflate` into the `gzip` [`Codec`], so a target that has a DEFLATE
//!   implementation (for example miniz in a microcontroller's ROM) only
//!   implements `Deflate`.
//!
//! The `miniz-oxide` feature adds the stock backend [`MinizOxide`] and the
//! ready-made [`GZIP`] codec, see [`Compression::gzip`].
//!
//! # Example
//!
//! ```
//! use protolink_grpc::compression::{Codec, Compression, CodecError, Deflate, Gzip};
//!
//! // A DEFLATE backend, here one without a compressor (decompress-only).
//! #[derive(Debug)]
//! struct RomInflate;
//!
//! impl Deflate for RomInflate {
//!     fn deflate(&self, _: &[u8], _: &mut Vec<u8>) -> Result<(), CodecError> {
//!         Err(CodecError::Unsupported)
//!     }
//!
//!     fn inflate(&self, input: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError> {
//!         // Call the ROM routine here; never produce more than `limit` bytes.
//! #       let _ = (input, out, limit);
//!         Err(CodecError::Unsupported)
//!     }
//! }
//!
//! static GZIP: Gzip<RomInflate> = Gzip::new(RomInflate);
//! static ACCEPT: [&dyn Codec; 1] = [&GZIP];
//!
//! // Accept gzip requests, never compress responses.
//! let compression = Compression::new(&ACCEPT);
//! # let _ = compression;
//! ```

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use crate::Status;

mod deflate;
#[cfg(feature = "miniz-oxide")]
mod miniz;

pub use deflate::{Deflate, Gzip};
#[cfg(feature = "miniz-oxide")]
pub use miniz::{GZIP, MinizOxide};

/// Why a [`Codec`] or [`Deflate`] operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CodecError {
    /// The decompressed data would exceed the given limit.
    TooLarge,
    /// The input is not valid compressed data.
    Corrupt,
    /// The backend cannot perform the operation, for example a ROM that only
    /// has a decompressor.
    Unsupported,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TooLarge => "decompressed data exceeds the size limit",
            Self::Corrupt => "corrupt compressed data",
            Self::Unsupported => "operation not supported by the compression backend",
        })
    }
}

impl core::error::Error for CodecError {}

impl CodecError {
    /// The status a failed decompression ends a call with.
    pub(crate) fn into_status(self) -> Status {
        match self {
            Self::TooLarge => {
                Status::resource_exhausted("decompressed message exceeds maximum size")
            }
            Self::Corrupt => Status::internal("corrupt compressed message"),
            Self::Unsupported => Status::internal("message decompression is not supported"),
        }
    }
}

/// One gRPC message encoding, i.e. one `grpc-encoding` value such as `gzip`.
///
/// Whole messages are processed at once, which suits both Rust libraries and
/// C APIs. Messages are small and bounded by the configured maximum message
/// size.
///
/// Codecs are shared between calls and threads, hence `Sync`: allocate scratch
/// memory per call, or use interior mutability.
pub trait Codec: Sync + fmt::Debug {
    /// The token sent in `grpc-encoding` and `grpc-accept-encoding`, for
    /// example `"gzip"`: lowercase ASCII without spaces or commas.
    /// `"identity"` is reserved, it is always supported implicitly.
    fn name(&self) -> &'static str;

    /// Append the compressed form of `input` to `out`.
    ///
    /// On error the contents of `out` are unspecified and discarded. A
    /// failure is not fatal: the message is then sent uncompressed.
    fn compress(&self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError>;

    /// Append the decompressed form of `input` to `out`.
    ///
    /// Must not append more than `limit` bytes: return
    /// [`CodecError::TooLarge`] as soon as the result would be larger. This
    /// bounds the memory a hostile peer can make us allocate. On error the
    /// contents of `out` are unspecified.
    fn decompress(&self, input: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError>;
}

/// Default for [`Compression::min_size`], in bytes.
pub const DEFAULT_MIN_SIZE: usize = 64;

/// Compression settings of a client or server.
///
/// Holds `'static` references so that it stays `Copy` and free of generics,
/// and works without an allocator: use `static` items for codecs. A codec
/// created at run time can be leaked with `Box::leak`.
#[derive(Debug, Clone, Copy)]
pub struct Compression {
    /// The encodings we can decode, in order of preference. They are
    /// advertised in `grpc-accept-encoding` (together with `identity`). A
    /// peer using any other encoding is answered with `UNIMPLEMENTED`.
    pub accept: &'static [&'static dyn Codec],
    /// The encoding for outgoing messages. A server only uses it when the
    /// client lists it in `grpc-accept-encoding`; a client uses it for every
    /// request, so only set it if the server supports it.
    ///
    /// It does not have to be in [`accept`](Self::accept): a device may
    /// decompress without being able to compress, or the other way round.
    pub send: Option<&'static dyn Codec>,
    /// Messages shorter than this many bytes are never compressed.
    pub min_size: usize,
}

impl Compression {
    /// No compression: only `identity`. This is the default.
    pub const NONE: Self = Self {
        accept: &[],
        send: None,
        min_size: DEFAULT_MIN_SIZE,
    };

    /// Accept the given encodings. Nothing is compressed until
    /// [`send`](Self::send) is set.
    pub const fn new(accept: &'static [&'static dyn Codec]) -> Self {
        Self {
            accept,
            send: None,
            min_size: DEFAULT_MIN_SIZE,
        }
    }

    /// Compress outgoing messages with `codec`.
    pub const fn send(mut self, codec: &'static dyn Codec) -> Self {
        self.send = Some(codec);
        self
    }

    /// Never compress messages shorter than `min_size` bytes.
    pub const fn min_size(mut self, min_size: usize) -> Self {
        self.min_size = min_size;
        self
    }

    /// Whether any encoding other than `identity` is configured.
    pub const fn is_enabled(&self) -> bool {
        self.send.is_some() || !self.accept.is_empty()
    }

    /// The codec named `name` among [`accept`](Self::accept).
    fn find(&self, name: &str) -> Option<&'static dyn Codec> {
        self.accept.iter().copied().find(|c| c.name() == name)
    }

    /// The `grpc-accept-encoding` value, if there is anything to advertise.
    pub(crate) fn accept_header(&self) -> Option<String> {
        if self.accept.is_empty() {
            return None;
        }
        let mut value = String::new();
        for codec in self.accept {
            value.push_str(codec.name());
            value.push(',');
        }
        value.push_str("identity");
        Some(value)
    }

    /// The codec for incoming messages, from the peer's `grpc-encoding`.
    /// `Err` if the peer uses an encoding we cannot decode.
    pub(crate) fn decoder_for(
        &self,
        grpc_encoding: Option<&str>,
    ) -> Result<Option<&'static dyn Codec>, UnsupportedEncoding> {
        match grpc_encoding.map(str::trim) {
            None | Some("") | Some("identity") => Ok(None),
            Some(name) => self.find(name).map(Some).ok_or(UnsupportedEncoding),
        }
    }

    /// The codec for outgoing messages: [`send`](Self::send), if the peer's
    /// `grpc-accept-encoding` lists it.
    pub(crate) fn encoder_for(
        &self,
        peer_accept_encoding: Option<&str>,
    ) -> Option<&'static dyn Codec> {
        let codec = self.send?;
        let accepted = peer_accept_encoding?
            .split(',')
            .any(|name| name.trim() == codec.name());
        accepted.then_some(codec)
    }

    /// Panics (in debug builds) on a codec name that cannot be sent in a
    /// header token list or that shadows `identity`.
    pub(crate) fn debug_validate(&self) {
        for codec in self.accept.iter().copied().chain(self.send) {
            let name = codec.name();
            debug_assert!(
                !name.is_empty()
                    && name != "identity"
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_graphic() && b != b',' && !b.is_ascii_uppercase()),
                "invalid compression codec name {name:?}"
            );
        }
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self::NONE
    }
}

/// The peer uses a `grpc-encoding` that is not configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnsupportedEncoding;
