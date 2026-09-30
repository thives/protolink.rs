//! # protolink-grpc
//!
//! Sans-IO, `no_std + alloc` unary gRPC core used by protolink:
//!
//! - [`Server`]: turns HTTP/2 requests into calls on a [`Handler`] and encodes
//!   responses, statuses and trailers.
//! - [`Client`]: issues unary calls and maps responses/trailers to
//!   `Result<Vec<u8>, Status>`.
//! - [`codec`]: micropb encode/decode helpers (feature `micropb`, default).
//! - [`UnaryTransport`] / [`BlockingUnaryTransport`]: the interface generated
//!   clients call into; implemented by protolink's I/O drivers.
//!
//! HTTP/2 framing comes from [`protolink_http2`]. Neither type performs I/O.
//!
//! ## Compatibility profile
//!
//! Supported: unary RPCs, uncompressed protobuf payloads,
//! `application/grpc[+proto]`, `grpc-status`/`grpc-message`, trailers-only
//! error responses, bounded message sizes.
//!
//! Not supported: streaming RPCs (answered with `UNIMPLEMENTED` by generated
//! servers), compression, deadlines (`grpc-timeout` is ignored), custom
//! metadata, reflection, health checking.
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

mod client;
#[cfg(feature = "micropb")]
pub mod codec;
mod handler;
pub mod lpm;
mod server;
pub mod status;

pub use protolink_http2 as http2;

pub use client::{CallId, Client, ClientConfig};
pub use handler::{FnHandler, Handler};
pub use server::{Server, ServerConfig};
pub use status::{Code, Status};

use alloc::vec::Vec;
use core::future::Future;

/// Default maximum size of a single protobuf message, in bytes.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 4096;

#[doc(hidden)]
pub mod __private {
    pub use alloc::vec::Vec;
}

/// Performs one unary gRPC call on already-encoded protobuf bytes.
///
/// Generated async clients are generic over this trait.
pub trait UnaryTransport {
    /// Call `path` (`/package.Service/Method`) with `request` and return the
    /// encoded response message.
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, Status>>;
}

/// Blocking counterpart of [`UnaryTransport`].
pub trait BlockingUnaryTransport {
    /// Call `path` (`/package.Service/Method`) with `request` and return the
    /// encoded response message.
    fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status>;
}

impl<T: UnaryTransport + ?Sized> UnaryTransport for &mut T {
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, Status>> {
        (**self).unary(path, request)
    }
}

impl<T: BlockingUnaryTransport + ?Sized> BlockingUnaryTransport for &mut T {
    fn unary(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        (**self).unary(path, request)
    }
}

#[cfg(test)]
mod tests;
