//! # protolink-grpc
//!
//! Sans-IO, `no_std + alloc` gRPC core used by protolink:
//!
//! - [`Server`]: turns HTTP/2 requests into calls on a [`Handler`] and encodes
//!   responses, statuses and trailers, for unary and streaming RPCs.
//! - [`Client`]: issues unary and streaming calls and maps responses and
//!   trailers to messages and [`Status`]es.
//! - [`lpm`]: incremental length-prefixed message framing.
//! - [`codec`]: micropb encode/decode helpers and typed streaming call
//!   wrappers (feature `micropb`, default).
//! - [`UnaryTransport`] / [`BlockingUnaryTransport`] and
//!   [`StreamingTransport`] / [`BlockingStreamingTransport`]: the interfaces
//!   generated clients call into; implemented by protolink's I/O drivers.
//!
//! HTTP/2 framing comes from [`protolink_http2`]. Neither type performs I/O.
//!
//! ## Streaming
//!
//! All four RPC shapes are supported: unary, server-streaming,
//! client-streaming and bidirectional. Streaming is executor-agnostic: server
//! handlers produce responses through a poll interface built on
//! [`core::task::Context`] (see [`Handler`]), and clients pull messages and
//! push requests through [`Client`]'s per-call methods.
//!
//! Memory stays bounded under HTTP/2 flow control. Both sides use
//! [`FlowControl::Manual`](protolink_http2::FlowControl::Manual) by default
//! and only credit received bytes back to the peer once the application has
//! consumed the messages they carry. A server stops pulling responses from a
//! call while one of its messages waits for the peer's flow-control window.
//!
//! ## Compression
//!
//! Message compression (`grpc-encoding` / `grpc-accept-encoding`) is optional
//! and off by default. The algorithm is pluggable through the
//! [`compression::Codec`] and [`compression::Deflate`] traits, so a target can
//! use a DEFLATE implementation it already has, such as one in ROM; the
//! `miniz-oxide` feature provides a stock one. See [`compression`].
//!
//! ## Deadlines
//!
//! A call's timeout ([`CallOptions::timeout`], [`ClientConfig::default_timeout`])
//! is sent as `grpc-timeout`, and both sides end the call with
//! `DEADLINE_EXCEEDED` when it runs out. The cores never read a clock: the
//! caller reports time with [`Client::tick`] / [`Server::tick`] (a monotonic
//! [`Duration`](core::time::Duration) since any fixed point) and learns when to
//! wake up from `next_deadline`. A server's [`Handler`] methods get the call's
//! deadline in [`CallContext`]. The I/O drivers in `protolink` do the ticking.
//!
//! ## Metadata
//!
//! Custom metadata ([`Metadata`]) travels as extra HTTP/2 headers and trailers.
//! A client sets request metadata in [`CallOptions::metadata`] and gets the
//! response's headers and trailers back in [`Response`] (unary) or from the
//! streaming call; trailers of a failed call are in [`Status::metadata`]. A
//! server reads request metadata and sets response metadata through
//! [`CallContext`]. Binary values (keys ending in `-bin`) are base64 on the
//! wire.
//!
//! ## Compatibility profile
//!
//! Supported: unary and streaming RPCs, protobuf payloads (optionally
//! compressed), `application/grpc[+proto]`, `grpc-status`/`grpc-message`,
//! trailers-only responses, bounded message sizes, deadlines (`grpc-timeout`),
//! custom metadata.
//!
//! Not supported: reflection, health checking.
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

mod client;
#[cfg(feature = "micropb")]
pub mod codec;
pub mod compression;
mod handler;
mod inbound;
pub mod lpm;
mod metadata;
mod server;
pub mod status;
mod timeout;

pub use protolink_http2 as http2;

pub use client::{CallOptions, Client, ClientConfig};
pub use compression::Compression;
pub use handler::{CallContext, FnHandler, Handler, ResponseMetadata};
pub use metadata::{InvalidMetadata, Metadata, MetadataValue};
pub use server::{Server, ServerConfig};
pub use status::{Code, Status};

use alloc::vec::Vec;
use core::future::Future;

/// Default maximum size of a single protobuf message, in bytes.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 4096;

/// Identifies an in-flight call on a [`Client`] or [`Server`] (its HTTP/2
/// stream id).
pub type CallId = protolink_http2::StreamId;

/// The successful outcome of a unary call: the response message and the
/// metadata that came with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response<T> {
    /// The response message.
    pub message: T,
    /// Metadata of the response headers. Empty for a trailers-only response.
    pub headers: Metadata,
    /// Metadata of the response trailers.
    pub trailers: Metadata,
}

impl<T> Response<T> {
    /// A response without metadata.
    pub fn new(message: T) -> Self {
        Self {
            message,
            headers: Metadata::new(),
            trailers: Metadata::new(),
        }
    }

    /// Map the message, keeping the metadata.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Response<U> {
        Response {
            message: f(self.message),
            headers: self.headers,
            trailers: self.trailers,
        }
    }

    /// Drop the metadata.
    pub fn into_message(self) -> T {
        self.message
    }
}

/// The shape of an RPC method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MethodKind {
    /// One request, one response.
    Unary,
    /// One request, zero or more responses.
    ServerStreaming,
    /// Zero or more requests, one response after the client half-closes.
    ClientStreaming,
    /// Zero or more requests and responses, flowing independently.
    BidiStreaming,
}

impl MethodKind {
    /// The client may send more than one request message.
    pub const fn is_client_streaming(self) -> bool {
        matches!(self, Self::ClientStreaming | Self::BidiStreaming)
    }

    /// The server may send more than one response message.
    pub const fn is_server_streaming(self) -> bool {
        matches!(self, Self::ServerStreaming | Self::BidiStreaming)
    }
}

/// The next item on a message stream: a message, or the end of the call with
/// its final status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next<T> {
    /// One message.
    Message(T),
    /// The call is complete. `Ok(())` is sent as `grpc-status: 0`.
    Done(Result<(), Status>),
}

impl<T> Next<T> {
    /// Map the message.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Next<U> {
        match self {
            Self::Message(m) => Next::Message(f(m)),
            Self::Done(r) => Next::Done(r),
        }
    }
}

#[doc(hidden)]
pub mod __private {
    pub use alloc::vec::Vec;
    pub use core::task::{Context, Poll};
}

/// Performs one unary gRPC call on already-encoded protobuf bytes.
///
/// Generated async clients are generic over this trait.
pub trait UnaryTransport {
    /// Call `path` (`/package.Service/Method`) with `request` and return the
    /// encoded response message and its metadata.
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> impl Future<Output = Result<Response<Vec<u8>>, Status>>;
}

/// Blocking counterpart of [`UnaryTransport`].
pub trait BlockingUnaryTransport {
    /// Call `path` (`/package.Service/Method`) with `request` and return the
    /// encoded response message and its metadata.
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> Result<Response<Vec<u8>>, Status>;
}

impl<T: UnaryTransport + ?Sized> UnaryTransport for &mut T {
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> impl Future<Output = Result<Response<Vec<u8>>, Status>> {
        (**self).unary(path, request, options)
    }
}

impl<T: BlockingUnaryTransport + ?Sized> BlockingUnaryTransport for &mut T {
    fn unary(
        &mut self,
        path: &str,
        request: &[u8],
        options: CallOptions,
    ) -> Result<Response<Vec<u8>>, Status> {
        (**self).unary(path, request, options)
    }
}

/// Starts streaming gRPC calls on already-encoded protobuf bytes.
///
/// Generated async clients use it for server-, client- and bidirectional
/// streaming methods. The returned [`StreamingCall`] borrows the transport;
/// dropping the call before it completes cancels it.
///
/// `start` takes `&self`, so it is up to the implementation how many calls may
/// be active on one transport at the same time, and what starting another one
/// does.
pub trait StreamingTransport {
    /// Handle of an active call.
    type Call<'a>: StreamingCall
    where
        Self: 'a;

    /// Start a call of `path` (`/package.Service/Method`).
    fn start(
        &self,
        path: &str,
        options: CallOptions,
    ) -> impl Future<Output = Result<Self::Call<'_>, Status>>;
}

/// An active streaming call started by a [`StreamingTransport`].
pub trait StreamingCall {
    /// Send one request message. Completes once the message is queued within
    /// the call's flow-control budget. Messages sent after the server has
    /// finished the call are discarded; its outcome is reported by
    /// [`message`](Self::message).
    fn send(&mut self, message: &[u8]) -> impl Future<Output = Result<(), Status>>;

    /// Half-close: no more request messages will be sent. Responses keep
    /// flowing.
    fn close_send(&mut self) -> impl Future<Output = Result<(), Status>>;

    /// Next response message; `Ok(None)` once the call completed with
    /// `grpc-status: 0`, `Err` if it failed. Messages received before a
    /// failure are returned first.
    fn message(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, Status>>;

    /// Metadata of the response headers, once the server has sent them. `None`
    /// before that, and for a trailers-only response.
    fn headers(&self) -> Option<Metadata>;

    /// Metadata of the response trailers. `None` until the call has completed
    /// (that is, [`message`](Self::message) returned `Ok(None)` or an error).
    fn trailers(&self) -> Option<Metadata>;
}

/// Blocking counterpart of [`StreamingTransport`].
pub trait BlockingStreamingTransport {
    /// Handle of an active call.
    type Call<'a>: BlockingStreamingCall
    where
        Self: 'a;

    /// Start a call of `path` (`/package.Service/Method`).
    fn start(&self, path: &str, options: CallOptions) -> Result<Self::Call<'_>, Status>;
}

/// Blocking counterpart of [`StreamingCall`].
pub trait BlockingStreamingCall {
    /// See [`StreamingCall::send`].
    fn send(&mut self, message: &[u8]) -> Result<(), Status>;
    /// See [`StreamingCall::close_send`].
    fn close_send(&mut self) -> Result<(), Status>;
    /// See [`StreamingCall::message`].
    fn message(&mut self) -> Result<Option<Vec<u8>>, Status>;
    /// See [`StreamingCall::headers`].
    fn headers(&self) -> Option<Metadata>;
    /// See [`StreamingCall::trailers`].
    fn trailers(&self) -> Option<Metadata>;
}

impl<T: StreamingTransport + ?Sized> StreamingTransport for &mut T {
    type Call<'a>
        = T::Call<'a>
    where
        Self: 'a;

    fn start(
        &self,
        path: &str,
        options: CallOptions,
    ) -> impl Future<Output = Result<Self::Call<'_>, Status>> {
        (**self).start(path, options)
    }
}

impl<T: BlockingStreamingTransport + ?Sized> BlockingStreamingTransport for &mut T {
    type Call<'a>
        = T::Call<'a>
    where
        Self: 'a;

    fn start(&self, path: &str, options: CallOptions) -> Result<Self::Call<'_>, Status> {
        (**self).start(path, options)
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_compression;
#[cfg(test)]
mod tests_deadline;
#[cfg(test)]
mod tests_metadata;

#[cfg(test)]
mod tests_streaming;
