//! # Protolink
//!
//! Protolink is a gRPC framework that unifies embedded transports, framing,
//! reliability, HTTP/2, gRPC bridging, and code generation.
//!
//! ## Layers
//!
//! ```text
//! application  ── implements generated `<Service>` trait / calls `<Service>Client`
//! codegen      ── protolink-grpc-gen (service glue) + micropb-gen or prost-build (messages)
//! gRPC         ── protolink-grpc   (re-exported as `protolink::grpc`)
//! HTTP/2       ── protolink-http2  (on zerodds-http2 + zerodds-hpack)
//! drivers      ── this crate: async / blocking / tokio I/O loops
//! link         ── this crate: COBS framing + ARQ reliability (optional)
//! transport    ── anything implementing embedded-io(-async) or tokio I/O
//! ```
//!
//! ## Features
//!
//! - `async` / `embedded-io`: [`serve`] and [`Client`] over
//!   `embedded_io_async::{Read, Write}`, and the [`link`] module (including the
//!   optional [`link::pump`] lower layer for DMA UARTs).
//! - `blocking`: [`blocking::serve`] and [`blocking::Client`] over
//!   `embedded_io::{Read, Write}`. [`blocking::serve_wakeable`] serves
//!   transports that implement [`blocking::WakeableRead`], so streaming
//!   handlers that were `Pending` progress as soon as they are woken, without
//!   relying on read timeouts.
//! - `tokio`: [`tokio`] helpers adapting tokio I/O to the async drivers.
//! - `portable-atomic`: use [`portable-atomic`](https://docs.rs/portable-atomic) for link and
//!   ARQ atomics on targets that need a portable implementation. On targets without native
//!   compare-and-swap, also enable `portable-atomic-critical-section` and link a platform-provided
//!   critical-section implementation.
//! - Deadlines: [`Timer`] / [`Clock`] let the drivers enforce `grpc-timeout`;
//!   see below.
//! - `std`: `std` support in dependencies, including [`link::reliable`] (which
//!   uses `std`'s clock for ARQ retransmission timeouts). Without it, use
//!   [`link::reliable_with_timer`] and supply a platform timer.
//!
//! An allocator is always required (`no_std + alloc`).
//!
//! ## Scope
//!
//! Unary, server-streaming, client-streaming and bidirectional streaming
//! RPCs. See [`grpc`] for the full compatibility profile.
//!
//! Streaming servers implement the generated `<Service>` trait's streaming
//! methods, which pull responses through a poll interface (see
//! [`Handler`]). Streaming clients get a [`Call`] (or [`blocking::Call`])
//! from [`Client::streaming`], or typed wrappers from generated clients. The
//! high-level clients allow several active streaming calls on one connection.
//! The async client drops a pending read when another call has something to
//! write, which needs a cancel-safe `read` (see [`Client`]); the blocking
//! client can't interrupt a read, so interleave sends before blocking on a
//! response. The sans-IO [`grpc::Client`] is available for direct scheduling.
//!
//! ## Deadlines
//!
//! A call's timeout ([`CallOptions`], [`ClientConfig::default_timeout`]) is
//! sent to the server as `grpc-timeout`, and generated clients have a
//! `<method>_with_options` variant of every method. Enforcing it needs a time
//! source, which the sans-IO cores don't have; the drivers take one:
//!
//! - tokio: [`tokio::serve`] and [`tokio::client`] use
//!   [`tokio::TokioTimer`].
//! - async: [`Client::with_timer`] and [`serve_with_timer`] take a [`Timer`].
//! - blocking: [`blocking::Client::with_clock`], [`blocking::serve_with_clock`]
//!   and [`blocking::serve_wakeable_with_clock`] take a [`Clock`], and the
//!   transport sets a read timeout through [`blocking::ReadTimeout`].
//!
//! Without a time source ([`NoTimer`], the default of [`Client::new`] and
//! [`serve`]) calls still carry `grpc-timeout` but are never expired locally.
//! With a timer the transport `read` must be cancel-safe, even without
//! streaming: a pending read is dropped when a deadline is reached. A unary
//! handler that is already running can't be preempted; it can check
//! [`CallContext::remaining`]. [`Timer`] is unrelated to
//! `link::Timer`, the ARQ retransmission timer.
#![cfg_attr(not(feature = "std"), no_std)]
// `deny` rather than `forbid` so that `link::ring`, the only module that needs
// `unsafe`, can opt in locally.
#![deny(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

pub use protolink_grpc as grpc;
pub use protolink_grpc::{
    BlockingStreamingCall, BlockingStreamingTransport, BlockingUnaryTransport, CallContext, CallId,
    CallOptions, ClientConfig, Code, Compression, Handler, InvalidMetadata, Metadata,
    MetadataValue, MethodKind, Next, Response, ResponseMetadata, Scheme, ServerConfig, Status,
    StreamingCall, StreamingTransport, UnaryTransport, compression,
};

mod timer;
pub use timer::{Clock, NoTimer, Timer};

mod error;
pub use error::Error;

#[cfg(feature = "async")]
mod asynch;
#[cfg(feature = "async")]
pub use asynch::{Call, Client, serve, serve_with_timer};
#[cfg(feature = "async")]
mod shared;

#[cfg(feature = "async")]
pub mod link;

#[cfg(feature = "blocking")]
pub mod blocking;

#[cfg(feature = "tokio")]
pub mod tokio;

/// Size of the stack buffer used by the drivers for each transport read.
pub const READ_CHUNK: usize = 512;
