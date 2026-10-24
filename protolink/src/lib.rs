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
//! ## Assembling a stack
//!
//! Every driver is assembled with the same vocabulary:
//!
//! | Operation | Contract |
//! |---|---|
//! | [`server(io)`](server), [`client(io)`](client) | Create a factory |
//! | `.with_*()` | Consume and configure a factory |
//! | `.as_*()` | Borrowed view or adapter, not an ownership transfer |
//! | `.into_*()` | Consuming conversion |
//! | `.build()` | Construct and validate; no transport I/O or task spawning |
//! | [`.serve(&mut services)`](Server::serve) | Drive one server connection |
//! | `.run()` | Drive an adaptor or pump runner |
//!
//! The async factories live at the crate root, the blocking ones in
//! [`blocking`] and the tokio ones in [`tokio`]. Application services are
//! independent of the driver: they implement generated service traits, are
//! wrapped in a generated `<Service>Dispatch` (or several in a
//! [`Registry`]), and meet the driver only at `serve`:
//!
//! ```no_run
//! # async fn demo<IO, T, H>(io: IO, timer: T, mut services: H) -> Result<(), protolink::Error<IO::Error>>
//! # where IO: embedded_io_async::Read + embedded_io_async::Write, T: protolink::Timer, H: protolink::Handler {
//! let server = protolink::server(io)
//!     .with_deadline_timer(timer)
//!     .with_max_message_size(4096)
//!     .build();
//! server.serve(&mut services).await?;
//! # Ok(()) }
//! ```
//!
//! ## Features
//!
//! - `async` / `embedded-io`: [`server`] and [`client`] over
//!   `embedded_io_async::{Read, Write}`, and the [`link`] module.
//! - `blocking`: [`blocking::server`] and [`blocking::client`] over
//!   `embedded_io::{Read, Write}`. A blocking server built
//!   [`with_wakeable_reads`](blocking::ServerBuilder::with_wakeable_reads)
//!   serves transports that implement [`blocking::WakeableRead`], so streaming
//!   handlers that were `Pending` progress as soon as they are woken, without
//!   relying on read timeouts.
//! - `tokio`: [`tokio`] factories adapting tokio I/O to the async drivers.
//! - Deadlines: [`Timer`] / [`Clock`] let the drivers enforce `grpc-timeout`;
//!   see below.
//! - `std`: `std` support in dependencies, including a default ARQ
//!   retransmission timer for [`link::reliable`] ([`link::StdTimer`]). Without
//!   it, supply a platform timer with
//!   [`with_retransmission_timer`](link::ReliableBuilder::with_retransmission_timer).
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
//! source, which the sans-IO cores don't have. Every factory makes the choice
//! explicit:
//!
//! - tokio: [`tokio::server`] and [`tokio::client`] supply a
//!   [`tokio::TokioTimer`].
//! - async: `.with_deadline_timer(timer)` takes a [`Timer`];
//!   `.without_deadlines()` opts out.
//! - blocking: `.with_deadline_clock(clock)` takes a [`Clock`] and needs a
//!   transport that implements [`blocking::ReadTimeout`] (and, to bound output
//!   too, [`blocking::OutputTimeout`]); `.without_deadlines()` opts out.
//!
//! Without a time source calls still carry `grpc-timeout` but are never
//! expired locally. With a timer the transport `read` must be cancel-safe,
//! even without streaming: a pending read is dropped when a deadline is
//! reached. A unary handler that is already running can't be preempted; it
//! can check [`CallContext::remaining`]. [`Timer`] is unrelated to
//! `link::Timer`, the ARQ retransmission timer.
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

pub use protolink_grpc as grpc;
pub use protolink_grpc::{
    BlockingStreamingCall, BlockingStreamingTransport, BlockingUnaryTransport, CallContext, CallId,
    CallOptions, ClientConfig, Code, Compression, Dispatcher, Handler, InvalidMetadata, Metadata,
    MetadataValue, MethodDescriptor, MethodKind, MultiDispatch, Next, Registry, Response,
    ResponseMetadata, Route, RouteConflict, Scheme, ServerConfig, Status, StreamingCall,
    StreamingTransport, UnaryTransport, compression,
};

mod builder;
pub use builder::DeadlinesUnspecified;

mod timer;
pub use timer::{Clock, NoTimer, Timer};

mod error;
pub use error::Error;

#[cfg(feature = "async")]
mod asynch;
#[cfg(feature = "async")]
pub use asynch::{Call, Client, ClientBuilder, Server, ServerBuilder, client, server};
#[cfg(feature = "async")]
mod driver;
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
