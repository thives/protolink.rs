//! Agent and DUT execution node: the `lua.execution.v1` service over Protolink, driving the reduced VM.
//!
//! One [`Node`] serves one execution contract. It is `no_std` and needs `alloc`, so the same code runs on the host and
//! on the remote targets. The embedding supplies everything that touches the world: a monotonic clock through
//! [`Node::tick`], the native operations through a poll-based [`Platform`], and, for a node with child slots, a
//! [`ChildClient`](protolink_lua::child::ChildClient) such as [`LinkChildren`] over one [`ChildLink`] per slot.
//!
//! Requests are answered from the lifecycle and the ledger, and never run Lua. Lua runs in [`Node::poll`], one bounded
//! round at a time, which the embedding calls between requests. This keeps control calls live while executions run.
//!
//! [`Loopback`] carries Protolink bytes between two nodes in one thread. It is the software stand-in for the I2C and
//! UART transports, and it is not a hardware transport. A hardware transport needs its own bench evidence.
#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

extern crate alloc;
#[cfg(test)]
extern crate std;

mod children;
mod config;
mod link;
mod loopback;
mod node;
mod platform;

#[cfg(test)]
mod tests;

pub use children::{ChildSettings, LinkChildren};
pub use config::{ChildBounds, ChildRoute, ConfigError, NodeConfig};
pub use link::{ChildLink, Immediate as ImmediateLink, ImmediateCall, LinkError};
pub use loopback::Loopback;
pub use node::{DiscardError, Envelope, Node, RecoverError, Usage};
pub use platform::{
    Immediate as ImmediateNatives, NativeCall, NativeStop, NativeTicket, NoNatives, Platform,
};
