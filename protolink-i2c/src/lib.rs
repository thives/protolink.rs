//! Hardware-facing I2C support for protolink: the mailbox protocol, the controller transaction primitives and the
//! target-driver contract.
//!
//! This crate is `no_std` and allocation-free. It owns no queues and starts no tasks; buffering, scheduling and runners
//! live in `protolink-adaptors`.
//!
//! # Mailbox protocol
//!
//! A byte stream rides on fixed-size mailbox transactions, see [`mailbox`]. The controller polls the target with a
//! [`MAILBOX_SIZE`]-byte read. Each reply names the target's boot identifier, the connection epoch it serves, and the
//! receive credit it has left. The controller writes requests with the same header: payload bytes of the connection it
//! addresses, or an open or retire operation.
//!
//! A connection is identified by a [`mailbox::ConnectionId`]. The controller opens a new connection, and the target
//! adopts it. Traffic that names any other connection is ignored, so bytes from an earlier connection cannot reach a
//! later one. An empty reply means *idle*, not end of stream.
//!
//! # Roles
//!
//! - [`controller`]: [`read_mailbox`](controller::read_mailbox) and [`write_transaction`](controller::write_transaction)
//!   over any [`embedded_hal_async::i2c::I2c`].
//! - [`target`]: the [`I2cTarget`](target::I2cTarget) trait, which a board implements for its I2C peripheral in target
//!   mode. `embedded-hal` has no such trait.
#![no_std]
#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod address;
pub mod controller;
pub mod mailbox;
pub mod target;

pub use address::{Address, InvalidAddress};
pub use mailbox::{ConnectionId, MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE};
