//! Hardware-facing I2C support for protolink: the mailbox protocol, the
//! controller transaction primitives and the target-driver contract.
//!
//! This crate is `no_std` and allocation-free. It owns no queues and starts no
//! tasks; buffering, scheduling and runners live in `protolink-adaptors`.
//!
//! # Mailbox protocol
//!
//! A byte stream rides on fixed-size mailbox transactions, see [`mailbox`].
//! The controller polls the target with a [`MAILBOX_SIZE`]-byte read and
//! writes raw payload bytes, limited by the receive credit the target
//! advertised. An empty mailbox means *idle*, not end of stream.
//!
//! # Roles
//!
//! - [`controller`]: [`read_mailbox`](controller::read_mailbox) and
//!   [`write_payload`](controller::write_payload) over any
//!   [`embedded_hal_async::i2c::I2c`].
//! - [`target`]: the [`I2cTarget`](target::I2cTarget) trait, which a board
//!   implements for its I2C peripheral in target mode. `embedded-hal` has no
//!   such trait.
#![no_std]
#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod address;
pub mod controller;
pub mod mailbox;
pub mod target;

pub use address::{Address, InvalidAddress};
pub use mailbox::{MAILBOX_PAYLOAD_MAX, MAILBOX_SIZE};
