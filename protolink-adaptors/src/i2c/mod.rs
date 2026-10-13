//! I2C adaptors: [`i2c_controller`] and [`i2c_target`].
//!
//! Two peers share the mailbox protocol of the [`protolink_i2c`] crate. The
//! controller owns the bus and polls; the target answers. Which side is the
//! RPC client and which the server is independent of that choice.

mod controller;
mod target;

pub use controller::{
    AnyPeers, BuildError, Controller, ControllerBuilder, ControllerError, ControllerRunner,
    ControllerState, SinglePeer, i2c_controller,
};
pub use target::{TargetBuilder, TargetError, TargetRunner, TargetState, i2c_target};
