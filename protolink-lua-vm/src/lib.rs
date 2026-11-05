//! The reduced remote Lua VM: a bounded interpreter over verified `protolink_lua_bytecode` artifacts.
//!
//! One [`Machine`] runs one root execution on one node. Execution is driven by the embedding: [`Machine::run`] executes
//! a fuel slice and reports how it stopped. A native, spawn or await effect suspends the machine until the embedding
//! completes it with [`Machine::complete`]. A yield suspends it until [`Machine::resume_yield`]. The VM never blocks,
//! never recurses on the Rust stack, and never reads a wall clock.
//!
//! # Scope and status
//!
//! This is the implemented subset of `lua54-bounded-v1`. Tables, varargs, local coroutines, metatables and the standard
//! library are not implemented, and the compiler rejects them. Memory is a bounded arena freed when the machine is
//! dropped. There is no collector yet, so a long loop that allocates can exhaust its object budget. That is reported as
//! a capacity failure, not as undefined behaviour. The VM has not been measured on target hardware (not MCU-fit
//! validated).
#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

extern crate alloc;

mod arith;
mod heap;
mod machine;
mod number;

#[cfg(test)]
mod tests;

pub use machine::{
    ControlError, Effect, EffectResult, Machine, Program, StartError, Step, VmLimits,
};
