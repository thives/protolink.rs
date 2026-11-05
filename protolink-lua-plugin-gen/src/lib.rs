//! Host-only generator for Lua plugin capability contracts.
//!
//! The generic core parses a versioned capability manifest, its imported child contracts and the native modules it takes
//! from them, validates them, computes a canonical digest, and emits three things into a consumer's own output
//! directory: Rust traits, LuaLS definitions, and checker rules for the remote-language checker. The crate contains no board, pin, role or HAL knowledge. A consumer
//! supplies its manifests and its language profile. Nothing here is linked into firmware.
//!
//! Diagnostics carry a stable code, the file, and a message. Identity is content, not authority: a digest says what a
//! contract says, not that a peer may use it.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod canonical;
mod diag;
mod lock;
mod model;
mod node_config;
mod output;
mod profile;
mod resolve;
mod rules;
mod schema;

pub use canonical::{CANONICAL_VERSION, DIGEST_DOMAIN};
pub use diag::{Code, Diagnostic};
pub use lock::{Lock, LockEntry};
pub use model::{
    ChildSlot, Contract, Enumeration, Execution, Limits, Module, Operation, Param, Resources, Type,
    Variant,
};
pub use node_config::node_module_source;
pub use output::{lua_definitions, rust_bindings};
pub use profile::{NativeAbi, Profile};
pub use resolve::{Graph, MIN_ANSWER_BYTES, Resolver, min_request_bytes};
pub use rules::{CheckerRules, ChildRules, EnumRules, ModuleRules, OperationRules};
