//! Canonical Protobuf schemas for the Lua distributed-execution protocol and the native-plugin bridge, with their generated
//! Rust bindings.
//!
//! The schemas live under `proto/` in this crate, so the published package carries them and no consumer reaches outside
//! its own directory. Each protocol has its own feature, so a consumer compiles only the bindings it speaks:
//!
//! - `execution`: `lua.execution.v1` messages, exposed as [`execution::wire::v1`].
//! - `plugin`: `lua.plugin.v1` messages and the Protolink bridge service, exposed as [`plugin::proto`].
//!
//! The crate is `no_std` and needs `alloc`. See `README.md` for the compatibility rules that govern these schemas.
#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

extern crate alloc;

/// Bindings for the `lua.execution.v1` schema.
#[cfg(feature = "execution")]
pub mod execution {
    /// Generated messages from `proto/lua/execution/v1/execution.proto`.
    pub mod wire {
        #![allow(
            missing_docs,
            dead_code,
            non_camel_case_types,
            non_snake_case,
            non_upper_case_globals,
            clippy::all,
            unused_imports,
            unused_mut,
            unused_variables,
            unused_labels,
            unused_parens
        )]
        include!(concat!(env!("OUT_DIR"), "/execution_messages.rs"));

        /// The `lua.execution.v1` messages under their Rust names.
        pub use self::lua_::execution_::v1_ as v1;
    }
}

/// Bindings for the `lua.plugin.v1` schema.
#[cfg(feature = "plugin")]
pub mod plugin {
    /// Generated `lua.plugin.v1` messages and bridge service bindings from `proto/lua/plugin/v1/plugin.proto`.
    #[allow(
        missing_docs,
        clippy::all,
        nonstandard_style,
        unused,
        irrefutable_let_patterns
    )]
    pub mod proto {
        include!(concat!(env!("OUT_DIR"), "/plugin_messages.rs"));
        include!(concat!(env!("OUT_DIR"), "/plugin_grpc.rs"));
    }
}
