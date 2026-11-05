//! The resolved contract model. Every output and the checker rules are derived from this one model.

use protolink_lua_bytecode::{MAX_NATIVE_ARGS, MAX_NATIVE_RESULTS, NativeRef};

/// A value type of a parameter or result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    /// `nil`.
    Nil,
    /// A boolean.
    Boolean,
    /// A signed 64-bit integer.
    Integer,
    /// A finite binary64 float.
    Float,
    /// A byte string no longer than `max_bytes`.
    Bytes {
        /// Declared byte bound.
        max_bytes: u32,
    },
    /// A value of an enumeration declared in a module, named `module.Enum`.
    Enum {
        /// Qualified enumeration name.
        reference: String,
    },
}

/// Whether an operation completes synchronously or through a future.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Execution {
    /// Returns without waiting. It must be bounded and non-blocking.
    Sync,
    /// Returns a future that the provider completes.
    Async,
}

/// A named parameter or result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Param {
    /// Parameter name.
    pub name: String,
    /// Value type.
    pub ty: Type,
}

/// One native operation of a module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    /// Operation name.
    pub name: String,
    /// Stable identifier, unique within the module.
    pub id: u32,
    /// Synchronous or asynchronous.
    pub execution: Execution,
    /// Ordered parameters.
    pub params: Vec<Param>,
    /// Ordered results.
    pub results: Vec<Param>,
    /// Names of the module errors this operation may return.
    pub errors: Vec<String>,
    /// Privileges the caller must hold, sorted.
    pub requires: Vec<String>,
}

/// One variant of an enumeration, exposed to Lua as its symbol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Variant {
    /// Variant name.
    pub name: String,
    /// Stable identifier, unique within the enumeration.
    pub id: u32,
    /// The Lua string value.
    pub symbol: String,
}

/// An enumeration declared by a module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enumeration {
    /// Enumeration name.
    pub name: String,
    /// Stable identifier, unique within the module.
    pub id: u32,
    /// Variants, sorted by id.
    pub variants: Vec<Variant>,
}

/// A native module of a contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Module {
    /// Module name, used as a Lua global.
    pub name: String,
    /// Stable identifier, unique within the contract.
    pub id: u32,
    /// Declared errors as `(name, id)`, sorted by id.
    pub errors: Vec<(String, u32)>,
    /// Enumerations, sorted by id.
    pub enums: Vec<Enumeration>,
    /// Operations, sorted by id.
    pub operations: Vec<Operation>,
}

/// A child execution slot. It exposes spawn to Lua and requires a child backend in firmware.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildSlot {
    /// Slot name, used as a Lua global such as `remote`.
    pub name: String,
    /// Stable identifier, unique within the contract.
    pub id: u32,
    /// Compile-time address of the remote node on this slot's link, unique among the contract's slots.
    pub index: u8,
    /// Identifier of the child contract. Names need not be unique across contracts, so the id is the reference.
    pub child: String,
    /// Digest of the child contract, which the parent records.
    pub child_digest: [u8; 32],
    /// Intrinsic the slot is bound to.
    pub intrinsic: String,
    /// Privileges the child needs, sorted.
    pub requires: Vec<String>,
    /// Privileges the child may delegate further, sorted.
    pub delegates: Vec<String>,
    /// Largest capture snapshot in bytes.
    pub max_capture_bytes: u32,
    /// Largest child result in bytes.
    pub max_result_bytes: u32,
}

/// Bounds of a contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Largest byte string.
    pub max_string_bytes: u32,
    /// Largest tuple.
    pub max_tuple_items: u32,
    /// Largest capture snapshot.
    pub max_capture_bytes: u32,
    /// Largest result.
    pub max_result_bytes: u32,
    /// Deepest child nesting.
    pub max_spawn_depth: u32,
}

/// The compile-time resource envelope of a remote node. See `RawResources`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resources {
    /// Largest encoded artifact, including nested children.
    pub max_artifact_bytes: u32,
    /// Largest upload chunk.
    pub max_chunk_bytes: u32,
    /// Most uploads open at once.
    pub max_uploads: u32,
    /// Most sessions the ledger tracks.
    pub max_sessions: u32,
    /// Most executions retained at once.
    pub max_executions: u32,
    /// Largest ledger answer kept for replay.
    pub max_answer_bytes: u32,
    /// Fuel per scheduler slice.
    pub slice_fuel: u32,
    /// Total instruction budget of one execution.
    pub total_fuel: u64,
    /// Deepest call nesting.
    pub max_frames: u32,
    /// Most heap objects of one execution.
    pub max_objects: u32,
    /// Largest heap footprint of one execution, in bytes.
    pub max_heap_bytes: u32,
    /// Idle time after which an abandoned session is discarded, in milliseconds.
    pub session_idle_ms: u32,
    /// Most distinct artifacts the node stores. Must exceed `max_executions`, so that a commit can always evict an
    /// artifact that no execution runs.
    pub max_artifacts: u32,
    /// Width of the session retirement window, in ids. At least `max_sessions`.
    pub retired_window: u32,
    /// Most child handles one execution keeps, settled ones included.
    pub max_child_tasks: u32,
    /// Deadline of one child operation, in milliseconds.
    pub call_timeout_ms: u32,
    /// Deadline of the cleanup that cancellation, expiry or a parent's exit starts, in milliseconds.
    pub cleanup_ms: u32,
    /// Largest request the node accepts, in encoded bytes. Large enough for the largest request the envelope allows.
    pub max_request_bytes: u32,
}

/// A resolved and validated contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Contract {
    /// Human-readable contract name. Two contracts may share one.
    pub name: String,
    /// Persistent contract identity, a UUID string.
    pub id: String,
    /// Contract version.
    pub version: String,
    /// Name of the language profile the contract selects.
    pub language_profile: String,
    /// Version of the selected profile.
    pub profile_version: String,
    /// Semantic fingerprint of the selected profile. Part of the digest, so a policy change is a new contract.
    pub profile_fingerprint: [u8; 32],
    /// Name of the remote data profile.
    pub remote_data_profile: String,
    /// Privileges this contract declares, sorted.
    pub privileges: Vec<String>,
    /// Bounds.
    pub limits: Limits,
    /// Native modules, sorted by id.
    pub modules: Vec<Module>,
    /// Child slots, sorted by id.
    pub children: Vec<ChildSlot>,
    /// Compile-time resource envelope, when the contract runs on a remote node.
    pub resources: Option<Resources>,
    /// Digest of the canonical contract. Identity by content.
    pub digest: [u8; 32],
}

impl Contract {
    /// Finds a module by name.
    pub fn module(&self, name: &str) -> Option<&Module> {
        self.modules.iter().find(|m| m.name == name)
    }

    /// The destination's native operation table: every operation of every module that an artifact can reference, as the
    /// compiler binds it. A node admits an artifact only when each of its native references equals one of these entries
    /// in all four fields.
    ///
    /// An operation whose signature the artifact ABI cannot carry (more than [`MAX_NATIVE_ARGS`] parameters, or more than
    /// [`MAX_NATIVE_RESULTS`] results) has no entry: no artifact can reference it, and the compiler refuses a call to it.
    /// Its full signature still stays in the contract, because a consumer with another native ABI may carry it. Identifiers
    /// are checked to `1..=65535` when the contract is resolved, so the conversions below cannot truncate.
    pub fn native_operations(&self) -> Vec<NativeRef> {
        self.modules
            .iter()
            .flat_map(|module| {
                module.operations.iter().filter_map(move |operation| {
                    let argc = u8::try_from(operation.params.len()).ok()?;
                    let results = u8::try_from(operation.results.len()).ok()?;
                    if argc > MAX_NATIVE_ARGS || results > MAX_NATIVE_RESULTS {
                        return None;
                    }
                    Some(NativeRef {
                        module: u16::try_from(module.id)
                            .expect("module ids are resolved to 1..=65535"),
                        operation: u16::try_from(operation.id)
                            .expect("operation ids are resolved to 1..=65535"),
                        argc,
                        results,
                    })
                })
            })
            .collect()
    }

    /// Finds a child slot by name.
    pub fn child(&self, name: &str) -> Option<&ChildSlot> {
        self.children.iter().find(|c| c.name == name)
    }
}
