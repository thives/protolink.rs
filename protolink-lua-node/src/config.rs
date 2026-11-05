//! The node's compile-time configuration and its validation.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use protolink_lua::frame;
use protolink_lua::ledger::{LedgerConfigError, MAX_MUTATION_ANSWER_BYTES};
use protolink_lua_bytecode::{
    Limits as BytecodeLimits, MAX_NATIVE_ARGS, MAX_NATIVE_RESULTS, NativeRef,
};
use protolink_lua_vm::VmLimits;

/// Bounds a parent applies to one child slot it spawns through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChildBounds {
    /// Largest payload of the captures sent to the child, in bytes.
    pub max_capture_bytes: usize,
    /// Largest payload of the result the child may return, in bytes.
    pub max_result_bytes: usize,
}

/// One child slot this node may spawn through. A spawn is routed by the slot id its artifact edge carries, never by the
/// child's contract digest, so two slots may run the same contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChildRoute {
    /// Stable child-slot id from the contract.
    pub slot: u32,
    /// Compile-time address of the child on its link.
    pub index: u8,
    /// Digest of the contract the child serves. A child artifact compiled for another contract is refused at `Start`.
    pub contract_digest: [u8; 32],
    /// Bounds of what crosses this slot.
    pub bounds: ChildBounds,
}

/// Bounds and identity of a node. The embedding builds it from the generated constants of the node's contract, so every
/// value is fixed at compile time. [`NodeConfig::validate`] checks that the bounds can work together.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Name of the contract the node serves, for display. Names are not unique, so admission uses the digest.
    pub contract: String,
    /// Digest of the contract the node serves. The node admits an artifact only when its header carries this digest.
    pub contract_digest: [u8; 32],
    /// Incarnation identity. The embedding supplies a fresh value on every boot, from a hardware random source when the
    /// chip has one. A request carrying another epoch is stale and is refused.
    pub epoch: u64,
    /// Bounds of the bytecode the node accepts. Build them with [`BytecodeLimits::for_destination`], the translation
    /// the compiler uses.
    pub bytecode: BytecodeLimits,
    /// The destination's native operation table: module, operation, argument count and result count of every operation
    /// the contract declares. The generator emits it from the same resolved contract the compiler binds against. An
    /// uploaded artifact is admitted only if each of its native references equals one of these entries. Matching the
    /// table grants no permission; the platform still decides whether an operation may run.
    pub operations: Vec<NativeRef>,
    /// Bounds of each machine.
    pub vm: VmLimits,
    /// Fuel per scheduler slice.
    pub slice_fuel: u32,
    /// Largest ledger answer kept for replay. At least [`MAX_MUTATION_ANSWER_BYTES`].
    pub max_answer_bytes: usize,
    /// Most sessions the ledger tracks.
    pub max_sessions: usize,
    /// Most uploads open at once.
    pub max_uploads: usize,
    /// Most executions held at once, unresolved records included. A released execution no longer counts.
    pub max_executions: usize,
    /// Largest accepted upload chunk.
    pub max_chunk_bytes: u32,
    /// Idle time after which a session is abandoned, in milliseconds. Its staged upload is discarded, and its execution
    /// is cancelled through the same cleanup as `Cancel`. A read counts as activity.
    pub session_idle_ms: u32,
    /// Largest payload of the captures this node accepts in `Start`, in bytes.
    pub max_capture_bytes: usize,
    /// Most distinct artifacts the node stores. Must exceed `max_executions`, so a commit can always evict an artifact
    /// that no running execution uses.
    pub max_artifacts: usize,
    /// Width of the session retirement window, in ids. At least `max_sessions`.
    pub retired_window: u64,
    /// Most child handles one execution keeps, settled ones included. A spawn beyond it fails in Lua.
    pub max_child_tasks: usize,
    /// Deadline of one child operation, in milliseconds of the node's clock.
    pub call_timeout_ms: u32,
    /// Deadline of the cleanup that cancellation, expiry or an execution's exit starts, in milliseconds.
    pub cleanup_ms: u32,
    /// Largest request the node accepts, in encoded bytes. Larger requests are refused before they are decoded.
    pub max_request_bytes: usize,
    /// The child slots this node routes, at most one route per slot id.
    pub routes: Vec<ChildRoute>,
}

/// Why a configuration cannot run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// The ledger bounds are unusable.
    Ledger(LedgerConfigError),
    /// A budget is zero.
    Zero(&'static str),
    /// The artifact store cannot always evict an artifact no execution runs.
    ArtifactStore,
    /// The retirement window does not cover every live session.
    WindowTooSmall,
    /// A full request of these limits does not fit the request bound.
    RequestBoundTooSmall {
        /// The smallest acceptable bound.
        required: usize,
    },
    /// Two routes share a slot id.
    DuplicateRoute(u32),
    /// An upload chunk cannot fit an artifact.
    ChunkTooLarge,
    /// Two operations share a module and operation id, so the table would be ambiguous.
    DuplicateOperation {
        /// Module id.
        module: u16,
        /// Operation id.
        operation: u16,
    },
    /// An operation declares more arguments or results than the artifact ABI allows.
    OperationShape {
        /// Module id.
        module: u16,
        /// Operation id.
        operation: u16,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ledger(e) => write!(f, "ledger bounds are unusable: {e:?}"),
            Self::Zero(what) => write!(f, "{what} must be greater than zero"),
            Self::ArtifactStore => f.write_str("max_artifacts must exceed max_executions"),
            Self::WindowTooSmall => f.write_str("retired_window must be at least max_sessions"),
            Self::RequestBoundTooSmall { required } => {
                write!(f, "max_request_bytes must be at least {required}")
            }
            Self::DuplicateRoute(slot) => write!(f, "slot {slot} has two routes"),
            Self::ChunkTooLarge => f.write_str("max_chunk_bytes cannot exceed max_artifact_bytes"),
            Self::DuplicateOperation { module, operation } => {
                write!(f, "module {module} operation {operation} is declared twice")
            }
            Self::OperationShape { module, operation } => write!(
                f,
                "module {module} operation {operation} declares a signature the artifact ABI does not allow"
            ),
        }
    }
}

impl NodeConfig {
    /// Checks that the bounds can work together. A node is never built from a configuration that fails this.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (what, value) in [
            ("slice_fuel", self.slice_fuel as usize),
            ("max_sessions", self.max_sessions),
            ("max_uploads", self.max_uploads),
            ("max_executions", self.max_executions),
            ("max_chunk_bytes", self.max_chunk_bytes as usize),
            ("session_idle_ms", self.session_idle_ms as usize),
            ("max_child_tasks", self.max_child_tasks),
            ("call_timeout_ms", self.call_timeout_ms as usize),
            ("cleanup_ms", self.cleanup_ms as usize),
        ] {
            if value == 0 {
                return Err(ConfigError::Zero(what));
            }
        }
        if self.max_answer_bytes < MAX_MUTATION_ANSWER_BYTES {
            return Err(ConfigError::Ledger(
                LedgerConfigError::AnswerBoundTooSmall {
                    required: MAX_MUTATION_ANSWER_BYTES,
                },
            ));
        }
        if self.max_artifacts <= self.max_executions {
            return Err(ConfigError::ArtifactStore);
        }
        if self.retired_window < self.max_sessions as u64 {
            return Err(ConfigError::WindowTooSmall);
        }
        if self.max_chunk_bytes as usize > self.bytecode.max_artifact_bytes {
            return Err(ConfigError::ChunkTooLarge);
        }
        let required = frame::min_request_bytes(
            self.vm.wire.max_items(),
            self.vm.wire.max_total_bytes(),
            self.max_chunk_bytes as usize,
        );
        if self.max_request_bytes < required {
            return Err(ConfigError::RequestBoundTooSmall { required });
        }
        for (index, route) in self.routes.iter().enumerate() {
            if self.routes[..index].iter().any(|r| r.slot == route.slot) {
                return Err(ConfigError::DuplicateRoute(route.slot));
            }
        }
        for (index, native) in self.operations.iter().enumerate() {
            if self.operations[..index]
                .iter()
                .any(|n| n.module == native.module && n.operation == native.operation)
            {
                return Err(ConfigError::DuplicateOperation {
                    module: native.module,
                    operation: native.operation,
                });
            }
            if native.argc > MAX_NATIVE_ARGS || native.results > MAX_NATIVE_RESULTS {
                return Err(ConfigError::OperationShape {
                    module: native.module,
                    operation: native.operation,
                });
            }
        }
        Ok(())
    }

    /// The route of child slot `slot`.
    pub fn route(&self, slot: u32) -> Option<&ChildRoute> {
        self.routes.iter().find(|route| route.slot == slot)
    }
}
