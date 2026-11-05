//! The manifest and profile schemas as they appear in TOML. Unknown keys are rejected, so a typo cannot silently
//! change a contract.

use serde::Deserialize;

/// The supported manifest schema version.
pub(crate) const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawManifest {
    pub schema_version: u32,
    pub contract: RawContract,
    #[serde(default)]
    pub imports: Vec<RawImport>,
    #[serde(default)]
    pub module_imports: Vec<RawModuleImport>,
    #[serde(default)]
    pub modules: Vec<RawModule>,
    #[serde(default)]
    pub child_execution: Vec<RawChild>,
    #[serde(default)]
    pub resources: Option<RawResources>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawContract {
    pub name: String,
    pub id: String,
    pub version: String,
    pub language_profile: String,
    pub remote_data_profile: String,
    #[serde(default)]
    pub privileges: Vec<String>,
    pub limits: RawLimits,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawLimits {
    pub max_string_bytes: u32,
    pub max_tuple_items: u32,
    pub max_capture_bytes: u32,
    pub max_result_bytes: u32,
    pub max_spawn_depth: u32,
}

/// The compile-time resource envelope of a remote node. Every value is a budget the firmware enforces, so each must be
/// positive. The values are part of the contract digest: an artifact is compiled for one envelope.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawResources {
    pub max_artifact_bytes: u32,
    pub max_chunk_bytes: u32,
    pub max_uploads: u32,
    pub max_sessions: u32,
    pub max_executions: u32,
    pub max_answer_bytes: u32,
    pub slice_fuel: u32,
    pub total_fuel: u64,
    pub max_frames: u32,
    pub max_objects: u32,
    pub max_heap_bytes: u32,
    pub session_idle_ms: u32,
    pub max_artifacts: u32,
    /// Width of the session retirement window, in ids. Part of the memory accounting: `ceil(W / 8)` bytes.
    pub retired_window: u32,
    /// Most child handles one execution keeps, settled ones included.
    pub max_child_tasks: u32,
    /// Deadline of one child operation, in milliseconds.
    pub call_timeout_ms: u32,
    /// Deadline of the cleanup that cancellation, expiry or a parent's exit starts, in milliseconds.
    pub cleanup_ms: u32,
    /// Largest request the node accepts, in encoded bytes.
    pub max_request_bytes: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawImport {
    pub alias: String,
    pub path: String,
    pub contract_id: String,
    pub version: String,
}

/// A native module taken from an imported contract. `alias` names an entry of `imports`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawModuleImport {
    pub alias: String,
    pub module: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawModule {
    pub name: String,
    pub id: u32,
    #[serde(default)]
    pub errors: Vec<RawError>,
    #[serde(default)]
    pub enums: Vec<RawEnum>,
    #[serde(default)]
    pub operations: Vec<RawOperation>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawError {
    pub name: String,
    pub id: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawEnum {
    pub name: String,
    pub id: u32,
    pub variants: Vec<RawVariant>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawVariant {
    pub name: String,
    pub id: u32,
    pub symbol: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawOperation {
    pub name: String,
    pub id: u32,
    pub execution: String,
    #[serde(default)]
    pub params: Vec<RawParam>,
    #[serde(default)]
    pub results: Vec<RawParam>,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub requires: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawParam {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(default)]
    pub max_bytes: Option<u32>,
    #[serde(rename = "ref", default)]
    pub reference: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawChild {
    pub name: String,
    pub id: u32,
    /// Compile-time address of the remote node on the link this slot uses. Unique among a manifest's child slots.
    pub index: u8,
    pub child_contract: String,
    pub intrinsic: String,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub delegates: Vec<String>,
    pub max_capture_bytes: u32,
    pub max_result_bytes: u32,
}

/// A language profile: the versioned, machine-readable list of remote features a consumer supports.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawProfile {
    pub schema_version: u32,
    pub name: String,
    pub version: String,
    pub features: RawFeatures,
    /// The consumer's native-call ABI. Without it, any result count is accepted.
    #[serde(default)]
    pub native_abi: Option<RawNativeAbi>,
}

/// What a consumer's VM can carry back from a native operation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawNativeAbi {
    pub name: String,
    pub max_results: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawFeatures {
    pub allowed: Vec<String>,
}
