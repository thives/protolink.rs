//! The node configuration that a built node embeds: its contract identity, its resource envelope and its child routes.
//!
//! A device build calls [`node_module_source`] from its build script and includes the result. The same mapping is the one
//! the central host uses, so a device admits exactly the artifacts the central compiles for it.

use std::fmt::Write;

use crate::model::Contract;

/// Rust source of a module with the contract's identity and the node configuration built from its manifest.
///
/// The module defines `CONTRACT_NAME`, `CONTRACT_DIGEST`, `PRIVILEGES`, `MAX_ARTIFACT_BYTES`, `MAX_REQUEST_BYTES` and
/// `node_config(epoch)`. The configuration carries the contract's native operation table, so the node admits only
/// artifacts whose native references the contract declares. Each child slot of the contract becomes a route, so the node
/// admits spawns on that slot.
///
/// # Panics
///
/// Panics when the contract declares no resource envelope. The generator refuses such a contract before this runs.
pub fn node_module_source(contract: &Contract) -> String {
    let resources = contract
        .resources
        .as_ref()
        .expect("the contract declares a resource envelope");
    let limits = &contract.limits;
    let mut out = String::new();
    writeln!(out, "pub const CONTRACT_NAME: &str = {:?};", contract.name).unwrap();
    writeln!(
        out,
        "pub const CONTRACT_DIGEST: [u8; 32] = {:?};",
        contract.digest
    )
    .unwrap();
    writeln!(
        out,
        "pub const PRIVILEGES: &[&str] = &{:?};",
        contract.privileges
    )
    .unwrap();
    writeln!(
        out,
        "pub const MAX_ARTIFACT_BYTES: u32 = {}u32;",
        resources.max_artifact_bytes
    )
    .unwrap();
    writeln!(
        out,
        "pub const MAX_REQUEST_BYTES: usize = {}usize;",
        resources.max_request_bytes
    )
    .unwrap();
    writeln!(
        out,
        "pub fn node_config(epoch: u64) -> protolink_lua_node::NodeConfig {{"
    )
    .unwrap();
    writeln!(out, "    protolink_lua_node::NodeConfig {{").unwrap();
    writeln!(
        out,
        "        contract: alloc::string::String::from(CONTRACT_NAME),"
    )
    .unwrap();
    writeln!(out, "        contract_digest: CONTRACT_DIGEST,").unwrap();
    writeln!(out, "        epoch,").unwrap();
    writeln!(
        out,
        "        bytecode: protolink_lua_bytecode::Limits::for_destination({}usize, {}u32),",
        resources.max_artifact_bytes, limits.max_spawn_depth
    )
    .unwrap();
    writeln!(out, "        vm: protolink_lua_vm::VmLimits {{").unwrap();
    writeln!(
        out,
        "            max_frames: {}usize,",
        resources.max_frames
    )
    .unwrap();
    writeln!(
        out,
        "            max_objects: {}usize,",
        resources.max_objects
    )
    .unwrap();
    writeln!(
        out,
        "            max_bytes: {}usize,",
        resources.max_heap_bytes
    )
    .unwrap();
    writeln!(
        out,
        "            wire: protolink_lua::value::Limits::new({}usize, {}usize, {}usize),",
        limits.max_tuple_items, limits.max_string_bytes, limits.max_result_bytes
    )
    .unwrap();
    writeln!(out, "            total_fuel: {}u64,", resources.total_fuel).unwrap();
    writeln!(out, "        }},").unwrap();
    writeln!(out, "        slice_fuel: {}u32,", resources.slice_fuel).unwrap();
    writeln!(
        out,
        "        max_answer_bytes: {}usize,",
        resources.max_answer_bytes
    )
    .unwrap();
    writeln!(
        out,
        "        max_sessions: {}usize,",
        resources.max_sessions
    )
    .unwrap();
    writeln!(out, "        max_uploads: {}usize,", resources.max_uploads).unwrap();
    writeln!(
        out,
        "        max_executions: {}usize,",
        resources.max_executions
    )
    .unwrap();
    writeln!(
        out,
        "        max_chunk_bytes: {}u32,",
        resources.max_chunk_bytes
    )
    .unwrap();
    writeln!(
        out,
        "        session_idle_ms: {}u32,",
        resources.session_idle_ms
    )
    .unwrap();
    writeln!(
        out,
        "        max_capture_bytes: {}usize,",
        limits.max_capture_bytes
    )
    .unwrap();
    writeln!(
        out,
        "        max_artifacts: {}usize,",
        resources.max_artifacts
    )
    .unwrap();
    writeln!(
        out,
        "        retired_window: {}u64,",
        resources.retired_window
    )
    .unwrap();
    writeln!(
        out,
        "        max_child_tasks: {}usize,",
        resources.max_child_tasks
    )
    .unwrap();
    writeln!(
        out,
        "        call_timeout_ms: {}u32,",
        resources.call_timeout_ms
    )
    .unwrap();
    writeln!(out, "        cleanup_ms: {}u32,", resources.cleanup_ms).unwrap();
    writeln!(
        out,
        "        max_request_bytes: {}usize,",
        resources.max_request_bytes
    )
    .unwrap();
    // The destination's operation table: the compiler's bindings, admitted exactly. Its entries are the same values the
    // compiler reads from `Contract::native_operations`, so an artifact the compiler emits is admitted by this node.
    writeln!(out, "        operations: alloc::vec![").unwrap();
    for native in contract.native_operations() {
        writeln!(
            out,
            "            protolink_lua_bytecode::NativeRef {{ module: {}u16, operation: {}u16, argc: {}u8, results: {}u8 }},",
            native.module, native.operation, native.argc, native.results
        )
        .unwrap();
    }
    writeln!(out, "        ],").unwrap();
    writeln!(out, "        routes: alloc::vec![").unwrap();
    for slot in &contract.children {
        writeln!(out, "            protolink_lua_node::ChildRoute {{").unwrap();
        writeln!(out, "                slot: {}u32,", slot.id).unwrap();
        writeln!(out, "                index: {}u8,", slot.index).unwrap();
        writeln!(
            out,
            "                contract_digest: {:?},",
            slot.child_digest
        )
        .unwrap();
        writeln!(
            out,
            "                bounds: protolink_lua_node::ChildBounds {{"
        )
        .unwrap();
        writeln!(
            out,
            "                    max_capture_bytes: {}usize,",
            slot.max_capture_bytes
        )
        .unwrap();
        writeln!(
            out,
            "                    max_result_bytes: {}usize,",
            slot.max_result_bytes
        )
        .unwrap();
        writeln!(out, "                }},").unwrap();
        writeln!(out, "            }},").unwrap();
    }
    writeln!(out, "        ],").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    out
}
