use std::env;
use std::path::{Path, PathBuf};

/// Generates the bindings for the enabled schemas from the proto files in this crate.
///
/// Each schema is parsed once by protolink-grpc-gen, and micropb generates its messages from that parse, so the messages
/// and any service glue cannot disagree. Only the schemas whose feature is enabled are generated, so an execution-only
/// consumer never compiles the plugin bridge.
fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let proto_dir = manifest_dir.join("proto");

    if env::var_os("CARGO_FEATURE_EXECUTION").is_some() {
        generate_execution(&proto_dir, &out_dir);
    }
    if env::var_os("CARGO_FEATURE_PLUGIN").is_some() {
        generate_plugin(&proto_dir, &out_dir);
    }

    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed={}", proto_dir.display());
}

/// Writes the `lua.execution.v1` messages to `execution_messages.rs`.
fn generate_execution(proto_dir: &Path, out_dir: &Path) {
    let schema = proto_dir.join("lua/execution/v1/execution.proto");
    let fdset = out_dir.join("execution_fdset.bin");

    protolink_grpc_gen::Generator::micropb()
        .with_include(proto_dir)
        .with_server(false)
        .with_client(false)
        .with_blocking_client(false)
        .with_file_descriptor_set_path(&fdset)
        .compile_protos(
            std::slice::from_ref(&schema),
            out_dir.join("execution_service.rs"),
        )
        .expect("failed to parse the lua.execution.v1 schema");

    let mut messages = micropb_gen::Generator::new();
    messages.use_container_alloc();
    messages
        .compile_fdset_file(&fdset, out_dir.join("execution_messages.rs"))
        .expect("failed to generate the lua.execution.v1 messages");
}

/// Writes the `lua.plugin.v1` messages to `plugin_messages.rs` and the bridge service glue to `plugin_grpc.rs`.
fn generate_plugin(proto_dir: &Path, out_dir: &Path) {
    let service = proto_dir.join("lua/plugin/v1/plugin.proto");
    let fdset = out_dir.join("plugin_fdset.bin");

    protolink_grpc_gen::Generator::micropb()
        .with_include(proto_dir)
        .with_blocking_client(false)
        .with_file_descriptor_set_path(&fdset)
        .compile_protos(&[service], out_dir.join("plugin_grpc.rs"))
        .expect("failed to generate the plugin bridge service bindings");

    let mut messages = micropb_gen::Generator::new();
    messages.use_container_alloc();
    messages
        .compile_fdset_file(&fdset, out_dir.join("plugin_messages.rs"))
        .expect("failed to generate the plugin bridge messages");
}
