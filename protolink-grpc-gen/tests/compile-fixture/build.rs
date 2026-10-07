use std::{
    env,
    path::{Path, PathBuf},
};

fn enabled(feature: &str) -> bool {
    env::var_os(format!("CARGO_FEATURE_{feature}")).is_some()
}

/// Generate bindings and messages for `proto/{name}.proto` into
/// `{name}_grpc.rs` and `{name}_messages.rs` through the descriptor-set
/// pipeline, honoring every generation mode of the fixture.
fn generate(out: &Path, name: &str, messages_path: &str) {
    let proto = format!("proto/{name}.proto");
    println!("cargo:rerun-if-changed={proto}");
    let suffixed = !enabled("UNSUFFIXED_PACKAGES");
    let fdset = out.join(format!("{name}.fdset"));
    let mut grpc = protolink_grpc_gen::Generator::new();
    grpc.runtime_path("::protolink_grpc")
        .suffixed_package_names(suffixed)
        .messages_path(messages_path)
        .server(enabled("SERVER"))
        .client(enabled("ASYNC_CLIENT"))
        .blocking_client(enabled("BLOCKING_CLIENT"))
        .file_descriptor_set_path(&fdset);
    grpc.compile_protos(&[&proto], out.join(format!("{name}_grpc.rs")))
        .unwrap();
    let mut messages = micropb_gen::Generator::new();
    messages.use_container_alloc();
    messages.suffixed_package_names(suffixed);
    messages
        .compile_fdset_file(&fdset, out.join(format!("{name}_messages.rs")))
        .unwrap();
}

fn main() {
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let suffixed = !enabled("UNSUFFIXED_PACKAGES");
    let mut grpc = protolink_grpc_gen::Generator::new();
    grpc.runtime_path("::protolink_grpc")
        .suffixed_package_names(suffixed)
        .server(enabled("SERVER"))
        .client(enabled("ASYNC_CLIENT"))
        .blocking_client(enabled("BLOCKING_CLIENT"));

    let protos = [
        PathBuf::from("proto/shapes.proto"),
        PathBuf::from("proto/alpha.proto"),
        PathBuf::from("proto/beta.proto"),
    ];
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
        grpc.include(proto.parent().unwrap());
    }
    let fdset = out.join("messages.fdset");
    grpc.file_descriptor_set_path(&fdset);
    grpc.compile_protos(&protos, out.join("grpc.rs")).unwrap();

    // Same descriptor-set API as examples/embedded-device/build.rs.
    let mut messages = micropb_gen::Generator::new();
    messages.use_container_alloc();
    messages.suffixed_package_names(suffixed);
    messages
        .compile_fdset_file(&fdset, out.join("messages.rs"))
        .unwrap();

    // Built-in client names remain legal on server traits, in every build mode.
    println!("cargo:rerun-if-changed=proto/lifecycle.proto");
    let lifecycle_fdset = out.join("lifecycle.fdset");
    let mut lifecycle = protolink_grpc_gen::Generator::new();
    lifecycle
        .runtime_path("::protolink_grpc")
        .suffixed_package_names(suffixed)
        .client(false)
        .blocking_client(false)
        .file_descriptor_set_path(&lifecycle_fdset);
    lifecycle
        .compile_protos(&["proto/lifecycle.proto"], out.join("lifecycle_grpc.rs"))
        .unwrap();
    let mut lifecycle_messages = micropb_gen::Generator::new();
    lifecycle_messages.use_container_alloc();
    lifecycle_messages.suffixed_package_names(suffixed);
    lifecycle_messages
        .compile_fdset_file(&lifecycle_fdset, out.join("lifecycle_messages.rs"))
        .unwrap();

    // Services named like the runtime types the bindings refer to.
    generate(&out, "names", "");
    // Service modules that collide with message modules.
    generate(&out, "collide", "");
    // Messages in a module whose name merely starts with `crate`.
    generate(&out, "relative", "crate_messages");
}
