use std::{env, fs, path::PathBuf};

fn enabled(feature: &str) -> bool {
    env::var_os(format!("CARGO_FEATURE_{feature}")).is_some()
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

    // Negative compilation cases must fail in the generator, not later in rustc.
    let invalid = if enabled("REJECT_NEW") {
        Some("message M {} service S { rpc New(M) returns (M); }")
    } else if enabled("REJECT_INTO_INNER") {
        Some("message M {} service S { rpc IntoInner(M) returns (stream M); }")
    } else if enabled("REJECT_TRANSPORT_MUT") {
        Some("message M {} service S { rpc TransportMut(stream M) returns (stream M); }")
    } else if enabled("REJECT_MODULES_NORMALIZED") {
        Some("package p; service Self {} service Self_ {}")
    } else if enabled("REJECT_MODULES_SHORT") {
        Some("service AlphaShared {}")
    } else {
        None
    };
    let mut protos = vec![
        PathBuf::from("proto/shapes.proto"),
        PathBuf::from("proto/alpha.proto"),
        PathBuf::from("proto/beta.proto"),
    ];
    if let Some(body) = invalid {
        let path = out.join("invalid.proto");
        fs::write(&path, format!("syntax = \"proto3\";\n{body}\n")).unwrap();
        protos.push(path);
    }
    if enabled("REJECT_MODULES_QUALIFIED") {
        for (filename, package) in [("dotted.proto", "a.b"), ("flat.proto", "a_b")] {
            let path = out.join(filename);
            fs::write(
                &path,
                format!("syntax = \"proto3\"; package {package}; service Shared {{}}"),
            )
            .unwrap();
            protos.push(path);
        }
    }
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
        grpc.include(proto.parent().unwrap());
    }
    let fdset = out.join("messages.fdset");
    grpc.file_descriptor_set_path(&fdset);
    grpc.compile_protos(&protos, out.join("grpc.rs"))
        .expect("service generation rejected the schema");

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
}
