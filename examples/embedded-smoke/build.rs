use std::path::PathBuf;

fn main() {
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let proto = "proto/smoke.proto";
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=link.x");

    // Use the same host-only, pure-Rust parse as embedded-device: no protoc.
    let fdset = out.join("smoke.fdset");
    let mut grpc = protolink_grpc_gen::Generator::new();
    grpc.file_descriptor_set_path(&fdset)
        .client(std::env::var_os("CARGO_FEATURE_ASYNC").is_some())
        .blocking_client(std::env::var_os("CARGO_FEATURE_BLOCKING").is_some());
    grpc.compile_protos(&[proto], out.join("smoke_grpc.rs"))
        .unwrap();
    let mut messages = micropb_gen::Generator::new();
    messages.use_container_alloc();
    messages
        .compile_fdset_file(&fdset, out.join("smoke.rs"))
        .unwrap();

    // Cargo --manifest-path does not load a nested .cargo/config.toml from the
    // repository root. Supply binary-only linker arguments here instead.
    if std::env::var("TARGET").unwrap() == "thumbv6m-none-eabi" {
        std::fs::copy("link.x", out.join("link.x")).unwrap();
        println!("cargo:rustc-link-search={}", out.display());
        println!("cargo:rustc-link-arg-bin=embedded-smoke=-Tlink.x");
        println!("cargo:rustc-link-arg-bin=embedded-smoke=--nmagic");
    }
}
