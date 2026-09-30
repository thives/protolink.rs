use std::path::PathBuf;

fn main() {
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let proto = "proto/device.proto";
    println!("cargo:rerun-if-changed={proto}");

    // One pure-Rust parse feeds both generators; no protoc required.
    let fdset = out.join("device.fdset");
    let mut grpc = protolink_grpc_gen::Generator::new();
    grpc.file_descriptor_set_path(&fdset);
    grpc.compile_protos(&[proto], out.join("device_grpc.rs"))
        .unwrap();

    let mut messages = micropb_gen::Generator::new();
    messages.use_container_alloc();
    messages
        .compile_fdset_file(&fdset, out.join("device.rs"))
        .unwrap();
}
