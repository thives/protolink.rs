use std::{env, path::PathBuf};

fn enabled(feature: &str) -> bool {
    env::var_os(format!("CARGO_FEATURE_{feature}")).is_some()
}

fn main() {
    let protos = ["proto/api.proto", "proto/second.proto"];
    let mut grpc = protolink_grpc_gen::Generator::new();
    grpc.runtime_path("::protolink_grpc")
        .server(enabled("SERVER"))
        .client(enabled("ASYNC_CLIENT"))
        .blocking_client(enabled("BLOCKING_CLIENT"));
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=proto/common.proto");
    println!("cargo:rerun-if-changed=proto/other.proto");
    grpc.include("proto");

    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let mut config = prost_build::Config::new();
    // `no_std` consumers cannot use the default `HashMap`.
    config.btree_map(["."]).out_dir(&out);
    grpc.compile_protos_with_prost(&protos, &mut config)
        .unwrap();

    escaped_override(&grpc, &out);
}

/// Second pass with a directly configured service generator (the convenience
/// API above replaces it): a keyword module override, from a hand-built
/// descriptor set so no `protoc` is involved.
fn escaped_override(grpc: &protolink_grpc_gen::Generator, out: &std::path::Path) {
    use prost_types::{
        DescriptorProto, FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto,
        ServiceDescriptorProto,
    };

    let message = ".escaped.Msg".to_owned();
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("escaped.proto".into()),
            package: Some("escaped".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Msg".into()),
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("Probe".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("Call".into()),
                    input_type: Some(message.clone()),
                    output_type: Some(message),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let mut service = grpc.prost_service_generator();
    service.service_module("escaped.Probe", "type");
    let errors = service.error_log();
    let dir = out.join("escaped");
    std::fs::create_dir_all(&dir).unwrap();
    let mut config = prost_build::Config::new();
    config
        .btree_map(["."])
        .out_dir(&dir)
        .service_generator(Box::new(service));
    config.compile_fds(fds).unwrap();
    let errors = errors.take();
    assert!(errors.is_empty(), "{errors:?}");
}
