use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "\
usage: protolink-grpc-gen [OPTIONS] -o OUT.rs FILE.proto...

options:
  -o, --out FILE             output Rust file (required)
  -I, --include DIR          import search path (repeatable)
      --fdset FILE           also write the parsed FileDescriptorSet (for micropb-gen)
      --messages-path PATH   module path of the micropb-generated code
      --runtime-path PATH    protolink gRPC runtime path (default ::protolink::grpc)
      --no-suffixed-packages mirror micropb-gen suffixed_package_names(false)
      --protoc               parse with protoc instead of the pure-Rust parser
      --no-server            do not generate service traits / dispatchers
      --no-client            do not generate async clients
      --no-blocking-client   do not generate blocking clients";

fn main() -> ExitCode {
    let mut generator = protolink_grpc_gen::Generator::micropb();
    let mut out = None;
    let mut protos: Vec<PathBuf> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| {
            args.next().unwrap_or_else(|| {
                eprintln!("missing value for {name}\n\n{USAGE}");
                std::process::exit(2)
            })
        };
        match arg.as_str() {
            "-o" | "--out" => out = Some(PathBuf::from(value(&arg))),
            "-I" | "--include" => {
                generator = generator.with_include(value(&arg));
            }
            "--fdset" => {
                generator = generator.with_file_descriptor_set_path(value(&arg));
            }
            "--messages-path" => {
                generator = generator.with_messages_path(value(&arg));
            }
            "--runtime-path" => {
                generator = generator.with_runtime_path(value(&arg));
            }
            "--no-suffixed-packages" => {
                generator = generator.with_suffixed_package_names(false);
            }
            "--protoc" => {
                generator = generator.with_protoc(true);
            }
            "--no-server" => {
                generator = generator.with_server(false);
            }
            "--no-client" => {
                generator = generator.with_client(false);
            }
            "--no-blocking-client" => {
                generator = generator.with_blocking_client(false);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            _ => protos.push(PathBuf::from(arg)),
        }
    }
    let Some(out) = out else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    match generator.compile_protos(&protos, &out) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
