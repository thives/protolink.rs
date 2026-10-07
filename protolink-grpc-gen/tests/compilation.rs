use std::{env, path::PathBuf, process::Command};

fn check(features: Option<&str>) {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compile-fixture");
    let mut cargo = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    cargo
        .arg("check")
        .arg("--locked")
        .arg("--color=never")
        .arg("--manifest-path")
        .arg(fixture.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(fixture.join("target"))
        .env_remove("CARGO_BUILD_TARGET");
    if let Some(features) = features {
        cargo
            .arg("--no-default-features")
            .arg("--features")
            .arg(features);
    }
    if let Some(target) = env::var_os("PROTOLINK_GRPC_GEN_TEST_TARGET") {
        cargo.arg("--target").arg(target);
    }
    let output = cargo
        .output()
        .expect("run Cargo to compile generated bindings");
    let log = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "features {features:?}:\n{log}");
}

#[test]
fn generated_bindings_compile_in_no_std_consumer() {
    // Suffixed and unsuffixed packages, each in every generation mode.
    for features in [
        None,
        Some(""),
        Some("server"),
        Some("async-client"),
        Some("blocking-client"),
        Some("server,async-client,blocking-client"),
        Some("unsuffixed-packages"),
        Some("server,unsuffixed-packages"),
        Some("async-client,unsuffixed-packages"),
        Some("blocking-client,unsuffixed-packages"),
        Some("server,async-client,blocking-client,unsuffixed-packages"),
    ] {
        check(features);
    }
}
