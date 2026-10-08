use std::{env, path::PathBuf, process::Command};

fn check(fixture_dir: &str, features: Option<&str>) {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(fixture_dir);
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
        check("compile-fixture", features);
    }
}

#[test]
fn prost_generated_bindings_compile_in_no_std_consumer() {
    for features in [
        Some(""),
        Some("server"),
        Some("async-client"),
        Some("blocking-client"),
        Some("server,async-client,blocking-client"),
    ] {
        check("compile-fixture-prost", features);
    }
}

/// A prost-only consumer must not build micropb, in the runtime or the
/// generator.
#[test]
fn prost_only_consumer_does_not_depend_on_micropb() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compile-fixture-prost");
    let output = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args(["tree", "--locked", "--prefix", "none", "--edges", "all"])
        .arg("--manifest-path")
        .arg(fixture.join("Cargo.toml"))
        .env_remove("CARGO_BUILD_TARGET")
        .output()
        .expect("run cargo tree");
    let tree = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(tree.contains("prost"), "{tree}");
    assert!(!tree.contains("micropb"), "{tree}");
}
