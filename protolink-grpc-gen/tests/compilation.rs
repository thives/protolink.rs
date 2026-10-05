use std::{env, path::PathBuf, process::Command};

fn check(features: Option<&str>, rejection: Option<&str>) {
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
    match rejection {
        None => assert!(output.status.success(), "features {features:?}:\n{log}"),
        Some(diagnostic) => {
            assert!(
                !output.status.success(),
                "invalid schema compiled: {features:?}"
            );
            assert!(
                log.contains("service generation rejected the schema"),
                "{log}"
            );
            assert!(log.contains(diagnostic), "expected {diagnostic:?}:\n{log}");
        }
    }
}

#[test]
fn generated_bindings_compile_in_no_std_consumer() {
    for features in [
        None,
        Some("server"),
        Some("async-client"),
        Some("blocking-client"),
        Some("server,async-client,blocking-client,unsuffixed-packages"),
    ] {
        check(features, None);
    }
}

#[test]
fn invalid_schemas_fail_during_generation() {
    // Exercise each client flavor independently: neither may emit a duplicate
    // inherent method, regardless of whether server generation is also enabled.
    for client in ["async-client", "blocking-client"] {
        for (feature, method) in [
            ("reject-new", "new"),
            ("reject-into-inner", "into_inner"),
            ("reject-transport-mut", "transport_mut"),
        ] {
            check(
                Some(&format!("{client},{feature}")),
                Some(&format!("reserved client method `{method}`")),
            );
        }
    }
    for (feature, module) in [
        ("reject-modules-normalized", "p_self_"),
        ("reject-modules-qualified", "a_b_shared"),
        ("reject-modules-short", "alpha_shared"),
    ] {
        check(
            Some(&format!("server,async-client,blocking-client,{feature}")),
            Some(&format!("both generate a module named `{module}`")),
        );
    }
}
