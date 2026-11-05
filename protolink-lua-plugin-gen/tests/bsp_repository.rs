//! Generator checks on the BSP tree. They read the fixture copy under `../protolink-lua-compiler/tests/fixtures/bsp`: the
//! manifests, profiles and committed lock.
//!
//! This file is excluded from the crate archive (see `exclude` in `Cargo.toml`), because it reads a tree outside this crate.
//! The fixture-based tests in `resolve.rs` cover the same rules without it.

use std::fs;
use std::path::{Path, PathBuf};

use protolink_lua_plugin_gen::{Lock, Resolver};

/// The BSP roots. The lock covers every contract that any of them reaches.
const BSP_ROOTS: [&str; 4] = [
    "host/bsp.toml",
    "base-esp32c6/bsp.toml",
    "base-esp32/bsp.toml",
    "ota/bsp.toml",
];

const AGENT_ID: &str = "2bdbdeef-600a-46de-82b5-1936195c2d11";
const DUT_ID: &str = "78240354-a109-42a2-9b91-e0497fbd55d0";

fn bsp() -> (PathBuf, protolink_lua_plugin_gen::Resolver) {
    let root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../protolink-lua-compiler/tests/fixtures/bsp");
    let resolver = Resolver::new(&root, root.join("profiles")).expect("resolver");
    (root, resolver)
}

#[test]
fn the_bsp_example_resolves_with_its_consumer_profile() {
    let (root, resolver) = bsp();
    let graphs: Vec<_> = BSP_ROOTS
        .iter()
        .map(|rel| resolver.resolve(root.join(rel)).expect("bsp root resolves"))
        .collect();
    let host = &graphs[0];
    assert_eq!(host.contracts().count(), 6);
    let agent = host.contract(AGENT_ID).expect("agent");
    assert_eq!(agent.child("remote").expect("remote").child, DUT_ID);
    let lock_path = root.join("bsp.lock");
    let lock =
        Lock::parse(&fs::read_to_string(&lock_path).expect("lock"), &lock_path).expect("parse");
    assert!(
        lock.verify(&graphs, &lock_path).is_ok(),
        "the committed lock matches the BSP inputs"
    );
}

#[test]
fn the_two_base_manifests_expose_identical_modules_under_their_own_identities() {
    let (root, resolver) = bsp();
    let c6 = resolver
        .resolve(root.join("base-esp32c6/bsp.toml"))
        .expect("esp32c6 base resolves");
    let esp32 = resolver
        .resolve(root.join("base-esp32/bsp.toml"))
        .expect("esp32 base resolves");
    let (c6, esp32) = (c6.root(), esp32.root());

    assert_eq!(
        c6.modules, esp32.modules,
        "the chips must expose the same base surface"
    );
    let names: Vec<&str> = c6.modules.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["chip", "reset"]);
    let base_privileges = ["base.read", "base.reset"];
    assert_eq!(c6.privileges, base_privileges);
    assert_eq!(esp32.privileges, base_privileges);
    assert_ne!(c6.id, esp32.id);
    assert_ne!(c6.name, esp32.name);
    assert_ne!(c6.digest, esp32.digest, "each chip is its own contract");
    for module in &c6.modules {
        for operation in &module.operations {
            // The VM's native protocol carries one value per call, so an operation has at most one result.
            assert!(
                operation.results.len() <= 1,
                "{}.{} returns several values",
                module.name,
                operation.name
            );
        }
    }
}

#[test]
fn the_ota_write_is_a_privileged_bounded_write_with_no_result() {
    let (root, resolver) = bsp();
    let graph = resolver
        .resolve(root.join("ota/bsp.toml"))
        .expect("ota resolves");
    let ota = graph.root();
    let write = ota
        .modules
        .iter()
        .find(|m| m.name == "ota")
        .and_then(|m| m.operations.iter().find(|o| o.name == "write"))
        .expect("ota.write");
    assert_eq!(write.requires, ["ota.write"]);
    assert!(write.results.is_empty(), "ota.write returns no value");
    let blob = write
        .params
        .iter()
        .find(|p| p.name == "blob")
        .expect("blob");
    assert!(
        matches!(
            blob.ty,
            protolink_lua_plugin_gen::Type::Bytes { max_bytes: 256 }
        ),
        "blob is bounded to 256 bytes"
    );
}

#[test]
fn the_base_manifests_expose_no_partition_or_flash_module() {
    let (root, resolver) = bsp();
    for manifest in ["base-esp32c6/bsp.toml", "base-esp32/bsp.toml"] {
        let graph = resolver
            .resolve(root.join(manifest))
            .expect("base resolves");
        for module in &graph.root().modules {
            assert!(
                module.name != "partition" && module.name != "flash",
                "{manifest}: {} is not in the base surface",
                module.name
            );
        }
    }
}

#[test]
fn the_base_manifests_resolve_to_the_same_digest_on_every_run() {
    let (root, resolver) = bsp();
    let first = resolver
        .resolve(root.join("base-esp32c6/bsp.toml"))
        .expect("first")
        .root()
        .digest;
    let second = resolver
        .resolve(root.join("base-esp32c6/bsp.toml"))
        .expect("second")
        .root()
        .digest;
    assert_eq!(first, second);
}

#[test]
fn one_root_is_checked_against_the_multi_root_lock() {
    let (root, resolver) = bsp();
    let host = resolver
        .resolve(root.join("host/bsp.toml"))
        .expect("host resolves");
    let lock_path = root.join("bsp.lock");
    let lock =
        Lock::parse(&fs::read_to_string(&lock_path).expect("lock"), &lock_path).expect("parse");
    assert!(lock.verify_root(&host, &lock_path).is_ok());
    let host_id = host.root().id.clone();
    let mut stale = lock.clone();
    for entry in &mut stale.contracts {
        if entry.id == host_id {
            entry.digest = "sha256:00".to_string();
        }
    }
    assert!(stale.verify_root(&host, &lock_path).is_err());
    let mut missing = lock;
    missing.contracts.retain(|entry| entry.id != host_id);
    assert!(missing.verify_root(&host, &lock_path).is_err());
}
