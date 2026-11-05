#!/bin/sh
# Checks all seven crates of the Lua family as a registry consumer sees them.
#
# Two evidence levels, chosen by the environment, and printed at the end so a log says which one it is:
#
#   locked (default)           The dependency versions are the ones in the repository's Cargo.lock. This proves the archives build and
#                              pass their tests against the versions the repository was developed with. It does NOT show
#                              that they build against newer releases of their dependencies, which is what a consumer who
#                              resolves fresh gets.
#   FRESH_RESOLUTION=1         Cargo.lock is removed from the scratch copy before vendoring, so every dependency resolves
#                              to the newest release that its requirement allows on the day the check runs. This proves
#                              the archives against the registry as it is now. It needs network access and is not
#                              reproducible: a failure can come from a dependency release made after the last green run.
#
#   1. Vendors the dependencies (locked, or freshly resolved) into a directory source. Every later cargo call in the
#      scratch copy resolves crates.io through that directory, so nothing from a developer's cache is used.
#   2. Packages the root crates the Lua crates depend on (protolink-http2, protolink-grpc, protolink-grpc-gen, protolink)
#      without verification, then packages and verifies each Lua crate in publish order. After each one, its archive is
#      staged in the directory source, so every later crate that depends on it resolves to the archive, not to the path in
#      the repository. The Lua archives' own builds exercise the root crates through that directory source.
#   3. Checks each archive's contents: licences, README, schemas, the test fixtures the archive's tests read, and no
#      relative path in the packaged manifest. The generator's repository-only BSP test must be absent.
#   4. Extracts each archive into its own empty directory and runs its tests there, offline, with an empty CARGO_HOME.
#   5. Builds clean consumers of each crate and each API feature set, on the host and, where the crate is no_std, on
#      riscv32imac-unknown-none-elf. The generator CLI runs from its archive on a copy of the BSP tree.
#
# The compiler and node tests read copies of one BSP tree under tests/fixtures/bsp. The check fails when the two copies
# differ, so the archives test the same bytes.
#
# The work happens in a scratch copy of the repository, so the repository's Cargo.lock and target/ are not changed. Needs network
# access the first time, to vendor the locked dependencies; a populated cache also works.
set -eu
repo=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

say() { printf '\n== %s\n' "$*"; }

# Publish order: a crate comes after every sibling it depends on, including its dev-dependencies, because the archive's
# own tests need them from the directory source.
crates="protolink-lua-api protolink-lua protolink-lua-bytecode protolink-lua-plugin-gen protolink-lua-vm protolink-lua-compiler protolink-lua-node"

version=0.0.1
mkdir -p "$work/ws"
(cd "$repo" && tar --exclude=target --exclude=.git -cf - .) | tar -xf - -C "$work/ws"
cp -R "$repo/protolink-lua-compiler/tests/fixtures/bsp" "$work/fixtures-bsp"
ws="$work/ws"
cd "$ws"

say "The compiler and node BSP fixtures are the same tree"
diff -r "$ws/protolink-lua-compiler/tests/fixtures/bsp" "$ws/protolink-lua-node/tests/fixtures/bsp" > /dev/null || {
    echo "protolink-lua-node/tests/fixtures/bsp differs from protolink-lua-compiler/tests/fixtures/bsp"
    exit 1
}
echo "compiler and node fixtures match"

if [ "${FRESH_RESOLUTION:-0}" = 1 ]; then
    say "Vendor freshly resolved dependencies (Cargo.lock removed from the scratch copy)"
    rm -f Cargo.lock
    cargo vendor "$work/vendor" > /dev/null
    evidence="fresh dependency resolution on $(date -u +%Y-%m-%d)"
else
    say "Vendor the locked dependencies"
    cargo vendor --locked "$work/vendor" > /dev/null
    evidence="the dependency versions locked in the repository's Cargo.lock; fresh resolution was not exercised"
fi

# Written after vendoring, so vendoring itself reads crates.io.
mkdir -p "$work/.cargo"
cat > "$work/.cargo/config.toml" <<EOF
[net]
offline = true

[source.crates-io]
replace-with = "staged"

[source.staged]
directory = "$work/vendor"
EOF

stage() {
    python3 -I - "$1" "$work/vendor" <<'PY'
import hashlib, json, os, sys, tarfile

crate_path, vendor_dir = sys.argv[1:3]
base = os.path.basename(crate_path)[: -len(".crate")]
with open(crate_path, "rb") as handle:
    package_sum = hashlib.sha256(handle.read()).hexdigest()
dest = os.path.join(vendor_dir, base)
os.makedirs(dest, exist_ok=False)
with tarfile.open(crate_path, "r:gz") as archive:
    for member in archive.getmembers():
        parts = member.name.split("/")
        if member.name.startswith("/") or ".." in parts or parts[0] != base:
            raise SystemExit(f"refusing unsafe member {member.name!r}")
        if not (member.isfile() or member.isdir()):
            raise SystemExit(f"refusing non-regular member {member.name!r}")
        member.name = "/".join(parts[1:])
        if member.name:
            archive.extract(member, dest, filter="data")
files = {}
for root, _dirs, names in os.walk(dest):
    for name in names:
        full = os.path.join(root, name)
        with open(full, "rb") as handle:
            files[os.path.relpath(full, dest)] = hashlib.sha256(handle.read()).hexdigest()
with open(os.path.join(dest, ".cargo-checksum.json"), "w") as handle:
    json.dump({"files": files, "package": package_sum}, handle)
print("staged", base)
PY
}

version_of() {
    cargo metadata --no-deps --format-version 1 --manifest-path "$ws/Cargo.toml" | python3 -I -c '
import json, sys
name = sys.argv[1]
print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == name))' "$1"
}

# $1 is the package. $2 is "--no-verify" for the root crates, and empty for the Lua crates, which are verified.
package() {
    cargo package $2 --target-dir "$work/target" -p "$1" > "$work/package-$1.log" 2>&1 || {
        cat "$work/package-$1.log"
        exit 1
    }
    tail -1 "$work/package-$1.log"
}

say "Package the root crates the Lua crates depend on, unverified, staging each before its dependants"
for crate in protolink-http2 protolink-grpc protolink-grpc-gen protolink; do
    package "$crate" --no-verify
    stage "$work/target/package/$crate-$(version_of "$crate").crate"
done

say "Package and verify each Lua crate in publish order, staging each archive before its dependants"
for crate in $crates; do
    package "$crate" ""
    stage "$work/target/package/$crate-$version.crate"
done

archive_has() {
    tar -tzf "$work/target/package/$1.crate" | grep -qx "$1/$2" || {
        echo "missing from $1: $2"
        exit 1
    }
}
archive_lacks() {
    if tar -tzf "$work/target/package/$1.crate" | grep -qx "$1/$2"; then
        echo "$1 still contains $2"
        exit 1
    fi
}
archive_manifest_has_no_path() {
    if tar -xzOf "$work/target/package/$1.crate" "$1/Cargo.toml" | grep -q '\.\./'; then
        echo "$1 still names a path dependency in its packaged manifest"
        exit 1
    fi
}

say "Check each archive: licences, README, manifest, and the files its tests read"
for crate in $crates; do
    name="$crate-$version"
    archive_has "$name" LICENSE-MIT
    archive_has "$name" LICENSE-APACHE
    archive_has "$name" README.md
    archive_manifest_has_no_path "$name"
done
archive_has protolink-lua-api-$version proto/lua/execution/v1/execution.proto
archive_has protolink-lua-api-$version proto/lua/plugin/v1/plugin.proto
archive_has protolink-lua-compiler-$version tests/fixtures/bsp/bsp.lock
archive_has protolink-lua-node-$version tests/fixtures/bsp/bsp.lock
archive_lacks protolink-lua-plugin-gen-$version tests/bsp_repository.rs
echo "every archive has its licences, README and manifest; the repository-only BSP test is absent"

# The BSP fixtures are copied into two archives, which are published under the crates' MIT OR Apache-2.0 licence. They are
# manifests written for this project. The scan below fails if third-party material appears in them: a link, a copyright
# or licence notice, or a confidentiality marking would each need a decision about redistribution before publication.
say "Check the packaged BSP fixtures for material that is not the project's own"
for crate in protolink-lua-compiler protolink-lua-node; do
    name="$crate-$version"
    listing=$(tar -tzf "$work/target/package/$name.crate" | grep "^$name/tests/fixtures/bsp/" | grep -v '/$' || true)
    [ -n "$listing" ] || {
        echo "$name packages no BSP fixtures"
        exit 1
    }
    # Anything but TOML, Markdown and the lock file is unexpected in the fixture tree.
    unexpected=$(printf '%s\n' "$listing" | grep -Ev '\.(toml|md)$|/bsp\.lock$' || true)
    [ -z "$unexpected" ] || {
        echo "$name packages unexpected fixture files: $unexpected"
        exit 1
    }
    if tar -xzOf "$work/target/package/$name.crate" $(printf '%s\n' "$listing") \
        | grep -qiE 'https?://|copyright|licen[sc]e|spdx|confidential|proprietary|all rights reserved'; then
        echo "$name: a packaged BSP fixture contains a link or a copyright, licence or confidentiality notice"
        exit 1
    fi
done
echo "the packaged BSP fixtures are TOML, Markdown and the lock file only, with no link or rights notice"

extract() {
    mkdir -p "$work/verify/$1"
    tar -xzf "$work/target/package/$1.crate" -C "$work/verify/$1" --strip-components=1
}
cold() {
    env CARGO_HOME="$work/cargo-home" "$@"
}

say "Run each archive's tests from its own directory, offline and with an empty CARGO_HOME"
for crate in $crates; do
    extract "$crate-$version"
done
(cd "$work/verify/protolink-lua-api-$version" && cold cargo test --quiet --features execution && cold cargo test --quiet --features plugin)
(cd "$work/verify/protolink-lua-$version" && cold cargo test --quiet && cold cargo test --quiet --features testing)
(cd "$work/verify/protolink-lua-bytecode-$version" && cold cargo test --quiet)
(cd "$work/verify/protolink-lua-plugin-gen-$version" && cold cargo test --quiet)
(cd "$work/verify/protolink-lua-vm-$version" && cold cargo test --quiet)
(cd "$work/verify/protolink-lua-compiler-$version" && cold cargo test --quiet)
(cd "$work/verify/protolink-lua-node-$version" && cold cargo test --quiet)
echo "every archive's tests pass from its extracted directory"

say "Run the generator CLI from its archive on a copy of the BSP tree"
gen="$work/verify/protolink-lua-plugin-gen-$version"
(cd "$gen" && cold cargo build --quiet --bins)
cli="$gen/target/debug/protolink-lua-plugin-gen"
bsp="$work/fixtures-bsp"
(cd "$bsp" && "$cli" resolve --root . --profiles profiles --lock bsp.lock host/bsp.toml base-esp32c6/bsp.toml base-esp32/bsp.toml)
(cd "$bsp" && "$cli" generate --root . --profiles profiles --lock bsp.lock --out "$work/generated" host/bsp.toml base-esp32c6/bsp.toml base-esp32/bsp.toml)
[ "$(ls "$work/generated" | wc -l)" -gt 0 ] || {
    echo "the generator wrote no files"
    exit 1
}
echo "generator resolved the committed lock and wrote $(ls "$work/generated" | wc -l) files"

say "Build clean consumers"
consumer() {
    name=$1
    dep=$2
    dir="$work/consumers/$name"
    mkdir -p "$dir/src"
    cat > "$dir/Cargo.toml" <<EOF
[package]
name = "consumer-$name"
version = "0.0.0"
edition = "2024"

[dependencies]
$dep
EOF
    cat > "$dir/src/lib.rs"
}

consumer api-execution 'protolink-lua-api = { version = "0.0.1", features = ["execution"] }' <<'RS'
#![cfg_attr(not(test), no_std)]

pub use protolink_lua_api::execution::wire::v1::Context;

#[test]
fn the_execution_context_is_the_schema_message() {
    let context = Context {
        epoch: 1,
        session_id: 2,
        sequence: 3,
    };
    assert_eq!((context.epoch, context.session_id, context.sequence), (1, 2, 3));
}
RS
consumer api-plugin 'protolink-lua-api = { version = "0.0.1", features = ["plugin"] }' <<'RS'
#![cfg_attr(not(test), no_std)]

pub use protolink_lua_api::plugin::proto::lua_::plugin_::v1_::DescribeRequest;

#[test]
fn the_plugin_request_is_the_schema_message() {
    let request = DescribeRequest { protocol: 1 };
    assert_eq!(request.protocol, 1);
}
RS
consumer api-both 'protolink-lua-api = { version = "0.0.1", features = ["execution", "plugin"] }' <<'RS'
#![cfg_attr(not(test), no_std)]

pub use protolink_lua_api::execution::wire::v1::Context;
pub use protolink_lua_api::plugin::proto::lua_::plugin_::v1_::DescribeRequest;
RS
consumer protolink-lua 'protolink-lua = "0.0.1"' <<'RS'
#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::vec;
use protolink_lua::value::{Limits, Tuple, Value, ValueError};

/// A tuple within the limits of a small node.
pub fn bounded() -> Result<Tuple, ValueError> {
    Tuple::new(vec![Value::Integer(1), Value::Nil], Limits::new(4, 8, 12))
}

#[test]
fn a_bounded_tuple_keeps_its_arity() {
    assert_eq!(bounded().expect("within limits").len(), 2);
}
RS
consumer protolink-lua-bytecode 'protolink-lua-bytecode = "0.0.1"' <<'RS'
#![cfg_attr(not(test), no_std)]

use protolink_lua_bytecode::{Artifact, Limits};

/// The artifact format's own bounds decode the empty input as an error, never a panic.
pub fn refuses_empty() -> bool {
    Artifact::decode(&[], &Limits::profile()).is_err()
}

#[test]
fn an_empty_input_is_refused() {
    assert!(refuses_empty());
}
RS
consumer protolink-lua-vm 'protolink-lua-vm = "0.0.1"
protolink-lua = "0.0.1"
protolink-lua-bytecode = "0.0.1"' <<'RS'
#![cfg_attr(not(test), no_std)]

use protolink_lua_bytecode::Limits;
use protolink_lua_vm::VmLimits;

/// The VM's bounds of a small node. Its constructors need no allocation.
pub fn small() -> (VmLimits, Limits) {
    let limits = Limits::profile();
    (
        VmLimits {
            max_frames: 4,
            max_objects: 64,
            max_bytes: 4096,
            wire: protolink_lua::value::Limits::new(8, 64, 128),
            total_fuel: 10_000,
        },
        limits,
    )
}

#[test]
fn the_small_node_bounds_are_constructible() {
    let (vm, _) = small();
    assert_eq!(vm.max_frames, 4);
}
RS
consumer protolink-lua-node 'protolink-lua-node = "0.0.1"' <<'RS'
#![cfg_attr(not(test), no_std)]

use protolink_lua_node::{ChildBounds, ChildRoute, NodeConfig};

/// The type a generated node configuration names. Its fields are public, so the embedding can build it.
pub fn no_routes(config: NodeConfig) -> bool {
    let _: Option<ChildRoute> = None;
    let _ = ChildBounds {
        max_capture_bytes: 0,
        max_result_bytes: 0,
    };
    config.routes.is_empty()
}
RS
consumer protolink-lua-compiler 'protolink-lua-compiler = "0.0.1"' <<'RS'
pub use protolink_lua_compiler::{Diagnostic, Workflow, compile};

#[test]
fn the_compiler_entry_point_is_available_to_consumers() {
    let _ = compile;
}
RS
consumer protolink-lua-plugin-gen 'protolink-lua-plugin-gen = "0.0.1"' <<'RS'
pub use protolink_lua_plugin_gen::Resolver;

#[test]
fn the_generator_resolver_is_available_to_consumers() {
    let _ = core::any::type_name::<Resolver>();
}
RS

for name in api-execution api-plugin api-both protolink-lua protolink-lua-bytecode protolink-lua-vm protolink-lua-node protolink-lua-compiler protolink-lua-plugin-gen; do
    (cd "$work/consumers/$name" && cold cargo test --quiet)
    echo "consumer $name: host tests pass"
done
for name in api-execution api-plugin api-both protolink-lua protolink-lua-bytecode protolink-lua-vm protolink-lua-node; do
    (cd "$work/consumers/$name" && cold cargo check --quiet --target riscv32imac-unknown-none-elf)
    echo "consumer $name: riscv32imac-unknown-none-elf check passes"
done

say "Package verification and clean consumers passed"
echo "evidence level: $evidence"
echo "not covered by this script: hardware, Docker image builds, or any target other than the host and riscv32imac-unknown-none-elf"
