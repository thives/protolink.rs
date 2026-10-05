> [!WARNING]
> This crate is in early development. The API is not yet stable and may change.

> [!CAUTION]
> This crate is not yet production-ready. It has not been widely tested and may contain bugs.

## Generated identifiers

Service modules use snake case, with Rust keywords escaped (for example,
`Type` generates `r#type`). Service traits also escape keyword names. Names
which cannot be raw Rust identifiers, such as `Self`, `crate`, or `_`, gain a
trailing underscore. Protobuf service names and RPC paths remain unchanged.

When service module names collide, the generator prefixes their normalized
package names. If the final Rust module names still collide (including `a.b`
versus `a_b`, or a qualified name versus another service's short name),
generation returns an error identifying both services instead of emitting
uncompilable Rust.

RPC collisions are checked in the generated Rust namespaces. Clients reserve
`new`, `into_inner`, and `transport_mut`, so RPCs named `New`, `IntoInner`, or
`TransportMut` are rejected when either client flavor is enabled. They remain
valid with `.client(false).blocking_client(false)` for server-only generation.
Server lifecycle methods and client `_with_options` methods are reserved only
when the corresponding bindings are enabled.

## Compilation regression fixture

`tests/compile-fixture` is a standalone Cargo workspace with a `#![no_std]`
consumer and runtime dependencies with `std` disabled. Its build script shares
the parsed descriptor set with `micropb-gen`, using the same APIs as the embedded
device example. It compiles all four RPC shapes, keyword service and method
names, nested message types, package-qualified modules, and server-only RPCs
whose names would conflict with client built-ins.

The integration tests run real `cargo check` invocations for combined,
server-only, async-client-only, blocking-client-only, and unsuffixed-package
configurations. Negative cases verify that each client flavor rejects its
built-in names and that normalized/qualified module collisions fail during
generation, rather than in the downstream Rust compiler.

From the repository root:

```sh
cargo test -p protolink-grpc-gen --locked
PROTOLINK_GRPC_GEN_TEST_TARGET=thumbv6m-none-eabi cargo test -p protolink-grpc-gen --test compilation --locked
PROTOLINK_GRPC_GEN_TEST_TARGET=thumbv7em-none-eabihf cargo test -p protolink-grpc-gen --test compilation --locked
```

Cross-compilation requires the named Rust target to be installed. The fixture
has its own lockfile and target directory; no parent workspace, configuration,
or CI changes are needed. Cargo may download fixture dependencies on the first
run; `CARGO_NET_OFFLINE=true` can be used when they are already cached.
