> [!WARNING]
> This crate is in early development. The API is not yet stable and may change.

> [!CAUTION]
> This crate is not yet production-ready. It has not been widely tested and may contain bugs.

## Message backends

The service glue is backend-independent; each generated service names its
codec explicitly (`__rt::codec::Micropb` or `__rt::codec::Prost`) in the helper
calls and streaming wrapper types. Wire service and method names
(`/pkg.Service/Method`) never depend on the backend.

| | micropb (default) | prost (`prost` feature) |
|---|---|---|
| Messages | `micropb-gen`, from the descriptor set written by `file_descriptor_set_path` | `prost-build`, driven by `Generator::compile_protos_with_prost` |
| Type names | resolved by this crate, mirroring micropb-gen (`messages_path`, `suffixed_package_names`, `extern_type_path` must match) | resolved by prost; this crate only re-roots the paths |
| Service placement | all services in one output file, planned together | in the package module prost writes, as a `<service>_grpc` submodule |
| Runtime feature | `protolink-grpc/micropb` (`protolink/micropb`) | `protolink-grpc/prost` (`protolink/prost`) |

### Prost

The `prost` runtime feature depends on `prost`/`bytes`, which need
pointer-width atomics: it builds for Cortex-M3 and up (`thumbv7em-*`) but not
for Cortex-M0 (`thumbv6m-*`), where micropb remains the codec.

```rust,ignore
// build.rs
let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
let mut config = prost_build::Config::new();
config.btree_map(["."]); // no_std consumers: no HashMap
protolink_grpc_gen::Generator::new()
    .compile_protos_with_prost(&["proto/service.proto"], &mut config)
    .unwrap();
```

No `protoc` is needed: the descriptors come from the same pure-Rust parser and
are handed to `prost_build::Config::compile_fds`, together with the files they
import. Prost then writes one file per package (`pkg.sub.rs`), containing the
messages and, for every service, `pub mod <service>_grpc { ... }`. Include it
the way you include any prost output:

```rust,ignore
pub mod pkg { pub mod sub { include!(concat!(env!("OUT_DIR"), "/pkg.sub.rs")); } }
// pkg::sub::greeter_grpc::{Greeter, GreeterServer, GreeterClient, GreeterBlockingClient}
```

Everything you configure on `prost_build::Config` (`extern_path`, `btree_map`,
type attributes, `include_file`, ...) applies as usual, and request/response
types follow it: well-known types become `::prost_types::...`, an `extern_path`
is used as given (absolute paths and non-path types are kept, relative ones are
re-rooted into the service module). `Generator::compile_fdset_file_with_prost`
takes an existing descriptor set (it must include imports,
`protoc --include_imports`).

Applications that already configure `prost_build::Config` themselves can use
`Generator::prost_service_generator()` (or `ProstServiceGenerator::new()`) with
`Config::service_generator` directly. prost's callback cannot return an error,
so a failure (for example an RPC name collision) is emitted as a `compile_error!`
in the generated module and recorded in `ProstServiceGenerator::error_log()`;
`compile_protos_with_prost` turns recorded errors into `Err`.

**Collision policy.** A service `Name` always generates the module
`name_grpc`, so it cannot shadow prost's own `Name` struct or nested-type module
`name`. Two services of one package (even from different files) that produce the
same module name are an error; rename one with
`ProstServiceGenerator::service_module("pkg.Service", "other_name")`. The
callback has no view of the package's messages, so a message that itself
generates a module such as `name_grpc` (a message called `NameGrpc`) is not
detected; it fails in rustc, and `service_module` resolves it.

A `service_module` override is one Rust module identifier, not a path: Unicode
XID syntax, not `_` alone, with its spelling and case kept. Keywords are
escaped (`type` becomes `r#type`) and `r#type` is accepted as is; the two share
one collision namespace. `self`, `super`, `crate`, `Self`, `extern` and `__rt`
gain a trailing underscore, while `r#self`, `r#super`, `r#crate` and `r#Self`
are rejected. Overrides are validated when the service is generated: an invalid
one (empty, `foo-bar`, `foo::bar`, `1name`, surrounding whitespace, ...) is
recorded in `error_log()` with the service and override named, emits only a
`compile_error!`, and reserves no name, so a corrected override can be retried.
Collision tracking is cleared per package in prost's `finalize_package`, so one
generator and `Config` can be reused for several compilations while collisions
between files of one package are still detected; overrides, settings and
recorded errors are kept.
The micropb-only settings (`messages_path`, `suffixed_package_names`,
`extern_type_path`) are ignored for prost.

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

When messages are included in the same Rust module as the bindings (the default
`messages_path`), a service module that would shadow a message-side name (a
root package module, or for empty packages a top-level message, enum, or
nested-type module) gets a `_grpc` suffix on its normalized name. With default
suffixing, `package foo; service Foo_` becomes `foo__grpc`; with
`suffixed_package_names(false)`, `package foo; service Foo` becomes `foo_grpc`.
Names that do not collide are unchanged. If the fallback also collides with a
message module or another service, generation fails and names the conflicting
items. A `messages_path` naming a different module never triggers the rename.

Generated code refers to runtime types only through the `__rt` alias
(`__rt::Status`, `__rt::__private::Context`, ...) and dispatches through
`<S as self::Service>`, so services may be named `S`, `Status`, `Context`,
`Poll`, `Vec`, `Result`, `Option`, or `__rt`. `messages_path` values beginning with `crate::` or `::` (or
equal to `crate`) are absolute; others, like `crate_messages`, are relative.

RPC collisions are checked in the generated Rust namespaces. Clients reserve
`new`, `into_inner`, and `transport_mut`, so RPCs named `New`, `IntoInner`, or
`TransportMut` are rejected when either client flavor is enabled. They remain
valid with `.client(false).blocking_client(false)` for server-only generation.
Server lifecycle methods and client `_with_options` methods are reserved only
when the corresponding bindings are enabled.

## Compilation regression fixtures

`tests/compile-fixture` is a standalone Cargo workspace with a `#![no_std]`
consumer and runtime dependencies with `std` disabled. Its build script shares
the parsed descriptor set with `micropb-gen`, using the same APIs as the embedded
device example. It compiles all four RPC shapes, keyword service and method
names, nested message types, package-qualified modules, and server-only RPCs
whose names would conflict with client built-ins.

The integration tests run real `cargo check` invocations for combined,
server-only, async-client-only, blocking-client-only, and combined
configurations, each with and without package suffixes. Negative cases verify that each client flavor rejects its
built-in names and that normalized/qualified module collisions fail during
generation, rather than in the downstream Rust compiler.

`tests/compile-fixture-prost` is the same for prost: a `#![no_std]` consumer
with only the `prost` runtime feature (a test asserts that `micropb` is not in its
dependency tree), covering all four RPC shapes, a keyword `service_module` override on a directly configured generator, nested and imported types,
two files in one package, keyword RPC names, well-known types, map fields and
the server-only, async-client-only and blocking-client-only configurations.
Wire compatibility between the backends and the malformed-message status
mapping are tested in `protolink-grpc` (`tests_codec`).

From the repository root:

```sh
cargo test -p protolink-grpc-gen --locked
PROTOLINK_GRPC_GEN_TEST_TARGET=thumbv6m-none-eabi cargo test -p protolink-grpc-gen --test compilation --locked
PROTOLINK_GRPC_GEN_TEST_TARGET=thumbv7em-none-eabihf cargo test -p protolink-grpc-gen --test compilation --locked
cargo test -p protolink-grpc-gen --features prost --locked
```

Cross-compilation requires the named Rust target to be installed. The fixture
has its own lockfile and target directory; no parent workspace, configuration,
or CI changes are needed. Cargo may download fixture dependencies on the first
run; `CARGO_NET_OFFLINE=true` can be used when they are already cached.
