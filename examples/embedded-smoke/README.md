# `no_std` / Cortex-M0 compile-and-link smoke

This fixture closes the embedded validation gaps described at the bottom of
`docs/REVIEW.md`. It is **not a board example or a hardware execution test**.
CI checks generated Rust for `thumbv6m-none-eabi` (ARMv6-M, no native CAS), then
links a bare-metal ARM ELF with an allocator and a platform critical section.
No flashing, emulation, successful RPC exchange, interrupt stress test, or
on-device memory/stack usage is claimed.

## Coverage and isolation

- `proto/smoke.proto` covers unary, server-streaming, client-streaming, and
  bidirectional RPCs, with allocated protobuf strings and repeated fields.
- `build.rs` follows `examples/embedded-device`: a host-only pure-Rust parse
  feeds both `protolink-grpc-gen` and `micropb-gen`. No `protoc` is required.
- `src/lib.rs` is unconditionally `#![no_std]`. It implements the generated
  server trait and monomorphizes both real server drivers, generated clients,
  and their typed streaming operations against EOF/error-only stub I/O.
- The async path also uses the ring buffer and registers a waker, retaining
  portable-atomic/CAS integration in the linked image. The optional compression
  path retains gzip decoding; it does not run the memory-heavy compressor.
- All target dependencies disable default features. The fixture has its own
  nested `[workspace]` and no dev-dependencies, so root tests and the Tokio
  example cannot unify `std` into its target graph. Host build dependencies
  intentionally use `std`; resolver 3 keeps them separate from target features.
- The generated async and blocking clients are independently selected by the
  fixture features. `portable-atomic-critical-section` is always supplied to
  the runtime and currently implies the runtime's `async` feature even for a
  blocking-only generated-client check. Thus that check does **not** claim an
  async-free runtime dependency graph.

The root `Cargo.toml` explicitly excludes this standalone workspace. Use
`--manifest-path`; do not add this fixture as a member, which would defeat
feature isolation and expose its ARM-only binary to host workspace builds.

## Commands (from the repository root)

Install the target once:

```sh
rustup target add thumbv6m-none-eabi
```

Check all generated code without linking:

```sh
cargo check --manifest-path examples/embedded-smoke/Cargo.toml \
  --release --target thumbv6m-none-eabi --lib --no-default-features \
  --features async,blocking,compression
```

The CI consumer matrix independently checks `async`, `blocking`,
`async,blocking`, and each combination with `compression`. Separately, the
runtime matrix checks `portable-atomic-critical-section` (which implies `async`)
with all combinations of `blocking` and `miniz-oxide`, using only `--lib` (not tests,
examples, or `--all-targets`, which could pull in std dev-dependencies). Separate
sans-I/O checks cover no codec, micropb, miniz-oxide, and both codecs without
`protolink`'s default micropb dependency unifying their features.

Build the **linked binary**, not just metadata:

```sh
cargo build --manifest-path examples/embedded-smoke/Cargo.toml \
  --release --target thumbv6m-none-eabi --bin embedded-smoke \
  --no-default-features --features async,blocking
cargo build --manifest-path examples/embedded-smoke/Cargo.toml \
  --release --target thumbv6m-none-eabi --bin embedded-smoke \
  --no-default-features --features async,blocking,compression
```

The ELF is at
`examples/embedded-smoke/target/thumbv6m-none-eabi/release/embedded-smoke`.
`build.rs` supplies binary-only linker arguments and copies `link.x` into
`OUT_DIR`, so these commands work from the root without relying on Cargo loading
configuration from the nested fixture directory.

Optional inspection on a host with GNU binutils:

```sh
readelf -h -A -S examples/embedded-smoke/target/thumbv6m-none-eabi/release/embedded-smoke
readelf -x .vector_table examples/embedded-smoke/target/thumbv6m-none-eabi/release/embedded-smoke
nm --defined-only examples/embedded-smoke/target/thumbv6m-none-eabi/release/embedded-smoke
cargo tree --manifest-path examples/embedded-smoke/Cargo.toml \
  --target thumbv6m-none-eabi --no-default-features \
  --features async,blocking,compression --edges normal,features
cargo fmt --manifest-path examples/embedded-smoke/Cargo.toml --all -- --check
```

The target-only tree should contain no `std` or Tokio features. The image should
have ARM/soft-float attributes, a vector table with an aligned RAM stack pointer
and Thumb reset address, the allocator, and `_critical_section_1_0_acquire` /
`_critical_section_1_0_release` definitions. Existing Miri jobs remain unchanged;
this cross-build does not replace their concurrency checks.

## Platform limits

`src/platform.rs` supplies its own minimal reset/vector code; it copies `.data`
and zeros `.bss` before using Rust globals. `link.x` uses a **generic**, not
board-specific, memory map: 256 KiB FLASH at `0x00000000`, 64 KiB RAM at
`0x20000000`, and a reserved 16 KiB stack. Linker assertions catch static
RAM/heap overlap with that stack; they do not measure runtime stack use.

The global allocator is a critical-section-protected 32 KiB bump arena with
alignment and exhaustion checks. It **never reclaims allocations** and is only
for this bounded link fixture, not a long-lived RPC server. Exhaustion goes to
the allocation-error/panic halt path. Compiling/linking does not establish that
the smoke sequence fits in that heap at runtime.

The critical section saves PRIMASK, disables configurable interrupts, and
restores the previous PRIMASK, supporting nested and already-disabled callers.
The assembly remains a compiler memory barrier. This implementation is only
valid for a **single-core Cortex-M0 in privileged mode**, with no
critical-section or allocator use from NMI/HardFault (PRIMASK does not mask
those exceptions). Panic and default
exception handlers halt without allocating. Real firmware must supply its
board's memory map, interrupt policy, I/O, allocator, and scheduling.
