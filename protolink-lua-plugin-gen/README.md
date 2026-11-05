# protolink-lua-plugin-gen

Host-only generator for Lua plugin capability contracts. It resolves BSP manifests into contract graphs, checks them,
writes a lock file, and renders Rust bindings and Lua definitions for each contract.

## Command line

```text
protolink-lua-plugin-gen resolve  --root DIR --profiles DIR [--lock FILE [--write-lock]] MANIFEST...
protolink-lua-plugin-gen generate --root DIR --profiles DIR [--lock FILE] --out DIR MANIFEST...
```

- `MANIFEST` is a root. The lock covers every contract that any root reaches.
- `resolve` reports diagnostics. With `--write-lock` it writes the lock file.
- `generate` writes one `.rs` and one `.lua` file per contract, named by its name and id. With `--lock`, a stale lock is an
  error and nothing is written.

## Use as a library

```toml
[dependencies]
protolink-lua-plugin-gen = "0.0.1"
```

The entry points are `Resolver`, `Graph`, `Lock`, `rust_bindings`, `lua_definitions` and `node_module_source`.

`node_module_source` renders the node configuration of a contract as Rust source. Its `operations` table is
`Contract::native_operations`: every operation that an artifact can reference, as `protolink_lua_bytecode::NativeRef`.
The compiler admits its output against the same table through `CheckerRules::natives`, so the two cannot disagree. An
operation whose signature the artifact ABI cannot carry (more than 32 parameters, or more than one result) is left out of
the table, and it stays in the contract.

## Tests

`cargo test` runs the fixture tests from the crate archive alone. They build their own manifests in temporary
directories. The tests that read the BSP fixture tree (`protolink-lua-compiler/tests/fixtures/bsp`) are kept out of the archive
and run only in the repository.

The generator depends on `protolink-lua-bytecode` for the native ABI limits and the operation type. Both crates are
published with the same version.
