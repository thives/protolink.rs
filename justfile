all: build embedded features test clippy fmt docs miri

# no_std crates that must build for embedded targets
no_std := "-p protolink -p protolink-grpc -p protolink-http2"

clippy:
  cargo clippy --workspace --all-features --all-targets -- -D warnings

# The root workspace and the two standalone fixture workspaces
fmt:
  cargo fmt --all -- --check
  cargo fmt --manifest-path examples/embedded-smoke/Cargo.toml --all -- --check
  cargo fmt --manifest-path protolink-grpc-gen/tests/compile-fixture/Cargo.toml --all -- --check

check:
  cargo check --workspace --all-features

embedded:
  cargo build --target thumbv7em-none-eabihf --no-default-features {{no_std}}
  cargo build --target thumbv7em-none-eabihf --no-default-features --features async,blocking {{no_std}}

# Every feature on its own, and the combinations that interact (needs cargo-hack)
features:
  cargo hack check --each-feature --no-dev-deps -p protolink -p protolink-grpc -p protolink-http2 -p protolink-grpc-gen
  cargo check --no-default-features --features async,std -p protolink
  cargo check --no-default-features --features blocking,std -p protolink
  cargo check --no-default-features --features async,blocking -p protolink
  cargo check --no-default-features --features async,blocking,std -p protolink

# Public API compared with the last release (report only; update the tag after releases)
semver:
  cargo semver-checks --workspace --exclude embedded-device-example --baseline-rev v0.0.2 --release-type patch --all-features

test:
  cargo nextest r --workspace --all-features
  cargo test --workspace --doc --all-features

miri:
  cargo +nightly miri test --features portable-atomic --lib link::ring
  cargo +nightly miri test --features portable-atomic --lib asynch::tests
  cargo +nightly miri test --features portable-atomic,std --lib asynch::tests

# Repeat the concurrency tests to catch rare hangs
stress:
  cargo nextest run --workspace --all-features -E 'test(/concurrent_/)' --stress-count 200

build:
  cargo build --workspace --all-features

docs:
  RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links -D missing_docs --cfg docsrs -Z unstable-options --generate-link-to-definition" cargo +nightly doc --workspace --all-features --no-deps

docs-html:
  RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links -D missing_docs --cfg docsrs -Z unstable-options --generate-link-to-definition" cargo +nightly doc --workspace --all-features --no-deps --open

coverage:
  cargo llvm-cov --workspace --all-features nextest --lcov --output-path target/lcov.info
  cargo llvm-cov --workspace --all-features report

coverage-html:
  cargo llvm-cov --workspace --all-features nextest --html --open

# Run the example gRPC server on 127.0.0.1:50051 (test with grpcurl)
example-server:
  cargo run -p embedded-device-example --bin embedded-device-server
