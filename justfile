all: check build embedded test clippy fmt docs coverage miri

# no_std crates that must build for embedded targets
no_std := "-p protolink -p protolink-grpc -p protolink-http2"

clippy:
  cargo clippy --workspace --all-features --all-targets -- -D warnings

fmt:
  cargo fmt --all -- --check

check:
  cargo check --workspace --all-features

embedded:
  cargo build --target thumbv7em-none-eabihf --no-default-features {{no_std}}
  cargo build --target thumbv7em-none-eabihf --no-default-features --features async,blocking {{no_std}}

test:
  cargo nextest r --workspace --all-features
  cargo test --workspace --doc --all-features

miri:
  cargo +nightly miri test --features portable-atomic --lib link::ring

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
