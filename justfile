all: check build embedded test clippy fmt docs coverage

clippy:
  cargo clippy -- -D warnings

fmt:
  cargo fmt --all -- --check

check:
  cargo check --all-features

embedded:
  cargo build --target thumbv7em-none-eabihf --no-default-features

test:
  cargo nextest r --all-features
  cargo test --doc

build:
  cargo build --all-features

docs:
  RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links -D missing_docs --cfg docsrs -Z unstable-options --generate-link-to-definition" cargo +nightly doc --all-features

docs-html:
  RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links -D missing_docs --cfg docsrs -Z unstable-options --generate-link-to-definition" cargo +nightly doc --all-features --open

coverage:
  cargo llvm-cov --all-features nextest --lcov --output-path target/lcov.info
  cargo llvm-cov --all-features report

coverage-html:
  cargo llvm-cov --all-features nextest --html --open
