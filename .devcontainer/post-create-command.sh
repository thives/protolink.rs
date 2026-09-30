#!/usr/bin/env bash
set -euo pipefail

# 1st argument: the workspace folder inside the container
if [ "${1-}" = "" ]; then
  echo "[devcontainer] ERROR: post-create-command.sh requires workspace folder path as argument" >&2
  exit 1
fi

rustup target add thumbv7em-none-eabihf
rustup component add clippy-preview
cargo install --locked cargo-llvm-cov just cargo-nextest rustfmt
