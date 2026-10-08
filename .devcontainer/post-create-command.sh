#!/usr/bin/env bash
set -euo pipefail

# 1st argument: the workspace folder inside the container
if [ "${1-}" = "" ]; then
  echo "[devcontainer] ERROR: post-create-command.sh requires workspace folder path as argument" >&2
  exit 1
fi

sudo apt-get update
sudo apt-get install -y --no-install-recommends golang-go ca-certificates jq

go install github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.3

rustup target add thumbv7em-none-eabihf
rustup component add clippy-preview
cargo install --locked cargo-llvm-cov just cargo-nextest rustfmt

# Keep Go-installed tools available in interactive Zsh sessions.
path_export='export PATH="$PATH:$(go env GOPATH)/bin"'
touch "$HOME/.zshrc"
if ! grep -Fxq "$path_export" "$HOME/.zshrc"; then
  printf '\n%s\n' "$path_export" >> "$HOME/.zshrc"
fi
