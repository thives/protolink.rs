#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

for command in grpcurl jq; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Required command not found: $command" >&2
    exit 1
  fi
done

cargo build -p embedded-device-example --bin embedded-device-server

addr=127.0.0.1:50051
proto=examples/embedded-device/proto
service=protolink.examples.embedded.device.Service

./target/debug/embedded-device-server "$addr" >server.log 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true' EXIT

ready=false
for _ in $(seq 1 30); do
  if grpcurl -plaintext -import-path "$proto" -proto device.proto \
    -d '{"correlationId":7,"getStatus":{}}' "$addr" "$service/Command" \
    >/tmp/unary.json 2>/dev/null &&
    jq -e '.correlationId == 7 and has("status")' /tmp/unary.json >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  cat server.log
  exit 1
fi

printf '%s\n' \
  '{"correlationId":1,"setOutput":{"channel":0,"enabled":true}}' \
  '{"correlationId":2,"setOutput":{"channel":1,"enabled":true}}' \
  '{"correlationId":3,"setOutput":{"channel":9,"enabled":true}}' |
  grpcurl -plaintext -import-path "$proto" -proto device.proto -d @ \
    "$addr" "$service/CommandBatch" |
  jq -e '.accepted == 2 and .rejected == 1' >/dev/null

printf '%s\n' \
  '{"correlationId":11,"getStatus":{}}' \
  '{"correlationId":12,"restart":{}}' |
  grpcurl -plaintext -import-path "$proto" -proto device.proto -d @ \
    "$addr" "$service/CommandStream" |
  jq -s -e 'length == 2 and .[0].correlationId == 11 and .[1].correlationId == 12' \
    >/dev/null

grpcurl -plaintext -import-path "$proto" -proto device.proto \
  -d '{}' "$addr" "$service/EventSubscribe" >/tmp/events.json
test ! -s /tmp/events.json

# Keep stdin open so the server-side deadline is exercised on a live stream.
deadline_out=$( (printf '%s\n' '{"correlationId":21,"getStatus":{}}'; sleep 3) |
  grpcurl -plaintext -max-time 1 -import-path "$proto" -proto device.proto -d @ \
    "$addr" "$service/CommandStream" 2>&1 || true)
grep -q 'DeadlineExceeded' <<<"$deadline_out" || { echo "$deadline_out"; exit 1; }

grpcurl -plaintext -import-path "$proto" -proto device.proto \
  -d '{"correlationId":8,"getStatus":{}}' "$addr" "$service/Command" |
  jq -e '.correlationId == 8' >/dev/null

echo "grpcurl interoperability checks passed"
