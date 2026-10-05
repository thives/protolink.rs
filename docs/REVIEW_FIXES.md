# Review fixes and validation

Implementation follow-up to `REVIEW.md`. The original review is preserved as the findings record.

## Finding coverage

| Finding | Implementation and regressions |
|---|---|
| 1 — HPACK allocation bounds | `protolink-http2/src/hpack.rs` and `huffman.rs`: bounded literal/Huffman decoding, indexed-size checks before cloning, negotiated dynamic-table maximum; decoder materialization regressions. Oversized lists terminate the connection rather than desynchronize HPACK. |
| 2 — Corrupted ARQ lengths | `protolink/src/link.rs` and `vendor/arq-io-async`: complete COBS frames enter exact-length/type/CRC/BCH validation. Invalid frames are discarded whole. Corrupt lengths, payloads, ACKs, fragmented/adjacent frames, retransmission and flush regressions. Noise processing yields after bounded work. |
| 3 — Transport replay | Blocking and async client terminal-failure latches reject subsequent calls and perform no further I/O. Scripted partial-write and flush failures verify no replay; buffered results remain accessible. |
| 4 — Async server cancellation | Serving-state RAII guard cancels active handlers exactly once on completion, error, future drop, and task abortion, including suspended read/write/flush. |
| 5 — Server draining deadlines | Handler completion and output completion are separate. Draining records retain deadlines through transport flush. Expiry resets flow-control-blocked output immediately; fully framed success output retains its expiry obligation so drivers retire indeterminate connections. |
| 6 — Completed unread responses | Opt-in HTTP/2 receive-credit ownership survives protocol stream removal. Unary and streaming results hold credit until consumption/discard. Client admission and response budgets cover completed results, decompressed messages and intentional early partial credit. Empty decoders reclaim allocations. |
| 7 — Cancelled waiters | Stable acquisition registrations replace wakers on repoll and unregister on completion/drop; disposal and waking occur outside shared-state critical sections. |
| 8 — Fresh clocks | Drivers tick before processing returned input and polling handlers; blocking read budgets are refreshed after output. Fake-clock regressions reject late success. |
| 9 — Output stalls | Async writes and flushes race deadlines and conservatively retire the connection. Armed output deadlines survive concurrent ticks/cancellation. Blocking `OutputTimeout` and I/O-bounded constructors provide explicit write/flush capabilities; read-only variants document their limitation. |
| 10 — Encoder synchronization | First outbound field section starts with the HPACK zero-table-size update, before fragmentation. |
| 11 — Stream admission | Peer concurrent-stream limits and 31-bit stream-ID exhaustion are checked before output or ID mutation, with distinct HTTP/2 errors. |
| 12 — HTTP message validation | Independent directional phases, informational/final headers, terminating trailers, field/pseudo-header validation, connection-specific fields, HTTP(S) targets and content-length accounting. Invalid streams preserve HPACK synchronization. gRPC ignores informational responses until final headers. Rejected terminal server fields reset rather than orphan a stream. |
| 13 — Automatic receive windows | Both flow-control modes enforce advertised stream and connection windows, including SETTINGS adjustment and padding; automatic mode only changes credit timing. |
| 14 — DATA-only response termination | Malformed DATA END_STREAM resets the stream, preserving already received messages while reclaiming an open request half. |
| 15 — Gzip suffixes | Bounded miniz inflation requires stream termination and full input consumption. Junk suffixes and concatenated members are rejected; empty/exact-limit valid cases remain supported. The backend contract is documented. |
| 16 — Combined binary metadata | Comma-separated binary values expand in order with separator whitespace handling. Strict parsing rejects malformed elements; lossy parsing preserves valid siblings. Expansion has independent allocation/entry bounds. |
| 17 — gRPC response validation | Explicit terminal gRPC status/message/metadata take precedence over HTTP fallback. Fallback is deferred until termination; non-gRPC content types cannot deliver messages or succeed. |
| 18 — Server fairness | Polling starts rotate and each call has a bounded response quantum; yielding runnable work schedules another wake. Infinite-producer and bidirectional progress regressions. |
| 19 — Arithmetic boundaries | Checked LPM frame arithmetic, wire-length conversion and allocation; saturating output budgets; bounded, re-evaluated Tokio sleeps for unrepresentable durations. |
| 20 — Generated Rust names | Namespace-aware keyword escaping and collision checks reserve client built-ins and detect normalized/package-qualified collisions. Actual Cargo compilation fixtures cover all RPC shapes and server/async/blocking bindings in a `no_std` consumer. |

## API and behavior changes

- `lpm::encode` and `lpm::frame` now return `Result<Vec<u8>, Status>`; callers must propagate or handle framing failures.
- `ClientConfig` adds `max_buffered_response_bytes` (default **1 MiB**) and `max_retained_calls` (default **64**). Consume/discard retained results and finished streaming metadata to free admission budget.
- HTTP/2 adds `retain_receive_capacity`; retained capacity must be explicitly released even after completion/reset. The gRPC client handles this ownership internally.
- HTTP/2 local opens can return `Error::StreamLimit` or `Error::StreamIdExhausted`; failed opens do not consume IDs or emit bytes.
- Custom sans-I/O server drivers using `pending_output`/`consume_output` must call `Server::output_flushed()` **only after successful transport flush**. `take_output()` instead transfers delivery responsibility to its caller and acknowledges fully serialized output.
- Transport/protocol failure permanently retires a client. Output deadline/cancellation retires the connection because a write or flush may not be cancel-safe. New `Error::WriteZero` and `Error::OutputDeadline` variants can affect exhaustive matches.
- Blocking whole-call I/O bounds require `ReadTimeout + OutputTimeout` and `with_io_timeouts` / `serve_with_io_timeouts`. Existing clock/read-timeout APIs do not bound write or ACK-waiting flush stalls.
- Received metadata blocks are capped at **128 expanded entries** and **8192 owned key/value bytes**, checked before allocation. These limits are independent of HTTP/2 limits; application-created metadata is not capped by the receive parser.
- `ReliableLink` uses the framed ARQ adapter. Constructors remain available, but explicitly spelled concrete adapter types may need migration. The link wire format is unchanged.
- Generators reject RPC names colliding with enabled client built-ins rather than emitting uncompilable code; such names can still be used in server-only bindings.

## Embedded validation

`.github/workflows/ci.yml` adds feature-isolated `thumbv6m-none-eabi` checks with portable-atomic critical sections, generated-code cross-compilation and linked Cortex-M0 smoke builds.

`examples/embedded-smoke` is a standalone `no_std` workspace with all four RPC shapes, generated clients/server bindings, a bounded allocator, linker script, reset/vector setup and a platform interrupt critical-section implementation. It is deliberately excluded from host workspace membership. See its README for feature matrices, commands and platform limitations.

## Validation performed

- Workspace all-features tests, including examples, HTTP/2 interoperability, generator compilation fixtures and doctests.
- Runtime/core tests with default features disabled and async/blocking/compression selected.
- Workspace Clippy with all features/targets and `-D warnings`.
- Rust 1.88 workspace all-features check, workspace formatting and diff whitespace checks.
- Strict nightly documentation build with missing-doc and broken-link errors enabled.
- Host and embedded library checks, including `thumbv7em-none-eabihf` and no-CAS `thumbv6m-none-eabi`.
- Embedded feature matrices and linked smoke builds with and without compression.
- Generator fixture compilation on host, `thumbv6m-none-eabi` and `thumbv7em-none-eabihf`.
- Vendored ARQ: **57 unit tests and 2 doctests**.
- Final Miri configurations: ring buffer **11 tests**, async RefCell state **17 tests**, async mutex state **17 tests**; all passed.

## Explicit limitations and release follow-up

- The public HTTP/2 `HeaderField` remains UTF-8-based. HPACK itself retains raw octets and synchronizes fully before HTTP/API validation, but legal non-UTF-8 `obs-text` values are rejected stream-locally, not converted lossily. A byte-capable public field API would be a separate compatibility expansion.
- Newly generated deadline-error trailers are best-effort after the call has already expired; their transmission is not raced against the expired deadline. Previously completed success output retains its deadline through flush. Synchronous handlers and scheduler starvation cannot be preempted by these drivers.
- The ARQ framed API is a local dependency patch. Before publishing to crates.io, upstream it or publish a renamed/versioned patched dependency and update the manifest; the published upstream 0.0.2 is not an API-compatible replacement.
- Cross-compilation and linking do not prove hardware execution, allocator sizing, interrupt stress behavior or runtime stack bounds. No hardware execution is claimed. The smoke allocator is intentionally not suitable for a long-lived server.
- Existing grpcurl CI coverage is preserved; grpcurl itself was not run locally during this implementation.
