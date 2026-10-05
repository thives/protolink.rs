I’ll review the code and feature configuration, focusing on correctness, robustness, and `no_std` compatibility. I’ll leave files unchanged and provide concrete implementation steps for each finding.

I’ll split the review across HTTP/2, gRPC state handling, and the I/O drivers, while I check `no_std` features, the link layer, and code generation. I’ll distinguish confirmed defects from validation gaps; the tools available here don’t include a terminal for running Cargo checks.

The review has surfaced potential memory-bound and lifecycle failures, especially around HPACK decoding, stream cleanup, and transport errors. I’m checking the strongest findings against the exact code paths and looking for embedded configurations that the current CI matrix might miss.

## Summary

The most serious issues are **ineffective receive-memory bounds, unreliable recovery from link corruption, and incomplete cleanup after transport failure or cancellation**.

I did **not find direct `std` leakage in the inspected runtime feature paths**, but I cannot certify that `no_std` builds pass: no terminal was available, so Cargo builds, tests, and Miri were not run. The review used current disk contents, existing tests, editor diagnostics, and versioned dependency sources. **No files were changed.**

Below, **P1** means high priority; **P2** means a correctness or robustness issue worth addressing next. Each finding includes concrete implementation and regression-test steps.

## High-priority findings

### 1. P1 — HPACK decoding can exceed the advertised memory limits dramatically

**Locations:** `protolink/protolink-http2/src/lib.rs:346`, `:742–749`, `:783–785`; `protolink/protolink-http2/Cargo.toml:16`.

Two independent problems exist:

- The resolved `zerodds-hpack` decoder accepts peer-supplied dynamic-table size updates without enforcing the negotiated maximum. An update does not allocate immediately, but subsequent headers can grow the retained table beyond 4,096 bytes.
- `max_header_list_size` is checked **after** the complete owned header list is decoded. A legitimate 4,000-byte dynamic entry followed by a 16 KiB block of repeated indexed references materializes approximately **62.5 MiB of values**, before overhead, despite the default 8 KiB limit. This is a source-derived calculation, not a measured peak.

The dependency’s [decoder](https://docs.rs/crate/zerodds-hpack/1.0.0-rc.6/source/src/decoder.rs) and [table lookup](https://docs.rs/crate/zerodds-hpack/1.0.0-rc.6/source/src/table.rs) confirm these paths. Both are particularly serious on allocator-constrained devices.

**Implementation plan**
1. Patch or replace the decoder with one that separately tracks the negotiated table maximum and rejects oversized updates before changing its capacity.
2. Enforce decoded-header limits incrementally, before cloning or retaining fields. On overflow, either terminate the connection or continue in bounded discard mode to maintain HPACK synchronization.
3. Add separate regressions for oversized table updates and indexed-header amplification. Measure allocations or materialized bytes **during decoding**; checking only the eventual reset would miss the current defect.

### 2. P1 — A corrupted ARQ length byte can permanently stall the “reliable” link

**Locations:** `protolink/protolink/src/link.rs:170–190`, `:260–268`, `:281–290`.

`CobsFramed` passes every structurally valid COBS payload to ARQ as an undelimited byte stream. COBS validity does not establish ARQ integrity.

For example, changing a DAT length from `125` to `253` can leave its COBS encoding valid. In `arq-io-async 0.0.2`, the length is then rejected before the DAT CRC check. The error leaves the offending bytes buffered, so retrying encounters the same error instead of reading the retransmission. In-range length corruption can instead consume a prefix of the next frame or leave a suffix of the damaged frame.

This is a documented limitation of the dependency’s raw byte-stream parser, but contradicts the composed link’s recovery claims. Existing link tests cover clean round trips, not corruption recovery.

**Implementation plan**
1. Add a frame-oriented receive path to ARQ and expose complete decoded COBS frames to it; keep generic `CobsFramed::Read` for standalone users.
2. Validate exact frame length, type, and CRC/BCH within each bounded frame. Discard the entire invalid frame without consuming bytes from another frame.
3. Update the alias/constructor and recovery documentation.
4. Add deterministic corruption tests for `125→253`, `125→61`, and `125→127`, plus payload/ACK corruption, fragmented reads, and adjacent frames. Assert ordered delivery and eventual `flush()` completion after retransmission.

### 3. P1 — Blocking transport errors can cause already-written bytes to be replayed

**Location:** `protolink/protolink/src/blocking.rs:502–532`.

`write_output()` consumes queued output only after both `write_all()` and `flush()` succeed. If a partial write fails, or writing succeeds but flushing fails, accepted bytes remain queued. `fail_all()` finishes current calls but does not retire the connection; starting another call can retransmit those bytes, corrupting the HTTP/2 stream.

**Implementation plan**
1. Latch a terminal transport-failure status and reject new calls or further transport activity after an indeterminate write/flush failure.
2. Centralize failure handling, while still allowing callers to retrieve buffered messages and terminal statuses.
3. Add scripted transports that fail after accepting a prefix and after accepting everything but failing flush. Verify subsequent calls perform no additional I/O and cannot replay output.
4. Apply an explicit, consistent fatal-error policy to the async client too; its incremental write accounting already avoids this particular blocking replay bug.

### 4. P1 — Dropping the async server future skips handler cancellation

**Location:** `protolink/protolink/src/asynch.rs:72–75`.

`server.cancel_all(handler)` executes only after the serving future returns. Aborting the task or dropping the future while it is suspended bypasses that cleanup. Handler-owned subscriptions and per-call resources can survive a dead connection.

**Implementation plan**
1. Introduce a serving-state RAII guard holding the server and handler borrow.
2. Perform cancellation in its `Drop`, using the same path for normal completion and errors to avoid duplicate callbacks.
3. Test dropping the future while suspended in read, write, and flush, plus Tokio task abortion. Assert exactly one cancellation per active streaming call.
4. Exercise the guard without the `std` feature as well.

### 5. P1 — Server deadlines do not reclaim flow-control-stalled responses

**Locations:** `protolink/protolink-grpc/src/server.rs:162–197`, `:559–561`, `:761–776`, `:838–858`.

If response DATA is blocked by the peer’s window, expiry queues deadline trailers behind that DATA. Any deferred reset waits behind it too.

Additionally, unary and client-streaming handlers are removed from `calls` immediately after their responses are queued. Their deadlines disappear even if the response cannot finish transmitting. Neither later ticks nor `cancel_all()` reclaim these draining streams.

**Implementation plan**
1. Separate handler completion from stream completion. Retain lightweight draining records with deadlines until queued output is resolved.
2. On expiry, do not leave the only termination mechanism behind indefinitely blocked DATA. Reset immediately when a valid terminal response cannot otherwise be sent.
3. Include draining streams in connection cancellation and resource cleanup.
4. Add zero-window and partially transmitted-response tests for unary and streaming calls. Verify expiry releases queued resources and later WINDOW_UPDATEs do not resume expired output.

## Additional correctness and robustness findings

### 6. P2 — Completed-but-unread responses bypass receive-memory backpressure

**Locations:** `protolink/protolink-http2/src/lib.rs:1085–1097`, `:1211–1216`; `protolink/protolink-grpc/src/client.rs:518–553`.

HTTP/2 removal returns all remaining receive credit, even when the gRPC client retains the corresponding messages. Normal response completion is sufficient; no malformed peer is required.

For example, two completed streaming calls, each retaining 65 messages of 1,000 bytes, can retain 130,650 framed bytes despite the 65,535-byte connection window. Growth requires the application to keep issuing calls without consuming completed results—it is not unlimited growth from one fixed call.

Unary completion has another early-release path: extracting the message releases credit before storing it in `done`.

**Implementation plan**
1. Keep receive-credit ownership independent of stream protocol lifetime; release credit when retained data is consumed or discarded.
2. Transfer unary credit obligations into completed-result entries until `take_response()` or cancellation.
3. Add a retention/admission budget covering completed-unread calls, accounting for decompressed sizes and intentional partial-message credit.
4. Test repeated finite streaming and unary completions without consuming results, then verify consumption/cancellation releases budget and credit exactly once.

### 7. P2 — Cancelled async I/O waiters retain unbounded wakers

**Locations:** `protolink/protolink/src/asynch.rs:338–377`, `:215–225`.

Waiting operations append cloned wakers to `State::waiters`, but cancellation does not unregister them. If another stream holds an indefinitely pending read, repeatedly creating and cancelling waiting operations with distinct wakers grows this vector indefinitely. Both the mutex and `no_std`/`RefCell` branches are affected.

**Implementation plan**
1. Give each acquisition a stable registration identity and cancellation guard.
2. Replace its waker on repoll; unregister on completion or drop.
3. Wake and dispose of removed wakers outside the shared-state critical section.
4. Hold one read pending while repeatedly polling/dropping another acquisition; assert bounded registrations and prompt waker deallocation in both feature configurations.

### 8. P2 — Stale clock samples allow late success and overlong blocking reads

**Locations:** `protolink/protolink/src/asynch.rs:105–115`, `:452–485`; `protolink/protolink/src/blocking.rs:312–327`, `:515–527`.

Clients feed returned input into the core without first updating time. A response that becomes available **after** its deadline can therefore complete successfully using stale time. The async server similarly polls a newly ready handler before applying expiry.

Blocking drivers also calculate read timeouts from time sampled before writing/flushing: an 80 ms write with 100 ms initially remaining still permits another 100 ms read.

**Implementation plan**
1. Tick immediately before processing returned input and before polling async server handlers.
2. Refresh blocking time after output processing; apply expiry before starting another read.
3. Calculate read budgets from fresh clock samples.
4. Add deterministic tests where responses/handlers become ready strictly after expiry, and writes advance a fake clock. Verify late success is rejected and only the remaining read budget is installed.

### 9. P2 — Write/flush stalls can postpone RPC deadlines indefinitely

**Locations:** `protolink/protolink/src/asynch.rs:91–96`, `:382–436`; `protolink/protolink/src/blocking.rs:312–327`, `:502–524`; `protolink/docs/DEADLINES.md:45–50`.

Deadline timers race reads, not writes or flushes. A permanently pending flush prevents even a unary timeout from being enforced. This matters for ARQ, whose flush waits for acknowledgments.

Blocked writes are already acknowledged elsewhere as a limitation; the additional problem is that the deadline documentation presents a stronger whole-call guarantee.

**Implementation plan**
1. Immediately document that writes and flushes can delay expiry indefinitely, including ACK-waiting ARQ flushes.
2. If retaining hard whole-call deadlines, race async output operations against deadlines. On timeout, conservatively retire the connection rather than reusing a stream after cancelling a potentially non-cancel-safe write.
3. Add deadline-aware write **and flush** capabilities for blocking transports; read timeouts alone cannot provide the guarantee.
4. Test permanently pending writes and flushes separately, including a flush that wakes repeatedly without completing.

### 10. P2 — The zero-sized HPACK encoder is not synchronized on the wire

**Locations:** `protolink/protolink-http2/src/lib.rs:337–340`, `:1261`.

`Encoder::with_max_size(0)` changes local storage but emits no HPACK size update. After acknowledging a peer’s `SETTINGS_HEADER_TABLE_SIZE = 0`, the next header block lacks the required update and can be rejected by a strict decoder.

**Implementation plan**
1. Prepend the HPACK zero-size update, `0x20`, to the first emitted field block before fragmentation.
2. Keep the permanently-zero policy; one initial update suffices without general resizing machinery.
3. Test initial zero settings, later reductions to zero, and fragmented first blocks against strict decoding or explicit wire assertions.

### 11. P2 — Local stream admission ignores peer limits and ID exhaustion

**Location:** `protolink/protolink-http2/src/lib.rs:947–966`.

`open_stream()` neither checks `peer.max_concurrent_streams` nor prevents allocation above the 31-bit stream-ID maximum. Known peer limits of zero or one are ignored. After the last valid ID, frame encoding masks the reserved bit and can reuse an old wire ID.

**Implementation plan**
1. Check active locally initiated streams against the peer limit before changing IDs or output.
2. Return distinct retryable-limit and exhausted-ID errors; allow existing streams to finish.
3. Test limits zero/one, settings reductions/increases, reopening after completion, and allocation at `0x7fff_ffff`. Failed opens must leave output and the next ID unchanged.

### 12. P2 — HTTP/2 accepts invalid message ordering and field sections

**Locations:** `protolink/protolink-http2/src/lib.rs:742–801`, `:1003–1015`.

Stream half-closure alone does not establish HTTP message phase. The implementation permits DATA before response headers and non-terminating trailer blocks.

Decoded fields also bypass mandatory HTTP/2 validation, including duplicate/late pseudo-headers, uppercase names, prohibited value bytes, and missing required request pseudo-headers.

**Implementation plan**
1. Track per-direction phases: initial headers, informational responses, final headers/body, and terminal trailers.
2. Validate DATA/HEADERS ordering and require END_STREAM on trailers.
3. Validate field syntax, connection-specific fields, and pseudo-header ordering, uniqueness, role, and required presence before publishing events; validate outbound sections too.
4. Add table-driven malformed-message tests and positive informational-response cases. Verify rejected streams do not desynchronize HPACK or break another valid stream.

### 13. P2 — Automatic flow control does not enforce small advertised receive windows

**Location:** `protolink/protolink-http2/src/lib.rs:559–568`.

Automatic mode compares a frame’s length with `initial_window_size.max(65_535)`. Consequently, after acknowledging a configured window of 100 bytes, it accepts a 101-byte DATA frame; a zero window also accepts nonempty DATA.

**Implementation plan**
1. Use receive-window accounting in both flow-control modes, including SETTINGS adjustments and padding.
2. Make automatic mode differ only in when valid bytes are credited back.
3. Add post-handshake zero-window, 100-byte-window, and padded-overrun tests. Assert stream flow-control errors while retaining valid automatic replenishment.

### 14. P2 — A response ending in DATA can orphan the client’s request half

**Location:** `protolink/protolink-grpc/src/client.rs:642–647`.

When response DATA carries END_STREAM without trailers, the client records `INTERNAL` without resetting the stream. For a bidirectional call whose request half is still open, subsequent send/close operations stop progressing; once the completed call is removed, cancellation cannot reclaim that request stream.

**Implementation plan**
1. Reset this malformed-response path, matching other terminal protocol errors.
2. Centralize cleanup so completed-call removal cannot strand an open local half or queued requests.
3. Add a bidirectional raw-peer test with an open request half and DATA END_STREAM. Verify message/status delivery, reset, and reuse of the connection under a small stream limit.

### 15. P2 — The stock gzip backend can silently ignore a compressed suffix

**Locations:** `protolink/protolink-grpc/src/compression/miniz.rs:53–63`; `protolink/protolink-grpc/src/compression/deflate.rs:89–109`.

`decompress_to_vec_with_limit()` stops at the first DEFLATE end marker without requiring complete input consumption. The gzip wrapper independently treats the last eight bytes as the trailer.

Junk inserted before the trailer can therefore be accepted. Concatenating identical gzip members can return only the first member’s payload while passing the final CRC/size check, silently truncating the value.

**Implementation plan**
1. Use a bounded lower-level miniz interface that reports consumed input.
2. Require complete stream termination and full body consumption; document that contract for all `Deflate` backends.
3. Add stock-backend tests for junk before the trailer and concatenated identical members. Reject both without delivering truncated messages; retain exact-limit and valid-empty cases.

### 16. P2 — Legal combined binary metadata is rejected or discarded

**Location:** `protolink/protolink-grpc/src/metadata.rs:185–219`.

A field such as `trace-bin: AQ==,Ag==` must be split before base64 decoding under the gRPC protocol. The parser decodes the entire field instead: requests fail with `INVALID_ARGUMENT`, while response metadata silently loses both values.

**Implementation plan**
1. Parse binary fields into multiple ordered entries by splitting comma-separated values before decoding.
2. Handle separator whitespace and define strict/lossy behavior per element.
3. Test combined and repeated fields, padded/unpadded values, empty values, and malformed siblings through request headers, response headers, and trailers.

### 17. P2 — Client response validation misclassifies explicit statuses and accepts non-gRPC responses

**Location:** `protolink/protolink-grpc/src/client.rs:571–615`.

Two concrete cases:

- `:status: 503` with explicit `grpc-status: 7` becomes `UNAVAILABLE`, discarding the authoritative `PERMISSION_DENIED`, message, and metadata. HTTP fallback applies only when gRPC status is absent.
- A `200` response with missing or `application/json` content type can succeed if its body happens to contain valid LPM bytes and successful trailers.

**Implementation plan**
1. Give explicit terminal gRPC status precedence over HTTP fallback, preserving metadata and message.
2. Retain fallback information until terminal status is known.
3. Validate initial response content type before accepting message bodies.
4. Add raw-peer tests for explicit status with non-200 HTTP responses, absent status fallback, and missing/incorrect content types.

### 18. P2 — A productive low-ID stream can starve other calls

**Locations:** `protolink/protolink-grpc/src/server.rs:232–236`, `:506–508`, `:639–701`.

Every poll starts with the lowest stream ID, which can fill the connection-wide output budget before later calls are visited. If the peer continually drains and replenishes that stream’s window, later ready streams—and bidirectional request delivery—can be starved indefinitely.

**Implementation plan**
1. Rotate the starting stream between polling passes.
2. Apply a bounded per-call response quantum while retaining the connection-wide output bound.
3. Ensure yielding schedules further work when no other wake source exists.
4. Test an infinite first stream alongside later finite and bidirectional streams, asserting bounded progress while continuously draining the first.

### 19. P2 — Boundary values trigger unchecked arithmetic failures

**Locations:** `protolink/protolink-grpc/src/lpm.rs:18–22`, `:165–203`; `protolink/protolink-grpc/src/server.rs:506–518`; `protolink/protolink/src/tokio.rs:61–64`.

- On 32-bit targets with permissive size configuration, the five-byte prefix `[0, 255, 255, 255, 255]` overflows `HEADER_LEN + len`, leading to a panic or invalid slice.
- The server output-budget calculation can overflow for `usize::MAX`; outbound framing truncates lengths through unchecked `as u32`.
- `TokioTimer` adds arbitrary `Duration` values to an `Instant`; `Duration::MAX` can panic.

These require nondefault inputs/configuration, but the public APIs permit them.

**Implementation plan**
1. Use checked framing arithmetic and checked wire-length conversions; propagate failures rather than truncating or panicking.
2. Validate/document supported size limits and make output-budget arithmetic overflow-safe.
3. Use checked instant conversion, with bounded re-evaluated sleeps for unrepresentable deadlines.
4. Add prefix-only 32-bit tests, configuration-boundary tests without huge allocations, and `Duration::MAX` timer tests.

### 20. P2 — Valid protobuf names can generate uncompilable Rust

**Locations:** `protolink/protolink-grpc-gen/src/lib.rs:289–301`, `:375–392`, `:839–845`.

Service-module names are emitted without Rust keyword escaping: a service named `Type` produces `pub mod type`. RPC collision checks also omit built-in client methods, so an RPC named `New` generates another `new` alongside the constructor.

**Implementation plan**
1. Normalize and escape identifiers by their Rust namespace, including service modules and trait names.
2. Reserve built-in client method names and detect collisions after normalization and package qualification.
3. Add actual compilation fixtures—not only substring assertions—for keyword service names, `New`, `IntoInner`, `TransportMut`, and colliding package-qualified modules.
4. Compile generated server, async-client, and blocking-client fixtures against a `no_std` consumer.

## `no_std` assessment and validation plan

The inspected structure is sensible:

- Runtime crates use `core`/`alloc`, with `std` functionality gated.
- ARQ and COBS default features are disabled.
- Portable atomics are forwarded to both waker implementations.
- Code generation runs on the host; generated code imports runtime-provided allocation/task types rather than `std`.

**Remaining validation gap:** `protolink/.github/workflows/ci.yml:129–161` checks `thumbv7em-none-eabihf`, but not a target without native compare-and-swap. The generated-code example also builds with Tokio, so it does not prove generated downstream code remains embedded-compatible.

**Concrete plan**
1. Add `thumbv6m-none-eabi` checks using `portable-atomic-critical-section`, including async, blocking, and compression combinations.
2. Add a small generated-code `no_std` consumer and cross-compile all RPC shapes.
3. Add a linked embedded smoke fixture supplying an allocator and platform critical-section implementation; `cargo check` alone cannot verify those integration requirements.
4. Preserve feature-isolated checks without dev-dependency feature unification, and run the existing Miri configurations after concurrency changes.

**Recommended order:** fix HPACK allocation bounds, reliable-link framing, and transport/cancellation cleanup first; then deadline/resource accounting; then protocol interoperability and the expanded embedded matrix.
