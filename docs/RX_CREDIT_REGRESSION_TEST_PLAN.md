# Discard-credit regression coverage completion

Status: implemented and validated (test-only). All five priorities below have permanent coverage;
all nine isolated mutation variants failed at runtime as intended. Test names, allocation measurements,
commands/results, and mutation diagnostics are recorded in the parent's
[regression coverage completion results](INCREMENTAL_HTTP2_RX_FIXES.md#regression-coverage-completion).
Parent: [incremental RX repair plan](INCREMENTAL_HTTP2_RX_FIXES.md).

## Objective and scope

Make permanent the review scenarios that passed in temporary harnesses, and close the specific
blind spot in the current client regression: it uses one END_STREAM DATA frame, so termination
cleanup can hide a missing nonterminal frame-finish credit release.

This is test-only work. Reuse existing fixtures, credit parsers, allocation instrumentation, and
raw-frame helpers. Do not change production flow-control behavior, raise output limits, add sleeps,
introduce a mock parser, or add a generic test framework. Keep the existing original-failure tests.
If a new test exposes a real defect, report/isolate it before expanding implementation scope.

Primary write scopes:

- [`protolink-grpc/tests/client.rs`](../protolink-grpc/tests/client.rs): ignored-body liveness,
  terminal status, metadata, and interruption tests.
- [`protolink-http2/tests/rx_contracts.rs`](../protolink-http2/tests/rx_contracts.rs): parser credit
  ownership, padding/retention/cancellation matrix, exact control-output failure boundaries.
- [`protolink-grpc/src/tests_lifecycle.rs`](../protolink-grpc/src/tests_lifecycle.rs): server-side
  call isolation after cancellation during DATA.
- [`protolink-grpc/tests/allocations.rs`](../protolink-grpc/tests/allocations.rs) and existing
  HTTP/2 allocation tests: focused measurements for ignored bodies and discarded tails.

## Shared testing rules

- Complete SETTINGS exchange, including the client's receive-window setting, before the scenario.
  The smaller window must actually be installed, not merely present in a config struct.
- Drain setup output before measurement. During each measured frame, block TX by leaving output
  queued; only exchange credit at explicitly named frame boundaries. Existing `exchange()` helpers
  drain both directions, so do not call them inside a blocked-TX phase.
- Decode WINDOW_UPDATE increments for connection and stream IDs separately, and count their frames.
  Validate full output consumption in the helper so truncated or extra frames do not pass silently.
- Assert exact credit totals/ownership and bounded frame counts together. A count-only assertion
  accepts a broken implementation that sends no credit; a total-only assertion misses floods.
- Use the real peer's send window/queued byte count to prove progress. `has_stream()` alone is
  insufficient evidence that an unrelated call still works.
- Make failure deterministic with state assertions and bounded exchange loops, not timeouts/sleeps.
- Keep parameter axes readable: separate semantic tests from the larger compact parser matrix.
  Use bytewise/full-frame plus one representative split for larger integration cases, and cover
  1/2/32/64/128/512/full and irregular chunks in a small shared scenario runner.
- Treat fail-all as intentionally connection-wide application failure: unlike local cancellation,
  the other call is expected to fail. Do not accidentally assert survival under `Client::fail_all()`.

## Priority 1 — Nonterminal per-frame credit liveness

Proposed test: `ignored_body_returns_credit_before_terminal_and_unblocks_next_frame` in `client.rs`.

1. Configure a 16,384-byte client stream receive window and 1,024-byte pending-output limit.
   Open ignored-response call A and unrelated call B; cover unary and streaming clients.
2. A receives HTTP 503 with a non-gRPC content type. Feed headers before the measured DATA and drain
   any setup output. A must remain pending with no protobuf messages or retained response payload.
3. Let the real peer send exactly one 16,384-byte DATA frame **without END_STREAM**. Verify its
   stream send window is exhausted. Queue the next frame and prove that its bytes are still blocked.
4. Feed the current frame bytewise without draining client TX. Stop before the final body byte and
   assert no body credit has been returned; then supply the final byte to complete the frame.
5. Before sending any terminal DATA/trailers, assert:
   - A remains pending and no result/message was produced;
   - exactly 16,384 connection-credit bytes and 16,384 stream-credit bytes are queued;
   - no more than one update per applicable window, and no output-budget failure;
   - ignored payload retention remains zero.
6. Forward only those credit bytes to the peer. Verify the queued next frame becomes sendable and
   is serialized; feed it through the same controlled boundary. Do not inject manual WINDOW_UPDATE
   frames or complete the response to manufacture progress.
7. Repeat for at least six frames (98,304 bytes), exceeding both one stream window and the initial
   65,535-byte connection window. Require progress at every frame boundary, not just an eventual
   final total. Keep at most one future frame queued in the test peer.
8. Only afterward send terminal trailers and check A's expected fallback/error status. Complete B
   with a genuine gRPC response and verify the exact payload. No unrelated cancellation is allowed.

Sensitivity requirement: disabling only the invalid-content-type `DataEnd -> finish_frame()` call
must fail this test at the first nonterminal credit assertion, even though termination cleanup is
unchanged. This is the highest-priority missing gate.

## Priority 2 — Client interruption and terminal semantics

### A. Ignored-body interruption

Proposed test: `interrupted_ignored_body_releases_delivered_credit_once` in `client.rs`.

For unary and streaming A, interrupt after 1 and 8,192 body bytes of a nonterminal 16 KiB frame.
Use the same real peer and blocked-TX setup as Priority 1. Keep B open.

- **Cancel:** `client.cancel(A)` releases the delivered prefix once, forgets A, and resets only A.
  Feed the remaining tail bytewise; the parser returns its unread credit once. No message/result
  should reappear for the cancelled call. Complete B successfully.
- **Deadline:** set A's explicit timeout and advance `tick()` to it; B has no expired deadline.
  Assert exactly one DEADLINE_EXCEEDED outcome for A, no lost held bytes, bounded tail output,
  and successful completion of B.
- **Fail-all:** invoke a known failure status after the prefix. Assert delivered-prefix credit is
  released once and both calls receive that status under their existing unary/streaming semantics.
  This models retirement of the transport: do not feed fictitious post-EOF bytes or require the
  connection to remain reusable. Do not credit unread tail bytes as received/completed data.

Snapshot credit before interruption, after cleanup, and after tail completion where applicable.
Separate application-held prefix from parser-owned tail. Repeated cancellation/ticks/result reads
must not release the prefix again. Assert exact reset/terminal-notice counts where meaningful.

### B. Status and metadata after an ignored body

Proposed test: `fragmented_ignored_body_preserves_terminal_status_and_metadata` in `client.rs`.

Cover unary and streaming clients, nonempty bytewise ignored DATA, and:

- error trailers (`grpc-status: 7`, message, and custom metadata) overriding HTTP 503;
- trailers without `grpc-status`, preserving HTTP fallback and metadata;
- success trailers on an invalid content type, preserving rejection rather than inventing success;
- terminal DATA, empty terminal DATA, and padding-only terminal DATA;
- missing content type and one representative wrong type. Existing validation tests already cover
  the larger media-type spelling set; do not duplicate it against every boundary combination.

Inspect A while its body is nonterminal: no messages, no completed result, zero retained payload.
At completion assert the exact code/message/metadata and no duplicate outcome. Credit remains exact;
closed streams receive connection updates only. Padding's admission-time credit is separate from
body credit, so do not require zero total output before frame completion in padded variants.

## Priority 3 — Parser ownership and completion boundary matrix

Extend `rx_contracts.rs` with a compact, named-case runner using the production `recv_step()` API.
Promote the review's temporary checks, but prefer a bounded representative permanent matrix over
thousands of opaque Cartesian cases with no case-specific failure messages.

Required axes/cases:

- manual and automatic flow control;
- retained and non-retained delivered credit;
- unpadded DATA and representative padding (0/7/255), including empty/padding-only frames;
- cancellation after header admission but before the first body byte, mid-body, after the last
  DATA event but before completion, and during trailing padding;
- immediate reset and deferred reset; a deferred reset subsequently aborted after a tail snapshot;
- END_STREAM present/absent;
- full, bytewise, fixed small, and deterministic irregular slicing;
- no prefix release, a partial prefix release, or full prefix release before cancellation; retained
  releases during tail/padding processing and after stream removal.

Each case records expected ownership phases, not only a final sum:

1. **Admission/prefix:** record automatic or padding credit separately and delivered/unreleased bytes.
2. **Cancellation:** cleanup returns only the delivered bytes it owns, or retains them explicitly.
3. **Incomplete tail:** no tail update yet; repeated empty polls produce neither new credit nor
   fabricated DataEnd. Do not accidentally drain completion while trying to stop at a boundary.
4. **Frame completion:** exactly one deferred tail update when owed; zero for automatic or
   admission-credited discards. No padding double count, no update for stream IDs already closed,
   and no extra DATA events from discarded bytes.
5. **Retained release:** return held prefix bytes once; later redundant releases/polls do nothing.
6. **Isolation:** actually send/receive body bytes or complete a response on stream B. Presence in
   the map is not enough. A deferred END_STREAM closure must not manufacture an unnecessary reset.

Add focused output-limit tests: leave exactly 13 bytes for the required tail WINDOW_UPDATE, then
repeat with only 12. The first succeeds; the second preserves the serialized prefix and sticky
failure, and draining output cannot revive it or re-emit tail credit. Incomplete-frame retirement
must not flush a deferred snapshot as if the frame had completed; no new EOF API is required.

## Priority 4 — Server/Lifecycle call isolation

Proposed test: `cancelled_request_tail_does_not_cancel_other_lifecycle_calls` in `tests_lifecycle.rs`.

Use a real client-side HTTP/2 peer and `Lifecycle<()>` with a 1 KiB output limit. Existing `Peers`
helper exchanges automatically and starts half-closed requests, so use a small local setup for this
case rather than changing unrelated fixture semantics.

- Admit request calls A and B with valid gRPC headers; keep A's request side open.
- Start a legal DATA frame on A and deliver one body byte. Choose the prefix so no complete LPM
  message can trigger premature handler behavior.
- End/cancel A through the lifecycle's public API while its request frame is incomplete; do not
  reach into HTTP/2 internals to bypass lifecycle cancellation/credit ownership.
- Feed the rest bytewise without draining lifecycle TX during the tail.
- Assert no connection failure, exact credit, bounded control output, and no cancellation of B.
  Verify A ends once using the lifecycle's existing finish/cancellation semantics; finishing a
  call locally is not automatically a `CallEvent::Cancelled` notification.
- Complete a valid request/response on B, drive it through the real peer, and verify payload and
  final status. Merely asserting `has_call(B)` is insufficient.

One lifecycle integration regression is enough initially; the parser matrix owns lower-level
padding/retention variations. Add a handler-level server test only if lifecycle coverage leaves
an independently relevant dispatch/cancellation behavior untested.

## Priority 5 — Memory measurements and mutation checks

- Extend existing allocation binaries with ignored-body and cancelled-tail scenarios using
  `protolink-alloc-track`. Prepare peer/input before the window; warm only explicitly excluded
  output/queue capacity. Assert no payload-sized allocation and no allocation/retained-storage
  growth with fragment count. Protocol/control bookkeeping allocations must be identified, not
  mislabeled as payload copies. Measure inline state separately from heap.
- Keep tracker capacity/overflow checks active. Do not turn the broad parser matrix into an
  allocation benchmark for its own event-collection test helpers.
- Demonstrate sensitivity with isolated mutations, one at a time, in a disposable copy/worktree
  or otherwise safely isolated from user changes. Never stash/reset the user's working tree:
  1. Remove ignored-body frame-finish release while keeping terminal cleanup: Priority 1 fails.
  2. Restore per-chunk ignored-body release: blocked-TX/count assertions fail.
  3. Restore per-chunk tail release: bytewise parser and lifecycle isolation regressions fail.
  4. Suppress the tail release: exact boundary credit/progress assertions fail.
  5. Return the tail twice, include padding, or record automatic-mode tail credit: ownership
     matrix fails for the corresponding named case.
  6. Move tail release after the removed-stream/`!publish` early returns: removed-stream case fails.
- Record the exact test and assertion that killed each mutation; compilation failure alone does
  not demonstrate a useful regression. Restore the correct implementation after each experiment.

## Execution, validation, and acceptance

Implement Priority 1 first, then client cleanup/status tests and parser matrix in independent file
scopes, then lifecycle isolation and focused measurements. Production fixes should not be edited
merely to make the tests easy. Keep fixtures local and cases deterministic with descriptive labels.

Run the targeted tests first, then broaden:

```sh
cargo test --offline --locked -p protolink-grpc --test client
cargo test --offline --locked -p protolink-http2 --test rx_contracts
cargo test --offline --locked -p protolink-grpc --lib tests_lifecycle
cargo test --offline --locked -p protolink-grpc --test allocations
cargo test --offline --locked -p protolink-http2 --test rx_allocations
cargo test --offline --locked --workspace --all-features
cargo clippy --offline --locked --workspace --all-features --all-targets -- -D warnings
cargo check --offline --locked --target thumbv6m-none-eabi --lib --no-default-features --features async -p protolink
cargo fmt --all -- --check
```

Also run relevant allocation tests in release mode and existing bounded serial/duplex/reliable-link
regressions. Update the parent repair checklist/results with test names, actual commands/results,
mutation evidence, and remaining omissions. Temporary review harness success is background evidence,
not permanent CI coverage; do not mark items complete until the tests land and run.

Acceptance: missing nonterminal frame release, per-fragment credit floods, lost/double tail credit,
padding/automatic-credit duplication, and unrelated-call failure all have permanent tests that
would detect those defects. No sleeps, raised resource limits, new dependencies, or changes to
production credit behavior are required to close the identified coverage gaps.
