# Incremental HTTP/2 RX: discard-credit repair plan

Status: implemented. The review reproduced both failures, the two local fixes below are applied,
and the permanent regressions named in the milestones now fail on the original per-chunk release and
pass on the fix. Parent: [incremental RX plan](INCREMENTAL_HTTP2_RX_PLAN.md).

## Scope and acceptance contract

Repair the two fragmentation-dependent credit floods without increasing output limits, restoring
frame-sized buffers, changing the borrowed-event API, or redesigning general flow control.

For an identical valid wire sequence and application cancellation point, transport chunking must
not multiply WINDOW_UPDATE output. A non-gRPC response or a cancelled stream must not exhaust the
connection's output budget merely because its DATA frame arrives in small pieces, failing unrelated
calls. Return each byte of credit exactly once, with no updates for zero bytes or closed stream IDs.

The current per-frame policy is sufficient for these two paths: a legal sender already has credit
for the whole declared DATA frame. It does not need more HTTP/2 credit to finish that frame. Keep
RX/ARQ polling active while TX is blocked. Do not make the implementation wait for the end of the
whole response; return credit after each complete frame so multi-frame error bodies keep moving.

Out of scope: receive work budgets, input leases, new EOF-finalization APIs, HPACK, changing public
`release_capacity()` behavior, and arbitrary callers that deliberately release one byte at a time.
Those remain independent work; do not describe this patch as global credit coalescing.

## Review findings and baseline reproductions

### F1 — Ignored non-gRPC response bodies release credit per borrowed chunk

[`client.rs`](../protolink-grpc/src/client.rs), `Client::on_event`, invalid-content-type DATA branch:
`release_capacity(stream_id, data.len())` now runs for each transport fragment.

Reproduction: two active unary calls, complete handshake, respond to A with HTTP 503 and
`content-type: text/html`, then one legal 16,384-byte DATA frame with END_STREAM; do not drain
client TX during its body.

- Whole frame: 26 bytes of output; B remains pending.
- Bytewise: default 256 KiB output budget is exhausted after approximately 10,083 body bytes;
  both A and B fail with the connection.

### F2 — Cancelled DATA tails return connection credit per discarded chunk

[`rx.rs`](../protolink-http2/src/rx.rs), `payload()`, `DataMode::Discard { tail, .. }`:
`release_connection(n)` serializes one WINDOW_UPDATE per fragment after local cancellation.

Reproduction: manual-flow server, 1,024-byte output limit, streams 1 and 3, a 16,384-byte DATA frame
on stream 1. Deliver one body byte, cancel stream 1, then feed its remaining 16,383 bytes without
writing queued TX.

- Whole tail: 39 bytes of output; stream 3 remains usable.
- Bytewise: 26 bytes immediately after reset, then 76 tail bytes fit; the 77th fails with
  `OutputLimitExceeded { limit: 1024, pending: 1014, attempted: 13 }`.
- Through gRPC Lifecycle, the same pattern also cancels the unrelated call on stream 3.

These figures are review baselines, not fixed required post-fix byte counts for every variant.
For example, after END_STREAM, credit can legitimately be connection-only rather than both
stream and connection. Tests must check correct credit ownership and a bounded frame count.

## Recommended design: two local fixes, no new queues

### A. Reuse client receive-credit bookkeeping for ignored bodies

Primary file: `protolink-grpc/src/client.rs`, `ResponseBuffer` and `Client::on_event`.

1. Add a small helper that records ignored body-byte credit without copying the bytes into the
   LPM decoder or allocating a message. Prefer reusing `ResponseBuffer::held`, whose existing
   cleanup already knows how to release outstanding delivered bytes, rather than adding another
   per-call counter with a parallel cancellation lifecycle.
2. On invalid-content-type DATA, record `data.len()` instead of calling `release_capacity()`.
   Do not parse HTML/JSON as protobuf, create retained messages, or charge ignored bytes as
   retained payload memory. Only the scalar credit ownership is held.
3. On its `DataEnd`, reconcile the ignored bytes through the frame-finish credit path before
   handling END_STREAM/fallback termination. With no decoded messages or partial input, this
   releases all ignored body bytes and resets the scalar bookkeeping.
4. Preserve terminal semantics: HTTP fallback stays provisional until terminal DATA/trailers;
   explicit terminal gRPC error status and metadata retain their existing precedence. Do not
   terminate an invalid-content-type response early just to stop the credit flood.
5. Cancellation/deadline/`fail_all` before DataEnd must release the held delivered bytes once via
   existing discard/termination cleanup. If HTTP/2 already removed the stream, retained receive
   capacity must still resolve the same delivered bytes at connection level.
6. Audit adjacent DATA discard branches: missing call, completed call, DATA before headers, and
   retention-budget failure. A branch that immediately resets once can release its current chunk
   once; the parser fix handles the remaining tail. Any branch that can repeatedly discard while
   leaving the stream open must use bounded deferred credit too. Do not broaden the patch into
   unrelated response validation changes.

Check the reuse invariant explicitly: invalid-content-type calls have no decoded messages or LPM
partial payload. Deferred credit must not accidentally enter `credited` as nonexistent protobuf
bytes or contaminate normal response accounting. The existing frame-finish path should leave
`held == 0`, `credited == 0`, and no retained message allocation after each ignored frame.

### B. Snapshot unread cancelled-tail credit; return it once on frame completion

Primary file: `protolink-http2/src/rx.rs`, DATA mode transition, discard consumption, and completion.

1. When a delivering frame first becomes locally cancelled/discarded, the unread unpadded body
   length is already known as `Part::Body { remaining }`. Record that amount once as deferred
   connection credit in the frame's discard state when manual flow control withheld it. Automatic
   mode records zero: its frame credit was already returned.
2. Replace the current `tail: bool` behavior with an explicit amount, or equivalent constant-size
   state. Snapshotting `remaining` avoids per-chunk increments and counters elsewhere. Discard
   consumption advances input/parser positions only; it does not serialize WINDOW_UPDATE.
3. At full DATA-frame completion, return that deferred amount once, independently of stream-map
   presence and whether a completion event will be published. This must happen **before** the
   existing `!publish` early return and before looking up a possibly removed stream.
4. Preserve `publish` semantics for deferred resets and END_STREAM. A cancelled/discarded body
   stays silent; do not introduce a new DataEnd event just to flush credit.
5. Leave admission-time discard paths distinct: a frame already credited because it arrived on
   a closed/refused stream, or one already credited in automatic mode, records zero deferred
   amount. Never return its entire declared length a second time.
6. Trailing padding/Pad Length already follow their own admission-time credit rule. Snapshot only
   unread **body** bytes, not `frame.left` including padding. Cancellation after the final body
   byte has no unread body credit to return.
7. On incomplete-frame EOF or a terminal parser/output failure, do not flush the snapshot as if
   the frame completed and do not publish END_STREAM. The connection is retired; no future input
   can reuse its pending reservation. Returning credit must not resurrect the connection.

No generic map, heap buffer, or queue is needed. Measure `Frame` layout before/after: replacing a
Boolean with a scalar may affect enum alignment even when the source change looks small.

## Credit ownership invariants

For a manually flow-controlled cancelled frame, partition body bytes into disjoint groups:

- delivered and still owned by application/client bookkeeping;
- delivered and already returned;
- unread tail owned by the parser's deferred snapshot;
- tail already returned once at frame completion.

Cleanup owns only delivered bytes. The parser snapshot owns only the unread body at the transition.
Padding has separate existing accounting. Removing the stream must not lose parser-owned credit;
retaining delivered credit after removal must not also retain the unread tail.

For ignored client bodies, every chunk is delivered to the client, so the parser does **not** own
its credit. The client holds it until DataEnd or termination. When termination switches the parser
to discard mode, it snapshots only bytes not yet delivered. The two fixes must therefore compose
without either double-returning or losing credit.

## Test-first milestones

### 1. Add permanent failing reproductions

- [x] F1 in [`protolink-grpc/tests/client.rs`](../protolink-grpc/tests/client.rs): A receives a
  fragmented non-gRPC error body, B stays active, TX is blocked. Compare whole-frame and bytewise
  input. Check no connection failure, bounded WINDOW_UPDATE output, A's expected final status,
  and a subsequent successful response for B. Run at defaults and at a 1 KiB output budget with
  setup output drained before the measured body.
- [x] F2 in [`rx_contracts.rs`](../protolink-http2/tests/rx_contracts.rs): two streams, manual flow,
  1 KiB output budget, cancel after one body byte, then bytewise tail. The original implementation
  must fail this new regression. Check at most one tail connection update, exact total credit,
  exactly one reset notice, no discarded DATA/DataEnd events, and a usable second stream.
- [x] Add the Lifecycle/server-level F2 isolation regression in the existing gRPC server/lifecycle
  test suite: the unrelated call remains active and can finish; it is not spuriously cancelled.

The current cancellation-credit regression feeds the tail in one slice and checks only totals;
it does not detect the amplification. Keep that coverage and extend it with output-frame bounds.

### 2. Implement and gate parser tail coalescing

- [x] Apply B and run F2 first. For the isolated unpadded raw reproduction, cancellation output
  stays constant across chunkings (the reviewed whole-tail case is 39 bytes, now three frames),
  not grow with the 16,383 discarded bytes.
- [x] Cover manual and automatic flow, retained and non-retained delivered credit, cancellation
  before the first body byte / in the middle / after the final body byte / during padding, and
  deferred-reset transitions. Assert totals as well as counts.
- [x] Verify no flush occurs from repeated empty-input polls, and no duplicate credit is emitted
  when frame completion produces no event or the stream has already been removed.

### 3. Implement and gate client ignored-body coalescing

- [x] Apply A, then run F1 and the existing content-type/HTTP fallback/terminal-status regressions.
- [x] Cover END_STREAM DATA and nonterminal DATA followed by trailers; empty and padding-only
  frames; multiple frames; missing/wrong content type; unary and streaming clients.
- [x] Preserve explicit terminal gRPC errors and trailer metadata after an ignored body. Without
  terminal gRPC status, preserve the existing fallback result. No successful protobuf message
  may be manufactured from a non-gRPC body.
- [x] Cancel, expire, and `fail_all` while ignored bytes are held. Verify delivered-byte credit is
  released once; the parser returns the unread tail once; unrelated calls survive local cancellation.

### 4. Combined liveness, memory, and failure gates

- [x] Test chunk sizes 1/2/32/64/128/512/full-frame and representative irregular splits. Do not
  drain TX during each measured frame; only drain setup output. Compare credit totals and bounded
  update counts, not incidental stream-vs-connection update timing at closure.
- [x] Test several legal frames with a small stream window and controlled peer writes. Feed each
  completed frame's updates back to the peer to demonstrate continued progress. Per-frame
  withholding must not become whole-response withholding or depend on another stream's traffic.
- [x] Keep real output-cap failure semantics: a budget genuinely too small for required control
  frames still fails terminally with its serialized prefix intact. Do not raise the cap, suppress
  required credit, ignore `OutputLimitExceeded`, or claim arbitrary blocked traffic never fills TX.
- [x] Reuse existing allocation instrumentation: ignored bytes create no payload allocations;
  tail handling adds only fixed scalar state. No credit buffer/map may grow with chunk count.
- [x] Prove test sensitivity by temporarily restoring each original per-chunk release: F1 and
  F2 must fail independently. Restore the fix immediately; do not commit deliberate defects.

### 5. Validation and documentation

Start targeted, then broaden:

```sh
cargo test --offline --locked -p protolink-http2 --test rx_contracts
cargo test --offline --locked -p protolink-grpc --test client
cargo test --offline --locked -p protolink-grpc --all-features
cargo test --offline --locked --workspace --all-features
cargo clippy --offline --locked --workspace --all-features --all-targets -- -D warnings
cargo check --offline --locked --target thumbv6m-none-eabi --lib --no-default-features --features async -p protolink
cargo fmt --all -- --check
```

Run existing RX allocation/oracle tests, release-mode allocation tests, and bounded serial/duplex/
reliable-link regressions too. Actual commands and results are recorded below.

- [x] Update `DataMode` and client credit-helper comments to describe the new ownership/timing.
- [x] Add the two repair results to the parent RX plan, distinguishing regressions fixed here from
  its separately deferred work-budget/EOF items. Correct the existing cancellation test comment
  that says tail credit is returned as each fragment arrives.

## Results

Both local fixes are applied and each is gated by a permanent regression that fails on the
original per-chunk release and passes on the fix (the defect was temporarily restored and the
named test failed, then the fix was restored, as required by milestone 4):

- `rx.rs`: `DataMode::Discard { publish, tail: usize }`. The unread body length is snapshotted
  once at the discard transition (`tail: manual ? remaining : 0`) and returned once in `finish_data`,
  before the `!publish` early return and before any removed-stream lookup. Discard consumption now
  advances positions only. `admit_data` closed/refused/reset-after-flush discards record `tail: 0`,
  and automatic mode records zero, so nothing is returned twice.
- `client.rs`: `ResponseBuffer::hold_ignored` accumulates ignored-body credit in `held`; the invalid
  `content-type` DATA branch records instead of releasing, and `DataEnd` calls `finish_frame` to reconcile
  once before terminal handling. Cancellation/deadline/`fail_all` still release the held delivered bytes
  once through the existing `discard_partial`/`discard` cleanup.

Regressions: `cancelled_data_tail_returns_credit_once_when_fed_bytewise`,
`cancelled_tail_returns_credit_at_frame_completion_not_response_end` (both in `rx_contracts`), and
`fragmented_non_grpc_error_body_keeps_credit_bounded_and_peer_alive` (in `client`). The extended
`cancelling_mid_frame_returns_each_byte_of_credit_once` now also bounds output frames.

Validation on this tree: `cargo test --offline --locked --workspace --all-features` (all green),
`cargo clippy --offline --locked --workspace --all-features --all-targets -- -D warnings` (clean),
`cargo fmt --all -- --check` (clean), and the `thumbv6m-none-eabi --lib --no-default-features --features
async -p protolink` embedded gate (builds). The existing `rx_oracle` and allocation tests still pass, and
ignored bytes add no payload allocations (the tail fix adds only a fixed scalar field).

## Regression coverage completion

The [regression coverage completion plan](RX_CREDIT_REGRESSION_TEST_PLAN.md) is implemented and
validated. This follow-up changes tests/documentation only; the production fixes described above
were left intact. No output limits were raised, dependencies added, or sleeps introduced.

### Permanent gates

- **Nonterminal liveness:** `ignored_body_returns_credit_before_terminal_and_unblocks_next_frame`
  in [`client.rs`](../protolink-grpc/tests/client.rs). Unary and streaming A each receive six
  nonterminal 16,384-byte frames (98,304 bytes, beyond the initial connection window). The next
  frame is queued before the current frame arrives and remains blocked until exactly one update
  per window is forwarded. Every iteration feeds the peer's newly serialized frame. TX is blocked
  until the boundary, A stays pending without retained payload, and B completes a real response.
  Fixed chunks 1/2/32/64/128/512/full and deterministic irregular cuts share the same runner.
- **Client cleanup/status:** `interrupted_ignored_body_releases_delivered_credit_once` covers
  cancel/deadline/fail-all after 1 or 8,192 delivered bytes, unary/streaming calls, once-only prefix
  cleanup, parser-owned tail credit, and B's survival (or intentional failure under fail-all).
  `fragmented_ignored_body_preserves_terminal_status_and_metadata` covers explicit error/statusless/
  success trailers, HTTP fallback, wrong/missing content type, metadata, and empty/padded terminal
  DATA. Its helpers also separate padding admission credit from body-completion credit.
- **Parser ownership:** `cancelled_data_frame_owns_credit_phase_by_phase` in
  [`rx_contracts.rs`](../protolink-http2/tests/rx_contracts.rs) runs 64 named cases across manual/
  automatic flow, retained credit, padding 0/7/255, END_STREAM, admission/mid-body cancellation,
  prefix releases, and representative fixed/whole/bytewise/irregular cuts. It asserts exact update
  vectors, silent incomplete-tail/empty polls, once-only retained release, and a real response on B.
  Focused tests cover the boundaries outside that grid:
  - `empty_and_padding_only_frames_owe_their_padding_once`
  - `cancelling_during_trailing_padding_owes_nothing_extra`
  - `cancelling_after_the_last_unpadded_data_event_suppresses_data_end`
  - `deferred_reset_credits_the_tail_and_closes_without_a_reset`
  - `aborting_a_deferred_reset_preserves_its_tail_snapshot_and_retained_prefix` (prefix releases
    during tail and padding, followed by immediate removal without losing the original snapshot)
  - `a_frame_on_a_removed_stream_is_credited_whole_at_admission`
  - `the_deferred_tail_update_needs_its_whole_frame` (exactly 13 bytes of room succeeds; 12 fails
    atomically, preserves the serialized prefix, and stays failed after draining)
  - `an_unfinished_frame_never_flushes_its_snapshot_on_retirement` (real GOAWAY with no streams,
    no invented completion/credit during repeated empty polls)
- **Lifecycle isolation:** `cancelled_request_tail_does_not_cancel_other_lifecycle_calls` in
  [`tests_lifecycle.rs`](../protolink-grpc/src/tests_lifecycle.rs) finishes A mid-frame, leaves A's
  response/reset queued under a 1 KiB cap, consumes its tail bytewise, and checks exact prefix/tail
  credit and one reset. B then takes its request and delivers an intact response and success
  trailers through the real peer. Neither call gets a spurious `CallEvent::Cancelled`.
- **Allocation gates:** `ignored_response_body_copies_nothing_however_it_is_cut` in
  [`allocations.rs`](../protolink-grpc/tests/allocations.rs) separates header/body/trailer phases;
  ignored DATA allocates/reallocates nothing and adds no live or transient heap storage. Header/
  trailer bookkeeping (8/9 allocations on this host) is invariant across 16/16,384-byte bodies
  and full/1024/64/bytewise cuts. `cancelled_data_tail_allocates_nothing_however_it_is_cut` in
  [`rx_allocations.rs`](../protolink-http2/tests/rx_allocations.rs) measures zero allocations,
  reallocations, largest request, live and peak heap bytes for 1/16 KiB bodies cut full/512/bytewise,
  plus the exact one-frame tail update. Inputs/admission/reset are prepared before measurement;
  only output capacity is warmed. Tracker overflow checks remain active. Inline sizes are reported
  separately (Client 896 B, Connection 696 B on this host), not mislabeled as heap allocations.

### Mutation evidence

Each mutation was applied alone to a disposable archive copy under `/tmp/rx-mutation-checks/m1`,
never to the working tree. The saved correct source was restored between experiments. All nine
variants compiled and failed at runtime; compilation failure was not counted. After restoration,
all 23 parser contracts, 29 client tests, and 9 Lifecycle tests passed in the disposable copy.

Commands used the copy's workspace explicitly:

```sh
cargo test --offline --locked --manifest-path /tmp/rx-mutation-checks/m1/Cargo.toml -p protolink-grpc --test client
cargo test --offline --locked --manifest-path /tmp/rx-mutation-checks/m1/Cargo.toml -p protolink-http2 --test rx_contracts
cargo test --offline --locked --manifest-path /tmp/rx-mutation-checks/m1/Cargo.toml -p protolink-grpc --lib tests_lifecycle
```

For focused runs, append the test name below (the Lifecycle test can be selected by its suffix).
`matrix` below abbreviates `cancelled_data_frame_owns_credit_phase_by_phase`; its diagnostic
includes the complete named scenario.

| Isolated defect | Test that killed it | Observed failure |
| --- | --- | --- |
| Remove only ignored-body `DataEnd -> finish_frame`; leave terminal cleanup intact | `ignored_body_returns_credit_before_terminal_and_unblocks_next_frame` | `unary=true chunk=1 frame 0 boundary credit`: `(0,0,0,0)` instead of `(16384,1,16384,1)`. The existing terminal-only `fragmented_non_grpc_error_body_keeps_credit_bounded_and_peer_alive` still passed in the same mutated client suite. |
| Replace `hold_ignored` with per-fragment `release_capacity` | Same liveness test | Receive unwrap fails with `OutputLimitExceeded { limit: 1024, pending: 1014, attempted: 13 }`. |
| Restore per-fragment discarded-tail release with no deferred snapshot | `cancelled_data_tail_returns_credit_once_when_fed_bytewise`; Lifecycle isolation test | Parser receive fails at the same 1 KiB cap; Lifecycle's `tail incomplete` output-length assertion sees 83 instead of 70 bytes after the first discarded byte. |
| Suppress `finish_data` tail release | `matrix` | Manual, retained, unpadded, nonterminal, admission cancel: `one deferred tail update, and automatic mode owes none` sees `[]` instead of `[40]`. |
| Release tail twice | `matrix` | Same case/assertion: `[40,40]` instead of `[40]`. |
| Snapshot `frame.left` (body plus padding) instead of remaining body | `matrix` | Manual, retained, pad=7, nonterminal, admission cancel: `[47]` instead of `[40]`. |
| Snapshot a tail in automatic mode too | `matrix` | Automatic, retained, unpadded, nonterminal, admission cancel: `[40]` instead of `[]`. |
| Move release below the `!publish` early return | `matrix` | Manual, retained, unpadded, nonterminal, admission cancel: `[]` instead of `[40]`. |
| Move release below the removed-stream lookup | `aborting_a_deferred_reset_preserves_its_tail_snapshot_and_retained_prefix` | `abort_in_padding=false end_stream=false: original snapshot once`: `[]` instead of `[34]`. |

### Final validation

All commands below passed on the completed test tree (offline and locked where applicable):

| Command | Result |
| --- | --- |
| `cargo test --offline --locked -p protolink-grpc --test client` | 29 passed |
| `cargo test --offline --locked -p protolink-http2 --test rx_contracts` | 23 passed |
| `cargo test --offline --locked -p protolink-grpc --lib tests_lifecycle` | 9 passed |
| `cargo test --offline --locked -p protolink-grpc --test allocations` | 13 passed |
| `cargo test --offline --locked -p protolink-http2 --test rx_allocations` | 9 passed |
| `cargo test --offline --locked --release -p protolink-grpc --test allocations` | 13 passed |
| `cargo test --offline --locked --release -p protolink-http2 --test rx_allocations` | 9 passed |
| `cargo test --offline --locked --workspace --all-features` | 937 passed, 0 failed; 7 ignored documentation examples |
| `cargo clippy --offline --locked --workspace --all-features --all-targets -- -D warnings` | Clean |
| `cargo check --offline --locked --target thumbv6m-none-eabi --lib --no-default-features --features async -p protolink` | Passed |
| `cargo fmt --all -- --check` | Clean |
| `git diff --check` | Clean |

Workspace validation includes the RX oracle `chunking_does_not_change_events_or_output`,
`serial_server_cannot_read_while_its_response_is_blocked`,
`duplex_server_handles_a_request_while_its_response_is_blocked`, `ready_input_obeys_work_budget`,
`small_pipe_matrix_raw_duplex`, `small_pipe_matrix_coordinated_reliable`, `reliable_link_cobs_arq`,
and reliable-client/server, I2C and UART transfers exceeding their link/engine buffer capacities.

No coverage item from this discard-credit plan remains open. This is not a new EOF API, an arbitrary
blocked-traffic liveness guarantee, a full Cartesian product of every slicing/release axis, or a
solution to the broader work-budget/EOF items separately deferred in the parent RX plan.

## Completion criteria

Both original failure scenarios pass without increasing output limits. Fragmentation no longer
changes the two discard paths from a bounded per-frame obligation into per-byte output. Credits,
terminal statuses, metadata, cancellation notices, and unrelated-call isolation remain correct.
No new payload storage, unbounded collections, production unsafe code, or unrelated parser changes.
