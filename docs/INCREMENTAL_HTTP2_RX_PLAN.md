# Incremental HTTP/2 receive implementation plan

Status: implemented in the working tree, not committed. Stage 1 (incremental frames, borrowed
DATA, direct gRPC delivery) and stage 2 (incremental HPACK) are in place and gated. The items
still open, and the choices that differ from the text below, are listed under
[Deviations and open items](#deviations-and-open-items). The validation record is at the end.
Parent: [memory footprint plan](MEMORY_PLAN.md), Milestone 2.

## Objective

Remove whole-frame receive accumulation and the owned HTTP/2 DATA-event copy. A connection must
accept legal HTTP/2 frames through small transport reads without allocating storage proportional
to the frame size or to the caller's input batch. Preserve gRPC interoperability, deadlines,
flow-control correctness, and RX progress while TX is blocked on duplex/reliable transports.

Breaking APIs are acceptable. Use one parser and one ownership model, not parallel legacy and
embedded engines. Keep the existing decoder buffer-transfer optimization; incremental RX may
change its hit rate, which must be measured rather than assumed.

Deliver two separately reported stages:

1. **Incremental frames and borrowed DATA:** fixed frame/control state, direct gRPC consumption,
   and one bounded aggregate HPACK block. This already removes DATA frame accumulation and copies.
2. **Incremental HPACK:** remove aggregate encoded header-block storage after the frame/credit
   contracts are stable. Do not claim the first stage eliminates all header-sized allocations.

## Current receive path and change scope

This section describes the tree before the change (commit `6ffb1a6`), which is the baseline for
the measurements below.

Rechecked on-disk sources:

- [`Connection::recv()` and `process_input()`](../protolink-http2/src/lib.rs) append the entire
  supplied slice to `input: Vec<u8>` and wait for complete frames. Consumed prefixes are drained;
  high-water allocation capacity remains.
- `on_data()` copies a complete unpadded body into `Event::Data { data: Vec<u8> }`. The generic
  owned-event queue may retain many frames before the caller drains it.
- `on_headers()` copies encoded headers into `PendingBlock`; HPACK decoding creates owned fields
  and may clone fields into its dynamic table. The encoded block is capped at
  `2 * max_header_list_size + 1024` (17,408 bytes at current defaults).
- [`Lifecycle::recv()`](../protolink-grpc/src/lifecycle.rs) and
  [`Client::recv()`](../protolink-grpc/src/client.rs) first feed HTTP/2, then drain its owned events.
  DATA is copied again into the gRPC decoder.
- [`headers::Body`](../protolink-http2/src/headers.rs) validates full unpadded DATA lengths and
  final content length before current frame bodies are published.
- The [`coordinator`](../protolink/src/coordinator.rs) takes each read result and passes the
  entire inbox to a node. The [`workers`](../protolink/src/driver.rs) can reuse that inbox after
  the result is taken. Partial consumption requires an explicit input lease.
- [`hpack.rs`](../protolink-http2/src/hpack.rs) currently decodes complete blocks; integer,
  string, Huffman, and table-update state is not resumable.

Primary implementation scope: `protolink-http2/src/{lib,headers,hpack,huffman}.rs`,
`protolink-grpc/src/{client,lifecycle,inbound,lpm,server,router}.rs`, and
`protolink/src/{coordinator,driver,asynch,blocking}.rs` where input ownership/EOF propagates.
A new private parser module is reasonable; discover its exact placement during implementation.
Do not mix in TX buffer redesign, new defaults, ARQ tuning, or a general storage abstraction.

## Architecture decisions

### 1. Pull one event at a time, borrowing only caller input

Replace raw HTTP/2's whole-slice `recv`/owned DATA queue model with a resumable pull step. Final
names are an implementation detail, but the type/lifetime contract is not:

- Input: `&mut Connection`, a caller-owned `&'input [u8]`, and an explicit work allowance.
- Output: bytes consumed from that exact slice and either one event, need-input, work-remaining /
  budget-exhausted, or terminal failure. Error outcomes also carry consumed progress.
- DATA contains `&'input [u8]`, never a borrow tied to `&mut Connection`. The caller can release
  capacity, reset a stream, and queue output immediately after receiving the event.
- Commit parser offsets and delivered-byte ownership **before** returning the event. Never store
  the borrowed input pointer/slice inside the connection or across an async suspension.
- Emit a separate DATA-frame completion notice carrying END_STREAM. Body chunks have no terminal
  flag; padding-only and empty frames must still complete. A completion may consume zero bytes,
  but it must advance internal state and occur exactly once.
- HEADERS initially returns an owned, bounded field list at END_HEADERS. Reset/GOAWAY notices are
  small. Process notices immediately instead of accumulating an arbitrary number per input batch.
- Internal work may remain without input: draining GOAWAY's refused streams, locally deferred
  resets, pending credit, frame completion, or ready TX. Distinguish that from need-input; do not
  spin on `consumed == 0`, drop suffixes, or mistakenly read over pending notifications.

Remove the implicit owned DATA queue from the production path. Migrate raw HTTP/2 users/tests.
A synchronous full-slice helper may repeatedly call the same step API without copying input, but
must document its CPU behavior; async drivers use bounded steps, not that helper. Any optional
owned-event collector is explicitly caller-owned and not used by gRPC. Do not keep a second parser.

### 2. Fixed frame state, bounded header ownership

Use a state machine along these lines:

```text
Preface -> FrameHeader -> Payload(kind) -> FrameComplete -> FrameHeader
                                  \-> TerminalFailure
```

The payload state records type, flags, stream ID, remaining lengths, validated body candidate,
and the active-frame credit ledger. Reuse small control scratch across mutually exclusive
payload kinds. Proposed byte-array storage is a nine-byte frame header plus at most eight bytes
of control scratch; preface matching uses an index into the static preface. Measure the complete
enum/type layout separately: counters, padding, and staged SETTINGS are additional RAM.

| Frame kind | Receive treatment |
|---|---|
| DATA | Parse optional Pad Length, deliver borrowed body chunks, skip trailing padding |
| HEADERS | Parse optional Pad Length and five-byte priority prefix; feed HPACK bytes only |
| CONTINUATION | Feed the same pending header block, enforcing its stream/connection lock |
| SETTINGS | Decode six-byte tuples incrementally into constant-size staged settings |
| PING | Eight-byte scratch; acknowledge only a complete valid non-ACK frame |
| RST_STREAM / WINDOW_UPDATE | Four-byte scratch; apply once complete |
| PRIORITY | Five-byte scratch; preserve validation without retaining payload storage |
| GOAWAY | Eight-byte fixed prefix; skip debug data; emit notices with bounded fan-out state |
| Unknown types | Skip declared payload incrementally, subject to startup/CONTINUATION rules |
| PUSH_PROMISE | Preserve unsupported-push connection failure policy |

HTTP/2's 16,384-byte minimum supported frame payload remains unchanged. A smaller transport chunk
is not a smaller advertised frame limit. The initial 65,535-byte connection receive window is
credit, not a reason to allocate a matching buffer.

### 3. Validate early; make completion semantics explicit

- Validate the envelope as soon as its nine bytes are available: frame size, first-frame non-ACK
  SETTINGS requirement, fixed/minimum lengths, SETTINGS divisibility, stream-zero rules, and
  CONTINUATION exclusivity. Audit checks currently performed by the dependency before replacing
  `decode_frame`; do not accidentally lose reserved-bit, padding, or priority validation.
- Define deterministic precedence for simultaneously invalid fields. Preserve existing error
  code/scope tests; document intentional changes from detecting failures earlier. The old parser
  is a regression oracle for supported semantics, not proof of all RFC corner cases.
- Validate padding length before publishing body/HPACK bytes. Exclude Pad Length, priority fields,
  and trailing padding from HTTP body and HPACK accounting.
- Once unpadded DATA length is known, use `Body::checked_data()` once for the entire frame,
  including the intended END_STREAM flag. Reject bodyless/content-length violations before its
  body is delivered. Do not count the frame length again on every chunk.
- Keep the body candidate and terminal transition separate from chunk delivery. Publish remote
  half-close and successful DATA-frame completion only after trailing padding is consumed.
- Delivery is now streaming, not frame-atomic: complete gRPC streaming messages may be delivered
  from a DATA frame that later ends in transport EOF. Those effects cannot be rolled back. Unary
  success/half-close must not be inferred from a partial END_STREAM frame. Document this semantic
  change instead of buffering the whole frame to simulate old atomicity.

### 4. One explicit DATA credit ledger, surviving local cancellation

Reserve/check full declared DATA payload credit once at frame admission, including pad length
and padding, in both automatic and manual modes. Keep HTTP body bytes distinct from wire bytes.
Define these counters/ownership states before editing existing credit helpers:

- frame wire bytes reserved but not yet consumed;
- body bytes actually delivered and still held by the application;
- consumed padding/discarded bytes eligible for return;
- returned or pending-to-return credit, never counted twice.

`stream.unreleased` must represent delivered application-owned bytes, not the whole reserved frame.
Before exposing a chunk, record its delivered ownership so an immediate `release_capacity()` is
valid. The active frame ledger is connection-owned and survives removal of the stream map entry.

On local reset, deadline cancellation, deferred-reset completion, or application removal:

1. Reconcile delivered credit using the current retain/release contract.
2. Change the unread frame tail to discard mode; it is not retained application data.
3. Consume/discard the remaining wire bytes without frame-sized buffering. Return eligible credit
   once as those bytes are consumed; retain enough ledger state to prevent cleanup from returning
   the same reservation a second time.
4. Recheck stream disposition after every application event: callbacks may synchronously cancel it.

Peer frames cannot interleave inside DATA, but local operations can. Test cancellation at the
header, body, padding, and completion boundaries. On terminal transport failure, cancel ownership
once; do not synthesize successful frame completion or pointless WINDOW_UPDATE delivery.

Already-granted credit is a storage commitment. Manual flow control must withhold further credit
before receive budgets are exhausted; partial gRPC-message crediting must still permit messages
larger than a stream window. Do not introduce a resumable pause halfway through authorized DATA
that prevents reaching the peer's WINDOW_UPDATE/ACK needed to unblock TX. Reserve the permitted
storage or reset/discard that stream under an explicit resource-exhaustion policy.

### 5. Coalesce credit and bound control obligations

Without coalescing, one-byte chunks plus gRPC's partial-message crediting can generate two
WINDOW_UPDATE frames per byte and exhaust output storage. Use pending credit counters, flush
thresholds, and a liveness rule. Do not wait solely for frame completion: the peer may be waiting
for credit while an application message or frame is still arriving. Reserve an emergency update
when the remaining window would stall progress, and flush pending credit at quiescence as needed.
Counters remain bounded by protocol window limits and cannot release more than owned credit.

Keep the current serialized-output failure policy during this RX project: exhausted output is
terminal, not an excuse to allocate more. PING ACKs cannot simply be coalesced away. Bound pending
notices/control obligations; a single notice slot plus a resumable stream cursor is preferable to
an arbitrary queue. Mutating APIs that could create another notice must first drain/reserve that
slot or report explicit pressure without losing state. Budget zero-byte fan-out work as well.
Preserve the serialized prefix, sticky failures, and driver flush/deadline obligations.

### 6. SETTINGS is streamed, not partially published

Decode and validate each tuple without a payload-sized array or a `Vec<Setting>`. Retain candidate
settings plus the cumulative and maximum prefix delta for INITIAL_WINDOW_SIZE. Validate duplicate
settings in order; a transient window overflow is still invalid even if a later tuple lowers it.
Preserve negative send windows and unknown-setting handling.

Commit only when the frame is complete. At that cut, validate effects against the currently live
streams, including streams created while the fragmented frame was arriving; previously closed
streams are not resurrected. Apply the staged settings atomically, ACK once, then serialize newly
unblocked output. If work is staged across polls, do not expose partially applied connection-wide
settings; budget this bounded stream scan explicitly. Header-table settings must not corrupt
HPACK state or silently change the separately negotiated receive table.

### 7. Preserve transport input ownership across yielding

Add an unread-suffix cursor to the existing inbox/worker protocol. Keep a read outcome/inbox leased
until its bytes are fully consumed: taking a result must no longer implicitly allow a worker to
overwrite a still-live suffix. Use offsets into existing storage, not a new copy of every read.

Poll TX workers, timers, owner handoff, other lanes, and ARQ progress while a suffix is pending.
Never hold a `Shared::with` borrow or a DATA event across an await/yield. Propagate consumed counts
through node/client/router layers. Apply the same rule to async client-owned reads and blocking
scratch storage; blocking helpers can finish a bounded read synchronously using the same parser.
Retiring a lane drops its lease and partial parser state without double-delivering bytes.

Add EOF finalization to the receive contract and wire it through every driver. Partial preface,
header, payload, header block, or SETTINGS is incomplete; EOF at a frame boundary can still leave
unfinished RPCs. Fail/cancel unfinished calls exactly once, preserve already delivered completed
streaming messages, and never manufacture END_STREAM or successful trailers.

## Implementation milestones and gates

[x] marks an item that is implemented and gated by the tests named in the validation record.
[ ] marks an item that is open or was replaced; the deviations section gives the reason.

### 0. Characterize current behavior and storage

- [x] Differential oracle: `protolink-http2/tests/rx_oracle.rs` runs 40 scenarios (valid, padded,
  CONTINUATION, SETTINGS, GOAWAY, manual flow control, and each protocol error) through the public
  API under 9 chunkings. Events, results and output must not depend on chunking. Its whole-slice
  trace matched the pre-change trace byte for byte.
- [x] Baselines for parser, owned path and gRPC receive, recorded before the change (below).

Gate: measured baseline and explicit state/credit/API contracts precede parser changes.

### 1. Introduce the pull contract and input leases

- [x] Progress and event types: `Step { consumed, event }` returns one event per call, and
  `Event<'a>` borrows DATA body bytes. `DataEnd` carries END_STREAM once per frame.
- [x] Event ordering: notices (HEADERS, RST_STREAM, GOAWAY, flushed resets) are delivered before
  any further input is parsed. A call without an event has used all its input.
- [x] Bounded notice fan-out: a GOAWAY refuses streams one per call, through a cursor, so it queues
  nothing. Queued notices from application calls are bounded by the streams with pending resets.
- [ ] Work budget per step. Not implemented; see deviations.
- [ ] Input leases through coordinator, client and blocking paths. Not needed: each driver read is
  consumed in full before the call returns, so no unread suffix survives an await.

Gate: input is never overwritten or replayed, connection mutation while handling DATA is possible
without unsafe lifetime tricks, and TX and deadlines stay live (the serial duplex matrix passes).

### 2. Implement incremental frames and DATA ownership

- [x] `input: Vec<u8>` removed. The state is the frame header scratch, the frame state (`rx.rs`),
  and the stream table; nothing holds the input.
- [x] Borrowed DATA with padding and discard handled in place, a completion after trailing padding,
  fixed-size controls, staged SETTINGS published once per frame, unknown payloads skipped, and one
  notice per frame.
- [x] Credit: admission-time credit is unchanged from the old code, so WINDOW_UPDATE bytes match.
  An END_STREAM frame credits the connection only. Partial-message credit is returned once per DATA
  frame, at its completion. Returning it per chunk stalled the serial link (see the validation
  record).
- [x] Header fragments go to the HPACK decoder as they arrive (stage 2).

Gate: legal 16 KiB DATA, ignored payloads and control frames allocate nothing (`rx_allocations`).
Header storage is reported separately, below.

### 3. Integrate gRPC and remove owned DATA accumulation

- [x] Client, lifecycle (server) and router drive `recv_step`. Body bytes go straight to the LPM
  decoder. DATA completion handles the client's end-of-response checks and the server's half-close.
- [x] Message size limits and prefix checks are unchanged: the LPM decoder still validates each
  prefix before accepting its message.
- [x] Receive drain points: each `recv` call runs until its input is used up. No queue sits between
  the layers.
- [ ] The owned collector (`recv` with `poll_event`) remains as an explicit, copying convenience.
  No gRPC path uses it; see deviations.

Gate: no HTTP/2-owned DATA payload allocation on the gRPC path. All four RPC shapes work over small
pipes (the duplex matrix passes; the embedded e2e matrix is in the workspace run). The buffer
transfer still happens for segmented delivery: the take allocates 13 bytes in both cases.

### 4. Make HPACK incremental

- [x] Resumable integer, string and Huffman decoding across fragments and CONTINUATION frames.
  Integer limits follow the dependency (at most eight continuation octets, checked `u64` sums).
  Huffman padding is checked at each string's end, and the Huffman state persists across fragments.
- [x] One bounded decoded field list and the negotiated dynamic table. Every declared length is
  checked against the budget before its octets are kept, and the 32-octet field overhead is charged
  before the index is read, as before.
- [x] Decoding happens for discarded and refused streams too, so the compression state stays in step.
- [x] Only table-size updates before the first field are legal (unchanged).
- [x] Encoded octets are not stored. The encoded-size limit is a running total, enforced from
  declared fragment and CONTINUATION sizes before any octet is seen.

Gate: header RX retains no encoded block. Measured: an 8000-octet literal fed one octet at a time
holds its decoded value and name plus the field list (3 allocations, 8,193 B peak, against 8,005
encoded octets); a 20 KiB block over CONTINUATION peaks at 22,126 B, dominated by its decoded value.
This is bounded header storage, not allocation-free HPACK.

### 5. Validate memory, interop, and migration

- [x] Allocation gates (`protolink-http2/tests/rx_allocations.rs`) and the gRPC measurements
  (`protolink-grpc/tests/allocations.rs`), below.
- [x] Chunk-size and batch-size independence: the oracle and `tests/rx_contracts.rs`. Credit
  conservation on cancellation mid-frame is covered by `cancelling_mid_frame_returns_each_byte_of_credit_once`.
- [x] Interop: `scripts/grpcurl-interop.sh` passes against the example server.
- [x] Docs: the crate docs describe `recv_step`, `Event`, and the owned collector. The memory plan
  points here.
- [ ] Miri: not run. The changed crates forbid unsafe code (`#![forbid(unsafe_code)]`).

## Required regression matrix

- Every preface/header split; payload/padding/priority/control-tuple splits; empty input; many
  frames per slice; 1/32/64/128/512-byte transport chunks and randomized partitions.
- Empty and padding-only DATA, large legal DATA, invalid padding, oversized frames, partial
  END_STREAM, and EOF at every meaningful boundary. No terminal notice before trailing padding.
- HEAD/204/304/bodyless and CONNECT rules; content-length overrun/underrun; no double body counting.
- Manual/automatic flow control, initial SETTINGS ACK window changes, window overrun scopes,
  messages larger than windows, retained closed-stream credit, partial-message early credit,
  reset/cancel/deadline at each frame stage, and callbacks releasing credit immediately.
- Duplicate SETTINGS including intermediate overflow then a safe final value, negative windows,
  invalid values late in a frame, stream creation/removal while staged, and exactly one ACK.
- HEADERS/CONTINUATION ordering, stream mismatch, padding/priority stripping, HPACK table updates,
  split integers/strings/Huffman codes, invalid octets followed by dynamic references, and refused
  streams that still affect the shared compression context.
- PING/SETTINGS/empty-DATA floods, unknown frames, long skipped payloads/GOAWAY debug data,
  fan-out notices, output-cap exhaustion, work-budget yielding, and no zero-progress spin.
- Inbox overwrite attempt, partial-read cancellation, client-owner handoff, backend isolation,
  simultaneous read error/TX completion, stalled flush, and reliable-link/ARQ duplex progress.
- Semantic equivalence across slicing, normalizing DATA chunk boundaries and legal credit
  coalescing. Compare byte content, event order, terminal outcomes, and credit totals—not one
  event or one WINDOW_UPDATE per original DATA frame. Introduce deliberate replay/drop/double-credit
  defects temporarily to prove that the new regressions detect them.

Suggested validation commands after corresponding tests are implemented:

```sh
cargo test --offline --locked -p protolink-http2
cargo test --offline --locked -p protolink-grpc --all-features
cargo test --offline --locked -p protolink --all-features
cargo test --offline --locked --workspace --all-features
cargo clippy --offline --locked --workspace --all-features --all-targets -- -D warnings
cargo check --offline --locked --target thumbv6m-none-eabi --lib --no-default-features --features async -p protolink
cargo fmt --all -- --check
```

Also run release-mode allocation tests, existing grpcurl/h2 interoperability, targeted Miri for
changed ownership paths, and bounded concurrency/reliable-link stress tests. Record toolchains,
features, input shapes, and stage-specific remaining allocations in the results.

## Acceptance and execution order

Land 0, then the contract/lease work in 1, then incremental frames/ledger in 2. Coordinate API
migration and gRPC integration in 3; do not leave a shipped path with an unbounded compatibility
DATA queue. HPACK streaming in 4 follows only after frame/credit invariants are stable. Close
with measured gates in 5. Each milestone includes its regressions, not a later testing milestone.

Success means no frame/batch-sized receive allocation, no HTTP/2-owned DATA copy on the gRPC
path, bounded header/notice storage, correct input ownership across yields, and preserved duplex
liveness/credit/error semantics. It does not mean zero allocations for decoded protobuf objects,
zero retained gRPC messages, faster Huffman decoding, or a proven total-RAM budget by itself.

## Validation record

Toolchain: rustc 1.98.1, cargo 1.98.1, offline and locked where the lock file allows it. Baseline and
measurements were taken on the pre-change commit `6ffb1a6` (a throwaway worktree, since removed) and on
this tree.

Tests and gates run on this tree:

- `cargo nextest run --workspace --all-features --no-fail-fast`: 876 passed (852 before, the
  difference being the new gates).
- `cargo test --workspace --doc --all-features`: passes.
- `cargo clippy --workspace --all-features --all-targets -- -D warnings`: clean.
- `cargo fmt --all -- --check`: clean.
- Embedded: `cargo build --target thumbv7em-none-eabihf --no-default-features` for `protolink`,
  `protolink-grpc`, `protolink-http2`, `protolink-i2c` and `protolink-adaptors`; the `async,blocking`
  and `prost` variants; `cargo check --target thumbv6m-none-eabi --lib --no-default-features --features async -p protolink`.
- `cargo hack check --each-feature --no-dev-deps -p protolink-http2 -p protolink-grpc`: passes.
- `RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links -D missing_docs" cargo doc --no-deps` for the two
  crates: clean.
- `cargo nextest run --workspace --all-features -E 'test(/concurrent_/)' --stress-count 30`: 30 of 30.
- `bash scripts/grpcurl-interop.sh`: passes.

Mutation checks (each defect was applied, the named test failed, and the code was restored):

- Huffman padding check removed: `resumable_decoding_matches_the_one_shot_reference` fails.
- Huffman state reset at each fragment: the same test fails.
- Partial-message credit returned per chunk: `bytewise_partial_message_is_credited_once_per_frame`
  sees 32,768 window updates where it expects 2.

Measured (bytes are requested sizes; peak and live are per `protolink-alloc-track`):

| Path | Before (`6ffb1a6`) | After |
|---|---|---|
| HTTP/2 receive, one 16 KiB DATA frame, `recv_step` | owned copy: 2 allocs, peak 32,803 B (input copy 16,393 + body 16,384) | 0 allocs, 0 B |
| Same frame, bytewise input | owned copy: 10 reallocs of the input buffer, peak 45,594 B | 0 allocs, 0 B |
| 64 KiB in four frames, one slice | peak 131,210 B | 0 allocs, 0 B |
| Unknown 16 KiB frame, SETTINGS, PING | peak 16,402 B | 0 allocs, 0 B |
| Same frame, owned `recv` (convenience) | peak 32,803 B | 1 alloc, 16,384 B (body only) |
| 20 KiB header block over CONTINUATION | peak 74,959 B | peak 22,126 B (decoded value) |
| 8000-octet literal, one octet at a time | not measured | 3 allocs, 8,193 B |
| gRPC client unary 1 KiB response, receive | 17 allocs, peak 3,785 B | 14 allocs, peak 2,529 B |
| gRPC client unary 4 KiB response, receive | 17 allocs, peak 13,001 B | 14 allocs, peak 5,601 B |
| gRPC 4 KiB response in 1460-byte segments, receive | 16 allocs, 3 reallocs, peak 14,678 B | 14 allocs, 2 reallocs, peak 9,988 B |
| Take of the 4 KiB response (buffer transfer) | 1 alloc, 13 B | 1 alloc, 13 B |
| LPM one-frame messages returned without allocation | 48/48 | 48/48 (LPM-level, unchanged code) |
| LPM 1460-byte segments returned without allocation | 0/48 | 0/48 (LPM-level, unchanged code) |

The 1-octet-per-piece owned `recv` now costs one allocation per piece by design: each chunk becomes an
owned event on the convenience path.

## Review repair plan

Review reproduced two fragmentation-dependent credit floods: ignored non-gRPC client response
bodies and unread DATA tails after local cancellation. Both can exhaust queued output and fail
unrelated calls. The [discard-credit repair plan](INCREMENTAL_HTTP2_RX_FIXES.md) defines local
per-frame coalescing fixes, permanent failing reproductions, and credit/isolation/memory gates.

Both repairs are now implemented with the two local fixes described there, and each is covered by
a permanent regression that fails on the original per-chunk release (proven by temporarily restoring
each defect):

- **Cancelled DATA tails** (`protolink-http2/src/rx.rs`, `DataMode::Discard { tail: usize }`): the
  unread body length is snapshotted once at the discard transition and returned once at frame
  completion, before the removed-stream and suppressed-event early returns. `cancelled_data_tail_returns_credit_once_when_fed_bytewise`
  in [`rx_contracts`](../protolink-http2/tests/rx_contracts.rs) fails with the reviewed `OutputLimitExceeded { limit: 1024, pending: 1014, attempted: 13 }`
  on the original code; `cancelled_tail_returns_credit_at_frame_completion_not_response_end` proves credit is not held until the whole response.
- **Ignored non-gRPC bodies** (`protolink-grpc/src/client.rs`, `ResponseBuffer::held` + `hold_ignored`):
  delivered-but-ignored credit is held and reconciled once at `DataEnd` through the frame-finish path,
  never per borrowed chunk. `fragmented_non_grpc_error_body_keeps_credit_bounded_and_peer_alive` in
  [`client`](../protolink-grpc/tests/client.rs) fails on the original code at defaults and at a 1 KiB
  buffer across 1/512/16 KiB chunk sizes, while an unrelated call still completes.

The [coverage-completion results](INCREMENTAL_HTTP2_RX_FIXES.md#regression-coverage-completion)
now also record permanent nonterminal window-exhaustion liveness, client interruption/trailer
semantics, parser ownership boundaries, Lifecycle isolation, and allocation gates. Nine isolated
mutations were caught at runtime, including missing frame-finish release that the terminal-only
regression still accepts. Final validation passes 937 workspace tests, strict Clippy, debug/release
allocation suites, and the embedded check; seven documentation examples remain ignored.

These are separate from the still-open items below (the receive work budget and the EOF-in-DATA test);
this patch is per-frame coalescing for the two discard paths only, not a general flow-control redesign.

## Deviations and open items

Choices that differ from the text above, and why:

- **Pull API naming and shape.** `Connection::recv_step(&[u8]) -> Result<Step, Error>`. `Step` has
  `consumed` and `event`. Errors are terminal and carry no consumed count, because a failed connection
  accepts no more input. `Event::Data` holds a `Cow`, so the owned collector and the borrowed path
  share one type.
- **Owned collector kept.** `recv` and `poll_event` copy body bytes into an explicit queue. They are
  not used by gRPC, and they keep raw-HTTP/2 tests and tools simple. The plan asked to remove the
  owned path; this keeps it as a labelled convenience.
- **No work budget.** `recv_step` does not yield after a fixed amount of work. Each driver read is
  consumed in full by one call, and the gRPC layers do no await in between, so no input outlives its
  read. Add a budget if a driver starts yielding inside a batch; the input lease is needed at that point.
- **Credit granularity.** Partial-message credit is returned once per DATA frame (at `DataEnd`), not per
  chunk. Per-chunk credit flooded the transport with window updates that the peer, a serial link that
  does not read during a reply, never drained, and the test `serial_server_cannot_read_while_its_response_is_blocked`
  timed out. This is the plan's coalescing requirement, found by the test.
- **Notice queue.** Notices from application calls (a flushed `reset_stream_after_flush`) wait for the
  next receive, as they did before. The queue is bounded by the streams with pending resets.
- **Error precedence.** Declared sizes are checked when declared. A block whose raw string declares a
  length past both its budget and its end is refused with `Limit` (EnhanceYourCalm) where the old
  one-shot decoder reported `Compression`. Both are fatal. The equivalence test scopes exact comparison
  to well-formed blocks.
- **Huffman.** Still bit at a time with a linear table search. This change did not make it faster.
- **EOF.** Driver behaviour on transport EOF is unchanged: clients retire their calls with "connection
  closed", and the server ends. Decoding never publishes END_STREAM before a frame completes. A test
  that an EOF inside a DATA frame yields no success is still open.
- **Open:** Miri (no unsafe code here); the work budget; the EOF test; `just semver` (the public
  `Event` gained a lifetime, which is a breaking change, allowed by AGENTS.md).
