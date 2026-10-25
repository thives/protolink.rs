# Incremental HTTP/2 receive implementation plan

Status: proposed; none of the parser/API changes below are implemented.
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

### 0. Characterize current behavior and storage

- [ ] Add source-to-consumer ownership/credit diagrams and table-driven frame tests. Record
  equivalent semantic results under complete, bytewise, and randomized slicing.
- [ ] Baseline HTTP/2-only and gRPC client/server peak/live allocations for a 16 KiB DATA frame,
  many frames in one supplied slice, padding, unknown frames, SETTINGS, and continued headers.
- [ ] Reuse [`protolink-alloc-track`](../protolink-alloc-track/src/lib.rs) and existing HTTP/2/gRPC
  allocation test binaries. Its current tracker holds 16 live blocks: split scenarios or extend
  test-only capacity for multi-stream tests; never ignore tracker overflow. Distinguish requested
  bytes from allocator metadata/realloc overlap. Measure inline parser/future sizes separately.

Gate: measured baseline and explicit state/credit/API contracts precede parser changes.

### 1. Introduce the pull contract and input leases

- [ ] Specify progress/event/EOF types, event ordering, zero-consumption transitions, and the work
  budget. Prototype input-only DATA lifetimes in compile-tested gRPC call sites.
- [ ] Add retained-inbox/suffix handling through coordinator/client/blocking paths. Test yield
  after partial consumption while a ready worker tries to read again.
- [ ] Bound notice fan-out and define admission for local mutating APIs. Adapt test helpers without
  silently recreating a production DATA queue. Keep fairness traversal invariants explicit when
  newly decoded headers can admit calls between parser steps.

Gate: input is never overwritten/replayed, connection mutation while handling DATA is possible
without unsafe lifetime tricks, and TX/deadlines remain live with an unread suffix.

### 2. Implement incremental frames and DATA ownership

- [ ] Replace `input: Vec<u8>` with preface/header/payload state and the DATA ledger.
- [ ] Implement borrowed DATA, completion, padding/discard, fixed controls, streamed SETTINGS,
  unknown payload skipping, and bounded GOAWAY/reset notice traversal.
- [ ] Initially append only HPACK fragments to one bounded `PendingBlock`, checking capacity
  before extension and retaining CONTINUATION state after stream cancellation/refusal.
- [ ] Preserve error scopes, output admission, and control ACK timing. Add credit coalescing with
  stalled-peer liveness tests before migrating normal gRPC traffic to chunk events.

Gate: legal 16 KiB DATA, ignored payloads, and control frames need no payload-sized parser buffer.
A supplied multi-frame batch does not get copied internally. Header-block storage is separately
reported, not hidden in that claim.

### 3. Integrate gRPC and remove owned DATA accumulation

- [ ] Drive HTTP/2 one event at a time in client/lifecycle/server/router receive paths. Pass borrowed
  body slices directly to bounded gRPC decoder storage, then drain applicable lifecycle notices.
- [ ] Propagate frame completion independently of chunk presence. Keep response validation,
  deadlines, trailers, unknown routes, compression negotiation, and retained-result credit correct.
- [ ] Bound LPM feed sizes and validate prefixes before accumulating an arbitrarily large caller
  chunk. Do not turn direct feeding into a new unbounded message/event accumulator.
- [ ] Define the receive-event drain points: call startup/cancellation and handler dispatch occur
  in order, with no unbounded queue between layers. Existing call/retention limits remain explicit;
  this work alone does not establish a total protocol heap ceiling.
- [ ] Remove the old raw receive/owned DATA event implementation and migrate all call sites/tests.

Gate: no HTTP/2-owned DATA payload allocation on the gRPC path. All four RPC shapes work over
small pipes while output, peer windows, and flush can be independently blocked. Re-measure the
buffer-transfer fast-path hit rate; do not weaken its exact-capacity guard to mask regressions.

### 4. Make HPACK incremental

- [ ] Add resumable representation/integer/string/Huffman state spanning input chunks and
  CONTINUATION frames. Bound integer lengths/overflow before allocation; distinguish need-input
  from invalid encoding. Validate Huffman padding at each string end, not each transport slice.
- [ ] Keep one bounded raw field-under-construction, bounded decoded fields, and the negotiated
  dynamic table. Enforce field/list/encoded-byte limits before allocation and account capacities.
  Only table-size updates before fields are legal; startup allowance remains 4 KiB as today unless
  a separately tested negotiation change is made.
- [ ] Decode rejected/reset streams too, preserving dynamic-table updates before HTTP/UTF-8
  rejection. Keep header-list overflow connection-fatal unless a real synchronized discard decoder
  is deliberately implemented. Never release the CONTINUATION lock on local stream cancellation.
- [ ] Emit a complete validated field section only at END_HEADERS; remove aggregate encoded-block
  storage without retaining another full encoded copy elsewhere. Preserve the encoded-byte cap
  as a running counter across the whole header block.

Gate: header RX retains no aggregate encoded block. Report the actual peak of field scratch,
decoded list, and dynamic-table clones; this is bounded header storage, not allocation-free HPACK.

### 5. Validate memory, interop, and migration

- [ ] Add allocation/capacity regressions that fail on the old whole-frame/copy path. Separate
  parser-only counters from application/protobuf/HPACK allocations and serialized output.
- [ ] Assert chunk-size/batch-size independence of parser storage, bounded notices, credit
  conservation, cleanup after error/EOF, and no retained high-water input buffer.
- [ ] Update raw HTTP/2 API docs/examples, driver EOF/lease contracts, and the parent memory plan.
  No public minimum-MCU-RAM claim until runtime stack/heap measurements cover a concrete firmware.

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
