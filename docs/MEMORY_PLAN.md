# Memory footprint optimization plan

Status: execution started with the focused allocation removals below. The broader milestones and
RAM targets remain unimplemented/unverified. Breaking API changes are acceptable.

## Objective and scope

Make the default Protolink endpoint small and predictably bounded, not merely configurable to
accept less traffic. Optimize in this order:

1. Peak total RAM, including transient allocations and driver/future storage.
2. Retained RAM after traffic, cancellation, and connection churn.
3. Allocation count and fragmentation.
4. Linked flash size, without trading away the RAM bound.

Keep direct HTTP/2 + gRPC compatibility, all four RPC shapes, deadlines, and duplex transport
progress. Scaling to larger messages or more streams must require explicit resource limits,
not another implementation. Compression, routers, and reliable links are separately budgeted.
A compact non-HTTP/2 wire protocol is outside this plan.

Do not replace all collections with large inline arrays: that can increase idle RAM, task sizes,
and stack movement. Do not add a general allocator/storage framework before measurements show
it is needed. Prefer fewer owners, bounded admission, and small reusable storage.

## First implemented increment

- HTTP/2 DATA serialization borrows the queued payload directly instead of allocating a temporary
  DATA chunk. Output admission, frame boundaries, send windows, and END_STREAM behavior are unchanged.
- Flushing queued streams uses an ordered cursor rather than allocating a stream-ID snapshot on
  each receive pass. The cursor tolerates stream removal during flushing.
- [`allocations.rs`](../protolink-http2/tests/allocations.rs) measures endpoint-thread allocation
  counts, excluding setup/peer storage. After warming existing output/queue capacity, sends of
  0/1/4096/16384/20000-byte payloads allocate no scratch storage. Repeated flushes of a blocked
  stream also allocate nothing. Both regressions failed before these changes and pass afterward.
- These checks do not measure peak/live heap or prove the endpoint RAM targets. The owned payload,
  serialized output, receive buffers, and larger architectural work remain.

## Next scoped increment

The [decoder buffer-transfer plan](BUFFER_TRANSFER_PLAN.md) is implemented. A single nonempty
uncompressed message in an exactly sized buffer is returned without copying, which removes one
payload allocation and its simultaneous bytes. The returned vector keeps five bytes of spare
capacity, inside the client's existing framed-byte charge. Measured on a 1 KiB eligible message,
the decoder's extraction now makes no allocation, and its local peak falls from 2,053 B to 1,029 B.
Segmented arrival does not qualify under the exact-capacity gate. Details and open items are in the
[results](BUFFER_TRANSFER_PLAN.md#results). This does not replace the broader incremental-RX work.

## Current evidence

Rechecked source locations:

- [`Config`](../protolink-http2/src/lib.rs): eight incoming streams, 65,535-byte stream windows,
  8 KiB decoded header lists, and a 256 KiB serialized-output limit.
- [`ServerConfig`](../protolink-grpc/src/server.rs): default connection receive window is
  `8 * 65_535`; the message limit is 4 KiB.
- [`ClientConfig`](../protolink-grpc/src/client.rs): 1 MiB response budget and 64 retained calls.
- [`Connection`](../protolink-http2/src/lib.rs): whole-frame input accumulation, owned event
  queue, tree-based stream bookkeeping, and contiguous serialized output.
- [`Decoder`](../protolink-grpc/src/lpm.rs): copies incoming bytes into an aggregate buffer. An
  uncompressed message that exactly fills an exactly sized buffer is handed over in that buffer;
  any other uncompressed message is copied again. It already trims/releases storage; preserve
  that behavior unless a measured reuse policy is better.
- [`Encode`](../protolink-grpc/src/codec.rs): produces an allocated protobuf `Vec` before
  transport admission. HTTP/2 copies payload bytes into serialized output, but no longer allocates
  a temporary DATA chunk first.
- [`Lane`](../protolink/src/coordinator.rs): boxed worker future; [`Port`](../protolink/src/driver.rs)
  duplicates worker RX/TX storage. Driver chunks are currently 512 bytes.
- [`link`](../protolink/src/link.rs): documented reliable-stack size is approximately 10.5 KiB
  on x86-64, largely ARQ windows. This is not an ARM measurement.
- [`tests_sizing.rs`](../protolink-grpc/src/tests_sizing.rs) measures serialized output length,
  not total allocation capacity, stack use, or the combined memory footprint.

Previous assessment's local ARM build evidence: the existing combined async/blocking smoke image
exceeded its 256 KiB flash region by 3,912 bytes with its normal release profile. With
`opt-level=z` and LTO it linked at 124.1 KiB, or 127.7 KiB with UART and 132.3 KiB with gzip decoding.
These are combined-client/server stub fixtures, not minimal endpoints or runtime RAM measurements.
Reproduce with recorded compiler/toolchain versions before using them as regression baselines.

Large limits are not preallocated reservations. Lowering them reduces permitted buffering, but
only allocation measurements can demonstrate savings on the same workload.

## Proposed default resource contract

These are starting values to validate, not claimed working defaults or measured requirements.
Use one lean implementation; applications explicitly raise limits when necessary.

| Resource | Proposed default |
|---|---:|
| Active calls / accepted concurrent streams | 1 |
| Maximum encoded protobuf message | 1,024 bytes |
| Stream receive window | 1,024 bytes |
| Retained client calls, including unread finished results | 1 |
| Client retained-response accounting budget | 4 KiB |
| Decoded header-list size, including HTTP/2 accounting overhead | 1 KiB per section |
| Header fields | 16 per section |
| Retained application metadata | 512 bytes per call, combined across headers/trailers |
| Serialized-output storage ceiling | 4 KiB; reduce further after incremental emission |
| Additional unsent message ownership | At most one message per call, byte-capped |
| HPACK receive dynamic table | Advertise zero; handle negotiation/startup correctly |
| Driver RX/TX transfer chunk | Start at 128 bytes; benchmark 64/128/256 |
| Compression | Disabled; no backend linked |

Set both a local active-call cap and the advertised incoming-stream cap: the peer's stream
limit is not a substitute for limiting locally initiated client calls. Bound queue entry counts
as well as bytes, including empty messages, events, metadata, and terminal bookkeeping.

### Protocol constraints

- HTTP/2 starts with a 65,535-byte connection window; it cannot be advertised as 1 KiB.
  A window is credit, not a mandatory allocation. Small stream windows, local admission,
  incremental parsing, and bounded retention must enforce the actual RAM limit.
- HTTP/2 must support frame payloads of at least 16,384 bytes. Do not claim a 1 KiB frame limit;
  process frames incrementally instead of allocating a frame-sized input buffer.
- A zero HPACK table setting is not an immediate unilateral reset of the peer's encoder.
  Budget and correctly decode traffic permitted before the setting takes effect, including
  the initial 4 KiB dynamic-table allowance. Test SETTINGS acknowledgement and table updates.
- Account for wire prefixes, header accounting overhead, binary metadata expansion, padding,
  partial messages, and already-granted receive credit.
- Small defaults are intentionally breaking: larger messages, metadata, or concurrent calls
  need explicit configuration. Document the migration and throughput/latency tradeoffs.

## Milestone 0 — Establish honest memory baselines

- [ ] Add isolated allocator instrumentation for representative endpoint processes/tests. Track
  current/peak requested heap bytes, allocation count, largest allocation, and realloc overlap.
  Keep harness/peer allocations out of the endpoint measurement; include encoding/decoding
  transients. Report allocator implementation overhead separately where measurable.
- [ ] Inventory each buffer's owner, capacity, lifetime, and release point. Record idle, startup,
  steady streaming, blocked RX/TX, and post-cancellation high-water marks.
- [ ] Measure ARM `size_of` for connection, link, storage, and concrete driver futures. Count a
  future's inline buffers once; adding its full size and its member arrays double-counts RAM.
- [ ] Add separate minimal server-only and client-only firmware fixtures. The current binary
  requires both async and blocking; retain it for coverage, not minimum-size claims.
- [ ] Measure flash with identical toolchains/settings and separate application features:
  unary, streaming, raw transport, reliable UART, and optional compression. Measure stack
  high-water separately on hardware/emulation; reserved stack space is not evidence of use.

Gate: a reproducible before/after report distinguishes logical bytes, allocation capacity,
static/task storage, runtime stack, and application/HAL memory. No peak-RAM claims based only
on `pending_output().len()` or a successful link.

## Milestone 1 — Bound admission at every ownership boundary

Primary files: `protolink-http2/src/lib.rs`, `protolink-grpc/src/{client,lifecycle,metadata}.rs`,
`protolink-grpc/src/codec.rs`, and generated-code call sites.

- [ ] Define a small validated resource-limits contract. Bound input scratch, retained inbound
  data, outbound payloads, serialized bytes, metadata, call slots, and queue entry counts.
  State which bounds cover capacity rather than length. Configuration validation must prevent
  incompatible limits; do not silently increase budgets to make a configuration work.
- [ ] Enforce admission inside HTTP/2 and gRPC send APIs; `can_send()` remains an optimization,
  not the only guard. Return recoverable backpressure with ownership intact. Reserve capacity
  before encoding/copying and avoid mutating stream state on rejection.
- [ ] Check encoded size before allocating an oversized reply/request. Add bounded encode-into
  support for micropb/prost and update generated bindings. Arbitrary application message-object
  allocations remain outside the protocol bound and must be documented.
- [ ] Bound completed-but-unread results, retained trailers, draining/delivery records, and
  cancellation registrations. Release reservations exactly once on every exit path.
- [ ] Separate normal output pressure from fatal protocol failure. A valid large message should
  drain over multiple output batches, not fail because a whole serialization burst does not fit.
- [ ] Reserve bounded control-frame progress, or defer processing before obligations accumulate.
  Do not permit PING/SETTINGS/reset traffic to grow a control queue without limit. On sustained
  abuse, terminate predictably; never grow a buffer to manufacture an error response.
- [ ] Apply the lean defaults only together with admission/progress tests. Larger concurrency
  tests must specify larger budgets rather than relying on implicit generous defaults.

Gate: a blocked peer, ignored `can_send()`, empty-message flood, metadata flood, and repeated
cancellation cannot grow retained protocol memory beyond the documented bounds. Allocation
failure is reported without panic where the library controls the allocation/error path.

## Milestone 2 — Stream HTTP/2 input; remove whole-frame/event duplication

The [incremental HTTP/2 RX implementation plan](INCREMENTAL_HTTP2_RX_PLAN.md) defines the pull API,
input leases, DATA credit ledger, staged SETTINGS, bounded header ownership, and validation gates.
It is proposed, not implemented. Incremental frame/DATA delivery and incremental HPACK are separate
stages so their memory savings and remaining allocations can be measured independently.

Primary files: `protolink-http2/src/{lib,hpack}.rs`, `protolink-grpc/src/{inbound,lpm}.rs`.

- [ ] Replace whole-frame accumulation with a resumable frame parser: fixed header state,
  bounded control/header scratch, and DATA delivered in borrowed chunks. Report consumed input
  and backpressure so caller-supplied batches are never copied wholesale into an internal `Vec`.
- [ ] Skip unknown-frame payloads and padding incrementally. Process large SETTINGS sequences
  without retaining the entire frame. Validate lengths, stream state, and flow-control rules
  at the earliest valid point, preserving protocol error ordering.
- [ ] Replace the unbounded owned DATA event queue with direct chunk consumption or a bounded
  event interface. Do not hold a borrowed input slice across an unrelated async I/O operation.
- [ ] Feed chunks directly into one bounded partial-message buffer per call. Deliver/decode
  uncompressed messages from that storage where lifetimes permit; transfer ownership when a
  result must outlive the parser. Copy only at a genuine lifetime boundary.
- [ ] Make HPACK decoding incremental across HEADERS/CONTINUATION chunks, or retain a strictly
  bounded encoded block as an intermediate step. Final target: avoid simultaneous full encoded
  and decoded header blocks. Parse protocol fields directly; retain application metadata once.
- [ ] Reserve storage against granted receive credit and only release credit when budget permits.
  Preserve progress for messages larger than the receive window. Do not solve boundedness by
  stopping all reads while writes are blocked: duplex/ARQ progress must survive small buffers.

Gate: legal 16 KiB frames and messages fragmented at every boundary work without a 16 KiB
frame allocation. Large messages with a smaller configured receive window finish. Header
fragmentation, padding, SETTINGS, HPACK startup, and control floods obey the bound.

## Milestone 3 — Single-owner TX payloads and incremental serialization

Primary files: `protolink-http2/src/lib.rs`, `protolink-grpc/src/{codec,lpm,lifecycle,client}.rs`.

- [ ] Keep one owned encoded payload while it waits to send. Represent the five-byte gRPC
  prefix and nine-byte HTTP/2 frame header separately instead of allocating another full framed
  payload. Do not introduce reference counting solely to avoid a small header copy.
- [x] Remove the temporary DATA `to_vec()` copy; serialize directly from the queued payload slice.
- [ ] Replace whole-output accumulation with incremental emission using bounded staging.
- [ ] Replace output prefix `Vec::drain()` with read offsets/bounded compaction or a small ring.
  Cap capacity and release oversized high-water allocations according to a measured reuse policy.
- [ ] Resume frames across short writes without replay or interleaving another frame mid-frame.
  Preserve payload ownership until the relevant bytes have been accepted. Keep transport flush
  acknowledgement and output deadlines distinct from local serialization completion.
- [ ] Support bounded encode-into-buffer reuse; evaluate direct encoding into final message
  storage. Avoid retaining an entire large allocation for a tiny queued suffix when that would
  violate the physical-capacity budget.

Gate: streaming TX has one retained encoded payload, small framing state, and documented bounded
staging; no second full gRPC-framed payload or temporary full DATA chunk. Byte-for-byte output,
short writes, cancellation, and flush/deadline regressions still pass.

## Milestone 4 — Shrink bookkeeping and driver/future storage

Primary files: `protolink-http2/src/lib.rs`, `protolink-grpc/src/{client,lifecycle,local}.rs`,
`protolink/src/{coordinator,driver,mode,asynch}.rs`.

- [ ] Replace per-stream tree/set nodes with one bounded, lazily sized slot table for small
  connection limits. Reuse slots and ready flags rather than collecting stream IDs on each pass.
  Prefer a compact linear scan at one/few streams; retain correct stream-ID lifecycle and credit
  bookkeeping. Do not embed the largest supported stream count into every connection object.
- [ ] Consolidate duplicated state only where ownership/lifetimes actually match. Avoid a broad
  router/lifecycle rewrite merely to save a few scalar fields.
- [ ] Remove duplicate port/worker TX staging and RX scratch/inbox where safe ownership transfer
  permits. If a retained HAL future requires a stable separate buffer, keep and budget it.
- [ ] Allow small transfer chunks with one implementation. Remove the worker `Box` on the
  single-endpoint path using pinned local/caller-owned storage where feasible. Verify that this
  reduces total storage rather than merely moving heap bytes into a larger executor task/stack.
- [ ] Preserve independent RX/TX polling, fairness budgets, owner handoff, cancellation safety,
  and delivery acknowledgements. No unsafe lifetime shortcuts or serial-mode substitutions.

Gate: report concrete ARM type/future sizes and total RAM before/after, not just fewer heap
allocations. Existing small-pipe, stalled-write/flush, handoff, and reliable-link tests pass.

## Milestone 5 — Make link/adaptor memory proportional to need

Primary files: `protolink/src/link.rs`, `protolink-adaptors/src/{pump,channel}.rs`, and the
ARQ dependency if its API needs extending.

- [ ] Measure reliable-link storage on ARM. Test retransmission windows of 1/2/4/8 where supported;
  prefer the smallest default that meets measured round-trip/throughput needs. Document the
  bandwidth-delay tradeoff. Check peer compatibility before changing wire-visible parameters.
- [ ] Expose transport capacities without layering another buffering abstraction on the pump.
  Tune UART rings to measured maximum service latency and baud, not an arbitrary minimum.
  A smaller ring must not manufacture physical UART overruns; retain jitter tests.
- [ ] Reuse COBS scratch where RX/TX lifetimes permit, preserving simultaneous progress and
  whole-frame integrity. Avoid moving large link objects through small stacks.
- [ ] Keep raw transports free of ARQ storage; keep I2C storage per configured route explicit.
  Audit optional dependencies/feature combinations rather than assuming enabled code is retained.
- [ ] Keep gzip out of the minimal path. Any enabled codec must declare/bound scratch and output
  allocation, not merely check decompressed size after allocating it.

Gate: reliable UART has its own end-to-end memory report under loss, reordering, retransmission,
blocked flush, and full receive rings. Savings include task storage, not just protocol heap.

## Milestone 6 — Lock in the footprint

Provisional acceptance targets, to confirm or revise with Milestone 0 evidence:

- Single raw endpoint, one active 1 KiB-message call: at most **16 KiB total Protolink-owned RAM**.
- Same endpoint over reliable buffered UART: at most **32 KiB total Protolink-owned RAM**.
- Count protocol heap, inline connection/link/adaptor state, driver future storage, and measured
  additional stack once. Exclude and separately report application message objects, board HAL
  buffers, executor framework, kernel buffers, and unrelated firmware. Include HPACK startup.
- No unbounded growth under a stalled or malicious peer. After repeated traffic/cancellation,
  retained capacity returns to the documented baseline/reuse allowance.
- Set flash regression ceilings from the new minimal fixtures; do not present the combined
  smoke image as a minimum. Evaluate `opt-level=s` versus `z`, LTO, and codegen settings against
  both flash and peak RAM. Build profiles belong to consuming firmware, not library assumptions.

Validation matrix:

- Unary, server-streaming, client-streaming, bidi; client and server roles separately.
- Empty, tiny, maximum-sized, oversized, fragmented, and batched messages/headers.
- Messages larger than windows; transport chunks of 1/32/64/128 bytes; stalled peer windows.
- Cancel/reset/deadline during RX/TX/flush; connection churn and unread finished results.
- Valid startup before SETTINGS acknowledgement; dynamic HPACK references; large legal frames.
- Metadata/header/control/empty-DATA floods; malformed lengths; allocation failure injection.
- Raw duplex, COBS, reliable UART, I2C, and routers with explicit per-lane limits.
- Existing interop, feature/no_std checks, Miri, concurrency regressions, and bounded stress runs.

Extend `tests_sizing.rs` to assert total retained capacity/peak allocation, alongside wire-output
length. Give multi-stream sizing tests explicit larger configurations. Add deterministic
single-process memory tests and same-toolchain ARM flash/type-size gates to CI. Add on-device
heap/stack high-water testing before advertising a board RAM requirement.

## Execution order and non-goals

Land Milestone 0 first, then 1. Incremental RX and TX (2/3) can proceed independently once the
admission contract is agreed. Follow with bookkeeping/drivers and transport tuning (4/5),
remeasuring after each change. Each change carries its own progress and memory regression tests.

Do not start with a bespoke arena, an allocator-free rewrite, a large Huffman lookup table, or
removing streaming/duplex correctness. A compact Huffman decoder is a separate CPU/flash task
unless profiling shows meaningful memory impact. Do not use smaller limits as the sole evidence
of success, or make zero-allocation claims while message codecs/application objects still allocate.
If bounded dynamic storage cannot meet the targets, evaluate caller-owned fixed pools using the
same ownership contract; do not maintain parallel protocol engines.
