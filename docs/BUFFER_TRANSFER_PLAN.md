# gRPC decoder buffer-transfer fast path

Status: implemented and measured (see [Results](#results)). Open items: server-path allocation
measurement and small-window eligible-transfer regressions.
Parent: [memory footprint plan](MEMORY_PLAN.md).

## Goal and scope

Avoid allocating a second payload buffer when the incremental gRPC decoder is returning one
uncompressed message and can give away its existing allocation instead.

Implement inside `Decoder::next_framed()` in
[`protolink-grpc/src/lpm.rs`](../protolink-grpc/src/lpm.rs). Both server
[`Inbound::next()`](../protolink-grpc/src/inbound.rs) and client
[`ResponseBuffer::push()`](../protolink-grpc/src/client.rs) use this path.

No public API changes, new dependencies, unsafe production code, protocol changes, smaller limits,
HTTP/2 parser changes, or changes to `Decoder::push()` allocation policy. Leave compression and
multi-message extraction on the existing path. This is buffer reuse, not zero-copy decoding:
removing the prefix still moves payload bytes within the original allocation.

## Why the current path costs memory

`next_framed()` currently calls `payload.to_vec()` while the framed input is still owned by the
decoder. When that message exhausts the buffer, `compact()` frees the original allocation.

For an eligible payload of `P` bytes:

| Property | Current path | Planned fast path |
|---|---:|---:|
| Decoder input allocation capacity | `P + 5` | `P + 5` |
| New payload allocation during extraction | `P` | None |
| Payload allocation retained after extraction | `P` | `P + 5` |
| Decoder capacity after extraction | 0 | 0 |
| Original framed wire length returned | `P + 5` | `P + 5` |

Thus extraction avoids one allocation and `P` bytes of simultaneous requested storage, but the
returned vector keeps five additional capacity bytes. For a 4 KiB eligible message, the local
requested-storage overlap drops from 8,197 bytes to 4,101 bytes. These are arithmetic predictions,
not measured whole-endpoint peak RAM or physical allocator-block sizes.

The client's `RetainedMessage::bytes()` already charges at least `P + 5` for an uncompressed
message. The conservative capacity gate below keeps transferred capacity within that existing
charge. This does not make all existing client memory accounting capacity-aware.

## Exact eligibility rule

Validate the prefix and obtain `wire_len` using the existing logic **before** evaluating this
fast path. Transfer only when all conditions hold:

1. The current message is uncompressed (`compressed == false`). An installed codec does not
   disable the fast path for an uncompressed message.
2. The payload is nonempty (`wire_len > HEADER_LEN`). Empty messages retain the existing path,
   which returns a zero-capacity vector instead of retaining a prefix-only allocation.
3. `start == 0`: no previously consumed leading bytes remain in the buffer.
4. `count == 1`: exactly one complete validated message is buffered.
5. `complete == buf.len()` and `wire_len == buf.len()`: there is no trailing partial or complete
   message and no retained input outside this message.
6. `buf.capacity() == wire_len`: the allocation is exactly sized to this framed message.

The exact-capacity rule is intentional. Do not use a fraction, a 1 KiB slack allowance, or a
`max_message_size`-based threshold: those could retain a large allocation for a small result.
Do not shrink/reallocate to make a message eligible. If any condition fails, run the current
copy/decompression/compaction path unchanged.

Fragmented arrivals may still qualify if their final capacity is exact. Geometric over-allocation,
small-vector minimum capacity, and batching will reduce the hit rate; measure this rather than
widening the gate in the first patch. A later message from an earlier batch may qualify after
existing compaction, but the fast path itself must not compact a batch to seek eligibility.

A pending `error` must not be cleared. The decoder promises to return valid buffered messages
before a subsequent framing error. Do not add `error.is_none()` as a semantic requirement;
preserve that state if an otherwise eligible buffer is transferred.

## State transition

Add one early-return branch after successful prefix validation and before `payload.to_vec()`:

1. Move `self.buf` into a local `Vec` with `core::mem::take()`.
2. Remove `HEADER_LEN` bytes from its front in place, for example with a fully consumed/dropped
   `drain(..HEADER_LEN)`. Do not use `split_off()`, `to_vec()`, `shrink_to_fit()`, or raw pointers.
3. Set `start = 0`, `complete = 0`, and `count = 0`; `self.buf` is already empty with zero capacity.
4. Leave `max_size`, `codec`, and `error` unchanged. Do not reconstruct the whole decoder with
   `Decoder::new()`, which would lose configuration/error state.
5. Return `Some(Ok((message, wire_len)))`, using the original validated framed length, not the
   shortened vector length or its capacity.

No call to `compact()` is needed for the transferred case. All fallback success/failure cleanup
remains unchanged. Keep the branch local and explicit; do not introduce another storage abstraction.

## Test-first implementation sequence

### 1. Add deterministic state/capacity regressions

Extend `boundary_tests` in `lpm.rs`. Use private exact-capacity fixtures where necessary, without
changing production allocation policy. A vector obtained from a boxed slice can provide a
known length/capacity; do not assume all allocator growth produces an exact-sized vector.

- [x] Eligible nonempty messages: correct bytes, original `wire_len`, unchanged allocation pointer
  and capacity, zero decoder capacity, and all positions/count reset. Include a maximum-size message.
- [x] Decoder reuse: decode another message afterward; size and installed codec configuration
  remain intact. Test uncompressed input with a codec installed.
- [x] Empty messages: output length/capacity zero, including `max_size == 0`.
- [x] Gate boundary: capacity equal to `wire_len` transfers; capacity greater by even one byte
  falls back. Check small payloads and oversized buffers without assuming a particular allocator's
  incidental rounding. Verify fallback returns compact storage rather than the old large allocation.
- [x] Two complete messages: preserve order and remaining state; do not transfer the batch.
- [x] Complete message followed by 1–4 prefix bytes or a partial payload: preserve the tail and
  existing compaction/shrink behavior; finish decoding when more bytes arrive.
- [x] Consumed leading bytes: keep the fallback when `start != 0`.
- [x] Fragmentation: split at every prefix boundary and representative payload boundaries; cover
  both exact final capacity and geometric excess capacity.
- [x] Valid message before invalid flag, oversized prefix, or other pending error: deliver the
  valid message first, retain the error for subsequent `next()`/`finish()`, and ignore later pushes
  as before. Include an eligible private-state fixture to verify the branch preserves `error`.
- [x] Compressed success, decompression failure, unsupported compression, and truncation: existing
  semantics and buffer cleanup remain unchanged. Do not transfer compressed bytes as decoded data.

Pointer equality alone is insufficient proof of avoided allocation: an allocator may recycle an
address. Combine it with capacity assertions and the allocation measurements below.

### 2. Measure allocations and local peak requested storage

Add a dedicated host integration test binary under `protolink-grpc/tests/` for allocator
instrumentation. Follow the thread-local isolation pattern in
[`protolink-http2/tests/allocations.rs`](../protolink-http2/tests/allocations.rs), but also track
live/peak requested bytes and successful allocation/reallocation sizes. Keep any necessary unsafe
allocator forwarding confined to that host test crate; the library remains `forbid(unsafe_code)`.
Do not add an instrumentation dependency or a production feature just for this test.

- [x] Prepare framed source bytes before measurement and keep them alive throughout it; exclude
  the source/peer/harness from the decoder measurement.
- [x] Track the complete decoder allocation lifetime from before `push()` through result/decoder
  drop. Use phase snapshots around extraction. Never reset live-byte accounting while measured
  allocations remain live, or subtract deallocations of untracked setup objects from it.
- [x] For eligible inputs, extraction must issue zero allocations/reallocations; the decoder must
  retain no storage and the result's capacity must not exceed its original framed wire length.
- [x] Compare with the current implementation before changing it: eligible extraction currently
  allocates a payload buffer. Record the baseline failure and local peak, then rerun after the patch.
- [x] Measure 1 KiB and 4 KiB eligible cases, small ineligible cases, fragmented inputs, and a large
  buffer followed by a tiny message. Oversized-capacity cases must not retain the large buffer.
- [ ] Exercise real client/server paths separately from decoder-only counting. Do not require the
  entire client/server receive operation to allocate nothing: events, result queues, and application
  decoding still allocate.

Document what the counters measure. Account only successful allocations; handle realloc failure
without changing live ownership. A moving realloc can briefly retain both blocks, so distinguish
logical live bytes from any unmeasured allocator-internal overlap and metadata. This fast path
must not require a realloc, making its extraction measurement independent of that ambiguity.

### 3. Implement the branch and verify credit/retention behavior

- [x] Implement the exact eligibility and state transition above.
- [x] Preserve the original framed wire length through both server and client credit accounting;
  no early, missing, or double WINDOW_UPDATE credit release.
- [ ] Add/extend client and server regressions for message retention with a deliberately small
  window, taking the result, cancellation, and connection completion. For an eligible transferred
  response, verify capacity is covered by the existing framed-byte charge.
- [x] Keep existing high-water release, partial-buffer trimming, malformed-input, and compression
  tests. Do not weaken their capacity assertions to accommodate the optimization.

### 4. Validation and documentation

Run targeted decoder/allocation tests first, then client/server/compression tests and the full
workspace. Expected commands from the repository root once the new test file exists:

```sh
cargo test --offline --locked -p protolink-grpc --lib lpm::boundary_tests
cargo test --offline --locked -p protolink-grpc --test allocations
cargo test --offline --locked -p protolink-grpc --all-features
cargo test --offline --locked --workspace --all-features
cargo clippy --offline --locked -p protolink-grpc --all-features --all-targets -- -D warnings
cargo check --offline --locked --target thumbv6m-none-eabi --lib --no-default-features --features async -p protolink
cargo fmt --all -- --check
```

- [x] Re-run allocation tests in release mode as well as debug; inspect hit rates for ordinary
  fragmented traffic without treating a missed fast path as a correctness failure.
- [x] Update decoder documentation and the parent memory plan with actual measured results,
  explicitly noting the five-byte returned-capacity tradeoff and workloads that fall back.

## Acceptance criteria

Eligible extraction avoids the second allocation, retains at most `payload.len() + 5` capacity,
leaves an empty/reusable decoder, and returns identical data/wire-credit/error semantics. Empty,
compressed, batched, partial-tail, and oversized-capacity cases preserve their existing behavior.
All existing tests pass. No new per-decoder fields, buffers, dependencies, or production unsafe code.

Do not broaden the patch into allocation-policy tuning to increase hit rate. If measured workloads
rarely qualify, report that limitation and evaluate a separate follow-up rather than weakening the
capacity guard or claiming general endpoint peak-RAM savings.

## Results

The transfer is implemented in `Decoder::next_framed()`. Measurements come from
[`allocations.rs`](../protolink-grpc/tests/allocations.rs), which counts successful allocations,
reallocations, and the requested bytes live in a measurement window. These are requested bytes, not
allocator block sizes, and not whole-endpoint RAM. Run with `--nocapture` to see the phase values.

Accounting, as defined in the test file's header: `live` is the requested bytes of the tracked
blocks currently live, and `peak` is the highest logical `live` within a phase, including the phase's
starting total. A successful realloc replaces the old block's bytes with the new size, once, whether
the block moved or was resized in place. Allocator metadata, rounding, and transient overlap during a
moving realloc are not measured.

An earlier tracker added a realloc's new size to the old live total, so a phase containing a realloc
could report an inflated peak. That is corrected. The figures below were re-measured with the
corrected tracker, and each before/after pair uses the same accounting. Debug and release runs agree
for both the pre-change baseline and the post-change code.

Decoder extraction phase, eligible single message (`P` payload bytes, wire length `P + 5`):

| Case | Before | After |
|---|---|---|
| 1 KiB (wire 1,029) | 1 allocation; peak 2,053 B; retained 1,024 B | 0 allocations; peak 1,029 B; retained 1,029 B |
| 4 KiB (wire 4,101) | 1 allocation; peak 8,197 B; retained 4,096 B | 0 allocations; peak 4,101 B; retained 4,101 B |

Both baselines match the predicted `2P + 5` peak. Neither extraction phase reallocates, so the
correction does not change these figures.

`lpm::decode_unary` for a whole exact body: 1 KiB before 2 allocations and peak 2,053 B; after 1
allocation, peak 1,029 B. 4 KiB before 2 allocations and peak 8,197 B; after 1 allocation, peak
4,101 B.

Real client receive path, one DATA frame holding the response, measured across `recv` and
`take_response`. The window includes the connection's own events and call state, so the allocation
count and retained total are not decoder-only figures:

| Case | Before | After |
|---|---|---|
| 1 KiB response | 18 allocations; peak 4,649 B; retained 3,424 B; capacity 1,024 | 17 allocations; peak 3,785 B; retained 3,429 B; capacity 1,029 |
| 4 KiB response | 18 allocations; peak 16,937 B; retained 9,568 B; capacity 4,096 | 17 allocations; peak 13,001 B; retained 9,573 B; capacity 4,101 |

The window's peak fell by 864 B at 1 KiB and 3,936 B at 4 KiB. The 1 KiB reduction is smaller
than the payload, since the window's peak is set by the whole receive operation. I have not
attributed the difference further.

Both client "before" rows were reproduced from the pre-change decoder under the corrected tracker,
in debug and release, and match the values above. Each window contains one realloc, and the earlier
and corrected trackers report the same peak for these phases, so no client peak is superseded.

Hit rate for ordinary delivery, decoder only, 48 messages of 64, 1,024, and 4,096 bytes:

- One DATA frame per message: 48 of 48 returned without allocation (0 of 48 before the change).
- The same stream in 1,460-byte segments: 0 of 48 (0 of 48 before the change).

Segmented arrival misses under the exact-capacity gate, as the plan anticipated. The fast path helps
when one DATA frame carries one whole message into an empty decoder, which is the common unary case.
Widening the gate would need a separate measured policy.

Tradeoff: a transferred message keeps five bytes of spare capacity. The client charge
`RetainedMessage::bytes()` is `max(len + 5, wire_len)`, so the spare bytes are inside the existing
framed-byte charge. The server's handler receives a slice, so the capacity is not visible there.

Fallbacks, all unchanged: compressed messages, empty messages, batches, partial tails, leading
consumed bytes, segmented or fragmented arrival, frames shorter than eight bytes (the standard
library rounds a fresh `u8` allocation up to eight), and any buffer with spare capacity.

Validation for the tracker correction: `allocations` in debug and release, serially with
`--nocapture`. It has 16 tests, nine of them tracker regressions. Six of those nine failed under the
earlier `resized()` formula. Also run `protolink-grpc` with all features, clippy with `-D warnings`
on `protolink-grpc` all targets, and `cargo fmt --all -- --check`. The correction changes only the
test harness, so the production decoder is unchanged. The earlier workspace and
`thumbv6m-none-eabi` no_std checks were not rerun.

Open items:

- Server-path allocation measurement. The server decodes through the same `Decoder`, but its
  handler sees only a slice, so only its credit is checked, in `tests_server.rs`.
- Eligible transfers under a deliberately small window. A transfer needs the whole frame inside
  the window, so a meaningful case needs a window at least the wire length. The client budget
  boundary test covers the retained charge.
- A large buffer followed by a tiny message in one batch. The existing compaction behavior is
  unchanged and has not been measured.
