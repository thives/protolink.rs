# Reliable raw links: frame-oriented receive and recovery

`link::reliable_with_timer` (and the `std` shortcut `link::reliable`) builds:

```text
application bytes -> ARQ -> COBS -> raw transport
```

Both endpoints use the same wire format, CRC-16/IBM-SDLC, 16-byte BCH ACK
codec, and eight-frame window. This change does not change the wire format.

## Authoritative receive boundaries

COBS structure alone does not establish ARQ integrity. The reliable stack now
uses the vendored ARQ `embedded_io::EiaFramed` adapter and `ReadFrame` interface,
not `EiaLower`'s raw byte-stream parser. Each receive passes exactly one complete,
nonempty decoded COBS frame to ARQ, bounded to 256 bytes.

- DAT, DAT-with-ACK-request, and FIN require a valid type, exactly `5 + length`
  bytes, at most 251 payload bytes, and a valid CRC covering the packet ID,
  length, and payload.
- ACK recognition requires exactly 16 bytes. BCH decoding must succeed and the
  reconstructed ACK type and CRC must validate. A valid ACK prefix followed by
  extra bytes is not accepted as an ACK.
- An invalid ARQ frame is discarded in its entirety. Its declared length never
  selects bytes from another COBS frame, and no damaged suffix is retained.
- Invalid or oversized COBS frames are discarded. Oversized unterminated frames
  are discarded through the next delimiter, rather than treating their suffix
  as a fresh frame. Receive work yields after 32 bounded processing steps,
  preserving framing state and scheduling a wake even for always-ready noise.

Consequently, length corruption such as `125 -> 253`, `125 -> 61`, or
`125 -> 127` cannot poison the parser or consume adjacent frames. Unacknowledged
frames are retransmitted by ARQ's timer. Correctable ACK corruption is repaired
by BCH; uncorrectable or lost ACKs recover when duplicate DATs elicit fresh ACKs.
CRC/BCH provide accidental-corruption detection, not authentication.

## Standalone COBS remains generic

`CobsFramed` still implements `embedded_io_async::Read` as a byte stream. Small
reads return partial decoded payloads, subsequent reads return their remainder,
and arbitrary non-ARQ payloads remain supported. It validates COBS structure
only. The `ReadFrame` implementation used by the reliable stack consumes whole
frames; switching from a partial generic read to `ReadFrame` discards the old
remainder rather than exposing it as a complete frame.

`ReliableLink` now contains `EiaFramed<CobsFramed<S>>`. Callers using the public
constructors require no changes. Code spelling out the previous alias's
`EiaLower` concrete type must migrate to the frame adapter.

## Driving the link

The raw transport must still provide cancel-safe, prompt reads and cancel-safe
writes/flushes. Framing state survives cancellation between fragmented reads and
writes. Transport errors are propagated, not treated as corrupt frames. Use
`link::pump` for drivers whose pending operations cannot safely be cancelled.

Both peers must continue driving ARQ until outstanding writes are acknowledged.
A receiver which has already obtained all application bytes may still need to
read to answer duplicates after its final ACK was lost; sender `flush()` waits
for that ACK, not merely for transport output to drain.

## Dependency and regression coverage

`protolink/Cargo.toml` uses `../vendor/arq-io-async`, based on upstream 0.0.2.
See `vendor/arq-io-async/PATCHES.md` for the patch. No new runtime dependencies
are introduced. The root workspace explicitly excludes this dependency; its
upstream tests run separately with `cargo test --manifest-path vendor/arq-io-async/Cargo.toml`.
Cargo resolves ARQ locally rather than to the registry checksum/source entry.

The framed adapter is not present in the published upstream 0.0.2 package.
Before a crates.io release, upstream the patch or publish an appropriately named
and versioned patched dependency and update the manifest. Cargo removes path
locations when publishing; falling back to the unpatched registry version is not supported.

`protolink/tests/link.rs` exercises deterministic corruption with virtual time,
1-byte, 7-byte, and coalesced reads, adjacent frames, partial/cancelled writes,
ordered delivery, absence of duplicate delivery, and eventual sender flush.
The vendored ARQ unit tests additionally verify exact frame validation, rejected
ACK prefixes, BCH reconstruction with a wrong CRC, discarded-frame state, and
bounded corrupt-frame work per poll.
