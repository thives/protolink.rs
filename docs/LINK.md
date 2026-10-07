# Reliable raw links: framing, ARQ and recovery

`link::reliable_with_timer` (and the `std` shortcut `link::reliable`) builds:

```text
application bytes -> ARQ -> COBS -> raw transport
```

## Dependencies and compatibility

The link uses the published crates `arq-io-async` 0.0.3 and `cobs-io-async`
0.0.4, with default features off. There is no vendored or patched copy.

- **The wire format changed with `arq-io-async` 0.0.3. Both endpoints must be
  upgraded together.** An ACK frame is now a type byte followed by the 16-byte
  BCH codeword (17 bytes; it was the bare 16-byte codeword). DAT, DAT with ACK
  request and FIN frames keep their layout (5-byte header, up to 251 payload
  bytes), as do CRC-16/IBM-SDLC, the BCH ACK codec and the eight-frame window.
  Each ARQ frame is still one zero-delimited COBS frame.
- ARQ 0.0.3 packs consecutive writes into one DAT payload (up to 251 bytes)
  instead of sending one frame per write, so frame boundaries no longer follow
  write boundaries.
- Retransmission is now bounded: 16 rounds without acknowledgement progress
  (250 ms doubling to 4 s) end the link with a terminal timeout. The vendored
  0.0.2 defined `ArqError::Timeout` but never produced it, so it retried
  without limit.
- COBS is used only for its in-memory codec (`cobs_io_async::sync`). Its
  `Reliable` layer is not used: that would add a second acknowledgement and
  retransmission protocol with a different wire format.

`arq-io-async` implements no runtime adapters, so protolink owns them:

| Type | Role |
|---|---|
| `CobsFramed<S>` | COBS framing over a raw `embedded_io_async` stream. As a byte stream (`Read`/`Write`) it is generic; `read_frame` returns one whole decoded frame. |
| `CobsTransport<S>` | `arq_io_async::Transport` for ARQ's lower layer: one `poll_read` returns one frame, one `poll_write` carries one whole frame, `poll_flush` drains and flushes. |
| `ReliableLink<S, Tmr>` | ARQ over `CobsTransport`, driven from `async` `Read`/`Write`; its error is `ReliableError`. |
| `StdTimer` | Executor-independent `Timer` (`std` feature): one reusable helper thread per timer. |

Public constructors keep their signatures. The concrete `ReliableLink` type
and its error type (`ReliableError`, wrapping `ArqError<LinkError<E>>`) changed;
code that spelled out the old alias must migrate.

## Authoritative receive boundaries

COBS structure alone does not establish ARQ integrity. `CobsTransport` hands
ARQ exactly one complete, nonempty decoded COBS frame per read, bounded to
256 bytes, and ARQ treats those boundaries as authoritative.

- DAT, DAT-with-ACK-request, and FIN require a valid type, exactly `5 + length`
  bytes, at most 251 payload bytes, and a valid CRC covering the packet ID,
  length, and payload.
- ACK recognition requires exactly 17 bytes. BCH decoding must succeed and the
  reconstructed ACK type and CRC must validate. A valid ACK prefix followed by
  extra bytes is not accepted as an ACK.
- An invalid ARQ frame is discarded in its entirety. Its declared length never
  selects bytes from another COBS frame, and no damaged suffix is retained.
- Invalid or oversized COBS frames are discarded. The decoder only ever sees a
  delimiter-terminated segment; an unterminated tail is never decoded, and
  oversized unterminated frames are discarded through the next delimiter.
  Receive work yields after 32 bounded processing steps, preserving framing
  state and scheduling a wake even for always-ready noise.

Consequently, length corruption cannot poison the parser or consume adjacent
frames. Unacknowledged frames are retransmitted by ARQ's timer. Correctable ACK
corruption is repaired by BCH; uncorrectable or lost ACKs recover when duplicate
DATs elicit fresh ACKs. CRC/BCH provide accidental-corruption detection, not
authentication.

## Standalone COBS remains generic

`CobsFramed` still implements `embedded_io_async::Read` as a byte stream. Small
reads return partial decoded payloads, subsequent reads return their remainder,
and arbitrary non-ARQ payloads remain supported. It validates COBS structure
only. `read_frame`, used by the reliable stack, consumes whole frames; switching
from a partial generic read to `read_frame` discards the old remainder rather
than exposing it as a complete frame.

`CobsTransport::poll_write` rejects a frame larger than 256 bytes with
`LinkError::FrameSize` instead of clipping it, and reports the full length only
once every encoded byte reached the raw stream, however many partial writes and
polls that takes.

## Driving the link

The raw transport must still provide cancel-safe, prompt reads and cancel-safe
writes/flushes: `CobsTransport` creates a fresh raw future on every poll, polls
it once, and drops it if pending. Framing state survives that, and cancellation
between fragmented reads and writes. Dropping an upper `read`, `write` or
`flush` future loses nothing, since ARQ keeps all protocol state in the link.
Transport errors are propagated, not treated as corrupt frames. Use
`link::pump` for drivers whose pending operations cannot safely be cancelled;
future recreation now happens in `CobsTransport`, not in an upstream adapter.

Both peers must continue driving ARQ until outstanding writes are acknowledged.
A receiver which has already obtained all application bytes may still need to
read to answer duplicates after its final ACK was lost. `flush()` completes only
when the peer has acknowledged everything written **and** every ACK owed to the
peer has been written and flushed on the raw stream.

## Failure semantics

| Event | Result |
|---|---|
| 16 retransmission rounds without ACK progress | `ArqError::Timeout` (`ErrorKind::TimedOut`), terminal |
| Raw stream error | `ArqError::Io(LinkError::Io(e))`, terminal, `e.kind()` |
| Raw stream accepts zero bytes | `LinkError::WriteZero` (`ErrorKind::WriteZero`), terminal |
| Raw stream ends (EOF) | buffered data is delivered first; then `Ok(0)` after a valid FIN, otherwise `ArqError::Closed` (`ErrorKind::BrokenPipe`) |
| Any operation after a terminal error | the original error is reported once, after buffered in-order data has been read; later operations fail with `Closed` |

## Memory

`CobsFramed` is about 1.1 KiB (1088 bytes on x86-64). The full stack built by
`reliable_with_timer` is about 10.5 KiB (10.7 KiB with `FromTokio` over a Tokio
duplex stream and `StdTimer`), dominated by ARQ's two window buffers.

## Regression coverage

`protolink/tests/link.rs` exercises deterministic corruption with virtual time
(length, payload, type, truncated and appended DAT/ACK frames, correctable and
uncorrectable ACK damage), 1-byte, 7-byte, and coalesced reads, adjacent frames,
partial/pending raw writes and flushes, oversized frames, ordered delivery,
absence of duplicate delivery, eventual flush, cancelled upper reads, the
terminal timeout with buffered data, owed-ACK flushes and lower EOF. Its raw
ARQ test uses an in-memory framed transport fixture. `protolink/tests/std_timer.rs`
and `link::timer`'s unit tests cover `StdTimer`; `protolink/tests/pump.rs` covers
the DMA pump.

The test fixtures write full 251-byte blocks, because ARQ coalesces smaller
writes and the damaged frame would otherwise depend on timing. Consequently the
"appended DAT" case is rejected at the COBS layer (257 bytes exceed the 256-byte
frame limit) and the declared-length cases (`253`, `61`, `127`) exercise ARQ's
exact-length validation.
