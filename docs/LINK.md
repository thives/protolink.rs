# Reliable raw links: framing, ARQ and recovery

`link::reliable(raw).with_retransmission_timer(timer).build()` (with `std`,
`link::reliable(raw).build()` uses a `StdTimer`) builds:

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

The link is built by the `link::reliable(raw)` factory. The concrete `ReliableLink` type
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
`protolink_adaptors::pump` for drivers whose pending operations cannot safely be cancelled;
future recreation now happens in `CobsTransport`, not in an upstream adapter.
The `protolink-adaptors` ports (I2C) are cancel-safe by construction, so they need no pump:
they only move bytes in and out of rings, while a runner owns the bus. See
[ADAPTORS.md](ADAPTORS.md). A port `flush` only means the bytes crossed the bus; the
reliable-link `flush` below still means the peer acknowledged them.

Both peers must continue driving ARQ until outstanding writes are acknowledged.
A receiver which has already obtained all application bytes may still need to
read to answer duplicates after its final ACK was lost. `flush()` completes only
when the peer has acknowledged everything written **and** every ACK owed to the
peer has been written and flushed on the raw stream.

## UART lines

A UART is a raw stream like any other, so the requirements above apply. Which UART path to use
depends on whether the driver is cancel-safe. [UART.md](UART.md) has the full contract.

| Raw stream | Assembly |
|---|---|
| A driver whose read loses no bytes when dropped, and whose write is all-or-nothing per poll | `link::reliable(uart)` directly |
| Split RX and TX halves, with a transfer that is aborted when its future is dropped (DMA) | `protolink_adaptors::uart(rx, tx).with_storage(..).build()`, and `link::reliable(port)` on its port |
| Linux serial port (`tokio-serial`) | `link::reliable(protolink::tokio::compat(LinuxUart::open(path, baud)?))` |

Without ARQ, a COBS stream can be split into its two directions: `link::cobs_split(reader, writer)`
returns a `CobsReader` and a `CobsWriter` with fresh state. They share nothing, so a read can wait
while a write is in progress, and each keeps the framing and cancel-safety rules of `CobsFramed`.
Split before the stream carries frames: an existing `CobsFramed` cannot be split without discarding
the bytes it has buffered. The reliable stack is not split: its one engine owns both directions.

The Linux wrapper meets these requirements; see [UART.md](UART.md#operations).

**Both ends of the line must use the same stack.** A UART has no framing or retransmission of its
own. A peer that speaks COBS cannot read ARQ frames, and a raw peer cannot read either. Choose the
stack explicitly on both ends, as the Linux examples do with `--transport raw|cobs|reliable`.

### Flush at each layer

A reliable `flush` is the only UART flush that tells you the peer has the bytes. It completes only
after the peer has acknowledged everything written, and every ACK owed to the peer has been written
through the raw stream. It does not mean the bytes are physically on the wire. The lower layers'
`flush` is local progress toward that, not the end-to-end guarantee. Each layer's guarantee is in
[UART.md](UART.md#flush).

### Timers

The reliable stack on a UART has two timers, and they are not interchangeable:

- the ARQ retransmission timer, from `with_retransmission_timer`, or `StdTimer` on `std`;
- the RPC deadline timer, from `with_deadline_timer` on the server and client, for example
  `protolink::tokio::TokioTimer`.

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
`link::reliable` is about 10.5 KiB (10.7 KiB with `FromTokio` over a Tokio
duplex stream and `StdTimer`), dominated by ARQ's two window buffers.

## Regression coverage

`protolink/tests/link.rs` exercises deterministic corruption with virtual time
(length, payload, type, truncated and appended DAT/ACK frames, correctable and
uncorrectable ACK damage), 1-byte, 7-byte, and coalesced reads, adjacent frames,
partial/pending raw writes and flushes, oversized frames, ordered delivery,
absence of duplicate delivery, eventual flush, cancelled upper reads, the
terminal timeout with buffered data, owed-ACK flushes and lower EOF. Its raw
ARQ test uses an in-memory framed transport fixture. `protolink/tests/std_timer.rs`
and `link::timer`'s unit tests cover `StdTimer`; `protolink-adaptors/tests/pump.rs` covers
the DMA pump.

The test fixtures write full 251-byte blocks, because ARQ coalesces smaller
writes and the damaged frame would otherwise depend on timing. Consequently the
"appended DAT" case is rejected at the COBS layer (257 bytes exceed the 256-byte
frame limit) and the declared-length cases (`253`, `61`, `127`) exercise ARQ's
exact-length validation.
