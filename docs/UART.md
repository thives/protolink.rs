# UART contract

A UART is a full-duplex, point-to-point byte stream. protolink supports three paths to it. Neither
adaptor adds framing or reliability: COBS and ARQ are always layered explicitly with
`protolink::link`, and each end of a line must use the same stack.

| Path | Purpose | Assembly |
|---|---|---|
| Direct embedded UART | A cancel-safe duplex HAL driver | pass it to protolink directly |
| Buffered embedded UART | split RX and TX halves, with a transfer that is aborted when its future is dropped (DMA) | `protolink_adaptors::uart(rx, tx).with_storage(&mut state).build()` |
| Linux UART | a configured serial device under Tokio | `protolink_adaptors::uart::linux::LinuxUart::open(path, baud)` |

Scope of the first version: full-duplex, point-to-point, async. No RS-485 direction control, no
multidrop addressing, no automatic reconnect, and no blocking UART integration.

## Operations

These requirements apply to every UART path, and to a direct driver in particular.

- **Reads** return the bytes available, as soon as there are some. They do not wait to fill the
  buffer. An idle line leaves the read pending; it must not return `Ok(0)` because nothing arrived.
  `Ok(0)` means end of stream (for example, a hung-up Linux peer).
- **Writes** may accept only a prefix of the buffer. Accepted bytes keep their order. A write that
  is dropped while pending writes nothing.
- **Cancellation.** A direct driver must meet the requirements of the layer above it. The reliable
  link polls its raw stream once and drops the future if it is pending, on every poll. So a
  direct read must lose no received byte when dropped, and a direct write must be all-or-nothing
  per poll. A driver that cannot meet this uses the buffered path, where the port operations
  never touch the hardware.
- **Errors** keep their concrete kind. Disconnects, configuration failures, and persistent
  hardware errors are reported, never suppressed. Only a recoverable condition may be skipped, and
  only by an explicit choice (`RxPump::skip_errors`), never as a default.
- **Configuration** is the platform driver's job: baud rate, parity, stop bits, flow control, pins,
  and DMA setup. The adaptor does not configure the line, except for the Linux wrapper's documented
  defaults (8N1, no flow control, raw mode).

## Flush

`flush` means something different at each layer. Read the one that applies:

| Operation | Guarantee |
|---|---|
| Direct embedded `flush` | the selected HAL's documented guarantee, and nothing more |
| Buffered UART `flush` | every byte accepted so far has been written, and the HAL's `flush` has returned |
| Linux wrapper `flush` | no adapter-owned output remains: every accepted byte is with the OS driver. **This is not wire-idle.** |
| Reliable-link `flush` | the peer has acknowledged everything written, and every ACK owed to the peer has been written through the lower transport |

The Linux wrapper deliberately never calls the upstream `poll_flush`, which calls `tcdrain`. That
call blocks until the device has transmitted the queued output (seconds at a low baud rate, or
while flow control holds the line), it cannot be cancelled, and it discards the error of the drain.
Moving it to `spawn_blocking` makes it neither abortable nor bounded. If an application needs the
bytes physically on the wire, for example before a baud-rate change, that needs its own explicit
operation.

## Shutdown and teardown

- A UART has no half-close. Shutdown on the Linux wrapper completes at once, like `flush`, and
  does not close the device. The device closes when the wrapper is dropped. Neither shutdown nor
  close shows that the peer received the queued bytes.
- **The buffered runner owns both halves.** When `UartRunner::run` returns, or its future is
  dropped, both halves are dropped too, so a waiting port operation never waits for a runner that
  is gone. A cancelled runner fails both directions with `BrokenPipe`. After a HAL failure, the
  failing direction reports the HAL's error kind and the other reports `BrokenPipe`. Received bytes
  that are already buffered are read first. The runner returns the concrete failure:
  `PumpFailure::Rx` or `PumpFailure::Tx` with the
  HAL's error.
- **A failure ends the session.** Recovery means building the UART, link, and RPC stack again, as
  a deliberate new session. Restarting the runner does not revive a terminal ARQ or RPC state.
- Cancelling a port operation never cancels a hardware transfer. The runner drives every transfer
  to its end, and the port only moves bytes in and out of its rings.

## Buffered facade

`protolink_adaptors::uart` is a thin facade over the `pump` module of `protolink-adaptors`. It reuses the pump's
rings, staging buffer, flush protocol, and error propagation; it does not add a second buffer
implementation.

- `UartState` holds the buffers. Its defaults are the pump's (`RING_SIZE` and `CHUNK_SIZE`, about
  1.2 KiB), not the 64-byte defaults of the I²C channel. It holds no HAL type and no heap, so it
  can be a local or a `static`.
- `build()` only borrows the storage. It performs no hardware operation and spawns no task.
- The port is the pump's `PumpHandle`, and the runner is `UartRunner`. Neither is forced into the
  I²C `Port` type.
- The receive and transmit halves must be separately owned. A driver that cannot be split must be
  wrapped first.

## Linux wrapper

`protolink_adaptors::uart::linux::LinuxUart` (feature `uart-linux`, Linux only) wraps a
`tokio_serial::SerialStream`:

- **Opening is a separate step.** `LinuxUart::open(path, baud)` performs I/O, and `build()` never
  opens a device. `from_serial` wraps a port that was configured elsewhere.
- **Line settings:** 8 data bits, no parity, one stop bit, no hardware or software flow control,
  raw mode. The peer must use the same baud rate and settings. Software flow control is never
  enabled.
- **Exclusivity:** `serialport` opens the port exclusively (`TIOCEXCL` and an advisory lock). A
  second opener fails until the first is dropped. The error comes from the dependency, and the
  wrapper does not promise its kind: contention can appear as `NotFound`, and errors while opening
  or configuring the port may have lost their numeric `errno`.
- **Empty writes** complete at once with `Ok(0)`, also while the output queue is full.
- **Permissions:** read and write access to the device node, usually through a group such as
  `dialout`.
- **DTR:** opening some USB-serial adapters asserts DTR, which resets boards that use it for
  auto-reset. The wrapper does not change DTR or RTS.
- **Timeouts:** `SerialStream::set_timeout` does nothing in the audited `tokio-serial` version.
  Use a protolink or Tokio deadline. Protolink's `TokioTimer` does this for the RPC deadline, and
  an ARQ timer is separate.
- **Dependencies:** `tokio-serial` 5.5.0 with default features off. Its `serialport` dependency
  does not pull in `libudev`.

Use it with the compatibility adapter, and a `StdTimer` supplies the ARQ timer:

```rust,ignore
let raw = LinuxUart::open("/dev/ttyUSB0", 115_200)?;
let io = protolink::link::reliable(protolink::tokio::compat(raw)).build();
let client = protolink::client(io)
    .with_deadline_timer(protolink::tokio::TokioTimer::new())
    .build();
```

## Wire format

Both ends of a line must use the same stack. The Linux examples in `examples/embedded-device` take
an explicit `--transport`:

| Transport | Stack, bottom to top | What an embedded peer implements |
|---|---|---|
| `raw` | HTTP/2 bytes | HTTP/2 frames directly on the line. Use it only where the line never drops or corrupts bytes. |
| `cobs` | COBS frames, no checksum, no retransmission | each write of up to 256 bytes (the decoded payload maximum) is one COBS frame (`cobs-io-async` 0.0.4), zero-delimited. Nothing checks a frame's contents: a corrupted message body reaches the application unnoticed, and only damage to the HTTP/2 framing fails the connection. |
| `reliable` | COBS frames, CRC-checked, with ARQ | as `cobs`, with each frame an ARQ DAT, ACK, or FIN (`arq-io-async` 0.0.3). A DAT carries up to 251 application bytes after its five-byte header. See [LINK.md](LINK.md). |

The line settings are 8N1 at the agreed baud rate, with no flow control.

### Reliable linger in the one-shot client

The example client (`uart_link::call`) is exclusive and one-shot. After its last RPC succeeds on
the `reliable` transport, it keeps reading the same link, and discards what arrives, for a grace
budget of five seconds (`RELIABLE_LINGER`). The reason is that the client's last ACK of the server's
final response frames can be lost. The server then retransmits, and ARQ answers a retransmission
only while somebody reads the link. The retained link state recognises the duplicate and writes a
replacement ACK. The server's pending response output can then complete.

- The grace is one absolute deadline. Arriving traffic does not extend it. It ends early on end of
  stream or on `ReliableError(ArqError::Closed)`. Neither shows that the final ACK arrived.
- It is separate from the RPC deadlines, which are unchanged. A successful reliable run takes up to
  the grace budget longer; `raw` and `cobs` runs take no longer. Five seconds is a timer budget,
  not a wall-clock guarantee.
- It is a bounded opportunity to recover. It is not a coordinated close, it is not physical
  wire-idle, and it does not prove that the final ACK arrived.
- It cannot save a server whose own RPC deadline has run out before the replacement ACK arrives:
  the server then fails with a deadline error. Choose the deadline with the retransmission time in
  mind (250 ms for the first round).
- An error during the linger (any error other than `Closed`) is returned as
  `RPCs completed; reliable linger failed: …`. It happens after the RPCs succeeded and does not
  undo their effects: an already successful `SetOutput` is neither rolled back nor safe to retry on
  that account.
- Discarding the bytes is right only because no call remains. It is not a technique for discarding
  input of a reusable or multiplexed client.

## What is verified

| Claim | Evidence |
|---|---|
| The buffered facade's data path, partial I/O, wrap-around, cancellation, flush, failure, overrun, teardown, and the reliable stack over it | `protolink-adaptors/tests/uart.rs`, against a mock DMA-like HAL |
| Linux wrapper: binary transparency, fragmentation, backpressure (sustained, with an empty write that still completes), cancellation, idle pending, hangup, exclusive open and reopen through the production open path | `protolink-adaptors/tests/uart_linux.rs`, over pseudo-terminals |
| Raw, COBS, and reliable RPCs with a deadline, and a deadline that expires while other work runs; a failed call does not linger | `examples/embedded-device/tests/uart_linux.rs`, over pseudo-terminals, through a relay that keeps both ends of the line until the test releases them |
| Loss of the client's ACK of the final streaming response is recovered by the linger; without the linger it is not; a recovery later than the server's deadline is too late | the same file, with a relay that finds the DAT that completes the `EventSubscribe` stream and drops the ACK that covers it |
| Linger policy: deadline expiry, no extension by traffic, early end on EOF and `Closed`, failure on other errors | unit tests in `examples/embedded-device/src/uart_link.rs`, with paused Tokio time |
| Two processes, one per end, over a virtual serial pair, for all three transports | manual run of the example binaries under `socat` |
| Embedded facade compiles and links for Cortex-M0 | `examples/embedded-smoke` (`uart` feature). Compile coverage only. |

**Not verified:** physical UART hardware, baud-rate timing, physical transmit completion, RTS/CTS
or any hardware flow control, and interoperation with an embedded board. Pseudo-terminals do not
model baud rate or wire drain. No board or HAL driver is shipped as supported. A board driver can be
advertised only after it passes the checks listed in the plan: `embedded-io-async` 0.7
compatibility, RX/TX split and partial reads, cancellation and flush audit for each half, board
firmware, Linux-to-board unary and streaming RPCs, application stalls, sustained bidirectional
traffic, and terminal failures that need an explicit session rebuild.
