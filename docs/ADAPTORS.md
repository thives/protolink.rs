# Bus adaptors

`protolink-adaptors` turns a hardware bus into a byte stream that the rest of protolink already
understands. It uses the same vocabulary as the rest of protolink (see [API.md](API.md)):

> **Create a factory → configure it with `with_*` (including caller-owned storage) → `build` a port
> and a runner → use the port with protolink, and `run` the runner.**

| Crate | Owns | Does not own |
|---|---|---|
| `protolink-i2c` | Mailbox encoding, controller transaction primitives, the target-driver trait | Queues, RPC, COBS/ARQ, executor tasks |
| `protolink-adaptors` | `i2c_controller` / `i2c_target` / `uart`, buffered ports, route storage, scheduling, runners, the ring buffer and the DMA pump | Board-specific peripheral setup |
| `protolink` | RPC drivers and the reliable link | Anything I2C specific, buffers and the DMA pump |

There is no new transport trait: a port implements `embedded_io_async::{Read, Write}`, which is what
`protolink::client`, `protolink::server` and `protolink::link::reliable` already take.

## Controller, one peer

```rust,ignore
use protolink_adaptors::{Binding, ControllerState, i2c_controller};

let mut state = ControllerState::<1>::new();
let Binding { port, runner } = i2c_controller(i2c, timer)
    .with_address(0x42)
    .with_storage(&mut state)
    .build()?;
```

- `i2c` is any `embedded_hal_async::i2c::I2c`; `timer` is any `Timer`: a monotonic `Clock` that can
  also wait until a deadline, with the same model as `protolink::timer`.
- `with_storage` attaches the caller-owned state; `build` cannot be called without it.
- `build` validates and constructs; it does **not** talk to the bus. Communication starts when the
  runner is polled.
- `port` is the application's byte stream. `runner` owns the hardware.

## Target

```rust,ignore
use protolink_adaptors::{Binding, TargetState, i2c_target};

let mut state = TargetState::new();
let Binding { port, runner } = i2c_target(target_driver, boot)
    .with_storage(&mut state)
    .build();
```

The target's own address is configured on the driver by the board, not by the adaptor. `boot`
identifies this boot of the target: draw it at random on each start, so that the controller can tell
a reboot from the connection it had. Building a target has nothing to validate and cannot fail, so
it returns the `Binding` directly. `target_driver` implements `protolink_i2c::target::I2cTarget`, see
[the HAL contract](#target-driver-contract).

A target starts unbound. Bytes the application writes before the controller opens the connection
wait in the send buffer and are delivered once it does.

## Drive the runner

Nothing is spawned for you. **The runner must be polled concurrently with the code that uses the
port**; constructing a port is not enough.

```rust,ignore
use embassy_futures::select::select;

select(runner.run(), async {
    let io = protolink::link::reliable(port)
        .with_retransmission_timer(arq_timer)
        .build();
    let mut client = protolink::client(io).with_deadline_timer(deadline_timer).build();
    client.unary("/pkg.Svc/Method", &request).await
}).await;
```

or give each its own task (`Port` and the runners are `Send` when the HAL types are):

```rust,ignore
static STATE: StaticCell<ControllerState<1>> = StaticCell::new();
let Binding { port, runner } = i2c_controller(i2c, timer)
    .with_address(0x42)
    .with_storage(STATE.init(ControllerState::new()))
    .build()?;
spawner.spawn(bus_task(runner))?;
spawner.spawn(app_task(port))?;
```

`ControllerRunner::run` never returns. A fault on one route ends only that route, and the other
routes keep their connections; [`run_observed`](#port-semantics) tells the application what
happened. `TargetRunner::run` returns only when the peripheral fails, as
`Result<Infallible, TargetError>`. Dropping or canceling either future tears the runner down: every
port then fails with `PortError::Closed` instead of hanging. A transaction interrupted that way is
**not** resumed; recovery is an explicit new `build`.

## Using the port with protolink

The port is the raw byte stream. COBS framing and ARQ are *not* added by `build`, because that would
hide a wire-format choice and a timer requirement. Layer them yourself, then assemble the driver and
join the services at `serve`:

```rust,ignore
let io = protolink::link::reliable(port)
    .with_retransmission_timer(arq_timer)
    .build();
let server = protolink::server(io).with_deadline_timer(deadline_timer).build();
let mut services = ServiceDispatch::new(&mut device);
select(runner.run(), server.serve(&mut services)).await;
```

I2C roles and RPC roles are independent: an I2C controller can host an RPC server, and an I2C target
can initiate RPC calls.

### Three different timers

| Timer | Where | Purpose |
|---|---|---|
| Controller pacing | `i2c_controller(i2c, timer)`, `.with_poll_interval(..)`, `.with_open_retry_interval(..)` | How long the runner sleeps when no peer has work (default 1 ms), and how long a route waits before it tries an unanswered open again (default 1 ms), counted from when that attempt completed |
| ARQ retransmission timer | `link::reliable(port).with_retransmission_timer(timer)` | Retransmit unacknowledged frames |
| RPC deadline timer | `protolink::{server, client}(io).with_deadline_timer(timer)` | Enforce `grpc-timeout` |

They are not interchangeable.

## Port semantics

| Operation / event | Contract |
|---|---|
| `read` | Returns the bytes available. An empty mailbox is *idle*, not EOF, so `read` waits. |
| `write` | Accepts a prefix when there is room, waits when there is none. |
| Cancel a pending port operation | No hardware operation is canceled; no accepted byte is lost. |
| Port `flush` | Every byte accepted so far has completed its I2C transfer. It fails with the connection's error when the connection ended first. |
| Reliable-link `flush` | ARQ peer acknowledgment, as documented in [LINK.md](LINK.md). |
| Bus or ambiguous transfer | The route's connection ends with `PortError::Hardware`. Bytes not yet transferred are discarded, never replayed. Buffered received bytes are still read first. |
| Peer protocol violation or overflow | The connection ends with `PortError::Protocol`. |
| Replaced, retired, rebooted, or aborted | The port fails with `PortError::Reset`, and its buffered bytes are discarded, whether or not the runner has processed the change yet. Attach again for the new connection. |
| Opening budget spent (controller) | The route ends with `RouteFault::OpenExhausted` and `PortError::Reset`, and no further opening operation is attempted. |
| Runner dropped / canceled | Waiting ports fail with `PortError::Closed`. |
| Target driver failure | `TargetRunner::run` returns the HAL error, and the driver is `disarm`ed before the buffers are released. |
| Route fault (controller) | Reported to `ControllerRunner::run_observed` with the HAL error, if any. The other routes continue. |

Dropping a port, or one of its halves, never ends the connection: the other half keeps working, and
an attachment continues the same connection with its buffered bytes. The explicit way to end a
connection is `Port::abort`, which resets the port and asks the peer to retire the connection.

Outgoing bytes are consumed only after their transaction succeeded, so a failed write or read
never removes them from the buffer silently: the connection ends, and the flush reports it.

## Connection lifecycle and reconstruction

A port is one handle to one connection. The protocol keeps the connection's identity on the wire
(see [Mailbox protocol](#mailbox-protocol)); the adaptor never reconstructs the connection or the
protocol state above it. Full state transitions and their test matrix are in
[I2C_CONNECTIONS.md](I2C_CONNECTIONS.md).

When a port fails with `PortError::Reset` (or `Hardware`/`Protocol`), the session built on it is over.
Recovery is the application's: it drops the stale port, attaches again through the `Attachment`
handle, and rebuilds its ARQ and RPC state over the new port. The new connection begins with a new
epoch, so nothing from the old one reaches it. Resuming an in-flight RPC, or exactly-once execution
of a request that was interrupted, is not provided.

```rust,ignore
let attachment = port.attachment();   // keep this; it outlives the port
drop(port);                           // the stale port, after its error
let port = attachment.attach().await?; // waits until the old connection has ended
// Rebuild everything above the port: the reliable link and the RPC client or server.
let io = protolink::link::reliable(port).with_retransmission_timer(arq_timer).build();
let mut client = protolink::client(io).with_deadline_timer(deadline_timer).build();
```

`attach` waits while the old connection is ending. It fails with `AttachError::Busy` when a half of the
port is still attached, even while the connection is ending, since waiting could then never succeed.
It also fails with `Busy` when another `attach` already waits on the same channel. Only one `attach`
waits at a time. Dropping a pending `attach` releases its place, so the next one can wait. A port
reserves both halves in one step, so a refused port holds neither. `try_attach` returns `Ending`
rather than waiting, when the connection is ending and no half is attached.
Dropping the last half of a replaced connection finishes it without any bus traffic, so the attachment
proceeds at once. The runner keeps serving the other routes meanwhile.

## Several peers on one controller bus

```rust,ignore
let mut state = ControllerState::<2>::new();
let mut controller = i2c_controller(i2c, timer)
    .with_poll_interval(Duration::from_millis(1))
    .with_storage(&mut state)
    .build()?;
let sensor = controller.open(0x42)?;
let actuator = controller.open(0x43)?;
let runner = controller.into_runner();
```

There is **one runner per bus**, not per peer. It serves the routes round-robin, one mailbox read
and at most one write per turn, so sustained traffic or a stalled consumer on one peer cannot
starve another. A route that waits for its target, after a failed discovery read or an unconfirmed
`OPEN`, is skipped until its own retry deadline, and neither its waiting nor the traffic of the other
routes moves that deadline.

### Opening, budgets and restarts

Each route discovers its target's boot identifier, then opens a connection with a new epoch. The
discovery reply supplies that identity only: its payload is never delivered to the port. A route
makes at most `with_open_attempts` opening operations, counting every failed read and every `OPEN`,
whether it succeeded or failed; the final permitted `OPEN` can still be confirmed. Epochs are
allocated per target address from a history kept in the `ControllerState`. Each address advances from
its own last epoch, so building again on the same state continues that sequence rather than restarting
it. Epochs advance modulo 65 535 and skip zero, and an epoch the target advertises is skipped as well.

A fresh state has no history. After a restart, seed it with `with_epoch_seed` to an epoch the previous
controller allocated, which the application must persist. The seed is the starting point of each
address that has never allocated an epoch; it never rewinds an address that has a history. Because
epochs wrap around, a seed is not a lower bound that lasts: an address can reuse an epoch after its
sequence has wrapped. An unadvertised pending replacement on the target can still collide. The history
costs 256 bytes per `ControllerState`, one slot per address. `open` rejects reserved addresses (outside
`0x08..=0x77`), duplicates and more peers than the state has routes (`BuildError`). `.with_address(a)` before `build` is the shorthand
for opening one port and taking the runner. Ports borrow the *state*, so they outlive the `Controller` value.

## Tuning

`ControllerState::<N>` and `TargetState` use 64-byte receive and send buffers per route. Choose
others with `with_buffers`:

```rust,ignore
let mut state = ControllerState::<2, 128, 256>::with_buffers(); // N, RX, TX
let mut state = TargetState::<128, 256>::with_buffers();
```

The controller's receive buffer must hold a full mailbox payload (24 bytes), enforced at compile
time. A controller only reads a peer while its receive buffer has room for 24 bytes, and goes back to
sleep when it has not; reading from the port wakes it. Storage names no HAL type, so it fits in a
`StaticCell`. `Port<'a>` defaults to the default sizes; `Port<'a, RX, TX>` names others.

## Mailbox protocol

Every transaction is one frame of at most 32 bytes: an 8-byte header, then payload. The format is
defined in `protolink_i2c::mailbox`, and the connection rules in
[I2C_CONNECTIONS.md](I2C_CONNECTIONS.md).

| Offset | Read reply (target to controller) | Write request (controller to target) |
|---|---|---|
| `0` | payload length, `0..=24`; `0` means idle, not EOF | payload length |
| `1` | flags: `SERVING` (the target serves the named connection), `RETIRE` | operation: `DATA`, `OPEN`, `RETIRE` |
| `2` | receive credit, free bytes saturated to 255 | zero |
| `3` | zero | zero |
| `4..6` | target boot identifier, little endian | target boot identifier, little endian |
| `6..8` | connection epoch served or retired; zero for none | connection epoch addressed; never zero |
| `8..` | payload, then ignored padding | payload (`DATA` only) |

- A connection is named by its boot identifier and an epoch. The controller chooses the epoch per
  route and never reuses a live one. Epochs are finite, so they can repeat after wraparound; the
  identity is isolation, not anti-replay protection for control requests.
- A target serves one connection. Data that names any other identity is ignored, so bytes of an
  earlier connection cannot reach a later one. A reply names the connection it serves.
- A malformed frame, or a reply that serves or retires a connection without naming one, is a
  protocol violation.

Both peers must run the same format. Packets are never retried below the reliable link.

## Target-driver contract

`embedded-hal` has no target role, so the board implements `protolink_i2c::target::I2cTarget`:

- `listen(buffer)` waits for the next transaction (`Command::Read` or `Command::Write(n)` with the
  bytes at the start of the 32-byte buffer; the bytes are one whole mailbox transaction).
- `respond_to_read(frame)` sends the mailbox and reports `ReadStatus::Done` only if the controller
  received all of it.
- `disarm()` stops servicing the bus and releases SCL. It must not wait for the bus to go idle, and
  must be safe to call at any time and more than once. The runner calls it on failure and on
  teardown, before its buffers are released.
- Dropping `listen` / `respond_to_read` mid-transaction leaves the peripheral in an unspecified
  state. The runner never resumes it; it disarms.
- The error type is an associated `Error: Debug` and is preserved in `TargetError::Hardware`.
- An `I2cTarget` error ends the runner. A protocol fault from the controller only ends the connection
  and is reported to the observer of `TargetRunner::run_observed`; the driver keeps serving.

## UART

A UART is a full-duplex, point-to-point stream with no bus arbitration. The contract is in
[UART.md](UART.md). Two paths exist, and the difference matters:

| Path | Use it when | Assembly |
|---|---|---|
| **Direct** | the HAL driver is already cancel-safe, with a read that loses no bytes and a write that is all-or-nothing per poll | pass the driver to `protolink::link::reliable` (or `client`/`server`) |
| **Buffered** | the RX and TX halves are split, and a transfer is aborted when its future is dropped (DMA) | `uart(rx, tx).with_storage(&mut state).build()` |

A direct driver needs no adaptor, so there is nothing to build. The buffered path is a thin facade
over the `pump` module of this crate: it provides the same vocabulary as I2C (`with_storage`, `build`, a
`Binding`, a runner to drive), with its own buffer defaults. The compiled assembly, with reliability
and the runner polled beside the server, is in
[API.md](API.md#embedded-buffered-uart-port-and-runner-optional-reliability-separate-services).

- `build` only borrows the storage: no peripheral I/O, no task spawned.
- The port is the pump's `PumpHandle`, not the I2C `Port`.
- Runner failure, cancellation and the error it returns are in
  [UART.md](UART.md#shutdown-and-teardown).

On Linux, `protolink_adaptors::uart::linux::LinuxUart` (feature `uart-linux`) wraps a configured
`tokio-serial` port. It is nonblocking, and opening it is separate from `build`. Its `flush` is not
wire-idle ([UART.md](UART.md#flush)). See [UART.md](UART.md#linux-wrapper) for the line settings,
exclusivity, permissions and DTR.

```rust,ignore
let raw = LinuxUart::open("/dev/ttyUSB0", 115_200)?;
let io = protolink::link::reliable(protolink::tokio::compat(raw)).build();
```

## Features

- `i2c` (default): the I2C adaptors.
- `uart`: the buffered UART factory (`uart`, `UartState`, `UartBuilder`, `UartRunner`). It adds
  no dependencies.
- `uart-linux`: the Linux serial wrapper over `tokio-serial`. Compiled only for Linux targets, and
  implies `uart`.
- `portable-atomic`, `portable-atomic-critical-section`: atomics for targets without native
  compare-and-swap (checked on `thumbv6m-none-eabi`). The second also needs a platform
  critical-section implementation.

The crates are `no_std` and the adaptors need no heap. The Linux wrapper is the only part that
uses `std`, and it is compiled only with `uart-linux` on Linux.
