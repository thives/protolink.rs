# Bus adaptors

`protolink-adaptors` turns a hardware bus into a byte stream that the rest of protolink already
understands. It uses the same vocabulary as the rest of protolink (see [API.md](API.md)):

> **Create a factory → configure it with `with_*` (including caller-owned storage) → `build` a port
> and a runner → use the port with protolink, and `run` the runner.**

| Crate | Owns | Does not own |
|---|---|---|
| `protolink-i2c` | Mailbox encoding, controller transaction primitives, the target-driver trait | Queues, RPC, COBS/ARQ, executor tasks |
| `protolink-adaptors` | `i2c_controller` / `i2c_target` / `uart`, buffered ports, route storage, scheduling, runners | Board-specific peripheral setup |
| `protolink` | RPC drivers, the reliable link, the ring and the DMA pump | Anything I2C specific |

There is no new transport trait: a port implements `embedded_io_async::{Read, Write}`, which is what
`protolink::client`, `protolink::server` and `protolink::link::reliable` already take.

## Controller, one peer

```rust,ignore
use protolink_adaptors::{Binding, ControllerState, i2c_controller};

let mut state = ControllerState::<1>::new();
let Binding { port, runner } = i2c_controller(i2c, delay)
    .with_address(0x42)
    .with_storage(&mut state)
    .build()?;
```

- `i2c` is any `embedded_hal_async::i2c::I2c`; `delay` any `DelayNs`.
- `with_storage` attaches the caller-owned state; `build` cannot be called without it.
- `build` validates and constructs; it does **not** talk to the bus. Communication starts when the
  runner is polled.
- `port` is the application's byte stream. `runner` owns the hardware.

## Target

```rust,ignore
use protolink_adaptors::{Binding, TargetState, i2c_target};

let mut state = TargetState::new();
let Binding { port, runner } = i2c_target(target_driver)
    .with_storage(&mut state)
    .build();
```

The target's own address is configured on the driver by the board, not by the adaptor. Building a
target has nothing to validate and cannot fail, so unlike the controller it returns the `Binding`
directly.
`target_driver` implements `protolink_i2c::target::I2cTarget`, see [the HAL contract](#target-driver-contract).

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
let Binding { port, runner } = i2c_controller(i2c, delay)
    .with_address(0x42)
    .with_storage(STATE.init(ControllerState::new()))
    .build()?;
spawner.spawn(bus_task(runner))?;
spawner.spawn(app_task(port))?;
```

`runner.run()` takes `self` and only returns on failure (`Result<Infallible, _>`). Dropping or
canceling the future tears the runner down: every port then fails with `PortError::Closed` instead
of hanging. A transaction interrupted that way is **not** resumed; recovery is an explicit new
`build`.

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
| Controller polling delay | `i2c_controller(i2c, delay)`, `.with_poll_interval(..)` | How long the controller sleeps when no peer has work (default 1 ms) |
| ARQ retransmission timer | `link::reliable(port).with_retransmission_timer(timer)` | Retransmit unacknowledged frames |
| RPC deadline timer | `protolink::{server, client}(io).with_deadline_timer(timer)` | Enforce `grpc-timeout` |

They are not interchangeable.

## Port semantics

| Operation / event | Contract |
|---|---|
| `read` | Returns the bytes available. An empty mailbox is *idle*, not EOF, so `read` waits. |
| `write` | Accepts a prefix when there is room, waits when there is none. |
| Cancel a pending port operation | No hardware operation is canceled; no accepted byte is lost. |
| Port `flush` | Every byte accepted so far has completed its I2C transfer. |
| Reliable-link `flush` | ARQ peer acknowledgment, as documented in [LINK.md](LINK.md). |
| Hardware failure | `run` returns the concrete HAL error. Ports read what was buffered, then fail with `PortError::Hardware` / `Protocol`; pending operations are woken. |
| Runner dropped / canceled | Waiting ports fail with `PortError::Closed`. |
| Target teardown | The driver is `disarm`ed before the buffers are released. |

Outgoing bytes are consumed only after their transaction succeeded, so a failed write or read
leaves them queued (and a pending `flush` fails rather than claiming completion).

## Several peers on one controller bus

```rust,ignore
let mut state = ControllerState::<2>::new();
let mut controller = i2c_controller(i2c, delay)
    .with_poll_interval(Duration::from_millis(1))
    .with_storage(&mut state)
    .build()?;
let sensor = controller.open(0x42)?;
let actuator = controller.open(0x43)?;
let runner = controller.into_runner();
```

There is **one runner per bus**, not per peer. It serves the routes round-robin, one mailbox read
and at most one write per turn, so sustained traffic or a stalled consumer on one peer cannot
starve another. `open` rejects reserved addresses (outside `0x08..=0x77`), duplicates and more
peers than the state has routes (`BuildError`). `.with_address(a)` before `build` is the shorthand
for opening one port and taking the runner. Ports borrow the *state*, so they outlive the `Controller` value.

## Tuning

`ControllerState::<N>` and `TargetState` use 64-byte receive and send buffers per route. Choose
others with `with_buffers`:

```rust,ignore
let mut state = ControllerState::<2, 128, 256>::with_buffers(); // N, RX, TX
let mut state = TargetState::<128, 256>::with_buffers();
```

The controller's receive buffer must hold a full mailbox payload (30 bytes), enforced at compile
time. A controller only reads a peer while its receive buffer has room for 30 bytes, and goes back to
sleep when it has not; reading from the port wakes it. Storage names no HAL type, so it fits in a
`StaticCell`. `Port<'a>` defaults to the default sizes; `Port<'a, RX, TX>` names others.

## Mailbox protocol

The wire format is the one the earlier private I2C link used, unchanged:

- A controller read always requests 32 bytes. Byte 0 is the payload length (`0..=30`, `0` = idle),
  byte 1 the target's receive credit (free receive bytes, saturated to 255), then the payload and
  ignored padding.
- A controller write carries raw payload bytes only (at most 32), never more than the advertised
  credit.
- Both peers must run the same format. Packets are never retried below the reliable link.

Failures are terminal: a bus error, malformed frame, write beyond the credit, oversized write or a
controller read that is not exactly one mailbox ends `run`.

## Target-driver contract

`embedded-hal` has no target role, so the board implements `protolink_i2c::target::I2cTarget`:

- `listen(buffer)` waits for the next transaction (`Command::Read` or `Command::Write(n)` with the
  bytes at the start of the 32-byte buffer).
- `respond_to_read(frame)` sends the mailbox and reports `ReadStatus::Done` only if the controller
  received all of it.
- `disarm()` stops servicing the bus and releases SCL. It must not wait for the bus to go idle, and
  must be safe to call at any time and more than once. The runner calls it on failure and on
  teardown, before its buffers are released.
- Dropping `listen` / `respond_to_read` mid-transaction leaves the peripheral in an unspecified
  state. The runner never resumes it; it disarms.
- The error type is an associated `Error: Debug` and is preserved in `TargetError::Hardware`.

## UART

A UART is a full-duplex, point-to-point stream with no bus arbitration. The contract is in
[UART.md](UART.md). Two paths exist, and the difference matters:

| Path | Use it when | Assembly |
|---|---|---|
| **Direct** | the HAL driver is already cancel-safe, with a read that loses no bytes and a write that is all-or-nothing per poll | pass the driver to `protolink::link::reliable` (or `client`/`server`) |
| **Buffered** | the RX and TX halves are split, and a transfer is aborted when its future is dropped (DMA) | `uart(rx, tx).with_storage(&mut state).build()` |

A direct driver needs no adaptor, so there is nothing to build. The buffered path is a thin facade
over `protolink::link::pump`: it provides the same vocabulary as I2C (`with_storage`, `build`, a
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
- `portable-atomic`, `portable-atomic-critical-section`: forwarded to `protolink` for targets
  without native atomics (checked on `thumbv6m-none-eabi`).

The crates are `no_std` and the adaptors need no heap. The Linux wrapper is the only part that
uses `std`, and it is compiled only with `uart-linux` on Linux.
