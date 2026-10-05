# gRPC Deadlines: Design and Implementation Record

gRPC deadlines are part of the gRPC over HTTP/2 protocol: the client sends the time it is willing to
wait as the `grpc-timeout` request header, and the call ends with `DEADLINE_EXCEEDED` (status 4) when it
runs out. Both sides enforce it: the client stops waiting, and the server stops working on the call.

**Status: done** for unary and all streaming shapes, in the sans-IO cores and in the async, blocking and
tokio drivers.

## Clock model

The sans-IO cores (`protolink_grpc::{Client, Server}`) never read a clock, so they stay `no_std` and
testable. The caller reports time instead:

- `tick(now)` takes a monotonic `core::time::Duration` since any fixed point. Values earlier than one
  already reported are ignored. It expires calls whose deadline has been reached.
- `next_deadline()` returns when the earliest pending deadline is reached, on the same clock, so a
  driver knows when to wake up.
- `now()` returns the latest time reported.

The drivers turn this into traits in `protolink`:

| Trait | Provides | Used by |
|---|---|---|
| `Clock` | `now() -> Duration` | blocking drivers |
| `Timer: Clock` | `sleep_until(deadline) -> impl Future<Output = ()>` | async drivers |
| `NoTimer` | `now()` is always zero, `sleep_until` never completes | the default; no enforcement |
| `tokio::TokioTimer` | tokio's clock (follows `tokio::time::pause`) | `protolink::tokio::{client, serve}` |
| `blocking::ReadTimeout` | `set_read_timeout(Option<Duration>)` | blocking drivers, to bound a read |

`protolink::Timer` is unrelated to `protolink::link::Timer` (`arq_io_async::Timer`, the ARQ
retransmission timer). A target usually implements both on top of its platform timer.

## Semantics

- **Start.** A client's deadline starts when the call is started, a server's when the request headers
  arrive. Both are relative budgets, so the clocks of the peers don't have to agree.
- **Header.** The client sends `grpc-timeout` right after `:authority`, in the finest unit that fits in
  8 digits (`n`, `u`, `m`, `S`, `M`, `H`), rounded up so the budget is never shortened. Budgets beyond
  `99999999H` are clamped. A client without a clock (`NoTimer`) still sends the header, so the server
  enforces the deadline.
- **Options.** `CallOptions::timeout` per call, `ClientConfig::default_timeout` for every call that
  doesn't set one. Generated clients have `<method>_with_options(.., options)` next to each method.
- **Zero timeout.** Fails locally with `DEADLINE_EXCEEDED` without sending anything.
- **Client expiry.** The call fails with `DEADLINE_EXCEEDED` and the stream is reset with
  `RST_STREAM(CANCEL)`. Messages that were already received are delivered before the status. A late
  response is ignored.
- **Server expiry.** The call ends with a trailers-only (or trailing) `DEADLINE_EXCEEDED` response.
  Streaming handlers get `Handler::on_cancel`, exactly once. A call that has already expired when its
  request completes never reaches the handler.
- **Malformed header.** A malformed `grpc-timeout` is answered with `INVALID_ARGUMENT`. `0n` is accepted
  and means already expired.
- **Handlers.** Every `Handler` method gets a `CallContext` (with `path`, `id` and `deadline`). `deadline` is on the
  server's clock and `remaining(now)` turns it into a budget for downstream work.

## Drivers

| Driver | Enforce with | Notes |
|---|---|---|
| sans-IO | call `tick(now)` and wake at `next_deadline()` | |
| async client | `Client::with_timer(io, config, timer)` | `Client::new` has no timer |
| async server | `serve_with_timer(io, handler, config, timer)` | `serve` has no timer |
| tokio | `protolink::tokio::{client, serve}` | use a `TokioTimer` |
| blocking client | `blocking::Client::with_clock(io, config, clock)` | `io: ReadTimeout` |
| blocking server | `blocking::serve_with_clock`, `serve_wakeable_with_clock` | `io: ReadTimeout`, or `WakeableRead::read_or_wake_timeout` |

The async drivers race the transport `read` against `timer.sleep_until(next_deadline())`. When the
timer wins, the read is dropped and the loop ticks. A call that is started later with an earlier
deadline wakes the reader through the same mechanism that lets another call write.

The blocking drivers set the transport's read timeout to the time left until the earliest deadline
before each read, and treat the resulting `ErrorKind::TimedOut` as an idle tick when a clock is in use.

## Limitations

- **A running unary handler can't be preempted.** `Handler::call` is synchronous and the server's clock
  only moves between calls to `tick`, so a handler that overruns its deadline still has its response
  sent. A client with its own deadline has given up by then. A handler that does long work should check
  `ctx.remaining(now)` itself and return `DEADLINE_EXCEEDED` early.
- **Waiting for a deadline drops a pending read.** With a timer, the transport `read` must be cancel-safe
  even for a unary-only server or a client with one call (see "Known limitations" in
  `STREAMING_GAP.md`). tokio streams, `CobsFramed` and the `link` stack are. A one-shot DMA UART goes
  behind `link::pump`.
- **Deadlines are only noticed while the driver is running.** An async `Client` expires calls while one of
  its operations is polled. The reset of an expired call is written by the next operation on the client.
- **Blocking reads need a timeout hook.** Without `ReadTimeout` (or `read_or_wake_timeout`) a blocking
  driver only notices a deadline when a read returns. `std` sockets report timeouts as `WouldBlock`,
  which `embedded_io` maps to `ErrorKind::Other`: map it to `TimedOut` in the adapter.
- **No propagation.** A server doesn't forward `ctx.deadline` automatically; the handler decides.

## Tests

| Area | Tests |
|---|---|
| `timeout` parse/format | `protolink-grpc/src/timeout.rs` |
| Sans-IO client, server, handler context | `protolink-grpc/src/tests_deadline.rs` |
| Async drivers, paused tokio clock | `protolink/tests/deadline.rs` |
| Blocking drivers | `protolink/tests/blocking_deadline.rs` |
| `h2` interop (wire format, malformed values, `RST_STREAM(CANCEL)`) | `protolink/tests/h2_interop.rs`, module `deadlines` |
| Generated clients | `protolink-grpc-gen` unit tests, `examples/embedded-device/tests/e2e.rs` |
| `grpcurl -max-time` | CI job `grpcurl-interop` |

The CI stress job repeats the deadline test binaries.
