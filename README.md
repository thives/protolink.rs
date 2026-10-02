[![Crates.io](https://img.shields.io/crates/v/protolink)](https://crates.io/crates/protolink)
[![docs.rs](https://img.shields.io/docsrs/protolink)](https://docs.rs/protolink)
[![ci](https://github.com/thives/protolink.rs/actions/workflows/ci.yml/badge.svg)](https://github.com/thives/protolink.rs/actions/workflows/ci.yml)

# protolink
Protolink is a gRPC framework that unifies embedded transports, framing, reliability, HTTP/2, gRPC bridging, and code generation.

It is async first, `no_std + alloc`, with options for embedded-io, blocking I/O, and tokio.

> [!WARNING]
> This crate is in early development. The API is not yet stable and may change.

> [!CAUTION]
> This crate is not yet production-ready. It has not been widely tested and may contain bugs.

## Crates

| Crate | Path | Purpose |
|---|---|---|
| `protolink` | `.` | I/O drivers (async, blocking, tokio) and the COBS + ARQ link stack |
| `protolink-grpc` | `grpc/` | Sans-IO gRPC server/client (unary and streaming), status mapping, micropb codec |
| `protolink-http2` | `http2/` | Sans-IO HTTP/2 connection, built on `zerodds-http2` + `zerodds-hpack` |
| `protolink-grpc-gen` | `grpc-gen/` | Code generator: service traits, servers and clients for micropb messages |

```text
application  ── implements generated <Service> trait / calls <Service>Client
codegen      ── protolink-grpc-gen (service glue) + micropb-gen (messages)
gRPC         ── protolink-grpc
HTTP/2       ── protolink-http2
drivers      ── protolink (async / blocking / tokio)
link         ── protolink::link (COBS framing + ARQ reliability, optional)
transport    ── anything implementing embedded-io(-async) or tokio I/O
```

## Usage

`build.rs` — one pure-Rust parse feeds both generators, so `protoc` is not needed:

```rust
let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
let fdset = out.join("service.fdset");

let mut grpc = protolink_grpc_gen::Generator::new();
grpc.file_descriptor_set_path(&fdset);
grpc.compile_protos(&["proto/service.proto"], out.join("service_grpc.rs")).unwrap();

micropb_gen::Generator::new()
    .compile_fdset_file(&fdset, out.join("service.rs"))
    .unwrap();
```

Include both outputs in the same module, then implement the generated trait:

```rust
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/service.rs"));
    include!(concat!(env!("OUT_DIR"), "/service_grpc.rs"));
}

impl proto::service::Service for MyDevice {
    fn command(&mut self, ctx: &protolink::CallContext<'_>, request: Command) -> Result<Reply, protolink::Status> { /* ... */ }
}

// Server, on any embedded_io_async::{Read, Write} transport:
let mut handler = proto::service::ServiceServer(MyDevice::default());
protolink::serve(io, &mut handler, protolink::ServerConfig::default()).await?;

// Client:
let mut client = proto::service::ServiceClient::new(protolink::Client::new(io, Default::default()));
let reply = client.command(&request).await?;
```

### Streaming

For an RPC `Method`, the generated trait has:

| Shape | Trait methods |
|---|---|
| unary | `method(ctx, request) -> Result<Resp, Status>` |
| server streaming | `method(ctx, request)`, `poll_method(ctx, cx) -> Poll<Next<Resp>>`, `cancel_method(ctx)` |
| client streaming | `method(ctx, request)` per message, `poll_method(ctx, cx) -> Poll<Result<Resp, Status>>`, `cancel_method(ctx)` |
| bidirectional | `method(ctx, request)` per message, `end_method(ctx)`, `poll_method(ctx, cx) -> Poll<Next<Resp>>`, `cancel_method(ctx)` |

Servers stay sans-IO and executor-agnostic: every method gets a `CallContext` with the method path, a
`CallId` that identifies the call and its `deadline`; request messages are
delivered as they arrive, and responses are pulled with `poll_*` (wake `cx` when a pending response
becomes ready). Streaming methods default to `UNIMPLEMENTED`, so adding one to a `.proto` does not
break existing implementations.

Clients return typed calls:

```rust
let mut events = client.event_subscribe(&EventSubscribe {}).await?;
while let Some(event) = events.message().await? { /* ... */ }

let mut batch = client.command_batch().await?;
batch.send(&command).await?;
let summary = batch.finish().await?;

let mut stream = client.command_stream().await?;
stream.send(&command).await?;
let reply = stream.message().await?;
stream.close_send().await?;
```

Dropping a call before it ended cancels it. Several streaming calls can be active on one client at
the same time, for example `join!`ed or, with the `std` feature, on separate tasks:

```rust
let mut first = client.command_stream().await?;
let mut second = client.command_stream().await?;
first.send(&first_command).await?;
second.send(&second_command).await?;
let (a, b) = futures::join!(first.message(), second.message());
```

The blocking server polls `Pending` streaming handlers when a read returns or times out. A transport that can
wait for input or a wake-up can implement `blocking::WakeableRead` and be served with
`blocking::serve_wakeable`, which polls a handler as soon as it wakes its waker, with no read timeout.

The transport is used by one operation at a time. A pending `message()` gives way when another call
has something to write, which requires a cancel-safe transport `read` (tokio, the `link` stack and
`CobsFramed` are). The blocking client can't interrupt a read, so there you send on every call the
peer waits for before blocking on a response. Unary calls need `&mut` access to the client, so they
can't run while a streaming call exists.

See [`examples/embedded-device`](examples/embedded-device) for a complete, tested embedded-device
example, including a TCP server (`just example-server`) that can be queried with `grpcurl`.

## Deadlines

gRPC deadlines are supported: a client sends the time it is willing to wait as `grpc-timeout`, and the
call ends with `DEADLINE_EXCEEDED` when it runs out, on both sides. See
[`docs/DEADLINES.md`](docs/DEADLINES.md) for the design and the details below.

```rust
use std::time::Duration;
use protolink::{CallOptions, ClientConfig};

// One call:
let reply = client
    .command_with_options(&request, CallOptions::timeout(Duration::from_millis(500)))
    .await?;
let stream = client.command_stream_with_options(CallOptions::timeout(Duration::from_secs(30))).await?;

// Every call that doesn't set its own:
let config = ClientConfig { default_timeout: Some(Duration::from_secs(2)), ..Default::default() };
```

Every generated client method has a `<method>_with_options` variant. The timeout covers the whole call,
not each message. A zero timeout fails at once with `DEADLINE_EXCEEDED` without sending anything.

On the server, every handler method gets a `CallContext` with the call's `deadline`; `ctx.remaining(now)`
turns it into a budget for work the handler starts. Expired streaming calls are reported to `on_cancel`.

**Time source.** The sans-IO cores never read a clock: the driver reports time with `tick(now)` and
learns when to wake with `next_deadline()`. The drivers need a clock to enforce deadlines, and without
one still *send* `grpc-timeout` (so the server enforces it) but never expire a call themselves:

| Driver | Enforce deadlines with |
|---|---|
| tokio | nothing to do: `protolink::tokio::{client, serve}` use `TokioTimer` |
| async (`embedded-io`) | `Client::with_timer(io, config, timer)`, `serve_with_timer(io, handler, config, timer)`, for a `protolink::Timer` |
| blocking | `blocking::Client::with_clock(io, config, clock)`, `serve_with_clock`, `serve_wakeable_with_clock`, for a `protolink::Clock` and an `io` that implements `blocking::ReadTimeout` |

`Clock` is `now() -> Duration` (monotonic, since any fixed point); `Timer` adds
`sleep_until(deadline) -> impl Future<Output = ()>`. On `no_std`, implement them on the platform timer:

```rust
impl protolink::Clock for EmbassyTimer {
    fn now(&self) -> Duration { Duration::from_micros(embassy_time::Instant::now().as_micros()) }
}
impl protolink::Timer for EmbassyTimer {
    fn sleep_until(&self, deadline: Duration) -> impl Future<Output = ()> {
        embassy_time::Timer::at(embassy_time::Instant::from_micros(deadline.as_micros() as u64))
    }
}
```

(`protolink::Timer` is not `protolink::link::Timer`, the ARQ retransmission timer.)

Things to know:

- **A running unary handler can't be preempted.** `Handler::call` is synchronous, so a handler that
  overruns its deadline still returns its response, which a client with its own deadline no longer waits
  for. Long-running handlers should check `ctx.remaining(now)`.
- **A cancel-safe `read` is required** whenever a timer is used, also for a unary-only server or a client
  with one call: the driver drops its pending read when a deadline is reached (the same requirement as
  streaming handlers and concurrent calls; tokio streams, `CobsFramed` and the `link` stack qualify).
- **Blocking drivers bound the read with a read timeout.** They set it to the time left before each read
  (`blocking::ReadTimeout`, or `WakeableRead::read_or_wake_timeout`) and treat `ErrorKind::TimedOut` as an
  idle tick. Without that hook a deadline is only noticed when a read returns. `std` sockets report
  timeouts as `WouldBlock`, which `embedded-io` maps to `ErrorKind::Other`; map it to `TimedOut`.
- When a client's call expires, buffered messages are still delivered before the status, the stream is
  reset with `RST_STREAM(CANCEL)`, and the reset is written by the next operation on the client.
- A malformed `grpc-timeout` is answered with `INVALID_ARGUMENT`.

## Compression

Message compression (`grpc-encoding` / `grpc-accept-encoding`) is off by default. Turn it on with
`Compression` in `ClientConfig` / `ServerConfig`:

```rust
use protolink::{Compression, ServerConfig};

let config = ServerConfig {
    compression: Compression::gzip(), // needs the `miniz-oxide` feature
    ..ServerConfig::default()
};
```

The algorithm is behind two traits in `protolink::compression`, so a target can use what it already has:

| Trait | Implement it for | Gives you |
|---|---|---|
| `Deflate` | a raw DEFLATE library: miniz in a microcontroller's ROM, zlib, a hardware block | `Gzip<D>`, the `gzip` encoding, with header, CRC-32 and size trailer handled for you |
| `Codec` | any other algorithm (one `grpc-encoding` value) | that encoding |

`Deflate` is two methods, `deflate` and `inflate` (plus an optional `crc32` override for a ROM CRC). A
backend that can only decompress returns `CodecError::Unsupported` from `deflate` and leaves
`Compression::send` unset: it then accepts compressed requests and answers uncompressed.

```rust
use protolink::Compression;
use protolink::compression::{Codec, Gzip};

// `RomMiniz` implements `Deflate` on top of the ROM routines.
static GZIP: Gzip<RomMiniz> = Gzip::new(RomMiniz);
static ACCEPT: [&dyn Codec; 1] = [&GZIP];
let compression = Compression::new(&ACCEPT).send(&GZIP);
```

The `miniz-oxide` feature adds the stock pure-Rust backend (`MinizOxide`, the ready-made `GZIP` and
`Compression::gzip()`); it is not enabled by default, and its compressor needs a lot of working memory.

- A server compresses a response only if the client lists the encoding in `grpc-accept-encoding`. A
  client compresses every request with `Compression::send`, so only set it for a server that supports it.
  A server answers an unsupported `grpc-encoding` with `UNIMPLEMENTED` and its `grpc-accept-encoding`.
- Messages shorter than `Compression::min_size` (64 bytes by default), and messages that do not get
  smaller, are sent uncompressed.
- `max_message_size` limits the *decompressed* size, so a small message that expands is rejected with
  `RESOURCE_EXHAUSTED` instead of being allocated. A `Codec` must honour the `limit` it is given.
- Compressed messages stay compressed while they wait to be taken, and flow-control credit is counted
  in bytes on the wire.

## Compatibility profile

Supported:
- unary, server-streaming, client-streaming and bidirectional streaming RPCs
- protobuf payloads, optionally compressed with a pluggable codec (see [Compression](#compression))
- `application/grpc` (and `+proto`) content types
- `grpc-status` / `grpc-message` (percent-encoded), trailers-only error responses
- bounded message sizes (4 KiB default, configurable) and bounded buffering under HTTP/2 flow control
- deadlines: `grpc-timeout` is sent and enforced, with `DEADLINE_EXCEEDED` (see [Deadlines](#deadlines))
- HTTP/2 h2c with preface, SETTINGS/PING acks, flow control, CONTINUATION, GOAWAY

Not supported:

- custom metadata
- reflection, health checking, interceptors, TLS

Interoperability is tested against the `h2` crate (the HTTP/2 stack under hyper and tonic).

## Status of the reliable link

`protolink::link` composes ARQ (`arq-io-async`) over COBS (`cobs-io-async`). The raw ARQ,
composed-link, and embedded-device end-to-end tests are enabled in `tests/link.rs` and
`examples/embedded-device/tests/e2e.rs`. COBS framing alone (`protolink::link::CobsFramed`) is also tested.

ARQ retransmits on a timeout, so it needs a time source (the `arq_io_async::Timer` trait, re-exported
as `protolink::link::Timer`, which is not the `protolink::Timer` used for [deadlines](#deadlines)):

| Constructor | Requires | Timer |
|---|---|---|
| `link::reliable_with_timer(raw, timer)` | `async` | any `Timer` implementation; the `no_std` path |
| `link::reliable(raw)` | `async` + `std` (implied by `tokio`) | `link::StdTimer` (a helper thread per link; works with any executor) |

On `no_std` targets, implement `Timer` on top of the platform timer (for example `embassy-time`) and
call `reliable_with_timer`. `std` is the only feature that turns on ARQ's `std` feature, so embedded
builds do not pull it in.

### DMA UARTs

ARQ drops its lower layer's futures after every poll, so the raw stream must be cancel-safe. Buffered
and interrupt-driven UARTs are; one-shot DMA drivers are not (a dropped read loses bytes, a dropped
write is aborted part-way). `link::pump` is an optional lower layer for those: it owns the DMA
halves, completes every transfer, and gives the link a cancel-safe `PumpHandle`. See the module
docs for when to use it, its caveats, and a usage example.
