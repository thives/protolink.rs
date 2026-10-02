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
    fn command(&mut self, request: Command) -> Result<Reply, protolink::Status> { /* ... */ }
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
| unary | `method(request) -> Result<Resp, Status>` |
| server streaming | `method(call, request)`, `poll_method(call, cx) -> Poll<Next<Resp>>`, `cancel_method(call)` |
| client streaming | `method(call, request)` per message, `poll_method(call, cx) -> Poll<Result<Resp, Status>>`, `cancel_method(call)` |
| bidirectional | `method(call, request)` per message, `end_method(call)`, `poll_method(call, cx) -> Poll<Next<Resp>>`, `cancel_method(call)` |

Servers stay sans-IO and executor-agnostic: calls are identified by a `CallId`, request messages are
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

## Compatibility profile

Supported:
- unary, server-streaming, client-streaming and bidirectional streaming RPCs
- uncompressed protobuf payloads
- `application/grpc` (and `+proto`) content types
- `grpc-status` / `grpc-message` (percent-encoded), trailers-only error responses
- bounded message sizes (4 KiB default, configurable) and bounded buffering under HTTP/2 flow control
- HTTP/2 h2c with preface, SETTINGS/PING acks, flow control, CONTINUATION, GOAWAY

Not supported:

- compression, deadlines (`grpc-timeout` is ignored), custom metadata
- reflection, health checking, interceptors, TLS

Interoperability is tested against the `h2` crate (the HTTP/2 stack under hyper and tonic).

## Status of the reliable link

`protolink::link` composes ARQ (`arq-io-async`) over COBS (`cobs-io-async`). The raw ARQ,
composed-link, and embedded-device end-to-end tests are enabled in `tests/link.rs` and
`examples/embedded-device/tests/e2e.rs`. COBS framing alone (`protolink::link::CobsFramed`) is also tested.

ARQ retransmits on a timeout, so it needs a time source (the `arq_io_async::Timer` trait, re-exported
as `protolink::link::Timer`):

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
