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
| `protolink-grpc` | `grpc/` | Sans-IO unary gRPC server/client, status mapping, micropb codec |
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

See [`examples/embedded-device`](examples/embedded-device) for a complete, tested embedded-device
example, including a TCP server (`just example-server`) that can be queried with `grpcurl`.

## Compatibility profile

Supported:
- unary RPCs, uncompressed protobuf payloads
- `application/grpc` (and `+proto`) content types
- `grpc-status` / `grpc-message` (percent-encoded), trailers-only error responses
- bounded message sizes (4 KiB default, configurable)
- HTTP/2 h2c with preface, SETTINGS/PING acks, flow control, CONTINUATION, GOAWAY

Not supported:
- client, server and bidirectional streaming (generated servers answer `UNIMPLEMENTED`)
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
