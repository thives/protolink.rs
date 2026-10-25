# Assembling protolink: the public API contract

Every protolink stack is assembled the same way. The application object implements generated
service traits; a generated dispatcher (or a registry of them) is borrowed by a server driver for
one connection; the driver owns the protocol stack down to the byte stream.

```text
Application object
    ↓ implements generated traits
Generated dispatcher / service registry
    ↓ borrowed for the connection lifetime
Server driver
    ↓ owns
gRPC → HTTP/2 → optional reliable link → byte stream
```

The driver never implements generated service traits and never permanently owns application
services. The execution boundary is always:

```rust,ignore
server.serve(&mut services).await?;
```

The examples on this page are compiled as doctests of `examples/embedded-device`, against its
generated `Service` and `Diagnostics` bindings.

## Vocabulary

| Operation | Contract |
|---|---|
| `server(io)`, `client(io)` | Create factories |
| `.with_*()` | Consume and configure a factory |
| `.as_*()` | Borrowed view or adapter, not an ownership transfer |
| `.into_*()` | Consuming conversion |
| `.build()` | Construct and validate; no transport I/O or task spawning |
| `.serve(&mut services)` | Drive one server connection |
| `.run()` | Drive an adaptor or pump runner |

The same words are used everywhere:

| Factory | Configure | Build | Drive |
|---|---|---|---|
| `protolink::server(io)` | `with_deadline_timer`, `without_deadlines`, `with_max_message_size`, `with_compression`, `with_config`, ... | `Server` | `serve(&mut services).await` |
| `protolink::client(io)` | `with_deadline_timer`, `without_deadlines`, `with_default_timeout`, ... | `Client` (a transport for generated clients) | its calls |
| `protolink::client_split(reader, writer)`, `protolink::server_split(reader, writer)` | as above | `Client`, `Server` on separate halves | as above |
| `protolink::client_reliable(link)`, `protolink::server_reliable(link)` | as above | `Client`, `Server` on one coordinated link | as above |
| `protolink::link::cobs_split(reader, writer)` | — | `(CobsReader, CobsWriter)`, the halves of a COBS stream | passed to `client_split` or `server_split` |
| `protolink::tokio::{server, client}(stream)` | as above; the deadline timer is already chosen | `Server`, `Client` | as above |
| `protolink::blocking::{server, client}(io)` | `with_deadline_clock`, `without_deadlines`, `with_output_timeouts`, `with_wakeable_reads` (server) | `blocking::Server`, `blocking::Client` | `serve(&mut services)` |
| `protolink::link::reliable(raw)` | `with_retransmission_timer` | `ReliableLink` | used as the driver's byte stream |
| `protolink::Registry::builder()` | `with_service(dispatcher)` | `Registry` (fails on duplicate paths) | passed to `serve` |
| `protolink::MultiDispatch::builder(app)` | `with_service(<Service>Methods)` | `MultiDispatch` (fails on duplicate paths) | passed to `serve` |
| `i2c_controller(i2c, delay)`, `i2c_target(driver)` | `with_address`, `with_poll_interval`, `with_storage` | `Binding { port, runner }` | `runner.run().await` |
| `uart(rx, tx)` | `with_storage` | `Binding { port, runner }` | `runner.run().await` |
| `protolink_grpc_gen::Generator::{micropb, prost}()` | `with_*` | — | `compile_protos` |

## Decisions

- **Separate namespaces.** Async factories are at the crate root, blocking ones in
  `protolink::blocking`, tokio ones in `protolink::tokio`.
- **Tokio supplies a deadline timer.** `protolink::tokio::{server, client}` start with a
  `TokioTimer`; `without_deadlines()` or `with_deadline_timer(..)` replace it.
- **Embedded async factories require a deadline policy.** `protolink::{server, client}` have no
  `build` until `.with_deadline_timer(timer)` or `.without_deadlines()` is chosen.
- **Blocking factories require an explicit deadline policy too**: `.with_deadline_clock(clock)`
  (the transport must bound reads, `ReadTimeout`) or `.without_deadlines()`. Each further
  capability is only available when the transport implements it: `.with_output_timeouts()` needs
  `OutputTimeout`, `.with_wakeable_reads()` needs `WakeableRead`, and wakeable reads with a deadline
  clock need `WakeableReadTimeout`, which has no default implementation that could silently ignore
  the timeout.
- **`build` returns `Result` only where construction can fail**: registries and multi-service
  dispatchers (duplicate paths) and I2C controllers (addresses, capacity, poll interval). Drivers,
  links and I2C targets cannot fail to build.
- **Construction performs no I/O**: no HTTP/2 preface, no SETTINGS, no bus traffic. Clients send
  their preface with the first call; servers when `serve` starts; adaptors when the runner runs.
- **Reliability is explicit.** No driver or adaptor adds COBS or ARQ; `link::reliable` does.
- **Low-level constructors remain.** The sans-I/O `protolink::grpc::{Server, Client}` state
  machines, `CobsFramed`, and hand-written `Handler` implementations (the dynamic routing facility,
  for example `FnHandler`) are still available.
- **Routing has one authority.** `Handler::route` resolves a path once per call, from the generated
  `METHODS` tables; every later event of the call carries that route.
- **Call state is connection-local.** `serve` borrows one dispatcher for one connection. Business
  state shared between connections lives behind handles the application chooses.

## A single service

The application object stays the application's: the dispatcher borrows it for the connection and
returns it afterwards.

```rust,no_run
use embedded_device_example::{Device, ServiceDispatch};

async fn serve_one_service(stream: tokio::net::TcpStream) -> Result<Device, Box<dyn std::error::Error>> {
    let mut device = Device::default();
    {
        let mut services = ServiceDispatch::new(&mut device);
        let server = protolink::tokio::server(stream).build();
        server.serve(&mut services).await?;
    }
    // The dispatcher's borrow has ended.
    Ok(device)
}
```

The owned form is equally simple: `ServiceDispatch::new(Device::default())`, with `get_ref`,
`get_mut` and `into_inner` to reach the device.

## Several independent services

Each registered dispatcher owns or borrows its own object. Building the registry rejects two
services that serve the same path.

```rust,no_run
use embedded_device_example::{Device, Diagnostics, DiagnosticsDispatch, ServiceDispatch};
use embedded_device_example::device::{Counters, GetCounters};
use protolink::{CallContext, Registry, Status};

/// Health reporting that is not part of the device.
#[derive(Default)]
struct Monitor;

impl Diagnostics for Monitor {
    fn get_counters(&mut self, _: &mut CallContext<'_>, _: GetCounters) -> Result<Counters, Status> {
        Ok(Counters::default())
    }
}

async fn serve_independent_services(
    stream: tokio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut device = Device::default();
    let mut monitor = Monitor;
    let mut services = Registry::builder()
        .with_service(ServiceDispatch::new(&mut device))
        .with_service(DiagnosticsDispatch::new(&mut monitor))
        .build()?;
    protolink::tokio::server(stream).build().serve(&mut services).await?;
    Ok(())
}
```

## One object implementing several services

The object is held once; the stateless generated `<Service>Methods` borrow it for each dispatch,
so nothing is cloned or locked.

```rust,no_run
use embedded_device_example::{Device, DiagnosticsMethods, ServiceMethods};

async fn serve_one_object(stream: tokio::net::TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut device = Device::default();
    let mut services = protolink::MultiDispatch::builder(&mut device)
        .with_service(ServiceMethods)
        .with_service(DiagnosticsMethods)
        .build()?;
    protolink::tokio::server(stream).build().serve(&mut services).await?;
    Ok(())
}
```

## A gateway: local services and forwarded calls

A router serves one caller connection. Some methods are served by local services, and the rest are
forwarded to a downstream connection and relayed back. Both are registered on the same builder, and
duplicate paths fail the build. The local services are owned by the router, so the device is moved
into them rather than borrowed.

```rust,no_run
use embedded_device_example::proto::diagnostics::METHODS as DIAGNOSTICS;
use embedded_device_example::{Device, ServiceMethods};

async fn serve_gateway(
    caller: tokio::net::TcpStream,
    downstream: tokio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let local = protolink::MultiDispatch::builder(Device::default())
        .with_service(ServiceMethods)
        .build()?;
    let mut router = protolink::grpc::Router::builder()
        .with_service(local)
        .with_route(
            DIAGNOSTICS,
            protolink::grpc::Client::new(protolink::grpc::ClientConfig::default()),
        )
        .build()?;
    let mut downstreams = [protolink::tokio::compat(downstream)];
    protolink::tokio::server(caller)
        .build()
        .serve_router(&mut router, &mut downstreams)
        .await?;
    Ok(())
}
```

`serve_router` takes one transport per downstream, in the order the routes were added. A
downstream whose transport fails is marked failed: its calls end `UNAVAILABLE`, and the caller's
other calls carry on. Several downstreams that serve the same service can be pooled with
`with_pool`, see [the routing design](ROUTING.md).

The tested guarantees of the driver:

- A write or flush that stays pending on one downstream does not hold up the other downstreams,
  the local services or the deadlines. A transport still runs one operation at a time, so its
  reads wait while its own writes are pending. Its `read` must be cancel-safe, because it is dropped
  when that transport has output to write.
- Output blocked past the deadline of a downstream's own calls retires that downstream only. The
  gateway's deadline is its own calls' deadline, and a downstream's deadline does not retire the
  gateway.
- Once `Router::shutdown` has been called, `serve_router` returns when the gateway's output has
  been delivered and nothing is left to write. It does not wait for the caller to hang up.
- The future is `Send` when the transports are, so it can be spawned on tokio. It panics if the
  number of downstream transports differs from the router's downstream connections.

## Tokio

```rust,no_run
use core::time::Duration;
use embedded_device_example::device::{Command, GetStatus, Command_};
use embedded_device_example::ServiceClient;

async fn call_a_device(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    let transport = protolink::tokio::client(stream)
        .with_default_timeout(Duration::from_secs(5))
        .build();
    let mut client = ServiceClient::new(transport);
    let command = Command {
        command: Some(Command_::Command::GetStatus(GetStatus {})),
        ..Command::default()
    };
    let reply = client.command(&command).await?;
    println!("{reply:?}");
    Ok(())
}
```

## Embedded: explicit deadline policy

Without tokio, the deadline policy is a deliberate choice, for the server and the client alike:

```rust,no_run
use embedded_device_example::{Device, ServiceClient, ServiceDispatch};
use embedded_io_async::{Read, Write};

async fn embedded_server<IO: Read + Write, T: protolink::Timer>(
    io: IO,
    deadline_timer: T,
    device: &mut Device,
) -> Result<(), protolink::Error<IO::Error>> {
    let server = protolink::server(io)
        .with_deadline_timer(deadline_timer)
        .with_max_message_size(1024)
        .build();
    server.serve(&mut ServiceDispatch::new(device)).await
}

fn embedded_client<IO: Read + Write>(
    io: IO,
) -> ServiceClient<protolink::Client<protolink::Serial<IO>>> {
    // `grpc-timeout` is still sent; the server enforces it.
    ServiceClient::new(protolink::client(io).without_deadlines().build())
}
```

Leaving the policy out does not compile:

```rust,compile_fail
async fn no_policy<IO: embedded_io_async::Read + embedded_io_async::Write>(io: IO) {
    let server = protolink::server(io).build();
}
```

Blocking drivers check transport capabilities the same way: a deadline clock needs a transport that
can bound its reads.

```rust,compile_fail
fn unbounded_reads<IO: embedded_io::Read + embedded_io::Write, C: protolink::Clock>(io: IO, clock: C) {
    let server = protolink::blocking::server(io).with_deadline_clock(clock).build();
}
```

## Embedded: I2C port and runner, optional reliability, separate services

The adaptor yields a port (the byte stream) and a runner (owns the bus). Reliability is layered
explicitly, the server is assembled on top, and the services are joined only at `serve`. Nothing
is spawned: the runner is polled next to the server.

```rust,no_run
use core::time::Duration;
use embedded_device_example::{Device, DiagnosticsMethods, ServiceMethods};
use embedded_hal_async::{delay::DelayNs, i2c::I2c};
use protolink_adaptors::{Binding, ControllerState, i2c_controller};

async fn device_over_i2c<I, D, A, T>(i2c: I, delay: D, arq_timer: A, deadline_timer: T)
where
    I: I2c,
    D: DelayNs,
    A: protolink::link::Timer,
    T: protolink::Timer,
{
    // Caller-owned storage; a `static` (e.g. a `StaticCell`) on a device.
    let mut state = ControllerState::<1>::new();
    let Binding { port, runner } = i2c_controller(i2c, delay)
        .with_address(0x42)
        .with_poll_interval(Duration::from_millis(1))
        .with_storage(&mut state)
        .build()
        .expect("a valid peer address");

    let io = protolink::link::reliable(port)
        .with_retransmission_timer(arq_timer)
        .build();
    let server = protolink::server(io).with_deadline_timer(deadline_timer).build();

    let mut device = Device::default();
    let mut services = protolink::MultiDispatch::builder(&mut device)
        .with_service(ServiceMethods)
        .with_service(DiagnosticsMethods)
        .build()
        .expect("services with distinct paths");

    // Poll both: the runner moves the bytes the server reads and writes.
    // With embassy: `select(runner.run(), server.serve(&mut services)).await`.
    tokio::select! {
        failed = runner.run() => {
            let _bus_error = failed;
        }
        served = server.serve(&mut services) => {
            let _connection_ended = served;
        }
    }
}
```

## Embedded: buffered UART port and runner, optional reliability, separate services

A UART with split, separately owned halves is assembled like the I²C port: the runner owns the
halves, the port is the byte stream, and nothing is spawned. A UART whose driver is already
cancel-safe needs no adaptor and is passed to `protolink::link::reliable` directly. See
[UART.md](UART.md) for the contract and the flush meanings.

```rust,no_run
use embedded_device_example::{Device, ServiceDispatch};
use embedded_io_async::{Read, Write};
use protolink_adaptors::{Binding, UartState, uart};

async fn device_over_uart<R, W, A, T>(uart_rx: R, uart_tx: W, arq_timer: A, deadline_timer: T)
where
    R: Read,
    W: Write,
    A: protolink::link::Timer,
    T: protolink::Timer,
{
    // Caller-owned storage; a `static` (e.g. a `StaticCell`) on a device.
    let mut state = UartState::new();
    let Binding { port, runner } = uart(uart_rx, uart_tx)
        .with_storage(&mut state)
        .build();

    let io = protolink::link::reliable(port)
        .with_retransmission_timer(arq_timer)
        .build();
    let server = protolink::server(io).with_deadline_timer(deadline_timer).build();

    let mut device = Device::default();
    let mut services = ServiceDispatch::new(&mut device);

    // Poll both: the runner moves the bytes, and returns only when a HAL half fails.
    tokio::select! {
        failed = runner.run() => {
            let _uart_error = failed;
        }
        served = server.serve(&mut services) => {
            let _connection_ended = served;
        }
    }
}
```

## Transport modes: raw, split COBS and coordinated reliable

Three assemblies serve the same generated services. Each is shown on its own; the
client side is the same with `protolink::client_split`, `client_reliable` or `client`.
Every one needs a deadline policy, and each example takes `without_deadlines()`
to keep the snippet short. Use `with_deadline_timer` in real code.

Raw, split halves: any stream the application can split into a reader and a writer, such
as the two halves of a UART or I²C port (see [UART.md](UART.md)). Each half is driven by its
own future, so the server reads requests while its replies are blocked.

```rust,no_run
use embedded_device_example::{Device, ServiceDispatch};
use embedded_io_async::{Read, Write};

async fn device_over_split_halves<R, W>(reader: R, writer: W, device: &mut Device)
where
    R: Read,
    W: Write<Error = R::Error>,
{
    let mut services = ServiceDispatch::new(device);
    let _ = protolink::server_split(reader, writer)
        .without_deadlines()
        .build()
        .serve(&mut services)
        .await;
}
```

Split COBS: the same halves framed with COBS, one zero-delimited frame per write. The
halves share no state, so a read can wait while a write is in progress on the other half.
Use it when the link does not need retransmission but the byte stream may carry noise:
corrupt frames are dropped, and the next delimiter resynchronises the stream.

```rust,no_run
use embedded_device_example::{Device, ServiceDispatch};
use embedded_io_async::{Read, Write};

async fn device_over_split_cobs<R, W>(reader: R, writer: W, device: &mut Device)
where
    R: Read,
    W: Write<Error = R::Error>,
{
    let (reader, writer) = protolink::link::cobs_split(reader, writer);
    let mut services = ServiceDispatch::new(device);
    let _ = protolink::server_split(reader, writer)
        .without_deadlines()
        .build()
        .serve(&mut services)
        .await;
}
```

Coordinated reliable: ARQ over COBS over a raw stream. It retransmits lost frames, so it
needs a retransmission timer. One engine owns both directions, so the link is not split.

```rust,no_run
use embedded_device_example::{Device, ServiceDispatch};
use embedded_io_async::{Read, Write};
use protolink::link::Timer;

async fn device_over_reliable<S, T>(raw: S, arq_timer: T, device: &mut Device)
where
    S: Read + Write,
    T: Timer,
{
    let link = protolink::link::reliable(raw)
        .with_retransmission_timer(arq_timer)
        .build();
    let mut services = ServiceDispatch::new(device);
    let _ = protolink::server_reliable(link)
        .without_deadlines()
        .build()
        .serve(&mut services)
        .await;
}
```

Restrictions:

- **Serial mode is not duplex.** `protolink::client(io)`, `protolink::server(io)` and
  `protolink::tokio::{client_serial, server_serial}` read only when they are not writing. On a
  transport that buffers less than the data in flight, a call that sends many requests before
  reading can block both peers. Use the split or duplex factories for that traffic, or interleave
  `send` and `message`, as the streaming documentation describes.
- **Blocking factories keep their limits.** `protolink::blocking::{server, client}` are not
  duplex, and a blocking read cannot be interrupted. See [STREAMING_GAP.md](STREAMING_GAP.md).
- **Error types are the same as the unsplit factories.** A split or reliable endpoint fails with
  `protolink::Error<E>` for its transport error `E`, and a reliable link reports its ARQ failure
  inside `ReliableError`. A link that ends because its peer hung up is reported as a closed link.
  An output deadline or an overload is never a clean end.
- **No background client progress.** A client has no task of its own. It reads and writes only
  while one of its calls is polled, and a response that arrives while no call is polled waits
  in the transport. Keep a call polled, for example by running the server and the application in
  one `select`, when nothing else drives the connection.

## Ownership and cancellation

- `serve` consumes the driver and borrows the dispatcher for the whole connection. When the
  connection ends, normally or with an error, every active streaming call is reported to
  `on_cancel` before `serve` returns.
- Dropping the async `serve` future drops the transport and makes the same `on_cancel` calls from
  its destructor. Nothing is written to the peer in that case.
- A blocking `serve` that unwinds from a panicking handler attempts no cleanup.
- Adaptor runners own the hardware; dropping `run()` tears the runner down and the ports fail with
  `PortError::Closed`.
- A buffered UART runner owns both halves. When `run()` returns or is dropped, the port reads its
  buffered bytes and then fails: `BrokenPipe` for a cancelled runner, or the HAL's error kind for
  the direction that failed. The runner's error keeps the concrete RX or TX error. A failed session
  is rebuilt, not restarted.

## Migration

| Removed | Replacement |
|---|---|
| `protolink::serve(io, &mut h, config)` | `protolink::server(io).with_config(config).without_deadlines().build().serve(&mut h)` |
| `protolink::serve_with_timer(io, &mut h, config, timer)` | `protolink::server(io).with_config(config).with_deadline_timer(timer).build().serve(&mut h)` |
| `protolink::Client::new(io, config)` | `protolink::client(io).with_config(config).without_deadlines().build()` |
| `protolink::Client::with_timer(io, config, timer)` | `protolink::client(io).with_config(config).with_deadline_timer(timer).build()` |
| `protolink::tokio::serve(stream, &mut h, config)` | `protolink::tokio::server(stream).with_config(config).build().serve(&mut h)` |
| `protolink::tokio::client(stream, config)` | `protolink::tokio::client(stream).with_config(config).build()` |
| `blocking::serve(io, &mut h, config)` | `blocking::server(io).without_deadlines().build().serve(&mut h)` |
| `blocking::serve_with_clock(io, &mut h, config, clock)` | `blocking::server(io).with_deadline_clock(clock).build().serve(&mut h)` |
| `blocking::serve_with_io_timeouts(io, &mut h, config, clock)` | `blocking::server(io).with_deadline_clock(clock).with_output_timeouts().build().serve(&mut h)` |
| `blocking::serve_wakeable(io, &mut h, config)` | `blocking::server(io).without_deadlines().with_wakeable_reads().build().serve(&mut h)` |
| `blocking::serve_wakeable_with_clock(io, &mut h, config, clock)` | `blocking::server(io).with_deadline_clock(clock).with_wakeable_reads().build().serve(&mut h)` |
| `WakeableRead::read_or_wake_timeout` (defaulted) | `WakeableReadTimeout::read_or_wake_timeout` (required) |
| `blocking::Client::new(io, config)` | `blocking::client(io).with_config(config).without_deadlines().build()` |
| `blocking::Client::with_clock(io, config, clock)` | `blocking::client(io).with_config(config).with_deadline_clock(clock).build()` |
| `blocking::Client::with_io_timeouts(io, config, clock)` | `blocking::client(io).with_deadline_clock(clock).with_output_timeouts().build()` |
| `link::reliable_with_timer(raw, timer)` | `link::reliable(raw).with_retransmission_timer(timer).build()` |
| `link::reliable(raw)` (std) | `link::reliable(raw).build()` |
| generated `<Service>Server(app)`, `<Service>Server::new(app)` | `<Service>Dispatch::new(app)` or `<Service>Dispatch::new(&mut app)` |
| `server.0` | `get_ref()`, `get_mut()`, `into_inner()` |
| tuple of handlers `(AServer(a), BServer(b))` | `Registry::builder().with_service(ADispatch::new(a)).with_service(BDispatch::new(b)).build()?` |
| one object behind several generated servers | `MultiDispatch::builder(app).with_service(AMethods).with_service(BMethods).build()?` |
| generated `METHODS: &[&str]` | `METHODS: &[MethodDescriptor]` (`.path`, `.kind`, `.method`) |
| `Handler::{method_kind, is_unknown_method}`, `call -> Option<Result<..>>` | `Handler::route(path) -> Option<Route>`, `call -> Result<..>` with `ctx.route` |
| `CallContext::new(path, id, ..)` | `CallContext::new(path, route, id, ..)` |
| `Compression::send(codec)`, `Compression::min_size(n)` | `with_send(codec)`, `with_min_size(n)` |
| `Generator::new()` and `&mut` setters | `Generator::micropb()` or `Generator::prost()` with consuming `with_*` setters |
| `Generator::compile_protos_with_prost(protos, config)` | `Generator::prost().compile_protos(protos, config)` |
| `Generator::compile_fdset_file_with_prost(fdset, config)` | `Generator::prost().compile_fdset_file(fdset, config)` |
| `Generator::prost_service_generator()` | `Generator::prost().service_generator()` |
| `ProstServiceGenerator::{runtime_path, server, client, blocking_client, service_module}` | the `with_*` equivalents |
| `i2c_controller(i2c, delay).at(a).bind(&mut state)` | `i2c_controller(i2c, delay).with_address(a).with_storage(&mut state).build()` |
| `i2c_controller(i2c, delay).bind(&mut state)` | `i2c_controller(i2c, delay).with_storage(&mut state).build()` |
| `.poll_interval(d)` | `.with_poll_interval(d)` |
| `i2c_target(driver).bind(&mut state)` | `i2c_target(driver).with_storage(&mut state).build()` |
| `BindError`, `PeerBuilder` | `BuildError`; `ControllerBuilder` with `with_address` |
| `Port::reliable_with_timer(timer)` | `protolink::link::reliable(port).with_retransmission_timer(timer).build()` |
| `Port::split()` | `Port::into_split()` |
