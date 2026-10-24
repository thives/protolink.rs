# Routing: local services and forwarded calls

A gateway serves one caller connection. Each call is either served by a local service, or relayed
to a downstream connection and answered with the downstream's response. This page records the
design: the contracts, the layers that implement them, and what is deliberately left out.

## Contracts

- **One routing table.** `Router::builder()` takes local services (`with_service`, any
  `Dispatcher`) and forwarded routes (`with_route`, or `with_pool` for several interchangeable
  downstreams). Both are merged into one exact-path table, so registration order does not matter.
  A path served twice fails `build()` with `RouteConflict`. A path nothing serves is answered
  `UNIMPLEMENTED`.
- **Descriptors, not prefixes.** Routes are the generated `METHODS` tables. They carry each
  method's shape, which drives the protocol (request and response counts), and a service prefix
  could not carry that.
- **Route once, keep the target.** A call's path is resolved when its headers arrive. A local
  call stays with its service; a forwarded call stays with the downstream it was given. Nothing
  migrates mid-call.
- **Pools are for interchangeable instances.** Members of a pool must serve the same protobuf
  service with the same behaviour. Distinct devices that expose the same service must not share a
  pool unless spreading their calls is intended. Selecting one logical device would need an
  application-level key, which the router does not have.

## Layers

| Layer | Module | Role |
|---|---|---|
| Lifecycle | `grpc::lifecycle::Lifecycle<K>` | Sans-IO protocol for one connection. Reports `Started`, `Expired` and `Cancelled` events. Request messages are taken on demand, and responses are sent explicitly. |
| Local engine | `local::LocalCalls` (crate-private) | Adapts a `Handler` to the lifecycle: pulls request messages into handler methods and responses out, respecting backpressure. `Server` is built on it. |
| Relay | `grpc::relay::Relay` | Sans-IO relay of one incoming call to one downstream `Client` call. |
| Router | `grpc::Router` | Owns one lifecycle, the local services, the relays and the downstream clients. Routes events. |
| Driver | `Server::serve_router` (`protolink`) | Drives the gateway transport and each downstream transport, with one future. |

`Server` (the classic handler API) and `Router` share the lifecycle and the local engine. The
behaviour of `Server` is unchanged: the existing test suite runs against the refactored core.

## Lifecycle events

`Lifecycle::poll_event` reports three things:

- `Started { id, key, path, metadata, deadline }`: the headers passed validation and the resolver
  returned a key. Messages are still to be taken. Routing happens here, once.
- `Expired { id }`: the deadline passed. The protocol does not send the status, because the caller
  must attach its trailing metadata. The caller finishes the call with `finish`. The router ignores
  this event for forwarded calls, whose relays see the deadline themselves.
- `Cancelled { id }`: the call is gone (reset by the peer, its stream failed, the connection
  closed, or its output was blocked past its deadline). Nothing more is sent for it.

A call the caller finishes produces no event. Unknown calls are ignored everywhere.

Events are applied after each read. A reset that arrives in the same read as a call's request
messages therefore cancels the call before those messages reach its handler. Earlier versions
delivered the messages first; the handler is still told about the cancellation.

## Backpressure and buffering

- A request message is taken from the gateway only when the downstream call has no bytes waiting
  for its flow-control window. So at most one message of a forwarded call waits in the downstream's
  outbound buffer. The gateway's flow-control credit for a message is returned only when the
  message is taken.
- A response message is taken from the downstream only when the gateway call can send it.
- The downstream `Client` bounds retained responses (`max_buffered_response_bytes`) and calls
  (`max_retained_calls`) for the whole connection. The gateway bounds its own buffering the same
  way (`ServerConfig`).
- `with_pool(.., max_active_calls)` caps the forwarded calls of one route; a call past the cap is
  failed `RESOURCE_EXHAUSTED`.

## Relay rules

`Relay::pump` moves whatever it can between the two calls, and reports whether anything moved.

- Response headers are forwarded as soon as the downstream has them, before any message.
- Messages are forwarded in order. The request half-close follows the last message it covers.
- The downstream's final status and trailers are forwarded as they are. A trailers-only response
  stays trailers-only.
- Once the downstream call has finished, no more request messages are taken from the gateway. A
  request that could not be sent (for example one larger than the downstream's limit) therefore
  cannot replace the backend's outcome, even when the gateway's output is backpressured.
- A successful downstream call that has no response message (unary or client streaming) ends the
  gateway call with `INTERNAL`, and its trailers still go out, once.
- Response messages are limited by the gateway's `max_message_size`, by their uncompressed size,
  as local responses are.
- The downstream call gets the remaining deadline budget, computed from the gateway's deadline and
  clock, so the absolute deadline is the same on both hops.
- Each hop negotiates its own compression. Messages are relayed as decoded protobuf bytes, which
  the relay never decodes.
- Shape rules hold on the relayed path as they do locally: a unary or server-streaming method takes
  one request, and a unary or client-streaming response has one message on success.
- Cancellation, deadline expiry and downstream failure end the gateway call once, and cancel the
  downstream call if it is still running.

## Failure isolation

- A downstream whose transport fails (read, write or protocol error) is marked failed. Its calls
  end with the failure's status. Calls routed to it afterwards go to another member of their pool,
  or end `UNAVAILABLE` when none is usable.
- A failed downstream does not affect the gateway connection or the other routes.
- A failure of the gateway connection ends `serve_router`. Dropping the future, or ending it,
  cancels every call, and forwarded calls cancel their downstream calls. Those cancellations are
  written only if the downstream is still up.

## Deadlines

The gateway and each downstream client use the same clock, ticked by the driver before each pass.
A forwarded call's downstream timeout is the gateway deadline minus the gateway's `now`, so both
expire at the same instant. Each `Client` also enforces its own timeout, and a downstream that
expires its call reports `DEADLINE_EXCEEDED` through the relay.

## Drivers

`Server::serve_router` drives the gateway and the downstreams in one future. A coordinator owns the
router, the handlers and the clock. Each transport has a worker, a future that borrows the transport
and performs one write, flush or read at a time; the coordinator asks for an operation and collects
its result through a small shared port. Nothing is spawned, and the workers are polled by the
coordinator, so the driver needs no task and works without `std`.

The guarantees below are the ones the tests check (`protolink/tests/router_transport.rs` and the
driver's unit tests):

- **Transports are isolated.** A write or flush that stays pending on one transport holds up
  neither the other transports, nor the local services, nor the coordinator's deadlines. Calls to
  other backends and to the gateway's own services keep progressing.
- **Same-transport operations are serialized.** A transport runs one operation at a time, so its
  reads wait while its own write is pending. A read is dropped only when that transport has output
  to write or flush, and then the transport's `read` must be cancel-safe, as it already had to be.
  Writes are never restarted.
- **Staging is bounded.** A write copies at most one chunk of the core's output (512 bytes). The
  core keeps the bytes until the write reports how many it accepted, and only those are consumed.
  The driver therefore adds a fixed amount of memory per connection, and the core's existing output
  buffering still provides the backpressure.
- **Flushes are acknowledged only on success.** Output appended while a flush is pending is not
  acknowledged with it, and finished calls keep their deadlines until their output is flushed.
- **Output deadlines are per connection.** The coordinator wakes for the earliest deadline of any
  call. Output still blocked past the deadline of its own connection retires that connection: for
  the gateway, its calls' deadlines (serving ends with `OutputDeadline`, as for `serve`); for a
  downstream, its own calls' deadlines (only that downstream is failed, and its calls end
  `UNAVAILABLE`). A downstream's deadline never retires the gateway. A call that has already
  expired keeps no deadline for its status, which is delivered on a best-effort basis, as on
  `serve`.
- **Deadlines come before progress.** Each pass of the coordinator reads the clock once and ticks
  the router with that reading, before any handler or worker is polled. The wait ends as soon as a
  deadline is due, before the router or a worker is polled, so a handler that becomes ready after
  its call's deadline is never polled for a response. Its call ends with `DEADLINE_EXCEEDED`, and
  the gateway keeps serving the others.
- **Output in flight keeps its deadline.** The deadline of an output operation is kept when it is
  issued, even if the tick ends the call that caused it. Output still past that deadline retires its
  connection, before its worker is polled again. Calls that are due end first, with their own
  status, so retiring a downstream's output does not replace their `DEADLINE_EXCEEDED`.
- **A protocol error writes a GOAWAY only when that is safe.** On the gateway, a protocol error
  writes its GOAWAY, best effort, only when no write or flush of the gateway is in flight. A queued,
  pending or partly performed operation leaves its bytes unknown, so the driver abandons it and writes
  nothing more. Bytes that completed writes accepted are not sent again, so the GOAWAY follows the
  output that was not yet accepted.
- **Failures are contained.** A downstream whose read, write, flush or protocol fails is retired,
  and the gateway continues. A gateway failure ends serving. Dropping the future cancels every call
  without writing to the peers.
- **Shutdown completes on delivery.** After `Router::shutdown`, serving returns once the gateway
  connection is closed, its output is written, and the last flush is acknowledged. It does not wait
  for the caller to hang up, nor for a stalled downstream.
- **`Send` when the transports are.** The future can be spawned on tokio. Without `std`, the shared
  state is a `RefCell`, so the future is not `Send`.

## Driver boundaries

- A transport whose output is abandoned at a deadline is retired and never reused: part of its
  output may already have been written, so its byte stream is no longer known.
- The gateway's output deadline covers the gateway's calls, including the ones it relays. A gateway
  write blocked past one of them ends the serving, as for `serve`.

## What is not built yet

These are the remaining steps of the plan, in the order they should land:

1. **Shared pools with handles and runners.** The router owns its downstream clients, so a pool
   serves one gateway connection. The plan's handle-plus-runner form would let several gateway
   connections share one downstream set: the runner owns the downstream transports and `Client`
   state, and cheap cloneable handles are given to routers. Routing calls then need a
   connection-unique token (for example the pair of connection id and stream id), since stream ids
   repeat across connections. Constructing a pool must not spawn tasks.
2. **Discovery and reconnection.** Downstreams are preconnected. A reconnect factory, discovery and
   topology changes are excluded for now, as are automatic retries.
3. **Blocking proxy.** `serve_router` is async-only. Local blocking services are unaffected.
4. **A standalone gateway binary.** `docs/API.md` shows the assembly; a runnable example is not
   provided yet.

## Tests

- `protolink-grpc/src/tests_lifecycle.rs`: the lifecycle's response rules, driven directly: a
  message at the limit is sent and one byte more is refused (compressible messages included),
  headers then messages then a default finish give valid trailers, and trailers-only responses keep
  their metadata.
- `protolink-grpc/src/tests_relay.rs`: the relay with one downstream, sans-IO: all four shapes, empty
  request streams, headers before the first message, a failing status after messages, trailers-only
  responses, binary metadata, the remaining deadline budget, expiry and cancellation in both
  directions, downstream failure, bounded buffering when the downstream is busy, and shape
  violations on the request side. A hand-written backend (`RawBackend`) covers the response side:
  a held request does not replace the backend's status, neither under flow control nor while the
  gateway's responses are backpressured, responses arrive in order before the status, a successful
  call without its response keeps its trailers, a unary call with two responses fails after the
  first, and forwarded responses are limited by the gateway.
- `protolink-grpc/src/tests_router.rs`: the router in memory: local and forwarded calls on one
  connection, unknown methods, duplicate paths in either registration order, status and metadata
  pass-through, interleaved streams, cancellation, downstream failure, the deadline budget, and the
  race of a held request with a backend status through the router.
- `protolink/src/driver.rs`: the workers, with scripted transports: a stalled write does not hold up
  another transport, a pending write survives unrelated wakeups without restarting, a pending read
  is dropped for a request without losing input, output is copied per request and bounded, input
  gets a turn between writes, flushes wait for stalled transports, and a tokio worker is `Send`.
- `protolink/src/asynch.rs` (the coordinator, with a manual clock and scripted transport): a
  handler made ready at or after its call's deadline is not polled and the call ends
  `DEADLINE_EXCEEDED`; one ready before its deadline answers; the gateway keeps serving after a call
  expires; overdue output retires the gateway without polling its worker; a protocol error during a
  pending write or flush writes nothing more; after a short write the rest and the GOAWAY are written,
  without repeating bytes; and a GOAWAY is written when no output is in flight.
- `protolink/tests/router.rs` (tokio): the same behaviour through `serve_router` over real
  transports, with round-robin pools, dead members, call limits, concurrent gateway connections,
  and a stuck call that does not block the others on its connection.
- `protolink/tests/router_transport.rs` (tokio): transports the test can stall, cut into partial
  writes, or hold on flush. Isolation across backends and the local services, deadlines enforced
  while a downstream is stuck (with paused time), a downstream retired by its own output deadline
  while the others serve, the gateway retired by a blocked output of a finished call, gateway
  hang-up while a downstream blocks, byte order after a stall, a
  gateway task spawned with `tokio::spawn`, and graceful shutdown (closed and drained, GOAWAY split
  across writes, GOAWAY with a pending flush, and active calls finishing).
