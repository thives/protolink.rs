# Duplex transport progress: implementation plan

Status: in progress. Implement the milestones below in order, with regression tests landing alongside
each implementation. Breaking concrete driver/builder types is acceptable. Checked items have source
and regression tests in the tree; unchecked items have not landed.

## Implementation status

Three facts are tracked separately: whether the implementation is present, whether the review
findings are accepted, and which regressions from the plan are still missing.

Implementation present (source and tests in the working tree, uncommitted):

- Milestone 1 (output admission, sticky failures, lane attribution, driver wiring) in
  `protolink-http2`, `protolink-grpc` and the `protolink` drivers. The default cap is
  `DEFAULT_MAX_PENDING_OUTPUT_BYTES` (256 KiB), chosen from the measurements in
  `protolink-grpc`'s `tests_sizing.rs`.
- Milestones 0, 2, 3 and 4: modes and factories (`Serial`, `Duplex`, `CoordinatedReliable`),
  retained RX/TX and reliable workers, the coordinator pass shared by servers and routers, and the
  owner-pump client with handoff.
- Milestone 5 (transport integration), each item with its tests:
  - 5.1 I2C: the existing `Port::into_split()` halves go to `client_split` and `server_split`, with
    one runner per bus (`tests/link.rs`: `split_port_halves_carry_rpc_in_both_role_assignments`,
    `withheld_credit_holds_writes_while_inbound_traffic_flows`,
    `reading_restores_receive_space_and_wakes_an_idle_controller`). No new I2C API.
  - 5.2 UART: `PumpHandle::into_split()` returns `PumpReader` and `PumpWriter`. Nothing is reset or
    copied, so the accepted count moves to the writer
    (`split_after_write_preserves_accepted_flush_target`,
    `split_reader_progresses_while_hal_flush_is_pending`, `runner_drop_wakes_split_reader_and_writer`,
    and the unit test `flush_target_wraps_with_the_accepted_count`). `UartBuilder::build` is unchanged.
  - 5.3 COBS: `link::cobs_split` returns `CobsReader` and `CobsWriter` with fresh state. The RX and TX
    logic moved into shared `RxState` and `TxState`, which `CobsFramed` also uses
    (`cobs_split_writer_resumes_without_replay`, `cobs_split_rx_progresses_during_partial_encoded_write`,
    `cobs_split_reader_yields_during_noise_and_keeps_the_following_frame`,
    `cobs_split_halves_exchange_frames_both_ways`).
  - 5.4 Coordinated reliable: the factories `client_reliable`, `server_reliable` and
    `serve_router_reliable` were already in place. The reliable transfer tests cover them.
  - 5.5 Sustained transfers: a 64 KiB transfer each way over the reliable stack, over the DMA UART mock
    (`reliable_uart_duplex_exceeds_engine_receive_storage`, which also checks that no DMA transfer was
    restarted) and over the I2C bus (`reliable_i2c_duplex_exceeds_engine_receive_storage`).
  - 5.6 Generated RPCs over 32, 64 and 128-byte pipes (`examples/embedded-device/tests/e2e.rs`):
    `small_pipe_matrix_raw_duplex`, `generated_streaming_over_split_cobs_small_pipe` and
    `small_pipe_matrix_coordinated_reliable`. Each exchange has unary calls, a client-streaming batch
    and a lockstep bidi call, with the HTTP/2 credit and retained-response budget set explicitly. Each
    case first runs a control: a peer that does not read holds a 4 KiB request back, which shows that
    intentional receive backpressure remains. The server's end must be a clean hang-up.
  - 5.7 Documentation: `docs/API.md` has a transport-mode section with a compile-checked snippet for
    each assembly, and the restrictions (serial mode, blocking factories, error types, no background
    client progress). `UART.md`, `LINK.md`, `STREAMING_GAP.md` and `TODO.md` item 1 are updated. The
    TODO item is marked done only for the duplex, split COBS and coordinated reliable endpoints.

Review acceptance: the eight findings in
[TRANSPORT_BACKPRESSURE_FIXES.md](TRANSPORT_BACKPRESSURE_FIXES.md) are fixed, each with the regression
it names, and the R6 gate passed (see the review repair gate). Acceptance covers those findings only.

Named regressions of the plan, all present and passing:

- `gateway_rx_progresses_during_its_pending_flush`, `backend_rx_progresses_during_its_pending_flush`,
  `local_handler_runs_while_gateway_tx_is_pending` (`protolink/tests/router_transport.rs`).
- `rx_reset_cancels_once_during_tx` (`protolink-grpc`, `tests_server.rs`).
- `simultaneous_rx_error_and_tx_completion_are_accounted` (`protolink-http2`, `output_regressions.rs`).
- `shutdown_flushes_output_appended_by_rx` (`protolink-grpc`, `tests_server.rs`).
- `ready_input_obeys_work_budget` (`protolink/tests/duplex.rs`).
- `flush_reply_is_reaped_before_next_write` (`protolink/src/coordinator.rs`).
- `consuming_waiter_response_wakes_owner_for_credit`, `owner_result_stays_buffered_until_safe_return`,
  `safe_handoff_gives_contender_a_turn`, `cancelled_registration_is_removed_during_duplex_tx`
  (`protolink/src/asynch.rs`).

The plan's "backend overdue isolation" variant is covered by the existing
`a_downstream_output_deadline_retires_only_that_downstream` (`router_transport.rs`), and
`unchanged_tick_does_not_wake_waiters` already existed.

Mutation checks (a deliberate defect that the test must catch, then restored): the split accepted
count reset, COBS re-encoding on every write, removing the I2C receive-space wake, removing the
owner's wake on result consumption, issuing a write while an operation is in flight, removing the
ready-pass budget, removing the contender's wake when the transport is returned, and skipping the
removal of a cancelled waiter's registration. The other new regressions (the router, reset, shutdown,
simultaneous-accounting and owner-result tests, and the matrix) were checked only by their assertions.

Deviations, recorded here so they are not forgotten:

- FIN payload counts as stream data in the UART relay (the fixes doc asked to exclude it). The ARQ
  receiver delivers FIN payload to its reader.
- The matrix's bidi call is lockstep, so it stays inside the unsplit `Call` API. Serial mode still
  deadlocks on such traffic over small pipes, which the docs state.
- `Mode::worker` names the crate-private `Port` and `Shared` types. They sit in `#[doc(hidden)]`
  modules, because the public trait must mention them and the plan asks for no private types in
  public signatures. `Mode` is sealed, so users cannot implement or misuse it.

Still open: the plan's named regressions are written, but the remaining un-mutated ones have no
evidence that they fail on the defect they name. The `just stress` filter repeats only the existing
`concurrent_` tests, not the new regressions. Hardware validation is not part of this work.

## Review repair gate

Passed on the working tree for R0–R6 of [TRANSPORT_BACKPRESSURE_FIXES.md](TRANSPORT_BACKPRESSURE_FIXES.md).
Results:

- `cargo test -p protolink --no-default-features --features async --lib`: 52 passed.
- `cargo test -p protolink --no-default-features --features async,std --lib`: 55 passed.
- `cargo test -p protolink-http2 --lib`: 75 passed.
- `cargo test -p protolink-grpc --all-features --lib --test client`: 262 and 24 passed.
- `cargo test -p protolink --all-features --lib --test duplex --test modes --test reliable --test client_state --test router_transport --test router`: all passed.
- `cargo test -p embedded-device-example --features uart-linux --test uart_linux -- --test-threads=1`:
  15 passed, the 11 UART tests and 4 inspector tests. Two runs, same result.
- `just test`: 736 nextest tests and 39 doctests passed.
- `just clippy`, `just fmt`, `just features`, `just embedded`, `just docs`: exit 0, no warnings.
- `just miri`: all five targets passed.
- `just stress`: 200 of 200 iterations passed. This repeats only the existing `concurrent_` filter,
  which does not include the R-package regressions.

All 16 regressions named in the fixes doc exist and pass. The API change to `protolink::client`
(`Client<Serial<IO>>`) required updating the example in `docs/API.md`.

The follow-up review (F1–F5 in the fixes doc) is implemented in the working tree, with a before/after
record of its regressions and the coverage gaps that remain open. See the validation record in
[TRANSPORT_BACKPRESSURE_FIXES.md](TRANSPORT_BACKPRESSURE_FIXES.md).

## Outcome and limits

On a supported async transport, input must reach the gRPC protocol while that **same connection's**
write or flush remains pending. Cover ordinary clients/servers and router gateway/downstream lanes.
Do not require spawned tasks, increase buffers to hide the problem, or relax deadline/cancellation rules.

This does not remove intentional HTTP/2 or application backpressure. Finite receive windows and
retained-message limits still apply. `Call::send` and `Call::message` both borrow the call mutably;
a split-call API, unlimited send-before-receive, background client runner, connection discovery,
blocking-driver concurrency and heterogeneous router transport collections are **not** in scope.
Blocking I/O needs OS readiness support or independently executing threads, not just split handles.

## Decisions

### 1. Explicit serial and duplex modes

Use small built-in `Serial<IO>`, `Duplex<R, W>` and `CoordinatedReliable<S, L>` transport modes behind
the existing builder vocabulary. The third mode owns one `ReliableLink<S, L>`, not independent halves.
Do not depend on overlapping blanket trait implementations or add a general transport plugin system.

| Assembly | Planned behavior |
|---|---|
| `protolink::{server, client}(io)` | Existing serialized fallback; do not infer duplex capability from `Read + Write`. |
| `protolink::{server_split, client_split}(reader, writer)` | New independently polled directions, with the existing configuration/deadline-policy methods. |
| `Server::serve_router(router, downstreams)` | Keep the serialized downstream path, callable with either gateway mode. |
| `Server::serve_router_split(router, downstreams)` | New path accepting `&mut [Duplex<R, W>]`, independent of gateway mode. |
| `protolink::{server_reliable, client_reliable}(link)` | New path selecting one coordinated ARQ engine; ordinary `server(link)` remains serialized. |
| `Server::serve_router_reliable(router, downstreams)` | New path accepting homogeneous coordinated reliable backends, independent of gateway mode. |
| `protolink::tokio::{server, client}(io)` | Split with `tokio::io::split`, adapt each half, and use the duplex driver by default. |
| Tokio split factories / `tokio::duplex(io)` helper | Accept native halves or split/adapt one stream for router downstream assembly. |

Names above are the API target, not existing APIs. In milestone 0, compile-check concrete return types
and extraction before freezing them. Keep one public client/server implementation parameterized by
mode; share connection bookkeeping, not separate copies of the protocol driver.

Require matching reader/writer errors, including the existing router gateway/backend error constraint.
Do not add blanket `Send`, `'static` or `Unpin` requirements to embedded halves. Preserve serial
`Client::into_io() -> IO`; split clients extract `(R, W)` through `into_parts()`, without promising
automatic reunification. Reliable-mode clients extract the original link through `into_link()`.
Generated service/client traits and the wire format stay unchanged.

Router transports correspond to **flattened backend connections**, not routes: registration order,
then each pool's member order. Validate against `Router::backend_count()`. Retain homogeneous slices;
use the same `Duplex` pair type rather than introducing another backend descriptor abstraction.
Only native-duplex or coordinated-reliable lanes gain the new progress guarantee. Reliable backends
are not represented as `Duplex<R, W>`: their mode needs its own worker selection. Preserve the existing
gateway/backend error compatibility constraint initially; do not advertise raw-TCP-to-reliable routing
without an error adapter or a separately approved relaxation of that constraint.

Keep `tokio::compat` as the unsplit compatibility helper; the new duplex helper adapts halves itself.
Enable `tokio/io-util` in the production feature definition. Test without dev-dependency feature
unification. Update concrete client type aliases and router assembly examples when Tokio defaults change.

### 2. One coordinator; independently retained RX and TX

Keep a single owner of connection protocol state. Reuse the router's worker/port/lane approach from
`protolink/src/driver.rs` and `asynch.rs`, separating transport scheduling from handlers/client results.

- RX retains one pending read with its own `READ_CHUNK` buffer and at most one unread completion.
- TX retains one immutable copied chunk, or one flush future. Never restart it on RX/handler activity.
- RX and TX are independently polled; neither direction monopolizes the transport while pending.
- No lock or borrow of protocol state spans an await. Native split halves retain their futures;
  they are not repeatedly constructed and dropped to simulate polling.
- Service at most one RX completion and one TX completion per connection per coordinator pass; rotate
  router connection order and bound handler/protocol work. If a ready-work budget is exhausted,
  schedule another poll; idle pending operations must not cause self-wake loops.
- Latch output obligations before ticking. Expire due calls and retire overdue output before accepting
  new progress; reap known TX completions before processing RX and deciding whether further output
  is safe. Recheck time when synchronous protocol/handler work may have advanced it.

A duplex read is not cancelled just because TX starts. Client ownership handoff/operation cancellation
can still drop RX, so reads must remain cancel-safe at those boundaries. Non-cancel-safe DMA stays
behind the adaptor pumps. Arbitrary single-owner HALs do not acquire duplex capability through a mutex
held across an async write.

### 3. Stable output prefix; conservative flush barrier

Source inspection establishes these existing contracts:

- `protolink-http2::Connection` appends serialized frames to output. `recv`, reset, timeout, shutdown
  and fatal protocol handling do not retract the existing serialized prefix. Only explicit
  `consume_output`/`take_output` remove it. Reallocation invalidates references, not prefix contents.
- `protolink-grpc::lifecycle::Lifecycle::output_flushed()` retires draining calls only when the
  **current entire output queue is empty**. It has no flush generation or per-batch token.

Use these contracts rather than introducing an output-cursor redesign:

1. Copy output for TX, keep the prefix in the core until completion, and consume exactly the accepted
   count once. A short write leaves the suffix at the head. Never use `take_output` in a live driver:
   the server/router versions acknowledge delivery as part of transferring output.
2. Issue flush only after preceding writes are reaped and the serialized queue is empty. While it
   is pending, or completed but unreaped, RX may append output but no later TX may start.
3. Reap a successful flush and call `output_flushed` **before** issuing the next write. If RX appended
   output, acknowledgment is conservatively deferred until a later queue-empty successful flush.
   Earlier draining calls may retain deadlines in the meantime; retain this existing contract rather
   than claiming per-batch delivery. Precise flush watermarks are a separate feature.
4. Keep `unflushed` state and the earliest delivery deadline across partial writes, operation
   transitions and client ownership handoffs, not merely in the current future. Cancellation or RX
   completing the initiating call cannot disarm output already in flight.

Add contract tests for prefix stability and flush A / append B / complete A before building on them.

### 4. Bound the real egress backlog, not just staging

Independent RX exposes an additional risk: repeated PING/SETTINGS input can append replies to the
HTTP/2 output `Vec` while TX is stalled. A 512-byte staging chunk does **not** bound that backlog.

Add a finite, configurable `Config::max_pending_output_bytes` in `protolink-http2`, covering serialized
frame bytes and connection-preface output. Enforce it before allocation/append, not after an arbitrarily
large `recv` or handler burst. Use existing gRPC `http2` configuration to set it per connection;
do not add a separate router-wide budget or a family of new builder methods.

Exhaustion must latch a distinct terminal output-limit error, stop further serialization, and retire
that connection. Do not drop a frame and then continue the connection, recursively allocate GOAWAY
beyond the cap, or pause RX indefinitely waiting for TX (which recreates the original deadlock).
Map interrupted client/relay calls to `UNAVAILABLE` with an actionable overload reason; expose the
server connection failure distinctly from a peer protocol violation. Preserve router lane isolation.

Milestone 1 must document the finite default and behavior for limits smaller than initial handshake
output. Test valid maximum-message/header configurations against that default: frame overhead and
legitimate larger output must be accounted for or explicitly configurable. Existing per-call queued
payload, header-decoder scratch and receive-retention bounds remain separate; do not claim this one
limit bounds all memory in the process.

### 5. Client ownership, results and notifications

The current `State`/`IoGuard`/`acquire` scheme in `asynch.rs` is the starting point. Keep one elected
I/O owner, but have it pump both directions during `send`, receive, unary and flush operations.
Keep cancellation-aware waiter registrations and notify on **state changes**, not only guard release.

- Non-owners check their completion predicate before acquiring I/O. They can consume a ready response
  while another operation retains TX. After RX, relevant timer/readiness changes or terminal failure,
  wake registered waiters outside `Shared::with`. Start with wake-all, not a new per-call subscription
  system. Do not notify repeatedly when a tick/poll changes nothing.
- Non-owner work that queues requests, cancels calls, releases receive credit or advances an earlier
  deadline must wake the owner. Protocol changes caused by consuming a response count too.
- After TX settles, check completion/handoff before beginning another output-draining cycle. Check
  ownership back in and yield once to registered contenders rather than immediately reacquiring in
  the same poll. This guarantees an opportunity, not strict FIFO scheduling.
- Preserve unflushed/deadline bookkeeping when ownership moves. A pending RX may be cancelled at
  this safe handoff boundary; an indeterminate TX may not be transferred or silently dropped.

| Event | Required behavior |
|---|---|
| Non-owner response becomes ready during blocked TX | Notify and let that waiter return without acquiring transport ownership. |
| Owner result becomes ready during blocked TX | Keep pumping; defer its consuming completion callback until TX is safe and the existing operation completion rule is satisfied. |
| Owner yields with no TX operation live | Cancel-safe RX may be dropped; check in ownership/bookkeeping and give contenders a poll opportunity. |
| Owner future is dropped during potentially partial TX | Permanently retire and wake waiters, as today. |
| Read/protocol error or output-limit failure during TX | Retire immediately; do not wait indefinitely for the pending output. |
| Output deadline arrives | Expire due calls first, preserve buffered responses, then retire the connection. |

Completion callbacks can be destructive (`try_next`, `take_response`). Do not consume the owner's
result and then await output: cancellation could discard that result. Leave it in the core until a
safe return. Without deadlines, that owner's ready result can remain delayed behind stalled TX;
prompt return, preservation of the connection and nontransferable owner-local TX cannot all be
promised. Do not retire a healthy connection merely because a response arrived early.

Preserve existing `send`/`close_send` status-reporting semantics; this work must not silently turn
successful enqueue/progress into a delivery acknowledgment or change where final call status appears.
No polled client operation means no background progress. Do not introduce a runner to hide these limits.

## Concrete implementation checklist

Each numbered milestone is a reviewable work package, not a request to create commits automatically.
Test names below are new regressions unless they name an existing test. Code signatures are design
targets to validate in milestone 0.

### 0. Lock down assembly and the two scheduling mechanisms

**Files:** `protolink/src/driver.rs`, `protolink/src/asynch.rs`, `protolink/src/link.rs`,
`protolink/tests/factories.rs`, `examples/embedded-smoke/src/lib.rs`.

- [x] **0.1 — Prototype the public mode shapes and extraction.** Use private fields and the following
  signature targets; keep the current configuration macros and deadline-policy transitions:

  ```text
  server(io)                         -> ServerBuilder<Serial<IO>>
  client(io)                         -> ClientBuilder<Serial<IO>>
  server_split(reader, writer)       -> ServerBuilder<Duplex<R, W>>
  client_split(reader, writer)       -> ClientBuilder<Duplex<R, W>>
  server_reliable(link)              -> ServerBuilder<CoordinatedReliable<S, L>>
  client_reliable(link)              -> ClientBuilder<CoordinatedReliable<S, L>>
  Duplex::new(reader, writer)        -> Duplex<R, W>
  CoordinatedReliable::new(link)     -> CoordinatedReliable<S, L>
  Client<Serial<IO>, T>::into_io()   -> IO
  Client<Duplex<R, W>, T>::into_parts() -> (R, W)
  Client<CoordinatedReliable<S, L>, T>::into_link() -> ReliableLink<S, L>
  ```

  Here `R: Read`, `W: Write<Error = R::Error>`, `S: Read + Write`, `L: link::Timer`;
  the builder's RPC deadline timer remains a separate type. Use mode-specific forwarding impls and
  private worker construction rather than leaking internal port types or unimplementable private
  bounds through the public API. Compile-test before changing all existing generic bounds.
- [x] **0.2 — Compile every advertised router mode.** Prototype `serve_router`, `serve_router_split`
  and `serve_router_reliable` on each supported gateway mode. Use a multi-member pool and assert
  flattened backend ordering. Retain matching error constraints; do not mistake a reliable engine
  for two `Read`/`Write` halves. Keep heterogeneous/mixed backend slices out of scope.
- [x] **0.3 — Prove the reliable worker with the locked dependency.** Add crate-private
  `ReliableLink::{poll_read_bytes,poll_write_bytes,poll_flush_bytes}`, delegating to the existing
  engine poll methods, preserving empty-buffer behavior. Existing async methods should use those
  methods too. Add a prototype `driver::run_reliable` that polls RX/TX sequentially with the same
  `Context`, one RX buffer and one stable TX request. Do not build public shared-engine halves.
- [ ] **0.4 — Prove client notification without owner release.** In the scripted client harness,
  use distinct wakers for owner A and waiter B. Make A's write/flush pending, inject B's response,
  and demonstrate B can be woken/completed while A still owns TX. Check that A's ready result is
  not consumed and then held across an await. Include an idle-poll no-self-wake assertion.

**Exit evidence:** factory/extraction compilation, same-coordinator reliable wakeups, and the client
ownership prototype work without unsafe code or hidden spawning. Source inspection indicates the
locked ARQ engine supports interleaved polls, but require tests for lower-I/O and timer wakeups before
freezing the reliable API. Fold the prototype into production machinery rather than retaining a second
scheduler. If a dependency change is necessary, record it explicitly before proceeding.

### 1. Make output admission and terminal failure observable

**Files:** `protolink-http2/src/lib.rs`, `protolink-http2/src/regressions.rs`,
`protolink-grpc/src/{lifecycle,server,client,router,relay,local,inbound}.rs`,
`protolink/src/{error,asynch,blocking}.rs` and their existing tests.

- [x] **1.1 — Add the cap and one sticky error contract.** Add `Config::max_pending_output_bytes`
  and `http2::Error::OutputLimitExceeded { limit, pending, attempted }`. Reuse the connection's
  terminal failure storage and add `Connection::failure() -> Option<&Error>`. Draining the old output
  must not clear this failure. Add a distinct `protolink::Error::OutputLimitExceeded` mapping;
  overload is not a peer protocol violation.
- [x] **1.2 — Guard all serialized appends.** Centralize overflow-safe size/admission checks used by
  `write_frame` and the client-preface append. Reject a whole frame before resizing the output;
  leave the existing prefix unchanged. Stop all subsequent serialization, including reset/credit/
  GOAWAY cleanup. Ensure `fail()` cannot overwrite overflow with a protocol error when its own
  GOAWAY cannot fit. Stop receive/continuation/stream-flush loops immediately on sticky failure.
- [x] **1.3 — Guard pre-serialization work.** In `flush_stream`, check DATA-frame admission before
  copying its payload. Establish a checked outbound-header encoding budget before creating a full
  HPACK block; inspect the encoder's size bounds rather than assuming the inbound decoded-header
  limit is also an outbound limit. Count continuation-frame overhead. Keep scratch-memory claims
  separate from the serialized queue cap.
- [x] **1.4 — Handle construction deterministically.** Preflight the entire initial preface/SETTINGS/
  initial-WINDOW_UPDATE batch. Keep construction infallible and I/O-free, but produce an immediately
  failed connection with empty output if the batch cannot fit; never emit a partial handshake or
  silently clamp the configured cap. Test zero, required-minus-one, exact and plus-one limits.
- [x] **1.5 — Select and record a finite default.** Measure default maximum-size unary/streaming,
  concurrent-call, header/trailer, compression and control-frame high-water marks. Verify existing
  64 KiB relay cases and fragmented large-header tests. Choose the named default from that evidence,
  document which larger configurations must raise it through `.with_http2(...)`, and add sizing
  regressions. Do not invent a numeric default without this evidence.
- [x] **1.6 — Forward failure through gRPC.** Add failure accessors/reconciliation to `Lifecycle`,
  `Server`, `Client` and router gateway state. Fallible operations return a newly latched failure;
  void/value-returning APIs latch and reconcile it. Check after `recv` event processing, `poll`,
  `tick`, `finish`, `shutdown`, cancellation and credit release in `take_request`, `try_next` and
  `take_response`. Do not return a saved successful `recv` result after later event/handler work
  overflowed. Cancel still-active services once and do not create new draining obligations after
  terminal failure.
- [x] **1.7 — Preserve lane attribution.** In relay/router pumping, inspect gateway and backend
  failures separately after starting/sending/finishing/consuming a relayed call. A gateway overflow
  caused by `backend_recv` is still a gateway failure. A locally generated backend overflow retires
  only that backend. Avoid recursive `backend_failed` → relay-pump → `backend_failed` reconciliation.
- [x] **1.8 — Wire retirement into current drivers before changing scheduling.** Check failure after
  construction and every core mutation, before more I/O or treating closure as success. Do not take
  the protocol-error best-effort-GOAWAY path for overflow. Fail interrupted client/relay calls with
  an overload-specific `UNAVAILABLE`, preserving buffered responses. Update blocking drivers for
  this shared-core error contract only; blocking concurrency remains out of scope.

Add `serialized_prefix_survives_recv_reset_and_goaway`, `output_limit_preface_boundaries`,
`output_limit_ping_and_settings_floods`, `output_limit_is_sticky_and_goaway_cannot_exceed_it`,
`flush_a_append_b_complete_a_defers_retirement`, `output_limit_local_handler_poll_cancels_once`,
`output_limit_result_consumption_retires_client`, and `output_limit_backend_failure_is_isolated`.
Test output exhaustion through local mutation and relay work, not only incoming bytes.

**Exit evidence:** prefix/flush contracts are tested; no serialized append can exceed the cap;
initialization, void APIs, client result consumption and router lane attribution expose the failure.
A capped or truncated connection is never reused. This work package can be tested with existing
serialized drivers and must pass before enabling continued RX during blocked TX.

### 2. Build mode-specific workers and shared delivery accounting

**Files:** `protolink/src/driver.rs`, `protolink/src/asynch.rs`, `protolink/src/lib.rs`,
`protolink/src/tokio.rs`, `protolink/Cargo.toml`, `protolink/tests/factories.rs`.

- [x] **2.1 — Separate worker mechanisms, not protocol drivers.** Rename the existing worker path
  `run_serial`. Add retained-future `run_rx(reader, port)` and `run_tx(writer, port)` loops and a
  `run_duplex` wrapper that pins/polls both. RX cannot start another read while an unread completion
  is held; TX cannot accept another request while its completion is unreaped. Reuse `Port`,
  `Request` and `Reply`, with separate RX/TX storage. Implement the reliable worker proven in 0.3
  against the same completion contract, using engine-owned poll state instead of async half futures.
- [ ] **2.2 — Persist delivery obligations.** Extract the current `Lane::op`/`unflushed` bookkeeping
  into a small reusable state, conceptually `Delivery { unflushed, deadline }` plus the current
  operation descriptor. Latch/minimize deadlines before core tick/cancellation, consume only a
  completed accepted prefix, and carry the obligation across short writes and write-to-flush
  transitions. Clear it only at the chosen successful flush barrier or terminal retirement.
- [ ] **2.3 — Make TX states explicit.** Use `Idle`, `WriteIssued(len)` and `FlushIssued` bookkeeping.
  `Idle` can issue a copied chunk or a flush; issued states only process their matching reply.
  An issued flush blocks further TX even if RX appends output. Reap flush before any next write.
  On retirement, drop worker futures and prevent all later polling. Do not store self-referential
  async futures inside the transport object; keep them pinned in the worker's async scope.
- [ ] **2.4 — Add bounded cooperative scheduling.** Each lane reports at most one RX and one TX
  completion per pass. Use a small private pass budget, initially 16, for immediately ready work;
  after exhaustion yield once and schedule continuation. Test that another ready task and other
  lanes receive a turn. An entirely idle connection must not repeatedly self-wake.
- [ ] **2.5 — Expose the factories proven in milestone 0.** Keep builder fields/configuration macros;
  change transport-specific bounds/forwarders instead of duplicating service dispatch or call
  state. Re-export the modes/factories from `lib.rs`. Assert construction performs no I/O and
  unspecified deadline policy still prevents `.build()`. Count fixed staging including port copies
  and worker buffers; do not claim `READ_CHUNK` is the entire per-connection footprint.
- [ ] **2.6 — Add Tokio glue.** Enable `tokio/io-util`; use `tokio::io::split` plus `FromTokio::new`
  on each half. Add native-half factories and `tokio::duplex` for downstream pairs. Keep `compat`
  unsplit. Migrate type aliases and extraction tests; do not introduce `Unpin` on embedded halves
  just because the Tokio wrapper requires it.

Add `duplex_reads_while_write_is_pending`, `duplex_reads_while_flush_is_pending`,
`rx_wake_does_not_restart_tx`, `flush_reply_is_reaped_before_next_write`,
`ready_input_obeys_work_budget`, `idle_workers_do_not_self_wake`, and factory compile tests for
conditional `Send` and non-`Unpin` embedded halves. Reliable tests must exercise both poll orders
and actual lower-I/O/timer wakeups, not repeated manual polling alone.

**Exit evidence:** inject RX only after TX has become pending and observe completion before releasing
TX. Native pending futures survive unrelated activity; ARQ retains its one engine; output accounting,
staging and portability checks hold independently of gRPC handler/client integration.

### 3. Apply one pass algorithm to router and server

**Files:** `protolink/src/asynch.rs` (`Lane`, `drive_router`, `serve_connection`, failure cleanup),
`protolink/tests/router_transport.rs`, `protolink/tests/router.rs` and server unit tests.

- [x] **3.1 — Construct the right worker for every lane.** Add the split/reliable downstream entry
  points from milestone 0. Validate flattened backend count before starting I/O. Support serial,
  native-duplex and coordinated-reliable gateway modes without changing local service ownership.
- [x] **3.2 — Replace the write-first server loop.** Use the same transport-completion/accounting
  primitives as the router, preserving `Serving`/`Routing` cancellation guards. Keep handlers and
  protocol mutation in the coordinator, never in the RX/TX workers.
- [x] **3.3 — Implement each pass in this order:**
  1. Capture current/earlier output obligations, sample the clock, tick the core and retire overdue
     or already-failed connections before accepting progress.
  2. Reap at most one TX completion per lane and account for accepted bytes/flush success.
  3. Process at most one RX completion per lane, applying data or terminal failure; recheck core
     failures and the clock after callbacks.
  4. Drive bounded local/relay work with fresh deadline checks; reconcile gateway/backend failures
     again because this work can itself serialize output.
  5. If live and TX is idle, issue one stable write chunk or the required flush. Check the graceful
     shutdown predicate using core output, issued TX and unflushed delivery obligations.
  6. Poll each lane's worker and the earliest timer with the coordinator waker. Process ready
     completions on another bounded pass; otherwise return `Pending`. Rotate backend start order.
- [x] **3.4 — Preserve terminal wire handling.** When RX/protocol failure abandons issued TX, retire
  without further output. Only permit existing best-effort GOAWAY draining when all accepted
  writes are accounted for and no indeterminate operation remains; output-limit failure never
  enters this drain path. Stalled backend TX must not retire the gateway or healthy backends.

Add `gateway_rx_progresses_during_its_pending_write`, `backend_rx_progresses_during_its_pending_flush`,
`local_handler_runs_while_gateway_tx_is_pending`, `rx_reset_cancels_once_during_tx`,
`simultaneous_rx_error_and_tx_completion_are_accounted`, and `shutdown_flushes_output_appended_by_rx`.
Retain existing deadline/protocol-error/partial-GOAWAY regressions.

**Exit evidence:** same-connection RX and protocol/handler progress, not just cross-connection
isolation. Deadline outcomes and shutdown behavior remain deterministic under stalled write/flush.

### 4. Replace async client write-only ownership intervals

**Files:** `protolink/src/asynch.rs` (`State`, `IoGuard`, `Registration`, `acquire`, `drive`, `flush`,
`write_all`, `output`, `read_once`, `Call` operations), `protolink/tests/client_state.rs`.

- [x] **4.1 — Separate ownership from notification.** Store the selected mode in the existing
  checkout slot and keep persistent delivery bookkeeping in client state across leases. Replace
  the read-only meaning of `holder`/`want_io` with `owner_waker`/`handoff_requested`. Keep waiter IDs
  and the `Registration` drop guard; do not introduce a second protocol owner or per-call task.
- [x] **4.2 — Centralize state-change wakeups.** After applied input, a due timer, send-readiness
  progress or first terminal transition, take registered waiter wakers under the short lock and
  wake them outside it. Re-registration uses the existing waiter mechanism. No-change polls/ticks
  do not wake everyone. Request/cancel/half-close and successful result consumption must wake the
  owner when they create output, release credit or advance a deadline, including a non-owner
  returning a result without checking out I/O.
- [x] **4.3 — Replace `write_all`/`read_once` sequencing with one owned pump.** After `acquire`, pin
  that mode's worker and process the same bounded passes as 3.3, without server handler work.
  Both `drive(step)` and internal `flush` use it. A `send` waiting to write/flush must still process
  input into the core, including responses, resets, SETTINGS and WINDOW_UPDATE. Preserve the
  existing public send/status semantics rather than adding a delivery guarantee.
- [x] **4.4 — Apply result callbacks only at safe points.** Non-owners still test completion before
  acquiring I/O. The owner defers destructive `try_next`/`take_response` callbacks while TX is
  issued; when safe and the operation completion rule is satisfied, consume and return without
  another await. A response becoming ready must not cause a live TX future to be dropped.
- [x] **4.5 — Handoff without starvation or lost obligations.** At a safe boundary, check ownership
  back in, retain `Delivery`, wake contenders and perform an executor-independent one-shot yield
  before retrying acquisition. Dropping cancel-safe RX is allowed at this boundary. If no contender
  exists, do not churn ownership or self-wake just to hand it back to the same operation.
- [x] **4.6 — Preserve cancellation/terminal paths.** Mark issued output in the guard until completion
  is reaped. Dropping the owner during issued output retires and wakes all waiters; a read/protocol/
  cap error retires without waiting for TX. Preserve stale-unary cleanup, dropped-call reset,
  buffered-messages-before-terminal-status and earliest output deadlines across other tasks' ticks.

Add `response_waiter_completes_while_owner_tx_is_pending`,
`consuming_waiter_response_wakes_owner_for_credit`, `owner_result_stays_buffered_until_safe_return`,
`safe_handoff_gives_contender_a_turn`, `dropping_owner_during_tx_retires_all_calls`,
`cancelled_registration_is_removed_during_duplex_tx`, and `unchanged_tick_does_not_wake_waiters`.
Run variants for pending write and pending flush, with distinct task wakers and with same-task joins.

**Exit evidence:** B returns its buffered response while A's TX remains pending; cancelling A cannot
discard B's data or replay wire bytes. Without deadlines A itself may still wait for its TX to settle,
as explicitly documented above. Test both `RefCell` and `std` mutex configurations and Tokio tasks.

### 5. Finish each supplied transport and prove finite end-to-end transfers

- [x] **5.1 — Reuse I2C splits, do not redesign the bus.** In adaptor API/link tests, pass existing
  `Port::into_split()` halves to split factories and keep the one controller/target runner polled.
  Withhold TX credit while permitting inbound traffic; reading must restore whole-mailbox receive
  space and wake the idle controller. No new I2C split API or concurrent bus owner is needed.
- [x] **5.2 — Split the UART application handle.** In `protolink-adaptors/src/pump.rs`, add
  `PumpHandle::into_split() -> (PumpReader, PumpWriter)`. RX owns the receive ring/control access;
  TX owns the transmit ring, sole wrapping `accepted` count and flush requests. Compose/delegate
  the unsplit handle to the same logic. Splitting after writes transfers the counter unchanged;
  do not reset rings/control or clone writers. Keep `flush_req`/`flushed` shared with `TxPump`,
  and leave persistent hardware operations in `RxPump::run`/`TxPump::run` untouched. Update UART
  API docs/tests without changing `UartBuilder::build`'s binding/runner ownership.
- [x] **5.3 — Split COBS directional state.** In `protolink/src/link.rs`, introduce
  `cobs_split(reader, writer) -> (CobsReader<R>, CobsWriter<W>)`. Factor RX decode and TX encode
  state so existing `CobsFramed` and new halves share algorithms. Preserve RX oversize discard and
  fairness budgets, whole-frame reads, TX partial-write offsets/accepted count and flush ordering.
  This constructs fresh state; do not silently rebuild an active wrapper and discard buffered
  frames. Retain the same-data retry requirement after a cancelled partial COBS write.
- [x] **5.4 — Wire coordinated reliable links.** Use the worker/mode proven in 0.3 through client,
  server and reliable router factories. Preserve one sequence/ACK/retransmission/timer engine and
  use the coordinator waker for every poll. Existing cancellation-safe unsplit `Port`/`PumpHandle`
  can remain beneath reliable ARQ; reliable support does not depend on public raw halves or on
  splitting `CobsTransport`. Keep its lower framed contract separate from upper byte polling.
- [x] **5.5 — Add sustained transport regressions.** Test split-reader progress while UART TX is
  full or HAL flush is pending, splitting after accepted writes, counter wrap, runner-drop wakeups
  and buffered RX before errors. Test COBS RX while encoded TX is partial, noise fairness and no
  reencoding/replay. For reliable I2C/UART, transfer a finite 64 KiB in each direction with independent
  draining, beyond the locked engine's 4,016-byte in-order buffer and additional receive window.
  Check lower DMA transfers are not restarted, and reliable flush still covers ACK obligations.
- [x] **5.6 — Prove generated RPCs, not only byte workers.** In
  `examples/embedded-device/tests/e2e.rs`, add raw/COBS/coordinated-reliable finite exchanges with
  a parameterized small-pipe matrix (initially 32, 64 and 128 bytes). Exchange enough data to exceed
  transport storage, while explicitly configuring HTTP/2 credit and retained-message capacity.
  Assert payload/order/final success; an output deadline or overload retirement is not a pass.
  Use a handler/test shape compatible with the existing unsplit `Call` API, not unbounded
  send-before-consume. Keep a negative control proving intentional receive backpressure remains.
- [x] **5.7 — Migrate examples and claims.** Update concrete type aliases, factory doctests, embedded
  smoke assembly and API/routing/link/UART/adaptor/streaming docs. Show raw split, split COBS and
  coordinated reliable assembly separately. Document remaining serial/blocking/error-compatibility
  restrictions and no-background-client-progress behavior. Only mark the TODO complete for the
  transport combinations whose regressions and compile fixtures pass.

Add `split_after_write_preserves_accepted_flush_target`,
`split_reader_progresses_while_hal_flush_is_pending`, `runner_drop_wakes_split_reader_and_writer`,
`cobs_split_rx_progresses_during_partial_encoded_write`, `cobs_split_writer_resumes_without_replay`,
`reliable_i2c_duplex_exceeds_engine_receive_storage`, `reliable_uart_duplex_exceeds_engine_receive_storage`
and `generated_streaming_over_split_cobs_small_pipe` at their corresponding existing test locations.

**Exit evidence:** real finite RPCs and sustained link traffic complete over small buffers; no hidden
runner replaces missing scheduling, and supported raw, COBS and reliable assembly is compile-tested.

### Suggested validation sequence

Run these from the workspace root **after implementing the relevant work package**, with bounded
terminal runtimes. They are planned validation, not checks that passed during documentation work.

```sh
# Output admission and core contracts (milestone 1).
cargo test -p protolink-http2 --lib
cargo test -p protolink-grpc --all-features --lib
cargo test -p protolink --all-features --lib asynch::tests
cargo test -p protolink --no-default-features --features blocking --test blocking_driver_regressions
cargo test -p protolink --all-features --test blocking_driver_regressions

# Worker and client ownership under both shared-state implementations (milestones 2–4).
cargo test -p protolink --no-default-features --features async --lib driver::tests
cargo test -p protolink --no-default-features --features async,std --lib driver::tests
cargo test -p protolink --no-default-features --features async --lib asynch::tests
cargo test -p protolink --no-default-features --features async,std --lib asynch::tests
cargo test -p protolink --all-features --test factories --test router --test router_transport --test client_state

# Link/adaptor/generated-RPC integration (milestone 5).
cargo test -p protolink --all-features --test link
cargo test -p protolink-adaptors --all-features --test controller --test target --test link --test pump --test uart
cargo test -p embedded-device-example --all-features --test e2e

# Production feature isolation; requires the existing cargo-hack development tool.
cargo hack check --no-dev-deps --no-default-features --features tokio -p protolink
```

Then run the full repository gates listed below. Preserve existing regressions rather than replacing
them with the new names; map failures to a milestone and report any unavailable tooling explicitly.

## Regression matrix and final gate

| Concern | Required evidence |
|---|---|
| Same-lane duplex progress | RX arrives after TX becomes pending; assert protocol/handler/call progress before manually releasing write **and** flush. |
| Wire correctness | Short writes plus RX-generated output; no restart/replay/reordering; prefix stable across reset/tick/shutdown/malformed input. |
| Flush ordering | Flush A, RX appends B, complete A; B is not acknowledged; another flush completes retirement. |
| Bounded memory | Sustained PING/SETTINGS replies hit the egress cap deterministically; withheld application consumption still withholds receive credit. |
| Client concurrency | Distinct wakers; owner/non-owner results; release-capacity wakeup; safe handoff; cancelled registrations; no idle self-wake loop. |
| Terminal races | RX/TX failures together, EOF/reset, owner cancellation, earliest deadline retained across handoff and write-to-flush transitions. |
| Router behavior | Multi-member pool transport ordering; gateway/backend duplex; one overflow/failure does not retire other lanes. |
| Link integration | Finite generated RPC exchange exceeding pipe capacity with enough HTTP/2 credit/retention; sustained reliable exchange beyond engine receive storage. |
| Runtime portability | `no_std + alloc`, `forbid(unsafe_code)`, conditional `Send`, production-only Tokio features and persistent DMA operations. |

Use deterministic polling assertions first, then bounded async timeouts as hang protection. A timeout
or connection retirement alone is not evidence of successful duplex transfer. Commit regressions with
their fix rather than leaving indefinitely hanging or permanently ignored tests in the default suite.

Primary locations: `protolink/src/driver.rs`, `protolink/src/asynch.rs`, `protolink/tests/client_state.rs`,
`protolink/tests/router_transport.rs`, `protolink/tests/router.rs`, `protolink/tests/link.rs`,
`protolink-grpc/src/tests_lifecycle.rs`, `protolink-http2/src/regressions.rs`, adaptor pump/UART/link
tests, and `examples/embedded-device/tests/e2e.rs`.

After targeted tests, run the repository gates from `justfile`: `just test`, `just clippy`, `just fmt`,
`just features`, `just embedded`, `just miri`, `just stress` and `just docs`. Report unavailable tools
or targets rather than claiming a pass. Hardware validation is optional follow-up, not a prerequisite
for proving the scheduling fix.

Definition of done: every advertised duplex assembly delivers and processes readable input during
pending TX, with bounded egress, correct notifications, no byte replay, intact cancellation/deadline
rules and no hidden task requirement. All remaining serial/blocking limitations stay documented.
