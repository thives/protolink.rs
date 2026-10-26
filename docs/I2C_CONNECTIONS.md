# I2C connections: state contract

This document fixes the connection and stream contract of the I2C adaptors. It
covers the wire identity (see `protolink-i2c::mailbox`), the lifecycle of a
*generation* (one connection between a controller route and a target), the
stream handles, and failure ownership. It names no application or RPC concept:
a generation is a byte stream with an identity, nothing more.

## Identity and isolation

- A connection is `ConnectionId { boot, epoch }`. `boot` is supplied by the
  target application per boot. `epoch` is chosen by the controller per route
  and is never zero for a connection-bearing request.
- The target serves at most one connection. Ordinary data writes that name any
  other identity are ignored, so bytes of an earlier connection cannot reach a
  later one.
- Identifiers are finite and can repeat after wraparound. Equality of the
  identity is isolation between generations, not anti-replay protection for
  control requests. Stronger guarantees need a wire change, specified
  separately.

## Ownership

- **Port** (application side): one reader and one writer half. Each half
  *claims* its ring role at runtime, so at most one port half of a generation
  is attached. Dropping a half is a local event: the other half keeps working,
  and no connection ends. Dropping both leaves the generation intact; buffered
  data stays for the next attachment.
- **Attachment** (`Port::attachment`): a copyable handle to the route. It
  attaches a new port (or half) later, which is how an application continues
  after a reset without rebuilding the bus. At most one `attach` waits on a
  channel at a time; a second one fails with `Busy` until the first is gone. A
  full port reserves both roles in one step: a held half, of either kind,
  refuses it as `Busy` even while the generation is ending, and an ending
  generation with no holder refuses it as `Ending`.
- **Explicit abort** (`Port::abort`): the only local way to end a connection.
  It fails the port with `PortError::Reset`, and the runner retires the
  connection on the wire, best effort and bounded to one attempt.
- **Runner**: owns the hardware and the runner-side ring roles for its
  lifetime. It never depends on ports: it moves bytes whether or not an
  application half is attached.

## Generation states

Controller route (`Idle`, `Discovering`, `Opening`, `Serving`, `Ended`):

| State | Entered when | Reads | Writes | Attach |
|---|---|---|---|---|
| `Idle` | no generation, or previous one finalized | no I/O | none | starts a new generation |
| `Discovering` | a port is attached to an idle route | read until one reply gives the target's boot identifier | none | continues it |
| `Opening(e)` | discovery answered; epoch `e` allocated | read; a reply confirms only if it serves `(boot, e)` | `OPEN(boot, e)`, once per attempt | continues it |
| `Serving(e)` | a reply serves `(boot, e)` | payload is queued | up to the advertised credit | continues it |
| `Ended(cause)` | any terminal cause (below) | no I/O | none | refused until the detach finalizes |

Target (`Unbound`, `Serving`, `Pending`):

| State | Reply | Accepts `OPEN(e)` | Accepts `RETIRE(e)` | Accepts data of `e` | Attach |
|---|---|---|---|---|---|
| `Unbound` | serving 0, epoch 0 | adopts `e`, keeps buffers (first establishment) | ignored | ignored | allowed |
| `Serving(e)` | serving 1, epoch `e` | same `e`: idempotent; other `e'`: pending replacement | pending unbind | yes, within credit | allowed |
| `Pending(next)` | serving 0, epoch `e` | `next` is replaced by `OPEN(e')` | the advertised `e` becomes unbind; other epochs ignored | ignored | refused |

`Pending` is entered when a replacement or retirement arrives while an
application half is attached, or when the generation has ended. Its requests
take effect only when no application half is attached (*finalize*): buffers
are cleared, and the state becomes `Serving(e')` or `Unbound`.

## Causes and precedence

| Cause | `PortError` | Set by |
|---|---|---|
| bus transaction failed or was ambiguous | `Hardware` | either runner |
| peer violated the mailbox format or overflowed credit | `Protocol` | either runner |
| replaced, retired, rebooted or aborted | `Reset` | runner or `Port::abort` |
| runner dropped | `Closed` | runner drop |

A generation is *closing* when it is being replaced, retired or aborted. Closing
is set by `Port::abort`, or by the target runner when a replacement or
retirement waits for an attached half. A generation that already failed with a
cause is never closing: it keeps that cause.

Precedence for an attached port:

- **Read**: if the generation is closing, or its cause is `Reset`, the buffered
  data is discarded and `Reset` is returned. This holds whether or not the
  runner has processed the change. Otherwise buffered bytes are delivered
  before a terminal cause.
- **Write**: a cause, or closing, returns immediately.
- **Flush**: returns the cause whenever bytes were discarded or are unsent. It
  succeeds only when every accepted byte has been transferred.

Ambiguous data transfer (a write whose outcome is unknown, or a read whose
payload may have been consumed) ends the generation with `Hardware`. Its bytes
are discarded, never replayed. A failed `OPEN` is different: `OPEN` is
idempotent for the same identity, so it may be retried within the opening
budget.

## Requests valid in each state

Requests whose identity is not the one the target serves are ignored. Requests
with epoch zero are rejected at decode time.

- `OPEN(boot, e)`: valid in every state. Ignored when `boot` is not the
  target's. Equal to the served `e` is a no-op.
- `DATA(boot, e)`: valid only in `Serving(e)` and only within the advertised
  credit. Otherwise it is ignored (wrong identity) or ends the generation with
  `Protocol` (overflow).
- `RETIRE(boot, e)`: valid for the epoch the target advertises, in `Serving(e)`
  and `Pending`. Other identities are ignored.

Replies carry the target's own boot and the epoch it serves, with flags
`SERVING` (accepts this connection's writes) and `RETIRE` (asks the controller
to retire the named connection).

## Establishment and replacement

- **Discovery**: a route's first reply supplies the target's boot identifier
  and the epoch the target advertises, and nothing else. Its payload and credit
  belong to whatever the target served before, so they are dropped.
- **Epoch allocation**: the route's epoch is the next nonzero epoch after the
  last epoch its target address was given, modulo 65 535, skipping the one the
  target advertises. An address that was never given an epoch starts from the
  state's seed (`with_epoch_seed`) instead. The allocation is recorded before
  the first `OPEN`, and the history lives in the `ControllerState`, so it
  survives rebuilding the same state.
- **Opening**: each `OPEN` of the route's epoch spends budget. Only a reply
  that serves `(boot, e)` confirms the connection, and its payload is taken
  then, never before.
- **First establishment** (`Unbound` to `Serving`) keeps the buffers. Bytes the
  target application wrote before the first `OPEN` are sent to the controller
  once it opens. The attached port stays valid. No runner ends.
- **Repeated `OPEN` of the served identity** is idempotent.
- **Replacement** (`OPEN` of a different epoch) while `Serving(e)`: an
  unattached port finalizes at once, clearing buffers. An attached port is
  reset (`Reset`) and waits in `Pending` until it is dropped. The controller
  keeps re-sending `OPEN` (idempotent) within its budget.
- **Retirement** (controller `RETIRE`, or local `abort`) follows the same
  rules, and finalizes to `Unbound`. A retirement of the advertised connection
  takes the place of a pending replacement.
- **Abort, observed once per generation.** The first time a generation's abort
  is seen, its pending action becomes `Unbind`, whatever it was, so a pending
  replacement is superseded. The channel keeps asking for retirement until the
  generation is finalized, but later settling does not reapply the abort. An
  `OPEN` received after it replaces the `Unbind` as usual. The next generation
  observes its own abort.
- **Abort decision boundary.** The exclusive finalization snapshot decides the
  abort. Finalization starts only once no application half is held, and the
  snapshot is read in that same atomic step. It holds every abort issued before
  the last half was released, and no abort can follow it. An abort that no
  earlier settling observed therefore still retires the generation, and the
  captured replacement is never committed over it. A finalization refused while
  a half stays attached keeps the abort for the next attempt.
- **Fault and pending open.** A fault ends the epoch being served, and that
  epoch is never served again. A pending `OPEN` of that epoch is replaced by
  `Unbind` before the generation is finalized. A pending `OPEN` of any other
  epoch is kept.
- A rebuilt target starts `Unbound`. A controller that had served a connection
  sees a reply that does not serve it and ends that route; it never resumes
  the old epoch on its own. Recovery means opening a new epoch.

## Opening budget and pacing

- **Budget.** A route makes at most `open_attempts` opening operations. A failed
  discovery or opening read spends one unit. Every `OPEN` write spends one unit,
  whether it succeeded or failed. No budgeted operation follows exhaustion. The
  final permitted `OPEN` can still be confirmed by one read, before the route
  gives up with `OpenExhausted`, which ends only that route.
- **Retry deadline.** After a failed read, or an `OPEN` that did not confirm,
  the route may act again only at its retry deadline, `open_retry_interval`
  later. The deadline is absolute, measured on the controller's `Timer`, and
  it starts when the failed transaction completes, not when its turn started.
  Other routes' traffic never moves it, and ordinary wakes never restart it.
- **Abort eligibility.** A route with local abort work is serviced at once,
  whatever its deadline. The abort ends it without a discovery read or an
  `OPEN`, and retires an identified connection once, best effort. This does
  not interrupt a transaction in flight, and it does not bound the retirement;
  the deferred supervision layer owns both.
- **Abort before discovery.** An aborted idle route has no identified
  connection, so aborting it needs no wire retirement. It publishes `Reset` and
  ends at once, with no read, `OPEN` or `RETIRE`, and it finishes once no half is
  attached. It waits for neither time nor the bus. A held port delays only the
  finish, never the `Reset` the attached port sees.
- **Runner.** It services the next route that is due, round-robin. It waits
  only when no route is due, or when a full cycle of due routes made no
  progress. It then sleeps until the earliest of its poll interval, the earliest
  retry deadline, and an application event. A deadline that has already passed
  yields once, so zero intervals never block the executor.

## Route-local and shared failure

- A route ends on its own cause. Other routes of the same controller keep
  their generations and their turns.
- The runner keeps running after route failures. The concrete HAL error is
  reported with the route's failure through `run_observed`, and the application
  learns the port error from the port.
- The controller runner never returns; it has no shared-failure path yet, so a
  bus error always ends one route. The target runner returns only when its
  driver fails, since a target has one route and a failed peripheral is shared.
- A route waiting for its peer is skipped until its own deadline, and healthy
  routes are still polled.

## Target runner: local events

The target keeps its connection state apart from its driver. While the listen
operation of the driver is pending, application events are processed in place:

- The last application half of a stale generation being dropped finishes the
  pending replacement or retirement at once. The listen operation is neither
  dropped nor started again for the event. No bus transaction is needed.
- The runner settles again after a listen completes, before the reply is built,
  so that a reply reflects every local event that happened before it.
- Nothing settles while a reply is in flight, so the payload it carries stays
  queued until the read is known to have completed.
- Canceling the runner disarms the driver before the buffers are released.

## Lifecycle invariants

These hold in every interleaving of the runner, the controller routes and the
application ports. Each one has a regression test.

1. A new controller generation cannot accept discovery payload as its own. A
   reply read before the route's first `OPEN` supplies the target's boot
   identifier and the epoch it advertises, and nothing else.
2. Every attempted opening operation is bounded by the opening budget. A failed
   read and an `OPEN` write each spend one unit, whether or not the write
   succeeded. The final permitted `OPEN` may still be confirmed.
3. One route's retry deadline never pauses a healthy route, and a healthy route
   never moves another route's deadline.
4. `Reset` discards buffered receive bytes, whatever the runner has already
   processed: the abort of a port, a replaced, retired or rebooted connection.
   Hardware and protocol failures still deliver buffered bytes first.
5. Attachments cannot observe a generation until its buffers and the runner's
   state are ready. Finalization is exclusive, and it publishes last.
6. Detaching the last application half of a stale generation finishes it
   without further bus traffic. The runner processes local events while a
   listen operation stays pending.
7. An attachment future either owns the single waiter slot, or fails with
   `Busy`. Its waker is registered only while it owns the slot, and cancelling
   it releases the slot.
8. A generation's abort takes effect once. Its pending action becomes `Unbind`
   on first observation, and settling never reapplies it over a later request.
9. A pending `OPEN` of an epoch that a fault ended is never finalized.
10. A flush succeeds only if its queue is empty and no terminal cause is set
    when that emptiness is observed. A failure that discards the queue fails
    the flush.
11. The exclusive finalization snapshot is the abort decision boundary. An
    abort issued before the last half is released retires the generation, even
    when no earlier settling observed it. An aborted idle route ends without bus
    traffic and without waiting.

### Controller epochs

The history lives in memory, and a fresh state starts with no history. The
application may seed a fresh state with `ControllerState::with_epoch_seed`, to
an epoch the previous controller allocated, which it must persist. The seed is
the baseline of every address with no history, so such an address's first epoch
is the one after the seed, skipping zero. A seed never rewinds an address that
has a history. Epochs advance modulo 65 535 and skip zero, so after the
wraparound an address continues from its own last epoch: the seed is a starting
point, not a lower bound that lasts. An unadvertised pending replacement on the
target can still collide, and no isolation is promised across arbitrary state
loss.

## Test matrix

| Area | Cases |
|---|---|
| Establishment | first `OPEN` keeps pre-open bytes; repeated `OPEN` idempotent; boot mismatch ignored; discovery payload never reaches the port |
| Identity | wrong-identity data ignored; zero epoch rejected; a rebuilt controller continues each address's epoch sequence, however its routes are ordered; a seed wraps past zero, skips the advertised epoch, and leaves used addresses alone |
| Replacement | attached old port gets `Reset` at once; pending until detach; old bytes never reach the new port; the last half's drop finishes it with no bus traffic |
| Retirement | `RETIRE` finalizes to `Unbound`; the advertised connection's retirement wins over a pending replacement; other identities are ignored; a retire flag on a serving reply ends the route before its payload is taken |
| Split ports | dropping a reader keeps the writer working; dropping both keeps the generation; a single half's drop leaves a replacement pending |
| Abort | `abort` fails the port with `Reset` and sends one `RETIRE`; buffered bytes are discarded before and after the runner processes it; an abort supersedes a pending replacement and is observed once per generation, so a later generation observes its own; an abort in the window before finalization retires the generation, not the captured replacement; a refused finalization keeps the abort; an in-flight answer holds finalization until it completes, its payload crosses once, and the runner polled while it is held changes nothing; an aborted idle route finishes on its first poll with no bus traffic, whether its port is held or dropped, and the next attachment transfers |
| Faults | a fault on the served epoch never resurrects it through a pending `OPEN`; a valid pending replacement survives a fault on another epoch |
| Terminal precedence | reads deliver buffered bytes first; flush fails when bytes were discarded, including a discard during its pause; a flush whose queue the runner consumed completes |
| Budget | failed discovery reads and failed or refused `OPEN`s spend the same budget; no operation follows exhaustion; the last permitted `OPEN` is confirmed |
| Pacing | an unanswered open waits for its deadline, counted from its completion, not from the start of its turn; another route's traffic does not shorten it; zero intervals retry at once and leave the clock alone; an abort during a cooldown finishes the route on a held clock |
| Admission | attachment during finalization is `Ending`, never `Busy`; a full port is `Busy` while any half is held, during an ending generation too; concurrent full and half attachments never hold one role twice; a second waiter is `Busy` until the first is gone; a partly failed attach releases its half; runner drop wakes a waiter with `Closed` |
| Routes | a failed route leaves the other route transferring; a reset route re-attaches with a new epoch |
| Recovery | `OPEN` retried within budget, then the route ends with `Reset` (`OpenExhausted`) |
| Send | ports, attachments and both runners stay `Send` |

## Status

Implemented in this change (uncommitted):

- Protocol: patch 0001, with zero-epoch rejection, reply flag validation,
  a length-safe `connection_of`, and exact-byte codec tests.
- Channels: generation-bound, with claimable port halves
  (`Attachment`), abort, and finalization only once no half is attached. One
  atomic word holds the lifecycle and the application roles, so admission is a
  single decision; finalization runs its steps in a fixed order, with a commit
  hook for the runner's state. The hook receives the exclusive finalization
  snapshot, which decides the abort.
- Target: unbound, serving and pending states, first establishment, idempotent
  open, replacement, retirement, and a faulted epoch that is never re-served.
  Local events are processed while the listen operation is pending.
- Controller: discovery, epoch allocation per target address, per-route opening,
  serving and ended stages, identity checks, a single opening budget, route-local
  retry deadlines on the `Timer`, route-local faults with an observer, a
  best-effort retirement on abort, and local ending of an aborted idle route.
- Supervision: the pure deadline policy from patch 0002, without its wrappers.

Deferred, and not part of this contract yet:

- Runner completion when no route is live. The controller never returns.
- A whole-bus failure policy and a backend classification of retryable versus
  permanent driver errors.
- Transaction deadlines: a route waits for its retry deadline, but a transfer
  that never completes is not timed out.
- Controller-initiated `RETIRE` when the target asks for retirement. The route
  ends with `Reset`, and no retirement goes out.
- The supervised wrappers and fault budgets of patch 0002, and a lossy or ARQ
  mode.

## Imports from the candidate patches

Recorded per batch. Patch 0001 is imported in full, with the corrections above.

| Patch hunk | Disposition |
|---|---|
| `protolink-i2c` mailbox v2: header, connection ids, codecs, 24-byte payload | imported; zero epochs and reply flags validated; `connection_of` length-safe |
| `protolink-i2c` controller `write_transaction` rename | imported |
| `protolink-i2c` `I2cTarget` for `&mut T` | imported |
| `protolink-i2c` target docs on completion and cleanup | imported; send-queue and ARQ wording dropped |
| 0002 `channel.rs` `session_gone` and drop-ends-session | omitted: dropping a port never ends a connection |
| 0002 `channel.rs` `Reset` error and reset-discard | adapted as `PortError::Reset` with attachment |
| 0002 `i2c/target.rs` `Superseded` teardown on first connection | omitted; replacement waits for detach |
| 0002 `i2c/controller.rs` sleeping fault backoff and fixed budgets | omitted; configurable open budget only |
| 0002 `i2c/supervision.rs` deadline policy | adapted as `supervision` (no clock dependency, checked constructor) |
| 0002 `i2c/supervised*.rs` and their tests | omitted for now |
| 0002 `Cargo.toml` `[patch.crates-io]` and extracted-package override | omitted, as the plan requires |
