1. Concurrent streaming calls on the async and blocking clients. Today a streaming `Call` borrows the client mutably, so only one call can be active (see "Known limitations" in `STREAMING_GAP.md`). Planned design:
   - one shared state object that is never held across an await; the transport is checked out for each I/O step and returned by a guard
   - one driver loop for unary and streaming operations
   - a reader that yields to writers, which requires a cancel-safe `read` when more than one call is active
   - `Sync` (so `Send` calls) only with the `std` feature; `RefCell`-based without it
   - `&self` receivers on `StreamingTransport`, `BlockingStreamingTransport` and generated streaming methods; generated unary methods stay `&mut self`
   - `inner()` replaced by `with_inner(|client| ...)`
2. Make blocked writes compatible with ongoing reads. Drivers don't read while a transport write is blocked, so bidi traffic can stall on transports with small buffers. Interleaving sends and receives, or using a transport with more buffering, is the current workaround ("Known limitations" in `STREAMING_GAP.md`). A split read/write transport API would fix this.
3. Improve unknown-method responses. Unknown paths are treated as unary, so a streaming client may not receive UNIMPLEMENTED until it half-closes.
4. Improve progress for blocking handlers. A blocking server advances Pending streams only when a read returns or times out. A more direct wake/idle mechanism could make this less dependent on transport timeouts.
5. Check out if it's worth to replace the WakerSet and the "Sync only with std" restriction by introducing embassy-sync as an optional dependency.

Add:
* compression
* deadlines
* custom metadata
* reflection?
* health checks
* TLS
