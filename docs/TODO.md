1. Make blocked writes compatible with ongoing reads. Drivers don't read while a transport write is blocked, so bidi traffic can stall on transports with small buffers. Interleaving sends and receives, or using a transport with more buffering, is the current workaround ("Known limitations" in `STREAMING_GAP.md`). A split read/write transport API would fix this.
2. Improve unknown-method responses. Unknown paths are treated as unary, so a streaming client may not receive UNIMPLEMENTED until it half-closes.
3. Improve progress for blocking handlers. A blocking server advances Pending streams only when a read returns or times out. A more direct wake/idle mechanism could make this less dependent on transport timeouts.
4. Check out if it's worth to replace the WakerSet and the "Sync only with std" restriction by introducing embassy-sync as an optional dependency.

Add:
* compression
* deadlines
* custom metadata
* reflection?
* health checks
* TLS
