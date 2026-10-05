1. Make blocked writes compatible with ongoing reads. Drivers don't read while a transport write is blocked, so bidi traffic can stall on transports with small buffers. Interleaving sends and receives, or using a transport with more buffering, is the current workaround ("Known limitations" in `STREAMING_GAP.md`). A split read/write transport API would fix this.

Add:
* reflection?
* health checks
* TLS
* interceptors
* load balancing
* opentelemetry metrics
* wait-for-ready
