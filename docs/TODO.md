1. Done for the duplex, split COBS and coordinated reliable endpoints: blocked writes no longer stop reads, as the small-pipe and sustained transfer tests show (see the [implementation plan](TRANSPORT_BACKPRESSURE.md)). Serialized and blocking paths keep their documented limits, and the interleaving workaround in `STREAMING_GAP.md` still applies to them.

Add:
* reflection?
* health checks
* TLS (transport layer - not here)
* interceptors
* load balancing
* opentelemetry metrics
* wait-for-ready

Memory footprint optimization is planned in [MEMORY_PLAN.md](MEMORY_PLAN.md). Prioritize bounded
memory and single-owner buffers before expanding the feature set.
