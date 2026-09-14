# Transport buffer ownership

TCP and UCX share a thread-local size-class pool. Runtime TCP must recycle ordinary
unregistered slabs even when the binary enables the `ucx` feature. Only UCX-runtime
registration fallbacks skip recycling, so registration can be retried later.
Eviction decrements registration counters only for registered slabs. Cross-thread
returns retain the existing home-thread ownership rule.

The integration test `tcp_recycles_buffers_including_in_a_ucx_capable_binary`
explicitly selects TCP and verifies pool hits, buffer reuse, and zero registered
bytes; run it both with and without `--features ucx`.

Current UCX I/O uses UCP Stream. A registered stable slab benefits send registration
reuse and application buffer ownership; it does not imply receiver-side end-to-end
zero-copy. Upstream UCX 1.16/1.18 Stream receive unpacks AM data into the destination.
