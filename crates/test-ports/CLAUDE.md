# autumn-test-ports

Test-only (a dev-dependency of manager, server, stream, etcd). Hands out ports
for tests that start servers by address, so parallel test processes (nextest
runs every test in its own process) never get the same one.

- Why not `bind(:0)` and release: the port is free only at that instant; another
  process can be handed it and bind it before our server does. Measured: 32
  processes picking and binding in parallel lost 0.11% of binds.
- Ports come from `[2000, ip_local_port_range low)`, which neither `bind(:0)` nor
  an outbound connection hands out, and each is held by `File::try_lock` on
  `/tmp/autumn-test-ports/<port>` for the life of the process (released by the
  kernel at exit, even on a crash). A port is also bind-checked before it is
  handed out, to skip ones a non-test service listens on.
- `port()`/`addr()` hold 8 consecutive ports: a partition server binds its base
  port + 1 per partition, so the hold covers 8 partitions on one PS. Beyond
  that, and the PS's own fallback band when a `base + ord` bind fails
  (base + 1024 + ord, then `:0`), are not held: a test opening more partitions
  on one PS takes its base from `reserve` with the span it needs. `port_with_control()` adds the 8 at +1000 for an
  extent node binary's control listener; `reserve(offsets)` holds exactly
  `base + o` for each offset (extent node shards at every 10, a chaos PS's
  whole span).
- Capacity: 8000 ports / 8 = 1000 picks at once with an ephemeral floor of
  10000; a pick that finds none free panics. A process that dies frees its
  ports, so another may take them then — correct, not a leak.
- Test: `tests/across_processes.rs` (16 processes × 25 picks, no overlap; red
  when the lock is skipped).
