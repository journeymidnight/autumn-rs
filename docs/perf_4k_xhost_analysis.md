# Cross-host 4K perf — two cliffs explained

Two questions surfaced from the cross-host bench matrix (2026-06-07):

1. **Why does TCP 4K drop so hard going from loopback to cross-host?**
   (4K read: 1,370,164 → 15,575 ops/s = **88× cliff**)
2. **Why does UCX flatten / hard-fail at high client thread counts?**
   (t=128: 11.5 K · t=256: 2.6 K · t=1024: 0 ops/s)

These are not bugs — they are physical / architectural facts of the
test shape. This doc walks the math.

## 1. TCP 4K — loopback vs cross-host

The collapse is asymmetric: **reads drop 88× but writes only 4×**.
The asymmetry is the whole answer.

### The "loopback was unrealistically fast" axis

Per-op time on the SERVER side is roughly:

|                       | 4K loopback | 4K cross-host |
|-----------------------|-------------|---------------|
| Server work (memtable lookup or partition_loop) | ~0.7 µs | ~0.7 µs |
| Wire RTT (one round trip, kernel TCP stack)     | ~0      | ~65 µs |
| Effective per-op time | ≈ 0.7 µs    | ≈ 66 µs |

`1 / 0.7 µs = 1.43 M ops/s` (matches the 1.37 M loopback read);
`1 / 66 µs = 15.2 K ops/s` (matches the 15.6 K cross-host read).

The 88× ratio is **loopback / cross-host RTT cost**, full stop. The
loopback number is dominated by syscall + scheduler + Bytes::clone,
not by anything autumn does on the wire. Cross-host adds 1 TCP RTT
per op (server response goes through eth0's kernel TCP stack), which
at 4 KiB completely dwarfs the per-op work.

### Why writes only drop 4×

Writes have a per-op floor that loopback ALSO pays — the fsync
coalescer + 3-replica fanout join — so the loopback advantage is
much smaller:

|                       | 4K loopback | 4K cross-host |
|-----------------------|-------------|---------------|
| Per-op work + fsync floor (3-replica) | ~25 µs | ~25 µs |
| Wire RTT (request + response, with fanout) | ~0 | ~75 µs |
| Effective per-op time | ≈ 25 µs | ≈ 100 µs |

`1/25 µs = 40 K ops/s` (matches 39.5 K loopback write);
`1/100 µs = 10 K ops/s` (matches 9.7 K cross-host write).

Loopback writes were "real" disk work, just without the wire. Adding
the wire pushes per-op time 4× higher because the wire is comparable
to the disk floor — not 88× higher.

### Test-shape implication

The published p=16 × shards=16 × t=1024 d=8 **162 K ops/s** ceiling
in `perf_check.sh`'s scaling table is **TCP loopback, not
cross-host**. Cross-host of any shape is at most
`min(threads_inflight, partitions × pipeline_depth) / per_op_wire_time`,
and at 4 KiB per_op_wire_time ≈ 1 RTT, so the cross-host 4 K
ceiling on this hardware is roughly:

```
client_inflight_limit / wire_RTT ≈ 256 / 65 µs ≈ 3.9 K ops/s/thread
```

with optimum at moderate t (16-64) before connection-management
overhead climbs faster than concurrency falls. This is what we saw:
TCP cross-host 4K peaked at t=16 (9.7 K) and degraded above that.

## 2. UCX — why high t cliffs

The bench shape that gave 160 K loopback was **t=1024 d=8**. UCX
hard-fails at that t (0 ops/s). The reason is structural to how
perf-check spawns work, not a UCX-itself limit.

### How perf-check uses threads

`crates/server/src/bin/autumn_client.rs::cmd_perf_check`
spawns ONE OS thread per `--threads`. Each thread:

```rust
std::thread::spawn(move || {
    compio::runtime::RuntimeBuilder::new().build().unwrap().block_on(async {
        let client = autumn_client::ClusterClient::connect(&mgr).await?;
        // ... per-thread fan_out(depth) loop ...
    })
})
```

So at `t=N`:

* N independent OS threads
* N independent compio runtimes (N io_urings)
* N independent `ClusterClient`s → N independent `ConnPool`s
* On UCX: N independent **ucp_workers**

The thread-local UCX worker is the key (`crates/transport/src/ucx/
worker.rs::with_thread_ctx`):

```rust
let (worker, efd) = unsafe { create_worker(ctx) };
*borrow = Some(UcxThreadCtx { worker, efd });
```

`ucp_worker_create` per OS thread, no sharing. So `t=N` means
N `ucp_worker`s in one process.

### What that costs UCX

Each `ucp_worker`:
* Has its own CQ (completion queue) reserved on the NIC
* Has its own progress thread / io_uring binding
* Creates its own QPs (queue pairs) on first `ucp_ep_create` to a
  given remote address
* In autumn's bench shape, talks to: manager (1 EP) + 16 partitions
  (1 EP each) + each partition's 3-replica EN ring (3 EPs each) =
  roughly 16 × 4 + 1 = **65 EPs per worker**, per replica fanout
  depth

So at `t=1024`:

* 1024 ucp_workers
* 1024 × 65 EPs ≈ **66 K endpoints**
* Each EP needs at least 1 QP for `rc_mlx5` → 66 K QPs

ConnectX-7 cards typically have ~256 K QPs theoretical headroom but
the **active concurrent set** that the NIC can drive efficiently
(without rcache thrash, without WQE backpressure) is in the
low-thousands range. 66 K QPs in active rotation blows past that;
`ucp_ep_create` either fails outright (the 0 ops/s case at t=1024)
or succeeds but devolves to single-digit ops/s per worker.

The memory note from the `perf-check` skill records this same shape
on loopback UCX:
> --threads 64 → 64 EPs/p → write 14k · p99 18ms ✗ cliff
> --threads 256 → 256 EPs/p → write ~0 · ✗ hard fail

— same NIC, same UCX, same autumn EP architecture; identical cliff.

### What about TCP at high t?

TCP cliffs too (t=1024: 4.6K, t=256: 5.2K), but for a different
reason: 1024 threads × 65 conns = 66 K **kernel sockets**. Each
socket eats kernel memory + an FD. Linux's
`/proc/sys/net/ipv4/ip_local_port_range` is 10000-65535 = 55 K
ephemeral ports → tight against the ceiling. The bench client also
spends most of CPU in scheduler + lock contention across the 1024
runtimes rather than in actual I/O.

TCP doesn't go to 0 (the kernel handles backpressure gracefully via
`EAGAIN` / queue depths); UCX does because libibverbs returns
hard error codes on QP exhaustion.

### Why UCX low-t is fast though

At t=16:
* 16 ucp_workers × 65 EPs ≈ 1040 endpoints
* Comfortably under NIC active-set headroom
* Each worker has ~16-64 in-flight ops via depth=8 × ~8 partitions
  routed to → workers are busy without being oversubscribed
* RDMA-vs-TCP wins on per-op latency: 4 K read `MSG_GET_ZC` is
  1-sided RDMA on the read path (skip remote CPU), so UCX 4K cross-
  host read at 854 K ops/s leaves TCP cross-host's 15 K in the dust

The sweet spot is `t ≤ 64` (per the memory note above). The 4K
write ceiling on this hardware cross-host is ~15-16 K ops/s under
UCX; the 8 M read ceiling is one NIC's line rate (~10.76 GB/s on
1 rail, ~17.83 GB/s on 2-rail).

### What would lift the UCX-at-high-t cliff

Not a UCX-version change — an autumn architecture change. Either:

1. **Share workers across threads.** Move the per-thread
   `with_thread_ctx` to a shared pool (e.g. one worker per N
   threads, with appropriate locking). UCX supports
   `UCS_THREAD_MODE_MULTI` workers; autumn currently uses
   `UCS_THREAD_MODE_SINGLE` (`worker.rs:265`).
2. **Per-conn worker reuse.** Have one worker per partition or per
   conn group, not per OS thread. Requires plumbing through
   `ConnPool` so the worker handle is reachable across the runtime.
3. **Multi-process clients.** Sharded clients each running ~64
   threads × M processes → P × 64 = total concurrency without
   exceeding per-process worker count.

Option 3 is what production kvcache adapters already do
(per-tenant processes), so the cliff doesn't bite real workloads —
it only bites `perf-check --threads 1024` which is a synthetic
"max threads on one client" shape.

## Summary

* **TCP 4K cross-host cliff** = adding a wire RTT on top of a
  workload whose loopback time was 0.7 µs. The 88× drop is wire-
  bound. Writes drop only 4× because both paths have a disk floor.
* **UCX high-t cliff** = `ucp_worker_create` per OS thread. At
  t=1024, 1024 workers × ~65 EPs = ~66 K active QPs, well past
  the NIC's efficient set. Hard fails at t≥256.
* The **162 K ops/s** number in `perf_check.sh`'s scaling table is
  loopback TCP only; the cross-host write ceiling at 4K is
  ~15-16 K ops/s under UCX (sweet spot t ≤ 64), capped by per-op
  wire RTT × concurrency.

Neither is a fixable bug at the autumn layer alone — production
workloads either use multi-process clients (no per-process
worker cliff) or accept the wire-RTT-bound cross-host ceiling for
small ops and use UCX for the large-block path where it actually
wins (8 M reads at 10.76-17.83 GB/s).
