# Core data path performance, 2026-09-14

## Scope and implementation

Baseline: `de7ee4a`, rebuilt with UCX support. Changes preserve wire and storage
formats, all-replica durability, authorization and per-item routing fallback.

- Runtime TCP recycles pool slabs even in a UCX-capable binary. Unregistered
  TCP eviction no longer decrements the registered-byte gauge.
- PS bulk receive works with namespace/authz enabled. Verified control is checked
  before receiving the value and again after receive, so revocation/expiry during
  receive cannot admit a stale request. Rejected frames preserve stream alignment.
- PS/EN keep receiving below their existing in-flight cap even with only one
  operation pending. PS no longer awaits an entire round trip inline based on
  the number of frames received in a socket read. Cached GETs that complete on
  first poll bypass the pending queue, and single-buffer replies use write_all.
- Batched direct reads request partition descriptors concurrently (bounded at 32),
  retaining input indices and per-item fallback.
- FUSE/headless filesystem reads select only intersecting extents. Cached legacy
  maps use binary search; striped files derive the requested units directly.
  Persisted stripe geometry and the existing extent-count validation are retained.

Not implemented in this change: append bulk wire redesign, CRC reuse between
replicas, UCX rendezvous/RMA, CLI owned-buffer streaming, random-write layout
changes, per-inode dispatcher barriers.

## Environment

H200-1, container `dongmao-autumn`, `/data/dongmao_dev/autumn-rs`. UCX 1.16.0.
RF=3; one EN on each of `/data03`, `/data05`, `/data08`; four shards per EN.
Test directories were under `dongmao-autumn-perf-20260914` on those disks.
Manager port 29001, EN base 29100, PS base 29301, etcd 27379. Automatic manager
policy is off; no EC/recovery workload was introduced. Normal flush/SST work still
occurs and can affect measurements immediately following writes.

TCP uses loopback. UCX uses `[fdbd:dc62:3:302::14]`,
`UCX_TLS=rc_mlx5,ud_mlx5,tcp,self`, `UCX_NET_DEVICES=mlx5_1:1`. All roles and
clients are on this host. These are **same-host RoCE results, not cross-host
network measurements or a whole-machine throughput ceiling**. The fourth disk
(`/data`) holds the build tree; RF3 data uses the three disks above.

The dated perf directories were removed on 2026-09-14 at the user's request.
Selected raw logs and JSON are archived on H200-1 in
`/data08/autumn-perf-evidence/20260914-compio-baselines-and-logs.tar.gz`.
Committed measurements and validation remain in `perf/core_path_20260914/`.
Synthetic data and obsolete build trees were deleted.

## Method and limitations

The initial perf-check runs used eight threads and depth eight, 12 seconds per
write/read phase. `--partitions 8` did not create partitions: the bench keyspace
had one partition. A later filesystem split changed the total partition count,
but did not stripe the bench keyspace. Initial runs are diagnostic only.

perf-check reads the keys produced by its write phase. A faster writer changes
the number of read keys and their memtable/SST distribution. Its large apparent
read gains were therefore not accepted as isolated code improvements.

The new core_path benchmark uses 256 fixed keys, one client thread, depth eight,
two seconds of per-operation warmup and eight seconds of measurement. Errors or
missing/short values fail the run. Latencies sample one in 16 operations. Repeated
read-only checks pin the client to CPU 40 and measure three six-second runs.
They switch the PS/client version while retaining the modified ENs and the same
data; they isolate PS/client read behavior, not the complete EN change.

Startup was explicitly checked through the PS `partition server serving` marker.
The first partition listener can open while another is still replaying; runs
that failed during partial startup are excluded.

## Results

Pool regression: baseline UCX-capable TCP binaries reported 0 hits in 1,702
8 MiB receive acquisitions. Modified runs reuse buffers at approximately
99.4–99.9% after warmup. This is independently verified by an integration test
that checks reuse and hit counters (not merely allocator address reuse).

Fixed-key write observations (MiB/s; eight-second samples, not confidence
intervals):

| Transport / size | Baseline | Modified observations | Interpretation |
|---|---:|---:|---|
| TCP / 4 KiB | 39.1 | 50.0–64.2 | Write improvement observed; batch/fsync variability |
| TCP / 8 MiB | 287.4 | 502.7–529.2 | About 1.75–1.84x in these runs |
| UCX / 4 KiB | 25.4 | 23.4–45.2 | Too variable for a stable gain claim |
| UCX / 8 MiB | 301.1 | 312.3–412.4 | Improvement varies; do not promise a fixed percentage |

Stable read-only medians (same data, client CPU 40, modified ENs held constant):

| Transport / size | Baseline | Modified | Baseline p99 | Modified p99 |
|---|---:|---:|---:|---:|
| TCP / 4 KiB | 113,786 ops/s | 113,472 ops/s | 82.3 us | 83.2 us |
| TCP / 8 MiB | 1,342 MiB/s | 1,336 MiB/s | 67.9 ms | 66.0 ms |
| UCX / 4 KiB | 104,137 ops/s | 105,608 ops/s | 97.2 us | 93.8 us |
| UCX / 8 MiB | 3,490 MiB/s | 3,449 MiB/s | 28.3 ms | 31.4 ms |

These read rates are approximately flat. UCX 8 MiB p99 is worse in the final
sample; the changes do not establish a general read-throughput or tail-latency
improvement. Earlier immediate post-write reads ranged much more widely and
must not be substituted for this table.

CPU accounting in fixed-key JSON records process CPU seconds from `/proc` for
PS/EN/manager/etcd and child user+system time for the client. It includes warmup
and process setup, whereas throughput covers the timed interval, so it must not
be presented as an exact cycles/byte or CPU-per-GiB metric. The important proven
mechanisms are eliminated allocation/reinitialization, removed frame-accumulation
copies under namespace checks, and improved ability to batch admitted writes.

## Filesystem

Read-plan benchmark, 2,000 cached 1 MiB read plans per case, nanoseconds per plan:

| Logical file | Legacy before / after | Striped before / after |
|---|---:|---:|
| 1 GiB | 334 / 205 | 333 / 174 |
| 64 GiB | 10,513 / 222 | 11,452 / 175 |
| 1 TiB | 257,064 / 267 | 254,480 / 182 |

This isolates planning CPU: cached metadata and extent maps, no value transfer.
The speedup is not an end-to-end file-throughput multiplier. It demonstrates
that each read no longer copies/scans all extents in the file.

A 1 GiB CLI upload/download round-tripped with `cmp` exit 0. CLI cat smoke times
were 0.882 seconds before and 0.550 seconds after, but the filesystem partition
layout changed between those runs; no causal throughput claim is made from them.
Real filesystem integration tests cover multi-extent content, cross-boundary
subranges, EOF, truncate and cold reload. No new kernel FUSE mount benchmark was
run; existing unrelated mounts were left alone.

## Validation

- Client: 52 unit tests; PS: 242 passed, one ignored; stream: 193 unit tests;
  FUSE: 57 unit tests; UCX-capable TCP pool integration: two tests.
- Existing extent_pipeline and extent_append_semantics integration suites pass.
- system_fuse_read and system_fuse_ns with include-ignored pass (eight test
  entries, including six shared harness checks and two filesystem scenarios).
- Four ablations fail at the intended assertion: TCP pool reuse, PS receiving a
  later request before the first reply, EN receiving into a busy owner mailbox,
  and concurrent descriptor requests to two partitions. Restoring fixes passes.
- Namespace bulk tests cover permitted pooled batch receive, rejection followed
  by a valid frame, and namespace revocation while awaiting the value tail.
- Release build with UCX passes. Scoped clippy commands complete with existing
  warnings; this is not a clean `-D warnings` claim. Full workspace fmt check
  encounters extensive pre-existing formatting differences; new benchmarks and
  edited blocks were formatted, and `git diff --check` passes.

Executable verification commands are in `docs/ops.md` under core-path performance
validation. Root CLAUDE.md asks for an independent subagent review; none was
spawned because the session explicitly restricts delegation to a user request or
AGENTS.md/skill instruction. The review here is the implementing agent's own.
