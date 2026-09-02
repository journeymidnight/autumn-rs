# autumn-rs on Kubernetes — perf investigation + YCSB (2026-07-03)

Benchmarks from the first real-cluster (Volcengine VKE) bring-up. Numbers are
specific to THIS cluster/config — treat as a baseline + methodology record, not
absolute limits. Driver is `autumn-client ycsb` (in-cluster), NOT the reference
Java YCSB — see caveats.

## Cluster under test

| | |
|---|---|
| Nodes | 5 × Ubuntu 22.04 (kernel 5.15), 12 vCPU / 93 GiB each |
| Roles | 3 PS (`replicas: 3`, anti-affinity), 5 EN, 1 manager, 1 etcd |
| EN storage | local **Solidigm 3.84 TB NVMe** (`nvme0n1`), NOT the 40 GB cloud root disk |
| EN sharding | **4 shards** (`AUTUMN_EXTENT_SHARDS=4`, cpuset 0-3) |
| Partitions | 32 (`AUTUMN_BOOTSTRAP_PRESPLIT=32`), ~11 per PS |
| Durability | RF=3, **every write fsync'd** (unconditional), TCP transport |
| Admission tune | `MAX_IMM_DEPTH=8`, `MAJOR_COMPACT_PARALLELISM=8`, compact-rate unlimited |
| Deploy | `deploy/overlays/vke` (see `docs/k8s_deploy.md`) |

## YCSB-equivalent results

`autumn-client ycsb`, 1 KB records (YCSB default), zipfian keys (theta 0.99),
32 threads / 32 partitions / pipeline-depth 16, 20 s run per workload.

**⚠️ Dataset size dominates read numbers.** With a small dataset that fits the
256 MB memtable, reads never touch SSTs (pure in-DRAM) and are inflated ~4–6×.
The representative numbers use a **9 GB dataset (280 MB/partition > the flush
threshold)** so reads go through the real path (bloom → block index → row_stream
+ block cache).

### Representative (out-of-memory, SST-bound) — use these

| Workload | Throughput | Read p50/p99 | Write p50/p99 |
|---|---|---|---|
| Load (100% insert) | 63–65K ops/s | — | — |
| **C** (100% read) | **~115K ops/s** | 3.7 / 18.7 ms | — |
| **B** (95/5) | **~172K ops/s** | 2.4 / 10.1 ms | 3.9 / 10.2 ms |
| **D** (95/5 read-latest) | **~179K ops/s** | 2.4 / 8.7 ms | 3.8 / 10.3 ms |
| **A** (50/50) | **~124K ops/s** | 3.2 / 8.6 ms | 4.4 / 9.7 ms |
| **F** (read-modify-write) | **~59K ops/s** | (rmw) 8.2 / 15.3 ms | |

Notes: C/B/D land ~115–180K (the C<B gap is run-order + block-cache-warmth
noise, not "writes speed it up"). F is the floor — each op is a serial
get-then-put (a read + an RF=3 durable write).

### Cache-hot (in-memory, 640 MB dataset) — reference only, NOT representative

C 730K · B 414K · D 409K · A 138K · F 63K ops/s. Reads here are pure memtable
hits; do not compare against published YCSB (which uses out-of-memory sets).

## Supporting findings (write path)

Pure-write sweeps that explain the shape above. Reads scale with PS count;
**writes are the ceiling** (RF=3 fsync + compaction of *inline* values).

**Write throughput scales with partition count** (4 KB, RF=3, burst-on-empty):

| partitions | write ops/s |
|---|---|
| 1 | 7.7K |
| 8 | 34.7K (×4.5) |
| 16 | 59K (×1.7) |
| 32 | 65K (×1.1, plateau) |

**Value size sets write amplification** (sustained/steady-state write):

| value | steady write | write-amp | why |
|---|---|---|---|
| 1 KB | ~43K ops/s | low | inline, small |
| 4 KB | ~26–33K ops/s | **~19×** | inline, largest inline value → most compaction |
| 8 KB | ~38K ops/s | ~6× | > `VALUE_THROTTLE` (4 KB) → ValuePointer, skips SST compaction |
| 8 MB | 584 MB/s write / 1073 MB/s read | ~3× (RF only) | VP + zero-copy, bandwidth-bound |

4 KB is the pathological worst case (inline + max compaction bytes). Values
> 4 KB become ValuePointers (written once to log_stream, never recompacted).

**Multi-shard EN** (1→4 shards): peak write barely moved (~65→72K — ENs were
never CPU/disk-saturated, ~2/12 cores, 622 MB/s of ~2–3 GB/s NVMe), but write
**p99 dropped 46→17 ms** (fsync spread across 4 io_uring cores). It's a
tail-latency win, not a throughput unlock.

**Compaction/admission tuning** (`MAX_IMM_DEPTH 4→8`, `MAJOR_COMPACT_PARALLELISM
4→8`, unlimited compact-rate): steady 4 KB write +~25% (~26→~32K) and a
longer/higher burst, at the cost of write p99 (46→97 ms) and memory (up to
2 GB/partition). The per-partition compact *rate* limit (256 MB/s × 32 = 8 GB/s)
is NOT the constraint; the constraint is the imm-full write stall.

**Steady-state limiter** (root cause): neither EN CPU nor NVMe bandwidth
(both had headroom); it's the per-partition RF=3-fsync append pipeline latency
plus flush/compaction of inline values → the imm-depth write stall.

## Caveats

- **YCSB-*equivalent*, not reference Java YCSB.** Same workload definitions
  (ratios, zipfian, 1 KB record), but our own in-cluster Rust driver
  (`autumn-client ycsb`) — no official YCSB binding exists for autumn. A real
  Java binding is the only nitpick-proof path.
- Zipfian skew is **per-thread** (each thread hot on its own partition), not a
  single global hot key.
- Cross-system comparison requires matching RF + durability + hardware +
  workload. autumn's RF=3 + per-write fsync is stricter than many defaults.

## Reproduce

```bash
# from an in-cluster client pod (v1 clients dial PS pod IPs directly)
M=$(getent hosts autumn-manager | awk '{print $1}')
# representative single workload (out-of-memory: records > 256 MB/partition):
autumn-client --manager "$M:9001" --transport tcp ycsb \
  --threads 32 --duration 20 --size 1024 --partitions 32 \
  --pipeline-depth 16 --records 280000 --key-dist zipfian --read-ratio 0.95   # B
# workloads: C=--read-ratio 1.0  B/D=0.95  A=0.5  F=--rmw
```

See `crates/server/CLAUDE.md` (autumn-client `ycsb`) for flag semantics and
`docs/k8s_deploy.md` for the deployment.
