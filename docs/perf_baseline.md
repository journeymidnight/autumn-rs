# Performance baselines

Current reference numbers for autumn-rs, each with the shape and command it was
measured under. Numbers are only comparable to a run of the same shape on the
same kind of host; read the caveats before calling anything a regression.

All runs: RF=3 (unless stated), every write fsync'd before it is acknowledged.

## Read this before comparing

- **A shared host skews loopback numbers.** When other workloads run on the
  same cores, loopback numbers are not comparable to runs on quiet cores. Measure
  on isolated cpusets, sampling idle cores right before each run and alternating
  A/B/A/B (`docs/ops.md` "Pin the cluster away from other workloads first").
- **io_uring workers stay inside `--cpuset` (now).** Before,
  buffered EN writes borrowed cores outside the cpuset; on a three-disk host the
  aggregate buffered write went 2.0 → 1.3 GB/s when that stopped. Baselines from
  before and after are not comparable. The disk limit of that host was ~2.7 GiB/s
  aggregate write (autumnfs, 24 partitions, buffered, measured before that change).
- **NVMe IOMMU/IOVA degradation on long-running hosts.** After a long uptime,
  large writes drop ~25%, 16 KiB writes to ~1/3, p99 rises ~10×, and the
  perf-check 4K write can halve. A device rebind restores it. Attribute a 4K-write
  drop to the device before blaming code. Hosts booted with `iommu=pt` do not show
  it, so numbers from hosts with and without it do not predict each other.
- **perf-check 4K read is bimodal with write volume.** perf-check reads the keys
  its write phase produced; once a partition switches its 256 MiB memtable, 4K
  read goes from ~1.3M to ~0.5M ops/s. A faster write phase can therefore make
  reads look slower (or faster). For read comparisons use the fixed-key
  benchmarks below.
- **One partition tops out around 30K ops/s** (P-log: one thread, one io_uring).
  Throughput scales with partitions, not per-partition workers; the auto-split /
  auto-merge QPS thresholds are 15K / 1.5K. A perf-check that leaves the bench
  keyspace in one partition (`cut points applied` absent from the log) shows a
  false ~4× write regression.
- **Cross-host UCX 8 MiB needs `--duration ≥ 40`.** The first ~5 s of a UCX leg
  is endpoint warm-up and manager `df` health flapping; a 10 s window halves
  cross-host 8 MiB reads. (The same-host perf-check baselines below are 10 s.)
- **UCX runs bind a RoCE IP** with `UCX_TLS=rc_mlx5,tcp,self` and a pinned
  `UCX_NET_DEVICES`; there is no loopback UCX configuration, and `posix`/`cma` are
  never enabled (the posix large-message path stalls concurrent ≥ 64 KiB transfers).

## 1. perf-check matrix — same host

The regression gate: `perf/perf_check.sh` builds release, starts a fresh 3-replica
cluster, runs 4 KiB and 8 MiB legs and compares each against
`perf/perf_baseline_<transport>_p8_d8_s<size>.json`. A leg passes when ops/s ≥ 80%
of baseline and p99 ≤ 1.2× baseline (`autumn-client perf-check --threshold`,
default 0.8; the p99 ceiling is `2 − threshold`).

Shape: `--3disk` (one EN per disk, three independent NVMe),
8 partitions, 8 EN shards, 16 threads × pipeline depth 8, 10 s per phase, direct
I/O on, io_uring workers confined. TCP on 127.0.0.1 with the cluster on one NUMA node
(one 8-core cpuset per EN, 16 cores for the PS); UCX bound to `[<RoCE IP>]`,
`UCX_NET_DEVICES=<rdma dev>:1`, cluster on the NIC's NUMA node with the same
cpuset sizes. Each file is the median-write run of three.

```bash
export AUTUMN_DATA_ROOT=<nvme dir>/autumn-rs AUTUMN_EXTENT_SHARDS=8   # plus cpusets
AUTUMN_DISKS=<d1>,<d2>,<d3> ./perf/perf_check.sh --3disk --partitions 8 --tcp
AUTUMN_BIND_HOST='[<RoCE IP>]' UCX_NET_DEVICES=<rdma dev>:1 \
  AUTUMN_DISKS=<d1>,<d2>,<d3> ./perf/perf_check.sh --3disk --partitions 8 --ucx
```

| Transport | Size | Write | Write p99 | Read | Read p99 |
|---|---|---:|---:|---:|---:|
| TCP | 4 KiB | 54.9K ops/s | 24.8 ms | 583K ops/s | 0.99 ms |
| TCP | 8 MiB | 2.41 GB/s | 598 ms | 7.46 GB/s | 451 ms |
| UCX | 4 KiB | 18.3K ops/s | 32.8 ms | 964K ops/s | 0.23 ms |
| UCX | 8 MiB | 2.15 GB/s | 1019 ms | 4.98 GB/s | 1551 ms |

UCX 4 KiB writes are slow because every small append pays an rc round trip
(older posix-shm baselines hid this). 4K write on quiet cores ranged
67-84K ops/s; on cores shared with other workloads the same binaries gave
33-48K.

UCX client concurrency: each PS partition runs one UCX worker, and the cliff is set
by endpoints per worker. Keep `client_threads ÷ partitions ≲ 32`; add partitions,
not threads (`perf_check.sh` header has the sweep).

## 2. Fixed-key benchmarks — same host, one partition

`controlled_path` (`crates/server/benches/controlled_path.rs`) and `core_path`
(`docs/ops.md` "Core-path performance validation") use a fixed key set, fixed work
per cell and a recorded partition grid, so read numbers do not depend on what a
write phase left behind. Shape: RF=3 on three independent NVMe, one
partition, client depth 8, TCP loopback or same-host RoCE (`<rdma dev>:1`),
compio 0.19. Median of the interleaved runs, MiB/s:

| Transport | Size | Write (`put`, zero-copy) | PS-proxied read (`get_pooled`) | Direct read (`get_direct`) |
|---|---|---:|---:|---:|
| TCP | 64 KiB | 141 | 1,850 | 2,546 |
| TCP | 1 MiB | 657 | 3,125 | 3,759 |
| TCP | 8 MiB | 811 | 2,834 | 7,614 |
| UCX | 64 KiB | 162 | 2,847 | 2,834 |
| UCX | 1 MiB | 467 | 4,718 | 4,573 |
| UCX | 8 MiB | 483 | 3,562 | 6,549 |

Plain `get` now runs on the same bulk path as `get_pooled` (one extra copy into
the returned `Vec`): TCP 4 KiB 468, 64 KiB 1,799, 1 MiB 3,170, 8 MiB 2,625 MiB/s;
UCX 515 / 2,583 / 4,509 / 3,244 MiB/s.

Partition scaling (8 MiB, depth 8, per-partition work fixed, so four partitions do
four times the work): 1 → 4 partitions gives TCP read 2.98×, write 2.22× (1,991
MiB/s write at 4 partitions); UCX read 2.65×, write 2.66× (1,297 MiB/s).

Large writes are bound by kernel copies and fsync: at 8 MiB, CPU is ~3.9 s/GiB on
the ENs (summed) and ~1.1 s/GiB on the PS (TCP).

## 3. Cross-host (client on another host)

Shape: whole cluster on host A bound to `[<RoCE IP>]` (`--3disk`, r=3,
8 partitions, 4 shards per EN with matching EN cpusets 0-3/4-7/8-11), bench client
on host B, 16 threads × depth 8, RoCE `<rdma dev>:1`, `ulimit -l unlimited` on both
hosts. Only the client↔PS hop crosses the wire.

```bash
# on host A
AUTUMN_BIND_HOST='[<RoCE IP>]' AUTUMN_TRANSPORT=ucx \
  UCX_TLS=rc_mlx5,tcp,self UCX_NET_DEVICES=<rdma dev>:1 \
  AUTUMN_BOOTSTRAP_PRESPLIT=8:hexstring AUTUMN_EXTENT_SHARDS=4 \
  AUTUMN_EXTENT_BASE_PORT=21000 AUTUMN_DATA_ROOT=<nvme dir>/autumn-rs \
  AUTUMN_DISKS=<d1>,<d2>,<d3> bash scripts/cluster.sh start 3 --3disk
# on host B (client built with --features ucx, same wire version)
autumn-client --manager '[<RoCE IP>]:9001' --transport ucx perf-check \
  --threads 16 --duration 40 --size 8388608 --partitions 8 --pipeline-depth 8
```

Latest (wire v28):

| Size | Metric | TCP | UCX | UCX/TCP |
|---|---|---:|---:|---:|
| 4 KiB | write ops/s | 9,974 | 12,482 | 1.25× |
| 4 KiB | read ops/s | 14,256 | 721,148 | 50× |
| 8 MiB | write MB/s | 414 | 1,821 | 4.4× |
| 8 MiB | read MB/s | 240 | 10,932 | 45× |

UCX 8 MiB read is ~87% of 100 GbE line rate (regpool 99.8% hit). An earlier
run of the same shape measured cross-host TCP far higher (4K read 914K ops/s, 8M
read 5.8 GB/s, 8M write 277 MB/s) against UCX 16.0K / 849K ops/s and 1,199 / 9,699
MB/s; the latest TCP read (217-240 MB/s at both 10 s and 40 s) matches a slow
regime seen in another earlier run. The TCP difference between those runs is not
explained; UCX's large-value lead holds in all of them.

Cross-host TCP writes between hosts were once capped near 40 MB/s per file: the EN
set `SO_RCVBUF` on accepted sockets, which disables receive-window autotuning and
froze every PS→EN window at 43 KB. Removing it gave 5.6-6.7×
on a 2-host striped put (37 → 205-247 MB/s). Cross-host numbers from before
that change that involve a remote EN are not comparable. If cross-host is slow,
check `ss -ti` for `rwnd_limited` / a fixed `snd_wnd` first.

Replica placement (UCX, 2 hosts, r=2 with one replica per host vs r=3
all on one host): 8 MiB write +32% (fsyncs on two machines instead of three disks
behind one host), 4K write −23% and 4K read −31% (wire RTT on the per-op path),
8 MiB read unchanged (one link is the ceiling).

UCX multi-rail (cluster on host A, client on host B, 8 threads × depth 8): one
rail was best: 8 MiB read 3,647 MB/s on 1 rail vs 2,661 (2 rails) and 2,544
(4 rails); 4 KiB flat. It supersedes an earlier 2-rail +66% 8 MiB read result.
Keep a single storage NIC.

## 4. Multi-node clusters

**Kubernetes cluster.** 3 PS, 5 EN (4 shards each, local NVMe), 30
partitions, TCP. `perf-check` 4 KiB, 32 threads × depth 16, `--bulk 64`, 20 s per
phase, client pinned to four cores:

| Server build | Write | Read | Write p99 | Read p99 |
|---|---:|---:|---:|---:|
| build A | 631 MiB/s (~162K ops/s) | 1,981 MiB/s (~507K ops/s) | 0.86 ms | 0.38 ms |
| build B | 582 MiB/s (~149K ops/s) | 1,835 MiB/s (~470K ops/s) | 1.09 ms | 0.39 ms |

Separate post-rollout runs with different cache and placement history, not a
controlled A/B.

**Multi-node thread sweep** (`iommu=pt`, TCP 4 KiB, bench keyspace
split into 8 partitions over 3 PS, depth 8, no `--bulk`). The default 16 × 8
perf-check gave write 57K / read 89K ops/s, which is the client's in-flight cap
(128 requests), not the cluster's. Sweeping threads, writes plateau at ~195K ops/s
(~24K per partition, near the per-partition ceiling):

| Threads | 16 | 32 | 64 | 128 | 256 | 512 | 1024 | 2048 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Write K ops/s | 59 | 103 | 143 | 153 | 176 | 191 | 197 | 194 |
| p99 ms | 9.8 | 11.4 | 20.7 | 31.2 | 52.4 | 92.5 | 159 | 441 |

For capacity, sweep threads; do not quote the default 16 × 8 numbers.

**Kubernetes YCSB-equivalent.** 5 nodes (12 vCPU, kernel 5.15), 3 PS, 5 EN
(4 shards, local NVMe), 32 partitions, TCP, `--max-imm-depth 8` (default 4). In-cluster
`autumn-client ycsb`, 1 KB records, zipfian 0.99 per thread, 32 threads × depth 16,
20 s, 9 GB dataset (larger than the memtables, so reads hit SSTs):

| Workload | ops/s | Read p50/p99 ms | Write p50/p99 ms |
|---|---:|---|---|
| Load (insert) | 63-65K | | |
| C (100% read) | ~115K | 3.7 / 18.7 | |
| B (95/5) | ~172K | 2.4 / 10.1 | 3.9 / 10.2 |
| D (95/5 latest) | ~179K | 2.4 / 8.7 | 3.8 / 10.3 |
| A (50/50) | ~124K | 3.2 / 8.6 | 4.4 / 9.7 |
| F (read-modify-write) | ~59K | 8.2 / 15.3 (rmw) | |

```bash
M=$(getent hosts autumn-manager | awk '{print $1}')
autumn-client --manager "$M:9001" --transport tcp ycsb --threads 32 --duration 20 \
  --size 1024 --partitions 32 --pipeline-depth 16 --records 280000 \
  --key-dist zipfian --read-ratio 0.95          # C=1.0  B/D=0.95  A=0.5  F=--rmw
```

A dataset that fits the memtables inflates reads 4-6× (C 730K ops/s); do not quote
it. Write findings from the same cluster: 4 KiB writes scale with partitions
(1 → 8 → 16 → 32: 7.7K → 34.7K → 59K → 65K, plateau); 4 KiB is the worst value size
for write amplification (~19×: the largest value still stored inline and
recompacted), while values > 4 KiB become value pointers written once (8 KiB ~6×,
8 MiB ~3× = RF only, 584 MB/s write / 1,073 MB/s read); 1 → 4 EN shards barely
moves peak write (65K → 72K) but cuts write p99 46 → 17 ms.

## 5. Findings that hold

- **Batch RPCs decide cross-host small-write throughput.** One op per frame over
  a real wire leaves each group-commit batch at ~14 ops (fsync + replica fan-out
  per batch dominate); a frame carrying N ops fills the batch at once.
  Cross-host TCP 4 KiB went 6.5K ops/s (per-op, p50 148 ms) → 45K
  (batched, 64 threads × 64 keys, p50 1.25 ms). perf-check exposes this as
  `--bulk N`; the kvcache and fs clients already batch.
- **UCX wins large values, TCP is competitive at 4 KiB.** The pooled /
  zero-copy bulk path engages for values ≥ 64 KiB (`BULK_MIN_BYTES`); below that
  per-op endpoint overhead makes UCX no faster than kernel TCP.
- **8 MiB writes are fsync- and copy-bound, not wire-bound**: spread replicas over
  hosts rather than over disks of one host.
- **Receive copies:** every receiving process does exactly one
  transport copy (TCP kernel copy or UCX Stream unpack). Application copies at
  8 MiB are 0.002–0.009× on every link except PS→EN append: 0.186× summed over
  3 ENs on TCP (~0.06× per replica), 0.048× on UCX — the part of an append that
  arrives before the EN has parsed its header (largest at 1 MiB). The other
  remainder is the 64 KiB prefix shared with the frame header (1.0× of a
  64 KiB value). Counting
  tooling: `perf/receive_copies`, in git history.
- **compio 0.19 vs 0.18 (640 fixed-work samples):** no broad CPU
  reduction; large single-partition TCP writes slightly faster. TCP send zero-copy
  (`--tcp-zerocopy-min-bytes`, default 0 = off) is slower on loopback (8 MiB
  single-partition write 711 vs 898 MiB/s, more CPU/GiB) and stays off. UCX 64 KiB
  single-partition reads were ~8% lower on 0.19 (2,573 vs 2,797 MiB/s; not at 4
  partitions), unresolved. The intermittent UCX `Local protection error` on SEND
  seen on both versions was UCX 1.16's registration-cache race; UCX ≥ 1.17 is
  required, and receives no longer ask for `NO_IMM_CMPL`, whose in-call completion
  reported an uninitialised status on release builds (ops.md "UCX library").
  SQPOLL adds CPU.
- **Replica CRC:** an append's payload crc32c is computed once per payload and
  rides every replica's ctrl (wire 80); before, each replica's frame scanned
  the payload again below 1 MiB, and sharing one scan via `crc32c_combine`
  above it.
- **fs read planning is O(intersecting extents)**: 1 TiB file, 2,000 cached 1 MiB
  plans, sub-microsecond per plan for both layouts
  (`cargo bench -p autumn-fs --bench read_plan -- <manager> [iterations]`; needs
  a running cluster).

Raw evidence for sections 2 and 5 (`perf/core_path_*/`, `perf/compio_upgrade_*/`,
`perf/controlled_validation/`, `perf/receive_copies/`) is no longer in the tree;
`git log -- perf/` finds it.
