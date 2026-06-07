# autumn-ioring Crate Guide

## Purpose

User-space io_uring-style daemon + client library that exposes a SHM-based
SQ/CQ ring over a Unix-domain socket, backed by `autumn-client` reads/writes
into autumn-rs's KV layer.

Three pieces:

| Module | Role |
|---|---|
| `bin/daemon.rs` | The server. One or more compio runtimes, each owns a `ClusterClient`, a `UnixListener`, and the session pool. |
| `bin/bench.rs` (`autumn-ioring-bench`) | Throughput / latency bench against a running daemon. |
| `client.rs` etc. | Client-library helpers: handshake, ring header layout, SQE / CQE codecs, buffer-pool management. |

## Multi-runtime daemon (`--runtimes N`)

The daemon is N OS threads, each with its own compio runtime + ClusterClient
+ UnixListener on `{socket}.{idx}`. Sessions are pinned to whichever runtime
accepted them — no cross-thread state. Clients distribute by `tid % N` to
pick a runtime index when connecting.

`--runtimes 1` (the default) preserves backward compat: one socket, one
runtime, one daemon process.

### Measured sweet spot (2026-06-06)

Tested on the 3-NVMe r=3 cluster (`p=16 partitions × shards=16`, base port
12000) with `--key 'dataset/sample.bin'` (single-key fan-in):

| Bench | runtimes=1 | runtimes=4 | runtimes=16 |
|---|---|---|---|
| 4K read t=16 d=8 | 53K ops/s | **59K ops/s (+12 %)** | 47K ops/s |
| 4K read t=64 d=8 | (n/a) | 59K ops/s | 50K ops/s |
| 4K read t=256 d=8 | 141 ops/s (collapse) | 1,360 ops/s (10× recovered) | 61 ops/s (worse) |
| 8M read t=16 d=1 | 154 ops/s | 151 ops/s | 148 ops/s |
| 4K write t=16 d=8 | 6.2K ops/s | **9.1K ops/s (+47 %)** | (n/a) |
| 4K write t=32 d=8 | (n/a) | 8.5K ops/s | (n/a) |
| 8M write t=16 d=1 | 22 ops/s | 21 ops/s | (n/a) |

**`--runtimes 4` is the practical sweet spot.** More runtimes (16) regress
because:

1. **Per-key fan-in.** All bench threads read the same `dataset/sample.bin`,
   so every SQE routes to the one PS partition that owns that key. More
   daemon runtimes can't split work the cluster has already serialised
   onto one partition.
2. **Per-runtime ClusterClient overhead.** Each runtime opens its own pool
   of PS connections. With 16 runtimes the cluster sees 16× the connection
   count without proportionally more useful concurrency.
3. **t=256 session-pool overload.** Per-session compio task overhead
   (handshake + lease bookkeeping + per-SQE poller) saturates even with 16
   runtimes when sessions exceed ~64/runtime. The pre-multirun collapse
   (141 ops/s at runtimes=1) softens to 1,360 ops/s at runtimes=4 but stays
   far below the t=16 ceiling.

8M reads/writes are bandwidth-bound; multi-runtime doesn't help them.

### What `--runtimes 4` does NOT fix

- **Single-extent serial write.** Each Write SQE runs one
  `fuse_write::write_into` end-to-end (extent walk + 1 `kv_put` + inode
  meta update). No client-side write coalescing. F244-D would close this
  gap by adding a per-session per-inode 1 MiB write buffer that fans out
  via `put_many` — comparable to FUSE's 1 MiB write_buf which is why FUSE
  4K-write looks fast (1 MiB buffer absorbs N pwrites into one fan-out).
- **Per-key fan-in.** A real workload (e.g. kvcache reading many distinct
  pages) doesn't have this artifact, but synthetic single-key benches do.
- **t=256+ scaling.** Eventually limited by daemon process resources
  (FD pressure, compio scheduler scaling).

### Recommended defaults

| Workload shape | `--runtimes` | Why |
|---|---|---|
| Production kvcache (8M block reads, ≤32 client threads) | 1 or 2 | Sufficient; doesn't justify extra runtime overhead. |
| Mixed RW perf bench (4K + 8M, t=16-64) | **4** | Gives +12-47 % over default with no downside at these thread counts. |
| Single-shot batch importer / migration tool | 1 | Per-session overhead amortises poorly. |
| t=256+ (uncommon) | 4 | At higher runtime counts the collapse comes back. |

## Bench (`autumn-ioring-bench`)

Mirrors the `--runtimes` arg of the daemon — `--runtimes N` makes worker
threads pick `{socket}.{tid % N}` so they spread across daemon runtimes.

`--mode <read|write>` (added 711cf1a): read mode uses the existing hot-key
shape; write mode opens with WRITE lease + submits Write SQEs with a
pre-filled deterministic payload. The bench's lease-per-file invariant means
write mode requires `--key 'path/wb_t%tid%.bin'` (the `%tid%` token expands
to the 2-digit worker tid), so each thread writes a distinct inode and
they don't fight for the per-inode WRITE-exclusive lease. The operator
must pre-create those files via the fuse mount before the bench (the
daemon's Open is fuse-style `resolve_path` and expects the file to exist).

Cluster prep for IORING benches:

1. Start the autumn cluster (`p=16 shards=16` recommended for throughput
   tests; see `.claude/skills/perf-check/SKILL.md`).
2. Mount fuse at `/mnt/abench`. Seed:
   - Read: write `dataset/sample.bin` of the right size via `dd` on the
     mount.
   - Write: pre-create per-thread targets `wb_t00.bin` … `wb_tNN.bin`.
3. Start the daemon: `--socket /run/autumn-ioring/ring.sock --runtimes 4`.
4. Run the bench with matching `--runtimes 4 --socket /run/autumn-ioring/ring.sock`.

Lease caveat between runs: writer leases hold for `DEFAULT_LEASE_TTL_SECS`
(30 s) after the bench drops them. Sequential write benches against the
same files within 30 s see EBUSY. Either restart the daemon (kills the
session, frees the lease at the manager) or wait the TTL.

## Large-file (≥ 1 MiB) write throughput — what works and what doesn't

Investigated 2026-06-07. Reference cluster: 3-disk r=3, p=16, shards=16.

### Measured ceilings

| path | 8 MiB write best | what it means |
|---|---|---|
| KV `perf-check` (t=64 d=8) | **2.24 GB/s** | SDK direct: pipeline_depth=8 saturates all 16 PS partitions in parallel |
| FUSE `fuse_wbench` (t=64) | 622 MB/s | **largely buffer-fill rate, NOT durable** — see "FUSE caveat" below |
| IORING bench (--runtimes 16 t=16 d=1) | **220 MB/s** | actual durable per-Write-SQE KV writes |

### IORING 220 MB/s is the structural ceiling at d=1

Each Write SQE goes through `write_into` which awaits one `cluster.put_zc` per
extent step (a 4 KiB-to-8 MiB Append step = 1 future). For one ino, the
extent_key `[0x03][ino BE][off BE]` routes by ino to ONE PS partition. So per
session, throughput is bound by 1 partition's 8 M write rate
(≈ 140 MB/s on this hardware). With 16 sessions on 16 partitions = 16 × 140 =
**2.24 GB/s theoretical** — but `--depth 1` per session means each only
keeps 1 in-flight at a time. 16 sessions × 1 in-flight = ~16 concurrent ops,
landing 220 MB/s in practice (= 1 partition's worth, plus some daemon
overhead).

`--depth > 1` was tested (2026-06-07) and didn't help — the daemon's
`FuturesUnordered` runs SQEs concurrently per runtime but the path through
`cluster.put_zc` for same-ino keys serialises per-partition. Adding depth just
deepens the per-partition queue without improving aggregate throughput.

### What does NOT help for 8 MiB writes

* **Within-SQE chunking** (split a single Write SQE's payload into N x 1 MiB
  sub-puts via `put_many`). Tested as F244-D Phase 3 then reverted.
  Same-ino chunks all route to the SAME PS partition — turns one
  serial put into N concurrent puts on the same partition_loop, which actively
  HURTS at high client concurrency (t=64 dropped from 175 → 80 MB/s vs no
  chunking). FUSE achieves higher numbers by chunking ACROSS DIFFERENT INODES
  (per-thread distinct file), which routes to distinct partitions; an IORING
  Write SQE can't chunk across inodes — it's by definition one ino.

* **Increasing `--depth`** beyond 1 for the bench. 8 M ops at d=2/4/8 sit
  at the same ~25 ops/s as d=1. The daemon doesn't expose more
  per-runtime concurrency than the bench feeds it, and the cluster path
  serialises per-ino regardless.

* **More `--runtimes`** past the partition count. Tested at 4/16. 16 wins
  at t=16 (one runtime per session) but going higher fragments the daemon
  for no gain.

### What WOULD help (deferred, see "Phase 3 retrospective" below)

* **Cross-SQE batching across DIFFERENT inodes.** Multiple Write SQEs from
  distinct sessions, deferred and bundled into ONE `put_many` across the
  entire batch. This is the "FUSE's 64 MiB write_buf" model, but at the
  daemon level across all sessions. Would amortise per-Write-SQE bookkeeping
  AND let the SDK's `fan_out_collect` keep multiple partitions saturated
  from a single batch call. Substantial change to the CQE-delivery contract
  (CQEs delayed until batch flushes) and to the daemon's session model.

* **A different transport for large values.** Stream the payload to PS via a
  multi-RPC sequence (start, N x chunk, end) instead of one giant put_zc.
  Lets the daemon pipeline data on the wire and the PS pipeline storage
  while data is still arriving. Requires PS-side protocol additions.

### FUSE caveat — the 622 MB/s number is buffer rate

`crates/fuse/src/write.rs`'s `WriteBuffer` is **64 MiB per inode** and flushes
on overflow or close. The `fuse_wbench` reference bench does NOT call
`fsync` between writes, so the reported ops/s is the rate at which the bench
fills the per-inode buffer — only every 8 ops (8 × 8 MiB = 64 MiB) does an
actual flush fire. The flush itself uses `put_many` at
`APPEND_PIPELINE_DEPTH` over ≤ `WRITE_BUF_EXTENTS` extents, which IS fast
(matches KV ceiling), but the bench's average throughput is dominated by
the cheap buffer-fill rate between flushes.

An application that needs every write durable (calls fsync, like a kvcache
checkpoint flush) will see FUSE drop close to the IORING / KV-direct floor.

### Recommended path per workload

| workload | tool |
|---|---|
| sglang kvcache write (large blocks, durability needed) | autumn-client SDK or Python BatchClient directly — skip the FUSE/IORING translation tax |
| POSIX-API workload that tolerates buffer-flush latency | FUSE mount (the 64 MiB write_buf is helpful here) |
| POSIX-like file abstraction with low-latency single ops | IORING daemon (4 K writes 12.5 K ops/s post-Phase-2, low-latency CQE per SQE) |
| 8 MiB throughput pushing absolute ceiling | autumn-client direct, t=64 d=8 (2.24 GB/s) |

The IORING daemon's strength is per-op latency + the SHM ring contract, not
peak large-write throughput. Don't try to make it match KV-direct's 2 GB/s
ceiling — the daemon is structurally an intermediate layer and the math
above caps it at ~220 MB/s on this hardware unless we add cross-SQE
batching (which trades the per-op CQE contract for throughput).
