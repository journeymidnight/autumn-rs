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
