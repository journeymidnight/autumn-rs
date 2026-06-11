# autumn-rs

Rust rewrite of `autumn`: a distributed KV storage engine. Architecturally inspired by the
**Azure Windows Azure Storage (WAS)** paper — a stream layer handles raw distributed log
storage, a partition layer builds an ordered KV store on top.

## Architecture

```
  Clients   (autumn-client / autumn-fuse / autumn-kvcache / your app)
     │  Put / Get / Delete / Range  (custom binary RPC on compio)
     ▼
  autumn-ps   Partition Server, one or more
  ┌────────────────────────────────────────────────────────┐
  │  LSM-tree per partition (memtable → SST)               │
  │  Each partition owns 3 streams:                         │
  │     log_stream   WAL + values > 4 KiB                   │
  │     row_stream   flushed SSTables                       │
  │     meta_stream  TableLocations checkpoints             │
  └──────────┬──────────────────────────────────────────────┘
             │  append / read
             ▼
  autumn-extent-node   one or more, holds raw extent files on local disk

  autumn-manager-server   control plane, etcd-backed
  ├── allocates streams + extents
  ├── routes partition → PS assignments
  ├── drives extent recovery + EC conversion
  └── runs the split / merge / GC advisory engine
```

**The three interfaces** built on top of the partition layer:
- **autumn-client** — generic KV CLI / SDK (`crates/client`)
- **autumn-fuse** — POSIX filesystem (`crates/fuse`, mount as a regular FS)
- **autumn-kvcache** — sglang / vLLM HiCache L3 backend (`python/autumn_kvcache`)

Each partition owns exactly 3 streams; streams are made of variable-length **extents** that
land on extent-nodes. Per-extent replication factor is fixed at 3 while open; the manager
EC-converts sealed extents (default K+1 parity) in the background. See
`crates/stream/CLAUDE.md` for the commit-protocol / fencing details, `crates/partition-server/CLAUDE.md`
for the LSM + split/merge mechanics.

## Prerequisites

- Rust toolchain (edition 2021)
- `etcd` ≥ 3.5 in PATH (one-liner: see [etcd releases](https://github.com/etcd-io/etcd/releases))
- `libfuse3-dev` (only if you build/run `autumn-fuse`)
- Linux ≥ 5.15 (compio uses io_uring)

## Build

```bash
cd autumn-rs
cargo build --release --workspace
# Or just one binary:
cargo build --release -p autumn-server      # manager / EN / PS / autumn-op / autumn-client
cargo build --release -p autumn-fuse         # FUSE filesystem
```

UCX (RDMA) is optional and OFF by default:
```bash
cargo build --release -p autumn-server --features ucx
```
See `crates/transport/CLAUDE.md` for the UCX runtime selection rules.

## Quick Start (single-host dev cluster)

The repo ships `start.sh` / `stop.sh` for a 1-host cluster (etcd + manager + 5 ENs + 1 PS,
data on local NVMes). Edit `DATA_DIRS` at the top of `start.sh` to match your disks; defaults
target `/data{03,05,06,07,08}/autumn-rs`.

```bash
./start.sh                   # bring up the cluster, run bootstrap
./target/release/autumn-op --manager 127.0.0.1:9001 info   # 5 nodes online, 1 partition

# Basic KV
AC="./target/release/autumn-client --manager 127.0.0.1:9001"
echo hello | $AC put mykey /dev/stdin
$AC get mykey                                              # → hello

./stop.sh                    # graceful shutdown
./stop.sh --wipe             # shutdown + wipe data (clean re-bootstrap)
```

Env knobs (set before `./start.sh`):
- `TRANSPORT=ucx` — use UCX instead of TCP (cluster must be UCX-built)
- `EN_BASE_PORT=NNNNN` — move EN ports if 18101+ conflicts on your host
- `WORK=/some/path` — override etcd + log + PS-local dir (default `/var/lib/autumn-rs`)

For full-feature multi-node / EC / chaos / per-process control use `cluster.sh` instead —
it's a richer driver (auto-EC bootstrap, per-process kill/restart, affinity layout, presplit
support). See `cluster.sh --help` and the test harness in `crates/manager/tests/support/`.

### Mount autumn-fuse

```bash
mkdir -p /mnt/autumn
nohup ./target/release/autumn-fuse \
    --manager 127.0.0.1:9001 \
    --mountpoint /mnt/autumn \
    --transport tcp \
    > /var/lib/autumn-rs/logs/fuse.log 2>&1 &

ls /mnt/autumn               # empty dir
echo hi > /mnt/autumn/x
cat /mnt/autumn/x            # → hi

fusermount3 -u /mnt/autumn   # unmount (needs `fuse3` package)
```

## Binaries

| Binary | Default port | Role |
|---|---|---|
| `autumn-manager-server` | 9001 | Control plane (streams, partitions, recovery) |
| `autumn-extent-node` | 9101+ | Data plane (raw extent files on disk) |
| `autumn-ps` | 9301 (+ per-partition) | LSM partition server |
| `autumn-client` | — | Data-plane CLI (put/get/del/head/ls/perf-check) |
| `autumn-op` | — | Admin CLI (bootstrap/split/merge/compact/gc/info/format) |
| `autumn-stream-cli` | — | Low-level stream debugging |
| `autumn-fuse` | — | FUSE mount of the KV namespace |

`autumn-client --help` / `autumn-op --help` lists subcommands. The wire schema for autumn-op
is stable; the Python policy controller in `python/node_policy.py` shells out to it.

## CLI cheatsheet

```bash
AC="./target/release/autumn-client --manager 127.0.0.1:9001"
AO="./target/release/autumn-op     --manager 127.0.0.1:9001"

# Data plane
echo body | $AC put KEY /dev/stdin       # write
$AC get KEY                              # read
$AC head KEY                             # size only
$AC del KEY                              # delete
$AC ls --prefix p/ --limit 100           # scan
$AC put-stream KEY /path/to/big.bin      # chunked striperados for large values
$AC perf-check --threads 16 --size 4096 --duration 10 --partitions 8

# F261 — SST block cache (paged SSTs; SST data blocks no longer RAM-resident)
# PS flag: autumn-ps --sst-block-cache-bytes N   (cluster.sh: AUTUMN_SST_BLOCK_CACHE_BYTES, default 512MB)
# Manual check: write >> RAM dataset, kill -TERM the PS, restart, then
#   `$AC get KEY` must byte-match and idle PS RSS stays at the replay-window
#   bound (GBs), not O(dataset). Recovery must log `open_partition: ready`
#   for every partition with no `stale_vp_offset_past_sealed_length` retries.
# F262 — async SST iteration (no whole-SST materialization for range/compact/split)
# Manual check: on a multi-GB dataset, `$AO compact PART_ID` must log
#   "compact part N: ... output=..." and `$AC ls --prefix p/` must return
#   correct entries, while PS RSS stays bounded during both (read side =
#   8MiB windows, not Σ SST bytes). Striped keys (put-stream) byte-compare
#   via `$AC get-stream --out F KEY` (plain `get` returns the 29-byte
#   stripe meta by design).

# Admin / observability
$AO info                                 # nodes / extents / streams / partitions
$AO bootstrap --replication 3+0 --presplit 8:hexstring
$AO split PART_ID
$AO merge SURVIVOR_PART_ID VICTIM_PART_ID
$AO compact PART_ID
$AO gc PART_ID --ratio 0.4
$AO policy-candidates                    # advisory engine output (split/merge/gc/compact/EC)

# Cluster lifecycle helpers
./start.sh                               # this repo: 1-host 5-EN dev cluster
./stop.sh --wipe                         # tear down + wipe
./cluster.sh start 3                     # richer driver: 3-replica cluster + auto-EC
```

## Tests

```bash
cargo test --workspace --exclude autumn-fuse --lib          # all crate unit tests
cargo test -p autumn-stream --lib                            # stream layer only
cargo test -p autumn-partition-server --lib                  # partition server only
cargo test -p autumn-manager                                 # integration (needs etcd)
```

Manager integration tests under `crates/manager/tests/` cover split / merge / chaos / crash
recovery. Some are gated with `#[ignore]` because they take minutes; use
`cargo test --release -- --ignored` to run the slow set.

**Write-pipeline changes (e.g. F256 natural batching) are verified with the perf matrix**
(`./perf/perf_check.sh --3disk --partitions 8` — builds release, starts a fresh 3-replica
cluster, runs tcp/ucx × 4K/8M and compares each leg against
`perf/perf_baseline_<transport>_p8_d8_s<size>.json`; a leg passes when ops/s ≥ 80% of
baseline and p99 ≤ 2×). The `--min-pipeline-batch` PS flag is deprecated since F256
(parsed, warns, no effect) — batch sizing is adaptive and needs no tuning knob.

## Inode-lease + close-to-open coherence (F-ioring-lease, in flight)

Multi-mount / multi-daemon coherence for `autumn-fuse` and
`autumn-ioring-daemon` runs through a JuiceFS-style inode lease served
by the manager. Plan + invariants live in
[`docs/autumn_fs_lease_plan.md`](docs/autumn_fs_lease_plan.md).

Landed so far:
- **F-ioring-lease-1** — manager state + 4 RPCs (`MSG_*_LEASE` /
  `MSG_POLL_INVALIDATIONS` = `0x46`–`0x49`), writer-lease etcd
  persistence under `inode_leases/<ino>`, TTL revoke loop.
- **F-ioring-lease-2** — autumn-ioring-daemon Open acquires (and
  Close releases) a write/read lease per inode. `RING_VERSION 1→2`:
  the Open SQE's flags byte now carries `LEASE_MODE_READ` (1) /
  `LEASE_MODE_WRITE` (2). A v1 client (flags=0) is interpreted as
  WRITE — the safe default. Two concurrent writers on the same
  inode (different daemons OR different sessions of the same
  daemon) get `libc::EBUSY` on the second Open.
- **F-ioring-lease-3** — long-poll invalidation channel.
  `MSG_POLL_INVALIDATIONS` blocks up to 10 s when the inbox is
  empty (manager parks a waker); a writer-close pushed by ANOTHER
  daemon fires the waker so the reader sees the event in ms, not
  via a retry tick. Daemon spawns a persistent
  `session_invalidation_poll_loop`; on transport error or overflow
  sentinel it wholesale-invalidates the session cache.
- **F-ioring-lease-4** — `OpenedExtents.lease_version` populated
  from the AcquireLease response; per-session `InvalidationMap`
  bumped by the poll loop. Read SQE arm calls `cache_is_stale`
  and on stale invokes `fuse_read::reload_extents` to re-fetch
  the inode meta + extent map before serving — close-to-open
  coherence end-to-end. **Phase 1 complete.**

Smoke-tests (no cluster boot required):

```bash
# Manager-side state machine + RPC contract.
cargo test -p autumn-manager --lib inode_lease
cargo test -p autumn-manager --test f_ioring_lease

# Daemon-side lease helpers + two-daemon conflict / read-coexistence /
# version monotonicity / heartbeat round-trip.
cargo test -p autumn-manager --test f_ioring_lease_2

# Long-poll: writer-close wakes a parked reader in ms (3 tests; the
# idle-timeout case waits the full 10s LONG_POLL_WAIT — ~30 s total).
cargo test -p autumn-manager --test f_ioring_lease_3

# Close-to-open cache invalidation: per-ino floor bumps on
# WriterClosed; reader's stale-cache predicate flips; overflow
# sentinel surfaces (overflow test takes ~10 s for its 1025 cycles).
cargo test -p autumn-manager --test f_ioring_lease_4
```

Daemon manual exercise (against a real cluster):

```bash
# Start a one-node cluster (cluster.sh reset 1) then a daemon:
cargo run -p autumn-ioring --features daemon --bin autumn-ioring-daemon -- \
  --manager 127.0.0.1:9001 --socket /tmp/ring.sock --runtimes 1

# Two test apps each call IoRingClient::submit with
#   Sqe { opcode: Opcode::Open, lease_mode: SQE_LEASE_MODE_WRITE, ... }
# against the same path → second CQE.result == -libc::EBUSY.
```

Phase 1 is complete. Future work tracked under separate features:
- **F-fuse-lease-1/2/3** — autumn-fuse mount opt-in: open/release
  call lease::acquire/release; kernel attribute cache invalidated
  via `fuser::notify_inval_inode`.
- **F-lease-preempt** — force-revoke / writer revoke protocol so
  "another daemon needs to write NOW" doesn't have to wait for
  the current writer to close.

## Documentation map

For anything deeper than the surface here, the source-of-truth lives in:

- **`CLAUDE.md`** — repo-wide engineering rules (long-task workflow, progress account, etc.)
- **`feature_list.md`** — feature ledger (F-numbered, every shipped feature with acceptance + status)
- **`crates/<name>/CLAUDE.md`** — architecture guide per crate:
  - `crates/manager/CLAUDE.md` — control plane, leader election, etcd mirror, 30+ programming notes
  - `crates/stream/CLAUDE.md` — extent nodes, commit protocol, fencing, recovery
  - `crates/partition-server/CLAUDE.md` — LSM, group commit, split/merge, F148-A publish invariant
  - `crates/client/CLAUDE.md` — SDK API + retry / epoch refresh contract
  - `crates/transport/CLAUDE.md` — TCP vs UCX selection
  - `crates/fuse/CLAUDE.md` — autumn-fuse design (3FS-inspired)
- **`docs/`** — design specs (autumn-kvcache plan, perf analyses, RFCs)

## License

See `LICENSE`.
