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
./target/release/autumn-op --manager 127.0.0.1:9001 df     # cluster capacity (Ceph `ceph df` style)

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
| `autumn-op` | — | Admin CLI (bootstrap/split/merge/compact/gc/info/df/format) |
| `autumn-stream-cli` | — | Low-level stream debugging |
| `autumn-fuse` | — | FUSE mount of the KV namespace |

`autumn-client --help` / `autumn-op --help` lists subcommands. The wire schema for autumn-op
is stable; the Python policy controller in `python/node_policy.py` shells out to it.

### Cluster capacity — `autumn-op df`

Ceph-`ceph df`-style aggregate capacity. RAW + autumn `physical_used` are summed
from every extent node's `df` report (each EN self-reports the REAL on-disk byte
count of its extents — replicas, EC shards, open tails — no amplification
formula); `STORED(sealed)` is the manager's de-amplified Σ distinct
`sealed_length`. Because EC makes usable LOGICAL capacity a RANGE (cold EC
1.25–1.33× vs hot 3-replica), `df` shows the empirical `AMPLIFICATION`
(`physical_used / stored`) plus the writable estimate as a range
`[raw_free/3 .. raw_free/best_ec]`:

```bash
autumn-op --manager 127.0.0.1:9001 df          # human-readable
autumn-op --manager 127.0.0.1:9001 --json df   # for scripts

# Sanity-check against the EN filesystems:
#   RAW total/free  ≈  Σ `df -h` of each EN data dir
#   PHYS_USED       ≈  Σ `du -sb` of each EN extent dir
```

The same snapshot backs FUSE `statfs`: `df -h <mountpoint>` now reflects real
backend capacity (conservatively, at the 3-replica factor) instead of a fixed
placeholder.

### Prometheus /metrics (observability batch 1)

Every server binary takes an opt-in `--metrics-port <PORT>` flag exposing a
Prometheus text endpoint at `http://<listen-host>:<PORT>/metrics` (plain
`std::net` listener on its own OS thread — zero interaction with the
io_uring data plane; absent flag = no listener). `cluster.sh` wires all
three with `AUTUMN_METRICS=1` (manager `9591`, EN `960<i>`, PS `9701`).

```bash
AUTUMN_METRICS=1 AUTUMN_TRANSPORT=tcp ./cluster.sh start 3
curl -s http://127.0.0.1:9591/metrics   # manager: leader/serving + streams/extents/nodes/partitions/ps/regions counts, per-disk online, inflight ops
curl -s http://127.0.0.1:9701/metrics   # PS: per-partition requests_total (monotonic), size/gc-debt/pending-compaction bytes, gc/compact inflight, sealed log extents
curl -s http://127.0.0.1:9601/metrics   # EN: append batches/bytes/ns totals, extents per shard + total, per-disk online
```

PS latency histograms (LAT-1): `autumn_ps_write_duration_seconds` (group-commit end-to-end across ALL write ops — Put/Delete —
observed per batched op from the already-measured WriteLoopMetrics
— zero added hot-path timing) and `autumn_ps_get_duration_seconds` (inline
serve incl. VP resolve) — Prometheus histogram exposition per partition,
buckets 0.5ms..250ms. A/B perf-checked (4K, p8, d8): no write regression.

Manual verify: write a few keys with `autumn-client put`, then confirm
`autumn_ps_partition_requests_total` increments on the owning partition and
`autumn_en_append_bytes_total` grows. Notes: all snapshots/gauges refresh
every 2 s (PS/manager publisher task; EN per-shard refresh loop); PS
`requests_total` resets on PS restart (normal Prometheus counter semantics
— use `rate()`).

### Disk-full (ENOSPC) behavior

A capacity error (ENOSPC/EDQUOT) on any EN write marks the disk **Full**,
distinct from **Faulted** (any other I/O error, permanent until restart):
a Full disk keeps serving reads and existing extents but hosts no NEW
extents, and **self-heals** back to Online within ~2 s of free space
returning above 5% of the disk (GC or operator cleanup — no process
restart needed). Watch `autumn_en_disk_full{disk_id=...}` on the EN
`/metrics` endpoint. The manager additionally soft-avoids allocating onto
nodes whose best disk has < `--min-alloc-free-bytes` free (default
256 MiB; 0 disables; cluster.sh env `AUTUMN_MGR_MIN_ALLOC_FREE_BYTES`).

E2E test (root, loop mounts): `./scripts/enospc_chaos.sh` — EN1 on a
512 MB loopback ext4 fills under live 1 MB puts; asserts Full-not-Faulted
classification, write failover to the other ENs, 2 s self-heal after
space frees, and byte-exact readback of every ACKed key. This harness
caught a real silent-corruption bug on its first pass: the batched append
used a raw `pwritev` and treated a SHORT write (the POSIX behavior when
some bytes fit) as success — a partial value was ACKed and read back
zero-padded. Fixed with the write-all form; the invariant is documented
in `crates/stream/CLAUDE.md` note 25a.

### WAL replay self-heal (log_stream bit-rot / truncated replica)

Partition open replays `log_stream`. If a sealed extent's serving replica
returns a **corrupt** record (per-record CRC / length mismatch) or a
**truncated** committed window (short read on a record boundary), recovery no
longer fails-and-wedges: it re-reads the SAME committed window from the other
*eligible* replicas, continues replay from the first that decodes clean, and
reports the bad replica(s) to the manager — which clears their `avali` bit and
bumps the extent eversion (so every PS refetches and stops serving from them)
**before** the partition serves. Fully automatic, no operator action. Watch the
PS log for `WAL self-heal: ... recovered the window from a clean replica` and
`isolated corrupt log_stream replica(s) via the manager`. An **OPEN-tail**
content corruption is sealed-and-rolled first (`WAL self-heal A4: sealed-and-rolled
the corrupt OPEN log_stream tail`) — frozen at the committed length via the F227
probe, then isolated in the same pass like a sealed extent. Still fails the open
loud (data lives on a healthy replica → recover / retry) for: an all-replicas-bad
extent, or an open tail that is **truncated** below the committed prefix (sealing
there could drop acked data — a separate F227 edge). EC extents route shard repair
through recovery, not this path. End-to-end fault injection lives in
`scripts/selfheal_chaos.sh` (3-EN cluster, flip one byte of slot[0]'s extent
`.dat`, restart → assert self-heal + byte-exact reads incl. the corrupted-value
key + slot isolated; plus an all-replicas-corrupt fail-loud negative). That
harness caught a real read-path bug on its first run: the avali isolation filter
was wired only into the copy read path, so the two VP-value fast paths
(`read_value_into_pooled` ZC proxy + `extent_read_descriptor` client-direct)
still served the bit-rotted-but-isolated replica — now both filter
`eligible_replica_slots`.

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
# u64 offset widening — extents may exceed 4 GiB (default seal 16 GiB)
# PS flag: autumn-ps --max-extent-size-bytes N   (default 16 GiB, clamp [1,64] GiB)
# Manual check: into ONE partition, put-stream a > 4.3 GiB value (4 MiB chunks
#   accumulate in one log_stream extent so later chunk VPs sit at byte offset
#   > u32::MAX). `$AC get-stream --out F KEY` must byte-match (sha256) — this
#   reads chunks via the now-u64 `ReadBytesReq.offset`. Then kill -9 the PS,
#   restart, and `get-stream` again must match (recovery replays SST + WAL with
#   u64 offsets). EC: with 16 GiB extents a shard exceeds 4 GiB, served via
#   per-shard chunked reads (no `payload_len: u32` overflow). EC convert is
#   ALSO chunked (stripe-wise encode + offset-tagged WriteShard streaming):
#   peak RAM = (K+M)x64MiB regardless of extent size. Manual check: seal a
#   >1 GiB extent (writes roll it), `autumn-op set-stream-ec --stream S --ec
#   3+1` then `force-ec-convert --extent E`, confirm the EN logs "phase 1
#   (prepare) complete ... (chunked)" + "phase 2 (commit) complete", then
#   `get-stream` the value back -> sha256 must match (chunk-encoded shards are
#   byte-identical to a whole-extent encode). Override stripe size with
#   AUTUMN_EXTENT_EC_STRIPE_BYTES on the EN to force many stripes on a smaller
#   extent. Repro script: the isolated memory-mode loopback recipe (manager
#   w/o --etcd, 4 single-shard ENs, 1 PS) used in dev.

# Admin / observability
$AO info                                 # nodes / extents / streams / partitions
$AO bootstrap --replication 3+0 --presplit 8:hexstring
$AO split PART_ID
$AO merge SURVIVOR_PART_ID VICTIM_PART_ID
$AO compact PART_ID
$AO gc PART_ID --ratio 0.4
$AO policy-candidates                    # advisory engine output (split/merge/gc/compact/EC)

# Extent refcount audit (MERGE-REFS-LEAK): cross-check every extent in etcd
# against live stream membership. Reports orphan extents (refs>0 but in 0
# streams -> invisible to `info`, never reclaimed), refs-vs-membership
# mismatches, and duplicate-in-one-stream listings. Exit code 1 if any found.
# NOTE: a flagged extent may still hold live data via vp_table_refs>0 — confirm
# that's 0 (major-compact the owning partitions first) before deleting files.
python3 python/audit_extent_refs.py     # --manager / --etcd / --op / --etcdctl overridable
# Repair a leaked refs value (refs != stream membership) — STOP the manager first
# (e.g. cluster.sh stop-manager), patch etcd, then restart so it replays the fix:
python3 python/patch_extent_refs.py 10:0 33:1            # dry-run (EID:new_refs)
python3 python/patch_extent_refs.py 10:0 33:1 --apply   # backs up to /tmp before writing

# Cluster lifecycle helpers
./start.sh                               # this repo: 1-host 5-EN dev cluster
./stop.sh --wipe                         # tear down + wipe
./cluster.sh start 3                     # richer driver: 3-replica cluster + auto-EC
```

## Chaos

```bash
# PS-failover chaos (2 PSes, kill one -> partitions must migrate, zero loss):
cargo test -p autumn-manager --test system_ps_failover_chaos -- --ignored
# Transport-layer chaos (real cluster.sh cluster; E1 EN kill+respawn, E2 PS
# kill -> migrate, E3 PS respawn, E4 manager kill+respawn (F265), E5 PS +
# manager double-kill inside the eviction window -> the interrupted eviction
# must converge and partitions FAIL BACK to the survivor (F265); every ACKed
# write verified afterwards):
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/transport_chaos.sh tcp
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/transport_chaos.sh ucx   # needs --features autumn-server/ucx binaries
# (ucx note: a node killed -9 leaves its port in TIME_WAIT ~60s; the UCX
#  listener now retries bind through that window instead of exiting.)
# E6: CHAOS_ROUNDS=N CHAOS_SEED=S randomized repeated kill rounds (F266).
# E7: split + mid-flight PS kill; merge + mid-freeze manager kill (F268).
# Kvcache-interface chaos (F275): python L3 backend under PS/manager kill
#   (NOTE: rebuild the wheel after ANY rust wire change — maturin build
#    --release + pip reinstall; a stale wheel mis-encodes requests):
#   ./scripts/kvcache_chaos.sh
# Fuse-interface chaos (F273): file workload through the mount under
#   PS-kill / manager-kill / fuse-kill+remount:  ./scripts/fuse_chaos.sh
# Cross-host chaos (F272, real network ::14+::15, remote via ssh):
#   ./scripts/crosshost_chaos.sh tcp | ucx
# Multi-manager HA chaos (F267): leader kill -> standby takeover, PS kill under
# the new leader, old leader rejoins as follower; zero ACKed-write loss:
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh tcp
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh ucx
# (F265 notes: manager restart used to black-hole client routing — part_addrs
#  is in-memory; the PS now re-reports it every ~2s sync tick. Ownership
#  failback used to wedge forever — owner_epoch now bumps on every acquire.)
```

## Rolling restart (R0 of docs/rolling_upgrade_design.md)

Same-binary rolling restart of a live cluster — one process at a time, a
convergence gate + per-partition write-liveness probe between every step,
fail-stop on the first gate that doesn't converge. Order: EN one-by-one →
PS → manager (most-depended-on end first, design §6).

```bash
# cluster must already be running (any cluster.sh start/reset shape)
bash scripts/rolling_restart.sh
# knobs: ROLL_GATE_TIMEOUT (180s), ROLL_HB_FRESH_SECS (10), ROLL_LIVENESS_TRIES (30)
# pass the same AUTUMN_DATA_ROOT / AUTUMN_TRANSPORT the cluster was started with
```

Manual verification:

```bash
bash cluster.sh reset 3                       # or: AUTUMN_BOOTSTRAP_PRESPLIT=4:hexstring bash cluster.sh reset 3
bash scripts/rolling_restart.sh               # expect: ... ROLLING RESTART COMPLETE ... zero loss
```

What it asserts per step: EN back `Online` with fresh heartbeat + recovery
drained (`recovery-stats` 0 inflight / 0 backoff); PS has every partition
routed (`info` shows no `ps=unknown`; the authoritative per-partition gate is
the liveness probe — one provably-in-range key per partition); manager answers
`info` again (leader re-elected from etcd replay) with all nodes Online.
Before the roll it seeds one 1 KiB key per partition + a 12 MiB striped value;
after the roll all are content-verified (zero ACKed loss). Probe keys are
namespaced `<range-prefix>__autumn-roll-<runid>-*` and deleted on exit; a
flock on `$AUTUMN_DATA_ROOT/rolling_restart.lock` rejects concurrent rolls.
Verified 2026-06-12 on a 3-EN/4-partition cluster under continuous external
writes: 191/191 ACKed keys survived.

`cluster.sh` grew the missing manager per-process subcommands for this:
`start-manager` / `stop-manager` / `restart-manager` (etcd state replay makes
a manager bounce a safe rolling step).

### cluster_version + wire-version interval (R1)

R1 lays the version-skew foundation: every binary carries a wire-version
interval `[WIRE_VERSION_MIN, WIRE_VERSION_MAX]` (crates/rpc), the startup
check accepts interval overlap instead of WIRE-1's fingerprint equality, and
the manager persists an operator-bumped `cluster_version` in etcd (ASCII
decimal at `autumn-rs/cluster_version`) that gates when new wire/persisted
formats may be emitted.

```bash
autumn-op cluster-version            # current gate + manager/op wire intervals
autumn-op upgrade-version [--to N]   # bump (default current+1) — run ONLY after
                                     # EVERY member runs the new binary; not rollbackable
```

Manual verification (all on a fresh `cluster.sh reset 3`):

```bash
autumn-op cluster-version            # expect: cluster_version: 1, intervals [1,1]
autumn-op upgrade-version            # expect REFUSED: 2 exceeds WIRE_VERSION_MAX=1
bash cluster.sh restart-manager && sleep 10
autumn-op cluster-version            # expect: still 1 (etcd replay)
# mixed-version refusal: any pre-R1 binary against this manager fails its
# startup check loudly ("decode GetClusterIdResp failed ... wire-schema mismatch")
```

Bump discipline lives in `crates/rpc/src/lib.rs` (`WIRE_VERSION_FINGERPRINTS`
registry): any wire-schema edit fails `cargo test -p autumn-rpc` until you
record the new fingerprint and consciously decide MIN/MAX. Rolling back a
binary past a `cluster_version` bump is refused at manager startup
(fail-closed in replay).

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

# BUG-LEASE-2 storage fencing (needs built binaries; boots a cluster):
# Phase 1 — stale-epoch MSG_PUT rejected with CODE_FENCED; anonymous
# (inode_hint=0) writes bypass.
cargo test -p autumn-manager --test bug_lease_2_storage_fencing -- --ignored
# Phase 2 — the floor SURVIVES a PS kill -9 + restart, on both recovery
# paths (WAL OP_FENCE_BUMP replay; TableLocations.fence_floors checkpoint
# after a flush), and MSG_PUT_ZC (the fuse/ioring large-write path) is
# fenced too. Manual check: write at epoch 1 then 5 for one ino, kill
# the PS, restart, retry epoch 1 → must get CODE_FENCED.
cargo test -p autumn-manager --test bug_lease_2_phase2_persistence -- --ignored
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
