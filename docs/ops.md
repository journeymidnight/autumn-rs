# autumn-rs — Operations & Manual Verification Manual

This is the operator/developer runbook: per-feature **manual verification steps**
(kept executable — repo rule: every feature keeps its manual-verify steps alive
here), observability, chaos suites, and CLI reference. For the user-facing intro
see [`README.md`](../README.md); for architecture see [`CLAUDE.md`](../CLAUDE.md)
and the per-crate `crates/*/CLAUDE.md`.

- [Binaries & ports](#binaries--ports)
- [Fuse daemon runbook](#fuse-daemon-runbook)
- [Cluster capacity — `autumn-op df`](#cluster-capacity--autumn-op-df)
- [Prometheus /metrics](#prometheus-metrics)
- [Disk-full (ENOSPC) behavior](#disk-full-enospc-behavior)
- [WAL replay self-heal](#wal-replay-self-heal-log_stream-bit-rot--truncated-replica)
- [Read route-around for Suspected nodes](#read-route-around-for-suspected-nodes)
- [autumn-memory verification](#autumn-memory-verification)
- [Retrieval-quality eval (`memory-mcp --eval`)](#retrieval-quality-eval-memory-mcp---eval)
- [Data-plane authz setup](#data-plane-authz-setup)
- [CLI cheatsheet](#cli-cheatsheet)
- [Chaos suites](#chaos-suites)
- [Rolling restart & upgrade versioning](#rolling-restart--upgrade-versioning)
- [Test matrix](#test-matrix)
- [Inode-lease + close-to-open coherence (in flight)](#inode-lease--close-to-open-coherence-in-flight)

## Binaries & ports

| Binary | Default port | Role |
|---|---|---|
| `autumn-manager-server` | 9001 | Control plane (streams, partitions, recovery) |
| `autumn-extent-node` | 9101+ | Data plane (raw extent files on disk) |
| `autumn-ps` | 9201 binary default; deployments use 9301 (+ per-partition) | LSM partition server |
| `autumn-client` | — | Data-plane CLI (put/get/del/head/ls/perf-check) |
| `autumn-op` | — | Admin CLI (bootstrap/split/merge/compact/gc/info/df/format) |
| `autumn-stream-cli` | — | Low-level stream debugging |
| `autumn-fuse` | — | FUSE mount of the `fs/` namespace (entrypoint role `fuse`) |
| `autumn-dashboard` | 8799 | Standalone web UI (drives the cluster via `autumn-op`) |
| `memory-mcp` | 5100 (HTTP mode) | autumn-memory retrieval; `--mcp` = stdio MCP server |

All of the above ship in the container image (`deploy/docker/Dockerfile`);
`entrypoint.sh` dispatches `manager|extent-node|ps|bootstrap|fuse`, and anything
else is exec'd verbatim, so `autumn-dashboard` / `memory-mcp` / the CLIs run as
plain commands.

`autumn-client --help` / `autumn-op --help` lists subcommands. (The standalone
Python `python/dashboard/` was retired 2026-07-04 — folded into the manager; see
"Web dashboard + auto-policy controller" below.)

## Async ops (op-ledger)

The seven long-running ops — `split` / `merge` / `rebalance` / `compact` / `gc` /
`forcegc` / `force-ec-convert` — are **asynchronous and uniform**: `autumn-op`
submits each to the leader's op-ledger and prints an `op_id` immediately instead
of blocking. This recovers the failure reason that the old fire-and-forget
`compact`/`gc` dropped — every op is queryable, including its error.

```bash
autumn-op compact 7                         # → submitted compact op <ID>
autumn-op ops status <ID>                   # pending|running|succeeded|failed|unknown (+ error/message)
autumn-op ops list --active                 # everything still in flight
autumn-op ops list --kind gc --limit 20     # recent gc ops
autumn-op gc 7 --wait --timeout 300         # block until terminal; non-zero exit on failure
```

- **`--wait [--timeout SECS]`** (global, default 600) blocks until the op reaches
  a terminal state and exits on its real outcome — for scripts (and `presplit`
  internally) that need the blocking error. Without it, poll `ops status`.
- **Where outcomes come from**: split/merge/rebalance close in-process on the
  leader; compact/gc/forcegc run on the PS and report their terminal outcome +
  error back on the 5 s load heartbeat (so terminal state appears within
  ~5–10 s); ec-convert closes when the conversion applies.
- **Auto-dispatched ops are tracked too.** Extent **recovery** (replica rebuild)
  is never submitted by an operator — it appears in the ledger on its own when
  the recovery loop dispatches it:
  ```bash
  autumn-op ops list --kind recovery
  # op 1170…  recovery  running   target=0->12 attempts=3  ERROR[3]: all recovery candidates rejected
  # op 1170…  recovery  succeeded target=0->10 attempts=1  recovered slot onto node 1
  ```
  A recovery **stays `running` while it retries** (the loop backs off but never
  gives up), carrying `attempts` and the **last** failure reason + `error_code` —
  so a repair that is looping instead of converging is visible per-extent, not
  just in aggregate `recovery-stats`.
- **Failover honesty**: the live ledger is leader-local (in-memory, cap 256).
  After a leader change, `ops status <old-id>` answers `unknown` (never a false
  `running`); durable terminal history is in `autumn-op audit-log`. A PS-executed
  op whose outcome never arrives flips to `unknown` after 30 min.
- **Dedup**: re-submitting the same target while one is in flight returns the
  existing `op_id` ("attached") rather than double-dispatching.

## Auto-policy controller (in the manager)

The manager only *emits* advisories (pure mechanism); the leader-fenced
**auto-policy controller** *decides + actuates* per an active policy. It runs
in-process — one crash-safe, leader-owned task (it survives as long as the leader
does). It is **leader-only** (never runs on a follower) and a state machine
`Off → DryRun → Armed`. `Armed` actuates; `DryRun` logs "would: …" but never
mutates — the **mode is the whole gate** (arming is per-policy; there is no
separate process-wide flag). Config is persisted to etcd (`autoPolicy/config` +
`autoPolicy/cooldowns`, leader-fenced) so the active policy survives leader
failover.

**Boot default.** The DEPLOY layer (entrypoint / autumn-deploy / k8s) seeds
`--auto-policy-default balanced`, which is seeded **Armed** — so a production
cluster boots running the `balanced` policy (GC + compaction + EC + region
rebalance — no split/merge) and actuates on its own, no extra flag. The seed
fires only on a FRESH cluster (no persisted `autoPolicy/config`) and is in-memory,
so the first operator change persists over it and a `deactivate` survives failover
(never re-seeded). `AUTUMN_AUTO_POLICY_DEFAULT=<preset|off>` changes or disables
it. **cluster.sh / chaos / perf leave it OFF** (they never set the env), so
dev/test behaviour is unchanged. Headless control:

```bash
autumn-op auto-policy status                 # mode + active + presets + action log
autumn-op auto-policy activate gc-only       # select + DryRun (observe, no actuation)
autumn-op auto-policy activate aggressive --arm   # select + Armed (actuate)
autumn-op auto-policy deactivate             # mode → Off
```

Presets (safest → most aggressive): `gc-only`, `maintenance`, `space-reclaim`,
`balanced`, `aggressive`.

## Web dashboard (standalone app)

The manager **no longer serves a web UI**. The dashboard is a standalone app,
`examples/dashboard` (the `autumn-dashboard` binary), which holds no cluster
state and drives the cluster ONLY through `autumn-op` (so the wire schema stays
in one place). It is **token-gated** — the admin token is required and forwarded
to every `autumn-op` call; read-only views ignore it, mutations (per-target Apply
buttons + auto-policy activate/deactivate) use it.

```bash
# autumn-op must be on PATH (or pass --autumn-op /path/to/autumn-op).
autumn-dashboard \
  --manager 127.0.0.1:9001 \
  --admin-token-file /etc/autumn/admin.token \
  --port 8799                        # → http://<host>:8799/

# k8s (vke overlay ships it as its own Deployment + internal ClusterIP):
kubectl -n autumn port-forward svc/autumn-dashboard 8799:8799   # → http://localhost:8799/
```

The page shows cluster capacity, node health, the PS→partition→extent hierarchy,
policy advisories, and per-target action buttons. Manual actions map to the
allow-listed `autumn-op` subcommands (`split` / `gc` / `compact` / `merge` /
`force-ec-convert` / `rebalance`).

**Auto-rebalance switch (Phase B).** A 6th policy switch,
`rebalance`, arms the automatic version of `autumn-op rebalance` (see "Rebalancing
region→PS assignment" below). When enabled + Armed, the controller emits a
cluster-level advisory whenever the per-PS partition-count spread exceeds
`rebalance_gap_threshold` (default 2) and actuates it by moving a bounded batch
(`rebalance_max_moves_per_tick`, default 4) per tick — gradual convergence, not a
storm. It is OFF in the conservative presets, ON in `balanced` + `aggressive`.
Custom-policy switches (incl. `rebalance`) are persisted in `autoPolicy/config`
(rkyv), so they survive leader failover; a pre-Phase-B config decodes unchanged
(the `switches` Vec is variable-length — an absent 6th switch reads as off).
The advisory THRESHOLDS live in the in-memory `PolicyConfig` (compiled defaults +
runtime override), like every other advisory threshold — not persisted.

**Verify leader-failover of the active policy** (the crash-safety guarantee):

```bash
# with an etcd-backed cluster:
autumn-op auto-policy activate gc-only --arm       # → mode=armed active=gc-only
kill -9 <leader-manager-pid>                        # crash the leader
# after the etcd lease expires (~10 s) a new leader wins + replays from etcd:
autumn-op auto-policy status                         # → STILL mode=armed active=gc-only
```

**Security posture (documented non-goal):** the dashboard app's HTTP port has no
per-request auth/TLS (same as the manager's `--metrics-port`). The admin token
gates *mutations* against the manager, not access to the page — so pair a
network-reachable dashboard with network ACLs (or bind `--listen 127.0.0.1` +
tunnel). On k8s it is an internal ClusterIP, reachable only via port-forward.

## Fuse daemon runbook

autumn-fuse is a **consumer** — a POSIX filesystem client that runs on the
application node and talks to a *running* cluster's manager; it is not part of
cluster deployment. Start it directly:

```bash
cargo build --release -p autumn-fuse        # add --features ucx for a UCX cluster
MP=/mnt/autumn
mkdir -p "$MP"

# --transport MUST match the cluster's transport: the fuse daemon is a data-plane
# client (process-global), so a tcp fuse cannot reach a ucx cluster.
nohup ./target/release/autumn-fuse \
    --manager 127.0.0.1:9001 \
    --mountpoint "$MP" \
    --transport tcp \
    > /tmp/autumn-fuse.log 2>&1 &

# Verify it actually mounted — a bad --manager / transport mismatch makes the
# daemon exit within ~1 s, and without this check you'd think it succeeded.
sleep 1
mountpoint -q "$MP" && echo "mounted" || { echo "FAILED — see log:"; tail -20 /tmp/autumn-fuse.log; }

ls "$MP"; echo hi > "$MP"/x; cat "$MP"/x      # → hi
fusermount3 -u "$MP"                          # unmount (needs the `fuse3` package)
```

If a previous daemon died it can leave a stale mount (`ls` reports "Transport
endpoint is not connected"); clear it before re-mounting with
`fusermount3 -u "$MP"` (or `umount -l "$MP"`).

**The mount is scoped to the WHOLE `fs/` namespace.**
`autumn-fuse` (and `autumnfs`, and the PyO3 `autumn.Fs.connect(...)`) has NO
`--tenant` — every inode/dirent/extent key lands under `fs/…` (one global tree).
A fuse mount, `autumnfs`, and the PyO3 client **all see the SAME filesystem**. To
run isolated filesystems in one cluster use DISTINCT NAMESPACES (`fsA`/`fsB`, each
`namespace-create`d), not a tenant. Inode numbers are cluster-unique (a single
global counter); a `schema_version` stamp makes a future incompatible layout fail
loud rather than mount empty. (Option 3, docs/key_namespace_split_design.md §8:
the tenant segment — and the short-lived SD-3 `{volume}` layer before it — were
removed; this is a stop-world data-reset change with a WIRE bump to v26.)

**UCX (RDMA):** with `--transport ucx`, export the UCX env before launching (the
UCX C library reads it directly): a positive `UCX_TLS` list — never `^` negation —
and a pinned RoCE device, e.g.

```bash
export UCX_TLS=rc_mlx5,ud_mlx5,tcp,self       # NEVER add posix/cma (2026-07-03: the posix
                                              # large-message path stalls concurrent >=64K
                                              # transfers — 3s timeout storms)
export UCX_NET_DEVICES=mlx5_1:1               # verify: scripts/check_roce.sh --listen-candidates
ulimit -l unlimited                           # ibv_reg_mr pins registered buffers
```

UCX_TLS rule (one rule, 2026-07-03): UCX clusters bind **RoCE NIC IPs**
(127.0.0.1 is not an RDMA device address) and use
`UCX_TLS=rc_mlx5,ud_mlx5,tcp,self` + a pinned `UCX_NET_DEVICES` — the single
list serves both intra-host (rc loopback in the HCA) and cross-host traffic.
`cluster.sh` / `autumn-deploy` apply this automatically for `TRANSPORT=ucx`
and refuse a loopback bind (legacy shm-only loopback needs an explicit
`UCX_TLS=posix,cma,tcp,self` and has ≥64K transfers known-broken — the
loopback chaos harnesses set it themselves). Explicit env always wins.

**In Kubernetes**, the shipped image carries `autumn-fuse` and the entrypoint
dispatches it as the `fuse` role, so a consumer pod mounts the `fs/` namespace
with a sidecar rather than a per-node DaemonSet (a DaemonSet works too, but a
sidecar keeps the mount's lifetime tied to the one workload that needs it):

```yaml
containers:
  - name: fuse
    image: <CR>/autumn-rs:<tag>
    args: ["fuse"]
    env:
      - { name: AUTUMN_FUSE_MOUNTPOINT, value: /mnt/autumn }
      - { name: AUTUMN_CREDENTIAL_FILE, value: /etc/autumn/cred/fs.cred }
    securityContext:
      privileged: true            # or capabilities.add:[SYS_ADMIN] + /dev/fuse device
    volumeMounts:
      - { name: mnt, mountPath: /mnt/autumn, mountPropagation: Bidirectional }
      - { name: cred, mountPath: /etc/autumn/cred, readOnly: true }
  - name: app
    volumeMounts:
      - { name: mnt, mountPath: /mnt/autumn, mountPropagation: HostToContainer }
volumes:
  - { name: mnt, emptyDir: {} }
```

The propagation pair is what makes the sidecar's mount visible to the app
container; without it the app sees an empty directory. The entrypoint clears a
stale mount (`fusermount3 -u`) before mounting, so a crashed daemon does not
wedge the next start. Env → flag: `AUTUMN_MANAGER`, `AUTUMN_FUSE_MOUNTPOINT`,
`AUTUMN_CREDENTIAL_FILE`, `AUTUMN_FUSE_DIRECT_READ`, `AUTUMN_FUSE_ALLOW_OTHER`.

⚠️ **The mount sets `FOPEN_DIRECT_IO` on every open** (`crates/fuse/src/ops.rs`),
so the kernel serves no page cache for it — and a **`MAP_SHARED` mmap of a file
on this mount fails with `ENODEV`** (the kernel refuses shared mappings on a
direct_io FUSE file). Readers that mmap must use `MAP_PRIVATE`, or read with
`pread`/`preadv`. This bites model loaders: a loader that falls back to
`mmap.mmap(fd, 0)` (Python's default is `MAP_SHARED`) will fail on this mount
even though plain reads work.

**`O_DIRECT` opens DO work** (measured 2026-09-01, VKE, kernel 5.15):

```bash
dd if=<file on mount> of=/dev/null bs=4096 count=1 iflag=direct   # OK
dd if=<file on mount> of=/dev/null bs=8M   count=1 iflag=direct   # OK
```

That matters for weight loaders that probe `O_DIRECT` once and fall back to
mmap on failure — the probe succeeds here, so they stay on the `preadv` path and
never reach the `MAP_SHARED` trap above. Verify it on a new kernel/topology
before relying on it; the two behaviours are independent (the FUSE `direct_io`
flag is about the page cache, `O_DIRECT` is about the opener's request).

### `--direct-read` — bypass the PS for large reads

Add `--direct-read` to the mount to make whole-extent reads (≥ 64 KiB) read
STRAIGHT from an extent node instead of proxying through the PS — a cross-host
throughput win for large-file / model serving (the PS NIC egress leaves the read
path). **Topology-dependent, default OFF**: the fuse host must be able to reach
EN *data* ports, which a hardened deploy often keeps on a PS-only subnet. It is
SAFE to enable even if some ENs are unreachable — every read falls back to the
PS proxy (one redirect RTT + fallback per extent), so correctness never depends
on it.

```bash
nohup ./target/release/autumn-fuse \
    --manager 127.0.0.1:9001 --mountpoint "$MP" --transport tcp \
    --direct-read \
    > /tmp/autumn-fuse.log 2>&1 &
sleep 1; mountpoint -q "$MP" && echo mounted   # log prints "direct-read enabled ..."

# Byte-identical vs proxy: write a >64 KiB file, read it back, diff.
head -c 5242880 /dev/urandom > /tmp/blob            # 5 MiB (multi-extent)
cp /tmp/blob "$MP"/blob
cmp /tmp/blob "$MP"/blob && echo "direct-read OK: byte-identical"
```

Verify the bypass actually engaged: with `--direct-read` a large read shows
`autumn_ps_read_bytes` on the PS staying flat (the value bytes don't traverse
the PS) while the EN's `MSG_READ_BYTES` traffic rises; without it the PS
read-bytes counter tracks the read. The same flag exists on every direct-read
frontend, all DEFAULT ON now (2026-07-09): fuse `--direct-read` (default true),
python `BatchClient(manager, ..., direct=True)`, `autumn.Fs.connect(...,
direct_read=True)`, kvcache `AutumnKVConnector` (`extra_config.direct_read`),
the `autumn-s3` gateway (`--direct-read`). Mixed-size batches route per
item — sub-64 KiB values still go through the PS; on a topology where ENs aren't
client-reachable each item falls back to the proxy and the client logs one WARN.

## Python `autumn.Fs` — shared inode-layout binding

`autumn.Fs` is a PyO3 binding over the **same** fuser-free FS core the
`autumn-fuse` mount runs on (inode/dirent/extent layout) — it's the programmatic
file surface (the `autumn-s3` gateway reads model weights through it). Headless
correctness (self-contained isolated memory-mode cluster — builds the wheel,
boots manager+EN+PS, drives the full `Fs` surface + a cross-instance byte-exact
check, tears down):

```bash
cargo build --workspace                    # debug binaries first
bash python/tests/run_fs_e2e.sh
#   → "PY M2 CROSS-INSTANCE byte-exact OK", "===== fs-e2e exit: 0 ====="

# M4 — lease fencing + cross-client coherence (two Fs clients):
bash python/tests/run_fs_lease_e2e.sh
#   → "PY M4 fencing OK", "PY M4 coherence OK", "===== fs-lease-e2e exit: 0 ====="
```

M4 write-fencing: `autumn.Fs` clients and a fuse mount both take the same
per-inode WRITE lease around writes (via `lease_tasks.rs`), so concurrent writers
to one inode conflict instead of corrupting each other; reads are close-to-open
coherent (fresh-read + `forget`-on-release). Behavior-preservation gate for the
`dispatch` Create/Unlink/init_root refactor + the M4 `lease_tasks` extraction
(the binding shares those core steps): the fuse e2e suite must stay green —
`cargo test -p autumn-manager --test system_fuse_read --test fuse_lease_1
--test fuse_lease_2 -- --ignored --test-threads=1`.

## Cluster capacity — `autumn-op df`

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

The same snapshot backs FUSE `statfs`: `df -h <mountpoint>` reflects real
backend capacity (conservatively, at the 3-replica factor) instead of a fixed
placeholder.

### Amplification in `df` = physical / footprint, not physical / sealed

`amplification` = `physical_used / (logical_stored_sealed + logical_open_tail)`
≈ the real replication/EC factor (~3× for 3-replica, lower with EC). The
denominator MUST include `logical_open_tail`: `physical_used` counts the
open-tail bytes (largely LIVE large-value / VP data in the open log tail — the
SST only holds pointers), so dividing by sealed-only inflates amp ~15× (a
3-replica cluster read 45× when a partition's data lived in open tails). The
human `df` prints the breakdown `logical: sealed=… + open_tail=… = footprint …`.
A high `amp` (>> replication factor) now genuinely means an EC/replication issue,
not just un-sealed data.

### WAL debt (dead large-value bytes) in `df`

`df` also prints `WAL debt: <bytes> dead (<pct>% of footprint, GC-reclaimable;
incl. open-tail)` and, in `--json`, `logical_wal_debt` + `wal_debt_ratio`. This is
the reclaimable garbage in `log_stream` — large values that were overwritten by a
newer version or deleted, still occupying replicas until GC punches them. It is
`Σ (sealed-extent dead + OPEN-tail dead)` across partitions:

- **sealed-extent dead** = each partition's `gc_debt_bytes` (already tracked).
- **open-tail dead** = the discard-map entry for the current OPEN log tail, which
  `gc_debt_bytes` excludes because GC can't punch an unsealed extent. Previously
  a log-heavy / all-open-tail partition (data entirely in one open log tail) showed
  `gc_debt = 0` and looked debt-free even when holding GBs of overwritten garbage;
  `df` now surfaces it.

Both are DERIVED each PS GC tick from the persisted SST discard maps (no bespoke
counter, no write-path cost) so they survive PS restart exactly like `gc_debt`.
Do NOT read `footprint − data` as debt — `size_bytes` is SST-only and excludes
live VP value bytes, so it would flag a healthy VP partition as ~all-debt. A high
`wal_debt_ratio` is the signal to run `compact` + `gc`/`forcegc` to reclaim.

### Per-partition size in `autumn-op info`

The cluster overview's per-partition size = the manager's authoritative
Σ `sealed_length` **plus** the PS-reported open-tail committed bytes (log + row +
meta open tails). Without the open-tail term a major-compacted or log-heavy
partition — whose data lives entirely in OPEN extents (manager `sealed_length` =
0) — renders `0 B` despite holding GBs. The open-tail bytes come from the PS's
5 s load report (refreshed by a throttled 30 s probe), so the overview is a
periodic rollup that can lag a live compaction by a few seconds:

```bash
autumn-op --manager 127.0.0.1:9001 info            # overview: size incl. open tails
autumn-op --manager 127.0.0.1:9001 info --part 17  # EXACT size (probes the EN live)
```

For an idle partition the two match to the byte; for one actively
GC/compacting they differ transiently — `info --part` is authoritative.

## Tuning `--max-extent-size-bytes` — reclamation granularity vs metadata

`autumn-ps --max-extent-size-bytes` (default **16 GiB**, clamp [1 GiB, 64 GiB])
is the tail-extent seal threshold. It is set per-PS and applies to **all three
streams** (log / row / meta) of every partition on that PS. It trades
**space-reclamation granularity** against **manager/etcd metadata pressure** —
bigger extents = fewer extents = less metadata, but coarser and more-delayed
space return to the EN disks.

Why the two streams react differently:

- **`log_stream`** (large values / VP records) reclaims via GC **`punch_holes`**,
  which is **per-extent** — GC relocates the still-live VPs off an extent, then
  frees *that specific* extent (not just the oldest). Coarser extents mean GC
  relocates more bytes per reclaim, but it can still target any sealed extent.
- **`row_stream`** (SSTables) reclaims **only** via `truncate`, a **prefix**
  operation: it frees the *oldest* extents, and only once **every** SST inside
  them has been compacted away (live data merged into newer SSTs). You cannot
  free a middle extent, and you cannot truncate the current tail. So a partition
  whose whole row_stream still fits in one 16 GiB extent returns **zero** SST
  space via truncate until that extent rolls — dead SST bytes accumulate up to
  ~one extent before any is reclaimed. A full 16 GiB extent holds ~128 × 128 MB
  SSTs; clearing it takes ~26 minor-compaction rounds (`COMPACT_N=5`/round) plus
  the matching write amplification.

Pick by workload:

| Workload | row_stream footprint | Recommendation |
|---|---|---|
| **Large-value** (fuse / model files / kvcache, most values > 4 KiB) | tiny — SSTs hold only VP pointers + small inline; data lives in log_stream (per-extent GC) | **keep 16 GiB** (or larger). row_stream space-amp is a non-issue; fewer extents wins. |
| **Small-value, high churn** (all values inline < 4 KiB, heavy overwrite/delete) | row_stream IS the data; dead SST bytes pile up | **lower to 1–4 GiB.** row_stream truncates far sooner; log_stream GC also gets finer-grained. Cost: more extents → more manager/etcd metadata + more append RPCs. |
| **Mixed / unsure** | — | leave the 16 GiB default; only lower if `autumn-op df` amplification or a partition's `info --part` shows row_stream disk held well above its live size for a sustained period. |

How to see whether it's biting you: `autumn-op df` reports the physical/logical
amplification; a single partition's held-vs-live gap shows in `autumn-op info
--part <ID>` (live size probes the EN). If a small-value partition's on-disk
row_stream sits far above its live SST bytes and stays there across several
compaction cycles, it is holding an un-truncatable extent's worth of dead SSTs —
lower `--max-extent-size-bytes` (whole-cluster restart to apply; it is a
per-process flag, not runtime-tunable). The `--admission-compact-rate-bytes-per-sec`
knob governs how fast compaction *does* that reclamation work, independently of
the granularity the extent size sets.

Note: this is a per-PS **restart** flag (no online change); changing it does not
rewrite existing extents — only new tail rolls use the new size, so the effect
phases in as old extents are compacted/truncated away.

## Prometheus /metrics

Every server binary takes an opt-in `--metrics-port <PORT>` flag exposing a
Prometheus text endpoint at `http://<listen-host>:<PORT>/metrics` (plain
`std::net` listener on its own OS thread — zero interaction with the
io_uring data plane; absent flag = no listener). `cluster.sh` wires all
three with `AUTUMN_METRICS=1` (manager `9591`, EN `960<i>`, PS `9701`);
the deploy paths accept the same env (autumn-deploy / k8s entrypoint).

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

## Disk-full (ENOSPC) behavior

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

## WAL replay self-heal (log_stream bit-rot / truncated replica)

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
the corrupt OPEN log_stream tail`) — frozen at the committed length via the lenient-seal
probe, then isolated in the same pass like a sealed extent. Still fails the open
loud (data lives on a healthy replica → recover / retry) for: an all-replicas-bad
extent, or an open tail that is **truncated** below the committed prefix (sealing
there could drop acked data — a separate lenient-seal edge; **the seal must be
lenient**: the seal path accepts a lenient/committed-length freeze rather than
demanding byte-perfect tails). EC extents route shard repair
through recovery, not this path. End-to-end fault injection lives in
`scripts/selfheal_chaos.sh` (3-EN cluster, flip one byte of slot[0]'s extent
`.dat`, restart → assert self-heal + byte-exact reads incl. the corrupted-value
key + slot isolated; plus an all-replicas-corrupt fail-loud negative). That
harness caught a real read-path bug on its first run: the avali isolation filter
was wired only into the copy read path, so the two VP-value fast paths
(`read_value_into_pooled` bulk proxy + `extent_read_descriptor` client-direct)
still served the bit-rotted-but-isolated replica — now both filter
`eligible_replica_slots`. Design: `docs/wal_selfheal_design.md`.

### Compaction never strands un-flushed writes past the replay-start

Each SSTable records a `vp_head` = the `log_stream` position recovery replays
FROM. A major compaction rewrites every SSTable, so whatever `vp_head` it stamps
becomes the whole partition's replay-start after the next restart. It stamps the
**MAX over the input SSTs' vp_heads** (the newest input's content boundary), NOT
the live write cursor — the cursor sits PAST writes that are acked + durable in
`log_stream` but still only in the active memtable (un-flushed), and stamping it
would drop those writes out of the replay window (silent loss on a crash between
the compaction and the next flush). MAX keeps the replay-start behind the
un-flushed tail while still advancing it past the fully-merged log region so GC
can reclaim there. No operator action; automatic. Regression:
`crates/manager/tests/system_compact_unflushed_vp_head.rs` (writes A→flush,
B→flush, C→NO flush, major-compact, crash, reopen → all of A/B/C must read back).
The MAX above is correct only because each SST's `vp_head` is now its true
content boundary: a flush stamps the position captured when the memtable was
FROZEN (`rotate_active`), not the live cursor at flush-claim (which foreground
writes could push ahead of that SST's content — a flush-race that stranded the
un-flushed tail before crash). Regression:
`crates/manager/tests/system_flush_race_vp_head.rs`. And on RESTART, recovery
seeds the write cursor `p.vp` to the committed log TAIL (not the replay start),
so the recovered active memtable also rotates with a forward boundary and the GC
floor advances for an idle-restarted partition — closing the "compact-then-GC
still won't reclaim" case. Guard:
`crates/manager/tests/system_recovery_vp_seed.rs`. The vp_head is now a true
content boundary on every path (flush, compaction, and recovery).

### Reading GC replay-floor protection — a skipped `forcegc` is usually CORRECT

GC protects any NON-EMPTY `log_stream` extent that sits AT/BEFORE the recovery
replay floor (`MIN` over every live SST's `vp_head` position). If you `forcegc`
such an extent it is refused — this is the replay-floor safety guard, **not a bug**: recovery
replays the log from `floor_extent` forward, so punching it could drop un-flushed
writes. How to tell CORRECT-protection from a real problem:

- **The PS log** now names it: `GC: protected extent(s) ... part_id=P
  protected=[E] floor_extent=F floor_pos=N pinned_by_sst_vp_extent=S` — the
  extent recovery replays FROM is `F`, pinned by SST whose vp_head is `S`.
- **`autumn-op info --part P`** shows `replay_floor = extent F (pos N)`, the
  `vp_seed(tail)`, and each SST's `vp_head` (the one that `← pins floor` is the
  lagging SST). If `floor_extent == the extent you tried to forcegc`, that extent
  IS the replay start — protection is correct.
- **`autumn-op forcegc P E`** returns a synchronous advisory when `E` is inside
  the replay window (which extents, and why), instead of you having to grep the PS
  log.

To actually reclaim a protected extent, **advance the floor**: run a MAJOR
compaction (`autumn-op compact P`) so every live SST's `vp_head` moves past that
extent (a lagging CoW-shared SST from a split is the usual cause), then re-issue
`forcegc`. If the floor still won't pass it, the partition genuinely still needs
that extent for replay (its data was all flushed while that extent was the log
tail) — nothing to reclaim until newer data supersedes it.

### Recovery is BOUNDED and reopens in parallel

A partition's reopen time is bounded by the un-flushed **log** window, NOT the
dataset size — if a full-takeover reopen (all a dead PS's partitions land on one
survivor) is slow, that's a symptom to investigate, not "the dataset is just big".
Three properties enforce this (2026-07-13):

- **Bounded replay window (BUG1).** The `MAX_WAL_GAP` (1 GiB default) force-rotate now
  measures the un-flushed **log bytes** (value included), not the memtable
  footprint. Before the fix, a large-value (VP) workload kept only ~24-byte
  pointers in the memtable, so the gap never tripped and the log_stream replay
  window grew with the dataset. If reopen replay is still large, check
  `autumn-op info --part P` `vp_seed(tail)` vs `replay_floor` spread — a wide
  spread means flushes are lagging (slow P-sst / row_stream), not a recovery bug.
- **Parallel reopen (BUG3).** `sync_regions_once` opens up to 64 partitions
  concurrently (each recovers on its own OS thread/core). A 32-partition takeover
  recovers in ~single-partition time, not ×32. In the PS log you'll see all
  `opening partition P` lines close together, then `partition P opened` as each
  finishes — interleaved, not strictly sequential.
- **Tighter GC reclaim (BUG2).** GC may now raise its replay floor to the newest
  **durably-ACKed flush checkpoint** vp (not just the MIN over all live SSTs'
  vp_heads), so the fully-flushed prefix `[oldest-SST-vp, newest-flush-vp)` is
  reclaimable without waiting for a major compaction to advance every SST's
  vp_head. The recovery replay-start is UNCHANGED (safe by design — the recovery
  code was deliberately not touched); it self-tightens once GC punches the covered
  prefix. `autumn-op info --part P` still shows the conservative MIN `replay_floor`
  (display-only); the effective GC floor can be higher. **No operator action** —
  this just means less lingering log debt on write-heavy partitions between major
  compactions.

### GC auto-reclaims empty sealed log extents from split/merge churn

Frequent split/merge mints **empty sealed** `log_stream` tail extents
(`sealed_length == 0`). These are free to reclaim (`punch_holes`, no data
movement) but used to STARVE under Auto GC: candidates sort by reclaimable-bytes
DESC (empties last) and shared the 3-per-tick rewrite budget with big candidates.
Auto GC now gives empties a separate, larger per-tick budget (`MAX_GC_EMPTY_ONCE
= 32`), so they drain on their own within a GC tick or two — no operator action.
If you see empty sealed log extents lingering (`autumn-op info --part P` → a
`role:log, open:false, size:0` extent that is NOT the tail), a manual `autumn-op
forcegc P <extent>` still punches it immediately. NOTE: a split/merge-sealed empty
can occasionally be stale-cached-as-open on the PS and skipped until its cache
refreshes (a read / restart) — a `forcegc` that logs "not authoritatively sealed
yet" is that case; re-issue after a moment.

## Read route-around for Suspected nodes

When the manager marks an EN **Suspected** (df heartbeats lapsed past the soft
timeout, ~10 s), the READ path proactively avoids it — not just allocation. For
**replicated** extents the client tries healthy replicas first and only falls
back to the suspected one if every healthy replica fails (suspected ≠ dead, and a
sealed extent's committed bytes are on every replica). For **EC** extents a
suspected data shard is reconstructed straight from parity (read K healthy shards
+ parity) instead of issuing a doomed shard read and waiting for it to time out.
This is a soft latency optimization layered on the existing failover — correctness
never depends on it, so a stale view only costs a little extra latency/parity
traffic, never data.

No new config or wire types: the client polls the existing
`autumn-op list-node-states` data (`MSG_LIST_NODE_STATES`) in the background,
TTL-gated at 2 s and never on the read's critical path. Because the refresh is
non-blocking, the avoidance is a **steady-state, self-healing** optimization, not
a per-read guarantee: the very first read after a node flips to `Suspected` (e.g.
on a previously-idle client) uses the current snapshot and only *kicks* the
refresh, so that one read can still pay a single timeout if it lands on the flaky
node — every read after the ~2 s refresh routes around it. This never regresses
the pre-existing reactive failover; it just removes the repeated per-read timeout
under sustained load. **Manual check:** on a 3-EN replicated cluster, `kill` one
EN; after the manager flips it to `Suspected` (`autumn-op info` /
`list-node-states`) and a couple seconds of read traffic, `get` of keys whose
extent has a replica on the dead node is served by a healthy replica instead of
stalling for the per-RPC timeout on every read.

## Stale owner-epoch fence self-heal (BUG-MGR-RETRY-CLASS)

A PS partition whose stream client holds a stale per-partition `owner_epoch`
(classic cause: a rebalance moved the partition and the old holder kept
serving, or any newer `acquire_owner_lock` on the same `partition/<id>` key)
is rejected by the manager with `CODE_PRECONDITION`
("owner_key=partition/N owner_epoch mismatch, expected X, got Y") on every
`alloc_new_extent`. Pre-fix symptoms: writes to ONE partition take ~15 s each
(20×500 ms futile manager retries + open overhead; `autumnfs put` = 45 s for
3 keys), reads stay fast, PS log shows the same `got Y` number forever.

Post-fix behavior (what to verify):
1. The first fenced manager call FAILS FAST (log: `"... got a deterministic
   manager error, failing fast"` + `"stream_alloc_extent fenced
   (LockedByOther): ..."`) — no 20-retry storm.
2. The PS poisons the partition (`"... fenced (LockedByOther) —
   poisoning partition for fresh-epoch reopen"` or `"LockedByOther detected,
   poisoning partition"`), its thread exits.
3. Within one region-sync tick (~2 s) the PS logs
   `"partition <id> thread exited (fence poison or crash) — dropping handle;
   region map decides reopen-with-fresh-epoch vs release"`, then either
   reopens it (still assigned here → fresh epoch, writes succeed) or leaves
   it closed (rebalanced away → the new owner serves it).

**Manual check** (any cluster): find a partition's owner key epoch, bump it
behind the PS's back, then write through it:
```bash
# bump the epoch for partition/17 behind the serving PS's back (manager CLI
# acquires the same owner lock the PS holds):
autumn-stream-cli --manager <mgr:9001> acquire-owner-lock partition/17   # if unavailable,
# any partition move (autumn-op rebalance 1) exercises the same path on the OLD PS.
# then:
time autumn-client --manager <mgr:9001> put <key-in-that-partition> v
# expect: first write may error/redirect once; within ~2-4 s writes to that
# partition succeed at normal latency (NOT 15 s each / NOT stuck forever).
# PS log shows the three-step sequence above, and the "got <epoch>" number
# CHANGES after the reopen (fresh epoch) instead of repeating.
```

## Node decommission runbook (fence → drain → remove)

Retiring an EN is operator-driven (HDFS-decommission style). The manager never
auto-removes a node; you fence it, the system drains it, `remove` gates on the
drain being complete.

```bash
AO=(./target/release/autumn-op --manager 127.0.0.1:9001)

"${AO[@]}" fence-node 56 --reason "retiring" --by you   # 1. fence
"${AO[@]}" info                                          # 2. watch shard count → 0
"${AO[@]}" remove 56 --by you                            # 3. remove (server-side gated)
```

What fencing triggers (all automatic):

- **No new data**: Fenced (and Maintenance / auto-Suspected) nodes are
  hard-excluded from every placement path — new extents, fallback walks,
  recovery targets, EC parity. Unlike soft excludes this is never backfilled;
  a cluster left with fewer eligible nodes than the replica count refuses
  allocation loudly rather than placing data on a draining node.
  (Availability note: a 3-EN RF-3 cluster with one *Suspected* node blocks new
  extent allocation until it heals — seconds — or is fenced.)
- **Sealed extents**: the recovery loop (`fenced_only` gate, default) rebuilds
  every sealed extent's fenced slots onto healthy nodes. Includes sealed-EMPTY
  extents (0-byte membership swap).
- **Open tails**: recovery only rebuilds sealed extents, so the manager's drain
  sweep (every 2 s tick, 30 s per-partition cooldown) asks the owning PS to
  seal + roll any OPEN tail with a fenced replica (`MSG_ROLL_TAILS`). On a
  SERVING partition the roll quiesces the live stream writer first (SealCommit
  handshake) and seals at its exact all-replica-acked commit, then redirects
  the writer onto a fresh tail on healthy nodes — so a busy partition drains
  without losing acked writes (a bare probe-seal behind a live writer was the
  cause of the split-child `stale_vp_offset_past_sealed_length` wedge / silent
  stale-read family; regression `system_roll_tails_live_writer`). With no live
  writer it seals by lenient probe (a dead fenced replica doesn't block). The
  next recovery tick rebuilds the now-sealed extent; an idle partition
  therefore drains with no client writes. The PS defers the roll while the
  partition is frozen for a split/merge (retried after the freeze).

Watching progress:

```bash
"${AO[@]}" info                       # per-node shard counts → 0 = drained
"${AO[@]}" extent-health --node 56 --all   # what's left + sealed state per extent
"${AO[@]}" recovery-stats             # in-flight rebuilds + backoff reasons
```

`remove <id>` is safe to run early — it refuses with the blocking extent ids
until the node is fully drained, and prints `remove: ok` only when the manager
has verified no extent / EC-marker references remain. After remove, the node_id
is tombstoned (same address cannot re-register); stop the EN process.

**Drain-never-completes checklist (root cause):** the
drain's last mile is the manager LEARNING that a rebuild finished — the EN
reports completed recoveries only in its `df` response, and the manager's df
goes to the node's **control address = advertise_host:(advertise_port+1000)**.
If anything sits between the manager and an EN (proxy, NAT, port forward), it
MUST forward the control port alongside the data port, or every df fails
silently: recoveries complete on the target ENs but are never applied, the
fenced node's slots never rewrite, and `remove` blocks forever while
`extent-health` shows the same blocking extents each probe. Symptoms of this
wiring failure: all nodes stuck in `Suspend` state (`list-nodes`), and
`recovery-stats` re-dispatching the same extent to a new candidate every
stale-sweep interval until every candidate refuses `extent already exists`
(re-dispatch to a candidate holding a verified-complete copy self-heals by
adopting it — but delivery still needs a working df channel).

Dead-EN notes (fence a node that's already unreachable):

- Everything above still works — seal probes and recovery just skip the dead
  replica. The failure modes that DON'T self-resolve are loud, never silent:
  an extent whose replicas are ALL unreachable refuses to seal
  (`Precondition`, sweep WARNs every cooldown), and a rebuild with no
  reachable source keeps retrying with the reason visible in
  `recovery-stats`'s backoff table.
- A fenced node that is still ALIVE drains faster (it serves as a recovery
  source). `cluster.sh` is fence-agnostic: `start`/`restart` launch fenced ENs
  normally (registration is one-time at `format`; only RE-registration of a
  fenced/removed node is refused).
- If a partition has no serving PS, its tails can't be rolled until the
  rebalancer assigns one (the sweep WARNs per cooldown). Manager-unilateral
  seal is a recorded follow-up, not built.

### EN identity is a UUID, not an address

An extent node's stable identity is a **UUID**, decoupled from its network
address — the same split the PS already has (`ps_id` vs advertise address).
`autumn-op format` mints a UUID v4 once and stamps it into a `node_uuid`
sentinel file in **every** `--data` dir (reused verbatim on a re-format, so a
re-format keeps the same `node_id`). It rides on `MSG_REGISTER_NODE`, and the
manager keys the node by it:

- **IP / shard-port change keeps the `node_id`.** A node that comes back at a
  different address (k8s pod reschedule) or with a changed shard-port layout is
  recognised by its UUID — the manager updates the routing address in place
  instead of minting a duplicate node. `list-nodes` shows the same `node_id`.
- **The fence / decommission tombstone is keyed by the UUID and survives
  removal.** A fenced/decommissioned node returning under its own UUID — at
  *any* address, and even after `remove` deleted its node record — is refused.
  Clear it with `autumn-op unfence <id>` (which now also lifts the
  `decommissioned/` tombstone) before it can rejoin, or wipe its data dirs for
  a fresh identity.
- **One address hosts exactly one node.** A *different* UUID registering at an
  address a live node already holds is **refused** (`CODE_PRECONDITION`) — two
  records at one address would make one physical EN two failure domains. To
  recycle a pod IP for a genuinely new node, `fence` + `remove` the old node
  first (freeing its address); the fresh UUID is then accepted.
- **Legacy (uuid-less) nodes are adopted.** A node that first registered before
  M0 (empty UUID) adopts the UUID on its next register at the same address.

The full design (including the k8s topology and the phased milestones) is in
[`en_dynamic_shard_design.md`](en_dynamic_shard_design.md). **Deploy note:** the
`node_uuid` field is in-struct on the persisted `MgrNodeInfo` — a same-commit
stop-world upgrade that requires an **etcd reset** (`cluster.sh reset`); there is
no rolling upgrade across this change.

### Resharding an extent node — changing its shard count

An EN's shard count = the number of io_uring cores it runs (one shard per core),
sized by `--cpuset` (`shard_count = cpuset_len`). Each shard `i` listens on
`--port + i*--shard-stride` and owns the extents where `extent_id % shard_count
== i`. Because the on-disk layout is hashed by `crc32c(extent_id)` (NOT by
shard) and all shards share the data dirs, **a reshard moves ZERO bytes on
disk** — only ownership/routing remaps by the new modulus.

Resharding is **stop-the-world for that node** (design decision #4): the EN
re-reports its live `shard_ports[]` to the manager on startup (needs
`--advertise`; the manager keys by `node_uuid` and updates the location in
place), so a restart with a different core count is the whole mechanism — no
`autumn-op format` re-run, no data migration.

```bash
# 1. Note the current shard count.
autumn-op --manager <MGR> list-nodes        # SHARDS column

# 2. Stop the EN process (SIGTERM). Its extents stay on disk untouched.
#    (Its slots go Suspected within ~2 s; reads/writes route to replicas.)

# 3. Restart the EN with the NEW core count. `--advertise` MUST be set so it
#    self-registers the new shard ports. Example: 2 -> 4 shards.
autumn-extent-node --data <DIRS> --port 9101 --manager <MGR> \
    --advertise <IP>:9101 --cpuset 0-3        # 4 cores = 4 shards

# 4. Verify the manager picked up the new layout (SHARDS should now read 4,
#    and the node returns to Online after its first df ~2 s later).
autumn-op --manager <MGR> list-nodes
```

Requirements / caveats:
- **The new shard ports (`port + i*stride`) must be free** on the host. On k8s
  the pod's Service must expose exactly `shard_count` data+control ports — that
  Service-port generation is a deploy-layer follow-up; on
  bare-metal / `cluster.sh` the ports just need to be unbound.
- **`--advertise` is what enables self-registration.** Without it the EN keeps
  the `format`-stamped location and the shard count stays frozen (pre-M1
  behavior). `cluster.sh` passes it automatically.
- Per-EN: shard count is independent per node — you can reshard one EN without
  touching the others (its extents remap under the new modulus; siblings are
  unaffected).
- A returning EN under its own `node_uuid` reuses its `node_id`; the manager's
  df-echo check (M1b) WARNs if the stored location drifts and refuses to serve
  an imposter that reused the node's IP under a different uuid.

## autumn-memory verification

`crates/autumn-memory` turns the cluster into an AI-agent-memory backend
(episodic logs, fact KV, BM25 + vector + hybrid retrieval, a graph). Design:
[`autumn_memory_plan.md`](autumn_memory_plan.md); crate guide:
`crates/autumn-memory/CLAUDE.md`. Consumers are Rust — the `memory-mcp`
example (MCP server + web UI, `examples/memory-mcp/README.md`) uses the crate
directly. Lexical (BM25) search needs no embedder; vector / hybrid use the
optional embedder.

**Manual verification (Rust core):**

```bash
cargo build --workspace                    # build the debug binaries first
cargo test -p autumn-memory                # pure unit tests (keys / BM25 / IVF / RRF)

# Full e2e against an ISOLATED throwaway cluster (memory-only manager, 1 EN,
# 1 PS, loopback, no etcd — does not touch any other cluster; tears down after):
bash crates/autumn-memory/tests/run_e2e.sh
#   → "===== e2e exit: 0 =====" and "test e2e_full_surface ... ok"

# Or run the e2e against an already-running cluster:
AUTUMN_MEMORY_E2E_MANAGER=127.0.0.1:9001 \
  cargo test -p autumn-memory --test e2e -- --ignored --nocapture

# Page-boundary regression (reconcile over a >page corpus counts exactly):
AUTUMN_MEMORY_E2E_MANAGER=127.0.0.1:9001 \
  cargo test -p autumn-memory --test scan_boundary -- --ignored --nocapture
```

**Manual verification (memory-mcp example — both corpora + MCP):**

```bash
./cluster.sh start 3
# code corpus (one crate for speed) + this repo's markdown docs:
./target/release/memory-mcp 127.0.0.1:9001 --root crates/autumn-memory --docs docs &

curl -s http://127.0.0.1:5100/stats
#   → {"docs":554,"edges":882,"is_clean":true,"symbols":573}  (counts move with the corpus)
curl -s 'http://127.0.0.1:5100/search?q=add%20a%20graph%20edge&corpus=code&mode=lexical&k=3'
#   → code symbols only (e.g. src/lib.rs::MemoryStore::add_edge)
curl -s 'http://127.0.0.1:5100/search?q=turnkey%20authz%20credentials&corpus=docs&mode=lexical&k=3'
#   → doc chunks with file + heading path + line range (e.g. docs/ops.md#L1127-L1179)
curl -s 'http://127.0.0.1:5100/members?id=docs/ops.md'      # document outline root

# MCP stdio round-trip (initialize / tools/list / a doc search):
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search_docs","arguments":{"query":"leader election","k":2}}}' \
  | ./target/release/memory-mcp 127.0.0.1:9001 --mcp 2>/dev/null
#   → serverInfo name "memory-mcp"; 17 tools; chunk hits with headings + line spans

# Graph database — nodes/edges with NO relation to code or documents. The point
# of the check is that arbitrary kinds and edge types round-trip, and that
# deleting a node takes its edges with it (`is_clean` stays true).
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"graph_upsert_node","arguments":{"id":"person:ada","kind":"Person","attrs":{"born":1815}}}}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"graph_upsert_node","arguments":{"id":"machine:engine","kind":"Machine"}}}' \
  '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"graph_add_edge","arguments":{"src":"person:ada","type":"WROTE_NOTES_ON","dst":"machine:engine","attrs":{"year":1843}}}}' \
  '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"graph_neighbors","arguments":{"id":"machine:engine","direction":"in"}}}' \
  '{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"graph_delete_node","arguments":{"id":"person:ada"}}}' \
  '{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"graph_delete_node","arguments":{"id":"machine:engine"}}}' \
  | ./target/release/memory-mcp 127.0.0.1:9001 --mcp --no-index 2>/dev/null
#   → id 5 returns the edge with its type, attrs {"year":1843} and the far node
curl -s http://127.0.0.1:5100/stats     # is_clean still true after the deletes

# Read half over HTTP (writes go through MCP, so no GET mutates):
curl -s 'http://127.0.0.1:5100/graph/nodes?kind=Document&limit=2'
curl -s 'http://127.0.0.1:5100/graph/neighbors?id=docs/ops.md&type=CONTAINS&limit=1'
curl -s 'http://127.0.0.1:5100/graph/traverse?id=docs/ops.md&max_depth=1&max_nodes=2'
./cluster.sh stop
```

**Ingest throughput must be measured against a REAL cluster, never loopback.**
Ingest is round-trip-bound, so loopback (~0.02 s/chunk) hides the cost that
dominates in a pod (~0.53 s/chunk before batching, i.e. ~45 min for a
5164-chunk corpus, which reads as a hang). To time it where it counts, run the
ingest from inside the cluster:

Startup ingest logs each phase's elapsed time, so a pod's own log is the
measurement — no separate harness:

```bash
kubectl -n autumn logs deploy/memory-mcp | grep -E 'ms/chunk|reconcile \+ train'
#   → ingested 5164 chunks (5164 outline edges) from 31 files in ... ms (... ms/chunk)
#   → reconcile + train_centroids in ... ms
```

Re-measure with `--reindex` (rebuilds the code corpus) or by pointing `--docs`
at a fresh corpus; a re-ingest of unchanged docs is an upsert and stays
representative of the write path.

If a change makes this slower, look for a `for k in keys { get(k).await }` loop
first — see "Round trips are the cost model" in `crates/autumn-memory/CLAUDE.md`.
A bulk ingest must also bracket itself with `begin_bulk_index()` /
`flush_stats()`; skipping the flush leaves `meta/stats` understating the corpus
until `repair_stats` runs (and `/stats` will show it).

## Retrieval-quality eval (`memory-mcp --eval`)

Every knob on the retrieval path — the tokenizer, BM25's `k1`/`b`, RRF fusion,
which leg `auto` picks, `NPROBE`, the centroid count, chunk size and overlap —
used to be tuned by argument, with the unit tests unable to tell whether a
change made search better or worse. Two incidents were caught by a person
reading results, not by a test. `--eval` scores a labelled query set against an
ingested corpus and compares it to a committed baseline.

Goldset: `examples/memory-mcp/eval/sutra.jsonl` (41 queries, JSONL + `#`
comments; each query labels relevance by `expect_file` / `expect_substr` /
`expect_id`, and may mark known-wrong hits with `reject_substr`).
Baseline: `examples/memory-mcp/eval/baseline.json`.
Corpus used for the committed baseline: `/data/dongmao_dev/md` — 17 Chinese
Buddhist books, 6 MB, 5164 chunks. It is deliberately NOT in the repo (size and
provenance); point `--docs` at your own copy and re-baseline if it differs.

```bash
cargo build --release -p memory-mcp
AUTUMN_DATA_ROOT=/tmp/autumn-eval ./cluster.sh reset 1

# 1. Build the index ONCE (ingest + reconcile + train_centroids), then score it.
./target/release/memory-mcp 127.0.0.1:9001 --agent eval \
    --docs /data/dongmao_dev/md --eval examples/memory-mcp/eval/sutra.jsonl
#   → ingested 5164 chunks ... (12.9 ms/chunk on loopback)
#   → mode=lexical  hit@1 0.976  hit@5 1.000  hit@k 1.000  MRR@k 0.988 ...

# 2. Every later run reuses that index — no --docs. THIS is the reproducible
#    loop, and the one a baseline comparison is valid across.
./target/release/memory-mcp 127.0.0.1:9001 --agent eval \
    --eval examples/memory-mcp/eval/sutra.jsonl \
    --eval-baseline examples/memory-mcp/eval/baseline.json
echo $?   # 0 = no regression, 1 = something got worse
```

**Passing `--docs` retrains the IVF centroids, and that alone moves
vector/hybrid.** Measured on this corpus: with no retrain, all three modes are
byte-identical across runs; with a retrain, lexical is unchanged while vector
and hybrid both shift (k-means re-initialises from the current IVF scan order —
the previous training's bucketing — so it settles into a different local
optimum). The report records `retrained`, and `compare` prints a NOTE when
either side of the comparison rebuilt. Do not chase a vector/hybrid delta
across a rebuild.

Reference numbers on the corpus above (`hash` embedder, k=10, 41 queries):

| mode | hit@1 | hit@5 | hit@k | MRR@k | P@k |
|---|---|---|---|---|---|
| lexical | 0.976 | 1.000 | 1.000 | 0.988 | 0.712 |
| vector  | 0.146 | 0.415 | 0.512 | 0.269 | 0.107 |
| hybrid  | 0.610 | 1.000 | 1.000 | 0.772 | 0.485 |

The vector row is the non-semantic `HashEmbedder` behaving as documented, and
the hybrid row is the cost of fusing it with a good lexical leg — which is why
`auto` resolves to `lexical` unless the embedder is semantic. Re-measure the
hybrid row before changing that rule.

**Check the eval is still alive** (a goldset that cannot go red is decoration).
Disable the CJK bigram emission in `crates/autumn-memory/src/recall.rs`
(the `if let Some(prev) = prev_cjk` block in the tokenizer), rebuild, ingest
into a scratch agent, and score against the baseline:

```bash
./target/release/memory-mcp 127.0.0.1:9001 --agent eval-nobigram \
    --docs /data/dongmao_dev/md --eval examples/memory-mcp/eval/sutra.jsonl \
    --eval-modes lexical --eval-baseline examples/memory-mcp/eval/baseline.json
#   → EXIT=1, and: WORSE lexical "慧能": rank 2 → miss
```

`--eval-update-baseline` overwrites the baseline instead of comparing. Use it
only when the corpus or goldset changed on purpose — a baseline that updates
itself records the regression instead of catching it.

**Getting a clean agent: reset the CLUSTER, not the agent.** `--reset` deletes
the agent's keys, and a document corpus is mostly BM25 postings — 5164 chunks
is ~2 million keys. Measured: ~3.4k keys/s, i.e. **~10 minutes** for a full
corpus, and it is scan-bound rather than write-bound (the partition itself
sustains 30k writes/s at 8 threads). `./cluster.sh reset 1` takes seconds.

## fs stripe geometry: lanes vs partitions

Large-file striping spreads one file's extents across N **lanes** so a single
write escapes the one-partition/one-log_stream ceiling. The key idea is that
**lanes and partitions are separate decisions**:

* **lanes** = the KEY LAYOUT. `lane = (offset / unit) % lanes`, encoded high in
  the key so it dominates routing. Default **24** (`DEFAULT_STRIPE_LANES`) —
  every file is striped whether or not anyone ran presplit.
* **partitions** = PLACEMENT. A partition owns a *contiguous run* of lanes.

Striping unconditionally is what makes placement changeable later: a file
written on a 1-partition fs already has its extents sorted by lane, so a split at
a lane boundary gives it parallelism **retroactively** — no data rewrite, no
re-stamping. (Before this, an fs that was never presplit wrote legacy keys that
sit in lane 0 forever; growing to 24 partitions did nothing for them.)

24 is over-provisioned on purpose. Any partition count that **divides** 24 —
1, 2, 3, 4, 6, 8, 12, 24 — distributes every file evenly, so the lane count is a
permanent constant instead of a function of cluster size. That is why a file's
stripe width never needs to widen.

Two pieces of state, different jobs:

* `fs/[0x04]stripe_geom` — the declared geometry `{lanes, unit_bytes}`. What NEW
  files get stamped with. Absent ⇒ the 24-lane default.
* `InodeMeta.stripe` — each file's ACTUAL geometry, immutable once written. Reads
  consult only this, never the cluster's current shape, so any
  split/merge/rebalance leaves existing files correct.

```bash
# Declare 24 lanes and cut 6 partitions (6 divides 24 → 4 lanes each).
$AO presplit --namespace fs --lanes 24 --parts 6      --admin-token-file $DATA_ROOT/authz/admin.token
# → declared fs stripe geometry: 24 lanes × 8 MiB units
# → presplit /fs: 5/5 cut points applied

$AO presplit --namespace fs --lanes 24 --parts 5     # rejected:
# parts must DIVIDE lanes ... Divisors of 24: 1, 2, 3, 4, 6, 8, 12, 24
```

`--parts` omitted ⇒ one partition per lane. `--lanes 1` turns striping off.

**Presplit an EMPTY keyspace, before loading data.** A data-bearing partition
can't be re-split until major compaction clears CoW out-of-range keys, so cuts
land only partially (`has_overlap`) if you load first.

### Declared boundaries: split there first, never merge there

`--admin-token[-file]` records the intended cut points on the namespace registry
row. That record drives BOTH halves of a symmetric rule:

* **merge refuses** to erase a declared boundary (`--force` to override). This
  matters because an EMPTY lane partition is a perfect auto-merge candidate
  (cold, tiny, zero QPS), and the window where lanes sit empty is exactly the
  reset → presplit → first-upload sequence. Merging one away is silent: every
  LATER large file just stripes narrower, with no error anywhere.
* **auto-split snaps** to the declared boundary nearest the middle of the
  partition, instead of the PS's median user key (which for fs lands *inside* a
  lane and breaks the whole-lane invariant). Once a partition holds no declared
  boundary, it falls back to median — an intra-lane inode split, which is the
  right cut at that point.

So you don't strictly have to run presplit at all: declare the points and the
cluster walks itself toward that layout as load grows. (Auto-split is local and
reactive, so it converges on "each partition owns a run of whole lanes", not on a
perfectly even parts-divides-lanes split — that evenness is a planned,
presplit-time property.)

```bash
$AO merge 12 13
# → refusing to merge 13 into 12: the boundary between them is a presplit point
#   declared for namespace 'fs' ... Re-run with --force if that is intended.
$AO merge 12 13 --force          # deliberate
```

The protection is generic — kvc hash buckets and mem agent cuts get it too; the
manager never learns what a "lane" is.

Notes:
* Striped WRITES are an `autumnfs` capability. A fuse mount reads and removes
  striped files correctly but **refuses** to write or truncate one (by design);
  write large files with `autumnfs put`.
* The download read window scales with the file's lane count (`get_window_extents`),
  because a window of W consecutive extents only spans W consecutive lanes —
  a fixed window would have quietly lost read parallelism once lanes were
  over-provisioned relative to partitions.

## Inspecting authz: who exists and what may they touch

`principal-create` / `principal-delete` shipped without a listing, so until now
answering "which principals exist and what are they granted" meant either
`ls $DATA_ROOT/authz/*.cred` (only what cluster.sh's turnkey path happened to
write — nothing an operator minted by hand) or an etcd key scan
(`etcdctl get --prefix --keys-only autumn-rs/tenantAccount/`), which shows names
but NOT grants because the value is rkyv.

```bash
$AO principal-list
# NAME                 GRANTS
# fs                   fs/
# kvc                  kvc/
# mem                  mem/

$AO principal-list --json     # [{"name":"fs","grants":["fs/"]}, ...]
```

Read-only and leader-routed (rotates on NOT_LEADER), so it needs **no**
`--admin-token`. It never prints credential material: the response row type
carries only `(name, grants)` — `credential_hash` is not a field on it, so there
is no flag or future edit that can make it leak. A lost credential is re-minted
(`principal-create` again, which rotates), never recovered.

The namespace-side counterpart is `namespace-list` (registry rows: name / prefix
/ owner / presplit / created_at).

## autumn-kvcache tenant / model identity (BUG-KVC-TENANT)

vLLM-connector KV keys are `kvc/{model}_{fingerprint}_{tp...}/vllm/...`. The
`{model}` segment is the autumn **weights-path basename** (e.g. `qwen7b` from
`model_loader_extra_config.path=models/qwen7b`), NOT the constant `/model-cfg`
config dir that several models can share — so the readable
segment ALONE distinguishes models even if the fingerprint ever degrades
(2026-08-11: keys now read `qwen7b_<fp>_0_1`, not the old collision-prone
`model-cfg_<fp>_0_1`). The 12-hex fingerprint carries the model's real identity
(arch shape + weights source + optional `model_id`; see
`python/autumn_kvcache/autumn_kvcache/_identity.py`). Before both, every model
served via the fixed local config dir shared ONE tenant and cross-read KV
(live 2026-07: Qwen2.5-7B/32B both under `kvc/model-cfg_0_1/`).

**Load is fail-closed (BUG-KVC-LOAD-ATOMIC, 2026-08-11).** When the scheduler
admits a request on the `__present__` marker but the worker cannot load EVERY
layer (TTL grace breach / tenant mismatch / backend fault), the connector now
injects NO KV for that request and reports its blocks via
`get_block_ids_with_load_errors()` so vLLM re-runs normal prefill. Previously it
injected the layers that loaded and skipped the rest → the request decoded on a
mix of loaded + uninitialised paged KV and emitted **silent garbage** (the live
symptom: `external KV load miss after positive presence` on layer 0..N). If you
see that warning now it is followed by a recompute, not a wrong answer. The fingerprint also folds in the two **layout
versions** — the running vLLM version (full `x.y.z`) and the connector's own
`VLLM_KV_STORAGE_FORMAT` (`_keys.py`) — so the same model on a
layout-incompatible stack never shares a tenant. Operational consequence:
**every vLLM upgrade (patch releases included) moves the tenant and
cold-invalidates the whole vLLM pool** — expected, one-time re-warm; the old
tenant's keys need the same manual reclaim as below.

**`--kv-cache-dtype` is part of the identity too** (added 2026-07-22): the
connector stores raw KV bytes and reinterprets them with the *current* runtime
dtype, and `CacheConfig.cache_dtype` is independent of the model dtype. The
silent case is a same-itemsize flip — `fp8_e4m3` ↔ `fp8_e5m2` are both one byte,
so nothing errors and the KV is just wrong. `cache_dtype` (plus
`kv_cache_dtype_skip_layers`) therefore splits the tenant. **Changing
`--kv-cache-dtype` moves the tenant and cold-invalidates the pool**, same as a
vLLM upgrade. Note this also means the FIRST deploy carrying this change starts
from a cold vLLM pool even with no config change, because the fingerprint gained
a source — orphaned old-tenant keys reclaim exactly as below.

```bash
# Offline unit tests (no cluster / engine / native module):
cd python/autumn_kvcache && uv run --with pytest python -m pytest tests/test_tenant_identity.py -q

# Manual verify on a live deployment: the connector logs its tenant + identity
# sources at startup — two DIFFERENT models must log two different tenants:
#   AutumnKVConnector role=... tenant=qwen7b_<fp>_0_1 ... identity={'layers': 28, ...}
# and the stored keys must not share a tenant prefix:
#   (autumn-client / python) list keys under kvc/ — one prefix per model.

# Upgrade note: the fingerprint changed every vLLM-pool key → old-tenant keys
# (e.g. kvc/model-cfg_0_1/vllm/...) are orphaned; with ttl_secs=0 they never
# expire. Reclaim manually when convenient (venv with the autumn wheel):
#   python - <<'EOF'
#   import asyncio, autumn
#   async def main():
#       c = await autumn.Client.connect("MGR:9001")
#       print("deleted:", await c.batch_delete(b"kvc/model-cfg_0_1/vllm/"))
#   asyncio.run(main())
#   EOF
# The load-miss-after-marker warning now states the plausible causes given the
# TTL config (ttl=0 ⇒ never blames TTL; points at tenant/model mismatch).
```

### External hit rate & the kill switch (BUG-KVC-NO-HIT)

The vLLM connector is an **L3 behind vLLM's own local prefix cache** (GPU + host
RAM). vLLM matches the local cache first and asks the connector only for tokens
*beyond* the local match, so:

- **same engine, repeated prompt** ⇒ local cache serves it ⇒ external is
  (correctly) never loaded ⇒ `External prefix cache hit rate: 0.0%`. **Expected,
  not a bug** — judge the connector by cross-instance / post-restart hit rate.
- **restarted or different engine, or after local eviction** ⇒ local cache is
  cold ⇒ the connector loads the prefix from autumn and skips prefill (measured
  ~3–4× TTFT win on a 1.3 k-token prefix).

Two changes killed the "kvc grows 20 GB while hit rate is 0%, prefill stalls"
symptom:

- **Almost everything is asynchronous.** On the forward pass `save_kv_layer`
  does ONLY the cheap GPU-side gather (a standalone tensor, no CPU sync). The
  D2H `.cpu()` copy, the **store-dedup probe**, the durable `put_from`, and the
  `__present__` marker all run on a background thread (a CUDA event orders the
  D2H after the gather; the marker publishes only after every layer ACKs). So a
  genuinely-new prefix no longer blocks prefill on the durable write, and a
  repeat is deduped in the background. Measured **no-hit overhead: TTFT +≈6–7 ms
  / TPOT ≈0** on both TCP and UCX (transport-independent, since the network work
  is off the critical path) — down from +≈148 ms when the D2H was synchronous.
- Staging: the in-flight background jobs hold *standalone GPU tensors* until
  their D2H runs (bounded per step by vLLM's token budget, and by
  `_MAX_INFLIGHT_SAVES` across steps); over the cap a save is dropped (a later
  request re-saves — pure cache).

Verify on a live deployment: a same-prompt request on a **freshly restarted**
engine (or after `reset_prefix_cache()`) should log an external hit and a much
lower TTFT than the cold-cluster first request; the first cold request returns
before its KV is durable, and the kvc partition's `live_size` grows in step with
distinct prefixes, not requests.

## Data-plane authz setup

Server-side key-range authorization for the `mem/` namespace
(`data_plane_authz_design.md`): the manager acts as a KDC that mints
short-TTL Ed25519 capability tokens; the PS verifies them per connection
(`AUTH_HELLO`) and enforces per request. **OPT-IN** — with no signing key
configured nothing changes (fuse / kvcache / perf-check / chaos all run
authz-off, anonymous, zero hot-path cost).

### Turnkey dev cluster with authz (`AUTUMN_AUTH=1`)

`cluster.sh` auto-provisions the whole authz bring-up so the examples work
end-to-end. `AUTUMN_AUTH=1` generates a signing key + admin token under
`$DATA_ROOT/authz/`, protects `mem/` & `gallery/`, registers the `gallery`
namespace, and mints per-example tenant credentials:

```bash
AUTUMN_AUTH=1 ./cluster.sh reset 5      # → $DATA_ROOT/authz/{signing.key,admin.token,memory.cred,gallery.cred}

# memory-mcp (mem/memory/, protected) — pass its tenant credential:
./target/release/memory-mcp --credential-file /tmp/autumn-rs/authz/memory.cred

# gallery (gallery/gallery/, protected) — Scoped client, credential via env:
AUTUMN_CREDENTIAL_FILE=/tmp/autumn-rs/authz/gallery.cred \
  ./target/release/gallery 127.0.0.1:9001
```

Both examples now bind a namespace scope (`{ns}/{tenant}/`, prepended by the SDK)
and auth via `--credential-file` / `AUTUMN_CREDENTIAL_FILE` (the SDK auto-mints
short-TTL tokens). Override the scope with `AUTUMN_NAMESPACE` / `AUTUMN_TENANT`
(gallery) or `--tenant` (memory-mcp). Tune protection with
`AUTUMN_AUTH_PROTECTED_PREFIXES` (default `mem/,gallery/`); an unprotected
namespace needs no credential.

```bash
# 0) One-shot cross-tenant e2e against an ISOLATED throwaway authz cluster
#    (gen key → manager with authz → two tenants → verify isolation):
bash crates/autumn-memory/tests/run_authz_e2e.sh
#   → "AUTHZ E2E OK: cross-tenant isolation + anon deny + ungated + MemoryStore pass-through"

# 1) Generate a signing key (LOCAL, no cluster needed) + an admin token file:
./target/release/autumn-op gen-signing-key --kid 1 > /path/signing.key
printf '%s' "$(openssl rand -hex 24)" > /path/admin.token

# 2) Start the cluster with authz enabled (cluster.sh env→flag translation;
#    protected prefixes default to mem/ when unset):
AUTUMN_AUTH_SIGNING_KEY_FILE=/path/signing.key \
AUTUMN_ADMIN_TOKEN_FILE=/path/admin.token \
  bash cluster.sh start 4

# 3) Create a PRINCIPAL (admin; credential printed ONCE as principal:/credential:
#    two lines — redirect straight to a credential file).
#    Keys are ns-first `{ns}/…` (no tenant); a grant is a whole namespace (`fs/`)
#    or an in-namespace sub-prefix (`mem/acme/`):
AO="./target/release/autumn-op --manager 127.0.0.1:9001"
$AO principal-create --principal acme --grant mem/acme/ --admin-token-file /path/admin.token > /path/acme.cred

# 4) Use it from the SDK / autumn-memory (auto-mints + renews tokens,
#    AUTH_HELLOs each PS connection; principal read from the credential file):
#      ClusterClient::connect_with_credential(mgr, "mem/acme", principal, secret)
#      MemoryStore::connect_with_credential(mgr, "acme", agent, principal, secret)
#    Cross-scope / anonymous access to a protected prefix fails PermissionDenied.

# Ops: mint a token by hand / revoke a principal:
$AO mint-token --principal acme --credential-file /path/acme.cred
$AO principal-delete --principal acme --admin-token-file /path/admin.token   # stops renewal; token dies at exp
# Key rotation: add a higher kid line to signing.key, restart the manager,
# wait a TTL, then mark the old line "disabled" (PS rejects it per request).
```

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
$AC put-stream KEY /path/to/big.bin      # chunked stripe-put for large values
$AC perf-check --threads 16 --size 4096 --duration 10 --partitions 8

# SST block cache (paged SSTs; SST data blocks no longer RAM-resident)
# PS flag: autumn-ps --sst-block-cache-bytes N   (cluster.sh: AUTUMN_SST_BLOCK_CACHE_BYTES, default 512MB)
# Manual check: write >> RAM dataset, kill -TERM the PS, restart, then
#   `$AC get KEY` must byte-match and idle PS RSS stays at the replay-window
#   bound (GBs), not O(dataset). Recovery must log `open_partition: ready`
#   for every partition with no `stale_vp_offset_past_sealed_length` retries.
# async SST iteration (no whole-SST materialization for range/compact/split)
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
#   (prepare) complete ... (chunked)" and then "EC shards staged on every
#   target; awaiting the manager's layout flip" (there is NO commit phase —
#   see the EC copy-on-write section below), then `get-stream` the value back
#   -> sha256 must match (chunk-encoded shards are byte-identical to a
#   whole-extent encode). Override stripe size with
#   AUTUMN_EXTENT_EC_STRIPE_BYTES on the EN to force many stripes on a smaller
#   extent. Repro script: the isolated memory-mode loopback recipe (manager
#   w/o --etcd, 4 single-shard ENs, 1 PS) used in dev.

# Admin / observability
$AO info                                 # nodes / extents / streams / partitions
$AO bootstrap --replication 3+0          # --presplit RETIRED; use `presplit --namespace <NS>` after
$AO split PART_ID                         # or: split PART --namespace <ns> --tenant <t> --at <suffix>
$AO merge SURVIVOR_PART_ID VICTIM_PART_ID # add --force to cross a declared presplit boundary
$AO rebalance [MAX_MOVES]                 # re-spread partitions across PS
$AO compact PART_ID
$AO gc --ratio 0.4 PART_ID                # NB: gc flags come BEFORE the partition id
$AO policy-candidates                    # advisory engine output (split/merge/gc/compact/EC)

# Cluster lifecycle (subshells so cwd stays at the repo root for ./cluster.sh)
(cd deploy/baremetal && ./autumn-deploy -t topology-singlehost.conf start)         # deploy path
(cd deploy/baremetal && ./autumn-deploy -t topology-singlehost.conf destroy --wipe) # tear down + wipe
./cluster.sh start 3                     # TEST harness only: 3-replica + auto-EC + chaos hooks
```

Extent refcount integrity (MERGE-REFS-LEAK class) is asserted by the in-process
chaos verify phase's STORAGE-ACCOUNTING invariants (see [Chaos suites](#chaos-suites))
— it reads the manager's etcd at a pinned revision and cross-checks every
extent's `refs` against live stream membership.

## Explicit split point — `autumn-op split --at`

`split PART_ID` with no extra flags lets the PS pick the median of the live keys
(legacy). To cut at an **operator-chosen** point — e.g. to pre-split an empty /
near-empty partition, or split two tenants into different partitions — name the
point on the CLI. The user-facing form speaks **namespace + tenant**, never raw
prefix bytes (the partition layer stays namespace-agnostic; the CLI assembles
the key and the wire carries only raw bytes):

The key order is **NAMESPACE-FIRST**: `{namespace}/{tenant}/{suffix}` (Option 3;
the older tenant-first `{tenant}/{namespace}/` was retired 2026-07-19).

```bash
# Cut exactly at the pair boundary "kvc/acme/" — splits `acme`'s kvc keyspace
# (and everything sorting >= it) off into a new partition. Empty/omitted
# suffix = the boundary itself.
$AO split PART_ID --namespace kvc --tenant acme --at ""

# Cut inside a pair at a text suffix -> key = "kvc/acme/" ++ "vllm/v1/80".
$AO split PART_ID --namespace kvc --tenant acme --at vllm/v1/80

# Binary suffix (e.g. an fs extent/inode prefix) via hex -> key = "fs/" ++ 0x0103ff.
$AO split PART_ID --namespace fs --at-hex 0103ff

# ADMIN escape hatch only (documented admin-only, like D7 raw()): a whole raw
# key, no namespace/tenant assembly. Operators should NOT hand-build prefixes.
# (hex below = "kvc/acme/" — namespace-first.)
$AO split PART_ID --at-raw-hex 6b76632f61636d652f
```

Rules & behavior:
- The assembled key must land **strictly inside** the target partition's
  `[start, end)` (equal to `start`, equal to/`>=` `end`, or out of range are all
  rejected). The CLI does a friendly pre-check (readable error naming your
  ns/tenant/suffix); the **PS is the authoritative validator**.
- With an explicit `--at`, an **empty or near-empty** partition can be split
  (the `>= 2 keys` gate is skipped) — this is the presplit primitive: cut an
  empty pair into two empty children. Without `--at`, an empty partition is
  still refused (`< 2 keys`).
- `--namespace` is required for `--at`/`--at-hex`/`--tenant`; `--tenant` is an
  OPTIONAL in-namespace sub-segment (mem/kvc — fs has none). `--at-raw-hex` is
  mutually exclusive with all of the above.

Manual verification (memory-mode loopback recipe, no etcd):
```bash
# 1. Bring up a 1-manager / 2-EN / 1-PS loopback cluster (see the dev recipe).
# 2. Create an EMPTY partition covering the keyspace, then presplit it at a
#    tenant boundary and confirm the region count goes 1 -> 2 with the new
#    boundary == the assembled key:
$AO info --json | jq '.partitions | length'          # -> 1
$AO split <PART> --namespace kvc --tenant acme --at "" --json
$AO info --json | jq '.partitions | length'          # -> 2
$AO info --json | jq -r '.partitions[].start_key'    # one range starts at acme/kvc/
# 3. Negative: a point outside the range is rejected up front:
$AO split <PART> --namespace zzz --tenant zzz --at "" ; echo "exit=$?"  # non-zero
```

## Namespace-aware presplit — `autumn-op presplit`

A raw-byte uniform split is **namespace-blind**: after key-namespacing every real key
sits in the `fs/…` / `kvc/…` / `mem/…` byte sliver (namespace-first, Option 3),
so uniform splitting over the whole 0x00..0xff space collapses everything into
one or two partitions (live: 19 GB fs on a single partition, 30 empty). That is
why `bootstrap --presplit` was retired. `presplit` instead splits a
`{namespace}/{tenant}/` keyspace along the namespace's **natural high-entropy
dimension** (built on the `split --at` primitive):

```bash
# fs — split by INODE (the fs data key is [0x03][ino BE][off BE]). Give the exact
# inodes (each safetensors shard = one inode = one partition), or a --count.
$AO presplit --namespace fs --fs-inos 4,5,6,7,8
$AO presplit --namespace fs --count 8            # → inodes 1..7

# kvc — split by CONTENT HASH (sha256 hexdigest). --hash-prefix is REQUIRED: it is
# the RELATIVE prefix from the namespace root down to just before the hash hex, and
# it is per-MODEL, so there is no default. The vLLM connector stores (Option 3, no
# tenant segment):
#   kvc/{model}/vllm/v1/{hash}/{layer}
# → the hash is under `{model}/vllm/v1/`, NOT directly under `vllm/`. Find the exact
# {model} fingerprint from a live key: `autumn-client --namespace kvc ls`.
$AO presplit --namespace kvc --tenant default --count 8 --hash-prefix "qwen3-8b_a1b2/vllm/v1/"
# sglang keys are {model}/{pool}/{hash} → pass "<model>/<pool>/".

# mem — split by AGENT.
$AO presplit --namespace mem --tenant default --agents alice,bob,carol

# fs --lanes N [--parts P] — split fs for large-file striping. LANES is the key
# layout (24 by default, a permanent constant), PARTS is how many partitions to
# create (must divide lanes; omit = one per lane). See the "fs stripe geometry:
# lanes vs partitions" section above for the full model + the sacred-boundary
# merge guard. Pass --admin-token[-file] so the boundaries are RECORDED (protected).
$AO presplit --namespace fs --lanes 24 --parts 6 --admin-token-file "$ADMIN_TOKEN"
```

### Stripe one large file across lanes (break the single-partition ceiling)

A single file = one inode = key-contiguous `[0x03][ino][off]` → ONE partition → ONE
log_stream. So a single file's write/read is capped by one stream's bandwidth
(measured ~220 MB/s single-connection, ~350 MB/s single-partition on fast NVMe;
disk/CPU are NOT the limit). To go faster, STRIPE the file across N lane partitions:

**Geometry is DECLARED, not auto-detected** (see the "fs
stripe geometry" section above for the full model). `presplit --lanes N` writes
the fs-wide `[0x04]stripe_geom`; every new file stamps that geometry into its own
`InodeMeta.stripe` at create (immutable), whether or not the partitions were cut
yet. There is no 64 MiB threshold and no per-upload flag — striping is on for the
whole fs once declared (default 24 lanes even with no presplit; declare `--lanes 1`
to turn it off).

```bash
# 1. Declare + cut on the EMPTY fs (before ingest). --parts spreads the lanes over
#    P partitions (must divide lanes). --admin-token records the boundaries so the
#    merge guard protects them.
$AO presplit --namespace fs --lanes 24 --parts 6 --admin-token-file "$ADMIN_TOKEN"
$AO info | grep part          # → 6 fs lane partitions, spread across PSs

# 2. Just upload. Every file's extents round-robin across the declared lanes:
#    extent e's offset o → lane (o/unit)%lanes, key [0x03][lane][ino][off];
#    autumnfs's concurrent batch_put fans them out → parallel across the lane PSs.
autumnfs --manager <mgr> put ./checkpoint.safetensors /ckpt
autumnfs --manager <mgr> get /ckpt ./out   # reader reads the file's own stamp
```

- The reader consults ONLY the file's stamped `InodeMeta.stripe` (never the current
  cluster shape), so old / non-striped files stay correct with **no migration**, and
  a later re-split at a lane boundary gives an existing file parallelism
  retroactively. `unit = MAX_EXTENT` (8 MiB) in v1 = each extent its own lane (max
  spread). MAX_EXTENT sweep (3-disk rig, EN-CPU-bound): striped-write peaks near
  4 MiB (~340 MB/s) and DECLINES for bigger extents (8→330, 16→298, 32→291); don't
  go above 8 MiB.
- **Scaling is bounded by whichever saturates first**: the lane PSs, the ENs' data
  plane, or the autumnfs client pipeline (window=8 + sync read barrier — a single
  file may not fully drive many lanes; running few parallel uploads or a deeper
  client pipeline closes the gap). Give ENs enough cores + put replicas on separate
  disks/hosts so the per-stream ceiling is high.
- **fuse mount**: READS and DELETES (unlink/rename-over) striped files correctly
  (an autumnfs-striped file is fully readable + removable via a mount on the same
  tenant). fuse WRITE/TRUNCATE of a striped file is refused fail-loud for now
  (streaming writes don't know the final size up front, so fuse can't decide the
  stripe geometry at create) — use `autumnfs put` to (re)write large striped files.
  Schema is **v3** (`InodeMeta.stripe`) — a stop-world reset from v2 (no in-place
  migration).

**CRITICAL — presplit the EMPTY keyspace BEFORE loading data.** A data-bearing
partition can't be split repeatedly: after the first CoW split, parent+child
share extents and the child's SSTs hold out-of-range keys, so the PS rejects the
next split with `precondition failed: cannot split: partition has overlapping
keys` until a major compaction clears them. `presplit` applies what it can and
prints the skipped points + this hint; the PS auto-major-compacts, so re-running
after a bit converges — but the correct order is **presplit first, then ingest.**
(Another known post-heavy-split hygiene item: manager `part_addrs` can go stale
after lots of split/merge — `merge X Y` says `partition X not served by this
P-log`; delete all PS pods in parallel to rebuild.)

## Chaos suites

```bash
# PS-failover chaos (2 PSes, kill one -> partitions must migrate, zero loss):
cargo test -p autumn-manager --test system_ps_failover_chaos -- --ignored
# vp_head multi-seed chaos: several seeds through the
# in-process system_chaos harness (real subprocess ENs + etcd + toxiproxy),
# nemesis focused on split/merge/compact/FORCEGC (+ gc/flush/EN-kill). forcegc
# bypasses the discard-ratio gate to punch specific sealed extents -> the maximal
# stress on the PS replay-floor guard; a wrong vp_head would let it punch a live
# extent = loss. Every acked put verified byte-exact per seed; PLUS a
# positive-reclaim check (verify_gc_reclaim): a final quiesce -> compact ->
# force-GC MUST physically DELETE extents (else the floor is stuck), and the
# punch pass is re-verified loss-free + leak-free:
./scripts/vphead_chaos.sh                              # 6 default seeds
VPHEAD_SEEDS="1 42 777" AUTUMN_CHAOS_DURATION_SECS=60 ./scripts/vphead_chaos.sh
#   (system_chaos's own action name for force GC is `forcegc`; AUTUMN_CHAOS_ACTIONS
#    to bisect, e.g. AUTUMN_CHAOS_ACTIONS=split,forcegc)
# Full-set + node DECOMMISSION chaos: same system_chaos
# harness but with the FULL nemesis set — including the ones vphead omits:
# fence (MSG_FENCE_NODE/clear), killfence (kill-then-fence), ec (convert-under-
# load), partition + latency (toxiproxy net faults). THEN a terminal one-shot:
# after the nemesis loop stops and the cluster heals, one EN is permanently
# removed the HDFS way (fence -> drain + fenced_only recovery relocate
# every extent off it -> MSG_REMOVE_NODE refuses until fully drained, tombstones
# the address), and the per-key/range/accounting verify proves NO loss with the
# node gone. Removal is a TERMINAL one-shot, NOT a per-cycle nemesis action
# (non-reversible: a permanent node loss injected every cycle would starve the
# cluster below quorum). Needs 6 ENs (removes 1, must leave >= K+M):
./scripts/decommission_chaos.sh                        # full set + remove, 3 seeds
AUTUMN_CHAOS_DECOMMISSION=0 ./scripts/decommission_chaos.sh   # full set, no remove
#   (any run of the base test can add the terminal remove with
#    AUTUMN_CHAOS_DECOMMISSION=1 AUTUMN_CHAOS_NUM_ENS=6)
# Transport-layer chaos (real cluster.sh cluster; E1 EN kill+respawn, E2 PS
# kill -> migrate, E3 PS respawn, E4 manager kill+respawn, E5 PS +
# manager double-kill inside the eviction window -> the interrupted eviction
# must converge and partitions FAIL BACK to the survivor; every ACKed
# write verified afterwards):
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/transport_chaos.sh tcp
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/transport_chaos.sh ucx   # needs --features autumn-server/ucx binaries
# (ucx note: a node killed -9 leaves its port in TIME_WAIT ~60s; the UCX
#  listener now retries bind through that window instead of exiting.)
# E6: CHAOS_ROUNDS=N CHAOS_SEED=S randomized repeated kill rounds.
# E7: split + mid-flight PS kill; merge + mid-freeze manager kill.
# Kvcache-interface chaos: python L3 backend under PS/manager kill
#   (NOTE: rebuild the wheel after ANY rust wire change — maturin build
#    --release + pip reinstall; a stale wheel mis-encodes requests):
#   ./scripts/kvcache_chaos.sh
# Fuse-interface chaos: file workload through the mount under
#   PS-kill / manager-kill / fuse-kill+remount + T1 truncate-shrink crash:
#   ./scripts/fuse_chaos.sh
# Fuse RMW corruption guard (RMW-GET-SWALLOW, 2026-06-23): partial in-place
#   overwrite during a PS kill+restart; a swallowed RMW read-error would zero
#   the untouched bytes of a file on a *successful* write. Single-PS (no
#   migration) so the verify read can't wedge:
#   AUTUMN_DATA_ROOT=/data05/autumn-rmw ./scripts/fuse_rmw_chaos.sh
# Fuse EN (data-plane) restart integrity (2026-06-23): kill+restart each EN;
#   durable + RMW files stay byte-exact across replica failover + rejoin.
#   (EN restart is CORRECT — no data loss. WRITES stall during EN-down ONLY at
#    exactly RF=3 ENs = capacity exhaustion, NOT a failover-latency bug: with
#    >RF ENs writes never stall; reads tolerate a down replica at any size.
#    See fuse CLAUDE.md "Restart behaviour".):
#   AUTUMN_DATA_ROOT=/data05/autumn-eni ./scripts/fuse_en_restart_chaos.sh
# Cross-host chaos (real network ::14+::15, remote via ssh):
#   ./scripts/crosshost_chaos.sh tcp | ucx
# Multi-manager HA chaos: leader kill -> standby takeover, PS kill under
# the new leader, old leader rejoins as follower; zero ACKed-write loss:
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh tcp
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh ucx
# (Notes: manager restart used to black-hole client routing — part_addrs
#  is in-memory; the PS now re-reports it every ~2s sync tick. Ownership
#  failback used to wedge forever — owner_epoch now bumps on every acquire.)
#
# In-process kill+split+merge+EC+fence chaos (manager + PS in the test process,
# EN as subprocesses spawned from target/debug — `cargo build --workspace` first).
# The test provisions its OWN throwaway etcd on random loopback ports (it does
# NOT use 127.0.0.1:2379 and cannot touch another cluster's etcd); it needs the
# `etcd`, `toxiproxy-server` and `toxiproxy-cli` binaries in PATH (overrides:
# AUTUMN_TEST_ETCD_BIN / AUTUMN_TEST_TOXIPROXY_SERVER / _CLI). The
# zero-data-loss invariant test — finds GC/seal/split data-loss + write-wedge:
AUTUMN_CHAOS_SEED=583 AUTUMN_CHAOS_DURATION_SECS=45 AUTUMN_CHAOS_NEMESIS_INTERVAL_MS=1500 \
  cargo test -p autumn-manager --test system_chaos \
  chaos_real_kill_split_merge_ec_fence_no_data_loss -- --nocapture --ignored
#   knobs: AUTUMN_CHAOS_SEED, _DURATION_SECS (30), _NEMESIS_INTERVAL_MS (3000),
#          _NUM_ENS, _EC_K/_EC_M, _ACTIONS (split,merge,ec,fence,flush,compact,
#          gc,kill,killfence,partition,latency).
#   verdict-gate: a real bug = `mismatches>0` OR a not_found that REPRODUCES on
#   DRAINED ports. A burst of not_found with `mismatches=0` after back-to-back
#   runs is almost always loopback PORT EXHAUSTION (cumulative TIME-WAIT) — a
#   wedged partition with no part_addr — NOT data loss. DRAIN-GATE before each
#   run: wait until `ss -tan | grep -c TIME-WAIT` < 4000 (see memory
#   project_chaos_long_soak_port_exhaustion). seed=583 = the GC stale-cache
#   big-value-loss regression guard (BUG-GC-STALE-CACHE); seed=603 (under
#   AUTUMN_CHAOS_NEMESIS_INTERVAL_MS=1500, 45s) = the seal-and-roll non-
#   idempotent-retry split-child-open wedge guard (BUG-IDEMPOTENT-ROLL).
#   STORAGE-ACCOUNTING invariants (beyond user data): the verify phase also
#   reads the manager's etcd (extents//streams/) at a single pinned revision and
#   asserts, for every extent, `refs == #streams listing it` + `vp_table_refs==0`
#   + no dangling membership — catching the extent-10 orphan / CoW double-free /
#   GC-leak classes that the per-key/range checkers can't see. Pure-logic unit
#   tests run in plain `cargo test` (no cluster): `... --test system_chaos
#   accounting_checker_tests`.
# extent delete carries a target identity
# What to expect: an EN that refuses a delete logs
#   `delete_extent addressed to a DIFFERENT node — refusing` at WARN, with
#   `for_node` / `this_node` uuids. Seeing this means a manager is retrying a
#   delete against an address now owned by a different node — almost always a
#   torn-down cluster whose persisted retries are still running against a host
#   that a NEW cluster reuses. Nothing was deleted; the correct action is to
#   stop the old manager, not to clear the warning.
# Manual check: start a node with `--advertise` (so it has a uuid), allocate an
#   extent, then send a delete naming a different uuid — the file must survive
#   and the WARN must appear; naming the node's own uuid must unlink it.
#   Unit-level equivalent:
#   `cargo test -p autumn-stream --lib delete_extent_refuses`.
# EC staging seal is durable (.meta payload_location)
# What to expect: after the manager flips an extent's layout to a shard file,
#   the owning EN persists that in `.meta` byte 41 and refuses any further
#   WriteShard for it — `write_shard from a SUPERSEDED conversion attempt` at
#   WARN, or a bare refusal once sealed. On boot the EN logs
#   `EC staging sealed on load ...` with the count it re-derived. A count of 0
#   on a node that holds shard files means the flip never reached its `.meta`
#   (check for the quarantine warning next to it) — the seal then holds only in
#   memory until the next reconcile round re-persists it.
# Manual check: convert an extent, confirm `.meta` byte 41 == 1 on a target
#   (`xxd -s 41 -l 1 <disk>/<hash>/extent-<id>.meta`), restart that EN, and
#   confirm the boot log reports it sealed. Unit-level equivalent:
#   `cargo test -p autumn-stream --lib the_ec_staging_seal_survives_a_restart`.
# op observability: live progress + durable history
# Two questions, two sources. `autumn-op ops list --active` reads the LEADER's
#   in-memory ledger — the only place a running op's progress exists, and it
#   dies with the leader. `autumn-op ops history` reads the etcd-backed log —
#   the only place a terminal op's FAILURE REASON survives.
# A memory-only manager (no --etcd) persists NO history: `ops history` now fails
#   loudly with "no durable store" rather than printing an empty list, because
#   an empty list there reads as "nothing failed".
# The dashboard shows both at GET /api/ops; it asks the leader through
#   autumn-op, never a file.
# Which kinds report progress: gc + forcegc (extent bytes scanned), compact
#   (SST data blocks merged) — sampled by the PS onto its load heartbeat; and
#   ec-convert (shard bytes encoded) + recovery (bytes copied) — sampled by the
#   EXTENT NODE onto `df`, keyed by extent_id since the node never learns the
#   op id. split/merge/rebalance are single-step and carry NO progress by
#   design; their result is in the leader log ("op succeeded" / "op FAILED"),
#   the audit trail and `ops history`.
# Manual check: `$AO ops list --active` during a large compact/gc, or during an
#   `$AO force-ec-convert` / a node rebuild, must show a percentage AND the raw
#   counts; after it finishes, `$AO ops history --limit 5` must carry its
#   outcome, with the error text in full for a failure. A finished op must stop
#   reporting a percentage — a repair frozen at a stale 75% is worse than none.
#   Automated equivalent (isolated cluster + etcd, asserts the endpoint shape):
#   `bash examples/dashboard/tests/ops_contract.sh`.
#   Dashboard: the panel shows the same numbers — verified live through
#   GET /api/ops during a conversion (18.6% → 37.2% → 55.8% → 74.4%, then
#   `succeeded 100%` in history). Render check without a cluster:
#   `node examples/dashboard/tests/render_check.js`.
#   LIVE EC-conversion progress: `bash scripts/ec_convert_progress.sh` — spins a
#   4-EN cluster (EC 3+1 needs four targets), rolls a 1 GiB log extent, converts
#   it and polls once a second. Measured 2026-08-28 on loopback: samples appear
#   ~5 s in (the marker is acquired before encoding starts, so `--` first),
#   advance one 64 MiB stripe at a time, and land on 100% at SUCCEEDED. The
#   denominator is THIS node's shard, ceil(extent / K), not the whole extent.
# chaos: pacing between runs is MANDATORY
# One `system_chaos` run burns ~50k loopback ephemeral ports, and TIME-WAIT
#   decays over ~60 s each — so back-to-back runs hit EADDRNOTAVAIL mid-run and
#   the verify reads that as a wedged partition: `not_found` on nearly every
#   key with `mismatches=0`. That shape is port exhaustion, NOT data loss (real
#   loss shows mismatches, or a SUBSET of not_founds). Gate each run on
#   `ss -tan | grep -c TIME-WAIT` < ~2000 and WAIT for the drain, don't skip.
# chaos: the EcConvert nemesis needs something to seal an extent first — it
#   logs "skipped — no sealed extents" otherwise, so an `ec`-only action list
#   converts nothing. Pair it with split/merge/gc (they roll streams), e.g.
#   `AUTUMN_CHAOS_ACTIONS=ec,kill,split,merge,gc`.
# Known harness note: only a ROLL seals an extent — restarting the PS replays
#   and keeps appending to the same open tail. And `autumn-client perf-check`
#   does not exit reliably once the log extent rolls (the cluster is fine
#   through it: the roll completes, the new tail's replicas agree, the manager
#   keeps probing) — the script caps it with `timeout -s KILL`.
```

## Rolling restart & upgrade versioning

**Deploy note (2026-08-27, MVCC internal-key comparator):** the partition
layer's internal-key encoding is `user_key ++ BE(u64::MAX - seq)` ordered by a
user-key-first comparator (no `0x00` separator byte). SSTs and WAL records
written under the older separator encoding are NOT readable — present keys
come back not-found and replay mis-splits keys. There is no migration: a
cluster carrying pre-change partition data must be rebuilt from empty
(`cluster.sh reset` on dev). Same-commit stop-world deploys after that
boundary are unaffected.

Same-binary rolling restart of a live cluster — one process at a time, a
convergence gate + per-partition write-liveness probe between every step,
fail-stop on the first gate that doesn't converge. Order: EN one-by-one →
PS → manager (most-depended-on end first).

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

`cluster.sh` provides the manager per-process subcommands for this:
`start-manager` / `stop-manager` / `restart-manager` (etcd state replay makes
a manager bounce a safe rolling step).

### Rebalancing region→PS assignment after a restart

**Symptom:** after a restart (especially a k8s rolling `kubectl apply`, which
bounces the PS pods one at a time) `autumn-op info` shows **all partitions
serving from one PS**, the others idle. This is expected, not a bug: the
region→PS assignment is **sticky in etcd** — the manager keeps a region on its
currently-registered PS and only reassigns regions whose PS is *unregistered*.
An eviction window during the restart (the PS being bounced misses its 10 s
heartbeat) moves its regions to whichever PS is up; when it comes back its old
regions are already sticky elsewhere. **A PS restart or a manager restart does
NOT re-spread them** (both keep the sticky assignment).

**Fix — actively re-spread with one command:**

```bash
autumn-op --manager <MGR> rebalance            # move as many as needed to balance
autumn-op --manager <MGR> rebalance 5           # throttle: at most 5 moves this call
autumn-op --manager <MGR> rebalance --json      # machine-readable {moved, moves[]}
```

The manager reassigns partitions most-loaded-PS → least-loaded-PS until the
per-PS count gap is ≤ 1 (count-based, like HBase `SimpleLoadBalancer` / WAS PM /
TiKV-PD `balance-region`). Each move rewrites the region's `ps_id`; the old PS's
`region_sync_loop` closes the partition and the new PS opens it (~2 s tick +
that partition's recover_partition). The key RANGE doesn't change (no
`region_epoch` bump); clients re-resolve the moved partition's listener via the
refreshed `part_addr` and the SDK's routing-miss retry absorbs the brief
per-partition reopen window. **Throttle with `[MAX_MOVES]`** on a large cluster
so the target PSes aren't hit by a reopen storm all at once — run it a few times,
or once unbounded on an idle cluster.

Verify:

```bash
autumn-op --manager <MGR> info | grep '  part' | awk '{print $4}' | sort | uniq -c
# expect the counts spread across all PS addresses, gap <= 1
```

Idempotent: re-running on an already-balanced cluster reports `0 moves`. (An
automatic version — the dashboard auto-policy `rebalance` switch — is
Phase B, not yet shipped.)

### cluster_version + wire-version interval (R1)

Version-skew foundation: every binary carries a wire-version
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

v28 changed the FRAME layer itself (one uniform shape:
`[header][ctrl_len][ctrl][crc][value]`, crc over header+ctrl, raw value tails
uncrc'd). Deploy note: a pre-v28 binary against a v28 peer fails at the FIRST
frame with a loud `frame CRC mismatch` connection error — it never reaches the
GetClusterId version handshake, so expect transport-level errors (not the
"wire-schema mismatch" message) in a mixed deploy. Same-commit deploys are
unaffected.

Bump discipline lives in `crates/rpc/src/lib.rs` (`WIRE_VERSION_FINGERPRINTS`
registry): any wire-schema edit fails `cargo test -p autumn-rpc` until you
record the new fingerprint and consciously decide MIN/MAX. Rolling back a
binary past a `cluster_version` bump is refused at manager startup
(fail-closed in replay).

## Test matrix

```bash
cargo test --workspace --exclude autumn-fuse --lib          # all crate unit tests
cargo test -p autumn-stream --lib                            # stream layer only
cargo test -p autumn-partition-server --lib                  # partition server only
cargo test -p autumn-manager                                 # integration (needs etcd)
```

Manager integration tests under `crates/manager/tests/` cover split / merge / chaos / crash
recovery. Some are gated with `#[ignore]` because they take minutes; use
`cargo test --release -- --ignored` to run the slow set.

GC data-integrity regression (full VP-identity liveness — a superseded older
version of a key in the same sealed extent must NOT revive over the newer one):

```bash
cargo test -p autumn-manager --test system_gc_multiversion_same_extent
```

**Write-pipeline changes (e.g. natural batching) are verified with the perf matrix**
(`./perf/perf_check.sh --3disk --partitions 8` — builds release, starts a fresh 3-replica
cluster, runs tcp/ucx × 4K/8M and compares each leg against
`perf/perf_baseline_<transport>_p8_d8_s<size>.json`; a leg passes when ops/s ≥ 80% of
baseline and p99 ≤ 2×). The `--min-pipeline-batch` PS flag is deprecated
(parsed, warns, no effect) — batch sizing is adaptive and needs no tuning knob.

## Inode-lease + close-to-open coherence (in flight)

Multi-mount / multi-daemon coherence for `autumn-fuse` and
`autumn-ioring-daemon` runs through a JuiceFS-style inode lease served
by the manager. Plan + invariants live in
[`autumn_fs_lease_plan.md`](autumn_fs_lease_plan.md).

Landed so far:
- **Manager lease state** — manager state + 4 RPCs (`MSG_*_LEASE` /
  `MSG_POLL_INVALIDATIONS` = `0x46`–`0x49`), writer-lease etcd
  persistence under `inode_leases/<ino>`, TTL revoke loop.
- **Daemon lease acquire** — autumn-ioring-daemon Open acquires (and
  Close releases) a write/read lease per inode. `RING_VERSION 1→2`:
  the Open SQE's flags byte now carries `LEASE_MODE_READ` (1) /
  `LEASE_MODE_WRITE` (2). A v1 client (flags=0) is interpreted as
  WRITE — the safe default. Two concurrent writers on the same
  inode (different daemons OR different sessions of the same
  daemon) get `libc::EBUSY` on the second Open.
- **Invalidation long-poll** — long-poll invalidation channel.
  `MSG_POLL_INVALIDATIONS` blocks up to 10 s when the inbox is
  empty (manager parks a waker); a writer-close pushed by ANOTHER
  daemon fires the waker so the reader sees the event in ms, not
  via a retry tick. Daemon spawns a persistent
  `session_invalidation_poll_loop`; on transport error or overflow
  sentinel it wholesale-invalidates the session cache.
- **Close-to-open coherence** — `OpenedExtents.lease_version` populated
  from the AcquireLease response; per-session `InvalidationMap`
  bumped by the poll loop. Read SQE arm calls `cache_is_stale`
  and on stale invokes `fuse_read::reload_extents` to re-fetch
  the inode meta + extent map before serving — close-to-open
  coherence end-to-end. **Phase 1 complete.**

Smoke-tests (no cluster boot required):

```bash
# Manager-side state machine + RPC contract.
cargo test -p autumn-manager --lib inode_lease
cargo test -p autumn-manager --test ioring_lease

# Daemon-side lease helpers + two-daemon conflict / read-coexistence /
# version monotonicity / heartbeat round-trip.
cargo test -p autumn-manager --test ioring_lease_2

# Long-poll: writer-close wakes a parked reader in ms (3 tests; the
# idle-timeout case waits the full 10s LONG_POLL_WAIT — ~30 s total).
cargo test -p autumn-manager --test ioring_lease_3

# Close-to-open cache invalidation: per-ino floor bumps on
# WriterClosed; reader's stale-cache predicate flips; overflow
# sentinel surfaces (overflow test takes ~10 s for its 1025 cycles).
cargo test -p autumn-manager --test ioring_lease_4

# BUG-LEASE-2 storage fencing (needs built binaries; boots a cluster):
# Phase 1 — stale-epoch MSG_PUT rejected with CODE_FENCED; anonymous
# (inode_hint=0) writes bypass.
cargo test -p autumn-manager --test bug_lease_2_storage_fencing -- --ignored
# Phase 2 — the floor SURVIVES a PS kill -9 + restart, on both recovery
# paths (WAL OP_FENCE_BUMP replay; TableLocations.fence_floors checkpoint
# after a flush), and MSG_PUT_BULK (the fuse/ioring large-write path) is
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
- **fuse mount lease + cache invalidation** — autumn-fuse mount opt-in: open/release
  call lease::acquire/release; kernel attribute cache invalidated
  via `fuser::notify_inval_inode`.
- **Force-revoke / writer revoke** — force-revoke / writer revoke protocol so
  "another daemon needs to write NOW" doesn't have to wait for
  the current writer to close.

## Zero-copy model load

Serve a model that lives in autumn straight into GPU memory via the pinned
zero-copy read seam (`autumn.Fs.read_into`) + batched EN direct-read, at
≈Run:ai-Model-Streamer throughput. The loader pipeline: parse the safetensors
header → per tensor `read_into` a **CUDA-pinned** host buffer (double-buffered)
→ async H2D overlapped with the next read. Storage reads go direct to the extent
nodes (`autumn.Fs.connect(direct_read=True)`); descriptors resolve in ONE PS
round-trip per file (`MSG_GET_REDIRECT_MANY`), so the ~N-extent reads fan across
all ENs with the PS off the metadata path.

**Build note (UCX):** binaries + wheel need `--features ucx` for
`--transport ucx`. The wheel MUST be built `--skip-auditwheel` (bundling UCX
libs segfaults — UCX `dlopen`s its transport modules from the system install;
the client must link **system** UCX like the daemons).

**A/B vs Model Streamer (intra-host UCX, GPU host):**
```bash
# cluster bound to a RoCE NIC IP (NOT loopback), UCX positive-list env from cluster.sh
AUTUMN_BIND_HOST="[<roce-nic-ip>]" AUTUMN_TRANSPORT=ucx \
  AUTUMN_DATA_ROOT=/data/autumn-ucx bash cluster.sh start 4
# client pinned to the SAME NIC (both-ends rule); run on a free GPU
AUTUMN_MANAGER="[<roce-nic-ip>]:9001" CUDA_VISIBLE_DEVICES=<free-gpu> \
  UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=mlx5_1:1 \
  python3 remote_bench.py     # set_transport("ucx"); upload model; A/B loader vs runai
```
Expect: **byte-exact** (loaded tensors == safetensors ground truth) and autumn
EN-direct at ~80% of Model Streamer's local-page-cache number at K≈4 (the fair
comparison is vs Model-Streamer-from-remote-storage, where autumn/RDMA wins).
The `Fs.read_into` seam alone (no GPU) is checkable headless with a `bytearray`
dest: `fs.read_into(ino, off, memoryview(buf))` byte-equals `fs.read(ino, off, n)`.

## Enabling authz

**Deploy layer = ON by default (Task 2, 2026-07-18).** Both deploy paths arm
data-plane authz automatically. **Protect-everything (tenant-first, 2026-07-19):**
with a signing key present, EVERY tenant-scoped write requires a token — there is
no protected-prefix list; a credential grants a key prefix — a whole namespace
(`fs/`) or an in-namespace sub-prefix (`mem/hermes/`). The key layout is
`{ns}/…` (NO tenant segment; see §8).

- **`deploy/baremetal/autumn-deploy start`** generates a signing key + admin
  token once (reused across re-deploys — rotating invalidates every credential),
  distributes the key to every manager host, and after bootstrap mints per-family
  principal credentials to `~/.autumn-deploy/authz/*.cred`. Clients pass
  `--credential-file ~/.autumn-deploy/authz/fs.cred` (the principal name is read
  from the file — no `--principal`/`--tenant`).
- **k8s** (`deploy/overlays/vke/deploy.sh`) generates the `autumn-authz` Secret
  (signing key + admin token) once and the manager StatefulSet mounts it (the
  signing key alone arms protect-everything — no prefix list). Mint a client
  credential + Secret with the manual steps below.
- **Escape hatch:** `AUTUMN_AUTH_DISABLE=1` (both paths) runs authz-OFF — for
  local debugging. The dev/test harness (`cluster.sh`, `scripts/*_chaos.sh`)
  never sets `AUTUMN_AUTH_*`, so it is authz-OFF unconditionally.
- **Native clients** all take `--credential-file <path>` (NO `--principal`/
  `--tenant` — the principal identity travels IN the file): `autumn-fuse`,
  `autumnfs`, `autumn-client`. The file is the two-line `principal:`/`credential:`
  form `autumn-op principal-create` prints (or `<name>\n<hex>`); the hex decodes
  to the raw bytes the manager hashed.

### Manual runbook (custom principals, or a non-deploy setup)

Client-side wiring: PyO3 `Client.connect(scope=,principal=,credential=)` +
`BatchClient(scope=,principal=,credential=)`, hermes provider
`AUTUMN_MEMORY_CREDENTIAL_FILE`. Everything below is the OPERATIONAL enablement
for a principal the deploy layer did NOT auto-provision. Gradual-rollout axis:
credentials-first (steps 1–4 are harmless with authz off), prefix-enforcement
last (step 5).

```bash
# 1. one-time: signing key (KEEP SAFE; k8s: put it in a Secret)
autumn-op gen-signing-key > /secrets/autumn-auth-signing.key

# 2. create the PRINCIPAL. Grant an in-namespace sub-prefix (`mem/hermes/`) or a
#    whole namespace (`fs/`). principal-create prints the two-line
#    principal:/credential: form (shown ONCE) — redirect it STRAIGHT to the
#    credential file (the reader parses the name + hex from it):
autumn-op --manager $M principal-create --principal hermes \
    --grant "mem/hermes/" --admin-token-file /secrets/admin.token \
  > /secrets/hermes.cred

# 3. hermes/memory clients: mount the Secret, set
#    AUTUMN_MEMORY_TENANT=hermes  (the mem sub-prefix)
#    AUTUMN_MEMORY_CREDENTIAL_FILE=/secrets/hermes.cred
#    (harmless while authz is off — credential is simply unused)

# 4. verify mint works BEFORE enforcing (minting is a manager RPC, unaffected by
#    whether the PS is enforcing yet — safe to run while authz is off):
autumn-op --manager $M mint-token --principal hermes \
    --credential-file /secrets/hermes.cred   # must print a token

# 5. ARM: manager gets --auth-signing-key-file (or env
#    AUTUMN_AUTH_SIGNING_KEY_FILE via entrypoint). PROTECT-EVERYTHING: the signing
#    key alone arms enforcement of EVERY namespaced write — there is no
#    protected-prefix list. Restart manager; PS picks it up via 5s authz poll.

# 6. verify enforcement: a credential-less write must fail
autumn-client --manager $M --namespace mem put x /tmp/f  # expect PermissionDenied
# hermes mem round-trip must still pass (it now carries the credential)
```

Rollback = remove the signing-key flag and restart the manager (no key =
authz fully off). Failure modes: a client missing its credential fails ALL
mem/ writes with PermissionDenied (terminal, not retried — that is the
fail-loud design); manager unreachable > TTL−300 s → token renewal fails →
writes rejected until the manager returns (enforcement adds a
grace-window=TTL availability dependency of the data plane on the manager).

## G2 — power-loss crash-consistency test (LazyFS, single machine)

Verifies the core durability contract: **every write the client got an ACK for
survives a power loss**, and recovery never fails-loud spuriously or serves
garbage. Uses [LazyFS](https://github.com/dsrhaslab/lazyfs) — a userspace FUSE
filesystem that only persists `fsync`'d data; its `clear-cache` command drops
everything not yet fsync'd = a power cut at that instant. **No kernel module**
(dm-log-writes needs `dm_log_writes.ko`, absent in this container; LazyFS is the
userspace equivalent). autumn's io_uring write path works on the FUSE backend.

Scope: single-node **RF1** cluster with the data plane (`AUTUMN_DATA_ROOT`) on
the LazyFS mount; **etcd is bind-mounted OFF LazyFS** so only autumn's
data-plane durability is under test (control plane assumed on its own durable
quorum).

```bash
# 0. Build LazyFS once (userspace; needs libfuse3-dev + cmake + g++):
git clone --recurse-submodules https://github.com/dsrhaslab/lazyfs /opt/lazyfs
(cd /opt/lazyfs/lazyfs/libs/libpcache && ./build.sh)   # or cmake -S . -B build && cmake --build build
(cd /opt/lazyfs/lazyfs/lazyfs        && ./build.sh)    # → /opt/lazyfs/lazyfs/lazyfs/build/lazyfs
# The harness auto-discovers /opt/lazyfs, ~/lazyfs, ../lazyfs; else set LAZYFS_BIN=<path>/lazyfs.

# 1. Run it (quiesced crash — coalescer settles, then power loss):
cargo build --release --workspace          # harness uses release binaries
scripts/g2_crash_consistency.sh
#   → "VERDICT: PASS — every acked write survived power loss, recovery clean" (exit 0)

# 2. Immediate crash (power loss the instant after the last ACK — probes any
#    ACK-before-fsync window; PASS proves synchronous durability):
scripts/g2_crash_consistency.sh --immediate

# Knobs: --keys N (small values) / --big M (2 MiB values; default 70×2 MiB =
# 140 MiB > MAX_WAL_GAP 128 MiB → forces a rotate+flush so recovery exercises the
# checkpoint-reload path too, not just WAL replay). Env: LAZYFS_BIN, G2_WORK,
# N_SMALL, N_BIG, BIG_BYTES, QUIESCE, MAX_WAL_GAP.
```

What it asserts, per acked key: present after restart + byte-identical (SHA-256);
counts LOST (acked→gone) and CORRUPT (bad bytes); scans PS/EN/manager logs for
fail-loud markers (`WAL-FAILSTOP`, `invalid meta`, `StaleVpOffset`,
`failed to open partition`, `panicked`). PASS requires 0 lost, 0 corrupt, and
`survived == acked`. The `open_partition: ready … tables=N sst_readers=N
max_seq=…` line in the summary confirms which recovery path ran (tables>0 =
checkpoint reload + WAL replay; tables=0 = pure WAL replay).

Mechanism has teeth: a standalone check (write file A with `fsync`, file B
without, `clear-cache`) shows A survives and B is dropped — so a real durability
gap would surface as LOST keys.

## Chaos: reading a failure

`cargo test -p autumn-manager --test system_chaos -- --ignored --nocapture`

The report is ordered so the first thing you read is the cause, not the symptom:

1. **`write failures by reason`** — the workload's rejected writes, tallied by
   PS code / RPC error / routing failure. A chaos workload MUST tolerate failed
   writes (that is the point of a nemesis), so the tally is the only thing that
   separates "faults are landing" from "nothing works".
2. **`WORKLOAD ACKED NOTHING`** — a hard failure. Every per-key invariant is
   vacuous over an empty expectation set, so a run that wrote nothing would
   otherwise report `0 mismatches, 0 not_found` and pass. If you see this, fix
   the workload before reading anything below it.
3. **`WHY:`** — a scan of the EN subprocess logs for fail-loud markers
   (`WAL-FAILSTOP`, `META-FAILCLOSED`, quarantine, stale VP, refused EC
   completions, superseded attempts, disk-offline, supervised-loop panics).
   **Their absence is the sharper finding**: the invariant broke while every
   layer believed it was fine. `logs:` gives the directory to dig in.
4. The per-category counts and samples.

Manager and PS run in-process, so their tracing goes to the test's own stderr,
not to `logs:`. Only EN logs are on disk — which is the right surface anyway,
since recovery, EC conversion, quarantine and disk health all live there.

**The trap this encodes.** For five weeks `system_chaos` reported "all
invariants OK" while every single write was rejected with `NamespaceUnknown`:
Layer-A namespace validation is always on, and the test wrote bare keys. Nothing
caught it because a chaos workload is *supposed* to swallow write failures —
the tolerance that makes it correct is what let 100% rejection look like normal
nemesis pressure. Ordinary tests were never exposed: `support::ps_put` retries
and then panics, so a rejected write fails loudly there.

Chaos keys are `mem/{b|q}{kid:06}` under the built-in `mem` namespace
(`CHAOS_NS`). A new chaos scenario must namespace its keys or every write will
be refused.

## EC copy-on-write conversion — what an operator sees

Conversion is **copy-on-write**: the EN stages each shard as an ADDITIVE file
`extent-{id}.shard{i}` and never touches the `.dat` it was derived from. The
manager's layout flip is the **only** commit point. There is no per-node commit,
no rename, and no intent marker — an abandoned attempt costs a delete of files
no reader is pointed at.

**The life of one conversion**, and where to look if it stalls:

| stage | evidence |
|---|---|
| dispatched | manager marker in `autumn-op extent-health` / `list-ec-inflight-markers` |
| staging | EN log `EC 2PC phase 1 (prepare) complete ... (chunked)` |
| staged | EN log `EC shards staged on every target; awaiting the manager's layout flip` |
| committed | `autumn-op ops list --kind ec` → `succeeded`; the marker drains |
| reclaimed | EN log `reconcile: reclaimed the pre-conversion .dat; this node now serves its shard` |

The last row lags the others by up to one reconcile sweep (5 min, or immediately
on EN restart). Until it happens the extent occupies BOTH forms — that is
expected, not a leak.

On-disk, a converted extent should end as exactly one `extent-{id}.shard{i}` per
member, each `sealed_length / K` bytes, plus `.meta`. The coordinator also keeps
`extent-{id}.ec.prepared` (16 bytes), which records which ATTEMPT staged the
shards; it is current-scheme state, not residue.

```bash
find <data-dir> -name 'extent-<ID>.*' -printf '%f(%s) '
```

**If a conversion never reaches `succeeded`:**

- `ops list --kind ec` carries the last failure reason and an attempt count.
- A marker whose coordinator went offline is released automatically and
  re-derived onto a live node — "gone" means absent from the cluster or
  `Suspected`, NOT merely "not Online" (a freshly registered node is `Suspend`
  until its first `df`, and abandoning on that would make any conversion longer
  than one tick impossible).
- A completion report from a superseded attempt is refused by nonce, logged as
  `ec_done is from a DIFFERENT conversion attempt than the live marker`. That is
  the system protecting itself, not an error to chase.

## EC copy-on-write conversion — cross-host verification

`scripts/ec_crosshost_verify.sh` exercises the whole EC conversion line across
TWO machines, which is the shape single-host loopback cannot test: manager + PS
+ EN0 on this host, EN1 + EN2 on the peer, `2+1` erasure coding, so shards fan
out over the network and two of the three holders are remote.

```bash
# Build first — the peer's release tree is whatever was last shipped to it, and
# the script scp's these binaries over.
cargo build --release --workspace
bash scripts/ec_crosshost_verify.sh
```

Edit `L6` / `R6` at the top for your two hosts; the peer is reached through
`.claude/skills/remote-autumn/remote-autumn.sh` (ssh -p 2222).

What it asserts, in order: the conversion op reaches `succeeded`; all 8 × 64 KiB
values read back byte-identical **after the layout flip**; every EN restarts and
its reconcile reclaims the pre-conversion `.dat` **on both hosts**; the same
values still read back byte-identical with **no `.dat` anywhere in the cluster**.
PASS requires all four. A converted extent should end as one `.shard{i}` per
node at `sealed_length / K` bytes.

Three traps this script exists to encode, all of which cost a run to find:

- **`--listen` defaults to `0.0.0.0`** on both the EN and the PS — the IPv4
  wildcard, which refuses the IPv6 address they advertise. Pass `--listen <v6>`
  explicitly on every node or the manager's `df` never connects and every node
  sits `Suspected`. (When `df` has never once succeeded, `list-nodes` prints
  `HB_AGO`/`SUSP_AGE` in the hundreds of seconds — that is an absent baseline,
  not stale state. Don't chase it.)
- **A peer data dir keeps its `cluster_id`.** Re-running against a fresh manager
  makes `autumn-op format` refuse to join a different cluster — that guard
  working, not a failure. Wipe the peer dirs between runs.
- **Restart the ENs only.** The PS serves the reads being verified; killing it
  makes the final check fail as `connect PS … failed`, which reads like a
  data-plane break and is not one.

## S3 gateway — serving autumn weights to engines with no loader plugin

`autumn-s3` is a read-only, unauthenticated S3 endpoint over the `fs/` tree. It
exists so SGLang and FreeToken — neither of which has a loader plugin seam —
can use their built-in `--load-format runai_streamer` to stream weights
concurrently, with no engine patches. `aws s3` and every other S3 client work
against it too.

Buckets are the first level under `fs/`: `s3://models/llama/x.safetensors` is
autumn `fs/models/llama/x.safetensors`.

```bash
# 1. Run it next to the engine (per-GPU-node sidecar keeps the RDMA hop long
#    and the HTTP hop on loopback).
autumn-s3 --manager 127.0.0.1:9000 --port 9100 \
          --credential-file /secrets/fs.cred      # omit when authz is off
# --workers N (default 8, capped at core count) — accept threads, SO_REUSEPORT.
# One thread caps an AWS-CRT client at ~40% of the read path; the knee is at 4.

# 2. Smoke it with the aws CLI. The credentials are DUMMY — the gateway never
#    looks at the Authorization header — but the SDK's credential chain runs
#    BEFORE the request is sent, so they must be set to something.
export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url http://127.0.0.1:9100 s3 ls
aws --endpoint-url http://127.0.0.1:9100 s3 ls s3://models/llama/
aws --endpoint-url http://127.0.0.1:9100 s3 cp s3://models/llama/config.json -

# 3. Ranged read (what the streamer actually issues) must answer 206 with an
#    exact Content-Range:
curl -s -D- -o /dev/null -H 'Range: bytes=0-7' \
     http://127.0.0.1:9100/models/llama/model-00001.safetensors
#   → HTTP/1.1 206 Partial Content
#     content-range: bytes 0-7/<size>

# 4. Verify the streamer path itself (no GPU needed). This is the exact code
#    SGLang's runai loader runs:
pip install runai-model-streamer-s3     # the AWS-SDK plugin; NOT in the base package
env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
    AWS_ENDPOINT_URL=http://127.0.0.1:9100 \
    AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_EC2_METADATA_DISABLED=true \
    python3 -c "
from runai_model_streamer import list_safetensors, SafetensorsStreamer
with SafetensorsStreamer() as st:
    st.stream_files(list_safetensors('s3://models/llama'))
    print(sorted(n for n, _ in st.get_tensors()))"

# 5. Serve with SGLang (same env; the proxy unset matters here too):
export AWS_ENDPOINT_URL=http://127.0.0.1:9100
python -m sglang.launch_server --model-path s3://models/llama \
       --load-format runai_streamer
# vLLM takes the same URL; on vLLM prefer --load-format autumn (native, RDMA
# zero-copy) unless you are A/B-ing the two.
```

Not supported, by design: PUT/DELETE, multipart, versioning, ACLs,
virtual-host addressing (use path-style, which is what `--endpoint-url`
selects), and SigV4 verification. Anything else answers `NotImplemented`.

Gotchas:
- **`HTTP_PROXY` silently swallows the streamer.** The Run:ai streamer's S3
  backend is aws-c-s3 (the CRT client), which honours `HTTP_PROXY`/`HTTPS_PROXY`
  and **ignores `NO_PROXY`** — verified: with `NO_PROXY` already listing
  `127.0.0.1`, every read still went to the proxy and came back
  `AWS_ERROR_S3_INTERNAL_ERROR` / "File access error", with no socket ever
  opened to the gateway. UNSET the proxy variables for the engine process:
  ```bash
  env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy python -m sglang.launch_server ...
  ```
  The boto3-side listing is unaffected (it does honour `NO_PROXY`), so the
  symptom is "the model directory lists fine, then every weight read fails".
- **The CRT sends absolute-form request lines** (`GET http://host:port/bucket/key`)
  rather than origin-form. The gateway handles both; a reverse proxy in front of
  it may not.
- **path-style only.** A client configured for virtual-host addressing resolves
  `bucket.host` and never reaches the gateway.
- **An undelimited listing walks the tree.** `aws s3 ls --recursive` from a
  bucket root is capped at 100k entries and logs a warning; prefer a prefix.
- **Listing costs one inode lookup per key** (for size/mtime). Fine for a model
  directory; not a directory-crawler substitute.
