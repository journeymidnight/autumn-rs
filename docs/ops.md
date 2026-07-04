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
- [Read route-around for Suspected nodes (F276)](#read-route-around-for-suspected-nodes-f276)
- [autumn-memory verification](#autumn-memory-verification)
- [Data-plane authz setup (F-AUTHZ-1)](#data-plane-authz-setup-f-authz-1)
- [CLI cheatsheet](#cli-cheatsheet)
- [Chaos suites](#chaos-suites)
- [Rolling restart & upgrade versioning](#rolling-restart-r0-of-docsrolling_upgrade_designmd)
- [Test matrix](#test-matrix)
- [Inode-lease + close-to-open coherence (in flight)](#inode-lease--close-to-open-coherence-f-ioring-lease-in-flight)

## Binaries & ports

| Binary | Default port | Role |
|---|---|---|
| `autumn-manager-server` | 9001 | Control plane (streams, partitions, recovery) |
| `autumn-extent-node` | 9101+ | Data plane (raw extent files on disk) |
| `autumn-ps` | 9201 binary default; deployments use 9301 (+ per-partition) | LSM partition server |
| `autumn-client` | — | Data-plane CLI (put/get/del/head/ls/perf-check) |
| `autumn-op` | — | Admin CLI (bootstrap/split/merge/compact/gc/info/df/format) |
| `autumn-stream-cli` | — | Low-level stream debugging |
| `autumn-fuse` | — | FUSE mount of the KV namespace |

`autumn-client --help` / `autumn-op --help` lists subcommands. (The standalone
Python `python/dashboard/` was retired 2026-07-04 — folded into the manager; see
"Web dashboard + auto-policy controller" below.)

## Web dashboard + auto-policy controller (F-DASH-IN-MGR)

The manager serves an embedded web dashboard AND hosts the auto-policy controller
in-process — one crash-safe, leader-owned process (the retired Python dashboard's
controller died with its webserver; this one survives as long as the leader does).

```bash
# Enable from cluster.sh (test harness; opt-in):
AUTUMN_DASHBOARD=1 ./cluster.sh start 3
#   → manager serves http://<manager-listen>:8799/   (read-only viewer)
# Arm manual actions + the controller (default is read-only):
AUTUMN_DASHBOARD=1 AUTUMN_DASHBOARD_ALLOW_MUTATIONS=1 ./cluster.sh start 3

# Deploy (autumn-deploy / k8s / docker) turns it ON by default; disable with
# AUTUMN_DASHBOARD=0. On k8s it rides the leader-gated Service:
kubectl -n autumn port-forward svc/autumn-manager 8799:8799   # → http://localhost:8799/

# Direct flags (env→flag is in the shell layer, never Rust):
autumn-manager-server --etcd … --dashboard-port 8799 [--dashboard-allow-mutations]
```

The page shows cluster capacity, node health, the PS→partition→extent hierarchy,
policy advisories, and (armed) per-target action buttons.

**Auto-policy controller** — the manager only *emits* advisories (F203 pure
mechanism); the controller *decides + actuates* per an active policy. It is
**leader-only** (never runs on a follower), **DEFAULT-OFF** (a fresh cluster stays
pure-mechanism until an operator arms it), and a state machine `Off → DryRun →
Armed`. `Armed` actuates only when `--dashboard-allow-mutations` is set (else it
degrades to DryRun and logs "would: …"). Config is persisted to etcd
(`autoPolicy/config` + `autoPolicy/cooldowns`, leader-fenced) so the active policy
survives leader failover. Headless control:

```bash
autumn-op auto-policy status                 # mode + active + presets + action log
autumn-op auto-policy activate gc-only       # select + DryRun (observe, no actuation)
autumn-op auto-policy activate aggressive --arm   # select + Armed (actuate)
autumn-op auto-policy deactivate             # mode → Off
```

Presets (safest → most aggressive): `gc-only`, `maintenance`, `space-reclaim`,
`balanced`, `aggressive`. The dashboard `/api/policies` UI can also create/select
custom policies (armed only).

**Verify leader-failover of the active policy** (the crash-safety guarantee):

```bash
# with an etcd-backed cluster + the dashboard armed:
autumn-op auto-policy activate gc-only --arm       # → mode=armed active=gc-only
kill -9 <leader-manager-pid>                        # crash the leader
# after the etcd lease expires (~10 s) a new leader wins + replays from etcd:
autumn-op auto-policy status                         # → STILL mode=armed active=gc-only
```

**Security posture (documented non-goal):** the dashboard port has no
per-request auth/TLS (same as `--metrics-port`). Default is a read-only viewer;
mutations require `--dashboard-allow-mutations`. When arming a network-reachable
dashboard, pair it with network ACLs (or `--dashboard-listen 127.0.0.1` +
tunnel). On k8s, mutations ride the leader-gated Service.

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

**In Kubernetes**, run autumn-fuse as a privileged per-node DaemonSet (mounts
`/dev/fuse`, `--manager autumn-manager:9001`) — a consumer workload on the app
nodes, separate from the storage StatefulSets.

## Python fsspec (`autumn://`) verification

`python/autumn_fsspec` is a thin fsspec facade over `autumn.Fs` — the **shared
inode layout** (F-FS-UNIFY M3), so a file written via fsspec is byte-identical
through an `autumn-fuse` mount and vice versa. It needs the `autumn` PyO3 SDK
built **from the cluster's commit** — a wheel older than the cluster fails
connect with `wire-version mismatch` (rebuild: `cd python && maturin build
--release && pip install --force-reinstall --no-deps target/wheels/autumn-*.whl`).

```bash
# Offline (no cluster) — a Python inode tree (FakeFs) backs the SAME facade code
# path; full FS surface + HuggingFace datasets + models upload/materialize:
cd python/autumn_fsspec
python -m pytest tests/test_fs_offline.py tests/test_datasets_offline.py \
                 tests/test_vllm_loader_offline.py tests/test_models_offline.py -q
#   → 28 passed

# Live — self-contained (boots an isolated memory-mode cluster, builds the
# wheel, runs the live suite against the autumn.Fs backing, tears down):
cargo build --workspace
bash python/autumn_fsspec/tests/run_fsspec_e2e.sh
#   → 9 passed, "===== fsspec-e2e exit: 0 ====="

# or against an already-running cluster:
AUTUMN_MANAGER=127.0.0.1:9001 python -m pytest tests/test_e2e_cluster.py -q
```

Model loading (materialize-to-local / FUSE-`eager` / streaming loader):
[`docs/model_loading.md`](model_loading.md).

### `autumn.Fs` — shared inode-layout binding (F-FS-UNIFY M2)

`autumn.Fs` is a PyO3 binding over the **same** fuser-free FS core the
`autumn-fuse` mount runs on (inode/dirent/extent layout) — the plumbing that
lets M3 rewrite `autumn_fsspec` as a facade sharing files with a fuse mount.
Headless correctness (self-contained isolated memory-mode cluster — builds the
wheel, boots manager+EN+PS, drives the full `Fs` surface + a cross-instance
byte-exact check, tears down):

```bash
cargo build --workspace                    # debug binaries first
bash python/tests/run_fs_e2e.sh
#   → "PY M2 CROSS-INSTANCE byte-exact OK", "===== fs-e2e exit: 0 ====="

# M4 — lease fencing + cross-client coherence (two Fs clients):
bash python/tests/run_fs_lease_e2e.sh
#   → "PY M4 fencing OK", "PY M4 coherence OK", "===== fs-lease-e2e exit: 0 ====="

# M4 — REAL cross-surface interop (needs /dev/fuse + fusermount3): write through
# an autumn-fuse kernel mount, read byte-exact via fsspec, and vice versa:
bash python/autumn_fsspec/tests/run_mount_fsspec_interop.sh
#   → "PY INTEROP OK: fuse mount + fsspec are one filesystem", exit 0
#   (skips cleanly if /dev/fuse or fusermount3 is absent)
```

M4 write-fencing: `autumn_fsspec` and a fuse mount both take the same per-inode
WRITE lease around writes (via `autumn.Fs` / `lease_tasks.rs`), so concurrent
writers to one inode conflict instead of corrupting each other; reads are
close-to-open coherent (fresh-read + `forget`-on-release). Behavior-preservation
gate for the `dispatch` Create/Unlink/init_root refactor + the M4 `lease_tasks`
extraction (the binding shares those core steps): the fuse e2e suite must stay
green — `cargo test -p autumn-manager --test system_fuse_read --test
f_fuse_lease_1 --test f_fuse_lease_2 -- --ignored --test-threads=1`.

Chaos (fsspec interface under failover — PS kill→migration, manager
kill→respawn, final byte-exact verify + write-liveness probe; timeouts are
dropped as UNCERTAIN, never counted as loss):

```bash
cargo build --release --workspace       # cluster.sh runs release binaries
AUTUMN_DATA_ROOT=/data05/autumn-rs bash scripts/fsspec_chaos.sh
#   → "=== FSSPEC CHAOS PASS ===" (MISMATCH/VERIFY-FAIL lines = corruption = FAIL)
```

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
the corrupt OPEN log_stream tail`) — frozen at the committed length via the F227
probe, then isolated in the same pass like a sealed extent. Still fails the open
loud (data lives on a healthy replica → recover / retry) for: an all-replicas-bad
extent, or an open tail that is **truncated** below the committed prefix (sealing
there could drop acked data — a separate F227 edge; **F227 — the seal must be
lenient**: the seal path accepts a lenient/committed-length freeze rather than
demanding byte-perfect tails). EC extents route shard repair
through recovery, not this path. End-to-end fault injection lives in
`scripts/selfheal_chaos.sh` (3-EN cluster, flip one byte of slot[0]'s extent
`.dat`, restart → assert self-heal + byte-exact reads incl. the corrupted-value
key + slot isolated; plus an all-replicas-corrupt fail-loud negative). That
harness caught a real read-path bug on its first run: the avali isolation filter
was wired only into the copy read path, so the two VP-value fast paths
(`read_value_into_pooled` ZC proxy + `extent_read_descriptor` client-direct)
still served the bit-rotted-but-isolated replica — now both filter
`eligible_replica_slots`. Design: `docs/wal_selfheal_design.md`.

## Read route-around for Suspected nodes (F276)

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
the pre-F276 reactive failover; it just removes the repeated per-read timeout
under sustained load. **Manual check:** on a 3-EN replicated cluster, `kill` one
EN; after the manager flips it to `Suspected` (`autumn-op info` /
`list-node-states`) and a couple seconds of read traffic, `get` of keys whose
extent has a replica on the dead node is served by a healthy replica instead of
stalling for the per-RPC timeout on every read.

## autumn-memory verification

`crates/autumn-memory` turns the cluster into an AI-agent-memory backend
(episodic logs, fact KV, BM25 + vector + hybrid retrieval). Design:
[`autumn_memory_plan.md`](autumn_memory_plan.md); crate guide:
`crates/autumn-memory/CLAUDE.md`. The Python stack sits on the Rust core:
`autumn.Memory` (PyO3) → `autumn_memory.AutumnMemory` (JSON + embedder hook) →
framework shells (stdio MCP server / LangGraph `BaseStore` / Hermes
`MemoryProvider`). Lexical (BM25) search needs no embedder; vector / hybrid use
the optional embedder.

**Manual verification (Phase 1 — Rust core):**

```bash
cargo build --workspace                    # build the debug binaries first
cargo test -p autumn-memory                # 19 pure unit tests (keys / BM25 / IVF / RRF)

# Full e2e against an ISOLATED throwaway cluster (memory-only manager, 1 EN,
# 1 PS, loopback, no etcd — does not touch any other cluster; tears down after):
bash crates/autumn-memory/tests/run_e2e.sh
#   → "===== e2e exit: 0 =====" and "test e2e_full_surface ... ok"

# Or run the e2e against an already-running cluster:
AUTUMN_MEMORY_E2E_MANAGER=127.0.0.1:9001 \
  cargo test -p autumn-memory --test e2e -- --ignored --nocapture
```

**Manual verification (Phase 2 — Python binding + MCP server):**

```bash
# Ergonomic layer (autumn.Memory + AutumnMemory) against an isolated cluster,
# with a fake embedder so the vector/hybrid legs also run (builds a throwaway
# venv + cluster, tears down):
bash python/autumn_memory/tests/run_smoke.sh
#   → "ERG SMOKE OK: AutumnMemory full surface (json + embedder hook)"

# MCP server driven through a REAL MCP client over the SDK in-memory transport
# (full tool surface: search/fetch/add/update/delete + episodic + facts):
bash python/autumn_memory_mcp/tests/run_mcp_test.sh
#   → "MCP INPROC OK: full tool surface ..." and "===== mcp-test exit: 0 ====="

# LangGraph BaseStore adapter (get/put/search/filter/query/list_namespaces/delete/ttl):
bash python/autumn_memory_langgraph/tests/run_store_test.sh
#   → "LANGGRAPH STORE OK: BaseStore surface ..." and "===== lg-store-test exit: 0 ====="

# Embedder client (OpenAI-compatible /embeddings; mock server, no cluster needed):
bash python/autumn_memory/tests/run_embedder_test.sh
#   → "EMBEDDER OK: ..." and "===== embedder-test exit: 0 ====="

# Hermes MemoryProvider adapter, driven against the REAL Hermes ABC (clone it
# first) — register/init/sync_turn→prefetch recall/tools/built-in-write mirror:
git clone https://github.com/NousResearch/hermes-agent /data/dongmao_dev/hermes-agent
bash python/hermes_memory_autumn/tests/run_hermes_test.sh
#   → "HERMES PROVIDER OK: real MemoryProvider ABC ..." and "===== hermes-test exit: 0 ====="

# Real-model semantic e2e: starts a local sglang embedding server + cluster and
# checks semantic recall with NO lexical overlap (needs a free GPU + a venv from
# the run above). The harness keeps sglang as a child of the one run.
EMBED_MODEL=Alibaba-NLP/gte-Qwen2-1.5B-instruct EMBED_GPU=7 \
  bash python/autumn_memory/tests/run_real_embed.sh
#   → "REAL EMBED OK: vector + hybrid semantic recall ..." and "===== real-embed-test exit: 0 ====="

# Launch the stdio server for a real host (config via env or CLI flags):
AUTUMN_MEMORY_MANAGER=127.0.0.1:9001 AUTUMN_MEMORY_AGENT=my-agent \
  python -m autumn_memory_mcp             # or the `autumn-memory-mcp` console script
# Enable semantic (vector/hybrid) search by pointing at an OpenAI-compatible
# /embeddings endpoint (sglang / vLLM / OpenAI):
#   AUTUMN_MEMORY_EMBED_URL=http://127.0.0.1:30000/v1 \
#   AUTUMN_MEMORY_EMBED_MODEL=BAAI/bge-m3 python -m autumn_memory_mcp
```

## Data-plane authz setup (F-AUTHZ-1)

Server-side key-range authorization for the `mem/` namespace
(`data_plane_authz_design.md`): the manager acts as a KDC that mints
short-TTL Ed25519 capability tokens; the PS verifies them per connection
(`AUTH_HELLO`) and enforces per request. **OPT-IN** — with no signing key
configured nothing changes (fuse / kvcache / perf-check / chaos all run
authz-off, anonymous, zero hot-path cost).

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

# 3) Create a tenant (admin; credential printed ONCE — hand it to the tenant):
AO="./target/release/autumn-op --manager 127.0.0.1:9001"
$AO tenant-create --tenant acme --prefix mem/acme/ --admin-token-file /path/admin.token

# 4) Use it from the SDK / autumn-memory (auto-mints + renews tokens,
#    AUTH_HELLOs each PS connection):
#      ClusterClient::connect_with_credential(mgr, "acme", credential)
#      MemoryStore::connect_with_credential(mgr, "acme", agent, credential)
#    Cross-tenant / anonymous access to mem/ now fails with PermissionDenied;
#    keys outside mem/ are ungated.

# Ops: mint a token by hand / revoke a tenant:
$AO mint-token --tenant acme --credential-file /path/acme.cred
$AO tenant-delete --tenant acme --admin-token-file /path/admin.token   # stops renewal; token dies at exp
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

# Cluster lifecycle (subshells so cwd stays at the repo root for ./cluster.sh)
(cd deploy/baremetal && ./autumn-deploy -t topology-singlehost.conf start)         # deploy path
(cd deploy/baremetal && ./autumn-deploy -t topology-singlehost.conf destroy --wipe) # tear down + wipe
./cluster.sh start 3                     # TEST harness only: 3-replica + auto-EC + chaos hooks
```

Extent refcount integrity (MERGE-REFS-LEAK class) is asserted by the in-process
chaos verify phase's STORAGE-ACCOUNTING invariants (see [Chaos suites](#chaos-suites))
— it reads the manager's etcd at a pinned revision and cross-checks every
extent's `refs` against live stream membership.

## Chaos suites

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
# Cross-host chaos (F272, real network ::14+::15, remote via ssh):
#   ./scripts/crosshost_chaos.sh tcp | ucx
# Multi-manager HA chaos (F267): leader kill -> standby takeover, PS kill under
# the new leader, old leader rejoins as follower; zero ACKed-write loss:
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh tcp
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh ucx
# (F265 notes: manager restart used to black-hole client routing — part_addrs
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

`cluster.sh` provides the manager per-process subcommands for this:
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
[`autumn_fs_lease_plan.md`](autumn_fs_lease_plan.md).

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
