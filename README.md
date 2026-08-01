# autumn-rs

**One storage engine for the AI stack — from the storage data model, through
inference KV cache, all the way to agent memory.**

Running an AI platform usually means operating a zoo of storage systems: an
object store or NFS for models and datasets, a cache tier for inference KV, a
vector database for agent memory. **autumn-rs collapses that zoo into one
distributed engine.** It is a Rust storage engine architecturally inspired by
the **Windows Azure Storage (WAS)** paper — a *stream layer* of replicated,
append-only extents underneath an ordered-KV *partition layer* — and every
AI-facing surface is a thin client on that single data plane:

```
   Files & checkpoints      Inference KV cache        Agent memory
   ┌────────────────┐   ┌──────────────────────┐   ┌─────────────────────────┐
   │  autumn-fuse   │   │   autumn-kvcache     │   │      autumn-memory      │
   │  POSIX mount   │   │ sglang / vLLM        │   │ episodic + facts +      │
   │                │   │ HiCache L3 backend   │   │ BM25/vector/hybrid      │
   └───────┬────────┘   └──────────┬───────────┘   │ web UI + MCP (examples) │
           │                       │               └───────────┬─────────────┘
           │        ┌──────────────┘                           │
           ▼        ▼                                          ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │        partition layer  (ordered KV, LSM, split/merge)          │
   │        stream layer     (replicated extents, EC, no local WAL)  │
   └──────────────────────────────────────────────────────────────────┘
        autumn-client CLI/SDK · autumn-manager (etcd-backed control plane)
```

One replication story, one capacity pool, one ops surface — model files, KV
cache pages, and agent memories all land in the same replicated, erasure-coded,
self-healing extents.

## Highlights

**The AI all-in-one:**
- **Storage data model** — ordered KV (put/get/delete/range, MVCC, streaming
  put/get for large values) via `autumn-client` CLI or the Rust/Python SDK, plus
  a **POSIX filesystem** (`autumn-fuse`) and a programmatic **`autumn.Fs`** Python
  binding (same shared inode layout) for models, datasets and checkpoints.
- **Inference KV cache** — `autumn-kvcache` implements the sglang / vLLM
  **HiCache L3** storage-backend API (pure Python adapter, no extra daemon);
  verified end-to-end against real models with correct cross-instance
  prefix-cache hits.
- **Agent memory** — `autumn-memory`: episodic logs, a fact store, an
  associative graph, and retrieval that combines **BM25** (CJK-aware), **IVF
  vector search**, and **hybrid RRF** — entirely client-side on plain KV. A
  Rust example app (web UI + **MCP** `--mcp` stdio) sits on it — `codebase-memory`
  indexes this repo's own source; recall P99 ≈ 25 ms on the agent turn loop.

**The engine underneath:**
- **Fast by construction** — thread-per-core on io_uring (compio), custom binary
  RPC with rkyv zero-copy, group commit, adaptive batching. Optional **RDMA
  (UCX)** transport with zero-copy reads *and* writes: cross-host 8 MiB reads
  run ~4.6× TCP (`docs/perf_tcp_vs_ucx_xhost.md`).
- **The log is the database** — no local WAL files anywhere; a min-replica
  commit protocol makes the replicated extents themselves the journal. Open
  extents are 3-way replicated; sealed extents are **erasure-coded (K+M)** in
  the background for cold capacity.
- **Self-healing** — WAL bit-rot heals from clean replicas at partition open;
  disk-full self-recovers in ~2 s; reads route around suspected nodes; partitions
  fail over automatically. All of it is exercised by a battery of chaos suites
  (`docs/ops.md`).
- **Elastic** — copy-on-write partition split/merge with an advisory engine for
  split/merge/GC/compaction/EC decisions, plus a built-in **auto-policy
  controller** that acts on them — leader-fenced and crash-safe (config in etcd,
  survives failover), default-off until you arm it.
- **Multi-tenant** — opt-in key-range authorization with short-TTL **Ed25519
  capability tokens** (manager as KDC, enforcement at the KV layer).
- **Operable** — an **embedded web dashboard** served by the manager itself (no
  separate process), declarative bare-metal deployer (systemd), Kubernetes
  manifests, Prometheus `/metrics`, `ceph df`-style capacity accounting,
  rolling restart with convergence gates.

## Quick start

Prerequisites: Linux ≥ 5.15 (io_uring), Rust toolchain, `etcd` ≥ 3.5 in PATH
(`libfuse3-dev` only if you build the FUSE client).

```bash
cargo build --release -p autumn-server
cd deploy/baremetal
./autumn-deploy -t topology-singlehost.conf start   # etcd + manager + 3 ENs + 1 PS, bootstrapped
./autumn-deploy -t topology-singlehost.conf status

AC="../../target/release/autumn-client --manager 127.0.0.1:9001"
echo hello | $AC put mykey /dev/stdin
$AC get mykey                                       # → hello

./autumn-deploy -t topology-singlehost.conf destroy --wipe   # tear down
```

Works on a laptop too: without systemd (macOS, containers, non-root) the
deployer falls back to a plain process backend automatically.

## Using it

### KV — CLI / SDK

```bash
AC="./target/release/autumn-client --manager 127.0.0.1:9001"
echo body | $AC put KEY /dev/stdin        # write
$AC get KEY                               # read
$AC ls --prefix p/ --limit 100            # ordered scan
$AC put-stream KEY /path/big.bin          # chunked stripe-put for large values
```

Rust: `autumn-client::ClusterClient` (`crates/client`). Python: the PyO3
bindings under `python/`. Full CLI reference: [`docs/ops.md`](docs/ops.md).

### Files — POSIX mount (models, datasets, checkpoints)

```bash
cargo build --release -p autumn-fuse
./target/release/autumn-fuse --manager 127.0.0.1:9001 --mountpoint /mnt/autumn --transport tcp &
cp model.safetensors /mnt/autumn/        # a regular filesystem, backed by the cluster
```

Full runbook (mount verification, stale-mount cleanup, RDMA env, k8s
DaemonSet): [`docs/ops.md`](docs/ops.md#fuse-daemon-runbook).

### Files — programmatic `autumn.Fs` (datasets, checkpoints)

The `autumn` PyO3 extension exposes `autumn.Fs` — the same shared inode layout a
fuse mount serves, so Python reads/writes the cluster directly without a mount:

```python
import autumn
fs = autumn.Fs.connect("127.0.0.1:9001")
ino = fs.resolve("/models/llama-3-8b")
```

Large files are transparently chunked (8 MiB, zero-copy reads). For serving a
model that lives in autumn (vLLM / SGLang), see
[`docs/model_loading.md`](docs/model_loading.md) — the `autumn_vllm_loader`
(`--load-format autumn`) streams weights straight to GPU.

### Inference KV cache — sglang / vLLM

`python/autumn_kvcache` plugs into the HiCache L3 storage-backend interface —
the cluster becomes the shared L3 tier behind GPU/CPU cache levels, so prefix
caches survive restarts and are shared across inference instances. Setup +
design: [`docs/autumn_kvcache_plan.md`](docs/autumn_kvcache_plan.md),
[`docs/hicache_l3_interface.md`](docs/hicache_l3_interface.md).

### Agent memory — web UI + MCP

**`examples/codebase-memory`** sits on `autumn-memory` (an Axum web UI + an MCP
`--mcp` stdio mode, so any MCP host — Claude Code/Desktop, Cursor — gets the same
tools): it indexes a codebase (autumn-rs itself), searches it (lexical / vector /
hybrid), and walks the call graph (callers / callees / trace) — in the browser or
as MCP tools for Claude.

```bash
cargo run -p codebase-memory -- 127.0.0.1:9001 --root crates/autumn-memory   # web UI at :5180
claude mcp add codebase-memory -- cargo run -q -p codebase-memory -- 127.0.0.1:9001 --mcp
```

Lexical (BM25) recall needs no embedder; vector/hybrid take a caller-supplied
vector (`autumn-memory`'s built-in `embed` module ships a zero-dep hash embedder
+ an optional Model2Vec static-int8 one). Design:
[`docs/autumn_memory_plan.md`](docs/autumn_memory_plan.md).

## Deployment

| Path | Use it for | Docs |
|---|---|---|
| `deploy/baremetal/autumn-deploy` | physical servers, single- or multi-host (systemd; process backend on macOS/containers) | [`docs/baremetal_deploy.md`](docs/baremetal_deploy.md) |
| `deploy/k8s/` | Kubernetes — one image, kustomize base, guarded bootstrap Job; local-disk ENs, network-volume etcd | [`docs/k8s_deploy.md`](docs/k8s_deploy.md) |
| `cluster.sh` | dev / chaos / perf **testing only** (raw process kill for fault injection) | [`docs/ops.md`](docs/ops.md) |

UCX (RDMA) builds are opt-in: `cargo build --release -p autumn-server --features ucx`.
With `TRANSPORT=ucx`, the launchers (`autumn-deploy`, `cluster.sh`) set the UCX env
automatically: clusters bind **RoCE NIC IPs** (loopback is not an RDMA address and is
refused) and get `UCX_TLS=rc_mlx5,ud_mlx5,tcp,self` plus a pinned `UCX_NET_DEVICES` —
one list serves intra-host and cross-host traffic (**never add `posix`/`cma`**: the
posix shm path stalls concurrent ≥64 KiB transfers).
Details: [`docs/baremetal_deploy.md`](docs/baremetal_deploy.md).

## Documentation

- [`docs/ops.md`](docs/ops.md) — **operations & manual-verification manual**
  (fuse runbook, metrics, capacity, chaos suites, rolling restart, CLI reference)
- [`CLAUDE.md`](CLAUDE.md) + `crates/*/CLAUDE.md` — architecture: stream layer
  commit protocol & fencing, LSM partition server, control plane, transports
- [`docs/baremetal_deploy.md`](docs/baremetal_deploy.md) / [`docs/k8s_deploy.md`](docs/k8s_deploy.md) — deployment guides
- [`docs/autumn_memory_plan.md`](docs/autumn_memory_plan.md) / [`docs/autumn_kvcache_plan.md`](docs/autumn_kvcache_plan.md) / [`docs/data_plane_authz_design.md`](docs/data_plane_authz_design.md) — subsystem designs
- [`feature_list.md`](feature_list.md) — the feature ledger

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
