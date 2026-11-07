# autumn-rs

**One storage engine for AI data — models, datasets, vector tables and
inference KV cache on one data plane.**

Running an AI platform usually means operating separate storage systems for
models, datasets, vector tables and inference KV cache. autumn-rs puts one
distributed data plane underneath their native interfaces. It is a Rust
storage engine in the shape of the **Windows Azure Storage (WAS)** paper — a
*stream layer* of replicated, append-only extents under an ordered-KV
*partition layer* — and every AI-facing surface is a thin client on it:

```
   Files & checkpoints       Vector tables          Inference KV cache
   ┌────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
   │  autumn-fuse   │   │ LanceDB over S3     │   │   autumn-kvcache     │
   │  POSIX mount   │   │ stock client        │   │ sglang / vLLM        │
   └───────┬────────┘   └──────────┬───────────┘   │ HiCache L3 backend   │
           │                       │               └──────────┬───────────┘
           │        ┌──────────────┘                          │
           ▼        ▼                                         ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │        partition layer  (ordered KV, LSM, split/merge)          │
   │        stream layer     (replicated extents, EC, no local WAL)  │
   └──────────────────────────────────────────────────────────────────┘
        autumn-client CLI/SDK · autumn-manager (etcd-backed control plane)
```

One replication story, one capacity pool, one ops surface: model files,
LanceDB tables and KV-cache pages all land in the same replicated,
erasure-coded, self-healing extents.

## Try it with docker compose

A one-host cluster from the published image, on a Docker host with a Linux
kernel ≥ 5.15: etcd, a manager, three extent nodes,
a partition server and the dashboard.

```bash
docker compose -f deploy/compose/docker-compose.yml up -d
docker compose -f deploy/compose/docker-compose.yml run --rm cli \
    sh -c 'echo hello > /tmp/v && autumn-client --manager manager:9001 --namespace kvc put greeting /tmp/v \
           && autumn-client --manager manager:9001 --namespace kvc get greeting'
open http://127.0.0.1:8799                                        # the dashboard
docker compose -f deploy/compose/docker-compose.yml down -v       # stop and wipe
```

It is for trying things out: authz is off, there is one manager, and the
nodes share one disk.

## Quick start

Linux ≥ 5.15 (io_uring), Rust ≥ 1.95, `etcd` ≥ 3.5 in PATH; on x86_64 a CPU
with SSE4.2. Without systemd (containers, non-root) the deployer runs plain
processes.

```bash
cargo build --release -p autumn-server
cd deploy/baremetal
./autumn-deploy -t topology-singlehost.conf start   # etcd + manager + 3 ENs + 1 PS, bootstrapped
./autumn-deploy -t topology-singlehost.conf status

# Nothing is served until its namespace exists: enable what you use.
AO="../../target/release/autumn-op --cluster-secret-file $HOME/.autumn-deploy/cluster.secret --manager 127.0.0.1:9001"
$AO namespace-create --name fs --parts 6     # autumnfs / FUSE / fsspec, 6 partitions
$AO namespace-create --name kvc --parts 16   # sglang / vLLM KV cache, 16 partitions

AC="../../target/release/autumn-client --manager 127.0.0.1:9001 --namespace kvc \
    --credential-file $HOME/.autumn-deploy/authz/kvc.cred"
echo hello | $AC put mykey /dev/stdin
$AC get mykey                                       # → hello

./autumn-deploy -t topology-singlehost.conf destroy --wipe   # tear down
```

## Using it

**KV — CLI / SDK.** `autumn-client put|get|ls|put-stream` against a namespace;
Rust `autumn-client::ClusterClient` (`crates/client`), Python via
`pip install autumn-rs` (`import autumn`; one abi3 wheel for every CPython ≥ 3.11
on Linux, source in [`python/`](python/README.md)). A failed write is either not applied or
`OutcomeUnknown` (read the key to settle it:
[write outcomes](docs/ops.md#write-outcomes-and-the-tail-seal-on-open)). CLI
reference: [`docs/ops.md`](docs/ops.md).

**Files — POSIX mount** (models, datasets, checkpoints).

```bash
cargo build --release -p autumn-fuse           # needs libfuse3-dev
./target/release/autumn-fuse --manager 127.0.0.1:9001 --mountpoint /mnt/autumn --transport tcp \
    --credential-file $HOME/.autumn-deploy/authz/fs.cred &     # authz is on by default
cp model.safetensors /mnt/autumn/              # a regular filesystem, backed by the cluster
```

Reads go through the kernel page cache and a parallel readahead, so a single
reader gets the mount's parallel bandwidth. Runbook (mount verification, stale
mounts, readahead window, RDMA env, k8s pod sidecar):
[Fuse daemon runbook](docs/ops.md#fuse-daemon-runbook).

**Files — programmatic `autumn.Fs`.** The `autumn` Python extension reads and
writes the same inode layout without a mount (`autumn.Fs.connect("127.0.0.1:9001")`);
large files are chunked at 8 MiB with zero-copy reads. To serve a model that
lives in autumn from vLLM / SGLang, run `autumn-s3` as a per-node sidecar and
point the engine's stock `--load-format runai_streamer` at it — no engine
patches ([S3 gateway](docs/ops.md#s3-gateway--reading-and-writing-autumn-over-s3)).

**Inference KV cache — sglang / vLLM.** The
[`autumn_kvcache`](https://github.com/journeymidnight/autumn_kvcache) adapters
implement sglang's HiCache L3 storage backend and vLLM's `KVConnectorBase_V1`:
the cluster is the shared L3 tier behind the GPU/CPU cache levels, so prefix
caches survive restarts and are shared across instances. Pure Python, no extra
daemon; this repository ships the client they are built on. With the partition
server's [memory tier](docs/ops.md#memory-tier-for-kv-cache---mem-tier-bytes---mem-tier-prefix)
(`--mem-tier-bytes`, `--mem-tier-prefix kvc/`) pages are served from PS DRAM.

**Vector tables — LanceDB over S3.** `autumn-s3` is a read/write S3 endpoint
over the shared `fs/` tree (reads, conditional writes, copy, delete, multipart);
the stock Python LanceDB client runs against it with no patched client and no
Autumn-specific index. A bucket is a first-level directory, created beforehand.

```bash
./target/release/autumnfs --manager 127.0.0.1:9001 --credential-file $HOME/.autumn-deploy/authz/fs.cred mkdir /tables
./target/release/autumn-s3 --manager 127.0.0.1:9001 --port 9100 --credential-file $HOME/.autumn-deploy/authz/fs.cred &
cd examples/lancedb-s3 && uv sync
uv run python workload.py http://127.0.0.1:9100 tables demo    # create, append, search, optimize, verify
```

Details and the captured request contract: [`examples/lancedb-s3/README.md`](examples/lancedb-s3/README.md).

## Numbers

All measured, with setup and method in [`docs/perf_baseline.md`](docs/perf_baseline.md)
and the `autumn_kvcache` README.

| What | Result |
|---|---|
| 4 KiB KV, batched 64 keys/request, TCP, k8s cluster (3 PS, 5 EN on local NVMe) | write ~162K ops/s (p99 0.86 ms), read ~507K ops/s (p99 0.38 ms) |
| 8 MiB writes from a client on a second host, UCX (RDMA) vs TCP | 1,821 vs 414 MB/s (4.4×); UCX 8 MiB reads 10.9 GB/s, ~87% of 100 GbE |
| YCSB-equivalent, 1 KB records, zipfian, 3 PS / 5 EN | B (95/5) ~172K ops/s, read p99 10.1 ms; A (50/50) ~124K ops/s |
| vLLM cross-instance prefix hit, 1.3k tokens | ~0.3 s load vs ~2 s recompute |
| vLLM connector cost when nothing is reused (a 32B VLM, 1k tokens, one GPU) | TTFT +≈6 ms, TPOT ≈0 |
| sglang + a 32B VLM with the PS memory tier | per-request p50 1344 → 980 ms |

## What is underneath

- **Fast by construction** — thread-per-core on io_uring (compio), rkyv zero-copy
  RPC, group commit, adaptive batching; optional **RDMA (UCX)** transport with
  zero-copy reads and writes.
- **The log is the database** — no local WAL files; a min-replica commit protocol
  makes the replicated extents the journal. Open extents are 3-way replicated,
  sealed extents are **erasure-coded** in the background.
- **Self-healing** — WAL bit-rot heals from clean replicas at partition open;
  disk-full recovers in ~2 s; reads route around suspected nodes; partitions
  fail over; `autumn-op repair` / `scrub` rebuild degraded or rotted copies.
- **Elastic** — copy-on-write partition split/merge, with an **auto-policy
  controller** (leader-fenced, config in etcd, default-off) acting on
  split/merge/GC/compaction/EC/repair advice.
- **Access control** — opt-in key-prefix authorization with short-TTL Ed25519
  capability tokens, enforced at the KV layer.
- **Operable** — a [web dashboard](crates/server/src/bin/autumn_dashboard/README.md)
  (keyspace, partitions, servers, nodes and disks, policy, logs, metrics),
  `autumn-op status` / `health` / `df` fleet views, Prometheus `/metrics` on
  every server, speed limits changeable on the running cluster, rolling restart
  with convergence gates.

Architecture: each crate's `crates/*/CLAUDE.md`; subsystem
designs in `docs/*_design.md`.

## Scope

- One site, one flat network: clients dial partition servers and extent nodes
  directly (on k8s, run them in-cluster).
- The container image is TCP-only; UCX needs `hostNetwork`, an RDMA device and
  UCX ≥ 1.17 (build with `--features ucx`; "UCX library" in `docs/ops.md`).
- The memory tier is a cache: a PS restart loses what it held. Use it only for
  data that can be recomputed, which is what the KV-cache adapters store there.
- The dashboard has no caller authentication; keep it behind `kubectl
  port-forward` or your own gateway.

## Deployment

| Path | Use it for | Docs |
|---|---|---|
| `deploy/compose/docker-compose.yml` | trying it on one host | [Try it with docker compose](#try-it-with-docker-compose) |
| `deploy/baremetal/autumn-deploy` | physical servers, single- or multi-host (systemd; plain processes without it) | [`docs/ops.md`](docs/ops.md) "Deployment" → Bare metal |
| `deploy/k8s/deploy.sh` | Kubernetes — one image, kustomize base, guarded bootstrap Job; local-disk ENs, network-volume etcd | [`docs/ops.md`](docs/ops.md) "Deployment" → Kubernetes |
| `cluster.sh` | dev / chaos / perf **testing only** (raw process kill for fault injection) | [`docs/ops.md`](docs/ops.md) |

Extent nodes write large appends with O_DIRECT by default (`--no-direct-io`
turns it off; tmpfs before Linux 6.6 refuses it): "Direct I/O on the extent
nodes" in `docs/ops.md`.

## Documentation

- [`docs/comparison.md`](docs/comparison.md) — how autumn-rs compares with Mooncake, InfiniStore and 3FS
- [`ROADMAP.md`](ROADMAP.md) — what is planned next
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — building, testing and sending a change
- [`docs/ops.md`](docs/ops.md) — operations and manual-verification manual
  (deployment, fuse runbook, metrics, capacity, chaos suites, rolling restart, CLI reference)
- [`docs/perf_baseline.md`](docs/perf_baseline.md) — performance baselines and how they were measured
- `crates/*/CLAUDE.md` — architecture: stream-layer
  commit protocol and fencing, LSM partition server, control plane, transports
- `docs/*_design.md` — subsystem designs (e.g. [`docs/data_plane_authz_design.md`](docs/data_plane_authz_design.md))

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
