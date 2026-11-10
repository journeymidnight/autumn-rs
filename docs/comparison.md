# How autumn-rs compares

This page compares autumn-rs with three systems people often consider for the
same jobs: [Mooncake](https://github.com/kvcache-ai/Mooncake) (Mooncake Store
and Transfer Engine), [InfiniStore](https://github.com/bytedance/InfiniStore)
and [3FS](https://github.com/deepseek-ai/3FS). It is written from each
project's own README, design docs and code as of November 2026, and from the
adapters in the projects that integrate them. Where a project's docs and code
disagree, or a point is not documented, the table says so. If something here
is wrong or out of date, please open an issue.

They are not four takes on one idea. Mooncake and InfiniStore are KV-cache
systems built around moving data fast between GPU hosts. 3FS is a durable
parallel file system. autumn-rs is a durable replicated storage engine with KV,
file and S3 interfaces, which also serves as a KV-cache tier.

## At a glance

| | autumn-rs | Mooncake Store | InfiniStore | 3FS |
|---|---|---|---|---|
| **What it is** | Replicated storage engine: ordered KV, with file, S3 and KV-cache interfaces on top | Distributed KV cache and transfer engine for LLM serving | KV store for KV-cache transfer and reuse in LLM inference | Distributed file system for AI training and inference |
| **Main interface** | KV put/get/range (Rust, Python, CLI); POSIX via FUSE; S3 gateway | Object put/get by key (C++, Python, Rust, Go) | Python client: write/read cache over TCP or RDMA | POSIX via FUSE; USRBIO zero-copy native API |
| **Where data lives** | Disk on the storage nodes; optional DRAM tier in the partition servers | DRAM segments contributed by clients; optional SSD offload, shared FS, object storage | Host DRAM pool. The design doc also lists SSD; the main branch implements DRAM only | NVMe SSD on the storage nodes |
| **Durable across a node loss** | Yes. A write is acknowledged once every replica has it; open extents keep 3 copies by default | Best effort. Replicas are configurable per object, and placement returns fewer than asked when space is short | No replication; data on a lost server is gone | Yes. Chain replication (CRAQ), strongly consistent |
| **Erasure coding** | Yes, sealed extents are converted in the background | Not documented | No | None found in the storage code; requested in [#409](https://github.com/deepseek-ai/3FS/issues/409) |
| **Control plane** | Manager, leader-elected over etcd | Master service; single master by default, optional HA over etcd, Redis (experimental) or Kubernetes leases | None; each server stands alone | Cluster managers with an elected primary; metadata in FoundationDB |
| **Transport** | TCP; RDMA via UCX (source build) | TCP, RDMA (IB, RoCE, eRDMA), GPUDirect RDMA, NVLink, EFA, NVMe-oF, CXL and more | TCP, RDMA via ibverbs | RDMA (IB, RoCE); TCP documented only for monitor traffic |
| **RDMA required** | No | No (recommended) | No | Yes, per the docs |
| **GPU memory path** | No; data passes through host memory | Yes (GPUDirect RDMA, NVLink) | RDMA from registered GPU tensors | Not documented |
| **Inference integrations** | SGLang HiCache L3, vLLM connector ([autumn_kvcache](https://github.com/journeymidnight/autumn_kvcache)) | vLLM, SGLang, LMCache, TensorRT-LLM, LMDeploy, NIXL, Dynamo and more | vLLM through LMCache per its README (LMCache removed the in-tree adapter in November 2026); SGLang in progress | SGLang HiCache, vLLM and LMCache adapters in those projects; Mooncake Store adapter (experimental) |
| **Other workloads** | Model files and checkpoints (FUSE, S3 for model loaders), vector tables (LanceDB over S3) | Model-weight and RL data transfer | Standalone KV store for other LLM services; no specific workload documented | Training data loading, checkpoints, data processing |
| **Kubernetes** | Manifests in the repo; also bare metal and docker compose | Deployment guide | Not documented | Not documented upstream (systemd units); community Helm chart and operator announced in issues |
| **Language, license** | Rust, Apache-2.0 | C++ (with Rust, Python), Apache-2.0 | C++ and Python, Apache-2.0 | C++ (with Rust), MIT |
| **Public production users** | None stated | Kimi (Moonshot AI) | Not documented | DeepSeek |

## Where each one is strongest

- **Mooncake** has by far the widest hardware and transport coverage and the
  deepest integration with inference engines. It moves KV cache between GPUs
  over NVLink and GPUDirect RDMA, which autumn-rs cannot do. It is built for
  speed first; durability is best effort.
- **InfiniStore** is small and simple: one process, TCP or RDMA, installed
  with pip. It suits a per-node DRAM pool that other inference nodes read
  across hosts. It keeps no replicas, and development has slowed: two commits
  in the past 12 months and no release since April 2025.
- **3FS** is a proven, strongly consistent file system at very large scale,
  with a POSIX interface through FUSE (file length is only eventually
  consistent while a file is being written) and careful recovery design. It
  expects RDMA and two external services (FoundationDB, and ClickHouse for
  metrics), and has no releases to pin.
- **autumn-rs** is the one to look at when the KV cache, model files and
  vector tables should share one durable, self-healing pool that runs over
  plain TCP and needs no RDMA. Every acknowledged write is on every replica,
  and sealed data can be erasure-coded.

## KV-cache benchmark: SGLang HiCache L3

**Single host, measured by this project.** All engines, storage processes and
NICs were on one machine, so RDMA traffic went out and back through the same
NIC. Cross-host numbers will differ. Treat this as one data point, not a
ranking.

**Workload.** Qwen3-VL-32B, TP1, SGLang 0.5.10 with HiCache (`write_through`,
`page_first_direct` layout, prefetch policy `wait_complete`, 64-token pages).
Each request is a ~17.3K-token document followed by a question whose answer is
in the document, so a wrong cache page shows up as a wrong answer. Each
document carries ~4.2 GiB of KV cache.

Each run has three steps:

1. Engine A computes 6 documents and writes their KV cache to the L3 store.
2. A fresh engine B, on another GPU, asks the same 6 questions one at a time.
3. A fresh engine C asks all 6 at once.

B and C have empty GPU and host caches, so a prefix can only come from L3.
Each configuration ran 3 times, interleaved; the table shows the median of the
three per-run medians. Every answer in every run was correct, and B and C
loaded 103,616 tokens from L3 in each run.

**Hardware.** Xeon Platinum 8457C, H200 GPUs, 200 Gb/s RoCE NIC (mlx5),
Intel D7-P5520 NVMe drives. autumn-rs ran 3 extent nodes on 3 drives and one
partition server. Mooncake 0.3.9 ran its master with SGLang contributing 48 GB
of DRAM per engine.

| Configuration | Where the cache lives | B: time to first token, one at a time | C: time to first token, 6 at once |
|---|---|---|---|
| No L3 (recompute) | n/a | 2.43 s | 8.33 s |
| autumn-rs, RDMA (UCX), DRAM tier | 3 replicas on disk, plus a DRAM copy in the partition server | 0.74 s | 1.67 s |
| autumn-rs, RDMA (UCX), no DRAM tier | 3 replicas on disk | 1.83 s | 2.22 s |
| autumn-rs, TCP, DRAM tier | 3 replicas on disk, plus a DRAM copy in the partition server | 1.73 s | 3.85 s |
| Mooncake Store, RDMA | Engine DRAM, 1 copy | 0.81 s | 1.62 s |

What this shows:

- **DRAM tier vs. Mooncake.** autumn-rs's DRAM tier and Mooncake were within
  run-to-run noise of each other. Per-run medians for B were 0.64–1.02 s and
  0.78–1.10 s. The difference is what survives a failure: autumn-rs has already
  written the pages to 3 disks, while Mooncake's single DRAM copy is gone with
  its host.
- **Without the DRAM tier.** autumn-rs is bound by NVMe reads, and is still
  1.3× (B) to 3.7× (C) faster than recomputing.
- **Over TCP.** The client's single receive thread caps a TCP read near
  4.5 GB/s. That is enough to beat recompute, but RDMA is worth it.

**3FS is not in the table.** We deployed 3FS on the same host (3 storage
targets, replication factor 3, FUSE and USRBIO) and ran it through SGLang's
`hf3fs` backend. USRBIO alone read at over 12 GB/s on our setup, but SGLang
took ~13 s per document to load from it. That points to our deployment, not
to 3FS, and we could not find the cause in the time we had. We would rather
show no number than a wrong one. Results from someone who runs 3FS in
production are welcome.

If you want to reproduce a row, open an issue and we will share the scripts.

## What autumn-rs does not do

- **No GPU-direct path.** Pages go through host memory; there is no NVLink or
  GPUDirect RDMA transfer.
- **One site.** Clients dial partition servers and extent nodes directly, on
  one flat network.
- **Young.** The first release is 0.1.0, the community is small, and every
  number in [`perf_baseline.md`](perf_baseline.md) was measured by the
  project itself on clusters of a few nodes.
- **RDMA needs a source build.** The published image and Python wheel are
  TCP-only.

## Sources

- Mooncake: [README](https://github.com/kvcache-ai/Mooncake),
  [Store design](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/design/store/mooncake-store.md),
  [supported protocols](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/getting_started/supported-protocols.md),
  [FAST '25 paper](https://www.usenix.org/system/files/fast25-qin.pdf)
- InfiniStore: [README](https://github.com/bytedance/InfiniStore),
  [design](https://github.com/bytedance/InfiniStore/blob/main/docs/source/design.rst)
- 3FS: [README](https://github.com/deepseek-ai/3FS),
  [design notes](https://github.com/deepseek-ai/3FS/blob/main/docs/design_notes.md),
  [setup guide](https://github.com/deepseek-ai/3FS/blob/main/deploy/README.md)
- autumn-rs: [README](../README.md), [`ops.md`](ops.md),
  [`perf_baseline.md`](perf_baseline.md)
