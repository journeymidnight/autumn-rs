# How autumn-rs compares

This page compares autumn-rs with three systems people often consider for the
same jobs: [Mooncake](https://github.com/kvcache-ai/Mooncake) (Mooncake Store
and Transfer Engine), [InfiniStore](https://github.com/bytedance/InfiniStore)
and [3FS](https://github.com/deepseek-ai/3FS). It is written from each
project's own README and design docs as of November 2026. Where a project's
docs and code disagree, or a point is not documented, the table says so. If
something here is wrong or out of date, please open an issue.

They are not four takes on one idea. Mooncake and InfiniStore are KV-cache
systems built around moving data fast between GPU hosts. 3FS is a durable
parallel file system. autumn-rs is a durable replicated storage engine with KV,
file and S3 interfaces, which also serves as a KV-cache tier.

## At a glance

| | autumn-rs | Mooncake Store | InfiniStore | 3FS |
|---|---|---|---|---|
| **What it is** | Replicated storage engine: ordered KV, with file, S3 and KV-cache interfaces on top | Distributed KV cache and transfer engine for LLM serving | KV store for KV-cache transfer and reuse in LLM inference | Distributed file system for AI training and inference |
| **Main interface** | KV put/get/range (Rust, Python, CLI); POSIX via FUSE; S3 gateway | Object put/get by key (C++, Python) | Python client: write/read cache over TCP or RDMA | POSIX via FUSE; USRBIO zero-copy native API |
| **Where data lives** | Disk on the storage nodes; optional DRAM tier in the partition servers | DRAM segments contributed by clients; optional SSD offload, shared FS, object storage | Host DRAM pool. The design doc also lists SSD; the main branch implements DRAM only | NVMe SSD on the storage nodes |
| **Durable across a node loss** | Yes. A write is acknowledged once every replica has it; open extents keep 3 copies by default | Best effort. Replicas are configurable per object, and placement returns fewer than asked when space is short | No replication; data on a lost server is gone | Yes. Chain replication (CRAQ), strongly consistent |
| **Erasure coding** | Yes, sealed extents are converted in the background | Not documented | No | Not documented |
| **Control plane** | Manager, leader-elected over etcd | Master service; single master by default, etcd-based HA optional | None; each server stands alone | Cluster managers with an elected primary; metadata in FoundationDB |
| **Transport** | TCP; RDMA via UCX (source build) | TCP, RDMA (IB, RoCE, eRDMA), GPUDirect RDMA, NVLink, EFA, NVMe-oF, CXL and more | TCP, RDMA via ibverbs | RDMA (IB, RoCE); a TCP path exists in the code but is not documented |
| **RDMA required** | No | No (recommended) | No | Yes, per the docs |
| **GPU memory path** | No; data passes through host memory | Yes (GPUDirect RDMA, NVLink) | RDMA from registered GPU tensors | No |
| **Inference integrations** | SGLang HiCache L3, vLLM connector ([autumn_kvcache](https://github.com/journeymidnight/autumn_kvcache)); an LMCache adapter is in preparation | vLLM, SGLang, LMCache, TensorRT-LLM, LMDeploy, NIXL, Dynamo and more | vLLM through LMCache; SGLang in progress | SGLang HiCache, vLLM, LMCache, Mooncake Store adapter |
| **Other workloads** | Model files and checkpoints (FUSE, S3 for model loaders), vector tables (LanceDB over S3) | Model-weight and RL data transfer | None | Training data loading, checkpoints, data processing |
| **Kubernetes** | Manifests in the repo; also bare metal and docker compose | Deployment guide | Not documented | Not documented; systemd units |
| **Language, license** | Rust, Apache-2.0 | C++ (with Rust, Python), Apache-2.0 | C++ and Python, Apache-2.0 | C++ (with Rust), MIT |
| **Public production users** | None stated | Kimi (Moonshot AI) | Not documented | DeepSeek |

## Where each one is strongest

- **Mooncake** has by far the widest hardware and transport coverage and the
  deepest integration with inference engines. It moves KV cache between GPUs
  over NVLink and GPUDirect RDMA, which autumn-rs cannot do. It is built for
  speed first; durability is best effort.
- **InfiniStore** is small and simple: one process, TCP or RDMA, installed
  with pip. It suits a single-node cache in front of LMCache. It keeps no
  replicas, and development has slowed.
- **3FS** is a proven, strongly consistent file system at very large scale,
  with full POSIX semantics and careful recovery design. It expects RDMA and
  several external services (FoundationDB, a cluster manager, a metrics
  store), and has no releases to pin.
- **autumn-rs** is the one to look at when the KV cache, model files and
  vector tables should share one durable, self-healing pool that runs over
  plain TCP and needs no RDMA. Every acknowledged write is on every replica,
  and sealed data can be erasure-coded.

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
