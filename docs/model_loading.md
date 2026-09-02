# Fast model loading from autumn (vLLM / SGLang / Transformers)

*How inference servers load weights, and the fastest ways to serve a model that
lives in autumn. Researched 2026-07-03; pin your vLLM/SGLang versions —
`--load-format` value sets and loader internals change between releases.*

## TL;DR

- **There is NO `autumn://` fsspec surface.** `transformers.from_pretrained`
  accepts only a local path or a Hub repo id; vLLM's `runai_streamer` /
  `tensorizer` accept only `s3://`/`gs://`/`http(s)://`, never arbitrary fsspec.
  autumn's file surface is the programmatic **`autumn.Fs`** binding + the
  **`autumn-fuse`** mount (byte transfer), and — for weights — the streaming
  loader below.
- **The way to serve an autumn-resident model: the partition server's hosted S3
  gateway (`--s3-gateway`) plus the engine's stock `--load-format
  runai_streamer`** — one path that works
  on vLLM *and* SGLang *and* anything else that speaks S3, with no engine
  patches to maintain. Measured at **98% of what `autumn.Fs.read_into` itself
  delivers**, because both are bounded by autumn's read path, not by the
  loopback hop between the engine and its sidecar. See **Recipe C**.
- **Fallbacks when you cannot run a sidecar:**
  materialize to local NVMe and serve unmodified (Recipe A — `autumn.Fs`
  download, zero engine code), or a FUSE mount with the *eager* read
  (Recipe B — never the mmap default, 30–50× slower).

## Why model load is slow, and how fast loaders win

Cold load is bounded by **storage/network bandwidth into pinned host memory,
then the host→GPU (H2D) PCIe copy**. The default HF/safetensors path is slow
because it `mmap`s the file and instantiates tensors one at a time under Python,
letting the kernel prefetcher drive I/O — it never saturates the device. Every
fast loader wins the same two ways: (1) **multi-threaded reads that saturate
storage**, and (2) **overlap the storage read with the H2D copy** (GPU pulls
tensor N from a pinned buffer while readers fetch N+1). GDS/GPUDirect Storage
goes further and DMAs storage→GPU, bypassing host DRAM.

**This is exactly autumn's strength** — large-value RDMA/UCX zero-copy
`get_into` into pinned buffers *is* the Run:ai-Model-Streamer mechanism. The
mistake would be to expose weights only as an `mmap` surface over FUSE, which
lands squarely on the slow path.

## vLLM `--load-format` landscape (as of vLLM ~v0.11 `main`)

> The set of values is version-specific and now plugin-extensible via
> `register_model_loader` (a string registry, no longer a hard enum). `gguf`
> was dropped from `main` (now auto-detected by extension). **Verify against
> your version.**

| `--load-format` | backends | path to GPU | notes for autumn |
|---|---|---|---|
| `auto`/`safetensors`/`hf` (default) | local FS, HF Hub | **mmap** → page cache → GPU | slow over FUSE — avoid the mmap path |
| `runai_streamer` / `_sharded` | local FS, **S3/S3-compat**, GCS, Azure | concurrent read → pinned buffer → GPU, overlapped | fast; **no fsspec** — needs an S3-compatible endpoint |
| `tensorizer` | local, **S3**, HTTP(S) | streamed deserialize → GPU | fast; requires pre-serialization; vLLM rejects non-s3/http URIs |
| `fastsafetensors` | local **GDS-capable** FS | **GDS DMA** storage→GPU (else bounce) | true GDS won't engage on generic FUSE; nogds still beats mmap (but see caveat) |
| `sharded_state` | local FS | per-rank shard → GPU | fast for large TP; pair with `save_sharded_state` |
| `dummy` | none | random init | profiling only |

Relevant `LoadConfig` knobs: `load_format`, `download_dir`,
`model_loader_extra_config`, **`safetensors_load_strategy`** (`lazy`=mmap
default / **`eager`**=read whole file to RAM then to GPU / `torchao`),
`safetensors_prefetch_num_threads` (8), `safetensors_prefetch_block_size`
(16 MiB).

**SGLang** looks like vLLM's loader model from the outside (same `--load-format`
spelling, same mmap default) and adds `remote`/`layered` plus GPU-to-GPU RDMA
weight transfer between live instances (R-Fork / P2P — fastest of all, but needs
a live source replica, not a cold start).

⚠️ **It is NOT the same registry.** Re-checked 2026-09-01 against sglang
`a757c1e3f`: `python/sglang/srt/model_loader/` imports nothing from vLLM, there
is no `register_model_loader` anywhere in the tree, and `get_model_loader`
(`model_loader/loader.py:3105`) is a hardcoded if-chain over a closed
`LoadFormat` enum. An out-of-tree loader has nowhere to register, which is why autumn serves
weights through an S3 endpoint rather than a per-engine plugin — SGLang's own
`runai_streamer` needs no patch. **TensorRT-LLM** builds a prebuilt engine loaded from local disk;
no serve-time streaming-from-object-storage path.

## Recipe A — materialize to local, serve unmodified (fallback: any engine, no loader)

Works with every engine, no engine code, full local-disk speed. Download the
model dir to local NVMe straight from the `autumn.Fs` API (no mount, no fsspec):

```python
import autumn, os
fs = autumn.Fs.connect("mgr:9001", direct_read=True)   # ≥64 KiB reads go EN-direct

def download(ino, dst):                                  # autumn dir → local dir
    os.makedirs(dst, exist_ok=True)
    for name, cino, kind in fs.readdir(ino):             # kind: DT_DIR=4, DT_REG=8
        p = os.path.join(dst, name)
        if kind == 4:
            download(cino, p)
        else:
            size, off = fs.getattr(cino)["size"], 0
            with open(p, "wb") as f:
                while off < size:
                    b = fs.read(cino, off, min(8 << 20, size - off))
                    if not b:
                        break
                    f.write(b); off += len(b)

download(fs.resolve("models/llama-3-8b"), "/scratch/llama")
```
```bash
vllm serve /scratch/llama            # or: python -m sglang.launch_server --model-path /scratch/llama
```

Cost: one full copy to local NVMe up front. Best when the node has fast local
scratch and the model is reused across restarts.

## Recipe B — FUSE mount + eager read (no copy-out)

Mount autumn and point the engine at the path — but **force `eager`**, or you
hit the documented mmap-over-FUSE page-fault trap (random per-page FUSE round
trips → 30–50× slowdowns).

```bash
autumn-fuse --manager mgr:9001 --mountpoint /mnt/autumn    # existing native mount
vllm serve /mnt/autumn/models/llama-3-8b \
    --model-loader-extra-config '{"safetensors_load_strategy":"eager"}'
```

`eager` turns random page faults into one big sequential read, which FUSE (and
autumn's large-extent reads) serve well. Never leave weights on the default
`lazy`/mmap path over FUSE. `fastsafetensors --load-format fastsafetensors`
(nogds) also reads a FUSE mount and beats mmap standalone — but vLLM's
integration is reported "slow without GDS installed"; benchmark before relying
on it. **True GDS DMA needs a GDS-native FS (local NVMe / NFSoRDMA / Lustre /
Weka), not generic FUSE.**

## Recipe C — the partition server's S3 gateway + stock `runai_streamer` (RECOMMENDED — every engine)

`autumn-ps --s3-gateway` (default off) serves a read-only, unauthenticated
S3 endpoint over the `fs/` tree from the partition server process, on its own
OS threads. It was once a separate `autumn-s3` binary run as a per-node
sidecar; hosting it in the partition server saves one copy on the ENGINE'S node
— a sidecar received the bytes over the network and sent them again over
loopback, and now that node receives them once. It does not make the reads
local: replica choice is a hash rotation with no locality preference, so on a
multi-node cluster most reads still come from a remote extent node. It serves only what the Run:ai streamer
issues — `ListObjectsV2`, ranged `GetObject`, whole `GetObject` — which is
enough for **SGLang's built-in `--load-format runai_streamer`**, and so gives
the engines that cannot register a loader a concurrent streaming weight path
with no engine patches. Every other S3 tool (`aws s3`, `s3fs`, `datasets`)
reads autumn through it as a side effect.

Buckets are the first level under `fs/`: `s3://models/llama/x.safetensors` is
autumn `fs/models/llama/x.safetensors`.

```bash
# On the storage node, as part of the partition server:
autumn-ps --psid 1 --manager mgr:9001 --s3-gateway --s3-gateway-port 9100 \
          --s3-gateway-credential-file /secrets/fs.cred

pip install runai-model-streamer-s3     # the AWS-SDK plugin; NOT in the base package
export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x   # dummy; never verified
export AWS_ENDPOINT_URL=http://127.0.0.1:9100
python -m sglang.launch_server --model-path s3://models/llama \
       --load-format runai_streamer
```

### Measured (2026-09-01, 3-node local cluster, 2 GiB safetensors shard, loopback)

| path | MB/s | vs native |
|---|---|---|
| `autumn.Fs.read_into`, 8 threads (the raw binding, as a ceiling) | **1327** | 100% |
| plain HTTP replaying the streamer's access pattern | 1430 | 108% |
| **`runai-model-streamer` → gateway (default 8 workers)** | **~1300** | **98%** |
| `runai-model-streamer` → gateway, `--workers 1` | 557 | 42% |
| `runai-model-streamer` → MinIO (local page cache, reference) | 2700–2830 | — |

**The sidecar lands within a couple of percent of the raw binding.** The HTTP
hop costs almost nothing on loopback; both paths are bounded by autumn's read
path, not by the transport between the gateway and the engine.

Getting there took one fix worth knowing about. The gateway was originally a
single compio thread, which capped runai at 557 MB/s while a plain HTTP client
doing the *same* 257 × 8 MiB ranged GETs got 1430. MinIO settled the attribution
— the same runai build reads 2.8 GB/s from it on the same loopback, so the CRT
client was never the problem. Serving the CRT cost the gateway 2.1× more CPU and
4.1× more `io_uring_enter` calls per byte (9608 vs 2329): it drains its sockets
slowly enough that each 8 MiB body write fragments into many partial writes, and
one thread drowns in the syscalls. `--workers N` (SO_REUSEPORT, one compio
runtime and one `FsState` each) fixes it; the knee is at 4 and it plateaus after.

Run it as a **per-GPU-node sidecar**: the long hop (EN → sidecar) keeps RDMA,
and only the loopback hop pays HTTP. A single central gateway would make itself
the bandwidth bottleneck and need its own HA story.

### Verified under real engines (2026-09-01, Qwen3-8B, 16 GiB, one H200)

| engine | path | engine-init seconds | output |
|---|---|---|---|
| vLLM 0.28 | local disk | 23.3 cold / 14.4 with a warm page cache | reference |
| vLLM 0.28 | **S3 gateway** (measured as a sidecar) | **18.5 / 19.0** | byte-identical |
| SGLang 0.5.10 | local disk | 35.7 | reference |
| SGLang 0.5.10 | **S3 gateway** (measured as a sidecar) | **37.1** | byte-identical |

Greedy decoding, same prompts; the completions match the local-disk run token for
token on both engines. The numbers are whole-engine init, not weight load alone —
on a 16 GiB model that is most of it, on a 0.5 B model it is noise.

Operational detail, including the path-style requirement and the `HTTP_PROXY`
trap, is in `docs/ops.md`.

## Datasets

There is no `autumn://` fsspec URL surface (the `autumn_fsspec` facade was
removed 2026-07-09 — thin wrapper, unused). Load datasets the same way as
weights: **materialize to local** via a fuse mount (or `autumn.Fs`), then point
`datasets` at the local path (`load_dataset(..., data_dir=...)` /
`load_from_disk(local_path)`).

## Decision guide

| situation | use |
|---|---|
| **serving an autumn model (the default, any engine)** | **C** — `autumn-ps --s3-gateway` + `--load-format runai_streamer` |
| any S3-speaking tool (`aws s3`, datasets, checkpoints) | **C** — the hosted S3 gateway |
| cannot run a sidecar / quick test | **A** (materialize via `autumn.Fs`) |
| want no copy-out on ephemeral nodes | **B** (FUSE + `eager`) |
| loading a **dataset** (not weights) | materialize to local (`autumn.Fs`), then load |

### Sources
vLLM load formats & `register_model_loader`
(`vllm/model_executor/model_loader/__init__.py`, docs.vllm.ai load config) ·
Run:ai Model Streamer (github.com/run-ai/runai-model-streamer; NVIDIA cold-start
blog) · tensorizer (github.com/coreweave/tensorizer) · fastsafetensors
(github.com/foundation-model-stack/fastsafetensors; arXiv 2505.23072; vLLM PR
#10647) · NVIDIA GPUDirect Storage docs · HF `datasets` filesystems &
`transformers` hub internals (issue #23412: no non-Hub remote models) · fsspec
developer guide · SGLang R-Fork / P2P weight-update blogs.
