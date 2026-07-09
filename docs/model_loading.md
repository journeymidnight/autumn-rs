# Fast model loading from autumn (vLLM / SGLang / Transformers)

*How inference servers load weights, and the fastest ways to serve a model that
lives in autumn. Researched 2026-07-03; pin your vLLM/SGLang versions —
`--load-format` value sets and loader internals change between releases.*

## TL;DR

- **fsspec (`autumn://`) feeds HuggingFace `datasets` directly, but does NOT
  feed model-weight loading.** `transformers.from_pretrained` accepts only a
  local path or a Hub repo id (it materializes remote repos to a local cache
  first); vLLM's `runai_streamer` / `tensorizer` accept only `s3://`/`gs://`/
  `http(s)://`, never arbitrary fsspec. So for **models**, fsspec is a transfer
  and byte-read API, not a load path.
- **The three practical ways to serve an autumn-resident model:**
  1. **Materialize to local NVMe → serve unmodified** — universal, full local
     speed, zero engine code. `fs.get(src, local, recursive=True)`.
  2. **FUSE mount + force the loader's *eager* read** — zero copy-out, but you
     MUST avoid the default mmap-over-FUSE path (30–50× slower).
  3. **Custom vLLM/SGLang streaming loader over autumn's zero-copy `read_into`**
     — highest cold-load throughput; plays to autumn's RDMA/UCX strength.
     Shipped + verified: the `autumn_vllm_loader` package (`--load-format autumn`).

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

**SGLang** reuses vLLM's loader model (same `--load-format`, same mmap default),
and adds `remote`/`layered` plus GPU-to-GPU RDMA weight transfer between live
instances (R-Fork / P2P — fastest of all, but needs a live source replica, not
a cold start). **TensorRT-LLM** builds a prebuilt engine loaded from local disk;
no serve-time streaming-from-object-storage path.

## Recipe A — materialize to local, serve unmodified (recommended default)

Works with every engine, no engine code, full local-disk speed.

```python
import fsspec
fs = fsspec.filesystem("autumn", manager="mgr:9001")
# trailing "/" on both = copy CONTENTS into the dir (put once; get per node)
fs.put("/data/hf/Llama-3-8B/", "models/llama-3-8b/", recursive=True)   # once
fs.get("models/llama-3-8b/", "/scratch/llama/", recursive=True)        # per node
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

## Recipe C — custom streaming loader (highest throughput; shipped + verified)

The **`autumn_vllm_loader`** package registers an out-of-tree vLLM loader
(`@register_model_loader("autumn")`) whose `load_weights` reads safetensors
shards straight from autumn via the zero-copy `Fs.read_into` seam (+ batched EN
direct-read), K parallel readers feeding `model.load_weights(...)` — the
Run:ai-streamer pipeline on autumn's transport. config.json + tokenizer stay on
vLLM's local `model=` path (the weights-from-a-streaming-backend split, like
runai_streamer/tensorizer). One implementation serves both vLLM and SGLang
(shared loader registry).

**Verified end-to-end** (vLLM 0.24, 8×H200): loads `gte-Qwen2-1.5B` from autumn,
embedding **byte-exact vs the default local-disk loader**; over RDMA reaches
~82% of Run:ai Model Streamer's local-page-cache throughput (F-REDIRECT-BATCH).
Package: `python/autumn_vllm_loader/` (+ `tests/run_vllm_e2e.sh`).

```bash
pip install -e python/autumn_vllm_loader   # env needs the `autumn` SDK + torch + vllm
vllm serve /path/to/model_dir --load-format autumn \
    --model-loader-extra-config '{"manager":"mgr:9001","path":"models/llama-3-8b","transport":"ucx","direct_read":true}'
```

The alternative zero-engine-code fast path is an **S3-compatible gateway** in
front of autumn + stock `--load-format runai_streamer` (`AWS_ENDPOINT_URL` +
path-style). Good as a first milestone / A/B baseline; add it if/when an S3
surface exists.

## Datasets (the part fsspec *does* solve today)

```python
import datasets
so = {"manager": "mgr:9001"}
datasets.load_dataset("json", data_files="autumn://raw/train.jsonl", storage_options=so)
ds.save_to_disk("autumn://prepared/ds", storage_options=so)
datasets.load_from_disk("autumn://prepared/ds", storage_options=so)
```
`save_to_disk`/`load_from_disk`/`download_and_prepare` are the reliable
arbitrary-fsspec paths; `load_dataset(..., storage_options=...)` is marked
Experimental but works for `data_files`.

## Decision guide

| situation | use |
|---|---|
| any engine, node has local NVMe, model reused | **A** (materialize) |
| want no copy-out / ephemeral nodes | **B** (FUSE + `eager`) |
| need max cold-load throughput, can ship a loader | **C** (streaming) or S3-gateway + `runai_streamer` |
| loading a **dataset** (not weights) | fsspec `autumn://` directly |

### Sources
vLLM load formats & `register_model_loader`
(`vllm/model_executor/model_loader/__init__.py`, docs.vllm.ai load config) ·
Run:ai Model Streamer (github.com/run-ai/runai-model-streamer; NVIDIA cold-start
blog) · tensorizer (github.com/coreweave/tensorizer) · fastsafetensors
(github.com/foundation-model-stack/fastsafetensors; arXiv 2505.23072; vLLM PR
#10647) · NVIDIA GPUDirect Storage docs · HF `datasets` filesystems &
`transformers` hub internals (issue #23412: no non-Hub remote models) · fsspec
developer guide · SGLang R-Fork / P2P weight-update blogs.
