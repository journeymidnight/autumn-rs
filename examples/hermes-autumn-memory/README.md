# Hermes agent on autumn-rs (vLLM + autumn-kvcache + autumn-memory)

Deployment **glue**, kept out of autumn-rs core on purpose: the core exposes only
the Rust `autumn-memory` lib + the stable `mem/` key schema
(`crates/autumn-memory/src/keys.rs`, `docs/autumn_memory_plan.md §6`). Each agent
builds its own memory ergonomics on that schema — this directory is that glue for
a [Hermes agent](https://github.com/NousResearch/hermes-agent).

```
Hermes agent ──LLM(OpenAI API)──► vLLM (32B-AWQ, single A30)
     │ MemoryProvider hook              │ AutumnKVConnector (KV offload)
     ▼                                  ▼
hermes_provider.AutumnMemoryProvider   autumn cluster (manager/PS/EN)
     │ on autumn.Client + mem/ schema   ▲
     └── AutumnMemory (same file) ───────┘
```

## Files

| File | Tracked? | What |
|---|---|---|
| `hermes_provider.py` | yes | The whole memory plugin in one file: `AutumnMemory` (core mem/-schema ops — episodic / facts / **lexical** search, byte-faithful to the Rust lib) + `AutumnMemoryProvider` (Hermes `MemoryProvider` adapter with a sync bridge). Merged since hermes is the only consumer. |
| `k8s/vllm.yaml`, `k8s/hermes.yaml` | yes | Manifests (vLLM+kvcache, agent). |
| `vllm-kvcache/Dockerfile` | yes | vLLM image (kvcache connector + weight loader); context = repo root, `RUSTUP_DIST_SERVER`/`RUSTUP_UPDATE_ROOT`/`CARGO_MIRROR` build args. |
| `vllm-kvcache/Dockerfile.hermes` | yes | hermes agent + memory provider image; context = repo root, `CARGO_MIRROR` arg. |

Nothing here touches the server-side structure (`crates/`, `deploy/docker/`).

## Build (context = repo root)

```bash
# vLLM + autumn (kvcache connector AND weight loader; pinned vLLM 0.23.0; builds the PyO3 wheel)
docker build -f examples/hermes-autumn-memory/vllm-kvcache/Dockerfile \
  --build-arg BASE_REGISTRY=docker.m.daocloud.io/ \
  --build-arg RUSTUP_DIST_SERVER=https://rsproxy.cn \
  --build-arg RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup \
  --build-arg CARGO_MIRROR=sparse+https://rsproxy.cn/index/ \
  -t <CR>/vllm-autumn:<tag> .

# Hermes agent + memory provider plugin
docker build -f examples/hermes-autumn-memory/vllm-kvcache/Dockerfile.hermes \
  --build-arg BASE_REGISTRY=docker.m.daocloud.io/ \
  --build-arg CARGO_MIRROR=sparse+https://rsproxy.cn/index/ \
  -t <CR>/hermes-autumn:<tag> .
```

## Deploy

```bash
# fill in <CR>/<tag>, <HF_AWQ_REPO>, <HF_TOKEN> in k8s/*.yaml first
kubectl apply -f examples/hermes-autumn-memory/k8s/vllm.yaml    # PVC + weights Job + vLLM + svc
kubectl -n autumn wait --for=condition=complete job/fetch-weights --timeout=1800s
kubectl apply -f examples/hermes-autumn-memory/k8s/hermes.yaml
```

- **Single card** (`nvidia.com/gpu: 1`, no TP) — lands on any GPU node with a free A30. A 32B-AWQ (~18 GB) leaves ~6–8 GB KV → decent context + concurrency. No NCCL / `/dev/shm` / RDMA. (70B would need 2 GPUs = TP=2 on one node — see the earlier note; single-card avoids all of that.)
- **Memory is lexical-only** (`AutumnMemory.search` = bounded keyword TF scan). Add semantic recall later by wiring an embeddings endpoint (`bge-m3`) into `search()` — the schema/callers don't change.
- Everything is internal (`ClusterIP`); nothing is exposed to the internet.

## ⚠️ Validate before trusting it (I couldn't test these here)

1. **Hermes `MemoryProvider` ABC** — `hermes_provider.py` follows the *pre-refactor* reference hook set; confirm the method names/signatures against the `hermes-agent` version you pin in `Dockerfile.hermes`, and set the real agent launch `command`.
2. **vLLM 0.23.0 + AutumnKVConnector** — pinned per the kvcache README; re-validate if you bump vLLM.
3. **The model repo** — pick a real 32B-AWQ HF repo (e.g. `Qwen2.5-32B-Instruct-AWQ`) + HF token. For an *actual Hermes* model on one card, use `Hermes-3-Llama-3.1-8B` in **BF16** (drop `--quantization`, ~16 GB).
4. **KV budget** — comfortable on a single A30 for 32B-AWQ; if you OOM, lower `--max-model-len`.
