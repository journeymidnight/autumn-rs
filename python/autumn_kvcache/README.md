# autumn-kvcache

Daemon-less KV-cache **L3 storage backends** built on the autumn-rs partition
layer. Two adapters, one data plane (`autumn.BatchClient` + the sync/async
`_bridge`):

| Adapter | Engine | Contract | Module |
|---------|--------|----------|--------|
| `AutumnKVCacheStorage` | **sglang** | HiCache `HiCacheStorage` (L3) | `autumn_kvcache.sglang_backend` |
| `AutumnKVConnector` | **vLLM** | `KVConnectorBase_V1` (external KV transfer) | `autumn_kvcache.vllm_connector` |

There is **no daemon and no local DRAM cache** — the autumn partition layer is
the only persistence path. KV pages are content-addressed
(`kvc/{tenant}/{pool}/{hash}/{layer}`), so there is no invalidation.

## Install

The native `autumn` PyO3 extension must be on the same interpreter:

```bash
# 1) build + install the native autumn client into your venv
cd python
maturin build --release --interpreter /path/to/venv/bin/python
/path/to/venv/bin/pip install --force-reinstall target/wheels/autumn-*.whl

# 2) install this package (pure-python) — still in python/ from step 1
/path/to/venv/bin/pip install -e autumn_kvcache
```

Both adapter modules import **without** the engine (sglang / vLLM) installed —
the engine import is defensive — so `python -c "from
autumn_kvcache.vllm_connector import AutumnKVConnector"` works for tooling and
the data-plane smoke test regardless.

## Using autumn-kvcache as a vLLM L3 (cross-instance prefix cache)

`AutumnKVConnector` is a native vLLM V1 KV connector (Phase 3a — CPU-offload
path). It offloads each request's block-aligned prompt-prefix KV into autumn on
the producing instance, and loads it back on **any** instance that later sees
the same prefix — a cross-instance / cross-restart prefix cache backed by the
partition layer, with no extra service to run.

> **Pinned vLLM:** verified against **vLLM 0.23.0** (torch 2.11). The
> `KVConnectorBase_V1` signatures drift across vLLM releases (e.g. the external
> connector constructor gained a 3rd `kv_cache_config` argument); the adapter
> forwards it when present and falls back to the 2-arg form, but pin a known
> version in production.

Point both engines at a running autumn cluster's **manager** endpoint and give
each `vllm serve` the connector config:

```bash
KVCFG='{
  "kv_connector":"AutumnKVConnector",
  "kv_connector_module_path":"autumn_kvcache.vllm_connector",
  "kv_role":"kv_both",
  "kv_connector_extra_config":{"endpoint":"127.0.0.1:9001","transport":"tcp"}
}'

# instance 1 (e.g. GPU 0)
CUDA_VISIBLE_DEVICES=0 vllm serve /models/Qwen3-8B --served-model-name qwen3 \
    --port 8101 --enforce-eager --kv-transfer-config "$KVCFG"

# instance 2 (e.g. GPU 1) — a SEPARATE engine, empty local prefix cache
CUDA_VISIBLE_DEVICES=1 vllm serve /models/Qwen3-8B --served-model-name qwen3 \
    --port 8102 --enforce-eager --kv-transfer-config "$KVCFG"
```

`kv_connector_extra_config` keys: `endpoint` (autumn manager `host:port`,
required), `transport` (`tcp` default, or `ucx`), `client_workers`,
`max_inflight`.

### Verifying the cross-instance hit

Send a long prompt (longer than the cache block size) to **instance 1**, then
the **same** prompt to **instance 2**. Instance 2 has never seen it locally, so
any external-cache hit is served from autumn:

```bash
curl -s localhost:8101/v1/completions -H 'Content-Type: application/json' \
  -d '{"model":"qwen3","prompt":"<long prompt>","max_tokens":1}' >/dev/null
curl -s localhost:8102/v1/completions -H 'Content-Type: application/json' \
  -d '{"model":"qwen3","prompt":"<same long prompt>","max_tokens":1}' >/dev/null

# instance 2's connector metrics now show the cross-instance hit:
curl -s localhost:8102/metrics | grep -E \
  'vllm:external_prefix_cache_hits_total|external_kv_transfer'
# vllm:external_prefix_cache_hits_total{...}                 272.0
# vllm:prompt_tokens_by_source_total{source="external_kv_transfer"} 272.0
```

(vLLM 0.23's OpenAI `usage` does **not** surface `cached_tokens` for the
connector path — use the `vllm:external_prefix_cache_*` /
`prompt_tokens_by_source{source="external_kv_transfer"}` metrics, which are the
authoritative external-hit signal.)

A scripted version of this check is `tests/` + the F250-D e2e in the repo.

## Data-plane smoke test (no engine required)

Exercises the autumn-facing core (`_AutumnKVStore`) against a real 1-node
cluster — key format, byte-exact page store/load round-trip, existence probe:

```bash
AUTUMN_KVCACHE_ENDPOINT=127.0.0.1:9001 AUTUMN_KVCACHE_TRANSPORT=tcp \
  python -m tests.test_vllm_dataplane    # run from python/autumn_kvcache/
```

## Architecture notes

- **Stateless adapter / content-addressed keys** — no invalidation, partition
  is the only persistence path.
- **Return fast** — never block past the engine's step budget; the load path is
  synchronous in Phase 3a (per-layer overlap is Phase 3b).
- **Tenant isolation** — keys carry a tenant suffix derived from model + TP/PP,
  so different models / parallel layouts never alias.
- See `docs/autumn_kvcache_plan.md` §13 for the full design.
