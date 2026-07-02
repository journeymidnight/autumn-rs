"""Shared key-namespace + tenant-suffix helpers for autumn-kvcache adapters.

Both the sglang HiCache L3 backend (`sglang_backend.py`) and the vLLM
`KVConnectorV1` adapter (`vllm_connector.py`) map an inference engine's KV
pages into the SAME autumn keyspace:

    kvc/{tenant_suffix}/{pool}/{content_hash}

- `kvc/`           — reserved namespace, distinguishes kvcache keys from
                     autumn-fuse / autumn-client keys on the shared cluster.
- `tenant_suffix`  — `(model, tp_rank, tp_size, pp_rank, pp_size)` scoping so two
                     models / parallel ranks never collide.
- `pool`           — `kv` for sglang, `vllm` for the vLLM connector (separate
                     keyspaces; also the v2 multi-pool reservation slot).
- `content_hash`   — the engine's own content-addressed hash (sglang chain hash
                     / vLLM block hash); adapters never re-hash.

See docs/autumn_kvcache_plan.md §5 (sglang) and §13.3 (vLLM).
"""

from __future__ import annotations

# Reserved namespace prefix shared by all autumn-kvcache adapters.
KEY_NAMESPACE = "kvc"


def build_tenant_suffix(cfg) -> str:
    """Build the per-tenant key suffix from a model/parallel config.

    Mirrors sglang's `HiCacheFile._get_suffixed_key`
    (docs/hicache_l3_interface.md:207-209). Accepts any object exposing the
    attributes below (an sglang `HiCacheStorageConfig`, a vLLM-derived shim, or
    a plain namespace), so both adapters share one implementation.

    Format: `{model}` then optionally `_{tp_rank}_{tp_size}` (skipped for MLA
    models, whose KV is rank-independent) then `_pp{pp_rank}_{pp_size}` (only if
    pp_size > 1).
    """
    if cfg is None:
        return "default"
    model = getattr(cfg, "model_name", None) or "unknown"
    # `--model-path` is commonly a filesystem path like
    # `/data/models/Qwen3-VL-32B`; strip/normalise slashes so the key reads as
    # `kvc/Qwen3-VL-32B/...` rather than `kvc//data/models/...`.
    model = str(model).strip("/").replace("/", "_")
    is_mla = bool(getattr(cfg, "is_mla_model", False))
    tp_rank = getattr(cfg, "tp_rank", 0)
    tp_size = getattr(cfg, "tp_size", 1)
    pp_rank = getattr(cfg, "pp_rank", 0)
    pp_size = getattr(cfg, "pp_size", 1)
    parts = [str(model)]
    if not is_mla:
        parts.append(f"{tp_rank}_{tp_size}")
    if pp_size and pp_size > 1:
        parts.append(f"pp{pp_rank}_{pp_size}")
    return "_".join(parts)


def full_key(tenant_suffix: str, content_hash: str, pool: str) -> bytes:
    """Compose the stored partition key (utf-8 bytes)."""
    return f"{KEY_NAMESPACE}/{tenant_suffix}/{pool}/{content_hash}".encode("utf-8")


def pool_prefix(tenant_suffix: str, pool: str) -> bytes:
    """Prefix covering one tenant's keys within a single pool.

    `AutumnKVCacheStorage.clear()` (sglang) must use this so a debug clear of
    the `kv` pool does not also wipe a co-tenant's `vllm` pool.
    """
    return f"{KEY_NAMESPACE}/{tenant_suffix}/{pool}/".encode("utf-8")
