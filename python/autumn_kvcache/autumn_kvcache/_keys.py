"""Shared key-namespace + tenant-suffix helpers for autumn-kvcache adapters.

Both the sglang HiCache L3 backend (`sglang_backend.py`) and the vLLM
`KVConnectorV1` adapter (`vllm_connector.py`) map an inference engine's KV
pages into the SAME autumn keyspace:

    kvc/{tenant_suffix}/{pool}/{content_hash}

- `kvc/`           — reserved namespace, distinguishes kvcache keys from
                     autumn-fuse / autumn-client keys on the shared cluster.
- `tenant_suffix`  — `(model, model_fingerprint, tp_rank, tp_size, pp_rank,
                     pp_size)` scoping so two models / parallel ranks never
                     collide. The fingerprint (BUG-KVC-TENANT, `_identity.py`)
                     exists because the model *path* alone is NOT an identity:
                     `autumn_vllm_loader` deployments serve every model from
                     the same fixed local config dir.
- `pool`           — `kv` for sglang, `vllm` for the vLLM connector (separate
                     keyspaces; also the v2 multi-pool reservation slot).
- `content_hash`   — the engine's own content-addressed hash (sglang chain hash
                     / vLLM block hash); adapters never re-hash.

See docs/autumn_kvcache_plan.md §5 (sglang) and §13.3 (vLLM).
"""

from __future__ import annotations

# Reserved namespace prefix shared by all autumn-kvcache adapters.
KEY_NAMESPACE = "kvc"

# The vLLM connector's OWN storage-format / KV-layout version. It is baked
# into every vLLM-pool key (`kvc/{tenant}/vllm/{THIS}/{hash}/{layer}`, see
# `_AutumnKVStore._key`) AND folded into the tenant fingerprint
# (`_identity.vllm_identity_sources`), so bumping it makes every old entry
# unreachable twice over — old bytes can NEVER be loaded by an incompatible
# newer connector under the same content hash; they just stop matching and
# lazily expire. kvcache is a pure cache, so invalidation-by-versioning is the
# correct (zero-migration) upgrade story.
#
# INVARIANT — BUMP THIS whenever the connector's saved KV byte layout or key
# scheme changes. Concretely: any change to `vllm_connector._extract_layer` /
# `_inject_layer` (the serialize/deserialize pair — e.g. the 2026-06-22
# K/V-dim-order fix), to `_byte_view`, or to how keys are composed. Changing
# that layout WITHOUT bumping = same failure class as BUG-KVC-TENANT: shapes
# may still line up, nothing errors, output is silently garbage.
# `test_tenant_identity.py` pins the current value so a bump is always a
# deliberate, reviewed act (it cold-invalidates the whole vLLM pool).
#
# Lives here (not in `vllm_connector.py`, which imports the `autumn` native
# module at top level) so the pure offline unit tests can import it.
VLLM_KV_STORAGE_FORMAT = "v1"


def build_tenant_suffix(cfg, model_fingerprint=None) -> str:
    """Build the per-tenant key suffix from a model/parallel config.

    Mirrors sglang's `HiCacheFile._get_suffixed_key`
    (docs/hicache_l3_interface.md:207-209). Accepts any object exposing the
    attributes below (an sglang `HiCacheStorageConfig`, a vLLM-derived shim, or
    a plain namespace), so both adapters share one implementation.

    Format: `{model}` then optionally `_{model_fingerprint}` then optionally
    `_{tp_rank}_{tp_size}` (skipped for MLA models, whose KV is
    rank-independent) then `_pp{pp_rank}_{pp_size}` (only if pp_size > 1).

    `model_fingerprint` (BUG-KVC-TENANT, see `_identity.py`) carries the
    model's REAL identity (architecture shape + weights source). It is
    REQUIRED on the vLLM connector path, where `model_name` is just the local
    config-dir path and is constant across models under `autumn_vllm_loader`
    — without it two different models silently share one tenant and cross-read
    KV (live incident: Qwen2.5-7B/32B both under `model-cfg_0_1`). When None
    (sglang default, or an unfingerprintable config) the format is exactly the
    pre-fingerprint one, so existing sglang tenants are unchanged.
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
    if model_fingerprint:
        parts.append(str(model_fingerprint))
    if not is_mla:
        parts.append(f"{tp_rank}_{tp_size}")
    if pp_size and pp_size > 1:
        parts.append(f"pp{pp_rank}_{pp_size}")
    return "_".join(parts)


def full_key(_tenant: str, model: str, content_hash: str, pool: str) -> bytes:
    """Compose the stored partition key (utf-8 bytes), RELATIVE to the client's
    ``kvc/{tenant}/`` binding scope.

    F-KEY-NS D7 (Prepend-only): the ``BatchClient`` is bound to
    ``(namespace="kvc", tenant)`` and PREPENDS ``kvc/{tenant}/`` itself, so scope
    is locked by construction and the builder must emit only the RELATIVE part →
    ``{model}/{pool}/{content_hash}`` (the full wire key becomes
    ``kvc/{tenant}/{model}/{pool}/{content_hash}``). ``model`` is the per-model
    instance suffix (arch + weights fingerprint + tp/pp — what
    ``build_tenant_suffix`` returns). ``_tenant`` is now owned by the binding and
    ignored here (kept on the signature only so callers don't churn).
    """
    return f"{model}/{pool}/{content_hash}".encode("utf-8")


def pool_prefix(_tenant: str, model: str, pool: str) -> bytes:
    """Prefix covering one model's keys within a single pool, RELATIVE to
    ``kvc/{tenant}/`` (see `full_key`).

    `AutumnKVCacheStorage.clear()` (sglang) must use this so a debug clear of
    the `kv` pool does not also wipe the same model's `vllm` pool.
    """
    return f"{model}/{pool}/".encode("utf-8")
