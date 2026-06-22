"""autumn-kvcache vLLM `KVConnectorV1` adapter (Phase 3a — CPU-offload path).

Native vLLM KV connector that offloads paged KV-cache blocks into the autumn
partition layer, reusing the SAME data plane as the sglang backend
(`autumn.BatchClient` + the sync/async `_bridge`). No daemon, no local DRAM
cache — partition is the only persistence path (docs/autumn_kvcache_plan.md §13).

Plug in via:
  vllm serve <model> \\
    --kv-transfer-config '{
      "kv_connector":"AutumnKVConnector",
      "kv_connector_module_path":"autumn_kvcache.vllm_connector",
      "kv_role":"kv_both",
      "kv_connector_extra_config":{"endpoint":"manager:9001"}
    }'

Design (route A, mirrors vLLM's reference `SharedStorageConnector`):
  - scheduler role: prefix-hash lookup → autumn existence → matched-token count,
    then build per-request load/save metadata.
  - worker role: per-layer extract/inject of the paged KV tensor through autumn
    keyed by `kvc/{tenant}/vllm/{prefix_hash}/{layer_name}`.

Phase 3a target = CPU-offload KV (no GPU staging buffers), synchronous load in
`start_load_kv` (overlap is Phase 3b). The autumn-facing byte I/O is factored
into `_AutumnKVStore`, which is unit-testable WITHOUT vLLM installed.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, List, Optional

import autumn

from ._bridge import new_loop, run, run_on
from ._keys import build_tenant_suffix, full_key

log = logging.getLogger(__name__)

# Separate keyspace pool segment from sglang's "kv" (docs §13.3).
VLLM_POOL_NAME = "vllm"

# When a TTL is configured, the layer pages are written with a longer TTL than
# the `__present__` marker so the marker ALWAYS expires first. The scheduler
# admits a load on the marker (`is_present`); if the marker outlived its layers
# the worker would load some layers and silently `continue` past the expired
# ones, leaving uninitialised KV in the paged cache (a correctness bug). With
# `layer_ttl = marker_ttl + grace` the invariant "marker present ⇒ all layers
# present" holds across both the save ordering (marker written last) and the
# scheduler-probe → worker-load gap. The grace just delays reclamation of
# already-cold pages; content-addressing makes the lingering harmless.
#
# Belt-and-suspenders: the marker is also gated on the layers ACTUALLY saving
# (see _save_failed / wait_for_save) — so a save failure for ANY reason (the
# u64 clamp below, a transport error, value-too-large) never publishes a marker
# without its layers. The grace is the timing guarantee; the gate is the
# write-success guarantee.
_TTL_LAYER_GRACE_SECS = 300

# The native `ttl_secs` arg is a Rust `u64`; a Python int above this fails the
# PyO3 conversion. We saturate (not raise) so an "effectively never expire"
# TTL is honoured rather than erroring — and, critically, so `marker_ttl + grace`
# can never exceed u64 and make the LAYER write fail while the marker (smaller
# TTL) still succeeds. Mirrors the Rust helper's `saturating_add`.
_U64_MAX = (1 << 64) - 1

# ── defensive vLLM import ────────────────────────────────────────────────────
# Module must import without vLLM installed (data-plane smoke test + tooling).
# Catch broad Exception, not just ImportError: a present-but-broken vLLM/torch
# stack can raise at import (cf. the flashinfer version-check RuntimeError that
# bites sglang in some envs) and must not crash adapter import.
try:
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorBase_V1,
        KVConnectorMetadata,
        KVConnectorRole,
    )

    _VLLM_AVAILABLE = True
except Exception:  # noqa: BLE001
    KVConnectorBase_V1 = object  # type: ignore[misc,assignment]
    KVConnectorMetadata = object  # type: ignore[misc,assignment]

    class KVConnectorRole:  # type: ignore[no-redef]
        SCHEDULER = 0
        WORKER = 1

    _VLLM_AVAILABLE = False

if TYPE_CHECKING:  # pragma: no cover - typing only
    import torch


# ── content hashing (mirror SharedStorageConnector foldername scheme) ────────

def align_to_block_size(n: int, block_size: int) -> int:
    """Floor `n` to a whole number of blocks."""
    if block_size <= 0:
        return n
    return (n // block_size) * block_size


def prefix_hash(
    token_ids: List[int],
    num_tokens: int,
    extra_keys: Optional[List[str]] = None,
) -> str:
    """Content hash of the first `num_tokens` token ids + context extra keys.

    Mirrors vLLM's `SharedStorageConnector._generate_foldername_debug`: a stable
    digest over the token-id bytes. Content-addressed ⇒ no invalidation ever
    (docs/autumn_kvcache_plan.md §9). MVP keys the WHOLE block-aligned prompt
    prefix (per-block keying is Phase 3c).

    `extra_keys` MUST include any context that changes the KV beyond the token
    ids — LoRA id, multimodal-input hashes — otherwise two requests with the
    same tokens but different LoRA/mm context would alias the same KV (a
    correctness bug). Ideally these come from vLLM's own `BlockHash` extra keys
    (docs/autumn_kvcache_plan.md §13.3); MVP derives a best-effort subset from
    the request and reconciles exact parity during the e2e (F250-D).
    """
    h = hashlib.sha256()
    # 4-byte little-endian per token id; deterministic across processes.
    h.update(b"".join(int(t).to_bytes(4, "little", signed=False) for t in token_ids[:num_tokens]))
    h.update(f"|{num_tokens}".encode("ascii"))
    for ek in extra_keys or ():
        h.update(b"|")
        h.update(str(ek).encode("utf-8"))
    return h.hexdigest()


def _request_extra_keys(request: Any) -> List[str]:
    """Best-effort LoRA + multimodal context keys from a vLLM request object.

    Field names differ across vLLM versions / between the scheduler `Request`
    and the `NewRequestData` seen in `scheduled_new_reqs`, so read defensively;
    parity between the two is validated in F250-D.
    """
    keys: List[str] = []
    lora = getattr(request, "lora_request", None)
    if lora is not None:
        keys.append("lora:" + str(getattr(lora, "lora_int_id", lora)))
    mm = getattr(request, "mm_hashes", None) or getattr(request, "mm_hash", None)
    if mm:
        if isinstance(mm, (list, tuple)):
            keys.extend("mm:" + str(x) for x in mm)
        else:
            keys.append("mm:" + str(mm))
    return keys


# ── autumn data plane (UNIT-TESTABLE WITHOUT vLLM) ───────────────────────────

class _AutumnKVStore:
    """Thin byte-level KV store over the autumn partition client.

    This is the entire autumn-facing surface the connector relies on; it has no
    vLLM/torch dependency so `tests/test_vllm_dataplane.py` can exercise it
    against a real 1-node cluster.

    Keys: `kvc/{tenant}/vllm/{content_hash}/{layer_name}` (the `content_hash`
    arg to `full_key` carries the `"{hash}/{layer}"` suffix).
    """

    def __init__(
        self,
        endpoint: str,
        tenant_suffix: str,
        transport: str = "tcp",
        n_workers: int = 1,
        max_inflight: Optional[int] = None,
        ttl_secs: int = 0,
    ):
        if not endpoint:
            raise ValueError("_AutumnKVStore requires a non-empty endpoint")
        self._tenant = tenant_suffix
        # Marker TTL = configured ttl_secs; layer TTL = ttl_secs + grace so the
        # marker always expires first (see _TTL_LAYER_GRACE_SECS). 0 = no expiry.
        # Both saturate at u64 so a huge "never expire" TTL is honoured and the
        # layer TTL can never overflow past the marker's (which would fail the
        # layer write while letting the marker publish — a false-positive marker).
        self._marker_ttl = min(max(0, int(ttl_secs)), _U64_MAX)
        self._layer_ttl = (
            min(self._marker_ttl + _TTL_LAYER_GRACE_SECS, _U64_MAX) if self._marker_ttl else 0
        )
        transport = (transport or "tcp").lower()
        if transport != "tcp":
            try:
                autumn.set_transport(transport)
            except Exception as e:  # noqa: BLE001
                log.warning("set_transport(%s) failed, falling back to tcp: %r", transport, e)
                transport = "tcp"
        default_cap = 16 if transport == "ucx" else 64
        cap = int(max_inflight) if max_inflight else default_cap
        # Hot-path: GIL-releasing batched client (ZC on UCX for large pages).
        self._batch = autumn.BatchClient(endpoint, max(1, int(n_workers)), max(1, cap))
        # Low-frequency existence probing on its own loop thread.
        self._loop = new_loop()
        self._client = run_on(self._loop, lambda: autumn.Client.connect(endpoint))

    # Reserved layer-name sentinel marking "all layers for this prefix were
    # saved". The scheduler probes THIS (not a real layer name) so existence
    # is independent of the worker's model-specific layer naming.
    PRESENT_MARKER = "__present__"

    def _key(self, content_hash: str, layer_name: str) -> bytes:
        return full_key(self._tenant, f"{content_hash}/{layer_name}", VLLM_POOL_NAME)

    def exists(self, content_hash: str, layer_name: str) -> bool:
        try:
            return bool(run(lambda: self._client.head(self._key(content_hash, layer_name))))
        except Exception as e:  # noqa: BLE001
            log.debug("exists() error: %r", e)
            return False

    def mark_present(self, content_hash: str) -> bool:
        """Write the completion marker after all layers of a prefix are saved.

        The marker carries the configured (shorter) TTL so it expires before its
        layers — see _TTL_LAYER_GRACE_SECS.
        """
        try:
            run(lambda: self._client.put(
                self._key(content_hash, self.PRESENT_MARKER), b"1", self._marker_ttl
            ))
            return True
        except Exception as e:  # noqa: BLE001
            log.debug("mark_present error: %r", e)
            return False

    def is_present(self, content_hash: str) -> bool:
        """Scheduler-side existence probe — layer-name independent."""
        return self.exists(content_hash, self.PRESENT_MARKER)

    def save_layers(self, content_hash: str, layer_names: List[str], views: List[Any]) -> List[bool]:
        """Batched zero-copy put of one page per (content_hash, layer).

        `views` are buffer-protocol objects (numpy uint8 / torch byte view);
        len(views) == len(layer_names).
        """
        keys = [self._key(content_hash, ln) for ln in layer_names]
        try:
            return list(self._batch.put_from(keys, views, self._layer_ttl))
        except Exception as e:  # noqa: BLE001
            log.debug("save_layers put_from error (n=%d): %r", len(keys), e)
            return [False] * len(keys)

    def load_layers(self, content_hash: str, layer_names: List[str], dests: List[Any]) -> List[bool]:
        """Batched zero-copy get into per-layer destination buffers."""
        keys = [self._key(content_hash, ln) for ln in layer_names]
        try:
            return list(self._batch.get_into(keys, dests))
        except Exception as e:  # noqa: BLE001
            log.debug("load_layers get_into error (n=%d): %r", len(keys), e)
            return [False] * len(keys)


# ── connector metadata ───────────────────────────────────────────────────────

@dataclass
class _ReqMeta:
    req_id: str
    content_hash: str
    # Flattened slot indices (block_id*block_size + offset) for this request's
    # full block-aligned prefix; used to gather/scatter the paged KV tensor.
    slot_mapping: Any  # torch.Tensor at runtime
    is_store: bool


@dataclass
class AutumnConnectorMetadata(KVConnectorMetadata):  # type: ignore[misc]
    """Per-step metadata: which requests to load from / save to autumn."""

    requests: List[_ReqMeta] = field(default_factory=list)

    def add(self, req_id: str, content_hash: str, slot_mapping: Any, is_store: bool) -> None:
        self.requests.append(_ReqMeta(req_id, content_hash, slot_mapping, is_store))


# ── torch helpers (worker side, vLLM runtime only) ──────────────────────────

def _slot_mapping_for_blocks(block_ids: List[int], block_size: int, num_tokens: int):
    """Compute the flattened slot indices for `num_tokens` over `block_ids`.

    Mirrors SharedStorageConnector: `offsets + block_id*block_size`, flattened,
    truncated to `num_tokens`.
    """
    import torch

    block_ids_t = torch.tensor(block_ids, dtype=torch.long)
    offsets = torch.arange(block_size, dtype=torch.long)
    slots = (offsets.reshape(1, block_size) + block_ids_t.reshape(-1, 1) * block_size).flatten()
    return slots[:num_tokens]


def _extract_layer(kv_layer: "torch.Tensor", slot_mapping, is_mla: bool):
    """Gather the KV for `slot_mapping` from a paged layer → contiguous tensor.

    Mirrors SharedStorageConnector.extract_kv_from_layer.
    """
    num_slots = slot_mapping.shape[0]
    if is_mla:
        return kv_layer.reshape(-1, *kv_layer.shape[2:])[slot_mapping, ...].contiguous()
    # non-MLA: [2, num_pages*page_size, ...]
    flat = kv_layer.reshape(2, -1, *kv_layer.shape[3:])
    return flat[:, slot_mapping, ...].contiguous()


def _inject_layer(kv_layer: "torch.Tensor", value: "torch.Tensor", slot_mapping, is_mla: bool) -> None:
    """Scatter a contiguous KV tensor back into the paged layer at `slot_mapping`.

    Mirrors SharedStorageConnector.inject_kv_into_layer.
    """
    if is_mla:
        kv_layer.reshape(-1, *kv_layer.shape[2:])[slot_mapping, ...] = value
    else:
        flat = kv_layer.reshape(2, -1, *kv_layer.shape[3:])
        flat[:, slot_mapping, ...] = value


def _byte_view(t: "torch.Tensor"):
    """uint8 numpy view of a CPU tensor's bytes (zero-copy bit-reinterpret)."""
    import torch

    return t.contiguous().view(torch.uint8).numpy()


# ── the connector ────────────────────────────────────────────────────────────

class AutumnKVConnector(KVConnectorBase_V1):  # type: ignore[misc]
    """vLLM `KVConnectorV1` backed by autumn-rs partition layer (Phase 3a)."""

    def __init__(self, vllm_config: Any, role: Any, kv_cache_config: Any = None):
        # External V1 KV connectors take `kv_cache_config` as a 3rd ctor arg
        # and MUST forward it to super().__init__() (vLLM >= 0.10-ish enforces
        # this with a pydantic validation error); older vLLM used the 2-arg
        # form. Accept both — pass the 3rd arg when present, fall back to the
        # 2-arg shape on TypeError — so the adapter tracks the pinned vLLM
        # without a hard version floor (F250-D reconciliation).
        if _VLLM_AVAILABLE:
            try:
                super().__init__(
                    vllm_config=vllm_config, role=role, kv_cache_config=kv_cache_config
                )
            except TypeError:
                super().__init__(vllm_config=vllm_config, role=role)
        self._role = role
        self._vllm_config = vllm_config
        self._kv_cache_config = kv_cache_config

        extra = {}
        block_size = 16
        tenant_cfg = None
        try:
            kt = getattr(vllm_config, "kv_transfer_config", None)
            extra = dict(getattr(kt, "kv_connector_extra_config", None) or {})
            block_size = int(getattr(getattr(vllm_config, "cache_config", None), "block_size", 16) or 16)
            tenant_cfg = _tenant_cfg_from_vllm(vllm_config)
        except Exception as e:  # noqa: BLE001
            log.warning("AutumnKVConnector: partial vllm_config (%r); using defaults", e)

        endpoint = extra.get("endpoint") or extra.get("manager")
        if not endpoint:
            raise ValueError(
                "AutumnKVConnector requires 'endpoint' in kv_connector_extra_config"
            )
        self._block_size = block_size
        self._tenant_suffix = build_tenant_suffix(tenant_cfg)
        self._is_mla = bool(getattr(tenant_cfg, "is_mla_model", False))
        transport = (extra.get("transport") or "tcp").lower()
        # `ttl_secs` (default 0 = no expiry) bounds how long an offloaded prefix
        # lives in autumn before lazy expiry reclaims it. Content-addressed keys
        # never invalidate, so a TTL is the only reclamation knob. A negative
        # value is a misconfiguration — fail fast rather than silently coercing
        # it to 0 (= never expire), which would mask the error and let storage
        # grow unbounded.
        ttl_secs = int(extra.get("ttl_secs", 0) or 0)
        if ttl_secs < 0:
            raise ValueError(f"ttl_secs must be non-negative, got {ttl_secs}")
        self._store = _AutumnKVStore(
            endpoint,
            self._tenant_suffix,
            transport=transport,
            n_workers=max(1, int(extra.get("client_workers", 1))),
            max_inflight=extra.get("max_inflight"),
            ttl_secs=ttl_secs,
        )

        # scheduler-side state: req_id -> (content_hash, num_tokens) for cache hits.
        self._reqs_need_load: dict = {}
        # req_id -> block ids recorded at alloc time (authoritative for load).
        self._alloc_blocks: dict = {}
        # worker-side state.
        self._kv_caches: dict = {}
        self._layer_order: List[str] = []
        self._meta: Optional[AutumnConnectorMetadata] = None
        # req_ids whose layer save FAILED this step — `wait_for_save` must NOT
        # publish their `__present__` marker (a marker without its layers is a
        # false positive the scheduler would later admit into a partial load).
        self._save_failed: set = set()
        log.info(
            "AutumnKVConnector role=%s tenant=%s block_size=%d vllm=%s",
            role, self._tenant_suffix, self._block_size, _VLLM_AVAILABLE,
        )

    # ── scheduler side ──────────────────────────────────────────────────────

    def get_num_new_matched_tokens(self, request: Any, num_computed_tokens: int):
        """Return (num_external_tokens_loadable, load_is_async)."""
        token_ids = list(getattr(request, "prompt_token_ids", []) or [])
        num_check = align_to_block_size(max(0, len(token_ids) - 1), self._block_size)
        if num_check <= num_computed_tokens:
            return 0, False
        chash = prefix_hash(token_ids, num_check, _request_extra_keys(request))
        # Probe the layer-name-independent completion marker, NOT a specific
        # layer key: the scheduler does not know the worker's model-specific
        # layer names (register_kv_caches is worker-side only).
        if self._store.is_present(chash):
            self._reqs_need_load[_req_id(request)] = (chash, num_check)
            # Phase 3a loads synchronously in start_load_kv → not async.
            return num_check - num_computed_tokens, False
        return 0, False

    def update_state_after_alloc(self, request: Any, blocks: Any, num_external_tokens: int) -> None:
        # Record the blocks allocated for this request so build_connector_meta
        # has an authoritative slot source (vs. guessing from scheduled_new_reqs).
        ids = _blocks_to_ids(blocks)
        if ids:
            self._alloc_blocks[_req_id(request)] = ids

    def build_connector_meta(self, scheduler_output: Any) -> AutumnConnectorMetadata:
        meta = AutumnConnectorMetadata()
        for new_req in getattr(scheduler_output, "scheduled_new_reqs", []) or []:
            req_id = _req_id(new_req)
            # Prefer alloc-time block ids; fall back to the new-req's own field.
            block_ids = self._alloc_blocks.pop(req_id, None)
            if not block_ids:
                block_ids = _flatten_block_ids(getattr(new_req, "block_ids", None))
            token_ids = list(getattr(new_req, "prompt_token_ids", []) or [])

            if req_id in self._reqs_need_load:
                chash, num_tokens = self._reqs_need_load.pop(req_id)
                is_store = False
            else:
                num_tokens = align_to_block_size(max(0, len(token_ids) - 1), self._block_size)
                if num_tokens <= 0:
                    continue
                chash = prefix_hash(token_ids, num_tokens, _request_extra_keys(new_req))
                is_store = True

            slots = _slot_mapping_for_blocks(block_ids, self._block_size, num_tokens)
            # Guard: never save/load empty KV. An empty slot mapping means we
            # could not resolve real block ids for this request — skip it loudly
            # rather than silently persisting/restoring zero bytes.
            if _slot_len(slots) == 0:
                log.warning(
                    "skip req=%s (%s): empty slot_mapping (num_tokens=%d, blocks=%d)",
                    req_id, "store" if is_store else "load", num_tokens, len(block_ids),
                )
                continue
            meta.add(req_id, chash, slots, is_store=is_store)
        return meta

    def request_finished(self, request: Any, block_ids: List[int]):
        # Synchronous save (Phase 3a): blocks are free to release immediately.
        self._alloc_blocks.pop(_req_id(request), None)
        return False, None

    # ── worker side ─────────────────────────────────────────────────────────

    def register_kv_caches(self, kv_caches: "dict") -> None:
        self._kv_caches = kv_caches
        self._layer_order = list(kv_caches.keys())

    def bind_connector_metadata(self, connector_metadata: Any) -> None:
        self._meta = connector_metadata

    def clear_connector_metadata(self) -> None:
        self._meta = None

    def start_load_kv(self, forward_context: Any, **kwargs: Any) -> None:
        """Synchronously load every cached request's KV into the paged cache.

        Phase 3a: load all layers up-front (no per-layer overlap). Each
        (content_hash, layer) page is fetched into a host staging buffer and
        scattered into the layer tensor at slot_mapping.
        """
        import numpy as np
        import torch

        meta = self._meta
        if meta is None:
            return
        for rm in meta.requests:
            if rm.is_store:
                continue
            if _slot_len(rm.slot_mapping) == 0:
                continue
            for layer_name, kv_layer in self._kv_caches.items():
                template = _extract_layer(kv_layer, rm.slot_mapping, self._is_mla)
                staging = np.empty(template.numel() * template.element_size(), dtype=np.uint8)
                ok = self._store.load_layers(rm.content_hash, [layer_name], [staging])[0]
                if not ok:
                    # Anomalous: this request was admitted because the scheduler
                    # saw the `__present__` marker, so every layer should be
                    # present. A miss here means a layer expired while its marker
                    # lived (a TTL grace breach — see _TTL_LAYER_GRACE_SECS) or a
                    # backend fault. The position keeps its (uninitialised) paged
                    # KV; surface it loudly rather than swallowing at debug.
                    log.warning(
                        "external KV load miss after positive presence: req=%s layer=%s "
                        "(prefix partially uncached — possible TTL grace breach)",
                        rm.req_id, layer_name,
                    )
                    continue
                value = torch.from_numpy(staging).view(template.dtype).reshape(template.shape)
                _inject_layer(kv_layer, value, rm.slot_mapping, self._is_mla)

    def wait_for_layer_load(self, layer_name: str) -> None:
        # Phase 3a loads synchronously in start_load_kv; nothing to await.
        return None

    def save_kv_layer(self, layer_name: str, kv_layer: "torch.Tensor", attn_metadata: Any, **kwargs: Any) -> None:
        meta = self._meta
        if meta is None:
            return
        for rm in meta.requests:
            if not rm.is_store:
                continue
            gathered = _extract_layer(kv_layer, rm.slot_mapping, self._is_mla).detach().cpu()
            ok = self._store.save_layers(rm.content_hash, [layer_name], [_byte_view(gathered)])
            if not ok or not ok[0]:
                # Any layer failing taints the whole prefix: record it so the
                # marker is withheld (an all-or-nothing presence guarantee).
                self._save_failed.add(rm.req_id)
                log.warning(
                    "layer save failed req=%s layer=%s — withholding presence marker",
                    rm.req_id, layer_name,
                )

    def wait_for_save(self) -> None:
        # put_from is awaited inside save_layers (write-through, ACK = durable).
        # All layers of each store-req are now persisted, so publish the
        # layer-name-independent completion marker the scheduler probes.
        meta = self._meta
        if meta is None:
            return
        for rm in meta.requests:
            # Gate on save success: a request whose any layer failed
            # (`_save_failed`) must NOT get a marker — else the next instance
            # sees a present prefix it can't fully load.
            if rm.is_store and _slot_len(rm.slot_mapping) > 0 and rm.req_id not in self._save_failed:
                self._store.mark_present(rm.content_hash)
        self._save_failed.clear()

    def get_finished(self, finished_req_ids: set):
        # No async send/recv in Phase 3a.
        return None, None


# ── vllm_config → tenant cfg shim ────────────────────────────────────────────

def _tenant_cfg_from_vllm(vllm_config: Any):
    """Adapt a VllmConfig into the duck-typed object `build_tenant_suffix` wants."""
    import types

    model = None
    tp_rank = tp_size = pp_rank = pp_size = None
    try:
        model = getattr(getattr(vllm_config, "model_config", None), "model", None)
    except Exception:  # noqa: BLE001
        pass
    try:
        par = getattr(vllm_config, "parallel_config", None)
        tp_size = getattr(par, "tensor_parallel_size", 1)
        pp_size = getattr(par, "pipeline_parallel_size", 1)
        tp_rank = getattr(par, "rank", 0)
    except Exception:  # noqa: BLE001
        pass
    return types.SimpleNamespace(
        model_name=model,
        tp_rank=tp_rank or 0,
        tp_size=tp_size or 1,
        pp_rank=pp_rank or 0,
        pp_size=pp_size or 1,
        is_mla_model=False,
    )


def _flatten_block_ids(block_ids: Any) -> List[int]:
    """vLLM passes block_ids as a per-group list-of-lists or a flat list."""
    if block_ids is None:
        return []
    out: List[int] = []
    for b in block_ids:
        if isinstance(b, (list, tuple)):
            out.extend(int(x) for x in b)
        else:
            out.append(int(b))
    return out


def _req_id(request: Any) -> Any:
    """Stable request identifier across `Request` / `NewRequestData` shapes."""
    rid = getattr(request, "request_id", None)
    if rid is None:
        rid = getattr(request, "req_id", None)
    return rid if rid is not None else id(request)


def _blocks_to_ids(blocks: Any) -> List[int]:
    """Extract block ids from a vLLM `KVCacheBlocks` (or list) defensively."""
    if blocks is None:
        return []
    # KVCacheBlocks exposes get_block_ids() (often -> per-group list-of-lists).
    getter = getattr(blocks, "get_block_ids", None)
    if callable(getter):
        try:
            return _flatten_block_ids(getter())
        except Exception:  # noqa: BLE001
            pass
    for attr in ("block_ids", "blocks"):
        val = getattr(blocks, attr, None)
        if val is not None:
            return _flatten_block_ids(val)
    try:
        return _flatten_block_ids(blocks)
    except Exception:  # noqa: BLE001
        return []


def _slot_len(slot_mapping: Any) -> int:
    """Length of a slot_mapping (torch.Tensor / numpy / list), 0 if empty/None."""
    if slot_mapping is None:
        return 0
    try:
        return int(len(slot_mapping))
    except TypeError:
        n = getattr(slot_mapping, "numel", None)
        return int(n()) if callable(n) else 0
