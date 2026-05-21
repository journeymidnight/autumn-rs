"""autumn-kvcache sglang HiCache L3 storage backend.

Plug in via:
  --enable-hierarchical-cache
  --hicache-storage-backend dynamic
  --hicache-storage-backend-extra-config '{
    "backend_name":"autumn",
    "module_path":"autumn_kvcache.sglang_backend",
    "class_name":"AutumnKVCacheStorage",
    "interface_v1":1,
    "endpoint":"manager:9001"
  }'

Architecture (docs/autumn_kvcache_plan.md): thin Python adapter over the
existing `autumn` PyO3 client. No local DRAM cache, no sidecar daemon —
partition layer's memtable + block cache serves as the implicit DRAM tier.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import autumn

from ._bridge import run, run_on, new_loop

try:
    from sglang.srt.mem_cache.hicache_storage import (
        HiCacheStorage,
        HiCacheStorageConfig,
    )

    _SGLANG_AVAILABLE = True
except ImportError:
    # Fall back to `object` so the module is importable without sglang
    # installed (smoke tests + standalone usage).
    HiCacheStorage = object  # type: ignore[misc,assignment]
    HiCacheStorageConfig = None  # type: ignore[assignment]
    _SGLANG_AVAILABLE = False

log = logging.getLogger(__name__)

# Reserved namespace prefix. Distinguishes kvcache keys from other autumn
# clients (autumn-fuse, autumn-client) sharing the same partition cluster.
KEY_NAMESPACE = "kvc"

# Reserved pool name. MVP only supports the "kv" pool; the slot is in the
# key format so future v2 multi-pool (mamba / swa) doesn't require a key
# migration.
DEFAULT_POOL_NAME = "kv"


def _build_tenant_suffix(cfg) -> str:
    """Mirror HiCacheFile._get_suffixed_key (docs/hicache_l3_interface.md:207-209).

    Format: f"{model_name}" then optionally "_{tp_rank}_{tp_size}" (skipped
    for MLA models) then "_pp{pp_rank}_{pp_size}" (if pp enabled).
    """
    if cfg is None:
        return "default"
    model = getattr(cfg, "model_name", None) or "unknown"
    # sglang passes the raw `--model-path` value here, which is commonly a
    # filesystem path like `/data/models/Qwen3-VL-32B-Instruct`. Slashes
    # work in autumn keys but produce noisy `kvc//data/...` strings; strip
    # leading slashes and replace inner ones to keep keys readable.
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


def _normalize_indices(host_indices) -> List[int]:
    """Accept torch.Tensor, numpy array, list, or any iterable of ints."""
    if hasattr(host_indices, "tolist"):
        return host_indices.tolist()
    return list(host_indices)


def _page_start_indices(keys, host_indices) -> List[int]:
    """Resolve the starting host_index of each page.

    With page_size>1, sglang passes `page_size` host_indices PER page (the
    indices of every token in the page), so len(host_indices) ==
    len(keys) * page_size. `get_data_page` wants the page's first index and
    returns the whole flattened page. Mirror sglang's own
    `host_indices[i * page_size]` (cache_controller `_generic_page_set`).

    page_size is derived as len(indices) // len(keys) so we don't need to
    track it separately; for page_size==1 this is the identity.
    """
    indices = _normalize_indices(host_indices)
    n = len(keys)
    if n == 0:
        return []
    page_size = max(1, len(indices) // n)
    return [indices[i * page_size] for i in range(n)]


class AutumnKVCacheStorage(HiCacheStorage):  # type: ignore[misc]
    """sglang HiCache L3 backend.

    The constructor signature `(storage_config, extra_kwargs)` matches what
    `StorageBackendFactory` passes when sglang's `--hicache-storage-backend
    dynamic` plugs in a user-supplied class.
    """

    def __init__(self, storage_config=None, extra_kwargs: Optional[dict] = None):
        # `storage_config.extra_config` is the parsed JSON dict from
        # `--hicache-storage-backend-extra-config '{...}'`. sglang's
        # `_create_dynamic_backend` does NOT split out individual keys from
        # extra_config into kwargs; everything lives on the dataclass.
        # `extra_kwargs` is just whatever the factory's caller passed (today
        # always {}), kept for forward-compat.
        extra_kwargs = extra_kwargs or {}
        extra_config = getattr(storage_config, "extra_config", None) or {}
        endpoint = (
            extra_config.get("endpoint")
            or extra_config.get("manager")
            or extra_kwargs.get("endpoint")
            or extra_kwargs.get("manager")
        )
        if not endpoint:
            raise ValueError(
                "AutumnKVCacheStorage requires 'endpoint' in extra_config "
                "(comma-separated list of manager addr:port)"
            )

        self.storage_config = storage_config
        self._tenant_suffix = _build_tenant_suffix(storage_config)
        # Optional transport selection ("tcp" default, or "ucx" for RDMA).
        # Must be set before the first connect; idempotent process-global.
        transport = (extra_config.get("transport") or "tcp").lower()
        if transport != "tcp":
            try:
                autumn.set_transport(transport)
                log.info("autumn transport set to %s", transport)
            except Exception as e:  # noqa: BLE001
                log.warning("set_transport(%s) failed, falling back to tcp: %r", transport, e)
        # Hot path: the GIL-releasing Rust BatchClient. One PyO3 call extracts
        # all dest pointers under the GIL, then py.allow_threads while Rust
        # workers pipeline transfers + memcpy into the pinned pages. This both
        # lifts throughput (~600 MB/s vs ~480 on the asyncio path) and frees
        # the GIL during a batch so sglang's other threads run concurrently.
        #
        # per_worker_cap bounds in-flight depth per worker, keeping UCX under
        # its single-worker rendezvous cliff (≤16). client_workers is the
        # number of Rust worker threads; in-process scaling is currently
        # capped by the shared process-global ucp_context (workers serialize
        # on it), so default 1 — >1 is wired for when per-thread contexts land.
        default_cap = 16 if transport == "ucx" else 64
        self._max_inflight = int(extra_config.get("max_inflight", default_cap))
        n_workers = max(1, int(extra_config.get("client_workers", 1)))
        # F216-E zero-copy path (MSG_PUT_ZC write + MSG_GET_ZC read). Opt-in via
        # extra_config "zc": true. Measured benefit through this adapter is a
        # modest write speedup (~1.05–1.25×, larger at bigger page sizes) and
        # ~parity reads intra-host — the transport-level ~2× write win is diluted
        # by the per-op Python/dispatch/routing overhead. The bigger payoff is
        # expected cross-host RDMA. Default off until the deep UCX multi-worker
        # path is hardened (high client_workers × partitions can collapse UCX).
        zc = bool(extra_config.get("zc", False)) and transport == "ucx"
        try:
            self._batch = autumn.BatchClient(endpoint, n_workers, max(1, self._max_inflight), zc)
        except TypeError:
            # Wheel predates the zc arg — fall back to the regular path.
            self._batch = autumn.BatchClient(endpoint, n_workers, max(1, self._max_inflight))
        self._zc = zc
        # Low-frequency v0 / batch_exists / clear paths use a regular async
        # Client on its own loop thread.
        self._loop0 = new_loop()
        self._client = run_on(self._loop0, lambda: autumn.Client.connect(endpoint))
        self._mem_pool_host = None
        self._stats = {
            "get_hit": 0,
            "get_miss": 0,
            "get_error": 0,
            "set_ok": 0,
            "set_error": 0,
        }
        log.info(
            "AutumnKVCacheStorage connected: endpoint=%s tenant=%s sglang=%s",
            endpoint,
            self._tenant_suffix,
            _SGLANG_AVAILABLE,
        )

    # ── lifecycle ──────────────────────────────────────────────────────────

    def register_mem_pool_host(self, mem_pool_host):
        """Cache the pinned-host KV pool reference.

        v1 batch_get/set use `mem_pool_host.get_data_page(idx, flat=True)` to
        resolve `host_indices` into a buffer view per docs:174.
        """
        self._mem_pool_host = mem_pool_host

    def _full_key(self, hash_str: str, pool_name: str = DEFAULT_POOL_NAME) -> bytes:
        return f"{KEY_NAMESPACE}/{self._tenant_suffix}/{pool_name}/{hash_str}".encode("utf-8")

    def _page_view(self, idx: int):
        """Resolve a host_index to a buffer-protocol view of the pinned page.

        sglang's HostKVCache.get_data_page(idx, flat=True) returns a 1-D
        torch.Tensor view into the pinned host pool. Its dtype matches the
        model's KV cache dtype (commonly bfloat16 or float16).

        autumn's `put_from` / `get_into` use PyBuffer<u8>, so we reinterpret
        the tensor as uint8 before exposing it. `.view(torch.uint8)` is a
        zero-copy bit-reinterpret on the same storage; `.numpy()` then yields
        a numpy view of the same bytes (numpy has no native bfloat16 dtype,
        so calling `.numpy()` directly on a bf16 tensor raises TypeError).
        """
        if self._mem_pool_host is None:
            raise RuntimeError("register_mem_pool_host has not been called")
        page = self._mem_pool_host.get_data_page(int(idx), flat=True)
        # torch.Tensor branch: bf16/fp16/fp32 → uint8 byte view → numpy.
        if hasattr(page, "view") and hasattr(page, "dtype") and hasattr(page, "numpy"):
            try:
                import torch  # lazy; available wherever sglang runs
                return page.view(torch.uint8).numpy()
            except ImportError:
                # Fall through to direct .numpy() — works for already-uint8
                # tensors (numpy/torch agree on uint8).
                return page.numpy()
        # numpy / memoryview / bytes already.
        return page

    # ── v1 zero-copy hot path ──────────────────────────────────────────────

    def batch_get_v1(self, keys, host_indices, extra_info=None) -> List[bool]:
        full_keys = [self._full_key(k) for k in keys]
        try:
            starts = _page_start_indices(keys, host_indices)
            views = [self._page_view(s) for s in starts]
            results = list(self._batch.get_into(full_keys, views))
        except Exception as e:  # noqa: BLE001
            log.debug("batch get_into error (n=%d): %r", len(keys), e)
            self._stats["get_error"] += len(keys)
            return [False] * len(keys)
        for ok in results:
            self._stats["get_hit" if ok else "get_miss"] += 1
        return results

    def batch_set_v1(self, keys, host_indices, extra_info=None) -> List[bool]:
        full_keys = [self._full_key(k) for k in keys]
        try:
            starts = _page_start_indices(keys, host_indices)
            views = [self._page_view(s) for s in starts]
            results = list(self._batch.put_from(full_keys, views))
        except Exception as e:  # noqa: BLE001
            log.debug("batch put_from error (n=%d): %r", len(keys), e)
            self._stats["set_error"] += len(keys)
            return [False] * len(keys)
        for ok in results:
            self._stats["set_ok" if ok else "set_error"] += 1
        return results

    def batch_exists(self, keys, extra_info=None) -> int:
        """Return contiguous-prefix length (NOT per-key list).

        Matches the HiCacheStorage `batch_exists` contract documented in
        docs/hicache_l3_interface.md:62-64. Uses `head` (metadata-only) so
        admission probing doesn't transfer the value bytes.
        """
        full_keys = [self._full_key(k) for k in keys]
        try:
            founds = list(run(lambda: self._client.batch_head(full_keys)))
        except Exception as e:  # noqa: BLE001
            log.debug("batch_head error (n=%d): %r", len(keys), e)
            return 0
        # Contiguous-prefix length: stop at the first missing key.
        count = 0
        for ok in founds:
            if not ok:
                break
            count += 1
        log.debug("batch_exists n=%d hit=%d", len(keys), count)
        return count

    # ── v0 thin wrappers (off hot path once interface_v1=1) ────────────────

    def get(self, key, target_location=None, target_sizes=None):
        full = self._full_key(key)
        try:
            return run(lambda: self._client.get(full))
        except Exception:  # noqa: BLE001
            return None

    def batch_get(self, keys, target_locations=None, target_sizes=None):
        return [self.get(k) for k in keys]

    def set(self, key, value=None, target_location=None, target_sizes=None) -> bool:
        if value is None:
            return False
        full = self._full_key(key)
        try:
            buf = bytes(value) if not isinstance(value, (bytes, bytearray, memoryview)) else value
        except (TypeError, ValueError):
            try:
                buf = value.detach().cpu().numpy().tobytes()  # torch.Tensor path
            except Exception:  # noqa: BLE001
                return False
        try:
            payload = bytes(buf)
            run(lambda: self._client.put(full, payload))
            return True
        except Exception:  # noqa: BLE001
            return False

    def batch_set(self, keys, values=None, target_locations=None, target_sizes=None) -> bool:
        if values is None or len(values) != len(keys):
            return False
        all_ok = True
        for k, v in zip(keys, values):
            all_ok = self.set(k, v) and all_ok
        return all_ok

    def exists(self, key) -> bool:
        full = self._full_key(key)
        try:
            return bool(run(lambda: self._client.head(full)))
        except Exception:  # noqa: BLE001
            return False

    # ── optional ───────────────────────────────────────────────────────────

    def clear(self) -> None:
        prefix = f"{KEY_NAMESPACE}/{self._tenant_suffix}/".encode("utf-8")
        try:
            n = run(lambda: self._client.batch_delete(prefix))
            log.info("AutumnKVCacheStorage.clear deleted %d keys under %r", n, prefix)
        except Exception as e:  # noqa: BLE001
            log.warning("clear() failed: %r", e)

    def get_stats(self):
        return dict(self._stats)
