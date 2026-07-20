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
from ._identity import fingerprint_from_sources, read_credential_pair as _read_credential_pair
from ._keys import build_tenant_suffix, full_key, pool_prefix

try:
    from sglang.srt.mem_cache.hicache_storage import (
        HiCacheStorage,
        HiCacheStorageConfig,
    )

    _SGLANG_AVAILABLE = True
except Exception:  # noqa: BLE001
    # Fall back to `object` so the module is importable without sglang
    # installed (smoke tests + standalone usage). Catch broad Exception, not
    # just ImportError: a present-but-broken sglang stack can raise at import
    # (e.g. flashinfer version-check RuntimeError) and must not crash the
    # adapter — the data-plane smoke test imports this without a model stack.
    HiCacheStorage = object  # type: ignore[misc,assignment]
    HiCacheStorageConfig = None  # type: ignore[assignment]
    _SGLANG_AVAILABLE = False

log = logging.getLogger(__name__)

# Reserved pool name. MVP only supports the "kv" pool; the slot is in the
# key format so future v2 multi-pool (mamba / swa) doesn't require a key
# migration. (The shared `kvc/` namespace + tenant suffix live in `_keys`.)
DEFAULT_POOL_NAME = "kv"


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
        # BUG-KVC-TENANT: on the sglang path `model_name` is the served
        # `--model-path` — normally a real identity (sglang has no equivalent
        # of autumn_vllm_loader's fixed config dir), so the default tenant
        # format is UNCHANGED (no key invalidation for existing sglang
        # deployments). `HiCacheStorageConfig` carries no architecture info to
        # fingerprint from, so the escape hatch for deployments where the path
        # is NOT unique (two finetunes at one path, containers mounting
        # different weights at the same mount point) is an explicit
        # extra_config["model_id"], folded in as a fingerprint.
        model_id = extra_config.get("model_id")
        fingerprint = (
            fingerprint_from_sources({"model_id": str(model_id)}) if model_id else None
        )
        self._tenant_suffix = build_tenant_suffix(storage_config, fingerprint)
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
        # `ttl_secs` (default 0 = no expiry): relative TTL applied to every L3
        # page write. Content-addressed keys never invalidate, so a TTL is the
        # only reclamation knob. sglang manages its own existence (its hash
        # table + L2), so a missing/expired L3 key is just a clean miss → no
        # marker-ordering concern (unlike the vLLM connector). A negative value
        # is a misconfiguration — fail fast rather than silently coercing to 0
        # (= never expire), which would mask the error.
        self._ttl_secs = int(extra_config.get("ttl_secs", 0) or 0)
        if self._ttl_secs < 0:
            raise ValueError(f"ttl_secs must be non-negative, got {self._ttl_secs}")
        # F216-E "ucx ⟹ zerocopy": the zero-copy data path (MSG_PUT_ZC write +
        # MSG_GET_ZC read for large pages) is now the DEFAULT whenever the
        # transport is UCX — no opt-in flag. BatchClient derives it from the
        # process transport set by set_transport() above. On TCP the regular
        # path is used. (The old extra_config["zc"] opt-in was removed; KV-cache
        # pages are large so reads cross the ZC size threshold and writes are
        # always ZC on UCX — both win at this size; see UCX_ZC_READ_MIN_BYTES.)
        # F-AUTHZ-BUILTIN (D6-kvc) / F-NS-PRINCIPAL-UNIFIED: same authz wiring as
        # the vLLM connector — `auth_credential_file` in extra_config is the only
        # required key (Option 3's credential file names its own principal),
        # `auth_principal` overrides that name, `auth_tenant` is the retired
        # spelling. The file read fails loudly at startup. Threads to BOTH
        # clients (authz gates reads on protected prefixes too — a
        # credential-less probe client would silently turn every hit into a miss).
        auth_cred_file = extra_config.get("auth_credential_file")
        auth_principal = extra_config.get("auth_principal")
        if auth_principal is None and extra_config.get("auth_tenant") is not None:
            auth_principal = extra_config["auth_tenant"]
            log.warning(
                "extra_config: `auth_tenant` is the retired (pre-Option-3) "
                "spelling — rename it to `auth_principal`"
            )
        # F-NS-PRINCIPAL-UNIFIED: NS-FIRST keys with no tenant segment — the
        # client binds the `kvc` SCOPE and PREPENDS `kvc/` (`_keys.py` emits the
        # relative `{model}/…`).
        auth: dict = {"scope": "kvc"}
        if auth_cred_file:
            file_principal, secret = _read_credential_pair(auth_cred_file)
            auth_principal = auth_principal or file_principal
            if not auth_principal:
                raise ValueError(
                    f"credential file {auth_cred_file!r} carries no principal "
                    f"name (expected a 'principal: <name>' line or "
                    f"'<name>\\n<hex>'); set `auth_principal` explicitly"
                )
            auth["principal"] = auth_principal
            auth["credential"] = secret
        elif auth_principal is not None:
            raise ValueError(
                "extra_config: auth_principal set without auth_credential_file"
            )
        self._batch = autumn.BatchClient(
            endpoint, n_workers, max(1, self._max_inflight), **auth
        )
        # Low-frequency v0 / batch_exists / clear paths use a regular async
        # Client on its own loop thread.
        self._loop0 = new_loop()
        self._client = run_on(self._loop0, lambda: autumn.Client.connect(endpoint, **auth))
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
        return full_key(self._tenant_suffix, hash_str, pool_name)

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

    def _batch_v1(self, keys, host_indices, transfer, verb, err_stat, stat_of):
        """Shared v1 transfer shape for batch_get_v1 / batch_set_v1: resolve
        page views, run `transfer(full_keys, views)`, account per-result via
        `stat_of(ok)`; any exception fails the whole batch under `err_stat`.
        """
        full_keys = [self._full_key(k) for k in keys]
        try:
            starts = _page_start_indices(keys, host_indices)
            views = [self._page_view(s) for s in starts]
            results = list(transfer(full_keys, views))
        except Exception as e:  # noqa: BLE001
            log.debug("batch %s error (n=%d): %r", verb, len(keys), e)
            self._stats[err_stat] += len(keys)
            return [False] * len(keys)
        for ok in results:
            self._stats[stat_of(ok)] += 1
        return results

    def batch_get_v1(self, keys, host_indices, extra_info=None) -> List[bool]:
        return self._batch_v1(
            keys,
            host_indices,
            self._batch.get_into,
            "get_into",
            "get_error",
            lambda ok: "get_hit" if ok else "get_miss",
        )

    def batch_set_v1(self, keys, host_indices, extra_info=None) -> List[bool]:
        return self._batch_v1(
            keys,
            host_indices,
            lambda fk, views: self._batch.put_from(fk, views, self._ttl_secs),
            "put_from",
            "set_error",
            lambda ok: "set_ok" if ok else "set_error",
        )

    def batch_exists(self, keys, extra_info=None) -> int:
        """Return contiguous-prefix length (NOT per-key list).

        Matches the HiCacheStorage `batch_exists` contract documented in
        docs/hicache_l3_interface.md:62-64. Uses `head` (metadata-only) so
        admission probing doesn't transfer the value bytes.

        This runs on sglang's ADMISSION path (per request, before prefill), so
        it must be cheap. The answer is the present prefix FROM key[0], so the
        overwhelmingly common cold/no-hit case (key[0] absent ⇒ 0) is settled by
        a single `head` — probe that first and early-out, instead of fanning out
        one `head` RPC per key (page granularity ⇒ hundreds of keys per prompt;
        that fan-out measured ~4.8 ms/call and was the entire L3 TTFT overhead).
        Only when key[0] IS present do we fan out to count how far the prefix
        reaches (a hit — the case that saves a whole prefill, so worth the RPCs).
        """
        if not keys:
            return 0
        full_keys = [self._full_key(k) for k in keys]
        try:
            if not run(lambda: self._client.head(full_keys[0])):
                return 0  # cold prefix — one probe, no fan-out
            if len(full_keys) == 1:
                return 1
            founds = list(run(lambda: self._client.batch_head(full_keys)))
        except Exception as e:  # noqa: BLE001
            log.debug("batch_exists head error (n=%d): %r", len(keys), e)
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
            run(lambda: self._client.put(full, payload, self._ttl_secs))
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
        # Pool-scoped: must NOT cross into a co-tenant's vLLM (`vllm`) pool.
        prefix = pool_prefix(self._tenant_suffix, DEFAULT_POOL_NAME)
        try:
            n = run(lambda: self._client.batch_delete(prefix))
            log.info("AutumnKVCacheStorage.clear deleted %d keys under %r", n, prefix)
        except Exception as e:  # noqa: BLE001
            log.warning("clear() failed: %r", e)

    def get_stats(self):
        return dict(self._stats)
