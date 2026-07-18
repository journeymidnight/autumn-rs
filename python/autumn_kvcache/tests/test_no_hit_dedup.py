"""BUG-KVC-NO-HIT regression: scheduler-side store-dedup + kill switches.

Drives `AutumnKVConnector`'s scheduler methods (get_num_new_matched_tokens →
build_connector_meta) against a FAKE `_AutumnKVStore`, so no cluster / vLLM
engine / GPU is needed. The connector is built with `object.__new__` + the
minimal attribute set the scheduler path touches (its real `__init__` connects
to a cluster), which is exactly the surface these tests want to pin.

Pins:
  - a prefix ALREADY present in autumn is NOT re-saved (the write amplification
    the bug reported), even when the local prefix cache covered the prompt;
  - a cold prefix IS saved (first write still happens);
  - a cold-local / warm-external prefix is LOADED (cross-instance hit intact);
  - enabled=false  → complete no-op;
  - enable_save=false → load-only (hits serve, nothing is written).
"""
from __future__ import annotations

import types

import pytest

import autumn_kvcache.vllm_connector as vc
from autumn_kvcache.vllm_connector import AutumnKVConnector, align_to_block_size

BLOCK = 16


class _FakeStore:
    """Records is_present() queries; presence set is caller-controlled."""

    def __init__(self, present: bool = False):
        self._present = present
        self.probes = 0

    def is_present(self, chash: str) -> bool:
        self.probes += 1
        return self._present

    @property
    def tenant(self) -> str:
        return "fake"


def _mk(present=False, enabled=True, enable_save=True) -> AutumnKVConnector:
    c = object.__new__(AutumnKVConnector)  # bypass cluster-connecting __init__
    c._enabled = enabled
    c._enable_save = enable_save
    c._block_size = BLOCK
    c._is_mla = False
    c._store = _FakeStore(present)
    c._reqs_need_load = {}
    c._presence = {}
    c._alloc_blocks = {}
    return c


def _req(rid: str, n_tokens: int):
    # block_ids cover the whole prompt (>= n_tokens/BLOCK blocks).
    n_blocks = (n_tokens // BLOCK) + 2
    return types.SimpleNamespace(
        request_id=rid,
        prompt_token_ids=list(range(n_tokens)),
        block_ids=[list(range(n_blocks))],
    )


def _sched(*reqs):
    return types.SimpleNamespace(scheduled_new_reqs=list(reqs))


def _num_check(n_tokens):
    return align_to_block_size(max(0, n_tokens - 1), BLOCK)


def test_cold_prefix_is_stored():
    c = _mk(present=False)
    r = _req("a", 1333)
    n, is_async = c.get_num_new_matched_tokens(r, 0)
    assert (n, is_async) == (0, False)  # not present → nothing to load
    meta = c.build_connector_meta(_sched(r))
    assert len(meta.requests) == 1 and meta.requests[0].is_store is True


def test_present_prefix_is_not_resaved_even_when_local_covered():
    # The reported bug: local prefix cache serves the repeat (num_computed ==
    # num_check) so external is never LOADED, yet the prefix was re-SAVED.
    c = _mk(present=True)
    r = _req("a", 1333)
    nc = _num_check(1333)
    n, is_async = c.get_num_new_matched_tokens(r, nc)  # local covers everything
    assert (n, is_async) == (0, False)  # no external load
    assert c._store.probes == 1  # probed exactly once
    meta = c.build_connector_meta(_sched(r))
    assert meta.requests == []  # DEDUP: no re-save
    assert c._store.probes == 1  # gnmt verdict reused, no second probe


def test_cross_instance_prefix_is_loaded():
    # Cold local cache (num_computed == 0), warm external → external hit + load.
    c = _mk(present=True)
    r = _req("a", 1333)
    n, is_async = c.get_num_new_matched_tokens(r, 0)
    assert n == _num_check(1333) and is_async is False
    meta = c.build_connector_meta(_sched(r))
    assert len(meta.requests) == 1 and meta.requests[0].is_store is False


def test_same_batch_cold_prefix_is_saved_once():
    # Two DIFFERENT requests with the SAME prompt (same chash), both cold, in one
    # scheduler batch → only the first is scheduled to store; the marker isn't
    # published yet so is_present() is False for both, but the per-batch set
    # de-dups the second (concurrent write amplification).
    c = _mk(present=False)
    a, b = _req("a", 1333), _req("b", 1333)  # identical prompt_token_ids
    meta = c.build_connector_meta(_sched(a, b))
    stores = [r for r in meta.requests if r.is_store]
    assert len(stores) == 1  # saved once, not twice


def test_disabled_is_a_total_noop():
    c = _mk(present=False, enabled=False)
    r = _req("a", 1333)
    assert c.get_num_new_matched_tokens(r, 0) == (0, False)
    assert c._store.probes == 0  # no probe at all
    assert c.build_connector_meta(_sched(r)).requests == []


def test_load_only_serves_hits_but_never_writes():
    # enable_save=False: a cold prefix is NOT stored...
    c = _mk(present=False, enable_save=True)
    c._enable_save = False
    r = _req("a", 1333)
    c.get_num_new_matched_tokens(r, 0)
    assert c.build_connector_meta(_sched(r)).requests == []  # no store
    # ...but a warm-external prefix is still LOADED.
    c2 = _mk(present=True)
    c2._enable_save = False
    r2 = _req("b", 1333)
    c2.get_num_new_matched_tokens(r2, 0)
    meta = c2.build_connector_meta(_sched(r2))
    assert len(meta.requests) == 1 and meta.requests[0].is_store is False


# ── real __init__ kill-switch path (no cluster) ─────────────────────────────
# Exercise the actual constructor (not the object.__new__ bypass) so the
# "enabled=false needs no endpoint and builds no store" guarantee (coco P2#1)
# can't silently regress. `_VLLM_AVAILABLE=False` skips the vLLM super().__init__.

def _fake_cfg(extra):
    return types.SimpleNamespace(
        kv_transfer_config=types.SimpleNamespace(kv_connector_extra_config=extra),
        cache_config=types.SimpleNamespace(block_size=BLOCK),
        model_config=None, parallel_config=None, load_config=None,
    )


def test_init_disabled_needs_no_endpoint_or_backend(monkeypatch):
    monkeypatch.setattr(vc, "_VLLM_AVAILABLE", False)
    built = {"n": 0}

    def _boom(*a, **k):  # a disabled connector must NEVER construct the store
        built["n"] += 1
        raise AssertionError("_AutumnKVStore must not be built when enabled=false")

    monkeypatch.setattr(vc, "_AutumnKVStore", _boom)
    c = AutumnKVConnector(_fake_cfg({"enabled": False}), role=0)  # note: NO endpoint
    assert c._store is None and c._enabled is False and built["n"] == 0
    # scheduler/worker entrypoints are all safe no-ops
    assert c.get_num_new_matched_tokens(_req("a", 1333), 0) == (0, False)
    assert c.build_connector_meta(_sched(_req("a", 1333))).requests == []


def test_init_enabled_requires_endpoint(monkeypatch):
    monkeypatch.setattr(vc, "_VLLM_AVAILABLE", False)
    monkeypatch.setattr(vc, "_AutumnKVStore", lambda *a, **k: None)
    with pytest.raises(ValueError, match="endpoint"):
        AutumnKVConnector(_fake_cfg({}), role=0)  # enabled default True, no endpoint


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
