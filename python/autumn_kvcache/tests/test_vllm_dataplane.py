"""autumn-kvcache vLLM connector — data-plane smoke test (NO vLLM dependency).

Exercises the autumn-facing core the vLLM connector relies on — `_AutumnKVStore`
(key format + per-layer zero-copy save/load + existence) — against an
externally-running 1-node autumn cluster. This is the half of F250 that does
NOT need a model / vLLM runtime; the full e2e (cross-instance prefix hit) is
F250-D in an isolated venv.

Setup before running:
    cd python && maturin develop --release         # builds the `autumn` module
    pip install -e python/autumn_kvcache           # installs this package
    ./cluster.sh reset 1                           # start a 1-node cluster
    AUTUMN_KVCACHE_ENDPOINT=127.0.0.1:9001 python -m autumn_kvcache.tests.test_vllm_dataplane

Exit code: 0 on pass, non-zero on failure.
"""

from __future__ import annotations

import logging
import os
import sys

import numpy as np

from autumn_kvcache._keys import KEY_NAMESPACE
from autumn_kvcache.vllm_connector import (
    VLLM_POOL_NAME,
    _AutumnKVStore,
    align_to_block_size,
    prefix_hash,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger("vllm-dataplane")


def _page(fill: int, nbytes: int) -> np.ndarray:
    a = np.empty(nbytes, dtype=np.uint8)
    a.fill(fill & 0xFF)
    return a


def main() -> int:
    endpoint = os.environ.get("AUTUMN_KVCACHE_ENDPOINT", "127.0.0.1:9001")
    transport = os.environ.get("AUTUMN_KVCACHE_TRANSPORT", "tcp")
    log.info("connecting to autumn manager at %s (transport=%s)", endpoint, transport)

    tenant = "vllm-dp-model_0_1"
    store = _AutumnKVStore(endpoint, tenant, transport=transport)

    # ── 1. pure-function sanity (no cluster) ─────────────────────────────────
    assert align_to_block_size(31, 16) == 16
    assert align_to_block_size(32, 16) == 32
    assert align_to_block_size(15, 16) == 0
    # deterministic + prefix-sensitive
    h_abc = prefix_hash([1, 2, 3, 4, 5], 3)
    assert h_abc == prefix_hash([1, 2, 3, 9, 9], 3), "hash must depend only on first num_tokens"
    assert h_abc != prefix_hash([1, 2, 4], 3), "different tokens → different hash"
    log.info("pure-function (align/prefix_hash) OK")

    # ── 2. key format ────────────────────────────────────────────────────────
    k = store._key("deadbeef", "layer.7")
    expected = f"{KEY_NAMESPACE}/{tenant}/{VLLM_POOL_NAME}/deadbeef/layer.7".encode()
    assert k == expected, f"key mismatch: {k!r} != {expected!r}"
    log.info("key format OK: %r", k)

    # ── 3. per-layer zero-copy save/load round-trip ──────────────────────────
    chash = prefix_hash([10, 20, 30, 40], 4)
    layers = ["layer.0", "layer.1", "layer.2", "layer.3"]
    page_bytes = 64 * 1024  # 64 KiB — crosses the UCX ZC threshold
    src = [_page(0xC0 + i, page_bytes) for i in range(len(layers))]

    save_ok = store.save_layers(chash, layers, src)
    assert all(save_ok), f"save_layers failures: {save_ok}"
    log.info("save_layers OK: %d layers stored (%d B each)", len(layers), page_bytes)

    # load into fresh zeroed buffers, verify byte-exact
    dst = [np.zeros(page_bytes, dtype=np.uint8) for _ in layers]
    load_ok = store.load_layers(chash, layers, dst)
    assert all(load_ok), f"load_layers misses: {load_ok}"
    for i, (d, s) in enumerate(zip(dst, src)):
        if not np.array_equal(d, s):
            log.error("layer %d mismatch: first=%#x last=%#x expected=%#x",
                      i, int(d[0]), int(d[-1]), int(s[0]))
            return 1
    log.info("load_layers OK: %d layers round-tripped byte-exact", len(layers))

    # ── 4. existence semantics ───────────────────────────────────────────────
    assert store.exists(chash, "layer.0") is True
    assert store.exists(chash, "layer.999") is False
    assert store.exists(prefix_hash([7, 7, 7], 3), "layer.0") is False
    log.info("exists() semantics OK")

    # ── 4b. layer-name-independent presence marker (scheduler probe) ─────────
    # The scheduler probes is_present(), which must be False until the worker
    # publishes the marker in wait_for_save — decoupled from layer names.
    assert store.is_present(chash) is False, "marker must not exist before mark_present"
    assert store.mark_present(chash) is True
    assert store.is_present(chash) is True, "marker must exist after mark_present"
    assert store.is_present(prefix_hash([5, 5], 2)) is False
    log.info("presence-marker (mark_present/is_present) OK")

    # ── 5. partial-load miss is graceful (no crash, returns False) ───────────
    miss_dst = [np.zeros(page_bytes, dtype=np.uint8)]
    res = store.load_layers("0" * 64, ["layer.0"], miss_dst)
    assert res == [False], f"expected miss=[False], got {res}"
    log.info("missing-key load returns False (graceful) OK")

    log.info("ALL vLLM DATA-PLANE TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
