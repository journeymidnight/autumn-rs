"""autumn-kvcache: sglang HiCache L3 + vLLM KVConnectorV1 backends over autumn-rs.

See docs/autumn_kvcache_plan.md for architecture and rationale (§4 sglang, §13 vLLM).

The adapter classes are re-exported LAZILY (PEP 562): both submodules import
the `autumn` native module at import time, but the pure helpers
(`_keys`, `_identity`) must stay importable — and unit-testable — in
environments without the PyO3 build (BUG-KVC-TENANT offline tests). Engines
plug in via the submodule paths (`autumn_kvcache.sglang_backend` /
`autumn_kvcache.vllm_connector`) anyway, so nothing relies on eager imports
here.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .sglang_backend import AutumnKVCacheStorage  # noqa: F401
    from .vllm_connector import AutumnKVConnector  # noqa: F401

_LAZY = {
    "AutumnKVCacheStorage": ".sglang_backend",
    "AutumnKVConnector": ".vllm_connector",
}

__all__ = ["AutumnKVCacheStorage", "AutumnKVConnector"]


def __getattr__(name: str):
    if name in _LAZY:
        import importlib

        return getattr(importlib.import_module(_LAZY[name], __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
