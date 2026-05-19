"""autumn-kvcache: sglang HiCache L3 storage backend backed by autumn-rs partition layer.

See docs/autumn_kvcache_plan.md for architecture and rationale.
"""

from .sglang_backend import AutumnKVCacheStorage

__all__ = ["AutumnKVCacheStorage"]
