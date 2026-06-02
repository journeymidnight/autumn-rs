"""autumn-kvcache: sglang HiCache L3 + vLLM KVConnectorV1 backends over autumn-rs.

See docs/autumn_kvcache_plan.md for architecture and rationale (§4 sglang, §13 vLLM).
"""

from .sglang_backend import AutumnKVCacheStorage
from .vllm_connector import AutumnKVConnector

__all__ = ["AutumnKVCacheStorage", "AutumnKVConnector"]
