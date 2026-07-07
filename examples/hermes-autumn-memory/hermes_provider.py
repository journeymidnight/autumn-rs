"""A Hermes-agent `MemoryProvider` backed by autumn-rs.

This is DEPLOYMENT GLUE, not part of autumn-rs core — the core keeps only the
Rust `autumn-memory` lib + the `mem/` schema (see autumn_mem.py). Drop this
package into the Hermes plugins dir (`$HERMES_HOME/plugins/autumn/`).

It implements the hooks the real `agent.memory_provider.MemoryProvider` ABC
exposes (per the pre-refactor reference: initialize / sync_turn /
queue_prefetch+prefetch / get_tool_schemas+handle_tool_call / on_memory_write /
shutdown). Signatures below follow that reference — CONFIRM them against the
`hermes-agent` version you deploy (the ABC drifts across releases) and adjust.

Config via env:
    AUTUMN_MEMORY_MANAGER   host:port of the autumn manager (ClusterIP in k8s)   [required]
    AUTUMN_MEMORY_TENANT    tenant namespace                                     [default: "default"]
Semantic recall is intentionally OFF (lexical-only) — no embedder wired.
"""
from __future__ import annotations

import asyncio
import os
import threading
from typing import Any, Optional

import autumn  # the PyO3 extension (Client)
from autumn_mem import AutumnMemory

try:  # the real ABC when running inside hermes-agent
    from agent.memory_provider import MemoryProvider as _Base
except Exception:  # standalone/tooling import must still work
    class _Base:  # minimal shim
        pass


class _SyncBridge:
    """Run async autumn coroutines from sync hooks on a private asyncio loop."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True).start()

    def run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def submit(self, coro):  # fire-and-forget (non-blocking writes)
        asyncio.run_coroutine_threadsafe(coro, self._loop)


class AutumnMemoryProvider(_Base):
    def __init__(self) -> None:
        self._bridge = _SyncBridge()
        self._mem: Optional[AutumnMemory] = None
        self._session = "default"
        self._read_only = False

    # ---- lifecycle --------------------------------------------------------
    def initialize(self, context: Any = None) -> None:
        manager = os.environ["AUTUMN_MEMORY_MANAGER"]
        tenant = os.environ.get("AUTUMN_MEMORY_TENANT", "default")
        # derive (agent, session, read-only) from the Hermes identity/context.
        agent = _ctx_get(context, "agent_identity", "user_id", default="hermes")
        self._session = str(_ctx_get(context, "session_id", "session", default="default"))
        # non-primary contexts (cron / subagent / flush) must not write.
        self._read_only = str(_ctx_get(context, "context_kind", default="primary")) != "primary"
        client = self._bridge.run(autumn.Client.connect(manager))
        self._mem = AutumnMemory(client, tenant, str(agent))

    def shutdown(self) -> None:
        pass  # writes are already flushed by submit(); nothing buffered here.

    # ---- turn ingestion (non-blocking) ------------------------------------
    def sync_turn(self, turn: Any) -> None:
        if self._read_only or self._mem is None:
            return
        self._bridge.submit(self._mem.append_event(self._session, _jsonable(turn)))

    def on_memory_write(self, namespace: str, key: str, value: Any) -> None:
        # mirror Hermes's MEMORY.md / USER.md writes as facts.
        if self._read_only or self._mem is None:
            return
        self._bridge.submit(self._mem.put_fact(namespace, key, _jsonable(value)))

    # ---- recall (prefetch is fast; heavy work is done in queue_prefetch) ---
    def queue_prefetch(self, query: str) -> None:
        self._pending = self._bridge.submit(self._mem.search(query, k=5)) if self._mem else None

    def prefetch(self, query: str) -> list:
        if self._mem is None:
            return []
        return self._bridge.run(self._mem.search(query, k=5))

    # ---- tools the agent can call -----------------------------------------
    def get_tool_schemas(self) -> list:
        return [
            {"name": "memory_search",
             "description": "Recall relevant past memory (lexical).",
             "parameters": {"type": "object",
                            "properties": {"query": {"type": "string"},
                                           "k": {"type": "integer", "default": 5}},
                            "required": ["query"]}},
            {"name": "memory_store",
             "description": "Store a durable fact.",
             "parameters": {"type": "object",
                            "properties": {"namespace": {"type": "string"},
                                           "key": {"type": "string"},
                                           "value": {}},
                            "required": ["namespace", "key", "value"]}},
        ]

    def handle_tool_call(self, name: str, args: dict) -> Any:
        if self._mem is None:
            return {"error": "memory not initialized"}
        if name == "memory_search":
            return self._bridge.run(self._mem.search(args["query"], k=int(args.get("k", 5))))
        if name == "memory_store":
            if self._read_only:
                return {"error": "read-only context"}
            self._bridge.run(self._mem.put_fact(args["namespace"], args["key"], args["value"]))
            return {"ok": True}
        return {"error": f"unknown tool {name}"}


def _ctx_get(ctx: Any, *names: str, default: Any = None) -> Any:
    for n in names:
        v = getattr(ctx, n, None) if ctx is not None and not isinstance(ctx, dict) else (ctx or {}).get(n)
        if v:
            return v
    return default


def _jsonable(x: Any) -> Any:
    if isinstance(x, (str, int, float, bool, list, dict)) or x is None:
        return x
    for attr in ("model_dump", "dict", "__dict__"):
        f = getattr(x, attr, None)
        if callable(f):
            return f()
        if f is not None:
            return f
    return str(x)
