"""A Hermes-agent `MemoryProvider` backed by autumn-rs.

This is DEPLOYMENT GLUE, not part of autumn-rs core — the core keeps only the
Rust `autumn-memory` lib + the `mem/` schema. This single file holds BOTH the
mem/-schema memory ops (`AutumnMemory`, merged from the former autumn_mem.py —
hermes is the only consumer) AND the Hermes `MemoryProvider` adapter. Drop it
into the Hermes plugins dir (`$HERMES_HOME/plugins/autumn/`).

It implements the hooks the real `agent.memory_provider.MemoryProvider` ABC
exposes (per the pre-refactor reference: initialize / sync_turn /
queue_prefetch+prefetch / get_tool_schemas+handle_tool_call / on_memory_write /
shutdown). Signatures below follow that reference — CONFIRM them against the
`hermes-agent` version you deploy (the ABC drifts across releases) and adjust.

Config via env:
    AUTUMN_MEMORY_MANAGER   host:port of the autumn manager (ClusterIP in k8s)   [required]
    AUTUMN_MEMORY_TENANT    tenant namespace                                     [default: "default"]
    AUTUMN_MEMORY_CREDENTIAL_FILE
                            path to the tenant credential (from `autumn-op
                            tenant-create`; k8s: mount a Secret). Required once
                            `mem/` enforcement is on (F-AUTHZ-BUILTIN D6-mem) —
                            without it every write dies with PermissionDenied
                            (terminal, not retried). Harmless when authz is off.
                            The SDK auto-mints + renews the short-TTL token.
Semantic recall is intentionally OFF (lexical-only) — no embedder wired.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import os
import re
import threading
import time
from typing import Any, Optional

import autumn  # the PyO3 extension (Client)

try:  # the real ABC when running inside hermes-agent
    from agent.memory_provider import MemoryProvider as _Base
except Exception:  # standalone/tooling import must still work
    class _Base:  # minimal shim
        pass


# ─── mem/ schema memory ops (merged from the former autumn_mem.py) ────────────
# hermes is the only consumer, so the (thin) core lives in this one plugin file.
# The `mem/` key schema is the stable contract — byte-identical to the Rust
# `autumn-memory` lib (crates/autumn-memory/src/keys.rs, docs/autumn_memory_plan.md §6):
#   episodic:  mem/{tenant}/{agent}/ep/{session}/{suffix}  suffix = BE(u64_max-ts_ns)++BE(u32_max-ctr)
#   fact:      mem/{tenant}/{agent}/fact/{namespace}/{key}
# Every dynamic component is percent-encoded (RFC-3986 unreserved kept).
# Lexical-only recall for now (no embedder) — swap `search()` for a vector/hybrid
# leg later without touching the schema.
_UNRESERVED = re.compile(rb"[A-Za-z0-9\-_.~]")
_U64_MAX = (1 << 64) - 1
_U32_MAX = (1 << 32) - 1


def q(s: str) -> bytes:
    """Percent-encode one dynamic key component (byte-identical to Rust `q`)."""
    out = bytearray()
    for b in s.encode("utf-8"):
        if _UNRESERVED.match(bytes([b])):
            out.append(b)
        else:
            out += b"%%%02X" % b
    return bytes(out)


def _unq(b: bytes) -> str:
    """Inverse of `q` — decode a percent-encoded component."""
    out = bytearray()
    i = 0
    while i < len(b):
        if b[i] == 0x25 and i + 2 < len(b):  # '%'
            out.append(int(b[i + 1:i + 3], 16))
            i += 3
        else:
            out.append(b[i])
            i += 1
    return out.decode("utf-8", "replace")


def _read_credential_file(path: str) -> bytes:
    """Read a tenant credential file -> RAW bytes for the SDK.

    On-disk format = the lowercase hex printed by `autumn-op tenant-create`
    (either the bare hex, or its `credential: <hex>` stdout line — accepted
    defensively since "save the output" is the predictable operator move).
    The SDK/manager contract is RAW credential bytes (`mint-token` hex-decodes
    before sending; the manager stores the SHA-256 of the raw bytes), so
    passing the ASCII hex through would mint with a WRONG credential and every
    protected-prefix op would die with PermissionDenied once enforcement is on
    (coco P1 2026-07-17). Fail loudly here, at startup, instead.
    """
    with open(path, "r", encoding="ascii") as f:
        lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
    for ln in lines:
        if ln.lower().startswith("credential:"):
            hexs = ln.split(":", 1)[1].strip()
            break
    else:
        if len(lines) != 1:
            raise ValueError(
                f"credential file {path!r}: expected a single hex line or a "
                f"'credential: <hex>' line, got {len(lines)} lines"
            )
        hexs = lines[0]
    try:
        return bytes.fromhex(hexs)
    except ValueError as e:
        raise ValueError(f"credential file {path!r}: not valid hex: {e}") from None


class AutumnMemory:
    """Per-(tenant, agent) memory over one autumn `Client`. All methods are async
    (`autumn.Client` methods return awaitables) — the provider below wraps them
    with a sync bridge for Hermes' synchronous hooks."""

    def __init__(self, client: Any, tenant: str, agent: str) -> None:
        self._c = client
        self._t = q(tenant)
        self._a = q(agent)
        # per-process monotonic counter so two events in the same ns still order
        self._ctr = itertools.count()

    # ---- key builders -----------------------------------------------------
    def _agent_prefix(self) -> bytes:
        return b"mem/" + self._t + b"/" + self._a + b"/"

    def _ep_agent_prefix(self) -> bytes:
        return self._agent_prefix() + b"ep/"

    def _ep_session_prefix(self, session: str) -> bytes:
        return self._ep_agent_prefix() + q(session) + b"/"

    def _ep_suffix(self, ts_ns: int, ctr: int) -> bytes:
        return (_U64_MAX - ts_ns).to_bytes(8, "big") + (_U32_MAX - (ctr & _U32_MAX)).to_bytes(4, "big")

    def _fact_prefix(self, namespace: str) -> bytes:
        return self._agent_prefix() + b"fact/" + q(namespace) + b"/"

    def _fact_key(self, namespace: str, key: str) -> bytes:
        return self._fact_prefix(namespace) + q(key)

    # ---- episodic ---------------------------------------------------------
    async def append_event(self, session: str, event: Any) -> None:
        """Append one event to the session's episodic log (newest sorts first)."""
        key = self._ep_session_prefix(session) + self._ep_suffix(time.time_ns(), next(self._ctr))
        await self._c.put(key, json.dumps(event).encode("utf-8"))

    async def recent(self, session: str, k: int = 20) -> list:
        """The k most-recent events in `session` (inverted-ts ⇒ range is newest-first)."""
        rows = await self._c.range(self._ep_session_prefix(session), b"", k)
        return [json.loads(v) for _, v in rows]

    async def replay(self, session: str, limit: int = 200) -> list:
        """Full session in chronological order (oldest-first)."""
        rows = await self._c.range(self._ep_session_prefix(session), b"", limit)
        return [json.loads(v) for _, v in reversed(rows)]

    # ---- facts ------------------------------------------------------------
    async def put_fact(self, namespace: str, key: str, value: Any) -> None:
        await self._c.put(self._fact_key(namespace, key), json.dumps(value).encode("utf-8"))

    async def get_fact(self, namespace: str, key: str) -> Optional[Any]:
        b = await self._c.get(self._fact_key(namespace, key))
        return None if b is None else json.loads(b)

    async def list_facts(self, namespace: str, limit: int = 200) -> list:
        rows = await self._c.range(self._fact_prefix(namespace), b"", limit)
        pfx = self._fact_prefix(namespace)
        out = []
        for kbytes, v in rows:
            name = _unq(kbytes[len(pfx):])
            out.append((name, json.loads(v)))
        return out

    # ---- lexical recall (no embedder) -------------------------------------
    async def search(self, query: str, k: int = 5, *, scan: int = 500) -> list:
        """Bounded keyword recall over this agent's episodic + fact records (TF score)."""
        terms = [t for t in re.split(r"\W+", query.lower()) if t]
        if not terms:
            return []
        candidates: list[tuple[str, Any]] = []
        for _, v in await self._c.range(self._ep_agent_prefix(), b"", scan):
            candidates.append(("episodic", json.loads(v)))
        for _, v in await self._c.range(self._agent_prefix() + b"fact/", b"", scan):
            candidates.append(("fact", json.loads(v)))
        scored = []
        for kind, doc in candidates:
            text = (doc if isinstance(doc, str) else json.dumps(doc, ensure_ascii=False)).lower()
            score = sum(text.count(t) for t in terms)
            if score:
                scored.append((score, kind, doc))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [{"score": s, "kind": kind, "doc": doc} for s, kind, doc in scored[:k]]


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
        cred_file = os.environ.get("AUTUMN_MEMORY_CREDENTIAL_FILE")
        if cred_file:
            credential = _read_credential_file(cred_file)
            client = self._bridge.run(
                autumn.Client.connect(manager, tenant=tenant, credential=credential)
            )
        else:
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
