"""Bespoke agent-memory glue for autumn-rs — built on the *stable* `mem/` key
schema, NOT the removed `autumn.Memory` binding.

The autumn-rs core deliberately keeps only the Rust `autumn-memory` library +
the `mem/` key schema as the contract (see `crates/autumn-memory/src/keys.rs`
and `docs/autumn_memory_plan.md §6`); per-agent ergonomics live with each
agent's deployment. This module is that glue for a Hermes agent, implemented
directly on the `autumn` PyO3 KV client (`Client.put/get/delete/range`) so it
interoperates byte-for-byte with the Rust lib and any other consumer.

Schema (must match keys.rs exactly):
    episodic:  mem/{tenant}/{agent}/ep/{session}/{suffix}   suffix = BE(u64_max-ts_ns)++BE(u32_max-ctr)
    fact:      mem/{tenant}/{agent}/fact/{namespace}/{key}
    shared:    mem/{tenant}/shared/{namespace}/{key}
Every dynamic component is percent-encoded (RFC-3986 unreserved kept).

Lexical-only recall for now (no embedder) — a bounded keyword scan + TF score.
Swap `search()` for a vector/hybrid leg later without touching the schema.
"""
from __future__ import annotations

import itertools
import json
import re
import time
from typing import Any, Optional

# RFC 3986 unreserved — everything else is %XX (uppercase), matching keys.rs `q`.
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


class AutumnMemory:
    """Per-(tenant, agent) memory over one autumn `Client`.

    All methods are async — `autumn.Client` methods return awaitables. Wrap with
    a sync bridge (see hermes_provider.py) if your agent hooks are synchronous.
    """

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
        """Bounded keyword recall over this agent's episodic + fact records.

        Simple TF scoring — scans up to `scan` recent episodic events (all
        sessions) plus facts, ranks by query-term hit count. Good enough to
        start; replace with a vector/hybrid leg (add an embedder) later without
        changing the schema or callers.
        """
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
