"""autumn-memory-langgraph — a LangGraph `BaseStore` backed by autumn-memory.

`AutumnStore` is a drop-in for LangGraph's persistent `BaseStore` (the
long-term memory `get` / `put` / `search` / `delete` / `list_namespaces`
surface) over the Rust-backed agent-memory core. It is a pure client-side
adapter on `autumn_memory.AutumnMemory` — no daemon, no server change (plan
§12c).

Mapping
-------
* A LangGraph **namespace** is a `tuple[str, ...]`; it is encoded to one
  autumn fact-namespace string (`_enc_ns`, per-component percent-encoded so a
  tuple round-trips and can't forge a separator). Each item is a fact:
  `put_fact(ns_str, key, envelope, ttl)`.
* The stored value is wrapped in a small **envelope** `{"value", "created_at",
  "updated_at"}` so the `Item` timestamps LangGraph expects are preserved
  (created_at is kept across updates).
* `search(namespace_prefix, …)` and `list_namespaces(…)` need to enumerate
  namespaces under a prefix; the facts API lists keys within ONE exact
  namespace, so the adapter keeps a small **namespace registry** (a reserved
  fact namespace) updated on put/delete.

Limitations (documented, not bugs)
-----------------------------------
* `search`'s `query` is ranked **lexically** over the JSON value (token
  overlap); the `index=` semantic-embedding path is not wired here (that is the
  recall index's job — a follow-up). `filter` is exact-match on value fields.
* `refresh_ttl` on read is ignored (no TTL-bump-on-access).
"""

from __future__ import annotations

import json
import operator
from datetime import datetime, timezone
from typing import Any, Iterable, Optional
from urllib.parse import quote

from langgraph.store.base import (
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    PutOp,
    SearchItem,
    SearchOp,
)

from autumn_memory import AutumnMemory

__all__ = ["AutumnStore"]

# Reserved fact-namespace holding the set of live data namespaces. The leading
# NUL can never appear in an `_enc_ns` output (percent-encoding escapes it), so
# this can't collide with any user namespace.
_REGISTRY_NS = "\x00lg_namespaces"


def _enc_ns(ns: tuple) -> str:
    return "/".join(quote(c, safe="") for c in ns)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(s: Any) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)


def _ttl_seconds(ttl) -> Optional[int]:
    # LangGraph TTLs are in MINUTES (float); autumn TTLs are seconds (0 = none).
    if ttl is None:
        return None
    try:
        secs = int(round(float(ttl) * 60))
    except (TypeError, ValueError):
        return None
    return secs if secs > 0 else None


def _match_path(ns: tuple, path: tuple, suffix: bool) -> bool:
    if len(path) > len(ns):
        return False
    seg = ns[-len(path):] if suffix else ns[: len(path)]
    return all(p == "*" or p == s for p, s in zip(path, seg))


# LangGraph `search(filter=...)` comparison operators.
_CMP_OPS = {"$gt": operator.gt, "$gte": operator.ge, "$lt": operator.lt, "$lte": operator.le}


def _match_one(actual: Any, expected: Any) -> bool:
    # An operator expression is a dict whose keys are all `$…` operators;
    # anything else is plain-value equality.
    if isinstance(expected, dict) and expected and all(k.startswith("$") for k in expected):
        for opk, opv in expected.items():
            if opk == "$eq":
                if actual != opv:
                    return False
            elif opk == "$ne":
                if actual == opv:
                    return False
            elif opk == "$in":
                if actual not in opv:
                    return False
            elif opk == "$nin":
                if actual in opv:
                    return False
            elif opk in _CMP_OPS:
                try:
                    if actual is None or not _CMP_OPS[opk](actual, opv):
                        return False
                except TypeError:  # non-comparable types → no match
                    return False
            else:
                raise ValueError(f"unsupported filter operator {opk!r}")
        return True
    return actual == expected


def _match_filter(value: Any, filt: dict) -> bool:
    if not isinstance(value, dict):
        return False
    return all(_match_one(value.get(k), v) for k, v in filt.items())


def _lexical_score(query: str, value: Any) -> float:
    q = {t for t in query.lower().split() if t}
    if not q:
        return 0.0
    hay = json.dumps(value, ensure_ascii=False).lower()
    toks = {t for t in hay.replace('"', " ").replace(":", " ").replace(",", " ").split() if t}
    return len(q & toks) / len(q)


class AutumnStore(BaseStore):
    """A LangGraph `BaseStore` over `autumn_memory.AutumnMemory`.

    Construct with an existing `AutumnMemory`, or `AutumnStore.connect(...)`.
    Only the sync surface is backed natively; `abatch` offloads to a thread.
    """

    supports_ttl = True

    def __init__(self, mem: AutumnMemory):
        self._mem = mem

    @classmethod
    def connect(cls, manager: str, tenant: str, agent: str, **kw) -> "AutumnStore":
        return cls(AutumnMemory(manager, tenant, agent, **kw))

    def close(self) -> None:
        self._mem.close()

    # -- BaseStore abstract surface ------------------------------------------

    def batch(self, ops: Iterable) -> list:
        out = []
        for op in ops:
            if isinstance(op, GetOp):
                out.append(self._get(op))
            elif isinstance(op, PutOp):
                out.append(self._put(op))
            elif isinstance(op, SearchOp):
                out.append(self._search(op))
            elif isinstance(op, ListNamespacesOp):
                out.append(self._list_ns(op))
            else:  # pragma: no cover - defensive
                raise TypeError(f"unsupported op {type(op).__name__}")
        return out

    async def abatch(self, ops: Iterable) -> list:
        import anyio

        materialized = list(ops)
        return await anyio.to_thread.run_sync(self.batch, materialized)

    # -- op handlers ---------------------------------------------------------

    def _get(self, op: GetOp):
        env = self._mem.get_fact(_enc_ns(op.namespace), op.key)
        if not isinstance(env, dict):
            return None
        return self._item(op.namespace, op.key, env)

    def _put(self, op: PutOp):
        ns_str = _enc_ns(op.namespace)
        if op.value is None:  # LangGraph encodes delete as a PutOp(value=None)
            self._mem.delete_fact(ns_str, op.key)
            self._maybe_deregister(ns_str)
            return None
        now = _iso_now()
        prev = self._mem.get_fact(ns_str, op.key)
        created = prev.get("created_at") if isinstance(prev, dict) else None
        env = {"value": op.value, "created_at": created or now, "updated_at": now}
        self._mem.put_fact(ns_str, op.key, env, _ttl_seconds(op.ttl))
        self._register(op.namespace, ns_str)
        return None

    def _search(self, op: SearchOp):
        matches = []
        for full, items in self._live_namespaces():
            if not _match_path(full, op.namespace_prefix, suffix=False):
                continue
            for key, env in items:
                if not isinstance(env, dict):
                    continue
                val = env.get("value")
                if op.filter and not _match_filter(val, op.filter):
                    continue
                score = _lexical_score(op.query, val) if op.query else None
                matches.append((full, key, env, score))
        if op.query:
            matches.sort(key=lambda m: (m[3] or 0.0), reverse=True)
        window = matches[op.offset : op.offset + op.limit]
        return [self._search_item(f, k, e, s) for (f, k, e, s) in window]

    def _list_ns(self, op: ListNamespacesOp):
        conds = op.match_conditions or ()
        out = set()
        for full, _items in self._live_namespaces():
            ok = True
            for c in conds:
                suffix = getattr(c.match_type, "value", c.match_type) == "suffix"
                if not _match_path(full, tuple(c.path), suffix=suffix):
                    ok = False
                    break
            if not ok:
                continue
            out.add(full[: op.max_depth] if op.max_depth is not None else full)
        ordered = sorted(out)
        return ordered[op.offset : op.offset + op.limit]

    # -- registry + builders -------------------------------------------------

    def _live_namespaces(self) -> list:
        """Registered namespaces that still hold live facts, as `(tuple, items)`
        pairs (items = `list_facts` output, reused by `_search`). A registry
        entry whose namespace has no live facts — every key deleted or
        TTL-expired — is lazily reaped here so `search`/`list_namespaces` never
        surface an empty/expired namespace and the registry can't leak. Reaping
        during a read is idempotent and self-heals (a later `put` re-registers);
        eventual-consistency, consistent with the §8.5 read contract."""
        out = []
        for _k, v in self._mem.list_facts(_REGISTRY_NS):
            if not (isinstance(v, dict) and isinstance(v.get("ns"), list)):
                continue
            full = tuple(v["ns"])
            ns_str = _enc_ns(full)
            items = self._mem.list_facts(ns_str)
            if items:
                out.append((full, items))
            else:
                self._mem.delete_fact(_REGISTRY_NS, ns_str)
        return out

    def _register(self, full: tuple, ns_str: str) -> None:
        self._mem.put_fact(_REGISTRY_NS, ns_str, {"ns": list(full)})

    def _maybe_deregister(self, ns_str: str) -> None:
        # drop the namespace from the registry once its last key is gone
        if not self._mem.list_facts(ns_str):
            self._mem.delete_fact(_REGISTRY_NS, ns_str)

    def _item(self, namespace: tuple, key: str, env: dict) -> Item:
        return Item(
            value=env.get("value") if isinstance(env.get("value"), dict) else {"value": env.get("value")},
            key=key,
            namespace=namespace,
            created_at=_parse_ts(env.get("created_at")),
            updated_at=_parse_ts(env.get("updated_at")),
        )

    def _search_item(self, namespace: tuple, key: str, env: dict, score) -> SearchItem:
        val = env.get("value")
        return SearchItem(
            namespace=namespace,
            key=key,
            value=val if isinstance(val, dict) else {"value": val},
            created_at=_parse_ts(env.get("created_at")),
            updated_at=_parse_ts(env.get("updated_at")),
            score=score,
        )
