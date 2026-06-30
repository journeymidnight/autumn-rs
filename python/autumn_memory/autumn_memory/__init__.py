"""autumn-memory — ergonomic Python layer over the Rust-backed `autumn.Memory`.

This is the **core Python library** the framework adapters (Hermes
`MemoryProvider`, LangGraph `BaseStore`, an MCP server) build on. It is a thin
wrapper over the `autumn.Memory` PyO3 binding (which wraps the Rust
`autumn_memory::MemoryStore`) that adds:

  * JSON (de)serialization for episodic events + facts (the binding takes bytes),
  * an optional **text embedder hook** so the vector / hybrid retrieval legs
    take TEXT — the caller plugs in their own embedder (e.g. an sglang/OpenAI
    `/embeddings` endpoint); without one, only the lexical (BM25) leg is used.

See docs/autumn_memory_plan.md. The heavy lifting (key schema, BM25/IVF, RRF)
is all in Rust; this layer is pure ergonomics.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

import autumn  # the PyO3 binding — provides `autumn.Memory`

from .embedder import http_embed_many, http_embedder

__all__ = ["AutumnMemory", "http_embedder", "http_embed_many"]

Embedder = Callable[[str], "list[float]"]


def _decode_meta(b: bytes):
    if not b:
        return None
    try:
        return json.loads(b)
    except Exception:
        return b  # opaque non-JSON meta


class AutumnMemory:
    """Per-`(tenant, agent)` agent-memory handle.

    Construct one per agent identity. `embed`, if given, is called to turn text
    into a vector for the vector / hybrid legs.
    """

    def __init__(
        self,
        manager: str,
        tenant: str,
        agent: str,
        *,
        embed: Optional[Embedder] = None,
        default_ttl: int = 0,
        transport: Optional[str] = None,
    ):
        if transport:
            autumn.set_transport(transport)
        self._m = autumn.Memory.connect(manager, tenant, agent, default_ttl)
        self._embed = embed
        self._tenant = tenant
        self._agent = agent

    # -- episodic ------------------------------------------------------------

    def append(self, session: str, event: Any, ttl: Optional[int] = None) -> int:
        return self._m.append_event(session, json.dumps(event).encode("utf-8"), ttl)

    def replay(self, session: str, limit: Optional[int] = None) -> list:
        return [json.loads(b) for b in self._m.replay_session(session, limit)]

    def recent(self, session: str, k: int = 20) -> list:
        return [json.loads(b) for b in self._m.recent_events(session, k)]

    # -- facts (LangGraph BaseStore model) -----------------------------------

    def put_fact(self, namespace: str, key: str, value: Any, ttl: Optional[int] = None) -> None:
        self._m.put_fact(namespace, key, json.dumps(value).encode("utf-8"), ttl)

    def get_fact(self, namespace: str, key: str):
        b = self._m.get_fact(namespace, key)
        return None if b is None else json.loads(b)

    def list_facts(self, namespace: str, limit: Optional[int] = None) -> list:
        return [(k, json.loads(v)) for k, v in self._m.list_facts(namespace, limit)]

    def delete_fact(self, namespace: str, key: str) -> None:
        self._m.delete_fact(namespace, key)

    # -- searchable memory ---------------------------------------------------

    def remember(self, doc_id: str, text: str, meta: Any = None, ttl: Optional[int] = None) -> None:
        """Index a searchable memory: BM25 always; also the vector leg iff an
        embedder is configured.

        The embedding (which may hit a remote endpoint and fail) is computed
        BEFORE any KV write, so an embedder failure leaves nothing persisted —
        no doc/BM25 record without the caller knowing the write didn't happen.
        """
        meta_b = b"" if meta is None else json.dumps(meta).encode("utf-8")
        vec = None
        if self._embed is not None:
            vec = [float(x) for x in self._embed(text)]
        self._m.index_memory(doc_id, text, meta_b, ttl)
        if vec is not None:
            self._m.index_vector(doc_id, vec, ttl)

    def get(self, doc_id: str):
        """Fetch one indexed memory by id → ``{"id", "text", "meta"}`` or
        ``None`` (deleted / expired / never indexed). Backs an MCP ``fetch``."""
        r = self._m.get_memory(doc_id)
        if r is None:
            return None
        text, meta = r
        return {"id": doc_id, "text": text, "meta": _decode_meta(meta)}

    def forget(self, doc_id: str) -> None:
        self._m.delete_memory(doc_id)

    def train(self, nlist: int, max_iter: int = 25, seed: int = 1) -> int:
        """(Re)train the IVF centroids over the current vectors."""
        return self._m.train_centroids(nlist, max_iter, seed)

    def search(self, query: str, k: int = 5, *, mode: str = "auto", nprobe: int = 8) -> list:
        """Search indexed memories.

        mode: ``"lexical"`` (BM25), ``"vector"`` (IVF), ``"hybrid"`` (RRF of
        both), or ``"auto"`` = hybrid when an embedder is set, else lexical.
        Lexical hits carry text+meta; vector/hybrid hits carry id+score.
        """
        if mode == "auto":
            mode = "hybrid" if self._embed is not None else "lexical"
        if mode == "lexical":
            return [
                {"id": h[0], "text": h[1], "meta": _decode_meta(h[2]), "score": h[3]}
                for h in self._m.search_lexical(query, k)
            ]
        if self._embed is None:
            raise ValueError(f"mode={mode!r} needs an embedder (pass embed=...)")
        qv = [float(x) for x in self._embed(query)]
        if mode == "vector":
            return [{"id": i, "score": s} for i, s in self._m.search_vector(qv, k, nprobe)]
        if mode == "hybrid":
            return [{"id": i, "score": s} for i, s in self._m.search_hybrid(query, qv, k, nprobe)]
        raise ValueError(f"unknown search mode {mode!r}")

    def close(self) -> None:
        self._m.close()
