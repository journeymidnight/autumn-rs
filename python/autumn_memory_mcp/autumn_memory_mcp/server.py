"""The MCP server: tools over an `AutumnMemory` handle.

Tool surface (plan §12a). The ChatGPT-recognized pair is **search** + **fetch**
(named exactly so ChatGPT's normal mode picks them up); the write tools
(**add** / **update** / **delete**) need Developer Mode there but work in every
other host. Episodic + fact tools round out a real agent-memory surface.

Every tool is `async` and offloads the blocking `AutumnMemory` call to a worker
thread (`anyio.to_thread.run_sync`) so the stdio event loop stays responsive.
"""

from __future__ import annotations

import os
from functools import partial
from typing import Any, Optional

import anyio
from mcp.server.fastmcp import FastMCP

from autumn_memory import AutumnMemory


def build_server(
    mem: AutumnMemory,
    *,
    name: str = "autumn-memory",
    default_mode: str = "auto",
    default_k: int = 5,
) -> FastMCP:
    """Build a FastMCP server whose tools delegate to `mem`.

    `default_mode` is the retrieval mode for `search` (`"auto"` → hybrid when an
    embedder is configured on `mem`, else lexical). Testable in-process via the
    MCP in-memory client transport — no subprocess, no stdio.
    """
    server = FastMCP(name)

    async def _call(fn, *args):
        # AutumnMemory methods block (the Rust worker releases the GIL); keep the
        # asyncio loop free by running them on a thread.
        return await anyio.to_thread.run_sync(fn, *args)

    async def _resolve(doc_id: str, text: Optional[str], meta: Any) -> Optional[dict]:
        # search hits from the vector/hybrid legs carry only id+score; resolve
        # their text/meta from the authoritative doc record so every result is
        # self-contained (the ChatGPT search→fetch contract wants `text`).
        #
        # The `doc/{id}` record is the correctness boundary (plan §8.5: a posting
        # is a candidate hint, the doc record decides truth). A vector/IVF hit
        # whose doc record is gone — deleted (delete only reaps the doc + BM25
        # postings, not the IVF posting) or expired — is NOT a real result:
        # return None and let the caller drop it, rather than leak a ghost
        # (empty-text) hit that exposes a deleted memory's id/url.
        if text is None:
            got = await _call(mem.get, doc_id)
            if got is None:
                return None
            text, meta = got["text"], got["meta"]
        title = ""
        if isinstance(meta, dict):
            title = str(meta.get("title") or meta.get("name") or "")
        return {
            "id": doc_id,
            "title": title or (text or "")[:60],
            "text": text or "",
            "url": f"autumn-memory://{mem_ref(mem)}/{doc_id}",
            "metadata": meta if isinstance(meta, dict) else None,
        }

    # -- ChatGPT-recognized pair: search + fetch -----------------------------

    @server.tool()
    async def search(query: str, k: int = default_k) -> dict:
        """Search the agent's stored memories and return the most relevant ones.

        Returns `{"results": [{id, title, text, url, metadata}]}`. Use `fetch`
        with a result's `id` to retrieve its full content.
        """
        hits = await _call(partial(mem.search, query, k, mode=default_mode))
        results = []
        for h in hits:
            r = await _resolve(h["id"], h.get("text"), h.get("meta"))
            if r is not None:  # drop hits whose doc record is gone (deleted/expired)
                results.append(r)
        return {"results": results}

    @server.tool()
    async def fetch(id: str) -> dict:
        """Fetch one stored memory by id → `{id, title, text, url, metadata}`.

        Raises if the id is unknown (deleted / expired / never stored).
        """
        got = await _call(mem.get, id)
        if got is None:
            raise ValueError(f"no memory with id {id!r}")
        return await _resolve(id, got["text"], got["meta"])

    # -- write tools (ChatGPT Developer Mode; native everywhere else) ---------

    @server.tool()
    async def add(text: str, id: Optional[str] = None, metadata: Optional[dict] = None) -> dict:
        """Store a searchable memory. If `id` is omitted a content-derived id is
        used; storing the same id again replaces it. Returns `{"id"}`."""
        doc_id = id or _auto_id(text)
        await _call(mem.remember, doc_id, text, metadata)
        return {"id": doc_id}

    @server.tool()
    async def update(id: str, text: str, metadata: Optional[dict] = None) -> dict:
        """Replace the text/metadata of an existing memory (re-indexes it)."""
        await _call(mem.remember, id, text, metadata)
        return {"id": id, "updated": True}

    @server.tool()
    async def delete(id: str) -> dict:
        """Delete a stored memory by id (idempotent)."""
        await _call(mem.forget, id)
        return {"id": id, "deleted": True}

    # -- episodic (conversation log) -----------------------------------------

    @server.tool()
    async def append_event(session: str, event: dict) -> dict:
        """Append an event (any JSON object) to a session's episodic log."""
        n = await _call(mem.append, session, event)
        return {"session": session, "seq": n}

    @server.tool()
    async def recent_events(session: str, k: int = 20) -> dict:
        """Return the `k` most recent events of a session, newest first."""
        return {"events": await _call(mem.recent, session, k)}

    @server.tool()
    async def replay_session(session: str) -> dict:
        """Return a session's full episodic log in chronological order."""
        return {"events": await _call(mem.replay, session)}

    # -- facts (durable key/value, namespaced) -------------------------------

    @server.tool()
    async def put_fact(namespace: str, key: str, value: Any) -> dict:
        """Store a durable fact (any JSON value) under `namespace`/`key`."""
        await _call(mem.put_fact, namespace, key, value)
        return {"namespace": namespace, "key": key}

    @server.tool()
    async def get_fact(namespace: str, key: str) -> dict:
        """Get a fact by `namespace`/`key`. `value` is null if absent."""
        return {"value": await _call(mem.get_fact, namespace, key)}

    @server.tool()
    async def list_facts(namespace: str) -> dict:
        """List `(key, value)` facts under a namespace."""
        items = await _call(mem.list_facts, namespace)
        return {"facts": [{"key": k, "value": v} for k, v in items]}

    @server.tool()
    async def delete_fact(namespace: str, key: str) -> dict:
        """Delete a fact (idempotent)."""
        await _call(mem.delete_fact, namespace, key)
        return {"namespace": namespace, "key": key, "deleted": True}

    return server


def mem_ref(mem: AutumnMemory) -> str:
    # opaque tenant/agent ref for result URLs (best-effort; never raises)
    t = getattr(mem, "_tenant", None)
    a = getattr(mem, "_agent", None)
    return f"{t or '_'}/{a or '_'}"


def _auto_id(text: str) -> str:
    import hashlib

    return "m-" + hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def main(argv: Optional[list] = None) -> None:
    """CLI / env entry point — construct an `AutumnMemory` and run stdio.

    MCP hosts launch servers as child processes, typically passing config via
    the `env` block of their server config, so env vars are the primary source;
    CLI flags override. Lexical (BM25) search needs no embedder, so the default
    server runs with no external endpoint. Set --embed-url + --embed-model (or
    AUTUMN_MEMORY_EMBED_URL / _MODEL) to enable the vector/hybrid legs against an
    OpenAI-compatible /embeddings endpoint (sglang / vLLM / OpenAI).
    """
    import argparse

    p = argparse.ArgumentParser(prog="autumn-memory-mcp", description="autumn-memory stdio MCP server")
    p.add_argument("--manager", default=os.environ.get("AUTUMN_MEMORY_MANAGER", "127.0.0.1:9001"))
    p.add_argument("--tenant", default=os.environ.get("AUTUMN_MEMORY_TENANT", "default"))
    p.add_argument("--agent", default=os.environ.get("AUTUMN_MEMORY_AGENT", "default"))
    p.add_argument("--transport", default=os.environ.get("AUTUMN_MEMORY_TRANSPORT") or None)
    p.add_argument("--default-ttl", type=int, default=int(os.environ.get("AUTUMN_MEMORY_TTL", "0")))
    p.add_argument("--name", default=os.environ.get("AUTUMN_MEMORY_NAME", "autumn-memory"))
    p.add_argument("--embed-url", default=os.environ.get("AUTUMN_MEMORY_EMBED_URL") or None)
    p.add_argument("--embed-model", default=os.environ.get("AUTUMN_MEMORY_EMBED_MODEL") or None)
    # search mode: 'auto' = hybrid iff an embedder is configured, else lexical.
    # Forcing 'lexical' keeps `search` available even if the embedding endpoint
    # is down (hybrid/vector make every query depend on that endpoint).
    p.add_argument(
        "--default-mode",
        choices=("auto", "lexical", "hybrid", "vector"),
        default=os.environ.get("AUTUMN_MEMORY_DEFAULT_MODE", "auto"),
    )
    args = p.parse_args(argv)

    # fail fast on a half-configured embedder rather than silently degrading to
    # lexical (a misconfig that's hard to notice — search quality just looks off)
    if bool(args.embed_url) != bool(args.embed_model):
        p.error("--embed-url and --embed-model must be set together (or via AUTUMN_MEMORY_EMBED_URL/_MODEL)")

    has_embed = bool(args.embed_url and args.embed_model)
    mode = args.default_mode
    if mode == "auto":
        mode = "hybrid" if has_embed else "lexical"
    # validate config BEFORE connecting (AutumnMemory connects on construct)
    if mode in ("hybrid", "vector") and not has_embed:
        p.error(f"--default-mode {mode} requires an embedder (set --embed-url/--embed-model)")

    embed = None
    if has_embed:
        from autumn_memory import http_embedder

        embed = http_embedder(args.embed_url, args.embed_model)

    mem = AutumnMemory(
        args.manager,
        args.tenant,
        args.agent,
        embed=embed,
        default_ttl=args.default_ttl,
        transport=args.transport,
    )
    server = build_server(mem, name=args.name, default_mode=mode)
    server.run()  # stdio transport


if __name__ == "__main__":
    main()
