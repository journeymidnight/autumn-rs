"""In-process MCP test: drive the autumn-memory MCP server through a real MCP
client over the SDK's in-memory transport (no subprocess, no stdio) against an
isolated cluster. Exercises the full tool surface end-to-end.

Run via tests/run_mcp_test.sh (brings up the cluster + venv). Standalone:
    AUTUMN_MEMORY_MANAGER=127.0.0.1:19001 python tests/test_mcp_inproc.py
"""

import asyncio
import os
import uuid

from mcp.shared.memory import create_connected_server_and_client_session as connect

from autumn_memory import AutumnMemory
from autumn_memory_mcp import build_server


def fake_embed(text):
    # deterministic 3-dim embedder (marker words) so the vector/hybrid legs run
    # without a real embeddings endpoint.
    t = text.lower()
    return [1.0 if "cat" in t else 0.0, 1.0 if "dog" in t else 0.0, 1.0 if "fish" in t else 0.0]


def payload(result):
    """Extract a tool's JSON payload (prefer structured content)."""
    if getattr(result, "structuredContent", None):
        return result.structuredContent
    import json

    for block in result.content:
        if getattr(block, "text", None):
            return json.loads(block.text)
    return None


async def call(sess, name, **args):
    res = await sess.call_tool(name, args)
    assert not res.isError, f"{name} errored: {res.content}"
    return payload(res)


async def main():
    manager = os.environ.get("AUTUMN_MEMORY_MANAGER", "127.0.0.1:19001")
    agent = "mcp-" + uuid.uuid4().hex[:8]
    mem = AutumnMemory(manager, "__am_mcp", agent)  # lexical-only (no embedder)
    server = build_server(mem, default_mode="lexical")

    async with connect(server) as session:
        # tool discovery
        tools = {t.name for t in (await session.list_tools()).tools}
        need = {
            "search", "fetch", "add", "update", "delete",
            "append_event", "recent_events", "replay_session",
            "put_fact", "get_fact", "list_facts", "delete_fact",
        }
        assert need <= tools, f"missing tools: {need - tools}"

        # add + search + fetch (ChatGPT-recognized pair)
        await call(session, "add", text="the cat sat on the mat", id="d1", metadata={"title": "cat note"})
        await call(session, "add", text="dogs chase cats in the yard", id="d2")
        await call(session, "add", text="quantum error correction codes", id="d3")

        sr = await call(session, "search", query="cat", k=10)
        ids = [r["id"] for r in sr["results"]]
        assert "d1" in ids and "d2" in ids and "d3" not in ids, ids   # plural fold cat~cats
        # every search result is self-contained (text resolved from the doc record)
        d1r = next(r for r in sr["results"] if r["id"] == "d1")
        assert "cat sat" in d1r["text"] and d1r["title"] == "cat note", d1r

        fr = await call(session, "fetch", id="d1")
        assert "cat sat on the mat" in fr["text"], fr

        # update changes what search/fetch see
        await call(session, "update", id="d1", text="the cat slept all day")
        fr2 = await call(session, "fetch", id="d1")
        assert "slept all day" in fr2["text"], fr2

        # delete removes from the index
        await call(session, "delete", id="d3")
        sq = await call(session, "search", query="quantum", k=5)
        assert sq["results"] == [], sq

        # fetch of an unknown id errors
        miss = await session.call_tool("fetch", {"id": "nope"})
        assert miss.isError, "fetch of unknown id should error"

        # episodic
        await call(session, "append_event", session="s1", event={"role": "user", "i": 0})
        await call(session, "append_event", session="s1", event={"role": "asst", "i": 1})
        rec = await call(session, "recent_events", session="s1", k=1)
        assert rec["events"][0]["i"] == 1, rec
        rep = await call(session, "replay_session", session="s1")
        assert [e["i"] for e in rep["events"]] == [0, 1], rep

        # facts
        await call(session, "put_fact", namespace="p", key="profile", value={"name": "Alice"})
        gf = await call(session, "get_fact", namespace="p", key="profile")
        assert gf["value"]["name"] == "Alice", gf
        lf = await call(session, "list_facts", namespace="p")
        assert len(lf["facts"]) == 1 and lf["facts"][0]["key"] == "profile", lf
        await call(session, "delete_fact", namespace="p", key="profile")
        gf2 = await call(session, "get_fact", namespace="p", key="profile")
        assert gf2["value"] is None, gf2

    mem.close()

    # -- coco P2 regression: a deleted vector-indexed doc must NOT surface as a
    # ghost result on the vector/hybrid legs. `delete` reaps the doc + BM25
    # postings but leaves the IVF posting orphaned; the search boundary must
    # drop any hit whose authoritative doc record is gone (plan §8.5).
    emem = AutumnMemory(manager, "__am_mcp", agent + "-v", embed=fake_embed)
    eserver = build_server(emem, default_mode="hybrid")
    async with connect(eserver) as es:
        await call(es, "add", text="the cat sat on the mat", id="v1")
        await call(es, "add", text="a dog barked loudly", id="v2")
        # both surface for a cat/dog query before deletion
        pre = await call(es, "search", query="cat and dog", k=5)
        assert {"v1", "v2"} <= {r["id"] for r in pre["results"]}, pre
        await call(es, "delete", id="v1")           # orphans ivf/*/v1
        post = await call(es, "search", query="a cat please", k=5)
        rids = [r["id"] for r in post["results"]]
        assert "v1" not in rids, f"ghost result for deleted v1: {post}"
        assert all(r["text"] for r in post["results"]), f"empty-text ghost: {post}"
        fr = await es.call_tool("fetch", {"id": "v1"})
        assert fr.isError, "fetch of deleted v1 should error"
    emem.close()

    print("MCP INPROC OK: full tool surface (search/fetch/add/update/delete + episodic + facts) + no-ghost-on-delete")


if __name__ == "__main__":
    asyncio.run(main())
