"""Real-model semantic e2e: drive AutumnMemory's vector + hybrid legs against a
REAL OpenAI-compatible /embeddings endpoint (e.g. sglang) and an autumn cluster.

The decisive check is **semantic match with NO lexical overlap**: a query that
shares no tokens with the target doc must still recall it via the vector leg —
which only works if real embeddings (not the test's fake 3-dim one) are flowing.

    AUTUMN_MEMORY_MANAGER=127.0.0.1:19001 \
    AUTUMN_MEMORY_EMBED_URL=http://127.0.0.1:30000/v1 \
    AUTUMN_MEMORY_EMBED_MODEL=Alibaba-NLP/gte-Qwen2-1.5B-instruct \
      python python/autumn_memory/tests/test_real_embed.py
"""

import os
import uuid

from autumn_memory import AutumnMemory, http_embedder


def main():
    manager = os.environ["AUTUMN_MEMORY_MANAGER"]
    url = os.environ["AUTUMN_MEMORY_EMBED_URL"]
    model = os.environ["AUTUMN_MEMORY_EMBED_MODEL"]
    embed = http_embedder(url, model)

    # sanity: the endpoint returns a real, fixed-width vector
    v = embed("hello world")
    assert isinstance(v, list) and len(v) >= 64, f"embedding dim looks wrong: {len(v)}"
    dim = len(v)
    print(f"embedder OK: dim={dim}")

    mem = AutumnMemory(manager, "__am_real", "agent-" + uuid.uuid4().hex[:8], embed=embed)

    docs = {
        "cat": "the cat sat quietly on the warm windowsill",
        "code": "distributed consensus protocols tolerate node failures",
        "food": "she baked fresh sourdough bread this morning",
    }
    for did, text in docs.items():
        mem.remember(did, text)

    # --- vector leg: semantic, NO lexical overlap with the docs ---
    q1 = "a small furry pet animal that purrs"          # -> cat (no shared words)
    top = mem.search(q1, 1, mode="vector")
    assert top and top[0]["id"] == "cat", f"semantic vector recall failed: {top}"
    print(f"vector '{q1}' -> {top[0]['id']} ✓ (semantic, no lexical overlap)")

    q2 = "how do databases stay consistent when servers crash"   # -> code
    top2 = mem.search(q2, 1, mode="vector")
    assert top2 and top2[0]["id"] == "code", f"semantic vector recall failed: {top2}"
    print(f"vector '{q2}' -> {top2[0]['id']} ✓")

    # --- IVF path: train, then vector search through the buckets ---
    n = mem.train(2, 25, 7)
    assert n >= 1
    top3 = mem.search(q1, 1, mode="vector", nprobe=2)
    assert top3 and top3[0]["id"] == "cat", f"post-train IVF recall failed: {top3}"
    print(f"post-train IVF '{q1}' -> {top3[0]['id']} ✓")

    # --- hybrid: lexical term + semantic, fused ---
    hy = mem.search("warm windowsill", 3, mode="hybrid")    # 'cat' wins both legs
    assert any(h["id"] == "cat" for h in hy), f"hybrid failed: {hy}"
    print(f"hybrid 'warm windowsill' -> {[h['id'] for h in hy]} ✓")

    # --- CJK semantic (the multilingual model should embed Chinese too) ---
    mem.remember("zh", "小猫在阳光下睡觉")                    # "the kitten sleeps in the sun"
    zh = mem.search("可爱的宠物", 1, mode="vector")           # "a cute pet" — no shared chars
    print(f"vector '可爱的宠物' -> {zh[0]['id'] if zh else None} (CJK semantic; informational)")

    for did in list(docs) + ["zh"]:
        mem.forget(did)
    mem.close()
    print("REAL EMBED OK: vector + hybrid semantic recall against a real embeddings endpoint")


if __name__ == "__main__":
    main()
