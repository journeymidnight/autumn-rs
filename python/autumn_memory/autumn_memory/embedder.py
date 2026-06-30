"""A text embedder for the vector / hybrid retrieval legs.

`AutumnMemory(embed=...)` (and the adapters) take an `embed: Callable[[str],
list[float]]`. This module provides a ready one that talks the
**OpenAI-compatible `/embeddings`** protocol — which sglang, vLLM, OpenAI, and
most inference servers all speak — so you don't have to write the HTTP glue.

Stdlib only (`urllib.request`); no extra dependency.

    from autumn_memory import AutumnMemory, http_embedder
    embed = http_embedder("http://127.0.0.1:30000/v1", "BAAI/bge-m3")
    mem = AutumnMemory("127.0.0.1:9001", "org", "agent", embed=embed)
    mem.remember("d1", "the cat sat on the mat")       # BM25 + vector
    mem.search("a feline", mode="vector")
"""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Callable, List, Optional
from urllib.parse import urlparse

__all__ = ["http_embedder", "http_embed_many"]


def _is_loopback(host: str) -> bool:
    h = (host or "").strip("[]").lower()
    return h in ("localhost", "::1", "0.0.0.0") or h == "127.0.0.1" or h.startswith("127.")


def http_embed_many(
    base_url: str,
    model: str,
    inputs: List[str],
    *,
    api_key: Optional[str] = None,
    timeout: float = 30.0,
    dimensions: Optional[int] = None,
) -> List[List[float]]:
    """Embed a batch of texts via `POST {base_url}/embeddings`.

    Returns one vector per input, in input order. `base_url` may or may not end
    in `/embeddings` — it is appended if absent (so both `.../v1` and
    `.../v1/embeddings` work).

    Auth: the bearer token comes from `api_key` or, if unset, the purpose-named
    `AUTUMN_EMBED_API_KEY` env (NOT `OPENAI_API_KEY` — that would silently leak a
    real OpenAI key to whatever `base_url` is configured). To avoid leaking a
    token in cleartext, a key is refused over plaintext `http://` to a
    non-loopback host.
    """
    if not inputs:  # nothing to embed — don't make a request many endpoints reject
        return []
    url = base_url if base_url.rstrip("/").endswith("/embeddings") else base_url.rstrip("/") + "/embeddings"
    payload: dict = {"model": model, "input": inputs}
    if dimensions is not None:
        payload["dimensions"] = dimensions
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    key = api_key if api_key is not None else os.environ.get("AUTUMN_EMBED_API_KEY")
    if key:
        parsed = urlparse(url)
        if parsed.scheme != "https" and not _is_loopback(parsed.hostname or ""):
            raise ValueError(
                f"refusing to send an API key in cleartext to {parsed.scheme}://{parsed.hostname} "
                "(use https, a loopback host, or unset the key)"
            )
        headers["Authorization"] = f"Bearer {key}"
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        doc = json.loads(resp.read().decode("utf-8"))
    # OpenAI shape: {"data": [{"embedding": [...], "index": i}, ...]}.
    data = doc["data"]
    if len(data) != len(inputs):
        raise ValueError(f"embeddings endpoint returned {len(data)} vectors for {len(inputs)} inputs")
    # If indices are present they MUST cover range(n) exactly (no dup/missing/
    # out-of-range), else text↔vector pairing would be silently wrong; if none
    # carry an index, trust the returned order.
    if any("index" in d for d in data):
        idx = sorted(d.get("index") for d in data)
        if idx != list(range(len(inputs))):
            raise ValueError(f"embeddings response has invalid indices {idx} for {len(inputs)} inputs")
        data = sorted(data, key=lambda d: d["index"])
    return [[float(x) for x in d["embedding"]] for d in data]


def http_embedder(
    base_url: str,
    model: str,
    *,
    api_key: Optional[str] = None,
    timeout: float = 30.0,
    dimensions: Optional[int] = None,
) -> Callable[[str], List[float]]:
    """Return a single-text `embed(text) -> list[float]` bound to an
    OpenAI-compatible `/embeddings` endpoint — drop into `AutumnMemory(embed=…)`.
    """

    def embed(text: str) -> List[float]:
        return http_embed_many(
            base_url, model, [text], api_key=api_key, timeout=timeout, dimensions=dimensions
        )[0]

    return embed
