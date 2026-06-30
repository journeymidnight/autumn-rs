"""Self-contained test for the OpenAI-compatible embedder client.

No cluster needed: spins up a mock `/embeddings` HTTP server (stdlib) that
echoes a deterministic embedding per input, and checks the client speaks the
protocol — request shape, batch order, auth header, single-text hook.

    python python/autumn_memory/tests/test_embedder.py
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from autumn_memory import http_embed_many, http_embedder


def _vec(text: str):
    # deterministic 3-dim "embedding" derived from the text
    t = text.lower()
    return [float(len(t)), float(sum(c in "aeiou" for c in t)), 1.0 if "cat" in t else 0.0]


class Handler(BaseHTTPRequestHandler):
    seen = {}

    def log_message(self, *a):  # silence
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        req = json.loads(self.rfile.read(n).decode("utf-8"))
        Handler.seen["path"] = self.path
        Handler.seen["auth"] = self.headers.get("Authorization")
        Handler.seen["model"] = req.get("model")
        Handler.seen["calls"] = Handler.seen.get("calls", 0) + 1
        inputs = req["input"]
        if isinstance(inputs, str):
            inputs = [inputs]
        if req.get("model") == "__bad_index__":
            # duplicate index 1, missing 0 → must be rejected (text↔vec mispair)
            data = [{"embedding": _vec(s), "index": 1} for s in inputs]
        else:
            # return out-of-order indices to prove the client re-sorts
            data = [{"embedding": _vec(s), "index": i} for i, s in enumerate(inputs)]
            data.reverse()
        out = json.dumps({"data": data, "model": req.get("model")}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


def main():
    srv = HTTPServer(("127.0.0.1", 0), Handler)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        base = f"http://127.0.0.1:{port}/v1"

        # batch: order preserved despite the server reversing indices
        texts = ["the cat sat", "a dog ran", "quantum codes"]
        vecs = http_embed_many(base, "mock-model", texts, api_key="sk-test")
        assert vecs == [_vec(t) for t in texts], vecs
        assert Handler.seen["path"].endswith("/embeddings"), Handler.seen
        assert Handler.seen["auth"] == "Bearer sk-test", Handler.seen
        assert Handler.seen["model"] == "mock-model", Handler.seen

        # base_url that already includes /embeddings is not double-appended
        vecs2 = http_embed_many(base + "/embeddings", "mock-model", ["x"])
        assert Handler.seen["path"] == "/v1/embeddings", Handler.seen
        assert vecs2 == [_vec("x")]

        # single-text hook = the AutumnMemory(embed=...) shape
        embed = http_embedder(base, "mock-model")
        v = embed("the cat sat")
        assert v == _vec("the cat sat"), v
        assert isinstance(v, list) and all(isinstance(x, float) for x in v)

        # security (coco P1): a present OPENAI_API_KEY must NOT be auto-sent;
        # only the purpose-named AUTUMN_EMBED_API_KEY is.
        os.environ["OPENAI_API_KEY"] = "sk-must-not-leak"
        os.environ.pop("AUTUMN_EMBED_API_KEY", None)
        http_embed_many(base, "mock-model", ["hi"])
        assert Handler.seen["auth"] is None, "OPENAI_API_KEY must not be auto-attached"
        os.environ["AUTUMN_EMBED_API_KEY"] = "sk-autumn"
        http_embed_many(base, "mock-model", ["hi"])
        assert Handler.seen["auth"] == "Bearer sk-autumn", Handler.seen
        os.environ.pop("AUTUMN_EMBED_API_KEY", None)
        os.environ.pop("OPENAI_API_KEY", None)

        # security (coco P1): refuse to send a key in cleartext to a non-loopback
        # host (no request is made).
        try:
            http_embed_many("http://example.com/v1", "m", ["x"], api_key="sk-leak")
            raise AssertionError("expected cleartext-key refusal")
        except ValueError as e:
            assert "cleartext" in str(e), e

        # correctness (coco P2#3): invalid response indices are rejected
        try:
            http_embed_many(base, "__bad_index__", ["a", "b"])
            raise AssertionError("expected invalid-index rejection")
        except ValueError as e:
            assert "indices" in str(e), e

        # edge (coco P3#6): empty batch is a no-op, no request issued
        before = Handler.seen.get("calls", 0)
        assert http_embed_many(base, "mock-model", []) == []
        assert Handler.seen.get("calls", 0) == before, "empty batch must not call the endpoint"

        print("EMBEDDER OK: OpenAI-compatible /embeddings client (batch order, auth, url-join, hook, key-safety, index-validate, empty)")
    finally:
        srv.shutdown()


if __name__ == "__main__":
    main()
