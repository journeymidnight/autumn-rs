"""Recall-latency benchmark (plan §13 "先测 P99"): the agent prefetch path is a
keys-only posting scan + per-candidate doc point-get (the §14 two-hop), so its
P99 decides whether memory recall is fast enough to sit on the turn loop.

Indexes a realistic single-agent corpus and measures `search_lexical` (= the
Hermes provider's lexical prefetch) latency over many queries. Lexical-only so
it needs no embedder; the vector leg's cost is the embedder RPC (measured
elsewhere).

    AUTUMN_MEMORY_MANAGER=127.0.0.1:19001 BENCH_N=2000 BENCH_Q=300 \
      python python/autumn_memory/tests/bench_recall.py
"""

import os
import time
import uuid

from autumn_memory import AutumnMemory

# a small vocabulary → realistic term-frequency distribution (some common terms
# with long posting lists, many rare ones).
COMMON = "system error data memory cluster node write read index cache".split()
MID = ("deploy config schema latency throughput replica partition stream extent "
       "compaction recovery commit fence epoch vector embedding hybrid recall token").split()
RARE = [f"w{i}" for i in range(400)]


def _doc(rng_a: int, rng_b: int) -> str:
    # deterministic pseudo-random text from two ints (no Math.random needed)
    words = []
    s = rng_a * 2654435761 + rng_b * 40503
    for _ in range(20):
        s = (s * 1103515245 + 12345) & 0x7FFFFFFF
        r = s % 100
        if r < 45:
            words.append(COMMON[s % len(COMMON)])
        elif r < 80:
            words.append(MID[s % len(MID)])
        else:
            words.append(RARE[s % len(RARE)])
    return " ".join(words)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))]


def main():
    manager = os.environ["AUTUMN_MEMORY_MANAGER"]
    n = int(os.environ.get("BENCH_N", "2000"))
    q = int(os.environ.get("BENCH_Q", "300"))
    if n <= 0 or q <= 0:
        raise SystemExit(f"BENCH_N and BENCH_Q must be > 0 (got N={n}, Q={q})")
    mem = AutumnMemory(manager, "__am_bench", "agent-" + uuid.uuid4().hex[:8])

    t0 = time.monotonic()
    for i in range(n):
        mem.remember(f"d{i}", _doc(i, 7))
    idx_s = time.monotonic() - t0
    print(f"indexed {n} docs in {idx_s:.1f}s ({1000*idx_s/n:.2f} ms/doc)")

    # warm + measure recall over a mix of common (worst-case long postings),
    # mid, and rare query terms.
    queries = []
    for i in range(q):
        bucket = (COMMON, MID, RARE)[i % 3]
        queries.append(bucket[i % len(bucket)])

    lat = []
    hits_total = 0
    for term in queries:
        t = time.monotonic()
        res = mem.search(term, 5, mode="lexical")
        lat.append((time.monotonic() - t) * 1000.0)
        hits_total += len(res)

    print(f"recall over {q} queries (k=5): {hits_total} hits total")
    print(f"  P50={pct(lat,50):.1f}ms  P90={pct(lat,90):.1f}ms  "
          f"P99={pct(lat,99):.1f}ms  max={max(lat):.1f}ms")

    # isolate worst case: a COMMON term (longest posting list → most candidate gets)
    cl = [(time.monotonic(), mem.search(COMMON[0], 5, mode="lexical"), time.monotonic())
          for _ in range(50)]
    common_lat = sorted((c - a) * 1000.0 for a, _b, c in cl)
    print(f"  worst-term '{COMMON[0]}' (longest postings): "
          f"P50={common_lat[25]:.1f}ms  P99={common_lat[-1]:.1f}ms")

    for i in range(n):
        if i < 5 or i % 500 == 0:
            pass  # leave the corpus; unique agent, harness tears the cluster down
    mem.close()
    print("RECALL BENCH OK")


if __name__ == "__main__":
    main()
