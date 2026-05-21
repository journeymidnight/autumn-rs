"""F216-E kvcache ZC A/B micro-bench (no sglang).

Drives autumn.BatchClient over a contiguous pinned-style host pool (numpy),
exactly the kvcache v1 hot path (per-page put_from / get_into into pinned
pages), and compares the regular path (zc=False) vs the zero-copy path
(zc=True: MSG_PUT_ZC write + MSG_GET_ZC read).

Run against a live UCX cluster:
    AUTUMN_KVCACHE_ENDPOINT="[<roce-ip>]:9001" \
      AUTUMN_BENCH_PAGES=512 AUTUMN_BENCH_PAGE_KB=256 \
      /tmp/autumn-py-venv/bin/python -m autumn_kvcache.tests.bench_zc
"""
from __future__ import annotations
import os, time, sys
import numpy as np
import autumn

ENDPOINT = os.environ.get("AUTUMN_KVCACHE_ENDPOINT", "127.0.0.1:9001")
TRANSPORT = os.environ.get("AUTUMN_TRANSPORT", "ucx")
N_PAGES = int(os.environ.get("AUTUMN_BENCH_PAGES", "512"))
PAGE = int(os.environ.get("AUTUMN_BENCH_PAGE_KB", "256")) * 1024
N_WORKERS = int(os.environ.get("AUTUMN_BENCH_WORKERS", "8"))
CAP = int(os.environ.get("AUTUMN_BENCH_CAP", "8"))
ROUNDS = int(os.environ.get("AUTUMN_BENCH_ROUNDS", "5"))


def run(zc: bool):
    bc = autumn.BatchClient(ENDPOINT, N_WORKERS, CAP, zc)
    # One contiguous pool; per-page views (zero-copy numpy slices).
    pool = np.zeros(N_PAGES * PAGE, dtype=np.uint8)
    for i in range(N_PAGES):
        pool[i * PAGE] = (i % 251) + 1  # make pages distinguishable
    views = [pool[i * PAGE:(i + 1) * PAGE] for i in range(N_PAGES)]
    keys = [f"zcbench/{zc}/{i:08d}".encode() for i in range(N_PAGES)]
    total_mb = N_PAGES * PAGE / 1e6

    # Warm (also the write under test).
    def time_put():
        t = time.perf_counter()
        ok = bc.put_from(keys, views)
        dt = time.perf_counter() - t
        return sum(ok), dt
    def time_get():
        # read into a fresh pool to verify content separately if desired
        t = time.perf_counter()
        ok = bc.get_into(keys, views)
        dt = time.perf_counter() - t
        return sum(ok), dt

    okw, _ = time_put()  # warm + first write
    best_w = min(time_put()[1] for _ in range(ROUNDS))
    okr = time_get()[0]
    best_r = min(time_get()[1] for _ in range(ROUNDS))
    print(f"  zc={zc!s:5}  write {total_mb/best_w:8.1f} MB/s ({N_PAGES/best_w:7.0f} pg/s, ok={okw}/{N_PAGES})"
          f"  |  read {total_mb/best_r:8.1f} MB/s ({N_PAGES/best_r:7.0f} pg/s, ok={okr}/{N_PAGES})")
    return total_mb / best_w, total_mb / best_r


def main():
    autumn.set_transport(TRANSPORT)
    print(f"endpoint={ENDPOINT} transport={TRANSPORT} pages={N_PAGES} page={PAGE//1024}KiB "
          f"workers={N_WORKERS} cap={CAP} rounds={ROUNDS}")
    w0, r0 = run(False)
    w1, r1 = run(True)
    print(f"  => write ZC/regular = {w1/w0:.2f}x   read ZC/regular = {r1/r0:.2f}x")


if __name__ == "__main__":
    main()
