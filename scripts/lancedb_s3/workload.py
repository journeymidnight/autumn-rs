"""LanceDB acceptance workload against an S3 endpoint.

Usage: workload.py <endpoint> <bucket> <prefix> [big_mib]
Every step prints a marker so a trace can be cut per phase.
"""
import datetime
import multiprocessing as mp
import os
import sys
import time

import lancedb
import numpy as np
import pyarrow as pa

ENDPOINT, BUCKET, PREFIX = sys.argv[1], sys.argv[2], sys.argv[3]
BIG_MIB = int(sys.argv[4]) if len(sys.argv) > 4 else 64
DIM = 128
OPTS = {
    "aws_access_key_id": "x",
    "aws_secret_access_key": "x",
    "aws_endpoint": ENDPOINT,
    "aws_region": "us-east-1",
    "allow_http": "true",
    "aws_virtual_hosted_style_request": "false",
}
URI = f"s3://{BUCKET}/{PREFIX}"


def mark(s):
    print(f"@@ {s}", flush=True)
    try:
        import urllib.request
        urllib.request.urlopen(f"{ENDPOINT}/__mark__/{s}", timeout=2).read()
    except Exception:
        pass


def batch(start, n, seed=0):
    rng = np.random.default_rng(seed + start)
    vec = rng.standard_normal((n, DIM), dtype=np.float32)
    return pa.table({
        "id": pa.array(np.arange(start, start + n, dtype=np.int64)),
        "vector": pa.FixedSizeListArray.from_arrays(pa.array(vec.reshape(-1)), DIM),
        "text": pa.array([f"row-{i}" for i in range(start, start + n)]),
    })


def writer(tag, barrier, out):
    db = lancedb.connect(URI, storage_options=OPTS)
    t = db.open_table("docs")
    barrier.wait()
    try:
        t.add(batch(10_000_000 + tag * 1000, 100, seed=tag))
        out.put((tag, "ok"))
    except Exception as e:  # a conflict must be explicit, never silent
        out.put((tag, f"err: {type(e).__name__}: {e}"))


def main():
    mark("connect")
    db = lancedb.connect(URI, storage_options=OPTS)

    mark("create")
    t = db.create_table("docs", batch(0, 1000), mode="overwrite")

    mark("append")
    t.add(batch(1000, 1000))

    mark("big_append")
    rows = BIG_MIB * 1024 * 1024 // (DIM * 4)
    t.add(batch(100_000, rows))

    mark("reopen")
    db2 = lancedb.connect(URI, storage_options=OPTS)
    t2 = db2.open_table("docs")
    n = t2.count_rows()
    assert n == 2000 + rows, (n, rows)

    mark("search")
    q = batch(0, 1000)["vector"][5].as_py()
    hits = t2.search(q).limit(3).to_list()
    assert hits[0]["id"] == 5, hits[0]["id"]

    mark("delete")
    t2.delete("id < 500")
    assert t2.count_rows() == 1500 + rows

    mark("concurrent")
    ctx = mp.get_context("spawn")
    barrier, out = ctx.Barrier(2), ctx.Queue()
    ps = [ctx.Process(target=writer, args=(i, barrier, out)) for i in range(2)]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
    res = sorted(out.get() for _ in ps)
    print("concurrent:", res, flush=True)
    ok = sum(1 for _, r in res if r == "ok")
    got = lancedb.connect(URI, storage_options=OPTS).open_table("docs").count_rows()
    assert got == 1500 + rows + 100 * ok, (got, ok)

    mark("optimize")
    t3 = lancedb.connect(URI, storage_options=OPTS).open_table("docs")
    t3.optimize(cleanup_older_than=datetime.timedelta(seconds=0), delete_unverified=True)
    assert t3.count_rows() == 1500 + rows + 100 * ok

    mark("verify")
    t4 = lancedb.connect(URI, storage_options=OPTS).open_table("docs")
    ids = t4.search().where("id >= 100000 AND id < 100010").select(["id", "vector"]).to_arrow()
    want = batch(100_000, rows)
    got_vec = {r: v for r, v in zip(ids["id"].to_pylist(), ids["vector"].to_pylist())}
    for i in range(100_000, 100_010):
        assert np.allclose(got_vec[i], want["vector"][i - 100_000].as_py()), i

    mark("drop")
    db.drop_table("docs")
    mark("done")
    print("WORKLOAD OK", flush=True)


if __name__ == "__main__":
    main()
