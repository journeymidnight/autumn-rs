#!/usr/bin/env python3
"""Does a large DELETE or AbortMultipartUpload hold up the gateway worker?

Run against a gateway started with --workers 1, so every request shares one
worker's state lock:

    uv run --with boto3 python scripts/s3_stall_check.py \
        --endpoint http://127.0.0.1:9100 --bucket stall

A prober HEADs a small object in a tight loop while the main thread deletes a
large multipart object and aborts a many-part upload. Deleting data must not
happen under the worker's lock, so a HEAD issued meanwhile must answer at about
its idle latency; the script exits non-zero if one waited longer than
--max-stall-ms. The objects it creates live under a fresh prefix.
"""

import argparse
import statistics
import sys
import threading
import time
import uuid

import boto3
from botocore.config import Config

MIB = 1 << 20


def client(endpoint):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="x",
        aws_secret_access_key="x",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}, max_pool_connections=4),
    )


class Prober:
    def __init__(self, endpoint, bucket, key):
        self.c = client(endpoint)
        self.bucket, self.key = bucket, key
        self.samples = []  # (start, latency)
        self.stop = False

    def run(self):
        while not self.stop:
            t = time.monotonic()
            self.c.head_object(Bucket=self.bucket, Key=self.key)
            self.samples.append((t, time.monotonic() - t))

    def window(self, a, b):
        return [lat for (t, lat) in self.samples if a <= t <= b or (t < a and t + lat > a)]


def summarize(name, lats):
    if not lats:
        return f"{name}: no samples"
    lats = sorted(lats)
    p50 = lats[len(lats) // 2] * 1e3
    p99 = lats[min(len(lats) - 1, int(len(lats) * 0.99))] * 1e3
    return f"{name}: n={len(lats)} p50={p50:.1f}ms p99={p99:.1f}ms max={lats[-1]*1e3:.1f}ms"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--parts", type=int, default=200)
    ap.add_argument("--big-mib", type=int, default=1000)
    ap.add_argument("--max-stall-ms", type=float, default=10.0)
    args = ap.parse_args()
    c = client(args.endpoint)
    pre = f"stall-{uuid.uuid4().hex[:8]}"
    small = f"{pre}/small"
    c.put_object(Bucket=args.bucket, Key=small, Body=b"s" * 1000)

    part = b"p" * (5 * MIB)
    # Setup: the big object and the upload, before any probing.
    big = f"{pre}/big"
    up = c.create_multipart_upload(Bucket=args.bucket, Key=big)["UploadId"]
    etags = []
    for i in range(1, args.big_mib // 5 + 1):
        etags.append({"PartNumber": i, "ETag": c.upload_part(Bucket=args.bucket, Key=big, UploadId=up, PartNumber=i, Body=part)["ETag"]})
    c.complete_multipart_upload(Bucket=args.bucket, Key=big, UploadId=up, MultipartUpload={"Parts": etags})
    abort_key = f"{pre}/aborted"
    up2 = c.create_multipart_upload(Bucket=args.bucket, Key=abort_key)["UploadId"]
    for i in range(1, args.parts + 1):
        c.upload_part(Bucket=args.bucket, Key=abort_key, UploadId=up2, PartNumber=i, Body=part)
    print(f"setup: {len(etags)}-part {len(etags)*5} MiB object, {args.parts}-part upload", flush=True)

    p = Prober(args.endpoint, args.bucket, small)
    th = threading.Thread(target=p.run, daemon=True)
    th.start()
    time.sleep(2.0)
    idle_a, idle_b = time.monotonic() - 2.0, time.monotonic()

    t0 = time.monotonic()
    c.delete_object(Bucket=args.bucket, Key=big)
    t1 = time.monotonic()
    time.sleep(1.0)
    t2 = time.monotonic()
    c.abort_multipart_upload(Bucket=args.bucket, Key=abort_key, UploadId=up2)
    t3 = time.monotonic()
    time.sleep(1.0)
    p.stop = True
    th.join()

    print(f"DELETE {len(etags)*5} MiB took {(t1-t0)*1e3:.1f}ms; Abort of {args.parts} parts took {(t3-t2)*1e3:.1f}ms")
    print(summarize("idle  HEAD", p.window(idle_a, idle_b)))
    print(summarize("during DELETE HEAD", p.window(t0, t1)))
    print(summarize("after  DELETE HEAD (1s)", p.window(t1, t2)))
    print(summarize("during ABORT  HEAD", p.window(t2, t3)))
    print(summarize("after  ABORT  HEAD (1s)", p.window(t3, t3 + 1.0)))
    print(f"prefix {pre}")
    during = p.window(t0, t1) + p.window(t2, t3)
    worst = max(during, default=0.0) * 1e3
    if worst > args.max_stall_ms:
        print(f"FAIL a HEAD waited {worst:.1f}ms while data was being deleted (limit {args.max_stall_ms}ms)")
        return 1
    print(f"PASS worst HEAD during the deletes {worst:.1f}ms")
    return 0


if __name__ == "__main__":
    sys.exit(main())
