#!/usr/bin/env python3
"""End-to-end check of the autumn-s3 write APIs with a real S3 SDK (boto3).

Run against a live gateway whose bucket already exists:

    uv run --with boto3 python scripts/s3_write_check.py \
        --endpoint http://127.0.0.1:9100 --bucket s3check

Every step asserts what an S3 client relies on: bytes, ETags, status codes and
error codes that the SDK parses into typed errors. The script creates its
objects under a fresh prefix and exits non-zero on the first failure.
"""

import argparse
import hashlib
import base64
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

MIB = 1 << 20
failures = 0


def step(name):
    print(f"-- {name}", flush=True)


def ok(cond, what):
    global failures
    if cond:
        print(f"   PASS {what}", flush=True)
    else:
        failures += 1
        print(f"   FAIL {what}", flush=True)
        sys.exit(1)


def err_code(fn, *a, **kw):
    """Run fn, expecting a ClientError; return (http status, error code)."""
    try:
        fn(*a, **kw)
    except ClientError as e:
        return e.response["ResponseMetadata"]["HTTPStatusCode"], e.response["Error"].get("Code")
    return 200, None


def body(n, seed):
    return hashlib.sha256(seed.encode()).digest() * (n // 32) + b"x" * (n % 32)


def race(args, b, p):
    s3 = boto3.client(
        "s3", endpoint_url=args.endpoint, aws_access_key_id="x", aws_secret_access_key="x",
        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )
    step("racing creators: exactly one If-None-Match: * wins (LanceDB's commit)")
    from concurrent.futures import ThreadPoolExecutor

    def client():
        return boto3.client(
            "s3", endpoint_url=args.endpoint, aws_access_key_id="x", aws_secret_access_key="x",
            region_name="us-east-1", config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
        )

    writers = [client() for _ in range(8)]
    for round_ in range(5):
        k = p + f"race/{round_}.manifest"

        def attempt(i):
            try:
                writers[i].put_object(Bucket=b, Key=k, Body=f"writer {i}".encode() * 100, IfNoneMatch="*")
                return i, 200
            except ClientError as e:
                return i, e.response["ResponseMetadata"]["HTTPStatusCode"]

        with ThreadPoolExecutor(len(writers)) as ex:
            results = list(ex.map(attempt, range(len(writers))))
        winners = [i for i, st in results if st == 200]
        others = sorted({st for _, st in results if st != 200})
        ok(len(winners) == 1 and others in ([412], [409], [409, 412]),
           f"round {round_}: one winner {winners}, the rest {others}")
        ok(s3.get_object(Bucket=b, Key=k)["Body"].read() == f"writer {winners[0]}".encode() * 100,
           f"round {round_}: the object is the winner's")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--race-only", action="store_true", help="run only the racing-creators check")
    args = ap.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id="x",
        aws_secret_access_key="x",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )
    b = args.bucket
    p = f"check-{uuid.uuid4().hex[:8]}/"
    if args.race_only:
        race(args, b, p)
        print("race check passed")
        return

    step("PutObject / GetObject / HeadObject")
    small = b"hello autumn"
    r = s3.put_object(Bucket=b, Key=p + "small.txt", Body=small)
    et = r["ETag"]
    ok(et.startswith('"') and len(et) == 34, f"PUT returns a quoted ETag {et}")
    g = s3.get_object(Bucket=b, Key=p + "small.txt")
    ok(g["Body"].read() == small, "GET returns the bytes")
    ok(g["ETag"] == et, "GET's ETag is the PUT's")
    h = s3.head_object(Bucket=b, Key=p + "small.txt")
    ok(h["ContentLength"] == len(small) and h["ETag"] == et, "HEAD length and ETag")
    ok(isinstance(h["LastModified"], datetime), "Last-Modified parses as an HTTP date")

    big = body(20 * MIB + 7, "big")
    r = s3.put_object(Bucket=b, Key=p + "a/b/big.bin", Body=big)
    g = s3.get_object(Bucket=b, Key=p + "a/b/big.bin")
    ok(g["Body"].read() == big, "20 MiB single PUT reads back byte-identical, parents created")
    g = s3.get_object(Bucket=b, Key=p + "a/b/big.bin", Range="bytes=8388600-16777300")
    ok(g["Body"].read() == big[8388600:16777301], "Range across the 8 MiB boundary")

    step("conditional PutObject")
    ok(err_code(s3.put_object, Bucket=b, Key=p + "small.txt", Body=b"x", IfNoneMatch="*") == (412, "PreconditionFailed"),
       "If-None-Match: * over an existing key is 412")
    r = s3.put_object(Bucket=b, Key=p + "new.txt", Body=b"first", IfNoneMatch="*")
    ok("ETag" in r, "If-None-Match: * creates a missing key")
    r2 = s3.put_object(Bucket=b, Key=p + "new.txt", Body=b"second", IfMatch=r["ETag"])
    ok(r2["ETag"] != r["ETag"], "If-Match with the current ETag replaces, and the ETag changes")
    ok(err_code(s3.put_object, Bucket=b, Key=p + "new.txt", Body=b"third", IfMatch=r["ETag"]) == (412, "PreconditionFailed"),
       "If-Match with a stale ETag is 412")
    ok(s3.get_object(Bucket=b, Key=p + "new.txt")["Body"].read() == b"second", "a refused PUT changes nothing")
    ok(err_code(s3.put_object, Bucket=b, Key=p + "missing.txt", Body=b"x", IfMatch=r2["ETag"]) == (404, "NoSuchKey"),
       "If-Match on a missing key is 404 NoSuchKey")
    same = s3.put_object(Bucket=b, Key=p + "same.txt", Body=b"abc")
    same2 = s3.put_object(Bucket=b, Key=p + "same.txt", Body=b"abd")
    ok(same["ETag"] != same2["ETag"], "a same-size rewrite in the same second changes the ETag")

    step("Content-MD5")
    md5 = base64.b64encode(hashlib.md5(b"digest me").digest()).decode()
    s3.put_object(Bucket=b, Key=p + "md5.txt", Body=b"digest me", ContentMD5=md5)
    ok(True, "a correct Content-MD5 is accepted")
    ok(err_code(s3.put_object, Bucket=b, Key=p + "md5.txt", Body=b"digest mf", ContentMD5=md5)[1] == "BadDigest",
       "a wrong Content-MD5 is BadDigest")
    ok(s3.get_object(Bucket=b, Key=p + "md5.txt")["Body"].read() == b"digest me", "the bad-digest PUT was not published")

    step("conditional GetObject / HeadObject")
    cur = s3.head_object(Bucket=b, Key=p + "new.txt")["ETag"]
    ok(err_code(s3.get_object, Bucket=b, Key=p + "new.txt", IfNoneMatch=cur)[0] == 304, "If-None-Match current is 304")
    ok(err_code(s3.get_object, Bucket=b, Key=p + "new.txt", IfMatch='"0"')[0] == 412, "If-Match other is 412")
    future = datetime.now(timezone.utc) + timedelta(days=1)
    past = datetime.now(timezone.utc) - timedelta(days=1)
    ok(err_code(s3.get_object, Bucket=b, Key=p + "new.txt", IfModifiedSince=future)[0] == 304, "If-Modified-Since future is 304")
    ok(err_code(s3.get_object, Bucket=b, Key=p + "new.txt", IfModifiedSince=past)[0] == 200, "If-Modified-Since past is 200")
    ok(err_code(s3.get_object, Bucket=b, Key=p + "new.txt", IfUnmodifiedSince=past)[0] == 412, "If-Unmodified-Since past is 412")
    ok(err_code(s3.head_object, Bucket=b, Key=p + "new.txt", IfNoneMatch=cur)[0] == 304, "HEAD honours If-None-Match")

    step("CopyObject")
    r = s3.copy_object(Bucket=b, Key=p + "copy/big.bin", CopySource={"Bucket": b, "Key": p + "a/b/big.bin"})
    ok("ETag" in r["CopyObjectResult"], "Copy returns a CopyObjectResult ETag")
    ok(s3.get_object(Bucket=b, Key=p + "copy/big.bin")["Body"].read() == big, "the copy is byte-identical")
    ok(err_code(s3.copy_object, Bucket=b, Key=p + "copy/x", CopySource={"Bucket": b, "Key": p + "small.txt"},
                CopySourceIfMatch='"0"')[0] == 412, "x-amz-copy-source-if-match mismatch is 412")
    ok(err_code(s3.copy_object, Bucket=b, Key=p + "copy/x", CopySource={"Bucket": b, "Key": p + "nope"}) == (404, "NoSuchKey"),
       "copying a missing source is NoSuchKey")
    s3.copy_object(Bucket=b, Key=p + "small.txt", CopySource={"Bucket": b, "Key": p + "small.txt"})
    ok(s3.get_object(Bucket=b, Key=p + "small.txt")["Body"].read() == small, "copying an object onto itself keeps its bytes")

    step("DeleteObject / DeleteObjects")
    ok(err_code(s3.delete_object, Bucket=b, Key=p + "new.txt", IfMatch='"0"')[0] == 412, "DeleteObject If-Match mismatch is 412")
    s3.delete_object(Bucket=b, Key=p + "new.txt")
    ok(err_code(s3.get_object, Bucket=b, Key=p + "new.txt") == (404, "NoSuchKey"), "a deleted key is NoSuchKey")
    s3.delete_object(Bucket=b, Key=p + "new.txt")
    ok(True, "deleting a missing key succeeds")
    r = s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": p + "same.txt"}, {"Key": p + "md5.txt"}, {"Key": p + "gone"}]})
    ok(sorted(d["Key"] for d in r.get("Deleted", [])) == sorted([p + "same.txt", p + "md5.txt", p + "gone"]) and not r.get("Errors"),
       "DeleteObjects reports every key, a missing one as deleted")
    ok(err_code(s3.head_object, Bucket=b, Key=p + "same.txt")[0] == 404, "DeleteObjects removed the objects")
    r = s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": p + "copy/x"}], "Quiet": True})
    ok(not r.get("Deleted"), "Quiet mode reports no successes")

    step("multipart upload")
    key = p + "mp/obj.bin"
    parts = [body(5 * MIB, "p1"), body(5 * MIB + 3, "p2"), body(MIB + 11, "p3")]
    up = s3.create_multipart_upload(Bucket=b, Key=key)["UploadId"]
    etags = {}
    for n in (3, 1, 2):  # out of order
        etags[n] = s3.upload_part(Bucket=b, Key=key, UploadId=up, PartNumber=n, Body=parts[n - 1])["ETag"]
    retry = s3.upload_part(Bucket=b, Key=key, UploadId=up, PartNumber=2, Body=parts[1])["ETag"]
    ok(retry != etags[2], "a retried part gets its own ETag")
    listing = [{"PartNumber": 1, "ETag": etags[1]}, {"PartNumber": 2, "ETag": etags[2]}, {"PartNumber": 3, "ETag": etags[3]}]
    ok(err_code(s3.complete_multipart_upload, Bucket=b, Key=key, UploadId=up, MultipartUpload={"Parts": listing})
       == (400, "InvalidPart"), "Complete naming a superseded part ETag is InvalidPart")
    listing[1]["ETag"] = retry
    ok(err_code(s3.complete_multipart_upload, Bucket=b, Key=key, UploadId=up,
                MultipartUpload={"Parts": [listing[2], listing[0]]}) == (400, "InvalidPartOrder"), "parts out of order are InvalidPartOrder")
    s3.put_object(Bucket=b, Key=key, Body=b"occupied")
    ok(err_code(s3.complete_multipart_upload, Bucket=b, Key=key, UploadId=up, MultipartUpload={"Parts": listing},
                IfNoneMatch="*") == (412, "PreconditionFailed"), "Complete with If-None-Match: * over an object is 412")
    r = s3.complete_multipart_upload(Bucket=b, Key=key, UploadId=up, MultipartUpload={"Parts": listing})
    ok(r["ETag"].startswith('"'), "Complete returns the object's ETag (the upload reopened after the 412)")
    ok(s3.get_object(Bucket=b, Key=key)["Body"].read() == b"".join(parts), "the completed object is the parts in order")
    ok(s3.head_object(Bucket=b, Key=key)["ETag"] == r["ETag"], "HEAD's ETag is Complete's")
    g = s3.get_object(Bucket=b, Key=key, Range=f"bytes={5 * MIB - 10}-{5 * MIB + 9}")
    ok(g["Body"].read() == b"".join(parts)[5 * MIB - 10:5 * MIB + 10], "a Range across two parts")
    ok(err_code(s3.upload_part, Bucket=b, Key=key, UploadId=up, PartNumber=4, Body=b"x") == (404, "NoSuchUpload"),
       "a part for a completed upload is NoSuchUpload")

    up = s3.create_multipart_upload(Bucket=b, Key=p + "mp/small")["UploadId"]
    e1 = s3.upload_part(Bucket=b, Key=p + "mp/small", UploadId=up, PartNumber=1, Body=b"a" * MIB)["ETag"]
    e2 = s3.upload_part(Bucket=b, Key=p + "mp/small", UploadId=up, PartNumber=2, Body=b"b")["ETag"]
    ok(err_code(s3.complete_multipart_upload, Bucket=b, Key=p + "mp/small", UploadId=up,
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": e1}, {"PartNumber": 2, "ETag": e2}]})
       == (400, "EntityTooSmall"), "a non-last part under 5 MiB is EntityTooSmall")
    s3.abort_multipart_upload(Bucket=b, Key=p + "mp/small", UploadId=up)
    ok(err_code(s3.complete_multipart_upload, Bucket=b, Key=p + "mp/small", UploadId=up,
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": e1}]}) == (404, "NoSuchUpload"),
       "Complete after Abort is NoSuchUpload")
    ok(err_code(s3.head_object, Bucket=b, Key=p + "mp/small")[0] == 404, "an aborted upload publishes nothing")
    ok(err_code(s3.abort_multipart_upload, Bucket=b, Key=p + "mp/small", UploadId="0123456789abcdef")[0] == 404,
       "aborting an unknown upload is 404")
    up = s3.create_multipart_upload(Bucket=b, Key=p + "mp/one")["UploadId"]
    ok(err_code(s3.abort_multipart_upload, Bucket=b, Key=p + "mp/other", UploadId=up)[0] == 404,
       "an upload id is valid only for its own key")
    s3.abort_multipart_upload(Bucket=b, Key=p + "mp/one", UploadId=up)

    step("boto3 managed transfer (parallel parts, the SDK's default checksums)")
    data = os.urandom(23 * MIB + 5)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(data)
        path = f.name
    cfg = TransferConfig(multipart_threshold=8 * MIB, multipart_chunksize=5 * MIB, max_concurrency=4)
    s3.upload_file(path, b, p + "managed.bin", Config=cfg)
    got = s3.get_object(Bucket=b, Key=p + "managed.bin")["Body"].read()
    ok(got == data, "a managed multipart upload reads back byte-identical")
    out = path + ".out"
    s3.download_file(b, p + "managed.bin", out, Config=cfg)
    ok(open(out, "rb").read() == data, "a managed parallel ranged download is byte-identical")
    os.unlink(path)
    os.unlink(out)

    race(args, b, p)

    step("directory keys and listing")
    s3.put_object(Bucket=b, Key=p + "emptydir/", Body=b"")
    ok(True, "a key ending in / creates a directory")
    ok(err_code(s3.put_object, Bucket=b, Key=p + "x//y", Body=b"z")[1] == "InvalidArgument", "an empty key component is InvalidArgument")
    r = s3.list_objects_v2(Bucket=b, Prefix=p, Delimiter="/")
    names = sorted(c["Key"] for c in r.get("Contents", []))
    prefixes = sorted(c["Prefix"] for c in r.get("CommonPrefixes", []))
    ok(names == sorted([p + "managed.bin", p + "small.txt"]), f"top-level objects listed: {names}")
    ok(p + "mp/" in prefixes and p + "a/" in prefixes, f"common prefixes listed: {prefixes}")
    r = s3.list_objects_v2(Bucket=b, Prefix=p + "mp/")
    ok([c["Key"] for c in r.get("Contents", [])] == [key], "only the completed multipart object is listed")
    ok(err_code(s3.put_object, Bucket="no-such-bucket-here", Key="k", Body=b"x") == (404, "NoSuchBucket"),
       "a PUT to a missing bucket is NoSuchBucket")

    print(f"\nall checks passed ({time.strftime('%H:%M:%S')})")


if __name__ == "__main__":
    main()
