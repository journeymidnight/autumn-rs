# LanceDB over Autumn S3

This example uses the stock Python LanceDB client against `autumn-s3`. It is
both a runnable introduction and the compatibility workload for the S3 surface;
there is no custom LanceDB build or Autumn-specific query layer.

Start an Autumn cluster and gateway, then create the bucket as a first-level
directory in the shared `fs/` tree:

```sh
cargo build --release -p autumn-server -p autumn-fuse
./cluster.sh start 3
./target/release/autumnfs --manager 127.0.0.1:9001 mkdir /tables
./target/release/autumn-s3 --manager 127.0.0.1:9001 --port 9100 &
```

The client is pinned: `lancedb==0.39.0` (it bundles Lance 12.0.0), resolved in
`uv.lock`. Use `uv run` from this directory so the locked environment is used,
never a locally modified LanceDB.

```sh
cd examples/lancedb-s3
uv sync

# Workload: create, append, a multipart-sized append, reopen, vector search,
# delete, two processes committing concurrently, optimize+cleanup (vacuum),
# byte check, drop. Prints WORKLOAD OK. The bucket must exist beforehand
# (for autumn-s3 that is a first-level directory under fs/).
uv run python workload.py http://127.0.0.1:9100 <bucket> <prefix> [big_mib]

# Record the requests a run sends: the proxy forwards to an upstream S3 and
# appends one JSON line per request (method, path+query, the headers that
# matter for compatibility, body size, status).
uv run python trace_proxy.py <upstream_host> <upstream_port> <listen_port> trace.jsonl &
uv run python workload.py http://127.0.0.1:<listen_port> <bucket> <prefix>
python3 analyze.py trace.jsonl     # one line per (method, query keys, headers, status)
```

`lancedb-0.39.0-moto-trace.jsonl` is the reference trace, recorded against
moto's S3 server. What it shows the gateway must serve:

- `GET` with `Range` (data files, manifests), `HEAD` object;
- `ListObjectsV2` with and without `delimiter=/` (`_versions/`, `_refs/branches/`);
- plain `PUT`, and `PUT` with `If-None-Match: *` for every manifest commit — the
  loser of a concurrent commit gets `412 PreconditionFailed` and then `HEAD`s /
  `GET`s the winning manifest, so a published object must never be visible
  partially;
- `POST ?delete` (DeleteObjects) with `Content-MD5`;
- multipart: `POST ?uploads`, `PUT ?partNumber&uploadId` with 5 MiB parts,
  `POST ?uploadId` (Complete).

Not sent by this client (still part of the feature's acceptance): CopyObject,
DeleteObject, `If-Match`, AbortMultipartUpload, HeadBucket.
