# autumn-object-store

Apache object_store 0.14.1 adapter. The current LanceDB integration target is
LanceDB df5709e / Lance 13.0.0-beta.6. No changes to Lance's query/index code.

## Storage and publication

The caller selects an existing namespace/sub-prefix exclusively for this format.
Within it, m/<path> is a small JSON metadata record (version 1, UUID generation,
UTC modification time, multipart part lengths). Payloads are immutable KV chunks:
d/<generation>/<part hex>/<chunk hex>, with 4 MiB chunks inside each part.
Ordinary puts use the same layout as one-part multipart uploads.

Publish metadata only after all data writes acknowledge. Create uses SDK
compare_put(expected=None); Update reads the current metadata, checks its ETag,
then compares the exact metadata bytes in the PS. UUID generations prevent ABA
when content is identical. Conflict is a PutResp body code, distinct from stale
region frame errors. A lost-ACK retry that encounters its own UUID is success.

Overwrites/deletes retain old payloads so previously returned GetResult streams
remain readable. vacuum_quiescent is explicitly OFFLINE: no readers, pending
uploads or concurrent writes anywhere in that scope. It scans live generations,
then deletes unreferenced chunks. Aborted/failed uploads are also reclaimed here.
There is no online payload GC, historical-version API, or persisted attributes.

## Runtime and performance

ObjectStore is Send + Sync; ClusterClient is Rc/compio and is not Send. A dedicated
compio thread owns each client's SDK. A bounded flume queue carries owned jobs,
with 32 active jobs and 32 queued. Cancellation does not cancel already accepted
mutations. All SDK futures and Rc values stay on the worker thread.

Large aligned buffers go directly to put_bulk. Reads return pooled Bytes and
fetch only intersecting chunks, with four in flight. Multipart writes track part
completion independently of future poll order. Copy allocates a new generation.

RANGE is keys-only. List scans 256 keys, then get_many fetches the small metadata
records; payloads are never loaded. Resume is last_key + NUL; cursor is clamped
to the directory prefix. object_store prefixes are directory segments, excluding
an object exactly equal to the prefix. Preserve percent-encoded path components.

## Tests

tests/contract.rs boots real RF2 services and runs upstream object_store contract
tests plus independent-worker Create/Update races, update after SST flush,
cross-chunk reads, snapshot-after-overwrite/delete, 1100-fragment pagination and
offline vacuum. The CAS ablation bypasses the PS mismatch branch and must fail
because both creates succeed. The macOS harness disables unsupported CPU pinning;
Linux uses the production affinity policy. examples/test_cluster.rs is a dedicated
disposable cluster for demos, with random ports and task-local TMPDIR.
