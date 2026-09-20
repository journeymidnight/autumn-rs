# LanceDB object-store validation — 2026-09-19

Environment: H200-1, dongmao-autumn, Linux 6.1, Rust 1.95. All source, target,
logs and data are isolated under /data08/autumn-lancedb-20260919-01a0bb27.
Existing checkouts and services were not modified. Source transfers after the
initial copy use git bundle; checkout is a separate clone.

## Correctness

- object_store 0.14.1 upstream contract: put/get/head/delete, conditional reads,
  Create/Update races, directory prefixes and escaped paths, list offset,
  copy/rename conflicts, multipart uploaded in a different polling order: pass.
- Independent Autumn worker/client Create race: exactly one winner. Independent
  Update race after forcing an SST flush: exactly one winner. Bypassing the PS
  mismatch branch makes the Create test fail with two winners; restored green.
- Cross-chunk and zero-length ranges, a GetResult stream held across replacement
  and deletion, 1100-key pagination, aborted/unpolled multipart completion and
  offline vacuum preserving live data: pass.
- Rust LanceDB 0.40.0-beta.3 / Lance 13.0.0-beta.6: create 100, append 100, nearest
  vector id 42, delete 10, reopen using a separate SDK worker, two concurrent
  appends of 10 each, checkout latest = 210 rows. 16 objects in Autumn; cleanup
  reclaimed 21 chunks. No LanceDB source patch.
- Python LanceDB 0.39.0 over a real FUSE mount: create/append/search/delete,
  concurrent reader with 20 appends, final 210 rows: pass. Initially failed at
  linkat(...manifest#1, ...manifest)=EPERM. Implemented FUSE hard links; normal
  rename had already worked. Core regression checks EEXIST and source unlink
  preserving the winning inode.
- Touched library regressions: client 57, partition-server 253, RPC 69 passed
  (two pre-existing ignored tests across the latter two suites). Object-store
  contract binary: three feature tests plus three imported support tests pass.

## Matched object workload smoke baseline

Same host, loopback TCP, same /data08 filesystem, 8 requests in flight, 64
objects at each size. Reads immediately follow writes and are warm-cache.
Autumn: one partition, two real extent nodes, RF2, debug server build.
MinIO: RELEASE.2025-09-07T16-13-09Z, one server, one data directory, no replication.
Client: debug build without debug symbols; not optimized. Storage benchmarks
ran sequentially. This is acceptance evidence, not a production performance
comparison; release tuning and equivalent durability are not established.

| Operation | Autumn MiB/s | MinIO MiB/s | Autumn P99 ms | MinIO P99 ms |
|---|---:|---:|---:|---:|
| Put 64 KiB | 45.0 | 110.3 | 50.11 | 19.16 |
| Range read 64 KiB | 291.0 | 193.6 | 2.37 | 2.70 |
| Put 1 MiB | 320.5 | 774.0 | 30.46 | 20.65 |
| Range read 1 MiB | 1828.4 | 1416.6 | 6.68 | 7.73 |
| Put 4 MiB | 420.7 | 1157.5 | 102.66 | 34.16 |
| Range read 4 MiB | 2612.5 | 2053.7 | 17.37 | 41.08 |

Full listing of 1100 fragments, 100 samples:

| Backend | P50 ms | P99 ms |
|---|---:|---:|
| Autumn | 28.08 | 29.07 |
| MinIO | 48.16 | 54.54 |

Autumn's list uses 256-key RANGE pages plus get_many for metadata because RANGE
is keys-only. Its 1 MiB read exceeds the task's historical 355 MB/s reference,
but these environments are not controlled A/B equivalents. Writes lag this
single-copy MinIO configuration; neither the replication cost nor debug-build
cost has been isolated.

## Limits

- Object payload GC is offline: all readers/writers of the scope must stop before
  vacuum_quiescent. Replaced, deleted and aborted-upload chunks otherwise remain.
- No historical object version API or attributes. ETags are UUID publications,
  not content hashes. Default object_store rename is copy then delete.
- The bridge has 32 active and 32 queued jobs, bounded by count, not payload bytes.
- FUSE hard links inherit the filesystem's nontransactional multi-key metadata
  and per-mount mutation serialization. The one-mount demo is verified; crash-
  atomic/multi-mount namespace operations are not claimed.
- Superseded 2026-09-20: the demo now connects through an autumn:// provider
  registered on the session registry, not the deprecated per-table injection.
  The commit handler is chosen by lancedb for autumn:// only on the listing
  database's create and open paths; namespace-backed tables and clone_table
  still fall to lance's UnsafeCommitHandler.
- Wire version is 44: rebuild the cluster and all embedded SDK clients together.

Raw evidence remains in the isolated remote logs: contract.log, native-demo4.log,
fuse-demo3.log, fuse-rename.strace, bench-autumn.log and bench-s3.log. Reproduction
commands are in examples/lancedb/README.md.


## Final verification and cleanup

The final committed implementation at fcfc51f was synchronized with git bundle
and tested inside the isolated Linux checkout. All three final commands exited 0:

- native-final.log: creation, append, vector search, deletion, reopening and
  concurrent append passed; 210 rows, 16 objects, 21 payload chunks vacuumed.
  The empty-scope guard and table-scoped cleanup were exercised.
- fuse-final.log: explicit hard-link destination collision and source-unlink
  checks plus the complete LanceDB lifecycle/concurrent-reader demo passed.
- contract-final.log: all 6 tests passed in 4.37 s, including multipart lifecycle,
  CAS, SST-backed comparison, range reads, pagination and FUSE link invariants.

Temporary-root access was renewed using the same orthrus-cli demand step as
local goto H200-1. The task-specific FUSE mount was unmounted, and the verified
task-owned test_cluster (3634637), FUSE (3726318) and MinIO (3653969) processes
were stopped. final-cleanup.json records mounted=false and all services inactive.
The independent checkout, data and logs remain for inspection. Existing checkouts
and services were untouched. TaskStatus=completed; feature passes=true.
