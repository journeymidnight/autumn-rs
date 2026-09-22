# LanceDB over FUSE validation — 2026-09-19

LanceDB is integrated through Autumn's POSIX FUSE mount. The example uses the
unmodified community Python package and a `file://` URI; there is no native
LanceDB object-store adapter or patched wheel in this repository.

## Verified behavior

The validation ran on Linux against a real Autumn FUSE mount with Python
LanceDB 0.39.0. It passed:

- create-only hard links, including `EEXIST` on destination collision and
  preserving the destination after the source is unlinked;
- table creation and append;
- nearest-vector search returning the expected row;
- predicate deletion and row-count checks;
- a concurrent reader while one writer performs repeated appends;
- table cleanup.

The initial LanceDB run exposed missing hard-link support because the local
Lance backend publishes manifests with `linkat`. After implementing FUSE hard
links, the full workflow passed with 210 final rows.

## Boundary

The verified configuration uses one FUSE mount. Hard links inherit the
filesystem's nontransactional multi-key metadata and per-mount mutation
serialization; crash-atomic namespace updates and concurrent access through
multiple mounts are not claimed by this validation.

Build every Autumn service and the FUSE client from compatible revisions. Use a
dedicated test directory inside the mount, stop all LanceDB processes before
unmounting, and unmount only the task-owned mount point.

Reproduction commands are in [the example README](../examples/lancedb/README.md).
