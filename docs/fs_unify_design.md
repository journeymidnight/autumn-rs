# Design: one filesystem, several front-ends

`autumn-fuse` (kernel POSIX mount), `autumnfs` (mount-free CLI) and the PyO3
`autumn.Fs` binding are **one filesystem**, not three namespaces. They share one
on-disk inode layout, one Rust FS core, one manager-granted inode counter and
one per-inode lease, so a file created through any of them is visible and
byte-identical through the others and concurrent writers are fenced against each
other.

---

## 1. Goal & non-goals

**Goal.** One filesystem, several front-ends over one shared Rust FS core:

- `autumn-fuse` — kernel POSIX mount (`fuser::Filesystem`).
- `autumnfs` — CLI (`ls`/`stat`/`mkdir`/`cat`/`put`/`get`/`rm`/`touch`) that
  talks to the cluster directly, no mount needed. Sole writer of lane-striped
  large files.
- `autumn.Fs` — PyO3 binding; `autumn_vllm_loader` reads model weights through it.

**Non-goals.**
- Full POSIX semantics on the Python side (hardlinks, mode/uid/gid fidelity,
  O_APPEND races). The Python surface is deliberately lean: file/dir, size,
  mtime, read/write/rename/unlink.
- Reimplementing namespace/lease logic in Python. The core stays in Rust, bound
  via PyO3 — one implementation, no drift.
- Cross-host distributed POSIX locking beyond what the inode lease provides.

---

## 2. The filesystem, grounded in code

### 2.1 On-disk layout (`crates/fuse/src/{key,schema,geom}.rs` — un-feature-gated)

Keys below are RELATIVE; the client prepends the `fs/` namespace (one global
tree, no tenant/volume segment — `FsState` connects with `scoped("fs")`).

| prefix | key | value |
|---|---|---|
| `0x01` | `[0x01][ino u64BE]` | `InodeMeta` (rkyv): mode/uid/gid/size/nlink/times, `inline_data` (≤4 KiB), `symlink_target`, `stripe` |
| `0x02` | `[0x02][parent_ino u64BE][name]` | `DirentValue { child_inode, file_type }` |
| `0x03` | `[0x03][ino u64BE][logical_off u64BE]` | extent bytes, ≤ `MAX_EXTENT` = 8 MiB (non-striped file) |
| `0x03` | `[0x03][lane u8][ino u64BE][logical_off u64BE]` | extent bytes of a lane-striped file |
| `0x04` | `[0x04][field]` | superblock: `next_inode` (migration floor only), `schema_version`, `stripe_geom`, `rmtomb/[ino]` |

Files: inline ≤ `INLINE_THRESHOLD` = 4 KiB in `InodeMeta.inline_data`; larger →
variable extents keyed by logical offset (a range-scan of the extent prefix
yields them in order). `ROOT_INO = 1`, `INODE_ALLOC_BATCH = 1000`,
`SCHEMA_VERSION = 3` (stamped in `[0x04]schema_version`; mismatch fails the
mount loud).

A file is stamped `InodeMeta.stripe = Some(StripeLayout { lanes, unit_bytes })`
at create when it is striped; the stamp is immutable and the reader branches on
it to pick the key layout, so non-striped files stay correct with no migration.
`lane = (off / unit_bytes) % lanes` sits HIGH in the key so lane boundaries
`[0x03][lane]` are static (ino-independent) and the fs can be pre-split into
lane partitions at bootstrap. `DEFAULT_STRIPE_LANES = 24` when the fs declares
no `[0x04]stripe_geom`.

### 2.2 Namespace core ops (pure over `ClusterClient` + `key`/`schema`)

- `meta.rs`: `alloc_inode`, `get_inode`, `put_inode`, `new_file_meta`,
  `new_dir_meta`, `ensure_root`, `ensure_schema_version`
- `dir.rs`: `lookup`, `lookup_opt`, `resolve`, `readdir`, `mkdir`, `create`,
  `rmdir`, `unlink`, `rename`
- `extent.rs`: `write_region`, `extents_snapshot`, `truncate_extents`,
  `delete_all_extents`, `clean_beyond_eof`, `remove_unreachable_inode`,
  `sweep_unlink_tombstones`
- `write.rs`: `write`, `flush_inode`, `truncate` · `read.rs`: `prepare` /
  `execute` / `read` / `read_into` · `geom.rs`: `read_stripe_geom` /
  `write_stripe_geom`

These take `&mut FsState` and return plain data (`InodeMeta`, `DirentValue`,
`ReaddirEntry` with a `DT_*` byte, byte buffers) — **no `fuser` types**.
`attr.rs` is the single fuse-gated conversion point (`inode_to_attr`,
`dt_to_filetype`); `dispatch.rs` + `ops.rs` are the FUSE-protocol glue and stay
fuse-only.

### 2.3 Lease library (`crates/client/src/lease.rs`)

Framework-agnostic, `ClusterClient`-based:

- `acquire(cluster, client: &DaemonClientId, ino, mode) -> AcquireResult`
  (+ `acquire_force`, `acquire_with_preempt_wait`)
- `release`, `heartbeat`, `poll_invalidations`, `apply_invalidation`,
  `cache_is_stale`
- `DaemonClientId::new_fuse(host)`; modes `LEASE_MODE_READ` / `LEASE_MODE_WRITE`
- One writer per inode, fenced by `lease_epoch` (manager
  `MgrInodeLeaseInfo.version`); the manager pushes `WriterClosed` /
  `LeaseRevoked` for close-to-open coherence.

Full protocol: `docs/autumn_fs_lease_plan.md`.

---

## 3. Architecture

```
   ┌──────────────────────┐  ┌──────────────────┐  ┌────────────────────────┐
   │  autumn-fuse (bin)   │  │ autumnfs (CLI)   │  │ autumn.Fs (PyO3)       │
   │  fuser::Filesystem   │  │ path↔inode ops   │  │ path↔inode facade      │
   │  dispatch.rs + attr.rs│ │ striped writes   │  │ sync bridge to compio  │
   │  kernel page-cache inv│ │                  │  │ lease on write         │
   └──────────┬───────────┘  └────────┬─────────┘  └───────────┬────────────┘
              │  InodeMeta/FileAttr    │                        │  PyO3
              ▼                        ▼                        ▼
   ┌───────────────────────────────────────────────────────────────────────┐
   │   SHARED FS CORE  (`core` feature — no fuser)                          │
   │   namespace: alloc_inode (manager grant) · lookup/readdir/mkdir/…      │
   │   data: write_region · read · truncate · extents_snapshot · geom       │
   │   lease: autumn_client::lease + lease_tasks (heartbeat/poll/evict)     │
   │   returns plain InodeMeta/DirentValue (NO fuser types)                 │
   └───────────────────────────────┬───────────────────────────────────────┘
                                   │ ClusterClient (put/get/range, fenced put)
                                   ▼
                    partition layer (ordered KV) · manager (leases, inode alloc)
```

Two decisions embodied here:

1. **The core returns plain data.** `InodeMeta`, `DirentValue`, byte buffers.
   The `→ fuser::FileAttr` conversion lives up in `attr.rs`/`dispatch.rs`
   (fuse-only); the Python binding converts to its own dict/`info` shape.
2. **`FsState` is the shared piece.** It owns the `ClusterClient`, the
   inode-alloc cursor, the `DaemonClientId`, held leases, the invalidation map
   and `direct_read`. FUSE-runtime-only caches (the `InodeState` map, per-inode
   `WriteBuffer`, `kernel_invalidator`, `lookup_count`) live in the fuse binary;
   the Python side keeps its own, simpler per-open state.

### 3.1 Feature split

The core lives in `crates/fuse` behind the `core` feature (`meta`/`dir`/
`extent`/`read`/`write`/`state`/`geom`/`lease_tasks` + `key`/`schema`; extra
deps are only `libc` and `compio`). `fuse = ["core", "fuser", "clap", …]` is the
default and adds the kernel-mount machinery. `python/` depends on `autumn-fuse`
with `default-features = false, features = ["core"]`.

**Invariant:** core files never import `fuser` — `cargo tree --features core`
must show a fuser count of 0, and every new core→fuser conversion goes in
`attr.rs`.

---

## 4. PyO3 FS-core API (`python/src/fs.rs`)

`autumn.Fs` sits beside `Client` and `BatchClient`. One dedicated compio worker
thread owns the `!Send` `FsState`; each sync Python method ships a job to that
worker and blocks for the result.

```
Fs.connect(manager, *, host=None, principal=None, credential=None,
           direct_read=False) -> Fs        # DaemonClientId::new_fuse(host)
  # path resolution + metadata
  resolve(path) -> ino | None              # walk dirents from ROOT_INO
  getattr(ino) -> dict                     # size/type/mtime/mode
  readdir(ino) -> list[(name, ino, kind)]
  lookup(parent_ino, name) -> (ino, kind) | None
  # mutations
  mkdir(parent, name, mode) -> ino
  create(parent, name, mode) -> ino        # empty file
  unlink(parent, name); rmdir(parent, name); rename(...)
  # data
  read(ino, offset, size) -> bytes
  read_into(ino, offset, buf) -> n         # buffer-protocol dest, no bytes copy
  write(ino, offset, data) -> n            # buffered → extents
  flush(ino); truncate(ino, size); forget(ino)
  # leases (explicit, so the caller drives the fence — §5)
  acquire(ino, mode) -> lease_epoch; heartbeat(ino) -> bool; release(ino)
```

`principal` + `credential` are a both-or-neither pair enforced by `connect`.
The binding calls the *same* core functions the fuse binary calls, so there is
no Python-side reimplementation to drift.

---

## 5. Lease flow

- **Reads** take no lease. Coherence comes from fresh reads plus the
  invalidation poll: the binding only caches on write and `forget`s the inode on
  release, and the fuse mount compares `InodeState.cached_version` against the
  lease version at Open.
- **Writes** take `acquire(ino, WRITE)`. One writer per inode means a concurrent
  holder makes the second caller fail with a conflict. The write lease is held
  across the whole write and heartbeated on a timer (`lease_tasks.rs`, 5 s) so
  multi-GB uploads keep it alive, then released on close/flush.
- **Fencing.** Writes carry `WriteLease { inode_hint, lease_epoch }`; the PS
  keeps a fence floor per inode, so a deposed writer's late RPCs are rejected
  server-side. Client-side, the invalidation poll marks a revoked lease and
  `write`/`flush`/`truncate` fast-fail on it. Anonymous writes
  (`WriteLease::ANON`, `lease_epoch = 0`) bypass the fence by design.
- **Crash safety.** A process that dies mid-write stops heartbeating; the
  manager expires the lease after its TTL and another front-end can take over —
  the epoch fence is what makes that safe.

---

## 6. Inode allocation

Inode numbers come from the manager, not from a client-side counter:
`ClusterClient::alloc_inodes(count, floor, volume)` → `MSG_ALLOC_INODES`
(`0x53`) → `crates/manager/src/fs_alloc.rs`.

- The authoritative counter is the etcd key `autumn-rs/fs/next_inode` (strict
  big-endian u64; malformed → refuse loudly).
- Every grant is a read → `txn_fenced` value-CAS loop with the leader fence
  prepended, so concurrent allocators can never receive overlapping ranges and a
  deposed leader's grant loses the transaction.
- `floor` is a migration floor: the counter is raised to at least this value
  before granting and never rewinds. The fuse mount passes the legacy
  `[0x04]next_inode` value on its first batch; that KV key is advisory-only
  afterwards.
- Callers refill in `INODE_ALLOC_BATCH` = 1000 batches. `handle_alloc_inodes` is
  leader-gated; a follower refuses with NOT_LEADER.
- This is deliberately *not* `alloc_ids` (which numbers manager entities
  replayed from etcd prefixes); inode numbers are fs-layer data with their own
  key.

`AllocInodesReq.volume` is frozen into the wire but **dormant**: the fuse layer
passes an empty volume, so there is a single global counter. The lease/fence
plane keys on the bare ino, so per-volume inode numbers would collide across
volumes and produce cross-volume write-lease conflicts. Isolation between trees
comes from the namespace prefix, not the inode number.

---

## 7. Known limits

- **Preemption is victim-side only.** The mechanism exists end to end
  (`lease::acquire_force` / `acquire_with_preempt_wait`; manager `WillRevokeIn`
  grace → deferred-push force-revoke), and the victim side is wired: the poll
  marks the lease revoked, writes fast-fail, the PS epoch fence backstops. The
  *preemptor* side is not: the fuse mount and `autumn.Fs` use non-force
  `acquire`, so a WRITE conflict fails immediately rather than
  bounded-wait-then-preempt. The target workload is single-writer (dataset prep
  / model upload / checkpoint) and mutual `force` writers risk livelock.
- **The fuse mount cannot write striped files.** `write::write` /
  `write::truncate` return "not supported yet; use autumnfs" for an inode with
  `meta.stripe` set. The mount *reads* striped files fine; striped writes are
  `autumnfs`-only.
- **`rename` is not atomic.** It is a sequence of unfenced KV operations.
- **Path→inode walks are chatty.** Every `info`/`ls` is a dirent walk plus a
  `getattr` round trip, which is more RPCs than a single range scan over a
  path-keyed layout would be.
