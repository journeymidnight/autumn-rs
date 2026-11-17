# autumn-fs Architecture Guide

## Purpose

`fs/` is the file tree itself: the key/value layout, namespace operations, the data read
and write paths, segmented files, whole-file publish and publish sessions, all built on the
partition layer's KV. **It contains no kernel mount code** and does not depend on libfuse.

The frontends are thin shells over it, with one implementation and no drift:

| Frontend | Location |
|------|------|
| Kernel mount | `autumn-fuse` (the only crate that depends on fuser / libfuse; see its CLAUDE.md) |
| S3 gateway | `crates/server/src/bin/autumn_s3/` |
| `autumnfs` CLI | `crates/server/src/bin/` (mkdir / rm through `dir`, touch of an existing file through `meta::put_inode`; the parent must be a directory, judged from its dirent (`dir::resolve_typed`) for mkdir / touch / rm and from its inode for put; a new file (put / touch) is published create-only via `dir::compare_put_settled`, its keys deleted when the publish is refused and kept when the outcome stays unknown — the same cross-session windows as rename apply) |
| PyO3 `autumn.Fs` + fsspec facade | `python/src/fs.rs` |

This code used to be the `core` feature of `autumn-fuse`. It became its own crate because
users such as the S3 gateway and the Python binding have nothing to do with kernel mounts,
yet had to depend on a crate called fuse and turn off its default features.

## Module responsibilities

| File | Responsibility |
|------|------|
| `key.rs` | KV key encoding/decoding (including the striped key builder) |
| `schema.rs` | InodeMeta / DirentValue / StripeLayout / ReaddirEntry / WriteBuffer + constants |
| `geom.rs` | declared stripe geometry read/write (`[0x04]stripe_geom`) |
| `meta.rs` | inode metadata get/put, `alloc_inode` (numbers from the manager), `ensure_root`/`ensure_schema_version`, S_IF* mode constants |
| `dir.rs` | lookup/readdir/mkdir/rmdir/rename/create/unlink/symlink/readlink/resolve/resolve_follow — returns `(ino, InodeMeta)` / DT_* entries |
| `extent.rs` | variable-length extent addressing/write/RMW/truncate/delete/`clean_beyond_eof`/`remove_unreachable_inode` |
| `read.rs` / `write.rs` | chunked read assembly / write buffer + flush |
| `segment.rs` | segmented files: map splice/clip/read planning (pure functions), data object and map page read/write, `reclaim` |
| `publish.rs` | whole-file publish of a new inode (conditional CAS), delete, sessions and dead-session recovery, `fence_all` |
| `multipart.rs` | S3 multipart: upload state machine, part writes, metadata-only Complete, Abort, cleanup and dead-session recovery |
| `lease_tasks.rs` | per-session lease background tasks (heartbeat + invalidation poll + revoked eviction) |
| `state.rs` | `FsState` (ClusterClient, inode batch cursor, lease bookkeeping, `direct_read`, `reclaim_later` reclaim handoff) |

Invariant: this crate **does not depend on fuser** and knows no kernel reply types; the
core→fuser conversion lives only in `autumn-fuse`'s `attr.rs`. The PyO3 binding gives the
`!Send` `FsState` to one dedicated compio worker thread (Python sync methods ship a job and
block for the result).

The manager's system tests such as `system_publish`, `system_segmented` and `system_fuse_*`
depend only on this crate and need neither the `fuse-tests` feature nor libfuse; only the
three tests that drive the mount dispatch loop (`fuse_lease_1`, `fuse_lease_2`,
`system_fuse_release_best_effort`) need them.


## Range-local read planning

`read::prepare` calls `extent::read_extents`: striped files calculate only the
requested units from the inode's persisted geometry; the same global extent-count
validation remains in `schema::striped_extent_count`. Legacy cached maps are binary
searched and only their intersecting entries are copied. Cold legacy reads still
scan once and retain the map when an inode cache entry exists. No read semantics or
on-disk format changes. The `read_plan` benchmark measures cached planning across
1/64/1024 GiB logical files separately from network/disk I/O.

### Inode-based path mapping

The design uses inodes (not a flat path=key): rename is O(1) (only the dirent changes),
hard links work (several dirents point at one inode), and the root inode = 1 (`ROOT_INO` /
FUSE_ROOT_ID).

**Inode allocation = numbers handed out by the manager**: `ClusterClient::alloc_inodes` →
`MSG_ALLOC_INODES` (leader-fenced etcd CAS), preallocating `inode_alloc_batch`=1000 per
batch. A global counter keeps concurrent allocators (two mounts, or a mount + Python
`autumn.Fs`) from reusing numbers. The `[0x04]next_inode` KV is only a **migration floor**
(the first batch passes the old value to the manager) + a best-effort write-back per batch
(advisory-only).

## KV key encoding

All file system data lives in one KV namespace; the first key byte distinguishes the type.
Big Endian keeps natural ordering: dirents of one parent cluster together, and extents of one
file are contiguous and ordered by logical offset.

| Prefix | Use | Key format | Value |
|------|------|---------|-------|
| `0x01` | Inode metadata | `[0x01][ino: u64 BE]` | InodeMeta (rkyv) |
| `0x02` | Directory entry | `[0x02][parent: u64 BE][name]` | DirentValue (rkyv) |
| `0x03` | File data extent | `[0x03][ino: u64 BE][logical_off: u64 BE]` | raw bytes ≤ 8 MiB (`MAX_EXTENT`) |
| `0x03` | Striped extent | `[0x03][lane: u8][ino BE][logical_off BE]` | raw bytes ≤ `MAX_EXTENT` |
| `0x04` | FS superblock | `[0x04][field]` | varies (`next_inode` / `schema_version` / `stripe_geom` / `rmtomb/[ino]` / `sess/` / `pend/` / `rbase/[S][ino]`) |

**Namespace binding**: wire key = `fs/[type][fields]` (one global tree). `autumn-fuse` /
`autumnfs` / PyO3 `autumn.Fs` have no scope parameter; `FsState` uses `connect(mgr, "fs")` / `scoped("fs")`. Several mutually isolated trees use different
namespaces (`fsA`/`fsB`). The table above shows RELATIVE keys — **the client owns the whole `fs/` prefix**
(prepends it, strips it back off returned range keys, clamps to the namespace boundary). The 8
`kv_*` choke points in `state.rs`, the `key::*` builders and every `parse_*` hand bare RELATIVE keys to the client,
with zero wire concatenation. The two batch data paths (`ChunkSpec.key` in `read::prepare`,
the append keys in `extent::flush_appends`, which call `get_many_*`/`put_many` directly for performance
and bypass `kv_*`) also hand over bare `key::*` keys — the client prepends in one place, the same as the metadata path. Authorization =
`principal-create --grant fs/` + `--credential-file` (the principal name is in the file; with authz
on, the whole `fs/` is protected). See docs/key_namespace_split_design.md.

### Variable-length extents

File data is **variable-length extents addressed by logical byte offset** (key = `[0x03][ino][logical_off BE]`,
value ≤ 8 MiB = `MAX_EXTENT`): sequential writes merge into extents close to 8 MiB, and the tail/partial extent
is shorter ("variable length like Linux extents"). Compared with fixed 256 KiB chunks, a large file goes from
hundreds of thousands of small blocks to orders of magnitude fewer extents of ≥ 64 KiB each, and each whole-extent read takes the bulk path of `get_many_into`
(`MSG_GET`, the target size for RDMA zero-copy).

- **The durable truth = the extent KV keys themselves** (implicit-key design; InodeMeta does **not** store an extent list).
- **Runtime cache** `InodeState.extents: Option<Vec<(start, len)>>`: a cold start range-scans
  the `[0x03][ino]` prefix for start offsets and infers lengths from the next start / the file size; writes maintain it incrementally,
  truncate invalidates it (sets `None`).
- **Invariant: extents never overlap**. A read requests the exact sub-range `[start, start+len)` of each overlapping
  extent; the PS get clips to the real value length and the rest of dest is zero-filled → short extents / sparse holes are both correct.
- **Small files** ≤ `INLINE_THRESHOLD`=4KB: inline in `InodeMeta.inline_data` (no extent;
  saves one KV operation on both read and write; growing past the threshold migrates to extent storage).
- All addressing/read/write/truncate/delete logic is in `crate::extent`.

### Lane striping (large files striped across partitions)

A large file's extents are spread across `lanes` partitions, so single-file reads and writes run in parallel past the single partition/log_stream
ceiling.

- **Per-file stamp**: `InodeMeta.stripe: Option<StripeLayout>`. `Some` = striped
  (extents use `[0x03][lane][ino][off]`), `None` = single-partition legacy layout
  (`[0x03][ino][off]`). **Set at create, immutable afterwards**; the read side branches on it to pick the key layout,
  so old files stay correct without migration.
- **striped key** (18 B): `[0x03][lane][ino BE][off BE]`, `lane =
  stripe_lane(off, lanes, unit_bytes) = (off / unit_bytes) % lanes`. The lane byte is in the
  HIGH position (right after 0x03) and dominates partition routing; the lane boundary `[0x03][lane]` is STATIC (independent of ino),
  so fs can be presplit into lane partitions at bootstrap without any ino.
- **declared geometry** `[0x04]stripe_geom` → rkyv `StripeLayout { lanes: u8,
  unit_bytes: u32 }`. `geom::read_stripe_geom` reads it once per session and caches it: key present = fs
  declared itself; key missing = default `DEFAULT_STRIPE_LANES`=24 lanes, unit=`MAX_EXTENT`; **hard KV
  errors PROPAGATE** (not swallowed as lanes=1; otherwise one transient blip creates a permanently single-partition large file whose only symptom is
  "inexplicably poor throughput", the hardest to diagnose). `autumn-op namespace-create --name fs --parts N` (24 lanes) writes the declaration in
  the **same command** that cuts the lane boundaries (written only after every split point is cut; if the write fails the command fails), so declaration and
  placement never diverge, and an fs whose split did not finish holds no declaration.
- **24 lanes is overprovisioning**: any partition count that divides 24 (1,2,3,4,6,8,12,24) spreads every
  file evenly; the lane count is a permanent layout constant, not a function of cluster shape → files written by a 1-partition fs are already
  sorted by lane, and a later split at lane boundaries gains parallelism RETROACTIVELY, with no data rewrite.
- **`striped_extent_offsets(size, unit)`** enumerates the aligned offsets `(0, u, 2u, …)` for
  reader/rm/delete to compute lane keys — **`unit` always comes from the file's PERSISTED
  `StripeLayout.unit_bytes`, not the `MAX_EXTENT` constant**: `MAX_EXTENT` may be
  retuned on fully provisioned hardware; stepping by the current constant would, after it shrinks, enumerate mismatched offsets for old striped files → half the file reads as zeros
  with no error (sparse semantics). `StripeLayout::checked()` validates `lanes ≥ 1 && unit_bytes ≥ 1`
  (guarding the key builder against div-by-zero), and every path that reads `meta.stripe` goes through it.
- **The fuse mount refuses striped writes**: `write::write` / `write::truncate` return
  "not supported yet; use autumnfs" for an inode whose `meta.stripe` is non-empty. fuse **can read** striped files;
  **striped writes are done only by `autumnfs`** (a large file is stamped striped at create per the declaration).

## KV data model

### Complete example

```
/                          (ino=1, dir) 
└── docs/                  (ino=2, dir) 
    └── readme.txt         (ino=3, file, 600KB)
```

```
  [0x01][ino=1]  →  InodeMeta{ mode=S_IFDIR|0755, nlink=3, size=0, ... }
  [0x01][ino=3]  →  InodeMeta{ mode=S_IFREG|0644, nlink=1, size=614400, inline_data=None, stripe=None }
  [0x02][parent=1]["docs"]        →  { child_inode=2, file_type=DT_DIR }
  [0x02][parent=2]["readme.txt"]  →  { child_inode=3, file_type=DT_REG }   (name in the key, not the value)
  [0x03][ino=3][off=0]            →  [≤ 8 MiB raw bytes]
  [0x04]["next_inode"] / ["schema_version"]=3 / ["stripe_geom"]=StripeLayout(rkyv)
```

`DirentValue.child_inode` points at an `InodeMeta`; the ino in an extent key is that InodeMeta's
inode number; `InodeMeta.size` bounds the visible extent range.

### InodeMeta — "what this thing is"

Key `[0x01][ino BE]`, describing the file/directory's **own attributes** (corresponding to Linux `struct stat`):
`mode` (type + permissions), `uid`/`gid`, `size` (0 for directories), `nlink`, `atime`/`mtime`/
`ctime`, `inline_data` (small-file data ≤4KB), `symlink_target`, `stripe` (stripe geometry or
None). **It contains neither the file name nor the parent directory** — an inode does not know its name or location, which is what makes hard links work.

### DirentValue — "who is called what under which directory"

Key `[0x02][parent_ino BE][name]`, two fields: `child_inode`, `file_type`
(DT_REG=8 / DT_DIR=4 / DT_LNK=10). The file name is in the key, not the value. `file_type` duplicates
`InodeMeta.mode` to **trade space for time**: `readdir` returns each entry's type directly without looking up
InodeMeta once per entry (as in ext4's `ext4_dir_entry_2.file_type`).

### Why InodeMeta and DirentValue are separate

| Operation | DirentValue only | InodeMeta only | Both |
|------|:---:|:---:|:---:|
| `rename` | ✓ | | |
| `chmod`/`chown` | | ✓ | |
| `write` | | ✓ (size/mtime) | |
| `link` | ✓ (new dirent) | ✓ (nlink++) | ✓ |
| `mkdir`/`unlink` | ✓ | ✓ | ✓ |

Embedding one in the other would make hard links impossible (several names sharing attributes), make rename heavy, and make chmod find every dirent.

### KV access pattern per operation

| FUSE operation | KV operations |
|-----------|---------|
| `lookup(parent, name)` | 1× Get dirent + 1× Get inode |
| `readdir(ino)` | paginated Range(prefix=[0x02][ino BE]) + one batched get per page |
| `getattr(ino)` | 1× Get inode |
| `mkdir`/`create(parent, name)` | 1× Put inode + 1× CAS dirent (create-only) + 1× Get parent + 1× CAS parent (create: at most once a second per directory); the parent's Get moves before the publish the first time this session meets that directory (`check_parent`) |
| `link`, `unlink` (file count) | 1× Get + 1× CAS inode (`change_links`); whole-inode writes (flush, truncate, setattr) also Get + CAS (one more round trip per dirty flush than the old put; an unknown outcome is settled by re-reading) |
| `unlink(parent, name)` | 1× Get dirent + 1× CAS delete dirent + tombstone + N× Delete extent + 1× Delete inode + 1× Get + 1× CAS parent |
| `rename(old, new)` | 3× Get dirent (+1 Range per check if the new name is a dir) + 1× CAS delete old + 1× CAS new + Get + CAS of each parent inode |
| `read(ino, off, size)` | 1× Get per overlapping extent (sub-range, batched and concurrent) |
| `write(ino, off, data)` | after buffering: aligned 1× Put / unaligned 1× Get + 1× Put (RMW) |
| `truncate(ino, 0)` | meta Put (commit) + N× Delete extent |

ino → inode data is an **O(log N) KV Get** (the ino is encoded in the key, an LSM-tree lookup, not ext4's
O(1) array index); ino → data is linked implicitly through extent keys, and the KV layer manages physical placement transparently. The performance
gap is mostly **network RTT** (one RPC per Get); the FUSE kernel cache (entry_timeout=30s) absorbs
most repeated lookups.

## Constants

| Constant | Value | Notes |
|------|-----|------|
| `MAX_EXTENT` | 8 MiB | extent value cap; the write buffer flushes at this granularity; whole-extent reads ≥64 KiB take the bulk path |
| `INLINE_THRESHOLD` | 4 KiB | small-file inline threshold (matches VALUE_THROTTLE) |
| `DEFAULT_STRIPE_LANES` | 24 | default lane count when the fs declares no geometry |
| `ROOT_INO` | 1 | root inode (FUSE_ROOT_ID) |
| `SCHEMA_VERSION` | 4 | see fail-loud below |
| `DT_REG`/`DT_DIR`/`DT_LNK` | 8/4/10 | dirent types |

Parameters (`FsParams` in `crates/params/src/fs.rs`, process-wide `autumn_fs::params()`, parsed as flags by the
binary that embeds the fs layer):

| flag | Default | Notes |
|------|-----|------|
| `--write-buf-extents` | 8 | per-inode write buffer capacity (in extents); `write_buf_cap()` = it × `MAX_EXTENT` = 64 MiB. When >1, `write_region` splits into several extents and `put_many` groups by wire key and fans out concurrently (client `--batch-put-concurrency`) |
| `--append-inflight-depth` | 2 | number of append flush batches in flight per inode; 1 batch = 8 extents concurrently. Measured ~6% faster than 1; see the write pipelining section |
| `--segment-stream-inflight` | 8 | unit puts in flight for a streamed data object (S3 PUT / UploadPart); see "Data objects are a continuous pipeline" |
| `--inode-alloc-batch` | 1000 | inodes taken from the manager per batch |
| `--segment-page-cache-cap` | 256 | segment map page cache cap |
| `--lease-heartbeat-secs` | 5 | write lease heartbeat period (manager lease TTL defaults to 30 s) |
| `--multipart-completed-retention-secs` | 3600 | how long the answer for a completed multipart is retained |

## Core data structures

```rust
struct InodeMeta {          // rkyv, key [0x01][ino BE]
    mode: u32, uid: u32, gid: u32, size: u64, nlink: u32,
    atime_secs: i64, atime_nsecs: u32,
    mtime_secs: i64, mtime_nsecs: u32,
    ctime_secs: i64, ctime_nsecs: u32,
    inline_data: Option<Vec<u8>>,     // ≤4KB small file
    symlink_target: Option<Vec<u8>>,
    stripe: Option<StripeLayout>,     // Some = striped, None = single-partition legacy
}

struct DirentValue { child_inode: u64, file_type: u8 }  // key [0x02][parent BE][name]

struct StripeLayout { lanes: u8, unit_bytes: u32 }       // [0x04]stripe_geom + per-inode

// runtime state (compio thread-local, not persisted)
struct InodeState {
    meta: InodeMeta,
    write_buf: Option<WriteBuffer>,        // buf capacity write_buf_cap()
    pending_flushes: VecDeque<PendingFlush>, // in-flight append flushes (≤ append_inflight_depth)
    flush_error: Option<String>,           // sticky writeback failure, consumed only by a reporting flush_inode
    dirty: bool,
    open_count: u32,
    extents: Option<Vec<(u64, u32)>>,      // runtime extent map (truncate sets None)
    cached_version: u64,                   // lease version when meta/extents were last refreshed from KV
}
```

`cached_version`: at Open it is compared with the version AcquireLease returns; on mismatch the cached InodeState is dropped and rebuilt from
`get_inode` (+ the extents are rescanned next time). This keeps close-to-open consistency (a second mount's
Open of an inode the first mount wrote and closed does not read a stale `meta`).

## Operation paths

### Read
1. A dirty write buffer that overlaps the read range → flush first (read-after-write consistency). This flush passes
   **`FlushReport::BestEffort`**: its `?` does hand EIO to the caller, but **a read is not where a writeback
   error retires**. Linux errseq retires a writeback error only once, at fsync/close/msync, for exactly this reason — an application reads back
   what it just wrote, gets EIO, retries the read successfully, then closes; if this barrier ate the sticky record, the fsync of that close
   would find nothing to do and publish the size over the hole. The read fails either way; the only difference is whether the record survives.
2. **When the offset ≥ the cached size, confirm EOF with the KV before reporting it** (the only path in `read::prepare` that can return "less
   than requested", so a size that is too small does not make the answer stale, it **truncates** the file: a sequential reader takes the empty reply as
   end of file and silently stops). Adopting the confirmed value is **one-way: only "actually larger" is accepted**.
   - The criterion is pinned to **the size this read will use** (`fresh.size > file_size`), not to "whether the cache was adopted":
     adoption can only happen when the inode is already in `state.inodes`, and the S3 gateway and PyO3 read paths fill that table only
     on **write** ⇒ judging by adoption would make this confirmation a pure wasted round trip for every read-only frontend, and would also drop the
     growth they could otherwise pick up.
   - Why the "smaller" direction is refused: it may well be stale — another mount truncating the file leaves the same shape.
     The asymmetry is in the **consequence**, not the evidence. Smaller is also the normal state of a file this mount is writing (extents landed early, size not yet
     published); the two cannot be told apart here, so the only choice is to lean toward the side that destroys nothing: refusing smaller only makes `getattr` stale for a while
     (until a lease version reload or FORGET), and **cannot change the answer of any read** (entering this branch already means
     `offset >= size`, and the clamp returns empty anyway); adopting smaller would make the next write see `cur_size < offset` →
     `clean_beyond_eof` → **delete every extent it has already landed**.
3. Small file `inline_data` → return it directly.
4. Load the extent map (runtime cache, range-scan on cold start); a striped file first validates its geometry with `checked()`,
   then a `ChunkSpec` is built for each overlapping extent (striped → lane key, otherwise `[0x03][ino][off]`),
   sub-range = the exact overlap, holes between extents are zero-filled (sparse semantics).
5. One batched read fetches every extent slice, multiple extents concurrently (compio spawn; the spawned `execute`
   does not hold `&FsState`).

**`--direct-read` (default ON)**: `read::execute` uses `get_many_direct` instead of
`get_many_into`; whole-extent reads ≥ 64 KiB **bypass the PS and read the EN directly** (PS NIC egress leaves the data
path; higher cross-host throughput for large files / model serving); < 64 KiB still goes through the PS proxy (gated per item by size).
Safety: each failed direct read **falls back to the PS proxy per item** (authoritative); on the first fallback the client logs one WARN
(EN unreachable). If the EN data port is on a PS-only subnet, `--direct-read false` saves one
redirect RTT per extent. Wiring: `FsState.direct_read` → `ReadPlan.direct_read` → `execute` picks the primitive.

### Write (buffered)
1. Lazily allocate the `WriteBuffer` (capacity `write_buf_cap()`=64 MiB).
2. Gap detection (non-contiguous write offset) → flush the current buffer.
3. Copy into the buffer; a full buffer → flush via `extent::write_region` (split into non-overlapping extents capped at `MAX_EXTENT`,
   pipelined through `put_many`).
4. Mark dirty.

**The buffer is MOVED to the flush, not copied to it.** `write_region` takes `&mut FsState`, and the buffer lives
inside it, so the data cannot be borrowed across that call — the original answer was `wb.buf[..wb.len].to_vec()`, which allocated and copied the whole 64 MiB
on every flush, measured at 12% of a 4 GiB write. Now it is moved out with `mem::take`, and after use
`reclaim_buffer` returns the Vec **as is** (not cleared — `mem::take` leaves a len-0 Vec,
and a cleared buffer would make every fill run `resize(.., 0)`, zeroing bytes the next line is about to overwrite,
i.e. one 64 MiB memset per flush). `wb.len` is the single source of truth for the length; all three consumers slice by it,
so stale bytes after `wb.len` are never read. Measured 197 → 234 MiB/s.

**Raising `write_buf_extents` from 8 to 16/32 measured slower (248→234/229); do not go there again** — the write path's bottleneck is not
the depth of one flush.

**`max_write` is not a knob either, but for a different reason**: the kernel clamps `max_pages` to `FUSE_MAX_MAX_PAGES`
(256), so 1 MiB is already the per-request cap; with 4/8 MiB the request count stays 4096 (4 GiB).
⚠️ I once recorded "1→8 MiB is slower (248→219)" — **that was wrong**: `fuser` was still `abi-7-12` then,
`FUSE_MAX_PAGES` was compiled out, both sides were sending 128 KiB, and what was measured was pure noise.

**The phase timing stays in the code** (`fuse write breakdown`, one line every 16 flushes): the two hypotheses above and
"to_vec doesn't matter" were all refuted with it.

### Write pipelining (`pipelined_writes`, only the mount turns it on)

A **pure append** flush is split in two: `extent::plan_append_only` plans under `&mut FsState` (it needs to read
the extent map), and `extent::execute_append` needs only `Rc<ClusterClient>` and is spawned. So while the previous
64 MiB batch is on the wire, the dispatcher goes back to receiving the next batch from the kernel. The read path has always had this shape
(`read::prepare` + spawn `read::execute`), which is exactly why reads could fan out and writes could not.
Measured **249 → 338 MiB/s (+36%)**, 98% of the CLI's 345.

**Multiple slots (`append_inflight_depth`=2)**: before planning the next batch it no longer unconditionally drains the previous one; it waits for the oldest only when the queue
is full (`write::make_room`). `InodeState.pending_flushes` is therefore a queue, not a
single slot. The remaining headroom was small to begin with: a single slot already keeps **a batch of 8 extents in flight concurrently**, exactly equal to `autumnfs`'s
non-striped `depth`=8; what multiple slots remove is the bubble between batches, **not** some 137 MiB/s gap
(the earlier recorded 208 predates the pipelining in this section and is a stale number; do not cite it again).
**Measured**: on a pinned (one core per EN on the same NUMA node; PS, fuse and the writer each on their own set of cores)
3-EN RF3 cluster, 4 GiB `dd bs=8M conv=fsync` into the mount, depth 2 and 1 alternated for 6 pairs,
3 pairs in each order: **depth 2 was faster in all 6 pairs**, six-pair mean 284.7 vs 266.3 MiB/s (+6.9%), on par with
`autumnfs put` of the same file in the same round (283.5). This pinned setup is slower than the 338/345 one above; a single slot reaches only
~94% of the CLI on it, and the missing ~6% is exactly the inter-batch bubble. The gain is small but distinguishable, so it stays at 2.

Multi-slot safety relies on two things, both verified: ① `extent::upsert` is an ordered insert-or-replace by start, and consecutive append
batches cover disjoint ranges, so **landing order does not matter** (pinned by `disjoint_batches_apply_the_same_in_any_order`;
ablation goes red); ② errors do not return early — the postcondition is "queue empty", and an early return would leave a slot behind while the caller believes it is quiescent.
**Do not "simplify" this into clearing the queue on error**: in compio 0.19 `JoinHandle::drop` still cancels the task;
dropping a `PendingFlush` **cancels** that flush (compio's "drop does not cancel"
is the contract of `spawn_blocking`, not `spawn` — I misquoted that line at the start of this round).

No second buffer is needed: the values in `plan` are already owned data copied out with `Bytes::copy_from_slice`,
so the buffer is free as soon as planning ends.

**Only pure appends take this path.** RMW / holes / leftovers past EOF all fall back to the inline `write_region` —
they need to read the extent map, and the extents of in-flight flushes are not in the map yet. The criteria of `plan_append_only`
(returns None if any of `offset > file_size`, `s >= file_size`, `s + l > offset` holds) are that boundary.

**The drain sits at one point in the dispatcher** (every non-Write request drains first), rather than auditing call site by call site —
such a list is complete only until the next handler is added. **This is also why `pipelined_writes` is off by default and only
the mount turns it on**: the PyO3 `autumn.Fs` worker calls the core ops directly and never passes through this dispatcher;
with pipelining on, its `read` would read zeros over the range of an in-flight flush.

**A failed flush must stick to the inode** (`InodeState.flush_error`); the return value alone is not enough: the dispatcher's
drain runs **before** the fsync handler and only logs, which would consume the only copy of the error; `flush_inode` would then see
nothing pending and persist a size covering the lost range — **fsync reports success while the file holds a hole of zeros**.
Now `flush_inode` first takes this sticky error and returns it, before it gets to persisting the size.

**But the right to "take" depends on the caller** (`flush_inode(state, ino, report)`): only callers passing `FlushReport::ToApplication`
take; all others pass `BestEffort` and only peek. **The criterion is not "will this caller hand the error to anyone" but "is this
a retirement point for writeback errors in the Linux sense"** — only three places qualify: FUSE_FLUSH (the kernel sends it on every `close()`), FSYNC,
and the PyO3 binding's explicit flush. The other six all pass `BestEffort`, in two groups:
- **Tell nobody**: periodic_sync and Destroy only log; the error RELEASE returns is dropped by the kernel.
- **Tell someone, but are not retirement points**: the read-after-write barrier, the write path's own gap flush, truncate. Their `?`
  does hand EIO to the caller, but Linux errseq retires only at fsync/close/msync — an application gets EIO on a read/write, retries
  successfully, then closes, and that close must still fail; consuming it here means close finds no record and reports success over a hole.

RELEASE is easy to misread in this list: it does
return an error, but fuser's contract says plainly "the error value is not returned to the close()/munmap() that triggered the release" — the kernel
drops it, so it is equivalent to the two that only log. `propagate_flush_err` (= `!revoked`) in `dispatch.rs`
only decides **whether to reply EIO**; it **must not** be used to decide whether the sticky record may be consumed (that is exactly the mistake at the start of this round).
Do not fall back on the assumption "FLUSH always comes before RELEASE anyway" either — the same fuser documentation says
"filesystems shouldn't assume that flush will always be called ... or that it will be
called at all", which is exactly the hole this record exists to plug.
⚠️ The reachable shape of this is **the extent key and the inode key landing in different partitions** (`[0x03][ino][off]` and
`[0x01][ino]` sort far apart and are necessarily separated after a split) with the extent side unavailable; lease revocation is **not** it —
`put_inode` carries the fence too, so both sides fail together. The fsync guard in `scripts/fuse_chaos.sh` kills the whole cluster and
**cannot isolate this one** (the meta put fails too); isolating it for real requires freezing a single partition.

### Flush
`extent::write_region`: aligned ranges are Put directly; an unaligned range inside an existing extent → RMW (read the old value,
overwrite the sub-range, write back). Then `InodeMeta.size` is updated.

### Directory operations
- **lookup**: Get dirent → Get inode.
- **readdir**: a Range scan over the dirent prefix, paged by key until done; a single page of 4096 entries is not
  a directory size cap. Each page continues from the successor of the previous page's last key (`key + 0x00`); offset is the position in
  name order. `readdir_bounded` limits how many entries one call returns: the kernel takes only one reply buffer (about a hundred entries) at a time
  and resumes from the last offset; if every call read the rest of the directory, a large directory would cost a quadratic number of gets. Names before the offset
  are scanned by key only, without fetching values; the returned entries' dirents are fetched with one `get_many` in `list_children`.
- **mkdir / create**: alloc inode + Put meta + publish the dirent only if the name does not exist
  (`compare_put(None)`, no prior lookup: the kernel already did a lookup before creating) + the parent directory inode. When two sessions
  create the same name only one wins; the other gets EEXIST and deletes the inode it wrote.
- **Parent directory inode updates** (`update_dir`: nlink, times): CAS until it lands. Losing once means someone else's
  change landed; after that it waits a random `0..window`, the window starting at 100 µs and doubling with each loss, capped at 1 ms. The original
  did not wait and gave up after 16 straight losses (logging only a WARN): 4 sessions doing 300 mkdirs each in one directory lost up to 9
  links; 8 sessions lost ~250 per round. release 4×1000 mkdir, 2 alternating runs each: the original code 3.5K/s, p99
  4 ms, max 48–52 ms, 35–83 lost per round; 1 ms cap 3.5K/s, p99 6 ms, max 39–58 ms,
  none lost; 10 ms cap p99 10–13 ms, max 65–125 ms (sessions that lost a lot waited too long). Test
  `system_fs_namespace::many_sessions_mkdir_in_one_parent_all_count` (red without backoff and with give-up after 16).
- **The parent must be a directory** (`check_parent`, for mkdir / create / link / rename's new parent):
  a file as parent returns ENOTDIR and nothing is written. On a mount the kernel already guarantees this; callers that pass inode numbers directly
  (PyO3 `autumn.Fs`) used to be able to create dirents under a file, and mkdir also raised the file's nlink to 2, never reclaimed.
  No extra read is spent: the first time this session meets a directory it reads its inode and checks it, and those bytes are the baseline for the following
  parent CAS; afterwards it is remembered in `FsState::known_dirs` (the type never changes after creation and inode numbers are not reused, so it cannot
  go stale; cleared past 65536 entries) and takes the original path directly. With pre-reading but no cache, the baseline predates the publish and
  concurrent CAS in the same directory fails more easily: 4 sessions mkdir in one directory −5%; with the cache, create 11.1K/s and mkdir 4.2K/s,
  on par with before the change (release, ABAB, 6 runs each). Test `system_fs_namespace::a_file_is_refused_as_a_parent`
  (with the check removed mkdir succeeds and it goes red).
- **Types along a path come from dirents**: `DirentValue.file_type` is set everywhere a dirent is written,
  rename copies it unchanged, and types are immutable, so it agrees with the inode. `dir::resolve_typed` returns the type of the last
  component; S3's `bucket_dir`, `publish::ensure_dirs` and the autumnfs CLI use it to judge directories
  without reading the inode: ensure_dirs on a 4-level existing path 13.1K → 27.2K/s.
- **Parent directory inode** nlink and mtime are CAS'd against the bytes read (`cas_dir`): those that change nlink (mkdir,
  rmdir, rename) re-read and retry on conflict, up to 16 times without sleeping (`update_dir`); those that change only mtime (create,
  unlink, link) try once and accept a loss — the winning change started later; if the directory mtime is already within the current
  whole second it is not written (a deliberate trade-off: directory times have 1-second precision, like file systems with 1-second timestamps: a change that
  falls in a new second always advances mtime/ctime; a stored mtime in another second — including the future — is written, so between clients with
  unsynchronized clocks it degrades to writing every time, slower but not wrong). When the parent was written every time, the
  conditional writes on that one key serialize on the PS (each must wait for the previous to commit before comparing): 4 sessions create in one directory at 6.2K/s vs 6.9K before
  the change (alternating in the same round); after skipping within the second 9.3K/8.4K vs 6.5K/6.3K before. Both happen after the name change (the commit point);
  failure only logs a WARN and returns no error: an error would make an operation that already happened look failed (FUSE handles one
  request at a time, and backoff would also stall the whole mount); the cost is a skewed nlink count (visible to stat), never something becoming unreachable.
  It used to be read-modify-write: two sessions doing mkdir in the same directory, or an unlink next to a mkdir, lost nlink.
- **An inode's link count is changed only by `meta::change_links`** (CAS on the stored bytes, also advancing ctime):
  link +1, unlink / rename over a file / link rollback −1; the one that brings the count to 0 does the reclaim; adding a link is refused (ENOENT) when the count
  is already 0 or the inode no longer exists. retire's last name is also CAS'd to 0 first before the
  tombstone is written. Whole-inode writes (`meta::put_inode`: write flush, truncate, setattr) keep the stored
  nlink and CAS against the bytes read; if the inode no longer exists they return ENOENT and do not write it back; new inodes use
  `put_new_inode`. An unknown conditional-write outcome (`OutcomeUnknown`, e.g. a timeout during a PS leader change) is not an error; it is settled by re-reading:
  reading back its own bytes counts as success (`change_links` writes a new ctime every time, so the value is unique); reading back the bytes it
  expected means it did not land, so try again; `change_links` reading any other value returns an error (except when the count is already 0 and this is −1:
  there are no names and never will be, so it reclaims) — that write may have landed and then been overwritten by
  another write, and decrementing again could reach 0 early and reclaim a file that still has names; the count may only err high, never low
  (`system_fs_links_lost_reply`, failpoint `CHANGE_LINKS_TEST_LOST_REPLY`). `put_inode`
  cannot take effect twice (nlink is the current value each time); reading any other value retries; it also keeps a newer ctime. The old
  unconditional put was resent by the client; with a conditional write and no re-read, flush / unlink would fail on a single blip.
  `change_links` does not carry the write lease: the link count is namespace state, and a write lease that has gone stale on the mount must not leave an unlink that already removed
  the name stuck on the count (a leak). All three places used to be read-modify-write: one session unlinking the only name while another session
  linked it at the same time — link, holding the old count, wrote the inode back or attached a new name to it while the data was reclaimed (in 29 of 40 rounds the new name
  dangled or could not be read); the writer's flush wrote the cached meta and reset a link another session had just added to the old value, and deleting the
  original name then reclaimed the new name's data; a directory chmod also wrote back the nlink from before another session's mkdir
  (`crates/manager/tests/system_fs_links.rs`; removing the corresponding mechanism turns each red).
- **unlink / rmdir** delete only the dirent they read (`remove_name`, a conditional delete): if someone else deleted it,
  ENOENT; if it was renamed or replaced, re-read and start over; it never decrements someone else's inode's nlink, and rmdir never deletes someone else's
  inode.
- **rename**: check before touching keys (`rename_onto`): if the target is a file the source must be a file; if the target is a directory the
  source must be a directory and the target empty (ENOTEMPTY / EISDIR / ENOTDIR); the same inode is a no-op. It used to check nothing when the target
  was a directory: `/srcdir` over a non-empty `/dstdir` returned Ok and the original directory subtree became unreachable from the root.
  First the old name is conditionally deleted (a crash only loses a name; it never produces two names with one nlink), then the new name is published conditionally against the target value
  read during the check; if someone else changed the new name, re-read, re-check and publish again; if the check fails or the retries run out, the old name is put back. Publishing the new name
  and putting the old name back are settled by re-reading when the outcome is unknown (`compare_put_settled`): reading back its own value counts as landed, reading back the expected
  old value means publish again (of two writes expecting the same bytes at most one lands), and reading any other value means it did not land and never can.
  After one unknown outcome, a later "compare failed" must also be settled by re-reading: it may have been beaten by a late-landing
  copy of the earlier write; treating it as not landed would put the old name back, giving two names for one nlink (deleting either reclaims, the other name dangles).
  It used to return an error on reading any other value, leaving the source inode without a name, and when the late publish landed the replaced target's nlink was not decremented
  (`system_fs_links_lost_reply::a_rename_publish_of_unknown_outcome_is_settled` /
  `a_late_landing_publish_is_not_taken_for_a_refusal`, failpoint `SETTLE_TEST_FAULT`). An unknown outcome when deleting the old name still returns an error (reading "absent" cannot tell whether it was
  deleted by us or unlinked by someone else); the source inode may be left without a name (a leak, no data loss), and the error says so. The error
  message does not carry the user's name: FUSE maps errno by substring. A replaced empty directory has its inode deleted, and the parent directory's
  nlink is adjusted accordingly. Still not atomic (v1 limitation; for rename-over see UNLINK-1).
  Known limitation (accepted): when rmdir or rename overwrites an empty directory, a name another session creates in that directory between the "is empty" check and
  the delete loses its entry — create returns success but the file is invisible
  (a leak; the data stays in the KV). It occurs only with cross-session concurrent rmdir and name creation on the same directory; closing it requires
  directory-level coordination (mark the directory "being deleted" before checking emptiness, and have the name-creation path re-check the parent after publishing). Reading back
  exactly its own value may also be caused by a third session linking the same inode to the same name; rename then treats it as landed
  (needs three or more concurrent parties; accepted). Likewise, if another session changes this name after the copy lands and before the re-read, the re-read misleads:
  unlinked back to the expected value, it publishes again (the name may point at a reclaimed inode); replaced by another value, it is treated as not landed
  and the old name is put back (the source has one extra uncounted name). Closing this requires versioned dirents or a per-attempt nonce; it used to
  return an error and leak here.
  Test: `crates/manager/tests/system_fs_namespace.rs`.

## Per-session lease + cross-frontend fencing

The per-session lease background tasks (5s heartbeat renewing every held lease + a persistent invalidation
long-poll + `LeaseRevoked` eviction) are in `lease_tasks.rs` (core); the mount passes a real kernel
invalidator, the binding passes None (headless, no kernel page cache eviction).
`FsState.page_cache_generation` is used only by the mount: the mount keeps the kernel page cache across opens; an Open without a lease
uses `meta::fetch_inode` (reads the KV directly, does not touch `state.inodes`) to get `generation` and compares it with this table to decide whether to keep it
(see "Page cache" in `crates/fuse/CLAUDE.md`).
`FsState.meta_invalidated`: when the poll loop receives an invalidation event for an ino (or the poll fails / overflows, in which case every
held ino is marked) it marks that ino, and `meta::get_inode` re-reads from the KV once on seeing the mark — cached meta no longer outlives
a writer closing elsewhere. A heartbeat getting `NotHeld` for an ino is handled the same way: the manager has forgotten the lease, and later writer closes
elsewhere will no longer be pushed to this session, so it forgets the invalidation version, marks, calls the kernel invalidator, and deletes the READ slot;
the WRITE slot stays and is set `revoked` (the same shape as a manager-initiated revoke): later writes get EIO, and flush still carries the original epoch and is
refused by the fence. If it were deleted, `write_lease_for` would fall back to ANON and dirty data would overwrite the current lease-holding writer without a fence
(the two cases in `crates/manager/tests/fuse_lease_loss.rs`). This handling is in `lease_tasks::lease_not_held`,
shared by the background heartbeat and the binding's manual `heartbeat()`. No re-read while this session holds the write lease or the inode is dirty (the cached size may be ahead of the KV; accepting a smaller one
is the data deletion `get_inode_uncached` describes). Both the mount and the binding go through here.

Poll failure or overflow (`lease_tasks::drop_after_lost_poll`): invalidation events may have been missed, so every cache is invalidated and
READ slots are deleted and released; WRITE slots stay and are not released — a poll failure says nothing about whether the write lease was lost; the heartbeat keeps renewing,
and if it really was lost, `NotHeld` → revoked. They used to be deleted and released together, which handed the write lease to the next mount that asked, and every later write
on this mount's open write fd got EIO (`crates/manager/tests/fuse_lease_drain.rs`, toxiproxy cuts the manager connection).

A WRITE slot's epoch comes only from its grant; the heartbeat does not change it (it refreshes only non-write slots' versions): a writer's version does not change while it holds the lease, but
a session that read before writing is also a reader at the manager; after a forced preemption the heartbeat renews it as a reader and brings back the new writer's version; copying that into
the write slot would pass the PS fence and overwrite bytes the new writer has acknowledged (`fuse_lease_deposed_writer.rs`: during a disconnect the inbox overflowed
and dropped `LeaseRevoked`). `LeaseRevoked` can be dropped when the inbox overflows (>1024 entries), or lost when the connection drops after the manager has taken it
but before the reply arrives. Once missed, the old writer keeps writing with the original epoch; if it read first, the heartbeat keeps renewing it as a reader and it never gets
`NotHeld`; the only thing stopping it is the PS inode fence: per partition, raised when the new writer first writes that partition, so the old writer's writes cannot get into
partitions the new writer has already written and cannot overwrite its acknowledged bytes.

`InodeState.write_epoch`: the WRITE lease epoch under which the dirty state (`write_buf`, `dirty`) was produced, stamped on write
(the write path in `write.rs`, the splice in `segment.rs`), cleared after a successful flush. When `write_lease_for` has no WRITE
entry it falls back to this: dirty data whose lease was lost is written with the original epoch and refused by the fence, rather than falling back to ANON and overwriting the current lease-holding
writer — there is more than one path that deletes the entry (Release drops a revoked entry, Open deletes before re-acquiring, poll-failure drain),
so deciding the flush's identity by whether the entry exists breaks whenever one of them runs. `flush_inode` treats a fence refusal as final: it drops the unpublished state and records a
sticky `flush_error` (the next fsync/close reports EIO); other failures also record a sticky error and keep `dirty` — while the record exists
flush only returns it and does not put; the flush after it has been reported retries (which is why the binding needs two `flush()` calls). `dirty` is cleared only after the put
lands (it used to be cleared before the put, so a failed put left nothing to retry and nothing to report).
Case: the second one in `crates/manager/tests/fuse_lease_loss.rs` (after A loses its lease, neither the release flush nor the later
periodic sync may overwrite B's 100 bytes; fsync reports an error).

- **Write-write fencing**: the write path is wrapped in `acquire(WRITE)`; on conflict the fsspec facade raises
  `BlockingIOError`, the preempted lease is marked revoked, and `write` fails fast on a revoked lease (anonymous writes without a lease
  are still allowed). The write lease is renewed during long writes.
- **Read consistency**: relies on fresh reads + Q1 write-only leases (the binding caches only on write; release
  `forget`s and evicts the inode cache).

## Segmented files (v4)

S3 multipart Complete must **read, copy and rewrite no part bodies**, so a file's content can be
a map of "logical range → data object" (`InodeMeta.segments`, `segment.rs`):

- **Data object**: written once, immutable, attached to no directory. Written **densely** by `unit` (= `MAX_EXTENT`) at
  `[0x03][lane][data_ino][off]`, `lane = (off/unit + data_ino) % lanes` — the starting
  lane is staggered per object, otherwise every 5 MiB part has a single extent and they all pile on lane 0. Dense means every key in a range the map covers
  must exist: reading a missing or short value is **data loss**; `ChunkSpec.strict` makes the read fail instead of zero-filling by sparse
  semantics (ablation: with strict off, after deleting one extent the read returns all zeros).
- **Map**: ≤ `INLINE_SEGMENTS`(64) segments are inlined in the inode (a single-PUT object or a typical Lance data file
  costs zero extra RPCs to read); larger maps are paged into `[0x05][map_id][page]`; pages are immutable, cached by `(map_id, page)`
  in `FsState.segment_pages`, and a read fetches only the pages it touches.
- **Write**: for a segmented inode `extent::write_region` goes to `segment::write_file_range` — the buffered
  `[off, off+n)` is written as a new data object and that range in the map is replaced (`splice`; old segments that get cut only adjust
  `off/len/data_off`); new pages are written under a new `map_id`, and the inode put at the end of the flush is the only commit point.
  **No reading of old data, no RMW, no whole-file materialization**. Pipelined appends do not apply to segmented files. truncate only `clip`s
  the map; a region extended afterwards is a hole and reads as zeros, never re-exposing truncated bytes.
- **generation**: incremented on every content change (write call, truncate, map replacement); together with the inode number it
  forms the S3 ETag, which changes even for a same-size rewrite in the same second.
- **Reclaim**: every data object / paged map is recorded at `[0x04]segc/[file][id]` **before** it is written (with the length and geometry needed to
  delete it), so everything a crash leaves behind can be found. `segment::reclaim(current)` takes the file's
  **current** map as the only truth: objects in the records no longer referenced by the current map, and pages not of the current `map_id`, are all deleted;
  records still referenced are kept for later modifications. This holds for any number of modifications and any crash point, with no per-change reconciliation.
  The caller must own the file exclusively (EXCLUSIVE lease), otherwise readers holding an old map would have their data pulled out from under them.
- **Reclaiming live files**: every modification writes a `[0x04]segg/[ino]` mark **before** writing any object/page (one put per session
  per file, recorded in `FsState.segment_garbage`). It is not written only when a reference is "known to be dropped": objects written but not published
  (crash, failed inode put) are also garbage, and no other path will ever look for them — `sweep_garbage` looks only at
  marks. On this session's last close (FUSE RELEASE, Python `release`) and in the sweep (`periodic_sync` every
  30 s, one page of 1024 marks per run, the cursor `garbage_sweep_from` rotating so marks held elsewhere do not block the ones behind)
  it uses `segment::reclaim_live`: take EXCLUSIVE, **re-read** the current map **from the KV** (never the cache — another session
  may have published a newer map, and an old map would judge new objects as garbage), reclaim, delete the mark. If there is another holder, the mark stays.
  `reclaim_live` removes the ino from this session's set whatever the outcome: another session may later reclaim and delete the mark, and
  a lingering set entry would make this session's next modification skip writing the mark.
- **Modifying a segmented file requires holding the WRITE lease** (`held_write_lease`, otherwise EBUSY): the lease blocks reclaim;
  in the window where a new object is written but the map referencing it is not yet published, reclaim would delete it as garbage. FUSE holds it on open; Python's
  bare `write` to a segmented file therefore needs `acquire` first (ordinary files are unchanged), and `write()` itself succeeds and only
  flush returns EBUSY, with the buffered bytes discarded along with that error.
- **Path truncate takes its own lease**: `ATOMIC_O_TRUNC` is not negotiated, so the kernel turns `open(O_TRUNC)` into a SETATTR(size) before OPEN,
  and `truncate(2)` does not open at all. When this session holds nothing on the file it takes a one-shot
  WRITE (`segment::hold_transient_write`; held by someone else = EBUSY), returns it right after the change and reclaims in place. FUSE
  SETATTR wraps **the whole request** in this lease (the inode put at the end must carry the fence too); the core
  `write::truncate` also takes it itself, for Python / autumnfs. Otherwise `cp`/`echo >`/editors overwriting files written by S3
  would all get EBUSY (`a_path_truncate_takes_its_own_lease`, red under ablation).
  **After taking the lease, drop the cached inode and re-read from the KV**: the cache of an unopened file may predate another session's rewrite
  (only Open judges staleness by lease version); clipping a stale map and publishing it would make the following reclaim delete the other session's objects as garbage
  — turning "EBUSY" into "data deletion" (`a_path_truncate_reads_the_map_under_its_lease`, ablation:
  without the re-read, already-deleted objects are read). While this mount has the file open read-only (`tail -f`) it is still EBUSY.
- **The last close after a failed flush drops the segmented inode's cache** (whether or not the lease has been revoked): the lease has
  just been returned, and the objects the unpublished map in the cache points at are about to be reclaimed; keeping it, a reopen would read deleted objects, and periodic
  sync would put it back as ANON. The next open reads the map from the KV.
- **Cost (a deliberate trade-off, not measured)**: one extra mark put per session that modifies a segmented file in place; the reclaim at last close
  is an EXCLUSIVE acquire + map read + a scan of all the file's `segc/` records (records still referenced are never deleted,
  so the scan is O(number of objects)). Lance files written by S3 are write-once and go through `publish`, not here, so this is acceptable; if a
  workload of "repeated small modifications to large multi-part objects" appears, change it to: write the mark with `compare_put(None)` to know whether we created it,
  remember locally "whether a reference was dropped / a put failed", and on a clean close only delete the mark.
- Segments read are first passed through `Segment::checked()`: corrupt 0 lanes/unit or overflow returns an error instead of panicking the mount in a division.

### Whole-file publish (`publish.rs`, the core of S3 PUT / Copy / Complete / Delete)

A new file is written into a **new inode** (`NewFile`), invisible to everyone, and then one fenced `compare_put` swaps the
dirent over — `If-None-Match: *` (expect absent) and `If-Match` (expect it to point at that inode) are decided by the PS,
not by read-then-write. Readers see only all of the old file or all of the new one. ETag = hex(ino) ++ hex(generation).

- **Session**: every write carries the epoch of the WRITE lease on the session inode; each operation records
  `[0x04]pend/[S][obj]` before writing anything. After a session dies, `recover_dead_sessions` seizes its lease (epoch raised),
  `fence_all` raises the fence for that session on every fs partition to the new epoch (first `refresh_regions`, repeated until
  a round finds no new partition: a long-lived client's cache may predate a split, and child partitions created after the split lack
  the floor raised afterwards), and then completes or undoes each record according to where the dirent points. **The caller is the S3 gateway's sweep thread** (once every
  `--sweep-interval-secs`). It pages through all of `sess/`: each gateway has 9 live sessions (8 workers +
  the reclaim thread); reading only the first page (1024), a cluster of roughly 113 or more gateways would leave dead sessions sorted later never recovered
  (`system_publish::a_dead_session_behind_a_page_of_live_ones_is_recovered`, red when reading only one page).
  - **Retire decrements nlink only once**: if the replaced (or S3-deleted) old inode still has other names, `retire`
    first records its current bytes at `[0x04]rbase/[S][ino]`, then CASes nlink−1 from those bytes (both writes carry the
    session lease; late writes after the session was recovered by someone else are blocked by the fence); then it deletes the `pend/` record first, then `rbase/`.
    When recovery sees a Retire record and the name already belongs to the successor: with `rbase/` → the inode being exactly "it minus one" means already decremented;
    being exactly "it" means decrement now; neither (someone changed the inode after the crash) means leave it alone (a leak; it never deletes an inode another name still
    points at); without `rbase/` → decrementing had not started, so decrement as usual. Recovery deletes the session's remaining `rbase/` at the end. Previously
    a crash in "decremented, record not deleted" decremented again, and with nlink=2 deleted the inode another name pointed at
    (`system_publish::a_retire_is_recovered_once_from_every_crash_point`, red when recovery ignores `rbase/`). With only one name left it writes the tombstone directly (idempotent) and writes no `rbase/` — unless an earlier
    attempt of the same retire already wrote one (its CAS lost to another change); that `rbase/` is deleted too; if a crash leaves it, recovery judges by it,
    and the worst outcome is a leak. What the session lease blocks is a session that "wakes up only after being recovered": its CAS retry would read the nlink=1
    that recovery already decremented and write the tombstone, which is exactly what deletes data another name still points at (`system_publish::
    a_stalled_session_s_retire_is_fenced_after_its_recovery`, failpoint
    `RETIRE_TEST_HOLD`; red when the tombstone and decrement carry no lease).
  One manager acquire per live session per round (refused → skipped). Records a live session leaves itself (swaps with unknown outcome) are handled only
  after it dies. The side holding a session must run the lease heartbeat (`spawn_lease_background_tasks`), otherwise the session lease
  expires after 30 s and the sweeper takes over and fences everything it is still writing.
- **The CAS is the commit point**; nothing after it may make the publish fail: before a replace/delete it records
  `PendingOp::Retire{parent,name,ino,successor}` (`successor` = the new inode being swapped in, `None` for a
  delete); after the CAS lands it retires the old inode (`drop_name_of`) and deletes the record only on success; failures are left to recovery (previously an error retiring the old
  inode made `NewFile::publish` undo a file that was **already published**, leaving the dirent dangling —
  the part of `system_publish` that corrupts the old inode; red under ablation).
- **Recovery retires the old inode only when the dirent is exactly `successor`**: a session that swapped the name away already retired it
  once itself; retiring again an inode with nlink=2 (a Lance manifest hard-linked through FUSE) would delete the file another name still points at
  (`a_retire_whose_swap_lost_leaves_a_linked_inode_alone`; ablation: judging by "the dirent no longer points at
  it" goes red). Records use `compare_put` (expected = None), create-only and never overwritten: a record left by an earlier swap with unknown outcome
  is kept as is, and `end_swap` deletes only the one this attempt created — otherwise a later failed retry would delete the record of an earlier one that may still land.
- **Unknown outcome ≠ not landed**: when the CAS RPC errors, the dirent is read back; the new value counts as success; otherwise it returns
  `PublishError::Other` and the caller **does not undo** (an in-flight request may still land afterwards), leaving the record to recovery. Only
  definite outcomes such as PreconditionFailed / NoSuchKey / Busy / NotAFile undo.
- **The `If-Match` generation is checked only while REPLACE is held**: it reads first and then takes the lease, and in between another writer can finish its change
  and release. The old inode no longer existing at this point (another publisher swapped it first) = PreconditionFailed. This client's own **writer** of the old
  inode also counts as a conflict (the manager lets the same client take REPLACE over its own WRITE): only
  `writer_refs > 0` or `mode == WRITE` count. It used to count as a conflict whenever the inode was in `held_leases`, so on the same
  gateway worker an ongoing GET (its STABLE pin is also recorded in `held_leases`) made a PUT overwriting it
  get 409, while another worker would succeed — readers and STABLE pins of other clients never blocked REPLACE in the first place.
- **Streaming writes do not need `FsState`**: `ObjectStream` takes an
  `Rc<ClusterClient>` from `FsState` at `new`, and afterwards `write/finish` take no parameters. `NewFile::start_object` gives the new file
  a data object from the start; after that `write_streamed(..)` does not touch `FsState`, `flush_streamed()` writes the buffered tail
  and waits for all in-flight puts, and `finish` is just one inode put; `PartWriter::write/finish/abort` and
  `NewFile::abort` (undo an unpublished file, `segment::reclaim_except`) likewise need no `FsState`. Callers sharing one
  `FsState` (each S3 gateway worker) therefore hold the lock only at begin, inode put and publish; bytes never move
  inside the lock.
- **Data objects are a continuous pipeline** (`ObjectStream`): as soon as a unit (= `MAX_EXTENT`) is full a
  put is spawned, at most `--segment-stream-inflight`=8 in flight; when full it waits only for the oldest; **there is no inter-batch barrier**, so while the body is still arriving
  the earlier units are already being written. It used to collect 8 units (64 MiB) before sending a batch, and go back to receiving the body only after the whole batch landed, so body reception
  and writing were fully serial. 3-EN, 4 lane partitions, urllib client, alternating A/B: 16 MiB single stream 228 → 259 MiB/s
  (p50 68 → 58 ms), 128 MiB single stream 256 → 408 MiB/s; 8 concurrent 413 → 434, where the bottleneck is the backend (1 partition
  291, 4 partitions 433, same code). In-flight puts are spawned tasks and make progress without being polled;
  compio 0.19 dropping a `JoinHandle` cancels the task.
- **The record always stays ahead of the data**: the `segc/` (or the upload's `mpa/`) record says how long this object can be at most, and undo/reclaim deletes by it.
  The invariant is that **before any data put is sent, the record durably covers its end** (`ensure_recorded`). The record leads the
  written end by at most `--segment-stream-inflight` units (64 MiB); once half the margin is used it is raised early (about once every 5 units), and only one
  raise is in flight at a time, so they land in order. When the caller knows the length (`start_object(state, Some(n))`,
  `PartWriter::begin(.., Some(n))`; the gateway passes Content-Length / `x-amz-decoded-content-length`),
  `declare` first writes the record to `min(n, 64 MiB)` in the background, overlapping with receiving the first unit — objects ≤64 MiB therefore
  write the record only once. **A declared length can only cap the record, never inflate it**: reclaim enumerates every key by the record
  (`object_extents` materializes the whole table), the gateway does not verify signatures, and one `Content-Length: 2^64-1` plus a disconnect would make the undo build
  2^41 keys and bring down the whole gateway process (found in review; the `u64::MAX` part of the test goes red with the cap removed). A record longer than the data
  only makes deletion touch a few nonexistent keys; `finish` trims it to the exact length after all puts complete.
  The test `a_streamed_object_records_ahead_of_its_writes` reads the durable record after every chunk written and asserts it is not behind the puts already
  sent (red when puts do not wait for the record).
- **Undo settles first**: when `write` returns its puts may still be in flight, so `NewFile::abort` / `PartWriter::abort`
  first call `ObjectStream::settle` (wait for in-flight puts and record raises), then delete by the record; the failure path (`fail`) also settles first.
  What it plugs is **the put task's own retry**: the client has one connection per PS and the PS decodes writes in arrival order, so an earlier put
  always lands before a later delete (which is also why the ablation removing settle **did not go red**); but a put that hits a timeout or
  PreconditionFailed (the partition just split) refreshes and resends (the fallback in `put_opts` / `batch_put`), and that
  resend can land after the delete. In the old code `write` returned only after its puts finished, so this held by construction; the pipeline broke that, and settle
  restores it, at a cost only on the failure path.
  **Handles are awaited in place and moved out only once settled** (`settle_oldest` / `settle_raise`): compio 0.19 dropping a
  `JoinHandle` cancels the task; if it were `pop`ped before the await and the request were cancelled at that await (client disconnect), the task would be cancelled and
  `settle` would not see it either, so a put / record already on the wire could land after reclaim.
- **The gateway PUT phase timing** stays in the code: `RUST_LOG=autumn_s3::write=debug` logs one
  `PUT breakdown` line per PUT (begin/body/flush/lock/finish/publish in milliseconds). 16 MiB, single stream, measured: begin 0.4,
  body 19.6, data write 50, finish 0.2, publish 0.4 — metadata totals under 1 ms, so "merging the inode put with deleting
  pending" was not done: the two sit on either side of the commit point (the dirent CAS) and could not be merged anyway.
- **Reclaim can be handed to the background** (`FsState.reclaim_later: Option<Box<dyn Fn(Reclaim)>>`, set only by gateway workers):
  `Reclaim::Inode(ino)` / `Reclaim::Upload(id)`. When set, `retire` (overwrite, delete, Complete
  overwriting an old object) only writes the tombstone (`extent::tombstone_unreachable`) and hands it over via
  `extent::reclaim_now_or_later` **after releasing REPLACE**; `multipart::abort` and Complete's `settle` hand over upload
  cleanup (`cleanup_now_or_later`). The durable state is written before the handoff (tombstone, final upload record), so a lost handoff
  only means waiting for the next sweep. The receiver must be **a different client identity**: the manager does not let this client's STABLE block
  this client's own EXCLUSIVE, so the same identity reclaiming outside the lock would delete data that a GET pinned on this worker after the name disappeared
  is still reading; with a different identity the pin blocks it as an ordinary lease conflict. For the same reason, the handoff must happen after this client releases
  its lease on that inode — otherwise its own REPLACE conflicts with the receiver's EXCLUSIVE and reclaim falls to the sweep.
  When unset (FUSE, Python, system tests) reclaim always happens in place, and `remove_unreachable_inode` behaves unchanged;
  the Retire branch of dead-session recovery also reclaims in place. A failed in-place reclaim in `retire` only logs a WARN and deletes the Retire
  record as usual: the tombstone is already written, and the sweep will find it.
  The handoff covers only cases that "can be reclaimed now"; the following still rely on the periodic sweep (with `--sweep-interval-secs 0` this
  gateway never reclaims, and it must wait for another gateway with sweeping on): files still held by another worker / gateway / mount (after the handoff,
  EXCLUSIVE conflicts and the tombstone stays); objects in a final upload that a live session still has pending (`cleanup` leaves the upload record);
  when the heartbeat finds a pin lost (`NotHeld`) it deletes it straight from `held_leases` without going through the release path, so that inode in `unlinked_open`
  is not handed over.
- Known limitations: recovery judges by "whether the dirent points at it";
  if a publisher crashes between the CAS and deleting the record, and someone renames the name away before recovery, it is misjudged as unpublished and
  undone; Retire recovery does not decrement nlink on an inode someone changed after the crash (a leak). `flush_inode` clears `dirty` before `put_inode`, so after a FUSE_FLUSH
  put fails RELEASE does not see the failure and the segmented inode's cache is not dropped — the next Open rebuilding by version covers it,
  and PyO3 reads a strict error (not zeros).

### S3 multipart (`multipart.rs`)

**Complete reads, copies and rewrites no part bodies**: each part is written as its own immutable data object
(`ObjectStream`, the same kind of object as segmented files); Complete only reads the part records, writes the new file's `segc/` records,
map and inode, then publishes the name via `publish_inode` — the work grows with the number of parts, independent of bytes. The test first
deletes every part body, then runs Complete, and compares all `[0x03]` keys before and after Complete, proving it touched not one data key.

- **The state machine moves only by CAS**: `[0x04]mpu/[id]` → `UploadRecord{parent,name,key,state}`,
  `Open → Completing{new_ino,session,frozen} → Completed{new_ino,frozen}`, or
  `Open → Aborted`; each step is a `compare_put` on that record, so Complete and Abort have only one
  winner. `frozen` is the list of data objects Complete froze: objects written by late or failed UploadParts afterwards
  are not in it, so they can neither get mixed into the file nor be deleted as part of the file.
- **Data object ownership**: the part record `[0x04]mpp/[id][part]` points at the part's **current** object; every object the upload
  ever created is first recorded at `[0x04]mpa/[id][session][obj]` (before the object is written, rewritten as the object grows),
  including those superseded by a retry of the same part number. While Open the upload owns all of them; Complete also records `segc/` under the file, and the file
  takes over the objects in `frozen`. A final upload's `cleanup` deletes every object **not** in `frozen` —
  superseded retries, parts the Complete list did not name, everything after Abort — but skips those that still have a pending record
  (some live session is still writing; it cleans up itself after seeing the upload is no longer Open); as long as such objects remain, the upload record
  stays for the next `sweep_uploads`. The session is in the `mpa/` key precisely so it can ask "is this session still writing it?".
- **ETag = hex(object number) ++ hex(CRC32C)**: every attempt's object number differs, so the Complete list can only name
  the attempt the client actually saw succeed.
- **Sessions and recovery**: every UploadPart and Complete records, under the session, before writing anything,
  `PendingOp::Part{upload,part,data_ino}` / `PendingOp::Complete{upload}` (the latter keyed by the new file's
  inode). The two branches of `recover_session`: Part — keep the object if the upload owns it (Open and the part record
  points at it, or it is in `frozen`), otherwise delete it; Complete — when the state is this session's, this inode's
  Completing, advance to Completed and clean up if the name points at that inode, otherwise undo and reopen the upload. Abort
  meeting someone else's Completing: Busy if that session is alive; if it is dead, recover it on the spot and look again.
- **Publish is the commit point, the same rule as `publish.rs`**: no step after the name is swapped (setting Completed,
  deleting pending, cleanup) may make Complete fail; they only log a WARN and leave it to retry / recovery / sweep. When publish returns
  `PublishError::Other` (unknown outcome: an in-flight request may still land afterwards) it **does not undo**; the upload stays in this session's
  Completing and the pending record is kept; otherwise a name that lands later would point at a deleted inode
  (ablation: undoing on Other too turns `system_multipart` red where it asserts the upload is still Completing). Only errors that definitely did not
  publish (condition not met, Busy, a directory in the way, a write failure before publish) undo.
- **This session's retry can finish the job**: when the upload is stuck in this session's Completing, a retried Complete first checks the name — if it already points at
  that file, advance to Completed and return it (ablation: always Busy, `system_multipart` goes red); otherwise
  it cannot tell an in-flight not-yet-landed publish from a failed undo, so it returns Busy and waits for session recovery.
- **A retry after success gets the same answer** (S3 behaves this way too; an SDK that loses the reply retries): `complete(state,
  upload, s3_key, ..)` returns `Completed{ino, generation}`. If the upload is Completed it answers directly, without checking whether the name
  still points at that file (it answers with the original ETag even after a later overwrite or delete). Before `cleanup` deletes the upload record it first writes
  `[0x04]mpx/[deadline BE][id]` (the expiry index) and then `[0x04]mpc/[id]` = `CompletedUpload{key,
  new_ino}`; once the record is gone `completed_earlier` answers from it and checks the S3 key. Retention is
  `--multipart-completed-retention-secs` = 1 h (SDK retries give up within minutes; `object_store` defaults to 3 min);
  `expire_completed` in `sweep_uploads` scans from the head of `mpx/` and stops at the first unexpired entry, **reading only expired
  entries**, so however many records are retained the per-round scan cost does not grow (putting them in `mpu/` would make the scan every 30 s read them
  one by one). The generation is `meta::FIRST_GENERATION`: what Complete publishes is always a brand-new inode. When two cleaners
  finish at the same time, or cleanup crashes after writing the answer and before deleting the upload record and runs again, one extra expiry index entry is written: when the earlier one
  expires it deletes the answer, so in that case retention counts from the first time the answer was written and can be shorter than 1 h after the record really disappears (a gateway stopped for
  50 min and restarted leaves a retry window of only about 10 min). It never answers wrongly and never touches live data.
  Ablation: skipping `remember_completed` turns `system_multipart` red at the retry.
- **Cost of cleanup**: `frozen` is turned into a `HashSet` and then compared against the `mpa/` records one by one (at most 10000 parts ×
  a few retries, linear); `sweep_uploads` pages through all upload pages, not just the first (the first 256 Open uploads do not block
  final uploads behind them).
- **Known limitations**:
  - The gateway's reclaim thread calls `sweep_uploads` and `recover_dead_sessions`, and receives the
    `Reclaim`s workers hand over; for the HTTP routes see the `autumn-s3` section of `crates/server/CLAUDE.md`.
  - A retry arriving after the retention period still gets NoSuchUpload.
  - `create` records no pending: if a session dies after creating the record, the Open upload it leaves has no parts and is not automatically
    cleaned up (S3 handles this case with lifecycle AbortIncompleteMultipartUpload; that does not exist here yet).
  - An UploadPart received while a Complete is in progress gets Busy; if that Complete then fails and reopens the upload, the part
    has already been deleted and the client must re-upload it.
  - `PendingOp` gained two variants at the end; the discriminants of existing variants are unchanged, so existing pending records
    need no conversion.

### Reclaiming unreachable inodes waits for holders

`remove_unreachable_inode` (unlink / rename over) first writes `rmtomb`, then: if this session still has it
open → it is recorded in `FsState.unlinked_open` and reclaimed at the last close (FUSE RELEASE); otherwise
`reclaim_unreachable` takes an **EXCLUSIVE** lease — if another client still holds any lease (another mount has it open,
an S3 GET is reading) it is a Conflict and the tombstone is left to the sweep (at mount + `periodic_sync` every 30 s).
Only once taken is the data deleted, the deletes fenced with that lease's epoch; between delete phases the same client re-acquires
EXCLUSIVE to confirm exclusivity again (a manager failover replays it as an ordinary WRITE and lets readers in). Inodes known not to be
segmented files skip the segc record scan. unlink used to delete data immediately, and a file open on another mount would read
holes. REPLACE/EXCLUSIVE is not persisted, so each unlink costs two more manager in-memory RTTs and no etcd write.
Tests `system_segmented.rs::reclaim_waits_for_other_holders` (ablation: skipping the lease, data
is deleted while held), `unlinked_while_open_here_is_reclaimed_at_last_close`.

The legacy extent prefix scans (`scan_extents`, `clean_beyond_eof`, `delete_all_extents_with`)
continue from the successor of **the last returned key**, not from the last parsed offset: 18-byte striped / data object keys
(lane 0, inode number = `ino << 8 | x`) fall inside a 17-byte legacy prefix, and when a whole page is such keys
the old cursor never advanced.

## Schema version stamp (fail-loud)

`schema::SCHEMA_VERSION` = **4**, stored at `[0x04]schema_version` (relative key, i.e.
`fs/[0x04]schema_version`). `meta::ensure_schema_version` at mount (the `ensure_root`
entry) stamps it if missing, checks it if present, and **fails loud and refuses to mount on mismatch** (guarding against a future incompatible layout silently reading/writing corrupt data).
- v1 = pre-namespace bare keys (never stamped).
- v2 = namespaced relative layout + global inode counter.
- v3 = lane striping: `InodeMeta` gains the `stripe` field (rkyv layout changes; v2 inode bytes do not decode),
  large files use lane-striped keys. v2→v3 is a stop-world reset with no in-place migration; small/legacy files
  stay `stripe=None` + `[0x03][ino][off]`. BUMP whenever the layout/encoding changes incompatibly.
- v4 = segmented files + content generation: `InodeMeta` gains `generation` and `segments`. The historical migration converted in place with a one-shot
  offline tool, keeping every inode's number/links/bytes, yielding `generation=1` and
  `segments=None`, changing the stamp last, with no reset; the tool has since been removed.
- **Missing stamp ≠ new tree**: only a tree with no inode at all counts as new and gets the current version stamped; inodes without a stamp were created by
  pre-v4 tools that did not stamp (`autumnfs`, the S3 gateway), and the inodes are v3 — mount is refused, pointing to
  the standalone conversion procedure of the corresponding historical release. It used to stamp v4 directly; in an actual conversion that made a v3 tree
  entirely unreadable (`an_unstamped_populated_tree_is_refused_not_stamped`).

## Key dependency files

| File | Use |
|------|------|
| `crates/client/src/lib.rs` | ClusterClient — entry point for all KV operations |
| `crates/rpc/src/partition_rpc.rs` | PutReq/GetReq/RangeReq/DeleteReq |

## Crash-consistency contract

The fuse layer has no multi-key atomic commit (the full design, a per-inode generation manifest, is still deferred). What it guarantees
is a strict ordering discipline: a crash **never fabricates data**, and at most loses the most recent un-fsynced writes. Rules (present-tense
invariant + one sentence of reason):

- **Grow / write**: the inode-meta size is advanced and persisted only **after** the extent KV put is ACKed by all replicas
  (`write_region(..).await?`). Reason: a crash only makes the durable size lag the written extents (the file looks
  older; POSIX accepts losing un-fsynced data); beyond-size extents are benign orphans (reads are bounded by size and do not see them,
  regrow overwrites them with the same `[0x03][ino][off]` key, unlink/truncate prefix scans reclaim them).
- **Read-after-write barrier**: before a read, a dirty write buffer that overlaps the read range **must be flushed first**. Reason:
  otherwise the read sees the old content not yet on disk, breaking read-after-write consistency.
- **In-place overwrite (RMW) read barrier**: a partial write inside an existing extent must
  read-modify-write, and reading the old value **must use the `kv_get_opt` barrier and PROPAGATE hard errors** (same as
  `clean_beyond_eof`). Reason: swallowing a transient RPC/routing/storage error as an empty value would zero-fill the untouched prefix
  `[start, offset)`, truncate the untouched suffix, and then put → fabricating zeros / losing bytes in a successful write (`get`/`put`
  each have an independent ~13s retry budget; when the PS is briefly unavailable, get exhausts its budget while put later succeeds → deterministic corruption). Only
  a genuine `Ok(None)` (should not happen for a mapped extent) is treated as a sparse empty value. Guard: `scripts/fuse_rmw_chaos.sh`
  (single PS, partial overwrite + PS kill + restart-at-16s so the get budget is exhausted while the put lands,
  asserting the untouched prefix is never zeroed).
- **Shrink / truncate**: the inode-meta put is the **COMMIT POINT**; it lands first, then extents are deleted/shrunk.
  Reason: the reverse order would read zeros inside the file when the durable size=old but the tail data is deleted. **Invariant:
  content[0..size] always equals the last successfully written content; a crash can only choose whether the old or new size survives.**
  Guard: `fuse_chaos.sh` T1 (truncate burst + kill -9 mid-burst + remount + exact prefix check).
- **Leftover reaping on grow** (`clean_beyond_eof(ino, eof)`): every grow path first does a
  raw prefix scan, deleting whole keys ≥ eof and shrinking the straddler by its **real KV value length**. Reason:
  beyond-size extents left by a crash after a meta-first shrink would later be taken by a grow as resurrected old data
  (POSIX requires zeros). It must run **before** the in-memory size bump (the bump erases the pre-grow EOF; otherwise at
  flush `write_region` sees the grown size and the stale straddler tail looks like legitimate in-file
  data). It runs at the `write::write` entry (`offset > size`), in the grow branch of `write::truncate`
  (before the meta put), and at the `write_region` entry (defensive). A hard kv error propagates and aborts the grow; only a genuinely missing
  key may be skipped (`kv_get_opt`). Consecutive appends are self-bounding (the new key at old-EOF caps the straddler's inferred
  length), so the hot path pays only one size comparison.
- **Post-commit cleanup errors do not fail truncate**: once the meta lands it is committed; reporting a cleanup error would make the
  caller retry into the `new_size == old_size` early return (a no-op that never cleans again). WARN + invalidate;
  leftovers are reclaimed by the next grow/unlink.
- **UNLINK-1 tombstone**: unlink and rename-over delete the target's data through `extent::remove_unreachable_inode`
  — an intent tombstone is written **the moment** the inode becomes UNREACHABLE
  (`[0x04]rmtomb/[ino]`), then the extents + inode key + tombstone are deleted;
  `sweep_unlink_tombstones` replays survivors on every mount (Init). **Invariant: tombstones are written only for
  unreachable inodes** (the sweep deletes unconditionally) — in rename-over this forces the delete to happen after the dirent is
  overwritten. Reason: the leftover leak window shrinks from "a whole scan + N deletes" to the single unreachability→tombstone RPC
  gap. rename-over must also delete the target's **extents** (otherwise the POSIX atomic-save pattern "write tmp; mv tmp
  file" leaks the entire old content every time). Guard: `fuse_chaos` T3.
- **Read-path boundary short-circuit** (partition-server + ioring): a
  sub-range GET entirely past the end of a VP value short-circuits to empty (`read_value_from_log` does not return stale
  content from a reused RegPool buffer); on the caller side, ioring `read_into` zeroes the unwritten tail of short/empty extent slices (dest is a reused
  ring buffer, not a fresh zero Vec). Reason: reading a shortened/sparse extent window should yield zeros, not garbage.

## Manifest hard links

Lance's local ConditionalRenameCommitHandler uses linkat(source, destination),
then unlinks its temporary source. The default fuser callback returned EPERM,
so table creation failed although ordinary rename worked (strace confirmed).
Link now dispatches to dir::link: regular files only, persist the increased nlink,
then create the destination dirent using compare_put(None). Existing destinations
return EEXIST; a lost-ACK retry recognizing the same inode succeeds. Source unlink
preserves the linked inode. Data is not copied. A pre-existing-target check avoids
unnecessary inode metadata writes in the usual conflict case.

This retains the filesystem's existing nontransactional namespace limitation:
inode reference counts and directory entries are separate KV writes, and mutation
serialization is per mount. A crash after the count update can leak a reference.
It is suitable for the one-mount Lance demo; it does not establish crash-atomic
or multi-mount namespace transactions.

## Symbolic links

Added for the HuggingFace cache: `hf download` makes `snapshots/<rev>/<file>` a
relative link to `../../blobs/<hash>`. On a mount without links it falls back to
copies (a warning, and revisions no longer share blobs), and `cp -a` of an
existing cache fails outright.

- **Storage**: a link is an `S_IFLNK|0777` inode with the target stored verbatim
  in `InodeMeta.symlink_target` (the field existed but was never written), `size`
  = the target's length (what `lstat` reports), dirent `DT_LNK`, no data
  extents. `dir::symlink` goes through `create_child`, create-only like `create`;
  the target is neither checked nor resolved. An empty target is ENOENT; over
  4095 bytes or containing NUL is ENAMETOOLONG. `dir::readlink` of anything else
  is EINVAL.
- **Remove and rename need no new code**: `unlink` and rename-over go by nlink
  through `change_links` → `remove_unreachable_inode`; a link has no extents, so
  reclaim deletes only the inode. A hard link to a link is still EPERM
  (`dir::link` takes regular files only); HF does not need one.
- **Path resolution**: on a mount the kernel resolves links; front-ends without
  one use `dir::resolve_follow(path, follow_last)`.
  - A relative target starts from the link's directory. `..` goes back to the
    directory actually walked before (not textually); the root's `..` is the root.
  - An absolute target starts from **this tree's root**, the only root a
    front-end without the mount knows. Absolute links made on a mount name host
    paths and usually resolve to nothing here.
  - Over 40 hops (Linux MAXSYMLINKS) is ELOOP.
  - `follow_last = false` is `lstat`: a link in the last component is returned
    itself; links before it are followed, and so is a last one written `link/`
    or `link/.`, as POSIX does. The last component is the one popped when the
    queue becomes empty: a link's expansion always goes to the front of the
    queue, so nothing it expanded into can be popped last.
  - A link whose inode is gone (removed after its name was read) resolves to
    nothing, not an error.
  - `resolve` / `resolve_typed` do not follow. The write paths (S3 directory
    creation, publish) keep using them: a link is not a directory, and writing
    under one is refused.
- **Per front-end**:
  - `autumn-s3`: GET / HEAD read the file a key's link leads to. A listing yields
    `DT_LNK` entries as objects and `list_objects` resolves each (a few KV reads
    per link, sequential): a link to a file is listed with **the target's** size
    and ETag; a dangling or looping link, or one to a directory, is left out.
    A looping link is a 404 like a dangling one, never a 500 (the gateway maps
    ELOOP to no object). A page can therefore hold fewer than `max-keys` entries
    and still carry a continuation token, which S3 allows. The recursive walk
    never descends into a directory link, but a prefix that passes through one
    is resolved like a GET, so `prefix=dirlink/` lists the target's entries under
    `dirlink/` while the parent's listing leaves `dirlink` out. PUT / DELETE of a
    link's key act on the link itself, as `rm` does; an `If-Match` with the ETag a
    GET reported (the target's) therefore fails 412 (`hold_for_swap` compares the
    dirent's inode).
  - Python `autumn.Fs`: `symlink(parent, name, target)`, `readlink(ino)`;
    `resolve(path)` follows (`os.stat`), `follow_symlinks=False` is `os.lstat`.
    The data calls (`read`, `write`, `truncate`) take an inode and do not check
    its type, as for a directory inode.
  - `autumnfs`: `ln -s TARGET PATH`, `readlink PATH` (links before the last
    component followed); `cat` / `get` follow; `ls` / `stat` / `rm` act on the
    link itself and do not follow links earlier in the path.
- **Tests**: `crates/manager/tests/system_fs_symlink.rs` (core: `..`, directory
  links, a link in the middle of a path, trailing `/`, absolute, dangling,
  vanished inode, ELOOP, EINVAL, EEXIST, unlink and rename-over reclaiming the
  link inode) and `crates/manager/tests/fuse_symlink.rs` (real mount +
  `autumn-s3`: the HF layout, `cp -a`, ELOOP on the mount, a looping link as an
  S3 404). Ablations, each red: `..` not going back, `follow_last` ignored, a
  trailing `/` not followed, a vanished link inode an error, S3 not following,
  the listing skipping links, ELOOP a 500, FUSE not answering readlink. By hand:
  `hf download` onto the mount prints no link warning, leaves 10 links, and
  `diff -r` matches the same download on a local disk.
