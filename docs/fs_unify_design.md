# Design: unify `autumn-fuse` + `autumn-fsspec` into one filesystem

**Status: APPROVED 2026-07-03 — decisions locked, implementation started (M0).**
User decisions on the open questions:
- **Q1 → option (ii)**: WRITE-only leases; reads stay coherent via
  `poll_invalidations` + `cache_is_stale` (no per-read manager RTT).
- **Q2 → option (B)**: manager-side `AllocInodes(n) → base` RPC; the manager
  persists the counter to etcd **with CAS** (txn on mod_revision), so even a
  leader-transition window cannot hand out overlapping ranges.
- **Q5 → lean surface**: file/dir/size/mtime/read/write/rename/unlink only —
  no mode/uid/gid/symlink fidelity through the Python facade.

Original context: requested 2026-07-03 after
the observation that a POSIX mount and a Python fsspec client, both being
*filesystem* surfaces, should be **one system** (shared on-disk layout **and**
shared lease/fencing), not two coexisting namespaces.

The `autumn_fsspec` adapter as first built uses its own `fs/`-prefixed,
path-keyed namespace — safe to run beside a fuse mount, but the two see
different files and neither fences the other. This doc specifies how to make
them the *same* filesystem.

---

## 1. Goal & non-goals

**Goal.** One filesystem, two front-ends:
- `autumn-fuse` — kernel POSIX mount (unchanged UX).
- `autumn_fsspec` — Python `autumn://` client.

A file created through either is visible and byte-identical through the other,
and concurrent writers are fenced by the **same per-inode lease** so they can't
corrupt each other. Both front-ends sit on **one shared Rust FS core**.

**Non-goals (v1).**
- Full POSIX semantics on the Python side (hardlinks, mode/uid/gid fidelity,
  O_APPEND races). fsspec is an object-store-shaped API; we map it onto the
  inode layout, not the reverse.
- Reimplementing the namespace/lease logic in Python. The core stays in Rust,
  bound via PyO3 — one implementation, no drift.
- Cross-host distributed POSIX locking beyond what the existing inode lease
  already provides.

---

## 2. What exists today (grounded in code)

### 2.1 fuse on-disk layout (`crates/fuse/src/{key,schema}.rs` — un-feature-gated)
| prefix | key | value |
|---|---|---|
| `0x01` | `[0x01][ino u64BE]` | `InodeMeta` (rkyv): mode/uid/gid/size/nlink/times, `inline_data` (≤4 KiB), `symlink_target` |
| `0x02` | `[0x02][parent_ino u64BE][name]` | `DirentValue { child_inode, file_type }` |
| `0x03` | `[0x03][ino u64BE][logical_off u64BE]` | extent bytes (variable, ≤ `MAX_EXTENT` = 8 MiB) |
| `0x04` | `[0x04][field]` | superblock (incl. `next_inode` counter) |

Files: inline ≤ 4 KiB in `InodeMeta.inline_data`; larger → variable extents
keyed by logical offset (range-scan `[0x03][ino]` yields them in order).
`ROOT_INO = 1`, `INODE_ALLOC_BATCH = 1000`.

### 2.2 Namespace core ops (already ~pure over `ClusterClient` + `key`/`schema`)
- `meta.rs`: `alloc_inode`, `get_inode`, `put_inode`, `new_file_meta`, `new_dir_meta`
- `dir.rs`: `lookup`, `readdir`, `mkdir`, `rmdir`, `rename`
- `extent.rs`: `write_region`, `extents_snapshot`, `truncate_extents`, `delete_all_extents`, `clean_beyond_eof`, `remove_unreachable_inode`
- `write.rs`: `write`, `flush_inode`, `truncate` · `read.rs`: `prepare`/`execute`/`read`

These take `&mut FsState`. **Coupling to `fuser` is thin** — only the return-side
`InodeMeta → fuser::FileAttr` / `fuser::FileType` conversions in `meta.rs`/`dir.rs`.
The KV logic itself is `fuser`-free. `dispatch.rs` (1668 lines) is the actual
FUSE-protocol glue (the `fuser::Filesystem` trait impl) and stays fuse-only.

### 2.3 Lease is ALREADY a reusable client library (`crates/client/src/lease.rs`)
Framework-agnostic, `ClusterClient`-based:
- `acquire(cluster, client: &DaemonClientId, ino, mode) -> AcquireResult`
  (+ `acquire_force`, `acquire_with_preempt_wait`)
- `release(...)`, `heartbeat(...)`, `poll_invalidations(...)`, `apply_invalidation`, `cache_is_stale`
- `DaemonClientId::new_fuse(host)`; modes `LEASE_MODE_READ` / `LEASE_MODE_WRITE`
- writer-XOR-readers per inode, fenced by `lease_epoch` (manager
  `MgrInodeLeaseInfo.version`); manager pushes `WriterClosed`/`LeaseRevoked`
  for close-to-open coherence.

**Implication:** the two hard parts (lease protocol; namespace KV logic) are
already reusable or nearly so. The work is *decoupling + binding + a facade*,
not new distributed-systems design.

### 2.4 The one real prerequisite bug — inode allocator is not CAS
`meta.rs:alloc_inode` does a **non-atomic RMW** on the `next_inode` counter:
```rust
let current = kv_get(next_inode_key);          // read
kv_put(next_inode_key, current + BATCH);       // write — NO compare-and-swap
```
Safe for a single allocator; two concurrent allocators (fuse + fsspec, or two
mounts) can both read `current` and claim the same [current, current+BATCH)
batch → **duplicate inodes → corruption**. Making fsspec a co-equal writer
requires fixing this (§6). (Arguably a latent multi-mount bug in fuse already.)

---

## 3. Target architecture

```
        ┌──────────────────────┐        ┌──────────────────────────┐
        │  autumn-fuse (bin)   │        │  autumn_fsspec (Python)  │
        │  fuser::Filesystem   │        │  AbstractFileSystem      │
        │  dispatch.rs + kernel│        │  path↔inode facade       │
        │  page-cache invalid. │        │  lease on open/close     │
        └──────────┬───────────┘        └────────────┬─────────────┘
                   │  InodeMeta/FileAttr              │  PyO3
                   ▼                                  ▼
        ┌───────────────────────────────────────────────────────────┐
        │   SHARED FS CORE  (feature-independent Rust)               │
        │   namespace: alloc_inode(CAS) · lookup/readdir/mkdir/…     │
        │   data: write_region · read · truncate · extents_snapshot  │
        │   lease: autumn_client::lease (acquire/heartbeat/release)  │
        │   returns plain InodeMeta/DirentValue (NO fuser types)     │
        └───────────────────────────┬───────────────────────────────┘
                                    │ ClusterClient (put/get/range/CAS)
                                    ▼
                     partition layer (ordered KV) · manager (leases)
```

Two decisions embodied here:
1. **Core returns plain data** (`InodeMeta`, `DirentValue`, byte buffers). The
   `→ fuser::FileAttr` conversion moves *up* into `dispatch.rs` (fuse-only). The
   Python binding converts to its own dict/`info` shape.
2. **`FsState` splits** into (a) a lean **`FsCore`** (the `ClusterClient`, the
   inode-alloc cursor, the `DaemonClientId`, `held_leases`, `invalidations`) that
   both front-ends own, and (b) FUSE-runtime-only caches (`inodes` InodeState
   map, per-inode `WriteBuffer`, `kernel_invalidator`, `notify_inval_failed`,
   `lookup_count`) that stay in the fuse binary. The Python side keeps its own,
   simpler per-open state.

### 3.1 Crate layout options
- **(a) In-place in `crates/fuse`** — move the pure core behind a *new default*
  feature (e.g. `core`) and gate only the `fuser`-touching bits behind `fuse`.
  `python/` deps `autumn-fuse` with `default-features=false, features=["core"]`.
  Least churn; keeps history.
- **(b) New `crates/fs-core`** — extract `key`/`schema`/`meta`/`dir`/`extent`/
  `read`/`write` into a standalone crate; `autumn-fuse` and `python/` both dep it.
  Cleaner boundary, more moving of files.

Recommendation: **(a) first** (feature split, low risk), promote to (b) later if
the boundary proves stable. Either way the diff to *behavior* is nil — it's a
visibility/typing refactor guarded by the existing fuse test suite.

---

## 4. PyO3 FS-core API (new, in `python/src/`)

A new `autumn.Fs` class (beside `Client`/`BatchClient`/`Memory`), one compio
worker thread hosting an `FsCore`, async methods bridged like `Client`:

```
Fs.connect(manager, *, host=None) -> Fs      # DaemonClientId::new_fuse(host)
  # path resolution + metadata
  resolve(path) -> ino | None                 # walk dirents from ROOT_INO
  getattr(ino) -> InodeInfo(dict)             # InodeMeta as a dict
  readdir(ino) -> list[(name, ino, kind)]
  lookup(parent_ino, name) -> (ino, kind) | None
  # mutations (each fences via lease as in §5)
  mkdir(parent, name, mode) -> ino
  create(parent, name, mode) -> ino           # empty file
  unlink(parent, name); rmdir(parent, name); rename(...)
  # data
  read(ino, offset, size) -> bytes            # read::read
  write(ino, offset, data) -> n               # write::write (buffered→extents)
  flush(ino); truncate(ino, size)
  # leases (explicit, for the facade to drive)
  acquire(ino, mode) -> lease_epoch; heartbeat(ino); release(ino)
```

`InodeInfo` carries what fsspec `info()` needs (size, type, mtime, mode). The
binding reuses the *same* `FsCore` methods the fuse binary calls — no logic
duplicated in Python.

---

## 5. Lease flow (the "one system" guarantee)

The facade drives the existing lease library exactly as the fuse dispatcher does:

- **Open for read** (`cat_file`, read `open`): `acquire(ino, READ)`. Honor
  `poll_invalidations` / `cache_is_stale(ino, lease_epoch, inv)` so a write by
  the fuse side (or another client) that bumped the epoch forces a re-`getattr`
  + extent rescan before serving bytes (close-to-open coherence).
- **Open for write** (`wb`/`ab`/`pipe_file`): `acquire(ino, WRITE)`. Writer-XOR
  means a concurrent fuse writer holding the lease makes us wait/`RevokePending`;
  we hold the write lease across the whole write, **`heartbeat` on a timer**
  (TTL renewal) for long multi-GB uploads, and `release` on close/flush.
- **Crash safety.** A Python script that dies mid-write stops heartbeating; the
  manager expires the lease after its TTL and a fuse mount (or a retry) can take
  over — the fencing epoch guarantees the dead writer's late RPCs are refused.
  This is precisely why fsspec must join the lease system rather than blind-write.

**Open design question (Q1):** leases cost a manager RTT per open. For
read-mostly dataset loading (thousands of small `info`/`cat` calls), per-open
READ leases may be wasteful. Options: (i) always lease (simplest, correct);
(ii) lease only writes, and for reads rely on epoch-stamped `getattr` +
invalidation without a formal read lease; (iii) a process-lifetime "session"
lease cache with lazy renewal. Recommend **(ii)** — write-fenced, read-coherent,
cheap — but call it out for your decision.

---

## 6. Inode allocator → crash-safe, multi-writer (prerequisite)

Two ways to make allocation safe for concurrent allocators:

- **(A) CAS on the KV counter.** Add a compare-and-swap put to the SDK
  (`put_if_eq(key, expected, new)`); `alloc_inode` loops read→CAS until it wins
  its batch. Minimal, stays client-side. Needs a PS-level CAS primitive (check
  whether one exists; the memtable is single-writer per partition so a
  conditional put is cheap to add).
- **(B) Manager-side `AllocInodes(n) -> base` RPC.** The manager (leader-fenced,
  crash-safe — cf. `[[feedback_orchestrator_must_be_crash_safe]]`) owns the
  counter and hands out ranges. Most robust; matches how the manager already
  grants owner/lease epochs. Slightly more work.

Recommendation: **(B)** if we want the allocator to be authoritative and
crash-safe like the rest of the control plane; **(A)** if we want to keep it in
the data plane and a CAS put is easy to add. **Open question (Q2)** for you.

Either way this is a **standalone, independently-shippable fix** that also
hardens multi-mount fuse today — worth doing first (Milestone 0).

---

## 7. fsspec facade rewrite

`AutumnFileSystem` methods re-expressed over `autumn.Fs` (inode layout):

| fsspec | maps to |
|---|---|
| `_strip_protocol` / path norm | unchanged |
| `info(path)` | `resolve(path)` → `getattr(ino)` → `{size,type,mtime}` |
| `ls(path)` | `resolve` → `readdir(ino)` → child names + per-child `getattr` (real dirents now — no keys-only-range dance) |
| `_open(rb)` / `cat_file` | `resolve`+`acquire(READ)`; `read(ino, off, size)` |
| `_open(wb/ab)` | `create`/resolve + `acquire(WRITE)`; buffered `write(ino,…)`; `flush`+`release` on close |
| `pipe_file` | create + write + flush + release |
| `rm_file` / `rm` | `unlink` / `rmdir` (recursive walk) |
| `mkdir`/`makedirs` | `mkdir` per component |

Wins: real directories (no s3fs-style implicit-dir emulation, no keys-only-range
manifest fetch); shared files with fuse; POSIX metadata. The current chunked
`fs/` layout, manifests, and the `ls`-via-multi-get logic are **retired**.

---

## 8. Migration & compatibility

- **No data migration needed.** `autumn_fsspec` is unreleased; the only `fs/`
  data is throwaway test data. Clean cutover — delete the `fs/` layout code.
- **fuse on-disk format unchanged** — we only refactor *where its code lives*
  and *what types it returns*, guarded by the existing fuse test suite.
- **kvcache/memory untouched** — they use `kvc/`/`doc/`… ASCII namespaces,
  disjoint from the `0x01–0x04` fuse layout.
- **Rollback story** intact — `[[feedback_stopworld_restart_primary]]`: no
  persistent-format change, so stop-world restart is safe.

---

## 9. Test matrix

- **Cross-surface interop (headless):** write a file via `autumn.Fs` (the core),
  read it back byte-identical via `autumn.Fs`; and — with a real mount in CI that
  supports FUSE — write via the mount, read via fsspec and vice versa.
- **Fencing:** two `Fs` clients (distinct `DaemonClientId`) both open the same
  inode for write → second gets `RevokePending`/waits; kill the first without
  release → after TTL the second acquires; the first's late `write` is refused
  (epoch fence). Mirror the existing `F-fuse-lease-*` manager tests.
- **Allocator concurrency:** N concurrent `create()` across 2+ clients → all
  inodes unique (the §6 fix; fails today).
- **Coherence:** fuse writes+closes, fsspec read sees new bytes (invalidation /
  `cache_is_stale`).
- **Existing fuse suite stays green** (the refactor is behavior-preserving).
- **datasets round-trip** still passes over the new backing layout.

---

## 10. Risks / open questions

- **Q1 (read leases):** always-lease vs write-only-lease + epoch-coherent reads.
  Recommend write-only + coherent reads (§5).
- **Q2 (allocator):** CAS-in-SDK vs manager `AllocInodes` RPC (§6).
- **Q3 (batch waste):** `INODE_ALLOC_BATCH=1000` per allocator means a
  short-lived Python script that creates one file burns a 1000-inode batch.
  Fine (inodes are u64), but consider a smaller batch or manager-side single
  alloc for the client identity.
- **Q4 (perf on tiny ops):** every `info`/`ls` becomes a dirent walk + getattr
  RTTs. For huge dataset trees this is more chatty than the current single
  range-scan. Mitigate with a short-lived path→ino + attr cache honoring
  invalidations. Measure before optimizing.
- **Q5 (POSIX surface breadth):** confirm the Python surface only needs
  file/dir/size/read/write (not mode/uid/gid/symlink fidelity) for the
  datasets/checkpoints use case, so we can keep the facade lean.
- **Concurrency model:** the core is `!Send` compio-thread-local (like `Client`);
  the PyO3 binding uses the same single-worker-thread bridge — fine.

---

## 11. Phased plan

| M | Deliverable | Acceptance |
|---|---|---|
| **M0** ✅ **DONE 2026-07-03** | Inode allocator → crash-safe multi-writer: manager `MSG_ALLOC_INODES` (0x53, WIRE v11) grants `[base, base+count)` via leader-fenced etcd **value-CAS** on `autumn-rs/fs/next_inode` (`crates/manager/src/fs_alloc.rs`); fuse `alloc_inode` refills batches from it, passing the legacy `[0x04]next_inode` value as the migration **floor** (+ best-effort legacy-key refresh for disaster rebuilds); `ClusterClient::alloc_inodes(count, floor)` | ✅ 16-way concurrent grants disjoint (memory) + 8-way over real etcd CAS; floor raises-never-rewinds; persisted watermark exact (BE u64); **follower refuses NOT_LEADER** (split-brain guard); count=0 refused; fuse suite green (lib 44 + f_fuse_lease_1 6/6 + f_fuse_lease_2 2/2 + system_fuse_read — all incl. `--ignored`); rpc/manager/client lib tests green; wire registry v11 fingerprint recorded |
| **M1** ✅ **DONE 2026-07-03** | Decouple FS core from `fuser`: new `core` feature (meta/dir/extent/read/write/state + key/schema; only extra dep = libc) with `fuse = ["core", …]`; new fuse-gated **`attr.rs`** = the ONLY core→fuser conversion point (`inode_to_attr`/`dt_to_filetype` moved out of meta/dir); `dir::lookup`/`mkdir` return `(ino, InodeMeta)`, `ReaddirEntry` moved to `schema` with `kind: DT_*` byte (bridge re-exports); dispatch/ops convert at the reply boundary; `S_IF*` mode-format consts made `pub` | ✅ `cargo build --no-default-features --features core` clean, **fuser count in `cargo tree` = 0**; default build 0 warnings; fuse lib 44 + core-only 12 tests; manager fuse e2e (lease 6/6 + 2/2 + system_fuse_read) green incl. `--ignored`; workspace builds; **fuse_chaos PASS (65 files)** — behavior-preservation gold standard |
| **M2** ✅ **DONE 2026-07-03** | PyO3 `autumn.Fs` binding (§4) over the core + `autumn_client::lease` (`python/src/fs.rs`): a sync-blocking façade (like `Memory`) — a dedicated compio worker owns the `!Send` `FsState`, jobs take `&mut FsState` via a `for<'a> FnOnce(&'a mut FsState) -> LocalBoxFuture<'a,()>` boxed closure. Full §4 surface: resolve/getattr/readdir/lookup/mkdir/create/unlink/rmdir/rename/read/write/flush/truncate + lease acquire/heartbeat/release. Core additions (single source, no fuse↔binding drift): `dir::create`/`dir::unlink`/`dir::resolve`, `meta::ensure_root` (fail-loud, no local seed — all inodes via the M0 manager grant), `FsState::new_with_host` (no env read). `dispatch.rs` Create/Unlink/init_root refactored to call the shared core. | ✅ headless py e2e (`python/tests/run_fs_e2e.sh`, isolated memory-mode cluster): create/write/flush/read byte-exact (inline + 10 MiB multi-extent + ranged across the 8 MiB boundary + EOF clamp), getattr/resolve/readdir/lookup, mkdir/rename/truncate/unlink, lease acquire→heartbeat→release smoke, **CROSS-INSTANCE byte-exact** (write via one `Fs`, read via a second + a committed shrink visible cross-client). Behavior preservation: fuse lib 44, fuse e2e `f_fuse_lease_1` 6/6 (incl. `fuse_create_acquires_writer_lease` + two-mount `init_root`) + `f_fuse_lease_2` 2/2 + `system_fuse_read` (write/read/truncate/delete_all_extents), core-only build with **fuser count = 0**, workspace build |
| **M3** ✅ **DONE 2026-07-03** | `autumn_fsspec` rewritten as a thin facade over `autumn.Fs` (§7): `info`/`ls`→`resolve`+`readdir` (real dirents), `cat_file`→`read`, write/`pipe_file`→auto-`mkdir`+`create`+`write`+`flush` (overwrite truncates-first for exact size), `mkdir`/`rm`/`mv`→`mkdir`/`unlink`+`rmdir`/`rename`. Fully synchronous (no asyncio bridge — `Fs` is sync). **Retired** the `fs/` chunked-manifest layout (`_layout.py`, `_bridge.py`, `fake_kv.py` deleted). Offline runs the SAME facade over a Python inode tree (`tests/fake_fs.py`, mirrors the `Fs` sync API). | ✅ offline 28 (`test_fs_offline` size-boundary round-trips/ranged/ls/info/dirs/mkdir/rm/overwrite-shrink/append/exclusive/mv/root-bucket + `test_datasets_offline` save_to_disk/load_from_disk + load_dataset('json') + `test_vllm_loader_offline` + `test_models_offline` upload/materialize SHA-verified); **live 9** over a real cluster (`run_fsspec_e2e.sh` isolated bring-up) — 0 B–5 MiB round-trips + cross-extent ranged, ls/find/rm, overwrite-shrink exact, cross-boundary append, exclusive create, **datasets round-trip**. Shared layout: fsspec + fuse now see the same inode keys |
| **M4** ✅ **DONE 2026-07-03** | Lease integration + fencing (§5, §9). The per-session lease background tasks (heartbeat + invalidation poll + revoked-eviction) extracted from the fuse-gated `dispatch.rs` into the fuser-free **`lease_tasks.rs`** (core); the PyO3 binding spawns them with `None` invalidator (headless) so a facade's WRITE lease is heartbeated for long writes + marked revoked on preemption. Binding gains `Fs.forget(ino)` (evict cache) + a revoked-write fast-fail. `autumn_fsspec` acquires the WRITE lease around every write (`pipe_file` + buffered `_initiate_upload`→`_upload_chunk`), releasing + `forget`ing on close — close-to-open coherence for cross-client reads (per Q1: write-only leases, reads coherent by fresh-read since the binding caches only on write). | ✅ **fencing** (`run_fs_lease_e2e.sh`, two distinct `DaemonClientId`): write-lease XOR (A holds ⇒ B conflicts; A releases ⇒ B acquires) + cross-client coherence + `forget()` evicts a stale cache; **facade coherence** (`test_e2e_cluster.py::test_cross_facade_coherence`: write via one `AutumnFileSystem`, read latest via an independent one); fsspec live 10 + offline 30 green on the lease path; fuse suite green after the extraction (`f_fuse_lease_1` 6/6 + `f_fuse_lease_2` 2/2 + `system_fuse_read`); core-only build `fuser`=0. **write-via-mount/read-via-fsspec byte-identity VERIFIED on a real `/dev/fuse` mount** (`run_mount_fsspec_interop.sh`): file written through the autumn-fuse kernel mount reads byte-exact via fsspec and vice versa (37 B + 10 MiB each way), directories interop both ways — genuinely one filesystem. |

M0 and M1 are independently valuable and low-risk; M2–M4 deliver the unified
filesystem. Estimated scope: M0 small, M1 medium (mechanical), M2 medium, M3
medium, M4 medium — several focused sessions, each commit-clean.

---

## 12. Recommendation

Proceed **M0 → M4** in order, deciding Q1 (write-only leases) and Q2
(allocator: manager RPC vs SDK CAS) before M0/M2. The design reuses the two
hard existing pieces (lease lib, namespace core) and the net new work is a
type-decoupling refactor + a PyO3 surface + a facade — no new distributed-systems
invention. Ship M0 first regardless (it fixes a latent multi-mount hazard).
