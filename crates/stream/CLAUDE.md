# autumn-stream Crate Guide

## Purpose

The stream layer: append-only replicated extents on extent-nodes, the storage
substrate under the partition layer. Four components (all exported from
`src/lib.rs`):

1. **`ExtentNode`** (`extent_node.rs`) — server-side storage daemon holding
   extents on local disk; implements ExtentService over autumn-rpc (custom
   binary protocol on compio).
2. **`StreamClient`** (`client.rs`) — client library used by `PartitionServer`
   to read/write streams.
3. **`erasure`** (`erasure.rs`) — Reed-Solomon EC codec (`ec_encode`,
   `ec_decode`, `ec_reconstruct_shard`, `ec_encode_stripe`) over the
   `reed-solomon-erasure` crate.
4. **`ConnPool`** (`conn_pool.rs`) — per-`SocketAddr` connection pool.

`extent_rpc` (the wire codec for all ExtentService RPCs) lives in
`autumn-rpc` (`crates/rpc/src/extent_rpc.rs`) so all three wire schemas share
one home; re-exported here (`pub use autumn_rpc::extent_rpc`) so
`crate::extent_rpc::*` / `autumn_stream::extent_rpc::*` paths resolve
unchanged. There is no extent-node WAL — SSDs make sequential WAL fsync no
faster than extent-file fsync; the data files are the journal.

---

## ExtentNode — Server Side

### Data Model (multi-disk)

An `ExtentNode` manages **multiple disk directories**, each a `DiskFS`
(disk_id, base_dir, online flag). File I/O uses `compio::fs::File` directly.

Extents use the hashed layout: `{data_dir}/{hash:02x}/extent-{id}.dat` +
`.meta`, where `hash = crc32c(extent_id_le_bytes) & 0xFF`. Hash subdirs are
created on-demand (matches the 256 subdirs `autumn-op format` creates).

Each extent file pair:
- `extent-{id}.dat` — raw data (append-only during active use)
- `extent-{id}.meta` — 40-byte binary sidecar (+ CRC trailer in V1):

| Bytes | Field |
|-------|-------|
| 0–7 | Magic: `EXTMETA\0` (V0) or `EXTMETA\x01` (V1) |
| 8–15 | `extent_id` (le u64) |
| 16–23 | `sealed_length` (le u64) |
| 24–31 | `eversion` (le u64) |
| 32–39 | `owner_epoch` (le i64) |
| 40–43 | CRC32C of bytes 0–39 (V1 only) |

`ExtentEntry` stores `disk_id` for path resolution. `choose_disk()` returns the
first online **allocatable** disk. `df()` returns real `statvfs` stats per disk.

**Usage** (production formats disks + registers node identity, then the node
self-registers its live location via `--advertise`):
```bash
autumn-op --manager ... format /disk1 /disk2
autumn-extent-node --data /disk1,/disk2 --manager ... --advertise host:9101 --port 9101
```
Single-disk (tests): `autumn-extent-node --data /tmp/data --disk-id 1 --manager ...`

In-memory the node holds `Rc<DashMap<u64, Rc<ExtentEntry>>>` (single-threaded
compio, no `Arc`/`Mutex`):

```rust
struct ExtentEntry {
    file: RefCell<Option<Rc<CompioFile>>>, // None = fd evicted (SEALED only)
    extent_id: u64,                // for re-open on cache miss
    len: AtomicU64,                // current byte length
    eversion: AtomicU64,           // bumped on seal or eversion change; defaults to 1 on alloc
    sealed_length: AtomicU64,      // 0 = active; >0 = sealed at this length
    avali: AtomicU32,              // availability flag (non-zero = sealed)
    owner_epoch: AtomicI64,        // per-extent write fence, most recent owner revision seen
    disk_id: u64,                  // immutable after creation
    // + durable_owner_revision, corrupt_meta (AtomicBool), durability
    //   watermarks (last_synced / pending_fsync), owner mailbox (per-extent
    //   single-writer append task)
}
```

Appends are serialized by the single-threaded compio runtime (sequential
processing in `handle_connection`); there is no `write_lock`.

**No `unsafe` in the file path.** All borrows go through `entry.file_rc()` /
`resident_file()` / `extent_file()`, which clone the inner `Rc<CompioFile>`
under a brief `RefCell::borrow()`; the clone is held by the I/O future across
`.await`, and the borrow is released before the first `.await`. The file-replace
path (`commit_shard_local`) uses `entry.replace_file(new)` (safe
`RefCell::borrow_mut()` + `Rc::replace`); the old `Rc` drops only when the last
concurrent reader releases its clone, so the fd can't dangle. `ec_conversion_
locks` remains only as higher-level serialisation against concurrent EC
dispatches on the staging path (not for memory safety).

### Shard files (`extent-{id}.shard{i}`) — the EN as a shard holder

An EC shard can live in its own ADDITIVE file beside `.dat`, so a conversion
never modifies or replaces the replica it is derived from. The index is in the
NAME, so a shard staged for one index can never be *served* as another.

- **On-disk shapes.** `.dat` only (a full replica); `.dat` + `.shard{i}`
  (converted, cleanup pending); `.shard{i}` only (cleanup done); shard-in-`.dat`
  (the pre-CoW scheme, which renamed over `.dat` — `payload_location = InDat`).
- **`ExtentEntry` tracks what exists**: `has_dat` plus `shard_files: index ->
  length`. Two indices at once is a LEGAL transient (two attempts, or a parity
  slot plus a data slot after a reassignment) — the extent's PUBLISHED layout,
  not this map, decides which is authoritative. Lengths live in the map so
  "which files exist" and "what they cost" cannot drift.
- **Startup discovery** (`discover_shard_files`, second pass of `load_extents`)
  attaches shard files to their extent, and builds an entry from `.meta` alone
  for a shard-only holder. **A file nothing scans survives every cleanup and
  then vanishes from the system at the next restart** — and a shard-only extent
  that looks absent to the manager is how a rebuilt copy becomes a blocking
  orphan. A shard-only entry carries NO fd and must never create `.dat`
  (`extent_file` opens without `create` for exactly this reason); its `len` is
  the shard's length, which is NOT the extent's `sealed_length`. No parseable
  `.meta` beside a real payload file is META-FAILCLOSED → quarantine.
- **`remove_extent_files` unlinks every `.shard{i}` found on disk**, and
  **`df`'s `extent_bytes` adds `shard_bytes()` to `len`** — a node mid-conversion
  legitimately holds both, and counting one under-reports the footprint that
  cluster-df and the `--min-alloc-free-bytes` gate read.
- Reads resolve their file through `payload_file(entry, PayloadRef)`: `.dat` via
  the fd cache, a shard opened per use (read-only after staging, and keeping it
  out of `FdLru` preserves that cache's one-fd-per-extent accounting).

### Bounded fd cache for SEALED extents (`FdLru`)

Open/active extents keep their fd PINNED (`file = Some`). SEALED idle extents
(the long tail) have their fds cached in a per-shard bounded LRU and evicted
under cap. `load_extents` drops a sealed extent's fd after reading `len`, so
startup fd peak is not O(all extents). `RLIMIT_NOFILE` is raised to 65535 at
startup (same as PS).

Two accessors:
- **`resident_file() -> Option<Rc>`** (sync): the write/durability path
  (`append_burst_frames` run by the per-extent owner, `truncate_to_commit`)
  resolves its fd ONCE in the prologue (while the seal re-checks just proved the
  extent open), HOLDS the `Rc` for the whole op, and treats `None` as
  "concurrently sealed" → clean `CODE_PRECONDITION`. `append_burst_frames`
  MOVES the `Rc` into the boxed I/O future (NOT lazily at first poll).
- **`extent_file(entry) -> Result<Rc>`** (async): every read / sealed-extent
  background op (read futures, copy, convert, re_avali, recovery, meta persist)
  — resident fast-path (LRU-touch if sealed) or **re-open on miss**. The
  returned `Rc` pins the fd across `.await`.

**Eviction — `fd_evictable() = sealed && pending_fsync<=last_synced &&
Rc::strong_count(fd)==1`.** The `strong_count==1` guard is load-bearing: the
cache being the SOLE holder means an in-flight write/read/fsync that
resolved-and-holds its `Rc` can never have its fd yanked mid-op. `FdLru` is
`BTreeMap<seq,id>` + `HashMap<id,seq>` (O(log n)); `evict_over_cap` scans
LRU→MRU evicting the first evictable victim and KEEPS non-evictable ones
tracked (so their resident fd stays cap-accounted). Per-shard cap =
`min(--fd-cache-cap, 60000/shard_count)` floored at 64 (`RLIMIT_NOFILE` is
process-wide but each shard is a separate `ExtentNode`, so the clamp keeps
N×cap under 65535). Default cap 4096; flag `--fd-cache-cap`.

**Invariants:** (1) any file-access on a possibly-SEALED extent uses
`extent_file()` (open-on-miss); (2) the write/durability path resolves once via
`resident_file()`, HOLDS the `Rc` for the whole op, and rejects `None` — NEVER
`.expect()`/panic on it.

### Connection handling & batch optimization (true SQ/CQ)

`handle_connection` is ONE compio task per TCP connection running a **true
SQ/CQ** loop: a persistent read future (SQ) and an inline `FuturesUnordered` of
in-flight batch I/O futures (CQ) are polled concurrently via
`futures::future::select`, so completions stream out to the client as soon as
they happen — not gated on a burst boundary.

```
┌─ ConnTask (single task, true SQ/CQ) ────────────────────────────┐
│  SQ — persistent read future (Option<LocalBoxFuture<ReadBurst>>) │
│    owns OwnedReadHalf + 512 KiB buf across iterations;           │
│    NEVER dropped mid-flight (io_uring SQE stability)             │
│  CQ — FuturesUnordered<Pin<Box<dyn Future<Vec<Bytes>>>>>         │
│    cap = AUTUMN_EXTENT_INFLIGHT_CAP (default 64)                 │
│    holds in-flight append/read batch + control-rpc futures       │
│  Loop:                                                           │
│    1. drain ready completions via `.next().now_or_never()`       │
│    2. flush tx_bufs with ONE `write_vectored_all` syscall        │
│    3. branch on (n_inflight, at_cap):                            │
│       n_inflight == 0 → await read alone                         │
│       at_cap          → await completion alone (back-pressure)   │
│       n_inflight == 1 → await completion alone (fast path)       │
│       n_inflight > 1  → select(read, inflight.next())            │
└─────────────────────────────────────────────────────────────────┘
```

1. **MSG_APPEND batch** — consecutive append frames grouped by extent_id →
   ONE append future issuing ONE `write_vectored_at` (pwritev), returning N
   already-encoded response frames.
2. **MSG_READ_BYTES batch** — same grouping; preads run sequentially inside.
3. **Control RPCs** (ALLOC, DF, RECOVERY, …) — each becomes one future on the
   same FU; responses fold into the same tx_bufs flush.
4. **Cross-extent concurrency** — batches for N different extents sit in FU
   simultaneously; the first completion's bytes flush immediately at the next
   loop top, not waiting for the slowest in-flight op.

**Extent-len reservation**: `build_append_future` stores `extent.len =
total_end` BEFORE returning the I/O future into FU, so overlapping same-extent
submits compute non-overlapping `file_start` values.

**Single-inflight fast path**: at client depth against one extent, every cycle
produces ONE batch future and the client waits on responses before sending
more, so no new reads arrive while the pwritev is in flight. The `n_inflight ==
1` branch awaits the completion alone (racing the always-pending read costs
~5-10 µs/cycle for no benefit). At `n_inflight > 1` (multi-extent burst or
mixed op) the select-based race kicks in and responses stream as each
completion lands (`cq_flushes_fast_ops_while_slow_op_runs` guards this).

### Chained append (MSG_APPEND_CHAIN, default OFF)

Large appends (>= `set_append_chain_min_bytes`, 0=off=default) ship ONE wire
copy to replica[0], which pipelines down the chain; the single ack means every
hop wrote (all-replica-ACK preserved; per-hop fencing/commit-truncation
unchanged). EN keeps a per-downstream-addr forwarder task; the conn loop
enqueues forwards UNBOUNDED in arrival order (a blocking submit there would
stall the whole `handle_connection` under backlog), the forwarder submits
sequentially (per-extent order ⊆ per-addr order) and hands back the response
receiver so downstream RTTs overlap. LIMITATION: per-append store-and-forward
stacks hop latencies; the win (writer egress 3x→1x) only exists where the
writer NIC is the bottleneck (cross-host). Keep OFF on loopback.

### Per-extent owner (write path) — appends serialized by one task

All appends to an extent are MESSAGES (`ExtentMsg::Append`) to a single
per-extent **owner task** (`send_to_owner` → `owner_loop`), lazily spawned on the
first message and exiting when its mailbox drains (dropping its `Rc<ExtentEntry>`
so a sealed/idle extent is never pinned past fd eviction; a later message
respawns it — lost-wake-free by the single-thread enqueue/exit-recheck argument).
This makes the write path single-writer BY CONSTRUCTION — the old fsync coalescer
+ its loop are DELETED (note 24). The ps-conn task stays the RPC engine: it builds
the message + a `resp` oneshot, sends it to the owner, and batches the returned
encoded frame into its existing vectored write.

`owner_loop` drains its mailbox in bursts and runs each burst through
`append_burst_frames` (the renamed `build_append_future`; still two-phase — a
synchronous prologue reserves `extent.len`, the returned future does pwrite + one
inline `sync_data`, a harmless vestige under the sole writer).

INVARIANT (burst-splitter — load-bearing for the owner_epoch fence): the mailbox
is a CROSS-CONNECTION aggregation point (a per-connection append batch never
was), so ONE drain can merge appends from two writers — a fenced zombie at epoch
E and the post-takeover owner at E+1, or a retry replayed on a fresh connection.
`append_burst_frames` validates `owner_epoch` + `commit` from its FIRST slot ONLY.
So `owner_loop` SPLITS each drain into homogeneous runs before group-committing: a
new run starts whenever `owner_epoch` changes OR commit-contiguity breaks
(`commit != prev.commit + prev.payload.len()`); each run's first slot then gets
the full prologue fence/commit check against the LIVE `extent.len`/`owner_epoch`,
and runs execute sequentially. Single-writer pipelining is single-epoch +
contiguous ⇒ ONE run ⇒ zero hot-path cost. Without the split, `[new, zombie]`
would ACK the zombie past the fence (acked-data loss) and `[zombie, new]` would
reject the rightful owner. Tests: `owner_burst_splitter_tests`.

### Append protocol (eversion → seal → fencing → commit truncation → write)

```
Append(AppendReq via autumn-rpc binary frame):
  1. Decode (extent_id, eversion, commit, revision, must_sync, payload).
  2. Eversion check:
       client eversion > local → fetch ExtentInfo from manager, apply if sealed
       client eversion < local → reject (PRECONDITION_FAILED)
  3. Sealed check: reject if sealed_length > 0 or avali > 0.
  4. Revision fencing:
       header.revision < owner_epoch → reject (CODE_LOCKED_BY_OTHER, stale owner)
       header.revision > owner_epoch → update owner_epoch, persist meta durably (note 23)
  5. Commit reconciliation:
       local len < header.commit → reject (data loss on our side)
       local len > header.commit:
         - first confirm with manager the extent is NOT sealed (if sealed →
           apply meta + reject CODE_PRECONDITION)
         - TRUNCATE to header.commit via truncate_to_commit: set_len then
           sync_data BEFORE updating extent.len. The fsync is load-bearing:
           without it, post-crash the file size can be observed at the
           pre-truncate length, letting the next commit_length probe report a
           wrong consensus and diverge replicas at the same offset.
         - RE-CHECK sealed_length/avali AFTER the truncate await, on BOTH the
           batched (build_append_future) and non-batched (handle_append) paths:
           a concurrent apply_extent_meta_durable may have landed a fresh seal
           DURING the truncate I/O; writing past the new sealed_length corrupts
           subsequent reads. Fire CODE_PRECONDITION; client retries via the
           standard soft-error path.
  6. Write payload (Direct path, serialized by the per-extent OWNER — see
     "Per-extent owner (write path)"). Appends are messages to ONE owner task
     per extent, so the write is single-writer by construction:
       - write_vectored_all_at(start, payload)   (see note 25a: MUST be *_all)
       - ONE sync_data per drained burst: pending_fsync.store(end) BEFORE the
         fsync, last_synced.store(end) AFTER — the two watermarks are all that
         remains of the old Coalescer struct. must_sync no longer gates a
         syscall (every burst fsyncs unconditionally); the flag stays on the
         wire for back-compat.
       - fsync error → fail the WHOLE burst (no len advance) + mark_disk_error.
  7. Advance extent.len (AFTER the burst's fsync).
  8. Return (offset=start, end=start+payload_len) per slot.
```

Returning `end=N` means all data in `0..N` is written. Step 5 (commit-based
truncation) is the consistency key: the data files themselves are the journal,
replacing a WAL.

### Commit protocol — all-replica, NO quorum

`StreamClient` computes `commit = min(commit_length on ALL replicas)` before
each append. Any replica that got ahead (partially acknowledged data before a
crash) is truncated back to the consensus point on the next append. Per-node
durability comes from the per-extent owner's one-sync_data-per-burst (note 24).

**This is a WAS stream layer: the append path is all-replica-ACK**
(`apply_completion` acks only when every replica wrote), so committed length
MUST be derived from ALL replicas, not a quorum subset. A subset `min` can sit
BELOW the acked length (include a short/catching-up replica → the next append's
`header.commit` truncates acked data on up-to-date replicas → silent loss) or
ABOVE it (exclude a member → keep un-acked data). **There must be NO quorum on
the commit path.**

- `current_commit` requires ALL replicas to respond (else `Err`);
  `ensure_tail_initialised` propagates that `Err` — seeding cursor 0 would make
  the next append's `header.commit=0` truncate EVERY replica to 0. `Ok(0)`
  (genuinely empty extent) still seeds nothing.
- `await_extent_synced_to` (the flush durability barrier) requires **ALL**
  replicas synced past `vp_offset` (was quorum-min). On a healthy cluster this
  is already satisfied because the append acked all-replicas; worst case it
  waits one coalesce window. Flush is background → invisible to clients.
- **Manager-side seal/commit** (`handle_stream_alloc_extent` /
  `handle_check_commit_length`) take `min` over the REACHABLE COMMITTED members
  only (catching-up = in-flight Recovery, excluded), requiring only `floor` of
  them to respond — **lenient seal-over-reachable, NOT strict-all-committed** (an
  EN can be down at seal time; the all-replica APPEND is the guarantee, not a
  strict seal). Do NOT revert to strict. See `crates/manager/CLAUDE.md` note 28.
- **Truncation of beyond-commit bytes stays correct** — those are un-acked and
  must be removed; do not add a floor that keeps them.

**Scope**: the all-replica rule targets the WRITE/SEAL/commit-truncation path
(the silent-loss vector). `commit_length_for_extent` (the READ-path helper
resolving a `length=0` "to-end" read on an *open* extent) is intentionally
quorum-min: it neither seals nor truncates, so its worst case is a short read
(surfaced as an error), and reads should tolerate a replica being down.

**Flush-time durability barrier**: every Put pays exactly one coalesce window
(1–5 ms). The durability wait lives in `flush_one_imm`, which calls
`await_log_synced_to(vp_extent_id, vp_offset)` (ALL replicas report
`last_synced >= vp_offset`) via `MSG_SYNCED_LENGTH` BEFORE uploading the SST.

### Recovery (`require_recovery` RPC)

Triggered by the manager when a replica node fails. **The request is a STANDING
INSTRUCTION, re-sent every tick from the manager's durable marker, so every
answer must be idempotent — a permanent refusal is a permanent wedge:**
1. Validate the manager endpoint is configured, then answer idempotently:
   - **already recovering this extent** → `CODE_OK` ("I am already doing exactly
     this" is the request being satisfied — the same contract
     `handle_convert_to_ec` uses). A `CODE_PRECONDITION` here would make the
     manager drain the marker of a HEALTHY in-flight recovery and go hunting for
     another target.
   - **a local copy exists** → `try_adopt_completed_recovery` returns a THREE-state
     `LocalCopyVerdict` (never two — the action for Incomplete is destructive, so
     "cannot tell" must never collapse into it):
     `Complete` (sealed, `eversion` equal, `len >= sealed_length` vs the
     manager's authoritative view) → re-push `RecoveryTaskDone` and `CODE_OK`
     (the completion report was lost, not the data);
     `Incomplete` (authoritative view obtained and the copy falls short) →
     **discard it and rebuild** — `run_recovery_task` persists `.meta` LAST, so a
     crash mid-copy leaves a partial `.dat` that reloads as an ordinary open
     extent, and refusing on it poisons that (node, extent) pair FOREVER (the
     orphan reconcile won't reap it either — the extent is alive; only its
     MEMBERSHIP says the copy is garbage). Safe because the manager dispatches
     recovery only to a NON-member, so the stub is referenced by no VP, no SST
     and no checkpoint;
     `Unknown` (manager unreachable, or EC'd / still-open / quarantined — shapes
     this comparison cannot judge) → refuse `CODE_PRECONDITION` and let the
     manager retry. **Never destroy a copy of unknown completeness.**
2. Spawn background `run_recovery_task` **with retry** (up to 10 attempts, 10 s
   backoff): fetch `ExtentInfo` for replica addresses; stream the full extent
   chunk-by-chunk from a healthy peer (`stream_extent_from_sources`); truncate
   local to 0, write, sync; update atomics and persist `.meta` durably.
3. On completion push `RecoveryTaskStatus` to the `recovery_done` channel; the
   periodic `df` RPC drains it so the manager learns recovery finished.
4. On max retries exhausted, remove from `recovery_inflight`; the manager
   re-dispatches on its next loop.

`run_recovery_task` performs a **verify-after-fetch**: after `sync_all` and
before writeback it re-reads local `eversion` and errors (→ retry) if it
advanced during the fetch; writeback is gated on the fetched length matching the
manager-reported `sealed_length` (a mismatch means a concurrent seal landed).

### Re-Avali (`re_avali` RPC)

Brings a **sealed, replicated** extent's lagging replica up to date (e.g. after
a node comes back):
- If `ec_converted == true` → return OK immediately. RE_AVALI is a
  replicated-extent-only repair primitive: on an EC'd extent the local shard
  size = `sealed_length / K`, so the `local_len >= sealed_length` check never
  short-circuits and the handler would fetch+overwrite raw bytes over the shard,
  corrupting EC. Missing-shard repair on an EC'd extent routes through
  `EXT_MSG_REQUIRE_RECOVERY` → `run_ec_recovery_payload`. The OK also auto-heals
  buggy `avali` etcd values (the manager ORs in the parity-slot bit on OK).
- If local data >= `sealed_length` → already up to date, return OK.
- Otherwise **temp-then-publish** via `peer_copy_full_extent_to_dat` (acquires an
  `acquire_recovery()` permit, same shared pool as recovery).

  **Never `stream_extent_from_sources` here.** That helper truncates the
  destination to 0 before each source attempt, which is correct when the
  destination has nothing to lose (a fresh or provably-incomplete recovery
  target) and DESTRUCTIVE here, where the destination is an existing copy: if
  no source can deliver, the replica ends up holding less than it started with
  (reproduced 4096 → 0 in `crates/manager/tests/re_avali_no_destroy.rs`).

  Those bytes are worth protecting even though `avali == 0` is what aimed
  repair at this replica. **`avali == 0` does not mean "lagging"** — a member
  merely UNREACHABLE at seal time has its bit left unset (manager CLAUDE.md,
  seal-over-reachable) while possibly holding the LONGEST copy in the cluster,
  and `stream_extent_from_sources` picks its sources from the member list
  WITHOUT consulting `avali`, so this file is exactly what another node's
  recovery would rebuild from. The absence of reconcile-down in
  `peer_copy_full_extent_to_dat` is deliberate on this path: adopting a SHORTER
  peer copy over a longer local one is the trade re_avali must not make.

### Heartbeat & Df

- `heartbeat`: streams a "beat" every second (keep-alive for the manager).
- `df`: returns per-disk `statvfs` + `extent_bytes` (EN self-reported per-disk
  extent footprint, summed by `disk_id`; the manager aggregates it into cluster
  `physical_used`), and drains BOTH `recovery_done` (completed recoveries) and
  `ec_done` (completed EC conversions). **`df` is the completion channel for
  every long-running EN task** — both are at-most-once (`mem::take`), and both
  converge after a lost report via manager re-dispatch + the EN's adopt guard.
- `handle_df` **ECHOES the EN's own identity** (`DfResp.node_uuid` /
  `advertise_addr` / `shard_ports`, from `self.registration`, empty when
  unset). The manager's `node_health_loop` uses the echo to self-heal
  stored-location drift and detect pod-IP reuse (a different process at a stored
  address → uuid mismatch → refuse to heal + fail the df for liveness). Only
  shard 0 is dialed by the manager df, so only shard 0 carries a non-empty
  `registration`.

### EN ports are FUNDAMENTALLY static — `serve_with_control` is fail-stop on bind conflict

**Do NOT add dynamic port fallback.** The address (`addr` + `shard_ports[]`) is
stamped into etcd once by `autumn-op format` and held for the node's lifetime;
EN startup just opens the configured port, with no per-session re-register. A
dynamic fallback would silently change the bound port while the manager still
routes to the old one → every PS / sibling / manager RPC black-holes. (PS is the
opposite — PS ports are dynamic, one per partition, broadcast every open, so PS
DOES have monotonic-next fallback.)

EN port conflicts are OPERATIONAL HYGIENE, not a runtime mechanism bug: another
tenant squatting → operator picks a different `--port`; own old process not yet
released → the existing 10 × 200 ms retry budget in `accept_loop_on` covers it;
port inside `ip_local_port_range` → operator picks a port below 32768.
Fail-stop with `bind data listener <addr>: <io error>` is correct; the operator
fixes the config and restarts.

### Delete extent (`MSG_DELETE_EXTENT = 11`)

Idempotent unlink of the physical `.dat` + `.meta` after the manager confirms
`refs == 0`. Sent fire-and-forget once per replica by the manager's
`extent_delete_loop`.

```
handle_delete_extent(extent_id, node_uuid):
  1. identity: if both uuids are non-empty and differ → CODE_LOCKED_BY_OTHER.
  2. shard ownership: if !owns_extent → forward to sibling shard.
  3. if recovery_inflight.contains_key(id) → return CODE_PRECONDITION
     (manager retries; once recovery clears, next retry succeeds).
  4. extents.remove(id) — subsequent appends fail fast NotFound.
  5. DiskFS::remove_extent_files(id): remove_file(.dat) + remove_file(.meta);
     NotFound → Ok(()) (idempotent, retries are safe).
  6. Returns CODE_OK | CODE_LOCKED_BY_OTHER | CODE_PRECONDITION | CODE_ERROR.
```

**Identity check** (step 1): the request names WHICH node must execute it, and
a mismatch is refused before anything is touched. Extent ids are unique only
within a cluster, while the manager's delete retries are persisted and retried
for up to an hour — outliving the address's ownership. Cluster A torn down with
retries outstanding, cluster B up on the same host and ports (shared-host port
bases, pod-IP reuse), and A's retry unlinks B's live extent with the matching
id. An empty uuid on either side means "unspecified" and skips the check, the
same convention `classify_df_echo` uses for `df`. The manager carries the uuid
in each `DeleteTarget` alongside the address, so a persisted retry keeps its
target identity across a leader change.

**Recovery-vs-delete mutual exclusion** (step 2) prevents two data-loss paths:
(a) **resurrection** — `run_recovery_task`'s `ensure_extent(create:true)` after
an unlink silently recreates an orphan; (b) **write-to-unlinked-inode** —
recovery holding an `Rc<ExtentEntry>` whose fd survives the unlink writes to a
doomed inode. The manager-side guards are primary; this EN check is
belt-and-braces for leader-failover where `pending_extent_deletes` is lost but
`recovery_inflight` survives on the EN.

### Startup + periodic orphan reconcile

`ExtentNode::new` spawns `spawn_reconcile_orphans_loop()` after
`load_extents()`: runs immediately, then every 5 minutes. Each iteration ships
every locally-loaded (shard-owned) `extent_id` to the manager via
`MSG_RECONCILE_EXTENTS = 0x31`. The answer is **file-granular**: `garbage`
(extents this node is not a member of — delete everything) plus `placements`
(`extent_id, payload_location, shard_index` for each extent it IS a member of).
From a placement the node derives the ONE payload file it should hold; anything
else it holds for that extent is residue. That single rule covers both halves of
CoW-conversion cleanup — dropping the redundant pre-conversion `.dat` after the
flip, and dropping an abandoned attempt's shards when the layout still says
`InDat` — with no second mechanism and no intent marker (a crash mid-cleanup is
resolved by startup discovery re-deriving what is on disk).

**An extent in NEITHER list has no verdict and is left strictly alone.** The
manager omits any extent with an in-flight ledger op, because its file set is
mid-change: a participant staging a shard for a not-yet-flipped conversion holds
a file the current layout does not name, and the node-side `ec_convert_inflight`
guard only sees conversions THIS node coordinates.

Deleting a payload file is destructive, so `apply_placements` gates it three
ways: **the keeper must already be here** (`.dat` is dropped only once the named
shard is actually held — otherwise a placement arriving before staging finishes
would delete the only copy), **no in-flight op**, and **only the manager
decides** (a node holding a complete shard beside a complete `.dat` cannot tell
which one the cluster is pointed at). The `.dat` transition stops serving
(`has_dat=false`, `len=0`, fd dropped, `FdLru::forget`) BEFORE the unlink, so no
read resolves an fd to a file that is about to vanish.

The rest of the sweep returns the subset **this node is not a MEMBER of** (`replicates ++ parity`), not merely the subset it has forgotten —
crash residue from a died-mid-copy recovery belongs to an extent that is very
much alive, so a "forgotten extent" predicate can never see it. The node unlinks
those via `remove_extent_files`, **skipping any extent with a live
`recovery_inflight` / `ec_convert_inflight`** — a recovery target is by
construction not yet a member, so without that guard the sweep would delete a
recovery out from under itself (`handle_delete_extent` has always refused for the
same reason; this path used to bypass it safely only because the old list could
never name a recovery target). The manager side counts
`NON_MEMBER_ROUNDS_BEFORE_GC = 3` consecutive rounds before listing a non-member,
because the membership view is momentarily wrong in normal operation (an
`apply_recovery_done` slot swap, a settling leader) and deleting real data on a
transient is far worse than holding residue for a few more minutes. A single
iteration handles both cold-start (a failed first attempt recovers on the next
tick) and steady-state. Per-sweep failures log at WARN; the loop continues. It
is a backstop for: exhausted `MSG_DELETE_EXTENT` retries, manager restart losing
`pending_extent_deletes`, EC-conversion leftovers, crashed-recovery residue, any
path that drops refs to 0 unilaterally. Generous cadence is fine for a backstop; if a node scales to 10k+
extents, switch to chunked rotation.

### Concurrency control: `ConcurrencyController`

Cross-extent concurrency cap for the two memory-heavy background paths on each
shard: `handle_convert_to_ec` and `run_recovery_task`.

```rust
pub struct ConcurrencyController {
    ec_convert_max: usize,
    recovery_max: usize,
    ec_convert_inflight: Cell<usize>,   // Cell, not Atomic: single-threaded shard
    recovery_inflight:   Cell<usize>,
}
// acquire_ec_convert() -> EcConvertPermit ; acquire_recovery() -> RecoveryPermit
// Permits decrement their counter on Drop; 50 ms backoff polling on contention.
```

One `Rc<ConcurrencyController>` per shard; both call sites acquire from it. The
two counters are independent (saturating EC convert doesn't block recovery).

**Why not the per-extent locks alone?** `ec_conversion_locks` and
`recovery_inflight` only serialise the SAME `extent_id`; both allow unbounded
cross-extent fanout (one manager tick finding 8 extents spawns 8 detached tasks,
each holding ~`payload × 2` memory). The controller caps that.

**Keep `recovery_max` aligned with the manager's
`RecoveryRateLimiter.max_per_target`** (both default 2). Manager throttles
DISPATCH (network fan-out); this caps EXECUTION (RAM). Defense-in-depth in
different processes — cannot be merged. If the manager's per-target cap exceeds
`recovery_max`, surplus dispatches just block in `acquire_recovery()`. See
`crates/manager/CLAUDE.md` note 27.

**No bytes/s rate cap on EN** (deliberate — different resource shape from PS):

| Operation | Dominates | Why concurrency cap suffices |
|-----------|-----------|------------------------------|
| `handle_convert_to_ec` | CPU (`spawn_blocking(ec_encode)`) + network fanout | each stripe holds `(K+M) × stripe`; cap=1 keeps peak at one encode |
| `run_recovery_task` | network (CopyExtent) + disk write+sync | chunked I/O caps per-syscall at 256 MiB; cap=2 keeps peak at ~one chunk each |

Both are bounded in time (seconds–minutes) and self-throttled by 3-replica
fanout. PS is different (foreground writes thousands/sec at small batch), which
is where bytes/s + iops limits matter.

### Configuration

| CLI flag | Env (shell→flag) | Default | Range |
|----------|------------------|---------|-------|
| `--ec-convert-parallelism` | `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM` | 1 | [1, 16] |
| `--recovery-parallelism` | `AUTUMN_EXTENT_RECOVERY_PARALLELISM` | 2 | [1, 16] |
| `--ec-stripe-bytes` | `AUTUMN_EXTENT_EC_STRIPE_BYTES` (test override) | 64 MiB | [1 MiB, 1 GiB] |
| `--fd-cache-cap` | `AUTUMN_EXTENT_FD_CACHE_CAP` | 4096 | floored 64, clamped by shard count |

`--ec-stripe-bytes` (process-global, `set_ec_encode_stripe_bytes`, OnceLock
first-call-wins; precedence flag > env > default) is the chunked EC-convert
stripe size. Peak EC-convert RAM = `(K+M) × stripe`; bigger = fewer WriteShard
RPCs / syncs at higher RAM. Max 1 GiB keeps a single stripe's WriteShard under
the frame `payload_len: u32` ceiling.

---

## StreamClient — Client Side

Used by `PartitionServer` and tests. Holds autumn-rpc connections to extent
nodes via `ConnPool`.

### Connection & ownership

```rust
StreamClient::connect(manager_endpoint, owner_key, max_extent_size, pool)
    -> Rc<StreamClient>           // Rc::new_cyclic
```
- `manager_endpoint` supports **comma-separated** addresses for multi-manager
  HA (`"h1:9001,h2:9001,h3:9001"`). Tries each to `acquire_owner_lock`, skipping
  `NotLeader`. `self.manager_addr()` returns the current leader.
- **Retry classification** (`retry_manager_call`, code-classified via typed
  `ManagerError{code, ctx, message}` — see note 30):
  - transport error / `CODE_ERROR` → `rotate_manager()` + retry;
  - `CODE_NOT_LEADER` → retry, rotated by `note_manager_code` (the loop must not
    double-rotate);
  - TRANSIENT `alloc_extent` conflict Preconditions (`is_transient_conflict`:
    "retry with a fresh snapshot" / "defer … until it completes") → retry
    WITHOUT rotating (leader-side self-heal, stay on the same leader);
  - DETERMINISTIC codes (NotFound / InvalidArgument / owner-epoch fence /
    deterministic business-rule Preconditions / unknown) → FAIL FAST, unwrapped.
- `owner_key` should be unique per logical writer (`"ps/{ps_id}/partition/{part_id}"`).
- Constructors return `Rc<Self>` so per-stream workers can hold
  `Weak<StreamClient>` for the exit-removal guard without an Rc cycle. Public
  API methods take `&self`.

### Append data flow (public API drives retry)

```
append*(stream_id, payload, must_sync):
  1. stream_worker_sender(stream_id): look up or lazily spawn the per-stream
     compio task (returns cloned mpsc::Sender<StreamSubmitMsg>, cap=256).
  2. ensure_tail_initialised(stream_id): first caller (under per-stream init
     mutex) loads tail from manager + current_commit on ALL replicas, then sends
     ResetTail + SeedCursor. If the manager-reported tail is already SEALED,
     allocate a fresh extent immediately instead of seeding the sealed tail —
     load-bearing after split/duplication (a child may inherit a sealed tail;
     waiting for append-time failure wedges descendant compaction on
     LockedByOther / overlap-clearing).
  3. Retry loop (MAX_ALLOC_PER_APPEND=3):
     a. Send Append (parks on the bounded channel under overload). Await ack_rx.
     b. Ok → if result.end >= max_extent_size, alloc_new_extent + ResetTail
        (preemptive roll). Return.
     c. Err "not found on replica" → alloc_new_extent + ResetTail. Retry.
     d. Err "LockedByOther" → propagate immediately (PS owner self-evicts).
     e. Err soft (retry <= 2) → sleep 100ms, reload tail, ResetTail. Retry.
     f. Err hard → alloc_new_extent + ResetTail. Retry.
```

### Per-stream worker (single-owner actor)

```
┌─ stream_worker_loop (ONE compio task per stream_id) ─────────────┐
│  OWNS: StreamAppendState                                         │
│     - tail: Option<StreamTail>                                   │
│     - commit: u32            (contiguous all-replica-acked prefix)│
│     - lease_cursor: u32                                          │
│     - pending_acks: BTreeMap<offset, end>                        │
│     - in_flight: u32                                             │
│     - poisoned: bool ; sealing: bool ; failure_floor             │
│  OWNS: inflight: FuturesUnordered<InflightFut>                   │
│     cap = AUTUMN_STREAM_INFLIGHT_CAP (default 32)                │
│  RECV: submit_rx: mpsc::Receiver<StreamSubmitMsg>                │
│                                                                 │
│  SQ (launch_append):                                            │
│     - lease offset range; header.commit = offset                │
│     - fire pool.send_vectored to each replica IN PARALLEL via    │
│       join_all over the 3 per-replica futures (each replica's    │
│       writer_task is single-writer → per-replica TCP byte order  │
│       = lease order; inter-replica fanout order is irrelevant)   │
│     - push the 3-replica join future into inflight; no await     │
│  CQ (apply_completion):                                         │
│     - parse 3 frames: success / NotFound / LockedByOther / err   │
│     - success → state.ack (caller ack fires as contiguous prefix │
│       advances, note 25a); error → rewind_or_poison             │
│  Loop:                                                          │
│     1. opportunistic CQ drain via inflight.next().now_or_never() │
│     2. n_inflight==0 → await submit_rx ; at_cap → await inflight │
│        else → select(submit_rx, inflight.next())                │
│  Messages: Append{payload_parts, must_sync, revision, ack_tx} ;  │
│     ResetTail{tail} ; SeedCursor{cursor} ;                       │
│     SealCommit{resp} (note 20) ; Shutdown                        │
└─────────────────────────────────────────────────────────────────┘
```

**No external Mutex**: all state mutations happen inside the worker; the public
API talks to it via bounded mpsc + per-op oneshot.

**Retry is in the public API**, not the worker — the worker is a pure stateful
single-op executor. **Tail invalidation is explicit**: after any
`alloc_new_extent` the public API sends `ResetTail` BEFORE the next Append;
because the retry loop awaits the previous ack before resetting, `in_flight` is
0 at the reset point (no old-extent leases stranded on the new extent).

**SeedCursor** initialises `commit = lease_cursor` to the replica-min
`commit_length` when a resumed stream's tail already has data (without it the
first append would overwrite committed bytes).

### Back-pressure, lifecycle, caching

| Concern | Behaviour |
|---------|-----------|
| Submit mpsc cap | 256 per stream. Parked callers wake as the worker drains. |
| Inflight cap | `AUTUMN_STREAM_INFLIGHT_CAP` (default 32); `at_cap` → CQ-only. |
| Worker lifecycle | Spawned lazily on first `append*`; exits on channel close / `Shutdown` after draining all inflight for a final ack. |
| Worker removal | On exit a `WorkerRemovalGuard` (via `Weak<StreamClient>`) removes the Sender; next `append*` spawns fresh. |
| `LockedByOther` | Propagated immediately; PS owner self-evicts. |

| Cache | Key | Value | Invalidated on |
|-------|-----|-------|----------------|
| `stream_workers` | stream_id | `mpsc::Sender<StreamSubmitMsg>` | Worker exit, drop |
| `stream_init_locks` | stream_id | `Rc<futures::lock::Mutex<bool>>` | Never |
| `nodes_cache` | node_id | address | Replica lookup failure (lazy) |
| `extent_info_cache` | extent_id | `ExtentInfo` | Replica lookup failure |

`nodes_cache` + `extent_info_cache` use `DashMap`; `stream_workers` +
`stream_init_locks` use `RefCell<HashMap>` (single compio thread per caller).

### Other public methods

| Method | Purpose |
|--------|---------|
| `append_batch(stream_id, blocks[], must_sync)` | Concatenate blocks, single append |
| `append_batch_repeated(stream_id, block, count, must_sync)` | Repeat one block N times |
| `read_bytes_from_extent(extent_id, offset, length)` | Read from extent (details below) |
| `read_committed_bytes_from_extent(...)` | Like the plain read but `length` CLAMPED to the committed end (details below) |
| `extent_read_descriptor(extent_id)` | `(eversion, replica addrs)` for a client-direct read; REFUSES EC-converted extents (shard bytes ≠ value); drops Suspected addresses |
| `read_extent_value_direct(pool, addr, …)` (free fn) | One-shot bulk EN read for MSG_GET_REDIRECT holders; short read = Err |
| `read_last_extent_data(stream_id)` | Read last non-empty extent |
| `punch_holes(stream_id, extent_ids[])` | GC: remove extents from a stream |
| `truncate(stream_id, extent_id)` | Remove all extents before extent_id |
| `get_stream_info` / `get_extent_info` | Query manager metadata |
| `multi_modify_split(mid, part, sealed_lengths, tail_extent_ids, timeout)` | Forward partition split to manager; `tail_extent_ids` = the tails the lengths were captured for (manager refuses `split captured tail moved` if any current tail differs) |
| `commit_length_with_tail(stream_id)` | `commit_length` + the tail extent id it was measured on (split capture) |
| `invalidate_stream(stream_id)` | Discard cached worker + init-lock; next append reloads the tail (used after split to prevent appending beyond a sealed tail) |

**A read NAMES its payload file.** `ReadBytesReq` carries a `PayloadRef`
(`payload_location` + `shard_index`) saying WHICH file on the target node to
serve — `extent-{id}.dat` or `extent-{id}.shard{i}`. The EN serves that file or
answers `CODE_PAYLOAD_NOT_HERE`; it never falls back to the other one, because
returning shard bytes where a whole value was asked for (or the reverse) is
silent corruption. The client sources the location from `ExtentInfo` (delivered
beside `MgrExtentInfo` on `ExtentInfoResp`, since that struct is the persisted
etcd value and cannot be widened) and the index from the slot it is reading —
`slot` in the replicated/failover/hedge paths, `shard_idx` in `ec_subrange_read`,
the peer's own `i` in `ec_reconstruct_shard_subrange` / `ec_read_full` /
`run_ec_recovery_payload`. **Every peer is asked for ITS OWN shard**; before the
file was named, EC shard recovery asked each peer for "the extent" and relied on
that peer's `.dat` happening to be its shard.

Two invariants make this safe rather than merely present:
- **`InDat` is ONE identity whatever the slot** (`PayloadRef::for_extent`
  normalises the index away). Otherwise replicated reads of one extent from
  different slots would look like different files.
- **The server's read batching groups by the FILE, not the extent.** One batch
  resolves one fd for every slot in it, so two requests naming different files
  must never share a batch. Both grouping sites (`MSG_READ_BYTES` and
  `MSG_READ_BYTES_BULK`) key on `(extent_id, payload_ref())`.

`read_plan` returns a typed `ReadRefusal` (`EversionStale` | `PayloadNotHere`)
rather than a bare `None`, so the two refusals reach the client as distinct
codes and each self-heals differently.

**Read path** (`read_bytes_from_extent`): the start replica rotates by
`(extent_id, offset)` hash so read IO spreads across all replicas; failover
walks the rest in rotated order. **OPEN-tail extents rotate too** (an open
tail's COMMITTED prefix is on every replica under all-replica-ACK, so a
committed VP read starts from any replica; concentrating hot open-log-tail reads
on replica[0] created a hotspot). Appends stay replica[0]-first
(`launch_append`, unaffected — rotation is in the read-only
`replicated_read_order`).

- **BULK-EXACT invariant**: the EN's bulk read (`build_read_future` bulk branch)
  REJECTS an unservable exact-length range with `CODE_PRECONDITION`, NOT
  `CODE_OK` + a silently short payload (`read_plan`'s clamp is only for the
  non-bulk scanner whose callers handle short reads). So `CODE_OK` on
  `MSG_READ_BYTES_BULK` implies the FULL requested length; no bulk consumer needs a
  defensive length check. **Never re-introduce a clamped-OK on the bulk path.**
- Optional **hedged read** (`set_read_hedge_ms`, default 0=off): if the first
  replica hasn't answered within the window, race the second and take the first
  Ok (eversion-stale still fails fast; loser drop is cancel-safe).
- **Chunked** at `AUTUMN_STREAM_READ_CHUNK_BYTES` (default 256 MiB) so reads
  >2 GiB don't trip the per-syscall pread ceiling (macOS INT_MAX / Linux
  0x7ffff000). EC = parallel per-shard reads with decode. `length=0` resolves
  to-end via `sealed_length` (sealed) or `commit_length_for_extent` (open)
  before chunking.

**`read_committed_bytes_from_extent`**: `length` is CLAMPED to the COMMITTED end
(sealed → `sealed_length`; open tail → min-replica commit probe per call) before
issuing. For chunked scanners (PS WAL replay): a replica legitimately holds
speculative bytes past the committed end (an ahead replica is never truncated
back after seal), so the plain explicit-length read only short-reads at the
SERVING replica's local length — a scanner stopping on `got < want` walks past
the seal and trips `StaleVpOffset` on the next chunk. Sealed + `offset >
sealed_length` still errors loudly (checkpoint-past-seal corruption is never
masked as end-of-scan); `offset == committed end` returns empty = clean stop.

---

## Programming Notes

Present-tense invariants. Numbers are stable identifiers (cross-referenced here
and from other crates' CLAUDE.md); do not renumber.

1. **Always pass the correct `revision`** — 0 or a stale revision → `CODE_LOCKED_BY_OTHER` from EN (propagated as an immediate non-retried error). Set at `connect` time.

2. **Eversion changes on seal** — a manager seal (split, extent rolling) bumps eversion; the next append sees a mismatch, fetches updated `ExtentInfo`, and handles accordingly.

3. **Parallel 3-replica fanout** — `launch_append` fires the 3 per-replica `pool.send_vectored` futures concurrently via `join_all`; one slow replica doesn't serialise the others. Per-replica TCP byte order is preserved because each `RpcClient` runs a single-writer `writer_task`; fanout order across replicas is irrelevant. `apply_completion` enforces that all replicas agree on the file-level `offset/end`.

4. **`must_sync` cost** — no longer a behavioural knob. The per-extent owner does ONE `sync_data` per drained burst (`pending_fsync` before, `last_synced` after), so every append is durable before it ACKs regardless of `must_sync`. `sync_data` is whole-file, so one burst's fsync covers every append in that burst. Clients always pass `must_sync=true` (there is no `--nosync`); the flag is kept on the wire for back-compat only.

5. **StreamClient is always `Rc<StreamClient>`** — constructors return `Rc<Self>` (via `Rc::new_cyclic`) so per-stream workers hold `Weak<StreamClient>` for the removal guard. Public API takes `&self`.

6. **EC is a per-stream property; all three streams can be EC-converted on seal.** Replication factor is fixed at **3** while open; EC default keeps `M=1` parity and grows `K = N-1` capped at 4 (N=4→3+1, N=5→4+1, N≥6→4+1; the cap bounds RS decode cost). `log_stream`'s arbitrary VP `(extent, offset, length)` sub-range reads go through `ec_subrange_read`'s generalised N-shard parallel scatter — one `read_shard_from_addr` per touched data shard, stitched in order (1/2-shard cases fall out as special cases); out-of-range offset (`start_shard >= data_shards`) routes to `ec_read_full_and_slice`, whose `ec_slice_decoded` returns `Err` (not panic) on `offset > full_payload.len()`.

    **`StaleVpOffset` wire contract** (load-bearing for Python operational tooling): an SST VP `(extent, offset, length)` whose `offset + length > manager.sealed_length` surfaces as the structured sentinel `StaleVpOffset` (`client.rs`), carried through the anyhow chain so the PS read path maps it to `FailedPrecondition`. Its Display string is a STABLE wire contract:
    ```
    stale_vp_offset_past_sealed_length: extent=<EID> offset=<OFF> length=<LEN> sealed_length=<SEAL>
    ```
    Regex consumers MUST treat the `stale_vp_offset_past_sealed_length:` prefix + the 4-field ordering as load-bearing; any format change is a wire-incompatible event. Such a VP means data past `sealed_length` is permanently gone (EC shards were physically truncated); remediation is `client del <key>` + major compact. There is no automated repair (the manager stores only the current sealed_length revision).

7. **EC offset semantics** — in EC mode `AppendResult.offset/end` are shard-level byte offsets (each shard has `shard_size(payload_len, data_shards)` bytes). Upper layers treat them as opaque and pass them unchanged to `read_bytes_from_extent`; the EC read path decodes transparently.

8. **EC shard index = position in `replicates ++ parity`** — `replica_addrs_from_cache` chains `replicates` then `parity`; shard `i` ↔ address `i` ↔ encode output shard `i`. Recovery's `replacing_index` uses the same ordering.

9. **Commit tracking is local, not per-append RPC** — `state.commit` is a plain `u32`, starts at 0, updates to `appended.end` after each success, resets to 0 after a new-extent alloc. `current_commit()` (which RPCs all replicas) exists for partition load time only, never in the hot append path.

10. **Extent allocation is capped per append** — at most 3 new-extent allocations per `append_payload` call (`MAX_ALLOC_PER_APPEND`), preventing runaway empty-extent creation if appends persistently fail.

11. **ConnPool is single-kind** — keys by `SocketAddr` alone; each address owns one sequential `RpcConn` on `Rc<RefCell<Option<RpcConn>>>` (take/put; if taken, open a fresh connection on the fly). There is no `PoolKind`.

12. **Chunked reads for >2 GiB extents** — `read_bytes_from_extent` splits requests larger than `AUTUMN_STREAM_READ_CHUNK_BYTES` (default 256 MiB) into per-replica RPCs. Without chunking a single `pread` of 3 GiB returns `EINVAL` (macOS INT_MAX, Linux 0x7ffff000). `length=0` resolves the byte count via `ExtentInfo.sealed_length` (sealed) or `commit_length_for_extent` (open), then chunks. EC dispatch goes straight to `ec_subrange_read`, so **`read_shard_from_addr` chunks internally at `read_chunk_bytes` (u64-offset)** — the single EC choke point that bounds each RPC ≤ 256 MiB (an EC shard is `sealed_length / data_shards`; a 16 GiB / K=3 extent has ~5.33 GiB shards, and a full-shard `MSG_READ_BYTES` would overflow the response frame's `payload_len: u32`).

13. **Chunked local-file I/O for >2 GiB extents** — server-side helpers in `extent_node.rs` (`FILE_IO_CHUNK_BYTES = 256 MiB`): `file_pread_chunked` (convert, read, copy) and `file_pwrite_chunked` (recovery, re_avali, write_shard; splits via `Bytes::split_to`, O(1)). Both fast-path a single syscall when payload ≤ 256 MiB. **Any new full-extent local-file read/write MUST use these — never call `file_pread`/`file_pwrite` directly with a `sealed_length`-sized buffer.**

14. **Sealed duplicated tails must allocate on init** — after `multi_modify_split` a child stream can inherit a tail extent the manager already sealed at the split point. `ensure_tail_initialised()` MUST treat this as "allocate fresh tail now", not "seed the worker and discover the seal on append" — the latter wedges descendant compaction (the first row_stream append hits the old tail, EN fencing surfaces `LockedByOther`, major compaction never clears `has_overlap`, the next split blocks forever).

14. **Read-side eversion freshness after EC conversion** — `StreamClient` passes its cached `ex.eversion` in every `ReadBytesReq`. A server reads reject `req.eversion < entry.eversion` with a `CODE_EVERSION_MISMATCH` **response** (not a frame-level error — the batched path must reach the client's `is_eversion_stale` detection), enforced in BOTH `handle_read_bytes` and `build_read_future`. The client surfaces `EversionStale` and runs a 2-attempt loop that `invalidate_extent_cache` + refetches `ExtentInfo` once; `read_replicated_with_failover` and `ec_subrange_read` fail-fast on it (every replica reports the same mismatch). **Invariant:** `entry.eversion` defaults to 1 on alloc (matches `MgrExtentInfo{eversion:1}`), so any `req.eversion = 0` is by construction stale (only bench/test fixtures pass 0). Never re-add a `> 0` "pass 0 to skip" clause — it silently let a stale-cached open-extent read scrape bytes from a shrunken post-EC shard.

15. **CPU-bound work MUST run on the blocking pool** — RS `ec_encode`/`ec_decode`/`ec_reconstruct_shard` take 100–300 ms per 128 MiB. All callers (`handle_convert_to_ec`, `run_ec_recovery_payload`, `ec_read_full`) wrap the call in `compio::runtime::spawn_blocking`; otherwise the compio event loop stalls while user RPCs queue. **Any new CPU-bound work in this crate (RS math, large CRC, large compression, big sort) MUST be wrapped in `spawn_blocking`.** Error plumbing is double `.map_err`+`?` (join-time panic-Box, then the inner erasure `Result`). Out of scope: WAL CRC32C on must_sync small writes (bounded ≤ 2 MiB, amortised).

16. **EC dispatch keys on `ExtentInfo.ec_converted`, NEVER on `parity.is_empty()`** — the manager pre-fills `parity` for every extent on an EC stream, so an open/pre-conversion extent has `parity != []` while still holding full replicated data on every K+M node. Only after `apply_ec_conversion_done` on a *sealed* extent does data physically split into K+M shards and `ec_converted` flip to `true`. Routing a pre-conversion extent through `ec_subrange_read` would compute `shard_size` from `sealed_length=0` and panic on the per-shard slice. Read dispatch (`read_with_layout`) and recovery dispatch (`run_recovery_task`) both branch on `ec_converted`. **Invariant:** `ec_converted == true` implies `sealed_length > 0`; never set `ec_converted` on an open extent.

    **Conversion is COPY-ON-WRITE: it adds files, it never replaces one.**
    `WriteShard` stages into `extent-{id}.shard{i}`; `.dat` is never renamed,
    truncated or overwritten, so an abandoned attempt costs a delete of files no
    reader is pointed at and a successor may pick a completely different
    assignment. **There is no commit phase** — the manager's layout flip is the
    single commit point. The old per-node rename left "some renamed, some not",
    a middle state nobody could classify, which is what made a stuck marker
    un-releasable. The commit phase is DELETED — `MSG_COMMIT_EC_SHARD` is a reserved
    tombstone and `.ec.dat` / `.ec.commit` no longer exist anywhere in the
    codebase. (They were retained briefly as repair code for a node upgraded
    mid-rename; on a development cluster with no historical data there was
    nothing to repair.) **`ec.prepared` remains**: it is the durable carrier of
    the attempt nonce, which travels in the request while the shard file on
    disk carries none — without it a coordinator cannot tell whether its
    staging came from THIS attempt and must re-encode on every re-dispatch.

    Consequences elsewhere: **EC shard recovery writes its rebuilt shard to the
    file the layout NAMES** (`.shard{i}` when `InShardFile`) — writing it into
    `.dat` would leave the node serving shard bytes to whoever still asks for
    the whole value, and the shard the layout points at missing. And
    **`read_plan` bounds a shard read by the SHARD file's length**, not the
    extent's: a shard is `sealed_length / K` while the `.dat` beside it (awaiting
    cleanup) holds the whole extent.

    **EC conversion is ACCEPT-then-BACKGROUND (same shape as recovery).**
    `handle_convert_to_ec` validates, refuses a duplicate via `ec_convert_inflight`
    (the manager re-dispatches from its durable marker every ~5 s, so without the
    guard every tick would spawn another converter), spawns
    `run_convert_to_ec_task`, and ACKs `CODE_OK = "accepted"` — **not "done"**. The
    completion is pushed to `ec_done` and drained by the next `df`
    (`DfResp.ec_done`), which is the ONLY signal the manager applies the layout on
    (from the etcd marker's PINNED assignment; a reported `new_eversion` that
    disagrees is REFUSED fail-loud). Rationale: an encode of a multi-GiB extent can
    outlive any RPC timeout, and a timeout is indistinguishable from a dead
    coordinator — that ambiguity is what made a stuck marker un-releasable. The
    idempotent-skip path ALSO reports done (the ADOPT case), so a completion lost
    to `df`'s at-most-once delivery converges on the next re-dispatch.

    **Attempt identity (`attempt_nonce`) rides the whole conversion.** The
    manager stamps each attempt with the etcd revision that created its marker;
    it flows `ConvertToEcReq` → `WriteShardReq` → `EcConvertDone`. Two EN-side
    consequences: (a) the coordinator's `ec.prepared` marker records
    `[new_eversion][attempt_nonce]` and the prepare-SKIP requires BOTH to match,
    because `new_eversion` repeats across a reissued attempt (it is `live + 1`
    and an abandoned attempt never bumped the extent) — a pre-nonce 8-byte
    marker reads as short and simply re-prepares, which is always safe; (b)
    `handle_write_shard` refuses a stripe whose nonce is LOWER than the attempt
    already staging on this extent (`ec_stage_nonce`, compared under the op
    lock). Nonces are etcd revisions, hence monotonic, which is what makes "this
    writer is superseded" decidable. The `owner_epoch` fence does not cover
    this: it only rises when the ex-coordinator was FENCED, whereas a routinely
    RELEASED coordinator keeps its epoch and would otherwise interleave stripes
    into its successor's staging file. `ec_stage_nonce` is in-memory (it
    arbitrates two live writers); across a restart the stripe-0 truncate and the
    epoch fence remain. `0` = pre-nonce peer, left unordered.

    **EC conversion is idempotent AND serialised on the coordinator.** `run_convert_to_ec_task` acquires a per-extent `ec_conversion_locks` `Rc<Mutex<()>>` across the entire prepare+commit, and re-runs the idempotency guard under it: if the extent is already EC-converted at this eversion (`entry.eversion >= req.eversion && sealed_length > 0 && avali > 0`) it returns `CODE_OK` without re-encoding. Without this, a re-dispatched or leader-failover-racing convert re-encodes the ALREADY-shrunk local shard as if it were the original payload, producing sub-shards ≈ `original / K²` → silent short reads. The manager dedups convert candidates by `extent_id` (primary fix); the coordinator lock + idempotency is defense-in-depth.

17. **Dead-replica recovery: closed-aware pool + append fanout timeout.** When an EN dies, autumn-rpc's `read_loop` sees EOF and clears `pending`, but the `Rc<RpcClient>` stays pooled. Three layers stop a caller hanging on a receiver that will never resolve: (a) `RpcClient::is_closed()` (set on `read_loop`/`writer_task` exit before `pending.clear()`); every `send_*` early-returns `ConnectionClosed`. (b) `ConnPool::get_client` skips + reconnects a closed entry; `send_vectored` evicts on submit error. (c) `launch_append` wraps each per-replica receiver in a bounded deadline (`StreamClientConfig.append_fanout_timeout`, default 5 s, clamped [200 ms, 60 s]; note 28 made this the BASE of a size-scaled deadline). `Elapsed` → soft error the retry loop escalates to `alloc_new_extent`. **Invariant:** any caller of `pool.send_vectored`/`call_vectored` against a peer that may go down MUST allow the surrounding logic to handle `Err` — never assume a returned receiver resolves.

18. **`read_with_layout` constructs `StaleVpOffset` upfront** when `ex.sealed_length > 0 && offset > ex.sealed_length`, BEFORE the `ec_converted` branch, so the `stale_vp_offset_past_sealed_length:` regex (note 6) fires on replicated VPs too and skips the wasted server round-trip + EC decode. EC's `ec_slice_decoded` keeps its own check as defense-in-depth. Open extents (sealed_length=0) are untouched (no authoritative bound).

19. **Recovery source-fetch MUST be chunked** (`copy_bytes_from_source` loops 256 MiB reads via `read_bytes_chunk`; `fetch_full_extent_from_sources` passes `extent.sealed_length`). A single `MSG_READ_BYTES length:0` on a multi-GB sealed extent trips the >2 GiB pread ceiling + oversized frame. **Caveat: `ReadBytesReq.offset` is u32 → covers extents up to 4 GiB** (guarded with an explicit error). The EC shard caller (`run_ec_recovery_payload`) keeps `total_len=0` (each shard ≈ `sealed/K`, under threshold). **Invariant:** any new full-extent fetch over `MSG_READ_BYTES` must chunk.

20. **Failover seal uses the SealCommit handshake — never a public-API-tracked commit.** On same-owner failover the writer seals the failed tail at its OWN all-replica-acked commit; letting the manager probe `commit_length` can capture a speculative/un-acked byte that only one soon-dead member holds → seals at a length no replica durably retains → recovery stuck forever (phantom seal). The only SAFE commit source is the worker's serialized `state.commit` at a QUIESCED point. Mechanism: `StreamSubmitMsg::SealCommit{resp}` → the worker `drain_inflight_for_seal` (awaits every in-flight append, bounded by each one's size-scaled deadline — note 28 — so it cannot hang) → replies the final contiguous `state.commit` → sets `sealing = true` (new appends on the doomed tail get a soft error → retry onto the fresh tail; cleared by ResetTail). The 3 failover sites call `seal_commit_watermark` then `alloc_new_extent(stream, Some(commit))`; preemptive roll passes `Some(result.end)`; new-owner/sealed-tail init passes `None` (→ manager probes). `StreamAllocExtentReq.seal_commit: Option<u32>` (Some = authoritative seal at exactly c incl 0; None = probe). **Invariant: never reintroduce a public-API commit-watermark cache; the seal length must come from the worker via SealCommit.**

    **Idempotent seal-and-roll pinned to `seal_extent_id`.** The SealCommit reply is `(commit, tail_extent_id)`; that extent id is threaded to the manager as `seal_extent_id`. `alloc_new_extent` runs under `retry_manager_call` (20×): if an attempt succeeds on the manager (seals tail T at `commit`, rolls fresh T') but its response is LOST, the retry re-sends the SAME `(commit, T)` (the worker's cached tail is still T — it never saw the lost ResetTail). The manager seals ONLY when its current tail still == `seal_extent_id` and is OPEN, else idempotent no-op — so it can NOT over-seal the now-current fresh tail T' at a stale `commit`. **Invariant: any authoritative (`Some(commit)`) alloc MUST pass the captured tail's `seal_extent_id`; only probe/`None` rolls may pass `0`.** Cross-ref: manager note 32a.

21. **`ExtentInfo.sealed` is the authoritative "is sealed" flag — NOT `sealed_length > 0`.** An authoritative empty seal is `sealed = true, sealed_length = 0` (e.g. a CoW-shared empty tail frozen by split/merge). `sealed` = STATE; `sealed_length` = LENGTH. `ensure_tail_initialised` + the soft-error reload check `.sealed`; `read_with_layout` uses `.sealed` for the to-end bound + stale-VP check (a sealed-empty extent reads as empty, no commit_length probe). The stale-VP read-BOUND offset checks keep using `sealed_length`. **Invariant:** `sealed_length > 0 ⇒ sealed`.

22. **`ResetTail` zeroes the worker's commit ONLY when it moves to a DIFFERENT extent.** `state.commit` is the contiguous all-replica-acked prefix — it advances ONLY on a full ACK, never speculatively, so it is ground truth and the value `seal_commit_watermark` reports. Two shapes (`apply_reset_tail`): a **DIFFERENT extent** (genuine roll to a fresh empty tail) → `reset_for_new_extent` (commit/lease_cursor=0, pending cleared, poisoned/sealing cleared); a **SAME extent** (a soft-error tail reload that did NOT change the tail, e.g. a transient replica refusal) → PRESERVE all append-progress state, refresh only cached replica metadata. Zeroing `commit` on a same-extent reload would make the next hard-path `seal_commit_watermark` report `commit=0` and `alloc_new_extent(Some(0))` seal the LIVE tail at `sealed_length=0`, orphaning every acked byte (a CoW child then never opens). `poisoned`/`sealing` MUST also be preserved (a poisoned hole excludes itself + everything after from the contiguous seal). **Invariant: never reset the worker's `commit`/`poisoned` when staying on the same tail extent.** Cross-ref: notes 20, 21, 9.

23. **`owner_epoch` fence is made DURABLE before any append is ACKed under it.** `owner_epoch` (`.meta` bytes 32–40) is the per-extent write fence: `revision < owner_epoch` → `CODE_LOCKED_BY_OTHER`. It is RAISED ONLY by the APPEND path (commit_length is check-only), monotonically, to the request's revision; the value originates from the manager owner-lock. It is NOT `eversion` (bytes 24–32) — independent, checked separately. Two coupled pieces, on BOTH append paths:
    - **In-memory bar raised SYNCHRONOUSLY** (`owner_epoch.fetch_max(R)`, before any await) so a stale lower owner is rejected immediately even while the new fence persists. `fetch_max` (not `store`) keeps it monotonic under two concurrent higher-revision appends.
    - **Durable high-water gates the ACK.** `ExtentEntry.durable_owner_revision` = the revision known durable in `.meta`. `ensure_fence_durable(id, entry, R)` returns Ok iff `durable >= R`; else it persists under `meta_write_lock` and advances `durable`. An append ACKs only after `durable >= R`. Fail-closed: a persist failure rejects (`CODE_PRECONDITION`) + marks the disk offline. Fast path = one atomic load; lock+persist fires only on a revision change.
    - After the durable step, **re-check** both `owner_epoch` (a higher owner may have taken over → LockedByOther) AND `sealed/sealed_length/avali` (a concurrent seal → CODE_PRECONDITION).

    `.meta` durability: ALL `.meta` writers go through `meta_write_lock` (per-extent, DISTINCT from the EC op-lock to avoid self-deadlock). `write_meta_locked` reads the LIVE atomics under that lock (so a stale-snapshot fence persist can't clobber a concurrent seal's `.meta`) and writes ATOMICALLY: `.meta.tmp` → fsync → rename → **fsync parent dir** (else the ACKed fence can regress on a host crash). **Invariants: (1) never ACK an append whose fence isn't durable; (2) raise the in-memory bar synchronously, persist before publishing `durable`; (3) every `.meta` write holds `meta_write_lock` and reads live atomics.**

24. **RESOLVED by the per-extent owner model — the old fsync-coalescer out-of-order-completion durability race is gone.** The deleted coalescer credited durability via a plain `pending_fsync.store` after the pwrite await; under out-of-order same-extent CQE completion a late low-offset write could `store` a SMALLER `pending_fsync` and be satisfied by a `pending <= synced` no-fsync branch, crediting durability to bytes the fsync did not cover. The owner rewrite deleted the coalescer + its loop entirely: appends to one extent are now serialized through ONE owner task (see "Per-extent owner") that drains a burst, issues ONE `write_vectored_all_at`, and does ONE `sync_data` covering exactly that burst's bytes (`pending_fsync` before, `last_synced` after), with bursts run SEQUENTIALLY. There is no concurrent same-extent completion to reorder, so `last_synced` can never advance past an unwritten prefix. The pre-owner analysis had judged the race near-unreachable under io_uring inline-FIFO + `i_rwsem` + single-writer SQE submission; the owner makes that single-writer property STRUCTURAL rather than incidental. **Any change that reintroduces concurrent same-extent writes (e.g. resurrecting a per-request FU on the SQ side) MUST re-validate this** — the load-bearing premise is now "one owner task, one fsync per sequential burst", not the incidental kernel ordering. Cross-ref: note 4, the Commit protocol section.

25a. **ENOSPC: disk health is a 3-state machine (`DiskHealth`: Online / Full / Faulted), the batched-append pwritev MUST be the `_all` form, and caller-ack ⊆ contiguous commit.**
    - **Every local file write MUST be a `*_all` form (or verify the count).** `build_append_future`'s batch path uses `write_vectored_all_at` (loops until done or a real error). POSIX pwritev on a nearly-full disk writes what fits and returns a SHORT count — `Ok(n)` from a raw positional write is NOT success; a partial append fsynced+ACKED reads its unwritten reserved tail back as zeros.
    - **Classification.** `mark_disk_error_for_extent(id, msg)` at every write/persist error site. ENOSPC/EDQUOT (`is_disk_full_error`, matched on the os-error suffix) ⇒ `Full`: the disk stops hosting NEW extents (`choose_disk` requires `allocatable()`) but keeps serving reads + existing extents, and the per-shard 2 s sweep SELF-HEALS it to Online once free ≥ 5%. Anything else ⇒ `Faulted` (permanent until restart). Manager: `select_nodes` soft-avoids nodes below `--min-alloc-free-bytes` (default 256 MiB, 0=off) with the note-17 fallback chain intact.
    - **Health is SHARED per physical dir across shards** — `DiskFS.health` is an `Arc<AtomicU8>` from the process-global `shared_disk_health(base_dir)` registry (canonical-path keyed), so shard B can't keep allocating onto a disk shard A marked Full.
    - **Caller-ack ⊆ contiguous commit (the seal-chop hole).** `apply_completion` fires the caller's oneshot Ok only as the contiguous prefix advances — NOT the moment a batch completes on all replicas when a LOWER lease on the same extent already failed (the mid-pipeline hole ENOSPC produces). Otherwise the writer's `commit` stays below the hole (correct) while the roll's SealCommit CHOPS an already-acked range. On poison, `failure_floor` = first failed offset; every pending/late completion at/above it resolves Err (its replica bytes become benign un-acked duplicates). **Invariant: a caller-visible append ack implies the range is inside the contiguous all-replica-acked prefix.**

25. **No `.meta` persist failure is ever swallowed** — the `.meta` sidecar is the only state a restart trusts (note 23); any path mutating eversion/sealed/avali/sealed_length then ignoring a failed persist reports a state the disk does not hold. All such sites are fail-closed (`mark_disk_offline_for_extent` + error): `run_recovery_task` final persist (also REMOVES the partial `ExtentEntry` so local retries can't reuse the offline disk and block manager re-dispatch), and both `handle_convert_to_ec` seal persists. Corollary: the note-16 **idempotent-skip must ENSURE durability, not just check atomics** — a prior attempt may have published the in-memory atomics then failed its persist, so the skip re-runs `save_meta` (idempotent) and fail-closes if it still can't. **Invariant: returning OK from any seal/convert/recovery RPC asserts the sidecar is durable, not merely that memory agrees.**

26. **/metrics.** The EN's authoritative state is shard-local `Rc` (unreadable from the metrics HTTP thread), so two process-global mirrors feed `render_en_metrics()`: (a) `EN_APPEND_TOTALS` — monotonic batch/bytes/ns counters (3 relaxed fetch_adds per BATCH); (b) `EN_SHARD_GAUGES` — one `Arc<EnShardGauges>` per ExtentNode, refreshed by a 2 s task spawned on THAT shard's runtime (NOT df-driven — the manager df reaches only shard 0, so a df-refresh left other shards permanently stale). The renderer sums extents across shards (disjoint sets) and reports a disk offline if ANY shard's view says so.

27. **Read-path Suspected avoidance (replicated route-around + EC proactive reconstruct).** `alloc_extent` already excludes manager-`Suspected` nodes; reads apply the SAME avoidance so rotation onto a flaky node does not cost a per-read timeout.
    - **Client-side snapshot, not new wire.** `StreamClient.suspected: Rc<RefCell<SuspectedCache>>` refreshed by `maybe_refresh_suspected()` at `read_with_layout` entry: TTL-gated (2 s) + NON-BLOCKING (spawns a detached poll of `MSG_LIST_NODE_STATES`, filter `NODE_AUTO_STATE_SUSPECTED`, swaps the set in; the current read proceeds on the slightly-stale snapshot and NEVER awaits a manager RTT). A failed poll keeps the prior snapshot — a failed refresh must NEVER widen the avoidance set.
    - **Replicated = soft deprioritize (never exclude).** `replicated_read_order(ex, offset, suspected)` folds rotated start + `avali` eligibility + Suspected-to-BACK; suspected slots are kept as a last-resort tail (suspected ≠ dead; a sealed extent's committed bytes are on EVERY replica). **Invariant: an empty Suspected snapshot reproduces the rotated walk byte-for-byte — the hot path is unchanged.**
    - **EC = proactive reconstruct.** In `ec_subrange_read` a data shard whose node is Suspected goes straight into `needs_reconstruct`, so the direct-read `join_all` never blocks on a flaky node's timeout before `ec_reconstruct_shard_subrange` (read K healthy + parity, first-K-wins) rebuilds it.
    - **ALL THREE VP read paths covered** (or a hot GET silently bypasses it): copy/chunked (`read_replicated_with_failover`), bulk value proxy (`read_value_into_pooled`, same `replicated_read_order`), client-direct descriptor (`extent_read_descriptor` uses `healthy_eligible_slots` to **DROP** Suspected addresses — the external SDK picks its own start, so ordering wouldn't help; keeps ALL when every one is Suspected → never strands).
    - **Soft hint, correctness-independent.** A stale/over-broad snapshot only costs latency or parity traffic, never data. Cross-ref: rotation+hedge, `eligible_replica_slots` (avali I2), write-path `bad_nodes` (distinct: alloc-only, per-stream).

28. **The append deadline is SIZE-SCALED, and a roll-away that sealed its tail EMPTY reclaims the abandoned extent.** A fixed 5 s `append_fanout_timeout` (sized for 4 KiB WAL) applied to a 256 MiB SST flush append times out while every EN durably wrote the bytes; those bytes are correctly EXCLUDED from `state.commit`, so the retry seals the tail at commit 0 and rolls, rewriting the same SST → a permanently unreclaimable `sealed_length=0` extent per iteration (invisible to all logical accounting, GC, and orphan-reconcile) → disk fill → death spiral.
    - **FIX #1 — size-scaled deadline** (`effective_append_timeout`): `deadline = min(append_fanout_timeout + payload_len / append_floor_bytes_per_sec, 600 s cap)`; threaded into `launch_append` as `AppendDeadline::for_payload`. `append_fanout_timeout` (clamped [200 ms, 60 s]) is the BASE; floor default 8 MiB/s — ~1.5 orders below measured healthy bulk, so it distinguishes "dead replica" from "slow but progressing" (4 KiB→5 s, 8 MiB→6 s, 256 MiB→37 s, 512 MiB→69 s). The 600 s cap preserves the note-17 dead-replica bound (every `InflightFut` still resolves → SealCommit drain can't hang). Tunable `with_append_floor_bytes_per_sec` [64 KiB/s, 1 GiB/s]. **Invariant: never apply a fixed deadline to an append path whose payload can span 4 KiB–512 MiB; derive from size.**
    - **The cap MUST be applied AFTER the chain hop-multiplier.** `AppendDeadline::for_chain = (for_payload(n) × hops × 3).min(EFFECTIVE_APPEND_TIMEOUT_CAP)`. Clamping inside `for_payload` then multiplying blows past the cap (RF=3 ⇒ 600 s × 9 = 90 min), destroying boundedness — every `InflightFut` must resolve within the cap or the SealCommit handshake can hang.
    - **FIX #2 — abandoned-tail reclaim** (`reclaim_abandoned_empty_tail`, from `alloc_new_extent` after a successful roll): when a failure-roll sealed its old tail at commit **0** (`seal_commit == Some(0)`, `seal_extent_id != 0`, and the manager did NOT hand that extent back as current tail — the idempotent-no-op branch), re-fetch the authoritative post-seal `ExtentInfo` (cache invalidated) and, ONLY if `sealed == true && sealed_length == 0`, best-effort `punch_holes` it out of the stream. SAFE because caller-ack ⊆ contiguous commit (note 25a): a sealed-AT-0 extent has NO acked byte ⇒ no VP/SST/checkpoint references it. **NEVER "fix" the leak by counting timed-out bytes as committed (silent-loss trap, notes 20/22/25a); never punch anything not authoritatively sealed-empty.** Residual (punch/extent_info failure on unreachable manager still leaks one extent) → backstop: a sealed-empty sweep. Cross-ref notes 17, 20, 22; manager note 32a.

29. **Reads use the same size-scaled deadline (`IoDeadline`, generalized from `AppendDeadline`).** A fixed 3 s (5 s on EC) applied to an 8 MiB transfer stormed under load. `IoDeadline` (`effective_io_timeout`, `IO_TIMEOUT_CAP`) is shared; READS use a SEPARATE (base, floor): `read_base_timeout` 3 s (every read < ~4 MiB is byte-identical to before) and `read_floor_bytes_per_sec` 4 MiB/s (looser than the 8 MiB/s append floor — a read is one replica's pread+transfer, no RF=3 fsync barrier). 8 MiB read → 5 s; 256 MiB chunk → 67 s < the 600 s cap (the cap is never binding for reads because every read RPC is chunk-bounded ≤ `read_chunk_bytes`; do NOT lower the cap below 67 s). Applied at all FIVE size-varying read sites (`read_extent_value_direct`, `read_value_into_pooled`, `read_shard_chunk_from_addr`, `ec_reconstruct_shard_subrange`, `ec_read_full`). **Retry damping on LIVENESS TIMEOUT** (`is_liveness_timeout`): the direct + bulk-proxy paths stop walking replicas after 2 timeouts and fall through to the authoritative proxy/copy path; FAST errors (eversion / GC'd / connect-refusal) keep the full rotation. **INVARIANT: any size-varying network I/O MUST derive its deadline from `IoDeadline::for_len(len)`, NEVER a fixed `Duration::from_secs(N)`.** Control-plane RPCs (commit_length, probe, stream/extent_info, alloc, punch, owner-lock) are fixed-size → a constant is correct there. Cross-ref note 28.

30. **Manager-call failures are TYPED (`ManagerError{code, ctx, message}`); `retry_manager_call` is code-classified; owner-epoch fences self-heal via the poison path instead of burning retries.** A flattened anyhow string made `retry_manager_call` (20×) treat an etcd-backed deterministic verdict (a permanently stale `owner_epoch`) as transient → 10 s of futile retries per call (rotating is useless — every manager reads the SAME etcd state). Three parts:
    - `check_manager_resp` returns `ManagerError` (code preserved for `downcast_ref`; Display keeps the legacy `"<ctx> failed: <message>"` shape for non-fence so log greps are unaffected).
    - `retry_manager_call` classifies before retrying (see the retry-classification list under "Connection & ownership"). The transient-conflict marker (`is_transient_conflict`) is retryable WITHOUT rotating; the fence + deterministic Preconditions fail fast, unwrapped.
    - **Owner-fence self-heal.** `ManagerError::is_owner_fence` (CODE_PRECONDITION + `autumn_common::is_owner_epoch_fence_message`, the matcher living next to its `ensure_owner_epoch` producer in `common/store.rs`) renders Display as `"<ctx> fenced (LockedByOther): …"`. The **"LockedByOther" token is LOAD-BEARING**: the PS's `is_locked_by_other` (`{e:#}` chain-wide so `.context` wraps can't hide it) poisons the partition → reopen re-acquires a fresh epoch (the SAME self-heal the EN's native `CODE_LOCKED_BY_OTHER` triggers). The PS also closes the liveness gap: a poisoned/crashed partition thread's dead handle is detected by `sync_regions_once` (`is_finished()`) and dropped, so the region map arbitrates the same tick (still-assigned → reopen with fresh epoch; moved-away → clean release). **Invariant: a stale `owner_epoch` must NEVER be silently re-acquired inside StreamClient** — auto-refresh would defeat fencing (a zombie writer could steal the lock back); recovery is always tear-down-and-reopen at the partition layer, arbitrated by the region map.

31. **`fence_tail` EAGERLY raises the EN fence floor on TAKEOVER (G1 "SIGSTOP zombie writer" fix).** The EN `owner_epoch` floor is raised ONLY by the APPEND path (note 23), so on an IDLE takeover (new owner acquires E_new but has not written yet) the floor stays at the OLD owner's epoch E_old. A paused-then-resumed previous owner (SIGSTOP > eviction window → reassign → SIGCONT) whose in-flight append still carries E_old then PASSES the fence (`E_old == stored`, neither `<` nor `>`), lands in the log extent, and is ACKed — a silent LOST UPDATE the new owner never sees. `fence_tail(stream_id, owner_epoch)` closes this: it resolves the stream's CURRENT tail extent + replica set (`get_stream_info` → `get_extent_info` → `replica_addrs_for_extent`) and sends `MSG_FENCE_EXTENT{tail, owner_epoch}` to every REACHABLE replica, raising the EN floor to E_new BEFORE the partition serves. The PS calls it in `partition_thread_main` for all three stream tails (log/row/meta) right after the initial `commit_length` loop and BEFORE `recover_partition` (fencing is a control op, not a row_stream append, so routing all three through `part_sc` is consistent with the row_stream single-writer rule). **Lenient + best-effort** (mirrors seal-over-reachable): append is all-replica-ACK, so fencing even ONE reachable replica already blocks the zombie (its append needs EVERY replica to accept). Returns Ok if ≥1 replica was fenced (`CODE_OK`) or already carries a higher epoch (`CODE_LOCKED_BY_OTHER`); an unreachable / fail-closed replica is logged + skipped; a total failure is logged and the open PROCEEDS (never wedge on a transient fence failure). **Invariant: the takeover fence must run before the partition accepts requests; it does NOT replace the append-path fence (note 23) — it makes it EAGER instead of first-append-lazy.** The read-side (`handle_get`) has no write fence, so a residual STALE READ from the old owner before it closes the reassigned partition is a documented SEPARATE follow-up (out of scope for this write-side fix). A second narrow residual: an extent allocated AFTER the fence is born at floor 0 (raised lazily by the first append), so a zombie that follows a tail ROLL into a fresh unfenced tail during the sub-second window before the new owner's first append could still slip through — far narrower than pre-fix (needs a sealed tail at takeover + tight timing); a born-fenced alloc (carry the allocator epoch in `AllocExtentReq`) is the hardening follow-up. The APPEND path also re-checks `owner_epoch` AFTER the commit-reconcile truncate await and (non-batched path) before the ACK, so a fence landing DURING an in-flight append's widest awaits still rejects it — the fence op's "durable ⇒ stale appends rejected here" contract holds across the whole prologue.

32. **`seal_and_roll_tail` is live-writer-aware — a LIVE stream's tail may only be sealed through its worker.** The manager learns a seal immediately, but the ENs learn it only LAZILY (nothing pushes seals; the append path detects them only via an eversion mismatch that a stale writer never triggers, since its cached eversion equals the ENs'). So a bare manager probe-seal (`alloc_new_extent(None, 0)`) behind a live per-stream worker freezes `sealed_length` while the worker keeps appending to the SAME extent and keeps ACKing clients — every post-seal acked byte sits above `sealed_length`, invisible to committed-clamped replay and to CoW split children (the chaos acked-write-loss family: `stale_vp_offset_past_sealed_length` child wedge / silent-stale reads; deterministic repro `crates/manager/tests/system_roll_tails_live_writer.rs`). `seal_and_roll_tail` therefore branches on `existing_stream_worker(stream_id)` (lookup-only, never spawns): worker present → SealCommit handshake (quiesce → the worker's exact all-replica-acked commit, freezes the doomed tail) → `alloc_new_extent(Some(commit), tail_id)` (notes 20/22 idempotent-roll rules) → `ResetTail` onto the fresh extent — identical mechanics to the append-failure roll; worker absent (WAL self-heal A4 runs before the worker spawns) → the original probe roll, race-free because there is no writer to race. **Invariant: never manager-seal a stream tail that a live worker may still be appending to without first quiescing THAT worker via SealCommit and redirecting it via ResetTail.** If the alloc fails after SealCommit, the worker is left `sealing = true` — self-healing: the next append soft-errors into the public-API retry path, which performs its own quiesced roll.

---

## RPC wire protocol (`extent_rpc.rs`, in autumn-rpc)

autumn-rpc custom binary protocol (10-byte frame header). No protobuf — hot-path
RPCs use hand-coded binary encoding; control-plane RPCs use rkyv zero-copy.

### Hot-path binary codecs

| RPC | msg_type | Request | Response |
|-----|----------|---------|----------|
| Append | 1 | 29B + payload | 9B |
| ReadBytes | 2 | 24B | 9B + payload |
| CommitLength | 3 | 16B | 5B |
| FenceExtent | 17 | 16B | [code]+msg |

### Control-plane (rkyv)

AllocExtent(4), Df(5), RequireRecovery(6), ReAvali(7), CopyExtent(8),
ConvertToEc(9), WriteShard(10), DeleteExtent(11), ReconcileExtents(0x31).

**`MSG_FENCE_EXTENT` (17) — eager owner_epoch fence, no append.** `handle_fence_
extent` raises the per-extent `owner_epoch` fence floor to `req.owner_epoch`
WITHOUT writing: it mirrors the APPEND fence prologue (`owner_epoch.fetch_max` +
`ensure_fence_durable`, fail-closed) minus the pwrite. `FenceExtentReq`
(16 B binary `[extent_id][owner_epoch]`, same shape as CommitLength) →
`FenceExtentResp` (`[code][message]`): `CODE_OK` = floor `>= owner_epoch` &
durable; `CODE_LOCKED_BY_OTHER` = a HIGHER owner already holds it (caller stale);
`CODE_PRECONDITION` = fail-closed (persist error / quarantined `.meta`).
`owner_epoch <= 0` is a protocol error. It deliberately does NOT reject a SEALED
extent — raising a sealed tail's floor is a harmless no-op. Used by
`StreamClient::fence_tail` on partition TAKEOVER to close the G1 idle-takeover
window (see note 31).

---

## Performance (`benches/extent_bench.rs`)

Single compio thread, loopback TCP, 4 KB payload, single pipelined connection:
- **Write depth=32**: 116k ops/s, 455 MB/s
- **Write depth=64**: 125k ops/s, 489 MB/s
- **Read depth=64**: 95k ops/s, 373 MB/s
- **Mixed 1w+1r**: 93k total ops/s

Optimizations: pwritev batch (consecutive MSG_APPEND → one `write_vectored_at`),
pread batch, `write_vectored_all` (all responses from one TCP read in one
syscall), client pipelining (sliding window hides RTT + enables server-side
batching). See `benches/bench_results.md`.

---

## Background-loop supervision (extent-node)

Every EN background loop runs under a supervisor (a bare `spawn(..).detach()`
swallows panics). Two helpers in `extent_node.rs`:
- `en_spawn_supervised(name, make)` — catch_unwind + ERROR-log + 1 s restart, for
  the RESTARTABLE orphan-reconcile sweep (re-derives from `node.clone()` each
  tick, owns no moved resource).
- `en_spawn_failstop(name, fut)` — catch_unwind; normal return = expected lazy
  exit; PANIC → ERROR-log + `std::process::exit(1)`, for the per-extent
  `owner_loop` (the durability-critical single writer; a panic mid-burst must not
  silently strand queued appends → fail-stop, EN restarts and recovers from disk).

**Bounded connect**: `ConnPool::get_client` wraps `RpcClient::connect` in a fixed
`CONNECT_TIMEOUT` (5 s) so a blackholed peer (SYN dropped) can't hang a caller
(`call_timeout` only bounds the call AFTER connect).

**Invariant:** never reintroduce a bare `spawn(..).detach()` for an EN
background loop — use `en_spawn_supervised` (re-derive-safe) or
`en_spawn_failstop` (moved-resource / durability). Request-triggered detached
tasks (`run_recovery_task`, EC convert) are NOT loops — a panic there fails one
retried operation, so they stay as-is.

---

## Streaming recovery / re_avali (extent-node memory)

`run_recovery_task` and `handle_re_avali` stream the full sealed extent from a
healthy peer **chunk-by-chunk** via `stream_extent_from_sources`: per chunk read
one `FILE_IO_CHUNK_BYTES` (256 MiB) range via `MSG_READ_BYTES` → `pwrite` at its
offset → drop → next. **Peak resident = one chunk**, independent of extent size;
one `sync_data` after the full write. `dest` is truncated to 0 before each
source attempt, so a mid-stream source failure abandons that source and the next
restarts from offset 0 (partial write discarded — no corruption). Succeeds only
on a full `sealed_length` transfer. EC recovery (`run_ec_recovery_payload`) still
buffers shard-sized (≈ `sealed/K`).

**Stripe-wise EC convert.** RS over GF(256) is byte-wise per offset
(`erasure::ec_encode_stripe`, byte-identical to a slice of `ec_encode`), so
`handle_convert_to_ec` reads the K data sub-ranges at shard-offset `s`, encodes
the M parity stripes, and streams each shard's stripe via `WriteShard` carrying
`shard_offset: u64`; the receiver `pwrite`s into `.ec.dat` at `shard_offset` +
`sync_data`s (`write_shard_stripe_local`). Peak RAM = `(K+M) ×
EC_ENCODE_STRIPE_BYTES` (64 MiB default → ~256 MiB), independent of extent size;
each WriteShard stays under the frame ceiling. Crash-safety: per-stripe
`sync_data` + sequential await-ack grow the durable prefix monotonically; the
coordinator writes its OWN shard-0 stripe LAST per stripe, so
`coordinator_prepared` ⇒ every participant durably staged every stripe.
`write_shard_stripe_local` bounds `shard_offset + stripe_len ≤ sealed_length` so
a malformed/stale WriteShard can't balloon `.ec.dat` into a sparse file. The
peer-copy (coordinator local short) streams into a TEMP file via
`peer_copy_full_extent_to_dat` and atomic-renames over `.dat` only after a full
copy lands (never `set_len(0)` a live replica before securing a complete copy).

**Invariant:** any new full-extent peer-copy-then-writeback path must stream via
`stream_extent_from_sources` / `peer_copy_full_extent_to_dat` (or an equivalent
read-chunk→write-chunk→drop loop) — never buffer the whole extent in a `Vec`
unless shard-sized. Any new EC encode must be stripe-wise so the transient + the
per-RPC WriteShard stay bounded regardless of extent size.

### Recovery over-promised-seal reconciliation

`stream_extent_from_sources` does NOT retry forever when the manager's
`sealed_length` is an unrecoverable over-promise (the lenient failover seal can
seal at a length no replica durably retains — e.g. a speculative byte that
rolled back). It tracks per-source outcome: `best` (longest copy delivered),
`err_count` (sources that errored mid-stream), `unverified` (non-excluded
sources that could not even be attempted). If a source delivers the full
`sealed_length` → return it. Otherwise, **only when EVERY non-excluded source
was REACHED and is consistently SHORT** (`err_count == 0 && unverified == 0`),
reconcile to the replica consensus: re-stream the longest available copy and
return it. SAFE under all-replica-ACK (the acked prefix is on every committed
replica, so the best reachable copy is ≥ acked; only phantom un-acked tail bytes
drop). Two load-bearing guards: (1) re-stream errors are propagated
(`stream_one_source(...).await?` then `got < best_len → Err`), NEVER swallowed to
`Ok(0)`; (2) `unverified == 0` (not just `err_count == 0`) — an un-attempted
source might hold the only full copy. **Invariant: never reconcile a sealed
extent DOWN while any non-excluded source was unreachable or unattempted.**

---

## 崩溃一致性 fail-closed 不变量

三处崩溃一致性都是 fail-closed（断电/内核崩溃触发；`kill -9` 不丢已 fsync 的
page cache 或 dirent，chaos 测不到）。

**META-FAILCLOSED — 损坏 `.meta` 隔离。** `load_extents` 读路径区分：(a) `.meta`
present-but-corrupt（parse None = CRC/magic/eid 不符）→ quarantine；(b) 非-NotFound
读错误（EIO/EACCES，状态未知）→ quarantine；(c) 真 NotFound → 默认 open（fresh
extent）。`ExtentEntry.corrupt_meta: AtomicBool`；quarantine 时 append / read /
commit_length **全部拒绝**；`write_meta_locked` 成功持久后清 flag（recovery/re_avali
重建即 un-quarantine）。**不变量：`.meta` 损坏/不可读 + `.dat` 在 = 绝不默认 open，
必 quarantine 待 manager 恢复**（否则一个本该 sealed/fenced 的 extent 重启即变
open+epoch0，stale 低-epoch writer 绕过 fence ghost-append = split-brain）。

**EC-PREPARE-DURABLE — EC 2PC staging 的父目录 fsync。** `write_shard_local`
（prepare）写 `.ec.dat` 后 `sync_data()` + **fsync 父目录**（`fsync_staging_dir`），
新写路径与幂等早退路径都调用（早退也要满足 durable-prepare 语义）；commit 的
`rename(.ec.dat→.dat)` 之后同样 fsync 父目录。POSIX 下新文件的目录项不随内容
fsync 持久，缺父目录 fsync 断电可整个丢 dirent → commit 找不到 staging → 2PC
participant 卡死。

**EC-COMMIT-ATOMIC — `rename(.ec.dat→.dat)` ↔ `save_meta` 崩溃窗的 intent
marker。** commit 是两步独立持久化（① rename+dir-fsync ② atomics+save_meta），两步
间崩溃（`kill -9` 即可复现）→ `.dat`=shard 但 `.meta`=pre-EC，旧幂等永久卡死 + 把
shard 当完整 value 读（损坏）。修复：`extent-{id}.ec.commit` marker
（`[new_eversion][sealed_length]`，**rename 前**durable 写，save_meta 后删）。
`finish_ec_commit` 共享 helper：rename-if-staging + **总是 reopen `.dat`**（同进程
retry 可能持旧 unlink fd）+ **单调 `fetch_max` eversion**（防旧 marker 回退）+
save_meta。`load_extents` 启动重放三态 `EcCommitMarker`：Valid+eversion<marker →
补齐；eversion≥marker → 仅清 marker；Corrupt → **quarantine**；Absent → 跳过。
同进程 retry 分支用 marker payload（marker 是已发布 `.dat` 的权威）+ 同样 eversion
门控。**不变量：`corrupt_meta` 的 extent 绝不 marker-replay**（marker 无
owner_epoch，replay 会写 owner_epoch=0 → fence 旁路 + 清 quarantine = META-FAILCLOSED
漏洞）。`remove_extent_files` 同时 unlink marker。
