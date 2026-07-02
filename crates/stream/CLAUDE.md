# autumn-stream Crate Guide

## Purpose

Five components in one crate:
1. **`ExtentNode`** (`extent_node.rs`) — the server-side storage daemon that holds extents on local disk, implements ExtentService via autumn-rpc (custom binary protocol on compio).
2. **`extent_rpc`** — wire codec for all 10 ExtentService RPCs. Hot-path uses binary encoding; control-plane uses rkyv. **CLUSTER-DF: relocated to `autumn-rpc` (`crates/rpc/src/extent_rpc.rs`)** so all three wire schemas share one home; re-exported here (`pub use autumn_rpc::extent_rpc` in `lib.rs`), so `crate::extent_rpc::*` / `autumn_stream::extent_rpc::*` paths are unchanged. `DiskStatus` gained `extent_bytes` (EN self-reported per-disk extent footprint); `handle_df` sums `ExtentEntry.len` by `disk_id` — real on-disk bytes, the source the manager aggregates into cluster `physical_used` (see manager CLAUDE note 40).
3. **`StreamClient`** (`client.rs`) — the client library used by `PartitionServer` to read/write streams. Manager calls are stubbed (F044 scope).
4. **`erasure`** (`erasure.rs`) — Reed-Solomon EC codec (`ec_encode`, `ec_decode`, `ec_reconstruct_shard`), wrapping `reed-solomon-erasure` crate.
5. _(removed in F150 Phase A-: extent-node WAL deleted; SSD-only deployments make sequential WAL fsync no longer beat extent-file fsync)_.

All are exported from `src/lib.rs`.

---

## ExtentNode — Server Side

### Data Model (F021: Multi-Disk)

An `ExtentNode` can manage **multiple disk directories**. Each directory is represented by a `DiskFS` struct (disk_id, base_dir, online flag). File I/O uses `compio::fs::File` directly (no IoEngine abstraction).

All extents use the hashed layout: `{data_dir}/{hash:02x}/extent-{id}.dat` + `.meta`. Hash = `crc32c(extent_id_le_bytes) & 0xFF` (low byte). Hash subdirs are created on-demand — no pre-formatting required. Matches the 256 subdirs created by `autumn-op format` (F213).

Each extent file pair:
- `extent-{id}.dat` — raw data (append-only during active use)
- `extent-{id}.meta` — 40-byte binary sidecar:

| Bytes | Field |
|-------|-------|
| 0–7 | Magic: `EXTMETA\0` (V0, legacy) or `EXTMETA\x01` (V1, post-F157) |
| 8–15 | `extent_id` (le u64) |
| 16–23 | `sealed_length` (le u64) |
| 24–31 | `eversion` (le u64) |
| 32–39 | `owner_epoch` (le i64) |
| 40–43 | **F157**: CRC32C of bytes 0–39 (V1 only; V0 lacks this trailer) |

`ExtentEntry` stores `disk_id` for path resolution. `choose_disk()` returns the first online disk (matches Go's strategy). `df()` returns real `statvfs` stats per disk.

**Multi-disk usage** (production):
```bash
# Format disks and register with manager
autumn-client --manager ... format --listen :9101 --advertise host:9101 /disk1 /disk2

# Start node with multiple disks (comma-separated or repeated)
autumn-extent-node --data /disk1,/disk2 --manager ...
```

**Single-disk usage** (tests / backward compat):
```bash
autumn-extent-node --data /tmp/data --disk-id 1 --manager ...
```

In memory, `ExtentNode` holds a `Rc<DashMap<u64, Rc<ExtentEntry>>>` (single-threaded compio, no `Arc`/`Mutex` needed):

```rust
struct ExtentEntry {
    file: RefCell<Rc<CompioFile>>, // F171: structural close of file-replace UB
    len: AtomicU64,                // current byte length
    eversion: AtomicU64,           // bumped on seal or eversion change
    sealed_length: AtomicU64,      // 0 = active; >0 = sealed at this length
    avali: AtomicU32,              // availability flag (non-zero = sealed)
    owner_epoch: AtomicI64,      // most recent owner revision seen
    disk_id: u64,                  // immutable after creation
}
```

No `write_lock` — appends are serialized by the single-threaded compio runtime (sequential processing in `handle_connection`).

**F171 — file handle is `RefCell<Rc<CompioFile>>` (no `unsafe` in the file path).**
Pre-F171 the field was `UnsafeCell<CompioFile>` and access used
`unsafe { &*file.get() }` for borrows, `unsafe { *file.get() = new_file }` for
the EC-commit replace. F166 removed the `&mut` aliasing UB on the borrow paths
by switching to shared refs through compio's `impl AsyncWriteAt for &File`
(`SharedFd` interior mutability). F167 encapsulated the replace path with a
documented invariant. F171 closes the remaining type-level UB structurally:
- All borrows go through `entry.file_rc()` which clones the inner `Rc<CompioFile>`
  under a brief `RefCell::borrow()`; the clone is held by the I/O future across
  `.await`. The `RefCell` borrow itself is released before the first `.await`.
- The file-replace path (`commit_shard_local`) calls `entry.replace_file(new)`
  which is a safe `RefCell::borrow_mut()` + `Rc::replace`. The OLD `Rc` is
  dropped only when the LAST concurrent reader releases its clone, so the
  underlying fd cannot dangle.
- F153's per-extent `ec_conversion_locks` is no longer load-bearing for memory
  safety; it remains as a higher-level serialisation against concurrent EC
  dispatches racing on the staging path.

Helper signatures (`file_pwrite`, `file_pread`, `file_pwrite_chunked`,
`file_pread_chunked`) all take `Rc<CompioFile>` by value; the I/O future
captures it. The free function `file_ref` was removed — callers use
`entry.file_rc()` directly. There is no `unsafe` in any file-access path
on the extent node post-F171.

### Connection Handling & Batch Optimization (R4 step 4.2 v3 — true SQ/CQ)

`handle_connection` is ONE compio task per TCP connection. It runs a
**true SQ/CQ** loop: a persistent read future (the "SQ") and an inline
`FuturesUnordered` of in-flight batch I/O futures (the "CQ") are polled
concurrently via `futures::future::select`, so completions stream out to
the client as soon as they happen — not gated on a burst boundary.

```
┌─ ConnTask (single task, true SQ/CQ) ────────────────────────────┐
│                                                                 │
│  SQ side — persistent read future:                              │
│    Option<LocalBoxFuture<'static, ReadBurst>>                   │
│    owns OwnedReadHalf + 512 KiB buf across iterations;          │
│    NEVER dropped mid-flight (io_uring SQE stability)            │
│                                                                 │
│  CQ side — FuturesUnordered<Pin<Box<dyn Future<Vec<Bytes>>>>>   │
│    cap = AUTUMN_EXTENT_INFLIGHT_CAP (default 64)                │
│    holds in-flight append/read batch + control-rpc futures      │
│                                                                 │
│  Loop:                                                          │
│    1. drain ready completions via `.next().now_or_never()`      │
│       → tx_bufs                                                 │
│    2. flush tx_bufs with ONE `write_vectored_all` syscall       │
│    3. branch on (n_inflight, at_cap):                           │
│       n_inflight == 0 → await read alone                        │
│       at_cap          → await completion alone (back-pressure)  │
│       n_inflight == 1 → await completion (fast path: a          │
│           pipelined client can't submit more until responses    │
│           flush, so racing the read has no upside and costs     │
│           ~5-10 µs of per-iter polling overhead)                │
│       n_inflight > 1  → select(read, inflight.next())           │
│           Left wins  → process frames, restart read_fut         │
│           Right wins → put read_fut back, extend tx_bufs        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

1. **MSG_APPEND batch** — consecutive append frames grouped by extent_id
   are packaged into ONE append future. The future's I/O body issues ONE
   `write_vectored_at` (pwritev) and returns N already-encoded response
   frame bytes.
2. **MSG_READ_BYTES batch** — same grouping; the future runs preads
   sequentially inside and returns N encoded response bytes.
3. **Control RPCs** (ALLOC, DF, RECOVERY, etc.) — each becomes one future
   pushed onto the same FU. Responses fold into the same tx_bufs flush.
4. **Cross-extent concurrency** — if a single TCP read produces batches
   for N different extents, all N futures sit in FU simultaneously. The
   underlying `write_vectored_at` on each extent's compio file future
   drives them in parallel; FU returns them as they complete (fastest
   disk first). With true SQ/CQ, the first completion's response bytes
   flush to the client immediately at the next loop top — they do NOT
   wait for the slowest in-flight op to finish.

**Extent-len reservation**: step 7 of `build_append_future` stores
`extent.len = total_end` BEFORE returning the I/O future into FU. This
guarantees overlapping same-extent submits (if pushed in the same burst)
compute non-overlapping `file_start` values — necessary for the SQ/CQ
overlap model.

**Why a single-inflight fast path?** In the sustained-pipelining bench
(client depth=64 against one extent), every request cycle produces ONE
batch future (all 64 frames grouped into one pwritev). The client waits
on responses before sending more, so no new reads arrive while the
pwritev is in flight. Running `select(read, completion)` in this case
pays ~5-10 µs per cycle for polling both futures but provides no
concurrency benefit (the read is always pending). The `n_inflight == 1`
branch awaits the completion alone, restoring hot-path parity with the
pre-4.2 baseline (`extent_bench` W d=64 ≈ 208k ops/s, within 1 % of the
210k baseline). Once `n_inflight > 1` (multi-extent burst or
heterogeneous op mix), the select-based race kicks in and responses
stream out as each completion lands — this is the path the new
`cq_flushes_fast_ops_while_slow_op_runs` integration test exercises.

**Why not `v2` burst structure?** v2 (commit `b1a92f7`) used a
burst-structured loop: `reader.read → push futures → while
inflight.next().await → flush`. This kept microbench perf at parity but
violated SQ/CQ: in a mixed "1 slow append + 100 fast reads" burst, all
100 read responses sat in `tx_bufs` until the slow append's pwritev+sync
finished, because the drain `while` waited for ALL in-flight to complete
before flushing. v3 fixes this by draining + flushing opportunistically
every iteration. Correctness proof: `cq_flushes_fast_ops_while_slow_op_runs`
measures that the first read response arrives in < 0.5 × the time it
takes the slow 64 MB must_sync append to complete (typically ~0.4×).

### F260 — chained append (MSG_APPEND_CHAIN, default OFF)

Large appends (>= `set_append_chain_min_bytes`, 0=off=default) can ship ONE
wire copy to replica[0] which pipelines down the chain; the single ack means
every hop wrote (all-replica-ACK preserved; per-hop fencing/commit-truncation
unchanged). EN keeps a per-downstream-addr forwarder task — the conn loop
enqueues forwards UNBOUNDED in arrival order (a blocking submit there stalls
the whole handle_connection under backlog — the v1 bug), the forwarder
submits sequentially (per-extent order ⊆ per-addr order) and hands the
response receiver back so downstream RTTs overlap. KNOWN LIMITATION: per-
append store-and-forward stacks hop latencies — deep 8M queues (128 in
flight) blow the timeout budget on loopback; the win (writer egress 3x->1x)
only exists where the writer NIC is the bottleneck (cross-host). Keep OFF on
loopback; cross-host acceptance pending.

### Append Protocol (eversion check → seal check → fencing → commit truncation → write)

```
Append(AppendReq via autumn-rpc binary frame):
  1. Decode binary request (extent_id, eversion, commit, revision, must_sync, payload)
  2. Eversion check:
       - If client eversion > local: fetch ExtentInfo from manager, apply if sealed
       - If client eversion < local: reject (PRECONDITION_FAILED)
  3. Sealed check: reject if sealed_length > 0 or avali > 0
  4. Revision fencing:
       - If header.revision < owner_epoch: reject (CODE_LOCKED_BY_OTHER — stale owner)
       - If header.revision > owner_epoch: update owner_epoch, persist meta
  5. Commit reconciliation:
       - If local file len < header.commit: reject (data loss on our side)
       - If local file len > header.commit:
           * F119-E / F123: first confirm with manager that extent is not sealed
             (if manager says sealed, apply meta + reject with CODE_PRECONDITION)
           * TRUNCATE file to header.commit (await truncate_to_commit_ref).
             F152: `truncate_to_commit` calls `set_len` then `sync_data`
             before updating `extent.len` — without the fsync, post-crash
             the file size could be observed at the pre-truncate length
             (kernel hasn't durably persisted the inode shrink), letting
             the next `commit_length` probe report a wrong consensus and
             diverging replicas at the same offset.
           * F146: re-check sealed_length / avali after the truncate await — a
             concurrent apply_extent_meta_durable (from handle_re_avali or
             another append's pre-truncate seal-confirm branch) may have landed
             a fresh seal DURING the truncate I/O. Without this re-check, pwritev
             would write bytes past the new sealed_length, corrupting subsequent
             reads as "logStream value short" or out-of-bounds slice panics on EC
             reads. The re-check fires CODE_PRECONDITION; client retries via the
             standard apply_completion soft-error path (same as F119/F123/F143).
           * F147-B: the same post-truncate seal recheck is also inserted in the
             non-batched `handle_append` path (line ~2437), which previously had
             the F146 guard only in `build_append_future` (the batched path in
             `handle_append_batch`). Without F147-B, a concurrent seal landing
             during the truncate await on a non-batched append wrote past the
             new sealed_length identically to the F146 race.
           * F147-C: `run_recovery_task` now performs a verify-after-fetch step
             after `sync_all` and before writing the recovered bytes back: it
             re-reads the local extent's `eversion` and refuses with an error
             (triggering retry) if it advanced during the fetch I/O. Additionally,
             `fetch_max writeback` is gated on the fetched length matching the
             manager-reported `sealed_length`; a mismatch means a concurrent seal
             landed during recovery and the task retries rather than persisting
             stale metadata.
           * F148-B: `handle_copy_extent` (extent_node.rs:3114-…) refuses with
             CODE_PRECONDITION when the local `entry.sealed_length` is 0
             after the manager-fetch + apply_extent_meta_durable step.
             Production callers (`run_recovery_task`,
             `handle_re_avali`) only target sealed extents by design — the
             manager dispatches both only after seal. Without this guard, a
             stray caller hitting an unsealed extent would race a concurrent
             in-flight `handle_append`'s `truncate_to_commit` await window
             and observe a mix of pre- and post-truncate bytes via
             `file_pread_chunked`. On a sealed extent the append protocol
             step 3 rejects concurrent appends, so the race only exists for
             unsealed extents. The guard converts the theoretical race into
             a clean CODE_PRECONDITION error and documents the invariant in
             code. Sibling note in `partition-server/CLAUDE.md` Programming
             Note 12 describes the matching publishing-order invariant for
             flush + compact metadata publishes.
  6. Write payload (Direct path — F150 Phase A- removed the WAL fast path,
                    F178 Phase 1 routes durability through the coalescer):
       - file.write_at(start, payload)
       - F178: advance `entry.coalescer.pending_fsync` to end. ALWAYS,
         regardless of must_sync. Pre-F178 must_sync=false meant "leave
         dirty pages in cache, no syscall"; post-F178 false means "no
         WAITER registered, but the coalescer task picks them up on its
         next 1-5 ms tick anyway". Gives LevelDB-style "always durable"
         semantics without paying syscall cost per write.
       - F178: if must_sync, register_sync_waiter(extent, end) → await
         oneshot. The coalescer task is event-driven (RocksDB group-
         commit style): the first waiter at an idle extent triggers
         `file.sync_data()` immediately; any pwrite that completes
         before the syscall returns rides along. Subsequent waiters
         that miss this group get a fresh wake → fresh fsync. No
         timer involved.
       - Lazy spawn / clean exit: the first register_sync_waiter
         creates an `mpsc::Unbounded<()>` wake channel and spawns the
         loop; subsequent registers push `()` onto the channel.
         The loop parks on `wake_rx.next()` between fsyncs; on
         "no work AND no waiters" it sets `wake_tx = None` and
         returns. A future register sees None and spawns a fresh
         task. Compio's single-threaded scheduling makes this race-
         free without locks.
       - Pre-F178: `if must_sync: file.sync_all()` ran inline on the
         compio runtime — every must_sync=true append paid one fsync;
         under sustained 4K writes that capped at the syscall rate
         (~1000-2000/s on tmpfs, ~200/s on real SSD). F178's coalescer
         removes that ceiling: 1 fsync covers a coalesce window's
         worth of appends regardless of count.
  7. Advance extent.len
  8. Return (offset=start, end=start+payload_len)
```

No `write_lock` — appends are serialized by sequential processing within `handle_connection`. The `end` watermark guarantee: returning end=N means all data in 0..N is written.

Step 5 (commit-based truncation) is the key to consistency: it effectively replaces a traditional WAL by using the data files themselves as journals.

### Commit Protocol Explained

The `StreamClient` computes `commit = min(commit_length on all replicas)` before each append. Any replica that got ahead (e.g., partially acknowledged data before a crash) is truncated back to the consensus point on the next append. Per-node durability comes from the F178 Phase 1 fsync coalescer — the first must_sync waiter at an idle extent triggers `sync_data` immediately (event-driven, RocksDB group-commit style); subsequent appends that complete before the syscall returns are durable on the same syscall. After F150 Phase A- there is no separate WAL file.

**F178 Phase 2 — flush-time durability barrier (replaces F150 Phase B's rotation barrier).** Pre-F178 the partition layer's `start_write_batch` promoted `must_sync=true` on the rotation-triggering batch, putting the entire memtable's worth of fsync cost on one unlucky writer. Post-F178 every Put pays exactly one coalesce window (1-5 ms) regardless of rotation; the durability wait moves to `flush_one_imm` via `MSG_SYNCED_LENGTH`. The flush calls `await_log_synced_to(vp_extent_id, vp_offset)` BEFORE uploading the SST — **F227: ALL replicas** (was quorum-min) must report `last_synced >= vp_offset`. On the happy path this waits ≈ 0 because the append already acked all-replicas (and the coalescer fires every 2 ms while flush builds the SST in parallel); on the worst case it waits one coalesce window. Flush is background, so this is invisible to clients.

**F156 (SUPERSEDED by F227): majority quorum required.** F156 made `current_commit` require `success >= ⌊N/2⌋ + 1` before treating its min as authoritative. **F227 removed this — there must be NO quorum on the commit path.**

**F227: all-replica, no quorum.** This is a WAS stream layer: the append path is all-replica-ACK (`apply_completion` acks only when every replica wrote), so the committed length must be derived from ALL replicas, not a quorum subset. A subset `min` can sit BELOW the acked length (include a short / catching-up replica → the next append's `header.commit` truncates acked data on the up-to-date replicas → silent loss) or ABOVE it (exclude a member → keep un-acked data). The majority-quorum was the bug, not the fix. F227 changes:
- `current_commit` requires ALL replicas to respond (else `Err`); `ensure_tail_initialised` propagates that `Err` instead of the old `unwrap_or(0)` — seeding cursor 0 made the next append's `header.commit=0` truncate EVERY replica to 0 (catastrophic). `Ok(0)` (genuinely empty extent) still seeds nothing.
- `await_extent_synced_to` (flush barrier) requires ALL replicas synced past `vp_offset` (was `⌊N/2⌋+1` of `last_synced`); on a healthy cluster this is already satisfied because the append acked all-replicas.
- Manager-side seal/commit (`handle_stream_alloc_extent` / `handle_check_commit_length`) take `min` over the REACHABLE COMMITTED members only (catching-up = in-flight Recovery, excluded), requiring only `floor` of them to respond — **lenient seal-over-reachable, NOT strict-all-committed** (an EN can be down at seal time; the all-replica APPEND is the guarantee, not a strict seal). Do NOT revert to strict. See `crates/manager/CLAUDE.md` note 28 + the F227 seal-lenient note in `docs/ops.md` (WAL self-heal section).

F156's stated worry ("commit at a position only one replica holds") was really a durability concern (operating at RF=1), conflated with commit-length correctness; under all-replica-ACK the committed prefix is on every replica, so `min` over the reachable committed members is always ≥ acked. Liveness when a node is down is handled by the manager seal + operator fence → recovery (reconfigure the dead member out), not by lowering a quorum threshold. **Truncation of beyond-commit bytes stays correct — those are un-acked and must be removed; do not add a floor that keeps them.**

**Scope: F227 targets the WRITE / SEAL / commit-truncation path** (the silent-loss vector). `commit_length_for_extent` (the READ-path helper that resolves a `length=0` "to-end" read on an *open* extent) is intentionally LEFT as quorum-min: it neither seals nor truncates, so its worst case is a short read (surfaced as an error), not data loss — and reads should tolerate a replica being down (the whole point of replication), so requiring all-replica there would be an availability regression.

### Recovery (`require_recovery` RPC)

Triggered by the manager when a replica node fails:

1. Validates manager endpoint is configured, extent doesn't exist locally, no in-flight recovery for this extent.
2. Spawns background task `run_recovery_task` **with retry** (up to 10 attempts, 10s backoff between failures):
   - Fetches `ExtentInfo` from manager to get all replica addresses.
   - Calls `fetch_full_extent_from_sources`: iterates replicas (skipping self and failed node), reads the full extent via `copy_bytes_from_source` (CopyExtent RPC).
   - Truncates local file to 0, writes full payload, syncs.
   - Updates all atomics and persists metadata sidecar.
3. On completion, pushes `RecoveryTaskStatus` to `recovery_done` channel.
4. The `df` RPC (called periodically by the manager) drains `recovery_done` and reports completed tasks.
5. On max retries exhausted, removes from `recovery_inflight`; manager will re-dispatch on next loop.

### Re-Avali (`re_avali` RPC)

Used to bring a **sealed, replicated** extent's lagging replica up to date (e.g., after a node comes back online):
- **F206**: if `extent_info.ec_converted == true` → return OK immediately. RE_AVALI is a replicated-extent-only repair primitive. On an EC'd extent the local shard size equals `sealed_length / K`, so the `local_len >= sealed_length` check below would never short-circuit and the handler would call `fetch_full_extent_from_sources` — which allocates a `sealed_length`-sized `Vec<u8>` per peer attempt and (on the impossible success case) would write raw bytes over the local shard, corrupting EC. Missing-shard repair on an EC'd extent must route through `EXT_MSG_REQUIRE_RECOVERY` → `run_ec_recovery_payload` instead. The same OK response auto-heals pre-F206 buggy `avali` etcd values: the manager's `mark_extent_available` on the RE_AVALI OK response ORs in the parity-slot bit and persists.
- If local data >= `sealed_length`: already up to date, return OK.
- Otherwise: copy full extent from peers, truncate, rewrite, sync.

### Heartbeat & Df

- `heartbeat`: streams a "beat" payload every second (keep-alive for the manager).
- `df`: returns disk space info (currently hardcoded placeholder) + drains `recovery_done` to report completed recovery tasks. This is the mechanism by which the manager learns recovery finished.

### `serve_with_control` is fail-stop on bind conflict — DO NOT add dynamic port fallback

Pre-existing fail-stop behaviour (commit `63f5fea`) is intentional. PS has Ceph-style monotonic-next port fallback (`crates/partition-server/src/lib.rs`, partition `bind`) because PS ports are FUNDAMENTALLY dynamic — one port per partition (F099-K `base_port + ord`), partitions can split/merge/relocate, port density is high (8–32+ per PS), and the bound port is broadcast every open via `MSG_REGISTER_PARTITION_ADDR` so clients route to the actual port. Fallback engages without manager wire churn.

EN ports are FUNDAMENTALLY static. The address (`addr` + `shard_ports[]`) is stamped into etcd once by `autumn-op format` and held there for the node's lifetime. EN startup just opens the configured port; there is no per-session re-register. A dynamic fallback at EN startup would silently change the bound port while the manager still routes to the old one — every PS / sibling / manager RPC black-holes until the operator runs `autumn-op format` again.

To make EN fallback ACTUALLY useful would need: `bind_with_fallback` + per-shard actual-port channel back to main + `disk_uuid` sentinel read at startup + a fresh `MSG_REGISTER_NODE` carrying the actual ports + a manager-side `handle_register_node` extension that matches by `disk_uuid` (since the address string has changed) + a sibling-list recomputation broadcast back to every shard so cross-shard control-RPC forwarding doesn't misroute. ~300 lines spanning EN library, EN binary, and manager handler — and a new wire-level semantic for re-register matching.

EN port conflicts are an OPERATIONAL HYGIENE problem, not a runtime mechanism bug:
- Another tenant squatting → operator picks a different `--port`.
- Own old process not yet released → the existing 10 × 200 ms retry budget in `accept_loop_on` already covers it.
- Port falls inside `/proc/sys/net/ipv4/ip_local_port_range` → operator picks a port below 32768 (the typical lower bound) — well-known Linux network admin practice, not a fallback problem.

Fail-stop with the existing error message (`bind data listener <addr>: <io error>`) is the correct failure mode. The operator fixes the config and restarts. **Do not add port fallback here.**

### F109: Delete Extent (`MSG_DELETE_EXTENT = 11`)

Idempotent unlink for the physical `extent-{id}.dat` + `.meta` files
after the manager has confirmed `refs == 0`. Sent fire-and-forget by
the manager's `extent_delete_loop` once per replica.

```
handle_delete_extent(extent_id):
  1. F099-M shard ownership: if !owns_extent → forward to sibling shard.
  2. F139: if recovery_inflight.contains_key(&extent_id) → return
     CODE_PRECONDITION. Manager's extent_delete_loop retries (60 × 2 s
     budget); once recovery_inflight clears, next retry succeeds.
  3. extents.remove(&id) — pull the in-memory ExtentEntry out so any
     subsequent append fails fast with NotFound. Any pwritev that
     already took the file handle keeps its inode (POSIX preserves
     open fds across unlink); the data is meaningless because manager
     refs are 0.
  4. DiskFS::remove_extent_files(id):
       a. compio::fs::remove_file({base}/{hash:02x}/extent-{id}.dat)
       b. compio::fs::remove_file({base}/{hash:02x}/extent-{id}.meta)
       Both NotFound errors are downgraded to Ok(()) — the contract is
       idempotent so manager retries are safe.
  5. Returns CodeResp { code: CODE_OK | CODE_PRECONDITION | CODE_ERROR }.
```

**F139 recovery-vs-delete mutual exclusion (belt-and-braces):**
`handle_require_recovery` inserts into `recovery_inflight` before
detaching the background `run_recovery_task`. `handle_delete_extent`
(step 2 above) checks `recovery_inflight` and refuses with
`CODE_PRECONDITION` if set. This prevents two data-loss paths:

(a) **Resurrection**: `ensure_extent` in `run_recovery_task` opens with
`create:true`; if delete already unlinked the file, it silently creates
a fresh one, writes the peer-fetched payload, and saves a sidecar — an
orphan with no manager record until the 5-min reconcile sweep.

(b) **Write-to-unlinked-inode**: if `ensure_extent` ran before delete
(entry is still in `self.extents`), recovery holds an `Rc<ExtentEntry>`
whose open fd survives the unlink. All subsequent writes succeed against
the unlinked inode but the data evaporates on fd drop.

The manager-side guards (F139 in `recovery.rs` and `rpc_handlers.rs`)
are the primary prevention. The extent-node check is belt-and-braces for
the leader-failover scenario where `pending_extent_deletes` is lost
in-memory but `recovery_inflight` survives on the extent-node process.

### F109+F113: Startup + Periodic Orphan Reconcile

`ExtentNode::new` calls `spawn_reconcile_orphans_loop()` after
`load_extents()`. This detaches a background task on the node's
compio runtime that runs in two phases:

Runs immediately on spawn, then every 5 minutes. Each iteration
ships every locally loaded `extent_id` (filtered through
`owns_extent` in F099-M shard mode) to the manager via
`MSG_RECONCILE_EXTENTS = 0x31`; the manager returns the subset that
is no longer in `s.extents`. The node unlinks the corresponding
`.dat`/`.meta` files via the same `remove_extent_files` helper used
by `handle_delete_extent`.

A single iteration handles BOTH cold-start (cluster boot, manager
not yet leader → first attempt fails, next sweep retries) and
steady-state (catch orphans missed by the manager-push path). No
separate "startup retry" phase needed: a cold-boot race is just a
failed first iteration that recovers on the next tick. Worst-case
orphan-cleanup latency on cold boot is one sweep interval.

Pre-F113 this was an inline single-shot await with WARN-and-give-up
in `ExtentNode::new`: if the extent-node hit the manager before
its etcd lease was won (`ensure_leader` returns "not leader"), the
orphan files persisted until the next operator-driven reboot.

The periodic sweep is a safety net for any case where an extent's
manager refs hit 0 while the node was momentarily unreachable:
- `MSG_DELETE_EXTENT` retry budget exhausted (60 sweeps × 2 s ≈
  2 min on the manager side).
- Manager restart loses its in-memory `pending_extent_deletes` queue
  between leader hand-offs.
- Future EC conversion: a replica-shaped extent that gets converted
  to EC leaves original-replica `.dat` files behind on data nodes;
  `convert_to_ec` updates manager metadata and the periodic
  reconcile reaps the leftovers without a separate cleanup RPC.
- Any future code path that drops manager refs to 0 unilaterally.

Per-sweep failures are logged at WARN; the loop continues. No
give-up state.

**Per-sweep cost**: each iteration sends every locally-loaded
`extent_id` to the manager. The node has no way to filter to
"suspects" — it can't know which ids are garbage without asking.
The cadence is therefore generous (5 min) — for a backstop role,
freshness doesn't matter much; an orphan already escaped the
primary push path, a few extra minutes on disk is harmless. If a
node ever scales to 10k+ extents, switch to chunked rotation
(bounded id batches per sweep, rotating through the full set over
multiple sweeps).

### Concurrency control: `ConcurrencyController` (F194 → renamed F196 D-r7)

Cross-extent concurrency cap for the two memory-heavy background paths
on each shard: `handle_convert_to_ec` and `run_recovery_task`. Renamed
from `ExtentNodeGate` in F196 D-r7 to mirror PS's
`partition_server::ConcurrencyController` — same role on both sides
("how many can run at once before RAM is at risk"), same `acquire_*()`
+ RAII permit API.

```rust
pub struct ConcurrencyController {
    ec_convert_max: usize,
    recovery_max: usize,
    ec_convert_inflight: Cell<usize>,
    recovery_inflight:   Cell<usize>,
}
// Methods (async):
//   acquire_ec_convert() -> EcConvertPermit
//   acquire_recovery()   -> RecoveryPermit
// Permits decrement their counter on Drop.
```

One `Rc<ConcurrencyController>` per `ExtentNode` shard; both call
sites (`handle_convert_to_ec` and `run_recovery_task`) acquire from
the same Arc. The two counters are independent — saturating EC
convert doesn't block recovery and vice versa (verified by
`f194_concurrency_gate_tests::ec_convert_and_recovery_counters_are_independent`).

**Cell vs AtomicUsize.** The extent-node shard runs on a
single-threaded compio runtime — every acquire and release happens on
the same OS thread. `Cell<usize>` is sufficient, no cross-thread atomic
needed. PS's counterpart uses `AtomicUsize` because it's shared across
partition threads. The two implementations are otherwise identical in
shape (CAS loop replaced by `cur + 1` set, 50 ms backoff polling on
contention — fine relative to EC/recovery wallclock of seconds-minutes).

**Why not the per-extent locks alone?** `ec_conversion_locks` (F153)
only serialises requests for the SAME `extent_id`; `recovery_inflight`
(F109) only blocks duplicate requests for the SAME `extent_id`. Both
allow unbounded cross-extent fanout: a single manager
`recovery_dispatch_loop` tick finding 8 different extents to recover
spawns 8 detached `run_recovery_task` tasks, each holding ~`payload × 2`
memory through fetch + write. The concurrency controller caps that to
`recovery_max` (default 2) across all extents.

**Keep `recovery_max` aligned with the manager's
`RecoveryRateLimiter.max_per_target`** (default 2; F211-H/F224). Both
cap "concurrent recoveries landing on this EN" — manager throttles
DISPATCH (network fan-out), this caps EXECUTION (RAM). Defense-in-depth,
different processes (cannot be merged). If the manager's per-target cap
exceeds `recovery_max`, surplus dispatches just block in
`acquire_recovery()`'s 50 ms backoff until a permit frees. See
`crates/manager/CLAUDE.md` Programming Note 27.

### Concurrency vs rate limiting (and why there's no rate cap on EN)

EN has concurrency caps but **no bytes/s rate limit** today. This is
deliberate — the resource shapes differ from PS's:

| Operation | What dominates | Why concurrency cap is enough |
|-----------|----------------|------------------------------|
| `handle_convert_to_ec` | CPU (`spawn_blocking(ec_encode)`) + network (parallel WriteShard fanout) | Each in-flight encode holds ~`payload × 2` of GF(256) intermediates. cap=1 default keeps peak RAM at one encode. |
| `run_recovery_task` | Network (CopyExtent reads) + disk (full-extent write + sync) | F115 chunked I/O caps per-syscall buffer at 256 MiB; per-task RAM ~one chunk. cap=2 default keeps peak at 512 MiB. |

Both paths are bounded in time (seconds to minutes per extent) and
self-throttled by 3-replica fanout latency; the manager dispatches at
~2 s ticks, so the natural rate is ~30 extents/min/shard with cap=1.
A bytes/s cap could be layered in if production observes runaway
extent-node bandwidth (would mirror PS's `RateController.account_*`
shape on Mutex<RateState>) — track as F197+ if needed.

PS's situation is different: foreground writes hit the partition's
write path thousands of times/sec at small batch sizes — that's where
rate limits (bytes/s + iops) matter. EN's heavy paths are bulk
operations measured in seconds, where concurrency = RAM cap is the
load-bearing constraint.

### Configuration

| CLI flag | Env var | Default | Range |
|----------|---------|---------|-------|
| `--ec-convert-parallelism` | `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM` | 1 | [1, 16] |
| `--recovery-parallelism` | `AUTUMN_EXTENT_RECOVERY_PARALLELISM` | 2 | [1, 16] |
| `--ec-stripe-bytes` | `AUTUMN_EXTENT_EC_STRIPE_BYTES` (test override) | 64 MiB | [1 MiB, 1 GiB] |

The two parallelism clamps live on `ExtentNodeConfig::with_*` builder methods;
the values flow through `ExtentNodeConfig` into `ConcurrencyController::new(ec,
recovery)` at `ExtentNode::new`.

`--ec-stripe-bytes` is the chunked EC-convert stripe size — a process-global
(all shards share it), set via `set_ec_encode_stripe_bytes` (OnceLock,
first-call-wins; the binary applies it in `main` before any convert). Precedence
**flag > env > 64 MiB default** (the env is a test override that the chaos/e2e
harness uses to force multi-stripe on small extents). Peak EC-convert RAM =
`(K+M) × stripe`; bigger stripe = fewer `WriteShard` RPCs + `sync_data`s
(faster convert) at higher peak RAM, smaller = lower RAM at more I/O ops. Max
1 GiB keeps a single stripe's `WriteShard` well under the frame `payload_len:
u32` ceiling.

---

## StreamClient — Client Side

Used by `PartitionServer` and tests. Holds autumn-rpc connections to extent
nodes via `ConnPool`. Manager calls are currently stubbed (F044 scope).

### Connection & Ownership

```rust
StreamClient::connect(manager_endpoint, owner_key, max_extent_size, pool)
    -> Rc<StreamClient>           // R4 step 4.3: returns Rc via Rc::new_cyclic
```
- `manager_endpoint` supports **comma-separated** addresses for multi-manager HA:
  `"host1:9001,host2:9001,host3:9001"`.
- Tries each manager to `acquire_owner_lock`, skipping `NotLeader` responses.
- All subsequent manager RPCs use `self.manager_addr()` which returns the current leader.
- On any manager RPC failure, `rotate_manager()` switches to the next address (round-robin).
- `owner_key` should be unique per logical writer (e.g., `"ps/{ps_id}/partition/{part_id}"`).
- **Return type change (4.3)**: `connect` / `new_with_owner_epoch` now return `Rc<StreamClient>`.
  The `Rc` is needed so the internal per-stream worker tasks can hold a
  `Weak<StreamClient>` for the exit-removal guard without creating an Rc cycle
  that would prevent shutdown. Public API methods still take `&self`, so
  callers deref `Rc<StreamClient> → &StreamClient` transparently.

### Append Data Flow (R4 step 4.3 — per-stream SQ/CQ worker)

```
append*(stream_id, payload, must_sync):
  1. stream_worker_sender(stream_id): look up or lazily spawn the per-stream
     compio task (returns a cloned mpsc::Sender<StreamSubmitMsg>, cap=256).
  2. ensure_tail_initialised(stream_id): first caller holds the per-stream
     init mutex, loads tail from manager + queries commit_length on replicas,
     then sends ResetTail + SeedCursor to the worker. Subsequent callers find
     initialized=true and skip.
    - If the manager-reported tail is already sealed (`sealed_length > 0`),
     `ensure_tail_initialised` allocates a fresh extent immediately instead
     of seeding the worker with the sealed tail. This is load-bearing after
     partition split / stream duplication: the child stream may inherit a
     sealed tail, and waiting for append-time failure leaves descendant
     compaction stuck behind `LockedByOther` / overlap-clearing failures.
  3. Retry loop (MAX_ALLOC_PER_APPEND=3):
     a. Send Append msg — parks on bounded channel under overload.
     b. Await ack_rx.
     c. Ok → if result.end ≥ max_extent_size, alloc_new_extent + ResetTail
        (preemptively rolls the extent before the next call). Return.
     d. Err "not found on replica" → alloc_new_extent + ResetTail. Retry.
     e. Err "LockedByOther" → propagate immediately (PS should self-evict).
     f. Err soft (retry ≤ 2) → sleep 100ms, reload tail, ResetTail. Retry.
     g. Err hard → alloc_new_extent + ResetTail. Retry.
```

### Per-stream worker (single-owner actor)

```
┌─ stream_worker_loop (ONE compio task per stream_id) ─────────────┐
│                                                                 │
│  OWNS: StreamAppendState                                        │
│     - tail: Option<StreamTail>                                   │
│     - commit: u32            (contiguous-prefix high-water)     │
│     - lease_cursor: u32                                          │
│     - pending_acks: BTreeMap<offset, end>                        │
│     - in_flight: u32                                             │
│     - poisoned: bool                                             │
│  OWNS: inflight: FuturesUnordered<InflightFut>                   │
│     cap = AUTUMN_STREAM_INFLIGHT_CAP (default 32)                │
│     holds in-flight 3-replica join futures                       │
│  RECV: submit_rx: mpsc::Receiver<StreamSubmitMsg>                │
│                                                                 │
│  SQ side (launch_append):                                       │
│     - lease offset range (state.lease)                           │
│     - build AppendReq header; header.commit = offset (Option A)  │
│     - fire pool.send_vectored to each replica IN PARALLEL via    │
│       futures::future::join_all over the 3 per-replica futures   │
│       (F099-B; each replica's writer_task is single-writer so    │
│        per-replica TCP byte order = lease order on that socket;  │
│        inter-replica fanout order is irrelevant).                │
│     - push the 3-replica join future into inflight               │
│     - return to event loop; no await on any receiver             │
│                                                                 │
│  CQ side (apply_completion):                                    │
│     - pop ready InflightResult from FU (or drain on demand)      │
│     - parse 3 frames: success / NotFound / LockedByOther / err   │
│     - success → state.ack; reply Ok(AppendResult) via ack_tx     │
│     - error → state.rewind_or_poison; reply Err(...) via ack_tx  │
│                                                                 │
│  Loop (per iteration):                                           │
│     1. while let Some(Some(r)) = inflight.next().now_or_never()  │
│          → apply_completion (opportunistic CQ drain)             │
│     2. if n_inflight == 0  → await submit_rx.next()              │
│        elif at_cap         → await inflight.next() (back-pressure) │
│        else                → select(submit_rx.next,              │
│                                    inflight.next())              │
│           Left  (SQ wins) → apply message                        │
│           Right (CQ wins) → apply_completion                     │
│                                                                 │
│  Messages:                                                       │
│     Append { payload_parts, must_sync, revision, ack_tx }        │
│     ResetTail { tail }        ← public API sends after alloc     │
│     SeedCursor { cursor }     ← seeds commit/lease_cursor        │
│                                  to non-zero on tail init        │
│     Shutdown                                                     │
└─────────────────────────────────────────────────────────────────┘
```

**No external Mutex**: the Arc<Mutex<StreamAppendState>> of R3 is removed.
All state mutations happen inside the worker task. The public API talks to
the worker via bounded mpsc + per-op oneshot.

**Retry is in the public API**, not the worker (Option A from the R4 spec).
The worker is a pure stateful single-op executor; the public API handles
alloc_new_extent + ResetTail on NotFound / soft error / extent-full.

**Tail invalidation is explicit**: after any alloc_new_extent, the public
API sends `ResetTail` to the worker BEFORE the next Append. Because the
retry loop awaits the previous ack before resetting, in_flight is always 0
at the reset point — no old-extent leases stranded on the new extent.

**SeedCursor** is used on stream first-use to initialise `commit = lease_cursor`
to the replica-min `commit_length` when the stream's tail extent already
has pre-existing data. Without it, the first append on a resumed stream
would try to overwrite committed bytes.

### Back-pressure, lifecycle, error paths

| Concern | Behaviour |
|---------|-----------|
| Submit mpsc cap | 256 per stream. Parked callers wake as worker drains. |
| Inflight cap | `AUTUMN_STREAM_INFLIGHT_CAP` (default 32). `at_cap` branch does CQ-only. |
| Worker lifecycle | Spawned lazily on first append* to that stream_id. Exits on channel close or `Shutdown` msg, after draining all inflight futures for a final ack. |
| Worker removal | On worker exit, a `WorkerRemovalGuard` drops and removes the stream's Sender from `stream_workers`. Uses `Weak<StreamClient>` to avoid Rc cycle. Next `append*` spawns a fresh worker. |
| StreamClient drop | All senders drop → channels close → all workers drain + exit cleanly. |
| `LockedByOther` | Propagated immediately; PS owner should self-evict. |

### Caching

| Cache | Key | Value | Invalidated on |
|-------|-----|-------|----------------|
| `stream_workers` | stream_id | `mpsc::Sender<StreamSubmitMsg>` | Worker exits (removal guard), StreamClient drop |
| `stream_init_locks` | stream_id | `Rc<futures::lock::Mutex<bool>>` | Never (cheap, lives with StreamClient) |
| `nodes_cache` | node_id | address | On replica lookup failure (lazy refresh) |
| `extent_info_cache` | extent_id | `ExtentInfo` | On replica lookup failure |

`nodes_cache` + `extent_info_cache` use `DashMap` for lock-free concurrent access.
`stream_workers` + `stream_init_locks` use `RefCell<HashMap<_,_>>` — the
StreamClient is used from a single compio thread per-caller so RefCell is
sufficient (and cheaper than DashMap).

### Other Public Methods

| Method | Purpose |
|--------|---------|
| `append_batch(stream_id, blocks[], must_sync)` | Concatenate multiple blocks, single append |
| `append_batch_repeated(stream_id, block, count, must_sync)` | Repeat one block N times |
| `read_bytes_from_extent(extent_id, offset, length)` | Read from extent; replication (F258): SEALED extents rotate the start replica by `(extent_id, offset)` hash so read IO spreads across all replicas (pre-F258 everything hit replica[0]); open-tail keeps replica[0]-first; failover walks the remaining replicas in rotated order. Optional hedged read (`set_read_hedge_ms`, default 0=off, `autumn-ps --read-hedge-ms` / cluster.sh `AUTUMN_READ_HEDGE_MS`): if the first replica hasn't answered within the window, race the second and take the first Ok (eversion-stale still fails fast; loser future drop is cancel-safe). **chunked at `AUTUMN_STREAM_READ_CHUNK_BYTES` (default 256 MiB)** so reads >2 GiB don't trip the per-syscall pread ceiling on macOS (INT_MAX) / Linux (0x7ffff000); EC: parallel shard reads with decode (per-shard size already bounded). `length=0` resolves to-end via `sealed_length` (sealed extents) or `commit_length_for_extent` (open extents) before chunking. |
| `read_committed_bytes_from_extent(extent_id, offset, length)` | F261: like the plain read but `length` is CLAMPED to the COMMITTED end (sealed → `sealed_length`; open tail → min-replica commit probe per call) before issuing. For chunked scanners (PS WAL replay): a replica's file legitimately holds speculative bytes past the committed end (an ahead replica is never truncated back after seal), so the plain explicit-length read only short-reads at the SERVING replica's local length — a scanner stopping on `got < want` walks past the seal and trips `StaleVpOffset` on the next chunk. Sealed + `offset > sealed_length` still errors loudly (checkpoint-past-seal corruption is never masked as end-of-scan); `offset == committed end` returns empty = clean stop. |
| `extent_read_descriptor(extent_id)` | F259: `(eversion, replica addrs)` for a client direct read; REFUSES EC-converted extents (shard bytes ≠ value) |
| `read_extent_value_direct(pool, addr, …)` (free fn) | F259: one-shot ZC EN read for MSG_GET_REDIRECT holders (MSG_READ_BYTES_ZC + call_into_pooled; short read = Err) |
| `read_last_extent_data(stream_id)` | Read last non-empty extent of a stream |
| `punch_holes(stream_id, extent_ids[])` | GC: remove extents from stream |
| `truncate(stream_id, extent_id)` | Remove all extents before extent_id |
| `get_stream_info(stream_id)` | Query StreamInfo from manager |
| `get_extent_info(extent_id)` | Query ExtentInfo from manager |
| `multi_modify_split(req)` | Forward partition split to manager |
| `invalidate_stream(stream_id)` | Discard cached worker + init-lock for a stream; next append re-loads the tail from manager and spawns a fresh worker. Used after split to prevent appending beyond the sealed tail. |

---

## Programming Notes

1. **Always pass the correct `revision`** — passing 0 or a stale revision will cause `CODE_LOCKED_BY_OTHER` from ExtentNode (propagated as immediate non-retried error by StreamClient). The revision is set at `StreamClient::connect` time.

2. **Eversion changes on seal** — if the manager seals an extent (e.g., during split or extent rolling), the eversion is bumped. The next append will see a mismatched eversion, fetch the updated ExtentInfo, and handle accordingly.

3. **Parallel 3-replica fanout (F099-B)** — `launch_append` fires the 3 per-replica `pool.send_vectored` futures concurrently via `futures::future::join_all`. Each per-replica future awaits its own RpcClient submit channel independently, so one slow/back-pressured replica doesn't serialise the others. Per-replica TCP byte order is still preserved because each RpcClient runs a single-writer `writer_task` (R4 step 4.1) — the fanout order across replicas is irrelevant because every replica is independent. The `AppendResp.offset/end` consistency check in `apply_completion` still enforces that all replicas agree on the file-level offset.

4. **`must_sync` cost (post-F178)** — the per-extent fsync coalescer is
   event-driven (RocksDB group-commit style). The flag controls whether
   the caller WAITS for durability:
   - `must_sync=true`: register a `(end_offset, oneshot)` waiter on the
     coalescer; await the receiver. Resolves Ok when the next coalesced
     `sync_data` covers `end_offset`. The first waiter at an idle extent
     triggers `sync_data` immediately (no timer floor). Typical wait =
     one fsync syscall (~1 ms tmpfs / 5-15 ms NVMe).
   - `must_sync=false`: advance `pending_fsync` and return immediately.
     The coalescer still picks up these bytes on its next tick and they
     become durable; the caller just doesn't wait.
   Pre-F178 `must_sync=true` was `file.write_at` + inline `file.sync_data()`
   per request; under heavy load this capped throughput at the kernel's
   fsync rate (~200/s on real SSD, ~2000/s on tmpfs). The coalescer
   removes that ceiling — 1 syscall per coalesce window covers ALL
   pending appends. F150 Phase B's load-bearing property (sync_data
   covers prior must_sync=false bytes) is preserved structurally because
   sync_data is whole-file. F178 Phase 3 removed `--nosync` from
   clients, so PS->extent_node always passes `must_sync=true`; the flag
   is kept on the wire for back-compat. SSTable data doesn't need an
   extra wait beyond append's coalescer wake.

5. **StreamClient is always held as `Rc<StreamClient>`** — constructors return `Rc<Self>` (via `Rc::new_cyclic`) so per-stream workers can hold `Weak<StreamClient>` for the removal-guard. Callers clone the `Rc` to share. Public API methods take `&self`, so `sc.append(...)` works transparently.

6. **EC for all three streams (post-fix)** — EC is a per-stream property. All three streams (`log_stream`, `row_stream`, `meta_stream`) can be EC-converted on seal. Replication factor is fixed at **3** while open; EC default keeps `M=1` parity and grows `K = N - 1` capped at 4 (so N=4 → 3+1, N=5 → 4+1, N≥6 → 4+1; cap bounds RS decode cost). `log_stream`'s arbitrary VP `(extent, offset, length)` sub-range reads are handled by `ec_subrange_read`'s generalised N-shard parallel scatter — one `read_shard_from_addr` per touched data shard, stitched in order; single- and two-shard cases fall out as plan-of-1 / plan-of-2 special cases. **Bug history (2026-04-27 production crash)**: with log_stream EC'd by default, three independent bugs combined to crash PS:
    - `cluster.sh` defaulted log_stream EC to a fixed 3+1 / 2+1 shape regardless of N — fine semantically, but exposed the next two bugs.
    - `ec_subrange_read`'s "two-shard fast path" only checked `end_shard < data_shards`. On K=3, a 64 MiB GC chunk read (start_shard=0, end_shard=2) entered this branch and joined shard 0 + shard 2's prefix while silently skipping shard 1. The server's CODE_OK-with-short-payload behaviour produced a buffer of the right total length but with the middle ~15 MiB replaced by shard 2's prefix → GC's record-stream parser surfaced `trailing bytes did not form a complete record`.
    - `handle_stream_alloc_extent` had no idempotency guard: when a writer's soft-error retry called `alloc_new_extent(stream_id, 0)` on an *already-sealed* tail (post-EC), the manager re-ran the seal block — re-queried commit_length on each replica (which after `write_shard_local` returned only `shard_size`), took the min, and CLOBBERED `tail.sealed_length` from the original payload size down to `shard_size`. Existing VPs in `[shard_size, original_payload_len)` were now past sealed_length, and `ec_read_full_and_slice` panicked the partition thread with `range start index N out of range for slice of length L`.

    Fixes:
    - cluster.sh: log + row default to `3+1` (N=4) / `4+1` (N≥5), with `M=1` parity, `K` capped at 4.
    - `ec_subrange_read` rewritten as generalised N-shard parallel scatter; out-of-range offset (`start_shard >= data_shards`) routes to `ec_read_full_and_slice` whose extracted `ec_slice_decoded` helper returns `Err` (not panic) on `offset > full_payload.len()`. Saturating-sub on `read_len` prevents unsigned wrap. Regression tests in `mod ec_slice_tests` (`client.rs`).
    - `handle_stream_alloc_extent` (manager): if `tail.sealed_length > 0`, skip the entire seal block (no commit_length re-query, no overwrite of `sealed_length / eversion / avali`). New-tail allocation still proceeds. This is the load-bearing fix: it preserves the manager's seal-time `sealed_length` against post-EC re-seal corruption.

    **F204 (2026-05-15) — historical-residue diagnostic for clusters that ran BEFORE the 2026-04-27 fix**: the manager-side fix prevents NEW corruption but cannot self-detect etcd values that were shrunk in the pre-fix window. SSTs from that era still hold `ValuePointer (extent, offset, length)` triples where `offset + length > manager.sealed_length`. Reading such a key still produces the slice-bounds error, but now it surfaces as a **structured sentinel** (`StaleVpOffset` in `crates/stream/src/client.rs`) carried through the anyhow chain, so the PS read path can map it to `StatusCode::FailedPrecondition` instead of `Internal`. The sentinel's Display string is a **stable wire contract** for Python operational tooling:

    ```
    stale_vp_offset_past_sealed_length: extent=<EID> offset=<OFF> length=<LEN> sealed_length=<SEAL>
    ```

    Tooling regex consumers (`/get` failures, log scrapers) MUST treat the `stale_vp_offset_past_sealed_length:` prefix + the 4-field ordering as load-bearing. Any future format change is a wire-incompatible event. The 4-field tuple is enough to identify the broken key (via the SST that holds it — which the OP tool can pin down by re-reading the same key under `client get` and matching the surfaced `extent_id`) and to decide remediation: **data is permanently gone (EC shards were physically truncated during the historical re-encode); the only cleanup is `client del <key>` followed by major compact to drop the VP entry**. Bytes past sealed_length are not recoverable — `ec_subrange_read`'s reconstruction (F200) can only rebuild bytes that were originally encoded, not bytes that no longer exist on any shard.

    Why no automated repair: manager has no "previous correct sealed_length" to compare against — etcd stores only the current revision. SST inspection is PS-local and expensive (would require an open-time walk of every block; F204 explicitly skipped this). Cleanup is OP-driven via Python scripts that consume the `stale_vp_offset_past_sealed_length:` wire signal.

7. **EC offset semantics** — In EC mode, `AppendResult.offset/end` are shard-level byte offsets. Each shard has `shard_size(payload_len, data_shards)` bytes. Upper layers treat these as opaque — they pass them unchanged to `read_bytes_from_extent`. The EC read path handles the decode transparently.

8. **EC shard index = position in replicates++parity** — `replica_addrs_from_cache` chains `replicates` then `parity` node IDs. Shard index `i` corresponds to address `i` in this combined list. The encode output shard `i` is sent to the `i`-th node. The recovery `replacing_index` uses the same ordering.

9. **Commit tracking is local, not per-append RPC** — `state.commit` is a plain `u32` (not `Option`), matching Go's `sc.end` pattern. It starts at 0 and is updated to `appended.end` after each successful append. After allocating a new extent, it resets to 0. `current_commit()` (which RPCs all replicas) exists for partition load time only, never in the hot append path.

10. **Extent allocation is capped per append** — `append_payload` allows at most 3 new extent allocations per single append call (`MAX_ALLOC_PER_APPEND`). This prevents runaway empty extent creation if appends persistently fail.

11. **ConnPool is single-kind (post-F093)** — `ConnPool` keys by `SocketAddr` alone; each address owns one sequential `RpcConn` on a `Rc<RefCell<Option<RpcConn>>>` (take/put pattern; if taken, a fresh connection is opened on the fly). Historical note: F087-bulk-mux introduced a `PoolKind::{Hot, Bulk}` distinction so 128 MB SSTable uploads wouldn't head-of-line-block small WAL frames on the same socket. F088 moved flush to a dedicated P-bulk OS thread with its own ConnPool + StreamClient, so the P-log SC now only carries WAL (+ rare compact writes) and the shared-socket HoL scenario no longer exists. F093 removed `PoolKind`, `set_stream_kind`, and `kind_for` as dead code.

12. **Chunked reads for >2 GiB extents (F105)** — `read_bytes_from_extent` splits requests larger than `AUTUMN_STREAM_READ_CHUNK_BYTES` (default 256 MiB) into multiple per-replica RPCs and concatenates the results. Without chunking, a single `pread` of 3 GiB on the extent_node returns `EINVAL` (errno 22) — macOS caps at `INT_MAX` (~2 GiB) and Linux at `0x7ffff000`. The pre-F105 GC + recovery path slurped sealed extents in one shot via `read_bytes_from_extent(eid, 0, sealed_length)`; once a sealed log_stream extent grew past 2 GiB, GC got stuck retrying every 30 s ("rpc status Internal: Invalid argument (os error 22)") and recovery would refuse to open the partition on the next restart. `length=0` ("to end") resolves the byte count via `ExtentInfo.sealed_length` for sealed extents or `commit_length_for_extent` (min-replica) for open extents, then chunks. EC reads route to the per-shard path (`ec_subrange_read` / `ec_read_full`); **`read_shard_from_addr` now chunks internally at `read_chunk_bytes` (u64-offset widening)** — this is load-bearing once `max_extent_size` can exceed 4 GiB: an EC shard is `sealed_length / data_shards`, so a 16 GiB / K=3 extent has ~5.33 GiB shards, and a single `MSG_READ_BYTES` for a full shard would overflow the response frame's `payload_len: u32` (≤ 4 GiB; `frame.rs::MAX_PAYLOAD_LEN`). Unlike the replicated branch (which `read_with_layout` chunks), EC dispatch goes straight to `ec_subrange_read`, so the per-shard `read_shard_from_addr` is the single EC choke point that bounds each RPC to ≤ `read_chunk_bytes` (256 MiB). At the old 3 GiB default shards were ~1 GiB (fit the frame), so this only became reachable when `max_extent_size` rose to 16 GiB. Test override: integration tests set `AUTUMN_STREAM_READ_CHUNK_BYTES` to small values (e.g. 1024) to exercise the chunked path without writing multi-GiB extents.

13. **Chunked local-file I/O for >2 GiB extents (F115)** — The F105 fix (note 12) only covered the `StreamClient` RPC path. The `ExtentNode` server-side local-file operations had the same EINVAL exposure on all full-extent I/O paths. Fixed by two helpers in `extent_node.rs` (`FILE_IO_CHUNK_BYTES = 256 MiB`):
    - `file_pread_chunked` — used by `handle_convert_to_ec`, `handle_read_bytes`, `handle_copy_extent`
    - `file_pwrite_chunked` (splits via `Bytes::split_to`, O(1) no-copy) — used by `run_recovery_task`, `handle_re_avali`, `write_shard_local`
    Both fast-path the common case: single syscall when payload ≤ 256 MiB; only loop when larger. Any new full-extent local-file read or write **must** use these helpers — never call `file_pread`/`file_pwrite` directly with a `sealed_length`-sized buffer.

14. **Sealed duplicated tails must allocate on init** — after `multi_modify_split`,
    the new child stream can inherit a tail extent that the manager has already
    sealed at the split point. `StreamClient::ensure_tail_initialised()` must
    treat this as "allocate fresh tail now", not "seed the worker and discover
    the seal later on append". The latter path can wedge descendant compaction:
    the first row_stream append hits the old duplicated tail, extent-node
    fencing surfaces `LockedByOther`, major compaction never clears
    `has_overlap`, and the next split stays blocked forever.

14. **Read-side eversion freshness after EC conversion (F116 + F119-C)**
    — The manager flips a sealed extent to EC by (a) sending
    `EXT_MSG_CONVERT_TO_EC` with the new `eversion` field, then (b)
    `apply_ec_conversion_done` rewriting `replicates` / `parity` and
    assigning the same eversion to etcd. Every target node bumps its
    own `entry.eversion` from inside `write_shard_local`, so the
    manager and all shard hosts agree on the post-EC eversion the
    moment the coordinator returns OK. `StreamClient` passes its
    **cached** `ex.eversion` in every `ReadBytesReq` (formerly
    hard-coded to 0). When a stale-cache client reads an EC-converted
    extent, the server returns `CODE_EVERSION_MISMATCH` (instead of
    letting the read silently scrape bytes from a shrunken shard
    file). The client side surfaces this as a private `EversionStale`
    `anyhow` sentinel; the top-level `read_bytes_from_extent` runs a
    2-attempt loop that calls `invalidate_extent_cache(extent_id)` and
    refetches `ExtentInfo` from the manager once.
    `read_replicated_with_failover` and `ec_subrange_read` both
    fail-fast on `EversionStale` rather than walking the remaining
    stale replicas — every replica reports the same mismatch by
    construction.

    **F119-C tightening — closes the eversion=0 silent-skip loophole.**
    Pre-F119-C the server-side check was
    `if req.eversion > 0 && req.eversion < ev`. The `> 0` clause was
    documented as a "pass 0 to skip" sentinel, but it silently let
    through a stale-cached `eversion=0` populated by
    `load_stream_tail` / `alloc_new_extent_once` while the extent was
    still open (eversion=0 in the cache, even after manager+server
    bump it past 0 via split + EC conversion). Concrete bug: a 14 MiB
    log_stream value at offset 398 MiB inside an extent EC-converted
    to 3+1 (shard_size ≈ 402 MiB) returned `min(14 MiB, 402 MiB - 398
    MiB) = 3.9 MiB` from data shard 0; the client's
    `read_replicated_with_failover` treated it as success and
    surfaced it as `logStream value short: need 14456954 bytes, got
    3909555`. The fix is enforced in **both** `handle_read_bytes`
    (the dispatch fallback) and `build_read_future` (the production
    batched path): drop the `> 0` clause; reject any `req.eversion <
    ev` with a CODE_EVERSION_MISMATCH **response** (not a frame-level
    error — the batched path previously emitted
    `FailedPrecondition`, which never reached the client's
    `is_eversion_stale` retry detection). The client-side 2-attempt
    retry loop then evicts the cache, refetches fresh `ExtentInfo`,
    and the second attempt routes through `ec_subrange_read`.

    **Invariant:** `entry.eversion` defaults to 1 on alloc
    (matches `MgrExtentInfo { eversion: 1, .. }`), so any
    `req.eversion = 0` is by construction stale. The only callers
    that pass 0 in production are bench/test fixtures that hand-roll
    fresh ExtentEntry state with `entry.eversion = 0`.

    **F119-D: convert_to_ec must be idempotent on the coordinator.**
    A separate corruption path (root cause: dispatch-loop candidates
    list contained one extent_id twice when the extent was CoW-shared
    across two streams post-split) caused
    `EXT_MSG_CONVERT_TO_EC` to fire twice on the same extent. The
    first call correctly encoded the original payload into K shards
    of `shard_size(original, K)` bytes. `write_shard_local` then
    shrunk every replica's local file to that shard size. The second
    call on the coordinator re-entered `handle_convert_to_ec`, found
    `entry.sealed_length > 0` (from the first call), skipped the
    "applied seal from manager" branch, read `entry.sealed_length`
    bytes (= the K=1 shard) from local, and re-encoded **that** as if
    it were the original payload — producing sub-shards of
    `shard_size(shard_size(original, K), K) ≈ original / K²` bytes.
    Manager state (sealed_length, ec_converted, eversion) didn't
    change — `apply_ec_conversion_done` is idempotent for those
    fields — so reads silently scraped sub-shard bytes. Surfaced as
    `logStream value short: need 11979455 bytes, got 1423859` on a
    cross-shard VP read, and as `ec_read_full_and_slice: offset 3951
    past decoded payload len 2636 (manager sealed_length=7902)` on
    SST replay during partition open after restart.

    Fix layered at two points:
    - Manager (`crates/manager/src/recovery.rs`
      `ec_conversion_dispatch_loop`): dedup candidates by
      `extent_id` via `HashSet`. The primary fix.
    - Coordinator (`handle_convert_to_ec`): defense-in-depth — if
      `entry.eversion >= req.eversion && sealed_length > 0 &&
      entry.avali > 0`, the extent is already EC-converted at this
      eversion. Return `CODE_OK` without re-encoding. Any future
      bug that re-dispatches a converted extent then becomes a
      no-op instead of a corruption.

16. **EC dispatch keys on `ExtentInfo.ec_converted`, NEVER on
    `parity.is_empty()` (F118)** — The manager pre-fills `parity` for
    every extent allocated via `stream_alloc_extent` on an EC stream
    (`crates/manager/src/rpc_handlers.rs`), so an open / pre-conversion
    extent has `parity != []` even though it still holds full
    replicated data on every K+M node. Only after the
    `ec_conversion_dispatch_loop` fires `apply_ec_conversion_done` on
    a *sealed* extent does the data physically split into K data + M
    parity shards; that's also when `ec_converted` flips to `true`.
    Routing a pre-conversion extent through `ec_subrange_read` would
    compute `shard_size` from `sealed_length=0` and panic on the
    per-shard slice with `range start index … out of range for slice
    of length …` — and the underlying data isn't EC-shaped yet
    anyway. Read-side dispatch (`client.rs::read_with_layout`) and
    recovery-side dispatch (`extent_node.rs::run_recovery_task`) both
    branch on `ec_converted`. The display path
    (`autumn_client::Info`) uses the same flag, so open extents
    correctly render as `replicas=[…all K+M nodes…]` instead of
    `EC(K+M)`. **Invariant:** `ec_converted == true` implies
    `sealed_length > 0` (the conversion loop refuses to act on open
    extents at `recovery.rs:377`). Any future code that tags an
    extent as EC must preserve this — never set `ec_converted` on an
    open extent.

    **F153: per-extent EC conversion serialisation lock on the coordinator.**
    F119-D's idempotency guard fires post-hoc — the eversion bump is the LAST
    step of the 2PC, so during the window between dispatch-1's start and
    dispatch-1's `commit_shard_local`, a concurrent dispatch-2 sees
    `entry.eversion < req.eversion` and proceeds. Two concurrent encodes race
    on the same `.ec.dat` staging file, producing the F119-D corruption shape.
    The manager-side `ec_conversion_inflight` set is purely in-memory and is
    lost on leader failover, so a deposed leader's in-flight conversion can
    race with the new leader's redispatch from the 5 s
    `ec_conversion_dispatch_loop`. F153 adds a per-extent
    `Rc<futures::lock::Mutex<()>>` map on `ExtentNode`, acquired at the start
    of `handle_convert_to_ec` and held across the entire prepare + commit
    phase. The second concurrent dispatch awaits the first; when the first
    releases, the second re-runs the F119-D guard UNDER the lock and exits as
    a no-op. Defense-in-depth against the manager-side double-dispatch —
    F119-D becomes load-bearing AFTER the lock. Pattern mirrors
    `client.rs::stream_init_locks`. Test: `f153_ec_lock_tests`.

15. **CPU-bound work MUST run on the blocking pool, not the compio
    event loop (F117)** — Reed-Solomon `ec_encode` / `ec_decode` /
    `ec_reconstruct_shard` each take 100–300 ms on a 128 MiB extent.
    All three callers wrap the call in
    `compio::runtime::spawn_blocking(move || …)` so the GF(256)
    polynomial math runs on a dedicated OS thread:
    - `extent_node.rs::handle_convert_to_ec` (encode of a sealed
      extent into k+m shards before WriteShard fanout).
    - `extent_node.rs::run_ec_recovery_payload` (reconstruct the
      single shard a recovering node should hold).
    - `client.rs::ec_read_full` (decode the original payload from
      k+m shards on the EC fallback / full-extent read path).
    Without this offload the extent-node compio runtime stalls on
    encode while the user's append/read RPCs queue up, and the
    PS-side P-log/P-bulk threads stall on decode while a row_stream
    fallback read is in flight. Pattern matches
    `partition-server::flush_one_imm_async` which has wrapped
    `build_sst_bytes` in `spawn_blocking` since F088. **Any new
    CPU-bound work in this crate (RS math, large CRC, large
    compression, big sort) MUST be wrapped in `spawn_blocking` —
    never call directly from a compio task.** The error plumbing
    pattern is double `.map_err`+`?` to handle (i) the join-time
    panic-Box from `JoinHandle<T> = Task<Result<T, Box<dyn Any +
    Send>>>` and (ii) the inner `Result` returned by the erasure
    function itself. Out of scope: WAL CRC32C
    (`wal.rs:172`/`:271`) on must_sync small writes — bounded at
    ≤ 2 MiB per call, amortised by `write_batch`.

17. **F121 dead-replica recovery: closed-aware pool + append fanout
    timeout.** When an extent-node dies (SIGTERM/SIGKILL) the kernel
    sends FIN to the PS-side TCP socket. autumn-rpc's `read_loop`
    sees EOF and clears `pending`, but the `Rc<RpcClient>` stays in
    `ConnPool` until somebody asks for it again. Pre-F121 the next
    `pool.send_vectored("dead.addr", …)` returned that dead client;
    `client.send_vectored` happily inserted a fresh
    `(req_id → tx)` into `pending`, but with no read_loop alive to
    dispatch responses, the caller's `rx.await` hung forever.
    Three layered fixes:
    - autumn-rpc `RpcClient` exposes `is_closed()` (set true on
      `read_loop`/`writer_task` exit, before `pending.clear()`);
      every `send_*` early-returns `ConnectionClosed` when set.
    - `ConnPool::get_client` skips and reconnects when the cached
      entry's `is_closed()` is true; `send_vectored` evicts on
      submit error (matches existing `call*` semantics).
    - `launch_append` wraps each per-replica response receiver in
      `compio::time::sleep + futures::future::select` with
      `append_fanout_timeout()` (env
      `AUTUMN_STREAM_APPEND_TIMEOUT_MS`, default 5 s, clamped to
      [200 ms, 60 s]). Mirrors Go autumn's
      `streamclient.go:770` `context.WithTimeout(ctx, 5*time.Second)`.
      `Elapsed` translates to a soft error that
      `apply_completion` already classifies, so the existing
      `append_payload_segments` retry loop escalates to
      `alloc_new_extent` exactly the same way it does for any
      other replica error. Backstop for the corner case where the
      kernel hasn't surfaced the FIN yet but writes still appear
      to succeed (half-open TCP).

    Invariant: any future caller of `pool.send_vectored` /
    `pool.call_vectored` against a peer that may go down between
    `get_client` and `rx.await` MUST allow the surrounding logic
    to handle `Err` — never assume a returned receiver will
    resolve.

18. **F210-E1 + H1: re_avali permit + stale-VP sentinel mirror.**

    **E1.** `handle_re_avali`'s replicated repair path now acquires
    `concurrency_ctrl.acquire_recovery()` AFTER the EC short-circuit
    and the already-up-to-date check (so cheap requests don't consume
    a permit) and BEFORE `fetch_full_extent_from_sources`. Held by
    RAII for the duration of the peer-fetch + truncate + pwrite +
    fsync. Uses the same shared pool as `run_recovery_task`; both
    paths have the same `payload × 2` transient working set per
    in-flight task and benefit from a unified cap.

    **H1.** `StreamClient::read_with_layout` constructs and returns
    `StaleVpOffset` upfront when `ex.sealed_length > 0 && offset >
    ex.sealed_length`, BEFORE the `ec_converted` branch. Covers both
    replicated and EC paths uniformly so operational tooling's
    `stale_vp_offset_past_sealed_length:` regex (programming note 6
    F204 wire contract) fires on replicated VPs too. EC's
    `ec_slice_decoded` retains its own check as defense-in-depth on
    the decoded payload. Skips the wasted server round-trip + EC
    decode on a known-stale VP. Open extents (sealed_length=0) are
    untouched — there's no authoritative bound to check against.

19. **F223 recovery source-fetch MUST be chunked
    (`copy_bytes_from_source`).** The replicated-extent recovery path
    (`fetch_full_extent_from_sources` → `copy_bytes_from_source`) used a
    single `MSG_READ_BYTES` oneshot with `length: 0` (read-to-end). On a
    multi-GB sealed extent that trips the same >2 GiB per-syscall pread
    ceiling (macOS INT_MAX / Linux 0x7ffff000) + oversized rpc frame that
    F105 (StreamClient reads, note 12) and F115 (EN local-file I/O, note
    13) chunk everywhere else — this raw path was the one place that
    didn't. Symptom: recovering a 3 GB extent from two healthy replicas
    failed 10×/10 with "no source replica available for copy"; the source
    EN logged nothing (the read died at rpc framing before the handler).
    F223 makes `copy_bytes_from_source(addr, eid, eversion, total_len)`
    loop 256 MiB (`FILE_IO_CHUNK_BYTES`) reads via `read_bytes_chunk`;
    `fetch_full_extent_from_sources` passes `extent.sealed_length`. The
    **EC** shard-recovery caller (`run_ec_recovery_payload`) keeps
    `total_len = 0` (legacy to-end read — each shard is ~`sealed/K`, under
    the threshold). **Caveat: `ReadBytesReq.offset` is u32, so this covers
    extents up to 4 GiB** (guarded with an explicit error); a wider wire
    offset is a separate change if `max_extent_size` ever exceeds 4 GiB.
    **Invariant: any new full-extent fetch over `MSG_READ_BYTES` must
    chunk — never issue a single `length: 0` read against a sealed
    multi-GB extent.**

20. **Failover seal uses the SealCommit handshake — never a public-API-tracked
    commit (seed=13 Mode A root fix).** On a same-owner failover the writer
    must seal the failed tail at its OWN all-replica-acked commit, NOT let the
    manager probe `commit_length` (a probe over reachable members can capture a
    speculative/un-acked byte that only one soon-dead member holds → seals at a
    length no replica durably retains → recovery stuck forever = the phantom
    seal). The only SAFE commit source is the worker's serialized `state.commit`
    read at a QUIESCED point — a value tracked in the public API lags the worker
    and races concurrent out-of-order appends (FuturesUnordered) + tail rolls
    (coco confirmed a real data-loss race in the `result.end` shortcut, twice).
    Mechanism: `StreamSubmitMsg::SealCommit { resp }` → the worker
    `drain_inflight_for_seal` (awaits every in-flight append, bounded by each
    one's `append_fanout_timeout` so it cannot hang) → replies with the final
    contiguous `state.commit` → sets `sealing = true` (new appends on the
    about-to-be-sealed tail are rejected with a soft error so they retry onto
    the fresh tail; cleared by ResetTail). The 3 failover sites call
    `seal_commit_watermark` then `alloc_new_extent(stream, Some(commit))`;
    preemptive roll passes `Some(result.end)`; new-owner/sealed-tail init passes
    `None` (→ manager probes). `StreamAllocExtentReq.seal_commit: Option<u32>`
    (Some = authoritative seal at exactly c incl 0; None = probe) — NOT a `(u32,
    bool)` pair. **Invariant: never reintroduce a public-API commit-watermark
    cache; the seal length must come from the worker via SealCommit.**

    **BUG2-IDEMPOTENT-ROLL (chaos seed=603, WIRE v8): the SealCommit reply is
    `(commit, tail_extent_id)`, and that extent id is threaded to the manager as
    `seal_extent_id` so seal-and-roll is IDEMPOTENT on retry.** `alloc_new_extent`
    runs under `retry_manager_call` (20×). If an attempt SUCCEEDS on the manager
    (seals tail T at `commit`, rolls fresh tail T') but its response is LOST
    (chaos latency), the retry re-sends the SAME `seal_commit` — and the worker,
    whose cached tail is still T (it never saw the lost ResetTail), re-reports
    `(commit, T)`. Pre-fix the manager sealed its now-current FRESH tail T' at the
    stale `commit` → over-sealed T' beyond what any replica durably holds → T'
    unrecoverable → a split child CoW-sharing the log stream WAL-FAILSTOPs on
    replay and never opens. Fix: the worker is the ONLY place that knows WHICH
    extent the drained `commit` belongs to (its cached `state.tail.extent_id`),
    so `SealCommit` returns it; `seal_commit_watermark` → `(commit, eid)` →
    `alloc_new_extent(stream, Some(commit), eid)` (the `Some(result.end)`
    preemptive-roll site passes `result.extent_id`; `None`/probe sites pass `0`).
    The manager seals ONLY when its current tail still == `seal_extent_id` (and
    is OPEN), else idempotent no-op. **Invariant: any authoritative
    (`Some(commit)`) alloc MUST pass the captured tail's `seal_extent_id`; only
    probe/`None` rolls may pass `0`.** Cross-ref: manager CLAUDE.md note 32a.

21. **`ExtentInfo.sealed` is the authoritative "is sealed" flag — NOT
    `sealed_length > 0`.** Mirrors `MgrExtentInfo.sealed`. An authoritative
    empty seal is `sealed = true, sealed_length = 0` (e.g. a CoW-shared empty
    tail frozen by split/merge so children alloc a fresh tail). `sealed` = the
    STATE; `sealed_length` = the LENGTH (read-bound / is-empty). `ensure_tail_
    initialised` + the soft-error reload check `.sealed` (alloc fresh on a
    sealed-empty inherited tail); `read_with_layout` uses `.sealed` for the
    to-end bound + stale-VP check (a sealed-empty extent reads as empty, no
    commit_length probe). The stale-VP read-BOUND checks that compare an offset
    against `sealed_length` keep using `sealed_length` (it is the length).
    Invariant: `sealed_length > 0 ⇒ sealed`.

22. **`ResetTail` zeroes the worker's commit ONLY when it moves to a DIFFERENT
    extent (BUG#2, seed=8).** `StreamAppendState::apply_reset_tail` is the
    single decision point. `state.commit` is the worker's contiguous
    all-replica-acked prefix for its current tail — it advances ONLY on a full
    all-replica ACK (`apply_completion`/`ack`), never speculatively ahead, so it
    is ground truth for that extent and is the value the failover
    `seal_commit_watermark` reports as the seal length. Two `ResetTail` shapes:
    - **DIFFERENT extent** (a genuine roll to a fresh, empty tail, after
      `alloc_new_extent`): `reset_for_new_extent` (commit/lease_cursor=0,
      pending cleared, poisoned/sealing cleared) — commit=0 is correct for an
      empty extent.
    - **SAME extent** (a public-API SOFT-ERROR tail RELOAD that did NOT change
      the tail — the common case is a transient replica failure, e.g. a
      killed/restarting node refusing the connection): PRESERVE all
      append-progress state, refresh only the cached replica metadata.
    The BUG#2 data-loss path (confirmed by writer-side trace): the soft-error
    open-tail reload used to `ResetTail` to the re-loaded SAME extent, running
    `reset_for_new_extent` and ZEROING `commit`. The next retry escalated to the
    hard path, whose `seal_commit_watermark` then reported `commit=0`, and
    `alloc_new_extent(Some(0))` sealed the LIVE tail at `sealed_length=0` —
    orphaning every acked VP/SST byte past 0. A partition split CoW-propagated
    that poisoned extent to the child, which could then NEVER open
    (`stale_vp_offset_past_sealed_length: ... sealed_length=0`). `poisoned`
    (and `sealing`) MUST also be preserved on a same-extent reload: under
    concurrent same-stream appends a non-tail-lease failure sets `poisoned` to
    mark a HOLE (`rewind_or_poison`); kept poisoned, the next Append is rejected
    so the caller escalates to seal-and-roll, which seals at the contiguous
    `commit` (hole + everything after correctly excluded) rather than resuming
    past the hole. **Invariant: never reset the worker's `commit`/`poisoned`
    when staying on the same tail extent.** Opt-in localization aid: the
    `bug2_trace` tracing target (silent unless `RUST_LOG=…,bug2_trace=info`)
    logs every alloc-seal (`seal_path` + `seal_commit`), SealCommit reply,
    probe responses, and EN under-seal — it pinned this root cause and is kept
    for the under-seal class. Test:
    `client::pipeline_tests::reset_tail_same_extent_preserves_commit_and_poison`.
    Cross-ref: notes 20 (SealCommit handshake — `commit` is the seal source),
    21 (`sealed` state), 9 (commit tracking is local).

23. **`owner_epoch` fence is made DURABLE before any append is ACKed under it
    (P0-B).** `owner_epoch` (EN `.meta` bytes 32–40) is the per-extent write
    fence: an append with `revision < owner_epoch` is rejected
    (`CODE_LOCKED_BY_OTHER`). It is RAISED ONLY by the APPEND path (commit_length
    is check-only — bug#3 Layer C), monotonically, to the request's revision; the
    value originates from the manager owner-lock (`acquire_owner_epoch`). It is
    NOT `eversion` (bytes 24–32, the seal/EC metadata version) — the two are
    independent and checked separately.
    Pre-P0-B the EN bumped `owner_epoch` in memory, wrote data, then did a
    best-effort `save_meta` AFTER the write and ACKed even on persist failure
    ("safe form"). A crash (or swallowed failure) in that window left the fence
    non-durable → on restart `.meta` held the old/0 revision → a stale lower
    owner re-passed the fence → split-brain / acked-data overwrite. The fix has
    two coupled pieces, on BOTH append paths (`build_append_future` +
    `handle_append`):
    - **In-memory bar raised SYNCHRONOUSLY** (`owner_epoch.fetch_max(R)`,
      before any await) so a stale lower owner is rejected immediately, even
      while the new fence is still being persisted. `fetch_max` (not `store`)
      keeps it monotonic under two concurrent higher-revision appends.
    - **Durable high-water gates the ACK.** New `ExtentEntry.durable_owner_
      revision` = the revision known durable in `.meta`. `ensure_fence_durable
      (id, entry, R)` returns Ok iff `durable >= R`; else it persists under the
      per-extent `meta_write_lock` and advances `durable`. An append ACKs only
      after `durable >= R`. Fail-closed: a persist failure rejects the append
      (`CODE_PRECONDITION`) + marks the disk offline — never ACK on a
      non-durable fence. Fast path = one atomic load (steady state); the
      lock+persist only fires on a revision change (rare).
    - After the (possibly awaiting) durable step, **re-check** both
      `owner_epoch` (a higher owner may have taken over → LockedByOther) AND
      `sealed/sealed_length/avali` (a concurrent seal/EC may have sealed the
      extent during the await → CODE_PRECONDITION; mirrors F147-B).
    `.meta` durability hardening (load-bearing for the above): ALL `.meta`
    writers go through `meta_write_lock` (per-extent, DISTINCT from the EC
    op-lock to avoid self-deadlock — EC commit / re_avali hold the op-lock and
    call `save_meta`). `write_meta_locked` reads the LIVE atomics under that lock
    (so a stale-snapshot fence persist can't clobber a concurrent seal's `.meta`)
    and writes ATOMICALLY: temp `.meta.tmp` → fsync → `rename` → **fsync parent
    dir** (the rename's directory-entry update must be durable, else the ACKed
    fence can regress on a host crash). **Invariants: (1) never ACK an append
    whose `owner_epoch` fence isn't durable; (2) raise the in-memory bar
    synchronously, persist before publishing `durable`; (3) every `.meta` write
    holds `meta_write_lock` and reads live atomics.** Cross-ref: notes 21
    (`sealed`), 20 (SealCommit), bug#3 Layer C (commit_length check-only).

24. **KNOWN BUG (deferred, reproduced 2026-05-31): the F178 fsync coalescer can
    ACK an append whose bytes were written AFTER the covering fsync, under
    out-of-order same-extent completion.** `build_append_future` / `handle_append`
    do `pending_fsync.store(total_end)` as a PLAIN store AFTER the pwrite await.
    Two same-extent append futures can coexist in the inflight `FuturesUnordered`
    (frames straddling read-burst boundaries; `extent.len` is reserved
    synchronously at build so their offsets are ascending+non-overlapping), and
    their pwrite CQEs can complete OUT OF ORDER. If the high-offset write
    completes first it sets `last_synced` via a real fsync; the low-offset write
    then completes late, `store`s a SMALLER `pending_fsync` (regresses it — plain
    store, not `fetch_max`), and its waiter is satisfied by the `pending <=
    synced` no-fsync branch (`coalescer_loop` ~624) — crediting durability to
    bytes the only fsync did not cover. **Severity: near-precluded in practice**
    — (a) loss is observable ONLY under power-loss / kernel-panic (a process kill
    does NOT drop un-fsynced page-cache writes, so no process-kill chaos can
    surface it — harness-invisible); (b) F227 all-replica-ACK + recovery
    re-replicate a single-replica loss. **NOT FIXED** (the only correct fix —
    contiguous completed-prefix tracking so `last_synced` advances only over a
    fully-written prefix — touches the hot-path coalescer, the most revert-prone
    area). Reproduced deterministically by `#[cfg(test)] mod
    p0_fsync_highwater_tests` in extent_node.rs (models the out-of-order
    completion; asserts the no-fsync ACK). **Invariant for a future fix:
    `last_synced` must never advance past the largest offset X such that ALL
    `[0,X)` pwrites have completed; land the fix WITH those tests as guard +
    coco /findbugs.** Cross-ref: note 4 (`must_sync` cost), F178 in the Commit
    Protocol section.
    **ATTEMPTED + REVERTED 2026-06-01 — do not re-walk this without a new plan.**
    The contiguous-prefix watermark (`record_completed` + a `completed` stash +
    `poisoned` flag + `epoch` counter) was built and run through 6 coco rounds.
    The data-safety CORE (the prefix watermark) was right, but the surrounding
    WAITER-LIFECYCLE state machine self-produced 6 reachable bugs in this
    io_uring hot path (busy-loop; failed-low-write hang; parked-loop leak;
    single-loop-invariant break that was a regression from its OWN leak fix;
    reset-orphans-high-waiters). Reverted whole: a power-loss-only,
    all-replica-masked, never-manifested bug does not justify a self-bug-
    producing hand-rolled state machine in the hottest concurrency path (Items
    2/4 discipline). io_uring CQE order is genuinely unordered per spec (no
    IOSQE_IO_LINK / DRAIN in this code), so the accounting hole is real on
    paper — but the 2 kept unit tests demonstrate it by HAND-ORDERING
    completions, NOT by real io_uring reordering, and real loss additionally
    needs power-loss in the µs window on enough replicas. **If production ever
    demands power-loss single-replica durability here, do it as a SEPARATE
    feature: per-extent SERIAL fsync accounting (RocksDB single-sequential-WAL-
    writer model — gap-free prefix by construction), NOT state bolted onto the
    concurrent coalescer.**

    **ARCHITECTURAL REACHABILITY ANALYSIS (2026-06-02 — strengthens the defer
    decision from "rare" to "structurally near-unreachable under current
    invariants").** The "CQEs can complete OUT OF ORDER" assumption above is
    formally correct per the io_uring spec but does not hold for autumn-rs's
    actual append path. Three layers stack to make same-extent page-cache
    ordering structurally enforced rather than statistically likely:

    1. **io_uring inline-FIFO for buffered writes.** `vfs_write_iter` →
       `generic_file_write_iter` for buffered writes only touches page cache —
       non-blocking in the common case, so io_uring's `io_uring_enter` syscall
       processes the SQE INLINE in the submitter thread's kernel context
       rather than punting to an io_wq worker. Inline processing is strictly
       FIFO over the SQ: A and B are run to completion (page-cache memcpy +
       CQE emission) one after the other in the same kernel-mode thread.
    2. **i_rwsem serialises same-inode writes** even when punted. If A
       does get punted to io_wq (memory pressure / contention), B's inline
       path still has to take `inode_lock` in write mode, which serialises
       against A's worker. Whichever wins the lock runs to completion first;
       the loser waits. Race window narrows to "which kthread grabs i_rwsem
       first" — not "both run truly concurrently".
    3. **Single-writer SQE submission per extent.** compio is one
       io_uring per OS thread. Both log_stream (P-log task) and row_stream
       (P-bulk thread) are single-writer per extent by design — no two tasks
       on the same thread submit pwrite SQEs to the same extent concurrently.
       SQ submission order = single-thread `await`-sequenced order = strictly
       in-order. The `FuturesUnordered` of inflight batches the note
       worries about (`frames straddling read-burst boundaries`) lives in
       handle_connection's CQ-side, NOT in the SQ-side — SQ submission for
       same extent is always serial.

    Combining: SQ FIFO submission + kernel inline FIFO processing + i_rwsem
    serial fallback ⇒ **A's bytes are in page cache strictly before B's bytes
    are written, for every same-extent A-then-B pair on this code path**. The
    CQE-reorder assumption is voided by construction. The "plain store
    regresses pending_fsync" hazard the original note 24 identified is then a
    formal code-review finding that has no concrete trigger.

    **Architectural assumptions this analysis depends on (any future change
    that violates these MUST re-validate the race + likely needs the
    deferred RocksDB-leaderless fix):**

    - **Buffered writes via `vfs_write_iter`.** Switching any extent to
      `O_DIRECT` removes inline processing — DIO writes always punt to io_wq.
    - **`IORING_SETUP_DEFAULT` (NOT SQPOLL).** SQPOLL mode replaces inline
      processing with a dedicated kernel poller thread that has its own
      scheduling — same-thread FIFO no longer applies.
    - **Single OS thread per io_uring** (compio thread-per-core invariant).
      A multi-thread runtime sharing one ring would lose the SQ-side FIFO
      property.
    - **Single-writer per extent within a thread.** Today: P-log owns
      log_stream, P-bulk owns row_stream; no thread submits two
      concurrent pwrites on the same extent. Any future change that
      multiplexes writers onto the same extent on the same thread
      (e.g., a fan-out append API for parallel SST builders) reintroduces
      the `FuturesUnordered` same-extent contention this note 24 originally
      modelled.
    - **Append-only writes** (no partial-page RMW). Append-only into reserved
      extent.len means each pwrite is into freshly-allocated pages — no
      `IOCB_NOWAIT` fail-and-punt for missing page reads.

    The original "near-precluded in practice" wording understated the case:
    on the current code path the race is **structurally voided**, not just
    statistically rare. The defer is correct because the bug is unreachable
    under current invariants, not because the probability is low enough to
    tolerate. If any of the five assumptions above is broken in a future
    refactor, **the deferred RocksDB-leaderless-style coalescer redesign
    (Issue #14627 ping-pong + LSN watermark) becomes load-bearing** —
    re-validate the race on the new architecture before shipping.

25a. **ENOSPC-1: disk health is a 3-state machine (`DiskHealth`:
    Online / Full / Faulted), and the batched-append pwritev MUST be the
    `_all` form.** Two coupled production fixes (2026-06-12):
    - **Short-write corruption (the headline bug, found by
      `scripts/enospc_chaos.sh`):** `build_append_future` used raw
      `write_vectored_at` and only checked `Err` — POSIX pwritev on a
      nearly-full disk writes what fits and returns the SHORT count, so a
      partial append (3.5 KB of a 1 MB value) was fsynced + ACKED and the
      unwritten reserved tail read back as zeros. Fixed with
      `write_vectored_all_at` (loops until done or real error). Invariant:
      **every local file write must be a `*_all` form or verify the
      written count — Ok(n) from a raw positional write is NOT success.**
      (`file_pwrite` already used `write_all_at`; the vectored batch path
      was the one raw call.)
    - **Classification:** `mark_disk_error_for_extent(extent_id, msg)`
      replaces the bare offline mark at every write/persist error site
      (append pwrite/fsync, fence persist, save_meta fail-closed paths,
      recovery, EC staging/commit — the EC family previously marked
      nothing). ENOSPC/EDQUOT (`is_disk_full_error`, matched on the os-
      error suffix since several sites only have stringified errors) ⇒
      `Full`: disk stops hosting NEW extents (`choose_disk` requires
      `allocatable()`) but keeps serving reads + existing extents, and the
      per-shard 2 s sweep SELF-HEALS it back to Online once free ≥ 5% of
      total — a transiently-full disk no longer stays dead until process
      restart. Anything else ⇒ `Faulted` (historical permanent-until-
      restart semantics; never downgraded by `set_full`/`try_clear_full`).
      Exported as `autumn_en_disk_full` / `autumn_en_disk_online`.
      Manager side: `node_health_loop` stashes each node's max per-disk
      free (in-memory), `select_nodes` soft-avoids nodes below
      `--min-alloc-free-bytes` (default 256 MiB, 0 = off) with the F121
      fallback chain intact. E2E: `scripts/enospc_chaos.sh` (loopback
      512 MB fs, fill → failover → self-heal → zero-loss verify).
    - **Health is SHARED per physical dir across shards (coco P1).** F196
      multi-shard builds one `DiskFS` per shard for the same dir; a
      shard-local flag let shard B keep allocating onto a disk shard A
      had marked Full. `DiskFS.health` is an `Arc<AtomicU8>` from the
      process-global `shared_disk_health(base_dir)` registry (canonical-
      path keyed) — one state per physical dir, test instances on
      distinct dirs stay isolated.
    - **Caller-ack ⊆ contiguous commit (coco P1, the seal-chop hole).**
      `client.rs::apply_completion` used to fire the caller's oneshot Ok
      the moment a batch completed on all replicas — even when a LOWER
      lease on the same extent had already failed (a permanent hole, the
      exact mid-pipeline profile ENOSPC produces). The writer's `commit`
      stays below the hole (correct — `apply_reset_tail` doc: seal at
      contiguous commit, "hole + everything after correctly excluded"),
      so the roll's SealCommit CHOPPED an already-acked range. Now
      `StreamAppendState::ack` carries the oneshot: caller acks fire only
      as the contiguous prefix advances; on poison, `failure_floor` =
      first failed offset, every pending/late completion at/above it
      resolves Err ("retry on a fresh extent" — the replica bytes become
      benign un-acked duplicates). Invariant: **a caller-visible append
      ack implies the range is inside the contiguous all-replica-acked
      prefix.** Cost: out-of-order completions wait for contiguity
      (inversions are rare; same bounded-p99 trade-off as F210-C1).
      Tests: `caller_ack_deferred_until_contiguous`,
      `completion_above_failed_lease_is_failed_not_acked`.

25. **No `.meta` persist failure is ever swallowed — P0-D closed the last
    `let _ = save_meta(...)` sites (2026-06-12).** The `.meta` sidecar is the
    only state a restart trusts (note 23); any path that mutates
    eversion/sealed/avali/sealed_length and then ignores a failed persist
    reports a state change the disk does not hold. The remaining three sites,
    all converted to fail-closed (`mark_disk_offline_for_extent` + error, the
    note-23 response to a sidecar-persist I/O error):
    - `run_recovery_task` final persist: a swallowed failure reported a
      recovered replica whose sidecar still carried the pre-recovery
      eversion/seal. Now the task FAILS (dispatch loop retries) and the
      partial `ExtentEntry` is REMOVED from `extents` — leaving it would let
      local retries reuse the offline disk via `ensure_extent`'s
      existing-entry fast path and block manager re-dispatch with "extent
      already exists" (the orphan `.dat` is reaped by F109/F113 reconcile).
    - `handle_convert_to_ec` prepare-path seal: a non-durable seal gating the
      EC encode meant a crash mid-convert could restart the extent as OPEN
      with shards already distributed. F153 per-extent lock + F119-D
      idempotency make the manager's retry safe.
    - `handle_convert_to_ec` post-convert eversion/seal persist: a stale
      pre-convert sidecar over shard-shaped data is the corruption family
      F119-C/D guard against.
    Corollary: the F119-D **idempotent-skip must ENSURE durability, not just
    check atomics** — a prior attempt may have published the in-memory
    atomics and then failed its persist (the fail-closed paths can't roll
    atomics back), so the skip path re-runs `save_meta` (idempotent) and
    fail-closes if it still can't persist. Invariant: **returning OK from any
    seal/convert/recovery RPC asserts the sidecar is durable, not merely that
    memory agrees.**

26. **/metrics (observability batch 1).** The EN's authoritative state is
    shard-local `Rc` (unreadable from the metrics HTTP thread), so two
    process-global mirrors feed `render_en_metrics()` (exported, called by
    the binary's `--metrics-port` listener via
    `autumn_common::metrics_http`): (a) `EN_APPEND_TOTALS` — monotonic
    append batch/bytes/ns counters, 3 relaxed fetch_adds per BATCH in
    `ExtentAppendMetrics::record`; (b) `EN_SHARD_GAUGES` — one
    `Arc<EnShardGauges>` slot per ExtentNode instance (registered in
    `new()`, cloned in `impl Clone`), refreshed by a 2 s task spawned in
    `new()` ON THAT SHARD's runtime (Rc clones stay same-thread). NOT
    df-driven: the manager's df probe only reaches the registered
    control_address — one shard — so a `handle_df` refresh left every
    other shard's slot permanently stale (caught live: 6 extents on disk,
    metrics said 3). Renderer sums extents across shards (disjoint sets)
    and reports a disk offline if ANY shard's view says so (each shard
    owns its own `DiskFS` copy). In-process tests accumulate inert extra
    slots — harmless, render is binary-only.

27. **F276 read-path Suspected avoidance (replicated route-around + EC proactive
    reconstruct).** `alloc_extent` already excludes manager-`Suspected` nodes
    (`select_nodes` via `online_node_ids`); F276 extends the SAME avoidance to
    the READ path, which was previously blind to node status (it only REACTED to
    an RPC failure — paying a per-read timeout when rotation/`replica[0]` landed
    on a flaky node, and on EC waiting for a doomed shard read to fail before
    reconstructing). Mechanism:
    - **Client-side snapshot, not new wire.** `StreamClient.suspected:
      Rc<RefCell<SuspectedCache>>` is refreshed by `maybe_refresh_suspected()`
      (called at the `read_with_layout` entry): TTL-gated (2 s, matches df
      cadence) + NON-BLOCKING — it spawns a detached task that polls the
      EXISTING `MSG_LIST_NODE_STATES` (filter `NODE_AUTO_STATE_SUSPECTED`) and
      swaps the set in; the current read proceeds on the slightly-stale snapshot
      and NEVER awaits a manager RTT. Idle clients don't poll (fires only while
      reads happen). A failed poll keeps the prior snapshot — a failed refresh
      must never WIDEN the avoidance set (that could strand reads).
    - **Replicated = soft deprioritize (never exclude).** Pure fn
      `replicated_read_order(ex, offset, suspected)` folds F258 rotated start +
      `avali` eligibility (self-heal I2) + Suspected-to-BACK. Healthy replicas
      are tried first; suspected slots are KEPT as a last-resort tail (suspected
      ≠ dead, and a sealed extent's committed bytes are on EVERY replica). The
      hedge races `order[0]/order[1]` (the two healthy-first slots).
      **Invariant: an empty Suspected snapshot reproduces the pre-F276 rotated
      walk byte-for-byte — the hot path is unchanged.**
    - **EC = proactive reconstruct (the "不等超时").** In `ec_subrange_read`, a
      data shard whose node is Suspected is NOT read directly — it goes straight
      into `needs_reconstruct`, so the `join_all` of direct shard reads never
      blocks on a flaky node's 5 s timeout before `ec_reconstruct_shard_subrange`
      (read K healthy shards + parity, first-K-wins) rebuilds it.
      `ec_reconstruct_shard_subrange` itself is UNCHANGED — first-K-wins already
      never waits on a dead peer.
    - **ALL THREE VP read paths covered (the fragmentation trap).** Per
      `[[project_vp_read_paths_fragmented]]`, a read-side policy must touch the
      copy path AND both ZC fast paths, or a hot GET silently bypasses it (coco
      caught the v1 commit covering only the copy path):
      - copy / chunked — `read_replicated_with_failover` (order above);
      - ZC value proxy — `read_value_into_pooled` (PS-side GET fast path) — same
        `replicated_read_order` (reads in-order, returns first OK, so ordering
        routes around the flaky node);
      - client-direct descriptor — `extent_read_descriptor` (F259) uses
        `healthy_eligible_slots` to **DROP** Suspected addresses (NOT reorder):
        the external SDK picks its own hash-rotated start over the returned list
        (`crates/client/src/lib.rs`), so ordering wouldn't help — exclusion is
        the only effective route-around. Keeps ALL eligible when every one is
        Suspected (never strands).
    - **Steady-state, not per-read (non-blocking design consequence).** The
      refresh is fire-and-forget, so the FIRST read after a node flips to
      Suspected (e.g. an idle client with an empty snapshot) only *kicks* the
      poll — it uses the old snapshot and can still pay one timeout if it lands
      on the flaky node; every read after the ~2 s refresh avoids it. This is
      deliberate: a synchronous first-refresh would put a manager RTT on the
      read's critical path, and idle background polling is the per-partition
      manager traffic we avoid. It never regresses the pre-F276 reactive
      failover — so the documented acceptance (docs/ops.md, Read route-around)
      is "after a couple seconds of read traffic", not "every single read incl.
      the first" (coco verify-pass P2, resolved as a doc-accuracy fix, design
      kept).
    - **Soft hint, correctness-independent.** A stale / over-broad snapshot only
      costs a little extra latency or parity traffic, never data: replicated
      reads still fall back to suspected replicas; EC reconstruction falls back
      to launching every peer if it can't gather K healthy shards. Tests:
      `client::f276_suspected_read_tests` (6 pure-fn cases incl. all-suspected
      never strands + avali-isolated never served). Cross-ref: F258 (rotation +
      hedge), self-heal `eligible_replica_slots` (avali I2), F190 `bad_nodes`
      (write-path client-learned health — distinct, alloc-only, per-stream).

---

## RPC Wire Protocol (extent_rpc.rs)

Uses autumn-rpc custom binary protocol (10-byte frame header). No protobuf — hot-path RPCs use hand-coded binary encoding for minimal overhead; control-plane RPCs use rkyv zero-copy serialization.

### Hot-path binary codecs

| RPC | msg_type | Request size | Response size |
|-----|----------|-------------|--------------|
| Append | 1 | 29B + payload | 9B |
| ReadBytes | 2 | 24B | 9B + payload |
| CommitLength | 3 | 16B | 5B |

### Control-plane (rkyv)

AllocExtent(4), Df(5), RequireRecovery(6), ReAvali(7), CopyExtent(8), ConvertToEc(9), WriteShard(10).

---

## Performance (benches/extent_bench.rs)

Benchmark setup: single compio thread, loopback TCP, 4KB payload.

Key results (single connection, pipelined):
- **Write depth=32**: 116k ops/s, 455 MB/s
- **Write depth=64**: 125k ops/s, 489 MB/s
- **Read depth=64**: 95k ops/s, 373 MB/s
- **Mixed 1w+1r**: 93k total ops/s

See `benches/bench_results.md` for full results and historical comparison.

### Performance optimizations

1. **pwritev batch** — consecutive MSG_APPEND frames coalesced into one `write_vectored_at` syscall
2. **pread batch** — consecutive MSG_READ_BYTES processed sequentially, responses collected
3. **write_vectored_all** — ALL responses from one TCP read written in one syscall
4. **Client pipelining** — sliding window depth hides RTT, enables server-side batching

---

## F229 background-loop supervision (extent-node)

Every EN background loop runs under a supervisor instead of a bare
`spawn(..).detach()` (which swallowed panics). Two helpers in `extent_node.rs`:

- `en_spawn_supervised(name, make)` — catch_unwind + ERROR-log + 1 s restart,
  for the RESTARTABLE orphan-reconcile sweep (`spawn_reconcile_orphans_loop`) —
  re-derives from `node.clone()` each tick, owns no moved resource.
- `en_spawn_failstop(name, fut)` — catch_unwind; NORMAL return = expected lazy
  exit (no-op); PANIC → ERROR-log + `std::process::exit(1)`, for the per-extent
  `coalescer_loop` which owns its moved wake-channel receiver and is
  durability-critical (the fsync coalescer). Restart-in-place is impossible (the
  receiver is gone) and unsafe; fail-stop → EN restarts and recovers extents
  from disk (the data files are the journal; nothing committed is lost).

**1A — bounded connect.** `ConnPool::get_client` (`conn_pool.rs`) wraps
`RpcClient::connect` in a fixed `CONNECT_TIMEOUT` (5 s) so a blackholed peer
(SYN dropped) can't hang any caller — `call_timeout` only bounds the call AFTER
connect. Mirrors the manager-side F228 connect-timeout fix; env-free per F195.

**Invariant:** never reintroduce a bare `spawn(..).detach()` for an EN
background loop — use `en_spawn_supervised` (re-derive-safe) or
`en_spawn_failstop` (moved-resource / durability). Request-triggered detached
tasks (`run_recovery_task`, EC convert) are NOT loops — a panic there fails one
operation (retried), so they stay as-is.

---

## F193 Stage C — streaming recovery / re_avali (extent-node memory)

Replication recovery (`run_recovery_task`) and `handle_re_avali` stream the full
sealed extent from a healthy peer **chunk-by-chunk** via `stream_extent_from_sources`
instead of `fetch_full_extent_from_sources` (which materialized the whole extent
in one `Vec<u8>` before writeback). Per chunk: read one `FILE_IO_CHUNK_BYTES`
(256 MiB) range via `MSG_READ_BYTES` → `pwrite` at its offset → drop → next.
**Peak resident = one chunk**, independent of extent size; one `sync_data` after
the full write.

Failover: `dest` is truncated to 0 before each source attempt, so a mid-stream
source failure / short read abandons that source and the next restarts from
offset 0 (the partial write is discarded — no corruption). Succeeds only on a
full `sealed_length` transfer; all-sources-failed → `Err`.

NOT changed (intentionally): `handle_copy_extent` already serves `[offset, size)`
ranges (`file_pread_chunked`) and has no production originator (only the
sibling-shard forward), and the recovery read path is `MSG_READ_BYTES`, not
`MSG_COPY_EXTENT`. EC recovery (`run_ec_recovery_payload`) still buffers shard-
sized (≈ `sealed_length / K`).

**F193 Stage B — DONE (chunked EC convert, u64-offset widening follow-up).** The
"Reopen only on a real EC-convert OOM signal" caveat was triggered by raising
`max_extent_size` to 16 GiB: the old whole-extent `ec_encode` held ~2× the
payload (~32 GiB at 16 GiB), AND — independently fatal — a whole shard at
16 GiB / K=3 is ~5.33 GiB, so the whole-shard `WriteShard` payload overflowed the
frame `payload_len: u32` (≤ 4 GiB). Both are now fixed by **stripe-wise EC
convert**: RS over GF(256) is byte-wise per offset (`erasure::ec_encode_stripe`,
byte-identical to a slice of `ec_encode` — proved by
`ec_encode_stripe_matches_whole`), so `handle_convert_to_ec` reads the K data
sub-ranges at shard-offset `s` from the local `.dat`, encodes the M parity
stripes, and streams each shard's stripe via `WriteShard` carrying a new
`shard_offset: u64` (WIRE v6). The receiving node `pwrite`s the stripe into
`.ec.dat` at `shard_offset` + `sync_data`s it (`write_shard_stripe_local`).
Peak RAM = `(K+M) × EC_ENCODE_STRIPE_BYTES` (64 MiB default → ~256 MiB),
independent of extent size; each `WriteShard` stays under the frame ceiling.
Crash-safety preserved: per-stripe `sync_data` + sequential await-ack make the
durable prefix grow monotonically; the coordinator writes its OWN shard 0 stripe
LAST per stripe, so `coordinator_prepared` (coord staging == shard_size) ⇒ every
participant durably staged every stripe; the 2PC commit + EC-COMMIT-ATOMIC #5
marker are unchanged. `write_shard_stripe_local` bounds `shard_offset +
stripe_len ≤ sealed_length` (coco P1) so a malformed/stale `WriteShard` can't
balloon `.ec.dat` into a sparse file that commit would publish. The peer-copy
(coordinator local short) now streams into a TEMP file via
`peer_copy_full_extent_to_dat` and atomic-renames over `.dat` only after a full
copy lands (coco P1 — never `set_len(0)` the live replica before securing a
complete copy, unlike `stream_extent_from_sources` whose dest is a node being
rebuilt). `fetch_full_extent_from_sources` (the whole-`Vec` buffering peer-copy)
was removed. **Test override `AUTUMN_EXTENT_EC_STRIPE_BYTES`** forces multi-stripe
without writing multi-GiB extents.

**Invariant:** any new full-extent peer-copy-then-writeback path must stream via
`stream_extent_from_sources` / `peer_copy_full_extent_to_dat` (or an equivalent
read-chunk→write-chunk→drop loop), never buffer the whole extent in a `Vec`,
unless it is shard-sized. Any new EC encode must be stripe-wise (`ec_encode_stripe`)
so the transient + the per-RPC `WriteShard` stay bounded regardless of extent size.

### Recovery over-promised-seal reconciliation (2026-05-30, seed=13 Mode A)

`stream_extent_from_sources` does NOT retry forever when the manager's
`sealed_length` is an unrecoverable over-promise. F227's lenient FAILOVER seal
(the `end==0` path in `handle_stream_alloc_extent` — `min` over reachable
committed members) can seal an extent at a length that NO replica durably
retains: e.g. one reachable replica reported a single speculative/un-acked byte
at seal time (sealed_length=1), but that byte rolled back on the next
min-commit truncation, so every replica now holds 0 bytes. Recovery streaming
`[0, sealed_length)` then sees every source return fewer bytes → SHORT → and
pre-2026-05-30 returned `Err("no source replica available")` and retried every
10 s for 60 s+, holding the F207 inflight Recovery marker → blocking
`punch_holes` / `split` (refuse-at-start) and freezing forward progress.

Now: track per-source outcome — `best` (longest copy any source delivered),
`err_count` (sources that errored mid-stream), `unverified` (non-excluded
sources that could not even be attempted: absent from `nodes_map` / unparseable
addr / `dest.set_len(0)` failed). If a source delivers the full `sealed_length`
→ return it (unchanged happy path). Otherwise, **only when EVERY non-excluded
source was REACHED and is consistently SHORT** (`err_count == 0 && unverified ==
0`), reconcile to the replica consensus: re-stream the longest available copy
and return it. This is SAFE under all-replica-ACK — the acked prefix is on every
committed replica, so the best reachable copy is ≥ the acked length; only phantom
(un-acked) tail bytes are dropped, which F227 already deems acceptable (manager
note 28). `run_recovery_task` still applies `sealed_length` via `fetch_max`, so
the recovered replica reports `synced_length = max(0, sealed_length)` and the
all-replica flush barrier still clears.

**Two guards are load-bearing (coco-reviewed):** (1) re-stream errors are
propagated (`stream_one_source(...).await?` then `got < best_len → Err`), NEVER
swallowed to `Ok(0)` — else an incomplete file would be marked recovered. (2)
`unverified == 0` (not just `err_count == 0`) — an un-attempted source might
still hold the full `sealed_length`, so reconciling to a short consensus while
ANY source is unverified could drop data that exists out of reach. **Invariant:
never reconcile a sealed extent DOWN while any non-excluded source was
unreachable or unattempted — that source may hold the only full copy.**

## META-FAILCLOSED + EC-PREPARE-DURABLE (生产就绪审计修复 2026-06-13)

两处崩溃一致性 fail-open,改为 fail-closed(coco arch 审计 P0,先复现再修):

**META-FAILCLOSED — 损坏 `.meta` 隔离(不再 fail-open 到 owner_epoch=0)。**
`.meta` 写路径一直是 tmp+sync_data+rename+parent-dir-fsync 的 fail-closed
(P0-B/F159);但 `load_extents` 读路径把 `parse_meta` 失败(CRC/magic/eid
不符 = bit rot/torn write)和文件缺失**都** `unwrap_or(DEFAULT open,
owner_epoch=0)` —— 断电后一个本该 sealed/fenced 的 extent(`.dat` 存活、
`.meta` 损坏)重启即变 open+epoch0,**stale 低-epoch writer 绕过 fence
ghost-append**(split-brain)。修复:`ExtentEntry.corrupt_meta: AtomicBool`,
load 时区分(a)`.meta` present-but-corrupt(parse None)→ quarantine;
(b)非-NotFound 读错误(EIO/EACCES)→ quarantine(coco P1,状态未知不可
fail-open);(c)真 NotFound → 默认 open(fresh extent)。quarantine 时
**append(handle_append + build_append_future)/ read(handle_read_bytes +
build_read_future 批量热路径,coco P1)/ commit_length 全部拒绝**;
`write_meta_locked` 成功持久后清 flag(recovery/re_avali 重建即 un-quarantine)。
不变量:`.meta` 损坏/不可读 + `.dat` 在 = 绝不默认 open,必 quarantine 待
manager 恢复。复现测试 `corrupt_meta_quarantines_extent_and_rejects_stale_append`
(先红后绿)。**已知残留(liveness,非本次)**:corrupt 自动触发 manager
recovery 需要额外上报路径;当前靠 read/commit_length 拒绝让副本"看起来不健
康" + 客户端 failover 到健康副本,recovery 跑到即清。

**EC-PREPARE-DURABLE — EC 2PC staging 缺 parent-dir fsync。**
`write_shard_local`(prepare)写 `.ec.dat` 后只 `sync_data()`(内容 durable),
**未 fsync 父目录** —— POSIX 下新文件的目录项不随内容 fsync 持久,断电可整个
丢 dirent,而 commit 注释承诺"`.ec.dat` persists as durable prepare record",
不成立 → commit 重试找不到 staging → 2PC participant 卡死。修复:helper
`fsync_staging_dir`,prepare 的**新写路径 + 幂等早退路径(coco P2:早退也要
满足同一 durable-prepare 语义)**都调用;commit 的 `rename(.ec.dat→.dat)` 之
后同样 fsync 父目录(dirent swap durable)。套用 `write_meta_locked` 既有
P0-B 模式。**触发=断电/内核崩溃**(kill -9 不丢 dirent,chaos 测不到),修复
为纯 additive fsync,EC 单测(f119-d/f153)全绿。

**EC-COMMIT-ATOMIC (#5) — `rename(.ec.dat→.dat)` ↔ `save_meta` 崩溃窗的
intent marker(2026-06-15,reproduce-first)。** `commit_shard_local` 的 commit
是两步独立持久化:① `rename(.ec.dat→.dat)`+dir-fsync(`.dat` 变 shard,durable)
② 改 atomics + `save_meta`(`.meta` 记 post-EC eversion/sealed_length)。两步间崩溃
(rename 已 fsync,**kill -9 即可复现**)→ `.dat`=shard 但 `.meta`=pre-EC,而旧幂等
(staging 缺失 + eversion stale → Err)使 commit **永久卡死** + 把 shard 当完整 value
读(损坏)。修复:`extent-{id}.ec.commit` marker(`[new_eversion][sealed_length]`,
**rename 前** durable 写,save_meta 后删)。`finish_ec_commit` 共享 helper:
rename-if-staging + **总是 reopen `.dat`**(同进程 retry 可能持旧 unlink fd)+
**单调 `fetch_max` eversion**(防旧 marker 回退)+ save_meta。`load_extents` 启动重放
三态 `EcCommitMarker`:Valid+eversion<marker → 补齐;eversion≥marker → 仅清 marker;
Corrupt(present 但损坏/截断)→ **quarantine 失败-关闭**;Absent → 跳过。同进程 retry
分支用 **marker payload(非 RPC 参数,marker 是已发布 `.dat` 的权威)** + 同样 eversion
门控。**不变量:`corrupt_meta` 的 extent 绝不 marker-replay**(marker 无 owner_epoch,
replay 会写 owner_epoch=0 → fence 旁路 + 清 quarantine = META-FAILCLOSED 漏洞)。
4 单测(状态损坏复现 / 重放修复+幂等 / corrupt-meta 跳过 / corrupt-marker quarantine);
coco 4 轮 P0-P3 全处置。`remove_extent_files` 同时 unlink marker。
