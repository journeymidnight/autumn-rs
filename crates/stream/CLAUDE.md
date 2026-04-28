# autumn-stream Crate Guide

## Purpose

Five components in one crate:
1. **`ExtentNode`** (`extent_node.rs`) — the server-side storage daemon that holds extents on local disk, implements ExtentService via autumn-rpc (custom binary protocol on compio).
2. **`extent_rpc`** (`extent_rpc.rs`) — wire codec for all 10 ExtentService RPCs. Hot-path uses binary encoding; control-plane uses rkyv zero-copy serialization.
3. **`StreamClient`** (`client.rs`) — the client library used by `PartitionServer` to read/write streams. Manager calls are stubbed (F044 scope).
4. **`erasure`** (`erasure.rs`) — Reed-Solomon EC codec (`ec_encode`, `ec_decode`, `ec_reconstruct_shard`), wrapping `reed-solomon-erasure` crate.
5. **`wal`** (`wal.rs`) — Extent node WAL for small-write durability (F035).

All are exported from `src/lib.rs`.

---

## ExtentNode — Server Side

### Data Model (F021: Multi-Disk)

An `ExtentNode` can manage **multiple disk directories**. Each directory is represented by a `DiskFS` struct (disk_id, base_dir, online flag). File I/O uses `compio::fs::File` directly (no IoEngine abstraction).

All extents use the hashed layout: `{data_dir}/{hash:02x}/extent-{id}.dat` + `.meta`. Hash = `crc32c(extent_id_le_bytes) & 0xFF` (low byte). Hash subdirs are created on-demand — no pre-formatting required. Matches the 256 subdirs created by `autumn-client format`.

Each extent file pair:
- `extent-{id}.dat` — raw data (append-only during active use)
- `extent-{id}.meta` — 40-byte binary sidecar:

| Bytes | Field |
|-------|-------|
| 0–7 | Magic: `EXTMETA\0` |
| 8–15 | `extent_id` (le u64) |
| 16–23 | `sealed_length` (le u64) |
| 24–31 | `eversion` (le u64) |
| 32–39 | `last_revision` (le i64) |

`ExtentEntry` stores `disk_id` for path resolution. `choose_disk()` returns the first online disk (matches Go's strategy). `df()` returns real `statvfs` stats per disk.

**Multi-disk usage** (production):
```bash
# Format disks and register with manager
autumn-client --manager ... format --listen :9101 --advertise host:9101 /disk1 /disk2

# Start node with multiple disks (comma-separated or repeated)
autumn-extent-node --data /disk1,/disk2 --manager ... [--wal-dir /nvme/wal]
```

**Single-disk usage** (tests / backward compat):
```bash
autumn-extent-node --data /tmp/data --disk-id 1 --manager ...
```

In memory, `ExtentNode` holds a `Rc<DashMap<u64, Rc<ExtentEntry>>>` (single-threaded compio, no `Arc`/`Mutex` needed):

```rust
struct ExtentEntry {
    file: UnsafeCell<CompioFile>, // compio::fs::File, UnsafeCell for async I/O
    len: AtomicU64,               // current byte length
    eversion: AtomicU64,          // bumped on seal or eversion change
    sealed_length: AtomicU64,     // 0 = active; >0 = sealed at this length
    avali: AtomicU32,             // availability flag (non-zero = sealed)
    last_revision: AtomicI64,     // most recent owner revision seen
    disk_id: u64,                 // immutable after creation
}
```

No `write_lock` — appends are serialized by the single-threaded compio runtime (sequential processing in `handle_connection`).

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

### Append Protocol (eversion check → seal check → fencing → commit truncation → write)

```
Append(AppendReq via autumn-rpc binary frame):
  1. Decode binary request (extent_id, eversion, commit, revision, must_sync, payload)
  2. Eversion check:
       - If client eversion > local: fetch ExtentInfo from manager, apply if sealed
       - If client eversion < local: reject (PRECONDITION_FAILED)
  3. Sealed check: reject if sealed_length > 0 or avali > 0
  4. Revision fencing:
       - If header.revision < last_revision: reject (CODE_LOCKED_BY_OTHER — stale owner)
       - If header.revision > last_revision: update last_revision, persist meta
  5. Commit reconciliation:
       - If local file len < header.commit: reject (data loss on our side)
       - If local file len > header.commit: TRUNCATE file to header.commit
  6. Write payload:
       - WAL path (must_sync=true AND payload ≤ 2MB AND WAL enabled):
           futures::join!(wal.write(record), file.write_at(start, payload))
       - Direct path:
           file.write_at(start, payload)
           if must_sync: file.sync_all()
  7. Advance extent.len
  8. Return (offset=start, end=start+payload_len)
```

No `write_lock` — appends are serialized by sequential processing within `handle_connection`. The `end` watermark guarantee: returning end=N means all data in 0..N is written.

Step 5 (commit-based truncation) is the key to consistency: it effectively replaces a traditional WAL by using the data files themselves as journals.

### WAL (wal.rs)

Small must_sync writes (≤ 2MB) use the WAL for lower-latency durability:

- **Format**: Pebble/LevelDB-style 128KB block framing. Each chunk has a 9-byte header: `[CRC32C: 4B][len: 4B][type: 1B]`. Chunk types: FULL=1, FIRST=2, MIDDLE=3, LAST=4.
- **Record**: `[uvarint extent_id][uvarint start][i64 revision][uvarint payload_len][payload]`
- **Synchronous writes**: `Wal` directly owns the `RecordWriter` (std::fs::File). No background task, no channels. `write()` and `write_batch()` are blocking sync calls — acceptable because WAL writes are sequential and fast (single rotating file, single fsync). Called from the compio event loop thread.
- **Batch support**: `write_batch(&mut self, records: &[WalRecord])` writes all records then syncs once, amortizing fsync cost. Used by `handle_append_batch` for pipelined writes.
- **Rotation**: at 250MB; old WAL files are kept for replay then deleted.
- **Startup replay**: `replay_wal_files()` called in `ExtentNode::new()` after `load_extents()`. Each record is written back to the extent file at `record.start`; idempotent if extent already has the data.
- **Config**: `ExtentNodeConfig::with_wal_dir(PathBuf)`. Binary defaults to `data_dir/wal/`.
- **Ownership**: Stored as `Option<Rc<RefCell<Wal>>>` in `ExtentNode`. Single-threaded compio, no Arc/Mutex needed.

### Commit Protocol Explained

The `StreamClient` computes `commit = min(commit_length on all replicas)` before each append. Any replica that got ahead (e.g., partially acknowledged data before a crash) is truncated back to the consensus point on the next append. The WAL provides per-node durability on top of this protocol.

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

Used to bring a **sealed** extent's lagging replica up to date (e.g., after a node comes back online):
- If local data >= `sealed_length`: already up to date, return OK.
- Otherwise: copy full extent from peers, truncate, rewrite, sync.

### Heartbeat & Df

- `heartbeat`: streams a "beat" payload every second (keep-alive for the manager).
- `df`: returns disk space info (currently hardcoded placeholder) + drains `recovery_done` to report completed recovery tasks. This is the mechanism by which the manager learns recovery finished.

### F109: Delete Extent (`MSG_DELETE_EXTENT = 11`)

Idempotent unlink for the physical `extent-{id}.dat` + `.meta` files
after the manager has confirmed `refs == 0`. Sent fire-and-forget by
the manager's `extent_delete_loop` once per replica.

```
handle_delete_extent(extent_id):
  1. F099-M shard ownership: if !owns_extent → forward to sibling shard.
  2. extents.remove(&id) — pull the in-memory ExtentEntry out so any
     subsequent append fails fast with NotFound. Any pwritev that
     already took the file handle keeps its inode (POSIX preserves
     open fds across unlink); the data is meaningless because manager
     refs are 0.
  3. DiskFS::remove_extent_files(id):
       a. compio::fs::remove_file({base}/{hash:02x}/extent-{id}.dat)
       b. compio::fs::remove_file({base}/{hash:02x}/extent-{id}.meta)
       Both NotFound errors are downgraded to Ok(()) — the contract is
       idempotent so manager retries are safe.
  4. Returns CodeResp { code: CODE_OK | CODE_ERROR }.
```

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
- **Return type change (4.3)**: `connect` / `new_with_revision` now return `Rc<StreamClient>`.
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
| `read_bytes_from_extent(extent_id, offset, length)` | Read from extent; replication: try replicas in order, **chunked at `AUTUMN_STREAM_READ_CHUNK_BYTES` (default 256 MiB)** so reads >2 GiB don't trip the per-syscall pread ceiling on macOS (INT_MAX) / Linux (0x7ffff000); EC: parallel shard reads with decode (per-shard size already bounded). `length=0` resolves to-end via `sealed_length` (sealed extents) or `commit_length_for_extent` (open extents) before chunking. |
| `read_last_extent_data(stream_id)` | Read last non-empty extent of a stream |
| `punch_holes(stream_id, extent_ids[])` | GC: remove extents from stream |
| `truncate(stream_id, extent_id)` | Remove all extents before extent_id |
| `get_stream_info(stream_id)` | Query StreamInfo from manager |
| `get_extent_info(extent_id)` | Query ExtentInfo from manager |
| `multi_modify_split(req)` | Forward partition split to manager |

---

## Programming Notes

1. **Always pass the correct `revision`** — passing 0 or a stale revision will cause `CODE_LOCKED_BY_OTHER` from ExtentNode (propagated as immediate non-retried error by StreamClient). The revision is set at `StreamClient::connect` time.

2. **Eversion changes on seal** — if the manager seals an extent (e.g., during split or extent rolling), the eversion is bumped. The next append will see a mismatched eversion, fetch the updated ExtentInfo, and handle accordingly.

3. **Parallel 3-replica fanout (F099-B)** — `launch_append` fires the 3 per-replica `pool.send_vectored` futures concurrently via `futures::future::join_all`. Each per-replica future awaits its own RpcClient submit channel independently, so one slow/back-pressured replica doesn't serialise the others. Per-replica TCP byte order is still preserved because each RpcClient runs a single-writer `writer_task` (R4 step 4.1) — the fanout order across replicas is irrelevant because every replica is independent. The `AppendResp.offset/end` consistency check in `apply_completion` still enforces that all replicas agree on the file-level offset.

4. **`must_sync` cost** — for small payloads (≤ 2MB) with WAL enabled, triggers parallel WAL sync + async extent write; the WAL sequential write is faster than random `sync_all()` on the extent file. For large payloads or WAL-disabled nodes, falls back to `sync_all()` on the extent file. Only set for records requiring guaranteed durability (e.g., partition WAL entries). SSTable data doesn't need `must_sync` since replication provides durability.

5. **StreamClient is always held as `Rc<StreamClient>`** — constructors return `Rc<Self>` (via `Rc::new_cyclic`) so per-stream workers can hold `Weak<StreamClient>` for the removal-guard. Callers clone the `Rc` to share. Public API methods take `&self`, so `sc.append(...)` works transparently.

6. **EC for all three streams (post-fix)** — EC is a per-stream property. All three streams (`log_stream`, `row_stream`, `meta_stream`) can be EC-converted on seal. Replication factor is fixed at **3** while open; EC default keeps `M=1` parity and grows `K = N - 1` capped at 4 (so N=4 → 3+1, N=5 → 4+1, N≥6 → 4+1; cap bounds RS decode cost). `log_stream`'s arbitrary VP `(extent, offset, length)` sub-range reads are handled by `ec_subrange_read`'s generalised N-shard parallel scatter — one `read_shard_from_addr` per touched data shard, stitched in order; single- and two-shard cases fall out as plan-of-1 / plan-of-2 special cases. **Bug history (2026-04-27 production crash)**: with log_stream EC'd by default, three independent bugs combined to crash PS:
    - `cluster.sh` defaulted log_stream EC to a fixed 3+1 / 2+1 shape regardless of N — fine semantically, but exposed the next two bugs.
    - `ec_subrange_read`'s "two-shard fast path" only checked `end_shard < data_shards`. On K=3, a 64 MiB GC chunk read (start_shard=0, end_shard=2) entered this branch and joined shard 0 + shard 2's prefix while silently skipping shard 1. The server's CODE_OK-with-short-payload behaviour produced a buffer of the right total length but with the middle ~15 MiB replaced by shard 2's prefix → GC's record-stream parser surfaced `trailing bytes did not form a complete record`.
    - `handle_stream_alloc_extent` had no idempotency guard: when a writer's soft-error retry called `alloc_new_extent(stream_id, 0)` on an *already-sealed* tail (post-EC), the manager re-ran the seal block — re-queried commit_length on each replica (which after `write_shard_local` returned only `shard_size`), took the min, and CLOBBERED `tail.sealed_length` from the original payload size down to `shard_size`. Existing VPs in `[shard_size, original_payload_len)` were now past sealed_length, and `ec_read_full_and_slice` panicked the partition thread with `range start index N out of range for slice of length L`.

    Fixes:
    - cluster.sh: log + row default to `3+1` (N=4) / `4+1` (N≥5), with `M=1` parity, `K` capped at 4.
    - `ec_subrange_read` rewritten as generalised N-shard parallel scatter; out-of-range offset (`start_shard >= data_shards`) routes to `ec_read_full_and_slice` whose extracted `ec_slice_decoded` helper returns `Err` (not panic) on `offset > full_payload.len()`. Saturating-sub on `read_len` prevents unsigned wrap. Regression tests in `mod ec_slice_tests` (`client.rs`).
    - `handle_stream_alloc_extent` (manager): if `tail.sealed_length > 0`, skip the entire seal block (no commit_length re-query, no overwrite of `sealed_length / eversion / avali`). New-tail allocation still proceeds. This is the load-bearing fix: it preserves the manager's seal-time `sealed_length` against post-EC re-seal corruption.

7. **EC offset semantics** — In EC mode, `AppendResult.offset/end` are shard-level byte offsets. Each shard has `shard_size(payload_len, data_shards)` bytes. Upper layers treat these as opaque — they pass them unchanged to `read_bytes_from_extent`. The EC read path handles the decode transparently.

8. **EC shard index = position in replicates++parity** — `replica_addrs_from_cache` chains `replicates` then `parity` node IDs. Shard index `i` corresponds to address `i` in this combined list. The encode output shard `i` is sent to the `i`-th node. The recovery `replacing_index` uses the same ordering.

9. **Commit tracking is local, not per-append RPC** — `state.commit` is a plain `u32` (not `Option`), matching Go's `sc.end` pattern. It starts at 0 and is updated to `appended.end` after each successful append. After allocating a new extent, it resets to 0. `current_commit()` (which RPCs all replicas) exists for partition load time only, never in the hot append path.

10. **Extent allocation is capped per append** — `append_payload` allows at most 3 new extent allocations per single append call (`MAX_ALLOC_PER_APPEND`). This prevents runaway empty extent creation if appends persistently fail.

11. **ConnPool is single-kind (post-F093)** — `ConnPool` keys by `SocketAddr` alone; each address owns one sequential `RpcConn` on a `Rc<RefCell<Option<RpcConn>>>` (take/put pattern; if taken, a fresh connection is opened on the fly). Historical note: F087-bulk-mux introduced a `PoolKind::{Hot, Bulk}` distinction so 128 MB SSTable uploads wouldn't head-of-line-block small WAL frames on the same socket. F088 moved flush to a dedicated P-bulk OS thread with its own ConnPool + StreamClient, so the P-log SC now only carries WAL (+ rare compact writes) and the shared-socket HoL scenario no longer exists. F093 removed `PoolKind`, `set_stream_kind`, and `kind_for` as dead code.

12. **Chunked reads for >2 GiB extents (F105)** — `read_bytes_from_extent` splits requests larger than `AUTUMN_STREAM_READ_CHUNK_BYTES` (default 256 MiB) into multiple per-replica RPCs and concatenates the results. Without chunking, a single `pread` of 3 GiB on the extent_node returns `EINVAL` (errno 22) — macOS caps at `INT_MAX` (~2 GiB) and Linux at `0x7ffff000`. The pre-F105 GC + recovery path slurped sealed extents in one shot via `read_bytes_from_extent(eid, 0, sealed_length)`; once a sealed log_stream extent grew past 2 GiB, GC got stuck retrying every 30 s ("rpc status Internal: Invalid argument (os error 22)") and recovery would refuse to open the partition on the next restart. `length=0` ("to end") resolves the byte count via `ExtentInfo.sealed_length` for sealed extents or `commit_length_for_extent` (min-replica) for open extents, then chunks. EC reads stay on the per-shard path (`ec_subrange_read`) — each shard is at most `sealed_length / data_shards` so the per-syscall ceiling is rarely hit there. Test override: integration tests set `AUTUMN_STREAM_READ_CHUNK_BYTES` to small values (e.g. 1024) to exercise the chunked path without writing multi-GiB extents.

13. **Chunked local-file I/O for >2 GiB extents (F115)** — The F105 fix (note 12) only covered the `StreamClient` RPC path. The `ExtentNode` server-side local-file operations had the same EINVAL exposure on all full-extent I/O paths. Fixed by two helpers in `extent_node.rs` (`FILE_IO_CHUNK_BYTES = 256 MiB`):
    - `file_pread_chunked` — used by `handle_convert_to_ec`, `handle_read_bytes`, `handle_copy_extent`
    - `file_pwrite_chunked` (splits via `Bytes::split_to`, O(1) no-copy) — used by `run_recovery_task`, `handle_re_avali`, `write_shard_local`
    Both fast-path the common case: single syscall when payload ≤ 256 MiB; only loop when larger. Any new full-extent local-file read or write **must** use these helpers — never call `file_pread`/`file_pwrite` directly with a `sealed_length`-sized buffer.

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
