# autumn-partition-server Crate Guide

## Purpose

An LSM-tree based KV store built on top of the stream layer. Each `PartitionServer` owns one or more **partitions**, each covering a contiguous key range. Implements the `PartitionKv` gRPC service.

## Architecture

### Thread Model (post F099-J/K/I, 2026-04-20)

```
Main compio thread (control plane + fd dispatcher)
├─ heartbeat_loop          ← periodic manager heartbeat
├─ region_sync_loop        ← discover/open/close partitions
└─ fd-dispatch loop        ← rx.next() → partition handle.fd_tx

Accept OS thread (blocking)
└─ std::net::accept → tx   ← dedicated accept, sends to main via channel

Partition threads — 2 OS threads per partition:
├─ part-N (P-log): OWNS
│     • merged_partition_loop (request dispatch + group-commit SQ/CQ)
│     • fd-drain task: fd_rx.next() → compio::TcpStream → spawn ps-conn task
│     • ps-conn task × K (one per live client connection, all on this runtime)
│     • background_flush_loop, background_compact_loop, background_gc_loop
│     • PartitionData (Rc<RefCell>) shared across all tasks on this runtime
│     • dedicated StreamClient + ConnPool for log_stream/meta_stream
│     • F099-D: write loop inlined into merged_partition_loop (no spawn/oneshot)
│     • F099-J: ps-conn tasks collocated here; per-request mpsc hop is now
│       same-thread (no eventfd, no cross-thread futex).
├─ part-N-bulk (P-bulk): flush_worker_loop
│     • own compio runtime + io_uring + ConnPool + StreamClient
│     • runs build_sst_bytes + row_stream.append + save_table_locs_raw
│
P-log → P-bulk: mpsc::Sender<FlushReq> (capacity 1 → sequential flushes)
P-bulk → P-log: oneshot::Sender<Result<(TableMeta, SstReader)>>
```

**Thread count**: `1 main + 1 ps-accept + 2N partition` = `2N + 2` OS threads.
At N=1 this is **4** OS threads total (vs pre-F099-J `3 + (CPU-count workers) + 2 = ~194`).

**Why two OS threads per partition?** A 128 MB row_stream flush holds the P-log
compio runtime for hundreds of ms (syscall + 3-replica fanout CQE wait), head-
of-line-blocking the log_stream 4 KB WAL batches sharing the same io_uring. The
F087-bulk-mux pool split separated the TCP sockets, but the runtime was still
single-threaded. F088 gives flush its own runtime so WAL appends make forward
progress concurrently with SST uploads.

**How ps-conn handoff works (F099-J + F099-K)**:
- Post F099-K, each partition OS thread binds its OWN `compio::net::TcpListener`
  on a unique port (`base_port + ord`) and runs its own accept loop + ps-conn
  tasks on the SAME compio runtime as `merged_partition_loop`. The main thread
  does NOT forward fds across partitions; clients connect directly to the
  owning partition's port (part_addr reported via `MSG_REGISTER_PARTITION_ADDR`
  and served to clients via `GetRegions.part_addrs`).
- Each ps-conn task runs `handle_ps_connection` on THIS runtime. Its
  `req_tx.send(req).await` is a same-thread mpsc send; the matching
  `req_rx.next().await` inside `merged_partition_loop` wakes via a local
  Waker (Rc-based) — no eventfd, no cross-thread futex.

**ps-conn handler — F099-I true SQ/CQ inner loop (commit f099i)**:
`handle_ps_connection` mirrors the ExtentNode R4 4.2 v3 pattern
(`stream::extent_node::handle_connection`, commit `1e7e456`):

```
┌─ handle_ps_connection (one task per TCP conn) ──────────────────┐
│                                                                 │
│  SQ side — persistent read future:                              │
│    Option<LocalBoxFuture<'static, PsReadBurst>>                 │
│    owns OwnedReadHalf + 64 KiB buf across iterations;           │
│    NEVER dropped mid-flight (io_uring SQE stability)            │
│                                                                 │
│  CQ side — FuturesUnordered<LocalBoxFuture<'static, Bytes>>     │
│    cap = AUTUMN_PS_CONN_INFLIGHT_CAP (default 64)               │
│    each future: clone req_tx → send PartitionRequest →          │
│                 await oneshot resp → encode Frame::response     │
│                                                                 │
│  Loop:                                                          │
│    (A) drain ready completions via `.next().now_or_never()`     │
│        → tx_bufs                                                │
│    (B) flush tx_bufs with ONE `write_vectored_all` syscall      │
│    (C) branch on (n_inflight, at_cap):                          │
│       n_inflight == 0 → await read alone; then                  │
│         d=1 FAST PATH: if the burst yielded exactly one          │
│         complete frame AND inflight/tx_bufs are empty,           │
│         run request→response→write inline via `write_all`       │
│         (no FU, no Box::pin, no write_vectored). Restores       │
│         pre-F099-I cost at pipeline-depth=1.                     │
│       at_cap          → await completion alone (back-pressure)  │
│       n_inflight == 1 → await completion (fast path: avoid      │
│           5-10 µs per-iter select polling cost at d=1)          │
│       n_inflight > 1  → select(read, inflight.next())           │
│           Left wins  → process frames, restart read_fut         │
│           Right wins → put read_fut back, extend tx_bufs        │
│    (D) on EOF: drain remaining inflight + final flush + return  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

At `--pipeline-depth=1`: the d=1 fast path engages — after reading a
single-frame burst with no earlier in-flight replies, the ps-conn task
does `tx.send(req) → resp_rx.await → writer.write_all(bytes)` inline.
No `Box::pin(async {...})` heap alloc, no `FuturesUnordered::push`, no
`write_vectored_all([1_iov])` — strictly cheaper than the pre-F099-I
baseline's `write_all(one_frame)` path.

At `--pipeline-depth ≥ N`: one TCP read delivers N frames → all N futures
in `inflight` concurrently → drain-all-ready collects up to N ready replies
into `tx_bufs` → one `write_vectored_all` = one `tcp_sendmsg`. Targeted
win against F099-H's measured 0.8 CPU cores of small-frame TCP kernel
overhead (91 % of `tcp_sendmsg` at 32–63 B PutResp headers, 22 µs each,
34 k/s → ~N× fewer kernel TCP traversals per Put).

Back-pressure: if `inflight.len()` reaches the cap mid-push, the inner
`push_frames_to_inflight` helper awaits one completion before pushing the
next future. This caps memory usage per pathological client (e.g. a large
pipeline-depth burst all targeting the same partition).

Mis-routed frames (`part_id != owner_part`) synthesise an immediate
`NotFound` error frame onto inflight — no mpsc hop. TODO(F099-K):
forward to owning partition's req_tx instead.

**N>1 behaviour (F099-K)**: with per-partition listeners, each
`handle_ps_connection` serves only frames whose `part_id == owner_part`.
The client (autumn-client `perf-check`) is F099-K-aware and opens one
TCP connection to each partition's port, striping requests across them
by partition id.

**Trade-off measured**:
- Pre-F099-J P-log CPU: ~57 % user / 43 % iouring-idle (F099-H §2.3).
- Post-F099-J P-log CPU: ~100 % — ps-conn decode + dispatch + response
  writes all run on this thread.
- Post F099-K: load distributes across N partition threads, each with
  its own listener + P-log.
- Post F099-I: ~N× fewer `tcp_sendmsg` calls at pipeline-depth=N, which
  at the 57 k N=1 × d=1 ceiling accounted for 0.8 CPU cores of pure
  kernel TCP overhead.

```
┌─────────────────── PartitionServer ────────────────────┐
│  Rc<RefCell<HashMap<part_id, PartitionHandle>>>         │
│  (F099-J: no Arc<PartitionRouter> — ps-conn tasks run   │
│           on the P-log runtime and use a same-thread    │
│           PartitionRequest mpsc; no cross-thread wake)  │
│                                                          │
│  ┌──────── PartitionData (per partition thread) ───┐    │
│  │  active: Memtable (RwLock<BTreeMap>, F099-C)     │    │
│  │  imm: VecDeque<Arc<Memtable>>   ← frozen tables  │    │
│  │  sst_readers: Vec<Arc<SstReader>>  ← oldest→new  │    │
│  │  tables: Vec<TableMeta>          ← aligned        │    │
│  │                                                   │    │
│  │  log_stream_id   ← WAL + large values             │    │
│  │  row_stream_id   ← SSTables                       │    │
│  │  meta_stream_id  ← TableLocations checkpoint      │    │
│  │                                                   │    │
│  │  (F099-D: no write_tx — writes come directly from │    │
│  │   req_rx via merged_partition_loop)                │    │
│  │  seq_number: monotonic MVCC counter               │    │
│  │  has_overlap: AtomicU32                           │    │
│  └───────────────────────────────────────────────────┘   │
│                                                          │
│  stream_client: Arc<StreamClient>                        │
└──────────────────────────────────────────────────────────┘
```

## MVCC Key Encoding

Internal (storage) key = `user_key ++ 0x00 ++ BigEndian(u64::MAX - seq_number)`

The null byte (`0x00`) is a **separator** between the user key and the inverted sequence number. This is critical: without the separator, a user key that is a prefix of another (e.g. `"mykey"` and `"mykey1"`) would sort incorrectly in internal-key space. With the separator, `"mykey\x00..."` sorts before `"mykey1\x00..."` because `0x00 < '1'`.

The **inverted** sequence ensures that for the same user key, newer writes (higher seq) sort **before** older writes in byte order. Lookup uses `seek_user_key` which seeks to `user_key ++ 0x00 ++ BE(0)` — the smallest possible internal key for this user key — then returns the first (newest) entry found.

## SST VP dependency tracking (`vp_deps`, 2026-04-29)

Each SST `MetaBlock` now persists `vp_deps: Vec<u64>`: the distinct log extent ids referenced by live `ValuePointer` entries in that SST.

Rules:

1. `vp_deps` is an SST-local fact derived while building the SST (`SstBuilder::add` sees `OP_VALUE_POINTER` entries and records their `extent_id`).
2. `vp_deps` is persisted in rowStream as part of the SST MetaBlock and recovered through `SstReader.vp_deps`.
3. `vp_deps` is NOT a refcount. The manager-owned aggregate is `MgrExtentInfo.vp_table_refs`, computed from full partition snapshots.

`PartitionData` recomputes and syncs the full live-SST snapshot (`extent_id -> number of live SSTs mentioning it`) at three points:

1. right after recovery/open succeeds
2. after every successful flush checkpoint (`save_table_locs_raw`)
3. after every successful compaction checkpoint

This closes the split-lifetime bug where shared SSTs could still contain old `ValuePointer`s after the current log stream had already truncated the underlying extent.

## Write Path: Put / Delete (Group Commit, R4 4.4 SQ/CQ, F099-D merged, F099-I batched)

```
Put(key, value, part_id, must_sync):
  1. ps-conn task (F099-I): decode frame from TCP read; push
     `async { clone req_tx → send PartitionRequest → await oneshot resp →
              encode Frame::response }` onto the per-conn inflight
     FuturesUnordered. MULTIPLE frames from the same TCP read end up in
     the inflight set concurrently.
  2. Same-thread mpsc: PartitionRequest delivered into merged_partition_loop.
  3. P-log merged_partition_loop: decode PutReq inline, push a
     WriteRequest with a direct `WriteResponder::Put { outer: resp_tx, key }`
     into the `pending` Vec. NO compio::spawn, NO inner oneshot.
  4. ps-conn awaits the outer resp_tx via the inflight future — the SAME
     oneshot that was sent into the request; Phase 3 fires its encoded
     `PutResp` frame directly into it.
  5. ps-conn loop top: drain-all-ready completions into `tx_bufs`; flush
     `tx_bufs` via ONE `write_vectored_all` syscall — coalescing all Put
     responses that became ready since the previous flush.

merged_partition_loop (per partition, F099-D fold-in of the old
                       background_write_loop_r1):
  OWNS:   FuturesUnordered<Pin<Box<dyn Future<Output = InflightCompletion>>>>
  CAP:    AUTUMN_PS_INFLIGHT_CAP (default 8, range [1, 64])
  GATE:   MIN_PIPELINE_BATCH = 256  (2nd+ batch requires pending >= 256)
  RECV:   req_rx: mpsc<PartitionRequest> (WRITE_CHANNEL_CAP = 1024)
          — the SAME channel that carries reads + writes from ps-conn

  Loop (per iteration):
    (A) drain ready completions via `inflight.next().now_or_never()`
        → run Phase 3 (memtable insert + direct WriteResponder::send_ok) each
    (B) if pending.non_empty && !at_cap && (n_inflight==0 || pending >= 256):
          launch_new_batch:
            Phase 1: validate, seq-assign, encode WAL records
            Launch Phase 2: stream_client.append_batch future (NOT awaited)
            Push (BatchData, Phase2Fut → InflightCompletion) into FU
          continue
    (C) if at_cap:
          await inflight.next() (back-pressure) → run Phase 3
          continue
    (D) branch on n_inflight:
          == 0:  await req_rx.next() alone (cold idle)
          >  0:  select(req_rx.next, inflight.next()) — race SQ vs CQ
                 Left  (SQ wins) → handle_incoming_req:
                                   - PUT/DELETE/STREAM_PUT: decode + pending.push
                                   - GET/HEAD/RANGE/SPLIT/MAINTENANCE: inline
                                     via dispatch_partition_rpc (reads still
                                     run inline on P-log)
                 Right (CQ wins) → run Phase 3 on the completion
    (E) non-blocking drain of any queued requests (still decode inline)

  Shutdown (req_rx closed):
    Drain all inflight via await-loop; run Phase 3 on each so clients
    receive their final ack. Then flush any residual pending as one last
    batch. Finally emit metrics.

  Error handling:
    LockedByOther on any completion  → set locked_by_other flag, drain
      remaining inflight cleanly, return (partition self-evicts in the
      enclosing loop).
    Other append errors              → log + propagate Err(_) to each
      client's oneshot. Loop continues.
```

**Phase 1 / Phase 3 primitives** (`start_write_batch` / `finish_write_batch`
in `background.rs`) are unchanged from R3; Phase 2 is wrapped into a boxed
`InflightCompletion` future. Phase 3 runs at most once per loop iteration
(single-threaded compio task), so the partition write lock is never held
concurrently — `maybe_rotate_locked` remains correct.

`Delete` sends `WriteOp::Delete{user_key}`, writes `op = 2` (tombstone).

### Why a `MIN_PIPELINE_BATCH` gate?

R3 Task 5b found that greedily splitting a naturally-full 256-op burst
into multiple small batches regressed throughput because per-batch
overhead (encode, 3-replica `send_vectored`, lease/ack state machine
cycle) outweighs the concurrency gain of running two small batches in
parallel. The gate says: a *second or later* batch launches only when
pending has grown to the full burst size (256 — matches the
`--threads 256` perf_check workload). The first batch after an idle
period always launches (avoiding starvation on low-load streams).

### Out-of-order completion is correct

Phase 2 completions may arrive in a different order than launch order
(e.g., batch B finishes before batch A if A had a larger payload and
consequently higher write bandwidth). This is fine because:
- **Seq numbers** were assigned in Phase 1 in batch-launch order, so A
  always has lower seqs than B.
- **Memtable MVCC keys** are `user_key ++ 0x00 ++ BE(u64::MAX - seq)`.
  Byte-sort order is independent of insertion order.
- **Client oneshot replies** are per-request, not per-batch-order.
- **LogStream ordering** is preserved by the stream worker's
  lease/ack cursor (step 4.3): both batches land at distinct contiguous
  offsets regardless of Phase 2 completion order.

### Cross-layer SQ/CQ stack (post-R4)

```
┌─ PS merged_partition_loop  (this crate, 4.4 + F099-D)        ┐
│    FU<InflightCompletion>, cap 8                              │
│    (was background_write_loop_r1 before F099-D; merged with   │
│     the request-dispatch loop to remove the per-Put spawn +   │
│     inner oneshot that cost ~30 % of P-log CPU at 256 × d=1)  │
└─────────────┬─────────────────────────────────────────────────┘
              ▼  stream_client.append_batch(log_stream_id, …)
┌─ autumn-stream stream_worker_loop  (step 4.3)                 ┐
│    FU<3-replica-join>, cap 32, per stream_id                  │
└─────────────┬─────────────────────────────────────────────────┘
              ▼  pool.send_vectored per replica
┌─ autumn-rpc writer_task (step 4.1) — single SQ per conn       ┐
└─────────────┬─────────────────────────────────────────────────┘
              ▼  TCP
┌─ autumn-stream handle_connection (step 4.2 v3, server side)   ┐
│    FU<batch-io>, cap 64, persistent read future               │
└───────────────────────────────────────────────────────────────┘
```

## P-bulk SQ/CQ (flush_worker_loop, R4 4.4)

Same FuturesUnordered + select pattern on the bulk thread, cap = 2
(default, env `AUTUMN_PS_BULK_INFLIGHT_CAP`, range [1, 16]). Each
in-flight flush holds a 128 MB SST buffer, so the cap is deliberately
small. The benefit is that while one SST is uploading via
`row_stream.append`, the next flush can start its `build_sst_bytes`
`spawn_blocking` — overlapping CPU (build) with network (upload) without
ballooning peak memory.

**Record format**: `[op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key][value]` (17-byte header)

**No local WAL file**: logStream is the sole write-ahead log. Recovery replays logStream from the VP head recorded in the last metaStream checkpoint.

## Read Path: Get

```
Get(key, part_id):
  1. Check key is in_range
  2. lookup_in_memtable(active, key)
  3. For each imm (newest first): lookup_in_memtable
  4. For each sst_reader (newest first):
       bloom_may_contain? → find_block_for_key → scan block
  5. If found:
       op == 2 → NotFound (tombstone)
       expires_at > 0 && expired → NotFound
       op has OP_VALUE_POINTER → resolve_value (read from log_stream)
       else → return raw value
```

## Flush Pipeline (F088: cross-thread hand-off)

Triggered when `active` exceeds `FLUSH_MEM_BYTES` (256 MB).

```
P-log: background_flush_loop
  1. recv flush_rx signal
  2. snapshot front imm + vp + tables → FlushReq
  3. flush_req_tx.send(req)      ← cross-thread hand-off (capacity 1)
  4. oneshot resp.await           ← ~1 ms–seconds depending on row_stream backlog

P-bulk: flush_worker_loop
  1. recv FlushReq
  2. build_sst_bytes(imm, vp_eid, vp_off)         ← spawn_blocking (CPU)
  3. bulk_sc.append(row_stream_id, sst_bytes)     ← 128 MB network upload
  4. SstReader::from_bytes(Bytes::from(sst_bytes))
  5. resp_tx.send(Ok((new_meta, reader)))

P-log: continuation
  6. part.tables.push(new_meta)
  7. part.sst_readers.push(Rc::new(reader))
  8. part.imm.pop_front()
  9. save_table_locs_raw(part_sc, meta_stream_id, part.tables.clone(), vp)
```

The in-thread legacy path (`flush_one_imm_local`) is retained as a fallback for
when bulk-thread spawn fails.

After flush, `save_table_locs_raw` writes `TableLocations` to `meta_stream` and
**truncates meta_stream to 1 extent** — only the latest checkpoint is kept.

**Checkpoint publication invariant (post-2026-04-29 fix):** only P-log may
publish `metaStream` checkpoints. P-bulk may upload the SST and build the
`SstReader`, but it must not write `TableLocations` from the `FlushReq`
snapshot. With `AUTUMN_PS_BULK_INFLIGHT_CAP > 1`, two in-flight flushes can
complete out of order; publishing from stale `tables_before` snapshots can drop
older SSTs or emit duplicate `(extent_id, offset)` entries in the checkpoint
(`part 19` restart corruption: extent 48 locs `[len=13754, len=8387]`, extent
24 missing). The authoritative checkpoint must be emitted only after P-log has
merged `new_meta` into `part.tables`. Already-corrupted historical checkpoints
must be repaired out of band; do not add silent normalization to the normal
reopen path.

**vp snapshot semantics**: the meta_stream checkpoint records the vp
(`vp_extent_id/vp_offset`) captured at FlushReq send time on P-log — NOT the
current vp at P-bulk commit time. Correctness: during replay, logStream from
the snapshot vp forward will include any records added after snapshot,
re-inserting them into memtable (some may already be in the just-flushed SST,
which is fine — duplicate entries with the same seq are idempotent). Trade-off:
avoids a second round trip; slightly more logStream retained until next flush.

## Compaction

Two modes, run in `background_compact_loop`. Public method: `trigger_major_compact(part_id) -> Result<(), &'static str>` — enqueues via `compact_tx` channel (capacity 1), non-blocking.

### Expiry-Triggered Major Compaction (automatic)
During each periodic timeout tick, the compact loop checks all SST readers for `min_expires_at > 0 && min_expires_at <= now`. If any SSTable contains expired keys, a major compaction is triggered on all tables (which drops expired entries and tombstones). This ensures partitions with TTL keys eventually clean up even without explicit compaction triggers.

### Minor Compaction (periodic, 10–20s jitter)
`pickup_tables` selects tables via one of two strategies:

**Head-extent strategy**: If the oldest extent's tables are < 30% of total data (`HEAD_RATIO`), pick up to 5 (`COMPACT_N`) tables from that extent. This clears old extents to enable `truncate` on `row_stream` (freeing disk/logStream extents).

**Size-tiered strategy**: Sort tables by sequence, find consecutive "small" tables (< 32MB = `COMPACT_RATIO * MAX_SKIP_LIST`), pick up to `COMPACT_N`.

After minor compaction, `do_compact` is called with `major=false`.

### Major Compaction (triggered via `compact_tx`, e.g., after overlap detected)
`do_compact` called with `major=true`. Processes all tables. Additionally:
- Drops tombstones (op=2)
- Drops expired entries
- Drops out-of-range keys (overlap cleanup)
- Clears `has_overlap` flag on success

### `do_compact` Logic (the core, F104 streaming)
```
  1. Read lock: collect SstReaders for selected tables, sort newest-first by last_seq
  2. Create MergeIterator over TableIterators
  3. Streaming merge loop (F104):
       - Maintain ONE in-progress SstBuilder + Vec<(TableMeta, Arc<SstReader>)> new_readers
       - Per item from merge.next():
         - Dedup: skip if same user_key already seen (newest wins)
         - Range filter: skip keys outside partition range
         - Discard tracking: when dropping VP entries, accumulate {extent_id → bytes}
         - Major filter: skip tombstones and expired entries
         - If current SstBuilder size > 2 × MAX_SKIP_LIST: finalize, append
           to row_stream, push (TableMeta, SstReader) into new_readers,
           start a fresh builder
         - Otherwise: SstBuilder.add(key, op, value, expires_at)
       - After loop: attach aggregated discards to the final SstBuilder,
         finalize, append, push to new_readers
       - NO `chunks: Vec<(Vec<IterItem>, u64)>` accumulator — pre-F104 this
         materialized every kept entry as a cloned IterItem (~150 B each
         for VP-path workloads), reaching ~6 GB per partition for a 5 GB
         SST set; with 4 partitions concurrent that compounded to ~24 GB
         on top of input + output bytes. See F104 in feature_list.md.
  4. Atomic swap: write lock → remove old SstReaders + tables → push
     new_readers entries → save_table_locs_raw to meta_stream
       (single linearization point; if we crash before this commit, new
        SSTs in row_stream are orphan bytes and recovery loads from the
        previous meta checkpoint — same crash semantics as pre-F104)
  5. If truncate_id returned: truncate row_stream up to that extent
```

### F104 — Cross-partition compaction concurrency cap
`PartitionServer` holds an `Arc<CompactionGate>` (lib.rs); each partition's
`background_compact_loop` calls `gate.acquire().await` BEFORE invoking
`do_compact` and drops the permit on RAII when the call returns. Default
parallelism = 1 (fully serialized across all partitions on this PS),
overridable via `AUTUMN_PS_MAJOR_COMPACT_PARALLELISM` env var, range [1, 64].
Without this cap, `autumn-client compact ALL` against an N-partition PS
would launch N concurrent `do_compact` calls each holding ~2× SST bytes
in memory, multiplying per-partition peak by N.

## GC (Garbage Collection)

Targets the **logStream** where large values (ValuePointers) are stored.

**Trigger**: periodic (30–60s jitter), via `gc_tx` channel (capacity 1), or via the `Maintenance` gRPC RPC. Two public methods on `PartitionServer`:
- `trigger_gc(part_id) -> Result<(), &'static str>` — enqueue `GcTask::Auto`
- `trigger_force_gc(part_id, extent_ids) -> Result<(), &'static str>` — enqueue `GcTask::Force { extent_ids }`

Auto-selects sealed extents with discard ratio > 40% (`GC_DISCARD_RATIO`), up to 3 per run (`MAX_GC_ONCE`).

**Discard snapshot RPC** (`MSG_GET_DISCARDS = 0x48`, FOPS-01): `handle_get_discards` in `rpc_handlers.rs`
reads a live snapshot of the partition's discard map without any manager state. It:
1. Snapshots `sst_readers` from `part.borrow()` (no await while borrowed).
2. Calls `background::get_discards(&readers)` — same aggregation the GC loop uses.
3. Fetches `log_stream extent_ids` via `part_sc.get_stream_info(log_stream_id)`.
4. Filters via `background::valid_discard(&mut discards, &log_extent_ids)` to drop
   extents already punched by a prior GC run.
5. Returns `(extent_id, reclaimable_bytes)` pairs to the caller.
Used by `autumn-client info` to display `discard: N ext / X pending` per log stream.

**Discard map**: Each SSTable's MetaBlock contains `HashMap<extent_id, reclaimable_bytes>`. During compaction, when a VP entry is dropped (dedup, range filter, tombstone/expiry), its extent_id and value length are added to the discard map. The GC loop aggregates across all SSTable readers.

**`run_gc` for one extent (F106 streaming)**:
```
  1. Loop until cur >= sealed_length:
       a. read_bytes_from_extent(eid, cur, AUTUMN_PS_GC_READ_CHUNK_BYTES)
       b. concatenate carry + chunk → buf
       c. process_gc_chunk(buf):
          - decode complete records left-to-right
          - on partial record at tail, stop; caller saves buf[consumed..]
            as carry for the next chunk
          - per record (if VP and in_range):
            * lookup current live version (active → imm → SSTables)
            * if live VP still points to (eid, offset): re-write value
              via stream_client.append, drop borrow_mut BEFORE awaiting
              the network RPC, then re-acquire borrow_mut to insert
              the updated VP into the memtable
       d. cur += chunk.len()
  2. carry must be empty at end (sealed extent records are byte-aligned);
     non-empty carry → refuse to punch and return error
  3. punch_holes([eid]) on log_stream → manager decrements refs;
     extent is physically freed when refs → 0 across all CoW-shared streams
```

Pre-F106 (~commit before this) `run_gc` slurped the entire sealed
extent into one Vec via `read_bytes_from_extent(eid, 0, sealed_length)`
and held `borrow_mut()` across the per-record `part_sc.append` await.
Two latent bugs: (i) for sealed log_stream extents > 2 GiB, the
extent_node `pread` failed with EINVAL (macOS INT_MAX limit), repeating
forever every 30s GC tick — also addressed by F105 chunked reads at the
StreamClient layer. (ii) the cross-await `RefMut` would panic if any
other task on the P-log runtime tried to borrow `part` during the
in-flight RPC. F106 fixes both: chunked carry-streaming (peak GC RAM
≈ one chunk + one record) and tighter borrow scopes around the await.

**Tunable**: `AUTUMN_PS_GC_READ_CHUNK_BYTES` (default 64 MiB) — chunk
size for the streaming read inside `run_gc`. Matches Go's
~1000-block (≈ 64 MiB) `replayLog` window in `valuelog.go::runGC`.

## Partition Split

`handle_split_part` runs inline on `merged_partition_loop` (the P-log
task) via `dispatch_partition_rpc`, so all partition-state mutations are
single-writer on the partition thread.

```
handle_split_part(req):
  1. Reject if part.has_overlap == 1 (run major compaction first)
  2. F103: fetch authoritative range from manager via MSG_GET_REGIONS
       — PS-local part.rg is set at open_partition and is NOT refreshed
         by sync_regions_once for already-open partitions, so after a
         previous split it still spans the pre-split wide range. Picking
         mid_key against the stale rg yields keys outside the manager's
         narrowed range and multi_modify_split rejects them.
  3. user_keys = unique_user_keys(part).filter(in_range(auth_rg))
       (returns sorted, dedup, tombstone-/expired-filtered keys; F103
        adds the auth-rg filter so CoW-shared SSTable keys spanning the
        old wide range are dropped before mid_key selection)
  4. If user_keys.len() < 2 → FailedPrecondition (run major compaction)
  5. flush_memtable_locked(part): rotate active + flush all imm via
       P-bulk
  6. mid_key = user_keys[user_keys.len() / 2]
  7. commit_length on each of {log, row, meta} stream
  8. multi_modify_split(mid_key, part_id, sealed_lengths) on manager
       (up to 8 retries, exponential backoff 100ms → 2s)
  9. F103: narrow PS-local part.rg to [auth_rg.start, mid_key) AND
       re-evaluate has_overlap by checking each sst_reader's smallest/
       biggest key against the new rg. Without this the same staleness
       bug recurs on the 3rd split.
```

After split, both child partitions' on-disk SSTables still span the
pre-split wider range (via CoW-shared extents). Per F103 step 9 the
left (split source) partition immediately observes `has_overlap = 1`
and refuses subsequent splits until major compaction drops the out-of-
range keys and clears the flag. The right (newly created) partition
gets opened by `sync_regions_once`, where `open_partition` evaluates
overlap against its (correct) authoritative range and likewise sets
`has_overlap = 1`.

## Crash Recovery (`open_partition`)

```
  0. Check commit_length on all 3 streams (log/row/meta) — infinite retry with 5s backoff
       Ensures last extent of each stream has consistent commit length across replicas
       (equivalent to Go checkCommitLength)
  1. Read last TableLocations checkpoint from metaStream
       (iterate all extents backward, find first non-empty)
  2. For each location: read SST bytes from rowStream, open SstReader
  3. Compute max seq_number and VP head (vp_extent_id, vp_offset) from SSTables
  4. Replay logStream from VP head forward:
       - Read extent data from vp_extent_id onward
       - Decode WAL records, re-insert into recovered memtable (active)
       - Large values (>4KB): VP points to record in logStream
       - Records with ts ≤ max_seq (already in SSTables) are skipped
  5. PartitionData.active = recovered memtable (preserves unflushed entries)
  6. F107: log final state (`open_partition: ready` with tables=N,
     sst_readers=N, has_overlap, max_seq, vp_extent_id, vp_offset) so
     operators can correlate a user-issued `compact <PARTID>` against
     the actual partition state — the major-compact path skips when
     `tables.len() < 2 && has_overlap == 0` (matches Go reference;
     correct when there's nothing to merge), but pre-F107 the silent
     skip and missing open-time state hid this from users.
  7. Spawn P-bulk OS thread (flush_worker_loop on own compio runtime)
  8. Spawn P-log background tasks on this thread: flush_loop (dispatcher),
     compact_loop, gc_loop, write_loop, dispatch_rpc
```

**F105 chunked reads also apply here**: the logStream replay step reads
each extent via `read_bytes_from_extent(eid, start_off, 0)`. Pre-F105
this would EINVAL on a >2 GiB sealed extent and prevent the partition
from opening at all — the GC failure was the visible symptom, but the
recovery failure was a ticking time bomb. F105's chunking inside
`StreamClient::read_bytes_from_extent` covers both paths transparently.

**Historical checkpoint repair (2026-04-29):** the bad `part 19` checkpoint was
repaired with a one-off server binary, `repair_metastream`, which appends a new
`TableLocations` record to the target meta stream and leaves normal recovery
strict. Use this path for preserved broken data; keep `recover_partition`
simple and authoritative.

## Fault Recovery: LockedByOther Self-Eviction

If the `merged_partition_loop` receives a `CODE_LOCKED_BY_OTHER` error from the stream layer
(meaning a newer partition owner has taken the lock), it sets a `locked_by_other` flag.
The main partition loop checks this flag on each request and exits if set.
This prevents split-brain where two PS nodes serve the same partition.

## F111: Heartbeat must outlive `sync_regions_once`

`finish_connect` spawns `heartbeat_loop` immediately after `register_ps`
succeeds, BEFORE the (potentially long) `sync_regions_once`. With
several hundred MiB of unflushed WAL across N partitions,
`sync_regions_once` can take 10+ s — past the manager's
`PS_DEAD_TIMEOUT` (10 s, F069). Pre-F111 the spawn lived in `serve()`
which only runs after `finish_connect` returns, so the first heartbeat
landed AFTER the manager had already evicted the PS, leaving every
region's `ps_addr` permanently `unknown`.

The `heartbeat_loop` also decodes the `CodeResp` from the manager. On
`CODE_NOT_FOUND` (manager doesn't know this `ps_id`) it logs a WARN
and re-runs `register_ps` + `sync_regions_once` so a transient
eviction (network blip, etcd lease hiccup) self-heals. Pre-F111 the
manager silently returned `CODE_OK` for unknown ps_id, so the running
PS never noticed it had been evicted.

## SSTable Format

### File Layout
```
[Block 0][Block 1]...[Block N][MetaBlock bytes][meta_len: u32 LE]
```
The last 4 bytes are `meta_len` — used by `SstReader::open` to locate the MetaBlock.

### Block Layout (64KB target, max 1000 entries)
```
[Entry 0][Entry 1]...[Entry N][entry_offsets: N×4B LE][num_entries: 4B LE][crc32c: 4B LE]
```

### Entry Layout (prefix-compressed)
```
[EntryHeader: 4B = overlap:u16 LE + diff_len:u16 LE][diff_key][op:1B][val_len:4B LE][expires_at:8B LE][value]
```
`overlap` = bytes shared with the block's **base key** (first key of the block, stored in MetaBlock index). Only the diff suffix is stored. This is **prefix compression**.

### MetaBlock Layout
```
MAGIC "AU7B" (4B) | VERSION (2B)
num_blocks (4B)
  per block: [key_len:2B][base_key][relative_offset:4B][block_len:4B]
bloom_len (4B) | bloom_data
smallest_key_len (2B) | smallest_key
biggest_key_len (2B) | biggest_key
estimated_size (8B)
seq_num (8B)
vp_extent_id (8B) | vp_offset (4B)
compression_type (1B, always 0)
discard_count (4B)
  per entry: [extent_id:8B][size:i64 8B]
min_expires_at (8B, 0 = no expiring keys)
crc32c (4B)
```

### Bloom Filter

Double hashing with xxh3:
- `h1 = xxh3_64(user_key)`, `h2 = xxh3_64_with_seed(user_key, SEED)`
- `hash_i = (h1 + i * h2) mod num_bits`

Operates on **user keys only** (8-byte MVCC suffix stripped before hashing). 1% target FPR, initial capacity 512 keys. Encoding: `[num_bits:4B LE][num_hashes:4B LE][bits...]`.

### Iterators

- `BlockIterator`: scan entries within one decoded block; `seek` via binary search over entry offsets.
- `TableIterator`: spans all blocks; advances to next block when current exhausted.
- `MergeIterator`: N-way merge of TableIterators; for duplicate internal keys, lower-index iterator (newer data) wins; `next()` advances ALL iterators at the current minimum key.
- `MemtableIterator`: snapshot of memtable entries as sorted Vec; uses `partition_point` for seek.

## Key Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `VALUE_THROTTLE` | 4 KB | Large value threshold (store as VP) |
| `FLUSH_MEM_BYTES` | 256 MB | Memtable size trigger for rotation |
| `MAX_SKIP_LIST` | 256 MB | Maximum skip list size |
| `MAX_WRITE_BATCH` | 256 | Max requests per group-commit batch |
| `BLOCK_SIZE_TARGET` | 64 KB | Target SSTable block size |
| `GC_DISCARD_RATIO` | 0.4 (40%) | Min discard ratio to trigger GC |
| `OP_VALUE_POINTER` | 0x80 | Op flag bit for ValuePointer entries |
| `MAX_IMM_DEPTH` (F120-A) | 4 | imm queue cap; merged_loop stalls req intake when reached. RocksDB's `max_write_buffer_number`. Env: `AUTUMN_PS_MAX_IMM_DEPTH` ([1, 64]). |
| `MAX_WAL_GAP` (F120-B) | 2 GiB | force-rotate active when `active.bytes + Σ imm.bytes` exceeds this. RocksDB's `max_total_wal_size`. Env: `AUTUMN_PS_MAX_WAL_GAP` ([128 MiB, 64 GiB]). |
| `SHUTDOWN_TIMEOUT_MS` (F120-C) | 60_000 | per-partition graceful drain deadline before SIGKILL fallback. Env: `AUTUMN_PS_SHUTDOWN_TIMEOUT_MS` ([1_000, 600_000]). |

## F120 — bounded recovery replay

**The problem (2026-04-27):** A killed-mid-write PS that had pushed many imm
tables behind a slow P-bulk left the entire `(vp_offset, log_stream commit)`
window for restart-time replay. Witnessed at **1.96 GB on partition 15** with
several hundred MiB across siblings, surfacing as a 16 GB process footprint
post-restart.

**The three fixes:**

1. **F120-A — imm depth cap + back-pressure.** `merged_partition_loop` reads
   `imm_full = part.imm.len() >= MAX_IMM_DEPTH` at top of loop. When full it
   skips both batch launches (B) and `req_rx.next()` (D), only polling
   `inflight.next()` and a new `imm_drained_rx` channel. `flush_one_imm`
   (and the legacy `flush_one_imm_local` fallback) signal `imm_drained_tx`
   after each successful `imm.pop_front()` so the loop wakes and resumes
   request intake. Worst-case unflushed-WAL window per partition is now
   `MAX_IMM_DEPTH * FLUSH_MEM_BYTES + active.bytes` = 1.25 GB.

2. **F120-B — WAL-gap forced rotate.** After each iteration of
   `merged_partition_loop`, compute `gap = active.bytes + Σ imm[i].bytes`.
   If `gap > MAX_WAL_GAP` AND `imm.len() < MAX_IMM_DEPTH`, call
   `rotate_active`. Bounds replay window for workloads that don't fill
   `FLUSH_MEM_BYTES` before triggering rotate (e.g. mostly-large-value
   writes where memtable is light but log_stream grows fast via VPs).

3. **F120-C — graceful shutdown.** New `PartitionServer::shutdown()` sends
   a `oneshot::Sender<()>` per partition through `drain_tx`. The
   `merged_partition_loop` picks it up via select, sets `drain_ack`, exits
   the main loop, runs the existing tail-drain block (in-flight + pending),
   THEN rotates `active` and loops `flush_one_imm` until imm empties,
   replies on the oneshot, exits. `serve_until_shutdown(addr,
   shutdown_signal)` wraps `serve()` with a future the binary drives from a
   SIGTERM/SIGINT handler. `cluster.sh stop` waits up to 60 s instead of
   the previous 5 s before SIGKILL fallback.

## Programming Notes

1. **Flush is 3-phase** — never hold the write lock during SSTable construction or stream I/O. Only take the write lock for the final reader swap.

2. **`pickup_tables` has two strategies** — understand both head-extent and size-tiered paths before modifying compaction selection logic.

3. **Discard map pipeline**: compaction drops VP entry → accumulates size in local `discard` map → attached to last output SST's MetaBlock → persisted to metaStream → aggregated by GC loop from all SstReaders. Break any link in this chain and GC will not collect dead VP data.

4. **`has_overlap` blocks split but not reads** — reads with `has_overlap` set do range-filter in `range()`. `get()` does NOT filter (point lookups are exact). Only `range()` scans need filtering.

5. **No local WAL file** — logStream is the sole WAL. All writes (small and large) go to logStream via `append_batch`. Recovery reads logStream from the VP head checkpoint in metaStream. If no checkpoint exists (tables is empty AND vp_eid == 0), recovery replays logStream from the very first extent, offset 0 — this covers partitions that accepted writes but were killed before their first flush. Unflushed imm tables that are in memory are also covered: logStream contains all records newer than the last SSTable flush.

6. **Group commit batching** — the merged_partition_loop drains up to MAX_WRITE_BATCH (256) requests per RPC cycle. If ANY request in a batch has `must_sync=true`, the entire batch is synced. This allows `--nosync` clients to piggyback on sync requests from other clients without extra overhead.

7. **Per-partition StreamClient** — each `PartitionData` holds its own `stream_client: Arc<StreamClient>` (no Mutex) created via `StreamClient::new_with_revision`. StreamClient is internally concurrent via per-stream locking (`DashMap<stream_id, Arc<Mutex<StreamAppendState>>>`). Different streams (log/row/meta) are fully concurrent; the same stream is serialized. The server-level `PartitionServer.stream_client` is used only in `split_part` for coordination RPCs.

8. **`start_write_batch` / `finish_write_batch` lock scope** — the write lock is held only for seq number assignment and block encoding (Phase 1), then released before the `append_batch` network RPC (Phase 2), then re-acquired for memtable insert and VP head update (Phase 3). This prevents the partition write lock from blocking reads/flushes/compaction during network I/O.

7. **`sst_readers` and `tables` are always aligned by index** — `tables[i]` and `sst_readers[i]` refer to the same SSTable. Operations on these must maintain alignment. Compaction's atomic swap replaces slices, not individual elements.

9. **Memtable backing = `parking_lot::RwLock<BTreeMap>` (F099-C)** — the active memtable has exactly one writer (the P-log thread's `merged_partition_loop` Phase 3) and N readers (ps-conn `handle_get` call sites + P-log itself). Correctness properties:
   - Writer holds the write lock for the duration of one `insert_batch` call (hot path, up to 256 entries), then releases. Subsequent readers take the read lock AFTER the writer releases → linearisable Put-then-Get.
   - Rotation (`rotate_active`) replaces the whole `Memtable` struct via `std::mem::replace` on the owning `PartitionData`; this is safe because `rotate_active` runs exclusively on P-log inside a `RefCell::borrow_mut`.
   - `imm: VecDeque<Arc<Memtable>>` — after rotation, frozen memtables are read-only from both P-log (during flush + GC + compaction) and P-bulk (during `build_sst_bytes`). Multiple readers acquire the read lock concurrently.
   - Hot path uses `insert_batch(iter)` (one write lock per batch of 256 inserts, not 256 locks), and `for_each(closure)` (read lock held for the iteration — used by `build_sst_bytes` and `rotate_active`).
   - The `bytes: AtomicU64` counter is not inside the lock, so `mem_bytes()` and `maybe_rotate` stay lock-free.
