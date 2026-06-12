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
│     • partition_loop (request dispatch + group-commit SQ/CQ)
│     • fd-drain task: fd_rx.next() → compio::TcpStream → spawn ps-conn task
│     • ps-conn task × K (one per live client connection, all on this runtime)
│     • background_flush_loop, background_compact_loop, background_gc_loop
│     • PartitionData (Rc<RefCell>) shared across all tasks on this runtime
│     • dedicated StreamClient + ConnPool for log_stream/meta_stream
│     • F099-D: write loop inlined into partition_loop (no spawn/oneshot)
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
  tasks on the SAME compio runtime as `partition_loop`. The main thread
  does NOT forward fds across partitions; clients connect directly to the
  owning partition's port (part_addr reported via `MSG_REGISTER_PARTITION_ADDR`
  and served to clients via `GetRegions.part_addrs`).
- Each ps-conn task runs `handle_ps_connection` on THIS runtime. Its
  `req_tx.send(req).await` is a same-thread mpsc send; the matching
  `req_rx.next().await` inside `partition_loop` wakes via a local
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
│  │   req_rx via partition_loop)                │    │
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
  2. Same-thread mpsc: PartitionRequest delivered into partition_loop.
  3. P-log partition_loop: decode PutReq inline, push a
     WriteRequest with a direct `WriteResponder::Put { outer: resp_tx, key }`
     into the `pending` Vec. NO compio::spawn, NO inner oneshot.
  4. ps-conn awaits the outer resp_tx via the inflight future — the SAME
     oneshot that was sent into the request; Phase 3 fires its encoded
     `PutResp` frame directly into it.
  5. ps-conn loop top: drain-all-ready completions into `tx_bufs`; flush
     `tx_bufs` via ONE `write_vectored_all` syscall — coalescing all Put
     responses that became ready since the previous flush.

partition_loop (per partition, F099-D fold-in of the old
                       background_write_loop_r1):
  OWNS:   FuturesUnordered<Pin<Box<dyn Future<Output = InflightCompletion>>>>
  CAP:    AUTUMN_PS_INFLIGHT_CAP (default 8, range [1, 64])
  GATE:   none (F256 natural batching — launch whatever is pending as
          soon as a pipeline slot is free; see "Natural batching" below)
  RECV:   req_rx: mpsc<PartitionRequest> (WRITE_CHANNEL_CAP = 1024)
          — the SAME channel that carries reads + writes from ps-conn

  Loop (per iteration):
    (A) drain ready completions via `inflight.next().now_or_never()`
        → run Phase 3 (memtable insert + direct WriteResponder::send_ok) each
    (B) if pending.non_empty && !at_cap && !imm_full (F256):
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
                                   - PUT/DELETE: decode + pending.push
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

### Natural batching (F256 — replaced the `MIN_PIPELINE_BATCH` gate)

History: R3 Task 5b found that greedily splitting a naturally-full
256-op burst into multiple small batches regressed throughput, so a
gate was added: a *second or later* batch launched only when pending
reached 256. But whenever per-partition concurrency was below 256 —
the common case at N>1 partitions (perf baseline: 16 in-flight per
partition) — pending could NEVER reach 256, so after the first launch
the loop waited for the WHOLE pipeline to drain (`n_inflight == 0`)
before launching again. Effective pipeline depth = 1 regardless of
`ps_inflight_cap()`; 4K writes ran lock-step at one batch per
(append RTT + fsync window). The background.rs F099-K/M/N comment
documented this and deferred it to an `AUTUMN_PS_MIN_BATCH` knob that
baselines never set.

F256 removes the gate: launch whatever `pending` holds as soon as a
pipeline slot is free (`!at_cap && !imm_full`). Batch size adapts to
arrival-rate × in-flight latency (group-commit style). The R3 Task 5b
fragmentation concern is prevented STRUCTURALLY, not by a gate:
ps-conn tasks share the P-log thread and enqueue a whole TCP burst
into req_rx before partition_loop is polled, and the (E) drain pulls
the entire channel into `pending` (up to MAX_WRITE_BATCH) each
iteration — so a naturally-full burst still launches as ONE batch.
The `--min-pipeline-batch` CLI flag is parsed but deprecated (no-op +
warning).

### In-order Phase 3 commit (F210-C1, post-2026-05-15)

Phase 2 futures execute concurrently up to `ps_inflight_cap()`, but
`partition_loop` uses **`FuturesOrdered`** (not
`FuturesUnordered`) so Phase 3 yields are strictly in launch order =
strictly in seq order. This is a load-bearing invariant for recovery
correctness.

Pre-F210-C1 (`FuturesUnordered`): completions could yield out of order.
Phase 3 inserts ran in completion order, not seq order. The rotated
active memtable was therefore NOT guaranteed to contain a contiguous
seq range — e.g. batch B (seq 101-200) could be Phase 3'd and the
active rotated to imm + flushed → SST with `last_seq=200`, while
batch A (seq 1-100) was still in flight. On crash before A's Phase 3,
replay's dedup predicate `if ts <= sst_max_seq { continue; }` would
skip seq 1-100 records (since `50 <= 200`), silently dropping ack'd
writes. Lost data was bounded by the in-flight cap (8 batches × 256
records each, up to 2K records / partition).

Post-F210-C1 (`FuturesOrdered`): rotated memtable = contiguous seq
range, SST.last_seq = bound on "every seq <= last_seq is in this SST
or an earlier one". Replay's dedup is sound. The trade-off — small
p99 latency uptick from head-of-line wait when in-flight batches'
Phase 2 latencies are unequal — is bounded by Phase 2 p99 (typically
~5-10 ms with F178 coalesced fsync); throughput unchanged.

Properties that still hold (unchanged from pre-F210-C1):
- **Seq numbers** assigned in Phase 1 in batch-launch order.
- **Memtable MVCC keys** are `user_key ++ 0x00 ++ BE(u64::MAX - seq)`.
  Byte-sort order is independent of insertion order.
- **Client oneshot replies** are per-request — Phase 3's response
  emission happens in seq order under `FuturesOrdered`, which is a
  side effect of the change but doesn't break clients.
- **LogStream ordering** is preserved by the stream worker's
  lease/ack cursor (step 4.3); concurrent Phase 2 still lands at
  distinct contiguous offsets regardless of Phase 2 completion order.

### Cross-layer SQ/CQ stack (post-R4)

```
┌─ PS partition_loop  (this crate, 4.4 + F099-D)        ┐
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

**Single-writer invariant for row_stream (post-2026-05-03 fix):** ALL
appends to `row_stream` MUST go through P-bulk's `StreamClient`. The
`flush_worker_loop` accepts two channel types:
- `FlushReq` (from flush_loop): build SST + row_stream.append
- `RowAppendReq` (from compaction on P-log): row_stream.append only

Both share P-bulk's single `StreamClient`, so the per-stream worker's
commit/lease state stays coherent. **Never use P-log's `part_sc` for
row_stream appends.** Pre-fix, compaction (`do_compact`) used P-log's
`part_sc.append(row_stream_id, ...)` while flush used P-bulk's
`bulk_sc.append(row_stream_id, ...)`. The two independent StreamClients
tracked commit position locally and independently. When one writer's
stale commit was sent in an append header, ExtentNode truncated data
written by the other, destroying SST data and causing `invalid meta_len`
corruption on PS restart.

Single-writer invariant is now type-level (post-F254): `open_partition`
returns Err when P-bulk fails to spawn (the manager reschedules the
partition), so `PartitionData.flush_req_tx` / `row_append_tx` are
non-`Option<Sender>`. The in-thread fallback that previously coincidentally
preserved single-writer when P-bulk spawn failed is gone (both
`run_flush_async_phase`'s `req_tx_opt == None` branch and
`compact_row_append`'s `else`).

**Record format**: `[op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key][value]` (17-byte header)

**No local WAL file**: logStream is the sole write-ahead log. Recovery replays logStream from the VP head recorded in the last metaStream checkpoint.

### BUG-LEASE-2 — per-ino fence floors (Phase 1 in-memory, Phase 2 persisted)

Write requests stamped with `(inode_hint != 0, lease_epoch)` are checked
against `PartitionData.fence_floors` (per-partition `HashMap<ino, max
epoch seen>`): `stamped < floor` ⇒ `CODE_FENCED` (a revoked writer's
late RPC), else admit + raise the floor. All three write entry points
run it — `enqueue_put`, `enqueue_put_zc` (meta carries the two fields,
`PUT_ZC_HEADER_LEN = 44`), `enqueue_batch_put` (per-op; a fenced op gets
CODE_FENCED in its statuses slot without failing the batch). Ordering
invariant (coco R2-P1 #5): the value-too-large check runs BEFORE the
fence check so a rejected oversized write can't poison the floor.

Phase 2 persistence — floors survive PS restart / partition reschedule:
- **WAL**: when `check_and_bump_fence` returns `Ok(true)` (raised), the
  dispatcher queues `WriteOp::FenceBump { ino, epoch }`
  (`WriteResponder::Fence`, no client reply) BEFORE the admitted write —
  same group-commit pipeline ⇒ the floor is durable no later than the
  write's ACK. The WAL record is op `OP_FENCE_BUMP` (0x08), key = ino
  BE8 (skips in_range), value = epoch LE8. It NEVER enters the memtable
  (Phase 3 filter) or SSTs; GC's VP scan skips it (no VP bit).
- **Checkpoint**: `TableLocations.fence_floors` snapshot, captured via
  `snapshot_fence_floors(&p)` under the SAME borrow as the tables
  snapshot at all three publish sites (F148-A intact). Covers floors
  raised before the replay window.
- **Recovery**: seeds floors by max-merge over ALL meta records (post-
  merge union of both sources), then replay max-merges every
  OP_FENCE_BUMP it sees (idempotent — deliberately BYPASSES ts-dedup,
  re-applying a checkpoint-covered bump is a no-op).

Client surface: `WriteLease` + `put_fenced`/`put_zc_fenced`/
`put_many_fenced` (`AutumnError::Fenced`); fuse stamps from
`held_leases[ino].version` (`FsState::write_lease_for`), the ioring
daemon from `OpenedExtents.lease_version` (`write_lease_of`) including
its deferred dirty-meta flush. DELETE is fenced too (enqueue_delete,
same range→fence admission order; fuse truncate/unlink stamp it).
MSG_STREAM_PUT was REMOVED outright (zero callers since F186 + its
handler bypassed both the inline cap and the fence — 0x46 reserved).
Admission
ordering invariant (coco): region_epoch → in_range → value-too-large →
fence — a rejected request must NEVER raise (or persist) the floor.

### `MSG_BATCH_PUT` (0x53) — server-batched Put (commit 1724ca3 / 09b2a39)

A second write entry point alongside `MSG_PUT` / `MSG_PUT_ZC`. One
frame from the SDK carries `BatchPutReq { part_id, region_epoch,
must_sync, ops: Vec<BatchPutOp{key, value, expires_at}> }`. The
SDK (`ClusterClient::batch_put`) groups items by owning partition
and emits one frame per partition.

Server side (`enqueue_batch_put` in `lib.rs`):
1. Decode the frame ONCE on the ps-conn task (vs. per-op decode
   for the N-frame `put_many` path it replaced).
2. Allocate one `Rc<BatchPutAccumulator>` carrying the outer
   `resp_tx`, a `RefCell<Vec<u8>>` statuses (one per op), and a
   `Cell<usize>` remaining counter.
3. For each op: push a `WriteRequest` whose `WriteResponder` is
   the new `BatchPut { accum, idx }` variant into
   `partition_loop.pending`. This is an ATOMIC injection of N
   ops as a single mpsc message — the same wide batch all
   landing in pending at once, where the F099-D group-commit
   loop picks them up. Compared with the pre-batch path (N
   independent mpsc messages), this saves N-1 wake/dispatch
   cycles and gives the group-commit loop the "fat arrival" it
   needs to fill MAX_WRITE_BATCH=256.
4. Each op's Phase 3 completion calls `accum.record(idx,
   status)` instead of sending its own outer oneshot. The LAST
   recorder (remaining==0) encodes the `BatchPutResp` and fires
   the single outer oneshot back to ps-conn.

Frozen-for-merge rejection path also handles MSG_BATCH_PUT —
returns `CODE_UNAVAILABLE` per-op uniformly via the same
accumulator path.

Measured win (cross-host TCP, 4 K values, 8 partitions × 64
threads × pipeline-depth 8): 6,531 ops/s baseline → 45,146 ops/s
with `--batch-put 64` (6.9×). Per-partition tracing showed
batch_size hitting cap=256 consistently, vs. ~8-16 on baseline.
Single-host loopback shows a smaller 1.7× win because per-frame
routing/decode overhead is already low over localhost.

### `MSG_BATCH_GET` (0x54) — server-batched Get

Read mirror. `BatchGetReq { part_id, region_epoch, keys: Vec<Vec<u8>> }`,
response `BatchGetResp { items: Vec<BatchGetItem{ status, value }> }`.

Server side (`handle_batch_get` in `rpc_handlers.rs`):
1. Runs INLINE on the ps-conn task (no `partition_loop` mpsc
   hop — same as `handle_get`).
2. Decode the frame once, brief `borrow()` to snapshot
   readers/state, then loop calling `get_value` per key in the
   batch.
3. Pack the per-key responses into a single `BatchGetResp` frame.

NO zero-copy for the response — `BatchGetResp` is rkyv-encoded
with each value embedded inline. For ≥ 64 KiB reads, callers
should use the client `get_many_into` (per-key `MSG_GET_ZC` into
caller-owned dests, fan-out via `fan_out_collect`) — that's the
right primitive when each value needs its own dest.

Measured win (single-host loopback, 4 K values, 8 threads ×
pipeline-depth 4, --batch-get 32): 158 K read ops/s baseline →
234 K (1.47×), p99 0.28 ms → 0.07 ms (4×). The bigger win is in
p99, not throughput — batching collapses the per-frame
write_vectored response coalescing.

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

### F261 — SST on-demand paging + bounded block cache (Stage-1)

`SstReader` no longer keeps SST bytes resident: production readers
(flush / compact / recovery) are PAGED — only MetaBlock state stays in
memory; data blocks are fetched on demand from row_stream through the
process-wide bounded `BlockCache` (`--sst-block-cache-bytes`, default
512 MB, sampled-LRU; keys = `(extent_id, abs_off)`; NO compaction
invalidation needed — extent ids are never reused, stale entries age
out). Recovery opens SSTs from the META TAIL ONLY (last-4-bytes →
meta region) and the WAL replay streams in 64 MB chunked-carry
(`decode_records_chunk`) — both pre-F261 whole-reads kept restart RSS
at O(dataset) (measured 18-28 GB on a 14 GB set; now 2,380 MB =
replay-window bound, dataset-independent). The replay chunks MUST use
`StreamClient::read_committed_bytes_from_extent` (committed-clamped),
never the plain explicit-length read: a replica's file legitimately
holds speculative bytes past `sealed_length`/min-commit, so the plain
read's short-read stop never fires at the committed end — the scan
ingests un-committed bytes and the next chunk trips `StaleVpOffset`,
permanently wedging the partition open (hit 2026-06-11 on 4/8
partitions after kill+restart).

Invariants:
- Point lookups go through `lookup_in_sst_via` (async): callers
  SNAPSHOT `Arc<SstReader>`s + stream_client under the borrow, DROP it,
  await, re-borrow (note 15). `lookup_in_sst` (sync) serves resident
  readers only and ERRORS on paged ones — never silently misses.
- Iteration is ASYNC (F262 = Stage-2): `AsyncTableIterator` /
  `AsyncMergeIterator` fetch blocks on demand. FetchMode::Cached
  (range — per-block via the global BlockCache, repeated lists hit warm
  blocks) vs FetchMode::Window(8MiB) (do_compact / split
  unique_user_keys — sequential bulk windows BYPASSING the cache,
  scan-resistant; one RPC per window; peak read memory = inputs × one
  window). `materialize`/`materialized_for_iteration` are GONE. The
  async API returns Result — a block-read error ABORTS the compaction
  (the old sync iterator stashed errors in an unread field, which with
  network-backed blocks would have silently truncated merge output).
  Sync TableIterator/MergeIterator remain Resident-only (tests,
  builder round-trips).
- Diag seq_opt/fullscan report miss on paged readers (diagnostic-only).

### F259 — large-VP client direct-read (MSG_GET_REDIRECT)

`handle_get_redirect` (rpc_handlers.rs): a FULL-value read of a VP whose
`vp.len >= 64 KiB` answers with a descriptor — `GetRedirectResp { extent_id,
value_offset, value_len, eversion, replica_addrs }` — instead of resolving
the bytes through this PS; the client (`ClusterClient::get_direct`) reads
the range straight from an EN (`autumn_stream::read_extent_value_direct`,
MSG_READ_BYTES_ZC zero-copy) and falls back to the proxy `get` on ANY
failure. Invariants:
- The GC writer-pin check (F162) runs BEFORE the redirect decision — an
  extent being punched surfaces NotFound exactly like the proxy path. The
  client's read window is deliberately unprotected: a GC punch in the gap
  is a failed EN read → proxy fallback (extents unlink whole + eversion
  fence ⇒ never a torn read).
- EC-converted extents NEVER get a descriptor
  (`StreamClient::extent_read_descriptor` refuses; shard bytes ≠ value) —
  handle_get_redirect falls back to `handle_get`.
- Short reads under CODE_OK are FAILURES in `read_extent_value_direct`
  (same "got < need" rule as `read_value_from_log`).
- Sub-range reads, inline values, small VPs: inline in the response
  (`extent_id == 0`); `get_value` (non-redirect callers) never yields
  `GetOutcome::Redirect`.
Loopback perf: latency win only (TCP 8M read p50 145→46ms, UCX 137→15ms);
the throughput win is cross-host where PS NIC egress leaves the data path.

### F216-E — UCX end-to-end zero-copy read (MSG_GET_ZC)

The kvcache SDK's `get_into` issues `MSG_GET_ZC` so the value lands in its
registered dest with no intermediate copies across all three tiers
(`EN → PS → client`). The seam is `resolve_value`/`read_value_from_log`
(`background.rs`) returning **`Bytes`**, not `Vec<u8>`:

- **VP value (UCX + TCP, F219)**: `read_value_from_log` calls
  `StreamClient::read_value_into_pooled`, which recvs the value straight into a
  `RegPool` `PooledBuf` (EN emits `[zc_meta][value]` as 2 `Bytes`, value aliases
  the EN pread buffer — no encode copy). **UCX** recvs into a *registered* buffer
  (RDMA); **TCP** (F219, gate relaxed from UCX-only) recvs via a compio owned
  read (`read_exact_into_pooled`) — only the kernel copy, no FrameDecoder copy.
  The PS hands the value onward as `Bytes::from_owner(pb)` (the value `Bytes`
  ALIASES the pool buffer; returns to the pool when that `Bytes` drops after the
  client write completes). Falls back to the `read_bytes_from_extent` copy path
  for EC / chunked / stale-eversion / length==0. No value crc is verified (F219).
- **inline value**: `resolve_value` returns a zero-copy `raw_value.slice(..)`
  of the memtable `Bytes`.
- **`handle_get_zc`** (`rpc_handlers.rs`) returns the response as TWO segments
  `(head, value)`: `head = [V0 frame header][zc_meta: code+value_len+crc32c]`
  (built by `ps_zc_head`, mirrors `extent_node::zc_read_head`), `value` is the
  aliasing `Bytes`. The ps-conn's inflight FU output type is
  `(Bytes, Option<Bytes>)`; `push_resp` pushes `head` then `value` into
  `tx_bufs` so ONE `write_vectored_all` emits them as a single wire frame with
  **no concat copy** (the d=1 fast path uses `write_vectored_all([head, value])`
  for ZC, plain `write_all(head)` otherwise — no regression). The on-the-wire
  bytes are identical to the pre-R4 concatenated form, so the client read path
  (`call_into_dest` / `call_into_pooled`) is unchanged.

`handle_get` (rkyv `GetResp`, generic SDK) copies the value once
(`value.to_vec()`); the rkyv encode copies regardless, so returning `Bytes`
from `resolve_value` does not regress it — the copy just moves. Net read-path
value copies: VP-over-UCX `get_into` = **0**, generic `get` = same as before.

UCX send from the registered `PooledBuf` is zero-copy via the rcache (the
buffer is `ucp_mem_map`-registered, so the unregistered `ucx_send` still finds
the registration). Cancel-safety of the registered recv lives in the read_loop
that OWNS the `PooledBuf` (returns it to the pool on cancel — never leaks); see
`autumn-transport/src/ucx/regpool.rs` + `autumn-rpc/src/client.rs`.

### F216-E W1 — PS write-recv zero-copy (`MSG_PUT_ZC`, large values)

Symmetric to the read path, on the WRITE recv side. `drain_zc_writes` (lib.rs)
runs in the ps-conn read loop right after `decoder.feed`, BEFORE the normal
decode: if the FRONT frame is a `MSG_PUT_ZC` whose value is
`≥ AUTUMN_PS_ZC_RECV_MIN_BYTES` (64 KiB), it recvs the value straight into a
`PooledBuf` instead of letting `FrameDecoder` accumulate (and copy) the whole
value. **Both transports (F219)**: UCX recvs into a *registered* buffer via
`ReadHalf::recv_into(dest, reg)` (RDMA, no off-wire copy); TCP recvs via
`ReadHalf::read_exact_into_pooled` (a compio *owned* read — only the unavoidable
kernel→userspace copy, no FrameDecoder copy). Mechanics:

- `peek_header` + `peek_payload` (new on `FrameDecoder`) read the frame header
  and the `[part_id][..][key_len][key]` meta WITHOUT consuming, to locate the
  value boundary. Gated on `part_id == owner_part` (mis-routed → normal path
  synths NotFound) and the size band `[64 KiB,
  AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT]`.
- It then `consume`s the header+meta+key, `drain_into`s any buffered value
  prefix into the `PooledBuf`, and recvs the remainder (UCX `recv_into` / TCP
  `read_exact_into_pooled`). **F219 removed the per-value CRC**: the V1 frame-crc
  trailer is still consumed off the wire (stream alignment) but no longer
  validated — value integrity is the transport's job (UCX NIC ICRC / TCP kernel
  segment checksum). Normal (non-ZC) frames keep their V1 frame-CRC (F165).
- The value rides onward as `Bytes::from_owner(pb)` (aliases the pool buffer, no
  copy) via a NEW `PartitionRequest.zc_value: Option<Bytes>` field; `payload`
  carries only `[meta][key]`. `enqueue_put_zc` uses `zc_value` directly when
  present (else slices from `payload` as before). The value never gets the
  off-wire / decoder-accumulation copy; the PS→EN `append_batch` send is already
  rcache-zero-copy (UCX) / Arc-Bytes (TCP) from the pool buffer.
- Cancel-safe on both transports: `drain_zc_writes` owns the `PooledBuf` across
  the recv. UCX → `recv_into`'s `InflightSlot` drains the NIC on drop; TCP →
  compio retains the owned buffer until the read CQE lands. Either way the buffer
  returns to the pool, never freed-under-the-kernel. The d=1 fast path is skipped
  when `drain_zc_writes` queued a reply (`inflight.is_empty()` guard) to keep
  in-order replies.
- `PS_ZC_WRITE_RECV_HITS` counts engagements; logged once on first engage
  (`transport="ucx(registered)"` / `"tcp(pooled)"`).

Small writes (< 64 KiB) keep the unchanged FrameDecoder path on both transports
— the only added cost there is one `peek_header` + a size-check branch per frame.
On TCP, only callers that send `MSG_PUT_ZC` engage this (perf-check + `put-zc`
CLI, for values ≥ 64 KiB); regular `MSG_PUT` writes are unaffected.

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

P-bulk spawn failure is fatal-for-this-partition (post-F254): `open_partition`
returns Err and the manager reschedules. There is no in-thread fallback. The
historic fallback (and `flush_one_imm_local`, removed earlier by F197) only
preserved single-writer by coincidence — see the row_stream invariant note
above.

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
Without this cap, `autumn-op compact ALL` against an N-partition PS
would launch N concurrent `do_compact` calls each holding ~2× SST bytes
in memory, multiplying per-partition peak by N.

## GC (Garbage Collection)

Targets the **logStream** where large values (ValuePointers) are stored.

**Trigger**: periodic (30–60s jitter), via `gc_tx` channel (capacity 1), or via the `Maintenance` gRPC RPC. Two public methods on `PartitionServer`:
- `trigger_gc(part_id) -> Result<(), &'static str>` — enqueue `GcTask::Auto(GcAutoParams::default())`
- `trigger_force_gc(part_id, extent_ids) -> Result<(), &'static str>` — enqueue `GcTask::Force { extent_ids }`

**F201 candidate selection** (`background.rs::background_gc_loop` Auto arm):
1. Candidates = all `sealed_extents` (`extent_ids[..len-1]`), sorted by reclaimable bytes desc. Pre-F201 candidates came only from `discards.keys()`, so empty sealed extents (no SST ever referenced them → never in any discards map) were invisible to the loop.
2. For each candidate, `get_extent_info(eid)`:
   - `sealed_length == 0` → push to holes (empty slot, no rewrite). `run_gc(eid, 0)` skips the read loop and goes straight to `flush_gc_batch` (no-op) + `punch_holes`.
   - Else apply the F201 multi-tier filter:
     - If `GcAutoParams::empty_only` is set → skip non-empty.
     - If `max_size` is set and `sealed_length > max_size` → skip.
     - Effective `ratio = max(GC_DISCARD_RATIO, params.ratio)`; halved when stream total discard ≥ `stream_debt` high-water.
     - Push if `discard_bytes / sealed_length > effective_ratio`.
3. Cap at `MAX_GC_ONCE` (3) per dispatch.

**F201 multi-tier params** (`GcTask::Auto(GcAutoParams)`):
- `ratio: Option<f64>` — discard-ratio threshold, default 0.4
- `max_size: Option<u64>` — only consider extents at most this size
- `stream_debt: Option<u64>` — when total reclaimable bytes ≥ threshold, halve the ratio
- `empty_only: bool` — pick only `sealed_length == 0` (cheapest, no rewrite)

External controllers / `client gc --ratio X --max-size Y --stream-debt Z --empty-only` compose effective tiers by issuing multiple dispatches back-to-back. The PS does not internally "schedule across tiers"; it executes exactly the set of params each dispatch carries.

**F201 cooldown classification** (`classify_gc_failure_cooldown`):
- Soft window (30 s) when the failure's anyhow chain contains `"precondition failed"` (manager refuses `punch_holes` while `ec_conversion_inflight` per F138/F145) or `"eversion mismatch"` (private `EversionStale` sentinel from autumn-stream — stale `extent_info_cache` after an EC bump).
- Hard window (300 s, was the only window pre-F201) for everything else.
- `gc_failure_cooldown` map shape is `HashMap<u64, (Instant, Duration)>` so each entry carries its own window.

Wire surface: `MaintenanceReq` carries 4 new optional fields (`gc_ratio` / `gc_max_size` / `gc_stream_debt` / `gc_empty_only`) — backward-incompatible at rkyv level; same-commit upgrade required (cluster.sh stops all roles before restart). Legacy callers (FLUSH, COMPACT, FORCE_GC) pass default values for these fields.

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

**`run_gc` for one extent (F106 streaming + F130 multi-frag pre-pass)**:
```
  0. F130 multi-frag rewrite pre-pass (NEW):
     `rewrite_multi_frag_for_extent(part, log_stream_id, eid, part_sc)`
       - Walk active memtable + imm queue for OP_VALUE_POINTER_MULTI
         entries whose mfvp has any fragment on `eid`. Dedup by user_key
         (newest seq wins). SST-only mfvps deferred to compaction's
         discard path (see F130 entry in feature_list.md).
       - For each candidate: read every fragment via
         read_bytes_from_extent (full-value rewrite, not partial),
         append each as a fresh OP_CHUNK_BLOB record, build new
         MultiFragVp, allocate seq + append OP_VALUE_POINTER_MULTI|1
         commit record, insert memtable entry.
       - Race semantics: foreground Put on the same user_key allocates
         a strictly newer seq under the same single-threaded P-log
         runtime → wins on read via MVCC. Rewrite chunks become orphan
         and are reclaimed by the next GC pass.
     Without F130, OP_CHUNK_BLOB records of live multi-frag values would
     be silently orphaned when punch_holes fired (the single-VP scan at
     step 1 skips OP_CHUNK_BLOB records since `op & OP_VALUE_POINTER == 0`).
  1. Single-VP loop (F106 streaming): until cur >= sealed_length:
       a. read_bytes_from_extent(eid, cur, AUTUMN_PS_GC_READ_CHUNK_BYTES)
       b. concatenate carry + chunk → buf
       c. process_gc_chunk(buf):
          - decode complete records left-to-right
          - on partial record at tail, stop; caller saves buf[consumed..]
            as carry for the next chunk
          - per record (if single-VP and in_range):
            * lookup current live version (active → imm → SSTables)
            * if live VP still points to (eid, offset): re-write value
              via stream_client.append, drop borrow_mut BEFORE awaiting
              the network RPC, then re-acquire borrow_mut to insert
              the updated VP into the memtable
          - OP_CHUNK_BLOB records: skipped (handled by F130 pre-pass)
          - OP_VALUE_POINTER_MULTI commit records: skipped (these are
            tiny — ~16 + n_frags*16 bytes — and the rewrite pre-pass
            covers their semantic content; the byte-trail of the old
            commit record falls within the punched extent same as
            chunks)
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

## Admission: rate limiting + concurrency control (F189 → F196 D-r7)

Background-IO admission split into TWO orthogonal mechanisms after
F196 D-r7. The split mirrors the physical resource they protect:
**rates** are per-partition because IO patterns are partition-local;
**concurrency** is PS-wide because the protected resource is process
RAM (each compact / GC operation holds ~hundreds of MB of buffers).

| Type | Scope | What it controls | Default (post D-r7-recal) | Code |
|------|-------|-----------------|---------------------------|------|
| `RateController` | **per-partition** | fg bytes/s + fg iops + compact bytes/s + gc bytes/s | fg 1 GiB/s + 30K iops; compact 256 MiB/s; gc 128 MiB/s | `lib.rs::RateController` |
| `ConcurrencyController` | **PS-wide** (`Arc<>`) | compact + gc concurrency permits | compact_max=4, gc_max=4 | `lib.rs::ConcurrencyController` |

`PartitionServer` carries one `Arc<ConcurrencyController>`; each
`PartitionData` carries its own fresh `Arc<RateController>` (independent
mutex/state) plus a clone of the PS-wide concurrency Arc.

### RateController — per-partition rate caps

Four independent rate dimensions in one 1-second window (lazy fixed-
window token bucket):

```rust
struct RateState {
    window_start: Instant,    // updated only when elapsed >= 1 s
    fg_bytes: u64,
    fg_ops: u64,
    compact_bytes: u64,
    gc_bytes: u64,
}
```

Public methods (all async, all sleep OUTSIDE the lock):

| Method | Semantics |
|--------|-----------|
| `account_fg(bytes, ops)` | fg write hot path. EITHER bytes OR ops cap reached → sleep (larger sleep wins). Default 256 MiB/s + 30K ops/s catches both 8 MiB-Put bytes-bound and 4 KiB-Put IOPS-bound workloads. |
| `account_compact(bytes)` | compact write. Sleeps until BOTH own compact rate AND fg-aware-yield allow. |
| `account_gc(bytes)` | gc write. Symmetric to compact, separate counter. |

**fg-aware yield** (inside `account_compact` and `account_gc`): yields
the remainder of the 1-s window when `fg_observed_bytes_rate > 0.8 × fg_rate`
OR `fg_observed_iops > 0.8 × fg_iops`. Disabled per-dimension when
that fg cap is 0. Each partition checks ITS OWN fg counters — cold
partition's compact doesn't yield because of hot partition's fg
pressure (partitions are isolated).

**Independent counters** (key invariant tested by
`compact_and_gc_rates_are_independent`): saturating compact does NOT
throttle gc and vice versa. Pre-D-r7 they shared a single `bg_rate_bytes_per_sec`
counter; the split lets operators allocate budgets independently
(compact tends to be bulk/burst; gc is more sustained).

Why per-partition? In a multi-partition PS, a hot partition's fg
pressure consuming a shared bg budget would starve cold partitions'
maintenance work. Per-partition rates also align with the thread-per-
core P-log model — each partition has its own io_uring; no reason its
admission state should fight other partitions' on a shared Mutex.

### ConcurrencyController — PS-wide RAM cap

Each `do_compact` holds ~2× SST bytes in memory; each `run_gc` holds
~64 MiB of chunk-read buffer + rewrite staging. Without a global cap,
`autumn-op compact ALL` would launch N concurrent compactions and
multiply peak RSS by N (the F104 incident hit 44 GB RSS).

```rust
pub struct ConcurrencyController {
    compact_max: usize,
    gc_max: usize,
    compact_inflight: AtomicUsize,
    gc_inflight: AtomicUsize,
}
```

Public methods:

| Method | Returns | Drop semantics |
|--------|---------|----------------|
| `acquire_compact()` | `CompactPermit` | decrements `compact_inflight` |
| `acquire_gc()` | `GcPermit` | decrements `gc_inflight` |

Atomic counters + 50 ms backoff loop (mirrors EN's
`stream::ConcurrencyController`). `parking_lot::Mutex` NOT used here
because per-Arc concurrency operations are inherently cross-thread
(multiple partition threads acquire from the same Arc) and Atomic
CAS is the right primitive.

### Call sites

| Path | Call |
|------|------|
| `background::start_write_batch` (fg hot path) | `rate_ctrl.account_fg(bytes, ops)` |
| `background::do_compact` (chunk emit + final emit) | `rate_ctrl.account_compact(chunk_bytes)` |
| `background::flush_gc_batch` (write side) | `rate_ctrl.account_gc(batch_bytes)` |
| `background::run_gc` (chunk read side) | `rate_ctrl.account_gc(chunk_len)` |
| `background::background_compact_loop` | `concurrency_ctrl.acquire_compact()` |
| `background::background_gc_loop` | `concurrency_ctrl.acquire_gc()` (in addition to per-partition F140 `gc_gate`) |
| `rpc_handlers::handle_split_part` | `concurrency_ctrl.acquire_compact()` + per-partition F140 `gc_gate.acquire()` |

F141's per-partition `GcRateLimiter` survives as a deprecated inner
cap layered before `account_gc`; it stays for back-compat with the
`--gc-rate-bytes-per-sec` flag (distinct from D-r7's
`--admission-gc-rate-bytes-per-sec`).

### Configuration (CLI flags + cluster.sh env)

| Flag | Env | Default | Knob |
|------|-----|---------|------|
| `--fg-rate-bytes-per-sec` | `AUTUMN_PS_FG_RATE_BYTES_PER_SEC` | **1 GiB/s** | per-partition fg bytes |
| `--fg-iops-per-sec` | `AUTUMN_PS_FG_IOPS_PER_SEC` | 30_000 | per-partition fg ops |
| `--admission-compact-rate-bytes-per-sec` | `AUTUMN_PS_ADMISSION_COMPACT_RATE_BYTES_PER_SEC` | **256 MiB/s** | per-partition compact bytes |
| `--admission-gc-rate-bytes-per-sec` | `AUTUMN_PS_ADMISSION_GC_RATE_BYTES_PER_SEC` | **128 MiB/s** | per-partition gc bytes |
| `--fg-saturated-threshold` | `AUTUMN_PS_FG_SATURATED_THRESHOLD` | 0.8 | fg-aware yield trigger ratio |
| `--major-compact-parallelism` | `AUTUMN_PS_MAJOR_COMPACT_PARALLELISM` | **4** | PS-wide compact concurrency |
| `--gc-parallelism` | `AUTUMN_PS_GC_PARALLELISM` | **4** | PS-wide gc concurrency |

Defaults sized to perf_check baselines. See `feature_list.md` F196
D-r7-recal entry for the derivation (4K p16 d8: per-partition fg
27 MB/s, flush 27 MB/s; 8M p8 d8: per-partition fg 218 MB/s).

`0` on any rate flag = unlimited for that dimension (per-dimension
opt-out without disabling the others). Removed in D-r7:
`--bg-rate-bytes-per-sec` (split into compact + gc), with a migration
error message at the CLI parser.

### Pre-D-r7 history (for archeologists)

- F141 (per-partition `GcRateLimiter`): standalone gc bytes/s limiter.
  Kept as a layered inner cap.
- F188 (`IoTokenBucket`): single PS-wide bg bytes/s cap. Replaced by F189.
- F189 (`AdmissionController` Stage 3): two-class (fg + bg) admission
  with fg-aware yield. PS-wide single Arc.
- D-r5 (early F196): made it per-partition with `total/N` split. **Bug**:
  one busy partition couldn't use full pool, double-bookkeeping vs
  F104's PS-wide compact_gate. Reverted by D-r6.
- D-r6: unified back to PS-wide AND folded F104 compact_gate +
  gc_concurrency_gate into the single controller. Cleaner but
  confused two orthogonal concerns (rate vs concurrency).
- **D-r7 (current)**: separates rate (per-partition `RateController`)
  from concurrency (PS-wide `ConcurrencyController`). compact/gc rates
  are split into independent counters.

### Algorithm: lazy fixed-window token bucket

Each rate dimension uses the same shape: a 1-s wall-clock window with
step-function reset, lazy `Instant::elapsed()` checks (vDSO
`clock_gettime`, ~20 ns user-space). The `parking_lot::Mutex` guards
synchronous accounting only — the sleep happens OUTSIDE the lock
(holding a non-async mutex across `.await` would deadlock the compio
runtime, same thread = recursive lock = futex_wait on self):

```rust
let sleep_for = {
    let mut s = self.state.lock();
    Self::maybe_reset_window(&mut s);
    s.<dim>_bytes += bytes;
    compute_sleep(&mut s)
};                                  // guard dropped here
if let Some(d) = sleep_for {
    compio::time::sleep(d).await;   // await OUTSIDE lock
}
```

Trade-off vs continuous-refill token bucket: at the 1-s boundary you
can burst 2× rate over a ~2 ms window. For our workloads (256 MB
compact chunks + 4 MiB GC batches) this is a feature — a single chunk
never gets sliced across windows.

### Industry comparison (legacy F189 notes)

- **RocksDB `GenericRateLimiter`**: fixed-window + sleep, 100 ms refill,
  separate hi/lo priority queues.
- **TiKV `file_system::RateLimiter`**: direct port of RocksDB.
- **Linux cgroups `blkio.throttle`**: kernel-side fixed-window slice
  with jiffies reset — we're the user-space equivalent.
- **CockroachDB `kvadmission`**: inspiration for fg/bg + fg-aware yield.
  Far more sophisticated; we ship the minimum viable subset.

If we ever need multi-priority + dynamic disk-bandwidth feedback,
follow CockroachDB's structure. The four counters in `RateState` are
already laid out for extension.

### Cooldown / scheduler invariants (F189-fix race-review notes)

Distributed-system race review of F187/F188/F189 found 7 bugs in
surrounding scheduler/cooldown logic (the controller itself was clean):
- `last_gc_at` / `last_compact_at` MUST be stamped on every loop
  iteration that ran eligibility check (not just on success) — the
  scheduler's cooldown gate gets stuck and re-dispatches every 5 s
  otherwise.
- `*_inflight` flags MUST be latched at receive-arm top (not after
  `acquire`), to prevent the scheduler from firing duplicates during
  the manager-RPC pre-flight.
- The compact channel's `bool` payload means `is_major`; receivers
  drain backlog and OR-merge to handle futures-channel's `cap +
  num_senders` semantics.
- The scheduler reads `req_count_monotonic` (never-reset) for the
  fg-quota gate; `req_count` (swap-reset by `report_load_loop`) is
  unsafe for diff-based rate calc.

## Partition Split

`handle_split_part` runs inline on `partition_loop` (the P-log
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
  8b. Invalidate stream workers: call part_sc.invalidate_stream()
       on all 3 stream IDs (log, row, meta) and set
       need_invalidate_row_stream for P-bulk. The manager sealed
       the old tails; without invalidation the stale workers keep
       appending beyond sealed_length and recovery misses that data.
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

**F140 split serialisation (dual-gate pattern):** `handle_split_part`
acquires two gates before calling `flush_memtable_locked` / `commit_length`:

1. `compact_gate` (PS-wide, same `Arc` held by `background_compact_loop`) —
   ensures no `RowAppendReq` is in-flight on P-bulk when row_stream's tail is
   sealed (`do_compact` holds this gate for its full duration and awaits every
   `compact_row_append` oneshot before releasing).
2. `gc_gate` (per-partition, new in F140) — ensures `run_gc` has no
   `log_stream` append in-flight. `background_gc_loop` acquires `gc_gate`
   around the `for eid in holes` block (not the preceding read-only RPC calls).

Both are held through `multi_modify_split` and released RAII on return.
Acquisition order is always compact→gc. `PartitionData` stores both `Arc`s so
`handle_split_part` (which only receives `part: &Rc<RefCell<PartitionData>>`)
can clone and acquire them without extra parameters.

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

If the `partition_loop` receives a `CODE_LOCKED_BY_OTHER` error from the stream layer
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

## region_epoch check (TiKV-style, 2026-05-16)

Each `PartitionData` carries `region_epoch: u64`, populated at open
time from `MgrRegionInfo.region_epoch` (manager bumps on every `rg`
rewrite — split / merge). Hot-path handlers compare the request's
stamped `region_epoch` against `p.region_epoch`; mismatch returns
`StatusCode::FailedPrecondition` so the SDK's `Err`-arm refresh path
engages.

Check sites:
- `rpc_handlers.rs::handle_get` — before in_range
- `rpc_handlers.rs::handle_head` — same
- `rpc_handlers.rs::handle_range` — at top; **this is the load-bearing
  one** for the gallery list bug. Pre-this, `handle_range` silently
  filtered out-of-range keys via `continue` and returned partial
  `Ok(RangeResp)` — the SDK couldn't tell. Now any stale-epoch range
  is rejected up front.
- `lib.rs::enqueue_put` / `enqueue_delete` / `enqueue_stream_put` —
  write path. `handle_incoming_req` snapshots `(region_epoch, part_id)`
  under one `borrow()` and threads it through.

`0` on the wire = "skip check" (tests / bench / legacy). Production
callers always stamp non-zero from `ClusterClient.lookup_epoch_for_part`.

`RangeResp.cur_end_key` carries `p.rg.end_key` as the resume cursor
for the SDK — CockroachDB-style ResumeSpan. The SDK uses it to
advance across partition boundaries within a single `range()` call,
so a split mid-scan auto-resolves.

`PartitionHandle.opened_with` extends to `(rg, log/row/meta, region_epoch)`
so `sync_regions_once`'s drop+reopen check catches an epoch bump even
in the (theoretical) case where rg byte-for-byte matches but epoch
moved.

**`handle_split_part` must bump `p.region_epoch` locally** (post-fix
above the rg/has_overlap update). The manager bumps the region epoch
in lock-step with rg rewrites (`next_region_epoch` in manager/src/lib.rs),
but the PS partition thread can't see that bump until either (a) the
next `sync_regions_once` tick drops + reopens this partition (seconds
later, expensive), or (b) it updates its own copy in place. Without
the in-place bump, a gallery `range()` call issued in the gap routes
to a partition whose `p.region_epoch` still matches the SDK's stale
cached epoch — the check passes, handle_range filters the SSTables
against the just-narrowed `part_rg`, returns left-only entries with
`cur_end_key = mid`, and the SDK's still-stale routing cache loops
back to the same partition, hits the cursor-non-advance defensive
trip, and the user sees an empty list. SDK additionally falls back
to `refresh_regions` + retry on the non-advance trip as a defensive
second layer (`crates/client/src/lib.rs::range`), so a future PS-side
bug of this shape self-heals instead of erroring out.

**`handle_split_part` must ALSO publish the new tuple to
`opened_with_shared`** so `sync_regions_once` skips the drop+reopen
that pre-fix-2 caused a 5-60+ s outage on every split (F212-fix-2).
`PartitionHandle.opened_with` is now
`Arc<parking_lot::Mutex<(Range, u64, u64, u64, u64)>>` shared with
`PartitionData.opened_with_shared` on the partition thread. The
in-place rg/epoch update block in `handle_split_part` is followed
by a second `borrow + lock + write` block that publishes the
post-split tuple. Same pattern as `Arc<PartitionMetrics>`
(programming note 11) and `Arc<ConcurrencyController>` (F196 D-r7) —
cross-thread shared state for low-frequency cross-thread reads.

Why this is load-bearing: pre-fix-2, `opened_with` was a frozen
snapshot from `open_partition` time. After `handle_split_part`
narrowed `p.rg` + bumped `p.region_epoch` in-place on the
partition thread, the snapshot stayed stale. On the next
`region_sync_loop` tick (~2 s), `sync_regions_once` compared the
stale snapshot against the manager's latest, saw `prev != latest`,
and dropped + reopened the SOURCE partition even though its
in-memory state was already perfectly correct. The reopen ran a
full `recover_partition` (log_stream replay), allocated a new
F099-K port, and during that window (5-60+ s depending on WAL
depth) the partition was unreachable. The user observed >2 min
gallery emptiness from this on a partition with a deep WAL tail.

Post-fix, only the **right child** still has an inherent open
window: it's a brand-new partition that has to be opened from
scratch on its assigned PS (~2 s `region_sync_loop` tick + the
new partition's `open_partition`). The SDK absorbs that window
via TiKV-style retry in `crates/client/src/lib.rs::range`,
`call_ps_for_key`, and `call_ps_for_part`
(`MAX_PS_REFRESHES = 10`, base 100 ms, cap 2000 ms, cumulative
~9 s — well over the typical 2-4 s convergence). The retry is
deliberately NOT large enough to hide multi-minute PS-side bugs
of the same shape (so the next one fails loudly here instead of
being masked).

Future in-place updaters of `PartitionData.rg` / `region_epoch`
(e.g., a future merge-survivor in-place widening) MUST follow
the same pattern: update local fields → release `borrow_mut` →
take a fresh `borrow` and lock `opened_with_shared` → write the
new tuple. Skip either step and the partition gets needlessly
torn down on the next `region_sync_loop` tick.

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
| `MAX_SST_BEFORE_AUTO_COMPACT` (F210-E2) | 32 | defensive: `background_compact_loop`'s timeout arm auto-triggers a minor compaction when `sst_readers.len()` exceeds this. Prevents bloom-FPR runaway on partitions where external policy is paused (1% per-SST bloom × N=32 ≈ 28% cumulative miss-path false-positive). Not env-tunable — mechanism-level defensive bound, not a policy knob. |

## F120 — bounded recovery replay

**The problem (2026-04-27):** A killed-mid-write PS that had pushed many imm
tables behind a slow P-bulk left the entire `(vp_offset, log_stream commit)`
window for restart-time replay. Witnessed at **1.96 GB on partition 15** with
several hundred MiB across siblings, surfacing as a 16 GB process footprint
post-restart.

**The three fixes:**

1. **F120-A — imm depth cap + back-pressure.** `partition_loop` reads
   `imm_full = part.imm.len() >= MAX_IMM_DEPTH` at top of loop. When full it
   skips both batch launches (B) and `req_rx.next()` (D), only polling
   `inflight.next()` and a new `imm_drained_rx` channel. `flush_one_imm`
   (and the legacy `flush_one_imm_local` fallback) signal `imm_drained_tx`
   after each successful `imm.pop_front()` so the loop wakes and resumes
   request intake. Worst-case unflushed-WAL window per partition is now
   `MAX_IMM_DEPTH * FLUSH_MEM_BYTES + active.bytes` = 1.25 GB.

2. **F120-B — WAL-gap forced rotate.** After each iteration of
   `partition_loop`, compute `gap = active.bytes + Σ imm[i].bytes`.
   If `gap > MAX_WAL_GAP` AND `imm.len() < MAX_IMM_DEPTH`, call
   `rotate_active`. Bounds replay window for workloads that don't fill
   `FLUSH_MEM_BYTES` before triggering rotate (e.g. mostly-large-value
   writes where memtable is light but log_stream grows fast via VPs).

3. **F120-C — graceful shutdown.** New `PartitionServer::shutdown()` sends
   a `oneshot::Sender<()>` per partition through `drain_tx`. The
   `partition_loop` picks it up via select, sets `drain_ack`, exits
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

6. **Group commit batching (post-F178)** — the partition_loop drains up to MAX_WRITE_BATCH (256) requests per RPC cycle. The batch's `must_sync` is the OR of caller flags only; the F150 Phase B rotation-trigger barrier (which auto-promoted `must_sync=true` when the active memtable would cross `FLUSH_MEM_BYTES`) was removed in F178 Phase 2. Durability is now guaranteed in two complementary places:
   - **Per-write coverage**: the extent-node's per-extent fsync coalescer (F178 Phase 1) fires `sync_data` every 1-5 ms; every append's bytes become durable within one coalesce window regardless of the AppendReq.must_sync flag.
   - **Flush barrier**: `flush_one_imm` (and `flush_one_imm_local`) call `part_sc.await_log_synced_to(vp_extent_id, vp_offset)` BEFORE uploading the SST. **F227: ALL log_stream replicas** (was quorum-min) must report `last_synced >= vp_offset` first. This guarantees that every byte the imm's ValuePointers reference is durable on every replica BEFORE the SST that names them is checkpointed — a fsync-quorum could publish an SST whose VP bytes are durable on only a subset, so a later min-commit truncation on the un-synced replica could orphan the VP (the `stale_vp_offset_past_sealed_length` class). On a healthy cluster this is satisfied immediately because the append already acked all-replicas. On the happy path this waits ≈ 0 because the coalescer fires every 2 ms in parallel with SST build; on the worst case it waits one coalesce window.
   Why this is better than F150 Phase B: the rotation-triggering writer no longer pays a 5-15 ms (real SSD) fsync cost as a tail-latency spike — every Put pays the same 1-5 ms coalesce floor. The fsync work moves entirely to background flush, latency-invisible to clients. F178 Phase 3 removed `--nosync` from CLI surfaces; the `must_sync` field on PutReq/AppendReq is kept for wire back-compat but always true in practice.

7. **Per-partition StreamClient** — each `PartitionData` holds its own `stream_client: Arc<StreamClient>` (no Mutex) created via `StreamClient::new_with_owner_epoch`. StreamClient is internally concurrent via per-stream locking (`DashMap<stream_id, Arc<Mutex<StreamAppendState>>>`). Different streams (log/row/meta) are fully concurrent; the same stream is serialized. The server-level `PartitionServer.stream_client` is used only in `split_part` for coordination RPCs.

8. **`start_write_batch` / `finish_write_batch` lock scope** — the write lock is held only for seq number assignment and block encoding (Phase 1), then released before the `append_batch` network RPC (Phase 2), then re-acquired for memtable insert and VP head update (Phase 3). This prevents the partition write lock from blocking reads/flushes/compaction during network I/O.

7. **`sst_readers` and `tables` are always aligned by index** — `tables[i]` and `sst_readers[i]` refer to the same SSTable. Operations on these must maintain alignment. Compaction's atomic swap replaces slices, not individual elements.

9. **Memtable backing = `parking_lot::RwLock<BTreeMap>` (F099-C)** — the active memtable has exactly one writer (the P-log thread's `partition_loop` Phase 3) and N readers (ps-conn `handle_get` call sites + P-log itself). Correctness properties:
   - Writer holds the write lock for the duration of one `insert_batch` call (hot path, up to 256 entries), then releases. Subsequent readers take the read lock AFTER the writer releases → linearisable Put-then-Get.
   - Rotation (`rotate_active`) replaces the whole `Memtable` struct via `std::mem::replace` on the owning `PartitionData`; this is safe because `rotate_active` runs exclusively on P-log inside a `RefCell::borrow_mut`.
   - `imm: VecDeque<Arc<Memtable>>` — after rotation, frozen memtables are read-only from both P-log (during flush + GC + compaction) and P-bulk (during `build_sst_bytes`). Multiple readers acquire the read lock concurrently.
   - Hot path uses `insert_batch(iter)` (one write lock per batch of 256 inserts, not 256 locks), and `for_each(closure)` (read lock held for the iteration — used by `build_sst_bytes` and `rotate_active`).
   - The `bytes: AtomicU64` counter is not inside the lock, so `mem_bytes()` and `maybe_rotate` stay lock-free.

10. **Metadata-publish ordering invariant (F148-A)** — `flush_one_imm` (lib.rs) and `do_compact` (background.rs) both publish to `meta_stream` via `save_table_locs_raw` followed by `sync_partition_vp_refs`. They run as separate background tasks on the single-threaded P-log compio runtime and therefore interleave at every `.await` point. Race-free concurrent publishing (i.e. the LATEST persisted meta_stream record always reflects ALL prior in-memory mutations from both publishers) rests on three load-bearing properties — DO NOT violate them in future refactors:
    - **(P1)** P-log compio runtime is single-threaded.
    - **(P2)** the `borrow_mut` block that captures `tables_snapshot` contains no `.await`.
    - **(P3)** the path `borrow_mut` drop → `rkyv_encode` → `stream_client.append` → mpsc-send-into-per-stream-worker is purely synchronous; the first `.await` is on the per-stream worker's `ack_rx`, *after* the message lands in the FIFO mpsc.

    Together (P1)–(P3) imply: `borrow_mut` order = mpsc-send order = meta_stream record order. The latest record's `tables_snapshot` therefore necessarily reflects all prior `borrow_mut` mutations, including those of any concurrent publisher. A refactor that introduces an `.await` between the `borrow_mut` drop and the `stream_client.append` (e.g., moving `rkyv_encode` behind an async helper, adding async metric flushes, holding a `futures::lock::Mutex` around the publish) re-opens a stale-snapshot race against the concurrent publisher: a flush whose snapshot was captured earlier could be ack'd later than a compact's, persisting tables that compact has already removed. On restart, recovery would load the stale checkpoint and resurrect compacted-away SSTs whose VPs may now point at GC-punched log_stream extents.

    Inline `// F148-A invariant` comments at both call sites (`flush_one_imm`, `flush_one_imm_local`, both branches of `do_compact`) state the rule next to the code. Test: `f148_publisher_invariant_tests::f148_concurrent_publisher_ordering_invariant` (lib.rs) exercises the pattern with two concurrent simulated publishers and asserts the LATER snapshot extends the EARLIER one.

11. **F183 metrics export.** Each `PartitionData` carries an
    `Arc<PartitionMetrics>` whose AtomicU64 counters are bumped by
    `partition_loop` (req_count on each `handle_incoming_req`,
    imm_full_count when the imm cap stalls intake). The same Arc is
    cloned into the `PartitionHandle` on the main thread; the main
    thread's `report_load_loop` (5 s cadence) snapshots all live
    handles' metrics, computes /sec rates, and ships
    `ReportPartitionLoadReq` to the manager. Manager's policy engine
    consumes the per-partition windowed history (30 min, 1-min buckets)
    to emit advisory split/merge candidates.

    **F187 maintenance debt extension.** Same `PartitionMetrics` struct
    grows six fields used by `compute_maintenance_advisory`:
    `gc_debt_bytes` (gauge, refreshed every GC tick from
    `Σ(get_discards filtered to live sealed log_stream extents)` —
    reuses existing aggregation, no extra RPCs); `pending_compaction_bytes`
    (gauge, refreshed every compact tick: when `has_overlap == 1` it's
    total SST bytes, else it's `pickup_tables(...)`'s output);
    `gc_inflight` / `compact_inflight` (0/1 booleans set around `run_gc`
    and `do_compact` awaits, lets the policy engine skip already-active
    partitions); `last_gc_at` / `last_compact_at` (unix-epoch i64 set
    on successful completion, drives the per-kind cooldown). Helper
    `compute_pending_compaction_bytes(part)` lives in `background.rs`
    and is callable both from the compact loop and (future Stage 2)
    from a PS-local maintenance scheduler.

    Stage 1 keeps the existing random-jitter scheduling intact (10-20s
    compact, 30-60s GC). Stage 2/3 (deferred): replace with a PS-local
    priority scheduler driven by these gauges + a shared fg/bg token
    bucket on top of F141's GC bytes/sec limiter and a new (currently
    missing) compact bytes/sec limiter.

    **F188 Stage 2 (shipped 2026-05-10).** PS-level
    `maintenance_scheduler_loop` (5s cadence on main thread) replaces
    the random-jitter timers as the primary trigger source. Reads the
    F187 metrics, computes `urgency = debt / threshold`, sorts desc,
    dispatches top-K minor compactions / GCs via Send-capable trigger
    channels held in PartitionHandle. Skips partitions whose
    `req_per_sec` (derived from `req_count` diff over the interval)
    exceeds `AUTUMN_PS_FG_QPS_QUOTA` (default 50K) — foreground always
    wins. Cooldowns drive from PS-side `last_*_at` so the gate respects
    actual completion. The compact channel's `bool` payload now means
    `is_major` (true: manual `client compact`, expiry; false: scheduler
    routine — picks via `pickup_tables`).

    Background loops (`background_compact_loop`, `background_gc_loop`)
    keep their channel-receive paths but their timeout branches are
    demoted to short 5-7s metric-refresh ticks — they NO LONGER fire
    compact/GC off the timer (except expiry-major, which is a
    wall-clock event the scheduler doesn't see). Helper
    `compute_pending_compaction_bytes(part)` lives in `background.rs`.

    PS-wide `IoTokenBucket` (parking_lot::Mutex<sliding-window>):
    `Arc<IoTokenBucket>` on PartitionServer cloned into every
    PartitionData. GC + compact append paths call
    `io_bucket.account(bytes).await` BEFORE every network append,
    sleeping if the cluster ceiling
    (`AUTUMN_PS_BG_RATE_BYTES_PER_SEC`, default 256 MiB/s; 0 =
    unlimited) would be exceeded. F141's per-partition GC limiter
    stays as a tighter inner cap.

    **F189 Stage 3 (shipped 2026-05-10)** replaces `IoTokenBucket` with
    `AdmissionController` — a two-class admission controller (CockroachDB
    `kvadmission` pattern, simplified). Same 1-second wall-clock window
    + parking_lot::Mutex<state>, but state now tracks `fg_bytes` and
    `bg_bytes` independently:

    - `account_fg(bytes).await`: sleeps only when an explicit fg
      ceiling (`AUTUMN_PS_FG_RATE_BYTES_PER_SEC`, default 0 =
      unlimited) is set AND would be exceeded. Default returns
      immediately after a single Mutex acquire — keeps the fg hot
      path cheap.
    - `account_bg(bytes).await`: sleeps until BOTH (1) bg's own
      ceiling (`AUTUMN_PS_BG_RATE_BYTES_PER_SEC`, default 256 MiB/s)
      AND (2) fg-aware yield — when fg observed rate >
      `AUTUMN_PS_FG_SATURATED_THRESHOLD * fg_rate` (default 0.8), bg
      waits till the next 1-second window. Fg-aware yield is
      disabled when fg_rate=0 (no baseline to detect saturation).

    Wire site for fg: `start_write_batch` in `background.rs` calls
    `admission.account_fg(total_value_bytes).await` ONCE per batch
    just before launching Phase 2. Per-batch (not per-op) keeps the
    lock acquisition rate at ~1/256 of per-op overhead.

    Wire sites for bg: `do_compact` (chunk-emit + final-emit),
    `flush_gc_batch` (write side), `run_gc` (chunk-read side),
    `process_gc_chunk` (passes through). All call `account_bg`
    instead of the old `account`. F141's per-partition GC limiter is
    kept as an inner cap.

    Tests in `f189_admission_tests` (7, all passing):
    fg_unlimited_no_sleep, bg_unlimited_no_sleep, bg_respects_own_rate,
    bg_yields_when_fg_saturated, bg_does_not_yield_when_fg_idle,
    bg_ignores_fg_when_fg_unlimited, window_resets_after_1s.

12. **F183/F185 partition merge.** F183 shipped the merge primitive
    as a manager-side atomic etcd txn (see `crates/manager/CLAUDE.md`
    note 16). F185 closes the F184-K ~5% merge-window data-loss gap by
    putting the orchestration in the manager (TiKV PrepareMerge model)
    and adding a PS-side write halt:

    Wire path: client → `MSG_MERGE_PARTITIONS { survivor, victim }` →
    manager.handle_merge_partitions → `MSG_MERGE_FREEZE { freeze: true }`
    to victim PS → same to survivor PS → 6× commit_length under the
    freeze → handle_multi_modify_merge atomic etcd txn → return.

    PS-side state on `PartitionData`:
      - `frozen_for_merge: Cell<Option<Instant>>` — `Some(set_at)` while
        the merge-window write halt is in effect.
      - `freeze_drain_ack: RefCell<Option<oneshot::Sender>>` — parked
        freeze response oneshot.

    `handle_incoming_req` short-circuits Put / Delete with
    `CODE_UNAVAILABLE` while frozen; reads + maintenance flow normally.

    `partition_loop` top-of-loop logic:
      - if `freeze_drain_ack.is_some() && pending.is_empty() &&
        inflight.is_empty()`: rotate active + flush every imm via
        `flush_one_imm`, then send OK on the parked oneshot. This is
        the strict precondition for the orchestrator's commit_length
        capture to be race-free.
      - if `frozen_for_merge.is_some_and(|t| t.elapsed() > FREEZE_TTL)`
        (30 s): auto-unfreeze + drop any stale ack with PRECONDITION.
        Backstop for orchestrator crash; happy path completes in <1 s
        and is unfrozen by region_sync_loop dropping the PartitionData.

    Recovery on success: the merge etcd txn deletes victim's region and
    widens survivor's. `region_sync_loop` (F184-B) sees both changes
    on its next tick (~2 s), drops the frozen `PartitionData` for
    victim, and reopens the survivor with `frozen_for_merge = None`.
    No explicit unfreeze needed.

    Recovery on failure: manager sends `MSG_MERGE_FREEZE { freeze:
    false }` rollback to anyone it already froze. If even that fails,
    the FREEZE_TTL backstop fires.

    Why not the spec §4.1 PS-orchestrated 4-gate design: it required
    new Send-capable cross-thread channels + a main-thread
    `merge_service_loop` registry to route freeze coordination between
    survivor's and victim's partition threads (each `PartitionData` is
    `Rc<RefCell<>>`, `!Send`). The TiKV-style "leader-fenced control
    plane orchestrates" model achieves the same 0-loss guarantee with
    no cross-thread plumbing. Trade-off: merge wallclock stays ~2-3 s
    instead of <1 s — bounded by region_sync_loop tick — but the
    write loss is what F184-K actually measured, and that's now 0.

13. **F229 background-loop supervision — no loop dies silently; durability
    loops fail-stop.** Every PS background loop runs under a supervisor wrapper
    instead of a bare `spawn(..).detach()` (which swallowed panics → a dead
    flush/heartbeat loop with no signal). Two helpers in `lib.rs`:
    - `spawn_supervised(name, make)` — catch_unwind + ERROR-log + 1 s restart.
      For RESTARTABLE loops that re-derive state each tick and own no moved
      resource: `heartbeat_loop`, `report_load_loop`, `region_sync_loop`,
      `vp_refs_retry_loop`. Mirrors manager F228 (note 29 there).
    - `spawn_failstop(name, fut)` — catch_unwind; NORMAL return is the expected
      shutdown (no-op); PANIC → ERROR-log + `std::process::exit(1)`. For
      NON-restartable loops that own a moved channel receiver / `TcpListener` and
      are durability/serving-critical: per-partition `background_flush_loop` /
      `background_compact_loop` / `background_gc_loop` and the per-partition
      accept loop. **Why fail-stop, not restart:** the moved receiver can't be
      re-acquired, and re-running on a mid-panic half-mutated `PartitionData`
      could double-apply; exiting lets the manager evict (F069/F111) and reopen
      the partition from the durable streams (log_stream WAL = source of truth →
      no committed loss). **Invariant: never reintroduce a bare
      `spawn(..).detach()` for a PS background loop — pick supervised (re-derive-
      safe) or failstop (moved-resource / durability).** Per-partition
      self-eviction (smaller blast radius than process exit) is a future
      refinement. 1A companion: `ConnPool::get_client` connect is bounded
      (`CONNECT_TIMEOUT`, stream crate) so a blackholed peer can't hang
      `region_sync`'s `open_partition`. Per-conn `handle_ps_connection` spawns
      are intentionally NOT wrapped — a panic there drops one client connection
      (request-scoped), not a background loop.

    **Why explicit `catch_unwind` when `compio::runtime::spawn` already wraps
    in catch_unwind** (`compio-runtime-0.11.0/src/runtime/mod.rs:202`:
    `spawn_impl(AssertUnwindSafe(future).catch_unwind())`; `JoinHandle<T> =
    Task<Result<T, Box<dyn Any + Send>>>`). compio's wrap keeps the *runtime
    thread* alive on task panic — that's exactly what made pre-F229 "silent
    death" possible: `spawn(loop_fn).detach()` had the panic caught by
    compio, then the `JoinHandle` was dropped via `detach`, dropping the
    captured panic with it. The thread survived; the loop didn't; no log
    surfaced. F229's explicit `catch_unwind` is for **observability +
    decisioning** (log, restart, exit), NOT for runtime survival —
    runtime survival is already compio's job. Equivalent designs that read
    compio's caught panic via `JoinHandle.await -> Result<T, Box<dyn Any>>`
    require an extra outer `spawn` to preserve the spawn-and-forget API and
    aren't simpler; the explicit form here matches the inline comment intent
    ("catch_unwind + 1 s restart" reads at the call site without indirection).
    The two catch_unwind layers are not bug-redundant — compio's catches a
    panic that our inner has already turned into `Result::Err`, so it sees
    nothing. Don't try to "remove the duplicate"; you'd lose the log + the
    restart/exit semantic.

14. **`lookup_in_sst` MUST hop to the next block when the in-block binary
    search runs off the end (block-boundary point-lookup).** The GET point
    lookup seeks `target = user_key ++ 0x00 ++ BE(0)` — SMALLER than every
    real entry for that key (entries carry `BE(u64::MAX - seq)`, always > 0).
    `SstReader::find_block_for_key` returns the LAST block whose base_key <=
    target. When a user_key's NEWEST entry happens to be the FIRST entry
    (base_key) of block B, `base_key[B] > target`, so the lookup lands on
    block B-1; the binary search there finds no key >= target (returns
    `lo == n`). Pre-fix `lookup_in_sst` returned `None` → GET fell through to
    an OLDER SST and returned a STALE value (the data was present and correct
    in block B all along). Fix: on `lo == n`, read block `block_idx + 1` and
    check its first entry (entry 0 = its base_key = the smallest key >
    target). **Invisible until SSTs exceed one block (>64 KiB / table)** — a
    64-key partition has a single block (block 0 always correct), so this hid
    until the 1024-key f250 reproducer built multi-block SSTs. This was the
    "fence+flush data loss" — actually a read-only block-boundary bug, no
    on-disk corruption. `TableIterator::seek` (compaction / range, iterator.rs)
    already does this next-block hop (`!bi.valid()` → `block_idx += 1`), so
    those paths were never affected. **Invariant: any point lookup over the
    block-indexed SST must treat "no key >= target in the selected block" as
    "check the next block's first entry," never as "absent."** Regression
    test: `background::lookup_block_boundary_tests::
    lookup_in_sst_finds_keys_at_block_boundaries`. Localization aid:
    `MSG_DIAG_TRACE_KEY` (per-SST newest-seq under bloom-gated / no-bloom /
    fullscan scans) distinguishes a bloom false-negative vs a block-selection
    miss vs true data loss; used by `crates/manager/tests/
    f250_fence_flush_invariant.rs`.

15. **The `flushing_imm_ptrs` claim MUST be released on EVERY flush error path,
    and reads (RANGE/HEAD) MUST be served off `partition_loop` (Mode B fix).**
    `flushing_imm_ptrs` is a per-imm "who is flushing this" latch shared by the
    two flush drivers on the P-log runtime — `background_flush_loop` (lazy) and
    `flush_one_imm` (eager, called inline by split / merge / graceful+freeze
    drain) — so the same imm is never double-flushed. Its load-bearing CONTRACT:
    *the claimer always reaches `commit_flush_outcome`, which pops the imm +
    removes the ptr.* If a flush ERRORS without removing the ptr, the imm is
    orphaned (claimed but no-one flushing) → every later flush attempt sees it
    "already claimed" and defers to a phantom flusher → the imm NEVER drains →
    `partition_loop` stays imm-full parked (F120-A) FOREVER → since RANGE/HEAD
    were delegated through the parked loop, they wedge permanently (GET kept
    working because it is served locally on ps-conn). Root cause = a
    `row_stream` flush append wedged on a dead replica during a degraded window.
    **Two-part fix, both load-bearing:**
    - **Release on every error.** `run_flush_async_phase` AND
      `commit_flush_outcome` are thin wrappers that `flushing_imm_ptrs.remove`
      their `src_imm_ptr` on any Err (idempotent — no-op if the inner already
      popped + removed). `background_flush_loop` also releases the ptr of a
      *successful* outcome it must DROP because a FIFO-earlier imm in the same
      batch failed. The loop then retries pending imm every `FLUSH_RETRY_BACKOFF`
      (2 s) so a failed flush self-heals once the cluster recovers — even with no
      new write to signal it (a parked loop produces no rotate → no `flush_tx`).
      **Invariant: any new flush error path must release the claim (or the imm
      orphans + the partition permanently wedges).**
    - **Reads off the write loop.** RANGE + HEAD are served LOCALLY on the
      ps-conn task (`serve_read_local`, both dispatch points), mirroring
      `serve_get_local` for GET. `handle_range`/`handle_head` brief-`borrow()` to
      snapshot then `drop` before iterating — same single-thread RefCell +
      memtable-`RwLock` discipline that makes GET-local safe — so a flush-wedged
      (back-pressured) `partition_loop` halts WRITES (correct) without taking
      READS down. Relaxes per-connection read-your-writes for a read pipelined
      behind an in-flight delegated write on the same connection (same gap
      GET-local already has); a connection-level prior-write barrier over all
      local reads is a noted follow-up. **Invariant: read handlers served locally
      must never hold a `RefCell` borrow across an `.await`.**

16. **row_stream single-writer is now a type-level invariant; P-bulk spawn
    failure is fatal-for-this-partition (F254).** All `row_stream.append` MUST go
    through P-bulk's `bulk_sc` — flush via `FlushReq`, compaction via
    `RowAppendReq`. Pre-F254 we kept a coincidence-based fallback: when
    `spawn_bulk_thread` failed, `flush_req_tx`/`row_append_tx` were `None` and
    both flush + compact ran inline on P-log against `part_sc`. That accidentally
    preserved single-writer because BOTH writers degraded to the same client; if
    only one degraded (or someone added a third row_stream-writing edge), the
    `invalid_meta_len` corruption mode (two `StreamClient`s racing per-stream
    commit watermarks on the same row_stream → ExtentNode truncates one
    writer's SST bytes per commit protocol step 5) would recur. **Mitigation:**
    `open_partition` propagates `spawn_bulk_thread` Err; the manager reschedules.
    `PartitionData.flush_req_tx` and `row_append_tx` are non-`Option<Sender>`.
    **Invariant: any new row_stream-mutating path MUST go through P-bulk
    (`FlushReq` or `RowAppendReq`). `part_sc` is for log/meta append and
    row_stream non-append ops (`truncate`, `get_stream_info`) only.**

    **Follow-up fixes shipped as F255 (closes the two coco-audit findings the
    F254 review surfaced; both predated F254 and are independent of the
    fallback removal):**
    - **(P0) split → P-bulk row_stream invalidate race — FIXED via SYNC
      BARRIER.** Pre-F255 (and the v1 F255 attempt) the partition carried
      a `Cell<bool> need_invalidate_row_stream` flag that
      `handle_split_part` set; each P-bulk message (FlushReq /
      RowAppendReq) piggybacked the flag via a `fetch-and-clear` at send
      time. coco /arch GPT-5.5 surfaced (2026-06-02, second pass) that
      the lazy form is racy with P-bulk's `FuturesUnordered` cap=2: a
      post-split FlushReq with `invalidate=false` (flag already taken by
      an earlier RowAppendReq) could enter the FU CONCURRENTLY with the
      invalidating RowAppendReq and append to the stale per-stream worker
      BEFORE the invalidate took effect → SST past sealed_length → orphan
      SST on recovery (`stale_vp_offset_past_sealed_length` / missing-SST).

      The v2 fix replaces the lazy flag with a SYNCHRONOUS P-log → P-bulk
      barrier:
      1. New `RowInvalidateBarrierReq { row_stream_id, resp_tx }` message
         + `mpsc::Sender<RowInvalidateBarrierReq>` channel
         (`PartitionData.row_invalidate_tx`, capacity 1).
      2. `flush_worker_loop` consumes the barrier on a priority-biased
         `select_with_strategy` (PollNext::Left = invalidate stream).
         When picked, it sets a `pending_barrier` slot.
      3. The next loop iteration drains `inflight` `FuturesUnordered` to
         ZERO via `while inflight.next().await`, then calls
         `bulk_sc.invalidate_stream(row_stream_id)`, then signals the
         barrier's `resp_tx`. No new SQ message is picked up until the
         barrier completes.
      4. `handle_split_part` sends the barrier INSIDE the critical section
         **BEFORE `multi_modify_split`** (after the drain + commit_length,
         before the manager seal) and AWAITS the ACK. Pre-seal placement
         is REQUIRED so the failure path is cleanly abortable: if the
         barrier send / ACK fails, the manager has not yet committed the
         seal, so unfreezing + returning Err leaves the cluster in a
         coherent pre-split state. Post-seal placement (the v2 first
         draft) would create an unrecoverable window — manager-committed
         seal + local state not converged + freeze cleared (coco
         /findbugs 2026-06-02 v2 review). At the moment of the send,
         gates are still held + `frozen_for_split` halts writes, so
         P-bulk's queue is normally empty; by the time gates release,
         bulk_sc is invalidated and subsequent compact / flush re-fetch
         a fresh tail from the manager — which by then carries the
         post-seal extent.

      The `need_invalidate_row_stream` / `invalidate_row_stream` field
      machinery is GONE — `FlushReq` and `RowAppendReq` carry no
      invalidate flag, `compact_row_append` doesn't borrow `part`, and
      `run_flush_async_phase_inner` doesn't fetch any flag. The barrier
      is the single source of truth.

      **Per-partition compact_gate is load-bearing.** v3 of the F255 review
      (coco /findbugs 2026-06-02) caught that the PS-wide
      `ConcurrencyController.acquire_compact()` permit (default max=4)
      does NOT serialize same-partition: split on partition P could acquire
      one permit while a concurrent `do_compact` on partition P held
      another and emit `compact_row_append` RowAppendReqs that raced the
      seal. Fix: `PartitionData.compact_gate: Arc<CompactionGate>` (max=1
      per partition). Acquired by `background_compact_loop`'s three
      do_compact dispatch points (main / expiry-major / defensive auto)
      and by `handle_split_part`. Acquisition order at the split site is
      strictly outer→inner: per-partition `compact_gate` → PS-wide
      `acquire_compact` → per-partition `gc_gate`. Same-partition
      compact-vs-split is now serialised; cross-partition concurrency
      preserved.

      **Invariants:**
      1. Any new control message that mutates row_stream from P-log MUST
         travel through P-bulk's channel set — never write row_stream
         directly via `part_sc` (note 16 above).
      2. `handle_split_part` MUST await the barrier ACK BEFORE releasing
         `compact_gate`, `gc_gate`, or clearing `frozen_for_split`. If a
         future refactor moves any of those releases earlier, the race
         re-opens — a freshly-resumed background_compact_loop /
         background_flush_loop can send a P-bulk request before
         bulk_sc is invalidated.
      3. The barrier channel is capacity 1; `handle_split_part` is the
         sole sender. If another sender is added later, prove the
         barrier sender count stays bounded (1) or convert to an
         unbounded channel.
      4. `flush_worker_loop`'s `pending_barrier` slot MUST drain
         `inflight` to ZERO before invalidating + ACKing. Skipping the
         drain reintroduces the race coco surfaced (in-flight append on
         stale worker landing after the invalidate ACKed).
      5. The priority-biased select (PollNext::Left) is DEFENSIVE — under
         today's gate discipline the SQ queue is empty when the barrier
         arrives, but the bias keeps the design race-free if a future
         refactor relaxes the gates.
      6. `handle_split_part` sends the barrier BEFORE `multi_modify_split`.
         Failure of the barrier send / ACK must abort split with the
         manager not yet committed; post-seal placement (the v2 first
         draft) would leak an unrecoverable window where manager-
         committed seal + local state not converged.
      7. ANY new do_compact dispatch site (future code paths that bypass
         `background_compact_loop`) MUST acquire `compact_gate` first,
         then `acquire_compact`. Same goes for split-shaped operations.
      8. BOTH the barrier `send().await` AND the ACK `recv().await`
         MUST be bounded by separate timers (5 s send + 10 s ACK
         today). The total budget (15 s) MUST stay well under
         `FREEZE_TTL` (30 s, lib.rs). Pre-bound (or only-ACK-bounded),
         a wedged P-bulk could block the await past `FREEZE_TTL`,
         `check_freeze_ttls` would unfreeze, new writes would extend
         the row_stream tail past the already-captured
         `commit_length`, and the eventual continuation would call
         `multi_modify_split` with the STALE `row_end` → post-TTL
         writes above sealed_length, invisible on recovery (coco
         /findbugs v4 + v5 findings). The send is independently
         bound-able because `row_invalidate_tx` is capacity 1: a prior
         split's barrier sitting un-consumed in the channel (P-bulk
         wedged on a stuck flush) back-pressures the next split's
         send BEFORE the ACK timer arms. Both timeouts unfreeze +
         return `FailedPrecondition` so the partition resumes serving
         and the client can retry. If future tuning changes either
         timeout, the relation `send + ack < FREEZE_TTL` MUST hold.

      Regression guards (lib.rs):
      `f255_row_invalidate_barrier_req_round_trips` (wire shape +
      ACK round-trip) + `f255_invalidate_wins_priority_select` (PollNext
      bias).

    - **(P2) P-bulk readiness handshake — FIXED.** `spawn_bulk_thread`
      now returns `(JoinHandle, oneshot::Receiver<Result<()>>)`. The
      spawned thread sends `Ok(())` only AFTER compio `RuntimeBuilder::
      build` succeeds AND `StreamClient::new_with_owner_epoch` succeeds AND
      `set_reporter_part_id` runs; runtime / StreamClient init failure
      sends `Err(_)` so P-log surfaces the real failure cause.
      `partition_thread_main` awaits the receiver before publishing
      `flush_req_tx` / `row_append_tx` / `row_invalidate_tx` into
      `PartitionData` — a partition is never half-opened with a live
      Sender to a dropped Receiver. Thread abort (drop without send)
      surfaces as `Err(Canceled)` so the open path returns instead of
      hanging. **Invariant: any future change to `spawn_bulk_thread`'s
      in-thread init order MUST send the ready signal only after ALL
      preconditions for `flush_worker_loop` are met — a premature
      `Ok(())` regresses to the pre-F255 wedge.**
      Regression guard:
      `f255_bulk_ready_signal_dropped_returns_canceled_not_hang`.

17. **F265 `part_addr` self-heal in `sync_regions_once`.** The manager's
    `part_addrs` (per-partition listener addresses, the thing
    `GetRegionsResp.part_addrs` serves to clients for routing) is
    in-memory only and is LOST on manager restart. Registration used to
    happen exactly once, inside `open_partition` — an already-open
    partition never re-reported, so a manager kill+respawn under a
    fully healthy cluster left clients unable to resolve any partition
    listener (every get/put failed; observed as a 30-minute total
    outage in the F265 transport chaos run, ending only when an
    unrelated PS failover forced reopens). `sync_regions_once` now
    compares the just-fetched `resp.part_addrs` against the open
    partitions' `PartitionHandle.part_addr` and re-sends
    `MSG_REGISTER_PARTITION_ADDR` for any missing/stale entry. Zero
    steady-state cost (view matches → no RPC); convergence after a
    manager restart is one ~2 s sync tick. The manager handler is no
    longer leader-gated (in-memory idempotent hint; see manager
    CLAUDE.md note 35). **Invariant: any new code path that binds or
    re-binds a partition listener must keep `PartitionHandle.part_addr`
    equal to the ACTUALLY advertised address — the self-heal trusts it
    as the source of truth.**

18. **F265 heartbeat-loss exit is a 90 s last resort, not a 10 s tripwire.**
    `heartbeat_loop`'s `MAX_CONSECUTIVE_FAILURES` is 45 (× 2 s = 90 s; was
    5 = 10 s). The `std::process::exit(1)` on sustained heartbeat failure
    guards the narrow "partitioned from the manager but not from clients"
    stale-serving case — it is NOT the primary fencing (owner_epoch fences
    writes at the ENs; region_epoch fences client routing). At 10 s, any
    manager outage > 10 s — and EVERY ucx manager kill+respawn pays a
    ~60 s TIME_WAIT bind retry (F264) — made the entire PS fleet exit
    simultaneously: a self-inflicted, unrecoverable data-plane outage
    (nothing left to re-register when the manager returned; observed in
    the F265 ucx chaos round). While the manager is down NO reassignment
    can happen (the manager performs it), so serving through its outage
    is safe. Companion manager-side guard: the eviction sweep is gated on
    `serving` (listener actually bound) and heartbeat clocks are re-seeded
    at listener-ready (manager CLAUDE.md note 35).

19. **F267 owner locks are PER-PARTITION, acquired fresh at every
    `open_partition`.** `owner_key = "partition/<part_id>"` — ONE stable logical key per
    partition, NO ps_id in the key (coco P1: a per-PS key shape would
    leave the old owner's key valid at the manager after takeover, so
    its lingering GC punch_holes/truncate could still mutate streams
    owned elsewhere; with a stable key the takeover acquire bumps THE
    SAME key and the old owner fails `ensure_owner_epoch` everywhere); `acquire_partition_owner_epoch` walks the manager list.
    P-log's `part_sc` and P-bulk's `bulk_sc` share the ONE epoch acquired
    for that open (via `new_with_owner_epoch`). Why: the epoch must be
    newest-at-TAKEOVER, not newest-at-process-start — a standing PS
    inheriting partitions from a PS that acquired LATER sat below the EN
    fence floors forever (manager-HA chaos H2; F265's bump-on-acquire
    only fixed the respawned-PS case). Per-partition keys scope fencing
    exactly: an open of partition X bumps only X's key — siblings keep
    their epochs; X's previous owner alone is fenced (manager equality +
    EN floor) and self-evicts via LockedByOther. The PS-lifetime
    `server_owner_key`/`server_revision` ("ps-<id>") remains ONLY for
    split-coordination RPCs. Cross-ref: manager CLAUDE.md note 35
    (F265 bump-on-acquire), stream CLAUDE.md note 1.

20. **F267 manager-list rotation invariants (4 bugs from manager-HA
    chaos — keep all four properties or HA failover silently dies).**
    (a) `PartitionServer.current_mgr` is `Rc<Cell<usize>>` — the struct
    is cloned once per supervised loop; a plain Cell gave each loop a
    PRIVATE rotation index (only heartbeat's rotated; region_sync +
    the F265 part_addrs self-heal hammered the dead manager forever).
    (b) Every StreamClient manager RPC routes through `manager_call`
    (rotate on transport failure) or `retry_manager_call`; decode sites
    call `note_manager_code` (rotate on CODE_NOT_LEADER). Never add a
    raw `pool.call_timeout(self.manager_addr(), ..)` site.
    (c) `MSG_GET_REGIONS` + `MSG_HEARTBEAT_PS` are leader-gated; the PS
    heartbeat Ok-arm rotates on NOT_LEADER and COUNTS it toward the 90 s
    stale-serving exit budget (only a real leader ACK resets — coco P1:
    a follower-only-reachable PS is partitioned from the leader and may
    be getting evicted),
    and `sync_regions_once` rotates on NOT_LEADER. A follower answering
    OK pins the shared rotation to itself while serving empty/stale
    `part_addrs` (manager-HA chaos H3).
    (d) Client SDK `refresh_regions` rotates + retries on NOT_LEADER.

21a. **LAT-1 latency histograms.** `PartitionMetrics.write_lat/get_lat` (write = Put+Delete+FenceBump batch ops, coco P3 rename)
    (`LatHist`: 9 finite ns buckets 0.5ms..250ms + sum + count,
    non-cumulative storage, cumulative `le` at render). PUT observes the
    ALREADY-MEASURED `BatchStats.end_to_end_ns` once per batch with
    `n = ops` (every op in a group-committed batch experienced the
    batch's latency — zero new hot-path timing); GET adds one `Instant`
    pair + RefCell borrow in `serve_get_local` (same cost class as the
    F183 req_count add). A/B perf-check (4K p8 d8): write 999,864 →
    1,069,685 ops (no regression). Rendered as Prometheus histograms in
    `metrics_text`.

21. **/metrics (observability batch 1).** `PartitionServer::metrics_text()`
    renders the Prometheus text snapshot on the MAIN compio thread (the
    `partitions` map is `Rc<RefCell>`); the binary's `--metrics-port` path
    spawns a 2 s publisher task that copies the string into an
    `Arc<RwLock<String>>` served by the `autumn_common::metrics_http`
    listener thread. Export rules: only `req_count_monotonic` (the
    never-reset counter — `req_count` is swap-reset every 30 s by
    `report_load_loop` and would saw-tooth) plus gauges
    (size/gc-debt/pending-compaction bytes, gc/compact inflight, sealed log
    extents). Emission is metric-major (all samples of one metric
    contiguous after its `# TYPE` line — the Prometheus text format
    requires grouping; the first cut interleaved per-partition and was
    non-compliant).
