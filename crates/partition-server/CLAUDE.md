# autumn-partition-server Crate Guide

## Purpose

An LSM-tree based KV store built on top of the stream layer. Each `PartitionServer` owns one or more **partitions**, each covering a contiguous key range. Implements the `PartitionKv` gRPC service.

## Architecture

### Thread Model

```
Main compio thread (control plane + fd dispatcher)
├─ heartbeat_loop          ← periodic manager heartbeat
├─ region_sync_loop        ← discover/open/close partitions
└─ fd-dispatch loop        ← rx.next() → partition handle.fd_tx

Accept OS thread (blocking)
└─ std::net::accept → tx   ← dedicated accept, sends to main via channel

Partition threads — 2 OS threads per partition:
├─ part-N (P-log): OWNS
│     • partition_loop (request dispatch + group-commit SQ/CQ; write loop is
│       inlined here — no per-Put spawn/oneshot)
│     • per-partition accept loop + ps-conn tasks (one per live client conn),
│       all on THIS compio runtime
│     • background_maintenance_loop (compaction + GC on ONE task), flush loop
│     • PartitionData (Rc<RefCell>) shared across all tasks on this runtime
│     • dedicated StreamClient + ConnPool for log_stream/meta_stream
├─ part-N-sst (P-sst): flush_worker_loop
│     • own compio runtime + io_uring + ConnPool + StreamClient
│     • runs build_sst_bytes + row_stream.append + save_table_locs_raw
│
P-log → P-sst: mpsc::Sender<FlushReq> (capacity 1 → sequential flushes)
P-sst → P-log: oneshot::Sender<Result<(TableMeta, SstReader)>>
```

**Thread count**: `1 main + 1 ps-accept + 2N partition` = `2N + 2` OS threads.

**Why two OS threads per partition?** A 128 MB row_stream flush holds the P-log
compio runtime for hundreds of ms (syscall + 3-replica fanout CQE wait),
head-of-line-blocking the log_stream 4 KB WAL batches sharing the same io_uring.
P-sst gives flush its own runtime so WAL appends make forward progress
concurrently with SST uploads.

**ps-conn handoff (per-partition listeners)**: each partition OS thread binds its
OWN `compio::net::TcpListener` on a unique port (`base_port + ord`) and runs its
own accept loop + ps-conn tasks on the SAME compio runtime as `partition_loop`.
The main thread does NOT forward fds across partitions; clients connect directly
to the owning partition's port (`part_addr` reported via
`MSG_REGISTER_PARTITION_ADDR`, served to clients via `GetRegions.part_addrs`).
Each ps-conn task's `req_tx.send(req).await` is a same-thread mpsc send; the
matching `req_rx.next().await` inside `partition_loop` wakes via a local Rc-based
Waker — no eventfd, no cross-thread futex.

**ps-conn handler — SQ/CQ inner loop** (`handle_ps_connection`, one task per TCP
conn):

```
┌─ handle_ps_connection ──────────────────────────────────────────┐
│  SQ side — persistent read future:                              │
│    Option<LocalBoxFuture<'static, PsReadBurst>>                 │
│    owns OwnedReadHalf + 64 KiB buf across iterations;           │
│    NEVER dropped mid-flight (io_uring SQE stability)            │
│                                                                 │
│  CQ side — FuturesUnordered<LocalBoxFuture<'static, ...>>       │
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
│         complete frame AND inflight/tx_bufs empty, run           │
│         request→response→write inline via write_all             │
│         (no FU, no Box::pin, no write_vectored)                 │
│       at_cap          → await completion alone (back-pressure)  │
│       n_inflight == 1 → await completion (avoid select cost)    │
│       n_inflight > 1  → select(read, inflight.next())           │
│    (D) on EOF: drain remaining inflight + final flush + return  │
└─────────────────────────────────────────────────────────────────┘
```

At `--pipeline-depth ≥ N`, one TCP read delivers N frames → all N futures run
concurrently → drain-all-ready collects up to N ready replies into `tx_bufs` →
one `write_vectored_all` = one `tcp_sendmsg`. This targets small-frame TCP kernel
overhead (32–63 B PutResp headers): ~N× fewer kernel TCP traversals per Put.

**Back-pressure**: if `inflight.len()` reaches the cap mid-push,
`push_frames_to_inflight` awaits one completion before pushing the next future —
caps memory per pathological client (a large pipeline burst all targeting one
partition).

**Mis-routed frames** (`part_id != owner_part`) synthesise an immediate
`NotFound` error frame onto inflight — no mpsc hop. With per-partition listeners
each `handle_ps_connection` serves only frames whose `part_id == owner_part`; a
partition-aware client opens one TCP connection per partition port and stripes
requests by partition id.

```
┌─────────────────── PartitionServer ────────────────────┐
│  Rc<RefCell<HashMap<part_id, PartitionHandle>>>         │
│  (ps-conn tasks run on the P-log runtime and use a      │
│   same-thread PartitionRequest mpsc; no cross-thread    │
│   wake, no Arc<PartitionRouter>)                        │
│                                                          │
│  ┌──────── PartitionData (per partition thread) ───┐    │
│  │  active: Memtable (RwLock<BTreeMap>)             │    │
│  │  imm: VecDeque<Arc<Memtable>>   ← frozen tables  │    │
│  │  sst_readers: Vec<Arc<SstReader>>  ← oldest→new  │    │
│  │  tables: Vec<TableMeta>          ← aligned        │    │
│  │                                                   │    │
│  │  log_stream_id   ← WAL + large values             │    │
│  │  row_stream_id   ← SSTables                       │    │
│  │  meta_stream_id  ← TableLocations checkpoint      │    │
│  │                                                   │    │
│  │  seq_number: monotonic MVCC counter               │    │
│  │  has_overlap: AtomicU32                           │    │
│  └───────────────────────────────────────────────────┘   │
│                                                          │
│  stream_client: Arc<StreamClient>                        │
└──────────────────────────────────────────────────────────┘
```

## MVCC Key Encoding

Internal (storage) key = `user_key ++ 0x00 ++ BigEndian(u64::MAX - seq_number)`

The null byte (`0x00`) is a **separator** between the user key and the inverted
sequence number. INVARIANT: without it, a user key that is a prefix of another
(`"mykey"` vs `"mykey1"`) sorts incorrectly in internal-key space —
`"mykey\x00..."` sorts before `"mykey1\x00..."` because `0x00 < '1'`.

The **inverted** sequence ensures that for the same user key, newer writes
(higher seq) sort **before** older writes in byte order. Lookup uses
`seek_user_key` which seeks to `user_key ++ 0x00 ++ BE(0)` — the smallest
possible internal key for this user key — then returns the first (newest) entry.

## SST VP dependency tracking (`vp_deps`)

Each SST `MetaBlock` still carries `vp_deps: Vec<u64>` (the distinct log extent
ids referenced by live `ValuePointer`s), and `SstReader.vp_deps` mirrors it, but
both are **INERT**: the builder writes it, the reader decodes it, nothing reads
it. The PS does NOT compute or sync any per-partition VP-ref snapshot. Retention
is `refs`-based; it is safe because GC's relocate-then-punch invariant guarantees
`refs == 0 ⇒ no live VP`. Split-lifetime correctness (shared SSTs still holding
old `ValuePointer`s after a child truncated the log) is upheld by GC relocating
live values out before `punch_holes` drops `refs`
(`crates/manager/tests/system_vp_after_split_gc.rs`).

## Write Path: Put / Delete (Group Commit)

```
Put(key, value, part_id, must_sync):
  1. ps-conn task: decode frame; push
     `async { clone req_tx → send PartitionRequest → await oneshot resp →
              encode Frame::response }` onto the per-conn inflight FU.
  2. Same-thread mpsc: PartitionRequest delivered into partition_loop.
  3. P-log partition_loop: decode PutReq inline, push a WriteRequest with a
     direct WriteResponder::Put { outer: resp_tx, key } into `pending`.
     NO spawn, NO inner oneshot.
  4. ps-conn awaits the outer resp_tx via the inflight future — Phase 3 fires
     its encoded PutResp frame directly into it.
  5. ps-conn loop top: drain-all-ready completions into tx_bufs; flush via ONE
     write_vectored_all — coalescing all responses ready since the last flush.

partition_loop (per partition):
  OWNS:   FuturesOrdered<Pin<Box<dyn Future<Output=InflightCompletion>>>>
  CAP:    AUTUMN_PS_INFLIGHT_CAP (default 8, range [1, 64])
  RECV:   req_rx: mpsc<PartitionRequest> (WRITE_CHANNEL_CAP = 1024)
          — the SAME channel carries reads + writes from ps-conn

  Loop (per iteration):
    (A) drain ready completions via inflight.next().now_or_never()
        → run Phase 3 (memtable insert + WriteResponder::send_ok) each
    (B) if pending.non_empty && !at_cap && !imm_full:
          launch_new_batch:
            Phase 1: validate, seq-assign, encode WAL records
            Launch Phase 2: stream_client.append_batch future (NOT awaited)
            Push (BatchData, Phase2Fut → InflightCompletion) into FU
    (C) if at_cap: await inflight.next() (back-pressure) → Phase 3
    (D) branch on n_inflight:
          == 0:  await req_rx.next() alone (cold idle)
          >  0:  select(req_rx.next, inflight.next())
                 Left  (SQ) → handle_incoming_req:
                              PUT/DELETE: decode + pending.push
                              GET/HEAD/RANGE/SPLIT/MAINTENANCE: inline via
                              dispatch_partition_rpc (reads run inline on P-log)
                 Right (CQ) → run Phase 3 on the completion
    (E) non-blocking drain of any queued requests (decode inline)

  Shutdown (req_rx closed): drain all inflight (await-loop, Phase 3 each so
    clients get their final ack); flush residual pending as one last batch.

  Error handling:
    LockedByOther on any completion → set locked_by_other, drain inflight,
      return (partition self-evicts in the enclosing loop).
    Other append errors → log + propagate Err to each client's oneshot.
```

Phase 1 / Phase 3 primitives are `start_write_batch` / `finish_write_batch`
(`background.rs`); Phase 2 is wrapped into a boxed `InflightCompletion` future.
Phase 3 runs at most once per loop iteration (single-threaded compio task), so
the partition write lock is never held concurrently — `maybe_rotate_locked`
stays correct.

`Delete` sends `WriteOp::Delete{user_key}`, writes `op = 2` (tombstone).

### Natural batching

`partition_loop` launches whatever `pending` holds as soon as a pipeline slot is
free (`!at_cap && !imm_full`) — no minimum-batch gate. Batch size adapts to
arrival-rate × in-flight latency (group-commit style). Fragmentation is prevented
STRUCTURALLY: ps-conn tasks share the P-log thread and enqueue a whole TCP burst
into req_rx before partition_loop is polled, and the (E) drain pulls the entire
channel into `pending` (up to MAX_WRITE_BATCH) each iteration, so a naturally-full
burst still launches as ONE batch. `--min-pipeline-batch` is parsed but a
deprecated no-op.

### In-order Phase 3 commit

INVARIANT: `partition_loop` uses **`FuturesOrdered`** (not `FuturesUnordered`) so
Phase 3 yields are strictly in launch order = seq order — the rotated active
memtable must contain a **contiguous** seq range. Otherwise batch B (seq 101–200)
could Phase 3 + flush → SST with `last_seq=200` while batch A (seq 1–100) is still
in flight; on crash, replay's dedup `if ts <= sst_max_seq { continue; }` skips
seq 1–100 (`50 <= 200`), silently dropping ack'd writes. The trade-off is a small
p99 uptick from head-of-line wait (bounded by Phase 2 p99, ~5–10 ms with
coalesced fsync); throughput unchanged.

Related always-hold properties: seq numbers assigned in Phase 1 in launch order;
memtable MVCC keys sort independently of insertion order; LogStream ordering is
preserved by the stream worker's lease/ack cursor (concurrent Phase 2 lands at
distinct contiguous offsets regardless of completion order).

### Cross-layer SQ/CQ stack

```
┌─ PS partition_loop  (this crate)                              ┐
│    FuturesOrdered<InflightCompletion>, cap 8                  │
└─────────────┬─────────────────────────────────────────────────┘
              ▼  stream_client.append_batch(log_stream_id, …)
┌─ autumn-stream stream_worker_loop                             ┐
│    FU<3-replica-join>, cap 32, per stream_id                  │
└─────────────┬─────────────────────────────────────────────────┘
              ▼  pool.send_vectored per replica
┌─ autumn-rpc writer_task — single SQ per conn                  ┐
└─────────────┬─────────────────────────────────────────────────┘
              ▼  TCP
┌─ autumn-stream handle_connection (server side)                ┐
│    FU<batch-io>, cap 64, persistent read future               │
└───────────────────────────────────────────────────────────────┘
```

### P-sst SQ/CQ (flush_worker_loop)

Same FuturesUnordered + select pattern on the bulk thread, cap = 2 (env
`AUTUMN_PS_SST_INFLIGHT_CAP`, range [1, 16]). Each in-flight flush holds a 128 MB
SST buffer, so the cap is deliberately small. The overlap benefit: while one SST
uploads via `row_stream.append`, the next flush can start its `build_sst_bytes`
`spawn_blocking` — CPU (build) overlaps network (upload) without ballooning peak
memory. P-sst needs no special `StreamClientConfig`; the stream client derives
every append deadline from the actual payload size (256 MiB → ~37 s), which also
covers P-log's large-value log_stream appends.

**Single-writer invariant for row_stream:** ALL appends to `row_stream` MUST go
through P-sst's `StreamClient`. Two independent `StreamClient`s track commit
position locally; if one writer's stale commit is sent in an append header,
ExtentNode truncates data written by the other → destroyed SST data and
`invalid meta_len` corruption on PS restart. `flush_worker_loop` accepts:
- `FlushReq` (from flush_loop): build SST + row_stream.append
- `RowAppendReq` (from compaction on P-log): row_stream.append only

Both share P-sst's single `StreamClient` so the per-stream worker's commit/lease
state stays coherent. **Never use P-log's `part_sc` for row_stream appends**;
`part_sc` is for log/meta append and row_stream non-append ops (`truncate`,
`get_stream_info`) only. The invariant is type-level: `open_partition` returns Err
when P-sst fails to spawn (the manager reschedules), so
`PartitionData.flush_req_tx` / `row_append_tx` are non-`Option<Sender>` — there is
no in-thread fallback.

**Record format** (WAL): `[op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key][value]` (17-byte header).

**No local WAL file**: logStream is the sole write-ahead log. Recovery replays
logStream from the VP head recorded in the last metaStream checkpoint.

### Per-ino fence floors (write leases)

Write requests stamped with `(inode_hint != 0, lease_epoch)` are checked against
`PartitionData.fence_floors` (per-partition `HashMap<ino, max epoch seen>`):
`stamped < floor` ⇒ `CODE_FENCED` (a revoked writer's late RPC), else admit +
raise the floor. All three write entry points run it — `enqueue_put`,
`enqueue_put_bulk` (meta carries the two fields, `PUT_BULK_HEADER_LEN = 44`),
`enqueue_batch_put` (per-op; a fenced op gets CODE_FENCED in its statuses slot
without failing the batch). DELETE is fenced too (`enqueue_delete`).

INVARIANT (admission ordering): `region_epoch → in_range → value-too-large →
fence` — a rejected request must NEVER raise or persist the floor (else a rejected
oversized write could poison the floor).

Persistence — floors survive restart / reschedule:
- **WAL**: when `check_and_bump_fence` returns `Ok(true)` (raised), the dispatcher
  queues `WriteOp::FenceBump { ino, epoch }` (`WriteResponder::Fence`, no client
  reply) BEFORE the admitted write — same group-commit pipeline ⇒ the floor is
  durable no later than the write's ACK. WAL record op `OP_FENCE_BUMP` (0x08),
  key = ino BE8 (skips in_range), value = epoch LE8. It NEVER enters the memtable
  (Phase 3 filter) or SSTs; GC's VP scan skips it (no VP bit).
- **Checkpoint**: `TableLocations.fence_floors` snapshot, captured under the SAME
  borrow as the tables snapshot at all three publish sites (metadata-publish
  ordering invariant intact).
- **Recovery**: seeds floors by max-merge over ALL meta records, then replay
  max-merges every OP_FENCE_BUMP (idempotent — deliberately bypasses ts-dedup;
  re-applying a checkpoint-covered bump is a no-op).

Client surface: `WriteLease` + `put_fenced`/`put_bulk_fenced`/`put_many_fenced`
(`AutumnError::Fenced`); fuse stamps from `held_leases[ino].version`, the ioring
daemon from `OpenedExtents.lease_version`. `MSG_STREAM_PUT` is REMOVED (0x46
reserved).

### `MSG_BATCH_PUT` (0x53) — server-batched Put

One frame carries `BatchPutReq { part_id, region_epoch, must_sync,
ops: Vec<BatchPutOp{key, value, expires_at}> }`. The SDK
(`ClusterClient::batch_put`) groups items by owning partition and emits one frame
per partition. Server side (`enqueue_batch_put`):
1. Decode the frame ONCE on the ps-conn task.
2. Allocate one `Rc<BatchPutAccumulator>` carrying the outer `resp_tx`, a
   `RefCell<Vec<u8>>` statuses (one per op), and a `Cell<usize>` remaining.
3. For each op push a `WriteRequest` whose `WriteResponder` is
   `BatchPut { accum, idx }` into `partition_loop.pending` — an ATOMIC injection
   of N ops as a single mpsc message, giving the group-commit loop the fat arrival
   it needs to fill MAX_WRITE_BATCH=256.
4. Each op's Phase 3 calls `accum.record(idx, status)`; the LAST recorder
   (remaining==0) encodes `BatchPutResp` and fires the single outer oneshot.

Frozen-for-merge rejection returns `CODE_UNAVAILABLE` per-op via the same
accumulator.

### `MSG_BATCH_GET` (0x54) — server-batched Get

`BatchGetReq { part_id, region_epoch, keys: Vec<Vec<u8>> }` →
`BatchGetResp { items: Vec<BatchGetItem{ status, value }> }`. Server side
(`handle_batch_get`) runs INLINE on the ps-conn task (no partition_loop hop, same
as `handle_get`): decode once, brief `borrow()` to snapshot readers/state, loop
`get_value` per key, pack into one frame. NO zero-copy for the response
(rkyv-encoded, values inline). For ≥ 64 KiB reads where each value needs its own
dest, callers use the client `get_many_into` (per-key `MSG_GET_BULK` into
caller-owned dests, fan-out via `fan_out_collect`).

## Open-tail size probe for the cluster overview

`background_maintenance_loop` refreshes `PartitionMetrics.open_tail_bytes` — the
Σ committed length on the partition's log/row/meta OPEN-tail extents — which
`report_load_loop` ships in `PartitionLoad.open_tail_bytes`. The manager adds it
to its authoritative sealed-length sum for the overview `live_size` (an open
extent's manager `sealed_length` is 0, so an all-open-tail partition would render
0 B without this). The refresh:
- runs at loop top, throttled to every 30 s;
- is **DETACHED** (`compio::runtime::spawn(...).detach()`) and guarded by an
  `open_tail_probe_inflight` CAS — it does `commit_length` on each of 3 stream
  tails (up to 15 s worst case) and must never stall the shared GC/compaction task;
- stores the sum ONLY if all 3 probes succeed (a partial sum would misreport);
  keeps the prior value on any error; 0 until the first success;
- uses `StreamClient::open_tail_committed_len` (NOT `commit_length`), which
  returns 0 when the tail is SEALED — a sealed tail's length is already in the
  manager's sealed-length sum, so counting it here would double-count.

INVARIANT: the probe must stay off the maintenance task's critical path (detached
+ in-flight-guarded) — a blocking `commit_length` here would gate GC/compaction on
manager/replica latency. The sibling `size_bytes` gauge is DEAD (no writer); do
not confuse it with `open_tail_bytes`.

### `open_tail_dead_bytes` — WAL debt on the open tail

`PartitionMetrics.open_tail_dead_bytes` is the dead (overwritten/deleted)
large-value bytes on the OPEN (last) `log_stream` extent. `gc_debt_bytes` is
SEALED-only (GC can't punch an unsealed extent), so a log-heavy / all-open-tail
partition reports `gc_debt=0` while holding real garbage; this gauge exposes it.
`gc_debt_bytes + open_tail_dead_bytes` is the full reclaimable WAL debt (the two
are DISJOINT — no double-count). Shipped via `PartitionLoad.open_tail_dead_bytes`;
the manager sums `Σ(gc_debt + open_tail_dead)` into
`ClusterDfResp.logical_wal_debt` for `autumn-op df`. Load-bearing:
- DERIVED each GC tick via `open_tail_dead_bytes(discards, extent_ids)` (=
  `discards[extent_ids.last()]`) from already-persisted SST discard maps — not a
  bespoke counter, so it survives restart exactly like `gc_debt` with zero
  write-path cost.
- Refreshed BEFORE the `extent_ids.len() < 2` gate in BOTH the `Sel::GcRecv` and
  `Sel::GcTimeout` paths — the all-open-tail case has a SINGLE log extent and
  would skip the gate, so a gate-gated refresh would leave it 0 exactly where it
  matters most.
- The `<2`-extent early exits also store `gc_debt_bytes = 0` (no sealed prefix ⇒
  no debt) so a GC-reclaimed-to-one-extent partition doesn't leave stale
  sealed-debt inflating `logical_wal_debt`.

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

### SST on-demand paging + bounded block cache

`SstReader` does NOT keep SST bytes resident: production readers (flush / compact
/ recovery) are PAGED — only MetaBlock state stays in memory; data blocks are
fetched on demand from row_stream through the process-wide bounded `BlockCache`
(`--sst-block-cache-bytes`, default 512 MB, sampled-LRU; keys =
`(extent_id, abs_off)`; NO compaction invalidation needed — extent ids are never
reused, stale entries age out). Recovery opens SSTs from the META TAIL ONLY
(last-4-bytes → meta region) and the WAL replay streams in 64 MB chunked-carry
(`decode_records_chunk`), so restart RSS = replay-window bound, dataset-independent.

INVARIANT: the replay chunks MUST use
`StreamClient::read_committed_bytes_from_extent` (committed-clamped), never the
plain explicit-length read — a replica's file legitimately holds speculative bytes
past `sealed_length`/min-commit, so a plain read ingests un-committed bytes and the
next chunk trips `StaleVpOffset`, permanently wedging the partition open.

Invariants:
- Point lookups go through `lookup_in_sst_via` (async): callers SNAPSHOT
  `Arc<SstReader>`s + stream_client under the borrow, DROP it, await, re-borrow.
  `lookup_in_sst` (sync) serves resident readers only and ERRORS on paged ones —
  never silently misses.
- Iteration is ASYNC (`AsyncTableIterator` / `AsyncMergeIterator`, fetch blocks on
  demand). `FetchMode::Cached` (range — per-block via the global BlockCache) vs
  `FetchMode::Window(8MiB)` (do_compact / split unique_user_keys — sequential bulk
  windows BYPASSING the cache, scan-resistant; one RPC per window; peak read memory
  = inputs × one window). The async API returns Result — a block-read error ABORTS
  the compaction (never silently truncates merge output). Sync
  TableIterator/MergeIterator remain Resident-only (tests, builder round-trips).
- Diag seq_opt/fullscan report miss on paged readers (diagnostic-only).

### Large-VP client direct-read (`MSG_GET_REDIRECT`)

`handle_get_redirect` (`rpc_handlers.rs`): a read of a VP whose CLAMPED requested
length `r_len` is `>= 64 KiB` answers with a descriptor `GetRedirectResp {
extent_id, value_offset, value_len, eversion, replica_addrs }` instead of resolving
the bytes through this PS; the client (`ClusterClient::get_direct`) reads the range
straight from an EN (`read_extent_value_direct`, `MSG_READ_BYTES_BULK` zero-copy) and
falls back to the proxy `get` on ANY failure. Sub-range support:
redirect fires for any VP sub-range with `r_len = (req.length==0 ? vp.len-r_off :
min(req.length, vp.len-r_off)) >= 64 KiB`, returning `value_offset = vp.offset +
req.offset`, `value_len = r_len`. Single-key `get_direct` (0,0) is the
`r_off==0, r_len==vp.len` special case. Invariants:
- The GC writer-pin check runs BEFORE the redirect decision — an extent being
  punched surfaces NotFound exactly like the proxy path. The client's read window
  is deliberately unprotected: a GC punch in the gap is a failed EN read → proxy
  fallback (extents unlink whole + eversion fence ⇒ never a torn read); `_vp_pin`
  drops at return exactly as the whole-value path.
- EC-converted extents NEVER get a descriptor (`extent_read_descriptor` refuses;
  shard bytes ≠ value) — falls back to `handle_get`.
- Short reads under CODE_OK are FAILURES in `read_extent_value_direct` (same
  "got < need" rule as `read_value_from_log`).
- Inline values / small VPs / sub-64 KiB / offset past value end: inline in the
  response (`extent_id == 0`); `get_value` (non-redirect callers) never yields
  `GetOutcome::Redirect`. No wire-struct change → no WIRE bump.

### UCX end-to-end zero-copy read (`MSG_GET_BULK`)

The kvcache SDK's `get_into` issues `MSG_GET_BULK` so the value crosses
`EN → PS → client` with no FrameDecoder/encode copies (the client lands it in a
registered pool buffer, then one memcpy into the caller's dest). The seam is
`resolve_value`/`read_value_from_log` (`background.rs`) returning **`Bytes`**, not
`Vec<u8>`:
- **VP value (UCX + TCP)**: `read_value_from_log` calls
  `StreamClient::read_value_into_pooled`, which recvs the value straight into a
  `RegPool` `PooledBuf` (EN emits `[bulk head][value]` as 2 `Bytes`, value aliases
  the EN pread buffer — no encode copy). UCX recvs into a *registered* buffer
  (RDMA); TCP recvs via a compio owned read (`read_exact_into_pooled`) — only the
  kernel copy, no FrameDecoder copy. The PS hands the value onward as
  `Bytes::from_owner(pb)` (aliases the pool buffer; returns to the pool when that
  `Bytes` drops after the client write completes). Falls back to the
  `read_bytes_from_extent` copy path for EC / chunked / stale-eversion / length==0.
  No value crc is verified (integrity is the transport's job).
- **inline value**: `resolve_value` returns a zero-copy `raw_value.slice(..)`.
- **`handle_get_bulk`** returns the response as TWO segments `(head, value)`:
  `head = [header][ctrl_len][code+message][crc]` (v28, built by `ps_bulk_head` →
  `frame::encode_bulk_response_head`; the crc covers header+ctrl, the value is a
  raw tail), `value` is the aliasing `Bytes`. The ps-conn inflight FU output
  type is `(Bytes, Option<Bytes>)`; `push_resp` pushes `head` then `value` into
  `tx_bufs` so ONE `write_vectored_all` emits them as a single wire frame with no
  concat copy. On-the-wire bytes are identical to the concatenated form, so the
  client read path (`call_into_pooled`) is unchanged.

`handle_get` (rkyv `GetResp`, generic SDK) copies the value once (the rkyv encode
copies regardless). Net read-path value copies: VP-over-UCX `get_into` = **1**
(the client-side pool→dest memcpy; the PS/EN hops stay 0-copy — the
recv-into-caller-dest primitive that made it 0 was removed for cancel-safety +
timeout-ability, see autumn-rpc CLAUDE "Why pooled-only"). Cancel-safety of the
registered recv lives in the read_loop that OWNS the `PooledBuf` (returns it to
the pool on cancel).

### PS write-recv zero-copy (`MSG_PUT_BULK`, large values)

Symmetric on the WRITE recv side. `drain_bulk_writes` (`lib.rs`) runs in the ps-conn
read loop right after `decoder.feed`, BEFORE the normal decode: if the FRONT frame
is a `MSG_PUT_BULK` whose value is `>= AUTUMN_PS_BULK_RECV_MIN_BYTES` (64 KiB), it
recvs the value straight into a `PooledBuf` instead of letting `FrameDecoder`
accumulate (and copy) it. Mechanics:
- `peek_header` + `peek_payload` read the frame header and the
  `[part_id][..][key_len][key]` meta WITHOUT consuming, to locate the value
  boundary. Gated on `part_id == owner_part` and the size band
  `[64 KiB, AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT]`.
- Consume the header+meta+key, `drain_into` any buffered value prefix into the
  `PooledBuf`, recv the remainder (UCX `recv_into` registered / TCP
  `read_exact_into_pooled` owned). The V1 frame-crc trailer is consumed off the
  wire (stream alignment) but not validated — value integrity is the transport's
  job. Normal (non-bulk) frames keep their V1 frame-CRC.
- The value rides onward as `Bytes::from_owner(pb)` via a
  `PartitionRequest.bulk_value: Option<Bytes>` field; `payload` carries only
  `[meta][key]`. `enqueue_put_bulk` uses `bulk_value` directly when present. The PS→EN
  `append_batch` send is already rcache-zero-copy (UCX) / Arc-Bytes (TCP).
- Cancel-safe on both transports: `drain_bulk_writes` owns the `PooledBuf` across the
  recv (UCX `InflightSlot` drains the NIC on drop; TCP compio retains the owned
  buffer until the read CQE lands). The d=1 fast path is skipped when
  `drain_bulk_writes` queued a reply (`inflight.is_empty()` guard) to keep in-order
  replies.

Small writes (< 64 KiB) keep the unchanged FrameDecoder path — the only added cost
is one `peek_header` + size-check branch per frame. `drain_bulk_writes` is SKIPPED
when authz is ON (see authz section).

## Flush Pipeline

Triggered when `active` exceeds `FLUSH_MEM_BYTES` (256 MB).

```
P-log: background_flush_loop
  1. recv flush_rx signal
  2. snapshot front imm + vp + tables → FlushReq
  3. flush_req_tx.send(req)      ← cross-thread hand-off (capacity 1)
  4. oneshot resp.await

P-sst: flush_worker_loop
  1. recv FlushReq
  2. build_sst_bytes(imm, vp_eid, vp_off)         ← spawn_blocking (CPU)
  3. sst_sc.append(row_stream_id, sst_bytes)     ← 128 MB network upload
  4. SstReader::from_bytes(...)
  5. resp_tx.send(Ok((new_meta, reader)))

P-log: continuation
  6. part.tables.push(new_meta)
  7. part.sst_readers.push(Rc::new(reader))
  8. part.imm.pop_front()
  9. save_table_locs_raw(part_sc, meta_stream_id, part.tables.clone(), vp)
```

P-sst spawn failure is fatal-for-this-partition: `open_partition` returns Err and
the manager reschedules. There is no in-thread fallback (see row_stream
single-writer invariant).

After flush, `save_table_locs_raw` writes `TableLocations` to `meta_stream` and
**truncates meta_stream to 1 extent** — only the latest checkpoint is kept.

INVARIANT (checkpoint publication): only P-log may publish `metaStream`
checkpoints. P-sst may upload the SST and build the `SstReader`, but it must not
write `TableLocations` from the `FlushReq` snapshot. With
`AUTUMN_PS_SST_INFLIGHT_CAP > 1`, two in-flight flushes can complete out of order;
publishing from stale `tables_before` snapshots can drop older SSTs or emit
duplicate `(extent_id, offset)` entries. The authoritative checkpoint must be
emitted only after P-log merges `new_meta` into `part.tables`.

**vp snapshot semantics**: the meta_stream checkpoint records the vp
(`vp_extent_id/vp_offset`) captured at FlushReq send time on P-log — NOT the vp at
P-sst commit time. Correctness: during replay, logStream from the snapshot vp
forward re-inserts any records added after the snapshot (some may already be in the
just-flushed SST — duplicate entries with the same seq are idempotent). Trade-off:
avoids a second round trip; slightly more logStream retained until next flush.

## Compaction

Two modes, run in `background_maintenance_loop`. Public method:
`trigger_major_compact(part_id) -> Result<(), &'static str>` — enqueues via
`compact_tx` (capacity 1), non-blocking.

### Expiry-Triggered Major Compaction (automatic)
During each periodic tick, the loop checks all SST readers for
`min_expires_at > 0 && min_expires_at <= now`. If any SSTable contains expired
keys, a major compaction runs on all tables (drops expired entries + tombstones),
so TTL partitions eventually clean up without explicit triggers.

### Minor Compaction (periodic, 10–20s jitter)
`pickup_tables` selects tables via one of two strategies:
- **Head-extent**: if the oldest extent's tables are < 30% of total data
  (`HEAD_RATIO`), pick up to 5 (`COMPACT_N`) tables from it — clears old extents to
  enable `truncate` on `row_stream` (freeing disk/logStream extents).
- **Size-tiered**: sort tables by sequence, find consecutive "small" tables
  (< 32MB = `COMPACT_RATIO * MAX_SKIP_LIST`), pick up to `COMPACT_N`.

Runs `do_compact(major=false)`.

### Major Compaction (`compact_tx`, e.g. after overlap detected)
`do_compact(major=true)`: processes all tables, additionally drops tombstones
(op=2), expired entries, out-of-range keys (overlap cleanup), and clears
`has_overlap` on success.

### `do_compact` Logic (streaming)
```
  1. Read lock: collect SstReaders for selected tables, sort newest-first by last_seq
  2. Create MergeIterator over TableIterators
  3. Streaming merge loop (ONE in-progress SstBuilder + Vec<new_readers>):
       - Dedup: skip if same user_key already seen (newest wins)
       - Range filter: skip keys outside partition range
       - Discard tracking: when dropping VP entries, accumulate {extent_id → bytes}
       - Major filter: skip tombstones and expired entries
       - If current SstBuilder size > 2 × MAX_SKIP_LIST: finalize, append to
         row_stream, push (TableMeta, SstReader) into new_readers, start fresh
       - Otherwise SstBuilder.add(key, op, value, expires_at)
       - After loop: attach aggregated discards to final SstBuilder, finalize,
         append, push to new_readers
  4. Atomic swap: write lock → remove old SstReaders + tables → push new_readers
     → save_table_locs_raw to meta_stream (single linearization point; crash before
     this leaves new SSTs as orphan bytes and recovery loads the prior checkpoint)
  5. If truncate_id returned: truncate row_stream up to that extent
```
The merge never materializes all kept entries into an accumulator Vec — it streams
one builder at a time to keep peak compaction RAM ≈ inputs + one output builder.

### Compaction output vp_head = MAX(input vp_heads) by stream position

INVARIANT: `do_compact` stamps its output SSTs + meta checkpoint with the MAX over
the INPUT SSTs' vp_heads by STREAM POSITION (first-occurrence index into
`log_extent_ids`; extent_id order is non-monotonic post-CoW-split so a raw
`max(extent_id)` is wrong), NOT the live write cursor `p.vp_extent_id/vp_offset`.
Why: the live cursor points PAST acked-but-un-flushed writes in the active
memtable; a major compaction replaces the ENTIRE live SST set, so stamping all
outputs at the live cursor makes recovery start replay PAST the un-flushed tail →
silent data loss on crash. The merged SST holds every input's data up to the newest
input's `last_seq`, so recovery only needs the log AFTER the newest input's content
= the MAX. This ADVANCES the GC replay floor (reclaims the fully-merged log region)
while staying ≤ the live cursor → strictly safer. `log_extent_ids` is a hard `?`
(a swallowed fetch-failure → empty list → `(0,0)` → recovery no-replay = loss); the
abort is before any row_stream append so nothing is half-published.
`background::compaction_vp_head_tests`.

### Flush stamps the imm's ROTATION-time vp_head

INVARIANT: `rotate_active` captures `p.vp_*` at the FREEZE instant (the imm's true
content boundary) into `PartitionData.imm_vp_heads` (`RefCell<HashMap<usize,
(u64,u64)>>` keyed by `Arc::as_ptr`; INSERT at push, REMOVE at the single
`commit_flush_outcome` pop, kept on flush ERROR since the imm stays queued for
retry). The flush reads the imm's captured vp — NOT the live cursor — for both the
stamped SST/meta vp_head AND the `await_log_synced_to` durability barrier. Why:
background flush lags the writer; foreground writes landing between an imm's
`rotate_active` and its claim push the cursor forward, so a claim-time cursor is
AHEAD of that imm's own content → on crash before those writes flush, recovery
starts past them → silent loss. This is also the premise the compaction-MAX rests
on (each input's vp_head must be its TRUE content boundary). Test-only affordances:
`set_flush_mem_bytes`, `set_flush_test_pause`, `flush_commit_count`.

Companion: recovery seeds `p.vp` = the committed log TAIL (tracks each replayed
extent's committed end), kept SEPARATE from the replay start. Without it, an
idle-restarted partition's recovered active rotates with a BACKWARD vp_head → the GC
floor can't advance without a fresh write. SAFE because reads are committed-clamped
(the tail is ≥ every replayed record) and the recovered active is always flushed
into an SST containing its data. `crates/manager/tests/system_recovery_vp_seed.rs`.

## GC/compaction merged loop (`background_maintenance_loop`)

Compaction and GC run on ONE P-log task. A unified `select` waits on `compact_rx` /
`gc_rx` + two per-kind deadline timers; whichever fires runs to completion before
the next select, so compaction and GC are **structurally serialized** — never
concurrent. This is load-bearing for the GC replay-floor guard (GC never observes
`sst_readers` mid-compaction-publish). They share one `maintenance_gate` vs split;
each section acquires its PS-wide cap (`acquire_compact` / `acquire_gc`) inner to
the gate. Flush stays a separate loop — it only ADDS forward-vp_head SSTs, which
can't lower the replay floor, so it needs no serialization with GC.

### GC WAL replay-floor guard — never punch what recovery replays

INVARIANT: GC only punches NON-EMPTY log extents STRICTLY BEFORE the replay floor
(empty `sealed_length==0` extents are always eligible). Punching an extent that
crash recovery still replays from loses un-flushed writes (small inline values not
yet in any SST; or, if the vp_head extent itself is punched, `chosen_pos==MAX` → no
replay at all). Relocating live large VPs (seq+1) is fine; the danger is punching
the replay window.

`gc_replay_floor` / `gc_extent_punchable` (used for both Auto and Force): compute
`replay_floor_pos = min` stream-position (FIRST-occurrence index into
`log_extent_ids`, matching recovery's `first_pos_by_eid` for CoW-shared extents)
over the live SSTs' vp_heads. The floor == recovery's `chosen_pos` exactly; the
single-task merge makes the in-memory `sst_readers` it reads match the durable
checkpoint recovery loads. `floor=0` (protect all non-empty) when no vp_head
resolves.

**Raise the floor to the durably-ACKed flush checkpoint vp**
(`gc_floor_raise_to_durable_ckpt`, a `max`): the MIN-over-live-SST floor is
over-conservative (drags back to the OLDEST live SST's vp_head, so GC can't reclaim
the fully-covered `[MIN, newest-flush-vp)` region). After `gc_replay_floor`, raise
to the position of `PartitionData.durable_ckpt_vp` when it resolves.
`durable_ckpt_vp` is the vp_head of the newest FLUSH-published, **durably-ACKed**
meta_stream checkpoint of THIS incarnation — set ONLY in
`commit_flush_outcome_inner` AFTER `save_table_locs_raw` returns Ok, `(0,0)` until
then, NOT seeded from the recovered checkpoint at open. Every log record strictly
below a durable checkpoint vp is in that checkpoint's persisted SST set (or
compaction-dead), so `[MIN, durable-vp)` is safe to punch.

INVARIANT (why ack-gated, never in-memory): `commit_flush_outcome_inner` pushes the
new SST into `tables`/`sst_readers` BEFORE the checkpoint append acks. A floor
derived from in-memory state (`p.vp_*`, or a MAX over live `sst_readers` vp_heads)
could run AHEAD of what a crash-time recovery loads — GC punches `[V_old, V_new)`,
the process crashes before the `V_new` checkpoint acks, recovery loads the `V_old`
checkpoint (whose SSTs don't cover `[V_old, V_new)`) → silent loss. The ack-gated
`durable_ckpt_vp` is immune. `recover_partition` is deliberately NOT changed to
raise its replay start (the naive "MIN→MAX" deletes the `chosen_pos==MAX` no-replay
rescue): the recovery-read half self-resolves — once GC punches the covered prefix
those extents vanish from `log_extent_ids`, the punched SSTs' vp_heads become
unresolvable, recovery skips them, and `chosen_pos` lands at the first surviving
position ≥ the raised floor. `background::gc_replay_floor_tests`.

## GC (Garbage Collection)

Targets the **logStream** where large values (ValuePointers) are stored.

**Trigger**: periodic (30–60s jitter), via `gc_tx` (capacity 1), or via the
`Maintenance` gRPC RPC. Public methods:
- `trigger_gc(part_id)` — enqueue `GcTask::Auto(GcAutoParams::default())`
- `trigger_force_gc(part_id, extent_ids)` — enqueue `GcTask::Force { extent_ids }`

**Candidate selection** (Auto arm):
1. Candidates = all `sealed_extents` (`extent_ids[..len-1]`), sorted by reclaimable
   bytes desc (includes empty sealed extents that no SST ever referenced).
2. For each candidate, `get_extent_info(eid)`:
   - `sealed_length == 0` → push to holes (empty slot, no rewrite). `run_gc(eid, 0)`
     skips the read loop and goes straight to `flush_gc_batch` (no-op) + `punch_holes`.
   - Else multi-tier filter: skip if `empty_only`; skip if `max_size` set and
     `sealed_length > max_size`; effective `ratio = max(GC_DISCARD_RATIO,
     params.ratio)` (halved when stream total discard ≥ `stream_debt` high-water);
     push if `discard_bytes / sealed_length > effective_ratio`.
3. Cap at `MAX_GC_ONCE` (3) per dispatch.

INVARIANT (never punch on a stale/open `extent_info`): the destructive
`sealed_length == 0` fast-punch branch keys on `get_extent_info(eid)`, a CACHED read.
A now-sealed extent can linger in `extent_info_cache` as its pre-seal OPEN snapshot
(`sealed=false, sealed_length=0`); trusting that would punch a sealed extent full of
live ValuePointers as if empty → silent big-value loss. `authoritative_sealed_length`
(used by BOTH Auto and Force, carrying the validated `(eid, sealed_length)` to `run_gc`
so there is no check/use re-read split): `sealed` is IMMUTABLE once set, so a cached
`sealed=true` is trustworthy; a candidate that reads NOT sealed is SKIPPED — refusing to
GC anything not authoritatively known sealed (an OPEN extent reports `sealed_length==0`
but is NOT empty). The reclamation gap this could leave is closed at the SOURCE:
`StreamClient::alloc_new_extent` (seal-and-roll) EVICTS the OLD tail from
`extent_info_cache`, so GC's next `get_extent_info` fetches the authoritative sealed
state and reclaims it. We EVICT, not synthesize `sealed_length = seal_commit` locally
(the manager's `already_sealed` branch keeps the existing `L ≥ seal_commit`, so a
synthesized smaller value would make GC punch committed `[seal_commit, L)`).

INVARIANT (GC liveness is FULL VP identity, not extent_id): a scanned record is the
live version of its key ONLY if the current live VP matches `extent_id` AND `offset`
AND `len` — it points at THIS record's exact bytes. Comparing `vp.extent_id` alone
and relocating the *scanned* record's value drops a lost-update for a key with two
large versions in the SAME sealed extent (relocate old A, then scan of new B sees
the live version pointing elsewhere → skip → punch drops B). The scanned record's
absolute value offset = `buf_base_offset + record_start + val_off`, where `val_off`
comes from the single source of truth `wal_record::value_offset_in_record` (V1
`22+key_len` / V0 `17+key_len`). Recovery's VP reconstruction (`lib.rs`) uses the
SAME helper. `crates/manager/tests/system_gc_multiversion_same_extent.rs`.

INVARIANT (GC never relocates a key with an in-flight WAL write): the liveness
lookup sees only the memtable + SSTs, but the write pipeline is 3-phase — Phase 1
`start_write_batch` bumps `seq_number` + encodes the WAL record, Phase 2
`append_batch` makes it durable, Phase 3 `finish_write_batch` inserts it into the
memtable. A Put/Delete between Phase 1 and Phase 3 is seq-ASSIGNED but NOT yet in
the memtable → invisible to the lookup. Because Phase 1 already bumped
`seq_number`, GC's relocation seq (`seq_number += 1`) would be HIGHER and shadow
that write (silent lost-update / a Delete would resurrect the value). So the write
path maintains `PartitionData.inflight_write_keys` (a refcounted set of 64-bit
`inflight_key_hash(user_key)`, add in Phase 1 under the seq-assign borrow, release
in Phase 3 under the memtable-insert borrow — no await between insert and release,
so every key is always in EITHER the memtable OR the set), and `process_gc_chunk`,
after the VP-identity match and before the seq allocation, ABORTS the extent's GC
round (Err — no relocation, no punch; retried next tick) if the key is in the set.
Aborting (not skipping) is required: the in-flight write may never commit, so the
scanned value could stay live and must not be punched. A hash collision only costs
a spurious abort (safe). Cost is ~a couple HashMap ops + one FNV hash per key on
the hot write path. `crates/manager/tests/system_gc_inflight_wal_put.rs`.

Timing — the in-flight set and the post-await re-check are COMPLEMENTARY; neither
subsumes the other. A write moves through: Phase 1 (seq assigned + `set.add`) →
Phase 3 (memtable insert + `set.remove`) → later flushed into a new SST. GC's
verdict can land at any point, and the guard that catches it depends on where the
write is (`V_old@5` is the value GC scans; the racing Put is `K = V_new`):

```
Case A — Put still in the WAL pipeline → the in-flight SET fires:
 GC (background_maintenance_loop)                partition_loop (Put K = V_new)
  T1 lookup_in_sst_via(K).await ── yields ─────▶ Phase 1: seq 5→6; inflight_write_keys.add(hash K)
                                                 Phase 2: append (durable); Phase 3 PENDING
  T3 resume; re-check: appeared_in_mem = FALSE   (K not in the memtable yet)
     VP-identity: live VP == (E,O,L) → MATCH
  T4 in-flight guard: set.has(hash K) → YES
     ⇒ Err, ABORT the extent round               Phase 3: insert K@6=V_new; set.remove(hash K)
     (no seq alloc, no relocate, no punch; retried next tick)

Case B — Put finished during the await → the RE-CHECK fires:
  T1 lookup_in_sst_via(K).await ── yields ─────▶ Phase 1→2→3 all run: insert K@6=V_new
                                                 (set add THEN remove → now in memtable, not in set)
  T3 resume; re-check: appeared_in_mem = TRUE ⇒ continue
     retry: mem branch finds K@6=V_new; VP-identity: V_new ≠ scanned V_old → skip (V_old is dead)
```

INVARIANT (no gap): Phase 3 does `insert` then `set.remove` under ONE borrow with
NO await between them, so at every instant K is in EITHER the set (Phase 1→3) OR
the memtable (post-Phase 3) — never neither. A GC verdict landing before Phase 3
is caught by the set (Case A); one landing after is caught by the re-check
(Case B). Deleting the re-check re-opens Case B; the set alone can't (it's cleared
at Phase 3). The pre-fix bug was the missing Case A: at T4, GC did
`seq_number += 1 → 7` and relocated `V_old` at seq 7, shadowing `K@6 = V_new`.

**`run_gc` for one extent (streaming)**:
```
  0. Multi-frag rewrite pre-pass
     (rewrite_multi_frag_for_extent): walk active memtable + imm queue for
     OP_VALUE_POINTER_MULTI entries whose mfvp has any fragment on `eid`. Dedup by
     user_key (newest seq wins; SST-only mfvps deferred to compaction's discard
     path). For each candidate: read every fragment via read_bytes_from_extent
     (full-value rewrite), append each as a fresh OP_CHUNK_BLOB record, build new
     MultiFragVp, allocate seq + append OP_VALUE_POINTER_MULTI|1 commit record,
     insert memtable entry. A foreground Put on the same user_key wins via a strictly
     newer seq (single-threaded P-log); rewrite chunks become orphan, reclaimed next
     GC pass. Without this, OP_CHUNK_BLOB records of live multi-frag values would be
     silently orphaned by punch_holes (the single-VP scan skips OP_CHUNK_BLOB).
  1. Single-VP loop (streaming): until cur >= sealed_length:
       a. read_bytes_from_extent(eid, cur, AUTUMN_PS_GC_READ_CHUNK_BYTES)
       b. concatenate carry + chunk → buf
       c. process_gc_chunk(buf):
          - decode complete records left-to-right; on partial record at tail stop,
            save buf[consumed..] as carry for the next chunk
          - per record (single-VP, in_range): lookup current live version
            (active → imm → SSTables); if live VP still points to (eid, offset, len):
            re-write value via stream_client.append, DROP borrow_mut BEFORE awaiting
            the RPC, re-acquire to insert the updated VP
          - OP_CHUNK_BLOB skipped (handled by the pre-pass); OP_VALUE_POINTER_MULTI
            commit records skipped (tiny; pre-pass covers their semantic content)
       d. cur += chunk.len()
  2. carry must be empty at end (sealed records are byte-aligned); non-empty carry →
     refuse to punch and return error
  3. punch_holes([eid]) on log_stream → manager decrements refs; extent physically
     freed when refs → 0 across all CoW-shared streams
```

**Discard map**: each SSTable's MetaBlock holds `HashMap<extent_id,
reclaimable_bytes>`. During compaction, dropping a VP entry (dedup/range/tombstone/
expiry) adds its extent_id + value length to the map. The GC loop aggregates across
all SstReaders.

**Discard snapshot RPC** (`MSG_GET_DISCARDS = 0x48`): `handle_get_discards` reads a
live snapshot of the partition's discard map without manager state — snapshots
`sst_readers` (no await while borrowed), calls `background::get_discards(&readers)`,
fetches `log_stream extent_ids`, filters via `valid_discard` (drops already-punched
extents), returns `(extent_id, reclaimable_bytes)` pairs. Used by `autumn-client
info`.

**Multi-tier params** (`GcTask::Auto(GcAutoParams)`): `ratio: Option<f64>` (default
0.4), `max_size: Option<u64>`, `stream_debt: Option<u64>` (halve ratio when total
reclaimable ≥ threshold), `empty_only: bool`. External controllers compose tiers by
issuing multiple back-to-back dispatches; the PS executes exactly the params each
dispatch carries. `MaintenanceReq` carries `gc_ratio` / `gc_max_size` /
`gc_stream_debt` / `gc_empty_only`.

**Cooldown classification** (`classify_gc_failure_cooldown`,
`gc_failure_cooldown: HashMap<u64, (Instant, Duration)>`): soft window (30 s) when
the failure chain contains `"precondition failed"` (manager refuses `punch_holes`
while `ec_conversion_inflight`) or `"eversion mismatch"` (stale `extent_info_cache`
after an EC bump); hard window (300 s) for everything else.

**Tunable**: `AUTUMN_PS_GC_READ_CHUNK_BYTES` (default 64 MiB) — chunk size for the
streaming read inside `run_gc`.

## Admission: rate limiting + concurrency control

Background-IO admission is split into two orthogonal mechanisms mirroring the
physical resource they protect: **rates** are per-partition (IO patterns are
partition-local); **concurrency** is PS-wide (the protected resource is process RAM
— each compact/GC operation holds hundreds of MB of buffers).

| Type | Scope | Controls | Defaults | Code |
|------|-------|----------|----------|------|
| `RateController` | per-partition | fg bytes/s + fg iops + compact bytes/s + gc bytes/s | fg 1 GiB/s + 30K iops; compact 256 MiB/s; gc 128 MiB/s | `lib.rs` |
| `ConcurrencyController` | PS-wide (`Arc<>`) | compact + gc concurrency permits | compact_max=4, gc_max=4 | `lib.rs` |

`PartitionServer` carries one `Arc<ConcurrencyController>`; each `PartitionData`
carries its own fresh `Arc<RateController>` plus a clone of the PS-wide concurrency
Arc.

### RateController — per-partition rate caps

Four independent rate dimensions in one 1-second lazy fixed-window token bucket:

```rust
struct RateState {
    window_start: Instant,    // updated only when elapsed >= 1 s
    fg_bytes: u64, fg_ops: u64, compact_bytes: u64, gc_bytes: u64,
}
```

Public methods (all async, all sleep OUTSIDE the lock):

| Method | Semantics |
|--------|-----------|
| `account_fg(bytes, ops)` | fg write hot path. EITHER bytes OR ops cap reached → sleep (larger wins). Catches both 8 MiB-Put bytes-bound and 4 KiB-Put IOPS-bound workloads. |
| `account_compact(bytes)` | compact write. Sleeps until BOTH own compact rate AND fg-aware-yield allow. |
| `account_gc(bytes)` | gc write. Symmetric to compact, separate counter. |

**fg-aware yield** (inside `account_compact`/`account_gc`): yields the remainder of
the 1-s window when `fg_observed_bytes_rate > 0.8 × fg_rate` OR `fg_observed_iops >
0.8 × fg_iops` (disabled per-dimension when that fg cap is 0). Each partition checks
its OWN fg counters — a cold partition's compact doesn't yield to a hot partition's
fg pressure. INVARIANT: compact and gc counters are INDEPENDENT (saturating compact
does not throttle gc, and vice versa), so operators can budget them separately
(compact is bulk/burst; gc is sustained). Per-partition also aligns with
thread-per-core — each partition has its own io_uring and admission state on no
shared Mutex.

The lock guards synchronous accounting only; the sleep happens OUTSIDE (holding a
non-async mutex across `.await` would deadlock the compio runtime — same thread =
recursive lock = futex_wait on self):
```rust
let sleep_for = { let mut s = self.state.lock();
    Self::maybe_reset_window(&mut s); s.<dim>_bytes += bytes; compute_sleep(&mut s) };
if let Some(d) = sleep_for { compio::time::sleep(d).await; }
```
Trade-off vs continuous-refill: at the 1-s boundary you can burst 2× rate over a
~2 ms window — a feature for our 256 MB compact chunks / 4 MiB GC batches (a chunk
never gets sliced across windows).

### ConcurrencyController — PS-wide RAM cap

Each `do_compact` holds ~2× SST bytes; each `run_gc` holds ~64 MiB chunk-read +
rewrite staging. Without a global cap, `autumn-op compact ALL` would launch N
concurrent compactions and multiply peak RSS by N.

```rust
pub struct ConcurrencyController {
    compact_max: usize, gc_max: usize,
    compact_inflight: AtomicUsize, gc_inflight: AtomicUsize,
}
```
`acquire_compact() -> CompactPermit` / `acquire_gc() -> GcPermit` — atomic counters
+ 50 ms backoff loop; drop decrements. Atomic CAS (not a Mutex) is right here
because acquisitions are inherently cross-thread (multiple partition threads share
the same Arc).

### Call sites

| Path | Call |
|------|------|
| `start_write_batch` (fg hot path) | `rate_ctrl.account_fg(bytes, ops)` |
| `do_compact` (chunk + final emit) | `rate_ctrl.account_compact(chunk_bytes)` |
| `flush_gc_batch` (write side) | `rate_ctrl.account_gc(batch_bytes)` |
| `run_gc` (chunk read side) | `rate_ctrl.account_gc(chunk_len)` |
| `background_maintenance_loop` compaction section | `maintenance_gate.acquire()` then `concurrency_ctrl.acquire_compact()` |
| `background_maintenance_loop` GC section | `maintenance_gate.acquire()` then `concurrency_ctrl.acquire_gc()` |
| `handle_split_part` | `maintenance_gate.acquire()` + `concurrency_ctrl.acquire_compact()` |

A per-partition `GcRateLimiter` survives as a deprecated inner cap layered before
`account_gc`, kept for the `--gc-rate-bytes-per-sec` flag (distinct from
`--admission-gc-rate-bytes-per-sec`).

### Configuration (CLI flags + env)

| Flag | Env | Default | Knob |
|------|-----|---------|------|
| `--fg-rate-bytes-per-sec` | `AUTUMN_PS_FG_RATE_BYTES_PER_SEC` | **1 GiB/s** | per-partition fg bytes |
| `--fg-iops-per-sec` | `AUTUMN_PS_FG_IOPS_PER_SEC` | 30_000 | per-partition fg ops |
| `--admission-compact-rate-bytes-per-sec` | `AUTUMN_PS_ADMISSION_COMPACT_RATE_BYTES_PER_SEC` | **256 MiB/s** | per-partition compact bytes |
| `--admission-gc-rate-bytes-per-sec` | `AUTUMN_PS_ADMISSION_GC_RATE_BYTES_PER_SEC` | **128 MiB/s** | per-partition gc bytes |
| `--fg-saturated-threshold` | `AUTUMN_PS_FG_SATURATED_THRESHOLD` | 0.8 | fg-aware yield trigger |
| `--major-compact-parallelism` | `AUTUMN_PS_MAJOR_COMPACT_PARALLELISM` | **4** | PS-wide compact concurrency (`compact_max`) |
| `--gc-parallelism` | `AUTUMN_PS_GC_PARALLELISM` | **4** | PS-wide gc concurrency (`gc_max`) |
| `--max-extent-size-bytes` | — | **16 GiB** | per-extent seal threshold to each partition's `StreamClient` (clamp [1 GiB, 64 GiB]) |

`0` on any rate flag = unlimited for that dimension (per-dimension opt-out).

**`--max-extent-size-bytes`**: the threshold at which a stream's tail rolls a fresh
extent. Enabled by widening every extent byte position on the read+append path from
`u32` to `u64` (wire `AppendReq.commit` / `AppendResp.offset/end` /
`ReadBytesReq.offset/length` / `CommitLengthResp.length`; persisted `ValuePointer`
16→24 B, `SstLocation`, `TableLocations.vp_offset`, SST `MetaBlock.vp_offset`,
`TableMeta.offset/len`). Bigger extents = fewer extents = less manager/etcd metadata
pressure. Flows through `set_max_extent_size_bytes` → `max_extent_size_bytes()`, read
at both `StreamClient::new_with_owner_epoch` sites (P-log + P-sst). With EC + 16 GiB
extents a shard exceeds 4 GiB, so `read_shard_from_addr` chunks internally to keep
each `MSG_READ_BYTES` under the frame's `payload_len: u32` ceiling.

## Partition Split

`handle_split_part` runs inline on `partition_loop` (the P-log task) via
`dispatch_partition_rpc`, so all partition-state mutations are single-writer on the
partition thread.

```
handle_split_part(req):
  1. Reject if part.has_overlap == 1 (run major compaction first)
  2. Fetch authoritative range from manager via MSG_GET_REGIONS — PS-local part.rg
       is set at open and NOT refreshed for already-open partitions, so after a
       previous split it still spans the pre-split wide range; picking mid_key
       against the stale rg yields keys the manager's narrowed range rejects.
  3. mid_key SELECTION — two sources:
       EXPLICIT (req.at_key = Some(key)): validate key STRICTLY inside auth_rg
         (start, end) (== start / == end / out-of-range → InvalidArgument), use
         verbatim. SKIPS the SST scan AND the `>= 2 keys` gate — an empty / near-
         empty partition can be split (presplit primitive). at_key is a RAW byte
         string; the PS is namespace-agnostic (CLI assembles the prefix).
       MEDIAN (req.at_key = None): user_keys = unique_user_keys(part).filter(
         in_range(auth_rg)) (sorted, dedup, tombstone-/expired-filtered; auth-rg
         filter drops CoW-shared SSTable keys spanning the old wide range)
  4. MEDIAN only: if user_keys.len() < 2 → FailedPrecondition
  5. flush_memtable_locked(part): rotate active + flush all imm via P-sst
  6. mid_key = user_keys[len/2] (MEDIAN) or req.at_key (EXPLICIT)
  7. commit_length on each of {log, row, meta} stream
  8. multi_modify_split(mid_key, part_id, sealed_lengths) on manager
       (up to 8 retries, backoff 100ms → 2s)
  8b. Row-stream invalidate BARRIER to P-sst (see below), INSIDE the critical
       section BEFORE the manager seal; await the ACK. Then invalidate the log +
       meta stream workers (part_sc.invalidate_stream) — the manager sealed the old
       tails; without invalidation stale workers keep appending past sealed_length
       and recovery misses that data.
  9. Narrow PS-local part.rg to [auth_rg.start, mid_key), re-evaluate has_overlap
       (each sst_reader's smallest/biggest vs new rg), bump p.region_epoch locally,
       publish the new tuple to opened_with_shared (see region_epoch section).
```

After split, both children's on-disk SSTables still span the pre-split range (via
CoW-shared extents), so the left (source) immediately observes `has_overlap = 1` and
refuses subsequent splits until major compaction drops the out-of-range keys. The
right (new) partition is opened by `sync_regions_once`, where `open_partition`
evaluates overlap against its authoritative range and likewise sets `has_overlap = 1`.

**Split serialisation (single `maintenance_gate`):** `handle_split_part` acquires
the per-partition `maintenance_gate` (a `CompactionGate::new(1)`, max=1, held by
`background_maintenance_loop` around BOTH its compaction and GC sections) before
`flush_memtable_locked` / `commit_length`. Holding it guarantees neither a
`do_compact` (`compact_row_append` racing the row_stream seal) NOR a `run_gc`
(log_stream append racing the log seal) is in flight while split seals — the merged
loop runs only one at a time. RAII-held through `multi_modify_split` + the P-sst
barrier ACK, then `acquire_compact` (PS-wide RAM cap) is acquired inner. **Lock
order**: compaction/split are gate-first (`maintenance_gate → acquire_compact`); GC
is **permit-first** (`acquire_gc → maintenance_gate`) so a GC queued on the global gc
permit doesn't hold the gate and block a same-partition split. Acyclic because GC
uses `acquire_gc` (GC-exclusive) and split/compaction use `acquire_compact`
(`acquire_gc → maintenance_gate → acquire_compact` has no back-edge). Compaction MUST
stay gate-first to match split, else `acquire_compact ↔ maintenance_gate` cycles.

## Crash Recovery (`open_partition`)

```
  0. Check commit_length on all 3 streams (log/row/meta) — infinite retry, 5s backoff
       (ensures the last extent has consistent commit length across replicas)
  1. Read last TableLocations checkpoint from metaStream (iterate extents backward,
     find first non-empty)
  2. For each location: read SST bytes from rowStream (META TAIL only), open SstReader
  3. Compute max seq_number + VP head from SSTables
  4. Replay logStream from VP head forward (committed-clamped, 64 MB chunked-carry):
       decode WAL records, re-insert into recovered memtable (active); large values
       (>4KB) via VP; records with ts ≤ max_seq (already in SSTables) skipped
  5. PartitionData.active = recovered memtable (preserves unflushed entries)
  6. Log final state (`open_partition: ready` with tables/sst_readers/has_overlap/
     max_seq/vp_extent_id/vp_offset)
  7. Spawn P-sst OS thread (flush_worker_loop on own compio runtime)
  8. Spawn P-log background tasks on this thread (maintenance loop, flush loop,
     accept loop, dispatch)
```

The logStream replay + GC both read via `StreamClient::read_bytes_from_extent`, which
chunks internally so a >2 GiB sealed extent never EINVALs.

INVARIANT (concurrent opens): `sync_regions_once` fans out the opens via
`futures::stream::iter(to_open).buffer_unordered(OPEN_PARALLELISM=64)` so a PS
inheriting N partitions (full takeover) doesn't pay ~N× the single-partition recovery
wallclock (each recovery runs on its own thread/core and is latency-bound). Race-free
because port ordinals are reserved synchronously per open
(`used_port_ords.borrow_mut()` before any await) and owner locks are per-partition
keys, so no cross-open shared-RefCell borrow is held across an await. Gating
(already-open / backoff / budget, with a LOCAL budget reservation counter) is applied
UP FRONT; results (insert handle + budget inc + backoff record) after `collect`.
`OPEN_PARALLELISM` caps only the manager-RPC burst; the live count is bounded by the
per-PS partition budget (cpuset_len / 2).

Corrupt historical checkpoints are repaired out of band (a one-off `repair_metastream`
that appends a fresh `TableLocations` record); `recover_partition` stays strict and
authoritative — do not add silent normalization to the reopen path.

## Fault Recovery: LockedByOther Self-Eviction

If `partition_loop` receives `CODE_LOCKED_BY_OTHER` from the stream layer (a newer
partition owner took the lock), it sets `locked_by_other`; the loop checks it each
request and exits, preventing split-brain. The classifier
`background::is_locked_by_other` matches the "LockedByOther" substring across the
WHOLE anyhow chain (`{e:#}`, not plain `{e}` which shows only the outermost
`.context`). It fires for BOTH fence layers: the EN's native `CODE_LOCKED_BY_OTHER`
AND the stream client's typed `ManagerError` fence for a manager-side
`ensure_owner_epoch` rejection (stale `owner_epoch`). INVARIANT: the poison → reopen
→ fresh-epoch re-acquire path is the ONLY sanctioned recovery — never re-acquire a
stale epoch in place.

**Liveness closure**: the poison makes `partition_loop` break → `partition_thread_main`
returns → the runtime drops (accept loop, ps-conn tasks, flush loop). But the
`PartitionHandle` would SURVIVE in `self.partitions`, so with an unchanged `(rg,
streams, region_epoch)` tuple `sync_regions_once`'s `contains_key` skips the reopen
forever (zombie: safe fencing but not live). So `sync_regions_once` DROPS any handle
whose partition thread `is_finished()` (one atomic load per partition per ~2 s tick),
making the manager's region map the arbiter that same tick:
- still assigned here → reopen → `acquire_partition_owner_epoch` gets a FRESH epoch →
  partition resumes;
- moved away (rebalance) → not in `wanted` → stays closed. Also self-heals a crashed
  partition thread.

`PartitionServer.shutting_down` (set first thing in `shutdown()`) makes
`sync_regions_once` a no-op so the dead-thread reopen can't resurrect partitions
drained for process exit.

## Heartbeat must outlive `sync_regions_once`

`finish_connect` spawns `heartbeat_loop` immediately after `register_ps` succeeds,
BEFORE the (potentially 10+ s with hundreds of MiB unflushed WAL) `sync_regions_once`
— else the first heartbeat lands AFTER the manager's `PS_DEAD_TIMEOUT` (10 s) evicts
the PS, leaving every region's `ps_addr` permanently `unknown`. `heartbeat_loop` also
decodes the manager `CodeResp`: on `CODE_NOT_FOUND` (unknown `ps_id`) it WARN-logs and
re-runs `register_ps` + `sync_regions_once`, so a transient eviction self-heals.

## Data-plane authz enforcement (`authz.rs`)

The PS is the KV-layer enforcement point for multi-tenant `mem/` isolation. It holds
ONLY the manager's PUBLIC keys (fetched via `MSG_GET_AUTHZ_CONFIG`, cached in
`PartitionServer.authz: Arc<AuthzState>`, refreshed by `authz_config_poll_loop`, 5 s).
It verifies a capability token ONCE per connection and enforces a byte prefix + `exp`
check per request — it NEVER calls the manager to enforce.

**OPT-IN.** `AuthzState.is_enabled()` is a single relaxed `AtomicBool` load; false (no
signing key configured cluster-wide) ⇒ the whole gate is skipped, so fuse / kvcache /
dev pay nothing. `enabled` flips true only after the config poll installs a keyring.

INVARIANT — **ONE choke point: `authz_gate`, at the TOP of every frame dispatch,
BEFORE routing.** Called from exactly two places — `push_one_frame_to_inflight` (the
canonical dispatch; also covers `push_frames_to_inflight` + the idle-branch direct
pushes) and `d1_fast_path_round_trip` (the d=1 inline path). It:
- handles `MSG_AUTH_HELLO`: `verify_auth_hello` (sig + `aud == cluster_id` +
  `nbf`/`exp`) binds the per-connection `principal: Option<BoundPrincipal>`. When
  authz is OFF, AUTH_HELLO is a no-op OK so an authz-aware client still works against
  a non-authz PS.
- else runs `authz_check(msg_type, payload, principal, …)`: per the request's user
  key(s), `check_key` (protected prefix ⇒ require an unexpired token whose
  `allowed_prefixes` covers the key; kid still in the live keyring) / `check_range`
  (whole scan interval ⊆ one allowed prefix). Reject ⇒ a `PermissionDenied` frame is
  emitted and the frame NEVER reaches serve/delegate.

**`drain_bulk_writes` is SKIPPED when authz is ON** (`!gate_active()`) — the bulk
write-recv fast path bypasses `authz_gate`, so with authz on a large `MSG_PUT_BULK` is
left to the normal `FrameDecoder` path where `push_one_frame_to_inflight`'s gate
enforces uniformly (one value copy — acceptable; large writes are rare on `mem/`).
Never re-enable it under authz without moving the key check into it.

**Two load-bearing INVARIANTS (breaking either = silent cross-tenant exposure):**
1. Any new frame-dispatch path (a new local-serve fast path, a new inline handler off
   the ps-conn task) MUST route through `authz_gate` before serving — the gate is the
   only enforcement point.
2. Any new client data-plane msg_type carrying a USER KEY MUST get an arm in
   `authz_check` that extracts the key and calls `check_key`/`check_range`. The
   catch-all `_ => None` admits ungated (correct only for non-keyed admin ops).

Wire: `MSG_AUTH_HELLO` (0x55) + `AuthHelloReq/Resp`; `StatusCode::PermissionDenied`
(7). SDK auto-mints/renews the token and AUTH_HELLOs each PS connection
(`ClusterClient::set_tenant_credential`); `AutumnError::PermissionDenied` is terminal
(not retried).

### Layer-A — put must target a REGISTERED namespace

`authz_gate` runs TWO checks. The `AuthzState::gate_active()` atomic (`is_enabled() ||
layer_a_enabled()`) is ONLY a lock-free fast-path SKIP HINT; the per-request DECISION
is derived from the CONSISTENT snapshot (`!snap.namespaces.is_empty()` for Layer-A,
`snap.enabled` for Layer-B) so a config-refresh window can't pair a stale flag with a
different-time config. A stale hint can only cause a one-request fail-open on the
turn-ON edge, never a false reject.
- **Layer-A** (`check_layer_a`): a **put-class** frame (`MSG_PUT`/`MSG_PUT_BULK`/
  `MSG_BATCH_PUT`) whose key falls under NO registered namespace prefix
  (`AuthzInner.namespaces`, from `GetAuthzConfigResp.namespaces`) is rejected with
  `StatusCode::NamespaceUnknown` (8). TOKEN-FREE — pure `starts_with` against the
  registry; anonymous connections checked too. Gated by `layer_a_enabled` = *registry
  non-empty*, INDEPENDENT of the signing key. delete/get/range are NOT Layer-A gated
  (Layer-A only prevents creating UNOWNED data via writes; reads/deletes are
  Layer-B's job).
- **Layer-B** (`authz_check`) runs after, gated by `is_enabled`.
- **`drain_bulk_writes` gate** is skipped when `!gate_active()` so a large `MSG_PUT_BULK`
  flows through the FrameDecoder path where the gate enforces BOTH layers.
- Deploy note: builtin namespaces (`fs/`,`kvc/`,`mem/`) are CAS-registered on the
  first leader of any etcd-backed cluster → registry non-empty → Layer-A ON. So raw
  `0x01`–`0x04` keys (pre-namespace fuse via `connect_raw`) would be rejected with
  `NamespaceUnknown` — Layer-A must only ship alongside the namespace-key migration
  that makes fuse write `fs/…`. In-process test managers run memory-mode and never
  seed builtins, so Layer-A is OFF in the suite unless a test `namespace-create`s.

## region_epoch check (TiKV-style)

Each `PartitionData` carries `region_epoch: u64`, populated at open from
`MgrRegionInfo.region_epoch` (manager bumps on every `rg` rewrite — split / merge).
Hot-path handlers compare the request's stamped `region_epoch` against
`p.region_epoch`; mismatch returns `StatusCode::FailedPrecondition` so the SDK's
`Err`-arm refresh path engages. Check sites: `handle_get` / `handle_head` (before
in_range), `handle_range` (at top — **load-bearing**: without it a stale-epoch range
silently filters out-of-range keys and returns a partial `Ok(RangeResp)` the SDK
can't detect), `enqueue_put` / `enqueue_delete` / `enqueue_stream_put`
(`handle_incoming_req` snapshots `(region_epoch, part_id)` under one `borrow()`).
`0` on the wire = "skip check" (tests/bench/legacy); production callers always stamp
non-zero from `ClusterClient.lookup_epoch_for_part`.

`RangeResp.cur_end_key` carries `p.rg.end_key` as a ResumeSpan cursor; the SDK uses
it to advance across partition boundaries within one `range()` call, so a split
mid-scan auto-resolves. `PartitionHandle.opened_with` is `(rg, log/row/meta,
region_epoch)` so `sync_regions_once`'s drop+reopen check catches an epoch bump even
if rg byte-matches.

INVARIANT: `handle_split_part` MUST bump `p.region_epoch` locally (in-place) AND
publish the new tuple to `opened_with_shared`. The manager bumps the region epoch in
lock-step with rg rewrites, but the partition thread can't see that until the next
`sync_regions_once` drops+reopens (seconds, expensive) unless it updates its own copy.
- Without the in-place bump: a `range()` in the gap routes to a partition whose
  `p.region_epoch` still matches the SDK's stale cached epoch — the check passes,
  `handle_range` filters against the just-narrowed `part_rg`, returns left-only
  entries with `cur_end_key = mid`, and the SDK loops back to the same partition → the
  user sees an empty list. (SDK also falls back to `refresh_regions` + retry on the
  cursor-non-advance trip as a second defensive layer.)
- Without publishing to `opened_with_shared`: the next `region_sync_loop` tick sees
  the stale snapshot `!= latest` and drops+reopens the SOURCE partition (full
  `recover_partition` + a new port) even though its in-memory state is already
  correct → a 5–60+ s outage on every split. `PartitionHandle.opened_with` is
  `Arc<parking_lot::Mutex<(Range, u64, u64, u64, u64)>>` shared with
  `PartitionData.opened_with_shared`; `handle_split_part` writes the post-split tuple.

Only the **right child** still has an inherent open window (a brand-new partition
opened from scratch on its assigned PS, ~2 s tick + `open_partition`). The SDK
absorbs it via TiKV-style retry (`MAX_PS_REFRESHES = 10`, base 100 ms, cap 2000 ms,
cumulative ~9 s), deliberately NOT large enough to mask multi-minute PS-side bugs of
this shape. INVARIANT: any future in-place updater of `PartitionData.rg` /
`region_epoch` (e.g. a merge-survivor widening) MUST follow the same pattern (update
local fields → release `borrow_mut` → fresh `borrow` + lock `opened_with_shared` →
write the new tuple), else the partition gets needlessly torn down next tick.

## SSTable Format

### File Layout
```
[Block 0][Block 1]...[Block N][MetaBlock bytes][meta_len: u32 LE]
```
The last 4 bytes are `meta_len` — used by `SstReader::open` to locate the MetaBlock.

### Block Layout (64KB target, max 1000 entries)
```
[Entry 0]...[Entry N][entry_offsets: N×4B LE][num_entries: 4B LE][crc32c: 4B LE]
```

### Entry Layout (prefix-compressed)
```
[EntryHeader: 4B = overlap:u16 LE + diff_len:u16 LE][diff_key][op:1B][val_len:4B LE][expires_at:8B LE][value]
```
`overlap` = bytes shared with the block's **base key** (first key of the block, stored
in the MetaBlock index). Only the diff suffix is stored (prefix compression).

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
Double hashing with xxh3: `h1 = xxh3_64(user_key)`, `h2 =
xxh3_64_with_seed(user_key, SEED)`, `hash_i = (h1 + i * h2) mod num_bits`. Operates
on **user keys only** (8-byte MVCC suffix stripped before hashing). 1% target FPR,
initial capacity 512 keys. Encoding: `[num_bits:4B LE][num_hashes:4B LE][bits...]`.

### Iterators
- `BlockIterator`: scan entries within one decoded block; `seek` via binary search
  over entry offsets.
- `TableIterator`: spans all blocks; advances to next block when current exhausted.
- `MergeIterator`: N-way merge of TableIterators; for duplicate internal keys, the
  lower-index (newer) iterator wins; `next()` advances ALL iterators at the current
  minimum key.
- `MemtableIterator`: snapshot of memtable entries as sorted Vec; `partition_point`
  for seek.

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
| `MAX_IMM_DEPTH` | 4 | imm queue cap; merged_loop stalls req intake when reached (RocksDB `max_write_buffer_number`). Env `AUTUMN_PS_MAX_IMM_DEPTH` ([1, 64]). |
| `MAX_WAL_GAP` | 1 GiB | force-rotate active when `active.log_bytes() + Σ imm.log_bytes()` exceeds this. Measures the un-flushed LOG bytes (value included), NOT `mem_bytes()`. RocksDB `max_total_wal_size`. Env `AUTUMN_PS_MAX_WAL_GAP` ([128 MiB, 64 GiB]). |
| `SHUTDOWN_TIMEOUT_MS` | 60_000 | per-partition graceful drain deadline before SIGKILL fallback. Env `AUTUMN_PS_SHUTDOWN_TIMEOUT_MS` ([1_000, 600_000]). |
| `MAX_SST_BEFORE_AUTO_COMPACT` | 32 | defensive: the compact loop's timeout arm auto-triggers a minor compaction when `sst_readers.len()` exceeds this (bounds bloom-FPR runaway: 1% per-SST × 32 ≈ 28% cumulative). Not env-tunable. |

## Bounded recovery replay

Three fixes bound the restart replay window (worst case per partition =
`MAX_IMM_DEPTH * FLUSH_MEM_BYTES + active.bytes` = 1.25 GB):

1. **imm depth cap + back-pressure.** `partition_loop` reads `imm_full = part.imm.len()
   >= MAX_IMM_DEPTH` at top of loop. When full it skips both batch launches (B) and
   `req_rx.next()` (D), only polling `inflight.next()` and `imm_drained_rx`.
   `flush_one_imm` signals `imm_drained_tx` after each successful `imm.pop_front()` so
   the loop wakes and resumes intake.

2. **WAL-gap forced rotate.** After each `partition_loop` iteration, if `gap =
   active.log_bytes() + Σ imm[i].log_bytes() > MAX_WAL_GAP` AND `imm.len() <
   MAX_IMM_DEPTH`, call `rotate_active`. INVARIANT: the gap MUST be the un-flushed LOG
   bytes, not the memtable footprint — for a large-value (VP) workload the memtable
   holds only the ~24-byte ValuePointer while the 8 MB value lives in log_stream, so a
   `mem_bytes` gap never trips → active never force-rotates → the un-flushed replay
   window grows O(dataset). `Memtable.log_bytes: AtomicU64` tracks the actual appended
   log bytes (value included), incremented in THREE places — group-commit Phase 3
   (`finish_write_batch`), the GC multi-frag rewrite, and recovery-replay — travels
   with the memtable through `rotate_active` into imm, and disappears from the sum when
   the imm flushes (durable in an SST → no replay needed). A fresh active starts at 0.

3. **Graceful shutdown.** `PartitionServer::shutdown()` sends a `oneshot::Sender<()>`
   per partition through `drain_tx`. `partition_loop` picks it up via select, sets
   `drain_ack`, exits the main loop, runs the tail-drain (in-flight + pending), rotates
   `active`, loops `flush_one_imm` until imm empties, replies on the oneshot, exits.
   `serve_until_shutdown(addr, shutdown_signal)` wraps `serve()` with a future the
   binary drives from SIGTERM/SIGINT. `cluster.sh stop` waits up to 60 s before SIGKILL.

## Programming Notes

1. **Flush is 3-phase** — never hold the write lock during SSTable construction or
   stream I/O. Take the write lock only for the final reader swap.

2. **`pickup_tables` has two strategies** — understand both head-extent and
   size-tiered paths before modifying compaction selection.

3. **Discard map pipeline**: compaction drops a VP entry → accumulates size in a local
   `discard` map → attaches to the last output SST's MetaBlock → persisted to
   metaStream → aggregated by GC from all SstReaders. Break any link and GC won't
   collect dead VP data.

4. **`has_overlap` blocks split but not reads** — `range()` with `has_overlap` set
   range-filters; `get()` does NOT filter (point lookups are exact).

5. **No local WAL file** — logStream is the sole WAL. All writes (small and large) go
   to logStream via `append_batch`. If no checkpoint exists (tables empty AND
   vp_eid == 0), recovery replays logStream from the first extent, offset 0 (covers
   partitions killed before their first flush). Unflushed imm tables in memory are
   covered — logStream contains all records newer than the last SSTable flush.

6. **Group commit batching + durability.** `partition_loop` drains up to
   MAX_WRITE_BATCH (256) requests per RPC cycle; the batch's `must_sync` is the OR of
   caller flags only. Durability lives in two complementary places:
   - **Per-write coverage**: the extent-node's per-extent fsync coalescer fires
     `sync_data` every 1–5 ms; every append's bytes become durable within one coalesce
     window regardless of `AppendReq.must_sync`.
   - **Flush barrier**: `flush_one_imm` calls
     `part_sc.await_log_synced_to(vp_extent_id, vp_offset)` BEFORE uploading the SST.
     INVARIANT: **ALL log_stream replicas** (not quorum-min) must report `last_synced >=
     vp_offset` first — every byte the imm's ValuePointers reference must be durable on
     every replica BEFORE the SST that names them is checkpointed, else a later
     min-commit truncation on an un-synced replica orphans the VP (the
     `stale_vp_offset_past_sealed_length` class). On a healthy cluster this waits ≈ 0.
   The fsync work is entirely background (latency-invisible); every Put pays only the
   1–5 ms coalesce floor. The `must_sync` field is kept for wire back-compat but is
   always true in practice.

7. **Per-partition StreamClient** — each `PartitionData` holds its own
   `stream_client: Arc<StreamClient>` (no Mutex) via `new_with_owner_epoch`.
   StreamClient is internally concurrent via per-stream locking (`DashMap<stream_id,
   Arc<Mutex<StreamAppendState>>>`): different streams (log/row/meta) are fully
   concurrent; the same stream is serialized. The server-level
   `PartitionServer.stream_client` is used only in `split_part` coordination RPCs.

8. **`start_write_batch` / `finish_write_batch` lock scope** — the write lock is held
   only for seq assignment + block encoding (Phase 1), released before the
   `append_batch` RPC (Phase 2), re-acquired for memtable insert + VP head update
   (Phase 3). Prevents the partition write lock from blocking reads/flushes/compaction
   during network I/O.

9. **`sst_readers` and `tables` are always aligned by index** — `tables[i]` and
   `sst_readers[i]` refer to the same SSTable. Compaction's atomic swap replaces
   slices, not individual elements.

10. **Memtable backing = `parking_lot::RwLock<BTreeMap>`** — the active memtable has
    exactly one writer (the P-log thread's Phase 3) and N readers (ps-conn
    `handle_get` + P-log). Correctness:
    - Writer holds the write lock for one `insert_batch` call (up to 256 entries) then
      releases; subsequent readers take the read lock AFTER → linearisable
      Put-then-Get.
    - Rotation (`rotate_active`) replaces the whole `Memtable` via `std::mem::replace`
      on the owning `PartitionData` — safe because it runs exclusively on P-log inside
      a `RefCell::borrow_mut`.
    - `imm: VecDeque<Arc<Memtable>>` — frozen memtables are read-only from P-log (flush
      + GC + compaction) and P-sst (`build_sst_bytes`); multiple readers acquire the
      read lock concurrently.
    - Hot path uses `insert_batch(iter)` (one write lock per 256 inserts) and
      `for_each(closure)` (read lock held for the iteration). The `bytes: AtomicU64`
      counter is outside the lock, so `mem_bytes()` / `maybe_rotate` stay lock-free.

11. **Metadata-publish ordering invariant.** `flush_one_imm` (lib.rs) and `do_compact`
    (background.rs) both publish to `meta_stream` via `save_table_locs_raw`. They run as
    separate background tasks on the single-threaded P-log runtime and interleave at
    every `.await`. INVARIANT (race-free concurrent publishing — the LATEST persisted
    meta_stream record always reflects ALL prior in-memory mutations from both
    publishers) rests on three load-bearing properties — DO NOT violate:
    - **(P1)** P-log compio runtime is single-threaded.
    - **(P2)** the `borrow_mut` block that captures `tables_snapshot` contains no `.await`.
    - **(P3)** the path `borrow_mut` drop → `rkyv_encode` → `stream_client.append` →
      mpsc-send-into-per-stream-worker is purely synchronous; the first `.await` is on
      the per-stream worker's `ack_rx`, AFTER the message lands in the FIFO mpsc.

    Together: `borrow_mut` order = mpsc-send order = meta_stream record order. Adding an
    `.await` between the `borrow_mut` drop and `stream_client.append` (moving
    `rkyv_encode` behind an async helper, an async metric flush, a `futures::lock::Mutex`
    around publish) re-opens a stale-snapshot race: a flush whose snapshot was captured
    earlier could be ack'd later than a compact's, persisting tables that compact already
    removed → on restart recovery resurrects compacted-away SSTs whose VPs may point at
    GC-punched log_stream extents. Inline invariant comments mark both call
    sites; test `publisher_invariant_tests` exercises two concurrent publishers.

12. **Metrics export.** Each `PartitionData` carries an `Arc<PartitionMetrics>` whose
    AtomicU64 counters are bumped by `partition_loop` (req_count on each
    `handle_incoming_req`, imm_full_count on imm-cap stalls). The same Arc is cloned into
    the `PartitionHandle` on the main thread; `report_load_loop` (5 s) snapshots all live
    handles, computes /sec rates, ships `ReportPartitionLoadReq`. Maintenance-debt gauges
    on the same struct feed `compute_maintenance_advisory`: `gc_debt_bytes` (refreshed
    each GC tick from `Σ(get_discards filtered to live sealed log extents)`),
    `pending_compaction_bytes` (each compact tick: total SST bytes if `has_overlap==1`,
    else `pickup_tables` output), `gc_inflight` / `compact_inflight` (0/1 around the
    awaits), `last_gc_at` / `last_compact_at` (unix-epoch, drives per-kind cooldown).
    `compute_pending_compaction_bytes(part)` lives in `background.rs`.

    **Async-op outcome reporting.** `PartitionMetrics.maintenance_outcomes` is a
    bounded ring (cap 8) of terminal `MaintenanceOutcome{op_id, kind, state, error,
    message}` for manager-submitted compact/gc/forcegc ops (those carrying a
    non-zero `MaintenanceReq.op_id`; `op_id == 0` = the PS-local scheduler / legacy
    SDK, recorded nowhere). `background_maintenance_loop` records the outcome at
    each terminal exit (compact Ok/Err/skip/freeze-defer; the GC holes loop's
    aggregate success / first-error) via `record_maint_outcome`. `report_load_loop`
    copies the ring onto `PartitionLoad.maintenance_outcomes` every heartbeat
    (idempotent retransmit — the ring isn't drained; the manager reconciles by
    op_id once, so a dropped report self-heals). This is the ONLY channel that
    carries a maintenance op's failure reason back — without it, a gc/compact error
    dies in a `tracing::error!` invisible to the operator.

    The PS-level `maintenance_scheduler_loop` (5 s, main thread) is the primary trigger
    source: reads the gauges, computes `urgency = debt / threshold`, sorts desc,
    dispatches top-K minor compactions / GCs via Send-capable trigger channels in
    `PartitionHandle`. Skips partitions whose `req_per_sec` (from `req_count` diff)
    exceeds `AUTUMN_PS_FG_QPS_QUOTA` (default 50K) — foreground always wins. Cooldowns
    drive from PS-side `last_*_at`. The compact channel's `bool` payload means `is_major`
    (true: manual `client compact`, expiry; false: scheduler routine, picks via
    `pickup_tables`). Background loops keep their channel-receive paths but their timeout
    branches are demoted to short metric-refresh ticks (they no longer fire compact/GC
    off the timer, except expiry-major which the scheduler doesn't see).

13. **Partition merge (manager-orchestrated, TiKV PrepareMerge + PS-side write halt).**
    The merge primitive is a manager-side atomic etcd txn (manager CLAUDE.md note 16);
    the PS closes the merge-window data-loss gap with a write halt. Wire: client →
    `MSG_MERGE_PARTITIONS { survivor, victim }` → manager → `MSG_MERGE_FREEZE { freeze:
    true }` to victim PS → same to survivor PS → 6× commit_length under freeze →
    `handle_multi_modify_merge` atomic etcd txn → return. PS-side state on
    `PartitionData`:
    - `frozen_for_merge: Cell<Option<Instant>>` — `Some(set_at)` while the write halt is
      in effect.
    - `freeze_drain_ack: RefCell<Option<oneshot::Sender>>` — parked freeze response.

    `handle_incoming_req` short-circuits Put / Delete with `CODE_UNAVAILABLE` while
    frozen; reads + maintenance flow normally. `partition_loop` top-of-loop:
    - if `freeze_drain_ack.is_some() && pending.is_empty() && inflight.is_empty()`:
      rotate active + flush every imm via `flush_one_imm`, then send OK on the parked
      oneshot (the strict precondition for the orchestrator's commit_length capture to
      be race-free).
    - if `frozen_for_merge` elapsed > `FREEZE_TTL` (30 s): auto-unfreeze + drop stale ack
      with PRECONDITION (orchestrator-crash backstop; happy path completes < 1 s).

    Recovery on success: the merge txn deletes victim's region and widens survivor's;
    `region_sync_loop` sees both on its next ~2 s tick, drops the frozen `PartitionData`
    for victim, reopens survivor with `frozen_for_merge = None` (no explicit unfreeze).
    Recovery on failure: manager sends `MSG_MERGE_FREEZE { freeze: false }` rollback; the
    FREEZE_TTL backstop fires if even that fails. Merge wallclock is ~2–3 s (bounded by
    the region_sync tick) but write loss is 0. This model avoids cross-thread plumbing
    (each `PartitionData` is `Rc<RefCell<>>`, `!Send`) that a PS-orchestrated design
    would need.

14. **Background-loop supervision — no loop dies silently; durability loops fail-stop.**
    Every PS background loop runs under a supervisor wrapper, never a bare
    `spawn(..).detach()` (which swallows panics → a dead loop with no signal). Two
    helpers in `lib.rs`:
    - `spawn_supervised(name, make)` — catch_unwind + ERROR-log + 1 s restart. For
      RESTARTABLE loops that re-derive state each tick and own no moved resource:
      `heartbeat_loop`, `report_load_loop`, `region_sync_loop`.
    - `spawn_failstop(name, fut)` — catch_unwind; NORMAL return is the expected shutdown
      (no-op); PANIC → ERROR-log + `std::process::exit(1)`. For NON-restartable loops that
      own a moved channel receiver / `TcpListener` and are durability/serving-critical:
      per-partition `background_flush_loop` / maintenance / the per-partition accept loop.
      Fail-stop (not restart) because the moved receiver can't be re-acquired and
      re-running on a mid-panic half-mutated `PartitionData` could double-apply; exiting
      lets the manager evict + reopen from the durable streams (log_stream WAL = source of
      truth → no committed loss).

    INVARIANT: never reintroduce a bare `spawn(..).detach()` for a PS background loop —
    pick supervised (re-derive-safe) or failstop (moved-resource / durability). Per-conn
    `handle_ps_connection` spawns are intentionally NOT wrapped — a panic there drops one
    request-scoped client connection, not a background loop. The explicit `catch_unwind`
    is for observability + decisioning (log, restart, exit); compio's own
    spawn-catch_unwind only keeps the runtime *thread* alive (which is exactly what made
    silent loop death possible — the caught panic was dropped with the detached
    JoinHandle). The two layers are not bug-redundant; don't "remove the duplicate."

15. **`flushing_imm_ptrs` claim MUST be released on EVERY flush error path, and reads
    (RANGE/HEAD) MUST be served off `partition_loop`.** `flushing_imm_ptrs` is a per-imm
    "who is flushing this" latch shared by the two flush drivers on the P-log runtime
    (`background_flush_loop` lazy, `flush_one_imm` eager — called inline by split / merge
    / graceful+freeze drain) so the same imm is never double-flushed. CONTRACT: the
    claimer always reaches `commit_flush_outcome`, which pops the imm + removes the ptr.
    If a flush ERRORS without removing the ptr, the imm is orphaned (claimed but no-one
    flushing) → every later attempt sees it "already claimed" → the imm NEVER drains →
    `partition_loop` stays imm-full parked FOREVER.
    - **Release on every error.** `run_flush_async_phase` AND `commit_flush_outcome`
      remove their `src_imm_ptr` on any Err (idempotent). `background_flush_loop` also
      releases the ptr of a *successful* outcome it must DROP because a FIFO-earlier imm
      in the same batch failed, and retries pending imm every `FLUSH_RETRY_BACKOFF` (2 s)
      so a failed flush self-heals once the cluster recovers (a parked loop produces no
      rotate → no `flush_tx`). INVARIANT: any new flush error path must release the claim.
    - **Reads off the write loop.** RANGE + HEAD are served LOCALLY on the ps-conn task
      (`serve_read_local`), mirroring `serve_get_local` for GET.
      `handle_range`/`handle_head` brief-`borrow()` to snapshot then `drop` before
      iterating, so a flush-wedged (back-pressured) `partition_loop` halts WRITES without
      taking READS down. INVARIANT: read handlers served locally must never hold a
      `RefCell` borrow across an `.await`. (This relaxes per-connection read-your-writes
      for a read pipelined behind an in-flight delegated write on the same connection —
      the same gap GET-local already has.)

16. **row_stream single-writer is type-level; split invalidates P-sst via a SYNC
    BARRIER.** All `row_stream.append` MUST go through P-sst's `sst_sc` — flush
    via `FlushReq`, compaction via `RowAppendReq` (see the row_stream single-writer
    invariant in the P-sst section). `part_sc` is for log/meta append and row_stream
    non-append ops only. P-sst spawn failure is fatal-for-this-partition;
    `flush_req_tx`/`row_append_tx` are non-`Option`.

    Split invalidates P-sst's stale row_stream worker via a synchronous P-log → P-sst
    barrier (a lazy per-message invalidate flag is racy with P-sst's `FuturesUnordered`
    cap=2 — a post-split FlushReq could enter the FU concurrently with the invalidating
    RowAppendReq and append to the stale worker BEFORE the invalidate took effect → SST
    past sealed_length → orphan SST on recovery):
    1. `RowInvalidateBarrierReq { row_stream_id, resp_tx }` on
       `PartitionData.row_invalidate_tx` (capacity 1).
    2. `flush_worker_loop` consumes the barrier on a priority-biased
       `select_with_strategy` (PollNext::Left) and sets a `pending_barrier` slot.
    3. The next iteration drains `inflight` `FuturesUnordered` to ZERO, then
       `sst_sc.invalidate_stream(row_stream_id)`, then signals `resp_tx`. No new SQ
       message is picked up until the barrier completes.
    4. `handle_split_part` sends the barrier INSIDE the critical section BEFORE
       `multi_modify_split` (after drain + commit_length, before the manager seal) and
       AWAITS the ACK.

    Same-partition compact-vs-split serialization is by the per-partition
    `maintenance_gate` (see Split serialisation).

    **INVARIANTS:**
    1. Any control message that mutates row_stream from P-log MUST travel through
       P-sst's channel set — never `part_sc`.
    2. `handle_split_part` MUST await the barrier ACK BEFORE releasing `maintenance_gate`
       or clearing `frozen_for_split` — a freshly-resumed compact/flush loop could
       otherwise send a P-sst request before `sst_sc` is invalidated.
    3. The barrier channel is capacity 1 and `handle_split_part` is the sole sender; a
       new sender must prove the sender count stays 1 or convert to unbounded.
    4. `flush_worker_loop`'s `pending_barrier` MUST drain `inflight` to ZERO before
       invalidating + ACKing (else an in-flight append on the stale worker lands after
       the invalidate ACKed).
    5. The priority-biased select (PollNext::Left) is DEFENSIVE — under today's gate
       discipline the SQ queue is empty when the barrier arrives, but the bias keeps it
       race-free if a future refactor relaxes the gates.
    6. `handle_split_part` sends the barrier BEFORE `multi_modify_split`; failure of the
       send / ACK must abort split with the manager not yet committed. Post-seal
       placement would leak an unrecoverable window (manager-committed seal + local state
       not converged + freeze cleared).
    7. Any new do_compact / split-shaped dispatch site MUST acquire `maintenance_gate`
       first, then `acquire_compact`.
    8. BOTH the barrier `send().await` AND the ACK `recv().await` MUST be bounded by
       separate timers (5 s send + 10 s ACK). The total (15 s) MUST stay well under
       `FREEZE_TTL` (30 s) — a wedged P-sst blocking past FREEZE_TTL would let
       `check_freeze_ttls` unfreeze, new writes extend the row_stream tail past the
       already-captured `commit_length`, and `multi_modify_split` would commit a STALE
       `row_end` → post-TTL writes above sealed_length, invisible on recovery. Both
       timeouts unfreeze + return `FailedPrecondition`. If tuning changes either, `send +
       ack < FREEZE_TTL` MUST hold.

    P-sst readiness handshake: `spawn_sst_thread` returns `(JoinHandle,
    oneshot::Receiver<Result<()>>)` and sends `Ok(())` only AFTER compio `build`,
    `StreamClient::new_with_owner_epoch`, and `set_reporter_part_id` all succeed;
    `partition_thread_main` awaits it before publishing `flush_req_tx` / `row_append_tx`
    / `row_invalidate_tx` — a partition is never half-opened with a live Sender to a
    dropped Receiver (thread abort surfaces as `Err(Canceled)`, not a hang). INVARIANT:
    any change to the in-thread init order MUST send the ready signal only after ALL
    `flush_worker_loop` preconditions are met.

17. **`part_addr` self-heal in `sync_regions_once`.** The manager's `part_addrs`
    (per-partition listener addresses served to clients for routing) is in-memory only
    and LOST on manager restart; registration happens once inside `open_partition`, so an
    already-open partition never re-reported → a manager kill+respawn under a healthy
    cluster left clients unable to resolve any partition listener. `sync_regions_once` now
    compares the just-fetched `resp.part_addrs` against open partitions'
    `PartitionHandle.part_addr` and re-sends `MSG_REGISTER_PARTITION_ADDR` for any
    missing/stale entry (zero steady-state cost; convergence one ~2 s tick). INVARIANT:
    any code path that binds/re-binds a partition listener must keep
    `PartitionHandle.part_addr` equal to the ACTUALLY advertised address — the self-heal
    trusts it as source of truth.

18. **Heartbeat-loss exit is a 90 s last resort, not a 10 s tripwire.**
    `heartbeat_loop`'s `MAX_CONSECUTIVE_FAILURES` is 45 (× 2 s = 90 s). The
    `std::process::exit(1)` on sustained heartbeat failure guards only the narrow
    "partitioned from the manager but not from clients" stale-serving case — it is NOT the
    primary fencing (owner_epoch fences writes at the ENs; region_epoch fences client
    routing). At a lower threshold, any manager outage longer than it (including the ~60 s
    TIME_WAIT bind retry after a manager respawn) makes the entire PS fleet exit
    simultaneously — a self-inflicted, unrecoverable data-plane outage. While the manager
    is down NO reassignment can happen, so serving through its outage is safe.

19. **Owner locks are PER-PARTITION, acquired fresh at every `open_partition`.**
    `owner_key = "partition/<part_id>"` — ONE stable logical key per partition, NO ps_id
    in the key (a per-PS key shape would leave the old owner's key valid at the manager
    after takeover, so its lingering GC punch_holes/truncate could mutate streams owned
    elsewhere; with a stable key the takeover acquire bumps THE SAME key and the old owner
    fails `ensure_owner_epoch` everywhere). P-log's `part_sc` and P-sst's `sst_sc` share
    the ONE epoch acquired for that open (via `new_with_owner_epoch`). The epoch must be
    newest-at-TAKEOVER, not newest-at-process-start — a standing PS inheriting partitions
    from a PS that acquired LATER would sit below the EN fence floors forever. Per-partition
    keys scope fencing exactly: an open of partition X bumps only X's key; siblings keep
    their epochs; X's previous owner alone is fenced and self-evicts via LockedByOther. The
    PS-lifetime `server_owner_key`/`server_revision` ("ps-<id>") remains ONLY for
    split-coordination RPCs.

    **EAGER takeover fence (G1 "SIGSTOP zombie writer" fix).** Acquiring E_new only
    fences the previous owner at the MANAGER (`ensure_owner_epoch` equality) — the EN
    per-extent `owner_epoch` floor is raised LAZILY, by the new owner's FIRST append
    (stream note 23). So on an IDLE takeover (E_new acquired, no write yet), the EN floor
    stays at E_old, and a paused-then-resumed old owner's in-flight append carrying E_old
    PASSES the EN fence (`E_old == stored`), lands in the log extent, and is silently
    ACKed — a lost update the new owner never sees. `partition_thread_main` closes this:
    right after the initial `commit_length` loop and BEFORE `recover_partition`, it calls
    `part_sc.fence_tail(sid, owner_epoch)` for each of log/row/meta, EAGERLY raising the EN
    floor to E_new via `MSG_FENCE_EXTENT` (a control op, not an append — so routing all
    three through `part_sc` respects the row_stream single-writer rule). Best-effort +
    lenient (append is all-replica-ACK, so one fenced replica already blocks the zombie);
    a transient fence failure is logged and the open PROCEEDS (never wedge). This makes the
    append-path fence EAGER, not first-append-lazy; it does not replace it. Regression
    guard: `crates/manager/tests/system_sigstop_zombie_writer.rs` (asserts the old owner
    can no longer cleanly ACK a stale-epoch write). The read-side `handle_get` write fence
    (a residual STALE READ before the old owner closes the reassigned partition) is a
    documented SEPARATE follow-up.

20. **Manager-list rotation invariants (keep all four or HA failover silently dies).**
    (a) `PartitionServer.current_mgr` is `Rc<Cell<usize>>` (the struct is cloned once per
    supervised loop; a plain Cell gives each loop a PRIVATE rotation index, so only
    heartbeat's rotates while region_sync + part_addr self-heal hammer the dead manager
    forever).
    (b) Every StreamClient manager RPC routes through `manager_call` (rotate on transport
    failure) or `retry_manager_call`; decode sites call `note_manager_code` (rotate on
    CODE_NOT_LEADER). Never add a raw `pool.call_timeout(self.manager_addr(), ..)` site.
    (c) `MSG_GET_REGIONS` + `MSG_HEARTBEAT_PS` are leader-gated; the PS heartbeat Ok-arm
    rotates on NOT_LEADER and COUNTS it toward the 90 s stale-serving budget (only a real
    leader ACK resets — a follower-only-reachable PS is partitioned from the leader);
    `sync_regions_once` rotates on NOT_LEADER. A follower answering OK pins the shared
    rotation to itself while serving stale `part_addrs`.
    (d) Client SDK `refresh_regions` rotates + retries on NOT_LEADER.

21. **Latency histograms + /metrics.** `PartitionMetrics.write_lat/get_lat` (`LatHist`:
    9 finite ns buckets 0.5ms..250ms + sum + count, non-cumulative storage, cumulative
    `le` at render). PUT observes the ALREADY-MEASURED `BatchStats.end_to_end_ns` once
    per batch with `n = ops` (zero new hot-path timing); GET adds one `Instant` pair +
    RefCell borrow in `serve_get_local`. `PartitionServer::metrics_text()` renders the
    Prometheus snapshot on the MAIN compio thread (the `partitions` map is `Rc<RefCell>`);
    `--metrics-port` spawns a 2 s publisher task that copies the string into an
    `Arc<RwLock<String>>` served by `autumn_common::metrics_http`. Export rules: only
    `req_count_monotonic` (the never-reset counter — `req_count` is swap-reset every 30 s
    and would saw-tooth) plus gauges (size / gc-debt / pending-compaction bytes,
    gc/compact inflight, sealed log extents). Emission is metric-major (all samples of one
    metric contiguous after its `# TYPE` line — the Prometheus text format requires it).

## WAL-FAILSTOP — mid-stream log_stream corruption fails recovery, not silent skip

INVARIANT: the log_stream replay decoder must FAIL LOUD on a `DecodeOne::Corrupt`
(a COMPLETE record whose V1 CRC or inner-length disagrees = bit rot / torn write), not
`skip + continue` — skipping drops an ACKed-but-unflushed write AND holes the replay
sequence → silent data loss.
- `decode_records_chunk` returns `anyhow::Result`: `Corrupt` → `Err`
  (`recover_partition` propagates via `?` → partition open fails loud → recover from a
  healthy replica). `Incomplete` (truncated TAIL) still `break`s clean (crash-tail).
- `process_gc_chunk` likewise `Err`s on `Corrupt` — refusing to punch_holes past records
  it can't parse (could reclaim still-live VP data).
- **Committed-end carry check**: a record whose LENGTH field is bit-flipped LARGER reports
  as `Incomplete` (the `bytes.len() < total` check precedes the CRC check), escaping the
  `Corrupt` arm. But replay reads are committed-clamped (`read_committed_bytes_from_extent`)
  and the committed boundary always lands on a record boundary (commit advances by whole
  all-replica-ACKed records), so a **non-empty carry at the committed end** can only be a
  length-corrupt / lagging-truncated committed record, never a legit crash-tail.
  `recover_partition` `Err`s on it instead of discarding.

Trigger is power-loss / bit-rot class (a process kill loses nothing un-fsynced + leaves
the dirent). Test-only `decode_records_full` / `decode_records_with_offsets` keep the old
skip (not on any production path).
