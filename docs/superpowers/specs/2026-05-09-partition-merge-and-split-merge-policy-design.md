# Partition Merge + Split/Merge Policy Engine — Design Spec

**Date:** 2026-05-09
**Scope:** Add partition merge primitive (CoW stream-extent splice, no value rewrite) + size+load-driven advisory policy engine for split/merge candidate detection. Manual triggers in Stage 1; auto-split + auto-merge gated behind feature flags in later stages.
**Reference:** HBase Normalizer (HBASE-7308), Azure WAS partition manager (WAS paper §5), CockroachDB range merges (v19.2).

---

## 0. Goals & Non-Goals

### Goals
1. Implement **partition merge** as the inverse of the existing F008/F037/F124/F140 split path — `MgrPartitionMeta` schema unchanged, single-stream-per-partition invariant preserved.
2. Implement a **size+load policy engine** in the manager that computes split/merge *candidates* from per-partition metrics carried in the existing 2 s heartbeat.
3. Ship Stage 1 (manual merge primitive + advisory engine + CLI) end-to-end. Future stages gated behind `AUTUMN_MGR_AUTO_SPLIT` / `AUTUMN_MGR_AUTO_MERGE` feature flags (default off).

### Non-Goals (deferred)
- **Cross-PS merge** — refused at advisory time. Operator must co-locate first (manual partition migration is itself a future feature).
- **Partition migration primitive** — deferred to a separate feature; merge policy ignores cross-PS pairs in Stage 1.
- **Multi-stream-per-partition schema** (HBase FAST_MERGE-style) — explicitly rejected (see §1.3).
- **Auto-trigger of split/merge** — Stages 2+, separate commits, gated by the burn-in criteria in §6.
- **Split-after-merge cooldown enforcement at the etcd layer** — recorded in `MgrPartitionMeta.last_op_at` and read by the policy engine; not a manager-side hard guard (covered by hysteresis on policy thresholds).

---

## 1. Architecture

### 1.1 Inverse-of-Split via Stream-Extent Splice (no value rewrite)

The split path uses `compute_duplicate_stream` to CoW the source partition's three streams into a new partition (`refs += 1` on shared extents). Merge is the metadata-symmetric inverse:

```
Two adjacent partitions:        After merge:
  L: rg [A, M)                    Survivor (= L): rg [A, Z)
     log_stream  L.log_id            log_stream:  extent_ids =
     row_stream  L.row_id              [L.log extents] +
     meta_stream L.meta_id              [V.log extents] +
                                        [new tail extent E_new]
  V: rg [M, Z)                       row_stream:  extent_ids =
     log_stream  V.log_id              [L.row extents] +
     row_stream  V.row_id              [V.row extents]
     meta_stream V.meta_id          meta_stream: ONE new TableLocations
                                       record = union(L.tables, V.tables)
                                       truncate to 1 extent

                                  Victim (= V): deleted from manager;
                                  V.log_id / V.row_id / V.meta_id stream
                                  metadata removed (extents survive
                                  through L's stream membership).
```

No SST bytes move. No VP-target log_stream extents move. Only manager metadata + survivor PS's in-memory `PartitionData` change. VP `(extent_id, offset)` tuples in victim's SSTs continue to resolve because `read_bytes_from_extent` is partition-agnostic at the extent-node layer; the extent IDs are now in survivor's log_stream's `extent_ids` list (`refs += 1` on each).

### 1.2 Why this works without schema change

| Concern | Why it's safe under stream-extent splice |
|---|---|
| Single `log_stream_id` per partition | Survivor's `PartitionData.log_stream_id` unchanged; victim's `log_stream` extents become *additional members* of survivor's log_stream's `extent_ids` list, not a separate stream |
| MVCC seq monotonicity | User-key ranges are **disjoint** by precondition (`L.end_key == V.start_key`); no cross-partition same-user-key collision. Survivor's seq counter advances to `max(L.last_seq, V.last_seq) + 1` post-merge |
| `vp_head` replay window | Merge writes a new merged `TableLocations` checkpoint into survivor's meta_stream with `vp_head = (E_new, 0)` where `E_new` is a freshly allocated tail extent; recovery walks forward from there and finds nothing to replay (both partitions drained pre-merge) |
| `extent_ids` ordering invariant | Strict order `[L.sealed extents] + [V.sealed extents] + [E_new]` — load-bearing; documented inline in `multi_modify_merge` next to F148-A class invariants |
| `vp_table_refs` accounting | Manager merges `partition_vp_refs[V]` into `partition_vp_refs[L]` and deletes `[V]` — same diff-apply pattern as `apply_partition_vp_refs` |
| `has_overlap` flag | Both partitions must enter merge with `has_overlap = 0` (precondition); merged ranges are tight-union → trivially `0` |
| Owner-lock fencing | Survivor keeps its own revision; victim's stream IDs are deleted from manager → any leaked stale victim PS append fails with `NotFound` |

### 1.3 Approaches Considered & Rejected

| Approach | Reason rejected |
|---|---|
| **A — HBase FAST_MERGE-style multi-stream-per-partition** | Schema change to `MgrPartitionMeta`, `PartitionData`, `open_partition` (multi-vp_head replay), `do_compact` (multi-source), `vp_resolve`, `gc`. Too large blast radius; until major compact unifies, every Get pays 2× bloom-check overhead; mixed seq-namespaces are a fragile invariant. |
| **B — naive read-side rewrite (Range → Put)** | O(user-data bytes) network + log_stream growth; large-value re-shipping through survivor's group commit; multi-hour for TB merges. |
| **B-refined (selected — §1.1)** | O(metadata) at merge, no schema change, no value rewrite, reuses existing CoW pattern. |

---

## 2. Wire Protocol Changes

### 2.1 New manager RPC: `MSG_MULTI_MODIFY_MERGE`

`crates/rpc/src/manager_rpc.rs`:

```rust
pub const MSG_MULTI_MODIFY_MERGE: u8 = 0x34;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MultiModifyMergeReq {
    pub survivor_part_id: u64,
    pub victim_part_id: u64,
    pub owner_key: String,
    pub revision: i64,
    /// commit_length on each stream, captured AFTER drain on both PSes
    /// indexed [0]=survivor, [1]=victim
    pub log_sealed_lengths:  [u64; 2],
    pub row_sealed_lengths:  [u64; 2],
    pub meta_sealed_lengths: [u64; 2],
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MultiModifyMergeResp {
    pub code: u8,
    pub message: String,
    /// extent_id of the newly-allocated empty tail for survivor's
    /// log_stream; PS uses this as the vp_head when writing the
    /// merged TableLocations checkpoint.
    pub new_log_tail_extent_id: u64,
}
```

Response carries `new_log_tail_extent_id` because the manager allocates it atomically inside Phase 1 (via `alloc_ids(1)` + `alloc_extent_on_node` calls — same flow as `handle_stream_alloc_extent`).

### 2.2 New PS RPC: `MSG_MERGE_PART`

`crates/rpc/src/partition_rpc.rs`:

```rust
pub const MSG_MERGE_PART: u8 = 0x4D;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergePartReq {
    pub survivor_part_id: u64,
    pub victim_part_id: u64,
}

pub struct MergePartResp { pub code: u8, pub message: String }
```

Sent to the **survivor's PS**. Survivor's PS:
1. Resolves victim's PS (which by precondition is *itself* — same-PS only) and runs both partitions' drain locally.
2. Coordinates with victim partition's `merged_partition_loop` via a new `MergeFreeze` partition message.

### 2.3 New PS-internal message: `MergeFreeze` / `MergeRelease`

`crates/partition-server/src/lib.rs` — extend the per-partition `PartitionRequest` enum (or add a sibling channel):

```rust
enum PartitionControl {
    Drain(oneshot::Sender<()>),         // F120-C, existing
    MergeFreeze(oneshot::Sender<()>),   // NEW: drain + halt new writes
    MergeRelease,                       // NEW: close partition, free threads
}
```

`MergeFreeze` semantics: stop `req_rx.next()` polling, drain `pending` + `inflight`, flush all `imm`, ack via oneshot. Subsequent `req_rx` writes return `CODE_UNAVAILABLE` until `MergeRelease` (which proceeds to close the partition entirely).

### 2.4 New manager RPC: `MSG_GET_POLICY_CANDIDATES`

`crates/rpc/src/manager_rpc.rs`:

```rust
pub const MSG_GET_POLICY_CANDIDATES: u8 = 0x35;

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct GetPolicyCandidatesReq {}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PolicyCandidate {
    pub kind: u8,                          // 0 = split, 1 = merge
    pub primary_part_id: u64,              // split: target; merge: survivor
    pub secondary_part_id: u64,            // split: 0; merge: victim
    pub reason: String,                    // human-readable trigger
    pub size_bytes: u64,                   // primary's size
    pub req_per_sec: u32,
    pub imm_full_per_sec: u32,
    pub same_ps: bool,                     // merge: false → infeasible candidate
    pub last_op_at: i64,                   // unix epoch sec
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetPolicyCandidatesResp {
    pub code: u8,
    pub message: String,
    pub candidates: Vec<PolicyCandidate>,
}
```

Used by `autumn-client policy candidates` (§5) and by Stage 2/3 auto-trigger loops.

### 2.5 New RPC: `MSG_REPORT_PARTITION_LOAD` (PS → manager, every 5 s)

Heartbeat schema is left UNCHANGED (rkyv struct evolution is fragile; an additive field on `HeartbeatPsReq` would break old-PS / new-manager mixed deployments). Metrics ride a dedicated periodic RPC instead:

```rust
pub const MSG_REPORT_PARTITION_LOAD: u8 = 0x36;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PartitionLoad {
    pub part_id: u64,
    pub size_bytes: u64,                   // SST total + active.bytes + Σ imm.bytes
    pub req_per_sec: u32,                  // 60 s rolling window on PS
    pub imm_full_per_sec: u32,             // F120-A back-pressure event rate
    pub p99_us: u32,                       // optional, 0 = not measured
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReportPartitionLoadReq {
    pub ps_id: u64,
    pub partitions: Vec<PartitionLoad>,
}
```

Response is `CodeResp`. Cadence: 5 s (between heartbeat 2 s for liveness and the policy engine's 60 s tick — gives the manager 12 metrics samples per tick worst-case). Old PS that doesn't send this → manager's policy window stays empty for that partition → policy returns no candidates for it. Fully backward-compatible.

### 2.6 Per-partition `last_op_at` tracked via sidecar etcd key

`MgrPartitionMeta` schema is **unchanged** (same rkyv-evolution concern). Add a sidecar etcd prefix:

```
partitionLastOp/<part_id>  -> i64 (unix epoch sec) as little-endian bytes
```

- `replay_from_etcd` reads all keys under `partitionLastOp/` into an `Rc<RefCell<HashMap<u64, i64>>>` on `AutumnManager`.
- `handle_multi_modify_split` and `handle_multi_modify_merge` write the survivor's (and right-child's) `last_op_at = now` as an additional kv in their atomic txn.
- Manager exposes `last_op_at(part_id) -> i64` (returns 0 if absent → "cooldown satisfied").
- Policy engine consults this getter when computing candidates.

Trade-off: one extra etcd put per split/merge event (negligible) vs zero risk to the existing `MgrPartitionMeta` archive layout.

### 2.7 Wire constants summary

| Const | Value | Notes |
|---|---|---|
| `MSG_MULTI_MODIFY_MERGE` | `0x34` | (0x33 already taken by MSG_SYNC_PARTITION_VP_REFS) |
| `MSG_GET_POLICY_CANDIDATES` | `0x35` | |
| `MSG_REPORT_PARTITION_LOAD` | `0x36` | PS → manager, 5 s cadence |
| `MSG_MERGE_PART` | `0x4D` | (0x46 already taken by MSG_STREAM_PUT; 0x49-0x4C used by F129 PutStream) |
| `CODE_PRECONDITION` (existing) | reused for cross-PS / has_overlap / inflight refusals |

---

## 3. Manager-side Implementation

### 3.1 `handle_multi_modify_merge`

`crates/manager/src/rpc_handlers.rs` — pattern matches `handle_multi_modify_split` (F124 single-txn + F138/F145/F146 inflight checks + F149 fence):

```
handle_multi_modify_merge(req):
  Phase 1 (under borrow_mut, NO awaits):
    ensure_leader + ensure_owner_revision
    fetch survivor + victim MgrPartitionMeta
    REFUSE if survivor == victim
    REFUSE if survivor.rg.end_key != victim.rg.start_key  (not adjacent)
    REFUSE if survivor or victim missing (NotFound)
    REFUSE if any extent in survivor's or victim's six streams is in
            ec_conversion_inflight (F138) OR recovery_tasks (F146) OR
            pending_extent_deletes (F139)
    snapshot pre_bump_eversion: HashMap<extent_id, u64>
    alloc_ids(1) → new_log_tail_extent_id
    select_nodes(...) → nodes for new tail extent
    build E_new MgrExtentInfo (replicates, parity, refs=1, sealed_length=0)
    compute compute_merge_streams(survivor, victim, sealed_lengths, E_new)
        → modified_streams: [survivor.log, survivor.row, survivor.meta]
          (extent_ids spliced; tail seal applied)
        → modified_extents: refs++ on every victim extent;
                            sealed_length applied to L's old tails;
                            E_new included with refs=1
    compute merged_vp_refs = survivor.vp_refs + victim.vp_refs (per-extent sum)
    compute vp_extent_puts = preview_partition_vp_refs_apply(merged_vp_refs)
    merged_extents = merge_extent_updates(modified_extents, vp_extent_puts)
    update survivor.rg.end_key = victim.rg.end_key
    delete-set:
      partitions/<victim_id>
      streams/<victim.log_id>, <victim.row_id>, <victim.meta_id>
      partitionVpRefs/<victim_id>
      regions/<victim_id>
      partitionLastOp/<victim_id>

  Phase 1.5 (await — extent-node calls):
    for each node in E_new's nodes:
      alloc_extent_on_node(node, E_new.extent_id)
      [matches handle_stream_alloc_extent's flow; on per-node failure,
       fall back to other healthy nodes via existing select_nodes retry]

  Phase 2 (etcd single fenced txn, F149):
    put: streams/<survivor.log/row/meta> (updated extent_ids)
    put: extents/<each modified extent>
    put: extents/<E_new>
    put: partitions/<survivor_id>
    put: partitionVpRefs/<survivor_id> (merged refs)
    put: regions/<survivor_id> (rg.end_key updated)
    put: partitionLastOp/<survivor_id> = now
    delete: partitions/<victim_id>
    delete: streams/<victim.log/row/meta>
    delete: partitionVpRefs/<victim_id>
    delete: regions/<victim_id>
    delete: partitionLastOp/<victim_id>
    [all in one put_msgs_txn → all-or-nothing]

  Phase 3 (under borrow_mut, post-etcd success):
    F146-style verify-at-apply: re-check pre_bump_eversion vs live; refuse if drift
    apply mutations to in-memory store.streams / .extents / .partitions /
          .partition_vp_refs / .regions
    update last_op_at HashMap entry for survivor_id
    delete last_op_at HashMap entry for victim_id
    rebalance_regions()  (no-op for survivor's PS, drops victim's region)

  Response: CodeResp(OK) + new_log_tail_extent_id
```

### 3.2 New manager helpers

| Helper | Module | Purpose |
|---|---|---|
| `compute_merge_streams(state, survivor, victim, log_sealed, row_sealed, meta_sealed, new_log_tail_id)` | `lib.rs` | Pure-fn analog of `compute_duplicate_stream`; returns updated stream metas + modified extents |
| `apply_merge_mutations(state, ...)` | `lib.rs` | Pure-fn applier; mirror of `apply_split_mutations` |
| `merged_partition_vp_refs(state, survivor_id, victim_id)` | `lib.rs` | Per-extent sum of two partitions' VP refs |

### 3.3 Policy engine

New file `crates/manager/src/policy.rs`:

```rust
struct PartitionMetricsWindow {
    // 30-min sliding window, 1-min buckets
    buckets: VecDeque<(i64, PartitionLoad)>,
}

struct PolicyEngine {
    metrics: HashMap<u64, PartitionMetricsWindow>,    // part_id -> window
    last_advisory_at: HashMap<u64, i64>,              // de-noise log spam
    advisory_cache: Vec<PolicyCandidate>,             // last computed
    advisory_cache_at: i64,
}

impl PolicyEngine {
    fn record(&mut self, ps_id: u64, loads: &[PartitionLoad], now: i64) {
        // append to per-partition window, drop buckets older than 30 min
    }

    fn compute_candidates(&mut self, state: &MetadataState, now: i64)
        -> Vec<PolicyCandidate>
    {
        // 1. SPLIT pass: for each partition, check 5/last 5 buckets:
        //    size > SPLIT_SIZE_HARD (50 GB)
        //    OR (req_per_sec > SPLIT_QPS_HIGH AND size > 1 GB)
        //    OR imm_full_per_sec > SPLIT_IMMFULL_HIGH
        //    AND now - last_op_at > SPLIT_COOLDOWN (1 h)
        //
        // 2. MERGE pass: for each adjacent pair (sorted by start_key),
        //    check 5/last 5 buckets BOTH show:
        //    size_l + size_r < MERGE_SIZE_LOW (1 GB)
        //    AND req_per_sec_l + req_per_sec_r < MERGE_QPS_LOW (5K)
        //    AND imm_full_per_sec == 0 on both
        //    AND has_overlap == 0 on both
        //    AND now - max(last_op_at_l, last_op_at_r) > MERGE_COOLDOWN (6 h)
        //    AND same_ps (mark false → still emit candidate but with same_ps=false
        //                 so operator/Stage-2 trigger can filter; advisory CLI shows it)
    }
}
```

Background loop on the manager's compio runtime:
- Tick every 60 s (`POLICY_TICK_INTERVAL`).
- Reads metrics window (already populated by `handle_heartbeat_ps`).
- Calls `compute_candidates` → updates `advisory_cache`.
- Logs each new candidate at INFO. De-duplicate via `last_advisory_at` (re-log only if 30 min elapsed since last identical).

### 3.4 Threshold constants

`crates/manager/src/policy.rs`:

```rust
pub const SPLIT_SIZE_HARD: u64       = 50 * GiB;
pub const SPLIT_SIZE_MIN: u64        = 1 * GiB;     // floor for QPS-driven split
pub const SPLIT_QPS_HIGH: u32        = 50_000;
pub const SPLIT_IMMFULL_HIGH: u32    = 10;
pub const SPLIT_COOLDOWN_SEC: i64    = 3600;        // 1 h

pub const MERGE_SIZE_LOW: u64        = 1 * GiB;     // each side's size bound
pub const MERGE_QPS_LOW: u32         = 5_000;
pub const MERGE_COOLDOWN_SEC: i64    = 6 * 3600;    // 6 h

pub const POLICY_BUCKET_SEC: i64     = 60;
pub const POLICY_WINDOW_BUCKETS: usize = 30;         // 30 min window
pub const POLICY_REQUIRED_BUCKETS: usize = 5;        // need 5 of last 5 to fire
pub const POLICY_TICK_INTERVAL_SEC: i64 = 60;
```

All constants are **hard-coded compile-time** (per the "no env reads in rs code" rule). Configurable per-cluster via CLI flags on `autumn-manager-server` if needed in later stages — Stage 1 keeps the defaults.

### 3.5 `handle_get_policy_candidates`

Returns `advisory_cache` directly. No recomputation per call (60 s cache).

### 3.6 `handle_report_partition_load`

New handler — appends `req.partitions` to `policy.metrics` window. Backward-compat: a PS that doesn't send this RPC at all simply has no policy metrics → policy returns no candidates for its partitions. Mixed-version cluster works correctly.

---

## 4. Partition-Server-side Implementation

### 4.1 `handle_merge_part`

`crates/partition-server/src/rpc_handlers.rs` — runs on the SURVIVOR's PS, dispatched on `merged_partition_loop` (analogous to `handle_split_part` running inline):

```
handle_merge_part(req, survivor: &Rc<RefCell<PartitionData>>, server: &Rc<PartitionServer>):
  1. Resolve victim partition handle on this PS.
     REFUSE Precondition if victim not on this PS (cross-PS merge).
  2. REFUSE Precondition if survivor.has_overlap != 0 OR victim.has_overlap != 0.
  3. Acquire dual-gate on BOTH partitions in strict order:
       (victim.compact_gate, victim.gc_gate, survivor.compact_gate, survivor.gc_gate)
     RAII permits held through manager RPC.
  4. Send MergeFreeze to victim's merged_partition_loop; await ack.
     This drains pending+inflight, flushes all imm on victim.
  5. flush_memtable_locked(survivor)  — drain survivor.
  6. Read commit_length on six streams (3 survivor + 3 victim).
     Sealed lengths array indexed [0]=survivor, [1]=victim per stream type.
  7. Call manager.multi_modify_merge(survivor_id, victim_id, sealed_lens).
     Manager allocates the new tail extent atomically inside the txn (§3.1
     Phase 1 + 1.5). On success, response carries new_log_tail_extent_id.
     Up to 8 retries with exponential backoff 100ms → 2s (Precondition errors
     surface immediately to caller; Internal/Unavailable retried).
  8. On manager OK: PS-side splice into survivor's PartitionData (under borrow_mut):
     - Read victim's current sst_readers (no I/O — already open on this PS)
     - Append victim.tables onto survivor.tables
     - Append victim.sst_readers onto survivor.sst_readers
     - Update survivor.rg.end_key = victim.rg.end_key
     - Set survivor.seq_number = max(survivor.seq, victim.seq) + 1
     - Re-evaluate has_overlap (stays 0 by §1.2)
     - Update survivor's part_sc state (extent_ids list invalidation)
  9. Write merged TableLocations checkpoint to survivor.meta_stream:
      vp_head = (new_log_tail_extent_id, 0)
      tables  = survivor.tables ++ victim.tables (concatenated)
      [truncate meta_stream to 1 extent as today]
     This is the LINEARIZATION POINT for the PS-side: any crash before this
     write means recovery on PS restart loads the OLD survivor checkpoint
     and refuses the (already-applied) merge until manual reconciliation.
     Mitigation: this write happens before MergeRelease so any failure
     surfaces to the caller, who retries. F148-A invariant comment inline.
  10. Invalidate survivor's three stream workers: part_sc.invalidate_stream
      on log/row/meta; survivor.need_invalidate_row_stream = true (signals
      P-bulk to refresh its row_stream worker on next FlushReq).
      Victim's stream workers are torn down with the partition in step 11.
  11. Send MergeRelease to victim partition.
      Victim's merged_partition_loop:
        - close req_rx (future requests return Unavailable)
        - close compact/gc/flush channels
        - signal P-bulk thread shutdown
        - exit merged_partition_loop
        - PartitionServer drops the PartitionHandle for victim_id
        - free P-log + P-bulk OS threads (drop runtime)
  12. Return CODE_OK.

  Note: partition_vp_refs sync to manager is NOT a step here — it rides
  the existing periodic background sync (already runs after every flush
  + compaction checkpoint). Manager already wrote the merged refs in
  Phase 2; the PS's next periodic sync confirms them.
```

### 4.2 PS-side metrics export

`merged_partition_loop` already counts requests via existing F024 instrumentation. New per-partition state:

```rust
struct PartitionMetrics {
    req_count_60s: AtomicU64,            // bumped every Put/Get/Delete/etc.
    imm_full_count_60s: AtomicU64,       // bumped when at_imm_cap stalls req_rx
    bytes_size: AtomicU64,                // recomputed on flush + memtable rotate
}
```

A PS-side `metrics_collect_loop` (1 Hz) computes `req_per_sec` / `imm_full_per_sec` as 60 s rolling means and stores them in a `Rc<RefCell<HashMap<part_id, PartitionLoad>>>` consumed by `heartbeat_loop`.

### 4.3 `merged_partition_loop` `MergeFreeze` handling

Extend the F120-C `Drain` arm of the select macro:

```rust
match select(...).await {
    PartitionControl::Drain(ack) => { /* existing */ }
    PartitionControl::MergeFreeze(ack) => {
        // same sequence as Drain BUT do not exit the loop;
        // set partition.frozen_for_merge = true so subsequent req_rx
        // arms reject with Unavailable.
        // After ack, fall back into select but only poll inflight (not req_rx).
    }
    PartitionControl::MergeRelease => { /* exit loop, return */ }
}
```

### 4.4 No changes to `do_compact` / `background_gc_loop` / `flush_one_imm`

By design (the dual-gate ensures merge runs in mutual exclusion with these). Survivor's existing background loops resume normally after merge — they see the spliced `extent_ids` as if it had grown organically.

---

## 5. CLI

`crates/server/src/bin/autumn_client.rs`:

| Command | Behavior |
|---|---|
| `autumn-client merge <SURVIVOR_PART_ID> <VICTIM_PART_ID>` | Calls survivor PS's `MSG_MERGE_PART`. Prints OK or error. |
| `autumn-client policy candidates` | Calls manager's `MSG_GET_POLICY_CANDIDATES`. Prints table. |

**`policy candidates` output format:**

```
KIND   PRIMARY  SECONDARY  REASON                                  SIZE     QPS     IMM/s  FEAS
split  17       -          imm_full_per_sec>10 sustained (20)      8.2 GB   62000   24     yes
split  23       -          size>50 GB hard cap                     53.1 GB  4500    0      yes
merge  31       32         size_sum<1 GB AND qps_sum<5K            456 MB   210     0      yes
merge  44       45         qualifying but on different PS          892 MB   180     0      no  (cross-PS)
```

`FEAS` column: `no` for advisory candidates that auto-trigger (Stage 2/3) cannot act on (cross-PS, has_overlap=1, etc.). Operator can still act manually.

### 5.1 `autumn-client info` extension

Existing `info` output gains a per-partition `last_op_at` line (from `MgrPartitionMeta`).

---

## 6. Staging & Rollout

```
Stage 1 (this commit family — F183):
  ├─ MSG_MULTI_MODIFY_MERGE / MSG_MERGE_PART implemented
  ├─ Policy engine running, advisory only (no auto-trigger)
  ├─ CLI `autumn-client merge` + `autumn-client policy candidates`
  ├─ Heartbeat carries PartitionLoad
  ├─ MgrPartitionMeta.last_op_at populated by split + merge handlers
  └─ Tests + manual repro in README

Stage 2 (F-future-A):  AUTUMN_MGR_AUTO_SPLIT flag (default off → on)
  Gate: ≥30 days Stage-1 burn-in; ≥50 advisory→manual splits/merges
        with 0 data correctness issues.

Stage 3 (F-future-B):  AUTUMN_MGR_AUTO_MERGE flag (default off → on)
  Gate: ≥60 days Stage-2 stable; 0 false-split events (split-then-merge
        same range within 24 h).

Stage 4: both flags default-on.
```

### 6.1 Why split-auto before merge-auto

Documented in `feedback_auto_split_before_merge.md` (auto-memory) and reproduced here:

> autumn-rs is thread-per-core: each partition's writes serialise through a single P-log compio runtime on one OS core. **Merge concentrates two partitions' SSTs + future load onto one core** — a wrongly-merged hot pair degrades immediately at the absolute worst place (the hot path's single-core ceiling). Split is the *relief valve* in TPC; its failure mode is mild (redundant partition, extra metadata). HBase / CRDB / TiDB all enabled auto-split years before auto-merge for the same structural reason.

---

## 7. Crash Recovery

### 7.1 Mid-merge crash semantics (single-txn invariant)

The merge etcd commit is **one** `put_msgs_txn` (F124-style) — atomic.

| Crash window | Outcome on next manager leader |
|---|---|
| Before Phase-2 etcd commit | No state change. Both partitions reopen normally on next `sync_regions_once`. PS-side drain flag is in-memory; cleared by PS process restart. |
| After Phase-2 etcd commit, before Phase-3 in-memory apply | etcd state already merged. Manager replay (`replay_from_etcd`) rebuilds in-memory store with merge applied. PS receives the merged region on its next `region_sync_loop` tick. |
| After Phase-3 fully applied | Same as steady-state — merge complete. |

No persistent `MERGING` marker needed. The single-txn commit is the linearization point.

### 7.2 PS crash mid-merge

| Crash on which PS | Recovery |
|---|---|
| Survivor PS crash before manager commit | Manager commit never fires (PS dropped the call). Both partitions re-open normally on PS restart. Drain flag is in-memory only. |
| Survivor PS crash after manager commit, before PS-side splice | `region_sync_loop` re-fetches the new merged `MgrRegionInfo`. New survivor PS opens partition with merged `extent_ids` from etcd-stored `MgrStreamInfo`; reads merged TableLocations from meta_stream (written in Phase-3 step 10). Catches up. |
| Victim PS crash mid-merge | Survivor PS times out the `MergeFreeze` ack → returns Unavailable to client. Operator retries after victim PS recovers; merge may have actually committed (in which case victim partition is already deleted and the retry returns NotFound, surfacing as a benign "victim does not exist" error). |

### 7.3 Drain consistency

PS-side drain flag (`frozen_for_merge`) is **in-memory only**; not persisted. Justification: a crash mid-drain loses the flag, but the drain's *effects* (flushed imm, sealed log_stream tail) are durable. The only consequence of losing the flag is that the post-crash PS can resume serving the partition normally — which is correct behaviour if the merge didn't commit.

---

## 8. Test Plan

Tests live in `crates/manager/tests/` (integration) and `crates/manager/src/lib.rs` / `crates/partition-server/src/` (unit):

### 8.1 Unit tests (no external deps)

| Test | Asserts |
|---|---|
| `policy_split_size_hard` | Single partition with 5 buckets >50 GB → emits split candidate |
| `policy_split_qps_high_only_above_1gb` | High QPS but size<1 GB → no split |
| `policy_split_cooldown_blocks` | last_op_at within 1h → no split candidate |
| `policy_merge_adjacent_pair_qualifying` | Pair with size_sum<1 GB, qps_sum<5 K, both same_ps → merge candidate feasible=yes |
| `policy_merge_cross_ps_marks_infeasible` | Same as above but different ps_id → feasible=no |
| `policy_merge_has_overlap_blocks` | Either side has_overlap=1 → no candidate |
| `policy_window_drops_old_buckets` | After 31 buckets, oldest dropped |
| `compute_merge_streams_extent_ids_order` | Output order = `[L.sealed]+[V.sealed]+[E_new]` |
| `compute_merge_streams_refs_increment` | victim extents' refs += 1 |
| `merged_partition_vp_refs_sums_correctly` | per-extent sum across two partitions |
| `apply_merge_mutations_deletes_victim_streams` | Phase-3 in-memory state has victim streams gone |

### 8.2 Manager integration tests (need etcd)

| Test (gated `#[ignore]`) | Asserts |
|---|---|
| `merge_basic_etcd_roundtrip` | After merge, etcd has survivor with merged extent_ids; victim partitions/streams keys absent |
| `merge_refuses_non_adjacent` | Phase-1 `Precondition` |
| `merge_refuses_ec_inflight` | F138-pattern guard |
| `merge_refuses_recovery_inflight` | F146-pattern guard |
| `merge_idempotent_on_replay` | Second call with same args returns NotFound for victim |
| `merge_crash_before_phase2_no_state_change` | Inject crash, replay etcd, both partitions still distinct |
| `merge_crash_after_phase2_replays_clean` | Inject crash, replay etcd, survivor has merged state |

### 8.3 PS integration tests

`crates/manager/tests/system_merge.rs` (new file, gated `#[ignore]` per crates/manager/tests convention):

| Test | Behavior |
|---|---|
| `merge_two_drained_partitions_same_ps` | Pre-split a partition into L+V on same PS (use existing `autumn-client split` test helper); write distinct keys to each; flush; merge; verify all keys readable from survivor; victim partition gone from `info`. |
| `merge_then_get_resolves_vp_from_victim_log` | Victim has large-value VPs; after merge, `get` on those keys reads the value from the (now-spliced-into-survivor's-log_stream) extents. |
| `merge_seq_monotonic_after_merge` | Post-merge writes have higher seq than max(L.seq, V.seq); MVCC dedup correct. |
| `merge_refuses_when_either_has_overlap` | Force has_overlap=1 on one side; merge returns Precondition. |
| `merge_concurrent_with_compaction_serialises` | Trigger major compact on victim; merge waits on compact_gate; succeeds after compact finishes. |
| `merge_invalidates_survivor_stream_workers` | Verify cached StreamWorker tail is stale post-merge → would be wrong without invalidation; assert the invalidation clears it. |
| `merge_advisory_then_manual_executes` | Run policy_candidates after writing low-load adjacent pair; observe candidate; run `autumn-client merge`; observe candidate disappears next tick. |

---

## 9. Files Changed

### 9.1 Wire types (2 files)
- `crates/rpc/src/manager_rpc.rs`: add `MSG_MULTI_MODIFY_MERGE`, `MultiModifyMergeReq`, `MultiModifyMergeResp`, `MSG_GET_POLICY_CANDIDATES`, `GetPolicyCandidatesReq/Resp`, `PolicyCandidate`, `MSG_REPORT_PARTITION_LOAD`, `ReportPartitionLoadReq`, `PartitionLoad`. **No** changes to `HeartbeatPsReq` or `MgrPartitionMeta` (rkyv-evolution-safe; metrics + last_op_at via separate RPC + sidecar etcd key respectively).
- `crates/rpc/src/partition_rpc.rs`: add `MSG_MERGE_PART`, `MergePartReq/Resp`.

### 9.2 Manager (4 files)
- `crates/manager/src/policy.rs`: NEW — engine + thresholds.
- `crates/manager/src/lib.rs`: add `compute_merge_streams`, `apply_merge_mutations`, `merged_partition_vp_refs`; add `last_op_at: Rc<RefCell<HashMap<u64, i64>>>` field on `AutumnManager` (loaded from `partitionLastOp/` prefix in `replay_from_etcd`); wire `policy: PolicyEngine` field; spawn `policy_tick_loop`; populate `last_op_at` in split + merge handlers (etcd key + in-memory map).
- `crates/manager/src/rpc_handlers.rs`: add `handle_multi_modify_merge`, `handle_get_policy_candidates`, `handle_report_partition_load`. Extend `handle_multi_modify_split` to write `partitionLastOp/<left_id>` + `partitionLastOp/<right_id>` in its existing single txn.
- `crates/manager/CLAUDE.md`: add note 16 — merge handler pattern + invariants; note `partitionLastOp/` prefix; record split-handler extension to write last_op_at.

### 9.3 Partition server (4 files)
- `crates/partition-server/src/lib.rs`: extend `PartitionRequest` enum with `MergeFreeze` / `MergeRelease`; add `frozen_for_merge: Cell<bool>` + `metrics: PartitionMetrics` to `PartitionData`; extend close-partition path.
- `crates/partition-server/src/rpc_handlers.rs`: add `handle_merge_part`.
- `crates/partition-server/src/background.rs`: extend `merged_partition_loop` select with `MergeFreeze` / `MergeRelease` arms; bump `metrics.req_count_60s` per Put/Get/Delete; bump `metrics.imm_full_count_60s` when at imm cap.
- `crates/partition-server/CLAUDE.md`: add Programming Note 11 — merge handler pattern + dual-gate ordering for two partitions.

### 9.4 Client + CLI (2 files)
- `crates/client/src/lib.rs`: add `merge_partitions` + `policy_candidates` methods on `ClusterClient`.
- `crates/server/src/bin/autumn_client.rs`: add `merge` + `policy candidates` subcommands.

### 9.5 Metrics report plumbing (1 file)
- `crates/partition-server/src/lib.rs`: spawn `report_load_loop` (5 s tick) that snapshots per-partition `metrics` (`size_bytes`, `req_per_sec`, `imm_full_per_sec`, `p99_us`) into `PartitionLoad` Vec and sends `MSG_REPORT_PARTITION_LOAD`. Heartbeat loop unchanged.

### 9.6 Tests + repro (3+ files)
- `crates/manager/src/policy_tests.rs`: NEW unit-test module.
- `crates/manager/src/lib.rs` (test module): merge unit tests as in §8.1.
- `crates/manager/tests/system_merge.rs`: NEW integration tests as in §8.3.

### 9.7 Docs (4 files)
- `README.md`: add "Manual partition merge" section + "Policy candidates" section.
- `feature_list.md`: add F183 entry (umbrella) + F183-A (merge primitive), F183-B (policy advisory).
- `claude-progress.txt`: status update on commit.
- `crates/rpc/CLAUDE.md`: list new message types `0x33` / `0x34` / `0x35` / `0x46`.

---

## 10. Open Questions / Deferred

1. **Cross-PS merge** — depends on partition migration primitive; advisory marks `feasible=no` and operator must co-locate first.
2. **Operator-triggered policy thresholds** — Stage 1 ships hard-coded; if Stage 2 needs per-cluster knobs, add CLI flags on `autumn-manager-server`.
3. **p99 latency in heartbeat** — included in `PartitionLoad` schema but not used by Stage 1 policy (defaults to 0); reserved for tie-breaking when multiple candidates compete.
4. **Stage 2/3 auto-trigger loop** — separate feature commits; manager periodically calls its own `compute_candidates` and dispatches matching `MSG_MULTI_MODIFY_SPLIT` / `MSG_MERGE_PART` itself.

---

## 11. Risks

| Risk | Mitigation |
|---|---|
| `extent_ids` ordering invariant violated by future refactor | Inline `// F183 invariant` comments at splice site; unit test `compute_merge_streams_extent_ids_order` |
| Survivor `sst_readers` doubles → bloom-FP rate doubles → read latency bump | Same shape as post-split has_overlap state; major compaction unifies. Document in PS CLAUDE.md. |
| Long `extent_ids` lists → `MSG_STREAM_INFO` wire bloat | At 100 GB partition, ~37 KB worst case; acceptable. Policy can refuse merging two huge partitions back-to-back via the `MERGE_SIZE_LOW=1 GB` threshold (already ensures both sides are small). |
| Merge-then-immediate-split oscillation | `SPLIT_COOLDOWN=1h`, `MERGE_COOLDOWN=6h`, plus 10× hysteresis between `SPLIT_QPS_HIGH=50K` and `MERGE_QPS_LOW=5K`. |
| Cross-PS merge attempted via direct CLI bypass | `handle_merge_part` returns Precondition with clear message; documented in `autumn-client merge --help`. |
| TPC: merged partition single-core ceiling reached | Auto-merge gated behind feature flag (Stage 3); advisory mode in Stage 1+2 surfaces the concern via the `same_ps` + `qps_sum` columns in `policy candidates`. |

---

**End of design spec.**
