# autumn-manager Crate Guide

## Purpose

The central control-plane service. Serves 18 RPCs via autumn-rpc (custom binary protocol on compio):
- StreamManager (14 RPCs): status, acquire_owner_lock, register_node, create_stream, update_stream_ec, stream_info, extent_info, nodes_info, check_commit_length, stream_alloc_extent, stream_punch_holes, truncate, multi_modify_split, reconcile_extents
- PartitionManager (4 RPCs): register_ps, upsert_partition, get_regions, heartbeat_ps

`update_stream_ec` (MSG_UPDATE_STREAM_EC = 0x32, FOPS-03): mutates `MgrStreamInfo.ec_data_shard / ec_parity_shard` on an existing stream. After the call, the `ec_conversion_dispatch_loop` (fires every 5 s) picks up any sealed extents in the stream and converts them to the new EC shape, allocating extra extent-node slots if K+M > current replica count.

Uses etcd (optional, via autumn-etcd compio-native client) for persistent metadata and leader election. Single-threaded compio runtime (Rc/RefCell, !Send).

## Core Struct: `AutumnManager`

```rust
pub struct AutumnManager {
    store: MetadataStore,           // Rc<RefCell<MetadataState>> — all in-memory cluster state
    leader: Rc<Cell<bool>>,         // are we the current leader?
    etcd: Option<EtcdMirror>,       // optional etcd persistence (autumn-etcd)
    conn_pool: Rc<ConnPool>,        // for extent node RPC calls
    recovery_tasks: Rc<RefCell<HashMap<u64, RecoveryTask>>>,
    // ...
}
```

The `store` (from `autumn-common`) holds everything: streams, extents, nodes, disks, partitions, regions, owner revisions. All mutations must also be mirrored to etcd when `self.etcd.is_some()`.

## RPC Wire Format

All 16 RPCs use rkyv zero-copy serialization over autumn-rpc 10-byte frame headers. Message types 0x20–0x2F defined in `autumn-rpc/src/manager_rpc.rs`. Manager calls to extent nodes use extent_rpc message types (0x01–0x0A) via ConnPool.

## Leader Election

Uses etcd **lease-based leader election**:

1. Creates a lease with 10-second TTL.
2. Attempts a CAS: if the leader key doesn't exist, write `instance_id` with the lease.
3. If successful:
   - Replays all state from etcd (`replay_from_etcd`) to rebuild in-memory state.
   - Sets `leader = true`.
   - Starts a **keepalive loop** (sends keepalive every 2 seconds to maintain lease).
4. If the lease expires or keepalive fails: sets `leader = false` (step down).
5. A background loop retries election every 2 seconds when not leader.

**Without etcd**: runs in memory-only mode (no persistence, no leader election, always "leader").

## Stream Lifecycle

### Create Stream
```
create_stream(data_shard, parity_shard):
  1. alloc_ids(2) → [stream_id, extent_id]
  2. Select first (data_shard + parity_shard) nodes sorted by node_id
  3. Call alloc_extent(extent_id) on each selected node (creates empty files)
  4. Create StreamInfo{stream_id, extent_ids:[extent_id]}
  5. Create ExtentInfo{extent_id, replicates, parity, eversion:0, refs:1}
  6. Mirror to etcd
```

### Seal + Alloc New Extent (`stream_alloc_extent`)
```
  1. Validate owner revision
  2. Query commit_length on all replicas of current tail → take MINIMUM → sealed_length
  3. Update ExtentInfo: sealed_length, bump eversion, set avali=1
  4. alloc_ids(1) → new extent_id
  5. Call alloc_extent on preferred nodes; if a node fails (dead), fall back to other
     registered nodes until enough healthy nodes are found or all are exhausted
  6. Append new extent to stream's extent_ids list
  7. Mirror to etcd
```

### GC: Punch Holes & Truncate
- `stream_punch_holes`: removes specified extent IDs from stream; decrements extent `refs`; deletes ExtentInfo when refs → 0.
- `truncate`: removes all extents before the specified `extent_id` (inclusive exclusive), same ref-counting logic.
- Extents can be shared across partitions (CoW split), so ref counting is critical — never delete an extent with refs > 0.

### F109: Physical extent file deletion (refs → 0)
When the refs→0 path fires inside `handle_stream_punch_holes` /
`handle_truncate`, the manager additionally:
1. **Snapshots** the replica address list (`replicates ++ parity` →
   shard-routed addresses via `Self::shard_addr_for_extent`) **before**
   removing the extent from `s.extents` — done inside the same
   `borrow_mut` block via the explicit `let s: &mut MetadataState =
   &mut guard;` pattern (RefMut auto-deref doesn't preserve disjoint-
   field borrow info, hence the manual deref).
2. After `mirror_stream_extent_mutation` succeeds, hands the snapshot
   to `enqueue_pending_deletes` (extent_delete.rs) which appends to
   `pending_extent_deletes: Rc<RefCell<VecDeque<PendingDelete>>>`.
3. The background `extent_delete_loop` (sweep every 2 s) drains the
   queue and fans out `EXT_MSG_DELETE_EXTENT` over the shared
   `ConnPool` to each replica. Replica addresses ack-by-ack are
   removed from the entry's `pending_addrs`. After 60 failed sweeps
   (≈ 2 min) the entry is dropped and a WARN is logged — orphan
   `.dat`/`.meta` files are reaped on the affected node's next
   startup via `MSG_RECONCILE_EXTENTS`.

Etcd-first ordering is preserved: the queue push happens **after**
`mirror_stream_extent_mutation` returns OK, so a failed mirror never
schedules a stale unlink.

The pending queue is in-memory only. Manager restart loses pending
entries; orphans are then reaped by node-startup reconcile (the
extent-node sends every locally-loaded `extent_id` to the manager
via `MSG_RECONCILE_EXTENTS`; the manager returns the subset that's
no longer in `s.extents`; the node unlinks the corresponding files).
This trade-off is intentional: persisting the queue to etcd would
double the manager's etcd traffic on the GC hot path for limited
benefit, since the reconcile backstop converges on next boot.

## Partition Split: `multi_modify_split`

The most complex operation. Atomically splits one partition into left + right:

```
multi_modify_split(part_id, mid_key, owner_key, revision, log_sealed_len, row_sealed_len, meta_sealed_len):
  1. Validate revision
  2. Validate mid_key is inside partition range
  3. alloc_ids(4) → [new_log_id, new_row_id, new_meta_id, new_part_id]
  4. duplicate_stream(log_stream, log_sealed_len) → new log stream (shares extents)
  5. duplicate_stream(row_stream, row_sealed_len) → new row stream (shares extents)
  6. duplicate_stream(meta_stream, meta_sealed_len) → new meta stream (shares extents)
  7. Left partition: update range to [start_key, mid_key)
  8. Right partition: create with range [mid_key, end_key), new stream IDs
  9. rebalance_regions()
  10. Persist everything to etcd in one transaction
```

### `duplicate_stream(src_stream_id, sealed_length)`
```
  1. Alloc new stream_id
  2. For each extent in src_stream (except tail):
       - Increment extent.refs
       - Add extent_id to new stream
  3. For the tail extent:
       - Set sealed_length = sealed_length (seals it at the split point)
       - Bump eversion
       - Increment refs
       - Add to new stream
  4. Return new stream_id
```

After split, both left and right partitions initially share the same physical extents. Their `PartitionServer` will detect `has_overlap = true` on open (SSTables contain keys outside the narrowed range). Major compaction cleans up out-of-range keys and frees the shared extents via GC.

### VP lifetime after split (`vp_table_refs`, 2026-04-29)

Split can duplicate row-stream SST ownership without duplicating the old log extents referenced by the SSTs' embedded `ValuePointer`s. The direct stream-membership refcount `MgrExtentInfo.refs` is therefore insufficient to protect old log extents after split.

Manager now tracks two independent lifetimes on every extent:

- `refs`: direct membership in some stream's `extent_ids`
- `vp_table_refs`: indirect retention by live SSTables whose `MetaBlock.vp_deps` still mention this extent

The source of truth for indirect retention is `partitionVpRefs/<part_id>` in etcd and `MetadataState.partition_vp_refs` in memory. Each snapshot stores `extent_id -> table_count` for the partition's CURRENT live SST set. The manager updates global `vp_table_refs` by diffing the old snapshot and the new snapshot.

Rules:

1. `MSG_SYNC_PARTITION_VP_REFS` replaces the partition's full snapshot; manager diffs old/new and adjusts `vp_table_refs` on touched extents.
2. `multi_modify_split` clones the parent snapshot to the right child immediately, because both children still reference the shared SST set until compaction rewrites them.
3. Extent deletion is allowed only when `refs == 0 && vp_table_refs == 0`.
4. `vp_table_refs` is manager-owned aggregate state. It must NEVER be written into SST format.

## EC Conversion Dispatch (`ec_conversion_dispatch_loop`)

Background loop that fires every 5 s. Picks any sealed extent on an EC stream where `ec_converted == false`, sends `EXT_MSG_CONVERT_TO_EC` to the coordinator (first replica), and on success calls `apply_ec_conversion_done` to flip `ec_converted = true` + bump `eversion = pre_ec + 1` in the manager + etcd.

### F119-D: candidates dedup for CoW-shared extents

The candidate-collection loop iterates `s.streams.values()` and pushes every sealed-not-converted extent onto a Vec. After a partition split, an extent has `refs >= 2` and appears in **both** child streams' `extent_ids`. Without dedup, the same extent_id ended up twice in `candidates`. The first iteration's `convert_to_ec` correctly encoded the original payload into K data + M parity shards (each `shard_size(original, K) ≈ original / K`); the second iteration then read each replica's local file (which had been shrunk to `shard_size` by the first round's `write_shard_local`) and passed it back through `ec_encode` as if it were the original payload, producing **sub-shards** of size `shard_size(shard_size(original), K) ≈ original / K²`.

The manager state ended up looking correct (`ec_converted=true`, `sealed_length=original_payload`, `eversion=pre_ec+1`), but the on-disk shards only encoded `original / K` bytes. Every read past `shard_size` returned short data and surfaced upstream as `logStream value short: need N got M` (cross-shard VP) or `ec_read_full_and_slice: offset N past decoded payload len M` (SST recovery on partition open).

Fix: dedup candidates by `extent_id` via `HashSet`. Per-stream `(ec_data_shard, ec_parity_shard)` are identical across CoW-shared streams by construction (`compute_duplicate_stream` clones them), so the first-seen entry's stream is sufficient.

Defense-in-depth on the coordinator (`extent_node.rs::handle_convert_to_ec`): if `entry.eversion >= req.eversion && entry.sealed_length > 0 && entry.avali > 0`, the extent has already been converted at this eversion — return CODE_OK without re-encoding. This makes `convert_to_ec` idempotent for any future bug that re-dispatches a converted extent.

## Recovery System

### Dispatch Loop (every 2 seconds)
Scans all sealed extents. For each replica slot:
- **Per-disk health check first**: looks up the disk_id from `replicate_disks`/`parity_disks`, checks `store.disks[disk_id].online`. If offline, immediately dispatch recovery (matches Go's `routineDispatchTask` pattern).
- Probes with `commit_length` RPC (or `re_avali` for known lagging replicas).
- If the node doesn't respond or returns an error: dispatch `require_recovery` to a healthy candidate node.
- Tracks in-flight recoveries in `recovery_tasks` to avoid double-dispatching.

### Disk Status Update Loop (every 10 seconds)
Polls all registered extent nodes via `df` RPC to update per-disk online status in `store.disks`. Matches Go's `routineUpdateDF`. Disk status is also updated opportunistically in the collect loop when polling for recovery task completion.

### Collect Loop (every 2 seconds)
Polls all registered nodes with the `df` RPC. The response includes completed recovery tasks. For each completion:
- Calls `apply_recovery_done`: replaces the failed node_id with the recovery node_id in `ExtentInfo.replicates`, increments eversion, marks slot as available.
- Mirrors updated ExtentInfo to etcd.

## Partition Assignment: `rebalance_regions`

Least-loaded allocation: for each partition, keep the existing PS if it is still registered (always refreshing `rg` from the current `PartitionMeta`); otherwise assign to the PS with the fewest current partitions. Called after `register_ps`, `upsert_partition`, and `multi_modify_split`.

The `rg` refresh on keep is critical: after a split, `multi_modify_split` updates the left partition's key range and calls `rebalance_regions`. Without refreshing `rg`, `GetRegions` would return the stale pre-split range to partition servers.

## PS Liveness Detection

`AutumnManager` tracks `ps_last_heartbeat: Arc<Mutex<HashMap<u64, Instant>>>` (ephemeral, not persisted to etcd).

- **`register_ps`** records an initial timestamp so the PS isn't immediately evicted.
- **`heartbeat_ps` RPC**: PS calls this every 2s to update its timestamp (F069 cadence).
- **`ps_liveness_check_loop`** (background, 2s interval, F069): if a PS hasn't heartbeated in 10s, it is removed from `ps_nodes`, `rebalance_regions` is called, and the updated state is mirrored to etcd.

The partition server side sends heartbeats from a `heartbeat_loop` spawned in `finish_connect` (F111: previously spawned in `serve()`, but `serve()` only runs after the initial `sync_regions_once` finishes opening every assigned partition; with hundreds of MiB of WAL replay per partition that exceeds the 10s eviction window). It also polls `GetRegions` every 2s via `region_sync_loop` to pick up reassignments.

### F111: surface eviction via `CODE_NOT_FOUND`

`handle_heartbeat_ps` returns `CODE_NOT_FOUND` (with `"ps {id} not registered"`) when the heartbeat's `ps_id` isn't in `ps_nodes`. Pre-F111 the handler silently returned `CODE_OK`, so a PS evicted by a transient hiccup never knew to re-register and stayed invisible to clients (`ps=unknown` in `info` output) until the next process restart. The PS-side `heartbeat_loop` reacts to `NOT_FOUND` by re-running `register_ps` + `sync_regions_once`, which restores the assignment via `rebalance_regions` (existing `r.ps_id` is preserved when the PS comes back into `ps_nodes`).

## Etcd Mirroring

All persistent state is mirrored to etcd under prefixes:
- `nodes/`, `disks/`, `streams/`, `extents/`, `partitions/`, `partitionVpRefs/`, `regions/`, `ps_nodes/`, `next_id`

On leader promotion, `replay_from_etcd` reads all prefixes to rebuild in-memory state. The etcd transaction in `multi_modify_split` groups all related writes/deletes atomically.

## Programming Notes

1. **Etcd-first mutation pattern** — all mutating RPC handlers follow: (1) compute mutations without modifying store, (2) persist to etcd, (3) apply to in-memory store. This ensures manager crash after step 1 but before step 2 leaves etcd and memory consistent. Exception: `register_ps`/`upsert_partition` apply to memory first because `mirror_partition_snapshot` reads from the store (these are idempotent on retry). The old function `duplicate_stream` (which modified state directly) has been replaced by `compute_duplicate_stream` (read-only) + `apply_split_mutations`. **F152 closed the last three handlers that violated this rule:** `handle_create_stream`, `handle_update_stream_ec`, and `handle_register_node` (both re-registration and new-node branches) all moved their `s.streams.insert / s.extents.insert / s.nodes.insert / s.disks.insert` calls to AFTER the corresponding `mirror_*` await. F125 had previously closed the same anti-pattern in `handle_stream_alloc_extent`. Any future RPC handler that mutates persistent state must follow this order.

2. **`compute_duplicate_stream` increments extent `refs`** — this is only the direct stream-membership refcount for CoW. If shared SSTs can retain old log extents via `ValuePointer`, update `partition_vp_refs` / `vp_table_refs` too. Physical extent deletion requires BOTH counters to reach zero.

3. **Owner revision must be validated before any stream mutation** — call `ensure_owner_revision` at the start of `stream_alloc_extent`, `stream_punch_holes`, `truncate`, `multi_modify_split`. Missing this allows split-brain.

4. **Leader check** — some RPCs should only execute when `self.leader.load()` is true. Writes to etcd from a non-leader will fail (etcd lease is expired), which will surface as an error.

5. **`alloc_ids` is the only ID source** — never generate IDs any other way. The `next_id` is derived from `max(all_entity_ids) + 1` during `replay_from_etcd`, so wasted IDs from failed mutations are safe.

6. **Rebalance is called eagerly** — `rebalance_regions` after every PS registration or partition upsert. This is safe because it's idempotent (keeps existing assignments, only changes unassigned ones).

7. **F121 disk-online tracking is call-result-driven, NOT
   payload-driven.** `disk_status_update_loop` and the `df` poll
   inside `recovery_collect_loop` use the helpers
   `mark_node_disks_offline(store, node)` on RPC error and
   `mark_node_disks_online(store, node)` on success. Both key on
   `MgrNodeInfo.disks` (manager-allocated `disk_id`s). The
   per-disk-id status carried in `DfResp.disk_status` is **the
   extent-node's local `disk_id`** (set via `--disk-id N` at
   process launch) which is unrelated to the manager's allocated
   `disk_id` — pre-F121 the success path tried `s.disks.get_mut(&local_id)`
   and silently no-op'd, so once a disk was marked offline (by my
   F121 mark-on-failure addition), the success path could never
   flip it back. The simple fix: trust the call-level liveness
   signal, ignore the response payload's per-disk online field.
   Per-disk failure inside an extent-node is still surfaced by
   `mark_disk_offline_for_extent` (`crates/stream/src/extent_node.rs:1293`)
   and propagates via the dedicated recovery RPCs.

8. **F121 `select_nodes` prefers nodes with at least one online
   disk**, falling back to the full set when too few healthy
   candidates remain. The fall-back exists because a cold leader
   that hasn't yet run its first `df` sweep would otherwise refuse
   to allocate. The per-RPC fall-back inside
   `handle_stream_alloc_extent` (which retries on a fresh node when
   `alloc_extent_on_node` fails) remains the load-bearing layer —
   F121's `select_nodes` change just narrows the candidate set in
   the common case so the user's expected behaviour
   (`stop-node 1` → new extent on `[3, 5, 7]`) is observable on the
   very first allocation attempt instead of only after a fall-back
   hop.

9. **F144 `select_nodes` shuffles the candidate set instead of
   sorting by `node_id`.** Pre-F144 a 4-node cluster `{1,3,5,7}`
   placed every 3-replica extent on `[1, 3, 5]` because the function
   sorted ascending and returned `take(count)`. The same bias also
   lived in two adjacent paths: `recovery.rs`'s EC-conversion
   extra-parity selection (`HashMap.values().take(extra)` —
   deterministic per process), and `rpc_handlers.rs`'s
   `handle_stream_alloc_extent` fall-back iterator (sorted by
   `node_id` before walking). All three sites now `shuffle` then
   `take`. The "online disk" filter from F121 is preserved, so
   degraded clusters still avoid known-dead peers in the common
   case. Capacity-aware selection (least-allocated) is intentionally
   deferred — it requires a per-node extent counter persisted in
   etcd; the random pick is the minimum change that fixes the
   observed concentration on `{1,3,5}` and is sufficient for
   uniform load over the long run.

    **F153 closes the failover-failure mode of this guard.** F138's
    `ec_conversion_inflight` set is purely in-memory; on leader failover it is
    lost. The new leader's `ec_conversion_dispatch_loop` (5 s tick) cannot see
    that a deposed leader had a conversion in flight, and re-fires
    `EXT_MSG_CONVERT_TO_EC` for the same extent. The coordinator-side F119-D
    guard fires post-hoc (after the eversion bump in `commit_shard_local`), so
    during the deposed leader's mid-`spawn_blocking ec_encode` window the new
    leader's dispatch passes the guard and races on `.ec.dat`. F153 adds a
    per-extent `Rc<futures::lock::Mutex<()>>` on the extent-node coordinator
    (`crates/stream/src/extent_node.rs::handle_convert_to_ec`) so a duplicate
    dispatch is serialised — the second one re-runs F119-D under the lock and
    exits as a no-op. Defense-in-depth: F138's manager-side guard remains the
    primary mechanism in steady-state; F153 closes the failover hole.

10. **F138 `ec_conversion_inflight` extends to an eversion-bump lock.**
    Before F138, `ec_conversion_inflight` only (i) prevented double EC
    dispatch and (ii) inhibited physical extent deletion. F138 extends
    the lock's meaning: while extent X ∈ `ec_conversion_inflight`, no
    other task may bump `ex.eversion` or rewrite `ex.replicates` on X.
    The race: `ec_conversion_dispatch_loop` captures `new_eversion =
    ex.eversion + 1` before the `EXT_MSG_CONVERT_TO_EC` await; if
    `apply_recovery_done`, `mark_extent_available`, or
    `handle_multi_modify_split` bump eversion during the await,
    `apply_ec_conversion_done`'s unconditional `ex.eversion =
    new_eversion` overwrites the intermediate bump, and its
    `ex.replicates = target_nodes[..data_shards]` silently reverts a
    recovery's slot replacement. Fix: (a) the `ec_conversion_inflight`
    `.remove` is now deferred until AFTER `apply_ec_conversion_done`
    completes; (b) `apply_recovery_done`, `mark_extent_available`, and
    `handle_multi_modify_split` check `ec_conversion_inflight` and
    return `Err(Precondition(...))` if set — retried on the next 2 s
    dispatch tick or client retry. Symmetric to F136's pre-existing
    guard (EC checks `recovery_tasks` before dispatch).

11. **F139 delete vs in-flight recovery symmetric exclusion.**
    Two race subwindows close here with the same in-flight-set pattern
    used by F138:
    - **Resurrection race**: `ensure_extent` (called by `run_recovery_task`)
      auto-creates with `OpenOptions::create(true)` on miss; if delete
      has already unlinked the file, it silently resurrects an orphan
      on disk with no manager record.
    - **Write-to-unlinked-inode race**: recovery holds `Rc<ExtentEntry>`;
      delete unlinks the path; recovery writes to the open (unlinked)
      fd; data evaporates on fd close.
    Four changes close both subwindows:
    (a) `dispatch_recovery_task` (`recovery.rs`) skips dispatch when the
      extent appears in `pending_extent_deletes` — the 2 s retry tick
      will naturally find the extent gone from `s.extents` once the
      queue drains.
    (b) `handle_stream_punch_holes` (`rpc_handlers.rs`) returns
      `Err(Precondition)` when any to-be-removed extent (refs→0) is
      present in `recovery_tasks` — PS GC retry backs off until recovery
      completes.
    (c) `handle_truncate` (`rpc_handlers.rs`) symmetric guard to (b).
    (d) `apply_recovery_done` (`recovery.rs`) None-branch (extent no
      longer in manager store) enqueues a targeted `PendingDelete` for
      the recovering node so any resurrected on-disk files are reaped
      immediately, not waiting for the 5-min orphan-reconcile sweep.
    Belt-and-braces: extent-node's `handle_delete_extent`
    (`extent_node.rs`) returns `CODE_PRECONDITION` when
    `recovery_inflight.contains_key(&extent_id)`; `extent_delete_loop`
    retries up to 60 × 2 s = 2 min, which exceeds typical recovery
    duration. This covers manager leader-failover where
    `pending_extent_deletes` is lost in-memory but `recovery_inflight`
    survives on the extent-node process.

12. **F145 completes the F138 eversion-bump lock across all five mutators.**
    F138 (note 10) declared: "while extent X ∈ `ec_conversion_inflight`,
    no other task may bump `ex.eversion`." F138 covered
    `apply_recovery_done`, `mark_extent_available`, and
    `handle_multi_modify_split`. **F145 covers the two missing mutators:**
    `handle_stream_punch_holes` and `handle_truncate`. Both handlers now
    return `Err(Precondition)` (mirroring the F139 pattern from note 11)
    if any to-be-removed extent is in `ec_conversion_inflight`. The
    violation was that both handlers' mutation loops unconditionally ran
    `extent.eversion += 1` for ec-inflight extents (refs<=1 else-branch
    and refs>1 branch), so `apply_ec_conversion_done`'s overwrite of
    `ex.eversion = new_eversion` silently lost the intermediate bump and
    its `ex.replicates = target_nodes` reverted a punch_holes-driven
    state update. PS GC retry (same `Precondition` path as F139) backs
    off until EC completes (typically seconds).

13. **F146 adds refuse-at-start + verify-at-apply to the two remaining
    snapshot-capture-then-await handlers.**

    **`handle_stream_alloc_extent`** (HIGH-1): The handler snapshots the
    tail extent under `borrow_mut`, then awaits `commit_length_on_node`,
    `alloc_extent_on_node`, and `mirror_stream_alloc_extent` before
    writing back via `s.extents.insert(tail_id, tail.clone())`. During
    any of those awaits, a concurrent mutator (recovery_done,
    ec_conversion_done, punch_holes, truncate, split) could bump
    `tail.eversion` and rewrite `tail.replicates` — the writeback would
    then silently overwrite those changes. F146 adds two defenses:
    (a) **Refuse-at-start**: if tail is in `ec_conversion_inflight` or
      `recovery_tasks`, return `Err(Precondition)` immediately, before
      any await.
    (b) **Verify-at-apply**: after the etcd mirror returns, re-read
      `s.extents[tail_id].eversion` under a fresh `borrow_mut` and
      compare against the pre-await snapshot. If they differ, another
      mutator ran during the await — refuse with `Precondition` rather
      than stomping live state. The orphan stale etcd revision is benign:
      failover replay reads the latest revision per key, which the
      client's retry will produce.

    **`handle_multi_modify_split`** (HIGH-2): The F138 guard at Phase-1
    already refuses when any source-stream extent is in
    `ec_conversion_inflight`. F146 adds the symmetric `recovery_tasks`
    check (same loop, same Precondition return). Additionally, Phase-3
    now captures `pre_bump_eversion: HashMap<u64, u64>` in Phase-1 and
    verifies each source-stream extent's live eversion matches before
    calling `apply_split_mutations` — same verify-at-apply pattern as
    alloc_extent above.

    **Deferred**: a heavier `alloc_extent_inflight` / `split_inflight`
    set scheme would provide a mutual-exclusion lock across the entire
    await window for the narrower dispatch-during-our-await sub-race. The
    verify-at-apply approach closes the window for the cases where the
    other mutator actually changes eversion (all current mutators do).
    Cross-reference: notes 10 (F138), 11 (F139), 12 (F145).

14. **F147-A `handle_sync_partition_vp_refs` refuse-at-start + verify-at-apply.**
    `MSG_SYNC_PARTITION_VP_REFS` replaces a partition's full VP-ref snapshot and
    diffs the old/new snapshots to adjust `vp_table_refs` on each touched extent.
    The handler snapshots the old entry under `borrow_mut`, then may await etcd
    persistence (`mirror_partition_vp_refs`) before applying the new diff.
    During that await, a concurrent `apply_recovery_done` or
    `apply_ec_conversion_done` could bump `ex.eversion` or rewrite
    `ex.replicates` on a touched extent; the handler's subsequent
    `vp_table_refs` adjustment would still complete, but the diff-apply
    diverges between the etcd-persisted state and in-memory state if the
    manager crashes mid-handler and replays with a stale etcd entry.
    F147-A adds the same two-stage pattern used by notes 12/13 (F145/F146):
    (a) **Refuse-at-start**: if any extent mentioned in the new snapshot
      is currently in `ec_conversion_inflight` or `recovery_tasks`, return
      `Err(Precondition)` before any await — the PS retries on the next
      flush cycle.
    (b) **Verify-at-apply**: after the etcd mirror returns, re-read each
      touched extent's `eversion` under a fresh `borrow_mut` and compare
      against the pre-await snapshot. If any eversion changed, return
      `Err(Precondition)` rather than applying a stale diff; the etcd
      write is benign (failover replay sees the latest revision per key,
      and the PS retry produces a fresh correct snapshot).
    Cross-reference: notes 10 (F138), 11 (F139), 12 (F145), 13 (F146).

15. **F149 leader-fence on every manager etcd write txn.**
    F005 already runs lease-based leader election + a 2 s keepalive on a
    10 s lease. The window between (a) the etcd lease expiring and (b)
    the deposed leader's keepalive task observing failure can stretch
    arbitrarily long under compio runtime starvation, GC pauses, or
    syscall hangs. During that window the deposed leader still believes
    `self.leader.get() == true` and happily issues etcd writes against
    keys (`streams/`, `extents/`, `partitions/`, `partitionVpRefs/`,
    `regions/`, `nodes/`, `disks/`, `psNodes/`, `recoveryTasks/`,
    `ownerLocks/`) that the new leader has already begun overwriting.
    Etcd CAS by itself does not protect us — these mirror_* paths use
    plain puts, not version-conditional CAS, so the deposed write is a
    bare last-writer-wins overwrite that can revert a freshly-applied
    recovery slot replacement, an EC conversion eversion bump, a split
    snapshot, etc.

    F149 closes this by making **every** manager → etcd write txn fenced
    on the value of the leader-key:

      compare prepended:
        Cmp::value("autumn-rs/stream-manager/leader") == self.instance_id

    If the fence holds, the txn applies as before. If the fence fails
    (someone else's `instance_id` is now in the leader-key, or the
    leader-key has been deleted entirely), the helper:
    - flips the in-process `leader: Rc<Cell<bool>>` to `false` so
      `ensure_leader()` short-circuits subsequent mutating RPCs without
      another etcd round-trip;
    - returns `AppError::NotLeader`, which the RPC handler translates
      into `CODE_NOT_LEADER` so the client retries against whoever etcd
      currently lists as leader.

    Implementation sits on `EtcdMirror`:

      - `EtcdMirror` carries `instance_id: Rc<String>` and
        `leader: Rc<Cell<bool>>` — both shared with `AutumnManager`.
      - `txn_fenced(extra_cmp, success, failure)` always prepends the
        fence compare, then runs the etcd txn. On `succeeded == false`
        it issues a follow-up GET on the leader-key to distinguish:
        * fence held but `extra_cmp` failed — return `Ok(false)` (this
          is the normal CAS-fail path used by the create_revision
          guards on `ownerLocks/` and `recoveryTasks/`);
        * fence broke — set leader=false + return `NotLeader`.
      - `put_msgs_txn` and `put_and_delete_txn` are thin wrappers around
        `txn_fenced(vec![], …)` and bubble `NotLeader` up.

    Five call paths route through this:
      1. all 9 `mirror_*` helpers (lib.rs ~1218–1431);
      2. `persist_extent` (lib.rs ~1217);
      3. `acquire_owner_revision` (lib.rs ~665) — extra_cmp is
         create_revision==0 for the owner-lock CAS;
      4. `dispatch_recovery_task` (recovery.rs ~107) — extra_cmp is
         create_revision==0 for the recoveryTasks/$id CAS;
      5. `handle_multi_modify_split`'s consolidated Phase-2 txn
         (rpc_handlers.rs ~1533).

    NOT fenced (intentionally):
      - `try_become_leader` — this txn IS the operation that establishes
        ownership of the leader-key; cannot fence on owning what we are
        about to acquire.
      - `replay_from_etcd` — read-only; fence has no semantics for GETs.
      - `leader_keepalive_loop` — pure lease keep-alive RPC; no k/v
        write.

    Cross-reference: F005 (lease-based leader election); F079
    (multi-manager failover for StreamClient + PS); test
    `crates/manager/tests/f149_leader_fence.rs` (gated on embedded
    etcd, marked `#[ignore]` per repo convention).


16. **F183 partition merge handler.** `handle_multi_modify_merge` is
    the inverse of `handle_multi_modify_split`. Pattern matches the
    F124 single-txn + F138/F145/F146 inflight checks + F149 fence.

    **Pure-fn helpers (lib.rs):**
    - `compute_merge_streams` — log_stream splice with `[L]+[V]+[E_new]`
      ordering. **Order invariant** is load-bearing for vp_head replay
      correctness; tested by `f181_compute_merge_streams_extent_ids_order_and_refs`.
    - `splice_streams_without_new_tail` — row + meta splice (no fresh tail).
    - `merged_partition_vp_refs` — per-extent sum of two partitions' VP refs.
    - `apply_merge_mutations` — in-memory applier; mirror of `apply_split_mutations`.

    **Phases (rpc_handlers.rs):**
    - Phase 1 (no awaits): F138/F145/F146 inflight checks, adjacency check,
      alloc_ids(1) + select_nodes for `E_new`, splice + VP-refs computation,
      eversion snapshot.
    - Phase 1.5 (await): `alloc_extent_on_node` per replica with F144-style
      shuffled fallback walk.
    - Phase 2 (etcd): single fenced `put_and_delete_txn` containing all
      puts + victim deletes (F124 atomicity).
    - Phase 3 (no awaits): F146 verify-at-apply on `pre_bump_eversion`,
      `apply_merge_mutations`, in-memory `last_op_at` update.

    **`partitionLastOp/<part_id>` sidecar etcd prefix** stores the last
    split or merge timestamp per partition (i64 unix-epoch LE). Loaded
    by `replay_from_etcd` into `AutumnManager.last_op_at`. Both split
    (F183-C2) and merge handlers write entries in their atomic txn.

    **Policy engine (policy.rs).** `policy_tick_loop` ticks every 60 s on
    the leader, reads per-partition load metrics from
    `MSG_REPORT_PARTITION_LOAD` aggregations, computes split/merge
    candidates over a 30-min sliding window with the thresholds in
    `policy.rs`, exposes via `MSG_GET_POLICY_CANDIDATES`. Stage 1 is
    advisory only.

17. **F185 orchestrated merge — `handle_merge_partitions`.** Wraps the
    F183 atomic merge txn with a TiKV-PrepareMerge-style freeze-drain
    sequence so writes that would otherwise race the FLUSH→commit
    window (the F184-K ~5% loss window) are halted at the source.

    Sequence:
      1. `ensure_leader` — manager state must be authoritative.
      2. Resolve `part_addr` + stream ids for both partitions in one
         borrow of `store.inner`.
      3. Acquire admin owner-lock keyed on the partition pair (so
         concurrent merge attempts targeting the same survivor
         serialize on the manager).
      4. `MSG_MERGE_FREEZE { freeze: true }` to victim PS — drains
         pending+inflight, flushes every imm, halts new writes with
         `CODE_UNAVAILABLE`. Returns OK only after the post-freeze
         checkpoint is durable.
      5. Same to survivor PS.
      6. Capture `commit_length` × 6 (3 streams × 2 partitions) by
         delegating to `handle_check_commit_length`. Race-free now
         that both PSes are frozen.
      7. Call `handle_multi_modify_merge` synchronously — its single
         `put_and_delete_txn` is the linearization point.
      8a. On OK: do NOT explicitly unfreeze. `region_sync_loop` on
          each PS observes the new (rg, stream_ids) on next ~2 s tick
          (F184-B), drops the frozen `PartitionData`, reopens the
          survivor with the merged state — natural unfreeze.
      8b. On error: best-effort `MSG_MERGE_FREEZE { freeze: false }`
          rollback to anyone we already froze. PS-side `FREEZE_TTL`
          (30 s) is the final backstop.

    Why this doesn't need a procedure-WAL (HBase ProcedureV2 style):
    the only crash window we have to cover is "manager crashed
    between the freeze RPC and the etcd commit" — sub-second on the
    happy path. TTL bounds worst-case freeze to 30 s, far below
    "frozen forever until PS restart". If we ever need cross-PS merge
    or merge frequency goes up, upgrade to a
    `mergeInProgress/<survivor>:<victim>` etcd marker written before
    freeze and deleted by the success-path txn; leader-promotion
    replay scans the prefix and decides unfreeze (rollback) or commit
    (continue) based on whether the partition deletion is already
    persisted. ~200 lines, ProcedureV2 in miniature. Recorded as
    deferred follow-up in `feature_list.md` F185.

18. **F187 maintenance advisory — `PolicyEngine::compute_maintenance_advisory`.**
    Symmetric to F183's `compute_candidates` for split/merge, but for the
    GC + compact loops: surfaces per-partition `gc_debt_bytes` /
    `pending_compaction_bytes` reported by the PS via the existing
    `MSG_REPORT_PARTITION_LOAD` (F183 wire, +6 fields), runs the same
    `required_buckets`-of-`bucket_sec` sliding-window check, gates by a
    per-kind cooldown (`gc_cooldown_sec` / `compact_cooldown_sec`,
    default 300 s), and skips a partition when its corresponding
    `*_inflight` flag is 1.

    Emits `POLICY_KIND_GC` (= 2) / `POLICY_KIND_MAJOR_COMPACT` (= 3,
    renamed from `POLICY_KIND_COMPACT` in F202 with #[deprecated] alias)
    candidates appended to the same `advisory_cache` returned by
    `MSG_GET_POLICY_CANDIDATES`. `policy_tick_loop` (lib.rs ~520) calls
    `compute_maintenance_advisory(now)` after `compute_candidates(...)`
    each tick, then OVERWRITES `advisory_cache` with the union — so the
    cache always carries the freshest 7-kind set (after F202:
    split / merge / gc / major / minor / ec / hotcold). The handler at
    `rpc_handlers.rs:2327` reads the cache untouched.

    **F202 — extended to 6 actionable advisory kinds + filters.**
    Stage 2 of the mechanism/policy separation refactor (plan
    `~/.claude/plans/elegant-tumbling-pumpkin.md`). Adds:

    - `POLICY_KIND_MINOR_COMPACT = 5`: third arm inside
      `compute_maintenance_advisory`. Gated by sustained
      `minor_compact_pending_bytes > MINOR_COMPACT_PENDING_HIGH`
      (default 512 MiB) AND non-empty pickup_tables in the latest
      bucket (common-sense filter) AND outside
      `minor_compact_cooldown_sec` (default 120 s — shorter than
      major because minor is cheaper).
    - `POLICY_KIND_EC = 6`: new `compute_ec_advisory(state, now)`.
      Iterates `state.streams + state.extents` directly (EC is
      per-extent, not bucketed). Filters: stream has EC policy
      (`ec_data_shard > 0`), extent is sealed, not converted,
      `sealed_length >= ec_min_extent_bytes` (default 64 MiB —
      common-sense filter against negative-EV EC conversions on
      small extents — below threshold the encode + K+M shard
      fanout + metadata churn outweighs the
      3 → K/(K+M) replication savings).
    - `PartitionLoad` extended with 5 fields:
      `sst_tombstone_bytes / sst_expired_bytes /
      sst_out_of_range_bytes / minor_compact_pending_bytes /
      sealed_log_extent_count`. PS-side `refresh_f202_metrics`
      populates the first 4 (the 5th left at 0 for Stage 2 —
      needs a PS-cached `get_stream_info` result). The advisory
      layer treats 0 in any dimension as "no signal".
    - `PolicyConfig` extended with `minor_compact_pending_high /
      minor_compact_cooldown_sec / ec_min_extent_bytes`, all
      runtime-tunable via `set_policy_config`.

    **F203 — end of the mechanism/policy refactor.** The manager is
    now PURE mechanism for operational decisions:

    - `auto_split_enabled` / `auto_merge_enabled` Cells deleted.
    - `set_auto_split` / `set_auto_merge` setters deleted.
    - `policy_tick_loop`'s auto-dispatch arm deleted. The loop still
      builds `advisory_cache` from all 4 compute_*_advisory helpers;
      that's its only job.
    - `ec_conversion_dispatch_loop` is **drain-only**: candidates
      come from `pending_ec_dispatch` keys, not from a fresh
      `s.streams` scan. New EC conversions enter via
      `MSG_FORCE_EC_CONVERT` (handler validates + persists rich
      marker + relies on the next tick to fire through the F198
      replay path). Leader-failover replay (`replay_from_etcd`
      rehydrating markers) works identically.
    - `--auto-split` / `--auto-merge` CLI flags removed from
      `autumn-manager-server`. Passing them now exits with a
      migration message.

    What stays in-kernel (correctness must-cleanup):
    - has_overlap-major / expiry-major / size-tiered minor compact
      (all on the PS side, mechanism-level).
    - F198 marker replay (etcd persistence for in-flight EC convert
      across failover — not optional).
    - All F138/F139/F145/F146/F147/F149 correctness mutex/fence
      guards.
    - `auto_dispatch_split` / `auto_dispatch_merge` helpers stay as
      mechanism layer; tests + the new `MSG_FORCE_EC_CONVERT` /
      `MSG_GET_PARTITION_DETAIL` RPCs are the external-policy
      surface.

    See `README.md` for the OP-driven workflow + cron + bash MVP
    controller example.

    **Stage 1 only** — advisory is purely informational. `last_op_at`
    and `auto_dispatch_*` paths are NOT touched (those would be Stage
    2/3 territory: a PS-local priority maintenance scheduler + shared
    fg/bg token bucket, mirroring how F184 added the `--auto-split`
    flag on top of F183's advisory). Manager-driven maintenance
    scheduling is deliberately NOT planned: GC/compact are local
    concerns (per-partition state, per-PS resources), unlike split /
    merge where range reassignment is inherently global. The advisory
    layer is the only manager involvement we want.

    `PolicyConfig` runtime-tunable via `set_policy_config` carries the
    new thresholds + cooldowns (defaults: 1 GiB / 4 GiB / 300 s / 300 s).
    Tests in `policy_tests.rs` (7 new + 11 existing = 18 passing) cover
    the trigger / cooldown / inflight / partial-window cases.

19. **F198 rich `ecConversionInflight/<id>` marker.** F173 persisted the
    EC-conversion inflight marker to etcd to preserve F138's eversion-bump
    lock across leader failover, but the marker had an EMPTY value and the
    `ec_conversion_dispatch_loop` body's `if ec_conversion_inflight.contains
    (&extent_id) { continue; }` permanently skipped replay-loaded markers.
    Result: after a crash mid-`apply_ec_conversion_done` (manager-side
    etcd commit didn't run, extent-node-side `commit_shard_local` already
    bumped local eversion), the manager's etcd state stayed pre-EC
    forever — every PS read against that extent surfaced the
    eversion-mismatch with no convergence path.

    F198 widens the marker to a rkyv-encoded `MgrEcDispatchInflight {
    extent_id, target_nodes, extra_disk_ids, data_shards, new_eversion }`.
    Replay decodes the value into `pending_ec_dispatch: Rc<RefCell<HashMap
    <u64, MgrEcDispatchInflight>>>` ALONGSIDE the existing lock-set; the
    dispatch loop checks `pending_ec_dispatch.get(&extent_id)` BEFORE the
    shuffle/`alloc_extent_on_node` path and reuses the persisted
    assignment exactly. The old skip is preserved (gated on
    `replay_params.is_none()`) so concurrent-dispatch semantics within a
    single process don't change.

    Why we can't just remove the skip: a naive re-dispatch with a fresh
    `shuffle().take(extra_needed)` could pick a different parity node
    than the original. Calling `alloc_extent_on_node` on a node that
    already received shard data RESETS that node's in-memory ExtentEntry
    (eversion=1, sealed=0) and overwrites the .meta sidecar, then
    `apply_ec_conversion_done` writes the new random parity to etcd —
    silently corrupting EC layout. F198's rich marker eliminates the
    randomness on re-dispatch.

    Companion: `ec_conversion_dispatch_loop`'s first-tick delay was
    reduced from 5 s → 500 ms so post-restart convergence is fast
    enough that the PS-side `recover_partition` retry budget covers it.
    PS side: `recover_partition`'s SST read now retries 30 × 1 s on
    `eversion mismatch` so the operator's first `cluster.sh restart`
    succeeds — pre-F198 they had to manually restart the PS a second
    time after the manager finished re-dispatching (and even then,
    re-dispatch was the no-op skip path so the first restart would
    fail too).
