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
  9. rebalance_regions()  ← also bumps left's region_epoch (rg changed)
                            and seeds right's region_epoch = 1 (new partition)
  10. Persist everything to etcd in one transaction
```

### `region_epoch` bumping (TiKV-style)

`MgrRegionInfo` carries a monotonic `region_epoch: u64`. The
manager bumps it whenever it rewrites a region's `rg`. Both
`rebalance_regions` (in-memory shadow) and
`compute_region_for_partition` (etcd-bound writer) route the bump
through the same helper `next_region_epoch(state, part_id, new_rg)`:

- No prior region → epoch = 1 (bootstrap; `0` is reserved on the wire
  as "skip check").
- rg byte-for-byte unchanged → epoch unchanged (idempotent rebalance,
  PS reassignment without range change).
- rg changed → epoch += 1.

Effect on the wire: SDKs stamp the cached epoch on every Put / Get /
Delete / Head / Range / StreamPut request; the PS rejects with
`StatusCode::FailedPrecondition` on mismatch (`enqueue_*`,
`handle_get/head/range`); SDK refreshes + retries. See the rpc and
partition-server CLAUDE.md for the wire details.

**Backward-incompat with pre-this etcd state**: the `regions/<id>`
rkyv blob shape changed; `cluster.sh reset` is the migration path
(matches the repo's standard same-commit deploy pattern).

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

20. **F206 `apply_ec_conversion_done` must refresh `avali` to cover all
    K+M slots.** Pre-F206 the function updated `replicates`, `parity`,
    `replicate_disks`, `parity_disks`, and `eversion` but **not**
    `avali`. The pre-EC `avali` (`all_bits(K)` from the seal path)
    persisted post-EC, so every parity slot bit stayed 0 — and
    `recovery_dispatch_loop` (`recovery.rs:339` `if (ex.avali & bit) ==
    0`) fired `EXT_MSG_RE_AVALI` to the parity holder every 2 s
    indefinitely. On the extent-node side `handle_re_avali`
    (pre-F206) didn't branch on `ec_converted`, so it compared the
    local shard size (~`sealed_length / K`) against the logical
    `sealed_length`, fell through to `fetch_full_extent_from_sources`,
    and allocated a `sealed_length`-sized `Vec<u8>` per peer attempt.
    Symptom: an idle 4-node cluster after `cluster.sh restart` showed
    extent-node RSS swinging through multiple GB per tick on the
    parity holder, plus `df` probe timeouts because the single
    compio core was tied up servicing the bogus copy traffic.

    Fix is one line in `apply_ec_conversion_done`:
    `ex.avali = Self::all_bits(target_nodes.len());`. The companion
    extent-node fix (`handle_re_avali` short-circuits with `CODE_OK`
    when `extent_info.ec_converted`) is load-bearing as a
    self-healing migration: existing pre-F206 etcd entries with the
    buggy `avali = all_bits(K)` are auto-repaired on the next
    dispatch tick — `mark_extent_available` runs on the RE_AVALI
    `CODE_OK` response and ORs in the missing slot bit, persisting
    `avali = all_bits(K+M)` to etcd. No data migration needed.

    Tests:
    - `f206_apply_ec_conversion_done_sets_avali_for_all_shards` (lib
      unit test) — asserts `ex.avali == 0xF` for a K=3+M=1 convert.
    - `system_extent_recovery.rs` — assertion strengthened from
      `ext.avali > 0` to `ext.avali == all_bits(replicates + parity)`,
      which would have caught the bug if it'd existed at landing
      time of EC.

    Cross-reference: notes 10 (F138 ec_conversion_inflight lock),
    19 (F198 rich marker).

21. **F207 unified extent in-flight ledger.** Replaces the four
    pre-F207 scattered inflight bookkeeping mechanisms
    (`ec_conversion_inflight: HashSet<u64>` F138, `pending_ec_dispatch:
    HashMap<_, MgrEcDispatchInflight>` F198, `recovery_tasks:
    HashMap<_, MgrRecoveryTask>`, `pending_extent_deletes:
    VecDeque<PendingDelete>` F109) with a single etcd-backed ledger
    keyed by extent_id. After F207-D, the previously-required
    F126/F138/F139/F145/F147-A refuse-at-start checks all collapse
    to a single `extent_inflight_op(eid)` probe (or two-snapshot
    helpers where the predicate is more nuanced than "any op
    blocks any other").

    **Layer boundary (Class A/B/C model, set by user 2026-05-15):**
    - The ledger is a STREAM-LAYER concept. Only stream-layer ops
      enrol: ConvertToEc / Recovery / Delete (i.e. ops the manager
      dispatches to extent-nodes as RPCs).
    - PS-layer ops (split / merge / punch_holes / truncate /
      sync_partition_vp_refs / alloc_extent) READ the ledger to
      refuse-at-start when a touched extent has a stream-layer op
      in flight, but DO NOT enrol themselves. They're
      partition-scoped, not extent-scoped; enrolling them would
      multiply etcd write traffic per split by source-extent count
      while violating the layer boundary.
    - Class A (PS handler starts while stream-layer op is in flight):
      PS reads ledger, refuses with Precondition. Single-line
      `extent_inflight_op` probe.
    - Class B (stream-layer op fires mid-PS-await): F146 / F147-A
      verify-at-apply pattern catches the snapshot-then-await race
      by re-reading eversion before the etcd-mirror writeback.
      Unchanged from notes 13 / 14.
    - Class C (two stream-layer ops race): exclusive-per-extent CAS
      via `acquire_extent_inflight`; second acquire returns
      Precondition.

    **Invariants (proved by code structure, not review):**
    - **I1** Leader-only writes — F149 fence on every `txn_fenced`
      call (note 15).
    - **I2** Every acquire has a matching release, OR
      `replay_from_etcd` reclaims the marker on leader failover.
      Stale markers (`started_at` > 24h) surface via WARN log from
      `extent_inflight_stale_sweep_loop` (5 min tick); operator
      decides via Python ops `--clear-stale-inflight`.
    - **I3** Release is bundled into the op's apply-done etcd txn
      (`put_and_delete_txn(extents/<id>, deletes=[extent_inflight/<id>])`).
      Atomic: either both effects land or neither does. Closes the
      pre-F207 latent leak window in `apply_ec_conversion_done`
      where the marker was deleted in a SEPARATE etcd round-trip
      after the extents/<id> put.
    - **I4** `replay_from_etcd` populates the in-memory shadow
      BEFORE `recovery_dispatch_loop` / `ec_conversion_dispatch_loop`
      / `extent_delete_loop` are spawned. Enforced by ordering
      in `new_with_etcd` (`replay_from_etcd` -> `try_become_leader`
      -> `start_runtime_tasks`).
    - **I5** Every manager handler that mutates extent state calls
      `extent_inflight_op` before clone-for-decision. Enforced by
      review + the fact that there is now exactly one helper, not
      five different sets to consult. Phase 2 (F207-C) migrated
      all 9 historical sites.

    **Atomicity headline:** pre-F207, `apply_ec_conversion_done`
    did two separate etcd round-trips (put extents/<id>, then
    delete ecConversionInflight/<id>). Manager crash between them
    leaked the marker permanently — a latent bug that the
    F119-D coordinator-side idempotency guard turned into a
    silent stall rather than corruption. F207-B (and now F207-D)
    bundles both into a single `txn_fenced`. F207-C did the same
    for `apply_recovery_done` and `extent_delete_loop`'s
    `release_delete_marker`. Same invariant I3, same protection.

    **Stale-marker sweep (`extent_inflight_stale_sweep_loop`,
    F208 — superseded F207-D's WARN-only design):**
    - Tick: every `AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS` (default
      60 s; floor 1).
    - Stale threshold: `AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS`
      (default 600 s = 10 min; floor 60).
    - On match: atomic `etcd put_and_delete_txn(delete=[
      extent_inflight/<id>])` under F149 leader fence, then
      `commit_extent_inflight_release` drops the in-memory shadow.
      Delete markers also clear the in-memory
      `delete_progress` entry.
    - WARN log per release with extent_id + op kind + age + threshold.
    - Started by `start_runtime_tasks` alongside the other loops.

    **Why auto-release is safe (for Recovery + Delete):** sweep ONLY
    touches the ledger marker. It does NOT mutate `extents/<id>` state
    and does NOT message any EN. If an EN-side task corresponding to a
    released marker is genuinely still running, the worst case is
    wasted retry work, never a data-correctness issue:
    - Recovery: EN completes, pushes `recovery_done`, next df probe
      drains it; `apply_recovery_done` runs with marker already cleared
      → no defer → apply proceeds.
    - Delete: EN `handle_delete_extent` is idempotent (NotFound → Ok).

    **F209-C: ConvertToEc is WARN-only and is NEVER auto-released.**
    Releasing a ConvertToEc marker opens a race with the original
    EN-side dispatch: a fresh `handle_force_ec_convert` (or the
    external Python policy controller's retry) can succeed in the gap
    between marker release and the original `apply_ec_conversion_done`
    landing, and shuffle a **different** parity-node assignment. Then
    the dispatch loop runs `apply_ec_conversion_done` with the new
    layout while the original EN-side bytes are what hit disk. F153's
    per-extent mutex serialises the two on the EN coordinator, but the
    manager state can still record the second dispatch's `target_nodes`
    / `extra_disk_ids` while the first dispatch's bytes are what
    physically landed — exactly the failure mode F198's rich marker
    was added to prevent. The sweep emits a WARN every tick for a
    stale ConvertToEc marker; operator must inspect EN state and
    decide manually (Python ops: confirm EN finished, then
    `etcdctl del extent_inflight/<id>`).

    **Leader-failover-as-reconcile invariant:** `started_at` is in the
    rkyv'd `MgrExtentInflightRecord` payload, persisted at acquire
    time. New leader's `replay_from_etcd` loads it unchanged, so the
    stale-detection clock continues across failovers. Markers that
    were already stale on the deposed leader get released by the new
    leader's sweep without needing any handoff.

    **In-memory ↔ etcd drift:** the ONLY supported source of drift is
    a human running `etcdctl del extent_inflight/<id>` directly. The
    remediation is "restart the manager" (or wait for the next
    failover); the new leader's replay rebuilds in-memory from etcd
    from scratch. F208 deliberately does NOT do a per-tick prefix
    read for reconcile — the operator-error scenario is too rare to
    justify the cost, and the failover path already handles it.

    **No backward compatibility with pre-F207 etcd state.** F207-A/B/C
    transitional fold-in of legacy `ecConversionInflight/` and
    `recoveryTasks/` prefixes is gone (F207-E). Deploying F207
    onto an existing pre-F207 cluster's etcd state is unsupported —
    the prior cluster must be torn down (`cluster.sh reset`). The
    EN-side recovery / EC convert task lifecycles are independent
    of the manager marker (e.g.
    `crates/stream/src/extent_node.rs::handle_require_recovery`
    detaches the work and pushes to `recovery_done`), so any
    abandoned legacy markers are inert — they would only mislead
    a fold-in path into treating dead state as live work. F207-C's
    fold-in turned out to be a real bug surface (alloc_extent
    refused indefinitely against a "ghost" Recovery task that no
    EN was actually running); F207-D removed it; F207-E removed
    even the WARN-on-detection path.

    **Test helpers (`#[cfg(test)] _test_mark_*_inflight`):** unit
    tests that simulate an in-flight op without going through etcd
    use these helpers. They bypass the CAS + F149 fence; do NOT
    use them in production code paths.

    Cross-reference: notes 10 (F138), 11 (F139), 12 (F145),
    13 (F146), 14 (F147-A), 19 (F198), 20 (F206). All those
    notes describe the historical context for individual races
    that F207 now unifies under one mechanism.

    **F209 hardening (post-review, 2026-05-15):**
    - **F209-A** `handle_get_partition_detail` now gates on
      `self.leader.get()`. A follower's `policy.metrics` is empty;
      pre-F209-A the handler silently returned `CODE_OK` + all-zero
      `PartitionLoad`, so `client info --part PID --detail` against
      a follower was indistinguishable from "PS hasn't reported yet".
    - **F209-B** `apply_recovery_done` slot-mismatch (`replace_id`
      not in extent's node list) now releases the Recovery marker
      before returning `Precondition`. Pre-F209-B the early-return
      happened inside `borrow_mut` and skipped the release —
      violating invariant I3 (every acquire has a matching release);
      F208 sweep was the safety net. Matches the existing
      `layout_changed == Some(true)` release pattern.
    - **F209-C** F208 sweep no longer auto-releases ConvertToEc
      markers — WARN-only with operator-driven remediation. See the
      block above.
    - **F209-D** `handle_force_ec_convert` adds the F146 eversion
      verify pattern, but **verify-BEFORE-acquire** rather than the
      verify-after-acquire shape that F146 uses for
      `handle_stream_alloc_extent` / `handle_multi_modify_split`.
      The race: between the L2436 snapshot and the acquire below
      there are N `alloc_extent_on_node` awaits — during them a
      Recovery that started after the L2416 `extent_inflight_op`
      probe can complete (acquire + apply + release its own marker)
      and bump `ex.eversion` + rewrite `ex.replicates`. Proceeding
      to acquire ConvertToEc with the stale snapshot's
      `ex.eversion + 1` would mean the dispatch loop's
      `apply_ec_conversion_done` later writes the stale
      `new_eversion` to etcd and overwrites the recovery's slot
      replacement.
      **Why verify-BEFORE, not verify-after** (the original F209-D
      shape, revised after codex review): a verify-after-acquire +
      drain-on-mismatch path has a failure mode where
      `drain_extent_inflight_marker` itself fails (NotLeader during
      the drain await, or transient etcd error) — the stale marker
      stays in etcd, the dispatch loop's next tick (or a successor
      leader's replay) picks it up, and applies the stale state.
      Verify-BEFORE-acquire skips the problem: no marker is ever
      written if state has drifted, so no drain is needed. After
      our acquire succeeds, F207's exclusive ledger CAS + every
      other mutator's `extent_inflight_op` refuse-at-start
      (apply_recovery_done, handle_*_punch_holes, handle_truncate,
      handle_multi_modify_split / merge, handle_sync_partition_vp_refs,
      handle_stream_alloc_extent) freezes `ex.eversion` until our
      apply runs.
    - **F209-E** `crates/manager/tests/f209_apply_done_atomicity.rs`
      (`#[ignore]`, requires embedded etcd) asserts the I3
      atomicity claim end-to-end: success path lands both effects,
      F149-fence-failed apply rolls back both atomically.

22. **F210-F/G policy + replay hardening (2026-05-16).**
    Closes a backlog of correctness gaps in the advisory pipeline,
    the PS-liveness etcd mirror, and the long-tail extent-delete
    retry queue. All are independent additive fixes.

    - **F210-F1** Wire-stability contract on the `POLICY_KIND_*` enum:
      numeric values frozen, new kinds APPEND only.
      `MSG_GET_POLICY_KIND_NAMES = 0x3B` const-dump RPC +
      `policy_kind_names()` helper let external controllers
      introspect the binary's actual mapping instead of hardcoding
      values that drift across releases.
    - **F210-F2** `PartitionMetricsWindow::push_with_cap_and_bucket`
      snaps `ts` to `bucket_sec` boundary; same-bucket pushes
      REPLACE. `take(required_buckets)` now spans the documented
      `required_buckets × bucket_sec` seconds regardless of report
      cadence. Pre-F210-F2 every 5 s PS report became its own
      bucket → all windowed advisories' "sustained over 5 min" was
      off by 12×.
    - **F210-F3** `PolicyEngine::prune_stale_metrics(state, now)`
      runs at top of `policy_tick_loop` before any compute_*. Drops
      metrics for partitions not in `state.partitions` (post-split /
      merge / PS-evict) and windows older than
      `STALE_METRICS_AGE_SEC = 300`. Also prunes `last_hot_cold_at`
      for evicted PSes. Closes the "zombie metrics keep firing
      advisories forever" gap.
    - **F210-F4** Hot/cold band guard. A partition qualifies as
      "hot" only when its min ≥ `qps_hottest / HOT_COLD_BAND_DIVISOR`
      (D=2), and "cold" only when max ≤ `qps_coldest * D`. Same on
      size. Empty band → suppress dimension entirely. Closes the
      "rotating hotspot on both lists" case.
    - **F210-F5** `handle_report_partition_load` now reads
      `p.config.window_buckets / bucket_sec` and dispatches
      `push_with_cap_and_bucket`. `PolicyConfig.window_buckets` is
      now load-bearing.
    - **F210-F6** `handle_get_policy_candidates` leader gate
      (returns `CODE_NOT_LEADER` + empty list on follower). Sister
      to F209-A.
    - **F210-G1** PS eviction etcd mirror now explicit. The
      `ps_liveness_check_loop` issues a fenced
      `put_and_delete_txn(deletes=[psNodes/<id>])` for each dead PS
      before calling `mirror_partition_snapshot`. Plus
      `replay_from_etcd` seeds `ps_last_heartbeat[ps_id] =
      Instant::now()` for every replayed PS so the liveness loop's
      `Some(t)` arm engages within `PS_DEAD_TIMEOUT` instead of the
      `None`-arm-as-alive zombie. The two together close the
      failover-resurrects-dead-PS path without persisting `Instant`
      to etcd (which would require wall-clock serialization).
    - **F210-G2** Persisted retry queue for budget-exhausted extent
      deletes. New `extentDeleteRetry/<id>` prefix +
      `MgrExtentDeleteRetry` rkyv struct. When
      `extent_delete_loop` exhausts the in-memory 60-attempt
      budget, the entry is persisted to etcd + moved to
      `failed_deletes: HashMap<u64, MgrExtentDeleteRetry>` instead
      of abandoned to the per-node startup reconcile. A new
      `extent_delete_retry_loop` (1 min cadence) walks the map
      with per-entry exponential backoff (60 s → 1 hr ceiling,
      2× per attempt up to 6 shifts) and retries every remaining
      replica. On full ack, etcd key + map entry are removed. The
      inflight ledger Delete marker is still RELEASED when the
      entry transitions to this queue, so future ops on the extent
      aren't blocked — the retry queue is independent etcd state.
      Replay rehydrates the map from the prefix; `attempts` +
      `last_attempt_at` survive failover so backoff windows are
      respected by the new leader.

    What this does NOT change:
    - F207 unified inflight ledger semantics — unchanged.
    - F208 stale-marker sweep (still auto-releases Recovery +
      Delete markers older than 10 min; ConvertToEc is WARN-only
      per F209-C). F210-G2's retry queue is orthogonal: the
      Delete marker is released the moment the entry enters the
      retry queue, so F208's sweep never sees a stale Delete
      marker coexisting with a long-lived retry entry.
    - F149 leader fence on every etcd write txn — all new
      mirrors (psNodes/ delete, extentDeleteRetry/) route through
      `put_and_delete_txn` / `put_msgs_txn` which thread the
      fence unchanged.

23. **F211 operator-driven node lifecycle (2026-05-17).**
    Reshapes node failure handling from "manager auto-detects + immediately
    rebuilds" to HDFS-decommission-style "manager exposes facts +
    operator confirms via admin RPC". Two layers:

    **Layer 1 — automatic, in-memory only (`crates/manager/src/node_state.rs`):**
    `NodeStateTracker` tracks `Online ↔ Suspected` per EN based on
    `disk_status_update_loop`'s df outcome. **Crucially: there is no
    automatic `Down` transition.** Fed at three points: (i) df failure
    in `disk_status_update_loop`, (ii) df success there, (iii)
    `register_node` initial-OK heartbeat. Replay seeds every EN node
    OK on leader-promotion so the new leader gets a fresh soft-timeout
    window before its judgement settles.

    **Layer 2 — operator-driven, etcd-persisted (`crates/manager/src/
    rpc_handlers.rs` F211-C handlers + `node_override/` prefix):**
    `mgr_fence_node` / `mgr_set_node_maintenance` / `mgr_clear_node_override`
    / `mgr_remove_node`. The persistent `MgrNodeOverride` keyed by
    `node_id` is the trigger for cleanup. Effects on `mgr_fence_node`:
    (i) write `node_override/<id>` (etcd), (ii) bump owner-lock
    revision for every extent the node touches (defence against
    revived ghost writes), (iii) `auto_abandon_for_fenced_node`
    sweeps ConvertToEc markers whose `target_nodes[0] == fenced_node`,
    atomically deletes them + writes `ec_convert_advisory/<id>` for
    operator follow-up. `mgr_remove_node` has the safe-decommission
    preconditions: must be Fenced AND no extent / marker still
    references the node — failure returns `Precondition` with the
    blocking extent/marker IDs in the response.

    **Recovery dispatch gate (`crates/manager/src/recovery.rs`):**
    `recovery_dispatch_loop` reads `AUTUMN_MGR_RECOVERY_GATE` (default
    `fenced_only`). In `fenced_only` mode, a per-slot replica is
    rebuilt ONLY when its owning node's override is `Fenced`.
    `auto_disk` rolls back to the legacy "trigger on disk.online ==
    false" path. This is a backward-incompat default — operators
    that haven't deployed a policy script can flip the env var.

    **Recovery rate limiter (`crates/manager/src/recovery_rate_limiter.rs`,
    F211-H):** per-source/target/global concurrency caps prevent a
    single fence from saturating cross-node bandwidth. Backoff is
    keyed by `(extent_id, slot)`: 2^N seconds capped at 300 s, reset
    on first success. `mgr_recovery_stats` exposes inflight + queue
    snapshot for monitoring.

    **EC dispatch suspended-skip (`crates/manager/src/recovery.rs`
    F211-F):** before each ConvertToEc dispatch the loop checks the
    coord's `(auto_state, override_kind)`. If Suspected/Fenced/
    Maintenance, the iteration silently skips — no log spam during a
    flap. The marker stays in the ledger and is picked up when the
    coord recovers or when `auto_abandon_for_fenced_node` deletes it.

    **Maintenance TTL (`crates/manager/src/recovery.rs`
    `tick_maintenance_ttl`):** invoked once per `recovery_dispatch_loop`
    tick. Walks `node_overrides`; any `Maintenance` entry with
    non-zero `expire_at <= now` is deleted from etcd + memory and
    logged INFO. No audit entry (the system, not the operator,
    triggered the clear — operator already set the expiry).

    **Zombie defense (`handle_register_node` F211-C #2):** if the
    requester's address is associated with a `node_id` whose override
    is currently `Fenced` OR which appears in the `decommissioned/`
    tombstone prefix, registration is refused with `Precondition`.
    Operator must `clear_node_override` (Fenced case) or pick a fresh
    address (decommissioned case) before the node can come back.

    **Audit log (`crates/manager/src/audit.rs`, F211-I):** etcd
    prefix `mgr_audit_log/<ts_ns>_<seq>` → rkyv `MgrAuditEntry`.
    Every F211-C admin RPC handler wraps its return in
    `append_audit`. Best-effort persistence — failure logs WARN but
    doesn't fail the primary op (the audit miss is the lesser of two
    bads; replay still has the override). `mgr_query_audit_log`
    RPC for retrieval; `audit_retention_gc` helper for 90-day GC.

    Cross-reference: notes 15 (F149 leader fence — every F211 etcd
    write routes through `txn_fenced`), 21 (F207 unified inflight
    ledger — F211-F's auto-abandon writes through the same release
    primitive `commit_extent_inflight_release` after the atomic
    delete + advisory put).
