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
- `nodes/`, `disks/`, `streams/`, `extents/`, `partitions/`, `regions/`, `ps_nodes/`, `next_id`

On leader promotion, `replay_from_etcd` reads all prefixes to rebuild in-memory state. The etcd transaction in `multi_modify_split` groups all related writes/deletes atomically.

## Programming Notes

1. **Etcd-first mutation pattern** — all mutating RPC handlers follow: (1) compute mutations without modifying store, (2) persist to etcd, (3) apply to in-memory store. This ensures manager crash after step 1 but before step 2 leaves etcd and memory consistent. Exception: `register_ps`/`upsert_partition` apply to memory first because `mirror_partition_snapshot` reads from the store (these are idempotent on retry). The old function `duplicate_stream` (which modified state directly) has been replaced by `compute_duplicate_stream` (read-only) + `apply_split_mutations`.

2. **`compute_duplicate_stream` increments extent `refs`** — this is the ref-counting mechanism for CoW. If you add new ways to share extents, increment refs. If you add new ways to remove extents, decrement refs and only delete when refs → 0.

3. **Owner revision must be validated before any stream mutation** — call `ensure_owner_revision` at the start of `stream_alloc_extent`, `stream_punch_holes`, `truncate`, `multi_modify_split`. Missing this allows split-brain.

4. **Leader check** — some RPCs should only execute when `self.leader.load()` is true. Writes to etcd from a non-leader will fail (etcd lease is expired), which will surface as an error.

5. **`alloc_ids` is the only ID source** — never generate IDs any other way. The `next_id` is derived from `max(all_entity_ids) + 1` during `replay_from_etcd`, so wasted IDs from failed mutations are safe.

6. **Rebalance is called eagerly** — `rebalance_regions` after every PS registration or partition upsert. This is safe because it's idempotent (keeps existing assignments, only changes unassigned ones).
