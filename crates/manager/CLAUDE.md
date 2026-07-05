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

### VP lifetime after split (`vp_table_refs` — MACHINERY removed 2026-06-18; gate guard kept until Stage 2)

Split can duplicate row-stream SST ownership without duplicating the old log
extents referenced by the SSTs' embedded `ValuePointer`s. Historically the
manager guarded those old log extents with a SECOND refcount,
`vp_table_refs` (indirect retention by live SSTs whose `MetaBlock.vp_deps`
mentioned the extent), synced from each PS via `MSG_SYNC_PARTITION_VP_REFS`
and stored as `partitionVpRefs/<part_id>`. Deletion required
`refs == 0 && vp_table_refs == 0`.

**The target end-state is `refs`-only retention.** The load-bearing invariant:
**GC relocates every live in-range value off a log extent BEFORE `punch_holes`
drops its `refs`** (relocate-then-punch, made correct by GC-VP-IDENTITY:
liveness is full VP identity, not just `extent_id`). So `refs == 0 ⇒ no live
ValuePointer`. CoW split keeps both children pointing at the shared log extents
via `refs` (each child's `log_stream.extent_ids` lists them →
`compute_duplicate_stream` does `refs += 1`); the extent is freed once BOTH
children GC it to `refs == 0`. No second counter is needed — `vp_table_refs`
only mattered when `refs` accounting was buggy (extent-10 class), and the
project's stance is to FIX such bugs, not net them.

**Why removal is STAGED (same-commit, stop-the-world deploy — never rolling).**
The `refs == 0 ⇒ no live VP` invariant only holds for extents that reached
`refs == 0` UNDER the post-GC-VP-IDENTITY relocate-then-punch path. A cluster
upgraded from a pre-removal build may hold LEGACY extents frozen in etcd at
`refs == 0 && vp_table_refs > 0` (live VPs the old net legitimately protected,
e.g. extent 10) — reached under an older buggy GC. Collapsing the gate to
`refs == 0` in the same release that removes the net would reap those on the
first post-upgrade sweep → DATA LOSS. So:

- **Stage 1 (done):** remove the entire *maintenance machinery* — the sync/pull
  RPCs (`MSG_SYNC_PARTITION_VP_REFS` / `MSG_PULL_VP_REFS`), the
  `partition_vp_refs` state + `partitionVpRefs/` load, the maintenance fns, the
  PS computation + `vp_refs_dirty` GC gate + retry. The `vp_table_refs` field
  becomes WRITE-FROZEN (no maintainer → every extent managed under this build
  has `vp_table_refs == 0`, so the gate is effectively `refs == 0` for them).
  `extent_can_delete` KEEPS `refs == 0 && vp_table_refs == 0` as an
  **upgrade-safety guard**: legacy `vp_table_refs > 0` extents are not reaped
  (a bounded space leak, never a loss). The persisted fields
  (`MgrExtentInfo.vp_table_refs`, SST `MetaBlock.vp_deps`) stay so existing
  records decode unchanged.
- **Stage 2 (pending):** a migration that re-confirms no live VP (PS re-scans
  SST `vp_deps` / forced major-compact) then clears `vp_table_refs`, collapses
  `extent_can_delete` to `refs == 0`, removes both persisted fields behind a
  versioned decode + one-time rewrite, and deletes stale `partitionVpRefs/` keys.

## Cluster overview size = sealed sum + PS-reported open-tail bytes (F-OVERVIEW-OPENTAIL)

`compute_cluster_overview_resp`'s per-partition `live_size` is
`Σ distinct extents' sealed_length` (manager-authoritative) **plus** the
latest PS-reported `PartitionLoad.open_tail_bytes` (`open_tail_of(pid)`, read
from the policy window's newest bucket; 0 → sealed-sum-only fallback). An OPEN
extent's manager `sealed_length` is 0, so a major-compacted / log-heavy
partition whose data lives entirely in open tails would otherwise render 0 B
(user hit it live: part 17 `0 B` in the overview vs `1.5 GB` from
`info --part 17`'s EN probe). The PS supplies the missing piece because the
manager cannot know an open tail's live length without an EN probe, and the
overview is deliberately one-RPC / scalable (no per-partition fan-out) — the PS
already reports load every 5 s, so the open-tail sum rides that channel for
free. The number is thus a periodic rollup (same staleness class as
`req_per_sec`); the exact size is `info --part` (which probes the EN live). The
dashboard `/api/overview` shares this builder, so it inherits the fix.
**Invariant: never re-introduce a sealed-length-only `live_size` — an
all-open-tail partition MUST count its open-tail bytes.** PS side:
partition-server CLAUDE.md (F-OVERVIEW-OPENTAIL probe). Related dead-gauge
follow-up: `PartitionLoad.size_bytes` has no writer (always 0) → the
`autumn_ps_partition_size_bytes` Prometheus gauge + size-based auto-split/merge
policy are inert; reviving it is deferred (feature_list F-PS-SIZE-BYTES-DEAD)
because it would silently arm size-based auto-policy.

## cluster-df amplification = physical / logical FOOTPRINT (F-DF-OPENTAIL)

`ClusterCapSnapshot.logical_open_tail` (Σ PS-reported `open_tail_bytes` across
partitions, summed each cluster-df tick from the policy load window) is the
companion to `logical_stored` (sealed-only). The amplification the consumer
(`autumn-op df` / dashboard) computes MUST use `physical_used / (logical_stored +
logical_open_tail)`, NOT sealed-only: `physical_used` (Σ EN `extent_bytes`)
INCLUDES open-tail bytes — which for a VP workload are largely LIVE large-value
bytes sitting in the open log tail (the SST holds only pointers) — so a
sealed-only denominator inflates amp ~15× (a 3-replica cluster showed 45× when
part 17's 1.6 GB open log was excluded). With the fix amp ≈ the real replication/
EC factor (~3× / lower). Open tails are refs=1 partition-private, so the sum
needs no CoW dedup (unlike the per-partition overview, which DOES double-count
CoW-shared extents across siblings — different view, see F-OVERVIEW-OPENTAIL).
**Invariant: any physical/logical ratio must compare like scopes — if the
numerator counts open-tail/replica bytes, the denominator must too.** The
accurate DEAD-byte (WAL-debt) metric is a separate deferred feature
(F-DF-WALDEBT) — do NOT approximate it as `footprint − size_bytes` (that
mis-flags live VP data as debt).

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

### Node Health Loop (every 2 seconds, F222 — merges the former Collect + Disk-Status loops)
ONE `df` caller per node per tick. Pre-F222 there were two: a 2 s `recovery_collect_loop` (target nodes only, non-empty `tasks`, applied `done_tasks`) and a 10 s `disk_status_update_loop` (all nodes, empty `tasks`, updated liveness, **discarded** `done_tasks`). Because the EN's `handle_df` does `std::mem::take(recovery_done)` when `req.tasks.is_empty()`, the disk-status loop drained completions and threw them away whenever its sweep won the race — so `apply_recovery_done` never ran (recovery wasted, orphan copy, marker stuck until F208). The merged `node_health_loop` iterates ALL nodes, sends empty `tasks` (EN drains its full `recovery_done`), and on every successful `df`:
- `mark_node_disks_online` + `recent_failure_reports.remove` + `node_states.on_heartbeat_ok` (F211-A Suspected→Online), and
- `apply_recovery_done` for EVERY returned `done_task` (replaces the failed node_id in `ExtentInfo.replicates`, bumps eversion, marks slot available, mirrors to etcd, releases the inflight marker atomically).
On `df` failure: `mark_node_disks_offline` + `node_states.on_heartbeat_fail` (Online→Suspected after the soft timeout; NOT a recovery trigger — that needs explicit fence, F211-E). One caller = the drain-and-discard race is structurally impossible. See Programming Note 25.

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

2. **`compute_duplicate_stream` increments extent `refs`** — the direct stream-membership refcount for CoW. Both split children list the shared log extents in their own `log_stream.extent_ids`, so `refs` covers them; the extent is freed only when both children GC it back to `refs == 0`. (`vp_table_refs`-removal: the second-counter *maintenance* machinery is gone, but `extent_can_delete` keeps `refs == 0 && vp_table_refs == 0` as an upgrade-safety guard for legacy extents until Stage 2's migration — see "VP lifetime after split". For extents managed under this build `vp_table_refs == 0`, so deletion is effectively `refs == 0`, safe because GC relocate-then-punch guarantees `refs == 0 ⇒ no live VP`.)

3. **Owner revision must be validated before any stream mutation** — call `ensure_owner_epoch` at the start of `stream_alloc_extent`, `stream_punch_holes`, `truncate`, `multi_modify_split`. Missing this allows split-brain.

4. **Leader check** — some RPCs should only execute when `self.leader.load()` is true. Writes to etcd from a non-leader will fail (etcd lease is expired), which will surface as an error.

5. **`alloc_ids` is the only ID source** — never generate IDs any other way. The `next_id` is derived from `max(all_entity_ids) + 1` during `replay_from_etcd`, so wasted IDs from failed mutations are safe.

6. **Rebalance is called eagerly** — `rebalance_regions` after every PS registration or partition upsert. This is safe because it's idempotent (keeps existing assignments, only changes unassigned ones).

7. **F121 disk-online tracking is call-result-driven, NOT
   payload-driven.** The `df` poll in `node_health_loop` (F222; was
   `disk_status_update_loop` + `recovery_collect_loop`) uses the helpers
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

14. **F147-A `handle_sync_partition_vp_refs` refuse-at-start + verify-at-apply
    — REMOVED 2026-06-18 with the vp_table_refs deletion.** The entire
    VP-ref sync RPC (`MSG_SYNC_PARTITION_VP_REFS`), its handler, and the
    `vp_table_refs` aggregate it maintained are gone; extent retention is now
    `refs`-only (see "VP lifetime after split"). The refuse-at-start /
    verify-at-apply two-stage pattern it pioneered still lives on the paths
    that remain (notes 12/13 — F145/F146 split/merge, F207 EC/recovery).
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
      3. `acquire_owner_epoch` (lib.rs ~665) — extra_cmp is
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

    **F-DASH-IN-MGR (2026-07-04): the controller is now IN the manager, but
    this does NOT revert F203's mechanism/policy split.** The retired Python
    `python/dashboard/` external controller was folded into the manager as
    `auto_policy.rs` + `auto_policy_tick_loop` (see the F-DASH-IN-MGR note
    below). Advisory emission (`recompute_advisory_cache`) stays a separable
    mechanism layer that never self-dispatches; the in-manager controller is a
    DISTINCT, **leader-fenced, DEFAULT-OFF** module (Off→DryRun→Armed; armed only
    when an operator selects+enables a policy AND `--dashboard-allow-mutations`
    is set), so a fresh cluster stays pure-mechanism until armed. What changed is
    the HOST process (crash-safe leader instead of a killable Python webserver),
    not the mechanism/policy boundary. Ops runbook: `docs/ops.md` (Web dashboard
    + auto-policy controller).

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
      alloc_extent) READ the ledger to
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
      `PartitionLoad`, so `autumn-op info --part PID --detail` against
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
      handle_multi_modify_split / merge,
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
    `node_health_loop`'s df outcome (F222; was `disk_status_update_loop`).
    **Crucially: there is no automatic `Down` transition.** Fed at three
    points: (i) df failure in `node_health_loop`, (ii) df success there,
    (iii) `register_node` initial-OK heartbeat. Replay seeds every EN node
    OK on leader-promotion so the new leader gets a fresh soft-timeout
    window before its judgement settles.

    **Layer 2 — operator-driven, etcd-persisted (`crates/manager/src/
    rpc_handlers.rs` F211-C handlers + `node_override/` prefix):**
    `mgr_fence_node` / `mgr_set_node_maintenance` / `mgr_clear_node_override`
    / `mgr_remove_node`. The persistent `MgrNodeOverride` keyed by
    `node_id` is the trigger for cleanup. Effects on `mgr_fence_node`:
    (i) capacity precheck unless `--force`, (ii) write
    `node_override/<id>` (etcd), (iii) `auto_abandon_for_fenced_node`
    sweeps ConvertToEc markers whose `target_nodes[0] == fenced_node`,
    atomically deletes them + writes `ec_convert_advisory/<id>` for
    operator follow-up. (The F211-D "bump owner-lock revision for every
    extent the node touches" was REMOVED by the BUG #3 Layer B fix —
    see the comment block + tombstone in `rpc_handlers.rs` near
    `fence_node_impl`; fencing an EN data node must not fence the PS
    partition owners. Writer fencing on takeover is `acquire_partition_
    owner_epoch`'s job, note 35.) `mgr_remove_node` has the
    safe-decommission preconditions: must be Fenced AND no extent /
    marker still references the node — failure returns `Precondition`
    with the blocking extent/marker IDs in the response. An OPEN tail
    extent's slot counts as a reference, hence the F-FENCE-DRAIN sweep
    below.

    **Recovery dispatch gate (`crates/manager/src/recovery.rs`):**
    `recovery_dispatch_loop` reads `AUTUMN_MGR_RECOVERY_GATE` (default
    `fenced_only`). In `fenced_only` mode, a per-slot replica is
    rebuilt ONLY when its owning node's override is `Fenced`.
    `auto_disk` rolls back to the legacy "trigger on disk.online ==
    false" path. This is a backward-incompat default — operators
    that haven't deployed a policy script can flip the env var.

    **F-FENCE-DRAIN (2026-07-04) — fence drains OPEN tails + hard
    placement exclusion.** Three gaps closed (found live: fenced nodes'
    shard counts GREW after fencing, and their open tails never drained
    so `remove` never unblocked):
    - **Placement hard-exclusion**: `placement_excluded_node_ids()`
      (lib.rs) = Fenced ∪ Maintenance (overrides) ∪ Suspected
      (`node_states.suspected_node_ids()`; `Suspend` deliberately NOT
      included — bootstrap seeds every node Suspend). Threaded as the
      `hard_excluded` param into `select_nodes` (filtered at the TOP so
      the count precheck AND the cold-leader degraded fallback both
      inherit it — unlike the F190 soft exclude, hard-excluded nodes are
      NEVER backfilled), all three fallback walks (create_stream /
      alloc / merge Phase 1.5), `dispatch_recovery_task`'s target
      candidates (a rebuilt replica must not land on a draining node),
      and `handle_force_ec_convert`'s extra-parity pool. Availability
      trade-off: a 3-EN RF-3 cluster with one Suspected node refuses new
      extent allocation until it heals (~2 s df tick) or is fenced.
    - **Open-tail drain sweep** (`drain_fenced_open_tails`, recovery.rs,
      runs each recovery tick): recovery only rebuilds SEALED extents
      (note 31), so an idle partition's open tail on a fenced node never
      drained. The sweep finds OPEN tails (`!ex.sealed`) with a fenced
      member, resolves the owning partition (streams → partitions →
      part_addrs, region/ps_nodes fallback) and sends `MSG_ROLL_TAILS`
      (partition_rpc 0x57, WIRE v13) to the serving PS with
      `(stream_id, expected_tail)` pairs; 30 s per-partition cooldown
      (`roll_tails_cooldown`, in-memory). The PS (`handle_roll_tails`,
      spawned like SPLIT_PART so its awaits don't block partition_loop)
      checks idempotency (current tail == expected, else skip), then
      seals+rolls: log/meta tails via P-log `part_sc.seal_and_roll_tail`
      (the WAL-self-heal primitive — F227 lenient probe seal, dead
      replicas don't block; ALL replicas unreachable → loud
      Precondition, never a silent seal); the ROW tail routes through
      P-bulk's F255 barrier with the new `seal_and_roll: true` flag
      (drains inflight to zero first — row_stream single-writer
      invariant). Once sealed, the next recovery tick's fenced branch
      rebuilds the extent's fenced slots → `remove` unblocks. Writer
      compatibility is all existing machinery: `already_sealed` no-op +
      `seal_extent_id` idempotency + client sealed-tail re-alloc.
    - **Sealed-empty rebuild**: the dispatch pre-filter keys on
      `!ex.sealed` (the STATE) instead of `sealed_length == 0`, so an
      authoritative sealed-EMPTY extent (split/merge tail seal, or an
      empty tail sealed by this drain) gets its fenced slots rebuilt
      (EN does the 0-byte copy and sets the sealed flag) instead of
      referencing the node forever.
    Follow-up (feature_list): manager-unilateral seal (EN-side seal RPC
    + append rejection) for the "partition has NO serving PS for a long
    time" case — deferred until actually hit; the sweep WARNs when it
    can't resolve a PS address.

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

24. **F214 cluster identity + Suspend state (2026-05-18).**
    Unifies the pre-F214 split of `autumn-op format` / `autumn-op
    register-node` / `autumn-extent-node --disk-id` into a single
    explicit per-disk preparation step plus a proper node-level
    state machine.

    **Cluster identity (`CLUSTER_ID_KEY`, F214-A).** Manager
    CAS-imprints `autumn-rs/cluster_id = <UUID>` exactly once on
    first leader-promotion through `try_become_leader` →
    `imprint_cluster_id`. Subsequent leaders re-CAS, observe
    `succeeded == false`, and re-read. `replay_from_etcd` installs
    the value on every leader-promotion. Memory-only mode (no etcd)
    keeps the per-process UUID from `Self::new()` so dev/test
    flows still work end-to-end. New RPC `MSG_GET_CLUSTER_ID = 0x45`
    exposes the value — no leader gate (followers answer from
    replayed state); the only failure mode is "manager never
    bootstrapped" which returns `CODE_ERROR`.

    **`autumn-op format` (F214-C) is the single per-EN entry point.**
    Calls `MSG_GET_CLUSTER_ID`, allocates a `disk_uuid` per dir,
    fires `MSG_REGISTER_NODE`, then writes `cluster_id` +
    `disk_uuid` + `node_id` + `disk_id` sentinel files in each dir.
    Idempotent: re-running against a dir whose `cluster_id` matches
    the manager reuses the stored `disk_uuid` so the manager's
    re-register branch returns the same `disk_id`. Mismatched
    cluster_id → refuse with both UUIDs in the error message.
    The standalone `register-node` CLI subcommand is gone — replaced
    by a print-and-exit migration stub that fires BEFORE
    `ClusterClient::connect` (matches the F213 `autumn-client op`
    pattern).

    **EN startup verification (F214-D).** `autumn-extent-node`
    runs two pre-flight checks before the listener binds:
    (a) `read_and_verify_cluster_id` — sync, in main() prelude.
        Reads each `--data` dir's `cluster_id` file; refuses on
        missing / empty / inter-dir disagreement. Shard threads
        never start against a misconfigured dir set.
    (b) `verify_manager_cluster_id` — async, runs on shard 0's
        compio runtime (or `run_single_shard`'s). One `MSG_GET_CLUSTER_ID`
        round-trip; refuses on mismatch. Caught the "wrong manager"
        misconfiguration that (a) alone cannot see.
    The pre-F214 `--disk-id N` CLI bypass was removed; same
    "feature removed; run `autumn-op format` first" error pattern
    as `--shards` (F196).

    **Suspend node state (F214-B).** `NodeAutoState` extends from
    `{Online, Suspected}` to `{Online, Suspected, Suspend}`. The
    new variant is the initial state for any node freshly added
    via `handle_register_node`'s first-register branch (line ~407)
    — `on_register_first(node_id)` seeds Suspend without touching
    `last_ok`. Transitions:
    - **Suspend → Online**: on first successful `df` heartbeat
      from `node_health_loop` (F222; was `disk_status_update_loop`).
      The 2 s sweep cadence means a Suspend node typically reaches
      Online within ~2-4 s of `MSG_REGISTER_NODE` (was 10-20 s).
    - **Suspend → Suspected**: NEVER. `on_heartbeat_fail` and
      `tick()` both no-op on Suspend; Suspected requires a prior
      verified-alive baseline (it means "was alive, now flaky").
    - **Suspend → Online**: also via operator re-register
      (`handle_register_node` re-register branch keeps the
      pre-F214 `on_heartbeat_ok` call — operator explicit vouches).
    - **Online ↔ Suspected**: unchanged from F211-A.

    **`select_nodes` gates on Online (F214-B).** The function takes
    a new `&HashSet<u64> online_node_ids` parameter. Each caller
    (`handle_create_stream`, `handle_stream_alloc_extent`,
    `handle_multi_modify_merge`) captures the set via
    `self.node_states.borrow().online_node_ids()` BEFORE the
    `store.inner.borrow_mut()` (the two RefCells are disjoint so
    holding the snapshot doesn't conflict with the borrow).
    F121's cold-leader fallback is preserved: when too few Online
    nodes exist (e.g. fresh cluster where no df sweep has run yet),
    the pool widens to the full set — the post-RPC walk in
    `handle_stream_alloc_extent` still recovers via per-RPC
    fallback. The EC dispatch loop's Suspected-window skip
    (`recovery.rs::ec_conversion_dispatch_loop`) extends to Suspend
    too — a never-verified coord is even less appropriate for
    shard fanout than a flaky one.

    **Why this beats the original `disk.online=false` proposal.**
    `disk.online` keeps one clean meaning (per-disk health from EN
    df), and the state machine becomes the primary control-plane
    abstraction. `select_nodes` ANDs both filters — Online node
    AND online disk. Operators looking at `autumn-op info` see
    `Suspend` as distinct from `Suspected` (`auto_state_str` was
    extended). `NODE_AUTO_STATE_SUSPEND = 2` is wire-stable; new
    variants append-only.

    Cross-reference: notes 7 (F121 `disk.online` filter — Suspend
    rides on top), 15 (F149 `txn_fenced` — F214-A cluster_id CAS
    inherits the leader fence), 23 (F211 NodeStateTracker — F214-B
    extends the same struct with the third variant).

25. **F222 single df caller (`node_health_loop`).** The manager must
    have exactly ONE loop calling `EXT_MSG_DF` per node, and it must
    apply `done_tasks`. Pre-F222 a second df caller
    (`disk_status_update_loop`, empty `tasks`) raced the recovery
    collector: the EN's `handle_df` drains its ENTIRE `recovery_done`
    via `std::mem::take` whenever `req.tasks.is_empty()`, and the
    disk-status loop discarded what it drained. Whichever loop polled a
    node first after a recovery completed won; when the disk-status
    sweep won, the completion was lost permanently —
    `apply_recovery_done` never ran, the slot stayed pointing at the
    dead/fenced node, the EN's recovered copy became an orphan (reaped
    later by F109/F113 reconcile, but only after being LOADED on the
    next EN restart, where it then makes `handle_require_recovery`
    return PRECONDITION "extent already exists" and blocks
    re-recovery), and the inflight marker survived until the F208 stale
    sweep (~10 min). **Invariant: never add a second `df` caller. If a
    future loop needs disk/liveness info, fold it into
    `node_health_loop` or have it apply any `done_tasks` it drains.**
    Cross-ref: F208 (stale-marker sweep — the safety net that hid this
    bug as a 10-min stall rather than a permanent one), 23 (F211-A
    node_states, fed here on df ok/fail), 26 (F224 limiter reseed,
    added here in the same loop).

26. **F224 recovery rate limiter is reseeded from the ledger each
    dispatch tick (it is NOT manually released).** Pre-F224
    `RecoveryRateLimiter::try_acquire`/`release` were never called in
    production — only in unit tests — so `recovery-stats`
    `global`/per-source/per-target were permanently `0`/empty AND the
    concurrency caps were unenforced (a big fence could still flood
    recoveries, the exact thing F211-H was meant to prevent; only
    `backoff_entries` was live). F224 implements the design the module
    doc always described: at the top of `recovery_dispatch_loop`,
    `reset_counts()` then `seed_inflight(replace_id, node_id)` for every
    Recovery entry in the F207 inflight ledger. The ledger is the source
    of truth (survives leader failover), so re-deriving every tick means
    NO manual release bookkeeping — a completed recovery drops out of
    the ledger and out of the count on the next tick. On NEW dispatches,
    `dispatch_recovery_task` calls `try_acquire(replace_id, candidate)`
    per candidate: cap-hit → try the next candidate; every candidate
    capped → return `Ok(())` (DEFERRED, not a failure — no backoff,
    retried next tick once capacity frees; distinct from
    `candidates.is_empty()` which is a real `Err` → backoff). RPC-failure
    paths `release` the slot they took (so an intra-tick failure doesn't
    over-count before the next reseed). **Invariant: do not add manual
    `release` calls in apply_recovery_done / drain — the per-tick reseed
    is the single source of truth; a stray release would double-count
    down.** Worst case after leader failover: one tick at pre-F211-H
    concurrency before the first reseed. Cross-ref: 25 (F222, same loop),
    21 (F207 ledger = the reseed source), 27 (per_target vs EN cap).

27. **`RecoveryRateLimiter.max_per_target` (manager) should track the
    extent-node's `ConcurrencyController.recovery_max` (EN).** These two
    caps bound the SAME physical quantity — "concurrent recoveries
    landing on one extent-node" — at two layers, and form
    defense-in-depth, NOT a conflict (they live in different processes,
    so they cannot and should not be merged into one object):
    - manager `max_per_target` (default **2**, env
      `AUTUMN_MGR_RECOVERY_MAX_PER_TARGET`): throttles DISPATCH fan-out
      (network, "don't flood a target node").
    - EN `recovery_max` (default **2**, flag `--recovery-parallelism` /
      env `AUTUMN_EXTENT_RECOVERY_PARALLELISM`): caps EXECUTION (each
      `run_recovery_task` holds ~payload×2 RAM; see
      `crates/stream/CLAUDE.md` ConcurrencyController).
    Aligned at 2 by default. If manager `max_per_target` is set HIGHER
    than EN `recovery_max`, the surplus dispatches are not dropped —
    they block in the EN's `acquire_recovery()` 50 ms backoff loop until
    a permit frees (correct, just wasted dispatch RPCs + ledger churn).
    If set LOWER, the EN never reaches its RAM ceiling (manager is the
    binding constraint). Keep them equal unless you deliberately want
    one layer to be the bottleneck. The other limiters are unrelated:
    the PS `RateController` (byte-rate) and PS/EN `ConcurrencyController`
    (RAM permits) protect different resources in different processes;
    F196 D-r6→D-r7 deliberately SPLIT rate from concurrency after a
    failed merge, so do not fold byte-rate and concurrency caps together.
    `RecoveryRateLimiter` is a concurrency + per-(extent,slot) backoff
    limiter (no byte-rate dimension) — closest in spirit to the
    `ConcurrencyController`s, not to `RateController`.

28. **F227 commit/seal: append is all-replica-ACK; the manager seal is
    LENIENT (seal-over-reachable), NOT quorum and NOT strict-all-committed
    (supersedes the F156 / F210-B2 majority-quorum).** This is a WAS stream
    layer. The GUARANTEE comes from the APPEND path being all-replica-ACK
    (`client.rs::apply_completion` acks only when every replica wrote), so
    the acked prefix is present on EVERY committed member. Therefore `min`
    over the REACHABLE committed members is always ≥ the acked length and
    never drops acked data — no matter which members are down at seal time.

    **The seal MUST stay lenient — do NOT revert it to strict
    (user decision 2026-05-29; see the F227 seal-lenient note in
    `docs/ops.md`, WAL self-heal section).** You
    seal precisely BECAUSE a node went down; requiring every committed
    member to respond would block the seal forever (that was bug #3's
    seal-wedge). The pre-F227 majority-quorum + min-over-responders was a
    DIFFERENT bug: a quorum subset `min` could sit BELOW the acked length
    (include a short / catching-up replica → next append's `header.commit`
    truncates acked data → silent loss). seal-over-reachable avoids BOTH:
    it excludes catching-up members AND never blocks on an unreachable one.

    The two seal/commit sites — `handle_stream_alloc_extent` failover seal
    (`req.end == 0` branch only; `req.end > 0` trusts the writer's own
    all-acked commit, the ideal exact seal with no probe) and
    `handle_check_commit_length` — both:
    - **exclude catching-up members** via `recovering_nodes_for_extent`
      (in-flight Recovery in the F207 ledger; a re-replication target holds
      a partial replica and must never lower the `min`),
    - probe committed members and feed results to the shared pure
      `compute_commit_seal(members, recovering, responses, floor)`,
    - seal at `min` over the **REACHABLE** committed members, requiring
      only `floor` of them to respond (NOT all). An unreachable committed
      member gets its `avali` bit left UNSET → reconciled by recovery /
      re_avali later; it does NOT block the seal.
    - `AUTUMN_MGR_SEAL_DURABILITY_FLOOR` (default 1) is the min number of
      committed members that must exist + respond — a durability floor, not
      a quorum vote on the commit *position* (position is always `min` over
      the responders).

    **Phantom-commit is ACCEPTABLE, not a bug to fix.** seal-over-reachable
    can promote an un-acked-but-replicated tail byte to committed (data
    *gain*, never *loss*) — e.g. an append reached the two reachable
    members but not the unreachable one. This aligns with the system's
    existing uncertain-write semantics (`feedback_chaos_timeout_uncertain`:
    a timed-out PUT may still land). Do NOT add strict-mode / watermark
    threading to kill it — that trades a benign data-gain for a real
    data-LOSS risk if the watermark is ever imperfect.

    **Truncation of beyond-commit bytes stays correct** — they are un-acked
    speculation; do NOT add a floor that retains them.

    **Backward-incompat (behavioral, no wire change):** the manager seal
    proceeds over the reachable members instead of a majority vote (WAS
    seal-over-reachable). Companion stream-side WRITE-path changes (NOT the
    seal): `current_commit` (all-replica — append tail init) and
    `await_extent_synced_to` flush barrier (all-replica), and
    `ensure_tail_initialised` propagates a `current_commit` failure instead
    of seeding cursor 0 (the old `unwrap_or(0)` made the next append's
    `header.commit=0` truncate every replica to 0). See
    `crates/stream/CLAUDE.md`. Pure-fn tests:
    `rpc_handlers::f227_commit_seal_tests`. Cross-ref: notes 21 (F207
    ledger = the catching-up signal), 23/24 (F211/F214 fence → recovery
    lifecycle that reconfigures dead members out);
    the F227 seal-lenient note in `docs/ops.md` (WAL self-heal section).

29. **F228 background-loop resilience — bound every await (1A) + supervise
    every loop (1C).** The node_health_loop production freeze (note 25 / the
    F227 incident) had two structural enablers that span ALL background
    loops, not just node_health:
    - **1A — no unbounded awaits.** A loop that hangs on an await never
      returns and silently stops doing its job. Audited all 9 loops: the
      only unbounded awaits reachable were (a) `autumn-etcd`'s `unary_call`
      (etcd-over-h2c has no request deadline — see `crates/etcd/CLAUDE.md`)
      and (b) `ConnPool::get_or_connect`'s `RpcConn::connect`, which sat
      OUTSIDE `call_timeout`'s wrapper (a hung TCP connect wedged the loop
      despite call_timeout). EN/PS *request* RPCs already used
      `call_timeout` (`commit_length_on_node` / `probe_extent_on_node` 5 s,
      `extent_delete` 10 s, `df` 5 s, `require_recovery` 5 s) — no bare
      `pool.call(` exists. Fixes: etcd `unary_call` timeout
      (`AUTUMN_ETCD_REQUEST_TIMEOUT_MS`, 10 s) + `get_or_connect` connect
      timeout (`AUTUMN_MGR_CONNECT_TIMEOUT_MS`, 5 s).
    - **1C — no unsupervised loops.** Pre-F228 every loop was
      `compio::runtime::spawn(...).detach()` — a panic killed that one task
      silently while the manager looked alive. `spawn_supervised(name,
      make)` runs the loop under `AssertUnwindSafe(make()).catch_unwind()`;
      on panic OR unexpected return it logs `ERROR bg_loop=<name>` and
      restarts after 1 s with a fresh `mgr.clone()`. All 9 loops route
      through it (`start_runtime_tasks`). `ps_liveness_check_loop` changed
      `&self` → `self` so its future owns the per-restart handle (the other
      8 were already `self`).

      **Note on layered `catch_unwind`.** `compio::runtime::spawn` itself
      wraps the future in `AssertUnwindSafe(future).catch_unwind()`
      (`compio-runtime-0.11.0/src/runtime/mod.rs:202`); its `JoinHandle<T>`
      is a `Task<Result<T, Box<dyn Any + Send>>>`. That's what made
      "silent death" possible pre-F228: compio caught the panic, the
      `.detach()`'d JoinHandle dropped the captured `Err`, the thread
      survived, no log surfaced. F228's explicit inner `catch_unwind` is
      for **observability + restart decisioning** — we read the `Result`
      to log + sleep + restart, instead of letting compio swallow it.
      Compio's outer wrap then sees a future that never panics (we already
      caught it), so the two layers are not bug-redundant; "removing the
      duplicate" would silently break the restart loop. Same shape mirrored
      on the PS side as `spawn_supervised` + `spawn_failstop` (see
      partition-server CLAUDE.md note 13).

    **Why both, not either:** `catch_unwind` cannot rescue a *hung* await (a
    stuck future never returns, so the supervisor waits forever too) — 1A
    prevents the hang. 1A cannot catch a *panic* — 1C does. **Invariant:
    never add a bare `spawn(...).detach()` for a manager loop (use
    `spawn_supervised`), and never add an unbounded await reachable from a
    loop (etcd → `unary_call`; any new pool RPC → `call_timeout`; any new
    connect → bound it).** Optional follow-up (not built): a per-loop
    heartbeat watchdog for stall *observability* — structurally redundant
    once 1A bounds awaits + 1C catches panics, but would surface a future
    unknown-cause stall faster than the F208 10-min sweep did. Cross-ref:
    note 25 (F222 — node_health_loop is the single df + apply caller, the
    thing that froze).

30. **F233 backoff is independent of the recovery marker, and now carries
    a reason.** Two facts operators (and future readers) get wrong:
    - **Backoff ≠ marker; backoff never stops retries.** The F207 Recovery
      inflight marker exists ONLY while a recovery is in-flight (released
      atomically by `apply_recovery_done`, note 21 I3). Backoff
      (`recovery_rate_limiter.backoff`, keyed `(extent_id, slot)`, IN-MEMORY
      only) just delays *re-dispatch* of a failed slot by `2^N s` (cap
      300 s). Candidates are re-derived every 2 s tick from `s.extents`
      (note: `recovery_dispatch_loop` scans the store, not a marker), so a
      dead slot keeps being retried forever until `record_success` clears
      it — **there is no give-up / abandon for recovery** (unlike
      ConvertToEc's fence auto-abandon or Delete's F210-G2 retry queue).
      Because backoff is in-memory, manager restart / leader-failover
      RESETS it → immediate retry (no window). So `backoff_entries=0` means
      "nothing in a failure-backoff window right now", NOT "not retrying".
    - **The failure reason is captured.** Pre-F233 `record_dispatch_outcome`
      took `ok: bool` and threw away the `dispatch_recovery_task` error;
      `BackoffState` had no reason. Now it takes `&Result<(), AppError>`,
      stores `e.to_string()` in `BackoffState.last_reason`, and
      `backoff_snapshot()` / `RecoveryStatsResp.backoff` /
      `autumn-op recovery-stats` expose per-entry `(extent, slot,
      consecutive_failures, last_attempt_at, next_retry_at, reason)`.
      **Invariant: keep `record_dispatch_outcome` taking the `Result` (not
      a bool) so the reason is never silently dropped again.** Cross-ref:
      notes 21 (F207 marker lifecycle), 24 (F224 limiter reseed), 26.

31. **`handle_stream_alloc_extent` on an ALREADY-SEALED tail must NOT
    re-write the tail (seed=13, 2026-05-30).** Two coupled facts: (a) every
    F207 ledger op (Recovery / ConvertToEc / Delete) acts ONLY on a SEALED
    extent, so an in-flight op on the tail ⇒ the tail is already sealed;
    (b) when the tail is already sealed, alloc does NOT change any tail field
    (`already_sealed` skips the seal block). So the early-snapshot
    `tail.clone()` writeback (etcd `mirror_stream_alloc_extent` + in-mem
    `s.extents.insert`) was BOTH pointless (rewrites an identical snapshot)
    AND a clobber hazard — a concurrent Recovery completing during the mirror
    RTT bumps `tail.replicates`/`eversion`, which the stale clone overwrites
    (the coco-found P1 that reverted the first naive guard-lift). Fix:
    - The refuse-at-start `extent_inflight_op(tail_id)` probe is gated on
      `tail.sealed_length == 0` (it only ever fired on already-sealed tails,
      so this lifts the wedge where a stuck Recovery on the sealed tail
      blocked new-extent allocation — see `crates/stream/CLAUDE.md` recovery
      reconciliation for WHY the recovery was stuck).
    - `mirror_stream_alloc_extent`'s `sealed_old` param is now
      `Option<&MgrExtentInfo>` — `None` on the already-sealed path (skip the
      tail etcd write); the in-mem `s.extents.insert(tail_id, …)` is likewise
      skipped. The sealer already durably persisted the tail, so skipping
      loses nothing and the concurrent Recovery's writeback is never clobbered.
    - The eversion verify-before-mirror (F210-A2) runs only for
      `!already_sealed` (it guards the tail re-seal, irrelevant when we don't
      touch the tail). A NEW **stream-membership baseline verify runs for BOTH
      paths** (coco P1): re-read the live stream before mirror+apply and refuse
      (Precondition) if its `extent_ids` no longer match the baseline
      `stream_after` was built from — else a concurrent `punch_holes` /
      `truncate` / `split` could be clobbered by the `stream_after` writeback
      (extent resurrection / membership rollback). The narrow etcd-mirror-RTT
      residual is the same F210-A1-followup window the eversion verify documents.
    Cross-ref: notes 13 (F146 verify-at-apply), 21 (F207 ledger Class A
    refuse-at-start), 28 (F227 lenient seal — the source of the phantom
    `sealed_length` that stalled the recovery).

32. **`MgrExtentInfo.sealed: bool` is the authoritative seal STATE; the failover
    seal is `StreamAllocExtentReq.seal_commit: Option<u32>` (seed=13 Mode A
    prevention).** Two coupled cleanups:
    - **`sealed` bool.** `already_sealed = tail.sealed` (NOT `sealed_length >
      0`), so an authoritative EMPTY seal (`sealed = true, sealed_length = 0` —
      a tail where nothing was ever all-acked, or a CoW-shared empty tail) is
      UNAMBIGUOUS. `sealed` = STATE ("is sealed"); `sealed_length` = LENGTH
      ("how much / is empty"). Invariant `sealed_length > 0 ⇒ sealed`. EVERY
      `sealed_length =` mutation also sets `sealed = true` (the alloc seal +
      all 5 split/merge tail seals). "Is-sealed" reads use `.sealed`
      (`already_sealed`, `handle_check_commit_length`); "is-empty/no-data"
      reads (recovery-dispatch skip, EC advisory min-size) KEEP `sealed_length`
      — they mean "nothing to recover/EC", not "is sealed".
    - **`seal_commit: Option<u32>`** replaces the `end: u32 + authoritative_
      commit: bool` pair. `Some(c)` ⇒ AUTHORITATIVE: seal at EXACTLY `c` (even
      0, no probe) — `c` is the writer's quiesced `state.commit` from the
      stream-side SealCommit handshake (see `crates/stream/CLAUDE.md` note 20),
      so a probe never promotes a speculative byte into `sealed_length` (the
      phantom). `None` ⇒ PROBE via `compute_commit_seal` (genuine new-owner
      takeover only).
    - **CoW empty-tail seal (coco P1).** `compute_duplicate_stream` (split) +
      `compute_merge_streams` / `splice_streams_without_new_tail` (merge) seal
      the shared old tail EVEN when its captured commit_length is 0 (the
      `&& *_sealed > 0` guard was DROPPED — now just `!ex.sealed`). An empty
      CoW-shared tail must be frozen (`sealed = true`, `avali` set) or BOTH
      child streams' writers would append to the same open extent (CoW
      isolation break). Sealed-empty ⇒ each child's `ensure_tail_initialised`
      allocs a fresh tail instead of sharing it.
    Wire change (`MgrExtentInfo` + `StreamAllocExtentReq`) is SAME-COMMIT deploy
    (repo convention — `cluster.sh reset` wipes etcd + restarts the whole
    cluster; no rolling upgrade). Known gap (pre-existing, deferred): the EN
    `.meta` sidecar does NOT persist `sealed` — but stale appends to a
    sealed-empty extent are eversion-fenced (eversion IS in `.meta`), same as
    any seal the EN hasn't been pushed yet. Cross-ref: note 28 (F227 lenient
    PROBE seal — still used for the `None`/new-owner path), 31 (alloc
    already-sealed no-tail-rewrite).

32a. **`handle_stream_alloc_extent` is IDEMPOTENT on retry via
    `StreamAllocExtentReq.seal_extent_id` (BUG2-IDEMPOTENT-ROLL, chaos
    seed=603, WIRE v8).** `seal_and_roll` was non-idempotent: the writer
    captures `seal_commit` (the worker's drained commit) for a specific tail T
    via the `SealCommit` handshake, then calls `alloc_new_extent(Some(commit))`
    to seal T + roll a fresh tail T'. If that alloc SUCCEEDS on the manager
    (seals T, rolls T') but its response is LOST (chaos latency), the writer
    retries with the SAME `seal_commit` — and the manager, whose current tail is
    now the fresh OPEN T', would **seal T' at the stale `seal_commit`**,
    over-sealing an extent that does NOT durably hold that many bytes → T' is
    unrecoverable → any partition replaying it (a split child CoW-sharing the log
    stream) hits WAL-FAILSTOP and NEVER opens (its keys unreachable;
    `mismatches=0`, data not physically lost — it's the seal length that's
    bogus). Fix: the writer pins `seal_extent_id` = T (the `SealCommit` handshake
    now returns `(commit, tail_extent_id)`; `seal_commit_watermark` →
    `alloc_new_extent(stream, Some(commit), seal_extent_id)`), reused verbatim
    across every `retry_manager_call` attempt. The manager seals ONLY when the
    current tail still equals `seal_extent_id` AND is OPEN; otherwise (the
    retried/stale case — T already sealed + rolled) it is an **idempotent no-op**
    that returns the current tail untouched. **`!tail.sealed` is load-bearing
    (coco P1):** if the current tail is itself SEALED (a later op rolled+sealed
    past T'), the no-op would hand a sealed extent back as a "fresh" tail → the
    writer's appends fail → roll/retry wedge; instead it falls through to the
    `already_sealed` path which preserves the seal AND allocs a NEW open tail.
    `seal_extent_id == 0` = no pinned target (probe / `None` seal /
    `seal_and_roll_tail`) → normal path. Does NOT touch F227 `compute_commit_seal`
    semantics — purely a retry-idempotency guard. Same non-idempotent-retry
    family as [[project_split_retry_cascade]]. Cross-ref: notes 28 (F227 lenient
    seal), 31 (already-sealed no-rewrite), 32 (sealed bool + authoritative
    seal_commit).

33. **Stream-membership etcd writes value-CAS on `streams/<id>` (Item 3 —
    resurrect-deleted-extent fix).** The manager is single-threaded but handlers
    interleave at every `.await`. `handle_stream_alloc_extent` (adds an extent),
    `handle_stream_punch_holes` / `handle_truncate` (remove extents) all do
    read-baseline → `.await`(EN-alloc + etcd-mirror) → write-baseline-back. The
    `.await` is the gap: a `punch_holes` committing DURING an `alloc`'s
    etcd-mirror RTT was overwritten by alloc's stale baseline → the removed
    extent RESURRECTED in both etcd and memory (lost GC / dangling stream ref).
    `verify-before-mirror` (F146/F210-A2) only catches mutations BEFORE the
    await — the real linearization point is the etcd commit, which was a CAS-less
    last-writer-wins put.
    Fix: the mirror txn now value-CAS's `streams/<id>` against the membership
    BASELINE the handler read (`mirror_stream_alloc_extent` /
    `mirror_stream_extent_mutation` take `stream_cas: Option<Vec<u8>>` =
    `rkyv_encode(stream_as_read)`; `EtcdMirror::put_delete_txn_cas` prepends
    `Cmp::value(streams/<id>) == baseline` to the F149 fenced txn). On conflict
    the fenced txn returns `Ok(false)` → `AppError::Precondition` → the client
    retries with a fresh snapshot. The CAS baseline byte-matches etcd because
    etcd-first keeps `streams/<id>` == `rkyv_encode(in-memory stream)` and rkyv
    is deterministic (verified: 0 spurious CAS conflicts across chaos).
    **Why CAS, not a per-stream serialization lock:** a lock held across the
    handler BLOCKS the write-path `alloc_new_extent` behind a slow concurrent
    GC/split/merge under kill → A/B-measured 2/6 lost writes (`not_found`). CAS
    never blocks — conflicting ops proceed; only a genuine conflict retries
    (A/B: 0/8 with CAS). **Invariant: any read-modify-write of a `streams/<id>`
    membership MUST value-CAS the write against the read baseline — never a bare
    last-writer-wins put.** Two accepted residuals (pre-existing, not worsened):
    a CAS-failed alloc orphans the just-created extent files (same as the
    F146/F210-A2 verify-precondition path; reaped by F109 reconcile; CAS
    conflicts ~0), and GC/compaction callers don't client-retry but their
    background loops re-attempt (`classify_gc_failure_cooldown` maps
    `precondition failed` → 30 s soft cooldown).

    **Uniform-CAS coverage — the MEMBERSHIP (stream `extent_ids`) class is now
    COMPLETE.** `put_delete_txn_cas` takes a `Vec` of `(key, baseline)` pairs
    (etcd ANDs the value-compares). The stream-membership read-modify-write
    mutators all CAS the touched `streams/<id>` against the pre-mutation
    baseline: `handle_stream_alloc_extent` / `handle_stream_punch_holes` /
    `handle_truncate` (single stream) + `handle_multi_modify_merge` (the splice
    of victim extents into the survivor's `extent_ids` — CAS's all 3 survivor
    streams' pre-splice baselines). `handle_multi_modify_split` does NOT mutate
    an existing stream's membership (it CREATES new child streams + seals the
    source tail), so the membership-resurrect race does not apply to it.
    **Validated:** merge-heavy chaos 8/8, merges succeed with 0 spurious CAS
    conflicts (rkyv baseline byte-matches etcd).

    **EXTENT-STATE `refs` CAS — SHIPPED for all PS-op writers (was the deferred
    follow-up; the `refs` sub-dimension WAS reproduced, 2026-06-28).** The
    `refs` extent-state read-modify-write on the four PS-op handlers now
    value-CAS's `extents/<id>` against its pre-mutation baseline:
    `handle_stream_punch_holes` / `handle_truncate` (via
    `compute_extent_ref_drops` returning per-extent CAS baselines for both the
    put and delete paths — 139b023) and `handle_multi_modify_split` /
    `handle_multi_modify_merge` (each modified extent's baseline added to the
    Phase-2 txn — 35acdfe + b05eab1). What flipped this from "deferred /
    near-precluded" to "ship it": a chaos fuzz-monitor REPRODUCED the clobber
    (seed 769351064, 3/4 FAIL) — a concurrent cross-partition punch on a
    CoW-shared extent during another PS-op's etcd RTT lost a `refs` decrement →
    orphan-leak (`refs > 0`, in no stream); the systematic study found
    DATA-LOSS variants too (split∥punch / split∥split lose a `refs` INCREMENT →
    `refs` too low → premature extent delete → a CoW child loses data;
    cross-partition because the per-partition `gc_gate` only serialises
    same-partition split-vs-GC). Post-fix: seed 769351064 ×4 + 603 + randoms,
    accounting errors = 0.

    **Split vs merge capture asymmetry (load-bearing — coco P1, b05eab1).**
    `handle_multi_modify_merge` MUST capture each extent's CAS baseline in
    **Phase 1** (carried in `Phase1Result.extent_baselines`), not in Phase 2,
    because merge has a **Phase-1.5 `alloc_extent_on_node` await** for the new
    tail: a Phase-2 capture (after that await) would read an already-mutated
    value (CAS passes → clobber) or a deleted extent (`s.extents.get` None → CAS
    skipped → stale PUT resurrects). `handle_multi_modify_split` captures in
    **Phase 2** and that is correct: its ONLY await is the Phase-2 etcd write
    itself (no Phase-1.5 alloc — split is CoW, it shares extents), so there is
    ZERO await between `modified_extents` being computed and the baseline
    capture → consistent.

    **Why the STREAM-membership of split-source / merge-victim is intentionally
    NOT CAS'd (coco re-review triage, 2026-06-28).** A coco /findbugs pass
    flagged that split reads the source stream membership (to derive the right
    child) and merge deletes the victim stream membership without CAS'ing
    either. Both are NOT reachable harms once the PS-side coordination is
    accounted for (coco saw only manager code): the split source partition is
    `frozen_for_split` AND holds `gc_gate` + `compact_gate` through the whole
    `multi_modify_split` (PS-side F210-C2 + F140) so its streams cannot mutate
    concurrently (the only reachable race — a DIFFERENT CoW-sharing partition
    GC'ing a shared extent — is caught by the `refs` extent CAS above); the
    merge victim is `frozen_for_merge` so no concurrent alloc can ADD an extent
    (orphan-via-alloc precluded), and `splice_victim_extents` baselines EVERY
    victim extent, so the only reachable victim mutation (a GC punch) trips the
    extent CAS via its `refs`-- write. Inline "why no membership CAS" comments
    at both Phase-2 sites prevent re-litigation.

    **STILL DEFERRED (reproduce-first — NOT reproduced):** the
    eversion / replicates / avali extent-state writes on the STREAM-LAYER
    appliers — `apply_ec_conversion_done`, `apply_recovery_done`, and
    `handle_multi_modify_split`'s source-tail *eversion bump* — remain
    un-CAS'd. The 2026-05-31 reachability investigation still holds for these:
    (a) `apply_recovery_done` reads + writes etcd AWAIT-ADJACENTLY (window =
    write RTT only); (b) the F207 ledger serialises recovery/EC per extent and
    note 31 stops alloc rewriting a sealed extent; (c) split-seal / ec_done
    await gaps are covered before-await by F146 / F207+F138 verify-at-apply.
    The worked CAS example (`apply_recovery_done`: capture
    `rkyv_encode(extent_as_read)` →
    `put_delete_txn_cas([extents/<id>=new], [inflight_key], [extents/<id>=baseline])`)
    is kept ready; apply ONLY if one of these is ever reproduced. Cross-ref:
    notes 13 (F146 verify-at-apply — the weaker before-await form), 15 (F149
    leader fence — the txn already carries it), 32 (sealed state).

34. **F-ioring-lease-1 InodeLease registry (Phase 1 ground floor for
    autumn-fuse coherence).** **NOTE (2026-06-30): the io_uring daemon
    — the original second consumer of this registry — was removed; the
    manager-side registry + 4 RPCs + client `lease.rs` helpers STAY (now
    serve autumn-fuse only). Daemon-side invariants L14–L18 below
    described `crates/ioring/src/bin/daemon.rs`, which no longer exists;
    they are kept as design rationale for the F-fuse-lease consumer.**
    JuiceFS-style
    inode-level lease served by the manager; same etcd backing as
    `acquire_owner_lock`. Daemons appear as `MgrClientId { kind, uuid,
    host }` — `host` is diagnostic only, identity is `(kind, uuid)`.
    Single writer XOR many readers per inode; writers and readers
    can coexist (reads through an open file remain legal — the
    writer's flush-before-close ordering in plan §6.2 keeps coherence
    intact). Pure state machine in
    `crates/manager/src/inode_lease.rs::LeaseRegistry`; 4 RPCs in
    `rpc_handlers.rs` (`handle_acquire_lease` /
    `handle_release_lease` / `handle_heartbeat_lease` /
    `handle_poll_invalidations`, MSG types `0x46`–`0x49`).

    **Invariants (plan §6, enforced by code structure):**
    - **L1** Manager is the single decision-maker. `acquire` returns
      `WriteConflict` synchronously when another client holds the
      writer slot. No client may "decide locally that I'll write
      first" (plan §6 invariant 1).
    - **L2** Writer release bumps `version` BEFORE pushing the
      invalidation so a reader sees the new generation paired with
      the event (close-to-open coherence).
    - **L3** Writer leases are PERSISTED to etcd
      (`inode_leases/<ino>`, F149-fenced via `put_msgs_txn` /
      `put_and_delete_txn`); reader leases are MEMORY-ONLY. Failover
      replay rehydrates writer leases via
      `install_persisted_writer`, clamping the in-memory deadline to
      the configured TTL so a long-future `expires_at` (clock skew)
      doesn't pin a dead writer. Reader-set loss is benign — plan
      §6.4 mandates daemons invalidate all cached versions on
      subscribe reconnect (wired in F-ioring-lease-3).
    - **L4** `LeaseRegistry::tick(now)` captures the reader set
      BEFORE evicting expired readers in the same pass. A reader
      whose lease expires on the same boundary as the writer's TTL
      must still get the invalidation push — its inbox lives
      independently of its lease entry. Order: writer revoke → reader
      expiry → drop-empty-inode → push invalidations.
    - **L5** `inode_lease_revoke_loop` runs under `spawn_supervised`
      (F228 1C) with one bounded `sleep` per iteration + the bounded
      etcd `put_and_delete_txn` call (no unbounded awaits, F228 1A).
      Etcd failure during revoke logs WARN and retries next tick —
      the in-memory revoke already fired, so the worst case is a
      stale persisted record that gets cleaned up later (a new
      leader's TTL pass expires it).
    - **L6** `MgrClientId.host` is diagnostic only — two processes
      that report different hostnames for the same `(kind, uuid)`
      hash to the same `ClientKey`. Tested by
      `host_field_does_not_affect_identity`.
    - **L11** (F-ioring-lease-2 regression fix) **`version` MUST be
      monotonic across the inode entry's full lifetime — not just
      within a single live entry.** Pre-this commit, the auto-
      remove of an empty inode entry on release reset `version` to
      1; a fresh acquire would then hand out `(ino, version=1)`
      which collides with a pre-existing reader cache tagged
      `(ino, version=1)` from the prior generation — close-to-
      open coherence silently broken. Fix:
      `LeaseRegistry.last_version: HashMap<u64, u64>` shadow
      preserves the high-water mark across remove/re-create; every
      `release / tick / acquire / install_persisted_writer` calls
      `remember_version(ino, version)`, and `inode_or_create`
      seeds new entries from the shadow. Tested by
      `version_is_monotonic_across_remove_and_reacquire`.
    - **L12** (F-ioring-lease-3 long-poll) at most ONE parked waker
      per `ClientInbox` at any time. `drain_or_park` REPLACES the
      previous waker transparently; the displaced sender drops →
      the prior handler's `recv` resolves `Err(Canceled)` → it
      treats it as "no events, retry." Correct because a single
      `client_id` is owned by a single in-flight long-poll task
      (per-session in the daemon).
    - **L13** `ClientInbox::push` MUST fire the parked waker before
      returning. Skipping it lets a long-poll wait up to
      `LONG_POLL_WAIT` (10 s) before noticing the event, breaking
      the "writer close → reader sees new bytes within ~ms" close-
      to-open guarantee. Tested by
      `drain_or_park_installs_waker_and_push_wakes_it`.
    - **L14** (daemon-side, in `bin/daemon.rs`) ANY failure of
      `lease::poll_invalidations` OR a `MetaChanged { ino=0 }`
      overflow sentinel MUST drop EVERY held lease AND every
      cached ring_fd in the session before retrying (plan §6.4 —
      "subscribe disconnect = invalidate everything"). Partial
      invalidation is a footgun: a daemon that kept `held_leases`
      while clearing `ring_fds` would refuse subsequent Opens with
      stale EBUSY. This is the only place the daemon's cache can
      ever be wholesale-dropped — the heartbeat loop's NotHeld
      branch only drops the single affected ino. The wholesale-
      clear path also clears the F-ioring-lease-4
      `invalidations` map (otherwise a freshly re-Open'd ino
      would compare against a stale per-ino floor).
    - **L15** (F-ioring-lease-4) `OpenedExtents.lease_version`
      MUST be the `MgrInodeLeaseInfo.version` the manager
      returned at the AcquireLease that opened (or refcounted-
      into) this ring_fd. Refcount-shared ring_fds inherit the
      FIRST opener's version so the per-ino staleness check
      stays single-valued.
    - **L16** (F-ioring-lease-4, daemon Read arm) cache-stale ⇒
      `fuse_read::reload_extents` BEFORE the `read_into`, OR
      EIO. Never serve bytes from a confirmed-stale
      `OpenedExtents` — even a partial read of pre-close bytes
      breaks close-to-open coherence.
    - **L17** (F-ioring-lease-4) reload bumps `lease_version`
      to the per-ino floor (the just-applied invalidation's max
      version), NOT to a fresh AcquireLease's response. The
      next Open of the same path takes its own server-side
      version on the new AcquireLease.
    - **L18** (F-ioring-lease-4) writes do NOT trigger reload.
      The writer holds the writer lease; no other writer can
      have raced this session's cache. Lease preemption
      (F-lease-preempt) would change this; deferred.

    **Out of scope this commit:** daemon Open/Close wiring
    (F-ioring-lease-2), long-poll loop + reconnect-invalidates-all
    (F-ioring-lease-3), `OpenedExtents` version tagging + e2e
    multi-daemon test (F-ioring-lease-4), autumn-fuse opt-in
    (F-fuse-lease-*), force-revoke / writer revoke protocol
    (F-lease-preempt). See `docs/autumn_fs_lease_plan.md` for the
    full plan. Cross-ref: 15 (F149 fence — all `inode_leases/` etcd
    writes route through `put_msgs_txn` /
    `put_and_delete_txn` and carry the fence), 29 (F228 bg-loop
    resilience — `inode_lease_revoke_loop` follows the same
    pattern).

35. **F265 owner_epoch bumps on EVERY acquire; `part_addrs` is in-memory
    and PS-self-healed (both found by manager-kill chaos, 2026-06-11).**
    Two independent fixes from `transport_chaos.sh` E4/E5:
    - **`acquire_owner_epoch` rewrites `ownerLocks/<key>` with an
      unconditional leader-fenced PUT and returns the fresh
      `mod_revision`** (was: create_revision==0 CAS + reuse the stable
      `create_revision` forever). `replay_from_etcd` reads
      `mod_revision` to match — replay MUST stay in lock-step with the
      acquire path or post-failover `ensure_owner_epoch` equality
      rejects every live owner. Why: a stable per-key epoch can never
      support ownership failback A→B→A (B's later-created key had a
      higher revision; once B touched an extent, the EN floor sat above
      A's frozen epoch forever — observed as partition open wedged on
      `commit_length ... CODE_LOCKED_BY_OTHER` from all 3 ENs after
      PS2-died-partitions-return-to-PS1). It also let two live
      processes acquiring the SAME owner_key share one epoch — no
      mutual fencing (split-brain). Bump-on-acquire gives newest-
      acquirer-wins: each PS incarnation acquires once at startup and
      keeps the epoch for its lifetime; per-partition StreamClients
      inherit it via `new_with_owner_epoch`. etcd revisions are
      globally monotonic, so cross-key comparisons at the EN floor
      stay correct. Memory-mode `MetadataState::acquire_owner_lock`
      mirrors this (common crate invariant 2).
    - **`handle_register_partition_addr` is NOT leader-gated** and
      `part_addrs` is deliberately NOT mirrored to etcd. It is a
      routing hint lost on manager restart; the PS re-reports it from
      `sync_regions_once` (~2 s tick) whenever the GetRegions response
      shows the manager's view missing/stale for a partition the PS
      serves. Pre-F265 nothing re-reported it (registration only
      happened inside `open_partition`), so a manager restart under a
      healthy cluster black-holed ALL client routing until partitions
      were reopened somewhere — a 30-minute outage in the chaos run,
      self-healing only by accident at the next PS failover. Gating on
      leadership would just stretch the outage by the election wait;
      a follower accepting the in-memory hint is harmless (idempotent,
      continuously refreshed).
    Cross-ref: note 3 (ensure_owner_epoch before stream mutations),
    15 (F149 fence — the acquire PUT carries it), 28 (F227 commit
    probe — the path the failback wedge blocked).

    **F265 addendum — `serving` gate on the eviction sweep.** `serve()`
    calls `mark_serving()` AFTER the listener bind returns: it re-seeds
    every `ps_last_heartbeat` clock and flips `serving=true`;
    `ps_liveness_check_loop` skips while `!serving`. Rationale: the ucx
    listener bind retries through a killed predecessor's TIME_WAIT for up
    to ~60 s (F264), and the respawned manager wins the election within
    seconds — pre-F265 it then evicted the ENTIRE healthy PS fleet at
    +10 s, while no PS could possibly heartbeat into the unbound socket.
    Companion PS-side change: heartbeat-loss exit threshold 10 s → 90 s
    (partition-server CLAUDE.md note 18).

    **etcd-chaos addendum (2026-06-12) — STALE-WHILE-LEADERLESS serving +
    audit GC wiring.** Three production fixes from the first-ever etcd
    kill/outage/restart chaos (`scripts/etcd_chaos.sh`, D1/D2/D3):
    - `displaced: Rc<Cell<bool>>` (default TRUE; cleared on winning the
      election; set when the election CAS or an F149 fence diagnosis
      observes a DIFFERENT instance holding the leader key — a missing
      key is lease-expiry, NOT displacement). `ensure_routable()` =
      `leader || !displaced` gates the two READ-ONLY routing/liveness
      RPCs (`get_regions`, `heartbeat_ps`); every mutating handler stays
      on the strict `ensure_leader`. Rationale: during an etcd outage
      the ex-leader's in-memory routing is the freshest in existence and
      NOTHING can supersede it (no election, no mutation) — pre-fix the
      strict gate black-holed every fresh client for the whole outage
      (D1) while cached-routing clients sailed through. The F267 H3
      blackhole stays closed: a rejoined follower is `displaced`.
      BOUNDED (coco P1): the mode lives at most `ROUTABLE_STALE_TTL`
      (15 min) from `leaderless_since` — in an ASYMMETRIC partition
      (only this manager lost etcd; a peer takes over) displacement is
      only detected once OUR etcd link recovers (the election CAS sees
      the new holder), so without the TTL this manager would pin the PS
      fleet to itself indefinitely while the real leader evicts them.
      Within the window that pinning is self-healing: PSes hit the TTL,
      get NOT_LEADER, rotate to the real leader, re-register via the
      heartbeat NOT_FOUND path.
    - PS-side: `MAX_CONSECUTIVE_NOT_LEADER = 450` (15 min) is a SEPARATE
      heartbeat exit budget from the 90 s transport budget — NOT_LEADER
      proves the manager is REACHABLE (not a network partition), and a
      leaderless control plane cannot evict anyone; sharing the 90 s
      budget made a routine >90 s etcd maintenance window suicide the
      whole PS fleet (reproduced pre-fix: D2 PS 1→0, then total outage +
      false "loss"). With `ensure_routable` heartbeats answer OK through
      the outage anyway; the 15 min budget covers the multi-manager
      follower-pinned case (bounds stale READS only — data safety is
      owner_epoch/region_epoch fencing, never this exit).
    - `audit_gc_loop` (daily, leader-only): `audit_retention_gc` existed
      since F211-I but had NO caller — `mgr_audit_log/` grew in etcd
      unboundedly. `--audit-retention-days` (default 90, 0=off; the
      helper's env read converted to CLI per the F195 rule).
    Validated: etcd_chaos D1 writes progressing 14 s into the outage,
    D2 fleet intact through 150 s, D3 instant recovery, 649 ACKed keys
    zero loss; manager-HA chaos (H1-H3) regression PASS.

    **F267 addendum — `handle_get_regions` / `handle_heartbeat_ps` are
    leader-gated (CODE_NOT_LEADER).** Reverses the F265 "ungated
    get_regions" stance: a rejoined FOLLOWER serves replay-stale regions
    with an EMPTY in-memory `part_addrs` — clients that connect to it
    first (manager-list order) black-hole completely, and a follower
    answering heartbeats OK pins the PS fleet's shared rotation index to
    itself (manager-HA chaos H3). Followers now answer NOT_LEADER and
    every caller rotates (PS heartbeat/sync, SDK refresh_regions, F267
    StreamClient note_manager_code). `handle_register_partition_addr`
    STAYS ungated (idempotent in-memory hint, continuously re-reported).

37. **ENOSPC-1: allocation soft-avoids space-low nodes.**
    `node_health_loop` stashes each node's MAX per-disk free from every
    successful df probe into `node_max_free` (in-memory only — a 2 s-
    fresh routing hint; the df payload's per-disk ids are EN-local and
    unmapped per note 7, but the max needs no id mapping).
    `select_nodes` filters `healthy` down to nodes not below
    `min_alloc_free_bytes` (`--min-alloc-free-bytes`, default 256 MiB,
    0 = off; `set_min_alloc_free_bytes`); when that under-fills the
    selection it falls back to the full healthy set — a capacity-
    crunched cluster still attempts allocation (the EN-side `Full` gate
    fails fast and the per-RPC fallback walks) rather than refusing.
    Unknown nodes (no df yet) are spacious — cold leader keeps
    allocating. EN-side counterpart: stream CLAUDE.md note 25a.

36. **/metrics (observability batch 1).** `AutumnManager::metrics_text()`
    renders leader/serving gauges + store counts (streams / extents /
    nodes / partitions / ps_nodes / regions / part_addrs), per-disk
    online (the df call-result signal, note 7) and the F207 inflight-op
    count. The store is `Rc<RefCell>` (!Send), so the binary's
    `--metrics-port` path runs a 2 s publisher task ON the compio runtime
    that copies the rendered string into an `Arc<RwLock<String>>`; the
    `autumn_common::metrics_http` listener thread serves the latest copy.
    A follower's counts reflect its replay-stale view — scrape
    `autumn_manager_leader` to pick the authoritative instance.

38. **R1 cluster_version (rolling upgrade design §3-R1).** Etcd key
    `autumn-rs/cluster_version`, ASCII decimal (deliberately NOT rkyv — it
    gates serialization-era transitions so it must outlive them all).
    CAS-imprinted by the first leader to its own `WIRE_VERSION_MAX`
    (`imprint_cluster_version`, same shape + best-effort semantics as
    F214-A's cluster_id — safe because no code gates on it yet and a bump
    against a missing key CAS-fails). `bump_cluster_version`: leader-only
    + exactly current+1 + capped at this binary's WIRE_VERSION_MAX +
    value-CAS against the current ASCII value (racing bumps can't both
    land; CAS-loser heals its in-memory copy and refuses).
    **Rollback fail-closed:** `parse_cluster_version` — the ONLY decode
    point (imprint / CAS-lost re-reads / replay / bump-heal) — refuses a
    persisted value above this binary's WIRE_VERSION_MAX; through replay
    that blocks an old binary from becoming leader after a bump (the
    "bump 后不可滚回" rule, enforced not advisory).
    `handle_get_cluster_version` does a FRESH etcd read (rare operator
    RPC; a follower's in-memory copy is replay-stale after a bump).
    `GetClusterIdResp` carries `{wire_version_min, wire_version_max,
    cluster_version}` for the startup handshake — that struct is FROZEN
    from R1 on (see rpc CLAUDE.md R1 section).

39. **升级安全 = 全停全启 + rkyv fail-loud(2026-06-13;R2-A prost 已回退)。**
    生产升级 = 全停→换二进制→全起,etcd 永不清(绝不 `cluster.sh reset`)。
    安全保证来自 **rkyv 校验式 `from_bytes`**:新二进制读旧 etcd,若持久
    结构布局不符则响亮失败(`replay_from_etcd` 经 `replay_decode_err` 报
    可操作提示 → manager 当不上 leader),**绝不静默把旧字节解成错值**。
    R1 的 `cluster_version`(note 38)作格式版本戳:回滚到读不懂当前 etcd
    的旧二进制被 `parse_cluster_version` 启动拒绝(回滚 fail-closed)。
    **etcd schema 演进按需补**:第一次真要不 reset 改某持久结构时,那个
    版本带一次性迁移(读旧→写新,幂等 + 格式戳收尾);在那之前零常驻演进
    代码。**不变量:任何持久结构(etcd 值 / SST / .meta / WAL)改动都要么
    保证 rkyv 同布局,要么随版本带迁移 —— 绝不依赖 reset。** 曾用 prost 给
    "免 reset 自动演进"(R2-A),因纯复杂度 + 对"忘迁移"反而静默风险更高
    而回退;详见 docs/rolling_upgrade_design.md §R2-final + [[feedback_stopworld_restart_primary]]。

40. **CLUSTER-DF capacity snapshot (`cluster_cap`, serves `MSG_CLUSTER_DF`).**
    Ceph-`ceph df`-style aggregate capacity, in-memory only (volatile, rebuilt
    from df + scan — never persisted; leader-only meaning). Two sources feed
    `ClusterCapSnapshot`, both inside the single `node_health_loop` (note 25 —
    do NOT add a second df caller):
    - **RAW + physical_used: summed from the EN df reports every tick.** The EN
      is the data owner — `handle_df` self-reports `DiskStatus.extent_bytes` =
      Σ its `ExtentEntry.len` (real on-disk bytes: replicas counted ×N, EC
      shards at shard size, open tails at live length). The manager just sums
      online-disk `total/free/extent_bytes` per node → `raw_total/raw_free/
      physical_used`. **No amplification formula, no extent scan, no mutation-
      site counters** — this is the Ceph/HDFS "owner reports, control plane
      sums" pattern, chosen because the manager's extent mutations are
      scattered (no choke point) so an incremental manager counter would mean
      6 revert-prone hot-path edits.
    - **logical_stored: a periodic (~30 s) READ-ONLY scan** of `s.extents`,
      `Σ distinct sealed_length` skipping `refs==0 && vp_table_refs==0`
      (pending physical delete). Read-only (touches no mutation site); pure
      in-memory CPU (MetadataState is the etcd mirror). Slower cadence than
      the df sum because it's O(extents); df/statfs read the cached value.
    `handle_cluster_df` is leader-gated (follower → CODE_NOT_LEADER; its
    snapshot is replay-stale + its loop doesn't run) and just copies the
    snapshot — O(per_node), no compute. Wire carries only raw u64 facts; the
    amplification factor (`physical_used/logical_stored`) and the EC-dependent
    writable RANGE `[raw_free/3 .. raw_free/best_ec]` are computed by the
    consumer (autumn-op df / fuse statfs). `node_max_free`/ENOSPC-1
    (note 37) is untouched. Cross-ref: note 25 (single df caller), 7 (df
    call-result vs payload disk_id).

41. **EXTENT10-AUTORECLAIM both-zero sweep (`extent_both_zero_sweep_loop`,
    2026-06-18).** A `refs==0 && vp_table_refs==0` extent that is in NO stream
    was never auto-deleted: the refs-side trigger lives in
    `handle_stream_punch_holes` / `handle_truncate`, which only inspect CURRENT
    stream members. An extent that lost its last membership out-of-band sat at
    both-zero with no path firing its delete — the extent-10 orphan class, leaked
    until manual reclaim. The leader-only sweep (`extent_delete.rs`, 60 s,
    `spawn_supervised`) reclaims them. **Candidate gate (load-bearing):**
    `extent_can_delete(ex)` (`refs==0 && vp_table_refs==0`) AND the extent is
    **absent from every stream's `extent_ids`** — the membership check is NOT
    redundant with `refs==0`: a refs under-count must never let the sweep delete
    a still-membered extent (a both-zero-but-in-a-stream extent is ERROR-logged +
    skipped). Delete is etcd-first **value-CAS** on the snapshot (refuses if a
    concurrent recovery/EC eversion bump or alloc rewrote `extents/<id>`), then
    in-memory remove + `enqueue_pending_deletes`. **Safety:** in-no-stream
    `refs==0` ⇒ relocate-then-punch (note GC-VP-IDENTITY in partition-server)
    moved live values out ⇒ no live VP; `vp_table_refs==0` is now the
    upgrade-safety guard (post-`vp_table_refs`-removal it is frozen, so a legacy
    `vp_table_refs>0` extent is correctly NOT reclaimed here until Stage 2's
    migration). **Accepted residual** (documented, non-loss): non-atomic
    delete+marker (matches `punch_holes`; F109 node-reconcile backstop). Test:
    `extent10_both_zero_orphan_is_auto_reclaimed_referenced_kept`.

42. **F-FS-UNIFY M0 fuse-fs inode allocation (`fs_alloc.rs`, `MSG_ALLOC_INODES`
    = 0x53, WIRE v11).** The manager grants contiguous inode ranges
    `[base, base+count)` for the fuse filesystem — replacing the client-side
    non-CAS read-modify-write on the fs KV `[0x04]next_inode` key, which
    duplicated batches under concurrent allocators (two mounts, or a mount +
    the Python `autumn.Fs` client; design: `docs/fs_unify_design.md` §6,
    decision Q2 = manager RPC with CAS). Semantics:
    - **Etcd mode:** authoritative counter at `autumn-rs/fs/next_inode`
      (strict BE u64; malformed value → refuse loudly, never guess). Every
      grant is a read → `txn_fenced` value-CAS loop (F149 leader fence
      prepended, so a deposed leader's grant loses the txn — no double-grant
      across a leader transition). First-create uses the create_revision==0
      pattern (same as owner locks). No in-memory cache: every grant reads
      etcd fresh, so failover needs no replay hook for this key.
    - **Memory mode (tests/dev):** `AutumnManager.fs_next_inode` Cell.
    - **Migration floor:** requests carry the legacy KV counter value; the
      grant never returns a base below it (`max(cur, floor)`) and the counter
      never rewinds. The fuse mount passes it on every batch refill and
      best-effort rewrites the legacy key (advisory freshness for a
      disaster rebuild where etcd is lost but fs data survives).
    - This is deliberately NOT `alloc_ids` (note 5): that counter numbers
      manager ENTITIES (streams/extents/partitions) replayed from etcd
      prefixes; inode numbers are fs-layer data with their own key.
    - Handler: `handle_alloc_inodes` (leader-gated like MSG_ACQUIRE_LEASE;
      count==0 → CODE_INVALID_ARGUMENT). Client: `ClusterClient::
      alloc_inodes(count, floor)` via `mgr_call_leader`. Tests:
      `tests/fs_alloc_inodes.rs` (16-way concurrent disjointness + floor
      monotonicity in memory mode; etcd CAS disjointness + persisted
      watermark + follower NOT_LEADER refusal), `fs_alloc::tests` unit.

43. **F-DASH-IN-MGR embedded web dashboard + auto-policy controller
    (2026-07-04).** The retired Python `python/dashboard/` (a browser UI + an
    external auto-policy controller that shelled out to `autumn-op --json`) is
    folded into the manager. Two pieces, both in-process (no subprocess):

    - **Dashboard** (`dashboard.rs`): `axum` served by the compio-native
      `cyper_axum::serve` over a `compio::net::TcpListener` — the SAME HTTP stack
      as `examples/gallery` (`send_wrapper::SendWrapper` bridges axum's `Send`
      bound over the `!Send` manager). The 29 KB page is `include_str!`'d
      (`dashboard_web.html`). Endpoints (byte-compatible with the retired Python
      contracts so the page is unchanged): `GET /` + `/healthz`; `/api/overview`
      (df + nodes + partitions + ps_roll + advisories, 1-s coalescing cache);
      `/api/partition/<id>`; `/api/policies` GET/POST activate/upsert/delete;
      `/api/action` (manual mutations). Fed IN-PROCESS via `compute_*_resp` pure
      builders (extracted from the `handle_*` RPC handlers — no self-RPC). Flags
      `--dashboard-port` / `--dashboard-listen` (default = `--listen`) /
      `--dashboard-allow-mutations`. `spawn_supervised` (F228); per-conn detached.
    - **Auto-policy controller** (`auto_policy.rs` + `auto_policy_tick_loop`):
      `AutoPolicyMode` state machine (Off/DryRun/Armed), 5 compiled-in presets +
      custom policies, ported pure `decide_actions`/`candidate_to_cmd`/
      `cooldown_key`. **INVARIANT: runs ONLY on the leader** (`leader.get()` gate
      every tick — no candidate read / decision / actuation on a follower).
      DEFAULT-OFF (fresh cluster is pure-mechanism, F203). `Armed` actuates only
      with `--dashboard-allow-mutations`, else degrades to DryRun. Actuation is
      in-process to the SAME ops the mechanism layer exposes: split →
      `auto_dispatch_split`; merge → the F185 freeze-drain `handle_merge_partitions`
      (NOT the F184 flush path — avoids the ~5% loss window); gc/compact/forcegc →
      PS `MSG_MAINTENANCE`; ec → `handle_force_ec_convert`.
    - **Config = etcd, leader-owned, crash-safe** (the whole point vs the
      killable Python webserver): `autoPolicy/config` (mode + active + custom
      policies) + `autoPolicy/cooldowns`, written **etcd-first** (F149-fenced
      `put_msgs_txn`) by `autopolicy_set` (compute-on-clone → persist → apply +
      a concurrent-update guard), reloaded by `replay_from_etcd` (fail-loud
      decode + `sanitize_entry` clamp). Presets are compiled-in, never persisted.
    - **Headless control**: `MSG_AUTOPOLICY_GET/SET` (0x54/0x55, WIRE v12) +
      `ClusterClient::auto_policy_get/set` + `autumn-op auto-policy
      status|activate <name> [--arm]|deactivate`. `/api/action` is STRUCTURED
      (`{"action":"split","part_id":7}` — no CLI command-string), leader-gated +
      `--dashboard-allow-mutations`-gated + verb/field-validated.

    **Security posture (documented non-goal):** no per-request auth/TLS on the
    dashboard port (same as `--metrics-port`); default read-only, mutations
    opt-in via the flag; pair network exposure with ACLs. On k8s the port rides
    the leader-gated `autumn-manager` Service. Cross-ref: notes 15 (F149 fence),
    16/18 (F183/F187/F202 advisory engine), 17 (F185 merge freeze), 29 (F228
    spawn_supervised).
