# autumn-manager Crate Guide

The central control-plane service. Metadata authority, leader-elected, etcd-backed.
Owns: stream/extent metadata, partition split/merge/rebalance, recovery + EC
dispatch, the auto-policy controller, authz/KDC, the namespace registry, inode
leases, and the embedded web dashboard.

Single-threaded compio runtime (`Rc`/`RefCell`, `!Send`). etcd (via the compio-native
`autumn-etcd` client) is optional: without it the manager runs memory-only (no
persistence, no election, always "leader") for dev/test/bench.

## RPC surface

Handlers dispatch on a `msg_type: u8` in `rpc_handlers.rs::dispatch`. Every message
is rkyv zero-copy over autumn-rpc 10-byte frame headers (types in
`autumn-rpc/src/manager_rpc.rs`). Manager→extent-node calls use `extent_rpc` types
via the shared `ConnPool`. RPC families:

- **StreamManager**: status, acquire_owner_lock, register_node, create_stream,
  update_stream_ec, stream_info, extent_info, nodes_info, check_commit_length,
  stream_alloc_extent, stream_punch_holes, truncate, multi_modify_split,
  multi_modify_merge, merge_partitions, reconcile_extents, force_ec_convert.
- **PartitionManager**: register_ps, upsert_partition, get_regions, heartbeat_ps,
  register_partition_addr, report_partition_load, rebalance_regions.
- **Policy/advisory**: get_policy_candidates, get_policy_kind_names,
  get_partition_detail, autopolicy_get/set (`0x54`/`0x55`).
- **Node lifecycle**: list_node_states, fence_node, set_node_maintenance,
  clear_node_override, remove_node, recovery_stats, query_audit_log,
  report_disk_failure, extent_health_report, list_ec_inflight_markers.
- **Identity/capacity**: get_cluster_id (`0x45`), get/bump_cluster_version,
  cluster_df, get_cluster_overview.
- **Inode leases** (`0x46`–`0x49`): acquire/release/heartbeat_lease,
  poll_invalidations. **fs inode alloc**: alloc_inodes (`0x53`).
- **Namespace/authz**: namespace_create (`0x57`), namespace_delete (`0x58`),
  namespace_list (`0x59`), namespace_set_presplit, principal_list (`0x5A`),
  get_authz_config.

`update_stream_ec` mutates `MgrStreamInfo.ec_data_shard / ec_parity_shard`; the
`ec_conversion_dispatch_loop` then converts sealed extents to the new shape.

### WIRE fingerprint discipline

Manager RPC structs are rkyv. Each wire-affecting change carries a fingerprint in
`manager_rpc.rs`; pre-R3 the binary pins `WIRE_VERSION_MIN == WIRE_VERSION_MAX`, so
any layout change is a **same-commit, stop-the-world deploy** (`cluster.sh reset`
wipes etcd; there is no rolling upgrade). `GetClusterIdResp`
(`{wire_version_min, wire_version_max, cluster_version}`, the startup handshake) is
**FROZEN** from R1 on — never reshape it. New message-type numbers and enum variants
(`POLICY_KIND_*`, `NODE_AUTO_STATE_*`) are **append-only**; existing numeric values
are frozen so external controllers can introspect the binary's mapping
(`MSG_GET_POLICY_KIND_NAMES = 0x3B`).

## Core struct

```rust
pub struct AutumnManager {
    store: MetadataStore,        // Rc<RefCell<MetadataState>> — all in-memory cluster state
    leader: Rc<Cell<bool>>,      // are we the current leader?
    displaced: Rc<Cell<bool>>,   // did a DIFFERENT instance take the leader key?
    etcd: Option<EtcdMirror>,    // optional etcd persistence + leader fence
    conn_pool: Rc<ConnPool>,     // extent-node RPCs
    // + inflight ledger, recovery limiter, node_states, policy engine,
    //   lease registry, namespaces, tenant_accounts, admin_token, authz keyring …
}
```

`store` (from `autumn-common`) holds streams, extents, nodes, disks, partitions,
regions, owner revisions. Every persistent mutation is mirrored to etcd when
`self.etcd.is_some()`.

## Leader election

Lease-based (10 s TTL):
1. Create lease; CAS-write `autumn-rs/stream-manager/leader = instance_id` if absent.
2. On win: `replay_from_etcd` rebuilds all in-memory state, set `leader = true`,
   start a keepalive loop (every 2 s).
3. Lease expiry / keepalive failure → `leader = false` (step down).
4. A background loop retries election every 2 s when not leader.

`replay_from_etcd` is **fail-loud**: a persisted rkyv blob that no longer decodes
refuses leadership (`replay_decode_err` with an actionable message) rather than
silently decoding stale bytes into wrong values.

## Data model (etcd key layout)

All writes go through the leader-fenced `txn_fenced` (below). On promotion
`replay_from_etcd` reads every prefix to rebuild memory.

| Prefix / key | Value | Notes |
|---|---|---|
| `nodes/<id>` | `MgrNodeInfo` | EN record; identity is `node_uuid`, not address |
| `disks/<id>` | disk info | manager-allocated `disk_id` |
| `streams/<id>` | `MgrStreamInfo` | membership RMW is value-CAS'd |
| `extents/<id>` | `MgrExtentInfo` | `refs` RMW is value-CAS'd |
| `partitions/<id>` | `MgrPartitionMeta` | key range |
| `regions/<id>` | `MgrRegionInfo` | carries `region_epoch` |
| `ps_nodes/<id>` | PS address | ephemeral fleet membership |
| `next_id` | u64 | the ONLY id source (`alloc_ids`) |
| `ownerLocks/<key>` | owner epoch | `owner_epoch` = the acquire's `mod_revision` |
| `extent_inflight/<id>` | `MgrExtentInflightRecord` | unified in-flight ledger |
| `extentLayout/<id>` | 1 byte | payload location; absent ⇒ `InDat` |
| `extentDeleteRetry/<id>` | `MgrExtentDeleteRetry` | budget-exhausted delete retries |
| `partitionLastOp/<id>` | i64 LE unix | last split/merge timestamp |
| `node_override/<id>` | `MgrNodeOverride` | Fenced / Maintenance |
| `decommissioned/<uuid>` | tombstone | uuid-keyed, survives node delete |
| `mgr_audit_log/<ts>_<seq>` | `MgrAuditEntry` | admin-op audit trail (90-day GC) |
| `inode_leases/<ino>` | writer lease | reader leases are memory-only |
| `namespace/<name>` | `MgrNamespace` | registry |
| `tenantAccount/<name>` | `MgrTenantAccount` | authz principal DB |
| `autoPolicy/config`, `autoPolicy/cooldowns` | policy state | leader-owned |
| `autumn-rs/cluster_id` | UUID | CAS-imprinted once |
| `autumn-rs/cluster_version` | ASCII decimal | format-version stamp |
| `autumn-rs/fs/next_inode` (or `…/fs/{tenant}/{volume}/next_inode`) | BE u64 | fs inode counter |

`part_addrs` (client routing hints) is deliberately **in-memory only** — see the
leaderless-routing note below.

## Admin auth & KDC

**Admin-token gating (opt-in).** `dispatch` calls
`autumn_rpc::manager_rpc::is_admin_mgr_msg(msg_type)` to identify cluster-mutating
ops. Enforcement is **opt-in**: a token-less manager (dev/test/bench/chaos, memory
mode) runs them bare. When `--admin-token-file` is set, the payload MUST carry a
matching length-prefixed token (`strip_admin_token` + constant-time
`authz::ct_eq_secret`); the stripped remainder is what the real handler decodes.
Zero wire-struct change. `is_admin_ps_msg` is the symmetric PS-side set (split /
maintenance); when actuating those, the manager prefixes its own admin token via
`admin_prefix_ps`.

`handle_namespace_set_presplit` follows the same opt-in shape rather than
fail-closed: it only *records* an operator-declared layout, and a token-less
cluster must still be able to arm the merge guard.

**KDC keyring (`authz.rs`).** `AuthzKeyring` is the manager's Ed25519 signing
keyring loaded from `--auth-signing-key-file`; **its mere presence = authz enabled**.
File format `<kid> <hex-32-byte-seed> [disabled]`, fail-loud on any malformed line
(never start half-armed). `active()` = highest-numbered ENABLED kid (mints new
tokens); `published()` publishes ALL kids incl. disabled so the PS learns to reject a
disabled kid. The token codec/claims live in `autumn_rpc::cap_token` (shared
signer/verifier). `credential_hash` = SHA-256; compares are constant-time
(`ct_eq_32` / `ct_eq_secret`) to avoid timing/length oracles.

**Principal accounts.** `tenantAccount/<name>` → `MgrTenantAccount {name,
credential_hash, grants}`; create/delete are admin-token-gated, etcd-first,
leader-fenced, serialized on `tenant_admin_lock`. `MSG_PRINCIPAL_LIST` (`0x5A`,
`handle_principal_list`) is leader-gated + read-only and returns
`PrincipalRow{name, grants}` — dropping `credential_hash` is structural: an
inspection RPC must never hand out the verifier for a credential.

## Stream lifecycle

**Create** `create_stream(data_shard, parity_shard)`: `alloc_ids(2)` → select the
first `K+M` nodes, `alloc_extent` on each (empty files), create `StreamInfo` +
`ExtentInfo{eversion:0, refs:1}`, mirror to etcd.

**Seal + alloc new tail** (`stream_alloc_extent`): validate owner epoch → seal the
current tail → `alloc_ids(1)` → `alloc_extent` on preferred nodes with a per-RPC
fallback walk over other registered nodes if one is dead → append to the stream →
mirror. Sealing semantics: see the lenient-seal note.

**GC**: `stream_punch_holes` removes named extent ids from a stream and decrements
extent `refs`; `truncate` removes all extents before a given id. Extents are
CoW-shared across partitions after a split, so **never delete an extent with
`refs > 0`**. When `refs → 0`, the handler snapshots the replica address list
**before** removing the extent from `s.extents`, and after the etcd mirror succeeds
hands it to `enqueue_pending_deletes`. `extent_delete_loop` (2 s) fans out
`EXT_MSG_DELETE_EXTENT` to each replica; after 60 failed sweeps the entry moves to
the persisted `extentDeleteRetry/` queue (`extent_delete_retry_loop`, 1 min,
exponential backoff 60 s → 1 hr). Orphan files are the reconcile backstop: on EN startup (and every 5 min) the node
sends every loaded `extent_id` via `MSG_RECONCILE_EXTENTS` and the manager
answers **file-granularly** — `garbage` (not a member: delete everything) plus
`placements` (`payload_location` + this node's slot as its shard index). The node
keeps the ONE named payload file and drops the rest, which is how a converted
extent's redundant `.dat` is reclaimed and how an abandoned attempt's shards are
swept, under one rule. **Any extent with an in-flight ledger op is omitted from
both lists** — its file set is mid-change, and only the manager knows about an
attempt driven from another node.

**The reporter must be IDENTIFIED or it gets no verdict at all.** Every answer
is relative to one node ("you are not a member of this", "your payload is in
that file"), so the manager resolves `node_id`, else `node_uuid`, and on failure
returns empty lists with a WARN. This is not defensive coding: the EN does not
know its own node_id (the manager assigns it) and once reported `0`, which under
a membership predicate made every extent on it look like garbage — and because
the grace counter is keyed `(node, extent)`, three nodes reporting `0` shared ONE
counter and burned the entire grace period in a single round each. The third
node was told to delete a live extent. Identity was diagnostic before membership
made it load-bearing; a node without `--advertise` now gets no cleanup, which is
the correct direction to fail. Etcd-first ordering: the queue push
happens only after the mirror returns OK, so a failed mirror never schedules a stale
unlink.

## Partition split / merge / rebalance

### `multi_modify_split`

Atomically splits one partition into left + right:
1. Validate owner epoch; validate `mid_key` inside the range; verify the
   request's captured tail extent ids still match each stream's CURRENT tail
   (refuse `split captured tail moved` otherwise — a roll that landed after
   the PS's capture would get the captured length stamped onto its fresh
   empty tail; 0 = no claim, skip).
2. `alloc_ids(4)` → new log/row/meta stream ids + new part id.
3. `duplicate_stream` each of the 3 streams at its sealed length (shares extents).
4. Left range → `[start, mid)`; right created as `[mid, end)` with new stream ids.
5. `rebalance_regions` (bumps left's `region_epoch`, seeds right's = 1).
6. Persist everything in one fenced etcd txn.

Both children initially share the same physical extents; each `PartitionServer`
detects `has_overlap` on open and major-compaction cleans out-of-range keys and
frees the shared extents via GC.

**`duplicate_stream`**: for each non-tail extent, `refs += 1` + add to the new
stream; for the tail, set its sealed length at the split point, bump `eversion`,
`refs += 1`, add. `compute_duplicate_stream` is the read-only pure form (the applier
is `apply_split_mutations`).

**`region_epoch` (TiKV-style)** on `MgrRegionInfo`, bumped through
`next_region_epoch(state, part_id, new_rg)` by both `rebalance_regions` and
`compute_region_for_partition`:
- no prior region → epoch = 1 (`0` is reserved on the wire = "skip check");
- `rg` byte-for-byte unchanged → unchanged (idempotent rebalance / PS reassignment);
- `rg` changed → `+= 1`.

SDKs stamp the cached epoch on every data-plane request; the PS rejects with
`FailedPrecondition` on mismatch and the SDK refreshes + retries.

### Merge

`handle_multi_modify_merge` is the inverse of split (pure helpers
`compute_merge_streams` — log splice `[L]+[V]+[E_new]`, order is load-bearing for
vp_head replay correctness; `splice_streams_without_new_tail` for row+meta;
`apply_merge_mutations`). Phases: (1) inflight checks + adjacency + `alloc_ids(1)` +
`select_nodes` for the new tail `E_new` + eversion/CAS-baseline snapshot; (1.5)
`alloc_extent_on_node` per replica; (2) single fenced `put_and_delete_txn` (all puts
+ victim deletes — the linearization point); (3) verify-at-apply + apply.

`handle_merge_partitions` wraps that txn with a TiKV-PrepareMerge-style freeze-drain
so writes that would race the flush→commit window are halted at the source. It first
acquires an admin owner-lock **keyed on the partition pair** (so concurrent merge
attempts targeting the same survivor serialize on the manager), then
`MSG_MERGE_FREEZE{true}` to victim then survivor (drains inflight, flushes imms,
halts new writes with `CODE_UNAVAILABLE`, returns only after a durable post-freeze
checkpoint) → capture `commit_length` ×6 → `handle_multi_modify_merge` → on OK do
NOT explicitly unfreeze (each PS's `region_sync_loop` sees the new (rg, stream_ids)
and reopens the survivor = natural unfreeze); on error best-effort unfreeze. PS-side
`FREEZE_TTL` (30 s) is the final backstop, so no procedure-WAL is needed.

### Rebalance

`rebalance_regions` is **STICKY, not a balancer**: it keeps a region on any
still-registered PS (only refreshing `rg`) and assigns unassigned ones least-loaded.
Called eagerly after `register_ps`, `upsert_partition`, `multi_modify_split` (safe
because idempotent). The `rg` refresh on keep is critical — otherwise `GetRegions`
returns a stale pre-split range.

The active balancer is `compute_rebalance_moves(state, max_moves)` (pure, greedy
most-loaded → least-loaded until the per-PS count gap ≤ 1, deterministic ties):
`handle_rebalance_regions` rewrites each moved region's `ps_id` in-memory then
`mirror_partition_snapshot`. `rg` is unchanged so `region_epoch` is NOT bumped (only
the serving PS moved); the PS `sync_regions_once` picks up the `ps_id` change and the
old PS drops / new PS opens. Exposed as `autumn-op rebalance [MAX_MOVES]` and as the
auto-policy `POLICY_KIND_REBALANCE` (7) arm.

**Rebalance actuation cooldown floor.** `decide_actions` floors rebalance's
actuation cooldown at a non-configurable `REBALANCE_MIN_ACTUATION_COOLDOWN_SEC`
(60 s). The advisory-side `rebalance_cooldown_sec` only gates EMISSION, but an
emitted candidate lingers in `advisory_cache` for a whole policy-tick window, so a
policy with `cooldown_sec = 0` would re-actuate the same cached candidate every tick
→ partition-reopen storm. The floor is rebalance-only.

## PS liveness

`ps_last_heartbeat: Arc<Mutex<HashMap<u64, Instant>>>` (ephemeral, not persisted).
`register_ps` seeds a timestamp; the PS calls `heartbeat_ps` every 2 s;
`ps_liveness_check_loop` (2 s) evicts a PS not seen in 10 s — fenced
`put_and_delete_txn(delete psNodes/<id>)` then `rebalance_regions`. On eviction
`handle_heartbeat_ps` returns `CODE_NOT_FOUND` so the PS re-registers +
`sync_regions_once` (silent `CODE_OK` would leave it invisible as `ps=unknown`).
`replay_from_etcd` seeds `ps_last_heartbeat = now` for every replayed PS so the
liveness loop's `Some(t)` arm engages instead of treating it as an immortal zombie.

The PS spawns its `heartbeat_loop` in `finish_connect` (NOT `serve()`, which only
runs after every assigned partition finishes WAL replay — that can exceed the
eviction window).

## Extent in-flight ledger (unified)

One etcd-backed ledger `extent_inflight/<id>` keyed by extent_id replaces all
per-race sets. **Layer boundary:** only STREAM-LAYER ops enrol — ConvertToEc /
Recovery / Delete (the ops the manager dispatches to extent-nodes). PS-layer ops
(split / merge / punch_holes / truncate / alloc_extent) **read** the ledger to
refuse-at-start but do NOT enrol (they're partition-scoped; enrolling would multiply
etcd traffic per split and cross the layer boundary).

Three race classes:
- **Class A** (PS handler starts while a stream-layer op is in flight): single-line
  `extent_inflight_op(eid)` probe → refuse `Precondition`.
- **Class B** (stream-layer op fires mid-PS-await): verify-at-apply — re-read
  eversion (and stream membership) before the etcd-mirror writeback; refuse if it
  changed. Used by `handle_stream_alloc_extent`, `handle_multi_modify_split/merge`.
- **Class C** (two stream-layer ops race): exclusive per-extent CAS via
  `acquire_extent_inflight`; second acquire returns `Precondition`.

Invariants:
- **I1** leader-only writes (leader fence on every `txn_fenced`).
- **I2** every acquire has a matching release OR `replay_from_etcd` reclaims it.
- **I3** the release is bundled into the op's apply-done etcd txn
  (`put_and_delete_txn(extents/<id>, deletes=[extent_inflight/<id>])`) — atomic, no
  separate-round-trip leak window.
- **I4** replay populates the in-memory shadow BEFORE the dispatch loops spawn
  (ordered in `new_with_etcd`).
- **I5** every extent-mutating handler calls `extent_inflight_op` before
  clone-for-decision (one helper, not five sets).

**Stale sweep** (`extent_inflight_stale_sweep_loop`): tick
`AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS` (default 60 s, floor 1); stale threshold
`AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS` (default 600 s, floor 60). `started_at`
is in the persisted record so the clock survives failover. **Only Delete markers
auto-release on wall-clock** (sweep touches only the marker, never `extents/<id>`
or an EN; delete is idempotent). **Recovery has NO TTL** — a marker is released
by an EVENT (its pinned executor stops being Online, or the recovery completes),
never by elapsed time; see the Recovery section. **ConvertToEc is WARN-only and
NEVER auto-released** — releasing it races the original EN dispatch and can record a
different parity assignment than the bytes that physically landed (silent EC
corruption); operator inspects EN state and clears manually.

Deploying onto pre-ledger etcd state is unsupported (`cluster.sh reset`).

## Recovery

**Dispatch loop** (2 s, `recovery.rs`): scans all SEALED extents; per replica slot
does a per-disk health check first (offline `disk_id` → dispatch immediately), then
probes `commit_length` (or `re_avali` for known-lagging replicas). On no-response /
error, dispatch `require_recovery` to a healthy candidate. In-flight recoveries live
in the unified inflight ledger so a double-dispatch is impossible across failover.

**The marker is a STANDING INSTRUCTION, not a do-not-disturb flag.** A marker pins
one `(extent, executor)` assignment; the leader keeps RE-SENDING that exact RPC
(`redispatch_pinned_recovery`, 5 s timeout, skipped when the pinned node is not
Online so a keep-alive to a corpse can't eat the whole dispatch tick) and **never
drains the marker on an RPC failure**. That is what makes an EN restart
self-healing without a TTL: the EN loses its in-memory `recovery_inflight`, the
next re-send simply starts it again, and every EN answer is idempotent by
contract (already-running → `CODE_OK`; complete local copy → re-report done;
incomplete residue → discard + rebuild — see `crates/stream/CLAUDE.md`).
**Release is EVENT-driven, at exactly two points:** `apply_recovery_done` (the
work finished) and `release_recovery_markers_for_dead_executors` (level-triggered
each tick — the pinned node is gone from `s.nodes` or no longer Online → drop the
marker so re-derivation picks a live target). **There is deliberately NO
wall-clock TTL**: a timeout is indistinguishable from a slow-but-progressing
rebuild, and releasing on one races the executor still writing the copy. Never
re-introduce a TTL, and never drain a Recovery marker on a dispatch error.

**Residue is collected by MEMBERSHIP** (`handle_reconcile_extents`): the garbage
list is "extents you are not in `replicates ++ parity` of", NOT "extents I have
forgotten". A recovery that died mid-copy leaves a partial `.dat` on a node whose
extent is still very much alive, so the forgotten-extent predicate could never
see it and the stub leaked forever. Guards: `NON_MEMBER_ROUNDS_BEFORE_GC = 3`
consecutive rounds (the membership view is transiently wrong during an
`apply_recovery_done` slot swap or a settling leader, and deleting live data on a
transient is far worse than holding residue a few minutes) and an
`extent_inflight_op` check (a recovery target is by construction a non-member —
it is BUILDING the copy that will make it one). Counters are leader-local and
pruned to what the node still reports, so a leader change only ever DELAYS a
deletion.

**Recovery gate** `AUTUMN_MGR_RECOVERY_GATE` (default `fenced_only`): a slot is
rebuilt only when its node's override is `Fenced`; `auto_disk` reverts to the legacy
"rebuild on `disk.online == false`". **A slot marked CORRUPT bypasses the gate**
(see below) — corruption is a stronger signal than what the gate waits for.

**Corrupt slots (`extent_corrupt.rs`, sibling key `extentCorrupt/<id>` → u32
bitmap).** A clear `avali` bit says a slot is not serving; it cannot say WHY,
and the two reasons need opposite handling. *Behind* → `re_avali` refetches the
missing tail. *Corrupt* → `re_avali` CANNOT help: its whole test is
`local_len >= sealed_length`, which a full-length rotted replica passes. So
`handle_report_corrupt_replica` records the darkened slots here in addition to
clearing their bits, and `recovery_dispatch_loop` force-dispatches a marked slot
regardless of `gate_mode`. `apply_recovery_done` clears the mark (the rebuilt
slot holds fresh bytes copied from a healthy peer), and extent deletion drops
the key alongside `extentLayout/`. **Without the mark the extent stays at RF-1
forever**: the gate skips the slot before it ever reads `avali`, so the copy is
isolated, unrepaired and silent. Sibling key rather than a `MgrExtentInfo` field
for the same reason as `extentLayout` — widening the persisted `extents/<id>`
value breaks rkyv replay validation, which refuses leadership.
Regression: `crates/manager/tests/system_corrupt_replica_rebuild.rs`; the
prerequisite that the loop can act at all is pinned by
`system_recovery_loop_drives.rs`.

**Node health loop** (`node_health_loop`, 2 s) is the **single** `EXT_MSG_DF` caller
per node. **INVARIANT: never add a second `df` caller.** The EN's `handle_df`
`std::mem::take`s its `recovery_done` when `req.tasks.is_empty()`, so a second empty
caller would drain-and-discard completions → `apply_recovery_done` never runs → the
slot stays on the dead node and the recovered copy becomes a blocking orphan. On
every `df` OK the loop marks disks online, clears failure reports, feeds
`node_states.on_heartbeat_ok`, applies EVERY returned `done_task`
(`apply_recovery_done`: swap the failed node id, bump eversion, mark slot available,
mirror, release the marker atomically), and stashes each node's max per-disk free
(`node_max_free`, ENOSPC routing hint). On `df` fail: mark disks offline +
`on_heartbeat_fail`.

**Rate limiter** (`recovery_rate_limiter.rs`): per-source/target/global concurrency
caps + per-`(extent_id, slot)` backoff (`2^N s`, cap 300 s, in-memory). It is
**reseeded from the ledger every tick** (`reset_counts` then `seed_inflight` for each
Recovery entry) — the ledger is the source of truth, so **never add manual `release`
calls** in `apply_recovery_done`/drain (a stray release double-counts down). Backoff
is independent of the marker and **never gives up** (candidates re-derived from
`s.extents` each tick; manager restart resets backoff → immediate retry), so
`backoff_entries = 0` means "nothing in a backoff window now", not "not retrying".
`record_dispatch_outcome` takes the `Result` so the failure reason is preserved
(`recovery-stats`). `max_per_target` (default 2,
`AUTUMN_MGR_RECOVERY_MAX_PER_TARGET`) should track the EN's `recovery_max` (default
2, `--recovery-parallelism`) — same physical quantity (concurrent recoveries landing
on one EN) throttled at two layers as defense-in-depth. `RecoveryRateLimiter` is a
concurrency + per-`(extent, slot)` backoff limiter with NO byte-rate dimension; keep it
distinct from the PS `RateController` (byte-rate) and the RAM-permit
`ConcurrencyController` — do NOT fold byte-rate and concurrency caps together.

**Fence-drain (open tails).** Recovery only rebuilds SEALED extents, so an idle
partition's open tail on a fenced node never drains and `remove_node` never unblocks.
`drain_fenced_open_tails` (each recovery tick) finds OPEN tails with a fenced member,
resolves the serving PS and sends `MSG_ROLL_TAILS` (30 s per-partition cooldown); the
PS idempotently seals+rolls (log/meta via `seal_and_roll_tail`, row via the
drain-to-zero barrier). The dispatch pre-filter keys on `!ex.sealed` (STATE),
not `sealed_length == 0`, so an authoritative sealed-EMPTY extent gets its fenced
slots rebuilt instead of referencing the node forever.

INVARIANT (live-writer roll): the tails this sweep targets belong to a SERVING
partition, so the PS-side roll MUST go through the live stream worker
(SealCommit quiesce → authoritative seal pinned to that tail → ResetTail) —
`StreamClient::seal_and_roll_tail` does this whenever a per-stream worker
exists. A bare manager probe-seal behind a live writer freezes `sealed_length`
while the writer (and the ENs, which learn seals only lazily) keep appending
and ACKING onto the same extent; every post-seal acked byte is then invisible
to committed-clamped replay and to CoW split children — the chaos
(`stale_vp_offset_past_sealed_length` child wedge / silent stale reads)
acked-write-loss family. The PS also DEFERS the roll while the partition is
frozen for split/merge: those orchestrations capture per-stream commit lengths
and the manager seals whatever extent is the tail at commit time, so a roll in
that window would get the captured length stamped onto its fresh empty extent.
A roll ALREADY IN FLIGHT when the freeze begins slips past that defer — which
is why `handle_multi_modify_split` verifies the request's captured tail ids
(`MultiModifySplitReq.log/row/meta_tail_extent_id`) against the CURRENT tails
in Phase 1 and refuses (`split captured tail moved`, Precondition) when any
moved; the PS aborts immediately (deterministic for those captures) and the
client's retried split re-captures. Deterministic repro of both halves:
`crates/manager/tests/system_roll_tails_live_writer.rs`
(`in_flight_roll_racing_split_commit_child_still_opens`).

**Placement hard-exclusion.** `placement_excluded_node_ids()` = Fenced ∪ Maintenance
(overrides) ∪ Suspected (`node_states`) — threaded as `hard_excluded` into
`select_nodes` (filtered at the top so both the count precheck and cold-leader
fallback inherit it — hard-excluded nodes are NEVER backfilled), all fallback walks,
`dispatch_recovery_task`'s targets, and `handle_force_ec_convert`'s parity pool.
Trade-off: a 3-EN RF-3 cluster with one Suspected node refuses new allocation until
it heals (~2 s df tick) or is fenced. (Bootstrap-seeded `Suspend` is deliberately NOT
excluded.)

**ENOSPC soft-avoid.** `select_nodes` then filters the healthy set down to nodes at or
above `min_alloc_free_bytes` (`--min-alloc-free-bytes`, default 256 MiB, 0 = off),
keyed on `node_max_free` (each node's max per-disk free from the last df — a 2 s-fresh
hint needing no disk-id mapping). If that under-fills the selection it falls back to the
full healthy set (a capacity-crunched cluster still attempts allocation; the EN-side
`Full` gate + per-RPC fallback walk handle the rest). Unknown nodes (no df yet) are
treated as spacious so a cold leader keeps allocating.

## EC conversion

`ec_conversion_dispatch_loop` (5 s, first tick at 500 ms) is **drain-only**:
candidates come from `pending_ec_dispatch` (rich `MgrEcDispatchInflight
{extent_id, target_nodes, extra_disk_ids, data_shards, new_eversion}` markers,
persisted + replay-decoded), NOT a fresh stream scan. New conversions enter via
`MSG_FORCE_EC_CONVERT`. The rich marker is load-bearing: a naive re-dispatch with a
fresh `shuffle().take()` could pick a different parity node than the one that already
holds shard bytes → `alloc_extent_on_node` resets that node's `ExtentEntry` and
`apply_ec_conversion_done` writes the new random layout to etcd → silent EC
corruption.

`apply_ec_conversion_done` flips `ec_converted = true`, bumps `eversion`, rewrites
`replicates`/`parity`/disks, and **MUST refresh `avali = all_bits(K+M)`** — otherwise
parity slot bits stay 0 and `recovery_dispatch_loop` fires `EXT_MSG_RE_AVALI` to the
parity holder forever (idle-cluster RSS churn). The EN `handle_re_avali`
short-circuits `CODE_OK` when `ec_converted`, self-healing legacy `avali`.

**The layout flip is the SINGLE commit point.** `apply_ec_conversion_done`
moves membership, eversion, `avali` AND `payload_location = InShardFile` in ONE
leader-fenced transaction, value-CAS'd against the snapshot the decision was
computed from. All three parts are load-bearing: a location published separately
from the layout it belongs to would, for the width of the gap, send readers to a
file the layout does not yet say anyone holds; and the CAS states explicitly
what today rests implicitly on the inflight ledger serialising per-extent ops.
Before the flip nothing is committed — the shards are additive files no reader
is pointed at — so **an EC marker whose coordinator is gone is now released**
like a recovery marker (`release_recovery_markers_for_dead_executors`), and the
successor is free to choose a different assignment. "Gone" means absent from the
cluster or `Suspected`, NOT merely "not Online": a freshly registered node sits
in `Suspend` until its first `df`, and abandoning on that makes a conversion
that outlives one tick impossible.

**Payload location (`extent_layout.rs`).** Which FILE holds an extent's payload
— `.dat` or `.shard{i}` — is per-extent metadata the manager owns and the EN
obeys; the EN never infers its own role. It lives in the sibling key
`extentLayout/<id>` (absent ⇒ `InDat`) rather than in `MgrExtentInfo`, because
that struct is the persisted `extents/<id>` value: widening it would make an
existing cluster's stored extents fail rkyv validation on replay, which refuses
leadership rather than degrading. It reaches readers on `ExtentInfoResp`
alongside the extent. `handle_extent_info` fills it; extent deletion drops it.
A legacy EC extent is `ec_converted = true, InDat` — the pre-CoW scheme renamed
each shard over `.dat` — so it keeps working with no backfill.

**Attempt identity (`attempt_nonce`).** A conversion attempt is identified by the
etcd revision of the txn that created its marker — taken from that txn's own
response (`txn_fenced_revision`), held in `inflight_attempt_nonce` beside the
ledger, and rebuilt on promotion from the key's `mod_revision`. It rides
`ExtConvertToEcReq` → `WriteShardReq` → `EcConvertDone`, and
`classify_ec_done(params, live_nonce, reporter, done)` is the single predicate
deciding whether a completion report may be applied.

Three checks, none redundant: **reporter identity** (only `target_nodes[0]`),
**eversion**, and **attempt**. A released-and-reissued attempt can pick the SAME
coordinator and carries the SAME `new_eversion` — it is `live + 1`, and an
abandoned attempt never bumped the extent — so only the nonce separates them.
Applying the wrong one flips the layout onto targets holding no shards, after
which cleanup deletes the last full replicas. **Every rejection retains the
marker.**

**The fence epoch is resolved LIVE on every dispatch; only the ASSIGNMENT is
pinned.** `dispatch_owner_epoch_for_extent(state, extent_id)` re-reads the
owner-lock epoch of whichever partition's stream holds the extent, and the submit
path merely seeds through the same resolver. The epoch is re-acquired — and
bumped — on every `open_partition`, so a value frozen at marker-creation time
falls below the ENs' per-extent floor after any routine PS reopen (restart,
rebalance, `LockedByOther` self-eviction); every participant then answers
`CODE_LOCKED_BY_OTHER`, the conversion never finishes, the marker is never
released, and that extent's GC is refused forever with "has in-flight EC
conversion" — an unbounded space leak from an ordinary restart. Refreshing keeps
what the fence is FOR: it rejects a FENCED ex-coordinator, which still carries
the older epoch it captured, so the ghost stays below the floor while only the
live dispatch moves up. Do NOT extend this to the targets/disks/eversion — a
re-derived assignment writes a layout onto nodes holding no shards.

The nonce is deliberately NOT in `MgrEcDispatchInflight`: that struct is nested
as an `Option` in the persisted `MgrExtentInflightRecord`, so widening it shifts
the archived layout and every live marker — recovery and delete too — would fail
replay validation, blocking leadership on upgrade. Because dispatch and apply
both read the same in-memory entry, a lost entry can only weaken the check to its
pre-nonce strength; it can never reject a legitimate report. `0` = pre-nonce
marker, and matches only a `0` report.

Candidates are deduped by `extent_id` (a CoW-shared extent appears in both child
streams; re-encoding an already-shrunk shard produces `original/K²` sub-shards). The
coordinator `handle_convert_to_ec` is idempotent (already-converted at this eversion
→ `CODE_OK` without re-encoding) and holds a per-extent mutex so a duplicate dispatch
after leader-failover serialises and no-ops. EC dispatch skips a coord whose state is
Suspected/Fenced/Maintenance/Suspend (no log spam during a flap).

## Node lifecycle & identity

**State machine (`node_state.rs`).** `NodeAutoState {Online, Suspected, Suspend}`,
driven by `node_health_loop`'s df outcome. **No automatic `Down` transition** — a
`Down`-equivalent is operator-driven only. `Suspend` is the initial state of a
freshly registered node (`on_register_first`, no `last_ok`); Suspend → Online on the
first df OK (~2–4 s) or operator re-register; Suspend → Suspected NEVER (Suspected
means "was alive, now flaky"). Replay seeds every EN OK on promotion so a fresh
soft-timeout window elapses before judgement. `NODE_AUTO_STATE_SUSPEND = 2` is
wire-stable. `select_nodes` ANDs Online-state AND online-disk filters.

**Operator overrides (etcd-persisted, `node_override/`).** `mgr_fence_node` /
`mgr_set_node_maintenance` / `mgr_clear_node_override` / `mgr_remove_node`.
`MgrNodeOverride` (keyed by node_id, carrying `node_uuid`) is the cleanup trigger.
`mgr_fence_node`: capacity precheck unless `--force`, write the override, then
`auto_abandon_for_fenced_node` sweeps ConvertToEc markers whose `target_nodes[0]` is
the fenced node (atomic delete + `ec_convert_advisory/` for follow-up). Fencing an EN
must NOT fence PS partition owners (writer fencing on takeover is
`acquire_partition_owner_epoch`'s job). `mgr_remove_node` requires Fenced AND no
extent/marker still references the node (an OPEN tail slot counts — hence
fence-drain); else `Precondition` with the blocking ids. `tick_maintenance_ttl`
(each recovery tick) clears expired Maintenance entries. **Zombie/imposter defense:**
`handle_register_node` refuses (Precondition) an address whose node is Fenced or in
the `decommissioned/` tombstone; the tombstone is uuid-keyed and survives node
deletion, so a fenced/decommissioned node can't return at any address.

**UUID identity (`node_uuid`, in-struct).** The EN's stable identity is its UUID, not
its address (survives k8s pod reschedules / fresh IPs). `handle_register_node`
resolves UUID-first: uuid-match → update address/`shard_ports`/`control_address` in
place; uuid present + address matches a legacy uuid-less node → adopt; uuid present +
address matches a DIFFERENT non-empty uuid → refuse (one address hosts one node
record, else RF double-placement). The EN self-registers its live location +
`shard_ports[]` at startup via `--advertise` (the reshard commit point). The df
identity echo (`ExtDfResp.node_uuid/advertise_addr/shard_ports`) is classified by the
pure `classify_df_echo`: **`Imposter`** (echo uuid ≠ stored) → treat df as failed, do
NOT heal (a different process answers at this address; pod-IP reuse); **`DriftWarn`**
(uuid matches, location drifted) → WARN only, no write (the CAS-safe auto-heal is a
deferred reproduce-first follow-up; the EN's own startup register is the sole location
writer). `autumn-op format` is IDENTITY-ONLY (registers with empty
location → the node stays Suspend, unselected, until it boots and self-registers).

**Audit log (`audit.rs`).** Every admin RPC wraps its return in `append_audit`
(`mgr_audit_log/<ts_ns>_<seq>` → `MgrAuditEntry`, best-effort). `mgr_query_audit_log`
retrieves; `audit_gc_loop` (daily, leader-only) enforces `--audit-retention-days`
(default 90, 0 = off).

## Seal / commit (WAS stream layer)

Append is **all-replica-ACK** (`client.rs::apply_completion` acks only when every
replica wrote), so the acked prefix is present on every committed member. The manager
seal is therefore **LENIENT (seal-over-reachable), NOT quorum, NOT strict-all**:
`min` over the REACHABLE committed members is always ≥ the acked length and never
drops acked data, regardless of which members are down. **The seal MUST stay lenient**
— you seal precisely because a node went down; requiring every member to respond
would wedge the seal forever.

Two seal sites — `handle_stream_alloc_extent` failover seal and
`handle_check_commit_length` — both exclude catching-up members
(`recovering_nodes_for_extent`, a re-replication target holds a partial replica and
must never lower the `min`), probe committed members, and feed the shared pure
`compute_commit_seal(members, recovering, responses, floor)`. `floor` =
`AUTUMN_MGR_SEAL_DURABILITY_FLOOR` (default 1) is a durability floor (min members that
must exist + respond), NOT a quorum vote on position (position is always `min` over
responders). An unreachable committed member gets its `avali` bit left unset →
reconciled by recovery later; it does not block the seal.

**Phantom-commit is ACCEPTABLE.** Seal-over-reachable can promote an
un-acked-but-replicated tail byte to committed (data *gain*, never *loss*),
consistent with uncertain-write semantics. Do NOT add strict-mode/watermark threading
to kill it — it trades a benign gain for a real loss risk.

**Authoritative seal state.** `MgrExtentInfo.sealed: bool` is the authoritative STATE
(`sealed_length` is the LENGTH; invariant `sealed_length > 0 ⇒ sealed`; every
`sealed_length =` also sets `sealed = true`). `already_sealed = tail.sealed` (NOT
`sealed_length > 0`) so an authoritative EMPTY seal is unambiguous. "Is-sealed" reads
use `.sealed`; "is-empty/nothing-to-recover" reads keep `sealed_length`. The failover
seal is `StreamAllocExtentReq.seal_commit: Option<u32>`: `Some(c)` = authoritative,
seal at exactly `c` (even 0, no probe — `c` is the writer's quiesced `state.commit`
from the SealCommit handshake, so no probe promotes a phantom); `None` = probe via
`compute_commit_seal` (genuine new-owner takeover only). CoW empty-tail seal: split
and merge seal the shared old tail even when its captured length is 0 (`!ex.sealed`),
else both children would append to the same open extent (CoW isolation break).

**Idempotent alloc-with-roll** via `StreamAllocExtentReq.seal_extent_id`: the writer
pins the target tail `T`; the manager seals ONLY when the current tail still equals
`seal_extent_id` AND is OPEN, else it is an idempotent no-op returning the current
tail untouched (a lost response won't over-seal the freshly-rolled `T'`). `!tail.sealed`
is load-bearing — if the current tail is itself sealed, fall through to the
`already_sealed` path (preserve the seal + alloc a NEW open tail) rather than handing a
sealed extent back as "fresh".

**Alloc on an already-sealed tail must NOT rewrite it.** The refuse-at-start
`extent_inflight_op(tail_id)` probe is gated on `sealed_length == 0` (it only ever
fired on already-sealed tails), and on the already-sealed path the tail etcd write +
`s.extents.insert` are skipped (the sealer already persisted it) — otherwise a
concurrent Recovery completing during the mirror RTT would be clobbered by the stale
clone. A stream-membership baseline verify runs for BOTH paths (refuse if
`extent_ids` changed) so a concurrent punch/truncate/split can't be clobbered.

## Crash-safety & fencing invariants

- **Etcd-first mutation.** Every mutating handler: (1) compute mutations without
  touching the store, (2) persist to etcd, (3) apply to memory. A crash between (1)
  and (2) leaves etcd and memory consistent. Exception: `register_ps` /
  `upsert_partition` apply to memory first because `mirror_partition_snapshot` reads
  the store (idempotent on retry). Any new persistent-state handler MUST follow this.
- **Leader fence on every etcd write** (`txn_fenced`). Prepends
  `Cmp::value("autumn-rs/stream-manager/leader") == instance_id` to the txn. On
  fence-fail it flips `leader = false` (so `ensure_leader` short-circuits later RPCs)
  and returns `NotLeader` → `CODE_NOT_LEADER`. Bare puts (not CAS) would let a deposed
  leader (still believing it leads during a starvation/GC window) last-writer-wins over
  the new leader. NOT fenced: `try_become_leader` (it establishes ownership),
  `replay_from_etcd` (read-only), the keepalive loop (no k/v write). All `mirror_*`,
  `persist_extent`, owner-lock and inflight CAS, split/merge Phase-2 route through it.
- **`alloc_ids` is the ONLY id source.** `next_id = max(all entity ids) + 1` at replay,
  so wasted ids from failed mutations are safe. Not used for fs inode numbers (those
  have their own counter).
- **`ensure_owner_epoch` before every stream mutation** (`stream_alloc_extent`,
  `stream_punch_holes`, `truncate`, `multi_modify_split`, merge) — missing it allows
  split-brain.
- **`owner_epoch` bumps on EVERY acquire.** `acquire_owner_epoch` rewrites
  `ownerLocks/<key>` with an unconditional leader-fenced PUT and returns the fresh
  `mod_revision`; `replay_from_etcd` reads `mod_revision` to match (replay and acquire
  MUST stay in lock-step or post-failover `ensure_owner_epoch` rejects every live
  owner). Newest-acquirer-wins: each PS incarnation acquires once at startup and keeps
  the epoch for its lifetime; per-partition StreamClients inherit it. A stable per-key
  epoch cannot support A→B→A failback and lets two live processes share an epoch (no
  mutual fencing). Memory-mode mirrors this.
- **Stream-membership etcd writes value-CAS.** Any read-modify-write of a
  `streams/<id>` membership MUST value-CAS the write against the read baseline
  (`put_delete_txn_cas` prepends `Cmp::value(streams/<id>) == baseline`), never a bare
  last-writer-wins put — else a `punch_holes` committing during an `alloc`'s mirror RTT
  is overwritten by alloc's stale baseline (resurrected extent / lost GC). CAS never
  blocks (a per-stream lock would serialize the write path behind slow GC/split/merge
  and lose writes under kill); a genuine conflict returns `Precondition` → client
  retries with a fresh snapshot. rkyv is deterministic so the baseline byte-matches
  etcd. Covered: `handle_stream_alloc_extent` / `handle_stream_punch_holes` /
  `handle_truncate` / `handle_multi_modify_merge` (all 3 survivor streams). Accepted
  residual: a CAS-failed alloc orphans the just-created extent files (reaped by the
  node-startup reconcile), and GC/compaction callers don't client-retry but their
  background loops re-attempt (`classify_gc_failure_cooldown` maps `precondition
  failed` → a 30 s soft cooldown).
- **Extent-state `refs` CAS.** The four PS-op handlers value-CAS `extents/<id>` against
  its pre-mutation baseline (`compute_extent_ref_drops` for punch/truncate; each
  modified extent's baseline in split/merge Phase-2). **Split vs merge capture
  asymmetry (load-bearing):** merge captures baselines in **Phase 1** (it has a
  Phase-1.5 `alloc_extent_on_node` await; a Phase-2 capture would read already-mutated
  or deleted state); split captures in **Phase 2** (its only await is the Phase-2 write
  itself — CoW, no Phase-1.5 alloc). Split-source / merge-victim *membership* is
  intentionally NOT CAS'd: the source is `frozen_for_split` + holds gc/compact gates,
  the victim is `frozen_for_merge`, and every victim extent's `refs` write is CAS'd —
  so the only reachable concurrent mutation (a cross-partition GC punch on a CoW-shared
  extent) trips the `refs` CAS. STILL DEFERRED (reproduce-first, not reproduced): the
  eversion/replicates/avali writes on the stream-layer appliers
  (`apply_ec_conversion_done`, `apply_recovery_done`, split's source-tail eversion
  bump) — protected today by await-adjacency, the ledger, and before-await verify.

## Background-loop resilience

Every manager loop runs under `spawn_supervised(name, make)`
(`AssertUnwindSafe(make()).catch_unwind()`; on panic OR unexpected return it logs
`ERROR bg_loop=<name>` and restarts after 1 s with a fresh `mgr.clone()`). **Never add
a bare `spawn(...).detach()`** — compio's own wrap swallows the panic and the task
dies silently. And **never add an unbounded await reachable from a loop**: etcd
`unary_call` has no request deadline (`AUTUMN_ETCD_REQUEST_TIMEOUT_MS`, 10 s) and
`ConnPool::get_or_connect`'s connect sits outside `call_timeout`
(`AUTUMN_MGR_CONNECT_TIMEOUT_MS`, 5 s) — both are bounded; any new pool RPC must use
`call_timeout`. Both are required: `catch_unwind` can't rescue a hung await; a bound
can't catch a panic.

## Policy engine (advisory)

`policy_tick_loop` (leader-only, every `POLICY_BUCKET_SEC = 60 s`) reads per-partition
metrics from `MSG_REPORT_PARTITION_LOAD` aggregations and rebuilds `advisory_cache`
(the ONLY job — the manager is pure mechanism; it never self-dispatches). Emits 7 kinds
(`POLICY_KIND_*`, wire-stable append-only): split / merge / gc / major_compact / minor
_compact / ec / rebalance. `handle_get_policy_candidates` and `handle_get_partition
_detail` are leader-gated (a follower's metrics are empty).

**Metrics window.** `PartitionMetricsWindow::push_with_cap_and_bucket` snaps `ts` to
`bucket_sec` (same-bucket pushes REPLACE), so `recent(required_buckets)` spans the
documented `required_buckets × bucket_sec` regardless of report cadence.
`PolicyConfig.window_buckets` (`POLICY_WINDOW_BUCKETS = 10`) and
`required_buckets` (`POLICY_REQUIRED_BUCKETS = 5`) are load-bearing.
`prune_stale_metrics` runs at the top of each tick (drops metrics for
split/merged/evicted partitions and windows older than `STALE_METRICS_AGE_SEC = 300`).
Hot/cold band guard: a partition is "hot" only if its min ≥
`qps_hottest / HOT_COLD_BAND_DIVISOR` (2), "cold" only if max ≤ `qps_coldest × 2`.

**Size metric (`est_live`).** Raw `PartitionLoad.size_bytes` is LSM-resident bytes
(SST + memtable) and UNDER-counts VP workloads (values > 4 KiB live in `log_stream`
behind ValuePointers, invisible to it — a 19 GB partition can look like ~700 MB). So
every size predicate consumes
`effective_size_bytes = max(size_bytes, est_live_bytes)` where
`est_live_bytes = sealed_sum + open_tail_bytes − gc_debt_bytes − open_tail_dead_bytes`
(saturating). `max` on both sides is strictly conservative: for split `old ∨ new` only
ADDS candidates; for merge `max < threshold` only REMOVES them, so a degraded
`est_live` (open-tail probe not yet run) can never flag a partition the LSM metric
still sees as big. **Never reason about split/merge off raw `size_bytes`.**

**SIZE is not debounced; QPS/imm-full are.** The `required_buckets` "all N buckets must
trigger" rule exists to filter QPS SPIKES. Size (sealed bytes) is a slow, near-monotone
signal, so `split_candidates` / `merge_candidates` evaluate the SIZE condition ONCE on
the CURRENT effective size (a single `sealed_sum` snapshot × the newest bucket), thrash-
guarded by the cooldowns, while the QPS and imm-full dimensions keep the all-N-buckets
debounce. This makes size-based auto-split/merge safe to arm.

**Sacred boundaries (operator-declared presplit cuts).** `handle_namespace_set_presplit`
records declared points into `MgrNamespace.presplit` (etcd-first). The rule is generic —
the manager never learns what a "lane" is; `sacred_boundary_owner(key)` returns the
owning namespace for any declared cut, so fs lane boundaries / kvc hash buckets / mem
agent cuts all get one predicate.
- **Merge guard:** `handle_merge_partitions` refuses (`CODE_PRECONDITION`, unless
  `--force`) when the vanishing boundary (the greater of the two partition start keys)
  is a `sacred_boundary_owner`. `merge_candidates` also SKIPs such pairs so the
  controller never retries a doomed op (the ideal-looking case — an empty cold lane —
  is exactly what must be protected).
- **Auto-split snap:** actuation snaps a split to `declared_split_point_within(part_id)`
  (the declared point nearest the middle of the range) when one lies inside, else falls
  back to PS median selection. Merge refuses to cross a declared boundary, so an
  un-snapped split would drift the layout one way only.

**Default thresholds (`policy.rs`, all runtime-tunable via `set_policy_config`; not
persisted):**

| Const | Default | Meaning |
|---|---|---|
| `SPLIT_SIZE_HARD` | 50 GiB | size-hard split trigger |
| `SPLIT_SIZE_MIN` | 1 GiB | size floor on the QPS split trigger |
| `SPLIT_QPS_HIGH` | 15 000 | sustained QPS split trigger (≈½ the ~30K single-partition ceiling) |
| `SPLIT_IMMFULL_HIGH` | 10 | sustained imm-full/s split trigger |
| `SPLIT_COOLDOWN_SEC` | 3600 | |
| `MERGE_SIZE_LOW` | 1 GiB | both sides small |
| `MERGE_QPS_LOW` | 1500 | summed cold QPS (5% of split-high) |
| `MERGE_COOLDOWN_SEC` | 21600 (6 h) | |
| `GC_DEBT_HIGH` | 1 GiB | GC advisory |
| `COMPACT_PENDING_HIGH` | 4 GiB | major-compact advisory |
| `MINOR_COMPACT_PENDING_HIGH` | 512 MiB | minor-compact advisory |
| `GC/COMPACT_COOLDOWN_SEC` | 300 | ; `MINOR_COMPACT_COOLDOWN_SEC` 120 |
| `EC_MIN_EXTENT_BYTES` | 64 MiB | below this, EC's encode+fanout costs outweigh savings |
| `HOT_COLD_RATIO` / `_SIZE_RATIO` | 10 | hot/cold spread |
| `HOT_COLD_MIN_HOT_QPS` | 10 000 | ; `_MIN_HOT_SIZE_BYTES` 25 GiB |
| `REBALANCE_COOLDOWN_SEC` | 120 | emission; `_MAX_MOVES_PER_TICK` 4 |

## Auto-policy controller

`auto_policy.rs` + `auto_policy_tick_loop`: the in-manager topology/maintenance
controller (folded in from a retired external Python controller; this does NOT revert
the mechanism/policy split — advisory emission stays a separable layer that never
self-dispatches). `AutoPolicyMode` = **Off → DryRun → Armed**. **INVARIANT: runs ONLY
on the leader** (`leader.get()` gate every tick — no candidate read / decision /
actuation on a follower). **DEFAULT-OFF** (a fresh cluster is pure-mechanism); `Armed`
actuates, `DryRun` logs "would: …" but never mutates. The **mode is the whole gate**
— arming is per-policy, with no separate process-wide flag.

Actuation is in-process to the same ops the mechanism layer exposes: split →
`auto_dispatch_split` (snapping to a sacred boundary); merge → the freeze-drain
`handle_merge_partitions` (NOT the raw flush path — avoids the loss window); gc /
compact / forcegc → PS `MSG_MAINTENANCE`; ec → `handle_force_ec_convert`.

Config is **etcd, leader-owned, crash-safe** (`autoPolicy/config` = mode + active +
custom policies, `autoPolicy/cooldowns`), written etcd-first + leader-fenced by
`autopolicy_set`, reloaded by `replay_from_etcd` (fail-loud decode + `sanitize_entry`
clamp — a shorter persisted `switches` Vec pads the absent trailing switches to off).
Switch order is `[split, ec, compact, gc, merge, rebalance]`. Presets are compiled-in,
never persisted, safest → most aggressive:

| Preset | Switches enabled |
|---|---|
| `gc-only` | gc |
| `maintenance` | compact, gc |
| `space-reclaim` | ec, gc |
| `balanced` (recommended steady-state) | ec, compact, gc, rebalance |
| `aggressive` | split, ec, compact, gc, merge, rebalance |

Headless control: `MSG_AUTOPOLICY_GET/SET` + `autumn-op auto-policy
status|activate <name> [--arm]|deactivate`. Manual per-target actions go through
the async op-ledger below (`autumn-op split/gc/compact/merge/force-ec-convert/
rebalance`), leader-routed — the same underlying ops the controller uses.

## Async op-ledger (`op_ledger.rs`)

Every long-running op (split/merge/rebalance/compact/gc/forcegc/ec-convert) is
**submitted through the leader** (`MSG_OP_SUBMIT`), assigned an `op_id`, actuated
in a background one-shot task that reuses `actuate_candidate`'s building blocks
(`auto_dispatch_split` — now takes an explicit `at_key` override — /
`handle_merge_partitions` / `handle_rebalance_regions` / `handle_force_ec_convert`
/ `send_maintenance`), and made queryable (`MSG_OP_QUERY`). This recovers the
failure reason the fire-and-forget maintenance ops used to drop.

- **`OpLedger`** = leader-local, in-memory `VecDeque<OpRecord>` cap 256 (the
  `ACTION_LOG_CAP` pattern). **State machine, not bools**: `Pending → Running →
  Succeeded|Failed`, plus a synthesized `Unknown` — the honest answer for an
  unknown/old id after a leader change (never a false `Running`). `op_id =
  (epoch_ms<<16)|seq16` (non-zero — `0` is the query "list" sentinel).
- **The LEDGER is not etcd-persisted** — orchestration crash-safety already
  lives in the fenced split/merge txns + EC inflight markers; the ledger is pure
  live state.
- **Durable terminal history is `op_log.rs`** (`opLog/<ts_ns>_<seq>` → the
  `OpRecord` itself, so history decodes into exactly what `ops status`
  renders). This is SEPARATE from the audit log on purpose: audit answers "who
  asked for what", is written for every admin RPC, keeps 90 days, and stores
  only `result_code: 0/1` — the error text is discarded at its call site. Op
  history answers "how did this run turn out" and must carry the reason.
  Every terminal transition queues its record synchronously
  (`queue_terminal`, reached from all five terminal paths via `finish` /
  `reconcile_outcome` / `complete_by_extent`); an async caller drains the queue
  (`flush_op_log`) so the etcd write never sits inside a `borrow_mut`. Drained
  from the PS load heartbeat right AFTER the outcome loop (so a completion is
  durable without waiting a heartbeat) and from the policy tick as a backstop
  for kinds no PS reports (recovery, ec-convert).
  **Rotation is by COUNT** (`OP_LOG_CAP`), amortised one sweep per
  `OP_LOG_GC_EVERY` writes: op volume tracks cluster activity rather than the
  clock, so a time window bounds it badly in both directions — a quiet week
  keeps nothing, a compaction storm writes more in an hour than anyone will page
  through. Best-effort like audit: failing an op because its history could not
  be written would turn an observability gap into an outage. Writes are BATCHED
  into one txn per drain — the drain sits on the PS load heartbeat, so a burst
  of completions must not become N serial etcd round-trips on the path that
  keeps fleet liveness accounting current.
- **Reading history** is `MSG_OP_HISTORY` (`handle_op_history` → `read_op_log`),
  deliberately a SEPARATE message from `MSG_OP_QUERY` rather than a flag on it:
  the two answer different questions ("what is running" vs "how did past runs
  turn out") off different sources (a leader-local ring vs etcd), so folding
  them together blurs both the leader-gating and the paging semantics. Keys are
  fixed-width zero-padded, so the prefix scan is already in timestamp order and
  "most recent N" is a tail slice — no sorting by a decoded field and no
  dependence on etcd's return order. An undecodable row is skipped with a
  warning: history is diagnostic, and one bad row must not deny the rest.
  Surfaced as `autumn-op ops history [--kind K] [--since UNIX] [--limit N]`,
  rendered through the SAME formatter as `ops list` so an operator reads one
  format whether a record is live or historical.
- **Terminal reporting split**: manager-orchestrated kinds (split/merge/rebalance)
  close their entry in-process on return; **PS-executed kinds (compact/gc/forcegc)
  stay Running and are closed by the load heartbeat** — the PS records a
  `MaintenanceOutcome{op_id,state,error}` in a small ring, piggybacks it on
  `PartitionLoad`, and `handle_report_partition_load` reconciles by op_id
  (`reconcile_outcome`, once) + audits. ec-convert closes via
  `apply_ec_conversion_done → complete_ec(extent_id)`.
- **TTL backstop**: a Running compact/gc/forcegc older than 30 min flips to
  `Unknown` (`sweep_running_ttl`, on the leader policy tick) — a lost PS outcome
  never sits Running forever. **Attach-dedup**: a resubmit of the same
  `(kind, part_id, secondary_id)` while active returns the existing op_id.
- **Auto-dispatched kinds** (`OP_KIND_RECOVERY`): extent recovery is entered by
  the recovery loop, not by a submit — `MSG_OP_SUBMIT` REFUSES it. Hooks:
  `dispatch_recovery_task` (EN accepted the rebuild) → `note_recovery_dispatch`
  (one entry per extent, counting `attempts`); `record_dispatch_outcome`'s Err
  arm → `record_recovery_failure`, which **keeps the entry RUNNING** (the loop
  retries with exponential backoff and never gives up) while carrying the last
  reason + `error_code` (`err_to_code`) + consecutive-failure count;
  `apply_recovery_done` → `complete_recovery`. This is why recovery belongs in
  the ledger: a repair looping on the same failure is otherwise invisible
  per-extent (only aggregate in `recovery-stats`).
- **`error` on a RUNNING op is deliberate** for auto-retrying kinds — it is the
  LAST attempt's failure, not a terminal verdict.
- **Live progress** (`OpRecord.progress_done` / `progress_total`) is carried as
  RAW COUNTS, never a percentage — the wire carries facts and the consumer
  derives the ratio (the same rule cluster-df follows). A bare "50%" cannot
  distinguish two tables from fifty gigabytes, and an operator deciding whether
  to wait needs the magnitude; `autumn-op ops` renders both, in the unit the
  kind actually measures (bytes for gc/forcegc, tables for compact). PS-executed
  kinds publish a sample from their own loop
  (`PartitionMetrics::set_maintenance_progress`, once per GC chunk — never per
  record) which rides `PartitionLoad.active_maintenance`. `update_progress`
  touches only RUNNING entries, so a sample arriving after the outcome — the
  PS re-sends its outcome ring every heartbeat — can neither reopen a closed op
  nor resurrect one the cap evicted. `record_maint_outcome` clears the sample at
  every terminal exit, so a finished op never shows as forever mid-flight.
- **EC convert now uses the recovery model** (dispatch ≠ completion): the
  coordinator EN ACKs "accepted" and encodes in the background;
  `node_health_loop` applies each `DfResp.ec_done` report using the etcd
  marker's PINNED assignment (`extent_inflight_payload_ec`), never the report's
  own fields — a `new_eversion` mismatch is refused fail-loud and the marker
  kept. `dispatch_one_ec_conversion` no longer finalizes on the RPC return; the
  EC dispatch loop is bounded-concurrent (8) so one slow coordinator can't stall
  the tick. This removes the "RPC timeout vs dead EN" ambiguity that made a stuck
  marker un-releasable; a dead PINNED coordinator still needs fence→auto_abandon.
- **Failover seeding is by durability, not by kind**: `seed_replay(kind, …)`
  reconstructs RUNNING entries for BOTH EC-convert and recovery on promotion
  (their etcd markers survived and this leader keeps working them);
  compact/gc/forcegc are PS-local, so an old id honestly answers `Unknown`.
- **`--wait`** is a pure client-side poll over `MSG_OP_QUERY` — one execution
  path, no divergent sync/async behavior. `MSG_OP_SUBMIT` is leader- + admin-gated
  (`is_admin_mgr_msg`); `MSG_OP_QUERY` is leader-gated (a follower's ledger is
  empty).

## Web dashboard (standalone app)

The manager **no longer serves a web UI** — the old in-manager `dashboard.rs`
(axum over `cyper_axum::serve` + `include_str!` HTML) is gone. The dashboard is
now a standalone app, `examples/dashboard` (the `autumn-dashboard` binary), which
holds no cluster state and drives the cluster ONLY through `autumn-op` — so the
wire schema stays in exactly one place. It is token-gated (`--admin-token[-file]`).

What survives in this crate is `dashboard_compose.rs`: the pure `/api/overview`
composer (df + nodes + partitions + amplification + advisories), shared with
`autumn-op overview` so the app renders the same view the manager used to serve.
Manual actions map to the allow-listed `autumn-op` subcommands above.

## GC lifetime, VP retention, both-zero reclaim

**`refs`-only retention (with an upgrade guard).** The load-bearing invariant: **GC
relocates every live in-range value off a log extent BEFORE `punch_holes` drops its
`refs`** (relocate-then-punch; liveness is full VP identity, not just `extent_id`), so
`refs == 0 ⇒ no live ValuePointer`. CoW split keeps both children pointing at the
shared log extents via `refs`; the extent is freed once BOTH children GC it to
`refs == 0`. `extent_can_delete` keeps `refs == 0 && vp_table_refs == 0` as an
**upgrade-safety guard**: the `vp_table_refs` maintenance machinery is gone (frozen at
0 for every extent managed under this build), but a legacy extent frozen in etcd at
`refs == 0 && vp_table_refs > 0` (a live VP the old buggy GC left) must not be reaped
until a Stage-2 migration re-confirms + clears it. Collapsing the gate to `refs == 0`
in the same release that removes the net would cause data loss on the first
post-upgrade sweep.

**Both-zero orphan sweep** (`extent_both_zero_sweep_loop`, leader-only, 60 s). An
extent that lost its last stream membership out-of-band sits at both-zero with no
`punch_holes`/`truncate` path to fire its delete. The sweep reclaims it. **Candidate
gate:** `extent_can_delete(ex)` AND the extent is **absent from every stream's
`extent_ids`** (the membership check is NOT redundant with `refs == 0` — a refs
under-count must never let the sweep delete a still-membered extent; a
both-zero-but-in-a-stream extent is ERROR-logged + skipped). Delete is etcd-first
value-CAS on the snapshot, then in-memory remove + `enqueue_pending_deletes`.

## Cluster identity, version, and capacity

**`cluster_id`** (`autumn-rs/cluster_id`): CAS-imprinted to a UUID by the first leader
(`imprint_cluster_id`); memory-mode keeps a per-process UUID. `MSG_GET_CLUSTER_ID`
(no leader gate — followers answer from replay). `autumn-op format` is the single
per-EN entry point (fetches cluster_id, allocates a `disk_uuid` per dir, registers,
writes sentinel files; idempotent; mismatched cluster_id → refuse). The EN verifies
cluster_id twice at startup (each `--data` dir agrees; one round-trip to the manager)
before the listener binds.

**`cluster_version`** (`autumn-rs/cluster_version`, ASCII decimal — deliberately not
rkyv so it outlives serialization eras). CAS-imprinted to this binary's
`WIRE_VERSION_MAX`; `bump_cluster_version` is leader-only, exactly current+1, capped at
`WIRE_VERSION_MAX`, value-CAS'd. `parse_cluster_version` (the only decode point) is
**fail-closed on rollback**: it refuses a persisted value above this binary's
`WIRE_VERSION_MAX`, so through replay an old binary can't become leader after a bump.

**Upgrade safety = stop-world + rkyv fail-loud.** 生产升级 = 全停 → 换二进制 → 全起,
etcd 永不清(绝不 `cluster.sh reset`)。安全来自 rkyv 校验式 `from_bytes`:新二进制
读旧 etcd,布局不符则响亮失败(`replay_from_etcd` 报错 → 当不上 leader),绝不静默解成
错值。**Invariant: any persistent-struct change (etcd value / SST / .meta / WAL) is
either same-rkyv-layout or ships a versioned one-time migration — never rely on reset.**

**`cluster_df`** (`MSG_CLUSTER_DF`, leader-gated). Ceph-style aggregate, in-memory only,
built inside the single `node_health_loop`: RAW + `physical_used` are summed from each
EN's self-reported `DiskStatus.extent_bytes` every tick (owner reports, control plane
sums — no manager-side counters); `logical_stored` is a periodic (~30 s) read-only scan
of `s.extents` (`Σ distinct sealed_length` skipping both-zero). The wire carries only
raw u64 facts; the amplification factor and EC-dependent writable range are computed by
the consumer.

**Overview / df open-tail rules.** `compute_cluster_overview_resp`'s per-partition
`live_size` = `Σ distinct extents' sealed_length` (manager-authoritative) **plus** the
latest PS-reported `open_tail_bytes` — an OPEN extent's manager `sealed_length` is 0, so
a log-heavy / major-compacted partition whose data lives in open tails would otherwise
render 0 B. **Invariant: never re-introduce a sealed-length-only `live_size`.** For
cluster-df, `ClusterCapSnapshot.logical_open_tail` companions `logical_stored`, and the
amplification MUST be `physical_used / (logical_stored + logical_open_tail)` — sealed-
only inflates it ~15×. **Invariant: any physical/logical ratio must compare like
scopes.** (Overview double-counts CoW-shared extents across siblings; df open tails are
`refs=1` partition-private, so no CoW dedup — different views.)

## Inode leases (fuse close-to-open coherence)

`inode_lease.rs::LeaseRegistry` — a JuiceFS-style inode-level lease served by the
manager (same etcd backing as owner locks; serves autumn-fuse). Single writer XOR many
readers per inode; a reader and the writer may coexist (reads through an open file stay
legal). 4 RPCs (`0x46`–`0x49`). Clients are `MgrClientId {kind, uuid, host}` — identity
is `(kind, uuid)`; `host` is diagnostic only.

Invariants (enforced by code structure):
- **L1** Manager is the single decision-maker — `acquire` returns `WriteConflict`
  synchronously when another client holds the writer slot.
- **L2** Writer release bumps `version` BEFORE pushing the invalidation (the reader
  sees the new generation paired with the event).
- **L3** Writer leases are PERSISTED (`inode_leases/<ino>`, leader-fenced); reader leases
  are memory-only. Failover rehydrates writers (`install_persisted_writer`, clamping the
  deadline to the TTL against clock skew); reader-set loss is benign (daemons invalidate
  everything on subscribe reconnect).
- **L4** `tick(now)` captures the reader set BEFORE evicting expired readers (order:
  writer revoke → reader expiry → drop-empty-inode → push invalidations) so a reader
  expiring on the writer's TTL boundary still gets the push.
- **L5** `inode_lease_revoke_loop` is `spawn_supervised` with only bounded awaits; etcd
  failure logs WARN and retries next tick (in-memory revoke already fired).
- **L6** `host` never affects identity.
- **L11** `version` is monotonic across the inode entry's FULL lifetime, not just a live
  entry: `last_version` shadow preserves the high-water mark across remove/re-create so a
  re-acquire never re-hands `(ino, version)` a stale reader cache still holds.
- **L12** at most one parked waker per `ClientInbox` (`drain_or_park` replaces it; the
  displaced sender drops → the prior long-poll resolves `Canceled` = "no events, retry").
- **L13** `ClientInbox::push` fires the parked waker before returning (else a long-poll
  waits up to `LONG_POLL_WAIT` = 10 s, breaking "writer close → reader sees bytes within
  ~ms").

Consumer-side coherence rules (design rationale for the fuse consumer): a subscribe
disconnect / overflow sentinel must drop EVERY held lease + cached fd (partial
invalidation is a footgun); a cache-stale Read must reload extents before serving or
return EIO (never serve pre-close bytes). See `docs/autumn_fs_lease_plan.md`.

## Namespace registry

Etcd string-keyed registry `namespace/<name>` → `MgrNamespace {name, prefix,
owner_tenant, presplit, created_at}` (modelled 1:1 on the `tenantAccount/` DB):
in-mem shadow, fail-loud replay, admin-token-gated create/delete (`MSG_NAMESPACE_CREATE`
`0x57` / `DELETE` `0x58`), etcd-first + leader-fenced, serialized on `namespace_admin_lock`.
Built-in families `fs`/`kvc`/`mem` are CAS-preregistered by the first leader
(`seed_builtin_namespaces`, `owner_tenant=None` = existence-only). Create rejects
reserved names + names failing `validate_namespace_name` (`[a-z0-9._-]+`) +
`namespace_prefix_conflicts` (a new `name/` may not be `starts_with`-related to any
existing prefix, either direction — pairwise-disjoint intervals). Delete refuses the
built-ins; the non-empty guard is CLIENT-SIDE in `autumn-op` (range-scan, `--force`
overrides) because the manager has no KV data-plane client.

`MSG_NAMESPACE_LIST` (`0x59`, leader-gated read-only) returns the rich rows
(`Vec<MgrNamespace>`, sorted). The 5 s authz-config poll stays lean (prefixes only).

**Authz bridge (`handle_get_authz_config`):** `namespaces` = every registered prefix
(the Layer-A data source the PS consumes); `protected_prefixes` = the manual
`--auth-protected-prefix` list ∪ every registry namespace whose `owner_tenant.is_some()`
(auto-protected). `CODE_NAMESPACE_UNKNOWN = 10` is the Layer-A reject the PS returns.

## fs inode allocation

`fs_alloc.rs`, `MSG_ALLOC_INODES = 0x53` — the manager grants contiguous inode ranges
`[base, base+count)` for the fuse fs (replacing a client-side non-CAS RMW that
duplicated batches under concurrent allocators). Etcd mode: authoritative counter at
`fs_next_inode_key(volume)` (strict BE u64; malformed → refuse loudly). Every grant is a
read → `txn_fenced` value-CAS loop (leader fence prepended, so a deposed leader's grant
loses the txn — no double-grant across a transition); first-create uses the
create_revision==0 pattern; no in-memory cache (failover needs no replay hook). Migration
floor: the request carries the legacy KV counter value; the grant never returns a base
below it (`max(cur, floor)`) and the counter never rewinds. This is deliberately NOT
`alloc_ids` (that numbers manager entities replayed from etcd prefixes; inode numbers
are fs-layer data with their own key).

Per-volume machinery is present but **DORMANT**: the fuse layer passes an EMPTY volume,
so production uses the single global `autumn-rs/fs/next_inode`. The lease/fence plane
keys by BARE ino, so per-volume inodes would collide across volumes → cross-volume
write-lease conflict. Data isolation comes from the `{volume}/` KEY prefix, not the
inode number; the frozen `AllocInodesReq.volume` field + machinery stay for a future
volume-aware-lease feature. `handle_alloc_inodes` is leader-gated.

## Routing while leaderless

`get_regions` and `heartbeat_ps` gate on `ensure_routable()` = `leader || !displaced`
(the two READ-ONLY routing/liveness RPCs) rather than the strict `ensure_leader` that
every mutating handler keeps. During an etcd outage the ex-leader's in-memory routing is
the freshest in existence and nothing can supersede it (no election, no mutation), so
strict-gating would black-hole every fresh client for the whole outage. `displaced`
(default TRUE, cleared on winning the election, set when the election CAS or a leader-fence
fence diagnosis observes a DIFFERENT instance in the leader key — a *missing* key is
lease-expiry, not displacement) keeps a rejoined FOLLOWER from serving replay-stale
regions. Bounded by `ROUTABLE_STALE_TTL` (15 min from `leaderless_since`): in an
asymmetric partition a peer may have taken over, and after the TTL PSes get NOT_LEADER
and rotate to the real leader. PS-side `MAX_CONSECUTIVE_NOT_LEADER = 450` (15 min) is a
SEPARATE heartbeat exit budget from the transport budget — NOT_LEADER proves the manager
is reachable, and a leaderless control plane can't evict anyone. Data safety is always
`owner_epoch`/`region_epoch` fencing, never these stale-read bounds.

**`part_addrs` is in-memory and PS-self-healed.** `handle_register_partition_addr` is
NOT leader-gated and `part_addrs` is deliberately NOT mirrored to etcd — it is a routing
hint lost on manager restart, re-reported by each PS from `sync_regions_once` (~2 s)
whenever the `GetRegions` response shows the manager's view missing for a partition it
serves. A follower accepting the idempotent hint is harmless.

**Serving gate on the eviction sweep.** `serve()` calls `mark_serving()` AFTER the
listener bind returns (re-seeds every `ps_last_heartbeat`, flips `serving = true`);
`ps_liveness_check_loop` skips while `!serving`. A respawned manager can win the election
seconds before its listener socket is bound (it retries through a predecessor's
TIME_WAIT for ~60 s) — without the gate it would evict the entire healthy PS fleet while
no PS could possibly heartbeat into the unbound socket.

## Observability

`AutumnManager::metrics_text()` renders leader/serving gauges + store counts (streams /
extents / nodes / partitions / ps_nodes / regions / part_addrs), per-disk online (the
`df` call-result signal), and the inflight-op count. Because the store is `!Send`, the
`--metrics-port` path runs a 2 s publisher task on the compio runtime that copies the
rendered string into an `Arc<RwLock<String>>` served by the shared
`autumn_common::metrics_http` listener thread. A follower's counts are replay-stale —
scrape `autumn_manager_leader` to pick the authoritative instance.
