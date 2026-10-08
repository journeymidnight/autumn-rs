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
  merge_partitions, reconcile_extents, force_ec_convert. The raw merge txn
  (`handle_multi_modify_merge`) runs only inside `merge_partitions`; its opcode
  0x34 is retired (no freeze drain, so it lost a source's unflushed writes).
- **PartitionManager**: register_ps, upsert_partition, get_regions,
  get_client_regions (`0x60` — the SAME routing answer narrowed to the four
  fields an SDK reads; `get_regions` keeps all seven for the PS and autumn-op,
  and both forms are served at once, see `crates/rpc/CLAUDE.md`), heartbeat_ps,
  register_partition_addr, report_partition_load, rebalance_regions.
- **Policy/advisory**: get_policy_candidates, get_policy_kind_names,
  get_partition_detail, autopolicy_get/set (`0x54`/`0x55`).
- **Node lifecycle**: list_node_states, fence_node, set_node_maintenance,
  clear_node_override, remove_node, recovery_stats, query_audit_log,
  report_disk_failure, extent_health_report, extent_health_summary (`0x63`),
  list_ec_inflight_markers, remove_member (`0x64`, see "PS membership"),
  get_cluster_status (`0x65`, see "Cluster status").

`extent_health_report` calls a slot unhealthy when its node is Suspected /
Fenced / Maintenance, or when `avali` is clear **on a SEALED extent**. The
`sealed` qualifier is not a refinement: `avali` is the per-slot "holds the
sealed content" bit, an OPEN extent carries none by construction, and without
the guard every open tail reads as a fault — 21 of 21 reported extents on a
7-partition cluster, which is no signal at all. The response carries
`sealed_length`, not the `sealed` flag, so the state is only visible
server-side; `autumn-op info --json --part P` prints an `open` flag per extent
when an operator needs it.
- **Identity/capacity**: get_cluster_id (`0x45`),
  cluster_df, get_cluster_overview.
- **Inode leases** (`0x46`–`0x49`): acquire/release/heartbeat_lease,
  poll_invalidations. **fs inode alloc**: alloc_inodes (`0x53`).
- **Namespace/authz**: namespace_create (`0x57`), namespace_delete (`0x58`),
  namespace_list (`0x59`), namespace_set_presplit, principal_list (`0x5A`),
  get_authz_config.

`update_stream_ec` mutates `persist::StreamRecord.ec_data_shard / ec_parity_shard`; the
`ec_conversion_dispatch_loop` then converts sealed extents to the new shape.

### WIRE version discipline

Manager RPC structs are rkyv, which has no version tag and no cross-version decode.
`WIRE_VERSION` in `crates/rpc/src/lib.rs` is maintained BY HAND — there is
no fingerprint and nothing checks the bump for you. Cluster peers require it to
match exactly on an internal connection. The first mandatory VERSION_HELLO
migration is stopworld; later wire changes can roll while unlike versions
refuse RPC, subject to the release's persistence and recovery analysis.
`GetClusterIdResp` preserves its frozen layout for identity lookup;
wire_version_min is the client floor, wire_version_max the server wire, and
cluster_version is reserved zero.

`handle_connection` completes `VERSION_HELLO`, then `PEER_AUTH` for Peer/Admin
connections (cluster secret, `autumn_rpc::peer_auth`), before the business
decoder.
Clients use the supported interval; internal/admin connections require exact
wire equality. It checks service/role/opcode before spawning business handlers.
`MSG_GET_REGIONS` is explicitly shared by admitted clients and peers; silent
or refused connections cannot reach it. `tests/client_wire_admission.rs` drives
the real listener (client boundaries, mismatch, role checks); the secret is
covered against the real binary by `autumn-server`'s `tests/cluster_secret.rs`.
New message-type numbers and enum variants
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
    //   lease registry, namespaces, tenant_accounts, authz keyring …
}
```

`store` (`src/store.rs`, moved here from `autumn-common`) holds streams,
extents, nodes, disks, partitions, regions, owner revisions. Every persistent
mutation is mirrored to etcd when `self.etcd.is_some()`.

It moved because a persisted record is `pub(crate)` to this crate (see
"Persisted records") and `MetadataState` is what will hold those records in
memory — a state struct in a SHARED crate cannot hold a type only the manager
may name. Nothing outside the manager referenced it, so no other crate changed.
`is_owner_epoch_fence_message` deliberately stayed in `autumn-common`, because
`autumn-stream` classifies manager rejections with it and must not depend on the
manager; producer and matcher are held together by the shared `OWNER_*_TOKEN`
constants plus `owner_fence_matcher_pairs_with_producer`, which came here with
the producer and reaches across to the matcher.

Invariants that came with it:
- **ID uniqueness** — every id (stream, extent, node, disk, partition) comes
  from one monotonic counter; never generate one outside `alloc_ids`.
- **The owner lock bumps on EVERY acquire** — `acquire_owner_lock` returns a
  strictly higher revision each call, fencing the previous holder. A stable
  per-key epoch makes failback A→B→A impossible and lets two live processes
  share one epoch (split-brain). Never mint owner revisions elsewhere.
- **`ensure_owner_epoch` before every stream mutation** — skipping it allows
  split-brain writes.

## Leader election

Preceded by holding `--manager-id` (see "Manager identity and membership").

Lease-based (10 s TTL):
1. Create lease; CAS-write `autumn-rs/stream-manager/leader = instance_id` if absent.
2. On win: `replay_from_etcd` rebuilds all in-memory state, set `leader = true`,
   start a keepalive loop (every 2 s).
3. Lease expiry / keepalive failure → `leader = false` (step down).
4. A background loop retries election every 2 s when not leader.

`replay_from_etcd` is **fail-loud**: a persisted rkyv blob that no longer decodes
refuses leadership (`replay_decode_err` with an actionable message) rather than
silently decoding stale bytes into wrong values.

## Persisted records (`persist/`) — the manager's own schema

Five schemas exist here, each answering "who is writing to whom", and four of
them carry their own version: SST/WAL/checkpoint (`AU7B` + `FORMAT_VERSION`),
`.meta`/`.ck` (`EXTMETA\x02`), the cluster-internal wire (`WIRE_VERSION` exact
equality), the client wire (the window). **The manager's records were the hole**
— defined in `crates/rpc/src/manager_rpc.rs` under the wire-schema banner, so
changing what the MANAGER remembers bumped the number every PS, EN and embedded
client is judged by. Measured: of 44 wire-version intervals only 8 (18%) truly
needed manager + PS + EN to move together, while the extent node could have
stayed up for 30 (68%).

A split record lives in `persist/records.rs`, is `pub(crate)` (the "only the
manager may reference it" rule, enforced by the compiler), and is stored inside
an envelope:

```text
[magic b"AUMG": 4][record_type: u8][format_version: u8][rkyv bytes …]
```

`record_type` catches a record written under the wrong key. `format_version` is
PER RECORD — adding a field to the namespace record moves that number and
nothing else, which is the whole point. **`RECORD_TYPE` numbers are frozen and
append-only**; they are written into every stored value, so renumbering one
silently re-labels every record on disk.

**Decoding VERIFIES and never sniffs.** A value without the expected envelope is
an error that refuses leadership, not "maybe it is the older form" — see the
Upgrade-safety section for why guessing from the leading bytes provably cannot
work here.

**ALL ARE SPLIT.** extent=4, stream=5, node=6, disk=7, partition=8,
region=9, audit=1, tenantAccount=2, namespace=3, recoveryAttempt=10,
member=11; the next record takes 12.
`MetadataState` (now `src/store.rs`, moved here from `autumn-common`) holds the
RECORDS, not the wire structs — which is what gives a purely persistent field
somewhere to live. `RangeRecord` is a FIELD of the partition and region records
rather than a record of its own: it has no key and no `RECORD_TYPE`, so changing
its shape moves BOTH of their `FORMAT_VERSION`s.

**Every writer goes through `persist_kv_entry` / `persist::encode`, and that
includes every CAS BASELINE.** `Cmp::value` compares stored bytes byte for byte,
so a baseline re-encoded without the envelope could never match a value written
with one: every CAS would fail, and split / merge / GC / recovery would retry
forever against a conflict that is not there. Baseline and value are the same
function for that reason. The bare-rkyv helpers `kv_entry` and
`replay_decode_id_map` are DELETED — with all nine records enveloped, a writer
that skipped the envelope could only be a mistake, so the tool for making it is
gone.

Other persisted keys stay BARE and must not be wrapped: `opLog/`,
`extent_inflight/`, `extentDeleteRetry/`, `node_override/`, `decommissioned/`,
`inode_leases/`, `autoPolicy/*`, `extentLayout/`, `extentCorrupt/`,
`partitionLastOp/`, `psNodes/`, `ownerLocks/`, and the three `autumn-rs/*`
singletons. Their types already live inside this crate; wrapping one without
adding it to the converter would make the manager unable to read it.

Verified on a LIVE cluster, because no automated test executes `Cmp::value` on
the split/merge path: real etcd + manager + EN + PS, 12 values written,
`autumn-op split` then `merge` both succeeded with every value byte-correct
afterwards, and a manager restart replayed all nine record kinds clean.

`persist/freeze.rs` records each encoding byte for byte. It is the deliberate
replacement for a guard that was ACCIDENTAL: while a record shared a file with
the wire schema, editing it forced a `WIRE_VERSION` bump. That was far too blunt
(it stopped every PS and EN for a change none of them could see) and removing it
without a replacement would have been far too loose. Read that file's header
before touching a recorded value — the answer to a red is never "re-record".

## Data model (etcd key layout)

All writes go through the leader-fenced `txn_fenced` (below). On promotion
`replay_from_etcd` reads every prefix to rebuild memory.

| Prefix / key | Value | Notes |
|---|---|---|
| `nodes/<id>` | `persist::NodeRecord` | EN record; identity is `node_uuid`, not address |
| `disks/<id>` | `persist::DiskRecord` | manager-allocated `disk_id` |
| `streams/<id>` | `persist::StreamRecord` | membership RMW is value-CAS'd |
| `extents/<id>` | `persist::ExtentRecord` | `refs` RMW is value-CAS'd |
| `partitions/<id>` | `persist::PartitionRecord` | key range |
| `regions/<id>` | `persist::RegionRecord` | carries `region_epoch` |
| `psNodes/<id>` | PS address | LIVE registry; deleted on eviction |
| `psMembers/<id>` | `persist::MemberRecord` | EXPECTED fleet; only `remove_member` deletes it |
| `managerAlive/<id>` | `<instance_id>\n<address>` | on the manager's own lease; claim + presence |
| `managerMembers/<id>` | `persist::MemberRecord` | expected managers; leader-written |
| `next_id` | u64 | the ONLY id source (`alloc_ids`) |
| `ownerLocks/<key>` | owner epoch | `owner_epoch` = the acquire's `mod_revision` |
| `extent_inflight/<id>` | `MgrExtentInflightRecord` | unified in-flight ledger |
| `extentLayout/<id>` | 1 byte | payload location; absent ⇒ `InDat` |
| `extentDeleteRetry/<id>` | `MgrExtentDeleteRetry` | budget-exhausted delete retries |
| `partitionLastOp/<id>` | i64 LE unix | last split/merge timestamp |
| `node_override/<id>` | `MgrNodeOverride` | Fenced / Maintenance |
| `decommissioned/<uuid>` | tombstone | uuid-keyed, survives node delete |
| `mgr_audit_log/<ts>_<seq>` | `persist::AuditRecord` | admin-op audit trail (90-day GC) |
| `inode_leases/<ino>` | writer lease | reader leases are memory-only |
| `namespace/<name>` | `persist::NamespaceRecord` | registry |
| `tenantAccount/<name>` | `persist::TenantAccountRecord` | authz principal DB |
| `autoPolicy/config`, `autoPolicy/cooldowns` | policy state | leader-owned |
| `autumn-rs/cluster_id` | UUID | CAS-imprinted once |
| `autumn-rs/fs/next_inode` (or `…/fs/{tenant}/{volume}/next_inode`) | BE u64 | fs inode counter |

`part_addrs` (client routing hints) is deliberately **in-memory only** — see the
leaderless-routing note below.

## Admin auth & KDC

**Operator-only ops.** `autumn_rpc::manager_rpc::is_admin_mgr_msg` lists the ops
served only on an Admin connection (cluster mutations, principal and namespace
admin); `check_opcode` refuses them on a Peer connection, and an Admin
connection must have proved the cluster secret (PEER_AUTH). There is no admin
token: handlers take the bare request. The manager's own split / flush / gc
calls to a PS go out over its Peer connection, authenticated the same way.

**KDC keyring (`authz.rs`).** `AuthzKeyring` is the manager's Ed25519 signing
keyring loaded from `--auth-signing-key-file`; **its mere presence = authz enabled**.
File format `<kid> <hex-32-byte-seed> [disabled]`, fail-loud on any malformed line
(never start half-armed). `active()` = highest-numbered ENABLED kid (mints new
tokens); `published()` publishes ALL kids incl. disabled so the PS learns to reject a
disabled kid. The token codec/claims live in `autumn_rpc::cap_token` (shared
signer/verifier). `credential_hash` = SHA-256; compares are constant-time
(`ct_eq_32`) to avoid timing/length oracles.

**Principal accounts.** `tenantAccount/<name>` → `persist::TenantAccountRecord
{tenant, credential_hash, allowed_prefixes}` (a PERSISTED record with its own
format version — see "Persisted records"; it has no wire twin at all); create/delete are Admin-connection-only, etcd-first,
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
1. Refuse an `op_id` the ledger has already ended (see "Async op-ledger");
   validate owner epoch; validate `mid_key` inside the range; verify the
   request's captured tail extent ids still match each stream's CURRENT tail
   (refuse `split captured tail moved` otherwise — a roll that landed after
   the PS's capture would get the captured length stamped onto its fresh
   empty tail; 0 = no claim, skip).
2. `alloc_ids(4)` → new log/row/meta stream ids + new part id.
3. `duplicate_stream` each of the 3 streams at its sealed length (shares extents).
4. Left range → `[start, mid)`; right created as `[mid, end)` with new stream ids.
5. `rebalance_regions` (bumps left's `region_epoch`, seeds right's = 1).
6. Persist everything in one fenced etcd txn.

No replay bookkeeping is kept here for split or merge: the PS's freeze drain
writes a checkpoint at each source's committed log end before the manager seals
anything, so a child or a merged survivor replays nothing from before the
operation (partition-server CLAUDE.md, "Recovery replay start").

Both children initially share the same physical extents; each `PartitionServer`
detects `has_overlap` on open and major-compaction cleans out-of-range keys and
frees the shared extents via GC.

**`duplicate_stream`**: for each non-tail extent, `refs += 1` + add to the new
stream; for the tail, set its sealed length at the split point, bump `eversion`,
`refs += 1`, add. `compute_duplicate_stream` is the read-only pure form (the applier
is `apply_split_mutations`).

**`region_epoch` (TiKV-style)** on `persist::RegionRecord`, bumped through
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

**Every PS choice goes through one ranking, `ps_placement.rs`.** A PS started
with `--cpuset` has `cpuset_len / 2` core slots and reports them as `slot_cap` on
register AND on every heartbeat; a PS without one reports `0` (capacity unknown).
The manager keeps them in `MetadataState::ps_slot_caps`, **in memory only** —
not etcd, so no persisted record changes: `replay_from_etcd` clears them and each
PS's next heartbeat (≤ 2 s) brings its cap back; until then that PS ranks as
capacity-unknown. Eviction drops the cap with the PS. The ranking, best first:

1. a cpuset PS with a free slot, most free slots first;
2. a capacity-unknown PS, fewest partitions first;
3. a full cpuset PS, lowest fill it would reach, `(used + 1) / cap`.

Tier 3 exists so a placement always has an answer: the manager never refuses
and reserves nothing. The PS still refuses to open a partition past its budget
(it shows `ps=unknown` until moved), so a placement there waits for a move. Ties break by lowest `ps_id` (the old count-based pick
iterated a `HashMap`, so its ties were arbitrary).

A rebalance move is made only when the target's seat ranks STRICTLY better
than the seat the partition leaves (`seat(to, used)` < `seat(from, used - 1)`),
and never onto a full cpuset PS: under the PS's refusal such a move only trades
which PS leaves a partition unopened, and the partition moved (the source's
largest `part_id`) may be one that was serving.
Each move therefore replaces one element of the cluster's multiset of seats by a
strictly smaller one; over a finite state space that cannot cycle, so rebalance
never moves a partition back and forth — the property is structural, not a
cooldown. Considering only the worst source and the best target is complete,
because a seat never improves as `used` grows.
`rebalance_reaches_a_fixed_point_from_every_small_start` checks termination and
the fixed point over every 3-PS configuration with caps 0–3 and 0–5 partitions
each. Among capacity-unknown PS all of this reduces exactly to the previous
count balancing (gap ≤ 1).

`rebalance_regions` is **STICKY, not a balancer**: it keeps a region on any
still-registered PS (only refreshing `rg`) and places unassigned ones by the
ranking above.
Called eagerly after `register_ps`, `upsert_partition`, `multi_modify_split` (safe
because idempotent). The `rg` refresh on keep is critical — otherwise `GetRegions`
returns a stale pre-split range.

The active balancer is `compute_rebalance_moves(state, max_moves)` (pure; moves
until no move ranks strictly better — see the ranking above; deterministic ties):
`handle_rebalance_regions` rewrites each moved region's `ps_id` in-memory then
`mirror_partition_snapshot`. `rg` is unchanged so `region_epoch` is NOT bumped (only
the serving PS moved); the PS `sync_regions_once` picks up the `ps_id` change and the
old PS drops / new PS opens. Exposed as `autumn-op rebalance [MAX_MOVES]` and as the
auto-policy `POLICY_KIND_REBALANCE` (7) arm. The advisory asks the moves' own
question with a hysteresis band: it fires when a move would still rank strictly
better after its source gave up `rebalance_gap_threshold` partitions
(`ps_placement::imbalanced`) — among capacity-unknown PS exactly the old
"max − min count > threshold", and silent on a count gap no move would touch
(a full cpuset PS beside a busier capacity-unknown one).

**Actuation cooldown floors (rebalance + compaction).** `decide_actions` floors the
actuation cooldown of rebalance and of BOTH compact kinds at a non-configurable 60 s
(`REBALANCE_MIN_ACTUATION_COOLDOWN_SEC` / `COMPACT_MIN_ACTUATION_COOLDOWN_SEC`). Both
compact kinds, because a major and a minor candidate actuate the identical `compact`
op. Advisory-side cooldowns only gate EMISSION, and an emitted candidate lingers in
`advisory_cache` for a whole 60 s policy-tick window while the loop ticks every
`interval_sec` (min 2 s) — so a policy with `cooldown_sec = 0` would re-actuate the
same cached candidate ~30 times per window: a partition-reopen storm for rebalance, a
repeated full-SST rewrite for compaction. For compaction the floor is the only guard
on the `unblocking_compact` path, which does not suppress on `compact_cooldown_sec` (it
keys on a FLAG, not a debt level). The two compact kinds keep separate cooldown keys,
so a partition with both rows is compacted at most twice per floor window. Both floors
sit below every preset's `cooldown_sec` (120-240 s), so they can only catch a
misconfiguration.

## PS liveness

`ps_last_heartbeat: Arc<Mutex<HashMap<u64, Instant>>>` (ephemeral, not persisted).
`register_ps` seeds a timestamp; the PS calls `heartbeat_ps` every 2 s (both
carry `slot_cap`, recorded only for a registered PS — see "Rebalance");
`ps_liveness_check_loop` (2 s) evicts a PS not seen in 10 s — fenced
`put_and_delete_txn(delete psNodes/<id>)` then `rebalance_regions`. On eviction
`handle_heartbeat_ps` returns `CODE_NOT_FOUND` so the PS re-registers +
`sync_regions_once` (silent `CODE_OK` would leave it invisible as `ps=unknown`).
`replay_from_etcd` seeds `ps_last_heartbeat = now` for every replayed PS so the
liveness loop's `Some(t)` arm engages instead of treating it as an immortal zombie.

The PS spawns its `heartbeat_loop` in `finish_connect` (NOT `serve()`, which only
runs after every assigned partition finishes WAL replay — that can exceed the
eviction window).

**Alive is not ready.** Because the heartbeat starts before any partition has
recovered, a fresh heartbeat says only that the process is up. Every heartbeat
also carries `open_parts` — `(part_id, region_epoch)` of each partition the PS
has open with a live thread, empty once it starts a graceful drain (which sends
one extra beat at once). The manager keeps the latest set per PS in
`MetadataState::ps_open_parts`, in memory only like the slot caps: replay
clears it, eviction drops it, and `register_ps` drops it, so a restarted process
never inherits its predecessor's report. The overview's `open_count` counts the
PS's assigned regions whose `(part_id, region_epoch)` is in that set (`None` =
no report yet); an epoch mismatch — a split the PS has not reloaded — does not
count. `PsOverview::ready()` = heartbeat younger than 6 s AND `open_count ==
partition_count`; autumn-op, the dashboard, `cluster.sh` and `autumn-deploy`
all use it. After a `kill -9` the old report still reads ready until the new
process registers or the heartbeat turns 6 s old — there is no signal the
manager could see sooner.

### PS membership

Eviction answers "is it alive", and its answer must not also become "how many
PS should there be": with only `psNodes/`, a dead PS vanished from every list
after 10 s and a 3-PS cluster read `2/2`. So the expected fleet is a second
key family, `psMembers/<id>` → `MemberRecord {address, joined_at_ms,
left_at_ms}`:
- `register_ps` makes the id a member (or clears `left_at_ms` and refreshes the
  address); it writes only when the record changes, and then writes
  `psNodes/<id>` in the same txn — a member replayed without it would never be
  in the live registry, so never evicted, and would read as neither live nor
  evicted.
- `evict_silent_ps` keeps the member and stamps `left_at_ms` in the same txn
  that deletes `psNodes/<id>`.
- `MSG_REMOVE_MEMBER` (Admin; `autumn-op ps-remove <id> --by X`) deletes it,
  refused (`Precondition`) while the id is in the live registry. A removed id
  that starts again simply rejoins; there is no tombstone (a PS holds no data).
- All three serialize on `ps_member_lock`, which the eviction pass takes BEFORE
  judging who is dead, so a registration cannot land between the verdict and
  the stamp.

The overview's `ps_servers` is members ∪ live registry; an evicted member has
no heartbeat and `evicted_at_ms` set. psid uniqueness is the OPERATOR's job:
two processes with one psid are counted as one PS, with no error (the
manager does not judge registrations). There is no seeding
from `psNodes/`: an upgrade is a full restart, and every PS registers.
Test: `tests/ps_members_etcd.rs` (evicted PS listed, survives a leader change,
live remove refused; ablations: eviction forgetting the member, replay not
loading it, remove skipping the live check — each red).

## Manager identity and membership (`manager_members.rs`)

`--manager-id` is hand-assigned and required with `--etcd`. Startup claims
`managerAlive/<id>` with a `create_revision == 0` txn on a 10 s lease of the
manager's own (separate from the leader lease) and retries every second for
as long as it takes, logging the holder: a predecessor's lease lapses within
10 s, a misconfigured duplicate keeps holding and the log names it. A replay
failure revokes the lease so a retry is not made to wait.

Losing the lease (keepalive fails, etcd blip) does NOT stop the manager — the
leader fence already makes a manager without leadership harmless, and exiting
on a blip would take every manager down at once and void the leaderless
routing window. `presence_loop` claims the id again; a key still carrying
this process's instance id is adopted with its lease. Only when the reclaim
finds ANOTHER process holding the id does the manager exit.

The leader folds presence into `managerMembers/<id>` every 2 s
(`sync_manager_members`, fenced): a present id becomes a member (or has its
address refreshed and `left_at_ms` cleared); a member no longer present gets
the time the leader noticed. Standbys write nothing. `remove_manager_member`
deletes a member in ONE fenced txn that also requires `managerAlive/<id>` to
be absent, so a running manager is refused without a check-then-act window;
a removed id that starts again rejoins. Test: `tests/manager_members_etcd.rs`
(ablations red: claim ignoring the holder, no reclaim, remove without the
presence compare). The exit path is checked by hand (docs/ops.md).

## Cluster status (`cluster_status.rs`)

`MSG_GET_CLUSTER_STATUS` (leader-gated) answers "is the fleet whole" with
every EXPECTED member in a list whose length is the denominator: managers =
`managerMembers/` ∪ present ids ∪ the leader itself (memory mode: just the
leader, id 0); partition servers = `ps_servers_overview` (members ∪ live
registry); extent nodes = registered nodes. Each carries a `FLEET_*` state —
leader / standby / absent; ready / opening / silent / evicted; online /
suspected / suspend / fenced / maintenance (an override wins) / unknown —
and an age. An extent node counts as online only after a `df` answer to THIS
leader (`has_first_hand_df`): replay seeds every node Online, so right after
a failover a dead fleet would otherwise read `EN Online 6/6`.
Plus the extent counts of the health summary and the Recovery markers in the
ledger. `managerAlive/` is read from etcd during the call (the only await;
leadership is re-checked after it), and everything else is read at one
instant stamped `sampled_at_ms`. A follower answers NOT_LEADER, never a stale
view. Rendered by `autumn-op status [--json]` (`dashboard_compose::status_json`
is the JSON shape) and carried as the overview's `status` field
(`build_overview_json`; `null` when the leader did not answer or predates the
opcode), which the dashboard draws as its Overview status bar. Tests: `cluster_status::tests` (classification) and
`tests/cluster_status_etcd.rs` (standby, evicted PS, a registered node that
never answered, a stopped standby turning absent; ablations: counting only
present managers, not telling an evicted PS apart — both red).

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

Recovery and EC commits share `inflight_commit`: snapshot the marker before the
first await, then compare both its persisted bytes and its etcd `mod_revision`
in the same transaction as the extent value-CAS and marker delete. The bytes
bind the assignment; the revision distinguishes a delete-and-recreate with
identical bytes. After the await, the same identity is checked again before
installing memory state, so a delayed response cannot release or overwrite a
successor attempt. Marker-only release uses the same identity check. A failed
durable Recovery cleanup keeps the in-memory marker; the standing-instruction
tick recognizes stale layout again and retries without requiring a leader
change.

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

Deploying onto pre-ledger etcd state is unsupported — those keys never existed,
so there is nothing to migrate from. That is a dev-cluster situation and NOT
licence to wipe a real cluster's etcd; see the upgrade-safety note below.

## Recovery

Recovery instructions and completions carry a nonzero marker creation revision
and a pinned source/target snapshot (wire 46). The source eversion, replacement
slot, sealed length, EC shape and payload location cannot drift between dispatch
and apply. Node UUID and the completed disk's ID/UUID must still match.

The unchanged bare inflight record is paired with a new enveloped
recoveryAttempt/<extent> record (type 10, format 1), created and deleted in the
same transactions. Replay checks both creation revisions; missing snapshots on
old Recovery markers cause cancellation and re-dispatch, never acceptance of an
unidentified completion. The persisted record has its own fields and exhaustive
wire conversions, so later wire edits do not reshape it. Its bytes are frozen in
recovery_attempt.rs. No existing record conversion is needed for this addition.

node_lifecycle_lock serializes marker acquisition and Recovery/EC publication
with register, fence, maintenance, override clear and remove. Recovery apply's
etcd transaction also compares node/disk bytes, absence of override/tombstone,
marker revision/bytes and snapshot revision/bytes. The lock covers the commit
through memory installation, not the data copy or post-commit cleanup. Fence
cancels target Recoveries; failed cancellation remains a Remove blocker and is
retried by the standing-instruction tick. Remove checks Recovery as well as EC
markers. A delayed dispatch failure is scoped to the request's nonce.


**Dispatch loop** (2 s, `recovery.rs`): scans all SEALED extents; per slot,
`slot_verdict` decides whether the copy moves (Rebuild → `require_recovery` to a
healthy candidate). A copy that stays and is BEHIND (`avali` clear, replicated
extent, node Online, disk not offline) is caught up in place with `re_avali` to
the sealed length. In-flight recoveries live in the unified inflight ledger so a
double-dispatch is impossible across failover.

**The marker is a STANDING INSTRUCTION, not a do-not-disturb flag.** A marker pins
one `(extent, executor)` assignment; the leader keeps RE-SENDING that exact RPC
(`redispatch_pinned_recovery`, 5 s timeout, skipped when the pinned node is not
Online so a keep-alive to a corpse can't eat the whole dispatch tick) and **never
drains the marker on an RPC failure**. That is what makes an EN restart
self-healing without a TTL: the EN loses its in-memory `recovery_inflight`, the
next re-send simply starts it again, and every EN answer is idempotent by
contract (same-attempt already-running → `CODE_OK`; complete local copy → re-report done;
incomplete residue → discard + rebuild — see `crates/stream/CLAUDE.md`).
**Release is EVENT-driven:** `apply_recovery_done` (the
work finished), `release_recovery_markers_for_dead_executors` (level-triggered
each tick — the pinned node is gone from `s.nodes` or no longer Online → drop the
marker so re-derivation picks a live target), and
`release_recovery_markers_for_healthy_slots` (level-triggered each tick — the
SOURCE slot no longer needs rebuilding at all). The standing-instruction tick
also retires a marker whose extent disappeared, source slot was replaced, or
target became a member at another slot; this makes a failed durable cleanup
self-retrying. **There is deliberately NO
wall-clock TTL**: a timeout is indistinguishable from a slow-but-progressing
rebuild, and releasing on one races the executor still writing the copy. Never
re-introduce a TTL, and never drain a Recovery marker on a dispatch error.

The third exists because **fence CREATES markers and unfence is on no release
path**. The two executor-shaped releases both ask about the node doing the work,
so an operator who fenced a node and changed their mind left one zombie marker
per slot: the work is pointless (the slot is healthy, so it can never complete)
yet the executor is alive (so nothing retired it), and each one held a
`RecoveryRateLimiter` slot for the life of the leader. Measured on a
fence→unfence drain: `global 4/64`, `per_source: node 5 → 4`, and a fresh
rebuild on another extent running normally beside them — the mechanism was
fine, only these four had outlived their reason.

Its predicate is "does this slot still need rebuilding", NOT "is the node
un-fenced" — a disk fault, a dark `avali` bit and a corrupt-slot mark are all
legitimate marker sources that do not care about overrides. So it asks the
DISPATCHER's own question, through `slot_verdict` rather than a re-derivation,
and releases only on `Keep`: the node is registered and Online, its `avali`
bit is set, its disk is online, and the verdict would not rebuild this slot now.
It is deliberately STRICTER than the dispatcher in one place — it counts ANY
override as a reason to keep the marker, where the dispatcher's verdict tests only
`NODE_OVERRIDE_FENCED` — so a node moved from Fenced into Maintenance keeps its
markers until that override clears.

One state looks healthy but must KEEP the marker (found in review): between a
promotion and the source node's first `df`, `faulted_disks` is empty and
`disks/<id>` replays `online: true`, so a genuinely faulted disk reads as
healthy. For DISPATCH that gap is the safe direction (keep the copy); for
RELEASE the sign flips and it would discard a real, possibly mid-copy rebuild
after an ordinary failover. `has_first_hand_df` gates on `node_max_free`,
which only a successful `df` writes and replay never seeds.

A marker that spins without progressing is also visible now: the re-send logs
refusals, undecodable replies and unreachable targets at WARN. They were all
`debug!`, so a manager at INFO rendered a marker re-sending every 2 s as
complete silence — during the same investigation that read as "recovery is not
dispatching at all", which was false and cost the wrong diagnosis.

`recovery_dispatch_loop` holds only the 2 s cadence and the leader gate; the
pass itself is `recovery_dispatch_tick`, so a test can drive exactly one tick
(`the_dispatch_tick_releases_an_obsolete_marker`). A release the loop never
reaches is the same green-tests-dead-mechanism shape as a flattened error code,
so the call site is pinned by a test, not just the predicate.

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

A third guard covers the OTHER predicate, "extents I have no record of", which
is still condemned immediately (startup reconcile is expected to clear orphans
in one round): `allocating_extents` holds an id for the length of its
allocation, because `place_extents_with_fallback` creates the files on the nodes
BEFORE the etcd commit publishes `extents/<id>`, and a sweep answered inside
that window would otherwise order the deletion of an extent that was just
created. It is an RAII guard, so no error path can leave an id stuck in it.

**Deletes name their target.** Each `DeleteTarget` in a pending/persisted delete
carries the node's `node_uuid` next to its address, and the node refuses a
request addressed to a different uuid. Address alone is not identity: a
persisted retry outlives the address's ownership, and extent ids restart from
small integers in the next cluster on that host — see the stream guide's
"Delete extent" section.

**One recovery mode.** `slot_verdict(fenced, corrupt, disk_faulted) -> Rebuild
| Keep` decides whether a slot's copy moves to another node. It is a function
because it was three inline checks whose ORDER was the bug — the gate returned
before anything looked at the disk, so a slot on a DEAD disk was never
considered, and nothing could assert on it.

Three things move a copy on their own, each because it is CONCLUSIVE rather
than suggestive: an operator `Fenced` node (that is what fencing is for), a
slot a partition owner PROVED corrupt (it replayed those bytes; `re_avali`
compares length, which a full-length rotted replica passes), and a disk **its
own node named faulted on its last df**. A fourth is a DECISION rather than
evidence: a repair request (see "Extent repair"), made by an operator or by
the repair policy after the slot stayed degraded past its grace. Nothing else
moves a copy: a node that stopped answering may be back in seconds, and moving
its data is expensive and irreversible.

A copy that stays may still be BEHIND: a member that was down when the extent
was sealed never had its `avali` bit set (seal needs only one answering
member). When its node answers again the loop sends `re_avali`, the node copies
the bytes up to `sealed_length` from its peers — lit ones first, never one
marked corrupt (a corrupt-isolated copy is full length; the manager sends the
bitmap beside the extent info, stream CLAUDE.md "Copy sources") — and
`mark_extent_available` sets the bit. The conditions and why:
- replicated extents only — an EC node answers `re_avali` OK without checking
  that its shard exists;
- node Online and disk not offline — an unreachable node would only time out;
- no op in flight on the extent — the bit flip bumps the eversion, and a
  sibling slot's pinned Recovery would be judged stale at its next re-send,
  retired, its finished copy refused and the rebuild started over.
  `mark_extent_available` refuses for the same reason if an op started during
  the copy. It also refuses a slot marked corrupt meanwhile (the catch-up
  vouched for length only) or one that no longer holds the node it caught up,
  and it writes etcd-first with a compare-and-set against the record it read,
  so a concurrent writer of the extent (split's refs, punch's ref drop) fails
  it instead of being overwritten;
- in the BACKGROUND (`start_catch_up`), at most `CATCH_UP_MAX_INFLIGHT` (8) at
  once and one per slot — the node copies before it answers (30 s timeout),
  and the dispatch tick is serial: a node back from a long absence with
  hundreds of behind extents would otherwise hold release, re-sends, every
  other rebuild and the fenced-tail drain for the sum of the copies.
A failure goes to the slot's CATCH-UP backoff (`catch_up_backoff`, cap 300 s,
timed from when the attempt ended — separate from the rebuild backoff, so
failed catch-ups never delay a rebuild the slot later needs) and leaves the
copy where it is; it is never a reason to rebuild elsewhere. Regressions: `tests/single_wal_replica_survivor.rs` (two replicas
return 4096 bytes short of a single-replica seal and are brought up to it; a
returning replica is never copied from a dark, rotted one) and
`a_behind_slot_waits_for_the_op_in_flight_on_its_extent`; the fenced sole seal
member is rebuilt from the dark replicas
(`a_fenced_sole_seal_member_is_rebuilt_from_its_own_copy`).

**The faulted fact is `faulted_disks`, NOT `MgrDiskInfo.online`, and that
distinction is the whole safety of this.** The bool carries three meanings —
the node said this disk is faulted, the node did not answer `df` at all
(`mark_node_disks_offline`, node-wide, on a 5 s timeout), and a quorum of
partition servers reported the node. Only the first is evidence about a DISK.
Reading `online` in the verdict would rebuild every sealed slot of any node
that missed one heartbeat: a rebuild storm, and the thing the documented
rolling-restart runbook relies on not happening. `faulted_disks` is
in-memory, leader-local, written only by `apply_df_disk_health` from the node's
own per-disk answer, and empty after a failover — so an unrebuilt slot is
kept until the owning node says again that its disk is bad.

`Full` never reaches here: it is transient and self-heals at 5% free, so the EN
reports it `online: true`. That mapping (`DiskFS::online()` is
`health() != Faulted`) is what keeps a cluster low on space from rebuilding
itself, and `health_state_machine_transitions` pins it.

`apply_df_disk_health` also stores the payload's per-disk `online` into
`s.disks`, and only for disks the reporting node OWNS. It used to set every one
of the node's disks online on any successful df — on the stated grounds that
the payload's disk ids were EN-local and unrelated to the manager's, which is
false — and since a node with one dead disk still answers df, that overwrote
the only fact that could repair it.

Scope: SEALED extents only, like all recovery. An open tail on a faulted disk
is not rolled (`drain_fenced_open_tails` is fence-only); it rolls as the
partition writes and is rebuilt once sealed.

**Corrupt slots (`extent_corrupt.rs`, sibling key `extentCorrupt/<id>` → u32
bitmap).** A clear `avali` bit says a slot is not serving; it cannot say WHY,
and the two reasons need opposite handling. *Behind* → `re_avali` refetches the
missing tail. *Corrupt* → `re_avali` CANNOT help: its whole test is
`local_len >= sealed_length`, which a full-length rotted replica passes. So
`handle_report_corrupt_replica` records the darkened slots here in addition to
clearing their bits, and `recovery_dispatch_loop` rebuilds a marked slot. `apply_recovery_done` clears the mark (the rebuilt
slot holds fresh bytes copied from a healthy peer), and extent deletion drops
the key alongside `extentLayout/`. **Without the mark the dark slot reads as
BEHIND**, and the dispatch loop's in-place catch-up (`re_avali`, a length
check the rotted copy passes) would put it back in service. So the isolation
and its mark are written in ONE etcd transaction
(`persist_isolation_with_mark`), on both the RPC and the scrub path, and a
repeated report of an already-dark slot records a missing mark on both paths
too (a slot isolated before the two shared a transaction). Sibling key rather than a `persist::ExtentRecord` field
for the same reason as `extentLayout` — widening the persisted `extents/<id>`
value breaks rkyv replay validation, which refuses leadership.
Regression: `crates/manager/tests/system_corrupt_replica_rebuild.rs`; the
prerequisite that the loop can act at all is pinned by
`system_recovery_loop_drives.rs`.
The report names one of the partition's streams, and the manager accepts the
log stream (a WAL replay's finding) or the row stream (an SST block that
failed its CRC on one replica and decoded from another,
`SstReader::reread_block`); the meta stream holds no SST block and is refused.
`handle_report_corrupt_replica` REFUSES EC-converted extents, before the
shared decision: the reporter's evidence — a CRC-failed WAL decode plus the same
bytes found clean on another copy — only exists for full replicas, and an EC
extent has no other copy of any byte. Extending the report to the EC READ path
(client infers corruption from a failed shard read) was considered and REJECTED
— see `crates/stream/CLAUDE.md` note 33. A rotted SHARD is reported by its own
node instead, when a scrub finds it. The bitmap + rebuild verdict here are
slot-generic over `replicates ++ parity`, and the EN-side scrub (see "Scrub"
below) is the second evidence source: it reports its own rot as a ROT
outcome on `DfResp.scrub_done` and `node_health_loop` runs the SAME decision —
`isolate_rotted_slot`, the helper every first-hand report goes through.
Self-reporting needs no fencing (a node saying "my copy is bad" can only hurt
itself), which is why it does not use the PS-shaped RPC.

Both entry points share `compute_corrupt_isolation`, which answers `Stale`
(not judged) when the eversion moved, refuses on an OPEN tail, when no reported node is a member, when it
would darken the LAST available slot — on an EC extent, when fewer than K
shards would stay available (below K nothing reconstructs, so the shard's range
would go from wrong to gone) — and answers `Stale` **while the extent
has a stream-layer op in flight** — isolating into that window moves the
eversion out from under the op, and an EC conversion's flip then recomputes
from the post-isolation baseline and lands with the eversion unchanged across a
replicated→EC layout change, leaving cached layouts with no signal to refetch.
Deferring costs a re-check, not the finding: the PS
retries its report, and a scrub finding is held in its op (`ScrubOp::recheck`)
and that copy scrubbed again under the current eversion once the extent has
nothing in flight and its node is Online (`recheck_scrub_findings`, after each
`df` round; synchronous, with the sends spawned, so a dead shard listener
cannot hold the `df` round, and with no await between the in-flight check and
the re-plan). Without that, two rotted copies of one extent lose the second
finding every time: the first one's isolation moves the eversion the second was
planned under. Accepted limits: with a spare node the first slot's rebuild can
start before the second finding is re-checked, and copy from that still-lit
rotted copy (the rebuilt copy has no checksums, so the next scrub records the
damage as its content); and a second rot refused as the last available copy is
settled without a mark. The
converse holds too: `acquire_extent_inflight` refuses a NEW EC conversion
marker on an extent with any corrupt-marked slot — the coordinator encodes from
its own copy whenever that copy is full length, and a marked copy is; the slot
is rebuilt first and the conversion proposed again after. The cost: an extent
whose marked slot has no rebuild target (RF = node count) cannot be converted
until a node is added. Rot nobody has reported is caught by the coordinator
itself (stream CLAUDE.md, "The EC coordinator checks the `.dat` it encodes"):
the dispatch answer `CODE_CONTENT_CORRUPT` makes `release_rotted_ec_attempt`
abandon the marker, fail the op and isolate the coordinator's slot at once —
isolation refuses while the marker stands, so this is the only moment it can. The
already-isolated path re-drives `mark_slots_corrupt` when the slot is dark but
unmarked — a slot isolated before isolation and mark shared a transaction can
be in that shape.

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
Backoff belongs to the NEED, not to an attempt: each tick keeps only the backoff
of slots its pass judged `Rebuild`, of slots whose Keep is not yet a verdict (a
repair request waiting on an in-place catch-up; a node this term has no
first-hand `df` from, whose faulted disk would read healthy) and of extents
under EC conversion (`end_rebuilds_no_longer_owed`); deleted and unsealed
extents lose theirs. The no-`df` hold lasts as long as the node stays silent,
up to the whole term for an unfenced dead node; `health` then reports that
slot unreachable, so the views still agree that something is owed. A failed
dispatch drains its marker but leaves the backoff behind, and a slot that stops
needing a rebuild (unfence: the copy serves again) is never dispatched again,
so without that the backoff outlived the need in `recovery-stats`
(`an_unfence_ends_the_failed_rebuild_s_backoff_and_op`).
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

**Placement scoring (`placement.rs`).** Allocation (`select_nodes`) and recovery
(`dispatch_recovery_task` via `recovery_candidate_order`) answer the same
question — of the nodes ALLOWED to hold this extent, which should — so they
share one scorer. They did not: allocation shuffled, recovery took the lowest
`node_id` past the rate limiter. Ascending id is an ACTIVE bias and it cost a
migration: draining one node sent 12 of its 27 shards onto the two nodes queued
for decommission next (smallest ids), while four empty nodes got nothing.

Ranking is BANDED LEXICOGRAPHIC, not a weighted sum — utilization in 5-point
bands, then open extents, then shards. A sum needs a constant trading bytes
against counts that nobody can defend. **Sealed is capacity, open is load**: a
sealed extent never grows and its bytes are already counted, while an open one
is an append target, so a node that just took ten tails still reports almost no
bytes and would keep winning on capacity alone. Load comes from
`cluster_cap.per_node` (per-disk SUMS each df tick — NOT `node_max_free`, which
is the MAX across a node's disks and is right only for "can this node take an
extent at all") plus per-node slot counts riding the chunked `logical_stored`
scan. No new telemetry.

**Allocation samples, recovery sorts.** `pick_least_loaded` takes the best of 2
random candidates because argmin herds — every concurrent allocation picks the
same emptiest node. Recovery uses `order_by_load` (full sort, ties shuffled)
because `RecoveryRateLimiter`'s `max_per_target` already spreads a burst down
the list, so sampling there only loses accuracy. `select_nodes` re-shuffles its
result before returning: `replicates[0]` is the append leader and chain head,
and score order would concentrate that on the emptiest node — a different
resource than this change is about.

Scoring runs strictly AFTER the hard constraints and cannot reach past them:
`occupied`, `hard_excluded`, online-disk, `min_alloc_free_bytes`, and the rate
limiter, which is a gate and never a score term.

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

`abandon_ec_marker` is CAS'd on the persisted record and re-reads the attempt
nonce after its etcd await, so a release decided against a stale view cannot
release a SUCCESSOR's marker — the release path is as attempt-scoped as the
apply path (`classify_ec_done`).

**Payload location (`extent_layout.rs`).** Which FILE holds an extent's payload
— `.dat` or `.shard{i}` — is per-extent metadata the manager owns and the EN
obeys; the EN never infers its own role. It lives in the sibling key
`extentLayout/<id>` (absent ⇒ `InDat`) rather than in `persist::ExtentRecord`, because
that struct is the persisted `extents/<id>` value: widening it would make an
existing cluster's stored extents fail rkyv validation on replay, which refuses
leadership rather than degrading. It reaches readers on `ExtentInfoResp`
alongside the extent. `handle_extent_info` fills it; extent deletion drops it.
A legacy EC extent is `ec_converted = true, InDat` — the pre-CoW scheme renamed
each shard over `.dat` — so it keeps working with no backfill.

**The stored byte is parsed ONCE, at replay, and it is fail-loud like every
other persisted value.** `extent_payload_location` holds the RESOLVED location,
not the byte, so no read of the map can re-answer the question. An entry naming
a location this build does not have refuses leadership, and so does an empty
value or an unparseable key — it can only have been written by a newer manager
that was then rolled back, which is the case `replay_from_etcd` is fail-loud
about everywhere else. This REVERSES the earlier rule ("drop it with a WARN and
read the extent as `InDat`, rather than refuse leadership over a byte that only
selects between two files"), whose premise was that `InDat` is the neutral
default. It is not: it is a positive claim that `extent-{id}.dat` holds the
payload, published to every reader on `ExtentInfoResp`, and on a converted
extent it points readers at the wrong file. ABSENT is untouched and still means
`InDat` — that is the migration story and the only reason this key can be
sparse.

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

**Target selection (`handle_force_ec_convert`).** Targets are the extent's
replicas (positionally paired with `replicate_disks`, the coordinator encodes its
own `.dat`) plus extra nodes drawn outside `placement_excluded_node_ids`. A
replica on an excluded node (Suspected / Fenced / Maintenance) refuses the
submit with `CODE_PRECONDITION`: no shard write to it can land, and the marker
would block the repair that moves the replica.

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
`mgr_fence_node`: precheck unless `--force` (`check_capacity_for_fence`: every extent
on the node needs a recovery target under `recovery_candidate_order`'s own filters —
not `occupied`, not fenced/maintenance/suspected — one of them with df-reported room
for its shard, and those receivers together 1.2x the bytes; no df row = no room),
write the override, then
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
(`mgr_audit_log/<ts_ns>_<seq>` → `persist::AuditRecord`, best-effort). `mgr_query_audit_log`
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
`compute_commit_seal(members, recovering, responses)`. We wish every committed member
would answer, but ONE is enough: whichever answers holds all the acked data (perhaps
more), and after the seal a committed member shorter than the min still holds all of
it, so a
single surviving replica of the WAL is enough to recover. It refuses only when no
committed member answered. There is no setting to demand more (the
`AUTUMN_MGR_SEAL_DURABILITY_FLOOR` env knob was removed). Position is always `min` over
responders. An unreachable committed member gets its `avali` bit left unset →
reconciled by recovery later; it does not block the seal.

**Phantom-commit is ACCEPTABLE.** Seal-over-reachable can promote an
un-acked-but-replicated tail byte to committed (data *gain*, never *loss*),
consistent with uncertain-write semantics. Do NOT add strict-mode/watermark threading
to kill it — it trades a benign gain for a real loss risk.

**Authoritative seal state.** `persist::ExtentRecord.sealed: bool` is the authoritative STATE
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
`call_timeout` (or `connect_then_call`, the same bound with the connect failure
kept apart). Both are required: `catch_unwind` can't rescue a hung await; a bound
can't catch a panic.

## Policy engine (advisory)

`policy_tick_loop` (leader-only, every `POLICY_BUCKET_SEC = 60 s`) reads per-partition
metrics from `MSG_REPORT_PARTITION_LOAD` aggregations and rebuilds `advisory_cache`
(the ONLY job — the manager is pure mechanism; it never self-dispatches). Emits 9 kinds
(`POLICY_KIND_*`, wire-stable append-only): split / merge / gc / major_compact / minor
_compact / ec / rebalance / repair / scrub (the last two from extent state and
the clock, not partition metrics — see "Extent repair" and "Scrub"). `handle_get_policy_candidates` and `handle_get_partition
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

**Two size measures, and which predicate gets which.**
- `PartitionLoad.size_bytes` — **LSM-resident**: Σ SST len + memtables.
- `effective_size_bytes = max(size_bytes, est_live_bytes)` — **carried**, where
  `est_live_bytes = sealed_sum + open_tail_bytes − gc_debt_bytes − open_tail_dead_bytes`
  (saturating). This adds the large-value payload sitting in `log_stream`, which a VP
  workload keeps out of the LSM behind ValuePointers — carried runs ~60× LSM.

**A SPLIT candidate must name a bottleneck a split relieves.** There are three, and
each has its own metric: request rate (one partition = one P-log thread, ~30K ops/s →
`req_per_sec`), byte rate (one partition = one log_stream, ~350 MB/s →
`write+read_bytes_per_sec`), and LSM size (compaction/memtable work, and it is what a
key-range cut actually halves → `size_bytes`). **Carried bytes are not a fourth**: the
payload lives in the shared log_stream, a CoW split leaves both children on the same
extents, and nothing separates them until a major compaction rewrites the tables. A
partition carrying 73 GiB behind a 0 MiB LSM with no load gets nothing from a split
except that compaction bill — and the policy advised exactly that, every window, until
`SPLIT_LSM_HARD` was pointed back at LSM bytes. Carried bytes keep their two other
jobs, where the payload IS the question: the floor under the rate triggers
(`SPLIT_SIZE_MIN`) and the veto on merge (`MERGE_SIZE_LOW`, since merging two fat
partitions really does put all those bytes behind one thread).

**Compact before split/merge — both refuse while `has_overlap` is set.**
`handle_split_part` refuses with `cannot split: partition has overlapping keys` while
the partition's SSTs still carry keys outside its range, and the PS's `MSG_MERGE_FREEZE`
refuses a merge (`cannot merge: …`) while EITHER side does; only a MAJOR compaction
clears the flag. For merge the victim is not exempt for being deleted: its tables become
the merged partition's, and an un-separated side re-exposes its out-of-range keys over
the sibling's history — pre-split values, and keys the sibling deleted after the split
(partition-server CLAUDE.md, "Merge requires both sides physically separated"). So the
merge pass emits the compaction for EVERY overlapping side of the pair.

Both paths emit `unblocking_compact`, a plain `POLICY_KIND_MAJOR_COMPACT` whose reason
names the op it unblocks, instead of the topology op; the op follows on a
later tick once the flag clears. It is gated on `compact_inflight` and deliberately NOT
on the compact cooldown — that cooldown throttles re-advising a debt LEVEL, while
`has_overlap` is a flag only a completed major compaction clears, so a compaction that
finished inside the window and left it set did not do the job. KNOWN GAP: while a
compaction IS running the partition contributes no advisory row (its state stays in
`info --part N --detail` and the dashboard drawer).

The flag reaches the manager as `PartitionLoad.has_overlap`; `sst_out_of_range_bytes`
is NOT a substitute (it is the SIZE of the out-of-range records, and reads 0 for an
overlapping partition whose shared tables hold no out-of-range key). If the active
policy has `split` on and `compact` off, the candidate is filtered out by
`kinds_from_switches` and nothing happens — the honest outcome, and a visible one,
unlike a refusal loop.

**SIZE is not debounced; the RATE dimensions are.** The `required_buckets` "all N
buckets must trigger" rule exists to filter rate SPIKES. Size is a slow, near-monotone
signal, so `split_candidates` / `merge_candidates` evaluate the SIZE conditions ONCE on
the current bucket (a single `sealed_sum` snapshot), thrash-guarded by the cooldowns,
while QPS, byte-rate and imm-full keep the all-N-buckets debounce.

**Major compaction has two reasons, and the second exists because deletes are
invisible to the first.** BACKLOG is `pending_compaction_bytes >
COMPACT_PENDING_HIGH` sustained. SETTLE is `PartitionLoad.unsettled_deletes > 0`
with the count unchanged across the whole window. A delete reaches GC only
through a compaction's discard, and for large values the LSM stays a few KB
however many GiB were deleted, so BACKLOG never fires and an idle partition
kept every deleted byte forever — its `est_live` never fell either, so the merge
veto and the split rate-floor kept reading phantom size. "Unchanged" means the
burst has ended: a partition that keeps deleting (cache eviction) would
otherwise be told to compact every cooldown, and one burst needs one
compaction. Same candidate kind, same switch, same inflight/cooldown gates; the
PS flushes its memtable before any major compaction, so the compaction reaches
the deletes wherever they sit, and a success zeroes the count. Real cluster: a
partition emptied of 270×64 MiB values and nothing else happening went from
`est_live` 16.88 GiB stuck forever to SETTLE → GC → reclaimed, unattended.

**One advisory row per (kind, target).** `recompute_advisory_cache` dedups the union by
`cooldown_key`, first wins. The actuator already collapses duplicates
(`decide_actions`), so a second row for the same op only ever reaches a human — and the
passes that know WHY an op is needed run first, so first-wins keeps the better reason.

**Sacred boundaries (operator-declared presplit cuts).** `handle_namespace_set_presplit`
records declared points into `persist::NamespaceRecord.presplit` (etcd-first). The rule is generic —
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
| `SPLIT_LSM_HARD` | 50 GiB | size-hard split trigger, on **LSM-resident** bytes |
| `SPLIT_SIZE_MIN` | 1 GiB | **carried**-size floor under the rate split triggers |
| `SPLIT_QPS_HIGH` | 15 000 | sustained QPS split trigger (≈½ the ~30K single-partition ceiling) |
| `SPLIT_BW_HIGH` | 175 MiB/s | sustained r+w byte-rate split trigger (½ the ~350 MB/s single-log_stream ceiling) |
| `SPLIT_IMMFULL_HIGH` | 10 | sustained imm-full/s split trigger |
| `SPLIT_COOLDOWN_SEC` | 3600 | |
| `MERGE_SIZE_LOW` | 1 GiB | both sides small (carried) |
| `MERGE_QPS_LOW` | 1500 | summed cold QPS (5% of split-high) |
| `MERGE_BW_LOW` | 17.5 MiB/s | summed cold byte rate (10× hysteresis vs `SPLIT_BW_HIGH`) |
| `MERGE_COOLDOWN_SEC` | 21600 (6 h) | |
| `GC_DEBT_HIGH` | 1 GiB | GC advisory — AND the per-extent absolute floor selection uses. Both manager paths build their `MaintenanceReq` through `maintenance_req_for_submitted_op`, which fills `gc_dead_bytes_high` (and `gc_stream_debt`) from this value for an AUTO_GC request that names no knobs — so the advisory and the selection judge on the same number. FORCE_GC carries the spec verbatim and the PS Force arm reads neither field. Before that they did not: the advisory fired on absolute dead bytes and selection asked for a ratio, and GC answered "no eligible extents" every cooldown. |
| `COMPACT_PENDING_HIGH` | 4 GiB | major-compact advisory (BACKLOG); SETTLE has no threshold — any unsettled delete, once none new for the window |
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
Switch order is `[split, ec, compact, gc, merge, rebalance, repair, scrub]` (append-only;
a config persisted with fewer switches reads the missing ones as off). Presets are compiled-in,
never persisted, safest → most aggressive:

| Preset | Switches enabled |
|---|---|
| `gc-only` | gc |
| `maintenance` | compact, gc, repair, scrub |
| `space-reclaim` | ec, gc |
| `balanced` (recommended steady-state) | ec, compact, gc, rebalance, repair, scrub |
| `aggressive` | split, ec, compact, gc, merge, rebalance, repair, scrub |

`repair` actuates first (`kind_priority` 0): the others tune performance and
space, a copy short is durability.

Headless control: `MSG_AUTOPOLICY_GET/SET` + `autumn-op auto-policy
status|activate <name> [--arm]|deactivate`. Manual per-target actions go through
the async op-ledger below (`autumn-op split/gc/compact/merge/force-ec-convert/
rebalance`), leader-routed — the same underlying ops the controller uses.

**A submitted GC that names no knobs gets the cluster's standing policy.**
`maintenance_req_for_submitted_op` (pure, unit-tested) decides: if the
`OpSubmitReq` sets none of `gc_ratio`/`gc_max_size`/`gc_stream_debt`/
`gc_dead_bytes_high`/`gc_empty_only`, the manager fills `gc_stream_debt` and
`gc_dead_bytes_high` from its own `gc_debt_high` and marks the request
`gc_policy_is_standing`. Naming ANY knob makes it an OVERRIDE: it runs exactly
as asked and the flag stays false.

That flag is the ONLY thing allowed to redefine the PS's `gc_debt_bytes` gauge
(see the partition-server guide's GC section), and it is explicit because
inference does not work: nothing about the params separates the controller from
an operator — `autumn-op gc --ratio 0.9 --dead-bytes 100G PART` carries a
perfectly real floor, and treating "has a floor" as "is policy" let one
operator command silence that partition's advisory until the partition
reopened. The fill matters for the same reason in the other direction: the
dashboard renders a GC button beside a GC advisory, and before this a
knob-less dispatch carried no floor, so pressing it ran a GC that could not
collect the bytes the advisory had just fired on.

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
- **Terminal reporting split**: merge/rebalance close their entry in-process
  on return; a split closes when its PS answers OR when its commit names it
  (below); **PS-executed kinds (compact/gc/forcegc)
  stay Running and are closed by the load heartbeat** — the PS records a
  `MaintenanceOutcome{op_id,state,error}` in a small ring, piggybacks it on
  `PartitionLoad`, and `handle_report_partition_load` reconciles by op_id
  (`reconcile_outcome`, once) + audits. ec-convert closes via
  `apply_ec_conversion_done → complete_ec(extent_id)`.
- **Reopen settles PS-executed ops**: the queued/running task lives only in
  the partition's open (cap-1 channel + in-memory progress/outcome ring), and
  every open acquires a fresh `partition/<id>` owner epoch. Dispatch records the
  epoch (`note_ps_dispatch`, read from the pre-send snapshot); the load heartbeat
  and the policy tick call `settle_reopened_maintenance` → `sweep_reopened`,
  which flips an op to `Unknown` once the epoch moved (PS restart, move, merge
  reopen). `Unknown`, not `Failed`: the compaction may have committed just
  before the reopen with only its report lost; after the reopen the EN tail
  fence rejects the old open's appends, so it cannot commit later. A real
  outcome that still arrives overwrites `Unknown` (`accepts_terminal_report`)
  and is written to `opLog/` as a second row (only the same-PS race where the
  reopen lands between the dispatch snapshot and the send can produce one).
  Without this a restarted PS left the op Running for 30 min and every
  resubmit attached to the dead op.
- **TTL backstop**: a Running compact/gc/forcegc older than 30 min flips to
  `Unknown` (`sweep_running_ttl`, on the leader policy tick) — covers a PS that
  stops reporting without the partition being reopened. **Attach-dedup**: a resubmit of the same
  `(kind, part_id, secondary_id)` while active returns the existing op_id.
- **Auto-dispatched kinds** (`OP_KIND_RECOVERY`): extent recovery is entered by
  the recovery loop, not by a submit — `MSG_OP_SUBMIT` REFUSES it. Hooks:
  `dispatch_recovery_task` (EN accepted the rebuild) → `note_recovery_dispatch`
  (one entry per extent); `record_dispatch_outcome`'s Err
  arm → `record_recovery_failure`, which **keeps the entry RUNNING** (the loop
  retries with exponential backoff and never gives up) while carrying the last
  reason + `error_code` (`err_to_code`) + consecutive-failure count;
  `apply_recovery_done` → `complete_recovery`. When the need goes away
  without a rebuild, the entry ends SUCCEEDED as "no rebuild needed any more:
  …" (`withdraw_recovery`): from the healthy-slot marker release, and each
  tick for an extent with an active entry, no slot judged `Rebuild` and no
  Recovery marker (a failed dispatch leaves exactly that: a RUNNING entry
  and no marker); the slots whose Keep is not yet a verdict count as owed.
  The entry's last failure is not kept (SUCCEEDED clears `error`); the
  leader log has it. A late completion of a released attempt is still refused
  ("recovery attempt changed"); that guard is unchanged. This is why recovery belongs in
  the ledger: a repair looping on the same failure is otherwise invisible
  per-extent (only aggregate in `recovery-stats`).
- **`error` on a RUNNING op is deliberate** for auto-retrying kinds — it is the
  LAST attempt's failure, not a terminal verdict.
- **Live progress** (`OpRecord.progress_done` / `progress_total`) is carried as
  RAW COUNTS, never a percentage — the wire carries facts and the consumer
  derives the ratio (the same rule cluster-df follows). A bare "50%" cannot
  distinguish two tables from fifty gigabytes, and an operator deciding whether
  to wait needs the magnitude; `autumn-op ops` renders both, in the unit the
  kind actually measures — BYTES for gc/forcegc (extent bytes scanned),
  ec-convert (shard bytes encoded) and recovery (bytes copied); SST DATA BLOCKS
  for compact (`merge.block_progress()`); PHASES for split/merge. Rendering a
  byte kind as a raw count prints a 16 GiB rebuild as
  `11895046144 / 17179981824`, and rendering blocks or phases as bytes is wrong
  the other way; `human_bytes_or_count` (CLI) and `fmtProgress` (dashboard) are
  the two places that decide, and they apply the same table (both append the unit
  word for count kinds). PS-executed
  kinds publish a sample from their own loop
  (`PartitionMetrics::set_maintenance_progress`, once per GC chunk — never per
  record) which rides `PartitionLoad.active_maintenance`. `update_progress`
  touches only RUNNING entries, so a sample arriving after the outcome — the
  PS re-sends its outcome ring every heartbeat — can neither reopen a closed op
  nor resurrect one the cap evicted. `record_maint_outcome` clears the sample at
  every terminal exit, so a finished op never shows as forever mid-flight.
- **Two ways in, because not every executor knows the op id.** `update_progress`
  is keyed by op id: gc/compact/forcegc, and a submitted split
  (`SplitPartReq.op_id`, wire 56). An untracked split (policy, presplit)
  publishes `op_id: 0` (`PartitionMetrics::set_maintenance_phase`, a separate
  setter and slot: the op-id one treats 0 as "PS-local, nothing to update",
  and a compaction's sample must not overwrite a split queued behind it) and
  the manager routes it to `update_progress_by_part(kind, part_id,
  secondary_id, ..)`. Same shape as
  `update_progress_by_extent`, which exists for the same reason on the EN side.
  `merge` is orchestrated on the leader, so it calls that directly.
  Split/merge report **phases, not bytes**: their steps cost wildly different
  amounts and a byte counter frozen through the expensive one reads as a hang.
  The PS holds a `MaintenancePhaseGuard` for the whole split so every exit —
  including three `?` through a closure and two barrier timeouts — clears the
  slot; a stale phase would otherwise be stamped onto the NEXT split on that
  partition, which has no sample of its own until it reaches phase 1.
  `finish` snaps a SUCCEEDED op to its total, because a split's last phases and
  its RPC reply have no `.await` between them and no heartbeat can land there.
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
- **A split whose reply is lost is not a failed split** (`split_op.rs`). The PS
  runs the split on its own task; the manager's reply timeout (60 s) cancels
  nothing, and recording FAILED there let a retry cut the partition again
  while the first split still committed. So a timeout or a dropped connection
  (`SplitReplyLost`, an error after the request was sent; a status answer or
  a connect that failed is still a failure — `connect_then_call` keeps them
  apart) leaves the op RUNNING in `unknown_splits` with "outcome unknown",
  and a resubmit attaches to it. A fact ends it: the commit
  (`handle_multi_modify_split` gets the op id and calls
  `end_split_op_committed` with no await after the etcd commit) → SUCCEEDED;
  the PS's failure on the load report → FAILED; the partition's owner epoch
  moved since dispatch (the split can no longer pass `ensure_owner_epoch`) →
  FAILED; no load report named the op for `SPLIT_SILENCE_SECS` (30 s; a
  running split names it in every phase sample) → FAILED. The last two are
  verdicts, and **the commit fence makes them true**: `handle_multi_modify_split`
  refuses an op id the ledger has ended. None of them fires while
  `split_inflight` holds the partition — a PS that gave up waiting on a slow
  commit reports FAILED (and its reply says so) while that commit may still
  land, so the actuation treats such a reply as unknown and the heartbeat
  defers the report until the commit attempt is over. Whoever ends the op
  audits it (`finish` returns whether this call ended it). Leader-local like
  the ledger: after a failover the id is unknown and a late commit is accepted.
  Tests: `tests/split_op_outcome.rs` (commit after the timeout → SUCCEEDED and
  a retry attaches; failure after the timeout → the PS's reason; second split
  refused at entry) and `split_op::tests`.
- **`--wait`** is a pure client-side poll over `MSG_OP_QUERY` — one execution
  path, no divergent sync/async behavior. `MSG_OP_SUBMIT` is leader- + admin-gated
  (`is_admin_mgr_msg`); `MSG_OP_QUERY` is leader-gated (a follower's ledger is
  empty).

## Extent health (`extent_health.rs`)

`MSG_EXTENT_HEALTH_SUMMARY` is the extent counterpart of Ceph's PG summary,
counts with no one-word verdict (the fleet is `MSG_GET_CLUSTER_STATUS`'s
job): how many
sealed extents are clean / degraded /
without redundancy / unavailable / rebuilding, how many slots are in each
`SLOT_STATE_*`, and the worst extents by name. One classification,
`classify_slot(SlotFacts)`, serves the summary, the degraded clock and the
repair policy, so they cannot disagree on what "degraded" means. Precedence
(first match): corrupt → fenced → disk faulted (the copy is going away) →
serving (node Online, disk not offline, `avali` set) → maintenance (expected
back) → unreachable (node not Online or disk offline) → behind (node answers,
bit clear). A maintenance node whose copy still serves is SERVING.

An extent's verdict compares serving copies with what a read needs
(`needed_copies`: 1 for a replicated extent — parity slots of an unconverted
extent are full replicas — and the data-shard count for an EC one): fewer →
unavailable, fewer than all → degraded, exactly `needed` → also
"no redundancy left". Open tails are counted, not classified (no `avali`, and a
writer rolls off a replica it cannot reach). Worst first: least margin above
`needed`, then the longest degraded slot.

Nothing here asks a node: it is the leader's own view (node states, the
`disks/` online bit, `faulted_disks`, corrupt marks, recovery markers), built
fresh per call — one pass over the store, cheaper than `extent_health_report`,
which clones every extent: the per-node and per-slot facts are borrowed once
per pass, and a clean extent is counted without being materialized (100 000
sealed extents scan and summarize in ~15 ms in a release build). The "degraded since" clock (`slot_degraded_since`)
is refreshed on the 60 s policy tick (which already walks the whole store) and
is leader-local, emptied at promotion; a slot that recovers and degrades again
between two ticks keeps its first time.

Surfaced as `autumn-op [--json] health [--detail N]` and as the overview's
`extent_health` field (`null` when the leader did not answer — the page says
"unknown", never "healthy"), which the dashboard renders as alert rows. Test:
`tests/extent_health_summary.rs` (a replica on a stopped node → degraded,
naming the extent and slot; back → 0 degraded; ablation: classify an
unreachable node's copy as serving).

## Extent repair (`extent_repair.rs`)

A repair request is a persisted per-slot bitmap (`extentRepair/<id>`, the
shape and the reasons of `extentCorrupt/`): "rebuild this slot on another node
now", without fencing its node. `slot_verdict` returns Rebuild for a requested
slot, the ordinary dispatch moves it, and `apply_recovery_done` clears the
request with the slot; extent deletion forgets it; leader replay reinstalls it
— a decision living only in leader memory would be lost at the first failover,
or whenever the rebuild's executor died.

A request is a decision about a copy that is NOT there, and its node coming
back changes that (Ceph's mark-in cancelling the remaps of its down→out). When
the slot's node answers again: a copy that SERVES has its request WITHDRAWN; a
BEHIND replica is caught up in place first while the request waits — once
caught up it serves and the request is withdrawn — and the request rebuilds it
elsewhere only once the node answers a catch-up that it has no such extent
(`catch_up_copy_gone`: a wiped node rejoined). A timeout or any other failure
is no evidence the copy is gone — a node just back, catching up hundreds of
extents, queues past the 30 s timeout — and moving a copy that a retry would
refill is the move this rule exists to prevent. Without this a node back after
the grace period had every requested copy moved anyway: onto a spare it was
never lost to, or, with no spare (RF = node count), pinned to a rebuild with no
target forever while the catch-up that would have fixed it never ran. All
writers of the request bitmap serialize on `extent_repair_lock` across their
read-modify-write and etcd write (policy, ops, the tick and recovery apply are
different tasks).

"Answers again" is FIRST-HAND only — registered, a `df` applied this leader
term (`has_first_hand_df`), Online, disk reported online — the predicate
`release_recovery_markers_for_healthy_slots` uses. After a promotion every
replayed node reads Online and every disk `online: true` until its first `df`,
dead nodes included; trusting that withdrew every persisted request at each
failover. Withdrawals are batched after the pass (a returning node may carry
one on every slot). A rebuild already dispatched for a copy that serves again
is released with the request (the marker-release predicate then holds); one
for a behind copy runs to completion; copies already moved stay moved.

`plan_repair` decides which slots a request covers: only slots that do not
serve (unreachable, behind; maintenance only on an operator's word) and that
nothing else is moving (fenced, corrupt and faulted-disk slots rebuild on
their own). It refuses an extent a read cannot be served from — no source to
rebuild from — and a slot already requested.

Two ways in, one function (`request_repair`):
- **Operator** — `OP_KIND_REPAIR` (`autumn-op repair <EXTENT>...` or
  `--node <N>`, which the manager expands to every degraded slot on that node;
  `part_id` carries the node). The op is the REQUEST: it is terminal once
  recorded, and the rebuilds then show as recovery entries. Naming a healthy
  or open extent is reported back, not an error for the others.
- **Policy** — `POLICY_KIND_REPAIR`, one advisory per NODE whose slots have
  been degraded at least `--repair-grace-secs` (default 600, Ceph's down→out
  interval), counting only extents with no op in flight and a source to
  rebuild from (`repair_candidates`, computed on the policy tick before the
  engine is borrowed). Per node, not per extent: a node that is gone degrades
  every extent it held, and one row is what an operator reads and what one
  actuation should cover. Actuation records requests only for slots on that
  node degraded past the grace. The degraded clock is leader-local and
  restarts at a leader change, so a failover delays the policy, never hastens
  it. The `repair` auto-policy switch arms it; `maintenance`, `balanced` and
  `aggressive` enable it.

Tests: `tests/extent_repair.rs` — an operator repair moves the copy of a
stopped node to the spare without a fence; the policy advises in DryRun and
moves nothing, then rebuilds when Armed; a request recorded on one leader is
served by the next after a spare joins; a node that returns before its
requests could be served keeps its copies when the spare comes back.
Ablations: `slot_verdict` ignoring the request (the first three red), replay
not reinstalling it (the failover test red), no repair candidates (the policy
test red), never withdrawing (the returning-node test red); unit
`a_repair_request_is_withdrawn_when_its_node_answers_again`.

Visible and cancellable (wire 54): the health summary marks each problem slot
that carries a request (`ProblemSlot.repair_requested`) and counts them
cluster-wide (`repair_requested_slots`); `autumn-op health` prints
"[repair requested]" and the dashboard "(repair requested)" with a Cancel
button. `OP_KIND_REPAIR_CANCEL` (`autumn-op repair --cancel <EXT>... |
--cancel --node N`, `cancel_repair`) withdraws them and restarts their
degraded clocks — without that an Armed policy re-requests a cancelled slot
on its next pass, since its clock already exceeds the grace. The clocks go
FIRST, before the batched withdrawal yields: a slot withdrawn in an early
batch but still carrying its old clock would otherwise be re-requested by a
policy actuation in that window (a reset whose withdrawal then fails only
delays the policy). With `--repair-grace-secs 0` a cancel holds only until the
next policy pass. A rebuild already dispatched runs to completion (its marker
is its own standing instruction). Tests:
`a_standing_repair_request_is_shown_and_can_be_cancelled` (ablations: cancel
that does not withdraw; a summary that does not mark) and
`extent_repair::tests::a_cancel_restarts_the_grace_period` (ablation: no
clock reset).

## Scrub (`extent_scrub.rs`)

`OP_KIND_SCRUB` (`autumn-op scrub EXT... | --part P | --all`, or the weekly
policy) checks sealed copies against their recorded checksums ON THE NODES THAT
HOLD THEM; no content crosses the network, and nothing on the hot path reads or
writes a checksum (design: `docs/autumn_integrity_plan.md`; the node side is
the stream crate's `extent_node/scrub.rs`).

`plan_scrub` (pure) turns the scope into one `ScrubTask` per LIT copy of every
SEALED, non-empty extent with no op in flight, NAMING the file and its length:
`.dat` at `sealed_length` for a replica, `.shard{i}` for slot `i` at
`ceil(sealed_length / K)` (`shard_len`, which must equal the stream crate's
`erasure::shard_size` — this crate does not link it) for a converted extent.
Left out and counted in the op's message: open/empty extents, an op in flight,
pre-CoW EC layouts (shards in `.dat`), dark slots, and copies on nodes that are
not Online. `dispatch_scrub` groups tasks by the EN shard that owns each extent
(`shard_addr_for_extent`) and sends `MSG_SCRUB_EXTENTS` in chunks of 4096; a
failed send records those files as FAILED at once.

The op is per-file accounting in `scrub_ops` (leader-local, like the ledger):
`record_scrub_outcome` takes each `DfResp.scrub_done` once — keyed by (extent,
node, file) — updates progress, and finishes the op when nothing is pending:
SUCCEEDED whatever was found (a ROT outcome is first handed to
`isolate_rotted_slot`; one it answers `Stale` is held for a re-check rather
than counted) and FAILED only if a file could not be checked.
Outcomes are at-most-once and a node's queue is in memory, so each `df` also
carries `scrub_queued` (`record_scrub_queued`): an op listed there is alive
however long its files wait; an op with files pending on a node that answers
WITHOUT listing it, past `SCRUB_QUEUE_GRACE_SECS` (30 s) after dispatch, has
lost them (restart, lost report) and they are FAILED at once.
`sweep_silent_scrub_ops` (policy tick) ends an op nothing has been heard about
for `SCRUB_OP_SILENCE_SECS` (2 h — every node holding its files stopped
answering) as UNKNOWN; a RUNNING op would attach-dedup every later submit of
its scope into a no-op. A failover ends running scrubs the same way; nodes keep
scrubbing and their outcomes are dropped from the accounting, but a ROT one is
still acted on (it cannot be held for a re-check without its op).

The `scrub` auto-policy switch (switch 8; on in `maintenance`, `balanced`,
`aggressive`) submits `scrub --all` through the ledger at most once per
`SCRUB_POLICY_INTERVAL_SEC` (7 days): `decide_actions` floors that kind's
cooldown at the interval whatever the policy says, and cooldowns are persisted.
`scrub_candidates` emits its one advisory row only when the week is up and no
scrub is running.

## Web dashboard (standalone app)

The manager **no longer serves a web UI** — the old in-manager `dashboard.rs`
(axum over `cyper_axum::serve` + `include_str!` HTML) is gone. The dashboard is
now a standalone app, `crates/server/src/bin/autumn_dashboard` (the `autumn-dashboard` binary), which
holds no cluster state and drives the cluster ONLY through `autumn-op` — so the
wire schema stays in exactly one place. It requires `--cluster-secret-file` (forwarded to autumn-op, which connects as an operator); the dashboard HTTP port itself has no authentication.

What survives in this crate is `dashboard_compose.rs`: the pure `/api/overview`
composer (df + nodes + partitions + ps_servers + amplification + advisories),
shared with `autumn-op overview` so the app renders the same view the manager
used to serve. Manual actions map to the allow-listed `autumn-op` subcommands
above.

Two of its fields exist because a ROLL-UP CANNOT ANSWER THE QUESTION THEY ANSWER:
- **`nodes[].disks`** — per-disk `disk_id/uuid/total/free/extent_bytes/reported/
  online/faulted`, filtered to disks the registry assigns to that node (the same
  ownership filter `apply_df_disk_health` applies — a stale `{dir}/disk_id` sentinel
  must not put a fault on the wrong machine). The node already sends these on every
  `df`; `node_health_loop` keeps them on `NodeCap` instead of only summing. A node with
  disks `[empty, full, full, full]` rolls up as half-free. Three states: online;
  faulted (the node's own verdict, via `faulted_disks` — the bit recovery keys on); and
  `reported: false` — the node ANSWERED df but omitted a disk the registry assigns to
  it (usually a dropped `--data` dir). A node that did NOT answer df gets no rows at
  all: "unreachable" and "disk missing" call for different actions.
- **`ps_servers`** — every PS MEMBER plus the live registry (see "PS
  membership"), with `ps_last_heartbeat`; an evicted member carries
  `evicted_at_ms`.
  A list derived from the partitions can only show a PS that owns something, so
  the two states most worth seeing (serving nothing; stopped heartbeating) are
  exactly the two it cannot express. `last_heartbeat_secs_ago = u64::MAX` (JSON
  `null`) means no heartbeat entry: an evicted member. The flip side of the replay seed: right after a leader
  change every replayed PS reads as freshly heard from until the 10 s eviction window
  judges it. Each row also carries `slot_cap` (JSON `null` = no `--cpuset`, or
  not heard from since this manager became leader); `autumn-op info` prints
  `used/cap slots` per PS and flags a PS past its cap. `open_count` and `ready`
  (see "PS liveness") say whether it serves everything assigned to it; the page
  lists a heartbeating PS that is not ready as "not serving every partition
  assigned".

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

**Sealed-empty member sweep** (`sealed_empty_sweep_loop`, leader-only, 60 s, 64
extents/tick). The other leak shape: an extent that IS a stream member, at
`sealed == true && sealed_length == 0`, refs ≥ 1, referenced by no VP/SST/
checkpoint. Nothing else looks at it — GC keys on discard bytes, truncate on the
head extent, the both-zero sweep on non-membership — which is why the live 5-node
incident leaked 10.4 TB against 222 GB logical. The writer punches its own
abandoned tail on roll-away (`reclaim_abandoned_empty_tail`), but that is
client-side and best-effort; this is the backstop for a failed punch, a writer
that died between seal and punch, and the backlog on a cluster poisoned before
that fix. **Candidate gate** (`sealed_empty_sweep_candidates`, pure + unit-tested):
NOT the stream's tail, `sealed && sealed_length == 0`, and not named by the
recovery/EC inflight ledger. It then REUSES the punch-holes mutation
(`compute_extent_ref_drops` → value-CAS'd `mirror_stream_extent_mutation` →
`enqueue_pending_deletes`), so it is a new way to CHOOSE extents, not a second way
to remove them.

INVARIANTS, each with a reason it is not optional:
- **The tail is never swept.** It is the live append target, the writer's own
  reclaim owns a tail sealed at 0, and skipping it is also what keeps the stream
  non-empty — which the membership mutation refuses.
- **Only an authoritative empty seal.** An OPEN extent also reports
  `sealed_length == 0` while holding data; `sealed` is the bit that distinguishes
  them, and it is immutable once set.
- **The inflight ledger is re-read per plan**, not taken from a snapshot before
  the loop. Recovery deliberately targets sealed-empty extents, and by the second
  plan a pre-loop snapshot is separated from the mutation by the previous
  iteration's awaits. `handle_stream_punch_holes` has no such gap (it snapshots
  and refuses inside one synchronous borrow); this sweep must re-read to match.
- **A stream listing an extent twice is refused and ERROR-logged**, never swept:
  `retain` drops every occurrence while the refs mutation decrements once, which
  would manufacture the `refs > 0, in no stream` orphan the both-zero sweep
  refuses to reap.

Scope worth knowing: PS GC already punches `sealed_length == 0` unconditionally
ahead of the replay floor, so the predicate is not novel — but GC walks one
partition's log_stream, and this walks EVERY stream, including row/meta streams
and partitions whose PS is gone. The safety argument is caller-ack ⊆ commit +
seal ≥ acked ⇒ a sealed-at-zero extent has no acked byte. That is a claim about
the absence of an under-seal bug class, not a structural property; if one recurs,
this turns a loud wedge with bytes still on disk into an unlink within a minute.

## Cluster identity, version, and capacity

**`cluster_id`** (`autumn-rs/cluster_id`): CAS-imprinted to a UUID by the first leader
(`imprint_cluster_id`); memory-mode keeps a per-process UUID. `MSG_GET_CLUSTER_ID`
(no leader gate — followers answer from replay). `autumn-op format` is the single
per-EN entry point (fetches cluster_id, allocates a `disk_uuid` per dir, registers,
writes sentinel files; idempotent; mismatched cluster_id → refuse). The EN verifies
cluster_id twice at startup (each `--data` dir agrees; one round-trip to the manager)
before the listener binds.

The persisted cluster_version latch, startup/replay checks, query/bump RPCs and
operator commands are removed. Legacy etcd keys are ignored; 0x4A/0x4B remain
reserved, and the frozen identity response's historical field is always zero.

Upgrade procedure is in docs/ops.md: pause policy, wait for dispatched and local
background work, drain, replace, verify recovery, then restore policy. The first
VERSION_HELLO deployment uses stopworld. Later wire-changing releases may roll
with temporary cross-wire failures. Existing ACK durability, fencing and recovery
rules remain necessary; Hello alone is not a data-safety proof.

Persisted formats change rarely. Analyze each actual change separately and
supply its required conversion/recovery procedure. Do not rely on rkyv validation
or a global wire number to prove persisted compatibility. One-time migration
code is removed after its migration; old SST/FS converters have been deleted.

**`cluster_df`** (`MSG_CLUSTER_DF`, leader-gated). Ceph-style aggregate, in-memory only,
built inside the single `node_health_loop`: RAW + `physical_used` are summed from each
EN's self-reported `DiskStatus.extent_bytes` every tick (owner reports, control plane
sums — no manager-side counters); `logical_stored` is a periodic (~30 s) read-only scan
of `s.extents` (`Σ distinct sealed_length` skipping both-zero). The wire carries only
raw u64 facts; raw-used/logical-size amplification and the EC-dependent writable range
are computed by the consumer. `physical_used` remains a diagnostic sum of extent file
lengths, not the amplification numerator.

**Overview / df open-tail rules.** `compute_cluster_overview_resp`'s per-partition
`live_size` = `Σ distinct extents' sealed_length` (manager-authoritative) **plus** the
latest PS-reported `open_tail_bytes` — an OPEN extent's manager `sealed_length` is 0, so
a log-heavy / major-compacted partition whose data lives in open tails would otherwise
render 0 B. **Invariant: never re-introduce a sealed-length-only `live_size`.** For
cluster-df, `ClusterCapSnapshot.logical_open_tail` companions `logical_stored`, and the
amplification is `(raw_total - raw_free) / (logical_stored + logical_open_tail)`.
The numerator is statvfs capacity consumed; the denominator is distinct sealed extent
size plus committed open extent size. (Overview double-counts CoW-shared extents across siblings; df open tails are
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

**Lease modes beyond READ/WRITE (wire 48).** The S3 gateway needs three more,
and the fuse mount and Python binding meet them as ordinary conflicts:

| Mode | Used for | Refused while ANOTHER client holds |
|---|---|---|
| READ | an open read fd | EXCLUSIVE |
| STABLE | an S3 GET pinning the content | WRITE, EXCLUSIVE |
| WRITE | an in-place writer | WRITE, REPLACE, EXCLUSIVE, STABLE |
| REPLACE | an S3 overwrite swapping the inode out of its name | WRITE, REPLACE, EXCLUSIVE |
| EXCLUSIVE | reclaiming the inode's data | anything |

A client never excludes itself. WRITE, REPLACE and EXCLUSIVE share the single
writer slot (`InodeLeaseState.writer_kind`); only a plain WRITE may use the
force-preempt path, only against another plain WRITE (a REPLACE or EXCLUSIVE
holder is never deposed mid-swap or mid-reclaim —
`force_write_never_deposes_replace_or_exclusive`), and a forced WRITE still
yields to a stable reader. STABLE
entries live beside `readers`, memory-only with the same TTL and heartbeat.
A forced WRITE is refused outright (`HolderConflict`) while a stable reader
exists, before any grace window opens.

**What a manager failover loses, stated because the modes promise exclusion.**
Only WRITE is persisted (`inode_leases/<ino>`); REPLACE and EXCLUSIVE are
memory-only like readers. They last one name swap or one reclaim, and persisting
them cost two etcd writes on every S3 overwrite and every unlink. A slot held as
WRITE that its owner re-acquires as EXCLUSIVE keeps its WRITE record, so a
replay brings it back as WRITE. After a failover an in-progress EXCLUSIVE is
gone: a READ can be granted mid-reclaim. The reclaimer re-acquires EXCLUSIVE
(same client, so every other holder is re-checked) between its delete phases,
which narrows but does not close that window; it only matters for an
UNREACHABLE inode, which a client can open only through a stale cached handle.
A lost REPLACE lets an in-place writer open the old inode mid-swap; its writes
land in an inode that is about to become unreachable. STABLE entries are
memory-only and vanish on failover: a WRITE can then be granted while a GET is
still streaming. The gateway treats a heartbeat `NotHeld` on a stable lease as
"abort the response"; a GET shorter than one heartbeat interval cannot notice.
Closing either gap needs the kind persisted or a post-failover write grace.

Stable readers receive no invalidation pushes: the only foreign writer they can
coexist with is REPLACE, which does not change their content. A same-client
WRITE → EXCLUSIVE upgrade keeps the epoch, so a client must drain its own writes
before reclaiming. Matrix: `mode_matrix_across_clients`; wire path:
`crates/manager/tests/lease_modes.rs`.

Consumer-side coherence rules (design rationale for the fuse consumer): a subscribe
disconnect / overflow sentinel must drop EVERY held lease + cached fd (partial
invalidation is a footgun); a cache-stale Read must reload extents before serving or
return EIO (never serve pre-close bytes). See `docs/autumn_fs_lease_plan.md`.

## Namespace registry

Etcd string-keyed registry `namespace/<name>` → `persist::NamespaceRecord {name, prefix,
owner_tenant, presplit, created_at}` (modelled 1:1 on the `tenantAccount/` DB):
in-mem shadow, fail-loud replay, Admin-connection-only create/delete (`MSG_NAMESPACE_CREATE`
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
create_revision==0 pattern; no in-memory cache (failover needs no replay hook). Grants
are **queued in the manager** (`fs_alloc_turn`, an async mutex held around the CAS loop):
only the leader writes the counter, so without the queue the conflicts were the leader's
own concurrent requests, and a burst of 64 allocators — eight 8-worker S3 gateways
writing for the first time — exhausted the 16 CAS attempts
(`fs_alloc_inodes::a_burst_of_allocators_is_all_granted`, red without the queue). The CAS
remains for a deposed leader still writing. A grant is two etcd round trips (get, txn) per
~1000 inodes, so the queue caps grants at roughly one per two etcd RTTs (not measured).
During an etcd outage queued grants fail one after another rather than together.
Migration
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

## Runtime dependency

Compio 0.19.2 and cyper-axum 0.9 are upgraded together so management HTTP and
RPC tasks share one runtime family. Rust >=1.95 is required. The etcd h2c adapter
uses cyper-core 0.9; manager scheduling and protocol/storage formats are unchanged.

## Outbound connection lifetime

`ConnPool` (manager → extent node: df, recovery, EC-conversion dispatch,
deletes) holds one multiplexed `autumn_rpc::client::RpcClient` per address.
It keeps a connection after a peer status response, evicts after I/O, frame,
closed-connection or timeout failures, and replaces a cached client whose
`is_closed()` is true — which now includes a peer the rpc keepalive judged
silent (see autumn-rpc CLAUDE.md "Dead-peer detection"). src/connection_tests.rs
counts actual accepts and checks both ordinary and timed calls.

This pool used to be a hand-rolled sequential connection with its own frame
loop. No keepalive reached it, so a connection to a node that had stopped
answering was only ever found by each caller's own timeout, one call at a
time. It also handed one `&mut RpcConn` to every task through a raw pointer:
two tasks calling the same node concurrently read each other's replies off one
socket. Do not reintroduce a private connection type here.

Row-reclamation regressions: `system_row_truncate_live_refs` checks empty-major
prefix cleanup and repeated single-SST majors after reopen, then reopens again
and reads every key. `system_row_truncate_queued_flush` expects the queued
rotation floor and the major's fresh tail to coexist until the flush commits.
