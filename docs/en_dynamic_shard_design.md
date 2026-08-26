# EN Dynamic Shard Count + Kubernetes-Native Identity

An extent node's (EN) shard count changes without a re-format, and EN identity is
decoupled from its network location — so a Kubernetes pod reschedule (new pod IP)
or a reshard (new port set) needs no per-pod ClusterIP Service and no manifest
surgery.

---

## 1. Why a reshard moves zero bytes

- **Files never move.** Every shard of a node opens the SAME data dirs (each
  shard thread builds `ExtentNodeConfig::new_multi(data_dirs)` with the full dir
  list; one `DiskFS` per shard per dir), and the on-disk layout is
  shard-independent: `{data_dir}/{crc32c(extent_id_le) & 0xFF:02x}/extent-{id}.dat`
  + `.meta` (stream CLAUDE.md, "Data Model"). The shard index appears nowhere on
  disk.
- **Ownership is a load-time filter, not a placement.** `load_extents`
  (`crates/stream/src/extent_node.rs`) scans every file on every dir and skips
  ids this shard does not own; the orphan reconcile filters by `owns_extent` the
  same way.
- **The extent→shard map is a pure function of `(extent_id, shard_count)`:**
  `autumn_rpc::shard_for_extent` (`crates/rpc/src/lib.rs`) = `splitmix64(extent_id)
  % shard_count`, with `shard_count <= 1` short-circuiting to 0. It is a hash, NOT
  `extent_id % shard_count` (plain modulo aliased bootstrap's contiguous ids onto
  shard 0). **INVARIANT: three call sites must compute the identical map** — the
  EN's `owns_extent`, the manager's `shard_addr_for_extent`
  (`crates/manager/src/lib.rs`), and the StreamClient's `shard_addr_for_extent`
  (`crates/stream/src/conn_pool.rs`). All three delegate to `shard_for_extent`.
- **Per-extent state is shard-agnostic.** `.meta` carries
  eversion/sealed/owner_epoch keyed by extent_id; manager state
  (`MgrExtentInfo.replicates`) is keyed by node_id. Nothing persists a shard
  index.

A shard-count change is therefore purely: (a) each shard picks up the extents it
now owns at load, and (b) the manager and clients re-route via
`shard_addr_for_extent` with the node's new `shard_ports`. Zero bytes move.

---

## 2. Identity / registration model

### 2.1 `node_uuid` is the identity; location is registered at startup

The split mirrors the PS: identity supplied out of band (`--psid`), location
self-registered at runtime (`MSG_REGISTER_PS` + per-partition
`MSG_REGISTER_PARTITION_ADDR`, self-healed each `sync_regions_once` tick).

- **`node_uuid`**: UUID v4, one per EN node, stored as a `node_uuid` sentinel
  file in every data dir, alongside the existing `cluster_id` / `disk_uuid` /
  `node_id` / `disk_id` sentinels written by `cmd_format`.
- `autumn-op format` mints it (or reuses an existing one) and stamps every dir
  atomically **before** registering.
- The EN reads it at startup via `read_node_identity`
  (`crates/server/src/bin/extent_node.rs`) — **fail-loud, never mints**: every
  dir must carry it and all must agree, else the process refuses to start (the
  same fail-stop shape as the cluster_id check).
- Identity is deliberately NOT derived from the disk_uuid set: disks get added,
  replaced and dropped over a node's life, and identity must not change when the
  disk set does. `disk_uuids` stay a cross-check, not the key.

### 2.2 `autumn-op format` is identity-only

`cmd_format` (`crates/server/src/bin/autumn_op/main.rs`):

1. Fetch cluster_id.
2. Allocate/reuse the per-dir `disk_uuid` and the node's `node_uuid`.
3. `MSG_REGISTER_NODE` with `node_uuid` + `disk_uuids` and **empty**
   `addr` / `shard_ports` / `control_address`. Empty `addr` = identity-only
   registration: the manager allocates `node_id` + `disk_id`s and persists an
   *unlocated* node.
4. Stamp the sentinels from the returned ids.

`--listen` / `--advertise` / `--shard-ports` on `format` are hard-error migration
stubs (`crates/server/src/bin/autumn_op/args.rs`). The transport-conditional
control_address (UCX registers an empty control_address so df falls back to the
data addr) lives in the EN's `build_register_req`, which knows its own transport.

Consequence: a formatted-but-never-booted node has no location, so df cannot
reach it, so it stays `Suspend` and is never selected for allocation.

### 2.3 EN startup self-registration

`--advertise <HOST:PORT>` is **required** whenever `--manager` is given
(validated in `main()`, covering both the multi-shard and `run_single_shard`
paths); `--manager`-less offline/test runs are exempt. HOST must be an IP — the
binary stays DNS-free, the shell resolves names.

- The advertise PORT is shard 0's port **as seen by peers**. It normally equals
  `--port`; a difference is accepted with a WARN (NAT/proxy case) and the
  REGISTERED ports derive from the advertise port: `advertise_port + i*stride`
  (stride default 10) for data, `advertise_port + 1000 + i*stride` for control.
  The BIND ports still derive from `--port`, so the two differ behind a proxy.
- Registration runs where `verify_manager_cluster_id` runs: on shard 0's compio
  runtime, before the shards serve. `read_node_identity` → the pure
  `build_register_req` → `register_with_manager`.
- `register_with_manager` retries a mid-election manager 30 × 1 s. A hard refusal
  (fenced/decommissioned uuid, disk mismatch, cluster_id mismatch) or exhausted
  retries is **fail-stop**: an EN the manager cannot route to must not serve.
- Registration precedes the accept loops. A later shard bind failure exits the
  whole process (existing fail-stop), the manager's df then fails and the node
  goes Suspected; a restart re-registers. No half-registered steady state is
  reachable.

### 2.4 Manager reconciliation — `handle_register_node`

`crates/manager/src/rpc_handlers.rs`. Identity is resolved BEFORE any decision, by
scanning `s.nodes` for the uuid (no separate index — the uuid is in-struct on
`MgrNodeInfo`). Precedence:

1. **uuid match** → this IS that node. Location (`address`, `shard_ports`,
   `control_address`) updates together, etcd-first via `mirror_register_node`
   under the leader fence, but ONLY when `req.addr` is non-empty. A `shard_ports`
   change here **is the reshard commit point** (§4, step R5).
2. **legacy address adopt** — a new uuid whose `req.addr` matches a UUID-LESS
   node: that node records `req.node_uuid`.
3. **identity-only registration** (`req.addr` empty): a known uuid returns the
   existing node_id + disk map and **preserves ALL live location** — empty
   ports/ctrl mean "unspecified", NOT "clear them", or shards 1..N would
   black-hole; an unknown uuid creates an unlocated node.
4. **address conflict** — `req.addr` matches a live node under a DIFFERENT
   non-empty uuid → refuse `CODE_PRECONDITION`. Two node records must never share
   one address: that makes one physical EN two failure domains (RF
   double-placement). Recycling an address is legitimate only AFTER the old node
   is fenced + removed (gone from `s.nodes` → the address is free → create).
5. **create** → allocate a new node_id + disks; the uuid persists in the
   `nodes/<id>` record.

**The zombie/decommission defense is uuid-keyed.** Before resolution, a non-empty
`req.node_uuid` is checked against the `decommissioned/` (`DECOMMISSIONED_PREFIX`)
and Fenced `node_overrides` tombstones **by uuid**, and a match is refused. This is
load-bearing because `remove_node` deletes `nodes/<id>`: a matched-node_id check
alone would miss a fully removed node, so the tombstone itself carries
`node_uuid` (§2.5). The matched-node_id Fenced check is kept for uuid-less legacy
registrants. `autumn-op unfence <id>` (`handle_clear_node_override`) lifts BOTH
the `node_override/` and the `decommissioned/` key, so the refusal has a remedy.
Keying on the uuid makes the tombstone travel with the node, not the IP.

Response: `RegisterNodeResp` (node_id + disk map).

### 2.5 Wire / persisted schema

All in `crates/rpc/src/manager_rpc.rs` and `crates/rpc/src/extent_rpc.rs`:

- `RegisterNodeReq.node_uuid: String` (empty = legacy caller).
- `MgrNodeInfo.node_uuid: String` — **in-struct**. `MgrNodeInfo` is BOTH wire and
  persisted (`kv_entry("nodes", node_id, node)`), so identity rides inside the
  persisted node record: `mirror_register_node` writes it as part of `nodes/<id>`
  in the existing fenced txn and `replay_from_etcd` decodes it for free. No
  sidecar prefix, no in-memory index.
- `MgrNodeOverride.node_uuid: String` — the fence/decommission tombstone.
  Load-bearing: after `remove_node` deletes `nodes/<id>`, the node_id→uuid mapping
  survives ONLY here.
- `DfResp` / `ExtDfResp` carry the echo fields `node_uuid`, `advertise_addr`,
  `shard_ports` (§2.6). `stream::NodeRegistration` +
  `ExtentNodeConfig::with_registration` thread the EN's own identity into shard
  0's `ExtentNode` (the only shard the manager dials for df); `handle_df` echoes
  them.
- `NodesInfoResp` serves `MgrNodeInfo` directly, so `autumn-op list-nodes` reads
  the UUID and SHARDS columns straight off the persisted struct.
- An empty `shard_ports` means "route to `address`" (legacy single-shard).
  New registrations always list their ports explicitly; `shard_addr_for_extent`
  on a 1-element vector routes identically to the empty-vector fallback.

**Operational rule:** any edit to those schema files changes `WIRE_FINGERPRINT`
and must be recorded in `WIRE_VERSION_FINGERPRINTS` with `WIRE_VERSION_MIN`/`MAX`
bumped (`crates/rpc/src/lib.rs`; the `wire_version_registry_tests` enforce it).
Because `MgrNodeInfo` / `MgrNodeOverride` are persisted, changing their layout is
a stop-world upgrade that requires an etcd reset (`cluster.sh reset`); rkyv's
fail-loud decode refuses un-reset old values rather than mis-reading them, and
there is no rollback across such a change.

### 2.6 df-echo drift detection and imposter refusal

`node_health_loop` (`crates/manager/src/recovery.rs`) already RPCs every node's
`control_address` every 2 s, so the echo rides that loop — there is no separate
EN→manager heartbeat. It compares the echoed `(node_uuid, advertise_addr,
shard_ports)` against the stored `MgrNodeInfo` through the pure
`classify_df_echo(...) -> DfEchoAction`:

- **`Imposter`** — the echoed uuid ≠ the stored uuid for this node_id: a
  DIFFERENT process is answering at this address (recycled pod IP). Do NOT heal;
  mark the node's disks offline, `on_heartbeat_fail`, and treat the df as failed.
  Self-protecting, no write.
- **`DriftWarn`** — uuid matches, location drifted: **WARN only, no write.** The
  authoritative writer is the EN's own startup self-register, and
  `register_with_manager` returns Ok only on a committed `CODE_OK`, so the
  "registration txn lost" drift shape is unreachable; the residual shape is
  hand-edited etcd. An auto-heal here would re-write the loop-start `nodes`
  snapshot after an await and could resurrect a deleted node. The fix for real
  drift is the EN's next boot (or the operator).
- **`Ok`** — nothing to do.

**Why location is persisted rather than in-memory-and-re-reported:** the manager
DIALS the EN (df is the only EN-directed channel), the inverse of the PS
relationship. An in-memory-only location would leave a restarted manager with no
address to dial and no periodic EN traffic to re-report on — unbounded blackout
until the EN process restarts. Persistence costs one fenced etcd txn per EN
**boot**, on the already-existing `mirror_register_node` path, and
`replay_from_etcd` restores routing for recovery dispatch / EC / delete
immediately after leader promotion.

---

## 3. Kubernetes topology

### 3.1 Pod-IP routing, no per-pod Services

The EN self-registers its **pod IP** + actual ports at startup, so:

- The manager and PS dial the registered `pod_ip:shard_port` directly. Pod IPs are
  routable cluster-wide under every standard CNI; no Service sits in the data or
  control path.
- A pod reschedule gives a new pod IP; the EN's startup registration updates the
  same `node_uuid`'s location and routing follows.
- There are no `autumn-en-<ordinal>` ClusterIP Services and no hand-maintained
  shard-port lists. Scaling EN replicas = bump `replicas` (+ `AUTUMN_EXPECT_NODES`
  for the bootstrap guard).
- Changing shard count = edit `AUTUMN_EXTENT_SHARDS` + the reshard runbook (§4);
  no Service patching, because there is no Service port list.

### 3.2 Manifest shape (`deploy/k8s/extent-node.yaml`)

- **StatefulSet** — kept for **storage identity**, not network identity:
  `volumeClaimTemplates` pin each ordinal to its PVC, and the PVC carries the
  `node_uuid` sentinel = the identity anchor. A Deployment would let a rescheduled
  pod grab a different PVC, i.e. a different identity.
- **Headless Service `autumn-en`** — purely the StatefulSet's `serviceName`; its
  ports section is vestigial under pod-IP routing.
- `env` carries the Downward-API pod IP:
  ```yaml
  - name: AUTUMN_ADVERTISE_IP
    valueFrom: { fieldRef: { fieldPath: status.podIP } }
  ```
  and `deploy/docker/entrypoint.sh` (`run_extent_node`) builds
  `--advertise ${AUTUMN_ADVERTISE_IP}:${AUTUMN_EXTENT_PORT}` and maps
  `AUTUMN_EXTENT_SHARDS` → `--cpuset 0-$((N-1))`. The `format` call carries no
  location. env→flag translation stays in the shell.
- `containerPort` entries are informational under pod-IP routing; the manifest
  lists the shard-0 data + control ports for humans.
- `readinessProbe: tcpSocket 9101` — the process is fail-stop on any shard bind
  failure, so shard 0 answering implies all shards bound; registration precedes
  serving, so Ready also implies registered.
- A pod restart with a new cpuset re-registers like any boot. **k8s caveat:**
  in-container `cpuset_len` derives from the cgroup cpuset, and
  `AUTUMN_EXTENT_SHARDS` explicitly sizes it via `--cpuset`, so the env is
  authoritative — resizing `resources.requests.cpu` alone does NOT reshard.

### 3.3 vke overlay

`deploy/overlays/vke/` carries no per-pod EN Services; the EN section of
`deploy.sh` patches `replicas` + `AUTUMN_EXPECT_NODES` (+ optional
`AUTUMN_EXTENT_SHARDS`). Validate clusterless with `bash deploy/validate.sh`.

### 3.4 Baremetal

`deploy/baremetal/autumn-deploy` passes `--advertise` (it knows per-host IPs from
`topology.conf`), so registration is automatic. It still **refuses a multi-core
`AUTUMN_EXTENT_CPUSET`**: the registration blocker is gone, but multi-shard
per-shard data/control port allocation is unvalidated on this path — use
`cluster.sh` for multi-shard until it is.

---

## 4. Reshard runbook — stop-the-world for the node being resharded

Stop-world is the mechanism, not a compromise: the only hard problems in
resharding are (i) an open tail mid-append changing owners while a writer holds a
lease cursor, and (ii) a window in which two routers disagree. A stop dissolves
both by construction, and since there is no data movement to overlap with
serving, a live drain + dual-routing window would buy nothing.

Actors: **operator** (runbook in `docs/ops.md`), **EN** (self-registers),
**manager** (reconciles; keeps running throughout).

### R0 — preconditions
- Cluster healthy: `autumn-op info`, `autumn-op extent-health --all` clean; no
  urgent recovery in flight (`autumn-op recovery-stats`) — noise reduction, not a
  correctness requirement (markers are extent/node-keyed; routing recomputes per
  dispatch).
- The new cpuset for the EN host prepared (`topology.conf` /
  `AUTUMN_EXTENT_SHARDS`).

### R1 — quiesce writers: stop the PS processes
`systemctl stop autumn-ps@*` / `kubectl scale sts autumn-ps --replicas=0`. This is
load-bearing, not hygiene: a StreamClient's `nodes_cache` only refetches when a
node id is MISSING, so a live PS keeps the pre-reshard `shard_ports` for a node it
already knows and there is no invalidation channel for a changed port vector.
Stopping loses nothing — the graceful drain flushes imm, and anything un-flushed
rides log_stream (the WAL) and replays on reopen.

### R2 — stop the EN(s)
`systemctl stop autumn-extent-node@*`. On k8s, R2+R3+R4 collapse into
`kubectl apply` + `kubectl rollout restart sts/autumn-en`: one pod = one EN node =
all its shards, so "all shards of a node restart together" is structural.

### R3 — change the shard count
Edit `--cpuset` / `AUTUMN_EXTENT_SHARDS`. Per-node values may differ — a node is
fully-N or fully-M, never mixed (§5.1), and heterogeneous counts across nodes are
legal (routing reads each node's own `shard_ports`).

### R4 — start the EN(s)
Each EN verifies its sentinels, derives `shard_count = cpuset_len`, binds
`port + i*stride` data + control listeners, and **self-registers** (uuid,
advertise, new `shard_ports`, control_address) with the 30 × 1 s retry. Each
shard's `load_extents` picks up exactly the extents it owns under the new count;
files are found by the hashed layout; open fds are re-established (the fd LRU
bounds them).

### R5 — manager reconciles (automatic; the commit point)
`handle_register_node` uuid-matches the node and updates
`address` / `shard_ports` / `control_address` in **one leader-fenced etcd txn**,
etcd-first. From that txn on, every `shard_addr_for_extent` for this node computes
`shard_for_extent(id, new_count)`. Registration IS the reconciliation; there is no
separate step to run.

### R6 — verify
`autumn-op list-nodes`: the node shows the expected SHARDS count and UUID, and
flips `Suspend/Suspected → Online` within ~2-4 s (df ticks). Optionally
`autumn-op extent-health --all`.

### R7 — restart the PSes
Each PS re-registers (`register_ps`), re-opens partitions with fresh
`StreamClient`s (fresh nodes/extent caches → new routing) and re-acquires
per-partition owner epochs; bump-on-acquire hands out epochs above anything
pre-stop, so even a hypothetical stale writer is fenced at the ENs.

### R8 — post-checks
Write-liveness probe, read a few known keys, watch `autumn_en_*` metrics.
`scripts/reshard_chaos.sh` exercises the whole 2→4→1 sequence with a
byte-exact corpus readback.

### Crash-safety of the ordering
Every step is idempotent and each EN's registration is atomic (one fenced txn).
Interrupt anywhere and the reachable states are: some nodes registered at new
counts, some at old, some down — all legal (§5.2), all healed by resuming the
runbook or just restarting the stragglers. A manager crash mid-reshard: replay
restores the last-committed locations, and ENs still in their retry loop register
against the new leader. Nothing requires cluster-wide atomicity, because
ownership is a per-node property.

---

## 5. Safety argument

Under stop-world + zero movement, the only failure class is a
**routing/ownership disagreement**. The agreement pairs:

### 5.1 Shard ↔ shard within one node
All shards derive from one `parse_args` + one `cpuset_len` in one process,
threaded as `with_shard(idx, count, siblings)`. A node is fully-N or fully-M by
construction — the count is a per-node boot property. `sibling_addrs`
(control-plane forwarding) come from the same vector, so they stay internally
consistent.

### 5.2 Manager ↔ node
The manager's `shard_ports` for node X changes ONLY via X's own registration,
carrying the ports X actually derived and will bind, applied etcd-first in one
fenced txn. Timeline: X stops (old count) → the manager still routes old ports →
connection refused / df fails → callers retry (recovery backoff, GC cooldown, PS
append retry), all existing down-node behavior. X registers (new count) → routing
flips atomically with the txn. X crashes between register and serve → node down,
healed on restart. **There is no reachable state where the manager routes
new-count to a node running old-count**, because the registration payload is
generated by the running process itself, not declared separately by an operator.
The df echo (§2.6) detects exotic drift within 2 s.

Heterogeneous counts across nodes are safe for the same reason mixed
`shard_ports` lengths already are: routing takes each node's OWN vector.

### 5.3 Client (PS / StreamClient) ↔ manager
The runbook order (PS down before the EN changes, up after) means every PS boots
with fresh caches against post-reshard state. Defense-in-depth if the runbook is
violated: a stale client routes to an old-map port and gets either
connection-refused (shrink) or the wrong live shard (grow), where the server-side
`owns_extent` check **rejects loudly** ("extent N belongs to shard X not shard Y")
— never a silent wrong-shard write. **INVARIANT: never remove or soften the
EN-side `owns_extent` rejection on data-plane ops; it is the backstop that makes
every routing bug loud instead of corrupting.** Control RPCs sibling-forward
instead, which is also count-consistent within the process.

### 5.4 Fencing is untouched
`owner_epoch` is per-extent, persisted in `.meta`, loaded by `load_extents`, and
enforced on every append regardless of which shard owns the extent. A reshard
changes *which shard* enforces it, not the value. A PS restart re-acquires
per-partition epochs above all prior values (bump-on-acquire), so post-reshard
writers dominate. Seal/commit semantics (all-replica ack, lenient seal) are keyed
by extent + node, not shard.

### 5.5 Manager crash mid-reshard
Registration is idempotent; etcd replay restores committed state; the uuid and the
node row live in one kv written in one fenced txn, so identity cannot tear. A
deposed leader's registration txn loses the leader fence and the EN's retry lands
on the new leader.

### 5.6 EN restarted with an unexpected count (operator error)
The node is fully-M while the operator intended N: legal and data-safe (files
unmoved; load filter and routing agree at M). The cost is only
parallelism/uniformity. Detectable via `list-nodes` (R6), and via the df-echo WARN
if etcd and the process ever disagree.

### 5.7 What cannot happen
- **Lost extents** — ownership is a total function over ids found by the disk
  scan; every file maps to exactly one shard for any count. Resharding re-owns,
  it cannot orphan.
- **Double-owned extents** — two shards of one process cannot both own an id
  (`shard_for_extent` is a function), and two processes cannot both be node X
  (uuid identity + fenced registration; the df uuid echo catches an
  address-level imposter).
- **Torn identity** — `node_uuid` lives IN the `nodes/<id>` record, a single kv
  written atomically. There is no separate index to fall out of sync.

---

## 6. Why a hash function and not consistent hashing / an explicit shard map

Consistent hashing (vnodes, CRUSH) and an explicit shard map both exist to
minimize data movement when membership changes. Here movement is exactly zero —
files live in a shard-independent hashed layout on dirs shared by all shards, so a
reshard is pure re-labeling that `shard_for_extent` computes from one integer
already registered per node. Consistent hashing would add ring state, vnode maps,
and a second placement authority that can disagree with the disk scan; an explicit
shard map adds a persisted artifact to migrate, validate and repair. The single
source of truth stays `len(shard_ports)` in the node's own registration, which has
no independent failure mode.
