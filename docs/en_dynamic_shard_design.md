# EN Dynamic Shard Count + Kubernetes-Native Identity — Design

Status: DESIGN (no code yet). Author: fable subagent, 2026-07-14.
Scope: extent-node (EN) shard count becomes changeable without re-format; EN
identity is decoupled from its network location so Kubernetes pod reschedules
(new pod IP) and reshards (new port set) need no per-pod ClusterIP Services and
no manifest surgery.

Three decisions are LOCKED by the user and this design builds around them
(§0). Everything else is recommended-with-tradeoffs.

---

## 0. Locked decisions (do not re-open)

1. **Ownership stays `extent_id % shard_count` (modulo).** No consistent
   hashing, no vnodes, no explicit shard map. §7 has the
   considered-and-rejected paragraph.
2. **Reshard is STOP-THE-WORLD (全停全启).** No online transition protocol.
   §5 explains why this is the *right* call, not a compromise.
3. **Identity/location decoupling mirrors autumn's own PS registration
   model.** `autumn-op format` registers a stable UUID identity only; the EN
   self-registers its current IP + actual shard ports at startup; self-heal
   rides the existing manager `node_health_loop` df poll (no new loop).
4. **`node_uuid` is IN-STRUCT on the persisted `MgrNodeInfo`, NOT a sidecar
   `nodeUuid/` prefix** (pivot, 2026-07-14, user: "代码不用非后向兼容, 我重新
   reset 就行"). No back-compat, no migration code: this is a same-commit
   stop-world deploy that **requires an etcd reset** (`cluster.sh reset`). The
   sidecar option below (§2.5) was written to keep `MgrNodeInfo` frozen for a
   no-reset upgrade — that constraint was dropped, so the simpler in-struct
   field wins. Any text in §2.4-2.5, §5.7, §6, §8, §9 that still describes a
   `nodeUuid/` prefix / "no reset" is superseded by this decision.

---

## 1. Problem statement — why shard count is frozen today

### 1.1 The routing source is format-stamped, not runtime-reported

- The EN binary sizes itself at boot: `shard_count = cpuset_len`
  (`crates/server/src/bin/extent_node.rs:498-499`, F196 removed `--shards`),
  and derives per-shard listeners `port + i*stride` / control
  `port+1000 + i*stride` (`extent_node.rs:513-523`, stride default 10).
- But the *manager's* routing source is `MgrNodeInfo.shard_ports`
  (`crates/rpc/src/manager_rpc.rs:270-279`), stamped **once** by
  `autumn-op format` → `MSG_REGISTER_NODE`
  (`crates/server/src/bin/autumn_op/main.rs:1475`, req built at
  `main.rs:1545-1554` with the operator-supplied `--shard-ports`).
- The EN binary **never registers anything**. Its only manager contact at
  startup is the read-only cluster_id cross-check
  (`extent_node.rs:309 verify_manager_cluster_id`, called at
  `extent_node.rs:622-626`). Grep-verified: no `MSG_REGISTER_NODE` in
  `bin/extent_node.rs`.
- All manager-side routing computes the serving shard on the fly:
  `shard_addr_for_extent(base, shard_ports, extent_id) =
  host:shard_ports[extent_id % len]`
  (`crates/manager/src/lib.rs:3811-3823`; used by `recovery.rs:91/376/597/1394`,
  `extent_delete.rs:426`). The StreamClient does the same client-side
  (`crates/stream/src/conn_pool.rs:213-229`, used at `client.rs:1948`), with
  `shard_ports` learned from the manager's `NodesInfo`.

So: change `--cpuset` today and the EN binds a *different* port set than the
manager routes to. Grown count → the manager keeps routing every extent to the
old (fewer) ports, where the server-side ownership check
(`crates/stream/src/extent_node.rs:3243-3244 owns_extent`; reject sites at
`:4186/:4196/:4213/:5962`) hard-errors data-plane ops for extents it no longer
owns. Shrunk count → routed ports aren't even bound. Either way the cluster is
down until the operator re-runs `format` with matching `--shard-ports` — hence
"shard count frozen at format".

### 1.2 The identity is the address, which freezes the IP too

`handle_register_node` (`crates/manager/src/rpc_handlers.rs:857`) matches an
existing node **by address** (`rpc_handlers.rs:912-923`
`s.nodes.values().find(|n| n.address == req.addr)`):

- Same address, disjoint disk_uuids → refused (`rpc_handlers.rs:938-948`).
- Same address, changed `shard_ports`/`control_address` → updated, etcd-first
  (`rpc_handlers.rs:950-978`) — **the wire path to update ports on
  re-register already exists**; only nothing ever calls it after format.
- **New address, same disks → a brand-new phantom node** (falls through to
  the create branch, `rpc_handlers.rs:999+`). This is exactly the k8s hazard
  documented in `deploy/k8s/extent-node.yaml:1-14` and `:133-150`: pod IPs
  are ephemeral, so today each EN needs its OWN per-pod ClusterIP Service
  (`autumn-en-<ordinal>`) with every shard port enumerated, plus the
  FQDN-vs-/etc/hosts dance to avoid registering the pod IP. Scaling = hand-add
  Services + keep `AUTUMN_EXTENT_SHARDS` in lockstep with format-time
  `--shard-ports` (`deploy/docker/entrypoint.sh:149-172`,
  `docs/k8s_deploy.md:170-200`). `deploy/baremetal/autumn-deploy:100` outright
  refuses multi-shard for the same reason.
- The F211-C zombie/decommission defense is also address-keyed
  (`rpc_handlers.rs:874-906`) — under pod IP reuse this is both too strict
  (a fresh node inheriting a decommissioned pod IP is refused) and too weak
  (a decommissioned node returning on a new IP registers as a phantom).

### 1.3 Why the data layer is already reshard-ready (the enabling facts)

- **Files never move.** Every shard of a node opens the SAME data dirs (each
  shard thread builds `ExtentNodeConfig::new_multi(data_dirs)` with the full
  dir list, `extent_node.rs:571,628`; one `DiskFS` per shard per dir), and the
  on-disk layout is shard-independent:
  `{dir}/{crc32c(extent_id_le)&0xFF:02x}/extent-{id}.dat` + `.meta`
  (stream CLAUDE.md "Data Model"). Shard index appears nowhere on disk.
- **Ownership is a load-time filter, not a placement.** `load_extents` scans
  every file and skips non-owned ids (`extent_node.rs:3643-3651`) — the
  comment there already anticipates "a prior run with a different
  shard_count". The orphan reconcile likewise filters by `owns_extent`
  (`extent_node.rs:3152,3172`).
- **Per-extent state is shard-agnostic.** `.meta` carries
  eversion/sealed/owner_epoch keyed by extent_id; manager state
  (`MgrExtentInfo.replicates`) is keyed by node_id. Nothing persists a shard
  index.

Therefore a shard-count change is purely: (a) each new shard re-picks-up the
extents it now owns by the new modulo at load, and (b) the manager re-routes
via `shard_addr_for_extent` with the new `shard_ports`. Zero bytes move.

---

## 2. New identity/registration model

### 2.1 Identity: `node_uuid` (stable), location: registered at startup

Mirror of the PS pattern: PS identity is `ps_id` supplied via `--psid`,
location self-registered at runtime via `MSG_REGISTER_PS`
(`crates/partition-server/src/lib.rs:3069`) + per-partition
`MSG_REGISTER_PARTITION_ADDR` on open, self-healed each `sync_regions_once`
tick (`lib.rs:3761-3812`, F265). Conceptual ancestry: HDFS DataNode storageID
(stable) + heartbeat-reported location; Ceph OSD uuid + monitor OSDMap; Kafka
`broker.id` + dynamically registered listeners. The concrete mechanism we copy
is autumn's own.

- **`node_uuid`**: UUID v4, one per EN node, stored as a sentinel file
  `node_uuid` in every data dir (alongside the existing F214 `cluster_id`,
  `disk_uuid`, `node_id`, `disk_id` sentinels written by `cmd_format`,
  `autumn_op/main.rs:1590-1600`).
  - Written by `format` for new dirs.
  - **Generate-if-missing at EN startup** for already-formatted dirs (the
    migration path — no re-format needed): extend
    `read_and_verify_cluster_id` (`extent_node.rs:355`, called at `:477`) to
    also read `node_uuid` from every dir; all-present-and-agreeing → use it;
    all-missing → generate one, write to every dir (tmp + rename), use it;
    disagreement or partial presence → refuse loudly (same fail-stop shape as
    the cluster_id check). Dir-set membership is already cross-checked by
    `disk_uuid`, so uuid cloning across nodes is only reachable by an operator
    copying dirs wholesale — the disk_uuid cross-check in the manager (§2.4)
    catches that.
- **Why not derive identity from the disk_uuid set**: disks get added,
  replaced, or dropped over a node's life; identity must not change when the
  disk set does. Disk_uuids stay as a *cross-check*, not the key.

### 2.2 `autumn-op format` → identity-only registration

`cmd_format` (`autumn_op/main.rs:1475`) changes to:

1. Fetch cluster_id (unchanged).
2. Allocate/reuse per-dir `disk_uuid` (unchanged) + allocate/reuse
   `node_uuid` (reuse if any dir already has the sentinel; else fresh).
3. `MSG_REGISTER_NODE` with `node_uuid` + `disk_uuids`, and **empty**
   `addr` / `shard_ports` / `control_address`. Empty `addr` = identity-only
   registration; the manager allocates `node_id` + `disk_id`s and persists an
   *unlocated* node.
4. Stamp sentinels (now including `node_uuid`) using the returned ids
   (unchanged shape, `main.rs:1570-1600`).

CLI surface: **remove `--shard-ports` and `--listen`/`--advertise` from
`format`** with a migration error message (the established F196/F214-C
stub pattern — `extent_node.rs:151-166` and the `RegisterNode` pre-connect
stub in `autumn_op`). The UCX control_address special case
(`main.rs:1534-1544`: UCX registers empty control_address so df falls back to
the data addr) **moves to the EN binary** (§2.3), which knows its own
transport anyway.

Nice side effect: a formatted-but-never-booted node has no location → df
can't reach it → it stays `Suspend` (F214-B) and is never selected for
allocation. Today a formatted node is immediately selectable at a stamped
address that may not be listening yet.

### 2.3 EN startup self-registration

New EN flag: `--advertise <HOST:PORT>` (required whenever `--manager` is
given; HOST must be an IP — the binary stays DNS-free per the repo rule, the
shell resolves names). The port component is the shard-0 data port (must equal
`--port`; validate). Env→flag translation stays in the shell
(`feedback_no_env_in_rs`).

Registration runs where `verify_manager_cluster_id` runs today — shard 0's
compio runtime, before `serve_with_control` (`extent_node.rs:622-626` for the
multi-shard path; the equivalent spot inside `run_single_shard` for
`shards == 1`, `extent_node.rs:561-563`). Factor one helper
`register_with_manager(mgr, advertise, transport, node_uuid, disk_uuids,
shard_ports, stride) -> Result<()>` used by both paths:

- `shard_ports` = the exact vector the process derived and will bind
  (`extent_node.rs:514-516`); single-shard registers `vec![port]` (today
  format registers an empty vec for single-shard "legacy mode" — keep the
  manager's empty-vec fallback for legacy rows, but new registrations always
  list their ports explicitly; `shard_addr_for_extent` with a 1-element vec is
  byte-identical routing to the empty-vec fallback for a 1-shard node).
- `control_address` = `advertise_host:(control_port_base)` for TCP; empty for
  UCX (logic relocated from `cmd_format`, see §2.2).
- Retry loop: manager may be mid-election → on transport error or
  `CODE_NOT_LEADER`, retry 30 × 1 s (mirrors PS `register_ps` retry,
  `partition-server/src/lib.rs:3005-3013`, and the entrypoint's format retry).
  Exhausted retries → **fail-stop** (`process::exit(1)`): an EN the manager
  can't route to must not serve (same rationale as the multi-shard bind
  fail-stop, `extent_node.rs:585-592`).
- Ordering vs bind: register from shard 0 **before** the shards' accept loops
  serve. A later shard bind failure exits the whole process (existing
  fail-stop), the manager's df then fails → node goes `Suspected`; restart
  re-registers. No half-registered steady state is reachable.
- A refused registration (fenced/decommissioned uuid, disk mismatch,
  cluster_id mismatch) is fail-stop with the manager's message verbatim.

### 2.4 Manager: `handle_register_node` reconciliation

`crates/manager/src/handle_register_node` — match precedence (as shipped in
M0). Resolution scans `s.nodes` by uuid (no separate index — the uuid is
in-struct on `MgrNodeInfo`):

1. **uuid match** (req.node_uuid non-empty and present on some node) → this IS
   that node. Cross-check disks: `req.disk_uuids ∩ existing disks` non-empty
   required; disjoint → refuse `CODE_PRECONDITION` ("cloned identity file?").
   Then update location — but ONLY when `req.addr` is non-empty (a real
   self-registration always ships addr + live ports + ctrl). `address`,
   `shard_ports`, and `control_address` update together, etcd-first (F152) via
   `mirror_register_node` under the F149 fence. A `shard_ports` change here **is
   the reshard commit point** (§4 step R5).
2. **legacy address match** (no uuid match; `req.addr` non-empty and matches a
   UUID-LESS node) → re-register branch + uuid ADOPTION: the matched node
   records `req.node_uuid` (zero-touch migration for pre-M0 nodes, §6).
3. **identity-only registration** (`req.addr` empty): uuid known → return the
   existing node_id + disk map, and **preserve ALL live location** (empty
   ports/ctrl mean "unspecified", NOT "clear them" — a torn shard route would
   otherwise black-hole shards 1..N); uuid unknown → create with empty location.
4. **address conflict** (uuid absent from `s.nodes`; `req.addr` matches a live
   node under a DIFFERENT non-empty uuid) → refuse `CODE_PRECONDITION`. Two node
   records must never share one address: that makes one physical EN two failure
   domains (RF double-placement), and the df loop — no identity echo until M1 —
   would keep both Online from the single EN's heartbeat. A recycled pod IP is
   legitimate only AFTER the old node is fence+removed (gone from `s.nodes` →
   the address is free → falls through to create).
5. **create** (addr non-empty & free, or empty addr with a fresh uuid) →
   allocate a new node_id + disks; the uuid persists in the `nodes/<id>` record.

**Zombie/decommission defense is uuid-keyed** (F211-C): BEFORE resolution, if
`req.node_uuid` is non-empty, scan the `decommissioned/` + Fenced
`node_overrides` tombstones **by uuid** and refuse a match. This is
load-bearing because `remove_node` deletes `nodes/<id>` — a matched-node_id
check alone would miss a fully-removed node (its record is gone), so the
tombstone carries `node_uuid` (§2.5) and the check finds it regardless of
address. The matched-node_id Fenced check is kept for uuid-less legacy
registrants (their node is still present). `clear-node-override` lifts BOTH the
`node_override/` and `decommissioned/` keys so the refusal has a real remedy.
The uuid key makes the tombstone travel with the node, not the IP — a strict
improvement to the old address-keyed F211-C (which refused innocent recycled-IP
tenants and missed a zombie on a fresh IP).

Response: unchanged `RegisterNodeResp` (node_id + disk map).

### 2.5 Wire delta (WIRE v19, one bump)

All in `crates/rpc/src/manager_rpc.rs` / `extent_rpc.rs`; any edit changes
`WIRE_FINGERPRINT` and MUST be recorded in `WIRE_VERSION_FINGERPRINTS`
(`crates/rpc/src/lib.rs:58-66`; currently MIN=MAX=18 → bump to MIN=MAX=19,
pre-R3 rule; the `registry_pins_current_schema_to_max_version` test enforces
it). Same-commit stop-world deploy as usual.

- `RegisterNodeReq` += `node_uuid: String` (empty = legacy caller — but after
  this change the only callers are the new format + EN, both always send it;
  the field's empty case exists for decode of… nothing on the wire, since
  deploys are same-commit; keep empty=legacy semantics anyway for
  memory-mode tests).
- `MgrNodeInfo` += `node_uuid: String` — **IN-STRUCT** (this struct is BOTH
  wire and persisted: `kv_entry("nodes", node_id, node)`,
  `crates/manager/src/lib.rs`). The identity rides inside the persisted node
  record; `mirror_register_node` writes it as part of `nodes/<id>` in the
  existing fenced txn, and `replay_from_etcd` decodes it for free. No sidecar
  prefix, no in-memory index, no migration code. See §0 decision 4 + §6.
- `MgrNodeOverride` += `node_uuid: String` (the fence/decommission tombstone).
  Load-bearing: `remove_node` deletes `nodes/<id>`, so after removal the
  node_id→uuid mapping survives ONLY on the tombstone — the re-register zombie
  check scans tombstones by uuid so a removed node returning under its own
  identity is refused even though its `MgrNodeInfo` is gone.

**Persisted-schema decision — IN-STRUCT (pivot, §0 decision 4).** The original
draft weighed (a) in-struct vs (b) a sidecar `nodeUuid/` prefix, and recommended
(b) purely to keep `MgrNodeInfo`'s persisted layout frozen for a *no-reset*
upgrade. That constraint was DROPPED by the user ("代码不用非后向兼容, 我重新
reset 就行"), so (a) wins: the rkyv layout of `nodes/<id>` (and
`node_override/` / `decommissioned/`) changes, old etcd values no longer decode
— which is fine because the deploy is a **same-commit stop-world upgrade that
resets etcd** (`cluster.sh reset`). rkyv's fail-loud decode (manager note 39)
guards against accidentally pointing a new binary at un-reset old state. No
`MgrNodeInfoV18` copy, no rewrite-on-promotion, no rollback stamp — none of it
is needed once reset is on the table.

- `NodesInfoResp`'s node entries serve `MgrNodeInfo` directly, which now
  carries `node_uuid` — `list-nodes` reads it straight off the persisted
  struct (no wire-only wrapper needed).
- `DfResp` (`crates/rpc/src/extent_rpc.rs:629-633`) += echo fields:
  `node_uuid: String`, `advertise_addr: String`, `shard_ports: Vec<u16>`
  — the self-heal channel (§2.6). The EN needs the full port vector on every
  shard: thread it through `ExtentNodeConfig` (a new
  `with_registration(node_uuid, advertise, shard_ports)` builder), since
  today's `sibling_addrs` are bind-host-based strings, not advertise ports.

### 2.6 Continuous self-heal — riding `node_health_loop`, and the state decision

**Decision needed (per task): in-memory + re-reported (PS `part_addrs`
model) vs etcd-persisted-and-overwritten-each-boot.**

**Recommendation: etcd-persisted, overwritten on every registration** (i.e.
keep `MgrNodeInfo.address/shard_ports/control_address` exactly where they are,
just make the EN the writer instead of format), with a **df-echo drift
detector** as the self-heal:

- The PS `part_addrs` in-memory model works because the *PS dials the manager*
  every 2 s (`heartbeat_ps` + `GetRegions` in `sync_regions_once`) — after a
  manager restart the very next tick re-reports. The EN relationship is
  **inverted**: the *manager dials the EN* (df, `node_health_loop`,
  manager note 25 — the single df caller). If the EN's location were
  in-memory-only, a restarted manager would have NO address to dial and the EN
  has no periodic manager-directed traffic to re-report on — we'd have to add
  a brand-new EN→manager heartbeat loop, which the task forbids and which
  would duplicate df. The bounded ~2 s "manager-restart routing window where
  `shard_addr_for_extent` has no port until the next report" from the
  in-memory option therefore isn't 2 s at all for the EN — it's *unbounded
  until the EN process restarts*. That kills the in-memory option on its own.
- Cost of persistence: one fenced etcd txn per EN **boot** (rare), on the
  already-existing `mirror_register_node` path. Nothing per-tick.
- Failover: `replay_from_etcd` restores the last-registered location →
  recovery dispatch / EC / delete routing (`recovery.rs:91` etc.) work
  immediately after leader promotion, no blackout.

**Self-heal (no new loop):** `node_health_loop` already RPCs every node's
`control_address` every 2 s and processes the `DfResp`. Add: compare the
echoed `(node_uuid, advertise_addr, shard_ports)` against the stored
`MgrNodeInfo`; on mismatch, WARN + heal etcd-first through the same
`mirror_register_node` txn (idempotent, leader-fenced). This closes every
residual drift shape within one tick:
  - registration landed on a leader whose fenced txn lost (EN believes
    registered, etcd doesn't) → healed;
  - operator hand-edited etcd → healed;
  - uuid echo ≠ stored uuid for that node_id → this is a *different process*
    answering on a stale address (pod IP reuse!) — do NOT heal; WARN loudly
    and mark the node's health accordingly (treat the df as failed for
    liveness purposes). This check is a k8s-specific safety net the current
    system cannot express at all.
- The echo also serves as the reshard misconfiguration detector for §5.

One judgment call inside the heal: `advertise_addr` echo differing from the
stored address means the stored address *worked* (we just dialed it for df)
but the EN believes it should be reached elsewhere — heal to the EN's claim
(the EN is the owner of its own location; the old address may be a dying
ClusterIP or NAT alias). shard_ports differing likewise heals to the EN's
claim. All heals are logged at WARN with before/after.

---

## 3. Kubernetes story

### 3.1 Does the per-pod Service problem disappear? — Yes

With the EN self-registering its **pod IP** + actual ports at startup:

- The manager and PS dial the registered `pod_ip:shard_port` directly. Pod IPs
  are routable cluster-wide under every standard CNI. No Service is involved
  in the data or control path at all.
- Pod reschedule → new pod IP → the EN's startup registration updates the same
  `node_uuid`'s location → routing follows. The phantom-node trap
  (`extent-node.yaml:133-150`) and the FQDN-vs-/etc/hosts workaround are
  deleted, not worked around.
- The **N per-pod ClusterIP Services and their hand-maintained port lists are
  removed** (`extent-node.yaml:33-85`, `docs/k8s_deploy.md:157/173/193-196`,
  `deploy/overlays/vke/en-extra-services.yaml` entirely). Scaling EN replicas
  = bump `replicas` (+ `AUTUMN_EXPECT_NODES` for the bootstrap guard). Nothing
  else.
- Changing shard count = edit one env (`AUTUMN_EXTENT_SHARDS`) + the
  stop-world reshard runbook (§4) — no Service patching, because there is no
  Service port list.

### 3.2 Manifest shape

- **Keep the StatefulSet** — not for network identity (no longer needed) but
  for **storage identity**: `volumeClaimTemplates` pin each ordinal to its PVC
  (`extent-node.yaml:164-175`), and the PVC carries the `node_uuid` sentinel =
  the identity anchor. A Deployment would let a rescheduled pod grab a
  *different* PVC, i.e. a different identity — wrong.
- **Keep the headless Service** (`autumn-en`, `extent-node.yaml:16-31`) purely
  as the StatefulSet's `serviceName`; its ports section becomes vestigial
  (harmless to keep 9101 for the readiness story).
- **Delete `autumn-en-0/1/2` Services.**
- Pod spec deltas:
  - `env`: replace the `AUTUMN_ADVERTISE_NAME` FQDN block with the Downward
    API pod IP:
    ```yaml
    - name: AUTUMN_ADVERTISE_IP
      valueFrom: { fieldRef: { fieldPath: status.podIP } }
    ```
  - entrypoint (`deploy/docker/entrypoint.sh run_extent_node`): build
    `--advertise ${AUTUMN_ADVERTISE_IP}:${AUTUMN_EXTENT_PORT}`; keep
    `AUTUMN_EXTENT_SHARDS` → `--cpuset 0-$((N-1))` (already there,
    `entrypoint.sh:158-166`); **drop the `--shard-ports` arg from the format
    call** (`entrypoint.sh:166-190`) — format is identity-only now. env→flag
    stays in the shell; no env reads in Rust.
  - `containerPort` list: informational only under pod-IP routing; list the
    shard-0 data + control ports for humans.
  - readinessProbe: keep `tcpSocket: 9101` — the process is fail-stop on any
    shard bind failure (`extent_node.rs:585-592`), so shard-0 answering
    implies all shards bound. Registration precedes serve, so Ready also
    implies registered.
- **A pod restart with a new cpuset re-registers** exactly like any boot:
  entrypoint computes the cpuset from the env, the EN derives
  `shard_count = cpuset_len`, binds, registers the new port vector under its
  uuid. Note the k8s caveat: in-container `cpuset_len` derives from the cgroup
  cpuset; `AUTUMN_EXTENT_SHARDS` explicitly sizes it via `--cpuset`, so the
  env is authoritative — document that resizing `resources.requests.cpu`
  alone does NOT reshard.

### 3.3 vke overlay

`deploy/overlays/vke/deploy.sh` renders/injects at apply time; today it must
also inject per-pod Services for extra ordinals
(`en-extra-services.yaml`). Under the new model: delete
`en-extra-services.yaml` from the kustomization, and deploy.sh's EN section
reduces to patching `replicas` + `AUTUMN_EXPECT_NODES` (+ optional
`AUTUMN_EXTENT_SHARDS`). Validate clusterless with `bash deploy/validate.sh`
as usual.

### 3.4 Baremetal

`deploy/baremetal/autumn-deploy:89-100` refuses multi-shard ("multi-shard
needs --shard-ports registration — use cluster.sh"). That guard is deleted:
autumn-deploy passes `--advertise` (it already knows per-host IPs from
topology.conf) and any cpuset; registration is automatic. This closes the
baremetal/multi-shard gap as a side effect.

---

## 4. STOP-THE-WORLD reshard runbook

Why stop-world is the right call (not a compromise): the only hard problems in
resharding are (i) open tails mid-append migrating owners while a writer holds
a lease cursor, and (ii) a transition window where two routers disagree. A
full stop dissolves both *by construction* — there are no in-flight appends
and no live routers during the change — which matches the repo's standing
upgrade doctrine (stop-world restart is the primary safety guarantee,
`feedback_stopworld_restart_primary`, manager note 39). The alternative (live
per-node drain + dual-routing window + 2PC over lease cursors) buys nothing
here because there is no data movement to overlap with serving; it would be
transition-protocol complexity purchased for a maintenance action that takes
under a minute.

Actors: **operator** (or a future `autumn-deploy reshard-en` wrapper — v2
nicety; v1 is this documented runbook in `docs/ops.md`), **EN** (self-
registers), **manager** (reconciles; keeps running throughout — it is the
reconciliation point and its background loops tolerate down nodes by design).

### R0 — preconditions
- Cluster healthy: `autumn-op info`, `autumn-op extent-health --all` clean;
  no urgent recovery in flight (`autumn-op recovery-stats`) — not a
  correctness requirement (markers are extent/node-keyed, routing recomputes
  per dispatch), just noise reduction.
- Build/config prepared: the new cpuset per EN host (topology.conf /
  `AUTUMN_EXTENT_SHARDS`).

### R1 — quiesce writers: stop ALL PS processes
`systemctl stop autumn-ps@*` per host / `kubectl scale sts autumn-ps
--replicas=0`. This stops appends, GC, flush, splits. F120-C graceful drain
flushes imm; anything un-flushed is in log_stream (the WAL) and replays on
reopen — a process stop loses nothing (page cache survives kill; F178
coalescer + all-replica ack cover the rest).

### R2 — stop ALL ENs
`systemctl stop autumn-extent-node@*` / (k8s) apply the env change and let the
rollout restart them — in k8s R2+R3+R4 collapse into `kubectl apply` +
`kubectl rollout restart sts/autumn-en` (or pod deletion), because one pod =
one EN node = all its shards, so "all shards of a node restart together" is
structural.

### R3 — change the shard count
Edit `--cpuset` / `AUTUMN_EXTENT_SHARDS`. Per-node values may differ — a node
is fully-N or fully-M, never mixed (§5.2), and heterogeneous counts across
nodes are legal (routing is per-node `shard_ports`).

### R4 — start ENs
Each EN: verifies sentinels, derives `shard_count = cpuset_len`, binds
`port + i*stride` data + control listeners, **self-registers**
(uuid, advertise, new `shard_ports`, control_address) with the 30×1 s retry.
Each shard's `load_extents` picks up exactly the extents it owns under the new
modulo (`extent_node.rs:3643`); files are found by the hash layout; open fds
are re-established (F-EN-FD-LRU bounds them).

### R5 — manager reconciles (automatic; the commit point)
`handle_register_node` uuid-matches the node and updates
`address/shard_ports/control_address` in **one leader-fenced etcd txn**,
etcd-first (`rpc_handlers.rs:950-978` extended; F152/F149). From this txn on,
every `shard_addr_for_extent` call for this node computes `id % new_count`.
There is no separate "reconcile step" to run — registration IS the
reconciliation.

### R6 — verify
`autumn-op list-nodes`: every node shows the expected shard_port count and
uuid; nodes flip `Suspend/Suspected → Online` within ~2-4 s (df ticks).
Optional: `autumn-op extent-health --all`.

### R7 — restart PSes
`systemctl start` / scale the sts back up. Each PS re-registers
(`register_ps`), re-opens partitions (fresh `StreamClient`s, fresh
nodes/extent caches → new routing), re-acquires per-partition owner epochs —
bump-on-acquire (manager note 35 / F265) hands out epochs above anything
pre-stop, so even a hypothetical stale writer is fenced at the ENs.

### R8 — post-checks
Write-liveness probe (the `system_chaos` post-settle probe pattern,
`project_chaos_writeliveness_check`), read a few known keys, watch
`autumn_en_*` metrics.

### Crash-safety of the runbook ordering
Every step is idempotent and each EN's registration is atomic (one fenced
txn). Interrupt the runbook anywhere and you have: some nodes registered with
new counts, some with old, some down — all *legal* states (§5.2), all healed
by simply resuming the runbook (or even by only restarting the stragglers).
The manager crashing mid-reshard: replay restores the last-committed
locations; ENs still in their retry loop re-register against the new leader.
Nothing requires cluster-wide atomicity because ownership is a per-node
property.

---

## 5. Safety proof sketch

Under stop-world + zero-movement, the ONLY failure class is a
**routing/ownership disagreement**. Enumerate the agreement pairs:

### 5.1 Shard ↔ shard within one node
All shards derive from one `parse_args` + one `cpuset_len` in one process
(`extent_node.rs:498-516`), threaded as `with_shard(idx, count, siblings)`
(`extent_node.rs:632`). A node is fully-N or fully-M by construction — the
count is a per-node boot property. `sibling_addrs` (control-plane forwarding,
`extent_node.rs:3265-3269`) are computed from the same vector — internally
consistent.

### 5.2 Manager ↔ node
The manager's `shard_ports` for node X changes ONLY via X's own registration
carrying the ports X actually derived-and-will-bind, applied etcd-first in one
fenced txn. Timeline: X stops (old count) → manager still routes old ports →
connection refused / df fails → callers retry (recovery backoff F233, GC
cooldown, PS append retry) — all existing down-node behavior. X registers (new
count) → routing flips atomically with the txn. X crashes between register and
serve → node down; healed on restart. **There is no reachable state where the
manager routes new-count to a node running old-count**, because the
registration payload is generated by the running process itself, not by an
operator's separate declaration. The df echo (§2.6) additionally detects any
exotic drift within 2 s.

Heterogeneous counts across nodes are safe for the same reason today's mixed
`shard_ports` lengths are: `shard_addr_for_extent` takes each node's OWN
vector (`recovery.rs:91` reads `candidate.shard_ports`, etc.).

### 5.3 Client (PS / StreamClient) ↔ manager
Stop-world order (PS down before ENs change, up after) means every PS boots
with fresh caches against post-reshard state. Defense-in-depth if the runbook
is violated (a PS left running): a stale client routes to an old-modulo port —
either connection-refused (shrink) or the wrong live shard (grow), where the
server-side `owns_extent` check **rejects loudly**
(`extent_node.rs:4186-4220`: "extent N belongs to shard X not shard Y") —
never a silent wrong-shard write. **Invariant to preserve: never remove or
soften the EN-side `owns_extent` rejection on data-plane ops; it is the
backstop that makes every routing bug loud instead of corrupting.** (Control
RPCs sibling-forward instead, `extent_node.rs:3265` — also count-consistent
within the process.)

### 5.4 Fencing is untouched
`owner_epoch` is per-extent, persisted in `.meta` bytes 32-40, loaded by
`load_extents`, enforced on every append regardless of which shard owns the
extent (stream note 23). A reshard changes *which shard* enforces it, not the
value. PS restart re-acquires per-partition epochs above all prior values
(F265 bump-on-acquire) → post-reshard writers dominate. Seal/commit semantics
(all-replica ack, lenient seal, F227) are keyed by extent + node, not shard.

### 5.5 Manager crash mid-reshard
Registration idempotent; etcd replay restores committed state; the uuid index
and node row are written in one fenced txn so identity can never tear. A
deposed leader's registration txn loses the F149 fence and the EN's retry
lands on the new leader.

### 5.6 EN restarted with an unexpected count (operator error)
Node is fully-M while the operator intended N: legal, data-safe (files
unmoved; load filter + routing agree at M). Cost is only
parallelism/uniformity. Detectable via `list-nodes` (R6) and the df echo WARN
if etcd and the process ever disagree (they won't, per 5.2 — this catches
e.g. hand-edited etcd).

### 5.7 What can NOT happen (and why)
- **Lost extents**: ownership is a total function (`id % count`) over ids
  found by the disk scan; every file maps to exactly one shard for any count.
  An extent can't be orphaned by resharding — only re-owned.
- **Double-owned extents**: two shards of one process can't both own an id
  (modulo is a function); two processes can't both be node X (uuid identity +
  fenced registration; the df uuid-echo catches an address-level imposter).
- **Torn identity**: `node_uuid` is IN the `nodes/<id>` record — a single kv,
  written atomically. There is no separate index to fall out of sync.

---

## 6. Migration from today's model

Same-commit, stop-world upgrade **that RESETS etcd** (pivot, §0 decision 4 —
the user accepted a reset, so the persisted `MgrNodeInfo` / `MgrNodeOverride`
layout changes are free and there is no zero-touch adoption to engineer):

1. Full stop → replace binaries → `cluster.sh reset` (wipe etcd) → full start.
   WIRE bump to 19 makes any accidental mixed-fleet contact refuse loudly
   (WIRE-1/R1 handshake), and rkyv fail-loud (manager note 39) refuses to
   decode un-reset old `nodes/` values rather than mis-reading them.
2. Fresh cluster bring-up (§ server CLAUDE.md startup ordering): each EN's
   `autumn-op format` mints a `node_uuid` (stamped in every dir, §2.1) and
   registers it → every node is uuid-keyed from first boot. The uuid-less
   "legacy adopt" branch (§2.4-2) still exists for robustness but is not
   exercised on a reset cluster (no pre-M0 records survive the wipe).
3. **NO rollback across this change.** An older binary cannot decode the new
   `nodes/` / `node_override/` layout (rkyv fail-loud blocks it from becoming
   leader — the "bump 后不可滚回" rule). If a rollback is truly needed, it is
   another stop-world + reset onto the old binary. This is the accepted cost of
   dropping the no-reset constraint.
4. `autumn-op format --shard-ports/--listen/--advertise` become hard errors
   with migration messages. `cluster.sh` / entrypoint / autumn-deploy /
   k8s manifests updated in the same commit (they are repo-internal callers).
5. Docs: `docs/ops.md` gains the §4 runbook (manual-verification steps,
   CLAUDE.md rule 11); `docs/k8s_deploy.md` loses the per-pod-Service scaling
   section; `docs/baremetal_deploy.md` loses the single-shard caveat.

Existing per-pod ClusterIP Services can be deleted after the first
post-upgrade EN registration (the registered address flips from ClusterIP to
pod IP on the next pod restart; until then the ClusterIP keeps working since
the Service still selects the pod — a graceful decommission of the old
plumbing).

---

## 7. Considered & rejected: (b) consistent hashing / vnodes / CRUSH, (c) explicit shard map

Both exist to solve a problem this system does not have: **minimizing data
movement when membership changes**. Here movement is already exactly zero —
files live in a shard-independent hashed layout on dirs shared by all shards,
so "re-sharding" is a pure re-labeling that modulo computes for free from one
integer already registered per node. Consistent hashing would add ring state,
vnode maps, and a second placement authority that can disagree with the disk
scan; an explicit shard map adds a persisted artifact to migrate, validate,
and repair. Zero data movement beats them all, and modulo's single source of
truth (`len(shard_ports)` in the node's own registration) has no independent
failure mode. Reject both.

---

## 8. Phased milestone plan

Each milestone is independently buildable, `cargo test -p <crate> --lib`
green, coco-reviewable, and committed as its own feature point (CLAUDE.md
rule 10: feature → tests → docs/ops.md → commit). M0+M1 each touch wire
schema files — each records its fingerprint in `WIRE_VERSION_FINGERPRINTS`
(two bumps total is fine; or land M0+M1's wire edits in M0 to bump once —
implementer's choice, the registry test forces correctness either way).

### M0 — `node_uuid` identity (no routing behavior change) — **DONE**
- **Files (as shipped)**: `crates/rpc/src/manager_rpc.rs`
  (`RegisterNodeReq.node_uuid` + **in-struct** `MgrNodeInfo.node_uuid` +
  `MgrNodeOverride.node_uuid`); `crates/manager/src/rpc_handlers.rs`
  (`handle_register_node` precedence: uuid-tombstone precheck → uuid match →
  legacy address-adopt → address-conflict refuse → identity-only preserve →
  create; fence/remove tombstones carry `node_uuid`; `clear-node-override`
  lifts the `decommissioned/` key too); `crates/server/src/bin/autumn_op/
  main.rs` (`cmd_format`: fail-loud multi-dir `node_uuid` read, atomic
  persist-before-register; still sends full location — behavior unchanged);
  `crates/rpc/src/lib.rs` (fingerprint registry, v19).
- **Wire/etcd**: WIRE v19 (MIN=MAX=19), **in-struct persisted fields** →
  same-commit stop-world deploy requiring an **etcd reset** (§0 decision 4, §6).
- **Acceptance (6 `f_en_dynshard_*` lib tests, all green)**: uuid match across
  an address+shard-port change → same node_id + updated location; legacy
  address node adopts the uuid; identity-only (empty addr) register preserves
  ALL live location (addr + ports + ctrl); a fenced/decommissioned uuid is
  refused at any address AND survives the real `remove_node` deletion, with
  `clear` re-admitting it; a recycled IP under a fresh uuid is accepted only
  after the old record is removed; a duplicate address under a different uuid is
  refused. rpc(44)+manager(188)+stream(96) lib green.

### M1 — EN startup self-registration + df echo self-heal (shard count becomes dynamic here)

**M1a — DONE (EN self-registration).** `crates/server/src/bin/extent_node.rs`:
`--advertise HOST:PORT` (OPTIONAL — backward-compatible; when unset the EN keeps
the format-stamped location, so existing launchers don't break), validated as
IP:port with port == `--port`; `read_node_identity` reads the `node_uuid` +
per-dir `disk_uuid` sentinels (fail-loud, no mint — `format` already stamped
them in M0); pure `build_register_req` (transport-conditional control_address,
unit-tested); `register_with_manager` (retry 30×1s through a mid-election
manager, fail-stop on hard refusal / exhaustion) called from shard 0 + from
`run_single_shard`, after `verify_manager_cluster_id`, before serving. The
manager side needs NO change — M0's uuid-match branch already updates location
in place. `cluster.sh` EN launch passes `--advertise "${BIND_HOST}:$port"`
(idempotent with `format`'s advertise → exercises the path end-to-end).

**M1b — OPEN (df-echo self-heal + format identity-only).**
- **Files**: `crates/stream/src/extent_node.rs` + config (thread
  `(node_uuid, advertise, shard_ports)` for the df echo; extend `handle_df`);
  `crates/rpc/src/extent_rpc.rs` (`DfResp` echo fields);
  `crates/manager/src/lib.rs` (`node_health_loop`: compare echo, heal via
  `mirror_register_node`, WARN on uuid-imposter + treat as df-fail);
  `crates/server/src/bin/autumn_op/main.rs` (`cmd_format` →
  identity-only registration; `--shard-ports`/`--listen`/`--advertise`
  removal stubs); `autumn-op list-nodes` uuid + shard-count display
  (wire-side response extension).
- **Wire/etcd**: WIRE bump (DfResp + any response-struct extension); no etcd
  change.
- **Acceptance**: integration test (etcd-gated, `crates/manager/tests/`):
  boot 1 manager + 1 EN at cpuset len 2 → write via a stream → stop EN →
  restart with cpuset len 4 → `list-nodes` shows 4 ports → read back all data
  (extents re-owned, zero movement) → restart with len 1 → read back again.
  Unit: df-echo mismatch heals etcd; uuid-mismatch echo does not heal and
  marks df failed. Manual: `docs/ops.md` reshard runbook draft executes on
  cluster.sh.

### M2 — deploy layer + docs
- **Files**: `deploy/docker/entrypoint.sh` (pod-IP advertise; drop
  `--shard-ports` from format; keep `AUTUMN_EXTENT_SHARDS`→cpuset);
  `deploy/k8s/extent-node.yaml` (delete per-pod Services; Downward-API
  `status.podIP`; StatefulSet kept for PVC identity);
  `deploy/k8s/kustomization.yaml`; `deploy/overlays/vke/` (delete
  `en-extra-services.yaml`, simplify `deploy.sh`);
  `deploy/baremetal/autumn-deploy` (lift the single-shard guard, pass
  `--advertise`); `cluster.sh`; `docs/k8s_deploy.md`,
  `docs/baremetal_deploy.md`, `docs/ops.md` (final runbook), `README.md` if
  user-visible usage changed.
- **Acceptance**: `bash deploy/validate.sh` green; kind/minikube (or vke)
  bring-up; pod delete → reschedule → same node_id at new IP, zero manifest
  edits; `AUTUMN_EXTENT_SHARDS` bump + rollout restart = reshard.

### M3 — reshard hardening: e2e + chaos
- **Files**: new `crates/manager/tests/system_en_reshard.rs` (stop-world
  grow 2→4 and shrink 4→1 under a written dataset: zero loss, VP reads OK,
  recovery of a killed replica post-reshard routes to the new shard);
  a chaos script arm (`scripts/*_chaos.sh` style): reshard mid-soak with the
  write-liveness probe; a runbook-violation test (leave one PS running
  through the reshard → assert loud `owns_extent` rejections + client retry
  convergence, no silent loss).
- **Acceptance**: e2e green 5/5; chaos run clean; coco arch pass over the
  full delta.

---

## 9. Residual uncertainties (verify during implementation)

1. **`NodesInfo` response plumbing** — RESOLVED by the in-struct pivot: uuid is
   a field on `MgrNodeInfo`, which the response serves directly, so `list-nodes`
   reads it with no wrapper struct or parallel vec. (M1 adds the `list-nodes`
   uuid + shard-count columns.)
2. **Empty-`shard_ports` legacy fallback** — `MgrNodeInfo` doc says empty =
   route to `address` (manager_rpc.rs:256-259). New single-shard registrations
   send `vec![port]`; verify no code path *requires* empty for single-shard
   (routing is equivalent, but check `autumn-op info` display and any
   `shard_ports.is_empty()` branches beyond `shard_addr_for_extent`).
3. **UCX + multi-shard** — format's UCX branch registers an empty
   control_address (one listener). Verify how multi-shard UCX ENs handle
   control listeners today (`serve_with_control` under UCX) before porting
   the transport-conditional into the EN registration helper.
4. **`ReconcileExtentsReq.node_id: 0`** (`extent_node.rs:3183-3186`) — with
   registration in the EN, the node_id could now be threaded down for better
   manager logging; optional cleanup, not required.
5. **Memory-mode manager** (no etcd) — RESOLVED: with uuid in-struct there is
   no index; `handle_register_node` scans `s.nodes` by uuid identically in
   memory-mode and etcd-mode (the 6 `f_en_dynshard_*` tests run in memory-mode).
6. **df dial vs control-port stability** — control base port doesn't change
   with count (base = port+1000), so the manager's df dial to a
   restarted-with-new-count node works even before its re-registration lands;
   confirm no assumption anywhere that `control_address`'s port relates to
   shard count.
