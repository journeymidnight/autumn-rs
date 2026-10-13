# Per-extent op arbitration — design

The manager runs several kinds of background work on a sealed extent: rebuilding
a copy (recovery), catching a lagging copy up in place, converting it to EC
shards, and reclaiming it (GC punch / truncate, the reclaim sweeps, the delete
fan-out). Every one of those paths asks the same two questions, and this
document gives each **one** answer:

1. *Can this node take part?* — one node classification (§1).
2. *Which op, if any, may run on this extent now?* — one arbiter with one
   priority order (§2–§4).

The per-extent in-flight ledger (`extent_inflight/<id>`: one op per extent,
created by an etcd CAS) is the lock. On top of it sit the rules below, and every
entry point — proposal, submit, dispatch, GC — asks the same arbiter, so no two
of them can disagree about the same extent.

---

## 1. Node classification

Two independent inputs:

- **auto-state** — what the manager observes through df, in memory, rebuilt
  every leader term: `Online` (answered a df this term and still answering),
  `Suspend` (no df answer yet this term), `Suspected` (answered before, silent
  past the soft timeout);
- **operator override** — what an operator decided, persisted in etcd, surviving
  failover: none, `Maintenance` (stopping for a while, coming back), `Fenced`
  (move the data off, the node is leaving). A decommission tombstone and an
  unregistered node count as `Fenced`.

One function, `node_class(node) -> NodeClass`, combines them into four classes.
An override only ever makes a node worse — there is no "treat as healthy"
override — and clearing it hands the node back to its auto-state:

| override \ auto-state | `Online` | `Suspend` | `Suspected` |
|---|---|---|---|
| none | **health** | **suspend** | **suspected** |
| `Maintenance` | **suspected** | **suspected** | **suspected** |
| `Fenced` / tombstone / unregistered | **fence** | **fence** | **fence** |

After a failover every node starts **suspend**; the df loop proves each one
within a round (2 s), and one still silent past the soft timeout — counted from
the start of the term, since it has no earlier answer to count from — becomes
**suspected**. Nothing is cancelled for being **suspend**. While leaders change
faster than the soft timeout, a dead node therefore stays **suspend** and the
markers on it are held; the first leader that stays up for the soft timeout
makes it **suspected**. A control plane that cannot keep a leader moves nothing
anyway.

What each class allows:

| | health | suspend | suspected | fence |
|---|---|---|---|---|
| receives new data (allocation, recovery target, EC target) | yes | only allocation's degraded fallback, when no node is **health** | no | no |
| stays in an in-flight op it takes part in | yes | yes — dispatch waits for its df | **no — the marker is cancelled** | **no — the marker is cancelled** |
| its copy is rebuilt elsewhere | — | no | auto `Suspected`: on a repair request; `Maintenance`: only on operator request | yes, at once |

`Maintenance` behaves as **suspected** in everything but the last row: an
operator said the node is coming back, so policy never moves its copies — for
as long as the override stands, with or without an expiry, the extents with a
copy there run one copy short.

A **fence** node may well be alive: fencing means "move the data off", not
"the node is dead". Its copy still serves as a recovery source.

A node carries one override. Setting `Maintenance` on a `Fenced` node is
refused — clear the fence first. A `Maintenance` override with an expiry is
cleared when it expires, and the node falls back to its auto-state. Clearing an
override also lifts a decommission tombstone: that is how a removed node is
re-admitted, and it comes back as **suspend** until its first df.

### Disk classification

A copy lives on a disk, so the rules act on the disk a copy is on.
`disk_class(disk)` combines the class of its node with two facts **the node
itself** reports in its df:

- **Faulted** — a local I/O error the node has already suffered; it does not
  clear without a process restart;
- **Full** — out of space; it clears itself once space is freed.

| disk class | condition |
|---|---|
| **fence** | its node is **fence**, or the disk is Faulted |
| **suspected** | its node is **suspected** |
| **suspend** | its node is **suspend** |
| **full** | its node is **health**, the disk is Full |
| **health** | its node is **health**, the disk is neither |

`online == false` on a disk record (set node-wide by a single df timeout, or by
partition servers' quorum report) is not a fact about the disk and plays no
part — the rolling-restart procedure depends on that. Faulted and Full are
term-local like every df fact: after a failover the node is **suspend** until
it answers, and so are its disks.

What each disk class allows:

| | health | full | suspend | suspected | fence |
|---|---|---|---|---|---|
| receives new data (allocation, recovery target, EC target) | yes | no | only allocation's degraded fallback | no | no |
| a copy on it serves reads and sources a rebuild or an encode | yes | yes | waits | no | a Faulted disk: no; a fenced node's disk: yes (fenced is not dead) |
| an in-flight op writing to it | continues | continues — a write that fails is the attempt's own report (§3) | waits | cancelled | cancelled |
| its copy is rebuilt elsewhere | — | no | no | as its node (§1 table) | yes, at once |

A node is a placement or recovery target only while it is **health** and has at
least one **health** disk.

---

## 2. Priority

At most one op runs on an extent, and **nothing preempts**: an op in flight runs
until it completes or §3 cancels it, and every other op on that extent waits.

When none is in flight, the first rule that applies may start:

0. **Nothing** if the extent is unsealed, or a split/merge holds its partition
   (`topology_gate.rs`).
1. **Removal** — GC punch / truncate, the both-zero and sealed-empty sweeps.
2. **Rebuild a corrupt copy.** Ahead of any EC conversion: the coordinator
   encodes from its own full copy, and a corrupt copy is full length.
3. **Rebuild a copy whose disk class is fence** — on a fence node or a Faulted
   disk.
4. **Rebuild a copy with a repair request** — filed by the auto-policy once a
   `Suspected` node's copy has been degraded past `repair_grace_secs`, or by an
   operator for any copy.
5. **Catch up a Behind copy in place** (`re_avali`).
6. **EC conversion**, when one is proposed or submitted (§5).

Rules 1–3 and 5 are mechanism: the recovery tick and the GC path act on them by
themselves. Rules 4 and 6 act on a decision: a `Suspected` node may be back in a
minute, so moving its copies — like converting an extent — waits for the
auto-policy controller (the deploy layer arms the `balanced` preset, which
includes repair and EC) or an operator.

**Removal waits too.** A punch or truncate is refused while the extent carries
any marker, whatever its `refs`: a punch on a CoW-shared extent changes its
`eversion`, which voids an in-flight recovery's attempt as surely as deleting
the extent would. So that a refused punch does not waste the relocation in
front of it, the PS GC asks the manager whether each candidate is free
**before** relocating (one RPC per candidate, at most `MAX_GC_ONCE` per run) and
skips a busy one with the soft cooldown. A marker taken between that check and
the punch still refuses the punch; the window is one relocation long.

Waiting has two costs, both accepted. Removal does not reserve the extent: a
recovery that keeps failing re-acquires its marker after each backoff, and GC
gets the extent in the gaps (the per-slot backoff grows to 300 s). And a
split/merge waits behind an in-flight op for at most `BLOCKER_WAIT_MAX` (600 s)
and then fails with the op as its reason.

---

## 3. A marker is one attempt

A marker pins one attempt to a fixed set of nodes. **The moment any node it
depends on is found abnormal, the marker is cancelled** — not retried in place,
not counted toward a give-up threshold. The next marker is written by whoever
owns the decision, from the state at that time:

| marker | depends on | written again by |
|---|---|---|
| Recovery | its executor and the disks it may write to | the recovery tick (§2 rules 2–5) |
| ConvertToEc | **every** target, the coordinator included — each receives a shard — and the disk each one writes to | the auto-policy controller or an operator (§2 rule 6) |

A recovery's sources are not pinned: the executor reads from each copy source in
turn (`stream_extent_from_sources`), so one dead source costs nothing, and the
node whose copy is being replaced failing is the reason for the recovery, not a
reason to cancel it. Only when no source can serve does the attempt fail — and
that failure is the attempt's own report below.

"Found abnormal" is either:

- **the class** of the node or disk — **suspected** or **fence** (§1),
  evaluated every tick from live state. Level-triggered: no hook on the fence / maintenance / suspect
  transitions, and it re-converges after a failover;
- **the attempt's own report** — a dispatch, a re-send or a participant write
  that fails or is refused names the node, and the marker is cancelled on the
  spot, before the node's class catches up (`Suspected` takes a 10 s silence).
  The node is a typed field of the report — the EC coordinator's accept answer
  and completion, the recovery completion and refusal carry the failed node's
  id — not text the manager parses out of a message.

A Recovery marker is also cancelled when its source slot is healthy again
(`release_recovery_markers_for_healthy_slots`): the work is no longer needed.

On the EN, a newer attempt supersedes an older one for the same extent: a
`ConvertToEc` request whose nonce is higher than the running attempt's stops
that attempt at its next stripe and starts the new one, instead of answering
"already running". Otherwise the successor is reported accepted while the
cancelled encode runs to its end.

Cancelling is always safe. A recovery's new copy and an EC attempt's staged
shards are additive files no reader is pointed at until the manager applies
them, and the attempt nonce refuses a late completion
(`ec_cow_conversion_design.md` §3.1, §4). Re-deriving costs no more than
retrying in place — a re-dispatched conversion re-encodes from scratch either
way — and the backoff is the owner's own cadence: the recovery tick's per-slot
backoff, the controller's interval and cooldown.

A **Delete** marker is different: its targets are where the files are, not a
choice, so there is nothing to re-derive and it is never cancelled. A **fence**
target is dropped from the fan-out (the node-startup reconcile reaps the files if
the node returns); a **suspected** target is retried with backoff. After 60
attempts the remaining targets move to the persisted retry queue
(`extentDeleteRetry/`) and the marker is removed — the Delete's own hand-off, the
only way its marker ends without every target acknowledging.

Every cancel goes through one function, `drain_extent_inflight_marker`, which
removes the marker and closes the op-ledger entry FAILED with the reason and the
node. There is no second release path with different side effects, and no
separate abandon record: the op entry is where the reason is read.

---

## 4. One arbiter, every entry point

| entry point | asks the arbiter | on "not now" |
|---|---|---|
| policy advisory (EC, repair, GC candidates) | before listing a candidate | not listed |
| auto-policy actuation | again at actuation (the advisory cache can be a policy tick old) | dropped, logged at debug — not filed as a FAILED op |
| `force-ec-convert` / op submit | before acquiring the marker | refused with the arbiter's reason |
| EC dispatch loop, recovery tick | every tick, per marker | §3 cancel, or wait (**suspend**) |
| PS GC | per candidate, before relocating | skipped, soft cooldown |
| punch / truncate / reclaim sweeps | before the etcd txn | refused; the PS retries on its next GC dispatch, the sweeps on their next tick |

The arbiter and `node_class` are pure functions over a snapshot
(`MetadataState`, the ledger, node states and overrides, corrupt marks, repair
requests, topology holds): unit-testable without a cluster, and every caller
gets the same answer for the same state. They run on the manager's existing
ticks over state those ticks already scan; the data path is untouched.

---

## 5. EC conversion onto healthy nodes

A conversion needs K+M **health** targets and one **health** coordinator holding a
full copy — its `avali` bit set, not corrupt-marked, on a disk of class
**health** or **full** (the coordinator only reads its copy). The targets are:

- the extent's replicas on **health** nodes whose disk class is **health** or
  **full** (a replica target keeps its copy where it is), the coordinator first;
- substitutes for the other replicas, plus the extras K+M needs, from **health**
  non-member nodes with a **health** disk (load-ordered, the same pick as
  allocation).

A replica on a **suspected** or **fence** node is therefore no reason to refuse: it is
left out of the target set, falls out of the layout at the flip, and the
reconcile deletes it when its node is back. The conversion restores redundancy
by itself, so a degraded extent that gets converted needs no recovery.

At the flip, each target's disk comes from the target itself — the replica's own
disk, or the disk allocated on a substitute or extra — never from a position in
`replicate_disks ++ extra_disk_ids`.

The target set stays pinned for its attempt. When a target is found abnormal the
marker is cancelled (§3) and the next proposal derives a fresh set; the attempt
nonce keeps the old and new attempts' staging apart on the ENs.

With fewer than K+M **health** nodes the conversion is refused, and rules 3–4
rebuild the copy instead.

---

## 6. Divergences in the code

Where the code still answers differently from §1–§5. Paths are under
`crates/manager/src/` unless noted. Node and disk classes (§1) are
`node_class.rs` and the arbiter (§2, §4) is `extent_arbiter.rs`; every path that
judges a node, a disk or whether an op may start uses them.

**Release paths**

- ConvertToEc markers are released by four paths: the fence sweep
  (`auto_abandon_for_fenced_node`, coordinator only) and the repeated-failure
  give-up (`abandon_ec_marker`, which also writes an `ec_convert_advisory/<extent>`
  key), the abnormal-node cancel and the stale-marker drain
  (`drain_extent_inflight_marker`). The `abandon_ec_marker` doc calls itself
  "the one place".
- A failed EC attempt is re-dispatched with the same targets; the marker is
  abandoned only after 24 consecutive reported failures
  (`EC_ABANDON_AFTER_CONSECUTIVE_FAILURES`, recovery.rs), and a coordinator that
  is itself unreachable never counts toward it.
- A recovery re-send that fails or is refused is only logged
  (`redispatch_pinned_recovery`); a failed first dispatch does drain the marker.
- The stale sweep releases Delete markers after 600 s without moving them to the
  persisted retry queue; the delete loop moves them there after 60 attempts.
  `snapshot_replica_targets` (extent_delete.rs) keeps a fence target in the
  fan-out.
- The EN answers "already running" to a `ConvertToEc` request whatever its nonce
  (`handle_convert_to_ec`, crates/stream/src/extent_node.rs), and the
  coordinator's failure reaches the manager only as text after "failed: " in
  that answer (`dispatch_one_ec_conversion`), with no node id.
- `ec_convert_advisory/<extent>` (ec_abandon.rs) is written on every abandon and
  read and deleted by nothing in the repository; it goes, with its writer.

**EC targets**

- `handle_force_ec_convert` (rpc_handlers.rs) takes every replica as a target and
  refuses an extent with a replica on an abnormal node, instead of substituting
  a **health** node (§5); the flip maps disks to targets by position.
- There is no PS GC precheck; a punch refused after relocation wastes it.
