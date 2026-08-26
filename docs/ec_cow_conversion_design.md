# Copy-on-write EC conversion — design

How an extent is converted from 3-way replication to EC shards. The shard is
staged as an **additive** file beside the untouched `.dat`, and the manager's
layout flip is the **sole** commit point — so before the flip nothing is
committed and an abandoned attempt costs only the deletion of files no reader is
pointed at.

This is what the code does; `crates/stream/CLAUDE.md` and
`crates/manager/CLAUDE.md` carry the per-crate invariants.

---

## 1. The invariant

> **An operation on a sealed extent's payload must never leave a state that is
> neither the old value nor the new one — and must never destroy the only copy
> before the replacement is durable.**
>
> In practice: **create new files and delete old ones. Do not rename over a live
> file, and do not truncate-and-refill in place.**

Scope: **sealed** extents. The append path legitimately truncates an **open**
extent back to the consensus commit point (`truncate_to_commit`) — that is the
commit-reconciliation protocol operating on a mutable object, and is out of
scope here.

The rule is stated as an outcome rather than a syntactic ban because one
existing rename is legitimate: `peer_copy_full_extent_to_dat`
(`crates/stream/src/extent_node.rs:7188`) streams a full copy into a temp file
and renames it over `.dat` **only after the complete copy has landed**, so it
never produces a neither-state and never destroys the only copy. The prohibition
is on renames and truncations that *do*.

| operation | verdict | why |
|---|---|---|
| create a new file (staging, temp) | ✅ | additive; a partial one is identifiable and discardable |
| delete a file | ✅ | atomic at the directory entry; idempotent |
| rename a **complete** temp over the file it supersedes | ✅ | never a neither-state (`peer_copy_full_extent_to_dat`) |
| rename that **changes what the payload means** (replica → shard) | ❌ | creates a state the manager cannot classify; needs an intent marker + replay |
| `set_len(0)` + refill in place | ❌ | a crash leaves a *corrupt* file the manager may still count |

Choosing which file is authoritative is a **metadata** decision, owned by the
manager, not a filesystem operation.

---

## 2. Why the textbook CoW does not apply here

The usual fix (HDFS EC reconstruction, WAS/Colossus) writes the encoded form to
a **new object/generation** and flips a pointer. Not available here: a
`ValuePointer` is a raw 24-byte record pinning `(extent_id, offset, len)`
(`crates/partition-server/src/lib.rs:617`), persisted in every SST and
checkpoint. Changing the extent id means rewriting every VP that references it —
GC-scale work that itself needs atomicity. **The extent id must survive the
conversion.** (Verified during review.)

---

## 3. Design

Keep the extent id. Make the shard an **additive second file**, and make the
manager's layout flip the **only** commit point.

```
stage .shard{i} ──▶ manager flips layout in ONE etcd txn ──▶ driven cleanup
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ atomic; sole commit point
```

- **Before the flip.** Every replica's `.dat` is untouched; `extent-{id}.shard{i}`
  is purely additive. The layout says *replicated* (`payload_location = InDat`);
  reads use `.dat`. Nothing is at risk.
- **The flip.** One leader-fenced etcd transaction sets `ec_converted`, the
  `replicates`/`parity` layout, `payload_location = InShardFile`, and the new
  `eversion`. **Precondition: all K+M targets confirmed a durable staged shard
  *for this attempt* (§3.1).**
- **After the flip.** Convergence is driven by the reconcile (§6): shard holders
  drop their now-redundant `.dat`; nodes no longer in the layout drop everything.
- **Rollback** is symmetric and free: delete the staged shards. Nothing was
  destroyed, so a later attempt may pick a completely fresh assignment.

### 3.1 Attempt identity (the precondition's teeth)

The flip precondition is only as good as the evidence behind it, and the
completion report alone cannot distinguish attempts:

- The apply trigger is the coordinator's df-carried `EcConvertDone
  {extent_id, new_eversion}` (`crates/rpc/src/extent_rpc.rs:714`).
- `new_eversion = live_eversion + 1` (`crates/manager/src/rpc_handlers.rs:4963`),
  and an abandoned attempt never bumps the extent's eversion — so **a re-issued
  attempt gets the same `new_eversion` as the one it replaced**.

Because §4 makes marker release routine, a stale report from a previous attempt's
coordinator can therefore match the *current* marker and fire the flip while the
current attempt has staged nothing. Post-flip cleanup would then delete the last
full replicas → unrecoverable loss.

**Required mechanism:**

- **A per-attempt nonce**, generated when the marker is written, persisted with
  it, and threaded through `ExtConvertToEcReq` → `WriteShardReq` →
  `EcConvertDone`. The apply refuses any report whose nonce does not match the
  live marker; a staging write carrying a stale nonce is refused by the EN.
- **Reporter identity** — only `target_nodes[0]` may complete its own marker.
  This is the belt; the nonce is the braces.

The nonce is also what lets the coordinator's prepare-skip be attempt-scoped,
and what stops a successor attempt's staging from being polluted by a
zombie predecessor writing the same index file (§3.3).

**As built.** The nonce is *the etcd revision of the txn that created the
marker*, read from that txn's own response (`txn_fenced_revision`) and rebuilt on
promotion from the key's `mod_revision`. This costs no new key and no persisted
struct change. The alternative was widening `MgrEcDispatchInflight`, which is
nested in `MgrExtentInflightRecord` as an `Option` — so the change would shift
that struct's archived layout and make every live marker (recovery and delete
included) fail rkyv validation on replay. That is only a hard blocker across an
upgrade, which does not apply here; it is still the worse design, because a
marker's identity is a fact etcd already knows and duplicating it into the value
invites the two to disagree.

Being a revision also makes nonces **monotonic**, which buys a guard a random
nonce could not: a participant refuses a `WriteShard` whose nonce is *lower* than
the attempt it is already staging. A merely-released coordinator (the routine
case) keeps its `owner_epoch`, so that fence does not stop it interleaving
stripes into its successor's staging file; the ordering does. That guard is
in-memory — it arbitrates two live writers, and a restart falls back to the
stripe-0 truncate and the epoch fence.

`0` means "no attempt identity". A 0-nonce report completes only a 0-nonce
marker, and can never complete an identified attempt — so the sentinel is inert
in normal operation and would let a mixed fleet converge if one ever existed.

### 3.2 Which file does the EN serve?

Both files can exist at once, so something must say which is authoritative. That
choice is metadata owned by the manager (§1), so **the manager states it and the
EN obeys — the EN never infers its own role.**

The request must carry **both**:

- **`payload_location`** — `InDat` (the whole payload is in `.dat`) or
  `InShardFile`.
- **the shard index** — a node can legitimately hold shard files at two different
  indices (different attempts, or a parity slot plus a data slot after a
  reassignment), so the location alone does not name a file.

Reads already resolve the layout before dispatching — `read_with_layout`
(`crates/stream/src/client.rs:4102`) branches on `ex.ec_converted` to choose
`ec_subrange_read:4625` versus the replicated path — so this transmits a decision
already made against the authority rather than adding one.

**Rule:** serve the file named by `(payload_location, shard_index)`. A request
naming a file the node does not hold is an **error** (a distinct code, §5), never
a silent fallback to the other file — returning shard bytes as a whole value, or
the reverse, is exactly the corruption this design exists to remove. The client
treats that error as "refresh the layout and retry", which converges because the
manager is authoritative.

**Read paths that must carry it** (each has a natural carrier):

| path | carrier |
|---|---|
| `read_with_layout` → replicated (`read_replicated_with_failover`) | `ExtentInfo` in hand |
| `read_with_layout` → `ec_subrange_read:4625` → `read_shard_from_addr:4513` | per-shard index known at dispatch |
| EC reconstruct (`ec_reconstruct_shard_subrange`) | same |
| PS bulk proxy `read_value_into_pooled` | `ExtentInfo` snapshot |
| **client-direct** `extent_read_descriptor` → `read_extent_value_direct` | the descriptor must carry it |
| recovery source fetch / `handle_re_avali` peer fetch | manager-supplied `ExtentInfo` |
| `commit_length` / probe | N/A (metadata only) |

The client-direct path is the one that must not be missed: the SDK holds a
descriptor and reads an EN with no further manager round-trip, so a stale
descriptor must fail loudly rather than read the wrong file.

### 3.3 Staging files are named by shard index

A staged shard is written to **`extent-{id}.shard{i}`**. The index is in the
filename, so a shard staged for one index can never be *served* as another —
which a single shared staging name could not prevent.

Note the limit of naming: it protects **serving**, not **staging
confirmation**. A node reused at the *same* index across two attempts holds a
same-size file — and for shard 0 the content is even byte-identical — so "the
file exists and has the right size" still cannot confirm that *this* attempt
prepared it. That confirmation is the nonce's job (§3.1), which is why
`ec.prepared` may only be deleted once the nonce subsumes it.

### 3.4 The EN's shard-holder model

The EN's entire extent lifecycle is keyed to `extent-{id}.dat`:
`load_extents` builds entries from `.dat` scans
(`crates/stream/src/extent_node.rs:3665-3710`, `parse_extent_id:525`),
`remove_extent_files:448` unlinks a fixed set, and the reconcile scans look for
known suffixes. **A shard file that nothing scans is a file that survives cleanup
and then vanishes from the system at the next restart** — so this is part of the
design, not an implementation detail.

- **On-disk shape.** A shard holder has `extent-{id}.shard{i}` + `.meta`. A
  pre-cleanup holder still also has `.dat`.
- **Startup discovery.** `load_extents` must build an entry from a shard file
  when `.dat` is absent, recording the index found. Two shard files at different
  indices is a legal transient — record both; the layout decides which is
  authoritative.
- **Entry model.** One `ExtentEntry` per extent, tracking which payload files
  exist. `len` / `sealed_length` semantics must be stated per state: for a shard
  holder the shard file's length is *not* the extent's `sealed_length`. The fd
  cache (`FdLru`) keys on extent id, so it must not assume a single file.
- **Cleanup transition** (member node, post-flip): delete `.dat`, repoint the
  entry, `save_meta`. Idempotent and re-convergent — a crash mid-transition is
  resolved by startup discovery, not by an intent marker.
- **`remove_extent_files` and the reconcile scans** must cover `.shard{i}`, or a
  deleted extent leaks shards.
- **`extent_bytes` accounting** (`extent_node.rs:6523`) must include shard files,
  or a converted cluster under-reports physical usage to `df` and to the
  `--min-alloc-free-bytes` admission gate.

### 3.5 The flip transaction

`apply_ec_conversion_done` (`crates/manager/src/recovery.rs:1707`, txn at
`:1759-1767`) was verified during review to be atomic and leader-fenced, with
clean crash windows on either side. Two additions:

- Write `payload_location` in that same transaction (never a second write).
- **Value-CAS `extents/<id>` against the snapshot the decision was made on**, so
  a concurrent mutation (recovery slot swap, seal) cannot be clobbered by a flip
  computed from a stale clone. The CAS states explicitly what would otherwise
  rest implicitly on the inflight ledger serialising per-extent operations.

---

## 4. Interaction with the marker model

The agreed model: *the marker is a standing instruction the leader re-sends until
the desired state is reached; it never expires by wall clock; it is released by
completion or by an event that makes its pinned target impossible (node
offline/fenced/removed); the executing RPCs must be idempotent.*

EC satisfies that model only under this design:

- **Idempotent execution** — no destructive step, so a re-dispatch re-stages into
  its own index-named file (§3.3) under a fresh nonce (§3.1). The source bytes
  remain on `.dat`, so a re-encode is always possible.
- **Safe release on offline** — releasing a marker before the flip destroys
  nothing, so a dead coordinator is handled by *release + re-derive*, with no
  probe and no constraint to preserve the previous assignment.

The nonce is what makes the second bullet safe: routine release is precisely what
raises the odds of a stale cross-attempt report (§3.1).

---

## 5. Persistence and wire

**`payload_location` lives in a sibling etcd key**, not in `MgrExtentInfo`. It
is per-extent metadata with a meaningful default (`absent ⇒ InDat`), so a key
that exists only for the extents that deviate is smaller and simpler than a
field every record must carry.

**Wire.** `ExtentInfo` carries `payload_location`; `ReadBytesReq` carries
`(payload_location, shard_index)`; `CODE_PAYLOAD_NOT_HERE` is the refusal when a
node does not hold the named file. Deploys are same-commit stop-the-world, so a
schema edit is a fingerprint refresh plus a version decision — not a
compatibility window.

---

## 6. Cost, cleanup, and the reconcile contract

- **Per extent the peak is two copies** — `.dat` and the staged shard coexist
  from staging until cleanup lands, so the cost is the *duration* of that
  window, not a new peak.
- **Fleet-aggregate is not unchanged.** With cleanup stalled (manager down, node
  isolated, reconcile backlogged) every converting extent holds both forms for as
  long as the stall lasts. Conversions must therefore be **admission-gated on
  cleanup backlog**, and policy-driven EC arming should stay off until the
  cleanup driver exists. Manual `force-ec-convert` meanwhile is acceptable with
  a loud WARN.
- **Cleanup is not "deletion only" on member nodes.** A shard holder must also
  repoint its entry and persist meta (§3.4). Only non-member cleanup is a pure
  delete.
- **The reconcile is file-granular.** An extent-granular answer ("which extents
  should you not hold") cannot express "keep the extent, drop the `.dat`", so
  the response carries, per extent, the `payload_location` and the node's
  assigned index; the EN derives its own file set from that. The same answer
  cleans rollback residue on member nodes.
- **Accounting**: shard bytes must be visible to `extent_bytes` (§3.4).

---
