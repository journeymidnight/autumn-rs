# Copy-on-write EC conversion — design

**Status:** design, not implemented. Supersedes the takeover state machine
sketched in `scratchpad/ec_takeover_design.md` (that document's adversarial
findings remain valid and are cited below; its *solution* — a probe RPC plus a
roll-forward/roll-back state machine — becomes unnecessary under this design).

**Origin:** a review of "what happens when the EN executing an EC conversion
dies" showed the conversion has an unrecoverable middle state, and that the only
escapes today are an operator or a fence-driven abandon. Tracing *why* it is
unrecoverable led to a single root cause, and to the invariant below.

---

## 1. The invariant

> **A sealed extent's data file is CREATE-ONCE and DELETE-ONLY. It is never
> renamed, never truncated in place, never rewritten.**

A sealed extent is immutable by definition — that is the property the whole
stream layer leans on (idempotent recovery, restartable EC, `sealed_length` as
an authoritative bound). Any operation that *mutates* a sealed file's identity
or contents re-introduces a state that is neither the old value nor the new one,
and every such state needs its own crash-recovery machinery.

The legal operations on a sealed extent's payload are therefore exactly two:

| operation | legal? | why |
|---|---|---|
| create a new file (staging, temp) | ✅ | additive; a partial one is identifiable and discardable |
| delete a file | ✅ | atomic at the directory entry; idempotent |
| `rename` over a live file | ❌ | mutates identity; needs an intent marker + replay |
| `set_len(0)` + rewrite | ❌ | a crash leaves a *corrupt* file the manager may still count |

Choosing which file is authoritative is a **metadata** decision (the manager's
layout), not a filesystem operation.

---

## 2. What the current design does, and what it costs

`run_convert_to_ec_task` (`crates/stream/src/extent_node.rs`) runs a 2PC:

1. **Prepare** — the coordinator RS-encodes the extent and streams each shard to
   its target's staging file `extent-{id}.ec.dat` (`write_shard_stripe_local`,
   `MSG_WRITE_SHARD` = 10).
2. **Commit** — every participant **renames `.ec.dat` → `.dat`**
   (`commit_shard_local:5482` → `finish_ec_commit:5584`, driven remotely by
   `MSG_COMMIT_EC_SHARD` = 12 / `handle_commit_ec_shard:7906`), destroying that
   node's full replica, then persists a new `eversion`.
3. The manager applies the layout (`apply_ec_conversion_done`,
   `crates/manager/src/recovery.rs:1707`).

The rename in step 2 is the invariant violation, and it is the origin of every
hard problem in this area:

- **A dangerous middle state exists.** Between the first and last participant's
  rename, some nodes hold shards while the manager's layout still says
  *replicated*. This state can be neither rolled back nor rolled forward from
  outside, because the manager cannot tell (without probing every node) which
  half it is in.
- **Re-encoding becomes impossible.** Once a participant renames, the full
  extent bytes are gone from that node. The coordinator can no longer re-derive
  the shards. This — not the marker bookkeeping — is why a dead coordinator
  cannot simply be replaced.
- **The marker cannot be auto-released.** `extent_inflight_stale_sweep_loop`
  deliberately refuses to release `ConvertToEc` markers (WARN-only) for exactly
  this reason, so a stuck conversion requires an operator.
- **A crash window inside the rename needs its own machinery.** The
  `extent-{id}.ec.commit` intent marker (`EcCommitMarker:254`,
  `write_ec_commit_marker:5728`, `read_ec_commit_marker:5784`, the three-state
  replay in `load_extents`) exists *only* to make the rename↔`save_meta` pair
  crash-atomic.
- **Attempt identity had to be added.** Because staging survives a failed
  attempt and carries no identity, a re-dispatch under a different assignment
  could skip prepare and commit stale, wrong-index staging over live replicas
  (the "wrong-shard adoption" hole). The `ec.prepared` marker
  (`ec_prepared_marker_path:422`) was added to close it.

---

## 3. Why the textbook CoW does not apply here

The usual fix (HDFS EC reconstruction, WAS/Colossus) is to write the encoded
form to a **new object/generation** and atomically flip a pointer. In autumn
that is not available: a `ValuePointer` persisted in every SST and checkpoint
pins `(extent_id, offset, len)` directly. Changing the extent id would require
rewriting every VP that references it — GC-scale work that itself needs
atomicity. **The extent id must survive the conversion.**

---

## 4. Design

Keep the extent id. Make the shard a **second, additive file**, and make the
manager's layout flip the **only** commit point.

```
today:   stage .ec.dat ──▶ [each node renames .ec.dat → .dat] ──▶ manager applies
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ dangerous middle state

design:  stage .shard{i} ──▶ manager flips layout in ONE etcd txn ──▶ lazy delete
                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ atomic; sole commit point
```

- **Before the flip.** Every replica's `.dat` is untouched. `extent-{id}.shard{i}`
  is purely additive staging. The layout says *replicated* with
  `payload_location = InDat`; reads use `.dat` and are correct. Nothing is at
  risk.
- **The flip.** One leader-fenced etcd transaction sets `ec_converted`, the
  `replicates`/`parity` layout, `payload_location = InShardFile`, and the new
  `eversion`. There is no partial flip.
  **Precondition: all K+M targets have confirmed a durable staged shard.**
  (Under the old design this was a hardening patch; here it is the natural
  meaning of "prepared".)
- **After the flip.** Cleanup is by **deletion only**:
  - a shard holder deletes its now-redundant `.dat`;
  - a node no longer in the layout becomes a **non-member** and is reaped by the
    membership-based reconcile (see §8).
- **Rollback** is symmetric and free: delete the staged shards. Nothing was ever
  destroyed, so any later attempt may pick a completely fresh assignment.

### 4.1 Which file does the EN serve?

Both files can exist at once, so something must say which one is authoritative
for a given read. That choice is **metadata owned by the manager** (§1), so the
manager states it and the EN obeys. **The EN never infers its own role.**

- The extent's layout carries a **`payload_location`**: `InDat` (a full replica,
  or a legacy shard converted under the old scheme) or `InShardFile` (a CoW
  shard). It is written by the flip, inside the same atomic transaction that
  sets `ec_converted` and the new layout.
- Reads already resolve the layout before dispatching — `read_with_layout`
  (`crates/stream/src/client.rs:4102`) branches on `ex.ec_converted` to choose
  `ec_subrange_read:4625` (shard reads) versus the replicated path. That decision
  is already made against the authority; the request simply carries it down.
- **Rule:** serve the file named by the request's `payload_location`. A request
  that asks for a file the node does not have is an **error**, never a silent
  fallback to the other file — returning shard bytes as a whole value (or the
  reverse) is exactly the corruption this design exists to remove.

This keeps the EN stateless with respect to its role: no new `.meta` field, no
notification, no inference from a counter. It is also the natural extension
point — a future re-encoding is a new `payload_location` value rather than
another in-place mutation.

### 4.2 Staging files are named by shard index

A staged shard is written to **`extent-{id}.shard{i}`**, not to a single shared
`.ec.dat`.

The index is part of the filename, so a shard staged for one index can never be
mistaken for another. That makes a whole class of failure structurally
impossible rather than guarded: a node that staged shard *j* for one attempt and
is assigned index *k* by a later attempt cannot satisfy a check for its own
staged shard, because the file it would need does not exist. (Under a single
shared staging name the two are indistinguishable — same extent and same K give
the same file size — which is why an attempt-identity marker was needed at all;
§5.)

---

## 5. What this removes

Because there is no rename, the machinery that existed to make the rename
crash-atomic is not merely simplified — it is deleted:

| removed | why it existed |
|---|---|
| the whole **commit phase**: `MSG_COMMIT_EC_SHARD`(12), `handle_commit_ec_shard:7906`, `commit_shard_local:5482`, `finish_ec_commit:5584` | per-node destructive publish |
| `ec.commit` intent marker: `EcCommitMarker:254`, `ec_commit_marker_path:426`, `write_ec_commit_marker:5728`, `read_ec_commit_marker:5784`, the `load_extents` three-state replay | rename ↔ `save_meta` crash window |
| `ec.prepared` attempt marker (`ec_prepared_marker_path:422` and its helpers) | staging outliving an attempt could be adopted by the next one — now impossible by construction (§4.2) |
| the EC takeover state machine + `MSG_PROBE_EC_STATE` (`scratchpad/ec_takeover_design.md` §D) | deciding roll-forward vs roll-back from participant state |
| the EC exception to "a node going offline releases its markers" | partial commits made release unsafe |

The 2PC collapses to **1PC (stage) + one atomic metadata flip**.

---

## 6. Interaction with the marker model

The agreed marker model is: *the marker is a standing instruction that the
leader re-sends until the desired state is reached; it never expires by wall
clock; it is released by completion or by an event that makes its pinned target
impossible (node offline/fenced/removed); the executing RPCs must be
idempotent.*

EC only satisfies that model under this design:

- **Idempotent execution** — with no destructive step, a re-dispatch simply
  re-stages into its own index-named file (§4.2). The source bytes remain on
  `.dat`, so a re-encode is always possible.
- **Safe release on offline** — releasing a marker before the flip destroys
  nothing, so a dead coordinator is handled by *release + re-derive*, with no
  probe and no constraint to preserve the previous assignment.

---

## 7. Migration and compatibility

Extents converted under the old scheme have their shard in `.dat`. They are
exactly `payload_location = InDat`, which is the default for every pre-existing
extent — so they keep working with no migration step, no rewrite, and no
backfill. New conversions produce `extent-{id}.shard{i}` and flip to
`InShardFile`, leaving `.dat` until cleanup. Both shapes coexist indefinitely; a
node may hold one of each for different extents.

Deployment is same-commit stop-the-world as always, so no mixed-version
negotiation is required.

---

## 8. Cost, and what it depends on

- **Space is not meaningfully worse.** `.dat` and the staged shard already coexist
  today during staging (the rename is what ends it). This design extends that
  window from "the 2PC" to "until cleanup runs", so the *peak* is unchanged and
  only its duration grows. Cleanup must therefore be driven, not best-effort.
- **Cleanup is the membership-based reconcile** already planned for recovery
  residue: the manager answers "which of the extents you report should you not
  be holding" using membership (`extent_nodes = replicates ++ parity`), with a
  grace period and respecting in-flight markers. Post-flip cleanup of the stale
  full replicas is the same mechanism, not a new one.
- **Read path** gains one file-selection branch on a hot path. The predicate is
  a local comparison plus file presence — no extra I/O in the common case.

---

## 9. A related violation to audit

The same invariant condemns in-place rewrite, not just rename.
`stream_extent_from_sources:4742` truncates the destination to 0 before each
source attempt; `handle_re_avali:6875` uses it to repair a *lagging existing
replica*. A crash mid-refill leaves a short file where a complete one used to
be. The recovery peer-copy path already learned this lesson —
`peer_copy_full_extent_to_dat:7188` streams into a temp file and publishes only
after a full copy lands.

**Action:** audit whether `handle_re_avali`'s target can be a replica the
manager still counts (i.e. whether its `avali` bit can be set at that moment).
If it can, that path must move to "write a new file, then publish" or
"delete-then-create with membership reflecting the gap" — same class of fix,
tracked separately from this design.

---

## 10. Test plan

**Unit (EN):**
- staged but not yet flipped → a read carrying `InDat` returns full data from
  `.dat`; the staged shard is never reachable through it.
- after the flip → a read carrying `InShardFile` returns shard bytes.
- a read whose `payload_location` names a file the node does not hold **errors**
  — it must never fall back to the other file.
- a node assigned index *k* after having staged index *j* cannot satisfy a
  check for shard *k* (structurally: the file does not exist).
- re-stage after a partial attempt produces byte-identical staging (RS is
  deterministic).
- legacy shape (shard in `.dat`, `payload_location = InDat`) still serves shard
  reads unchanged.

**Unit / system (manager):**
- flip requires all K+M staged confirmations; a missing one blocks it.
- marker release before the flip leaves the extent fully replicated and
  readable; a subsequent conversion with a *different* assignment succeeds.

**Crash matrix (the point of the design — every one should self-heal):**
| kill point | expected |
|---|---|
| mid-encode on the coordinator | re-dispatch re-encodes; no state to reconcile |
| after some targets staged, before flip | re-dispatch re-stages the rest; or release + fresh assignment |
| coordinator dies permanently before flip | marker released on offline; fresh assignment; **no probe** |
| immediately after the flip txn | conversion is done; cleanup is idempotent and re-drivable |
| during cleanup | cleanup re-runs; deletion is idempotent |

**Harnesses:** `crates/manager/src/ec_g4_wedge_harness.rs` for the apply-fail
path; the live 3-EN `scratchpad/ec_smoke.sh` extended with coordinator
kill/SIGSTOP at each of the points above.

---

## 11. Sequencing

This is a structural change to the most corruption-sensitive path in the
system. It should land **after** the recovery-side work that establishes the
same model on a lower-risk path:

1. `handle_require_recovery` becomes idempotent (complete ⇒ adopt, incomplete ⇒
   reset and rebuild) — removes the permanent `already exists` poisoning.
2. Recovery's marker becomes a standing instruction (drop the "marker exists ⇒
   skip dispatch" short-circuit) and the wall-clock stale sweep for Recovery is
   deleted.
3. Offline/fence releases that node's Recovery markers.
4. Membership-based reconcile with grace + in-flight guard (residue GC).
5. **This document** — CoW EC conversion; afterwards step 3 extends to EC
   unconditionally and the takeover design is dropped.
