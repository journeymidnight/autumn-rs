# Copy-on-write EC conversion — design

**Status:** design, not implemented. **Revised after an adversarial review**
(`scratchpad/ec_cow_review.md`) that found two P0s and six P1s in the first
draft; every finding is folded in below. Supersedes the takeover state machine
in `scratchpad/ec_takeover_design.md` — that document's *findings* remain valid
and are tracked here; its *solution* (a probe RPC + roll-forward/roll-back state
machine) is unnecessary under this design.

**Already shipped from this line of work:** the reporter-identity check on
completion reports (commit `8282712`) — §4.1 explains why it is only half the
mechanism.

**Origin:** "what happens when the EN executing an EC conversion dies" has no
good answer today. Tracing *why* led to one root cause and to the invariant
below.

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

## 2. What the current design does, and what it costs

`run_convert_to_ec_task` (`crates/stream/src/extent_node.rs`) runs a 2PC:

1. **Prepare** — the coordinator RS-encodes and streams each shard to its
   target's staging file `extent-{id}.ec.dat` (`write_shard_stripe_local`,
   `MSG_WRITE_SHARD` = 10).
2. **Commit** — every participant **renames `.ec.dat` → `.dat`**
   (`commit_shard_local:5482` → `finish_ec_commit:5584`, driven remotely by
   `MSG_COMMIT_EC_SHARD` = 12 / `handle_commit_ec_shard:7906`), destroying that
   node's full replica, then persists a new `eversion`.
3. The manager applies the layout (`apply_ec_conversion_done`,
   `crates/manager/src/recovery.rs:1707`).

The rename in step 2 is the invariant violation, and it is the origin of every
hard problem here:

- **A middle state exists that nobody can classify.** Between the first and last
  rename, some nodes hold shards while the layout still says *replicated*. It can
  be neither rolled back nor forward without probing every node.
- **Re-encoding becomes impossible.** Once a participant renames, the full bytes
  are gone from that node — so the coordinator can no longer re-derive the
  shards. This, not marker bookkeeping, is why a dead coordinator cannot simply
  be replaced.
- **The marker cannot be auto-released.** `extent_inflight_stale_sweep_loop`
  refuses to release `ConvertToEc` markers (WARN-only) for exactly this reason,
  so a stuck conversion needs an operator.
- **The rename needs its own crash machinery.** The `extent-{id}.ec.commit`
  intent marker (`EcCommitMarker:254`, `write_ec_commit_marker:5728`,
  `read_ec_commit_marker:5784`, the three-state replay in `load_extents`) exists
  *only* to make rename↔`save_meta` atomic.
- **Attempt identity had to be bolted on.** Staging survives a failed attempt and
  carries no identity, so a re-dispatch under a different assignment could skip
  prepare and commit stale, wrong-index staging over live replicas. The
  `ec.prepared` marker (`ec_prepared_marker_path:422`) closes that today.

---

## 3. Why the textbook CoW does not apply here

The usual fix (HDFS EC reconstruction, WAS/Colossus) writes the encoded form to
a **new object/generation** and flips a pointer. Not available here: a
`ValuePointer` is a raw 24-byte record pinning `(extent_id, offset, len)`
(`crates/partition-server/src/lib.rs:617`), persisted in every SST and
checkpoint. Changing the extent id means rewriting every VP that references it —
GC-scale work that itself needs atomicity. **The extent id must survive the
conversion.** (Verified during review.)

---

## 4. Design

Keep the extent id. Make the shard an **additive second file**, and make the
manager's layout flip the **only** commit point.

```
today:   stage .ec.dat ──▶ [each node renames .ec.dat → .dat] ──▶ manager applies
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ middle state nobody can classify

design:  stage .shard{i} ──▶ manager flips layout in ONE etcd txn ──▶ driven cleanup
                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ atomic; sole commit point
```

- **Before the flip.** Every replica's `.dat` is untouched; `extent-{id}.shard{i}`
  is purely additive. The layout says *replicated* (`payload_location = InDat`);
  reads use `.dat`. Nothing is at risk.
- **The flip.** One leader-fenced etcd transaction sets `ec_converted`, the
  `replicates`/`parity` layout, `payload_location = InShardFile`, and the new
  `eversion`. **Precondition: all K+M targets confirmed a durable staged shard
  *for this attempt* (§4.1).**
- **After the flip.** Convergence is driven by the reconcile (§8): shard holders
  drop their now-redundant `.dat`; nodes no longer in the layout drop everything.
- **Rollback** is symmetric and free: delete the staged shards. Nothing was
  destroyed, so a later attempt may pick a completely fresh assignment.

### 4.1 Attempt identity (the precondition's teeth)

The flip precondition is only as good as the evidence behind it, and today that
evidence cannot distinguish attempts:

- The apply trigger is the coordinator's df-carried `EcConvertDone
  {extent_id, new_eversion}` (`crates/rpc/src/extent_rpc.rs:714`).
- `new_eversion = live_eversion + 1` (`crates/manager/src/rpc_handlers.rs:4963`),
  and an abandoned attempt never bumps the extent's eversion — so **a re-issued
  attempt gets the same `new_eversion` as the one it replaced**.

Because §6 makes marker release routine, a stale report from a previous attempt's
coordinator can therefore match the *current* marker and fire the flip while the
current attempt has staged nothing. Post-flip cleanup would then delete the last
full replicas → unrecoverable loss.

**Required mechanism:**

- **A per-attempt nonce**, generated when the marker is written, persisted with
  it, and threaded through `ExtConvertToEcReq` → `WriteShardReq` →
  `EcConvertDone`. The apply refuses any report whose nonce does not match the
  live marker; a staging write carrying a stale nonce is refused by the EN.
- **Reporter identity** — only `target_nodes[0]` may complete its own marker.
  *(Shipped, `8282712`.)* This is the belt; the nonce is the braces.

The nonce is also what lets the coordinator's prepare-skip be attempt-scoped
(§5), and what stops a successor attempt's staging from being polluted by a
zombie predecessor writing the same index file (§4.3).

**As built.** The nonce is *the etcd revision of the txn that created the
marker*, read from that txn's own response (`txn_fenced_revision`) and rebuilt on
promotion from the key's `mod_revision`. This costs no new key and no persisted
struct change, which matters more than it first appears: `MgrEcDispatchInflight`
is nested in `MgrExtentInflightRecord` as an `Option`, so widening it shifts that
struct's archived layout and every live marker — **recovery and delete
included** — would fail rkyv validation on replay, blocking leadership on
upgrade. The sibling key contemplated above was the alternative; etcd already
storing this value made it unnecessary.

Being a revision also makes nonces **monotonic**, which buys a guard a random
nonce could not: a participant refuses a `WriteShard` whose nonce is *lower* than
the attempt it is already staging. A merely-released coordinator (the routine
case) keeps its `owner_epoch`, so that fence does not stop it interleaving
stripes into its successor's staging file; the ordering does. That guard is
in-memory — it arbitrates two live writers, and a restart falls back to the
stripe-0 truncate and the epoch fence.

`0` means "no attempt identity" (a pre-nonce marker or peer). A 0-nonce report
completes only a 0-nonce marker, so an upgrade converges instead of wedging,
while a 0-nonce report can never complete an identified attempt.

### 4.2 Which file does the EN serve?

Both files can exist at once, so something must say which is authoritative. That
choice is metadata owned by the manager (§1), so **the manager states it and the
EN obeys — the EN never infers its own role.**

The request must carry **both**:

- **`payload_location`** — `InDat` (full replica, or a legacy shard converted
  under the old scheme) or `InShardFile`.
- **the shard index** — a node can legitimately hold shard files at two different
  indices (different attempts, or a parity slot plus a data slot after a
  reassignment), so the location alone does not name a file.

Reads already resolve the layout before dispatching — `read_with_layout`
(`crates/stream/src/client.rs:4102`) branches on `ex.ec_converted` to choose
`ec_subrange_read:4625` versus the replicated path — so this transmits a decision
already made against the authority rather than adding one.

**Rule:** serve the file named by `(payload_location, shard_index)`. A request
naming a file the node does not hold is an **error** (a distinct code, §7), never
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

### 4.3 Staging files are named by shard index

A staged shard is written to **`extent-{id}.shard{i}`**, not a single shared
`.ec.dat`. The index is in the filename, so a shard staged for one index can
never be *served* as another.

Note the limit found in review: naming protects **serving**, not **staging
confirmation**. A node reused at the *same* index across two attempts holds a
same-size file — and for shard 0 the content is even byte-identical — so "the
file exists and has the right size" still cannot confirm that *this* attempt
prepared it. That confirmation is the nonce's job (§4.1), which is why
`ec.prepared` may only be deleted once the nonce subsumes it (§5).

### 4.4 The EN's shard-holder model

The EN's entire extent lifecycle is keyed to `extent-{id}.dat` today:
`load_extents` builds entries from `.dat` scans
(`crates/stream/src/extent_node.rs:3665-3710`, `parse_extent_id:525`),
`remove_extent_files:448` unlinks a fixed set, and the reconcile scans look for
known suffixes. **A shard file that nothing scans is a file that survives cleanup
and then vanishes from the system at the next restart** — so this is part of the
design, not an implementation detail.

- **On-disk shape.** A shard holder has `extent-{id}.shard{i}` + `.meta`. A
  pre-cleanup holder still also has `.dat`. A legacy holder has its shard *in*
  `.dat` and no shard file.
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

### 4.5 The flip transaction

`apply_ec_conversion_done` (`crates/manager/src/recovery.rs:1707`, txn at
`:1759-1767`) was verified during review to be atomic and leader-fenced, with
clean crash windows on either side. Two additions:

- Write `payload_location` in that same transaction (never a second write).
- **Value-CAS `extents/<id>` against the snapshot the decision was made on**, so
  a concurrent mutation (recovery slot swap, seal) cannot be clobbered by a flip
  computed from a stale clone. Today's safety rests on the inflight ledger
  serialising per-extent operations — state that dependency explicitly rather
  than leaving it implicit.

---

## 5. What this removes — and what must NOT be swept with it

Because there is no rename, the machinery that existed to make the rename
crash-atomic goes away:

| removed | why it existed | gated on |
|---|---|---|
| the commit phase: `MSG_COMMIT_EC_SHARD`(12), `handle_commit_ec_shard:7906`, `commit_shard_local:5482`, `finish_ec_commit:5584` | per-node destructive publish | step 6 (§10) |
| `ec.commit` marker + its `load_extents` three-state replay | rename ↔ `save_meta` crash window | **retain as frozen repair code** — §7 |
| `ec.prepared` attempt marker | staging outliving an attempt | ONLY after the §4.1 nonce takes over its role |
| the takeover state machine + `MSG_PROBE_EC_STATE` | deciding roll-forward vs roll-back | immediately (never built) |

**Guards that must survive the deletions** — they are not rename-related and were
shipped in `50be92f` against real holes:

- the per-extent op lock around staging writes;
- the post-lock `owner_epoch` re-read (a fenced zombie must not stage);
- stripe-0 truncate on the staging file;
- the `corrupt_meta` (META-FAILCLOSED) refusal;
- `mark_disk_error_for_extent` wiring on every staging write/sync failure.

The 2PC collapses to **1PC (stage) + one atomic metadata flip** — but only the
*commit* half is deleted.

---

## 6. Interaction with the marker model

The agreed model: *the marker is a standing instruction the leader re-sends until
the desired state is reached; it never expires by wall clock; it is released by
completion or by an event that makes its pinned target impossible (node
offline/fenced/removed); the executing RPCs must be idempotent.*

EC satisfies that model only under this design:

- **Idempotent execution** — no destructive step, so a re-dispatch re-stages into
  its own index-named file (§4.3) under a fresh nonce (§4.1). The source bytes
  remain on `.dat`, so a re-encode is always possible.
- **Safe release on offline** — releasing a marker before the flip destroys
  nothing, so a dead coordinator is handled by *release + re-derive*, with no
  probe and no constraint to preserve the previous assignment.

The nonce is what makes the second bullet safe: routine release is precisely what
raises the odds of a stale cross-attempt report (§4.1).

---

## 7. Persistence, migration, and legacy

**`payload_location` must not widen a persisted rkyv struct.** `MgrExtentInfo` is
stored in etcd and decoded on replay; adding a field makes an existing cluster's
stored values undecodable, which blocks leadership — the opposite of a free
migration. Carry it (and the §4.1 nonce) in a **sibling etcd key** keyed by extent
id, absent ⇒ `InDat`. That is what makes "every pre-existing extent defaults to
`InDat`" actually true.

**Wire.** `ExtentInfo` gains `payload_location`; `ReadBytesReq` gains
`(payload_location, shard_index)`; plus a distinct error code for "the requested
payload file is not held here". WIRE fingerprint refresh + version decision as
usual (same-commit stop-world deploy).

**Legacy shards.** Extents converted under the old scheme have their shard in
`.dat` and are `InDat` by default — they keep working with no backfill.

**Pre-upgrade crash states.** `ec.commit` replay and the `recovered` heuristic
must be **retained as frozen repair code**, not deleted with the commit phase: a
node upgraded while holding a mid-rename crash state still needs them. The
alternative, if they are to be deleted, is an enforced upgrade precondition (no
live `ConvertToEc` markers, no `.ec.dat` / `.ec.commit` on any node) with a
fail-loud check at EN startup.

**Live old-scheme markers at upgrade.** Define this explicitly: either drain them
before upgrading (recommended — and required anyway if the nonce rides in a
widened struct), or have the new code recognise an old-shape marker and refuse to
act on it until an operator clears it.

---

## 8. Cost, cleanup, and the reconcile contract

- **Per extent the peak is unchanged** — `.dat` and the staged shard already
  coexist today; the flip is what used to end that. What changes is the
  *duration*.
- **Fleet-aggregate is not unchanged.** With cleanup stalled (manager down, node
  isolated, reconcile backlogged) every converting extent holds both forms for as
  long as the stall lasts. Conversions must therefore be **admission-gated on
  cleanup backlog**, and policy-driven EC arming should stay off until the
  cleanup driver exists (§10 step 5). Manual `force-ec-convert` meanwhile is
  acceptable with a loud WARN.
- **Cleanup is not "deletion only" on member nodes.** A shard holder must also
  repoint its entry and persist meta (§4.4). Only non-member cleanup is a pure
  delete.
- **The reconcile must be file-granular.** Today's answer is extent-granular
  ("which extents should you not hold"), which cannot express "keep the extent,
  drop the `.dat`". The response must carry, per extent, the `payload_location`
  and the node's assigned index; the EN derives its own file set from that. The
  same answer cleans rollback residue on member nodes.
- **Accounting**: shard bytes must be visible to `extent_bytes` (§4.4).

---

## 9. A related violation to audit

`stream_extent_from_sources:4742` truncates the destination to 0 before each
source attempt, and `handle_re_avali:6875` uses it to repair a *lagging existing
replica*. A crash mid-refill leaves a short file where a complete one used to be
— the "neither state" §1 forbids. The recovery peer-copy path already avoids this
via temp-then-publish (`peer_copy_full_extent_to_dat:7188`).

**Action:** determine whether `handle_re_avali`'s target can be a replica the
manager still counts (i.e. whether its `avali` bit can be set at that moment). If
it can, move that path to temp-then-publish. Tracked separately from this design.

---

## 10. Implementation order

Chosen to minimise the window in which both schemes are live. Steps 2–3 are
individually shippable and revert-safe; step 4 is the only commit where the two
schemes meet, and by then every reader, scanner, and cleaner understands both
shapes.

1. **Attempt nonce + reporter check against the CURRENT 2PC** (§4.1). Fixes
   today's exposure independently; everything later inherits it. **SHIPPED** —
   reporter check `8282712`, nonce below.
2. **Persistence + wire plumbing, inert** (§7): sibling etcd key defaulting to
   `InDat`, `ExtentInfo.payload_location`, `ReadBytesReq (location, index)`,
   missing-file error code. Nothing writes `InShardFile`; every read carries
   `InDat`; behaviour byte-identical. **SHIPPED.**

   Two things surfaced while building it that the design above did not
   anticipate, both load-bearing:

   - **The server batches reads by `extent_id` alone.** One batch resolves ONE
     fd and serves every slot from it, so two requests naming different payload
     files would have been answered out of one file. The grouping key had to
     widen to the file identity. This is the same class of bug as serving the
     wrong file directly — it just arrives through the batching path, which no
     read-path audit of "who names a file" would have caught.
   - **`InDat` must have ONE identity regardless of shard index.** A replicated
     read carries the slot it read from; if that index survived into the file
     identity, reads of one extent from different slots would look like
     different files and stop batching. `PayloadRef::for_extent` normalises the
     index away for `InDat`, where it names nothing.

   The client-direct SDK path needed no change: `extent_read_descriptor` already
   refuses `ec_converted` extents, and `InShardFile` is published only by the
   flip, which sets `ec_converted` — so a shard file is unreachable from a
   descriptor. That invariant is now checked explicitly there rather than
   relied upon silently.
3. **EN shard-holder model, inert** (§4.4): load/scan/delete/reconcile/df
   awareness of `.shard{i}`, the two-file entry, read-by-request-location with
   error-on-absent. Unit-testable with hand-planted files before any producer
   exists. **SHIPPED.**

   The reconcile needed no change to SEE shard-only extents: it reports whatever
   is in `self.extents`, and startup discovery now puts them there. What it
   still cannot express is "keep the extent, drop the `.dat`" — that is step 5.

   `shard_files` holds `index -> length` rather than a set plus a byte counter,
   so the footprint cannot drift from the file list. Shard fds are deliberately
   NOT in `FdLru`: the cache accounts one fd per extent, and a second cached fd
   per entry would silently over-commit the process budget.
4. **The scheme switch, one commit**: staging goes to `.shard{i}` (retaining every
   guard in §5), the commit phase stops being sent, `apply_ec_conversion_done`
   writes `InShardFile` (+ value-CAS, §4.5), and marker release on
   coordinator-offline is enabled for EC. Old-scheme repair code stays.
5. **File-granular reconcile + cleanup driver** (§8). Until this lands, flips are
   allowed but the space window is unbounded — keep policy-driven EC arming off.
6. **After a soak and one release boundary**: delete the commit phase and the
   `ec.commit` machinery (subject to §7), and `ec.prepared` only once the nonce
   has taken over its role.

This sits **after** the recovery-side work that establishes the same model on a
lower-risk path: `handle_require_recovery` becomes idempotent (complete ⇒ adopt,
incomplete ⇒ reset and rebuild); recovery's marker becomes a standing instruction
and its wall-clock sweep is deleted; offline/fence releases that node's markers;
membership-based reconcile with grace.

---

## 11. Test plan

**Unit (EN):**
- staged but not flipped → a read carrying `InDat` returns full data from `.dat`;
  the staged shard is unreachable through it.
- after the flip → a read carrying `(InShardFile, i)` returns shard *i*.
- a read naming a payload file the node does not hold **errors** — never falls
  back to the other file.
- legacy shape (shard in `.dat`, `InDat`) serves shard reads unchanged.
- startup discovery builds an entry from a shard file with no `.dat`; two shard
  files at different indices are both recorded.
- `remove_extent_files` / reconcile scans / `extent_bytes` all see `.shard{i}`.

**Unit / system (manager):**
- the flip requires all K+M staged confirmations **carrying the live nonce**; a
  stale-nonce or wrong-reporter report is refused and the marker retained.
- marker release before the flip leaves the extent fully replicated and readable;
  a later attempt with a *different* assignment succeeds.

**Crash / interleaving matrix — every row must self-heal:**

| scenario | expected |
|---|---|
| kill mid-encode on the coordinator | re-dispatch re-encodes; no state to reconcile |
| some targets staged, then kill before flip | re-stage the rest, or release + fresh assignment |
| coordinator dies permanently before flip | marker released on offline; fresh assignment; no probe |
| kill immediately after the flip txn | conversion is done; cleanup is idempotent |
| kill during cleanup / shard-holder restart mid-cleanup | startup discovery resolves; cleanup re-runs |
| **full-cluster stop/start mid-window** | shards discovered; extents servable |
| **stale cross-attempt `ec_done`** | refused by nonce + reporter; no flip |
| **prepare-skip under permuted reuse** | nonce mismatch forces a full re-stage |
| **(K,M) changed between attempts over residue** | stale-shape staging never adopted |
| delete / `punch_holes` of a flipped extent | shard files unlinked; no orphans |
| EC shard recovery under the new naming | rebuilds into `.shard{i}` |
| pre-upgrade `.ec.commit` state on an upgraded EN | resolved by retained replay (§7) |
| pre-upgrade etcd values | decode unchanged; default `InDat` |
| leader failover between staged-confirm and flip | marker replay → re-dispatch → adopt-report → flip |
| `FdLru` holding an unlinked fd across cleanup | no stale reads; fd released |

**Harnesses:** `crates/manager/src/ec_g4_wedge_harness.rs`; the live 3-EN
`scratchpad/ec_smoke.sh` extended with coordinator kill/SIGSTOP at each point
above; `scratchpad/recovery_smoke.sh` for the shared df/reconcile paths.
