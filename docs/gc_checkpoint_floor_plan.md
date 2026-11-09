# Plan: let compact safely advance the checkpoint / GC floor

Status: not yet implemented.

## 1. Goal and problem boundary

Goal: when the data in a WAL prefix is fully covered by the persisted SST set, compact can publish a new checkpoint and advance the replay/GC floor even with no further user writes, so that old sealed log extents eventually go through normal GC.

Keep the "never moves backward" guarantee from the earlier fix. Reclaiming space must not skip acknowledged writes that have not been flushed, and must not simply drop replay protection.

Confirmed current behavior:

- `background.rs::compaction_checkpoint_cursor`: when `durable_ckpt_vp != (0,0)` it is reused unchanged, for both real compacts and no-op compacts.
- After `do_compact` successfully publishes a checkpoint it only updates the SST set; it does not advance `durable_ckpt_vp` to a new safe boundary.
- flush, freeze and merged checkpoint each have a path that updates this cursor after a successful persist.
- GC only deletes non-empty extents strictly before the floor; even when the floor offset is at the end of an extent, the current extent-position check still protects that extent.
- The ratio/byte eligibility check in `gc_debt` is not the same full check as the final replay protection, which can produce an idle run of "debt present → GC → no eligible extent".

"compact has walked every SST" is not by itself proof that the WAL tail is safe. Pending/inflight writes, the active memtable, the immutable queue, flushes being built or not yet published, and GC relocation writes must all be part of the proof.

The lack of discard accounting for old-format inline WAL is a separate historical issue: this fix does not reconstruct old WAL positions from inline SSTs. A controlled forcegc cleanup of old extents is not the same as automatically back-filling the accounting.

## 2. Invariants that must hold

1. **Coverage**: every WAL record before the checkpoint cursor C that recovery needs is covered by the SSTs referenced by that checkpoint, or has been superseded by a persisted newer version or delete record. Live payloads pointed to by a VP remain protected by GC's liveness check.
2. **Advance after ACK**: the new table set is published and `durable_ckpt_vp` updated only after the checkpoint has been persisted successfully; on failure or an uncertain outcome, nothing advances and GC is not relaxed.
3. **Single snapshot**: the table set, the cursor and the fence floors form one consistent checkpoint under `publish_lock`; an old SST set must not be stitched together with the cursor of a flush that finished later.
4. **No regression**: within a log stream, old and new boundaries are compared by actual member position and offset, not by numeric extent ID; an existing durable cursor must not be pulled back by an old SST header.
5. **Write isolation**: writes after boundary C do not have to be included in this compact, but must not be mistaken for data that landed in an SST before C.
6. **Topology isolation**: when split/merge, owner fencing, tail rotation, GC and similar operations change log membership, an old proof must be re-validated or discarded; cursor comparisons must not be applied across streams or owner epochs.
7. **Recovery reachability**: the anchor of a new checkpoint must be locatable by recovery after a restart; the anchor must not be deleted first with a full replay relied on as the fallback.

## 3. Phase A: failing tests and ordering proof first

Scope: `crates/manager/tests/`, partition-server internal tests.

Build a controllable regression first:

1. Write and flush data, then produce a sealed WAL prefix that contains only records already covered by SSTs; no further user writes.
2. Leave the durable checkpoint in the old sealed extent, and run a major compact and a no-op compact.
3. Assert that the checkpoint/floor advances safely and the old prefix can be GC'd; a restart does not rely on full replay, and all acknowledged live keys and delete states are correct.

Add failpoints that pause at each of these steps: write ACK, memtable insert, flush upload, flush publish, compact publish, checkpoint ACK. First map out the actual lock and await order of the partition loop and flush/compact, and prove that the proposed barrier cannot end up waiting on the maintenance gate, the flush drain or GC relocation (or they on it).

Acceptance: the tests fail with the new behavior disabled; a wrong implementation that merely sets the cursor to the live tail must fail the unflushed-write / crash tests. Tests must not only assert that some field increased.

## 4. Phase B: an explicit proof of the "landed in SST" boundary

Scope: `partition-server/src/lib.rs`, `rpc_handlers.rs`, the partition loop / write / flush paths.

Implement an internal checkpoint barrier; do not reuse the externally visible split/merge freeze state directly:

1. Insert a boundary marker into the serial write dispatch path and drain the pending/inflight writes ahead of it, covering every WAL write source, including GC relocation.
2. Capture the committed WAL boundary C at this ordered point, rotating active if needed; confirm that no write that is "committed to the WAL but not yet visible in the memtable" can fall before C.
3. Flush and publish the immutables before C in their existing order; if the corresponding data is already all in SSTs, there is no need to produce a new non-empty SST and no need to wait for new user writes.
4. Requests that arrive during the short barrier are queued; once isolation is established, writes after C continue, so user traffic is not frozen for the whole of a large compact. The queue must be bounded.
5. Produce an internal proof containing the stream, owner epoch, boundary C and coverage state; on validation failure, timeout or topology change, do not advance, and fall back safely to the existing durable cursor.

Prefer reusing the ordering guarantees of the existing freeze/drain and the flush ACK path, but do not just check `active.is_empty()`, do not just hold `publish_lock`, and above all do not read the live tail as the checkpoint when coverage cannot be proven.

The concrete barrier implementation is decided by the ordering audit in Phase A; when the coverage proof does not hold, keep the old boundary rather than jumping heuristically.

## 5. Phase C: compact publishes a new checkpoint

Scope: `background.rs::do_compact`, `checkpoint_and_truncate_row_prefix`, `compaction_checkpoint_cursor`, the checkpoint publish helper.

- Establish the Phase B landed-in-SST boundary before choosing compact inputs. A major compact uses that boundary; a no-op compact can also publish the boundary and must not skip early just because no SST was rewritten.
- A minor compact does not force a full drain; it keeps using the latest safe durable boundary unless a valid new coverage proof already exists.
- Under `publish_lock`, re-read the latest durable cursor and the current SST set; replace the inputs with the compact output, and keep any new flush SSTs that were published successfully in the meantime.
- Compare the proven boundary and the current durable boundary only within the same valid log member order, and choose the safe value that does not regress. If a concurrent flush has already moved ahead, this compact's older C must not overwrite it.
- Pass the table set, the boundary and the fence floors to `save_table_locs_raw` together.
- After the checkpoint succeeds, update the SST set and `durable_ckpt_vp` in the same publish critical section; a successful no-op publish also updates that cell.
- The `(0,0)` fallback keeps the conservative recovery path; if the fallback produces a valid new checkpoint, the runtime cell should match it.
- Do not rewrite every SST header to advance the global checkpoint; old SST headers may keep their original content boundary.

Shortcuts that must not be taken: deriving a WAL byte cursor from the largest SST seq; taking the largest extent ID; advancing from not-yet-flushed `p.vp_*`; just taking a `max` of cursors without verifying that the table set covers them.

## 6. Phase D: resolve the conservative retention of an anchor at the end of an extent

A non-regressing checkpoint is not enough: `C=(sealed_extent, sealed_length)` still protects the whole extent under the current strict before-floor rule.

- If C's extent is authoritatively sealed, the offset exactly reaches sealed_length, and the same stream currently has a validated successor extent, the equivalent replay start may be normalized to `(successor,0)`.
- The normalized anchor is persisted together with the table set; only after the ACK may GC remove the old extent.
- Comparison and normalization both follow the actual stream order; non-monotonic IDs from split/merge, the existing recovery semantics for duplicate members, and concurrent membership changes must all be handled.
- If there is no successor, the old tail is still open, or C does not reach the end, keep the original anchor. This plan does not add a forced seal/roll to release the last open tail.
- Explicitly verify which anchor recovery picks for the new anchor; an old SST header must not pull an already confirmed durable boundary back down.

## 7. Phase E: unify GC debt / candidates / actual execution

Scope: the debt basis and GC candidate selection in `background.rs`; the forcegc advisory / diagnostics in `rpc_handlers.rs`; manager policy.

- Decide "currently selectable extents" from the same stream membership, authoritative seal state, effective durable floor and ratio/absolute byte thresholds.
- `gc_debt` no longer claims bytes known to be protected by the replay floor as currently reclaimable; if the total garbage is to be shown, keep raw dead / replay-protected dead as separate values, not silently merged into one meaning.
- Refresh/invalidate the debt basis whenever the floor or stream membership changes; an idle refresh must not restore an old value that includes protected bytes.
- Diagnostics, the forcegc advisory and execution use the same effective floor, including the durable checkpoint promotion; fix the current wording that describes what is actually "at/after" as "at/before".
- When the floor blocks a significant amount of garbage, expose the reason and the need for a checkpoint advance. Automatic actions are driven by boundary changes and a cooldown, so that idle GC runs are not just replaced by idle checkpoint runs.
- A split inheriting the same existing discard accounting is correct behavior; the raw debt of the partitions cannot be summed into physically reclaimable space. Add an inheritance test; this plan does not change it to split evenly by refs.

Phase E must not remove the replay guard on the execution side; even if the policy's accounting is wrong, execution continues to fail closed.

## 8. Separate follow-up: bounded retry on shared-reference CAS conflicts

This is a concurrent completion-rate issue, not a prerequisite for advancing the floor; it is committed separately.

- In the metadata mutation layer, on an explicit CAS rejection, rebuild the latest stream/extent snapshot, re-run the owner, membership, EC/recovery/topology guards, then submit the transaction again.
- Bounded retries with backoff; never reuse old refs/eversion; do not re-run a scan of a whole large extent to resolve a single refs update conflict.
- Owner fencing, unknown commit outcomes, CRC errors and ordinary timeouts are not blindly retried as CAS conflicts.
- Stay idempotent for extents that no longer belong to the stream; keep the reference-count CAS protection.
- Run GC concurrently from several shared holders and assert that in the end `refs == in_streams`, with no ghost refs and no premature physical deletion.

## 9. Regression and acceptance matrix

| Scenario | Must verify |
|---|---|
| No new user writes, everything already in SSTs | compact/no-op can advance the checkpoint; the eligible sealed prefix is eventually reclaimed |
| active / queued imm / flush inflight | The new floor does not cross data not covered by the checkpoint |
| Paused after WAL ACK, before memtable visibility | The barrier waits for that write and does not wrongly conclude empty |
| Concurrent flush during compact | The table set includes the new flush; the cursor neither regresses nor runs ahead |
| Checkpoint publish fails or ACK is uncertain | The in-memory floor is not updated early, no out-of-bounds GC runs; data is complete after restart |
| Crash immediately after checkpoint ACK | Recovery uses the new boundary; data and delete states are correct |
| Durable at the end of a sealed extent | The old extent can be removed only after successor anchor normalization succeeds |
| User writes continue after the boundary | A restart replays the full suffix and misses no acknowledged write |
| split / merge / fence / manager switchover | Old proofs are not used across topology/owner; cursor comparison does not depend on ID magnitude |
| All keys deleted, discard-only SST | discard/seq/checkpoint are kept; restart does not resurrect keys |
| Mixed inline and large VP | No fabricated accounting for old inline data; live VPs are migrated safely before GC releases them |
| replay-protected garbage | debt does not drive repeated idle GC runs; diagnostics state the blocking reason clearly |
| Consecutive splits after 50% discard | Child partitions inherit the existing 50% accounting; physical space is not the plain sum of child-partition debt |
| Concurrent modification of shared refs | CAS retries are bounded, refs match the actual stream count, deletion happens only after the last reference |

The existing related regressions must be run: `system_compact_ckpt_cursor`, `system_restart_replay_cursor`, `system_compact_unflushed_vp_head`, `system_compact_checkpoint_fail`, `system_crash_mid_compact`, `system_compact_settles_deletes`, `system_gc_multiversion_same_extent`, `system_gc_inline_wal`, `system_merge_single_checkpoint`, `system_merge_replay_reachability`, and the split/fence/VP series.

New tests reproduce first and fix second, with ablation against wrong implementations. System tests use release and an appropriate `RUST_MIN_STACK` as the repository already requires; passing fmt/clippy is not a substitute for consistency verification.

## 10. Commit order and cluster verification

Suggested separate commits: A (failing tests and ordering proof) → B/C/D (safe boundary and checkpoint advance) → E (debt/candidate consistency) → CAS retry.

1. Verify first on an isolated test cluster; on partitions with stable SSTs and no new writes, record: log membership, old/new checkpoint, raw/protected/eligible debt, refs, actual disk usage.
2. Then test sustained reads/writes + compact + GC + PS crash / manager switchover; separately verify ACKed keys, delete states, the recovery replay start, and service availability.
3. For extents shared by several partitions, wait until every holder has dropped its reference and the EN delete queue has finished, and confirm that physical space goes down; `reclaimed N extent(s)` must not be taken to mean disk space has been freed.
4. For boundary partitions that span several namespaces, verify live VPs; do not infer from a key count of 0 in one namespace that the whole partition/extent is empty.
5. New-write scenarios converge without manual forcegc; old unaccounted extents are handled by the established cleanup procedure and tracked separately as historical debt.

On any missing data, cursor running ahead/regressing, unlocatable anchor or refs inconsistency, stop adding maintenance actions immediately and preserve metadata/logs. Do not assume that rolling back to the old binary can safely read WAL already deleted by the new GC; a rollback must first prove that the old recovery accepts the new checkpoint. Prefer pausing reclamation and fixing forward.
