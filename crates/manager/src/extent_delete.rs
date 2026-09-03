//! physical extent file deletion when refs → 0.
//!
//! When `handle_stream_punch_holes` / `handle_truncate` decrement an
//! extent's refcount to 0, the snapshot of its replica set is captured
//! and passed to `enqueue_pending_deletes`. This background loop then
//! fans out `EXT_MSG_DELETE_EXTENT` over the shared `ConnPool` to each
//! replica address. Idempotent on the receiver side, so retries from
//! this loop are safe.
//!
//! the persistence of "delete in flight on this extent" is now
//! the unified inflight ledger (`extent_inflight/<id>` etcd prefix +
//! `AutumnManager.inflight` in-memory map). The ledger entry's payload
//! carries a snapshot of `pending_targets` so a new leader's
//! `replay_from_etcd` can restart the delete fanout after failover.
//! Per-attempt live state (pending_targets as they ack, attempts
//! counter) lives in `AutumnManager.delete_progress` — in-memory only;
//! a new leader's first attempt is its own "attempt 1", which is
//! correct.
//!
//! Backstop: when retries exhaust (offline node never recovers in time),
//! the orphan files left on disk are reaped on the affected node's next
//! startup via the `MSG_RECONCILE_EXTENTS` round-trip.

use std::collections::HashMap;
use std::time::Duration;

use autumn_common::error::AppError;
use autumn_rpc::manager_rpc::*;
use rkyv::{Archive, Deserialize, Serialize};

use crate::AutumnManager;

/// long-lived persisted retry queue for deletes whose primary
/// retry budget (`MAX_ATTEMPTS = 60`) was exhausted. Etcd prefix. Without
/// this, deleted-extent metadata in etcd already had refs=0 but the
/// physical files persisted on the offline replica until that node's
/// startup `MSG_RECONCILE_EXTENTS` round-trip eventually reaped them —
/// unbounded "free space leak" for any node that stayed down longer
/// than 2 minutes. Persisting lets the manager keep retrying with
/// backoff after failover or process restart.
pub(crate) const EXTENT_DELETE_RETRY_PREFIX: &str = "extentDeleteRetry/";

pub(crate) fn extent_delete_retry_key(extent_id: u64) -> String {
    format!("{EXTENT_DELETE_RETRY_PREFIX}{extent_id}")
}

/// rkyv-persisted payload for the retry queue. `attempts` carries the
/// accumulated retry count (used by the in-memory backoff calculator);
/// `last_attempt_at` is the unix-epoch seconds of the most recent try
/// (resets after a manager restart — recomputed retry timing is
/// equivalent to "try now" which is acceptable since the queue is
/// already a backstop path).
/// One replica a delete still owes an ack, named by BOTH where to send it and
/// WHICH node must execute it.
///
/// The address alone is not identity: this entry is persisted and retried for
/// as long as an hour, outliving the address's ownership. `node_uuid` is what
/// the target checks before unlinking, so a retry that survives its cluster
/// cannot destroy a different cluster's extent that happens to share the id.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct DeleteTarget {
    /// Shard-routed address for this extent.
    pub addr: String,
    /// Stable identity of the node that must execute it. Empty = the node had
    /// no registered uuid, so the target will skip its identity check.
    pub node_uuid: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrExtentDeleteRetry {
    pub extent_id: u64,
    pub pending_targets: Vec<DeleteTarget>,
    pub attempts: u32,
    pub last_attempt_at: i64,
}

/// One outstanding delete the manager still needs to ship to one or
/// more replicas. The etcd persistence side lives in the
/// unified inflight ledger as `ExtentOpPayload::Delete`; this in-memory
/// struct tracks per-attempt live state that does NOT survive failover.
#[derive(Debug, Clone)]
pub(crate) struct PendingDelete {
    pub extent_id: u64,
    /// Every replica that still owes us an ack, by address AND identity.
    /// Drained as each one acks. Entry leaves `delete_progress` when empty.
    pub pending_targets: Vec<DeleteTarget>,
    /// Sweep counter. After `MAX_ATTEMPTS` failed sweeps, drop the
    /// entry; reconcile path will catch the orphan on next node boot.
    pub attempts: u32,
}

/// At ~2s per sweep, 60 attempts = ~2 min retry window per replica.
const MAX_ATTEMPTS: u32 = 60;
const SWEEP_INTERVAL: Duration = Duration::from_secs(2);

/// EXTENT10-AUTORECLAIM: how often the leader sweeps for both-zero orphan
/// extents (`refs == 0 && vp_table_refs == 0` per `extent_can_delete`, but in
/// NO stream).
const BOTH_ZERO_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// How often the sealed-empty backstop runs.
const SEALED_EMPTY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Extents reclaimed per sealed-empty tick.
///
/// Rate-limited because the first tick on a cluster poisoned before the
/// writer-side reclaim landed has a large backlog to work through — tens of
/// thousands of extents on the one this was written for — and each reclaimed
/// extent costs an etcd CAS plus a delete fanout. Draining that over minutes is
/// deliberate; doing it in one tick would put a metadata storm in front of the
/// foreground path. Be concrete about what that buys and costs: at 64 per 60 s
/// tick, the ~40k backlog that motivated this drains in roughly ten HOURS, not
/// minutes. That is the intended trade for a backstop — the leak is already
/// there and stable, and the foreground path is not.
const SEALED_EMPTY_SWEEP_MAX_PER_TICK: usize = 64;

/// backoff cadence for the persisted retry loop. Each
/// `MgrExtentDeleteRetry` increments `attempts` once per attempt; the
/// backoff for the next try is `RETRY_BACKOFF_BASE * 2^min(attempts, MAX_SHIFT)`,
/// floor `RETRY_BACKOFF_BASE`, ceiling `RETRY_BACKOFF_MAX`. With base
/// = 60 s and max shift = 6, the schedule grows 60 s → 2 min → 4 min →
/// 8 min → 16 min → 32 min → 1 hr. Ceiling = 1 hr keeps the loop
/// retrying long-down extents without spamming.
const RETRY_LOOP_INTERVAL: Duration = Duration::from_secs(60);
const RETRY_BACKOFF_BASE_SECS: i64 = 60;
const RETRY_BACKOFF_MAX_SECS: i64 = 3600;
const RETRY_BACKOFF_MAX_SHIFT: u32 = 6;

fn retry_backoff_secs(attempts: u32) -> i64 {
    let shift = attempts.min(RETRY_BACKOFF_MAX_SHIFT);
    let v = RETRY_BACKOFF_BASE_SECS.saturating_mul(1i64 << shift);
    v.clamp(RETRY_BACKOFF_BASE_SECS, RETRY_BACKOFF_MAX_SECS)
}

fn epoch_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl AutumnManager {
    /// enqueue pending deletes. Acquires a Delete entry in the
    /// unified inflight ledger (atomic CAS create_revision==0 + leader
    /// fence) for each extent, and populates the in-memory
    /// `delete_progress` map with live attempt state.
    ///
    /// Called by `punch_holes` / `truncate` *after* the etcd commit that
    /// drops refs to 0 succeeds, so a failed mirror never enqueues
    /// stale deletes. On the orphan-cleanup path from
    /// `apply_recovery_done`, called after the Recovery marker has been
    /// released (the ledger is exclusive-per-extent).
    ///
    /// Errors:
    /// - `Precondition` from `acquire_extent_inflight` is downgraded to
    ///   a WARN log here: a Delete marker may have been left from a
    ///   prior leader's incomplete sweep (replay will surface it). The
    ///   caller has already mutated extent metadata (refs=0); the
    ///   eventual node-startup reconcile will reap orphans even if this
    ///   acquire fails.
    /// - `NotLeader` bubbles up.
    pub(crate) async fn enqueue_pending_deletes(
        &self,
        deletes: Vec<PendingDelete>,
    ) -> Result<(), AppError> {
        for d in deletes {
            // Acquire ledger entry with a snapshot of pending_targets.
            let payload = crate::extent_inflight::ExtentOpPayload::Delete(
                crate::extent_inflight::PersistedPendingDelete {
                    extent_id: d.extent_id,
                    pending_targets: d.pending_targets.clone(),
                },
            );
            match self.acquire_extent_inflight(d.extent_id, payload).await {
                Ok(()) => {
                    self.delete_progress.borrow_mut().insert(d.extent_id, d);
                }
                Err(AppError::Precondition(_)) => {
                    // Another op or stale Delete marker held. Skip — the
                    // node-startup reconcile is the backstop.
                    tracing::warn!(
                        extent_id = d.extent_id,
                        "enqueue_pending_deletes saw existing inflight marker; \
                         relying on node-startup reconcile backstop"
                    );
                }
                Err(other) => return Err(other),
            }
        }
        Ok(())
    }

    /// EXTENT10-AUTORECLAIM: reclaim extents that reached both-zero
    /// (`refs == 0 && vp_table_refs == 0`, per `extent_can_delete`) but are in
    /// NO stream. The refs-side delete trigger lives in `punch_holes` /
    /// `truncate`, which only inspect CURRENT stream members. An extent that
    /// lost its last membership out-of-band sits at `refs == 0` with no path
    /// firing its delete — the extent-10 orphan class. Without this sweep it
    /// leaks on disk until manual reclaim.
    ///
    /// SAFETY — deleting on both-zero is sound:
    /// - `refs == 0` ⇒ every log_stream relocated its live values out before
    ///   dropping the extent (relocate-then-punch, made correct by
    ///   GC-VP-IDENTITY), so no LIVE ValuePointer points here;
    /// - `vp_table_refs == 0` ⇒ no live SST physically references it.
    /// The `vp_table_refs == 0` half is now an UPGRADE-SAFETY GUARD (see
    /// `extent_can_delete`): the maintenance machinery is gone, so for extents
    /// managed under this build it is always 0, but a legacy `vp_table_refs > 0`
    /// extent (live VPs the old net protected) is correctly NOT reclaimed here
    /// until Stage 2's migration clears the field.
    ///
    /// Mirrors `punch_holes`: fenced `extents/<id>` etcd delete → in-memory
    /// remove → `enqueue_pending_deletes` (acquires the Delete marker;
    /// `extent_delete_loop` fans out the physical unlink). Returns the count
    /// reclaimed this pass.
    pub(crate) async fn extent_both_zero_sweep_once(&self) -> usize {
        if !self.leader.get() {
            return 0;
        }
        // Collect both-zero candidates not already in the inflight ledger or
        // the delete-progress map. Snapshot replica addrs AND the full extent
        // record (the value-CAS baseline) under the same borrow.
        let candidates: Vec<(PendingDelete, Vec<u8>)> = {
            let s = self.store.inner.borrow();
            let inflight = self.inflight.borrow();
            let progress = self.delete_progress.borrow();
            // coco P1 #2: this sweep reclaims ONLY extents in NO stream. Don't
            // trust `refs == 0` alone as a proxy for "no stream lists it" — a
            // refs under-count bug (e.g. the merge `saturating_sub` gap) could
            // leave `refs == 0` while a stream's `extent_ids` still references
            // it; deleting then would dangle the membership AND (since the
            // relocate-then-punch "refs==0 ⇒ live values relocated" guarantee
            // only holds for genuinely-dropped extents) risk dropping live data.
            // Verify against actual membership; on a mismatch skip + log loud.
            let stream_members: std::collections::HashSet<u64> = s
                .streams
                .values()
                .flat_map(|st| st.extent_ids.iter().copied())
                .collect();
            let mut v = Vec::new();
            for (eid, ex) in s.extents.iter() {
                if !Self::extent_can_delete(ex) {
                    continue;
                }
                if stream_members.contains(eid) {
                    tracing::error!(
                        extent_id = *eid,
                        "both-zero sweep: extent is refs==0 && vp_table_refs==0 yet STILL listed \
                         in a stream's extent_ids (refs accounting bug) — skipping reclaim, investigate"
                    );
                    continue;
                }
                if inflight.contains_key(eid) || progress.contains_key(eid) {
                    continue;
                }
                v.push((
                    PendingDelete {
                        extent_id: *eid,
                        pending_targets: Self::snapshot_replica_targets(&s.nodes, *eid, ex),
                        attempts: 0,
                    },
                    rkyv_encode(ex).to_vec(),
                ));
            }
            v
        };
        if candidates.is_empty() {
            return 0;
        }
        let mut reclaimed = 0usize;
        for (d, baseline) in candidates {
            let eid = d.extent_id;
            // Value-CAS the `extents/<id>` delete on the snapshot (coco P1):
            // a concurrent op (recovery / EC conversion eversion bump, or an
            // alloc re-adding the extent to a stream) can rewrite `extents/<id>`
            // during this await. An unguarded delete would then drop a record
            // that is no longer `refs == 0`. CAS on the snapshot ⇒ if anything
            // changed it since the snapshot, the txn is refused (Precondition)
            // and we skip — never removing from memory or enqueuing a physical
            // delete against a stale record. (etcd-first; memory-mode skips it.)
            // Matches the membership-write CAS pattern (manager note 33).
            //
            // Accepted residual (coco P2 #3), same bar as the rest of the
            // extent-state path:
            // - #3 atomicity: the `extents/<id>` delete and the Delete inflight
            //   marker are separate steps (same as `punch_holes`); a crash
            //   between them is reaped by the node-startup reconcile. A
            //   space-leak backstop, not data loss.
            if let Some(etcd) = &self.etcd {
                let key = format!("extents/{eid}");
                if let Err(e) = etcd
                    .put_delete_txn_cas(Vec::new(), vec![key.clone()], vec![(key, baseline)])
                    .await
                {
                    tracing::warn!(
                        extent_id = eid,
                        error = %e,
                        "both-zero sweep: CAS extent delete refused (concurrent change) \
                         or etcd error; retry next tick"
                    );
                    continue;
                }
            }
            self.store.inner.borrow_mut().extents.remove(&eid);
            if let Err(e) = self.forget_payload_location(eid).await {
                tracing::warn!(
                    extent_id = eid,
                    error = %e,
                    "both-zero sweep: could not drop the extent's payload-location key"
                );
            }
            // Same lifetime as the payload location: the extent is gone, so the
            // mark describing its slots must not outlive it.
            if let Err(e) = self.forget_corrupt_slots(eid).await {
                tracing::warn!(extent_id = eid, error = %e, "could not drop the corrupt-slot mark");
            }
            if let Err(e) = self.enqueue_pending_deletes(vec![d]).await {
                tracing::warn!(
                    extent_id = eid,
                    error = %e,
                    "both-zero sweep: enqueue_pending_deletes failed (node-startup \
                     reconcile is the backstop)"
                );
            }
            reclaimed += 1;
        }
        if reclaimed > 0 {
            tracing::info!(reclaimed, "both-zero sweep reclaimed orphan extent(s)");
        }
        reclaimed
    }

    /// Backstop for sealed-empty non-tail members the writer never reclaimed.
    ///
    /// The writer punches its own abandoned tail on roll-away, but that is
    /// client-side and best-effort: if the punch or the authoritative re-fetch
    /// fails, or the writer dies right after the roll, the extent stays
    /// `sealed=true, sealed_length=0` with refs >= 1 forever — a member of a
    /// stream, referenced by nothing, and invisible to GC, truncate and orphan
    /// reconcile alike. That shape is what leaked 10.4 TB against 222 GB of
    /// logical data on a live cluster.
    ///
    /// Reuses the punch-holes MUTATION — same `compute_extent_ref_drops`, same
    /// value-CAS'd `mirror_stream_extent_mutation`, same pending-delete queue —
    /// so this adds a new way to CHOOSE extents, not a second way to remove them.
    /// It does NOT inherit that handler's guards: there is no `ensure_owner_epoch`
    /// (the manager is not a fenced writer) and no empty-stream refusal (the tail
    /// exclusion makes it unreachable). The inflight refusal it DOES need is done
    /// per plan below.
    ///
    /// `pub` so an integration test can drive exactly one tick of it; the loop
    /// below is the only production caller.
    pub async fn sealed_empty_sweep_once(&self) -> usize {
        if !self.leader.get() {
            return 0;
        }
        let (ec_inflight, recovery_inflight) = self.inflight_snapshot_ec_recovery();

        // Plan under ONE borrow, with no await inside it: the mutation below
        // re-borrows, and the etcd CAS is what makes a concurrent change lose.
        let plans: Vec<(u64, MgrStreamInfo, Vec<u8>, std::collections::HashSet<u64>)> = {
            let s = self.store.inner.borrow();
            let mut budget = SEALED_EMPTY_SWEEP_MAX_PER_TICK;
            let mut out = Vec::new();
            for (sid, st) in s.streams.iter() {
                if budget == 0 {
                    break;
                }
                let members: Vec<SweepMember> = st
                    .extent_ids
                    .iter()
                    .map(|eid| {
                        // A member with no extent record is NOT swept: `sealed`
                        // defaults false, so it fails the predicate. That is a
                        // different leak and not this sweep's to guess at.
                        let ex = s.extents.get(eid);
                        SweepMember {
                            extent_id: *eid,
                            sealed: ex.is_some_and(|e| e.sealed),
                            sealed_length: ex.map_or(0, |e| e.sealed_length),
                            inflight: recovery_inflight.contains(eid)
                                || ec_inflight.contains(eid),
                        }
                    })
                    .collect();
                let mut cands = sealed_empty_sweep_candidates(&members);
                // A stream that lists the same extent twice would have its refs
                // UNDER-decremented: `retain` drops every occurrence while
                // `compute_extent_ref_drops` decrements once, leaving
                // `refs > 0, in no stream` — the exact orphan the sibling sweep
                // refuses to reap. New duplicates are prevented at the merge, but
                // this sweep's stated target is a cluster poisoned BEFORE that,
                // so refuse and say so rather than quietly make it worse.
                cands.retain(|id| {
                    let n = st.extent_ids.iter().filter(|m| *m == id).count();
                    if n > 1 {
                        tracing::error!(
                            stream_id = *sid,
                            extent_id = *id,
                            occurrences = n,
                            "sealed-empty sweep: extent listed more than once in one stream \
                             (refs accounting bug) — skipping reclaim, investigate"
                        );
                    }
                    n == 1
                });
                if cands.is_empty() {
                    continue;
                }
                cands.truncate(budget);
                budget -= cands.len();
                let removed: std::collections::HashSet<u64> = cands.into_iter().collect();
                let mut updated = st.clone();
                updated.extent_ids.retain(|id| !removed.contains(id));
                out.push((*sid, updated, rkyv_encode(st).to_vec(), removed));
            }
            out
        };

        let mut reclaimed = 0usize;
        for (stream_id, updated, baseline, removed) in plans {
            // Re-read the ledger HERE, not from the snapshot taken before the
            // loop. Recovery deliberately targets sealed-empty extents, and by
            // the second plan that snapshot is separated from this mutation by
            // the previous iteration's awaits — a marker acquired in that window
            // would be invisible. `handle_stream_punch_holes` has no equivalent
            // gap: it snapshots and refuses inside one synchronous borrow.
            // Nothing else guards this; the per-extent CAS compares the extent
            // record, and acquiring a ledger marker does not touch it.
            //
            // Untested, and not testable in-process: the plan-time predicate
            // already drops anything the pre-loop snapshot named, so this fires
            // only for a marker acquired DURING the sweep — and with no etcd the
            // awaits above do not actually suspend, so the harness cannot open
            // that window. Ablating this check leaves every test green. It is
            // here because the window is real against a live etcd, not because
            // something proves it fires.
            let drops = {
                let s = self.store.inner.borrow();
                let (ec_now, rec_now) = self.inflight_snapshot_ec_recovery();
                if removed.iter().any(|id| ec_now.contains(id) || rec_now.contains(id)) {
                    None
                } else {
                    Some(Self::compute_extent_ref_drops(&s, &removed, &ec_now))
                }
            };
            let Some((extent_puts, extent_deletes, pending_deletes, extent_cas)) = drops else {
                tracing::info!(
                    stream_id,
                    "sealed-empty sweep: an op claimed one of these extents mid-sweep — deferring"
                );
                continue;
            };
            // etcd first; on failure the in-memory store is untouched and the
            // next tick re-plans from whatever actually persisted.
            if let Err(e) = self
                .mirror_stream_extent_mutation(
                    &updated,
                    &extent_puts,
                    &extent_deletes,
                    Some(baseline),
                    extent_cas,
                )
                .await
            {
                tracing::warn!(
                    stream_id,
                    count = removed.len(),
                    error = %e,
                    "sealed-empty sweep: could not persist the membership drop; retrying next tick"
                );
                continue;
            }
            {
                let mut s = self.store.inner.borrow_mut();
                if let Some(st) = s.streams.get_mut(&stream_id) {
                    *st = updated.clone();
                }
                for ex in &extent_puts {
                    s.extents.insert(ex.extent_id, ex.clone());
                }
                for eid in &extent_deletes {
                    s.extents.remove(eid);
                }
            }
            for &eid in &extent_deletes {
                if let Err(e) = self.forget_payload_location(eid).await {
                    tracing::warn!(extent_id = eid, error = %e, "sealed-empty sweep: payload-location key survives the extent");
                }
                if let Err(e) = self.forget_corrupt_slots(eid).await {
                    tracing::warn!(extent_id = eid, error = %e, "sealed-empty sweep: corrupt-slot mark survives the extent");
                }
            }
            // Each enqueue is an etcd CAS via the inflight ledger, and its
            // errors are already downgraded to WARN inside `enqueue_pending_deletes`
            // so one failed acquire cannot abandon the rest of the batch. The
            // membership drop is already durable either way; an extent whose
            // fanout did not start is picked up by node-startup reconcile.
            let _ = self.enqueue_pending_deletes(pending_deletes).await;
            tracing::info!(
                stream_id,
                count = removed.len(),
                "sealed-empty sweep: reclaimed leaked non-tail members"
            );
            reclaimed += removed.len();
        }
        reclaimed
    }

    pub(crate) async fn sealed_empty_sweep_loop(self) {
        loop {
            compio::time::sleep(SEALED_EMPTY_SWEEP_INTERVAL).await;
            self.sealed_empty_sweep_once().await;
        }
    }

    /// EXTENT10-AUTORECLAIM background loop (leader-only via the gate inside
    /// `extent_both_zero_sweep_once`; supervised by `start_runtime_tasks`).
    pub(crate) async fn extent_both_zero_sweep_loop(self) {
        loop {
            compio::time::sleep(BOTH_ZERO_SWEEP_INTERVAL).await;
            self.extent_both_zero_sweep_once().await;
        }
    }

    pub(crate) async fn extent_delete_loop(self) {
        loop {
            compio::time::sleep(SWEEP_INTERVAL).await;
            if !self.leader.get() {
                continue;
            }

            // drain the in-memory progress map (NOT the ledger).
            // The ledger is the "delete is in flight" flag; live retry
            // state lives in-memory. On failover, the new leader's
            // `replay_from_etcd` rehydrates `delete_progress` from the
            // ledger's snapshot payloads with attempts=0.
            let batch: Vec<PendingDelete> = {
                let mut q = self.delete_progress.borrow_mut();
                q.drain().map(|(_, v)| v).collect()
            };
            if batch.is_empty() {
                continue;
            }

            let mut keep: HashMap<u64, PendingDelete> = HashMap::new();
            for mut entry in batch {
                let mut still_pending = Vec::new();
                let targets = std::mem::take(&mut entry.pending_targets);
                for target in targets {
                    let acked = self.try_delete_one(&target, entry.extent_id).await;
                    if !acked {
                        still_pending.push(target);
                    }
                }
                if still_pending.is_empty() {
                    tracing::info!(
                        extent_id = entry.extent_id,
                        attempts = entry.attempts + 1,
                        "extent delete: all replicas acked",
                    );
                    // release the ledger entry atomically with
                    // the in-memory removal (which already happened via
                    // the drain at the top of this iteration).
                    self.release_delete_marker(entry.extent_id).await;
                    continue;
                }
                entry.pending_targets = still_pending;
                entry.attempts += 1;
                if entry.attempts < MAX_ATTEMPTS {
                    keep.insert(entry.extent_id, entry);
                } else {
                    tracing::warn!(
                        extent_id = entry.extent_id,
                        attempts = entry.attempts,
                        remaining_replicas = entry.pending_targets.len(),
                        "extent delete: max retries exhausted in primary loop; \
                         moving to persisted retry queue",
                    );
                    // instead of abandoning to the
                    // node-startup reconcile backstop (unbounded
                    // free-space-leak window for any node that stays
                    // down >2 min), persist the pending addrs to the
                    // `extentDeleteRetry/` etcd prefix so the slow
                    // retry loop keeps trying with backoff across
                    // manager restart / leader failover. The inflight
                    // ledger marker is still released so future ops on
                    // the extent aren't blocked — the retry queue is
                    // an independent etcd record orthogonal to the
                    // inflight ledger.
                    if let Err(e) = self
                        .persist_failed_delete(entry.extent_id, entry.pending_targets.clone())
                        .await
                    {
                        tracing::error!(
                            extent_id = entry.extent_id,
                            error = %e,
                            "persist failed delete to etcd failed; \
                             will fall back to node-startup reconcile"
                        );
                    }
                    self.release_delete_marker(entry.extent_id).await;
                }
            }

            if !keep.is_empty() {
                let mut q = self.delete_progress.borrow_mut();
                for (eid, e) in keep {
                    q.insert(eid, e);
                }
            }
        }
    }

    /// Release a Delete marker from the ledger (both new key and the
    /// in-memory shadow). The `recoveryTasks/` legacy key does NOT need
    /// cleanup here — Delete never wrote to it.
    async fn release_delete_marker(&self, extent_id: u64) {
        if let Some(etcd) = &self.etcd {
            if let Err(e) = etcd
                .put_and_delete_txn(Vec::new(), vec![Self::extent_inflight_key(extent_id)])
                .await
            {
                tracing::warn!(
                    extent_id,
                    error = %e,
                    "failed to clear Delete inflight marker; \
                     will retry on next loop iteration if state is consistent"
                );
                return;
            }
        }
        self.commit_extent_inflight_release(extent_id);
    }

    /// Resolve every replica node-id of `extent` to a shard-routed
    /// address. Called from `handle_stream_punch_holes` /
    /// `handle_truncate` while still holding the store borrow that
    /// removes the extent — captures the address list before the
    /// in-memory record is gone.
    ///
    /// Takes `&HashMap<u64, MgrNodeInfo>` (not `&MetadataState`) so it
    /// composes with a concurrent `s.extents.get_mut(...)` partial
    /// borrow on the other side of the `MetadataState` struct.
    pub(crate) fn snapshot_replica_targets(
        nodes: &HashMap<u64, MgrNodeInfo>,
        extent_id: u64,
        extent: &MgrExtentInfo,
    ) -> Vec<DeleteTarget> {
        let mut addrs = Vec::with_capacity(extent.replicates.len() + extent.parity.len());
        for nid in extent.replicates.iter().chain(extent.parity.iter()) {
            if let Some(n) = nodes.get(nid) {
                addrs.push(DeleteTarget {
                    addr: Self::shard_addr_for_extent(&n.address, &n.shard_ports, extent_id),
                    // Empty for a node with no registered uuid (legacy /
                    // no `--advertise`); the target then skips its check.
                    node_uuid: n.node_uuid.clone(),
                });
            }
            // Missing node_id (deregistered) → silently skip; its files
            // are already unreachable.
        }
        addrs
    }

    /// persist a long-lived retry entry to etcd + the in-memory
    /// shadow. Idempotent: re-persisting an existing entry simply updates
    /// the `attempts` / `last_attempt_at` fields.
    pub(crate) async fn persist_failed_delete(
        &self,
        extent_id: u64,
        pending_targets: Vec<DeleteTarget>,
    ) -> Result<(), AppError> {
        let entry = MgrExtentDeleteRetry {
            extent_id,
            pending_targets,
            attempts: 0,
            last_attempt_at: epoch_seconds(),
        };
        if let Some(etcd) = &self.etcd {
            let key = extent_delete_retry_key(extent_id);
            let value = autumn_rpc::manager_rpc::rkyv_encode(&entry).to_vec();
            etcd.put_msgs_txn(vec![(key, value)]).await?;
        }
        self.failed_deletes.borrow_mut().insert(extent_id, entry);
        Ok(())
    }

    /// slow retry loop for entries persisted to the
    /// `extentDeleteRetry/` etcd prefix. Wakes every
    /// `RETRY_LOOP_INTERVAL` (1 min). For each entry whose `last_attempt_at`
    /// + per-entry exponential backoff has elapsed, retries every
    /// remaining replica address via `try_delete_one`. On success
    /// (every replica acked), deletes the etcd key. On partial / total
    /// failure, increments `attempts` and updates `last_attempt_at` so
    /// the next attempt is further out.
    ///
    /// Cleanup on failover: a new leader's `replay_from_etcd` rehydrates
    /// `failed_deletes` from the etcd prefix; `attempts` is reset to 0
    /// implicitly (we trust the persisted counter), `last_attempt_at`
    /// stays as written so the new leader respects the in-flight
    /// backoff window. If the new leader wakes inside the backoff
    /// window, the first tick is a no-op for that entry and the next
    /// tick after the window expires picks it up.
    pub(crate) async fn extent_delete_retry_loop(self) {
        loop {
            compio::time::sleep(RETRY_LOOP_INTERVAL).await;
            if !self.leader.get() {
                continue;
            }

            let now = epoch_seconds();
            let due: Vec<MgrExtentDeleteRetry> = {
                let queue = self.failed_deletes.borrow();
                queue
                    .values()
                    .filter(|e| now - e.last_attempt_at >= retry_backoff_secs(e.attempts))
                    .cloned()
                    .collect()
            };
            if due.is_empty() {
                continue;
            }

            for mut entry in due {
                let extent_id = entry.extent_id;
                let mut still_pending = Vec::new();
                let addrs = std::mem::take(&mut entry.pending_targets);
                for addr in addrs {
                    let acked = self.try_delete_one(&addr, extent_id).await;
                    if !acked {
                        still_pending.push(addr);
                    }
                }
                if still_pending.is_empty() {
                    tracing::info!(
                        extent_id,
                        attempts = entry.attempts + 1,
                        "persisted-retry delete finally acked on every replica",
                    );
                    self.failed_deletes.borrow_mut().remove(&extent_id);
                    if let Some(etcd) = &self.etcd {
                        let key = extent_delete_retry_key(extent_id);
                        if let Err(e) = etcd.put_and_delete_txn(Vec::new(), vec![key]).await {
                            tracing::warn!(
                                extent_id,
                                error = %e,
                                "failed to clear retry etcd key; will re-clear next tick"
                            );
                            // Re-insert so the next tick retries clearing.
                            self.failed_deletes.borrow_mut().insert(
                                extent_id,
                                MgrExtentDeleteRetry {
                                    extent_id,
                                    pending_targets: Vec::new(),
                                    attempts: entry.attempts.saturating_add(1),
                                    last_attempt_at: now,
                                },
                            );
                        }
                    }
                    continue;
                }
                entry.pending_targets = still_pending;
                entry.attempts = entry.attempts.saturating_add(1);
                entry.last_attempt_at = now;
                if let Some(etcd) = &self.etcd {
                    let key = extent_delete_retry_key(extent_id);
                    let value = autumn_rpc::manager_rpc::rkyv_encode(&entry).to_vec();
                    if let Err(e) = etcd.put_msgs_txn(vec![(key, value)]).await {
                        tracing::warn!(
                            extent_id,
                            error = %e,
                            "failed to update retry etcd key; in-memory still updated"
                        );
                    }
                }
                self.failed_deletes.borrow_mut().insert(extent_id, entry);
            }
        }
    }

    async fn try_delete_one(&self, target: &DeleteTarget, extent_id: u64) -> bool {
        let payload = rkyv_encode(&DeleteExtentReq {
            extent_id,
            node_uuid: target.node_uuid.clone(),
        });
        let addr = target.addr.as_str();
        // 10 s — DELETE_EXTENT is a single fs::remove pair on EN.
        // Bounded so the per-2 s sweep loop doesn't get stuck behind
        // one paged-out EN; the failed delete just gets retried on
        // the next sweep (already up to 60 retries).
        let resp = match self
            .conn_pool
            .call_timeout(
                addr,
                EXT_MSG_DELETE_EXTENT,
                payload,
                Duration::from_secs(10),
            )
            .await
        {
            Ok(v) => v,
            Err(_) => return false,
        };
        match rkyv_decode::<autumn_rpc::extent_rpc::CodeResp>(&resp) {
            Ok(r) => r.code == CODE_OK,
            Err(_) => false,
        }
    }
}

/// One stream member, as the sealed-empty sweep sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SweepMember {
    pub extent_id: u64,
    pub sealed: bool,
    pub sealed_length: u64,
    /// Currently named by the inflight ledger (recovery / EC conversion).
    pub inflight: bool,
}

/// Which members a sealed-empty sweep may reclaim, given a stream's `extent_ids`
/// IN ORDER.
///
/// A sealed-at-zero extent should hold no acked byte — under caller-ack ⊆ commit
/// and seal ≥ acked — so nothing should reference it, and nothing looks at it
/// either: it is invisible to accounting, GC and truncate alike. That is both
/// what makes it reclaimable and what made it leak. The writer reclaims its own
/// on roll-away, best-effort; this is the backstop for the ones that got away (a
/// punch that failed, a writer that died between the seal and the punch) and for
/// the backlog on a cluster poisoned before that fix.
///
/// Two things to be honest about, because "nothing can reference it" is a claim
/// about the ABSENCE of a bug class rather than a structural property:
///  - The repo documents under-seal bugs that produced `sealed=true,
///    sealed_length=0` on extents that DID hold acked data (the split CoW-tail
///    seal, `stale_vp_offset_past_sealed_length`). Those are believed fixed. If
///    one recurs, this sweep changes the failure from a loud wedge with the
///    bytes still on disk into an unlink within a minute.
///  - The predicate is the same branch PS GC already takes — `gc_extent_punchable`
///    punches `sealed_length == 0` unconditionally, ahead of the replay floor. So
///    the hazard is not new. What IS new is scope: GC walks one partition's
///    log_stream, this walks every stream, including partitions whose PS is gone
///    or whose GC is stalled.
///
/// Three exclusions, each load-bearing:
///  - **the tail is never swept.** It is the live append target; `sealed=false`
///    normally, but a tail sealed at 0 is a roll that has not landed yet, and
///    the writer's own reclaim owns that case. Skipping it also keeps the stream
///    non-empty, which the membership mutation requires.
///  - **`sealed_length > 0` is never swept**, and neither is an UNSEALED member:
///    only an authoritative empty seal proves there is nothing to lose. An open
///    extent reports `sealed_length == 0` while holding data.
///  - **an inflight-ledger extent is deferred**, not skipped forever — a
///    recovery or EC conversion naming it may be about to write.
pub(crate) fn sealed_empty_sweep_candidates(members: &[SweepMember]) -> Vec<u64> {
    let Some((_tail, rest)) = members.split_last() else {
        return Vec::new();
    };
    rest.iter()
        .filter(|m| m.sealed && m.sealed_length == 0 && !m.inflight)
        .map(|m| m.extent_id)
        .collect()
}

#[cfg(test)]
mod sealed_empty_sweep_tests {
    use super::*;

    fn m(extent_id: u64, sealed: bool, sealed_length: u64, inflight: bool) -> SweepMember {
        SweepMember {
            extent_id,
            sealed,
            sealed_length,
            inflight,
        }
    }

    #[test]
    fn a_sealed_empty_non_tail_member_is_swept() {
        let members = [m(1, true, 0, false), m(2, true, 4096, false), m(3, false, 0, false)];
        assert_eq!(sealed_empty_sweep_candidates(&members), vec![1]);
    }

    #[test]
    fn the_tail_is_never_swept_even_sealed_at_zero() {
        // A tail sealed at 0 is a roll that has not landed; the writer's own
        // reclaim owns it. Sweeping it would also empty a single-member stream.
        let members = [m(1, true, 4096, false), m(2, true, 0, false)];
        assert!(sealed_empty_sweep_candidates(&members).is_empty());
        assert!(sealed_empty_sweep_candidates(&[m(9, true, 0, false)]).is_empty());
        assert!(sealed_empty_sweep_candidates(&[]).is_empty());
    }

    #[test]
    fn a_sealed_member_with_bytes_is_never_swept() {
        let members = [m(1, true, 1, false), m(2, true, 0, false)];
        // Only the tail is left out here, so 1 is judged on its own merits.
        assert!(!sealed_empty_sweep_candidates(&members).contains(&1));
    }

    #[test]
    fn an_unsealed_member_is_never_swept() {
        // An OPEN extent reports sealed_length == 0 while holding data — the one
        // shape that must never be mistaken for an empty seal.
        let members = [m(1, false, 0, false), m(2, true, 0, false)];
        assert!(!sealed_empty_sweep_candidates(&members).contains(&1));
    }

    #[test]
    fn an_inflight_member_is_deferred() {
        let members = [m(1, true, 0, true), m(2, true, 0, false), m(3, true, 8, false)];
        // 2 goes; 1 waits for the recovery / EC conversion naming it.
        assert_eq!(sealed_empty_sweep_candidates(&members), vec![2]);
    }

    #[test]
    fn sweeping_can_never_empty_a_stream() {
        // Every member sealed-at-zero: the tail still survives, so the
        // membership mutation cannot produce the empty stream it rejects.
        let members: Vec<SweepMember> = (1..=5).map(|i| m(i, true, 0, false)).collect();
        let swept = sealed_empty_sweep_candidates(&members);
        assert_eq!(swept.len(), members.len() - 1);
        assert!(!swept.contains(&5));
    }
}
