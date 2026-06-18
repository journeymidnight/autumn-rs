//! F109 — physical extent file deletion when refs → 0.
//!
//! When `handle_stream_punch_holes` / `handle_truncate` decrement an
//! extent's refcount to 0, the snapshot of its replica set is captured
//! and passed to `enqueue_pending_deletes`. This background loop then
//! fans out `EXT_MSG_DELETE_EXTENT` over the shared `ConnPool` to each
//! replica address. Idempotent on the receiver side, so retries from
//! this loop are safe.
//!
//! F207-C: the persistence of "delete in flight on this extent" is now
//! the unified inflight ledger (`extent_inflight/<id>` etcd prefix +
//! `AutumnManager.inflight` in-memory map). The ledger entry's payload
//! carries a snapshot of `pending_addrs` so a new leader's
//! `replay_from_etcd` can restart the delete fanout after failover.
//! Per-attempt live state (pending_addrs as they ack, attempts
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

/// F210-G2: long-lived persisted retry queue for deletes whose primary
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
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrExtentDeleteRetry {
    pub extent_id: u64,
    pub pending_addrs: Vec<String>,
    pub attempts: u32,
    pub last_attempt_at: i64,
}

/// One outstanding delete the manager still needs to ship to one or
/// more replicas. F207-C: the etcd persistence side lives in the
/// unified inflight ledger as `ExtentOpPayload::Delete`; this in-memory
/// struct tracks per-attempt live state that does NOT survive failover.
#[derive(Debug, Clone)]
pub(crate) struct PendingDelete {
    pub extent_id: u64,
    /// Already shard-routed addresses for every replica that still
    /// owes us an ack. Drained as each one acks. Entry leaves
    /// `delete_progress` when this Vec is empty.
    pub pending_addrs: Vec<String>,
    /// Sweep counter. After `MAX_ATTEMPTS` failed sweeps, drop the
    /// entry; reconcile path will catch the orphan on next node boot.
    pub attempts: u32,
}

/// At ~2s per sweep, 60 attempts = ~2 min retry window per replica.
const MAX_ATTEMPTS: u32 = 60;
const SWEEP_INTERVAL: Duration = Duration::from_secs(2);

/// EXTENT10-AUTORECLAIM: how often the leader sweeps for orphan extents
/// (`refs == 0` per `extent_can_delete` but in NO stream). Named "both-zero"
/// historically (when the gate was `refs==0 && vp_table_refs==0`); since the
/// vp_table_refs removal the predicate is `refs == 0` alone.
const BOTH_ZERO_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// F210-G2: backoff cadence for the persisted retry loop. Each
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
    /// F207-C: enqueue pending deletes. Acquires a Delete entry in the
    /// unified inflight ledger (atomic CAS create_revision==0 + F149
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
            // Acquire ledger entry with a snapshot of pending_addrs.
            let payload = crate::extent_inflight::ExtentOpPayload::Delete(
                crate::extent_inflight::PersistedPendingDelete {
                    extent_id: d.extent_id,
                    pending_addrs: d.pending_addrs.clone(),
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
                        "F207-C: enqueue_pending_deletes saw existing inflight marker; \
                         relying on node-startup reconcile backstop"
                    );
                }
                Err(other) => return Err(other),
            }
        }
        Ok(())
    }

    /// EXTENT10-AUTORECLAIM: reclaim extents that reached `refs == 0`
    /// (per `extent_can_delete`) but are in NO stream. The normal refs-side
    /// delete trigger lives in `punch_holes` / `truncate`, which only inspect
    /// extents that are CURRENT stream members. An extent that lost its last
    /// stream membership without going through that path (e.g. a born-orphan,
    /// or a CoW-shared extent whose final referencing stream was rewritten
    /// out-of-band) sits at `refs == 0` with no path firing its delete — the
    /// extent-10 orphan class. Without this sweep it leaks on disk until manual
    /// reclaim.
    ///
    /// SAFETY — deleting on `refs == 0` is sound: every log_stream relocated
    /// its live values out before dropping the extent (relocate-then-punch,
    /// made correct by GC-VP-IDENTITY), so `refs == 0` ⇒ no LIVE ValuePointer
    /// points here. (This is the same invariant `punch_holes` relies on; the
    /// former `vp_table_refs == 0` second condition was a net for membership-
    /// accounting bugs and was removed — `extent_can_delete` is now `refs == 0`.)
    ///
    /// Mirrors `punch_holes`: fenced `extents/<id>` etcd delete → in-memory
    /// remove → `enqueue_pending_deletes` (acquires the F207 Delete marker;
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
                        "orphan sweep: extent is refs==0 yet STILL listed in a stream's \
                         extent_ids (refs accounting bug) — skipping reclaim, investigate"
                    );
                    continue;
                }
                if inflight.contains_key(eid) || progress.contains_key(eid) {
                    continue;
                }
                v.push((
                    PendingDelete {
                        extent_id: *eid,
                        pending_addrs: Self::snapshot_replica_addrs(&s.nodes, *eid, ex),
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
            //   between them is reaped by the F109 node-startup reconcile. A
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

    /// EXTENT10-AUTORECLAIM background loop (leader-only via the gate inside
    /// `extent_both_zero_sweep_once`; F228-supervised by `start_runtime_tasks`).
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

            // F207-C: drain the in-memory progress map (NOT the ledger).
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
                let addrs = std::mem::take(&mut entry.pending_addrs);
                for addr in addrs {
                    let acked = self.try_delete_one(&addr, entry.extent_id).await;
                    if !acked {
                        still_pending.push(addr);
                    }
                }
                if still_pending.is_empty() {
                    tracing::info!(
                        extent_id = entry.extent_id,
                        attempts = entry.attempts + 1,
                        "F109 extent delete: all replicas acked",
                    );
                    // F207-C: release the ledger entry atomically with
                    // the in-memory removal (which already happened via
                    // the drain at the top of this iteration).
                    self.release_delete_marker(entry.extent_id).await;
                    continue;
                }
                entry.pending_addrs = still_pending;
                entry.attempts += 1;
                if entry.attempts < MAX_ATTEMPTS {
                    keep.insert(entry.extent_id, entry);
                } else {
                    tracing::warn!(
                        extent_id = entry.extent_id,
                        attempts = entry.attempts,
                        remaining_replicas = entry.pending_addrs.len(),
                        "F109 extent delete: max retries exhausted in primary loop; \
                         moving to F210-G2 persisted retry queue",
                    );
                    // F210-G2: instead of abandoning to the
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
                        .persist_failed_delete(entry.extent_id, entry.pending_addrs.clone())
                        .await
                    {
                        tracing::error!(
                            extent_id = entry.extent_id,
                            error = %e,
                            "F210-G2: persist failed delete to etcd failed; \
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
                    "F207-C: failed to clear Delete inflight marker; \
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
    pub(crate) fn snapshot_replica_addrs(
        nodes: &HashMap<u64, MgrNodeInfo>,
        extent_id: u64,
        extent: &MgrExtentInfo,
    ) -> Vec<String> {
        let mut addrs = Vec::with_capacity(extent.replicates.len() + extent.parity.len());
        for nid in extent.replicates.iter().chain(extent.parity.iter()) {
            if let Some(n) = nodes.get(nid) {
                addrs.push(Self::shard_addr_for_extent(
                    &n.address,
                    &n.shard_ports,
                    extent_id,
                ));
            }
            // Missing node_id (deregistered) → silently skip; its files
            // are already unreachable.
        }
        addrs
    }

    /// F210-G2: persist a long-lived retry entry to etcd + the in-memory
    /// shadow. Idempotent: re-persisting an existing entry simply updates
    /// the `attempts` / `last_attempt_at` fields.
    pub(crate) async fn persist_failed_delete(
        &self,
        extent_id: u64,
        pending_addrs: Vec<String>,
    ) -> Result<(), AppError> {
        let entry = MgrExtentDeleteRetry {
            extent_id,
            pending_addrs,
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

    /// F210-G2: slow retry loop for entries persisted to the
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
                let addrs = std::mem::take(&mut entry.pending_addrs);
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
                        "F210-G2: persisted-retry delete finally acked on every replica",
                    );
                    self.failed_deletes.borrow_mut().remove(&extent_id);
                    if let Some(etcd) = &self.etcd {
                        let key = extent_delete_retry_key(extent_id);
                        if let Err(e) = etcd.put_and_delete_txn(Vec::new(), vec![key]).await {
                            tracing::warn!(
                                extent_id,
                                error = %e,
                                "F210-G2: failed to clear retry etcd key; will re-clear next tick"
                            );
                            // Re-insert so the next tick retries clearing.
                            self.failed_deletes.borrow_mut().insert(
                                extent_id,
                                MgrExtentDeleteRetry {
                                    extent_id,
                                    pending_addrs: Vec::new(),
                                    attempts: entry.attempts.saturating_add(1),
                                    last_attempt_at: now,
                                },
                            );
                        }
                    }
                    continue;
                }
                entry.pending_addrs = still_pending;
                entry.attempts = entry.attempts.saturating_add(1);
                entry.last_attempt_at = now;
                if let Some(etcd) = &self.etcd {
                    let key = extent_delete_retry_key(extent_id);
                    let value = autumn_rpc::manager_rpc::rkyv_encode(&entry).to_vec();
                    if let Err(e) = etcd.put_msgs_txn(vec![(key, value)]).await {
                        tracing::warn!(
                            extent_id,
                            error = %e,
                            "F210-G2: failed to update retry etcd key; in-memory still updated"
                        );
                    }
                }
                self.failed_deletes.borrow_mut().insert(extent_id, entry);
            }
        }
    }

    async fn try_delete_one(&self, addr: &str, extent_id: u64) -> bool {
        let payload = rkyv_encode(&ExtDeleteExtentReq { extent_id });
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
        match rkyv_decode::<ExtCodeResp>(&resp) {
            Ok(r) => r.code == CODE_OK,
            Err(_) => false,
        }
    }
}
