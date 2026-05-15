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

use crate::AutumnManager;

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
                        "F109 extent delete: max retries exhausted; orphan files will be reaped on node-startup reconcile",
                    );
                    // Even on exhaustion, release the ledger so the
                    // extent isn't blocked for future ops. The orphan
                    // files are reaped by the per-node startup reconcile.
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
                .put_and_delete_txn(
                    Vec::new(),
                    vec![Self::extent_inflight_key(extent_id)],
                )
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

    async fn try_delete_one(&self, addr: &str, extent_id: u64) -> bool {
        let payload = rkyv_encode(&ExtDeleteExtentReq { extent_id });
        // 10 s — DELETE_EXTENT is a single fs::remove pair on EN.
        // Bounded so the per-2 s sweep loop doesn't get stuck behind
        // one paged-out EN; the failed delete just gets retried on
        // the next sweep (already up to 60 retries).
        let resp = match self
            .conn_pool
            .call_timeout(addr, EXT_MSG_DELETE_EXTENT, payload, Duration::from_secs(10))
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
