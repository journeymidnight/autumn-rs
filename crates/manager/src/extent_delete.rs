//! F109 — physical extent file deletion when refs → 0.
//!
//! When `handle_stream_punch_holes` / `handle_truncate` decrement an
//! extent's refcount to 0, the snapshot of its replica set is captured
//! into `pending_extent_deletes` (a `VecDeque<PendingDelete>`). This
//! background loop then fans out `EXT_MSG_DELETE_EXTENT` over the
//! shared `ConnPool` to each replica address. Idempotent on the
//! receiver side, so retries from this loop are safe.
//!
//! Backstop: when retries exhaust (offline node never recovers in time),
//! the orphan files left on disk are reaped on the affected node's next
//! startup via the `MSG_RECONCILE_EXTENTS` round-trip.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use autumn_rpc::manager_rpc::*;

use crate::AutumnManager;

/// One outstanding delete the manager still needs to ship to one or
/// more replicas.
#[derive(Debug, Clone)]
pub(crate) struct PendingDelete {
    pub extent_id: u64,
    /// Already shard-routed addresses for every replica that still
    /// owes us an ack. Drained as each one acks. Entry leaves the
    /// queue when this Vec is empty.
    pub pending_addrs: Vec<String>,
    /// Sweep counter. After `MAX_ATTEMPTS` failed sweeps, drop the
    /// entry; reconcile path will catch the orphan on next node boot.
    pub attempts: u32,
}

/// At ~2s per sweep, 60 attempts = ~2 min retry window per replica.
const MAX_ATTEMPTS: u32 = 60;
const SWEEP_INTERVAL: Duration = Duration::from_secs(2);

impl AutumnManager {
    /// Push pending deletes into the queue. Called by `punch_holes` /
    /// `truncate` *after* the etcd commit succeeds, so a failed mirror
    /// never enqueues stale deletes.
    pub(crate) fn enqueue_pending_deletes(&self, deletes: Vec<PendingDelete>) {
        if deletes.is_empty() {
            return;
        }
        self.pending_extent_deletes.borrow_mut().extend(deletes);
    }

    pub(crate) async fn extent_delete_loop(self) {
        loop {
            compio::time::sleep(SWEEP_INTERVAL).await;
            if !self.leader.get() {
                continue;
            }

            // Snapshot the queue out of the RefCell so we never hold
            // the borrow across an await.
            let batch: Vec<PendingDelete> = {
                let mut q = self.pending_extent_deletes.borrow_mut();
                q.drain(..).collect()
            };
            if batch.is_empty() {
                continue;
            }

            let mut keep: VecDeque<PendingDelete> = VecDeque::new();
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
                    continue;
                }
                entry.pending_addrs = still_pending;
                entry.attempts += 1;
                if entry.attempts < MAX_ATTEMPTS {
                    keep.push_back(entry);
                } else {
                    tracing::warn!(
                        extent_id = entry.extent_id,
                        attempts = entry.attempts,
                        remaining_replicas = entry.pending_addrs.len(),
                        "F109 extent delete: max retries exhausted; orphan files will be reaped on node-startup reconcile",
                    );
                }
            }

            if !keep.is_empty() {
                let mut q = self.pending_extent_deletes.borrow_mut();
                for e in keep {
                    q.push_back(e);
                }
            }
        }
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
        let resp = match self
            .conn_pool
            .call(addr, EXT_MSG_DELETE_EXTENT, payload)
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
