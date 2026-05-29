//! F211-F: EC convert auto-abandon when the coord EN is fenced.
//!
//! After F211-C's `mgr_fence_node` persists the override + bumps
//! owner-lock revisions, this module sweeps the unified inflight ledger
//! for ConvertToEc markers whose `target_nodes[0]` (the coord) matches
//! the freshly-fenced node. For each match it atomically deletes the
//! ledger marker + writes an audit `ec_convert_advisory/<extent_id>`
//! breadcrumb. The advisory entry is what the OP policy script later
//! reads to decide whether to `force_ec_convert` reissue. F138's
//! in-memory exclusion is released by the same put-and-delete txn.
//!
//! **EC convert reissue is NOT automatic.** The advisory exists only
//! to surface the decision to the OP — the OP confirms the extent is
//! 3R-healthy (recovery finished) and explicitly calls
//! `force_ec_convert` with a fresh `new_eversion + target_nodes`.

use autumn_etcd::Op;
use autumn_rpc::manager_rpc::rkyv_encode;
use rkyv::{Archive, Deserialize, Serialize};

use crate::extent_inflight::{ExtentOpKind, EXTENT_INFLIGHT_PREFIX};
use crate::AutumnManager;

/// Etcd prefix for the EC convert advisory ledger. Written by
/// `auto_abandon_for_fenced_node` (this module) and by the
/// stale-marker sweep (`extent_inflight_stale_sweep_loop`).
pub const EC_CONVERT_ADVISORY_PREFIX: &str = "ec_convert_advisory/";

/// rkyv'd value of `ec_convert_advisory/<extent_id>`. Persistent because
/// the OP policy script may not poll for hours after the abandon fires.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrEcAdvisoryEntry {
    pub extent_id: u64,
    pub original_coord_node_id: u64,
    /// Reason short-code. `"fence_abandon"` for F211-F's auto-abandon
    /// path; `"stale_sweep"` for `extent_inflight_stale_sweep_loop`.
    pub reason: String,
    pub set_at: i64,
}

pub fn advisory_key(extent_id: u64) -> String {
    format!("{}{}", EC_CONVERT_ADVISORY_PREFIX, extent_id)
}

impl AutumnManager {
    /// F211-F: invoked from `handle_fence_node` AFTER the override has
    /// been persisted to etcd + F211-D's owner-lock revision bumps.
    ///
    /// Returns the list of extent_ids whose markers were abandoned, so
    /// the caller can audit the chain.
    pub(crate) async fn auto_abandon_for_fenced_node(&self, fenced_node: u64) -> Vec<u64> {
        // Snapshot under one borrow. We need the extent_ids whose
        // ledger payload is ConvertToEc AND whose target_nodes[0] is
        // the freshly-fenced coord. Also capture full target_nodes so
        // the Tier 2 fence-handover push below can reach each live
        // remote target.
        let abandoned: Vec<(u64, Vec<u64>)> = {
            let map = self.inflight.borrow();
            map.iter()
                .filter_map(|(eid, rec)| {
                    let (kind, payload) = rec.unpack()?;
                    if kind != ExtentOpKind::ConvertToEc {
                        return None;
                    }
                    let crate::extent_inflight::ExtentOpPayload::ConvertToEc(p) = payload else {
                        return None;
                    };
                    if p.target_nodes.first().copied() == Some(fenced_node) {
                        Some((*eid, p.target_nodes.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        };
        let abandoned_ids: Vec<u64> = abandoned.iter().map(|(id, _)| *id).collect();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        for (extent_id, target_nodes) in &abandoned {
            // Atomic etcd txn: delete the inflight marker + put the
            // advisory entry. The fence path will not be safe until
            // BOTH effects land, so we route via `txn_fenced`.
            let advisory = MgrEcAdvisoryEntry {
                extent_id: *extent_id,
                original_coord_node_id: fenced_node,
                reason: "fence_abandon".to_string(),
                set_at: now,
            };
            let key = advisory_key(*extent_id);
            let marker_key = format!("{}{}", EXTENT_INFLIGHT_PREFIX, extent_id);
            let bytes = rkyv_encode(&advisory).to_vec();
            if let Some(etcd) = &self.etcd {
                let ops = vec![
                    Op::put(key.as_bytes(), &bytes),
                    Op::delete(marker_key.as_bytes()),
                ];
                if let Err(e) = etcd.txn_fenced(vec![], ops, vec![]).await {
                    tracing::warn!(
                        extent_id = *extent_id,
                        fenced_node,
                        error = %e,
                        "F211-F: failed to abandon inflight marker; will retry next fence"
                    );
                    continue;
                }
            }
            // In-memory release follows etcd success.
            self.commit_extent_inflight_release(*extent_id);
            tracing::warn!(
                extent_id = *extent_id,
                fenced_node,
                "F211-F: auto-abandoned EC convert marker after fence — \
                 advisory persisted; OP policy script must decide \
                 whether to force_ec_convert reissue"
            );

            // F211-D Tier 2: push the post-fence owner-lock revision to
            // each live (non-fenced) target EN via `commit_length_on_node`.
            // The EN's `handle_check_commit_length` does fence-handover
            // when `req.revision > entry.owner_revision` — bumps and
            // persists `.meta`. After this, a ghost ex-coord whose
            // in-flight 2PC continues with the OLD revision will be
            // rejected by `handle_write_shard` / `handle_commit_ec_shard`
            // (`req.revision < entry.owner_revision → CODE_LOCKED_BY_OTHER`),
            // preventing it from overwriting `.dat` on remotes after the
            // marker has been abandoned.
            //
            // Best-effort: per-target failure is logged at WARN; the
            // existing `ec_convert_advisory` breadcrumb is the OP's
            // signal that manual inspection may be needed.
            self.push_fence_handover_to_targets(*extent_id, target_nodes, fenced_node)
                .await;
        }
        abandoned_ids
    }

    /// F211-D Tier 2: push the post-fence owner-lock revision to each
    /// live (non-fenced) target EN of an abandoned ConvertToEc marker.
    /// Uses `commit_length_on_node` (which the EN turns into a
    /// fence-handover bump of `entry.owner_revision`). Best-effort.
    async fn push_fence_handover_to_targets(
        &self,
        extent_id: u64,
        target_nodes: &[u64],
        fenced_node: u64,
    ) {
        // Look up: post-fence owner_lock revision for the partition that
        // owns this extent, and address for each non-fenced target.
        // Single borrow.
        struct Plan {
            revision: i64,
            targets: Vec<(u64, String)>,
        }
        let plan: Option<Plan> = {
            let s = self.store.inner.borrow();
            let mut revision: i64 = 0;
            'outer: for part in s.partitions.values() {
                let streams = [part.log_stream, part.row_stream, part.meta_stream];
                for sid in streams {
                    if s.streams
                        .get(&sid)
                        .map(|st| st.extent_ids.contains(&extent_id))
                        .unwrap_or(false)
                    {
                        let key = format!("partition/{}", part.part_id);
                        if let Some(&rev) = s.owner_revisions.get(&key) {
                            revision = rev;
                        }
                        break 'outer;
                    }
                }
            }
            if revision <= 0 {
                None
            } else {
                let targets: Vec<(u64, String)> = target_nodes
                    .iter()
                    .filter(|nid| **nid != fenced_node)
                    .filter_map(|nid| s.nodes.get(nid).map(|n| (*nid, n.address.clone())))
                    .collect();
                Some(Plan { revision, targets })
            }
        };
        let Some(plan) = plan else {
            return; // No owner-revision context (e.g. memory-only / dev).
        };
        for (node_id, addr) in &plan.targets {
            match self
                .commit_length_on_node(addr, extent_id, plan.revision)
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        extent_id,
                        node_id,
                        revision = plan.revision,
                        "F211-D Tier 2: pushed fence-handover to live target"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        extent_id,
                        node_id,
                        revision = plan.revision,
                        error = %e,
                        "F211-D Tier 2: fence-handover push failed; \
                         ghost ex-coord may still complete 2PC against this target. \
                         Manual check required (see ec_convert_advisory)"
                    );
                }
            }
        }
    }
}
