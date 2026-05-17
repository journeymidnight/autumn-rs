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
        // the freshly-fenced coord.
        let abandoned: Vec<u64> = {
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
                        Some(*eid)
                    } else {
                        None
                    }
                })
                .collect()
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        for extent_id in &abandoned {
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
        }
        abandoned
    }
}
