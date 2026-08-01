//! EC convert auto-abandon when the coord EN is fenced.
//!
//! After `mgr_fence_node` persists the override + bumps
//! owner-lock revisions, this module sweeps the unified inflight ledger
//! for ConvertToEc markers whose `target_nodes[0]` (the coord) matches
//! the freshly-fenced node. For each match it atomically deletes the
//! ledger marker + writes an audit `ec_convert_advisory/<extent_id>`
//! breadcrumb. The advisory entry is what the OP policy script later
//! reads to decide whether to `force_ec_convert` reissue. The
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
    /// Reason short-code. `"fence_abandon"` for the auto-abandon
    /// path; `"stale_sweep"` for `extent_inflight_stale_sweep_loop`.
    pub reason: String,
    pub set_at: i64,
}

pub fn advisory_key(extent_id: u64) -> String {
    format!("{}{}", EC_CONVERT_ADVISORY_PREFIX, extent_id)
}

impl AutumnManager {
    /// invoked from `handle_fence_node` AFTER the override has
    /// been persisted to etcd + owner-lock owner_epoch bumps.
    ///
    /// Returns the list of extent_ids whose markers were abandoned, so
    /// the caller can audit the chain.
    pub(crate) async fn auto_abandon_for_fenced_node(&self, fenced_node: u64) -> Vec<u64> {
        // Snapshot under one borrow: the extent_ids whose ledger payload is
        // ConvertToEc AND whose target_nodes[0] (the coordinator) is the
        // freshly-fenced node.
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
                    (p.target_nodes.first().copied() == Some(fenced_node)).then_some(*eid)
                })
                .collect()
        };
        let abandoned_ids: Vec<u64> = abandoned.clone();
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
                        "failed to abandon inflight marker; will retry next fence"
                    );
                    continue;
                }
            }
            // In-memory release follows etcd success.
            self.commit_extent_inflight_release(*extent_id);
            tracing::warn!(
                extent_id = *extent_id,
                fenced_node,
                "auto-abandoned EC convert marker after fence — \
                 advisory persisted; OP policy script must decide \
                 whether to force_ec_convert reissue"
            );

            // #3 (2026-06-15): the former "Tier 2 fence-handover push"
            // was DELETED here — it was DEAD CODE. It called
            // `commit_length_on_node` with the post-fence owner_epoch expecting
            // the EN's `handle_commit_length` to BUMP `entry.owner_epoch`
            // (handover) and thereby fence out a ghost ex-coordinator's in-flight
            // 2PC. But `handle_commit_length` has been CHECK-ONLY-NEVER-HANDOVER
            // since the 2026-05-29 three-concepts rule (a higher owner_epoch hits
            // the `>= → no-op, return length` branch and never stores it — proven
            // by `extent_node::ec3_fence_handover_tests`). So the push raised
            // nothing; the ghost was never fenced. Worse, the WARN log claimed a
            // protection it didn't provide. Keeping a dead defense is a false
            // sense of security, so it's removed.
            //
            // KNOWN RESIDUAL (accepted, NOT reproduced — deleting the dead push
            // does NOT worsen it; the push protected nothing). A coordinator
            // fenced mid-convert can keep its (alive) process sending
            // WriteShard / CommitEcShard to live targets — EC acts on SEALED
            // extents (no appends) and the EC write/commit handlers only CHECK
            // owner_epoch (never raise it), so the targets' fence is never
            // bumped. Why this is bounded (and why a speculative code gate is NOT
            // built in this revert-prone area until reproduced):
            //   • Ghost ALONE is a LOUD WEDGE, not silent corruption: it commits
            //     the targets at the convert's eversion N (= manager_old + 1),
            //     but after the abandon the manager stays at eversion_old +
            //     ec_converted=false. A read uses the manager's stale eversion →
            //     the target rejects it (`req.eversion < local` → EVERSION_
            //     MISMATCH) → reads fail LOUDLY; the OP reconciles. The eversion
            //     fence does its job.
            //   • Reissue-race harm is ROUTE-BOUNDED: read routing follows the
            //     manager's FINAL layout (the reissue's apply_ec_conversion_done
            //     sets replicates/parity); a ghost shard left on a node NOT in
            //     that layout is never routed to, and orphan reconcile reaps it.
            //   • The advisory above (`ec_convert_advisory/<id>`) is the OP
            //     breadcrumb: confirm the fenced EN is quiesced before reissuing
            //     `force_ec_convert`.
            // IF reproduced, the fix is a code gate (handle_force_ec_convert
            // refuses while an advisory is outstanding; OP clears it after
            // confirming quiesce) and/or a DEDICATED fence-bump RPC — NEVER
            // commit_length (that re-breaks the three-concepts rule and fences
            // out the LIVE PS, the exact bug that made it check-only).
        }
        abandoned_ids
    }
}
