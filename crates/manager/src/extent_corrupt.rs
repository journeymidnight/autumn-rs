//! Which of an extent's slots hold bytes a partition owner PROVED wrong.
//!
//! `avali` says a slot is not serving. It does not say WHY, and the two reasons
//! need opposite handling:
//!
//! - **behind** — the replica is short. `re_avali` refetches the missing tail.
//! - **corrupt** — the replica is full-length and wrong. `re_avali` cannot fix
//!   it; its entire test is `local_len >= sealed_length`, which a bit-rotted
//!   copy passes. Only a rebuild replaces the bytes.
//!
//! Without somewhere to record the difference, a corrupt report clears the bit
//! and nothing else, and the dispatch loop reads a dark slot as behind: it
//! catches it up in place with `re_avali`, whose length check the rotted copy
//! passes, and puts it back in service. The mark makes the loop rebuild the
//! slot instead, and it is written in the same transaction as the isolation so
//! there is no moment when the slot is dark without it.
//!
//! Kept in a sibling key rather than widening `ExtentRecord`, following
//! `extent_layout`: that struct is the persisted `extents/<id>` value, and
//! growing it would make every stored extent fail rkyv validation on replay,
//! which refuses leadership rather than degrading.

use std::collections::HashMap;

use autumn_common::AppError;

use crate::AutumnManager;
use autumn_rpc::manager_rpc::{CODE_PRECONDITION};
use crate::persist::records::ExtentRecord;

pub(crate) const EXTENT_CORRUPT_PREFIX: &str = "extentCorrupt/";

pub(crate) fn extent_corrupt_key(extent_id: u64) -> String {
    format!("{EXTENT_CORRUPT_PREFIX}{extent_id}")
}

/// What isolating the reported replicas of one extent would do.
///
/// Extracted so the two evidence sources — a partition server that failed a WAL
/// CRC during replay, and an extent node whose scrub found its own bytes rotted
/// — decide identically. They arrive by different routes and carry different
/// authority, but what may be darkened, and when refusing is mandatory, is a
/// property of the EXTENT, not of who is speaking.
#[derive(Debug)]
pub(crate) enum IsolationOutcome {
    /// Clear these bits and bump eversion.
    Isolate {
        updated: ExtentRecord,
        cleared_mask: u32,
    },
    /// The reported slots are already dark — a retried report after the first
    /// isolation landed. Success, not a no-op to be confused with a refusal.
    AlreadyIsolated,
    /// Not now: the extent is mid-change (an op in flight) or has changed
    /// since the finding was made (its eversion moved). Nothing about the
    /// finding is judged; the same copy checked again, once the extent
    /// settles, decides. Answered as `CODE_PRECONDITION` on the RPC path.
    Stale { message: String },
    /// `code` is what the RPC entry point answers with; the heartbeat entry
    /// point only logs `message`. Both are kept so neither caller has to
    /// re-derive the other's half.
    Refused { code: u8, message: String },
}

/// What became of a node's finding that its own copy is rotted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RotFinding {
    /// Decided: isolated, already isolated, or refused for good (the last
    /// copy, too few EC shards, not a member).
    Settled,
    /// Not judged yet (`IsolationOutcome::Stale`, or the isolation could not
    /// be written or applied): the copy must be checked again.
    Stale,
}

/// Decide whether the named replicas of `ex` may be isolated.
///
/// `reported_eversion` is what the reporter saw. Every refusal below is
/// load-bearing: an eversion that moved means the finding describes content
/// that may have been replaced (so is `Stale`: checked again, not believed); an OPEN tail cannot be isolated without a
/// seal-and-roll; and clearing the LAST available bit would make the extent
/// unreadable, which is worse than serving a copy known to be damaged.
///
/// On an EC extent a slot is one shard, and its `avali` bit is that shard's
/// availability — which is what a finding about the shard's own bytes is
/// about. "Last" there means K: below K serving shards nothing can be
/// reconstructed, so the damaged shard's range would go from wrong to gone.
pub(crate) fn compute_corrupt_isolation(
    ex: &ExtentRecord,
    corrupt_node_ids: &[u64],
    reported_eversion: u64,
    op_in_flight: bool,
) -> IsolationOutcome {
    // An extent with a stream-layer op in flight is mid-change: its membership,
    // its eversion and which file holds its payload are all being rewritten by
    // something this decision cannot see. Isolating into that window bumps the
    // eversion out from under the op — an EC conversion then fails its
    // value-CAS, re-reports, and the layout flip recomputes from the
    // post-isolation baseline, where its PINNED `new_eversion` is no longer one
    // above what the extent now holds. The flip lands with the eversion
    // unchanged across a replicated→EC layout change, and every client caching
    // that layout has nothing to tell it to refetch. Deferring costs a
    // re-check: a scrub finding is checked again once the extent settles
    // (`ScrubOp::recheck`), and rot does not heal.
    //
    // This mirrors `handle_reconcile_extents`, which withholds a verdict for
    // exactly the same reason.
    if op_in_flight {
        return IsolationOutcome::Stale {
            message: format!(
                "extent {} has a stream-layer op in flight; isolating now would move the \
                 eversion under it",
                ex.extent_id
            ),
        };
    }
    if ex.eversion != reported_eversion {
        return IsolationOutcome::Stale {
            message: format!(
                "extent {} eversion moved ({} != reported {}) — the report describes \
                 content that has since been replaced",
                ex.extent_id, ex.eversion, reported_eversion
            ),
        };
    }
    if !ex.sealed {
        return IsolationOutcome::Refused {
            code: CODE_PRECONDITION,
            message: format!(
                "extent {} is OPEN; isolation on an unsealed tail needs seal-and-roll",
                ex.extent_id
            ),
        };
    }
    let mut updated = ex.clone();
    let slots: Vec<u64> = updated
        .replicates
        .iter()
        .chain(updated.parity.iter())
        .copied()
        .collect();
    let mut cleared_mask = 0u32;
    let mut found = 0u32;
    for nid in corrupt_node_ids {
        if let Some(slot) = slots.iter().position(|s| s == nid) {
            found += 1;
            // `avali` is u32 — never shift past its width on a malformed or
            // future-wider layout.
            if slot >= 32 {
                continue;
            }
            let bit = 1u32 << slot;
            if updated.avali & bit != 0 {
                updated.avali &= !bit;
                cleared_mask |= bit;
            }
        }
    }
    if found == 0 {
        return IsolationOutcome::Refused {
            code: CODE_PRECONDITION,
            message: format!(
                "none of {corrupt_node_ids:?} are replicas of extent {} (slots {slots:?}) — \
                 stale layout",
                ex.extent_id
            ),
        };
    }
    if cleared_mask == 0 {
        return IsolationOutcome::AlreadyIsolated;
    }
    if ex.ec_converted {
        let data_shards = ex.replicates.len() as u32;
        if updated.avali.count_ones() < data_shards {
            return IsolationOutcome::Refused {
                code: CODE_PRECONDITION,
                message: format!(
                    "refusing to isolate shard(s) of EC extent {}: {} would remain available, \
                     below the {data_shards} needed to reconstruct — unrecoverable",
                    ex.extent_id,
                    updated.avali.count_ones()
                ),
            };
        }
    } else if updated.avali == 0 {
        return IsolationOutcome::Refused {
            code: CODE_PRECONDITION,
            message: format!(
                "refusing to isolate the last available replica(s) of extent {} — \
                 unrecoverable",
                ex.extent_id
            ),
        };
    }
    updated.eversion += 1;
    IsolationOutcome::Isolate {
        updated,
        cleared_mask,
    }
}

impl AutumnManager {
    /// Slots of `extent_id` proven corrupt, as a bitmap. Absent ⇒ none.
    pub(crate) fn corrupt_slots_of(&self, extent_id: u64) -> u32 {
        self.extent_corrupt_slots
            .borrow()
            .get(&extent_id)
            .copied()
            .unwrap_or(0)
    }

    /// Is this specific slot known corrupt?
    pub(crate) fn slot_is_corrupt(&self, extent_id: u64, slot: usize) -> bool {
        slot < 32 && (self.corrupt_slots_of(extent_id) & (1u32 << slot)) != 0
    }

    /// Record `bits` as corrupt, ORed onto what is already known. Etcd-first:
    /// memory is only updated once the key is durable, so the loop never
    /// dispatches a rebuild justified by a reason that did not survive.
    pub(crate) async fn mark_slots_corrupt(
        &self,
        extent_id: u64,
        bits: u32,
    ) -> Result<(), AppError> {
        if bits == 0 {
            return Ok(());
        }
        let merged = self.corrupt_slots_of(extent_id) | bits;
        if let Some(etcd) = &self.etcd {
            let key = extent_corrupt_key(extent_id);
            etcd.put_and_delete_txn(vec![(key, merged.to_le_bytes().to_vec())], vec![])
                .await?;
        }
        // OR, not overwrite: another report may have landed during the write.
        *self
            .extent_corrupt_slots
            .borrow_mut()
            .entry(extent_id)
            .or_insert(0) |= merged;
        Ok(())
    }

    /// Persist an isolation and the reason for it in ONE transaction, and
    /// return the merged bitmap for the caller to install alongside the
    /// extent once its verify-at-apply passes.
    ///
    /// Two writes left a window — the isolation landed, the mark did not —
    /// where a slot was dark with nothing saying why. The dispatch loop reads
    /// an unmarked dark slot as BEHIND and catches it up in place with
    /// `re_avali`, whose whole test is length: a full-length rotted copy
    /// passes it and goes back into service.
    pub(crate) async fn persist_isolation_with_mark(
        &self,
        updated: &ExtentRecord,
        cleared_mask: u32,
    ) -> Result<u32, AppError> {
        let merged = self.corrupt_slots_of(updated.extent_id) | cleared_mask;
        if let Some(etcd) = &self.etcd {
            etcd.put_and_delete_txn(
                vec![
                    (
                        format!("extents/{}", updated.extent_id),
                        crate::persist::encode(updated),
                    ),
                    (
                        extent_corrupt_key(updated.extent_id),
                        merged.to_le_bytes().to_vec(),
                    ),
                ],
                vec![],
            )
            .await?;
        }
        Ok(merged)
    }

    /// Clear one slot's corrupt mark — the rebuild that replaces those bytes has
    /// landed. Dropping the whole key once the last bit clears keeps this from
    /// growing without bound on a long-lived cluster.
    pub(crate) async fn clear_corrupt_slot(
        &self,
        extent_id: u64,
        slot: usize,
    ) -> Result<(), AppError> {
        if slot >= 32 {
            return Ok(());
        }
        let cur = self.corrupt_slots_of(extent_id);
        let next = cur & !(1u32 << slot);
        if next == cur {
            return Ok(());
        }
        if let Some(etcd) = &self.etcd {
            let key = extent_corrupt_key(extent_id);
            if next == 0 {
                etcd.put_and_delete_txn(vec![], vec![key]).await?;
            } else {
                etcd.put_and_delete_txn(vec![(key, next.to_le_bytes().to_vec())], vec![])
                    .await?;
            }
        }
        let mut m = self.extent_corrupt_slots.borrow_mut();
        if next == 0 {
            m.remove(&extent_id);
        } else {
            m.insert(extent_id, next);
        }
        Ok(())
    }

    /// Drop an extent's marks when the extent itself is gone.
    pub(crate) async fn forget_corrupt_slots(&self, extent_id: u64) -> Result<(), AppError> {
        let had = self
            .extent_corrupt_slots
            .borrow_mut()
            .remove(&extent_id)
            .is_some();
        if !had {
            return Ok(());
        }
        if let Some(etcd) = &self.etcd {
            etcd.put_and_delete_txn(vec![], vec![extent_corrupt_key(extent_id)])
                .await?;
        }
        Ok(())
    }

    pub(crate) fn install_replayed_corrupt_slots(&self, decoded: HashMap<u64, u32>) {
        *self.extent_corrupt_slots.borrow_mut() = decoded;
    }

    /// Decode replayed per-extent slot bitmaps (`extentCorrupt/`,
    /// `extentRepair/`; `what` names the prefix in the warning). A malformed
    /// value is DROPPED rather than failing replay: losing a mark costs a
    /// missed rebuild, while refusing leadership over one costs the whole
    /// cluster.
    pub(crate) fn decode_slot_bitmap_kvs<'a>(
        kvs: impl Iterator<Item = (u64, &'a [u8])>,
        what: &str,
    ) -> HashMap<u64, u32> {
        let mut out = HashMap::new();
        for (id, raw) in kvs {
            if let Ok(arr) = <[u8; 4]>::try_from(raw) {
                let bits = u32::from_le_bytes(arr);
                if bits != 0 {
                    out.insert(id, bits);
                }
            } else {
                tracing::warn!(
                    extent_id = id,
                    len = raw.len(),
                    what,
                    "malformed slot-bitmap value; dropping the mark"
                );
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_drops_malformed_and_empty_values() {
        let three = [1u8, 2, 3];
        let five = [0u8; 5];
        let ok = 0b101u32.to_le_bytes();
        let zero = 0u32.to_le_bytes();
        let decoded = AutumnManager::decode_slot_bitmap_kvs(
            [
                (1u64, ok.as_slice()),
                (2u64, three.as_slice()),
                (3u64, five.as_slice()),
                (4u64, zero.as_slice()),
            ]
            .into_iter(),
            EXTENT_CORRUPT_PREFIX,
        );
        assert_eq!(decoded.get(&1).copied(), Some(0b101));
        assert!(!decoded.contains_key(&2), "3-byte value is malformed");
        assert!(!decoded.contains_key(&3), "5-byte value is malformed");
        assert!(!decoded.contains_key(&4), "an empty bitmap is not a mark");
    }

    #[test]
    fn key_is_prefixed_and_parseable() {
        assert_eq!(extent_corrupt_key(42), "extentCorrupt/42");
        assert!(extent_corrupt_key(7).starts_with(EXTENT_CORRUPT_PREFIX));
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;

    fn extent(avali: u32, replicates: Vec<u64>) -> ExtentRecord {
        ExtentRecord {
            extent_id: 42,
            replicates,
            parity: vec![],
            eversion: 7,
            refs: 1,
            vp_table_refs: 0,
            sealed_length: 4096,
            sealed: true,
            avali,
            replicate_disks: vec![10, 11, 12],
            parity_disks: vec![],
            ec_converted: false,
        }
    }

    /// Timing, not evidence: the same report that is deferred mid-op is acted
    /// on once the op clears and the copy is checked again.
    #[test]
    fn a_report_is_deferred_while_the_extent_has_an_op_in_flight() {
        let ex = extent(0b111, vec![1, 3, 5]);
        assert!(matches!(
            compute_corrupt_isolation(&ex, &[3], 7, true),
            IsolationOutcome::Stale { .. }
        ));
        assert!(matches!(
            compute_corrupt_isolation(&ex, &[3], 7, false),
            IsolationOutcome::Isolate { .. }
        ));
    }

    #[test]
    fn a_reported_replica_is_darkened_and_the_eversion_moves() {
        let ex = extent(0b111, vec![1, 3, 5]);
        match compute_corrupt_isolation(&ex, &[3], 7, false) {
            IsolationOutcome::Isolate { updated, cleared_mask } => {
                assert_eq!(cleared_mask, 0b010, "slot 1 is node 3");
                assert_eq!(updated.avali, 0b101);
                assert_eq!(updated.eversion, 8, "readers must refetch");
            }
            other => panic!("expected isolation, got {other:?}"),
        }
    }

    /// The last available copy is worse gone than damaged: an extent nobody can
    /// read at all is a harder failure than one served from a copy known bad.
    #[test]
    fn the_last_available_replica_is_never_darkened() {
        let ex = extent(0b001, vec![1, 3, 5]);
        assert!(matches!(
            compute_corrupt_isolation(&ex, &[1], 7, false),
            IsolationOutcome::Refused { .. }
        ));
    }

    /// A finding describes the bytes the reporter read. If the extent has moved
    /// on — a recovery rebuilt it, a conversion replaced it — those bytes are
    /// gone and the finding is about nothing.
    #[test]
    fn a_finding_about_replaced_content_is_refused() {
        let ex = extent(0b111, vec![1, 3, 5]);
        assert!(matches!(
            compute_corrupt_isolation(&ex, &[3], 6, false),
            IsolationOutcome::Stale { .. }
        ));
    }

    #[test]
    fn a_repeated_report_is_success_not_a_refusal() {
        // Slot 1 already dark.
        let ex = extent(0b101, vec![1, 3, 5]);
        assert!(matches!(
            compute_corrupt_isolation(&ex, &[3], 7, false),
            IsolationOutcome::AlreadyIsolated
        ));
    }

    /// 2+1 over nodes 1, 3 (data) and 5 (parity).
    fn ec_extent(avali: u32) -> ExtentRecord {
        let mut ex = extent(avali, vec![1, 3]);
        ex.parity = vec![5];
        ex.replicate_disks = vec![10, 11];
        ex.parity_disks = vec![12];
        ex.ec_converted = true;
        ex
    }

    /// A rotted shard is darkened like a rotted replica: its slot, by its
    /// position in `replicates ++ parity`, and the eversion moves.
    #[test]
    fn a_rotted_shard_is_darkened_while_k_shards_still_serve() {
        for (node, bit) in [(3u64, 0b010u32), (5, 0b100)] {
            match compute_corrupt_isolation(&ec_extent(0b111), &[node], 7, false) {
                IsolationOutcome::Isolate { updated, cleared_mask } => {
                    assert_eq!(cleared_mask, bit, "node {node}");
                    assert_eq!(updated.avali, 0b111 & !bit);
                    assert_eq!(updated.eversion, 8);
                }
                other => panic!("node {node}: expected isolation, got {other:?}"),
            }
        }
    }

    /// Below K serving shards nothing reconstructs, so the damaged shard's range
    /// would go from wrong to unreadable — the EC form of "never the last copy".
    #[test]
    fn a_shard_is_never_darkened_below_k() {
        // Parity already dark: darkening a data shard would leave 1 of K=2.
        assert!(matches!(
            compute_corrupt_isolation(&ec_extent(0b011), &[3], 7, false),
            IsolationOutcome::Refused { .. }
        ));
    }

    #[test]
    fn an_unsealed_extent_and_a_non_member_are_both_refused() {
        let mut open = extent(0b111, vec![1, 3, 5]);
        open.sealed = false;
        assert!(matches!(
            compute_corrupt_isolation(&open, &[3], 7, false),
            IsolationOutcome::Refused { .. }
        ));
        let ex = extent(0b111, vec![1, 3, 5]);
        assert!(
            matches!(
                compute_corrupt_isolation(&ex, &[99], 7, false),
                IsolationOutcome::Refused { .. }
            ),
            "a report naming a node that is not a replica is a stale layout"
        );
    }
}
