//! Slots someone has decided to rebuild on another node NOW — an operator
//! (`autumn-op repair`) or the repair policy — without fencing their node.
//!
//! The recovery loop on its own moves a copy only on conclusive evidence (a
//! fenced node, a corrupt slot, a disk its node calls faulted). A node that
//! stopped answering may be back in seconds, so its copies stay put; that is
//! right for a transient and wrong for a node that is not coming back, where
//! the extent sits a copy short until someone fences the whole node. A repair
//! request is that someone deciding about the EXTENT instead: the slot's
//! verdict becomes Rebuild, and the ordinary dispatch moves it.
//!
//! Persisted (`extentRepair/<id>` → u32 slot bitmap, the same shape and the
//! same reasons as `extentCorrupt/`): a decision that only lived in the
//! leader's memory would be lost at the first failover or the first time the
//! rebuild's executor died, and nothing would make it again until the policy
//! re-derived it — or, for an operator's request, ever. Cleared when the
//! rebuild lands (`apply_recovery_done`) and when the extent is deleted. And it
//! is WITHDRAWN when its premise goes — on first-hand evidence the slot's node
//! answers again and the copy serves — like Ceph's mark-in cancelling the
//! remaps of its down→out; a behind replica on a node that answers is caught up
//! in place first and the request waits, rebuilding only if the node answers
//! that it has no such extent (`recovery_dispatch_tick`). Without that, a node back after the grace
//! period had every requested copy moved anyway: to a spare it was never lost
//! to, or, with no spare, pinned forever to a rebuild with no target while the
//! catch-up that would have fixed it never ran. A rebuild already dispatched
//! for a copy that serves again is released with it; one for a behind copy runs
//! to completion.

use std::collections::{BTreeMap, HashMap};

use autumn_common::AppError;
use autumn_rpc::manager_rpc::{
    PolicyCandidate, POLICY_KIND_REPAIR, SLOT_STATE_BEHIND, SLOT_STATE_MAINTENANCE,
    SLOT_STATE_SERVING, SLOT_STATE_UNREACHABLE,
};

use crate::extent_health::ExtentView;
use crate::AutumnManager;

pub(crate) const EXTENT_REPAIR_PREFIX: &str = "extentRepair/";

/// At most this many puts per etcd transaction (etcd's default `max-txn-ops`
/// is 128).
const REPAIR_MARKS_PER_TXN: usize = 64;

pub(crate) fn extent_repair_key(extent_id: u64) -> String {
    format!("{EXTENT_REPAIR_PREFIX}{extent_id}")
}

/// Who is asking. An operator may move a copy off a node in Maintenance — the
/// override says "expect it back", and the operator is the one who said so;
/// the policy may not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RepairRequester {
    Operator,
    Policy,
}

/// Which slots of `view` a repair would move, given the slots already
/// requested (`already`). `Err` says why nothing is.
///
/// Only slots that do not serve and that nothing else is already moving:
/// fenced, corrupt and faulted-disk slots are rebuilt by the loop on its own.
/// Refused when a read could not be served right now — there is then no
/// source to rebuild from, and marking the slots would only queue rebuilds
/// that fail until a copy returns.
pub(crate) fn plan_repair(
    view: &ExtentView,
    already: u32,
    who: RepairRequester,
) -> Result<u32, String> {
    let serving = view
        .slots
        .iter()
        .filter(|(_, st, _)| *st == SLOT_STATE_SERVING)
        .count() as u32;
    if serving < view.needed {
        return Err(format!(
            "extent {}: {serving} serving cop{} but a read needs {} — nothing to rebuild from",
            view.extent_id,
            if serving == 1 { "y" } else { "ies" },
            view.needed
        ));
    }
    let mut mask = 0u32;
    for (slot, (_, st, _)) in view.slots.iter().enumerate() {
        let wanted = match *st {
            // A behind replica is caught up in place first; the request
            // rebuilds it only if that keeps failing (`recovery_dispatch_tick`).
            SLOT_STATE_UNREACHABLE | SLOT_STATE_BEHIND => true,
            SLOT_STATE_MAINTENANCE => who == RepairRequester::Operator,
            _ => false,
        };
        if wanted && slot < 32 {
            mask |= 1u32 << slot;
        }
    }
    let new = mask & !already;
    if new == 0 {
        return Err(format!(
            "extent {}: nothing to repair — every copy serves, is already requested, or is \
             already being rebuilt for another reason",
            view.extent_id
        ));
    }
    Ok(new)
}

/// What `request_repair` did.
#[derive(Debug, Default)]
pub(crate) struct RepairOutcome {
    /// Slots newly requested.
    pub requested_slots: u32,
    /// Extents with at least one slot newly requested.
    pub extents: Vec<u64>,
    /// Why a named extent got nothing (only for the extents a caller named).
    pub refused: Vec<String>,
}

impl RepairOutcome {
    /// One line for an op's message or the policy's action log.
    pub(crate) fn describe(&self) -> String {
        let mut out = format!(
            "requested a rebuild of {} slot(s) on {} extent(s)",
            self.requested_slots,
            self.extents.len()
        );
        if !self.refused.is_empty() {
            out.push_str(&format!("; nothing to do for: {}", self.refused.join("; ")));
        }
        out
    }
}

impl AutumnManager {
    /// Record repair requests: for each extent (every degraded one when
    /// `extent_ids` is empty), the slots `plan_repair` allows, narrowed to
    /// those on `only_node` and degraded at least `min_degraded_secs`. The
    /// recovery loop rebuilds them on other nodes from its next tick.
    ///
    /// An operator names extents and asks for everything degraded on them
    /// now; the policy names a node and passes its grace period.
    pub(crate) async fn request_repair(
        &self,
        extent_ids: &[u64],
        only_node: Option<u64>,
        min_degraded_secs: u64,
        who: RepairRequester,
    ) -> Result<RepairOutcome, AppError> {
        let scan = self.extent_health_scan(Self::epoch_seconds());
        let mut by_id: HashMap<u64, ExtentView> = scan
            .problems
            .into_iter()
            .map(|v| (v.extent_id, v))
            .collect();
        let mut outcome = RepairOutcome::default();
        let targets: Vec<ExtentView> = if extent_ids.is_empty() {
            let mut all: Vec<ExtentView> = by_id.into_values().collect();
            all.sort_by_key(|v| v.extent_id);
            all
        } else {
            let mut named = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for id in extent_ids.iter().filter(|id| seen.insert(**id)) {
                match by_id.remove(id) {
                    Some(v) => named.push(v),
                    None => {
                        let why = match self.store.inner.borrow().extents.get(id) {
                            None => "no such extent",
                            Some(ex) if !ex.sealed => {
                                "open: its writer rolls off a replica it cannot reach"
                            }
                            Some(_) => "every copy serves",
                        };
                        outcome.refused.push(format!("extent {id}: {why}"));
                    }
                }
            }
            named
        };
        let mut requests: Vec<(u64, u32)> = Vec::new();
        for v in &targets {
            let planned = match plan_repair(v, self.repair_slots_of(v.extent_id), who) {
                Ok(mask) => mask,
                Err(why) => {
                    if !extent_ids.is_empty() {
                        outcome.refused.push(why);
                    }
                    continue;
                }
            };
            let mut mask = 0u32;
            for (slot, (node_id, _, secs)) in v.slots.iter().enumerate() {
                let bit = if slot < 32 { 1u32 << slot } else { 0 };
                if planned & bit != 0
                    && only_node.is_none_or(|n| n == *node_id)
                    && *secs >= min_degraded_secs
                {
                    mask |= bit;
                }
            }
            if mask == 0 {
                if !extent_ids.is_empty() {
                    outcome
                        .refused
                        .push(format!("extent {}: no slot qualifies", v.extent_id));
                }
                continue;
            }
            outcome.requested_slots += mask.count_ones();
            outcome.extents.push(v.extent_id);
            requests.push((v.extent_id, mask));
        }
        if requests.is_empty() && extent_ids.is_empty() {
            if let Some(node) = only_node {
                outcome.refused.push(format!(
                    "node {node}: no copy to move — its copies serve, are already requested, \
                     or are already being rebuilt"
                ));
            }
        }
        if let Err((done, e)) = self.request_repairs(&requests).await {
            return Err(AppError::Internal(format!(
                "recorded repair requests for {done} of {} extent(s), then: {e}",
                requests.len()
            )));
        }
        if !requests.is_empty() {
            tracing::info!(
                ?who,
                node = ?only_node,
                slots = outcome.requested_slots,
                extents = requests.len(),
                "repair requested: these slots are rebuilt on other nodes"
            );
        }
        Ok(outcome)
    }

    /// The repair policy's advisories: one per node whose slots have stayed
    /// degraded (behind or unreachable) for at least `--repair-grace-secs`,
    /// on extents a rebuild can actually serve, that nothing is already
    /// moving. Per NODE, not per extent: a node that is gone degrades every
    /// extent it held, and one row — "node 5: 1 234 extents, longest 812 s" —
    /// is what an operator reads and what one actuation should cover.
    pub(crate) fn repair_candidates(&self, now_s: i64) -> Vec<PolicyCandidate> {
        let grace = self.repair_grace_secs.get();
        let scan = self.extent_health_scan(now_s);
        // node -> (extents, bytes, longest secs, with no redundancy left)
        let mut per_node: BTreeMap<u64, (u64, u64, u64, u64)> = BTreeMap::new();
        for v in &scan.problems {
            if self.extent_inflight_op(v.extent_id).is_some() {
                continue;
            }
            let Ok(planned) = plan_repair(
                v,
                self.repair_slots_of(v.extent_id),
                RepairRequester::Policy,
            ) else {
                continue;
            };
            let serving = v
                .slots
                .iter()
                .filter(|(_, st, _)| *st == SLOT_STATE_SERVING)
                .count() as u32;
            let mut nodes_here: Vec<u64> = Vec::new();
            for (slot, (node_id, _, secs)) in v.slots.iter().enumerate() {
                if slot < 32
                    && planned & (1u32 << slot) != 0
                    && *secs >= grace
                    && !nodes_here.contains(node_id)
                {
                    nodes_here.push(*node_id);
                    let e = per_node.entry(*node_id).or_default();
                    e.0 += 1;
                    e.1 += v.sealed_length;
                    e.2 = e.2.max(*secs);
                    e.3 += u64::from(serving == v.needed);
                }
            }
        }
        per_node
            .into_iter()
            .map(
                |(node_id, (extents, bytes, longest, bare))| PolicyCandidate {
                    kind: POLICY_KIND_REPAIR,
                    primary_part_id: 0,
                    secondary_part_id: node_id,
                    reason: format!(
                        "{extents} extent(s) with a copy on node {node_id} degraded at least \
                     {grace} s (longest {longest} s), {bare} with no redundancy left — \
                     rebuild those copies on other nodes"
                    ),
                    size_bytes: bytes,
                    req_per_sec: 0,
                    imm_full_per_sec: 0,
                    same_ps: false,
                    last_op_at: 0,
                },
            )
            .collect()
    }

    /// Slots of `extent_id` with a repair request, as a bitmap.
    pub(crate) fn repair_slots_of(&self, extent_id: u64) -> u32 {
        self.extent_repair_slots
            .borrow()
            .get(&extent_id)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn slot_repair_requested(&self, extent_id: u64, slot: usize) -> bool {
        slot < 32 && (self.repair_slots_of(extent_id) & (1u32 << slot)) != 0
    }

    /// Record repair requests, ORed onto what is already requested.
    /// Etcd-first, in transactions of `REPAIR_MARKS_PER_TXN`: memory takes a
    /// batch only once it is durable, so the loop never rebuilds on a request
    /// that did not survive. Returns how many extents were recorded before an
    /// error, with the error.
    pub(crate) async fn request_repairs(
        &self,
        requests: &[(u64, u32)],
    ) -> Result<usize, (usize, AppError)> {
        let _serial = self.extent_repair_lock.lock().await;
        let mut done = 0usize;
        for batch in requests.chunks(REPAIR_MARKS_PER_TXN) {
            let merged: Vec<(u64, u32)> = batch
                .iter()
                .map(|(id, bits)| (*id, self.repair_slots_of(*id) | bits))
                .collect();
            if let Some(etcd) = &self.etcd {
                let puts = merged
                    .iter()
                    .map(|(id, bits)| (extent_repair_key(*id), bits.to_le_bytes().to_vec()))
                    .collect();
                if let Err(e) = etcd.put_and_delete_txn(puts, vec![]).await {
                    return Err((done, e));
                }
            }
            let mut m = self.extent_repair_slots.borrow_mut();
            for (id, bits) in merged {
                m.insert(id, bits);
            }
            done += batch.len();
        }
        Ok(done)
    }

    /// Withdraw requests (`(extent, bits)`), in transactions of
    /// `REPAIR_MARKS_PER_TXN`: their premise is gone (`recovery_dispatch_tick`).
    /// Etcd-first; memory drops a batch only once it is durable.
    pub(crate) async fn withdraw_repairs(
        &self,
        withdrawals: &[(u64, u32)],
    ) -> Result<(), (usize, AppError)> {
        let _serial = self.extent_repair_lock.lock().await;
        let mut done = 0usize;
        for batch in withdrawals.chunks(REPAIR_MARKS_PER_TXN) {
            let next: Vec<(u64, u32)> = batch
                .iter()
                .map(|(id, bits)| (*id, self.repair_slots_of(*id) & !bits))
                .collect();
            if let Some(etcd) = &self.etcd {
                let puts = next
                    .iter()
                    .filter(|(_, bits)| *bits != 0)
                    .map(|(id, bits)| (extent_repair_key(*id), bits.to_le_bytes().to_vec()))
                    .collect();
                let deletes = next
                    .iter()
                    .filter(|(_, bits)| *bits == 0)
                    .map(|(id, _)| extent_repair_key(*id))
                    .collect();
                if let Err(e) = etcd.put_and_delete_txn(puts, deletes).await {
                    return Err((done, e));
                }
            }
            let mut m = self.extent_repair_slots.borrow_mut();
            for (id, bits) in next {
                if bits == 0 {
                    m.remove(&id);
                } else {
                    m.insert(id, bits);
                }
            }
            done += batch.len();
        }
        Ok(())
    }

    /// Clear one slot's request — the rebuild it asked for has landed.
    pub(crate) async fn clear_repair_slot(
        &self,
        extent_id: u64,
        slot: usize,
    ) -> Result<(), AppError> {
        if slot >= 32 {
            return Ok(());
        }
        let _serial = self.extent_repair_lock.lock().await;
        let cur = self.repair_slots_of(extent_id);
        let next = cur & !(1u32 << slot);
        if next == cur {
            return Ok(());
        }
        if let Some(etcd) = &self.etcd {
            let key = extent_repair_key(extent_id);
            if next == 0 {
                etcd.put_and_delete_txn(vec![], vec![key]).await?;
            } else {
                etcd.put_and_delete_txn(vec![(key, next.to_le_bytes().to_vec())], vec![])
                    .await?;
            }
        }
        let mut m = self.extent_repair_slots.borrow_mut();
        if next == 0 {
            m.remove(&extent_id);
        } else {
            m.insert(extent_id, next);
        }
        Ok(())
    }

    /// Drop an extent's requests when the extent itself is gone. Etcd-first,
    /// like every writer here.
    pub(crate) async fn forget_repair_slots(&self, extent_id: u64) -> Result<(), AppError> {
        let _serial = self.extent_repair_lock.lock().await;
        if !self.extent_repair_slots.borrow().contains_key(&extent_id) {
            return Ok(());
        }
        if let Some(etcd) = &self.etcd {
            etcd.put_and_delete_txn(vec![], vec![extent_repair_key(extent_id)])
                .await?;
        }
        self.extent_repair_slots.borrow_mut().remove(&extent_id);
        Ok(())
    }

    pub(crate) fn install_replayed_repair_slots(&self, decoded: HashMap<u64, u32>) {
        *self.extent_repair_slots.borrow_mut() = decoded;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_rpc::manager_rpc::{SLOT_STATE_CORRUPT, SLOT_STATE_FENCED};

    fn view(needed: u32, states: &[u8]) -> ExtentView {
        ExtentView {
            extent_id: 9,
            sealed_length: 1,
            ec_converted: needed > 1,
            needed,
            recovering: false,
            slots: states
                .iter()
                .enumerate()
                .map(|(i, s)| (i as u64, *s, 0))
                .collect(),
        }
    }

    const S: u8 = SLOT_STATE_SERVING;

    #[test]
    fn unreachable_and_behind_slots_are_planned() {
        let v = view(1, &[S, SLOT_STATE_UNREACHABLE, SLOT_STATE_BEHIND]);
        assert_eq!(plan_repair(&v, 0, RepairRequester::Policy), Ok(0b110));
        assert_eq!(
            plan_repair(&v, 0b010, RepairRequester::Policy),
            Ok(0b100),
            "an already-requested slot is not requested twice"
        );
        assert!(plan_repair(&v, 0b110, RepairRequester::Policy).is_err());
    }

    #[test]
    fn maintenance_is_moved_only_on_an_operators_word() {
        let v = view(1, &[S, SLOT_STATE_MAINTENANCE]);
        assert!(plan_repair(&v, 0, RepairRequester::Policy).is_err());
        assert_eq!(plan_repair(&v, 0, RepairRequester::Operator), Ok(0b10));
    }

    #[test]
    fn slots_the_loop_already_moves_are_left_to_it() {
        let v = view(1, &[S, SLOT_STATE_FENCED, SLOT_STATE_CORRUPT]);
        assert!(plan_repair(&v, 0, RepairRequester::Operator).is_err());
    }

    #[test]
    fn no_source_is_refused() {
        let v = view(1, &[SLOT_STATE_UNREACHABLE, SLOT_STATE_UNREACHABLE]);
        let err = plan_repair(&v, 0, RepairRequester::Operator).unwrap_err();
        assert!(err.contains("nothing to rebuild from"), "{err}");
        let ec = view(2, &[S, SLOT_STATE_UNREACHABLE, SLOT_STATE_UNREACHABLE]);
        assert!(
            plan_repair(&ec, 0, RepairRequester::Operator).is_err(),
            "an EC extent short of its data shards has no source either"
        );
    }

    #[test]
    fn key_is_prefixed_and_parseable() {
        assert_eq!(extent_repair_key(42), "extentRepair/42");
    }
}
