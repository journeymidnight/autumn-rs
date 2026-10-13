//! Which op may start on an extent now (`docs/extent_op_arbitration_design.md`
//! §2, §4). One answer for the policy advisory, its actuation, submit, the
//! recovery dispatch and GC's punch / truncate, so no two of them disagree
//! about the same extent.
//!
//! At most one op runs on an extent and nothing preempts: an op in flight runs
//! until it completes or is cancelled, and everything else waits.

use std::collections::HashMap;

use crate::extent_inflight::ExtentOpKind;
use crate::node_class::{Classes, DiskClass};
use crate::AutumnManager;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtentOp {
    /// GC punch / truncate, the reclaim sweeps.
    Removal,
    /// Recovery of one slot.
    Rebuild,
    EcConvert,
}

/// What the arbiter reads about one extent.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExtentFacts {
    pub extent_id: u64,
    pub sealed: bool,
    pub in_flight: Option<ExtentOpKind>,
    /// The partition a split or merge holds it under (`topology_gate.rs`).
    pub topology_held: Option<u64>,
    /// Bitmaps over the extent's slots.
    pub corrupt_slots: u32,
    pub repair_slots: u32,
    /// Each replica's node and the class of the disk it is on, in slot order.
    pub replica_disks: Vec<(u64, DiskClass)>,
}

/// `Ok` when `op` may start on the extent now, else why not.
pub(crate) fn may_start(op: ExtentOp, f: &ExtentFacts) -> Result<(), String> {
    let id = f.extent_id;
    if let Some(kind) = f.in_flight {
        return Err(format!(
            "extent {id} has an in-flight {kind:?}; retry after it completes"
        ));
    }
    if op == ExtentOp::Removal {
        // A punch checks the topology hold itself: the split or merge that holds
        // the partition punches its own streams.
        return Ok(());
    }
    if !f.sealed {
        return Err(format!("extent {id} is not sealed"));
    }
    if let Some(part) = f.topology_held {
        return Err(format!(
            "extent {id} belongs to partition {part}, which is being split or merged; \
             retry after it completes"
        ));
    }
    if op == ExtentOp::Rebuild {
        return Ok(());
    }
    // EC ranks last: every rebuild the extent is owed goes first.
    if f.corrupt_slots != 0 {
        return Err(format!(
            "extent {id} has a corrupt-marked slot; it is rebuilt before any EC conversion"
        ));
    }
    if f.repair_slots != 0 {
        return Err(format!(
            "extent {id} has a copy an operator or the repair policy asked to move; \
             it is converted after"
        ));
    }
    if let Some((node, _)) = f.replica_disks.iter().find(|(_, d)| *d == DiskClass::Fence) {
        return Err(format!(
            "extent {id} has a replica on node {node} that is fenced or on a faulted disk; \
             it is rebuilt first"
        ));
    }
    if let Some((node, _)) = f.replica_disks.iter().find(|(_, d)| d.is_abnormal()) {
        return Err(format!(
            "extent {id} has a replica on node {node}, which is suspected or in maintenance; \
             retry once the node is back or the replica is repaired off it"
        ));
    }
    Ok(())
}

/// What the arbiter reads that is not per extent, taken once for a pass over
/// many extents: node and disk classes, and which extents a split or merge holds.
pub(crate) struct ArbiterView {
    pub classes: Classes,
    /// extent -> the held partition whose streams list it.
    held: HashMap<u64, u64>,
}

impl AutumnManager {
    pub(crate) fn arbiter_view(&self) -> ArbiterView {
        let mut held = HashMap::new();
        {
            let parts = self.topology_held.borrow();
            let s = self.store.inner.borrow();
            for &pid in parts.iter() {
                let Some(p) = s.partitions.get(&pid) else { continue };
                for sid in [p.log_stream, p.row_stream, p.meta_stream] {
                    if let Some(st) = s.streams.get(&sid) {
                        held.extend(st.extent_ids.iter().map(|&e| (e, pid)));
                    }
                }
            }
        }
        ArbiterView { classes: self.classes(), held }
    }

    /// The facts `may_start` reads, from live state. `None` = no such extent.
    pub(crate) fn extent_facts(&self, view: &ArbiterView, extent_id: u64) -> Option<ExtentFacts> {
        let classes = &view.classes;
        let (sealed, replica_disks) = {
            let s = self.store.inner.borrow();
            let ex = s.extents.get(&extent_id)?;
            let disks = ex
                .replicates
                .iter()
                .enumerate()
                .map(|(j, &n)| {
                    let class = match ex.replicate_disks.get(j) {
                        Some(&d) => classes.disk(n, d),
                        None => crate::node_class::disk_class(classes.node(n), false, false),
                    };
                    (n, class)
                })
                .collect();
            (ex.sealed && ex.sealed_length > 0, disks)
        };
        Some(ExtentFacts {
            extent_id,
            sealed,
            in_flight: self.extent_inflight_op(extent_id),
            topology_held: view.held.get(&extent_id).copied(),
            corrupt_slots: self.corrupt_slots_of(extent_id),
            repair_slots: self.repair_slots_of(extent_id),
            replica_disks,
        })
    }

    /// `may_start` for `extent_id` from live state.
    pub(crate) fn arbitrate(
        &self,
        view: &ArbiterView,
        extent_id: u64,
        op: ExtentOp,
    ) -> Result<(), String> {
        let facts = self
            .extent_facts(view, extent_id)
            .ok_or_else(|| format!("extent {extent_id} not found"))?;
        may_start(op, &facts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn free() -> ExtentFacts {
        ExtentFacts {
            extent_id: 7,
            sealed: true,
            replica_disks: vec![(1, DiskClass::Health), (2, DiskClass::Health), (3, DiskClass::Health)],
            ..Default::default()
        }
    }

    /// Nothing preempts: any op in flight holds every other back, removal
    /// included, whatever its kind.
    #[test]
    fn an_op_in_flight_holds_everything_back() {
        for kind in [ExtentOpKind::Recovery, ExtentOpKind::ConvertToEc, ExtentOpKind::Delete] {
            let f = ExtentFacts { in_flight: Some(kind), ..free() };
            for op in [ExtentOp::Removal, ExtentOp::Rebuild, ExtentOp::EcConvert] {
                assert!(may_start(op, &f).is_err(), "{op:?} under {kind:?}");
            }
        }
        for op in [ExtentOp::Removal, ExtentOp::Rebuild, ExtentOp::EcConvert] {
            assert!(may_start(op, &free()).is_ok(), "{op:?}");
        }
    }

    #[test]
    fn a_topology_hold_stops_rebuild_and_ec_but_not_removal() {
        let f = ExtentFacts { topology_held: Some(5), ..free() };
        assert!(may_start(ExtentOp::Rebuild, &f).is_err());
        assert!(may_start(ExtentOp::EcConvert, &f).is_err());
        assert!(may_start(ExtentOp::Removal, &f).is_ok());
    }

    /// EC ranks last: a corrupt copy, a copy on a fence disk, or a copy an
    /// operator / the policy asked to move is rebuilt first.
    #[test]
    fn ec_waits_for_every_rebuild_the_extent_is_owed() {
        let corrupt = ExtentFacts { corrupt_slots: 0b10, ..free() };
        let requested = ExtentFacts { repair_slots: 0b1, ..free() };
        let mut fenced = free();
        fenced.replica_disks[2].1 = DiskClass::Fence;
        let mut suspected = free();
        suspected.replica_disks[1].1 = DiskClass::Suspected;
        for f in [&corrupt, &requested, &fenced, &suspected] {
            assert!(may_start(ExtentOp::EcConvert, f).is_err(), "{f:?}");
            assert!(may_start(ExtentOp::Rebuild, f).is_ok(), "{f:?}");
        }
        let mut full = free();
        full.replica_disks[0].1 = DiskClass::Full;
        assert!(may_start(ExtentOp::EcConvert, &full).is_ok(), "a full disk only reads");
    }
}
