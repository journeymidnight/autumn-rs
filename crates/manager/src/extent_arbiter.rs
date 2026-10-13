//! Which op may start on an extent now (`docs/extent_op_arbitration_design.md`
//! §2, §4). One answer for the policy advisory, its actuation, submit, the
//! recovery dispatch and GC's punch / truncate, so no two of them disagree
//! about the same extent.
//!
//! At most one op runs on an extent and nothing preempts: an op in flight runs
//! until it completes or is cancelled, and everything else waits.

use std::collections::{HashMap, HashSet};

use crate::extent_inflight::ExtentOpKind;
use crate::node_class::{Classes, DiskClass};
use crate::persist::records::ExtentRecord;
use crate::AutumnManager;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtentOp {
    /// GC punch / truncate, the reclaim sweeps.
    Removal,
    /// Recovery of one slot.
    Rebuild,
    /// Conversion to `shards` = K + M shards.
    EcConvert { shards: usize },
}

/// One replica of the extent, as the arbiter sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaFacts {
    pub node: u64,
    /// The class of the disk the copy is on.
    pub disk: DiskClass,
    /// The copy is full and trusted: its `avali` bit is set and it is not
    /// corrupt-marked.
    pub full: bool,
}

impl ReplicaFacts {
    /// An EC target that keeps its copy where it is: a disk that serves reads
    /// (`Health` or `Full`, so its node is `Health`) holding a full, trusted
    /// copy. The first one is the coordinator, which encodes from its own copy.
    pub(crate) fn ec_usable(&self) -> bool {
        self.full && matches!(self.disk, DiskClass::Health | DiskClass::Full)
    }
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
    /// The replicas, in slot order.
    pub replicas: Vec<ReplicaFacts>,
    /// Nodes that take new data and hold no copy of the extent: where an EC
    /// conversion finds substitutes and extras.
    pub spare_nodes: usize,
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
    let ExtentOp::EcConvert { shards } = op else {
        return Ok(());
    };
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
    if let Some(r) = f.replicas.iter().find(|r| r.disk == DiskClass::Fence) {
        return Err(format!(
            "extent {id} has a replica on node {} that is fenced or on a faulted disk; \
             it is rebuilt first",
            r.node
        ));
    }
    // Not heard from yet this term: its copy may be perfectly good, so wait for
    // its df rather than substitute it.
    if let Some(r) = f.replicas.iter().find(|r| r.disk == DiskClass::Suspend) {
        return Err(format!(
            "extent {id} has a replica on node {}, not heard from yet this term; retry after \
             its first df",
            r.node
        ));
    }
    // A copy that is merely behind on a node that serves is caught up in place
    // (§2 rule 5) before a conversion would drop it.
    if let Some(r) = f
        .replicas
        .iter()
        .find(|r| !r.full && matches!(r.disk, DiskClass::Health | DiskClass::Full))
    {
        return Err(format!(
            "extent {id} has a behind replica on node {}; it is caught up first",
            r.node
        ));
    }
    // A replica on a suspected or maintenance node is substituted by a health
    // node (§5): what the conversion needs is a copy to encode from and enough
    // health nodes.
    let usable = f.replicas.iter().filter(|r| r.ec_usable()).count();
    if usable == 0 {
        return Err(format!(
            "extent {id} has no full copy on a health node to encode from"
        ));
    }
    let available = usable.min(shards) + f.spare_nodes;
    if available < shards {
        return Err(format!(
            "not enough health nodes for {shards} shards of extent {id} ({available} available)"
        ));
    }
    Ok(())
}

/// What the arbiter reads that is not per extent, taken once for a pass over
/// many extents: node and disk classes, the nodes that take new data, and which
/// extents a split or merge holds.
pub(crate) struct ArbiterView {
    pub classes: Classes,
    /// Nodes that take new data (`Classes::takes_new_data`).
    pub ready: HashSet<u64>,
    /// extent -> the held partition whose streams list it.
    held: HashMap<u64, u64>,
}

impl ArbiterView {
    /// The extent's replicas as the arbiter sees them, in slot order.
    pub(crate) fn replicas(&self, ex: &ExtentRecord, corrupt_slots: u32) -> Vec<ReplicaFacts> {
        ex.replicates
            .iter()
            .enumerate()
            .map(|(j, &n)| {
                let disk = match ex.replicate_disks.get(j) {
                    Some(&d) => self.classes.disk(n, d),
                    None => crate::node_class::disk_class(self.classes.node(n), false, false),
                };
                let bit = 1u32 << j;
                ReplicaFacts {
                    node: n,
                    disk,
                    full: ex.avali & bit != 0 && corrupt_slots & bit == 0,
                }
            })
            .collect()
    }
}

impl AutumnManager {
    pub(crate) fn arbiter_view(&self) -> ArbiterView {
        let classes = self.classes();
        let mut held = HashMap::new();
        let ready;
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
            ready = s
                .nodes
                .values()
                .filter(|n| classes.takes_new_data(n))
                .map(|n| n.node_id)
                .collect();
        }
        ArbiterView { classes, ready, held }
    }

    /// The facts `may_start` reads, from live state. `None` = no such extent.
    pub(crate) fn extent_facts(&self, view: &ArbiterView, extent_id: u64) -> Option<ExtentFacts> {
        let corrupt_slots = self.corrupt_slots_of(extent_id);
        let (sealed, replicas, spare_nodes) = {
            let s = self.store.inner.borrow();
            let ex = s.extents.get(&extent_id)?;
            let members: HashSet<u64> =
                ex.replicates.iter().chain(ex.parity.iter()).copied().collect();
            let spare = view.ready.len() - members.iter().filter(|m| view.ready.contains(m)).count();
            (
                ex.sealed && ex.sealed_length > 0,
                view.replicas(ex, corrupt_slots),
                spare,
            )
        };
        Some(ExtentFacts {
            extent_id,
            sealed,
            in_flight: self.extent_inflight_op(extent_id),
            topology_held: view.held.get(&extent_id).copied(),
            corrupt_slots,
            repair_slots: self.repair_slots_of(extent_id),
            replicas,
            spare_nodes,
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

    const EC: ExtentOp = ExtentOp::EcConvert { shards: 3 };

    fn replica(node: u64) -> ReplicaFacts {
        ReplicaFacts { node, disk: DiskClass::Health, full: true }
    }

    fn free() -> ExtentFacts {
        ExtentFacts {
            extent_id: 7,
            sealed: true,
            replicas: vec![replica(1), replica(2), replica(3)],
            spare_nodes: 2,
            ..Default::default()
        }
    }

    /// Nothing preempts: any op in flight holds every other back, removal
    /// included, whatever its kind.
    #[test]
    fn an_op_in_flight_holds_everything_back() {
        for kind in [ExtentOpKind::Recovery, ExtentOpKind::ConvertToEc, ExtentOpKind::Delete] {
            let f = ExtentFacts { in_flight: Some(kind), ..free() };
            for op in [ExtentOp::Removal, ExtentOp::Rebuild, EC] {
                assert!(may_start(op, &f).is_err(), "{op:?} under {kind:?}");
            }
        }
        for op in [ExtentOp::Removal, ExtentOp::Rebuild, EC] {
            assert!(may_start(op, &free()).is_ok(), "{op:?}");
        }
    }

    #[test]
    fn a_topology_hold_stops_rebuild_and_ec_but_not_removal() {
        let f = ExtentFacts { topology_held: Some(5), ..free() };
        assert!(may_start(ExtentOp::Rebuild, &f).is_err());
        assert!(may_start(EC, &f).is_err());
        assert!(may_start(ExtentOp::Removal, &f).is_ok());
    }

    /// EC ranks last: a corrupt copy, a copy on a fence disk, or a copy an
    /// operator / the policy asked to move is rebuilt first.
    #[test]
    fn ec_waits_for_every_rebuild_the_extent_is_owed() {
        let corrupt = ExtentFacts { corrupt_slots: 0b10, ..free() };
        let requested = ExtentFacts { repair_slots: 0b1, ..free() };
        let mut fenced = free();
        fenced.replicas[2].disk = DiskClass::Fence;
        for f in [&corrupt, &requested, &fenced] {
            assert!(may_start(EC, f).is_err(), "{f:?}");
            assert!(may_start(ExtentOp::Rebuild, f).is_ok(), "{f:?}");
        }
        let mut full = free();
        full.replicas[0].disk = DiskClass::Full;
        assert!(may_start(EC, &full).is_ok(), "a full disk only reads");
    }

    /// A replica on a suspected or maintenance node is substituted, not waited
    /// for — as long as a full copy is left to encode from and enough health
    /// nodes exist. One on a node not heard from yet is waited for.
    #[test]
    fn ec_substitutes_a_suspected_replica() {
        let mut f = free();
        f.replicas[1].disk = DiskClass::Suspected;
        assert!(may_start(EC, &f).is_ok());
        f.spare_nodes = 0;
        let err = may_start(EC, &f).unwrap_err();
        assert!(err.contains("not enough health nodes"), "{err}");

        let mut f = free();
        for r in &mut f.replicas {
            r.disk = DiskClass::Suspected;
        }
        f.spare_nodes = 9;
        assert!(may_start(EC, &f).unwrap_err().contains("no full copy"));

        let mut f = free();
        f.replicas[1].full = false;
        assert!(
            may_start(EC, &f).unwrap_err().contains("caught up first"),
            "a behind copy on a node that serves is caught up, not dropped"
        );
        f.replicas[1].disk = DiskClass::Suspected;
        assert!(may_start(EC, &f).is_ok(), "on a suspected node it is substituted");

        let mut f = free();
        f.replicas[2].disk = DiskClass::Suspend;
        assert!(may_start(EC, &f).unwrap_err().contains("not heard from yet"));
    }
}
