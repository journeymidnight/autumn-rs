//! Whether an extent node, and a disk on it, may take part in extent work: one
//! answer for allocation, recovery, EC, scrub and delete
//! (`docs/extent_op_arbitration_design.md` §1).
//!
//! A node's class combines the auto-state (what the df loop observes) with the
//! operator override (what an operator decided). An override only makes a node
//! worse; clearing it hands the node back to its auto-state. A disk's class
//! is its node's, narrowed by what the node itself reports about the disk.

use std::collections::{HashMap, HashSet};

use autumn_rpc::manager_rpc::{NODE_OVERRIDE_FENCED, NODE_OVERRIDE_MAINTENANCE};

use crate::node_state::NodeAutoState;
use crate::AutumnManager;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeClass {
    /// Answered this term, no override: takes new work.
    Health,
    /// Not heard from yet this term. Takes no new work, but is not abnormal:
    /// the work it is in waits for its first df.
    Suspend,
    /// Silent past the soft timeout, or under Maintenance. Abnormal: the work
    /// it is in is cancelled.
    Suspected,
    /// Fenced, decommissioned, or not registered. Its copies move off at once.
    Fence,
}

impl NodeClass {
    /// The work a node of this class takes part in is cancelled.
    pub(crate) fn is_abnormal(self) -> bool {
        matches!(self, NodeClass::Suspected | NodeClass::Fence)
    }
}

pub(crate) fn node_class(
    registered: bool,
    tombstoned: bool,
    override_kind: Option<u8>,
    auto: NodeAutoState,
) -> NodeClass {
    if !registered || tombstoned || override_kind == Some(NODE_OVERRIDE_FENCED) {
        return NodeClass::Fence;
    }
    if override_kind == Some(NODE_OVERRIDE_MAINTENANCE) {
        return NodeClass::Suspected;
    }
    match auto {
        NodeAutoState::Online => NodeClass::Health,
        NodeAutoState::Suspend => NodeClass::Suspend,
        NodeAutoState::Suspected { .. } => NodeClass::Suspected,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DiskClass {
    Health,
    /// Out of space (its node says so); clears itself. Keeps its copies and the
    /// work on them, takes no new data.
    Full,
    Suspend,
    Suspected,
    /// On a `Fence` node, or Faulted (its node says so). Its copies move off.
    Fence,
}

impl DiskClass {
    pub(crate) fn is_abnormal(self) -> bool {
        matches!(self, DiskClass::Suspected | DiskClass::Fence)
    }
}

/// `faulted` and `full` are what the disk's own node reported in its df.
/// A disk record's `online` bit plays no part: a single df timeout clears it
/// node-wide, which says nothing about the disk.
pub(crate) fn disk_class(node: NodeClass, faulted: bool, full: bool) -> DiskClass {
    match node {
        NodeClass::Fence => DiskClass::Fence,
        _ if faulted => DiskClass::Fence,
        NodeClass::Suspected => DiskClass::Suspected,
        NodeClass::Suspend => DiskClass::Suspend,
        NodeClass::Health if full => DiskClass::Full,
        NodeClass::Health => DiskClass::Health,
    }
}

impl AutumnManager {
    pub(crate) fn node_class_of(&self, node_id: u64) -> NodeClass {
        let registered = self.store.inner.borrow().nodes.contains_key(&node_id);
        node_class(
            registered,
            self.decommissioned.borrow().contains_key(&node_id),
            self.node_overrides.borrow().get(&node_id).map(|o| o.kind),
            self.node_states.borrow().state_of(node_id),
        )
    }

    pub(crate) fn disk_class_of(&self, node_id: u64, disk_id: u64) -> DiskClass {
        let full = self
            .store
            .inner
            .borrow()
            .en_reports
            .get(&node_id)
            .is_some_and(|r| r.full_disks.contains(&disk_id));
        disk_class(
            self.node_class_of(node_id),
            self.faulted_disks.borrow().contains(&disk_id),
            full,
        )
    }

    /// Every registered node's class and the disk facts, taken once for a pass
    /// over many extents.
    pub(crate) fn classes(&self) -> Classes {
        let s = self.store.inner.borrow();
        let tomb = self.decommissioned.borrow();
        let overrides = self.node_overrides.borrow();
        let states = self.node_states.borrow();
        let nodes = s
            .nodes
            .keys()
            .map(|&id| {
                let class = node_class(
                    true,
                    tomb.contains_key(&id),
                    overrides.get(&id).map(|o| o.kind),
                    states.state_of(id),
                );
                (id, class)
            })
            .collect();
        let full = s
            .en_reports
            .iter()
            .flat_map(|(&node, r)| r.full_disks.iter().map(move |&d| (node, d)))
            .collect();
        Classes {
            nodes,
            faulted: self.faulted_disks.borrow().clone(),
            full,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Classes {
    pub(crate) nodes: HashMap<u64, NodeClass>,
    faulted: HashSet<u64>,
    /// (node, disk) the node reported Full.
    full: HashSet<(u64, u64)>,
}

impl Classes {
    /// A node missing from the map is unregistered, i.e. `Fence`.
    pub(crate) fn node(&self, node_id: u64) -> NodeClass {
        self.nodes.get(&node_id).copied().unwrap_or(NodeClass::Fence)
    }

    pub(crate) fn disk(&self, node_id: u64, disk_id: u64) -> DiskClass {
        disk_class(
            self.node(node_id),
            self.faulted.contains(&disk_id),
            self.full.contains(&(node_id, disk_id)),
        )
    }

    /// A placement or recovery target: a `Health` node with a `Health` disk.
    pub(crate) fn takes_new_data(&self, node: &crate::persist::records::NodeRecord) -> bool {
        self.node(node.node_id) == NodeClass::Health
            && node
                .disks
                .iter()
                .any(|&d| self.disk(node.node_id, d) == DiskClass::Health)
    }
}

impl AutumnManager {
    /// Register `ids` (when not yet known) with one disk each (`id * 10`) and
    /// bring each one `Online`, i.e. class `Health`. Public for integration
    /// tests: an EC target on an unregistered node is `Fence` and refused.
    #[doc(hidden)]
    pub fn _test_register_health_nodes(&self, ids: impl IntoIterator<Item = u64>) {
        for node_id in ids {
            self.store
                .inner
                .borrow_mut()
                .nodes
                .entry(node_id)
                .or_insert_with(|| crate::persist::records::NodeRecord {
                    node_id,
                    address: format!("127.0.0.1:{}", 9100 + node_id),
                    disks: vec![node_id * 10],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                });
            let disk_id = node_id * 10;
            self.store.inner.borrow_mut().disks.entry(disk_id).or_insert_with(|| {
                crate::persist::records::DiskRecord {
                    disk_id,
                    online: true,
                    uuid: format!("disk-{disk_id}"),
                }
            });
            self.node_states.borrow_mut().on_heartbeat_ok(node_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    const ONLINE: NodeAutoState = NodeAutoState::Online;
    const SUSPEND: NodeAutoState = NodeAutoState::Suspend;

    fn suspected() -> NodeAutoState {
        NodeAutoState::Suspected { since: Instant::now() }
    }

    #[test]
    fn disk_classes() {
        use NodeClass::*;
        assert_eq!(disk_class(Health, false, false), DiskClass::Health);
        assert_eq!(disk_class(Health, false, true), DiskClass::Full);
        assert_eq!(disk_class(Health, true, true), DiskClass::Fence);
        assert_eq!(disk_class(Suspend, false, true), DiskClass::Suspend);
        assert_eq!(disk_class(Suspected, true, false), DiskClass::Fence);
        assert_eq!(disk_class(Suspected, false, false), DiskClass::Suspected);
        assert_eq!(disk_class(Fence, false, false), DiskClass::Fence);
    }

    #[test]
    fn auto_state_alone() {
        assert_eq!(node_class(true, false, None, ONLINE), NodeClass::Health);
        assert_eq!(node_class(true, false, None, SUSPEND), NodeClass::Suspend);
        assert_eq!(node_class(true, false, None, suspected()), NodeClass::Suspected);
    }

    /// An override only makes a node worse, whatever the auto-state says.
    #[test]
    fn overrides_win() {
        for auto in [ONLINE, SUSPEND, suspected()] {
            let m = Some(NODE_OVERRIDE_MAINTENANCE);
            assert_eq!(node_class(true, false, m, auto), NodeClass::Suspected);
            let f = Some(NODE_OVERRIDE_FENCED);
            assert_eq!(node_class(true, false, f, auto), NodeClass::Fence);
            assert_eq!(node_class(true, true, None, auto), NodeClass::Fence);
            assert_eq!(node_class(false, false, None, auto), NodeClass::Fence);
        }
    }
}

#[cfg(test)]
impl Classes {
    /// `ids` `Health`; `faulted` disks Faulted.
    pub(crate) fn _test_health(
        ids: impl IntoIterator<Item = u64>,
        faulted: impl IntoIterator<Item = u64>,
    ) -> Self {
        Self {
            nodes: ids.into_iter().map(|id| (id, NodeClass::Health)).collect(),
            faulted: faulted.into_iter().collect(),
            full: HashSet::new(),
        }
    }
}
