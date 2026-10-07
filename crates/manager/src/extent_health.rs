//! Extent health: what each slot of a sealed extent is doing, and how many
//! extents are short a copy — the extent counterpart of Ceph's placement-group
//! summary.
//!
//! One classification serves three readers: the summary an operator polls
//! (`MSG_EXTENT_HEALTH_SUMMARY`), the policy tick that keeps the "degraded
//! since" clock, and the repair policy that acts on that clock. They must agree
//! on what "degraded" means, so it is decided here, once, from facts the
//! manager already holds — nothing here asks a node anything.

use std::collections::HashMap;

use autumn_rpc::manager_rpc::{
    ExtentHealthSummaryResp, ProblemExtent, ProblemSlot, CODE_OK, NODE_OVERRIDE_FENCED,
    NODE_OVERRIDE_MAINTENANCE, NODE_OVERRIDE_NONE,
    SLOT_STATE_BEHIND, SLOT_STATE_CORRUPT, SLOT_STATE_DISK_FAULTED, SLOT_STATE_FENCED,
    SLOT_STATE_MAINTENANCE, SLOT_STATE_SERVING, SLOT_STATE_UNREACHABLE,
};

use crate::persist::records::ExtentRecord;
use crate::store::MetadataState;
use crate::AutumnManager;

/// Every `SLOT_STATE_*`, in value order. `slot_counts` is indexed by state, so
/// its length comes from here — a new state is added to this list or the
/// `every_state_has_a_slot_count` test fails, never a panic on the leader.
const ALL_SLOT_STATES: [u8; 7] = [
    SLOT_STATE_SERVING,
    SLOT_STATE_BEHIND,
    SLOT_STATE_UNREACHABLE,
    SLOT_STATE_MAINTENANCE,
    SLOT_STATE_FENCED,
    SLOT_STATE_CORRUPT,
    SLOT_STATE_DISK_FAULTED,
];
const SLOT_STATES: usize = ALL_SLOT_STATES.len();

/// What the manager knows about one slot, gathered without asking anyone.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SlotFacts {
    /// The slot's `avali` bit.
    pub avali: bool,
    /// The node is registered and its auto-state is Online.
    pub node_online: bool,
    /// `NODE_OVERRIDE_*` on the node.
    pub override_kind: u8,
    pub corrupt: bool,
    /// The node named this disk faulted on its last `df`.
    pub disk_faulted: bool,
    /// The disk record's `online` bit; `None` when the layout names no disk or
    /// the disk has no record — no evidence against the copy either way.
    pub disk_online: Option<bool>,
}

/// `SLOT_STATE_*` for one slot of a SEALED extent. The order of the checks is
/// the documented precedence on the constants.
pub(crate) fn classify_slot(f: &SlotFacts) -> u8 {
    if f.corrupt {
        return SLOT_STATE_CORRUPT;
    }
    if f.override_kind == NODE_OVERRIDE_FENCED {
        return SLOT_STATE_FENCED;
    }
    if f.disk_faulted {
        return SLOT_STATE_DISK_FAULTED;
    }
    let reachable = f.node_online && f.disk_online != Some(false);
    if reachable && f.avali {
        return SLOT_STATE_SERVING;
    }
    if f.override_kind == NODE_OVERRIDE_MAINTENANCE {
        return SLOT_STATE_MAINTENANCE;
    }
    if !reachable {
        return SLOT_STATE_UNREACHABLE;
    }
    SLOT_STATE_BEHIND
}

/// Serving copies a read of `ex` needs: any one replica of a replicated
/// extent, every data shard's worth of an EC one. An extent that is not yet
/// EC-converted keeps full replicas in its parity slots too.
pub(crate) fn needed_copies(ex: &ExtentRecord) -> u32 {
    if ex.ec_converted {
        ex.replicates.len() as u32
    } else {
        1
    }
}

/// One sealed extent, classified.
pub(crate) struct ExtentView {
    pub extent_id: u64,
    pub sealed_length: u64,
    pub ec_converted: bool,
    pub needed: u32,
    pub recovering: bool,
    /// `(node_id, SLOT_STATE_*, degraded_secs)` per slot, in slot order.
    pub slots: Vec<(u64, u8, u64)>,
    /// Slots with a standing repair request (`extent_repair`), as a bitmap.
    pub requested: u32,
}

impl ExtentView {
    fn serving(&self) -> u32 {
        self.slots
            .iter()
            .filter(|(_, st, _)| *st == SLOT_STATE_SERVING)
            .count() as u32
    }
}

/// One pass over the store: the counts, and a view of each extent that has a
/// slot not serving. A clean extent is counted and never materialized — the
/// summary is polled every few seconds and nearly every extent is clean.
#[derive(Default)]
pub(crate) struct HealthScan {
    pub open: u64,
    pub clean: u64,
    /// Clean extents with a rebuild in flight (a problem view carries its own).
    pub clean_recovering: u64,
    /// Slots with a standing repair request, over every extent.
    pub repair_requested_slots: u64,
    pub problems: Vec<ExtentView>,
}

/// Count the scan and keep the worst `max_problems`.
pub(crate) fn summarize(scan: HealthScan, max_problems: usize) -> ExtentHealthSummaryResp {
    let mut r = ExtentHealthSummaryResp {
        code: CODE_OK,
        open_extents: scan.open,
        clean: scan.clean,
        sealed_extents: scan.clean,
        recovering: scan.clean_recovering,
        repair_requested_slots: scan.repair_requested_slots,
        slot_counts: vec![0; SLOT_STATES],
        ..Default::default()
    };
    let mut problems: Vec<(i64, u64, ExtentView)> = Vec::with_capacity(scan.problems.len());
    for v in scan.problems {
        r.sealed_extents += 1;
        if v.recovering {
            r.recovering += 1;
        }
        for (_, st, _) in &v.slots {
            if *st != SLOT_STATE_SERVING {
                r.slot_counts[*st as usize] += 1;
            }
        }
        let serving = v.serving();
        r.degraded_bytes += v.sealed_length;
        if serving < v.needed {
            r.unavailable += 1;
        } else {
            r.degraded += 1;
            if serving == v.needed {
                r.no_redundancy += 1;
            }
        }
        let margin = i64::from(serving) - i64::from(v.needed);
        let longest = v.slots.iter().map(|(_, _, secs)| *secs).max().unwrap_or(0);
        problems.push((margin, longest, v));
    }
    // Worst first: unreadable, then the least margin above what a read needs,
    // then the slot that has been down longest.
    problems.sort_by_key(|(margin, longest, v)| {
        (*margin, std::cmp::Reverse(*longest), v.extent_id)
    });
    problems.truncate(max_problems);
    r.problems = problems
        .into_iter()
        .map(|(_, _, v)| ProblemExtent {
            extent_id: v.extent_id,
            sealed_length: v.sealed_length,
            ec_converted: v.ec_converted,
            serving: v.serving(),
            total: v.slots.len() as u32,
            needed: v.needed,
            recovering: v.recovering,
            slots: v
                .slots
                .iter()
                .enumerate()
                .filter(|(_, (_, st, _))| *st != SLOT_STATE_SERVING)
                .map(|(i, (node_id, st, secs))| ProblemSlot {
                    slot_index: i as u32,
                    node_id: *node_id,
                    state: *st,
                    degraded_secs: *secs,
                    repair_requested: i < 32 && v.requested & (1u32 << i) != 0,
                })
                .collect(),
        })
        .collect();
    r
}

/// The leader's per-node and per-slot facts, borrowed ONCE for a whole pass
/// rather than per slot.
struct FactSources<'a> {
    node_states: std::cell::Ref<'a, crate::node_state::NodeStateTracker>,
    overrides: std::cell::Ref<'a, HashMap<u64, autumn_rpc::manager_rpc::MgrNodeOverride>>,
    faulted: std::cell::Ref<'a, std::collections::HashSet<u64>>,
    corrupt: std::cell::Ref<'a, HashMap<u64, u32>>,
}

impl FactSources<'_> {
    fn facts(
        &self,
        s: &MetadataState,
        ex: &ExtentRecord,
        slot: usize,
        node_id: u64,
    ) -> SlotFacts {
        let disk_id = if slot < ex.replicates.len() {
            ex.replicate_disks.get(slot).copied()
        } else {
            ex.parity_disks.get(slot - ex.replicates.len()).copied()
        };
        SlotFacts {
            avali: slot < 32 && (ex.avali & (1u32 << slot)) != 0,
            node_online: s.nodes.contains_key(&node_id)
                && self.node_states.state_of(node_id).is_online(),
            override_kind: self
                .overrides
                .get(&node_id)
                .map(|o| o.kind)
                .unwrap_or(NODE_OVERRIDE_NONE),
            corrupt: slot < 32
                && self
                    .corrupt
                    .get(&ex.extent_id)
                    .is_some_and(|bits| bits & (1u32 << slot) != 0),
            disk_faulted: disk_id.is_some_and(|d| self.faulted.contains(&d)),
            disk_online: disk_id.and_then(|d| s.disks.get(&d)).map(|d| d.online),
        }
    }
}

impl AutumnManager {
    /// One pass over every extent (see `HealthScan`). `now_s` dates the
    /// degraded clocks of the problem slots.
    pub(crate) fn extent_health_scan(&self, now_s: i64) -> HealthScan {
        let s = self.store.inner.borrow();
        let since = self.slot_degraded_since.borrow();
        let inflight = self.inflight.borrow();
        let requests = self.extent_repair_slots.borrow();
        let src = FactSources {
            node_states: self.node_states.borrow(),
            overrides: self.node_overrides.borrow(),
            faulted: self.faulted_disks.borrow(),
            corrupt: self.extent_corrupt_slots.borrow(),
        };
        let mut scan = HealthScan {
            repair_requested_slots: requests
                .iter()
                .filter(|(id, _)| s.extents.contains_key(*id))
                .map(|(_, bits)| u64::from(bits.count_ones()))
                .sum(),
            ..Default::default()
        };
        // Reused per extent; a view copies it only when the extent is a problem.
        let mut states: Vec<(u64, u8)> = Vec::new();
        for ex in s.extents.values() {
            if !ex.sealed {
                scan.open += 1;
                continue;
            }
            states.clear();
            let mut all_serving = true;
            for (slot, node_id) in ex.replicates.iter().chain(ex.parity.iter()).enumerate() {
                let st = classify_slot(&src.facts(&s, ex, slot, *node_id));
                all_serving &= st == SLOT_STATE_SERVING;
                states.push((*node_id, st));
            }
            let recovering = matches!(
                inflight.get(&ex.extent_id).and_then(|r| r.kind()),
                Some(crate::extent_inflight::ExtentOpKind::Recovery)
            );
            if all_serving {
                scan.clean += 1;
                scan.clean_recovering += u64::from(recovering);
                continue;
            }
            let slots = states
                .iter()
                .enumerate()
                .map(|(slot, (node_id, st))| {
                    let secs = if *st == SLOT_STATE_SERVING {
                        0
                    } else {
                        since
                            .get(&(ex.extent_id, slot as u32))
                            .map(|t| now_s.saturating_sub(*t).max(0) as u64)
                            .unwrap_or(0)
                    };
                    (*node_id, *st, secs)
                })
                .collect();
            scan.problems.push(ExtentView {
                extent_id: ex.extent_id,
                sealed_length: ex.sealed_length,
                ec_converted: ex.ec_converted,
                needed: needed_copies(ex),
                recovering,
                slots,
                requested: requests.get(&ex.extent_id).copied().unwrap_or(0),
            });
        }
        scan
    }

    /// Re-derive the "degraded since" clock from one pass over every sealed
    /// extent: a slot not serving keeps the time it was first seen so (or now,
    /// if new); a serving slot, or one whose extent is gone, drops out.
    ///
    /// Run on the policy tick (60 s), which already walks the whole store
    /// every pass and is where the repair policy reads the clock; the 2 s
    /// dispatch tick would pay a second full walk thirty times as often for a
    /// clock measured in minutes. A slot that recovers and degrades again
    /// between two ticks keeps its first time, and a leader change restarts
    /// every clock (the map is leader-local) — the safe direction for a policy
    /// that waits before moving data.
    pub(crate) fn refresh_slot_degraded_since(&self, now_s: i64) {
        let scan = self.extent_health_scan(now_s);
        let old = self.slot_degraded_since.borrow();
        let mut next: HashMap<(u64, u32), i64> = HashMap::new();
        for v in &scan.problems {
            for (slot, (_, st, _)) in v.slots.iter().enumerate() {
                if *st != SLOT_STATE_SERVING {
                    let key = (v.extent_id, slot as u32);
                    next.insert(key, old.get(&key).copied().unwrap_or(now_s));
                }
            }
        }
        drop(old);
        *self.slot_degraded_since.borrow_mut() = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serving() -> SlotFacts {
        SlotFacts {
            avali: true,
            node_online: true,
            override_kind: NODE_OVERRIDE_NONE,
            corrupt: false,
            disk_faulted: false,
            disk_online: Some(true),
        }
    }

    #[test]
    fn each_state_is_reachable_and_precedence_holds() {
        assert_eq!(classify_slot(&serving()), SLOT_STATE_SERVING);
        assert_eq!(
            classify_slot(&SlotFacts { avali: false, ..serving() }),
            SLOT_STATE_BEHIND
        );
        assert_eq!(
            classify_slot(&SlotFacts { node_online: false, ..serving() }),
            SLOT_STATE_UNREACHABLE
        );
        assert_eq!(
            classify_slot(&SlotFacts { disk_online: Some(false), ..serving() }),
            SLOT_STATE_UNREACHABLE,
            "a disk marked offline (node-wide on a missed df) is unreachable, not faulted"
        );
        assert_eq!(
            classify_slot(&SlotFacts {
                node_online: false,
                override_kind: NODE_OVERRIDE_MAINTENANCE,
                ..serving()
            }),
            SLOT_STATE_MAINTENANCE
        );
        assert_eq!(
            classify_slot(&SlotFacts { override_kind: NODE_OVERRIDE_MAINTENANCE, ..serving() }),
            SLOT_STATE_SERVING,
            "a maintenance node that still serves the copy is serving"
        );
        // Going-away states win over everything, serving or not.
        assert_eq!(
            classify_slot(&SlotFacts { override_kind: NODE_OVERRIDE_FENCED, ..serving() }),
            SLOT_STATE_FENCED
        );
        assert_eq!(
            classify_slot(&SlotFacts { disk_faulted: true, ..serving() }),
            SLOT_STATE_DISK_FAULTED
        );
        assert_eq!(
            classify_slot(&SlotFacts {
                corrupt: true,
                override_kind: NODE_OVERRIDE_FENCED,
                disk_faulted: true,
                ..serving()
            }),
            SLOT_STATE_CORRUPT
        );
        assert_eq!(
            classify_slot(&SlotFacts { disk_online: None, ..serving() }),
            SLOT_STATE_SERVING,
            "a layout that names no disk is no evidence against the copy"
        );
    }

    #[test]
    fn needed_copies_is_one_replica_or_every_data_shard() {
        let rep = ExtentRecord {
            replicates: vec![1, 2, 3],
            ..Default::default()
        };
        assert_eq!(needed_copies(&rep), 1);
        let pre_ec = ExtentRecord {
            replicates: vec![1, 2],
            parity: vec![3],
            ..Default::default()
        };
        assert_eq!(needed_copies(&pre_ec), 1, "parity slots hold full replicas until converted");
        let ec = ExtentRecord {
            replicates: vec![1, 2, 3, 4],
            parity: vec![5, 6],
            ec_converted: true,
            ..Default::default()
        };
        assert_eq!(needed_copies(&ec), 4);
    }

    /// What `extent_health_scan` would hand `summarize` for these views.
    fn scan(views: Vec<ExtentView>, open: u64) -> HealthScan {
        let mut sc = HealthScan {
            open,
            ..Default::default()
        };
        for v in views {
            if v.slots.iter().all(|(_, st, _)| *st == SLOT_STATE_SERVING) {
                sc.clean += 1;
                sc.clean_recovering += u64::from(v.recovering);
            } else {
                sc.problems.push(v);
            }
        }
        sc
    }

    #[test]
    fn every_state_has_a_slot_count() {
        for (i, st) in ALL_SLOT_STATES.iter().enumerate() {
            assert_eq!(*st as usize, i, "ALL_SLOT_STATES is in value order");
        }
        let all = [true, false];
        for avali in all {
            for node_online in all {
                for corrupt in all {
                    for disk_faulted in all {
                        for override_kind in [0u8, 1, 2] {
                            for disk_online in [None, Some(true), Some(false)] {
                                let st = classify_slot(&SlotFacts {
                                    avali,
                                    node_online,
                                    override_kind,
                                    corrupt,
                                    disk_faulted,
                                    disk_online,
                                });
                                assert!((st as usize) < SLOT_STATES, "state {st} has no count");
                            }
                        }
                    }
                }
            }
        }
    }

    fn view(id: u64, needed: u32, states: &[(u8, u64)]) -> ExtentView {
        ExtentView {
            extent_id: id,
            sealed_length: 100,
            ec_converted: needed > 1,
            needed,
            recovering: false,
            slots: states
                .iter()
                .enumerate()
                .map(|(i, (st, secs))| (i as u64 + 10, *st, *secs))
                .collect(),
            requested: 0,
        }
    }

    #[test]
    fn summary_counts_and_status() {
        let s = SLOT_STATE_SERVING;
        let u = SLOT_STATE_UNREACHABLE;
        let b = SLOT_STATE_BEHIND;
        let r = summarize(
            scan(
                vec![
                    view(1, 1, &[(s, 0), (s, 0), (s, 0)]),
                    view(2, 1, &[(s, 0), (u, 50), (s, 0)]),
                    view(3, 1, &[(s, 0), (u, 50), (b, 10)]),
                ],
                4,
            ),
            10,
        );
        assert_eq!((r.sealed_extents, r.open_extents), (3, 4));
        assert_eq!((r.clean, r.degraded, r.no_redundancy, r.unavailable), (1, 2, 1, 0));
        assert_eq!(r.degraded_bytes, 200);
        assert_eq!(r.slot_counts[u as usize], 2);
        assert_eq!(r.slot_counts[b as usize], 1);
        assert_eq!(r.slot_counts[s as usize], 0);
        assert_eq!(
            r.problems.iter().map(|p| p.extent_id).collect::<Vec<_>>(),
            vec![3, 2],
            "the one with no redundancy left comes first"
        );
        assert_eq!(r.problems[0].slots.len(), 2, "only the non-serving slots are listed");
        assert_eq!(r.problems[0].slots[0].slot_index, 1);
    }

    #[test]
    fn an_ec_extent_short_of_its_data_shards_is_an_error() {
        let s = SLOT_STATE_SERVING;
        let u = SLOT_STATE_UNREACHABLE;
        let r = summarize(
            scan(vec![view(7, 4, &[(s, 0), (s, 0), (s, 0), (u, 5), (u, 5), (u, 5)])], 0),
            10,
        );
        assert_eq!((r.degraded, r.unavailable), (0, 1));
    }

    /// A slot with a standing repair request is marked as such, and the count
    /// comes through.
    #[test]
    fn a_standing_repair_request_is_shown() {
        let s = SLOT_STATE_SERVING;
        let u = SLOT_STATE_UNREACHABLE;
        let mut v = view(5, 1, &[(s, 0), (u, 700)]);
        v.requested = 0b10;
        let mut sc = scan(vec![v], 0);
        sc.repair_requested_slots = 1;
        let r = summarize(sc, 10);
        assert_eq!(r.repair_requested_slots, 1);
        assert!(r.problems[0].slots[0].repair_requested);
        assert_eq!(r.problems[0].slots[0].slot_index, 1);
    }

    #[test]
    fn everything_clean_lists_nothing() {
        let s = SLOT_STATE_SERVING;
        let r = summarize(scan(vec![view(1, 1, &[(s, 0), (s, 0)])], 0), 10);
        assert_eq!((r.clean, r.degraded, r.unavailable), (1, 0, 0));
        assert!(r.problems.is_empty());
    }

    #[test]
    fn problems_are_capped() {
        let s = SLOT_STATE_SERVING;
        let u = SLOT_STATE_UNREACHABLE;
        let views = (0..5).map(|i| view(i, 1, &[(s, 0), (u, i)])).collect();
        let r = summarize(scan(views, 0), 2);
        assert_eq!(r.degraded, 5);
        assert_eq!(
            r.problems.iter().map(|p| p.extent_id).collect::<Vec<_>>(),
            vec![4, 3],
            "equal margin: the longest-degraded first"
        );
    }
}

