//! Which partition server a partition goes on.
//!
//! A PS started with `--cpuset` pins each partition to two cores and so has a
//! fixed number of slots, `cpuset_len / 2`, reported as `slot_cap`. A PS
//! started without one reports `0`: its capacity is unknown. The manager keeps
//! the caps in memory only (`MetadataState::ps_slot_caps`); every heartbeat
//! repeats them, so a new leader has them back within one beat and treats a PS
//! it has not heard from yet as capacity-unknown.
//!
//! One ranking answers every placement question — a new partition, a region
//! whose PS left, a rebalance move, and whether the rebalance advisory fires:
//!
//! 1. a cpuset PS with a free slot, the most free slots first;
//! 2. a PS of unknown capacity, the fewest partitions first;
//! 3. a full cpuset PS, the lowest fill it would reach (`(used + 1) / cap`).
//!
//! Tier 3 exists so a placement always has an answer; the manager does not
//! refuse it and reserves nothing. The PS does refuse: it will not open a
//! partition past its own budget (the partition shows `ps=unknown`), so a
//! placement there waits for a move.
//!
//! Rebalancing moves a partition only when its new seat ranks STRICTLY better
//! than the one it leaves, and never onto a full cpuset PS: that PS would
//! refuse to open it, and the partition moved might be one that was serving. Every move therefore replaces one seat in the
//! cluster's multiset of seats by a strictly smaller one, and over a finite
//! state space that ordering has no cycle: rebalance cannot move partitions
//! back and forth. That property, not a cooldown, is what keeps it stable.
//!
//! Everything here is pure; ties break by lowest `ps_id` so a dry run matches
//! the applied set.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use autumn_rpc::manager_rpc::RebalanceMove;

use crate::store::MetadataState;

/// How a PS ranks as the home of one more partition. Lower is better.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Seat {
    /// A cpuset PS with `free` slots left.
    Free { free: u32 },
    /// No `--cpuset`, or not heard from since this manager became leader.
    Unknown { used: usize },
    /// A cpuset PS already holding `used >= cap` partitions.
    Over { used: usize, cap: u32 },
}

pub(crate) fn seat(cap: u32, used: usize) -> Seat {
    if cap == 0 {
        Seat::Unknown { used }
    } else if used < cap as usize {
        Seat::Free {
            free: cap - used as u32,
        }
    } else {
        Seat::Over { used, cap }
    }
}

impl Seat {
    fn tier(&self) -> u8 {
        match self {
            Seat::Free { .. } => 0,
            Seat::Unknown { .. } => 1,
            Seat::Over { .. } => 2,
        }
    }
}

impl Ord for Seat {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Seat::Free { free: a }, Seat::Free { free: b }) => b.cmp(a),
            (Seat::Unknown { used: a }, Seat::Unknown { used: b }) => a.cmp(b),
            (Seat::Over { used: ua, cap: ca }, Seat::Over { used: ub, cap: cb }) => {
                // (ua + 1) / ca  vs  (ub + 1) / cb, cross-multiplied.
                let lhs = (*ua as u128 + 1) * *cb as u128;
                let rhs = (*ub as u128 + 1) * *ca as u128;
                lhs.cmp(&rhs)
            }
            _ => self.tier().cmp(&other.tier()),
        }
    }
}

impl PartialOrd for Seat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Seat {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Seat {}

fn cap_of(state: &MetadataState, ps_id: u64) -> u32 {
    state.ps_slot_caps.get(&ps_id).copied().unwrap_or(0)
}

/// Regions per REGISTERED PS; a registered PS with none reads 0. A region on
/// an unregistered PS is the eviction path's job and counts nowhere.
pub(crate) fn loads(state: &MetadataState) -> BTreeMap<u64, usize> {
    let mut out: BTreeMap<u64, usize> = state.ps_nodes.keys().map(|&id| (id, 0)).collect();
    for region in state.regions.values() {
        if let Some(n) = out.get_mut(&region.ps_id) {
            *n += 1;
        }
    }
    out
}

/// The best home for one more partition, given the current `loads`.
pub(crate) fn pick_home(state: &MetadataState, loads: &BTreeMap<u64, usize>) -> Option<u64> {
    loads
        .iter()
        .min_by(|(a, na), (b, nb)| {
            seat(cap_of(state, **a), **na)
                .cmp(&seat(cap_of(state, **b), **nb))
                .then(a.cmp(b))
        })
        .map(|(&id, _)| id)
}

/// The one move worth considering: take from the PS that is worst off after
/// giving up `slack` partitions, give to the best-ranked other PS, and only if
/// that seat ranks strictly better. `slack = 1` is a real move; a larger slack
/// asks whether the imbalance exceeds a hysteresis band.
///
/// A full cpuset PS is never a target (see the module doc). Excluding it keeps
/// the rest of the argument: `Over` is the worst tier, so if the best target
/// is full, every target is.
///
/// Considering only this pair loses nothing: `seat` never improves as `used`
/// grows, so if the best target does not beat the worst source, no other
/// pairing can.
fn best_move(state: &MetadataState, loads: &BTreeMap<u64, usize>, slack: usize) -> Option<(u64, u64)> {
    let (from, from_seat) = loads
        .iter()
        .filter(|(_, &n)| n >= slack && n > 0)
        .map(|(&id, &n)| (id, seat(cap_of(state, id), n - slack)))
        // max seat; ties → lowest ps_id
        .min_by(|(a, sa), (b, sb)| sb.cmp(sa).then(a.cmp(b)))?;
    let (to, to_seat) = loads
        .iter()
        .filter(|(&id, _)| id != from)
        .map(|(&id, &n)| (id, seat(cap_of(state, id), n)))
        .min_by(|(a, sa), (b, sb)| sa.cmp(sb).then(a.cmp(b)))?;
    (!matches!(to_seat, Seat::Over { .. }) && to_seat < from_seat).then_some((from, to))
}

/// Moves that leave no strictly-improving move behind, at most `max_moves`
/// (`0` = unbounded). The partition moved off a PS is its largest `part_id`.
pub(crate) fn rebalance_moves(state: &MetadataState, max_moves: u32) -> Vec<RebalanceMove> {
    let mut by_ps: BTreeMap<u64, Vec<u64>> =
        state.ps_nodes.keys().map(|&id| (id, Vec::new())).collect();
    for (part_id, region) in &state.regions {
        if let Some(v) = by_ps.get_mut(&region.ps_id) {
            v.push(*part_id);
        }
    }
    for v in by_ps.values_mut() {
        v.sort_unstable();
    }
    let mut loads: BTreeMap<u64, usize> = by_ps.iter().map(|(&id, v)| (id, v.len())).collect();
    let cap = if max_moves == 0 {
        usize::MAX
    } else {
        max_moves as usize
    };
    let mut moves = Vec::new();
    while moves.len() < cap {
        let Some((from, to)) = best_move(state, &loads, 1) else {
            break;
        };
        let part_id = by_ps.get_mut(&from).and_then(|v| v.pop()).expect("source has a partition");
        by_ps.entry(to).or_default().push(part_id);
        *loads.entry(from).or_default() -= 1;
        *loads.entry(to).or_default() += 1;
        moves.push(RebalanceMove {
            part_id,
            from_ps: from,
            to_ps: to,
        });
    }
    moves
}

/// Whether a move would still improve placement after its source gave up
/// `gap_threshold` partitions. Among capacity-unknown PS this is exactly
/// "max − min partition count > gap_threshold".
pub(crate) fn imbalanced(state: &MetadataState, gap_threshold: usize) -> bool {
    best_move(state, &loads(state), gap_threshold.max(1)).is_some()
}

/// `id:used/cap` per registered PS (`cap` is `?` when unknown), for reasons
/// and logs.
pub(crate) fn describe(state: &MetadataState) -> String {
    loads(state)
        .iter()
        .map(|(id, n)| match cap_of(state, *id) {
            0 => format!("{id}:{n}/?"),
            c => format!("{id}:{n}/{c}"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::records::RegionRecord;

    fn state(ps: &[(u64, u32, usize)]) -> MetadataState {
        let mut s = MetadataState::default();
        let mut part_id = 1;
        for &(id, cap, used) in ps {
            s.ps_nodes.insert(id, format!("ps{id}"));
            if cap > 0 {
                s.ps_slot_caps.insert(id, cap);
            }
            for _ in 0..used {
                s.regions.insert(
                    part_id,
                    RegionRecord {
                        part_id,
                        ps_id: id,
                        ..Default::default()
                    },
                );
                part_id += 1;
            }
        }
        s
    }

    fn apply(s: &mut MetadataState, moves: &[RebalanceMove]) {
        for m in moves {
            s.regions.get_mut(&m.part_id).unwrap().ps_id = m.to_ps;
        }
    }

    #[test]
    fn placement_order_free_then_unknown_then_least_overfilled() {
        // 1: cpuset 4 slots, 2 used (2 free); 2: cpuset 2, 1 used (1 free);
        // 3: no cpuset, empty.
        let s = state(&[(1, 4, 2), (2, 2, 1), (3, 0, 0)]);
        assert_eq!(pick_home(&s, &loads(&s)), Some(1), "most free slots first");

        // Both cpuset PS full → the capacity-unknown PS, even though it holds more.
        let s = state(&[(1, 4, 4), (2, 2, 2), (3, 0, 9)]);
        assert_eq!(pick_home(&s, &loads(&s)), Some(3));

        // Only full cpuset PS → lowest fill reached: 1 → 5/4, 2 → 3/2.
        let s = state(&[(1, 4, 4), (2, 2, 2)]);
        assert_eq!(pick_home(&s, &loads(&s)), Some(1));
    }

    #[test]
    fn placement_fills_in_order_as_partitions_arrive() {
        let mut s = state(&[(1, 2, 0), (2, 0, 0)]);
        let mut homes = Vec::new();
        for part_id in 100..104 {
            let home = pick_home(&s, &loads(&s)).unwrap();
            homes.push(home);
            s.regions.insert(
                part_id,
                RegionRecord {
                    part_id,
                    ps_id: home,
                    ..Default::default()
                },
            );
        }
        // Two slots on PS 1 first, then the capacity-unknown PS 2 takes the rest.
        assert_eq!(homes, vec![1, 1, 2, 2]);
    }

    #[test]
    fn without_caps_rebalance_is_count_balancing() {
        let mut s = state(&[(1, 0, 7), (2, 0, 1), (3, 0, 1)]);
        let moves = rebalance_moves(&s, 0);
        apply(&mut s, &moves);
        let l = loads(&s);
        let (max, min) = (l.values().max().unwrap(), l.values().min().unwrap());
        assert!(max - min <= 1, "{l:?}");
        assert!(imbalanced(&state(&[(1, 0, 4), (2, 0, 1)]), 2)); // gap 3 > 2
        assert!(!imbalanced(&state(&[(1, 0, 3), (2, 0, 1)]), 2)); // gap 2
    }

    #[test]
    fn rebalance_fills_free_slots_but_never_overfills_to_do_it() {
        // An unknown-capacity PS with 6, a cpuset PS with 3 free slots.
        let mut s = state(&[(1, 0, 6), (2, 3, 0)]);
        let moves = rebalance_moves(&s, 0);
        assert!(moves.iter().all(|m| m.from_ps == 1 && m.to_ps == 2));
        apply(&mut s, &moves);
        assert_eq!(loads(&s)[&2], 3, "fills PS 2 to its cap and stops there");
        assert!(rebalance_moves(&s, 0).is_empty());
    }

    #[test]
    fn rebalance_drains_overfill_onto_free_slots() {
        let mut s = state(&[(1, 2, 5), (2, 4, 1)]);
        let moves = rebalance_moves(&s, 0);
        apply(&mut s, &moves);
        let l = loads(&s);
        assert_eq!((l[&1], l[&2]), (2, 4), "{l:?}");
    }

    /// Between two full cpuset PS a move only trades which one refuses to
    /// open a partition, and may close one that was serving.
    #[test]
    fn rebalance_never_moves_onto_a_full_cpuset_ps() {
        let s = state(&[(1, 2, 3), (2, 4, 4)]); // fills 3/2 and 5/4 if moved
        assert!(rebalance_moves(&s, 0).is_empty());
        assert!(!imbalanced(&s, 1));
        // Onto a capacity-unknown PS it still moves.
        let s = state(&[(1, 2, 3), (2, 4, 4), (3, 0, 0)]);
        let moves = rebalance_moves(&s, 0);
        assert_eq!(moves.len(), 1);
        assert_eq!((moves[0].from_ps, moves[0].to_ps), (1, 3));
    }

    /// Termination and stability over every small configuration: moves reach a
    /// fixed point, and the fixed point is stable (no move undoes another).
    #[test]
    fn rebalance_reaches_a_fixed_point_from_every_small_start() {
        let caps = [0u32, 1, 2, 3];
        for c1 in caps {
            for c2 in caps {
                for c3 in caps {
                    for n1 in 0..6 {
                        for n2 in 0..6 {
                            for n3 in 0..6 {
                                let mut s = state(&[(1, c1, n1), (2, c2, n2), (3, c3, n3)]);
                                let moves = rebalance_moves(&s, 0);
                                assert!(moves.len() <= n1 + n2 + n3, "{moves:?}");
                                let mut seen = std::collections::HashSet::new();
                                for m in &moves {
                                    assert!(seen.insert(m.part_id), "partition moved twice: {moves:?}");
                                }
                                apply(&mut s, &moves);
                                assert!(rebalance_moves(&s, 0).is_empty());
                            }
                        }
                    }
                }
            }
        }
    }
}
