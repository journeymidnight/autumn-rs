//! Which nodes a replica goes on.
//!
//! One answer, two callers. Allocation (`select_nodes`) and recovery
//! (`dispatch_recovery_task`) are the same question — "of the nodes allowed to
//! hold this extent, which should" — and they used to answer it differently:
//! allocation shuffled, recovery took the lowest `node_id` that got past the
//! rate limiter. The second is worse than the first. A shuffle is unbiased but
//! does not converge (balls-in-bins variance is inherent); ascending id is an
//! ACTIVE systematic bias, and it produced the migration where draining one
//! node sent 12 of its 27 shards onto the two nodes queued to be
//! decommissioned next, while four brand-new empty nodes received nothing —
//! the old nodes simply had smaller ids and the walk stopped at the first
//! candidate with limiter headroom.
//!
//! Everything here is pure. The store borrows, the RPCs and the rate limiter
//! live at the call sites; what is left is the part that decides, which is the
//! part worth testing in isolation.

use std::cmp::Ordering;
use std::collections::HashMap;

use rand::Rng;

/// Utilization is compared in bands this many percentage points wide.
///
/// Not a raw comparison, for two reasons. The input is a `df` sample up to two
/// seconds old, so sub-band differences are noise being read as signal. And
/// exact ordering would make utilization lexicographically dominant: a node
/// 1% emptier would win however many open tails it is already carrying, which
/// is the wrong trade — within a few percent of each other on capacity, which
/// node is a write hotspot is the more useful question.
///
/// The unit is the NODE, not a disk: `total_bytes` sums a node's online disks,
/// so on a 3-disk 3.5 TB machine one band is ~525 GB — about thirty extents at
/// the 16 GiB default. Nodes inside one band are genuinely comparable on
/// capacity.
///
/// Band edges are hard and there is no hysteresis: 84.9% and 85.1% land in
/// different bands and utilization then decides outright. That is the noise
/// the banding is meant to suppress, reappearing at 1/20th of the boundaries —
/// tolerable because the consequence is a mis-ranked pair, not a wrong
/// placement, and because the alternative (remembering the previous band per
/// node) is state that has to be right across a leader change.
pub(crate) const UTILIZATION_BAND_PERCENT: u64 = 5;

/// Sorted after every known band, so a node we have no `df` for loses to any
/// node we do — but ties with other unknowns, which is what keeps a cold
/// leader (no `df` sweep yet, every node unknown) placing exactly as it does
/// today instead of refusing.
const UNKNOWN_BAND: u64 = u64::MAX;

/// What placement knows about one node.
///
/// Deliberately small, and deliberately only things the manager already
/// collects: `used`/`total` are the per-disk sums `cluster_cap.per_node`
/// publishes every `df` tick, and the two counts ride the chunked periodic
/// scan of `s.extents` that already computes `logical_stored`. Nothing here
/// needs new telemetry, which is why it can ship without an agent change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct NodeLoad {
    /// Σ per-disk `extent_bytes` across ONLINE disks — the real footprint.
    pub used_bytes: u64,
    /// Σ per-disk capacity across ONLINE disks. `0` = no `df` yet (unknown).
    pub total_bytes: u64,
    /// Slots this node holds whose extent is still OPEN.
    ///
    /// Separate from `shards` because they measure different things and only
    /// one of them predicts the near future. A sealed extent is CAPACITY: it
    /// is immutable, it will never grow, and its cost is already in
    /// `used_bytes`. An open one is LOAD: it is an append target that will
    /// keep taking writes until it rolls. A node that just received ten new
    /// tails still reports almost no bytes, so on capacity alone it looks like
    /// the emptiest node in the cluster and keeps winning — right up until
    /// those ten extents fill.
    pub open_extents: u64,
    /// Every slot this node holds, open or sealed. Tie-break only.
    pub shards: u64,
}

impl NodeLoad {
    /// Which utilization band this node is in; lower is emptier.
    fn utilization_band(&self) -> u64 {
        if self.total_bytes == 0 {
            return UNKNOWN_BAND;
        }
        let percent = self
            .used_bytes
            .saturating_mul(100)
            .checked_div(self.total_bytes)
            .unwrap_or(0);
        percent / UTILIZATION_BAND_PERCENT
    }
}

/// Order two nodes by how much we want to place on them; `Less` is better.
///
/// Banded lexicographic rather than a weighted sum of terms. A sum needs
/// weights that trade bytes against counts, and nobody can say what "ten open
/// tails are worth N% of a disk" means — so the number would be picked to make
/// a test pass and then never revisited. Levels need no cross-unit constant,
/// each one is independently testable, and a later term (a failure domain, say)
/// is inserted as a level rather than tuned against everything else.
///
/// `Equal` is a real answer and the caller MUST break it randomly. Falling back
/// to `node_id` is what this whole module exists to remove.
pub(crate) fn compare_load(a: &NodeLoad, b: &NodeLoad) -> Ordering {
    let (band_a, band_b) = (a.utilization_band(), b.utilization_band());
    if band_a != band_b {
        return band_a.cmp(&band_b);
    }
    if a.open_extents != b.open_extents {
        return a.open_extents.cmp(&b.open_extents);
    }
    a.shards.cmp(&b.shards)
}

/// How many candidates to sample per pick.
///
/// Power of `d` choices, not `argmin`, and ONLY where nothing else spreads the
/// decision. Taking the global minimum herds: the emptiest node wins every
/// concurrent allocation at once, so a freshly added node absorbs the whole
/// cluster's writes until it stops being the emptiest — and the input is a
/// `df` sample seconds old and slot counts a scan cycle old, so "stops being"
/// is observed long after it happened. Sampling bounds the over-share: in a
/// 7-node cluster the emptiest node is in a 2-sample about 29% of the time
/// rather than 100%, so it fills at roughly twice its share. It stops winning
/// once its utilization crosses a band, which at 5% of a node is a lot of
/// extents — the taper is measured in bands, not in allocations.
///
/// Recovery does NOT use this — see `order_by_load`.
pub(crate) const SAMPLE_SIZE: usize = 2;

/// Pick up to `count` distinct nodes, least loaded first.
///
/// Each pick samples `SAMPLE_SIZE` of what is left and keeps the best; ties
/// resolve to whichever was sampled first, and sampling is uniform, so equal
/// nodes are chosen uniformly. That property is load-bearing: with no load
/// data at all every node compares `Equal`, and this degrades to exactly the
/// uniform random subset the shuffle used to produce.
///
/// `pool` is consumed.
pub(crate) fn pick_least_loaded<R: Rng>(
    mut pool: Vec<(u64, NodeLoad)>,
    count: usize,
    rng: &mut R,
) -> Vec<u64> {
    let mut picked = Vec::with_capacity(count.min(pool.len()));
    while picked.len() < count && !pool.is_empty() {
        let sample = SAMPLE_SIZE.min(pool.len());
        // Sample WITHOUT replacement by drawing from a shrinking prefix: swap
        // each draw to the front so it cannot be drawn twice. With replacement
        // a 2-sample would compare a node against itself ~1/n of the time,
        // quietly turning that fraction of picks into a uniform pick.
        for i in 0..sample {
            let j = i + rng.gen_range(0..pool.len() - i);
            pool.swap(i, j);
        }
        let mut best = 0usize;
        for i in 1..sample {
            if compare_load(&pool[i].1, &pool[best].1) == Ordering::Less {
                best = i;
            }
        }
        picked.push(pool.swap_remove(best).0);
    }
    picked
}

/// Every candidate, least loaded first, ties in random order.
///
/// Recovery uses this rather than sampling, because the thing sampling defends
/// against cannot happen here. `RecoveryRateLimiter` caps concurrent rebuilds
/// per target (`max_per_target`, default 2) and the dispatch walk SKIPS a
/// capped candidate and takes the next — so a burst already spreads down the
/// list on its own: two to the emptiest node, two to the next, and so on.
/// Sampling on top of that buys no protection and costs accuracy: with three
/// loaded nodes among seven, one first pick in seven still went to a loaded
/// node, which on the 27-shard drain that motivated all this is a handful of
/// shards still landing on machines queued for decommission.
///
/// Ties are shuffled BEFORE the sort, and the sort is stable, so equal nodes
/// come out in random order. Without that, equal nodes would emerge in
/// `HashMap` iteration order — arbitrary, but fixed for a given process, which
/// is the same failure as ordering by id with extra steps.
pub(crate) fn order_by_load<R: Rng>(mut pool: Vec<(u64, NodeLoad)>, rng: &mut R) -> Vec<u64> {
    for i in (1..pool.len()).rev() {
        pool.swap(i, rng.gen_range(0..=i));
    }
    pool.sort_by(|a, b| compare_load(&a.1, &b.1));
    pool.into_iter().map(|(id, _)| id).collect()
}

/// Assemble the pool a placement decision runs on.
///
/// `eligible` is the caller's hard-constraint result — this function does not
/// re-derive it. Which nodes MAY hold the extent is a property of the extent
/// and the cluster (membership, operator overrides, disk health, free-space
/// floor, and for recovery the rate limiter), and every one of those already
/// has an owner. Scoring must never be able to overrule them, so it is not
/// given the chance to see them.
pub(crate) fn pool_for<I: IntoIterator<Item = u64>>(
    eligible: I,
    load: &HashMap<u64, NodeLoad>,
) -> Vec<(u64, NodeLoad)> {
    eligible
        .into_iter()
        .map(|id| (id, load.get(&id).copied().unwrap_or_default()))
        .collect()
}

// A NOTE FOR WHOEVER ADDS FAILURE DOMAINS
//
// Deliberately absent: any notion of rack or availability zone. The cluster is
// single-zone today, so the constraint could never fire — an unexercised
// branch in the placement path, which is the shape this codebase keeps finding
// in its own green test runs.
//
// When it is added, it MUST be a preference with a node-level floor, never
// "the K+M shards must land in K+M distinct domains". A hard rule refuses
// every allocation the moment a cluster has fewer domains than replicas, which
// is today's cluster on its first day. The right shape is: spread across as
// many domains as exist, and fall back to node-level distinctness (`occupied`,
// which the callers already enforce) when there are not enough. It goes in as
// a level in `compare_load`, above utilization.

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn load(used: u64, total: u64, open: u64, shards: u64) -> NodeLoad {
        NodeLoad {
            used_bytes: used,
            total_bytes: total,
            open_extents: open,
            shards,
        }
    }

    #[test]
    fn an_emptier_node_wins_once_the_difference_is_a_whole_band() {
        let empty = load(0, 1000, 0, 0);
        let half = load(500, 1000, 0, 0);
        assert_eq!(compare_load(&empty, &half), Ordering::Less);
        assert_eq!(compare_load(&half, &empty), Ordering::Greater);
    }

    /// Sub-band differences are `df` noise, not signal, and must not be able to
    /// outrank a real write-hotspot difference.
    #[test]
    fn inside_one_band_the_write_hotspot_decides() {
        // 10% vs 14% — same band, so utilization says nothing.
        let quiet = load(140, 1000, 0, 0);
        let hot = load(100, 1000, 9, 0);
        assert_eq!(
            compare_load(&quiet, &hot),
            Ordering::Less,
            "a node carrying nine open tails must lose to one carrying none, \
             even though it holds slightly fewer bytes"
        );
    }

    #[test]
    fn shards_only_break_a_tie() {
        let a = load(100, 1000, 2, 5);
        let b = load(100, 1000, 2, 50);
        assert_eq!(compare_load(&a, &b), Ordering::Less);
        assert_eq!(compare_load(&a, &a), Ordering::Equal);
    }

    /// A node we have no `df` for must not read as empty — otherwise every
    /// unknown node looks like the best target in the cluster.
    #[test]
    fn an_unmeasured_node_loses_to_a_measured_one_but_ties_with_unknowns() {
        let unknown = load(0, 0, 0, 0);
        let nearly_full = load(990, 1000, 0, 0);
        assert_eq!(compare_load(&nearly_full, &unknown), Ordering::Less);
        assert_eq!(compare_load(&unknown, &unknown), Ordering::Equal);
    }

    /// The property the two existing `select_nodes` tests rely on: with no load
    /// data every node compares equal, and the pick degrades to the uniform
    /// random subset the shuffle used to give.
    #[test]
    fn with_nothing_measured_the_pick_is_uniform() {
        let mut rng = StdRng::seed_from_u64(7);
        let mut counts: HashMap<u64, usize> = HashMap::new();
        const ITERS: usize = 4000;
        for _ in 0..ITERS {
            let pool = pool_for([1u64, 3, 5, 7], &HashMap::new());
            for id in pick_least_loaded(pool, 3, &mut rng) {
                *counts.entry(id).or_insert(0) += 1;
            }
        }
        // 3 of 4 per draw ⇒ each node ~75%.
        for id in [1u64, 3, 5, 7] {
            let c = *counts.get(&id).unwrap_or(&0);
            assert!(
                (2700..=3300).contains(&c),
                "node {id} picked {c}/{ITERS}, expected ~3000"
            );
        }
    }

    /// The migration in one test: four empty nodes beside three loaded ones,
    /// and the loaded ones have the LOWEST ids — the exact arrangement the old
    /// ascending-id walk sent every rebuild to.
    #[test]
    fn rebuilds_flow_to_the_empty_nodes_not_the_lowest_ids() {
        let mut load_map = HashMap::new();
        for id in [1u64, 3, 5] {
            load_map.insert(id, load(900, 1000, 4, 45));
        }
        for id in [102u64, 104, 106, 108] {
            load_map.insert(id, load(0, 1000, 0, 0));
        }
        let mut rng = StdRng::seed_from_u64(11);
        let mut to_new = 0usize;
        const ITERS: usize = 2000;
        for _ in 0..ITERS {
            let pool = pool_for([1u64, 3, 5, 102, 104, 106, 108], &load_map);
            let first = pick_least_loaded(pool, 1, &mut rng)[0];
            if first >= 102 {
                to_new += 1;
            }
        }
        // A 2-sample from 3 loaded + 4 empty contains an empty node
        // 1 - C(3,2)/C(7,2) = 18/21 = 85.7% of the time, and an empty node
        // always wins when present. The threshold sits well BELOW that mean,
        // not on it: at 85 this test would fail about half the time, which it
        // duly did. Against the bias it exists to catch — ascending node id,
        // where the loaded nodes hold the low ids — the rate is 0%.
        assert!(
            to_new * 100 / ITERS >= 75,
            "only {to_new}/{ITERS} rebuilds went to the empty nodes"
        );
    }

    /// Sampling must be without replacement: with it, a 2-sample compares a
    /// node against itself often enough to launder that fraction of picks into
    /// uniform ones, which is invisible in aggregate but real.
    #[test]
    fn the_loaded_node_is_never_chosen_over_an_empty_one() {
        let mut load_map = HashMap::new();
        load_map.insert(1u64, load(990, 1000, 0, 0));
        load_map.insert(2u64, load(0, 1000, 0, 0));
        let mut rng = StdRng::seed_from_u64(3);
        for _ in 0..500 {
            let pool = pool_for([1u64, 2], &load_map);
            assert_eq!(
                pick_least_loaded(pool, 1, &mut rng)[0],
                2,
                "with the whole pool sampled, the emptier node must always win"
            );
        }
    }

    /// Recovery walks its ordering until the rate limiter lets one through, so
    /// the ordering must contain every candidate exactly once.
    #[test]
    fn a_full_ordering_keeps_every_candidate() {
        let mut rng = StdRng::seed_from_u64(5);
        let pool = pool_for([1u64, 3, 5, 7], &HashMap::new());
        let mut order = order_by_load(pool, &mut rng);
        assert_eq!(order.len(), 4);
        order.sort_unstable();
        assert_eq!(order, vec![1, 3, 5, 7]);
    }

    /// Full ordering is exact, unlike sampling: every loaded node comes after
    /// every empty one, with no fraction left over.
    #[test]
    fn a_full_ordering_puts_every_empty_node_ahead_of_every_loaded_one() {
        let mut load_map = HashMap::new();
        for id in [1u64, 3, 5] {
            load_map.insert(id, load(900, 1000, 4, 45));
        }
        for id in [102u64, 104, 106, 108] {
            load_map.insert(id, load(0, 1000, 0, 0));
        }
        let mut rng = StdRng::seed_from_u64(13);
        for _ in 0..200 {
            let pool = pool_for([1u64, 3, 5, 102, 104, 106, 108], &load_map);
            let order = order_by_load(pool, &mut rng);
            assert!(
                order[..4].iter().all(|id| *id >= 102),
                "the four empty nodes must occupy the first four places: {order:?}"
            );
        }
    }

    /// Equal nodes must not come out in whatever order the caller's map
    /// happened to hand over — a fixed arbitrary order is the same failure as
    /// ordering by id, just harder to see.
    #[test]
    fn a_full_ordering_breaks_ties_at_random() {
        let mut rng = StdRng::seed_from_u64(17);
        let mut first_seen: std::collections::HashSet<u64> =
            std::collections::HashSet::new();
        for _ in 0..200 {
            let pool = pool_for([1u64, 3, 5, 7], &HashMap::new());
            first_seen.insert(order_by_load(pool, &mut rng)[0]);
        }
        assert_eq!(first_seen.len(), 4, "got {first_seen:?}");
    }

    #[test]
    fn asking_for_more_than_the_pool_holds_returns_the_pool() {
        let mut rng = StdRng::seed_from_u64(9);
        let pool = pool_for([1u64, 3], &HashMap::new());
        assert_eq!(pick_least_loaded(pool, 10, &mut rng).len(), 2);
    }
}
