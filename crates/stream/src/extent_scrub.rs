//! Pacing and candidate selection for the background content scrub.
//!
//! The scrub exists because detection must not depend on someone reading. A
//! replica that rots while idle is found here, or not at all: the read check
//! only sees blocks a read fully covers, and a cold extent is never read.
//!
//! It has a second job that is easy to miss. There is no seal event on an
//! extent node — the manager seals in its own metadata and a replicated
//! extent's holder learns lazily — so a rolled tail can sit with no checksum
//! sidecar indefinitely. The scrub BACKFILLS those. That half is
//! trust-on-first-use: content already at rest with no prior digest gets
//! whatever it currently holds recorded as truth. It is still strictly better
//! than no checksum, which protects nothing at any time.
//!
//! Everything here is pure. The walk, the reads and the reporting need the
//! node; the decisions about how fast to go and what to look at next do not,
//! and they are where the mistakes would be silent.

/// How much content the scrub may read per second, PER SHARD.
///
/// A scrub competes with live traffic for the same disks, and its whole value
/// is that it runs continuously in the background — so it is paced by BYTES,
/// not by extents. An extent-per-tick budget would read a 16 GiB extent as
/// eagerly as a 1 MiB one.
///
/// Per shard, not per node: the loop is spawned once per `ExtentNode` and an
/// EN runs one of those per shard, so a 64-shard node reads at 64× this. Set it
/// with that multiplier in mind — the honest node-level figure is
/// `SCRUB_DEFAULT_BYTES_PER_SEC × shard_count`.
pub(crate) const SCRUB_DEFAULT_BYTES_PER_SEC: u64 = 8 * 1024 * 1024;

/// A byte allowance that refills over time.
///
/// Deliberately NOT a general token bucket: it never accumulates more than one
/// tick's worth. A scrub that has been idle (nothing to do, or the node was
/// busy) must not earn the right to a burst that competes with live reads —
/// the point is a ceiling on interference per shard, not a throughput
/// guarantee. It bounds the SCRUB's reads only: a seal observed on a control
/// path hashes its extent unpaced, on the caller that observed it.
///
/// `take` debits what it grants even when the grant is short, so a caller that
/// needs more than a tick holds must be able to make PARTIAL progress and come
/// back. Demanding a whole extent up front is how backfill silently died for
/// every extent larger than one second's budget.
#[derive(Debug)]
pub(crate) struct ScrubBudget {
    bytes_per_sec: u64,
    available: u64,
}

impl ScrubBudget {
    pub(crate) fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            available: 0,
        }
    }

    /// Grant this tick's allowance. `elapsed_ms` is time since the last refill.
    pub(crate) fn refill(&mut self, elapsed_ms: u64) {
        let earned = self
            .bytes_per_sec
            .saturating_mul(elapsed_ms.min(1000))
            / 1000;
        // Cap at one second's worth: no burst credit for having been idle.
        self.available = earned.min(self.bytes_per_sec);
    }

    /// Take up to `want`, returning what was granted. Zero means "wait".
    pub(crate) fn take(&mut self, want: u64) -> u64 {
        let granted = want.min(self.available);
        self.available -= granted;
        granted
    }

    pub(crate) fn available(&self) -> u64 {
        self.available
    }
}

/// Where the scrub is in its sweep.
///
/// The cursor is an extent id, not an index: the set of extents changes under
/// the walk (allocation, deletion, reassignment), and an index would silently
/// re-scan or skip when it shifts. Resuming from "the first id greater than the
/// last one I looked at" is stable under both.
#[derive(Debug, Default)]
pub(crate) struct ScrubCursor {
    last: Option<u64>,
}

impl ScrubCursor {
    /// The next extent to examine from a sorted candidate list, and advance.
    ///
    /// Wraps to the beginning when it runs off the end, so a node with a small
    /// stable set keeps re-verifying it rather than stopping — rot is not a
    /// one-time event and a sweep that finishes has nothing to do next.
    pub(crate) fn next(&mut self, sorted_candidates: &[u64]) -> Option<u64> {
        if sorted_candidates.is_empty() {
            self.last = None;
            return None;
        }
        // Binary search, not a scan. Every skipped candidate (in-flight,
        // open past the probe budget, vanished) costs one of these with no
        // await in between, so a linear find makes an all-skipped pass
        // quadratic in the candidate count — a shard holding thousands of
        // extents would block its event loop on comparisons alone.
        let pick = match self.last {
            None => sorted_candidates[0],
            Some(last) => {
                let i = sorted_candidates.partition_point(|id| *id <= last);
                if i < sorted_candidates.len() {
                    sorted_candidates[i]
                } else {
                    sorted_candidates[0]
                }
            }
        };
        self.last = Some(pick);
        Some(pick)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_budget_paces_and_never_banks_a_burst() {
        let mut b = ScrubBudget::new(1000);
        b.refill(1000);
        assert_eq!(b.available(), 1000, "a full second earns a full allowance");
        assert_eq!(b.take(400), 400);
        assert_eq!(b.take(1000), 600, "only what is left");
        assert_eq!(b.take(1), 0, "exhausted means wait");

        // Half a second earns half.
        b.refill(500);
        assert_eq!(b.available(), 500);

        // Idle for a minute: still only one second's worth. A scrub that has
        // been quiet must not repay itself with a burst against live reads.
        b.refill(60_000);
        assert_eq!(b.available(), 1000, "idle time must not bank credit");
    }

    #[test]
    fn the_cursor_sweeps_in_order_and_wraps() {
        let mut c = ScrubCursor::default();
        let ids = vec![3u64, 7, 11];
        assert_eq!(c.next(&ids), Some(3));
        assert_eq!(c.next(&ids), Some(7));
        assert_eq!(c.next(&ids), Some(11));
        assert_eq!(c.next(&ids), Some(3), "a finished sweep starts again");
    }

    /// The candidate set changes under the walk. Resuming by ID rather than by
    /// index is what keeps that from silently skipping or repeating extents.
    #[test]
    fn the_cursor_survives_the_set_changing_underneath_it() {
        let mut c = ScrubCursor::default();
        assert_eq!(c.next(&[3, 7, 11]), Some(3));
        // 5 is allocated: the walk picks it up without rewinding.
        assert_eq!(c.next(&[3, 5, 7, 11]), Some(5));
        // 7 is deleted: resume past 5, skipping the hole rather than stalling.
        assert_eq!(c.next(&[3, 5, 11]), Some(11));
        // Everything after the cursor is gone: wrap rather than stop.
        assert_eq!(c.next(&[3]), Some(3));
        // An empty node has nothing to do and forgets where it was.
        assert_eq!(c.next(&[]), None);
        assert_eq!(c.next(&[3, 5]), Some(3));
    }
}
