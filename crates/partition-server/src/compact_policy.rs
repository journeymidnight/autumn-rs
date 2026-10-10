//! Which tables a minor compaction merges. Two independent selectors over the
//! table list, both returning a run CONTIGUOUS IN LIST ORDER (list order is
//! every key's recency order, and the output takes the run's place):
//!
//! - `select_minor` — HBase's `ExploringCompactionPolicy`, to bound the table
//!   count (every point-read miss consults every table's bloom filter).
//! - `select_reclaim` — rewrite the row stream's head extent's last live
//!   tables so the stream's prefix can be truncated. The row stream is only
//!   ever cut as a prefix, so one old table nothing else rewrites (the ratio
//!   rule skips big tables) pins its extent and everything after it.

use std::ops::Range;
use std::sync::OnceLock;

use crate::TableMeta;

/// `autumn-ps --compact-*` knobs. Defaults are HBase's
/// (`hbase.hstore.compaction.{min,max,ratio,min.size}`,
/// `hbase.hstore.blockingStoreFiles`), except `min_size`: HBase uses the
/// flush size because its flushes produce files that big; here a flush SST
/// ranges from KBs (WAL-gap flushes of large-value partitions) to the memtable
/// size. A window under `min_size` skips the ratio, so with `min_size` far
/// above the flush SSTs one growing table joined every couple of flushes and
/// was rewritten each time (4 MiB flushes, 128 MiB: a 125 MB table rewritten
/// 14 times per partition in 30 s, 4K writes 21K → 7.7K ops/s). At 1 MiB only
/// tiny windows skip the ratio, and tables grow by tiers.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MinorPolicy {
    pub min_files: usize,
    pub max_files: usize,
    pub ratio: f64,
    /// A window smaller than this in total is accepted without the ratio test.
    pub min_size: u64,
    /// A table this large or larger is never in a window: merging flush-sized
    /// tables (4K inline values, 256 MiB SSTs) into full ones cost 4K writes
    /// ~15% and buys only halving their count (user, 2026-10-10: performance
    /// must not drop). Only a major or the head-extent reclaim rewrites them.
    pub large_size: u64,
    /// At this many mergeable tables (under the size bounds) a window is picked
    /// even when none passes the ratio.
    pub blocking_files: usize,
}

impl Default for MinorPolicy {
    fn default() -> Self {
        Self {
            min_files: 3,
            max_files: 10,
            ratio: 1.2,
            min_size: 1024 * 1024,
            large_size: 128 * 1024 * 1024,
            blocking_files: 16,
        }
    }
}

static MINOR_POLICY: OnceLock<MinorPolicy> = OnceLock::new();

/// Set the minor-compaction policy (first call wins).
pub fn set_minor_policy(p: MinorPolicy) -> Result<(), String> {
    if p.min_files < 2 {
        return Err(format!("min files {} must be at least 2", p.min_files));
    }
    if p.max_files < p.min_files {
        return Err(format!("max files {} below min files {}", p.max_files, p.min_files));
    }
    if p.large_size == 0 {
        return Err("large size must be positive".to_string());
    }
    if !(p.ratio > 0.0 && p.ratio.is_finite()) {
        return Err(format!("ratio {} must be positive", p.ratio));
    }
    if p.blocking_files < p.min_files {
        return Err(format!(
            "blocking files {} below min files {}",
            p.blocking_files, p.min_files
        ));
    }
    MINOR_POLICY
        .set(p)
        .map_err(|_| "minor compaction policy already set".to_string())
}

pub(crate) fn minor_policy() -> MinorPolicy {
    *MINOR_POLICY.get_or_init(MinorPolicy::default)
}

/// A rewrite reclaims the head extent once its live tables are below this
/// share of its sealed bytes: it rewrites < 30% to free > 70%.
pub(crate) const RECLAIM_LIVE_PERCENT: u64 = 30;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MinorPick {
    /// List indices of the tables to merge.
    pub window: Range<usize>,
    /// No window passed the ratio test; picked because the mergeable table
    /// count reached `blocking_files`.
    pub forced: bool,
}

/// HBase's `ExploringCompactionPolicy.applyCompactionPolicy` over every window
/// of `min_files..=max_files` tables: keep windows whose total is under
/// `min_size` or where no table exceeds `ratio` × the rest; prefer the most
/// tables, then the smallest total. At `blocking_files` mergeable tables (HBase
/// counts every file; here large ones never merge and writes never wait, so
/// counting them would only force pointless windows) the comparison becomes
/// tables-removed per byte (a new window must be 5% better), and with no
/// window in ratio the smallest window is taken.
///
/// A table of `large_size` or more is never in a window (see the field), nor
/// one of `max_table` bytes or more (HBase's `compaction.max.size`). Sizes are
/// SST bytes (`TableMeta.len`), the unit `do_compact` cuts its outputs in (at
/// `max_sst_bytes`). The caller passes 0.6 × that cap: flush tables (at most
/// half of it) stay inside, full
/// outputs stay out, and k ≥ 3 tables under 0.6 × cap total under 0.6·k caps,
/// which cut into fewer than k outputs with room for the bloom, index and
/// packing slack. Without the bound three ~500 MB tables merged back into
/// three ~500 MB tables on every tick.
pub(crate) fn select_minor(
    tables: &[TableMeta],
    p: &MinorPolicy,
    max_table: u64,
) -> Option<MinorPick> {
    let n = tables.len();
    let limit = max_table.min(p.large_size);
    // The blocking count is of the tables a window may hold: counting large
    // ones, 16 of them made a partition "stuck" for good, and the tail window
    // [growing, tiny, tiny] (out of ratio) was then forced every couple of
    // flushes, rewriting the growing table each time.
    let mergeable = tables.iter().filter(|t| t.len < limit).count();
    if mergeable < p.min_files {
        return None;
    }
    let might_be_stuck = mergeable >= p.blocking_files;
    let mut best: Option<(Range<usize>, u64)> = None;
    let mut smallest: Option<(Range<usize>, u64)> = None;
    for start in 0..n {
        let mut size = 0u64;
        for end in start..n.min(start + p.max_files) {
            if tables[end].len >= limit {
                break;
            }
            size += tables[end].len;
            let len = end + 1 - start;
            if len < p.min_files {
                continue;
            }
            if might_be_stuck && smallest.as_ref().is_none_or(|(_, s)| size < *s) {
                smallest = Some((start..end + 1, size));
            }
            if size >= p.min_size && !files_in_ratio(&tables[start..=end], size, p.ratio) {
                continue;
            }
            if is_better(best.as_ref(), len, size, might_be_stuck) {
                best = Some((start..end + 1, size));
            }
        }
    }
    match (best, smallest) {
        (Some((window, _)), _) => Some(MinorPick { window, forced: false }),
        (None, Some((window, _))) => Some(MinorPick { window, forced: true }),
        (None, None) => None,
    }
}

fn files_in_ratio(window: &[TableMeta], total: u64, ratio: f64) -> bool {
    window
        .iter()
        .all(|t| t.len as f64 <= (total - t.len) as f64 * ratio)
}

fn is_better(best: Option<&(Range<usize>, u64)>, len: usize, size: u64, might_be_stuck: bool) -> bool {
    let Some((best_window, best_size)) = best else {
        return true;
    };
    let best_len = best_window.len();
    if might_be_stuck && *best_size > 0 && size > 0 {
        const REPLACE_IF_BETTER_BY: f64 = 1.05;
        return (best_len as f64 / *best_size as f64) * REPLACE_IF_BETTER_BY
            < len as f64 / size as f64;
    }
    len > best_len || (len == best_len && size < *best_size)
}

/// The tables to rewrite so the row stream's head extent `head` (sealed at
/// `head_sealed_len` bytes, not the tail) can be truncated: from its first
/// listed table through its last, at most `max_files`. A window may hold
/// tables of other extents in between (it must be contiguous) and may be a
/// single table. `None` while the head's live tables (their SST bytes) are at
/// least `RECLAIM_LIVE_PERCENT` of it, or it holds none (the truncate after
/// any compaction drops it then).
pub(crate) fn select_reclaim(
    tables: &[TableMeta],
    head: u64,
    head_sealed_len: u64,
    max_files: usize,
) -> Option<Range<usize>> {
    let live: u64 = tables
        .iter()
        .filter(|t| t.extent_id == head)
        .map(|t| t.len)
        .sum();
    if live == 0 || live.saturating_mul(100) >= head_sealed_len.saturating_mul(RECLAIM_LIVE_PERCENT) {
        return None;
    }
    let first = tables.iter().position(|t| t.extent_id == head)?;
    let last = tables.iter().rposition(|t| t.extent_id == head)?;
    Some(first..(last + 1).min(first + max_files.max(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;

    fn t(extent_id: u64, size: u64) -> TableMeta {
        TableMeta {
            extent_id,
            offset: 0,
            len: size,
            estimated_size: size,
            last_seq: 0,
        }
    }

    fn sized(sizes: &[u64]) -> Vec<TableMeta> {
        sizes.iter().map(|&s| t(1, s * MB)).collect()
    }

    /// No size bound unless a test is about it.
    fn pick(sizes: &[u64]) -> Option<MinorPick> {
        select_minor(&sized(sizes), &MinorPolicy::default(), u64::MAX)
    }

    #[test]
    fn below_min_files_nothing() {
        assert_eq!(pick(&[10, 10]), None);
    }

    #[test]
    fn small_tables_merge_up_to_max_files() {
        let p = pick(&[10; 12]).expect("pick");
        assert_eq!(p.window.len(), 10);
        assert!(!p.forced);
    }

    /// The shape that stopped the old selector: one old table alone in the
    /// head extent and many small flushes after it.
    #[test]
    fn a_lone_head_table_does_not_block_the_small_ones() {
        let mut tables = vec![t(10, 50 * MB)];
        for i in 0..34u64 {
            tables.push(t(11 + i / 8, 10 * MB));
        }
        let p = select_minor(&tables, &MinorPolicy::default(), u64::MAX).expect("pick");
        assert!(p.window.len() >= 3 && !p.forced, "{p:?}");
    }

    /// A big old table fails the ratio against small new ones, so the window
    /// is the small tail; it joins once its neighbours are comparable.
    #[test]
    fn ratio_keeps_a_big_table_out_until_the_rest_catches_up() {
        let p = MinorPolicy { large_size: u64::MAX, ..Default::default() };
        let pick = |sizes: &[u64]| select_minor(&sized(sizes), &p, u64::MAX);
        assert_eq!(pick(&[400, 50, 50, 50]).expect("pick").window, 1..4);
        assert_eq!(pick(&[400, 150, 150, 150]).expect("pick").window, 0..4);
    }

    /// Flush-sized tables (4K inline values) never merge, however many there
    /// are; the small ones between them still do.
    #[test]
    fn large_tables_never_merge() {
        let p = MinorPolicy::default();
        let bound = 512 * MB / 5 * 3;
        let big = |n: usize| sized(&vec![258; n]);
        assert_eq!(select_minor(&big(3), &p, bound), None);
        assert_eq!(select_minor(&big(40), &p, bound), None);
        let mixed = sized(&[258, 10, 10, 10, 258]);
        assert_eq!(select_minor(&mixed, &p, bound).expect("pick").window, 1..4);
    }

    /// Windows under `min_size` (1 MiB) skip the ratio test; above it the
    /// same shape fails it.
    #[test]
    fn a_window_under_min_size_ignores_the_ratio() {
        let kb = |sizes: &[u64]| sizes.iter().map(|&k| t(1, k * 1024)).collect::<Vec<_>>();
        let p = MinorPolicy::default();
        assert_eq!(select_minor(&kb(&[800, 16, 16]), &p, u64::MAX).expect("pick").window, 0..3);
        assert_eq!(select_minor(&kb(&[8000, 160, 160]), &p, u64::MAX), None);
    }

    /// Same table count: the smaller total wins (2000 fails the ratio in
    /// every window holding it).
    #[test]
    fn equal_counts_prefer_the_smaller_window() {
        let p = MinorPolicy { large_size: u64::MAX, ..Default::default() };
        let pick = select_minor(&sized(&[300, 300, 300, 2000, 20, 20, 20]), &p, u64::MAX);
        assert_eq!(pick.expect("pick").window, 4..7);
    }

    #[test]
    fn nothing_in_ratio_below_blocking_picks_nothing() {
        let p = MinorPolicy { large_size: u64::MAX, ..Default::default() };
        assert_eq!(select_minor(&sized(&[1000, 200, 30]), &p, u64::MAX), None);
    }

    /// At `blocking_files` tables with no window in ratio, the smallest window
    /// is taken anyway.
    #[test]
    fn stuck_takes_the_smallest_window() {
        // Each table 4x the one before: the newest always exceeds 1.2x the rest.
        let sizes: Vec<u64> = (0..16).map(|i| 128u64 << (2 * i)).collect();
        let policy = MinorPolicy { large_size: u64::MAX, ..Default::default() };
        let pick = |sizes: &[u64]| select_minor(&sized(sizes), &policy, u64::MAX);
        assert_eq!(pick(&sizes[..15]), None, "precondition: nothing in ratio below blocking");
        let p = pick(&sizes).expect("pick");
        assert!(p.forced);
        assert_eq!(p.window, 0..3);
    }

    /// With a 512 MiB output cap the bound is 0.6 of that: three ~500 MB
    /// tables would come out as three again and stay out; flush tables a
    /// little over 256 MiB (random keys encode slightly larger) stay in; the
    /// small ones between big tables still merge.
    #[test]
    fn tables_at_the_bound_stay_out() {
        let bound = 512 * MB / 5 * 3;
        let p = MinorPolicy { large_size: u64::MAX, ..Default::default() };
        assert_eq!(select_minor(&sized(&[500, 500, 500]), &p, bound), None);
        assert_eq!(select_minor(&sized(&[258, 258, 258]), &p, bound).expect("pick").window, 0..3);
        let pick = select_minor(&sized(&[500, 100, 100, 100, 500]), &p, bound).expect("pick");
        assert_eq!(pick.window, 1..4);
    }

    /// Large tables never merge, so they do not count toward the blocking
    /// level: 16 of them plus an out-of-ratio tail must not force a window.
    #[test]
    fn large_tables_do_not_make_a_partition_stuck() {
        let p = MinorPolicy::default();
        let mut tables = sized(&vec![200; 16]);
        tables.extend([t(1, 20 * MB), t(1, 60 * 1024), t(1, 60 * 1024)]);
        assert_eq!(select_minor(&tables, &p, u64::MAX), None);
    }

    #[test]
    fn policy_validation() {
        let ok = MinorPolicy::default();
        assert!(set_minor_policy(MinorPolicy { min_files: 1, ..ok }).is_err());
        assert!(set_minor_policy(MinorPolicy { max_files: 2, ..ok }).is_err());
        assert!(set_minor_policy(MinorPolicy { ratio: 0.0, ..ok }).is_err());
        assert!(set_minor_policy(MinorPolicy { blocking_files: 2, ..ok }).is_err());
        assert!(set_minor_policy(MinorPolicy { large_size: 0, ..ok }).is_err());
    }

    #[test]
    fn reclaim_rewrites_a_mostly_dead_head() {
        // Head extent 10 sealed at 1000 MB, 200 MB of it still listed.
        let tables = vec![t(10, 150 * MB), t(11, 5 * MB), t(10, 50 * MB), t(12, 5 * MB)];
        assert_eq!(select_reclaim(&tables, 10, 1000 * MB, 10), Some(0..3));
        // 300 MB live = 30%: left alone.
        let tables = vec![t(10, 300 * MB), t(11, 5 * MB)];
        assert_eq!(select_reclaim(&tables, 10, 1000 * MB, 10), None);
    }

    #[test]
    fn reclaim_may_be_one_table_and_is_capped() {
        let tables = vec![t(10, 100 * MB), t(11, 5 * MB)];
        assert_eq!(select_reclaim(&tables, 10, 1000 * MB, 10), Some(0..1));
        let mut tables = vec![t(10, MB)];
        tables.extend((0..12).map(|_| t(11, MB)));
        tables.push(t(10, MB));
        assert_eq!(select_reclaim(&tables, 10, 1000 * MB, 10), Some(0..10));
    }

    #[test]
    fn reclaim_needs_a_live_table_in_the_head() {
        let tables = vec![t(11, MB)];
        assert_eq!(select_reclaim(&tables, 10, 1000 * MB, 10), None);
    }
}
