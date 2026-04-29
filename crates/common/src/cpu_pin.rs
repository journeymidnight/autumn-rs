//! CPU affinity helper used by the per-partition / per-shard / per-bench-worker
//! OS threads in autumn-rs.
//!
//! Policy: at process start, snapshot the available CPU cores via
//! `core_affinity::get_core_ids()` (which honors any cpuset applied by
//! `taskset -c <set> ...`). Each work-unit (partition, extent shard, bench
//! worker) takes one core in ascending order: ord N (0-indexed) → cores[N].
//!
//! If a process has more work-units than cores in its cpuset, the surplus
//! ones log a WARN and stay un-pinned (kernel scheduler picks). Modulo
//! wrapping was rejected — it would force two work-units to fight for
//! one core, which is worse than letting the kernel float them.
//!
//! To restrict the core pool, run the binary under `taskset -c <set> ...`
//! before exec; `get_core_ids()` returns only the cores in the cpuset.
//!
//! The snapshot is cached in a `OnceLock`, so callers can invoke
//! `pick_cpu_for_ord` without re-reading the cpuset on every spawn.

use std::collections::HashSet;
use std::sync::OnceLock;

fn available_cpu_cores() -> &'static [usize] {
    static CELL: OnceLock<Vec<usize>> = OnceLock::new();
    CELL.get_or_init(|| {
        let mut v: Vec<usize> = core_affinity::get_core_ids()
            .map(|ids| ids.into_iter().map(|c| c.id).collect())
            .unwrap_or_default();
        v.sort_unstable();
        v
    })
}

/// Pick the CPU core to pin the `zero_based_ord`-th OS thread to. Returns
/// `None` if either the platform doesn't support affinity (then nothing is
/// pinned) or the ord exceeds the available cpuset (a WARN is logged).
pub fn pick_cpu_for_ord(zero_based_ord: usize) -> Option<usize> {
    let cores = available_cpu_cores();
    if cores.is_empty() {
        return None;
    }
    if zero_based_ord >= cores.len() {
        tracing::warn!(
            ord = zero_based_ord,
            cpu_set_len = cores.len(),
            "more work-units than cores in cpuset; this thread stays unpinned"
        );
        return None;
    }
    Some(cores[zero_based_ord])
}

/// Build a `HashSet` containing exactly `cpu` (or empty if `None`), ready to
/// hand to `compio::runtime::RuntimeBuilder::thread_affinity`.
pub fn affinity_set(cpu: Option<usize>) -> HashSet<usize> {
    let mut set = HashSet::new();
    if let Some(c) = cpu {
        set.insert(c);
    }
    set
}
