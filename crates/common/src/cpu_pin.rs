//! CPU affinity helper used by the per-partition / per-shard / per-bench-worker
//! OS threads in autumn-rs.
//!
//! Policy: at process start, snapshot the available CPU cores via
//! `core_affinity::get_core_ids()` (which honors any cpuset applied by
//! `taskset -c <set> ...`). Each work-unit (partition, extent shard, bench
//! worker) takes one core in ascending order: ord N (0-indexed) →
//! cores[CPU_OFFSET + N].
//!
//! `CPU_OFFSET` is set once at startup via `set_cpu_offset` (typically from a
//! `--cpu-start` CLI flag). Multi-process clusters (e.g. cluster.sh local
//! deploy: 3 extent-nodes + 1 PS sharing one host) need disjoint offsets so
//! independent processes don't pile their ord=0 threads onto the same core.
//!
//! If a process has more work-units than cores remaining at the offset, the
//! surplus ones log a WARN and stay un-pinned (kernel scheduler picks).
//! Modulo wrapping was rejected — it would force two work-units to fight for
//! one core, which is worse than letting the kernel float them.
//!
//! To restrict the core pool, run the binary under `taskset -c <set> ...`
//! before exec; `get_core_ids()` returns only the cores in the cpuset.

use std::collections::HashSet;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

static CPU_OFFSET: AtomicUsize = AtomicUsize::new(0);

/// Set the global cpu offset added to every `pick_cpu_for_ord` call. Intended
/// to be called once at process startup from a `--cpu-start` CLI flag, before
/// any work-unit threads are spawned.
pub fn set_cpu_offset(offset: usize) {
    CPU_OFFSET.store(offset, Ordering::Relaxed);
}

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
/// pinned) or the resolved index `CPU_OFFSET + ord` exceeds the cpuset
/// (a WARN is logged).
pub fn pick_cpu_for_ord(zero_based_ord: usize) -> Option<usize> {
    let cores = available_cpu_cores();
    if cores.is_empty() {
        return None;
    }
    let offset = CPU_OFFSET.load(Ordering::Relaxed);
    let idx = offset.checked_add(zero_based_ord)?;
    if idx >= cores.len() {
        tracing::warn!(
            ord = zero_based_ord,
            cpu_offset = offset,
            cpu_set_len = cores.len(),
            "more work-units than cores remaining at cpu offset; this thread stays unpinned"
        );
        return None;
    }
    Some(cores[idx])
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
