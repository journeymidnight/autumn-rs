//! CPU affinity helper used by the per-partition / per-shard / per-bench-worker
//! OS threads in autumn-rs.
//!
//! ## Policy
//!
//! At process start we snapshot the set of CPU cores this binary is allowed
//! to pin work to. The snapshot comes from one of two sources, in order:
//!
//! 1. **`--cpuset <SPEC>`** (F196): operator-supplied explicit list, e.g.
//!    `4-11`, `4,5,6`, `0-3,8-11`. When present, this is the final
//!    list (offset is ignored). Each work-unit (partition pair, extent
//!    shard, bench worker) takes one core in ascending order:
//!    `ord N → cpuset[N]`.
//!
//! 2. **`core_affinity::get_core_ids()`** (legacy): auto-detected from the
//!    process's cpuset (`taskset -c <set>` carrying through). Combined
//!    with `CPU_OFFSET` set by `--cpu-start N`, ord N pins to
//!    `cores[CPU_OFFSET + N]`. Existing zero-config deployments keep
//!    working unchanged.
//!
//! If a process has more work-units than cores remaining at the resolved
//! offset, the surplus ones log a WARN and stay un-pinned (kernel
//! scheduler picks). Modulo wrapping was rejected — it would force two
//! work-units to fight for one core, which is worse than letting the
//! kernel float them.
//!
//! `--cpuset` and `--cpu-start` are mutually exclusive at the CLI layer;
//! when both are present the binary refuses to start.

use std::collections::HashSet;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

static CPU_OFFSET: AtomicUsize = AtomicUsize::new(0);
static CPU_SET_OVERRIDE: OnceLock<Vec<usize>> = OnceLock::new();

/// Set the global cpu offset added to every `pick_cpu_for_ord` call. Intended
/// to be called once at process startup from a `--cpu-start` CLI flag, before
/// any work-unit threads are spawned.
pub fn set_cpu_offset(offset: usize) {
    CPU_OFFSET.store(offset, Ordering::Relaxed);
}

/// F196: install an explicit cpuset overriding `core_affinity::get_core_ids()`.
/// First-call-wins (matches the F195 tunable pattern); subsequent calls
/// return `false` without mutating the override. Must be called BEFORE the
/// first `pick_cpu_for_ord` or `cpuset_len` call, since both cache the
/// resolved list in a `OnceLock`.
///
/// When this override is active, `CPU_OFFSET` is ignored — the cpuset is
/// taken to be the final list of cores the process may pin to. Operators
/// running multi-process clusters on one host should hand each process a
/// disjoint cpuset directly.
pub fn set_cpuset(cores: Vec<usize>) -> bool {
    let mut v = cores;
    v.sort_unstable();
    v.dedup();
    CPU_SET_OVERRIDE.set(v).is_ok()
}

/// F196: parse a taskset-style cpu list spec into a sorted, deduped
/// vector of core indices. Accepts `<int>` or `<int>-<int>` segments
/// separated by commas, e.g. `"4"`, `"4-11"`, `"0-3,8,12-15"`. The
/// `:groupsize/chunksize` advanced form is intentionally not supported.
pub fn parse_cpuset(spec: &str) -> Result<Vec<usize>, String> {
    let mut out: Vec<usize> = Vec::new();
    for seg in spec.split(',') {
        let seg = seg.trim();
        if seg.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = seg.split_once('-') {
            let lo: usize = lo
                .trim()
                .parse()
                .map_err(|_| format!("cpuset range lo not a non-negative integer: {seg:?}"))?;
            let hi: usize = hi
                .trim()
                .parse()
                .map_err(|_| format!("cpuset range hi not a non-negative integer: {seg:?}"))?;
            if hi < lo {
                return Err(format!("cpuset range hi<lo: {seg:?}"));
            }
            for c in lo..=hi {
                out.push(c);
            }
        } else {
            let c: usize = seg
                .parse()
                .map_err(|_| format!("cpuset segment not a non-negative integer: {seg:?}"))?;
            out.push(c);
        }
    }
    if out.is_empty() {
        return Err("cpuset is empty".to_string());
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn available_cpu_cores() -> &'static [usize] {
    static CELL: OnceLock<Vec<usize>> = OnceLock::new();
    CELL.get_or_init(|| {
        // F196: explicit --cpuset overrides auto-detection when set.
        if let Some(over) = CPU_SET_OVERRIDE.get() {
            return over.clone();
        }
        let mut v: Vec<usize> = core_affinity::get_core_ids()
            .map(|ids| ids.into_iter().map(|c| c.id).collect())
            .unwrap_or_default();
        v.sort_unstable();
        v
    })
}

/// F196: number of cores available for pinning in the resolved cpuset.
/// Used by PS for partition-budget gating and by EN for default shard
/// count. Returns 0 only on platforms without affinity support.
pub fn cpuset_len() -> usize {
    available_cpu_cores().len()
}

/// F196: returns true iff the operator passed `--cpuset` explicitly
/// (i.e. `set_cpuset` was called). PS uses this to decide whether to
/// enforce its partition budget. EN uses it to decide whether to
/// auto-size `--shards` from the cpuset.
pub fn cpuset_explicit() -> bool {
    CPU_SET_OVERRIDE.get().is_some()
}

/// Pick the CPU core to pin the `zero_based_ord`-th OS thread to. Returns
/// `None` if either the platform doesn't support affinity (then nothing is
/// pinned) or the resolved index `CPU_OFFSET + ord` exceeds the cpuset
/// (a WARN is logged). When `--cpuset` is set, `CPU_OFFSET` is ignored
/// and ord N maps directly to `cpuset[N]`.
pub fn pick_cpu_for_ord(zero_based_ord: usize) -> Option<usize> {
    let cores = available_cpu_cores();
    if cores.is_empty() {
        return None;
    }
    // F196: cpuset override = explicit list; ord indexes it directly with
    // no offset. Without override, fall back to legacy CPU_OFFSET + ord.
    let idx = if CPU_SET_OVERRIDE.get().is_some() {
        zero_based_ord
    } else {
        let offset = CPU_OFFSET.load(Ordering::Relaxed);
        offset.checked_add(zero_based_ord)?
    };
    if idx >= cores.len() {
        tracing::warn!(
            ord = zero_based_ord,
            cpu_offset = if CPU_SET_OVERRIDE.get().is_some() {
                0
            } else {
                CPU_OFFSET.load(Ordering::Relaxed)
            },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cpuset_single() {
        assert_eq!(parse_cpuset("4").unwrap(), vec![4]);
    }

    #[test]
    fn parse_cpuset_range() {
        assert_eq!(parse_cpuset("4-7").unwrap(), vec![4, 5, 6, 7]);
    }

    #[test]
    fn parse_cpuset_mixed() {
        assert_eq!(
            parse_cpuset("0-3,8,12-13").unwrap(),
            vec![0, 1, 2, 3, 8, 12, 13]
        );
    }

    #[test]
    fn parse_cpuset_dedup() {
        assert_eq!(parse_cpuset("4-7,5,6,7").unwrap(), vec![4, 5, 6, 7]);
    }

    #[test]
    fn parse_cpuset_empty_errors() {
        assert!(parse_cpuset("").is_err());
        assert!(parse_cpuset(",, ").is_err());
    }

    #[test]
    fn parse_cpuset_reverse_range_errors() {
        assert!(parse_cpuset("7-4").is_err());
    }

    #[test]
    fn parse_cpuset_garbage_errors() {
        assert!(parse_cpuset("a").is_err());
        assert!(parse_cpuset("1-x").is_err());
    }
}
