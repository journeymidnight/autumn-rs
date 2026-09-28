//! CPU affinity helper used by the per-partition / per-shard / per-bench-worker
//! OS threads in autumn-rs.
//!
//! ## Policy
//!
//! At process start we snapshot the set of CPU cores this binary is allowed
//! to pin work to. The snapshot comes from one of two sources, in order:
//!
//! 1. **`--cpuset <SPEC>`**: operator-supplied explicit list, e.g.
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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

static CPU_OFFSET: AtomicUsize = AtomicUsize::new(0);
static CPU_SET_OVERRIDE: OnceLock<Vec<usize>> = OnceLock::new();

/// Set the global cpu offset added to every `pick_cpu_for_ord` call. Intended
/// to be called once at process startup from a `--cpu-start` CLI flag, before
/// any work-unit threads are spawned.
pub fn set_cpu_offset(offset: usize) {
    CPU_OFFSET.store(offset, Ordering::Relaxed);
}

/// install an explicit cpuset overriding `core_affinity::get_core_ids()`.
/// First-call-wins (matches the tunable pattern); subsequent calls
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

/// parse a taskset-style cpu list spec into a sorted, deduped
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
        // explicit --cpuset overrides auto-detection when set.
        if let Some(over) = CPU_SET_OVERRIDE.get() {
            return over.clone();
        }
        if !PLATFORM_PINS {
            return Vec::new();
        }
        let mut v: Vec<usize> = core_affinity::get_core_ids()
            .map(|ids| ids.into_iter().map(|c| c.id).collect())
            .unwrap_or_default();
        v.sort_unstable();
        v
    })
}

/// Whether this OS can bind a thread to one core. macOS cannot: it has only
/// affinity hints, `core_affinity::set_for_current` always fails there, yet
/// `get_core_ids` still reports every core. Auto-detection therefore finds
/// nothing to pin to on such a platform, which is what "no affinity support"
/// means for every caller. An explicit `--cpuset` is still honoured and fails
/// loudly, since the operator asked for it.
const PLATFORM_PINS: bool = cfg!(any(
    target_os = "linux",
    target_os = "android",
    target_os = "windows",
    target_os = "freebsd"
));

/// number of cores available for pinning in the resolved cpuset.
/// Used by PS for partition-budget gating and by EN for default shard
/// count. Returns 0 only on platforms without affinity support.
pub fn cpuset_len() -> usize {
    available_cpu_cores().len()
}

/// returns true iff the operator passed `--cpuset` explicitly
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
    // cpuset override = explicit list; ord indexes it directly with
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

/// Pin the calling thread to `cpu` (no-op for `None`). Every work-unit thread
/// calls this before building its runtime; nothing pins through compio's
/// `RuntimeBuilder::thread_affinity`, because compio intersects the target with
/// the mask the thread inherited and silently binds nothing when they are
/// disjoint. A launcher wrapped in `taskset -c A,B` (or a pinned parent thread)
/// then voided every `--cpuset`, with each thread still logging its "assigned"
/// core. `sched_setaffinity` moves the thread anywhere the enclosing
/// cgroup/cpuset allows, and a core outside that is an error, not a skip.
pub fn pin_current(cpu: Option<usize>) -> std::io::Result<()> {
    if let Some(id) = cpu {
        if !core_affinity::set_for_current(core_affinity::CoreId { id }) {
            return Err(std::io::Error::other(format!(
                "cannot pin thread to CPU {id}"
            )));
        }
    }
    Ok(())
}

/// Keep `rt`'s io_uring worker threads (`iou-wrk-*`) on the cores this
/// process pins its work to. Call it on the work-unit thread, right after
/// building that thread's runtime: io_uring's workers belong to the calling
/// task, so the registration covers exactly this runtime's.
///
/// `pin_current` binds the work-unit thread, but io_uring runs what it cannot
/// complete inline — buffered writes, fsync — on its own worker threads, and
/// those take the whole NUMA node's cores, not the creating thread's affinity.
/// Only a cgroup cpuset or `IORING_REGISTER_IOWQ_AFF` confines them. Without
/// this, an extent node given six `--cpuset` cores was measured running four
/// more cores of page-cache copy and writeback outside them — `--cpuset`
/// silently meant "the shard threads", not "this process".
///
/// Linux before 5.14 has no such registration, and newer kernels refuse cores
/// outside the task's cgroup cpuset: one WARN, and the workers stay where the
/// kernel puts them (isolation lost, not correctness). Nothing to do on the
/// polling driver, which has no io_uring workers, or off Linux.
pub fn confine_io_workers(rt: &compio::runtime::Runtime) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        if !rt.driver_type().is_iouring() {
            return;
        }
        let cores = io_worker_cores();
        if cores.is_empty() {
            return;
        }
        if let Err(e) = register_io_worker_cores(rt.as_raw_fd(), &cores) {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                tracing::warn!(
                    error = %e,
                    ?cores,
                    "cannot confine io_uring worker threads to the cpuset \
                     (Linux < 5.14, or cpuset cores outside the cgroup's); \
                     --cpuset binds only the work-unit threads"
                )
            });
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = rt;
}

/// The cores work units are pinned from: the explicit `--cpuset`, or the
/// detected cores from `--cpu-start` on (to the end of the mask, like the pool
/// `pick_cpu_for_ord` draws from — it can include a co-located process's cores).
#[cfg(target_os = "linux")]
fn io_worker_cores() -> Vec<usize> {
    let cores = available_cpu_cores();
    if CPU_SET_OVERRIDE.get().is_some() {
        return cores.to_vec();
    }
    let offset = CPU_OFFSET.load(Ordering::Relaxed);
    cores.get(offset..).unwrap_or_default().to_vec()
}

/// `IORING_REGISTER_IOWQ_AFF`: the io-wq of the CALLING task (the one that
/// owns `ring`) runs its workers on `cores` only.
#[cfg(target_os = "linux")]
fn register_io_worker_cores(ring: std::os::fd::RawFd, cores: &[usize]) -> std::io::Result<()> {
    const IORING_REGISTER_IOWQ_AFF: libc::c_uint = 17;
    // SAFETY: cpu_set_t is plain bits; all-zero is the empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let mut any = false;
    for &c in cores.iter().filter(|&&c| c < libc::CPU_SETSIZE as usize) {
        // SAFETY: `c` is below CPU_SETSIZE, so the bit is inside `set`.
        unsafe { libc::CPU_SET(c, &mut set) };
        any = true;
    }
    if !any {
        return Err(std::io::Error::other(
            "no core below CPU_SETSIZE to register",
        ));
    }
    // SAFETY: `set` outlives the call, and the length passed is its size.
    let r = unsafe {
        libc::syscall(
            libc::SYS_io_uring_register,
            ring,
            IORING_REGISTER_IOWQ_AFF,
            &set as *const libc::cpu_set_t,
            std::mem::size_of::<libc::cpu_set_t>(),
        )
    };
    if r < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "linux")]
    fn child_can_move_off_its_parents_single_cpu() {
        let cores = core_affinity::get_core_ids().unwrap();
        if cores.len() < 2 {
            return;
        }
        let (a, b) = (cores[0].id, cores[1].id);
        std::thread::spawn(move || {
            pin_current(Some(a)).unwrap();
            std::thread::spawn(move || {
                assert_eq!(core_affinity::get_core_ids().unwrap()[0].id, a);
                pin_current(Some(b)).unwrap();
                assert_eq!(
                    core_affinity::get_core_ids().unwrap(),
                    vec![core_affinity::CoreId { id: b }]
                );
            })
            .join()
            .unwrap();
        })
        .join()
        .unwrap();
    }

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

    /// Registered cores reach the io_uring workers that the runtime spawns
    /// for punted work (buffered writes, fsync) — without it they take the
    /// whole NUMA node.
    #[test]
    #[cfg(target_os = "linux")]
    fn io_workers_follow_the_registered_cores() {
        let target = core_affinity::get_core_ids().unwrap().last().unwrap().id;
        std::thread::spawn(move || {
            use std::os::fd::AsRawFd;
            let rt = compio::runtime::Runtime::new().unwrap();
            assert!(
                rt.driver_type().is_iouring(),
                "io_uring unavailable here; this test needs it"
            );
            register_io_worker_cores(rt.as_raw_fd(), &[target]).unwrap();
            // SAFETY: gettid has no preconditions.
            let me = unsafe { libc::gettid() };
            let dir = std::env::temp_dir().join(format!("iowq-aff-{me}"));
            rt.block_on(async {
                use compio::io::AsyncWriteAtExt;
                let mut f = compio::fs::File::create(&dir).await.unwrap();
                for i in 0..64u64 {
                    f.write_all_at(vec![7u8; 1 << 16], i << 16).await.0.unwrap();
                    f.sync_data().await.unwrap();
                }
            });
            let mut workers = Vec::new();
            for task in std::fs::read_dir("/proc/self/task").unwrap().flatten() {
                let comm = std::fs::read_to_string(task.path().join("comm")).unwrap_or_default();
                if comm.trim() != format!("iou-wrk-{me}") {
                    continue;
                }
                let status = std::fs::read_to_string(task.path().join("status")).unwrap();
                let list = status
                    .lines()
                    .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
                    .unwrap()
                    .trim()
                    .to_string();
                workers.push(parse_cpuset(&list).unwrap());
            }
            std::fs::remove_file(&dir).unwrap();
            assert!(!workers.is_empty(), "no io_uring worker was spawned");
            for w in workers {
                assert_eq!(w, vec![target]);
            }
        })
        .join()
        .unwrap();
    }
}
