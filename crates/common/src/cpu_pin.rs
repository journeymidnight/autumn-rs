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
//!    list. Each EN shard / bench worker takes one
//!    core in ascending order: `ord N → cpuset[N]`. The PS pins nothing:
//!    its cpuset confines the whole process, like `taskset`
//!    (`float_current` on its main thread at start).
//!
//! 2. **`core_affinity::get_core_ids()`**: auto-detected from the
//!    process's mask (`taskset -c <set>` carrying through); ord N pins to
//!    `cores[N]`.
//!
//! If a process has more work-units than cores, `pick_cpu_for_ord` logs a
//! WARN for the surplus and leaves them un-pinned.

use std::sync::OnceLock;

static CPU_SET_OVERRIDE: OnceLock<Vec<usize>> = OnceLock::new();

/// install an explicit cpuset overriding `core_affinity::get_core_ids()`.
/// First-call-wins (matches the tunable pattern); subsequent calls
/// return `false` without mutating the override. Must be called BEFORE the
/// first `pick_cpu_for_ord` or `cpuset_len` call, since both cache the
/// resolved list in a `OnceLock`.
///
/// The cpuset is the final list of cores the process may pin to. Operators
/// running multi-process clusters on one host hand each process a disjoint
/// cpuset.
pub fn set_cpuset(cores: Vec<usize>) -> bool {
    let mut v = cores;
    v.sort_unstable();
    v.dedup();
    CPU_SET_OVERRIDE.set(v).is_ok()
}

/// Taskset-style core list (`4`, `4-11`, `0-3,8,12-15`); the `--cpuset`
/// argument type parses with it.
pub use autumn_params::parse_cpuset;

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
/// The PS reports it as its core capacity; the EN sizes its shard count by
/// it. Returns 0 only on platforms without affinity support.
pub fn cpuset_len() -> usize {
    available_cpu_cores().len()
}

/// returns true iff the operator passed `--cpuset` explicitly
/// (i.e. `set_cpuset` was called). The PS reports a core capacity only
/// then. EN uses it to decide whether to auto-size `--shards` from the
/// cpuset.
pub fn cpuset_explicit() -> bool {
    CPU_SET_OVERRIDE.get().is_some()
}

/// Pick the CPU core to pin the `zero_based_ord`-th OS thread to:
/// `cores[ord]`. `None` if the platform doesn't support affinity (then
/// nothing is pinned) or `ord` is past the cores (a WARN is logged).
pub fn pick_cpu_for_ord(zero_based_ord: usize) -> Option<usize> {
    let cores = available_cpu_cores();
    if cores.is_empty() {
        return None;
    }
    let Some(&core) = cores.get(zero_based_ord) else {
        tracing::warn!(
            ord = zero_based_ord,
            cpu_set_len = cores.len(),
            "more work-units than cores; this thread stays unpinned"
        );
        return None;
    };
    Some(core)
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
        let cores = pinning_pool();
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

/// The cores a process runs its work on (the EN pins its shards to them, the
/// PS confines itself to them): the explicit `--cpuset`, or the mask the
/// process started with. Empty where the OS cannot pin.
pub fn pinning_pool() -> Vec<usize> {
    available_cpu_cores().to_vec()
}

/// Let the calling thread run on any core of `pinning_pool()`. The PS calls it
/// on its main thread at start, before spawning anything: it pins no thread,
/// so every thread it starts inherits the cpuset. A no-op where nothing can be
/// pinned, and off Linux (no affinity masks there). An error when any core
/// asked for cannot be had (outside the cgroup cpuset, offline, out of range).
pub fn float_current() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let pool = pinning_pool();
        if !pool.is_empty() {
            set_mask_exactly(&pool)?;
        }
    }
    Ok(())
}

/// `sched_setaffinity` to `cores`, refusing a narrower result: the kernel
/// silently drops the cores the cgroup does not allow and fails only when none
/// is left.
#[cfg(target_os = "linux")]
fn set_mask_exactly(cores: &[usize]) -> std::io::Result<()> {
    let size = std::mem::size_of::<libc::cpu_set_t>();
    let set = cpu_set_of(cores)?;
    // SAFETY: `set` outlives the call, and the length passed is its size.
    if unsafe { libc::sched_setaffinity(0, size, &set) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: cpu_set_t is plain bits; the kernel fills `got` up to `size`.
    let mut got: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    if unsafe { libc::sched_getaffinity(0, size, &mut got) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: every index is below CPU_SETSIZE.
    let got: Vec<usize> = (0..libc::CPU_SETSIZE as usize)
        .filter(|&c| unsafe { libc::CPU_ISSET(c, &got) })
        .collect();
    let mut want = cores.to_vec();
    want.sort_unstable();
    if got != want {
        return Err(std::io::Error::other(format!(
            "asked for cores {want:?}, allowed only {got:?} (outside the cgroup cpuset, offline, or past CPU {})",
            libc::CPU_SETSIZE - 1
        )));
    }
    Ok(())
}

/// The kernel thread id of the calling thread (`None` off Linux).
pub fn current_tid() -> Option<i32> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: gettid has no arguments and cannot fail.
        Some(unsafe { libc::syscall(libc::SYS_gettid) } as i32)
    }
    #[cfg(not(target_os = "linux"))]
    None
}

#[cfg(target_os = "linux")]
fn cpu_set_of(cores: &[usize]) -> std::io::Result<libc::cpu_set_t> {
    // SAFETY: cpu_set_t is plain bits; all-zero is the empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let mut any = false;
    for &c in cores.iter().filter(|&&c| c < libc::CPU_SETSIZE as usize) {
        // SAFETY: `c` is below CPU_SETSIZE, so the bit is inside `set`.
        unsafe { libc::CPU_SET(c, &mut set) };
        any = true;
    }
    if !any {
        return Err(std::io::Error::other("no core below CPU_SETSIZE"));
    }
    Ok(set)
}

/// `IORING_REGISTER_IOWQ_AFF`: the io-wq of the CALLING task (the one that
/// owns `ring`) runs its workers on `cores` only.
#[cfg(target_os = "linux")]
fn register_io_worker_cores(ring: std::os::fd::RawFd, cores: &[usize]) -> std::io::Result<()> {
    const IORING_REGISTER_IOWQ_AFF: libc::c_uint = 17;
    let set = cpu_set_of(cores)?;
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

    /// A thread spawned by a pinned parent leaves that one core for the pool.
    #[test]
    #[cfg(target_os = "linux")]
    fn float_current_widens_to_the_pool() {
        let pool = pinning_pool();
        if pool.len() < 2 {
            return;
        }
        let ids = |v: Vec<core_affinity::CoreId>| v.into_iter().map(|c| c.id).collect::<Vec<_>>();
        std::thread::spawn(move || {
            pin_current(Some(pool[0])).unwrap();
            assert_eq!(ids(core_affinity::get_core_ids().unwrap()), vec![pool[0]]);
            float_current().unwrap();
            assert_eq!(ids(core_affinity::get_core_ids().unwrap()), pool);
        })
        .join()
        .unwrap();
    }

    /// A core the kernel cannot give (here one that does not exist) is an
    /// error, not a silently narrower mask.
    #[test]
    #[cfg(target_os = "linux")]
    fn a_narrowed_mask_is_refused() {
        let pool = pinning_pool();
        let missing = libc::CPU_SETSIZE as usize - 1;
        if pool.is_empty() || pool.contains(&missing) {
            return;
        }
        std::thread::spawn(move || {
            set_mask_exactly(&pool).unwrap();
            let err = set_mask_exactly(&[pool[0], missing]).unwrap_err();
            assert!(err.to_string().contains("allowed only"), "{err}");
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
