//! How much CPU the P-log threads use, and how long they wait for it.
//!
//! The PS pins no thread: its `--cpuset` confines the whole process and the
//! kernel places the threads. Every 2 s `sample` reads each P-log thread's
//! `/proc/self/task/<tid>/schedstat` (CPU time, and run_delay: time runnable
//! but waiting for a CPU); `/metrics` shows the sums. A run_delay that grows
//! toward the CPU figure says the cpuset is too small for the partitions on it.

use std::sync::atomic::{AtomicI32, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Instant;

use autumn_common::metrics_http::{push_metric, push_type};

pub(crate) struct PlogLoad {
    inner: parking_lot::Mutex<Inner>,
    next_lease: AtomicU64,
}

#[derive(Default)]
struct Inner {
    plogs: Vec<Plog>,
    last_sample: Option<Instant>,
    /// P-log CPU time / wall time over the last sample, summed.
    cpu: f64,
    /// P-log run_delay / wall time over the last sample, summed.
    run_delay: f64,
}

struct Plog {
    lease: u64,
    /// Kernel thread id, 0 until the P-log thread has started.
    tid: Arc<AtomicI32>,
    /// `(cpu_ns, run_delay_ns)` at the last sample.
    last: Option<(u64, u64)>,
}

/// One P-log's place in the sample; dropping it removes the P-log.
pub(crate) struct PlogLease {
    load: Arc<PlogLoad>,
    lease: u64,
    tid: Arc<AtomicI32>,
}

impl PlogLease {
    /// Where the P-log thread records its kernel thread id.
    pub(crate) fn tid_cell(&self) -> Arc<AtomicI32> {
        self.tid.clone()
    }
}

impl Drop for PlogLease {
    fn drop(&mut self) {
        self.load.inner.lock().plogs.retain(|p| p.lease != self.lease);
    }
}

impl PlogLoad {
    pub(crate) fn new() -> Self {
        Self {
            inner: parking_lot::Mutex::new(Inner::default()),
            next_lease: AtomicU64::new(0),
        }
    }

    pub(crate) fn register(self: &Arc<Self>) -> PlogLease {
        let lease = self.next_lease.fetch_add(1, Relaxed);
        let tid = Arc::new(AtomicI32::new(0));
        self.inner.lock().plogs.push(Plog { lease, tid: tid.clone(), last: None });
        PlogLease { load: self.clone(), lease, tid }
    }

    pub(crate) fn sample(&self) {
        self.sample_with(Instant::now(), read_schedstat);
    }

    fn sample_with(&self, now: Instant, read: impl Fn(i32) -> Option<(u64, u64)>) {
        let mut inner = self.inner.lock();
        let elapsed_ns = inner
            .last_sample
            .map(|t| now.saturating_duration_since(t).as_nanos() as f64);
        inner.last_sample = Some(now);
        let (mut cpu_ns, mut delay_ns) = (0u64, 0u64);
        for p in &mut inner.plogs {
            let tid = p.tid.load(Relaxed);
            let Some(stat) = (tid > 0).then(|| read(tid)).flatten() else {
                continue;
            };
            if let Some((c0, d0)) = p.last {
                cpu_ns += stat.0.saturating_sub(c0);
                delay_ns += stat.1.saturating_sub(d0);
            }
            p.last = Some(stat);
        }
        if let Some(dt) = elapsed_ns.filter(|&dt| dt > 0.0) {
            inner.cpu = cpu_ns as f64 / dt;
            inner.run_delay = delay_ns as f64 / dt;
        }
    }

    /// PS-wide gauges for `/metrics`.
    pub(crate) fn render(&self, out: &mut String) {
        let inner = self.inner.lock();
        for (name, value) in [
            ("autumn_ps_plog_threads", inner.plogs.len() as f64),
            ("autumn_ps_plog_cpu_ratio", inner.cpu),
            ("autumn_ps_plog_run_delay_ratio", inner.run_delay),
        ] {
            push_type(out, name, "gauge");
            push_metric(out, name, &[], value);
        }
    }
}

/// `(cpu_ns, run_delay_ns)` of one of this process's threads (run_delay stays 0
/// on a kernel without CONFIG_SCHEDSTATS, unless delay accounting is on).
fn read_schedstat(tid: i32) -> Option<(u64, u64)> {
    let s = std::fs::read_to_string(format!("/proc/self/task/{tid}/schedstat")).ok()?;
    let mut f = s.split_whitespace().map(|v| v.parse::<u64>().ok());
    Some((f.next()??, f.next()??))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn ratios_sum_the_plogs_and_a_dropped_lease_leaves() {
        let load = Arc::new(PlogLoad::new());
        let (a, b) = (load.register(), load.register());
        a.tid_cell().store(100, Relaxed);
        b.tid_cell().store(200, Relaxed);
        let stats = parking_lot::Mutex::new(HashMap::from([(100, (0, 0)), (200, (0, 0))]));
        let read = |tid| stats.lock().get(&tid).copied();
        let t0 = Instant::now();
        load.sample_with(t0, read);
        // Over 2 s: 1.6 s + 0.4 s of CPU, 40 ms + 0 of run_delay.
        stats.lock().extend([(100, (1_600_000_000, 40_000_000)), (200, (400_000_000, 0))]);
        load.sample_with(t0 + Duration::from_secs(2), read);
        let mut out = String::new();
        load.render(&mut out);
        assert!(out.contains("autumn_ps_plog_threads 2\n"), "{out}");
        assert!(out.contains("autumn_ps_plog_cpu_ratio 1\n"), "{out}");
        assert!(out.contains("autumn_ps_plog_run_delay_ratio 0.02\n"), "{out}");
        drop(a);
        let mut out = String::new();
        load.render(&mut out);
        assert!(out.contains("autumn_ps_plog_threads 1\n"), "{out}");
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn schedstat_of_this_thread_reads() {
        let tid = autumn_common::cpu_pin::current_tid().unwrap();
        // The kernel adds CPU time at a tick or a switch: burn some, then sleep.
        let t = Instant::now();
        while t.elapsed() < std::time::Duration::from_millis(5) {
            std::hint::spin_loop();
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        let (cpu, _) = read_schedstat(tid).expect("own schedstat");
        assert!(cpu > 0);
    }
}
