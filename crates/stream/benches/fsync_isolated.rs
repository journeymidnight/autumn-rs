//! Isolated `fdatasync` latency micro-benchmark.
//!
//! Bypasses the entire autumn-rs stack (rpc / partition / stream-client / ExtentNode)
//! and measures the disk-side fsync cost directly via `std::fs::write` +
//! `libc::fdatasync`. The kernel fsync path is identical whether the EN
//! reaches it through compio's io_uring `sync_data` or through a blocking
//! `std::fs` call — `io_uring` only hides the thread blocking, the disk
//! still does the same flush.
//!
//! Use this when investigating "the write path feels slow" to attribute
//! the cost to one of: (a) disk fsync hardware/contention, (b) EN
//! coalescer overhead, (c) autumn stack overhead (rpc + 3-replica fanout
//! + memtable + ack join). The bench answers (a); the gap between this
//! and `extent_bench` answers (b); the gap between `extent_bench` and
//! `autumn-client perf-check` answers (c).
//!
//! Two scenarios per directory:
//!
//!  * **Per-write fsync** — write + fdatasync per op. Models the
//!    pre-coalescer ceiling (every Put = one fsync, no coalescing).
//!  * **Group commit** — K writes then 1 fdatasync. Models the
//!    coalescer's amortized cost. K=16 mirrors `--depth 8` batch sizes;
//!    K=256 mirrors `MAX_WRITE_BATCH`.
//!
//! Run:
//!
//! ```bash
//! cargo bench -p autumn-stream --bench fsync_isolated
//!
//! # Target specific dirs (default scans /data03 /data05 /data08 /tmp):
//! AUTUMN_FSYNC_BENCH_DIRS=/data05,/data08 cargo bench -p autumn-stream \
//!   --bench fsync_isolated
//! ```
//!
//! Healthy NVMe baseline (3.5 TB consumer NVMe, idle):
//!   per-write 4 K  : p50 ~ 60-70 µs, p99 < 700 µs
//!   per-write 8 M  : p50 ~ 9-11 ms (data-write bound, NOT fsync)
//!   group K=16  4K : amortized 60-100 K ops/s/disk
//!   group K=256 4K : amortized 130-190 K ops/s/disk
//!
//! Run iostat alongside to spot tenant contention — a `f_await` in
//! the 18-128 ms range while this bench is running indicates a
//! co-tenant is pinning the fsync queue.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

fn percentile(samples: &mut [u128], p: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let idx = ((samples.len() as f64) * p) as usize;
    samples[idx.min(samples.len() - 1)]
}

fn fdatasync(fd: i32) {
    let rc = unsafe { libc::fdatasync(fd) };
    if rc != 0 {
        let errno = unsafe { *libc::__errno_location() };
        panic!("fdatasync failed: errno={errno}");
    }
}

fn bench_per_write_fsync(path: &Path, size: usize, n: usize) {
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .expect("open bench file");
    let mut latencies = Vec::with_capacity(n);
    let t_total = Instant::now();
    for _ in 0..n {
        let t = Instant::now();
        f.write_all(&payload).expect("write");
        fdatasync(f.as_raw_fd());
        latencies.push(t.elapsed().as_micros());
    }
    let total = t_total.elapsed();
    let p50 = percentile(&mut latencies, 0.50);
    let p99 = percentile(&mut latencies, 0.99);
    let max = *latencies.iter().max().unwrap();
    let ops_s = n as f64 / total.as_secs_f64();
    let mbps = (size as f64) * (n as f64) / total.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "  per-write-fsync N={n} size={size}B: p50={p50}µs p99={p99}µs max={max}µs  \
         ops/s={ops_s:.0}  MB/s={mbps:.1}"
    );
    let _ = std::fs::remove_file(path);
}

fn bench_group_commit(path: &Path, size: usize, group: usize, n_groups: usize) {
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .expect("open bench file");
    let mut latencies = Vec::with_capacity(n_groups);
    let t_total = Instant::now();
    let total_ops = group * n_groups;
    for _ in 0..n_groups {
        let t = Instant::now();
        for _ in 0..group {
            f.write_all(&payload).expect("write");
        }
        fdatasync(f.as_raw_fd());
        latencies.push(t.elapsed().as_micros());
    }
    let total = t_total.elapsed();
    let p50 = percentile(&mut latencies, 0.50);
    let p99 = percentile(&mut latencies, 0.99);
    let max = *latencies.iter().max().unwrap();
    let ops_s = total_ops as f64 / total.as_secs_f64();
    let mbps = (size as f64) * (total_ops as f64) / total.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "  group-commit-K={group:3} N={n_groups} size={size}B: p50={p50}µs p99={p99}µs \
         max={max}µs  amortized_ops/s={ops_s:.0}  MB/s={mbps:.1}"
    );
    let _ = std::fs::remove_file(path);
}

fn main() {
    let dirs: Vec<PathBuf> = match std::env::var("AUTUMN_FSYNC_BENCH_DIRS") {
        Ok(v) => v.split(',').filter(|s| !s.is_empty()).map(PathBuf::from).collect(),
        Err(_) => ["/data03", "/data05", "/data08", "/tmp"]
            .iter()
            .map(PathBuf::from)
            .collect(),
    };
    println!(
        "fsync_isolated bench — dirs={:?}\n\
         (set AUTUMN_FSYNC_BENCH_DIRS=/path1,/path2 to override)\n",
        dirs
    );

    for dir in &dirs {
        if !dir.exists() {
            println!("=== {} : SKIP (not present)", dir.display());
            continue;
        }
        println!("\n=== {}  (one-shot, no concurrency)", dir.display());
        let bench_path = dir.join("_autumn_fsync_bench.tmp");
        bench_per_write_fsync(&bench_path, 4096, 200);
        bench_per_write_fsync(&bench_path, 8 * 1024 * 1024, 50);
        bench_group_commit(&bench_path, 4096, 16, 200);
        bench_group_commit(&bench_path, 4096, 256, 100);
        bench_group_commit(&bench_path, 8 * 1024 * 1024, 4, 25);
    }
}
