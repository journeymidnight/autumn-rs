#[cfg(unix)]
extern crate libc;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use autumn_client::ClusterClient;
use serde::{Deserialize, Serialize};

// (F213: hex_split_ranges moved to autumn_op.rs along with `bootstrap`.)

/// F099-N-c — generate a bench key guaranteed to lie in the partition
/// identified by `start_key`. Returns an ASCII string (valid UTF-8). The write
/// and read perf-check phases call this with the SAME (tid, seq) so the read
/// phase regenerates exactly the keys the write phase stored (F246-B).
///
/// Strategy: for empty `start_key` (first partition [""..X)), prefix with
/// "!" (0x21, smaller than any hex digit '0'..'f' = 0x30..0x66). For any
/// other partition, prefix the key with `start_key + "!"` — this is
/// strictly >= start_key and strictly < the next partition's start_key
/// (because `start_key + "!"` shares the full `start_key` prefix and then
/// '!' = 0x21 is smaller than any trailing character of the next split).
fn key_for_partition(start_key: &[u8], tag: &str, tid: usize, seq: u64) -> String {
    let mut key = Vec::with_capacity(start_key.len() + tag.len() + 32);
    if !start_key.is_empty() {
        key.extend_from_slice(start_key);
    }
    key.push(b'!');
    key.extend_from_slice(tag.as_bytes());
    key.push(b'_');
    key.extend_from_slice(tid.to_string().as_bytes());
    key.push(b'_');
    key.extend_from_slice(seq.to_string().as_bytes());
    // SAFETY: all inputs are ASCII (start_key is hex digits, tag is ASCII,
    // tid/seq are decimal digits, separators are '!' and '_').
    String::from_utf8(key).expect("ASCII bench key")
}

/// F-NS-PRINCIPAL-UNIFIED: the benchmarks bind the scope `bench/perf`
/// (Prepend `bench/perf/`), so their keys are USER keys.
const BENCH_SCOPE: &str = "bench/perf";

/// F-KEY-NS D7: derive the USER-space partition start keys covering the bench
/// namespace. `all_partitions_with_range` returns WIRE ranges; the client
/// prepends `perf/bench/`, so a bench key built off a partition's wire start
/// would route into the WRONG namespace. Instead, take each partition whose wire
/// start lies under `perf/bench/`, strip that prefix, and feed the remainder to
/// `key_for_partition` — the binding re-prepends `perf/bench/`, landing the key
/// back in that partition. Requires the `bench` namespace to be presplit for
/// multi-partition spread; a non-presplit bench yields a single empty start (one
/// partition), so the perf run measures one partition (documented in ops.md).
fn bench_user_starts(partitions: &[(u64, String, Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    let prefix = format!("{BENCH_SCOPE}/").into_bytes();
    let mut starts: Vec<Vec<u8>> = Vec::new();
    for (_pid, _addr, start, _end) in partitions {
        if let Some(rest) = start.strip_prefix(prefix.as_slice()) {
            starts.push(rest.to_vec());
        }
    }
    // Always cover the namespace's lower bound (the first bench partition may
    // start at/below `bench/perf/`, whose stripped start is empty).
    if !starts.iter().any(|s| s.is_empty()) {
        starts.push(Vec::new());
    }
    starts.sort();
    starts.dedup();
    starts
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------


mod args;
use args::{parse_args, Command};

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct BenchConfig {
    threads: usize,
    duration_secs: u64,
    value_size: usize,
    report_interval_secs: u64,
    part_id: Option<u64>,
    reuse_value: bool,
    #[serde(default)]
    partition_count: usize,
    #[serde(default)]
    group_commit_cap: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone)]
struct BenchSummaryRecord {
    total_ops: u64,
    total_bytes: u64,
    ops_per_sec: f64,
    throughput_mb_per_sec: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Serialize, Deserialize)]
struct PerfBaseline {
    version: u32,
    write: BenchSummaryRecord,
    read: BenchSummaryRecord,
    config: BenchConfig,
    recorded_at: u64,
}

// (F213: Info{Disk,Node,Extent,Stream,Discard,Partition,Snapshot}View
// moved to autumn_op.rs along with the `info` subcommand.)

struct LatencyHist {
    samples_ms: Vec<f64>,
}

impl LatencyHist {
    fn percentile(&mut self, p: f64) -> f64 {
        if self.samples_ms.is_empty() {
            return 0.0;
        }
        self.samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * self.samples_ms.len() as f64) as usize;
        self.samples_ms[idx.min(self.samples_ms.len() - 1)]
    }
}

fn print_bench_summary(
    label: &str,
    threads: usize,
    value_size: usize,
    elapsed: Duration,
    total_ops: u64,
    latencies: &mut LatencyHist,
) -> BenchSummaryRecord {
    let secs = elapsed.as_secs_f64();
    let total_bytes = total_ops * value_size as u64;
    let total_bytes_f64 = total_bytes as f64;
    let p50_ms = latencies.percentile(50.0);
    let p95_ms = latencies.percentile(95.0);
    let p99_ms = latencies.percentile(99.0);
    let ops_per_sec = total_ops as f64 / secs.max(1e-9);
    let throughput_mb_per_sec = total_bytes_f64 / 1024.0 / 1024.0 / secs.max(1e-9);
    println!("\nSummary");
    println!("Threads         : {threads}");
    if value_size > 0 {
        println!("Value size      : {value_size} bytes");
    }
    println!("Time taken      : {:.3} seconds", secs);
    println!("Complete ops    : {total_ops}");
    println!(
        "Total data      : {:.2} MB",
        total_bytes_f64 / 1024.0 / 1024.0
    );
    println!("Ops/sec         : {:.2}", ops_per_sec);
    println!("Throughput/sec  : {:.2} MB/s", throughput_mb_per_sec);
    println!(
        "{} latency p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        label, p50_ms, p95_ms, p99_ms,
    );
    BenchSummaryRecord {
        total_ops,
        total_bytes,
        ops_per_sec,
        throughput_mb_per_sec,
        p50_ms,
        p95_ms,
        p99_ms,
    }
}

// (F213: derive_control_address + format_disk moved to autumn_op.rs
// along with the `format` subcommand.)

// ---------------------------------------------------------------------------
// YCSB-equivalent mixed workload
// ---------------------------------------------------------------------------

/// Gray et al. Zipfian generator (YCSB default, theta ≈ 0.99). Maps a uniform
/// `u` in [0,1) to an item index in [0, n) with a power-law skew — index 0 is
/// hottest. Setup is O(n) (computes zeta(n)); cheap per-draw.
struct Zipf {
    n: f64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
    n_minus_1: u64,
}

impl Zipf {
    fn new(n: u64, theta: f64) -> Self {
        let nf = n as f64;
        let zeta = |m: f64| -> f64 {
            let mut s = 0.0;
            let mut i = 1.0;
            while i <= m {
                s += 1.0 / i.powf(theta);
                i += 1.0;
            }
            s
        };
        let zeta2 = zeta(2.0);
        let zetan = zeta(nf);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / nf).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Zipf { n: nf, theta, alpha, zetan, eta, n_minus_1: n.saturating_sub(1) }
    }
    fn pick(&self, u: f64) -> u64 {
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        let ret = (self.n * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        ret.min(self.n_minus_1)
    }
}

/// (p50, p95, p99) in the units of `v`. Sorts `v` in place.
fn pcts(v: &mut [f64]) -> (f64, f64, f64) {
    if v.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    let idx = |p: f64| ((n - 1) as f64 * p) as usize;
    (v[idx(0.50)], v[idx(0.95)], v[idx(0.99)])
}

#[allow(clippy::too_many_arguments)]
async fn cmd_ycsb(
    client: &ClusterClient,
    threads: usize,
    duration_secs: u64,
    value_size: usize,
    partitions_flag: usize,
    pipeline_depth: usize,
    read_ratio: f64,
    zipfian: bool,
    records: u64,
    rmw: bool,
    manager: &str,
) -> Result<()> {
    use futures::stream::StreamExt;
    use rand::{Rng, SeedableRng};

    let depth = pipeline_depth.max(1);
    let records = records.max(1);
    let partitions = client.all_partitions_with_range().await?;
    if partitions.is_empty() {
        bail!("no partitions found, run bootstrap first");
    }
    if partitions.len() != partitions_flag {
        eprintln!(
            "warning: --partitions={} but cluster has {} partitions; using cluster value",
            partitions_flag,
            partitions.len()
        );
    }
    let workload = if rmw {
        "F (read-modify-write)".to_string()
    } else {
        format!("read_ratio={read_ratio}")
    };
    println!(
        "==> ycsb: {threads} threads, {duration_secs}s, {value_size}B records, \
         {records} keys/thread, dist={}, workload {workload}",
        if zipfian { "zipfian" } else { "uniform" }
    );

    // Each thread owns one partition's key range and its own [0,records) keyspace.
    // F-KEY-NS D7: user-space starts within the bench namespace (see
    // `bench_user_starts`); the client re-prepends `bench/perf/`.
    let bench_starts = bench_user_starts(&partitions);
    let start_keys: Vec<Vec<u8>> = (0..threads)
        .map(|tid| bench_starts[tid % bench_starts.len()].clone())
        .collect();
    let mgr = Arc::new(manager.to_string());

    // ---- LOAD phase (YCSB load): insert `records` keys per thread ----
    let load_ops = Arc::new(AtomicU64::new(0));
    let load_start = Instant::now();
    let mut load_handles = Vec::new();
    for (tid, sk) in start_keys.iter().cloned().enumerate() {
        let mgr = Arc::clone(&mgr);
        let load_ops = Arc::clone(&load_ops);
        let value_bytes: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        load_handles.push(std::thread::spawn(move || {
            compio::runtime::RuntimeBuilder::new().build().unwrap().block_on(async move {
                let client = match ClusterClient::connect(&mgr, BENCH_SCOPE).await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("ycsb load thread {tid} connect error: {e}");
                        return;
                    }
                };
                let cref = &client;
                let skref = sk.as_slice();
                let vref = value_bytes.as_slice();
                let mut id = 0u64;
                let futs = std::iter::from_fn(move || {
                    if id >= records {
                        return None;
                    }
                    let key = key_for_partition(skref, "ycsb", tid, id);
                    id += 1;
                    Some(async move { cref.put(key.as_bytes(), vref).await.is_ok() })
                });
                let mut s = autumn_client::fan_out(futs, depth);
                let mut n = 0u64;
                while let Some((_, ok)) = s.next().await {
                    if ok {
                        n += 1;
                    }
                }
                load_ops.fetch_add(n, Ordering::Relaxed);
            })
        }));
    }
    for h in load_handles {
        let _ = h.join();
    }
    let load_el = load_start.elapsed().as_secs_f64().max(1e-9);
    let loaded = load_ops.load(Ordering::Relaxed);
    println!(
        "LOAD: {loaded} keys in {load_el:.1}s = {:.0} ops/s",
        loaded as f64 / load_el
    );

    // ---- RUN phase: one mixed loop, read_ratio reads / rest updates ----
    let run_ops = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let run_start = Instant::now();
    let mut run_handles = Vec::new();
    for (tid, sk) in start_keys.iter().cloned().enumerate() {
        let mgr = Arc::clone(&mgr);
        let run_ops = Arc::clone(&run_ops);
        let value_bytes: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        run_handles.push(std::thread::spawn(move || -> (Vec<f64>, Vec<f64>) {
            compio::runtime::RuntimeBuilder::new().build().unwrap().block_on(async move {
                let client = match ClusterClient::connect(&mgr, BENCH_SCOPE).await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("ycsb run thread {tid} connect error: {e}");
                        return (Vec::new(), Vec::new());
                    }
                };
                let cref = &client;
                let skref = sk.as_slice();
                let vref = value_bytes.as_slice();
                let mut rng = rand::rngs::StdRng::seed_from_u64(0x59CB_0000 ^ tid as u64);
                let zipf = if zipfian { Some(Zipf::new(records, 0.99)) } else { None };
                let mut read_lats: Vec<f64> = Vec::new();
                let mut write_lats: Vec<f64> = Vec::new();
                let futs = std::iter::from_fn(move || {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    let is_read = rng.gen::<f64>() < read_ratio;
                    let id = match &zipf {
                        Some(z) => z.pick(rng.gen::<f64>()),
                        None => rng.gen_range(0..records),
                    };
                    let key = key_for_partition(skref, "ycsb", tid, id);
                    Some(async move {
                        let t0 = Instant::now();
                        // kind: 0=read (read latency), 1=write/rmw (write latency)
                        if rmw {
                            let _ = cref.get(key.as_bytes()).await;
                            let ok = cref.put(key.as_bytes(), vref).await.is_ok();
                            (1u8, ok, t0.elapsed())
                        } else if is_read {
                            let ok = cref.get(key.as_bytes()).await.is_ok();
                            (0u8, ok, t0.elapsed())
                        } else {
                            let ok = cref.put(key.as_bytes(), vref).await.is_ok();
                            (1u8, ok, t0.elapsed())
                        }
                    })
                });
                let mut s = autumn_client::fan_out(futs, depth);
                let mut n = 0u64;
                while let Some((_, (kind, ok, el))) = s.next().await {
                    if ok {
                        n += 1;
                        let ms = el.as_secs_f64() * 1000.0;
                        if kind == 0 {
                            read_lats.push(ms);
                        } else {
                            write_lats.push(ms);
                        }
                    }
                }
                run_ops.fetch_add(n, Ordering::Relaxed);
                (read_lats, write_lats)
            })
        }));
    }
    let mut all_read: Vec<f64> = Vec::new();
    let mut all_write: Vec<f64> = Vec::new();
    for h in run_handles {
        if let Ok((r, w)) = h.join() {
            all_read.extend(r);
            all_write.extend(w);
        }
    }
    let run_el = run_start.elapsed().as_secs_f64().max(1e-9);
    let total = run_ops.load(Ordering::Relaxed);
    println!(
        "\nRUN: {total} ops in {run_el:.1}s = {:.0} ops/s",
        total as f64 / run_el
    );
    if !all_read.is_empty() {
        let (p50, p95, p99) = pcts(&mut all_read);
        println!(
            "  read : {} ops  p50={p50:.2}ms p95={p95:.2}ms p99={p99:.2}ms",
            all_read.len()
        );
    }
    if !all_write.is_empty() {
        let (p50, p95, p99) = pcts(&mut all_write);
        println!(
            "  {} : {} ops  p50={p50:.2}ms p95={p95:.2}ms p99={p99:.2}ms",
            if rmw { "rmw  " } else { "write" },
            all_write.len()
        );
    }
    Ok(())
}

// Main
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn cmd_perf_check(client: &ClusterClient, threads: usize, duration_secs: u64, value_size: usize, baseline_file: String, threshold: f64, update_baseline: bool, partitions_meta_from_flag: usize, pipeline_depth: usize, group_commit_cap: Option<usize>, bulk: usize, ramp_ms: u64, direct_read: bool, manager: &str) -> Result<()> {
    let pipeline_depth = pipeline_depth.max(1);
    // ZC ("ucx ⟹ zerocopy") selection — ONE symmetric rule (F235), shared
    // with the python BatchClient + `get_many_into` via `zc_worthwhile`:
    // engage ZC iff value >= 64 KiB, for BOTH reads and writes and BOTH
    // transports. Mirrors the PS recv gates (client UCX_ZC_READ_MIN_BYTES +
    // PS AUTUMN_PS_ZC_RECV_MIN_BYTES, both 64 KiB): below 64 KiB the per-op
    // registered/pooled-recv machinery exceeds the copy saved (small UCX read
    // -18%) and the PS recv doesn't ZC anyway. (F234 kept an asymmetric WRITE
    // rule `is_ucx || large`; F235 dropped `is_ucx ||` — small UCX writes only
    // saved client-side allocs while the PS still FrameDecoder-copied, i.e.
    // not real end-to-end ZC.)
    let zc_write = autumn_client::zc_worthwhile(value_size);
    let zc_read = autumn_client::zc_worthwhile(value_size);
    let zc_tag = match (zc_write, zc_read) {
        (true, true) => " [ZC: MSG_PUT_ZC + MSG_GET_ZC]",
        (true, false) => " [ZC: MSG_PUT_ZC; read regular]",
        _ => "",
    };
    // ---- Write phase ----
    if pipeline_depth > 1 {
        println!(
            "==> perf-check: write ({threads} threads, {duration_secs}s, {value_size}B, depth={pipeline_depth}){zc_tag}"
        );
    } else {
        println!(
            "==> perf-check: write ({threads} threads, {duration_secs}s, {value_size}B){zc_tag}"
        );
    }

    // F099-N-c: use `all_partitions_with_range` so each thread can
    // generate keys that fall inside its assigned partition's range.
    let partitions = client.all_partitions_with_range().await?;
    if partitions.is_empty() {
        bail!("no partitions found, run bootstrap first");
    }
    if partitions.len() != partitions_meta_from_flag {
        eprintln!(
            "warning: --partitions={} but cluster has {} partitions; using cluster value",
            partitions_meta_from_flag,
            partitions.len()
        );
    }

    // F246-B: per-thread `ClusterClient`; continuous pipelining via the
    // shared `fan_out` streaming primitive. A lazy, deadline-bounded
    // iterator yields ONE single-op future per key; `fan_out(.., depth)`
    // keeps `depth` in-flight (sliding window) until the deadline. Each
    // future is one `kv_put` (put_zc for ZC, else put) — kv_put is the unit
    // the fan-out composes. The SDK routes per key; no per-partition striping.
    // F-KEY-NS D7: user-space starts within the bench namespace (see
    // `bench_user_starts`); the client re-prepends `bench/perf/`.
    let bench_starts = bench_user_starts(&partitions);
    let start_keys: Vec<Vec<u8>> = (0..threads)
        .map(|tid| bench_starts[tid % bench_starts.len()].clone())
        .collect();
    let mgr = Arc::new(manager.to_string());

    let deadline =
        Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
    let total_ops = Arc::new(AtomicU64::new(0));
    let bench_start = Instant::now();

    let barrier = Arc::new(std::sync::Barrier::new(start_keys.len()));
    let mut write_handles = Vec::new();
    for (tid, start_key) in start_keys.iter().cloned().enumerate() {
        let mgr = Arc::clone(&mgr);
        let deadline = Arc::clone(&deadline);
        let total_ops = Arc::clone(&total_ops);
        let value_bytes = (0..value_size)
            .map(|i| (i % 256) as u8)
            .collect::<Vec<u8>>();
        let depth = pipeline_depth;

        let barrier = Arc::clone(&barrier);
        let handle = std::thread::spawn(move || {
            if ramp_ms > 0 {
                std::thread::sleep(Duration::from_millis(tid as u64 * ramp_ms));
            }
            compio::runtime::RuntimeBuilder::new()
                .build()
                .unwrap()
                .block_on(async move {
                    use futures::stream::StreamExt;
                    let client = match autumn_client::ClusterClient::connect(&mgr, BENCH_SCOPE).await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("write thread {tid} connect error: {e}");
                            // #1: a connect-failed thread MUST still satisfy the
                            // fixed-size barrier (armed under --ramp-ms), else
                            // the threads that DID connect wait forever and
                            // main's join() hangs.
                            if ramp_ms > 0 {
                                barrier.wait();
                            }
                            return (Vec::<f64>::new(), 0u64);
                        }
                    };
                    let value_zc: bytes::Bytes = bytes::Bytes::from(value_bytes);
                    let mut lats: Vec<f64> = Vec::new();
                    let mut written = 0u64;
                    let cref = &client;
                    // F258-bench: with --ramp-ms the timed window
                    // starts only after EVERY thread has connected
                    // (worker created) — measure steady state, not
                    // the creation ramp.
                    let mut local_dl = *deadline.as_ref();
                    if ramp_ms > 0 {
                        barrier.wait();
                        local_dl = std::time::SystemTime::now()
                            + Duration::from_secs(duration_secs);
                    }
                    let dl = &local_dl;
                    let vz = &value_zc;
                    let sk = start_key.as_slice();
                    let mut seq = 0u64;
                    // --bulk N → one put_many per round of N items.
                    // SDK groups by partition, emits one
                    // MSG_BATCH_PUT per group for small values,
                    // per-op MSG_PUT_ZC for ≥ 64 KiB.
                    if bulk > 0 {
                        while std::time::SystemTime::now() < *dl {
                            let keys: Vec<String> = (0..bulk)
                                .map(|_| {
                                    let k = key_for_partition(sk, "pc", tid, seq);
                                    seq += 1;
                                    k
                                })
                                .collect();
                            let items: Vec<(&[u8], bytes::Bytes, u64)> = keys
                                .iter()
                                .map(|k| (k.as_bytes(), vz.clone(), 0u64))
                                .collect();
                            let t0 = Instant::now();
                            let res = cref.put_many(&items).await;
                            let el = t0.elapsed();
                            let n_ok = res.iter().filter(|r| r.is_ok()).count();
                            if n_ok > 0 {
                                total_ops.fetch_add(n_ok as u64, Ordering::Relaxed);
                                written += n_ok as u64;
                                let per_op_ms =
                                    el.as_secs_f64() * 1000.0 / n_ok as f64;
                                for _ in 0..n_ok {
                                    lats.push(per_op_ms);
                                }
                            }
                        }
                        return (lats, written);
                    }
                    // Deadline-bounded lazy source of single put futures.
                    let futs = std::iter::from_fn(move || {
                        if std::time::SystemTime::now() >= *dl {
                            return None;
                        }
                        let key = key_for_partition(sk, "pc", tid, seq);
                        seq += 1;
                        let val = vz.clone();
                        Some(async move {
                            let t0 = Instant::now();
                            let ok = if zc_write {
                                cref.put_zc(key.as_bytes(), val).await.is_ok()
                            } else {
                                cref.put(key.as_bytes(), val.as_ref()).await.is_ok()
                            };
                            (ok, t0.elapsed())
                        })
                    });
                    let mut s = autumn_client::fan_out(futs, depth);
                    while let Some((_, (ok, el))) = s.next().await {
                        if ok {
                            total_ops.fetch_add(1, Ordering::Relaxed);
                            written += 1;
                            lats.push(el.as_secs_f64() * 1000.0);
                        }
                    }
                    (lats, written)
                })
        });
        write_handles.push(handle);
    }

    let total_ops_w = Arc::clone(&total_ops);
    // #4: stop flag so the progress printer actually exits at end-of-phase
    // (dropping the JoinHandle does NOT stop the thread — it would keep
    // printing `[write] ops/s` across the read phase / summary).
    let stop_w = Arc::new(AtomicBool::new(false));
    let stop_w_c = Arc::clone(&stop_w);
    let progress_w = std::thread::spawn(move || {
        let mut last = 0u64;
        while !stop_w_c.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));
            let cur = total_ops_w.load(Ordering::Relaxed);
            eprint!("\r[write] ops/s={}", cur - last);
            last = cur;
        }
    });

    let mut all_write_latencies: Vec<f64> = Vec::new();
    let mut written_per_thread: Vec<u64> = vec![0; threads];
    for (tid, h) in write_handles.into_iter().enumerate() {
        if let Ok((lats, written)) = h.join() {
            all_write_latencies.extend(lats);
            written_per_thread[tid] = written;
        }
    }
    stop_w.store(true, Ordering::Relaxed);
    let _ = progress_w.join();
    eprintln!();

    let write_elapsed = bench_start.elapsed();
    let write_ops = total_ops.load(Ordering::Relaxed);
    let mut write_hist = LatencyHist {
        samples_ms: all_write_latencies,
    };
    let write_summary = print_bench_summary(
        "Write",
        threads,
        value_size,
        write_elapsed,
        write_ops,
        &mut write_hist,
    );

    if written_per_thread.iter().all(|&w| w == 0) {
        bail!("write phase produced no keys — is the cluster running?");
    }

    // ---- Read phase ----
    if pipeline_depth > 1 {
        println!(
            "\n==> perf-check: read ({threads} threads, {duration_secs}s, depth={pipeline_depth})"
        );
    } else {
        println!("\n==> perf-check: read ({threads} threads, {duration_secs}s)");
    }

    // F246-B: read phase mirrors the write phase — per-thread ClusterClient,
    // continuous pipelining via `fan_out` streaming. Keys are regenerated
    // deterministically (the same `key_for_partition(start_key, "pc", tid,
    // seq)` the write phase used, cycling seq in `0..written`), so every
    // read hits a key the write phase actually stored. Each future is one
    // kv_get: `get_into` into a per-future dest for ZC (>= 64 KiB), else `get`.
    let written_per_thread = Arc::new(written_per_thread);
    let deadline =
        Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
    let total_ops = Arc::new(AtomicU64::new(0));
    let bench_start = Instant::now();

    let read_participants = written_per_thread.iter().filter(|w| **w > 0).count();
    let barrier = Arc::new(std::sync::Barrier::new(read_participants.max(1)));
    let mut read_handles = Vec::new();
    for (tid, start_key) in start_keys.iter().cloned().enumerate() {
        let written = written_per_thread[tid];
        if written == 0 {
            continue;
        }
        let mgr = Arc::clone(&mgr);
        let deadline = Arc::clone(&deadline);
        let total_ops = Arc::clone(&total_ops);
        let depth = pipeline_depth;
        let barrier = Arc::clone(&barrier);
        let handle = std::thread::spawn(move || {
            if ramp_ms > 0 {
                std::thread::sleep(Duration::from_millis(tid as u64 * ramp_ms));
            }
            compio::runtime::RuntimeBuilder::new()
                .build()
                .unwrap()
                .block_on(async move {
                    use futures::stream::StreamExt;
                    let client = match autumn_client::ClusterClient::connect(&mgr, BENCH_SCOPE).await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("read thread {tid} connect error: {e}");
                            // #1: satisfy the barrier so connected threads don't hang.
                            if ramp_ms > 0 {
                                barrier.wait();
                            }
                            return Vec::<f64>::new();
                        }
                    };
                    let mut lats: Vec<f64> = Vec::new();
                    let cref = &client;
                    let mut local_dl = *deadline.as_ref();
                    if ramp_ms > 0 {
                        barrier.wait();
                        local_dl = std::time::SystemTime::now()
                            + Duration::from_secs(duration_secs);
                    }
                    let dl = &local_dl;
                    let sk = start_key.as_slice();
                    if bulk > 0 {
                        // --bulk N → one `get_many_into(N items)`
                        // per round. SDK does client-side
                        // fan-out (ZC `get_into` for ≥ 64 KiB
                        // dest, else `get` with a copy into the
                        // dest). One unified knob; same wire
                        // semantics as the prior --batch-get.
                        let mut ki = 0u64;
                        // One pre-allocated dest buf per item, reused
                        // across rounds. Avoids per-round Vec churn at
                        // the 4 K hot path.
                        let mut bufs: Vec<Vec<u8>> =
                            (0..bulk).map(|_| vec![0u8; value_size]).collect();
                        while std::time::SystemTime::now() < *dl {
                            let keys: Vec<String> = (0..bulk)
                                .map(|_| {
                                    let seq = ki % written;
                                    ki += 1;
                                    key_for_partition(sk, "pc", tid, seq)
                                })
                                .collect();
                            let mut items: Vec<autumn_client::GetManyItem<'_>> = keys
                                .iter()
                                .zip(bufs.iter_mut())
                                .map(|(k, b)| autumn_client::GetManyItem {
                                    key: k.as_bytes(),
                                    offset: 0,
                                    length: 0,
                                    dest: b.as_mut_slice(),
                                })
                                .collect();
                            let t0 = Instant::now();
                            let res = cref.get_many_into(&mut items).await;
                            let el = t0.elapsed();
                            // #6: count only keys that actually returned a value
                            // (Ok(Some)). Ok(None) = key missing — counting it as
                            // a successful read inflates throughput and hides the
                            // write+read sanity check perf-check is meant to be.
                            let n_ok = res.iter().filter(|r| matches!(r, Ok(Some(_)))).count();
                            if n_ok > 0 {
                                total_ops.fetch_add(n_ok as u64, Ordering::Relaxed);
                                let per_op_ms =
                                    el.as_secs_f64() * 1000.0 / n_ok as f64;
                                for _ in 0..n_ok {
                                    lats.push(per_op_ms);
                                }
                            }
                        }
                        return lats;
                    }
                    let mut ki = 0u64;
                    let futs = std::iter::from_fn(move || {
                        if std::time::SystemTime::now() >= *dl {
                            return None;
                        }
                        let seq = ki % written; // cycle through the written keys
                        ki += 1;
                        let key = key_for_partition(sk, "pc", tid, seq);
                        Some(async move {
                            let t0 = Instant::now();
                            // #6: a present value is Ok(Some(_)); Ok(None) (missing
                            // key) must NOT count as a successful read.
                            let ok = if direct_read {
                                matches!(cref.get_direct(key.as_bytes()).await, Ok(Some(_)))
                            } else if zc_read {
                                let mut dest = vec![0u8; value_size];
                                matches!(cref.get_into(key.as_bytes(), &mut dest).await, Ok(Some(_)))
                            } else {
                                matches!(cref.get(key.as_bytes()).await, Ok(Some(_)))
                            };
                            (ok, t0.elapsed())
                        })
                    });
                    let mut s = autumn_client::fan_out(futs, depth);
                    while let Some((_, (ok, el))) = s.next().await {
                        if ok {
                            total_ops.fetch_add(1, Ordering::Relaxed);
                            lats.push(el.as_secs_f64() * 1000.0);
                        }
                    }
                    lats
                })
        });
        read_handles.push(handle);
    }

    let total_ops_r = Arc::clone(&total_ops);
    // #4: stop flag (see write phase) so the read progress printer exits.
    let stop_r = Arc::new(AtomicBool::new(false));
    let stop_r_c = Arc::clone(&stop_r);
    let progress_r = std::thread::spawn(move || {
        let mut last = 0u64;
        while !stop_r_c.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));
            let cur = total_ops_r.load(Ordering::Relaxed);
            eprint!("\r[read] ops/s={}", cur - last);
            last = cur;
        }
    });

    let mut all_read_latencies: Vec<f64> = Vec::new();
    for h in read_handles {
        if let Ok(lats) = h.join() {
            all_read_latencies.extend(lats);
        }
    }
    stop_r.store(true, Ordering::Relaxed);
    let _ = progress_r.join();
    eprintln!();

    let read_elapsed = bench_start.elapsed();
    let read_ops = total_ops.load(Ordering::Relaxed);
    let mut read_hist = LatencyHist {
        samples_ms: all_read_latencies,
    };
    let read_summary = print_bench_summary(
        "Read",
        threads,
        value_size,
        read_elapsed,
        read_ops,
        &mut read_hist,
    );

    // Regpool utilization (post-bench). Counters are process-global
    // — they include the warmup-write phase, this read phase, and
    // any earlier perf-check legs in the same process.
    let pool = autumn_transport::regpool_snapshot();
    println!(
        "regpool: acquire={} hit={} ({:.1}% hit), out_of_pool={}, \
         over_cap={}, register_failed={}, registered_bytes={}",
        pool.acquire_total,
        pool.hit_total,
        pool.hit_rate() * 100.0,
        pool.out_of_pool_total,
        pool.over_cap_total,
        pool.register_failed_total,
        pool.registered_bytes,
    );

    // ---- Regression check ----
    let mut regressed = false;
    let baseline_opt: Option<PerfBaseline> = std::fs::read_to_string(&baseline_file)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    if let Some(ref bl) = baseline_opt {
        let lat_ceil = 2.0 - threshold;

        macro_rules! check_throughput {
            ($label:expr, $cur:expr, $base:expr) => {
                let pct = $cur / $base;
                if pct < threshold {
                    println!(
                        "WARNING: {} ops/sec regressed: {:.0} vs baseline {:.0} ({:.0}%)",
                        $label,
                        $cur,
                        $base,
                        pct * 100.0
                    );
                    regressed = true;
                }
            };
        }
        macro_rules! check_latency {
            ($label:expr, $cur:expr, $base:expr) => {
                if $base > 0.0 {
                    let ratio = $cur / $base;
                    if ratio > lat_ceil {
                        println!(
                            "WARNING: {} p99 latency spiked: {:.2}ms vs baseline {:.2}ms ({:.0}%)",
                            $label, $cur, $base,
                            ratio * 100.0
                        );
                        regressed = true;
                    }
                }
            };
        }

        check_throughput!("write", write_summary.ops_per_sec, bl.write.ops_per_sec);
        check_throughput!("read", read_summary.ops_per_sec, bl.read.ops_per_sec);
        check_latency!("write", write_summary.p99_ms, bl.write.p99_ms);
        check_latency!("read", read_summary.p99_ms, bl.read.p99_ms);

        if !regressed {
            println!(
                "perf-check OK (write={:.0} ops/s read={:.0} ops/s, within {:.0}% of baseline)",
                write_summary.ops_per_sec, read_summary.ops_per_sec,
                threshold * 100.0
            );
        }
    } else {
        println!(
            "no baseline at '{baseline_file}' — run with --update-baseline to create one"
        );
    }

    if update_baseline {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let bl = PerfBaseline {
            version: 1,
            write: write_summary,
            read: read_summary,
            config: BenchConfig {
                threads,
                duration_secs,
                value_size,
                report_interval_secs: 1,
                part_id: None,
                reuse_value: true,
                partition_count: partitions_meta_from_flag,
                // F195: explicit CLI flag (was env read at baseline-write time).
                group_commit_cap,
            },
            recorded_at: now_secs,
        };
        let json = serde_json::to_string_pretty(&bl)?;
        std::fs::write(&baseline_file, json)?;
        println!("baseline saved to {baseline_file}");
    }

    if regressed {
        std::process::exit(2);
    }
    Ok(())
}

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = parse_args();
    if let Some(cap) = args.ucx_regpool_cap_bytes {
        if !autumn_transport::set_regpool_cap_bytes(cap) {
            tracing::warn!(cap, "regpool cap already set (ignored — first-call-wins)");
        }
    }

    // F213: handle the `op` stub BEFORE attempting to connect to the
    // manager — the user is trying to run an op command via the wrong
    // binary; making them wait on a connection attempt would be hostile.
    if let Command::OpStub { args: op_args } = &args.command {
        eprintln!("admin / operator commands moved to `autumn-op`.");
        eprintln!("  autumn-op --help              # list subcommands");
        eprintln!("  autumn-op list-nodes");
        eprintln!("  autumn-op fence-node <id> --reason \"...\"");
        if !op_args.is_empty() {
            eprintln!();
            eprintln!(
                "hint: did you mean `autumn-op --manager {} {}`?",
                args.manager,
                op_args.join(" ")
            );
        }
        std::process::exit(1);
    }

    let _ = autumn_transport::init_with(args.transport);
    // F-KEY-NS D7: the top-level client's binding depends on the command.
    //  - perf-check / ycsb use it ONLY for partition listing (a routing op,
    //    binding-independent) and connect their own bench-scoped clients per
    //    thread → Raw here.
    //  - data-plane KV commands (put/get/del/head/ls/streams) MUST declare their
    //    scope → connect(mgr, scope); absent = migration error.
    let client = match &args.command {
        Command::PerfCheck { .. } | Command::Ycsb { .. } | Command::OpStub { .. } => {
            ClusterClient::connect_raw(&args.manager).await?
        }
        _ => {
            let scope = match &args.namespace {
                Some(s) => s.as_str(),
                None => {
                    eprintln!(
                        "autumn-client: --namespace <SCOPE> is REQUIRED for KV commands \
                         (F-NS-PRINCIPAL-UNIFIED — every write must declare its scope, a whole \
                         namespace like `fs` or a sub-prefix like `mem/agent7`). List namespaces \
                         with `autumn-op namespace-list`."
                    );
                    std::process::exit(2);
                }
            };
            // F-AUTHZ-BUILTIN: with a credential when `--credential-file` is given
            // (principal read from the file). Fails fast if it doesn't cover `{scope}/`.
            match &args.credential_file {
                Some(path) => {
                    let (principal, secret) =
                        autumn_client::read_credential_file(path).context("--credential-file")?;
                    if principal.is_empty() {
                        bail!("--credential-file: missing principal name (expected '<principal>\\n<hex>')");
                    }
                    ClusterClient::connect_with_credential(
                        &args.manager,
                        scope,
                        principal,
                        secret,
                    )
                    .await?
                }
                None => ClusterClient::connect(&args.manager, scope).await?,
            }
        }
    };

    match args.command {
        Command::OpStub { .. } => unreachable!("handled before connect"),
        Command::Put { key, file } => {
            let value = std::fs::read(&file).with_context(|| format!("read file {file}"))?;
            client
                .put(key.as_bytes(), &value)
                .await
                .map_err(|e| anyhow!("put: {e}"))?;
            println!("ok");
        }

        Command::PutZc { key, file } => {
            let value = std::fs::read(&file).with_context(|| format!("read file {file}"))?;
            let value = bytes::Bytes::from(value);
            // UCX rcache auto-registers the value's backing memory on first
            // send (one-time ~100 µs ibv_reg_mr); no SDK-level reg hook needed.
            client
                .put_zc(key.as_bytes(), value)
                .await
                .map_err(|e| anyhow!("put-zc: {e}"))?;
            println!("ok");
        }

        Command::PutStream {
            key,
            file,
            chunk_size,
        } => {
            // #2: reject chunk_size 0 — the chunk loop below would never advance
            // (end == idx) → an infinite stream of empty chunks, and
            // `payload.len().div_ceil(chunk_size)` panics on a 0 divisor.
            if chunk_size == 0 {
                bail!("--chunk-size must be >= 1");
            }
            // F129: read full payload (stdin if file = "-"), drive
            // PutBegin → N×Chunk → Commit. The handle owns the cached
            // RpcClient so all chunks land on the same PS connection.
            use std::io::Read;
            let payload: Vec<u8> = if file == "-" {
                let mut buf = Vec::new();
                std::io::stdin()
                    .read_to_end(&mut buf)
                    .with_context(|| "read stdin")?;
                buf
            } else {
                std::fs::read(&file).with_context(|| format!("read file {file}"))?
            };
            let mut handle = client.put_stream_begin(key.as_bytes(), 0);
            let mut sent = 0u64;
            let mut idx: usize = 0;
            while idx < payload.len() {
                let end = idx.saturating_add(chunk_size).min(payload.len());
                let n = handle
                    .send(&payload[idx..end])
                    .await
                    .map_err(|e| anyhow!("put-stream chunk #{}: {e}", handle.chunks_sent()))?;
                sent = n;
                idx = end;
            }
            handle
                .commit()
                .await
                .map_err(|e| anyhow!("put-stream commit: {e}"))?;
            println!(
                "ok ({sent} bytes, {} chunks)",
                payload.len().div_ceil(chunk_size)
            );
        }

        Command::GetStream {
            key,
            chunk_size,
            out,
        } => {
            use std::io::Write;
            let mut stream = match client
                .get_stream(key.as_bytes(), chunk_size)
                .await
                .map_err(|e| anyhow!("get-stream: {e}"))?
            {
                Some(s) => s,
                None => {
                    eprintln!("not found");
                    std::process::exit(1);
                }
            };
            let total = stream.total_bytes();
            let mut writer: Box<dyn Write> = match out {
                Some(p) => Box::new(
                    std::fs::File::create(&p).with_context(|| format!("create output {p}"))?,
                ),
                None => Box::new(std::io::stdout().lock()),
            };
            while let Some(chunk) = stream
                .next_chunk()
                .await
                .map_err(|e| anyhow!("get-stream chunk: {e}"))?
            {
                writer.write_all(&chunk)?;
            }
            eprintln!("ok ({total} bytes)");
        }

        Command::Get { key } => match client.get(key.as_bytes()).await {
            Ok(Some(value)) => {
                use std::io::Write;
                std::io::stdout().write_all(&value)?;
            }
            Ok(None) => {
                eprintln!("key not found");
                std::process::exit(2);
            }
            Err(e) => bail!("get: {e}"),
        },

        Command::DirectGet { key } => match client.get_direct(key.as_bytes()).await {
            Ok(Some(value)) => {
                use std::io::Write;
                std::io::stdout().write_all(&value)?;
            }
            Ok(None) => {
                eprintln!("key not found");
                std::process::exit(2);
            }
            Err(e) => bail!("direct-get: {e}"),
        },
        Command::ZcGet { key } => {
            // Size the dest from head() (kvcache caller knows the size; the
            // CLI discovers it). Then read the value straight into dest.
            let meta = client
                .head(key.as_bytes())
                .await
                .map_err(|e| anyhow!("zc-get head: {e}"))?;
            if !meta.found {
                eprintln!("key not found");
                std::process::exit(2);
            }
            let mut dest = vec![0u8; meta.value_length as usize];
            // UCX rcache auto-registers `dest` on first recv (one-time
            // ~100 µs ibv_reg_mr); no SDK-level reg hook needed.
            match client.get_into(key.as_bytes(), &mut dest).await {
                Ok(Some(n)) => {
                    use std::io::Write;
                    std::io::stdout().write_all(&dest[..n])?;
                }
                Ok(None) => {
                    eprintln!("key not found");
                    std::process::exit(2);
                }
                Err(e) => bail!("zc-get: {e}"),
            }
        }

        Command::Del { key } => {
            client
                .delete(key.as_bytes())
                .await
                .map_err(|e| anyhow!("delete: {e}"))?;
            println!("ok");
        }

        Command::Head { key } => {
            let meta = client
                .head(key.as_bytes())
                .await
                .map_err(|e| anyhow!("head: {e}"))?;
            if meta.found {
                println!("key: {}, length: {}", key, meta.value_length);
            } else {
                println!("key not found");
            }
        }

        Command::Ls {
            prefix,
            start,
            limit,
        } => {
            let result = client
                .range(prefix.as_bytes(), start.as_bytes(), limit)
                .await
                .map_err(|e| anyhow!("range: {e}"))?;
            for e in &result.entries {
                println!("{}", String::from_utf8_lossy(&e.key));
            }
            if result.has_more {
                eprintln!("(truncated, more results available)");
            }
        }

        Command::PerfCheck {
            threads,
            duration_secs,
            value_size,
            baseline_file,
            threshold,
            update_baseline,
            partitions: partitions_meta_from_flag,
            pipeline_depth,
            group_commit_cap,
            bulk,
            ramp_ms,
            direct_read,
        } => cmd_perf_check(
            &client, threads, duration_secs, value_size, baseline_file, threshold, update_baseline, partitions_meta_from_flag, pipeline_depth, group_commit_cap, bulk, ramp_ms, direct_read, &args.manager,
        )
        .await?,

        Command::Ycsb {
            threads,
            duration_secs,
            value_size,
            partitions,
            pipeline_depth,
            read_ratio,
            zipfian,
            records,
            rmw,
        } => cmd_ycsb(
            &client, threads, duration_secs, value_size, partitions, pipeline_depth, read_ratio, zipfian, records, rmw, &args.manager,
        )
        .await?,
    }

    Ok(())
}
