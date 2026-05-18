#[cfg(unix)]
extern crate libc;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use autumn_client::{ClusterClient, parse_addr};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{PutReq, GetReq, MSG_PUT, MSG_GET};
use serde::{Deserialize, Serialize};

// (F213: hex_split_ranges moved to autumn_op.rs along with `bootstrap`.)

/// F099-N-c — generate a bench key guaranteed to lie in the partition
/// identified by `start_key`. Returns an ASCII string so it remains
/// JSON-safe for wbench's result file.
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

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

enum Command {
    /// F213: stub that points to `autumn-op` (all op subcommands moved).
    /// `args` carries everything after the literal `op` token so we can
    /// suggest the equivalent autumn-op invocation.
    OpStub {
        args: Vec<String>,
    },
    Put {
        key: String,
        file: String,
        nosync: bool,
    },
    /// F129: multipart upload of a large value via PutBegin/Chunk/Commit.
    /// Reads `file` (or stdin if file = "-"), splits into `chunk_size`
    /// byte chunks, and commits.
    PutStream {
        key: String,
        file: String,
        chunk_size: usize,
    },
    /// F129: streaming read. Walks the value via offset/length GetReqs
    /// and writes chunks to stdout (or `out` if provided).
    GetStream {
        key: String,
        chunk_size: u32,
        out: Option<String>,
    },
    Get {
        key: String,
    },
    Del {
        key: String,
        nosync: bool,
    },
    Head {
        key: String,
    },
    Ls {
        prefix: String,
        start: String,
        limit: u32,
    },
    WBench {
        threads: usize,
        duration_secs: u64,
        value_size: usize,
        nosync: bool,
        report_interval_secs: u64,
        part_id: Option<u64>,
        reuse_value: bool,
    },
    RBench {
        threads: usize,
        duration_secs: u64,
        result_file: String,
    },
    PerfCheck {
        threads: usize,
        duration_secs: u64,
        value_size: usize,
        nosync: bool,
        baseline_file: String,
        threshold: f64,
        update_baseline: bool,
        partitions: usize,
        pipeline_depth: usize,
        /// F195: was env `AUTUMN_GROUP_COMMIT_CAP`; recorded in baseline.
        group_commit_cap: Option<usize>,
    },
}

struct Args {
    manager: String,
    command: Command,
    transport: autumn_transport::TransportKind,
}

fn usage() -> ! {
    eprintln!("Usage: autumn-client --manager <ADDR> <COMMAND>");
    eprintln!();
    eprintln!("Data-plane commands (KV + bench):");
    eprintln!("  put <KEY> <FILE>                  Put key with value from file");
    eprintln!("  put-stream [--chunk-size N] <KEY> <FILE-or->>");
    eprintln!("                                    Chunked stream put (default 4 MiB chunks; F186)");
    eprintln!("  get <KEY>                         Get value for key");
    eprintln!("  get-stream [--chunk-size N] [--out FILE] <KEY>");
    eprintln!("                                    Chunked stream get (default 4 MiB chunks)");
    eprintln!("  del <KEY>                         Delete key");
    eprintln!("  head <KEY>                        Get key metadata (size)");
    eprintln!("  ls [--prefix P] [--start S] [--limit N]  List keys");
    eprintln!("  wbench [--threads 4] [--duration 10] [--size 8192] [--report-interval 1] [--part-id ID] [--reuse-value true|false]");
    eprintln!("                                    Write benchmark (always durable; F178 removed --nosync)");
    eprintln!("  rbench [--threads 40] [--duration 10] <RESULT_FILE>");
    eprintln!("                                    Read benchmark");
    eprintln!("  perf-check [--threads 256] [--duration 10] [--size 4096] [--baseline perf_baseline.json] [--threshold 0.8] [--update-baseline] [--partitions N] [--pipeline-depth K]");
    eprintln!("                                    Quick write+read bench; warns if >threshold regression vs baseline");
    eprintln!();
    eprintln!("Operator / admin commands moved to `autumn-op` (F213):");
    eprintln!("  bootstrap, set-stream-ec, force-ec-convert, split, merge,");
    eprintln!("  compact, gc, forcegc, register-node, format, info, policy-candidates");
    eprintln!("  Run `autumn-op --help` for the full list.");
    std::process::exit(1);
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut manager = String::from("127.0.0.1:9001");
    let mut transport = autumn_transport::TransportKind::Tcp;
    let mut i = 1;

    while i < raw.len() {
        match raw[i].as_str() {
            "--manager" => {
                i += 1;
                manager = raw[i].clone();
                i += 1;
            }
            "--transport" => {
                i += 1;
                transport = autumn_transport::parse_transport_flag(&raw[i])
                    .unwrap_or_else(|bad| {
                        eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                        std::process::exit(2);
                    });
                i += 1;
            }
            "--help" | "-h" => usage(),
            _ => break,
        }
    }

    if i >= raw.len() {
        usage();
    }

    let subcmd = raw[i].as_str();
    i += 1;

    let command = match subcmd {
        // F213: op commands moved to autumn-op. Common typos and the
        // explicit `op` namespace prefix all route here.
        "op" | "bootstrap" | "set-stream-ec" | "force-ec-convert" | "split" | "merge"
        | "policy-candidates" | "policy_candidates" | "policy" | "compact" | "gc"
        | "forcegc" | "register-node" | "format" | "info" => {
            // Reconstruct the equivalent autumn-op invocation. For
            // bare `op`, the original args[i..] are the autumn-op
            // command + flags. For other typos (e.g. `autumn-client
            // split 1`), we re-prepend the matched subcommand so the
            // hint is `autumn-op split 1`.
            let mut args: Vec<String> = Vec::new();
            if subcmd != "op" {
                args.push(subcmd.to_string());
            }
            args.extend(raw[i..].iter().cloned());
            Command::OpStub { args }
        }
        "put" => {
            let nosync = false; // F178: always durable; --nosync ignored
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--nosync" {
                    warn_nosync_deprecated_once();
                }
                i += 1;
            }
            if i + 1 >= raw.len() {
                eprintln!("put requires <KEY> <FILE>");
                std::process::exit(1);
            }
            let key = raw[i].clone();
            let file = raw[i + 1].clone();
            Command::Put { key, file, nosync }
        }
        "put-stream" | "putstream" => {
            // put-stream [--chunk-size N] <KEY> <FILE-or-->
            let mut chunk_size: usize = 4 * 1024 * 1024; // 4 MiB default
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--chunk-size" && i + 1 < raw.len() {
                    chunk_size = raw[i + 1].parse().unwrap_or(chunk_size);
                    i += 2;
                    continue;
                }
                i += 1;
            }
            if i + 1 >= raw.len() {
                eprintln!("put-stream requires <KEY> <FILE-or-->");
                std::process::exit(1);
            }
            let key = raw[i].clone();
            let file = raw[i + 1].clone();
            Command::PutStream { key, file, chunk_size }
        }
        "get-stream" | "getstream" => {
            // get-stream [--chunk-size N] [--out FILE] <KEY>
            let mut chunk_size: u32 = 4 * 1024 * 1024;
            let mut out: Option<String> = None;
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--chunk-size" && i + 1 < raw.len() {
                    chunk_size = raw[i + 1].parse().unwrap_or(chunk_size);
                    i += 2;
                    continue;
                }
                if raw[i] == "--out" && i + 1 < raw.len() {
                    out = Some(raw[i + 1].clone());
                    i += 2;
                    continue;
                }
                i += 1;
            }
            if i >= raw.len() {
                eprintln!("get-stream requires <KEY>");
                std::process::exit(1);
            }
            Command::GetStream { key: raw[i].clone(), chunk_size, out }
        }
        "get" => {
            if i >= raw.len() {
                eprintln!("get requires <KEY>");
                std::process::exit(1);
            }
            Command::Get {
                key: raw[i].clone(),
            }
        }
        "del" => {
            let nosync = false; // F178: always durable
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--nosync" {
                    warn_nosync_deprecated_once();
                }
                i += 1;
            }
            if i >= raw.len() {
                eprintln!("del requires <KEY>");
                std::process::exit(1);
            }
            Command::Del {
                key: raw[i].clone(),
                nosync,
            }
        }
        "head" => {
            if i >= raw.len() {
                eprintln!("head requires <KEY>");
                std::process::exit(1);
            }
            Command::Head {
                key: raw[i].clone(),
            }
        }
        "ls" => {
            let mut prefix = String::new();
            let mut start = String::new();
            let mut limit: u32 = 100;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--prefix" => {
                        i += 1;
                        prefix = raw[i].clone();
                    }
                    "--start" => {
                        i += 1;
                        start = raw[i].clone();
                    }
                    "--limit" => {
                        i += 1;
                        limit = raw[i].parse().expect("--limit must be a number");
                    }
                    _ => {
                        if prefix.is_empty() {
                            prefix = raw[i].clone();
                        }
                    }
                }
                i += 1;
            }
            Command::Ls {
                prefix,
                start,
                limit,
            }
        }
        "wbench" => {
            let mut threads: usize = 4;
            let mut duration_secs: u64 = 10;
            let mut value_size: usize = 8192;
            let nosync = false; // F178: always durable
            let mut report_interval_secs: u64 = 1;
            let mut part_id: Option<u64> = None;
            let mut reuse_value = true;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--threads" | "-t" => {
                        i += 1;
                        threads = raw[i].parse().expect("--threads must be a number");
                    }
                    "--duration" | "-d" => {
                        i += 1;
                        duration_secs = raw[i].parse().expect("--duration must be a number");
                    }
                    "--size" | "-s" => {
                        i += 1;
                        value_size = raw[i].parse().expect("--size must be a number");
                    }
                    "--report-interval" => {
                        i += 1;
                        report_interval_secs = raw[i]
                            .parse::<u64>()
                            .expect("--report-interval must be a number")
                            .max(1);
                    }
                    "--part-id" => {
                        i += 1;
                        part_id = Some(raw[i].parse().expect("--part-id must be a number"));
                    }
                    "--reuse-value" => {
                        i += 1;
                        reuse_value = parse_bool_flag(&raw[i], "--reuse-value")
                            .expect("--reuse-value must be true or false");
                    }
                    "--nosync" => {
                        warn_nosync_deprecated_once();
                    }
                    _ => {}
                }
                i += 1;
            }
            Command::WBench {
                threads,
                duration_secs,
                value_size,
                nosync,
                report_interval_secs,
                part_id,
                reuse_value,
            }
        }
        "rbench" => {
            let mut threads: usize = 40;
            let mut duration_secs: u64 = 10;
            let mut result_file = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--threads" | "-t" => {
                        i += 1;
                        threads = raw[i].parse().expect("--threads must be a number");
                    }
                    "--duration" | "-d" => {
                        i += 1;
                        duration_secs = raw[i].parse().expect("--duration must be a number");
                    }
                    _ => result_file = raw[i].clone(),
                }
                i += 1;
            }
            if result_file.is_empty() {
                eprintln!("rbench requires <RESULT_FILE>");
                std::process::exit(1);
            }
            Command::RBench {
                threads,
                duration_secs,
                result_file,
            }
        }
        "perf-check" => {
            let mut threads = 256usize;
            let mut duration_secs = 10u64;
            let mut value_size = 4096usize;
            let nosync = false; // F178: always durable
            let mut baseline_file = "perf_baseline.json".to_string();
            let mut threshold = 0.8f64;
            let mut update_baseline = false;
            let mut partitions_meta_from_flag: usize = 1;
            let mut pipeline_depth: usize = 1;
            // F195: was `AUTUMN_GROUP_COMMIT_CAP` env read at baseline-
            // write time. Now an explicit CLI flag — operators pass the
            // same value to autumn-ps (`--group-commit-cap N`) AND to
            // perf-check (`--group-commit-cap N`) so the baseline JSON
            // reflects the server config that was active.
            let mut group_commit_cap: Option<usize> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--threads" | "-t" => {
                        i += 1;
                        threads = raw[i].parse().expect("--threads must be a number");
                    }
                    "--duration" | "-d" => {
                        i += 1;
                        duration_secs = raw[i].parse().expect("--duration must be a number");
                    }
                    "--size" => {
                        i += 1;
                        value_size = raw[i].parse().expect("--size must be a number");
                    }
                    "--nosync" => {
                        warn_nosync_deprecated_once();
                    }
                    "--baseline" => {
                        i += 1;
                        baseline_file = raw[i].clone();
                    }
                    "--threshold" => {
                        i += 1;
                        threshold = raw[i].parse().expect("--threshold must be a float");
                    }
                    "--update-baseline" => {
                        update_baseline = true;
                    }
                    "--partitions" => {
                        i += 1;
                        partitions_meta_from_flag = raw[i].parse().expect("--partitions must be a positive integer");
                        if partitions_meta_from_flag == 0 {
                            eprintln!("--partitions must be >= 1");
                            usage();
                        }
                    }
                    "--pipeline-depth" => {
                        i += 1;
                        pipeline_depth = raw[i]
                            .parse()
                            .expect("--pipeline-depth must be a positive integer");
                        if pipeline_depth == 0 || pipeline_depth > 256 {
                            eprintln!("--pipeline-depth must be in [1, 256]");
                            usage();
                        }
                    }
                    "--group-commit-cap" => {
                        i += 1;
                        group_commit_cap = Some(
                            raw[i].parse().expect("--group-commit-cap must be a u64"),
                        );
                    }
                    other => {
                        eprintln!("unknown perf-check flag: {other}");
                        usage();
                    }
                }
                i += 1;
            }
            Command::PerfCheck {
                threads,
                duration_secs,
                value_size,
                nosync,
                baseline_file,
                threshold,
                update_baseline,
                partitions: partitions_meta_from_flag,
                pipeline_depth,
                group_commit_cap,
            }
        }
        other => {
            eprintln!("unknown command: {other}");
            usage();
        }
    };

    Args { manager, command, transport }
}

/// F178 Phase 3: `--nosync` was removed because writes are now ALWAYS
/// durable via the per-extent fsync coalescer (Phase 1) + flush-time
/// quorum durability wait (Phase 2). The flag is kept as a parser
/// no-op so existing scripts (perf_check.sh, ad-hoc invocations) don't
/// hard-fail; callers always get the durable path. Logged once per
/// invocation so deprecation is visible.
fn warn_nosync_deprecated_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        eprintln!(
            "[autumn-client] note: --nosync was removed in F178 (LevelDB-style \
             coalescing makes writes always durable). Flag ignored, behaviour \
             unchanged from --sync."
        );
    });
}

fn parse_bool_flag(value: &str, flag: &str) -> Result<bool> {
    match value {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => bail!("{flag} must be true or false"),
    }
}

fn parse_positive_usize_flag(value: &str, flag: &str) -> Result<usize> {
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{flag} must be a positive number"))?;
    if parsed == 0 {
        bail!("{flag} must be a positive number");
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct BenchResult {
    key: String,
    start_time: f64,
    elapsed: f64,
}

#[derive(Serialize, Deserialize)]
struct BenchConfig {
    threads: usize,
    duration_secs: u64,
    value_size: usize,
    nosync: bool,
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
struct BenchSample {
    second: u64,
    ops: u64,
    cumulative_ops: u64,
}

#[derive(Serialize, Deserialize)]
struct WriteBenchReport {
    version: u32,
    config: BenchConfig,
    summary: BenchSummaryRecord,
    ops_samples: Vec<BenchSample>,
    results: Vec<BenchResult>,
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
    fn new() -> Self {
        Self {
            samples_ms: Vec::new(),
        }
    }

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

fn parse_write_results(json: &str) -> Result<(Vec<BenchResult>, usize)> {
    let trimmed = json.trim_start();
    if trimmed.starts_with('[') {
        let results: Vec<BenchResult> =
            serde_json::from_str(trimmed).context("parse legacy result file")?;
        return Ok((results, 0));
    }
    let report: WriteBenchReport = serde_json::from_str(trimmed).context("parse result report")?;
    if report.results.is_empty() {
        bail!("no keys in result file");
    }
    let value_size = report.config.value_size;
    Ok((report.results, value_size))
}

// (F213: derive_control_address + format_disk moved to autumn_op.rs
// along with the `format` subcommand.)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_flag_accepts_expected_values() {
        assert!(parse_bool_flag("true", "--reuse-value").unwrap());
        assert!(!parse_bool_flag("false", "--reuse-value").unwrap());
        assert!(parse_bool_flag("1", "--reuse-value").unwrap());
        assert!(parse_bool_flag("maybe", "--reuse-value").is_err());
    }

    #[test]
    fn parse_positive_usize_flag_rejects_zero() {
        assert_eq!(
            parse_positive_usize_flag("8", "--channels-per-ps").unwrap(),
            8
        );
        assert!(parse_positive_usize_flag("0", "--channels-per-ps").is_err());
        assert!(parse_positive_usize_flag("abc", "--channels-per-ps").is_err());
    }

    #[test]
    fn parse_write_results_supports_legacy_format() {
        let json = serde_json::to_string(&vec![BenchResult {
            key: "k1".to_string(),
            start_time: 0.0,
            elapsed: 1.0,
        }])
        .unwrap();
        let (parsed, vs) = parse_write_results(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k1");
        assert_eq!(vs, 0);
    }

    #[test]
    fn parse_write_results_supports_report_wrapper() {
        let json = serde_json::to_string(&WriteBenchReport {
            version: 1,
            config: BenchConfig {
                threads: 4,
                duration_secs: 10,
                value_size: 8192,
                nosync: true,
                report_interval_secs: 1,
                part_id: Some(7),
                reuse_value: true,
                partition_count: 1,
                group_commit_cap: None,
            },
            summary: BenchSummaryRecord {
                total_ops: 1,
                total_bytes: 8192,
                ops_per_sec: 1.0,
                throughput_mb_per_sec: 1.0,
                p50_ms: 1.0,
                p95_ms: 1.0,
                p99_ms: 1.0,
            },
            ops_samples: vec![BenchSample {
                second: 1,
                ops: 1,
                cumulative_ops: 1,
            }],
            results: vec![BenchResult {
                key: "k2".to_string(),
                start_time: 0.1,
                elapsed: 0.2,
            }],
        })
        .unwrap();
        let (parsed, vs) = parse_write_results(&json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k2");
        assert_eq!(vs, 8192);
    }

    #[test]
    fn parse_write_results_supports_report_wrapper_without_channels_per_ps() {
        let json = r#"{
            "version": 1,
            "config": {
                "threads": 4,
                "duration_secs": 10,
                "value_size": 8192,
                "nosync": true,
                "report_interval_secs": 1,
                "part_id": 7,
                "reuse_value": true
            },
            "summary": {
                "total_ops": 1,
                "total_bytes": 8192,
                "ops_per_sec": 1.0,
                "throughput_mb_per_sec": 1.0,
                "p50_ms": 1.0,
                "p95_ms": 1.0,
                "p99_ms": 1.0
            },
            "ops_samples": [
                { "second": 1, "ops": 1, "cumulative_ops": 1 }
            ],
            "results": [
                { "key": "k3", "start_time": 0.1, "elapsed": 0.2 }
            ]
        }"#;
        let (parsed, vs) = parse_write_results(json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "k3");
        assert_eq!(vs, 8192);
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = parse_args();

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
    let client = ClusterClient::connect(&args.manager).await?;

    match args.command {
        Command::OpStub { .. } => unreachable!("handled before connect"),
        Command::Put { key, file, nosync: _ } => {
            let value = std::fs::read(&file).with_context(|| format!("read file {file}"))?;
            client.put(key.as_bytes(), &value).await
                .map_err(|e| anyhow!("put: {e}"))?;
            println!("ok");
        }

        Command::PutStream { key, file, chunk_size } => {
            // F129: read full payload (stdin if file = "-"), drive
            // PutBegin → N×Chunk → Commit. The handle owns the cached
            // RpcClient so all chunks land on the same PS connection.
            use std::io::Read;
            let payload: Vec<u8> = if file == "-" {
                let mut buf = Vec::new();
                std::io::stdin().read_to_end(&mut buf)
                    .with_context(|| "read stdin")?;
                buf
            } else {
                std::fs::read(&file).with_context(|| format!("read file {file}"))?
            };
            let mut handle = client.put_stream_begin(key.as_bytes(), 0);
            let mut sent = 0u64;
            let mut idx: usize = 0;
            while idx < payload.len() {
                let end = (idx + chunk_size).min(payload.len());
                let n = handle
                    .send(&payload[idx..end])
                    .await
                    .map_err(|e| anyhow!("put-stream chunk #{}: {e}", handle.chunks_sent()))?;
                sent = n;
                idx = end;
            }
            handle.commit().await.map_err(|e| anyhow!("put-stream commit: {e}"))?;
            println!("ok ({sent} bytes, {} chunks)", payload.len().div_ceil(chunk_size));
        }

        Command::GetStream { key, chunk_size, out } => {
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
                Some(p) => Box::new(std::fs::File::create(&p)
                    .with_context(|| format!("create output {p}"))?),
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

        Command::Get { key } => {
            match client.get(key.as_bytes()).await {
                Ok(Some(value)) => {
                    use std::io::Write;
                    std::io::stdout().write_all(&value)?;
                }
                Ok(None) => {
                    eprintln!("key not found");
                    std::process::exit(2);
                }
                Err(e) => bail!("get: {e}"),
            }
        }

        Command::Del { key, nosync: _nosync } => {
            client.delete(key.as_bytes()).await
                .map_err(|e| anyhow!("delete: {e}"))?;
            println!("ok");
        }

        Command::Head { key } => {
            let meta = client.head(key.as_bytes()).await
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
            let result = client.range(prefix.as_bytes(), start.as_bytes(), limit).await
                .map_err(|e| anyhow!("range: {e}"))?;
            for e in &result.entries {
                println!("{}", String::from_utf8_lossy(&e.key));
            }
            if result.has_more {
                eprintln!("(truncated, more results available)");
            }
        }

        Command::WBench {
            threads,
            duration_secs,
            value_size,
            nosync,
            report_interval_secs,
            part_id,
            reuse_value,
        } => {
            #[cfg(unix)]
            {
                let needed = (threads * 4 + 512) as u64;
                unsafe {
                    let mut rl = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 {
                        if rl.rlim_cur < needed {
                            let target = needed.min(rl.rlim_max);
                            rl.rlim_cur = target;
                            if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 || target < needed {
                                eprintln!(
                                    "warning: need {} open files for {} threads, \
                                     but limit is {} (hard limit {}). \
                                     Run: ulimit -n 65536",
                                    needed, threads, target, rl.rlim_max
                                );
                            }
                        }
                    }
                }
            }

            // F099-N-c: partition ranges for range-aware key generation.
            let partitions: Vec<(u64, String, Vec<u8>, Vec<u8>)> =
                if let Some(part_id) = part_id {
                    // Single-partition mode — fetch its range from the full list.
                    let all = client.all_partitions_with_range().await?;
                    let entry = all
                        .into_iter()
                        .find(|(pid, _, _, _)| *pid == part_id)
                        .ok_or_else(|| anyhow!("partition {} not found", part_id))?;
                    vec![entry]
                } else {
                    client.all_partitions_with_range().await?
                };
            if partitions.is_empty() {
                bail!("no partitions found, run bootstrap first");
            }

            // Resolve PS addresses for each thread
            let mut thread_targets: Vec<(u64, SocketAddr, Vec<u8>)> =
                Vec::with_capacity(threads);
            for tid in 0..threads {
                let (part_id, ps_addr, start_key, _end_key) =
                    &partitions[tid % partitions.len()];
                thread_targets.push((*part_id, parse_addr(ps_addr)?, start_key.clone()));
            }

            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let total_errors = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();
            let ops_samples = Arc::new(Mutex::new(Vec::<BenchSample>::new()));

            let mut handles = Vec::new();
            for (tid, (part_id, ps_addr, start_key)) in thread_targets.into_iter().enumerate() {
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let total_errors = Arc::clone(&total_errors);
                let value_template = (0..value_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();

                let handle = std::thread::spawn(move || {
                    compio::runtime::RuntimeBuilder::new()
                        .build()
                        .unwrap()
                        .block_on(async {
                        let ps = match RpcClient::connect(ps_addr).await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("thread {tid} connect error: {e}");
                                return (Vec::new(), Vec::new());
                            }
                        };
                        let mut seq: u64 = 0;
                        let mut local_latencies: Vec<f64> = Vec::new();
                        let mut local_results: Vec<BenchResult> = Vec::new();

                        loop {
                            if std::time::SystemTime::now() >= *deadline {
                                break;
                            }
                            // F099-N-c: range-aware key (falls in this partition).
                            let key = key_for_partition(&start_key, "bench", tid, seq);
                            seq += 1;

                            let t0 = Instant::now();
                            let op_start = bench_start.elapsed().as_secs_f64();
                            let value = if reuse_value {
                                value_template.clone()
                            } else {
                                value_template.clone()
                            };
                            let res = ps
                                .call(
                                    MSG_PUT,
                                    rkyv_encode(&PutReq {
                                        part_id,
                                        key: key.as_bytes().to_vec(),
                                        value,
                                        expires_at: 0,
                                        // Bench captures topology up front and assumes
                                        // a static cluster; stamp 0 to bypass the
                                        // post-Phase-3 epoch check.
                                        region_epoch: 0,
                                    }),
                                )
                                .await;
                            let elapsed = t0.elapsed();

                            match res {
                                Ok(_) => {
                                    total_ops.fetch_add(1, Ordering::Relaxed);
                                    local_latencies.push(elapsed.as_secs_f64() * 1000.0);
                                    local_results.push(BenchResult {
                                        key,
                                        start_time: op_start,
                                        elapsed: elapsed.as_secs_f64(),
                                    });
                                }
                                Err(e) => {
                                    total_errors.fetch_add(1, Ordering::Relaxed);
                                    if seq == 1 {
                                        eprintln!("thread {tid} put error: {e}");
                                    }
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                            }
                        }
                        (local_latencies, local_results)
                    })
                });
                handles.push(handle);
            }

            // Progress reporter
            let total_ops_clone = Arc::clone(&total_ops);
            let ops_samples_clone = Arc::clone(&ops_samples);
            let progress = std::thread::spawn(move || {
                let mut last = 0u64;
                let mut second = 0u64;
                loop {
                    std::thread::sleep(Duration::from_secs(report_interval_secs));
                    let cur = total_ops_clone.load(Ordering::Relaxed);
                    second += report_interval_secs;
                    let delta = cur - last;
                    eprint!("\rops/s={delta}");
                    ops_samples_clone.lock().unwrap().push(BenchSample {
                        second,
                        ops: delta,
                        cumulative_ops: cur,
                    });
                    last = cur;
                }
            });

            let mut all_latencies: Vec<f64> = Vec::new();
            let mut all_results: Vec<BenchResult> = Vec::new();
            for h in handles {
                if let Ok((lats, res)) = h.join() {
                    all_latencies.extend(lats);
                    all_results.extend(res);
                }
            }
            drop(progress);
            eprintln!();

            let elapsed = bench_start.elapsed();
            let ops = total_ops.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            if errs > 0 {
                eprintln!("errors: {errs}");
            }

            let mut hist = LatencyHist::new();
            hist.samples_ms = all_latencies;
            let summary =
                print_bench_summary("Write", threads, value_size, elapsed, ops, &mut hist);

            let report = WriteBenchReport {
                version: 1,
                config: BenchConfig {
                    threads,
                    duration_secs,
                    value_size,
                    nosync,
                    report_interval_secs,
                    part_id,
                    reuse_value,
                    partition_count: 1,
                    group_commit_cap: None,
                },
                summary,
                ops_samples: ops_samples.lock().unwrap().drain(..).collect(),
                results: all_results,
            };

            let json = serde_json::to_string_pretty(&report)?;
            std::fs::write("write_result.json", json)?;
            println!("results written to write_result.json");
        }

        Command::RBench {
            threads,
            duration_secs,
            result_file,
        } => {
            let json = std::fs::read_to_string(&result_file)
                .with_context(|| format!("read {result_file}"))?;
            let (write_results, value_size) = parse_write_results(&json)?;
            let keys: Vec<String> = write_results.into_iter().map(|r| r.key).collect();
            if keys.is_empty() {
                bail!("no keys in result file");
            }

            let keys = Arc::new(keys);
            let manager_addr = Arc::new(args.manager.clone());
            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let total_errors = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();

            let mut handles = Vec::new();
            let keys_per_thread = (keys.len() + threads - 1) / threads;

            for tid in 0..threads {
                let keys = Arc::clone(&keys);
                let manager_addr = Arc::clone(&manager_addr);
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let total_errors = Arc::clone(&total_errors);

                let handle = std::thread::spawn(move || {
                    compio::runtime::RuntimeBuilder::new()
                        .build()
                        .unwrap()
                        .block_on(async {
                        let cc = match ClusterClient::connect(&manager_addr).await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("thread {tid} connect error: {e}");
                                return Vec::new();
                            }
                        };
                        let start_idx = tid * keys_per_thread;
                        let end_idx = (start_idx + keys_per_thread).min(keys.len());
                        if start_idx >= end_idx {
                            return Vec::new();
                        }
                        let my_keys = &keys[start_idx..end_idx];
                        let mut ki = 0usize;
                        let mut local_latencies: Vec<f64> = Vec::new();
                        let mut logged_errors = 0u32;

                        loop {
                            if std::time::SystemTime::now() >= *deadline {
                                break;
                            }
                            let key = &my_keys[ki % my_keys.len()];
                            ki += 1;

                            let (part_id, ps_addr) = match cc.resolve_key(key.as_bytes()).await {
                                Ok(r) => r,
                                Err(e) => {
                                    total_errors.fetch_add(1, Ordering::Relaxed);
                                    if logged_errors < 3 {
                                        eprintln!("thread {tid} resolve_key error: {e}");
                                        logged_errors += 1;
                                    }
                                    continue;
                                }
                            };
                            let ps = match cc.get_ps_client(&ps_addr).await {
                                Ok(ps) => ps,
                                Err(e) => {
                                    total_errors.fetch_add(1, Ordering::Relaxed);
                                    if logged_errors < 3 {
                                        eprintln!("thread {tid} get_ps_client error: {e}");
                                        logged_errors += 1;
                                    }
                                    continue;
                                }
                            };
                            let t0 = Instant::now();
                            let res = ps
                                .call(
                                    MSG_GET,
                                    rkyv_encode(&GetReq {
                                        part_id,
                                        key: key.as_bytes().to_vec(),
                                        offset: 0,
                                        length: 0,
                                        // Bench: static topology, skip epoch check.
                                        region_epoch: 0,
                                    }),
                                )
                                .await;
                            let elapsed = t0.elapsed();

                            match res {
                                Ok(_) => {
                                    total_ops.fetch_add(1, Ordering::Relaxed);
                                    local_latencies.push(elapsed.as_secs_f64() * 1000.0);
                                }
                                Err(e) => {
                                    total_errors.fetch_add(1, Ordering::Relaxed);
                                    if logged_errors < 3 {
                                        eprintln!("thread {tid} get error: {e}");
                                        logged_errors += 1;
                                    }
                                }
                            }
                        }
                        local_latencies
                    })
                });
                handles.push(handle);
            }

            let total_ops_clone = Arc::clone(&total_ops);
            let progress = std::thread::spawn(move || {
                let mut last = 0u64;
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    let cur = total_ops_clone.load(Ordering::Relaxed);
                    eprint!("\rops/s={}", cur - last);
                    last = cur;
                }
            });

            let mut all_latencies: Vec<f64> = Vec::new();
            for h in handles {
                if let Ok(lats) = h.join() {
                    all_latencies.extend(lats);
                }
            }
            drop(progress);
            eprintln!();

            let elapsed = bench_start.elapsed();
            let ops = total_ops.load(Ordering::Relaxed);
            let errs = total_errors.load(Ordering::Relaxed);
            if errs > 0 {
                eprintln!("errors: {errs}");
            }

            let mut hist = LatencyHist::new();
            hist.samples_ms = all_latencies;
            let _ = print_bench_summary("Read", threads, value_size, elapsed, ops, &mut hist);
        }

        Command::PerfCheck {
            threads,
            duration_secs,
            value_size,
            nosync,
            baseline_file,
            threshold,
            update_baseline,
            partitions: partitions_meta_from_flag,
            pipeline_depth,
            group_commit_cap,
        } => {
            let pipeline_depth = pipeline_depth.max(1);
            // ---- Write phase ----
            if pipeline_depth > 1 {
                println!(
                    "==> perf-check: write ({threads} threads, {duration_secs}s, {value_size}B, depth={pipeline_depth})"
                );
            } else {
                println!(
                    "==> perf-check: write ({threads} threads, {duration_secs}s, {value_size}B)"
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
                    partitions_meta_from_flag, partitions.len()
                );
            }

            let mut thread_targets: Vec<(u64, SocketAddr, Vec<u8>)> =
                Vec::with_capacity(threads);
            for tid in 0..threads {
                let (part_id, ps_addr, start_key, _end_key) =
                    &partitions[tid % partitions.len()];
                thread_targets.push((*part_id, parse_addr(ps_addr)?, start_key.clone()));
            }

            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();

            let mut write_handles = Vec::new();
            for (tid, (part_id, ps_addr, start_key)) in thread_targets.into_iter().enumerate() {
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let value_bytes =
                    (0..value_size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();

                let max_depth = pipeline_depth;
                let handle = std::thread::spawn(move || {
                    compio::runtime::RuntimeBuilder::new()
                        .build()
                        .unwrap()
                        .block_on(async {
                        use futures::stream::{FuturesUnordered, StreamExt};
                        let ps = match RpcClient::connect(ps_addr).await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("thread {tid} connect error: {e}");
                                return (Vec::new(), Vec::new());
                            }
                        };
                        let mut seq: u64 = 0;
                        let mut local_latencies: Vec<f64> = Vec::new();
                        let mut local_keyinfo: Vec<(String, u64, SocketAddr)> = Vec::new();
                        let mut inflight = FuturesUnordered::new();
                        loop {
                            // Refill pipeline up to max_depth while deadline not expired.
                            while inflight.len() < max_depth
                                && std::time::SystemTime::now() < *deadline
                            {
                                // F099-N-c: key must fall in this thread's
                                // partition range or PS will reject it with
                                // "key out of range".
                                let key = key_for_partition(&start_key, "pc", tid, seq);
                                seq += 1;
                                let req_bytes = rkyv_encode(&PutReq {
                                    part_id,
                                    key: key.as_bytes().to_vec(),
                                    value: value_bytes.clone(),
                                    expires_at: 0,
                                    // Bench: static topology, skip epoch check.
                                    region_epoch: 0,
                                });
                                let ps_clone = ps.clone();
                                let t0 = Instant::now();
                                inflight.push(async move {
                                    let res = ps_clone.call(MSG_PUT, req_bytes).await;
                                    (res, key, t0.elapsed())
                                });
                            }
                            // Drain one completion. When deadline passes AND inflight is
                            // empty, next() returns None and we exit.
                            match inflight.next().await {
                                Some((res, key, elapsed)) => {
                                    if res.is_ok() {
                                        total_ops.fetch_add(1, Ordering::Relaxed);
                                        local_latencies.push(elapsed.as_secs_f64() * 1000.0);
                                        local_keyinfo.push((key, part_id, ps_addr));
                                    }
                                }
                                None => break,
                            }
                        }
                        (local_latencies, local_keyinfo)
                    })
                });
                write_handles.push(handle);
            }

            let total_ops_w = Arc::clone(&total_ops);
            let progress_w = std::thread::spawn(move || {
                let mut last = 0u64;
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    let cur = total_ops_w.load(Ordering::Relaxed);
                    eprint!("\r[write] ops/s={}", cur - last);
                    last = cur;
                }
            });

            let mut all_write_latencies: Vec<f64> = Vec::new();
            let mut all_write_keyinfo: Vec<(String, u64, SocketAddr)> = Vec::new();
            for h in write_handles {
                if let Ok((lats, keyinfo)) = h.join() {
                    all_write_latencies.extend(lats);
                    all_write_keyinfo.extend(keyinfo);
                }
            }
            drop(progress_w);
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

            if all_write_keyinfo.is_empty() {
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

            let pc_keyinfo = Arc::new(all_write_keyinfo);
            let manager_addr = Arc::new(args.manager.clone());
            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();
            let keys_per_thread = (pc_keyinfo.len() + threads - 1) / threads;

            let mut read_handles = Vec::new();
            for tid in 0..threads {
                let pc_keyinfo = Arc::clone(&pc_keyinfo);
                let _manager_addr = Arc::clone(&manager_addr); // kept for parity, unused now
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);

                let max_depth = pipeline_depth;
                let handle = std::thread::spawn(move || {
                    compio::runtime::RuntimeBuilder::new()
                        .build()
                        .unwrap()
                        .block_on(async {
                        use futures::stream::{FuturesUnordered, StreamExt};
                        // Per-thread RpcClient connection cache keyed by ps_addr.
                        let mut conns: std::collections::HashMap<SocketAddr, Rc<RpcClient>> =
                            std::collections::HashMap::new();
                        let start_idx = tid * keys_per_thread;
                        let end_idx = (start_idx + keys_per_thread).min(pc_keyinfo.len());
                        if start_idx >= end_idx {
                            return Vec::new();
                        }
                        let my_slice = &pc_keyinfo[start_idx..end_idx];
                        let mut ki = 0usize;
                        let mut local_latencies: Vec<f64> = Vec::new();
                        let mut inflight = FuturesUnordered::new();

                        loop {
                            // Refill pipeline up to max_depth while deadline not expired.
                            while inflight.len() < max_depth
                                && std::time::SystemTime::now() < *deadline
                            {
                                let (key, part_id, ps_addr) = &my_slice[ki % my_slice.len()];
                                ki += 1;
                                let ps = match conns.get(ps_addr) {
                                    Some(c) => c.clone(),
                                    None => match RpcClient::connect(*ps_addr).await {
                                        Ok(c) => {
                                            conns.insert(*ps_addr, c.clone());
                                            c
                                        }
                                        Err(_) => continue,
                                    },
                                };
                                let req_bytes = rkyv_encode(&GetReq {
                                    part_id: *part_id,
                                    key: key.as_bytes().to_vec(),
                                    offset: 0,
                                    length: 0,
                                    // Bench: static topology, skip epoch check.
                                    region_epoch: 0,
                                });
                                let t0 = Instant::now();
                                inflight.push(async move {
                                    let res = ps.call(MSG_GET, req_bytes).await;
                                    (res, t0.elapsed())
                                });
                            }
                            // Drain one completion. When deadline passes AND inflight is
                            // empty, next() returns None and we exit.
                            match inflight.next().await {
                                Some((res, elapsed)) => {
                                    if res.is_ok() {
                                        total_ops.fetch_add(1, Ordering::Relaxed);
                                        local_latencies.push(elapsed.as_secs_f64() * 1000.0);
                                    }
                                }
                                None => break,
                            }
                        }
                        local_latencies
                    })
                });
                read_handles.push(handle);
            }

            let total_ops_r = Arc::clone(&total_ops);
            let progress_r = std::thread::spawn(move || {
                let mut last = 0u64;
                loop {
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
            drop(progress_r);
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
                                $label, $cur, $base,
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
                        nosync,
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
        }

    }

    Ok(())
}
