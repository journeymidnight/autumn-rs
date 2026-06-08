#[cfg(unix)]
extern crate libc;

use std::sync::atomic::{AtomicU64, Ordering};
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
    /// F216-E verification: zero-copy PUT via ClusterClient::put_zc. Reads
    /// `file` into a Bytes, registers it for UCX zero-copy send (ucx build),
    /// writes via MSG_PUT_ZC (value sent as its own iovec, no client-side copy).
    PutZc {
        key: String,
        file: String,
    },
    Get {
        key: String,
    },
    /// F216 verification: zero-copy GET via ClusterClient::get_into. heads
    /// the key, allocates a dest buffer, registers it for UCX zero-copy
    /// (ucx build only), reads the value straight into dest, writes to stdout.
    ZcGet {
        key: String,
    },
    Del {
        key: String,
    },
    Head {
        key: String,
    },
    Ls {
        prefix: String,
        start: String,
        limit: u32,
    },
    PerfCheck {
        threads: usize,
        duration_secs: u64,
        value_size: usize,
        baseline_file: String,
        threshold: f64,
        update_baseline: bool,
        partitions: usize,
        pipeline_depth: usize,
        /// F195: was env `AUTUMN_GROUP_COMMIT_CAP`; recorded in baseline.
        group_commit_cap: Option<usize>,
        /// Experimental: when > 0, write loop uses `put_many` with this
        /// batch size instead of `kv_put` per iteration. Tests whether
        /// writer_task tcp_sendmsg coalescing + read_loop bulk decode
        /// gives partition_loop the "fat arrival" needed to grow batch_size.
        batch_put: usize,
        /// Read-side counterpart: when > 0, read loop issues
        /// `batch_get(N)` (one MSG_BATCH_GET per partition) instead of
        /// per-key `get` / `get_into`. Validates the BATCH_GET wire +
        /// server inline-loop path end-to-end and surfaces the read
        /// equivalent of batch_put's 6.9× write win when small values
        /// are dominated by per-op routing + ps-conn dispatch overhead.
        batch_get: usize,
    },
}

struct Args {
    manager: String,
    command: Command,
    transport: autumn_transport::TransportKind,
    /// Per-thread regpool cap (pinned/registered bytes). `None` = library
    /// default (512 MiB/thread). Useful for perf-check tuning — large
    /// `--threads --pipeline-depth` 8 MiB workloads can pin many slabs
    /// in-flight and benefit from a higher cap; constrained hosts can
    /// shrink to fit. Clamped to [16 MiB, 64 GiB].
    ucx_regpool_cap_bytes: Option<usize>,
}

fn usage() -> ! {
    eprintln!("Usage: autumn-client --manager <ADDR> <COMMAND>");
    eprintln!();
    eprintln!("Data-plane commands (KV + bench):");
    eprintln!("  put <KEY> <FILE>                  Put key with value from file");
    eprintln!("  put-stream [--chunk-size N] <KEY> <FILE-or->>");
    eprintln!(
        "                                    Chunked stream put (default 4 MiB chunks; F186)"
    );
    eprintln!("  get <KEY>                         Get value for key");
    eprintln!("  get-stream [--chunk-size N] [--out FILE] <KEY>");
    eprintln!("                                    Chunked stream get (default 4 MiB chunks)");
    eprintln!("  del <KEY>                         Delete key");
    eprintln!("  head <KEY>                        Get key metadata (size)");
    eprintln!("  ls [--prefix P] [--start S] [--limit N]  List keys");
    eprintln!("  perf-check [--threads 256] [--duration 10] [--size 4096] [--baseline perf_baseline.json] [--threshold 0.8] [--update-baseline] [--partitions N] [--pipeline-depth K]   (zero-copy auto on --transport ucx)");
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
    let mut ucx_regpool_cap_bytes: Option<usize> = None;
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
                transport = autumn_transport::parse_transport_flag(&raw[i]).unwrap_or_else(|bad| {
                    eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                    std::process::exit(2);
                });
                i += 1;
            }
            "--ucx-regpool-cap-bytes" => {
                i += 1;
                ucx_regpool_cap_bytes = Some(
                    raw[i]
                        .parse()
                        .expect("--ucx-regpool-cap-bytes usize"),
                );
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
        | "policy-candidates" | "policy_candidates" | "policy" | "compact" | "gc" | "forcegc"
        | "register-node" | "format" | "info" => {
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
            Command::Put { key, file }
        }
        "put-zc" => {
            if i + 1 >= raw.len() {
                eprintln!("put-zc requires <KEY> <FILE>");
                std::process::exit(1);
            }
            Command::PutZc {
                key: raw[i].clone(),
                file: raw[i + 1].clone(),
            }
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
            Command::PutStream {
                key,
                file,
                chunk_size,
            }
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
            Command::GetStream {
                key: raw[i].clone(),
                chunk_size,
                out,
            }
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
        "zc-get" => {
            if i >= raw.len() {
                eprintln!("zc-get requires <KEY>");
                std::process::exit(1);
            }
            Command::ZcGet {
                key: raw[i].clone(),
            }
        }
        "del" => {
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
        "perf-check" => {
            let mut threads = 256usize;
            let mut duration_secs = 10u64;
            let mut value_size = 4096usize;
            let mut baseline_file = "perf_baseline.json".to_string();
            let mut threshold = 0.8f64;
            let mut update_baseline = false;
            let mut partitions_meta_from_flag: usize = 1;
            let mut pipeline_depth: usize = 1;
            // Experimental: when > 0, the write loop bundles N keys per
            // `put_many` call (client-side fan-out) instead of issuing one
            // `kv_put` per iteration. Tests whether writer_task's
            // tcp_sendmsg coalescing + read_loop bulk decode gives the
            // server-side "fat arrival" partition_loop needs to grow
            // batch_size. 0 = legacy per-op fan_out.
            let mut batch_put: usize = 0;
            let mut batch_get: usize = 0;
            // F195: was `AUTUMN_GROUP_COMMIT_CAP` env read at baseline-
            // write time. Now an explicit CLI flag — operators pass the
            // same value to autumn-ps (`--group-commit-cap N`) AND to
            // perf-check (`--group-commit-cap N`) so the baseline JSON
            // reflects the server config that was active.
            let mut group_commit_cap: Option<usize> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--zc" => {
                        // F216-E: removed. Zero-copy is now the DEFAULT on the
                        // UCX transport (writes always; reads when value >=
                        // UCX_ZC_READ_MIN_BYTES). Kept as a no-op so existing
                        // perf_check.sh / scripts don't hard-fail; on TCP it
                        // stays the regular path. Same spirit as --nosync.
                        warn_zc_flag_deprecated_once();
                    }
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
                        partitions_meta_from_flag = raw[i]
                            .parse()
                            .expect("--partitions must be a positive integer");
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
                    "--batch-put" => {
                        i += 1;
                        batch_put = raw[i]
                            .parse()
                            .expect("--batch-put must be a non-negative integer");
                    }
                    "--batch-get" => {
                        i += 1;
                        batch_get = raw[i]
                            .parse()
                            .expect("--batch-get must be a non-negative integer");
                    }
                    "--group-commit-cap" => {
                        i += 1;
                        group_commit_cap =
                            Some(raw[i].parse().expect("--group-commit-cap must be a u64"));
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
                baseline_file,
                threshold,
                update_baseline,
                partitions: partitions_meta_from_flag,
                pipeline_depth,
                group_commit_cap,
                batch_put,
                batch_get,
            }
        }
        other => {
            eprintln!("unknown command: {other}");
            usage();
        }
    };

    Args {
        manager,
        command,
        transport,
        ucx_regpool_cap_bytes,
    }
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

fn warn_zc_flag_deprecated_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        eprintln!(
            "[autumn-client] note: --zc was removed (F216-E). Zero-copy is now \
             the DEFAULT on --transport ucx: writes always; reads when value \
             >= {} B. On --transport tcp the regular path is used. Flag ignored.",
            autumn_client::UCX_ZC_READ_MIN_BYTES
        );
    });
}

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
    let client = ClusterClient::connect(&args.manager).await?;

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
            // Register the value's backing memory so the UCX send is zero-copy
            // via rcache (ucx build). Hold the RegisteredMem until put_zc
            // completes. On TCP / no-ucx this is a no-op.
            #[cfg(feature = "ucx")]
            let _reg = (!value.is_empty())
                .then(|| {
                    autumn_transport::register_memory(
                        value.as_ptr() as *mut std::ffi::c_void,
                        value.len(),
                    )
                })
                .transpose()
                .map_err(|e| anyhow!("put-zc register source: {e}"))?;
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
                let end = (idx + chunk_size).min(payload.len());
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
            // Register dest for true UCX zero-copy receive (ucx build only).
            #[cfg(feature = "ucx")]
            let reg = (!dest.is_empty())
                .then(|| {
                    autumn_transport::register_memory(
                        dest.as_mut_ptr() as *mut std::ffi::c_void,
                        dest.len(),
                    )
                })
                .transpose()
                .map_err(|e| anyhow!("zc-get register dest: {e}"))?;
            #[cfg(not(feature = "ucx"))]
            let reg: Option<autumn_rpc::RegisteredMem> = None;
            match client
                .get_into(key.as_bytes(), &mut dest, reg.as_ref())
                .await
            {
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
            batch_put,
            batch_get,
        } => {
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
            let start_keys: Vec<Vec<u8>> = (0..threads)
                .map(|tid| partitions[tid % partitions.len()].2.clone())
                .collect();
            let mgr = Arc::new(args.manager.clone());

            let deadline =
                Arc::new(std::time::SystemTime::now() + Duration::from_secs(duration_secs));
            let total_ops = Arc::new(AtomicU64::new(0));
            let bench_start = Instant::now();

            let mut write_handles = Vec::new();
            for (tid, start_key) in start_keys.iter().cloned().enumerate() {
                let mgr = Arc::clone(&mgr);
                let deadline = Arc::clone(&deadline);
                let total_ops = Arc::clone(&total_ops);
                let value_bytes = (0..value_size)
                    .map(|i| (i % 256) as u8)
                    .collect::<Vec<u8>>();
                let depth = pipeline_depth;
                let batch_put = batch_put;
                let handle = std::thread::spawn(move || {
                    compio::runtime::RuntimeBuilder::new()
                        .build()
                        .unwrap()
                        .block_on(async move {
                            use futures::stream::StreamExt;
                            let client = match autumn_client::ClusterClient::connect(&mgr).await {
                                Ok(c) => c,
                                Err(e) => {
                                    eprintln!("write thread {tid} connect error: {e}");
                                    return (Vec::<f64>::new(), 0u64);
                                }
                            };
                            let value_zc: bytes::Bytes = bytes::Bytes::from(value_bytes);
                            let mut lats: Vec<f64> = Vec::new();
                            let mut written = 0u64;
                            let cref = &client;
                            let dl = deadline.as_ref();
                            let vz = &value_zc;
                            let sk = start_key.as_slice();
                            let mut seq = 0u64;
                            if batch_put > 0 {
                                // Server-side BATCH_PUT path: build a group
                                // of N (key, value) per round, submit via
                                // `batch_put`. One MSG_BATCH_PUT frame per
                                // partition → server decodes once, injects
                                // all per-partition ops into partition_loop
                                // pending as ONE mpsc message → wide batch
                                // fires + multiple concurrent batches.
                                // Compared with the old put_many path
                                // (which also sent the same wire data
                                // but as N separate frames), this saves
                                // server-side per-frame decode overhead +
                                // gives the partition_loop atomic pending
                                // injection (the actual perf-improving
                                // primitive).
                                while std::time::SystemTime::now() < *dl {
                                    let keys: Vec<String> = (0..batch_put)
                                        .map(|_| {
                                            let k = key_for_partition(sk, "pc", tid, seq);
                                            seq += 1;
                                            k
                                        })
                                        .collect();
                                    let items: Vec<(&[u8], bytes::Bytes)> = keys
                                        .iter()
                                        .map(|k| (k.as_bytes(), vz.clone()))
                                        .collect();
                                    let t0 = Instant::now();
                                    let res = cref.batch_put(&items).await;
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
            let mut written_per_thread: Vec<u64> = vec![0; threads];
            for (tid, h) in write_handles.into_iter().enumerate() {
                if let Ok((lats, written)) = h.join() {
                    all_write_latencies.extend(lats);
                    written_per_thread[tid] = written;
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
                let batch_get = batch_get;
                let handle = std::thread::spawn(move || {
                    compio::runtime::RuntimeBuilder::new()
                        .build()
                        .unwrap()
                        .block_on(async move {
                            use futures::stream::StreamExt;
                            let client = match autumn_client::ClusterClient::connect(&mgr).await {
                                Ok(c) => c,
                                Err(e) => {
                                    eprintln!("read thread {tid} connect error: {e}");
                                    return Vec::<f64>::new();
                                }
                            };
                            let mut lats: Vec<f64> = Vec::new();
                            let cref = &client;
                            let dl = deadline.as_ref();
                            let sk = start_key.as_slice();
                            if batch_get > 0 {
                                // Server-side BATCH_GET path: mirrors the
                                // write phase's `batch_put`. Per round we
                                // build N keys and submit via
                                // `cref.batch_get(&keys)`; the SDK groups by
                                // owning partition and emits ONE
                                // MSG_BATCH_GET frame per partition.
                                // Server decodes once, runs the per-key
                                // get inline on the ps-conn task (no mpsc
                                // hop), packs the responses into a single
                                // BatchGetResp frame. Validates the
                                // BATCH_GET wire end-to-end + measures
                                // the read equivalent of the write-side
                                // 6.9× per-frame amortisation win.
                                let mut ki = 0u64;
                                while std::time::SystemTime::now() < *dl {
                                    let keys: Vec<String> = (0..batch_get)
                                        .map(|_| {
                                            let seq = ki % written;
                                            ki += 1;
                                            key_for_partition(sk, "pc", tid, seq)
                                        })
                                        .collect();
                                    let key_refs: Vec<&[u8]> =
                                        keys.iter().map(|k| k.as_bytes()).collect();
                                    let t0 = Instant::now();
                                    let res = cref.batch_get(&key_refs).await;
                                    let el = t0.elapsed();
                                    // We treat both `Ok(Some(_))` and
                                    // `Ok(None)` as "served by the
                                    // server" — perf-check sample
                                    // signal is wire RTT + decode, not
                                    // a presence assertion (the write
                                    // phase guarantees presence by
                                    // construction modulo a tiny tail
                                    // of in-flight writes never landed
                                    // on the read deadline).
                                    let n_ok = res.iter().filter(|r| r.is_ok()).count();
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
                                    let ok = if zc_read {
                                        let mut dest = vec![0u8; value_size];
                                        cref.get_into(key.as_bytes(), &mut dest, None).await.is_ok()
                                    } else {
                                        cref.get(key.as_bytes()).await.is_ok()
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
        }
    }

    Ok(())
}
