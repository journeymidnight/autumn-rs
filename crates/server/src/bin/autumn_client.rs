#[cfg(unix)]
extern crate libc;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use autumn_client::{ClusterClient, parse_addr, decode_err, DEFAULT_RPC_TIMEOUT};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{
    PutReq, GetReq, MSG_PUT, MSG_GET,
    GetDiscardsReq, GetDiscardsResp, MSG_GET_DISCARDS,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Hex presplit algorithm
// ---------------------------------------------------------------------------

fn hex_split_ranges(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    if n <= 1 {
        return vec![(vec![], vec![])];
    }
    let start: u64 = 0x00000000;
    let end: u64 = 0xFFFFFFFF;
    let size = (end - start) / n as u64;

    let mut split_points: Vec<Vec<u8>> = Vec::new();
    for i in 1..n {
        let point = start + size * i as u64;
        let hex_str = format!("{:08x}", point);
        split_points.push(hex_str.into_bytes());
    }

    let mut ranges = Vec::new();
    for i in 0..n {
        let start_key = if i == 0 {
            vec![]
        } else {
            split_points[i - 1].clone()
        };
        let end_key = if i == n - 1 {
            vec![]
        } else {
            split_points[i].clone()
        };
        ranges.push((start_key, end_key));
    }
    ranges
}

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
    Bootstrap {
        replication: String,
        presplit: String,
        /// parsed `--log-ec K+M`; None = use same replicates as meta_stream, no EC
        log_ec: Option<(u32, u32)>,
        /// parsed `--row-ec K+M`; None = use same replicates as meta_stream, no EC
        row_ec: Option<(u32, u32)>,
    },
    SetStreamEc {
        stream_id: u64,
        ec_data: u32,
        ec_parity: u32,
    },
    /// F203: per-extent EC convert trigger. OP runs this after reading
    /// `client policy` to act on a specific `POLICY_KIND_EC` advisory.
    /// The manager persists a marker; the next ec dispatch tick (~5s)
    /// drives the conversion.
    ForceEcConvert {
        extent_id: u64,
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
    Split {
        part_id: u64,
    },
    /// F183: merge two adjacent partitions on the same PS.
    /// Survivor keeps its part_id; victim is deleted.
    Merge {
        survivor_part_id: u64,
        victim_part_id: u64,
    },
    /// F183: show advisory split/merge candidates from the manager's
    /// policy engine.
    PolicyCandidates,
    Compact {
        part_id: u64,
    },
    Gc {
        part_id: u64,
        /// F201: discard-ratio threshold; None = use PS default (0.4).
        ratio: Option<f64>,
        /// F201: only consider sealed extents at most this many bytes.
        max_size: Option<u64>,
        /// F201: when partition's total reclaimable bytes exceed this,
        /// PS halves the per-extent ratio for this dispatch.
        stream_debt: Option<u64>,
        /// F201: only pick `sealed_length == 0` non-tail extents.
        empty_only: bool,
    },
    ForceGc {
        part_id: u64,
        extent_ids: Vec<u64>,
    },
    RegisterNode {
        addr: String,
        disks: Vec<String>,
        /// F099-M: per-shard listener ports on the extent-node process.
        /// Empty = legacy single-thread extent-node (client routes all
        /// extents to `addr`).
        shard_ports: Vec<u16>,
        /// F191: control-plane listener address. Empty = manager falls
        /// back to `addr` for DF (legacy behaviour).
        control_address: String,
    },
    Format {
        listen: String,
        advertise: String,
        dirs: Vec<String>,
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
    Info {
        json: bool,
        part: Option<u64>,
        /// F203: print PartitionLoad detail (F202 fields) for `part`.
        /// Requires `--part PID`. Reads from the manager's cached
        /// `MSG_GET_PARTITION_DETAIL`, not from the PS directly.
        detail: bool,
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
    eprintln!("Commands:");
    eprintln!("  bootstrap [--replication 3+0] [--log-ec K+M] [--row-ec K+M] [--presplit 1:normal|N:hexstring]");
    eprintln!("                                    Create initial partition(s) and streams");
    eprintln!("  set-stream-ec --stream <ID> --ec K+M");
    eprintln!("                                    Change EC policy of an existing stream (FOPS-03)");
    eprintln!("  force-ec-convert --extent <EXTID> (F203: per-extent EC trigger)");
    eprintln!("  put <KEY> <FILE>                  Put key with value from file");
    eprintln!("  put-stream [--chunk-size N] <KEY> <FILE-or->>");
    eprintln!("                                    Chunked stream put (default 4 MiB chunks; F186)");
    eprintln!("  get <KEY>                         Get value for key");
    eprintln!("  get-stream [--chunk-size N] [--out FILE] <KEY>");
    eprintln!("                                    Chunked stream get (default 4 MiB chunks)");
    eprintln!("  del <KEY>                         Delete key");
    eprintln!("  head <KEY>                        Get key metadata (size)");
    eprintln!("  ls [--prefix P] [--start S] [--limit N]  List keys");
    eprintln!("  split <PARTID>                    Split partition");
    eprintln!("  merge <SURVIVOR_PARTID> <VICTIM_PARTID>");
    eprintln!("                                    Merge two adjacent partitions on the same PS (F183)");
    eprintln!("  policy                            Show advisory split/merge candidates from the manager (F183)");
    eprintln!("  compact <PARTID>                  Trigger major compaction");
    eprintln!("  gc [--ratio R] [--max-size B] [--stream-debt B] [--empty-only] <PARTID>");
    eprintln!("                                    Trigger auto GC (F201: optional multi-tier filters)");
    eprintln!("  forcegc <PARTID> <EXTID>...       Force GC specific extents");
    eprintln!("  register-node --addr <ADDR> --disk <UUID> [--disk <UUID>...] [--shard-ports P1,P2,...] [--control-address <ADDR>]");
    eprintln!("                                    Register an already-formatted extent node with the manager");
    eprintln!("  format --listen <ADDR> --advertise <ADDR> <DIR>...");
    eprintln!("                                    Format disks and register node");
    eprintln!("  wbench [--threads 4] [--duration 10] [--size 8192] [--report-interval 1] [--part-id ID] [--reuse-value true|false]");
    eprintln!("                                    Write benchmark (always durable; F178 removed --nosync)");
    eprintln!("  rbench [--threads 40] [--duration 10] <RESULT_FILE>");
    eprintln!("                                    Read benchmark");
    eprintln!("  perf-check [--threads 256] [--duration 10] [--size 4096] [--baseline perf_baseline.json] [--threshold 0.8] [--update-baseline] [--partitions N] [--pipeline-depth K]");
    eprintln!("                                    Quick write+read bench; warns if >threshold regression vs baseline");
    eprintln!("  info [--json] [--part PID] [--detail]  Show cluster info (F203 --detail: PartitionLoad snapshot)");
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
        "bootstrap" => {
            let mut replication = String::from("3+0");
            let mut presplit = String::from("1:normal");
            let mut log_ec: Option<(u32, u32)> = None;
            let mut row_ec: Option<(u32, u32)> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--replication" => {
                        i += 1;
                        replication = raw[i].clone();
                    }
                    "--presplit" => {
                        i += 1;
                        presplit = raw[i].clone();
                    }
                    "--log-ec" => {
                        i += 1;
                        log_ec = Some(parse_ec_flag(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--log-ec: {e}");
                            std::process::exit(1);
                        }));
                    }
                    "--row-ec" => {
                        i += 1;
                        row_ec = Some(parse_ec_flag(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--row-ec: {e}");
                            std::process::exit(1);
                        }));
                    }
                    _ => break,
                }
                i += 1;
            }
            Command::Bootstrap {
                replication,
                presplit,
                log_ec,
                row_ec,
            }
        }
        "set-stream-ec" => {
            let mut stream_id: Option<u64> = None;
            let mut ec: Option<(u32, u32)> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--stream" => {
                        i += 1;
                        stream_id = Some(raw[i].parse().unwrap_or_else(|_| {
                            eprintln!("--stream requires a numeric stream ID");
                            std::process::exit(1);
                        }));
                    }
                    "--ec" => {
                        i += 1;
                        ec = Some(parse_ec_flag(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--ec: {e}");
                            std::process::exit(1);
                        }));
                    }
                    _ => break,
                }
                i += 1;
            }
            let stream_id = stream_id.unwrap_or_else(|| {
                eprintln!("set-stream-ec requires --stream <ID>");
                std::process::exit(1);
            });
            let (ec_data, ec_parity) = ec.unwrap_or_else(|| {
                eprintln!("set-stream-ec requires --ec K+M");
                std::process::exit(1);
            });
            Command::SetStreamEc { stream_id, ec_data, ec_parity }
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
        "split" => {
            if i >= raw.len() {
                eprintln!("split requires <PARTID>");
                std::process::exit(1);
            }
            Command::Split {
                part_id: raw[i].parse().expect("PARTID must be a number"),
            }
        }
        "merge" => {
            if i + 1 >= raw.len() {
                eprintln!("merge requires <SURVIVOR_PART_ID> <VICTIM_PART_ID>");
                std::process::exit(1);
            }
            Command::Merge {
                survivor_part_id: raw[i].parse().expect("SURVIVOR_PART_ID must be a number"),
                victim_part_id: raw[i + 1].parse().expect("VICTIM_PART_ID must be a number"),
            }
        }
        "policy-candidates" | "policy_candidates" | "policy" => Command::PolicyCandidates,
        "compact" => {
            if i >= raw.len() {
                eprintln!("compact requires <PARTID>");
                std::process::exit(1);
            }
            Command::Compact {
                part_id: raw[i].parse().expect("PARTID must be a number"),
            }
        }
        "gc" => {
            // F201: gc [--ratio R] [--max-size B] [--stream-debt B] [--empty-only] <PARTID>
            let mut ratio: Option<f64> = None;
            let mut max_size: Option<u64> = None;
            let mut stream_debt: Option<u64> = None;
            let mut empty_only = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--ratio" => {
                        i += 1;
                        ratio = Some(raw[i].parse().unwrap_or_else(|_| {
                            eprintln!("--ratio expects a float 0.0..=1.0");
                            std::process::exit(1);
                        }));
                    }
                    "--max-size" => {
                        i += 1;
                        max_size = Some(parse_byte_size(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--max-size: {e}");
                            std::process::exit(1);
                        }));
                    }
                    "--stream-debt" => {
                        i += 1;
                        stream_debt = Some(parse_byte_size(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--stream-debt: {e}");
                            std::process::exit(1);
                        }));
                    }
                    "--empty-only" => {
                        empty_only = true;
                    }
                    _ => break,
                }
                i += 1;
            }
            if i >= raw.len() {
                eprintln!("gc requires <PARTID>");
                std::process::exit(1);
            }
            Command::Gc {
                part_id: raw[i].parse().expect("PARTID must be a number"),
                ratio,
                max_size,
                stream_debt,
                empty_only,
            }
        }
        "forcegc" => {
            if i >= raw.len() {
                eprintln!("forcegc requires <PARTID> <EXTID>...");
                std::process::exit(1);
            }
            let part_id: u64 = raw[i].parse().expect("PARTID must be a number");
            i += 1;
            let mut extent_ids = Vec::new();
            while i < raw.len() {
                extent_ids.push(raw[i].parse::<u64>().expect("EXTID must be a number"));
                i += 1;
            }
            if extent_ids.is_empty() {
                eprintln!("forcegc requires at least one <EXTID>");
                std::process::exit(1);
            }
            Command::ForceGc {
                part_id,
                extent_ids,
            }
        }
        "register-node" => {
            let mut addr = String::new();
            let mut disks: Vec<String> = Vec::new();
            let mut shard_ports: Vec<u16> = Vec::new();
            // F191: optional --control-address override; default = empty
            // (manager falls back to data-plane addr).
            let mut control_address = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--addr" => { i += 1; addr = raw[i].clone(); }
                    "--disk" => { i += 1; disks.push(raw[i].clone()); }
                    "--shard-ports" => {
                        i += 1;
                        for part in raw[i].split(',') {
                            let p = part.trim();
                            if p.is_empty() { continue; }
                            let port: u16 = p.parse()
                                .expect("--shard-ports entries must be u16");
                            shard_ports.push(port);
                        }
                    }
                    "--control-address" => { i += 1; control_address = raw[i].clone(); }
                    _ => {}
                }
                i += 1;
            }
            if addr.is_empty() {
                eprintln!("register-node requires --addr");
                std::process::exit(1);
            }
            if disks.is_empty() {
                disks.push("disk-default".to_string());
            }
            Command::RegisterNode { addr, disks, shard_ports, control_address }
        }
        "format" => {
            let mut listen = String::new();
            let mut advertise = String::new();
            let mut dirs = Vec::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--listen" => {
                        i += 1;
                        listen = raw[i].clone();
                    }
                    "--advertise" => {
                        i += 1;
                        advertise = raw[i].clone();
                    }
                    _ => dirs.push(raw[i].clone()),
                }
                i += 1;
            }
            if listen.is_empty() || advertise.is_empty() || dirs.is_empty() {
                eprintln!("format requires --listen <ADDR> --advertise <ADDR> <DIR>...");
                std::process::exit(1);
            }
            Command::Format {
                listen,
                advertise,
                dirs,
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
        "info" => {
            // F205: dropped `--top N` flag. Python scripts use
            // `info --json | jq sort_by(...)` for ranking; no need
            // for the kernel to host that policy logic.
            let mut json = false;
            let mut part: Option<u64> = None;
            let mut detail = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--json" => { json = true; }
                    "--part" => {
                        i += 1;
                        if i >= raw.len() { eprintln!("--part requires a number"); usage(); }
                        part = Some(raw[i].parse().unwrap_or_else(|_| { eprintln!("--part requires a number"); usage() }));
                    }
                    "--detail" => { detail = true; }
                    "--top" => {
                        // F205: removed. Helpful migration message
                        // (passes nothing on; just exits).
                        eprintln!("--top removed in F205; use `info --json | jq 'sort_by(.size_bytes) | reverse | .[:N]'` instead");
                        std::process::exit(2);
                    }
                    other => {
                        eprintln!("unknown info flag: {other}");
                        usage();
                    }
                }
                i += 1;
            }
            if detail && part.is_none() {
                eprintln!("--detail requires --part <PID>");
                usage();
            }
            Command::Info { json, part, detail }
        }
        "force-ec-convert" => {
            // F203: force-ec-convert --extent <EXTID>
            let mut extent_id: Option<u64> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--extent" => {
                        i += 1;
                        if i >= raw.len() { eprintln!("--extent requires a number"); usage(); }
                        extent_id = Some(raw[i].parse().unwrap_or_else(|_| {
                            eprintln!("--extent requires a number");
                            usage()
                        }));
                    }
                    _ => break,
                }
                i += 1;
            }
            let extent_id = extent_id.unwrap_or_else(|| {
                eprintln!("force-ec-convert requires --extent <EXTID>");
                std::process::exit(1);
            });
            Command::ForceEcConvert { extent_id }
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

/// F201: accept human-readable byte sizes for `--max-size` / `--stream-debt`.
/// Plain integers are bytes; suffixes (B/K/M/G/Ki/Mi/Gi, case-insensitive,
/// optional "B" tail) scale accordingly. Examples: "16777216", "16MiB",
/// "1G", "512K".
fn parse_byte_size(s: &str) -> Result<u64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        bail!("empty byte-size value");
    }
    let bytes = trimmed.as_bytes();
    let mut split = bytes.len();
    for (i, &b) in bytes.iter().enumerate() {
        if !b.is_ascii_digit() && b != b'.' {
            split = i;
            break;
        }
    }
    let (num, unit) = trimmed.split_at(split);
    let num: f64 = num.parse().context("byte-size numeric prefix")?;
    let mul: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0_f64.powi(4),
        other => bail!("unknown byte-size suffix {other:?}"),
    };
    let n = (num * mul).round();
    if n.is_sign_negative() || !n.is_finite() {
        bail!("byte-size out of range");
    }
    Ok(n as u64)
}

fn parse_replication(s: &str) -> Result<u32> {
    let n_str = s.split('+').next().unwrap_or(s);
    let n: u32 = n_str.parse().context("parse replica count")?;
    if n == 0 {
        bail!("replication count must be >= 1");
    }
    Ok(n)
}

/// Parse `K+M` EC shape string into `(data_shards, parity_shards)`.
fn parse_ec_flag(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = s.splitn(2, '+').collect();
    if parts.len() != 2 {
        bail!("EC shape must be K+M (e.g. '3+1'), got '{s}'");
    }
    let k: u32 = parts[0].parse().with_context(|| format!("parse K in EC '{s}'"))?;
    let m: u32 = parts[1].parse().with_context(|| format!("parse M in EC '{s}'"))?;
    if k == 0 || m == 0 {
        bail!("EC K and M must both be >= 1, got '{s}'");
    }
    Ok((k, m))
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

// ---------------------------------------------------------------------------
// `info` JSON output types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct InfoDiskView {
    disk_id: u64,
    uuid: String,
    online: bool,
}

#[derive(Serialize)]
struct InfoNodeView {
    node_id: u64,
    address: String,
    disks: Vec<InfoDiskView>,
}

#[derive(Serialize)]
struct InfoExtentView {
    extent_id: u64,
    size: u64,
    open: bool,
    replicas: Vec<u64>,
    parity: Vec<u64>,
    refs: u64,
    eversion: u64,
}

#[derive(Serialize)]
struct InfoStreamView {
    stream_id: u64,
    replicates: u32,
    ec_data: u32,
    ec_parity: u32,
    extent_ids: Vec<u64>,
    total_size: u64,
}

#[derive(Serialize)]
struct InfoDiscardEntry {
    extent_id: u64,
    bytes: i64,
}

#[derive(Serialize)]
struct InfoPartitionView {
    part_id: u64,
    ps_addr: String,
    range_start: String,
    range_end: String,
    live_size: u64,
    total_extents: usize,
    log_stream_id: u64,
    row_stream_id: u64,
    meta_stream_id: u64,
    discards: Vec<InfoDiscardEntry>,
}

#[derive(Serialize)]
struct InfoSnapshot {
    nodes: Vec<InfoNodeView>,
    extents: Vec<InfoExtentView>,
    streams: Vec<InfoStreamView>,
    partitions: Vec<InfoPartitionView>,
}

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

// ---------------------------------------------------------------------------
// Format helpers
// ---------------------------------------------------------------------------

/// F191: derive the default control-plane address from a data-plane
/// `host:port` (or `[v6]:port`). Returns the same host with `port + 1000`.
/// Returns the empty string on parse failure — the manager treats empty
/// `control_address` as "fall back to the data-plane addr". Tested below.
pub(crate) fn derive_control_address(advertise: &str) -> String {
    let Some(colon) = advertise.rfind(':') else {
        return String::new();
    };
    let (host, port_str) = advertise.split_at(colon);
    let port_str = &port_str[1..];
    let Ok(port) = port_str.parse::<u16>() else {
        return String::new();
    };
    let Some(ctl_port) = port.checked_add(1000) else {
        return String::new();
    };
    format!("{host}:{ctl_port}")
}

fn format_disk(dir: &str) -> Result<String> {
    for byte in 0u8..=255 {
        let subdir = format!("{}/{:02x}", dir, byte);
        std::fs::create_dir_all(&subdir)
            .with_context(|| format!("create hash subdir {subdir}"))?;
    }
    let disk_uuid = uuid::Uuid::new_v4().to_string();
    let marker_path = format!("{}/{}", dir, disk_uuid);
    std::fs::File::create(&marker_path)
        .with_context(|| format!("create UUID marker {marker_path}"))?;
    Ok(disk_uuid)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F191: control-address derivation defaults to `host:port + 1000`,
    /// falls back to empty on bogus input.
    #[test]
    fn derive_control_address_ipv4_offsets_port_by_1000() {
        assert_eq!(
            super::derive_control_address("127.0.0.1:9101"),
            "127.0.0.1:10101"
        );
        assert_eq!(
            super::derive_control_address("10.0.0.42:9201"),
            "10.0.0.42:10201"
        );
    }

    #[test]
    fn derive_control_address_v6_bracketed_offsets_port_by_1000() {
        assert_eq!(
            super::derive_control_address("[fe80::1]:9101"),
            "[fe80::1]:10101"
        );
    }

    #[test]
    fn derive_control_address_falls_back_to_empty_on_bad_input() {
        assert_eq!(super::derive_control_address("no-port-here"), "");
        assert_eq!(super::derive_control_address(""), "");
        // port overflow → empty
        assert_eq!(super::derive_control_address("127.0.0.1:65000"), "");
    }

    /// F201: --max-size / --stream-debt accept human-readable byte
    /// sizes. Test the canonical shapes operators are most likely to
    /// type.
    #[test]
    fn parse_byte_size_accepts_plain_integer() {
        assert_eq!(super::parse_byte_size("16777216").unwrap(), 16 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("0").unwrap(), 0);
    }

    #[test]
    fn parse_byte_size_accepts_suffixes() {
        assert_eq!(super::parse_byte_size("512K").unwrap(), 512 * 1024);
        assert_eq!(super::parse_byte_size("16M").unwrap(), 16 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("16MiB").unwrap(), 16 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("2T").unwrap(), 2u64 * 1024 * 1024 * 1024 * 1024);
        // case insensitive
        assert_eq!(super::parse_byte_size("4mib").unwrap(), 4 * 1024 * 1024);
    }

    #[test]
    fn parse_byte_size_rejects_garbage() {
        assert!(super::parse_byte_size("").is_err());
        assert!(super::parse_byte_size("foo").is_err());
        assert!(super::parse_byte_size("16XB").is_err());
    }

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
    let _ = autumn_transport::init_with(args.transport);
    let mut client = ClusterClient::connect(&args.manager).await?;

    match args.command {
        Command::Bootstrap {
            replication,
            presplit,
            log_ec,
            row_ec,
        } => {
            let meta_replicates = parse_replication(&replication)?;

            // Per-stream (replicates, ec_data, ec_parity):
            // - Replica streams: replicates=N, ec_data=N, ec_parity=0.
            // - EC streams: replicates is the OPEN-EXTENT replica count
            //   (= meta_replicates, typically 3), and (ec_data,
            //   ec_parity) describes the POST-SEAL EC encoding (e.g.
            //   4+1, 7+1). The two are independent — the
            //   `ec_conversion_dispatch_loop` reads the sealed payload
            //   from one of the open replicas and re-encodes it into
            //   K+M shards, allocating any extra host slots beyond
            //   `replicates` on the fly. So a 3-replica stream can be
            //   converted to 4+1, 7+1, etc.
            //
            //   Pre-fix we sent `replicates=K+M`, forcing open extents
            //   onto K+M nodes (each holding a full replica). The M
            //   extra replicas got overwritten with parity bytes on
            //   conversion anyway — pure waste — and the seal/EC race
            //   blast radius was wider than necessary.
            let log_params = log_ec
                .map(|(k, m)| (meta_replicates, k, m))
                .unwrap_or((meta_replicates, meta_replicates, 0));
            let row_params = row_ec
                .map(|(k, m)| (meta_replicates, k, m))
                .unwrap_or((meta_replicates, meta_replicates, 0));
            let meta_params = (meta_replicates, meta_replicates, 0u32);

            let ranges: Vec<(Vec<u8>, Vec<u8>)> = {
                let parts: Vec<&str> = presplit.splitn(2, ':').collect();
                let n: usize = parts[0].parse().unwrap_or(1);
                let kind = parts.get(1).copied().unwrap_or("normal");
                match kind {
                    "hexstring" => hex_split_ranges(n),
                    _ => vec![(vec![], vec![])],
                }
            };

            let create_stream_once = |label: &'static str, replicates: u32, ec_data: u32, ec_parity: u32| {
                let client = &client;
                async move {
                    let req_bytes = rkyv_encode(&CreateStreamReq {
                        replicates,
                        ec_data_shard: ec_data,
                        ec_parity_shard: ec_parity,
                    });
                    let mut attempt = 0u32;
                    loop {
                        // mgr_call honors ClusterClient.rpc_timeout
                        // (default 30 s) — bounded RPC.
                        let resp_bytes = client
                            .mgr_call(MSG_CREATE_STREAM, req_bytes.clone())
                            .await
                            .with_context(|| format!("create {label} stream"))?;
                        let resp: CreateStreamResp =
                            rkyv_decode(&resp_bytes).map_err(decode_err)?;
                        if resp.code == CODE_OK {
                            return Ok::<u64, anyhow::Error>(
                                resp.stream.map(|s| s.stream_id).unwrap_or(0),
                            );
                        }
                        if resp.code == CODE_NOT_LEADER && attempt < 60 {
                            attempt += 1;
                            compio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        bail!("create {label} stream failed: code={} {}", resp.code, resp.message);
                    }
                }
            };

            for (idx, (start_key, end_key)) in ranges.iter().enumerate() {
                let (log_repl, log_k, log_m) = log_params;
                let (row_repl, row_k, row_m) = row_params;
                let (meta_repl, meta_k, meta_m) = meta_params;
                let log_stream_id = create_stream_once("log", log_repl, log_k, log_m).await?;
                let row_stream_id = create_stream_once("row", row_repl, row_k, row_m).await?;
                let meta_stream_id = create_stream_once("meta", meta_repl, meta_k, meta_m).await?;

                let meta = MgrPartitionMeta {
                    log_stream: log_stream_id,
                    row_stream: row_stream_id,
                    meta_stream: meta_stream_id,
                    part_id: 0, // auto-assigned by manager via alloc_ids
                    rg: Some(MgrRange {
                        start_key: start_key.clone(),
                        end_key: end_key.clone(),
                    }),
                };

                let req_bytes = rkyv_encode(&UpsertPartitionReq { meta });
                let mut attempt = 0u32;
                let resp = loop {
                    let resp_bytes = client
                        .mgr_call(MSG_UPSERT_PARTITION, req_bytes.clone())
                        .await
                        .context("upsert partition")?;
                    let resp: UpsertPartitionResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
                    if resp.code == CODE_OK {
                        break resp;
                    }
                    if resp.code == CODE_NOT_LEADER && attempt < 60 {
                        attempt += 1;
                        compio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    bail!("bootstrap partition {} failed: code={} {}", idx, resp.code, resp.message);
                };

                let start_s = if start_key.is_empty() {
                    String::from("\"\"")
                } else {
                    String::from_utf8_lossy(start_key).to_string()
                };
                let end_s = if end_key.is_empty() {
                    String::from("\"\"")
                } else {
                    String::from_utf8_lossy(end_key).to_string()
                };
                let (_, log_k, log_m) = log_params;
                let (_, row_k, row_m) = row_params;
                let (_, meta_k, meta_m) = meta_params;
                println!(
                    "partition {} created: id={} log={} ({}+{}) row={} ({}+{}) meta={} ({}+{}) range=[{}..{})",
                    idx, resp.part_id,
                    log_stream_id, log_k, log_m,
                    row_stream_id, row_k, row_m,
                    meta_stream_id, meta_k, meta_m,
                    start_s, end_s,
                );
            }
            println!("bootstrap succeeded: {} partition(s)", ranges.len());
        }

        Command::SetStreamEc { stream_id, ec_data, ec_parity } => {
            let req_bytes = rkyv_encode(&UpdateStreamEcReq {
                stream_id,
                ec_data_shard: ec_data,
                ec_parity_shard: ec_parity,
            });
            let mut attempt = 0u32;
            loop {
                let resp_bytes = client
                    .mgr_call(MSG_UPDATE_STREAM_EC, req_bytes.clone())
                    .await
                    .context("update stream EC")?;
                let resp: UpdateStreamEcResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
                if resp.code == CODE_OK {
                    println!(
                        "stream {} EC updated to {}+{}; conversion will run on next manager tick (~5s)",
                        stream_id, ec_data, ec_parity
                    );
                    break;
                }
                if resp.code == CODE_NOT_LEADER && attempt < 60 {
                    attempt += 1;
                    compio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                bail!("set-stream-ec failed: code={} {}", resp.code, resp.message);
            }
        }

        Command::ForceEcConvert { extent_id } => {
            // F203
            let req = rkyv_encode(&autumn_rpc::manager_rpc::ForceEcConvertReq { extent_id });
            let resp_bytes = client
                .mgr_call(autumn_rpc::manager_rpc::MSG_FORCE_EC_CONVERT, req)
                .await
                .context("force-ec-convert")?;
            let resp: autumn_rpc::manager_rpc::ForceEcConvertResp =
                rkyv_decode(&resp_bytes).map_err(decode_err)?;
            if resp.code != autumn_rpc::manager_rpc::CODE_OK {
                bail!("force-ec-convert: code={} {}", resp.code, resp.message);
            }
            println!("{}", resp.message);
        }

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

        Command::Split { part_id } => {
            client.split(part_id).await
                .map_err(|e| anyhow!("split: {e}"))?;
            println!("split ok");
        }

        Command::Merge { survivor_part_id, victim_part_id } => {
            eprintln!(
                "F183: stop writes to partitions {survivor_part_id} and {victim_part_id} \
                 before continuing. The CLI will FLUSH both, then issue the manager merge. \
                 The survivor's PS picks up the wider range on the next region_sync (~2 s)."
            );
            client.merge_partitions(survivor_part_id, victim_part_id).await
                .map_err(|e| anyhow!("merge: {e}"))?;
            println!("merge ok: partition {victim_part_id} merged into {survivor_part_id}");
        }

        Command::PolicyCandidates => {
            let cands = client.policy_candidates().await
                .map_err(|e| anyhow!("policy_candidates: {e}"))?;
            if cands.is_empty() {
                println!("(no candidates)");
            } else {
                println!(
                    "{:<7} {:<10} {:<10} {:<46} {:<10} {:<8} {:<6} {:<5}",
                    "KIND", "PRIMARY", "SECONDARY", "REASON", "SIZE", "QPS", "IMM/s", "FEAS"
                );
                for c in cands {
                    let kind = match c.kind {
                        autumn_rpc::manager_rpc::POLICY_KIND_SPLIT => "split",
                        autumn_rpc::manager_rpc::POLICY_KIND_MERGE => "merge",
                        autumn_rpc::manager_rpc::POLICY_KIND_GC => "gc",
                        autumn_rpc::manager_rpc::POLICY_KIND_MAJOR_COMPACT => "major",
                        autumn_rpc::manager_rpc::POLICY_KIND_HOT_COLD => "hotcold",
                        // F202 additions
                        autumn_rpc::manager_rpc::POLICY_KIND_MINOR_COMPACT => "minor",
                        autumn_rpc::manager_rpc::POLICY_KIND_EC => "ec",
                        _ => "?",
                    };
                    // F187/F202: GC/COMPACT/EC advisories aren't bound by
                    // `same_ps` — they're always per-partition (or
                    // per-extent for EC) and locally feasible. F196:
                    // HOT_COLD is by-construction per-PS, always feasible
                    // from the operator's view. Render `feas` as "n/a"
                    // for them so operators don't misread the column.
                    let feas = match c.kind {
                        autumn_rpc::manager_rpc::POLICY_KIND_GC
                        | autumn_rpc::manager_rpc::POLICY_KIND_MAJOR_COMPACT
                        | autumn_rpc::manager_rpc::POLICY_KIND_MINOR_COMPACT
                        | autumn_rpc::manager_rpc::POLICY_KIND_EC
                        | autumn_rpc::manager_rpc::POLICY_KIND_HOT_COLD => "n/a",
                        _ if c.same_ps => "yes",
                        _ => "no",
                    };
                    let secondary = if c.secondary_part_id == 0 {
                        "-".to_string()
                    } else {
                        c.secondary_part_id.to_string()
                    };
                    println!(
                        "{:<7} {:<10} {:<10} {:<46} {:<10} {:<8} {:<6} {:<5}",
                        kind,
                        c.primary_part_id,
                        secondary,
                        c.reason,
                        format!("{} MB", c.size_bytes / (1024 * 1024)),
                        c.req_per_sec,
                        c.imm_full_per_sec,
                        feas,
                    );
                }
            }
        }

        Command::Compact { part_id } => {
            client.compact(part_id).await
                .map_err(|e| anyhow!("compact: {e}"))?;
            println!("compact triggered for partition {part_id}");
        }

        Command::Gc { part_id, ratio, max_size, stream_debt, empty_only } => {
            // F201: forward multi-tier params. Default (no flags) →
            // pre-F201 single-tier behaviour + empty-sealed pick.
            let params = autumn_client::GcAutoParams {
                ratio,
                max_size,
                stream_debt,
                empty_only,
            };
            client
                .gc_with_params(part_id, params.clone())
                .await
                .map_err(|e| anyhow!("gc: {e}"))?;
            println!(
                "gc triggered for partition {part_id} (ratio={:?} max_size={:?} stream_debt={:?} empty_only={})",
                params.ratio, params.max_size, params.stream_debt, params.empty_only
            );
        }

        Command::ForceGc {
            part_id,
            extent_ids,
        } => {
            client.force_gc(part_id, extent_ids.clone()).await
                .map_err(|e| anyhow!("forcegc: {e}"))?;
            println!("forcegc triggered for partition {part_id}, extents={extent_ids:?}");
        }

        Command::RegisterNode { addr, disks, shard_ports, control_address } => {
            // Retry on CODE_NOT_LEADER: manager may still be completing etcd
            // leader election when cluster.sh fires the first register-node.
            let req_bytes = rkyv_encode(&RegisterNodeReq {
                addr: addr.clone(),
                disk_uuids: disks,
                shard_ports: shard_ports.clone(),
                control_address: control_address.clone(),
            });
            let mut attempt = 0u32;
            let resp = loop {
                let resp_bytes = client
                    .mgr_call(MSG_REGISTER_NODE, req_bytes.clone())
                    .await
                    .context("register node")?;
                let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
                if resp.code == CODE_OK {
                    break resp;
                }
                if resp.code == CODE_NOT_LEADER && attempt < 60 {
                    attempt += 1;
                    compio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                bail!("register-node failed: code={} {}", resp.code, resp.message);
            };
            println!("node registered: node_id={}, addr={}", resp.node_id, addr);
            for (uuid, disk_id) in &resp.disk_uuids {
                println!("  disk {uuid} → disk_id={disk_id}");
            }
        }

        Command::Format {
            listen,
            advertise,
            dirs,
        } => {
            let mut disk_uuids = Vec::new();
            for dir in &dirs {
                std::fs::create_dir_all(dir).with_context(|| format!("create dir {dir}"))?;
                let uuid = format_disk(dir)?;
                println!("formatted {dir}: disk_uuid={uuid}");
                disk_uuids.push(uuid);
            }

            // F191: derive the control address from `advertise` by
            // offsetting the port by +1000 (matching the ExtentNode
            // default control listener). If parsing fails (unusual host
            // form), fall back to empty so the manager keeps using the
            // data-plane addr.
            let control_address = derive_control_address(&advertise);
            let resp_bytes = client
                .mgr_call(
                    MSG_REGISTER_NODE,
                    rkyv_encode(&RegisterNodeReq {
                        addr: advertise.clone(),
                        disk_uuids: disk_uuids.clone(),
                        // F099-M: format path always uses legacy single-shard mode.
                        shard_ports: vec![],
                        control_address,
                    }),
                )
                .await
                .context("register node")?;
            let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;

            let node_id = resp.node_id;
            println!("node registered: node_id={node_id}");

            for (dir, disk_uuid) in dirs.iter().zip(disk_uuids.iter()) {
                let disk_id = resp
                    .disk_uuids
                    .iter()
                    .find(|(u, _)| u == disk_uuid)
                    .map(|(_, id)| *id)
                    .unwrap_or(0);
                std::fs::write(format!("{dir}/node_id"), node_id.to_string())
                    .with_context(|| format!("write node_id in {dir}"))?;
                std::fs::write(format!("{dir}/disk_id"), disk_id.to_string())
                    .with_context(|| format!("write disk_id in {dir}"))?;
                println!("  {dir}: node_id={node_id}, disk_id={disk_id}");
            }
            println!("\nFormat complete.");
            println!("listen={listen}, advertise={advertise}");
            println!("Start the extent node with:");
            println!(
                "  autumn-extent-node --port {} --manager {} --data {}",
                listen.split(':').last().unwrap_or("9101"),
                args.manager,
                dirs.join(",")
            );
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
                        let mut cc = match ClusterClient::connect(&manager_addr).await {
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

        Command::Info { json, part, detail } => {
            // F203: --detail prints only the PartitionLoad snapshot for
            // `part`, sourced from the manager's cached metric window.
            // Short-circuits the full info path.
            if detail {
                let pid = part.expect("--detail requires --part PID; checked at parse time");
                let req = rkyv_encode(&autumn_rpc::manager_rpc::GetPartitionDetailReq {
                    part_id: pid,
                });
                let resp_bytes = client
                    .mgr_call(autumn_rpc::manager_rpc::MSG_GET_PARTITION_DETAIL, req)
                    .await
                    .context("get partition detail")?;
                let resp: autumn_rpc::manager_rpc::GetPartitionDetailResp =
                    rkyv_decode(&resp_bytes).map_err(decode_err)?;
                if resp.code != autumn_rpc::manager_rpc::CODE_OK {
                    bail!("get_partition_detail: code={} {}", resp.code, resp.message);
                }
                if json {
                    let l = &resp.load;
                    let v = serde_json::json!({
                        "part_id": pid,
                        "bucket_ts": resp.bucket_ts,
                        "size_bytes": l.size_bytes,
                        "req_per_sec": l.req_per_sec,
                        "imm_full_per_sec": l.imm_full_per_sec,
                        "p99_us": l.p99_us,
                        "gc_debt_bytes": l.gc_debt_bytes,
                        "pending_compaction_bytes": l.pending_compaction_bytes,
                        "minor_compact_pending_bytes": l.minor_compact_pending_bytes,
                        "sst_tombstone_bytes": l.sst_tombstone_bytes,
                        "sst_expired_bytes": l.sst_expired_bytes,
                        "sst_out_of_range_bytes": l.sst_out_of_range_bytes,
                        "gc_inflight": l.gc_inflight,
                        "compact_inflight": l.compact_inflight,
                        "last_gc_at": l.last_gc_at,
                        "last_compact_at": l.last_compact_at,
                        "sealed_log_extent_count": l.sealed_log_extent_count,
                    });
                    println!("{}", serde_json::to_string_pretty(&v).context("serialize detail")?);
                } else {
                    let l = &resp.load;
                    println!("=== Partition {pid} (bucket_ts={}) ===", resp.bucket_ts);
                    println!("  size_bytes={}", l.size_bytes);
                    println!("  req_per_sec={}  imm_full_per_sec={}", l.req_per_sec, l.imm_full_per_sec);
                    println!("  gc_debt_bytes={} ({} MiB)", l.gc_debt_bytes, l.gc_debt_bytes / (1024*1024));
                    println!("  pending_compaction_bytes={} ({} MiB)  [major]", l.pending_compaction_bytes, l.pending_compaction_bytes / (1024*1024));
                    println!("  minor_compact_pending_bytes={} ({} MiB)", l.minor_compact_pending_bytes, l.minor_compact_pending_bytes / (1024*1024));
                    println!("  sst_tombstone_bytes={}  sst_expired_bytes={}  sst_out_of_range_bytes={}",
                        l.sst_tombstone_bytes, l.sst_expired_bytes, l.sst_out_of_range_bytes);
                    println!("  gc_inflight={}  compact_inflight={}", l.gc_inflight, l.compact_inflight);
                    println!("  last_gc_at={}  last_compact_at={}", l.last_gc_at, l.last_compact_at);
                    println!("  sealed_log_extent_count={}", l.sealed_log_extent_count);
                }
                return Ok(());
            }

            // === Fetch manager data ===
            let stream_resp_bytes = client
                .mgr_call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq { stream_ids: Vec::new() }))
                .await
                .context("stream info")?;
            let stream_resp: StreamInfoResp = rkyv_decode(&stream_resp_bytes).map_err(decode_err)?;

            let nodes_resp_bytes = client
                .mgr_call(MSG_NODES_INFO, Bytes::new())
                .await
                .context("nodes info")?;
            let nodes_resp: NodesInfoResp = rkyv_decode(&nodes_resp_bytes).map_err(decode_err)?;

            let regions_resp_bytes = client
                .mgr_call(MSG_GET_REGIONS, Bytes::new())
                .await
                .context("get regions")?;
            let regions_resp: GetRegionsResp = rkyv_decode(&regions_resp_bytes).map_err(decode_err)?;

            // === Build lookup maps ===
            let mut extent_map: HashMap<u64, MgrExtentInfo> = stream_resp.extents.into_iter().collect();
            let disk_map: HashMap<u64, MgrDiskInfo> = nodes_resp.disks_info.into_iter().collect();

            let mut nodes_sorted: Vec<(u64, MgrNodeInfo)> = nodes_resp.nodes.into_iter().collect();
            nodes_sorted.sort_by_key(|(id, _)| *id);
            let node_map: HashMap<u64, String> = nodes_sorted.iter().map(|(id, n)| (*id, n.address.clone())).collect();

            let mut streams_sorted: Vec<(u64, MgrStreamInfo)> = stream_resp.streams.into_iter().collect();
            streams_sorted.sort_by_key(|(id, _)| *id);
            let stream_map: HashMap<u64, MgrStreamInfo> = streams_sorted.iter().cloned().collect();

            let regions: HashMap<u64, MgrRegionInfo> = regions_resp.regions.into_iter().collect();
            let ps_details: HashMap<u64, MgrPsDetail> = regions_resp.ps_details.into_iter().collect();
            let part_addr_map: HashMap<u64, String> = regions_resp.part_addrs.into_iter().collect();

            let mut part_ids: Vec<u64> = regions.keys().copied().collect();
            part_ids.sort();

            // === Query commit_length for open extents ===
            // When --part is given, only probe that partition's extents.
            let probe_set: HashSet<u64> = if let Some(pid) = part {
                regions.get(&pid).into_iter()
                    .flat_map(|r| [r.log_stream, r.row_stream, r.meta_stream])
                    .flat_map(|sid| stream_map.get(&sid).into_iter().flat_map(|s| s.extent_ids.iter().copied()))
                    .collect()
            } else {
                HashSet::new()
            };

            let mut open_extents: HashSet<u64> = HashSet::new();
            for (eid, ext) in extent_map.iter_mut() {
                if ext.sealed_length == 0 {
                    open_extents.insert(*eid);
                    if part.is_some() && !probe_set.contains(eid) {
                        continue;
                    }
                    if let Some(node_id) = ext.replicates.first() {
                        if let Some(addr) = node_map.get(node_id) {
                            if let Ok(en_client) = client.get_ps_client(addr).await {
                                // F210-H3 Tier 2: `info` has no PS-owner
                                // context so it must NOT use the
                                // fence-gated commit_length RPC. The
                                // dedicated probe RPC returns the same
                                // `(code, length)` shape without
                                // touching `last_revision`.
                                let req = ExtProbeExtentReq { extent_id: *eid };
                                if let Ok(resp_bytes) = en_client
                                    .call_timeout(EXT_MSG_PROBE_EXTENT, req.encode(), DEFAULT_RPC_TIMEOUT)
                                    .await
                                {
                                    if let Ok(resp) = ExtProbeExtentResp::decode(resp_bytes) {
                                        ext.sealed_length = resp.length as u64;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // === Fetch pending discard snapshots from each PS ===
            let pids_to_query: Vec<u64> = if let Some(pid) = part { vec![pid] } else { part_ids.clone() };
            let mut part_discards: HashMap<u64, Vec<(u64, i64)>> = HashMap::new();
            for pid in &pids_to_query {
                let r = match regions.get(pid) { Some(r) => r, None => continue };
                let ps_addr = part_addr_map.get(pid)
                    .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
                    .map(|s| s.as_str())
                    .unwrap_or("");
                if ps_addr.is_empty() { continue; }
                let req_bytes = rkyv_encode(&GetDiscardsReq { part_id: *pid });
                match client.get_ps_client(ps_addr).await {
                    Ok(ps_client) => match ps_client
                        .call_timeout(MSG_GET_DISCARDS, req_bytes, DEFAULT_RPC_TIMEOUT)
                        .await
                    {
                        Ok(resp_bytes) => match rkyv_decode::<GetDiscardsResp>(&resp_bytes) {
                            Ok(resp) if resp.code == autumn_rpc::partition_rpc::CODE_OK => {
                                part_discards.insert(*pid, resp.discards);
                            }
                            Ok(resp) => eprintln!("[warning] discard fetch part {pid}: {}", resp.message),
                            Err(e) => eprintln!("[warning] discard decode part {pid}: {e}"),
                        },
                        Err(e) => eprintln!("[warning] discard fetch failed for part {pid}: {e}"),
                    },
                    Err(e) => eprintln!("[warning] connect PS for part {pid}: {e}"),
                }
            }

            fn human_size(bytes: u64) -> String {
                if bytes >= 1 << 30 {
                    format!("{:.1} GB", bytes as f64 / (1u64 << 30) as f64)
                } else if bytes >= 1 << 20 {
                    format!("{:.1} MB", bytes as f64 / (1u64 << 20) as f64)
                } else if bytes >= 1 << 10 {
                    format!("{:.1} KB", bytes as f64 / (1u64 << 10) as f64)
                } else {
                    format!("{} B", bytes)
                }
            }

            fn stream_total(s: &MgrStreamInfo, extent_map: &HashMap<u64, MgrExtentInfo>) -> u64 {
                s.extent_ids.iter().filter_map(|eid| extent_map.get(eid)).map(|e| e.sealed_length).sum()
            }

            if json {
                // === JSON output ===
                let nodes_view: Vec<InfoNodeView> = nodes_sorted.iter()
                    .map(|(nid, n)| InfoNodeView {
                        node_id: *nid,
                        address: n.address.clone(),
                        disks: n.disks.iter().map(|did| InfoDiskView {
                            disk_id: *did,
                            uuid: disk_map.get(did).map(|d| d.uuid.clone()).unwrap_or_default(),
                            online: disk_map.get(did).map(|d| d.online).unwrap_or(false),
                        }).collect(),
                    })
                    .collect();

                let extents_view: Vec<InfoExtentView> = {
                    let mut v: Vec<_> = extent_map.iter()
                        .map(|(eid, e)| InfoExtentView {
                            extent_id: *eid,
                            size: e.sealed_length,
                            open: open_extents.contains(eid),
                            replicas: e.replicates.clone(),
                            parity: e.parity.clone(),
                            refs: e.refs,
                            eversion: e.eversion,
                        })
                        .collect();
                    v.sort_by_key(|e| e.extent_id);
                    v
                };

                let streams_view: Vec<InfoStreamView> = streams_sorted.iter()
                    .map(|(sid, s)| {
                        let r = if s.replicates > 0 {
                            s.replicates
                        } else {
                            s.extent_ids.iter()
                                .find_map(|eid| extent_map.get(eid)
                                    .filter(|e| !e.ec_converted)
                                    .map(|e| e.replicates.len() as u32))
                                .unwrap_or(0)
                        };
                        InfoStreamView {
                            stream_id: *sid,
                            replicates: r,
                            ec_data: s.ec_data_shard,
                            ec_parity: s.ec_parity_shard,
                            extent_ids: s.extent_ids.clone(),
                            total_size: stream_total(s, &extent_map),
                        }
                    })
                    .collect();

                let mut partitions_view: Vec<InfoPartitionView> = part_ids.iter()
                    .filter_map(|pid| {
                        let r = regions.get(pid)?;
                        let rg = r.rg.as_ref()?;
                        let ps_addr = part_addr_map.get(pid)
                            .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
                            .cloned()
                            .unwrap_or_else(|| "unknown".to_string());
                        let mut live_size = 0u64;
                        let mut total_extents = 0usize;
                        for sid in [r.log_stream, r.row_stream, r.meta_stream] {
                            if let Some(s) = stream_map.get(&sid) {
                                live_size += stream_total(s, &extent_map);
                                total_extents += s.extent_ids.len();
                            }
                        }
                        let discards = part_discards.get(pid)
                            .map(|v| v.iter().map(|&(eid, bytes)| InfoDiscardEntry { extent_id: eid, bytes }).collect())
                            .unwrap_or_default();
                        Some(InfoPartitionView {
                            part_id: *pid,
                            ps_addr,
                            range_start: String::from_utf8_lossy(&rg.start_key).into_owned(),
                            range_end: if rg.end_key.is_empty() { String::new() } else { String::from_utf8_lossy(&rg.end_key).into_owned() },
                            live_size,
                            total_extents,
                            log_stream_id: r.log_stream,
                            row_stream_id: r.row_stream,
                            meta_stream_id: r.meta_stream,
                            discards,
                        })
                    })
                    .collect();

                // F205: sort by live_size desc kept (consistent
                // ordering for human eyes when the same partition
                // table is dumped). Top-N truncation was removed —
                // Python consumers do `jq 'sort_by(.live_size) | reverse | .[:N]'`.
                partitions_view.sort_by(|a, b| b.live_size.cmp(&a.live_size));

                if let Some(pid) = part {
                    match partitions_view.into_iter().find(|p| p.part_id == pid) {
                        Some(pv) => println!("{}", serde_json::to_string_pretty(&pv)?),
                        None => eprintln!("partition {pid} not found"),
                    }
                } else {
                    let snapshot = InfoSnapshot { nodes: nodes_view, extents: extents_view, streams: streams_view, partitions: partitions_view };
                    println!("{}", serde_json::to_string_pretty(&snapshot)?);
                }
            } else {
                // === Text output ===
                // Determine which partitions to show. F205 removed --top.
                let show_pids: Vec<u64> = if let Some(pid) = part {
                    vec![pid]
                } else {
                    part_ids.clone()
                };

                // Nodes / Extents / Streams sections only in full mode
                if part.is_none() {
                    println!("=== Nodes ===");
                    for (nid, n) in &nodes_sorted {
                        // F191: display the control_address when present.
                        // Empty = legacy node; manager falls back to addr.
                        if n.control_address.is_empty() {
                            println!("  node {}: addr={}", nid, n.address);
                        } else {
                            println!(
                                "  node {}: addr={}, control_addr={}",
                                nid, n.address, n.control_address
                            );
                        }
                        for did in &n.disks {
                            if let Some(d) = disk_map.get(did) {
                                println!("    disk {}: uuid={}, online={}", did, d.uuid, d.online);
                            } else {
                                println!("    disk {}: (no info)", did);
                            }
                        }
                    }

                    println!("\n=== Extents ===");
                    let mut extents: Vec<(&u64, &MgrExtentInfo)> = extent_map.iter().collect();
                    extents.sort_by_key(|(id, _)| **id);
                    for (eid, e) in &extents {
                        let tag = if open_extents.contains(eid) { " (open)" } else { "" };
                        // `ec_converted` is the authoritative EC marker — set
                        // only by `apply_ec_conversion_done` after a sealed
                        // extent is actually RS-encoded. Open / pre-conversion
                        // extents on EC streams still hold full replicated
                        // data on every K+M node despite having `parity != []`
                        // pre-filled by `stream_alloc_extent`.
                        let layout = if e.ec_converted {
                            format!("EC({}+{}), data={:?}, parity={:?}",
                                e.replicates.len(), e.parity.len(), e.replicates, e.parity)
                        } else if e.parity.is_empty() {
                            format!("replicas={:?}", e.replicates)
                        } else {
                            let mut all = e.replicates.clone();
                            all.extend(e.parity.iter().copied());
                            format!("replicas={:?}", all)
                        };
                        println!("  extent {}: size={}{}, {}, refs={}, eversion={}",
                            eid, human_size(e.sealed_length), tag, layout, e.refs, e.eversion);
                    }

                    println!("\n=== Streams ===");
                    for (sid, s) in &streams_sorted {
                        let total = stream_total(s, &extent_map);
                        // (replicates, ec_data, ec_parity) triple. For
                        // legacy streams without `replicates` (default 0),
                        // derive from a non-EC-converted extent.
                        let r = if s.replicates > 0 {
                            s.replicates
                        } else {
                            s.extent_ids.iter()
                                .find_map(|eid| extent_map.get(eid)
                                    .filter(|e| !e.ec_converted)
                                    .map(|e| e.replicates.len() as u32))
                                .unwrap_or(0)
                        };
                        let layout = if s.ec_parity_shard == 0 {
                            format!("repl={}", r)
                        } else {
                            format!("repl={}, EC={}+{}", r, s.ec_data_shard, s.ec_parity_shard)
                        };
                        println!("  stream {} ({}): extents={:?}, total={}",
                            sid, layout, s.extent_ids, human_size(total));
                    }
                }

                // === Partitions section ===
                let section_header = if let Some(pid) = part {
                    format!("\n=== Partition {pid} ===")
                } else {
                    "\n=== Partitions ===".to_string()
                };
                println!("{section_header}");

                for pid in &show_pids {
                    let r = match regions.get(pid) {
                        Some(r) => r,
                        None => { println!("  part {pid}: not found"); continue; }
                    };
                    let rg = match r.rg.as_ref() { Some(r) => r, None => continue };
                    let ps_addr = part_addr_map.get(pid)
                        .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
                        .map(|s| s.as_str())
                        .unwrap_or("unknown");
                    println!("  part {}: ps={}, range=[{}..{})",
                        pid, ps_addr,
                        String::from_utf8_lossy(&rg.start_key),
                        if rg.end_key.is_empty() { "\u{221e}".to_string() } else { String::from_utf8_lossy(&rg.end_key).to_string() }
                    );
                    let discards = part_discards.get(pid);
                    let mut part_total = 0u64;
                    let mut part_extents = 0usize;
                    for (label, sid) in [("log", r.log_stream), ("row", r.row_stream), ("meta", r.meta_stream)] {
                        if let Some(s) = stream_map.get(&sid) {
                            let total = stream_total(s, &extent_map);
                            part_total += total;
                            part_extents += s.extent_ids.len();
                            let mut line = format!("    {}: stream {}, extents={:?}, size={}", label, sid, s.extent_ids, human_size(total));
                            if label == "log" {
                                if let Some(d) = discards {
                                    if !d.is_empty() {
                                        let total_discard: i64 = d.iter().map(|(_, b)| b).sum();
                                        line.push_str(&format!(", discard: {} ext / {} pending", d.len(), human_size(total_discard as u64)));
                                    }
                                }
                            }
                            println!("{line}");
                        }
                    }
                    println!("    total: {} extents, {}", part_extents, human_size(part_total));
                }
            }
        }
    }

    Ok(())
}
