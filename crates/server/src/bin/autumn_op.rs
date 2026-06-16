//! F211-G + F213: companion CLI for manager control plane.
//!
//! `autumn-op` is the canonical operator interface. It speaks rkyv over
//! the manager RPC framing and prints either human-readable or
//! `--json` output (the Python policy script in `python/node_policy.py`
//! consumes the JSON form).
//!
//! Two command families:
//!   * F211 node-lifecycle: list-nodes / extent-health / list-ec-markers
//!     / recovery-stats / audit-log + fence-node / maintenance / unfence
//!     / remove.
//!   * F213 (moved from autumn-client): bootstrap / set-stream-ec /
//!     force-ec-convert / split / merge / policy-candidates / compact /
//!     gc / forcegc / register-node / format / info.
//!
//! The data-plane CLI `autumn-client` MUST NOT regrow direct manager
//! admin RPCs — if it needs op data in the future, route through this
//! binary.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use autumn_client::{decode_err, ClusterClient, DEFAULT_RPC_TIMEOUT};
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{GetDiscardsReq, GetDiscardsResp, MSG_GET_DISCARDS};
use autumn_transport::TransportKind;
use bytes::Bytes;
use serde::Serialize;

fn usage() -> ! {
    eprintln!("usage: autumn-op [--manager addr] [--json] <command>");
    eprintln!();
    eprintln!("read / observability commands:");
    eprintln!("  list-nodes                   show every EN's auto-state + override");
    eprintln!("  extent-health [--node ID] [--all]");
    eprintln!("                               per-slot health (default: only unhealthy)");
    eprintln!("  list-ec-markers              ConvertToEc inflight markers + coord state");
    eprintln!("  recovery-stats               in-flight + per-source/target counters");
    eprintln!("  audit-log [--op N] [--node N] [--since S] [--until U] [--limit L]");
    eprintln!("                               query operator action history");
    eprintln!("  info [--part PID] [--detail] show cluster snapshot (F213; --detail = F203 partition load)");
    eprintln!(
        "  policy-candidates            advisory split/merge/gc/compact/ec candidates (F213)"
    );
    eprintln!("  cluster-version              persisted cluster_version + manager wire interval (R1)");
    eprintln!();
    eprintln!("rolling upgrade (R1, docs/rolling_upgrade_design.md):");
    eprintln!("  upgrade-version [--to N]     bump cluster_version (default current+1); run ONLY");
    eprintln!("                               after every member binary is upgraded; not rollbackable");
    eprintln!();
    eprintln!("node-lifecycle admin commands:");
    eprintln!("  fence-node <id> --reason \"...\" --by alice [--force]");
    eprintln!("  maintenance <id> --reason \"...\" --by alice [--expire UNIX_TS]");
    eprintln!("  unfence <id> --by alice");
    eprintln!("  remove <id> --by alice");
    eprintln!();
    eprintln!("cluster / partition admin commands (F213, moved from autumn-client):");
    eprintln!("  bootstrap [--replication 3+0] [--log-ec K+M] [--row-ec K+M] [--presplit 1:normal|N:hexstring|N:fuse]");
    eprintln!("  set-stream-ec --stream <ID> --ec K+M");
    eprintln!("  force-ec-convert --extent <EXTID>");
    eprintln!("  split <PARTID>");
    eprintln!("  merge <SURVIVOR_PARTID> <VICTIM_PARTID>");
    eprintln!("  compact <PARTID>");
    eprintln!("  gc [--ratio R] [--max-size B] [--stream-debt B] [--empty-only] <PARTID>");
    eprintln!("  forcegc <PARTID> <EXTID>...");
    // F214-C: `register-node` removed; `format` is the single per-EN setup.
    eprintln!("  format --listen <ADDR> --advertise <ADDR> <DIR>...");
    eprintln!("                               format dir(s), register node, stamp cluster_id");
    std::process::exit(1);
}

struct Args {
    manager: String,
    json: bool,
    transport: TransportKind,
    cmd: Command,
}

enum Command {
    // F211 read / observability
    ListNodes,
    // cluster-df: aggregate capacity summary (Ceph `ceph df` style)
    Df,
    // R1 rolling upgrade
    ClusterVersion,
    UpgradeVersion {
        to: Option<u32>,
    },
    ExtentHealth {
        node_filter: Vec<u64>,
        include_healthy: bool,
    },
    ListEcMarkers,
    RecoveryStats,
    AuditLog {
        op: u8,
        node_id: u64,
        since: i64,
        until: i64,
        limit: u32,
    },
    // F211 node-lifecycle admin
    Fence {
        node_id: u64,
        reason: String,
        by: String,
        force: bool,
    },
    Maintenance {
        node_id: u64,
        reason: String,
        by: String,
        expire: u64,
    },
    Unfence {
        node_id: u64,
        by: String,
    },
    Remove {
        node_id: u64,
        by: String,
    },
    // F213 read / observability (migrated from autumn-client)
    Info {
        part: Option<u64>,
        detail: bool,
    },
    PolicyCandidates,
    // F213 cluster / partition admin (migrated from autumn-client)
    Bootstrap {
        replication: String,
        presplit: String,
        log_ec: Option<(u32, u32)>,
        row_ec: Option<(u32, u32)>,
    },
    SetStreamEc {
        stream_id: u64,
        ec_data: u32,
        ec_parity: u32,
    },
    ForceEcConvert {
        extent_id: u64,
    },
    Split {
        part_id: u64,
    },
    Merge {
        survivor_part_id: u64,
        victim_part_id: u64,
    },
    Compact {
        part_id: u64,
    },
    Gc {
        part_id: u64,
        ratio: Option<f64>,
        max_size: Option<u64>,
        stream_debt: Option<u64>,
        empty_only: bool,
    },
    ForceGc {
        part_id: u64,
        extent_ids: Vec<u64>,
    },
    // F214-C: `register-node` merged into `format`. Variant kept so
    // the parser can route the legacy spelling to a migration stub
    // (in run()) instead of failing at parse with "unknown subcommand".
    RegisterNode,
    Format {
        listen: String,
        advertise: String,
        dirs: Vec<String>,
        /// F099-M: per-shard listener ports the EN binds. Empty = single-
        /// shard mode (manager routes everything to `advertise`). Multi-
        /// shard clusters (AUTUMN_EXTENT_SHARDS>1) MUST pass these so
        /// the manager can route extent ops to the owning shard by
        /// `extent_id % shard_count`.
        shard_ports: Vec<u16>,
    },
}

fn parse() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut manager = "127.0.0.1:9001".to_string();
    let mut json = false;
    let mut transport = TransportKind::Tcp;
    let mut i = 1usize;
    while i < raw.len() {
        match raw[i].as_str() {
            "--manager" => {
                i += 1;
                manager = raw.get(i).cloned().unwrap_or_else(|| usage());
                i += 1;
            }
            "--json" => {
                json = true;
                i += 1;
            }
            // Must match the manager's transport — a TCP autumn-op cannot
            // talk to a UCX-only manager (the connect/RPC just hangs).
            "--transport" => {
                i += 1;
                let raw_t = raw.get(i).cloned().unwrap_or_else(|| usage());
                transport = autumn_transport::parse_transport_flag(&raw_t).unwrap_or_else(|bad| {
                    eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                    usage()
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
    let sub = raw[i].as_str();
    i += 1;
    let cmd = match sub {
        // F211 read
        "list-nodes" => Command::ListNodes,
        "df" => Command::Df,
        // R1 rolling upgrade
        "cluster-version" => Command::ClusterVersion,
        "upgrade-version" => {
            let mut to: Option<u32> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--to" => {
                        i += 1;
                        if i >= raw.len() {
                            usage();
                        }
                        to = Some(raw[i].parse().unwrap_or_else(|_| usage()));
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::UpgradeVersion { to }
        }
        "extent-health" => {
            let mut node_filter: Vec<u64> = Vec::new();
            let mut include_healthy = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--node" => {
                        i += 1;
                        node_filter.push(raw[i].parse().unwrap_or_else(|_| usage()));
                        i += 1;
                    }
                    "--all" => {
                        include_healthy = true;
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::ExtentHealth {
                node_filter,
                include_healthy,
            }
        }
        "list-ec-markers" => Command::ListEcMarkers,
        "recovery-stats" => Command::RecoveryStats,
        "audit-log" => {
            let mut op = 0u8;
            let mut node_id = 0u64;
            let mut since = 0i64;
            let mut until = 0i64;
            let mut limit = 100u32;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--op" => {
                        i += 1;
                        op = raw[i].parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    "--node" => {
                        i += 1;
                        node_id = raw[i].parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    "--since" => {
                        i += 1;
                        since = raw[i].parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    "--until" => {
                        i += 1;
                        until = raw[i].parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    "--limit" => {
                        i += 1;
                        limit = raw[i].parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::AuditLog {
                op,
                node_id,
                since,
                until,
                limit,
            }
        }
        // F211 admin
        "fence-node" => {
            let node_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let (reason, by, force) = parse_admin_flags(&raw, &mut i);
            Command::Fence {
                node_id,
                reason,
                by,
                force,
            }
        }
        "maintenance" => {
            let node_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let mut reason = String::new();
            let mut by = String::new();
            let mut expire: u64 = 0;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--reason" => {
                        i += 1;
                        reason = raw[i].clone();
                        i += 1;
                    }
                    "--by" => {
                        i += 1;
                        by = raw[i].clone();
                        i += 1;
                    }
                    "--expire" => {
                        i += 1;
                        expire = raw[i].parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::Maintenance {
                node_id,
                reason,
                by,
                expire,
            }
        }
        "unfence" => {
            let node_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
            Command::Unfence { node_id, by }
        }
        "remove" => {
            let node_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
            Command::Remove { node_id, by }
        }
        // F213 read
        "info" => {
            let mut part: Option<u64> = None;
            let mut detail = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--part" => {
                        i += 1;
                        if i >= raw.len() {
                            eprintln!("--part requires a number");
                            usage();
                        }
                        part = Some(raw[i].parse().unwrap_or_else(|_| {
                            eprintln!("--part requires a number");
                            usage()
                        }));
                        i += 1;
                    }
                    "--detail" => {
                        detail = true;
                        i += 1;
                    }
                    other => {
                        eprintln!("unknown info flag: {other}");
                        usage();
                    }
                }
            }
            if detail && part.is_none() {
                eprintln!("--detail requires --part <PID>");
                usage();
            }
            Command::Info { part, detail }
        }
        "policy-candidates" | "policy_candidates" | "policy" => Command::PolicyCandidates,
        // F213 admin
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
                        i += 1;
                    }
                    "--presplit" => {
                        i += 1;
                        presplit = raw[i].clone();
                        i += 1;
                    }
                    "--log-ec" => {
                        i += 1;
                        log_ec = Some(parse_ec_flag(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--log-ec: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    "--row-ec" => {
                        i += 1;
                        row_ec = Some(parse_ec_flag(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--row-ec: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    _ => break,
                }
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
                        i += 1;
                    }
                    "--ec" => {
                        i += 1;
                        ec = Some(parse_ec_flag(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--ec: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    _ => break,
                }
            }
            let stream_id = stream_id.unwrap_or_else(|| {
                eprintln!("set-stream-ec requires --stream <ID>");
                std::process::exit(1);
            });
            let (ec_data, ec_parity) = ec.unwrap_or_else(|| {
                eprintln!("set-stream-ec requires --ec K+M");
                std::process::exit(1);
            });
            Command::SetStreamEc {
                stream_id,
                ec_data,
                ec_parity,
            }
        }
        "force-ec-convert" => {
            let mut extent_id: Option<u64> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--extent" => {
                        i += 1;
                        if i >= raw.len() {
                            eprintln!("--extent requires a number");
                            usage();
                        }
                        extent_id = Some(raw[i].parse().unwrap_or_else(|_| {
                            eprintln!("--extent requires a number");
                            usage()
                        }));
                        i += 1;
                    }
                    _ => break,
                }
            }
            let extent_id = extent_id.unwrap_or_else(|| {
                eprintln!("force-ec-convert requires --extent <EXTID>");
                std::process::exit(1);
            });
            Command::ForceEcConvert { extent_id }
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
                        i += 1;
                    }
                    "--max-size" => {
                        i += 1;
                        max_size = Some(parse_byte_size(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--max-size: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    "--stream-debt" => {
                        i += 1;
                        stream_debt = Some(parse_byte_size(&raw[i]).unwrap_or_else(|e| {
                            eprintln!("--stream-debt: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    "--empty-only" => {
                        empty_only = true;
                        i += 1;
                    }
                    _ => break,
                }
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
            // F214-C: route to the migration stub. Consume any remaining
            // arguments so the parser doesn't misinterpret them.
            while i < raw.len() {
                i += 1;
            }
            Command::RegisterNode
        }
        "format" => {
            let mut listen = String::new();
            let mut advertise = String::new();
            let mut dirs = Vec::new();
            let mut shard_ports: Vec<u16> = Vec::new();
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
                    "--shard-ports" => {
                        // Comma-separated u16 list. Required for
                        // F099-M multi-shard ENs so the manager can
                        // route per-extent ops to the owning shard.
                        i += 1;
                        for part in raw[i].split(',') {
                            let p = part.trim();
                            if p.is_empty() {
                                continue;
                            }
                            let port: u16 = p.parse().expect("--shard-ports entries must be u16");
                            shard_ports.push(port);
                        }
                    }
                    _ => dirs.push(raw[i].clone()),
                }
                i += 1;
            }
            if listen.is_empty() || advertise.is_empty() || dirs.is_empty() {
                eprintln!(
                    "format requires --listen <ADDR> --advertise <ADDR> [--shard-ports P1,P2,...] <DIR>..."
                );
                std::process::exit(1);
            }
            Command::Format {
                listen,
                advertise,
                dirs,
                shard_ports,
            }
        }
        _ => usage(),
    };
    Args {
        manager,
        json,
        transport,
        cmd,
    }
}

fn parse_admin_flags(raw: &[String], i: &mut usize) -> (String, String, bool) {
    let mut reason = String::new();
    let mut by = String::new();
    let mut force = false;
    while *i < raw.len() {
        match raw[*i].as_str() {
            "--reason" => {
                *i += 1;
                reason = raw[*i].clone();
                *i += 1;
            }
            "--by" => {
                *i += 1;
                by = raw[*i].clone();
                *i += 1;
            }
            "--force" => {
                force = true;
                *i += 1;
            }
            _ => break,
        }
    }
    (reason, by, force)
}

// ---------------------------------------------------------------------------
// F213 helpers (migrated from autumn_client.rs)
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

/// Split keys aimed at the autumn-fuse keyspace
/// (`crates/fuse/src/key.rs` — `[0x01]inode_meta`, `[0x02]dirent`,
/// `[0x03]file_extent`, `[0x04]super`). The bulk of fuse data is
/// the `[0x03][ino BE][logical_off BE]` file extents — they outsize
/// the other prefixes by orders of magnitude on real model-serving
/// workloads (sglang / vllm checkpoints).
///
/// We split on the BOTTOM byte of `ino` (byte index 8 of the key —
/// `[0x03][7 high zero bytes][low byte]`). Rationale:
/// - Sequential inode allocation steps through low bytes
///   `0x00, 0x01, ..., 0xFF, 0x00, ...` → uniform round-robin
///   across N partitions for any cluster size.
/// - High bytes only change once inode count exceeds 256 / 65k /
///   16M — they're effectively zero for the lifetime of a
///   practical fuse fileset, so splitting on them puts everything
///   in partition 0.
/// - A single file's extents all share one inode → land in one
///   partition; concurrent reads of DIFFERENT files distribute,
///   which is the read-scatter shape sglang wants.
///
/// Partition 0 also absorbs `[0x01]` inode-meta + `[0x02]` dirent
/// (both prefix-sort before `[0x03]`); partition N-1 absorbs
/// `[0x04]` superblock (sorts after every `[0x03]` key) and any
/// non-fuse prefix ≥ 0x05. These are tiny next to file data.
fn fuse_split_ranges(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    if n <= 1 {
        return vec![(vec![], vec![])];
    }
    let n = n.min(256); // 1 byte → at most 256 buckets
    let stride = 256usize / n;
    let mut split_points: Vec<Vec<u8>> = Vec::with_capacity(n - 1);
    for i in 1..n {
        let byte = (i * stride) as u8; // 0x20, 0x40, ... for N=8
        split_points.push(vec![0x03, 0, 0, 0, 0, 0, 0, 0, byte]);
    }
    let mut ranges = Vec::with_capacity(n);
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

fn parse_ec_flag(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = s.splitn(2, '+').collect();
    if parts.len() != 2 {
        bail!("EC shape must be K+M (e.g. '3+1'), got '{s}'");
    }
    let k: u32 = parts[0]
        .parse()
        .with_context(|| format!("parse K in EC '{s}'"))?;
    let m: u32 = parts[1]
        .parse()
        .with_context(|| format!("parse M in EC '{s}'"))?;
    if k == 0 || m == 0 {
        bail!("EC K and M must both be >= 1, got '{s}'");
    }
    Ok((k, m))
}

/// F191: derive default control-plane addr from data-plane host:port by
/// offsetting the port by +1000. Returns empty on parse failure — the
/// manager treats empty `control_address` as "fall back to addr".
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
        std::fs::create_dir_all(&subdir).with_context(|| format!("create hash subdir {subdir}"))?;
    }
    let disk_uuid = uuid::Uuid::new_v4().to_string();
    let marker_path = format!("{}/{}", dir, disk_uuid);
    std::fs::File::create(&marker_path)
        .with_context(|| format!("create UUID marker {marker_path}"))?;
    Ok(disk_uuid)
}

/// F214-C: fetch the manager's cluster_id. Retries on `CODE_NOT_LEADER`
/// (same pattern as other admin RPCs in this binary). Returns an error
/// if the cluster has never been imprinted (manager replied with
/// `CODE_ERROR` "not yet bootstrapped"), which only happens before the
/// first leader election against a fresh etcd.
async fn fetch_cluster_id(client: &ClusterClient) -> Result<String> {
    let req_bytes = rkyv_encode(&GetClusterIdReq {});
    let mut attempt = 0u32;
    loop {
        let resp_bytes = client
            .mgr_call(MSG_GET_CLUSTER_ID, req_bytes.clone())
            .await
            .context("get cluster_id")?;
        let resp: GetClusterIdResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
        if resp.code == CODE_OK {
            if resp.cluster_id.is_empty() {
                bail!("manager replied OK with empty cluster_id");
            }
            return Ok(resp.cluster_id);
        }
        if resp.code == CODE_NOT_LEADER && attempt < 60 {
            attempt += 1;
            compio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        bail!("get cluster_id failed: code={} {}", resp.code, resp.message);
    }
}

/// F214-C: probe a data dir for prior `format` state. The dedicated
/// `cluster_id` + `disk_uuid` sentinel files (added in F214-C) are the
/// canonical "already formatted" signal. Returns `(cluster_id, disk_uuid)`
/// when both files are present and readable; `None` for a fresh dir.
fn read_existing_format(dir: &str) -> Result<Option<(String, String)>> {
    let cluster_path = format!("{dir}/cluster_id");
    if !std::path::Path::new(&cluster_path).exists() {
        return Ok(None);
    }
    let cid = std::fs::read_to_string(&cluster_path)
        .with_context(|| format!("read cluster_id in {dir}"))?
        .trim()
        .to_string();
    let uuid_path = format!("{dir}/disk_uuid");
    if !std::path::Path::new(&uuid_path).exists() {
        // Defensive: cluster_id present but disk_uuid missing means a
        // partially-formatted dir (interrupted previous run, or a
        // pre-F214 dir that's been hand-patched). Treat as fresh and
        // let the new format overwrite — the operator hit this path
        // intentionally by running format on this dir.
        return Ok(None);
    }
    let did = std::fs::read_to_string(&uuid_path)
        .with_context(|| format!("read disk_uuid in {dir}"))?
        .trim()
        .to_string();
    Ok(Some((cid, did)))
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
    s.extent_ids
        .iter()
        .filter_map(|eid| extent_map.get(eid))
        .map(|e| e.sealed_length)
        .sum()
}

// ---------------------------------------------------------------------------
// JSON view types
// ---------------------------------------------------------------------------

fn auto_state_str(b: u8) -> &'static str {
    match b {
        NODE_AUTO_STATE_ONLINE => "Online",
        NODE_AUTO_STATE_SUSPECTED => "Suspected",
        // F214-B: registered, never verified alive. Distinct from
        // Suspected — Suspended means "was alive, now flaky".
        NODE_AUTO_STATE_SUSPEND => "Suspend",
        _ => "Unknown",
    }
}

fn override_str(b: u8) -> &'static str {
    match b {
        NODE_OVERRIDE_NONE => "-",
        NODE_OVERRIDE_FENCED => "Fenced",
        NODE_OVERRIDE_MAINTENANCE => "Maintenance",
        _ => "Unknown",
    }
}

fn op_name(b: u8) -> &'static str {
    match b {
        AUDIT_OP_FENCE_NODE => "fence_node",
        AUDIT_OP_SET_NODE_MAINTENANCE => "maintenance",
        AUDIT_OP_CLEAR_NODE_OVERRIDE => "clear_override",
        AUDIT_OP_REMOVE_NODE => "remove_node",
        AUDIT_OP_FORCE_EC_CONVERT => "force_ec_convert",
        AUDIT_OP_FORCE_ABANDON_EC_MARKER => "force_abandon_ec_marker",
        _ => "unknown",
    }
}

#[derive(Serialize)]
struct JsonNode {
    node_id: u64,
    address: String,
    auto_state: String,
    last_heartbeat_secs_ago: u64,
    suspected_age_secs: u64,
    override_kind: String,
    override_reason: String,
    override_set_by: String,
    override_set_at: i64,
    override_expire_at: u64,
}

#[derive(Serialize)]
struct JsonAudit {
    op: String,
    node_id: u64,
    extent_id: u64,
    by: String,
    reason: String,
    result_code: u8,
    result_message: String,
    ts_ns: u64,
}

// ---------------------------------------------------------------------------
// F213 `info` JSON output types (migrated from autumn_client.rs)
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

// ---------------------------------------------------------------------------
// run()
// ---------------------------------------------------------------------------

async fn run(args: Args) -> Result<()> {
    // F214-C: the register-node migration stub must print BEFORE we
    // try to connect to the manager — otherwise users hit "manager
    // unreachable" instead of the actionable migration message.
    if matches!(args.cmd, Command::RegisterNode) {
        eprintln!(
            "register-node has merged into 'autumn-op format'.\n\
             Run: autumn-op --manager <ADDR> format --listen :<PORT> \\\n\
                  --advertise <HOST:PORT> <DIR> [<DIR>...]\n\
             'format' fetches the cluster_id, registers the node, \
             and stamps every data dir in one step."
        );
        std::process::exit(1);
    }
    // Select the process-global transport before connecting. Without this an
    // autumn-op invoked against a UCX manager would default to TCP and hang.
    let _ = autumn_transport::init_with(args.transport);
    let client = ClusterClient::connect(&args.manager).await?;
    match args.cmd {
        // ---------------- F211 read ----------------
        Command::ClusterVersion => {
            let bytes = client
                .mgr_call(
                    MSG_GET_CLUSTER_VERSION,
                    rkyv_encode(&GetClusterVersionReq {}),
                )
                .await?;
            let resp: GetClusterVersionResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("cluster-version: {}", resp.message);
            }
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "cluster_version": resp.cluster_version,
                        "manager_wire_version_min": resp.wire_version_min,
                        "manager_wire_version_max": resp.wire_version_max,
                        "op_wire_version_min": autumn_rpc::WIRE_VERSION_MIN,
                        "op_wire_version_max": autumn_rpc::WIRE_VERSION_MAX,
                    }))?
                );
            } else {
                println!("cluster_version: {}", resp.cluster_version);
                println!(
                    "manager wire interval: [{}, {}]",
                    resp.wire_version_min, resp.wire_version_max
                );
                println!(
                    "this autumn-op binary:  [{}, {}]",
                    autumn_rpc::WIRE_VERSION_MIN,
                    autumn_rpc::WIRE_VERSION_MAX
                );
                if resp.cluster_version < resp.wire_version_max {
                    println!(
                        "NOTE: manager binaries support up to v{} — `upgrade-version` can bump \
once EVERY member runs the new binary",
                        resp.wire_version_max
                    );
                }
            }
        }
        Command::UpgradeVersion { to } => {
            // Resolve the default target (current+1) from a fresh read so
            // the printed intent matches what the manager will validate.
            let bytes = client
                .mgr_call(
                    MSG_GET_CLUSTER_VERSION,
                    rkyv_encode(&GetClusterVersionReq {}),
                )
                .await?;
            let cur: GetClusterVersionResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if cur.code != CODE_OK {
                bail!("upgrade-version: read current failed: {}", cur.message);
            }
            let target = to.unwrap_or(cur.cluster_version + 1);
            let bytes = client
                .mgr_call(
                    MSG_BUMP_CLUSTER_VERSION,
                    rkyv_encode(&BumpClusterVersionReq { to: target }),
                )
                .await?;
            let resp: BumpClusterVersionResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!(
                    "upgrade-version to {} REFUSED (cluster_version stays {}): {}",
                    target,
                    resp.cluster_version,
                    resp.message
                );
            }
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "cluster_version": resp.cluster_version,
                    }))?
                );
            } else {
                println!(
                    "cluster_version bumped: {} -> {} — rollback to older binaries is no \
longer safe (new formats may now be emitted/persisted)",
                    cur.cluster_version, resp.cluster_version
                );
            }
        }
        Command::ListNodes => {
            let bytes = client
                .mgr_call(MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {}))
                .await?;
            let resp: ListNodeStatesResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("list-nodes: {}", resp.message);
            }
            if args.json {
                let out: Vec<JsonNode> = resp
                    .nodes
                    .into_iter()
                    .map(|n| JsonNode {
                        node_id: n.node_id,
                        address: n.address,
                        auto_state: auto_state_str(n.auto_state).to_string(),
                        last_heartbeat_secs_ago: n.last_heartbeat_secs_ago,
                        suspected_age_secs: n.suspected_age_secs,
                        override_kind: override_str(n.override_kind).to_string(),
                        override_reason: n.override_reason,
                        override_set_by: n.override_set_by,
                        override_set_at: n.override_set_at,
                        override_expire_at: n.override_expire_at,
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!(
                    "{:<6} {:<24} {:<10} {:<8} {:<8} {:<12} REASON",
                    "ID", "ADDRESS", "AUTO", "HB_AGO", "SUSP_AGE", "OVERRIDE"
                );
                for n in resp.nodes {
                    let hb = if n.last_heartbeat_secs_ago == u64::MAX {
                        "never".to_string()
                    } else {
                        format!("{}s", n.last_heartbeat_secs_ago)
                    };
                    println!(
                        "{:<6} {:<24} {:<10} {:<8} {:<8} {:<12} {}",
                        n.node_id,
                        n.address,
                        auto_state_str(n.auto_state),
                        hb,
                        n.suspected_age_secs,
                        override_str(n.override_kind),
                        n.override_reason,
                    );
                }
            }
        }
        Command::Df => {
            let r = client.cluster_df().await?;
            let raw_used = r.raw_total.saturating_sub(r.raw_free);
            // Empirical amplification (physical_used / logical_stored): the
            // REAL current cold/hot mix, more useful than the theoretical
            // [1.25, 3] bound. n/a when nothing is stored yet.
            let amp = if r.logical_stored > 0 {
                r.physical_used as f64 / r.logical_stored as f64
            } else {
                0.0
            };
            // Writable logical estimate is a RANGE under EC: best EC shape is
            // K = min(4, node_count-1) data shards + 1 parity → factor
            // (K+1)/K; worst is 3-replica. Point estimate uses the empirical
            // amplification (falls back to the conservative /3).
            let k = if r.node_count >= 2 {
                (r.node_count - 1).min(4)
            } else {
                0
            };
            let best_factor = if k >= 1 {
                (k as f64 + 1.0) / k as f64
            } else {
                3.0
            };
            let writable_low = r.raw_free / 3; // conservative: 3-replica
            let writable_high = (r.raw_free as f64 / best_factor) as u64;
            let writable_est = if amp > 0.0 {
                (r.raw_free as f64 / amp) as u64
            } else {
                writable_low
            };
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let snap_age = now_ms.saturating_sub(r.last_update_ms) / 1000;
            let logical_age = now_ms.saturating_sub(r.logical_last_update_ms) / 1000;

            if args.json {
                let per_node: Vec<serde_json::Value> = r
                    .per_node
                    .iter()
                    .map(|n| {
                        serde_json::json!({
                            "node_id": n.node_id,
                            "total": n.total,
                            "free": n.free,
                            "extent_bytes": n.extent_bytes,
                            "online": n.online,
                        })
                    })
                    .collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "raw_total": r.raw_total,
                        "raw_used": raw_used,
                        "raw_free": r.raw_free,
                        "physical_used": r.physical_used,
                        "logical_stored_sealed": r.logical_stored,
                        "amplification": amp,
                        "writable_est": writable_est,
                        "writable_low_3x": writable_low,
                        "writable_high_ec": writable_high,
                        "best_ec_data_shards": k,
                        "node_count_online": r.node_count,
                        "snapshot_age_secs": snap_age,
                        "logical_scan_age_secs": logical_age,
                        "per_node": per_node,
                    }))?
                );
            } else {
                let amp_str = if amp > 0.0 {
                    format!("{amp:.2}x")
                } else {
                    "n/a".to_string()
                };
                println!("=== Cluster df ===");
                println!(
                    "RAW:     total={:<10} used={:<10} avail={}",
                    human_size(r.raw_total),
                    human_size(raw_used),
                    human_size(r.raw_free),
                );
                println!(
                    "AUTUMN:  phys_used={:<10} stored(sealed)={:<10} amplification={}",
                    human_size(r.physical_used),
                    human_size(r.logical_stored),
                    amp_str,
                );
                println!(
                    "WRITABLE(est): {}   range [{} .. {}]  (3-replica .. EC {}+1)",
                    human_size(writable_est),
                    human_size(writable_low),
                    human_size(writable_high),
                    k,
                );
                println!(
                    "NODES: {} online   (snapshot {}s ago; logical scan {}s ago)",
                    r.node_count, snap_age, logical_age,
                );
                println!(
                    "  {:<6} {:<10} {:<10} {:<10} ONLINE",
                    "ID", "TOTAL", "FREE", "PHYS_USED"
                );
                for n in &r.per_node {
                    println!(
                        "  {:<6} {:<10} {:<10} {:<10} {}",
                        n.node_id,
                        human_size(n.total),
                        human_size(n.free),
                        human_size(n.extent_bytes),
                        if n.online { "yes" } else { "no" },
                    );
                }
            }
        }
        Command::ExtentHealth {
            node_filter,
            include_healthy,
        } => {
            let req = ExtentHealthReq {
                node_id_filter: node_filter,
                include_healthy,
            };
            let bytes = client
                .mgr_call(MSG_EXTENT_HEALTH_REPORT, rkyv_encode(&req))
                .await?;
            let resp: ExtentHealthResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("extent-health: {}", resp.message);
            }
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &resp
                            .extents
                            .into_iter()
                            .map(|e| {
                                serde_json::json!({
                                    "extent_id": e.extent_id,
                                    "eversion": e.eversion,
                                    "sealed_length": e.sealed_length,
                                    "ec_converted": e.ec_converted,
                                    "unhealthy": e.unhealthy,
                                    "slots": e.slots.into_iter().map(|s| serde_json::json!({
                                        "slot": s.slot_index,
                                        "node_id": s.node_id,
                                        "avali": s.avali,
                                        "auto_state": auto_state_str(s.auto_state),
                                        "override": override_str(s.override_kind),
                                    })).collect::<Vec<_>>(),
                                })
                            })
                            .collect::<Vec<_>>()
                    )?
                );
            } else {
                if resp.extents.is_empty() {
                    println!("(no extents match filter)");
                }
                for e in resp.extents {
                    println!(
                        "extent {}  eversion={} sealed={} ec={} unhealthy={}",
                        e.extent_id, e.eversion, e.sealed_length, e.ec_converted, e.unhealthy
                    );
                    for s in e.slots {
                        println!(
                            "  slot[{}] node={:<4} avali={:<5} auto={} override={}",
                            s.slot_index,
                            s.node_id,
                            s.avali,
                            auto_state_str(s.auto_state),
                            override_str(s.override_kind),
                        );
                    }
                }
            }
        }
        Command::ListEcMarkers => {
            let bytes = client
                .mgr_call(
                    MSG_LIST_EC_INFLIGHT_MARKERS,
                    rkyv_encode(&ListEcInflightMarkersReq {}),
                )
                .await?;
            let resp: ListEcInflightMarkersResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("list-ec-markers: {}", resp.message);
            }
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &resp
                            .markers
                            .into_iter()
                            .map(|m| {
                                serde_json::json!({
                                    "extent_id": m.extent_id,
                                    "coord_node_id": m.coord_node_id,
                                    "coord_auto_state": auto_state_str(m.coord_auto_state),
                                    "coord_override": override_str(m.coord_override_kind),
                                    "target_nodes": m.target_nodes,
                                    "data_shards": m.data_shards,
                                    "new_eversion": m.new_eversion,
                                    "started_at": m.started_at,
                                    "age_secs": m.age_secs,
                                })
                            })
                            .collect::<Vec<_>>()
                    )?
                );
            } else {
                if resp.markers.is_empty() {
                    println!("(no inflight EC markers)");
                }
                for m in resp.markers {
                    println!(
                        "ext={} coord={} ({}/{}) targets={:?} K={} new_ev={} age={}s",
                        m.extent_id,
                        m.coord_node_id,
                        auto_state_str(m.coord_auto_state),
                        override_str(m.coord_override_kind),
                        m.target_nodes,
                        m.data_shards,
                        m.new_eversion,
                        m.age_secs,
                    );
                }
            }
        }
        Command::RecoveryStats => {
            let bytes = client
                .mgr_call(MSG_RECOVERY_STATS, rkyv_encode(&RecoveryStatsReq {}))
                .await?;
            let resp: RecoveryStatsResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("recovery-stats: {}", resp.message);
            }
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "global_inflight": resp.global_inflight,
                        "max_global": resp.max_global,
                        "max_per_source": resp.max_per_source,
                        "max_per_target": resp.max_per_target,
                        "per_source": resp.per_source,
                        "per_target": resp.per_target,
                        "backoff_entries": resp.backoff_entries,
                        "backoff": resp.backoff.iter().map(|b| serde_json::json!({
                            "extent_id": b.extent_id,
                            "slot": b.slot,
                            "consecutive_failures": b.consecutive_failures,
                            "last_attempt_at": b.last_attempt_at,
                            "next_retry_at": b.next_retry_at,
                            "reason": b.reason,
                        })).collect::<Vec<_>>(),
                    }))?
                );
            } else {
                println!(
                    "global: {}/{}  per_source<={}  per_target<={}  backoff_entries={}",
                    resp.global_inflight,
                    resp.max_global,
                    resp.max_per_source,
                    resp.max_per_target,
                    resp.backoff_entries,
                );
                if !resp.per_source.is_empty() {
                    println!("per-source:");
                    for (id, c) in resp.per_source {
                        println!("  node {:<4} {}", id, c);
                    }
                }
                if !resp.per_target.is_empty() {
                    println!("per-target:");
                    for (id, c) in resp.per_target {
                        println!("  node {:<4} {}", id, c);
                    }
                }
                if !resp.backoff.is_empty() {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    println!("backoff:");
                    println!(
                        "  {:<10} {:<4} {:<6} {:<10} reason",
                        "extent", "slot", "fails", "retry_in"
                    );
                    for b in &resp.backoff {
                        let retry_in = b.next_retry_at - now;
                        let retry_str = if retry_in <= 0 {
                            "now".to_string()
                        } else {
                            format!("{retry_in}s")
                        };
                        println!(
                            "  {:<10} {:<4} {:<6} {:<10} {}",
                            b.extent_id, b.slot, b.consecutive_failures, retry_str, b.reason
                        );
                    }
                }
            }
        }
        Command::AuditLog {
            op,
            node_id,
            since,
            until,
            limit,
        } => {
            let req = QueryAuditLogReq {
                op_filter: op,
                node_id_filter: node_id,
                since_ts_s: since,
                until_ts_s: until,
                limit,
            };
            let bytes = client
                .mgr_call(MSG_QUERY_AUDIT_LOG, rkyv_encode(&req))
                .await?;
            let resp: QueryAuditLogResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("audit-log: {}", resp.message);
            }
            if args.json {
                let out: Vec<JsonAudit> = resp
                    .entries
                    .into_iter()
                    .map(|e| JsonAudit {
                        op: op_name(e.op).to_string(),
                        node_id: e.node_id,
                        extent_id: e.extent_id,
                        by: e.by,
                        reason: e.reason,
                        result_code: e.result_code,
                        result_message: e.result_message,
                        ts_ns: e.ts_ns,
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                if resp.entries.is_empty() {
                    println!("(no audit entries match filter)");
                }
                for e in resp.entries {
                    println!(
                        "{}  op={:<22} node={:<4} ext={:<8} by={:<12} code={} reason={}",
                        e.ts_ns,
                        op_name(e.op),
                        e.node_id,
                        e.extent_id,
                        e.by,
                        e.result_code,
                        e.reason,
                    );
                    if !e.result_message.is_empty() {
                        println!("    => {}", e.result_message);
                    }
                }
            }
        }
        // ---------------- F211 admin ----------------
        Command::Fence {
            node_id,
            reason,
            by,
            force,
        } => {
            if reason.is_empty() || by.is_empty() {
                bail!("--reason and --by are required");
            }
            let req = FenceNodeReq {
                node_id,
                reason,
                set_by: by,
                force,
            };
            let bytes = client.mgr_call(MSG_FENCE_NODE, rkyv_encode(&req)).await?;
            let resp: CodeResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            print_code(args.json, "fence-node", &resp);
        }
        Command::Maintenance {
            node_id,
            reason,
            by,
            expire,
        } => {
            if by.is_empty() {
                bail!("--by is required");
            }
            let req = SetNodeMaintenanceReq {
                node_id,
                reason,
                set_by: by,
                expire_at: expire,
            };
            let bytes = client
                .mgr_call(MSG_SET_NODE_MAINTENANCE, rkyv_encode(&req))
                .await?;
            let resp: CodeResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            print_code(args.json, "maintenance", &resp);
        }
        Command::Unfence { node_id, by } => {
            if by.is_empty() {
                bail!("--by is required");
            }
            let req = ClearNodeOverrideReq {
                node_id,
                set_by: by,
            };
            let bytes = client
                .mgr_call(MSG_CLEAR_NODE_OVERRIDE, rkyv_encode(&req))
                .await?;
            let resp: CodeResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            print_code(args.json, "unfence", &resp);
        }
        Command::Remove { node_id, by } => {
            if by.is_empty() {
                bail!("--by is required");
            }
            let req = RemoveNodeReq {
                node_id,
                set_by: by,
            };
            let bytes = client.mgr_call(MSG_REMOVE_NODE, rkyv_encode(&req)).await?;
            let resp: RemoveNodeResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "code": resp.code,
                        "message": resp.message,
                        "blocking_extent_ids": resp.blocking_extent_ids,
                        "blocking_marker_extent_ids": resp.blocking_marker_extent_ids,
                    }))?
                );
            } else if resp.code == CODE_OK {
                println!("remove: ok");
            } else {
                println!("remove: code={} {}", resp.code, resp.message);
                if !resp.blocking_extent_ids.is_empty() {
                    println!("  blocking extents: {:?}", resp.blocking_extent_ids);
                }
                if !resp.blocking_marker_extent_ids.is_empty() {
                    println!("  blocking markers: {:?}", resp.blocking_marker_extent_ids);
                }
            }
            if resp.code != CODE_OK {
                std::process::exit(2);
            }
        }
        // ---------------- F213 read ----------------
        Command::PolicyCandidates => {
            let cands = client
                .policy_candidates()
                .await
                .map_err(|e| anyhow!("policy_candidates: {e}"))?;
            if args.json {
                let out: Vec<_> = cands
                    .iter()
                    .map(|c| {
                        let kind = match c.kind {
                            POLICY_KIND_SPLIT => "split",
                            POLICY_KIND_MERGE => "merge",
                            POLICY_KIND_GC => "gc",
                            POLICY_KIND_MAJOR_COMPACT => "major",
                            POLICY_KIND_HOT_COLD => "hotcold",
                            POLICY_KIND_MINOR_COMPACT => "minor",
                            POLICY_KIND_EC => "ec",
                            _ => "?",
                        };
                        serde_json::json!({
                            "kind": kind,
                            "primary_part_id": c.primary_part_id,
                            "secondary_part_id": c.secondary_part_id,
                            "reason": c.reason,
                            "size_bytes": c.size_bytes,
                            "req_per_sec": c.req_per_sec,
                            "imm_full_per_sec": c.imm_full_per_sec,
                            "same_ps": c.same_ps,
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else if cands.is_empty() {
                println!("(no candidates)");
            } else {
                println!(
                    "{:<7} {:<10} {:<10} {:<46} {:<10} {:<8} {:<6} {:<5}",
                    "KIND", "PRIMARY", "SECONDARY", "REASON", "SIZE", "QPS", "IMM/s", "FEAS"
                );
                for c in cands {
                    let kind = match c.kind {
                        POLICY_KIND_SPLIT => "split",
                        POLICY_KIND_MERGE => "merge",
                        POLICY_KIND_GC => "gc",
                        POLICY_KIND_MAJOR_COMPACT => "major",
                        POLICY_KIND_HOT_COLD => "hotcold",
                        POLICY_KIND_MINOR_COMPACT => "minor",
                        POLICY_KIND_EC => "ec",
                        _ => "?",
                    };
                    let feas = match c.kind {
                        POLICY_KIND_GC
                        | POLICY_KIND_MAJOR_COMPACT
                        | POLICY_KIND_MINOR_COMPACT
                        | POLICY_KIND_EC
                        | POLICY_KIND_HOT_COLD => "n/a",
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
        Command::Info { part, detail } => {
            run_info(&client, args.json, part, detail).await?;
        }
        // ---------------- F213 admin ----------------
        Command::Bootstrap {
            replication,
            presplit,
            log_ec,
            row_ec,
        } => {
            run_bootstrap(&client, args.json, &replication, &presplit, log_ec, row_ec).await?;
        }
        Command::SetStreamEc {
            stream_id,
            ec_data,
            ec_parity,
        } => {
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
                    if args.json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "code": resp.code,
                                "stream_id": stream_id,
                                "ec_data": ec_data,
                                "ec_parity": ec_parity,
                            }))?
                        );
                    } else {
                        println!(
                            "stream {} EC updated to {}+{}; conversion will run on next manager tick (~5s)",
                            stream_id, ec_data, ec_parity
                        );
                    }
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
            let req = rkyv_encode(&ForceEcConvertReq { extent_id });
            let resp_bytes = client
                .mgr_call(MSG_FORCE_EC_CONVERT, req)
                .await
                .context("force-ec-convert")?;
            let resp: ForceEcConvertResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
            if resp.code != CODE_OK {
                bail!("force-ec-convert: code={} {}", resp.code, resp.message);
            }
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "code": resp.code,
                        "extent_id": extent_id,
                        "message": resp.message,
                    }))?
                );
            } else {
                println!("{}", resp.message);
            }
        }
        Command::Split { part_id } => {
            client
                .split(part_id)
                .await
                .map_err(|e| anyhow!("split: {e}"))?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "ok": true,
                        "part_id": part_id,
                    }))?
                );
            } else {
                println!("split ok");
            }
        }
        Command::Merge {
            survivor_part_id,
            victim_part_id,
        } => {
            eprintln!(
                "F183: stop writes to partitions {survivor_part_id} and {victim_part_id} \
                 before continuing. The CLI will FLUSH both, then issue the manager merge. \
                 The survivor's PS picks up the wider range on the next region_sync (~2 s)."
            );
            client
                .merge_partitions(survivor_part_id, victim_part_id)
                .await
                .map_err(|e| anyhow!("merge: {e}"))?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "ok": true,
                        "survivor": survivor_part_id,
                        "victim": victim_part_id,
                    }))?
                );
            } else {
                println!("merge ok: partition {victim_part_id} merged into {survivor_part_id}");
            }
        }
        Command::Compact { part_id } => {
            client
                .compact(part_id)
                .await
                .map_err(|e| anyhow!("compact: {e}"))?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "ok": true,
                        "part_id": part_id,
                    }))?
                );
            } else {
                println!("compact triggered for partition {part_id}");
            }
        }
        Command::Gc {
            part_id,
            ratio,
            max_size,
            stream_debt,
            empty_only,
        } => {
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
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "ok": true,
                        "part_id": part_id,
                        "ratio": params.ratio,
                        "max_size": params.max_size,
                        "stream_debt": params.stream_debt,
                        "empty_only": params.empty_only,
                    }))?
                );
            } else {
                println!(
                    "gc triggered for partition {part_id} (ratio={:?} max_size={:?} stream_debt={:?} empty_only={})",
                    params.ratio, params.max_size, params.stream_debt, params.empty_only
                );
            }
        }
        Command::ForceGc {
            part_id,
            extent_ids,
        } => {
            client
                .force_gc(part_id, extent_ids.clone())
                .await
                .map_err(|e| anyhow!("forcegc: {e}"))?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "ok": true,
                        "part_id": part_id,
                        "extents": extent_ids,
                    }))?
                );
            } else {
                println!("forcegc triggered for partition {part_id}, extents={extent_ids:?}");
            }
        }
        Command::RegisterNode => {
            // Already handled by the pre-connect stub above.
            unreachable!("Command::RegisterNode handled before connect");
        }
        Command::Format {
            listen,
            advertise,
            dirs,
            shard_ports,
        } => {
            // F214-C: fetch the manager's cluster_id BEFORE touching
            // any disk. Failure here means the manager is not yet
            // leader (retries internally) or has never bootstrapped
            // (fatal — operator must start the manager first).
            let cluster_id = fetch_cluster_id(&client).await?;

            // For each dir, decide whether to fresh-format or reuse
            // existing. Refuse on cluster_id mismatch — that's the
            // "wrong cluster" diagnostic.
            let mut disk_uuids = Vec::new();
            let mut freshly_formatted: Vec<bool> = Vec::with_capacity(dirs.len());
            for dir in &dirs {
                std::fs::create_dir_all(dir).with_context(|| format!("create dir {dir}"))?;
                match read_existing_format(dir)? {
                    Some((existing_cid, existing_did)) if existing_cid == cluster_id => {
                        // Idempotent path — already formatted for this
                        // cluster. Reuse the disk_uuid so the manager's
                        // re-register branch returns the existing
                        // disk_id without allocating a fresh one.
                        if !args.json {
                            println!(
                                "{dir}: already formatted (cluster_id matches), reusing disk_uuid={existing_did}"
                            );
                        }
                        disk_uuids.push(existing_did);
                        freshly_formatted.push(false);
                    }
                    Some((existing_cid, _)) => {
                        // Different cluster — refuse rather than risk
                        // joining a disk to the wrong cluster.
                        bail!(
                            "{dir} is already formatted for cluster {existing_cid}, \
                             but the manager at {} reports cluster {}. \
                             Wipe the dir or point at the original cluster.",
                            args.manager,
                            cluster_id
                        );
                    }
                    None => {
                        // Fresh dir — full format: 256 hash subdirs +
                        // fresh disk_uuid.
                        let uuid = format_disk(dir)?;
                        if !args.json {
                            println!("formatted {dir}: disk_uuid={uuid}");
                        }
                        disk_uuids.push(uuid);
                        freshly_formatted.push(true);
                    }
                }
            }

            // F214-C: register against the manager. Re-register branch
            // (existing address known) returns the existing node_id +
            // matching disk_ids, so idempotency holds end-to-end.
            // F099-M: pass `shard_ports` so the manager routes
            // per-extent operations to the owning shard via
            // `extent_id % shard_count`. Empty vec = single-shard EN
            // (manager routes everything to `advertise`).
            // F191 control-plane port. Under UCX a second ucp_listener on the
            // same RoCE device can't bind ("Device is busy"), so the extent
            // node serves control RPCs on the data listener instead. Register
            // an empty control_address so the manager's DF falls back to the
            // data address (manager treats "" as "use addr"). TCP keeps the
            // separate control port for HoL isolation.
            let control_address = if args.transport == TransportKind::Ucx {
                String::new()
            } else {
                derive_control_address(&advertise)
            };
            let resp_bytes = client
                .mgr_call(
                    MSG_REGISTER_NODE,
                    rkyv_encode(&RegisterNodeReq {
                        addr: advertise.clone(),
                        disk_uuids: disk_uuids.clone(),
                        shard_ports: shard_ports.clone(),
                        control_address,
                    }),
                )
                .await
                .context("register node")?;
            let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;

            let node_id = resp.node_id;
            let mut disk_assignments: Vec<(String, String, u64)> = Vec::new();
            for (dir, disk_uuid) in dirs.iter().zip(disk_uuids.iter()) {
                let disk_id = resp
                    .disk_uuids
                    .iter()
                    .find(|(u, _)| u == disk_uuid)
                    .map(|(_, id)| *id)
                    .unwrap_or(0);
                // F214-C: cluster_id + disk_uuid sentinel files. The
                // extent-node binary's startup check reads cluster_id
                // and cross-checks against the manager; disk_uuid is
                // used by re-formats to preserve idempotency.
                std::fs::write(format!("{dir}/cluster_id"), &cluster_id)
                    .with_context(|| format!("write cluster_id in {dir}"))?;
                std::fs::write(format!("{dir}/disk_uuid"), disk_uuid)
                    .with_context(|| format!("write disk_uuid in {dir}"))?;
                std::fs::write(format!("{dir}/node_id"), node_id.to_string())
                    .with_context(|| format!("write node_id in {dir}"))?;
                std::fs::write(format!("{dir}/disk_id"), disk_id.to_string())
                    .with_context(|| format!("write disk_id in {dir}"))?;
                disk_assignments.push((dir.clone(), disk_uuid.clone(), disk_id));
            }

            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "node_id": node_id,
                        "cluster_id": cluster_id,
                        "listen": listen,
                        "advertise": advertise,
                        "disks": disk_assignments.iter()
                            .map(|(d, u, id)| serde_json::json!({
                                "dir": d, "uuid": u, "disk_id": id,
                            }))
                            .collect::<Vec<_>>(),
                    }))?
                );
            } else {
                println!("node registered: node_id={node_id}");
                println!("cluster_id={cluster_id}");
                for (dir, _u, disk_id) in &disk_assignments {
                    println!("  {dir}: node_id={node_id}, disk_id={disk_id}");
                }
                println!("\nFormat complete.");
                println!("listen={listen}, advertise={advertise}");
                println!("Start the extent node with:");
                println!(
                    "  autumn-extent-node --port {} --manager {} --data {}",
                    listen.split(':').next_back().unwrap_or("9101"),
                    args.manager,
                    dirs.join(",")
                );
            }
        }
    }
    let _ = std::io::stdout().flush();
    Ok(())
}

fn print_code(json: bool, op: &str, resp: &CodeResp) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "code": resp.code,
                "message": resp.message,
            }))
            .unwrap()
        );
    } else if resp.code == CODE_OK {
        println!("{}: ok", op);
    } else {
        println!("{}: code={} {}", op, resp.code, resp.message);
    }
    if resp.code != CODE_OK {
        std::process::exit(2);
    }
}

// ---------------------------------------------------------------------------
// F213 bootstrap (migrated from autumn_client.rs)
// ---------------------------------------------------------------------------

async fn run_bootstrap(
    client: &ClusterClient,
    json_out: bool,
    replication: &str,
    presplit: &str,
    log_ec: Option<(u32, u32)>,
    row_ec: Option<(u32, u32)>,
) -> Result<()> {
    let meta_replicates = parse_replication(replication)?;

    // Per-stream (replicates, ec_data, ec_parity):
    //   - Replica streams: replicates=N, ec_data=N, ec_parity=0.
    //   - EC streams: replicates is the open-extent replica count (=
    //     meta_replicates), and (ec_data, ec_parity) describes the
    //     post-seal EC encoding (e.g. 4+1, 7+1). The two are independent.
    let log_params = log_ec.map(|(k, m)| (meta_replicates, k, m)).unwrap_or((
        meta_replicates,
        meta_replicates,
        0,
    ));
    let row_params = row_ec.map(|(k, m)| (meta_replicates, k, m)).unwrap_or((
        meta_replicates,
        meta_replicates,
        0,
    ));
    let meta_params = (meta_replicates, meta_replicates, 0u32);

    let ranges: Vec<(Vec<u8>, Vec<u8>)> = {
        let parts: Vec<&str> = presplit.splitn(2, ':').collect();
        let n: usize = parts[0].parse().unwrap_or(1);
        let kind = parts.get(1).copied().unwrap_or("normal");
        match kind {
            "hexstring" => hex_split_ranges(n),
            "fuse" => fuse_split_ranges(n),
            _ => vec![(vec![], vec![])],
        }
    };

    let create_stream_once =
        |label: &'static str, replicates: u32, ec_data: u32, ec_parity: u32| async move {
            let req_bytes = rkyv_encode(&CreateStreamReq {
                replicates,
                ec_data_shard: ec_data,
                ec_parity_shard: ec_parity,
            });
            let mut attempt = 0u32;
            loop {
                let resp_bytes = client
                    .mgr_call(MSG_CREATE_STREAM, req_bytes.clone())
                    .await
                    .with_context(|| format!("create {label} stream"))?;
                let resp: CreateStreamResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
                if resp.code == CODE_OK {
                    return Ok::<u64, anyhow::Error>(resp.stream.map(|s| s.stream_id).unwrap_or(0));
                }
                if resp.code == CODE_NOT_LEADER && attempt < 60 {
                    attempt += 1;
                    compio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                bail!(
                    "create {label} stream failed: code={} {}",
                    resp.code,
                    resp.message
                );
            }
        };

    let mut created: Vec<serde_json::Value> = Vec::new();
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
            part_id: 0,
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
            bail!(
                "bootstrap partition {} failed: code={} {}",
                idx,
                resp.code,
                resp.message
            );
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
        if json_out {
            created.push(serde_json::json!({
                "index": idx,
                "part_id": resp.part_id,
                "log_stream_id": log_stream_id,
                "row_stream_id": row_stream_id,
                "meta_stream_id": meta_stream_id,
                "log_ec": [log_k, log_m],
                "row_ec": [row_k, row_m],
                "meta_ec": [meta_k, meta_m],
                "range_start": start_s,
                "range_end": end_s,
            }));
        } else {
            println!(
                "partition {} created: id={} log={} ({}+{}) row={} ({}+{}) meta={} ({}+{}) range=[{}..{})",
                idx,
                resp.part_id,
                log_stream_id,
                log_k,
                log_m,
                row_stream_id,
                row_k,
                row_m,
                meta_stream_id,
                meta_k,
                meta_m,
                start_s,
                end_s,
            );
        }
    }
    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "partitions": created,
            }))?
        );
    } else {
        println!("bootstrap succeeded: {} partition(s)", ranges.len());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// F213 info (migrated from autumn_client.rs)
// ---------------------------------------------------------------------------

async fn run_info(
    client: &ClusterClient,
    json_out: bool,
    part: Option<u64>,
    detail: bool,
) -> Result<()> {
    // F203: `--detail` prints PartitionLoad snapshot for `part`.
    if detail {
        let pid = part.expect("--detail requires --part PID; checked at parse time");
        let req = rkyv_encode(&GetPartitionDetailReq { part_id: pid });
        let resp_bytes = client
            .mgr_call(MSG_GET_PARTITION_DETAIL, req)
            .await
            .context("get partition detail")?;
        let resp: GetPartitionDetailResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
        if resp.code != CODE_OK {
            bail!("get_partition_detail: code={} {}", resp.code, resp.message);
        }
        let l = &resp.load;
        if json_out {
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
            println!(
                "{}",
                serde_json::to_string_pretty(&v).context("serialize detail")?
            );
        } else {
            println!("=== Partition {pid} (bucket_ts={}) ===", resp.bucket_ts);
            println!("  size_bytes={}", l.size_bytes);
            println!(
                "  req_per_sec={}  imm_full_per_sec={}",
                l.req_per_sec, l.imm_full_per_sec
            );
            println!(
                "  gc_debt_bytes={} ({} MiB)",
                l.gc_debt_bytes,
                l.gc_debt_bytes / (1024 * 1024)
            );
            println!(
                "  pending_compaction_bytes={} ({} MiB)  [major]",
                l.pending_compaction_bytes,
                l.pending_compaction_bytes / (1024 * 1024)
            );
            println!(
                "  minor_compact_pending_bytes={} ({} MiB)",
                l.minor_compact_pending_bytes,
                l.minor_compact_pending_bytes / (1024 * 1024)
            );
            println!(
                "  sst_tombstone_bytes={}  sst_expired_bytes={}  sst_out_of_range_bytes={}",
                l.sst_tombstone_bytes, l.sst_expired_bytes, l.sst_out_of_range_bytes
            );
            println!(
                "  gc_inflight={}  compact_inflight={}",
                l.gc_inflight, l.compact_inflight
            );
            println!(
                "  last_gc_at={}  last_compact_at={}",
                l.last_gc_at, l.last_compact_at
            );
            println!("  sealed_log_extent_count={}", l.sealed_log_extent_count);
        }
        return Ok(());
    }

    // === Fetch manager data ===
    let stream_resp_bytes = client
        .mgr_call(
            MSG_STREAM_INFO,
            rkyv_encode(&StreamInfoReq {
                stream_ids: Vec::new(),
            }),
        )
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
    let node_map: HashMap<u64, String> = nodes_sorted
        .iter()
        .map(|(id, n)| (*id, n.address.clone()))
        .collect();

    let mut streams_sorted: Vec<(u64, MgrStreamInfo)> = stream_resp.streams.into_iter().collect();
    streams_sorted.sort_by_key(|(id, _)| *id);
    let stream_map: HashMap<u64, MgrStreamInfo> = streams_sorted.iter().cloned().collect();

    let regions: HashMap<u64, MgrRegionInfo> = regions_resp.regions.into_iter().collect();
    let ps_details: HashMap<u64, MgrPsDetail> = regions_resp.ps_details.into_iter().collect();
    let part_addr_map: HashMap<u64, String> = regions_resp.part_addrs.into_iter().collect();

    let mut part_ids: Vec<u64> = regions.keys().copied().collect();
    part_ids.sort();

    // === Query commit_length for open extents ===
    let probe_set: HashSet<u64> = if let Some(pid) = part {
        regions
            .get(&pid)
            .into_iter()
            .flat_map(|r| [r.log_stream, r.row_stream, r.meta_stream])
            .flat_map(|sid| {
                stream_map
                    .get(&sid)
                    .into_iter()
                    .flat_map(|s| s.extent_ids.iter().copied())
            })
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
                        // F210-H3 Tier 2: probe RPC has no PS-owner context;
                        // must NOT use the fence-gated commit_length RPC.
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
    let pids_to_query: Vec<u64> = if let Some(pid) = part {
        vec![pid]
    } else {
        part_ids.clone()
    };
    let mut part_discards: HashMap<u64, Vec<(u64, i64)>> = HashMap::new();
    for pid in &pids_to_query {
        let r = match regions.get(pid) {
            Some(r) => r,
            None => continue,
        };
        let ps_addr = part_addr_map
            .get(pid)
            .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
            .map(|s| s.as_str())
            .unwrap_or("");
        if ps_addr.is_empty() {
            continue;
        }
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

    if json_out {
        let nodes_view: Vec<InfoNodeView> = nodes_sorted
            .iter()
            .map(|(nid, n)| InfoNodeView {
                node_id: *nid,
                address: n.address.clone(),
                disks: n
                    .disks
                    .iter()
                    .map(|did| InfoDiskView {
                        disk_id: *did,
                        uuid: disk_map
                            .get(did)
                            .map(|d| d.uuid.clone())
                            .unwrap_or_default(),
                        online: disk_map.get(did).map(|d| d.online).unwrap_or(false),
                    })
                    .collect(),
            })
            .collect();

        let extents_view: Vec<InfoExtentView> = {
            let mut v: Vec<_> = extent_map
                .iter()
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

        let streams_view: Vec<InfoStreamView> = streams_sorted
            .iter()
            .map(|(sid, s)| {
                let r = if s.replicates > 0 {
                    s.replicates
                } else {
                    s.extent_ids
                        .iter()
                        .find_map(|eid| {
                            extent_map
                                .get(eid)
                                .filter(|e| !e.ec_converted)
                                .map(|e| e.replicates.len() as u32)
                        })
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

        let mut partitions_view: Vec<InfoPartitionView> = part_ids
            .iter()
            .filter_map(|pid| {
                let r = regions.get(pid)?;
                let rg = r.rg.as_ref()?;
                let ps_addr = part_addr_map
                    .get(pid)
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
                let discards = part_discards
                    .get(pid)
                    .map(|v| {
                        v.iter()
                            .map(|&(eid, bytes)| InfoDiscardEntry {
                                extent_id: eid,
                                bytes,
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some(InfoPartitionView {
                    part_id: *pid,
                    ps_addr,
                    range_start: String::from_utf8_lossy(&rg.start_key).into_owned(),
                    range_end: if rg.end_key.is_empty() {
                        String::new()
                    } else {
                        String::from_utf8_lossy(&rg.end_key).into_owned()
                    },
                    live_size,
                    total_extents,
                    log_stream_id: r.log_stream,
                    row_stream_id: r.row_stream,
                    meta_stream_id: r.meta_stream,
                    discards,
                })
            })
            .collect();

        // F205: keep sort_by live_size desc for consistent ordering.
        partitions_view.sort_by_key(|p| std::cmp::Reverse(p.live_size));

        if let Some(pid) = part {
            match partitions_view.into_iter().find(|p| p.part_id == pid) {
                Some(pv) => println!("{}", serde_json::to_string_pretty(&pv)?),
                None => eprintln!("partition {pid} not found"),
            }
        } else {
            let snapshot = InfoSnapshot {
                nodes: nodes_view,
                extents: extents_view,
                streams: streams_view,
                partitions: partitions_view,
            };
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
    } else {
        // === Text output ===
        let show_pids: Vec<u64> = if let Some(pid) = part {
            vec![pid]
        } else {
            part_ids.clone()
        };

        if part.is_none() {
            println!("=== Nodes ===");
            for (nid, n) in &nodes_sorted {
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
                let tag = if open_extents.contains(eid) {
                    " (open)"
                } else {
                    ""
                };
                let layout = if e.ec_converted {
                    format!(
                        "EC({}+{}), data={:?}, parity={:?}",
                        e.replicates.len(),
                        e.parity.len(),
                        e.replicates,
                        e.parity
                    )
                } else if e.parity.is_empty() {
                    format!("replicas={:?}", e.replicates)
                } else {
                    let mut all = e.replicates.clone();
                    all.extend(e.parity.iter().copied());
                    format!("replicas={:?}", all)
                };
                println!(
                    "  extent {}: size={}{}, {}, refs={}, eversion={}",
                    eid,
                    human_size(e.sealed_length),
                    tag,
                    layout,
                    e.refs,
                    e.eversion
                );
            }

            println!("\n=== Streams ===");
            for (sid, s) in &streams_sorted {
                let total = stream_total(s, &extent_map);
                let r = if s.replicates > 0 {
                    s.replicates
                } else {
                    s.extent_ids
                        .iter()
                        .find_map(|eid| {
                            extent_map
                                .get(eid)
                                .filter(|e| !e.ec_converted)
                                .map(|e| e.replicates.len() as u32)
                        })
                        .unwrap_or(0)
                };
                let layout = if s.ec_parity_shard == 0 {
                    format!("repl={}", r)
                } else {
                    format!("repl={}, EC={}+{}", r, s.ec_data_shard, s.ec_parity_shard)
                };
                println!(
                    "  stream {} ({}): extents={:?}, total={}",
                    sid,
                    layout,
                    s.extent_ids,
                    human_size(total)
                );
            }
        }

        let section_header = if let Some(pid) = part {
            format!("\n=== Partition {pid} ===")
        } else {
            "\n=== Partitions ===".to_string()
        };
        println!("{section_header}");

        for pid in &show_pids {
            let r = match regions.get(pid) {
                Some(r) => r,
                None => {
                    println!("  part {pid}: not found");
                    continue;
                }
            };
            let rg = match r.rg.as_ref() {
                Some(r) => r,
                None => continue,
            };
            let ps_addr = part_addr_map
                .get(pid)
                .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
                .map(|s| s.as_str())
                .unwrap_or("unknown");
            println!(
                "  part {}: ps={}, range=[{}..{})",
                pid,
                ps_addr,
                String::from_utf8_lossy(&rg.start_key),
                if rg.end_key.is_empty() {
                    "\u{221e}".to_string()
                } else {
                    String::from_utf8_lossy(&rg.end_key).to_string()
                }
            );
            let discards = part_discards.get(pid);
            let mut part_total = 0u64;
            let mut part_extents = 0usize;
            for (label, sid) in [
                ("log", r.log_stream),
                ("row", r.row_stream),
                ("meta", r.meta_stream),
            ] {
                if let Some(s) = stream_map.get(&sid) {
                    let total = stream_total(s, &extent_map);
                    part_total += total;
                    part_extents += s.extent_ids.len();
                    let mut line = format!(
                        "    {}: stream {}, extents={:?}, size={}",
                        label,
                        sid,
                        s.extent_ids,
                        human_size(total)
                    );
                    if label == "log" {
                        if let Some(d) = discards {
                            if !d.is_empty() {
                                let total_discard: i64 = d.iter().map(|(_, b)| b).sum();
                                line.push_str(&format!(
                                    ", discard: {} ext / {} pending",
                                    d.len(),
                                    human_size(total_discard as u64)
                                ));
                            }
                        }
                    }
                    println!("{line}");
                }
            }
            println!(
                "    total: {} extents, {}",
                part_extents,
                human_size(part_total)
            );
        }
    }
    Ok(())
}

fn main() {
    let args = parse();
    let rt = compio::runtime::Runtime::new().expect("compio runtime");
    if let Err(e) = rt.block_on(run(args)) {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn parse_byte_size_accepts_plain_integer() {
        assert_eq!(
            super::parse_byte_size("16777216").unwrap(),
            16 * 1024 * 1024
        );
        assert_eq!(super::parse_byte_size("0").unwrap(), 0);
    }

    #[test]
    fn parse_byte_size_accepts_suffixes() {
        assert_eq!(super::parse_byte_size("512K").unwrap(), 512 * 1024);
        assert_eq!(super::parse_byte_size("16M").unwrap(), 16 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("16MiB").unwrap(), 16 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(
            super::parse_byte_size("2T").unwrap(),
            2u64 * 1024 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn parse_ec_flag_accepts_valid_shapes() {
        assert_eq!(super::parse_ec_flag("3+1").unwrap(), (3, 1));
        assert_eq!(super::parse_ec_flag("4+2").unwrap(), (4, 2));
        assert!(super::parse_ec_flag("3").is_err());
        assert!(super::parse_ec_flag("0+1").is_err());
        assert!(super::parse_ec_flag("3+0").is_err());
    }

    #[test]
    fn parse_replication_extracts_first_n() {
        assert_eq!(super::parse_replication("3+0").unwrap(), 3);
        assert_eq!(super::parse_replication("3").unwrap(), 3);
        assert!(super::parse_replication("0+0").is_err());
    }

    #[test]
    fn hex_split_ranges_partitions_full_space() {
        let ranges = super::hex_split_ranges(1);
        assert_eq!(ranges.len(), 1);
        assert!(ranges[0].0.is_empty() && ranges[0].1.is_empty());

        let ranges = super::hex_split_ranges(4);
        assert_eq!(ranges.len(), 4);
        assert!(ranges[0].0.is_empty());
        assert!(ranges[3].1.is_empty());
        for i in 0..3 {
            assert_eq!(ranges[i].1, ranges[i + 1].0);
        }
    }

    #[test]
    fn fuse_split_ranges_single_partition_is_full_space() {
        let ranges = super::fuse_split_ranges(1);
        assert_eq!(ranges.len(), 1);
        assert!(ranges[0].0.is_empty() && ranges[0].1.is_empty());
    }

    #[test]
    fn fuse_split_ranges_n8_keys_are_inode_bottom_byte_boundaries() {
        let ranges = super::fuse_split_ranges(8);
        assert_eq!(ranges.len(), 8);
        // First range starts at empty, last range ends at empty.
        assert!(ranges[0].0.is_empty());
        assert!(ranges[7].1.is_empty());
        // Contiguous: each range's end == next range's start.
        for i in 0..7 {
            assert_eq!(ranges[i].1, ranges[i + 1].0);
        }
        // Each split key is [0x03, 0,0,0,0,0,0,0, byte_i] with byte_i
        // stepping 0x20 → 0x40 → ... → 0xE0.
        for i in 0..7 {
            let split_key = &ranges[i].1;
            assert_eq!(split_key.len(), 9);
            assert_eq!(split_key[0], 0x03);
            for b in &split_key[1..8] {
                assert_eq!(*b, 0u8);
            }
            assert_eq!(split_key[8], ((i + 1) * 32) as u8);
        }
    }

    #[test]
    fn fuse_split_ranges_distributes_sequential_inodes_round_robin() {
        use std::collections::HashMap;
        let n = 8;
        let ranges = super::fuse_split_ranges(n);
        let mut hits: HashMap<usize, usize> = HashMap::new();
        // Walk 256 sequential inodes (ino=0..=255 — all bottom-byte
        // values exactly once) and check the bottom-byte split places
        // them uniformly across 8 partitions.
        for ino in 0u64..256 {
            let mut key = vec![0x03];
            key.extend_from_slice(&ino.to_be_bytes());
            key.extend_from_slice(&0u64.to_be_bytes()); // logical_off = 0
            // Find the partition whose range contains `key`.
            let mut found = None;
            for (idx, (start, end)) in ranges.iter().enumerate() {
                let after_start = start.is_empty() || key.as_slice() >= start.as_slice();
                let before_end = end.is_empty() || key.as_slice() < end.as_slice();
                if after_start && before_end {
                    found = Some(idx);
                    break;
                }
            }
            *hits.entry(found.unwrap()).or_insert(0) += 1;
        }
        // Each of the 8 partitions should get exactly 32 inodes.
        for p in 0..n {
            assert_eq!(*hits.get(&p).unwrap_or(&0), 32, "partition {p}");
        }
    }

    #[test]
    fn fuse_split_ranges_inode_meta_and_dirent_live_in_partition_zero() {
        let ranges = super::fuse_split_ranges(8);
        // [0x01]inode_meta + [0x02]dirent both prefix-sort before
        // [0x03]extents, so they MUST land in partition 0.
        let inode_meta_key = vec![0x01, 0, 0, 0, 0, 0, 0, 0, 0x42];
        let dirent_key = vec![0x02, 0, 0, 0, 0, 0, 0, 0, 0x01, b'x'];
        assert!(ranges[0].0.is_empty());
        assert!(inode_meta_key.as_slice() < ranges[0].1.as_slice());
        assert!(dirent_key.as_slice() < ranges[0].1.as_slice());
    }
}
