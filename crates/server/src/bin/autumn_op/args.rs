//! `autumn-op` CLI argument parsing: `Args` / `Command` + the hand-rolled
//! parser and its value-parse helpers. Split out of `main.rs` (2026-06-24,
//! behaviour-preserving). `usage()` / `parse_admin_flags` / `parse_byte_size`
//! / `parse_ec_flag` stay private — only `parse()` (and the unit tests) use
//! them; the rest is `pub(crate)` for `main.rs`'s dispatcher + bootstrap.

use anyhow::{bail, Context, Result};
use autumn_transport::TransportKind;

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

pub(crate) struct Args {
    pub(crate) manager: String,
    pub(crate) json: bool,
    pub(crate) transport: TransportKind,
    pub(crate) cmd: Command,
}

pub(crate) enum Command {
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

pub(crate) fn parse() -> Args {
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

pub(crate) fn hex_split_ranges(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
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
pub(crate) fn fuse_split_ranges(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
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

pub(crate) fn parse_replication(s: &str) -> Result<u32> {
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
