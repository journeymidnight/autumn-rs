//! F211-G companion: a thin CLI wrapper around the F211 admin / health
//! RPCs on the manager. Intentionally separate from `autumn-client` so
//! operator tooling is isolated from data-plane CLI churn (per the
//! F211 plan, which forbids adding admin subcommands to the
//! data-plane CLI).
//!
//! The Python policy script (`python/node_policy.py`) shells out to
//! this binary; JSON output is the wire between them.

use std::io::Write;

use anyhow::{anyhow, bail, Result};
use autumn_client::ClusterClient;
use autumn_rpc::manager_rpc::*;
use serde::Serialize;

fn usage() -> ! {
    eprintln!("usage: autumn-op [--manager addr] [--json] <command>");
    eprintln!();
    eprintln!("read commands:");
    eprintln!("  list-nodes                   show every EN's auto-state + override");
    eprintln!("  extent-health [--node ID] [--all]");
    eprintln!("                               per-slot health (default: only unhealthy)");
    eprintln!("  list-ec-markers              ConvertToEc inflight markers + coord state");
    eprintln!("  recovery-stats               in-flight + per-source/target counters");
    eprintln!("  audit-log [--op N] [--node N] [--since S] [--until U] [--limit L]");
    eprintln!("                               query operator action history");
    eprintln!();
    eprintln!("admin commands:");
    eprintln!("  fence-node <id> --reason \"...\" --by alice [--force]");
    eprintln!("  maintenance <id> --reason \"...\" --by alice [--expire UNIX_TS]");
    eprintln!("  unfence <id> --by alice");
    eprintln!("  remove <id> --by alice");
    std::process::exit(1);
}

struct Args {
    manager: String,
    json: bool,
    cmd: Command,
}

enum Command {
    ListNodes,
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
}

fn parse() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut manager = "127.0.0.1:9001".to_string();
    let mut json = false;
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
        "list-nodes" => Command::ListNodes,
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
        "fence-node" => {
            let node_id: u64 = raw.get(i).and_then(|s| s.parse().ok()).unwrap_or_else(|| usage());
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
            let node_id: u64 = raw.get(i).and_then(|s| s.parse().ok()).unwrap_or_else(|| usage());
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
            let node_id: u64 = raw.get(i).and_then(|s| s.parse().ok()).unwrap_or_else(|| usage());
            i += 1;
            let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
            Command::Unfence { node_id, by }
        }
        "remove" => {
            let node_id: u64 = raw.get(i).and_then(|s| s.parse().ok()).unwrap_or_else(|| usage());
            i += 1;
            let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
            Command::Remove { node_id, by }
        }
        _ => usage(),
    };
    Args { manager, json, cmd }
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

fn auto_state_str(b: u8) -> &'static str {
    match b {
        NODE_AUTO_STATE_ONLINE => "Online",
        NODE_AUTO_STATE_SUSPECTED => "Suspected",
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

async fn run(args: Args) -> Result<()> {
    let client = ClusterClient::connect(&args.manager).await?;
    match args.cmd {
        Command::ListNodes => {
            let bytes = client
                .mgr_call(MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {}))
                .await?;
            let resp: ListNodeStatesResp =
                rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
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
                    "{:<6} {:<24} {:<10} {:<8} {:<8} {:<12} {}",
                    "ID", "ADDRESS", "AUTO", "HB_AGO", "SUSP_AGE", "OVERRIDE", "REASON"
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
            let resp: ExtentHealthResp =
                rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("extent-health: {}", resp.message);
            }
            if args.json {
                println!("{}", serde_json::to_string_pretty(&resp.extents.into_iter().map(|e| {
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
                }).collect::<Vec<_>>())?);
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
            let resp: ListEcInflightMarkersResp =
                rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("list-ec-markers: {}", resp.message);
            }
            if args.json {
                println!("{}", serde_json::to_string_pretty(&resp.markers.into_iter().map(|m| {
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
                }).collect::<Vec<_>>())?);
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
            let resp: RecoveryStatsResp =
                rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
            if resp.code != CODE_OK {
                bail!("recovery-stats: {}", resp.message);
            }
            if args.json {
                println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                    "global_inflight": resp.global_inflight,
                    "max_global": resp.max_global,
                    "max_per_source": resp.max_per_source,
                    "max_per_target": resp.max_per_target,
                    "per_source": resp.per_source,
                    "per_target": resp.per_target,
                    "backoff_entries": resp.backoff_entries,
                }))?);
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
            let bytes = client.mgr_call(MSG_QUERY_AUDIT_LOG, rkyv_encode(&req)).await?;
            let resp: QueryAuditLogResp =
                rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
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
                println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                    "code": resp.code,
                    "message": resp.message,
                    "blocking_extent_ids": resp.blocking_extent_ids,
                    "blocking_marker_extent_ids": resp.blocking_marker_extent_ids,
                }))?);
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

fn main() {
    let args = parse();
    let rt = compio::runtime::Runtime::new().expect("compio runtime");
    if let Err(e) = rt.block_on(run(args)) {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}
