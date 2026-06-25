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


mod args;
use args::{
    derive_control_address, fuse_split_ranges, hex_split_ranges, parse, parse_replication,
    Args, Command,
};

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
    /// vp_table_refs-removal note: the maintenance machinery is gone, so for
    /// extents managed under this build this is always 0. A non-zero value is a
    /// LEGACY extent carried over from a pre-removal build — retained by the
    /// upgrade-safety guard (`refs==0 && vp_table_refs>0` is NOT reclaimed) and
    /// a Stage-2 migration target. Removed from the schema in Stage 2.
    vp_table_refs: u64,
    eversion: u64,
    /// How many live streams list this extent in `extent_ids`. 0 = orphan
    /// (not in any stream): either retained-by-vp_table_refs legacy live data,
    /// or a both-zero reclaimable extent (the EXTENT10-AUTORECLAIM sweep reaps
    /// it). `refs` should equal this; a gap is a refcount leak (MERGE-REFS-LEAK).
    in_streams: usize,
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
            // A FAILED registration (follower / non-leader, fenced or
            // decommissioned address, same-addr-different-disk) returns
            // code != CODE_OK with node_id=0 and no/partial disk_uuids.
            // Writing the local format sentinels on that response leaves a
            // dir that LOOKS formatted (cluster_id present) but the manager
            // never accepted — the EN then starts on a node/disk the manager
            // doesn't know about.
            if resp.code != CODE_OK {
                bail!("register node failed: code={} {}", resp.code, resp.message);
            }

            let node_id = resp.node_id;
            // Resolve every requested disk_uuid -> disk_id BEFORE writing any
            // sentinel: a missing mapping (the manager returned only the
            // disks it actually accepted) must FAIL, not silently write
            // disk_id=0 (a pseudo-formatted dir the manager can't route to).
            // Two passes so a partial failure leaves NO half-written dirs.
            let uuid_to_id: HashMap<&str, u64> = resp
                .disk_uuids
                .iter()
                .map(|(u, id)| (u.as_str(), *id))
                .collect();
            let mut disk_assignments: Vec<(String, String, u64)> = Vec::new();
            for (dir, disk_uuid) in dirs.iter().zip(disk_uuids.iter()) {
                let disk_id = *uuid_to_id.get(disk_uuid.as_str()).ok_or_else(|| {
                    anyhow!(
                        "manager did not assign a disk_id for {dir} (uuid {disk_uuid}); \
                         not writing format sentinels"
                    )
                })?;
                disk_assignments.push((dir.clone(), disk_uuid.clone(), disk_id));
            }
            for (dir, disk_uuid, disk_id) in &disk_assignments {
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
    // Check the manager's response code BEFORE building the snapshot: a
    // follower / non-routable manager returns code != CODE_OK with EMPTY
    // collections, which the renderer would otherwise present as a real,
    // empty cluster (silent in `--json`, and the exit code stays 0).
    if stream_resp.code != CODE_OK {
        bail!("stream info: code={} {}", stream_resp.code, stream_resp.message);
    }

    let nodes_resp_bytes = client
        .mgr_call(MSG_NODES_INFO, Bytes::new())
        .await
        .context("nodes info")?;
    let nodes_resp: NodesInfoResp = rkyv_decode(&nodes_resp_bytes).map_err(decode_err)?;
    if nodes_resp.code != CODE_OK {
        bail!("nodes info: code={} {}", nodes_resp.code, nodes_resp.message);
    }

    let regions_resp_bytes = client
        .mgr_call(MSG_GET_REGIONS, Bytes::new())
        .await
        .context("get regions")?;
    let regions_resp: GetRegionsResp = rkyv_decode(&regions_resp_bytes).map_err(decode_err)?;
    if regions_resp.code != CODE_OK {
        bail!("get regions: code={} {}", regions_resp.code, regions_resp.message);
    }

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
        // Authoritative seal STATE is the explicit `sealed` flag, NOT
        // `sealed_length == 0` (manager note 32): a failover can seal an
        // EMPTY tail (`sealed = true, sealed_length = 0`). Treating that as
        // open mislabels it `(open)` and pointlessly probes a sealed extent.
        if !ext.sealed {
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

    // Per-extent live stream-membership count (how many streams list it in
    // extent_ids, duplicates counted). 0 = orphan (not in any stream). `refs`
    // should equal this; a gap is a leaked refcount (MERGE-REFS-LEAK). Computed
    // before the json/text split so both renderings can flag orphans.
    let mut stream_member_count: HashMap<u64, usize> = HashMap::new();
    for (_sid, s) in &streams_sorted {
        for eid in &s.extent_ids {
            *stream_member_count.entry(*eid).or_insert(0) += 1;
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
                    vp_table_refs: e.vp_table_refs,
                    eversion: e.eversion,
                    in_streams: stream_member_count.get(eid).copied().unwrap_or(0),
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
                let in_streams = stream_member_count.get(*eid).copied().unwrap_or(0);
                let mut tag = String::new();
                if open_extents.contains(eid) {
                    tag.push_str(" (open)");
                }
                // Orphan = in no live stream. Distinguish a LEGACY extent still
                // retained by the upgrade-safety guard (vp_table_refs>0 — live
                // VPs from a pre-removal build, a Stage-2 migration target) from
                // a both-zero reclaimable extent (the EXTENT10-AUTORECLAIM sweep
                // reaps it). Also flag a refs vs membership gap (MERGE-REFS-LEAK).
                if in_streams == 0 {
                    tag.push_str(if e.vp_table_refs > 0 {
                        " (ORPHAN: no stream, retained by vp_table_refs — legacy live data, Stage-2 migrate)"
                    } else {
                        " (ORPHAN: no stream, vp_table_refs=0 — reclaimable, pending sweep)"
                    });
                } else if e.refs as usize != in_streams {
                    tag.push_str(" (refs-leak)");
                }
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
                    "  extent {}: size={}{}, {}, refs={} (streams={}), vp_table_refs={}, eversion={}",
                    eid,
                    human_size(e.sealed_length),
                    tag,
                    layout,
                    e.refs,
                    in_streams,
                    e.vp_table_refs,
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

