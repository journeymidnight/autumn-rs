//! `/api/overview` JSON compose, extracted so both the (soon-removed) in-manager
//! dashboard and the standalone `autumn-op overview` subcommand emit the IDENTICAL
//! shape the web page's JS consumes. Pure: manager RPC responses in, JSON string
//! out — no `&self`, so `autumn-op` (which already calls the four RPCs) can reuse
//! it verbatim. Keep byte-compatible with the page; a changed key blanks a panel.

use std::collections::HashMap;

use autumn_rpc::manager_rpc::{
    ClusterDfResp, ExtentHealthSummaryResp, GetClusterOverviewResp, ListNodeStatesResp,
    NodeCapWire, NodeStateEntry, PolicyCandidate, CODE_OK, HEALTH_ERR, HEALTH_OK, HEALTH_WARN,
    NODE_AUTO_STATE_ONLINE, NODE_AUTO_STATE_SUSPECTED, NODE_AUTO_STATE_SUSPEND,
    NODE_OVERRIDE_FENCED, NODE_OVERRIDE_MAINTENANCE, POLICY_KIND_EC, POLICY_KIND_GC,
    POLICY_KIND_MAJOR_COMPACT, POLICY_KIND_MERGE, POLICY_KIND_MINOR_COMPACT,
    POLICY_KIND_REBALANCE, POLICY_KIND_REPAIR, POLICY_KIND_SPLIT, SLOT_STATE_BEHIND,
    SLOT_STATE_CORRUPT,
    SLOT_STATE_DISK_FAULTED, SLOT_STATE_FENCED, SLOT_STATE_MAINTENANCE, SLOT_STATE_SERVING,
    SLOT_STATE_UNREACHABLE,
};
use serde_json::json;

/// Raw-capacity amplification: bytes consumed on the EN filesystems divided by
/// the de-amplified extent payload size. `logical_size` is the sum of distinct
/// sealed extent sizes plus committed open extent sizes.
///
/// `physical_used` is kept as a diagnostic field, but it is not this ratio's
/// numerator: it is an EN-maintained sum of extent file lengths and can diverge
/// from statvfs capacity consumption (sparse/punched files and filesystem
/// allocation are the important examples).
pub fn raw_capacity_amplification(raw_used: u64, logical_size: u64) -> f64 {
    if logical_size == 0 {
        0.0
    } else {
        raw_used as f64 / logical_size as f64
    }
}

use crate::auto_policy::{cooldown_key, describe_candidate, policy_kind_str};

/// Node auto-state byte → the string the page shows.
fn node_auto_state_str(b: u8) -> &'static str {
    match b {
        NODE_AUTO_STATE_ONLINE => "Online",
        NODE_AUTO_STATE_SUSPECTED => "Suspected",
        NODE_AUTO_STATE_SUSPEND => "Suspend",
        _ => "Online",
    }
}

/// Override-kind byte → the page string (`"-"` = no override).
fn node_override_kind_str(b: u8) -> &'static str {
    match b {
        NODE_OVERRIDE_FENCED => "fenced",
        NODE_OVERRIDE_MAINTENANCE => "maintenance",
        _ => "-",
    }
}

pub fn health_status_str(b: u8) -> &'static str {
    match b {
        HEALTH_OK => "HEALTH_OK",
        HEALTH_WARN => "HEALTH_WARN",
        HEALTH_ERR => "HEALTH_ERR",
        _ => "HEALTH_UNKNOWN",
    }
}

/// `SLOT_STATE_*` as the word `autumn-op health` and the page print.
pub fn slot_state_str(b: u8) -> &'static str {
    match b {
        SLOT_STATE_SERVING => "serving",
        SLOT_STATE_BEHIND => "behind",
        SLOT_STATE_UNREACHABLE => "unreachable",
        SLOT_STATE_MAINTENANCE => "maintenance",
        SLOT_STATE_FENCED => "fenced",
        SLOT_STATE_CORRUPT => "corrupt",
        SLOT_STATE_DISK_FAULTED => "disk-faulted",
        _ => "unknown",
    }
}

/// The extent health summary as JSON: `autumn-op health --json` and the
/// overview's `extent_health` field are this one shape.
pub fn health_json(r: &ExtentHealthSummaryResp) -> serde_json::Value {
    json!({
        "status": health_status_str(r.status),
        "sealed_extents": r.sealed_extents,
        "open_extents": r.open_extents,
        "clean": r.clean,
        "degraded": r.degraded,
        "no_redundancy": r.no_redundancy,
        "unavailable": r.unavailable,
        "recovering": r.recovering,
        "degraded_bytes": r.degraded_bytes,
        "repair_requested_slots": r.repair_requested_slots,
        "slots_not_serving": r
            .slot_counts
            .iter()
            .enumerate()
            .filter(|(state, _)| *state != SLOT_STATE_SERVING as usize)
            .map(|(state, n)| (slot_state_str(state as u8).to_string(), json!(n)))
            .collect::<serde_json::Map<_, _>>(),
        "problems": r.problems.iter().map(|p| json!({
            "extent_id": p.extent_id,
            "sealed_length": p.sealed_length,
            "ec_converted": p.ec_converted,
            "serving": p.serving,
            "total": p.total,
            "needed": p.needed,
            "recovering": p.recovering,
            "slots": p.slots.iter().map(|s| json!({
                "slot": s.slot_index,
                "node_id": s.node_id,
                "state": slot_state_str(s.state),
                "degraded_secs": s.degraded_secs,
                "repair_requested": s.repair_requested,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

/// Advisory candidate → the structured `/api/action` payload the page's `Apply`
/// button sends (or `None` for advisory-only / no target).
fn candidate_to_action(c: &PolicyCandidate) -> Option<serde_json::Value> {
    match c.kind {
        POLICY_KIND_EC => {
            if c.secondary_part_id == 0 {
                return None;
            }
            Some(json!({ "action": "force_ec_convert", "extent_id": c.secondary_part_id }))
        }
        POLICY_KIND_SPLIT => Some(json!({ "action": "split", "part_id": c.primary_part_id })),
        POLICY_KIND_MERGE => {
            if c.secondary_part_id == 0 {
                return None;
            }
            Some(json!({
                "action": "merge",
                "part_id": c.primary_part_id,
                "victim_part_id": c.secondary_part_id,
            }))
        }
        POLICY_KIND_GC => Some(json!({ "action": "gc", "part_id": c.primary_part_id })),
        POLICY_KIND_MAJOR_COMPACT | POLICY_KIND_MINOR_COMPACT => {
            Some(json!({ "action": "compact", "part_id": c.primary_part_id }))
        }
        POLICY_KIND_REBALANCE => Some(json!({ "action": "rebalance" })),
        POLICY_KIND_REPAIR => {
            if c.secondary_part_id == 0 {
                return None;
            }
            Some(json!({ "action": "repair", "node_id": c.secondary_part_id }))
        }
        _ => None, // hotcold / unknown → advisory only
    }
}

/// Build the `/api/overview` JSON string from the four manager RPC responses.
/// `ov` is taken by value so partitions can be range-sorted in place (the page
/// builds merge adjacency from array order). `ts` is the render epoch-seconds.
pub fn build_overview_json(
    df: &ClusterDfResp,
    mut ov: GetClusterOverviewResp,
    node_states: &ListNodeStatesResp,
    candidates: &[PolicyCandidate],
    extent_health: Option<&ExtentHealthSummaryResp>,
    ts: i64,
) -> String {
    // Range-sort partitions: empty range_start (−∞) first, then bytewise.
    ov.partitions.sort_by(|a, b| {
        (!a.range_start.is_empty(), &a.range_start)
            .cmp(&(!b.range_start.is_empty(), &b.range_start))
    });

    let ns_by_id: HashMap<u64, &NodeStateEntry> =
        node_states.nodes.iter().map(|n| (n.node_id, n)).collect();

    let mut errors: Vec<String> = Vec::new();
    if df.code != CODE_OK {
        errors.push(format!("df: {}", df.message));
    }
    if ov.code != CODE_OK {
        errors.push(format!("overview: {}", ov.message));
    }

    let raw_used = df.raw_total.saturating_sub(df.raw_free);
    // User-visible amplification is raw filesystem capacity consumed per one
    // logical extent byte. Logical size = sealed extents + committed open
    // extents, each de-amplified. A 4+1-only cluster is ~1.25x; RF3 is ~3x.
    let logical_size = df.logical_stored.saturating_add(df.logical_open_tail);
    let amp = raw_capacity_amplification(raw_used, logical_size);
    let disks_json = |n: &NodeCapWire| -> Vec<serde_json::Value> {
        n.disks
            .iter()
            .map(|d| {
                json!({
                    "disk_id": d.disk_id,
                    "uuid": d.uuid,
                    "total": d.total,
                    "free": d.free,
                    "extent_bytes": d.extent_bytes,
                    "reported": d.reported,
                    "online": d.online,
                    "faulted": d.faulted,
                })
            })
            .collect()
    };
    let per_node: Vec<serde_json::Value> = df
        .per_node
        .iter()
        .map(|n: &NodeCapWire| {
            json!({
                "node_id": n.node_id,
                "total": n.total,
                "free": n.free,
                "extent_bytes": n.extent_bytes,
                "online": n.online,
                "disks": disks_json(n),
            })
        })
        .collect();
    let df_json = json!({
        "raw_total": df.raw_total,
        "raw_used": raw_used,
        "raw_free": df.raw_free,
        "physical_used": df.physical_used,
        "logical_stored_sealed": df.logical_stored,
        "logical_open_tail": df.logical_open_tail,
        "logical_size": logical_size,
        // Kept for dashboard API compatibility; same value and definition.
        "logical_footprint": logical_size,
        "logical_wal_debt": df.logical_wal_debt,
        "wal_debt_ratio": if logical_size > 0 {
            df.logical_wal_debt as f64 / logical_size as f64
        } else {
            0.0
        },
        "amplification": amp,
        "node_count_online": df.node_count,
        "per_node": per_node,
    });

    let mut df_by_node: HashMap<u64, &NodeCapWire> = HashMap::new();
    for n in &df.per_node {
        df_by_node.insert(n.node_id, n);
    }
    let nodes: Vec<serde_json::Value> = ov
        .nodes
        .iter()
        .map(|n| {
            let dn = df_by_node.get(&n.node_id);
            let ns = ns_by_id.get(&n.node_id);
            let heartbeat = ns
                .map(|x| x.last_heartbeat_secs_ago)
                .filter(|v| *v != u64::MAX);
            json!({
                "node_id": n.node_id,
                "address": n.address,
                "extent_count": n.extent_count,
                "free": dn.map(|d| d.free),
                "total": dn.map(|d| d.total),
                "extent_bytes": dn.map(|d| d.extent_bytes),
                "online": dn.map(|d| d.online).unwrap_or(false),
                "auto_state": ns.map(|x| node_auto_state_str(x.auto_state)).unwrap_or("Online"),
                "last_heartbeat_secs_ago": heartbeat,
                "suspected_age_secs": ns.map(|x| x.suspected_age_secs),
                "override_kind": ns.map(|x| node_override_kind_str(x.override_kind)).unwrap_or("-"),
                "override_reason": ns.map(|x| x.override_reason.clone()).unwrap_or_default(),
                "override_set_by": ns.map(|x| x.override_set_by.clone()).unwrap_or_default(),
                "override_set_at": ns.map(|x| x.override_set_at).unwrap_or(0),
                "override_expire_at": ns.map(|x| x.override_expire_at).unwrap_or(0),
                // Per-disk rows, from the node's own last df. The node-level
                // total/free above are sums over these and cannot say which
                // disk is full or which one the node calls bad.
                "disks": dn.map(|d| disks_json(d)).unwrap_or_default(),
                "shard_ports": ns.map(|x| x.shard_ports.clone()).unwrap_or_default(),
                "node_uuid": ns.map(|x| x.node_uuid.clone()).unwrap_or_default(),
            })
        })
        .collect();

    // Roll up by PS INSTANCE (ps_id), not per-partition addr.
    let mut ps_roll: HashMap<u64, (String, u64, u64)> = HashMap::new();
    let partitions: Vec<serde_json::Value> = ov
        .partitions
        .iter()
        .map(|p| {
            let entry = ps_roll
                .entry(p.ps_id)
                .or_insert_with(|| (p.ps_addr.clone(), 0, 0));
            entry.1 += 1;
            entry.2 += p.live_size;
            json!({
                "part_id": p.part_id,
                "ps_id": p.ps_id,
                "ps_addr": p.ps_addr,
                "range_start": String::from_utf8_lossy(&p.range_start),
                "range_end": String::from_utf8_lossy(&p.range_end),
                "live_size": p.live_size,
                "total_extents": p.total_extents,
                "log_stream": p.log_stream,
                "row_stream": p.row_stream,
                "meta_stream": p.meta_stream,
                "req_per_sec": p.req_per_sec,
                "write_bytes_per_sec": p.write_bytes_per_sec,
                "read_bytes_per_sec": p.read_bytes_per_sec,
            })
        })
        .collect();
    let mut ps_roll_vec: Vec<serde_json::Value> = ps_roll
        .into_iter()
        .map(|(ps_id, (addr, n, size))| json!({ "ps_id": ps_id, "addr": addr, "n": n, "size": size }))
        .collect();
    ps_roll_vec.sort_by_key(|v| v.get("ps_id").and_then(|x| x.as_u64()).unwrap_or(0));

    // Every REGISTERED partition server, enriched with what the page can only
    // get by walking the partition list. A PS with no partitions has no row
    // there at all, which is exactly the state worth seeing.
    let ps_servers: Vec<serde_json::Value> = ov
        .ps_servers
        .iter()
        .map(|p| {
            let mine = ov.partitions.iter().filter(|x| x.ps_id == p.ps_id);
            let (mut n, mut size, mut iops, mut wr, mut rd, mut ext) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
            for x in mine {
                n += 1;
                size += x.live_size;
                iops += x.req_per_sec;
                wr += x.write_bytes_per_sec;
                rd += x.read_bytes_per_sec;
                ext += x.total_extents as u64;
            }
            json!({
                "ps_id": p.ps_id,
                "addr": p.address,
                // null = no heartbeat entry (defensive — replay and
                // registration both seed one). Never render it as "0 s ago".
                "last_heartbeat_secs_ago": (p.last_heartbeat_secs_ago != u64::MAX)
                    .then_some(p.last_heartbeat_secs_ago),
                "partition_count": p.partition_count,
                // null = no `--cpuset` on that PS, or not heard from since
                // this manager became leader.
                "slot_cap": (p.slot_cap > 0).then_some(p.slot_cap),
                // null = no open-partition report yet (just registered, or
                // this manager just became leader).
                "open_count": p.open_count,
                "ready": p.ready(),
                // Recomputed from the partitions the page is showing, so the
                // count here and the list it drills into cannot disagree.
                "n": n,
                "size": size,
                "req_per_sec": iops,
                "write_bytes_per_sec": wr,
                "read_bytes_per_sec": rd,
                "total_extents": ext,
            })
        })
        .collect();

    let advisories: Vec<serde_json::Value> = candidates
        .iter()
        .map(|c| {
            json!({
                "kind": policy_kind_str(c.kind),
                "primary_part_id": c.primary_part_id,
                "secondary_part_id": c.secondary_part_id,
                "reason": c.reason,
                "desc": describe_candidate(c),
                "action": candidate_to_action(c),
                "key": cooldown_key(c),
            })
        })
        .collect();

    json!({
        "ts": ts,
        "df": df_json,
        "nodes": nodes,
        "partitions": partitions,
        "ps_roll": ps_roll_vec,
        "ps_servers": ps_servers,
        "part_count": ov.partitions.len(),
        "ps_count": ov.ps_count,
        "total_req_per_sec": ov.total_req_per_sec,
        "total_write_bytes_per_sec": ov.total_write_bytes_per_sec,
        "total_read_bytes_per_sec": ov.total_read_bytes_per_sec,
        "advisories": advisories,
        // `null` when the summary could not be read (off-leader, or a manager
        // that predates it): the page then says "unknown", never "healthy".
        "extent_health": extent_health.map(health_json),
        "errors": errors,
    })
    .to_string()
}

#[cfg(test)]
mod capacity_tests {
    use super::{build_overview_json, raw_capacity_amplification};
    use autumn_rpc::manager_rpc::{
        ClusterDfResp, GetClusterOverviewResp, ListNodeStatesResp, CODE_OK,
    };

    #[test]
    fn raw_amplification_matches_ec_replication_and_mixed_layouts() {
        assert_eq!(raw_capacity_amplification(5, 4), 1.25, "4+1 EC");
        assert_eq!(raw_capacity_amplification(3, 1), 3.0, "three replicas");
        assert_eq!(raw_capacity_amplification(17, 8), 2.125, "mixed layout");
        assert_eq!(
            raw_capacity_amplification(123, 0),
            0.0,
            "empty logical size is n/a"
        );
    }

    /// Regression for the dashboard's impossible 0.35x: `physical_used` was
    /// 350 against logical 1000 even though statvfs said raw used was 1250.
    /// Restoring the old numerator makes this assertion read 0.35, not 1.25.
    #[test]
    fn overview_uses_raw_used_not_extent_file_lengths() {
        let df = ClusterDfResp {
            code: CODE_OK,
            message: String::new(),
            raw_total: 2_000,
            raw_free: 750,
            physical_used: 350,
            logical_stored: 900,
            logical_open_tail: 100,
            logical_wal_debt: 0,
            node_count: 5,
            last_update_ms: 0,
            logical_last_update_ms: 0,
            per_node: Vec::new(),
        };
        let overview = GetClusterOverviewResp {
            code: CODE_OK,
            message: String::new(),
            partitions: Vec::new(),
            nodes: Vec::new(),
            total_req_per_sec: 0,
            total_write_bytes_per_sec: 0,
            total_read_bytes_per_sec: 0,
            ps_count: 0,
            ps_servers: Vec::new(),
        };
        let states = ListNodeStatesResp {
            code: CODE_OK,
            message: String::new(),
            nodes: Vec::new(),
        };
        let value: serde_json::Value =
            serde_json::from_str(&build_overview_json(&df, overview, &states, &[], None, 0)).unwrap();
        assert_eq!(value["df"]["raw_used"], 1_250);
        assert_eq!(value["df"]["logical_size"], 1_000);
        assert_eq!(value["df"]["amplification"], 1.25);
    }
}
