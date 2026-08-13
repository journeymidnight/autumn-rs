//! `/api/overview` JSON compose, extracted so both the (soon-removed) in-manager
//! dashboard and the standalone `autumn-op overview` subcommand emit the IDENTICAL
//! shape the web page's JS consumes. Pure: manager RPC responses in, JSON string
//! out — no `&self`, so `autumn-op` (which already calls the four RPCs) can reuse
//! it verbatim. Keep byte-compatible with the page; a changed key blanks a panel.

use std::collections::HashMap;

use autumn_rpc::manager_rpc::{
    ClusterDfResp, GetClusterOverviewResp, ListNodeStatesResp, NodeCapWire, NodeStateEntry,
    PolicyCandidate, CODE_OK, NODE_AUTO_STATE_ONLINE, NODE_AUTO_STATE_SUSPECTED,
    NODE_AUTO_STATE_SUSPEND, NODE_OVERRIDE_FENCED, NODE_OVERRIDE_MAINTENANCE, POLICY_KIND_EC,
    POLICY_KIND_GC, POLICY_KIND_MAJOR_COMPACT, POLICY_KIND_MERGE, POLICY_KIND_MINOR_COMPACT,
    POLICY_KIND_REBALANCE, POLICY_KIND_SPLIT,
};
use serde_json::json;

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

    // Empirical amplification = physical / logical FOOTPRINT (sealed + open-tail).
    let logical_footprint = df.logical_stored.saturating_add(df.logical_open_tail);
    let amp = if logical_footprint > 0 {
        df.physical_used as f64 / logical_footprint as f64
    } else {
        0.0
    };
    let raw_used = df.raw_total.saturating_sub(df.raw_free);
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
        "logical_footprint": logical_footprint,
        "logical_wal_debt": df.logical_wal_debt,
        "wal_debt_ratio": if logical_footprint > 0 {
            df.logical_wal_debt as f64 / logical_footprint as f64
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
        "part_count": ov.partitions.len(),
        "ps_count": ov.ps_count,
        "total_req_per_sec": ov.total_req_per_sec,
        "total_write_bytes_per_sec": ov.total_write_bytes_per_sec,
        "total_read_bytes_per_sec": ov.total_read_bytes_per_sec,
        "advisories": advisories,
        "errors": errors,
    })
    .to_string()
}
