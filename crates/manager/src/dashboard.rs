//! F-DASH-IN-MGR — embedded web dashboard served from the manager process.
//!
//! Folds the retired standalone Python `python/dashboard/` (a browser UI + an
//! auto-policy controller that shelled out to `autumn-op --json`) into the
//! `autumn-manager` binary. All-in-one: one process, and — once M2 lands the
//! controller — the auto-policy loop lives on the leader-fenced manager so it
//! survives as long as the leader does, instead of dying when a separate Python
//! webserver is stopped.
//!
//! **HTTP stack = the same one `examples/gallery` uses** (chosen for one
//! consistent web-serving pattern across the repo): `axum` routes served by the
//! compio-native `cyper_axum::serve` over a `compio::net::TcpListener`. Because
//! axum's handler/state bounds are `Send` but the manager is `!Send`
//! (`Rc`/`RefCell`, single-threaded compio), stateful handlers wrap their
//! captured `AutumnManager` clone and their returned future in `SendWrapper`
//! (safe: everything runs on one thread) — exactly gallery's idiom
//! (main.rs `SendWrapper::new((client.clone(), …))` + `SendWrapper::new(async …)`).
//! The whole surface is 1 embedded page (`include_str!`) + a few JSON endpoints,
//! so `DefaultBodyLimit` is the only hardening layer needed on request bodies.
//!
//! Milestones (see `docs/dashboard_in_manager_plan.md`):
//!   * M0 (this): serve the embedded page + read-only `/api/overview`
//!     (in-process capacity + partitions). Other endpoints are stubs.
//!   * M1: full data parity (`/api/partition/<id>`, advisories, ps_roll).
//!   * M2: leader-fenced auto-policy loop + etcd config + `/api/policies`.
//!   * M3: `--dashboard-allow-mutations` + `/api/action`.
//!
//! **Leader invariant (see plan §4):** the mutating surface + the (M2) policy
//! loop are leader-only. Read endpoints (`/api/overview`) already answer
//! NOT_LEADER-shaped empty data on a follower (the underlying `compute_*` gate
//! on `self.leader`), so pointing a browser at a follower degrades gracefully.
//!
//! **DoS posture (same as gallery, deliberately):** `DefaultBodyLimit` caps
//! request bodies; `cyper_axum::serve` does not expose a connection/header-read
//! timeout, so a slow-header (Slowloris) client is not actively cut off — the
//! compensating controls are the read-only default (mutations gated behind
//! `--dashboard-allow-mutations`) and the docs' guidance to pair network
//! exposure with ACLs. If active Slowloris cut-off is ever required, replace
//! `cyper_axum::serve` with a custom accept loop that bounds each connection.

use std::cell::RefCell;
use std::rc::Rc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path};
use axum::http::{Response, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use send_wrapper::SendWrapper;

use autumn_rpc::manager_rpc::{
    MgrAutoPolicyEntry, NodeCapWire, NodeStateEntry, PolicyCandidate, AUTOPOLICY_OP_DELETE,
    AUTOPOLICY_OP_SET_ACTIVE, AUTOPOLICY_OP_SET_MODE, AUTOPOLICY_OP_UPSERT, CODE_OK,
    NODE_AUTO_STATE_ONLINE, NODE_AUTO_STATE_SUSPECTED, NODE_AUTO_STATE_SUSPEND, NODE_OVERRIDE_FENCED,
    NODE_OVERRIDE_MAINTENANCE, POLICY_KIND_EC, POLICY_KIND_GC, POLICY_KIND_MAJOR_COMPACT,
    POLICY_KIND_MERGE, POLICY_KIND_MINOR_COMPACT, POLICY_KIND_REBALANCE, POLICY_KIND_SPLIT,
};

use crate::auto_policy::{cooldown_key, describe_candidate, policy_kind_str};
use crate::AutumnManager;

/// `(epoch_second, rendered_json)` — a 1-second coalescing cache for
/// `/api/overview` so repeated/concurrent polls reuse one snapshot.
type OverviewCache = Rc<RefCell<Option<(i64, String)>>>;

/// The single-page UI, embedded at compile time (byte-for-byte the retired
/// Python page). Its JS fetches only relative `/api/*` paths, so no edits are
/// needed to point it at the manager.
const DASHBOARD_HTML: &str = include_str!("dashboard_web.html");

/// Request-body cap wired via `DefaultBodyLimit`. NOTE: this only takes effect
/// once a handler EXTRACTS the body (`Json`/`Bytes`/…). The M0 POST stubs read
/// no body, so a large body to them is simply never buffered by hyper (not
/// actively 413'd). M3's `/api/action` extracts `{cmd:[…]}`, at which point this
/// limit rejects oversized bodies with 413 (coco P3).
const MAX_BODY_BYTES: usize = 64 * 1024;

impl AutumnManager {
    /// Spawn the embedded dashboard HTTP server under `spawn_supervised`
    /// (F228). Called from `autumn-manager-server` main when `--dashboard-port`
    /// is set, BEFORE the blocking `serve()`. Binding is done inside the
    /// supervised body so a transient bind failure (e.g. a killed predecessor's
    /// TIME_WAIT) self-heals on the 1 s restart instead of disabling the
    /// dashboard for the process lifetime.
    pub fn start_dashboard(&self, listen_host: String, port: u16, allow_mutations: bool) {
        let mgr = self.clone();
        Self::spawn_supervised("dashboard_http", move || {
            mgr.clone()
                .dashboard_run(listen_host.clone(), port, allow_mutations)
        });
    }

    /// Bind + serve. On bind or serve failure logs + returns (spawn_supervised
    /// retries + rebinds in 1 s).
    async fn dashboard_run(self, host: String, port: u16, allow_mutations: bool) {
        // Bracket a bare IPv6 host; IPv4 / "0.0.0.0" / hostnames pass through
        // (same rule as metrics_http).
        let hostb = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host.clone()
        };
        let bind = format!("{hostb}:{port}");
        let listener = match compio::net::TcpListener::bind(&*bind).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(bind = %bind, error = %e, "dashboard bind failed; retrying");
                // Avoid a hot rebind-fail loop while the port is genuinely taken.
                compio::time::sleep(std::time::Duration::from_secs(5)).await;
                return;
            }
        };
        tracing::info!(
            bind = %bind,
            mutations = allow_mutations,
            "F-DASH-IN-MGR: embedded dashboard up (open http://{bind}/ )"
        );
        if allow_mutations {
            // Armed: manual /api/action AND the auto-policy controller (once a
            // policy is activated + Armed) can mutate the cluster. Surface it as
            // a security reminder — this port has no per-request auth.
            tracing::warn!(
                "F-DASH-IN-MGR: --dashboard-allow-mutations is SET — the dashboard's \
                 manual actions AND the auto-policy controller (when armed) can mutate \
                 the cluster (split/merge/gc/compact/ec). Keep this port behind a \
                 trusted network."
            );
        }
        let app = self.dashboard_router(allow_mutations);
        if let Err(e) = cyper_axum::serve(listener, app).await {
            tracing::error!(error = %e, "dashboard serve loop ended; restarting");
        }
    }

    /// Build the axum router. Stateful routes capture a `SendWrapper<AutumnManager>`
    /// and return a `SendWrapper` future (gallery idiom) so they satisfy axum's
    /// `Send` bound over the `!Send` manager. Stateless handlers (page / healthz
    /// / stubs) are plain `async fn`.
    fn dashboard_router(&self, _allow_mutations: bool) -> Router {
        // Read-only overview: needs in-process store/policy/cap state. The
        // 1-second cache bounds the O(extents+partitions) compute to at most
        // once per wall-clock second regardless of how many browsers poll
        // (coco P2) — the metrics endpoint uses the same snapshot idea.
        let cache: OverviewCache = Rc::new(RefCell::new(None));
        let state = SendWrapper::new((self.clone(), cache));
        let overview_route = get(move || {
            let state = state.clone();
            SendWrapper::new(async move {
                let (mgr, cache) = (&state.0, &state.1);
                overview_cached(mgr, cache)
            })
        });

        // Per-partition detail drawer (on-demand when a partition is expanded).
        let mgr_pd = SendWrapper::new(self.clone());
        let partition_route = get(move |Path(id): Path<String>| {
            let mgr = mgr_pd.clone();
            SendWrapper::new(async move { partition_detail_response(&mgr, &id).await })
        });

        // ── auto-policy controller (M2) ──────────────────────────────────
        // GET is always allowed; the POST mutations require
        // --dashboard-allow-mutations (checked inside each handler).
        let mgr_pg = SendWrapper::new(self.clone());
        let policies_route = get(move || {
            let mgr = mgr_pg.clone();
            SendWrapper::new(async move { policies_get_response(&mgr) })
        });
        let mgr_pa = SendWrapper::new(self.clone());
        let policies_activate = post(move |body: axum::body::Bytes| {
            let mgr = mgr_pa.clone();
            SendWrapper::new(async move { policies_activate_response(&mgr, &body).await })
        });
        let mgr_pu = SendWrapper::new(self.clone());
        let policies_upsert = post(move |body: axum::body::Bytes| {
            let mgr = mgr_pu.clone();
            SendWrapper::new(async move { policies_upsert_response(&mgr, &body).await })
        });
        let mgr_px = SendWrapper::new(self.clone());
        let policies_delete = post(move |body: axum::body::Bytes| {
            let mgr = mgr_px.clone();
            SendWrapper::new(async move { policies_delete_response(&mgr, &body).await })
        });

        // Manual per-target actions + advisory Apply (M3).
        let mgr_action = SendWrapper::new(self.clone());
        let action_route = post(move |body: axum::body::Bytes| {
            let mgr = mgr_action.clone();
            SendWrapper::new(async move { action_response(&mgr, &body).await })
        });

        Router::new()
            .route("/", get(index_handler))
            .route("/healthz", get(healthz_handler))
            .route("/api/overview", overview_route)
            .route("/api/partition/{id}", partition_route)
            .route("/api/policies", policies_route)
            .route("/api/policies/activate", policies_activate)
            .route("/api/policies/upsert", policies_upsert)
            .route("/api/policies/delete", policies_delete)
            .route("/api/action", action_route)
            .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
    }

    /// Build the `/api/overview` JSON, byte-compatible with the retired Python
    /// `build_overview` contract the embedded page consumes. M0 populates
    /// capacity (`df`), node rows, partitions, ps rollup, and totals from
    /// in-process state; `advisories` is filled in M1 (needs the ported
    /// `describe_candidate`/`candidate_to_cmd` helpers).
    fn overview_json(&self) -> String {
        use serde_json::json;

        let df = self.compute_cluster_df_resp();
        let mut ov = self.compute_cluster_overview_resp();
        let ts = Self::epoch_seconds();

        // Range-sort partitions (Python `_range_key`: empty range_start = −∞
        // first, then bytewise). `compute_cluster_overview_resp` iterates
        // `regions` (a BTreeMap keyed by part_id), so post-split the part_id
        // order diverges from key-range order — but the page builds merge
        // adjacency (`NEXT_OF`) from array order, so it MUST be range-sorted.
        ov.partitions.sort_by(|a, b| {
            (!a.range_start.is_empty(), &a.range_start)
                .cmp(&(!b.range_start.is_empty(), &b.range_start))
        });

        // Full per-node state (auto_state + heartbeat + suspected-age +
        // override) so the node-detail drawer has everything the Python
        // `build_overview` merged from `list-nodes` (coco P2).
        let node_states = self.compute_list_node_states_resp();
        let ns_by_id: std::collections::HashMap<u64, &NodeStateEntry> =
            node_states.nodes.iter().map(|n| (n.node_id, n)).collect();

        let mut errors: Vec<String> = Vec::new();
        if df.code != CODE_OK {
            errors.push(format!("df: {}", df.message));
        }
        if ov.code != CODE_OK {
            errors.push(format!("overview: {}", ov.message));
        }

        // Empirical amplification = physical / logical FOOTPRINT (sealed +
        // open-tail; matches autumn-op df). Including open-tail is load-bearing:
        // physical_used counts open-tail bytes (largely live VP/log data), so a
        // sealed-only denominator inflates amp ~15× (F-DF-OPENTAIL).
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
            // F-DF-WALDEBT: dead (GC-reclaimable) bytes incl. open-tail debt.
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

        // Merge node rows: overview (node_id/address/extent_count) + df.per_node
        // (free/total/extent_bytes/online). `auto_state` (from list-nodes) is
        // added in M1; the page tolerates its absence.
        let mut df_by_node: std::collections::HashMap<u64, &NodeCapWire> =
            std::collections::HashMap::new();
        for n in &df.per_node {
            df_by_node.insert(n.node_id, n);
        }
        let nodes: Vec<serde_json::Value> = ov
            .nodes
            .iter()
            .map(|n| {
                let dn = df_by_node.get(&n.node_id);
                let ns = ns_by_id.get(&n.node_id);
                // u64::MAX = "never heartbeated" → null so the page shows "-".
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

        // Roll up by PS INSTANCE (ps_id), not per-partition addr (F099-K gives
        // each partition its own listener, so addr-grouping over-counts PS).
        let mut ps_roll: std::collections::HashMap<u64, (String, u64, u64)> =
            std::collections::HashMap::new();
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
            .map(|(ps_id, (addr, n, size))| {
                json!({ "ps_id": ps_id, "addr": addr, "n": n, "size": size })
            })
            .collect();
        ps_roll_vec.sort_by_key(|v| v.get("ps_id").and_then(|x| x.as_u64()).unwrap_or(0));

        // Pending policy advisories (leader-only `advisory_cache`), rendered
        // through the M1-ported helpers — M2's controller decides on the SAME
        // fns. `cmd` is null for advisory-only kinds (hotcold).
        let advisories: Vec<serde_json::Value> = self
            .policy
            .borrow()
            .advisory_cache
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

    /// Build the `/api/partition/<id>` JSON (the per-partition detail drawer),
    /// byte-compatible with the Python `partition_detail` contract: load metrics
    /// from the cached `PartitionLoad` + topology/extents read directly from the
    /// store (the partition's 3 streams). Cheap (O(this partition's extents)) —
    /// no cache needed (called on-demand when one partition is expanded).
    /// Sync render (no EN probe) — used by tests. Open extents show the
    /// manager's `sealed_length` (0). `partition_detail_json_live` probes.
    #[cfg(test)]
    fn partition_detail_json(&self, pid: u64) -> String {
        self.partition_detail_value(pid).0.to_string()
    }

    /// Async render that PROBES open extents for their live length. The
    /// manager only knows SEALED lengths; an open tail (log/row/meta) holds
    /// live WAL/SST bytes whose `sealed_length` is 0, so without this probe
    /// the dashboard shows every open extent as 0B (mirrors the fix already in
    /// `autumn-op info --part`, `EXT_MSG_PROBE_EXTENT`).
    async fn partition_detail_json_live(&self, pid: u64) -> String {
        let (mut value, probes, used_topology_fallback) = self.partition_detail_value(pid);
        let mut probed_extra = 0u64;
        for (idx, eid, addrs) in probes {
            // Try every replica in order; first success wins. Only fall back to
            // 0B (and note it) when EVERY replica probe fails — a single down
            // replica[0] no longer hides a healthy sibling's true length.
            let mut probed = None;
            for addr in &addrs {
                if let Ok(len) = self.probe_extent_on_node(addr, eid).await {
                    probed = Some(len);
                    break;
                }
            }
            match probed {
                Some(len) => {
                    if let Some(sz) = value
                        .get_mut("extents")
                        .and_then(|e| e.get_mut(idx))
                        .and_then(|e| e.get_mut("size"))
                    {
                        probed_extra = probed_extra.saturating_add(len);
                        *sz = serde_json::json!(len);
                    }
                }
                None => {
                    if let Some(errs) = value.get_mut("errors").and_then(|e| e.as_array_mut()) {
                        errs.push(serde_json::json!(format!(
                            "open extent {eid}: all {} replica probe(s) failed (showing 0B)",
                            addrs.len()
                        )));
                    }
                }
            }
        }
        // Add the probed open-extent bytes to the rollup ONLY when size_bytes
        // came from the topology fallback (PS hasn't reported). A PS-reported
        // size already counts open bytes; adding probed_extra there double-counts.
        // Gate on the explicit flag, NOT a `size_bytes == live_size` coincidence
        // (which double-counted whenever the two happened to be equal).
        if used_topology_fallback && probed_extra > 0 {
            if let Some(sb) = value.get("size_bytes").and_then(|v| v.as_u64()) {
                value["size_bytes"] = serde_json::json!(sb.saturating_add(probed_extra));
            }
        }
        if let Some(obj) = value.as_object_mut() {
            obj.remove("live_size_internal");
        }
        value.to_string()
    }

    /// Build the partition-detail JSON `Value` plus the list of OPEN extents
    /// `(extents-array index, extent_id, ALL replica EN addresses)` that need an
    /// EN probe to show their live length, plus `used_topology_fallback` — true
    /// when `size_bytes` came from the topology rollup (the PS hasn't reported a
    /// size), the ONLY case where the probed open-extent bytes should be added
    /// to the rollup. Sync (no I/O).
    fn partition_detail_value(
        &self,
        pid: u64,
    ) -> (serde_json::Value, Vec<(usize, u64, Vec<String>)>, bool) {
        use serde_json::json;

        let detail = self.compute_partition_detail_resp(pid);
        let load = &detail.load;
        let mut errors: Vec<String> = Vec::new();
        if detail.code != CODE_OK {
            errors.push(format!("detail: {}", detail.message));
        }

        let (ps_addr, range_start, range_end, extents, live_size, open_probes) = {
            let s = self.store.inner.borrow();
            match s.regions.get(&pid) {
                Some(r) => {
                    let ps_addr = s
                        .part_addrs
                        .get(&pid)
                        .or_else(|| s.ps_nodes.get(&r.ps_id))
                        .cloned()
                        .unwrap_or_default();
                    let (rs, re) = r
                        .rg
                        .as_ref()
                        .map(|g| (g.start_key.clone(), g.end_key.clone()))
                        .unwrap_or_default();
                    // Distinct extents across the 3 streams (dedup like
                    // `autumn-op info --part` — coco P3), each rendered with the
                    // full `extChip` contract (role/size/open/ec/ec_shape/
                    // replicas/refs/eversion/missing — coco P1). `role` = the
                    // stream it belongs to; `ec_shape` = its stream's K+M.
                    let mut extents: Vec<serde_json::Value> = Vec::new();
                    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
                    let mut live = 0u64;
                    // (index into `extents`, extent_id, EN address) for open,
                    // non-EC extents whose live length must be probed off an EN.
                    let mut open_probes: Vec<(usize, u64, Vec<String>)> = Vec::new();
                    for (role, sid) in [
                        ("log", r.log_stream),
                        ("row", r.row_stream),
                        ("meta", r.meta_stream),
                    ] {
                        let Some(st) = s.streams.get(&sid) else { continue };
                        let (k, m) = (st.ec_data_shard, st.ec_parity_shard);
                        for eid in &st.extent_ids {
                            if !seen.insert(*eid) {
                                continue; // distinct extents only
                            }
                            match s.extents.get(eid) {
                                Some(e) => {
                                    live = live.saturating_add(e.sealed_length);
                                    let replicas: Vec<u64> =
                                        e.replicates.iter().chain(e.parity.iter()).copied().collect();
                                    let ec_shape = if e.ec_converted {
                                        format!("{k}+{m}")
                                    } else {
                                        String::new()
                                    };
                                    // Open, non-EC extent: sealed_length is 0 but
                                    // it holds live bytes — record it for an EN probe.
                                    if !e.sealed && !e.ec_converted {
                                        // Record EVERY replica addr (not just the
                                        // first): if replica[0]'s EN is down but a
                                        // sibling is healthy, the open extent still
                                        // shows its true length instead of 0B
                                        // (degraded-replica robustness).
                                        let addrs: Vec<String> = e
                                            .replicates
                                            .iter()
                                            .filter_map(|nid| {
                                                s.nodes.get(nid).map(|n| n.address.clone())
                                            })
                                            .collect();
                                        if !addrs.is_empty() {
                                            open_probes.push((extents.len(), *eid, addrs));
                                        }
                                    }
                                    extents.push(json!({
                                        "extent_id": eid,
                                        "role": role,
                                        "size": e.sealed_length,
                                        "open": !e.sealed,
                                        "ec": e.ec_converted,
                                        "ec_shape": ec_shape,
                                        "replicas": replicas,
                                        "refs": e.refs,
                                        "eversion": e.eversion,
                                        "missing": false,
                                    }));
                                }
                                None => {
                                    // Referenced by the stream but absent from
                                    // `extents` — surface it, don't hide it.
                                    extents.push(json!({
                                        "extent_id": eid,
                                        "role": role,
                                        "size": 0,
                                        "open": false,
                                        "ec": false,
                                        "ec_shape": "",
                                        "replicas": [],
                                        "refs": 0,
                                        "eversion": 0,
                                        "missing": true,
                                    }));
                                }
                            }
                        }
                    }
                    (ps_addr, rs, re, extents, live, open_probes)
                }
                None => {
                    errors.push(format!("partition {pid} not found"));
                    (String::new(), Vec::new(), Vec::new(), Vec::new(), 0u64, Vec::new())
                }
            }
        };

        // Reported size when the PS has flushed; else the topology rollup.
        let used_topology_fallback = load.size_bytes == 0;
        let size_bytes = if load.size_bytes > 0 { load.size_bytes } else { live_size };
        let value = json!({
            "part_id": pid,
            "ps_addr": ps_addr,
            "range_start": String::from_utf8_lossy(&range_start),
            "range_end": String::from_utf8_lossy(&range_end),
            "req_per_sec": load.req_per_sec,
            "write_bytes_per_sec": load.write_bytes_per_sec,
            "read_bytes_per_sec": load.read_bytes_per_sec,
            "p99_us": load.p99_us,
            "size_bytes": size_bytes,
            // Internal helper for `partition_detail_json_live` to know whether
            // size_bytes came from the topology rollup (probeable) or the PS
            // report; removed before the response is serialized.
            "live_size_internal": live_size,
            "gc_debt_bytes": load.gc_debt_bytes,
            "pending_compaction_bytes": load.pending_compaction_bytes,
            "gc_inflight": load.gc_inflight != 0,
            "compact_inflight": load.compact_inflight != 0,
            "sealed_log_extent_count": load.sealed_log_extent_count,
            "extents": extents,
            "errors": errors,
        });
        (value, open_probes, used_topology_fallback)
    }
}

// ── axum handlers ────────────────────────────────────────────────────────────

async fn index_handler() -> Response<Body> {
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .header("cache-control", "no-cache")
        .body(Body::from(DASHBOARD_HTML))
        .unwrap()
}

async fn healthz_handler() -> Response<Body> {
    json_response(StatusCode::OK, "ok".to_string(), "text/plain")
}

/// Stateful `/api/overview` with a 1-second coalescing cache: recompute the
/// snapshot at most once per wall-clock second, else return the cached body
/// (coco P2). No RefCell borrow is held across the recompute.
fn overview_cached(mgr: &AutumnManager, cache: &OverviewCache) -> Response<Body> {
    let now = AutumnManager::epoch_seconds();
    {
        let c = cache.borrow();
        if let Some((sec, json)) = c.as_ref() {
            if *sec == now {
                return json_response(StatusCode::OK, json.clone(), "application/json");
            }
        }
    }
    let json = mgr.overview_json();
    *cache.borrow_mut() = Some((now, json.clone()));
    json_response(StatusCode::OK, json, "application/json")
}

/// `/api/partition/<id>` — parse the id, then render the detail JSON. A bad id
/// returns page-shaped JSON (not axum's default text 400) so `r.json()` holds.
async fn partition_detail_response(mgr: &AutumnManager, id: &str) -> Response<Body> {
    match id.parse::<u64>() {
        Ok(pid) => json_response(
            StatusCode::OK,
            mgr.partition_detail_json_live(pid).await,
            "application/json",
        ),
        Err(_) => json_response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"bad partition id","extents":[]}"#.to_string(),
            "application/json",
        ),
    }
}

/// Node auto-state byte → the string the page shows (matches autumn-op's
/// `auto_state_str`).
fn node_auto_state_str(b: u8) -> &'static str {
    match b {
        NODE_AUTO_STATE_ONLINE => "Online",
        NODE_AUTO_STATE_SUSPECTED => "Suspected",
        NODE_AUTO_STATE_SUSPEND => "Suspend",
        _ => "Online",
    }
}

/// Override-kind byte → the page string (`"-"` = no override; matches
/// autumn-op's `override_kind_str`).
fn node_override_kind_str(b: u8) -> &'static str {
    match b {
        NODE_OVERRIDE_FENCED => "fenced",
        NODE_OVERRIDE_MAINTENANCE => "maintenance",
        _ => "-",
    }
}

// ── stubs (later milestones) ─────────────────────────────────────────────────

/// `GET /api/policies` — the controller state, byte-compatible with the Python
/// `AutoPolicy.state()` contract the page consumes: `enabled` = mode != Off,
/// each policy's `switches` as a `{split,ec,compact,gc,merge}` dict, and
/// `interval`/`cooldown`/`max_actions`.
fn policies_get_response(mgr: &AutumnManager) -> Response<Body> {
    use serde_json::json;
    // Leader-only, like the MSG_AUTOPOLICY_GET handler: a follower's controller
    // state is replay-stale and its loop doesn't run (coco P2).
    if !mgr.is_leader() {
        let body = json!({
            "enabled": false, "active": "", "allow_mutations": false,
            "policies": [], "switch_order": ["split", "ec", "compact", "gc", "merge", "rebalance"],
            "log": [], "error": "not leader — point the dashboard at the leader manager",
        });
        return json_response(StatusCode::OK, body.to_string(), "application/json");
    }
    let snap = mgr.autopolicy_snapshot();
    let policies: Vec<serde_json::Value> = snap
        .policies
        .iter()
        .map(|p| {
            json!({
                "name": p.name,
                "desc": p.desc,
                "builtin": p.builtin,
                "interval": p.interval_sec,
                "cooldown": p.cooldown_sec,
                "max_actions": p.max_actions,
                "switches": switches_to_dict(&p.switches),
            })
        })
        .collect();
    let log: Vec<serde_json::Value> = snap
        .log
        .iter()
        .map(|l| json!({ "ts": l.ts, "level": l.level, "msg": l.msg }))
        .collect();
    let body = json!({
        "enabled": snap.mode != 0,          // 0 = Off
        "active": snap.active,
        "allow_mutations": snap.allow_mutations,
        "policies": policies,
        "switch_order": ["split", "ec", "compact", "gc", "merge", "rebalance"],
        "log": log,
    });
    json_response(StatusCode::OK, body.to_string(), "application/json")
}

/// [split, ec, compact, gc, merge, rebalance] Vec → the `{split,ec,…}` dict the page reads.
fn switches_to_dict(sw: &[bool]) -> serde_json::Value {
    let g = |i: usize| sw.get(i).copied().unwrap_or(false);
    serde_json::json!({
        "split": g(0), "ec": g(1), "compact": g(2), "gc": g(3), "merge": g(4), "rebalance": g(5),
    })
}

/// Map an advisory candidate to the STRUCTURED `/api/action` payload the page's
/// `Apply` button sends (or `null` for advisory-only / no target). ec →
/// `force_ec_convert` on the extent (`secondary_part_id`); merge = survivor
/// (`primary`) + victim (`secondary`); major/minor → `compact`.
fn candidate_to_action(c: &PolicyCandidate) -> Option<serde_json::Value> {
    use serde_json::json;
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
        // F-REGION-REBALANCE Phase B: cluster-scoped, no target id.
        POLICY_KIND_REBALANCE => Some(json!({ "action": "rebalance" })),
        _ => None, // hotcold / unknown → advisory only
    }
}

/// Backend trust boundary: the action must be one of the known verbs and carry
/// its required typed fields. Never trust the client for a mutation.
fn validate_action(
    action: &str,
    part_id: u64,
    victim_part_id: u64,
    extent_id: u64,
    extent_ids: &[u64],
) -> Result<(), String> {
    // part_id / extent_id == 0 means the field was absent (ids are ≥ 1).
    match action {
        "split" | "gc" | "compact" => {
            if part_id == 0 {
                return Err(format!("{action} requires part_id"));
            }
        }
        "merge" => {
            if part_id == 0 || victim_part_id == 0 {
                return Err("merge requires part_id + victim_part_id".to_string());
            }
            if part_id == victim_part_id {
                return Err("merge survivor and victim must differ".to_string());
            }
        }
        "forcegc" => {
            if part_id == 0 {
                return Err("forcegc requires part_id".to_string());
            }
            if extent_ids.is_empty() {
                return Err("forcegc requires a non-empty extent_ids".to_string());
            }
        }
        "force_ec_convert" => {
            if extent_id == 0 {
                return Err("force_ec_convert requires extent_id".to_string());
            }
        }
        // F-REGION-REBALANCE Phase B: cluster-scoped, takes no typed fields.
        // REJECT target fields (coco P3) so a caller can't POST
        // `{"action":"rebalance","part_id":7}` and wrongly believe it scoped the
        // rebalance to one partition — it always rebalances the whole cluster.
        "rebalance" => {
            if part_id != 0 || victim_part_id != 0 || extent_id != 0 || !extent_ids.is_empty() {
                return Err(
                    "rebalance is cluster-scoped and takes no target fields \
                     (part_id / victim_part_id / extent_id / extent_ids)"
                        .to_string(),
                );
            }
        }
        "" => return Err("missing action".to_string()),
        _ => return Err(format!("action '{action}' not allowed")),
    }
    Ok(())
}

/// `POST /api/action` — actuate a whitelisted cluster op from a STRUCTURED body
/// (`{"action":"split","part_id":7}`; see the per-action fields in
/// `validate_action`). Gated by `--dashboard-allow-mutations`; validated;
/// dispatched in-process to the same ops as the controller loop (no CLI string,
/// no subprocess).
async fn action_response(mgr: &AutumnManager, body: &[u8]) -> Response<Body> {
    if !mgr.dashboard_allow_mutations.get() {
        return json_response(
            StatusCode::FORBIDDEN,
            r#"{"ok":false,"error":"server is read-only; relaunch the manager with --dashboard-allow-mutations"}"#.to_string(),
            "application/json",
        );
    }
    // Leader-only (coco P1): a follower must never dispatch cluster mutations
    // (its metadata is replay-stale). actuate_action backstops this too.
    if !mgr.is_leader() {
        return json_response(
            StatusCode::FORBIDDEN,
            r#"{"ok":false,"error":"not leader — point the dashboard at the leader manager"}"#.to_string(),
            "application/json",
        );
    }
    let v: serde_json::Value = serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);
    let action = v.get("action").and_then(|x| x.as_str()).unwrap_or("");
    let part_id = v.get("part_id").and_then(|x| x.as_u64()).unwrap_or(0);
    let victim_part_id = v.get("victim_part_id").and_then(|x| x.as_u64()).unwrap_or(0);
    let extent_id = v.get("extent_id").and_then(|x| x.as_u64()).unwrap_or(0);
    // Strict extent_ids: reject any non-positive-integer element rather than
    // silently dropping it (coco P3) — a partial forcegc would mislead the operator.
    let mut extent_ids: Vec<u64> = Vec::new();
    if let Some(arr) = v.get("extent_ids").and_then(|x| x.as_array()) {
        for (i, x) in arr.iter().enumerate() {
            match x.as_u64() {
                Some(id) if id > 0 => extent_ids.push(id),
                _ => {
                    let body = serde_json::json!({
                        "ok": false,
                        "error": format!("extent_ids[{i}] is not a positive integer"),
                    })
                    .to_string();
                    return json_response(StatusCode::BAD_REQUEST, body, "application/json");
                }
            }
        }
    }

    // Cluster-scoped rebalance takes NO target fields. Reject on raw JSON KEY
    // presence (coco P3): the parsed check in `validate_action` folds a
    // wrong-type / explicit-`0` value to 0, so `{"action":"rebalance",
    // "part_id":"7"}` or `"part_id":0` would otherwise slip past it and run a
    // whole-cluster rebalance while the caller believed it was scoped.
    if action == "rebalance" {
        for f in ["part_id", "victim_part_id", "extent_id", "extent_ids"] {
            if v.get(f).is_some() {
                let body = serde_json::json!({
                    "ok": false,
                    "error": format!(
                        "rebalance is cluster-scoped and takes no target fields (got '{f}')"
                    ),
                })
                .to_string();
                return json_response(StatusCode::BAD_REQUEST, body, "application/json");
            }
        }
    }

    if let Err(e) = validate_action(action, part_id, victim_part_id, extent_id, &extent_ids) {
        let body = serde_json::json!({ "ok": false, "error": e }).to_string();
        return json_response(StatusCode::BAD_REQUEST, body, "application/json");
    }
    match mgr
        .actuate_action(action, part_id, victim_part_id, extent_id, extent_ids)
        .await
    {
        Ok(out) => {
            let body = serde_json::json!({ "ok": true, "output": out }).to_string();
            json_response(StatusCode::OK, body, "application/json")
        }
        Err(e) => {
            // A manager refusal (precondition / inflight) is benign — surface it.
            let body = serde_json::json!({ "ok": false, "error": e.to_string() }).to_string();
            json_response(StatusCode::OK, body, "application/json")
        }
    }
}

/// Shared stub for the three `POST /api/policies/{upsert,activate,delete}` the
/// page fires. Returns valid page-shaped JSON (not a 404) so the page's
/// `r.json()` doesn't throw; the real controller lands in M2.
/// `POST /api/policies/activate` — `{active: name}` selects the active policy;
/// `{enabled: bool}` starts (Armed) / stops (Off) the controller. Both keys may
/// be present. Returns the fresh state.
async fn policies_activate_response(mgr: &AutumnManager, body: &[u8]) -> Response<Body> {
    if !mgr.dashboard_allow_mutations.get() {
        return read_only_response();
    }
    let v: serde_json::Value = serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);
    if let Some(active) = v.get("active").and_then(|x| x.as_str()) {
        if let Err(e) = mgr
            .autopolicy_set(AUTOPOLICY_OP_SET_ACTIVE, 0, active.to_string(), None)
            .await
        {
            return error_response(&e.to_string());
        }
    }
    if let Some(enabled) = v.get("enabled").and_then(|x| x.as_bool()) {
        // Start = Armed (2), Stop = Off (0). The loop still degrades Armed→DryRun
        // if --dashboard-allow-mutations is absent, but we already gated on it.
        let mode = if enabled { 2 } else { 0 };
        if let Err(e) = mgr
            .autopolicy_set(AUTOPOLICY_OP_SET_MODE, mode, String::new(), None)
            .await
        {
            return error_response(&e.to_string());
        }
    }
    policies_get_response(mgr)
}

/// `POST /api/policies/upsert` — `{name,
/// switches:{split,ec,compact,gc,merge,rebalance}, interval, cooldown,
/// max_actions}` creates/updates a custom policy.
async fn policies_upsert_response(mgr: &AutumnManager, body: &[u8]) -> Response<Body> {
    if !mgr.dashboard_allow_mutations.get() {
        return read_only_response();
    }
    let v: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return error_response("bad JSON body"),
    };
    let name = v.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let sw = v.get("switches").cloned().unwrap_or(serde_json::Value::Null);
    let b = |k: &str| sw.get(k).and_then(|x| x.as_bool()).unwrap_or(false);
    let entry = MgrAutoPolicyEntry {
        name: name.clone(),
        desc: v
            .get("desc")
            .and_then(|x| x.as_str())
            .unwrap_or("custom policy")
            .to_string(),
        switches: vec![b("split"), b("ec"), b("compact"), b("gc"), b("merge"), b("rebalance")],
        // Clamp BEFORE the u32 cast so a huge value doesn't truncate to 0 (coco
        // P2); autopolicy_set's sanitize_entry re-clamps as the authority.
        interval_sec: v.get("interval").and_then(|x| x.as_u64()).unwrap_or(30).max(2),
        cooldown_sec: v.get("cooldown").and_then(|x| x.as_u64()).unwrap_or(180),
        max_actions: v.get("max_actions").and_then(|x| x.as_u64()).unwrap_or(2).clamp(1, 100) as u32,
        builtin: false,
    };
    match mgr
        .autopolicy_set(AUTOPOLICY_OP_UPSERT, 0, name, Some(entry))
        .await
    {
        Ok(_) => policies_get_response(mgr),
        Err(e) => error_response(&e.to_string()),
    }
}

/// `POST /api/policies/delete` — `{name}` removes a custom policy.
async fn policies_delete_response(mgr: &AutumnManager, body: &[u8]) -> Response<Body> {
    if !mgr.dashboard_allow_mutations.get() {
        return read_only_response();
    }
    let v: serde_json::Value = serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);
    let name = v.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
    match mgr.autopolicy_set(AUTOPOLICY_OP_DELETE, 0, name, None).await {
        Ok(_) => policies_get_response(mgr),
        Err(e) => error_response(&e.to_string()),
    }
}

fn read_only_response() -> Response<Body> {
    json_response(
        StatusCode::FORBIDDEN,
        r#"{"error":"server is read-only; relaunch the manager with --dashboard-allow-mutations"}"#.to_string(),
        "application/json",
    )
}

/// A logical error the page renders as a toast (`r.error`); 200 so the page's
/// `r.json()` reads the body regardless.
fn error_response(msg: &str) -> Response<Body> {
    let body = serde_json::json!({ "error": msg }).to_string();
    json_response(StatusCode::OK, body, "application/json")
}

fn json_response(status: StatusCode, body: String, content_type: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("cache-control", "no-store")
        .body(Body::from(body))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use crate::AutumnManager;

    #[test]
    fn overview_json_is_valid_and_has_contract_keys() {
        // Memory mode → leader=true, so compute_* return CODE_OK with the
        // (empty) fresh-cluster state.
        let mgr = AutumnManager::new();
        let s = mgr.overview_json();
        let v: serde_json::Value = serde_json::from_str(&s).expect("overview must be valid JSON");
        for k in [
            "ts",
            "df",
            "nodes",
            "partitions",
            "ps_roll",
            "part_count",
            "ps_count",
            "total_req_per_sec",
            "total_write_bytes_per_sec",
            "total_read_bytes_per_sec",
            "advisories",
            "errors",
        ] {
            assert!(v.get(k).is_some(), "overview JSON missing key `{k}`");
        }
        // df sub-object contract the page reads.
        for k in ["raw_total", "raw_used", "raw_free", "amplification"] {
            assert!(v["df"].get(k).is_some(), "df JSON missing key `{k}`");
        }
        assert!(v["errors"].is_array());
    }

    #[test]
    fn partition_detail_json_renders_full_extent_contract() {
        use autumn_rpc::manager_rpc::{MgrExtentInfo, MgrRange, MgrRegionInfo, MgrStreamInfo};
        let mgr = AutumnManager::new(); // memory mode → leader=true
        {
            let mut s = mgr.store.inner.borrow_mut();
            s.regions.insert(
                1,
                MgrRegionInfo {
                    rg: Some(MgrRange {
                        start_key: b"a".to_vec(),
                        end_key: b"z".to_vec(),
                    }),
                    part_id: 1,
                    ps_id: 7,
                    log_stream: 10,
                    row_stream: 11,
                    meta_stream: 12,
                    region_epoch: 1,
                },
            );
            s.part_addrs.insert(1, "127.0.0.1:9301".to_string());
            // log stream: an EC-converted sealed extent (100, K+M=3+1) + an open
            // tail (101). Extent 100 is ALSO listed in the row stream to exercise
            // the cross-stream dedup path (must appear ONCE, role="log").
            s.streams.insert(
                10,
                MgrStreamInfo {
                    stream_id: 10,
                    extent_ids: vec![100, 101],
                    ec_data_shard: 3,
                    ec_parity_shard: 1,
                    replicates: 3,
                },
            );
            s.streams.insert(
                11,
                MgrStreamInfo {
                    stream_id: 11,
                    extent_ids: vec![100],
                    ec_data_shard: 0,
                    ec_parity_shard: 0,
                    replicates: 1,
                },
            );
            s.streams.insert(
                12,
                MgrStreamInfo {
                    stream_id: 12,
                    extent_ids: vec![],
                    ec_data_shard: 0,
                    ec_parity_shard: 0,
                    replicates: 1,
                },
            );
            s.extents.insert(
                100,
                MgrExtentInfo {
                    extent_id: 100,
                    replicates: vec![1, 2, 3],
                    parity: vec![4],
                    eversion: 5,
                    refs: 2,
                    vp_table_refs: 0,
                    sealed_length: 8192,
                    sealed: true,
                    avali: 0xF,
                    replicate_disks: vec![],
                    parity_disks: vec![],
                    ec_converted: true,
                },
            );
            s.extents.insert(
                101,
                MgrExtentInfo {
                    extent_id: 101,
                    replicates: vec![1],
                    parity: vec![],
                    eversion: 1,
                    refs: 1,
                    vp_table_refs: 0,
                    sealed_length: 0,
                    sealed: false,
                    avali: 1,
                    replicate_disks: vec![],
                    parity_disks: vec![],
                    ec_converted: false,
                },
            );
        }
        let v: serde_json::Value =
            serde_json::from_str(&mgr.partition_detail_json(1)).expect("valid JSON");
        assert_eq!(v["part_id"], 1);
        assert_eq!(v["ps_addr"], "127.0.0.1:9301");
        assert_eq!(v["range_start"], "a");
        assert_eq!(v["range_end"], "z");
        let exts = v["extents"].as_array().unwrap();
        assert_eq!(exts.len(), 2, "extent 100 shared across streams must dedup");

        let e100 = exts.iter().find(|e| e["extent_id"] == 100).unwrap();
        assert_eq!(e100["role"], "log", "first-seen stream wins");
        assert_eq!(e100["ec"], true);
        assert_eq!(e100["ec_shape"], "3+1");
        assert_eq!(e100["open"], false);
        assert_eq!(e100["size"], 8192);
        assert_eq!(e100["refs"], 2);
        assert_eq!(e100["eversion"], 5);
        assert_eq!(e100["replicas"], serde_json::json!([1, 2, 3, 4])); // replicates ∪ parity
        assert_eq!(e100["missing"], false);

        let e101 = exts.iter().find(|e| e["extent_id"] == 101).unwrap();
        assert_eq!(e101["open"], true);
        assert_eq!(e101["ec"], false);
        assert_eq!(e101["ec_shape"], "");
        assert_eq!(e101["replicas"], serde_json::json!([1]));
    }
}
