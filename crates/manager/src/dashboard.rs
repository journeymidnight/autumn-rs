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

use autumn_rpc::manager_rpc::{CODE_OK, NodeCapWire};

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
            // The flag is plumbed for M2/M3; until those land it has no effect
            // (all mutating endpoints are stubs). Say so rather than let an
            // operator believe the dashboard is armed (coco P3).
            tracing::warn!(
                "F-DASH-IN-MGR: --dashboard-allow-mutations is set, but dashboard \
                 actions + the auto-policy controller are not wired until M2/M3; \
                 no effect yet"
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

        Router::new()
            .route("/", get(index_handler))
            .route("/healthz", get(healthz_handler))
            .route("/api/overview", overview_route)
            // ── stubs (real impls land in later milestones) ──────────────────
            .route("/api/partition/{id}", get(partition_stub))
            .route("/api/policies", get(policies_stub))
            // The page POSTs to these three (Create/activate/delete policy); they
            // MUST return page-shaped JSON, not a bare 404 that breaks its
            // `r.json()` (coco P3). Real controller lands in M2.
            .route("/api/policies/upsert", post(policies_mutate_stub))
            .route("/api/policies/activate", post(policies_mutate_stub))
            .route("/api/policies/delete", post(policies_mutate_stub))
            .route("/api/action", post(action_stub))
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

        let mut errors: Vec<String> = Vec::new();
        if df.code != CODE_OK {
            errors.push(format!("df: {}", df.message));
        }
        if ov.code != CODE_OK {
            errors.push(format!("overview: {}", ov.message));
        }

        // Empirical amplification = physical / logical (matches autumn-op df).
        let amp = if df.logical_stored > 0 {
            df.physical_used as f64 / df.logical_stored as f64
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
                json!({
                    "node_id": n.node_id,
                    "address": n.address,
                    "extent_count": n.extent_count,
                    "free": dn.map(|d| d.free),
                    "total": dn.map(|d| d.total),
                    "extent_bytes": dn.map(|d| d.extent_bytes),
                    "online": dn.map(|d| d.online).unwrap_or(false),
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
            "advisories": [],           // M1
            "errors": errors,
        })
        .to_string()
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

// ── stubs (later milestones) ─────────────────────────────────────────────────

async fn partition_stub(Path(_id): Path<String>) -> Response<Body> {
    json_response(
        StatusCode::OK,
        r#"{"extents":[],"errors":["partition detail not implemented until M1"]}"#.to_string(),
        "application/json",
    )
}

async fn policies_stub() -> Response<Body> {
    json_response(
        StatusCode::OK,
        // Shape the page expects; controller arrives in M2.
        r#"{"enabled":false,"active":null,"allow_mutations":false,"policies":[],"switch_order":["split","ec","compact","gc","merge"],"log":[]}"#.to_string(),
        "application/json",
    )
}

async fn action_stub() -> Response<Body> {
    json_response(
        StatusCode::FORBIDDEN,
        r#"{"ok":false,"error":"mutations not implemented until M3"}"#.to_string(),
        "application/json",
    )
}

/// Shared stub for the three `POST /api/policies/{upsert,activate,delete}` the
/// page fires. Returns valid page-shaped JSON (not a 404) so the page's
/// `r.json()` doesn't throw; the real controller lands in M2.
async fn policies_mutate_stub() -> Response<Body> {
    json_response(
        StatusCode::OK,
        r#"{"error":"policy controller not implemented until M2"}"#.to_string(),
        "application/json",
    )
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
}
