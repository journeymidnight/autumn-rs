//! autumn-rs web dashboard — server component.
//!
//! Serves the single-page UI (`static/index.html`) and proxies every `/api/*`
//! call to the `autumn-op` CLI (`--json`), so the manager wire schema stays in
//! exactly one place and the dashboard needs no direct RPC or `autumn-*` crate
//! dep. The leader-fenced auto-policy CONTROLLER stays in the manager; this app
//! exposes the operator controls without owning controller state.
//!
//! Usage:
//!   autumn-dashboard --manager H:P [--transport tcp|ucx] [--port 8799]
//!                    [--listen 0.0.0.0] [--autumn-op autumn-op]
//!                    --cluster-secret-file FILE
//!
//! The cluster secret file is REQUIRED and its path is forwarded to every
//! `autumn-op` call: autumn-op connects as an operator, which the manager
//! refuses without the secret, read-only calls included.
//!
//! It also scrapes every partition server's `/metrics` (the address each PS
//! reports on its heartbeat) on its own, and keeps the history in memory for
//! `/api/metrics` — see `metrics_store.rs`.

mod metrics_store;

use std::cell::RefCell;
use std::io::{Read, Write};
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use autumn_params::dashboard::DashboardParams;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query};
use axum::http::{Response, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use send_wrapper::SendWrapper;
use serde::Deserialize;

use metrics_store::{MetricsStore, PsStatus, Scope};

const INDEX_HTML: &str = include_str!("static/index.html");
fn usage() -> String {
    format!(
        "usage: autumn-dashboard --manager H:P [--transport tcp|ucx] [--port 8799] \
         [--listen 0.0.0.0] [--autumn-op autumn-op] --cluster-secret-file FILE\n\n\
         parameters:\n{}",
        autumn_params::help::<DashboardParams>()
    )
}

/// Dashboard tunables (`autumn_params::dashboard`).
fn params() -> &'static DashboardParams {
    static PARAMS: std::sync::LazyLock<DashboardParams> =
        std::sync::LazyLock::new(DashboardParams::default);
    &PARAMS
}

/// Hard deadline on each `autumn-op` subprocess. Without it a manager that
/// ACCEPTS the connection but never answers hangs the HTTP handler forever
/// and — worse — pins a spawn_blocking thread; enough hung calls exhaust the
/// pool and wedge the whole dashboard. On expiry the child is killed + reaped
/// (no zombie) and the call returns a timeout error, freeing the thread.
/// Generous: read ops finish in ms; mutations only issue the RPC, they don't
/// wait for completion (`--op-timeout-secs`).
fn op_timeout() -> Duration {
    Duration::from_secs(params().op_timeout_secs.get())
}

struct Config {
    manager: String,
    transport: String,
    autumn_op: String,
    cluster_secret_file: String,
    metrics_interval: Duration,
    metrics_retention: Duration,
}

/// How often the poller re-reads the target list (`autumn-op info
/// --file-sd`, `--target-refresh-secs`): a new PS is listed once its heartbeat
/// carried its address.
fn ps_list_refresh() -> Duration {
    Duration::from_secs(params().target_refresh_secs.get())
}
/// One PS scrape: connect + read the whole reply (`--scrape-timeout-secs`).
fn scrape_timeout() -> Duration {
    Duration::from_secs(params().scrape_timeout_secs.get())
}

impl Config {
    /// Run `autumn-op --manager .. --transport .. --cluster-secret-file .. --json <args>`
    /// off the async runtime; returns (combined output, success).
    async fn run_op(&self, args: Vec<String>) -> (String, bool) {
        let (manager, transport, bin, secret_file) = (
            self.manager.clone(),
            self.transport.clone(),
            self.autumn_op.clone(),
            self.cluster_secret_file.clone(),
        );
        compio::runtime::spawn_blocking(move || {
            let mut cmd = std::process::Command::new(&bin);
            cmd.arg("--manager")
                .arg(&manager)
                .arg("--transport")
                .arg(&transport)
                .arg("--cluster-secret-file")
                .arg(&secret_file)
                .arg("--json")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            for a in &args {
                cmd.arg(a);
            }
            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => return (format!("failed to exec {bin}: {e}"), false),
            };
            // Drain stdout/stderr on their own threads: a large reply must never
            // fill the pipe buffer and deadlock the child while we poll for exit.
            let mut so = child.stdout.take().expect("stdout piped");
            let mut se = child.stderr.take().expect("stderr piped");
            let t_out = std::thread::spawn(move || {
                let mut b = Vec::new();
                let _ = so.read_to_end(&mut b);
                b
            });
            let t_err = std::thread::spawn(move || {
                let mut b = Vec::new();
                let _ = se.read_to_end(&mut b);
                b
            });
            // Poll for exit until --op-timeout-secs; on expiry kill + reap (closing the
            // pipes also unblocks the reader threads), so a stuck manager frees
            // this blocking thread instead of pinning it forever.
            let deadline = Instant::now() + op_timeout();
            let status = loop {
                match child.try_wait() {
                    Ok(Some(s)) => break Some(s),
                    Ok(None) => {
                        if Instant::now() >= deadline {
                            let _ = child.kill();
                            let _ = child.wait();
                            break None;
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(_) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        break None;
                    }
                }
            };
            let out = t_out.join().unwrap_or_default();
            let err = t_err.join().unwrap_or_default();
            match status {
                Some(s) if s.success() => (String::from_utf8_lossy(&out).into_owned(), true),
                Some(_) => (
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&out),
                        String::from_utf8_lossy(&err)
                    )
                    .trim()
                    .to_string(),
                    false,
                ),
                None => (
                    format!("autumn-op timed out after {}s", op_timeout().as_secs()),
                    false,
                ),
            }
        })
        .await
        .unwrap_or_else(|_| ("autumn-op subprocess panicked".to_string(), false))
    }
}

fn json_resp(status: StatusCode, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Body::from(body))
        .unwrap()
}

/// Success ⇒ raw autumn-op stdout (already JSON); failure ⇒ `{"error": …}`.
fn passthrough(out: String, ok: bool) -> Response<Body> {
    if ok {
        json_resp(StatusCode::OK, out)
    } else {
        json_resp(
            StatusCode::BAD_GATEWAY,
            serde_json::json!({ "error": out }).to_string(),
        )
    }
}

const SWITCH_ORDER: [&str; 8] =
    ["split", "ec", "compact", "gc", "merge", "rebalance", "repair", "scrub"];

/// `autumn-op auto-policy status --json` speaks its own shape; the page's
/// contract (the one the manager used to serve) differs. Translate: `mode`
/// string → `enabled` bool (+ pass `mode` through); each policy's `switches` `[bool;6]` → a named object;
/// `interval_sec`/`cooldown_sec` → `interval`/`cooldown`; add `switch_order`.
fn reshape_policies(out: String, ok: bool) -> Response<Body> {
    if !ok {
        return json_resp(
            StatusCode::BAD_GATEWAY,
            serde_json::json!({ "error": out }).to_string(),
        );
    }
    let v: serde_json::Value = match serde_json::from_str(&out) {
        Ok(v) => v,
        Err(e) => {
            return json_resp(
                StatusCode::BAD_GATEWAY,
                serde_json::json!({ "error": format!("autumn-op status: bad json: {e}") })
                    .to_string(),
            )
        }
    };
    let mode = v.get("mode").and_then(|x| x.as_str()).unwrap_or("off");
    let policies: Vec<serde_json::Value> = v
        .get("policies")
        .and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .map(|p| {
                    let sw = p.get("switches").and_then(|x| x.as_array());
                    let g = |i: usize| sw.and_then(|a| a.get(i)).and_then(|b| b.as_bool()).unwrap_or(false);
                    serde_json::json!({
                        "name": p.get("name").cloned().unwrap_or(serde_json::Value::Null),
                        "desc": p.get("desc").cloned().unwrap_or(serde_json::Value::Null),
                        "builtin": p.get("builtin").cloned().unwrap_or(serde_json::json!(false)),
                        "interval": p.get("interval_sec").cloned().unwrap_or(serde_json::json!(0)),
                        "cooldown": p.get("cooldown_sec").cloned().unwrap_or(serde_json::json!(0)),
                        "max_actions": p.get("max_actions").cloned().unwrap_or(serde_json::json!(0)),
                        "switches": {
                            "split": g(0), "ec": g(1), "compact": g(2),
                            "gc": g(3), "merge": g(4), "rebalance": g(5), "repair": g(6),
                            "scrub": g(7),
                        },
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let body = serde_json::json!({
        "enabled": mode != "off",
        "mode": mode,
        "active": v.get("active").cloned().unwrap_or_else(|| serde_json::json!("")),
        "allow_mutations": v.get("allow_mutations").cloned().unwrap_or(serde_json::json!(true)),
        "policies": policies,
        "switch_order": SWITCH_ORDER,
    });
    json_resp(StatusCode::OK, body.to_string())
}

/// A policy name becomes a positional CLI argument; leading flags would be
/// interpreted by autumn-op's parser instead of naming a policy.
fn valid_policy_name(name: &str) -> bool {
    !name.trim().is_empty() && !name.starts_with('-')
}

fn bad_request(message: &str) -> Response<Body> {
    json_resp(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"error": message}).to_string(),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivatePolicy {
    active: Option<String>,
    enabled: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyName {
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpsertPolicy {
    name: String,
    switches: std::collections::BTreeMap<String, bool>,
    interval: Option<u64>,
    cooldown: Option<u64>,
    max_actions: Option<u32>,
    desc: Option<String>,
}

async fn index() -> Response<Body> {
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(Body::from(INDEX_HTML))
        .unwrap()
}

async fn overview(cfg: &Config) -> Response<Body> {
    let (out, ok) = cfg.run_op(vec!["overview".into()]).await;
    passthrough(out, ok)
}

#[derive(Deserialize)]
struct MetricsQuery {
    /// `cluster` (default) or `ps`.
    scope: Option<String>,
    id: Option<u64>,
}

/// `/api/metrics?scope=cluster|ps&id=N` — the scraped history, summed
/// over the scope (see `MetricsStore::query`).
fn metrics(store: &RefCell<MetricsStore>, q: MetricsQuery) -> Response<Body> {
    let scope = match (q.scope.as_deref().unwrap_or("cluster"), q.id) {
        ("cluster", _) => Scope::Cluster,
        ("ps", Some(id)) => Scope::Ps(id),
        _ => return bad_request("scope must be cluster, or ps with an id"),
    };
    json_resp(StatusCode::OK, store.borrow().query(&scope).to_string())
}

/// `(ps_id, metrics_addr)` of every live PS with an endpoint, from the
/// Prometheus target list `autumn-op info --file-sd` prints.
async fn ps_metrics_targets(cfg: &Config) -> Result<Vec<(u64, String)>> {
    let (out, ok) = cfg.run_op(vec!["info".into(), "--file-sd".into()]).await;
    if !ok {
        bail!("autumn-op info --file-sd: {out}");
    }
    parse_metrics_targets(&serde_json::from_str(&out)?)
}

fn parse_metrics_targets(file_sd: &serde_json::Value) -> Result<Vec<(u64, String)>> {
    let groups = file_sd.as_array().ok_or_else(|| anyhow!("file_sd is not a list"))?;
    Ok(groups
        .iter()
        .filter(|g| g["labels"]["autumn_role"] == "ps")
        .filter_map(|g| {
            let id = g["labels"]["ps_id"].as_str()?.parse().ok()?;
            Some((id, g["targets"][0].as_str()?.to_string()))
        })
        .collect())
}

/// GET `http://{addr}/metrics`, blocking (run on a blocking thread).
fn scrape(addr: &str) -> Result<String> {
    use std::net::ToSocketAddrs;
    let sa = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("{addr} resolves to nothing"))?;
    let mut conn = std::net::TcpStream::connect_timeout(&sa, scrape_timeout())?;
    conn.set_read_timeout(Some(scrape_timeout()))?;
    conn.set_write_timeout(Some(scrape_timeout()))?;
    write!(conn, "GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n")?;
    let mut reply = String::new();
    conn.read_to_string(&mut reply)?;
    let (head, body) = reply
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow!("malformed HTTP reply"))?;
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        bail!("{}", head.lines().next().unwrap_or(head));
    }
    Ok(body.to_string())
}

/// Fold one PS's scrape of round `t` into the store. A failed scrape leaves
/// the round incomplete for the PS's scopes.
fn fold_scrape(st: &mut MetricsStore, id: u64, t: u64, addr: String, got: Result<String>) {
    let last_ok_ms = st.status(id).and_then(|s| s.last_ok_ms);
    let status = match got {
        Ok(text) => {
            st.ingest(id, t, &text);
            PsStatus { metrics_addr: Some(addr), last_ok_ms: Some(t), error: None }
        }
        Err(e) => {
            st.record_failure(id);
            PsStatus { metrics_addr: Some(addr), last_ok_ms, error: Some(format!("{e:#}")) }
        }
    };
    st.set_status(id, status);
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Scrape every PS once per `metrics_interval`, forever. The target list
/// comes from the manager every `--target-refresh-secs`; a failed refresh keeps
/// the last.
async fn metrics_poller(cfg: Rc<Config>, store: Rc<RefCell<MetricsStore>>) {
    let mut targets: Vec<(u64, String)> = Vec::new();
    let mut next_list = Instant::now();
    loop {
        let round = Instant::now();
        if round >= next_list {
            match ps_metrics_targets(&cfg).await {
                Ok(t) => {
                    targets = t;
                    next_list = round + ps_list_refresh();
                    let ids: Vec<u64> = targets.iter().map(|(id, _)| *id).collect();
                    store.borrow_mut().retain_ps(&ids);
                }
                Err(e) => tracing::warn!("metrics poller: PS list: {e:#}"),
            }
        }
        let t = now_ms();
        store.borrow_mut().begin_round(t);
        let scrapes = targets.iter().map(|(id, addr)| {
            let (id, addr) = (*id, addr.clone());
            async move {
                let a = addr.clone();
                let got = compio::runtime::spawn_blocking(move || scrape(&a))
                    .await
                    .unwrap_or_else(|_| Err(anyhow!("scrape thread panicked")));
                (id, addr, got)
            }
        });
        for (id, addr, got) in futures::future::join_all(scrapes).await {
            fold_scrape(&mut store.borrow_mut(), id, t, addr, got);
        }
        compio::time::sleep(cfg.metrics_interval.saturating_sub(round.elapsed())).await;
    }
}

/// `/api/ops` — one round trip for the ops panel.
///
/// Two sources, deliberately kept apart in the reply because they answer
/// different questions and have different lifetimes: `live` is the leader's
/// in-memory ledger (what is running NOW, with progress), and `history` is the
/// durable etcd log (what finished, with the failure reason). The ledger is a
/// bounded ring that dies with the leader, so an op absent from `live` is not
/// necessarily gone — it is in `history`.
///
/// Both come from the leader over RPC via `autumn-op`; the dashboard never
/// reads a file.
async fn ops(cfg: &Config) -> Response<Body> {
    let arr = |out: String, ok: bool| -> Result<serde_json::Value, String> {
        if !ok {
            return Err(out);
        }
        let v: serde_json::Value =
            serde_json::from_str(&out).map_err(|e| format!("autumn-op ops: bad json: {e}"))?;
        Ok(v.get("ops")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])))
    };

    let (live_out, live_ok) = cfg
        .run_op(vec!["ops".into(), "list".into(), "--active".into()])
        .await;
    let live = match arr(live_out, live_ok) {
        Ok(v) => v,
        Err(e) => {
            return json_resp(
                StatusCode::BAD_GATEWAY,
                serde_json::json!({ "error": e }).to_string(),
            )
        }
    };

    // History is best-effort: a cluster with no durable store still has a live
    // ledger, and an ops panel that shows nothing because the log is
    // unavailable is worse than one that shows what is running.
    let (hist_out, hist_ok) = cfg
        .run_op(vec![
            "ops".into(),
            "history".into(),
            "--limit".into(),
            "50".into(),
        ])
        .await;
    let (history, history_error) = match arr(hist_out, hist_ok) {
        Ok(v) => (v, serde_json::Value::Null),
        Err(e) => (serde_json::json!([]), serde_json::json!(e)),
    };

    json_resp(
        StatusCode::OK,
        serde_json::json!({
            "live": live,
            "history": history,
            "history_error": history_error,
        })
        .to_string(),
    )
}

async fn partition(cfg: &Config, id: String) -> Response<Body> {
    // Numeric id only — never interpolate a raw path segment into argv.
    let pid: u64 = match id.parse() {
        Ok(x) => x,
        Err(_) => {
            return json_resp(
                StatusCode::BAD_REQUEST,
                r#"{"error":"bad partition id"}"#.into(),
            )
        }
    };
    // The page's detail drawer wants ONE flat object: PartitionLoad metrics AND
    // the per-extent list. autumn-op splits these across two views — `--detail`
    // gives the metrics, the plain scoped view gives `extents[]` — so fetch both
    // and merge. (Neither alone renders the drawer completely.)
    let (dout, dok) = cfg
        .run_op(vec![
            "info".into(),
            "--part".into(),
            pid.to_string(),
            "--detail".into(),
        ])
        .await;
    if !dok {
        return json_resp(
            StatusCode::BAD_GATEWAY,
            serde_json::json!({ "error": dout }).to_string(),
        );
    }
    let mut detail: serde_json::Value = match serde_json::from_str(&dout) {
        Ok(v) => v,
        Err(e) => {
            return json_resp(
                StatusCode::BAD_GATEWAY,
                serde_json::json!({ "error": format!("autumn-op detail: bad json: {e}") })
                    .to_string(),
            )
        }
    };
    let (sout, sok) = cfg
        .run_op(vec!["info".into(), "--part".into(), pid.to_string()])
        .await;
    // Best-effort: if the scoped view fails, keep the metrics (the drawer just
    // shows an empty extent list) rather than 502 the whole panel.
    if sok {
        if let Ok(scoped) = serde_json::from_str::<serde_json::Value>(&sout) {
            if let (Some(obj), Some(exts)) = (detail.as_object_mut(), scoped.get("extents")) {
                obj.insert("extents".into(), exts.clone());
            }
        }
    }
    json_resp(StatusCode::OK, detail.to_string())
}

async fn action(cfg: &Config, body: Bytes) -> Response<Body> {
    let v: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return json_resp(StatusCode::BAD_REQUEST, r#"{"error":"bad json"}"#.into()),
    };
    let u64f = |k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
    let action = v.get("action").and_then(|x| x.as_str()).unwrap_or("");
    let (part, victim, extent) = (u64f("part_id"), u64f("victim_part_id"), u64f("extent_id"));
    let node = u64f("node_id");
    // Trust boundary: only known verbs, ids validated non-zero, mapped to argv.
    let args: Vec<String> = match action {
        "split" if part > 0 => vec!["split".into(), part.to_string()],
        "gc" if part > 0 => vec!["gc".into(), part.to_string()],
        "compact" if part > 0 => vec!["compact".into(), part.to_string()],
        "merge" if part > 0 && victim > 0 => {
            vec!["merge".into(), part.to_string(), victim.to_string()]
        }
        "force_ec_convert" if extent > 0 => {
            vec![
                "force-ec-convert".into(),
                "--extent".into(),
                extent.to_string(),
            ]
        }
        "rebalance" => vec!["rebalance".into()],
        // The weekly scrub advisory's action: every sealed copy in the cluster.
        "scrub" => vec!["scrub".into(), "--all".into()],
        // Rebuild on other nodes now: one extent's degraded slots, or every
        // degraded slot on one node.
        "repair" if extent > 0 => vec!["repair".into(), extent.to_string()],
        "repair" if node > 0 => vec!["repair".into(), "--node".into(), node.to_string()],
        "repair_cancel" if extent > 0 => {
            vec!["repair".into(), "--cancel".into(), extent.to_string()]
        }
        "repair_cancel" if node > 0 => vec![
            "repair".into(),
            "--cancel".into(),
            "--node".into(),
            node.to_string(),
        ],
        _ => {
            return json_resp(
                StatusCode::BAD_REQUEST,
                r#"{"error":"unknown or incomplete action"}"#.into(),
            )
        }
    };
    let (out, ok) = cfg.run_op(args).await;
    json_resp(
        if ok {
            StatusCode::OK
        } else {
            StatusCode::BAD_GATEWAY
        },
        serde_json::json!({ "ok": ok, "output": out }).to_string(),
    )
}

async fn policies(cfg: &Config) -> Response<Body> {
    let (out, ok) = cfg
        .run_op(vec!["auto-policy".into(), "status".into()])
        .await;
    reshape_policies(out, ok)
}

/// The page drives the controller with two keys, which map onto autumn-op's
/// `start <name>` / `deactivate`:
///   `{active:<name>}` (`enabled` absent or true) → `start <name>`
///   `{enabled:true}`  (no name)  → start the CURRENT active policy
///   `{enabled:false}`            → `deactivate`  (Off)
async fn policies_activate(cfg: &Config, body: Bytes) -> Response<Body> {
    let request: ActivatePolicy = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request(&e.to_string()),
    };
    if request.active.is_none() && request.enabled.is_none() {
        return bad_request("active or enabled is required");
    }
    if request
        .active
        .as_deref()
        .is_some_and(|n| !valid_policy_name(n))
    {
        return bad_request("invalid policy name");
    }
    let args = if request.enabled == Some(false) {
        vec!["auto-policy".into(), "deactivate".into()]
    } else {
        let name = match request.active {
            Some(n) => n,
            None => {
                let (out, ok) = cfg
                    .run_op(vec!["auto-policy".into(), "status".into()])
                    .await;
                if !ok {
                    return passthrough(out, false);
                }
                let status: serde_json::Value = match serde_json::from_str(&out) {
                    Ok(v) => v,
                    Err(e) => {
                        return passthrough(format!("autumn-op status: bad json: {e}"), false)
                    }
                };
                status
                    .get("active")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            }
        };
        if !valid_policy_name(&name) {
            return bad_request("no valid active policy — select one first");
        }
        vec!["auto-policy".into(), "start".into(), name]
    };
    let (out, ok) = cfg.run_op(args).await;
    json_resp(
        if ok {
            StatusCode::OK
        } else {
            StatusCode::BAD_GATEWAY
        },
        serde_json::json!({ "ok": ok, "output": out }).to_string(),
    )
}

/// `POST /api/policies/upsert` — create/replace a custom policy. The page sends
/// `{name, switches:{split,ec,…}, interval, cooldown, max_actions}`; map it onto
/// `autumn-op auto-policy upsert <name> --switches <csv> --interval N …`.
async fn policies_upsert(cfg: &Config, body: Bytes) -> Response<Body> {
    let request: UpsertPolicy = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request(&e.to_string()),
    };
    if !valid_policy_name(&request.name) {
        return bad_request("invalid policy name");
    }
    if request
        .switches
        .keys()
        .any(|k| !SWITCH_ORDER.contains(&k.as_str()))
    {
        return bad_request("unknown policy switch");
    }
    if request.interval.is_some_and(|n| n < 2)
        || request.max_actions.is_some_and(|n| !(1..=100).contains(&n))
    {
        return bad_request("interval must be >= 2 and max_actions must be in 1..=100");
    }
    let enabled: Vec<&str> = SWITCH_ORDER
        .iter()
        .copied()
        .filter(|k| request.switches.get(*k) == Some(&true))
        .collect();
    let mut args = vec![
        "auto-policy".into(),
        "upsert".into(),
        request.name,
        "--switches".into(),
        enabled.join(","),
    ];
    if let Some(n) = request.interval {
        args.extend(["--interval".into(), n.to_string()]);
    }
    if let Some(n) = request.cooldown {
        args.extend(["--cooldown".into(), n.to_string()]);
    }
    if let Some(n) = request.max_actions {
        args.extend(["--max".into(), n.to_string()]);
    }
    if let Some(desc) = request.desc {
        args.extend(["--desc".into(), desc]);
    }
    let (out, ok) = cfg.run_op(args).await;
    json_resp(
        if ok {
            StatusCode::OK
        } else {
            StatusCode::BAD_GATEWAY
        },
        serde_json::json!({ "ok": ok, "output": out }).to_string(),
    )
}

/// `POST /api/policies/delete` — remove a custom policy: `{name}` →
/// `autumn-op auto-policy delete <name>`.
async fn policies_delete(cfg: &Config, body: Bytes) -> Response<Body> {
    let request: PolicyName = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request(&e.to_string()),
    };
    if !valid_policy_name(&request.name) {
        return bad_request("invalid policy name");
    }
    let (out, ok) = cfg
        .run_op(vec!["auto-policy".into(), "delete".into(), request.name])
        .await;
    json_resp(
        if ok {
            StatusCode::OK
        } else {
            StatusCode::BAD_GATEWAY
        },
        serde_json::json!({ "ok": ok, "output": out }).to_string(),
    )
}

fn req(raw: &[String], i: usize) -> Result<String> {
    raw.get(i)
        .cloned()
        .ok_or_else(|| anyhow!("missing value for {}", raw[i - 1]))
}

fn parse_args() -> Result<(Config, String, u16)> {
    let mut manager = "127.0.0.1:9001".to_string();
    let mut transport = "tcp".to_string();
    let mut listen = "0.0.0.0".to_string();
    let mut port: u16 = 8799;
    let mut autumn_op = "autumn-op".to_string();
    let mut cluster_secret_file: Option<String> = None;
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--manager" => {
                i += 1;
                manager = req(&raw, i)?;
            }
            "--transport" => {
                i += 1;
                transport = req(&raw, i)?;
            }
            "--listen" => {
                i += 1;
                listen = req(&raw, i)?;
            }
            "--port" => {
                i += 1;
                port = req(&raw, i)?.parse()?;
            }
            "--autumn-op" => {
                i += 1;
                autumn_op = req(&raw, i)?;
            }
            "--cluster-secret-file" => {
                i += 1;
                cluster_secret_file = Some(req(&raw, i)?);
            }
            "-h" | "--help" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            other => match autumn_params::take_flag(params(), other, raw.get(i + 1).map(|s| s.as_str()))
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(2);
                }) {
                autumn_params::Flag::Taken => i += 1,
                autumn_params::Flag::Unknown => bail!("unknown flag {other:?}\n{}", usage()),
            },
        }
        i += 1;
    }
    let Some(cluster_secret_file) = cluster_secret_file else {
        bail!("--cluster-secret-file is REQUIRED\n{}", usage())
    };
    // Fail at startup, not on the first click: autumn-op reads it per call.
    std::fs::metadata(&cluster_secret_file)
        .map_err(|e| anyhow!("--cluster-secret-file {cluster_secret_file}: {e}"))?;
    Ok((
        Config {
            manager,
            transport,
            autumn_op,
            cluster_secret_file,
            metrics_interval: Duration::from_secs(params().metrics_interval_secs.get()),
            metrics_retention: Duration::from_secs(params().metrics_retention_secs.get()),
        },
        listen,
        port,
    ))
}

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let (cfg, listen, port) = parse_args()?;
    let cfg = Rc::new(cfg);
    let store = Rc::new(RefCell::new(MetricsStore::new(
        cfg.metrics_interval,
        cfg.metrics_retention,
    )));
    compio::runtime::spawn(metrics_poller(cfg.clone(), store.clone())).detach();

    // Wrap the !Send Rc<Config> in SendWrapper to satisfy axum's Send bound —
    // safe because compio runs everything on one thread (the gallery idiom).
    let c = SendWrapper::new(cfg.clone());
    let overview_route = get(move || {
        let c = c.clone();
        SendWrapper::new(async move { overview(&c).await })
    });
    let c = SendWrapper::new(cfg.clone());
    let partition_route = get(move |Path(id): Path<String>| {
        let c = c.clone();
        SendWrapper::new(async move { partition(&c, id).await })
    });
    let c = SendWrapper::new(cfg.clone());
    let action_route = post(move |body: Bytes| {
        let c = c.clone();
        SendWrapper::new(async move { action(&c, body).await })
    });
    let c = SendWrapper::new(cfg.clone());
    let policies_route = get(move || {
        let c = c.clone();
        SendWrapper::new(async move { policies(&c).await })
    });
    let c = SendWrapper::new(cfg.clone());
    let ops_route = get(move || {
        let c = c.clone();
        SendWrapper::new(async move { ops(&c).await })
    });
    let c = SendWrapper::new(cfg.clone());
    let activate_route = post(move |body: Bytes| {
        let c = c.clone();
        SendWrapper::new(async move { policies_activate(&c, body).await })
    });
    let c = SendWrapper::new(cfg.clone());
    let upsert_route = post(move |body: Bytes| {
        let c = c.clone();
        SendWrapper::new(async move { policies_upsert(&c, body).await })
    });
    let c = SendWrapper::new(cfg.clone());
    let delete_route = post(move |body: Bytes| {
        let c = c.clone();
        SendWrapper::new(async move { policies_delete(&c, body).await })
    });
    let st = SendWrapper::new(store.clone());
    let metrics_route = get(move |Query(q): Query<MetricsQuery>| {
        let st = st.clone();
        SendWrapper::new(async move { metrics(&st, q) })
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/api/overview", overview_route)
        .route("/api/ops", ops_route)
        .route("/api/partition/{id}", partition_route)
        .route("/api/action", action_route)
        .route("/api/policies", policies_route)
        .route("/api/policies/activate", activate_route)
        .route("/api/policies/upsert", upsert_route)
        .route("/api/policies/delete", delete_route)
        .route("/api/metrics", metrics_route);

    let listener = compio::net::TcpListener::bind(format!("{listen}:{port}")).await?;
    tracing::info!(
        "autumn-dashboard on http://{listen}:{port}  (manager {}, autumn-op {})",
        cfg.manager,
        cfg.autumn_op
    );
    cyper_axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{fold_scrape, parse_metrics_targets};
    use crate::metrics_store::{MetricsStore, Scope};
    use anyhow::anyhow;
    use std::time::Duration;

    const TEXT: &str = "# TYPE autumn_ps_sst_tables gauge\nautumn_ps_sst_tables 3\n";

    #[test]
    fn a_failed_scrape_leaves_a_gap_in_the_cluster_view() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        st.begin_round(10_000);
        fold_scrape(&mut st, 1, 10_000, "a:1".into(), Ok(TEXT.into()));
        assert_eq!(st.query(&Scope::Cluster)["series"]["sst_tables"], serde_json::json!([3.0]));
        st.begin_round(20_000);
        fold_scrape(&mut st, 1, 20_000, "a:1".into(), Ok(TEXT.into()));
        fold_scrape(&mut st, 3, 20_000, "c:3".into(), Err(anyhow!("refused")));
        assert_eq!(
            st.query(&Scope::Cluster)["series"]["sst_tables"],
            serde_json::json!([3.0, null])
        );
    }

    #[test]
    fn scrape_targets_are_the_partition_servers_in_the_file_sd_list() {
        let sd = serde_json::json!([
            {"targets": ["10.0.0.1:9801"], "labels": {"autumn_role": "ps", "ps_id": "1"}},
            {"targets": ["10.0.0.7:9601"], "labels": {"autumn_role": "en", "node_id": "7"}},
        ]);
        assert_eq!(parse_metrics_targets(&sd).unwrap(), vec![(1, "10.0.0.1:9801".to_string())]);
    }
}
