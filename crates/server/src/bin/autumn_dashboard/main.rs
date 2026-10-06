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

use std::io::Read;
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::http::{Response, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use send_wrapper::SendWrapper;
use serde::Deserialize;

const INDEX_HTML: &str = include_str!("static/index.html");
const USAGE: &str = "usage: autumn-dashboard --manager H:P [--transport tcp|ucx] \
[--port 8799] [--listen 0.0.0.0] [--autumn-op autumn-op] \
--cluster-secret-file FILE";

/// Hard deadline on each `autumn-op` subprocess. Without it a manager that
/// ACCEPTS the connection but never answers hangs the HTTP handler forever
/// and — worse — pins a spawn_blocking thread; enough hung calls exhaust the
/// pool and wedge the whole dashboard. On expiry the child is killed + reaped
/// (no zombie) and the call returns a timeout error, freeing the thread.
/// Generous: read ops finish in ms; mutations only issue the RPC, they don't
/// wait for completion.
const OP_TIMEOUT: Duration = Duration::from_secs(30);

struct Config {
    manager: String,
    transport: String,
    autumn_op: String,
    cluster_secret_file: String,
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
            // Poll for exit until OP_TIMEOUT; on expiry kill + reap (closing the
            // pipes also unblocks the reader threads), so a stuck manager frees
            // this blocking thread instead of pinning it forever.
            let deadline = Instant::now() + OP_TIMEOUT;
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
                    format!("autumn-op timed out after {}s", OP_TIMEOUT.as_secs()),
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
/// string → `enabled` bool (+ pass `mode` through for the Off/DryRun/Armed
/// distinction); each policy's `switches` `[bool;6]` → a named object;
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
        "log": v.get("log").cloned().unwrap_or_else(|| serde_json::json!([])),
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

/// The page drives the controller with two independent-ish keys, which map onto
/// autumn-op's coupled `activate <name> [--arm]` / `deactivate`:
///   `{active:<name>}`            → `activate <name>`  (select, DryRun / observe)
///   `{active:<name>, enabled:t}` → `activate <name> --arm`  (select + Arm)
///   `{enabled:true}`  (no name)  → arm the CURRENT active policy
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
        let mut args = vec!["auto-policy".into(), "activate".into(), name];
        if request.enabled == Some(true) {
            args.push("--arm".into());
        }
        args
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
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => bail!("unknown flag {other:?}\n{USAGE}"),
        }
        i += 1;
    }
    let Some(cluster_secret_file) = cluster_secret_file else {
        bail!("--cluster-secret-file is REQUIRED\n{USAGE}")
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

    let app = Router::new()
        .route("/", get(index))
        .route("/api/overview", overview_route)
        .route("/api/ops", ops_route)
        .route("/api/partition/{id}", partition_route)
        .route("/api/action", action_route)
        .route("/api/policies", policies_route)
        .route("/api/policies/activate", activate_route)
        .route("/api/policies/upsert", upsert_route)
        .route("/api/policies/delete", delete_route);

    let listener = compio::net::TcpListener::bind(format!("{listen}:{port}")).await?;
    tracing::info!(
        "autumn-dashboard on http://{listen}:{port}  (manager {}, autumn-op {})",
        cfg.manager,
        cfg.autumn_op
    );
    cyper_axum::serve(listener, app).await?;
    Ok(())
}
