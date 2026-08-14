//! autumn-rs web dashboard — standalone example app.
//!
//! Serves the single-page UI (`static/index.html`) and proxies every `/api/*`
//! call to the `autumn-op` CLI (`--json`), so the manager wire schema stays in
//! exactly one place and the dashboard needs no direct RPC or `autumn-*` crate
//! dep. The leader-fenced auto-policy CONTROLLER stays in the manager; this app
//! is presentation only.
//!
//! Usage:
//!   autumn-dashboard --manager H:P [--transport tcp|ucx] [--port 8799]
//!                    [--listen 0.0.0.0] [--autumn-op autumn-op]
//!                    (--admin-token TOK | --admin-token-file FILE)
//!
//! The admin token is REQUIRED (the dashboard is token-gated) and forwarded to
//! every `autumn-op` call; read-only ops ignore it, mutations (the Apply buttons
//! and auto-policy) use it.

use std::rc::Rc;

use anyhow::{anyhow, bail, Result};
use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::http::{Response, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use send_wrapper::SendWrapper;

const INDEX_HTML: &str = include_str!("../static/index.html");
const USAGE: &str = "usage: autumn-dashboard --manager H:P [--transport tcp|ucx] \
[--port 8799] [--listen 0.0.0.0] [--autumn-op autumn-op] \
(--admin-token TOK | --admin-token-file FILE)";

struct Config {
    manager: String,
    transport: String,
    autumn_op: String,
    admin_token: String,
}

impl Config {
    /// Run `autumn-op --manager .. --transport .. --admin-token .. --json <args>`
    /// off the async runtime; returns (combined output, success).
    async fn run_op(&self, args: Vec<String>) -> (String, bool) {
        let (manager, transport, bin, token) = (
            self.manager.clone(),
            self.transport.clone(),
            self.autumn_op.clone(),
            self.admin_token.clone(),
        );
        compio::runtime::spawn_blocking(move || {
            let mut cmd = std::process::Command::new(&bin);
            cmd.arg("--manager")
                .arg(&manager)
                .arg("--transport")
                .arg(&transport)
                .arg("--admin-token")
                .arg(&token)
                .arg("--json");
            for a in &args {
                cmd.arg(a);
            }
            match cmd.output() {
                Ok(out) if out.status.success() => {
                    (String::from_utf8_lossy(&out.stdout).into_owned(), true)
                }
                Ok(out) => {
                    let s = String::from_utf8_lossy(&out.stdout);
                    let e = String::from_utf8_lossy(&out.stderr);
                    (format!("{s}{e}").trim().to_string(), false)
                }
                Err(e) => (format!("failed to exec {bin}: {e}"), false),
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
        json_resp(StatusCode::BAD_GATEWAY, serde_json::json!({ "error": out }).to_string())
    }
}

const SWITCH_ORDER: [&str; 6] = ["split", "ec", "compact", "gc", "merge", "rebalance"];

/// `autumn-op auto-policy status --json` speaks its own shape; the page's
/// contract (the one the manager used to serve) differs. Translate: `mode`
/// string → `enabled` bool (+ pass `mode` through for the Off/DryRun/Armed
/// distinction); each policy's `switches` `[bool;6]` → a named object;
/// `interval_sec`/`cooldown_sec` → `interval`/`cooldown`; add `switch_order`.
fn reshape_policies(out: String, ok: bool) -> Response<Body> {
    if !ok {
        return json_resp(StatusCode::BAD_GATEWAY, serde_json::json!({ "error": out }).to_string());
    }
    let v: serde_json::Value = match serde_json::from_str(&out) {
        Ok(v) => v,
        Err(e) => {
            return json_resp(
                StatusCode::BAD_GATEWAY,
                serde_json::json!({ "error": format!("autumn-op status: bad json: {e}") }).to_string(),
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
                            "gc": g(3), "merge": g(4), "rebalance": g(5),
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
        "allow_mutations": v.get("allow_mutations").cloned().unwrap_or_else(|| serde_json::json!(true)),
        "policies": policies,
        "switch_order": SWITCH_ORDER,
        "log": v.get("log").cloned().unwrap_or_else(|| serde_json::json!([])),
    });
    json_resp(StatusCode::OK, body.to_string())
}

/// The controller's currently-active policy name (`""` if none / on error) —
/// so a bare Arm toggle (no explicit policy) can target it.
async fn current_active_policy(cfg: &Config) -> String {
    let (out, ok) = cfg.run_op(vec!["auto-policy".into(), "status".into()]).await;
    if !ok {
        return String::new();
    }
    serde_json::from_str::<serde_json::Value>(&out)
        .ok()
        .and_then(|v| v.get("active").and_then(|x| x.as_str()).map(|s| s.to_string()))
        .unwrap_or_default()
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

async fn partition(cfg: &Config, id: String) -> Response<Body> {
    // Numeric id only — never interpolate a raw path segment into argv.
    let pid: u64 = match id.parse() {
        Ok(x) => x,
        Err(_) => return json_resp(StatusCode::BAD_REQUEST, r#"{"error":"bad partition id"}"#.into()),
    };
    // The page's detail drawer wants ONE flat object: PartitionLoad metrics AND
    // the per-extent list. autumn-op splits these across two views — `--detail`
    // gives the metrics, the plain scoped view gives `extents[]` — so fetch both
    // and merge. (Neither alone renders the drawer completely.)
    let (dout, dok) = cfg
        .run_op(vec!["info".into(), "--part".into(), pid.to_string(), "--detail".into()])
        .await;
    if !dok {
        return json_resp(StatusCode::BAD_GATEWAY, serde_json::json!({ "error": dout }).to_string());
    }
    let mut detail: serde_json::Value = match serde_json::from_str(&dout) {
        Ok(v) => v,
        Err(e) => {
            return json_resp(
                StatusCode::BAD_GATEWAY,
                serde_json::json!({ "error": format!("autumn-op detail: bad json: {e}") }).to_string(),
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
    // Trust boundary: only known verbs, ids validated non-zero, mapped to argv.
    let args: Vec<String> = match action {
        "split" if part > 0 => vec!["split".into(), part.to_string()],
        "gc" if part > 0 => vec!["gc".into(), part.to_string()],
        "compact" if part > 0 => vec!["compact".into(), part.to_string()],
        "merge" if part > 0 && victim > 0 => {
            vec!["merge".into(), part.to_string(), victim.to_string()]
        }
        "force_ec_convert" if extent > 0 => {
            vec!["force-ec-convert".into(), "--extent".into(), extent.to_string()]
        }
        "rebalance" => vec!["rebalance".into()],
        _ => {
            return json_resp(
                StatusCode::BAD_REQUEST,
                r#"{"error":"unknown or incomplete action"}"#.into(),
            )
        }
    };
    let (out, ok) = cfg.run_op(args).await;
    json_resp(
        if ok { StatusCode::OK } else { StatusCode::BAD_GATEWAY },
        serde_json::json!({ "ok": ok, "output": out }).to_string(),
    )
}

async fn policies(cfg: &Config) -> Response<Body> {
    let (out, ok) = cfg.run_op(vec!["auto-policy".into(), "status".into()]).await;
    reshape_policies(out, ok)
}

/// The page drives the controller with two independent-ish keys, which map onto
/// autumn-op's coupled `activate <name> [--arm]` / `deactivate`:
///   `{active:<name>}`            → `activate <name>`  (select, DryRun / observe)
///   `{active:<name>, enabled:t}` → `activate <name> --arm`  (select + Arm)
///   `{enabled:true}`  (no name)  → arm the CURRENT active policy
///   `{enabled:false}`            → `deactivate`  (Off)
async fn policies_activate(cfg: &Config, body: Bytes) -> Response<Body> {
    let v: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return json_resp(StatusCode::BAD_REQUEST, r#"{"error":"bad json"}"#.into()),
    };
    let explicit = v.get("active").and_then(|x| x.as_str()).map(|s| s.to_string());
    let enabled = v.get("enabled").and_then(|x| x.as_bool());
    let args: Vec<String> = if enabled == Some(false) {
        vec!["auto-policy".into(), "deactivate".into()]
    } else {
        // Selecting or arming: resolve the target policy. Explicit `active` wins;
        // a bare Arm toggle falls back to the controller's current active policy.
        let name = match explicit {
            Some(n) => n,
            None => current_active_policy(cfg).await,
        };
        if name.is_empty() {
            return json_resp(
                StatusCode::BAD_REQUEST,
                r#"{"error":"no active policy — select one first"}"#.into(),
            );
        }
        let mut a = vec!["auto-policy".into(), "activate".into(), name];
        if enabled == Some(true) {
            a.push("--arm".into());
        }
        a
    };
    let (out, ok) = cfg.run_op(args).await;
    json_resp(
        if ok { StatusCode::OK } else { StatusCode::BAD_GATEWAY },
        serde_json::json!({ "ok": ok, "output": out }).to_string(),
    )
}

/// The custom-policy editor (upsert/delete) needs `auto-policy upsert`/`delete`
/// subcommands on autumn-op (follow-up); until then surface a clear message so
/// the button never silently no-ops. Preset activate/deactivate already works.
async fn policies_unsupported(_body: Bytes) -> Response<Body> {
    json_resp(
        StatusCode::NOT_IMPLEMENTED,
        r#"{"error":"custom-policy upsert/delete not wired yet — use preset activate/deactivate (autumn-op auto-policy upsert/delete is a follow-up)"}"#.into(),
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
    let mut admin_token: Option<String> = None;
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
            "--admin-token" => {
                i += 1;
                admin_token = Some(req(&raw, i)?);
            }
            "--admin-token-file" => {
                i += 1;
                admin_token = Some(std::fs::read_to_string(req(&raw, i)?)?.trim().to_string());
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => bail!("unknown flag {other:?}\n{USAGE}"),
        }
        i += 1;
    }
    let admin_token = match admin_token {
        Some(t) if !t.is_empty() => t,
        _ => bail!("--admin-token or --admin-token-file is REQUIRED (the dashboard is token-gated)\n{USAGE}"),
    };
    Ok((
        Config {
            manager,
            transport,
            autumn_op,
            admin_token,
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
    let activate_route = post(move |body: Bytes| {
        let c = c.clone();
        SendWrapper::new(async move { policies_activate(&c, body).await })
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/api/overview", overview_route)
        .route("/api/partition/{id}", partition_route)
        .route("/api/action", action_route)
        .route("/api/policies", policies_route)
        .route("/api/policies/activate", activate_route)
        .route("/api/policies/upsert", post(policies_unsupported))
        .route("/api/policies/delete", post(policies_unsupported));

    let listener = compio::net::TcpListener::bind(format!("{listen}:{port}")).await?;
    tracing::info!(
        "autumn-dashboard on http://{listen}:{port}  (manager {}, autumn-op {})",
        cfg.manager,
        cfg.autumn_op
    );
    cyper_axum::serve(listener, app).await?;
    Ok(())
}
