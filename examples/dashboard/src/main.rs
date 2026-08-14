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
    let (out, ok) = cfg
        .run_op(vec!["info".into(), "--part".into(), pid.to_string(), "--detail".into()])
        .await;
    passthrough(out, ok)
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
    passthrough(out, ok)
}

async fn policies_activate(cfg: &Config, body: Bytes) -> Response<Body> {
    let v: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return json_resp(StatusCode::BAD_REQUEST, r#"{"error":"bad json"}"#.into()),
    };
    let args = if let Some(name) = v.get("active").and_then(|x| x.as_str()) {
        // activate <name>; the page's Armed toggle rides `enabled`.
        let mut a = vec!["auto-policy".into(), "activate".into(), name.to_string()];
        if v.get("enabled").and_then(|x| x.as_bool()).unwrap_or(false) {
            a.push("--arm".into());
        }
        a
    } else if v.get("enabled").and_then(|x| x.as_bool()) == Some(false) {
        vec!["auto-policy".into(), "deactivate".into()]
    } else {
        return json_resp(
            StatusCode::BAD_REQUEST,
            r#"{"error":"expected {active:<name>} or {enabled:false}"}"#.into(),
        );
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
