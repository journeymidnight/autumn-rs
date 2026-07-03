//! memory-browser — a web UI + MCP server over autumn-memory's general agent
//! memory: remembered docs (lexical/vector/hybrid search), facts (namespaced KV
//! with TTL), an episodic timeline, and an associative graph of linked memories.
//! A Rust example like `gallery`/`codebase-memory`: Axum on compio + a `--mcp`
//! stdio mode so any agent (Claude, …) can use the same store.
//!
//! Usage:
//!   cargo run -p memory-browser -- [MANAGER] [--tenant T] [--agent A] [--port P]
//!                                  [--reset] [--mcp]
//! Then open http://127.0.0.1:5200 .

use autumn_memory::embed;
mod store;

use std::collections::HashMap;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use autumn_client::AutumnError;
use autumn_memory::MemoryStore;
use axum::body::Body;
use axum::extract::Query;
use axum::http::{Response, StatusCode};
use axum::routing::{delete, get, post, put};
use axum::Router;
use send_wrapper::SendWrapper;
use serde_json::{json, Value};

use embed::Embedder;
use store::Mem;

const LISTEN_PORT: u16 = 5200;

struct App {
    mem: Mem,
    cfg: Value,
}
type Shared = Rc<App>;

enum AppError {
    Mem(AutumnError),
    Bad(String),
    Other(anyhow::Error),
}
impl From<AutumnError> for AppError {
    fn from(e: AutumnError) -> Self {
        AppError::Mem(e)
    }
}
impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::Other(e)
    }
}

fn resp(body: Vec<u8>, ctype: &str, code: StatusCode) -> Response<Body> {
    Response::builder()
        .status(code)
        .header("content-type", ctype)
        .header("cache-control", "no-store")
        .body(Body::from(body))
        .unwrap()
}
fn json_ok(v: &Value) -> Response<Body> {
    resp(serde_json::to_vec(v).unwrap_or_default(), "application/json", StatusCode::OK)
}
fn to_response(e: AppError) -> Response<Body> {
    let (code, msg) = match e {
        AppError::Mem(AutumnError::NotFound) => (StatusCode::NOT_FOUND, "not found".into()),
        AppError::Bad(m) => (StatusCode::BAD_REQUEST, m),
        AppError::Mem(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        AppError::Other(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    resp(serde_json::to_vec(&json!({ "error": msg })).unwrap_or_default(), "application/json", code)
}

type P = HashMap<String, String>;
fn req<'a>(p: &'a P, k: &str) -> Result<&'a str, AppError> {
    p.get(k)
        .map(String::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::Bad(format!("missing query param `{k}`")))
}
fn opt<'a>(p: &'a P, k: &str, d: &'a str) -> &'a str {
    p.get(k).map(String::as_str).filter(|s| !s.is_empty()).unwrap_or(d)
}
fn ttl_opt(p: &P) -> Option<u64> {
    p.get("ttl_secs").and_then(|s| s.parse().ok())
}
fn now_id(prefix: &str) -> String {
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("{prefix}-{ns}")
}

// -- handlers ---------------------------------------------------------------

async fn h_remember(app: &App, p: P, body: String) -> Result<Response<Body>, AppError> {
    let id = opt(&p, "id", "").to_string();
    let id = if id.is_empty() { now_id("mem") } else { id };
    app.mem.remember(&id, &body, p.get("meta").map(String::as_str), ttl_opt(&p)).await?;
    Ok(json_ok(&json!({ "id": id })))
}
async fn h_search(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let q = req(&p, "q")?;
    let mode = opt(&p, "mode", "auto");
    let mode = if mode == "auto" { "hybrid" } else { mode };
    let k: usize = opt(&p, "k", "10").parse().unwrap_or(10);
    Ok(json_ok(&json!({ "hits": app.mem.search(q, mode, k).await? })))
}
async fn h_get_memory(app: &App, p: P) -> Result<Response<Body>, AppError> {
    match app.mem.get_memory(req(&p, "id")?).await? {
        Some(v) => Ok(json_ok(&v)),
        None => Err(AppError::Mem(AutumnError::NotFound)),
    }
}
async fn h_forget(app: &App, p: P) -> Result<Response<Body>, AppError> {
    app.mem.forget(req(&p, "id")?).await?;
    Ok(json_ok(&json!({ "ok": true })))
}

async fn h_put_fact(app: &App, p: P, body: String) -> Result<Response<Body>, AppError> {
    app.mem.put_fact(req(&p, "ns")?, req(&p, "key")?, &body, ttl_opt(&p)).await?;
    Ok(json_ok(&json!({ "ok": true })))
}
async fn h_get_fact(app: &App, p: P) -> Result<Response<Body>, AppError> {
    match app.mem.get_fact(req(&p, "ns")?, req(&p, "key")?).await? {
        Some(v) => Ok(json_ok(&json!({ "value": v }))),
        None => Err(AppError::Mem(AutumnError::NotFound)),
    }
}
async fn h_list_facts(app: &App, p: P) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&json!({ "facts": app.mem.list_facts(req(&p, "ns")?).await? })))
}
async fn h_delete_fact(app: &App, p: P) -> Result<Response<Body>, AppError> {
    app.mem.delete_fact(req(&p, "ns")?, req(&p, "key")?).await?;
    Ok(json_ok(&json!({ "ok": true })))
}

async fn h_event(app: &App, p: P, body: String) -> Result<Response<Body>, AppError> {
    let ts = app.mem.append_event(req(&p, "session")?, &body, ttl_opt(&p)).await?;
    Ok(json_ok(&json!({ "ts": ts })))
}
async fn h_events(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let limit: usize = opt(&p, "limit", "50").parse().unwrap_or(50);
    Ok(json_ok(&json!({ "events": app.mem.recent_events(req(&p, "session")?, limit).await? })))
}
async fn h_replay(app: &App, p: P) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&json!({ "events": app.mem.replay(req(&p, "session")?).await? })))
}

async fn h_link(app: &App, p: P) -> Result<Response<Body>, AppError> {
    app.mem.link(req(&p, "src")?, opt(&p, "type", "RELATED"), req(&p, "dst")?).await?;
    Ok(json_ok(&json!({ "ok": true })))
}
async fn h_neighbors(app: &App, p: P) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&json!({ "symbols": app.mem.neighbors(req(&p, "id")?, opt(&p, "dir", "out")).await? })))
}
async fn h_graph(app: &App) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&app.mem.graph().await?))
}
async fn h_train(app: &App) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&json!({ "centroids": app.mem.train().await? })))
}
async fn h_stats(app: &App) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&app.mem.stats().await?))
}
async fn h_config(app: &App) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&app.cfg))
}
async fn index_handler() -> Response<Body> {
    resp(include_str!("../static/index.html").as_bytes().to_vec(), "text/html; charset=utf-8", StatusCode::OK)
}

fn router(shared: Shared) -> Router {
    macro_rules! q {
        ($h:path) => {{
            let s = SendWrapper::new(shared.clone());
            move |Query(p): Query<P>| {
                let s = s.clone();
                SendWrapper::new(async move { match $h(&s, p).await { Ok(r) => r, Err(e) => to_response(e) } })
            }
        }};
    }
    macro_rules! qb {
        ($h:path) => {{
            let s = SendWrapper::new(shared.clone());
            move |Query(p): Query<P>, body: String| {
                let s = s.clone();
                SendWrapper::new(async move { match $h(&s, p, body).await { Ok(r) => r, Err(e) => to_response(e) } })
            }
        }};
    }
    macro_rules! s {
        ($h:path) => {{
            let s = SendWrapper::new(shared.clone());
            move || {
                let s = s.clone();
                SendWrapper::new(async move { match $h(&s).await { Ok(r) => r, Err(e) => to_response(e) } })
            }
        }};
    }
    Router::new()
        .route("/", get(index_handler))
        .route("/config", get(s!(h_config)))
        .route("/stats", get(s!(h_stats)))
        .route("/graph", get(s!(h_graph)))
        .route("/remember", post(qb!(h_remember)))
        .route("/search", get(q!(h_search)))
        .route("/memory", get(q!(h_get_memory)))
        .route("/memory", delete(q!(h_forget)))
        .route("/fact", put(qb!(h_put_fact)))
        .route("/fact", get(q!(h_get_fact)))
        .route("/fact", delete(q!(h_delete_fact)))
        .route("/facts", get(q!(h_list_facts)))
        .route("/event", post(qb!(h_event)))
        .route("/events", get(q!(h_events)))
        .route("/replay", get(q!(h_replay)))
        .route("/link", post(q!(h_link)))
        .route("/neighbors", get(q!(h_neighbors)))
        .route("/train", post(s!(h_train)))
}

// -- MCP (stdio JSON-RPC) ----------------------------------------------------

fn mcp_tool_defs() -> Value {
    let id = json!({"type":"object","properties":{"id":{"type":"string"}},"required":["id"]});
    json!([
        {"name":"remember","description":"Store a memory (searchable + a graph node). Returns its id.",
         "inputSchema":{"type":"object","properties":{"text":{"type":"string"},"id":{"type":"string"},"meta":{"type":"string"}},"required":["text"]}},
        {"name":"search","description":"Search memories (mode lexical|vector|hybrid|auto).",
         "inputSchema":{"type":"object","properties":{"query":{"type":"string"},"mode":{"type":"string"},"k":{"type":"integer"}},"required":["query"]}},
        {"name":"get_memory","description":"Fetch one memory by id.","inputSchema":id.clone()},
        {"name":"put_fact","description":"Store a namespaced fact with optional TTL (seconds).",
         "inputSchema":{"type":"object","properties":{"namespace":{"type":"string"},"key":{"type":"string"},"value":{"type":"string"},"ttl_secs":{"type":"integer"}},"required":["namespace","key","value"]}},
        {"name":"get_fact","description":"Get a fact.","inputSchema":{"type":"object","properties":{"namespace":{"type":"string"},"key":{"type":"string"}},"required":["namespace","key"]}},
        {"name":"list_facts","description":"List a fact namespace.","inputSchema":{"type":"object","properties":{"namespace":{"type":"string"}},"required":["namespace"]}},
        {"name":"append_event","description":"Append an episodic event to a session log.",
         "inputSchema":{"type":"object","properties":{"session":{"type":"string"},"text":{"type":"string"}},"required":["session","text"]}},
        {"name":"recent_events","description":"Most-recent events for a session (newest-first).",
         "inputSchema":{"type":"object","properties":{"session":{"type":"string"},"limit":{"type":"integer"}},"required":["session"]}},
        {"name":"link","description":"Add a typed edge between two memory ids.",
         "inputSchema":{"type":"object","properties":{"src":{"type":"string"},"type":{"type":"string"},"dst":{"type":"string"}},"required":["src","dst"]}},
        {"name":"neighbors","description":"Linked memories of an id (direction out|in).",
         "inputSchema":{"type":"object","properties":{"id":{"type":"string"},"direction":{"type":"string"}},"required":["id"]}},
        {"name":"trace","description":"Bounded graph walk from an id (direction out|in).",
         "inputSchema":{"type":"object","properties":{"id":{"type":"string"},"direction":{"type":"string"}},"required":["id"]}}
    ])
}

async fn mcp_tool_call(mem: &Mem, params: &Value) -> Result<Value> {
    let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
    let a = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
    let s = |k: &str| a.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let data: Value = match name {
        "remember" => {
            let id = if s("id").is_empty() { now_id("mem") } else { s("id") };
            let meta = a.get("meta").and_then(|v| v.as_str());
            mem.remember(&id, &s("text"), meta, None).await?;
            json!({ "id": id })
        }
        "search" => {
            let mode = match a.get("mode").and_then(|v| v.as_str()).unwrap_or("auto") {
                "auto" => "hybrid",
                m => m,
            };
            let k = a.get("k").and_then(|v| v.as_u64()).unwrap_or(8) as usize;
            json!(mem.search(&s("query"), mode, k).await?)
        }
        "get_memory" => mem.get_memory(&s("id")).await?.unwrap_or(Value::Null),
        "put_fact" => {
            let ttl = a.get("ttl_secs").and_then(|v| v.as_u64());
            mem.put_fact(&s("namespace"), &s("key"), &s("value"), ttl).await?;
            json!({ "ok": true })
        }
        "get_fact" => json!(mem.get_fact(&s("namespace"), &s("key")).await?),
        "list_facts" => json!(mem.list_facts(&s("namespace")).await?),
        "append_event" => json!({ "ts": mem.append_event(&s("session"), &s("text"), None).await? }),
        "recent_events" => {
            let limit = a.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize;
            json!(mem.recent_events(&s("session"), limit).await?)
        }
        "link" => {
            let ty = a.get("type").and_then(|v| v.as_str()).unwrap_or("RELATED");
            mem.link(&s("src"), ty, &s("dst")).await?;
            json!({ "ok": true })
        }
        "neighbors" => json!(mem.neighbors(&s("id"), a.get("direction").and_then(|v| v.as_str()).unwrap_or("out")).await?),
        "trace" => json!(mem.trace(&s("id"), a.get("direction").and_then(|v| v.as_str()).unwrap_or("out")).await?),
        other => return Ok(json!({"content":[{"type":"text","text":format!("unknown tool {other}")}],"isError":true})),
    };
    Ok(json!({"content":[{"type":"text","text": serde_json::to_string(&data)?}]}))
}

async fn run_mcp_stdio(mem: &Mem) -> Result<()> {
    use std::io::Write;
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let Ok(msg): std::result::Result<Value, _> = serde_json::from_str(t) else {
            continue;
        };
        let id = msg.get("id").cloned();
        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let result: Option<Value> = match method {
            "initialize" => Some(json!({"protocolVersion":"2024-11-05","serverInfo":{"name":"memory-browser","version":"0.1.0"},"capabilities":{"tools":{}}})),
            "tools/list" => Some(json!({ "tools": mcp_tool_defs() })),
            "tools/call" => Some(mcp_tool_call(mem, &msg.get("params").cloned().unwrap_or_else(|| json!({}))).await?),
            "ping" => Some(json!({})),
            _ => None,
        };
        if let Some(id) = id {
            let env = match result {
                Some(r) => json!({"jsonrpc":"2.0","id":id,"result":r}),
                None => json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}),
            };
            let mut out = std::io::stdout();
            writeln!(out, "{}", serde_json::to_string(&env)?)?;
            out.flush()?;
        }
    }
    Ok(())
}

// -- args --------------------------------------------------------------------

struct Args {
    manager: String,
    tenant: String,
    agent: String,
    host: String,
    port: u16,
    reset: bool,
    mcp: bool,
    embed_model: Option<String>,
    tokenizer: Option<String>,
}

fn parse_args() -> Args {
    let mut a = Args {
        manager: "127.0.0.1:9001".into(),
        tenant: "memory".into(),
        agent: "default".into(),
        host: "127.0.0.1".into(),
        port: LISTEN_PORT,
        reset: false,
        mcp: false,
        embed_model: None,
        tokenizer: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--tenant" => a.tenant = it.next().unwrap_or(a.tenant),
            "--agent" => a.agent = it.next().unwrap_or(a.agent),
            "--host" => a.host = it.next().unwrap_or(a.host),
            "--port" => a.port = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.port),
            "--reset" => a.reset = true,
            "--mcp" => a.mcp = true,
            "--embed-model" => a.embed_model = it.next(),
            "--tokenizer" => a.tokenizer = it.next(),
            s if !s.starts_with("--") => a.manager = s.to_string(),
            other => tracing::warn!("ignoring unknown arg `{other}`"),
        }
    }
    a
}

fn build_embedder(a: &Args) -> Embedder {
    if let Some(model) = &a.embed_model {
        #[cfg(feature = "static-embed")]
        {
            let tok = a.tokenizer.clone().unwrap_or_else(|| "tokenizer.json".into());
            match embed::StaticTableEmbedder::load(model, &tok) {
                Ok(s) => {
                    tracing::info!("embedder: static-int8 ({model})");
                    return Embedder::Static(s);
                }
                Err(e) => tracing::warn!("static embedder failed ({e}); using hash"),
            }
        }
        #[cfg(not(feature = "static-embed"))]
        tracing::warn!("--embed-model {model} ignored: rebuild with --features static-embed; using hash");
    }
    Embedder::Hash(embed::HashEmbedder)
}

async fn wipe_agent(store: &MemoryStore, tenant: &str, agent: &str) -> Result<usize> {
    let client = store.client();
    let prefix = autumn_memory::keys::agent_prefix(tenant, agent);
    let mut start: Vec<u8> = Vec::new();
    let mut total = 0usize;
    loop {
        let res = client.range(&prefix, &start, 512).await?;
        let n = res.entries.len();
        if n == 0 {
            break;
        }
        let last = res.entries[n - 1].key.clone();
        for e in res.entries {
            client.delete(&e.key).await?;
            total += 1;
        }
        if n < 512 {
            break;
        }
        start = last;
        start.push(0);
    }
    Ok(total)
}

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = parse_args();
    let store = Rc::new(
        MemoryStore::connect(&args.manager, args.tenant.clone(), args.agent.clone())
            .await?
            .with_page_limit(256),
    );
    let emb = Rc::new(build_embedder(&args));
    let mem = Mem {
        store: store.clone(),
        emb: emb.clone(),
        tenant: args.tenant.clone(),
        agent: args.agent.clone(),
    };

    if args.mcp {
        tracing::info!("memory-browser MCP stdio server (tenant={}, agent={})", args.tenant, args.agent);
        return run_mcp_stdio(&mem).await;
    }
    if args.reset {
        let n = wipe_agent(&store, &args.tenant, &args.agent).await?;
        tracing::info!("reset: deleted {n} keys under mem/{}/{}/", args.tenant, args.agent);
    }

    let cfg = json!({
        "tenant": args.tenant, "agent": args.agent, "manager": args.manager,
        "embedder": emb.name(), "dim": emb.dim(), "modes": ["lexical", "vector", "hybrid"],
    });
    let shared: Shared = Rc::new(App { mem, cfg });
    let listener = compio::net::TcpListener::bind(format!("{}:{}", args.host, args.port)).await?;
    tracing::info!("memory-browser → http://{}:{}  (embedder={})", args.host, args.port, emb.name());
    cyper_axum::serve(listener, router(shared)).await?;
    Ok(())
}
