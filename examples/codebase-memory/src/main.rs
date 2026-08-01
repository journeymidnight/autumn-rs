//! codebase-memory — index a Rust codebase (autumn-rs itself, by default) into
//! autumn-memory and search it from a browser, with a CALLS graph you can walk.
//! A Rust example (like `gallery`): Axum on the compio runtime over
//! `autumn-memory` directly.
//!
//! Usage:
//!   cargo run -p codebase-memory -- [MANAGER] [--root PATH]
//!       [--tenant T] [--agent A] [--host H] [--port P] [--no-index]
//!       [--embed-model M --tokenizer T]   (with --features static-embed)
//! Then open http://127.0.0.1:5100 .

use autumn_memory::embed;
mod indexer;
mod store;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use anyhow::Result;
use autumn_client::AutumnError;
use autumn_memory::MemoryStore;
use axum::body::Body;
use axum::extract::Query;
use axum::http::{Response, StatusCode};
use axum::routing::get;
use axum::Router;
use send_wrapper::SendWrapper;
use serde_json::{json, Value};

use embed::Embedder;
use store::Code;

const LISTEN_PORT: u16 = 5100;

struct App {
    code: Code,
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
    resp(
        serde_json::to_vec(&json!({ "error": msg })).unwrap_or_default(),
        "application/json",
        code,
    )
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

// -- handlers ---------------------------------------------------------------

async fn h_search(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let q = req(&p, "q")?;
    let mode = opt(&p, "mode", "auto");
    let mode = if mode == "auto" {
        if app.cfg["modes"].as_array().map(|a| a.len() > 1).unwrap_or(false) {
            "hybrid"
        } else {
            "lexical"
        }
    } else {
        mode
    };
    let k: usize = opt(&p, "k", "10").parse().unwrap_or(10);
    let hits = app.code.search_code(q, mode, k).await?;
    Ok(json_ok(&json!({ "hits": hits })))
}

async fn h_symbol(app: &App, p: P) -> Result<Response<Body>, AppError> {
    match app.code.get_symbol(req(&p, "id")?).await? {
        Some(v) => Ok(json_ok(&v)),
        None => Err(AppError::Mem(AutumnError::NotFound)),
    }
}

async fn h_callers(app: &App, p: P) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&json!({ "symbols": app.code.callers(req(&p, "id")?).await? })))
}
async fn h_callees(app: &App, p: P) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&json!({ "symbols": app.code.callees(req(&p, "id")?).await? })))
}
async fn h_members(app: &App, p: P) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&json!({ "symbols": app.code.members(req(&p, "id")?).await? })))
}
async fn h_trace(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let id = req(&p, "id")?;
    Ok(json_ok(&json!({ "path": app.code.trace(id, opt(&p, "dir", "out")).await? })))
}
async fn h_stats(app: &App) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&app.code.stats().await?))
}
async fn h_config(app: &App) -> Result<Response<Body>, AppError> {
    Ok(json_ok(&app.cfg))
}
async fn index_handler() -> Response<Body> {
    resp(
        include_str!("../static/index.html").as_bytes().to_vec(),
        "text/html; charset=utf-8",
        StatusCode::OK,
    )
}

fn router(shared: Shared) -> Router {
    macro_rules! q {
        ($h:path) => {{
            let s = SendWrapper::new(shared.clone());
            move |Query(p): Query<P>| {
                let s = s.clone();
                SendWrapper::new(async move {
                    match $h(&s, p).await {
                        Ok(r) => r,
                        Err(e) => to_response(e),
                    }
                })
            }
        }};
    }
    macro_rules! s {
        ($h:path) => {{
            let s = SendWrapper::new(shared.clone());
            move || {
                let s = s.clone();
                SendWrapper::new(async move {
                    match $h(&s).await {
                        Ok(r) => r,
                        Err(e) => to_response(e),
                    }
                })
            }
        }};
    }
    Router::new()
        .route("/", get(index_handler))
        .route("/config", get(s!(h_config)))
        .route("/stats", get(s!(h_stats)))
        .route("/search", get(q!(h_search)))
        .route("/symbol", get(q!(h_symbol)))
        .route("/callers", get(q!(h_callers)))
        .route("/callees", get(q!(h_callees)))
        .route("/members", get(q!(h_members)))
        .route("/trace", get(q!(h_trace)))
}

// -- args + embedder --------------------------------------------------------

struct Args {
    manager: String,
    root: Option<PathBuf>,
    tenant: String,
    agent: String,
    /// path to the tenant credential (from `autumn-op tenant-create`,
    /// hex). REQUIRED when `mem/` is a protected namespace — the SDK auto-mints
    /// short-TTL tokens scoped to `mem/{tenant}/`. Omit on an unprotected/authz-
    /// off cluster.
    credential_file: Option<String>,
    host: String,
    port: u16,
    no_index: bool,
    reset: bool,
    reindex: bool,
    mcp: bool,
    embed_model: Option<String>,
    tokenizer: Option<String>,
}

/// Delete every key under `mem/{tenant}/{agent}/` — a complete wipe of this
/// agent's memory (nodes/edges/docs/postings/vectors/stats), so a following
/// re-index starts clean (no stale symbols removed from the code).
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
        start.push(0); // exclusive successor of the last key
    }
    Ok(total)
}

fn parse_args() -> Args {
    let mut a = Args {
        manager: "127.0.0.1:9001".into(),
        root: None,
        tenant: "codebase".into(),
        agent: "default".into(),
        credential_file: std::env::var("AUTUMN_CREDENTIAL_FILE").ok().filter(|s| !s.is_empty()),
        host: "127.0.0.1".into(),
        port: LISTEN_PORT,
        no_index: false,
        reset: false,
        reindex: false,
        mcp: false,
        embed_model: None,
        tokenizer: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--root" => a.root = it.next().map(PathBuf::from),
            "--tenant" => a.tenant = it.next().unwrap_or(a.tenant),
            "--agent" => a.agent = it.next().unwrap_or(a.agent),
            "--credential-file" => a.credential_file = it.next(),
            "--host" => a.host = it.next().unwrap_or(a.host),
            "--port" => a.port = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.port),
            "--no-index" => a.no_index = true,
            "--reset" => a.reset = true,
            "--reindex" => a.reindex = true,
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

// ---------------------------------------------------------------------------
// MCP (stdio) — newline-delimited JSON-RPC 2.0 over stdin/stdout, so Claude can
// call the code index as tools. No SDK: the protocol surface is tiny.
// ---------------------------------------------------------------------------

fn mcp_tool_defs() -> Value {
    let id = json!({"type":"object","properties":{"id":{"type":"string"}},"required":["id"]});
    json!([
        {"name":"search_code","description":"Search the indexed codebase (mode: lexical|vector|hybrid|auto). Returns symbols with source, kind, file:line, score.",
         "inputSchema":{"type":"object","properties":{"query":{"type":"string"},"mode":{"type":"string"},"k":{"type":"integer"}},"required":["query"]}},
        {"name":"get_symbol","description":"Full source + metadata for a symbol id (e.g. 'src/lib.rs::MemoryStore::add_edge').","inputSchema":id},
        {"name":"find_callers","description":"Symbols that call `id`.","inputSchema":id},
        {"name":"find_callees","description":"Symbols that `id` calls.","inputSchema":id},
        {"name":"trace_call_path","description":"Bounded call-path from `id` (direction out=callees, in=callers).",
         "inputSchema":{"type":"object","properties":{"id":{"type":"string"},"direction":{"type":"string"}},"required":["id"]}}
    ])
}

async fn mcp_tool_call(code: &Code, params: &Value) -> Result<Value> {
    let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
    let s = |k: &str| args.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let data: Value = match name {
        "search_code" => {
            let mode = match args.get("mode").and_then(|v| v.as_str()).unwrap_or("auto") {
                "auto" => "hybrid",
                m => m,
            };
            let k = args.get("k").and_then(|v| v.as_u64()).unwrap_or(8) as usize;
            json!(code.search_code(&s("query"), mode, k).await?)
        }
        "get_symbol" => code.get_symbol(&s("id")).await?.unwrap_or(Value::Null),
        "find_callers" => json!(code.callers(&s("id")).await?),
        "find_callees" => json!(code.callees(&s("id")).await?),
        "trace_call_path" => {
            let dir = args.get("direction").and_then(|v| v.as_str()).unwrap_or("out");
            json!(code.trace(&s("id"), dir).await?)
        }
        other => {
            return Ok(json!({"content":[{"type":"text","text":format!("unknown tool {other}")}],"isError":true}))
        }
    };
    Ok(json!({"content":[{"type":"text","text": serde_json::to_string(&data)?}]}))
}

async fn run_mcp_stdio(code: &Code) -> Result<()> {
    use std::io::Write;
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break; // EOF — client closed
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(msg): std::result::Result<Value, _> = serde_json::from_str(trimmed) else {
            continue;
        };
        let id = msg.get("id").cloned();
        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let result: Option<Value> = match method {
            "initialize" => Some(json!({
                "protocolVersion": "2024-11-05",
                "serverInfo": {"name": "codebase-memory", "version": "0.1.0"},
                "capabilities": {"tools": {}}
            })),
            "tools/list" => Some(json!({ "tools": mcp_tool_defs() })),
            "tools/call" => {
                Some(mcp_tool_call(code, &msg.get("params").cloned().unwrap_or_else(|| json!({}))).await?)
            }
            "ping" => Some(json!({})),
            _ => None, // notifications (no id) or unknown
        };
        // Only requests (with an id) get a response; notifications are silent.
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

#[compio::main]
async fn main() -> Result<()> {
    // Logs go to stderr — in --mcp mode stdout is the JSON-RPC channel.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = parse_args();
    // Default: index autumn-rs itself (the repo root, two levels up from this crate).
    let root = args.root.clone().unwrap_or_else(|| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap_or(Path::new("."))
            .to_path_buf()
    });

    let store = Rc::new(
        match &args.credential_file {
            // protected `mem/` — connect with the principal
            // credential (SDK auto-mints/renews short-TTL tokens scoped to
            // `mem/{tenant}/`). The principal identity is read from the file.
            Some(path) => {
                let (principal, secret) = autumn_client::read_credential_file(path)
                    .map_err(|e| anyhow::anyhow!("--credential-file: {e}"))?;
                MemoryStore::connect_with_credential(
                    &args.manager,
                    args.tenant.clone(),
                    args.agent.clone(),
                    principal,
                    secret,
                )
                .await?
            }
            None => MemoryStore::connect(&args.manager, args.tenant.clone(), args.agent.clone())
                .await?,
        }
            .with_page_limit(256),
    );
    let emb = Rc::new(build_embedder(&args));
    let code = Code {
        store: store.clone(),
        emb: emb.clone(),
    };

    // MCP stdio mode: speak JSON-RPC over stdin/stdout against the existing
    // index (no HTTP, no indexing — index via the web app first).
    if args.mcp {
        tracing::info!("codebase-memory MCP stdio server (agent={})", args.agent);
        return run_mcp_stdio(&code).await;
    }

    if args.reset {
        let removed = wipe_agent(&store, &args.tenant, &args.agent).await?;
        tracing::info!("reset: deleted {removed} keys under mem/{}/{}/", args.tenant, args.agent);
    }

    // Index only when needed. --reset always rebuilds; otherwise, if the agent
    // already holds symbols we serve the EXISTING index (re-indexing the whole
    // tree on every startup is slow + pointless). Pass --reindex to force it.
    let existing = code.stats().await.ok();
    let already = existing
        .as_ref()
        .and_then(|s| s.get("symbols"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let mut files = 0usize;
    let mut symbols = 0u64;
    let mut edges = 0u64;
    if !args.no_index && (args.reset || args.reindex || already == 0) {
        tracing::info!("indexing {} ...", root.display());
        let (f, s, e) = indexer::index_path(&store, &emb, &root).await?;
        files = f;
        symbols = s as u64;
        edges = e as u64;
        if s > 0 {
            store.train_centroids((s / 20).clamp(1, 64), 25, 7).await?;
        }
        tracing::info!("indexed {symbols} symbols, {edges} edges from {files} files");
    } else if already > 0 {
        symbols = already;
        edges = existing
            .as_ref()
            .and_then(|s| s.get("edges"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        tracing::info!("agent already indexed ({symbols} symbols) — serving it; --reindex to rebuild");
    }

    let cfg = json!({
        "tenant": args.tenant, "agent": args.agent, "manager": args.manager,
        "embedder": emb.name(), "dim": emb.dim(),
        "modes": ["lexical", "vector", "hybrid"],
        "root": root.display().to_string(),
        "files": files, "symbols": symbols, "edges": edges,
    });

    let shared: Shared = Rc::new(App { code, cfg });
    let listener =
        compio::net::TcpListener::bind(format!("{}:{}", args.host, args.port)).await?;
    tracing::info!(
        "codebase-memory → http://{}:{}  ({} symbols, embedder={})",
        args.host,
        args.port,
        symbols,
        emb.name()
    );
    cyper_axum::serve(listener, router(shared)).await?;
    Ok(())
}
