//! memory-mcp — put a corpus into autumn and serve retrieval from it to any
//! MCP client. Two ingesters share one store: a Rust-code indexer
//! (tree-sitter: symbols + a CALLS/CONTAINS graph) and a markdown/plain-text
//! ingester (heading-aware chunks + a CONTAINS outline). The same binary is a
//! web UI for the code corpus, and an MCP server over both on TWO transports:
//! `--mcp` speaks stdio, and `POST /mcp` speaks the same JSON-RPC over HTTP.
//! They share one dispatch (`mcp_dispatch`), so the two can't drift apart in
//! which tools they offer. HTTP is what lets a consumer treat this as a URL
//! rather than a process it must spawn, credential and supervise.
//! A Rust example (like `gallery`): Axum on the compio runtime over
//! `autumn-memory` directly.
//!
//! Usage:
//!   cargo run -p memory-mcp -- [MANAGER] [--root PATH] [--docs PATH]...
//!       [--tenant T] [--agent A] [--host H] [--port P] [--no-index] [--mcp]
//!       [--embed-model M --tokenizer T]   (with --features static-embed)
//! Then open http://127.0.0.1:5100 — or point an MCP client at the stdio
//! `--mcp` form, or at `http://127.0.0.1:5100/mcp`.
//!
//! Retrieval-quality run (scores the ingested corpus against a labelled query
//! set and exits — see `eval.rs`):
//!   cargo run -p memory-mcp -- [MANAGER] --agent eval \
//!       --docs /path/to/corpus --eval eval/sutra.jsonl \
//!       [--eval-k 10] [--eval-modes lexical,hybrid] \
//!       [--eval-baseline eval/baseline.json [--eval-update-baseline]]

use autumn_memory::embed;
mod docs;
mod eval;
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
use axum::routing::{get, post};
use axum::Router;
use send_wrapper::SendWrapper;
use serde_json::{json, Value};

use embed::Embedder;
use store::{Code, Corpus};

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

/// What `mode=auto` resolves to.
///
/// It has to ask whether the EMBEDDER is semantic, not whether a vector index
/// exists. The previous version tested `cfg["modes"].len() > 1` — but `modes` is
/// a hardcoded `["lexical","vector","hybrid"]`, so that was a constant `true`
/// and `auto` always meant `hybrid`; the MCP path did not even check, mapping
/// `auto` straight to `hybrid`.
///
/// With the default `HashEmbedder` the vector leg is noise, and RRF fusion
/// pulls that noise into the top ranks: on a corpus of Chinese Buddhist texts,
/// `坐禅` under `lexical` returned five on-topic passages, while the same query
/// under the `auto` default put vector noise at ranks 1-2. Anyone who asked for
/// nothing in particular — which is every MCP `search_docs` call — got the
/// degraded channel.
///
/// An explicit `mode=vector` / `mode=hybrid` is still honoured: this only
/// decides what "no preference" means.
fn auto_mode(emb: &embed::Embedder) -> &'static str {
    if emb.is_semantic() {
        "hybrid"
    } else {
        "lexical"
    }
}

// -- handlers ---------------------------------------------------------------

async fn h_search(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let q = req(&p, "q")?;
    let mode = opt(&p, "mode", "auto");
    let mode = if mode == "auto" { auto_mode(&app.code.emb) } else { mode };
    let k: usize = opt(&p, "k", "10").parse().unwrap_or(10);
    let corpus = Corpus::parse(opt(&p, "corpus", "code"));
    let hits = app.code.search(q, mode, k, corpus).await?;
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
// Graph database over HTTP — the same surface as the `graph_*` MCP tools, so
// the web UI and a curl-wielding operator reach the graph the same way an
// agent does. Reads only: mutations go through MCP (or the indexers), which
// keeps a GET from writing.
async fn h_graph_node(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let n = app.code.graph_get_node(req(&p, "id")?).await?;
    Ok(json_ok(&json!({ "node": n })))
}
async fn h_graph_neighbors(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let et = p.get("type").map(String::as_str).filter(|s| !s.is_empty());
    let lim = p.get("limit").and_then(|v| v.parse::<usize>().ok());
    let dir = opt(&p, "direction", "out");
    Ok(json_ok(&json!({ "edges": app.code.graph_neighbors(req(&p, "id")?, dir, et, lim).await? })))
}
async fn h_graph_traverse(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let et = p.get("type").map(String::as_str).filter(|s| !s.is_empty());
    let depth = p.get("max_depth").and_then(|v| v.parse::<u32>().ok()).unwrap_or(3);
    let nodes = p.get("max_nodes").and_then(|v| v.parse::<usize>().ok()).unwrap_or(200);
    let dir = opt(&p, "direction", "out");
    Ok(json_ok(&json!({ "path": app.code.graph_traverse(req(&p, "id")?, dir, et, depth, nodes).await? })))
}
async fn h_graph_nodes(app: &App, p: P) -> Result<Response<Body>, AppError> {
    let lim = p.get("limit").and_then(|v| v.parse::<usize>().ok());
    Ok(json_ok(&json!({ "nodes": app.code.graph_nodes(req(&p, "kind")?, lim).await? })))
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
    // POST + a raw body, which neither `q!` (query params) nor `s!` (no args)
    // covers. The body is taken as a String rather than `Json<Value>` so a
    // malformed request answers our own -32700-shaped message instead of axum's
    // generic 422, which an MCP client reports as an unusable server.
    macro_rules! mcp {
        ($h:path) => {{
            let s = SendWrapper::new(shared.clone());
            move |body: String| {
                let s = s.clone();
                SendWrapper::new(async move {
                    match $h(&s, body).await {
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
        .route("/graph/node", get(q!(h_graph_node)))
        .route("/graph/neighbors", get(q!(h_graph_neighbors)))
        .route("/graph/traverse", get(q!(h_graph_traverse)))
        .route("/graph/nodes", get(q!(h_graph_nodes)))
        .route("/mcp", post(mcp!(h_mcp)))
}

// -- args + embedder --------------------------------------------------------

struct Args {
    manager: String,
    root: Option<PathBuf>,
    /// document trees/files (`--docs`, repeatable) ingested at startup.
    docs: Vec<PathBuf>,
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
    /// Goldset path — presence switches the binary into evaluation mode:
    /// ingest (if `--docs`), score, report, exit. No server.
    eval: Option<PathBuf>,
    eval_k: usize,
    eval_modes: Vec<String>,
    eval_baseline: Option<PathBuf>,
    /// Write the baseline instead of comparing against it. Opt-in, because a
    /// baseline that updates itself records the regression instead of catching
    /// it.
    eval_update_baseline: bool,
    eval_tolerance: f64,
    eval_out: Option<PathBuf>,
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
        // `RangeReq.start` is INCLUSIVE; `last ++ 0x00` resumes strictly
        // after `last` (exact successor — see the field's docs), so the
        // boundary key is neither re-deleted nor double-counted.
        start = last;
        start.push(0);
    }
    Ok(total)
}

fn parse_args() -> Args {
    let mut a = Args {
        manager: "127.0.0.1:9001".into(),
        root: None,
        docs: Vec::new(),
        tenant: "memory".into(),
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
        eval: None,
        eval_k: 10,
        eval_modes: Vec::new(),
        eval_baseline: None,
        eval_update_baseline: false,
        eval_tolerance: 0.01,
        eval_out: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--root" => a.root = it.next().map(PathBuf::from),
            "--docs" => {
                if let Some(p) = it.next() {
                    a.docs.push(PathBuf::from(p));
                }
            }
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
            "--eval" => a.eval = it.next().map(PathBuf::from),
            "--eval-k" => a.eval_k = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.eval_k),
            "--eval-modes" => {
                a.eval_modes = it
                    .next()
                    .map(|s| s.split(',').map(|m| m.trim().to_string()).filter(|m| !m.is_empty()).collect())
                    .unwrap_or_default()
            }
            "--eval-baseline" => a.eval_baseline = it.next().map(PathBuf::from),
            "--eval-update-baseline" => a.eval_update_baseline = true,
            "--eval-tolerance" => {
                a.eval_tolerance = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.eval_tolerance)
            }
            "--eval-out" => a.eval_out = it.next().map(PathBuf::from),
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
    let query = json!({"type":"object","properties":{"query":{"type":"string"},"mode":{"type":"string"},"k":{"type":"integer"}},"required":["query"]});
    json!([
        {"name":"search_code","description":"Search the indexed codebase (mode: lexical|vector|hybrid|auto). Returns symbols with source, kind, file:line, score. Code only — use search_docs for prose.",
         "inputSchema": query},
        {"name":"get_symbol","description":"Full text + metadata for an id — a code symbol ('src/lib.rs::MemoryStore::add_edge') or a document chunk ('docs/ops.md#L10-L42').","inputSchema":id},
        {"name":"find_callers","description":"Symbols that call `id`.","inputSchema":id},
        {"name":"find_callees","description":"Symbols that `id` calls.","inputSchema":id},
        {"name":"trace_call_path","description":"Bounded call-path from `id` (direction out=callees, in=callers).",
         "inputSchema":{"type":"object","properties":{"id":{"type":"string"},"direction":{"type":"string"}},"required":["id"]}},
        {"name":"ingest_documents","description":"Ingest markdown/plain-text (.md/.markdown/.txt) from `path` (file or directory, server-side) into the memory store: heading-aware chunks, BM25+vector indexed, heading hierarchy as a CONTAINS outline. Upserts by chunk id; returns counts.",
         "inputSchema":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}},
        {"name":"search_docs","description":"Search ingested documents (mode: lexical|vector|hybrid|auto). Returns chunks with text, source file, heading path, line range, score — enough to cite 'file › headings, lines a-b'.",
         "inputSchema": query},
        {"name":"list_documents","description":"List ingested document files.",
         "inputSchema":{"type":"object","properties":{}}},
        {"name":"document_outline","description":"Heading outline of an ingested document (`id` = its file path, from list_documents or a chunk's `file`), depth-tagged.",
         "inputSchema":id},

        // Graph database. The store's node/edge layer is domain-agnostic, so
        // these are not code tools: ids, kinds and edge types are whatever the
        // caller decides. The code/document graphs this server builds are just
        // one occupant of the same graph.
        {"name":"graph_upsert_node","description":"Create or replace a node. `id` and `kind` are caller-defined labels; `attrs` is arbitrary JSON stored with it.",
         "inputSchema":{"type":"object","properties":{"id":{"type":"string"},"kind":{"type":"string"},"attrs":{"type":"object"}},"required":["id","kind"]}},
        {"name":"graph_get_node","description":"A node with its full attrs, or null if absent.","inputSchema":id},
        {"name":"graph_delete_node","description":"Delete a node and every edge touching it.","inputSchema":id},
        {"name":"graph_add_edge","description":"Create a typed edge src -[type]-> dst, with optional JSON `attrs`. Endpoints need not exist yet.",
         "inputSchema":{"type":"object","properties":{"src":{"type":"string"},"type":{"type":"string"},"dst":{"type":"string"},"attrs":{"type":"object"}},"required":["src","type","dst"]}},
        {"name":"graph_delete_edge","description":"Delete one edge src -[type]-> dst.",
         "inputSchema":{"type":"object","properties":{"src":{"type":"string"},"type":{"type":"string"},"dst":{"type":"string"}},"required":["src","type","dst"]}},
        {"name":"graph_neighbors","description":"Edges incident to `id`. direction out (default) or in; `type` filters the edge type, omit for all. Returns each edge with its type, attrs and the far node.",
         "inputSchema":{"type":"object","properties":{"id":{"type":"string"},"direction":{"type":"string"},"type":{"type":"string"},"limit":{"type":"integer"}},"required":["id"]}},
        {"name":"graph_traverse","description":"Bounded breadth-first walk from `id`, each node tagged with its depth. direction out (default) or in; `type` filters the edge type; max_depth/max_nodes bound the fan-out.",
         "inputSchema":{"type":"object","properties":{"id":{"type":"string"},"direction":{"type":"string"},"type":{"type":"string"},"max_depth":{"type":"integer"},"max_nodes":{"type":"integer"}},"required":["id"]}},
        {"name":"graph_nodes","description":"List nodes of one `kind` — how to find an entry point without already knowing an id.",
         "inputSchema":{"type":"object","properties":{"kind":{"type":"string"},"limit":{"type":"integer"}},"required":["kind"]}}
    ])
}

async fn mcp_tool_call(code: &Code, params: &Value) -> Result<Value> {
    let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
    let s = |k: &str| args.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let mode = match args.get("mode").and_then(|v| v.as_str()).unwrap_or("auto") {
        "auto" => auto_mode(&code.emb),
        m => m,
    };
    let k = args.get("k").and_then(|v| v.as_u64()).unwrap_or(8) as usize;
    let data: Value = match name {
        "search_code" => json!(code.search(&s("query"), mode, k, Corpus::Code).await?),
        "search_docs" => json!(code.search(&s("query"), mode, k, Corpus::Docs).await?),
        "get_symbol" => code.get_symbol(&s("id")).await?.unwrap_or(Value::Null),
        "find_callers" => json!(code.callers(&s("id")).await?),
        "find_callees" => json!(code.callees(&s("id")).await?),
        "trace_call_path" => {
            let dir = args.get("direction").and_then(|v| v.as_str()).unwrap_or("out");
            json!(code.trace(&s("id"), dir).await?)
        }
        "ingest_documents" => {
            let path = PathBuf::from(s("path"));
            if !path.exists() {
                return Ok(json!({"content":[{"type":"text",
                    "text":format!("path not found: {}", path.display())}],"isError":true}));
            }
            let (files, chunks, edges) = docs::ingest_path(&code.store, &code.emb, &path).await?;
            if chunks > 0 {
                let r = code.store.reconcile().await?;
                code.store
                    .train_centroids(((r.docs as usize) / 20).clamp(1, 64), 25, 7)
                    .await?;
            }
            json!({"files": files, "chunks": chunks, "edges": edges})
        }
        "list_documents" => json!(code.documents().await?),
        "document_outline" => json!(code.outline(&s("id")).await?),

        "graph_upsert_node" => {
            let attrs = args.get("attrs").cloned().unwrap_or_else(|| json!({}));
            code.graph_put_node(&s("id"), &s("kind"), &attrs).await?
        }
        "graph_get_node" => code.graph_get_node(&s("id")).await?.unwrap_or(Value::Null),
        "graph_delete_node" => code.graph_delete_node(&s("id")).await?,
        "graph_add_edge" => {
            let attrs = args.get("attrs").cloned().unwrap_or_else(|| json!({}));
            code.graph_add_edge(&s("src"), &s("type"), &s("dst"), &attrs).await?
        }
        "graph_delete_edge" => code.graph_delete_edge(&s("src"), &s("type"), &s("dst")).await?,
        "graph_neighbors" => {
            let dir = args.get("direction").and_then(|v| v.as_str()).unwrap_or("out");
            let et = args.get("type").and_then(|v| v.as_str());
            let lim = args.get("limit").and_then(|v| v.as_u64()).map(|n| n as usize);
            json!(code.graph_neighbors(&s("id"), dir, et, lim).await?)
        }
        "graph_traverse" => {
            let dir = args.get("direction").and_then(|v| v.as_str()).unwrap_or("out");
            let et = args.get("type").and_then(|v| v.as_str());
            let depth = args.get("max_depth").and_then(|v| v.as_u64()).unwrap_or(3) as u32;
            let nodes = args.get("max_nodes").and_then(|v| v.as_u64()).unwrap_or(200) as usize;
            json!(code.graph_traverse(&s("id"), dir, et, depth, nodes).await?)
        }
        "graph_nodes" => {
            let lim = args.get("limit").and_then(|v| v.as_u64()).map(|n| n as usize);
            json!(code.graph_nodes(&s("kind"), lim).await?)
        }
        other => {
            return Ok(json!({"content":[{"type":"text","text":format!("unknown tool {other}")}],"isError":true}))
        }
    };
    Ok(json!({"content":[{"type":"text","text": serde_json::to_string(&data)?}]}))
}

/// One JSON-RPC method dispatch, shared by BOTH transports. `Ok(None)` means the
/// method is unknown — the caller decides what that costs: stdio and HTTP both
/// answer -32601 for a request, and stay silent for a notification. Keeping the
/// dispatch here (rather than duplicating the match per transport) is what makes
/// "the HTTP server exposes the same tools as the stdio one" a property of the
/// code instead of a promise in a comment.
async fn mcp_dispatch(code: &Code, method: &str, params: Value) -> Result<Option<Value>> {
    Ok(match method {
        "initialize" => Some(json!({
            "protocolVersion": "2024-11-05",
            "serverInfo": {"name": "memory-mcp", "version": "0.1.0"},
            "capabilities": {"tools": {}}
        })),
        "tools/list" => Some(json!({ "tools": mcp_tool_defs() })),
        "tools/call" => Some(mcp_tool_call(code, &params).await?),
        "ping" => Some(json!({})),
        _ => None,
    })
}

/// Answer ONE JSON-RPC message. Returns `None` for a notification (no `id`),
/// which the HTTP layer turns into 202 Accepted with no body, per the MCP
/// streamable-HTTP transport.
async fn mcp_one(code: &Code, msg: &Value) -> Option<Value> {
    let id = msg.get("id").cloned();
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));
    let outcome = mcp_dispatch(code, method, params).await.map_err(|e| format!("{e:#}"));
    mcp_envelope(id, outcome)
}

/// Build the JSON-RPC reply envelope. Split out from `mcp_one` because this is
/// the part with the rules worth pinning, and it needs no `Code` — so a test can
/// reach the real function instead of re-stating its output as a literal.
///
/// `None` means "write nothing": a notification carries no `id`, so it gets no
/// reply even when the dispatch failed. There is nobody to tell.
fn mcp_envelope(id: Option<Value>, outcome: std::result::Result<Option<Value>, String>) -> Option<Value> {
    let id = id?;
    Some(match outcome {
        Ok(Some(r)) => json!({"jsonrpc":"2.0","id":id,"result":r}),
        // -32601 keeps the connection usable. An HTTP 4xx here would make a
        // client mark the whole server dead over one unknown method.
        Ok(None) => json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}),
        Err(msg) => json!({"jsonrpc":"2.0","id":id,"error":{"code":-32603,"message":msg}}),
    })
}

/// Shape the HTTP response body. The reply must MIRROR the request's shape: a
/// client that sent a bare object indexes the response directly, and handing it
/// a one-element array breaks it. `None` = 202 with no body (all notifications).
fn mcp_reply_body(was_batch: bool, mut replies: Vec<Value>) -> Option<Value> {
    if replies.is_empty() {
        return None;
    }
    Some(if was_batch { json!(replies) } else { replies.remove(0) })
}

/// `POST /mcp` — the same MCP server as `--mcp`, over HTTP instead of stdin.
///
/// Why this exists: a stdio MCP server has to be SPAWNED by its client, so every
/// consumer needs this binary, its autumn credential and its lifecycle. Over HTTP
/// the server is just a URL, which is what lets a web UI point hermes at
/// `http://memory-mcp:5100/mcp` and own neither.
///
/// Accepts a single JSON-RPC object or a batch array. A batch of only
/// notifications answers 202 with no body, same as a single one.
async fn h_mcp(app: &App, body: String) -> Result<Response<Body>, AppError> {
    let msg: Value = serde_json::from_str(&body)
        .map_err(|e| AppError::Bad(format!("mcp: request is not JSON: {e}")))?;
    let replies: Vec<Value> = match &msg {
        Value::Array(batch) => {
            let mut out = Vec::with_capacity(batch.len());
            for m in batch {
                if let Some(r) = mcp_one(&app.code, m).await {
                    out.push(r);
                }
            }
            out
        }
        _ => mcp_one(&app.code, &msg).await.into_iter().collect(),
    };
    match mcp_reply_body(msg.is_array(), replies) {
        Some(payload) => Ok(json_ok(&payload)),
        None => Ok(resp(Vec::new(), "application/json", StatusCode::ACCEPTED)),
    }
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
        let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));
        let result: Option<Value> = mcp_dispatch(code, method, params).await?;
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

/// Evaluation mode: score the ingested corpus against the goldset, print the
/// report, and hand back the process exit status — nonzero only when a
/// baseline comparison found a regression, so a shell `&&` and CI agree.
async fn run_eval(code: &Code, a: &Args, retrained: bool) -> Result<i32> {
    let path = a.eval.as_ref().expect("called only in eval mode");
    let queries = eval::load(path)?;
    let modes: Vec<String> = if a.eval_modes.is_empty() {
        ["lexical", "vector", "hybrid"].iter().map(|s| s.to_string()).collect()
    } else {
        a.eval_modes.clone()
    };
    println!(
        "eval: {} queries x {} modes, k={}, embedder={} (semantic={}), agent=mem/{}/{}",
        queries.len(),
        modes.len(),
        a.eval_k,
        code.emb.name(),
        code.emb.is_semantic(),
        a.tenant,
        a.agent,
    );
    let mut report = eval::run(code, &queries, a.eval_k, &modes).await?;
    // Stamped into the report so a baseline says what produced it: the same
    // goldset scored against a different corpus, or with a different embedder,
    // is a different measurement wearing the same numbers.
    report["corpus"] = json!(a.docs.iter().map(|p| p.display().to_string()).collect::<Vec<_>>());
    report["embedder"] = json!(code.emb.name());
    report["retrained"] = json!(retrained);
    if let Some(out) = &a.eval_out {
        std::fs::write(out, serde_json::to_vec_pretty(&report)?)?;
        println!("\nreport -> {}", out.display());
    }
    let Some(baseline) = &a.eval_baseline else {
        return Ok(0);
    };
    if a.eval_update_baseline {
        std::fs::write(baseline, serde_json::to_vec_pretty(&report)?)?;
        println!("\nbaseline updated -> {}", baseline.display());
        return Ok(0);
    }
    if !baseline.exists() {
        println!(
            "\nno baseline at {} yet — rerun with --eval-update-baseline to record this run",
            baseline.display()
        );
        return Ok(0);
    }
    let base: Value = serde_json::from_slice(&std::fs::read(baseline)?)?;
    Ok(i32::from(eval::compare(&report, &base, a.eval_tolerance)))
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
        tracing::info!("memory-mcp MCP stdio server (agent={})", args.agent);
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
    // Evaluation scores the DOCUMENT corpus, and the code indexer would put
    // thousands of symbols into the same agent's BM25 stats — changing idf and
    // avgdl for every document query. So eval mode never indexes code; point it
    // at its own agent and give it only `--docs`.
    if args.eval.is_none() && !args.no_index && (args.reset || args.reindex || already == 0) {
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

    // Timed per phase: ingest is round-trip-bound, so when it feels slow the
    // useful question is WHICH phase, and per-chunk ms is the number that
    // compares across corpora and across clusters.
    let mut doc_chunks = 0usize;
    for d in &args.docs {
        tracing::info!("ingesting documents from {} ...", d.display());
        let t0 = std::time::Instant::now();
        let (f, c, e) = docs::ingest_path(&store, &emb, d).await?;
        let ms = t0.elapsed().as_millis();
        let per = if c > 0 { ms as f64 / c as f64 } else { 0.0 };
        tracing::info!("ingested {c} chunks ({e} outline edges) from {f} files in {ms} ms ({per:.1} ms/chunk)");
        doc_chunks += c;
    }
    if doc_chunks > 0 {
        let t0 = std::time::Instant::now();
        let r = store.reconcile().await?;
        store.train_centroids(((r.docs as usize) / 20).clamp(1, 64), 25, 7).await?;
        tracing::info!("reconcile + train_centroids in {} ms", t0.elapsed().as_millis());
    }

    if args.eval.is_some() {
        let status = run_eval(&code, &args, doc_chunks > 0).await?;
        // `exit` rather than a returned Err: a regression is a verdict, not a
        // crash, and the caller wants the code without a backtrace above it.
        std::process::exit(status);
    }

    let cfg = json!({
        "tenant": args.tenant, "agent": args.agent, "manager": args.manager,
        "embedder": emb.name(), "dim": emb.dim(),
        // Consumed by `auto_mode`, and worth exposing: a caller comparing
        // `mode=vector` results against `lexical` needs to know which of the
        // two the index can actually support.
        "embedder_semantic": emb.is_semantic(),
        "modes": ["lexical", "vector", "hybrid"],
        "root": root.display().to_string(),
        "files": files, "symbols": symbols, "edges": edges,
    });

    let shared: Shared = Rc::new(App { code, cfg });
    let listener =
        compio::net::TcpListener::bind(format!("{}:{}", args.host, args.port)).await?;
    tracing::info!(
        "memory-mcp → http://{}:{}  ({} symbols, embedder={})",
        args.host,
        args.port,
        symbols,
        emb.name()
    );
    cyper_axum::serve(listener, router(shared)).await?;
    Ok(())
}

#[cfg(test)]
mod mcp_http_tests {
    use super::*;

    // These call the real framing functions. The dispatch itself needs a live
    // `Code` and is exercised by the stdio path; what is new here — and what
    // these pin — is the envelope and body shaping.

    #[test]
    fn a_notification_gets_no_reply_even_when_the_dispatch_failed() {
        // No `id` => nobody to answer. Emitting an error envelope for a
        // notification makes a client correlate it against a request it never
        // sent.
        assert_eq!(mcp_envelope(None, Ok(Some(json!({})))), None);
        assert_eq!(mcp_envelope(None, Ok(None)), None);
        assert_eq!(mcp_envelope(None, Err("boom".into())), None);
    }

    #[test]
    fn an_unknown_method_is_method_not_found_and_keeps_the_id() {
        let env = mcp_envelope(Some(json!(7)), Ok(None)).expect("a request gets a reply");
        assert_eq!(env["error"]["code"], -32601);
        assert_eq!(env["id"], 7);
        assert_eq!(env["jsonrpc"], "2.0");
    }

    #[test]
    fn a_dispatch_error_is_internal_error_carrying_the_chain() {
        let env = mcp_envelope(Some(json!("abc")), Err("outer: inner".into())).unwrap();
        assert_eq!(env["error"]["code"], -32603);
        assert_eq!(env["error"]["message"], "outer: inner");
        assert_eq!(env["id"], "abc", "a string id must survive as a string");
    }

    #[test]
    fn the_reply_shape_mirrors_the_request_shape() {
        let one = json!({"jsonrpc":"2.0","id":1,"result":{}});
        assert!(mcp_reply_body(false, vec![one.clone()]).unwrap().is_object());
        assert!(mcp_reply_body(true, vec![one.clone()]).unwrap().is_array());
        // All-notifications => nothing to send => 202 with no body.
        assert_eq!(mcp_reply_body(true, vec![]), None);
        assert_eq!(mcp_reply_body(false, vec![]), None);
    }

    #[test]
    fn tool_defs_are_the_one_list_both_transports_serve() {
        let defs = mcp_tool_defs();
        let arr = defs.as_array().expect("tool defs are an array");
        assert!(!arr.is_empty(), "an MCP server with no tools is not useful");
        for d in arr {
            assert!(d.get("name").and_then(|n| n.as_str()).is_some(), "every tool needs a name");
            assert!(d.get("inputSchema").is_some(), "every tool needs an inputSchema");
        }
    }
}
