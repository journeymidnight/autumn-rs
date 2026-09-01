# memory-mcp Example

**An MCP server that puts a corpus into autumn and serves retrieval from it to
any MCP client** (Claude Code, Claude Desktop, or anything else that speaks
MCP over stdio). Two ingesters share one store:

- **code** — a Rust codebase (autumn-rs itself, by default), parsed with
  tree-sitter into symbols plus a call graph you can walk;
- **documents** — markdown / plain text (`.md` / `.markdown` / `.txt`),
  chunked along heading boundaries with the heading hierarchy kept as an
  outline.

Everything lands in autumn-memory's `mem/{tenant}/{agent}/…` keyspace
(`node/`, `edge/`, `redge/`, `nidx/`, `doc/`, `idx/`, `ivf/`) and is served
back as BM25 lexical + vector + hybrid search, with graph walks (`CALLS` for
code, `CONTAINS` for module membership and document outlines). Written in Rust
like `gallery` (Axum on the compio runtime, calling the `autumn-memory` crate
directly); the same binary is also a web UI for the code corpus.

## Prerequisites

A running autumn-rs cluster (`./cluster.sh start 3` from the repo root after
`cargo build --release --workspace`).

## Quick start: documents in, retrieval out (MCP)

```bash
# 1. Ingest a markdown corpus (here: this repo's docs/) — one-shot, then Ctrl-C
#    once it serves, or keep the web UI running:
cargo run -p memory-mcp -- 127.0.0.1:9001 --no-index --docs docs

# 2. Register the MCP server with Claude Code (run from the repo root):
claude mcp add autumn-memory -- cargo run -q -p memory-mcp -- 127.0.0.1:9001 --mcp
```

Now any agent with the server attached can `search_docs("how do I …")` and get
chunks with provenance (`file`, `headings`, line range) to cite — or ingest
more at runtime with `ingest_documents {path}` (no restart, no re-registration).

## Quick start: code in, retrieval out

```bash
# no --root → indexes autumn-rs itself; --root narrows/redirects
cargo run -p memory-mcp -- 127.0.0.1:9001 --root crates/autumn-memory
# then open http://127.0.0.1:5100  (search UI over the code corpus)
```

`127.0.0.1:9001` is the manager. Both ingestion modes compose: one server, one
agent, one index holding both corpora — searches are corpus-filtered so code
queries never return prose and vice versa.

## MCP tools

| Tool | What |
|---|---|
| `search_code {query, mode?, k?}` | code symbols: source, kind, `file:line`, score |
| `search_docs {query, mode?, k?}` | document chunks: text, source file, heading path, line range, score |
| `ingest_documents {path}` | ingest `.md`/`.markdown`/`.txt` under a server-side path (file or dir); upserts |
| `list_documents {}` | the ingested document files |
| `document_outline {id}` | a document's heading outline (`CONTAINS` walk), depth-tagged |
| `get_symbol {id}` | full text + metadata for any id — code symbol or doc chunk |
| `find_callers {id}` / `find_callees {id}` | CALLS graph neighbors |
| `trace_call_path {id, direction?}` | bounded call-path BFS |

### Graph database

The store's node/edge layer is a general graph, not a code index: ids, kinds
and edge types are labels the caller chooses, and attributes are arbitrary
JSON. The code and document graphs this server builds are just its first two
occupants — anything else can share the same graph.

| Tool | What |
|---|---|
| `graph_upsert_node {id, kind, attrs?}` | create or replace a node |
| `graph_get_node {id}` | a node with its full attrs (`null` if absent) |
| `graph_delete_node {id}` | delete a node **and every edge touching it** |
| `graph_add_edge {src, type, dst, attrs?}` | typed edge; endpoints need not exist yet |
| `graph_delete_edge {src, type, dst}` | delete one edge |
| `graph_neighbors {id, direction?, type?, limit?}` | incident edges with type, attrs, and the far node |
| `graph_traverse {id, direction?, type?, max_depth?, max_nodes?}` | bounded BFS, depth-tagged |
| `graph_nodes {kind, limit?}` | nodes of one kind — an entry point when you have no id |

`direction` is `out` (default) or `in`; omitting `type` matches every edge
type. `graph_neighbors` returns the EDGE rather than just the far node, so the
edge type and its attributes survive the round trip — a graph query that drops
them cannot answer "how are these two related". `max_depth` and `max_nodes`
are capped server-side (16 / 2000) so one traversal can't walk the whole graph.

```jsonc
// A graph that has nothing to do with code:
graph_upsert_node {"id":"person:ada","kind":"Person","attrs":{"born":1815}}
graph_upsert_node {"id":"machine:engine","kind":"Machine"}
graph_add_edge    {"src":"person:ada","type":"WROTE_NOTES_ON","dst":"machine:engine","attrs":{"year":1843}}
graph_neighbors   {"id":"machine:engine","direction":"in"}
// → [{"src":"person:ada","type":"WROTE_NOTES_ON","dst":"machine:engine",
//     "attrs":{"year":1843},"node":{"id":"person:ada","kind":"Person"}}]
```

`find_callers` / `find_callees` / `members` / `document_outline` /
`trace_call_path` are named shorthands over exactly these calls, fixed to one
edge type and direction. They stay because "who calls this" is the question an
agent actually asks — not because the graph knows what a call is.

`mode` is `lexical` \| `vector` \| `hybrid` (default `hybrid`). Code and docs
keep separate search tools (rather than one `search` with a corpus flag)
because the result shapes differ — symbols carry kind + graph handles,
chunks carry heading-path provenance — and an agent picking a tool by name
needs no extra parameter to state its intent.

## What ingestion stores

**Code** — for every item (`fn` / `struct` / `enum` / `union` / `trait` /
`mod`), keyed `"<relpath>::<qualname>"` (e.g. `src/lib.rs::MemoryStore::add_edge`):
a searchable doc (BM25 + vector), a graph node, and `CONTAINS` /
`CALLS` edges (callee resolved by short name; an MVP that over-links on name
collisions and ignores calls outside the index — `tree-sitter` +
`tree-sitter-rust` do the parsing).

**Documents** — chunks keyed `"<relpath>#L<start>-L<end>"` (GitHub-style line
anchor: human-readable, collision-free against code ids). Chunking never
crosses a heading (fenced code blocks are masked); an oversized section splits
at paragraph boundaries (~2.8 KB cap) with a one-paragraph overlap. Each chunk
is indexed with a `file › heading › subheading` breadcrumb prepended, and the
heading hierarchy becomes `Document → Section` `CONTAINS` edges — the outline
is a graph walk, same as tracing calls.

## Delete all memory & re-ingest

Ingestion **upserts** (an id overwrites), so edits re-index cleanly — but
symbols/chunks whose ids disappeared (deleted code, moved doc lines) would
linger. To start fresh, pick one:

```bash
# 1. Wipe THIS agent's memory (every key under mem/{tenant}/{agent}/), then re-index:
cargo run -p memory-mcp -- 127.0.0.1:9001 --reset --docs docs

# 2. Or ingest under a fresh namespace (old data sits unused, isolated):
cargo run -p memory-mcp -- 127.0.0.1:9001 --agent v2 --docs docs
```

## Flags

`--root PATH` (code tree; default = this repo) · `--docs PATH` (document tree
or file, repeatable) · `--no-index` (skip code indexing) · `--reindex` (force
code re-index) · `--reset` (wipe agent first) · `--tenant` / `--agent`
(default `memory` / `default`; isolate indexes) · `--credential-file` (authz
clusters: `cluster.sh` mints `$DATA_ROOT/authz/memory.cred` granting
`mem/memory/`) · `--host` / `--port` · `--mcp` (stdio MCP server; indexes
nothing at startup — ingest via the tools or a prior run) · `--embed-model` /
`--tokenizer` (below).

## Embedder

The vector / hybrid legs need embeddings (autumn-memory takes caller-supplied
vectors). Two options:

- **`hash`** (default) — a zero-dependency signed-hashing embedder. Real
  plumbing, weak semantics; makes `cargo run` work with no model file.
- **`static-int8`** — a Model2Vec-style static int8 lookup table (real
  semantics, no service):

  ```bash
  python3 examples/memory-mcp/tools/fetch_model.py \
      --out model.m2vs --tokenizer-out tokenizer.json          # MIT, 256-dim
  cargo run -p memory-mcp --features static-embed -- 127.0.0.1:9001 \
      --embed-model model.m2vs --tokenizer tokenizer.json
  ```

## Web UI + HTTP endpoints

The web UI at `http://127.0.0.1:5100` is the **code view** (search, with
caller/callee chips to walk the graph). The HTTP API serves both corpora:

| Route | Returns |
|---|---|
| `GET /search?q=&mode=lexical\|vector\|hybrid&k=&corpus=code\|docs\|all` | ranked hits (text + location + score) |
| `GET /symbol?id=` | one symbol's / chunk's full text + metadata |
| `GET /callers?id=` / `GET /callees?id=` / `GET /members?id=` | graph neighbors (CALLS / CONTAINS) |
| `GET /trace?id=&dir=out\|in` | bounded call-path (BFS) |
| `GET /graph/node?id=` | one node with its full attrs |
| `GET /graph/nodes?kind=&limit=` | nodes of one kind |
| `GET /graph/neighbors?id=&direction=&type=&limit=` | incident edges (type + attrs + far node) |
| `GET /graph/traverse?id=&direction=&type=&max_depth=&max_nodes=` | bounded BFS, depth-tagged |
| `GET /stats` / `GET /config` | index counts / server config |

The `/graph/*` routes are the read half of the graph-database surface; writes
go through the MCP `graph_*` tools (or the indexers), which keeps a GET from
mutating the graph.

## Manual verification

```bash
./cluster.sh start 3
# code corpus (one crate for speed) + this repo's markdown docs:
cargo run -p memory-mcp -- 127.0.0.1:9001 --root crates/autumn-memory --docs docs &

curl -s 'http://127.0.0.1:5100/stats'   # symbols+chunks / edges / docs counts
curl -s 'http://127.0.0.1:5100/search?q=add%20a%20graph%20edge&mode=lexical&k=4'
curl -s 'http://127.0.0.1:5100/search?q=start%20the%20cluster&corpus=docs&k=3'
curl -s 'http://127.0.0.1:5100/callees?id=tests/e2e.rs::e2e_graph_traversal'
curl -s 'http://127.0.0.1:5100/members?id=docs/ops.md' # top-level outline
./cluster.sh stop
```
