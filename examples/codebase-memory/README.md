# codebase-memory Example

Index a **Rust** codebase — **autumn-rs itself, by default** — into
autumn-memory and search it from a browser, with a call graph you can walk. A
small [codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp)-style
tool, written in Rust like `gallery` (Axum on the compio runtime, calling the
`autumn-memory` crate directly).

It shows off the graph layer: symbols are stored as an **adjacency list on the
ordered KV**, so the call graph is just prefix range scans — `out_edges` for
callees, `in_edges` for callers, `bfs` for a call trace.

## What it stores

For every item (`fn` / `struct` / `enum` / `union` / `trait` / `mod`), keyed
`"<relpath>::<qualname>"` (e.g. `src/lib.rs::MemoryStore::add_edge`):

- a **searchable doc** — BM25 lexical + a vector (from the embedder below),
- a **graph node** (`Function` / `Method` / `Struct` / `Enum` / `Union` /
  `Trait` / `Module`) with `{name, file, start, end}`,
- **edges** — `CONTAINS` (module / trait / `impl` type → member) and `CALLS`
  (caller → callee, resolved by short name; an MVP that over-links on name
  collisions and ignores calls to items outside the index). The `tree-sitter` +
  `tree-sitter-rust` crates do the parsing; symbol resolution is the only
  bespoke part (a precise resolver via `tree-sitter-stack-graphs` is the upgrade
  path).

Everything lands under autumn-memory's `mem/{tenant}/{agent}/…` keyspace
(`node/`, `edge/`, `redge/`, `nidx/`, `doc/`, `idx/`, `ivf/`).

## Prerequisites

A running autumn-rs cluster (`./cluster.sh start 3` from the repo root after
`cargo build --release --workspace`).

## Run

```bash
# no --root → indexes autumn-rs itself
cargo run -p codebase-memory -- 127.0.0.1:9001
# then open http://127.0.0.1:5100
```

`127.0.0.1:9001` is the manager. `--root PATH` indexes a different tree (or a
narrower subtree — `--root crates/autumn-memory` indexes ~176 symbols in a
couple seconds; the whole repo is much larger). Flags: `--tenant` / `--agent`
(isolate indexes), `--host` / `--port`, `--no-index` (serve an already-indexed
agent), `--reset` (see below).

## Delete all memory & re-index

Re-running the indexer **upserts** (a symbol's id overwrites), so unchanged /
edited symbols re-index cleanly — but symbols you *deleted* from the code would
linger. To start fresh, pick one:

```bash
# 1. Wipe THIS agent's memory, then re-index (deletes every key under mem/{tenant}/{agent}/):
cargo run -p codebase-memory -- 127.0.0.1:9001 --reset

# 2. Or index under a fresh namespace (old data just sits unused, isolated):
cargo run -p codebase-memory -- 127.0.0.1:9001 --agent v2
```

The UI has two tabs:
- **Search** — type a query, toggle **lexical / vector / hybrid**, click a hit to
  see its source + callers/callees; click a caller/callee chip to walk the graph.
- **Graph** — a 3D force-directed view of the whole code graph (nodes colored by
  kind, `CALLS`/`CONTAINS` edges), rendered with
  [`3d-force-graph`](https://github.com/vasturiano/3d-force-graph) (loaded from a
  CDN). Toggle kinds in the legend; click a node to jump to its source.

## Use with Claude (MCP)

The same binary is also an MCP server over stdio (`--mcp`) — no extra process,
no Python. It queries the already-indexed agent (index once via the web app
first). Register it with Claude Code, run from the repo root:

```bash
claude mcp add codebase-memory -- cargo run -q -p codebase-memory -- 127.0.0.1:9001 --mcp
```

Tools: `search_code` · `get_symbol` · `find_callers` · `find_callees` ·
`trace_call_path`. (For Claude Desktop, put the same command/args under
`mcpServers` in `claude_desktop_config.json`.)

## Embedder

The vector / hybrid legs need embeddings (autumn-memory takes caller-supplied
vectors). Two options:

- **`hash`** (default) — a zero-dependency signed-hashing embedder. Real
  plumbing, weak semantics; makes `cargo run` work with no model file.
- **`static-int8`** — a Model2Vec-style static int8 lookup table (real
  semantics, no service):

  ```bash
  python3 examples/codebase-memory/tools/fetch_model.py \
      --out model.m2vs --tokenizer-out tokenizer.json          # MIT, 256-dim
  cargo run -p codebase-memory --features static-embed -- 127.0.0.1:9001 \
      --embed-model model.m2vs --tokenizer tokenizer.json
  ```

## HTTP endpoints (what the UI calls)

| Route | Returns |
|---|---|
| `GET /search?q=&mode=lexical\|vector\|hybrid&k=` | ranked symbols (source + location + score) |
| `GET /symbol?id=` | one symbol's full source + metadata |
| `GET /callers?id=` / `GET /callees?id=` / `GET /members?id=` | graph neighbors (CALLS / CONTAINS) |
| `GET /trace?id=&dir=out\|in` | bounded call-path (BFS) |
| `GET /graph` | the whole graph `{nodes, links, counts}` for the Graph tab |
| `GET /stats` / `GET /config` | index counts / server config |

## Manual verification

```bash
./cluster.sh start 3
# index one crate for a quick check, then drive search + the call graph:
cargo run -p codebase-memory -- 127.0.0.1:9001 --root crates/autumn-memory &
open http://127.0.0.1:5100

curl -s 'http://127.0.0.1:5100/stats'                     # {"symbols":176,"edges":497,"is_clean":true}
curl -s 'http://127.0.0.1:5100/search?q=add%20a%20graph%20edge&mode=lexical&k=4'
curl -s 'http://127.0.0.1:5100/callees?id=tests/e2e.rs::e2e_graph_traversal'  # add_edge, bfs, out_edges, …
curl -s 'http://127.0.0.1:5100/callers?id=src/lib.rs::MemoryStore::add_edge'  # e2e_graph_traversal
curl -s 'http://127.0.0.1:5100/members?id=src/lib.rs::MemoryStore'            # the impl methods
./cluster.sh stop
```
