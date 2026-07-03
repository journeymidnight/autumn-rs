# memory-browser Example

A web UI **and** MCP server over **autumn-memory**'s general agent-memory API —
the universal replacement for a framework-specific memory adapter. A Rust example
like `gallery`/`codebase-memory` (Axum on compio + a `--mcp` stdio mode).

Four things an agent's memory needs, all on the ordered KV:

- **Search** — remembered notes with **lexical / vector / hybrid** retrieval
  (`index_memory` + `index_vector`; each memory is also a graph node).
- **Facts** — namespaced key/value with per-key **TTL**.
- **Timeline** — an append-only **episodic** event log per session.
- **Graph** — an **associative graph** linking memories, shown as a 3D
  force-directed view (click a node → its detail; add typed links between
  memories).

Everything is namespaced under autumn-memory's `mem/{tenant}/{agent}/…`, so it
coexists with other apps on one cluster (gallery under `gallery/`, etc.).

## Prerequisites

A running cluster (`./cluster.sh start 3` after `cargo build --release --workspace`).

## Run

```bash
cargo run -p memory-browser -- 127.0.0.1:9001
# open http://127.0.0.1:5200
```

Flags: `--tenant` / `--agent` (isolate memory sets), `--port`, `--reset` (wipe
this agent's memory first), `--embed-model model.m2vs --tokenizer tokenizer.json`
with `--features static-embed` for real semantics (see
`../codebase-memory/tools/fetch_model.py`; default is a zero-dep hash embedder).

## Use with Claude (MCP)

The same binary speaks MCP over stdio (`--mcp`), so any agent gets your memory as
tools. Run from the repo root:

```bash
claude mcp add memory -- cargo run -q -p memory-browser -- 127.0.0.1:9001 --mcp
```

Tools: `remember` · `search` · `get_memory` · `put_fact` · `get_fact` ·
`list_facts` · `append_event` · `recent_events` · `link` · `neighbors` · `trace`.

## HTTP endpoints

| Route | |
|---|---|
| `POST /remember?id=&meta=&ttl_secs=` (body = text) | store a memory → `{id}` |
| `GET /search?q=&mode=lexical\|vector\|hybrid&k=` | ranked memories |
| `GET /memory?id=` · `DELETE /memory?id=` | fetch / forget a memory |
| `PUT /fact?ns=&key=&ttl_secs=` (body = value) · `GET /fact?ns=&key=` · `GET /facts?ns=` · `DELETE /fact?ns=&key=` | facts |
| `POST /event?session=` (body = text) · `GET /events?session=&limit=` · `GET /replay?session=` | episodic |
| `POST /link?src=&type=&dst=` · `GET /neighbors?id=&dir=out\|in` | graph |
| `GET /graph` · `GET /stats` · `POST /train` · `GET /config` | viz / ops |

## Manual verification

```bash
./cluster.sh start 3
cargo run -p memory-browser -- 127.0.0.1:9001 --reset &
open http://127.0.0.1:5200

curl -s -XPOST localhost:5200/remember --data 'user prefers dark mode and vim'
curl -s 'localhost:5200/search?q=editor%20settings&mode=hybrid&k=3'
curl -s -XPUT 'localhost:5200/fact?ns=user&key=name' --data 'Ada'; curl -s 'localhost:5200/facts?ns=user'
curl -s -XPOST 'localhost:5200/event?session=s1' --data 'hello'; curl -s 'localhost:5200/events?session=s1'
curl -s localhost:5200/stats
```
