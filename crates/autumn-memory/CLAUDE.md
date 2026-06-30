# autumn-memory Crate Guide

## Purpose

Framework-agnostic **AI-agent-memory** core, built as a pure client-side
library over `autumn-client::ClusterClient` (no daemon, no server-side change).
The Rust crate is the reusable core; thin adapters sit ON it — a PyO3 binding
(`autumn.Memory`) → the `autumn_memory.AutumnMemory` ergonomic layer → framework
shells (a **stdio MCP server** `python/autumn_memory_mcp`; planned Hermes
`MemoryProvider` / LangGraph `BaseStore`). Design + rationale:
`docs/autumn_memory_plan.md`.

`MemoryStore` is `!Send` (single-thread compio, like the whole client surface)
— drive its async methods on a compio runtime.

## Three memory kinds (Phase 1 — built only on put/get/delete/range + TTL)

| Kind | API | Storage shape |
|---|---|---|
| **episodic** | `append_event` / `recent_events` / `replay_session` | append-only, newest-first by key order |
| **facts** | `put_fact` / `get_fact` / `delete_fact` / `list_facts` | point-get + namespace-prefix-list (LangGraph `BaseStore`), per-key TTL |
| **lexical recall (BM25-on-KV)** ✅ | `index_memory` / `delete_memory` / `search_lexical` / `get_memory` | `recall.rs` |
| **vector recall (SPFresh-IVF-on-KV)** ✅ | `index_vector` / `train_centroids` / `search_vector` | `vector.rs` |
| **hybrid (RRF)** ✅ | `search_hybrid` | `recall::rrf_fuse` |

Phase 1 is e2e-validated against a live cluster (`tests/e2e.rs` #[ignore] + the
isolated-cluster harness `tests/run_e2e.sh`).

## Lexical recall — BM25-on-KV (`recall.rs`, plan §7 词法腿)

Posting-on-KV done directly (no brute-force MVP — user directive 2026-06-30):

- `idx/{term}/{doc_id}` = **existence marker** (empty value) → candidate
  discovery is a keys-only range scan.
- `doc/{doc_id}` = the **authoritative** `IndexedDoc {doc_len, terms->tf, text,
  meta}` — the single source of truth, written AFTER its postings (the COMMIT
  POINT). A stale posting (term removed on re-index) is simply absent from the
  doc's current `terms` map and ignored at query time → **no generation
  stamping needed** for the lexical leg (the doc validates every posting; plan
  §8.5: posting = candidate hint, doc = correctness boundary).
- `meta/stats` = `{n_docs, sum_doc_len}` (16 bytes LE) for idf + length-norm,
  single-writer per agent (shared/multi-writer → delta-log per §8.5).
- Query: tokenize → per-term keys-only scan → fetch candidate docs → Okapi
  BM25 (`bm25_term`, k1=1.2 b=0.75) over each doc's current term map → top-k.
  `df` ≈ posting count (stale-orphan over-count is bounded; idf robust).
- Tokenizer: lowercase + maximal-alphanumeric runs + small stopword set
  (CJK/segmentation = follow-up). Values are opaque `meta` bytes.

Values are **opaque bytes** — the caller/adapter chooses the encoding (JSON,
rkyv, …); the core never imposes one.

## Key schema (`keys.rs`, plan §6)

```text
mem/{tenant}/{agent}/ep/{session}/{12-byte suffix}   episodic
mem/{tenant}/{agent}/fact/{namespace}/{key}          fact
mem/{tenant}/shared/{namespace}/{key}                cross-agent shared
```

- Reserved `mem/` namespace separates these from fuse / kvcache / client keys.
- Dynamic components are **percent-encoded** (`q`/`unq`) so a `/` inside a
  tenant/agent/session/namespace/key can't forge a separator or another
  agent's prefix (tested: `agent_prefix_isolation`).
- Episodic `{suffix}` = `BE(u64::MAX - ts_ns) ++ BE(u32::MAX - counter)` →
  ascending range scan = **newest-first**; the per-store counter breaks
  same-ns ties.

## Read-path semantics (plan §8.5 contract)

- `range` returns **keys only** (server-side `value` is empty) → every list/
  replay is a keys-scan + per-key point-get (the known two-hop; MVP uses
  sequential `get`, a later iteration swaps `get_many_into`).
- Recall/list is **near-real-time / eventually consistent**, NOT a snapshot:
  `get_values` skips keys that vanished (deleted/expired) between the scan and
  the get. Main-record point-get is the correctness boundary.
- This boundary extends to the **vector leg**: `delete_memory` reaps the
  `doc/{id}` record + BM25 postings but not the IVF posting `ivf/{c}/{id}`
  (centroid unknown at delete time), so a vector/hybrid hit may name a deleted
  id. A resolver MUST drop hits whose `doc/{id}` is gone (`get_memory` → None) —
  the MCP server's `_resolve` does (coco P2). Reaping the orphan IVF posting is
  index hygiene, tracked as F-MEM-4; `train_centroids` is the full reaper today.
- Pagination resumes EXCLUSIVELY via the successor of the last key
  (`last_key ++ 0x00`).

## Multi-tenant isolation (plan §9.5)

Key prefix `mem/{tenant}/{agent}/` is ORGANIZATION, not security — a client
can forge another agent's key. Real isolation requires **server-side authz**
in autumn (Phase 0, plan §16); a single trusted org can run on prefixes alone.

## Tests

`cargo test -p autumn-memory` — pure key-schema unit tests in `keys.rs`
(percent round-trip, fact-key round-trip, newest-first ordering, ts round-trip,
prefix isolation). The async store ops are exercised e2e against a live cluster
(next iteration).
