# autumn-memory Crate Guide

## Purpose

Framework-agnostic **AI-agent-memory** core, built as a pure client-side
library over `autumn-client::ClusterClient` (no daemon, no server-side change).
Consumers are Rust: the `examples/memory-mcp` app uses the crate directly
(MCP server + web UI) to ingest a corpus — code symbols and/or markdown chunks —
search it, and walk its graph. Design + rationale: `docs/autumn_memory_plan.md`.

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
| **graph (adjacency-on-KV)** ✅ | `put_node` / `get_node` / `delete_node` / `add_edge` / `delete_edge` / `out_edges` / `in_edges` / `neighbors` / `nodes_by_kind` / `bfs` | `graph.rs` (codec) + `keys.rs` |

Phase 1 is e2e-validated against a live cluster (`tests/e2e.rs` #[ignore] + the
isolated-cluster harness `tests/run_e2e.sh`).

## Optional built-in embedder (`embed.rs`)

The vector/hybrid legs take a **caller-supplied** `&[f32]` (production feeds them
from a shared sglang/vLLM endpoint — deliberately NOT an in-process model, see
plan §11). For callers that just want a built-in embedder without a model
server, `autumn_memory::embed` provides one: `HashEmbedder` (zero-dep, signed-FNV
hashing — always available) and, behind the **`static-embed`** feature,
`StaticTableEmbedder` (a Model2Vec-style int8 lookup table, needs `tokenizers`).
`Embedder` dispatches; all emit an `EMBED_DIM`-length L2-normalized vector.
Errors are a local `EmbedError` (the core takes no `anyhow` dep). The `gallery`-
style example `examples/memory-mcp` uses it.

## Lexical recall — BM25-on-KV (`recall.rs`, plan §7 词法腿)

Posting-on-KV, done directly:

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
- Tokenizer: lowercase + maximal-alphanumeric runs + small stopword set +
  conservative plural folding; **CJK** (Han / kana / Hangul) emits BOTH the
  per-codepoint unigram (单字常是整词, 如 猫/狗 — keeps single-character queries
  working) AND the adjacent bigram inside one run. A bigram never bridges
  punctuation, whitespace or Latin, so it only ever joins characters the writer
  wrote together.
  Bigrams are searchable but **not length-bearing**: `doc_len` still counts
  unigrams + Latin terms only, so BM25's length normalization keeps the
  no-Latin-vs-CJK-skew property that unigram-only had.
  Why: unigram alone loses phrase structure and that loses real searches —
  measured, `慧能` (a proper name whose second character 能 is extremely common)
  returned four passages from three unrelated sutras and none from the one he
  wrote. The design had assumed the hybrid vector leg would supply phrase
  precision; it cannot, because the default `HashEmbedder` is non-semantic and
  `mode=auto` therefore resolves to lexical.
  Cost ≈ 2n index entries for an n-character CJK run (postings are empty-value
  markers, so entries not bytes). Values are opaque `meta` bytes.

Values are **opaque bytes** — the caller/adapter chooses the encoding (JSON,
rkyv, …); the core never imposes one.

## Key schema (`keys.rs`, plan §6)

> **Wire key = `mem/{tenant}/{agent}/…`** (namespace-first; `{tenant}` is an
> in-namespace sub-prefix the memory app owns, NOT an SDK tenant concept).
> `MemoryStore::connect` uses `ClusterClient::connect(mgr, "mem/{tenant}")`;
> `connect_with_credential` adds a `principal` arg (credential owner, from the
> credential file). `keys.rs` emits keys RELATIVE to the binding (starting at
> `{agent}/…`); the client prepends `mem/{tenant}/`. Grant a memory tenant with
> `principal-create --grant mem/{tenant}/`. See §8 of
> docs/key_namespace_split_design.md.

```text
mem/{tenant}/{agent}/ep/{session}/{12-byte suffix}   episodic
mem/{tenant}/{agent}/fact/{namespace}/{key}          fact
mem/{tenant}/shared/{namespace}/{key}                cross-agent shared
mem/{tenant}/{agent}/node/{id}                       graph node (authoritative)
mem/{tenant}/{agent}/nidx/{kind}/{id}                graph by-kind index (marker)
mem/{tenant}/{agent}/edge/{src}/{type}/{dst}         forward edge (authoritative, attrs)
mem/{tenant}/{agent}/redge/{dst}/{type}/{src}        reverse edge index (marker/hint)
```

Graph families (`graph.rs` + `keys.rs`): a generic node/edge graph as
**adjacency lists**, so every traversal is a prefix range-scan — `out_edges`
scans `edge/{src}/`, `in_edges` scans `redge/{dst}/`, `bfs` chains them. All
components are strings → `q()`-encoded + `/`-separated like the BM25 postings
(no binary trick). The forward `edge/*` is authoritative; `redge/*` is a derived
reverse-index hint validated against the forward edge at read time (same
posting-vs-authoritative contract as BM25). `add_edge` writes `redge` (hint)
then `edge` (commit); `delete_edge` removes `edge` then `redge`. Domain-agnostic
(ids / edge-types opaque strings, attrs opaque bytes) — code-graph schema lives
in the consumer.

**This is a general graph database, and consumers should expose it as one.**
The layer has always been domain-agnostic, but `memory-mcp` only ever surfaced
two hard-wired edge types (`CALLS`, `CONTAINS`), so from outside it looked like
a code index — a caller could read the graph its indexers had built and nothing
else. It now exposes the layer as it is (`graph_*` tools + `/graph/*` routes:
upsert/delete node and typed edge, list by kind, neighbours, bounded traverse),
with callers/callees/members/outline/trace kept as named shorthands pinned to
one edge type and direction. A new consumer should follow that shape rather
than adding another pair of domain verbs.

- Reserved `mem/` namespace separates these from fuse / kvcache / client keys.
- Dynamic components are **percent-encoded** (`q`/`unq`) so a `/` inside a
  tenant/agent/session/namespace/key can't forge a separator or another
  agent's prefix (tested: `agent_prefix_isolation`).
- Episodic `{suffix}` = `BE(u64::MAX - ts_ns) ++ BE(u32::MAX - counter)` →
  ascending range scan = **newest-first**; the per-store counter breaks
  same-ns ties.

## Read-path semantics (plan §8.5 contract)

- `range` returns **keys only** (server-side `value` is empty) → every list/
  replay is a keys-scan + per-key point-get (the known two-hop).
- Recall/list is **near-real-time / eventually consistent**, NOT a snapshot:
  `get_values` skips keys that vanished (deleted/expired) between the scan and
  the get. Main-record point-get is the correctness boundary.
- **Vector leg**: `delete_memory` reaps BOTH legs — BM25 postings + `doc/{id}`,
  AND the IVF posting `ivf/{c}/{id}` via `delete_vector`, located in O(1) by the
  reverse pointer `ivf_meta/vptr/{id} -> centroid`. `train_centroids`
  re-buckets every posting it scans WITHOUT checking doc existence, so it never
  reaps a deleted vector — the vptr reap is the only reaper. `index_vector`
  keeps exactly one IVF copy per id (reaps the old bucket on a move) and keeps
  vptr current; `train` updates vptr on re-bucket. Belt-and-suspenders: a
  resolver should still drop a hit whose `doc/{id}` is gone (`get_memory` →
  None) — the MCP `_resolve` does — covering any in-flight/expiry race.
- Pagination resumes EXCLUSIVELY via the successor of the last key
  (`last_key ++ 0x00`) — exact under the PS's user-key-first internal-key
  comparator (`RangeReq.start` docs).

## Round trips are the cost model — batch every key loop

Everything here is `{scan keys} → {point-get each}` over a network KV, so the
unit of cost is the ROUND TRIP, not the byte or the CPU cycle. Measured on the
live cluster (~1 ms RTT): an ingest was 0.53 s per chunk against the cluster
versus 0.02 s on loopback, and its reconcile phase burned 84 s of wall clock
for 1.9 s of CPU — 98% of ingest was waiting, and none of it was work.

The rule that follows: **a `for k in keys { client.get(k).await }` loop is a
bug.** Use `get_many` / `put_many` / `delete_many`, chunked (256) so one batch
never becomes an unbounded frame. Sites that follow it: `search_lexical`'s
candidate fetch, `index_memory`'s postings write and stale-term reap,
`delete_memory`'s reap, `reconcile`'s doc and vptr walks (via `get_all`),
`train_centroids`' vector read, and both vector search paths.

Two more ingest-specific levers, both opened explicitly by the caller and
closed by `flush_stats`:

- **`begin_bulk_index()` defers `meta/stats`.** The per-document
  read-modify-write of one counter key was two round trips per document, AND
  the reason a bulk loop had to stay serial — concurrent writers to a single
  key lose updates. Deferring makes the counter STALE for the window (idf/avgdl
  seen by a concurrent reader reflect the pre-ingest corpus), which is the same
  eventual consistency `repair_stats` already exists for. Deltas are exact:
  call `flush_stats` on the ERROR path too, or a partial ingest silently
  understates the corpus.
- The same flag memoises the **centroid table**, which `index_vector` otherwise
  re-reads and re-decodes for every single document.

With stats deferred the ingest loop is order-independent, so `memory-mcp`'s
`ingest_path` splits into a sequential CPU pass (chunking, ids, embeddings, and
outline-parent resolution, which IS order-dependent through `path_owner`) and a
concurrent I/O pass with a bounded number of chunks in flight.

## Index reconcile / repair (`reconcile` / `repair_stats`, plan §16)

An OFF-hot-path integrity audit + heal (the plan's per-phase acceptance tool):

- `reconcile() -> ReconcileReport` (read-only): recounts live `doc/{id}` records
  vs `meta/stats`, and cross-checks IVF postings against their `vptr`s. Counts
  ACTUAL postings (never folds by id) so it flags `duplicate_ivf` (same id in
  two buckets — a train-crash residual), `orphan_ivf` (posting whose id has no
  vptr), `dangling_vptr` (vptr whose bucket has no posting), `malformed_vptr`
  (value ≠ 4 bytes). `is_clean()` = stats match + all four counts 0. SCOPE:
  STRUCTURAL integrity only, NOT centroid-assignment optimality — a train-crash
  mid-migration can leave a posting in a now-suboptimal bucket (ANN-recall
  quality, not corruption; re-run `train_centroids` to heal). A `stale_bucket`
  check (decode every posting + recompute nearest) is a deliberate O(ids) →
  O(postings·k) follow-up, omitted to keep the audit cheap.
- **Graph checks** (same read-only pass): counts `nodes` / `edges` / `redges`,
  and cross-checks `dangling_edge` (fwd edge whose `src`/`dst` node record is
  absent), `orphan_redge` (reverse marker with no fwd edge), `missing_redge`
  (fwd edge with no reverse marker). `is_clean()` additionally requires all
  three are 0. Cost is proportional to graph size only (empty scans for
  non-graph agents), so it stays folded into the single `reconcile()`.
- `repair_stats()`: rewrites `meta/stats` from a fresh `doc/` recount (no IVF
  scan). MUST run in a writer-quiesced maintenance window — it is itself a
  read-then-write with no CAS, so a concurrent write would be clobbered.
- **Page-boundary resume in `scan_keys`**: the PS serves a range `start` as an
  INCLUSIVE user-key bound, and `scan_keys` resumes each page with
  `last_key ++ 0x00` — the exact "strictly after last" start, because the PS
  orders internal keys with a user-key-first comparator (`cmp_internal_keys`;
  the fixed-width inverted-seq suffix only breaks ties). The boundary key is
  neither re-served (double-count) nor can a real `last ++ 0x00 ++ …` key be
  skipped. Regression: `tests/scan_boundary.rs` (live-cluster, like `e2e.rs`).

This is the deliberate answer to the **multi-writer `meta/stats` RMW race**:
`update_stats` is read-modify-write, so two processes writing the SAME agent
concurrently can lose a stats update. The harm is LOW — it skews only idf /
avgdl (BM25 scores), never which docs are found (postings + doc records are
per-doc, no cross-doc race) — and it needs a non-primary multi-process-same-
agent topology. So rather than serialize the hot write path (per-writer shards
or a server-side atomic increment — rejected as hot-path / server complexity),
we **tolerate the drift and detect + `repair_stats` it off the hot path**.

## Multi-tenant isolation (plan §9.5)

Key prefix `mem/{tenant}/{agent}/` is ORGANIZATION, not security — a client
can forge another agent's key. Real isolation requires **server-side authz**
in autumn (Phase 0, plan §16); a single trusted org can run on prefixes alone.

## Tests

`cargo test -p autumn-memory --lib` — pure unit tests (key-schema round-trips +
prefix isolation in `keys.rs`; tokenizer incl. plural-fold + CJK in `recall.rs`;
BM25 monotonicity; vector cosine / kmeans / IVF in `vector.rs`). The async store
ops + reconcile/repair are exercised e2e against a live cluster (`tests/e2e.rs`
+ the isolated-cluster harness `tests/run_e2e.sh`); `tests/scan_boundary.rs`
pins the page-boundary dedupe (reconcile over a >page corpus counts exactly).
