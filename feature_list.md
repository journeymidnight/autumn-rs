# autumn go→rust feature list

**Last updated:** 2026-06-30

**Rules:**
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries (F-number + Trigger + `passes:false`), not as plan-file footnotes.
- Entries below the Completed table document non-obvious decisions, root causes, and active designs.

---

(cleared 2026-06-30 — prior feature history in git + docs/feature_list_archive.md)

## Active

### F-MEM-1 — autumn-memory Phase 1 core (Rust)
- **Trigger**: build autumn-rs into an AI-agent-memory backend (design: `docs/autumn_memory_plan.md`). User directive: core in **Rust** (`crates/autumn-memory`), not Python; Python is only a later thin PyO3 adapter.
- **Scope (Phase 1)**: framework-agnostic core over `autumn-client::ClusterClient`, built only on put/get/delete/range + TTL (no embedder, no server change):
  - key schema (`mem/` namespace, percent-encoded components, newest-first episodic suffix) — plan §6
  - episodic log: `append_event` / `recent_events` / `replay_session`
  - facts (LangGraph BaseStore model): `put_fact` / `get_fact` / `delete_fact` / `list_facts`, per-key TTL
  - posting-on-KV retrieval (user directive 2026-06-30: skip the brute-force MVP, build the real index): **BM25-on-KV** (lexical) + **SPFresh-IVF-on-KV** (vector) + hybrid (RRF)
- **Acceptance**: `cargo test -p autumn-memory` green AND an e2e against a live cluster passes (index → BM25 search → vector search → hybrid).
- **Status**: `passes: true` (completed 2026-06-30) — episodic + facts + BM25-on-KV lexical + SPFresh-IVF-on-KV vector + hybrid RRF all DONE; **18/18 pure unit tests green** (now 19 after F-MEM-3's `get_memory`); **e2e green** against an isolated live cluster (`tests/e2e.rs` + `tests/run_e2e.sh`). Phase 2 (Python PyO3 binding + Hermes `MemoryProvider`) is next.

### F-MEM-2 — Python binding + ergonomic layer
- **Trigger**: framework adapters are Python; give them a synchronous, JSON-friendly handle over the `!Send` Rust core without leaking compio/bytes.
- **Scope**: (a) `autumn.Memory` PyO3 binding (`python/src/memory.rs`) — sync blocking façade: a dedicated compio worker thread owns the `!Send` `MemoryStore`, methods ship closures + block with the GIL released; full surface (episodic/fact/BM25/IVF/hybrid). (b) `autumn_memory.AutumnMemory` ergonomic layer (`python/autumn_memory`) — JSON (de)serialization for events/facts + an optional **text-embedder hook** (vector/hybrid legs take text; no embedder → lexical-only); `append/replay/recent`, `put/get/list/delete_fact`, `remember/forget/train/search(lexical|vector|hybrid|auto)`.
- **Acceptance**: `python/tests/run_memory_e2e.sh` (binding) and `python/autumn_memory/tests/run_smoke.sh` (ergonomic layer, fake embedder) green against an isolated cluster.
- **Status**: `passes: true` (completed 2026-06-30; commits 22bcbd8 binding, 0d295f8 plural-fold, 24af6ec ergonomic layer) — both smokes green.

### F-MEM-3 — stdio MCP server (universal touch point)
- **Trigger**: plan §12a — one server reaches every MCP host (Claude Desktop / Cursor / Cline / ChatGPT Developer Mode) per session; model-invoked, no daemon.
- **Scope**: `python/autumn_memory_mcp` — a FastMCP **stdio** server (per-session child process) over `AutumnMemory`. Tools: ChatGPT-recognized `search`+`fetch` pair (search resolves every hit's text from the authoritative doc record so results are self-contained) + write `add`/`update`/`delete` + episodic `append_event`/`recent_events`/`replay_session` + facts `put_fact`/`get_fact`/`list_facts`/`delete_fact`. Async tools offload the blocking core call to a thread (event loop stays responsive). Lexical mode = zero embedder. Adds a `get_memory(doc_id)` point-get to the Rust core + PyO3 + `AutumnMemory.get` (backs `fetch`).
- **Acceptance**: full tool surface driven through a REAL MCP client over the SDK in-memory transport against an isolated cluster (`python/autumn_memory_mcp/tests/run_mcp_test.sh`).
- **Status**: `passes: true` (completed 2026-06-30) — `MCP INPROC OK`, mcp-test exit 0; 19/19 core unit tests green. coco review (GPT-5.5): P1 (missing `autumn-memory` dep) FIXED; P2 (ghost result from a deleted vector-indexed doc) FIXED at the search boundary (`_resolve` drops hits whose authoritative doc record is gone — §8.5) + turned into a regression test; the deeper orphan-vector-posting reaping is deferred as F-MEM-4.

### F-MEM-5 — LangGraph `BaseStore` adapter
- **Trigger**: plan §12c — LangGraph long-term memory is the `BaseStore` ABC; a native adapter lets any LangGraph agent persist memory in autumn with no daemon.
- **Scope**: `python/autumn_memory_langgraph` — `AutumnStore(BaseStore)` over `AutumnMemory` (pure client-side). Implements the only abstract surface (`batch`/`abatch`); the concrete `get`/`put`/`search`/`delete`/`list_namespaces` dispatch through it. Namespace `tuple[str,…]` ↔ one percent-encoded fact-namespace; a value envelope `{value,created_at,updated_at}` preserves `Item` timestamps (created_at kept across updates); a reserved **namespace registry** fact-namespace supports prefix `search`/`list_namespaces` (the facts API only lists within one exact namespace); a registry entry whose namespace has no live facts (all keys deleted or TTL-expired) is lazily reaped on enumeration. `search` = namespace-prefix scan + `filter` (LangGraph operators `$eq/$ne/$gt/$gte/$lt/$lte/$in/$nin`) + lexical `query` ranking; `list_namespaces` honors prefix/suffix (with `*` wildcard) + `max_depth`. TTL minutes→seconds. Documented limits: `index=` semantic-embedding path and `refresh_ttl` not wired (follow-ups).
- **Acceptance**: the public `BaseStore` surface (get/put/update-preserves-created_at/search+filter-ops+query/limit+offset/list_namespaces prefix+suffix+max_depth/delete-deregisters/ttl-expiry-reap) green against an isolated cluster (`python/autumn_memory_langgraph/tests/run_store_test.sh`).
- **Status**: `passes: true` (completed 2026-06-30) — `LANGGRAPH STORE OK`, lg-store-test exit 0. coco review (GPT-5.5): P2#1 (filter operators silently mis-compared) FIXED + tested; P2#2 (TTL-expired namespace lingered in registry/list_namespaces) FIXED via lazy liveness-reap + tested (real 3s-TTL expiry); P3 (pipe masked maturin/pip failure) FIXED (`set -uo pipefail` + guarded installs).

### F-MEM-4 — vector-leg deletion / orphan-posting reaping (follow-up)
- **Trigger**: coco P2 (2026-06-30). `delete_memory` reaps the `doc/{id}` record + BM25 postings but NOT the IVF posting `ivf/{centroid}/{doc_id}` (the centroid isn't known at delete time). Orphan vectors then accumulate in IVF buckets: they waste storage, occupy `nprobe`/top-k slots, and add RRF noise to hybrid ranking. **Correctness is already safe** — the search boundary (F-MEM-3 `_resolve`, plan §8.5) drops any hit whose doc record is gone, so an orphan vector can never surface as a result; this is purely index hygiene/quality.
- **Scope (proposed)**: O(1) reverse pointer `vptr/{doc_id}` → centroid, written by `index_vector`, maintained by `train_centroids` reassign, read+deleted by `delete_memory` so it can delete `ivf/{centroid}/{doc_id}`. Note the existing design tolerates a vector in two buckets after a train-crash (search dedups by id; `train_centroids` is the full reaper) — vptr-based delete is best-effort for the common case, with `_resolve` remaining the airtight correctness backstop.
- **Acceptance**: after `delete_memory`, a full `ivf/` prefix scan finds no posting for the deleted id (common, non-crash case); orphan count stays bounded across index/delete churn; hybrid recall unaffected.
- **Status**: `passes: false` (deferred — reproduce-first; do deliberately, not rushed into the MCP commit; the F-MEM-3 regression test already proves the correctness backstop holds).
