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
- **Status**: `passes: true` (completed 2026-06-30) — episodic + facts + BM25-on-KV lexical + SPFresh-IVF-on-KV vector + hybrid RRF all DONE; **18/18 pure unit tests green**; **e2e green** against an isolated live cluster (`tests/e2e.rs` + `tests/run_e2e.sh`). Phase 2 (Python PyO3 binding + Hermes `MemoryProvider`) is next.
