//! autumn-memory — a framework-agnostic AI-agent-memory store over autumn.
//!
//! Pure client-side library (no daemon): a thin façade over
//! [`autumn_client::ClusterClient`], mirroring the autumn-kvcache pattern.
//! See `docs/autumn_memory_plan.md`. It gives an agent three kinds of memory
//! on the shared autumn cluster:
//!
//! * **episodic** — an append-only, time-ordered event log per session
//!   (newest-first by key order; [`MemoryStore::replay_session`] reverses to
//!   chronological).
//! * **facts** — a point-get + namespace-prefix-list KV with optional per-key
//!   TTL (the LangGraph `BaseStore` model).
//! * recall over these (lexical then vector) lands with the next phase (§7).
//!
//! Phase 1 deliberately needs NO embedding model and NO server-side change:
//! it is built entirely on `put` / `get` / `delete` / `range` + TTL. Values
//! are opaque bytes — the caller / adapter chooses the encoding (JSON, rkyv,
//! …); the core never imposes one.
//!
//! `MemoryStore` holds an `Rc<ClusterClient>` and is `!Send` (single-thread
//! compio, like the rest of the client surface); drive its async methods on a
//! compio runtime. A PyO3 binding (for the Hermes / LangGraph adapters) wraps
//! it for synchronous Python callers.

pub mod embed;
pub mod keys;
mod graph;
mod recall;
mod vector;

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use autumn_client::{AutumnError, ClusterClient};
use bytes::Bytes;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Decode a `vptr` value (exactly 4 BE bytes) into a centroid id. Strict on
/// length — a vptr we wrote is always 4 bytes, so anything else is corruption
/// and must be surfaced (None), not silently truncated.
fn read_u32(b: &[u8]) -> Option<u32> {
    if b.len() == 4 {
        Some(u32::from_be_bytes(b.try_into().unwrap()))
    } else {
        None
    }
}

/// Per-`(tenant, agent)` handle onto agent memory backed by autumn.
///
/// Construct one per agent identity. Clone is cheap (`Rc` client); the append
/// counter is per-store.
pub struct MemoryStore {
    client: Rc<ClusterClient>,
    tenant: String,
    agent: String,
    default_ttl: u64,
    page_limit: u32,
    seq: Cell<u32>,
    /// When set, `update_stats` accumulates here instead of doing I/O.
    /// `Some((d_docs, d_len))` = a bulk index is open; see `begin_bulk_index`.
    pending_stats: Cell<Option<(i64, i64)>>,
    /// Centroid table memoised for the duration of a bulk index. Outer `None`
    /// = nothing cached; inner `None` = the index is untrained, which is a
    /// real answer worth caching too.
    centroid_cache: RefCell<Option<Option<Rc<vector::Centroids>>>>,
}

impl MemoryStore {
    /// Connect a fresh client and build a store for `(tenant, agent)`.
    pub async fn connect(
        manager: &str,
        tenant: impl Into<String>,
        agent: impl Into<String>,
    ) -> Result<Self, AutumnError> {
        // (Prepend-only): bind the client to the scope
        // `mem/{tenant}` — it PREPENDS `mem/{tenant}/` to every key, so `keys.rs`
        // emits keys RELATIVE to that scope (starting at `{agent}/…`). NEVER make
        // keys.rs re-add the prefix or every op double-prefixes. `{tenant}` is an
        // in-namespace sub-prefix the memory app owns (Option 3 dropped the SDK
        // tenant concept; isolation is by grant on `mem/{tenant}/`).
        let tenant = tenant.into();
        let client = ClusterClient::connect(manager, &format!("mem/{tenant}"))
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        Ok(Self::with_client(Rc::new(client), tenant, agent))
    }

    /// connect a client bound to `credential` and build a
    /// store for `(tenant, agent)`. Use against an authz-enabled cluster — the
    /// client AUTH_HELLOs each PS connection with a short-TTL token (auto-minted +
    /// renewed) scoped to `mem/{tenant}/`. `principal` is the credential owner
    /// (from the credential file's name line); its grant MUST cover `mem/{tenant}/`.
    pub async fn connect_with_credential(
        manager: &str,
        tenant: impl Into<String>,
        agent: impl Into<String>,
        principal: impl Into<String>,
        credential: Vec<u8>,
    ) -> Result<Self, AutumnError> {
        let tenant = tenant.into();
        let client = ClusterClient::connect_with_credential(
            manager,
            &format!("mem/{tenant}"),
            principal,
            credential,
        )
        .await
        .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        Ok(Self::with_client(Rc::new(client), tenant, agent))
    }

    /// Build a store reusing an existing (already-connected) client.
    pub fn with_client(
        client: Rc<ClusterClient>,
        tenant: impl Into<String>,
        agent: impl Into<String>,
    ) -> Self {
        Self {
            client,
            tenant: tenant.into(),
            agent: agent.into(),
            default_ttl: 0,
            page_limit: 256,
            seq: Cell::new(0),
            pending_stats: Cell::new(None),
            centroid_cache: RefCell::new(None),
        }
    }

    /// Set the default TTL (seconds, `0` = none) applied when a write doesn't
    /// pass an explicit one.
    pub fn with_default_ttl(mut self, ttl_secs: u64) -> Self {
        self.default_ttl = ttl_secs;
        self
    }

    /// Set the range-scan page size (keys fetched per round-trip).
    pub fn with_page_limit(mut self, limit: u32) -> Self {
        self.page_limit = limit.max(1);
        self
    }

    /// Raw access to the underlying client (e.g. to share it).
    pub fn client(&self) -> &Rc<ClusterClient> {
        &self.client
    }

    // -- internals -----------------------------------------------------------

    fn next_seq(&self) -> u32 {
        let s = self.seq.get();
        self.seq.set(s.wrapping_add(1));
        s
    }

    fn ttl(&self, ttl: Option<u64>) -> u64 {
        ttl.unwrap_or(self.default_ttl)
    }

    async fn put_kv(&self, key: &[u8], value: &[u8], ttl_secs: u64) -> Result<(), AutumnError> {
        if ttl_secs == 0 {
            self.client.put(key, value).await
        } else {
            let expires_at = ClusterClient::ttl_to_expires_at(ttl_secs);
            let items = [(key, Bytes::copy_from_slice(value), expires_at)];
            self.client
                .put_many(&items)
                .await
                .into_iter()
                .next()
                .unwrap_or(Ok(()))
        }
    }

    /// Paginated prefix range-scan returning KEYS (the server returns keys
    /// only; callers point-get the values). Exclusive resume via the
    /// successor of the last key.
    async fn scan_keys(
        &self,
        prefix: &[u8],
        max_items: Option<usize>,
    ) -> Result<Vec<Vec<u8>>, AutumnError> {
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut start: Vec<u8> = Vec::new();
        loop {
            let res = self.client.range(prefix, &start, self.page_limit).await?;
            let n = res.entries.len();
            if n == 0 {
                break;
            }
            let last_key = res.entries[n - 1].key.clone();
            for e in res.entries {
                out.push(e.key);
                if let Some(m) = max_items {
                    if out.len() >= m {
                        return Ok(out);
                    }
                }
            }
            if n < self.page_limit as usize {
                break;
            }
            // `RangeReq.start` is INCLUSIVE; `last ++ 0x00` is the exact
            // "strictly after last" start (see the field's docs: the PS
            // compares user keys first, so this skips exactly last's own
            // MVCC versions and nothing else — no boundary duplicate to
            // drop, no `last ++ 0x00 ++ …` key lost).
            start = last_key;
            start.push(0);
        }
        Ok(out)
    }

    /// Point-get each key (BATCHED), skipping any that vanished (deleted/expired)
    /// between the scan and the get — near-real-time recall semantics (plan
    /// §8.5 #8). `get_many` keeps input order, so episodic newest-first /
    /// chronological ordering from the scan is preserved.
    async fn get_values(&self, key_list: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, AutumnError> {
        if key_list.is_empty() {
            return Ok(Vec::new());
        }
        let key_refs: Vec<&[u8]> = key_list.iter().map(|k| k.as_slice()).collect();
        let mut out = Vec::with_capacity(key_list.len());
        for v in self.client.get_many(&key_refs).await {
            if let Some(b) = v? {
                out.push(b);
            }
        }
        Ok(out)
    }

    // -- episodic ------------------------------------------------------------

    /// Append one event to a session's log. Returns its ns timestamp.
    pub async fn append_event(
        &self,
        session: &str,
        event: &[u8],
        ttl: Option<u64>,
    ) -> Result<u64, AutumnError> {
        let ts = now_ns();
        let ctr = self.next_seq();
        let key = keys::episodic_key(&self.tenant, &self.agent, session, ts, ctr);
        self.put_kv(&key, event, self.ttl(ttl)).await?;
        Ok(ts)
    }

    /// Most-recent `limit` events for a session, NEWEST-FIRST.
    pub async fn recent_events(
        &self,
        session: &str,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, AutumnError> {
        let prefix = keys::episodic_prefix(&self.tenant, &self.agent, session);
        let key_list = self.scan_keys(&prefix, Some(limit)).await?;
        self.get_values(&key_list).await
    }

    /// All events for a session in CHRONOLOGICAL (oldest-first) order.
    pub async fn replay_session(
        &self,
        session: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<u8>>, AutumnError> {
        let prefix = keys::episodic_prefix(&self.tenant, &self.agent, session);
        let mut key_list = self.scan_keys(&prefix, limit).await?;
        key_list.reverse();
        self.get_values(&key_list).await
    }

    // -- facts (LangGraph BaseStore model) -----------------------------------

    pub async fn put_fact(
        &self,
        namespace: &str,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> Result<(), AutumnError> {
        let k = keys::fact_key(&self.tenant, &self.agent, namespace, key);
        self.put_kv(&k, value, self.ttl(ttl)).await
    }

    pub async fn get_fact(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, AutumnError> {
        let k = keys::fact_key(&self.tenant, &self.agent, namespace, key);
        self.client.get(&k).await
    }

    pub async fn delete_fact(&self, namespace: &str, key: &str) -> Result<(), AutumnError> {
        let k = keys::fact_key(&self.tenant, &self.agent, namespace, key);
        self.client.delete(&k).await
    }

    /// List `(key, value)` pairs in a fact namespace (prefix scan + point-get).
    pub async fn list_facts(
        &self,
        namespace: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(String, Vec<u8>)>, AutumnError> {
        let prefix = keys::fact_prefix(&self.tenant, &self.agent, namespace);
        let key_list = self.scan_keys(&prefix, limit).await?;
        let key_refs: Vec<&[u8]> = key_list.iter().map(|k| k.as_slice()).collect();
        let vals = self.client.get_many(&key_refs).await; // batched (was serial)
        let mut out = Vec::with_capacity(key_list.len());
        for (k, v) in key_list.iter().zip(vals) {
            if let Some(b) = v? {
                out.push((keys::fact_key_name(k, &prefix), b));
            }
        }
        Ok(out)
    }
}

/// A scored hit from a lexical/vector memory search.
#[derive(Debug, Clone)]
pub struct ScoredDoc {
    pub id: String,
    pub text: String,
    pub meta: Vec<u8>,
    pub score: f32,
}

impl MemoryStore {
    // -- BM25-on-KV lexical retrieval (plan §7 lexical leg) -------------------

    async fn read_stats(&self) -> Result<(u64, u64), AutumnError> {
        let skey = keys::stats_key(&self.tenant, &self.agent);
        match self.client.get(&skey).await? {
            Some(b) if b.len() >= 16 => Ok((
                u64::from_le_bytes(b[..8].try_into().unwrap()),
                u64::from_le_bytes(b[8..16].try_into().unwrap()),
            )),
            _ => Ok((0, 0)),
        }
    }

    async fn update_stats(&self, d_docs: i64, d_len: i64) -> Result<(), AutumnError> {
        // Bulk mode: accumulate and return. This is what makes a bulk ingest
        // both fast and correct.
        //
        // Fast: the per-document path was a read-modify-write of one key, so a
        // 5164-chunk corpus paid 10328 round trips for a counter.
        //
        // Correct: that RMW is also why the ingest loop could not be run
        // concurrently. Concurrent writers to a single key lose updates — the
        // crate already documents that hazard for multi-PROCESS writers, and
        // overlapping the loop would have made it happen inside one process on
        // every ingest. Accumulating in a `Cell` sidesteps it: the read and
        // write of the cell straddle no await point, so on this single-threaded
        // runtime they cannot interleave.
        if let Some((n0, s0)) = self.pending_stats.get() {
            self.pending_stats.set(Some((n0 + d_docs, s0 + d_len)));
            return Ok(());
        }
        let (n, s) = self.read_stats().await?;
        let n = (n as i64 + d_docs).max(0) as u64;
        let s = (s as i64 + d_len).max(0) as u64;
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&n.to_le_bytes());
        buf[8..].copy_from_slice(&s.to_le_bytes());
        self.client
            .put(&keys::stats_key(&self.tenant, &self.agent), &buf)
            .await
    }

    /// Defer `meta/stats` maintenance until [`flush_stats`](Self::flush_stats).
    ///
    /// For bulk ingest. Without it every indexed document pays a read-modify-
    /// write of a single counter key, which is both the largest fixed cost in
    /// an ingest and the reason the caller's loop has to stay serial.
    ///
    /// The deltas are exact — nothing is dropped, only postponed — but the
    /// counter is STALE for the duration, so BM25 idf/avgdl computed by a
    /// concurrent reader reflect the pre-ingest corpus. That is the same
    /// eventual-consistency the crate already accepts for stats (see
    /// `repair_stats`), narrowed to a window the caller opens deliberately.
    ///
    /// Idempotent: calling it while a bulk is already open keeps the running
    /// total rather than discarding it.
    pub fn begin_bulk_index(&self) {
        if self.pending_stats.get().is_none() {
            self.pending_stats.set(Some((0, 0)));
        }
    }

    /// Apply everything accumulated since [`begin_bulk_index`] in ONE update,
    /// and leave bulk mode. A no-op when no bulk is open.
    ///
    /// Call it even on the error path of a partial ingest: the documents that
    /// did land are real, and skipping this leaves the counter understating the
    /// corpus until someone runs `repair_stats`.
    pub async fn flush_stats(&self) -> Result<(), AutumnError> {
        self.centroid_cache.borrow_mut().take();
        let Some((d_docs, d_len)) = self.pending_stats.take() else {
            return Ok(());
        };
        if d_docs == 0 && d_len == 0 {
            return Ok(());
        }
        self.update_stats(d_docs, d_len).await
    }

    /// Index (or re-index) a searchable memory document. `text` is tokenized
    /// into a BM25 inverted index; `meta` is opaque bytes returned with hits.
    ///
    /// The `doc` record is the COMMIT POINT — written AFTER its postings — so a
    /// crash mid-write never makes a half-indexed version recallable: the doc's
    /// own term map validates every posting at query time (plan §8.5, no
    /// generation stamping needed for the lexical leg).
    pub async fn index_memory(
        &self,
        doc_id: &str,
        text: &str,
        meta: &[u8],
        ttl: Option<u64>,
    ) -> Result<(), AutumnError> {
        let ttl = self.ttl(ttl);
        let (tf_map, doc_len) = recall::term_freqs(text);
        let dkey = keys::doc_key(&self.tenant, &self.agent, doc_id);
        let old = match self.client.get(&dkey).await? {
            Some(b) => recall::IndexedDoc::decode(&b),
            None => None,
        };
        let old_len = old.as_ref().map(|d| d.doc_len).unwrap_or(0);
        let is_new = old.is_none();

        // 1. postings (existence markers) for current terms — ONE batched write.
        //
        // These were written one term at a time, which is a round trip per
        // UNIQUE TERM per document. The CJK tokenizer emits unigrams, so a
        // ~1.5 KB Chinese chunk carries several hundred distinct terms, and
        // ingest cost a few hundred serial RTTs per chunk: measured 0.53 s per
        // chunk against a real cluster (~1 ms RTT) versus ~0.02 s on loopback,
        // i.e. the whole cost was latency, not work. A 5164-chunk corpus came
        // to ~45 minutes and read as a hang.
        //
        // Postings are empty-value existence markers, so batching them is
        // exactly what `put_many` is for. They are still written BEFORE the doc
        // record, which stays the commit point: a partial posting set with no
        // doc record is invisible to search (`search_lexical` resolves postings
        // through `doc/{id}`), while the reverse would let a doc be found with
        // terms missing from the index.
        let expires_at = if ttl == 0 {
            0
        } else {
            ClusterClient::ttl_to_expires_at(ttl)
        };
        let posting_keys: Vec<Vec<u8>> = tf_map
            .keys()
            .map(|term| keys::idx_posting_key(&self.tenant, &self.agent, term, doc_id))
            .collect();
        if !posting_keys.is_empty() {
            let items: Vec<(&[u8], Bytes, u64)> = posting_keys
                .iter()
                .map(|k| (k.as_slice(), Bytes::new(), expires_at))
                .collect();
            for r in self.client.put_many(&items).await {
                r?;
            }
        }
        // 2. the doc record — the COMMIT POINT
        let doc = recall::IndexedDoc {
            doc_len,
            terms: tf_map,
            text: text.to_string(),
            meta: meta.to_vec(),
        };
        self.put_kv(&dkey, &doc.encode(), ttl).await?;
        // 3. post-commit cleanup: drop postings for terms removed since last index
        if let Some(o) = old {
            // Batched for the same reason the writes above are: re-indexing an
            // edited chunk can drop hundreds of terms, and one delete per term
            // is one round trip per term.
            let stale: Vec<Vec<u8>> = o
                .terms
                .keys()
                .filter(|t| !doc.terms.contains_key(*t))
                .map(|t| keys::idx_posting_key(&self.tenant, &self.agent, t, doc_id))
                .collect();
            if !stale.is_empty() {
                let refs: Vec<&[u8]> = stale.iter().map(|k| k.as_slice()).collect();
                for r in self.client.delete_many(&refs).await {
                    r?;
                }
            }
        }
        // 4. corpus stats (single-writer per agent; shared -> delta-log, §8.5)
        self.update_stats(if is_new { 1 } else { 0 }, doc_len as i64 - old_len as i64)
            .await
    }

    /// Remove an indexed memory document and its postings (both legs: BM25
    /// postings + doc record, AND the IVF vector posting via `delete_vector`).
    pub async fn delete_memory(&self, doc_id: &str) -> Result<(), AutumnError> {
        // reap the vector leg first — unconditional, since a doc may have a
        // vector but no BM25 record (vector-only caller); the BM25 reap below
        // early-returns when there's no doc record.
        self.delete_vector(doc_id).await?;
        let dkey = keys::doc_key(&self.tenant, &self.agent, doc_id);
        let doc = match self.client.get(&dkey).await? {
            Some(b) => match recall::IndexedDoc::decode(&b) {
                Some(d) => d,
                None => return self.client.delete(&dkey).await,
            },
            None => return Ok(()),
        };
        // One batched reap, not one round trip per term — a CJK document
        // carries hundreds of distinct terms.
        let pks: Vec<Vec<u8>> = doc
            .terms
            .keys()
            .map(|term| keys::idx_posting_key(&self.tenant, &self.agent, term, doc_id))
            .collect();
        if !pks.is_empty() {
            let refs: Vec<&[u8]> = pks.iter().map(|k| k.as_slice()).collect();
            for r in self.client.delete_many(&refs).await {
                r?;
            }
        }
        self.client.delete(&dkey).await?;
        self.update_stats(-1, -(doc.doc_len as i64)).await
    }

    /// BM25 lexical search over indexed memories — up to `top_k` hits, highest
    /// score first. Near-real-time / eventually consistent: each candidate's
    /// doc is fetched and its current term map validates the posting (plan
    /// §8.5 #8); candidates that vanished between scan and get are skipped.
    pub async fn search_lexical(
        &self,
        query: &str,
        top_k: usize,
    ) -> Result<Vec<ScoredDoc>, AutumnError> {
        let q_terms = recall::query_terms(query);
        if q_terms.is_empty() {
            return Ok(Vec::new());
        }
        let (n_docs, sum_len) = self.read_stats().await?;
        if n_docs == 0 {
            return Ok(Vec::new());
        }
        let avgdl = sum_len as f32 / n_docs as f32;

        // candidate doc_ids per query term (keys-only posting scan) + df
        let mut df: HashMap<String, u64> = HashMap::new();
        let mut cands: HashMap<String, Vec<String>> = HashMap::new();
        for term in &q_terms {
            let tprefix = keys::idx_term_prefix(&self.tenant, &self.agent, term);
            let key_list = self.scan_keys(&tprefix, None).await?;
            df.insert(term.clone(), key_list.len() as u64);
            for k in &key_list {
                let doc_id = keys::idx_posting_doc_id(k, &tprefix);
                cands.entry(doc_id).or_default().push(term.clone());
            }
        }

        // Fetch + score candidate docs in BOUNDED CHUNKS. Batching the
        // per-candidate gets fixes the recall P99 bottleneck (a common term has
        // a long posting list → was hundreds of serial round-trips), but an
        // UNBOUNDED `get_many` over EVERY candidate would pull all their bodies
        // into one frame (OOM / frame-limit at scale, since BM25 has no early
        // termination). So chunk: one `MSG_BATCH_GET` per RECALL_GET_CHUNK
        // candidates, score the chunk, keep only a streaming top-k → memory +
        // frame size stay bounded. Result i ⇔ chunk[i]; a per-key error still
        // propagates (matches the old `?`).
        const RECALL_GET_CHUNK: usize = 256;
        let cand_list: Vec<(String, Vec<String>)> = cands.into_iter().collect();
        let trim_at = top_k.saturating_mul(8).max(64);
        // Same total order as `top_k_sorted`: ties by doc id. Candidates arrive
        // in `HashMap` order, so a score-only comparator leaves tied documents
        // in per-process-random order — and the streaming trim below then keeps
        // an arbitrary subset of them. With id as the tiebreak the trim is
        // exact (a document in the true top-k can never rank below the kept
        // top-k of any prefix) AND stable across runs.
        let by_score = |a: &ScoredDoc, b: &ScoredDoc| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        };

        let mut scored: Vec<ScoredDoc> = Vec::new();
        for chunk in cand_list.chunks(RECALL_GET_CHUNK) {
            let dkeys: Vec<Vec<u8>> = chunk
                .iter()
                .map(|(doc_id, _)| keys::doc_key(&self.tenant, &self.agent, doc_id))
                .collect();
            let key_refs: Vec<&[u8]> = dkeys.iter().map(|k| k.as_slice()).collect();
            let vals = self.client.get_many(&key_refs).await;
            for ((doc_id, terms), val) in chunk.iter().zip(vals) {
                let doc = match val? {
                    Some(b) => match recall::IndexedDoc::decode(&b) {
                        Some(d) => d,
                        None => continue,
                    },
                    None => continue, // vanished between scan and get
                };
                let mut score = 0.0f32;
                for term in terms {
                    if let Some(&tf) = doc.terms.get(term) {
                        let dft = *df.get(term).unwrap_or(&1);
                        score += recall::bm25_term(tf, dft, n_docs, doc.doc_len, avgdl);
                    }
                }
                if score > 0.0 {
                    scored.push(ScoredDoc {
                        id: doc_id.clone(),
                        text: doc.text,
                        meta: doc.meta,
                        score,
                    });
                }
            }
            // keep only the best top_k seen so far (a streaming top-k is exact:
            // a doc trimmed here ranked below the kept top_k and can't re-enter).
            if scored.len() > trim_at {
                scored.sort_by(by_score);
                scored.truncate(top_k);
            }
        }
        scored.sort_by(by_score);
        scored.truncate(top_k);
        Ok(scored)
    }

    /// Fetch a single indexed memory by id — the authoritative `doc/{id}`
    /// record (`text`, opaque `meta`). `None` if it was never indexed, was
    /// deleted/expired, or its record is unreadable. This is the point-get that
    /// backs an MCP `fetch` / "open this search hit" affordance: `search_*`
    /// returns ids+scores, `get_memory` resolves an id to its content.
    pub async fn get_memory(
        &self,
        doc_id: &str,
    ) -> Result<Option<(String, Vec<u8>)>, AutumnError> {
        let dkey = keys::doc_key(&self.tenant, &self.agent, doc_id);
        Ok(match self.client.get(&dkey).await? {
            Some(b) => recall::IndexedDoc::decode(&b).map(|d| (d.text, d.meta)),
            None => None,
        })
    }
}

impl MemoryStore {
    // -- SPFresh-IVF-on-KV vector retrieval (plan §7 vector leg) --------------
    // Vectors are caller-supplied (`&[f32]`); embedding is sglang's job.

    /// Load the centroid table, memoised while a bulk index is open.
    ///
    /// `index_vector` needs it for EVERY document, so an uncached read is a
    /// round trip (plus a full decode of the k×dim table) per chunk — pure
    /// repetition, since the table only changes in `train_centroids`, which the
    /// caller runs after the ingest, not during it. The cache is scoped to the
    /// bulk window rather than left on permanently so a long-lived store still
    /// sees another writer's retraining.
    async fn load_centroids(&self) -> Result<Option<Rc<vector::Centroids>>, AutumnError> {
        if let Some(hit) = self.centroid_cache.borrow().as_ref() {
            return Ok(hit.clone());
        }
        let key = keys::ivf_centroids_key(&self.tenant, &self.agent);
        let got = self
            .client
            .get(&key)
            .await?
            .and_then(|b| vector::Centroids::decode(&b))
            .map(Rc::new);
        if self.pending_stats.get().is_some() {
            *self.centroid_cache.borrow_mut() = Some(got.clone());
        }
        Ok(got)
    }

    async fn store_centroids(&self, c: &vector::Centroids) -> Result<(), AutumnError> {
        self.centroid_cache.borrow_mut().take();
        let key = keys::ivf_centroids_key(&self.tenant, &self.agent);
        self.client.put(&key, &c.encode()).await
    }

    /// Index a vector for `doc_id` into its nearest centroid bucket (bucket 0
    /// before the index is trained). Re-indexing overwrites that bucket's copy;
    /// a stale copy in another bucket is reaped by the next `train_centroids`,
    /// and `search_vector` dedups by id meanwhile.
    pub async fn index_vector(
        &self,
        doc_id: &str,
        vec: &[f32],
        ttl: Option<u64>,
    ) -> Result<(), AutumnError> {
        let nv = vector::normalize(vec);
        let centroid = match self.load_centroids().await? {
            Some(c) if !c.cs.is_empty() => vector::nearest(&nv, &c.cs) as u32,
            _ => 0,
        };
        let ttl = self.ttl(ttl);
        let vptr = keys::ivf_vptr_key(&self.tenant, &self.agent, doc_id);
        // Re-index that lands in a different bucket: reap the prior posting so
        // exactly ONE IVF copy exists per id — this keeps `delete_vector`'s
        // vptr-based reap exact (one pointer, one posting).
        if let Some(b) = self.client.get(&vptr).await? {
            if let Some(old) = read_u32(&b) {
                if old != centroid {
                    let old_pk = keys::ivf_posting_key(&self.tenant, &self.agent, old, doc_id);
                    self.client.delete(&old_pk).await?;
                }
            }
        }
        let key = keys::ivf_posting_key(&self.tenant, &self.agent, centroid, doc_id);
        self.put_kv(&key, &vector::encode_vec(&nv), ttl).await?;
        // The posting + vptr are two non-atomic writes. If the vptr write fails
        // we'd leave a posting that the vptr-based `delete_vector` can never find
        // (a silent, unreapable orphan), so compensate: undo the posting and
        // surface an error — better a failed index than an orphan. (same TTL on
        // vptr → it expires with its vector, so no orphan vptr either.) Residual:
        // only a double-fault (vptr write AND this compensating delete both fail)
        // leaves an orphan, and that is still correctness-backstopped by the
        // resolver dropping hits whose doc record is gone.
        if let Err(e) = self.put_kv(&vptr, &centroid.to_be_bytes(), ttl).await {
            self.client.delete(&key).await?;
            return Err(e);
        }
        Ok(())
    }

    /// Reap a doc's IVF vector posting (and its reverse pointer). The
    /// `vptr/{id}` -> centroid pointer locates the bucket in O(1) (no scan).
    /// No-op if the id has no vector. `delete_memory` calls this so deleting a
    /// memory reaps BOTH legs; usable on its own for vector-only callers.
    ///
    /// Note `train_centroids` re-buckets every IVF posting it scans WITHOUT
    /// checking doc existence — it never reaps a deleted vector — so this
    /// vptr-based reap is the only reaper for delete-orphans.
    pub async fn delete_vector(&self, doc_id: &str) -> Result<(), AutumnError> {
        let vptr = keys::ivf_vptr_key(&self.tenant, &self.agent, doc_id);
        if let Some(b) = self.client.get(&vptr).await? {
            if let Some(centroid) = read_u32(&b) {
                let pk = keys::ivf_posting_key(&self.tenant, &self.agent, centroid, doc_id);
                self.client.delete(&pk).await?;
            }
            self.client.delete(&vptr).await?;
        }
        Ok(())
    }

    /// (Re)train IVF centroids with k-means over the current vectors and
    /// re-assign every vector to its new bucket; returns the centroid count.
    /// Reassign is write-new-then-delete-old, so a crash leaves a vector in two
    /// buckets at worst (search dedups by id) — never lost.
    pub async fn train_centroids(
        &self,
        nlist: usize,
        max_iter: usize,
        seed: u64,
    ) -> Result<usize, AutumnError> {
        let all = keys::ivf_all_prefix(&self.tenant, &self.agent);
        let key_list = self.scan_keys(&all, None).await?;
        let mut items: Vec<(String, u32, Vec<f32>)> = Vec::new();
        let vals = self.get_all(&key_list).await?;
        for (k, val) in key_list.iter().zip(vals) {
            let (Some(vid), Some(oc)) =
                (keys::ivf_all_vec_id(k, &all), keys::ivf_all_centroid(k, &all))
            else {
                continue;
            };
            if let Some(b) = val {
                if let Some(v) = vector::decode_vec(&b) {
                    items.push((vid, oc, v));
                }
            }
        }
        if items.is_empty() {
            return Ok(0);
        }
        let vecs: Vec<Vec<f32>> = items.iter().map(|(_, _, v)| v.clone()).collect();
        let cs = vector::kmeans(&vecs, nlist, max_iter, seed);
        if cs.is_empty() {
            return Ok(0);
        }
        let epoch = self.load_centroids().await?.map(|c| c.epoch).unwrap_or(0) + 1;
        let dim = vecs[0].len() as u32;
        self.store_centroids(&vector::Centroids {
            epoch,
            dim,
            cs: cs.clone(),
        })
        .await?;
        // Re-bucket in three batched PHASES rather than three writes per
        // vector. This loop was 2-3 serial round trips per vector — 10k+ of
        // them for a 5k-document corpus, which measured at 88 s and was the
        // whole cost of `reconcile + train_centroids` once the reads were
        // batched. The vectors themselves are already in memory.
        //
        // The phase ORDER carries the same guarantee the per-vector loop did,
        // and must not be reordered: the new posting exists before any vptr
        // names its bucket, and an old posting is dropped only after the vptr
        // has moved off it. Deleting first would leave a vptr pointing at a
        // bucket with no posting — an entry `delete_vector` can never reap.
        let mut postings: Vec<(Vec<u8>, Bytes)> = Vec::with_capacity(items.len());
        let mut vptrs: Vec<(Vec<u8>, Bytes)> = Vec::with_capacity(items.len());
        let mut stale: Vec<Vec<u8>> = Vec::new();
        for (vid, oc, v) in &items {
            let nc = vector::nearest(v, &cs) as u32;
            postings.push((
                keys::ivf_posting_key(&self.tenant, &self.agent, nc, vid),
                Bytes::from(vector::encode_vec(v)),
            ));
            // keep the reverse pointer accurate after re-bucketing (so a later
            // delete reaps the posting from its CURRENT bucket).
            vptrs.push((
                keys::ivf_vptr_key(&self.tenant, &self.agent, vid),
                Bytes::copy_from_slice(&nc.to_be_bytes()),
            ));
            if nc != *oc {
                stale.push(keys::ivf_posting_key(&self.tenant, &self.agent, *oc, vid));
            }
        }
        self.put_all(&postings).await?;
        self.put_all(&vptrs).await?;
        self.delete_all(&stale).await?;
        Ok(cs.len())
    }

    /// ANN search: probe the `nprobe` nearest centroid buckets, cosine-rerank,
    /// top-k (id, score). Before training (no centroids) brute-forces every
    /// vector. Dedups by id.
    pub async fn search_vector(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
    ) -> Result<Vec<(String, f32)>, AutumnError> {
        let q = vector::normalize(query);
        let buckets: Vec<u32> = match self.load_centroids().await? {
            Some(c) if !c.cs.is_empty() => vector::nearest_centroids(&q, &c.cs, nprobe)
                .into_iter()
                .map(|i| i as u32)
                .collect(),
            _ => return self.search_vector_bruteforce(&q, top_k).await,
        };
        // Collect the probed buckets' keys first, then read them in one batch:
        // a point-get per candidate put the whole probe on the round-trip
        // critical path, and this is the query path.
        let mut cand: Vec<(String, Vec<u8>)> = Vec::new();
        for centroid in buckets {
            let bprefix = keys::ivf_bucket_prefix(&self.tenant, &self.agent, centroid);
            for k in self.scan_keys(&bprefix, None).await? {
                let vid = keys::ivf_vec_id(&k, &bprefix);
                cand.push((vid, k));
            }
        }
        let mut best: HashMap<String, f32> = HashMap::new();
        let ckeys: Vec<Vec<u8>> = cand.iter().map(|(_, k)| k.clone()).collect();
        let vals = self.get_all(&ckeys).await?;
        for ((vid, _), val) in cand.into_iter().zip(vals) {
            if let Some(b) = val {
                if let Some(v) = vector::decode_vec(&b) {
                    rerank_insert(&mut best, vid, vector::cosine(&q, &v));
                }
            }
        }
        Ok(top_k_sorted(best, top_k))
    }

    async fn search_vector_bruteforce(
        &self,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<(String, f32)>, AutumnError> {
        let all = keys::ivf_all_prefix(&self.tenant, &self.agent);
        let mut best: HashMap<String, f32> = HashMap::new();
        let key_list = self.scan_keys(&all, None).await?;
        let vals = self.get_all(&key_list).await?;
        for (k, val) in key_list.iter().zip(vals) {
            let Some(vid) = keys::ivf_all_vec_id(k, &all) else {
                continue;
            };
            if let Some(b) = val {
                if let Some(v) = vector::decode_vec(&b) {
                    rerank_insert(&mut best, vid, vector::cosine(query, &v));
                }
            }
        }
        Ok(top_k_sorted(best, top_k))
    }

    /// Hybrid retrieval: BM25 lexical (`query_text`) + IVF vector (`query_vec`,
    /// supplied by the caller's embedder) fused with Reciprocal Rank Fusion.
    /// Returns up to `top_k` `(doc_id, fused_score)`, highest first. Each leg is
    /// over-fetched so the fusion has depth; RRF is ordinal so the two legs need
    /// no score normalization (plan §7 hybrid — the lexical leg catches literal
    /// tokens the vector leg misses, and vice-versa).
    pub async fn search_hybrid(
        &self,
        query_text: &str,
        query_vec: &[f32],
        top_k: usize,
        nprobe: usize,
    ) -> Result<Vec<(String, f32)>, AutumnError> {
        let depth = top_k.max(10) * 2;
        let lex: Vec<String> = self
            .search_lexical(query_text, depth)
            .await?
            .into_iter()
            .map(|d| d.id)
            .collect();
        let vec: Vec<String> = self
            .search_vector(query_vec, depth, nprobe)
            .await?
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        Ok(top_k_sorted(recall::rrf_fuse(&[lex, vec], 60.0), top_k))
    }
}

/// Snapshot of an agent's index integrity (plan §16 acceptance: index-reconcile).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Live `doc/{id}` records counted by a full scan.
    pub docs: u64,
    /// Σ of live docs' `doc_len` (recount).
    pub sum_doc_len: u64,
    /// `meta/stats` n_docs as currently stored.
    pub stats_docs: u64,
    /// `meta/stats` sum_doc_len as currently stored.
    pub stats_sum_doc_len: u64,
    /// IVF vector postings counted by a full scan (ACTUAL postings, not unique
    /// ids — a `vec_id` with copies in two buckets counts as 2).
    pub ivf_postings: u64,
    /// reverse pointers (`ivf_meta/vptr/*`) counted by a full scan.
    pub vptrs: u64,
    /// IVF postings whose `vec_id` has NO reverse pointer — unreapable orphans
    /// (the `index_vector` double-fault residual). Should be 0.
    pub orphan_ivf: u64,
    /// extra IVF postings beyond one per `vec_id` — e.g. a `train_centroids`
    /// crash that wrote the new-bucket posting but didn't delete the old one.
    /// Should be 0.
    pub duplicate_ivf: u64,
    /// reverse pointers whose pointed-to bucket holds no posting — dangling
    /// (e.g. a compensated/partial write left the vptr but not the posting).
    pub dangling_vptr: u64,
    /// reverse-pointer values that failed to decode (not exactly 4 bytes) —
    /// corruption. Should be 0.
    pub malformed_vptr: u64,
    /// graph node records counted by a full `node/` scan.
    pub nodes: u64,
    /// forward graph edges counted by a full `edge/` scan.
    pub edges: u64,
    /// reverse-edge markers (`redge/*`) counted by a full scan.
    pub redges: u64,
    /// forward edges whose `src` or `dst` node record is absent — dangling
    /// (e.g. a node deleted without reaping an incident edge). Should be 0.
    pub dangling_edge: u64,
    /// reverse markers with no matching forward edge — orphan hint (e.g. an
    /// `add_edge` crash after the redge write, or a partial delete). Should be 0.
    pub orphan_redge: u64,
    /// forward edges with no matching reverse marker — the reverse index is
    /// missing an entry, so `in_edges` would miss it. Should be 0.
    pub missing_redge: u64,
}

impl ReconcileReport {
    /// `meta/stats` exactly matches the recount (no multi-writer drift).
    pub fn stats_consistent(&self) -> bool {
        self.docs == self.stats_docs && self.sum_doc_len == self.stats_sum_doc_len
    }
    /// Every STRUCTURAL invariant holds: stats match + no orphan / duplicate /
    /// dangling / malformed vectors. NOTE: this is structural integrity, NOT
    /// centroid-assignment optimality — a `train_centroids` crash after the
    /// manifest write but before re-bucketing a vector can leave a posting in a
    /// now-suboptimal bucket (an ANN-recall quality issue, not corruption; the
    /// vector still exists and is reaped/found correctly). Re-running
    /// `train_centroids` re-establishes optimal buckets; a dedicated
    /// `stale_bucket` check (decode every posting + recompute nearest) is a
    /// future enhancement, deliberately omitted here to keep the audit O(ids).
    pub fn is_clean(&self) -> bool {
        self.stats_consistent()
            && self.orphan_ivf == 0
            && self.duplicate_ivf == 0
            && self.dangling_vptr == 0
            && self.malformed_vptr == 0
            && self.dangling_edge == 0
            && self.orphan_redge == 0
            && self.missing_redge == 0
    }
}

impl MemoryStore {
    // -- index reconcile / repair (plan §16 acceptance) ----------------------
    // An OFF-hot-path integrity audit. This is also the deliberate answer to the
    // multi-writer `meta/stats` RMW race: rather than serialize the hot write
    // path (per-writer shards / server atomics — rejected as hot-path/server
    // complexity for a LOW-harm, idf/avgdl-only score-skew that needs a
    // non-primary multi-process-same-agent topology), tolerate the drift and
    // detect + `repair_stats` it here.

    /// Recount live docs + Σ doc_len from the authoritative `doc/{id}` records
    /// (the BM25-stats ground truth). Scans `doc/` only — no IVF work.
    /// Point-get many keys, batched. `reconcile` walks every doc and every
    /// vptr, and a serial `get` per key made it one round trip per document:
    /// measured at 84 s wall for ~2 s of CPU, i.e. 98% waiting. Chunked so a
    /// large corpus doesn't build one enormous request.
    async fn get_all(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, AutumnError> {
        const BATCH: usize = 256;
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(BATCH) {
            let refs: Vec<&[u8]> = chunk.iter().map(|k| k.as_slice()).collect();
            for r in self.client.get_many(&refs).await {
                out.push(r?);
            }
        }
        Ok(out)
    }

    /// Write many keys, batched and chunked like `get_all`. TTL-free: every
    /// caller of this is index maintenance, whose lifetime is the doc's.
    async fn put_all(&self, items: &[(Vec<u8>, Bytes)]) -> Result<(), AutumnError> {
        const BATCH: usize = 256;
        for chunk in items.chunks(BATCH) {
            let refs: Vec<(&[u8], Bytes, u64)> =
                chunk.iter().map(|(k, v)| (k.as_slice(), v.clone(), 0u64)).collect();
            for r in self.client.put_many(&refs).await {
                r?;
            }
        }
        Ok(())
    }

    /// Delete many keys, batched and chunked. A no-op on an empty slice.
    async fn delete_all(&self, keys: &[Vec<u8>]) -> Result<(), AutumnError> {
        const BATCH: usize = 256;
        for chunk in keys.chunks(BATCH) {
            let refs: Vec<&[u8]> = chunk.iter().map(|k| k.as_slice()).collect();
            for r in self.client.delete_many(&refs).await {
                r?;
            }
        }
        Ok(())
    }

    async fn recount_doc_stats(&self) -> Result<(u64, u64), AutumnError> {
        let doc_keys = self.scan_keys(&keys::doc_prefix(&self.tenant, &self.agent), None).await?;
        let (mut docs, mut sum_len) = (0u64, 0u64);
        for b in self.get_all(&doc_keys).await?.into_iter().flatten() {
            if let Some(d) = recall::IndexedDoc::decode(&b) {
                docs += 1;
                sum_len += d.doc_len as u64;
            }
        }
        Ok((docs, sum_len))
    }

    /// Audit this agent's index integrity: recount doc stats vs `meta/stats`,
    /// and cross-check IVF postings against their reverse pointers. Read-only.
    /// Structural integrity only — see [`ReconcileReport::is_clean`] on the
    /// centroid-assignment boundary.
    pub async fn reconcile(&self) -> Result<ReconcileReport, AutumnError> {
        let mut r = ReconcileReport::default();

        // 1. docs vs stats
        let (docs, sum_len) = self.recount_doc_stats().await?;
        r.docs = docs;
        r.sum_doc_len = sum_len;
        let (sd, ss) = self.read_stats().await?;
        r.stats_docs = sd;
        r.stats_sum_doc_len = ss;

        // 2. IVF postings: vec_id -> the bucket(s) it has a posting in. Do NOT
        //    fold to one centroid — duplicate postings (same id in two buckets,
        //    e.g. a train-crash residual) must be counted, not hidden.
        let all = keys::ivf_all_prefix(&self.tenant, &self.agent);
        let post_keys = self.scan_keys(&all, None).await?;
        let mut ivf: HashMap<String, Vec<u32>> = HashMap::new();
        for k in &post_keys {
            if let (Some(vid), Some(c)) =
                (keys::ivf_all_vec_id(k, &all), keys::ivf_all_centroid(k, &all))
            {
                ivf.entry(vid).or_default().push(c);
            }
        }
        r.ivf_postings = ivf.values().map(|v| v.len() as u64).sum();
        r.duplicate_ivf = ivf.values().filter(|v| v.len() > 1).map(|v| v.len() as u64 - 1).sum();

        // 3. reverse pointers: vec_id -> centroid (from the vptr value)
        let vpre = keys::ivf_vptr_prefix(&self.tenant, &self.agent);
        let vptr_keys = self.scan_keys(&vpre, None).await?;
        let mut vptr: HashMap<String, u32> = HashMap::new();
        let vptr_vals = self.get_all(&vptr_keys).await?;
        for (k, val) in vptr_keys.iter().zip(vptr_vals) {
            let vid = keys::ivf_vptr_vec_id(k, &vpre);
            // A key that vanished between scan and get is not an inconsistency.
            if let Some(b) = val {
                match read_u32(&b) {
                    Some(c) => {
                        vptr.insert(vid, c);
                    }
                    None => r.malformed_vptr += 1, // present but undecodable = corruption
                }
            }
        }
        r.vptrs = vptr.len() as u64;

        // 4. cross-check: every posting's id must have a vptr; every vptr's
        //    bucket must hold a posting for that id.
        for (vid, centroids) in &ivf {
            if !vptr.contains_key(vid) {
                r.orphan_ivf += centroids.len() as u64;
            }
        }
        for (vid, c) in &vptr {
            if !ivf.get(vid).map(|cs| cs.contains(c)).unwrap_or(false) {
                r.dangling_vptr += 1;
            }
        }

        // 5. graph integrity: node ids, forward edges, reverse markers, and the
        //    two directional cross-checks (edge endpoints exist; fwd<->redge
        //    agree). Same shape as the IVF orphan/dangling checks above.
        let node_all = keys::node_prefix(&self.tenant, &self.agent);
        let node_ids: HashSet<String> = self
            .scan_keys(&node_all, None)
            .await?
            .iter()
            .map(|k| keys::node_id_name(k, &node_all))
            .collect();
        r.nodes = node_ids.len() as u64;

        let eall = keys::edge_all_prefix(&self.tenant, &self.agent);
        let mut fwd: HashSet<(String, String, String)> = HashSet::new();
        for k in &self.scan_keys(&eall, None).await? {
            if let Some((src, et, dst)) = keys::edge_all_parse(k, &eall) {
                if !node_ids.contains(&src) || !node_ids.contains(&dst) {
                    r.dangling_edge += 1;
                }
                fwd.insert((src, et, dst));
            }
        }
        r.edges = fwd.len() as u64;

        let rall = keys::redge_all_prefix(&self.tenant, &self.agent);
        let mut rev: HashSet<(String, String, String)> = HashSet::new();
        for k in &self.scan_keys(&rall, None).await? {
            if let Some((dst, et, src)) = keys::redge_all_parse(k, &rall) {
                if !fwd.contains(&(src.clone(), et.clone(), dst.clone())) {
                    r.orphan_redge += 1;
                }
                rev.insert((src, et, dst));
            }
        }
        r.redges = rev.len() as u64;
        r.missing_redge = fwd.iter().filter(|e| !rev.contains(*e)).count() as u64;

        Ok(r)
    }

    /// Rewrite `meta/stats` from a fresh doc recount, healing multi-writer
    /// drift. Returns the repaired `(n_docs, sum_doc_len)`.
    ///
    /// MUST run in a maintenance window with NO concurrent writer for this
    /// `(tenant, agent)`: it is itself a read(recount)-then-write, with no CAS,
    /// so a write landing between the recount and the put would be clobbered.
    /// (Scans `doc/` only — independent of IVF size.)
    pub async fn repair_stats(&self) -> Result<(u64, u64), AutumnError> {
        let (docs, sum_len) = self.recount_doc_stats().await?;
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&docs.to_le_bytes());
        buf[8..].copy_from_slice(&sum_len.to_le_bytes());
        self.client
            .put(&keys::stats_key(&self.tenant, &self.agent), &buf)
            .await?;
        Ok((docs, sum_len))
    }
}

/// A node in the generic memory graph.
#[derive(Debug, Clone)]
pub struct GraphNode {
    pub id: String,
    pub kind: String,
    pub attrs: Vec<u8>,
}

/// A directed, typed edge `src --etype--> dst` with opaque `attrs` bytes.
#[derive(Debug, Clone)]
pub struct Edge {
    pub src: String,
    pub etype: String,
    pub dst: String,
    pub attrs: Vec<u8>,
}

/// Edge direction for `neighbors` / `bfs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    /// Follow outgoing edges (`src -> dst`).
    Out,
    /// Follow incoming edges (`dst <- src`).
    In,
}

impl MemoryStore {
    // -- generic graph: nodes + typed edges as adjacency lists ---------------
    // Traversal is prefix range-scan on the ordered KV: out-edges scan
    // `edge/{src}/`, in-edges scan `redge/{dst}/`. The forward `edge/*` record
    // is authoritative; the `redge/*` marker is a derived reverse index (a
    // hint) validated against the forward edge at read time — the same
    // "posting = candidate, authoritative record = correctness boundary"
    // contract as the BM25 leg (plan §8.5). Domain-agnostic: node ids / edge
    // types are opaque strings, attrs opaque bytes.

    /// Upsert a node: authoritative `node/{id}` record + `nidx/{kind}/{id}`
    /// by-kind index marker. Re-`put_node`ing with a different `kind` leaves a
    /// stale `nidx` marker under the OLD kind (a discardable hint; `delete_node`
    /// clears the current one) — callers that re-kind should `delete_node` first.
    pub async fn put_node(
        &self,
        id: &str,
        kind: &str,
        attrs: &[u8],
        ttl: Option<u64>,
    ) -> Result<(), AutumnError> {
        let ttl = self.ttl(ttl);
        let rec = graph::NodeRecord {
            kind: kind.to_string(),
            attrs: attrs.to_vec(),
        };
        self.put_kv(&keys::node_key(&self.tenant, &self.agent, id), &rec.encode(), ttl)
            .await?;
        self.put_kv(&keys::nidx_key(&self.tenant, &self.agent, kind, id), &[], ttl)
            .await
    }

    /// Fetch a node record, or `None` if it does not exist / fails to decode.
    pub async fn get_node(&self, id: &str) -> Result<Option<GraphNode>, AutumnError> {
        let key = keys::node_key(&self.tenant, &self.agent, id);
        Ok(self.client.get(&key).await?.and_then(|b| {
            graph::NodeRecord::decode(&b).map(|r| GraphNode {
                id: id.to_string(),
                kind: r.kind,
                attrs: r.attrs,
            })
        }))
    }

    /// Delete a node together with ALL its incident edges (both directions).
    /// Bounded by the node's degree (out-scan `edge/{id}/` + in-scan
    /// `redge/{id}/`) — potentially many point-deletes for a hub node.
    pub async fn delete_node(&self, id: &str) -> Result<(), AutumnError> {
        // Collect every key first, delete in batches: a hub node's degree can
        // be thousands, and two point-deletes per edge is two round trips per
        // edge.
        let mut doomed: Vec<Vec<u8>> = Vec::new();
        // Outgoing edges: drop each forward edge + its reverse marker.
        let sp = keys::edge_src_prefix(&self.tenant, &self.agent, id);
        for k in self.scan_keys(&sp, None).await? {
            if let Some((etype, dst)) = keys::edge_parse_tail(&k, &sp) {
                doomed.push(keys::redge_key(&self.tenant, &self.agent, id, &etype, &dst));
                doomed.push(k);
            }
        }
        // Incoming edges: drop the forward edge (by rebuilding its key) + marker.
        let rp = keys::redge_dst_prefix(&self.tenant, &self.agent, id);
        for k in self.scan_keys(&rp, None).await? {
            if let Some((etype, src)) = keys::redge_parse_tail(&k, &rp) {
                doomed.push(keys::edge_key(&self.tenant, &self.agent, &src, &etype, id));
                doomed.push(k);
            }
        }
        self.delete_all(&doomed).await?;
        // The by-kind index marker (need the kind — read the record first).
        if let Some(node) = self.get_node(id).await? {
            self.client
                .delete(&keys::nidx_key(&self.tenant, &self.agent, &node.kind, id))
                .await?;
        }
        self.client
            .delete(&keys::node_key(&self.tenant, &self.agent, id))
            .await
    }

    /// Add a directed edge. Writes the reverse marker FIRST (the hint), then the
    /// authoritative forward edge (the commit point) — a crash in between leaves
    /// only a dangling `redge`, dropped by `in_edges` validation and surfaced by
    /// `reconcile`. Idempotent: deterministic keys, retry never duplicates.
    pub async fn add_edge(
        &self,
        src: &str,
        etype: &str,
        dst: &str,
        attrs: &[u8],
        ttl: Option<u64>,
    ) -> Result<(), AutumnError> {
        let ttl = self.ttl(ttl);
        self.put_kv(&keys::redge_key(&self.tenant, &self.agent, src, etype, dst), &[], ttl)
            .await?;
        self.put_kv(&keys::edge_key(&self.tenant, &self.agent, src, etype, dst), attrs, ttl)
            .await
    }

    /// Remove a directed edge: authoritative forward key first (so reads stop
    /// seeing it), then the reverse marker.
    pub async fn delete_edge(&self, src: &str, etype: &str, dst: &str) -> Result<(), AutumnError> {
        self.client
            .delete(&keys::edge_key(&self.tenant, &self.agent, src, etype, dst))
            .await?;
        self.client
            .delete(&keys::redge_key(&self.tenant, &self.agent, src, etype, dst))
            .await
    }

    /// Outgoing edges of `src` (optionally filtered to one `etype`). Forward
    /// edges are authoritative — no validation needed.
    pub async fn out_edges(
        &self,
        src: &str,
        etype: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Edge>, AutumnError> {
        let scan_prefix = match etype {
            Some(t) => keys::edge_src_type_prefix(&self.tenant, &self.agent, src, t),
            None => keys::edge_src_prefix(&self.tenant, &self.agent, src),
        };
        // Parse (etype, dst) relative to the src prefix regardless of filter.
        let src_prefix = keys::edge_src_prefix(&self.tenant, &self.agent, src);
        let key_list = self.scan_keys(&scan_prefix, limit).await?;
        let key_refs: Vec<&[u8]> = key_list.iter().map(|k| k.as_slice()).collect();
        let vals = self.client.get_many(&key_refs).await;
        let mut out = Vec::with_capacity(key_list.len());
        for (k, v) in key_list.iter().zip(vals) {
            // A key that vanished between scan and get is simply skipped.
            let attrs = match v? {
                Some(b) => b,
                None => continue,
            };
            if let Some((et, dst)) = keys::edge_parse_tail(k, &src_prefix) {
                out.push(Edge {
                    src: src.to_string(),
                    etype: et,
                    dst,
                    attrs,
                });
            }
        }
        Ok(out)
    }

    /// Incoming edges of `dst` (optionally filtered to one `etype`). Reverse
    /// markers are hints — each is validated against its authoritative forward
    /// edge (a vanished/dangling marker is dropped); attrs come from the forward
    /// edge value.
    pub async fn in_edges(
        &self,
        dst: &str,
        etype: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Edge>, AutumnError> {
        let scan_prefix = match etype {
            Some(t) => keys::redge_dst_type_prefix(&self.tenant, &self.agent, dst, t),
            None => keys::redge_dst_prefix(&self.tenant, &self.agent, dst),
        };
        let dst_prefix = keys::redge_dst_prefix(&self.tenant, &self.agent, dst);
        let key_list = self.scan_keys(&scan_prefix, limit).await?;
        let parsed: Vec<(String, String)> = key_list
            .iter()
            .filter_map(|k| keys::redge_parse_tail(k, &dst_prefix))
            .collect();
        let fwd_keys: Vec<Vec<u8>> = parsed
            .iter()
            .map(|(et, src)| keys::edge_key(&self.tenant, &self.agent, src, et, dst))
            .collect();
        let refs: Vec<&[u8]> = fwd_keys.iter().map(|k| k.as_slice()).collect();
        let vals = self.client.get_many(&refs).await;
        let mut out = Vec::with_capacity(parsed.len());
        for ((et, src), v) in parsed.into_iter().zip(vals) {
            if let Some(attrs) = v? {
                out.push(Edge {
                    src,
                    etype: et,
                    dst: dst.to_string(),
                    attrs,
                });
            }
        }
        Ok(out)
    }

    /// Neighboring node ids in `dir` (optionally filtered to one `etype`).
    pub async fn neighbors(
        &self,
        id: &str,
        dir: Dir,
        etype: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<String>, AutumnError> {
        Ok(match dir {
            Dir::Out => self
                .out_edges(id, etype, limit)
                .await?
                .into_iter()
                .map(|e| e.dst)
                .collect(),
            Dir::In => self
                .in_edges(id, etype, limit)
                .await?
                .into_iter()
                .map(|e| e.src)
                .collect(),
        })
    }

    /// List node ids of a given `kind` (prefix scan of `nidx/{kind}/`).
    pub async fn nodes_by_kind(
        &self,
        kind: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>, AutumnError> {
        let prefix = keys::nidx_kind_prefix(&self.tenant, &self.agent, kind);
        let key_list = self.scan_keys(&prefix, limit).await?;
        Ok(key_list.iter().map(|k| keys::nidx_id_name(k, &prefix)).collect())
    }

    /// Bounded breadth-first traversal from `start` following `dir` edges
    /// (optionally one `etype`). Returns `(node_id, depth)` in BFS order,
    /// including `start` at depth 0. Stops at `max_depth` (edges) or once
    /// `max_nodes` are collected — the guards against unbounded fan-out.
    pub async fn bfs(
        &self,
        start: &str,
        dir: Dir,
        etype: Option<&str>,
        max_depth: u32,
        max_nodes: usize,
    ) -> Result<Vec<(String, u32)>, AutumnError> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, u32)> = VecDeque::new();
        let mut out: Vec<(String, u32)> = Vec::new();
        visited.insert(start.to_string());
        queue.push_back((start.to_string(), 0));
        while let Some((node, depth)) = queue.pop_front() {
            out.push((node.clone(), depth));
            if out.len() >= max_nodes {
                break;
            }
            if depth >= max_depth {
                continue;
            }
            for nb in self.neighbors(&node, dir, etype, None).await? {
                if visited.insert(nb.clone()) {
                    queue.push_back((nb, depth + 1));
                }
            }
        }
        Ok(out)
    }
}

fn rerank_insert(best: &mut HashMap<String, f32>, id: String, score: f32) {
    best.entry(id)
        .and_modify(|e| {
            if score > *e {
                *e = score
            }
        })
        .or_insert(score);
}

fn top_k_sorted(best: HashMap<String, f32>, top_k: usize) -> Vec<(String, f32)> {
    let mut out: Vec<(String, f32)> = best.into_iter().collect();
    // Ties break by doc id, which is what makes this a TOTAL order and the
    // result reproducible. Sorting by score alone left ties in `HashMap`
    // iteration order — random per process — so two runs of the same query
    // against the same index could return different documents, not merely a
    // different order: `truncate` cuts through the tied group. RRF makes that
    // the common case rather than a corner one, because a fused score is a sum
    // of `1/(k+rank)` terms and any two documents holding the same rank in one
    // leg and absent from the other are exactly equal. Retrieval quality cannot
    // be measured against a baseline that moves on its own.
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    out.truncate(top_k);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `top_k_sorted` must be a TOTAL order, or a baseline cannot be compared
    /// against: its input is a `HashMap`, whose iteration order is randomised
    /// per process, so a score-only comparator lets `truncate` keep a different
    /// subset of a tied group on every run.
    #[test]
    fn top_k_breaks_ties_by_id_so_results_are_reproducible() {
        let tied = || {
            let mut m: HashMap<String, f32> = HashMap::new();
            for id in ["d", "b", "a", "c"] {
                m.insert(id.to_string(), 0.5); // one fully tied group
            }
            m.insert("z".into(), 0.9);
            m
        };
        let first = top_k_sorted(tied(), 3);
        assert_eq!(first[0].0, "z", "score still dominates the tiebreak");
        assert_eq!(
            first.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            vec!["z", "a", "b"],
        );
        // Same input, fresh HashMaps: the cut through the tied group is stable.
        for _ in 0..16 {
            assert_eq!(top_k_sorted(tied(), 3), first);
        }
    }
}
