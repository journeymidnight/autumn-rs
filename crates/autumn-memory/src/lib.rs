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

pub mod keys;
mod recall;
mod vector;

use std::cell::Cell;
use std::collections::HashMap;
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
}

impl MemoryStore {
    /// Connect a fresh client and build a store for `(tenant, agent)`.
    pub async fn connect(
        manager: &str,
        tenant: impl Into<String>,
        agent: impl Into<String>,
    ) -> Result<Self, AutumnError> {
        let client = ClusterClient::connect(manager)
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        Ok(Self::with_client(Rc::new(client), tenant, agent))
    }

    /// F-AUTHZ-1: connect a client bound to `credential` and build a store for
    /// `(tenant, agent)`. Use against an authz-enabled cluster — the client
    /// AUTH_HELLOs each PS connection with a short-TTL token (auto-minted +
    /// renewed) scoped to the tenant's granted `mem/{tenant}/` prefix. The
    /// `tenant` here MUST be the same tenant the credential was created for, so
    /// the token's `allowed_prefixes` cover this store's `mem/{tenant}/` keys.
    pub async fn connect_with_credential(
        manager: &str,
        tenant: impl Into<String>,
        agent: impl Into<String>,
        credential: Vec<u8>,
    ) -> Result<Self, AutumnError> {
        let tenant = tenant.into();
        let client = ClusterClient::connect_with_credential(manager, tenant.clone(), credential)
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
            start = last_key;
            start.push(0u8); // exclusive successor of last_key
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
    // -- BM25-on-KV lexical retrieval (plan §7 词法腿) ------------------------

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

        // 1. postings (existence markers) for current terms
        for term in tf_map.keys() {
            let pk = keys::idx_posting_key(&self.tenant, &self.agent, term, doc_id);
            self.put_kv(&pk, &[], ttl).await?;
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
            for term in o.terms.keys() {
                if !doc.terms.contains_key(term) {
                    let pk = keys::idx_posting_key(&self.tenant, &self.agent, term, doc_id);
                    self.client.delete(&pk).await?;
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
        for term in doc.terms.keys() {
            let pk = keys::idx_posting_key(&self.tenant, &self.agent, term, doc_id);
            self.client.delete(&pk).await?;
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
        let by_score =
            |a: &ScoredDoc, b: &ScoredDoc| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal);

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
    // -- SPFresh-IVF-on-KV vector retrieval (plan §7 向量腿) ------------------
    // Vectors are caller-supplied (`&[f32]`); embedding is sglang's job.

    async fn load_centroids(&self) -> Result<Option<vector::Centroids>, AutumnError> {
        let key = keys::ivf_centroids_key(&self.tenant, &self.agent);
        Ok(self
            .client
            .get(&key)
            .await?
            .and_then(|b| vector::Centroids::decode(&b)))
    }

    async fn store_centroids(&self, c: &vector::Centroids) -> Result<(), AutumnError> {
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
        for k in &key_list {
            let (Some(vid), Some(oc)) =
                (keys::ivf_all_vec_id(k, &all), keys::ivf_all_centroid(k, &all))
            else {
                continue;
            };
            if let Some(b) = self.client.get(k).await? {
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
        for (vid, oc, v) in &items {
            let nc = vector::nearest(v, &cs) as u32;
            let nk = keys::ivf_posting_key(&self.tenant, &self.agent, nc, vid);
            self.put_kv(&nk, &vector::encode_vec(v), 0).await?;
            // keep the reverse pointer accurate after re-bucketing (so a later
            // delete reaps the posting from its CURRENT bucket).
            let vptr = keys::ivf_vptr_key(&self.tenant, &self.agent, vid);
            self.put_kv(&vptr, &nc.to_be_bytes(), 0).await?;
            if nc != *oc {
                let ok = keys::ivf_posting_key(&self.tenant, &self.agent, *oc, vid);
                self.client.delete(&ok).await?;
            }
        }
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
        let mut best: HashMap<String, f32> = HashMap::new();
        for centroid in buckets {
            let bprefix = keys::ivf_bucket_prefix(&self.tenant, &self.agent, centroid);
            for k in self.scan_keys(&bprefix, None).await? {
                let vid = keys::ivf_vec_id(&k, &bprefix);
                if let Some(b) = self.client.get(&k).await? {
                    if let Some(v) = vector::decode_vec(&b) {
                        rerank_insert(&mut best, vid, vector::cosine(&q, &v));
                    }
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
        for k in self.scan_keys(&all, None).await? {
            let Some(vid) = keys::ivf_all_vec_id(&k, &all) else {
                continue;
            };
            if let Some(b) = self.client.get(&k).await? {
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
    /// (the `index_vector` double-fault residual, F-MEM-4). Should be 0.
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
    async fn recount_doc_stats(&self) -> Result<(u64, u64), AutumnError> {
        let doc_keys = self.scan_keys(&keys::doc_prefix(&self.tenant, &self.agent), None).await?;
        let (mut docs, mut sum_len) = (0u64, 0u64);
        for k in &doc_keys {
            if let Some(b) = self.client.get(k).await? {
                if let Some(d) = recall::IndexedDoc::decode(&b) {
                    docs += 1;
                    sum_len += d.doc_len as u64;
                }
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
        for k in &vptr_keys {
            let vid = keys::ivf_vptr_vec_id(k, &vpre);
            // A key that vanished between scan and get is not an inconsistency.
            if let Some(b) = self.client.get(k).await? {
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
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    out.truncate(top_k);
    out
}
