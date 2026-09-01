//! `Code` — retrieval helpers over an `Rc<MemoryStore>` + embedder, shared by
//! the web UI and the MCP server. One store holds two corpora side by side —
//! code symbols (kinds Function/Method/Struct/…) and document chunks (kinds
//! Document/Section) — and search filters by corpus so `search_code` never
//! returns prose and `search_docs` never returns symbols.
//!
//! The graph is a GENERAL graph database, not a code index. `MemoryStore`'s
//! node/edge layer is already domain-agnostic — ids and edge types are opaque
//! strings, attributes are opaque bytes — but only two hard-wired edge types
//! were ever reachable from outside, so callers could read a CALLS or CONTAINS
//! graph the indexer had built and nothing else. The `graph_*` methods below
//! expose the layer as it actually is: create and delete nodes and typed
//! edges, list by kind, walk neighbours, traverse. `callers` / `callees` /
//! `members` / `outline` / `trace` remain as named shorthands for the two
//! edge types this binary's own indexers write.

use std::rc::Rc;

use anyhow::Result;
use autumn_memory::{Dir, MemoryStore};
use serde_json::{json, Value};

use crate::embed::Embedder;

const NPROBE: usize = 8;

/// Which corpus a search should return. Both live in the same index; the
/// filter is on each hit's meta `kind`.
#[derive(Clone, Copy, PartialEq)]
pub enum Corpus {
    Code,
    Docs,
    All,
}

impl Corpus {
    pub fn parse(s: &str) -> Corpus {
        match s {
            "docs" => Corpus::Docs,
            "all" => Corpus::All,
            _ => Corpus::Code,
        }
    }
    fn admits(self, kind: Option<&str>) -> bool {
        let is_doc = matches!(kind, Some("Section") | Some("Document"));
        match self {
            Corpus::All => true,
            Corpus::Docs => is_doc,
            Corpus::Code => !is_doc,
        }
    }
}

pub struct Code {
    pub store: Rc<MemoryStore>,
    pub emb: Rc<Embedder>,
}

impl Code {
    fn meta_of(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).unwrap_or(Value::Null)
    }

    /// Brief symbol info for a node, or `None` if the node no longer exists —
    /// e.g. a dangling edge left by re-indexing without `--reset` (its target
    /// symbol was removed/renamed). Callers filter these out so the UI never
    /// shows a blank chip.
    async fn brief(&self, id: &str) -> Option<Value> {
        match self.store.get_node(id).await {
            Ok(Some(n)) => {
                let m = Self::meta_of(&n.attrs);
                let mut b = json!({"id": id, "kind": n.kind});
                // Only the attributes that are actually there. `file` / `start`
                // are what this binary's code and document indexers write; a
                // node from any other caller has neither, and emitting them as
                // nulls made every generic graph result look code-shaped.
                for f in ["name", "file", "start", "headings"] {
                    if let Some(v) = m.get(f).filter(|v| !v.is_null()) {
                        b[f] = v.clone();
                    }
                }
                Some(b)
            }
            _ => None,
        }
    }

    /// One search hit. `headings` is present only on document chunks.
    fn hit(id: &str, m: &Value, text: &str, score: Option<f32>) -> Value {
        let mut v = json!({
            "id": id, "name": m.get("name"), "kind": m.get("kind"),
            "file": m.get("file"), "start": m.get("start"), "end": m.get("end"),
            "source": text,
        });
        if let Some(h) = m.get("headings") {
            v["headings"] = h.clone();
        }
        if let Some(s) = score {
            v["score"] = json!(s);
        }
        v
    }

    pub async fn get_symbol(&self, id: &str) -> Result<Option<Value>> {
        match self.store.get_memory(id).await? {
            Some((text, meta)) => Ok(Some(Self::hit(id, &Self::meta_of(&meta), &text, None))),
            None => Ok(None),
        }
    }

    pub async fn search(&self, q: &str, mode: &str, k: usize, corpus: Corpus) -> Result<Vec<Value>> {
        // Both corpora share one index, so over-fetch and post-filter by kind.
        let fetch = if matches!(corpus, Corpus::All) { k } else { k * 3 + 8 };
        let mut out = Vec::new();
        match mode {
            "vector" | "hybrid" => {
                let qv = self.emb.embed(q)?;
                let hits = if mode == "vector" {
                    self.store.search_vector(&qv, fetch, NPROBE).await?
                } else {
                    self.store.search_hybrid(q, &qv, fetch, NPROBE).await?
                };
                for (id, score) in hits {
                    if out.len() >= k {
                        break;
                    }
                    if let Some((text, meta)) = self.store.get_memory(&id).await? {
                        let m = Self::meta_of(&meta);
                        if corpus.admits(m.get("kind").and_then(|v| v.as_str())) {
                            out.push(Self::hit(&id, &m, &text, Some(score)));
                        }
                    }
                }
            }
            _ => {
                for d in self.store.search_lexical(q, fetch).await? {
                    if out.len() >= k {
                        break;
                    }
                    let m = Self::meta_of(&d.meta);
                    if corpus.admits(m.get("kind").and_then(|v| v.as_str())) {
                        out.push(Self::hit(&d.id, &m, &d.text, Some(d.score)));
                    }
                }
            }
        }
        Ok(out)
    }

    // The named shorthands. Each is one edge type and one direction over the
    // generic walk above — kept because "who calls this" is what a caller
    // actually asks, not because the graph knows what a call is.

    async fn far_briefs(&self, id: &str, dir: &str, etype: &str) -> Result<Vec<Value>> {
        Ok(self
            .graph_neighbors(id, dir, Some(etype), None)
            .await?
            .into_iter()
            .filter_map(|e| e.get("node").cloned())
            .collect())
    }

    pub async fn callers(&self, id: &str) -> Result<Vec<Value>> {
        self.far_briefs(id, "in", "CALLS").await
    }

    pub async fn callees(&self, id: &str) -> Result<Vec<Value>> {
        self.far_briefs(id, "out", "CALLS").await
    }

    pub async fn members(&self, id: &str) -> Result<Vec<Value>> {
        self.far_briefs(id, "out", "CONTAINS").await
    }

    /// The ingested document files (their per-file `Document` nodes).
    pub async fn documents(&self) -> Result<Vec<Value>> {
        let mut v = Vec::new();
        for id in self.store.nodes_by_kind("Document", Some(500)).await? {
            if let Some(b) = self.brief(&id).await {
                v.push(b);
            }
        }
        Ok(v)
    }

    /// Heading outline of one document (or subtree of one section): BFS over
    /// CONTAINS from `id`, each entry tagged with its depth.
    pub async fn outline(&self, id: &str) -> Result<Vec<Value>> {
        self.graph_traverse(id, "out", Some("CONTAINS"), 8, 500).await
    }

    pub async fn trace(&self, id: &str, dir: &str) -> Result<Vec<Value>> {
        self.graph_traverse(id, dir, Some("CALLS"), 6, 200).await
    }

    // -- graph database ------------------------------------------------------
    // Generic node/edge access. `kind` and `etype` are caller-defined labels;
    // `attrs` is caller-defined JSON. Nothing here knows about code.

    /// A node as the graph sees it: id, kind, and its attributes. Distinct
    /// from `brief`, which projects the few fields this binary's own indexers
    /// happen to write — a caller storing its own schema needs all of them
    /// back, so `attrs` is returned whole (parsed when it is JSON, otherwise
    /// as a lossy string, since the store keeps opaque bytes).
    pub async fn graph_get_node(&self, id: &str) -> Result<Option<Value>> {
        Ok(self.store.get_node(id).await?.map(|n| {
            let attrs = serde_json::from_slice::<Value>(&n.attrs)
                .unwrap_or_else(|_| json!(String::from_utf8_lossy(&n.attrs)));
            json!({"id": id, "kind": n.kind, "attrs": attrs})
        }))
    }

    pub async fn graph_put_node(&self, id: &str, kind: &str, attrs: &Value) -> Result<Value> {
        let bytes = serde_json::to_vec(attrs)?;
        self.store.put_node(id, kind, &bytes, None).await?;
        Ok(json!({"id": id, "kind": kind}))
    }

    /// Deletes the node AND every edge touching it — a dangling edge is worse
    /// than a missing one, and `reconcile` counts them as damage.
    pub async fn graph_delete_node(&self, id: &str) -> Result<Value> {
        self.store.delete_node(id).await?;
        Ok(json!({"id": id, "deleted": true}))
    }

    pub async fn graph_add_edge(
        &self,
        src: &str,
        etype: &str,
        dst: &str,
        attrs: &Value,
    ) -> Result<Value> {
        let bytes = serde_json::to_vec(attrs)?;
        self.store.add_edge(src, etype, dst, &bytes, None).await?;
        Ok(json!({"src": src, "type": etype, "dst": dst}))
    }

    pub async fn graph_delete_edge(&self, src: &str, etype: &str, dst: &str) -> Result<Value> {
        self.store.delete_edge(src, etype, dst).await?;
        Ok(json!({"src": src, "type": etype, "dst": dst, "deleted": true}))
    }

    /// Edges incident to `id`. `dir` is "out" (default) or "in"; `etype` None
    /// means every type. Returns the EDGE, not just the far node, so the edge
    /// type and attributes survive the round trip — a graph query that drops
    /// them can't answer "how are these two related".
    pub async fn graph_neighbors(
        &self,
        id: &str,
        dir: &str,
        etype: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let edges = if dir == "in" {
            self.store.in_edges(id, etype, limit).await?
        } else {
            self.store.out_edges(id, etype, limit).await?
        };
        let mut v = Vec::new();
        for e in edges {
            let other = if dir == "in" { &e.src } else { &e.dst };
            let mut o = json!({"src": e.src, "type": e.etype, "dst": e.dst});
            if !e.attrs.is_empty() {
                if let Ok(a) = serde_json::from_slice::<Value>(&e.attrs) {
                    o["attrs"] = a;
                }
            }
            // The far node's brief, when it has one. A node id with no record
            // is a dangling edge, and the edge itself is still worth returning.
            if let Some(b) = self.brief(other).await {
                o["node"] = b;
            }
            v.push(o);
        }
        Ok(v)
    }

    /// Bounded BFS from `start`. `max_depth` / `max_nodes` are the fan-out
    /// guards; both are capped server-side so one query can't walk the graph.
    pub async fn graph_traverse(
        &self,
        start: &str,
        dir: &str,
        etype: Option<&str>,
        max_depth: u32,
        max_nodes: usize,
    ) -> Result<Vec<Value>> {
        let d = if dir == "in" { Dir::In } else { Dir::Out };
        let mut v = Vec::new();
        for (nid, depth) in self
            .store
            .bfs(start, d, etype, max_depth.min(16), max_nodes.min(2000))
            .await?
        {
            let mut b = self.brief(&nid).await.unwrap_or_else(|| json!({"id": nid}));
            b["depth"] = json!(depth);
            v.push(b);
        }
        Ok(v)
    }

    /// Node ids of one kind — the graph's index of last resort, and how a
    /// caller finds an entry point without already knowing an id.
    pub async fn graph_nodes(&self, kind: &str, limit: Option<usize>) -> Result<Vec<Value>> {
        let mut v = Vec::new();
        for id in self.store.nodes_by_kind(kind, Some(limit.unwrap_or(500).min(2000))).await? {
            v.push(self.brief(&id).await.unwrap_or_else(|| json!({"id": id})));
        }
        Ok(v)
    }

    pub async fn stats(&self) -> Result<Value> {
        let r = self.store.reconcile().await?;
        // `nodes` is the graph-database name; `symbols` is kept as an alias
        // because the bundled web UI reads it.
        Ok(json!({"nodes": r.nodes, "symbols": r.nodes, "edges": r.edges,
                  "docs": r.docs, "is_clean": r.is_clean()}))
    }

}
