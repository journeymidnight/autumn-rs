//! `Code` — retrieval helpers over an `Rc<MemoryStore>` + embedder, shared by
//! the web UI and the MCP server. One store holds two corpora side by side —
//! code symbols (kinds Function/Method/Struct/…) and document chunks (kinds
//! Document/Section) — and search filters by corpus so `search_code` never
//! returns prose and `search_docs` never returns symbols. The graph methods
//! expose CALLS edges as callers/callees/a bounded trace, and CONTAINS edges
//! as members (impl/mod for code, heading outline for documents).

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
                let mut b = json!({"id": id, "kind": n.kind, "name": m.get("name"),
                                   "file": m.get("file"), "start": m.get("start")});
                if let Some(h) = m.get("headings") {
                    b["headings"] = h.clone();
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

    pub async fn callers(&self, id: &str) -> Result<Vec<Value>> {
        let mut v = Vec::new();
        for e in self.store.in_edges(id, Some("CALLS"), None).await? {
            if let Some(b) = self.brief(&e.src).await {
                v.push(b);
            }
        }
        Ok(v)
    }

    pub async fn callees(&self, id: &str) -> Result<Vec<Value>> {
        let mut v = Vec::new();
        for e in self.store.out_edges(id, Some("CALLS"), None).await? {
            if let Some(b) = self.brief(&e.dst).await {
                v.push(b);
            }
        }
        Ok(v)
    }

    pub async fn members(&self, id: &str) -> Result<Vec<Value>> {
        let mut v = Vec::new();
        for e in self.store.out_edges(id, Some("CONTAINS"), None).await? {
            if let Some(b) = self.brief(&e.dst).await {
                v.push(b);
            }
        }
        Ok(v)
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
        let mut v = Vec::new();
        for (nid, depth) in self.store.bfs(id, Dir::Out, Some("CONTAINS"), 8, 500).await? {
            if let Some(mut b) = self.brief(&nid).await {
                b["depth"] = json!(depth);
                v.push(b);
            }
        }
        Ok(v)
    }

    pub async fn trace(&self, id: &str, dir: &str) -> Result<Vec<Value>> {
        let d = if dir == "in" { Dir::In } else { Dir::Out };
        let mut v = Vec::new();
        for (nid, depth) in self.store.bfs(id, d, Some("CALLS"), 6, 200).await? {
            if let Some(mut b) = self.brief(&nid).await {
                b["depth"] = json!(depth);
                v.push(b);
            }
        }
        Ok(v)
    }

    pub async fn stats(&self) -> Result<Value> {
        let r = self.store.reconcile().await?;
        Ok(json!({"symbols": r.nodes, "edges": r.edges, "docs": r.docs, "is_clean": r.is_clean()}))
    }

}
