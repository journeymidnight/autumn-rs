//! `Code` — code-flavored helpers over an `Rc<MemoryStore>` + embedder.
//! Search returns symbols with source + location; the graph methods expose the
//! CALLS/CONTAINS edges as callers / callees / members / a bounded call trace.

use std::rc::Rc;

use anyhow::Result;
use autumn_memory::{Dir, MemoryStore};
use serde_json::{json, Value};

use crate::embed::Embedder;

const NPROBE: usize = 8;

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
                Some(json!({"id": id, "kind": n.kind, "name": m.get("name"),
                            "file": m.get("file"), "start": m.get("start")}))
            }
            _ => None,
        }
    }

    pub async fn get_symbol(&self, id: &str) -> Result<Option<Value>> {
        match self.store.get_memory(id).await? {
            Some((text, meta)) => {
                let m = Self::meta_of(&meta);
                Ok(Some(json!({
                    "id": id, "name": m.get("name"), "kind": m.get("kind"),
                    "file": m.get("file"), "start": m.get("start"), "end": m.get("end"),
                    "source": text,
                })))
            }
            None => Ok(None),
        }
    }

    pub async fn search_code(&self, q: &str, mode: &str, k: usize) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        match mode {
            "vector" | "hybrid" => {
                let qv = self.emb.embed(q)?;
                let hits = if mode == "vector" {
                    self.store.search_vector(&qv, k, NPROBE).await?
                } else {
                    self.store.search_hybrid(q, &qv, k, NPROBE).await?
                };
                for (id, score) in hits {
                    if let Some(mut sym) = self.get_symbol(&id).await? {
                        sym["score"] = json!(score);
                        out.push(sym);
                    }
                }
            }
            _ => {
                for d in self.store.search_lexical(q, k).await? {
                    let m = Self::meta_of(&d.meta);
                    out.push(json!({
                        "id": d.id, "name": m.get("name"), "kind": m.get("kind"),
                        "file": m.get("file"), "start": m.get("start"), "end": m.get("end"),
                        "source": d.text, "score": d.score,
                    }));
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
