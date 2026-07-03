//! `Code` — code-flavored helpers over an `Rc<MemoryStore>` + embedder.
//! Search returns symbols with source + location; the graph methods expose the
//! CALLS/CONTAINS edges as callers / callees / members / a bounded call trace.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use anyhow::Result;
use autumn_memory::{keys, Dir, MemoryStore};
use serde_json::{json, Value};

use crate::embed::Embedder;

const NPROBE: usize = 8;
pub const KINDS: [&str; 8] = [
    "Function", "Method", "Struct", "Enum", "Union", "Trait", "Module", "Type",
];

pub struct Code {
    pub store: Rc<MemoryStore>,
    pub emb: Rc<Embedder>,
    pub tenant: String,
    pub agent: String,
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

    /// The whole graph for the force-directed view: every node (id, name, kind)
    /// + every edge whose endpoints both still exist (drops dangling edges).
    /// Nodes come from the per-kind index; edges from ONE `edge/` prefix scan.
    pub async fn graph(&self) -> Result<Value> {
        let mut nodes = Vec::new();
        let mut ids: HashSet<String> = HashSet::new();
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for kind in KINDS {
            for id in self.store.nodes_by_kind(kind, None).await? {
                let name = id.rsplit("::").next().unwrap_or(&id).to_string();
                ids.insert(id.clone());
                *counts.entry(kind).or_default() += 1;
                nodes.push(json!({"id": id, "name": name, "kind": kind}));
            }
        }

        let client = self.store.client();
        let prefix = keys::edge_all_prefix(&self.tenant, &self.agent);
        let mut start: Vec<u8> = Vec::new();
        let mut links = Vec::new();
        loop {
            let res = client.range(&prefix, &start, 1024).await?;
            let n = res.entries.len();
            if n == 0 {
                break;
            }
            let last = res.entries[n - 1].key.clone();
            for e in &res.entries {
                if let Some((src, etype, dst)) = keys::edge_all_parse(&e.key, &prefix) {
                    if ids.contains(&src) && ids.contains(&dst) {
                        links.push(json!({"source": src, "target": dst, "type": etype}));
                    }
                }
            }
            if n < 1024 {
                break;
            }
            start = last;
            start.push(0);
        }
        Ok(json!({"nodes": nodes, "links": links, "counts": counts}))
    }
}
