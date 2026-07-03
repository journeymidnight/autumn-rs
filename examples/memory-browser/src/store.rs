//! `Mem` — a JSON-shaped façade over autumn-memory's general agent-memory API:
//! remembered documents (lexical/vector/hybrid search), facts (namespaced KV
//! with TTL), an episodic event log, and an associative graph linking memories.
//! Each remembered memory is ALSO a graph node (kind `Memory`) so the graph view
//! shows the whole memory web; `link` adds typed edges between them.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use anyhow::Result;
use autumn_memory::{keys, Dir, MemoryStore};
use serde_json::{json, Value};

use crate::embed::Embedder;

const NPROBE: usize = 8;

pub struct Mem {
    pub store: Rc<MemoryStore>,
    pub emb: Rc<Embedder>,
    pub tenant: String,
    pub agent: String,
}

fn preview(text: &str) -> String {
    let one = text.split('\n').next().unwrap_or("").trim();
    if one.chars().count() > 48 {
        format!("{}…", one.chars().take(48).collect::<String>())
    } else {
        one.to_string()
    }
}

impl Mem {
    // -- remembered memories (searchable docs + graph nodes) -----------------

    pub async fn remember(&self, id: &str, text: &str, meta: Option<&str>, ttl: Option<u64>) -> Result<()> {
        let attrs = json!({"name": preview(text), "kind": "Memory", "meta": meta.unwrap_or("")});
        let attrs_b = serde_json::to_vec(&attrs)?;
        self.store.index_memory(id, text, &attrs_b, ttl).await?;
        self.store.index_vector(id, &self.emb.embed(text)?, ttl).await?;
        self.store.put_node(id, "Memory", &attrs_b, ttl).await?;
        Ok(())
    }

    pub async fn get_memory(&self, id: &str) -> Result<Option<Value>> {
        match self.store.get_memory(id).await? {
            Some((text, meta)) => {
                let m: Value = serde_json::from_slice(&meta).unwrap_or(Value::Null);
                Ok(Some(json!({"id": id, "text": text, "meta": m.get("meta"), "name": m.get("name")})))
            }
            None => Ok(None),
        }
    }

    pub async fn forget(&self, id: &str) -> Result<()> {
        self.store.delete_memory(id).await?;
        self.store.delete_node(id).await?;
        Ok(())
    }

    pub async fn search(&self, q: &str, mode: &str, k: usize) -> Result<Vec<Value>> {
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
                    if let Some(mut m) = self.get_memory(&id).await? {
                        m["score"] = json!(score);
                        out.push(m);
                    }
                }
            }
            _ => {
                for d in self.store.search_lexical(q, k).await? {
                    let meta: Value = serde_json::from_slice(&d.meta).unwrap_or(Value::Null);
                    out.push(json!({"id": d.id, "text": d.text, "meta": meta.get("meta"),
                                    "name": meta.get("name"), "score": d.score}));
                }
            }
        }
        Ok(out)
    }

    // -- facts (namespaced KV + TTL) -----------------------------------------

    pub async fn put_fact(&self, ns: &str, key: &str, value: &str, ttl: Option<u64>) -> Result<()> {
        self.store.put_fact(ns, key, value.as_bytes(), ttl).await?;
        Ok(())
    }
    pub async fn get_fact(&self, ns: &str, key: &str) -> Result<Option<String>> {
        Ok(self.store.get_fact(ns, key).await?.map(|v| String::from_utf8_lossy(&v).into_owned()))
    }
    pub async fn delete_fact(&self, ns: &str, key: &str) -> Result<()> {
        self.store.delete_fact(ns, key).await?;
        Ok(())
    }
    pub async fn list_facts(&self, ns: &str) -> Result<Vec<Value>> {
        Ok(self
            .store
            .list_facts(ns, None)
            .await?
            .into_iter()
            .map(|(k, v)| json!({"key": k, "value": String::from_utf8_lossy(&v).into_owned()}))
            .collect())
    }

    // -- episodic (per-session event log) ------------------------------------

    pub async fn append_event(&self, session: &str, text: &str, ttl: Option<u64>) -> Result<u64> {
        Ok(self.store.append_event(session, text.as_bytes(), ttl).await?)
    }
    fn events_json(items: Vec<Vec<u8>>) -> Vec<Value> {
        items.into_iter().map(|b| json!(String::from_utf8_lossy(&b).into_owned())).collect()
    }
    pub async fn recent_events(&self, session: &str, limit: usize) -> Result<Vec<Value>> {
        Ok(Self::events_json(self.store.recent_events(session, limit).await?))
    }
    pub async fn replay(&self, session: &str) -> Result<Vec<Value>> {
        Ok(Self::events_json(self.store.replay_session(session, None).await?))
    }

    // -- graph (associative links between memories) --------------------------

    pub async fn link(&self, src: &str, etype: &str, dst: &str) -> Result<()> {
        self.store.add_edge(src, etype, dst, &[], None).await?;
        Ok(())
    }
    async fn brief(&self, id: &str) -> Option<Value> {
        match self.store.get_node(id).await {
            Ok(Some(n)) => {
                let m: Value = serde_json::from_slice(&n.attrs).unwrap_or(Value::Null);
                Some(json!({"id": id, "kind": n.kind, "name": m.get("name")}))
            }
            _ => None,
        }
    }
    pub async fn neighbors(&self, id: &str, dir: &str) -> Result<Vec<Value>> {
        let edges = if dir == "in" {
            self.store.in_edges(id, None, None).await?
        } else {
            self.store.out_edges(id, None, None).await?
        };
        let mut out = Vec::new();
        for e in edges {
            let other = if dir == "in" { &e.src } else { &e.dst };
            if let Some(mut b) = self.brief(other).await {
                b["etype"] = json!(e.etype);
                out.push(b);
            }
        }
        Ok(out)
    }

    /// Whole graph for the force-directed view (nodes + edges, dangling dropped).
    pub async fn graph(&self) -> Result<Value> {
        let mut nodes = Vec::new();
        let mut ids: HashSet<String> = HashSet::new();
        let mut counts: HashMap<String, usize> = HashMap::new();
        // Any node kind (memories are "Memory"; callers may put other kinds).
        let all_prefix = keys::node_prefix(&self.tenant, &self.agent);
        let client = self.store.client();
        let mut start: Vec<u8> = Vec::new();
        loop {
            let res = client.range(&all_prefix, &start, 1024).await?;
            let n = res.entries.len();
            if n == 0 {
                break;
            }
            let last = res.entries[n - 1].key.clone();
            for e in &res.entries {
                let id = keys::node_id_name(&e.key, &all_prefix);
                if let Ok(Some(node)) = self.store.get_node(&id).await {
                    let m: Value = serde_json::from_slice(&node.attrs).unwrap_or(Value::Null);
                    let name = m.get("name").and_then(|x| x.as_str()).unwrap_or(&id).to_string();
                    *counts.entry(node.kind.clone()).or_default() += 1;
                    ids.insert(id.clone());
                    nodes.push(json!({"id": id, "name": name, "kind": node.kind}));
                }
            }
            if n < 1024 {
                break;
            }
            start = last;
            start.push(0);
        }

        let epre = keys::edge_all_prefix(&self.tenant, &self.agent);
        let mut start2: Vec<u8> = Vec::new();
        let mut links = Vec::new();
        loop {
            let res = client.range(&epre, &start2, 1024).await?;
            let n = res.entries.len();
            if n == 0 {
                break;
            }
            let last = res.entries[n - 1].key.clone();
            for e in &res.entries {
                if let Some((src, etype, dst)) = keys::edge_all_parse(&e.key, &epre) {
                    if ids.contains(&src) && ids.contains(&dst) {
                        links.push(json!({"source": src, "target": dst, "type": etype}));
                    }
                }
            }
            if n < 1024 {
                break;
            }
            start2 = last;
            start2.push(0);
        }
        Ok(json!({"nodes": nodes, "links": links, "counts": counts}))
    }

    pub async fn train(&self) -> Result<usize> {
        Ok(self.store.train_centroids(16, 25, 7).await?)
    }
    pub async fn stats(&self) -> Result<Value> {
        let r = self.store.reconcile().await?;
        Ok(json!({"memories": r.docs, "nodes": r.nodes, "edges": r.edges, "is_clean": r.is_clean()}))
    }

    /// bfs helper for the MCP trace tool.
    pub async fn trace(&self, id: &str, dir: &str) -> Result<Vec<Value>> {
        let d = if dir == "in" { Dir::In } else { Dir::Out };
        let mut out = Vec::new();
        for (nid, depth) in self.store.bfs(id, d, None, 6, 200).await? {
            if let Some(mut b) = self.brief(&nid).await {
                b["depth"] = json!(depth);
                out.push(b);
            }
        }
        Ok(out)
    }
}
