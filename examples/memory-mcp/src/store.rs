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

use std::path::{Path, PathBuf};
use std::rc::Rc;

use anyhow::Result;
use autumn_memory::{Dir, MemoryStore};
use serde_json::{json, Value};

use crate::embed::Embedder;

const NPROBE: usize = 8;

/// The most lines one `read_file` may return. A search hit's span is usually
/// tens of lines; this is only here so "read the file" cannot become a way to
/// spend a whole context window in one call.
const MAX_READ_LINES: usize = 400;


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
    /// `None` when no embedder is configured: the service then offers the
    /// lexical leg only, and says so, rather than answering vector queries with
    /// vectors that mean nothing.
    pub emb: Option<Rc<Embedder>>,
    /// The indexed tree, and the ONLY tree `read_file` will read from. A
    /// search hit names a file and a line span; reading it is the caller's
    /// separate, bounded act, and this is what bounds WHERE it can read.
    pub root: PathBuf,
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

    /// One hit. `headings` is present only on document chunks.
    ///
    /// `body` is what the hit carries of the symbol's text: `Some` for a
    /// deliberate single fetch (`get_symbol`), `None` for a SEARCH hit.
    ///
    /// A SEARCH RESULT IS A LOCATION, NOT A DELIVERY. Every hit used to carry
    /// the symbol's whole body, and one body can be enormous: on this index
    /// `extent node replication` answered with 7 hits and 24,608 characters of
    /// source, of which ONE hit was 17,396 — 71% of the reply for a single
    /// function — while the median hit was 485. A caller asking three such
    /// questions has spent its context on code it never chose to read, and it
    /// cannot decline: the bytes arrive before there is anything to decide
    /// with.
    ///
    /// Trimming to an excerpt was the first attempt and it is still the wrong
    /// shape — an excerpt is a smaller delivery, and the caller has to take
    /// it. So a hit now says WHERE: id, name, kind, file, and the line span.
    /// Reading is a separate, bounded act the caller performs on purpose —
    /// `read_file` for a line range, `get_symbol` for one whole symbol — and
    /// it chooses how much. Dropping the body takes an 8-hit result from
    /// 6,563 characters to 1,712.
    fn hit(id: &str, m: &Value, body: Option<&str>, score: Option<f32>) -> Value {
        let mut v = json!({
            "id": id, "name": m.get("name"), "kind": m.get("kind"),
            "file": m.get("file"), "start": m.get("start"), "end": m.get("end"),
        });
        if let Some(text) = body {
            v["source"] = json!(text);
        }
        if let Some(h) = m.get("headings") {
            v["headings"] = h.clone();
        }
        if let Some(sc) = score {
            v["score"] = json!(sc);
        }
        v
    }

    pub async fn get_symbol(&self, id: &str) -> Result<Option<Value>> {
        match self.store.get_memory(id).await? {
            Some((text, meta)) => Ok(Some(Self::hit(id, &Self::meta_of(&meta), Some(&text), None))),
            None => Ok(None),
        }
    }

    /// A line range of one indexed file.
    ///
    /// The other half of "a search hit is a location": the hit says
    /// file + start + end, and this is how the caller turns that into text —
    /// deliberately, and at a size IT chooses, instead of being handed every
    /// matching body whether it wanted them or not.
    ///
    /// Two bounds, and both matter. `MAX_READ_LINES` stops "read the file"
    /// from being a way to spend a context window in one call. And the path
    /// is resolved and confined to `root`: the corpus is what this server
    /// indexes, not the pod's filesystem, so `../..` or an absolute path
    /// cannot reach a credential mounted next door. Resolution is by
    /// canonicalize + prefix check rather than a scan for "..", because a
    /// symlink inside the tree reaches outside it without the string ever
    /// containing one.
    pub fn read_file(&self, path: &str, start: Option<usize>, end: Option<usize>) -> Result<Value> {
        Self::read_within(&self.root, path, start, end)
    }

    /// The body of `read_file`, without the store. Reading a file has nothing
    /// to do with the index, and keeping it free of `self` is what lets the
    /// confinement above be tested without standing up a MemoryStore.
    fn read_within(
        tree: &Path,
        path: &str,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<Value> {
        let root = tree.canonicalize().unwrap_or_else(|_| tree.to_path_buf());
        let joined = if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else {
            root.join(path)
        };
        let target = joined
            .canonicalize()
            .map_err(|e| anyhow::anyhow!("cannot read {path}: {e}"))?;
        if !target.starts_with(&root) {
            anyhow::bail!("{path} is outside the indexed tree");
        }
        let text = std::fs::read_to_string(&target)
            .map_err(|e| anyhow::anyhow!("cannot read {path}: {e}"))?;
        let lines: Vec<&str> = text.lines().collect();
        // 1-based and inclusive, to match what a hit reports and what a human
        // reads off an editor gutter.
        let from = start.unwrap_or(1).max(1);
        let to = end.unwrap_or(lines.len()).min(lines.len());
        if from > lines.len() {
            anyhow::bail!("{path} has {} lines; start={from} is past the end", lines.len());
        }
        let capped = to.min(from + MAX_READ_LINES - 1);
        let body = lines[from - 1..capped].join("\n");
        Ok(json!({
            "file": path,
            "start": from,
            "end": capped,
            "total_lines": lines.len(),
            "truncated": capped < to,
            "text": body,
        }))
    }

    pub async fn search(&self, q: &str, mode: &str, k: usize, corpus: Corpus) -> Result<Vec<Value>> {
        // Both corpora share one index, so over-fetch and post-filter by kind.
        let fetch = if matches!(corpus, Corpus::All) { k } else { k * 3 + 8 };
        let mut out = Vec::new();
        match mode {
            "vector" | "hybrid" => {
                let Some(emb) = self.emb.as_ref() else {
                    anyhow::bail!(
                        "mode `{mode}` needs an embedder; this instance has none \
                         (start it with --embed-url, or use mode=lexical)"
                    );
                };
                let qv = emb.embed(q).await?;
                let hits = if mode == "vector" {
                    self.store.search_vector(&qv, fetch, NPROBE).await?
                } else {
                    self.store.search_hybrid(q, &qv, fetch, NPROBE).await?
                };
                for (id, score) in hits {
                    if out.len() >= k {
                        break;
                    }
                    // The body is no longer part of a hit (see `hit`), but
                    // `get_memory` is what carries the meta the kind filter
                    // needs; a lighter node-only read is possible and is not
                    // worth changing retrieval semantics for late.
                    if let Some((_text, meta)) = self.store.get_memory(&id).await? {
                        let m = Self::meta_of(&meta);
                        if corpus.admits(m.get("kind").and_then(|v| v.as_str())) {
                            out.push(Self::hit(&id, &m, None, Some(score)));
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
                        out.push(Self::hit(&d.id, &m, None, Some(d.score)));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Value {
        json!({"name": "write_bulk", "kind": "Function", "file": "src/put.rs",
               "start": 10, "end": 900})
    }

    /// A search hit says WHERE. One symbol on this index is 17,396 characters
    /// — 71% of a 7-hit reply — and every caller paid for it before it could
    /// decide whether it wanted the body at all.
    #[test]
    fn a_search_hit_carries_no_body() {
        let v = Code::hit("src/put.rs::write_bulk", &meta(), None, Some(0.9));
        assert!(v.get("source").is_none(), "a search hit must not deliver code");
        // What a caller needs to read it deliberately, and to cite it.
        for f in ["id", "name", "kind", "file", "start", "end", "score"] {
            assert!(v.get(f).is_some(), "a hit must still say {f}");
        }
    }

    /// get_symbol is the deliberate single fetch, so it still answers whole:
    /// dropping the body everywhere would leave no way to read a symbol at all.
    #[test]
    fn get_symbol_answers_whole() {
        let body = "x".repeat(20_000);
        let v = Code::hit("id", &meta(), Some(&body), None);
        assert_eq!(v["source"].as_str().unwrap().len(), 20_000);
    }

    /// A tree to read from, and one credential-shaped file OUTSIDE it.
    fn corpus(name: &str) -> (PathBuf, PathBuf) {
        // Per-test directory: these tests run in PARALLEL, and one shared path
        // meant each one's setup deleted the others' tree mid-run — three
        // failures that all passed when run alone.
        let dir = std::env::temp_dir()
            .join(format!("mmcp-read-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let root = dir.join("src");
        std::fs::create_dir_all(root.join("sub")).unwrap();
        std::fs::write(dir.join("fs.cred"), "SECRET").unwrap();
        let body: String = (1..=600).map(|n| format!("line {n}\n")).collect();
        std::fs::write(root.join("big.rs"), &body).unwrap();
        std::fs::write(root.join("sub/small.rs"), "a\nb\nc\n").unwrap();
        (dir, root)
    }

    #[test]
    fn read_file_returns_the_range_the_caller_asked_for() {
        let (dir, root) = corpus("range");
        let v = Code::read_within(&root, "big.rs", Some(10), Some(12)).unwrap();
        assert_eq!(v["text"], json!("line 10\nline 11\nline 12"));
        assert_eq!(v["start"], json!(10));
        assert_eq!(v["end"], json!(12));
        assert_eq!(v["total_lines"], json!(600));
        assert_eq!(v["truncated"], json!(false));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// "Read the file" must not be a way to spend a context window in one
    /// call — the problem this whole change exists to fix.
    #[test]
    fn read_file_is_capped_and_says_so() {
        let (dir, root) = corpus("capped");
        let v = Code::read_within(&root, "big.rs", None, None).unwrap();
        assert_eq!(v["truncated"], json!(true));
        assert_eq!(v["end"], json!(MAX_READ_LINES as u64));
        assert_eq!(v["text"].as_str().unwrap().lines().count(), MAX_READ_LINES);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// This server runs in a pod that mounts an autumn credential. A path
    /// argument that escapes the indexed tree must be refused, by resolution
    /// and not by scanning for "..": a symlink inside the tree reaches outside
    /// it without the string ever containing one.
    #[test]
    fn read_file_cannot_leave_the_indexed_tree() {
        let (dir, root) = corpus("escape");
        for escape in ["../fs.cred", "sub/../../fs.cred", "/etc/hostname"] {
            let r = Code::read_within(&root, escape, None, None);
            assert!(r.is_err(), "{escape} must be refused, got {r:?}");
        }
        #[cfg(unix)]
        {
            let link = root.join("sneak");
            std::os::unix::fs::symlink(dir.join("fs.cred"), &link).unwrap();
            let r = Code::read_within(&root, "sneak", None, None);
            assert!(r.is_err(), "a symlink out of the tree must be refused: {r:?}");
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_file_reports_a_start_past_the_end() {
        let (dir, root) = corpus("past-end");
        assert!(Code::read_within(&root, "sub/small.rs", Some(99), None).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Document chunks carry their heading path; that is provenance, not body.
    #[test]
    fn a_document_hit_keeps_its_heading_path() {
        let m = json!({"name": "ops", "kind": "Section", "file": "docs/ops.md",
                       "start": 10, "end": 42, "headings": ["Ops", "Restarts"]});
        let v = Code::hit("docs/ops.md#L10-L42", &m, None, Some(0.5));
        assert_eq!(v["headings"], json!(["Ops", "Restarts"]));
        assert!(v.get("source").is_none());
    }
}
