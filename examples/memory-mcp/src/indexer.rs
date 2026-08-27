//! Index a **Rust** codebase (autumn-rs itself, by default) into autumn-memory
//! with tree-sitter.
//!
//! Each item becomes a symbol keyed `"<relpath>::<qualname>"`:
//!   * a searchable doc (`index_memory` BM25 + `index_vector`),
//!   * a graph node — kind Function / Method / Struct / Enum / Union / Trait /
//!     Module,
//! plus edges `CONTAINS` (module/trait/impl-type → member) and `CALLS`
//! (caller → callee, resolved by short name — an MVP that over-links on name
//! collisions and ignores calls to items outside the index). tree-sitter does
//! the parsing; resolution is the only bespoke part.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use autumn_memory::MemoryStore;
use tree_sitter::{Node, Parser};

use crate::embed::Embedder;

const MAX_CALLS_PER_NAME: usize = 8;

struct Def {
    id: String,
    kind: &'static str,
    name: String,
    qualname: String,
    file: String,
    start: usize,
    end: usize,
    src: String,
}

#[derive(Default)]
struct FileIndex {
    defs: Vec<Def>,
    contains: Vec<(String, String)>, // (container_short_name, member_id)
    calls: Vec<(String, String)>,    // (caller_id, callee_short_name)
}

fn node_text(src: &[u8], n: Node) -> String {
    String::from_utf8_lossy(&src[n.start_byte()..n.end_byte()]).into_owned()
}

fn field_name(src: &[u8], n: Node) -> Option<String> {
    n.child_by_field_name("name").map(|c| node_text(src, c))
}

/// First `type_identifier` under an impl's `type` field (`impl Foo<T>` → "Foo").
fn type_name(src: &[u8], n: Node) -> Option<String> {
    if n.kind() == "type_identifier" {
        return Some(node_text(src, n));
    }
    let mut cursor = n.walk();
    for c in n.children(&mut cursor) {
        if let Some(name) = type_name(src, c) {
            return Some(name);
        }
    }
    None
}

impl FileIndex {
    fn push_def(
        &mut self,
        relpath: &str,
        scope: &[String],
        name: &str,
        kind: &'static str,
        node: Node,
        src: &[u8],
    ) -> String {
        let qual = if scope.is_empty() {
            name.to_string()
        } else {
            format!("{}::{}", scope.join("::"), name)
        };
        let id = format!("{relpath}::{qual}");
        self.defs.push(Def {
            id: id.clone(),
            kind,
            name: name.to_string(),
            qualname: qual,
            file: relpath.to_string(),
            start: node.start_position().row + 1,
            end: node.end_position().row + 1,
            src: node_text(src, node),
        });
        id
    }
}

fn add_contains(fi: &mut FileIndex, container: Option<&str>, child_id: &str) {
    if let Some(c) = container {
        fi.contains.push((c.to_string(), child_id.to_string()));
    }
}

#[allow(clippy::too_many_arguments)]
fn walk(
    node: Node,
    src: &[u8],
    relpath: &str,
    scope: &mut Vec<String>,
    container: Option<&str>,
    in_type: bool, // inside an impl/trait → functions are Methods
    fi: &mut FileIndex,
) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "function_item" => {
                let name = field_name(src, child).unwrap_or_else(|| "<anon>".into());
                let kind = if in_type { "Method" } else { "Function" };
                let id = fi.push_def(relpath, scope, &name, kind, child, src);
                add_contains(fi, container, &id);
                collect_calls(child, src, &id, fi);
                scope.push(name.clone());
                walk(child, src, relpath, scope, Some(&name), false, fi);
                scope.pop();
            }
            "struct_item" | "enum_item" | "union_item" | "type_item" => {
                let name = field_name(src, child).unwrap_or_else(|| "<anon>".into());
                let kind = match child.kind() {
                    "struct_item" => "Struct",
                    "enum_item" => "Enum",
                    "union_item" => "Union",
                    _ => "Type",
                };
                let id = fi.push_def(relpath, scope, &name, kind, child, src);
                add_contains(fi, container, &id);
            }
            "trait_item" => {
                let name = field_name(src, child).unwrap_or_else(|| "<anon>".into());
                let id = fi.push_def(relpath, scope, &name, "Trait", child, src);
                add_contains(fi, container, &id);
                scope.push(name.clone());
                walk(child, src, relpath, scope, Some(&name), true, fi);
                scope.pop();
            }
            "mod_item" => {
                let name = field_name(src, child).unwrap_or_else(|| "<anon>".into());
                let id = fi.push_def(relpath, scope, &name, "Module", child, src);
                add_contains(fi, container, &id);
                scope.push(name.clone());
                walk(child, src, relpath, scope, Some(&name), false, fi);
                scope.pop();
            }
            "impl_item" => {
                // Not a symbol itself; sets the container/scope for its methods.
                match child
                    .child_by_field_name("type")
                    .and_then(|t| type_name(src, t))
                {
                    Some(t) => {
                        scope.push(t.clone());
                        walk(child, src, relpath, scope, Some(&t), true, fi);
                        scope.pop();
                    }
                    None => walk(child, src, relpath, scope, container, true, fi),
                }
            }
            _ => walk(child, src, relpath, scope, container, in_type, fi),
        }
    }
}

fn collect_calls(node: Node, src: &[u8], caller_id: &str, fi: &mut FileIndex) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "function_item" | "impl_item" | "struct_item" | "enum_item" | "trait_item"
            | "mod_item" | "union_item" => continue, // each nested item collects its own
            "call_expression" => {
                if let Some(name) = child.child_by_field_name("function").and_then(|f| ident_of(src, f)) {
                    fi.calls.push((caller_id.to_string(), name));
                }
                collect_calls(child, src, caller_id, fi);
            }
            _ => collect_calls(child, src, caller_id, fi),
        }
    }
}

/// The final identifier of a call target (`a::b::foo` / `x.foo` / `foo::<T>` → "foo").
fn ident_of(src: &[u8], n: Node) -> Option<String> {
    match n.kind() {
        "identifier" | "type_identifier" | "field_identifier" => Some(node_text(src, n)),
        "scoped_identifier" => n.child_by_field_name("name").and_then(|x| ident_of(src, x)),
        "field_expression" => n.child_by_field_name("field").and_then(|x| ident_of(src, x)),
        "generic_function" => n.child_by_field_name("function").and_then(|x| ident_of(src, x)),
        _ => None,
    }
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        let name = e.file_name().to_string_lossy().into_owned();
        if p.is_dir() {
            if !name.starts_with('.') && name != "target" && name != "node_modules" {
                collect_rs(&p, out);
            }
        } else if p.extension().map(|x| x == "rs").unwrap_or(false) {
            out.push(p);
        }
    }
}

/// Index every `.rs` under `root`. Returns `(files, symbols, edges)`.
pub async fn index_path(
    store: &MemoryStore,
    emb: &Embedder,
    root: &Path,
) -> Result<(usize, usize, usize)> {
    let mut parser = Parser::new();
    parser.set_language(&tree_sitter::Language::new(tree_sitter_rust::LANGUAGE))?;

    let mut rs_files = Vec::new();
    collect_rs(root, &mut rs_files);

    let mut files: Vec<FileIndex> = Vec::new();
    for path in &rs_files {
        let Ok(src) = std::fs::read(path) else {
            continue;
        };
        let rel = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned();
        let Some(tree) = parser.parse(&src, None) else {
            continue;
        };
        let mut fi = FileIndex::default();
        let mut scope = Vec::new();
        walk(tree.root_node(), &src, &rel, &mut scope, None, false, &mut fi);
        files.push(fi);
    }

    // short-name → symbol ids (for CALLS + CONTAINS resolution by name).
    let mut by_name: HashMap<String, Vec<String>> = HashMap::new();
    for fi in &files {
        for d in &fi.defs {
            by_name.entry(d.name.clone()).or_default().push(d.id.clone());
        }
    }

    let (mut n_sym, mut n_edge) = (0usize, 0usize);
    let mut seen: HashSet<(&str, String, String)> = HashSet::new();
    for fi in &files {
        for d in &fi.defs {
            let meta = serde_json::json!({
                "name": d.name, "kind": d.kind, "qualname": d.qualname,
                "file": d.file, "start": d.start, "end": d.end,
            });
            let meta_b = serde_json::to_vec(&meta)?;
            store.index_memory(&d.id, &d.src, &meta_b, None).await?;
            store.index_vector(&d.id, &emb.embed(&d.src)?, None).await?;
            store.put_node(&d.id, d.kind, &meta_b, None).await?;
            n_sym += 1;
        }
        // CONTAINS: resolve the container short-name → its node id, edge container → member.
        for (container_name, member_id) in &fi.contains {
            let Some(cids) = by_name.get(container_name) else {
                continue;
            };
            for cid in cids.iter().take(MAX_CALLS_PER_NAME) {
                if cid == member_id || !seen.insert(("CONTAINS", cid.clone(), member_id.clone())) {
                    continue;
                }
                store.add_edge(cid, "CONTAINS", member_id, &[], None).await?;
                n_edge += 1;
            }
        }
        // CALLS: resolve the callee short-name → its node id, edge caller → callee.
        for (caller, callee_name) in &fi.calls {
            let Some(tids) = by_name.get(callee_name) else {
                continue;
            };
            for tid in tids.iter().take(MAX_CALLS_PER_NAME) {
                if tid == caller || !seen.insert(("CALLS", caller.clone(), tid.clone())) {
                    continue;
                }
                store.add_edge(caller, "CALLS", tid, &[], None).await?;
                n_edge += 1;
            }
        }
    }
    Ok((files.len(), n_sym, n_edge))
}
