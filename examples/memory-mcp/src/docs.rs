//! Ingest markdown / plain-text documents into autumn-memory.
//!
//! Chunking respects markdown structure: a chunk never crosses an ATX heading
//! (`#`..`######`, fenced code blocks ignored), and an oversized section is
//! split at paragraph boundaries with a one-paragraph overlap so no sentence is
//! cut mid-thought. Each chunk becomes
//!   * a searchable doc (`index_memory` BM25 + `index_vector`), keyed
//!     `"<relpath>#L<start>-L<end>"` (GitHub-style line anchor — collision-free
//!     against code ids, human-readable, cite-able),
//!   * a graph node (kind `Section`) hanging off a per-file `Document` node via
//!     `CONTAINS` edges that mirror the heading hierarchy, so the same graph
//!     walks that trace calls can walk a document outline.
//! Meta carries `{name, file, headings, start, end}` — enough provenance for an
//! agent to cite "file › heading path, lines a-b".

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use autumn_memory::MemoryStore;

use crate::embed::Embedder;

/// Soft cap on a chunk's body (bytes). Sections under this stay whole.
const MAX_CHUNK_BYTES: usize = 2800;
/// Overlap carried into the next chunk when a section is split: the previous
/// chunk's last paragraph, but never more than this many bytes.
const MAX_OVERLAP_BYTES: usize = 400;

pub struct Chunk {
    pub headings: Vec<String>,
    pub start_line: usize, // 1-based, inclusive
    pub end_line: usize,   // 1-based, inclusive
    pub body: String,
}

/// One paragraph (blank-line separated run of lines) with its line span.
struct Para {
    start: usize,
    end: usize,
    text: String,
}

fn heading_level(line: &str) -> Option<(usize, &str)> {
    let hashes = line.bytes().take_while(|b| *b == b'#').count();
    if (1..=6).contains(&hashes) {
        if let Some(rest) = line[hashes..].strip_prefix(' ') {
            let title = rest.trim().trim_end_matches('#').trim();
            if !title.is_empty() {
                return Some((hashes, title));
            }
        }
    }
    None
}

/// Split a section's lines (absolute 1-based numbering via `first_line`) into
/// paragraphs at blank lines; a paragraph longer than the chunk cap is further
/// split at line boundaries so a giant table/code block can't defeat the cap.
fn paragraphs(lines: &[&str], first_line: usize) -> Vec<Para> {
    let mut out: Vec<Para> = Vec::new();
    let mut cur: Vec<(usize, &str)> = Vec::new();
    let flush = |cur: &mut Vec<(usize, &str)>, out: &mut Vec<Para>| {
        if cur.is_empty() {
            return;
        }
        let mut piece: Vec<(usize, &str)> = Vec::new();
        let mut size = 0usize;
        for &(n, l) in cur.iter() {
            if size > 0 && size + l.len() + 1 > MAX_CHUNK_BYTES {
                out.push(Para {
                    start: piece[0].0,
                    end: piece[piece.len() - 1].0,
                    text: piece.iter().map(|(_, l)| *l).collect::<Vec<_>>().join("\n"),
                });
                piece.clear();
                size = 0;
            }
            size += l.len() + 1;
            piece.push((n, l));
        }
        if !piece.is_empty() {
            out.push(Para {
                start: piece[0].0,
                end: piece[piece.len() - 1].0,
                text: piece.iter().map(|(_, l)| *l).collect::<Vec<_>>().join("\n"),
            });
        }
        cur.clear();
    };
    for (i, l) in lines.iter().enumerate() {
        if l.trim().is_empty() {
            flush(&mut cur, &mut out);
        } else {
            cur.push((first_line + i, l));
        }
    }
    flush(&mut cur, &mut out);
    out
}

/// Chunk one markdown (or plain-text — it just has no headings) document.
pub fn chunk_markdown(text: &str) -> Vec<Chunk> {
    let lines: Vec<&str> = text.lines().collect();

    // Pass 1: section boundaries. A section = heading line + content until the
    // next heading (of any level). Fenced code blocks mask headings.
    // Each entry: (heading path snapshot, first line 1-based, line range lo..hi).
    let mut sections: Vec<(Vec<String>, usize, usize, usize)> = Vec::new();
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut in_fence = false;
    let mut sec_start = 0usize; // index into `lines`
    let mut sec_path: Vec<String> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let t = line.trim_start();
        if t.starts_with("```") || t.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        if let Some((level, title)) = heading_level(line) {
            if i > sec_start || !sec_path.is_empty() {
                sections.push((sec_path.clone(), sec_start + 1, sec_start, i));
            }
            while stack.last().map(|(l, _)| *l >= level).unwrap_or(false) {
                stack.pop();
            }
            stack.push((level, title.to_string()));
            sec_path = stack.iter().map(|(_, t)| t.clone()).collect();
            sec_start = i;
        }
    }
    if sec_start < lines.len() || !sec_path.is_empty() {
        sections.push((sec_path.clone(), sec_start + 1, sec_start, lines.len()));
    }

    // Pass 2: within each section, pack paragraphs into ≤MAX_CHUNK_BYTES chunks
    // with a one-paragraph overlap between consecutive chunks of a section.
    let mut chunks: Vec<Chunk> = Vec::new();
    for (path, first_line, lo, hi) in sections {
        let body_lines = &lines[lo..hi];
        if body_lines.iter().all(|l| l.trim().is_empty()) {
            continue;
        }
        let paras = paragraphs(body_lines, first_line);
        let mut cur: Vec<&Para> = Vec::new();
        let mut size = 0usize;
        let mut overlap: Option<String> = None;
        let mut emit = |cur: &mut Vec<&Para>, overlap: &mut Option<String>| {
            if cur.is_empty() {
                return;
            }
            let mut body = String::new();
            if let Some(o) = overlap.take() {
                body.push_str(&o);
                body.push_str("\n\n");
            }
            body.push_str(&cur.iter().map(|p| p.text.as_str()).collect::<Vec<_>>().join("\n\n"));
            let last = cur[cur.len() - 1];
            if last.text.len() <= MAX_OVERLAP_BYTES {
                *overlap = Some(last.text.clone());
            }
            chunks.push(Chunk {
                headings: path.clone(),
                start_line: cur[0].start,
                end_line: last.end,
                body,
            });
            cur.clear();
        };
        for p in &paras {
            if size > 0 && size + p.text.len() > MAX_CHUNK_BYTES {
                emit(&mut cur, &mut overlap);
                size = overlap.as_ref().map(|o| o.len()).unwrap_or(0);
            }
            size += p.text.len() + 2;
            cur.push(p);
        }
        emit(&mut cur, &mut overlap);
    }
    chunks
}

fn collect_docs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        let name = e.file_name().to_string_lossy().into_owned();
        if p.is_dir() {
            if !name.starts_with('.') && name != "target" && name != "node_modules" {
                collect_docs(&p, out);
            }
        } else if p
            .extension()
            .map(|x| x == "md" || x == "markdown" || x == "txt")
            .unwrap_or(false)
        {
            out.push(p);
        }
    }
}

/// Path used in doc ids: relative to the current directory when possible so
/// ids stay short and human-readable (`docs/ops.md`, not `/data/.../ops.md`).
fn display_path(p: &Path) -> String {
    let abs = p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
    match std::env::current_dir().ok().and_then(|cwd| abs.strip_prefix(&cwd).ok().map(Path::to_path_buf)) {
        Some(rel) => rel.to_string_lossy().into_owned(),
        None => abs.to_string_lossy().into_owned(),
    }
}

/// Ingest every `.md` / `.markdown` / `.txt` under `root` (or `root` itself if
/// it is a file). Returns `(files, chunks, edges)`. Upserts by chunk id — a
/// re-ingest of an edited file overwrites chunks at unchanged line spans;
/// `--reset` wipes stale ones whose spans moved.
pub async fn ingest_path(
    store: &MemoryStore,
    emb: &Embedder,
    root: &Path,
) -> Result<(usize, usize, usize)> {
    let mut paths = Vec::new();
    if root.is_file() {
        paths.push(root.to_path_buf());
    } else {
        collect_docs(root, &mut paths);
    }
    paths.sort();

    // Ingest runs in two passes, because the two halves have opposite needs.
    //
    // Pass 1 is strictly sequential: outline parents resolve through
    // `path_owner`, where a chunk hangs under the FIRST chunk seen at its
    // longest known heading prefix. That is order-dependent by construction, so
    // it cannot be parallelised — but it is also pure CPU (chunking, ids,
    // embeddings), which is not where the time goes.
    //
    // Pass 2 is the I/O, and profiling put ~99% of ingest wall-clock there
    // while the CPU sat idle: it was one document per round trip, serialised.
    // Nothing in it is order-dependent — ids and parents are already decided —
    // so it runs with several requests in flight.
    let mut work: Vec<PendingChunk> = Vec::new();
    let mut doc_nodes: Vec<(String, Vec<u8>)> = Vec::new();
    let mut n_file = 0usize;
    let mut n_unreadable = 0usize;
    for path in &paths {
        // A file we cannot read is REPORTED, not skipped.
        //
        // `collect_docs` already established this path exists and matched its
        // extension, so a read failing here means the file is unreadable, not
        // absent — a broken mount, a permission problem, an I/O error. Silently
        // continuing turned exactly that into "ingested 0 chunks from 0 files"
        // after 389 seconds, with the server then coming up healthy and simply
        // returning nothing for every query. A retrieval service that indexes
        // nothing and says so only in a count nobody reads is worse than one
        // that fails to start.
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(path = %path.display(), error = %e, "ingest: cannot read file");
                n_unreadable += 1;
                continue;
            }
        };
        let text = String::from_utf8_lossy(&bytes);
        let chunks = chunk_markdown(&text);
        if chunks.is_empty() {
            continue;
        }
        let relpath = display_path(path);
        let fname = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| relpath.clone());
        n_file += 1;

        // Per-file Document node — the outline root.
        let total_lines = text.lines().count();
        let doc_meta = serde_json::json!({
            "name": fname, "kind": "Document", "file": relpath,
            "start": 1, "end": total_lines,
        });
        doc_nodes.push((relpath.clone(), serde_json::to_vec(&doc_meta)?));

        // Heading-path → node id of the first chunk at that path, for outline
        // parent resolution (child hangs under its longest known prefix).
        let mut path_owner: HashMap<String, String> = HashMap::new();
        for c in &chunks {
            let id = format!("{relpath}#L{}-L{}", c.start_line, c.end_line);
            let name = c.headings.last().cloned().unwrap_or_else(|| fname.clone());
            let breadcrumb = if c.headings.is_empty() {
                relpath.clone()
            } else {
                format!("{relpath} › {}", c.headings.join(" › "))
            };
            let indexed = format!("{breadcrumb}\n\n{}", c.body);
            let meta = serde_json::json!({
                "name": name, "kind": "Section", "file": relpath,
                "headings": c.headings, "start": c.start_line, "end": c.end_line,
            });
            let meta_b = serde_json::to_vec(&meta)?;
            let vector = emb.embed(&indexed)?;

            let key = c.headings.join("\u{1}");
            let parent = (0..c.headings.len())
                .rev()
                .map(|n| c.headings[..n].join("\u{1}"))
                .find_map(|k| path_owner.get(&k).cloned())
                .unwrap_or_else(|| relpath.clone());
            path_owner.entry(key).or_insert_with(|| id.clone());

            work.push(PendingChunk { id, indexed, meta_b, vector, parent });
        }
    }

    // Every candidate unreadable is a failure, not an empty corpus. Say so
    // loudly rather than returning zeros the caller will log as a success.
    if n_unreadable > 0 && n_file == 0 {
        anyhow::bail!(
            "ingest: all {n_unreadable} candidate file(s) under {} were unreadable — \
             refusing to report an empty ingest as success",
            root.display()
        );
    }
    if n_unreadable > 0 {
        tracing::warn!(unreadable = n_unreadable, indexed = n_file, "ingest: some files unreadable");
    }

    let (n_chunk, n_edge) = (work.len(), work.len());

    // Defer the `meta/stats` read-modify-write to a single update at the end.
    // Per document it was two round trips for a counter, and — because
    // concurrent writers to one key lose updates — it is also what pass 2 would
    // otherwise race on.
    store.begin_bulk_index();
    let res = write_all(store, doc_nodes, work).await;
    // Flush on both paths: a partial ingest still wrote real documents, and
    // dropping their deltas would leave the corpus stats understated until
    // someone ran a repair.
    let flushed = store.flush_stats().await;
    res?;
    flushed?;

    Ok((n_file, n_chunk, n_edge))
}

/// One chunk's work, with every order-dependent decision already made.
struct PendingChunk {
    id: String,
    indexed: String,
    meta_b: Vec<u8>,
    vector: Vec<f32>,
    parent: String,
}

/// How many chunks are in flight at once. Sized to keep the pipe full without
/// letting one ingest monopolise the partition servers it shares with live
/// queries; each chunk is itself several round trips.
const INGEST_CONCURRENCY: usize = 16;

/// Pass 2: the I/O. `Document` nodes go first so an edge never names a parent
/// that is not there yet.
async fn write_all(
    store: &MemoryStore,
    doc_nodes: Vec<(String, Vec<u8>)>,
    work: Vec<PendingChunk>,
) -> Result<()> {
    use futures::stream::{self, StreamExt};

    let mut docs = stream::iter(doc_nodes)
        .map(|(relpath, meta)| async move {
            store.put_node(&relpath, "Document", &meta, None).await
        })
        .buffer_unordered(INGEST_CONCURRENCY);
    while let Some(r) = docs.next().await {
        r?;
    }

    let mut chunks = stream::iter(work)
        .map(|w| async move {
            // Kept in order within a chunk: `index_memory` commits the doc only
            // after its postings land, and the node should exist before an edge
            // points at it.
            store.index_memory(&w.id, &w.indexed, &w.meta_b, None).await?;
            store.index_vector(&w.id, &w.vector, None).await?;
            store.put_node(&w.id, "Section", &w.meta_b, None).await?;
            store.add_edge(&w.parent, "CONTAINS", &w.id, &[], None).await
        })
        .buffer_unordered(INGEST_CONCURRENCY);
    while let Some(r) = chunks.next().await {
        r?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heading_hierarchy_and_line_spans() {
        let md = "intro line\n\n# A\n\ntext a\n\n## B\n\ntext b\n\n# C\n\ntext c\n";
        let chunks = chunk_markdown(md);
        let paths: Vec<(Vec<String>, usize, usize)> = chunks
            .iter()
            .map(|c| (c.headings.clone(), c.start_line, c.end_line))
            .collect();
        // preamble, A (with its text), A>B, C — heading lines are inside their
        // own section's span.
        assert_eq!(paths.len(), 4);
        assert_eq!(paths[0], (vec![], 1, 1));
        assert_eq!(paths[1], (vec!["A".into()], 3, 5));
        assert_eq!(paths[2], (vec!["A".into(), "B".into()], 7, 9));
        assert_eq!(paths[3], (vec!["C".into()], 11, 13));
        assert!(chunks[2].body.contains("text b"));
    }

    #[test]
    fn fenced_hash_is_not_a_heading() {
        let md = "# Real\n\n```\n# not a heading\n```\nafter\n";
        let chunks = chunk_markdown(md);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].headings, vec!["Real".to_string()]);
        assert!(chunks[0].body.contains("# not a heading"));
    }

    #[test]
    fn oversized_section_splits_at_paragraphs_with_overlap() {
        let para = "x".repeat(1500);
        let md = format!("# Big\n\n{para}\n\nSHORT TAIL\n\n{para}\n");
        let chunks = chunk_markdown(&md);
        assert!(chunks.len() >= 2, "expected a split, got {}", chunks.len());
        for c in &chunks {
            assert_eq!(c.headings, vec!["Big".to_string()]);
        }
        // the short paragraph is carried over as overlap into the next chunk.
        let with_tail: Vec<usize> = chunks
            .iter()
            .enumerate()
            .filter(|(_, c)| c.body.contains("SHORT TAIL"))
            .map(|(i, _)| i)
            .collect();
        assert!(with_tail.len() >= 2, "overlap not carried: {with_tail:?}");
        // ids (line spans) stay distinct and ordered.
        let spans: Vec<(usize, usize)> =
            chunks.iter().map(|c| (c.start_line, c.end_line)).collect();
        let mut sorted = spans.clone();
        sorted.dedup();
        assert_eq!(sorted.len(), spans.len());
    }

    #[test]
    fn plain_text_without_headings_chunks_by_paragraphs() {
        let big = format!("{}\n\n{}\n\n{}", "a".repeat(1500), "b".repeat(1500), "c".repeat(200));
        let chunks = chunk_markdown(&big);
        assert!(chunks.len() >= 2);
        assert!(chunks.iter().all(|c| c.headings.is_empty()));
    }
}
