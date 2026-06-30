//! Lexical retrieval (BM25-on-KV) for autumn-memory — plan §7 词法腿.
//!
//! The inverted index lives as ordinary autumn keys: `idx/{term}/{doc_id}` is
//! an EXISTENCE marker (empty value) so candidate discovery is a pure
//! keys-only range scan. The authoritative record is `doc/{doc_id}` ->
//! [`IndexedDoc`] `{doc_len, terms->tf, text, meta}`: it is the single source
//! of truth, so a stale posting (a term removed on re-index) is simply absent
//! from the doc's current `terms` map and is ignored at query time — no
//! generation stamping needed for the lexical leg (plan §8.5: posting = a
//! candidate hint, doc = the correctness boundary).
//!
//! Scoring is Okapi BM25 computed client-side over the scanned candidates.

use std::collections::HashMap;

/// BM25 term-frequency saturation.
const K1: f32 = 1.2;
/// BM25 length-normalization.
const B: f32 = 0.75;

/// A minimal English stopword set (kept small — over-pruning hurts recall on
/// short agent-memory snippets). CJK / other scripts tokenize per maximal
/// alphanumeric run; proper segmentation is a follow-up.
const STOPWORDS: &[&str] = &[
    "the", "a", "an", "and", "or", "but", "is", "are", "was", "were", "be", "been", "to", "of",
    "in", "on", "for", "with", "as", "at", "by", "it", "its", "this", "that", "these", "those",
    "i", "you", "he", "she", "they", "we", "me", "my", "your", "do", "does", "did", "so", "if",
];

pub(crate) fn is_stopword(t: &str) -> bool {
    STOPWORDS.contains(&t)
}

/// Tokenize into lowercase alphanumeric terms, dropping stopwords.
pub(crate) fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            for lc in ch.to_lowercase() {
                cur.push(lc);
            }
        } else if !cur.is_empty() {
            push_term(&mut out, std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        push_term(&mut out, cur);
    }
    out
}

fn push_term(out: &mut Vec<String>, t: String) {
    if !t.is_empty() && !is_stopword(&t) {
        out.push(fold_plural(&t));
    }
}

/// Conservative English plural folding so a query token matches its plural
/// (and vice-versa): "cats"->"cat", "boxes"->"box", "categories"->"category".
/// Deliberately does NOT touch verb forms (no -ing/-ed) — a naive verb stem
/// like "running"->"runn" would just MISS "run", so it would hurt, not help.
/// Index + query both run through `tokenize`, so the folding stays symmetric.
/// (Full Porter/Snowball via `rust-stemmers` is a possible follow-up.)
fn fold_plural(t: &str) -> String {
    let n = t.len();
    if n < 4 || !t.ends_with('s') {
        return t.to_string();
    }
    // "-ies" -> "-y"  (categories -> category)
    if n >= 5 && t.ends_with("ies") {
        return format!("{}y", &t[..n - 3]);
    }
    // keep genuine non-plural -s endings (class, bus, axis, gas, chaos, ...)
    if matches!(&t[n - 2..], "ss" | "us" | "is" | "as" | "os") {
        return t.to_string();
    }
    // "-es" after a sibilant -> strip "es" (boxes->box, wishes->wish, buzzes->buzz)
    if n >= 5 && t.ends_with("es") {
        let stem = &t[..n - 2];
        if stem.ends_with(['s', 'x', 'z']) || stem.ends_with("ch") || stem.ends_with("sh") {
            return stem.to_string();
        }
    }
    // plain plural -s (cats->cat, dogs->dog, errors->error)
    t[..n - 1].to_string()
}

/// Term frequencies + total token count (doc_len) for a document.
pub(crate) fn term_freqs(text: &str) -> (HashMap<String, u32>, u32) {
    let toks = tokenize(text);
    let len = toks.len() as u32;
    let mut tf: HashMap<String, u32> = HashMap::new();
    for t in toks {
        *tf.entry(t).or_insert(0) += 1;
    }
    (tf, len)
}

/// Unique query terms (sorted, deduped).
pub(crate) fn query_terms(query: &str) -> Vec<String> {
    let mut t = tokenize(query);
    t.sort();
    t.dedup();
    t
}

/// One query-term's Okapi BM25 contribution to a document's score.
pub(crate) fn bm25_term(tf: u32, df: u64, n_docs: u64, doc_len: u32, avgdl: f32) -> f32 {
    if tf == 0 || n_docs == 0 {
        return 0.0;
    }
    let idf = (1.0 + (n_docs as f32 - df as f32 + 0.5) / (df as f32 + 0.5)).ln();
    let tf = tf as f32;
    let denom = tf + K1 * (1.0 - B + B * (doc_len as f32) / avgdl.max(1.0));
    idf * (tf * (K1 + 1.0)) / denom
}

/// Reciprocal Rank Fusion of several ranked id-lists into one score map.
/// `score(id) = Σ_lists 1/(k + rank)` (1-based rank). RRF is ordinal — scale-
/// immune — so the lexical (BM25) and vector (cosine) legs fuse without any
/// score normalization. `k ≈ 60` is the standard constant. Plan §7 hybrid.
pub(crate) fn rrf_fuse(lists: &[Vec<String>], k: f32) -> HashMap<String, f32> {
    let mut scores: HashMap<String, f32> = HashMap::new();
    for list in lists {
        for (rank, id) in list.iter().enumerate() {
            *scores.entry(id.clone()).or_insert(0.0) += 1.0 / (k + rank as f32 + 1.0);
        }
    }
    scores
}

/// The authoritative indexed-document record stored at `doc/{doc_id}`.
pub(crate) struct IndexedDoc {
    pub doc_len: u32,
    pub terms: HashMap<String, u32>,
    pub text: String,
    pub meta: Vec<u8>,
}

impl IndexedDoc {
    /// Encoding (all u32 little-endian):
    /// `[doc_len][n_terms]( [tlen][term][tf] )*[text_len][text][meta_len][meta]`
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(16 + self.text.len() + self.meta.len());
        b.extend_from_slice(&self.doc_len.to_le_bytes());
        b.extend_from_slice(&(self.terms.len() as u32).to_le_bytes());
        for (term, tf) in &self.terms {
            b.extend_from_slice(&(term.len() as u32).to_le_bytes());
            b.extend_from_slice(term.as_bytes());
            b.extend_from_slice(&tf.to_le_bytes());
        }
        b.extend_from_slice(&(self.text.len() as u32).to_le_bytes());
        b.extend_from_slice(self.text.as_bytes());
        b.extend_from_slice(&(self.meta.len() as u32).to_le_bytes());
        b.extend_from_slice(&self.meta);
        b
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        let mut c = Cursor { b: buf, i: 0 };
        let doc_len = c.u32()?;
        let n_terms = c.u32()? as usize;
        let mut terms = HashMap::with_capacity(n_terms);
        for _ in 0..n_terms {
            let tlen = c.u32()? as usize;
            let term = String::from_utf8(c.take(tlen)?.to_vec()).ok()?;
            let tf = c.u32()?;
            terms.insert(term, tf);
        }
        let tlen = c.u32()? as usize;
        let text = String::from_utf8(c.take(tlen)?.to_vec()).ok()?;
        let mlen = c.u32()? as usize;
        let meta = c.take(mlen)?.to_vec();
        Some(IndexedDoc {
            doc_len,
            terms,
            text,
            meta,
        })
    }
}

struct Cursor<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cursor<'a> {
    fn u32(&mut self) -> Option<u32> {
        let s = self.take(4)?;
        Some(u32::from_le_bytes(s.try_into().ok()?))
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.i.checked_add(n)?;
        if end > self.b.len() {
            return None;
        }
        let s = &self.b[self.i..end];
        self.i = end;
        Some(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_basics() {
        assert_eq!(tokenize("Hello, WORLD!"), vec!["hello", "world"]);
        // stopwords dropped
        assert_eq!(tokenize("the cat and a dog"), vec!["cat", "dog"]);
        // punctuation / digits split into runs
        assert_eq!(tokenize("err-code 42x ok"), vec!["err", "code", "42x", "ok"]);
        assert!(tokenize("the and is").is_empty());
    }

    #[test]
    fn plural_folding() {
        // plurals fold to singular so "cat" matches "cats" (the e2e gotcha)
        assert_eq!(tokenize("cats"), vec!["cat"]);
        assert_eq!(tokenize("dogs chase cats"), vec!["dog", "chase", "cat"]);
        assert_eq!(fold_plural("boxes"), "box");
        assert_eq!(fold_plural("wishes"), "wish");
        assert_eq!(fold_plural("categories"), "category");
        assert_eq!(fold_plural("errors"), "error");
        // non-plural -s endings and short words are preserved
        assert_eq!(fold_plural("class"), "class");
        assert_eq!(fold_plural("bus"), "bus");
        assert_eq!(fold_plural("cat"), "cat"); // too short / already singular
        assert_eq!(fold_plural("chaos"), "chaos");
        // verb forms are intentionally untouched (no run/running mismatch)
        assert_eq!(tokenize("running"), vec!["running"]);
    }

    #[test]
    fn bm25_monotonicity() {
        // rarer term (lower df) scores higher than common term, all else equal
        let rare = bm25_term(1, 1, 1000, 10, 10.0);
        let common = bm25_term(1, 500, 1000, 10, 10.0);
        assert!(rare > common, "rarer term must score higher");
        // more occurrences -> higher (saturating) score
        assert!(bm25_term(3, 10, 1000, 10, 10.0) > bm25_term(1, 10, 1000, 10, 10.0));
        // longer doc (same tf) -> lower score (length norm)
        assert!(bm25_term(2, 10, 1000, 5, 10.0) > bm25_term(2, 10, 1000, 50, 10.0));
        // empty / degenerate inputs are 0, not NaN
        assert_eq!(bm25_term(0, 10, 1000, 10, 10.0), 0.0);
        assert_eq!(bm25_term(2, 10, 0, 10, 10.0), 0.0);
    }

    #[test]
    fn indexed_doc_roundtrip() {
        let (terms, doc_len) = term_freqs("the quick brown fox fox");
        let doc = IndexedDoc {
            doc_len,
            terms,
            text: "the quick brown fox fox".to_string(),
            meta: b"{\"src\":\"chat\"}".to_vec(),
        };
        let enc = doc.encode();
        let dec = IndexedDoc::decode(&enc).expect("decode");
        assert_eq!(dec.doc_len, doc_len);
        assert_eq!(dec.text, "the quick brown fox fox");
        assert_eq!(dec.meta, b"{\"src\":\"chat\"}");
        assert_eq!(dec.terms.get("fox"), Some(&2)); // tf
        assert_eq!(dec.terms.get("quick"), Some(&1));
        assert!(!dec.terms.contains_key("the")); // stopword not indexed
    }

    #[test]
    fn decode_rejects_truncated() {
        assert!(IndexedDoc::decode(&[0, 0, 0]).is_none());
        assert!(IndexedDoc::decode(&[]).is_none());
    }

    #[test]
    fn rrf_rewards_agreement() {
        // "b" is top of the vector list and 2nd of the lexical list; "a" tops
        // lexical only. Agreement across both legs should lift "b" highest.
        let lex = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let vec = vec!["b".to_string(), "d".to_string(), "a".to_string()];
        let s = rrf_fuse(&[lex, vec], 60.0);
        assert!(s["b"] > s["a"], "doc ranked well in BOTH legs wins");
        assert!(s["a"] > s["c"], "appearing in two legs beats one leg");
        assert!(s.contains_key("d"));
    }
}
