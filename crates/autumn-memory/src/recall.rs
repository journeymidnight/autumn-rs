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
/// short agent-memory snippets).
const STOPWORDS: &[&str] = &[
    "the", "a", "an", "and", "or", "but", "is", "are", "was", "were", "be", "been", "to", "of",
    "in", "on", "for", "with", "as", "at", "by", "it", "its", "this", "that", "these", "those",
    "i", "you", "he", "she", "they", "we", "me", "my", "your", "do", "does", "did", "so", "if",
];

pub(crate) fn is_stopword(t: &str) -> bool {
    STOPWORDS.contains(&t)
}

/// CJK & kana/hangul codepoint blocks (Han, Hiragana, Katakana, Hangul + their
/// extensions). These scripts have no whitespace word boundaries, so a
/// maximal-alphanumeric run would swallow a whole sentence into one term. We
/// tokenize them per **codepoint** (unigram) AND emit the adjacent **bigram**
/// of every neighbouring pair inside one run.
///
/// Unigrams alone keep single-character queries working (in Chinese a lone
/// character is often a whole word — 猫 = cat, 狗 = dog), but they lose the
/// query's phrase structure, and that loses real searches. Measured on a corpus
/// of Chinese Buddhist texts: `慧能` — the name of the Sixth Patriarch, author
/// of one of the indexed sutras — returned four passages from three OTHER
/// sutras and not one from his own. Split into `慧` + `能`, the common character
/// `能` (able / can) appears in nearly every document, so BM25 ranked whichever
/// text used it most.
///
/// The bigram fixes exactly that: `慧能` also emits the term `慧能`, which is
/// rare, so it dominates scoring for the phrase while the unigrams still serve
/// single-character queries. Index and query both run through this function, so
/// the two stay symmetric for free.
///
/// This was always the plan — the previous version of this comment called
/// bigrams "a future precision refinement" and said the hybrid vector leg
/// supplied phrase-level precision in the meantime. It does not: the default
/// embedder is `HashEmbedder`, a bag-of-words hash whose vectors carry no
/// meaning, so `mode=auto` resolves to lexical and there is no second leg to
/// lean on. The compensation the design assumed was never there.
///
/// Cost: roughly 2n terms for an n-character CJK run instead of n. Postings are
/// empty-value existence markers, so this is index entries, not bytes of value.
///
/// NOTE: the index is a DERIVED artifact — the authoritative `doc/{id}` record
/// keeps the original `text`, so changing this tokenizer never loses data; a
/// deployed corpus is brought current by re-running `index_memory` (reindex).
///
/// This block test is intentionally AND-ed with `is_alphanumeric()` at the call
/// site so kana-block punctuation / combining marks (・ U+30FB, the combining
/// sound marks U+3099/309A, …) are NOT emitted as standalone terms.
fn is_cjk(ch: char) -> bool {
    matches!(ch as u32,
        0x1100..=0x11FF |    // Hangul Jamo
        0x3040..=0x30FF |    // Hiragana + Katakana
        0x3130..=0x318F |    // Hangul Compatibility Jamo
        0x31F0..=0x31FF |    // Katakana Phonetic Extensions
        0x3400..=0x4DBF |    // CJK Unified Ideographs Ext A
        0x4E00..=0x9FFF |    // CJK Unified Ideographs
        0xA960..=0xA97F |    // Hangul Jamo Extended-A
        0xAC00..=0xD7AF |    // Hangul Syllables (+ Jamo Extended-B tail)
        0xF900..=0xFAFF |    // CJK Compatibility Ideographs
        0xFF66..=0xFF9D |    // Halfwidth Katakana
        0x20000..=0x2A6DF |  // CJK Unified Ideographs Ext B
        0x2A700..=0x2EBEF |  // CJK Unified Ideographs Ext C..F
        0x2F800..=0x2FA1F |  // CJK Compatibility Ideographs Supplement
        0x30000..=0x323AF)   // CJK Unified Ideographs Ext G..H
}

/// Tokenize into lowercase alphanumeric terms (dropping stopwords + plural-
/// folding), with CJK codepoints emitted as individual unigram terms.
pub(crate) fn tokenize(text: &str) -> Vec<String> {
    tokenize_inner(text).0
}

/// `(terms, length_bearing_count)`.
///
/// Bigrams are searchable but do NOT count toward `doc_len`. BM25 normalises by
/// document length, and letting an n-character CJK run contribute 2n-1 while an
/// n-word Latin run contributes n would penalise CJK-heavy documents in a mixed
/// corpus — the exact Latin-vs-CJK skew that emitting unigrams was chosen to
/// avoid in the first place. The bigram is a precision device, not more
/// document; length stays measured in the tokens a reader would count.
fn tokenize_inner(text: &str) -> (Vec<String>, u32) {
    let mut base_len: u32 = 0;
    let mut out = Vec::new();
    let mut cur = String::new();
    // The previous character, when it was CJK and immediately precedes the
    // current one. Cleared by anything else so bigrams stay inside one run.
    let mut prev_cjk: Option<char> = None;
    for ch in text.chars() {
        // Require alphanumeric so kana-block punctuation / combining marks fall
        // through to the separator branch instead of becoming bogus terms.
        if is_cjk(ch) && ch.is_alphanumeric() {
            // flush the pending Latin/alnum run, then emit this CJK char as its
            // own term (no case-fold / no plural-fold — neither applies to CJK).
            if !cur.is_empty() {
                let before = out.len();
                push_term(&mut out, std::mem::take(&mut cur));
                base_len += (out.len() - before) as u32;
            }
            // The bigram joins this char to the previous one ONLY when that one
            // was CJK and directly adjacent. Punctuation, spaces and Latin all
            // reset `prev_cjk`, so a bigram never bridges a boundary the writer
            // put there — 「慧能」and「能仁」stay distinct from a 能 that merely
            // follows a comma.
            if let Some(prev) = prev_cjk {
                let mut bi = String::with_capacity(8);
                bi.push(prev);
                bi.push(ch);
                out.push(bi);
            }
            out.push(ch.to_string());
            base_len += 1;
            prev_cjk = Some(ch);
            continue;
        } else if ch.is_alphanumeric() {
            for lc in ch.to_lowercase() {
                cur.push(lc);
            }
        } else if !cur.is_empty() {
            let before = out.len();
            push_term(&mut out, std::mem::take(&mut cur));
            base_len += (out.len() - before) as u32;
        }
        prev_cjk = None;
    }
    if !cur.is_empty() {
        let before = out.len();
        push_term(&mut out, cur);
        base_len += (out.len() - before) as u32;
    }
    (out, base_len)
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
    let (toks, len) = tokenize_inner(text);
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
    // ── CJK bigram tokenization ────────────────────────────────────────────
    //
    // These pin the behaviour that motivated bigrams: a two-character proper
    // noun whose second character is common must not be drowned out by
    // documents that merely use that common character a lot.

    #[test]
    fn cjk_emits_unigrams_and_adjacent_bigrams() {
        let t = super::tokenize("慧能");
        assert!(t.contains(&"慧".to_string()), "unigram 慧 missing: {t:?}");
        assert!(t.contains(&"能".to_string()), "unigram 能 missing: {t:?}");
        assert!(t.contains(&"慧能".to_string()), "bigram 慧能 missing: {t:?}");
    }

    #[test]
    fn single_cjk_char_still_tokenizes() {
        // A lone character is often a whole word in Chinese; bigrams must not
        // cost us that query.
        assert_eq!(super::tokenize("猫"), vec!["猫".to_string()]);
    }

    #[test]
    fn bigrams_do_not_bridge_a_boundary() {
        // 能 follows a comma, so 慧能 is NOT a phrase the writer wrote.
        let t = super::tokenize("慧，能");
        assert!(t.contains(&"慧".to_string()));
        assert!(t.contains(&"能".to_string()));
        assert!(
            !t.contains(&"慧能".to_string()),
            "bigram bridged punctuation: {t:?}"
        );
    }

    #[test]
    fn bigrams_do_not_bridge_latin() {
        let t = super::tokenize("慧a能");
        assert!(!t.contains(&"慧能".to_string()), "bigram bridged latin: {t:?}");
    }

    #[test]
    fn bigram_is_rarer_than_its_common_half() {
        // The failure this fixes, in miniature: a corpus where 能 is everywhere
        // but 慧能 appears once. Term frequency across documents is what BM25
        // scores on, so the discriminating term has to be the bigram.
        let corpus = [
            "慧能大师说法",          // the one document about 慧能
            "能够如是能行能住",      // 能 everywhere, 慧能 nowhere
            "不能不能亦复不能",
        ];
        let counts: Vec<(usize, usize)> = corpus
            .iter()
            .map(|d| {
                let t = super::tokenize(d);
                (
                    t.iter().filter(|x| x.as_str() == "能").count(),
                    t.iter().filter(|x| x.as_str() == "慧能").count(),
                )
            })
            .collect();
        // 能 is common across the corpus...
        assert!(counts[1].0 > counts[0].0, "setup wrong: {counts:?}");
        // ...but the bigram occurs only in the document that is actually about it.
        assert_eq!(counts[0].1, 1, "{counts:?}");
        assert_eq!(counts[1].1, 0, "{counts:?}");
        assert_eq!(counts[2].1, 0, "{counts:?}");
    }

    #[test]
    fn query_and_index_tokenize_identically() {
        // Symmetry is what makes the bigram usable at query time; both sides go
        // through the same function, and this pins that they stay that way.
        let doc = super::tokenize("六祖慧能坛经");
        let query = super::tokenize("慧能");
        for q in &query {
            assert!(doc.contains(q), "query term {q:?} not produced for the doc: {doc:?}");
        }
    }

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
    fn cjk_unigram_tokenization() {
        // a CJK run becomes per-character terms (not one swallowed token), so a
        // single-character query can match a longer doc.
        // Unigrams in reading order, each preceded by its bigram with the
        // previous character. The unigram stream is what this test originally
        // pinned and it is unchanged; the bigrams are additive.
        assert_eq!(
            tokenize("我喜欢猫"),
            vec!["我", "我喜", "喜", "喜欢", "欢", "欢猫", "猫"]
        );
        assert_eq!(query_terms("猫"), vec!["猫"]);
        let (tf, len) = term_freqs("我喜欢猫");
        // doc_len still counts 4: bigrams are searchable but not length-bearing,
        // so BM25's normalisation is unchanged by adding them.
        assert_eq!(len, 4);
        assert_eq!(tf.get("猫"), Some(&1));
        assert_eq!(tf.get("欢猫"), Some(&1));
        // mixed Latin + CJK: Latin runs stay whole + lowercased, CJK splits, and
        // no bigram bridges the Latin boundary on either side.
        let mixed = tokenize("AI很强mode");
        assert_eq!(mixed, vec!["ai", "很", "很强", "强", "mode"]);
        // Japanese kana + Korean hangul: same treatment, since the same runs of
        // script have the same absent word boundaries.
        assert_eq!(tokenize("ねこ"), vec!["ね", "ねこ", "こ"]);
        assert_eq!(tokenize("고양이"), vec!["고", "고양", "양", "양이", "이"]);
        // repeated CJK char accumulates term frequency
        assert_eq!(term_freqs("猫猫狗").0.get("猫"), Some(&2));
        // kana-block PUNCTUATION (・ U+30FB) is a separator, not a term — it
        // must not pollute candidates (coco P2)
        // The separator's job is now doubly visible: 東京 and 大阪 each pair up,
        // and NO 京大 bigram spans the punctuation.
        let jp = tokenize("東京・大阪");
        assert_eq!(jp, vec!["東", "東京", "京", "大", "大阪", "阪"]);
        assert!(!jp.contains(&"京大".to_string()), "bigram crossed ・: {jp:?}");
        // halfwidth katakana is covered too (coco P3)
        assert_eq!(tokenize("ｱｲｳ"), vec!["ｱ", "ｱｲ", "ｲ", "ｲｳ", "ｳ"]);
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
