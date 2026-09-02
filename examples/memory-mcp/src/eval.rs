//! Retrieval-quality evaluation: a fixed corpus, a labelled query set, and the
//! numbers that make a change to the retrieval path reviewable.
//!
//! Every knob on that path — the tokenizer's unigrams/bigrams/stopwords, BM25's
//! `k1`/`b`, whether bigrams count toward `doc_len`, RRF fusion, which leg
//! `auto` picks, `NPROBE`, the centroid count, chunk size and overlap, whether
//! the heading breadcrumb is prepended to the indexed text — is currently set
//! by argument. The unit tests assert that the tokenizer emits certain terms
//! and that BM25 is monotonic; they can all pass while a query that used to
//! find the right passage stops finding it. That has happened twice, and both
//! times it was a person noticing, not a test.
//!
//! So this measures the REAL path (`Code::search`, corpus filter and `auto`
//! included), not a reimplementation of it, and reports per mode:
//!
//! * `hit@n` — share of queries with a relevant document in the top n. With
//!   incomplete labels (nobody has judged all 5164 chunks) this is the honest
//!   form of recall: it asks "did the query find something right", which is
//!   answerable, instead of "what share of all right answers did it find",
//!   which is not.
//! * `MRR@k` — mean reciprocal rank of the FIRST relevant hit. This is the
//!   metric that moves when something correct slips from rank 1 to rank 7,
//!   which is the shape of every regression seen so far.
//! * `P@k` — mean share of the top k that is relevant. Only comparable between
//!   runs (its ceiling depends on how many chunks a label happens to cover),
//!   which is exactly what a baseline needs.
//! * `FP@k` — mean share of the top k matching an explicit `reject_substr`.
//!   Lets a label say "these hits are the known wrong answer" rather than only
//!   "this hit is right".
//!
//! Labels are deliberately loose-first (`expect_file` = any chunk of these
//! files), because a label nobody can afford to write is a label that does not
//! exist: judging 17 books by filename is an afternoon, judging 5164 chunks is
//! not.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value};

use crate::store::{Code, Corpus};

/// One labelled query. At least one positive judgment is required — a query
/// with no way to be right is a silent always-miss that drags every metric
/// down for no reason.
#[derive(Debug, Clone)]
pub struct Query {
    pub q: String,
    /// Relevant if the hit's `file` ends with any of these. Suffix, not equal:
    /// a chunk's `file` is relative to the process's cwd when the corpus lives
    /// under it and absolute otherwise, and a label should not care which.
    pub expect_file: Vec<String>,
    /// Relevant if the hit's text contains any of these.
    pub expect_substr: Vec<String>,
    /// Relevant if the hit's id is any of these.
    pub expect_id: Vec<String>,
    /// Counted as a false positive (never as relevant) if the text contains any.
    pub reject_substr: Vec<String>,
    /// Why this query is in the set. Free text, carried into the report so a
    /// regression names the reason the case exists.
    pub why: String,
}

impl Query {
    fn from_json(v: &Value, line_no: usize) -> Result<Query> {
        let strs = |key: &str| -> Vec<String> {
            v.get(key)
                .and_then(|x| x.as_array())
                .map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_string)).collect())
                .unwrap_or_default()
        };
        let q = v
            .get("q")
            .and_then(|x| x.as_str())
            .with_context(|| format!("line {line_no}: missing \"q\""))?
            .to_string();
        let out = Query {
            q,
            expect_file: strs("expect_file"),
            expect_substr: strs("expect_substr"),
            expect_id: strs("expect_id"),
            reject_substr: strs("reject_substr"),
            why: v.get("why").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        };
        if out.expect_file.is_empty() && out.expect_substr.is_empty() && out.expect_id.is_empty() {
            bail!(
                "line {line_no}: query {:?} has no expect_file/expect_substr/expect_id — \
                 it can never be judged right",
                out.q
            );
        }
        Ok(out)
    }

    /// Does this hit satisfy the label? `reject_substr` wins over every
    /// positive: a label saying "this passage is the known wrong answer" is
    /// making a stronger claim than a filename glob.
    fn judges(&self, hit: &Value) -> Judgment {
        let text = hit.get("source").and_then(|v| v.as_str()).unwrap_or("");
        if self.reject_substr.iter().any(|s| text.contains(s.as_str())) {
            return Judgment::Rejected;
        }
        let file = hit.get("file").and_then(|v| v.as_str()).unwrap_or("");
        let id = hit.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let ok = self.expect_file.iter().any(|f| file.ends_with(f.as_str()))
            || self.expect_substr.iter().any(|s| text.contains(s.as_str()))
            || self.expect_id.iter().any(|i| i == id);
        if ok {
            Judgment::Relevant
        } else {
            Judgment::Irrelevant
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Judgment {
    Relevant,
    Irrelevant,
    Rejected,
}

/// Read a goldset. One JSON object per line; blank lines and lines whose first
/// non-space character is `#` are comments — JSON has none, and a label set
/// whose reasons cannot be written next to the labels loses the reasons.
pub fn load(path: &Path) -> Result<Vec<Query>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading goldset {}", path.display()))?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        let v: Value = serde_json::from_str(t)
            .with_context(|| format!("{}:{}: not valid JSON", path.display(), i + 1))?;
        out.push(Query::from_json(&v, i + 1)?);
    }
    if out.is_empty() {
        bail!("goldset {} has no queries", path.display());
    }
    Ok(out)
}

/// What one query did under one mode.
#[derive(Debug, Clone, Default)]
struct Outcome {
    /// 1-based rank of the first relevant hit; 0 = not found in the top k.
    rank: usize,
    /// Relevant hits within the top k.
    n_rel: usize,
    /// `reject_substr` hits within the top k.
    n_rej: usize,
}

/// Aggregate metrics over a goldset, all in `[0,1]`, all "higher is better"
/// except `fp_at_k`.
#[derive(Debug, Clone, Default, PartialEq)]
struct Metrics {
    hit_at_1: f64,
    hit_at_5: f64,
    hit_at_k: f64,
    mrr_at_k: f64,
    p_at_k: f64,
    fp_at_k: f64,
}

fn aggregate(outcomes: &[Outcome], k: usize) -> Metrics {
    if outcomes.is_empty() {
        return Metrics::default();
    }
    let n = outcomes.len() as f64;
    let hit_within = |limit: usize| {
        outcomes.iter().filter(|o| o.rank > 0 && o.rank <= limit).count() as f64 / n
    };
    Metrics {
        hit_at_1: hit_within(1),
        hit_at_5: hit_within(5.min(k)),
        hit_at_k: hit_within(k),
        mrr_at_k: outcomes
            .iter()
            .map(|o| if o.rank > 0 { 1.0 / o.rank as f64 } else { 0.0 })
            .sum::<f64>()
            / n,
        // Divided by k, not by the number of hits returned: a mode that returns
        // three documents where ten were asked for has NOT earned the precision
        // of a full page. Under-filling is a real defect (the corpus filter
        // post-filters an over-fetch), and dividing by the count would hide it.
        p_at_k: outcomes.iter().map(|o| o.n_rel as f64 / k as f64).sum::<f64>() / n,
        fp_at_k: outcomes.iter().map(|o| o.n_rej as f64 / k as f64).sum::<f64>() / n,
    }
}

impl Metrics {
    fn to_json(&self) -> Value {
        // Rounded to 4 places so a baseline diff shows real movement rather
        // than float noise in the last bits.
        let r = |x: f64| (x * 10_000.0).round() / 10_000.0;
        json!({
            "hit@1": r(self.hit_at_1), "hit@5": r(self.hit_at_5), "hit@k": r(self.hit_at_k),
            "mrr@k": r(self.mrr_at_k), "p@k": r(self.p_at_k), "fp@k": r(self.fp_at_k),
        })
    }
    /// `(name, value, higher_is_better)` — the order the report prints.
    fn fields(&self) -> [(&'static str, f64, bool); 6] {
        [
            ("hit@1", self.hit_at_1, true),
            ("hit@5", self.hit_at_5, true),
            ("hit@k", self.hit_at_k, true),
            ("mrr@k", self.mrr_at_k, true),
            ("p@k", self.p_at_k, true),
            ("fp@k", self.fp_at_k, false),
        ]
    }
}

/// A miss worth printing: the query, why it is in the set, and what came back
/// instead — without the top few hits a miss is a number nobody can act on.
struct Miss {
    q: String,
    why: String,
    top: Vec<String>,
}

/// Run the goldset under each mode and build the report.
pub async fn run(code: &Code, queries: &[Query], k: usize, modes: &[String]) -> Result<Value> {
    let mut modes_json = Map::new();
    for mode in modes {
        let mut outcomes = Vec::with_capacity(queries.len());
        let mut ranks = Map::new();
        let mut misses: Vec<Miss> = Vec::new();
        for query in queries {
            // Corpus::Docs, because the goldset labels documents. An eval that
            // searched everything would score the code index too and blur which
            // corpus moved.
            let hits = code.search(&query.q, mode, k, Corpus::Docs).await?;
            let mut o = Outcome::default();
            for (i, hit) in hits.iter().enumerate() {
                match query.judges(hit) {
                    Judgment::Relevant => {
                        o.n_rel += 1;
                        if o.rank == 0 {
                            o.rank = i + 1;
                        }
                    }
                    Judgment::Rejected => o.n_rej += 1,
                    Judgment::Irrelevant => {}
                }
            }
            if o.rank == 0 {
                misses.push(Miss {
                    q: query.q.clone(),
                    why: query.why.clone(),
                    top: hits
                        .iter()
                        .take(3)
                        .map(|h| {
                            format!(
                                "{} [{}]",
                                h.get("id").and_then(|v| v.as_str()).unwrap_or("?"),
                                h.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0)
                            )
                        })
                        .collect(),
                });
            }
            ranks.insert(query.q.clone(), json!(o.rank));
            outcomes.push(o);
        }
        let m = aggregate(&outcomes, k);
        print_mode(mode, &m, &misses, queries.len());
        modes_json.insert(
            mode.clone(),
            json!({ "metrics": m.to_json(), "ranks": Value::Object(ranks) }),
        );
    }
    Ok(json!({
        "k": k,
        "queries": queries.len(),
        "modes": Value::Object(modes_json),
    }))
}

fn print_mode(mode: &str, m: &Metrics, misses: &[Miss], n_q: usize) {
    println!(
        "\nmode={mode:<8} hit@1 {:.3}  hit@5 {:.3}  hit@k {:.3}  MRR@k {:.3}  P@k {:.3}  FP@k {:.3}   ({}/{} found)",
        m.hit_at_1,
        m.hit_at_5,
        m.hit_at_k,
        m.mrr_at_k,
        m.p_at_k,
        m.fp_at_k,
        n_q - misses.len(),
        n_q,
    );
    for miss in misses {
        println!("  MISS {:?}{}", miss.q, if miss.why.is_empty() { String::new() } else { format!("  — {}", miss.why) });
        for t in &miss.top {
            println!("       got {t}");
        }
    }
}

/// Compare a report against a baseline. Returns true when something regressed
/// by more than `tol`, and prints every movement — metric-level first, then the
/// per-query rank changes, which are what actually name the broken case.
pub fn compare(report: &Value, baseline: &Value, tol: f64) -> bool {
    let mut regressed = false;
    let empty = Map::new();
    let base_modes = baseline.get("modes").and_then(|v| v.as_object()).unwrap_or(&empty);
    let cur_modes = report.get("modes").and_then(|v| v.as_object()).unwrap_or(&empty);

    // A baseline is only meaningful against the same goldset, the same corpus
    // and the same embedder. Comparing across any of them produces a number
    // that looks like a regression and is not one, so say so loudly rather
    // than let someone chase it.
    // Re-ingesting retrains the IVF centroids, and k-means re-initialises from
    // the CURRENT scan order — which is the previous training's bucketing — so
    // it settles into a different local optimum each time. Measured on the
    // 17-book corpus: with no retrain, all three modes are byte-identical
    // across runs; with a retrain, lexical is unchanged while vector and hybrid
    // both move. That movement is the clustering, not the retrieval code, and
    // without this note it reads as a regression in whatever was edited last.
    if report.get("retrained") == Some(&Value::Bool(true))
        || baseline.get("retrained") == Some(&Value::Bool(true))
    {
        println!(
            "\nNOTE: an index rebuild retrained the IVF centroids in one of these runs — \
             vector/hybrid movement below may be the clustering, not the retrieval path. \
             The lexical leg is unaffected."
        );
    }
    for field in ["queries", "k", "corpus", "embedder"] {
        let (b, c) = (baseline.get(field), report.get(field));
        if b.is_some() && b != c {
            println!(
                "\nNOTE: {field} changed ({} → {}) — this run is not comparable to the \
                 baseline; re-baseline deliberately.",
                b.unwrap_or(&Value::Null),
                c.unwrap_or(&Value::Null),
            );
        }
    }
    println!("\n--- vs baseline (tolerance {tol:.3}) ---");
    for (mode, cur) in cur_modes {
        let Some(base) = base_modes.get(mode) else {
            println!("mode={mode}: not in baseline (new mode)");
            continue;
        };
        for (name, _, higher_better) in Metrics::default().fields() {
            let get = |v: &Value| {
                v.get("metrics").and_then(|m| m.get(name)).and_then(|x| x.as_f64())
            };
            let (Some(b), Some(c)) = (get(base), get(cur)) else {
                continue;
            };
            let delta = c - b;
            let worse = if higher_better { delta < -tol } else { delta > tol };
            if worse {
                regressed = true;
                println!("  REGRESSED {mode:<8} {name:<6} {b:.4} → {c:.4}  ({delta:+.4})");
            } else if delta.abs() > tol {
                println!("  improved  {mode:<8} {name:<6} {b:.4} → {c:.4}  ({delta:+.4})");
            }
        }
        // Per-query ranks: the metric says something moved, this says what.
        let rank_of = |v: &Value, q: &str| {
            v.get("ranks").and_then(|r| r.get(q)).and_then(|x| x.as_u64()).unwrap_or(0)
        };
        let cur_ranks = cur.get("ranks").and_then(|v| v.as_object()).unwrap_or(&empty);
        // BTreeMap so the printed order is the query text, not hash order.
        let ordered: BTreeMap<&String, &Value> = cur_ranks.iter().collect();
        for (q, _) in ordered {
            let (b, c) = (rank_of(base, q), rank_of(cur, q));
            if b == c {
                continue;
            }
            let fmt = |r: u64| if r == 0 { "miss".to_string() } else { format!("rank {r}") };
            // A rank of 0 is a miss, which is WORSE than any rank — comparing
            // the numbers directly would read a new miss as an improvement.
            let worse = c == 0 || (b != 0 && c > b);
            let tag = if worse { "WORSE " } else { "better" };
            if worse {
                regressed = true;
            }
            println!("  {tag} {mode:<8} {:?}: {} → {}", q, fmt(b), fmt(c));
        }
    }
    if !regressed {
        println!("  no regression");
    }
    regressed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(id: &str, file: &str, text: &str) -> Value {
        json!({"id": id, "file": file, "source": text, "score": 1.0})
    }

    fn q(v: Value) -> Query {
        Query::from_json(&v, 1).unwrap()
    }

    #[test]
    fn expect_file_matches_by_suffix_not_equality() {
        let query = q(json!({"q": "慧能", "expect_file": ["liuzu-tanjing.md"]}));
        // absolute (corpus outside cwd) and relative (inside) must both judge.
        for f in ["/data/dongmao_dev/md/liuzu-tanjing.md", "md/liuzu-tanjing.md"] {
            assert_eq!(query.judges(&hit("x", f, "…")), Judgment::Relevant, "{f}");
        }
        assert_eq!(
            query.judges(&hit("x", "md/fjsj-fahua-jing.md", "…")),
            Judgment::Irrelevant,
        );
    }

    #[test]
    fn reject_beats_every_positive_judgment() {
        // The 慧能 shape: the right book also contains the wrong sense.
        let query = q(json!({
            "q": "慧能",
            "expect_file": ["liuzu-tanjing.md"],
            "reject_substr": ["智慧能"],
        }));
        let h = hit("x", "md/liuzu-tanjing.md", "運用大智慧能使我們的心");
        assert_eq!(query.judges(&h), Judgment::Rejected);
    }

    #[test]
    fn substr_and_id_labels_judge() {
        let by_substr = q(json!({"q": "无念", "expect_substr": ["无念为宗"]}));
        assert_eq!(
            by_substr.judges(&hit("a", "any.md", "我此法门 无念为宗")),
            Judgment::Relevant,
        );
        let by_id = q(json!({"q": "x", "expect_id": ["md/a.md#L1-L9"]}));
        assert_eq!(by_id.judges(&hit("md/a.md#L1-L9", "z.md", "")), Judgment::Relevant);
        assert_eq!(by_id.judges(&hit("md/a.md#L2-L9", "z.md", "")), Judgment::Irrelevant);
    }

    #[test]
    fn a_query_with_no_positive_label_is_rejected_at_load() {
        let e = Query::from_json(&json!({"q": "x", "reject_substr": ["y"]}), 7).unwrap_err();
        assert!(e.to_string().contains("never be judged right"), "{e}");
    }

    #[test]
    fn goldset_skips_comments_and_blank_lines() {
        let dir = std::env::temp_dir().join("memory-mcp-eval-test");
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("g.jsonl");
        std::fs::write(
            &p,
            "# a comment\n\n{\"q\":\"a\",\"expect_file\":[\"x.md\"]}\n   # indented comment\n{\"q\":\"b\",\"expect_substr\":[\"s\"]}\n",
        )
        .unwrap();
        let qs = load(&p).unwrap();
        assert_eq!(qs.len(), 2);
        assert_eq!(qs[1].q, "b");
    }

    #[test]
    fn metrics_are_the_textbook_arithmetic() {
        // ranks 1, 3, miss, 2 over k=10; hand-computed.
        let outcomes = vec![
            Outcome { rank: 1, n_rel: 2, n_rej: 0 },
            Outcome { rank: 3, n_rel: 1, n_rej: 1 },
            Outcome { rank: 0, n_rel: 0, n_rej: 3 },
            Outcome { rank: 2, n_rel: 1, n_rej: 0 },
        ];
        let m = aggregate(&outcomes, 10);
        assert!((m.hit_at_1 - 0.25).abs() < 1e-9);
        assert!((m.hit_at_5 - 0.75).abs() < 1e-9);
        assert!((m.hit_at_k - 0.75).abs() < 1e-9);
        // (1/1 + 1/3 + 0 + 1/2) / 4
        assert!((m.mrr_at_k - (1.0 + 1.0 / 3.0 + 0.5) / 4.0).abs() < 1e-9);
        // (2 + 1 + 0 + 1) / (4 * 10)
        assert!((m.p_at_k - 0.1).abs() < 1e-9);
        assert!((m.fp_at_k - 0.1).abs() < 1e-9);
    }

    #[test]
    fn hit_at_5_does_not_exceed_k() {
        // With k=3 a "hit@5" would otherwise claim credit for ranks nobody asked for.
        let m = aggregate(&[Outcome { rank: 3, n_rel: 1, n_rej: 0 }], 3);
        assert!((m.hit_at_5 - 1.0).abs() < 1e-9);
        assert!((m.hit_at_k - 1.0).abs() < 1e-9);
    }

    #[test]
    fn a_new_miss_is_a_regression_even_though_zero_is_a_small_number() {
        let base = json!({"queries": 1, "modes": {"lexical": {
            "metrics": {"hit@1":1.0,"hit@5":1.0,"hit@k":1.0,"mrr@k":1.0,"p@k":0.1,"fp@k":0.0},
            "ranks": {"慧能": 1}}}});
        let now = json!({"queries": 1, "modes": {"lexical": {
            "metrics": {"hit@1":0.0,"hit@5":0.0,"hit@k":0.0,"mrr@k":0.0,"p@k":0.0,"fp@k":0.0},
            "ranks": {"慧能": 0}}}});
        assert!(compare(&now, &base, 0.01));
        assert!(!compare(&base, &base, 0.01));
    }

    #[test]
    fn a_rise_in_false_positives_is_a_regression_though_the_number_went_up() {
        let mk = |fp: f64| json!({"queries": 1, "modes": {"lexical": {
            "metrics": {"hit@1":1.0,"hit@5":1.0,"hit@k":1.0,"mrr@k":1.0,"p@k":0.1,"fp@k":fp},
            "ranks": {"慧能": 1}}}});
        assert!(compare(&mk(0.4), &mk(0.1), 0.01));
        assert!(!compare(&mk(0.1), &mk(0.4), 0.01));
    }
}
