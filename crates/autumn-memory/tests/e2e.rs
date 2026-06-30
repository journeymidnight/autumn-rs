//! End-to-end test for `autumn-memory` against a LIVE cluster.
//!
//! `#[ignore]` — needs a running autumn cluster (manager + EN(s) + PS). Point
//! it at the manager and run:
//!
//! ```bash
//! AUTUMN_MEMORY_E2E_MANAGER=127.0.0.1:9001 \
//!   cargo test -p autumn-memory --test e2e -- --ignored --nocapture
//! ```
//!
//! Exercises the full Phase-1 surface end to end: episodic log, fact KV,
//! BM25-on-KV lexical recall, SPFresh-IVF-on-KV vector recall, and hybrid RRF.
//! Uses a unique `(tenant, agent)` per run so it never collides and cleans up
//! after itself (best-effort).

use autumn_memory::MemoryStore;

fn manager_addr() -> String {
    std::env::var("AUTUMN_MEMORY_E2E_MANAGER").unwrap_or_else(|_| "127.0.0.1:9001".to_string())
}

fn unique_agent() -> String {
    // process id + a monotonic-ish stamp keeps re-runs from colliding.
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("e2e-{}-{}", std::process::id(), t)
}

#[test]
#[ignore = "needs a live autumn cluster (set AUTUMN_MEMORY_E2E_MANAGER)"]
fn e2e_full_surface() {
    compio::runtime::Runtime::new()
        .expect("compio runtime")
        .block_on(async {
            let agent = unique_agent();
            let mem = MemoryStore::connect(&manager_addr(), "__am_e2e", agent.as_str())
                .await
                .expect("connect");

            // ---- episodic log: append + replay (chronological) + recent (newest-first)
            for (i, role) in ["user", "assistant", "user"].iter().enumerate() {
                let ev = format!(r#"{{"role":"{role}","seq":{i}}}"#);
                mem.append_event("sess-1", ev.as_bytes(), None)
                    .await
                    .expect("append_event");
            }
            let replay = mem.replay_session("sess-1", None).await.expect("replay");
            assert_eq!(replay.len(), 3, "replay returns all 3 events");
            assert!(
                String::from_utf8_lossy(&replay[0]).contains(r#""seq":0"#),
                "replay is chronological (oldest first)"
            );
            let recent = mem.recent_events("sess-1", 2).await.expect("recent");
            assert_eq!(recent.len(), 2);
            assert!(
                String::from_utf8_lossy(&recent[0]).contains(r#""seq":2"#),
                "recent is newest-first"
            );

            // ---- fact KV (LangGraph BaseStore model)
            mem.put_fact("profile", "name", b"Alice", None)
                .await
                .expect("put_fact");
            mem.put_fact("profile", "lang", b"rust", None)
                .await
                .expect("put_fact");
            assert_eq!(
                mem.get_fact("profile", "name").await.expect("get_fact"),
                Some(b"Alice".to_vec())
            );
            let facts = mem.list_facts("profile", None).await.expect("list_facts");
            assert_eq!(facts.len(), 2, "two facts listed");
            mem.delete_fact("profile", "lang").await.expect("delete_fact");
            assert!(mem.get_fact("profile", "lang").await.expect("get").is_none());

            // ---- BM25-on-KV lexical recall
            // Plural folding makes the query "cat" match d1 ("cat") AND d2
            // ("cats" -> "cat"); d3 has neither.
            mem.index_memory("d1", "the cat sat on the mat", b"", None)
                .await
                .expect("index d1");
            mem.index_memory("d2", "dogs chase cats in the yard", b"", None)
                .await
                .expect("index d2");
            mem.index_memory("d3", "quantum error correction codes", b"", None)
                .await
                .expect("index d3");
            let hits = mem.search_lexical("cat", 10).await.expect("search_lexical");
            let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
            assert!(ids.contains(&"d1") && ids.contains(&"d2"), "cat -> d1, d2: {ids:?}");
            assert!(!ids.contains(&"d3"), "d3 has no 'cat'");
            let mat = mem.search_lexical("mat", 10).await.expect("search mat");
            assert_eq!(mat.first().map(|h| h.id.as_str()), Some("d1"), "mat -> d1 only");

            // ---- CJK unigram tokenization (Chinese): a single-character query
            // matches a longer doc through the full posting path (CJK term bytes
            // percent-encoded in the posting key, scanned, scored).
            mem.index_memory("zh1", "我喜欢猫", b"", None).await.expect("index zh1");
            mem.index_memory("zh2", "狗很可爱", b"", None).await.expect("index zh2");
            let zh = mem.search_lexical("猫", 10).await.expect("search 猫");
            let zids: Vec<&str> = zh.iter().map(|h| h.id.as_str()).collect();
            assert!(zids.contains(&"zh1") && !zids.contains(&"zh2"), "猫 -> zh1 only: {zids:?}");

            // ---- SPFresh-IVF-on-KV vector recall (caller-supplied vectors)
            mem.index_vector("d1", &[1.0, 0.0, 0.0], None).await.expect("vec d1");
            mem.index_vector("d2", &[0.0, 1.0, 0.0], None).await.expect("vec d2");
            mem.index_vector("d3", &[0.0, 0.0, 1.0], None).await.expect("vec d3");
            // before training there are no centroids -> brute-force fallback
            let v = mem.search_vector(&[0.9, 0.1, 0.0], 2, 4).await.expect("search_vector");
            assert_eq!(v.first().map(|p| p.0.as_str()), Some("d1"), "nearest = d1: {v:?}");
            // train + search again through the IVF buckets
            let ncent = mem.train_centroids(3, 25, 7).await.expect("train");
            assert!(ncent >= 1, "trained at least one centroid");
            let v2 = mem.search_vector(&[0.1, 0.95, 0.0], 2, 3).await.expect("search post-train");
            assert_eq!(v2.first().map(|p| p.0.as_str()), Some("d2"), "nearest = d2: {v2:?}");

            // ---- hybrid RRF (both legs)
            let hy = mem
                .search_hybrid("cat", &[0.9, 0.1, 0.0], 3, 3)
                .await
                .expect("search_hybrid");
            assert!(!hy.is_empty(), "hybrid returns fused results");
            assert!(hy.iter().any(|p| p.0 == "d1"), "d1 wins both legs: {hy:?}");

            // ---- F-MEM-4: deleting a memory reaps its IVF vector posting
            // (not just doc + BM25), so it no longer surfaces in vector search.
            // (train_centroids never reaps a deleted vector — it re-buckets
            // every posting it scans — so the vptr-based reap is the reaper.)
            mem.delete_memory("d1").await.expect("delete d1");
            let after = mem
                .search_vector(&[0.95, 0.05, 0.0], 3, 4)
                .await
                .expect("search after delete");
            assert!(!after.iter().any(|p| p.0 == "d1"), "d1 IVF posting reaped: {after:?}");

            // ---- index-reconcile (plan §16 acceptance): after inserts, a
            // delete (d1), and CJK docs, the index is internally consistent —
            // stats match a full recount, and every IVF posting has a live
            // reverse pointer (no orphan/dangling vectors).
            let rep = mem.reconcile().await.expect("reconcile");
            assert!(rep.is_clean(), "index reconcile clean: {rep:?}");
            assert_eq!(rep.docs, 4, "live docs = d2,d3,zh1,zh2: {rep:?}");
            assert_eq!(rep.ivf_postings, 2, "vectors d2,d3 (d1 reaped): {rep:?}");
            // repair is a no-op on an already-consistent index
            assert_eq!(mem.repair_stats().await.expect("repair"), (4, rep.sum_doc_len));
            assert!(mem.reconcile().await.expect("reconcile2").is_clean());

            // ---- best-effort cleanup
            for id in ["d1", "d2", "d3", "zh1", "zh2"] {
                let _ = mem.delete_memory(id).await;
            }
            mem.delete_fact("profile", "name").await.ok();

            println!("autumn-memory e2e: full surface OK (agent={agent})");
        });
}

/// Reconcile DETECTS multi-writer `meta/stats` drift and `repair_stats` heals
/// it. We inject the drift deterministically (clobber the stats key via the raw
/// client) instead of racing two writers, so the test isn't flaky. This is the
/// deliberate answer to the stats RMW race: tolerate it on the hot path, detect
/// + repair off it (plan §16).
#[test]
#[ignore = "needs a live autumn cluster (set AUTUMN_MEMORY_E2E_MANAGER)"]
fn e2e_reconcile_repairs_stats_drift() {
    use autumn_client::ClusterClient;
    use std::rc::Rc;
    compio::runtime::Runtime::new()
        .expect("compio runtime")
        .block_on(async {
            let agent = unique_agent();
            let client = Rc::new(ClusterClient::connect(&manager_addr()).await.expect("connect"));
            let mem = MemoryStore::with_client(client.clone(), "__am_e2e", agent.clone());

            for (i, id) in ["a", "b", "c"].iter().enumerate() {
                mem.index_memory(id, &format!("doc number {i} some content"), b"", None)
                    .await
                    .expect("index");
            }
            assert!(mem.reconcile().await.expect("rec").stats_consistent(), "consistent after writes");

            // inject drift: claim 1 doc when there are 3 (a lost-update would
            // look like this).
            let skey = autumn_memory::keys::stats_key("__am_e2e", &agent);
            let mut bogus = [0u8; 16];
            bogus[..8].copy_from_slice(&1u64.to_le_bytes());
            client.put(&skey, &bogus).await.expect("inject drift");

            let drift = mem.reconcile().await.expect("rec drift");
            assert!(!drift.stats_consistent(), "drift detected: {drift:?}");
            assert_eq!(drift.docs, 3, "recount sees all 3: {drift:?}");
            assert_eq!(drift.stats_docs, 1, "stored stat is the bogus 1: {drift:?}");

            assert_eq!(mem.repair_stats().await.expect("repair").0, 3, "repaired to 3");
            assert!(mem.reconcile().await.expect("rec healed").is_clean(), "clean after repair");

            for id in ["a", "b", "c"] {
                let _ = mem.delete_memory(id).await;
            }
            println!("autumn-memory e2e: reconcile detect+repair OK (agent={agent})");
        });
}
