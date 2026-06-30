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

            // ---- best-effort cleanup
            for id in ["d1", "d2", "d3", "zh1", "zh2"] {
                let _ = mem.delete_memory(id).await;
            }
            mem.delete_fact("profile", "name").await.ok();

            println!("autumn-memory e2e: full surface OK (agent={agent})");
        });
}
