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

/// TTL nemesis (plan §16): a memory's TTL expiry removes its doc + postings +
/// vector + vptr, but NOTHING decrements `meta/stats` on expiry — so the BM25
/// corpus stats drift. `reconcile` must DETECT that drift (recount < stats)
/// while the vector legs stay consistent (posting + vptr expire together), and
/// `repair_stats` must heal it. (Split / compaction survival of these plain KV
/// keys is covered by the partition layer's own range-scan chaos tests for all
/// clients — autumn-memory keys are not special there — so this nemesis focuses
/// on the autumn-memory-specific TTL-drift case.)
#[test]
#[ignore = "needs a live autumn cluster (set AUTUMN_MEMORY_E2E_MANAGER); ~7s (TTL wait)"]
fn e2e_reconcile_heals_ttl_drift() {
    use std::time::Duration;
    compio::runtime::Runtime::new()
        .expect("compio runtime")
        .block_on(async {
            let agent = unique_agent();
            let mem = MemoryStore::connect(&manager_addr(), "__am_e2e", agent.as_str())
                .await
                .expect("connect");

            // 3 permanent docs + 2 short-TTL docs (one with a vector).
            for id in ["p1", "p2", "p3"] {
                mem.index_memory(id, "permanent gravity fact", b"", None)
                    .await
                    .expect("index permanent");
            }
            mem.index_memory("e1", "ephemeral lunar note", b"", Some(3))
                .await
                .expect("index e1");
            mem.index_vector("e1", &[1.0, 0.0, 0.0], Some(3)).await.expect("vec e1");
            mem.index_memory("e2", "ephemeral solar note", b"", Some(3))
                .await
                .expect("index e2");

            let r0 = mem.reconcile().await.expect("reconcile pre");
            assert_eq!(r0.docs, 5, "5 docs before expiry: {r0:?}");
            assert!(r0.stats_consistent(), "consistent before expiry: {r0:?}");
            // e1's vector was actually written (guards against a false-positive
            // where the vector never landed).
            assert_eq!(r0.ivf_postings, 1, "e1 vector present before expiry: {r0:?}");
            assert_eq!(r0.vptrs, 1, "e1 vptr present before expiry: {r0:?}");

            // wait out the TTL (server enforces expiry on read).
            std::thread::sleep(Duration::from_secs(6));

            let r1 = mem.reconcile().await.expect("reconcile post");
            assert_eq!(r1.docs, 3, "expired e1,e2 gone from recount: {r1:?}");
            assert_eq!(r1.stats_docs, 5, "stats still count the expired docs: {r1:?}");
            assert!(!r1.stats_consistent(), "TTL expiry drifted stats: {r1:?}");
            // the vector leg expired ATOMICALLY with its doc: the posting AND the
            // vptr are gone (not merely structurally consistent — that would also
            // hold if neither expired), so no orphan/dangling either.
            assert_eq!(r1.ivf_postings, 0, "e1 vector posting expired: {r1:?}");
            assert_eq!(r1.vptrs, 0, "e1 vptr expired: {r1:?}");
            assert_eq!(r1.orphan_ivf, 0, "{r1:?}");
            assert_eq!(r1.dangling_vptr, 0, "{r1:?}");
            assert_eq!(r1.duplicate_ivf, 0, "{r1:?}");
            assert_eq!(r1.malformed_vptr, 0, "{r1:?}");

            // repair heals the drift.
            assert_eq!(mem.repair_stats().await.expect("repair").0, 3);
            assert!(mem.reconcile().await.expect("reconcile healed").is_clean(), "clean after repair");

            // search still serves the survivors, not the expired entries.
            let hits = mem.search_lexical("gravity", 10).await.expect("search");
            assert!(hits.iter().any(|h| h.id == "p1"), "permanent docs still found: {hits:?}");
            let gone = mem.search_lexical("lunar", 10).await.expect("search expired");
            assert!(!gone.iter().any(|h| h.id == "e1"), "expired doc not found: {gone:?}");

            for id in ["p1", "p2", "p3"] {
                let _ = mem.delete_memory(id).await;
            }
            println!("autumn-memory e2e: reconcile heals TTL drift OK (agent={agent})");
        });
}

/// Graph leg: nodes + typed edges as adjacency lists, traversal via range scan.
/// Build A --CALLS--> B --CALLS--> C, then exercise out/in edges, neighbors,
/// nodes_by_kind, and a bounded BFS (the `trace_call_path` primitive). Delete an
/// edge and confirm the reverse index drops it. reconcile stays clean throughout.
#[test]
#[ignore = "needs a live autumn cluster (set AUTUMN_MEMORY_E2E_MANAGER)"]
fn e2e_graph_traversal() {
    use autumn_memory::Dir;
    compio::runtime::Runtime::new()
        .expect("compio runtime")
        .block_on(async {
            let agent = unique_agent();
            let mem = MemoryStore::connect(&manager_addr(), "__am_e2e", agent.as_str())
                .await
                .expect("connect");

            for id in ["A", "B", "C"] {
                mem.put_node(id, "Function", format!("name={id}").as_bytes(), None)
                    .await
                    .expect("put_node");
            }
            mem.add_edge("A", "CALLS", "B", b"line=10", None).await.expect("edge A->B");
            mem.add_edge("B", "CALLS", "C", b"line=20", None).await.expect("edge B->C");

            // out / in edges
            let out_a = mem.out_edges("A", None, None).await.expect("out A");
            assert_eq!(out_a.len(), 1);
            assert_eq!((out_a[0].dst.as_str(), out_a[0].attrs.as_slice()), ("B", &b"line=10"[..]));
            let in_c = mem.in_edges("C", None, None).await.expect("in C");
            assert_eq!(in_c.len(), 1);
            assert_eq!(in_c[0].src, "B");

            // neighbors + nodes_by_kind
            assert_eq!(mem.neighbors("A", Dir::Out, Some("CALLS"), None).await.expect("nb"), vec!["B"]);
            let mut kinds = mem.nodes_by_kind("Function", None).await.expect("by kind");
            kinds.sort();
            assert_eq!(kinds, vec!["A", "B", "C"]);

            // bounded BFS (trace_call_path): A@0, B@1, C@2
            let path = mem.bfs("A", Dir::Out, Some("CALLS"), 5, 100).await.expect("bfs");
            assert_eq!(path, vec![("A".into(), 0), ("B".into(), 1), ("C".into(), 2)]);

            assert!(mem.reconcile().await.expect("rec").is_clean(), "graph clean");

            // delete an edge → reverse index must drop it
            mem.delete_edge("A", "CALLS", "B").await.expect("del edge");
            assert!(mem.out_edges("A", None, None).await.expect("out A2").is_empty());
            assert!(mem.in_edges("B", None, None).await.expect("in B2").is_empty());
            assert!(mem.reconcile().await.expect("rec2").is_clean(), "clean after edge delete");

            for id in ["A", "B", "C"] {
                let _ = mem.delete_node(id).await;
            }
            println!("autumn-memory e2e: graph traversal OK (agent={agent})");
        });
}

/// reconcile FLAGS a dangling edge (a forward edge to a non-existent node),
/// injected via the raw client + `keys::edge_key`, and `is_clean()` goes false.
#[test]
#[ignore = "needs a live autumn cluster (set AUTUMN_MEMORY_E2E_MANAGER)"]
fn e2e_reconcile_flags_dangling_edge() {
    use autumn_client::ClusterClient;
    use std::rc::Rc;
    compio::runtime::Runtime::new()
        .expect("compio runtime")
        .block_on(async {
            let agent = unique_agent();
            let client = Rc::new(ClusterClient::connect(&manager_addr()).await.expect("connect"));
            let mem = MemoryStore::with_client(client.clone(), "__am_e2e", agent.clone());

            mem.put_node("real", "Node", b"", None).await.expect("put real");
            // forward edge real -> ghost, but ghost has no node record.
            let ek = autumn_memory::keys::edge_key("__am_e2e", &agent, "real", "REF", "ghost");
            client.put(&ek, b"").await.expect("inject fwd edge");
            // matching reverse marker so ONLY the missing-node invariant trips.
            let rk = autumn_memory::keys::redge_key("__am_e2e", &agent, "real", "REF", "ghost");
            client.put(&rk, b"").await.expect("inject redge");

            let r = mem.reconcile().await.expect("reconcile");
            assert_eq!(r.dangling_edge, 1, "one dangling edge (ghost node missing): {r:?}");
            assert_eq!(r.orphan_redge, 0, "reverse marker matches the fwd edge: {r:?}");
            assert_eq!(r.missing_redge, 0, "fwd edge has its reverse marker: {r:?}");
            assert!(!r.is_clean(), "not clean with a dangling edge: {r:?}");

            let _ = mem.delete_node("real").await;
            let _ = client.delete(&ek).await;
            let _ = client.delete(&rk).await;
            println!("autumn-memory e2e: reconcile flags dangling edge OK (agent={agent})");
        });
}
