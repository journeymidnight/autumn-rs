//! Page-boundary regression test for `scan_keys`-based audits, against a LIVE
//! cluster (like `e2e.rs`):
//!
//! ```bash
//! AUTUMN_MEMORY_E2E_MANAGER=127.0.0.1:9001 \
//!   cargo test -p autumn-memory --test scan_boundary -- --ignored --nocapture
//! ```
//!
//! The PS serves a range `start` as an INCLUSIVE user-key bound and the
//! `last_key+\0` resume idiom does not exclude the boundary key (the MVCC
//! internal encoding sorts `K+"\0"` at ts=MAX before K's own versions), so a
//! paged scan sees the boundary key twice. `scan_keys` dedupes; this pins that
//! a corpus larger than one page reconciles to the exact count.

use autumn_memory::MemoryStore;

fn manager_addr() -> String {
    std::env::var("AUTUMN_MEMORY_E2E_MANAGER").unwrap_or_else(|_| "127.0.0.1:9001".to_string())
}

#[test]
#[ignore = "needs a live autumn cluster (set AUTUMN_MEMORY_E2E_MANAGER)"]
fn reconcile_exact_across_page_boundaries() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let agent = format!(
            "scanb-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let mem = MemoryStore::connect(&manager_addr(), "__am_e2e", agent.as_str())
            .await
            .expect("connect")
            .with_page_limit(128); // force ≥2 page boundaries at N=300
        const N: usize = 300;
        for i in 0..N {
            let id = format!("doc-{i:04}");
            mem.index_memory(&id, "alpha beta gamma", b"{}", None)
                .await
                .expect("index_memory");
        }

        let r = mem.reconcile().await.expect("reconcile");
        assert_eq!(r.docs, N as u64, "recount must not double-count page-boundary keys");
        assert_eq!(r.stats_docs, N as u64);
        assert!(r.stats_consistent(), "stats must reconcile exactly: {r:?}");

        // cleanup (best-effort): delete everything this agent wrote.
        for i in 0..N {
            let id = format!("doc-{i:04}");
            mem.delete_memory(&id).await.expect("delete_memory");
        }
    });
}
