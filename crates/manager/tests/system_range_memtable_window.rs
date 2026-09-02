//! The range path's memtable view is a WINDOW, not the whole memtable — this
//! pins that paging across the window's edge still returns every key exactly
//! once, in order.
//!
//! `handle_range` used to snapshot (and sort, and clone the values of) the
//! entire memtable on every request, so a 512-key page over a hot partition
//! paid O(memtable) before it looked at a single key. That made any
//! full-prefix walk quadratic. The window fixes the cost; what it puts at risk
//! is correctness at the seam, and there are two seams worth covering.
//!
//! The first is paging: with far more memtable entries than the window holds,
//! every page ends at the window's edge, and only `has_more` keeps the client
//! walking.
//!
//! The second is the one the BOUND exists for. A key can live in an SST while
//! its tombstone lives in the memtable past the window. Reading on after the
//! window would find the SST copy, never see the delete that shadows it, and
//! hand back a key that no longer exists. That is exactly the wipe workload:
//! delete a prefix, and tombstones pile up in the memtable while the keys
//! themselves sit in SSTs.

mod support;

use autumn_rpc::client::RpcClient;

use support::*;

/// Enough keys that the window (4 x limit, floored at 4096) truncates on every
/// page, so the seam is exercised repeatedly rather than once at the end.
const N: u32 = 20_000;
const PAGE: u32 = 512;

#[test]
fn range_pages_across_the_memtable_window_return_every_key_once() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 40).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 951, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Everything stays in the memtable — no flush is triggered at this
        // size, which is exactly the state the window is for.
        for i in 0..N {
            ps_put(&ps, 951, format!("k-{i:06}").as_bytes(), b"v").await;
        }

        let mut collected: Vec<Vec<u8>> = Vec::new();
        let mut start_key: Vec<u8> = Vec::new();
        let mut pages = 0u32;
        loop {
            let page = psr_range(&router, 951, b"k-", &start_key, PAGE).await;
            pages += 1;
            assert!(
                pages <= N / 8 + 64,
                "paging is not terminating — {pages} pages for {N} keys"
            );
            collected.extend(page.entries.iter().map(|e| e.key.clone()));
            if !page.has_more {
                break;
            }
            assert!(
                !page.entries.is_empty(),
                "has_more with an empty page would loop forever: the client \
                 resumes from the same start"
            );
            start_key = page.entries.last().unwrap().key.clone();
            start_key.push(0x00);
        }

        assert_eq!(collected.len(), N as usize, "every key exactly once");
        let mut expected: Vec<Vec<u8>> = (0..N).map(|i| format!("k-{i:06}").into_bytes()).collect();
        expected.sort();
        assert_eq!(collected, expected, "keys must come back sorted, no gaps");
    });
}

/// A key in an SST whose DELETE sits in the memtable past the window must not
/// come back. Without the bound the scan reads on past the window from SSTs
/// alone, cannot see the tombstones, and resurrects every deleted key after the
/// first window's worth.
#[test]
fn range_never_resurrects_a_key_whose_tombstone_is_past_the_window() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 40).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 952, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(72, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        for i in 0..N {
            ps_put(&ps, 952, format!("k-{i:06}").as_bytes(), b"v").await;
        }
        // Push them into an SST, so the deletes below land in a fresh memtable
        // and the only copy of each key is on the SST side of the merge.
        ps_flush(&ps, 952).await;

        // Far more than one window's worth (4 x 512 = 4096) of tombstones.
        const DELETED: u32 = 12_000;
        for i in 0..DELETED {
            ps_delete(&ps, 952, format!("k-{i:06}").as_bytes()).await;
        }

        let mut collected: Vec<Vec<u8>> = Vec::new();
        let mut start_key: Vec<u8> = Vec::new();
        let mut pages = 0u32;
        loop {
            let page = psr_range(&router, 952, b"k-", &start_key, PAGE).await;
            pages += 1;
            assert!(
                pages <= N / 4 + 64,
                "paging is not terminating ({pages} pages)"
            );
            collected.extend(page.entries.iter().map(|e| e.key.clone()));
            if !page.has_more {
                break;
            }
            assert!(
                !page.entries.is_empty(),
                "has_more with an empty page cannot make progress"
            );
            start_key = page.entries.last().unwrap().key.clone();
            start_key.push(0x00);
        }

        let resurrected: Vec<String> = collected
            .iter()
            .filter(|k| {
                String::from_utf8_lossy(k)
                    .strip_prefix("k-")
                    .and_then(|d| d.parse::<u32>().ok())
                    .is_some_and(|i| i < DELETED)
            })
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect();
        assert!(
            resurrected.is_empty(),
            "{} deleted keys came back (first few: {:?})",
            resurrected.len(),
            &resurrected[..resurrected.len().min(5)]
        );
        assert_eq!(
            collected.len(),
            (N - DELETED) as usize,
            "exactly the surviving keys"
        );
    });
}

/// The window must be INVISIBLE on the wire: a short page still means "that is
/// everything", because callers page by exactly that.
///
/// `wipe_agent`, `MemoryStore::scan_keys` and the fuse extent loops all break on
/// `n < limit` and never read `has_more`, and `ClusterClient::range` advances its
/// cursor to the partition's END key on a page it believes complete. So a page
/// that stops at the window's edge does not merely cost a round trip — it ends
/// those scans early, or jumps a gap.
///
/// The shape that produces it: one user key with more unflushed versions than
/// the window holds, and NOTHING in an SST. The window fills with versions of
/// that one key, dedup collapses them to a single entry, and then BOTH
/// iterators run dry — so a merge that treats exhaustion as end-of-scan returns
/// a 1-entry page and calls the prefix finished, with every later key still in
/// the memtable.
#[test]
fn a_hot_key_deeper_than_the_window_does_not_end_the_scan_early() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 40).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 953, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(73, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // "k-hot" sorts before "k-nnnnnn" ('h' < 'n'), so its versions are what
        // the first window is spent on.
        const VERSIONS: u32 = 5_000; // > one window (4 x 512 = 4096)
        const OTHERS: u32 = 100;
        for i in 0..VERSIONS {
            ps_put(&ps, 953, b"k-hot", format!("v{i}").as_bytes()).await;
        }
        for i in 0..OTHERS {
            ps_put(&ps, 953, format!("k-n{i:06}").as_bytes(), b"v").await;
        }
        // Deliberately NO flush: everything must stay in the memtable.

        // Page the way the in-repo consumers do — on the SHORT PAGE, not on
        // has_more. That is the contract the window must not change.
        let mut collected: Vec<Vec<u8>> = Vec::new();
        let mut start_key: Vec<u8> = Vec::new();
        let mut pages = 0u32;
        loop {
            let page = psr_range(&router, 953, b"k-", &start_key, PAGE).await;
            pages += 1;
            assert!(pages <= 64, "paging is not terminating ({pages} pages)");
            let n = page.entries.len() as u32;
            collected.extend(page.entries.iter().map(|e| e.key.clone()));
            if n < PAGE {
                break;
            }
            start_key = page.entries.last().unwrap().key.clone();
            start_key.push(0x00);
        }

        assert_eq!(
            collected.len(),
            (OTHERS + 1) as usize,
            "short-page paging must still see every key; got {:?}...",
            &collected[..collected.len().min(3)]
        );
        assert!(collected.contains(&b"k-hot".to_vec()), "hot key missing");
        for i in 0..OTHERS {
            let want = format!("k-n{i:06}").into_bytes();
            assert!(
                collected.contains(&want),
                "key {} was never returned — the scan ended at the window edge",
                String::from_utf8_lossy(&want)
            );
        }
    });
}
