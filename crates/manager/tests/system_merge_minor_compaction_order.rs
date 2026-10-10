//! Minor compactions after a merge must not let an older SST shadow a newer
//! one.
//!
//! Point reads walk the table list from the back and stop at the first hit, so
//! the list has to hold, for every key, its newer versions later. After a
//! merge the list is the survivor's tables then the victim's, and the two
//! sequence counters were independent (`last_seq` order V0 < S1 < S2 < V1 <
//! S3 here). A selector that picked a run contiguous in `last_seq` order —
//! `[S1, S2, V1]`, not contiguous in the list — put the output at S1's slot,
//! in front of V0, and V0's older copy of a key V1 overwrote was found first.
//! Windows are now runs of the LIST; this keeps the shape and checks the newest
//! value survives the merged partition's minor compactions.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, MergePartitionsReq, MergePartitionsResp, MSG_MERGE_PARTITIONS,
};
use autumn_rpc::partition_rpc::{self, DiagTraceKeyReq, DiagTraceKeyResp, CODE_OK};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

const SURVIVOR: u64 = 1401;
const VICTIM: u64 = 1402;
/// Small memtables so a few hundred puts make a table.
const FLUSH_BYTES: u64 = 128 * 1024;
const VALUE: usize = 256;
const KEY: &[u8] = b"n-key";

/// SSTs the survivor's checkpoint lists (its last record).
async fn listed_ssts(mgr_addr: std::net::SocketAddr, meta_stream: u64) -> usize {
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        "merge-minor-order-test".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    let info = sc.get_stream_info(meta_stream).await.expect("meta stream info");
    let eid = *info.extent_ids.last().expect("meta extent");
    let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await.expect("read meta");
    decode_last_table_locations(&payload).locs.len()
}

/// Every SST's `last_seq`, in list order.
async fn table_seqs(c: &RpcClient, part: u64) -> Vec<u64> {
    let bytes = c
        .call(
            partition_rpc::MSG_DIAG_TRACE_KEY,
            partition_rpc::rkyv_encode(&DiagTraceKeyReq { part_id: part, user_key: KEY.to_vec() }),
        )
        .await
        .expect("diag trace key");
    let r: DiagTraceKeyResp = partition_rpc::rkyv_decode(&bytes).expect("decode trace");
    assert_eq!(r.code, CODE_OK, "trace: {}", r.message);
    r.sst_last_seqs
}

async fn put_n(c: &RpcClient, part: u64, prefix: &str, n: usize) {
    let value = vec![b'v'; VALUE];
    for i in 0..n {
        ps_put(c, part, format!("{prefix}-{i:05}").as_bytes(), &value).await;
    }
}

#[test]
fn a_minor_compaction_after_a_merge_keeps_the_newest_version_visible() {
    assert!(autumn_partition_server::set_flush_mem_bytes(FLUSH_BYTES));
    autumn_partition_server::set_compact_max_sst_bytes(2 * FLUSH_BYTES).expect("compact max sst");
    autumn_partition_server::background::set_minor_compaction_paused(true);
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("mgr");
        register_node(&mgr, &en_addr.to_string(), "uuid-merge-minor").await;
        let (l, r, m) = (create_stream(&mgr, 1).await, create_stream(&mgr, 1).await, create_stream(&mgr, 1).await);
        upsert_partition(&mgr, SURVIVOR, l, r, m, b"", b"m").await;
        let (l, r, m) = (create_stream(&mgr, 1).await, create_stream(&mgr, 1).await, create_stream(&mgr, 1).await);
        upsert_partition(&mgr, VICTIM, l, r, m, b"m", b"").await;

        let ps_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps_addr);
        let router = PsRouter::new(mgr_addr, ps_addr);
        let s = router.client_for(SURVIVOR).await;
        let v = router.client_for(VICTIM).await;

        // V0 holds KEY's old value; victim seq ~300.
        ps_put(&v, VICTIM, KEY, b"old").await;
        put_n(&v, VICTIM, "n-v0", 299).await;
        ps_flush(&v, VICTIM).await;
        // S1: survivor seq ~350.
        put_n(&s, SURVIVOR, "b-s1", 350).await;
        ps_flush(&s, SURVIVOR).await;
        // S2: ~370.
        put_n(&s, SURVIVOR, "b-s2", 20).await;
        ps_flush(&s, SURVIVOR).await;
        // V1 ends with KEY's new value; victim seq ~400.
        put_n(&v, VICTIM, "n-v1", 99).await;
        ps_put(&v, VICTIM, KEY, b"new").await;
        ps_flush(&v, VICTIM).await;
        // S3: ~670.
        put_n(&s, SURVIVOR, "b-s3", 300).await;
        ps_flush(&s, SURVIVOR).await;
        assert_eq!(ps_get(&v, VICTIM, KEY).await.value, b"new");

        // By the tables' own seqs: V0 < S1 < S2 < V1 < S3.
        let v_seqs = table_seqs(&v, VICTIM).await;
        let s_seqs = table_seqs(&s, SURVIVOR).await;
        assert_eq!(v_seqs.len(), 2, "victim tables: {v_seqs:?}");
        assert_eq!(s_seqs.len(), 3, "survivor tables: {s_seqs:?}");
        let (v0, v1, s1, s2, s3) = (v_seqs[0], v_seqs[1], s_seqs[0], s_seqs[1], s_seqs[2]);
        assert!(
            v0 < s1 && s1 < s2 && s2 < v1 && v1 < s3,
            "seq order: V0 {v0} S1 {s1} S2 {s2} V1 {v1} S3 {s3}"
        );

        let bytes = mgr
            .call(
                MSG_MERGE_PARTITIONS,
                rkyv_encode(&MergePartitionsReq {
                    survivor_part_id: SURVIVOR,
                    victim_part_id: VICTIM,
                    force: false,
                }),
            )
            .await
            .expect("merge call");
        let resp: MergePartitionsResp = rkyv_decode(&bytes).expect("merge resp");
        assert_eq!(resp.code, CODE_OK, "merge: {}", resp.message);

        let meta = get_regions(&mgr)
            .await
            .regions
            .iter()
            .find(|(id, _)| *id == SURVIVOR)
            .expect("survivor region")
            .1
            .meta_stream;
        // The merged survivor serves the victim's range once it reopens (the
        // old listener answers the old range until then) and lists all five
        // tables before serving.
        let deadline = Instant::now() + Duration::from_secs(90);
        let merged = loop {
            if let Ok(c) = router.try_client_for(SURVIVOR).await {
                if ps_get(&c, SURVIVOR, KEY).await.code == CODE_OK {
                    break c;
                }
            }
            assert!(Instant::now() < deadline, "the merged survivor never served the victim range");
            compio::time::sleep(Duration::from_millis(200)).await;
        };
        assert_eq!(listed_ssts(mgr_addr, meta).await, 5, "the merged list");
        // Let the minor compactions run, reading KEY between them.
        let runs = || autumn_partition_server::background::minor_compaction_runs().1;
        let before = runs();
        autumn_partition_server::background::set_minor_compaction_paused(false);
        let watch_until = Instant::now() + Duration::from_secs(20);
        while Instant::now() < watch_until || runs() == before {
            assert!(Instant::now() < deadline, "no minor compaction of the merged survivor");
            let got = ps_get(&merged, SURVIVOR, KEY).await;
            assert_eq!(got.code, CODE_OK);
            assert_eq!(
                String::from_utf8_lossy(&got.value),
                "new",
                "an older SST shadows the newer value after a post-merge minor compaction \
                 (tables {:?})",
                table_seqs(&merged, SURVIVOR).await
            );
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(listed_ssts(mgr_addr, meta).await < 5, "the minor compactions changed nothing");
    });
}
