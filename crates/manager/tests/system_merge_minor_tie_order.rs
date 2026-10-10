//! A minor compaction after a merge must keep its output where its window was,
//! even when the output's `last_seq` ties a newer table's.
//!
//! The two merge sources counted sequence numbers independently, so a table of
//! one side can carry the same `last_seq` as a table of the other. An output
//! takes the largest input seq; when that comes from the other side and ties
//! the next table of its own keys' side, placing outputs by `last_seq` put it
//! AFTER that newer table, and a Get (list walked from the back, first hit
//! wins) returned the older copy. Here the merged list is `[S0, S1, S2, V0,
//! V1]`; S0 is 300 entries and V1 300 entries of 3 KiB, so the only window in
//! ratio is `[S1, S2, V0]`; its
//! output carries S2's seq, which is V1's, and V0 holds KEY's old value that
//! V1 overwrote.

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

const SURVIVOR: u64 = 1411;
const VICTIM: u64 = 1412;
const KEY: &[u8] = b"n-key";

/// SSTs the survivor's checkpoint lists (its last record).
async fn listed_ssts(mgr_addr: std::net::SocketAddr, meta_stream: u64) -> usize {
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        "merge-minor-tie-test".to_string(),
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

async fn put_flush(c: &RpcClient, part: u64, prefix: &str, n: usize) {
    for i in 0..n {
        ps_put(c, part, format!("{prefix}-{i:03}").as_bytes(), b"x").await;
    }
    ps_flush(c, part).await;
}

#[test]
fn a_tied_last_seq_does_not_move_a_minor_output_past_a_newer_table() {
    // Every window faces the ratio test, so the big tables stay out of it.
    autumn_partition_server::compact_policy::set_minor_policy(
        autumn_partition_server::compact_policy::MinorPolicy {
            min_size: 1,
            ..Default::default()
        },
    )
    .expect("policy");
    autumn_partition_server::background::set_minor_compaction_paused(true);
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        register_node(&mgr, &en_addr.to_string(), "uuid-merge-minor-tie").await;
        let (l, r, m) = (create_stream(&mgr, 1).await, create_stream(&mgr, 1).await, create_stream(&mgr, 1).await);
        upsert_partition(&mgr, SURVIVOR, l, r, m, b"", b"m").await;
        let (l, r, m) = (create_stream(&mgr, 1).await, create_stream(&mgr, 1).await, create_stream(&mgr, 1).await);
        upsert_partition(&mgr, VICTIM, l, r, m, b"m", b"").await;

        let ps_addr = pick_addr();
        start_partition_server(92, mgr_addr, ps_addr);
        let router = PsRouter::new(mgr_addr, ps_addr);
        let s = router.client_for(SURVIVOR).await;
        let v = router.client_for(VICTIM).await;

        put_flush(&s, SURVIVOR, "b-s0", 300).await;
        put_flush(&s, SURVIVOR, "b-s1", 1).await;
        put_flush(&s, SURVIVOR, "b-s2", 1).await;
        // V0 holds KEY's old value; V1 (big) ends with KEY's new value at S2's seq.
        ps_put(&v, VICTIM, KEY, b"old").await;
        ps_flush(&v, VICTIM).await;
        let v0 = table_seqs(&v, VICTIM).await[0];
        let s_seqs = table_seqs(&s, SURVIVOR).await;
        let s2 = s_seqs[2];
        // 3 KiB values (inline, under the 4 KiB value-pointer bound): V1 is
        // bigger in bytes than everything else together.
        let big = vec![b'v'; 3 * 1024];
        for i in 0..(s2 - v0 - 1) {
            ps_put(&v, VICTIM, format!("n-v1-{i:03}").as_bytes(), &big).await;
        }
        ps_put(&v, VICTIM, KEY, b"new").await;
        ps_flush(&v, VICTIM).await;
        assert_eq!(ps_get(&v, VICTIM, KEY).await.value, b"new");
        let v_seqs = table_seqs(&v, VICTIM).await;
        assert_eq!(v_seqs.len(), 2, "victim tables: {v_seqs:?}");
        assert_eq!(s_seqs.len(), 3, "survivor tables: {s_seqs:?}");
        let v1 = v_seqs[1];
        assert!(v1 == s2 && s_seqs[0] < v1, "seqs: victim {v_seqs:?} survivor {s_seqs:?}");

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
        // The tick finds the window; stop after that one minor (the three
        // tables it leaves are in ratio, and the next tick would merge them).
        let runs = || autumn_partition_server::background::minor_compaction_runs().1;
        let before = runs();
        autumn_partition_server::background::set_minor_compaction_paused(false);
        while runs() == before {
            assert!(Instant::now() < deadline, "no minor compaction of the merged survivor");
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        autumn_partition_server::background::set_minor_compaction_paused(true);
        assert_eq!(listed_ssts(mgr_addr, meta).await, 3, "one window of three merged");
        // The window was [S1, S2, V0]: S0 and V1 are still their own tables.
        let merged_seqs = table_seqs(&merged, SURVIVOR).await;
        assert!(
            merged_seqs.contains(&s_seqs[0])
                && merged_seqs.iter().filter(|&&q| q == v1).count() == 2
                && !merged_seqs.contains(&v0),
            "tables after the minor: {merged_seqs:?} (S {s_seqs:?}, V0 {v0}, V1 {v1})"
        );
        let got = ps_get(&merged, SURVIVOR, KEY).await;
        assert_eq!(got.code, CODE_OK);
        assert_eq!(
            String::from_utf8_lossy(&got.value),
            "new",
            "the minor output (V0's old copy) landed after V1 on a tied last_seq"
        );
    });
}
