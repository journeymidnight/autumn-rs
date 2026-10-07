//! A lease fence floor raised by a WAL-only record survives a split and a
//! restart on both children.
//!
//! A compare-write whose comparison fails but whose lease epoch is newer
//! commits a floor bump and nothing else: no memtable entry, so no flush will
//! ever carry it. Recovery used to find it by replaying the WAL from the last
//! checkpoint. A split child now starts replay at the split's freeze
//! checkpoint — written at the committed log end even when the drain flushed
//! nothing — so the floor must be IN that checkpoint (`fence_floors`), or the
//! revoked writer's older epoch is admitted again.

mod support;

use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{
    self, rkyv_decode, rkyv_encode, CompareWriteReq, PutReq, PutResp, SplitPartReq,
    SplitPartResp, CODE_FENCED, CODE_OK, MSG_COMPARE_WRITE, MSG_PUT, MSG_SPLIT_PART,
};
use support::*;

const PART: u64 = 1501;
const INO: u64 = 77;

async fn try_put_fenced(c: &RpcClient, part: u64, key: &[u8], epoch: u64) -> Option<u8> {
    let bytes = c
        .call(
            MSG_PUT,
            rkyv_encode(&PutReq {
                part_id: part,
                key: key.to_vec(),
                value: b"v".to_vec(),
                expires_at: 0,
                region_epoch: 0,
                inode_hint: INO,
                lease_epoch: epoch,
            }),
        )
        .await
        .ok()?;
    Some(rkyv_decode::<PutResp>(&bytes).expect("decode PutResp").code)
}

async fn put_fenced(c: &RpcClient, part: u64, key: &[u8], epoch: u64) -> u8 {
    try_put_fenced(c, part, key, epoch).await.expect("put")
}

#[test]
fn a_wal_only_fence_bump_survives_a_split_and_a_restart() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        register_node(&mgr, &en_addr.to_string(), "uuid-split-fence").await;
        let (l, r, m) = (create_stream(&mgr, 1).await, create_stream(&mgr, 1).await, create_stream(&mgr, 1).await);
        upsert_partition(&mgr, PART, l, r, m, b"a", b"z").await;
    });

    let ps_addr = pick_addr();
    let (stop, join) = start_partition_server_stoppable(95, mgr_addr, ps_addr);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps_addr);
        let c = router.client_for(PART).await;
        // Floor 1 goes into a checkpoint with the data.
        assert_eq!(put_fenced(&c, PART, b"b-key", 1).await, CODE_OK);
        assert_eq!(put_fenced(&c, PART, b"q-key", 1).await, CODE_OK);
        ps_flush(&c, PART).await;

        // Epoch 5 with a failing comparison: the bump alone commits.
        let bytes = c
            .call(
                MSG_COMPARE_WRITE,
                rkyv_encode(&CompareWriteReq {
                    part_id: PART,
                    region_epoch: 0,
                    key: b"b-key".to_vec(),
                    expected: Some(b"not-the-value".to_vec()),
                    value: Some(b"w".to_vec()),
                    inode_hint: INO,
                    lease_epoch: 5,
                }),
            )
            .await
            .expect("compare write");
        let _cmp: PutResp = rkyv_decode(&bytes).expect("decode compare-write reply");
        assert_eq!(
            put_fenced(&c, PART, b"b-key", 3).await,
            CODE_FENCED,
            "the failed compare-write must still have raised the floor"
        );

        // Split with nothing in the memtable to flush.
        let bytes = c
            .call(
                MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&SplitPartReq { part_id: PART, at_key: Some(b"m".to_vec()), op_id: 0 }),
            )
            .await
            .expect("split");
        let sr: SplitPartResp = rkyv_decode(&bytes).expect("decode split");
        assert_eq!(sr.code, CODE_OK, "split: {}", sr.message);
    });
    // Let the children settle, then stop the PS (graceful: its drain has
    // nothing to flush, so it writes no newer checkpoint) and restart it, so
    // both children recover from their checkpoints.
    std::thread::sleep(Duration::from_secs(2));
    let right = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        get_regions(&mgr)
            .await
            .regions
            .iter()
            .map(|(id, _)| *id)
            .find(|id| *id != PART)
            .expect("right child")
    });
    stop.shutdown();
    join.join().expect("join PS");

    let ps2_addr = pick_addr();
    start_partition_server(95, mgr_addr, ps2_addr);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps2_addr);
        for (part, key) in [(PART, &b"b-key"[..]), (right, &b"q-key"[..])] {
            let deadline = Instant::now() + Duration::from_secs(60);
            let code = loop {
                if let Ok(c) = router.try_client_for(part).await {
                    if let Some(code) = try_put_fenced(&c, part, key, 3).await {
                        if code != partition_rpc::CODE_NOT_FOUND {
                            break code;
                        }
                    }
                }
                assert!(Instant::now() < deadline, "part {part} never served");
                compio::time::sleep(Duration::from_millis(200)).await;
            };
            assert_eq!(code, CODE_FENCED, "part {part}: the revoked epoch 3 is admitted again");
        }
    });
}
