//! BUG-LEASE-2 (P0 #2, coco arch review 2026-06-05) Phase 1 —
//! end-to-end wire test for storage-layer fencing.
//!
//! Drives a real cluster (manager + 2 EN + 1 PS), submits three
//! Put RPCs:
//!
//! 1. `inode_hint=42, lease_epoch=1` → CODE_OK (seeds the floor).
//! 2. `inode_hint=42, lease_epoch=5` → CODE_OK (bumps the floor).
//! 3. `inode_hint=42, lease_epoch=1` → **CODE_FENCED** — this is
//!    the bug repro: a stale writer trying to land its RPC AFTER
//!    a newer-epoch writer has been observed. Pre-fix the write
//!    would have been accepted (mingling with the new writer's
//!    data); post-fix the PS rejects with the typed code.
//!
//! Also checks: writes with `inode_hint=0` (anonymous) bypass
//! fencing entirely — KV CLI and non-lease-aware paths must not
//! be affected by Phase 1's per-inode floor.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{
    self, rkyv_decode, rkyv_encode, PutReq, PutResp, CODE_FENCED, CODE_OK, MSG_PUT,
};

use support::*;

async fn put_with_fence(
    ps: &RpcClient,
    part_id: u64,
    key: &[u8],
    inode_hint: u64,
    lease_epoch: u64,
) -> PutResp {
    let payload = rkyv_encode(&PutReq {
        part_id,
        key: key.to_vec(),
        value: b"x".to_vec(),
        expires_at: 0,
        region_epoch: 0,
        inode_hint,
        lease_epoch,
    });
    let bytes = ps.call(MSG_PUT, payload).await.expect("MSG_PUT");
    rkyv_decode::<PutResp>(&bytes).expect("decode PutResp")
}

#[test]
#[ignore] // requires full cluster
fn p0_2_storage_fencing_phase1() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 159).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        let part_id = 15901u64;
        upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(159, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        let ps = RpcClient::connect(ps_addr).await.expect("connect PS");

        // (1) writer A, epoch 1 → seeds the floor.
        let r1 = put_with_fence(&ps, part_id, b"k1", 42, 1).await;
        assert_eq!(r1.code, CODE_OK, "first write must seed floor: {r1:?}");

        // (2) writer B, epoch 5 → bumps the floor to 5.
        let r2 = put_with_fence(&ps, part_id, b"k2", 42, 5).await;
        assert_eq!(r2.code, CODE_OK, "higher-epoch write must bump floor: {r2:?}");

        // (3) writer A, epoch 1 (stale; after force-revoke) →
        // MUST be fenced. This is the BUG-LEASE-2 contract.
        let r3 = put_with_fence(&ps, part_id, b"k3", 42, 1).await;
        assert_eq!(
            r3.code, CODE_FENCED,
            "BUG-LEASE-2: stale-epoch write MUST be fenced: {r3:?}"
        );
        assert!(
            r3.message.contains("fenced"),
            "fence message should be diagnostic: {}",
            r3.message
        );

        // (4) anonymous writes (inode_hint=0) bypass fencing
        // entirely — KV CLI / non-lease-aware paths must keep
        // working even with insanely high or low epochs.
        let r4 = put_with_fence(&ps, part_id, b"k4", 0, 0).await;
        assert_eq!(r4.code, CODE_OK, "inode_hint=0 must bypass fencing: {r4:?}");
        let r5 = put_with_fence(&ps, part_id, b"k5", 0, 999_999).await;
        assert_eq!(r5.code, CODE_OK, "inode_hint=0 bypass holds at any epoch");

        // (5) a different ino is independent — high floor on ino 42
        // must NOT fence writes for ino 99.
        let r6 = put_with_fence(&ps, part_id, b"k6", 99, 1).await;
        assert_eq!(
            r6.code, CODE_OK,
            "fence floor is per-ino; ino 99 must seed its own: {r6:?}"
        );

        // (6) writer C, ino 42, epoch 5 (equal to floor) → accepted.
        let r7 = put_with_fence(&ps, part_id, b"k7", 42, 5).await;
        assert_eq!(r7.code, CODE_OK, "equal-epoch must pass: {r7:?}");
    });
}
