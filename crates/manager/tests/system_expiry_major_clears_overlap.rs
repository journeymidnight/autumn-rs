//! System test — the EXPIRY-major pass must clear `has_overlap`, like every
//! other arm that runs a successful major compaction.
//!
//! A CoW split's children inherit the parent's SSTs and open with
//! `has_overlap = 1`; `handle_split_part` refuses to split again until a MAJOR
//! compaction rewrites those tables without their out-of-range keys. Only the
//! dispatched-compact arm cleared the flag. The periodic tick runs a major
//! compaction of its own when an SST holds expired keys, and that arm did not —
//! so a child whose expiry pass ran could never be split again, while the
//! manager's advisory re-issued a compaction that had nothing left to do, every
//! window, until the partition server reopened the partition.
//!
//! The observable here is the split verdict, the same one
//! `system_split_overlap` uses: after the expiry pass, a further split must be
//! accepted. Nothing dispatches a compaction in this test — if the flag is
//! cleared, only the expiry arm can have done it.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;

use support::*;

/// The split verdict as one string: `"ok"`, else the refusal text.
///
/// `has_overlap` is not directly observable from a test, but its refusal is:
/// `handle_split_part` answers "cannot split: partition has overlapping keys"
/// while the flag is set. Asserting on the REASON matters — once the expiry
/// pass has dropped every expired key the child can legitimately fail to split
/// for an unrelated reason (nothing left to pick a mid-key from), and a test
/// that only looked at success/failure would read that as the flag still set.
async fn split_verdict(ps: &RpcClient, part_id: u64) -> String {
    match ps
        .call(
            partition_rpc::MSG_SPLIT_PART,
            partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id, at_key: None }),
        )
        .await
    {
        Ok(resp) => {
            let r: partition_rpc::SplitPartResp =
                partition_rpc::rkyv_decode(&resp).expect("decode SplitPartResp");
            if r.code == partition_rpc::CODE_OK { "ok".to_string() } else { r.message }
        }
        Err(e) => format!("{e}"),
    }
}

#[test]
fn expiry_major_clears_overlap_so_the_child_can_split_again() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 67).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Every record carries a SHORT ttl: the expiry pass keys on an SST's
        // `min_expires_at`, so without one the periodic tick never compacts.
        for i in 0u8..10 {
            ps_put_ttl(&ps, 901, format!("k-{i:02}").as_bytes(), b"v", 3).await;
        }
        ps_flush(&ps, 901).await;

        // The CoW split leaves the left child holding the parent's tables.
        let first = split_verdict(&ps, 901).await;
        assert_eq!(first, "ok", "first split must succeed: {first}");
        compio::time::sleep(Duration::from_millis(1500)).await;

        // Precondition: the child overlaps, so a second split is refused FOR
        // THAT REASON. If this ever stops holding the test proves nothing.
        let blocked = split_verdict(&ps, 901).await;
        assert!(
            blocked.contains("overlap"),
            "a CoW child must refuse a second split because it overlaps, got: {blocked}"
        );

        // Let the TTL lapse and the periodic tick pick it up. NOTHING dispatches
        // a compaction here — the timer's expiry-major arm is the only thing
        // that can clear the flag, which is the whole point of this test. The
        // tick is jittered 5-7 s, so allow several.
        let mut last = String::new();
        let mut cleared = false;
        for _ in 0..12 {
            compio::time::sleep(Duration::from_millis(3000)).await;
            last = split_verdict(&ps, 901).await;
            if !last.contains("overlap") {
                cleared = true;
                break;
            }
        }
        assert!(
            cleared,
            "the expiry-major pass must clear has_overlap; the child still \
             refuses to split for overlap after the TTL lapsed: {last}"
        );
    });
}
