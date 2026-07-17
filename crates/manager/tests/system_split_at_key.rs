//! F-SPLIT-AT-KEY (design doc D4): operator/controller-specified split point.
//!
//! Covers the four cases the feature adds on top of the legacy median path
//! (which stays exercised by `system_range_split` / `system_multi_split_chain`):
//!   1. explicit `--at` interval validation — inside succeeds; == start,
//!      == end, above end, and below start all reject with InvalidArgument;
//!   2. empty partition + `--at` succeeds (cuts into two empty children — the
//!      D8 presplit primitive);
//!   3. empty partition WITHOUT `--at` still rejects (`< 2 keys`);
//!   4. app-agnostic: a binary split key (not any known namespace) works.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;

use support::*;

/// Issue a SPLIT_PART with an optional explicit split point; return the raw
/// call result so callers can assert Ok(code) or Err (rejected).
async fn split_at(
    ps: &RpcClient,
    part_id: u64,
    at_key: Option<Vec<u8>>,
) -> Result<bytes::Bytes, autumn_rpc::error::RpcError> {
    ps.call(
        partition_rpc::MSG_SPLIT_PART,
        partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id, at_key }),
    )
    .await
}

fn assert_split_ok(resp: &bytes::Bytes, ctx: &str) {
    let sr: partition_rpc::SplitPartResp =
        partition_rpc::rkyv_decode(resp).expect("decode SplitPartResp");
    assert_eq!(sr.code, partition_rpc::CODE_OK, "{ctx}: {}", sr.message);
}

#[test]
fn split_at_explicit_point_validation_and_success() {
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
        // Partition covers [a, z).
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(61, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Populate with in-range keys so the (rejected) attempts can't be
        // masked by the `< 2 keys` gate — these must fail on the interval
        // check alone.
        for i in 0u32..20 {
            ps_put(&ps, 901, format!("m{i:02}").as_bytes(), b"v").await;
        }
        ps_flush(&ps, 901).await;

        // --at == start_key ("a") → reject (empty left child).
        assert!(
            split_at(&ps, 901, Some(b"a".to_vec())).await.is_err(),
            "split --at == start_key must reject"
        );
        // --at below start ("\x20" < 'a') → reject (out of range).
        assert!(
            split_at(&ps, 901, Some(vec![0x20])).await.is_err(),
            "split --at below start_key must reject"
        );
        // --at == end_key ("z") → reject (empty right child).
        assert!(
            split_at(&ps, 901, Some(b"z".to_vec())).await.is_err(),
            "split --at == end_key must reject"
        );
        // --at above end ("zz" > 'z') → reject (out of range).
        assert!(
            split_at(&ps, 901, Some(b"zz".to_vec())).await.is_err(),
            "split --at above end_key must reject"
        );

        // A rejected split must NOT have mutated the region map.
        assert_eq!(
            get_regions(&mgr).await.regions.len(),
            1,
            "rejected splits must leave a single partition"
        );

        // --at strictly inside ("m10") → success.
        let resp = split_at(&ps, 901, Some(b"m10".to_vec()))
            .await
            .expect("explicit in-range split should succeed");
        assert_split_ok(&resp, "explicit in-range split");
        compio::time::sleep(Duration::from_millis(1500)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 2, "explicit split must yield 2 parts");
        // The new boundary must be exactly the requested key.
        assert!(
            regions
                .regions
                .iter()
                .any(|(_, r)| r.rg.as_ref().map(|g| g.end_key.as_slice()) == Some(b"m10".as_slice())
                    || r.rg.as_ref().map(|g| g.start_key.as_slice()) == Some(b"m10".as_slice())),
            "split boundary must be the explicit key m10"
        );
    });
}

#[test]
fn split_at_allows_empty_partition_but_median_path_rejects() {
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
        // Empty partition covering the whole keyspace [ , ).
        upsert_partition(&mgr, 902, log, row, meta, b"", b"").await;

        let ps_addr = pick_addr();
        start_partition_server(62, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // No writes at all. Median path (no --at) must reject: `< 2 keys`.
        assert!(
            split_at(&ps, 902, None).await.is_err(),
            "median split of an empty partition must reject (< 2 keys)"
        );
        assert_eq!(
            get_regions(&mgr).await.regions.len(),
            1,
            "rejected median split must leave the empty partition intact"
        );

        // Explicit --at on the empty partition succeeds — a binary split key
        // (app-agnostic: not any known namespace prefix). This is the D8
        // presplit primitive: cut an empty pair into two empty children.
        let at = vec![0x01, 0x80, 0x00, 0xffu8];
        let resp = split_at(&ps, 902, Some(at.clone()))
            .await
            .expect("explicit split of empty partition should succeed");
        assert_split_ok(&resp, "explicit empty-partition split");
        compio::time::sleep(Duration::from_millis(1500)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(
            regions.regions.len(),
            2,
            "explicit split of empty partition must yield 2 (empty) parts"
        );
        assert!(
            regions.regions.iter().any(|(_, r)| r
                .rg
                .as_ref()
                .map(|g| g.start_key.as_slice() == at.as_slice() || g.end_key.as_slice() == at.as_slice())
                .unwrap_or(false)),
            "empty-partition split boundary must be the explicit key"
        );
    });
}
