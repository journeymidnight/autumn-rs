//! Regression test: repeated split+compact on a gallery-like keyspace must
//! preserve large original objects in the rightmost child.

mod support;

use std::sync::Once;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

fn init_test_tracing() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init();
    });
}

fn rightmost_part_id(regions: &GetRegionsResp) -> u64 {
    regions
        .regions
        .iter()
        .find(|(_, info)| {
            info.rg
                .as_ref()
                .map(|rg| rg.end_key.is_empty())
                .unwrap_or(false)
        })
        .map(|(_, info)| info.part_id)
        .expect("rightmost partition")
}

#[test]
fn split_compact_preserves_gallery_originals_on_rightmost_child() {
    init_test_tracing();
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 131).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"", b"").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        let large_value = vec![b'V'; 8 * 1024];
        let thumb_value = vec![b'T'; 256];
        for i in 0..320u16 {
            let original = format!(
                "Patreon--leeesovely-October-2024-MissKON.com-{i:03}.jpg"
            );
            let thumb = format!(".thumb/320/{original}");
            ps_put(&ps, 901, thumb.as_bytes(), &thumb_value, false).await;
            if i >= 300 {
                ps_put(&ps, 901, original.as_bytes(), &large_value, false).await;
            }
        }
        ps_flush(&ps, 901).await;

        let target_original = b"Patreon--leeesovely-October-2024-MissKON.com-300.jpg";
        let target_thumb = b".thumb/320/Patreon--leeesovely-October-2024-MissKON.com-300.jpg";

        let before = ps_get(&ps, 901, target_original).await;
        assert_eq!(
            before.value.len(),
            large_value.len(),
            "target original must exist before any split"
        );

        let mut current_part = 901u64;
        for round in 0..3 {
            let client = if round == 0 {
                ps.clone()
            } else {
                router.client_for(current_part).await
            };
            let resp = client
                .call(
                    partition_rpc::MSG_SPLIT_PART,
                    partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                        part_id: current_part,
                    }),
                )
                .await
                .expect("split rightmost partition");
            let split_resp: partition_rpc::SplitPartResp =
                partition_rpc::rkyv_decode(&resp).expect("decode split response");
            assert_eq!(
                split_resp.code,
                partition_rpc::CODE_OK,
                "split round {round} failed: {}",
                split_resp.message
            );

            compio::time::sleep(Duration::from_millis(700)).await;
            let regions = get_regions(&mgr).await;
            current_part = rightmost_part_id(&regions);

            // F099-K: the new right child binds a dedicated per-partition
            // port asynchronously after region propagation. Wait long enough
            // for `part_addrs` to become visible before routed maintenance.
            compio::time::sleep(Duration::from_millis(6000)).await;
            psr_compact(&router, current_part).await;
            compio::time::sleep(Duration::from_millis(6000)).await;

            let original = psr_get(&router, current_part, target_original).await;
            assert_eq!(
                original.value.len(),
                large_value.len(),
                "round {round}: target original disappeared from rightmost partition {current_part}"
            );

            let thumb = psr_get(&router, current_part, target_thumb).await;
            assert_eq!(
                thumb.value.len(),
                thumb_value.len(),
                "round {round}: target thumbnail disappeared from rightmost partition {current_part}"
            );
        }
    });
}

#[test]
fn single_split_compact_preserves_gallery_originals_on_right_child() {
    init_test_tracing();
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 231).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"", b"").await;

        let ps_addr = pick_addr();
        start_partition_server(72, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        let large_value = vec![b'V'; 8 * 1024];
        let thumb_value = vec![b'T'; 256];
        for i in 1..=301u16 {
            let original = format!(
                "Patreon--leeesovely-October-2024-MissKON.com-{i:03}.jpg"
            );
            let thumb = format!(".thumb/320/{original}");
            ps_put(&ps, 902, thumb.as_bytes(), &thumb_value, false).await;
            if i == 3 || i >= 276 {
                ps_put(&ps, 902, original.as_bytes(), &large_value, false).await;
            }
        }
        ps_flush(&ps, 902).await;

        let target_original = b"Patreon--leeesovely-October-2024-MissKON.com-300.jpg";
        let before = ps_get(&ps, 902, target_original).await;
        assert_eq!(
            before.value.len(),
            large_value.len(),
            "target original must exist before split"
        );

        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 902 }),
            )
            .await
            .expect("split partition");
        let split_resp: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&resp).expect("decode split response");
        assert_eq!(
            split_resp.code,
            partition_rpc::CODE_OK,
            "single split failed: {}",
            split_resp.message
        );

        compio::time::sleep(Duration::from_millis(700)).await;
        let regions = get_regions(&mgr).await;
        let right_part = rightmost_part_id(&regions);
        assert_ne!(right_part, 902, "right child must be a new partition");

        compio::time::sleep(Duration::from_millis(6000)).await;
        psr_compact(&router, right_part).await;
        compio::time::sleep(Duration::from_millis(6000)).await;

        let after = psr_get(&router, right_part, target_original).await;
        assert_eq!(
            after.value.len(),
            large_value.len(),
            "single split+compact dropped target original from right child {right_part}"
        );
    });
}

#[test]
fn two_split_compact_preserves_gallery_originals_on_rightmost_descendant() {
    init_test_tracing();
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 331).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 903, log, row, meta, b"", b"").await;

        let ps_addr = pick_addr();
        start_partition_server(73, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        let large_value = vec![b'V'; 8 * 1024];
        let thumb_value = vec![b'T'; 256];
        for i in 1..=301u16 {
            let original = format!(
                "Patreon--leeesovely-October-2024-MissKON.com-{i:03}.jpg"
            );
            let thumb = format!(".thumb/320/{original}");
            ps_put(&ps, 903, thumb.as_bytes(), &thumb_value, false).await;
            if i == 3 || i >= 276 {
                ps_put(&ps, 903, original.as_bytes(), &large_value, false).await;
            }
        }
        ps_flush(&ps, 903).await;

        let target_original = b"Patreon--leeesovely-October-2024-MissKON.com-300.jpg";

        let mut current = 903u64;
        for round in 0..2 {
            let client = if round == 0 {
                ps.clone()
            } else {
                router.client_for(current).await
            };
            let split_req = partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: current });
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            let resp = loop {
                match client
                    .call(partition_rpc::MSG_SPLIT_PART, split_req.clone())
                    .await
                {
                    Ok(resp) => break resp,
                    Err(err) if err.to_string().contains("overlapping keys") => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "split round {round} stayed blocked on overlap for too long: {err}"
                        );
                        compio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(err) => panic!("split round {round} failed unexpectedly: {err}"),
                }
            };
            let split_resp: partition_rpc::SplitPartResp =
                partition_rpc::rkyv_decode(&resp).expect("decode split response");
            assert_eq!(
                split_resp.code,
                partition_rpc::CODE_OK,
                "split round {round} failed: {}",
                split_resp.message
            );

            compio::time::sleep(Duration::from_millis(700)).await;
            let regions = get_regions(&mgr).await;
            current = rightmost_part_id(&regions);

            compio::time::sleep(Duration::from_millis(6000)).await;
            psr_compact(&router, current).await;
            compio::time::sleep(Duration::from_millis(6000)).await;

            let after = psr_get(&router, current, target_original).await;
            assert_eq!(
                after.value.len(),
                large_value.len(),
                "round {round}: target original disappeared from rightmost descendant {current}"
            );
        }
    });
}