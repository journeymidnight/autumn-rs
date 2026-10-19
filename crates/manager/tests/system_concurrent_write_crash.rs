#![allow(clippy::redundant_pattern_matching)] // integration-test file
//! Data integrity: concurrent writers + crash/split — no acknowledged writes lost.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn concurrent_writers_crash_no_data_loss() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 85).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(97, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // 5 concurrent writers, each writing 20 keys
        let mut handles = Vec::new();
        for w in 0u32..5 {
            let ps_clone = RpcClient::connect(ps1_addr).await.expect("connect writer");
            handles.push(compio::runtime::spawn(async move {
                let mut keys = Vec::new();
                for i in 0u32..20 {
                    let key = format!("w{w}-key-{i:02}");
                    let val = format!("w{w}-val-{i:02}");
                    ps_put(&ps_clone, 901, key.as_bytes(), val.as_bytes()).await;
                    keys.push(key);
                }
                keys
            }));
        }

        // Collect all acknowledged keys
        let mut all_keys: Vec<String> = Vec::new();
        for h in handles {
            let keys = h.await.expect("writer task panicked");
            all_keys.extend(keys);
        }
        assert_eq!(all_keys.len(), 100);

        // Crash without flushing
        drop(ps1);

        // PS2 recovers from logStream
        let ps2_addr = pick_addr();
        start_partition_server(97, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Every acknowledged key must be readable
        for key in &all_keys {
            let expected = key.replace("-key-", "-val-");
            let resp = ps_get(&ps2, 901, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                expected.as_bytes(),
                "{key} missing or wrong after recovery"
            );
        }
    });
}

/// Puts racing a split: every put the PS acknowledged is readable from the
/// child that owns its key. Split freezes the source while it captures the
/// tails, so a put is either refused or lands before the capture.
#[test]
fn concurrent_writers_during_split() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 86).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(98, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Pre-populate 50 keys, flush
        for i in 0u32..50 {
            ps_put(
                &ps,
                902,
                format!("pre-{i:02}").as_bytes(),
                format!("pv-{i:02}").as_bytes(),
            )
            .await;
        }
        ps_flush(&ps, 902).await;

        // Start split and concurrent writes
        let split_ps = RpcClient::connect(ps_addr).await.expect("connect split");
        let write_ps = RpcClient::connect(ps_addr).await.expect("connect writer");

        // The writer runs until the split has answered. Only puts that land
        // before the freeze can be acked (during it they are refused, after
        // it they are out of the left's range), so the split starts once one
        // has been.
        let split_done = std::rc::Rc::new(std::cell::Cell::new(false));
        let writer_stop = split_done.clone();
        let acked = std::rc::Rc::new(std::cell::Cell::new(0usize));
        let writer_acked = acked.clone();
        let write_handle = compio::runtime::spawn(async move {
            let mut written = Vec::new();
            for i in 0u32.. {
                if writer_stop.get() {
                    break;
                }
                // Above every `pre-*` key, so these land in the right child:
                // the one that replays the copied log only up to the
                // captured length.
                let key = format!("q-concurrent-{i:05}");
                let val = format!("cv-{i:05}");
                // A refused put (frozen, or no longer in range) is not acked.
                if let Ok(bytes) = write_ps
                    .call(
                        partition_rpc::MSG_PUT,
                        partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                            part_id: 902,
                            key: key.as_bytes().to_vec(),
                            value: val.as_bytes().to_vec(),

                            expires_at: 0,
                            region_epoch: 0,
                        inode_hint: 0,
                        lease_epoch: 0,
                        }),
                    )
                    .await
                {
                    let resp: partition_rpc::PutResp =
                        partition_rpc::rkyv_decode(&bytes).expect("decode put");
                    if resp.code == partition_rpc::CODE_OK {
                        written.push((key, val));
                        writer_acked.set(written.len());
                    }
                }
            }
            written
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while acked.get() == 0 {
            assert!(std::time::Instant::now() < deadline, "no put acked before the split");
            compio::time::sleep(Duration::from_millis(5)).await;
        }
        let split_handle = compio::runtime::spawn(async move {
            split_ps
                .call(
                    partition_rpc::MSG_SPLIT_PART,
                    partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 902, at_key: None, op_id: 0 }),
                )
                .await
                .expect("split")
        });

        let split_resp_bytes = split_handle.await.expect("split task panicked");
        split_done.set(true);
        let sr: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&split_resp_bytes).expect("decode");
        assert_eq!(
            sr.code,
            partition_rpc::CODE_OK,
            "split failed: {}",
            sr.message
        );

        let concurrent_keys = write_handle.await.expect("writer task panicked");
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 2);
        let left_rg = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 902)
            .unwrap()
            .1
            .clone();
        let right_id = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id != 902)
            .unwrap()
            .1
            .part_id;
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();
        compio::time::sleep(Duration::from_millis(6000)).await;

        // All pre-split keys must be in the correct child
        for i in 0u32..50 {
            let key = format!("pre-{i:02}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() {
                902
            } else {
                right_id
            };
            let resp = psr_get(&router, part_id, kb).await;
            assert_eq!(resp.value, format!("pv-{i:02}").as_bytes(), "{key} wrong");
        }

        eprintln!("acked {} puts around the split", concurrent_keys.len());
        for (key, val) in &concurrent_keys {
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 902 } else { right_id };
            let resp = psr_get(&router, part_id, kb).await;
            assert_eq!(resp.value, val.as_bytes(), "acked {key} lost across the split");
        }

        // Compact both, re-verify pre-split keys
        ps_compact(&ps, 902).await;
        psr_compact(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        for i in 0u32..50 {
            let key = format!("pre-{i:02}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() {
                902
            } else {
                right_id
            };
            let resp = psr_get(&router, part_id, kb).await;
            assert_eq!(
                resp.value,
                format!("pv-{i:02}").as_bytes(),
                "{key} wrong after compact"
            );
        }
    });
}
