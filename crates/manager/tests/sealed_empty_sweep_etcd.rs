//! The sealed-empty backstop, against a real etcd.
//!
//! The in-process manager has no etcd, and `mirror_stream_extent_mutation` is a
//! no-op without one — so the unit tests next to the sweep cover its selection
//! and its in-memory apply, and nothing else. This covers what they cannot: that
//! the membership drop and the ref drop actually reach ETCD, so the reclaim
//! survives the manager.
//!
//! What it does NOT prove: that the value-CAS baselines are compared. With no
//! concurrent writer a blind put would pass identically, and nothing here drives
//! a losing CAS.
//!
//! The scenario is the one the ledger asks for — a writer that died between the
//! seal and the punch. The client-side reclaim (`reclaim_abandoned_empty_tail`)
//! punches its own abandoned tail on roll-away, best effort; here the writer
//! simply goes away after rolling, which is what leaves the extent stranded at
//! `sealed = true, sealed_length = 0` with nothing left to reclaim it.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

#[test]
#[ignore] // requires a real etcd binary on PATH
fn the_sweep_reclaims_a_tail_the_dead_writer_never_punched() {
    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        // Hold the manager in THIS runtime so the sweep can be driven a tick at
        // a time; serving runs beside it.
        let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
            .await
            .expect("manager with etcd");
        let mgr_addr = pick_addr();
        let serving = manager.clone();
        compio::runtime::spawn(async move {
            let _ = serving.serve(mgr_addr).await;
        })
        .detach();
        compio::time::sleep(Duration::from_millis(600)).await;

        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 51).await;
        let stream_id = create_stream(&mgr, 2).await;

        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/sealed-empty-etcd/0".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        // Real bytes, so the first extent is sealed at a real length and must
        // NOT be swept — the sweep has to distinguish it from the empty one.
        let payload = vec![7u8; 4096];
        let appended = client.append(stream_id, &payload).await.expect("append");
        let extent_with_bytes = appended.extent_id;

        // Roll once: the extent holding the bytes is sealed at its real length,
        // and a fresh EMPTY tail appears.
        let seal1 = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: client.owner_key().to_string(),
                    owner_epoch: client.owner_epoch(),
                    seal_commit: Some(appended.end),
                    exclude_node_ids: vec![],
                    seal_extent_id: 0,
                }),
            )
            .await
            .unwrap();
        let seal1: StreamAllocExtentResp = rkyv_decode(&seal1).unwrap();
        assert_eq!(seal1.code, CODE_OK, "first roll: {}", seal1.message);
        let empty_tail = seal1
            .stream_info
            .as_ref()
            .and_then(|s| s.extent_ids.last().copied())
            .expect("rolled stream has a tail");

        // Roll again at commit 0: the empty tail is sealed AT ZERO and stops
        // being the tail. This is the shape a writer strands when it dies before
        // punching — from here nothing in the system reclaims it.
        let seal2 = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: client.owner_key().to_string(),
                    owner_epoch: client.owner_epoch(),
                    seal_commit: Some(0),
                    exclude_node_ids: vec![],
                    seal_extent_id: 0,
                }),
            )
            .await
            .unwrap();
        let seal2: StreamAllocExtentResp = rkyv_decode(&seal2).unwrap();
        assert_eq!(seal2.code, CODE_OK, "second roll: {}", seal2.message);

        // The writer dies here — no punch, no reclaim.
        drop(client);

        // Precondition: exactly the leaked shape, straight from the manager.
        let info = mgr
            .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id: empty_tail }))
            .await
            .expect("extent_info");
        let info: ExtentInfoResp = rkyv_decode(&info).expect("decode");
        let ex = info.extent.expect("the stranded extent still exists");
        assert!(ex.sealed && ex.sealed_length == 0, "precondition: sealed at zero");

        // Pin the PRE-sweep etcd state. Without this the post-sweep "it is gone"
        // assertions pass trivially if alloc-time mirroring ever stops
        // persisting — the test would go quietly vacuous instead of red.
        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("aux etcd client");
        let pre = aux
            .get(format!("extents/{empty_tail}").as_bytes())
            .await
            .expect("etcd get before");
        assert!(
            !pre.kvs.is_empty(),
            "precondition: etcd must hold extents/{empty_tail} before the sweep"
        );

        let before: Vec<u64> = stream_extent_ids(&mgr, stream_id).await;
        assert!(before.contains(&empty_tail), "it is still a member: {before:?}");
        assert!(before.contains(&extent_with_bytes));

        // One tick.
        let reclaimed = manager.sealed_empty_sweep_once().await;
        assert_eq!(reclaimed, 1, "the stranded tail must be reclaimed");

        // Membership and refs both persisted, and the sealed-with-bytes extent
        // and the live tail were left alone.
        let after: Vec<u64> = stream_extent_ids(&mgr, stream_id).await;
        assert!(!after.contains(&empty_tail), "swept from membership: {after:?}");
        assert!(after.contains(&extent_with_bytes), "the extent with bytes stays");
        assert_eq!(after.len(), before.len() - 1, "exactly one member left");

        let gone = mgr
            .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id: empty_tail }))
            .await
            .expect("extent_info after sweep");
        let gone: ExtentInfoResp = rkyv_decode(&gone).expect("decode");
        assert!(
            gone.extent.is_none(),
            "its last ref went with the membership, so the record must be gone"
        );

        // etcd is the authority — check it directly, not the manager's cache.
        // This is the half the in-memory tests cannot reach: without a real
        // mirror, `mirror_stream_extent_mutation` returns Ok having written
        // nothing, and every assertion above would still pass.
        let raw = aux
            .get(format!("extents/{empty_tail}").as_bytes())
            .await
            .expect("etcd get");
        assert!(
            raw.kvs.is_empty(),
            "etcd still holds extents/{empty_tail} — the mutation did not persist"
        );
        let raw_stream = aux
            .get(format!("streams/{stream_id}").as_bytes())
            .await
            .expect("etcd get stream");
        let kv = raw_stream.kvs.first().expect("stream record in etcd");
        let persisted: MgrStreamInfo = rkyv_decode(&bytes::Bytes::from(kv.value.clone()))
            .expect("decode the persisted stream");
        assert!(
            !persisted.extent_ids.contains(&empty_tail),
            "etcd's membership still lists the swept extent: {:?}",
            persisted.extent_ids
        );
    });
}

async fn stream_extent_ids(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
    let resp = mgr
        .call(
            MSG_STREAM_INFO,
            rkyv_encode(&StreamInfoReq {
                stream_ids: vec![stream_id],
            }),
        )
        .await
        .expect("stream_info");
    let resp: StreamInfoResp = rkyv_decode(&resp).expect("decode StreamInfoResp");
    resp.streams
        .into_iter()
        .find(|(id, _)| *id == stream_id)
        .expect("stream present")
        .1
        .extent_ids
}
