//! Count real TCP accepts while probing stale epochs after actual split/merge.
mod support;

use autumn_client::{AutumnError, ClusterClient};
use autumn_rpc::{client::RpcClient, partition_rpc::*};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use std::{cell::Cell, rc::Rc, time::Duration};
use support::*;

// Forward bytes unchanged to a real PS listener; only count TCP connections.
async fn counting_proxy(
    target: String,
) -> (String, Rc<Cell<usize>>, compio::runtime::JoinHandle<()>) {
    let listener = compio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accepts = Rc::new(Cell::new(0));
    let count = accepts.clone();
    let task = compio::runtime::spawn(async move {
        let mut tasks = Vec::new();
        loop {
            let (incoming, _) = listener.accept().await.unwrap();
            count.set(count.get() + 1);
            let outgoing = compio::net::TcpStream::connect(&target).await.unwrap();
            let (mut ir, mut iw) = incoming.into_split();
            let (mut or, mut ow) = outgoing.into_split();
            tasks.push(compio::runtime::spawn(async move {
                let up = async move {
                    loop {
                        let compio::BufResult(n, buf) = ir.read(vec![0; 8192]).await;
                        let Ok(n) = n else { return };
                        if n == 0 {
                            return;
                        }
                        if ow
                            .write_all(Bytes::copy_from_slice(&buf[..n]))
                            .await
                            .0
                            .is_err()
                        {
                            return;
                        }
                    }
                };
                let down = async move {
                    loop {
                        let compio::BufResult(n, buf) = or.read(vec![0; 8192]).await;
                        let Ok(n) = n else { return };
                        if n == 0 {
                            return;
                        }
                        if iw
                            .write_all(Bytes::copy_from_slice(&buf[..n]))
                            .await
                            .0
                            .is_err()
                        {
                            return;
                        }
                    }
                };
                futures::pin_mut!(up, down);
                let _ = futures::future::select(up, down).await;
            }));
        }
    });
    (addr, accepts, task)
}

async fn probe_stale_epoch(
    client: &ClusterClient,
    part: u64,
    stale_epoch: u64,
    phase: &str,
) -> usize {
    let target = client.resolve_part_id(part).await.unwrap();
    let (addr, accepts, _proxy) = counting_proxy(target).await;
    for _ in 0..6 {
        let stale = rkyv_encode(&HeadReq {
            part_id: part,
            key: b"c-key".to_vec(),
            region_epoch: stale_epoch,
        });
        let error = client.ps_call(&addr, MSG_HEAD, stale).await.unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<AutumnError>(),
                Some(AutumnError::PreconditionFailed(_))
            ),
            "{phase}: {error:?}"
        );
        let current = rkyv_encode(&HeadReq {
            part_id: part,
            key: b"c-key".to_vec(),
            region_epoch: client.lookup_epoch_for_part(part),
        });
        let bytes = client.ps_call(&addr, MSG_HEAD, current).await.unwrap();
        let head: HeadResp = rkyv_decode(&bytes).unwrap();
        assert_eq!(head.code, CODE_OK);
        assert!(head.found);
    }
    eprintln!(
        "{phase}: 6 stale-epoch refusals + 6 successful reads, TCP accepts={}",
        accepts.get()
    );
    accepts.get()
}

#[test]
fn split_merge_status_refusals_reuse_healthy_connections() {
    let (mgr_addr, n1, n2, _d1, _d2) = setup_two_node_infra(180);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        compio::time::timeout(Duration::from_secs(90), async {
            let mgr = RpcClient::connect(mgr_addr).await.unwrap();
            register_two_nodes(&mgr, n1, n2, 180).await;
            let (log, row, meta) = create_three_streams(&mgr).await;
            upsert_partition(&mgr, 1801, log, row, meta, b"a", b"z").await;
            let ps_addr = pick_addr();
            start_partition_server(180, mgr_addr, ps_addr);
            let admin = ClusterClient::connect_raw(&mgr_addr.to_string())
                .await
                .unwrap();
            admin.put(b"c-key", b"left").await.unwrap();
            admin.put(b"r-key", b"right").await.unwrap();
            admin.flush(1801).await.unwrap();
            let observer = ClusterClient::connect_raw(&mgr_addr.to_string())
                .await
                .unwrap();
            let old_epoch = observer.lookup_epoch_for_part(1801);
            assert!(old_epoch > 0);
            admin.split_at(1801, Some(b"m".to_vec())).await.unwrap();
            assert!(
                poll_until_async(
                    Duration::from_secs(15),
                    Duration::from_millis(100),
                    || async {
                        let r = get_regions(&mgr).await;
                        r.regions.len() == 2 && r.part_addrs.len() == 2
                    }
                )
                .await
            );
            // Cached pre-split epoch must refresh and retry transparently.
            observer.put(b"c-key", b"left-after-split").await.unwrap();
            assert_ne!(observer.lookup_epoch_for_part(1801), old_epoch);
            let split_accepts = probe_stale_epoch(&observer, 1801, old_epoch, "after split").await;
            admin.refresh_regions().await.unwrap();
            let right = admin
                .all_partitions()
                .await
                .unwrap()
                .into_iter()
                .find(|(id, _)| *id != 1801)
                .unwrap()
                .0;
            admin.compact(1801).await.unwrap();
            admin.compact(right).await.unwrap();
            let pre_merge_epoch = observer.lookup_epoch_for_part(1801);
            admin.merge_partitions(1801, right, true).await.unwrap();
            // Merge returns before the PS unfreezes. Plain put currently exposes
            // that body-level refusal; wait for readiness without changing its
            // retry policy as part of connection-lifetime verification.
            assert!(
                poll_until_async(
                    Duration::from_secs(15),
                    Duration::from_millis(100),
                    || async {
                        match observer.put(b"c-key", b"left-after-merge").await {
                            Ok(()) => true,
                            Err(AutumnError::ServerError(m))
                                if m.contains("partition frozen for merge") =>
                            {
                                false
                            }
                            Err(e) => panic!("unexpected post-merge write error: {e}"),
                        }
                    }
                )
                .await
            );
            assert_ne!(observer.lookup_epoch_for_part(1801), pre_merge_epoch);
            let merge_accepts =
                probe_stale_epoch(&observer, 1801, pre_merge_epoch, "after merge").await;
            assert_eq!(
                observer.get(b"c-key").await.unwrap().unwrap(),
                b"left-after-merge"
            );
            assert_eq!(observer.get(b"r-key").await.unwrap().unwrap(), b"right");
            assert_eq!(
                (split_accepts, merge_accepts),
                (1, 1),
                "status errors must not add TCP handshakes"
            );
        })
        .await
        .expect("split/merge validation exceeded 90 seconds");
    });
}
