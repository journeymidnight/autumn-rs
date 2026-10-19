//! coco R2-P0 #1 (2026-06-06) — heartbeat etcd refresh must NOT
//! resurrect a released record.
//!
//! A writer heartbeat renews in memory, then refreshes its etcd record.
//! Between the two, a concurrent release can delete the record; a blind put
//! would resurrect it, so the refresh goes through `read_then_cas_put`.
//! The race is driven by deleting the record behind the manager after
//! acquire: the in-memory heartbeat still renews, so the refresh path runs.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, AcquireLeaseReq, AcquireLeaseResp, HeartbeatLeaseReq,
    HeartbeatLeaseResp, MgrClientId, CODE_OK, LEASE_CLIENT_KIND_FUSE, LEASE_MODE_WRITE,
    MSG_ACQUIRE_LEASE, MSG_HEARTBEAT_LEASE,
};
use support::{pick_addr, start_etcd};

fn start_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint], support::manager_identity())
                .await
                .expect("new manager with etcd");
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(400));
}

async fn acquire_writer(mgr: &RpcClient, writer: &MgrClientId, ino: u64) {
    let resp = mgr
        .call(
            MSG_ACQUIRE_LEASE,
            rkyv_encode(&AcquireLeaseReq {
                client: writer.clone(),
                ino,
                mode: LEASE_MODE_WRITE,
                force: false,
            }),
        )
        .await
        .unwrap();
    let acq: AcquireLeaseResp = rkyv_decode(&resp).unwrap();
    assert_eq!(acq.code, CODE_OK);
}

async fn heartbeat(mgr: &RpcClient, writer: &MgrClientId, ino: u64) {
    let resp = mgr
        .call(
            MSG_HEARTBEAT_LEASE,
            rkyv_encode(&HeartbeatLeaseReq {
                client: writer.clone(),
                ino,
            }),
        )
        .await
        .unwrap();
    let hb: HeartbeatLeaseResp = rkyv_decode(&resp).unwrap();
    // Renewed in memory: the etcd refresh path runs.
    assert_eq!(hb.code, CODE_OK, "{hb:?}");
}

#[test]
fn heartbeat_refresh_never_resurrects_a_deleted_record() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;
        let mgr_addr = pick_addr();
        start_etcd_manager(mgr_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_secs(2)).await;

        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");
        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint).await.unwrap();
        let writer = MgrClientId {
            kind: LEASE_CLIENT_KIND_FUSE,
            uuid: [0xc1; 16],
            host: "writer".to_string(),
        };
        let ino = 5555u64;
        acquire_writer(&mgr, &writer, ino).await;
        let key = format!("inode_leases/{ino}");
        assert!(!aux.get(key.as_bytes()).await.unwrap().kvs.is_empty());
        aux.delete(key.as_bytes()).await.unwrap();
        heartbeat(&mgr, &writer, ino).await;
        assert!(
            aux.get(key.as_bytes()).await.unwrap().kvs.is_empty(),
            "heartbeat resurrected a deleted lease record"
        );
    });
}
