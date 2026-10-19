//! coco R2-P0 #1 (2026-06-06) — heartbeat etcd refresh must NOT
//! resurrect a released record.
//!
//! A writer heartbeat renews in memory, then refreshes its etcd record.
//! Between the two, a concurrent release can delete the record; a blind put
//! would resurrect it, so the refresh goes through `refresh_writer_record`.
//! The race is driven by deleting the record behind the manager after
//! acquire: the in-memory heartbeat still renews, so the refresh path runs.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use std::sync::atomic::Ordering;

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_manager::{AutumnManager, HEARTBEAT_REFRESH_TEST_HOLD};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, AcquireLeaseReq, AcquireLeaseResp, HeartbeatLeaseReq,
    HeartbeatLeaseResp, MgrClientId, MgrInodeLeaseRecord, CODE_OK, LEASE_CLIENT_KIND_FUSE,
    LEASE_MODE_WRITE, MSG_ACQUIRE_LEASE, MSG_HEARTBEAT_LEASE,
};
use support::{pick_addr, start_etcd};

/// The heartbeat failpoint is process-wide: any writer heartbeat may take it.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Serializes the tests and disarms what a failed one may have left armed.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    let g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    HEARTBEAT_REFRESH_TEST_HOLD.store(0, Ordering::Relaxed);
    g
}

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
    let _serial = serial();
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

/// A force-preempt that is granted while the old writer's heartbeat sits
/// between its in-memory renewal and its etcd read: the refresh must leave
/// the new writer's record alone.
#[test]
fn heartbeat_refresh_never_overwrites_the_next_writers_record() {
    let _serial = serial();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;
        let mgr_addr = pick_addr();
        start_etcd_manager(mgr_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_secs(2)).await;

        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint).await.unwrap();
        let old = MgrClientId {
            kind: LEASE_CLIENT_KIND_FUSE,
            uuid: [0xa1; 16],
            host: "old".to_string(),
        };
        let new = MgrClientId {
            kind: LEASE_CLIENT_KIND_FUSE,
            uuid: [0xb2; 16],
            host: "new".to_string(),
        };
        let ino = 5600u64;
        let key = format!("inode_leases/{ino}");
        let stored_writer = |value: &[u8]| -> [u8; 16] {
            rkyv_decode::<MgrInodeLeaseRecord>(value).unwrap().writer.uuid
        };

        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");
        acquire_writer(&mgr, &old, ino).await;

        HEARTBEAT_REFRESH_TEST_HOLD.store(1, Ordering::Relaxed);
        let hb_mgr = RpcClient::connect(mgr_addr).await.expect("connect");
        let hb_client = old.clone();
        let hb = compio::runtime::spawn(async move { heartbeat(&hb_mgr, &hb_client, ino).await });
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while HEARTBEAT_REFRESH_TEST_HOLD.load(Ordering::Relaxed) != 2 {
            assert!(std::time::Instant::now() < deadline, "heartbeat never reached the hold");
            compio::time::sleep(Duration::from_millis(5)).await;
        }

        let cluster = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let granted = lease::acquire_with_preempt_wait(
            &cluster,
            &DaemonClientId::from_wire(new.clone()),
            ino,
            LEASE_MODE_WRITE,
            8_000,
        )
        .await
        .unwrap();
        assert!(matches!(granted, AcquireResult::Granted(_)), "{granted:?}");
        assert_eq!(stored_writer(&aux.get(key.as_bytes()).await.unwrap().kvs[0].value), new.uuid);

        HEARTBEAT_REFRESH_TEST_HOLD.store(0, Ordering::Relaxed);
        hb.await.expect("heartbeat task");
        assert_eq!(
            stored_writer(&aux.get(key.as_bytes()).await.unwrap().kvs[0].value),
            new.uuid,
            "the old writer's heartbeat overwrote the new writer's record"
        );
    });
}
