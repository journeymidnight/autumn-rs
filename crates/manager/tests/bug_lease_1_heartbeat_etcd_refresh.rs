//! BUG-LEASE-1 (P0 #1, coco arch review 2026-06-05) — heartbeat
//! doesn't refresh the persisted `MgrInodeLeaseRecord.expires_at` in
//! etcd, so a manager leader failover causes the new leader to
//! replay a STALE deadline and revoke an active writer.
//!
//! Reproduction (lightweight — no full failover required; we read
//! the etcd record directly to show the persistence gap):
//!
//! 1. Start manager M1 + etcd.
//! 2. AcquireLease for a write lease via M1. Etcd now has
//!    `inode_leases/<ino>` with `expires_at = now_epoch + 30`.
//! 3. Sleep 3 s.
//! 4. HeartbeatLease via M1 (multiple times to mirror production).
//! 5. Read the etcd record directly.
//!    - **Pre-fix** (the bug): `expires_at` is unchanged at
//!      `now_epoch + 30`. The 3 s of life the writer's heartbeats
//!      bought is INVISIBLE to anyone reading from etcd — incl. a
//!      new leader on failover.
//!    - **Post-fix**: `expires_at ≈ now_epoch + 3 + 30 = now_epoch + 33`.
//!      The writer's heartbeats are durably persisted.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, AcquireLeaseReq, AcquireLeaseResp, HeartbeatLeaseReq,
    HeartbeatLeaseResp, MgrClientId, MgrInodeLeaseRecord, CODE_OK, LEASE_CLIENT_KIND_FUSE,
    LEASE_MODE_WRITE, MSG_ACQUIRE_LEASE, MSG_HEARTBEAT_LEASE,
};

use support::{pick_addr, start_etcd};

fn start_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint])
                .await
                .expect("new manager with etcd");
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(400));
}

async fn read_etcd_lease_record(
    etcd_endpoint: &str,
    ino: u64,
) -> Option<MgrInodeLeaseRecord> {
    let client = autumn_etcd::EtcdClient::connect(etcd_endpoint).await.unwrap();
    let key = format!("inode_leases/{ino}");
    let resp = client.get(key.as_bytes()).await.unwrap();
    resp.kvs.first().and_then(|kv| rkyv_decode(&kv.value).ok())
}

#[test]
#[ignore] // requires embedded etcd
fn p0_1_heartbeat_persists_etcd_expires_at() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        let mgr_addr = pick_addr();
        start_etcd_manager(mgr_addr, etcd_endpoint.clone());

        // Wait for leader election to settle.
        compio::time::sleep(Duration::from_secs(2)).await;

        let mgr = RpcClient::connect(mgr_addr).await.expect("connect M");

        let writer = MgrClientId {
            kind: LEASE_CLIENT_KIND_FUSE,
            uuid: [0xa1; 16],
            host: "writer".to_string(),
        };
        let ino = 1234u64;

        // (1) Acquire writer lease.
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
            .expect("acquire");
        let resp: AcquireLeaseResp = rkyv_decode(&resp).expect("decode acquire");
        assert_eq!(resp.code, CODE_OK, "acquire must succeed");

        // (2) Read the initial persisted expires_at.
        let rec0 = read_etcd_lease_record(&etcd_endpoint, ino)
            .await
            .expect("etcd should have inode_leases/<ino>");
        let expires_at_0 = rec0.expires_at;

        // (3) Sleep so heartbeats land at a different wall-clock
        // moment, then heartbeat several times.
        let sleep_secs = 3i64;
        compio::time::sleep(Duration::from_secs(sleep_secs as u64)).await;
        for _ in 0..3 {
            let resp = mgr
                .call(
                    MSG_HEARTBEAT_LEASE,
                    rkyv_encode(&HeartbeatLeaseReq {
                        client: writer.clone(),
                        ino,
                    }),
                )
                .await
                .expect("heartbeat");
            let resp: HeartbeatLeaseResp = rkyv_decode(&resp).expect("decode hb");
            assert_eq!(resp.code, CODE_OK, "heartbeat must succeed");
        }

        // (4) Re-read the persisted expires_at. POST-FIX: must
        // reflect the post-heartbeat deadline (i.e. moved forward
        // by roughly `sleep_secs`). PRE-FIX (the bug): unchanged.
        let rec1 = read_etcd_lease_record(&etcd_endpoint, ino)
            .await
            .expect("etcd should still have inode_leases/<ino>");
        let expires_at_1 = rec1.expires_at;

        let delta = expires_at_1 - expires_at_0;
        eprintln!(
            "BUG-LEASE-1 repro: initial expires_at={}, post-heartbeat expires_at={}, delta={}s (expected ≈ {}s post-fix; 0s pre-fix)",
            expires_at_0, expires_at_1, delta, sleep_secs
        );
        assert!(
            delta >= sleep_secs - 1,
            "BUG-LEASE-1: heartbeat must refresh etcd `expires_at` so a manager failover sees the writer's actual deadline; got delta={delta}s (pre-fix bug: delta=0)"
        );
        // Also the version should still be unchanged (heartbeat
        // never bumps it).
        assert_eq!(rec0.version, rec1.version);
    });
}
