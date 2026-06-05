//! coco R2-P0 #1 (2026-06-06) — heartbeat etcd refresh must NOT
//! resurrect a released or overwritten record.
//!
//! Pre-fix scenario: writer A heartbeats; in-memory deadline moves
//! forward; etcd put is a blind write. Meanwhile writer A is also
//! Releasing the lease (close path). The release deletes the etcd
//! record AND wipes in-memory writer; the heartbeat's blind put
//! then RESURRECTS the record, with stale expires_at. Failover
//! replay sees the resurrected record and tries to revoke a writer
//! that no longer exists in-memory (a wasted revoke push) and
//! blocks the next AcquireLease until TTL.
//!
//! Test strategy: directly drive the heartbeat handler to force
//! the race by:
//! 1. Acquire writer lease (etcd record V0 written).
//! 2. Release lease (etcd record deleted).
//! 3. Immediately Heartbeat. Pre-fix: heartbeat would have
//!    blind-put a stale record. Post-fix: CAS sees no baseline
//!    record → returns Ok(false) → skip → etcd stays clean.
//!
//! Assert: post-Release + post-Heartbeat, etcd has NO inode_leases
//! record for the ino.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, AcquireLeaseReq, AcquireLeaseResp, HeartbeatLeaseReq,
    HeartbeatLeaseResp, MgrClientId, ReleaseLeaseReq, ReleaseLeaseResp, CODE_OK,
    LEASE_CLIENT_KIND_FUSE, LEASE_MODE_WRITE, MSG_ACQUIRE_LEASE, MSG_HEARTBEAT_LEASE,
    MSG_RELEASE_LEASE,
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

async fn etcd_has_inode_lease(etcd_endpoint: &str, ino: u64) -> bool {
    let client = autumn_etcd::EtcdClient::connect(etcd_endpoint).await.unwrap();
    let key = format!("inode_leases/{ino}");
    let resp = client.get(key.as_bytes()).await.unwrap();
    !resp.kvs.is_empty()
}

#[test]
#[ignore] // requires embedded etcd
fn r2_p0_1_heartbeat_after_release_must_not_resurrect_etcd_record() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;
        let mgr_addr = pick_addr();
        start_etcd_manager(mgr_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_secs(2)).await;

        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");
        let writer = MgrClientId {
            kind: LEASE_CLIENT_KIND_FUSE,
            uuid: [0xc1; 16],
            host: "writer".to_string(),
        };
        let ino = 5555u64;

        // (1) Acquire writer lease → etcd record V0 written.
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
        assert!(
            etcd_has_inode_lease(&etcd_endpoint, ino).await,
            "etcd record must be present after acquire"
        );

        // (2) Release → etcd record deleted.
        let resp = mgr
            .call(
                MSG_RELEASE_LEASE,
                rkyv_encode(&ReleaseLeaseReq {
                    client: writer.clone(),
                    ino,
                }),
            )
            .await
            .unwrap();
        let rel: ReleaseLeaseResp = rkyv_decode(&resp).unwrap();
        assert_eq!(rel.code, CODE_OK);
        assert!(
            !etcd_has_inode_lease(&etcd_endpoint, ino).await,
            "etcd record must be absent after release"
        );

        // (3) Heartbeat the now-released lease. Pre-fix: a blind
        // etcd put inside the heartbeat handler would re-create
        // the record. Post-fix (R2-P0 #1): the
        // `read_then_cas_put` helper sees no baseline record →
        // returns Ok(false) → skip the put → etcd stays clean.
        //
        // The in-memory `heartbeat()` call returns `NotHeld`
        // since the release already cleared the writer slot, so
        // we never reach the etcd write path anyway — but this
        // test still exercises the WIRE end-to-end against a
        // best-effort actor that doesn't know it was released.
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
        // Heartbeat returns NotFound for a released lease — this
        // is the in-memory state catching it before etcd is
        // touched.
        assert_eq!(
            hb.code,
            autumn_rpc::manager_rpc::CODE_NOT_FOUND,
            "heartbeat after release must return NotFound: {hb:?}"
        );

        // The KEY assertion: etcd record MUST stay absent. Pre-fix
        // (blind put) this would have been TRUE only because the
        // in-memory NotHeld short-circuited the etcd write —
        // i.e. the in-memory check already protects this exact
        // sequence. The CAS fix is the BELT-AND-BRACES guarantee
        // for the harder case where in-memory state DRIFTS during
        // the etcd await (which we can't deterministically
        // construct here without manager-internal hooks). Still
        // worth asserting end-to-end.
        assert!(
            !etcd_has_inode_lease(&etcd_endpoint, ino).await,
            "BUG-LEASE-1 R2-P0 #1: etcd record MUST stay absent after release+heartbeat"
        );
    });
}
