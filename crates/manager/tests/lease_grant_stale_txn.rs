//! A write-lease grant whose etcd put is still in flight must not land
//! after a later lifecycle op on the same inode: the grant updates memory
//! first and persists second, so without ordering the late put writes an
//! older holder over the record a later grant or release left.

mod support;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use autumn_manager::{
    AutumnManager, FENCED_TXN_TEST_HOLD_ARMED as ARMED, FENCED_TXN_TEST_HOLD_PREFIX as PREFIX,
    FENCED_TXN_TEST_PARKED as PARKED,
};
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, AcquireLeaseReq, AcquireLeaseResp, HeartbeatLeaseReq, HeartbeatLeaseResp,
    MgrClientId, MgrInodeLeaseRecord, ReleaseLeaseReq, ReleaseLeaseResp, CODE_NOT_LEADER, CODE_OK,
    CODE_PRECONDITION, LEASE_CLIENT_KIND_FUSE, LEASE_MODE_WRITE,
};
use support::start_etcd;

/// The txn failpoint is process-wide.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    let g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    ARMED.store(false, Ordering::Relaxed);
    PREFIX.lock().unwrap().clear();
    g
}

fn client(n: u8) -> MgrClientId {
    MgrClientId { kind: LEASE_CLIENT_KIND_FUSE, uuid: [n; 16], host: format!("host-{n}") }
}

async fn acquire(m: &AutumnManager, c: &MgrClientId, ino: u64) -> AcquireLeaseResp {
    let req = AcquireLeaseReq { client: c.clone(), ino, mode: LEASE_MODE_WRITE, force: false };
    rkyv_decode(&m.handle_acquire_lease(rkyv_encode(&req)).await.unwrap()).unwrap()
}

async fn release(m: &AutumnManager, c: &MgrClientId, ino: u64) -> ReleaseLeaseResp {
    let req = ReleaseLeaseReq { client: c.clone(), ino };
    rkyv_decode(&m.handle_release_lease(rkyv_encode(&req)).await.unwrap()).unwrap()
}

async fn heartbeat(m: &AutumnManager, c: &MgrClientId, ino: u64) -> HeartbeatLeaseResp {
    let req = HeartbeatLeaseReq { client: c.clone(), ino };
    rkyv_decode(&m.handle_heartbeat_lease(rkyv_encode(&req)).await.unwrap()).unwrap()
}

/// Arms the hold for `ino`'s record and waits until the next txn writing it
/// is parked.
async fn park_next_write(ino: u64, within: Duration) {
    *PREFIX.lock().unwrap() = format!("inode_leases/{ino}");
    let before = PARKED.load(Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let deadline = Instant::now() + within;
    while PARKED.load(Ordering::Relaxed) == before {
        assert!(Instant::now() < deadline, "no txn reached the hold");
        compio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn stored_writer(aux: &autumn_etcd::EtcdClient, ino: u64) -> Option<[u8; 16]> {
    let resp = aux.get(format!("inode_leases/{ino}").as_bytes()).await.unwrap();
    resp.kvs
        .first()
        .map(|kv| rkyv_decode::<MgrInodeLeaseRecord>(&kv.value).unwrap().writer.uuid)
}

/// Parks W's first write-lease put before it is sent, runs `later` (which
/// may or may not finish while W is parked), then releases W.
async fn with_parked_grant<F, Fut>(m: &AutumnManager, w: &MgrClientId, ino: u64, later: F)
where
    F: FnOnce(AutumnManager) -> Fut,
    Fut: std::future::Future<Output = ()> + 'static,
{
    *PREFIX.lock().unwrap() = format!("inode_leases/{ino}");
    let before = PARKED.load(Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let (mm, ww) = (m.clone(), w.clone());
    let first = compio::runtime::spawn(async move { acquire(&mm, &ww, ino).await });
    let deadline = Instant::now() + Duration::from_secs(10);
    while PARKED.load(Ordering::Relaxed) == before {
        assert!(Instant::now() < deadline, "the first grant never reached the hold");
        compio::time::sleep(Duration::from_millis(5)).await;
    }
    let rest = compio::runtime::spawn(later(m.clone()));
    // Long enough for the later ops to finish if nothing orders them
    // behind the parked grant.
    compio::time::sleep(Duration::from_secs(1)).await;
    ARMED.store(false, Ordering::Relaxed);
    // The late grant may succeed (it was W's) or not; either way it must
    // not be the last word.
    first.await.unwrap();
    rest.await.unwrap();
}

/// W's first grant parks; W retries, releases; X is granted. Releasing the
/// first grant must leave X in etcd and in memory.
#[test]
fn a_late_grant_does_not_overwrite_the_next_writer() {
    let _serial = serial();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd, endpoint) = start_etcd().await;
        let m = AutumnManager::new_with_etcd(vec![endpoint.clone()], support::manager_identity())
            .await
            .unwrap();
        let aux = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
        let (w, x, ino) = (client(0xa1), client(0xb2), 76_543);

        let (w2, x2) = (w.clone(), x.clone());
        with_parked_grant(&m, &w, ino, move |m| async move {
            assert_eq!(acquire(&m, &w2, ino).await.code, CODE_OK);
            assert_eq!(release(&m, &w2, ino).await.code, CODE_OK);
            assert_eq!(acquire(&m, &x2, ino).await.code, CODE_OK);
        })
        .await;

        assert_eq!(stored_writer(&aux, ino).await, Some(x.uuid), "a late grant overwrote X's record");
        // Memory agrees: W cannot take the inode from X.
        assert_eq!(acquire(&m, &w, ino).await.code, CODE_PRECONDITION);
    });
}

/// W's first grant parks; W releases. The late put must not bring back a
/// record for a writer memory no longer holds.
#[test]
fn a_late_grant_does_not_outlive_its_release() {
    let _serial = serial();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd, endpoint) = start_etcd().await;
        let m = AutumnManager::new_with_etcd(vec![endpoint.clone()], support::manager_identity())
            .await
            .unwrap();
        let aux = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
        let (w, x, ino) = (client(0xa1), client(0xb2), 76_544);

        let w2 = w.clone();
        with_parked_grant(&m, &w, ino, move |m| async move {
            assert_eq!(release(&m, &w2, ino).await.code, CODE_OK);
        })
        .await;

        assert_eq!(stored_writer(&aux, ino).await, None, "a late grant outlived its release");
        // Memory agrees: the inode is free.
        assert_eq!(acquire(&m, &x, ino).await.code, CODE_OK);
        assert_eq!(stored_writer(&aux, ino).await, Some(x.uuid));
    });
}

/// The TTL sweep's delete of an expired writer's record parks; the writer
/// acquires again meanwhile. Its record must survive the late delete.
#[test]
fn a_late_revoke_does_not_delete_a_renewed_grant() {
    let _serial = serial();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd, endpoint) = start_etcd().await;
        let params = autumn_params::manager::ManagerParams::default();
        params.inode_lease_ttl_secs.set(15);
        let m = AutumnManager::new_with_etcd_params(vec![endpoint.clone()], support::manager_identity(), params)
            .await
            .unwrap();
        m.start_runtime_tasks();
        let aux = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
        let (w, x, ino) = (client(0xa1), client(0xb2), 76_545);
        assert_eq!(acquire(&m, &w, ino).await.code, CODE_OK);

        // W never heartbeats: the sweep finds it expired after the TTL.
        *PREFIX.lock().unwrap() = format!("inode_leases/{ino}");
        let before = PARKED.load(Ordering::Relaxed);
        ARMED.store(true, Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_secs(40);
        while PARKED.load(Ordering::Relaxed) == before {
            assert!(Instant::now() < deadline, "the sweep never reached the hold");
            compio::time::sleep(Duration::from_millis(20)).await;
        }
        let (mm, ww) = (m.clone(), w.clone());
        let again = compio::runtime::spawn(async move { acquire(&mm, &ww, ino).await });
        compio::time::sleep(Duration::from_secs(1)).await;
        ARMED.store(false, Ordering::Relaxed);
        assert_eq!(again.await.unwrap().code, CODE_OK);
        // Let the sweep finish its commit phase.
        compio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(stored_writer(&aux, ino).await, Some(w.uuid), "a late revoke deleted W's renewed record");
        assert_eq!(acquire(&m, &x, ino).await.code, CODE_PRECONDITION);
    });
}

/// The sweep's delete of an expired writer's record parks; the writer's
/// heartbeat arrives meanwhile. It must not renew a lease the sweep is
/// deleting: memory and etcd end without W, and X can take the inode.
#[test]
fn an_expired_writer_is_not_renewed_under_the_sweep() {
    let _serial = serial();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd, endpoint) = start_etcd().await;
        let params = autumn_params::manager::ManagerParams::default();
        params.inode_lease_ttl_secs.set(15);
        let m = AutumnManager::new_with_etcd_params(vec![endpoint.clone()], support::manager_identity(), params)
            .await
            .unwrap();
        m.start_runtime_tasks();
        let aux = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
        let (w, x, ino) = (client(0xa1), client(0xb2), 76_546);
        assert_eq!(acquire(&m, &w, ino).await.code, CODE_OK);

        park_next_write(ino, Duration::from_secs(40)).await;
        let hb = heartbeat(&m, &w, ino).await;
        ARMED.store(false, Ordering::Relaxed);
        assert_ne!(hb.code, CODE_OK, "an expired writer was renewed");
        compio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(acquire(&m, &x, ino).await.code, CODE_OK, "memory still holds W");
        assert_eq!(stored_writer(&aux, ino).await, Some(x.uuid));
    });
}

/// A grant waiting for the inode's turn while the leader term changes
/// (the turn's holder parked across a lease loss and re-election) is
/// answered NOT_LEADER, not carried into the new term.
#[test]
fn a_grant_waiting_across_a_term_change_is_not_carried_over() {
    let _serial = serial();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd, endpoint) = start_etcd().await;
        let m = AutumnManager::new_with_etcd(vec![endpoint.clone()], support::manager_identity())
            .await
            .unwrap();
        m.start_runtime_tasks();
        let aux = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
        let (w, z, ino) = (client(0xa1), client(0xc3), 76_547);
        let leader_lease = || async {
            let r = aux.get(b"autumn-rs/stream-manager/leader").await.unwrap();
            r.kvs.first().map(|kv| kv.lease)
        };

        let (mm, ww) = (m.clone(), w.clone());
        let first = compio::runtime::spawn(async move { acquire(&mm, &ww, ino).await });
        park_next_write(ino, Duration::from_secs(10)).await;
        let (mm, zz) = (m.clone(), z.clone());
        let waiting = compio::runtime::spawn(async move { acquire(&mm, &zz, ino).await });
        compio::time::sleep(Duration::from_millis(200)).await;

        let old = leader_lease().await.expect("leader key");
        aux.lease_revoke(old).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        while leader_lease().await.is_none_or(|l| l == old) {
            assert!(Instant::now() < deadline, "the manager was not elected again");
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        ARMED.store(false, Ordering::Relaxed);
        assert_ne!(first.await.unwrap().code, CODE_OK, "the old term's grant landed");
        let z_resp = waiting.await.unwrap();
        assert_eq!(z_resp.code, CODE_NOT_LEADER, "a grant admitted in the old term ran in the new one: {z_resp:?}");
    });
}
