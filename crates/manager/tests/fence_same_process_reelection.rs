//! A fenced txn sent in one leader term of a manager must not commit in a
//! later term of the same process. The leader key then holds this process's id
//! again at a newer revision; reading that as "the fence moved within the
//! term" re-sent the old txn under the new fence, after the new term's replay
//! had already rebuilt memory from etcd. For a write-lease grant that gave
//! the lease to two writers: W's grant, decided in the old term, landed after X
//! was granted the same inode in the new term, and W's persisted record
//! replaced X's.
//!
//! W's grant txn is held after it took its fence revision (a request still in
//! flight, `FENCED_TXN_TEST_HOLD_*`); the test revokes the leader's etcd lease,
//! the manager steps down and is elected again, X takes the lease, then W's
//! txn is released. Needs `etcd` on `PATH`.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_manager::{FENCED_TXN_TEST_HOLD_ARMED, FENCED_TXN_TEST_HOLD_PREFIX, FENCED_TXN_TEST_PARKED};
use autumn_rpc::manager_rpc::{rkyv_decode, MgrInodeLeaseRecord, LEASE_MODE_WRITE};
use support::{pick_addr, start_etcd};

const INO: u64 = 4242;
const LEADER_KEY: &[u8] = b"autumn-rs/stream-manager/leader";

fn start_manager(addr: SocketAddr, etcd: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let m = autumn_manager::AutumnManager::new_with_etcd(vec![etcd], support::manager_identity())
                .await
                .expect("manager");
            if let Err(e) = m.serve(addr).await {
                eprintln!("manager serve: {e:#}");
            }
        });
    });
}

async fn leader_lease(aux: &autumn_etcd::EtcdClient) -> Option<i64> {
    aux.get(LEADER_KEY).await.expect("get leader key").kvs.first().map(|kv| kv.lease)
}

async fn persisted_writer(aux: &autumn_etcd::EtcdClient) -> Option<[u8; 16]> {
    let r = aux.get(format!("inode_leases/{INO}").as_bytes()).await.expect("get record");
    r.kvs.first().map(|kv| {
        let rec: MgrInodeLeaseRecord = rkyv_decode(&kv.value).expect("decode record");
        rec.writer.uuid
    })
}

#[test]
fn a_grant_from_an_earlier_term_does_not_land_in_the_next() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd) = start_etcd().await;
        let mgr = pick_addr();
        start_manager(mgr, etcd.clone());
        let aux = autumn_etcd::EtcdClient::connect(&etcd).await.expect("aux etcd");
        let start = Instant::now();
        let cluster = loop {
            match ClusterClient::connect(&mgr.to_string(), "fs").await {
                Ok(c) => break std::rc::Rc::new(c),
                Err(e) => {
                    assert!(start.elapsed() < Duration::from_secs(30), "manager never led: {e:#}");
                    compio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        };
        let (w, x) = (DaemonClientId::new_fuse("w"), DaemonClientId::new_fuse("x"));

        // W's grant parks after taking its fence revision.
        *FENCED_TXN_TEST_HOLD_PREFIX.lock().unwrap() = format!("inode_leases/{INO}");
        let parked = FENCED_TXN_TEST_PARKED.load(Ordering::Relaxed);
        FENCED_TXN_TEST_HOLD_ARMED.store(true, Ordering::Relaxed);
        let w_task = {
            let (cluster, w) = (cluster.clone(), w.clone());
            compio::runtime::spawn(async move { lease::acquire(&cluster, &w, INO, LEASE_MODE_WRITE).await })
        };
        while FENCED_TXN_TEST_PARKED.load(Ordering::Relaxed) == parked {
            assert!(start.elapsed() < Duration::from_secs(60), "W's grant never reached etcd");
            compio::time::sleep(Duration::from_millis(10)).await;
        }

        // The leader's lease goes; the same process is elected again.
        let old = leader_lease(&aux).await.expect("leader key");
        aux.lease_revoke(old).await.expect("revoke leader lease");
        let t = Instant::now();
        loop {
            if let Some(new) = leader_lease(&aux).await {
                if new != old {
                    break;
                }
            }
            assert!(t.elapsed() < Duration::from_secs(60), "the manager was not elected again");
            compio::time::sleep(Duration::from_millis(100)).await;
        }

        // X takes the lease in the new term.
        let t = Instant::now();
        loop {
            match lease::acquire(&cluster, &x, INO, LEASE_MODE_WRITE).await {
                Ok(AcquireResult::Granted(_)) => break,
                other => {
                    assert!(t.elapsed() < Duration::from_secs(30), "X was not granted: {other:?}");
                    compio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        assert_eq!(persisted_writer(&aux).await, Some(x.as_wire().uuid));

        // W's old-term txn is released.
        FENCED_TXN_TEST_HOLD_ARMED.store(false, Ordering::Relaxed);
        let w_result = w_task.await;
        eprintln!("W: {w_result:?}");
        assert!(
            !matches!(w_result, Ok(Ok(AcquireResult::Granted(_)))),
            "W was granted the write lease X holds"
        );
        assert_eq!(persisted_writer(&aux).await, Some(x.as_wire().uuid), "W's record replaced X's");
        // X still holds it.
        let y = DaemonClientId::new_fuse("y");
        let y_result = lease::acquire(&cluster, &y, INO, LEASE_MODE_WRITE).await;
        assert!(matches!(y_result, Ok(AcquireResult::Conflict { .. })), "Y: {y_result:?}");
    });
}
