//! Something from an earlier leader term of a manager must not step down the
//! term the same process leads now. Two such things exist: a fence diagnosis
//! whose read of the leader key (gone) is answered only after the process won
//! the key again, and the keepalive loop of the old leader lease, which runs
//! on after the process stepped down for another reason and fails later.
//! Stepped down while its own key stays alive, the process never runs for
//! leader again: the election CAS finds its key and it waits for a deletion
//! that does not come. Needs `etcd` on `PATH`.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_manager::{
    DIAGNOSE_FENCE_TEST_HOLD, FENCED_TXN_TEST_HOLD_ARMED, FENCED_TXN_TEST_HOLD_PREFIX, FENCED_TXN_TEST_PARKED,
};
use autumn_rpc::manager_rpc::LEASE_MODE_WRITE;
use support::{pick_addr, start_etcd};

const LEADER_KEY: &[u8] = b"autumn-rs/stream-manager/leader";

/// The failpoints are process-wide.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Serializes the tests and disarms what a failed one may have left armed.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    let g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    FENCED_TXN_TEST_HOLD_ARMED.store(false, Ordering::Relaxed);
    FENCED_TXN_TEST_HOLD_PREFIX.lock().unwrap_or_else(|e| e.into_inner()).clear();
    DIAGNOSE_FENCE_TEST_HOLD.store(0, Ordering::Relaxed);
    g
}

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

async fn connect(mgr: SocketAddr) -> Rc<ClusterClient> {
    let start = Instant::now();
    loop {
        match ClusterClient::connect(&mgr.to_string(), "fs").await {
            Ok(c) => return Rc::new(c),
            Err(e) => {
                assert!(start.elapsed() < Duration::from_secs(30), "manager never led: {e:#}");
                compio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn leader_lease(aux: &autumn_etcd::EtcdClient) -> Option<i64> {
    aux.get(LEADER_KEY).await.expect("get leader key").kvs.first().map(|kv| kv.lease)
}

async fn wait_new_term(aux: &autumn_etcd::EtcdClient, old: i64) {
    let t = Instant::now();
    loop {
        if leader_lease(aux).await.is_some_and(|l| l != old) {
            return;
        }
        assert!(t.elapsed() < Duration::from_secs(60), "the manager was not elected again");
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A write-lease grant on a fresh inode answers once the manager leads.
async fn leads_within(cluster: &ClusterClient, ino: &mut u64, within: Duration) -> bool {
    let c = DaemonClientId::new_fuse("probe");
    let t = Instant::now();
    while t.elapsed() < within {
        *ino += 1;
        if matches!(lease::acquire(cluster, &c, *ino, LEASE_MODE_WRITE).await, Ok(AcquireResult::Granted(_))) {
            return true;
        }
        compio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[test]
fn a_late_fence_diagnosis_does_not_step_down_the_next_term() {
    let _serial = serial();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd) = start_etcd().await;
        let mgr = pick_addr();
        start_manager(mgr, etcd.clone());
        let aux = autumn_etcd::EtcdClient::connect(&etcd).await.expect("aux etcd");
        let cluster = connect(mgr).await;
        let mut probe = 1_000_000u64;

        // W's grant parks before it is sent.
        *FENCED_TXN_TEST_HOLD_PREFIX.lock().unwrap() = "inode_leases/7".to_string();
        let parked = FENCED_TXN_TEST_PARKED.load(Ordering::Relaxed);
        FENCED_TXN_TEST_HOLD_ARMED.store(true, Ordering::Relaxed);
        let w_task = {
            let cluster = cluster.clone();
            compio::runtime::spawn(async move {
                lease::acquire(&cluster, &DaemonClientId::new_fuse("w"), 7, LEASE_MODE_WRITE).await
            })
        };
        let t = Instant::now();
        while FENCED_TXN_TEST_PARKED.load(Ordering::Relaxed) == parked {
            assert!(t.elapsed() < Duration::from_secs(30), "W's grant never reached etcd");
            compio::time::sleep(Duration::from_millis(10)).await;
        }

        // The leader key goes; W's txn fails its compare and the diagnosis
        // that reads the key gone is held before it acts.
        let old = leader_lease(&aux).await.expect("leader key");
        aux.lease_revoke(old).await.expect("revoke leader lease");
        DIAGNOSE_FENCE_TEST_HOLD.store(1, Ordering::Relaxed);
        FENCED_TXN_TEST_HOLD_ARMED.store(false, Ordering::Relaxed);
        let t = Instant::now();
        while DIAGNOSE_FENCE_TEST_HOLD.load(Ordering::Relaxed) != 2 {
            assert!(t.elapsed() < Duration::from_secs(30), "no diagnosis read the key");
            compio::time::sleep(Duration::from_millis(10)).await;
        }

        // The process wins the key again and leads.
        wait_new_term(&aux, old).await;
        assert!(leads_within(&cluster, &mut probe, Duration::from_secs(30)).await, "no new term");

        // The held diagnosis acts now; the new term must keep leading.
        DIAGNOSE_FENCE_TEST_HOLD.store(0, Ordering::Relaxed);
        let w = w_task.await;
        assert!(!matches!(w, Ok(Ok(AcquireResult::Granted(_)))), "W, decided in the old term, was granted: {w:?}");
        compio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            leads_within(&cluster, &mut probe, Duration::from_secs(20)).await,
            "an old-term diagnosis stepped the new term down for good"
        );
    });
}

#[test]
fn the_old_leader_lease_failing_does_not_step_down_the_next_term() {
    let _serial = serial();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd) = start_etcd().await;
        let mgr = pick_addr();
        start_manager(mgr, etcd.clone());
        let aux = autumn_etcd::EtcdClient::connect(&etcd).await.expect("aux etcd");
        let cluster = connect(mgr).await;
        let mut probe = 2_000_000u64;

        // The key goes but its lease stays: the next fenced write steps the
        // manager down, the keepalive of the old lease keeps running.
        let old = leader_lease(&aux).await.expect("leader key");
        aux.delete(LEADER_KEY).await.expect("delete leader key");
        let _ = lease::acquire(&cluster, &DaemonClientId::new_fuse("w"), 7, LEASE_MODE_WRITE).await;
        wait_new_term(&aux, old).await;
        assert!(leads_within(&cluster, &mut probe, Duration::from_secs(30)).await, "no new term");

        // The old lease fails now; the new term must keep leading.
        aux.lease_revoke(old).await.expect("revoke the old leader lease");
        compio::time::sleep(Duration::from_secs(5)).await;
        assert!(
            leads_within(&cluster, &mut probe, Duration::from_secs(20)).await,
            "the old lease's keepalive stepped the new term down for good"
        );
    });
}
