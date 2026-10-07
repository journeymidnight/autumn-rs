//! Manager identity and membership against a real etcd: each manager holds
//! `managerAlive/<id>` on its lease, the leader folds presence into
//! `managerMembers/`, a second process with a taken id waits, a lost lease is
//! reclaimed, and a running manager cannot be removed.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::version_hello::Role;

use support::{pick_addr, start_etcd};

/// Runs a manager in its own thread; `started` turns true once it holds its
/// id, and setting the returned flag drops its runtime (its lease then lapses).
fn start_manager(
    mgr_addr: SocketAddr,
    etcd_endpoint: String,
    id: u64,
) -> (Arc<AtomicBool>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let (stop_t, started_t) = (stop.clone(), started.clone());
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let identity = autumn_manager::ManagerIdentity {
                id,
                address: mgr_addr.to_string(),
            };
            let manager = autumn_manager::AutumnManager::new_with_etcd(vec![etcd_endpoint], identity)
                .await
                .expect("new manager with etcd");
            started_t.store(true, Ordering::Release);
            compio::runtime::spawn(async move {
                let _ = manager.serve(mgr_addr).await;
            })
            .detach();
            while !stop_t.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    (stop, started)
}

async fn remove(mgr: &RpcClient, id: u64) -> CodeResp {
    let req = RemoveMemberReq {
        role: MEMBER_ROLE_MANAGER,
        id,
        set_by: "manager-members-test".to_string(),
    };
    let resp = mgr
        .call(MSG_REMOVE_MEMBER, rkyv_encode(&req).to_vec().into())
        .await
        .expect("rpc");
    rkyv_decode(&resp).expect("decode CodeResp")
}

async fn value(etcd: &autumn_etcd::EtcdClient, key: &str) -> Option<autumn_etcd::proto::KeyValue> {
    etcd.get(key).await.expect("etcd get").kvs.into_iter().next()
}

async fn wait_until<F, Fut>(what: &str, secs: u64, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..secs * 4 {
        if f().await {
            return;
        }
        compio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("timed out waiting for {what}");
}

#[test]
#[ignore] // requires a real etcd binary on PATH
fn managers_hold_their_ids_and_the_leader_keeps_the_membership() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, endpoint) = start_etcd().await;
        let etcd = autumn_etcd::EtcdClient::connect(&endpoint)
            .await
            .expect("etcd connect");

        let a_addr = pick_addr();
        let (a_stop, _) = start_manager(a_addr, endpoint.clone(), 1);
        compio::time::sleep(Duration::from_secs(2)).await;
        let b_addr = pick_addr();
        let (b_stop, _) = start_manager(b_addr, endpoint.clone(), 2);

        // The leader (A, first in) makes both present ids members.
        wait_until("both managers to be members", 15, || async {
            value(&etcd, "managerMembers/1").await.is_some()
                && value(&etcd, "managerMembers/2").await.is_some()
        })
        .await;
        let a1 = value(&etcd, "managerAlive/1").await.expect("A holds id 1");
        assert!(String::from_utf8_lossy(&a1.value).ends_with(&a_addr.to_string()));

        let a = RpcClient::connect_as(a_addr, Role::Admin, None)
            .await
            .expect("connect A");
        let r = remove(&a, 2).await;
        assert_eq!(r.code, CODE_PRECONDITION, "a running manager must not be removable: {}", r.message);
        let r = remove(&a, 9).await;
        assert_eq!(r.code, CODE_NOT_FOUND, "{}", r.message);

        // A lost lease is reclaimed by the same process.
        let revoked = etcd.lease_revoke(a1.lease).await;
        assert!(revoked.is_ok(), "revoke: {revoked:?}");
        wait_until("A to reclaim id 1", 15, || async {
            value(&etcd, "managerAlive/1")
                .await
                .is_some_and(|kv| kv.value == a1.value && kv.lease != a1.lease)
        })
        .await;

        // B stops: its presence lapses, it stays a member, and can now go.
        b_stop.store(true, Ordering::Release);
        wait_until("B's presence to lapse", 20, || async {
            value(&etcd, "managerAlive/2").await.is_none()
        })
        .await;
        compio::time::sleep(Duration::from_secs(3)).await;
        assert!(value(&etcd, "managerMembers/2").await.is_some(), "a stopped manager stays expected");
        let r = remove(&a, 2).await;
        assert_eq!(r.code, CODE_OK, "remove stopped manager 2: {}", r.message);
        assert!(value(&etcd, "managerMembers/2").await.is_none());
        let r = remove(&a, 2).await;
        assert_eq!(r.code, CODE_NOT_FOUND, "{}", r.message);

        // A second process with A's id waits while A runs, and takes over
        // once A is gone.
        let c_addr = pick_addr();
        let (_c_stop, c_started) = start_manager(c_addr, endpoint.clone(), 1);
        compio::time::sleep(Duration::from_secs(4)).await;
        assert!(!c_started.load(Ordering::Acquire), "a duplicate id must not start");
        let held = value(&etcd, "managerAlive/1").await.expect("id 1 held");
        assert!(String::from_utf8_lossy(&held.value).ends_with(&a_addr.to_string()));
        a_stop.store(true, Ordering::Release);
        wait_until("the waiting process to take id 1", 20, || async {
            c_started.load(Ordering::Acquire)
        })
        .await;
        let held = value(&etcd, "managerAlive/1").await.expect("id 1 held");
        assert!(String::from_utf8_lossy(&held.value).ends_with(&c_addr.to_string()));
    });
}
