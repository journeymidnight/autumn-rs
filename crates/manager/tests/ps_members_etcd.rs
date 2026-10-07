//! PS membership against a real etcd: a PS that dies stays in the expected
//! fleet (evicted, not forgotten) across a leader change, and only an
//! operator's remove — refused while the PS is live — takes it out.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::version_hello::Role;

use support::{pick_addr, start_etcd};

fn start_stoppable_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_thread = flag.clone();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = autumn_manager::AutumnManager::new_with_etcd(vec![etcd_endpoint], support::manager_identity())
                .await
                .expect("new manager with etcd");
            compio::runtime::spawn(async move {
                let _ = manager.serve(mgr_addr).await;
            })
            .detach();
            while !flag_thread.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    flag
}

async fn code_of(mgr: &RpcClient, msg: u8, payload: Vec<u8>) -> CodeResp {
    let resp = mgr.call(msg, payload.into()).await.expect("rpc");
    rkyv_decode(&resp).expect("decode CodeResp")
}

async fn register(mgr: &RpcClient, ps_id: u64) -> CodeResp {
    let req = RegisterPsReq {
        ps_id,
        address: format!("127.0.0.1:{}", 19000 + ps_id),
        slot_cap: 0,
    };
    code_of(mgr, MSG_REGISTER_PS, rkyv_encode(&req).to_vec()).await
}

async fn heartbeat(mgr: &RpcClient, ps_id: u64) {
    let req = HeartbeatPsReq {
        ps_id,
        slot_cap: 0,
        open_parts: Vec::new(),
    };
    let r = code_of(mgr, MSG_HEARTBEAT_PS, rkyv_encode(&req).to_vec()).await;
    assert_eq!(r.code, CODE_OK, "heartbeat ps {ps_id}: {}", r.message);
}

async fn remove(mgr: &RpcClient, ps_id: u64) -> CodeResp {
    let req = RemoveMemberReq {
        role: MEMBER_ROLE_PS,
        id: ps_id,
        set_by: "ps-members-test".to_string(),
    };
    code_of(mgr, MSG_REMOVE_MEMBER, rkyv_encode(&req).to_vec()).await
}

async fn ps_servers(mgr: &RpcClient) -> Vec<PsOverview> {
    let resp = mgr
        .call(MSG_GET_CLUSTER_OVERVIEW, bytes::Bytes::new())
        .await
        .expect("overview rpc");
    let ov: GetClusterOverviewResp = rkyv_decode(&resp).expect("decode overview");
    assert_eq!(ov.code, CODE_OK, "overview: {}", ov.message);
    ov.ps_servers
}

/// Heartbeat PS 1 and 2 (never 3) for `secs` seconds.
async fn keep_alive_1_2(mgr: &RpcClient, secs: u64) {
    for _ in 0..secs {
        heartbeat(mgr, 1).await;
        heartbeat(mgr, 2).await;
        compio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn connect_leader(addr: SocketAddr) -> std::rc::Rc<RpcClient> {
    let mgr = RpcClient::connect_as(addr, Role::Admin, None)
        .await
        .expect("connect");
    // Lease TTL 10 s plus the election tick. The successor seeds a fresh
    // heartbeat for every replayed PS, so 1 and 2 survive the wait.
    for _ in 0..40 {
        let resp = mgr
            .call(MSG_GET_CLUSTER_OVERVIEW, bytes::Bytes::new())
            .await
            .expect("overview rpc");
        let ov: GetClusterOverviewResp = rkyv_decode(&resp).expect("decode");
        if ov.code == CODE_OK {
            return mgr;
        }
        compio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("manager at {addr} never became leader");
}

#[test]
#[ignore] // requires a real etcd binary on PATH
fn an_evicted_ps_stays_expected_across_a_leader_change_until_removed() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        let mgr1_addr = pick_addr();
        let mgr1_flag = start_stoppable_etcd_manager(mgr1_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_secs(2)).await;
        let mgr1 = RpcClient::connect_as(mgr1_addr, Role::Admin, None)
            .await
            .expect("connect mgr1");

        for ps_id in [1, 2, 3] {
            let r = register(&mgr1, ps_id).await;
            assert_eq!(r.code, CODE_OK, "register ps {ps_id}: {}", r.message);
        }
        // PS 3 goes silent; the 10 s eviction drops it from the live registry.
        keep_alive_1_2(&mgr1, 14).await;

        let servers = ps_servers(&mgr1).await;
        let ids: Vec<u64> = servers.iter().map(|p| p.ps_id).collect();
        assert_eq!(ids, vec![1, 2, 3], "the evicted PS must still be listed");
        let ps3 = &servers[2];
        assert!(ps3.evicted_at_ms > 0, "ps 3 not marked evicted: {ps3:?}");
        assert_eq!(ps3.last_heartbeat_secs_ago, u64::MAX);
        assert!(!ps3.ready());
        assert!(servers[0].evicted_at_ms == 0 && servers[1].evicted_at_ms == 0);
        assert!(servers.iter().all(|p| p.joined_at_ms > 0));
        let evicted_at = ps3.evicted_at_ms;

        let r = remove(&mgr1, 1).await;
        assert_eq!(r.code, CODE_PRECONDITION, "a live PS must not be removable: {}", r.message);
        let r = remove(&mgr1, 9).await;
        assert_eq!(r.code, CODE_NOT_FOUND, "{}", r.message);

        // Leader change: the successor knows the fleet from etcd alone.
        let mgr2_addr = pick_addr();
        let _mgr2_flag = start_stoppable_etcd_manager(mgr2_addr, etcd_endpoint.clone());
        mgr1_flag.store(true, Ordering::Release);
        let mgr2 = connect_leader(mgr2_addr).await;
        keep_alive_1_2(&mgr2, 2).await;

        let servers = ps_servers(&mgr2).await;
        let ids: Vec<u64> = servers.iter().map(|p| p.ps_id).collect();
        assert_eq!(ids, vec![1, 2, 3], "membership must survive the leader change");
        assert_eq!(servers[2].evicted_at_ms, evicted_at);

        let r = remove(&mgr2, 3).await;
        assert_eq!(r.code, CODE_OK, "remove evicted ps 3: {}", r.message);
        let ids: Vec<u64> = ps_servers(&mgr2).await.iter().map(|p| p.ps_id).collect();
        assert_eq!(ids, vec![1, 2]);
        let r = remove(&mgr2, 3).await;
        assert_eq!(r.code, CODE_NOT_FOUND, "{}", r.message);

        let etcd = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("etcd connect");
        assert!(etcd.get("psMembers/3").await.expect("get").kvs.is_empty());
        assert!(!etcd.get("psMembers/1").await.expect("get").kvs.is_empty());

        // A removed id that starts again simply rejoins.
        let r = register(&mgr2, 3).await;
        assert_eq!(r.code, CODE_OK, "{}", r.message);
        let servers = ps_servers(&mgr2).await;
        assert_eq!(servers.len(), 3);
        assert_eq!(servers[2].evicted_at_ms, 0);
    });
}
