//! `MSG_GET_CLUSTER_STATUS` against a real etcd: every count is measured
//! against the expected members, so a stopped standby, an evicted PS and a
//! registered node that never answered all stay in the denominator.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::version_hello::Role;

use support::{pick_addr, register_node, start_etcd, start_extent_node};

fn start_manager(mgr_addr: SocketAddr, etcd_endpoint: String, id: u64) -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let identity = autumn_manager::ManagerIdentity {
                id,
                address: mgr_addr.to_string(),
            };
            let manager = autumn_manager::AutumnManager::new_with_etcd(vec![etcd_endpoint], identity)
                .await
                .expect("new manager with etcd");
            compio::runtime::spawn(async move {
                let _ = manager.serve(mgr_addr).await;
            })
            .detach();
            while !stop_t.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    stop
}

async fn status(mgr: &RpcClient) -> ClusterStatusResp {
    let resp = mgr
        .call(MSG_GET_CLUSTER_STATUS, bytes::Bytes::new())
        .await
        .expect("status rpc");
    rkyv_decode(&resp).expect("decode ClusterStatusResp")
}

async fn ps_call(mgr: &RpcClient, msg: u8, payload: Vec<u8>) {
    let resp = mgr.call(msg, payload.into()).await.expect("rpc");
    let r: CodeResp = rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, CODE_OK, "{}", r.message);
}

fn states(v: &[FleetMember]) -> Vec<(u64, u8)> {
    v.iter().map(|m| (m.id, m.state)).collect()
}

#[test]
#[ignore] // requires a real etcd binary on PATH
fn status_counts_against_the_expected_fleet() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, endpoint) = start_etcd().await;
        let a_addr = pick_addr();
        let _a = start_manager(a_addr, endpoint.clone(), 1);
        compio::time::sleep(Duration::from_secs(2)).await;
        let b_addr = pick_addr();
        let b_stop = start_manager(b_addr, endpoint.clone(), 2);
        compio::time::sleep(Duration::from_secs(1)).await;
        let a = RpcClient::connect_as(a_addr, Role::Admin, None)
            .await
            .expect("connect A");
        let b = RpcClient::connect_as(b_addr, Role::Admin, None)
            .await
            .expect("connect B");
        assert_eq!(status(&b).await.code, CODE_NOT_LEADER, "a standby must not answer");

        // One extent node that answers, one registered that never does.
        let en_dir = tempfile::tempdir().expect("en dir");
        let en_addr = pick_addr();
        start_extent_node(en_addr, en_dir.path().to_path_buf(), 1);
        register_node(&a, &en_addr.to_string(), "uuid-status-1").await;
        register_node(&a, &pick_addr().to_string(), "uuid-status-2").await;

        for ps_id in [1u64, 2, 3] {
            let req = RegisterPsReq {
                ps_id,
                address: format!("127.0.0.1:{}", 19100 + ps_id),
                slot_cap: 0,
            };
            ps_call(&a, MSG_REGISTER_PS, rkyv_encode(&req).to_vec()).await;
        }
        // PS 3 goes silent and is evicted after 10 s.
        for _ in 0..13 {
            for ps_id in [1u64, 2] {
                let req = HeartbeatPsReq {
                    ps_id,
                    slot_cap: 0,
                    open_parts: Vec::new(),
                };
                ps_call(&a, MSG_HEARTBEAT_PS, rkyv_encode(&req).to_vec()).await;
            }
            compio::time::sleep(Duration::from_secs(1)).await;
        }

        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let s = status(&a).await;
        assert_eq!(s.code, CODE_OK, "{}", s.message);
        assert!((s.sampled_at_ms - before).abs() < 5_000, "sampled_at_ms {}", s.sampled_at_ms);
        assert_eq!(
            states(&s.managers),
            vec![(1, FLEET_MANAGER_LEADER), (2, FLEET_MANAGER_STANDBY)]
        );
        assert_eq!(
            states(&s.partition_servers),
            vec![(1, FLEET_PS_READY), (2, FLEET_PS_READY), (3, FLEET_PS_EVICTED)]
        );
        let en: Vec<u8> = s.extent_nodes.iter().map(|m| m.state).collect();
        assert_eq!(en.len(), 2, "both registered nodes are expected");
        assert_eq!(
            en.iter().filter(|st| **st == FLEET_EN_ONLINE).count(),
            1,
            "{:?}",
            s.extent_nodes
        );
        assert_eq!(s.unavailable, 0);
        assert_eq!(s.recovery_inflight, 0);

        // The standby stops: still expected, now absent.
        b_stop.store(true, Ordering::Release);
        let mut absent = false;
        for _ in 0..20 {
            for ps_id in [1u64, 2] {
                let req = HeartbeatPsReq {
                    ps_id,
                    slot_cap: 0,
                    open_parts: Vec::new(),
                };
                ps_call(&a, MSG_HEARTBEAT_PS, rkyv_encode(&req).to_vec()).await;
            }
            let s = status(&a).await;
            if states(&s.managers) == vec![(1, FLEET_MANAGER_LEADER), (2, FLEET_MANAGER_ABSENT)] {
                absent = true;
                break;
            }
            compio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(absent, "the stopped standby must be listed as absent");
    });
}
