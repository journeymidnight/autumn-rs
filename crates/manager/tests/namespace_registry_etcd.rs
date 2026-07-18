//! F-KEY-NS D2 (SD-1) — etcd-backed namespace registry: bootstrap
//! pre-registration + replay rehydration across a leader failover, plus the
//! coco-P1 leader-gating of `MSG_GET_AUTHZ_CONFIG` over the real wire.
//!
//! The memory-mode create/delete/reserved/disjoint/bridge/leader-gate
//! behaviours are covered by the in-crate unit tests in
//! `rpc_handlers::namespace_registry_tests` (no etcd needed). These tests pin
//! the parts that ONLY exist with etcd:
//!
//! 1. First leader `seed_builtin_namespaces` CAS-persists `fs`/`kvc`/`mem`
//!    rows under `namespace/`, and an admin `namespace-create` persists its
//!    own row — the etcd keys are present with the expected prefixes.
//! 2. `GET_AUTHZ_CONFIG` is leader-gated: a FOLLOWER refuses (`CODE_NOT_LEADER`)
//!    even though it replayed the registry at startup — so a PS never installs a
//!    follower's config.
//! 3. After the leader is stopped, a SUCCESSOR leader's `replay_from_etcd`
//!    rehydrates the full registry: the promoted manager serves fs/kvc/mem/bench
//!    in `namespaces` and bridges the owned `bench` into `protected_prefixes`.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, GetAuthzConfigResp, NamespaceCreateReq, NamespaceCreateResp, CODE_OK,
    MSG_GET_AUTHZ_CONFIG, MSG_NAMESPACE_CREATE,
};

use support::{pick_addr, start_etcd};

const ADMIN: &str = "admin-secret";

/// Start an etcd-backed manager WITH an admin token, STOPPABLE: dropping the
/// returned flag's runtime (via `.store(true)`) tears down the manager's compio
/// runtime → its leader-keepalive stops → the etcd lease expires → a successor
/// wins the election. Mirrors `support::start_extent_node_stoppable`.
fn start_stoppable_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_thread = flag.clone();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = autumn_manager::AutumnManager::new_with_etcd(vec![etcd_endpoint])
                .await
                .expect("new manager with etcd");
            manager.set_admin_token(ADMIN.to_string());
            compio::runtime::spawn(async move {
                let _ = manager.serve(mgr_addr).await;
            })
            .detach();
            while !flag_thread.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
            // block_on returns → Runtime drops → serve task + keepalive loop
            // cancelled → the etcd lease is no longer renewed and expires.
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    flag
}

async fn ns_create(mgr: &RpcClient, name: &str, owner: Option<&str>) -> NamespaceCreateResp {
    let payload = rkyv_encode(&NamespaceCreateReq {
        admin_token: ADMIN.to_string(),
        name: name.to_string(),
        owner_tenant: owner.map(|s| s.to_string()),
        presplit: Vec::new(),
    });
    let resp = mgr
        .call(MSG_NAMESPACE_CREATE, payload)
        .await
        .expect("namespace-create rpc");
    rkyv_decode(&resp).expect("decode NamespaceCreateResp")
}

async fn authz_config(mgr: &RpcClient) -> GetAuthzConfigResp {
    let resp = mgr
        .call(MSG_GET_AUTHZ_CONFIG, bytes::Bytes::new())
        .await
        .expect("get_authz_config rpc");
    rkyv_decode(&resp).expect("decode GetAuthzConfigResp")
}

#[test]
#[ignore] // requires embedded etcd
fn bootstrap_persists_leader_gates_and_successor_replay_rehydrates() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        // ── First leader: bootstrap seeds fs/kvc/mem, admin creates OWNED bench.
        let mgr1_addr = pick_addr();
        let mgr1_flag = start_stoppable_etcd_manager(mgr1_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_secs(2)).await;
        let mgr1 = RpcClient::connect(mgr1_addr).await.expect("connect mgr1");

        let created = ns_create(&mgr1, "bench", Some("acme")).await;
        assert_eq!(created.code, CODE_OK, "create failed: {}", created.message);

        // The etcd registry rows exist with the expected prefixes.
        let etcd = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("etcd connect");
        for name in ["fs", "kvc", "mem", "bench"] {
            let got = etcd.get(&format!("namespace/{name}")).await.expect("etcd get");
            assert!(!got.kvs.is_empty(), "namespace/{name} not persisted to etcd");
        }

        // Leader serves the registry + bridges the owned namespace.
        let leader_cfg = authz_config(&mgr1).await;
        assert_eq!(leader_cfg.code, CODE_OK, "leader must answer OK");
        assert!(leader_cfg.protected_prefixes.contains(&b"bench/".to_vec()));

        // ── Follower: replays at startup but REFUSES to serve the registry.
        let mgr2_addr = pick_addr();
        let _mgr2_flag = start_stoppable_etcd_manager(mgr2_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_secs(2)).await;
        let mgr2 = RpcClient::connect(mgr2_addr).await.expect("connect mgr2");

        let follower_cfg = authz_config(&mgr2).await;
        assert_eq!(
            follower_cfg.code,
            autumn_rpc::manager_rpc::CODE_NOT_LEADER,
            "follower must refuse GET_AUTHZ_CONFIG (leader-gated)"
        );
        assert!(
            follower_cfg.namespaces.is_empty(),
            "a refused response must carry no registry data"
        );

        // ── Stop mgr1 → lease expires → mgr2 wins election + replays.
        mgr1_flag.store(true, Ordering::Release);
        // Poll mgr2 until it is promoted (lease TTL ~10 s + a 2 s election tick).
        let mut promoted: Option<GetAuthzConfigResp> = None;
        for _ in 0..40 {
            let cfg = authz_config(&mgr2).await;
            if cfg.code == CODE_OK {
                promoted = Some(cfg);
                break;
            }
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        let cfg = promoted.expect("mgr2 was never promoted to leader within ~20 s");

        // Successor leader replayed the FULL registry from etcd.
        for p in [
            b"fs/".to_vec(),
            b"kvc/".to_vec(),
            b"mem/".to_vec(),
            b"bench/".to_vec(),
        ] {
            assert!(cfg.namespaces.contains(&p), "post-replay namespaces missing {p:?}");
        }
        // Owned bench is bridged; existence-only fs is not.
        assert!(
            cfg.protected_prefixes.contains(&b"bench/".to_vec()),
            "owned namespace not bridged after replay"
        );
        assert!(
            !cfg.protected_prefixes.contains(&b"fs/".to_vec()),
            "existence-only family should not be protected"
        );

        // Re-creating bench on the successor is rejected (replay saw it).
        let dup = ns_create(&mgr2, "bench", Some("acme")).await;
        assert_ne!(dup.code, CODE_OK, "duplicate create should fail after replay");
    });
}
