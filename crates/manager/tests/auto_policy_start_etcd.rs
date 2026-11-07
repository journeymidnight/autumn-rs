//! `auto-policy start` publishes one complete config: the policy name and the
//! Armed mode land in a single etcd write, so no reader, failure or second
//! operator can see the policy selected but not armed, or armed under the old
//! name. A start naming no policy writes nothing.
//!
//! Ablation: `ClusterClient::auto_policy_start` as the two requests it used to
//! be (SET_ACTIVE, then SET_MODE) writes the key twice, the first time with the
//! new name and the old mode.

mod support;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use autumn_client::ClusterClient;
use autumn_rpc::manager_rpc::{rkyv_decode, MgrAutoPolicyConfig};
use support::{pick_addr, start_etcd};

const KEY: &[u8] = b"autoPolicy/config";

fn start_manager(addr: SocketAddr, etcd: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let m = autumn_manager::AutumnManager::new_with_etcd(
                vec![etcd],
                support::manager_identity(),
            )
            .await
            .expect("manager");
            if let Err(e) = m.serve(addr).await {
                eprintln!("manager serve: {e:#}");
            }
        });
    });
}

/// (writes so far, the persisted config). 0 writes = key absent.
async fn persisted(etcd: &autumn_etcd::EtcdClient) -> (i64, Option<MgrAutoPolicyConfig>) {
    let r = etcd.get(KEY).await.expect("etcd get");
    match r.kvs.first() {
        None => (0, None),
        Some(kv) => (kv.version, Some(rkyv_decode(&kv.value).expect("decode config"))),
    }
}

#[test]
fn starting_a_policy_writes_its_name_and_armed_mode_once() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd) = start_etcd().await;
        let mgr = pick_addr();
        start_manager(mgr, etcd.clone());
        let aux = autumn_etcd::EtcdClient::connect(&etcd).await.expect("aux etcd client");
        let start = Instant::now();
        let op = loop {
            let led = match ClusterClient::connect_admin(&mgr.to_string()).await {
                Ok(op) => op.auto_policy_get().await.map(|_| op),
                Err(e) => Err(e),
            };
            match led {
                Ok(op) => break op,
                Err(e) => {
                    assert!(start.elapsed() < Duration::from_secs(20), "manager never led: {e:#}");
                    compio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        };

        let (v0, _) = persisted(&aux).await;
        op.auto_policy_start("gc-only").await.expect("start gc-only");
        let (v1, cfg) = persisted(&aux).await;
        let cfg = cfg.expect("config persisted");
        assert_eq!(v1, v0 + 1, "one start, one write");
        assert_eq!((cfg.active.as_str(), cfg.mode), ("gc-only", 2));

        // Armed already: switching policies is still one write.
        op.auto_policy_start("maintenance").await.expect("start maintenance");
        let (v2, cfg) = persisted(&aux).await;
        let cfg = cfg.expect("config persisted");
        assert_eq!(v2, v1 + 1, "one start, one write");
        assert_eq!((cfg.active.as_str(), cfg.mode), ("maintenance", 2));

        let err = op.auto_policy_start("no-such-policy").await.unwrap_err();
        assert!(format!("{err:#}").contains("no such policy"), "{err:#}");
        assert_eq!(persisted(&aux).await.0, v2, "a refused start wrote");
        let st = op.auto_policy_get().await.unwrap();
        assert_eq!((st.active.as_str(), st.mode), ("maintenance", 2));
    });
}
