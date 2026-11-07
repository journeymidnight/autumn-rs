//! A persisted auto-policy mode this build does not have refuses leadership.
//!
//! Mode 1 was the observe mode, removed: a manager must not read it as off or
//! armed (either is a guess about what the operator wanted), so replay fails
//! and names the converter (`migratev1_v2`), which turns it off. A config the
//! converter wrote (mode 0) loads, with its policy still selected.
//!
//! Ablation: `AutoPolicyMode::from_u8(1)` answering a mode lets the manager
//! start on the mode-1 config.

mod support;

use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{rkyv_encode, MgrAutoPolicyConfig};
use support::start_etcd;

fn config(mode: u8) -> Vec<u8> {
    rkyv_encode(&MgrAutoPolicyConfig {
        ver: 1,
        mode,
        active: "gc-only".to_string(),
        policies: vec![],
    })
    .to_vec()
}

#[test]
fn a_persisted_observe_mode_refuses_leadership_until_converted() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;
        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("aux etcd client");
        aux.put(b"autoPolicy/config".as_slice(), config(1).as_slice())
            .await
            .expect("seed mode 1");

        let res =
            AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()], support::manager_identity())
                .await;
        let Err(e) = res else {
            panic!("a manager led on a persisted observe mode");
        };
        let msg = format!("{e:#}");
        assert!(msg.contains("autoPolicy/config mode 1"), "{msg}");
        assert!(msg.contains("migratev1_v2"), "{msg}");

        // What the converter writes.
        aux.put(b"autoPolicy/config".as_slice(), config(0).as_slice())
            .await
            .expect("write mode 0");
        AutumnManager::new_with_etcd(vec![etcd_endpoint], support::manager_identity())
            .await
            .expect("a converted config loads");
    });
}
