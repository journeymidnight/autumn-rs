//! Runtime parameter overrides against a real etcd: `config set` reaches the
//! PS, the EN and the manager itself within a poll, `unset` returns a process
//! to its flag value, a refused key or value changes nothing, and after a
//! leader change the next leader serves the overrides and installs its own.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_client::ClusterClient;
use autumn_params::MIB;
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use support::{pick_addr, start_etcd};

/// An etcd-backed manager on its own thread. Returns its stop flag and a cell
/// mirroring its `recovery_max_global` (the manager's params live inside the
/// thread).
fn start_manager(addr: SocketAddr, etcd: String) -> (Arc<AtomicBool>, Arc<AtomicU64>) {
    let stop = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(AtomicU64::new(0));
    let (stop_t, seen_t) = (stop.clone(), seen.clone());
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let m = autumn_manager::AutumnManager::new_with_etcd(
                vec![etcd],
                support::manager_identity(),
            )
            .await
            .expect("manager");
            let serving = m.clone();
            compio::runtime::spawn(async move {
                if let Err(e) = serving.serve(addr).await {
                    eprintln!("manager serve: {e:#}");
                }
            })
            .detach();
            while !stop_t.load(Ordering::Acquire) {
                seen_t.store(m.params().recovery_max_global.get() as u64, Ordering::Release);
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    (stop, seen)
}

async fn wait_until(what: &str, deadline: Duration, cond: impl Fn() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < deadline, "timed out waiting for {what}");
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn runtime_overrides_reach_every_role_and_survive_a_leader_change() {
    let ps = autumn_partition_server::params();
    let en = autumn_stream::en_params();
    ps.config_poll_secs.set(1);
    en.config_poll_secs.set(1);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd) = start_etcd().await;
        let mgr1 = pick_addr();
        let (stop1, seen1) = start_manager(mgr1, etcd.clone());
        compio::time::sleep(Duration::from_secs(2)).await;
        let mgr2 = pick_addr();
        let (_stop2, seen2) = start_manager(mgr2, etcd.clone());
        let managers = format!("{mgr1},{mgr2}");

        let en_addr = pick_addr();
        let en_dir = tempfile::tempdir().unwrap();
        let en_dir_path = en_dir.path().to_path_buf();
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async move {
                let cfg = ExtentNodeConfig::new(en_dir_path, 1).with_manager_endpoint(mgr1.to_string());
                let n = ExtentNode::new(cfg).await.expect("extent node");
                if let Err(e) = n.serve(en_addr).await {
                    eprintln!("extent node serve: {e:#}");
                }
            });
        });
        let ps_addr = pick_addr();
        let ps_managers = managers.clone();
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async move {
                let s = autumn_partition_server::PartitionServer::connect_with_advertise_and_port(
                    1,
                    &ps_managers,
                    Some(ps_addr.to_string()),
                    ps_addr,
                )
                .await
                .expect("partition server");
                if let Err(e) = s.serve(ps_addr).await {
                    eprintln!("partition server serve: {e:#}");
                }
            });
        });

        let op = ClusterClient::connect_admin(&managers).await.expect("admin client");
        assert_eq!(ps.gc_rate_bytes_per_sec.get(), 64 * MIB);

        // Refused: not a number, not a runtime parameter, out of range, and a
        // value that breaks a cross-field check (max files below min files).
        for (key, value) in [
            ("ps.gc-rate-bytes-per-sec", "fast"),
            ("en.fd-cache-cap", "100"),
            ("ps.no-such-knob", "1"),
            ("en.recovery-parallelism", "0"),
            ("ps.compact-max-files", "2"),
        ] {
            let r = op.runtime_config_set(key, Some(value.into()), "test").await;
            assert!(r.is_err(), "{key} = {value} was accepted");
        }
        assert!(op.runtime_config_get().await.unwrap().is_empty());

        op.runtime_config_set("ps.gc-rate-bytes-per-sec", Some("32M".into()), "alice")
            .await
            .unwrap();
        op.runtime_config_set("en.recovery-parallelism", Some("5".into()), "alice")
            .await
            .unwrap();
        op.runtime_config_set("manager.recovery-max-global", Some("7".into()), "alice")
            .await
            .unwrap();
        let t = Duration::from_secs(10);
        wait_until("PS override", t, || ps.gc_rate_bytes_per_sec.get() == 32 * MIB).await;
        wait_until("EN override", t, || en.recovery_parallelism.get() == 5).await;
        wait_until("manager override", t, || seen1.load(Ordering::Acquire) == 7).await;
        let listed = op.runtime_config_get().await.unwrap();
        assert_eq!(listed.len(), 3);
        assert!(listed.iter().all(|e| e.set_by == "alice" && e.set_at > 0));

        // Unset: back to the flag value.
        op.runtime_config_set("en.recovery-parallelism", None, "alice")
            .await
            .unwrap();
        wait_until("EN back to its flag", t, || en.recovery_parallelism.get() == 2).await;
        assert!(
            op.runtime_config_set("en.recovery-parallelism", None, "alice").await.is_err(),
            "unsetting a key with no override must say so"
        );

        // Every set / unset is audited, refused values included.
        let audit = op
            .mgr_call(
                autumn_rpc::manager_rpc::MSG_QUERY_AUDIT_LOG,
                autumn_rpc::manager_rpc::rkyv_encode(&autumn_rpc::manager_rpc::QueryAuditLogReq {
                    op_filter: autumn_rpc::manager_rpc::AUDIT_OP_RUNTIME_CONFIG,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let audit: autumn_rpc::manager_rpc::QueryAuditLogResp =
            autumn_rpc::manager_rpc::rkyv_decode(&audit).unwrap();
        let reasons: Vec<(&str, u8)> =
            audit.entries.iter().map(|e| (e.reason.as_str(), e.result_code)).collect();
        assert!(reasons.contains(&("ps.gc-rate-bytes-per-sec = 32M", 0)), "{reasons:?}");
        assert!(reasons.contains(&("unset en.recovery-parallelism", 0)), "{reasons:?}");
        assert!(reasons.iter().any(|(r, c)| *r == "ps.compact-max-files = 2" && *c != 0), "{reasons:?}");

        // Leader change. A standby installs the overrides only from its own
        // replay (at startup and on promotion); it does not follow later sets.
        assert_eq!(seen2.load(Ordering::Acquire), 64);
        stop1.store(true, Ordering::Release);
        let t = Duration::from_secs(40);
        wait_until("the standby to install the manager override", t, || {
            seen2.load(Ordering::Acquire) == 7
        })
        .await;
        let start = Instant::now();
        let listed = loop {
            match op.runtime_config_get().await {
                Ok(l) => break l,
                Err(e) => {
                    assert!(start.elapsed() < t, "new leader never answered: {e:#}");
                    compio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        };
        let keys: Vec<&str> = listed.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["manager.recovery-max-global", "ps.gc-rate-bytes-per-sec"]);
        assert_eq!(ps.gc_rate_bytes_per_sec.get(), 32 * MIB, "the PS kept its override");

        // The PS follows the new leader.
        op.runtime_config_set("ps.gc-rate-bytes-per-sec", Some("16M".into()), "bob")
            .await
            .unwrap();
        wait_until("PS override from the new leader", Duration::from_secs(15), || {
            ps.gc_rate_bytes_per_sec.get() == 16 * MIB
        })
        .await;
    });
}
