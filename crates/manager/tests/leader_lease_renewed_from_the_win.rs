//! The leader lease is renewed from the moment the process leads, not only
//! after the rest of the takeover. `imprint_cluster_id` runs after
//! `set_leader(true)` and makes etcd calls bounded at 10 s each; while the
//! renewal loop started only after it, a slow or held imprint left the
//! process leading with nobody renewing: the lease expired under it. The
//! imprint's write is held here by a failpoint. Needs `etcd` on `PATH`.

mod support;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use autumn_manager::{
    AutumnManager, CLUSTER_ID_KEY, FENCED_TXN_TEST_HOLD_ARMED, FENCED_TXN_TEST_HOLD_PREFIX,
    FENCED_TXN_TEST_PARKED,
};
use support::{pick_addr, start_etcd};

const LEADER_KEY: &[u8] = b"autumn-rs/stream-manager/leader";

#[test]
fn the_lease_is_renewed_while_the_takeover_finishes() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd, etcd_url) = start_etcd().await;
        let etcd = autumn_etcd::EtcdClient::connect(&etcd_url)
            .await
            .expect("etcd");
        *FENCED_TXN_TEST_HOLD_PREFIX.lock().unwrap() = CLUSTER_ID_KEY.to_string();
        FENCED_TXN_TEST_HOLD_ARMED.store(true, Ordering::Relaxed);

        let addr = pick_addr();
        let url = etcd_url.clone();
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                let m = AutumnManager::new_with_etcd(vec![url], support::manager_identity())
                    .await
                    .expect("manager");
                if let Err(e) = m.serve(addr).await {
                    eprintln!("manager serve: {e:#}");
                }
            });
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while FENCED_TXN_TEST_PARKED.load(Ordering::Relaxed) == 0 {
            assert!(
                Instant::now() < deadline,
                "the imprint never reached the failpoint"
            );
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        let parked = Instant::now();

        // The lease (TTL 10 s) was granted before the replay: unrenewed, it
        // is gone by now.
        compio::time::sleep(Duration::from_secs(13)).await;
        let held = etcd.get(LEADER_KEY).await.expect("read leader key");
        FENCED_TXN_TEST_HOLD_ARMED.store(false, Ordering::Relaxed);
        assert_eq!(
            held.kvs.len(),
            1,
            "leader key expired {:?} into a held takeover: nobody renewed its lease",
            parked.elapsed()
        );
    });
}
