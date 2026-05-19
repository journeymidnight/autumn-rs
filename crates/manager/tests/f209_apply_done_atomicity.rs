//! F209-E: F207 invariant I3 — `apply_ec_conversion_done` bundles the
//! `extents/<id>` put and the `extent_inflight/<id>` delete into one
//! etcd transaction. Either both effects land or neither does.
//!
//! Asserts:
//! 1. Success path: after a successful apply, etcd has the new
//!    `extents/<id>` AND no `extent_inflight/<id>`.
//! 2. Failure path: with a deposed leader (F149 fence breaks), the
//!    txn is atomically rejected — etcd shows the original
//!    `extent_inflight/<id>` still present AND no `extents/<id>` write.
//!
//! Requires the `etcd` binary on `$PATH` (or override via
//! `AUTUMN_TEST_ETCD_BIN`). Marked `#[ignore]` per repo convention.

mod support;

use autumn_manager::extent_inflight::{ExtentOpPayload, EXTENT_INFLIGHT_PREFIX};
use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{MgrEcDispatchInflight, MgrExtentInfo};

use support::start_etcd;

const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";

fn extent_inflight_key(eid: u64) -> String {
    format!("{}{}", EXTENT_INFLIGHT_PREFIX, eid)
}

fn make_pre_ec_extent(extent_id: u64) -> MgrExtentInfo {
    // K=3, M=0 pre-EC (replicates only). Post-apply will set K=3, M=1.
    MgrExtentInfo {
        extent_id,
        replicates: vec![1, 3, 5],
        parity: vec![],
        eversion: 3,
        refs: 1,
        vp_table_refs: 0,
        sealed_length: 4096,
        avali: 0x7,
        replicate_disks: vec![10, 30, 50],
        parity_disks: vec![],
        ec_converted: false,
    }
}

fn make_dispatch_record(extent_id: u64) -> MgrEcDispatchInflight {
    MgrEcDispatchInflight {
        extent_id,
        target_nodes: vec![1, 3, 5, 7],
        extra_disk_ids: vec![70],
        data_shards: 3,
        new_eversion: 4,
    }
}

#[test]
#[ignore] // requires embedded etcd (go runtime)
fn f209_e_apply_ec_conversion_done_atomic_success() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        // Manager M1 — becomes leader on construction.
        let m = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
            .await
            .expect("manager with etcd");

        // Pre-populate the in-memory extent. `apply_ec_conversion_done`
        // reads from in-memory and writes the updated copy to etcd.
        let extent_id: u64 = 4209;
        m.store
            .inner
            .borrow_mut()
            .extents
            .insert(extent_id, make_pre_ec_extent(extent_id));

        // Acquire the ConvertToEc marker — this writes
        // `extent_inflight/<id>` to etcd under the F149 fence.
        m.acquire_extent_inflight(
            extent_id,
            ExtentOpPayload::ConvertToEc(make_dispatch_record(extent_id)),
        )
        .await
        .expect("acquire marker");

        // Sanity: the marker is in etcd.
        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("aux etcd client");
        let pre_marker = aux
            .get(extent_inflight_key(extent_id).as_bytes())
            .await
            .expect("get marker");
        assert!(
            pre_marker.kvs.iter().any(|kv| kv.key
                == extent_inflight_key(extent_id).into_bytes()),
            "F207: extent_inflight marker must be in etcd after acquire"
        );

        // Apply the conversion — the atomic put-and-delete txn.
        m.apply_ec_conversion_done(extent_id, vec![1, 3, 5, 7], vec![70], 3, 4)
            .await
            .expect("apply");

        // After apply: both effects must have landed.
        let post_marker = aux
            .get(extent_inflight_key(extent_id).as_bytes())
            .await
            .expect("get marker post");
        assert!(
            !post_marker
                .kvs
                .iter()
                .any(|kv| kv.key == extent_inflight_key(extent_id).into_bytes()),
            "F207 I3: extent_inflight marker must be deleted by apply"
        );

        let post_extent = aux
            .get(format!("extents/{}", extent_id).as_bytes())
            .await
            .expect("get extents/<id>");
        assert!(
            post_extent
                .kvs
                .iter()
                .any(|kv| kv.key == format!("extents/{}", extent_id).into_bytes()),
            "F207 I3: extents/<id> must be written by apply"
        );
    });
}

#[test]
#[ignore] // requires embedded etcd (go runtime)
fn f209_e_apply_ec_conversion_done_atomic_failure_under_deposed_leader() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        let m = AutumnManager::new_with_etcd(vec![etcd_endpoint.clone()])
            .await
            .expect("manager with etcd");

        let extent_id: u64 = 4210;
        m.store
            .inner
            .borrow_mut()
            .extents
            .insert(extent_id, make_pre_ec_extent(extent_id));

        m.acquire_extent_inflight(
            extent_id,
            ExtentOpPayload::ConvertToEc(make_dispatch_record(extent_id)),
        )
        .await
        .expect("acquire marker");

        // Externally depose M1 — overwrite the leader key with a different
        // instance_id. F149 fence on the next etcd write txn (from M1)
        // will fail; the whole apply txn must atomically reject.
        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("aux etcd client");
        let _ = aux.delete(LEADER_KEY.as_bytes()).await;
        aux.put(LEADER_KEY.as_bytes(), b"f209e-impostor")
            .await
            .expect("overwrite leader key");

        // M1 still believes it's the leader (cell hasn't flipped). The
        // apply must fail atomically — neither effect lands.
        let res = m
            .apply_ec_conversion_done(extent_id, vec![1, 3, 5, 7], vec![70], 3, 4)
            .await;
        assert!(
            res.is_err(),
            "F149 + F207 I3: apply must fail atomically under deposed leader; got {res:?}"
        );

        // Marker still in etcd (delete didn't land).
        let post_marker = aux
            .get(extent_inflight_key(extent_id).as_bytes())
            .await
            .expect("get marker post");
        assert!(
            post_marker
                .kvs
                .iter()
                .any(|kv| kv.key == extent_inflight_key(extent_id).into_bytes()),
            "F207 I3: marker must survive a fence-rejected apply (atomicity)"
        );

        // `extents/<id>` was never written (no pre-test put on this
        // extent), so the failed apply must not have created it.
        let post_extent = aux
            .get(format!("extents/{}", extent_id).as_bytes())
            .await
            .expect("get extents/<id>");
        assert!(
            !post_extent
                .kvs
                .iter()
                .any(|kv| kv.key == format!("extents/{}", extent_id).into_bytes()),
            "F207 I3: extents/<id> must not be written when apply txn fence-fails"
        );
    });
}
