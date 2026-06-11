//! BUG-LEASE-3 (P0 #3, coco arch review 2026-06-05) — wire-level
//! end-to-end: drive `lease::poll_invalidations` against a real
//! manager + use the daemon/fuse-side eviction helpers to verify
//! that a force-revoke push leads to IMMEDIATE eviction of the
//! revoked writer's `held_leases` (not the 5 s-deferred eviction
//! that the pre-fix poll loop produced).
//!
//! The actual daemon binary's poll loop is binary-private, so this
//! test stops short of running it. Instead it does the work the
//! loop does — poll for events + apply `apply_invalidation` +
//! call `evict_revoked_held_leases` (fuse) — and asserts on the
//! held_leases delta. This is enough to catch a regression that
//! removes the eviction wiring, since the pure-fn unit tests cover
//! the contract and this test verifies the WIRE event actually
//! carries `LEASE_INVAL_LEASE_REVOKED` for the force-revoke case
//! (so the helper has something to fire on).

mod support;

use std::collections::HashMap;
use std::time::Duration;

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_FUSE, LEASE_INVAL_LEASE_REVOKED, LEASE_MODE_WRITE,
};

use autumn_fuse::dispatch::evict_revoked_held_leases;
use autumn_fuse::state::FuseLease;

use support::pick_addr;

fn start_manager(addr: std::net::SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

fn cid(byte: u8, host: &str) -> DaemonClientId {
    DaemonClientId::from_wire(MgrClientId {
        kind: LEASE_CLIENT_KIND_FUSE,
        uuid: [byte; 16],
        host: host.to_string(),
    })
}

#[test]
fn force_revoke_event_is_lease_revoked_kind_and_evict_fn_clears_held_lease() {
    // Wire round-trip: writer acquires; preempter force-acquires
    // past the grace window; manager pushes LEASE_INVAL_LEASE_REVOKED
    // to the writer; the writer's poll surfaces it; the eviction
    // helper clears held_leases. Pre-fix the poll loop never called
    // the helper → held_leases stayed populated until heartbeat.
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_w = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_p = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let writer = cid(0xa1, "writer");
        let preempter = cid(0xa2, "preempter");
        let ino = 7000u64;

        // Writer acquires write lease.
        let info = match lease::acquire(&cluster_w, &writer, ino, LEASE_MODE_WRITE)
            .await
            .unwrap()
        {
            AcquireResult::Granted(info) => info,
            other => panic!("acquire: {other:?}"),
        };
        // Drain any initial events on the writer's inbox so the
        // post-revoke poll sees ONLY the LeaseRevoked push.
        let _ = lease::poll_invalidations(&cluster_w, &writer)
            .await
            .unwrap();

        // Populate the writer's local held_leases map (as the
        // production daemon/fuse Open arm would have done).
        let mut held: HashMap<u64, FuseLease> = HashMap::new();
        held.insert(
            ino,
            FuseLease {
                mode: LEASE_MODE_WRITE,
                refcount: 1,
                lease_epoch: info.version,
                revoked: false,
            },
        );

        // Preempter force-acquires + waits past the grace window so
        // the manager force-revokes. `acquire_with_preempt_wait`
        // does the retry loop for us (default grace = 5 s).
        let r = lease::acquire_with_preempt_wait(
            &cluster_p,
            &preempter,
            ino,
            LEASE_MODE_WRITE,
            8_000,
        )
        .await
        .unwrap();
        assert!(
            matches!(r, AcquireResult::Granted(_)),
            "preempter must end up granted: {r:?}"
        );

        // Writer's NEXT poll surfaces the LeaseRevoked event.
        let events = lease::poll_invalidations(&cluster_w, &writer)
            .await
            .unwrap();
        let revoked: Vec<_> = events
            .iter()
            .filter(|e| e.kind == LEASE_INVAL_LEASE_REVOKED && e.ino == ino)
            .collect();
        assert!(
            !revoked.is_empty(),
            "force-revoke must push LeaseRevoked to the deposed writer; events={events:?}"
        );

        // Apply the eviction helper (the BUG-LEASE-3 fix). Pre-fix
        // the loop did NOT call this; post-fix it does.
        // R2-P0 #2/#3 (2026-06-06): the helper now MARKS the entry
        // as revoked rather than removing it — so Release can still
        // flush before drop, and Write can fast-fail with EIO.
        let newly_revoked = evict_revoked_held_leases(&events, &mut held);
        assert_eq!(newly_revoked, vec![ino]);
        assert!(
            held.contains_key(&ino),
            "R2-P0 #2/#3: held_leases entry must STAY (marker-not-remove)"
        );
        assert!(
            held.get(&ino).unwrap().revoked,
            "R2-P0 #2/#3: held_leases[ino].revoked must be true after the helper runs"
        );
    });
}
