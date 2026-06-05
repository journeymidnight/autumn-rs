//! BUG-LEASE-4 (P1 #4, coco arch review 2026-06-05; fix 2026-06-06)
//! — wire-level end-to-end for the deferred-push 2PC.
//!
//! The handler-level scenario this verifies:
//! 1. Writer A acquires writer lease (etcd record present).
//! 2. Writer B force-acquires through the grace window; the
//!    manager's in-memory `acquire_with_force_deferred` bumps
//!    version + clears A's writer slot + grants B + STAGES
//!    LeaseRevoked pushes for A.
//! 3. Manager writes B's record to etcd. Two paths:
//!    3a. Etcd OK → `flush_deferred_pushes` lands the LeaseRevoked
//!        event in A's inbox. (This is the happy path; A's next
//!        long-poll sees it.) Test #1 below.
//!    3b. Etcd FAIL (hard to deterministically induce here without
//!        manager-internal hooks; pure-fn tests in `inode_lease`
//!        module assert the revert path).
//!
//! What this wire test guarantees beyond the pure-fn tests:
//! - The MSG_ACQUIRE_LEASE handler's deferred-push integration
//!   is correctly wired: an end-to-end force-revoke STILL
//!   delivers LeaseRevoked to A (the integration didn't drop the
//!   event on the floor in the happy path while restructuring).
//! - The wire codes are unchanged: a granted force-acquire still
//!   returns CODE_OK with the new lease info, not CODE_REVOKE_PENDING
//!   or some intermediate state from the 2PC split.

mod support;

use std::time::Duration;

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_FUSE, LEASE_INVAL_LEASE_REVOKED,
    LEASE_INVAL_WILL_REVOKE_IN, LEASE_MODE_WRITE,
};

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

/// Happy-path 2PC: deferred-push integration MUST still deliver
/// LeaseRevoked to the deposed writer when etcd commit succeeds.
/// Regression guard for the BUG-LEASE-4 refactor: if a future edit
/// forgets to call `flush_deferred_pushes` on the Granted path,
/// this test would catch the silent event loss.
#[test]
fn bug_lease_4_force_revoke_still_delivers_lease_revoked_after_2pc() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_a = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_b = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let writer_a = cid(0xb1, "a");
        let writer_b = cid(0xb2, "b");
        let ino = 8001u64;

        // A acquires writer lease.
        let acq = lease::acquire(&cluster_a, &writer_a, ino, LEASE_MODE_WRITE)
            .await
            .unwrap();
        assert!(matches!(acq, AcquireResult::Granted(_)), "A acquire: {acq:?}");
        // Drain A's initial events.
        let _ = lease::poll_invalidations(&cluster_a, &writer_a)
            .await
            .unwrap();

        // B force-acquires through the grace window. Default grace
        // is 5 s; 8 s timeout gives the retry loop room.
        let r = lease::acquire_with_preempt_wait(
            &cluster_b, &writer_b, ino, LEASE_MODE_WRITE, 8_000,
        )
        .await
        .unwrap();
        assert!(
            matches!(r, AcquireResult::Granted(_)),
            "B force-acquire must end Granted: {r:?}"
        );

        // A's next poll must surface the LeaseRevoked event — proves
        // `flush_deferred_pushes` ran on the Granted path of the 2PC.
        // (Pre-refactor, this event would have landed inline; the
        // refactor MUST not have orphaned it in the deferred bundle.)
        let events = lease::poll_invalidations(&cluster_a, &writer_a)
            .await
            .unwrap();
        let revoked: Vec<_> = events
            .iter()
            .filter(|e| e.kind == LEASE_INVAL_LEASE_REVOKED && e.ino == ino)
            .collect();
        assert_eq!(
            revoked.len(),
            1,
            "BUG-LEASE-4: deposed writer A MUST see exactly one LeaseRevoked after B's force-revoke commits: events={events:?}"
        );
        assert!(
            revoked[0].version > 0,
            "LeaseRevoked must carry the post-bump version"
        );
    });
}

/// RevokePending path (grace started but not yet expired) must
/// STILL deliver WillRevokeIn to the current writer. This arm
/// flushes deferred pushes directly (no etcd write between stage
/// and flush). Regression guard for that integration.
#[test]
fn bug_lease_4_revoke_pending_delivers_will_revoke_in() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_a = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_b = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let writer_a = cid(0xc1, "a");
        let writer_b = cid(0xc2, "b");
        let ino = 8002u64;

        let _ = lease::acquire(&cluster_a, &writer_a, ino, LEASE_MODE_WRITE)
            .await
            .unwrap();
        // Drain A's initial events.
        let _ = lease::poll_invalidations(&cluster_a, &writer_a)
            .await
            .unwrap();

        // B's first force-acquire (no wait — sees RevokePending).
        let r = lease::acquire_force(&cluster_b, &writer_b, ino, LEASE_MODE_WRITE)
            .await
            .unwrap();
        match r {
            AcquireResult::RevokePending { .. } => {}
            other => panic!("expected RevokePending, got {other:?}"),
        }

        // A's poll MUST see WillRevokeIn — the RevokePending arm
        // of the handler flushes the deferred push directly.
        let events = lease::poll_invalidations(&cluster_a, &writer_a)
            .await
            .unwrap();
        let will: Vec<_> = events
            .iter()
            .filter(|e| e.kind == LEASE_INVAL_WILL_REVOKE_IN && e.ino == ino)
            .collect();
        assert_eq!(
            will.len(),
            1,
            "BUG-LEASE-4: WillRevokeIn must reach A's inbox on the RevokePending arm: events={events:?}"
        );
    });
}
