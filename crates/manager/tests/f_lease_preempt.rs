//! F-lease-preempt — force-revoke / writer revoke protocol.
//! End-to-end via the real `AcquireLease` RPC + the
//! `autumn-client::lease` helpers.
//!
//! Plan §5 Phase 3: `AcquireLease(force=true)` against a held
//! writer pushes `WillRevokeIn { grace }` to the writer and
//! returns `RevokePending`. After the grace window the next
//! force-acquire force-revokes (bumps version, pushes
//! LeaseRevoked) and is granted.
//!
//! Tests use a 200 ms revoke grace at the registry level (would
//! need `set_revoke_grace` on AutumnManager to override per
//! test); since the manager is constructed via
//! `AutumnManager::new()` which uses the 5s default, we use
//! `acquire_with_preempt_wait` with a budget that comfortably
//! covers the grace window.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_FUSE, LEASE_INVAL_LEASE_REVOKED, LEASE_INVAL_WILL_REVOKE_IN,
    LEASE_MODE_WRITE,
};

fn pick_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn start_manager(addr: SocketAddr) {
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
fn force_acquire_pushes_will_revoke_to_current_writer() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_w = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_p = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let w = cid(0x11, "writer");
        let p = cid(0x22, "preempter");

        // Drain w's initial-empty events so the next poll observes
        // only the WillRevokeIn we're about to push.
        let _ = lease::acquire(&cluster_w, &w, 50, LEASE_MODE_WRITE)
            .await
            .unwrap();
        let _ = lease::poll_invalidations(&cluster_w, &w).await.unwrap();

        // Preempter force-acquires → RevokePending + WillRevokeIn
        // pushed to w.
        let r = lease::acquire_force(&cluster_p, &p, 50, LEASE_MODE_WRITE)
            .await
            .unwrap();
        match r {
            AcquireResult::RevokePending { eta_ms, .. } => {
                assert!(eta_ms > 0, "eta_ms must be positive");
                assert!(eta_ms <= 5_000, "eta_ms must fit the default grace");
            }
            other => panic!("expected RevokePending, got {other:?}"),
        }

        // w's next poll surfaces WillRevokeIn.
        let events = lease::poll_invalidations(&cluster_w, &w).await.unwrap();
        let wri = events
            .iter()
            .find(|e| e.kind == LEASE_INVAL_WILL_REVOKE_IN)
            .expect("WillRevokeIn must be pushed to the current writer");
        assert_eq!(wri.ino, 50);
        assert!(wri.version > 0, "version carries grace ms; must be > 0");
    });
}

#[test]
fn voluntary_release_within_grace_lets_preempter_acquire_cleanly() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_w = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_p = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let w = cid(0xa1, "writer");
        let p = cid(0xa2, "preempter");

        let v_before = match lease::acquire(&cluster_w, &w, 60, LEASE_MODE_WRITE)
            .await
            .unwrap()
        {
            AcquireResult::Granted(info) => info.version,
            other => panic!("{other:?}"),
        };

        // Start the grace window.
        let r1 = lease::acquire_force(&cluster_p, &p, 60, LEASE_MODE_WRITE)
            .await
            .unwrap();
        assert!(matches!(r1, AcquireResult::RevokePending { .. }));

        // w voluntarily releases.
        let new_v = lease::release(&cluster_w, &w, 60)
            .await
            .unwrap()
            .expect("writer-close must return new_version");
        assert_eq!(new_v, v_before + 1);

        // Preempter retries → Granted (no force-revoke needed).
        let r2 = lease::acquire_force(&cluster_p, &p, 60, LEASE_MODE_WRITE)
            .await
            .unwrap();
        match r2 {
            AcquireResult::Granted(info) => {
                assert_eq!(info.version, new_v, "inherits voluntary-release bump");
                assert!(info.writer_present);
            }
            other => panic!("expected Granted, got {other:?}"),
        }
    });
}

#[test]
fn force_acquire_after_grace_expiry_forces_revoke_and_grants() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_w = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_p = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let w = cid(0xb1, "stubborn-writer");
        let p = cid(0xb2, "preempter");

        let v_before = match lease::acquire(&cluster_w, &w, 70, LEASE_MODE_WRITE)
            .await
            .unwrap()
        {
            AcquireResult::Granted(info) => info.version,
            other => panic!("{other:?}"),
        };
        // Drain w's initial events.
        let _ = lease::poll_invalidations(&cluster_w, &w).await.unwrap();

        // Use `acquire_with_preempt_wait` with a budget that covers
        // the default 5s grace + some buffer.
        let start = Instant::now();
        let r = lease::acquire_with_preempt_wait(
            &cluster_p,
            &p,
            70,
            LEASE_MODE_WRITE,
            8_000,
        )
        .await
        .unwrap();
        let elapsed = start.elapsed();

        match r {
            AcquireResult::Granted(info) => {
                assert_eq!(
                    info.version,
                    v_before + 1,
                    "force-revoke must bump version once"
                );
                assert!(info.writer_present);
            }
            other => panic!(
                "expected Granted after grace expiry, got {other:?} (elapsed={elapsed:?})"
            ),
        }
        // Should have waited at least the grace window (~5s default)
        // but not absurdly long.
        assert!(
            elapsed >= Duration::from_secs(4),
            "must wait ~grace window before force-revoke; elapsed={elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(8),
            "must NOT wait > 8s; elapsed={elapsed:?}"
        );

        // Stubborn writer w receives a LeaseRevoked push (it never
        // voluntarily released).
        let events = lease::poll_invalidations(&cluster_w, &w).await.unwrap();
        assert!(
            events.iter().any(|e| e.kind == LEASE_INVAL_LEASE_REVOKED && e.ino == 70),
            "stubborn writer must receive LeaseRevoked: events={events:?}"
        );
    });
}

#[test]
fn force_acquire_on_no_writer_grants_immediately() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let p = cid(0xc1, "p");
        // Fresh ino with no writer → force-acquire just grants.
        let r = lease::acquire_force(&cluster, &p, 99, LEASE_MODE_WRITE)
            .await
            .unwrap();
        assert!(matches!(r, AcquireResult::Granted(_)));
    });
}
