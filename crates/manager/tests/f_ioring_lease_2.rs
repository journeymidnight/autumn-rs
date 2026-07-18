//! F-ioring-lease-2 — daemon-side client surface end-to-end. Drives
//! the `autumn_client::lease` helpers (used inside the daemon's
//! Open/Close arms) against an in-process AutumnManager + the real
//! `ClusterClient::mgr_call_retry` transport.
//!
//! Covers what F-ioring-lease-1's RPC-only tests can't:
//! 1. Two simulated daemons (distinct `DaemonClientId`s) attempting
//!    a WRITE lease on the same inode — second returns
//!    `AcquireResult::Conflict` (this is what the daemon maps to
//!    `libc::EBUSY` on the Open SQE).
//! 2. Concurrent READ leases from two daemons on the same inode
//!    succeed — read-read does not conflict (writer-not-present).
//! 3. After the first writer releases, a second daemon can acquire
//!    and version monotonically bumps (close-to-open marker).
//! 4. Heartbeat round-trips against a held lease and returns
//!    `NotHeld` after a release (the path used by the daemon's
//!    session_heartbeat_loop to detect external revocation).
//!
//! Multi-daemon coherence over actual ring buffers + cache
//! invalidation lands in F-ioring-lease-4.

use std::net::SocketAddr;
use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_IORING, LEASE_MODE_READ, LEASE_MODE_WRITE,
};

use autumn_client::lease::{self, AcquireResult, DaemonClientId, HeartbeatResult};

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
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

/// Build a `DaemonClientId` with a fixed UUID so test assertions
/// can reason about identity without flakiness from random UUIDs.
fn cid(byte: u8, host: &str) -> DaemonClientId {
    DaemonClientId::from_wire(MgrClientId {
        kind: LEASE_CLIENT_KIND_IORING,
        uuid: [byte; 16],
        host: host.to_string(),
    })
}

#[test]
fn two_daemons_write_lease_conflict() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr_a = format!("{}", mgr_addr);
        let mgr_b = mgr_a.clone();
        let cluster_a = ClusterClient::connect_raw(&mgr_a).await.expect("client A");
        let cluster_b = ClusterClient::connect_raw(&mgr_b).await.expect("client B");
        let id_a = cid(0xa1, "daemon-a");
        let id_b = cid(0xb2, "daemon-b");

        let r = lease::acquire(&cluster_a, &id_a, 42, LEASE_MODE_WRITE)
            .await
            .expect("daemon A acquire");
        match r {
            AcquireResult::Granted(info) => {
                assert_eq!(info.ino, 42);
                assert!(info.writer_present);
            }
            other => panic!("daemon A must be granted, got {other:?}"),
        }

        let r = lease::acquire(&cluster_b, &id_b, 42, LEASE_MODE_WRITE)
            .await
            .expect("daemon B acquire");
        match r {
            AcquireResult::Conflict { manager_message } => {
                assert!(
                    manager_message.contains("daemon-a"),
                    "msg='{manager_message}' should name the holder"
                );
            }
            other => panic!("daemon B must conflict, got {other:?}"),
        }
    });
}

#[test]
fn two_daemons_read_lease_coexist() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_a = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let cluster_b = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let id_a = cid(0x11, "reader-a");
        let id_b = cid(0x22, "reader-b");

        for (cluster, id) in [(&cluster_a, &id_a), (&cluster_b, &id_b)] {
            match lease::acquire(cluster, id, 7, LEASE_MODE_READ).await.unwrap() {
                AcquireResult::Granted(info) => {
                    assert!(!info.writer_present, "no writer; readers coexist");
                }
                other => panic!("read leases must coexist, got {other:?}"),
            }
        }
    });
}

#[test]
fn writer_release_unblocks_second_daemon_and_bumps_version() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_a = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let cluster_b = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let id_a = cid(0x33, "writer-a");
        let id_b = cid(0x44, "writer-b");

        let v_a = match lease::acquire(&cluster_a, &id_a, 100, LEASE_MODE_WRITE)
            .await
            .unwrap()
        {
            AcquireResult::Granted(info) => info.version,
            other => panic!("expected Granted, got {other:?}"),
        };

        // Release; manager returns the new version.
        let new_v = lease::release(&cluster_a, &id_a, 100)
            .await
            .unwrap()
            .expect("writer-close must return new_version");
        assert_eq!(new_v, v_a + 1);

        // Second daemon now succeeds and inherits the bumped version.
        let v_b = match lease::acquire(&cluster_b, &id_b, 100, LEASE_MODE_WRITE)
            .await
            .unwrap()
        {
            AcquireResult::Granted(info) => info.version,
            other => panic!("expected Granted after release, got {other:?}"),
        };
        assert_eq!(v_b, new_v, "second writer inherits the bumped version");
    });
}

#[test]
fn heartbeat_round_trip_and_post_release_not_held() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let id = cid(0x55, "writer-c");
        let _ = lease::acquire(&cluster, &id, 200, LEASE_MODE_WRITE)
            .await
            .unwrap();
        match lease::heartbeat(&cluster, &id, 200).await.unwrap() {
            HeartbeatResult::Renewed(info) => assert!(info.writer_present),
            other => panic!("expected Renewed, got {other:?}"),
        }
        // Idempotent release returns Some(new_version).
        assert!(lease::release(&cluster, &id, 200).await.unwrap().is_some());
        // Heartbeat after release surfaces NotHeld — what the daemon's
        // session_heartbeat_loop uses to detect external revocation +
        // evict the cached ring_fds.
        assert!(matches!(
            lease::heartbeat(&cluster, &id, 200).await.unwrap(),
            HeartbeatResult::NotHeld
        ));
    });
}
