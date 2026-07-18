//! F-ioring-lease-3 — long-poll invalidation channel. Drives the
//! end-to-end shape: a reader's `MSG_POLL_INVALIDATIONS` parks on
//! the manager side; a concurrent writer-close fires the waker; the
//! poll returns the WriterClosed event within ~ms (NOT after the
//! 10 s LONG_POLL_WAIT timeout).
//!
//! Covers:
//! 1. Idle poll on an inbox with no events blocks (returns within a
//!    few hundred ms when a writer-close fires).
//! 2. Empty long-poll eventually times out and returns Ok with no
//!    events — the client retry loop is bounded.
//! 3. Multiple consecutive events queue while the reader is parked
//!    and all surface on the next poll (no events lost across the
//!    long-poll cycle).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use autumn_client::ClusterClient;
use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_IORING, LEASE_INVAL_WRITER_CLOSED, LEASE_MODE_READ,
    LEASE_MODE_WRITE,
};

use autumn_client::lease::{self, DaemonClientId};

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

fn cid(byte: u8, host: &str) -> DaemonClientId {
    DaemonClientId::from_wire(MgrClientId {
        kind: LEASE_CLIENT_KIND_IORING,
        uuid: [byte; 16],
        host: host.to_string(),
    })
}

#[test]
fn long_poll_returns_promptly_on_writer_close() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // Two separate ClusterClient instances so the writer's
        // ReleaseLease and the reader's PollInvalidations travel
        // over distinct TCP connections — mirrors two daemons.
        let cluster_w = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let cluster_r = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let writer = cid(0xa1, "writer");
        let reader = cid(0xb2, "reader");

        // Reader subscribes (so the manager has its inbox) and acquires
        // a read lease (so the WriterClosed push targets the reader).
        let _ = lease::acquire(&cluster_r, &reader, 7, LEASE_MODE_READ)
            .await
            .unwrap();
        let _ = lease::poll_invalidations(&cluster_r, &reader)
            .await
            .unwrap();
        // Writer acquires.
        let _ = lease::acquire(&cluster_w, &writer, 7, LEASE_MODE_WRITE)
            .await
            .unwrap();

        // Reader starts a long-poll. Meanwhile fire the writer-close
        // after a short delay. The poll must complete promptly (well
        // under the 10 s LONG_POLL_WAIT) once the waker fires.
        let poll_start = Instant::now();
        let poll_fut = lease::poll_invalidations(&cluster_r, &reader);
        let release_fut = async {
            compio::time::sleep(Duration::from_millis(200)).await;
            lease::release(&cluster_w, &writer, 7).await.unwrap();
        };

        let (poll_result, _) = futures::future::join(poll_fut, release_fut).await;
        let elapsed = poll_start.elapsed();
        let events = poll_result.unwrap();

        assert!(
            elapsed < Duration::from_secs(2),
            "long-poll should resolve via waker, not via the 10s timeout; elapsed={elapsed:?}"
        );
        // The release went out after a 200 ms sleep — the poll must
        // have actually waited (not returned the empty-then-retry
        // result our pre-F3 implementation would).
        assert!(
            elapsed >= Duration::from_millis(150),
            "long-poll resolved too fast; expected ~200 ms wait; elapsed={elapsed:?}"
        );
        assert_eq!(events.len(), 1, "events={events:?}");
        assert_eq!(events[0].ino, 7);
        assert_eq!(events[0].kind, LEASE_INVAL_WRITER_CLOSED);
    });
}

#[test]
fn long_poll_times_out_when_idle() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // Bound the test runtime — the manager's LONG_POLL_WAIT is
        // 10 s; we use a higher SDK timeout so the manager's timer
        // is what fires.
        let cluster = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        cluster.set_rpc_timeout(Duration::from_secs(30));
        let id = cid(0xc3, "idle");
        // Subscribe so the inbox exists, then poll. The poll has no
        // queued events and the test never pushes any — must time
        // out around LONG_POLL_WAIT (10 s) with `Ok(vec![])`.
        let _ = lease::acquire(&cluster, &id, 1, LEASE_MODE_READ)
            .await
            .unwrap();
        let _ = lease::poll_invalidations(&cluster, &id).await.unwrap();

        let start = Instant::now();
        let events = lease::poll_invalidations(&cluster, &id).await.unwrap();
        let elapsed = start.elapsed();
        assert!(events.is_empty(), "idle long-poll should return empty");
        // Tolerance: ±2 s around the 10 s wait.
        assert!(
            elapsed >= Duration::from_secs(8) && elapsed < Duration::from_secs(15),
            "idle long-poll should wait ~10 s, got {elapsed:?}"
        );
    });
}

#[test]
fn long_poll_returns_all_queued_events() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let reader = cid(0xd4, "reader-multi");
        let w1 = cid(0xe5, "writer-1");
        let w2 = cid(0xe6, "writer-2");

        let _ = lease::acquire(&cluster, &reader, 100, LEASE_MODE_READ)
            .await
            .unwrap();
        let _ = lease::acquire(&cluster, &reader, 200, LEASE_MODE_READ)
            .await
            .unwrap();
        let _ = lease::poll_invalidations(&cluster, &reader)
            .await
            .unwrap();

        // Queue two events BEFORE polling so they're both ready
        // synchronously.
        let _ = lease::acquire(&cluster, &w1, 100, LEASE_MODE_WRITE)
            .await
            .unwrap();
        let _ = lease::release(&cluster, &w1, 100).await.unwrap();
        let _ = lease::acquire(&cluster, &w2, 200, LEASE_MODE_WRITE)
            .await
            .unwrap();
        let _ = lease::release(&cluster, &w2, 200).await.unwrap();

        let events = lease::poll_invalidations(&cluster, &reader).await.unwrap();
        // Both events must be in this single poll response — the
        // long-poll handler drains the entire queue per call.
        assert_eq!(events.len(), 2, "events={events:?}");
        let inos: std::collections::BTreeSet<u64> = events.iter().map(|e| e.ino).collect();
        assert!(inos.contains(&100));
        assert!(inos.contains(&200));
    });
}
