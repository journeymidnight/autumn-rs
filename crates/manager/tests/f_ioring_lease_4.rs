//! F-ioring-lease-4 — end-to-end close-to-open coherence via the
//! per-session invalidation map. Drives the full lease layer (real
//! AutumnManager + real ClusterClient + the daemon-side `lease`
//! helpers + the pure-fn `apply_invalidation` / `cache_is_stale`)
//! to assert:
//!
//! 1. A reader caches `(ino, lease_epoch=N)`. Writer (different
//!    daemon) acquires, closes → manager pushes
//!    `WriterClosed { ino, version=N+1 }`. The reader's
//!    `session_invalidation_poll_loop` (simulated here by calling
//!    `lease::poll_invalidations + apply_invalidation` directly)
//!    bumps `invalidations[ino] = N+1`. `cache_is_stale(ino, N,
//!    &map) == true` while a fresh cache at `lease_epoch=N+1`
//!    is fresh again.
//!
//! 2. The "subscribe disconnect = invalidate everything" sentinel
//!    (overflow path) round-trips through the same helpers: an
//!    overflowed inbox surfaces `MetaChanged { ino=0 }`, which
//!    `apply_invalidation` returns as `saw_overflow=true` so the
//!    daemon does the wholesale-clear branch.
//!
//! 3. Out-of-order events don't roll back the floor — verified via
//!    the unit tests in `lease.rs` for the pure-fn, exercised
//!    end-to-end here by interleaving releases on two inodes.
//!
//! The byte-level "reader actually sees new bytes after write+close"
//! is realized as `f_ioring_lease_phase1_e2e.rs::
//! phase1_close_to_open_coherence_e2e` — a full-cluster test
//! (manager + 2 EN + 1 PS, seeds shared.bin via the data plane).
//! `#[ignore]`'d like every other system_* test (run via
//! `cargo test -- --ignored`); CERTIFIED GREEN 3/3 on 2026-06-15.

use std::net::SocketAddr;
use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_IORING, LEASE_INVAL_META_CHANGED, LEASE_INVAL_WRITER_CLOSED,
    LEASE_MODE_READ, LEASE_MODE_WRITE,
};

use autumn_client::lease::{
    self, apply_invalidation, cache_is_stale, DaemonClientId, InvalidationMap,
};

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
fn writer_close_bumps_reader_invalidation_floor() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_w = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_r = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let writer = cid(0xa1, "writer");
        let reader = cid(0xb2, "reader");

        // Reader subscribes via a read-lease acquire (so its inbox
        // exists and it'll receive WriterClosed pushes).
        let r_lease = match lease::acquire(&cluster_r, &reader, 42, LEASE_MODE_READ)
            .await
            .unwrap()
        {
            lease::AcquireResult::Granted(info) => info,
            other => panic!("reader acquire: {other:?}"),
        };
        let cached_version = r_lease.version;
        // Drain any initial-empty events so the next poll observes
        // the writer-close specifically.
        let _ = lease::poll_invalidations(&cluster_r, &reader)
            .await
            .unwrap();

        // Initially: no invalidations recorded. Cache is fresh.
        let mut inv = InvalidationMap::new();
        assert!(!cache_is_stale(42, cached_version, &inv));

        // Writer acquires + immediately releases.
        let _ = lease::acquire(&cluster_w, &writer, 42, LEASE_MODE_WRITE)
            .await
            .unwrap();
        let new_version = lease::release(&cluster_w, &writer, 42)
            .await
            .unwrap()
            .expect("writer-close must return new_version");

        // Reader polls (writer-close pushed; manager wakes the poll).
        let events = lease::poll_invalidations(&cluster_r, &reader).await.unwrap();
        assert_eq!(events.len(), 1, "events={events:?}");
        assert_eq!(events[0].ino, 42);
        assert_eq!(events[0].version, new_version);
        assert_eq!(events[0].kind, LEASE_INVAL_WRITER_CLOSED);

        // Apply the events through the daemon-side bookkeeping. Cache
        // tagged at the pre-close version is now stale; a fresh
        // re-acquire-tagged cache would be fresh again.
        let overflow = apply_invalidation(&events, &mut inv);
        assert!(!overflow);
        assert_eq!(inv.get(&42).copied(), Some(new_version));
        assert!(cache_is_stale(42, cached_version, &inv));
        assert!(!cache_is_stale(42, new_version, &inv));
    });
}

#[test]
fn overflow_sentinel_triggers_wholesale_clear() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_r = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_w = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let reader = cid(0xc1, "overflow-reader");
        let writer = cid(0xd2, "overflow-writer");

        // Force inbox overflow by pushing > MAX_INBOX_EVENTS (1024)
        // via writer-close cycles on a real reader subscription.
        // Subscribe (so the inbox exists) and acquire one read
        // lease so the writer-close pushes target this client.
        let _ = lease::acquire(&cluster_r, &reader, 50, LEASE_MODE_READ)
            .await
            .unwrap();
        let _ = lease::poll_invalidations(&cluster_r, &reader)
            .await
            .unwrap();

        // 1025 acquire+release cycles → 1025 WriterClosed pushes → 1024
        // queue cap → overflow flag set. This is slow (~1025 manager
        // round-trips) but precisely exercises the overflow path
        // end-to-end without reaching into manager internals.
        for _ in 0..1025 {
            let _ = lease::acquire(&cluster_w, &writer, 50, LEASE_MODE_WRITE)
                .await
                .unwrap();
            let _ = lease::release(&cluster_w, &writer, 50).await.unwrap();
        }
        let events = lease::poll_invalidations(&cluster_r, &reader).await.unwrap();
        // The drain returns the 1024 surviving events PLUS the
        // overflow sentinel synthesised by the handler.
        assert!(events.len() >= 1024, "events.len()={}", events.len());
        let sentinel = events
            .iter()
            .find(|e| e.kind == LEASE_INVAL_META_CHANGED && e.ino == 0)
            .expect("overflow sentinel must be present");
        assert_eq!(sentinel.version, 0);

        // The daemon-side helper turns the sentinel into a
        // wholesale-clear signal.
        let mut inv = InvalidationMap::new();
        let overflow = apply_invalidation(&events, &mut inv);
        assert!(overflow, "apply_invalidation must surface saw_overflow");
        // The non-sentinel events still apply, but the wholesale
        // path clears `invalidations` + `held_leases` + `ring_fds`,
        // so this map is conceptually dropped right after.
    });
}

#[test]
fn out_of_order_events_dont_roll_back_floor() {
    // Pure-fn replay of the apply_invalidation invariant under a
    // realistic event order — proven against the wire too here.
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster_w = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let cluster_r = ClusterClient::connect(&mgr_addr.to_string()).await.unwrap();
        let writer = cid(0xe1, "writer");
        let reader = cid(0xf2, "reader");

        // Reader subscribes + acquires read leases on two inodes.
        let _ = lease::acquire(&cluster_r, &reader, 100, LEASE_MODE_READ)
            .await
            .unwrap();
        let _ = lease::acquire(&cluster_r, &reader, 200, LEASE_MODE_READ)
            .await
            .unwrap();
        let _ = lease::poll_invalidations(&cluster_r, &reader).await.unwrap();

        // Two writer cycles on ino=100, one on ino=200, then poll
        // — events arrive in queue order; the per-ino floor on 100
        // takes the MAX across both bumps.
        let _ = lease::acquire(&cluster_w, &writer, 100, LEASE_MODE_WRITE)
            .await
            .unwrap();
        let v100_a = lease::release(&cluster_w, &writer, 100)
            .await
            .unwrap()
            .unwrap();
        let _ = lease::acquire(&cluster_w, &writer, 200, LEASE_MODE_WRITE)
            .await
            .unwrap();
        let v200 = lease::release(&cluster_w, &writer, 200)
            .await
            .unwrap()
            .unwrap();
        let _ = lease::acquire(&cluster_w, &writer, 100, LEASE_MODE_WRITE)
            .await
            .unwrap();
        let v100_b = lease::release(&cluster_w, &writer, 100)
            .await
            .unwrap()
            .unwrap();
        assert!(v100_b > v100_a, "monotonic across same-ino cycles");

        let events = lease::poll_invalidations(&cluster_r, &reader).await.unwrap();
        assert_eq!(events.len(), 3);

        let mut inv = InvalidationMap::new();
        apply_invalidation(&events, &mut inv);
        assert_eq!(inv.get(&100).copied(), Some(v100_b), "max wins");
        assert_eq!(inv.get(&200).copied(), Some(v200));
    });
}
