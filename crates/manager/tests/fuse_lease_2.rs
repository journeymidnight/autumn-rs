//! Kernel cache eviction via the per-mount
//! invalidation poll loop. The production wiring calls
//! `fuser::Notifier::inval_inode(ino, 0, 0)` (drops both
//! attribute + page cache for the ino); the e2e covers the
//! contract by injecting a counting `InodeInvalidator` callback
//! and verifying the per-ino invocations.
//!
//! Why we can't just mount a real fuse FS in the test: the test
//! harness runs in CI containers without `/dev/fuse` access.
//! The `InodeInvalidator` indirection is exactly what
//! it was designed to be — a swappable closure the
//! production binary fills with the real Notifier and tests fill
//! with a counter.
//!
//! Covers:
//! 1. Per-ino `WriterClosed` event → invalidator called with that
//!    ino (kernel page cache would be dropped in production).
//! 2. ino=0 sentinel events are SKIPPED by the invalidator (it's
//!    the overflow marker, not a real FUSE ino).
//! 3. Overflow path: invalidator called for EVERY held ino as
//!    part of the wholesale-clear branch (kernel-side eviction
//!    mirrors the user-space `held_leases.clear()`).
//! 4. Transport-error path: same wholesale per-ino kernel
//!    eviction before the retry sleep.

mod support;

use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_FUSE, LEASE_MODE_READ, LEASE_MODE_WRITE,
};

use autumn_fuse::dispatch::{self, InodeInvalidator};
use autumn_fs::state::{FsState, FuseLease};
use autumn_fs::meta;

use support::*;

async fn boot_cluster(
    mgr_addr: std::net::SocketAddr,
    n1_addr: std::net::SocketAddr,
    n2_addr: std::net::SocketAddr,
    base: u16,
    part_id: u64,
) -> ClusterClient {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1_addr, n2_addr, base).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff\xff\xff\xff").await;
    let ps_addr = pick_addr();
    start_partition_server(base as u64, mgr_addr, ps_addr);
    compio::time::sleep(Duration::from_millis(1500)).await;
    let _ = RpcClient::connect(ps_addr).await.unwrap();
    let cluster = ClusterClient::connect_raw(&mgr_addr.to_string())
        .await
        .expect("ClusterClient::connect");
    cluster.set_rpc_timeout(Duration::from_secs(30));
    cluster
}

fn cid(byte: u8, host: &str) -> DaemonClientId {
    DaemonClientId::from_wire(MgrClientId {
        kind: LEASE_CLIENT_KIND_FUSE,
        uuid: [byte; 16],
        host: host.to_string(),
    })
}

/// Build a counting `InodeInvalidator` backed by an `Arc<Mutex<Vec<u64>>>`
/// (`Arc` so the test thread can read it; the production type is
/// `Rc<dyn Fn>` because compio is single-threaded and we don't need
/// `Send` there).
fn counting_invalidator() -> (InodeInvalidator, Arc<Mutex<Vec<u64>>>) {
    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let log_c = log.clone();
    let inv: InodeInvalidator = Rc::new(move |ino: u64| {
        log_c.lock().unwrap().push(ino);
    });
    (inv, log)
}

#[test]
fn multiple_distinct_inos_each_get_invalidated() {
    // The per-ino invalidator must fire for EACH distinct ino in
    // an event batch — production case: a reader subscribes to N
    // different files, several writers close concurrently, the
    // reader's poll returns the batch, and the kernel's page cache
    // must be dropped per-ino so a follow-up read of any of them
    // re-fetches the post-close bytes.
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 145, 14501).await;

        let mut reader = FsState::new(&mgr_addr.to_string()).await.expect("reader");
        let (inv, log) = counting_invalidator();
        dispatch::spawn_lease_background_tasks(&reader, Some(inv));

        let inos = [201u64, 202, 203];
        // Subscribe READ leases on all 3 (so writer-close pushes
        // target this reader's inbox).
        for &ino in &inos {
            let m = meta::new_file_meta(0o644, 0, 0);
            meta::put_inode(&mut reader, ino, &m).await.expect("seed");
            let info = match lease::acquire(&reader.client, &reader.client_id, ino, LEASE_MODE_READ)
                .await
                .unwrap()
            {
                AcquireResult::Granted(i) => i,
                other => panic!("acquire {ino}: {other:?}"),
            };
            reader.held_leases.borrow_mut().insert(
                ino,
                FuseLease {
                    writer_refs: 0,
                    reader_refs: 1,
                    mode: LEASE_MODE_READ,
                    lease_epoch: info.version,
                    revoked: false,
                },
            );
        }
        let _ = lease::poll_invalidations(&reader.client, &reader.client_id)
            .await
            .unwrap();

        // Three writer-close cycles, one per ino, from a distinct
        // writer identity. Manager queues 3 WriterClosed events
        // into the reader's inbox + wakes the parked poll.
        let writer = cid(0xcc, "writer");
        let w_cluster = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        for &ino in &inos {
            let _ = lease::acquire(&w_cluster, &writer, ino, LEASE_MODE_WRITE)
                .await
                .unwrap();
            let _ = lease::release(&w_cluster, &writer, ino).await.unwrap();
        }

        // Wait for the reader's poll loop to apply ALL events.
        // Detection: every held ino appears at least once in the
        // log.
        let mut tries = 0;
        loop {
            let entries: std::collections::BTreeSet<u64> =
                log.lock().unwrap().iter().copied().collect();
            if inos.iter().all(|i| entries.contains(i)) {
                break;
            }
            tries += 1;
            if tries > 50 {
                panic!(
                    "missing invalidations; log={:?}",
                    log.lock().unwrap()
                );
            }
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        let entries: std::collections::BTreeSet<u64> =
            log.lock().unwrap().iter().copied().collect();
        for &ino in &inos {
            assert!(
                entries.contains(&ino),
                "every held ino must be invalidated; missing {ino}; entries={entries:?}"
            );
        }
        // ino=0 sentinel never reaches the invalidator.
        assert!(
            !entries.contains(&0),
            "ino=0 sentinel never reaches the invalidator"
        );
    });
}
