//! F-ioring-lease Phase 1 byte-level end-to-end test.
//!
//! Boots a real cluster (manager + 2 EN + 1 PS), seeds a fuse file
//! via the write path, then drives the full F-1/2/3/4 contract from
//! the daemon side: writer-close → invalidation push → reader's
//! cache-stale predicate flips → reload_extents → next read
//! observes the NEW bytes.
//!
//! This is the deferred byte-correctness repro called out in
//! F-ioring-lease-4's "out of scope" list. `#[ignore]`'d like every
//! other system_* test — runs under `cargo test -- --ignored`.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_IORING, LEASE_INVAL_WRITER_CLOSED, LEASE_MODE_READ,
    LEASE_MODE_WRITE,
};

use autumn_fuse::schema::{self, DirentValue, DT_REG, MAX_EXTENT, ROOT_INO};
use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, key, meta, write};

use autumn_ioring::fuse_read;
use autumn_ioring::lease::{
    self, apply_invalidation, cache_is_stale, AcquireResult, DaemonClientId, InvalidationMap,
};

use support::*;

fn pattern_v1(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut i: u64 = 0;
    while out.len() + 8 <= len {
        out.extend_from_slice(&i.to_le_bytes());
        i = i.wrapping_add(1);
    }
    while out.len() < len {
        out.push(0);
    }
    out
}

fn pattern_v2(len: usize) -> Vec<u8> {
    // Distinct from v1 — wrapping_add(0xdead_beef) shifts the
    // sequence so any byte position differs from v1.
    let mut out = Vec::with_capacity(len);
    let mut i: u64 = 0xdead_beef;
    while out.len() + 8 <= len {
        out.extend_from_slice(&i.to_le_bytes());
        i = i.wrapping_add(1);
    }
    while out.len() < len {
        out.push(0xab);
    }
    out
}

fn cid(byte: u8, host: &str) -> DaemonClientId {
    DaemonClientId::from_wire(MgrClientId {
        kind: LEASE_CLIENT_KIND_IORING,
        uuid: [byte; 16],
        host: host.to_string(),
    })
}

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
    let cluster = ClusterClient::connect(&mgr_addr.to_string())
        .await
        .expect("ClusterClient::connect");
    cluster.set_rpc_timeout(Duration::from_secs(30));
    cluster
}

/// End-to-end test for Phase 1 close-to-open coherence.
///
/// Steps:
/// 1. Boot a 2-EN / 1-PS cluster.
/// 2. Seed `shared.bin` (10 MiB) with `pattern_v1` via the fuse
///    WRITE path (so the file exists with v1 bytes).
/// 3. Reader daemon B subscribes (Acquire READ lease), opens
///    `shared.bin`, reads → expects v1.
/// 4. Writer daemon A Acquires WRITE lease, overwrites with
///    `pattern_v2` via `fuse_write::write_into`, Releases.
/// 5. Reader B's poll surfaces the `WriterClosed` event.
/// 6. `apply_invalidation` bumps `inv[ino]` past B's cached
///    `lease_version` → `cache_is_stale == true`.
/// 7. `fuse_read::reload_extents` refreshes B's cache.
/// 8. B reads again → MUST be v2. (This is the close-to-open
///    coherence guarantee.)
#[test]
#[ignore]
fn phase1_close_to_open_coherence_e2e() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 138, 13801).await;

        // ── (2) seed shared.bin with v1 via the fuse WRITE path.
        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");

        let total = 10 * 1024 * 1024usize;
        let v1 = pattern_v1(total);
        let v2 = pattern_v2(total);
        assert!(v1 != v2, "patterns must differ byte-wise");
        let ino = 300u64;

        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m)
            .await
            .expect("put_inode");
        let dk = key::dirent_key(ROOT_INO, b"shared.bin");
        let dv = schema::encode_dirent(&DirentValue {
            child_inode: ino,
            file_type: DT_REG,
        });
        state.kv_put(&dk, &dv).await.expect("put dirent");
        write::write(&mut state, ino, 0, &v1).await.expect("write v1");
        write::flush_inode(&mut state, ino).await.expect("flush v1");

        // ── (3) Reader daemon B: own ClusterClient + a UUID-stable
        // DaemonClientId. Subscribes via Acquire READ + drains any
        // initial events so future polls observe only the v1→v2
        // writer-close.
        let cluster_b = ClusterClient::connect(&mgr_addr.to_string())
            .await
            .expect("daemon B client");
        cluster_b.set_rpc_timeout(Duration::from_secs(30));
        let id_b = cid(0xb1, "daemon-B");

        let b_acquire = match lease::acquire(&cluster_b, &id_b, ino, LEASE_MODE_READ)
            .await
            .unwrap()
        {
            AcquireResult::Granted(info) => info,
            other => panic!("B read acquire: {other:?}"),
        };
        let _ = lease::poll_invalidations(&cluster_b, &id_b).await.unwrap();

        // B opens the path → resolves to ino + extent map at v1.
        let mut opened_b = fuse_read::open(&cluster_b, b"shared.bin")
            .await
            .expect("B open");
        // Tag the cache with the lease version (as the daemon's
        // Open arm does in production).
        opened_b.lease_version = b_acquire.version;
        assert_eq!(opened_b.ino, ino);
        assert_eq!(opened_b.size, total as u64);

        // B reads v1.
        let dest_v1 = fuse_read::read(&cluster_b, &opened_b, 0, total as u32)
            .await
            .expect("B read v1");
        assert!(
            dest_v1 == v1,
            "reader B must see v1 before any writer-close"
        );

        // ── (4) Writer daemon A: own ClusterClient + UUID. Acquires
        // WRITE lease, overwrites with v2 via fuse_write::write_into,
        // Releases (this is what fires the WriterClosed push to B).
        let cluster_a = ClusterClient::connect(&mgr_addr.to_string())
            .await
            .expect("daemon A client");
        cluster_a.set_rpc_timeout(Duration::from_secs(30));
        let id_a = cid(0xa1, "daemon-A");

        let _ = match lease::acquire(&cluster_a, &id_a, ino, LEASE_MODE_WRITE)
            .await
            .unwrap()
        {
            AcquireResult::Granted(info) => info,
            other => panic!("A write acquire: {other:?}"),
        };

        // A overwrites the entire file with v2. fuse_write reuses
        // the same extent map for an in-place overwrite at off 0
        // → off+8 MiB (matches the two-extent layout v1 produced).
        let mut opened_a = fuse_read::open(&cluster_a, b"shared.bin")
            .await
            .expect("A open");
        let n1 = autumn_ioring::fuse_write::write_into(
            &cluster_a,
            &mut opened_a,
            0,
            &v2[..MAX_EXTENT],
        )
        .await
        .expect("A write extent 0");
        assert_eq!(n1, MAX_EXTENT);
        let n2 = autumn_ioring::fuse_write::write_into(
            &cluster_a,
            &mut opened_a,
            MAX_EXTENT as u64,
            &v2[MAX_EXTENT..],
        )
        .await
        .expect("A write extent 1");
        assert_eq!(n2, total - MAX_EXTENT);

        let new_version = lease::release(&cluster_a, &id_a, ino)
            .await
            .unwrap()
            .expect("writer close must return new_version");
        assert!(
            new_version > b_acquire.version,
            "release must bump version above B's cached one"
        );

        // ── (5) B polls → sees the WriterClosed event.
        let events = lease::poll_invalidations(&cluster_b, &id_b).await.unwrap();
        assert_eq!(events.len(), 1, "events={events:?}");
        assert_eq!(events[0].ino, ino);
        assert_eq!(events[0].version, new_version);
        assert_eq!(events[0].kind, LEASE_INVAL_WRITER_CLOSED);

        // ── (6) apply_invalidation bumps the per-ino floor.
        let mut inv = InvalidationMap::new();
        let overflow = apply_invalidation(&events, &mut inv);
        assert!(!overflow);
        assert_eq!(inv.get(&ino).copied(), Some(new_version));
        assert!(
            cache_is_stale(opened_b.ino, opened_b.lease_version, &inv),
            "B's cache must be flagged stale post-invalidation"
        );

        // ── (7) reload_extents refreshes B's view.
        fuse_read::reload_extents(&cluster_b, &mut opened_b)
            .await
            .expect("B reload_extents");
        opened_b.lease_version = new_version;
        assert!(
            !cache_is_stale(opened_b.ino, opened_b.lease_version, &inv),
            "post-reload cache must be fresh"
        );

        // ── (8) B reads again — MUST observe v2.
        let dest_v2 = fuse_read::read(&cluster_b, &opened_b, 0, total as u32)
            .await
            .expect("B read post-reload");
        assert_eq!(dest_v2.len(), total);
        assert!(
            dest_v2 == v2,
            "close-to-open coherence VIOLATED: reader saw stale bytes after writer-close + invalidation"
        );

        // Spot-check a cross-extent sub-range too.
        let coff = (MAX_EXTENT - 50 * 1024) as u64;
        let clen = 100 * 1024u32;
        let cross = fuse_read::read(&cluster_b, &opened_b, coff, clen)
            .await
            .expect("B cross-extent post-reload");
        assert!(
            cross == v2[coff as usize..coff as usize + clen as usize],
            "cross-extent post-reload mismatch"
        );

        // Best-effort: B releases its read lease.
        let _ = lease::release(&cluster_b, &id_b, ino).await.unwrap();
    });
}
