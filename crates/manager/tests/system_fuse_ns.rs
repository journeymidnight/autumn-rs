//! F-KEY-NS — fuse batch-data-path key-consistency regression test.
//!
//! The extent DATA path bypasses `FsState`'s `kv_*` helpers for performance
//! (read → `get_many_into`, write append → `put_many_fenced`), calling the
//! scoped client directly. Both paths and the `kv_*` metadata paths must land
//! at the SAME `fs/{tenant}/[type]…` keys, or a cold remount's range-scan/read
//! wouldn't find the extents the append path wrote. Every OTHER fuse test
//! write+read within ONE warm `FsState`, so a key mismatch there is invisible;
//! this test is the exact blind spot:
//!
//! `cold_remount_reads_back_written_extents` — write a multi-extent file, drop
//! the `FsState` (unmount), open a NEW one (cold `scan_extents`), read it back
//! byte-exact. Fails if the append path's extents aren't at the same keys the
//! cold scan/read use.
//!
//! `two_tenants_isolate_same_inode` — two tenants write the SAME inode number
//! into one cluster; each cold-remount must read back its OWN bytes. Guards the
//! `fs/{tenant}/` prefix as the isolation boundary (post-`{volume}`-removal, the
//! tenant is the isolation unit).
//!
//! `stale_volume_data_refuses_mount` — a tenant still holding pre-`{volume}`-
//! removal data must refuse to mount (fail-loud) instead of silently shadowing
//! it with an empty FS. Guards the `ensure_schema_version` migration check.
//!
//! Driven directly through `FsState` (the kernel FUSE mount layer is unchanged).
//! `#[ignore]` — needs a live cluster (manager + 2 EN + PS), same as
//! `system_fuse_read.rs`.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, meta, read, write};

use support::*;

/// Deterministic, seed-varied byte pattern so two volumes' payloads differ.
fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut i: u64 = seed;
    while out.len() + 8 <= len {
        out.extend_from_slice(&i.to_le_bytes());
        i = i.wrapping_add(1);
    }
    while out.len() < len {
        out.push(seed as u8);
    }
    out
}

/// Stand up a 2-EN + PS cluster with one partition covering the whole keyspace.
/// Returns an admin client kept alive for the test's duration.
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

#[test]
#[ignore]
fn cold_remount_reads_back_written_extents() {
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
        let mgr = mgr_addr.to_string();

        // 10 MiB → 2 extents, written through the buffered append path
        // (flush_appends → put_many_fenced — the batch write that bypassed the
        // kv_* choke point pre-fix).
        let total = 10 * 1024 * 1024usize;
        let data = pattern(total, 0xA5);
        let ino = 100u64;

        // ── mount #1: write + flush, then DROP (unmount) ──
        {
            let mut state = FsState::new(&mgr, "default")
                .await
                .expect("mount1");
            dispatch::init_root(&mut state).await.expect("init_root");
            meta::put_inode(&mut state, ino, &meta::new_file_meta(0o644, 0, 0))
                .await
                .expect("put_inode");
            let n = write::write(&mut state, ino, 0, &data).await.expect("write");
            assert_eq!(n as usize, total, "write full length");
            write::flush_inode(&mut state, ino).await.expect("flush");
            // state dropped here → all in-memory extent caches gone.
        }

        // ── mount #2: fresh FsState (cold), read back through scan_extents ──
        let mut state2 = FsState::new(&mgr, "default")
            .await
            .expect("mount2");
        // init_root is idempotent — root already exists, this is a no-op.
        dispatch::init_root(&mut state2).await.expect("init_root2");
        let back = read::read(&mut state2, ino, 0, total as u32)
            .await
            .expect("cold read");
        assert_eq!(back.len(), total, "cold read length");
        assert!(
            back == data,
            "cold remount read did NOT match written bytes — extents are not at \
             the volume-scoped keys the cold scan/read use (SD-3 P0 regression)"
        );
    });
}

/// P1 guard (coco): mounting a tenant that still holds pre-`{volume}`-removal
/// data must REFUSE LOUDLY, not silently present an empty FS + shadow the old
/// tree. The old SD-3 layout wrote `fs/{tenant}/{volume}/…` — relative keys
/// beginning with a volume-name byte (≥ 0x2d); our keys begin with a type byte
/// (0x01–0x04). `ensure_schema_version`'s fresh branch scans for any relative
/// key ≥ 0x05 and bails when it finds one.
#[test]
#[ignore]
fn stale_volume_data_refuses_mount() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 141, 14101).await;
        let mgr = mgr_addr.to_string();

        // Plant one old-layout key via a RAW (unscoped) client: an inode key
        // under `fs/tenant-stale/oldvol/` — exactly what the removed `{volume}`
        // layer produced. `connect_raw` does NO prefixing, so we spell the full
        // wire key.
        let mut wire = b"fs/tenant-stale/oldvol/".to_vec();
        wire.push(0x01); // PREFIX_INODE
        wire.extend_from_slice(&100u64.to_be_bytes());
        admin
            .put(&wire, b"stale-inode-meta")
            .await
            .expect("plant old-volume key");

        // A fresh scoped mount for the same tenant must refuse (no schema stamp
        // + old data present ⇒ loud bail, NOT a silent empty FS).
        let mut state = FsState::new(&mgr, "tenant-stale")
            .await
            .expect("mount tenant-stale");
        let err = dispatch::init_root(&mut state)
            .await
            .expect_err("init_root MUST refuse when un-migrated volume data exists");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("pre-`{volume}`-removal layout"),
            "wrong error — expected the stale-volume refusal, got: {msg}"
        );

        // A DIFFERENT, clean tenant on the same cluster still mounts fine (the
        // guard is scoped to the tenant that actually holds old data).
        let mut clean = FsState::new(&mgr, "tenant-clean")
            .await
            .expect("mount tenant-clean");
        dispatch::init_root(&mut clean)
            .await
            .expect("clean tenant mounts");
    });
}

/// Two tenants writing the SAME inode number into ONE cluster must not see each
/// other's bytes — the `fs/{tenant}/` prefix (client-side `NamespaceBinding`)
/// is the isolation boundary. This replaces the deleted SD-3 two-VOLUME test:
/// after `{volume}` removal, the tenant IS the isolation unit.
///
/// Both tenants write ino 100 (multi-extent, through the buffered append path),
/// drop their mounts, then cold-remount and read back — each must recover its
/// OWN pattern with zero cross-contamination. A prefix bug (missing/short
/// binding, or the extent append landing at an unscoped key) would let tenant B
/// overwrite tenant A's extents, surfacing here as a byte mismatch on cold read.
#[test]
#[ignore]
fn two_tenants_isolate_same_inode() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 140, 14001).await;
        let mgr = mgr_addr.to_string();

        // Same inode number, DIFFERENT tenants + DIFFERENT payloads.
        let total = 5 * 1024 * 1024usize; // 5 MiB → 1 extent + a partial
        let ino = 100u64;
        let data_a = pattern(total, 0xA1);
        let data_b = pattern(total, 0xB2);
        assert_ne!(data_a, data_b, "seeds must produce distinct payloads");

        // ── write tenant A, then tenant B (interleaved cold mounts) ──
        for (tenant, data) in [("tenant-a", &data_a), ("tenant-b", &data_b)] {
            let mut state = FsState::new(&mgr, tenant)
                .await
                .unwrap_or_else(|e| panic!("mount {tenant}: {e:?}"));
            dispatch::init_root(&mut state).await.expect("init_root");
            meta::put_inode(&mut state, ino, &meta::new_file_meta(0o644, 0, 0))
                .await
                .expect("put_inode");
            let n = write::write(&mut state, ino, 0, data).await.expect("write");
            assert_eq!(n as usize, total, "{tenant} write full length");
            write::flush_inode(&mut state, ino).await.expect("flush");
            // dropped here → cold on next mount.
        }

        // ── cold-remount each tenant, assert it reads back ITS OWN bytes ──
        for (tenant, want) in [("tenant-a", &data_a), ("tenant-b", &data_b)] {
            let mut state = FsState::new(&mgr, tenant)
                .await
                .unwrap_or_else(|e| panic!("remount {tenant}: {e:?}"));
            dispatch::init_root(&mut state).await.expect("init_root2");
            let back = read::read(&mut state, ino, 0, total as u32)
                .await
                .expect("cold read");
            assert_eq!(back.len(), total, "{tenant} cold read length");
            assert!(
                back == **want,
                "{tenant} cold read did NOT match its OWN written bytes — \
                 fs/{{tenant}}/ prefix failed to isolate two tenants sharing ino {ino}"
            );
        }
    });
}

