//! F-fuse-lease-1 — autumn-fuse mount Open/Release acquires +
//! releases an inode lease via the manager. End-to-end:
//!
//! 1. Two `FsState` instances (representing two mounts) acquire
//!    leases via the fuse `Open` dispatch path. A WRITE-mode Open
//!    on the same ino from the second mount returns Err
//!    (conflict).
//! 2. After mount A releases, mount B's WRITE Open succeeds.
//! 3. Read-only Opens from N mounts coexist on the same ino.
//! 4. The held_leases bookkeeping refcounts in-mount: multiple
//!    Opens of the same ino bump the refcount; ReleaseLease fires
//!    only on the 1→0 transition.
//!
//! Drives `dispatch::handle_request` directly through the bridge
//! channel rather than through a real `fuser` mount — same code
//! path, no kernel involvement, fits the existing `#[ignore]`'d
//! cluster-boot pattern.

mod support;

use std::time::Duration;

use autumn_client::lease::{self, AcquireResult, DaemonClientId, HeartbeatResult};
use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    MgrClientId, LEASE_CLIENT_KIND_FUSE, LEASE_MODE_READ, LEASE_MODE_WRITE,
};

use autumn_fuse::schema::{self, DirentValue, DT_REG, ROOT_INO};
use autumn_fuse::state::FsState;
use autumn_fuse::{bridge, dispatch, key, meta};

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

/// Seed a file inode + dirent on `state` so `dispatch::Open` finds
/// it via the inode key path.
async fn seed_file(state: &mut FsState, name: &[u8], ino: u64) {
    let m = meta::new_file_meta(0o644, 0, 0);
    meta::put_inode(state, ino, &m).await.expect("put_inode");
    let dk = key::dirent_key(ROOT_INO, name);
    let dv = schema::encode_dirent(&DirentValue {
        child_inode: ino,
        file_type: DT_REG,
    });
    state.kv_put(&dk, &dv).await.expect("put dirent");
}

/// Open via the dispatch path. Returns Result<u64 /* fh */, anyhow::Error>.
async fn dispatch_open(
    state: &mut FsState,
    ino: u64,
    flags: i32,
) -> anyhow::Result<u64> {
    let (tx, rx) = bridge::reply_channel::<u64>();
    let req = bridge::FsRequest::Open {
        ino,
        flags,
        reply: tx,
    };
    dispatch::handle_request(state, req).await;
    rx.recv_timeout(Duration::from_secs(10))
        .map_err(|_| anyhow::anyhow!("reply timeout"))?
}

async fn dispatch_release(state: &mut FsState, ino: u64) -> anyhow::Result<()> {
    let (tx, rx) = bridge::reply_channel::<()>();
    let req = bridge::FsRequest::Release {
        ino,
        flush: false,
        reply: tx,
    };
    dispatch::handle_request(state, req).await;
    rx.recv_timeout(Duration::from_secs(10))
        .map_err(|_| anyhow::anyhow!("reply timeout"))?
}

#[test]
#[ignore]
fn fuse_two_mounts_write_lease_conflict_and_release_unblocks() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 139, 13901).await;

        // Each "mount" is its own FsState (own ClusterClient + own
        // DaemonClientId UUID). Mirrors two daemons on different
        // hosts mounting the same partition layer.
        let mut mount_a = FsState::new(&mgr_addr.to_string(), "default")
            .await
            .expect("mount A");
        let mut mount_b = FsState::new(&mgr_addr.to_string(), "default")
            .await
            .expect("mount B");
        assert_ne!(
            mount_a.client_id.as_wire().uuid,
            mount_b.client_id.as_wire().uuid,
            "mounts must have distinct UUIDs"
        );
        assert_eq!(
            mount_a.client_id.as_wire().kind,
            LEASE_CLIENT_KIND_FUSE,
            "mount uses FUSE kind"
        );

        dispatch::init_root(&mut mount_a).await.expect("init_root A");
        // Root already exists; init on B is idempotent.
        dispatch::init_root(&mut mount_b).await.expect("init_root B");

        // Seed shared.bin (visible to both mounts since they share
        // the same backing partition).
        let ino = 700u64;
        seed_file(&mut mount_a, b"shared.bin", ino).await;

        // Mount A opens WRITE → granted.
        let fh_a = dispatch_open(&mut mount_a, ino, /* O_RDWR = */ 2)
            .await
            .expect("A open write");
        assert_eq!(fh_a, ino);
        assert_eq!(
            mount_a.held_leases.borrow().get(&ino).map(|s| s.mode),
            Some(LEASE_MODE_WRITE)
        );
        assert_eq!(
            mount_a.held_leases.borrow().get(&ino).map(|s| s.refcount),
            Some(1)
        );

        // Mount B opens WRITE → conflict → Err.
        let err_b = dispatch_open(&mut mount_b, ino, /* O_WRONLY = */ 1)
            .await
            .err()
            .expect("B open write must fail (A holds writer lease)");
        assert!(
            err_b.to_string().contains("EBUSY"),
            "expected EBUSY in error, got: {err_b}"
        );
        // B must NOT have a local lease entry after a failed acquire.
        assert!(mount_b.held_leases.borrow().get(&ino).is_none());

        // Mount A releases → B's WRITE Open now succeeds.
        dispatch_release(&mut mount_a, ino)
            .await
            .expect("A release");
        assert!(mount_a.held_leases.borrow().get(&ino).is_none());

        let fh_b = dispatch_open(&mut mount_b, ino, /* O_RDWR = */ 2)
            .await
            .expect("B open write after A release");
        assert_eq!(fh_b, ino);
    });
}

#[test]
#[ignore]
fn fuse_read_only_opens_coexist() {
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

        let mut mount_a = FsState::new(&mgr_addr.to_string(), "default")
            .await
            .expect("mount A");
        let mut mount_b = FsState::new(&mgr_addr.to_string(), "default")
            .await
            .expect("mount B");
        dispatch::init_root(&mut mount_a).await.expect("init A");
        dispatch::init_root(&mut mount_b).await.expect("init B");

        let ino = 800u64;
        seed_file(&mut mount_a, b"readonly.bin", ino).await;

        // Both mounts open RDONLY → both granted.
        let _fh_a = dispatch_open(&mut mount_a, ino, /* O_RDONLY = */ 0)
            .await
            .expect("A open ro");
        let _fh_b = dispatch_open(&mut mount_b, ino, /* O_RDONLY = */ 0)
            .await
            .expect("B open ro");
        assert_eq!(
            mount_a.held_leases.borrow().get(&ino).map(|s| s.mode),
            Some(LEASE_MODE_READ)
        );
        assert_eq!(
            mount_b.held_leases.borrow().get(&ino).map(|s| s.mode),
            Some(LEASE_MODE_READ)
        );
    });
}

#[test]
#[ignore]
fn fuse_refcount_only_last_release_fires_releaselease() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 141, 14101).await;

        let mut mount = FsState::new(&mgr_addr.to_string(), "default")
            .await
            .expect("mount");
        dispatch::init_root(&mut mount).await.expect("init");
        let ino = 900u64;
        seed_file(&mut mount, b"refcount.bin", ino).await;

        // Open the same ino twice in WRITE mode within the same mount.
        let _ = dispatch_open(&mut mount, ino, /* O_RDWR = */ 2)
            .await
            .expect("first open");
        let _ = dispatch_open(&mut mount, ino, /* O_RDWR = */ 2)
            .await
            .expect("second open (refcount)");
        assert_eq!(
            mount.held_leases.borrow().get(&ino).map(|s| s.refcount),
            Some(2),
            "two opens in same mount → refcount=2"
        );

        // First release → refcount drops to 1; manager-side lease
        // still held (proven by heartbeat round-trip below).
        dispatch_release(&mut mount, ino)
            .await
            .expect("first release");
        assert_eq!(
            mount.held_leases.borrow().get(&ino).map(|s| s.refcount),
            Some(1),
            "first release → refcount=1, lease still held"
        );
        // Manager still recognises the writer (heartbeat Renewed).
        let hb = lease::heartbeat(&mount.client, &mount.client_id, ino)
            .await
            .unwrap();
        assert!(matches!(hb, HeartbeatResult::Renewed(_)));

        // Second release → refcount→0 → ReleaseLease fires.
        dispatch_release(&mut mount, ino)
            .await
            .expect("second release");
        assert!(mount.held_leases.borrow().get(&ino).is_none());

        // Manager now NotHeld.
        let hb = lease::heartbeat(&mount.client, &mount.client_id, ino)
            .await
            .unwrap();
        assert!(matches!(hb, HeartbeatResult::NotHeld));
    });
}

#[test]
#[ignore]
fn fuse_mode_mismatch_in_same_mount_rejects() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 142, 14201).await;

        let mut mount = FsState::new(&mgr_addr.to_string(), "default")
            .await
            .expect("mount");
        dispatch::init_root(&mut mount).await.expect("init");
        let ino = 950u64;
        seed_file(&mut mount, b"mismatch.bin", ino).await;

        // First open as WRITE.
        let _ = dispatch_open(&mut mount, ino, /* O_RDWR = */ 2)
            .await
            .expect("write open");
        // Second open as READ on the same ino in the same mount →
        // rejected (no silent downgrade).
        let err = dispatch_open(&mut mount, ino, /* O_RDONLY = */ 0)
            .await
            .err()
            .expect("READ-after-WRITE in same mount must fail");
        assert!(
            err.to_string().contains("lease mode mismatch"),
            "msg: {err}"
        );
    });
}

async fn dispatch_create(
    state: &mut FsState,
    parent: u64,
    name: &str,
    flags: i32,
) -> anyhow::Result<(autumn_fuse::fuser::FileAttr, u64)> {
    let (tx, rx) = bridge::reply_channel::<(autumn_fuse::fuser::FileAttr, u64)>();
    let req = bridge::FsRequest::Create {
        parent,
        name: name.into(),
        mode: 0o644,
        flags,
        reply: tx,
    };
    dispatch::handle_request(state, req).await;
    rx.recv_timeout(Duration::from_secs(10))
        .map_err(|_| anyhow::anyhow!("reply timeout"))?
}

#[test]
#[ignore]
fn fuse_create_acquires_writer_lease() {
    // Regression for coco P1 #1: `FsRequest::Create` used to
    // bypass the lease layer entirely, so a freshly-created
    // writeable fd had no manager-side lease — a concurrent
    // mount could acquire the writer lease on the same ino.
    // Post-fix: Create runs the same AcquireLease path as Open.
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 143, 14301).await;

        let mut mount_a = FsState::new(&mgr_addr.to_string(), "default")
            .await
            .expect("mount A");
        let mut mount_b = FsState::new(&mgr_addr.to_string(), "default")
            .await
            .expect("mount B");
        dispatch::init_root(&mut mount_a).await.expect("init A");
        dispatch::init_root(&mut mount_b).await.expect("init B");

        // Mount A creates the file. Create with O_WRONLY → WRITE
        // lease. After Create returns, A's held_leases MUST
        // contain the new ino with WRITE mode.
        let (attr, fh) = dispatch_create(&mut mount_a, ROOT_INO, "fresh.bin", /* O_WRONLY */ 1)
            .await
            .expect("A create");
        assert_eq!(fh, attr.ino);
        let ino = attr.ino;
        assert_eq!(
            mount_a.held_leases.borrow().get(&ino).map(|s| s.mode),
            Some(LEASE_MODE_WRITE),
            "Create with O_WRONLY MUST acquire WRITE lease"
        );

        // Mount B tries to open the new file WRITE → conflict.
        // (B has to lookup the name first via Open — but our
        // simplified test uses ino directly; manager-side the
        // lease state is what matters.)
        let err = dispatch_open(&mut mount_b, ino, /* O_RDWR = */ 2)
            .await
            .err()
            .expect("B open WRITE must conflict with A's Create-acquired lease");
        assert!(
            err.to_string().contains("EBUSY"),
            "expected EBUSY, got: {err}"
        );
    });
}

#[test]
#[ignore]
fn fuse_uses_fuse_kind_identity() {
    // Sanity: the FUSE mount's DaemonClientId carries kind=FUSE so
    // an `autumn-op` lease list can tell which daemon family holds
    // a given lease.
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap();
        let id_fuse = DaemonClientId::new_fuse("host-x");
        let id_iouring = DaemonClientId::new("host-x");
        assert_eq!(id_fuse.as_wire().kind, LEASE_CLIENT_KIND_FUSE);
        assert_ne!(id_iouring.as_wire().kind, LEASE_CLIENT_KIND_FUSE);
        // Both can hold the same path's leases independently — the
        // manager keys on (kind, uuid), so different kinds are
        // different clients.
        let id_b = DaemonClientId::from_wire(MgrClientId {
            kind: id_fuse.as_wire().kind,
            uuid: [0xff; 16],
            host: "other".to_string(),
        });
        let r1 = lease::acquire(&cluster, &id_fuse, 1234, LEASE_MODE_WRITE)
            .await
            .unwrap();
        assert!(matches!(r1, AcquireResult::Granted(_)));
        let r2 = lease::acquire(&cluster, &id_b, 1234, LEASE_MODE_WRITE)
            .await
            .unwrap();
        assert!(matches!(r2, AcquireResult::Conflict { .. }));
    });
}
