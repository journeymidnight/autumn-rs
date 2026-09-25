//! Whole-file publication (`autumn_fs::publish`), the S3 gateway's PUT /
//! Copy / delete core, against a live in-process cluster: conditional
//! publish decided by the partition server, replaced files reclaimed, races
//! on directory creation, a busy in-place writer refused, and a dead
//! session's late publish fenced out after another session recovers it.

mod support;

use std::time::Duration;

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::LEASE_MODE_WRITE;

use autumn_fs::publish::{self, Condition, NewFile, PublishError};
use autumn_fs::state::FsState;
use autumn_fs::{dir, key, meta, read};

use support::*;

fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 8);
    let mut i = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    while out.len() < len {
        out.extend_from_slice(&i.to_le_bytes());
        i = i.wrapping_add(1);
    }
    out.truncate(len);
    out
}

async fn boot(mgr_addr: std::net::SocketAddr, n1: std::net::SocketAddr, n2: std::net::SocketAddr, base: u16, part: u64) -> ClusterClient {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1, n2, base).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part, log, row, meta, b"", b"\xff\xff\xff\xff").await;
    let ps_addr = pick_addr();
    start_partition_server(base as u64, mgr_addr, ps_addr);
    compio::time::sleep(Duration::from_millis(1500)).await;
    let _ = RpcClient::connect(ps_addr).await.unwrap();
    let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
    c.set_rpc_timeout(Duration::from_secs(30));
    c
}

async fn put(st: &mut FsState, parent: u64, name: &str, data: &[u8], cond: Condition) -> Result<u64, PublishError> {
    let mut f = NewFile::begin(st, parent, name.as_bytes()).await?;
    for chunk in data.chunks(3 << 20) {
        f.write(st, chunk).await?;
    }
    f.finish(st).await?;
    let ino = f.ino;
    f.publish(st, cond).await.map(|()| ino)
}

async fn read_name(st: &mut FsState, parent: u64, name: &str) -> Option<Vec<u8>> {
    let (ino, m) = dir::lookup_opt(st, parent, std::ffi::OsStr::new(name)).await.unwrap()?;
    let mut out = Vec::new();
    while (out.len() as u64) < m.size {
        let got = read::read(st, ino, out.len() as i64, 16 << 20).await.unwrap();
        assert!(!got.is_empty());
        out.extend_from_slice(&got);
    }
    Some(out)
}

#[test]
#[ignore]
fn publish_conditions_replace_delete_and_dead_sessions() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot(mgr_addr, n1, n2, 151, 15101).await;
        let mgr = mgr_addr.to_string();
        let mut st = FsState::new(&mgr).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");

        // Nested directories, created by two sessions racing: one inode.
        let mut other = FsState::new(&mgr).await.expect("mount2");
        let comps: Vec<&[u8]> = vec![b"bkt", b"t", b"docs.lance", b"_versions"];
        let (a, b) = futures::join!(
            publish::ensure_dirs(&mut st, 1, &comps),
            publish::ensure_dirs(&mut other, 1, &comps)
        );
        let dir_ino = a.unwrap();
        assert_eq!(dir_ino, b.unwrap(), "both sessions resolve the same directory");

        // Inline and multi-extent objects, conditionally created.
        let small = b"manifest v1".to_vec();
        put(&mut st, dir_ino, "1.manifest", &small, Condition::Absent).await.expect("create small");
        assert_eq!(read_name(&mut st, dir_ino, "1.manifest").await.unwrap(), small);
        let big = pattern(20 << 20, 1);
        let big_ino = put(&mut st, dir_ino, "big.lance", &big, Condition::Absent).await.expect("create big");
        assert_eq!(read_name(&mut st, dir_ino, "big.lance").await.unwrap(), big);

        // A second If-None-Match loses, and leaves nothing behind.
        let loser = NewFile::begin(&mut st, dir_ino, b"1.manifest").await.unwrap();
        let loser_ino = loser.ino;
        let mut loser = loser;
        loser.write(&mut st, &pattern(1 << 20, 2)).await.unwrap();
        loser.finish(&mut st).await.unwrap();
        assert!(matches!(loser.publish(&mut st, Condition::Absent).await, Err(PublishError::PreconditionFailed)));
        assert_eq!(st.kv_get_opt(&key::inode_key(loser_ino)).await.unwrap(), None, "loser undone");
        let (s, _) = st.kv_range_page(&key::segc_prefix(loser_ino), &key::segc_prefix(loser_ino), 10).await.unwrap();
        assert!(s.is_empty(), "loser's data object reclaimed");
        assert_eq!(read_name(&mut st, dir_ino, "1.manifest").await.unwrap(), small, "winner intact");

        // If-Match: the right version replaces, a stale one is refused.
        let m = meta::get_inode_uncached(&mut st, big_ino).await.unwrap().0;
        let v1 = Condition::Version { ino: big_ino, generation: m.generation };
        let big2 = pattern(9 << 20, 3);
        let big2_ino = put(&mut st, dir_ino, "big.lance", &big2, v1).await.expect("if-match replace");
        assert_eq!(read_name(&mut st, dir_ino, "big.lance").await.unwrap(), big2);
        assert!(matches!(
            put(&mut st, dir_ino, "big.lance", b"x", v1).await,
            Err(PublishError::PreconditionFailed)
        ));
        assert!(matches!(
            put(&mut st, dir_ino, "nope", b"x", v1).await,
            Err(PublishError::NoSuchKey)
        ));
        // The replaced inode is unreachable and nobody holds it: gone.
        assert_eq!(st.kv_get_opt(&key::inode_key(big_ino)).await.unwrap(), None, "replaced inode reclaimed");

        // An in-place writer on the current version makes a replace busy.
        let fuse = DaemonClientId::new_fuse("other-mount");
        assert!(matches!(
            lease::acquire(&st.client, &fuse, big2_ino, LEASE_MODE_WRITE).await.unwrap(),
            AcquireResult::Granted(_)
        ));
        assert!(matches!(put(&mut st, dir_ino, "big.lance", b"y", Condition::None).await, Err(PublishError::Busy(_))));
        assert!(matches!(publish::delete_name(&mut st, dir_ino, b"big.lance", Condition::None).await, Err(PublishError::Busy(_))));
        lease::release(&st.client, &fuse, big2_ino).await.unwrap();

        // Delete: present, then absent (idempotent), a directory is not an object.
        assert!(publish::delete_name(&mut st, dir_ino, b"big.lance", Condition::None).await.unwrap());
        assert!(!publish::delete_name(&mut st, dir_ino, b"big.lance", Condition::None).await.unwrap());
        assert!(!publish::delete_name(&mut st, 1, b"bkt", Condition::None).await.unwrap());
        assert_eq!(st.kv_get_opt(&key::inode_key(big2_ino)).await.unwrap(), None);
        assert!(matches!(
            publish::delete_name(&mut st, dir_ino, b"1.manifest", Condition::Absent).await,
            Err(PublishError::PreconditionFailed)
        ));

        // This client's own in-place writer is a conflict too (the manager
        // lets a client replace over its own WRITE).
        let own = put(&mut st, dir_ino, "own.lance", b"v1", Condition::None).await.unwrap();
        st.held_leases.borrow_mut().insert(
            own,
            autumn_fs::state::FuseLease { writer_refs: 1, reader_refs: 0, mode: LEASE_MODE_WRITE, lease_epoch: 1, revoked: false },
        );
        assert!(matches!(put(&mut st, dir_ino, "own.lance", b"v2", Condition::None).await, Err(PublishError::Busy(_))));
        st.held_leases.borrow_mut().remove(&own);

        // The swap is the commit point: failing to retire the replaced inode
        // (here its meta is unreadable) leaves the new file published, and
        // the retirement recorded for recovery.
        st.kv_put_fenced(&key::inode_key(own), b"not an inode", autumn_client::WriteLease::ANON).await.unwrap();
        let fresh = put(&mut st, dir_ino, "own.lance", b"v2", Condition::None).await.expect("published despite the retire failing");
        assert_eq!(read_name(&mut st, dir_ino, "own.lance").await.unwrap(), b"v2");
        assert!(st.kv_get_opt(&key::inode_key(fresh)).await.unwrap().is_some(), "the new file is not undone");
        let session = st.session.unwrap();
        assert!(st.kv_get_opt(&key::pending_key(session, own)).await.unwrap().is_some(), "retire left for recovery");
        st.kv_delete(&key::pending_key(session, own)).await.unwrap();

        // A session that dies mid-put: its lease expires (no heartbeat runs in
        // this test), another session recovers it, and the late publish of
        // the dead one is fenced out even though its process is still here.
        let mut dead = FsState::new(&mgr).await.expect("mount3");
        let mut f = NewFile::begin(&mut dead, dir_ino, b"orphan.lance").await.unwrap();
        f.write(&mut dead, &pattern(2 << 20, 4)).await.unwrap();
        f.finish(&mut dead).await.unwrap();
        let orphan = f.ino;
        compio::time::sleep(Duration::from_secs(33)).await;
        assert_eq!(publish::recover_dead_sessions(&mut st).await.unwrap(), 2, "the dead session and `other`");
        assert_eq!(st.kv_get_opt(&key::inode_key(orphan)).await.unwrap(), None, "undone by recovery");
        match f.publish(&mut dead, Condition::None).await {
            Err(PublishError::Other(e)) => assert!(format!("{e:#}").contains("fenced"), "{e:#}"),
            other => panic!("a recovered session's publish must be fenced, got {other:?}"),
        }
        assert!(dir::lookup_opt(&mut st, dir_ino, std::ffi::OsStr::new("orphan.lance")).await.unwrap().is_none());
    });
}

/// A retire record whose swap never landed — the name went to another
/// session's file instead, which retired the old inode itself — must not
/// drop a name from that inode again at recovery: with a second hard link
/// that second drop deletes a file another name still reaches.
#[test]
#[ignore]
fn a_retire_whose_swap_lost_leaves_a_linked_inode_alone() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot(mgr_addr, n1, n2, 152, 15201).await;
        let mgr = mgr_addr.to_string();
        let mut st = FsState::new(&mgr).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");
        let body = pattern(1 << 20, 7);
        let linked = put(&mut st, 1, "h", &body, Condition::None).await.unwrap();
        dir::link(&mut st, linked, 1, std::ffi::OsStr::new("h2")).await.expect("hard link");

        // A session that set out to replace `h` and died before its swap
        // landed (or with its outcome unknown): the record names the file it
        // meant to publish.
        let mut dead = FsState::new(&mgr).await.expect("mount2");
        let lease = publish::session_lease(&mut dead).await.unwrap();
        let s = lease.inode_hint;
        let never_published = meta::alloc_inode(&mut dead).await.unwrap();
        let op = autumn_fs::schema::PendingOp::Retire { parent: 1, name: b"h".to_vec(), ino: linked, successor: Some(never_published) };
        dead.kv_put_fenced(&key::pending_key(s, linked), &autumn_fs::schema::encode_pending(&op), lease).await.unwrap();
        lease::release(&dead.client, &dead.client_id, s).await.unwrap();

        // Meanwhile another session replaced `h`, retiring `linked` once.
        put(&mut st, 1, "h", b"other", Condition::None).await.unwrap();
        let m = meta::get_inode_uncached(&mut st, linked).await.unwrap().0;
        assert_eq!(m.nlink, 1, "one name left");

        let mut rescuer = FsState::new(&mgr).await.expect("mount3");
        assert!(publish::recover_dead_sessions(&mut rescuer).await.unwrap() >= 1);
        assert!(rescuer.kv_get_opt(&key::inode_key(linked)).await.unwrap().is_some(), "still linked, still there");
        assert_eq!(read_name(&mut rescuer, 1, "h2").await.unwrap(), body);
    });
}
