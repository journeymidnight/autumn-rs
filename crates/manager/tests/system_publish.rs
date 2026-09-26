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
use autumn_fs::schema::{self, SegcRecord, MAX_EXTENT};
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

/// A dead session behind more live ones than one page of session records
/// (a large gateway fleet: 8 workers each) is still found and recovered.
#[test]
#[ignore]
fn a_dead_session_behind_a_page_of_live_ones_is_recovered() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot(mgr_addr, n1, n2, 153, 15301).await;
        let mgr = mgr_addr.to_string();
        let mut st = FsState::new(&mgr).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");

        // 1030 live sessions, all held by one client, all sorting first.
        let mut fleet = FsState::new(&mgr).await.expect("fleet");
        for _ in 0..1030 {
            let s = meta::alloc_inode(&mut fleet).await.unwrap();
            let granted = lease::acquire(&fleet.client, &fleet.client_id, s, autumn_rpc::manager_rpc::LEASE_MODE_WRITE)
                .await
                .unwrap();
            assert!(matches!(granted, lease::AcquireResult::Granted(_)));
            fleet.kv_put(&key::session_key(s), b"fleet").await.unwrap();
        }

        // A session that died with a file half written: its number comes
        // from a later batch, so its record sorts after all of them.
        let mut dead = FsState::new(&mgr).await.expect("dead");
        let mut f = publish::NewFile::begin(&mut dead, 1, b"orphan").await.unwrap();
        f.write(&mut dead, b"never published").await.unwrap();
        f.finish(&mut dead).await.unwrap();
        let s = dead.session.unwrap();
        assert!(fleet.session.is_none() && s > 1030);
        lease::release(&dead.client, &dead.client_id, s).await.unwrap();

        let mut rescuer = FsState::new(&mgr).await.expect("rescuer");
        assert_eq!(publish::recover_dead_sessions(&mut rescuer).await.unwrap(), 1, "only the dead one");
        assert_eq!(rescuer.kv_get_opt(&key::pending_key(s, f.ino)).await.unwrap(), None, "its write undone");
        assert_eq!(rescuer.kv_get_opt(&key::inode_key(f.ino)).await.unwrap(), None);
        assert_eq!(rescuer.kv_get_opt(&key::session_key(s)).await.unwrap(), None);
    });
}

/// The data object's record at `[0x04]segc/[file]`: its object id and the
/// length it allows; `None` before it is written.
async fn object_record(st: &mut FsState, file: u64) -> Option<(u64, u64)> {
    let p = key::segc_prefix(file);
    let (rows, _) = st.kv_range_page(&p, &p, 10).await.unwrap();
    if rows.is_empty() {
        return None;
    }
    assert_eq!(rows.len(), 1, "one data object");
    let (_, id) = key::parse_segc_key(&rows[0]).unwrap();
    let v = st.kv_get_opt(&rows[0]).await.unwrap().unwrap();
    match schema::decode_segc(&v).unwrap() {
        SegcRecord::Object { len, .. } => Some((id, len)),
        other => panic!("not an object record: {other:?}"),
    }
}

/// Every data key in the tree.
async fn data_keys(st: &mut FsState) -> Vec<Vec<u8>> {
    let p = vec![0x03u8];
    let mut out = Vec::new();
    let mut from = p.clone();
    loop {
        let (rows, more) = st.kv_range_page(&p, &from, 1000).await.unwrap();
        if let Some(last) = rows.last() {
            from = dir::name_successor(last);
        }
        out.extend(rows);
        if !more {
            return out;
        }
    }
}

/// A streamed body (the S3 gateway's PUT) keeps many unit puts in flight
/// while more of it arrives. The object's record must stay ahead of every
/// put — declared up front or raised as the body grows — and end at the
/// exact length; an abort with puts still in flight must leave nothing.
#[test]
#[ignore]
fn a_streamed_object_records_ahead_of_its_writes() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot(mgr_addr, n1, n2, 154, 15401).await;
        let mut st = FsState::new(&mgr_addr.to_string()).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");
        let dir = publish::ensure_dirs(&mut st, 1, &[b"bkt"]).await.unwrap();
        let unit = MAX_EXTENT;

        // (body, declared): unknown over more than the in-flight window, so
        // the record is raised again mid-body; exact; and too short.
        let cases = [
            (10 * unit + 12345, None),
            (3 * unit + 12345, Some((3 * unit + 12345) as u64)),
            (3 * unit + 12345, Some(unit as u64)),
        ];
        for (i, (body, declared)) in cases.into_iter().enumerate() {
            let name = format!("o{i}");
            let data = pattern(body, 10 + i as u64);
            let mut f = NewFile::begin(&mut st, dir, name.as_bytes()).await.unwrap();
            f.start_object(&mut st, declared).await.unwrap();
            let mut handed = 0;
            for chunk in data.chunks(1_000_003) {
                f.write_streamed(chunk).await.unwrap();
                handed += chunk.len();
                // Every full unit is put by now, so the record must already
                // cover it — durably, not just requested.
                let issued = (handed / unit * unit) as u64;
                if issued > 0 {
                    let rec = object_record(&mut st, f.ino).await.map_or(0, |r| r.1);
                    assert!(rec >= issued, "record {rec} behind the puts issued ({issued}), {declared:?}");
                }
            }
            f.flush_streamed().await.unwrap();
            f.finish(&mut st).await.unwrap();
            let ino = f.ino;
            f.publish(&mut st, Condition::Absent).await.unwrap();
            assert_eq!(read_name(&mut st, dir, &name).await.unwrap(), data, "{declared:?}");
            assert_eq!(object_record(&mut st, ino).await.unwrap().1, body as u64, "record trimmed to the length, {declared:?}");
        }

        // A declared length is not trusted to size the record: the record stays
        // within the in-flight window of what was written, so an absurd
        // Content-Length cannot make the abort walk 2^41 keys.
        let before = data_keys(&mut st).await;
        let mut f = NewFile::begin(&mut st, dir, b"liar").await.unwrap();
        f.start_object(&mut st, Some(u64::MAX)).await.unwrap();
        f.write_streamed(&pattern(2 * unit, 7)).await.unwrap();
        let rec = object_record(&mut st, f.ino).await.unwrap().1;
        assert!(rec <= 10 * unit as u64, "record {rec} past the window of 2 written units");
        f.abort(&st.client.clone()).await.unwrap();
        assert_eq!(data_keys(&mut st).await, before, "data keys left behind by the abort");

        // Abort with puts in flight: everything the stream wrote is deleted.
        // The abort comes right after the writes, while their puts are out.
        let before = data_keys(&mut st).await;
        let total = 4 * unit as u64;
        let mut f = NewFile::begin(&mut st, dir, b"aborted").await.unwrap();
        f.start_object(&mut st, Some(total)).await.unwrap();
        // Three full units: each put is issued, none is waited for.
        f.write_streamed(&pattern(3 * unit, 99)).await.unwrap();
        let ino = f.ino;
        f.abort(&st.client.clone()).await.unwrap();
        // A put the abort did not wait for would land after its delete.
        compio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(data_keys(&mut st).await, before, "data keys left behind by the abort");
        let p = key::segc_prefix(ino);
        assert!(st.kv_range_page(&p, &p, 10).await.unwrap().0.is_empty(), "record reclaimed");
        assert_eq!(st.kv_get_opt(&key::inode_key(ino)).await.unwrap(), None);
    });
}
