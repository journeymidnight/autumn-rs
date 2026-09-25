//! S3 multipart uploads (`autumn_fs::multipart`) against a live in-process
//! cluster: parts in any order with a retried part, a Complete that touches
//! no part body (proved by deleting the bodies first and counting data keys),
//! list validation, Complete/Abort racing to one winner, a late part cleaning
//! up after itself, a publish with an unknown outcome left alone, a session's
//! own retry finishing a landed Complete, and a dead session's part and
//! Complete recovered.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fs::multipart::{self, MultipartError, PartWriter};
use autumn_fs::publish::{self, Condition, PublishError};
use autumn_fs::schema::{self, UploadState};
use autumn_fs::state::{FsState, Reclaim};
use autumn_fs::{dir, key, meta, read, segment};

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

async fn upload_part(st: &mut FsState, upload: u64, part: u32, data: &[u8]) -> Result<(String, u64), MultipartError> {
    let mut w = PartWriter::begin(st, upload, part).await?;
    for chunk in data.chunks(3 << 20) {
        w.write(&st.client, chunk).await?;
    }
    w.finish(&st.client).await
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

/// Every data key (`[0x03]…`) in the tree.
async fn data_keys(st: &mut FsState) -> Vec<Vec<u8>> {
    let prefix = vec![0x03u8];
    let mut from = prefix.clone();
    let mut out = Vec::new();
    loop {
        let (keys, more) = st.kv_range_page(&prefix, &from, 4096).await.unwrap();
        let last = keys.last().cloned();
        out.extend(keys);
        match (more, last) {
            (true, Some(l)) => from = dir::name_successor(&l),
            _ => return out,
        }
    }
}

fn data_ino_of(etag: &str) -> u64 {
    u64::from_str_radix(&etag[..16], 16).unwrap()
}

fn object_keys(st: &FsState, d: u64, len: u64) -> Vec<Vec<u8>> {
    let lanes = st.stripe_geom.as_ref().unwrap().lanes;
    segment::object_extents(d, len, lanes, schema::MAX_EXTENT as u32).into_iter().map(|(_, k)| k).collect()
}

#[test]
#[ignore]
fn multipart_complete_is_metadata_only_and_races_resolve() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot(mgr_addr, n1, n2, 161, 16101).await;
        let mgr = mgr_addr.to_string();
        let mut st = FsState::new(&mgr).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");
        let d = publish::ensure_dirs(&mut st, 1, &[b"bkt", b"data"]).await.unwrap();
        let mib = 1usize << 20;

        // ── parts out of order, one retried; Complete; read back ──
        let up = multipart::create(&mut st, d, b"a.lance", b"bkt/data/a.lance").await.unwrap();
        let p1 = pattern(5 * mib, 1);
        let p2 = pattern(5 * mib + 123, 2);
        let p3 = pattern(mib + 7, 3);
        let (e3, _) = upload_part(&mut st, up, 3, &p3).await.unwrap();
        let (e2_old, _) = upload_part(&mut st, up, 2, &pattern(5 * mib, 99)).await.unwrap();
        let (e1, _) = upload_part(&mut st, up, 1, &p1).await.unwrap();
        let (e2, _) = upload_part(&mut st, up, 2, &p2).await.unwrap();
        assert_ne!(e2, e2_old, "each attempt has its own ETag");
        // The superseded attempt's ETag no longer names the part.
        assert!(matches!(
            multipart::complete(&mut st, up, b"bkt/data/a.lance", &[(1, e1.clone()), (2, e2_old.clone()), (3, e3.clone())], Condition::None).await,
            Err(MultipartError::InvalidPart(2))
        ));
        let done = multipart::complete(&mut st, up, b"bkt/data/a.lance", &[(1, e1.clone()), (2, e2.clone()), (3, e3.clone())], Condition::Absent)
            .await
            .expect("complete");
        let ino = done.ino;
        let mut want = p1.clone();
        want.extend_from_slice(&p2);
        want.extend_from_slice(&p3);
        assert_eq!(meta::get_inode(&mut st, ino).await.unwrap().size, want.len() as u64);
        assert_eq!(read_name(&mut st, d, "a.lance").await.unwrap(), want);
        let map = meta::get_inode(&mut st, ino).await.unwrap().segments.unwrap();
        let named: Vec<u64> = map.inline.iter().map(|s| s.data_ino).collect();
        assert_eq!(named, vec![data_ino_of(&e1), data_ino_of(&e2), data_ino_of(&e3)], "the parts' own objects, in place");
        for k in object_keys(&st, data_ino_of(&e2_old), 5 * mib as u64) {
            assert_eq!(st.kv_get_opt(&k).await.unwrap(), None, "superseded attempt reclaimed");
        }
        assert_eq!(st.kv_get_opt(&key::upload_key(up)).await.unwrap(), None, "upload gone");
        let (keys, _) = st.kv_range_page(&key::upload_alloc_prefix(up), &key::upload_alloc_prefix(up), 10).await.unwrap();
        assert!(keys.is_empty(), "alloc records gone");
        // A retried Complete after a lost reply, with the upload record gone:
        // the same answer, as from S3 — for this upload's key only.
        assert_eq!(
            multipart::complete(&mut st, up, b"bkt/data/a.lance", &[(1, e1.clone())], Condition::Absent).await.unwrap(),
            done,
            "a retry after success gets the original answer"
        );
        assert!(matches!(
            multipart::complete(&mut st, up, b"bkt/data/other", &[(1, e1.clone())], Condition::None).await,
            Err(MultipartError::NoSuchUpload)
        ));
        // The file owns the objects: overwriting it reclaims them.
        let mut f = publish::NewFile::begin(&mut st, d, b"a.lance").await.unwrap();
        f.write(&mut st, b"small").await.unwrap();
        f.finish(&mut st).await.unwrap();
        f.publish(&mut st, Condition::None).await.unwrap();
        autumn_fs::extent::sweep_unlink_tombstones(&mut st).await.unwrap();
        for k in object_keys(&st, data_ino_of(&e1), 5 * mib as u64) {
            assert_eq!(st.kv_get_opt(&k).await.unwrap(), None, "file's objects reclaimed with it");
        }
        // The answer outlives the file it named, as S3's does...
        assert_eq!(
            multipart::complete(&mut st, up, b"bkt/data/a.lance", &[(1, e1.clone())], Condition::None).await.unwrap(),
            done
        );
        // ...until its retention passes.
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        assert_eq!(multipart::expire_completed(&mut st, now).await.unwrap(), 0, "not due yet");
        assert!(multipart::expire_completed(&mut st, now + multipart::COMPLETED_RETENTION_SECS + 5).await.unwrap() >= 1);
        assert!(matches!(
            multipart::complete(&mut st, up, b"bkt/data/a.lance", &[(1, e1.clone())], Condition::None).await,
            Err(MultipartError::NoSuchUpload)
        ));
        assert_eq!(st.kv_get_opt(&key::completed_upload_key(up)).await.unwrap(), None);
        let xp = key::completed_expiry_prefix();
        assert!(st.kv_range_page(&xp, &xp, 10).await.unwrap().0.is_empty(), "expiry entries gone");

        // ── Complete reads and writes no body: delete every body first ──
        let up = multipart::create(&mut st, d, b"b.lance", b"bkt/data/b.lance").await.unwrap();
        let mut list = Vec::new();
        let mut sizes = Vec::new();
        for i in 1..=4u32 {
            let len = if i < 4 { 5 * mib } else { 3 * mib + 1 };
            let (e, _) = upload_part(&mut st, up, i, &pattern(len, 10 + i as u64)).await.unwrap();
            sizes.push(len as u64);
            list.push((i, e));
        }
        for ((_, e), len) in list.iter().zip(&sizes) {
            for k in object_keys(&st, data_ino_of(e), *len) {
                st.kv_delete(&k).await.unwrap();
            }
        }
        let before = data_keys(&mut st).await;
        let ino_b = multipart::complete(&mut st, up, b"bkt/data/b.lance", &list, Condition::Absent).await.expect("complete without bodies").ino;
        assert_eq!(meta::get_inode(&mut st, ino_b).await.unwrap().size, sizes.iter().sum::<u64>());
        assert_eq!(data_keys(&mut st).await, before, "Complete wrote no data key");
        let map = meta::get_inode(&mut st, ino_b).await.unwrap().segments.unwrap();
        let named: Vec<u64> = map.inline.iter().map(|s| s.data_ino).collect();
        assert_eq!(named, list.iter().map(|(_, e)| data_ino_of(e)).collect::<Vec<_>>());
        // With the bodies gone a read fails rather than inventing zeros.
        let (bino, _) = dir::lookup_opt(&mut st, d, std::ffi::OsStr::new("b.lance")).await.unwrap().unwrap();
        assert!(read::read(&mut st, bino, 0, 4096).await.is_err());

        // ── list validation ──
        let up = multipart::create(&mut st, d, b"c.lance", b"bkt/data/c.lance").await.unwrap();
        let (s1, _) = upload_part(&mut st, up, 1, &pattern(mib, 20)).await.unwrap();
        let (s2, _) = upload_part(&mut st, up, 2, &pattern(mib, 21)).await.unwrap();
        assert!(matches!(
            multipart::complete(&mut st, up, b"bkt/data/c.lance", &[(1, s1.clone()), (2, s2.clone())], Condition::None).await,
            Err(MultipartError::EntityTooSmall(1))
        ));
        assert!(matches!(
            multipart::complete(&mut st, up, b"bkt/data/c.lance", &[(2, s2.clone()), (1, s1.clone())], Condition::None).await,
            Err(MultipartError::InvalidPartOrder)
        ));
        assert!(matches!(
            multipart::complete(&mut st, up, b"bkt/data/c.lance", &[(1, s1.clone()), (5, s2.clone())], Condition::None).await,
            Err(MultipartError::InvalidPart(5))
        ));
        assert!(matches!(PartWriter::begin(&mut st, up, 0).await, Err(MultipartError::InvalidPart(0))));
        // A failed condition reopens the upload.
        let (only, _) = (s2.clone(), ());
        assert!(matches!(
            multipart::complete(&mut st, up, b"bkt/data/c.lance", &[(2, only.clone())], Condition::Version { ino: 1, generation: 1 }).await,
            Err(MultipartError::Publish(_))
        ));
        assert_eq!(multipart::get(&mut st, up).await.unwrap().unwrap().state, UploadState::Open);
        assert!(dir::lookup_opt(&mut st, d, std::ffi::OsStr::new("c.lance")).await.unwrap().is_none());

        // ── Abort wins: no Complete afterwards, data reclaimed; a late part
        //    cleans up after itself ──
        let mut late = PartWriter::begin(&mut st, up, 3).await.unwrap();
        late.write(&st.client, &pattern(2 * mib, 30)).await.unwrap();
        multipart::abort(&mut st, up).await.expect("abort");
        assert!(matches!(late.finish(&st.client).await, Err(MultipartError::NoSuchUpload)));
        assert!(matches!(
            multipart::complete(&mut st, up, b"bkt/data/c.lance", &[(2, only.clone())], Condition::None).await,
            Err(MultipartError::NoSuchUpload)
        ));
        multipart::sweep_uploads(&mut st).await.unwrap();
        assert_eq!(multipart::get(&mut st, up).await.unwrap(), None, "aborted upload fully reclaimed");
        for (e, len) in [(&s1, mib as u64), (&s2, mib as u64)] {
            for k in object_keys(&st, data_ino_of(e), len) {
                assert_eq!(st.kv_get_opt(&k).await.unwrap(), None);
            }
        }

        // ── Complete wins: Abort is NoSuchUpload and the file stays ──
        let up = multipart::create(&mut st, d, b"e.lance", b"bkt/data/e.lance").await.unwrap();
        let body = pattern(mib + 5, 40);
        let (e, _) = upload_part(&mut st, up, 1, &body).await.unwrap();
        multipart::complete(&mut st, up, b"bkt/data/e.lance", &[(1, e)], Condition::None).await.unwrap();
        assert!(matches!(multipart::abort(&mut st, up).await, Err(MultipartError::NoSuchUpload)));
        assert_eq!(read_name(&mut st, d, "e.lance").await.unwrap(), body);

        // ── a publish whose outcome is unknown is not undone ──
        // An undecodable dirent at the name makes the publish fail, before any
        // swap, with `PublishError::Other`: the answer that also means "the
        // swap may still land". Undoing then would leave a landed name
        // pointing at a deleted inode, so the file, the Completing state and
        // the pending record all stay for the session's recovery.
        let up_g = multipart::create(&mut st, d, b"g.lance", b"bkt/data/g.lance").await.unwrap();
        let (ge, _) = upload_part(&mut st, up_g, 1, &pattern(mib, 60)).await.unwrap();
        st.kv_put(&key::dirent_key(d, b"g.lance"), b"not a dirent").await.unwrap();
        assert!(matches!(
            multipart::complete(&mut st, up_g, b"bkt/data/g.lance", &[(1, ge.clone())], Condition::None).await,
            Err(MultipartError::Publish(PublishError::Other(_)))
        ));
        let g_state = multipart::get(&mut st, up_g).await.unwrap().unwrap().state;
        let UploadState::Completing { new_ino: g_ino, session: g_s, .. } = g_state else {
            panic!("an unknown outcome leaves the upload Completing, got {g_state:?}")
        };
        assert_eq!(Some(g_s), st.session);
        assert!(st.kv_get_opt(&key::inode_key(g_ino)).await.unwrap().is_some(), "an unknown outcome is not undone");
        assert!(st.kv_get_opt(&key::pending_key(g_s, g_ino)).await.unwrap().is_some(), "left for recovery");
        // The same session retrying while the name does not name the file
        // cannot tell a swap still in flight from one that failed: Busy.
        st.kv_delete(&key::dirent_key(d, b"g.lance")).await.unwrap();
        assert!(matches!(
            multipart::complete(&mut st, up_g, b"bkt/data/g.lance", &[(1, ge.clone())], Condition::None).await,
            Err(MultipartError::Busy(_))
        ));

        // ── the session's own retry finishes a Complete whose publish landed ──
        // What a failed Completing -> Completed CAS after a landed publish
        // leaves: the name names the file, the record is still Completing
        // under this session.
        let up_h = multipart::create(&mut st, d, b"h.lance", b"bkt/data/h.lance").await.unwrap();
        let (he, _) = upload_part(&mut st, up_h, 1, &pattern(mib, 70)).await.unwrap();
        let mut hf = publish::NewFile::begin(&mut st, d, b"h.lance").await.unwrap();
        hf.write(&mut st, b"h").await.unwrap();
        hf.finish(&mut st).await.unwrap();
        let h_ino = hf.ino;
        hf.publish(&mut st, Condition::None).await.unwrap();
        let lease = publish::session_lease(&mut st).await.unwrap();
        let cur = st.kv_get_opt(&key::upload_key(up_h)).await.unwrap().unwrap();
        let mut rec = schema::decode_upload(&cur).unwrap();
        rec.state = UploadState::Completing { new_ino: h_ino, session: lease.inode_hint, frozen: vec![data_ino_of(&he)] };
        assert!(st.client.compare_write(&key::upload_key(up_h), Some(&cur), Some(&schema::encode_upload(&rec)), lease).await.unwrap());
        let got = multipart::complete(&mut st, up_h, b"bkt/data/h.lance", &[(1, he.clone())], Condition::None)
            .await
            .expect("the retry finishes a landed Complete");
        assert_eq!(got.ino, h_ino);
        assert_eq!(multipart::get(&mut st, up_h).await.unwrap(), None, "completed and cleaned up");

        // ── with a background reclaimer, Abort and delete hand the bytes
        //    over instead of deleting them in the caller ──
        let handed: std::rc::Rc<std::cell::RefCell<Vec<Reclaim>>> = Default::default();
        let mut hooked = FsState::new(&mgr).await.expect("hooked");
        {
            let handed = handed.clone();
            hooked.reclaim_later = Some(Box::new(move |r| handed.borrow_mut().push(r)));
        }
        let mut reclaimer = FsState::new(&mgr).await.expect("reclaimer");
        let up = multipart::create(&mut hooked, d, b"r.lance", b"bkt/data/r.lance").await.unwrap();
        let r_body = pattern(2 * mib, 80);
        let (re, _) = upload_part(&mut hooked, up, 1, &r_body).await.unwrap();
        multipart::abort(&mut hooked, up).await.expect("abort");
        assert_eq!(handed.borrow().as_slice(), &[Reclaim::Upload(up)]);
        let r_keys = object_keys(&hooked, data_ino_of(&re), r_body.len() as u64);
        assert!(hooked.kv_get_opt(&r_keys[0]).await.unwrap().is_some(), "the abort deleted no data itself");
        let lease = publish::session_lease(&mut reclaimer).await.unwrap();
        assert!(multipart::cleanup(&mut reclaimer, up, lease).await.unwrap());
        for k in &r_keys {
            assert_eq!(reclaimer.kv_get_opt(k).await.unwrap(), None, "the reclaimer deleted it");
        }
        handed.borrow_mut().clear();
        // A delete: the name goes at once, the bytes when the reclaimer — another
        // client — takes the hand-off, which the deleter's own leases on the
        // file no longer stop.
        let up = multipart::create(&mut hooked, d, b"s.lance", b"bkt/data/s.lance").await.unwrap();
        let (se, _) = upload_part(&mut hooked, up, 1, &r_body).await.unwrap();
        let s_ino = multipart::complete(&mut hooked, up, b"bkt/data/s.lance", &[(1, se.clone())], Condition::None).await.unwrap().ino;
        assert_eq!(handed.borrow().as_slice(), &[Reclaim::Upload(up)], "a Complete's leftovers are handed over too");
        handed.borrow_mut().clear();
        assert!(publish::delete_name(&mut hooked, d, b"s.lance", Condition::None).await.unwrap());
        assert_eq!(handed.borrow().as_slice(), &[Reclaim::Inode(s_ino)]);
        assert!(dir::lookup_opt(&mut hooked, d, std::ffi::OsStr::new("s.lance")).await.unwrap().is_none());
        let s_keys = object_keys(&hooked, data_ino_of(&se), r_body.len() as u64);
        assert!(hooked.kv_get_opt(&s_keys[0]).await.unwrap().is_some(), "the delete deleted no data itself");
        assert!(autumn_fs::extent::reclaim_unreachable(&mut reclaimer, s_ino).await.unwrap(), "not held by anyone");
        for k in &s_keys {
            assert_eq!(reclaimer.kv_get_opt(k).await.unwrap(), None);
        }
        assert_eq!(reclaimer.kv_get_opt(&key::unlink_tombstone_key(s_ino)).await.unwrap(), None);

        // ── a dead session's part and Complete ──
        let up = multipart::create(&mut st, d, b"f.lance", b"bkt/data/f.lance").await.unwrap();
        let (fe1, _) = upload_part(&mut st, up, 1, &pattern(mib, 50)).await.unwrap();
        let mut dead = FsState::new(&mgr).await.expect("mount2");
        let mut orphan = PartWriter::begin(&mut dead, up, 2).await.unwrap();
        orphan.write(&dead.client, &pattern(4 * mib, 51)).await.unwrap();
        let dead_session = dead.session.unwrap();
        // The dead session had also started completing: its record and
        // Completing state exist, the name was never published.
        let lease = publish::session_lease(&mut dead).await.unwrap();
        let fake_ino = meta::alloc_inode(&mut dead).await.unwrap();
        let op = schema::PendingOp::Complete { upload: up };
        dead.kv_put_fenced(&key::pending_key(dead_session, fake_ino), &schema::encode_pending(&op), lease).await.unwrap();
        let cur = dead.kv_get_opt(&key::upload_key(up)).await.unwrap().unwrap();
        let mut rec = schema::decode_upload(&cur).unwrap();
        rec.state = UploadState::Completing { new_ino: fake_ino, session: dead_session, frozen: vec![data_ino_of(&fe1)] };
        assert!(dead.client.compare_write(&key::upload_key(up), Some(&cur), Some(&schema::encode_upload(&rec)), lease).await.unwrap());
        // While it lives, an Abort is refused.
        assert!(matches!(multipart::abort(&mut st, up).await, Err(MultipartError::Busy(_))));
        compio::time::sleep(Duration::from_secs(33)).await;
        // `st`'s own session expired too (no heartbeat in this test); a fresh
        // one recovers.
        let mut rescuer = FsState::new(&mgr).await.expect("mount3");
        assert!(publish::recover_dead_sessions(&mut rescuer).await.unwrap() >= 1);
        assert_eq!(multipart::get(&mut rescuer, up).await.unwrap().unwrap().state, UploadState::Open, "unpublished Complete undone");
        assert!(matches!(orphan.finish(&dead.client).await, Err(_)), "the dead writer is fenced out");
        let (keys, _) = rescuer
            .kv_range_page(&key::upload_alloc_prefix(up), &key::upload_alloc_prefix(up), 10)
            .await
            .unwrap();
        assert_eq!(keys.len(), 1, "only part 1's object is still the upload's");
        multipart::abort(&mut rescuer, up).await.unwrap();
        assert_eq!(multipart::get(&mut rescuer, up).await.unwrap(), None);
        // `st`'s own session was recovered too: its Complete of g, whose name
        // was never published, is undone and the upload reopened.
        assert_eq!(multipart::get(&mut rescuer, up_g).await.unwrap().unwrap().state, UploadState::Open);
        assert_eq!(rescuer.kv_get_opt(&key::inode_key(g_ino)).await.unwrap(), None, "g's unpublished file undone");
    });
}
