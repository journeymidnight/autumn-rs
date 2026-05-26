//! F248 — the io_uring daemon reads autumn-fuse files (inode + F247 variable-
//! length extents), via `autumn_ioring::fuse_read` (path resolve + extent read).
//!
//! Pre-F248 the daemon treated the SQE path as a flat KV key and pulled the
//! WHOLE value per Read. F248 resolves a fuse PATH → inode → extent map at Open
//! and fans out across only the covering extents' sub-ranges via `get_many_into`.
//!
//! This seeds a real fuse file through the fuse WRITE path (dirent + inode +
//! F247 extents) on a live cluster, then drives the daemon-side resolution +
//! read functions directly (the shm ring plumbing is unchanged F244-C; the NEW
//! logic is path-walk + extent fan-out, which this covers end-to-end):
//!   - `fuse_read::open` walks the path → inode + the 2-extent map (10 MiB file)
//!   - leading-slash path variant resolves identically
//!   - full / whole-extent / sub-range / cross-extent / EOF reads byte-exact
//!   - a missing path → Err (→ ENOENT at the daemon)

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fuse::schema::{self, DirentValue, DT_REG, MAX_EXTENT, ROOT_INO};
use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, key, meta, write};

use autumn_ioring::{fuse_read, fuse_write};

use support::*;

fn pattern(len: usize) -> Vec<u8> {
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

#[test]
#[ignore]
fn f248_ioring_reads_fuse_file() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 136, 13601).await;

        // ── Seed a real fuse file "model.bin" under root (dirent + inode + F247
        //    extents) via the fuse write path. 10 MiB → 2 extents.
        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");

        let total = 10 * 1024 * 1024usize;
        let data = pattern(total);
        let ino = 100u64;

        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m)
            .await
            .expect("put_inode");
        // dirent so the daemon's path walk (root → "model.bin") resolves.
        let dk = key::dirent_key(ROOT_INO, b"model.bin");
        let dv = schema::encode_dirent(&DirentValue {
            child_inode: ino,
            file_type: DT_REG,
        });
        state.kv_put(&dk, &dv).await.expect("put dirent");
        write::write(&mut state, ino, 0, &data)
            .await
            .expect("write");
        write::flush_inode(&mut state, ino).await.expect("flush");

        // ── Drive the daemon-side resolution + read (its OWN ClusterClient).
        let dclient = ClusterClient::connect(&mgr_addr.to_string())
            .await
            .expect("daemon client");
        dclient.set_rpc_timeout(Duration::from_secs(30));

        // Open: path walk → inode + variable-length extent map.
        let opened = fuse_read::open(&dclient, b"model.bin")
            .await
            .expect("open model.bin");
        assert_eq!(opened.size, total as u64, "resolved size");
        assert_eq!(
            opened.extents,
            vec![(0, MAX_EXTENT as u32), (MAX_EXTENT as u64, 2 * 1024 * 1024)],
            "two variable-length extents (off 0 + off 8 MiB)"
        );

        // Leading-slash path resolves identically.
        let opened_slash = fuse_read::open(&dclient, b"/model.bin")
            .await
            .expect("open /model.bin");
        assert_eq!(opened_slash.size, total as u64);

        // Full read (fans out across both extents).
        let full = fuse_read::read(&dclient, &opened, 0, total as u32)
            .await
            .expect("full read");
        assert_eq!(full.len(), total);
        assert!(full == data, "full read mismatch");

        // Whole first extent (8 MiB ≥ 64 KiB → ZC branch inside get_many_into).
        let e0 = fuse_read::read(&dclient, &opened, 0, MAX_EXTENT as u32)
            .await
            .expect("extent0");
        assert!(e0 == data[..MAX_EXTENT], "extent0 mismatch");

        // Small sub-range (< 64 KiB → regular get).
        let sub = fuse_read::read(&dclient, &opened, 1000, 4096)
            .await
            .expect("sub");
        assert!(sub == data[1000..1000 + 4096], "sub-range mismatch");

        // Cross-extent sub-range spanning the 8 MiB boundary.
        let coff = (MAX_EXTENT - 50 * 1024) as u64;
        let clen = 100 * 1024u32;
        let cross = fuse_read::read(&dclient, &opened, coff, clen)
            .await
            .expect("cross");
        assert!(
            cross == data[coff as usize..coff as usize + clen as usize],
            "cross-extent mismatch"
        );

        // EOF clamp.
        let tail = fuse_read::read(&dclient, &opened, (total - 10) as u64, 4096)
            .await
            .expect("tail");
        assert_eq!(tail.len(), 10, "EOF clamp");
        assert!(tail == data[total - 10..], "tail mismatch");

        // Missing path → Err (daemon maps to ENOENT).
        assert!(
            fuse_read::open(&dclient, b"nope.bin").await.is_err(),
            "missing path errors"
        );
    });
}

#[test]
#[ignore]
fn f242_ioring_writes_fuse_file() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 137, 13701).await;

        // ── Seed a fuse INODE (no data extents) via the fuse meta path: dirent
        // for the daemon's path walk + empty InodeMeta. The daemon's Write
        // (F242) is what populates the file extents.
        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");
        let ino = 200u64;
        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m)
            .await
            .expect("put_inode");
        let dk = key::dirent_key(ROOT_INO, b"daemon-write.bin");
        let dv = schema::encode_dirent(&DirentValue {
            child_inode: ino,
            file_type: DT_REG,
        });
        state.kv_put(&dk, &dv).await.expect("put dirent");

        // ── Drive the daemon-side WRITE path directly (mirrors how the daemon
        // services an Opcode::Write SQE: open → write_into → mutate
        // OpenedExtents). 10 MiB written in 3 chunks: 8 MiB append + 2 MiB
        // append (crosses extent boundary, so plan_write should split into
        // two steps + create extents at 0 and 8 MiB) + a small overwrite.
        let dclient = ClusterClient::connect(&mgr_addr.to_string())
            .await
            .expect("daemon client");
        dclient.set_rpc_timeout(Duration::from_secs(30));

        let mut opened = fuse_read::open(&dclient, b"daemon-write.bin")
            .await
            .expect("open empty");
        assert_eq!(opened.size, 0);
        assert!(opened.extents.is_empty());

        let total = 10 * 1024 * 1024usize;
        let data = pattern(total);

        // Two writes: first 8 MiB (single Append, ZC path), then 2 MiB
        // continuation (Append in the new extent at off=8 MiB).
        let n1 = fuse_write::write_into(&dclient, &mut opened, 0, &data[..MAX_EXTENT])
            .await
            .expect("write extent 0");
        assert_eq!(n1, MAX_EXTENT);
        assert_eq!(opened.size, MAX_EXTENT as u64);
        assert_eq!(opened.extents, vec![(0, MAX_EXTENT as u32)]);

        let n2 = fuse_write::write_into(
            &dclient,
            &mut opened,
            MAX_EXTENT as u64,
            &data[MAX_EXTENT..],
        )
        .await
        .expect("write extent 1");
        assert_eq!(n2, 2 * 1024 * 1024);
        assert_eq!(opened.size, total as u64);
        assert_eq!(
            opened.extents,
            vec![(0, MAX_EXTENT as u32), (MAX_EXTENT as u64, 2 * 1024 * 1024)],
            "two variable-length extents (off 0 + off 8 MiB)"
        );

        // ── Verify: daemon Read sees what daemon Write wrote.
        let full = fuse_read::read(&dclient, &opened, 0, total as u32)
            .await
            .expect("daemon-side full read");
        assert_eq!(full.len(), total);
        assert!(full == data, "daemon read mismatch after daemon write");

        // Small sub-range (<64K → regular MSG_GET) inside extent 0.
        let sub = fuse_read::read(&dclient, &opened, 1000, 4096)
            .await
            .expect("daemon-side sub-range");
        assert!(sub == data[1000..1000 + 4096], "sub-range mismatch");

        // Cross-extent read spanning the 8 MiB boundary.
        let coff = (MAX_EXTENT - 32 * 1024) as u64;
        let clen = 64 * 1024u32;
        let cross = fuse_read::read(&dclient, &opened, coff, clen)
            .await
            .expect("cross-extent");
        assert!(
            cross == data[coff as usize..coff as usize + clen as usize],
            "cross-extent mismatch"
        );

        // ── Cross-verify: an INDEPENDENT Open sees the persisted size + map
        // (the InodeMeta.size write in fuse_write::write_into is what makes
        // load_extent_map infer the right length for the trailing 2 MiB extent).
        let opened2 = fuse_read::open(&dclient, b"daemon-write.bin")
            .await
            .expect("re-open after daemon write");
        assert_eq!(opened2.size, total as u64, "InodeMeta.size persisted");
        assert_eq!(
            opened2.extents,
            vec![(0, MAX_EXTENT as u32), (MAX_EXTENT as u64, 2 * 1024 * 1024)],
            "extent map reloaded from KV"
        );

        // ── Overwrite (RMW) inside extent 0: rewrite [1000, 1100) with a
        // distinctive byte pattern, then read it back from the SAME extent.
        let patch = vec![0xAAu8; 100];
        let n3 = fuse_write::write_into(&dclient, &mut opened, 1000, &patch)
            .await
            .expect("overwrite RMW");
        assert_eq!(n3, 100);
        // Size unchanged (RMW inside existing extent, end stays within).
        assert_eq!(opened.size, total as u64);
        // Extent map unchanged in shape.
        assert_eq!(opened.extents.len(), 2);
        assert_eq!(opened.extents[0], (0, MAX_EXTENT as u32));
        let after = fuse_read::read(&dclient, &opened, 1000, 100)
            .await
            .expect("read patched bytes");
        assert!(after == patch, "RMW round-trip mismatch");
        // Bytes immediately before and after the patch are unchanged.
        let before = fuse_read::read(&dclient, &opened, 0, 1000)
            .await
            .expect("read prefix");
        assert!(before == data[..1000], "prefix unchanged by RMW");
        let tail = fuse_read::read(&dclient, &opened, 1100, 8192)
            .await
            .expect("read after RMW");
        assert!(tail == data[1100..1100 + 8192], "tail unchanged by RMW");

        // ── Verify the FUSE-mount write path can still read what the daemon
        // wrote: open via FsState (the kernel-fuse-side reader path uses the
        // same KV layout) and read the file via the fuse read::execute path.
        let mut state2 = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new 2");
        let read_full = autumn_fuse::read::read(&mut state2, ino, 0, total as u32)
            .await
            .expect("fuse-side read");
        assert_eq!(read_full.len(), total);
        // Reconstruct the expected file content after the RMW.
        let mut expected = data.clone();
        expected[1000..1100].copy_from_slice(&patch);
        assert!(
            read_full == expected,
            "fuse-side read mismatch — daemon write not visible to mount path"
        );
    });
}
