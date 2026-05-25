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

use autumn_ioring::fuse_read;

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
