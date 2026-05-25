//! F244-B — autumn-fuse read path now routes through the shared
//! `ClusterClient::get_many_into` batch primitive (was a hand-rolled `join_all`).
//!
//! This exercises the real `read::prepare` + `read::execute` against a live
//! cluster (manager + 2 EN + PS): seed a file's chunks + inode directly in the
//! KV, then read it back via `read::read` and assert byte-exactness across the
//! paths F244-B touches:
//!   - multi-chunk full read (3 × 256 KiB-ish chunks → 3 `GetManyItem`s)
//!   - whole 256 KiB chunk (>= 64 KiB → the ZC `MSG_GET_ZC` branch)
//!   - small sub-range (< 64 KiB → the regular `MSG_GET` branch)
//!   - cross-chunk sub-range (offset/length spanning two chunks)
//!
//! (The kernel FUSE mount layer is unchanged by F244-B; a mount probe confirmed
//! the env can mount, but the read *logic* is what changed, so we drive it
//! directly through `FsState`.)

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, key, meta, read};

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
    cluster.set_rpc_timeout(Duration::from_secs(15));
    cluster
}

#[test]
#[ignore]
fn f244b_fuse_read_via_get_many_into() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // Set up cluster + partition + PS (throwaway admin client).
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 130, 13001).await;

        // The fuse daemon connects fresh to the manager.
        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");

        // A 600 KiB file = 3 chunks (256 KiB + 256 KiB + 88 KiB). CHUNK_SIZE is
        // 256 KiB; the first two qualify for ZC (>= 64 KiB), as does the 88 KiB
        // tail. The inode is NOT inline (> 4 KiB), so reads route to chunks.
        let chunk = 256 * 1024usize;
        let total = 2 * chunk + 88 * 1024;
        let data = pattern(total);
        let ino = 100u64;

        for i in 0..3usize {
            let start = i * chunk;
            let end = (start + chunk).min(total);
            state
                .kv_put(&key::chunk_key(ino, i as u64), &data[start..end])
                .await
                .expect("put chunk");
        }
        let mut m = meta::new_file_meta(0o644, 0, 0);
        m.size = total as u64;
        m.inline_data = None;
        meta::put_inode(&mut state, ino, &m)
            .await
            .expect("put_inode");

        // Full multi-chunk read.
        let full = read::read(&mut state, ino, 0, total as u32)
            .await
            .expect("full read");
        assert_eq!(full.len(), total, "full read length");
        assert!(full == data, "full read content mismatch");

        // Whole first chunk (256 KiB → ZC MSG_GET_ZC branch).
        let c0 = read::read(&mut state, ino, 0, chunk as u32)
            .await
            .expect("chunk0 read");
        assert!(c0 == data[..chunk], "chunk0 mismatch");

        // Small sub-range within chunk 0 (4 KiB < 64 KiB → regular MSG_GET branch).
        let sub = read::read(&mut state, ino, 1000, 4096)
            .await
            .expect("sub read");
        assert!(sub == data[1000..1000 + 4096], "sub-range mismatch");

        // Cross-chunk sub-range: offset 200 KiB, len 100 KiB spans chunk 0 → 1.
        let cross_off = 200 * 1024usize;
        let cross_len = 100 * 1024usize;
        let cross = read::read(&mut state, ino, cross_off as i64, cross_len as u32)
            .await
            .expect("cross read");
        assert!(
            cross == data[cross_off..cross_off + cross_len],
            "cross-chunk mismatch"
        );

        // Read past EOF returns just the tail.
        let tail = read::read(&mut state, ino, (total - 10) as i64, 4096)
            .await
            .expect("tail read");
        assert_eq!(tail.len(), 10, "EOF-clamped length");
        assert!(tail == data[total - 10..], "tail mismatch");
    });
}
