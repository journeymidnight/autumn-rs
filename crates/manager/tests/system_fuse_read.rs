//! F247 — autumn-fuse variable-length extents (was fixed 256 KiB chunks).
//!
//! Exercises the REAL write + read paths against a live cluster (manager + 2 EN
//! + PS): write a multi-extent file through `write::write` / `flush_inode` (so
//! the extent KV keys are produced by the code under test), then assert:
//!   - the persisted layout is variable-length extents keyed by LOGICAL OFFSET
//!     (`[0x03][ino][off BE]`): a 10 MiB file → extents at off 0 and off 8 MiB
//!     (`MAX_EXTENT` = 8 MiB), NOT 40 × 256 KiB chunks.
//!   - full multi-extent read round-trips byte-exactly
//!   - whole-extent read (8 MiB ≥ 64 KiB → ZC `MSG_GET_ZC` branch)
//!   - small sub-range (< 64 KiB → regular `MSG_GET` branch)
//!   - cross-extent sub-range (spans the 8 MiB boundary)
//!   - EOF-clamped read
//!   - truncate drops the past-EOF extent + shrinks the straddling one
//!   - `delete_all_extents` (unlink path) range-scan-deletes every extent
//!
//! Driven directly through `FsState` (the kernel FUSE mount layer is unchanged).

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fuse::schema::MAX_EXTENT;
use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, extent, key, meta, read, write};

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
    let cluster = ClusterClient::connect_raw(&mgr_addr.to_string())
        .await
        .expect("ClusterClient::connect");
    cluster.set_rpc_timeout(Duration::from_secs(30));
    cluster
}

#[test]
#[ignore]
fn f247_variable_length_extents() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 134, 13401).await;

        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");

        // 10 MiB file → two variable-length extents: [0, 8 MiB) + [8 MiB, 10 MiB).
        let total = 10 * 1024 * 1024usize;
        let data = pattern(total);
        let ino = 100u64;

        // Create the inode (size 0), then write the whole file through the real
        // buffered write path so the extent keys are produced by the code.
        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m)
            .await
            .expect("put_inode");
        let n = write::write(&mut state, ino, 0, &data)
            .await
            .expect("write");
        assert_eq!(n as usize, total, "write returned full length");
        write::flush_inode(&mut state, ino).await.expect("flush");

        // Layout: exactly 2 extents, keyed at logical offsets 0 and 8 MiB.
        let prefix = key::extent_prefix(ino);
        let keys = state
            .kv_range_keys(&prefix, &prefix, 4096)
            .await
            .expect("range");
        let mut offs: Vec<u64> = keys
            .iter()
            .filter_map(|k| key::parse_extent_key(k).map(|(_, o)| o))
            .collect();
        offs.sort_unstable();
        assert_eq!(
            offs,
            vec![0, MAX_EXTENT as u64],
            "10 MiB → extents at off 0 and 8 MiB (variable-length, not 256 KiB chunks)"
        );

        // Full multi-extent read.
        let full = read::read(&mut state, ino, 0, total as u32)
            .await
            .expect("full read");
        assert_eq!(full.len(), total, "full read length");
        assert!(full == data, "full read content mismatch");

        // Whole first extent (8 MiB ≥ 64 KiB → ZC MSG_GET_ZC branch).
        let e0 = read::read(&mut state, ino, 0, MAX_EXTENT as u32)
            .await
            .expect("extent0 read");
        assert!(e0 == data[..MAX_EXTENT], "extent0 mismatch");

        // Small sub-range (4 KiB < 64 KiB → regular MSG_GET branch).
        let sub = read::read(&mut state, ino, 1000, 4096)
            .await
            .expect("sub read");
        assert!(sub == data[1000..1000 + 4096], "sub-range mismatch");

        // Cross-extent sub-range: spans the 8 MiB boundary.
        let cross_off = MAX_EXTENT - 50 * 1024;
        let cross_len = 100 * 1024usize;
        let cross = read::read(&mut state, ino, cross_off as i64, cross_len as u32)
            .await
            .expect("cross read");
        assert!(
            cross == data[cross_off..cross_off + cross_len],
            "cross-extent mismatch"
        );

        // Read past EOF returns just the tail.
        let tail = read::read(&mut state, ino, (total - 10) as i64, 4096)
            .await
            .expect("tail read");
        assert_eq!(tail.len(), 10, "EOF-clamped length");
        assert!(tail == data[total - 10..], "tail mismatch");

        // Truncate to 5 MiB: drops the 8 MiB extent, shrinks extent 0's value.
        let new_size = 5 * 1024 * 1024u64;
        write::truncate(&mut state, ino, new_size)
            .await
            .expect("truncate");
        let after = read::read(&mut state, ino, 0, total as u32)
            .await
            .expect("read after truncate");
        assert_eq!(after.len(), new_size as usize, "truncated read length");
        assert!(after == data[..new_size as usize], "truncated content");
        let keys2 = state
            .kv_range_keys(&prefix, &prefix, 4096)
            .await
            .expect("range2");
        let offs2: Vec<u64> = keys2
            .iter()
            .filter_map(|k| key::parse_extent_key(k).map(|(_, o)| o))
            .collect();
        assert_eq!(offs2, vec![0], "8 MiB extent dropped by truncate");

        // delete_all_extents (the unlink path) removes every extent key.
        extent::delete_all_extents(&mut state, ino)
            .await
            .expect("delete_all_extents");
        let keys3 = state
            .kv_range_keys(&prefix, &prefix, 4096)
            .await
            .expect("range3");
        assert!(keys3.is_empty(), "all extents deleted");
    });
}
