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

/// Boot an N-partition cluster sliced on `[0x03][ino BE][logical_off BE]`
/// boundaries for `ino`: extent at offset `i * 8 MiB` lands on partition `i`,
/// so a multi-extent write to `ino` distributes across N PS cores. Splits are
/// ino-specific — other inos' extent keys all land on a single end-partition;
/// this fixture targets one inode for perf measurement.
async fn boot_multi_partition_cluster_for_ino(
    mgr_addr: std::net::SocketAddr,
    n1_addr: std::net::SocketAddr,
    n2_addr: std::net::SocketAddr,
    base: u16,
    ino: u64,
    n_parts: u64,
) -> ClusterClient {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1_addr, n2_addr, base).await;

    for i in 0..n_parts {
        let (log, row, meta) = create_three_streams(&mgr).await;
        let start_key: Vec<u8> = if i == 0 {
            Vec::new()
        } else {
            key::extent_key(ino, i * MAX_EXTENT as u64)
        };
        let end_key: Vec<u8> = if i == n_parts - 1 {
            vec![0xff, 0xff, 0xff, 0xff]
        } else {
            key::extent_key(ino, (i + 1) * MAX_EXTENT as u64)
        };
        let part_id = (base as u64) * 100 + i + 1;
        upsert_partition(&mgr, part_id, log, row, meta, &start_key, &end_key).await;
    }

    let ps_addr = pick_addr();
    start_partition_server(base as u64, mgr_addr, ps_addr);
    // Multi-partition opens take longer than single — give P-log/P-bulk threads
    // time to spin up + region_sync time to assign all parts.
    compio::time::sleep(Duration::from_millis(3000)).await;
    let _ = RpcClient::connect(ps_addr).await.unwrap();
    let cluster = ClusterClient::connect(&mgr_addr.to_string())
        .await
        .expect("ClusterClient::connect");
    cluster.set_rpc_timeout(Duration::from_secs(120));
    cluster
}

/// Measure `write_into` (parallel, fan_out) vs `write_into_sequential` on
/// multi-extent writes. Single-partition cluster — same-partition extents
/// serialise on the PS's single core, so the speedup here is the client-side
/// in-flight RPC pipelining win. The cross-partition parallel-PS win is
/// covered by `f241a_parallel_write_into_perf_multi_partition`. Numbers
/// logged via println!; run with `--nocapture`.
#[test]
#[ignore]
fn f241a_parallel_write_into_perf() {
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

        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");

        let dclient = ClusterClient::connect(&mgr_addr.to_string())
            .await
            .expect("daemon client");
        dclient.set_rpc_timeout(Duration::from_secs(120));

        async fn make_empty_file(state: &mut FsState, name: &[u8], ino: u64) {
            let m = meta::new_file_meta(0o644, 0, 0);
            meta::put_inode(state, ino, &m).await.expect("put_inode");
            let dk = key::dirent_key(ROOT_INO, name);
            let dv = schema::encode_dirent(&DirentValue {
                child_inode: ino,
                file_type: DT_REG,
            });
            state.kv_put(&dk, &dv).await.expect("put dirent");
        }

        // Sizes covering 1 / 8 / 16 extents per write.
        let cases: &[(&str, usize)] = &[
            ("8 MiB  (1 extent)", MAX_EXTENT),
            ("64 MiB (8 extents)", 8 * MAX_EXTENT),
            ("128 MiB (16 extents)", 16 * MAX_EXTENT),
        ];
        let iters = 3;
        let mut next_ino: u64 = 300;

        println!();
        println!("=== write_into perf (single-partition) ===");
        println!();
        println!(
            "{:24}  {:>10}  {:>12}  {:>12}  {:>10}  {:>6}",
            "size", "iters", "seq ms/op", "par ms/op", "par MB/s", "ratio"
        );

        for (label, size) in cases {
            let data = pattern(*size);

            // Sequential samples (fresh inode per iter — pure appends, no RMW).
            let mut seq_total = std::time::Duration::ZERO;
            for i in 0..iters {
                let ino = next_ino;
                next_ino += 1;
                let name = format!("seq-{label}-{i}-{ino}.bin");
                make_empty_file(&mut state, name.as_bytes(), ino).await;
                let mut opened = fuse_read::open(&dclient, name.as_bytes())
                    .await
                    .expect("open");
                let t0 = std::time::Instant::now();
                let n = fuse_write::write_into_sequential(&dclient, &mut opened, 0, &data)
                    .await
                    .expect("seq write");
                let dt = t0.elapsed();
                assert_eq!(n, *size);
                seq_total += dt;
            }

            // Parallel samples (fresh inode per iter).
            let mut par_total = std::time::Duration::ZERO;
            for i in 0..iters {
                let ino = next_ino;
                next_ino += 1;
                let name = format!("par-{label}-{i}-{ino}.bin");
                make_empty_file(&mut state, name.as_bytes(), ino).await;
                let mut opened = fuse_read::open(&dclient, name.as_bytes())
                    .await
                    .expect("open");
                let t0 = std::time::Instant::now();
                let n = fuse_write::write_into(&dclient, &mut opened, 0, &data)
                    .await
                    .expect("par write");
                let dt = t0.elapsed();
                assert_eq!(n, *size);
                par_total += dt;
            }

            let seq_ms = seq_total.as_secs_f64() * 1000.0 / iters as f64;
            let par_ms = par_total.as_secs_f64() * 1000.0 / iters as f64;
            let par_mbs =
                (*size as f64) / (par_total.as_secs_f64() / iters as f64) / (1024.0 * 1024.0);
            let ratio = if par_ms > 0.0 { seq_ms / par_ms } else { 0.0 };
            println!(
                "{:24}  {:>10}  {:>12.2}  {:>12.2}  {:>10.1}  {:>5.2}x",
                label, iters, seq_ms, par_ms, par_mbs, ratio
            );
        }
        println!();
    });
}

/// Multi-partition counterpart of `f241a_parallel_write_into_perf` — the
/// cluster is sliced into 4 partitions for a fixed `ino`, so the 4-extent
/// (32 MiB) and 8-extent (64 MiB) writes land each extent on a different PS
/// core. This is where parallel `write_into` is supposed to win: cross-
/// partition fan-out, not same-partition serialisation.
#[test]
#[ignore]
fn f241a_parallel_write_into_perf_multi_partition() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // The "fixed inode" the partition layout targets — all extents in the
        // perf test go to this inode so they hit the planned per-extent splits.
        let target_ino: u64 = 600;
        let n_parts: u64 = 4;
        let _admin = boot_multi_partition_cluster_for_ino(
            mgr_addr, n1_addr, n2_addr, 139, target_ino, n_parts,
        )
        .await;

        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");

        let dclient = ClusterClient::connect(&mgr_addr.to_string())
            .await
            .expect("daemon client");
        dclient.set_rpc_timeout(Duration::from_secs(120));

        // At 8 extents we wrap around the 4 partitions twice (2 per partition).
        let cases: &[(&str, usize)] = &[
            ("8 MiB  (1 extent  → 1 part)", MAX_EXTENT),
            ("16 MiB (2 extents → 2 parts)", 2 * MAX_EXTENT),
            ("32 MiB (4 extents → 4 parts)", 4 * MAX_EXTENT),
            ("64 MiB (8 extents → 4 parts ×2)", 8 * MAX_EXTENT),
        ];

        // Reuse target_ino exclusively across cases; non-overlapping offset
        // ranges per write avoid RMW. One iter per (case, mode) — partition
        // splits are at every 8 MiB boundary, so all cases still hit every
        // partition.
        let name = b"perf.bin";
        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, target_ino, &m)
            .await
            .expect("put_inode target");
        let dk = key::dirent_key(ROOT_INO, name);
        let dv = schema::encode_dirent(&DirentValue {
            child_inode: target_ino,
            file_type: DT_REG,
        });
        state.kv_put(&dk, &dv).await.expect("put dirent");

        println!();
        println!("=== write_into perf (multi-partition) ===");
        println!(
            "{} partitions, splits every 8 MiB for ino={}",
            n_parts, target_ino
        );
        println!(
            "{:36}  {:>12}  {:>12}  {:>10}  {:>6}",
            "size", "seq ms/op", "par ms/op", "par MB/s", "ratio"
        );

        let mut cursor: u64 = 0;

        for (label, size) in cases {
            let data = pattern(*size);

            let mut opened = fuse_read::open(&dclient, name).await.expect("open seq");
            let t0 = std::time::Instant::now();
            let n = fuse_write::write_into_sequential(&dclient, &mut opened, cursor, &data)
                .await
                .expect("seq write");
            let seq = t0.elapsed();
            assert_eq!(n, *size);
            cursor += *size as u64;

            let mut opened = fuse_read::open(&dclient, name).await.expect("open par");
            let t0 = std::time::Instant::now();
            let n = fuse_write::write_into(&dclient, &mut opened, cursor, &data)
                .await
                .expect("par write");
            let par = t0.elapsed();
            assert_eq!(n, *size);
            cursor += *size as u64;

            let seq_ms = seq.as_secs_f64() * 1000.0;
            let par_ms = par.as_secs_f64() * 1000.0;
            let par_mbs = (*size as f64) / par.as_secs_f64() / (1024.0 * 1024.0);
            let ratio = if par_ms > 0.0 { seq_ms / par_ms } else { 0.0 };
            println!(
                "{:36}  {:>12.2}  {:>12.2}  {:>10.1}  {:>5.2}x",
                label, seq_ms, par_ms, par_mbs, ratio
            );
        }
        println!();
    });
}
