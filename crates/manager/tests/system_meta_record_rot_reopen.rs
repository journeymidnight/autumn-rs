//! A meta extent whose last checkpoint record does not decode must not open
//! the partition from an older record, or from none.
//!
//! Recovery takes the last record of each meta extent. An extent holding
//! bytes but no record that decodes was skipped with a warning, so the open
//! carried on with whatever other records there were: with the only
//! checkpoint rotted it opened with no tables, and every flushed key read as
//! absent. The next flush would publish that empty list. Now another copy is
//! read, and with none that decodes the open fails.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 1501;
const PS_ID: u64 = 97;
const KEYS: u32 = 20;

async fn extents(mgr: &RpcClient, stream: u64) -> Vec<u64> {
    use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
    let resp = mgr
        .call(MSG_STREAM_INFO, manager_rpc::rkyv_encode(&StreamInfoReq { stream_ids: vec![stream] }))
        .await
        .expect("stream_info rpc");
    let resp: StreamInfoResp = manager_rpc::rkyv_decode(&resp).expect("decode StreamInfoResp");
    resp.streams.into_iter().find(|(id, _)| *id == stream).expect("stream").1.extent_ids
}

async fn roll(ps: &RpcClient, stream: u64, tail: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq { part_id: PART, entries: vec![(stream, tail)] }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!((resp.code, resp.rolled), (partition_rpc::CODE_OK, 1), "roll_tails: {}", resp.message);
}

async fn force_gc(ps: &RpcClient, extent_ids: Vec<u64>) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id: PART,
                op: partition_rpc::MAINTENANCE_FORCE_GC,
                extent_ids,
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_dead_bytes_high: None,
                gc_empty_only: false,
                gc_policy_is_standing: false,
                op_id: 0,
            }),
        )
        .await
        .expect("forcegc");
    let r: partition_rpc::MaintenanceResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, partition_rpc::CODE_OK, "forcegc: {}", r.message);
}

/// The `.dat` of `extent_id` under the extent node's data directory.
fn dat_file(root: &Path, extent_id: u64) -> PathBuf {
    let name = format!("extent-{extent_id}.dat");
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).expect("read_dir").flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.file_name().is_some_and(|n| n == name.as_str()) {
                return p;
            }
        }
    }
    panic!("{name} not under {root:?}");
}

/// The payloads of the records from byte `from` on, their length prefixes and
/// the records before them intact. Returns the bytes as they were.
fn rot_from(path: &Path, from: usize) -> Vec<u8> {
    let orig = std::fs::read(path).expect("read meta .dat");
    let mut bytes = orig.clone();
    let (mut at, mut rotted) = (0usize, 0);
    while at + 4 <= orig.len() {
        let len = u32::from_le_bytes(orig[at..at + 4].try_into().unwrap()) as usize;
        if at >= from {
            for b in &mut bytes[at + 4..at + 4 + len] {
                *b ^= 0xA5;
            }
            rotted += 1;
        }
        at += 4 + len;
    }
    assert_eq!(at, orig.len(), "whole records only");
    assert!(from > 0 && rotted > 0, "non-vacuity: older records precede the rotted ones ({from}, {rotted})");
    std::fs::write(path, &bytes).expect("rot meta .dat");
    orig
}

/// One extent node per dir, every stream at that many replicas: a first
/// checkpoint without the keys, then `KEYS` keys flushed into a second, and
/// the log they were written to GC'd, so only the second's tables hold them.
/// Returns the meta extent and where the records listing the keys start.
async fn flushed_and_log_gone(mgr_addr: SocketAddr, dirs: &[PathBuf]) -> (u64, usize) {
    let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
        .await
        .expect("mgr");
    let mut en_addrs = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let addr = pick_addr();
        start_extent_node(addr, d.clone(), (i + 1) as u64);
        en_addrs.push(addr);
    }
    for (i, addr) in en_addrs.iter().enumerate() {
        let _ = register_node(&mgr, &addr.to_string(), &format!("uuid-meta-record-rot-{i}")).await;
    }
    let n = dirs.len() as u32;
    let (log, row, meta) = (create_stream(&mgr, n).await, create_stream(&mgr, n).await, create_stream(&mgr, n).await);
    upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;
    let ps_addr = pick_addr();
    start_partition_server(PS_ID, mgr_addr, ps_addr);
    let ps = RpcClient::connect(ps_addr).await.expect("ps");
    ps_put(&ps, PART, b"early", b"e").await;
    ps_flush(&ps, PART).await;
    let early = extents(&mgr, meta).await;
    assert_eq!(early.len(), 1, "one meta extent: {early:?}");
    let keyless = std::fs::metadata(dat_file(&dirs[0], early[0])).expect("meta .dat").len() as usize;
    for i in 0..KEYS {
        ps_put(&ps, PART, format!("k{i:03}").as_bytes(), b"v").await;
    }
    ps_flush(&ps, PART).await;
    let log0 = *extents(&mgr, log).await.last().unwrap();
    roll(&ps, log, log0).await;
    ps_put(&ps, PART, b"pad", b"pad").await;
    ps_flush(&ps, PART).await;
    force_gc(&ps, vec![log0]).await;
    let punched = poll_until_async(Duration::from_secs(20), Duration::from_millis(50), || {
        let mgr = &mgr;
        async move { !extents(mgr, log).await.contains(&log0) }
    })
    .await;
    assert!(punched, "non-vacuity: GC must punch the first log extent");
    let meta_ids = extents(&mgr, meta).await;
    assert_eq!(meta_ids, early, "the records listing the keys are in the same extent");
    (meta_ids[0], keyless)
}

/// Crash: a same-id PS takes the partition over and reopens it.
fn reopen(mgr_addr: SocketAddr) -> SocketAddr {
    let ps2_addr = pick_addr();
    spawn_partition_server(PS_ID, mgr_addr, ps2_addr);
    ps2_addr
}

#[test]
fn the_only_copy_rotted_does_not_open() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (eid, keyless) = flushed_and_log_gone(mgr_addr, &[dir.path().to_path_buf()]).await;
        rot_from(&dat_file(dir.path(), eid), keyless);
        let ps2_addr = reopen(mgr_addr);
        // The takeover's first region sync waits on the open, so a PS whose
        // open fails never listens; one that opened from the older record
        // serves k000 as absent well inside the window.
        let served = poll_until_async(Duration::from_secs(8), Duration::from_millis(200), || async move {
            let Ok(ps2) = RpcClient::connect(ps2_addr).await else {
                return false;
            };
            let Ok(g) = try_ps_get(&ps2, PART, b"k000").await else {
                return false;
            };
            assert!(
                g.code != partition_rpc::CODE_NOT_FOUND,
                "k000 reads absent: the partition opened from the older record"
            );
            g.code == partition_rpc::CODE_OK
        })
        .await;
        assert!(!served, "the partition served with no copy of its checkpoint decoding");
    });
}

#[test]
fn the_copy_read_first_rotted_opens_from_another() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().expect("tempdir")).collect();
    let paths: Vec<PathBuf> = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (eid, keyless) = flushed_and_log_gone(mgr_addr, &paths).await;
        // Rot one copy at a time until a read returns it: that is the copy
        // the reopen reads first. Every copy holds the same records.
        let sc = StreamClient::connect(&mgr_addr.to_string(), "rot-probe".into(), 128 << 20, Rc::new(ConnPool::new()))
            .await
            .expect("stream client");
        let mut first = None;
        for p in &paths {
            let file = dat_file(p, eid);
            let orig = rot_from(&file, keyless);
            let (got, _) = sc.read_bytes_from_extent(eid, 0, 0).await.expect("read meta");
            if got.as_ref() != orig.as_slice() {
                first = Some(file);
                break;
            }
            std::fs::write(&file, &orig).expect("restore");
        }
        assert!(first.is_some(), "non-vacuity: one rotted copy is the one read first");

        let ps2_addr = reopen(mgr_addr);
        let up = poll_until_async(Duration::from_secs(20), Duration::from_millis(200), || async move {
            let Ok(ps2) = RpcClient::connect(ps2_addr).await else {
                return false;
            };
            match try_ps_get(&ps2, PART, b"k000").await {
                Ok(g) => {
                    assert!(g.code != partition_rpc::CODE_NOT_FOUND, "k000 reads absent after the reopen");
                    g.code == partition_rpc::CODE_OK
                }
                Err(_) => false,
            }
        })
        .await;
        assert!(up, "the partition never opened from the good copies");
        let ps2 = RpcClient::connect(ps2_addr).await.expect("ps2");
        for i in 0..KEYS {
            let k = format!("k{i:03}");
            assert_eq!(ps_get(&ps2, PART, k.as_bytes()).await.value, b"v", "{k}");
        }
    });
    drop(dirs);
}
