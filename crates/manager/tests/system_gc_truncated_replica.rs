//! GC must not punch a sealed log extent unless it read every byte up to the
//! sealed length. An extent node whose `.dat` was truncated (a lost tail, a
//! wiped file, a torn write) answers a read with code OK and a short or empty
//! payload, and the `.ck` sidecar can vouch only for whole blocks, so the
//! reader's own length check is the last line of defence: a scan that ends
//! early, even exactly on a record boundary, would otherwise relocate nothing
//! and punch an extent still full of live values.
//!
//! Each case seals a log extent E0 holding live big values, overwritten and
//! deleted ones and inline records, then truncates E0's `.dat` on BOTH replicas
//! (so the replica the reader asks first is short whichever it is), with or
//! without its `.ck`, restarts both nodes on those files (a live node's in-memory
//! length still exceeds the file, so its read fails with an internal error; a
//! restarted one loads the shorter length and answers short with code OK), and force-GCs E0. GC must refuse: E0 stays in the log
//! stream. The original bytes are then put back, GC is run again and must
//! relocate and punch E0; every acknowledged value is verified byte for byte,
//! then again after the PS is SIGKILLed and a new one replays the partition.
//!
//! Ablation: letting `run_gc` accept a short chunk as end-of-extent (and
//! dropping `ensure_gc_scan_complete`) punches E0 while its `.dat` is short;
//! every case then fails at "GC punched E0 from a truncated replica".

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    self, rkyv_decode, rkyv_encode, ListNodeStatesReq, ListNodeStatesResp, StreamInfoReq, StreamInfoResp,
    MSG_LIST_NODE_STATES, MSG_STREAM_INFO, NODE_AUTO_STATE_ONLINE,
};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use autumn_rpc::partition_rpc::{self, CODE_OK};

use support::*;

const PS_ID: u64 = 83;
const BIG_LEN: usize = 8 * 1024;
const SMALL_KEYS: u32 = 30;

#[test]
fn child_ps() {
    child_ps_main();
}

#[derive(Clone, Copy, Debug)]
enum Cut {
    /// The file is empty.
    Empty,
    /// Exactly the first half of the records survive, no partial record.
    HalfAtRecordBoundary,
    /// The same, plus the first bytes of the next record.
    HalfPlusPartialRecord,
}

type Expected = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

fn big(tag: u8) -> Vec<u8> {
    vec![tag; BIG_LEN]
}

async fn stream_extents(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
    let resp = mgr
        .call(
            MSG_STREAM_INFO,
            manager_rpc::rkyv_encode(&StreamInfoReq {
                stream_ids: vec![stream_id],
            }),
        )
        .await
        .expect("stream_info rpc");
    let resp: StreamInfoResp = manager_rpc::rkyv_decode(&resp).expect("decode StreamInfoResp");
    assert_eq!(resp.code, manager_rpc::CODE_OK, "stream_info: {}", resp.message);
    resp.streams
        .into_iter()
        .find(|(id, _)| *id == stream_id)
        .expect("stream in response")
        .1
        .extent_ids
}

async fn maintenance(ps: &RpcClient, part: u64, op: u8, extent_ids: Vec<u64>) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id: part,
                op,
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
        .expect("maintenance rpc");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "maintenance {op}: {}", r.message);
}

async fn roll_tail(ps: &RpcClient, part: u64, log: u64, extent: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: part,
                entries: vec![(log, extent)],
            }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!(resp.code, partition_rpc::CODE_OK, "roll_tails: {}", resp.message);
    assert_eq!(resp.rolled, 1, "roll the log tail");
}

async fn put(ps: &RpcClient, part: u64, want: &mut Expected, key: &str, value: Vec<u8>) {
    ps_put(ps, part, key.as_bytes(), &value).await;
    want.insert(key.as_bytes().to_vec(), Some(value));
}

async fn delete(ps: &RpcClient, part: u64, want: &mut Expected, key: &str) {
    let r = ps_delete(ps, part, key.as_bytes()).await;
    assert_eq!(r.code, partition_rpc::CODE_OK, "delete {key}: {}", r.message);
    want.insert(key.as_bytes().to_vec(), None);
}

async fn assert_state(ps: &RpcClient, part: u64, want: &Expected, when: &str) {
    for (key, value) in want {
        let k = String::from_utf8_lossy(key);
        let r = ps_get(ps, part, key).await;
        match value {
            Some(v) => {
                assert_eq!(r.code, partition_rpc::CODE_OK, "{when}: get {k}: {}", r.message);
                assert!(r.value == *v, "{when}: {k} has the wrong value");
            }
            None => assert_eq!(
                r.code,
                partition_rpc::CODE_NOT_FOUND,
                "{when}: deleted {k} came back ({} bytes)",
                r.value.len()
            ),
        }
    }
}

/// `extent-<id>.<suffix>` under an extent node's data dir (hashed subdirs).
fn extent_file(dir: &Path, extent: u64, suffix: &str) -> Option<PathBuf> {
    let name = format!("extent-{extent}.{suffix}");
    for sub in std::fs::read_dir(dir).ok()? {
        let p = sub.ok()?.path().join(&name);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

/// End offsets of the complete WAL records in `dat` (V1: sentinel, u32 length,
/// payload, crc).
fn record_ends(dat: &[u8]) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut at = 0usize;
    while at + 9 <= dat.len() {
        assert_eq!(dat[at], 0xff, "record at {at} is not a V1 record");
        let len = u32::from_le_bytes(dat[at + 1..at + 5].try_into().unwrap()) as usize;
        at += 9 + len;
        assert!(at <= dat.len(), "E0's last record is incomplete");
        ends.push(at);
    }
    ends
}

fn cut_len(dat: &[u8], cut: Cut) -> usize {
    let ends = record_ends(dat);
    let half = ends[ends.len() / 2 - 1];
    match cut {
        Cut::Empty => 0,
        Cut::HalfAtRecordBoundary => half,
        Cut::HalfPlusPartialRecord => half + 11,
    }
}

async fn wait_until_punched(mgr: &RpcClient, log: u64, e0: u64, why: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while stream_extents(mgr, log).await.contains(&e0) {
        assert!(std::time::Instant::now() < deadline, "{why}");
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn reopened(addr: SocketAddr, part: u64) -> std::rc::Rc<RpcClient> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(ps) = RpcClient::connect(addr).await {
            if ps_get(&ps, part, b"x0").await.code == partition_rpc::CODE_OK {
                return ps;
            }
        }
        assert!(std::time::Instant::now() < deadline, "reopened partition never served");
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}


/// One extent node whose process can be stopped and started again over the
/// same data dir and port.
struct En {
    dir: PathBuf,
    disk_id: u64,
    node_uuid: String,
    disk_uuid: String,
    addr: SocketAddr,
    stop: ShutdownFlag,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn spawn_en(dir: PathBuf, disk_id: u64, addr: SocketAddr, mgr_addr: SocketAddr) -> (ShutdownFlag, std::thread::JoinHandle<()>) {
    let flag = ShutdownFlag::new();
    let flag_thread = flag.clone();
    let handle = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let cfg = ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr_addr.to_string());
            let n = ExtentNode::new(cfg).await.expect("extent node");
            compio::runtime::spawn(async move {
                if let Err(e) = n.serve(addr).await {
                    eprintln!("EN serve({addr}) exited: {e}");
                }
            })
            .detach();
            while !flag_thread.is_shutdown() {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    (flag, handle)
}

impl En {
    fn start(dir: PathBuf, disk_id: u64, tag: &str, mgr_addr: SocketAddr) -> En {
        let addr = pick_addr();
        let (stop, thread) = spawn_en(dir.clone(), disk_id, addr, mgr_addr);
        En {
            dir,
            disk_id,
            node_uuid: format!("gc-node-{tag}"),
            disk_uuid: format!("gc-disk-{tag}"),
            addr,
            stop,
            thread: Some(thread),
        }
    }

    async fn register(&self, mgr: &RpcClient) {
        let r = register_node_with_uuid(mgr, &self.addr.to_string(), &self.disk_uuid, &self.node_uuid).await;
        assert_eq!(r.code, CODE_OK, "register {}: {}", self.addr, r.message);
    }

    async fn online(&self, mgr: &RpcClient) {
        let address = self.addr.to_string();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(bytes) = mgr.call(MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {})).await {
                if let Ok(r) = rkyv_decode::<ListNodeStatesResp>(&bytes) {
                    if r.nodes.iter().any(|n| n.address == address && n.auto_state == NODE_AUTO_STATE_ONLINE) {
                        return;
                    }
                }
            }
            assert!(std::time::Instant::now() < deadline, "{address} never came back Online");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    fn stop(&mut self) {
        self.stop.shutdown();
        self.thread.take().expect("running").join().expect("join EN");
    }

    async fn restart(&mut self, mgr: &RpcClient, mgr_addr: SocketAddr) {
        let (stop, thread) = spawn_en(self.dir.clone(), self.disk_id, self.addr, mgr_addr);
        self.stop = stop;
        self.thread = Some(thread);
        self.online(mgr).await;
    }
}

/// Stop every node, let `change` rewrite the files while nothing has them
/// open, and start the nodes again: each loads the files as they now are, so a
/// read past the end of a shortened `.dat` answers a short payload with code OK
/// (a live node refuses it instead, its in-memory length being longer).
async fn bounce(ens: &mut [En], mgr: &RpcClient, mgr_addr: SocketAddr, change: impl FnOnce()) {
    for en in ens.iter_mut() {
        en.stop();
    }
    compio::time::sleep(Duration::from_millis(500)).await;
    change();
    for en in ens.iter_mut() {
        en.restart(mgr, mgr_addr).await;
    }
}

fn run_case(part: u64, cut: Cut, drop_ck: bool) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let mut ens = [
        En::start(n1_dir.path().to_path_buf(), part, "1", mgr_addr),
        En::start(n2_dir.path().to_path_buf(), part + 1, "2", mgr_addr),
    ];

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        for en in &ens {
            en.register(&mgr).await;
        }
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, part, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        let mut child = ChildPs::spawn(PS_ID, mgr_addr, ps1_addr, ChildFailpoints::default());
        let ps = RpcClient::connect(ps1_addr).await.expect("connect ps1");
        let mut want = Expected::new();

        for i in 0..4u8 {
            put(&ps, part, &mut want, &format!("c{i}"), big(0xc0 + i)).await;
            put(&ps, part, &mut want, &format!("d{i}"), big(0xd0 + i)).await;
            put(&ps, part, &mut want, &format!("o{i}"), big(0xa0 + i)).await;
        }
        for i in 0..SMALL_KEYS {
            put(&ps, part, &mut want, &format!("s{i:02}"), format!("small-{i}").into_bytes()).await;
        }
        ps_flush(&ps, part).await;
        for i in 0..4u8 {
            delete(&ps, part, &mut want, &format!("d{i}")).await;
            put(&ps, part, &mut want, &format!("o{i}"), big(0xb0 + i)).await;
        }
        ps_flush(&ps, part).await;

        let e0 = *stream_extents(&mgr, log).await.last().expect("log tail");
        roll_tail(&ps, part, log, e0).await;
        put(&ps, part, &mut want, "x0", b"after-roll".to_vec()).await;
        ps_flush(&ps, part).await;
        assert_state(&ps, part, &want, "before the damage").await;

        // A node describes a sealed extent's content once the manager has told
        // it about the seal.
        for dir in [n1_dir.path(), n2_dir.path()] {
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while extent_file(dir, e0, "ck").is_none() {
                assert!(std::time::Instant::now() < deadline, "no .ck sidecar for the sealed E0");
                compio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        // Damage E0 on both replicas, keeping the originals, and restart the
        // nodes so they serve the damaged files.
        let mut saved: Vec<(PathBuf, Vec<u8>)> = Vec::new();
        bounce(&mut ens, &mgr, mgr_addr, || {
            for dir in [n1_dir.path(), n2_dir.path()] {
                let dat = extent_file(dir, e0, "dat").expect("E0's .dat on every replica");
                let bytes = std::fs::read(&dat).unwrap();
                let keep = cut_len(&bytes, cut);
                let f = std::fs::OpenOptions::new().write(true).open(&dat).unwrap();
                f.set_len(keep as u64).unwrap();
                saved.push((dat, bytes));
                let ck = extent_file(dir, e0, "ck");
                if drop_ck {
                    std::fs::remove_file(ck.expect("a sealed extent has its .ck sidecar")).unwrap();
                } else {
                    assert!(ck.is_some(), "a sealed extent has its .ck sidecar");
                }
            }
        })
        .await;

        maintenance(&ps, part, partition_rpc::MAINTENANCE_FORCE_GC, vec![e0]).await;
        compio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            stream_extents(&mgr, log).await.contains(&e0),
            "GC punched E0 from a truncated replica ({cut:?}, drop_ck={drop_ck})"
        );

        // Put the bytes back and restart the nodes on them.
        bounce(&mut ens, &mgr, mgr_addr, || {
            for (dat, bytes) in &saved {
                std::fs::write(dat, bytes).unwrap();
            }
        })
        .await;
        assert_state(&ps, part, &want, "after the refused GC").await;

        maintenance(&ps, part, partition_rpc::MAINTENANCE_FORCE_GC, vec![e0]).await;
        wait_until_punched(&mgr, log, e0, "GC never punched E0 once its bytes were back").await;
        assert_state(&ps, part, &want, "after the real GC").await;

        drop(ps);
        child.kill();
        let ps2_addr = pick_addr();
        let (stop, join) = start_partition_server_stoppable(PS_ID, mgr_addr, ps2_addr);
        let ps = reopened(ps2_addr, part).await;
        assert_state(&ps, part, &want, "after the PS was killed and reopened").await;
        drop(ps);
        stop.shutdown();
        join.join().expect("join ps2");
        for en in ens.iter_mut() {
            en.stop();
        }
    });
}

#[test]
fn gc_refuses_an_empty_replica_with_its_checksum_sidecar() {
    run_case(961, Cut::Empty, false);
}

#[test]
fn gc_refuses_an_empty_replica_without_its_checksum_sidecar() {
    run_case(962, Cut::Empty, true);
}

#[test]
fn gc_refuses_a_replica_cut_at_a_record_boundary_with_its_sidecar() {
    run_case(963, Cut::HalfAtRecordBoundary, false);
}

#[test]
fn gc_refuses_a_replica_cut_at_a_record_boundary_without_its_sidecar() {
    run_case(964, Cut::HalfAtRecordBoundary, true);
}

#[test]
fn gc_refuses_a_replica_cut_inside_a_record() {
    run_case(965, Cut::HalfPlusPartialRecord, false);
}
