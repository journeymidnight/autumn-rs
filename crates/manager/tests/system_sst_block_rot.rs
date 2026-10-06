//! A rotted SST block on one copy must not cost the read.
//!
//! Nothing on the hot path checks a checksum sidecar, so a rotted row-stream
//! copy stays in service until a scrub finds it — up to a week on the policy's
//! schedule. What the partition server has is its own block CRC: a block that
//! fails it is fetched again from every other copy (each replica; for an EC
//! extent, one reconstruction per covering data shard with that shard left
//! out), and the first copy that decodes is served. A replica whose copy
//! failed while another passed is reported, and the manager isolates it.
//!
//! Each case writes small values (inline in the SST), flushes, seals the row
//! extent, rots the first half of one copy's file with nobody reading, and
//! then reads every key; nothing has read the SST since its flush, so the
//! PS's block cache is cold. Blocks are many
//! and the replica a read starts on rotates with the offset, so the rotted
//! copy is read first for some of them.
//!
//! Ablations: serving the first decode failure (`read_block_via` without
//! `reread_block`) fails both read cases; asking the distrusted shard's own
//! node during the reconstruction (`distrust` not honoured there) fails the EC case;
//! a window block's decode failure returned as is fails the compaction case.

mod support;

use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;
use autumn_rpc::version_hello::Role;

use support::*;

const KEYS: u32 = 4000;

fn value(i: u32) -> Vec<u8> {
    format!("value-{i:05}-")
        .into_bytes()
        .into_iter()
        .cycle()
        .take(200)
        .collect()
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

async fn extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode extent_info");
    r.extent.expect("extent present")
}

async fn stream_extents(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
    let resp = mgr
        .call(
            MSG_STREAM_INFO,
            rkyv_encode(&StreamInfoReq {
                stream_ids: vec![stream_id],
            }),
        )
        .await
        .expect("stream_info");
    let r: StreamInfoResp = rkyv_decode(&resp).expect("decode StreamInfoResp");
    r.streams
        .into_iter()
        .find(|(id, _)| *id == stream_id)
        .expect("stream")
        .1
        .extent_ids
}

async fn wait_layout(mgr: &RpcClient, extent_id: u64, what: &str, pred: impl Fn(&MgrExtentInfo) -> bool) -> MgrExtentInfo {
    let start = Instant::now();
    loop {
        let e = extent_info(mgr, extent_id).await;
        if pred(&e) {
            return e;
        }
        assert!(start.elapsed() < Duration::from_secs(60), "{what}");
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn find_file(dir: &Path, name: &str) -> Option<PathBuf> {
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        if p.is_dir() {
            if let Some(f) = find_file(&p, name) {
                return Some(f);
            }
        } else if p.file_name().is_some_and(|n| n == name) {
            return Some(p);
        }
    }
    None
}

/// Flip one byte in every 4 KiB of the first half of `path`: every data block
/// there fails its CRC, and the SST's meta block at the end is left alone.
fn rot_first_half(path: &Path) {
    let mut b = std::fs::read(path).expect("read");
    let half = b.len() / 2;
    for at in (100..half).step_by(4096) {
        b[at] ^= 0x5A;
    }
    std::fs::write(path, &b).expect("rot");
}

struct Cluster {
    mgr_addr: std::net::SocketAddr,
    dirs: Vec<tempfile::TempDir>,
    nodes: Vec<u64>,
}

impl Cluster {
    async fn start(tag: &str) -> (Cluster, Rc<RpcClient>) {
        let mgr_addr = pick_addr();
        start_manager(mgr_addr);
        let mut dirs = Vec::new();
        let mut addrs = Vec::new();
        for i in 0..3 {
            let addr = pick_addr();
            let dir = tempfile::tempdir().unwrap();
            let disk = format_node(mgr_addr, addr, &format!("{tag}-{i}"));
            start_extent_node_with_manager(addr, dir.path().to_path_buf(), disk, mgr_addr);
            dirs.push(dir);
            addrs.push(addr);
        }
        let mgr = RpcClient::connect_as(mgr_addr, Role::Admin, None).await.expect("mgr");
        let mut nodes = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            nodes.push(register_node(&mgr, &addr.to_string(), &format!("{tag}-{i}")).await.node_id);
        }
        (Cluster { mgr_addr, dirs, nodes }, mgr)
    }

    fn dir(&self, node: u64) -> &Path {
        let i = self.nodes.iter().position(|n| *n == node).expect("our node");
        self.dirs[i].path()
    }
}

async fn create_ec_stream(mgr: &RpcClient) -> u64 {
    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates: 2,
                ec_data_shard: 2,
                ec_parity_shard: 1,
            }),
        )
        .await
        .expect("create stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode");
    assert_eq!(created.code, CODE_OK, "create_stream: {}", created.message);
    created.stream.expect("stream").stream_id
}

async fn roll_tail(ps: &RpcClient, part: u64, stream: u64, extent: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: part,
                entries: vec![(stream, extent)],
            }),
        )
        .await
        .expect("roll_tails rpc");
    let r: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, partition_rpc::CODE_OK, "roll_tails: {}", r.message);
    assert_eq!(r.rolled, 1, "roll the row tail");
}

/// Write the keys, flush them into `ssts` SSTs and seal the row extent
/// holding them. Returns the PS client and that extent.
async fn write_and_seal(
    c: &Cluster,
    mgr: &RpcClient,
    part: u64,
    row: u64,
    ps_id: u64,
    ssts: u32,
) -> (Rc<RpcClient>, u64) {
    let log = create_stream(mgr, 2).await;
    let meta = create_stream(mgr, 2).await;
    upsert_partition(mgr, part, log, row, meta, b"a", b"z").await;
    let ps_addr = pick_addr();
    start_partition_server(ps_id, c.mgr_addr, ps_addr);
    let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
    for i in 0..KEYS {
        ps_put(&ps, part, &key(i), &value(i)).await;
        if (i + 1) % (KEYS / ssts) == 0 {
            ps_flush(&ps, part).await;
        }
    }
    let extent = *stream_extents(mgr, row).await.first().expect("row extent");
    roll_tail(&ps, part, row, extent).await;
    wait_layout(mgr, extent, "the row extent never sealed", |e| e.sealed).await;
    (ps, extent)
}

/// Read every key. Nothing has read the SST since it was flushed, so the
/// PS's block cache holds none of its blocks and every read goes to a copy.
async fn read_all(ps: &RpcClient, part: u64, when: &str) {
    for i in 0..KEYS {
        let r = ps_get(ps, part, &key(i)).await;
        assert_eq!(r.code, partition_rpc::CODE_OK, "{when}: get k{i:05}: {}", r.message);
        assert!(r.value == value(i), "{when}: k{i:05} read back wrong");
    }
}

#[test]
fn a_rotted_replica_of_an_sst_block_is_read_around_and_reported() {
    const TAG: &str = "sst-rot-rep";
    const PART: u64 = 971;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (c, mgr) = Cluster::start(TAG).await;
        let row = create_stream(&mgr, 2).await;
        let (ps, extent) = write_and_seal(&c, &mgr, PART, row, 91, 1).await;
        let layout = extent_info(&mgr, extent).await;
        let victim = layout.replicates[0];
        let dat = find_file(c.dir(victim), &format!("extent-{extent}.dat")).expect("victim .dat");
        rot_first_half(&dat);

        read_all(&ps, PART, "replicated, one copy rotted").await;
        let bit = 1u32 << layout.replicates.iter().position(|n| *n == victim).unwrap();
        let after =
            wait_layout(&mgr, extent, "the rotted replica was never isolated", |e| e.avali & bit == 0).await;
        assert_ne!(after.avali, 0, "the healthy replica must stay in service");
    });
}

#[test]
fn a_rotted_ec_shard_under_an_sst_block_is_reconstructed_around() {
    const TAG: &str = "sst-rot-ec";
    const PART: u64 = 973;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (c, mgr) = Cluster::start(TAG).await;
        let row = create_ec_stream(&mgr).await;
        let (ps, extent) = write_and_seal(&c, &mgr, PART, row, 93, 1).await;
        let resp = mgr
            .call(MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id: extent }))
            .await
            .expect("force_ec");
        let f: ForceEcConvertResp = rkyv_decode(&resp).expect("decode force_ec");
        assert_eq!(f.code, CODE_OK, "force_ec_convert: {}", f.message);
        let layout = wait_layout(&mgr, extent, "the row extent never converted", |e| e.ec_converted).await;
        let victim = layout.replicates[0];
        let shard = find_file(c.dir(victim), &format!("extent-{extent}.shard0")).expect("victim shard0");
        rot_first_half(&shard);

        read_all(&ps, PART, "EC, data shard 0 rotted").await;
    });
}

/// Compaction reads a whole window of blocks in one read, from one copy, and
/// bypasses the block cache; a block in it that fails its CRC is fetched again
/// on its own. Without that the major compaction fails on the first rotted
/// window and the rotted extent never leaves the row stream.
#[test]
fn a_compaction_reads_around_a_rotted_replica() {
    const TAG: &str = "sst-rot-compact";
    const PART: u64 = 975;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (c, mgr) = Cluster::start(TAG).await;
        let row = create_stream(&mgr, 2).await;
        let (ps, extent) = write_and_seal(&c, &mgr, PART, row, 95, 2).await;
        let layout = extent_info(&mgr, extent).await;
        let victim = layout.replicates[0];
        let dat = find_file(c.dir(victim), &format!("extent-{extent}.dat")).expect("victim .dat");
        rot_first_half(&dat);

        ps_compact(&ps, PART).await;
        let start = Instant::now();
        while stream_extents(&mgr, row).await.contains(&extent) {
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "the compaction never replaced the rotted extent's SSTs"
            );
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        read_all(&ps, PART, "after compacting around a rotted copy").await;
    });
}
