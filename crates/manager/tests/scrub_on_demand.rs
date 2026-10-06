//! `autumn-op scrub`, end to end: the manager names each sealed copy, its node
//! reads and hashes it locally, rot is reported, the slot isolated and rebuilt.
//!
//! Nothing on the hot path computes or reads a checksum, so the only way a
//! copy gets one is a scrub, and the only way rot is found is a later scrub —
//! which is the sequence each case drives: scrub (records), scrub (clean), rot
//! a byte with nobody reading, scrub (rot found), isolation, rebuild on a spare
//! that joins only then (so nothing rebuilds before the isolation is seen),
//! and a scrub of the rebuilt copy, which has no checksums yet and gets them.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::version_hello::Role;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

async fn extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode extent_info");
    r.extent.expect("extent present")
}

/// Submit a scrub op and wait for its terminal record.
async fn scrub(mgr: &RpcClient, extent_ids: Vec<u64>, part_id: u64) -> OpRecord {
    let resp = mgr
        .call(
            MSG_OP_SUBMIT,
            rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_SCRUB,
                part_id,
                secondary_id: extent_ids.first().copied().unwrap_or(0),
                extent_ids,
                requested_by: "test".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("submit scrub");
    let r: OpSubmitResp = rkyv_decode(&resp).expect("decode submit");
    assert_eq!(r.code, CODE_OK, "scrub refused: {}", r.message);
    let start = Instant::now();
    loop {
        let resp = mgr
            .call(
                MSG_OP_QUERY,
                rkyv_encode(&OpQueryReq {
                    op_id: r.op_id,
                    ..Default::default()
                }),
            )
            .await
            .expect("query op");
        let q: OpQueryResp = rkyv_decode(&resp).expect("decode query");
        let op = q.ops.into_iter().next().expect("the op");
        if op.state != OP_STATE_PENDING && op.state != OP_STATE_RUNNING {
            return op;
        }
        assert!(start.elapsed() < Duration::from_secs(60), "scrub op never finished");
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

fn flip_byte(path: &Path, at: usize) {
    let mut b = std::fs::read(path).expect("read");
    b[at] ^= 0x5A;
    std::fs::write(path, &b).expect("rot");
}

fn slot_bit(ex: &MgrExtentInfo, node: u64) -> u32 {
    1u32 << ex
        .replicates
        .iter()
        .chain(ex.parity.iter())
        .position(|n| *n == node)
        .expect("node holds a slot")
}

struct Cluster {
    mgr_addr: SocketAddr,
    addrs: Vec<SocketAddr>,
    dirs: Vec<tempfile::TempDir>,
    nodes: Vec<(u64, usize)>,
}

impl Cluster {
    /// `live` nodes running now; the rest join later via `start_spare`.
    fn new(total: usize, live: usize, tag: &str) -> Self {
        let mgr_addr = pick_addr();
        start_manager(mgr_addr);
        let addrs: Vec<SocketAddr> = (0..total).map(|_| pick_addr()).collect();
        let dirs: Vec<tempfile::TempDir> = (0..total).map(|_| tempfile::tempdir().unwrap()).collect();
        for i in 0..live {
            let disk = format_node(mgr_addr, addrs[i], &format!("{tag}-{i}"));
            start_extent_node_with_manager(addrs[i], dirs[i].path().to_path_buf(), disk, mgr_addr);
        }
        Cluster {
            mgr_addr,
            addrs,
            dirs,
            nodes: Vec::new(),
        }
    }

    async fn register(&mut self, mgr: &RpcClient, i: usize, tag: &str) -> u64 {
        let r = register_node(mgr, &self.addrs[i].to_string(), &format!("{tag}-{i}")).await;
        self.nodes.push((r.node_id, i));
        r.node_id
    }

    async fn start_spare(&mut self, mgr: &RpcClient, i: usize, tag: &str) -> u64 {
        let uuid = format!("{tag}-{i}");
        let r = register_node(mgr, &self.addrs[i].to_string(), &uuid).await;
        let disk = r
            .disk_uuids
            .iter()
            .find(|(u, _)| *u == uuid)
            .map(|(_, d)| *d)
            .expect("spare disk id");
        start_extent_node_with_manager(self.addrs[i], self.dirs[i].path().to_path_buf(), disk, self.mgr_addr);
        self.nodes.push((r.node_id, i));
        r.node_id
    }

    fn dir(&self, node: u64) -> &Path {
        let i = self.nodes.iter().find(|(n, _)| *n == node).expect("our node").1;
        self.dirs[i].path()
    }
}

async fn seal(mgr: &RpcClient, sc: &StreamClient, stream_id: u64, commit: u64) {
    let resp = mgr
        .call(
            MSG_STREAM_ALLOC_EXTENT,
            rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key: sc.owner_key().to_string(),
                owner_epoch: sc.owner_epoch(),
                seal_commit: Some(commit),
                exclude_node_ids: vec![],
                seal_extent_id: 0,
            }),
        )
        .await
        .expect("seal");
    let r: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
    assert_eq!(r.code, CODE_OK, "seal: {}", r.message);
}

async fn force_ec(mgr: &RpcClient, extent_id: u64) -> ForceEcConvertResp {
    let resp = mgr
        .call(MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id }))
        .await
        .expect("force_ec");
    rkyv_decode(&resp).expect("decode force_ec")
}

/// Wait until `pred` holds for the extent's layout.
async fn wait_layout(mgr: &RpcClient, extent_id: u64, what: &str, pred: impl Fn(&MgrExtentInfo) -> bool) -> MgrExtentInfo {
    for _ in 0..120 {
        let e = extent_info(mgr, extent_id).await;
        if pred(&e) {
            return e;
        }
        compio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("{what}");
}

#[test]
fn a_replica_is_recorded_then_checked_and_a_rotted_one_rebuilt() {
    const TAG: &str = "scrub-rep";
    let mut c = Cluster::new(4, 3, TAG);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(c.mgr_addr, Role::Admin, None).await.expect("mgr");
        for i in 0..3 {
            c.register(&mgr, i, TAG).await;
        }
        let stream_id = create_stream(&mgr, 3).await;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&c.mgr_addr.to_string(), "owner/scrub-rep/0".into(), 256 << 20, pool)
            .await
            .expect("stream client");
        const N: usize = 2 * 1024 * 1024 + 500;
        let payload: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal(&mgr, &sc, stream_id, r.end).await;
        let layout = extent_info(&mgr, extent_id).await;
        assert_eq!(layout.replicates.len(), 3);
        let ck_name = format!("extent-{extent_id}.ck");
        for n in &layout.replicates {
            assert!(
                find_file(c.dir(*n), &ck_name).is_none(),
                "nothing but a scrub writes checksums — not the seal"
            );
        }

        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert_eq!(op.state, OP_STATE_SUCCEEDED, "{} {}", op.message, op.error);
        assert!(op.message.contains("3 recorded for the first time"), "{}", op.message);
        for n in &layout.replicates {
            assert!(find_file(c.dir(*n), &ck_name).is_some(), "node {n} has no checksums");
        }
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("3 clean"), "{}", op.message);

        // Rot one replica with nobody reading; the next scrub finds it.
        let victim = layout.replicates[0];
        let bit = slot_bit(&layout, victim);
        let dat = find_file(c.dir(victim), &format!("extent-{extent_id}.dat")).expect("victim .dat");
        flip_byte(&dat, 1024 * 1024 + 512 * 1024);
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert_eq!(op.state, OP_STATE_SUCCEEDED, "finding rot is a result, not a failure");
        assert!(op.message.contains("1 rotted"), "{}", op.message);
        wait_layout(&mgr, extent_id, "the rotted replica was never isolated", |e| {
            e.avali & bit == 0 || !e.replicates.contains(&victim)
        })
        .await;

        // A spare joins; recovery rebuilds the isolated copy on it.
        let spare = c.start_spare(&mgr, 3, TAG).await;
        let rebuilt = wait_layout(&mgr, extent_id, "the isolated replica was never rebuilt", |e| {
            !e.replicates.contains(&victim) && e.avali.count_ones() == 3
        })
        .await;
        assert!(rebuilt.replicates.contains(&spare));
        let copy = find_file(c.dir(spare), &format!("extent-{extent_id}.dat")).expect("rebuilt .dat");
        assert!(std::fs::read(&copy).unwrap()[..N] == payload[..], "the rebuild is not the original");

        // The rebuilt copy has no checksums yet; the next scrub records them,
        // and the whole-cluster scope covers it too.
        let op = scrub(&mgr, vec![], 0).await;
        assert_eq!(op.state, OP_STATE_SUCCEEDED, "{} {}", op.message, op.error);
        assert!(
            op.message.contains("1 recorded for the first time") && op.message.contains("2 clean"),
            "{}",
            op.message
        );

        sc.invalidate_extent_cache(extent_id);
        let (got, _) = sc.read_bytes_from_extent(extent_id, 0, N as u64).await.expect("read");
        assert!(got == payload, "the extent does not read back clean");
    });
}

#[test]
fn an_ec_shard_is_recorded_then_checked_and_a_rotted_one_rebuilt() {
    const TAG: &str = "scrub-ec";
    let mut c = Cluster::new(4, 3, TAG);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(c.mgr_addr, Role::Admin, None).await.expect("mgr");
        for i in 0..3 {
            c.register(&mgr, i, TAG).await;
        }
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
            .unwrap();
        let created: CreateStreamResp = rkyv_decode(&resp).unwrap();
        assert_eq!(created.code, CODE_OK, "create_stream: {}", created.message);
        let stream_id = created.stream.expect("stream").stream_id;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&c.mgr_addr.to_string(), "owner/scrub-ec/0".into(), 256 << 20, pool)
            .await
            .expect("stream client");
        // Odd, so a shard is ceil(N / 2): a shard length computed any other
        // way names a file length no node holds.
        const N: usize = 4 * 1024 * 1024 + 1001;
        let payload: Vec<u8> = (0..N).map(|i| ((i * 13) % 251) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal(&mgr, &sc, stream_id, r.end).await;
        let resp = mgr
            .call(MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id }))
            .await
            .expect("force_ec");
        let f: ForceEcConvertResp = rkyv_decode(&resp).expect("decode force_ec");
        assert_eq!(f.code, CODE_OK, "force_ec_convert: {}", f.message);
        let layout = wait_layout(&mgr, extent_id, "the extent never converted", |e| e.ec_converted).await;
        let victim = layout.replicates[0];
        let shard_name = format!("extent-{extent_id}.shard0");
        let ck_name = format!("{shard_name}.ck");
        assert!(
            find_file(c.dir(victim), &ck_name).is_none(),
            "nothing but a scrub writes checksums — not the conversion"
        );

        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("3 recorded for the first time"), "{}", op.message);
        let shard_path = find_file(c.dir(victim), &shard_name).expect("victim's shard");
        assert!(find_file(c.dir(victim), &ck_name).is_some());
        let clean_shard = std::fs::read(&shard_path).unwrap();
        assert_eq!(&clean_shard[..64], &payload[..64]);

        flip_byte(&shard_path, 1024 * 1024 + 512 * 1024);
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("1 rotted"), "{}", op.message);
        let bit = slot_bit(&layout, victim);
        wait_layout(&mgr, extent_id, "the rotted shard was never isolated", |e| {
            e.avali & bit == 0 || e.replicates[0] != victim
        })
        .await;

        let spare = c.start_spare(&mgr, 3, TAG).await;
        let rebuilt = wait_layout(&mgr, extent_id, "the isolated shard was never rebuilt", |e| {
            e.replicates[0] != victim && e.avali == 0b111
        })
        .await;
        assert_eq!(rebuilt.replicates[0], spare);
        let new_shard = find_file(c.dir(spare), &shard_name).expect("rebuilt shard");
        assert!(std::fs::read(&new_shard).unwrap() == clean_shard, "the rebuilt shard is not the original");

        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(
            op.message.contains("1 recorded for the first time") && op.message.contains("2 clean"),
            "{}",
            op.message
        );
        sc.invalidate_extent_cache(extent_id);
        let (got, _) = sc.read_bytes_from_extent(extent_id, 0, N as u64).await.expect("read");
        assert!(got == payload, "the extent does not read back clean");
    });
}

/// Two rotted copies of one extent, found by ONE scrub, are both isolated.
///
/// The first isolation moves the extent's eversion, and the manager does not
/// judge a finding whose eversion moved — so the op must hold the second
/// finding and have that copy scrubbed again under the current eversion, or
/// the second copy stays in service, unmarked, until the next scrub.
#[test]
fn two_rotted_replicas_found_by_one_scrub_are_both_isolated() {
    const TAG: &str = "scrub-two";
    let mut c = Cluster::new(3, 3, TAG);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(c.mgr_addr, Role::Admin, None).await.expect("mgr");
        for i in 0..3 {
            c.register(&mgr, i, TAG).await;
        }
        let stream_id = create_stream(&mgr, 3).await;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&c.mgr_addr.to_string(), "owner/scrub-two/0".into(), 256 << 20, pool)
            .await
            .expect("stream client");
        let payload: Vec<u8> = (0..(2 * 1024 * 1024 + 7)).map(|i| (i % 249) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal(&mgr, &sc, stream_id, r.end).await;
        let layout = extent_info(&mgr, extent_id).await;
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("3 recorded for the first time"), "{}", op.message);

        let victims = [layout.replicates[0], layout.replicates[1]];
        let mut bits = 0u32;
        for v in victims {
            bits |= slot_bit(&layout, v);
            let dat = find_file(c.dir(v), &format!("extent-{extent_id}.dat")).expect("victim .dat");
            flip_byte(&dat, 700_000);
        }
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("2 rotted"), "{}", op.message);
        wait_layout(&mgr, extent_id, "both rotted replicas must be isolated", |e| e.avali & bits == 0).await;
    });
}

/// A copy a scrub found rotted is never the source of an EC conversion.
///
/// The coordinator encodes from its own full-length copy, and a rotted copy is
/// full length; once the conversion's cleanup drops the replicas, the damage
/// would be the only version, with parity agreeing. Three nodes for RF 3, so
/// the isolated slot has no rebuild target and stays marked.
#[test]
fn an_extent_with_a_rotted_copy_is_not_ec_converted() {
    const TAG: &str = "scrub-noec";
    let mut c = Cluster::new(3, 3, TAG);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(c.mgr_addr, Role::Admin, None).await.expect("mgr");
        for i in 0..3 {
            c.register(&mgr, i, TAG).await;
        }
        let resp = mgr
            .call(
                MSG_CREATE_STREAM,
                rkyv_encode(&CreateStreamReq {
                    replicates: 3,
                    ec_data_shard: 2,
                    ec_parity_shard: 1,
                }),
            )
            .await
            .unwrap();
        let created: CreateStreamResp = rkyv_decode(&resp).unwrap();
        assert_eq!(created.code, CODE_OK, "create_stream: {}", created.message);
        let stream_id = created.stream.expect("stream").stream_id;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&c.mgr_addr.to_string(), "owner/scrub-noec/0".into(), 256 << 20, pool)
            .await
            .expect("stream client");
        let payload: Vec<u8> = (0..(2 * 1024 * 1024 + 3)).map(|i| (i % 241) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal(&mgr, &sc, stream_id, r.end).await;
        let layout = extent_info(&mgr, extent_id).await;
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("3 recorded for the first time"), "{}", op.message);

        // The coordinator's own copy: slot 0.
        let victim = layout.replicates[0];
        let bit = slot_bit(&layout, victim);
        let dat = find_file(c.dir(victim), &format!("extent-{extent_id}.dat")).expect("victim .dat");
        flip_byte(&dat, 10);
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("1 rotted"), "{}", op.message);
        wait_layout(&mgr, extent_id, "the rotted replica was never isolated", |e| e.avali & bit == 0).await;

        let resp = mgr
            .call(MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id }))
            .await
            .expect("force_ec");
        let f: ForceEcConvertResp = rkyv_decode(&resp).expect("decode force_ec");
        assert_eq!(f.code, CODE_PRECONDITION, "converted with a rotted copy: {}", f.message);
        assert!(f.message.contains("corrupt-marked"), "{}", f.message);
        assert!(!extent_info(&mgr, extent_id).await.ec_converted);
    });
}

/// Rot nobody has reported yet is caught by the conversion itself: the
/// coordinator checks the `.dat` it encodes against what a scrub recorded,
/// refuses, and the manager isolates that copy. Once it is rebuilt and
/// recorded again, the conversion goes through, checked clean.
#[test]
fn an_ec_conversion_refuses_a_coordinator_copy_that_fails_its_checksums() {
    const TAG: &str = "scrub-ecsrc";
    let mut c = Cluster::new(3, 3, TAG);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(c.mgr_addr, Role::Admin, None).await.expect("mgr");
        for i in 0..3 {
            c.register(&mgr, i, TAG).await;
        }
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
            .unwrap();
        let created: CreateStreamResp = rkyv_decode(&resp).unwrap();
        assert_eq!(created.code, CODE_OK, "create_stream: {}", created.message);
        let stream_id = created.stream.expect("stream").stream_id;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&c.mgr_addr.to_string(), "owner/scrub-ecsrc/0".into(), 256 << 20, pool)
            .await
            .expect("stream client");
        // Over one 1 MiB checksum block per shard, and odd, so a block
        // straddles the boundary between the two data shards.
        const N: usize = 3 * 1024 * 1024 + 777;
        let payload: Vec<u8> = (0..N).map(|i| ((i * 31) % 251) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal(&mgr, &sc, stream_id, r.end).await;
        let layout = extent_info(&mgr, extent_id).await;
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("2 recorded for the first time"), "{}", op.message);

        // Rot the coordinator's copy (slot 0) and tell nobody.
        let coord = layout.replicates[0];
        let bit = slot_bit(&layout, coord);
        let dat = find_file(c.dir(coord), &format!("extent-{extent_id}.dat")).expect("coordinator .dat");
        flip_byte(&dat, 1024 * 1024 + 5);
        let f = force_ec(&mgr, extent_id).await;
        assert_eq!(f.code, CODE_OK, "force_ec_convert: {}", f.message);
        let isolated = wait_layout(&mgr, extent_id, "the rotted coordinator copy was never isolated", |e| {
            e.avali & bit == 0
        })
        .await;
        assert!(!isolated.ec_converted, "converted from a copy that fails its checksums");
        let f = force_ec(&mgr, extent_id).await;
        assert_eq!(f.code, CODE_PRECONDITION, "the marked copy must be rebuilt first: {}", f.message);

        // Recovery rebuilds the copy on the third node from the clean replica,
        // a scrub records the rebuilt copy, and the conversion now goes through.
        wait_layout(&mgr, extent_id, "the isolated copy was never rebuilt", |e| {
            !e.replicates.contains(&coord) && e.avali.count_ones() as usize == e.replicates.len()
        })
        .await;
        let op = scrub(&mgr, vec![extent_id], 0).await;
        assert!(op.message.contains("1 recorded for the first time"), "{}", op.message);
        let f = force_ec(&mgr, extent_id).await;
        assert_eq!(f.code, CODE_OK, "force_ec_convert after rebuild: {}", f.message);
        let converted = wait_layout(&mgr, extent_id, "the extent never converted", |e| e.ec_converted).await;
        let shard0 = find_file(c.dir(converted.replicates[0]), &format!("extent-{extent_id}.shard0"))
            .expect("shard 0");
        assert_eq!(&std::fs::read(&shard0).unwrap()[..4096], &payload[..4096]);
        sc.invalidate_extent_cache(extent_id);
        let (got, _) = sc.read_bytes_from_extent(extent_id, 0, N as u64).await.expect("read");
        assert!(got == payload, "the converted extent does not read back clean");
    });
}
