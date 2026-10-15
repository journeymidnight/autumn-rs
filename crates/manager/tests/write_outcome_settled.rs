//! A write whose log append failed has an unknown outcome, but once anyone has
//! read its key the answer must never change.
//!
//! Ablations: a partition open that does not seal its tails before replay
//! (`commit_length` in place of `seal_tail`) turns the replay test red: the
//! record replayed while a replica was down is truncated off the log by the
//! first append once that replica is back, and the second crash loses it. A
//! partition that answers a failed log append with `Internal` and keeps
//! serving turns the crash test red: k1 reads absent, then present after the
//! crash. Reopening for every failed append (not only one that was sent)
//! turns the serving test red: each later write reopens the partition, and the
//! read right after it is refused.
//!
//! Three extent nodes and streams of three replicas: with one node down every
//! append fails on it and no new tail can be allocated, so the writer gives up
//! with the log tail still open, holding the failed record on the two live
//! replicas only (`docs/seal_length_design.md` §3, the "shorter" copy).

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_client::{AutumnError, ClusterClient};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use support::*;

const PART: u64 = 961;
const PS_ID: u64 = 81;

/// The re-executed child PS (`support::ChildPs`).
#[test]
fn child_ps() {
    child_ps_main();
}

/// `None` = the PS answered OK; `Some(why)` = it did not (or no answer).
async fn try_put(ps_addr: SocketAddr, key: &[u8], value: &[u8]) -> Option<String> {
    let ps = match RpcClient::connect(ps_addr).await {
        Ok(c) => c,
        Err(e) => return Some(format!("connect: {e}")),
    };
    let payload = partition_rpc::rkyv_encode(&partition_rpc::PutReq {
        part_id: PART,
        key: key.to_vec(),
        value: value.to_vec(),
        expires_at: 0,
        region_epoch: 0,
        inode_hint: 0,
        lease_epoch: 0,
    });
    match compio::time::timeout(Duration::from_secs(60), ps.call(partition_rpc::MSG_PUT, payload)).await {
        Err(_) => Some("no answer in 60 s".into()),
        Ok(Err(e)) => Some(format!("{e:?}")),
        Ok(Ok(resp)) => {
            let r: partition_rpc::PutResp = partition_rpc::rkyv_decode(&resp).expect("decode PutResp");
            (r.code != partition_rpc::CODE_OK).then(|| format!("code {}: {}", r.code, r.message))
        }
    }
}

/// The key's value, or `None` when the partition says it is absent. Retries
/// while the partition is not serving.
async fn read(ps_addr: SocketAddr, key: &[u8]) -> Option<Vec<u8>> {
    for _ in 0..300 {
        if let Ok(ps) = RpcClient::connect(ps_addr).await {
            let req = partition_rpc::rkyv_encode(&partition_rpc::GetReq {
                part_id: PART,
                key: key.to_vec(),
                offset: 0,
                length: 0,
                region_epoch: 0,
            });
            if let Ok(got) = ps.call_into_pooled(partition_rpc::MSG_GET_BULK, req).await {
                match got.code {
                    partition_rpc::CODE_OK => return Some(got.buf.filled().to_vec()),
                    partition_rpc::CODE_NOT_FOUND => return None,
                    _ => {}
                }
            }
        }
        compio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("partition {PART} never served a read of {key:?}");
}

/// One read attempt: the value, `None` when absent, or why it was not answered.
async fn get_once(ps_addr: SocketAddr, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let ps = RpcClient::connect(ps_addr).await.map_err(|e| format!("connect: {e}"))?;
    let req = partition_rpc::rkyv_encode(&partition_rpc::GetReq {
        part_id: PART,
        key: key.to_vec(),
        offset: 0,
        length: 0,
        region_epoch: 0,
    });
    let got = ps
        .call_into_pooled(partition_rpc::MSG_GET_BULK, req)
        .await
        .map_err(|e| format!("{e:?}"))?;
    match got.code {
        partition_rpc::CODE_OK => Ok(Some(got.buf.filled().to_vec())),
        partition_rpc::CODE_NOT_FOUND => Ok(None),
        code => Err(format!("code {code}: {}", got.message)),
    }
}

struct Cluster {
    mgr: SocketAddr,
    addrs: Vec<SocketAddr>,
    dirs: Vec<tempfile::TempDir>,
    nodes: Vec<Option<(ShutdownFlag, std::thread::JoinHandle<()>)>>,
}

impl Cluster {
    fn start() -> Self {
        let mgr = pick_addr();
        start_manager(mgr);
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().expect("tmpdir")).collect();
        // Below the ephemeral range: a node restarted on its port must not
        // find it taken by an outbound connection made while it was down.
        let addrs: Vec<SocketAddr> = (0..3)
            .map(|_| SocketAddr::from(([127, 0, 0, 1], pick_stable_port_pair())))
            .collect();
        let nodes = (0..3)
            .map(|i| Some(start_extent_node_stoppable(addrs[i], dirs[i].path().to_path_buf(), 9600 + i as u64)))
            .collect();
        Cluster { mgr, addrs, dirs, nodes }
    }

    async fn create_partition(&self) {
        let mgr = RpcClient::connect(self.mgr).await.expect("connect mgr");
        for (i, addr) in self.addrs.iter().enumerate() {
            register_node(&mgr, &addr.to_string(), &format!("uuid-outcome-{i}")).await;
        }
        let log = create_stream(&mgr, 3).await;
        let row = create_stream(&mgr, 3).await;
        let meta = create_stream(&mgr, 3).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
    }

    fn stop(&mut self, i: usize) {
        let (flag, handle) = self.nodes[i].take().expect("node running");
        flag.shutdown();
        handle.join().expect("join extent node");
    }

    fn restart(&mut self, i: usize) {
        assert!(self.nodes[i].is_none());
        self.nodes[i] = Some(start_extent_node_stoppable(
            self.addrs[i],
            self.dirs[i].path().to_path_buf(),
            9600 + i as u64,
        ));
    }
}

/// The failed write must read the same before and after the partition server
/// dies and the partition reopens, and its writer must be told to retry, not
/// that it failed: the partition reopens on its own and the seal before replay
/// decides whether the write counts.
#[test]
fn a_failed_write_reads_the_same_after_a_crash() {
    let mut c = Cluster::start();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        c.create_partition().await;
        let ps_addr = pick_addr();
        let mut ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        assert_eq!(try_put(ps_addr, b"k0", b"v0").await, None, "k0 with every node up");

        c.stop(2);
        let failed = try_put(ps_addr, b"k1", b"v1").await.expect("k1 acknowledged with a replica down");
        // The reply rides the partition's own connection, which closes when
        // the poisoned partition's thread exits, so it may be lost with it.
        assert!(
            failed == "ConnectionClosed"
                || (failed.contains("OutcomeUnknown") && failed.contains("may or may not have been applied")),
            "k1's writer must be told the outcome is unknown and to retry: {failed}"
        );
        let before = read(ps_addr, b"k1").await;

        ps.kill();
        let ps_addr = pick_addr();
        let _ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        let after = read(ps_addr, b"k1").await;
        assert_eq!(
            before, after,
            "k1 (put answered {failed:?}) read {before:?} before the crash and {after:?} after"
        );
    });
}

/// Once the failed write is settled, the partition must not reopen again for
/// writes that fail before anything is sent: with one of three nodes down no
/// new tail can be allocated, so every later write fails that way, and a
/// reopen per write would take reads down with it.
#[test]
fn writes_failing_before_they_are_sent_leave_the_partition_serving() {
    let mut c = Cluster::start();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        c.create_partition().await;
        let ps_addr = pick_addr();
        let _ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        assert_eq!(try_put(ps_addr, b"k0", b"v0").await, None, "k0 with every node up");

        c.stop(2);
        try_put(ps_addr, b"k1", b"v1").await.expect("k1 acknowledged with a replica down");
        // The partition reopens for k1; wait until it serves again.
        read(ps_addr, b"k0").await;
        for key in [&b"k2"[..], b"k3", b"k4"] {
            let failed = try_put(ps_addr, key, b"v").await.expect("acknowledged with a replica down");
            assert!(
                failed.contains("Unavailable") && failed.contains("was not applied"),
                "{key:?}: must be answered as not applied, by a partition still serving: {failed}"
            );
            assert_eq!(
                get_once(ps_addr, b"k0").await,
                Ok(Some(b"v0".to_vec())),
                "k0 must be served right after {key:?} failed"
            );
        }
    });
}

/// Through the SDK: a put whose first attempt may have been applied ends in
/// `OutcomeUnknown` once its retries run out; a later put that the partition
/// refused before sending anything ends in a plain connection error.
#[test]
fn the_sdk_reports_an_unknown_outcome_only_for_a_write_that_may_have_applied() {
    let mut c = Cluster::start();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        c.create_partition().await;
        let ps_addr = pick_addr();
        let _ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        let sdk = ClusterClient::connect_raw(&c.mgr.to_string()).await.expect("sdk");
        sdk.put(b"k0", b"v0").await.expect("k0 with every node up");

        c.stop(2);
        match sdk.put(b"k1", b"v1").await {
            Err(AutumnError::OutcomeUnknown(_)) => {}
            other => panic!("k1: want OutcomeUnknown, got {other:?}"),
        }
        match sdk.put(b"k2", b"v2").await {
            Err(AutumnError::ConnectionError(m)) if m.contains("not applied") => {}
            other => panic!("k2 (never sent): want a not-applied ConnectionError, got {other:?}"),
        }
        let k1 = sdk.get(b"k1").await.expect("read k1");
        assert_eq!(sdk.get(b"k1").await.expect("read k1 again"), k1, "k1 settled");
    });
}

/// A conditional write is not retried once an attempt may have been applied:
/// a retry could report a conflict caused by its own first attempt.
#[test]
fn the_sdk_stops_a_conditional_write_at_its_first_unknown_outcome() {
    let mut c = Cluster::start();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        c.create_partition().await;
        let ps_addr = pick_addr();
        let _ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        let sdk = ClusterClient::connect_raw(&c.mgr.to_string()).await.expect("sdk");
        sdk.put(b"k0", b"v0").await.expect("k0 with every node up");

        c.stop(2);
        let started = std::time::Instant::now();
        match sdk.compare_put(b"k1", None, b"v1").await {
            Err(AutumnError::OutcomeUnknown(_)) => {}
            other => panic!("k1: want OutcomeUnknown, got {other:?}"),
        }
        // The retry budget is ~9 s of backoff; one attempt takes far less.
        assert!(started.elapsed() < Duration::from_secs(5), "retried: {:?}", started.elapsed());
    });
}

/// A batch whose frame may have been applied reports `OutcomeUnknown` for
/// its ops, even when a later attempt is answered "not applied" per op.
#[test]
fn the_sdk_reports_an_unknown_outcome_for_a_batch_that_may_have_applied() {
    let mut c = Cluster::start();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        c.create_partition().await;
        let ps_addr = pick_addr();
        let _ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        let sdk = ClusterClient::connect_raw(&c.mgr.to_string()).await.expect("sdk");
        sdk.put(b"k0", b"v0").await.expect("k0 with every node up");

        c.stop(2);
        let results = sdk
            .put_many(&[
                (&b"k1"[..], bytes::Bytes::from_static(b"v1"), 0),
                (&b"k2"[..], bytes::Bytes::from_static(b"v2"), 0),
            ])
            .await;
        for (i, r) in results.iter().enumerate() {
            assert!(matches!(r, Err(AutumnError::OutcomeUnknown(_))), "op {i}: {r:?}");
        }
    });
}

/// A reopened partition replays its log up to what the replicas it reaches
/// hold. The answer it then gives for the failed write must survive the
/// missing replica coming back, a new write, and another crash.
#[test]
fn a_replayed_write_survives_the_missing_replica_returning() {
    let mut c = Cluster::start();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        c.create_partition().await;
        let ps_addr = pick_addr();
        let mut ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        assert_eq!(try_put(ps_addr, b"k0", b"v0").await, None, "k0 with every node up");

        c.stop(2);
        let failed = try_put(ps_addr, b"k1", b"v1").await;
        assert!(failed.is_some(), "k1 was acknowledged with a replica down");

        ps.kill();
        let ps_addr = pick_addr();
        let mut ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        let replayed = read(ps_addr, b"k1").await;
        // The two live replicas hold k1, so the open replays it.
        assert_eq!(replayed.as_deref(), Some(&b"v1"[..]), "k1 after the first reopen");

        c.restart(2);
        // Let the manager see the node again.
        compio::time::sleep(Duration::from_secs(6)).await;
        let mut wrote = None;
        for _ in 0..30 {
            wrote = try_put(ps_addr, b"k2", b"v2").await;
            if wrote.is_none() {
                break;
            }
            compio::time::sleep(Duration::from_secs(1)).await;
        }
        assert_eq!(wrote, None, "k2 with every node up again");
        let live = read(ps_addr, b"k1").await;

        ps.kill();
        let ps_addr = pick_addr();
        let _ps = ChildPs::spawn(PS_ID, c.mgr, ps_addr, ChildFailpoints::default());
        let after = read(ps_addr, b"k1").await;
        assert_eq!(
            (&replayed, &live),
            (&after, &after),
            "k1 (put answered {failed:?}) read {replayed:?} after the first reopen, {live:?} \
             once the node was back, {after:?} after the second crash"
        );
        assert_eq!(read(ps_addr, b"k2").await.as_deref(), Some(&b"v2"[..]), "acknowledged k2");
    });
}
