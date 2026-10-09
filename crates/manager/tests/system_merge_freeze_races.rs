//! Merge freeze races: every write a PS acknowledged while a merge was in
//! flight must be readable after it, whatever the merge's outcome.
//!
//! The freeze lives only in PS memory, so three things can let a source
//! partition accept writes between the merge's `commit_length` capture and its
//! commit: the PS restarts (its reopen starts unfrozen), a stale `freeze=false`
//! arrives (a deposed leader's rollback), or the commit lands after
//! `FREEZE_TTL`. Before the merge took its sources over, each lost every write
//! acknowledged in that window.
//! `MERGE_TEST_PAUSE_MS` stalls the coordinator between capture and commit,
//! which is what a slow etcd or a paused manager looks like there.

mod support;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

const SURVIVOR: u64 = 23001; // [a, m)
const VICTIM: u64 = 23002; // [m, z)
const PS_ID: u64 = 130;

/// `MERGE_TEST_PAUSE_MS` is process-global: one scenario at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn set_pause(ms: u64) {
    autumn_manager::MERGE_TEST_PAUSE_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
}

fn set_takeover_pause(ms: u64) {
    autumn_manager::MERGE_TEST_TAKEOVER_PAUSE_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
}

/// The scenarios below need writes acked inside the window, and a merge that
/// noticed the source was reopened rather than one refused for another reason.
fn assert_window_exercised(acked: &[(u64, String)], resp: &MergePartitionsResp) {
    assert!(!acked.is_empty(), "no write was acked in the window; the scenario did not run");
    if resp.code != CODE_OK {
        assert!(
            resp.message.contains("merge source reopened"),
            "refused for another reason: {}",
            resp.message
        );
    }
}

async fn unfreeze_both(c: &Cluster) {
    for part in [VICTIM, SURVIVOR] {
        let ps = c.router.client_for(part).await;
        let r = ps
            .call(
                partition_rpc::MSG_MERGE_FREEZE,
                partition_rpc::rkyv_encode(&partition_rpc::MergeFreezeReq {
                    part_id: part,
                    freeze: false,
                }),
            )
            .await
            .expect("unfreeze rpc");
        let r: partition_rpc::MergeFreezeResp = partition_rpc::rkyv_decode(&r).unwrap();
        assert_eq!(r.code, partition_rpc::CODE_OK, "unfreeze {part}: {}", r.message);
    }
}

/// One PUT on a fresh connection; true only on a clean `CODE_OK`.
async fn try_put(router: &PsRouter, part_id: u64, key: &[u8], value: &[u8]) -> bool {
    let Ok(c) = router.try_client_for(part_id).await else {
        return false;
    };
    let payload = partition_rpc::rkyv_encode(&partition_rpc::PutReq {
        part_id,
        key: key.to_vec(),
        value: value.to_vec(),
        expires_at: 0,
        region_epoch: 0,
        inode_hint: 0,
        lease_epoch: 0,
    });
    let Ok(Ok(resp)) = compio::time::timeout(
        Duration::from_secs(3),
        c.call(partition_rpc::MSG_PUT, payload),
    )
    .await
    else {
        return false;
    };
    partition_rpc::rkyv_decode::<partition_rpc::PutResp>(&resp)
        .map(|r| r.code == partition_rpc::CODE_OK)
        .unwrap_or(false)
}

struct Cluster {
    mgr_addr: std::net::SocketAddr,
    ps_addr: std::net::SocketAddr,
    mgr: Rc<RpcClient>,
    router: PsRouter,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

/// Manager (etcd-backed when `etcd` names an endpoint) + 2 ENs + nothing on
/// the PS side yet; survivor [a,m) and victim [m,z) created directly (no
/// split, so no CoW overlap).
async fn cluster(node_base: u16, etcd: Option<String>) -> Cluster {
    let mgr_addr = pick_addr();
    match etcd {
        None => start_manager(mgr_addr),
        Some(endpoint) => {
            std::thread::spawn(move || {
                compio::runtime::Runtime::new().unwrap().block_on(async {
                    let m = autumn_manager::AutumnManager::new_with_etcd(
                        vec![endpoint],
                        manager_identity(),
                    )
                    .await
                    .expect("manager with etcd");
                    let _ = m.serve(mgr_addr).await;
                });
            });
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    let n1_dir = tempfile::tempdir().unwrap();
    let n2_dir = tempfile::tempdir().unwrap();
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);
    let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
        .await
        .unwrap();
    register_two_nodes(&mgr, n1_addr, n2_addr, node_base).await;
    // A registered node takes allocations only after its first df.
    let ready = poll_until_async(Duration::from_secs(20), Duration::from_millis(200), || async {
        let req = rkyv_encode(&CreateStreamReq {
            replicates: 2,
            ec_data_shard: 2,
            ec_parity_shard: 0,
        });
        let resp = mgr.call(MSG_CREATE_STREAM, req).await.unwrap();
        rkyv_decode::<CreateStreamResp>(&resp).unwrap().stream.is_some()
    })
    .await;
    assert!(ready, "extent nodes never took an allocation");
    let (s_log, s_row, s_meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, SURVIVOR, s_log, s_row, s_meta, b"a", b"m").await;
    let (v_log, v_row, v_meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, VICTIM, v_log, v_row, v_meta, b"m", b"z").await;
    let ps_addr = pick_addr();
    Cluster {
        mgr_addr,
        ps_addr,
        mgr,
        router: PsRouter::new(mgr_addr, ps_addr),
        _dirs: (n1_dir, n2_dir),
    }
}

async fn wait_both_open(c: &Cluster) {
    let opened = poll_until_async(Duration::from_secs(30), Duration::from_millis(200), || async {
        let r = get_regions(&c.mgr).await;
        r.regions.len() == 2 && r.part_addrs.len() == 2
    })
    .await;
    assert!(opened, "both partitions must open");
}

/// Flushed baseline on both sides, so the post-merge check can tell a
/// reopened survivor from the old one.
async fn baseline(c: &Cluster) {
    for i in 0..5 {
        psr_put(&c.router, SURVIVOR, format!("a/pre-{i:02}").as_bytes(), b"pre").await;
        psr_put(&c.router, VICTIM, format!("m/pre-{i:02}").as_bytes(), b"pre").await;
    }
    psr_flush(&c.router, SURVIVOR).await;
    psr_flush(&c.router, VICTIM).await;
}

type MergeSlot = Rc<RefCell<Option<MergePartitionsResp>>>;

fn spawn_merge(mgr: Rc<RpcClient>) -> MergeSlot {
    let slot: MergeSlot = Rc::new(RefCell::new(None));
    let out = slot.clone();
    compio::runtime::spawn(async move {
        let reply = mgr
            .call(
                MSG_MERGE_PARTITIONS,
                rkyv_encode(&MergePartitionsReq {
                    survivor_part_id: SURVIVOR,
                    victim_part_id: VICTIM,
                    force: false,
                }),
            )
            .await;
        // A refusal can come back as a status error; it is still an answer.
        let resp = match reply {
            Ok(bytes) => rkyv_decode(&bytes).expect("decode merge resp"),
            Err(e) => MergePartitionsResp {
                code: CODE_PRECONDITION,
                message: format!("{e:?}"),
                new_log_tail_extent_id: 0,
            },
        };
        *out.borrow_mut() = Some(resp);
    })
    .detach();
    slot
}

/// Writes alternating sides until every merge slot holds an answer; returns
/// the keys that were acknowledged.
async fn write_until_done(c: &Cluster, slots: &[MergeSlot], tag: &str) -> Vec<(u64, String)> {
    let mut acked = Vec::new();
    let mut i = 0u32;
    while slots.iter().any(|s| s.borrow().is_none()) {
        for (part, prefix) in [(VICTIM, "m"), (SURVIVOR, "a")] {
            let key = format!("{prefix}/{tag}-{i:05}");
            if try_put(&c.router, part, key.as_bytes(), b"post").await {
                acked.push((part, key));
            }
        }
        i += 1;
        compio::time::sleep(Duration::from_millis(50)).await;
    }
    acked
}

/// Every acknowledged key must read back, from wherever its range now lives.
async fn assert_all_readable(c: &Cluster, merged: bool, acked: &[(u64, String)]) {
    let home = |part: u64| if merged { SURVIVOR } else { part };
    if merged {
        let one = poll_until_async(Duration::from_secs(20), Duration::from_millis(250), || async {
            get_regions(&c.mgr).await.regions.len() == 1
        })
        .await;
        assert!(one, "a committed merge must leave one region");
    }
    // The old survivor instance answers [a,m) from its own tables until the
    // reopen; a victim-range read on the survivor proves the reopen happened.
    let reopened = poll_until_async(Duration::from_secs(60), Duration::from_millis(250), || async {
        psr_get(&c.router, home(VICTIM), b"m/pre-00").await.code == partition_rpc::CODE_OK
            && psr_get(&c.router, home(SURVIVOR), b"a/pre-00").await.code
                == partition_rpc::CODE_OK
    })
    .await;
    assert!(reopened, "partitions never served the baseline again");
    let mut lost = Vec::new();
    for (part, key) in acked {
        let r = psr_get(&c.router, home(*part), key.as_bytes()).await;
        if r.code != partition_rpc::CODE_OK || r.value != b"post" {
            lost.push(key.clone());
        }
    }
    eprintln!("acked={} lost={}", acked.len(), lost.len());
    if !merged {
        // An aborted merge leaves fenced sources: they must take writes again.
        for (part, key) in [(SURVIVOR, "a/after"), (VICTIM, "m/after")] {
            let ok = poll_until_async(Duration::from_secs(30), Duration::from_millis(250), || {
                try_put(&c.router, part, key.as_bytes(), b"after")
            })
            .await;
            assert!(ok, "partition {part} never took a write after the aborted merge");
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {} acknowledged writes lost across the merge (merged={merged}), e.g. {:?}",
        lost.len(),
        acked.len(),
        &lost[..lost.len().min(8)]
    );
}

/// The PS restarts after answering the freeze; its reopen starts unfrozen.
#[test]
fn ps_restart_between_freeze_and_commit() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = cluster(130, None).await;
        let mut ps = start_partition_server_killable(PS_ID, c.mgr_addr, c.ps_addr);
        wait_both_open(&c).await;
        baseline(&c).await;

        set_pause(12_000);
        let merge = spawn_merge(c.mgr.clone());
        compio::time::sleep(Duration::from_millis(1500)).await;
        ps.kill();
        let ps2 = start_partition_server_killable(PS_ID, c.mgr_addr, c.ps_addr);
        let acked = write_until_done(&c, &[merge.clone()], "restart").await;
        set_pause(0);
        let resp = merge.borrow_mut().take().unwrap();
        eprintln!("merge: code={} msg={}", resp.code, resp.message);
        assert_window_exercised(&acked, &resp);
        assert_all_readable(&c, resp.code == CODE_OK, &acked).await;
        drop(ps2);
    });
}

/// A `freeze=false` the merge did not send reaches both sources while the
/// merge is between capture and commit — what a deposed leader's rollback
/// does. (Two merges on one leader cannot overlap: the topology hold refuses
/// the second at entry.)
#[test]
fn stale_unfreeze_during_a_merge() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = cluster(132, None).await;
        let _ps = start_partition_server_killable(PS_ID + 1, c.mgr_addr, c.ps_addr);
        wait_both_open(&c).await;
        baseline(&c).await;

        set_pause(8_000);
        let merge = spawn_merge(c.mgr.clone());
        compio::time::sleep(Duration::from_millis(2000)).await;
        unfreeze_both(&c).await;
        let acked = write_until_done(&c, &[merge.clone()], "stale").await;
        set_pause(0);
        let resp = merge.borrow_mut().take().unwrap();
        eprintln!("merge: code={} msg={}", resp.code, resp.message);
        assert_window_exercised(&acked, &resp);
        assert_all_readable(&c, resp.code == CODE_OK, &acked).await;
    });
}

/// The commit lands after the PS's `FREEZE_TTL` (30 s) unfroze it.
#[test]
fn commit_after_freeze_ttl() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = cluster(134, None).await;
        let _ps = start_partition_server_killable(PS_ID + 2, c.mgr_addr, c.ps_addr);
        wait_both_open(&c).await;
        baseline(&c).await;

        set_pause(34_000);
        let merge = spawn_merge(c.mgr.clone());
        let acked = write_until_done(&c, &[merge.clone()], "ttl").await;
        set_pause(0);
        let resp = merge.borrow_mut().take().unwrap();
        eprintln!("merge: code={} msg={}", resp.code, resp.message);
        assert_window_exercised(&acked, &resp);
        assert_all_readable(&c, resp.code == CODE_OK, &acked).await;
    });
}

/// The same stale `freeze=false`, but before the takeover, and the writes
/// stop before it: nothing writes into the fence, so no reopen moves the owner
/// lock, and the capture includes the writes. The merged open replays from the
/// sources' freeze checkpoints, so those writes must not be committed.
#[test]
fn stale_unfreeze_before_the_takeover() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = cluster(138, None).await;
        let _ps = start_partition_server_killable(PS_ID + 4, c.mgr_addr, c.ps_addr);
        wait_both_open(&c).await;
        baseline(&c).await;

        set_takeover_pause(7_000);
        let merge = spawn_merge(c.mgr.clone());
        compio::time::sleep(Duration::from_millis(1500)).await;
        unfreeze_both(&c).await;
        let mut acked = Vec::new();
        for i in 0..40 {
            for (part, prefix) in [(VICTIM, "m"), (SURVIVOR, "a")] {
                let key = format!("{prefix}/early-{i:05}");
                if try_put(&c.router, part, key.as_bytes(), b"post").await {
                    acked.push((part, key));
                }
            }
        }
        while merge.borrow().is_none() {
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        set_takeover_pause(0);
        let resp = merge.borrow_mut().take().unwrap();
        eprintln!("merge: code={} msg={}", resp.code, resp.message);
        assert!(!acked.is_empty(), "no write was acked before the takeover");
        // Any other refusal (e.g. a drain still running at the unfreeze) would
        // leave the check this scenario is about unexercised.
        assert!(
            resp.code != CODE_OK && resp.message.contains("took writes after its freeze drain"),
            "expected the drained-cursor refusal, got code={} {}",
            resp.code,
            resp.message
        );
        assert_all_readable(&c, resp.code == CODE_OK, &acked).await;
    });
}

/// A freeze OK lost on its way back: the merge gives up, and the side it
/// does not know is frozen must take writes again at once, not after
/// `FREEZE_TTL` (30 s). Once for each side.
#[test]
fn a_lost_freeze_reply_does_not_leave_the_side_frozen() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = cluster(140, None).await;
        let _ps = start_partition_server_killable(PS_ID + 5, c.mgr_addr, c.ps_addr);
        wait_both_open(&c).await;
        baseline(&c).await;

        for (side, prefix) in [(VICTIM, "m"), (SURVIVOR, "a")] {
            autumn_manager::MERGE_TEST_DROP_FREEZE_REPLY
                .store(side, std::sync::atomic::Ordering::Relaxed);
            let merge = spawn_merge(c.mgr.clone());
            while merge.borrow().is_none() {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
            autumn_manager::MERGE_TEST_DROP_FREEZE_REPLY
                .store(0, std::sync::atomic::Ordering::Relaxed);
            let resp = merge.borrow_mut().take().unwrap();
            eprintln!("side {side}: merge code={} msg={}", resp.code, resp.message);
            assert!(
                resp.code != CODE_OK && resp.message.contains("reply dropped"),
                "expected the lost-reply refusal, got code={} {}",
                resp.code,
                resp.message
            );
            let key = format!("{prefix}/after-lost-reply");
            let started = std::time::Instant::now();
            let ok = poll_until_async(Duration::from_secs(5), Duration::from_millis(100), || {
                try_put(&c.router, side, key.as_bytes(), b"v")
            })
            .await;
            eprintln!("side {side}: writable={ok} after {:?}", started.elapsed());
            assert!(ok, "partition {side} still refuses writes 5 s after the lost freeze reply");
        }
    });
}

/// The commit's etcd compares (source owner revisions, all six stream
/// baselines) must hold on an undisturbed merge; a wrong key or revision
/// would refuse every merge.
#[test]
fn an_undisturbed_merge_commits_through_etcd() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd, endpoint) = start_etcd().await;
        let c = cluster(136, Some(endpoint)).await;
        // The keys' namespaces; an etcd-backed manager enforces registration.
        for name in ["a", "m"] {
            let req = rkyv_encode(&NamespaceCreateReq {
                name: name.to_string(),
                presplit: vec![],
            });
            let resp = c.mgr.call(MSG_NAMESPACE_CREATE, req).await.unwrap();
            let r: NamespaceCreateResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK, "namespace {name}: {}", r.message);
        }
        let _ps = start_partition_server_killable(PS_ID + 3, c.mgr_addr, c.ps_addr);
        wait_both_open(&c).await;
        baseline(&c).await;
        let merge = spawn_merge(c.mgr.clone());
        let acked = write_until_done(&c, &[merge.clone()], "etcd").await;
        let resp = merge.borrow_mut().take().unwrap();
        assert_eq!(resp.code, CODE_OK, "merge through etcd: {}", resp.message);
        assert_all_readable(&c, true, &acked).await;
    });
}
