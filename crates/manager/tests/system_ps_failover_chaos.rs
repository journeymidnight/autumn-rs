//! PS-failover chaos: 2 partition servers, kill one, the manager must
//! evict it (PS_DEAD_TIMEOUT = 10 s) and migrate its partitions to the
//! survivor; reads AND writes must recover with zero data loss.
//!
//! Coverage gap this closes: `system_chaos.rs` kills ENs (subprocesses)
//! but runs a SINGLE in-process PS that is never killed — the
//! PS-eviction → rebalance_regions → survivor open_partition (full
//! recovery from the three streams) path had no chaos coverage.
//!
//! Scenarios:
//! 1. `ps_kill_migrates_partitions`: kill -9 the PS owning partition A;
//!    every pre-kill key must stay readable and new writes must land
//!    within a bounded window (eviction 10 s + region_sync ~2 s + the
//!    survivor's open_partition/recovery). Then restart the killed PS
//!    (same psid) and verify the cluster still serves everything.
//! 2. `ps_kill_during_write_storm`: continuous writes while the kill
//!    happens; ACKed writes must survive (uncertain-timeout writes may
//!    or may not land — chaos rule: a timed-out PUT is dropped from the
//!    expectation set, never mis-recorded).
//!
//! PSes run as real `autumn-ps` subprocesses (kill -9-able), the same
//! pattern as `bug_lease_2_phase2_persistence.rs`.

mod support;

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use support::*;

fn ps_binary() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let target = match std::env::var("CARGO_TARGET_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => workspace.join("target"),
    };
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for profile in ["debug", "release"] {
        let p = target.join(profile).join("autumn-ps");
        if let Ok(meta) = std::fs::metadata(&p) {
            let mt = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            if best.as_ref().is_none_or(|(bt, _)| mt > *bt) {
                best = Some((mt, p));
            }
        }
    }
    best.map(|(_, p)| p)
        .unwrap_or_else(|| panic!("autumn-ps binary not found under {}", target.display()))
}

struct PsProc {
    psid: u64,
    addr: std::net::SocketAddr,
    child: Option<Child>,
}

impl PsProc {
    fn spawn(psid: u64, mgr: std::net::SocketAddr) -> Self {
        let addr = pick_addr();
        let child = Command::new(ps_binary())
            .arg("--psid")
            .arg(psid.to_string())
            .arg("--port")
            .arg(addr.port().to_string())
            .arg("--manager")
            .arg(mgr.to_string())
            .arg("--listen")
            .arg("127.0.0.1")
            .arg("--advertise")
            .arg(addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn autumn-ps");
        PsProc {
            psid,
            addr,
            child: Some(child),
        }
    }

    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn respawn(&mut self, mgr: std::net::SocketAddr) {
        assert!(self.child.is_none(), "respawn over a live child");
        let child = Command::new(ps_binary())
            .arg("--psid")
            .arg(self.psid.to_string())
            .arg("--port")
            .arg(self.addr.port().to_string())
            .arg("--manager")
            .arg(mgr.to_string())
            .arg("--listen")
            .arg("127.0.0.1")
            .arg("--advertise")
            .arg(self.addr.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("respawn autumn-ps");
        self.child = Some(child);
    }
}

impl Drop for PsProc {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Map part_id → ps_id from the manager's region table.
async fn assignment(mgr: &RpcClient) -> HashMap<u64, u64> {
    get_regions(mgr)
        .await
        .regions
        .iter()
        .map(|(_, r)| (r.part_id, r.ps_id))
        .collect()
}

/// Wait until every partition is assigned to SOME ps in `want` and (if
/// `exclude` is Some) none is assigned to the excluded ps.
async fn await_assignment(
    mgr: &RpcClient,
    part_ids: &[u64],
    exclude: Option<u64>,
    deadline: Duration,
) -> HashMap<u64, u64> {
    let t0 = Instant::now();
    loop {
        let asg = assignment(mgr).await;
        let ok = part_ids.iter().all(|p| match asg.get(p) {
            Some(ps) => exclude != Some(*ps),
            None => false,
        });
        if ok {
            return asg;
        }
        assert!(
            t0.elapsed() < deadline,
            "partitions not (re)assigned within {deadline:?}: {asg:?} exclude={exclude:?}"
        );
        compio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Boot manager + 3 EN (in-process) + 2 subprocess PSes + 2 partitions
/// splitting the keyspace at 0x80. Returns (mgr client, ps procs,
/// part_ids).
async fn boot(
    mgr_addr: std::net::SocketAddr,
    base: u16,
) -> (std::rc::Rc<RpcClient>, Vec<PsProc>, Vec<u64>) {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();

    let n1 = pick_addr();
    let n2 = pick_addr();
    let d1 = tempfile::tempdir().expect("d1");
    let d2 = tempfile::tempdir().expect("d2");
    // tempdirs must outlive the test process: leak them deliberately.
    start_extent_node(n1, d1.keep(), 1);
    start_extent_node(n2, d2.keep(), 2);
    register_two_nodes(&mgr, n1, n2, base).await;

    let (log_a, row_a, meta_a) = create_three_streams(&mgr).await;
    let (log_b, row_b, meta_b) = create_three_streams(&mgr).await;
    let part_a = (base as u64) * 100 + 1;
    let part_b = (base as u64) * 100 + 2;
    upsert_partition(&mgr, part_a, log_a, row_a, meta_a, b"", b"\x80").await;
    upsert_partition(&mgr, part_b, log_b, row_b, meta_b, b"\x80", b"\xff\xff\xff\xff").await;

    let ps1 = PsProc::spawn(base as u64 + 1, mgr_addr);
    let ps2 = PsProc::spawn(base as u64 + 2, mgr_addr);
    // Wait for both partitions to be assigned + opened.
    await_assignment(&mgr, &[part_a, part_b], None, Duration::from_secs(30)).await;
    compio::time::sleep(Duration::from_millis(2000)).await;
    (mgr, vec![ps1, ps2], vec![part_a, part_b])
}

/// Keys that land left / right of the 0x80 partition split.
fn key_for(part_idx: usize, i: u32) -> Vec<u8> {
    let prefix: u8 = if part_idx == 0 { b'a' } else { 0xA0 };
    let mut k = vec![prefix];
    k.extend_from_slice(format!("k{i:05}").as_bytes());
    k
}

#[test]
#[ignore] // requires built binaries; boots a full cluster
fn ps_kill_migrates_partitions() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, mut pses, parts) = boot(mgr_addr, 210).await;

        let cluster = ClusterClient::connect(&mgr_addr.to_string())
            .await
            .expect("ClusterClient::connect");
        cluster.set_rpc_timeout(Duration::from_secs(10));

        // Seed both partitions: small + a >4 KiB (VP) value per slot.
        let mut expected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for part_idx in 0..2 {
            for i in 0..50u32 {
                let k = key_for(part_idx, i);
                let v = if i % 10 == 0 {
                    vec![(i % 251) as u8; 8192] // VP-sized
                } else {
                    format!("v{part_idx}-{i}").into_bytes()
                };
                cluster.put(&k, &v).await.expect("seed put");
                expected.push((k, v));
            }
        }

        // Find which PS owns partition A and kill it.
        let asg = assignment(&mgr).await;
        let victim_ps = *asg.get(&parts[0]).expect("part A assigned");
        let victim_idx = pses
            .iter()
            .position(|p| p.psid == victim_ps)
            .expect("victim in our ps set");
        eprintln!("killing ps {victim_ps} (owns part {})", parts[0]);
        pses[victim_idx].kill();

        // The manager must evict (10 s) + reassign everything to the
        // survivor; allow recovery time on top.
        let post = await_assignment(&mgr, &parts, Some(victim_ps), Duration::from_secs(45)).await;
        eprintln!("post-kill assignment: {post:?}");

        // Every pre-kill key must read back exactly; first reads may need
        // the SDK's refresh+retry while the survivor opens the partition.
        let t0 = Instant::now();
        for (k, v) in &expected {
            let got = loop {
                match cluster.get(k).await {
                    Ok(Some(g)) => break g,
                    Ok(None) => panic!("key {:02x?} NOT FOUND after PS failover", &k[..4]),
                    Err(e) => {
                        assert!(
                            t0.elapsed() < Duration::from_secs(90),
                            "reads did not recover within 90s: {e}"
                        );
                        compio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            };
            assert_eq!(&got, v, "value mismatch for {:02x?}", &k[..4]);
        }

        // Write liveness on BOTH partitions post-failover.
        for part_idx in 0..2 {
            let k = key_for(part_idx, 90_000 + part_idx as u32);
            let t1 = Instant::now();
            loop {
                match cluster.put(&k, b"post-failover").await {
                    Ok(()) => break,
                    Err(e) => {
                        assert!(
                            t1.elapsed() < Duration::from_secs(60),
                            "post-failover write wedged on part_idx={part_idx}: {e}"
                        );
                        compio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }

        // Restart the killed PS with the same psid; cluster must stay
        // consistent (partitions may or may not rebalance back).
        pses[victim_idx].respawn(mgr_addr);
        compio::time::sleep(Duration::from_secs(5)).await;
        for (k, v) in expected.iter().step_by(7) {
            let got = cluster.get(k).await.expect("get after respawn");
            assert_eq!(got.as_deref(), Some(v.as_slice()), "post-respawn mismatch");
        }
        let k = key_for(0, 95_000);
        cluster.put(&k, b"post-respawn").await.expect("post-respawn write");
    });
}

#[test]
#[ignore] // requires built binaries; boots a full cluster
fn ps_kill_during_write_storm() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, mut pses, parts) = boot(mgr_addr, 220).await;

        let cluster = ClusterClient::connect(&mgr_addr.to_string())
            .await
            .expect("ClusterClient::connect");
        cluster.set_rpc_timeout(Duration::from_secs(5));

        let asg = assignment(&mgr).await;
        let victim_ps = *asg.get(&parts[0]).expect("part A assigned");
        let victim_idx = pses.iter().position(|p| p.psid == victim_ps).unwrap();

        // Storm: interleave writes; kill the victim mid-storm. ACKed
        // writes go into `acked`; timed-out/error writes are dropped from
        // the expectation set (uncertain, may or may not have landed —
        // feedback_chaos_timeout_uncertain).
        let mut acked: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut killed = false;
        let t0 = Instant::now();
        let mut i: u32 = 0;
        while t0.elapsed() < Duration::from_secs(40) {
            let part_idx = (i % 2) as usize;
            let k = key_for(part_idx, 10_000 + i);
            let v = format!("storm-{i}").into_bytes();
            match cluster.put(&k, &v).await {
                Ok(()) => acked.push((k, v)),
                Err(_) => { /* uncertain — drop from expectations */ }
            }
            i += 1;
            if !killed && t0.elapsed() > Duration::from_secs(5) {
                eprintln!("killing ps {victim_ps} mid-storm at i={i}");
                pses[victim_idx].kill();
                killed = true;
            }
        }
        eprintln!("storm done: {} acked / {} attempted", acked.len(), i);
        assert!(killed, "kill never fired");
        // The storm must have made progress AFTER the kill (writes to the
        // survivor's partition + migrated partition once reassigned).
        assert!(
            acked.len() as u32 > i / 4,
            "storm acked too few writes ({} of {i}) — write path wedged",
            acked.len()
        );

        await_assignment(&mgr, &parts, Some(victim_ps), Duration::from_secs(45)).await;

        // EVERY acked write must be present and exact. Reads retry while
        // the survivor finishes opening.
        let t1 = Instant::now();
        for (k, v) in &acked {
            let got = loop {
                match cluster.get(k).await {
                    Ok(Some(g)) => break g,
                    Ok(None) => panic!(
                        "ACKED key {:02x?} lost after PS kill (data loss!)",
                        &k[..4]
                    ),
                    Err(e) => {
                        assert!(
                            t1.elapsed() < Duration::from_secs(90),
                            "reads did not recover: {e}"
                        );
                        compio::time::sleep(Duration::from_millis(300)).await;
                    }
                }
            };
            assert_eq!(&got, v, "acked value mismatch for {:02x?}", &k[..4]);
        }
    });
}
