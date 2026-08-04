//! G1 — "SIGSTOP zombie writer" reproduce-first chaos harness.
//!
//! HYPOTHESIS (from an architecture review; verified against code before
//! writing this harness — see the report):
//!   - EN write fence is `owner_epoch`, raised on the EN ONLY by the first
//!     append carrying a higher revision (`extent_node.rs` step 3/3b).
//!   - On partition takeover the new owner's `open_partition` acquires a
//!     FRESH *manager-level* owner epoch but does NOT seal the inherited
//!     open tail and does NOT push its epoch to the EN until its first
//!     write. So an "idle takeover" leaves the OLD revision valid at the EN.
//!   - `handle_get` has NO server-side write fence — only the client-stamped
//!     `region_epoch`, which a sticky rebalance (ps_id-only move) does NOT
//!     bump.
//!
//! PREDICTED ANOMALY: freeze the partition-holding PS past the manager
//! eviction window (SIGSTOP > 10 s) so the manager reassigns the partition
//! to another PS (idle takeover). Then SIGCONT. A write issued to the OLD
//! (thawed) owner carries the OLD revision; because the EN fence was never
//! raised (idle takeover) the zombie append PASSES the fence, lands in the
//! log extent, and ACKs its client — yet the new owner (which already
//! finished its recovery replay) never sees it:
//!   (a) LOST UPDATE — a cleanly-ACKed write is absent on the new owner.
//!   (b) STALE READ  — a pinned client reading the old PS gets a value the
//!       new authoritative owner does not have (split-brain / non-monotonic).
//!
//! REPRODUCE-FIRST STRATEGY (deterministic, black-box, no product change):
//! rather than race a single in-flight append against SIGSTOP timing, we
//! issue the zombie write to the OLD owner *after* SIGCONT — inside the
//! ~1-2 s window before the thawed PS re-syncs and closes the reassigned
//! partition. By then the survivor has provably replayed the seed value, so
//! any write the old owner still lands is provably post-takeover. This turns
//! the "did the append reach the EN before the freeze" race into a wide,
//! reliably-hit window. (A pure-timing SIGSTOP-mid-append variant is noted
//! in the report but is inherently flaky on loopback.)
//!
//! Acked-vs-uncertain discipline (feedback_chaos_timeout_uncertain): a write
//! whose RPC times out is UNCERTAIN and dropped from the must-persist set;
//! only a cleanly-ACKed (CODE_OK) write is a real lost-update / stale-read.
//!
//! PSes run as real `autumn-ps` SUBPROCESSES so they are SIGSTOP/SIGCONT-able
//! (the same host/template as `system_ps_failover_chaos.rs`). Build first:
//!   cargo build --release -p autumn-ps
//! Run:
//!   cargo test -p autumn-manager --test system_sigstop_zombie_writer \
//!       -- --ignored --nocapture

mod support;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;

use support::*;

// ── subprocess PS wrapper (SIGSTOP/SIGCONT-able) ──────────────────────

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
        .unwrap_or_else(|| panic!("autumn-ps binary not found under {} — run `cargo build --release -p autumn-ps` first", target.display()))
}

struct PsProc {
    psid: u64,
    child: Option<Child>,
}

impl PsProc {
    fn spawn(psid: u64, mgr: SocketAddr) -> Self {
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
            child: Some(child),
        }
    }

    fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(|c| c.id())
    }

    /// PauseResume fault, freeze half: SIGSTOP the whole PS process. Freezes
    /// every thread (incl. the heartbeat loop → manager eviction fires) and
    /// suspends any in-flight append.
    fn sigstop(&self) {
        let pid = self.pid().expect("live child for SIGSTOP");
        let ok = Command::new("kill")
            .arg("-STOP")
            .arg(pid.to_string())
            .status()
            .expect("run kill -STOP")
            .success();
        assert!(ok, "kill -STOP {pid} failed");
    }

    /// PauseResume fault, thaw half: SIGCONT. Every thread resumes exactly
    /// where it was frozen.
    fn sigcont(&self) {
        let pid = self.pid().expect("live child for SIGCONT");
        let ok = Command::new("kill")
            .arg("-CONT")
            .arg(pid.to_string())
            .status()
            .expect("run kill -CONT")
            .success();
        assert!(ok, "kill -CONT {pid} failed");
    }

    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            // Ensure a stopped process is resumable so kill+wait doesn't hang.
            if let Some(pid) = Some(c.id()) {
                let _ = Command::new("kill").arg("-CONT").arg(pid.to_string()).status();
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for PsProc {
    fn drop(&mut self) {
        self.kill();
    }
}

// ── per-key operation history ─────────────────────────────────────────

#[derive(Clone)]
struct OpRec {
    t_invoke_ms: u128,
    t_complete_ms: u128,
    kind: &'static str, // seed-put / zombie-put / survivor-get / victim-get / probe
    who: &'static str,  // pinned-victim / fresh-survivor
    key: String,
    value: String,
    outcome: String, // ACK(code=0) / REJECT(code=N) / uncertain(timeout) / value=.. / miss
}

struct History {
    start: Instant,
    recs: Vec<OpRec>,
}

impl History {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            recs: Vec::new(),
        }
    }
    fn now_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }
    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        t_invoke_ms: u128,
        kind: &'static str,
        who: &'static str,
        key: &[u8],
        value: &str,
        outcome: String,
    ) {
        let t_complete_ms = self.now_ms();
        self.recs.push(OpRec {
            t_invoke_ms,
            t_complete_ms,
            kind,
            who,
            key: String::from_utf8_lossy(key).to_string(),
            value: value.to_string(),
            outcome,
        });
    }
    fn dump(&self) {
        eprintln!("─── operation history (invoke→complete ms) ───────────────────");
        for r in &self.recs {
            eprintln!(
                "  [{:>6}→{:>6}ms] {:<12} {:<15} key={:<16} val={:<10} => {}",
                r.t_invoke_ms, r.t_complete_ms, r.kind, r.who, r.key, r.value, r.outcome
            );
        }
        eprintln!("──────────────────────────────────────────────────────────────");
    }
}

// ── raw pinned-client RPC (never refreshes routing) ───────────────────

async fn raw_put(cli: &RpcClient, part_id: u64, key: &[u8], value: &[u8]) -> Result<u8, String> {
    let payload = partition_rpc::rkyv_encode(&partition_rpc::PutReq {
        part_id,
        key: key.to_vec(),
        value: value.to_vec(),
        expires_at: 0,
        region_epoch: 0, // pinned client: skip freshness check (stale-client model)
        inode_hint: 0,
        lease_epoch: 0,
    });
    match cli.call(partition_rpc::MSG_PUT, payload).await {
        Ok(resp) => partition_rpc::rkyv_decode::<partition_rpc::PutResp>(&resp)
            .map(|r| r.code)
            .map_err(|e| format!("decode PutResp: {e}")),
        Err(e) => Err(format!("call: {e}")),
    }
}

async fn raw_get(cli: &RpcClient, part_id: u64, key: &[u8]) -> Result<(u8, Vec<u8>), String> {
    let payload = partition_rpc::rkyv_encode(&partition_rpc::GetReq {
        part_id,
        key: key.to_vec(),
        offset: 0,
        length: 0,
        region_epoch: 0,
    });
    match cli.call(partition_rpc::MSG_GET, payload).await {
        Ok(resp) => partition_rpc::rkyv_decode::<partition_rpc::GetResp>(&resp)
            .map(|r| (r.code, r.value))
            .map_err(|e| format!("decode GetResp: {e}")),
        Err(e) => Err(format!("call: {e}")),
    }
}

// ── manager helpers ───────────────────────────────────────────────────

async fn assignment(mgr: &RpcClient) -> HashMap<u64, u64> {
    get_regions(mgr)
        .await
        .regions
        .iter()
        .map(|(_, r)| (r.part_id, r.ps_id))
        .collect()
}

async fn part_addr(mgr: &RpcClient, part_id: u64) -> Option<SocketAddr> {
    get_regions(mgr)
        .await
        .part_addrs
        .into_iter()
        .find(|(pid, _)| *pid == part_id)
        .and_then(|(_, a)| a.parse().ok())
}

// ── boot: manager (in-proc) + 2 EN (in-proc) + 2 subprocess PS + 2 parts ─

async fn boot(mgr_addr: SocketAddr, base: u16) -> (Rc<RpcClient>, Vec<PsProc>, u64, u64) {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();

    let n1 = pick_addr();
    let n2 = pick_addr();
    let d1 = tempfile::tempdir().expect("d1");
    let d2 = tempfile::tempdir().expect("d2");
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

    // Wait until both partitions are assigned + opened + part_addrs published.
    let t0 = Instant::now();
    loop {
        let asg = assignment(&mgr).await;
        let a_ok = asg.get(&part_a).is_some() && part_addr(&mgr, part_a).await.is_some();
        let b_ok = asg.get(&part_b).is_some() && part_addr(&mgr, part_b).await.is_some();
        if a_ok && b_ok {
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(30), "cluster never came up");
        compio::time::sleep(Duration::from_millis(300)).await;
    }
    compio::time::sleep(Duration::from_millis(1500)).await;
    (mgr, vec![ps1, ps2], part_a, part_b)
}

/// timeout-wrapped get so a call to a frozen PS can't hang the driver.
async fn get_bounded(
    addr: SocketAddr,
    part_id: u64,
    key: &[u8],
    to: Duration,
) -> Result<(u8, Vec<u8>), String> {
    let fut = async {
        let cli = RpcClient::connect(addr).await.map_err(|e| format!("connect: {e}"))?;
        raw_get(&cli, part_id, key).await
    };
    match compio::time::timeout(to, fut).await {
        Ok(r) => r,
        Err(_) => Err("timeout".into()),
    }
}

#[test]
#[ignore] // requires the built autumn-ps binary; boots a full subprocess cluster
fn sigstop_zombie_writer_g1() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut hist = History::new();
        let (mgr, mut pses, part_a, _part_b) = boot(mgr_addr, 230).await;

        // The single victim key lives in partition A's range (prefix < 0x80).
        let key: &[u8] = b"a/zombie-key";
        let v0 = b"V0-original".to_vec();
        let v_zombie = b"V_ZOMBIE-lost".to_vec();

        // Identify the victim PS (owner of A) + its partition-A listener addr.
        let asg = assignment(&mgr).await;
        let victim_ps = *asg.get(&part_a).expect("part A assigned");
        let victim_idx = pses
            .iter()
            .position(|p| p.psid == victim_ps)
            .expect("victim in ps set");
        let survivor_ps = pses
            .iter()
            .find(|p| p.psid != victim_ps)
            .map(|p| p.psid)
            .expect("survivor exists");
        let victim_a_addr = part_addr(&mgr, part_a).await.expect("part A addr");
        eprintln!(
            "victim_ps={victim_ps} (pid={:?}, part-A @ {victim_a_addr}); survivor_ps={survivor_ps}",
            pses[victim_idx].pid()
        );

        // ── seed K=V0 via the pinned client to the victim (acked) ──
        let pinned = RpcClient::connect(victim_a_addr)
            .await
            .expect("connect pinned victim");
        {
            let ti = hist.now_ms();
            let code = raw_put(&pinned, part_a, key, &v0).await.expect("seed put call");
            assert_eq!(code, partition_rpc::CODE_OK, "seed put not acked (code={code})");
            hist.push(ti, "seed-put", "pinned-victim", key, "V0", format!("ACK(code={code})"));
        }
        // read-back V0 from the victim (still the owner)
        {
            let ti = hist.now_ms();
            let (c, v) = raw_get(&pinned, part_a, key).await.expect("seed get");
            assert_eq!(c, partition_rpc::CODE_OK);
            assert_eq!(v, v0, "seed read-back mismatch");
            hist.push(ti, "victim-get", "pinned-victim", key, "V0", "value=V0".into());
        }

        // ── SIGSTOP the victim > eviction window ──
        let t_stop = hist.now_ms();
        pses[victim_idx].sigstop();
        eprintln!("[{t_stop}ms] SIGSTOP victim_ps={victim_ps}");

        // ── wait for takeover: A reassigned away from victim + survivor
        //    provably replayed V0 (reached via a FRESH refreshing route) ──
        let survivor_a_addr: SocketAddr = {
            let t0 = Instant::now();
            loop {
                let asg = assignment(&mgr).await;
                let now_ps = asg.get(&part_a).copied();
                if now_ps.is_some() && now_ps != Some(victim_ps) {
                    // reassigned; resolve the NEW addr and confirm it isn't the frozen victim
                    if let Some(a) = part_addr(&mgr, part_a).await {
                        if a != victim_a_addr {
                            // confirm the survivor has replayed V0
                            if let Ok((c, v)) =
                                get_bounded(a, part_a, key, Duration::from_secs(3)).await
                            {
                                if c == partition_rpc::CODE_OK && v == v0 {
                                    let ti = hist.now_ms();
                                    hist.push(ti, "survivor-get", "fresh-survivor", key, "V0",
                                        "value=V0 (replayed)".into());
                                    break a;
                                }
                            }
                        }
                    }
                }
                assert!(
                    t0.elapsed() < Duration::from_secs(60),
                    "takeover/replay did not complete within 60s (asg={asg:?})"
                );
                compio::time::sleep(Duration::from_millis(500)).await;
            }
        };
        let t_takeover = hist.now_ms();
        eprintln!(
            "[{t_takeover}ms] TAKEOVER complete: part A now @ {survivor_a_addr} (survivor), replayed V0. \
             Held frozen for ~{}s.",
            (t_takeover - t_stop) / 1000
        );

        // ── SIGCONT the victim; it still believes it owns A with the OLD
        //    revision. Immediately drive the pinned (stale) client's zombie
        //    write before the thawed PS re-syncs and closes A. ──
        pses[victim_idx].sigcont();
        let t_cont = hist.now_ms();
        eprintln!("[{t_cont}ms] SIGCONT victim_ps={victim_ps}");

        // Zombie write to the OLD (thawed) owner. Use the ALREADY-OPEN pinned
        // connection FIRST (no connect latency → beats the re-sync close),
        // then reconnect-retry as a safety net. On a clean ACK, immediately
        // read back on the SAME connection to capture the STALE-READ evidence
        // before the old owner closes the reassigned partition.
        let mut zombie_ack = false;
        let mut zombie_last_code: Option<u8> = None;
        let mut zombie_err: Option<String> = None;
        let mut victim_stale_val: Option<Vec<u8>> = None;

        // attempt 0: the existing pinned connection (fastest).
        {
            let ti = hist.now_ms();
            match compio::time::timeout(
                Duration::from_secs(2),
                raw_put(&pinned, part_a, key, &v_zombie),
            )
            .await
            {
                Ok(Ok(code)) => {
                    zombie_last_code = Some(code);
                    if code == partition_rpc::CODE_OK {
                        zombie_ack = true;
                        hist.push(ti, "zombie-put", "pinned-victim", key, "V_ZOMBIE",
                            format!("ACK(code={code})"));
                        // immediate stale read on the SAME connection
                        let ti2 = hist.now_ms();
                        if let Ok((c, v)) = raw_get(&pinned, part_a, key).await {
                            if c == partition_rpc::CODE_OK {
                                victim_stale_val = Some(v.clone());
                                hist.push(ti2, "victim-get", "pinned-victim", key,
                                    display_val(&v), format!("value={}", display_val(&v)));
                            }
                        }
                    } else {
                        hist.push(ti, "zombie-put", "pinned-victim", key, "V_ZOMBIE",
                            format!("REJECT(code={code})"));
                    }
                }
                Ok(Err(e)) => {
                    zombie_err = Some(e.clone());
                    hist.push(ti, "zombie-put", "pinned-victim", key, "V_ZOMBIE", format!("ERR({e})"));
                }
                Err(_) => {
                    zombie_err = Some("timeout".into());
                    hist.push(ti, "zombie-put", "pinned-victim", key, "V_ZOMBIE",
                        "uncertain(timeout)".into());
                }
            }
        }

        // safety-net reconnect retries if attempt 0 didn't cleanly ACK.
        if !zombie_ack {
            let t0 = Instant::now();
            while t0.elapsed() < Duration::from_secs(4) {
                let ti = hist.now_ms();
                let attempt = async {
                    let cli = RpcClient::connect(victim_a_addr)
                        .await
                        .map_err(|e| format!("connect: {e}"))?;
                    let code = raw_put(&cli, part_a, key, &v_zombie).await?;
                    let sv = if code == partition_rpc::CODE_OK {
                        raw_get(&cli, part_a, key)
                            .await
                            .ok()
                            .filter(|(c, _)| *c == partition_rpc::CODE_OK)
                            .map(|(_, v)| v)
                    } else {
                        None
                    };
                    Ok::<_, String>((code, sv))
                };
                match compio::time::timeout(Duration::from_secs(2), attempt).await {
                    Ok(Ok((code, sv))) => {
                        zombie_last_code = Some(code);
                        if code == partition_rpc::CODE_OK {
                            zombie_ack = true;
                            victim_stale_val = sv;
                            hist.push(ti, "zombie-put", "pinned-victim", key, "V_ZOMBIE",
                                format!("ACK(code={code})"));
                            break;
                        } else {
                            hist.push(ti, "zombie-put", "pinned-victim", key, "V_ZOMBIE",
                                format!("REJECT(code={code})"));
                        }
                    }
                    Ok(Err(e)) => {
                        zombie_err = Some(e.clone());
                        hist.push(ti, "zombie-put", "pinned-victim", key, "V_ZOMBIE", format!("ERR({e})"));
                    }
                    Err(_) => {
                        zombie_err = Some("timeout".into());
                        hist.push(ti, "zombie-put", "pinned-victim", key, "V_ZOMBIE",
                            "uncertain(timeout)".into());
                    }
                }
                compio::time::sleep(Duration::from_millis(120)).await;
            }
        }
        eprintln!(
            "[{}ms] zombie write: acked={zombie_ack} last_code={zombie_last_code:?} err={zombie_err:?}",
            hist.now_ms()
        );

        // ── read the authoritative (survivor) value AND the pinned (victim)
        //    value to classify the anomaly ──
        let survivor_val = get_bounded(survivor_a_addr, part_a, key, Duration::from_secs(5))
            .await
            .ok()
            .filter(|(c, _)| *c == partition_rpc::CODE_OK)
            .map(|(_, v)| v);
        {
            let ti = hist.now_ms();
            let disp = match &survivor_val {
                Some(v) if *v == v0 => "value=V0",
                Some(v) if *v == v_zombie => "value=V_ZOMBIE",
                Some(_) => "value=OTHER",
                None => "miss/err",
            };
            hist.push(ti, "survivor-get", "fresh-survivor", key,
                survivor_val.as_deref().map(display_val).unwrap_or("∅"),
                format!("{disp}"));
        }
        let victim_val = get_bounded(victim_a_addr, part_a, key, Duration::from_secs(3))
            .await
            .ok()
            .filter(|(c, _)| *c == partition_rpc::CODE_OK)
            .map(|(_, v)| v);
        {
            let ti = hist.now_ms();
            let disp = match &victim_val {
                Some(v) if *v == v0 => "value=V0",
                Some(v) if *v == v_zombie => "value=V_ZOMBIE",
                Some(_) => "value=OTHER",
                None => "miss/err(closed)",
            };
            hist.push(ti, "victim-late", "pinned-victim", key,
                victim_val.as_deref().map(display_val).unwrap_or("∅"),
                format!("{disp}"));
        }

        hist.dump();

        // ── verdict ──
        // LOST UPDATE: the old owner cleanly ACKed V_ZOMBIE, yet the new
        // authoritative owner does NOT reflect it.
        let lost_update = zombie_ack && survivor_val.as_deref() != Some(v_zombie.as_slice());
        // STALE READ / split-brain: the old owner serves V_ZOMBIE (captured
        // immediately after the ACK on the pinned conn, or on the late read)
        // while the authoritative survivor still holds V0.
        let victim_ghost = victim_stale_val.as_deref() == Some(v_zombie.as_slice())
            || victim_val.as_deref() == Some(v_zombie.as_slice());
        let stale_read = victim_ghost && survivor_val.as_deref() == Some(v0.as_slice());

        eprintln!("╔══════════════════════ G1 VERDICT ══════════════════════════╗");
        eprintln!("║ zombie write cleanly ACKed by OLD owner : {}", yn(zombie_ack));
        eprintln!(
            "║ authoritative (survivor) value          : {}",
            survivor_val.as_deref().map(display_val).unwrap_or("∅")
        );
        eprintln!(
            "║ pinned (old owner) value                : {}",
            victim_stale_val
                .as_deref()
                .or(victim_val.as_deref())
                .map(display_val)
                .unwrap_or("∅")
        );
        eprintln!("║ (a) LOST UPDATE (acked write absent new owner): {}", yn(lost_update));
        eprintln!("║ (b) STALE READ  (old owner serves ghost value): {}", yn(stale_read));
        eprintln!("╚═════════════════════════════════════════════════════════════╝");

        // FIX VERIFICATION (was: assert the anomaly reproduces). The eager
        // takeover fence — `StreamClient::fence_tail` on `open_partition`
        // raising the EN `owner_epoch` floor to E_new BEFORE the partition
        // serves — must make the OLD owner's stale-epoch (E_old) append
        // REJECTED at the EN, so it can no longer be cleanly ACKed into a lost
        // update.
        if !zombie_ack {
            eprintln!(
                "G1 FIXED: the OLD owner's post-thaw write was NOT cleanly ACKed \
                 (last_code={zombie_last_code:?}) — the eager takeover fence rejected \
                 the stale-epoch append (LockedByOther), or the reassigned partition \
                 had already closed. No lost update."
            );
        } else if survivor_val.as_deref() == Some(v_zombie.as_slice()) {
            eprintln!(
                "G1: zombie acked AND the survivor also reflects it — the write \
                 reached the authoritative owner (not a lost update). Unexpected for \
                 a stale owner post-takeover; inspect the history above."
            );
        } else {
            eprintln!(
                "G1 REGRESSION: the OLD owner cleanly ACKed a stale-epoch write the \
                 new authoritative owner does NOT have — the takeover fence did not fire."
            );
        }

        // Stale-read is the READ-side `handle_get` fence — OUT OF SCOPE for this
        // WRITE-side fix. Record it HONESTLY; do NOT assert it away. With the
        // write fenced the zombie value never lands, so a stale read of it
        // should not occur either — but if it does (a residual old-owner serving
        // window before it closes the reassigned partition), that is a DOCUMENTED
        // separate follow-up, not a failure of this fix.
        eprintln!(
            "stale-read status (read-side handle_get, out of scope for this fix): {}",
            yn(stale_read)
        );
        if stale_read {
            eprintln!(
                "NOTE: a STALE READ was still observed (old owner served a ghost value \
                 while the survivor holds V0). That is the read-side handle_get fence — \
                 a separate follow-up — NOT regressed by this write-side fix."
            );
        }

        // Harness self-guards already asserted above (seed acked; survivor
        // provably replayed V0). REGRESSION GUARD: the zombie write can no
        // longer be silently ACKed with its stale `owner_epoch` — the write-side
        // guarantee this fix provides. `lost_update ⊆ zombie_ack`, so asserting
        // `!zombie_ack` is the strictly stronger, non-flaky form (it also fails
        // if the write acked yet happened to propagate). A missed window makes
        // the write error out (`zombie_ack=false`) and still passes — the
        // assertion only fails if a stale-epoch write is genuinely accepted.
        assert!(
            !zombie_ack,
            "G1 REGRESSION: the OLD (zombie) owner cleanly ACKed a write carrying its \
             STALE owner_epoch after an idle takeover (lost_update={lost_update}, \
             stale_read={stale_read}, last_code={zombie_last_code:?}). The eager \
             takeover fence (StreamClient::fence_tail on open_partition, raising the \
             EN owner_epoch floor to E_new) failed to reject it."
        );

        // teardown: resume/kill children (Drop also covers this).
        for p in &mut pses {
            p.kill();
        }
        drop(mgr);
    });
}

fn display_val(v: &[u8]) -> &'static str {
    if v == b"V0-original" {
        "V0"
    } else if v == b"V_ZOMBIE-lost" {
        "V_ZOMBIE"
    } else {
        "OTHER"
    }
}

fn yn(b: bool) -> &'static str {
    if b {
        "YES"
    } else {
        "no"
    }
}
