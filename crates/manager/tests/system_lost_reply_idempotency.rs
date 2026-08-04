//! G9 — LOST-REPLY / DUPLICATE-DELIVERY idempotency on manager control RPCs.
//!
//! REPRODUCE-ONLY (no fix). A *lost reply* is a distinct fault from a full
//! network partition: the manager RECEIVED the request, PROCESSED it, and its
//! EFFECT COMMITTED — but the ack never reached the client, so the client's
//! retry machinery (`StreamClient::retry_manager_call`, 20×) re-sends the SAME
//! request. The invariant under test: however many times a control RPC is
//! re-delivered after its effect landed, the cluster CONVERGES to the same
//! state — no duplicate extent, no duplicate split child, no over-seal of a
//! fresh tail at a stale commit, no resurrected/removed-twice extent.
//!
//! ── How the fault is injected ────────────────────────────────────────────
//! An in-process TCP proxy (`spawn_lost_reply_proxy`) fronts the MANAGER's
//! port. For a chosen `msg_type` it forwards the request UPSTREAM, waits for
//! the manager to answer (so the effect is *guaranteed* committed), then DROPS
//! that reply and resets the client connection — the client's `call` resolves
//! `ConnectionClosed`, exactly as a timed-out RPC does. `call_with_lost_reply_retry`
//! then reconnects and re-sends the byte-identical request, mirroring the real
//! `retry_manager_call` loop. The proxy can drop the first N replies so we can
//! prove convergence is independent of retry count. Only the operation-under-
//! test flows through the proxy; cluster setup + final assertions use a direct
//! manager client.
//!
//! ── The three idempotency mechanisms exercised ───────────────────────────
//!   (a) alloc / seal+roll  — `StreamAllocExtentReq.seal_extent_id` pins the
//!       retry: the manager seals+rolls ONLY while the current tail still ==
//!       `seal_extent_id`; once a prior attempt rolled a fresh tail, the retry
//!       is an idempotent no-op that RETURNS the fresh tail untouched (guard in
//!       `handle_stream_alloc_extent`, rpc_handlers.rs; stream note 21).
//!   (b) multi_modify_split — `split_inflight` RAII guard (concurrent retry) +
//!       the mid_key range check (post-completion retry): after the source
//!       partition's `end_key` becomes `mid_key`, a re-delivered split with the
//!       same `mid_key` is out-of-range and refused — no second child. This is
//!       the fix for the real 1→6 split-retry cascade (scripts/split_repro6).
//!   (c) punch_holes / GC delete — `removed = requested ∩ current members`: a
//!       retry finds the extent already gone → empty removal → idempotent
//!       no-op.
//!
//! Retry classification (`is_transient_conflict`) lives in stream/src/client.rs;
//! the *server-side* idempotency guards it converges onto live in the three
//! manager handlers above.
//!
//! NOTE: default (TCP) transport only. The proxy speaks the 10-byte wire header
//! (`[req_id:4][msg_type:1][flags:1][payload_len:4]`) directly.

mod support;

use std::io::Read;
use std::io::Write;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::RpcError;

use support::*;

// ── Wire framing (mirror of autumn_rpc::frame, kept local so we never touch
//    shared files) ────────────────────────────────────────────────────────
const HEADER_LEN: usize = 10;
const FLAG_RESPONSE: u8 = 0x01;

/// Read exactly one wire frame from `r`. Returns `(msg_type, flags, raw_bytes)`
/// where `raw_bytes` is the full header+payload ready to forward. `None` on
/// EOF / socket error.
fn read_frame(r: &mut impl Read) -> Option<(u8, u8, Vec<u8>)> {
    let mut header = [0u8; HEADER_LEN];
    r.read_exact(&mut header).ok()?;
    let msg_type = header[4];
    let flags = header[5];
    let payload_len =
        u32::from_le_bytes([header[6], header[7], header[8], header[9]]) as usize;
    let mut raw = Vec::with_capacity(HEADER_LEN + payload_len);
    raw.extend_from_slice(&header);
    if payload_len > 0 {
        let mut payload = vec![0u8; payload_len];
        r.read_exact(&mut payload).ok()?;
        raw.extend_from_slice(&payload);
    }
    Some((msg_type, flags, raw))
}

// ── In-process lost-reply proxy ───────────────────────────────────────────

pub struct ProxyStats {
    /// Remaining target replies to DROP (shared across reconnects).
    drops_remaining: AtomicUsize,
    /// Target replies actually dropped so far.
    dropped: AtomicUsize,
    /// Target REQUESTS forwarded upstream (proof the effect could commit).
    target_reqs: AtomicUsize,
}

impl ProxyStats {
    fn take_drop(&self) -> bool {
        let mut cur = self.drops_remaining.load(Ordering::SeqCst);
        loop {
            if cur == 0 {
                return false;
            }
            match self.drops_remaining.compare_exchange(
                cur,
                cur - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
    fn dropped(&self) -> usize {
        self.dropped.load(Ordering::SeqCst)
    }
    fn target_reqs(&self) -> usize {
        self.target_reqs.load(Ordering::SeqCst)
    }
}

/// Front `upstream` (the manager) with a proxy on a fresh loopback port.
/// For `target_msg`, the proxy forwards the request, lets the manager answer
/// (effect commits), then DROPS the first `drop_budget` replies and resets the
/// connection so the client sees `ConnectionClosed` and retries.
fn spawn_lost_reply_proxy(
    upstream: SocketAddr,
    target_msg: u8,
    drop_budget: usize,
) -> (SocketAddr, Arc<ProxyStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind lost-reply proxy");
    let front = listener.local_addr().expect("proxy front addr");
    let stats = Arc::new(ProxyStats {
        drops_remaining: AtomicUsize::new(drop_budget),
        dropped: AtomicUsize::new(0),
        target_reqs: AtomicUsize::new(0),
    });
    let stats_accept = stats.clone();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let client = match conn {
                Ok(c) => c,
                Err(_) => continue,
            };
            let up = match TcpStream::connect(upstream) {
                Ok(u) => u,
                Err(_) => {
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                }
            };
            // request pump: client → manager (also counts target requests)
            let client_r = client.try_clone().expect("clone client");
            let up_w = up.try_clone().expect("clone up");
            let stats_req = stats_accept.clone();
            std::thread::spawn(move || pump_requests(client_r, up_w, target_msg, stats_req));
            // response pump: manager → client, with drop-and-reset logic
            let stats_resp = stats_accept.clone();
            std::thread::spawn(move || pump_responses(up, client, target_msg, stats_resp));
        }
    });
    (front, stats)
}

fn pump_requests(mut from: TcpStream, mut to: TcpStream, target: u8, stats: Arc<ProxyStats>) {
    while let Some((msg, flags, raw)) = read_frame(&mut from) {
        if msg == target && (flags & FLAG_RESPONSE) == 0 {
            stats.target_reqs.fetch_add(1, Ordering::SeqCst);
        }
        if to.write_all(&raw).is_err() {
            break;
        }
    }
    let _ = to.shutdown(Shutdown::Both);
}

fn pump_responses(mut from: TcpStream, mut to: TcpStream, target: u8, stats: Arc<ProxyStats>) {
    while let Some((msg, flags, raw)) = read_frame(&mut from) {
        let is_response = (flags & FLAG_RESPONSE) != 0;
        if is_response && msg == target && stats.take_drop() {
            // The manager ANSWERED (effect committed), but we swallow the ack
            // and reset both halves → the client's `call` sees ConnectionClosed.
            stats.dropped.fetch_add(1, Ordering::SeqCst);
            let _ = to.shutdown(Shutdown::Both);
            let _ = from.shutdown(Shutdown::Both);
            return;
        }
        if to.write_all(&raw).is_err() {
            break;
        }
    }
    let _ = to.shutdown(Shutdown::Both);
}

/// Send `msg_type(payload)` through the proxy, retrying on a lost reply exactly
/// as `retry_manager_call` would (fresh connection per attempt). Returns
/// `(response_bytes, attempts)`. Panics on a non-transport error or if retries
/// are exhausted.
async fn call_with_lost_reply_retry(
    front: SocketAddr,
    msg_type: u8,
    payload: Bytes,
    max_attempts: u32,
) -> (Bytes, u32) {
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let client = RpcClient::connect(front)
            .await
            .expect("connect proxy front");
        match client.call(msg_type, payload.clone()).await {
            Ok(resp) => return (resp, attempts),
            Err(RpcError::ConnectionClosed) => {
                assert!(
                    attempts < max_attempts,
                    "lost-reply retries exhausted after {attempts} attempts"
                );
                compio::time::sleep(Duration::from_millis(80)).await;
            }
            Err(other) => panic!("unexpected non-transport RPC error: {other:?}"),
        }
    }
}

// ── direct-to-manager helpers (setup + assertions, no proxy) ───────────────

async fn stream_extents(mgr: &RpcClient, stream_id: u64) -> (Vec<u64>, u64) {
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
    let si = r
        .streams
        .iter()
        .find(|(id, _)| *id == stream_id)
        .map(|(_, s)| s.clone())
        .expect("stream present");
    let tail = *si.extent_ids.last().expect("stream has a tail extent");
    (si.extent_ids, tail)
}

async fn extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode ExtentInfoResp");
    r.extent.expect("extent present")
}

/// Register a PS *node* (no real server) so `rebalance_regions` assigns the
/// partition and `get_regions` reports it — the lightest way to count split
/// children without standing up a partition server.
async fn register_dummy_ps(mgr: &RpcClient, ps_id: u64) {
    let resp = mgr
        .call(
            MSG_REGISTER_PS,
            rkyv_encode(&RegisterPsReq {
                ps_id,
                address: "127.0.0.1:1".to_string(),
            }),
        )
        .await
        .expect("register_ps");
    let r: CodeResp = rkyv_decode(&resp).expect("decode CodeResp");
    assert_eq!(r.code, CODE_OK, "register_ps failed: {}", r.message);
}

/// A direct (non-proxied) authoritative seal+roll: seal `tail` at
/// `seal_commit`, allocate a fresh tail. Used to grow a stream for the punch
/// test.
async fn alloc_roll(mgr: &RpcClient, stream_id: u64, tail: u64, seal_commit: u64) {
    let resp = mgr
        .call(
            MSG_STREAM_ALLOC_EXTENT,
            rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key: String::new(),
                owner_epoch: 0,
                seal_commit: Some(seal_commit),
                exclude_node_ids: vec![],
                seal_extent_id: tail,
            }),
        )
        .await
        .expect("alloc_roll");
    let r: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode StreamAllocExtentResp");
    assert_eq!(r.code, CODE_OK, "alloc_roll failed: {}", r.message);
}

// ── (a) alloc / seal-roll under lost reply ─────────────────────────────────

/// A seal+roll whose reply is lost commits ONE new extent; the retry, pinned by
/// `seal_extent_id`, is an idempotent no-op that returns the fresh tail — it
/// must NOT allocate a second extent nor over-seal the fresh tail at the stale
/// authoritative commit.
#[test]
fn lost_reply_alloc_seal_roll_yields_exactly_one_extent() {
    let (mgr_addr, n1, n2, _d1, _d2) = setup_two_node_infra(300);
    // Drop the FIRST alloc reply → the effect commits but the client retries.
    let (front, stats) = spawn_lost_reply_proxy(mgr_addr, MSG_STREAM_ALLOC_EXTENT, 1);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1, n2, 300).await;
        let stream_id = create_stream(&mgr, 2).await;

        let (extents0, e0) = stream_extents(&mgr, stream_id).await;
        assert_eq!(extents0.len(), 1, "fresh stream owns exactly one open tail");

        // Authoritative seal at a NON-zero commit: a mis-idempotent retry would
        // over-seal the FRESH tail at this stale length. seal_extent_id pins it.
        let seal_commit = 4096u64;
        let payload = rkyv_encode(&StreamAllocExtentReq {
            stream_id,
            owner_key: String::new(),
            owner_epoch: 0,
            seal_commit: Some(seal_commit),
            exclude_node_ids: vec![],
            seal_extent_id: e0,
        });
        let (resp_bytes, attempts) =
            call_with_lost_reply_retry(front, MSG_STREAM_ALLOC_EXTENT, payload, 8).await;
        let resp: StreamAllocExtentResp =
            rkyv_decode(&resp_bytes).expect("decode alloc resp");
        assert_eq!(resp.code, CODE_OK, "alloc must converge OK: {}", resp.message);

        // The lost reply forced a retry, and the manager saw the request twice.
        assert!(attempts >= 2, "dropped reply must force a retry (attempts={attempts})");
        assert_eq!(stats.dropped(), 1, "exactly one reply dropped");
        assert!(stats.target_reqs() >= 2, "manager received the alloc >= twice");

        // ── invariants ──
        let (extents1, e1) = stream_extents(&mgr, stream_id).await;
        assert_eq!(
            extents1.len(),
            2,
            "seal-roll under lost reply must create EXACTLY ONE new extent; got {extents1:?}"
        );
        assert_ne!(e1, e0, "the rolled tail is a new extent");

        let ei0 = extent_info(&mgr, e0).await;
        assert!(
            ei0.sealed && ei0.sealed_length == seal_commit,
            "old tail sealed at the authoritative commit (sealed={} len={})",
            ei0.sealed,
            ei0.sealed_length
        );

        let ei1 = extent_info(&mgr, e1).await;
        assert!(
            !ei1.sealed && ei1.sealed_length == 0,
            "FRESH tail must stay OPEN — no over-seal at the stale commit \
             (sealed={} len={})",
            ei1.sealed,
            ei1.sealed_length
        );
    });
}

// ── (b) multi_modify_split under lost reply ────────────────────────────────

/// A split whose reply is lost commits ONE child; the retry (same part_id +
/// mid_key) is refused by the range check (the source's end_key is now the
/// mid_key) — it does NOT multiply into the 1→6 cascade. Convergence is proved
/// by dropping SEVERAL replies (each retry re-delivers the split).
#[test]
fn lost_reply_split_yields_exactly_one_child() {
    let (mgr_addr, n1, n2, _d1, _d2) = setup_two_node_infra(310);
    // Drop up to 3 split replies → at least 3 forced re-deliveries.
    let (front, stats) = spawn_lost_reply_proxy(mgr_addr, MSG_MULTI_MODIFY_SPLIT, 3);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1, n2, 310).await;

        register_dummy_ps(&mgr, 1).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 0, log, row, meta, b"a", b"z").await; // auto-assign part_id

        let regions_before = get_regions(&mgr).await.regions;
        assert_eq!(regions_before.len(), 1, "one partition before split");
        let part_id = regions_before[0].0;

        let payload = rkyv_encode(&MultiModifySplitReq {
            part_id,
            owner_key: String::new(),
            owner_epoch: 0,
            mid_key: b"m".to_vec(),
            log_stream_sealed_length: 0,
            row_stream_sealed_length: 0,
            meta_stream_sealed_length: 0,
        });
        let (resp_bytes, attempts) =
            call_with_lost_reply_retry(front, MSG_MULTI_MODIFY_SPLIT, payload, 12).await;
        // The converged reply is CodeResp: OK if the last delivery raced first,
        // or Precondition ("mid_key not in range") once the effect is in place.
        // The INVARIANT is the child count, not this code.
        let resp: CodeResp = rkyv_decode(&resp_bytes).expect("decode split resp");
        assert!(
            resp.code == CODE_OK || resp.code == CODE_PRECONDITION,
            "unexpected split code {} ({})",
            resp.code,
            resp.message
        );
        assert!(attempts >= 2, "dropped reply must force a retry (attempts={attempts})");
        assert!(stats.dropped() >= 1, "at least one split reply dropped");
        assert!(stats.target_reqs() >= 2, "manager received the split >= twice");

        // ── invariant: EXACTLY ONE child (no 1→6 cascade) ──
        let regions_after = get_regions(&mgr).await.regions;
        assert_eq!(
            regions_after.len(),
            2,
            "split under lost-reply+retry must yield EXACTLY 2 partitions (no cascade); \
             got {}",
            regions_after.len()
        );
        assert!(
            regions_after
                .iter()
                .any(|(_, r)| r.rg.as_ref().map(|g| g.end_key.as_slice())
                    == Some(b"m".as_slice())),
            "the single split boundary must be exactly mid_key"
        );
    });
}

/// Duplicate-delivery variant: the SAME split delivered twice CONCURRENTLY (no
/// proxy) still yields exactly one child — via the `split_inflight` guard
/// and/or the range check. Direct manager clients; the manager serialises the
/// two dispatches.
#[test]
fn concurrent_duplicate_split_yields_exactly_one_child() {
    let (mgr_addr, n1, n2, _d1, _d2) = setup_two_node_infra(340);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1, n2, 340).await;

        register_dummy_ps(&mgr, 1).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 0, log, row, meta, b"a", b"z").await;
        let part_id = get_regions(&mgr).await.regions[0].0;

        let payload = rkyv_encode(&MultiModifySplitReq {
            part_id,
            owner_key: String::new(),
            owner_epoch: 0,
            mid_key: b"m".to_vec(),
            log_stream_sealed_length: 0,
            row_stream_sealed_length: 0,
            meta_stream_sealed_length: 0,
        });

        // Two independent connections, both firing the identical split.
        let c1 = RpcClient::connect(mgr_addr).await.expect("c1");
        let c2 = RpcClient::connect(mgr_addr).await.expect("c2");
        let p1 = payload.clone();
        let p2 = payload.clone();
        let f1 = async move { c1.call(MSG_MULTI_MODIFY_SPLIT, p1).await };
        let f2 = async move { c2.call(MSG_MULTI_MODIFY_SPLIT, p2).await };
        let (r1, r2) = futures::future::join(f1, f2).await;
        let d1: CodeResp = rkyv_decode(&r1.expect("split1 ok")).expect("decode1");
        let d2: CodeResp = rkyv_decode(&r2.expect("split2 ok")).expect("decode2");
        // Exactly one commits; the other is refused (in-progress OR out-of-range).
        let oks = [&d1, &d2].iter().filter(|d| d.code == CODE_OK).count();
        assert_eq!(
            oks, 1,
            "exactly one of the duplicate splits may commit; got codes {} / {}",
            d1.code, d2.code
        );

        let regions_after = get_regions(&mgr).await.regions;
        assert_eq!(
            regions_after.len(),
            2,
            "concurrent duplicate split must yield EXACTLY 2 partitions; got {}",
            regions_after.len()
        );
    });
}

// ── (c) punch_holes / GC delete under lost reply ───────────────────────────

/// A punch_holes whose reply is lost removes the target extent ONCE; the retry
/// finds it already gone (`removed = requested ∩ members` is empty) and is an
/// idempotent no-op — the stream is NOT emptied further and no surviving extent
/// is touched.
#[test]
fn lost_reply_punch_holes_is_idempotent() {
    let (mgr_addr, n1, n2, _d1, _d2) = setup_two_node_infra(320);
    // Drop the first 2 punch replies → the effect + one idempotent no-op are
    // both re-delivered.
    let (front, stats) = spawn_lost_reply_proxy(mgr_addr, MSG_STREAM_PUNCH_HOLES, 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1, n2, 320).await;
        let stream_id = create_stream(&mgr, 2).await;

        // Grow the stream to [E0, E1, E2] via two direct seal-rolls.
        let (_, e0) = stream_extents(&mgr, stream_id).await;
        alloc_roll(&mgr, stream_id, e0, 0).await;
        let (_, e1) = stream_extents(&mgr, stream_id).await;
        alloc_roll(&mgr, stream_id, e1, 0).await;
        let (exts, _) = stream_extents(&mgr, stream_id).await;
        assert_eq!(exts.len(), 3, "stream must have 3 extents before punch: {exts:?}");
        let victim = exts[1]; // punch the MIDDLE member
        let survivor_a = exts[0];
        let survivor_b = exts[2];
        assert_eq!(victim, e1);

        let payload = rkyv_encode(&PunchHolesReq {
            stream_id,
            owner_key: String::new(),
            owner_epoch: 0,
            extent_ids: vec![victim],
        });
        let (resp_bytes, attempts) =
            call_with_lost_reply_retry(front, MSG_STREAM_PUNCH_HOLES, payload, 8).await;
        let resp: PunchHolesResp = rkyv_decode(&resp_bytes).expect("decode punch resp");
        assert_eq!(resp.code, CODE_OK, "punch must converge OK: {}", resp.message);
        assert!(attempts >= 2, "dropped reply must force a retry (attempts={attempts})");
        assert!(stats.dropped() >= 1, "at least one punch reply dropped");
        assert!(stats.target_reqs() >= 2, "manager received the punch >= twice");

        // ── invariant: exactly one removal, survivors untouched ──
        let (exts_after, _) = stream_extents(&mgr, stream_id).await;
        assert_eq!(
            exts_after.len(),
            2,
            "punch under lost-reply+retry removed exactly one extent: {exts_after:?}"
        );
        assert!(!exts_after.contains(&victim), "victim extent must be gone");
        assert!(
            exts_after.contains(&survivor_a) && exts_after.contains(&survivor_b),
            "the two non-target extents must survive: {exts_after:?}"
        );
    });
}
