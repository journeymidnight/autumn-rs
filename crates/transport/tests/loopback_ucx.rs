//! UCX loopback suite — same shape as `loopback_tcp.rs` but binds on a
//! RoCE-attached IPv6 so the data path actually goes through `rc_mlx5`.
//!
//! ## Status
//!
//! - `ping_pong`, `large_payload`, `many_concurrent_4`: pass individually
//!   (UCX 1.16 + `rc_mlx5/mlx5_0:1`, real RoCEv2 wire). Run with
//!   `--test-threads=1` to keep the per-thread worker contention bounded.
//! - `half_close`: `#[ignore]` — `ucp_stream` is a duplex byte pipe with
//!   no FIN/EOF concept, so a half-close test cannot pass.
//!
//! ### Production caveat — close-on-drop
//!
//! `UcxConn::Drop` does NOT call `ucp_ep_close_nbx` (see endpoint.rs);
//! UCX's close synchronously cancels in-flight ops on the peer's side
//! of the EP, which races with the peer's pending recv (TCP buffers
//! bytes in the kernel; UCX RC does not). For path 1/2 in autumn-rs
//! this is fine: connection pools (autumn-stream conn_pool, the
//! manager's RpcConn map) reuse one EP per peer address for the life of the
//! process. The full close path (with `ucp_worker_flush` + await) is
//! tracked in spec §10 as a P3+ follow-up.

#![cfg(feature = "ucx")]

mod common;

use autumn_transport::UcxTransport;
use std::net::SocketAddr;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect(
            "set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address \
             (e.g. from scripts/check_roce.sh --listen-candidates)",
        )
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

#[compio::test]
async fn ping_pong() {
    common::ping_pong_at(UcxTransport, bind_addr()).await;
}

#[compio::test]
async fn large_payload() {
    common::large_payload_at(UcxTransport, bind_addr()).await;
}

#[compio::test]
#[ignore = "ucp_stream has no FIN/EOF semantics"]
async fn half_close() {
    common::half_close_at(UcxTransport, bind_addr()).await;
}

#[compio::test]
async fn many_concurrent_4() {
    common::many_concurrent_at(UcxTransport, bind_addr(), 4).await;
}

/// Repro for transport_bench hang: many sequential ping-pongs over a single
/// connection should work, just like TCP. If this hangs, the wrapper has a
/// progress-starvation bug that doesn't surface in the single-roundtrip
/// `ping_pong` test.
#[compio::test]
async fn many_iter_pingpong() {
    use autumn_transport::AutumnTransport;
    use compio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use compio::BufResult;

    let t = UcxTransport;
    let mut listener = t.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    const SIZE: usize = 1024;
    const ITERS: usize = 50;

    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        let (mut r, mut w) = c.into_split();
        for _ in 0..ITERS {
            let buf = vec![0u8; SIZE];
            let BufResult(res, b) = r.read_exact(buf).await;
            res.unwrap();
            let BufResult(res, _) = w.write_all(b).await;
            res.unwrap();
        }
    });

    let c = t.connect(bound).await.unwrap();
    let (mut r, mut w) = c.into_split();
    for i in 0..ITERS {
        let payload = vec![(i & 0xff) as u8; SIZE];
        w.write_all(payload).await.0.unwrap();
        let BufResult(res, b) = r.read_exact(vec![0u8; SIZE]).await;
        res.unwrap();
        assert!(b.iter().all(|&v| v == (i & 0xff) as u8));
    }
    drop(w);
    drop(r);
    let _ = server.await;
}

/// Cancel-safety: drop a `read` future mid-await. With `InflightGuard`
/// in place this must not free the buffer while UCX still holds the
/// pointer (use-after-free). Pre-fix this would either UAF or segfault;
/// post-fix the cancel + sync progress drains the request before we
/// return, and the test completes cleanly.
#[compio::test]
async fn drop_read_mid_await_is_safe() {
    use autumn_transport::AutumnTransport;
    use compio::io::{AsyncRead as _, AsyncWriteExt as _};
    use std::time::Duration;

    let t = UcxTransport;
    let mut listener = t.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();

    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        // Sleep before sending so the client's read sits "pending" long
        // enough for us to drop it via timeout.
        compio::time::sleep(Duration::from_millis(200)).await;
        let (_r, mut w) = c.into_split();
        let _ = w.write_all(vec![0xa5u8; 64]).await;
    });

    let c = t.connect(bound).await.unwrap();
    let (mut r, _w) = c.into_split();

    // Race a 1ms timeout against a 64-byte read. The timeout wins; the
    // read future is dropped mid-await. Without InflightGuard this would
    // be UAF.
    let buf = vec![0u8; 64];
    let race = async {
        let _ = r.read(buf).await;
    };
    let _ = compio::time::timeout(Duration::from_millis(1), race).await;

    // Wait for the server to finish so we don't tear it down early.
    let _ = server.await;
}

/// Cancel-safety for `recv_into` a REGISTERED RegPool buffer — the foundation
/// every server-side recv-into-registered hop rests on. A recv posted into a
/// `ucp_mem_map`-registered buffer means the NIC will DMA into it asynchronously;
/// if the recv future is dropped mid-flight, the buffer must NOT be freed /
/// returned-to-pool / reused until UCX confirms it's done. `recv_into`'s
/// `InflightSlot` drains UCX (`ucp_request_cancel` + progress-to-completion) on
/// drop, and because the `PooledBuf` is owned across the recv (drop order:
/// recv guard before `PooledBuf`), the registered slab is safe to re-pool after.
/// This test drops a registered recv mid-await, then REUSES the pool for a
/// clean recv — a corrupted slab / still-in-flight DMA would crash or garble it.
#[compio::test]
async fn drop_recv_into_registered_mid_await_is_safe() {
    use autumn_transport::{regpool_acquire, AutumnTransport};
    use compio::io::AsyncWriteExt as _;
    use std::time::Duration;

    const LEN: usize = 256 * 1024; // big enough to be a real registered/rendezvous recv

    let t = UcxTransport;
    let mut listener = t.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();

    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        // Sleep so the first recv sits pending long enough to be cancelled,
        // then send twice (the cancelled recv + the clean reuse recv).
        compio::time::sleep(Duration::from_millis(200)).await;
        let (_r, mut w) = c.into_split();
        let _ = w.write_all(vec![0xa5u8; LEN]).await;
        let _ = w.write_all(vec![0x5au8; LEN]).await;
    });

    let c = t.connect(bound).await.unwrap();
    let (mut r, _w) = c.into_split();

    // (1) Registered recv dropped mid-await by a 1ms timeout.
    {
        let mut pb = regpool_acquire(LEN);
        assert!(
            pb.reg().is_some(),
            "buffer should be registered (under memlock cap)"
        );
        let race = async {
            let (dest, reg) = pb.dest_and_reg();
            let _ = r.recv_into(dest, reg).await;
        };
        let _ = compio::time::timeout(Duration::from_millis(1), race).await;
        // pb drops here → registered slab returns to the pool. Safe ONLY because
        // recv_into's InflightSlot drained UCX before `race` unwound.
    }

    // (2) Reuse the pool for a clean registered recv that completes. If (1) had
    // corrupted the slab or left the NIC mid-DMA into it, this corrupts/crashes.
    let mut pb2 = regpool_acquire(LEN);
    let (dest, reg) = pb2.dest_and_reg();
    let n = r.recv_into(dest, reg).await.expect("clean registered recv");
    assert!(n > 0, "reuse recv returned {n} bytes");

    let _ = server.await;
}

#[compio::test]
async fn write_vectored_one_shot() {
    common::write_vectored_one_shot_at(UcxTransport, bind_addr()).await;
}
