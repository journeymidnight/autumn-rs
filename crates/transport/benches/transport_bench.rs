//! Transport-level perf micro-bench. Measures latency and throughput for
//! TCP and (under `--features ucx`) UCX, in the same process via loopback.
//! No cluster required — just `cargo bench -p autumn-transport [--features ucx]`.
//!
//! Two scenarios per transport:
//!   - small: 64 B ping-pong, 1000 iters → reports avg μs per round-trip
//!   - large: 1 MB unidirectional, 100 iters → reports MB/s and avg op time
//!
//! Bind:
//!   - TCP always uses 127.0.0.1:0
//!   - UCX uses AUTUMN_UCX_TEST_BIND env var, or `[fdbb:dc62:3:3::16]:0`
//!     (build host's RoCE-attached IPv6) by default. Set to `127.0.0.1:0`
//!     to force UCX-over-TCP fallback for comparison.

use autumn_transport::{AutumnTransport, TcpTransport};
use compio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use compio::BufResult;
use std::net::SocketAddr;
use std::time::Instant;

const SMALL_PAYLOAD: usize = 64;
const SMALL_ITERS: usize = 1000;
const LARGE_PAYLOAD: usize = 1024 * 1024;
const LARGE_ITERS: usize = 100;

async fn small_pingpong<T: AutumnTransport + Clone>(t: T, addr: SocketAddr) -> f64 {
    let mut listener = t.bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        let (mut r, mut w) = c.into_split();
        for _ in 0..SMALL_ITERS {
            let buf = vec![0u8; SMALL_PAYLOAD];
            let BufResult(res, b) = r.read_exact(buf).await;
            res.expect("server read");
            let BufResult(res, _) = w.write_all(b).await;
            res.expect("server write");
        }
    });

    let c = t.connect(bound).await.unwrap();
    let (mut r, mut w) = c.into_split();
    let payload = vec![0xa5u8; SMALL_PAYLOAD];

    let t0 = Instant::now();
    for _ in 0..SMALL_ITERS {
        let BufResult(res, _) = w.write_all(payload.clone()).await;
        res.expect("client write");
        let buf = vec![0u8; SMALL_PAYLOAD];
        let BufResult(res, _) = r.read_exact(buf).await;
        res.expect("client read");
    }
    let elapsed = t0.elapsed();
    let _ = server.await;

    elapsed.as_secs_f64() * 1e6 / SMALL_ITERS as f64
}

async fn large_throughput<T: AutumnTransport + Clone>(t: T, addr: SocketAddr) -> (f64, f64) {
    let mut listener = t.bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        let (mut r, _w) = c.into_split();
        for _ in 0..LARGE_ITERS {
            let buf = vec![0u8; LARGE_PAYLOAD];
            let BufResult(res, _) = r.read_exact(buf).await;
            res.expect("server read");
        }
    });

    let c = t.connect(bound).await.unwrap();
    let (_r, mut w) = c.into_split();
    let payload = vec![0xa5u8; LARGE_PAYLOAD];

    let t0 = Instant::now();
    for _ in 0..LARGE_ITERS {
        let BufResult(res, _) = w.write_all(payload.clone()).await;
        res.expect("client write");
    }
    let elapsed = t0.elapsed();
    let _ = server.await;

    let total_bytes = (LARGE_PAYLOAD * LARGE_ITERS) as f64;
    let mb_per_sec = total_bytes / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let avg_us = elapsed.as_secs_f64() * 1e6 / LARGE_ITERS as f64;
    (mb_per_sec, avg_us)
}

fn report(name: &str, ping_us: f64, mb_s: f64, large_us: f64) {
    println!(
        "{name:>20}: ping_pong {SMALL_PAYLOAD}B = {ping_us:>7.2} μs/op  |  \
         throughput {LARGE_PAYLOAD}B = {mb_s:>8.1} MB/s ({large_us:>7.0} μs/op)"
    );
}

#[compio::main]
async fn main() {
    let tcp_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    // TCP
    let p = small_pingpong(TcpTransport, tcp_addr).await;
    let (mb, ius) = large_throughput(TcpTransport, tcp_addr).await;
    report("TCP (loopback)", p, mb, ius);

    #[cfg(feature = "ucx")]
    {
        // UCX over RoCE IP (or whatever AUTUMN_UCX_TEST_BIND is set to).
        let ucx_addr_str = std::env::var("AUTUMN_UCX_TEST_BIND")
            .unwrap_or_else(|_| "[fdbb:dc62:3:3::16]:0".to_string());
        let ucx_addr: SocketAddr = ucx_addr_str.parse().expect("parse UCX bind");

        let p = small_pingpong(autumn_transport::UcxTransport, ucx_addr).await;
        let (mb, ius) = large_throughput(autumn_transport::UcxTransport, ucx_addr).await;
        report(
            &format!("UCX ({})", if ucx_addr.ip().is_loopback() { "tcp-fallback" } else { "rc_mlx5" }),
            p,
            mb,
            ius,
        );
    }
}
