//! Transport-level perf bench, in the same shape as `ucx_perftest`. Sweeps
//! a ladder of message sizes for three scenarios:
//!
//!   * **rtt**       — single connection, send-N / wait-N ping-pong.
//!                     Reports msg/s + p50/p99 latency.
//!   * **bw1**       — single connection, streaming sends, server drains.
//!                     Reports msg/s + MB/s on one socket/EP.
//!   * **bwK**       — K parallel connections, each streaming. Reports
//!                     aggregate msg/s + MB/s. Models the autumn-rpc
//!                     `ConnPool` fan-out pattern.
//!
//! Runs both TCP and UCX (when built with `--features ucx`) so the two
//! transports can be compared on the same machine, same NIC binding.
//!
//! ## Run
//!
//! TCP only (no NIC binding needed):
//! ```
//! cargo bench -p autumn-transport --bench transport_bench
//! ```
//!
//! TCP + UCX over a RoCE-attached IP (set bind to a routable RoCE GID):
//! ```
//! AUTUMN_UCX_TEST_BIND="[fdbd:dc62:3:302::14]:0" \
//!     cargo bench -p autumn-transport --features ucx --bench transport_bench
//! ```
//!
//! Force a specific UCX transport (e.g. RDMA only, no shm/self):
//! ```
//! UCX_TLS=rc_mlx5,self UCX_NET_DEVICES=mlx5_1:1 \
//! AUTUMN_UCX_TEST_BIND="[fdbd:dc62:3:302::14]:0" \
//!     cargo bench -p autumn-transport --features ucx --bench transport_bench
//! ```
//!
//! ## Tunables (env vars; sensible defaults)
//!
//!   * `AUTUMN_PERF_SIZES`     — comma-separated bytes (default
//!                               `8,64,1024,4096,65536,1048576,4194304`)
//!   * `AUTUMN_PERF_DURATION`  — seconds per cell (default `3`)
//!   * `AUTUMN_PERF_CONNS`     — fan-out for the bwK scenario (default `8`)
//!   * `AUTUMN_PERF_RTT_ITERS_CAP` — cap on RTT iters per cell so the small
//!                                   sizes don't run forever (default
//!                                   `200000`)
//!   * `AUTUMN_UCX_TEST_BIND`  — UCX bind address (required if `ucx`
//!                               feature is on)

use autumn_transport::{AutumnTransport, TcpTransport};
use bytes::Bytes;
use compio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use compio::BufResult;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

fn parse_sizes() -> Vec<usize> {
    std::env::var("AUTUMN_PERF_SIZES")
        .ok()
        .as_deref()
        .unwrap_or("8,64,1024,4096,65536,1048576,4194304")
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .collect()
}

fn parse_duration() -> Duration {
    let s = std::env::var("AUTUMN_PERF_DURATION")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(3.0);
    Duration::from_secs_f64(s.max(0.1))
}

fn parse_conns() -> usize {
    std::env::var("AUTUMN_PERF_CONNS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(8)
}

fn parse_rtt_iters_cap() -> u64 {
    std::env::var("AUTUMN_PERF_RTT_ITERS_CAP")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(200_000)
}

#[derive(Clone, Copy)]
struct RttResult {
    iters: u64,
    elapsed: Duration,
    p50_us: f64,
    p99_us: f64,
}

#[derive(Clone, Copy)]
struct BwResult {
    msgs: u64,
    elapsed: Duration,
    size: usize,
}

impl BwResult {
    fn msg_per_s(&self) -> f64 {
        self.msgs as f64 / self.elapsed.as_secs_f64()
    }
    fn mb_per_s(&self) -> f64 {
        (self.msgs as f64 * self.size as f64)
            / self.elapsed.as_secs_f64()
            / (1024.0 * 1024.0)
    }
}

async fn rtt_one<T: AutumnTransport + Clone>(
    t: T,
    addr: SocketAddr,
    size: usize,
    duration: Duration,
    iters_cap: u64,
) -> RttResult {
    let mut listener = t.bind(addr).await.expect("bind");
    let bound = listener.local_addr().expect("local_addr");

    // Two-phase RTT (matches bw_one): client tells server the warm + timed
    // iter counts. Server echoes exactly that many. UCX stream has no
    // FIN/EOF, so an explicit count is the cleanest stop signal.
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.expect("accept");
        let (mut r, mut w) = c.into_split();
        for _phase in 0..2 {
            let BufResult(res, b) = r.read_exact(vec![0u8; 8]).await;
            if res.is_err() { return; }
            let n: u64 = u64::from_le_bytes(b[..8].try_into().unwrap());
            for _ in 0..n {
                let buf = vec![0u8; size];
                let BufResult(res, b) = r.read_exact(buf).await;
                if res.is_err() { return; }
                let BufResult(res, _) = w.write_all(b).await;
                if res.is_err() { return; }
            }
        }
    });

    let c = t.connect(bound).await.expect("connect");
    let (mut r, mut w) = c.into_split();

    // Phase 1 — warmup (drain wireup, estimate per-msg time).
    // Send-side payload is `Bytes` (O(1) clone). Recv side has to allocate
    // a fresh Vec each iter because read_exact needs IoBufMut and Bytes is
    // immutable; for RTT this is unavoidable on the receive path.
    let payload: Bytes = Bytes::from(vec![0xa5u8; size]);
    let warm_iters: u64 = 64;
    w.write_all(warm_iters.to_le_bytes().to_vec()).await.0.expect("warm count");
    let warm_t0 = Instant::now();
    for _ in 0..warm_iters {
        w.write_all(payload.clone()).await.0.expect("warm write");
        r.read_exact(vec![0u8; size]).await.0.expect("warm read");
    }
    let warm_elapsed = warm_t0.elapsed();
    let per_msg = warm_elapsed.as_secs_f64() / warm_iters as f64;
    let timed_iters: u64 = ((duration.as_secs_f64() / per_msg).max(64.0) as u64).min(iters_cap);

    // Phase 2 — timed.
    w.write_all(timed_iters.to_le_bytes().to_vec()).await.0.expect("timed count");
    let mut samples: Vec<u64> = Vec::with_capacity(timed_iters as usize);
    let global_t0 = Instant::now();
    for _ in 0..timed_iters {
        let t0 = Instant::now();
        w.write_all(payload.clone()).await.0.expect("rtt write");
        r.read_exact(vec![0u8; size]).await.0.expect("rtt read");
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    let elapsed = global_t0.elapsed();
    drop(w);
    drop(r);
    let _ = server.await;

    samples.sort_unstable();
    let p = |q: f64| -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        let idx = ((samples.len() as f64) * q).floor() as usize;
        let idx = idx.min(samples.len() - 1);
        samples[idx] as f64 / 1000.0 // ns → μs
    };
    RttResult {
        iters: samples.len() as u64,
        elapsed,
        p50_us: p(0.50),
        p99_us: p(0.99),
    }
}

async fn bw_one<T: AutumnTransport + Clone>(
    t: T,
    addr: SocketAddr,
    size: usize,
    duration: Duration,
) -> BwResult {
    let mut listener = t.bind(addr).await.expect("bind");
    let bound = listener.local_addr().expect("local_addr");

    // Two-phase protocol: warmup is iter-counted (small), then the client
    // tells the server the timed iter count. Server reads exactly that
    // many, so no FIN/EOF assumption.
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.expect("accept");
        let (mut r, _w) = c.into_split();
        // Warmup count.
        let BufResult(res, b) = r.read_exact(vec![0u8; 8]).await;
        if res.is_err() {
            return;
        }
        let warm: u64 = u64::from_le_bytes(b[..8].try_into().unwrap());
        for _ in 0..warm {
            let BufResult(res, _) = r.read_exact(vec![0u8; size]).await;
            if res.is_err() {
                return;
            }
        }
        // Timed count.
        let BufResult(res, b) = r.read_exact(vec![0u8; 8]).await;
        if res.is_err() {
            return;
        }
        let timed: u64 = u64::from_le_bytes(b[..8].try_into().unwrap());
        for _ in 0..timed {
            let BufResult(res, _) = r.read_exact(vec![0u8; size]).await;
            if res.is_err() {
                return;
            }
        }
    });

    let c = t.connect(bound).await.expect("connect");
    let (_r, mut w) = c.into_split();

    // `Bytes::clone()` is an Arc refcount bump, not a memcpy — the per-iter
    // overhead on the bench side is now O(1) regardless of `size`, so the
    // single-conn `bw1` numbers reflect actual wrapper + UCX cost rather
    // than the bench's allocator. compio supports IoBuf for `bytes::Bytes`
    // under the `bytes` feature.
    let payload: Bytes = Bytes::from(vec![0xa5u8; size]);

    // Phase 1 — warmup.
    let warm_iters: u64 = 64;
    w.write_all(warm_iters.to_le_bytes().to_vec()).await.0.expect("send warm count");
    let warm_t0 = Instant::now();
    for _ in 0..warm_iters {
        w.write_all(payload.clone()).await.0.expect("warm write");
    }
    let warm_elapsed = warm_t0.elapsed();
    let per_msg = warm_elapsed.as_secs_f64() / warm_iters as f64;
    let timed_iters: u64 = ((duration.as_secs_f64() / per_msg).max(64.0) as u64).min(50_000_000);

    // Phase 2 — timed.
    w.write_all(timed_iters.to_le_bytes().to_vec()).await.0.expect("send timed count");
    let t0 = Instant::now();
    for _ in 0..timed_iters {
        w.write_all(payload.clone()).await.0.expect("timed write");
    }
    let elapsed = t0.elapsed();
    drop(w);
    drop(_r);
    let _ = server.await;

    BwResult { msgs: timed_iters, elapsed, size }
}

async fn bw_k<T: AutumnTransport + Clone + 'static>(
    t: T,
    addr: SocketAddr,
    size: usize,
    duration: Duration,
    conns: usize,
) -> BwResult {
    let mut listener = t.bind(addr).await.expect("bind");
    let bound = listener.local_addr().expect("local_addr");

    // Server: per-connection task uses the same warm/timed count protocol
    // as bw_one. UCX has no FIN/EOF; explicit counts are required.
    let server = compio::runtime::spawn(async move {
        let mut handles = Vec::with_capacity(conns);
        for _ in 0..conns {
            let (c, _) = listener.accept().await.expect("accept");
            handles.push(compio::runtime::spawn(async move {
                let (mut r, _w) = c.into_split();
                let BufResult(res, b) = r.read_exact(vec![0u8; 8]).await;
                if res.is_err() { return; }
                let warm: u64 = u64::from_le_bytes(b[..8].try_into().unwrap());
                for _ in 0..warm {
                    if r.read_exact(vec![0u8; size]).await.0.is_err() { return; }
                }
                let BufResult(res, b) = r.read_exact(vec![0u8; 8]).await;
                if res.is_err() { return; }
                let timed: u64 = u64::from_le_bytes(b[..8].try_into().unwrap());
                for _ in 0..timed {
                    if r.read_exact(vec![0u8; size]).await.0.is_err() { return; }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    });

    // Clients: K parallel sender tasks, each running warm + timed phases.
    // The bench measures aggregate timed-phase msgs over the longest
    // timed elapsed (proxy for wall time when all conns finish).
    let mut handles = Vec::with_capacity(conns);
    for _ in 0..conns {
        let t2 = t.clone();
        let dur = duration;
        handles.push(compio::runtime::spawn(async move {
            let c = t2.connect(bound).await.expect("connect");
            let (_r, mut w) = c.into_split();
            let payload: Bytes = Bytes::from(vec![0xa5u8; size]);

            let warm_iters: u64 = 64;
            w.write_all(warm_iters.to_le_bytes().to_vec()).await.0.expect("warm count");
            let warm_t0 = Instant::now();
            for _ in 0..warm_iters {
                w.write_all(payload.clone()).await.0.expect("warm write");
            }
            let warm_elapsed = warm_t0.elapsed();
            let per_msg = warm_elapsed.as_secs_f64() / warm_iters as f64;
            let timed_iters: u64 = ((dur.as_secs_f64() / per_msg).max(64.0) as u64).min(50_000_000);

            w.write_all(timed_iters.to_le_bytes().to_vec()).await.0.expect("timed count");
            let t0 = Instant::now();
            for _ in 0..timed_iters {
                w.write_all(payload.clone()).await.0.expect("timed write");
            }
            let elapsed = t0.elapsed();
            drop(w);
            drop(_r);
            (timed_iters, elapsed)
        }));
    }

    let mut total_msgs = 0u64;
    let mut max_elapsed = Duration::ZERO;
    for h in handles {
        if let Ok((n, e)) = h.await {
            total_msgs += n;
            if e > max_elapsed { max_elapsed = e; }
        }
    }
    let _ = server.await;

    BwResult { msgs: total_msgs, elapsed: max_elapsed, size }
}

fn fmt_size(b: usize) -> String {
    if b >= 1024 * 1024 {
        format!("{}M", b / (1024 * 1024))
    } else if b >= 1024 {
        format!("{}K", b / 1024)
    } else {
        format!("{}B", b)
    }
}

fn fmt_msg_rate(r: f64) -> String {
    if r >= 1e6 {
        format!("{:>7.2}M", r / 1e6)
    } else if r >= 1e3 {
        format!("{:>7.2}K", r / 1e3)
    } else {
        format!("{:>8.0}", r)
    }
}

async fn run_transport<T: AutumnTransport + Clone + 'static>(
    label: &str,
    t: T,
    addr: SocketAddr,
    sizes: &[usize],
    duration: Duration,
    conns: usize,
    rtt_iters_cap: u64,
) {
    println!();
    println!("{label}");
    println!("{:-<width$}", "", width = label.len());
    println!(
        "{:>10} | {:>9} {:>11} {:>11} | {:>9} {:>11} | {:>9} {:>11}",
        "size", "rtt msg/s", "p50 (us)", "p99 (us)",
        "bw1 msg/s", "bw1 MB/s",
        "bwK msg/s", "bwK MB/s"
    );
    for &s in sizes {
        let rtt = rtt_one(t.clone(), addr, s, duration, rtt_iters_cap).await;
        let bw1 = bw_one(t.clone(), addr, s, duration).await;
        let bwk = bw_k(t.clone(), addr, s, duration, conns).await;
        let rtt_rate = rtt.iters as f64 / rtt.elapsed.as_secs_f64();
        println!(
            "{:>10} | {} {:>11.2} {:>11.2} | {} {:>11.1} | {} {:>11.1}",
            fmt_size(s),
            fmt_msg_rate(rtt_rate), rtt.p50_us, rtt.p99_us,
            fmt_msg_rate(bw1.msg_per_s()), bw1.mb_per_s(),
            fmt_msg_rate(bwk.msg_per_s()), bwk.mb_per_s(),
        );
    }
}

#[compio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let sizes = parse_sizes();
    let duration = parse_duration();
    let conns = parse_conns();
    let rtt_iters_cap = parse_rtt_iters_cap();

    println!(
        "transport_bench: sizes={:?} duration={}s conns={} rtt_iters_cap={}",
        sizes.iter().map(|s| fmt_size(*s)).collect::<Vec<_>>(),
        duration.as_secs_f64(),
        conns,
        rtt_iters_cap,
    );

    let tcp_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    run_transport(
        "TCP (loopback)",
        TcpTransport,
        tcp_addr,
        &sizes,
        duration,
        conns,
        rtt_iters_cap,
    )
    .await;

    #[cfg(feature = "ucx")]
    {
        let ucx_addr_str = std::env::var("AUTUMN_UCX_TEST_BIND")
            .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" for the UCX run");
        let ucx_addr: SocketAddr = ucx_addr_str.parse().expect("parse UCX bind");
        let label = format!(
            "UCX (bind={}, UCX_TLS={}, UCX_NET_DEVICES={})",
            ucx_addr,
            std::env::var("UCX_TLS").unwrap_or_else(|_| "<unset>".into()),
            std::env::var("UCX_NET_DEVICES").unwrap_or_else(|_| "<unset>".into()),
        );
        run_transport(
            &label,
            autumn_transport::UcxTransport,
            ucx_addr,
            &sizes,
            duration,
            conns,
            rtt_iters_cap,
        )
        .await;
    }
}
