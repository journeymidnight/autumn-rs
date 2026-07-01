#![allow(
    dead_code,
    unused_imports,
    unused_must_use,
    clippy::while_let_loop,
    clippy::needless_range_loop
)] // bench/throwaway perf code
//! PS<-EN read-path microbench — isolates whether ONE RpcClient's `read_loop`
//! serializes N concurrent 8 MiB `call_into_pooled` recvs (the PS uses ONE
//! connection per partition->EN) versus N RpcClients (N read_loops).
//!
//! The CLIENT (the part that mimics the PS read side) is pinned to ONE core;
//! the stub-EN server runs on all the OTHER cores, so it is never the
//! bottleneck. The server emits the exact EN MSG_READ_BYTES_ZC response shape:
//! `[V0 header][zc_meta: code+value_len+crc32c][value]`.
//!
//! Run (UCX):
//!   ulimit -l unlimited
//!   AUTUMN_UCX_TEST_BIND="[<roce-ip>]:0" UCX_TLS="^sysv,posix" \
//!     UCX_NET_DEVICES="mlx5_1:1" AUTUMN_PSREAD_SIZE=8388608 \
//!     AUTUMN_PSREAD_CONNS=8 AUTUMN_PSREAD_DURATION=5 AUTUMN_PSREAD_CORE=3 \
//!     cargo bench -p autumn-rpc --features ucx --bench ps_read_bench

use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};

use autumn_rpc::client::{encode_zc_meta, RpcClient};
use autumn_rpc::frame::FLAG_RESPONSE;
use autumn_rpc::HEADER_LEN;
use autumn_transport::{AutumnTransport, TransportKind};

const MSG_READ_BYTES_ZC: u8 = 15;
const CODE_OK: u8 = 0;

fn env(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(d)
}

fn pin_to(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}
fn pin_avoid(core: usize, ncpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        for c in 0..ncpu {
            if c != core {
                libc::CPU_SET(c, &mut set);
            }
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// `[V0 header][zc_meta]` — exactly what the EN's `zc_read_head` emits.
fn zc_head(req_id: u32, value: &[u8]) -> Bytes {
    let meta = encode_zc_meta(CODE_OK, value);
    let payload_len = meta.len() + value.len();
    let mut h = BytesMut::with_capacity(HEADER_LEN + meta.len());
    h.put_u32_le(req_id);
    h.put_u8(MSG_READ_BYTES_ZC);
    h.put_u8(FLAG_RESPONSE); // V0 (value crc rides in the meta)
    h.put_u32_le(payload_len as u32);
    h.put_slice(&meta);
    h.freeze()
}

async fn serve_conn(conn: autumn_transport::Conn, value: Bytes) {
    use compio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut r, mut w) = conn.into_split();
    loop {
        // Read the 10-byte request header; req_id at [0..4], wire payload_len at
        // [6..10] (V1 includes the +4 CRC trailer — we just drain it).
        let compio::BufResult(res, hdr) = r.read_exact(vec![0u8; HEADER_LEN]).await;
        if res.is_err() {
            return;
        }
        let req_id = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let msg_type = hdr[4];
        let payload_len = u32::from_le_bytes(hdr[6..10].try_into().unwrap()) as usize;
        if payload_len > 0 {
            let compio::BufResult(res, _p) = r.read_exact(vec![0u8; payload_len]).await;
            if res.is_err() {
                return;
            }
        }
        if msg_type == MSG_READ_BYTES_ZC {
            // ZC: [zc_head][value] (value-separable, EN's MSG_READ_BYTES_ZC).
            if w.write_all(zc_head(req_id, &value)).await.0.is_err() {
                return;
            }
            if w.write_all(value.clone()).await.0.is_err() {
                return;
            }
        } else {
            // Non-ZC: regular framed response, value as the payload (the client's
            // `call` recvs it into the decode buffer — no recv-into-registered).
            let resp = autumn_rpc::frame::Frame::response(req_id, msg_type, value.clone()).encode();
            if w.write_all(resp).await.0.is_err() {
                return;
            }
        }
    }
}

/// Bench driver: spawn `conns` request loops built by `mk_task` (each gets the
/// shared byte counter + deadline), await them all, return MB/s.
async fn drive<F, Fut>(conns: usize, dur: Duration, mk_task: F) -> f64
where
    F: Fn(Rc<Cell<u64>>, Instant) -> Fut,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let bytes = Rc::new(Cell::new(0u64));
    let deadline = Instant::now() + dur;
    let mut tasks = Vec::new();
    for _ in 0..conns {
        tasks.push(compio::runtime::spawn(mk_task(bytes.clone(), deadline)));
    }
    let t0 = Instant::now();
    for t in tasks {
        t.await;
    }
    bytes.get() as f64 / t0.elapsed().as_secs_f64() / 1e6
}

/// Loop ZC reads (`call_into_pooled`, recv-into-registered) until the deadline.
async fn zc_read_loop(c: Rc<RpcClient>, b: Rc<Cell<u64>>, deadline: Instant) {
    while Instant::now() < deadline {
        match c
            .call_into_pooled(MSG_READ_BYTES_ZC, Bytes::from_static(b"r"))
            .await
        {
            Ok((pb, _)) => b.set(b.get() + pb.len() as u64),
            Err(_) => break,
        }
    }
}

/// Loop plain reads (regular `call`, recv into the decode buffer) until the
/// deadline. Mirrors the pre-F216-E read path.
async fn plain_read_loop(c: Rc<RpcClient>, b: Rc<Cell<u64>>, deadline: Instant) {
    // msg_type 2 ≠ MSG_READ_BYTES_ZC → server replies with a regular framed
    // response.
    const MSG_GET: u8 = 2;
    while Instant::now() < deadline {
        match c.call(MSG_GET, Bytes::from_static(b"r")).await {
            Ok(resp) => b.set(b.get() + resp.len() as u64),
            Err(_) => break,
        }
    }
}

/// One RpcClient, `conns` concurrent call_into_pooled loops (PS-style: a single
/// connection / read_loop multiplexing N in-flight reads).
async fn client_shared(addr: SocketAddr, conns: usize, dur: Duration) -> f64 {
    let client = RpcClient::connect(addr).await.expect("connect");
    drive(conns, dur, |b, deadline| {
        zc_read_loop(client.clone(), b, deadline)
    })
    .await
}

/// `conns` RpcClients (N connections / N read_loops), one read loop each.
async fn client_perconn(addr: SocketAddr, conns: usize, dur: Duration) -> f64 {
    drive(conns, dur, |b, deadline| async move {
        let c = RpcClient::connect(addr).await.expect("connect");
        zc_read_loop(c, b, deadline).await;
    })
    .await
}

/// Non-ZC: one RpcClient, `conns` concurrent regular `call`s — the response
/// value is recv'd into the FrameDecoder buffer (a copy off the wire) and
/// returned as a `Bytes` slice. No recv-into-registered, no memh.
async fn client_nozc(addr: SocketAddr, conns: usize, dur: Duration) -> f64 {
    let client = RpcClient::connect(addr).await.expect("connect");
    drive(conns, dur, |b, deadline| {
        plain_read_loop(client.clone(), b, deadline)
    })
    .await
}

fn main() {
    let size = env("AUTUMN_PSREAD_SIZE", 8 * 1024 * 1024) as usize;
    let conns = env("AUTUMN_PSREAD_CONNS", 8) as usize;
    let dur = Duration::from_secs(env("AUTUMN_PSREAD_DURATION", 5));
    let core = env("AUTUMN_PSREAD_CORE", 3) as usize;
    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    let transport = match std::env::var("AUTUMN_TRANSPORT").as_deref() {
        Ok("ucx") => TransportKind::Ucx,
        _ => TransportKind::Tcp,
    };
    autumn_transport::init_with(transport);
    // F219: the ZC value crc on the hot path was removed entirely (no toggle).
    // AUTUMN_PSREAD_NOCRC is now a no-op kept for script back-compat.
    let _ = std::env::var("AUTUMN_PSREAD_NOCRC").is_ok();

    let bind: SocketAddr = std::env::var("AUTUMN_UCX_TEST_BIND")
        .unwrap_or_else(|_| "127.0.0.1:0".into())
        .parse()
        .expect("bind addr");

    let value = Bytes::from(vec![0xa5u8; size]);
    println!(
        "ps_read_bench: transport={transport:?} size={size} conns={conns} dur={}s client_core={core} ncpu={ncpu}",
        dur.as_secs()
    );

    // ---- Server thread: all cores EXCEPT the client core ----
    let (tx, rx) = std::sync::mpsc::channel::<SocketAddr>();
    let sval = value.clone();
    std::thread::spawn(move || {
        pin_avoid(core, ncpu);
        let rt = compio::runtime::Runtime::new().expect("server rt");
        rt.block_on(async move {
            let mut listener = autumn_transport::current().bind(bind).await.expect("bind");
            tx.send(listener.local_addr().expect("local_addr")).unwrap();
            loop {
                match listener.accept().await {
                    Ok((conn, _)) => {
                        let v = sval.clone();
                        compio::runtime::spawn(serve_conn(conn, v)).detach();
                    }
                    Err(_) => break,
                }
            }
        });
    });
    let addr = rx.recv().expect("server addr");

    // ---- Client: pinned to ONE core ----
    pin_to(core);
    let rt = compio::runtime::Runtime::new().expect("client rt");
    rt.block_on(async move {
        // warm
        let _ = client_perconn(addr, 1, Duration::from_secs(1)).await;
        let shared = client_shared(addr, conns, dur).await;
        let nozc = client_nozc(addr, conns, dur).await;
        let perconn = client_perconn(addr, conns, dur).await;
        let one = client_perconn(addr, 1, dur).await;
        println!("  1 RpcClient  (1 conn)              : {one:8.1} MB/s");
        println!("  1 RpcClient, {conns} concurrent ZC    : {shared:8.1} MB/s  (PS-style: call_into_pooled, recv-into-registered)");
        println!("  1 RpcClient, {conns} concurrent NON-ZC: {nozc:8.1} MB/s  (regular call, recv into decode buffer)");
        println!("  {conns} RpcClients (N read_loops)      : {perconn:8.1} MB/s");
    });
}
