//! Shared loopback test helpers — used by `loopback_tcp.rs` (Phase 1) and
//! `loopback_ucx.rs` (Phase 3). Each helper takes the bind address so UCX can
//! pass a RoCE-attached IPv6 instead of `127.0.0.1`.

use autumn_transport::{AutumnTransport, Conn};
use compio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use compio::BufResult;
use std::net::SocketAddr;

pub async fn ping_pong_at<T: AutumnTransport + Clone>(t: T, addr: SocketAddr) {
    let mut listener = t.bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        echo_n(c, 1024).await;
    });
    let c = t.connect(bound).await.unwrap();
    write_then_read(c, 1024).await;
    let _ = server.await;
}

pub async fn large_payload_at<T: AutumnTransport + Clone>(t: T, addr: SocketAddr) {
    const N: usize = 2 * 1024 * 1024;
    let mut listener = t.bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        echo_n(c, N).await;
    });
    let c = t.connect(bound).await.unwrap();
    write_then_read(c, N).await;
    let _ = server.await;
}

pub async fn many_concurrent_at<T: AutumnTransport + Clone>(
    t: T,
    addr: SocketAddr,
    n: usize,
) {
    let mut listener = t.bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move {
        for _ in 0..n {
            let (c, _) = listener.accept().await.unwrap();
            compio::runtime::spawn(async move {
                echo_n(c, 64).await;
            })
            .detach();
        }
    });
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let t2 = t.clone();
        handles.push(compio::runtime::spawn(async move {
            let c = t2.connect(bound).await.unwrap();
            write_then_read(c, 64).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let _ = server.await;
}

pub async fn half_close_at<T: AutumnTransport + Clone>(t: T, addr: SocketAddr) {
    let mut listener = t.bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        let (mut r, mut w) = c.into_split();
        let buf = vec![0u8; 5];
        let BufResult(n, b) = r.read(buf).await;
        assert_eq!(n.unwrap(), 5);
        let BufResult(n, _) = w.write(b.to_vec()).await;
        assert_eq!(n.unwrap(), 5);
        // Expect EOF after the client shuts down its write half.
        let BufResult(n, _) = r.read(vec![0u8; 1]).await;
        assert_eq!(n.unwrap(), 0);
    });
    let c = t.connect(bound).await.unwrap();
    let (mut r, mut w) = c.into_split();
    let BufResult(n, _) = w.write(b"hello".to_vec()).await;
    assert_eq!(n.unwrap(), 5);
    w.shutdown().await.unwrap();
    let buf = vec![0u8; 5];
    let BufResult(n, _) = r.read(buf).await;
    assert_eq!(n.unwrap(), 5);
    let _ = server.await;
}

// ---------------- internals ----------------

async fn echo_n(c: Conn, n: usize) {
    let (mut r, mut w) = c.into_split();
    let buf = vec![0u8; n];
    let BufResult(res, b) = r.read_exact(buf).await;
    res.expect("read_exact");
    let BufResult(res, _) = w.write_all(b).await;
    res.expect("write_all");
}

async fn write_then_read(c: Conn, n: usize) {
    let (mut r, mut w) = c.into_split();
    let payload = vec![0xa5u8; n];
    let BufResult(res, _) = w.write_all(payload).await;
    res.expect("write_all");
    let buf = vec![0u8; n];
    let BufResult(res, b) = r.read_exact(buf).await;
    res.expect("read_exact");
    assert!(b.iter().all(|&v| v == 0xa5));
}
