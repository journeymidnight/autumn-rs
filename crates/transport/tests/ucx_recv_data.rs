//! `UcxReadHalf::recv_data_with`: queued stream data handed over chunk by
//! chunk, in order, with nothing posted to UCX — so a call dropped while it
//! waits leaves the connection readable (a dropped posted recv would take the
//! next bytes and fail every later read). Binds `AUTUMN_UCX_TEST_BIND`, like
//! `loopback_ucx.rs`.

#![cfg(feature = "ucx")]

use autumn_transport::{AutumnTransport, UcxTransport};
use compio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use std::net::SocketAddr;
use std::time::Duration;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(7)).collect()
}

#[compio::test]
async fn recv_data_with_hands_over_the_stream_in_order() {
    let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let n = 3 << 20;
    let data = pattern(n);
    let sent = data.clone();
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        let (_r, mut w) = c.into_split();
        let mut off = 0;
        for size in [1usize, 100, 64 << 10, 1 << 20].into_iter().cycle() {
            let end = (off + size).min(n);
            w.write_all(sent[off..end].to_vec()).await.0.unwrap();
            off = end;
            if off == n {
                break;
            }
        }
        compio::time::sleep(Duration::from_millis(200)).await;
    });
    let c = UcxTransport.connect(bound).await.unwrap();
    let (mut r, _w) = c.into_split();
    let mut got = Vec::with_capacity(n);
    while got.len() < n {
        r.recv_data_with(|chunk| got.extend_from_slice(chunk)).await.unwrap();
    }
    assert_eq!(got, data);
    server.await.expect("server task");
}

#[compio::test]
async fn a_dropped_recv_data_with_leaves_the_connection_readable() {
    let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        let (_r, mut w) = c.into_split();
        compio::time::sleep(Duration::from_millis(200)).await;
        w.write_all(pattern(64)).await.0.unwrap();
        compio::time::sleep(Duration::from_millis(200)).await;
    });
    let c = UcxTransport.connect(bound).await.unwrap();
    let (mut r, _w) = c.into_split();
    // Nothing has been sent yet: the call waits and is dropped.
    let waiting = r.recv_data_with(|_| panic!("no data was sent yet"));
    assert!(compio::time::timeout(Duration::from_millis(50), waiting).await.is_err());
    // The bytes sent afterwards are all there for an ordinary read.
    let compio::BufResult(res, buf) = r.read_exact(vec![0u8; 64]).await;
    res.expect("read after the dropped call");
    assert_eq!(buf, pattern(64));
    server.await.expect("server task");
}
