//! Stream recvs posted after their data has already arrived must complete in
//! place, never through UCX's request callback.
//!
//! With `UCP_OP_ATTR_FLAG_NO_IMM_CMPL`, UCX completes such a recv inside
//! `ucp_stream_recv_nbx` by calling the callback with `req->status` — a field
//! release builds up to 1.20.1 never set for a stream recv (stream_recv.c,
//! assert-only), so a recycled request reported whatever its memory held
//! (`UCS_INPROGRESS` on the PS's 8 MiB receives); the request is also never
//! marked completed, so `ucp_request_free` leaks it. Without the flag UCX
//! unpacks queued data in place and returns the length, no request involved.
//! The transport counts both paths; this test drives the queued-data case and
//! fails if any recv went through the callback.
//!
//! Needs a RoCE address: `AUTUMN_UCX_TEST_BIND="[<roce-ip>]:0"`, plus the
//! usual `UCX_TLS` / `UCX_NET_DEVICES`.

#![cfg(feature = "ucx")]

use autumn_transport::{ucx_recv_counts, AutumnTransport, UcxTransport};
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::BufResult;
use std::net::SocketAddr;

const ROUNDS: usize = 200;
const MSG: usize = 4096;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

#[test]
fn queued_data_completes_in_place() {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (sent_tx, sent_rx) = std::sync::mpsc::channel::<()>();

    // The sender writes everything before the receiver posts its first recv,
    // and the receiver lets its progress loop queue all of it first, so every
    // recv finds its data already there. (Relying on the first recv's progress
    // to pull a backlog left how many were queued to timing: 0 of 200 in some
    // runs, every run once the pool's buffers came from one large chunk.)
    let receiver = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let (c, _) = listener.accept().await.unwrap();
            let (mut r, _w) = c.into_split();
            sent_rx.recv().unwrap();
            compio::time::sleep(std::time::Duration::from_millis(100)).await;
            let mut buf = vec![0u8; MSG];
            for round in 0..ROUNDS {
                let mut got = 0;
                while got < MSG {
                    let BufResult(n, b) = r.read(buf).await;
                    buf = b;
                    let n = n.unwrap_or_else(|e| panic!("round {round}: recv failed: {e}"));
                    assert!(n > 0, "round {round}: empty recv");
                    got += n;
                }
                assert!(buf.iter().all(|&v| v == (round & 0xff) as u8), "round {round}: bad bytes");
            }
            ucx_recv_counts()
        })
    });
    let addr = addr_rx.recv().unwrap();

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let c = UcxTransport.connect(addr).await.unwrap();
        let (_r, mut w) = c.into_split();
        for round in 0..ROUNDS {
            let BufResult(res, _) = w.write_all(vec![(round & 0xff) as u8; MSG]).await;
            res.unwrap();
        }
        sent_tx.send(()).unwrap();
    });
    let (in_place, via_callback) = receiver.join().unwrap();
    eprintln!("recvs completed in place: {in_place}, through the request callback: {via_callback}");
    assert_eq!(in_place as usize, ROUNDS, "every recv should have found its data already queued");
    assert_eq!(via_callback, 0, "a queued-data recv completed through UCX's request callback");
}
