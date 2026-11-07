//! The lanes UCX gives a connection between two workers under the documented
//! `UCX_TLS` / `UCX_NET_DEVICES`: the CM lane and rc_mlx5, nothing else. With
//! `ud_mlx5` listed (or a netdev beside the RDMA port, for tcp) every PEER-mode
//! endpoint also gets a UD (tcp) lane for its keepalive, and every worker
//! progress call polls that interface ("Peer failure" in CLAUDE.md).
//!
//! Needs a RoCE address: `AUTUMN_UCX_TEST_BIND="[<roce-ip>]:0"`, plus the
//! documented `UCX_TLS` / `UCX_NET_DEVICES`.

#![cfg(feature = "ucx")]

use autumn_transport::{AutumnTransport, TransportKind, UcxTransport};
use compio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

#[compio::test]
async fn every_lane_but_the_cm_runs_on_rc() {
    autumn_transport::init_with(TransportKind::Ucx);
    let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    // Dialed from another thread, so another worker: a connection within one
    // worker runs on `self`.
    let (tx, rx) = std::sync::mpsc::channel();
    let dialer = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let c = UcxTransport.connect(bound).await.unwrap();
            let (mut r, w) = c.into_split();
            tx.send(w.ucx_transports().unwrap().unwrap()).unwrap();
            // Held open until the other end has queried its side.
            r.read_exact(vec![0u8; 1]).await.0.unwrap();
        })
    });
    let (c, _) = listener.accept().await.unwrap();
    let (_r, mut w) = c.into_split();
    let accepted = w.ucx_transports().unwrap().unwrap();
    let dialed = rx.recv().unwrap();
    w.write_all(vec![1u8]).await.0.unwrap();
    dialer.join().unwrap();

    println!("accepted: {accepted:?}\ndialed: {dialed:?}");
    for (side, lanes) in [("accepted", &accepted), ("dialed", &dialed)] {
        let data: Vec<_> = lanes.iter().filter(|t| t.as_str() != "rdmacm").collect();
        assert!(!data.is_empty(), "{side}: no lanes besides the CM");
        assert!(
            data.iter().all(|t| t.as_str() == "rc_mlx5"),
            "{side} connection has a lane not on rc_mlx5: {lanes:?}"
        );
    }
}
