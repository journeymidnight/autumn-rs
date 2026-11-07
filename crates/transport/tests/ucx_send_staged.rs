//! A stream send leaves from a registered pool slab, so the caller's memory is
//! never handed to UCX's registration cache: a send out of `.rodata`, which
//! `ibv_reg_mr` refuses (Bad address), arrives. The peer runs on its own
//! thread so the connection takes the NIC; the `self` transport registers
//! nothing. Binds `AUTUMN_UCX_TEST_BIND`, like `loopback_ucx.rs`.

#![cfg(feature = "ucx")]

use autumn_transport::{AutumnTransport, TransportKind, UcxTransport};
use bytes::Bytes;
use compio::io::{AsyncReadExt, AsyncWriteExt};
use compio::BufResult;
use std::net::SocketAddr;
use std::time::Duration;

const LEN: usize = 8192;

/// Immutable and non-zero, so it lands in `.rodata`.
static RODATA: [u8; LEN] = {
    let mut a = [0u8; LEN];
    let mut i = 0;
    while i < LEN {
        a[i] = (i % 251) as u8 ^ 0x5a;
        i += 1;
    }
    a
};

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

#[test]
fn a_send_from_memory_that_cannot_be_registered_arrives() {
    autumn_transport::init_with(TransportKind::Ucx);
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let (c, _) = listener.accept().await.unwrap();
            let (mut r, _w) = c.into_split();
            let BufResult(res, got) = compio::time::timeout(Duration::from_secs(10), r.read_exact(vec![0u8; 3 * LEN]))
                .await
                .expect("the stream stopped after a send from .rodata");
            res.unwrap();
            got
        })
    });
    let addr = addr_rx.recv().unwrap();
    let conn = compio::runtime::Runtime::new().unwrap().block_on(async move {
        let c = UcxTransport.connect(addr).await.unwrap();
        let (r, mut w) = c.into_split();
        w.write_all(Bytes::from_static(&RODATA)).await.0.unwrap();
        w.write_vectored_all(vec![Bytes::from_static(&RODATA), Bytes::from(RODATA.to_vec())])
            .await
            .0
            .unwrap();
        (r, w)
    });
    let got = server.join().unwrap();
    for (i, chunk) in got.chunks(LEN).enumerate() {
        assert!(chunk == &RODATA[..], "send {i} arrived with other bytes");
    }
    drop(conn);
}
