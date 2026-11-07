//! A value under `BULK_MIN_BYTES` goes eager even where UCX's own threshold
//! (4 KiB here) would send it by rendezvous. The other side of the cut is
//! `ucx_am.rs`. Binds `AUTUMN_UCX_TEST_BIND`.

#![cfg(feature = "ucx")]

use autumn_transport::am::{self, Arrival};
use autumn_transport::{AutumnTransport, RecvDest, TransportKind, UcxTransport};
use bytes::Bytes;
use std::net::SocketAddr;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

#[compio::test]
async fn a_value_under_the_bulk_cut_goes_eager() {
    // Before the first UCX use in this process, which reads it.
    unsafe { std::env::set_var("UCX_RNDV_THRESH", "4k") };
    autumn_transport::init_with(TransportKind::Ucx);
    let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap().0 });
    let _client = UcxTransport.connect(bound).await.unwrap();
    let (_r, w) = accept.await.unwrap().into_split();
    let tx = w.am_sender().expect("a UCX connection");
    for n in [60usize << 10, autumn_transport::BULK_MIN_BYTES] {
        let value: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        let mut mem = vec![0u8; n];
        let tag = am::next_tag();
        let (dest, _released) = unsafe { RecvDest::new(mem.as_mut_ptr(), n) };
        let expected = am::expect(tag, dest, None).unwrap();
        let before = autumn_transport::ucx_am_received_counts();
        let (sent, got) = futures::join!(tx.send(tag, Bytes::from(value.clone())), expected.arrived());
        sent.unwrap();
        assert!(matches!(got.unwrap(), Arrival::Received(m) if m == n));
        expected.finish().await;
        assert_eq!(mem, value);
        let after = autumn_transport::ucx_am_received_counts();
        assert_eq!(after.1 > before.1, n >= autumn_transport::BULK_MIN_BYTES, "{n} bytes: (eager, rndv) {before:?} -> {after:?}");
    }
}
