//! Tagged values as UCX active messages (`autumn_transport::am`): a value
//! lands in the memory registered for its tag (eager below
//! `BULK_MIN_BYTES`, rendezvous from it on); a value nobody registered, or
//! registered and given up on, is dropped and its send still completes; a
//! value longer than its memory is reported, not received. Binds
//! `AUTUMN_UCX_TEST_BIND`, like `loopback_ucx.rs`.
//!
//! UCX's own threshold is set to 1 MiB here, so a 64 KiB value goes by
//! rendezvous only because `send` asks for it; `ucx_am_eager.rs` sets it to
//! 4 KiB for the eager side.

#![cfg(feature = "ucx")]

use autumn_transport::am::{self, Arrival};
use autumn_transport::{AutumnTransport, RecvDest, TransportKind, UcxTransport};
use bytes::Bytes;
use std::net::SocketAddr;
use std::time::Duration;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

/// A connected pair; returns the sending half of the accepted side.
pub async fn pair_with_rndv_thresh(thresh: &str) -> (am::Sender, impl Sized) {
    // Before the first UCX use in this process, which reads it.
    unsafe { std::env::set_var("UCX_RNDV_THRESH", thresh) };
    autumn_transport::init_with(TransportKind::Ucx);
    let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap().0 });
    let client = UcxTransport.connect(bound).await.unwrap();
    let server = accept.await.unwrap();
    let (_r, w) = server.into_split();
    (w.am_sender().expect("a UCX connection"), client)
}

#[compio::test]
async fn values_land_in_the_registered_memory_and_strangers_are_dropped() {
    let (tx, _client) = pair_with_rndv_thresh("1m").await;
    for n in [1024usize, 60 << 10, 64 << 10, 1 << 20, 16 << 20] {
        let value = pattern(n, n as u8);
        let mut mem = vec![0u8; n];
        let tag = am::next_tag();
        let (dest, released) = unsafe { RecvDest::new(mem.as_mut_ptr(), n) };
        let expected = am::expect(tag, dest).unwrap();
        let before = autumn_transport::ucx_am_received_counts();
        let send = tx.send(tag, Bytes::from(value.clone()));
        let (sent, got) = futures::join!(send, expected.arrived());
        sent.unwrap();
        assert!(matches!(got.unwrap(), Arrival::Received(m) if m == n), "{n}");
        expected.finish().await;
        assert!(released.is_released());
        assert_eq!(mem, value, "value of {n} bytes");
        let after = autumn_transport::ucx_am_received_counts();
        let rndv = after.1 > before.1;
        // Rendezvous exactly from BULK_MIN_BYTES on, whatever UCX's own threshold.
        assert_eq!(rndv, n >= autumn_transport::BULK_MIN_BYTES, "{n} bytes: (eager, rndv) {before:?} -> {after:?}");
    }
    // Nobody registered this tag: dropped, and the send completes all the same.
    let stranger = compio::time::timeout(Duration::from_secs(5), tx.send(am::next_tag(), Bytes::from(vec![1u8; 16 << 20])));
    stranger.await.expect("a dropped value's send completes").unwrap();
}

#[compio::test]
async fn given_up_and_too_long_values_leave_the_memory_alone() {
    let (tx, _client) = pair_with_rndv_thresh("1m").await;
    // Given up before it arrived: the memory is released at once, the late
    // value is dropped (its send completes) and writes nothing.
    let mut mem = vec![0u8; 16 << 20];
    let tag = am::next_tag();
    let (dest, released) = unsafe { RecvDest::new(mem.as_mut_ptr(), mem.len()) };
    am::expect(tag, dest).unwrap().finish().await;
    assert!(released.is_released());
    let late = compio::time::timeout(Duration::from_secs(5), tx.send(tag, Bytes::from(vec![9u8; 16 << 20])));
    late.await.expect("a late value's send completes").unwrap();
    assert!(mem.iter().all(|&b| b == 0), "the late value was written");

    // Longer than the memory: reported, not received.
    let mut small = vec![0u8; 1 << 20];
    let tag = am::next_tag();
    let (dest, released) = unsafe { RecvDest::new(small.as_mut_ptr(), small.len()) };
    let expected = am::expect(tag, dest).unwrap();
    let (sent, got) = futures::join!(tx.send(tag, Bytes::from(vec![7u8; 2 << 20])), expected.arrived());
    sent.unwrap();
    assert!(matches!(got.unwrap(), Arrival::TooLong(n) if n == 2 << 20));
    expected.finish().await;
    assert!(released.is_released());
    assert!(small.iter().all(|&b| b == 0), "a too-long value was written");
}
