//! Frame values (`am::Sender::post_frame_value` / `am::receive_frame_value`):
//! the value of an RPC frame, sent beside it and received into one pool
//! buffer whichever of the two arrives first. Binds `AUTUMN_UCX_TEST_BIND`,
//! like `loopback_ucx.rs`.

#![cfg(feature = "ucx")]

use autumn_transport::am;
use autumn_transport::{AutumnTransport, ReadHalf, TransportKind, UcxTransport};
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

/// A connected pair: the accepted side's sender and the dialing side's reader.
async fn pair() -> (am::Sender, ReadHalf, impl Sized) {
    autumn_transport::init_with(TransportKind::Ucx);
    let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap().0 });
    let client = UcxTransport.connect(bound).await.unwrap();
    let server = accept.await.unwrap();
    let (server_r, server_w) = server.into_split();
    let (client_r, client_w) = client.into_split();
    (server_w.am_sender().expect("a UCX connection"), client_r, (server_r, client_w))
}

/// A value whose memory reports when the sender let go of it.
fn tracked(data: Vec<u8>) -> (Bytes, Arc<AtomicBool>) {
    struct Owner(Vec<u8>, Arc<AtomicBool>);
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.1.store(true, Ordering::Relaxed);
        }
    }
    let gone = Arc::new(AtomicBool::new(false));
    (Bytes::from_owner(Owner(data, gone.clone())), gone)
}

async fn until(what: &str, f: impl Fn() -> bool) {
    for _ in 0..500 {
        if f() {
            return;
        }
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting until {what}");
}

#[compio::test]
async fn a_value_lands_whether_it_comes_before_or_after_its_frame() {
    let (tx, rx, _keep) = pair().await;
    for (n, early) in [(64usize << 10, false), (64 << 10, true), (16 << 20, false), (16 << 20, true)] {
        let value = pattern(n, n as u8 ^ early as u8);
        let tag = 1000 + n as u64 + early as u64;
        let before = autumn_transport::ucx_am_received_counts();
        if early {
            tx.post_frame_value(tag, Bytes::from(value.clone()));
            // Let the rendezvous request arrive and be held.
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        let segments = [(n, tag)];
        let receive = am::receive_frame_value(&rx, &segments, &[]);
        if !early {
            tx.post_frame_value(tag, Bytes::from(value.clone()));
        }
        let got = compio::time::timeout(Duration::from_secs(10), receive).await.expect("received").unwrap();
        assert_eq!(got.filled(), value.as_slice(), "{n} bytes, early={early}");
        let after = autumn_transport::ucx_am_received_counts();
        assert_eq!(after.1, before.1 + 1, "received by rendezvous: {before:?} -> {after:?}");
    }
}

#[compio::test]
async fn pieces_from_the_stream_and_from_values_join_in_order() {
    let (tx, rx, _keep) = pair().await;
    let a = pattern(100, 1);
    let b = pattern(1 << 20, 2);
    let c = pattern(50, 3);
    let d = pattern(64 << 10, 4);
    tx.post_frame_value(7, Bytes::from(d.clone()));
    tx.post_frame_value(6, Bytes::from(b.clone()));
    let inline = [a.clone(), c.clone()].concat();
    let segments = [(a.len(), 0), (b.len(), 6), (c.len(), 0), (d.len(), 7)];
    let got = am::receive_frame_value(&rx, &segments, &inline).await.unwrap();
    assert_eq!(got.filled(), [a, b, c, d].concat().as_slice());
}

#[compio::test]
async fn a_value_of_another_length_is_refused_and_its_sender_completes() {
    let (tx, rx, _keep) = pair().await;
    let (value, sent) = tracked(vec![5u8; 1 << 20]);
    tx.post_frame_value(9, value);
    let Err(err) = am::receive_frame_value(&rx, &[(2 << 20, 9)], &[]).await else { panic!("received") };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
    until("the sender let go of the refused value", || sent.load(Ordering::Relaxed)).await;
}

#[compio::test]
async fn values_nobody_will_receive_let_their_senders_complete() {
    // Given up before it arrived: dropped on arrival.
    let (tx, rx, keep) = pair().await;
    let segments = [(1 << 20, 11)];
    let receive = am::receive_frame_value(&rx, &segments, &[]);
    assert!(compio::time::timeout(Duration::from_millis(20), receive).await.is_err());
    let (value, sent) = tracked(vec![1u8; 1 << 20]);
    tx.post_frame_value(11, value);
    until("a given-up value's sender completed", || sent.load(Ordering::Relaxed)).await;

    // Held for a frame that never comes: released with the connection.
    let (value, sent) = tracked(vec![2u8; 1 << 20]);
    tx.post_frame_value(12, value);
    compio::time::sleep(Duration::from_millis(50)).await;
    assert!(!sent.load(Ordering::Relaxed), "the value should wait for its frame");
    drop(rx);
    drop(keep);
    until("a held value's sender completed after its connection went", || sent.load(Ordering::Relaxed)).await;

    // Sent after its connection went (UCX keeps the endpoint): dropped on arrival.
    let (value, sent) = tracked(vec![3u8; 1 << 20]);
    tx.post_frame_value(13, value);
    until("a value for a connection already gone let its sender complete", || sent.load(Ordering::Relaxed)).await;
}

#[compio::test]
async fn values_started_together_are_waited_in_any_order() {
    let (tx, rx, _keep) = pair().await;
    let (a, b) = (pattern(4 << 20, 7), pattern(1 << 20, 8));
    tx.post_frame_value(21, Bytes::from(a.clone()));
    tx.post_frame_value(22, Bytes::from(b.clone()));
    let first = am::start_frame_value(&rx, &[(a.len(), 21)], &[]).unwrap();
    let second = am::start_frame_value(&rx, &[(b.len(), 22)], &[]).unwrap();
    assert_eq!(second.wait().await.unwrap().filled(), b.as_slice());
    assert_eq!(first.wait().await.unwrap().filled(), a.as_slice());
}
