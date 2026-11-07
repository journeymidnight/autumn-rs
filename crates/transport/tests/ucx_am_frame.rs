//! Frame values (`am::Sender::post_frame_value` / `am::receive_frame_value`):
//! the value of an RPC frame, sent beside it and received into one pool
//! buffer whichever of the two arrives first. Binds `AUTUMN_UCX_TEST_BIND`,
//! like `loopback_ucx.rs`.

#![cfg(feature = "ucx")]

use autumn_transport::am;
use autumn_transport::{AutumnTransport, ReadHalf, TransportKind, UcxTransport};
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

/// Staging bytes this thread holds: a plain value is sent from a copy in a
/// pool buffer (charged here, where the send runs), released when the sender
/// lets go of it.
fn staged() -> usize {
    autumn_transport::regpool_thread_held(autumn_transport::PoolUse::Staging)
}

/// Post a plain value and wait until its sender has let go of its staged
/// copy. The send is spawned, so "nothing staged" alone would also hold before
/// it ran: it must have been sent too.
async fn post_until_released(tx: &am::Sender, tag: u64, value: Vec<u8>, what: &str) {
    let sent = am::sent_counts();
    tx.post_frame_value(tag, Bytes::from(value));
    until(what, || am::sent_counts() != sent && staged() == 0).await;
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
    let sent = am::sent_counts();
    tx.post_frame_value(9, Bytes::from(vec![5u8; 1 << 20]));
    let Err(err) = am::receive_frame_value(&rx, &[(2 << 20, 9)], &[]).await else { panic!("received") };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
    until("the sender let go of the refused value", || am::sent_counts() != sent && staged() == 0).await;
}

#[compio::test]
async fn values_nobody_will_receive_let_their_senders_complete() {
    // Given up before it arrived: dropped on arrival.
    let (tx, rx, keep) = pair().await;
    let segments = [(1 << 20, 11)];
    let receive = am::receive_frame_value(&rx, &segments, &[]);
    assert!(compio::time::timeout(Duration::from_millis(20), receive).await.is_err());
    post_until_released(&tx, 11, vec![1u8; 1 << 20], "a given-up value's sender completed").await;

    // Held for a frame that never comes: released with the connection.
    tx.post_frame_value(12, Bytes::from(vec![2u8; 1 << 20]));
    compio::time::sleep(Duration::from_millis(50)).await;
    assert!(staged() >= 1 << 20, "the value should wait for its frame");
    drop(rx);
    drop(keep);
    until("a held value's sender completed after its connection went", || staged() == 0).await;

    // Sent after its peer closed the connection: the send fails.
    post_until_released(&tx, 13, vec![3u8; 1 << 20], "a value for a connection already gone let its sender complete").await;
}

/// A peer that closes its connection fails the frame values still awaited from
/// it, and any asked for afterwards: the sender of a value that will never
/// come is gone, and the receiver must not wait for it.
#[compio::test]
async fn a_value_awaited_from_a_closed_connection_fails() {
    let (tx, rx, keep) = pair().await;
    let segments = [(1usize << 20, 61)];
    let mut awaited = Box::pin(am::receive_frame_value(&rx, &segments, &[]));
    assert!(compio::time::timeout(Duration::from_millis(20), &mut awaited).await.is_err(), "nothing sent yet");
    // The peer's end: its sender and its read half.
    drop(tx);
    drop(keep);
    let failed = compio::time::timeout(Duration::from_secs(10), awaited).await.expect("failed, not waiting");
    assert!(failed.is_err(), "nothing was sent");

    let later = am::receive_frame_value(&rx, &[(1 << 20, 62)], &[]);
    let failed = compio::time::timeout(Duration::from_secs(1), later).await.expect("failed at once");
    assert!(failed.is_err(), "asked for after the connection failed");
}

/// A value still awaited when its own connection is dropped fails: the
/// endpoint is closed and its address may be reused, so nothing stays
/// registered for it.
#[compio::test]
async fn a_value_awaited_past_its_connection_fails() {
    let (tx, rx, keep) = pair().await;
    let pending = am::start_frame_value(&rx, &[(1 << 20, 71)], &[], autumn_transport::regpool_acquire(1 << 20)).unwrap();
    // Our end goes (its write half is the last holder); the peer's, held by
    // `tx`, stays up, so no failure comes from its side.
    drop(rx);
    drop(keep);
    let failed = compio::time::timeout(Duration::from_secs(5), pending.wait()).await.expect("failed, not waiting");
    assert!(failed.is_err(), "nothing was sent");
    drop(tx);
}

#[compio::test]
async fn values_started_together_are_waited_in_any_order() {
    let (tx, rx, _keep) = pair().await;
    let (a, b) = (pattern(4 << 20, 7), pattern(1 << 20, 8));
    tx.post_frame_value(21, Bytes::from(a.clone()));
    tx.post_frame_value(22, Bytes::from(b.clone()));
    let first = am::start_frame_value(&rx, &[(a.len(), 21)], &[], autumn_transport::regpool_acquire(a.len())).unwrap();
    let second = am::start_frame_value(&rx, &[(b.len(), 22)], &[], autumn_transport::regpool_acquire(b.len())).unwrap();
    assert_eq!(second.wait().await.unwrap().filled(), b.as_slice());
    assert_eq!(first.wait().await.unwrap().filled(), a.as_slice());
}

/// A frame refused for want of a buffer gives its value up: one already held
/// is released, one still to come is dropped on arrival; both senders complete.
#[compio::test]
async fn an_abandoned_value_lets_its_sender_complete_whether_it_came_first_or_not() {
    let (tx, rx, _keep) = pair().await;
    tx.post_frame_value(31, Bytes::from(vec![4u8; 1 << 20]));
    compio::time::sleep(Duration::from_millis(50)).await;
    assert!(staged() >= 1 << 20, "held for its frame");
    am::abandon_frame_value(&rx, &[(1 << 20, 31)]);
    until("a held value's sender completed once abandoned", || staged() == 0).await;

    am::abandon_frame_value(&rx, &[(1 << 20, 32)]);
    post_until_released(&tx, 32, vec![5u8; 1 << 20], "a value abandoned before it came let its sender complete").await;
}

/// A pool buffer from the middle of its chunk goes out with the chunk's memh
/// and arrives whole by rendezvous: the receiver's read starts at the buffer,
/// not at the start of the registered region.
#[compio::test]
async fn a_value_from_the_middle_of_a_pool_chunk_arrives_by_rendezvous() {
    let (tx, rx, _keep) = pair().await;
    let head = autumn_transport::regpool_acquire(3 << 20);
    let mut pb = autumn_transport::regpool_acquire(64 << 10);
    let value = pattern(64 << 10, 77);
    pb.dest_mut().copy_from_slice(&value);
    let reg = pb.freeze();
    assert!(reg.is_registered(), "a pool buffer carries its chunk's memh");
    let at = (reg.bytes().as_ptr() as usize).wrapping_sub(head.filled().as_ptr() as usize);
    assert!(at >= 3 << 20 && at < 64 << 20, "above the 3 MiB buffer in the same chunk, {at} bytes on");

    let sent = am::sent_counts();
    let received = autumn_transport::ucx_am_received_counts();
    let tag = 4242;
    let segments = [(64usize << 10, tag)];
    let receive = am::receive_frame_value(&rx, &segments, &[]);
    tx.post_frame_value(tag, reg);
    let got = compio::time::timeout(Duration::from_secs(10), receive).await.expect("received").unwrap();
    assert_eq!(got.filled(), value.as_slice());
    assert_eq!(autumn_transport::ucx_am_received_counts().1, received.1 + 1, "by rendezvous");
    assert_eq!(am::sent_counts().0, sent.0 + 1, "sent with its memh");
    drop(head);
}

/// An empty slice of a registered value is a static `Bytes` that does not hold
/// the pool buffer, so it must not claim the buffer's registration.
#[compio::test]
async fn an_empty_slice_of_a_registered_value_keeps_no_memh() {
    let (_tx, _rx, _keep) = pair().await;
    let reg = autumn_transport::regpool_acquire(64 << 10).freeze();
    assert!(reg.is_registered());
    assert!(reg.slice(10..20).is_registered(), "a part keeps it");
    let empty = reg.slice(7..7);
    drop(reg);
    assert!(!empty.is_registered(), "nothing holds the buffer behind an empty slice");
}

/// A value lent from a registered region goes out with the region's memh (no
/// copy into the pool) and is returned once its send let go of it; one lent
/// from memory no region holds is copied into the pool and returned at once.
#[compio::test]
async fn a_lent_value_is_sent_from_its_region_and_returned_after() {
    let (tx, rx, _keep) = pair().await;
    let mut mem = pattern(2 << 20, 41);
    let region = unsafe { autumn_transport::Region::register(mem.as_mut_ptr(), mem.len()) }.unwrap();
    assert!(region.is_registered());
    let (value, returned) = unsafe { autumn_transport::lend(mem.as_ptr().add(1 << 20), 1 << 20, Some(&region)) };
    assert!(value.is_registered(), "inside the region: its memh");
    let (sent, staged) = (am::sent_counts(), am::staged_count());
    let segments = [(1usize << 20, 51)];
    let receive = am::receive_frame_value(&rx, &segments, &[]);
    tx.post_frame_value(51, value);
    let got = compio::time::timeout(Duration::from_secs(10), receive).await.expect("received").unwrap();
    assert_eq!(got.filled(), &mem[1 << 20..]);
    compio::time::timeout(Duration::from_secs(10), returned).await.expect("returned once sent");
    assert_eq!(am::sent_counts().0, sent.0 + 1, "sent with the region's memh");
    assert_eq!(am::staged_count(), staged, "not copied");

    let other = pattern(1 << 20, 42);
    let (value, returned) = unsafe { autumn_transport::lend(other.as_ptr(), other.len(), Some(&region)) };
    assert!(!value.is_registered(), "outside the region: none");
    let receive = am::receive_frame_value(&rx, &[(1 << 20, 52)], &[]);
    tx.post_frame_value(52, value);
    compio::time::timeout(Duration::from_secs(10), returned).await.expect("returned once copied");
    assert_eq!(compio::time::timeout(Duration::from_secs(10), receive).await.unwrap().unwrap().filled(), other.as_slice());
    drop(region);
}

/// A value in memory the process maps read-only (a read-only file mapping,
/// `np.memmap(mode='r')`) arrives: it is copied into a pool buffer at the send.
/// Handed to UCX as it is, the rendezvous never completes and the value never
/// arrives.
#[compio::test]
async fn a_value_in_read_only_memory_arrives() {
    let (tx, rx, _keep) = pair().await;
    let len = 2 << 20;
    // SAFETY: a private anonymous mapping, filled, then made read-only; it is
    // unmapped by the owner below, which nothing outlives.
    let ptr = unsafe {
        let p = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0);
        assert_ne!(p, libc::MAP_FAILED);
        std::slice::from_raw_parts_mut(p as *mut u8, len).copy_from_slice(&pattern(len, 61));
        assert_eq!(libc::mprotect(p, len, libc::PROT_READ), 0);
        p as *const u8
    };
    struct ReadOnly(*const u8, usize);
    unsafe impl Send for ReadOnly {}
    impl AsRef<[u8]> for ReadOnly {
        fn as_ref(&self) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.0, self.1) }
        }
    }
    impl Drop for ReadOnly {
        fn drop(&mut self) {
            unsafe { libc::munmap(self.0 as *mut libc::c_void, self.1) };
        }
    }
    let segments = [(len, 71)];
    let receive = am::receive_frame_value(&rx, &segments, &[]);
    tx.post_frame_value(71, Bytes::from_owner(ReadOnly(ptr, len)));
    let got = compio::time::timeout(Duration::from_secs(10), receive).await.expect("a read-only value arrives").unwrap();
    assert_eq!(got.filled(), pattern(len, 61).as_slice());
}

/// A value too large for one pool buffer is received one buffer per piece:
/// the parts come back in order, each a registered pool buffer, the stream
/// piece copied into its own.
#[compio::test]
async fn a_value_received_in_parts_comes_back_in_order() {
    let (tx, rx, _keep) = pair().await;
    let (a, b, c) = (pattern(100, 1), pattern(1 << 20, 2), pattern(3 << 20, 3));
    tx.post_frame_value(81, Bytes::from(b.clone()));
    tx.post_frame_value(82, Bytes::from(c.clone()));
    let segments = [(a.len(), 0), (b.len(), 81), (c.len(), 82)];
    let started = am::start_frame_value_parts(&rx, &segments, &a, autumn_transport::PoolUse::Receive).unwrap();
    let parts = compio::time::timeout(Duration::from_secs(10), started.wait()).await.expect("arrived").unwrap();
    assert_eq!(parts.len(), 3);
    for (part, want) in parts.iter().zip([&a, &b, &c]) {
        assert_eq!(&part[..], want.as_slice());
        assert!(part.is_registered(), "each part in a pool buffer");
    }
}

/// A frame's values with no memh are copied into the pool a few at a time:
/// while the receiver takes none, only `STAGED_WINDOW` of them are staged.
#[compio::test]
async fn unregistered_values_are_staged_a_few_at_a_time() {
    let (tx, rx, _keep) = pair().await;
    let n = 16usize;
    let values: Vec<(u64, autumn_transport::RegBytes)> =
        (0..n).map(|i| (90 + i as u64, Bytes::from(pattern(1 << 20, i as u8)).into())).collect();
    tx.post_frame_values(values);
    until("the window was staged", || staged() >= am::STAGED_WINDOW << 20).await;
    compio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(staged(), am::STAGED_WINDOW << 20, "only the window is staged while the receiver waits");
    let segments: Vec<(usize, u64)> = (0..n).map(|i| (1 << 20, 90 + i as u64)).collect();
    let got = compio::time::timeout(Duration::from_secs(10), am::receive_frame_value(&rx, &segments, &[]))
        .await
        .expect("arrived")
        .unwrap();
    let want: Vec<u8> = (0..n).flat_map(|i| pattern(1 << 20, i as u8)).collect();
    assert_eq!(got.filled(), want.as_slice());
    until("every staged value was let go", || staged() == 0).await;
}
