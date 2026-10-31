//! A stream recv whose future is dropped stays posted on the endpoint: UCX
//! has no cancel for stream operations (`ucp_request_cancel` handles tag
//! receives only), so the next bytes on that connection land in the dropped
//! recv's buffer and never reach a later recv. The transport must therefore
//! keep that buffer alive until UCX is done with it, and refuse further reads
//! on the connection instead of losing bytes silently.
//!
//! Needs a RoCE address: `AUTUMN_UCX_TEST_BIND="[<roce-ip>]:0"`, plus the
//! usual `UCX_TLS` / `UCX_NET_DEVICES`.

#![cfg(feature = "ucx")]

use autumn_transport::{regpool_acquire, ucx_orphan_counts, AutumnTransport, UcxTransport};
use compio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use compio::BufResult;
use std::net::SocketAddr;
use std::time::Duration;

const LEN: usize = 256 * 1024;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

/// Server: accept one connection, send `LEN` bytes of `fill` once told to.
fn spawn_sender(
    listener: autumn_transport::Listener,
    fill: u8,
) -> (
    futures::channel::oneshot::Sender<()>,
    compio::runtime::JoinHandle<()>,
) {
    let (go_tx, go_rx) = futures::channel::oneshot::channel::<()>();
    let mut listener = listener;
    let task = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        let (_r, mut w) = c.into_split();
        go_rx.await.unwrap();
        w.write_all(vec![fill; LEN]).await.0.unwrap();
    });
    (go_tx, task)
}

/// The pool slab a dropped recv was posted into stays out of the pool until
/// the bytes land, then comes back. Without the orphan list the slab returns
/// to the pool at once, the next `acquire` hands it out again, and the bytes
/// the peer then sends are unpacked into it by the still-posted recv.
#[compio::test]
async fn a_dropped_recv_keeps_its_pool_slab_until_the_bytes_land() {
    autumn_transport::init_with(autumn_transport::TransportKind::Ucx);
    let t = UcxTransport;
    let listener = t.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let (go, server) = spawn_sender(listener, 0xa5);

    let c = t.connect(bound).await.unwrap();
    let (mut r, _w) = c.into_split();

    let pb = regpool_acquire(LEN);
    assert!(pb.reg().is_some(), "slab should be registered");
    let first_slab = pb.filled().as_ptr();
    let (orphaned0, reaped0, _) = ucx_orphan_counts();
    let race = async {
        let _ = r.recv_pooled(pb, 0).await;
    };
    let _ = compio::time::timeout(Duration::from_millis(1), race).await;
    let (orphaned, reaped, _) = ucx_orphan_counts();
    assert_eq!((orphaned, reaped), (orphaned0 + 1, reaped0), "the recv was orphaned");

    // Whoever gets a slab from the pool now owns it alone.
    let mut other = regpool_acquire(LEN);
    other.dest_mut().fill(0x11);
    assert_ne!(
        other.filled().as_ptr(),
        first_slab,
        "the slab of a still-posted recv was handed out again"
    );

    go.send(()).unwrap();
    compio::time::sleep(Duration::from_millis(300)).await;
    let foreign = other.filled().iter().filter(|&&b| b != 0x11).count();
    assert_eq!(foreign, 0, "{foreign} bytes of a recycled pool slab were overwritten");
    drop(other);

    // The bytes landed in the orphan; the progress loop or this `acquire`
    // frees it, and its slab is in the pool again (under `other`, which went
    // back on top).
    let again = regpool_acquire(LEN);
    let again2 = regpool_acquire(LEN);
    let (orphaned, reaped, _) = ucx_orphan_counts();
    assert_eq!((orphaned, reaped), (orphaned0 + 1, reaped0 + 1), "the orphan was reaped");
    assert!(
        again.filled().as_ptr() == first_slab || again2.filled().as_ptr() == first_slab,
        "the orphan's slab did not return to the pool"
    );
    server.await.expect("server task");
}

/// After a recv future was dropped, a later read on the same connection must
/// fail rather than wait for bytes the orphaned recv already took.
#[compio::test]
async fn a_read_after_a_dropped_recv_fails_instead_of_losing_bytes() {
    let t = UcxTransport;
    let listener = t.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let (go, server) = spawn_sender(listener, 0x5a);

    let c = t.connect(bound).await.unwrap();
    let (mut r, _w) = c.into_split();

    let race = async {
        let _ = r.read(vec![0u8; LEN]).await;
    };
    let _ = compio::time::timeout(Duration::from_millis(1), race).await;

    go.send(()).unwrap();
    let later = compio::time::timeout(Duration::from_secs(2), r.read(vec![0u8; LEN])).await;
    match later {
        Err(_) => panic!("the read after a dropped recv waited forever: the bytes went to the orphan"),
        Ok(BufResult(Ok(n), _)) => panic!("the read after a dropped recv returned {n} bytes"),
        Ok(BufResult(Err(e), _)) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionAborted, "{e}"),
    }
    server.await.expect("server task");
}

/// An orphan whose peer never sends again never completes. Later operations on
/// the thread must not pay for it: the reap fast path looks at completions,
/// not at the list, so no scan runs while the orphan merely exists. Each
/// round acquires a pool buffer, as every bulk operation does, which is one of
/// the two places that reap (the other, the progress loop, never wakes here:
/// the inline spin catches every completion).
#[compio::test]
async fn a_pending_orphan_costs_later_operations_no_scan() {
    use compio::io::AsyncWriteExt as _;

    let t = UcxTransport;
    let mut listener = t.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    const ROUNDS: usize = 200;
    const MSG: usize = 64;

    // First connection: silent, kept open (closing it would complete the
    // orphan). Second: echo.
    let server = compio::runtime::spawn(async move {
        let (silent, _) = listener.accept().await.unwrap();
        let (c, _) = listener.accept().await.unwrap();
        let (mut r, mut w) = c.into_split();
        for _ in 0..ROUNDS {
            let BufResult(res, b) = r.read_exact(vec![0u8; MSG]).await;
            res.unwrap();
            w.write_all(b).await.0.unwrap();
        }
        silent
    });

    let silent = t.connect(bound).await.unwrap();
    let (mut sr, _sw) = silent.into_split();
    let race = async {
        let _ = sr.read(vec![0u8; MSG]).await;
    };
    let _ = compio::time::timeout(Duration::from_millis(1), race).await;
    let (orphaned0, reaped0, scans0) = ucx_orphan_counts();

    let c = t.connect(bound).await.unwrap();
    let (mut r, mut w) = c.into_split();
    for i in 0..ROUNDS {
        let _slab = regpool_acquire(4096);
        w.write_all(vec![(i & 0xff) as u8; MSG]).await.0.unwrap();
        let BufResult(res, b) = r.read_exact(vec![0u8; MSG]).await;
        res.unwrap();
        assert!(b.iter().all(|&v| v == (i & 0xff) as u8));
    }
    let _silent = server.await.expect("server task");
    let (orphaned, reaped, scans) = ucx_orphan_counts();
    assert_eq!(orphaned, orphaned0, "no further orphan");
    assert_eq!(reaped, reaped0, "the silent orphan cannot complete");
    assert_eq!(scans, scans0, "a pending orphan put later operations on the scan path");
}

/// Closing a connection completes the receives still posted on it (as
/// cancelled), so a dropped recv's buffer comes back even though its peer
/// never sends again.
#[compio::test]
async fn an_orphan_is_freed_when_its_connection_closes() {
    autumn_transport::init_with(autumn_transport::TransportKind::Ucx);
    let t = UcxTransport;
    let mut listener = t.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move { listener.accept().await.unwrap().0 });
    let c = t.connect(bound).await.unwrap();
    let _silent = server.await.expect("server task");

    let (mut r, w) = c.into_split();
    let race = async {
        let _ = r.read(vec![0u8; 64]).await;
    };
    let _ = compio::time::timeout(Duration::from_millis(1), race).await;
    let (orphaned0, reaped0, _) = ucx_orphan_counts();
    assert!(orphaned0 > 0, "the recv was dropped in flight");
    drop(r);
    drop(w);
    // A pool acquire reaps.
    drop(regpool_acquire(4096));
    let (_, reaped, _) = ucx_orphan_counts();
    assert_eq!(reaped, reaped0 + 1, "the closed connection's orphan was freed");
}
