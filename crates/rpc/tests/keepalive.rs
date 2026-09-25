//! Dead-peer detection: a connection whose peer stops sending bytes is closed
//! by the client itself, and one whose peer keeps answering is never judged —
//! however long a single request of its own takes.
#[path = "support/status_peer.rs"]
#[allow(dead_code)] // shared fixture; this suite uses part of it
mod peer;

use autumn_rpc::client::{Keepalive, RpcClient};
use autumn_rpc::{Frame, RpcError, StatusCode};
use bytes::Bytes;
use peer::*;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Short enough to keep the suite fast, with the same 1:5 shape as the default.
const FAST: Keepalive = Keepalive {
    interval: Duration::from_millis(100),
    dead_after: Duration::from_millis(500),
};

async fn connect(addr: &str) -> std::rc::Rc<RpcClient> {
    let addr: SocketAddr = addr.parse().unwrap();
    RpcClient::connect_with(addr, FAST).await.expect("connect")
}

/// The incident shape: the peer's kernel holds the socket open and ACKs,
/// its process never answers. A call with no deadline of its own used to wait
/// for as long as the process lived; now it is released with
/// `ConnectionClosed` once the connection has been silent for `dead_after`,
/// and `is_closed()` tells every pool to replace the connection.
#[compio::test]
async fn a_silent_peer_is_closed_and_its_waiting_call_released() {
    let _ = autumn_transport::current_or_init();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        let _ = stop_rx.recv_timeout(Duration::from_secs(30));
        drop(sock);
    });

    let client = connect(&addr).await;
    let started = Instant::now();
    // The outer bound is only there to turn a regression into a failure
    // instead of a hung test; the call itself carries no deadline.
    let outcome = compio::time::timeout(
        Duration::from_secs(5),
        client.call(ECHO, Bytes::from_static(b"x")),
    )
    .await
    .expect("a call on a silent connection must be released by the keepalive");
    let waited = started.elapsed();

    assert!(matches!(outcome, Err(RpcError::ConnectionClosed)), "{outcome:?}");
    assert!(client.is_closed());
    assert!(waited >= FAST.dead_after, "closed after only {waited:?}");
    assert!(waited < FAST.dead_after * 3, "took {waited:?}");
    // And nothing new can enter it.
    assert!(matches!(
        client.call(ECHO, Bytes::new()).await,
        Err(RpcError::ConnectionClosed)
    ));

    let _ = stop_tx.send(());
    holder.join().unwrap();
}

/// A live peer that is slow on ONE request is not a dead peer. The request
/// here never gets a reply, for longer than `dead_after` several times over,
/// while the peer answers pings — the connection must stay open and the call
/// must end by its own deadline, not by the keepalive.
#[compio::test]
async fn a_slow_request_on_an_answering_peer_is_not_mistaken_for_death() {
    let _ = autumn_transport::current_or_init();
    let peer = Peer::start(respond).await;
    let client = connect(&peer.addr).await;

    let own_deadline = FAST.dead_after * 4;
    let outcome = client.call_timeout(HANG, Bytes::new(), own_deadline).await;
    assert!(matches!(outcome, Err(RpcError::Timeout(_))), "{outcome:?}");
    assert!(!client.is_closed());
    assert_eq!(client.call(ECHO, Bytes::new()).await.unwrap(), b"ok"[..]);
    assert_eq!(peer.accepts.get(), 1);
}

/// An idle connection to a healthy peer stays open, whether the peer answers
/// the ping as a ping or — a server built before `MSG_TYPE_PING` existed —
/// refuses it as an unknown msg_type. Any reply is bytes, and bytes are life.
#[compio::test]
async fn an_idle_connection_stays_open_whatever_the_peer_answers_the_ping_with() {
    let _ = autumn_transport::current_or_init();
    fn refuses_ping(frame: Frame) -> Reply {
        if frame.msg_type == autumn_rpc::MSG_TYPE_PING {
            let status = RpcError::encode_status(
                StatusCode::InvalidArgument,
                &format!("unknown msg_type {}", frame.msg_type),
            );
            return Reply::Frame(Frame::error(frame.req_id, frame.msg_type, status));
        }
        respond(frame)
    }
    for peer in [Peer::start(respond).await, Peer::start(refuses_ping).await] {
        let client = connect(&peer.addr).await;
        compio::time::sleep(FAST.dead_after * 3).await;
        assert!(!client.is_closed());
        assert_eq!(client.call(ECHO, Bytes::new()).await.unwrap(), b"ok"[..]);
        assert_eq!(peer.accepts.get(), 1);
    }
}

/// Read `sock` like a slow but healthy server: at most `bytes_per_sec`, and
/// answer every complete frame with "ok" — the ping included.
fn serve_slowly(mut sock: std::net::TcpStream, bytes_per_sec: usize) {
    use std::io::{Read, Write};
    let mut decoder = autumn_rpc::FrameDecoder::new();
    let mut buf = vec![0u8; bytes_per_sec / 20];
    loop {
        let n = match sock.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        decoder.feed(&buf[..n]);
        while let Ok(Some(f)) = decoder.try_decode() {
            let reply = Frame::response(f.req_id, f.msg_type, Bytes::from_static(b"ok")).encode();
            if sock.write_all(&reply).is_err() {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A large request crossing a slow link is not a silent peer. Nothing comes
/// back while it is in flight, and a ping queued behind it cannot be answered
/// until it is through — timing the ping from when it was queued, or even from
/// when it was handed to the socket, closed the connection mid-transfer and
/// failed the call, on every retry alike. What does show the peer alive is its
/// kernel ACKing the bytes as they arrive.
///
/// The link is modelled by the peer's receive buffer: kept small, the peer's
/// kernel can only ACK as fast as the peer reads, so the bytes wait in OUR send
/// buffer — which is where they wait on a slow link. (A large receive buffer
/// would instead model a peer that received everything and is not reading,
/// which is the dead-peer signature and is meant to be closed.)
#[compio::test]
async fn a_large_request_on_a_slow_link_is_not_mistaken_for_death() {
    use std::os::fd::AsRawFd;
    let _ = autumn_transport::current_or_init();
    const RATE: usize = 16 * 1024 * 1024;
    let cfg = Keepalive {
        interval: Duration::from_millis(100),
        dead_after: Duration::from_millis(1000),
    };
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rcvbuf: libc::c_int = 64 * 1024;
    // SAFETY: a valid listening fd and a c_int option value.
    let rc = unsafe {
        libc::setsockopt(
            listener.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&rcvbuf as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0);
    let addr: SocketAddr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        serve_slowly(sock, RATE);
    });

    let client = RpcClient::connect_with(addr, cfg).await.expect("connect");
    // The small window paces the peer's reads to about 1.3 MB/s here, so this
    // takes about 3 s — three times `dead_after`.
    let big = Bytes::from(vec![7u8; 4 * 1024 * 1024]);
    let started = Instant::now();
    let reply = client
        .call_vectored_bulk(ECHO, vec![Bytes::from_static(b"k")], big)
        .await;
    let took = started.elapsed();
    assert_eq!(reply.expect("a slow link to a live peer is not death"), b"ok"[..]);
    assert!(took > cfg.dead_after, "the transfer must outlast dead_after ({took:?})");
    assert!(!client.is_closed());
    // The close is processed by this runtime; a blocking join here would
    // starve it and the peer would never see EOF.
    drop(client);
    while !server.is_finished() {
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    server.join().unwrap();
}

/// A bulk value receive already under way has left `pending` — the reader
/// holds its sender — so clearing `pending` cannot release it. The peer here
/// sends the head of an 8 MiB bulk reply and part of the value, then freezes
/// with the socket open. The caller carries no deadline of its own.
#[compio::test]
async fn a_bulk_receive_frozen_mid_value_is_released_by_the_close() {
    use std::io::{Read, Write};
    let _ = autumn_transport::current_or_init();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut decoder = autumn_rpc::FrameDecoder::new();
        let mut buf = vec![0u8; 4096];
        let req = loop {
            let n = sock.read(&mut buf).unwrap();
            decoder.feed(&buf[..n]);
            if let Some(f) = decoder.try_decode().unwrap() {
                break f;
            }
        };
        let value_len = 8 * 1024 * 1024;
        let head =
            autumn_rpc::frame::encode_bulk_response_head(req.req_id, req.msg_type, 0, "", value_len);
        sock.write_all(&head).unwrap();
        sock.write_all(&vec![1u8; value_len / 2]).unwrap();
        // Frozen: never another byte, never a read, socket held open.
        let _ = stop_rx.recv_timeout(Duration::from_secs(30));
    });

    let client = connect(&addr).await;
    let outcome = compio::time::timeout(
        Duration::from_secs(5),
        client.call_into_pooled(ECHO, Bytes::new()),
    )
    .await
    .expect("a receive frozen mid-value must be released by the keepalive close");
    assert!(matches!(outcome, Err(RpcError::ConnectionClosed)), "{outcome:?}");
    assert!(client.is_closed());
    let _ = stop_tx.send(());
    server.join().unwrap();
}

/// A peer whose process has stopped still has a kernel that ACKs whatever
/// arrives until its receive window fills — thousands of small requests. So
/// "the peer ACKed more of our bytes" is not a sign of life on its own: callers
/// that keep sending to a frozen peer (a fuse mount with concurrent readers
/// sharing one connection) would reset the verdict on every tick, forever.
#[compio::test]
async fn caller_traffic_to_a_frozen_peer_does_not_keep_it_alive() {
    let _ = autumn_transport::current_or_init();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        let _ = stop_rx.recv_timeout(Duration::from_secs(30));
        drop(sock);
    });

    let client = connect(&addr).await;
    let started = Instant::now();
    while !client.is_closed() && started.elapsed() < FAST.dead_after * 6 {
        // Fire-and-forget: the ACK is all the kernel will ever give back.
        let _ = client.send_oneshot(ECHO, Bytes::from_static(b"still here?")).await;
        compio::time::sleep(FAST.interval / 2).await;
    }
    assert!(
        client.is_closed(),
        "a frozen peer must be closed even while callers keep sending to it"
    );

    let _ = stop_tx.send(());
    while !holder.is_finished() {
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    holder.join().unwrap();
}

/// The same frozen peer under a steady stream of small requests. Its kernel
/// delays each ACK (up to ~40 ms), so at almost any instant one segment is
/// still unacknowledged — "bytes in flight" is true at nearly every sample
/// while the ACK count keeps rising. Treating that as a slow link kept the
/// peer alive for as long as the traffic lasted; only bytes queued behind the
/// peer's window (`notsent`) mark a slow link.
#[compio::test]
async fn steady_small_requests_to_a_frozen_peer_do_not_keep_it_alive() {
    let _ = autumn_transport::current_or_init();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        let _ = stop_rx.recv_timeout(Duration::from_secs(30));
        drop(sock);
    });

    let client = connect(&addr).await;
    let started = Instant::now();
    while !client.is_closed() && started.elapsed() < FAST.dead_after * 6 {
        let _ = client.send_oneshot(ECHO, Bytes::from_static(b"still here?")).await;
        compio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        client.is_closed(),
        "a frozen peer must be closed however fast callers keep sending to it"
    );
    // By the rule, not by the peer's receive window filling up — which would
    // stop the ACKs under ANY rule and so prove nothing.
    assert!(started.elapsed() < FAST.dead_after * 2, "closed only after {:?}", started.elapsed());

    let _ = stop_tx.send(());
    while !holder.is_finished() {
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    holder.join().unwrap();
}
