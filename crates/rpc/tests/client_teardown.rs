//! Dropping an `RpcClient` must CLOSE its connection.
//!
//! An evicted client (`ps_conns.remove` after a timeout, `ConnPool::evict`,
//! `ps_conns.clear()` on token renewal) is dropped while its two background
//! tasks are blocked on the wire. Nothing else can end them: `writer_task`
//! blocks in `write_all` on a peer that stopped reading, so it never returns to
//! notice its channel closed, and `read_loop` blocks in `read` until EOF. With
//! the task handles detached, only the PEER closing could free any of it —
//! measured at 5 leaked ESTABLISHED sockets and 70 MiB of pinned request values
//! over 5 evictions, and the same leak for an evicted HEALTHY idle connection.
//!
//! Ablation: restore `.detach()` on either handle in `RpcClient::from_conn` and
//! both tests below fail — no EOF ever reaches the peer.

use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use bytes::Bytes;

/// A request value that reports when its last reference goes away, so a frame
/// still pinned in a zombie connection's submit queue is visible as a count.
struct Tracked {
    data: Vec<u8>,
    live: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
fn tracked(len: usize, live: &Arc<AtomicUsize>) -> Bytes {
    live.fetch_add(1, Ordering::SeqCst);
    Bytes::from_owner(Tracked {
        data: vec![7u8; len],
        live: live.clone(),
    })
}

/// Kernel states of this process's sockets connected to `server`
/// (IPv4 loopback). A socket the client closed is gone from here, or is winding
/// down (FIN_WAIT*/TIME_WAIT); one whose fd is still held reads ESTABLISHED.
#[cfg(target_os = "linux")]
fn client_socket_states(server: SocketAddr) -> Vec<String> {
    let text = std::fs::read_to_string("/proc/self/net/tcp").expect("read /proc/self/net/tcp");
    let want_rem = format!("0100007F:{:04X}", server.port());
    text.lines()
        .skip(1)
        .filter_map(|line| {
            let f: Vec<&str> = line.split_whitespace().collect();
            (f.len() > 3 && f[2] == want_rem).then(|| f[3].to_string())
        })
        .collect()
}

/// Accept `n` connections and hold them without reading a byte — the peer that
/// blocks `writer_task` mid-write. On `stop`, drain each socket and report
/// whether it reached EOF (i.e. the client closed its end) within 5 s.
fn never_reading_peer(
    n: usize,
) -> (
    SocketAddr,
    mpsc::Sender<()>,
    std::thread::JoinHandle<Vec<bool>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let h = std::thread::spawn(move || {
        let mut socks: Vec<TcpStream> = Vec::new();
        for _ in 0..n {
            socks.push(listener.accept().expect("accept").0);
        }
        stop_rx.recv().expect("stop signal");
        socks.into_iter().map(drain_to_eof).collect()
    });
    (addr, stop_tx, h)
}

fn drain_to_eof(mut sock: TcpStream) -> bool {
    sock.set_read_timeout(Some(Duration::from_secs(5))).expect("read timeout");
    let mut buf = vec![0u8; 1 << 20];
    loop {
        match sock.read(&mut buf) {
            Ok(0) => return true,  // FIN: the client closed its end
            Ok(_) => {}            // request bytes; keep draining
            Err(_) => return false, // 5 s with neither data nor EOF
        }
    }
}

/// Keep the compio runtime polled while waiting on a std thread — a blocking
/// `join` freezes every client task and the socket states stop moving.
async fn join_polling<T>(h: std::thread::JoinHandle<T>) -> T {
    while !h.is_finished() {
        compio::time::sleep(Duration::from_millis(20)).await;
    }
    h.join().expect("peer thread")
}

async fn settle() {
    compio::time::sleep(Duration::from_millis(500)).await;
}

/// The eviction shape: every call times out against a peer that never reads,
/// the client is dropped, and the next attempt reconnects. Each dropped client
/// must take its socket, its tasks and its queued request values with it.
#[compio::test]
async fn dropping_a_client_frees_its_queued_values_and_closes_its_socket() {
    let _ = autumn_transport::current_or_init();
    const CYCLES: usize = 3;
    const CALLS: usize = 16;
    const VAL: usize = 1 << 20;

    let live = Arc::new(AtomicUsize::new(0));
    let (addr, stop, peer) = never_reading_peer(CYCLES);

    for cycle in 0..CYCLES {
        let client = RpcClient::connect(addr).await.expect("connect");
        let calls = (0..CALLS).map(|_| {
            let client = client.clone();
            let value = tracked(VAL, &live);
            async move {
                compio::time::timeout(
                    Duration::from_millis(300),
                    client.call_vectored_bulk(0x51, vec![Bytes::from_static(b"meta")], value),
                )
                .await
            }
        });
        let outcomes = futures::future::join_all(calls).await;
        assert!(
            outcomes.iter().all(|r| r.is_err()),
            "cycle {cycle}: a peer that never reads cannot answer; if these \
             completed, the fixture stopped reproducing the eviction shape"
        );

        drop(client); // what `ps_conns.remove(ps_addr)` does after a timeout
        settle().await;

        assert_eq!(
            live.load(Ordering::SeqCst),
            0,
            "cycle {cycle}: every request value must be freed when the client is \
             dropped; a surviving one is pinned in the dead connection's submit \
             queue until the peer closes (measured: 14 MiB per eviction)"
        );
        #[cfg(target_os = "linux")]
        assert!(
            !client_socket_states(addr).iter().any(|s| s == "01"),
            "cycle {cycle}: no ESTABLISHED socket may outlive its client, or \
             every eviction leaks one fd on each side: {:?}",
            client_socket_states(addr)
        );
    }

    stop.send(()).expect("stop peer");
    assert_eq!(
        join_polling(peer).await,
        vec![true; CYCLES],
        "each dropped client must close its end so the peer sees EOF; without \
         it the peer holds its own socket and conn task forever too"
    );
}

/// The same teardown is owed to a HEALTHY connection: a server status error
/// (a stale region epoch) and a token renewal both evict one with nothing in
/// flight, and that client's `read_loop` is parked in `read` on a live peer.
#[compio::test]
async fn dropping_an_idle_client_closes_the_connection() {
    let _ = autumn_transport::current_or_init();
    const CLIENTS: usize = 3;

    let (addr, stop, peer) = never_reading_peer(CLIENTS);
    for _ in 0..CLIENTS {
        let client = RpcClient::connect(addr).await.expect("connect");
        settle().await; // let both tasks park on the wire
        drop(client);
    }
    settle().await;

    stop.send(()).expect("stop peer");
    assert_eq!(
        join_polling(peer).await,
        vec![true; CLIENTS],
        "an idle client's peer must see EOF when the client is dropped"
    );
}
