//! A graceful shutdown leaves no partition serving: region sync cannot reopen
//! a partition the drain is closing.
//!
//! The PS hands a clone of itself to each supervised loop, region sync among
//! them. Two ways region sync used to slip past a shutdown, each reopening a
//! partition after (or while) the drain closed it:
//!
//! - the shutdown flag was a by-value `Cell`, so a clone never saw it set
//!   (`a_clone_cannot_reopen_after_shutdown`);
//! - a pass already past the flag check kept going across its manager call
//!   while the drain ran, then acted on what it fetched
//!   (`a_sync_pass_in_flight_cannot_reopen_after_shutdown`).
//!
//! Both cases change the partition's range under the PS, so the sync pass
//! that slips through reopens it deterministically (a changed region tuple is
//! dropped and reopened).

mod support;

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_partition_server::{replay_read_bytes, set_shutdown_timeout_ms, PartitionServer};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::CODE_OK;
use support::*;

/// What a replay that starts at the tail may still read.
const TAIL_REPLAY_BOUND: u64 = 64 * 1024;

struct Cluster {
    mgr_addr: SocketAddr,
    _dir: tempfile::TempDir,
    streams: (u64, u64, u64),
}

fn start_cluster(part_id: u64) -> Cluster {
    // Bounds every wait inside `shutdown()`; first setter in the process wins.
    set_shutdown_timeout_ms(10_000);
    let mgr_addr = pick_addr();
    let n1_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(n1_addr, dir.path().to_path_buf(), 1);
    let streams = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        let _ = register_node(&mgr, &n1_addr.to_string(), &format!("uuid-shut-{part_id}")).await;
        let log = create_stream(&mgr, 1).await;
        let row = create_stream(&mgr, 1).await;
        let meta = create_stream(&mgr, 1).await;
        upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff").await;
        (log, row, meta)
    });
    Cluster {
        mgr_addr,
        _dir: dir,
        streams,
    }
}

/// Move the partition's end key: the next sync pass on a PS that still holds
/// it sees a changed region tuple and reopens it.
async fn change_range(c: &Cluster, part_id: u64, end_key: &[u8]) {
    let mgr = RpcClient::connect(c.mgr_addr).await.expect("mgr");
    let (log, row, meta) = c.streams;
    upsert_partition(&mgr, part_id, log, row, meta, b"", end_key).await;
}

/// The first partition a PS opens listens on its `--port`.
async fn serving(part_addr: SocketAddr) -> bool {
    RpcClient::connect(part_addr).await.is_ok()
}

/// A TCP relay that can hold every byte in both directions, so a manager call
/// can be kept in flight for as long as the test wants.
struct PausableProxy {
    addr: SocketAddr,
    paused: Arc<AtomicBool>,
}

impl PausableProxy {
    fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let paused = Arc::new(AtomicBool::new(false));
        let p = paused.clone();
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                let server = TcpStream::connect(target).expect("proxy connect target");
                relay(client.try_clone().unwrap(), server.try_clone().unwrap(), p.clone());
                relay(server, client, p.clone());
            }
        });
        Self { addr, paused }
    }

    fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }
}

fn relay(mut from: TcpStream, mut to: TcpStream, paused: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            while paused.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(10));
            }
            if to.write_all(&buf[..n]).is_err() {
                break;
            }
        }
        to.shutdown(Shutdown::Write).ok();
    });
}

#[test]
fn a_clone_cannot_reopen_after_shutdown() {
    let part_id = 1201;
    let c = start_cluster(part_id);
    let ps_addr = pick_addr();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = PartitionServer::connect_with_advertise_and_port(
            1,
            &c.mgr_addr.to_string(),
            Some(ps_addr.to_string()),
            ps_addr,
        )
        .await
        .expect("connect partition server");
        assert!(serving(ps_addr).await, "partition {part_id} did not open");
        // The copy a supervised loop (region sync, heartbeat) runs on.
        let region_sync = ps.clone();

        ps.shutdown().await.expect("shutdown");
        assert!(!serving(ps_addr).await, "partition still serving after the drain");

        change_range(&c, part_id, b"\xfe").await;
        region_sync.sync_regions_once().await.expect("sync");
        assert!(
            !serving(ps_addr).await,
            "region sync on a clone reopened partition {part_id} after shutdown"
        );
    });
}

#[test]
fn a_sync_pass_in_flight_cannot_reopen_after_shutdown() {
    let part_id = 1202;
    let c = start_cluster(part_id);
    let proxy = PausableProxy::start(c.mgr_addr);
    let ps_addr = pick_addr();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = PartitionServer::connect_with_advertise_and_port(
            1,
            &proxy.addr.to_string(),
            Some(ps_addr.to_string()),
            ps_addr,
        )
        .await
        .expect("connect partition server");
        let client = RpcClient::connect(ps_addr).await.expect("ps");
        ps_put(&client, part_id, b"k", b"v").await;
        drop(client);

        // Hold a sync pass inside its manager call, then change the region
        // under it: once released, the pass sees a changed tuple and reopens.
        proxy.pause();
        let region_sync = ps.clone();
        let pass = compio::runtime::spawn(async move { region_sync.sync_regions_once().await });
        compio::time::sleep(Duration::from_millis(300)).await;
        assert!(!pass.is_finished(), "the sync pass was not held in its manager call");
        change_range(&c, part_id, b"\xfe").await;

        // Release the held pass after shutdown has had time to drain without
        // it (its own manager report times out after 2 s on the paused relay).
        let release = async {
            compio::time::sleep(Duration::from_secs(4)).await;
            proxy.resume();
        };
        let (shut, ()) = futures::join!(ps.shutdown(), release);
        shut.expect("shutdown");
        pass.await.expect("sync pass task").expect("sync pass");
        assert!(
            !serving(ps_addr).await,
            "a sync pass in flight across shutdown reopened partition {part_id}"
        );
    });

    // The pass's reopen (if any) happened before the drain, so the drain
    // published a checkpoint at the tail and a restart replays only that.
    let before = replay_read_bytes(part_id);
    let ps_addr = pick_addr();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = PartitionServer::connect_with_advertise_and_port(
            1,
            &c.mgr_addr.to_string(),
            Some(ps_addr.to_string()),
            ps_addr,
        )
        .await
        .expect("restart partition server");
        let client = RpcClient::connect(ps_addr).await.expect("ps");
        let r = ps_get(&client, part_id, b"k").await;
        assert_eq!(r.code, CODE_OK, "k lost across the restart");
        assert_eq!(r.value, b"v");
        drop(client);
        ps.shutdown().await.expect("shutdown");
    });
    let replayed = replay_read_bytes(part_id) - before;
    assert!(
        replayed < TAIL_REPLAY_BOUND,
        "a restart after the drain replayed {replayed} WAL bytes"
    );
}
