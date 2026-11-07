//! A peer process killed mid-transfer: the value it was receiving is returned
//! to its sender, and the value awaited from it fails, instead of either side
//! waiting forever (UCX peer error handling, endpoint.rs "EP lifetime and peer
//! failure"). The peer is this test binary run again as `peer_process`.
//!
//! Needs a RoCE address: `AUTUMN_UCX_TEST_BIND="[<roce-ip>]:0"`, plus the
//! usual `UCX_TLS` / `UCX_NET_DEVICES`.

#![cfg(feature = "ucx")]

use autumn_transport::am;
use autumn_transport::{AutumnTransport, TransportKind, UcxTransport};
use compio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Set for the child run: the address it connects to.
const PEER_OF: &str = "AUTUMN_UCX_DEAD_PEER_OF";

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

/// The child: connect, say so, then hold whatever arrives (a frame value with
/// no frame is held, unreceived) until killed.
#[compio::test]
async fn peer_process() {
    let Ok(addr) = std::env::var(PEER_OF) else { return };
    autumn_transport::init_with(TransportKind::Ucx);
    let c = UcxTransport.connect(addr.parse().unwrap()).await.unwrap();
    let (_r, mut w) = c.into_split();
    w.write_all(vec![1u8]).await.0.unwrap();
    compio::time::sleep(Duration::from_secs(120)).await;
}

/// The peer process, killed whatever happens to the test.
struct Killed(std::process::Child);

impl Drop for Killed {
    fn drop(&mut self) {
        // Already gone on the test's own path.
        if self.0.try_wait().ok().flatten().is_none() {
            self.0.kill().ok();
            self.0.wait().ok();
        }
    }
}

#[compio::test]
async fn a_killed_peer_returns_the_value_it_held_and_fails_the_one_awaited() {
    autumn_transport::init_with(TransportKind::Ucx);
    let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let mut child = Killed(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["peer_process", "--exact", "--nocapture"])
            .env(PEER_OF, bound.to_string())
            .spawn()
            .unwrap(),
    );
    let (c, _) = compio::time::timeout(Duration::from_secs(30), listener.accept())
        .await
        .expect("the peer process connected")
        .unwrap();
    let (mut r, w) = c.into_split();
    let ready = r.read_exact(vec![0u8; 1]).await;
    ready.0.expect("the peer connected");
    let tx = w.am_sender().expect("a UCX connection");

    // Sent from a registered region: the peer holds its rendezvous request,
    // so the send cannot complete until the peer receives or goes.
    let mut mem = vec![7u8; 1 << 20];
    let region = unsafe { autumn_transport::Region::register(mem.as_mut_ptr(), mem.len()) }.unwrap();
    let (value, mut returned) = unsafe { autumn_transport::lend(mem.as_ptr(), mem.len(), Some(&region)) };
    tx.post_frame_value(5, value);
    let segments = [(1usize << 20, 7)];
    let mut awaited = Box::pin(am::receive_frame_value(&r, &segments, &[]));
    assert!(compio::time::timeout(Duration::from_millis(300), &mut awaited).await.is_err(), "nothing sent");
    assert!(!returned.is_returned(), "the peer holds the value");

    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let killed = Instant::now();
    compio::time::timeout(Duration::from_secs(30), &mut returned).await.expect("the value came back");
    let failed = compio::time::timeout(Duration::from_secs(30), awaited).await.expect("the awaited value failed");
    assert!(failed.is_err(), "the peer sent nothing");
    eprintln!("both ends settled {:?} after the kill", killed.elapsed());
    drop(region);
}
