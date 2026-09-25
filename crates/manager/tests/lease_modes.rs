//! STABLE / REPLACE / EXCLUSIVE inode leases through the SDK against a real
//! manager (memory mode): the S3 gateway pins an object for a GET with
//! STABLE, swaps a name with REPLACE, and reclaims data with EXCLUSIVE.
//!
//! The conflict matrix itself is unit-tested in `inode_lease::tests`; this
//! covers the wire path — the SDK's version gate, the handler accepting the
//! new modes, and a refusal arriving as `AcquireResult::Conflict`.

use std::net::SocketAddr;
use std::time::Duration;

use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_client::ClusterClient;
use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::{
    LEASE_MODE_EXCLUSIVE, LEASE_MODE_READ, LEASE_MODE_REPLACE, LEASE_MODE_STABLE, LEASE_MODE_WRITE,
};

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn start_manager(addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let _ = AutumnManager::new().serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

async fn granted(c: &ClusterClient, id: &DaemonClientId, ino: u64, mode: u8) -> bool {
    match lease::acquire(c, id, ino, mode).await.expect("acquire rpc") {
        AcquireResult::Granted(_) => true,
        AcquireResult::Conflict { .. } => false,
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn stable_replace_and_exclusive_over_the_wire() {
    let addr = pick_addr();
    start_manager(addr);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = ClusterClient::connect_raw(&addr.to_string()).await.expect("connect");
        assert!(c.negotiated_cluster_wire() >= autumn_rpc::WIRE_VERSION_WITH_LEASE_MODES);
        let (get, fuse, put, gc) = (
            DaemonClientId::new("s3-get"),
            DaemonClientId::new_fuse("mount"),
            DaemonClientId::new("s3-put"),
            DaemonClientId::new("reclaim"),
        );

        // A GET pins the content: an in-place writer is refused while it
        // lasts, and an S3 overwrite (REPLACE) is not.
        assert!(granted(&c, &get, 7, LEASE_MODE_STABLE).await);
        assert!(!granted(&c, &fuse, 7, LEASE_MODE_WRITE).await);
        assert!(granted(&c, &put, 7, LEASE_MODE_REPLACE).await);
        // Reclaim waits for every holder.
        assert!(!granted(&c, &gc, 7, LEASE_MODE_EXCLUSIVE).await);
        lease::release(&c, &get, 7).await.unwrap();
        lease::release(&c, &put, 7).await.unwrap();
        assert!(granted(&c, &gc, 7, LEASE_MODE_EXCLUSIVE).await);
        // ... and nobody gets in while it runs, not even a plain reader.
        assert!(!granted(&c, &fuse, 7, LEASE_MODE_READ).await);
        assert!(!granted(&c, &get, 7, LEASE_MODE_STABLE).await);
        lease::release(&c, &gc, 7).await.unwrap();

        // The reverse order: an open writer refuses the GET immediately.
        assert!(granted(&c, &fuse, 8, LEASE_MODE_WRITE).await);
        assert!(!granted(&c, &get, 8, LEASE_MODE_STABLE).await);
        assert!(!granted(&c, &put, 8, LEASE_MODE_REPLACE).await);
        lease::release(&c, &fuse, 8).await.unwrap();
        assert!(granted(&c, &get, 8, LEASE_MODE_STABLE).await);
        // A stable lease heartbeats like any other.
        assert!(matches!(
            lease::heartbeat(&c, &get, 8).await.unwrap(),
            lease::HeartbeatResult::Renewed(_)
        ));
    });
}
