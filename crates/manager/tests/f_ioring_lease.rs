//! F-ioring-lease-1 — end-to-end RPC validation for the inode-lease
//! handlers. Single in-process AutumnManager (memory-only, no etcd),
//! real autumn-rpc TCP client. Covers:
//!
//! 1. Two writers from different clients on the same ino: second
//!    AcquireLease returns CODE_PRECONDITION with a useful message.
//! 2. Writer release bumps `version` and a reader's
//!    PollInvalidations sees the event (close-to-open coherence).
//! 3. Heartbeat round-trips on a held lease; returns CODE_NOT_FOUND
//!    after the lease is released.
//!
//! Time-based TTL revoke is exercised in `inode_lease::tests` (pure
//! state machine; no need to drive the 1s background loop here).

use std::net::SocketAddr;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn start_manager(addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

fn cid(kind: u8, byte: u8, host: &str) -> MgrClientId {
    MgrClientId {
        kind,
        uuid: [byte; 16],
        host: host.to_string(),
    }
}

async fn acquire(c: &RpcClient, req: &AcquireLeaseReq) -> AcquireLeaseResp {
    let bytes = c
        .call(MSG_ACQUIRE_LEASE, rkyv_encode(req))
        .await
        .expect("acquire rpc");
    rkyv_decode(&bytes).expect("decode AcquireLeaseResp")
}

async fn release(c: &RpcClient, req: &ReleaseLeaseReq) -> ReleaseLeaseResp {
    let bytes = c
        .call(MSG_RELEASE_LEASE, rkyv_encode(req))
        .await
        .expect("release rpc");
    rkyv_decode(&bytes).expect("decode ReleaseLeaseResp")
}

async fn heartbeat(c: &RpcClient, req: &HeartbeatLeaseReq) -> HeartbeatLeaseResp {
    let bytes = c
        .call(MSG_HEARTBEAT_LEASE, rkyv_encode(req))
        .await
        .expect("heartbeat rpc");
    rkyv_decode(&bytes).expect("decode HeartbeatLeaseResp")
}

async fn poll(c: &RpcClient, req: &PollInvalidationsReq) -> PollInvalidationsResp {
    let bytes = c
        .call(MSG_POLL_INVALIDATIONS, rkyv_encode(req))
        .await
        .expect("poll rpc");
    rkyv_decode(&bytes).expect("decode PollInvalidationsResp")
}

#[test]
fn ioring_lease_write_conflict_returns_precondition() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");
        let w1 = cid(LEASE_CLIENT_KIND_IORING, 0xa1, "host-a");
        let w2 = cid(LEASE_CLIENT_KIND_FUSE, 0xb2, "host-b");

        let r1 = acquire(
            &mgr,
            &AcquireLeaseReq {
                client: w1.clone(),
                ino: 42,
                mode: LEASE_MODE_WRITE,
            },
        )
        .await;
        assert_eq!(r1.code, CODE_OK, "first writer must win");
        let lease = r1.lease.expect("lease present");
        assert!(lease.writer_present);
        assert_eq!(lease.version, 1);
        assert_eq!(lease.ttl_secs, autumn_manager::inode_lease::DEFAULT_LEASE_TTL_SECS);

        let r2 = acquire(
            &mgr,
            &AcquireLeaseReq {
                client: w2.clone(),
                ino: 42,
                mode: LEASE_MODE_WRITE,
            },
        )
        .await;
        assert_eq!(r2.code, CODE_PRECONDITION, "second writer must lose");
        assert!(r2.message.contains("host-a"), "msg={}", r2.message);
        assert!(r2.lease.is_none());
    });
}

#[test]
fn ioring_lease_writer_close_bumps_version_and_reader_polls_event() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");
        let writer = cid(LEASE_CLIENT_KIND_IORING, 0x11, "writer-host");
        let reader = cid(LEASE_CLIENT_KIND_IORING, 0x22, "reader-host");

        // Reader subscribes first so the writer-close push lands in
        // its inbox.
        let r = acquire(
            &mgr,
            &AcquireLeaseReq {
                client: reader.clone(),
                ino: 7,
                mode: LEASE_MODE_READ,
            },
        )
        .await;
        assert_eq!(r.code, CODE_OK);
        let v_before = r.lease.expect("lease").version;

        // Writer acquires + immediately releases.
        let r = acquire(
            &mgr,
            &AcquireLeaseReq {
                client: writer.clone(),
                ino: 7,
                mode: LEASE_MODE_WRITE,
            },
        )
        .await;
        assert_eq!(r.code, CODE_OK);

        let r = release(
            &mgr,
            &ReleaseLeaseReq {
                client: writer.clone(),
                ino: 7,
            },
        )
        .await;
        assert_eq!(r.code, CODE_OK);
        assert_eq!(
            r.new_version,
            Some(v_before + 1),
            "writer-close must bump version"
        );

        // Reader polls — exactly one WriterClosed event for ino=7.
        let r = poll(
            &mgr,
            &PollInvalidationsReq {
                client: reader.clone(),
            },
        )
        .await;
        assert_eq!(r.code, CODE_OK);
        assert_eq!(r.events.len(), 1, "events={:?}", r.events);
        let ev = &r.events[0];
        assert_eq!(ev.ino, 7);
        assert_eq!(ev.version, v_before + 1);
        assert_eq!(ev.kind, LEASE_INVAL_WRITER_CLOSED);

        // Second poll on the same reader returns empty.
        let r = poll(
            &mgr,
            &PollInvalidationsReq {
                client: reader.clone(),
            },
        )
        .await;
        assert_eq!(r.code, CODE_OK);
        assert!(r.events.is_empty(), "drain must be idempotent");
    });
}

#[test]
fn ioring_lease_heartbeat_then_release_round_trip() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");
        let w = cid(LEASE_CLIENT_KIND_FUSE, 0xcc, "fuse-host");

        let r = acquire(
            &mgr,
            &AcquireLeaseReq {
                client: w.clone(),
                ino: 100,
                mode: LEASE_MODE_WRITE,
            },
        )
        .await;
        assert_eq!(r.code, CODE_OK);

        let r = heartbeat(
            &mgr,
            &HeartbeatLeaseReq {
                client: w.clone(),
                ino: 100,
            },
        )
        .await;
        assert_eq!(r.code, CODE_OK);
        let lease = r.lease.expect("renewed");
        assert!(lease.writer_present);
        assert_eq!(lease.ino, 100);

        let r = release(
            &mgr,
            &ReleaseLeaseReq {
                client: w.clone(),
                ino: 100,
            },
        )
        .await;
        assert_eq!(r.code, CODE_OK);

        let r = heartbeat(
            &mgr,
            &HeartbeatLeaseReq {
                client: w.clone(),
                ino: 100,
            },
        )
        .await;
        assert_eq!(r.code, CODE_NOT_FOUND, "post-release heartbeat must surface NotFound");
        assert!(r.lease.is_none());
    });
}
