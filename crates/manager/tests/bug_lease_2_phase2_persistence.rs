//! BUG-LEASE-2 Phase 2 — fence-floor PERSISTENCE across PS restart.
//!
//! The Phase 2 acceptance scenario: a PS crash-restart during a
//! force-revoke window must NOT forget the fence floor — the revoked
//! writer's late RPC must still be rejected with `CODE_FENCED` after
//! the restart.
//!
//! Two recovery paths are exercised:
//! 1. **WAL replay** (`restart_without_flush`): the floor bump lives only
//!    in `OP_FENCE_BUMP` log_stream records; restart replays them.
//! 2. **Checkpoint snapshot** (`restart_after_flush`): a Maintenance FLUSH
//!    publishes a `TableLocations` checkpoint (with `fence_floors`) and
//!    moves the vp head PAST the bump records — restart must seed the
//!    floor from the checkpoint, not the (no longer replayed) records.
//!
//! Also covers the Phase 2 bulk wire change: a stale-epoch `MSG_PUT_BULK`
//! (the path fuse/ioring large writes take) is fenced too.
//!
//! The PS runs as a real `autumn-ps` SUBPROCESS (chaos-test pattern) so
//! the test can kill -9 it and restart with the same psid.

mod support;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{
    self, rkyv_decode, rkyv_encode, PutReq, PutResp, CODE_FENCED, CODE_OK, MSG_PUT, MSG_PUT_BULK,
};

use support::*;

fn ps_binary() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let target = match std::env::var("CARGO_TARGET_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => workspace.join("target"),
    };
    // Pick the NEWEST build across profiles — a stale binary with an old
    // wire layout silently mis-parses the Phase 2 bulk meta.
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for profile in ["debug", "release"] {
        let p = target.join(profile).join("autumn-ps");
        if let Ok(meta) = std::fs::metadata(&p) {
            let mt = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            if best.as_ref().is_none_or(|(bt, _)| mt > *bt) {
                best = Some((mt, p));
            }
        }
    }
    best.map(|(_, p)| p)
        .unwrap_or_else(|| panic!("autumn-ps binary not found under {}", target.display()))
}

fn spawn_ps(psid: u64, mgr: std::net::SocketAddr, ps: std::net::SocketAddr) -> Child {
    Command::new(ps_binary())
        .arg("--psid")
        .arg(psid.to_string())
        .arg("--port")
        .arg(ps.port().to_string())
        .arg("--manager")
        .arg(mgr.to_string())
        .arg("--listen")
        .arg("127.0.0.1")
        .arg("--advertise")
        .arg(ps.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn autumn-ps")
}

async fn put_with_fence(
    ps: &RpcClient,
    part_id: u64,
    key: &[u8],
    inode_hint: u64,
    lease_epoch: u64,
) -> PutResp {
    let payload = rkyv_encode(&PutReq {
        part_id,
        key: key.to_vec(),
        value: b"x".to_vec(),
        expires_at: 0,
        region_epoch: 0,
        inode_hint,
        lease_epoch,
    });
    let bytes = ps.call(MSG_PUT, payload).await.expect("MSG_PUT");
    rkyv_decode::<PutResp>(&bytes).expect("decode PutResp")
}

async fn put_bulk_with_fence(
    ps: &RpcClient,
    part_id: u64,
    key: &[u8],
    inode_hint: u64,
    lease_epoch: u64,
) -> PutResp {
    let meta =
        partition_rpc::encode_put_bulk_meta(part_id, 0, 0, key, inode_hint, lease_epoch);
    let bytes = ps
        .call_vectored(MSG_PUT_BULK, vec![meta, bytes::Bytes::from_static(b"zcval")])
        .await
        .expect("MSG_PUT_BULK");
    rkyv_decode::<PutResp>(&bytes).expect("decode PutResp")
}

#[test]
#[ignore] // requires built binaries + full cluster
fn p2_fence_floor_survives_ps_restart_via_wal_replay() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 161).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        let part_id = 16101u64;
        upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff\xff\xff\xff").await;

        let ps_addr = pick_addr();
        let mut child = spawn_ps(161, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(2500)).await;
        let ps = RpcClient::connect(ps_addr).await.expect("connect PS");

        // Seed + bump the floor: writer A epoch 1, writer B epoch 5.
        let r1 = put_with_fence(&ps, part_id, b"k1", 42, 1).await;
        assert_eq!(r1.code, CODE_OK, "seed: {r1:?}");
        let r2 = put_with_fence(&ps, part_id, b"k2", 42, 5).await;
        assert_eq!(r2.code, CODE_OK, "bump: {r2:?}");

        // bulk stale write is fenced pre-restart (Phase 2 wire change).
        let rz = put_bulk_with_fence(&ps, part_id, b"kz", 42, 1).await;
        assert_eq!(rz.code, CODE_FENCED, "bulk stale must be fenced: {rz:?}");

        // Crash the PS (no flush ran — the floor exists ONLY as
        // OP_FENCE_BUMP records in log_stream).
        child.kill().expect("kill ps");
        let _ = child.wait();
        compio::time::sleep(Duration::from_millis(500)).await;

        // Restart with the same psid; wait for the partition to reopen.
        let mut child2 = spawn_ps(161, mgr_addr, ps_addr);
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let ps2 = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "PS did not become ready within 60s after restart (addr={ps_addr})"
            );
            compio::time::sleep(Duration::from_millis(500)).await;
            if let Ok(c) = RpcClient::connect(ps_addr).await {
                let r = put_with_fence(&c, part_id, b"__probe", 0, 0).await;
                if r.code == CODE_OK {
                    break c;
                }
            }
        };

        // THE ACCEPTANCE: writer A's late stale-epoch RPC after restart
        // must be fenced — the floor was recovered from the WAL replay.
        let r3 = put_with_fence(&ps2, part_id, b"k3", 42, 1).await;
        assert_eq!(
            r3.code, CODE_FENCED,
            "Phase 2: stale epoch must STAY fenced across PS restart (WAL replay): {r3:?}"
        );
        // bulk stale write equally fenced post-restart.
        let rz2 = put_bulk_with_fence(&ps2, part_id, b"kz2", 42, 1).await;
        assert_eq!(rz2.code, CODE_FENCED, "bulk stale post-restart: {rz2:?}");
        // The live writer (epoch 5) keeps working.
        let r4 = put_with_fence(&ps2, part_id, b"k4", 42, 5).await;
        assert_eq!(r4.code, CODE_OK, "live epoch must pass: {r4:?}");

        child2.kill().expect("kill ps2");
        let _ = child2.wait();
    });
}

#[test]
#[ignore] // requires built binaries + full cluster
fn p2_fence_floor_survives_ps_restart_via_checkpoint() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 162).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        let part_id = 16201u64;
        upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff\xff\xff\xff").await;

        let ps_addr = pick_addr();
        let mut child = spawn_ps(162, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(2500)).await;
        let ps = RpcClient::connect(ps_addr).await.expect("connect PS");

        let r1 = put_with_fence(&ps, part_id, b"k1", 77, 9).await;
        assert_eq!(r1.code, CODE_OK, "seed floor=9: {r1:?}");

        // Force a FLUSH so the floor is captured in the TableLocations
        // checkpoint and the vp head moves PAST the OP_FENCE_BUMP record
        // (the WAL-replay path can no longer be the source).
        let m = rkyv_encode(&partition_rpc::MaintenanceReq {
            part_id,
            op: partition_rpc::MAINTENANCE_FLUSH,
            extent_ids: vec![],
            gc_ratio: None,
            gc_max_size: None,
            gc_stream_debt: None,
            gc_empty_only: false,
            op_id: 0,
        });
        let mb = ps
            .call(partition_rpc::MSG_MAINTENANCE, m)
            .await
            .expect("maintenance flush");
        let mr: partition_rpc::MaintenanceResp = rkyv_decode(&mb).expect("decode");
        assert_eq!(mr.code, CODE_OK, "flush: {}", mr.message);
        // Give the checkpoint publish a moment.
        compio::time::sleep(Duration::from_millis(1500)).await;

        child.kill().expect("kill ps");
        let _ = child.wait();
        compio::time::sleep(Duration::from_millis(500)).await;

        let mut child2 = spawn_ps(162, mgr_addr, ps_addr);
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let ps2 = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "PS did not become ready within 60s after restart (addr={ps_addr})"
            );
            compio::time::sleep(Duration::from_millis(500)).await;
            if let Ok(c) = RpcClient::connect(ps_addr).await {
                let r = put_with_fence(&c, part_id, b"__probe", 0, 0).await;
                if r.code == CODE_OK {
                    break c;
                }
            }
        };

        let r2 = put_with_fence(&ps2, part_id, b"k2", 77, 3).await;
        assert_eq!(
            r2.code, CODE_FENCED,
            "Phase 2: floor must come from the CHECKPOINT after flush+restart: {r2:?}"
        );
        let r3 = put_with_fence(&ps2, part_id, b"k3", 77, 9).await;
        assert_eq!(r3.code, CODE_OK, "live epoch must pass: {r3:?}");

        child2.kill().expect("kill ps2");
        let _ = child2.wait();
    });
}
