//! A replica reported CORRUPT must eventually be rebuilt.
//!
//! `handle_report_corrupt_replica` isolates the rotted copy by clearing its
//! `avali` bit, and that is the whole of it. Nothing marks the slot as needing
//! a rebuild, and nothing fences the node — so under the default
//! `fenced_only` recovery gate the dispatch loop skips that slot forever:
//!
//! ```text
//! if gate_mode == FencedOnly && !is_fenced { continue; }   // before avali is even read
//! ```
//!
//! The extent is then permanently short one usable replica. Durability quietly
//! drops to RF-1 with no alarm, no repair, and no path back except an operator
//! noticing and fencing the node by hand. Isolation without repair is a leak.
//!
//! Corruption is a STRONGER signal than the conditions that do trigger a
//! rebuild (an offline disk, a fenced node): the owner replayed those bytes and
//! proved them wrong. It should not need a weaker signal to be acted on.
//!
//! NON-VACUITY: the cluster shape here is identical to
//! `system_recovery_loop_drives.rs`, which pins that this configuration — three
//! nodes each wired to the manager, RF=2, so a non-member target exists — does
//! drive a rebuild to completion when a member is fenced. So "no rebuild
//! happened" below is a fact about the corrupt path, not about an inert loop.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 901;

async fn extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode extent_info");
    r.extent.expect("extent present")
}

fn spawn_en(addr: SocketAddr, disk_id: u64, mgr: SocketAddr) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tmpdir");
    start_extent_node_with_manager(addr, dir.path().to_path_buf(), disk_id, mgr);
    dir
}

#[test]
fn a_replica_reported_corrupt_is_eventually_rebuilt() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let a = pick_addr();
    let b = pick_addr();
    let c = pick_addr();
    let _da = spawn_en(a, 1, mgr_addr);
    let _db = spawn_en(b, 2, mgr_addr);
    let _dc = spawn_en(c, 3, mgr_addr); // spare target for the rebuild

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        for (i, addr) in [a, b, c].iter().enumerate() {
            register_node(&mgr, &addr.to_string(), &format!("uuid-cr-{i}")).await;
        }

        // The corrupt report is fenced on `owner_epochs["partition/<id>"]`, and
        // a StreamClient opened with that owner_key holds exactly that epoch.
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            format!("partition/{PART}"),
            1 << 20,
            pool,
        )
        .await
        .expect("connect sc");

        let (log, row, meta) = create_three_streams(&mgr).await; // RF=2
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;

        // Seal a log extent: the report is only accepted for a sealed extent.
        let payload = vec![0xC7u8; 64 * 1024];
        let appended = sc.append(log, &payload).await.expect("append");
        let extent_id = appended.extent_id;
        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id: log,
                    owner_key: sc.owner_key().to_string(),
                    owner_epoch: sc.owner_epoch(),
                    seal_commit: Some(appended.end),
                    exclude_node_ids: vec![],
                    seal_extent_id: extent_id,
                }),
            )
            .await
            .expect("seal");
        let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
        assert_eq!(seal.code, CODE_OK, "seal failed: {}", seal.message);

        let before = extent_info(&mgr, extent_id).await;
        assert!(before.sealed, "extent must be sealed to be reportable");
        assert_eq!(before.replicates.len(), 2, "RF=2 expected");
        let victim = before.replicates[0];

        let resp = mgr
            .call(
                MSG_REPORT_CORRUPT_REPLICA,
                rkyv_encode(&ReportCorruptReplicaReq {
                    partition_id: PART,
                    owner_epoch: sc.owner_epoch(),
                    log_stream_id: log,
                    extent_id,
                    eversion: before.eversion,
                    corrupt_node_ids: vec![victim],
                }),
            )
            .await
            .expect("report corrupt");
        let rep: ReportCorruptReplicaResp = rkyv_decode(&resp).expect("decode report");
        assert_eq!(rep.code, CODE_OK, "report refused: {}", rep.message);

        assert_eq!(
            extent_info(&mgr, extent_id).await.avali & 1,
            0,
            "the report must isolate the victim slot"
        );

        // Isolation is only half the job. The rotted copy must also be replaced,
        // or the extent stays at RF-1 indefinitely.
        let mut rebuilt = false;
        for _ in 0..60 {
            compio::time::sleep(Duration::from_millis(500)).await;
            if !extent_info(&mgr, extent_id)
                .await
                .replicates
                .contains(&victim)
            {
                rebuilt = true;
                break;
            }
        }
        assert!(
            rebuilt,
            "node {victim} was reported holding CORRUPT bytes for extent {extent_id} and \
             was isolated, but 30 s later it is still a member and nothing rebuilt it. \
             The report clears the avali bit and records nothing else, so the dispatch \
             loop's `fenced_only` gate skips the slot before it ever looks at avali — \
             the extent is left permanently at RF-1 with no repair path"
        );
    });
}
