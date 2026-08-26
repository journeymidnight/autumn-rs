//! Baseline capability: can an integration test actually make the manager's
//! recovery dispatch loop DO something?
//!
//! Several invariants worth asserting (corrupt-replica isolation, re_avali
//! behaviour, slot rebuild) are only meaningful if the loop acts. A test that
//! asserts "nothing bad happened" while the loop is inert passes vacuously —
//! which is exactly what happened when this was first attempted with two extent
//! nodes and RF=2.
//!
//! Two conditions have to hold, and neither is a bug:
//!   - the node must know its MANAGER ENDPOINT. `handle_require_recovery`
//!     validates it before anything else and refuses when unset, so a node
//!     started without one looks like a valid candidate and rejects every
//!     dispatch ("all recovery candidates rejected").
//!   - a NON-member node must exist to rebuild onto. With two nodes at RF=2
//!     every node is already a member, so there is no target.
//!
//! Two things are pinned here, and the second is what makes the first
//! trustworthy:
//!   1. with a spare node, fencing a member drives a rebuild to completion;
//!   2. without one, the loop is inert — so any future "nothing happened"
//!      assertion MUST supply a spare node or it proves nothing.

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

/// Seed a sealed log extent owned by `PART`, returning its id.
async fn sealed_extent(mgr: &RpcClient, sc: &StreamClient, log: u64) -> u64 {
    let payload = vec![0xC7u8; 64 * 1024];
    let appended = sc.append(log, &payload).await.expect("append");
    let resp = mgr
        .call(
            MSG_STREAM_ALLOC_EXTENT,
            rkyv_encode(&StreamAllocExtentReq {
                stream_id: log,
                owner_key: sc.owner_key().to_string(),
                owner_epoch: sc.owner_epoch(),
                seal_commit: Some(appended.end),
                exclude_node_ids: vec![],
                seal_extent_id: appended.extent_id,
            }),
        )
        .await
        .expect("seal");
    let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
    assert_eq!(seal.code, CODE_OK, "seal failed: {}", seal.message);
    appended.extent_id
}

async fn fence(mgr: &RpcClient, node_id: u64) {
    let resp = mgr
        .call(
            MSG_FENCE_NODE,
            rkyv_encode(&FenceNodeReq {
                node_id,
                reason: "recovery-loop drive test".to_string(),
                set_by: "test".to_string(),
                force: true,
            }),
        )
        .await
        .expect("fence");
    let f: CodeResp = rkyv_decode(&resp).expect("decode fence");
    assert_eq!(f.code, CODE_OK, "fence failed: {}", f.message);
}

/// Wait until `victim` is no longer a member of the extent.
async fn wait_rebuilt(mgr: &RpcClient, extent_id: u64, victim: u64, secs: u64) -> bool {
    for _ in 0..(secs * 2) {
        compio::time::sleep(Duration::from_millis(500)).await;
        let e = extent_info(mgr, extent_id).await;
        if !e.replicates.contains(&victim) {
            return true;
        }
    }
    false
}

fn spawn_en(addr: SocketAddr, disk_id: u64, mgr: SocketAddr) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tmpdir");
    start_extent_node_with_manager(addr, dir.path().to_path_buf(), disk_id, mgr);
    dir
}

#[test]
fn fencing_a_member_rebuilds_the_slot_when_a_spare_node_exists() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let a = pick_addr();
    let b = pick_addr();
    let c = pick_addr();
    let _da = spawn_en(a, 1, mgr_addr);
    let _db = spawn_en(b, 2, mgr_addr);
    let _dc = spawn_en(c, 3, mgr_addr); // the spare — recovery needs a NON-member target

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        for (i, addr) in [a, b, c].iter().enumerate() {
            register_node(&mgr, &addr.to_string(), &format!("uuid-drv-{i}")).await;
        }

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
        let extent_id = sealed_extent(&mgr, &sc, log).await;

        let before = extent_info(&mgr, extent_id).await;
        assert_eq!(before.replicates.len(), 2, "RF=2 expected");
        let victim = before.replicates[0];

        fence(&mgr, victim).await;

        assert!(
            wait_rebuilt(&mgr, extent_id, victim, 30).await,
            "fenced node {victim} is still a member of extent {extent_id} after 30 s — \
             the recovery dispatch loop did not complete a rebuild even with a spare \
             node available; any test asserting on recovery behaviour would be vacuous"
        );
    });
}

#[test]
fn the_loop_is_inert_without_a_spare_node() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let a = pick_addr();
    let b = pick_addr();
    let _da = spawn_en(a, 1, mgr_addr);
    let _db = spawn_en(b, 2, mgr_addr); // no spare: every node is already a member

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, a, b, 88).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            format!("partition/{PART}"),
            1 << 20,
            pool,
        )
        .await
        .expect("connect sc");

        let (log, row, meta) = create_three_streams(&mgr).await; // RF=2 over 2 nodes
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
        let extent_id = sealed_extent(&mgr, &sc, log).await;

        let before = extent_info(&mgr, extent_id).await;
        let victim = before.replicates[0];
        fence(&mgr, victim).await;

        // Documents WHY the first attempt at a corrupt-isolation test passed
        // vacuously: with no target, the loop cannot rebuild, so "the membership
        // did not change" is guaranteed regardless of the behaviour under test.
        assert!(
            !wait_rebuilt(&mgr, extent_id, victim, 8).await,
            "a rebuild completed with no spare node — then the inertia this test \
             documents is gone and the caution it encodes can be dropped"
        );
    });
}
