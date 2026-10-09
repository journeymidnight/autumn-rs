//! Rebuilding a degraded copy on another node NOW, without fencing its node.
//!
//! The recovery loop on its own moves a copy only on conclusive evidence (a
//! fenced node, a corrupt slot, a faulted disk): a node that stopped answering
//! may be back in seconds. A repair request is someone deciding about the
//! EXTENT instead — an operator (`autumn-op repair`) or the repair policy once a
//! slot has stayed degraded past its grace period — and it survives a leader
//! change like the rebuild it schedules.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::version_hello::Role;
use autumn_stream::{ConnPool, StreamClient};
use support::*;

/// A stoppable extent node that knows its manager (a rebuild target asks it
/// for the extent's layout).
fn start_node(
    addr: SocketAddr,
    dir: std::path::PathBuf,
    disk_id: u64,
    mgr: String,
) -> (ShutdownFlag, std::thread::JoinHandle<()>) {
    let flag = ShutdownFlag::new();
    let stop = flag.clone();
    let handle = std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let cfg =
                    autumn_stream::ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr);
                let node = autumn_stream::ExtentNode::new(cfg)
                    .await
                    .expect("extent node");
                compio::runtime::spawn(async move {
                    if let Err(e) = node.serve(addr).await {
                        eprintln!("extent node {addr} stopped serving: {e}");
                    }
                })
                .detach();
                while !stop.is_shutdown() {
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
            });
    });
    std::thread::sleep(Duration::from_millis(200));
    (flag, handle)
}

/// An in-memory manager with a short repair grace and a 1 s policy tick.
fn start_fast_policy_manager(addr: SocketAddr, grace_secs: u64) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let m = AutumnManager::new();
            m.set_repair_grace_secs(grace_secs);
            m.set_policy_config(autumn_manager::policy::PolicyConfig {
                tick_interval_sec: 1,
                ..Default::default()
            });
            if let Err(e) = m.serve(addr).await {
                eprintln!("manager stopped serving: {e}");
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

/// Append one record to a fresh RF 3 stream and seal it with every member
/// answering. Returns the extent and its members.
async fn sealed_rf3_extent(mgr_addr: SocketAddr, mgr: &RpcClient, owner: &str) -> (u64, Vec<u64>) {
    let stream_id = create_stream(mgr, 3).await;
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        owner.to_string(),
        1 << 20,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    let appended = sc
        .append(stream_id, &[0x6b_u8; 8192])
        .await
        .expect("append");
    let resp = mgr
        .call(
            MSG_STREAM_ALLOC_EXTENT,
            rkyv_encode(&StreamAllocExtentReq {
                stream_id,
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
    let ex = extent_info(mgr, appended.extent_id).await;
    assert!(ex.sealed && ex.avali == 0b111, "{ex:?}");
    (appended.extent_id, ex.replicates)
}

async fn extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode extent info");
    r.extent.expect("extent exists")
}

async fn summary(mgr: &RpcClient) -> ExtentHealthSummaryResp {
    let resp = mgr
        .call(
            MSG_EXTENT_HEALTH_SUMMARY,
            rkyv_encode(&ExtentHealthSummaryReq { max_problems: 10 }),
        )
        .await
        .expect("health summary");
    rkyv_decode(&resp).expect("decode summary")
}

/// Wait until the manager sees the extent degraded (the node's slot is not
/// serving), so a repair has something to act on.
async fn wait_degraded(mgr: &RpcClient, extent_id: u64) {
    let start = Instant::now();
    loop {
        let r = summary(mgr).await;
        if r.problems.iter().any(|p| p.extent_id == extent_id) {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "extent never read as degraded: {r:?}"
        );
        compio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Wait until `gone` is no longer a member and every slot is available again.
async fn wait_replaced(
    mgr: &RpcClient,
    extent_id: u64,
    gone: u64,
    secs: u64,
    what: &str,
) -> MgrExtentInfo {
    let start = Instant::now();
    loop {
        let ex = extent_info(mgr, extent_id).await;
        if !ex.replicates.contains(&gone) && ex.avali == 0b111 {
            return ex;
        }
        assert!(
            start.elapsed() < Duration::from_secs(secs),
            "{what}: {secs} s on, node {gone} is still in {:?} (avali {:#b})",
            ex.replicates,
            ex.avali
        );
        compio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn submit_repair(admin: &RpcClient, extent_ids: Vec<u64>) -> OpRecord {
    let resp = admin
        .call(
            MSG_OP_SUBMIT,
            rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_REPAIR,
                secondary_id: extent_ids[0],
                extent_ids,
                requested_by: "test".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("submit repair");
    let r: OpSubmitResp = rkyv_decode(&resp).expect("decode submit");
    assert_eq!(r.code, CODE_OK, "repair refused: {}", r.message);
    let start = Instant::now();
    loop {
        let resp = admin
            .call(
                MSG_OP_QUERY,
                rkyv_encode(&OpQueryReq {
                    op_id: r.op_id,
                    ..Default::default()
                }),
            )
            .await
            .expect("query op");
        let q: OpQueryResp = rkyv_decode(&resp).expect("decode op query");
        if let Some(op) = q
            .ops
            .into_iter()
            .find(|o| o.state != OP_STATE_RUNNING && o.state != OP_STATE_PENDING)
        {
            return op;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "repair op never finished"
        );
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The repair ops the controller submitted for `node`, as the ledger lists them.
async fn policy_repair_ops(admin: &RpcClient, node: u64) -> Vec<OpRecord> {
    let resp = admin
        .call(
            MSG_OP_QUERY,
            rkyv_encode(&OpQueryReq {
                kind_filter: OP_KIND_REPAIR,
                ..Default::default()
            }),
        )
        .await
        .expect("query ops");
    let q: OpQueryResp = rkyv_decode(&resp).expect("decode op query");
    q.ops
        .into_iter()
        .filter(|o| o.requested_by == "auto-policy" && o.part_id == node)
        .collect()
}

fn no_override(mgr_states: &ListNodeStatesResp, node: u64) -> bool {
    mgr_states
        .nodes
        .iter()
        .find(|n| n.node_id == node)
        .is_some_and(|n| n.override_kind == NODE_OVERRIDE_NONE)
}

async fn node_states(mgr: &RpcClient) -> ListNodeStatesResp {
    let resp = mgr
        .call(MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {}))
        .await
        .expect("list node states");
    rkyv_decode(&resp).expect("decode node states")
}

/// An operator asks for a repair of one extent whose replica sits on a node
/// that stopped: the copy is rebuilt on the spare node, and the node is never
/// fenced. Without the request the loop would leave it there — a silent node
/// may come back.
#[test]
fn an_operator_repair_rebuilds_a_degraded_copy_without_a_fence() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs: Vec<_> = (0..4)
        .map(|_| tempfile::tempdir().expect("tmpdir"))
        .collect();
    let addrs: Vec<_> = (0..4).map(|_| pick_addr()).collect();
    let disks: Vec<u64> = (0..4)
        .map(|i| format_node(mgr_addr, addrs[i], &format!("uuid-repair-op-{i}")))
        .collect();
    let mut nodes: Vec<_> = (0..4)
        .map(|i| {
            Some(start_node(
                addrs[i],
                dirs[i].path().to_path_buf(),
                disks[i],
                mgr_addr.to_string(),
            ))
        })
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None)
            .await
            .expect("connect mgr");
        let mut node_ids = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            node_ids.push(
                register_node(&admin, &addr.to_string(), &format!("uuid-repair-op-{i}"))
                    .await
                    .node_id,
            );
        }
        let (extent, members) = sealed_rf3_extent(mgr_addr, &admin, "repair-op/owner").await;
        let victim = node_ids.iter().position(|n| *n == members[1]).unwrap();
        let (flag, handle) = nodes[victim].take().unwrap();
        flag.shutdown();
        handle.join().expect("join extent node");
        wait_degraded(&admin, extent).await;

        let op = submit_repair(&admin, vec![extent]).await;
        assert_eq!(op.state, OP_STATE_SUCCEEDED, "{op:?}");
        assert!(
            op.message.contains("1 slot(s) on 1 extent(s)"),
            "{}",
            op.message
        );

        wait_replaced(&admin, extent, members[1], 60, "after the repair request").await;
        assert!(
            no_override(&node_states(&admin).await, members[1]),
            "the repaired node must not have been fenced"
        );
        // A healthy extent has nothing to repair, and says so.
        let resp = admin
            .call(
                MSG_OP_SUBMIT,
                rkyv_encode(&OpSubmitReq {
                    kind: OP_KIND_REPAIR,
                    secondary_id: extent,
                    extent_ids: vec![extent],
                    requested_by: "test".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("submit repair");
        let r: OpSubmitResp = rkyv_decode(&resp).expect("decode submit");
        assert_eq!(r.code, CODE_OK);
    });
    drop(nodes);
}

/// The repair policy: selected but Off, the policy engine still advises (one
/// row per node, naming it — the controller itself does nothing while Off) and
/// nothing moves; Armed, it rebuilds the copy once the slot has been degraded
/// for the grace period.
#[test]
fn the_repair_policy_advises_while_off_and_rebuilds_when_armed() {
    let mgr_addr = pick_addr();
    start_fast_policy_manager(mgr_addr, 2);
    let dirs: Vec<_> = (0..4)
        .map(|_| tempfile::tempdir().expect("tmpdir"))
        .collect();
    let addrs: Vec<_> = (0..4).map(|_| pick_addr()).collect();
    let disks: Vec<u64> = (0..4)
        .map(|i| format_node(mgr_addr, addrs[i], &format!("uuid-repair-pol-{i}")))
        .collect();
    let mut nodes: Vec<_> = (0..4)
        .map(|i| {
            Some(start_node(
                addrs[i],
                dirs[i].path().to_path_buf(),
                disks[i],
                mgr_addr.to_string(),
            ))
        })
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None)
            .await
            .expect("connect mgr");
        let mut node_ids = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            node_ids.push(
                register_node(&admin, &addr.to_string(), &format!("uuid-repair-pol-{i}"))
                    .await
                    .node_id,
            );
        }
        let (extent, members) = sealed_rf3_extent(mgr_addr, &admin, "repair-policy/owner").await;

        // A custom policy with only the repair switch, re-deciding every 2 s.
        let set = |op: u8, mode: u8, name: &str, entry: Option<MgrAutoPolicyEntry>| {
            let admin = &admin;
            let name = name.to_string();
            async move {
                let resp = admin
                    .call(
                        MSG_AUTOPOLICY_SET,
                        rkyv_encode(&AutoPolicySetReq {
                            op,
                            mode,
                            name,
                            entry,
                        }),
                    )
                    .await
                    .expect("autopolicy set");
                let r: AutoPolicySetResp = rkyv_decode(&resp).expect("decode autopolicy set");
                assert_eq!(r.code, CODE_OK, "autopolicy set: {}", r.message);
            }
        };
        let mut switches = vec![false; 7];
        switches[6] = true;
        set(
            AUTOPOLICY_OP_UPSERT,
            0,
            "repair-only",
            Some(MgrAutoPolicyEntry {
                name: "repair-only".to_string(),
                desc: "test".to_string(),
                switches,
                interval_sec: 2,
                cooldown_sec: 0,
                max_actions: 5,
                builtin: false,
            }),
        )
        .await;
        set(AUTOPOLICY_OP_SET_ACTIVE, 0, "repair-only", None).await; // mode stays Off

        let victim = node_ids.iter().position(|n| *n == members[1]).unwrap();
        let (flag, handle) = nodes[victim].take().unwrap();
        flag.shutdown();
        handle.join().expect("join extent node");

        // Off: the advisory appears, naming the node — and nothing moves.
        let start = Instant::now();
        loop {
            let resp = admin
                .call(
                    MSG_GET_POLICY_CANDIDATES,
                    rkyv_encode(&GetPolicyCandidatesReq {}),
                )
                .await
                .expect("candidates");
            let c: GetPolicyCandidatesResp = rkyv_decode(&resp).expect("decode candidates");
            if c.candidates
                .iter()
                .any(|c| c.kind == POLICY_KIND_REPAIR && c.secondary_part_id == members[1])
            {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "no repair advisory for node {}",
                members[1]
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        compio::time::sleep(Duration::from_secs(6)).await;
        assert!(
            extent_info(&admin, extent)
                .await
                .replicates
                .contains(&members[1]),
            "a stopped policy must not move anything"
        );
        assert!(
            policy_repair_ops(&admin, members[1]).await.is_empty(),
            "a stopped policy submits no op"
        );

        set(AUTOPOLICY_OP_SET_MODE, 2, "", None).await; // Armed
        wait_replaced(
            &admin,
            extent,
            members[1],
            60,
            "with the repair policy armed",
        )
        .await;
        assert!(
            no_override(&node_states(&admin).await, members[1]),
            "the policy repairs extents, it does not fence the node"
        );
        // The armed policy's action is an op in the ledger, like an operator's.
        let ops = policy_repair_ops(&admin, members[1]).await;
        assert!(
            ops.iter().any(|o| o.state == OP_STATE_SUCCEEDED),
            "no succeeded auto-policy repair op for node {}: {ops:?}",
            members[1]
        );
    });
    drop(nodes);
}

/// A repair request survives a leader change. Three nodes hold the RF 3 extent
/// and there is no spare, so the request recorded on the first leader cannot
/// be served there; after failover a spare joins and the NEW leader rebuilds
/// the copy on it — which it would never do on its own, since the silent node
/// is not fenced.
#[test]
fn a_repair_request_survives_a_leader_change() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;
        let mgr1_addr = pick_addr();
        let m1 = start_etcd_manager_stoppable(mgr1_addr, etcd_endpoint.clone());
        let mgr2_addr = pick_addr();
        drop(start_etcd_manager_stoppable(
            mgr2_addr,
            etcd_endpoint.clone(),
        ));
        let admin1 = RpcClient::connect_as(mgr1_addr, Role::Admin, None)
            .await
            .expect("connect mgr1");

        let dirs: Vec<_> = (0..4)
            .map(|_| tempfile::tempdir().expect("tmpdir"))
            .collect();
        let addrs: Vec<_> = (0..4).map(|_| pick_addr()).collect();
        let mut nodes = Vec::new();
        let mut node_ids = Vec::new();
        for i in 0..3 {
            let r = register_node(
                &admin1,
                &addrs[i].to_string(),
                &format!("uuid-repair-ha-{i}"),
            )
            .await;
            node_ids.push(r.node_id);
            let disk = r.disk_uuids[0].1;
            nodes.push(Some(start_node(
                addrs[i],
                dirs[i].path().to_path_buf(),
                disk,
                mgr1_addr.to_string(),
            )));
        }
        compio::time::sleep(Duration::from_secs(3)).await;
        let (extent, members) = sealed_rf3_extent(mgr1_addr, &admin1, "repair-ha/owner").await;
        let victim = node_ids.iter().position(|n| *n == members[1]).unwrap();
        let (flag, handle) = nodes[victim].take().unwrap();
        flag.shutdown();
        handle.join().expect("join extent node");
        wait_degraded(&admin1, extent).await;
        let op = submit_repair(&admin1, vec![extent]).await;
        assert_eq!(op.state, OP_STATE_SUCCEEDED, "{op:?}");
        compio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            extent_info(&admin1, extent)
                .await
                .replicates
                .contains(&members[1]),
            "no spare node: nothing can be rebuilt yet"
        );

        // Fail over to M2.
        let (flag, handle) = m1;
        flag.shutdown();
        handle.join().expect("join M1");
        let admin2 = RpcClient::connect_as(mgr2_addr, Role::Admin, None)
            .await
            .expect("connect mgr2");
        let start = Instant::now();
        while summary(&admin2).await.code != CODE_OK {
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "M2 never took over"
            );
            compio::time::sleep(Duration::from_millis(300)).await;
        }

        // A spare joins the new leader; the request recorded on M1 is served.
        let r = register_node(&admin2, &addrs[3].to_string(), "uuid-repair-ha-3").await;
        nodes.push(Some(start_node(
            addrs[3],
            dirs[3].path().to_path_buf(),
            r.disk_uuids[0].1,
            mgr2_addr.to_string(),
        )));
        let ex = wait_replaced(&admin2, extent, members[1], 90, "after failover").await;
        assert!(
            ex.replicates.contains(&r.node_id),
            "rebuilt onto the spare: {:?}",
            ex.replicates
        );
        drop(nodes);
    });
}

/// An etcd-backed manager whose thread ends when the flag is set (a crash to
/// the standby: its lease is not revoked).
fn start_etcd_manager_stoppable(
    mgr_addr: SocketAddr,
    etcd_endpoint: String,
) -> (ShutdownFlag, std::thread::JoinHandle<()>) {
    let flag = ShutdownFlag::new();
    let flag_thread = flag.clone();
    let handle = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint], support::manager_identity())
                .await
                .expect("new manager with etcd");
            let serve = manager.serve(mgr_addr);
            let stop = async {
                while !flag_thread.is_shutdown() {
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            futures::pin_mut!(serve, stop);
            if let futures::future::Either::Left((r, _)) =
                futures::future::select(serve, stop).await
            {
                panic!("manager serve ended on its own: {r:?}");
            }
        });
    });
    std::thread::sleep(Duration::from_millis(300));
    (flag, handle)
}

/// Select a policy with only the repair switch, re-deciding every 2 s, in
/// `mode` (0 = Off, 2 = Armed).
async fn repair_only_policy(admin: &RpcClient, mode: u8) {
    let set = |op: u8, mode: u8, name: &str, entry: Option<MgrAutoPolicyEntry>| {
        let name = name.to_string();
        async move {
            let resp = admin
                .call(
                    MSG_AUTOPOLICY_SET,
                    rkyv_encode(&AutoPolicySetReq {
                        op,
                        mode,
                        name,
                        entry,
                    }),
                )
                .await
                .expect("autopolicy set");
            let r: AutoPolicySetResp = rkyv_decode(&resp).expect("decode autopolicy set");
            assert_eq!(r.code, CODE_OK, "autopolicy set: {}", r.message);
        }
    };
    let mut switches = vec![false; 7];
    switches[6] = true;
    let entry = MgrAutoPolicyEntry {
        name: "repair-only".to_string(),
        desc: "test".to_string(),
        switches,
        interval_sec: 2,
        cooldown_sec: 0,
        max_actions: 5,
        builtin: false,
    };
    set(AUTOPOLICY_OP_UPSERT, 0, "repair-only", Some(entry)).await;
    set(AUTOPOLICY_OP_SET_ACTIVE, 0, "repair-only", None).await;
    set(AUTOPOLICY_OP_SET_MODE, mode, "", None).await;
}

/// The policy recorded repair requests for a node's copies, and the node came
/// back before any could be served (the only spare was down too). The copies
/// are where they belong again: the requests are withdrawn, and when the spare
/// returns nothing is moved to it. A request that outlived its premise moved
/// the copies of a node that was never lost — or, with no spare at all, pinned
/// them to a rebuild with no target forever.
#[test]
fn a_node_that_returns_keeps_its_copies_despite_repair_requests() {
    let mgr_addr = pick_addr();
    start_fast_policy_manager(mgr_addr, 2);
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().expect("tmpdir")).collect();
    let addrs: Vec<_> = (0..4).map(|_| pick_addr()).collect();
    let disks: Vec<u64> = (0..4)
        .map(|i| format_node(mgr_addr, addrs[i], &format!("uuid-repair-back-{i}")))
        .collect();
    let mut nodes: Vec<_> = (0..4)
        .map(|i| {
            Some(start_node(
                addrs[i],
                dirs[i].path().to_path_buf(),
                disks[i],
                mgr_addr.to_string(),
            ))
        })
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None)
            .await
            .expect("connect mgr");
        let mut node_ids = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            node_ids.push(
                register_node(&admin, &addr.to_string(), &format!("uuid-repair-back-{i}"))
                    .await
                    .node_id,
            );
        }
        let (extent, members) = sealed_rf3_extent(mgr_addr, &admin, "repair-back/owner").await;
        let c = node_ids.iter().position(|n| *n == members[1]).unwrap();
        let d = (0..4).find(|i| !members.contains(&node_ids[*i])).unwrap();
        repair_only_policy(&admin, 2).await;

        for i in [c, d] {
            let (flag, handle) = nodes[i].take().unwrap();
            flag.shutdown();
            handle.join().expect("join extent node");
        }
        // The armed policy records the request (it cannot be served: the
        // only spare is down too).
        let start = Instant::now();
        loop {
            let ops = policy_repair_ops(&admin, members[1]).await;
            if ops.iter().any(|o| o.state == OP_STATE_SUCCEEDED) {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "the armed policy never requested a repair for node {}: {ops:?}",
                members[1]
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        }

        // C returns first, then the spare.
        nodes[c] = Some(start_node(
            addrs[c],
            dirs[c].path().to_path_buf(),
            disks[c],
            mgr_addr.to_string(),
        ));
        compio::time::sleep(Duration::from_secs(8)).await;
        nodes[d] = Some(start_node(
            addrs[d],
            dirs[d].path().to_path_buf(),
            disks[d],
            mgr_addr.to_string(),
        ));
        compio::time::sleep(Duration::from_secs(15)).await;
        let ex = extent_info(&admin, extent).await;
        assert!(
            ex.replicates.contains(&members[1]) && ex.avali == 0b111,
            "node {} came back and keeps its copy: {:?} avali {:#b}",
            members[1],
            ex.replicates,
            ex.avali
        );
    });
    drop(nodes);
}

async fn submit(admin: &RpcClient, kind: u8, extent_ids: Vec<u64>) -> OpRecord {
    let resp = admin
        .call(
            MSG_OP_SUBMIT,
            rkyv_encode(&OpSubmitReq {
                kind,
                secondary_id: extent_ids[0],
                extent_ids,
                requested_by: "test".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("submit");
    let r: OpSubmitResp = rkyv_decode(&resp).expect("decode submit");
    assert_eq!(r.code, CODE_OK, "submit refused: {}", r.message);
    let start = Instant::now();
    loop {
        let resp = admin
            .call(
                MSG_OP_QUERY,
                rkyv_encode(&OpQueryReq {
                    op_id: r.op_id,
                    ..Default::default()
                }),
            )
            .await
            .expect("query op");
        let q: OpQueryResp = rkyv_decode(&resp).expect("decode op query");
        if let Some(op) = q
            .ops
            .into_iter()
            .find(|o| o.state != OP_STATE_RUNNING && o.state != OP_STATE_PENDING)
        {
            return op;
        }
        assert!(start.elapsed() < Duration::from_secs(10), "op never finished");
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A standing request is visible — `autumn-op health` marks the slot and
/// counts it — and an operator can withdraw it: the mark goes, and a spare
/// that joins afterwards receives nothing. Three nodes hold the RF 3 extent and
/// there is no spare, so the request stands until it is cancelled.
#[test]
fn a_standing_repair_request_is_shown_and_can_be_cancelled() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().expect("tmpdir")).collect();
    let addrs: Vec<_> = (0..4).map(|_| pick_addr()).collect();
    let uuid = |i: usize| format!("uuid-repair-cancel-{i}");
    let disks: Vec<u64> = (0..3).map(|i| format_node(mgr_addr, addrs[i], &uuid(i))).collect();
    let mut nodes: Vec<_> = (0..3)
        .map(|i| {
            Some(start_node(
                addrs[i],
                dirs[i].path().to_path_buf(),
                disks[i],
                mgr_addr.to_string(),
            ))
        })
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None)
            .await
            .expect("connect mgr");
        let mut node_ids = Vec::new();
        for (i, addr) in addrs.iter().take(3).enumerate() {
            node_ids.push(register_node(&admin, &addr.to_string(), &uuid(i)).await.node_id);
        }
        let (extent, members) = sealed_rf3_extent(mgr_addr, &admin, "repair-cancel/owner").await;
        let victim = node_ids.iter().position(|n| *n == members[1]).unwrap();
        let (flag, handle) = nodes[victim].take().unwrap();
        flag.shutdown();
        handle.join().expect("join extent node");
        wait_degraded(&admin, extent).await;

        let op = submit(&admin, OP_KIND_REPAIR, vec![extent]).await;
        assert_eq!(op.state, OP_STATE_SUCCEEDED, "{op:?}");
        let marked = |r: &ExtentHealthSummaryResp| {
            r.problems
                .iter()
                .find(|p| p.extent_id == extent)
                .is_some_and(|p| {
                    p.slots
                        .iter()
                        .any(|s| s.node_id == members[1] && s.repair_requested)
                })
        };
        let r = summary(&admin).await;
        assert_eq!(r.repair_requested_slots, 1, "{r:?}");
        assert!(marked(&r), "the requested slot is marked: {r:?}");

        let op = submit(&admin, OP_KIND_REPAIR_CANCEL, vec![extent]).await;
        assert_eq!(op.state, OP_STATE_SUCCEEDED, "{op:?}");
        assert!(op.message.contains("withdrew 1 repair request"), "{}", op.message);
        let r = summary(&admin).await;
        assert_eq!(r.repair_requested_slots, 0, "{r:?}");
        assert!(!marked(&r), "the mark is gone: {r:?}");
        // Nothing left to cancel says so.
        let op = submit(&admin, OP_KIND_REPAIR_CANCEL, vec![extent]).await;
        assert_eq!(op.state, OP_STATE_FAILED, "{op:?}");

        // A spare joins: with the request withdrawn, nothing moves to it.
        let r = register_node(&admin, &addrs[3].to_string(), &uuid(3)).await;
        nodes.push(Some(start_node(
            addrs[3],
            dirs[3].path().to_path_buf(),
            r.disk_uuids[0].1,
            mgr_addr.to_string(),
        )));
        compio::time::sleep(Duration::from_secs(12)).await;
        assert!(
            extent_info(&admin, extent).await.replicates.contains(&members[1]),
            "a cancelled request must not rebuild anything"
        );
    });
    drop(nodes);
}
