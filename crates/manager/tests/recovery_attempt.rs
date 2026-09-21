mod support;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc::{self as ext, RecoveryTask};
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ExtentNode, ExtentNodeConfig};

async fn setup(manager: &AutumnManager) -> (RecoveryTask, u64) {
    let response = manager
        .handle_register_node(rkyv_encode(&RegisterNodeReq {
            addr: "127.0.0.1:19009".into(),
            disk_uuids: vec!["disk-target".into()],
            node_uuid: "target".into(),
            shard_ports: vec![],
            control_address: String::new(),
        }))
        .await
        .unwrap();
    let target: RegisterNodeResp = rkyv_decode(&response).unwrap();
    assert_eq!(target.code, CODE_OK);
    let disk_id = target.disk_uuids[0].1;
    let task = RecoveryTask {
        extent_id: 1000,
        replace_id: 100,
        node_id: target.node_id,
        start_time: 1,
    };
    manager
        ._test_seed_persisted_extent(
            1000,
            MgrExtentInfo {
                extent_id: 1000,
                replicates: vec![100, 200],
                replicate_disks: vec![101, 201],
                sealed: true,
                sealed_length: 0,
                eversion: 3,
                refs: 1,
                avali: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    (task, disk_id)
}

#[compio::test]
async fn target_restart_refuses_old_attempt_and_same_assignment_successor_completes() {
    let manager = AutumnManager::new();
    manager._test_disable_background_tasks();
    let mgr_addr = support::pick_addr();
    let serving = manager.clone();
    let _manager_server = compio::runtime::spawn(async move {
        serving.serve(mgr_addr).await.unwrap();
    });
    let (task, disk_id) = setup(&manager).await;
    let a = manager
        ._test_recovery_instruction(task.clone())
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    // Both node and all connection tasks live in a dedicated runtime. Joining
    // the thread proves the predecessor cannot keep writing after restart.
    fn start(
        dir: &std::path::Path,
        disk: u64,
        mgr: std::net::SocketAddr,
    ) -> (
        std::net::SocketAddr,
        std::sync::mpsc::Sender<()>,
        std::thread::JoinHandle<()>,
    ) {
        let addr = support::pick_addr();
        let dir = dir.to_path_buf();
        let (stop, receiver) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let node = ExtentNode::new(
                        ExtentNodeConfig::new(dir, disk)
                            .with_manager_endpoint(mgr.to_string())
                            .with_registration("target", "", vec![]),
                    )
                    .await
                    .unwrap();
                    compio::runtime::spawn(async move {
                        node.serve(addr).await.unwrap();
                    })
                    .detach();
                    while receiver.try_recv().is_err() {
                        compio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                });
        });
        (addr, stop, thread)
    }
    async fn connect(addr: std::net::SocketAddr) -> std::rc::Rc<RpcClient> {
        for _ in 0..100 {
            if let Ok(client) = RpcClient::connect(addr).await {
                return client;
            }
            compio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("node did not start");
    }
    async fn done(client: &RpcClient) -> ext::RecoveryTaskDone {
        for _ in 0..100 {
            let reply = client
                .call(
                    ext::MSG_DF,
                    rkyv_encode(&ext::DfReq {
                        tasks: vec![],
                        disk_ids: vec![],
                    }),
                )
                .await
                .unwrap();
            let mut df: ext::DfResp = rkyv_decode(&reply).unwrap();
            if let Some(done) = df.done_tasks.pop() {
                return done;
            }
            compio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("recovery never completed");
    }
    let (addr, stop, thread) = start(dir.path(), disk_id, mgr_addr);
    let client = connect(addr).await;
    let response: ext::CodeResp = rkyv_decode(
        &client
            .call(ext::MSG_REQUIRE_RECOVERY, rkyv_encode(&a))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(response.code, CODE_OK, "{}", response.message);
    let old_done = done(&client).await;
    assert_eq!(old_done.attempt, a.attempt);
    stop.send(()).unwrap();
    thread.join().unwrap();
    drop(client);

    manager._test_release_recovery(1000).await.unwrap();
    let b = manager
        ._test_recovery_instruction(task.clone())
        .await
        .unwrap();
    let (addr, stop, thread) = start(dir.path(), disk_id, mgr_addr);
    let client = connect(addr).await;
    let response: ext::CodeResp = rkyv_decode(
        &client
            .call(ext::MSG_REQUIRE_RECOVERY, rkyv_encode(&a))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response.code, CODE_PRECONDITION,
        "old request after restart must be refused"
    );
    assert!(manager._test_apply_recovery(old_done).await.is_err());
    let response: ext::CodeResp = rkyv_decode(
        &client
            .call(ext::MSG_REQUIRE_RECOVERY, rkyv_encode(&b))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(response.code, CODE_OK, "{}", response.message);
    let new_done = done(&client).await;
    assert_eq!(new_done.attempt, b.attempt);
    manager._test_apply_recovery(new_done).await.unwrap();
    stop.send(()).unwrap();
    thread.join().unwrap();
}

#[compio::test]
#[ignore = "requires etcd"]
async fn durable_snapshot_replay_and_same_assignment_reissue() {
    let (_guard, endpoint) = support::start_etcd().await;
    let m = AutumnManager::new_with_etcd(vec![endpoint.clone()])
        .await
        .unwrap();
    let (task, disk) = setup(&m).await;
    let a = m._test_recovery_instruction(task.clone()).await.unwrap();
    let client = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
    let marker = client.get("extent_inflight/1000").await.unwrap();
    let snapshot = client.get("recoveryAttempt/1000").await.unwrap();
    assert_eq!(marker.kvs[0].mod_revision, snapshot.kvs[0].mod_revision);
    assert_eq!(a.attempt.nonce, marker.kvs[0].mod_revision as u64);
    m._test_replay_metadata().await.unwrap();
    m._test_release_recovery(1000).await.unwrap();
    assert!(client
        .get("recoveryAttempt/1000")
        .await
        .unwrap()
        .kvs
        .is_empty());
    let b = m._test_recovery_instruction(task).await.unwrap();
    assert_ne!(a.attempt.nonce, b.attempt.nonce);
    assert!(m
        ._test_apply_recovery(ext::RecoveryTaskDone {
            task: a.task,
            ready_disk_id: disk,
            attempt: a.attempt
        })
        .await
        .is_err());
    assert_eq!(
        client.get("extent_inflight/1000").await.unwrap().kvs[0].mod_revision as u64,
        b.attempt.nonce
    );
    // A target removed durably after the manager's last view must also fail
    // the apply transaction, even before the in-memory identity is refreshed.
    let disk_key = format!("disks/{disk}");
    let disk_value = client.get(&disk_key).await.unwrap().kvs[0].value.clone();
    client.delete(&disk_key).await.unwrap();
    assert!(m
        ._test_apply_recovery(ext::RecoveryTaskDone {
            task: b.task.clone(),
            ready_disk_id: disk,
            attempt: b.attempt.clone(),
        })
        .await
        .is_err());
    assert_eq!(
        client.get("extent_inflight/1000").await.unwrap().kvs[0].mod_revision as u64,
        b.attempt.nonce
    );
    client.put(&disk_key, &disk_value).await.unwrap();
    m._test_apply_recovery(ext::RecoveryTaskDone {
        task: b.task,
        ready_disk_id: disk,
        attempt: b.attempt,
    })
    .await
    .unwrap();
    assert!(client
        .get("extent_inflight/1000")
        .await
        .unwrap()
        .kvs
        .is_empty());
    assert!(client
        .get("recoveryAttempt/1000")
        .await
        .unwrap()
        .kvs
        .is_empty());
}
