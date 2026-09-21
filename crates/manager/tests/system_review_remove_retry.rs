mod support;

use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::*;

#[test]
fn remove_lost_reply_retries_after_manager_runtime_restart() {
    let (_etcd_guard, endpoint) = compio::runtime::Runtime::new()
        .unwrap()
        .block_on(support::start_etcd());
    let registration = RegisterNodeReq {
        addr: "127.0.0.1:19091".to_string(),
        disk_uuids: vec!["remove-replay-disk".to_string()],
        node_uuid: "remove-replay-node".to_string(),
        shard_ports: vec![],
        control_address: String::new(),
    };
    let (node_id, tombstone) = {
        let runtime = compio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![endpoint.clone()])
                .await
                .unwrap();
            let registered: RegisterNodeResp = rkyv_decode(
                &manager
                    .handle_register_node(rkyv_encode(&registration))
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(registered.code, CODE_OK, "{}", registered.message);
            let fenced: CodeResp = rkyv_decode(
                &manager
                    .handle_fence_node(rkyv_encode(&FenceNodeReq {
                        node_id: registered.node_id,
                        reason: "retire".to_string(),
                        set_by: "test".to_string(),
                        force: true,
                    }))
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(fenced.code, CODE_OK, "{}", fenced.message);
            let reply = manager
                .handle_remove_node(rkyv_encode(&RemoveNodeReq {
                    node_id: registered.node_id,
                    set_by: "test".to_string(),
                }))
                .await
                .unwrap();
            drop(reply);
            let metadata = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
            let tombstone = metadata
                .get(format!("decommissioned/{}", registered.node_id))
                .await
                .unwrap();
            assert_eq!(
                tombstone.kvs.len(),
                1,
                "remove must have committed before restart"
            );
            (registered.node_id, tombstone.kvs[0].value.clone())
        })
    };
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let metadata = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
        metadata
            .delete("autumn-rs/stream-manager/leader")
            .await
            .unwrap();
        let manager = AutumnManager::new_with_etcd(vec![endpoint.clone()])
            .await
            .unwrap();
        for _ in 0..2 {
            let reply: RemoveNodeResp = rkyv_decode(
                &manager
                    .handle_remove_node(rkyv_encode(&RemoveNodeReq {
                        node_id,
                        set_by: "test".to_string(),
                    }))
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(reply.code, CODE_OK, "{}", reply.message);
        }
        let persisted = metadata
            .get(format!("decommissioned/{node_id}"))
            .await
            .unwrap();
        assert_eq!(persisted.kvs.len(), 1);
        assert_eq!(persisted.kvs[0].value, tombstone);
        let rejected: RegisterNodeResp = rkyv_decode(
            &manager
                .handle_register_node(rkyv_encode(&RegisterNodeReq {
                    addr: "127.0.0.1:19092".to_string(),
                    ..registration
                }))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(rejected.code, CODE_PRECONDITION, "{}", rejected.message);
    });
}
