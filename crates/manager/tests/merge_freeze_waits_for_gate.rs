//! A merge freeze that has to wait for a running compaction must not stop
//! the partition.
//!
//! The freeze drain needs the partition's maintenance gate, which a
//! compaction holds for its whole run. The drain used to await that gate
//! inside `partition_loop`, so a long major compaction stopped every write,
//! the freeze TTL check and the manager's `freeze=false` with it (a stress
//! run: a freeze and PUTs to one partition both went unanswered).

mod support;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use autumn_partition_server::background::{compaction_held_count, set_compaction_hold};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;

use support::*;

const PART: u64 = 25001;

async fn put(ps: &RpcClient, key: &[u8]) -> Result<u8, String> {
    let req = partition_rpc::rkyv_encode(&partition_rpc::PutReq {
        part_id: PART,
        key: key.to_vec(),
        value: b"v".to_vec(),
        expires_at: 0,
        region_epoch: 0,
        inode_hint: 0,
        lease_epoch: 0,
    });
    match compio::time::timeout(Duration::from_secs(3), ps.call(partition_rpc::MSG_PUT, req)).await {
        Err(_) => Err("put timed out".into()),
        Ok(Err(e)) => Err(format!("{e:?}")),
        Ok(Ok(resp)) => partition_rpc::rkyv_decode::<partition_rpc::PutResp>(&resp)
            .map(|r| r.code)
            .map_err(|e| e.to_string()),
    }
}

async fn freeze(ps: &RpcClient, freeze: bool) -> Result<partition_rpc::MergeFreezeResp, String> {
    let req = partition_rpc::rkyv_encode(&partition_rpc::MergeFreezeReq {
        part_id: PART,
        freeze,
    });
    let resp = ps
        .call(partition_rpc::MSG_MERGE_FREEZE, req)
        .await
        .map_err(|e| format!("{e:?}"))?;
    partition_rpc::rkyv_decode(&resp).map_err(|e| e.to_string())
}

#[test]
fn a_freeze_waiting_for_a_compaction_keeps_the_partition_serving() {
    set_compaction_hold(false);
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().unwrap();
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .unwrap();
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-freeze-gate").await;
        let (log, row, meta) = (
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
        );
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;
    });

    let ps_addr = pick_addr();
    let (stop, join) = start_partition_server_stoppable(1, mgr_addr, ps_addr);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = Rc::new(RpcClient::connect(ps_addr).await.expect("ps"));
        for round in 0..2 {
            for i in 0..200 {
                assert_eq!(put(&ps, format!("k{round}-{i:04}").as_bytes()).await, Ok(0));
            }
            ps_flush(&ps, PART).await;
        }

        // A compaction takes the maintenance gate and parks holding it.
        let held = compaction_held_count();
        set_compaction_hold(true);
        ps_compact(&ps, PART).await;
        let parked = poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
            compaction_held_count() > held
        })
        .await;
        assert!(parked, "the compaction never parked");

        let pending: Rc<RefCell<Option<Result<partition_rpc::MergeFreezeResp, String>>>> =
            Rc::new(RefCell::new(None));
        {
            let ps = ps.clone();
            let out = pending.clone();
            compio::runtime::spawn(async move {
                *out.borrow_mut() = Some(freeze(&ps, true).await);
            })
            .detach();
        }
        compio::time::sleep(Duration::from_millis(500)).await;

        // While the freeze waits for the gate, the partition still serves.
        assert_eq!(
            put(&ps, b"during-wait").await,
            Ok(0),
            "a write was not served while a merge freeze waited for the compaction"
        );

        // The manager's rollback ends the wait; the waiting freeze answers.
        let unfreeze = compio::time::timeout(Duration::from_secs(3), freeze(&ps, false)).await;
        assert!(
            matches!(unfreeze, Ok(Ok(ref r)) if r.code == partition_rpc::CODE_OK),
            "freeze=false was not answered while the freeze waited: {unfreeze:?}"
        );
        let answered = poll_until(Duration::from_secs(3), Duration::from_millis(20), || {
            pending.borrow().is_some()
        })
        .await;
        assert!(answered, "the cancelled freeze never answered");
        let r = pending.borrow_mut().take().unwrap();
        assert!(
            matches!(r, Ok(ref r) if r.code != partition_rpc::CODE_OK),
            "a cancelled freeze must not answer OK: {r:?}"
        );
        assert_eq!(put(&ps, b"after-cancel").await, Ok(0));

        // Once the compaction ends, a freeze drains and freezes as before.
        set_compaction_hold(false);
        let r = compio::time::timeout(Duration::from_secs(30), freeze(&ps, true)).await;
        let r = match r {
            Ok(Ok(r)) => r,
            other => panic!("freeze after the compaction: {other:?}"),
        };
        assert_eq!(r.code, partition_rpc::CODE_OK, "freeze: {}", r.message);
        assert!(r.log_tail_extent_id != 0, "a drained freeze reports its log position");
        assert_ne!(put(&ps, b"while-frozen").await, Ok(0), "a frozen partition took a write");
        let r = freeze(&ps, false).await.unwrap();
        assert_eq!(r.code, partition_rpc::CODE_OK);
        assert_eq!(put(&ps, b"after-unfreeze").await, Ok(0));
    });
    stop.shutdown();
    let _ = join.join();
}
