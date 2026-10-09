//! `stream_alloc_extent` awaits for seconds (commit-length probes, creating the
//! extent files on the nodes) between its entry checks and its commit. The
//! commit must refuse when, in that window, another allocation changed the
//! stream or another owner took it over; it used to check both against state
//! read after the awaits, which always matched.

mod support;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

use support::*;

const PART: u64 = 26001;

/// `ALLOC_TEST_PAUSE_MS` is process-global: one scenario at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn pause_next_alloc(ms: u64) {
    autumn_manager::ALLOC_TEST_PAUSE_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
}

async fn acquire(mgr: &RpcClient, key: &str) -> i64 {
    let resp = mgr
        .call(
            MSG_ACQUIRE_OWNER_LOCK,
            rkyv_encode(&AcquireOwnerLockReq { owner_key: key.to_string() }),
        )
        .await
        .unwrap();
    let r: AcquireOwnerLockResp = rkyv_decode(&resp).unwrap();
    assert_eq!(r.code, CODE_OK, "acquire {key}: {}", r.message);
    r.owner_epoch
}

async fn alloc(
    mgr: &RpcClient,
    stream_id: u64,
    key: &str,
    epoch: i64,
    seal_commit: Option<u64>,
    seal_extent_id: u64,
) -> StreamAllocExtentResp {
    let req = rkyv_encode(&StreamAllocExtentReq {
        stream_id,
        owner_key: key.to_string(),
        owner_epoch: epoch,
        seal_commit,
        exclude_node_ids: vec![],
        seal_extent_id,
    });
    rkyv_decode(&mgr.call(MSG_STREAM_ALLOC_EXTENT, req).await.unwrap()).unwrap()
}

async fn stream(mgr: &RpcClient, stream_id: u64) -> (Vec<u64>, Vec<(u64, bool)>) {
    let resp = mgr
        .call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq { stream_ids: vec![stream_id] }))
        .await
        .unwrap();
    let r: StreamInfoResp = rkyv_decode(&resp).unwrap();
    let ids = r.streams.into_iter().next().expect("stream").1.extent_ids;
    let sealed = r.extents.into_iter().map(|(id, e)| (id, e.sealed)).collect();
    (ids, sealed)
}

fn spawn_alloc(
    mgr: Rc<RpcClient>,
    stream_id: u64,
    key: String,
    epoch: i64,
    seal_commit: Option<u64>,
    seal_extent_id: u64,
) -> Rc<RefCell<Option<StreamAllocExtentResp>>> {
    let out = Rc::new(RefCell::new(None));
    let slot = out.clone();
    compio::runtime::spawn(async move {
        let r = alloc(&mgr, stream_id, &key, epoch, seal_commit, seal_extent_id).await;
        *slot.borrow_mut() = Some(r);
    })
    .detach();
    out
}

async fn wait(slot: &Rc<RefCell<Option<StreamAllocExtentResp>>>) -> StreamAllocExtentResp {
    for _ in 0..300 {
        if let Some(r) = slot.borrow_mut().take() {
            return r;
        }
        compio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("alloc never answered");
}

/// Manager (etcd-backed when `etcd` names an endpoint) + 2 ENs and a
/// partition's three streams, owned by `partition/<PART>`; no partition server.
async fn setup(node_base: u16, etcd: Option<String>) -> (Rc<RpcClient>, u64, u64, u64, String) {
    let mgr_addr = pick_addr();
    match etcd {
        None => start_manager(mgr_addr),
        Some(endpoint) => {
            std::thread::spawn(move || {
                compio::runtime::Runtime::new().unwrap().block_on(async {
                    let m = autumn_manager::AutumnManager::new_with_etcd(
                        vec![endpoint],
                        manager_identity(),
                    )
                    .await
                    .expect("manager with etcd");
                    let _ = m.serve(mgr_addr).await;
                });
            });
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (n1, n2) = (pick_addr(), pick_addr());
    start_extent_node(n1, d1.keep(), 1);
    start_extent_node(n2, d2.keep(), 2);
    let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
        .await
        .unwrap();
    register_two_nodes(&mgr, n1, n2, node_base).await;
    let ready = poll_until_async(Duration::from_secs(20), Duration::from_millis(200), || async {
        let req = rkyv_encode(&CreateStreamReq {
            replicates: 2,
            ec_data_shard: 2,
            ec_parity_shard: 0,
        });
        rkyv_decode::<CreateStreamResp>(&mgr.call(MSG_CREATE_STREAM, req).await.unwrap())
            .unwrap()
            .stream
            .is_some()
    })
    .await;
    assert!(ready, "extent nodes never took an allocation");
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
    (mgr, log, row, meta, format!("partition/{PART}"))
}

/// The tail is sealed (a split sealed it), so neither allocation re-seals it
/// and nothing about the tail changes between them. Both used to succeed:
/// `[.., T, N1, N2]`, an open extent left in the middle of the stream.
#[test]
fn two_allocations_on_a_sealed_tail_do_not_both_succeed() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, log, _row, _meta, key) = setup(160, None).await;
        let epoch = acquire(&mgr, &key).await;
        let req = rkyv_encode(&MultiModifySplitReq {
            part_id: PART,
            owner_key: key.clone(),
            owner_epoch: epoch,
            mid_key: b"m".to_vec(),
            log_stream_sealed_length: 0,
            row_stream_sealed_length: 0,
            meta_stream_sealed_length: 0,
            log_tail_extent_id: 0,
            row_tail_extent_id: 0,
            meta_tail_extent_id: 0,
            op_id: 0,
        });
        let r: CodeResp = rkyv_decode(&mgr.call(MSG_MULTI_MODIFY_SPLIT, req).await.unwrap()).unwrap();
        assert_eq!(r.code, CODE_OK, "split: {}", r.message);
        let (before, sealed) = stream(&mgr, log).await;
        let tail = *before.last().unwrap();
        assert!(sealed.contains(&(tail, true)), "the split did not seal the tail");

        pause_next_alloc(2_000);
        let first = spawn_alloc(mgr.clone(), log, key.clone(), epoch, None, 0);
        compio::time::sleep(Duration::from_millis(300)).await;
        let second = alloc(&mgr, log, &key, epoch, None, 0).await;
        assert_eq!(second.code, CODE_OK, "second alloc: {}", second.message);
        let first = wait(&first).await;
        let (after, sealed) = stream(&mgr, log).await;
        eprintln!("first: code={} msg={}; stream {before:?} -> {after:?}", first.code, first.message);
        let open_inside: Vec<u64> = after[..after.len() - 1]
            .iter()
            .copied()
            .filter(|id| sealed.contains(&(*id, false)))
            .collect();
        assert!(open_inside.is_empty(), "open extent(s) {open_inside:?} inside the stream {after:?}");
        assert_ne!(first.code, CODE_OK, "both allocations on one sealed tail succeeded");
        assert!(first.message.contains("membership changed"), "refused for another reason: {}", first.message);
    });
}

/// The owner changes while its allocation awaits: the old owner must not get
/// a fresh tail, which no EN fence protects and the new owner never fenced.
#[test]
fn an_allocation_whose_owner_changed_meanwhile_is_refused() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, log, _row, _meta, key) = setup(162, None).await;
        let old_epoch = acquire(&mgr, &key).await;
        let (before, _) = stream(&mgr, log).await;
        let tail = *before.last().unwrap();

        pause_next_alloc(2_000);
        let old = spawn_alloc(mgr.clone(), log, key.clone(), old_epoch, Some(0), tail);
        compio::time::sleep(Duration::from_millis(300)).await;
        let new_epoch = acquire(&mgr, &key).await;
        assert!(new_epoch > old_epoch);
        let old = wait(&old).await;
        let (after, _) = stream(&mgr, log).await;
        eprintln!("old owner: code={} msg={}; stream {before:?} -> {after:?}", old.code, old.message);
        assert_ne!(old.code, CODE_OK, "a deposed owner got a fresh tail");
        assert!(old.message.contains("owner_epoch mismatch"), "refused for another reason: {}", old.message);
        assert_eq!(after, before, "a deposed owner's allocation changed the stream");
    });
}

/// An `update_stream_ec` landing while an allocation awaits: the allocation
/// writes its entry snapshot of the stream back, so it must refuse rather
/// than revert the change (the etcd CAS refuses it; memory mode must agree).
#[test]
fn a_stream_record_change_during_an_allocation_is_not_reverted() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, log, _row, _meta, key) = setup(164, None).await;
        let epoch = acquire(&mgr, &key).await;
        let (before, _) = stream(&mgr, log).await;
        let tail = *before.last().unwrap();

        pause_next_alloc(2_000);
        let pending = spawn_alloc(mgr.clone(), log, key.clone(), epoch, Some(0), tail);
        compio::time::sleep(Duration::from_millis(300)).await;
        let req = rkyv_encode(&UpdateStreamEcReq { stream_id: log, ec_data_shard: 2, ec_parity_shard: 1 });
        let r: UpdateStreamEcResp =
            rkyv_decode(&mgr.call(MSG_UPDATE_STREAM_EC, req).await.unwrap()).unwrap();
        assert_eq!(r.code, CODE_OK, "update_stream_ec: {}", r.message);
        let refused = wait(&pending).await;
        let resp = mgr
            .call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq { stream_ids: vec![log] }))
            .await
            .unwrap();
        let info: StreamInfoResp = rkyv_decode(&resp).unwrap();
        let st = &info.streams[0].1;
        eprintln!("alloc: code={} msg={}; ec now {}+{}", refused.code, refused.message, st.ec_data_shard, st.ec_parity_shard);
        assert_eq!((st.ec_data_shard, st.ec_parity_shard), (2, 1), "the allocation reverted update_stream_ec");
        assert_ne!(refused.code, CODE_OK, "an allocation committed over a changed stream record");
        assert!(refused.message.contains("membership changed"), "refused for another reason: {}", refused.message);
    });
}

/// Through etcd: the commit's compares (the stream bytes, the owner lock's
/// revision) hold for an undisturbed allocation.
#[test]
fn an_undisturbed_allocation_commits_through_etcd() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd, endpoint) = start_etcd().await;
        let (mgr, log, _row, _meta, key) = setup(166, Some(endpoint)).await;
        let epoch = acquire(&mgr, &key).await;
        let (before, _) = stream(&mgr, log).await;
        let tail = *before.last().unwrap();
        let r = alloc(&mgr, log, &key, epoch, Some(0), tail).await;
        assert_eq!(r.code, CODE_OK, "alloc through etcd: {}", r.message);
        let (after, _) = stream(&mgr, log).await;
        assert_eq!(after.len(), before.len() + 1);
    });
}
