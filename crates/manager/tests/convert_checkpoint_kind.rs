//! `autumn-convert checkpoint-kind`: a partition whose checkpoint was written
//! before 0.3.0 (kind 0) does not open; converted, it opens with every value,
//! and a rerun changes nothing.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_partition_server::checkpoint_convert::{convert_partition, downgrade_partition_for_tests, Outcome};
use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{CHECKPOINT_KIND_NORMAL, CODE_OK};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

const PART: u64 = 1461;
/// Two kind-0 records of its own (a deferred cut).
const PAIRED: u64 = 1462;
const KEYS: usize = 30;

fn spawn_ps(mgr_addr: SocketAddr, ps_addr: SocketAddr, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let ps = PartitionServer::connect_with_advertise_and_port(
                1,
                &mgr_addr.to_string(),
                Some(ps_addr.to_string()),
                ps_addr,
            )
            .await
            .expect("connect partition server");
            let _ = ps.sync_regions_once().await;
            let stop_fut = async move {
                while !stop.load(Ordering::Acquire) {
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            ps.serve_until_shutdown(ps_addr, stop_fut).await.expect("serve_until_shutdown");
        });
    })
}

fn stop_ps(stop: &AtomicBool, join: std::thread::JoinHandle<()>) {
    stop.store(true, Ordering::Release);
    join.join().expect("PS thread panicked");
}

fn key(i: usize) -> String {
    format!("k{i:03}")
}

/// The kinds of the last record of each non-empty meta extent.
async fn kinds(mgr_addr: SocketAddr, meta: u64) -> Vec<u32> {
    let sc = StreamClient::connect(&mgr_addr.to_string(), "convert-kind-probe".into(), 128 << 20, Rc::new(ConnPool::new()))
        .await
        .expect("stream client");
    let mut out = Vec::new();
    for eid in sc.get_stream_info(meta).await.expect("meta info").extent_ids {
        let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await.expect("read meta");
        if !payload.is_empty() {
            out.push(decode_last_table_locations(&payload).kind);
        }
    }
    out
}

#[test]
fn a_pre_kind_checkpoint_is_refused_until_converted() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let (meta, paired_meta) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-convert-kind").await;
        let (log, row, meta) = (create_stream(&mgr, 1).await, create_stream(&mgr, 1).await, create_stream(&mgr, 1).await);
        upsert_partition(&mgr, PART, log, row, meta, b"", b"m").await;
        let (log2, row2, meta2) = (create_stream(&mgr, 1).await, create_stream(&mgr, 1).await, create_stream(&mgr, 1).await);
        upsert_partition(&mgr, PAIRED, log2, row2, meta2, b"m", b"\xff").await;
        (meta, meta2)
    });

    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps_addr);
        for i in 0..KEYS {
            psr_put(&router, PART, key(i).as_bytes(), b"flushed").await;
        }
        psr_flush(&router, PART).await;
        psr_put(&router, PAIRED, b"paired", b"p").await;
        psr_flush(&router, PAIRED).await;
        // In the log only: the converted cursor must still replay it.
        for i in 0..KEYS / 2 {
            psr_put(&router, PART, key(i).as_bytes(), b"logged").await;
        }
    });
    stop_ps(&stop, join);

    let mgr_ep = mgr_addr.to_string();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let pool = Rc::new(ConnPool::new());
        downgrade_partition_for_tests(&mgr_ep, pool.clone(), PART, meta, true).await.expect("downgrade");
        assert_eq!(kinds(mgr_addr, meta).await, vec![0], "the pre-0.3.0 shape");
        downgrade_partition_for_tests(&mgr_ep, pool.clone(), PAIRED, paired_meta, true).await.expect("downgrade");
        downgrade_partition_for_tests(&mgr_ep, pool.clone(), PAIRED, paired_meta, false).await.expect("downgrade");
        assert_eq!(kinds(mgr_addr, paired_meta).await, vec![0, 0], "a deferred cut's pair");
    });

    // This build does not open it.
    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps_addr);
        let opened = poll_until_async(Duration::from_secs(5), Duration::from_millis(200), || {
            let router = &router;
            async move {
                match router.try_client_for(PART).await {
                    Ok(c) => try_ps_get(&c, PART, key(0).as_bytes()).await.is_ok_and(|r| r.code == CODE_OK),
                    Err(_) => false,
                }
            }
        })
        .await;
        assert!(!opened, "a kind-0 checkpoint must not open");
    });
    stop_ps(&stop, join);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let pool = Rc::new(ConnPool::new());
        let dry = convert_partition(&mgr_ep, pool.clone(), PART, meta, 16 << 30, true, false).await.expect("dry run");
        assert_eq!(dry, Outcome::Converted);
        assert_eq!(kinds(mgr_addr, meta).await, vec![0], "a dry run writes nothing");
        let r = convert_partition(&mgr_ep, pool.clone(), PART, meta, 16 << 30, false, false).await.expect("convert");
        assert_eq!(r, Outcome::Converted);
        assert_eq!(kinds(mgr_addr, meta).await, vec![CHECKPOINT_KIND_NORMAL]);
        let r = convert_partition(&mgr_ep, pool.clone(), PART, meta, 16 << 30, false, false).await.expect("rerun");
        assert_eq!(r, Outcome::Already);
        assert_eq!(kinds(mgr_addr, meta).await, vec![CHECKPOINT_KIND_NORMAL]);

        // Two records: which supersedes which is the operator's word.
        let e = convert_partition(&mgr_ep, pool.clone(), PAIRED, paired_meta, 16 << 30, false, false)
            .await
            .expect_err("two kind-0 records must be refused");
        assert!(format!("{e:#}").contains("--keep-newest 1462"), "{e:#}");
        assert_eq!(kinds(mgr_addr, paired_meta).await, vec![0, 0], "a refusal writes nothing");
        let r = convert_partition(&mgr_ep, pool.clone(), PAIRED, paired_meta, 16 << 30, false, true)
            .await
            .expect("keep newest");
        assert_eq!(r, Outcome::Converted);
        assert_eq!(kinds(mgr_addr, paired_meta).await, vec![CHECKPOINT_KIND_NORMAL]);
    });

    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps_addr);
        let opened = poll_until_async(Duration::from_secs(30), Duration::from_millis(200), || {
            let router = &router;
            async move {
                match router.try_client_for(PART).await {
                    Ok(c) => try_ps_get(&c, PART, key(0).as_bytes()).await.is_ok_and(|r| r.code == CODE_OK),
                    Err(_) => false,
                }
            }
        })
        .await;
        assert!(opened, "the converted partition never opened");
        for i in 0..KEYS {
            let r = psr_get(&router, PART, key(i).as_bytes()).await;
            let want: &[u8] = if i < KEYS / 2 { b"logged" } else { b"flushed" };
            assert_eq!((r.code, r.value.as_slice()), (CODE_OK, want), "{}", key(i));
        }
        let r = psr_get(&router, PAIRED, b"paired").await;
        assert_eq!((r.code, r.value.as_slice()), (CODE_OK, &b"p"[..]));
    });
    stop_ps(&stop, join);
}
