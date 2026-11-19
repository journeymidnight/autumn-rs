//! `autumn-convert sst` (the real binary) against an in-process cluster. A
//! partition's SSTs are written by this build (MetaBlock v3), the PS stopped,
//! and the partition rewritten as MetaBlock v2 wrote it
//! (`downgrade_partition_for_tests`, the same procedure run backwards). The
//! tool's dry run counts them and writes nothing; a run converts them, a rerun
//! finds nothing to do, and a PS opened afterwards serves every key byte for
//! byte. A run stopped between publishing its checkpoint and cutting the old
//! one is finished by the next.
//!
//! Build first: `cargo build -p autumn-server --bins`.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::ConnPool;
use support::*;

const PART: u64 = 974;
const BIG: usize = 64 * 1024;

fn value(i: u32) -> Vec<u8> {
    if i % 50 == 0 {
        vec![(i % 251) as u8; BIG]
    } else {
        format!("value-{i:05}-{}", "x".repeat((i % 40) as usize)).into_bytes()
    }
}

fn run_tool(mgr: &str, extra: &[&str]) -> (bool, String) {
    let out = std::process::Command::new(support_binary_path("autumn-convert"))
        .args(["sst", "--manager", mgr, "--cluster-secret-file"])
        .arg(cluster_secret_file())
        .args(extra)
        .output()
        .expect("run autumn-convert");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

struct V2Cluster {
    mgr_addr: std::net::SocketAddr,
    streams: [u64; 3],
    v2_tables: usize,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

/// A cluster whose partition `PART` holds two flushed tables (as MetaBlock v2)
/// plus writes left in the log, every PS stopped.
fn v2_cluster(base: u16) -> V2Cluster {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (n1, n2) = (pick_addr(), pick_addr());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, base).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff\xff\xff\xff").await;

        let ps_addr = pick_addr();
        let (stop, join) = start_partition_server_stoppable(base as u64, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.unwrap();
        for (range, flush) in [(0..300u32, true), (300..500, true), (500..520, false)] {
            for i in range {
                ps_put(&ps, PART, format!("k{i:05}").as_bytes(), &value(i)).await;
            }
            if flush {
                ps_flush(&ps, PART).await;
            }
        }
        drop(ps);
        stop.shutdown();
        join.join().unwrap();

        let r = autumn_partition_server::sst_convert::downgrade_partition_for_tests(
            &mgr_addr.to_string(),
            Rc::new(ConnPool::new()),
            PART,
            [log, row, meta],
        )
        .await
        .expect("downgrade to v2");
        assert!(r.converted >= 2, "{r:?}");
        V2Cluster { mgr_addr, streams: [log, row, meta], v2_tables: r.converted, _dirs: dirs }
    })
}

/// Open the partition on a new PS and read every key back.
fn assert_served(c: &V2Cluster, base: u16) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps_addr = pick_addr();
        let (stop, join) = start_partition_server_stoppable(base as u64, c.mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        while ps_get(&ps, PART, b"k00001").await.code != partition_rpc::CODE_OK {
            assert!(Instant::now() < deadline, "the converted partition never served");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        for i in 0..520u32 {
            let got = ps_get(&ps, PART, format!("k{i:05}").as_bytes()).await;
            assert_eq!(got.code, partition_rpc::CODE_OK, "k{i:05}: {}", got.message);
            assert!(got.value == value(i), "k{i:05}: wrong value ({} bytes)", got.value.len());
        }
        drop(ps);
        stop.shutdown();
        join.join().unwrap();
    });
}

#[test]
fn v2_ssts_convert_and_the_partition_serves_them() {
    let c = v2_cluster(274);
    let mgr = c.mgr_addr.to_string();
    let n = c.v2_tables;
    let (ok, text) = run_tool(&mgr, &["--dry-run"]);
    assert!(ok && text.contains(&format!("converted {n} SSTs")), "{text}");
    let (ok, text) = run_tool(&mgr, &[]);
    assert!(ok && text.contains(&format!("converted {n} SSTs")), "{text}");
    let (ok, text) = run_tool(&mgr, &[]);
    assert!(ok && text.contains(&format!("converted 0 SSTs (0 bytes), {n} already v3")), "{text}");
    assert_served(&c, 274);
}

/// Stopped after its checkpoint is published, before the old one is cut: the
/// meta stream holds both records, and the next run finishes.
#[test]
fn a_run_stopped_after_publishing_is_finished_by_the_next() {
    let c = v2_cluster(275);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::sst_convert::FAIL_AFTER_PUBLISH.store(true, std::sync::atomic::Ordering::Relaxed);
        let err = autumn_partition_server::sst_convert::convert_partition(
            &c.mgr_addr.to_string(),
            Rc::new(ConnPool::new()),
            PART,
            c.streams,
            16 << 30,
            false,
        )
        .await
        .expect_err("the failpoint stops the run");
        assert!(err.to_string().contains("stopped after publishing"), "{err:#}");
    });
    let n = c.v2_tables;
    let (ok, text) = run_tool(&c.mgr_addr.to_string(), &[]);
    assert!(ok && text.contains(&format!("converted 0 SSTs (0 bytes), {n} already v3")), "{text}");
    assert_served(&c, 275);
}
