//! A partition flushes while one replica of its log extent is down.
//!
//! A large value lives in the log (WAL) extent; the SST only points at it. The
//! append that wrote it was acked by every replica after each had fsynced it,
//! so the flush has nothing more to wait for. A flush-time barrier used to
//! ask every replica of that log extent for its fsynced length before
//! publishing the SST; with one replica down that never succeeded, so every
//! flush of the partition failed until the node returned or was fenced, and
//! writes eventually stalled behind the unflushed memtables.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_stream::{ConnPool, StreamClient};
use support::*;

/// Whether `dir` (hashed `{base}/{hh}/` layout) holds extent-{id}.dat.
fn has_dat(dir: &std::path::Path, extent_id: u64) -> bool {
    let name = format!("extent-{extent_id}.dat");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|e| {
        e.path().is_dir()
            && std::fs::read_dir(e.path()).is_ok_and(|files| {
                files
                    .flatten()
                    .any(|f| f.file_name().to_str() == Some(name.as_str()))
            })
    })
}

#[test]
fn flush_succeeds_with_a_log_replica_down() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().expect("tmpdir")).collect();
    let addrs: Vec<_> = (0..3).map(|_| pick_addr()).collect();
    let mut nodes: Vec<_> = (0..3)
        .map(|i| {
            Some(start_extent_node_stoppable(
                addrs[i],
                dirs[i].path().to_path_buf(),
                8400 + i as u64,
            ))
        })
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        for (i, addr) in addrs.iter().enumerate() {
            register_node(&mgr, &addr.to_string(), &format!("uuid-dead-log-{i}")).await;
        }
        // RF 2 on 3 nodes: with one down, every stream can still roll onto
        // two live ones.
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 911, log, row, meta, b"a", b"z").await;
        drop(mgr);

        let ps_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Larger than the inline threshold: the value stays in the log extent.
        let value = vec![0x5a_u8; 64 * 1024];
        ps_put(&ps, 911, b"k1", &value).await;

        let observer = StreamClient::connect(
            &mgr_addr.to_string(),
            "flush-dead-log-replica/observer".to_string(),
            1 << 30,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("observer StreamClient");
        let info = observer.get_stream_info(log).await.expect("log stream info");
        let tail = *info.extent_ids.last().expect("log tail");
        let victim = (0..3)
            .find(|&i| has_dat(dirs[i].path(), tail))
            .expect("some node holds the log tail");

        let (flag, handle) = nodes[victim].take().unwrap();
        flag.shutdown();
        handle.join().expect("join extent node");
        // Let the manager's 2 s health poll see it, so new extents avoid it.
        compio::time::sleep(Duration::from_secs(6)).await;

        // Red before the barrier was removed: "flush failed: ...
        // await_extent_synced_to: replica ... could not be queried".
        ps_flush(&ps, 911).await;
        let got = ps_get(&ps, 911, b"k1").await;
        assert_eq!(got.code, autumn_rpc::partition_rpc::CODE_OK, "k1 after the flush: {}", got.message);
        assert!(got.value == value, "k1 changed after the flush");
    });
    drop(nodes);
}
