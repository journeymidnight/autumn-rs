//! Conditional writes commit in the partition server's group commit, compared
//! in arrival order against everything admitted before them: concurrent
//! compare-and-sets behave as if run one at a time.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use support::*;

async fn boot(base: u16, part: u64) -> (Vec<ClusterClient>, (tempfile::TempDir, tempfile::TempDir)) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let (n1, n2) = (pick_addr(), pick_addr());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1, n2, base).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part, log, row, meta, b"", b"\xff\xff\xff\xff").await;
    start_partition_server(base as u64, mgr_addr, pick_addr());
    compio::time::sleep(Duration::from_millis(1500)).await;
    let mut clients = Vec::new();
    for _ in 0..4 {
        clients.push(ClusterClient::connect_raw(&mgr_addr.to_string()).await.unwrap());
    }
    (clients, dirs)
}

/// Many create-if-absent writes of one key at once: exactly one wins, and
/// the key holds its value.
#[test]
fn one_of_many_creates_wins() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (clients, _dirs) = boot(241, 24101).await;
        let attempts = (0..32u8).map(|i| {
            let c = &clients[i as usize % clients.len()];
            async move { (i, c.compare_put(b"winner", None, Some(bytes::Bytes::copy_from_slice(&[i])), autumn_client::WriteOptions::default()).await.unwrap()) }
        });
        let won: Vec<u8> = futures::future::join_all(attempts)
            .await
            .into_iter()
            .filter_map(|(i, won)| won.then_some(i))
            .collect();
        assert_eq!(won.len(), 1, "winners: {won:?}");
        assert_eq!(clients[0].get(b"winner").await.unwrap(), Some(vec![won[0]]));
    });
}

/// Read-increment-CAS loops on one counter from many tasks: no increment is
/// lost, which needs each comparison to see the writes admitted before it.
#[test]
fn concurrent_increments_lose_nothing() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (clients, _dirs) = boot(242, 24201).await;
        clients[0].put(b"counter", &0u64.to_le_bytes(), autumn_client::WriteOptions::default()).await.unwrap();
        const TASKS: usize = 8;
        const EACH: u64 = 25;
        let tasks = (0..TASKS).map(|t| {
            let c = &clients[t % clients.len()];
            async move {
                for i in 0..EACH {
                    loop {
                        let cur = c.get(b"counter").await.unwrap().unwrap();
                        let next = (u64::from_le_bytes(cur[..8].try_into().unwrap()) + 1).to_le_bytes();
                        if c.compare_put(b"counter", Some(&cur), Some(bytes::Bytes::copy_from_slice(&next)), autumn_client::WriteOptions::default()).await.unwrap() {
                            break;
                        }
                    }
                    // Unrelated writes keep the group commit busy around them.
                    c.put(format!("noise/{t}/{i}").as_bytes(), b"x", autumn_client::WriteOptions::default()).await.unwrap();
                }
            }
        });
        futures::future::join_all(tasks).await;
        let v = clients[0].get(b"counter").await.unwrap().unwrap();
        assert_eq!(u64::from_le_bytes(v[..8].try_into().unwrap()), TASKS as u64 * EACH);
    });
}

/// Puts and compare-and-sets of the same keys sent back to back on one
/// connection, many at once: each comparison sees the put before it, though
/// that put is not committed yet. Relies on one connection's frames reaching
/// the partition write actor in the order they were sent.
#[test]
fn a_comparison_sees_a_put_admitted_before_it() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (clients, _dirs) = boot(243, 24301).await;
        let c = &clients[0];
        let pairs = (0..50u8).map(|i| async move {
            let key = format!("k{i}");
            let (put, swapped) = futures::join!(c.put(key.as_bytes(), b"a", autumn_client::WriteOptions::default()), c.compare_put(key.as_bytes(), Some(b"a"), Some(bytes::Bytes::copy_from_slice(b"b")), autumn_client::WriteOptions::default()));
            put.unwrap();
            (i, swapped.unwrap())
        });
        let missed: Vec<u8> = futures::future::join_all(pairs)
            .await
            .into_iter()
            .filter_map(|(i, swapped)| (!swapped).then_some(i))
            .collect();
        assert!(missed.is_empty(), "comparisons that missed the put before them: {missed:?}");
        assert_eq!(c.get(b"k0").await.unwrap(), Some(b"b".to_vec()));
    });
}
