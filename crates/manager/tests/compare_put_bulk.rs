//! `compare_put_bulk` / `put_many_if_absent` against a live in-process
//! cluster: a present key is left untouched, an absent one is written with its
//! TTL, `expected` still compares, deleted and expired keys count as absent,
//! the presence check works from the memtable and from an SST, both tail paths
//! (received into a pooled buffer, or decoded with the frame) carry the value,
//! and concurrent creators of one key get exactly one winner.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;
use bytes::Bytes;

use support::*;

const PART: u64 = 16101;

fn pattern(len: usize, seed: u64) -> Bytes {
    let mut out = Vec::with_capacity(len + 8);
    let mut i = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    while out.len() < len {
        out.extend_from_slice(&i.to_le_bytes());
        i = i.wrapping_add(1);
    }
    out.truncate(len);
    Bytes::from(out)
}

#[test]
fn put_if_absent_and_compare_with_bulk_values() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 161).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(161, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        let ps = RpcClient::connect(ps_addr).await.unwrap();
        let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
        c.set_rpc_timeout(Duration::from_secs(30));
        let far = ClusterClient::ttl_to_expires_at(3600);

        // Large (pooled receive) and small (decoded with the frame) tails.
        for (key, len) in [(&b"big"[..], 4 << 20), (&b"small"[..], 100)] {
            let first = pattern(len, 1);
            assert!(c.compare_put_bulk(key, None, first.clone(), far).await.unwrap(), "absent → written");
            assert_eq!(c.get(key).await.unwrap().as_deref(), Some(&first[..]));
            assert!(!c.compare_put_bulk(key, None, pattern(len, 2), far).await.unwrap(), "present → refused");
            assert_eq!(c.get(key).await.unwrap().as_deref(), Some(&first[..]), "present key untouched");
        }

        // `expected` compares the visible value; the new value is large.
        let small = c.get(b"small").await.unwrap().unwrap();
        let wrong = b"not-the-value".to_vec();
        assert!(!c.compare_put_bulk(b"small", Some(&wrong), pattern(1 << 20, 3), far).await.unwrap());
        let replacement = pattern(1 << 20, 4);
        assert!(c.compare_put_bulk(b"small", Some(&small), replacement.clone(), far).await.unwrap());
        assert_eq!(c.get(b"small").await.unwrap().as_deref(), Some(&replacement[..]));
        assert!(!c.compare_put_bulk(b"absent", Some(&small), pattern(10, 5), far).await.unwrap(), "expected on an absent key");

        // Over the PS inline cap: refused at once, not retried as a routing miss.
        let t = std::time::Instant::now();
        let r = c.compare_put_bulk(b"huge", None, Bytes::from(vec![0u8; (64 << 20) + 1]), far).await;
        assert!(matches!(r, Err(autumn_client::AutumnError::ValueTooLarge { .. })), "{r:?}");
        assert!(t.elapsed() < Duration::from_secs(5), "took {:?}", t.elapsed());
        assert_eq!(c.get(b"huge").await.unwrap(), None);
        let long = vec![1u8; 64 * 1024 + 1];
        assert!(matches!(
            c.compare_put_bulk(b"small", Some(&long), pattern(10, 6), far).await,
            Err(autumn_client::AutumnError::InvalidArgument(_))
        ));

        // Presence from an SST rather than the memtable.
        ps_flush(&ps, PART).await;
        assert!(!c.compare_put_bulk(b"big", None, pattern(4 << 20, 6), far).await.unwrap(), "present in an SST");

        // A deleted key is absent.
        c.delete(b"big").await.unwrap();
        let again = pattern(2 << 20, 7);
        assert!(c.compare_put_bulk(b"big", None, again.clone(), far).await.unwrap(), "deleted → written");
        assert_eq!(c.get(b"big").await.unwrap().as_deref(), Some(&again[..]));

        // The TTL reaches the stored entry, and an expired key is absent.
        let soon = ClusterClient::ttl_to_expires_at(2);
        assert!(c.compare_put_bulk(b"ttl", None, pattern(1 << 20, 8), soon).await.unwrap());
        assert!(c.get(b"ttl").await.unwrap().is_some());
        compio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(c.get(b"ttl").await.unwrap(), None, "the write carried its TTL");
        assert!(c.compare_put_bulk(b"ttl", None, pattern(1 << 20, 9), far).await.unwrap(), "expired → written");

        // Batched: present keys are reported, absent ones written.
        let vals: Vec<Bytes> = (0..6).map(|i| pattern(1 << 20, 100 + i)).collect();
        let keys: Vec<Vec<u8>> = (0..6).map(|i| format!("batch/{i}").into_bytes()).collect();
        for i in [1usize, 4] {
            c.put(&keys[i], b"already").await.unwrap();
        }
        let items: Vec<(&[u8], Bytes, u64)> =
            keys.iter().zip(&vals).map(|(k, v)| (k.as_slice(), v.clone(), far)).collect();
        let results: Vec<bool> = c.put_many_if_absent(&items).await.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(results, vec![true, false, true, true, false, true]);
        for (i, k) in keys.iter().enumerate() {
            let want: &[u8] = if results[i] { &vals[i] } else { b"already" };
            assert_eq!(c.get(k).await.unwrap().as_deref(), Some(want), "batch key {i}");
        }

        // Concurrent creators of one key: exactly one wins, and its value stays.
        let racers: Vec<Bytes> = (0..8).map(|i| pattern(1 << 20, 200 + i)).collect();
        let outcomes = futures::future::join_all(
            racers.iter().map(|v| c.compare_put_bulk(b"race", None, v.clone(), far)),
        )
        .await;
        let winners: Vec<usize> = outcomes
            .into_iter()
            .enumerate()
            .filter_map(|(i, r)| r.unwrap().then_some(i))
            .collect();
        assert_eq!(winners.len(), 1, "one creator wins: {winners:?}");
        assert_eq!(c.get(b"race").await.unwrap().as_deref(), Some(&racers[winners[0]][..]));
    });
}
