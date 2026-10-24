//! The write API's options against a live in-process cluster.
//!
//! `compare_put`: a present key is left untouched, an absent one is written,
//! `expected` still compares, deleted and expired keys count as absent, the
//! presence check works from the memtable and from an SST, both tail paths
//! (received into a pooled buffer, or decoded with the frame) carry the value,
//! and concurrent creators of one key get exactly one winner.
//!
//! `WriteOptions`: a TTL and a lease each reach the stored entry through every
//! write that takes them — small and large `put`, `put_many` items of both
//! sizes, `compare_put`, and (lease) `delete`.

mod support;

use std::time::Duration;

use autumn_client::{AutumnError, ClusterClient, WriteLease, WriteOptions};
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

/// One manager, two extent nodes, one partition over the whole key space and
/// its PS. Returns the client and a direct connection to the PS.
async fn boot(base: u16, part: u64) -> (ClusterClient, std::rc::Rc<RpcClient>, (tempfile::TempDir, tempfile::TempDir)) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs = (tempfile::tempdir().expect("n1"), tempfile::tempdir().expect("n2"));
    let (n1, n2) = (pick_addr(), pick_addr());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1, n2, base).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part, log, row, meta, b"", b"\xff\xff\xff\xff").await;
    let ps_addr = pick_addr();
    start_partition_server(base as u64, mgr_addr, ps_addr);
    compio::time::sleep(Duration::from_millis(1500)).await;
    let ps = RpcClient::connect(ps_addr).await.unwrap();
    let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
    c.set_rpc_timeout(Duration::from_secs(30));
    (c, ps, dirs)
}

#[test]
fn compare_put_with_small_and_large_values() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (c, ps, _dirs) = boot(161, PART).await;
        let far = WriteOptions::default().with_ttl(Duration::from_secs(3600));

        // Large (pooled receive) and small (decoded with the frame) tails.
        for (key, len) in [(&b"big"[..], 4 << 20), (&b"small"[..], 100)] {
            let first = pattern(len, 1);
            assert!(c.compare_put(key, None, Some(first.clone()), far).await.unwrap(), "absent → written");
            assert_eq!(c.get(key).await.unwrap().as_deref(), Some(&first[..]));
            assert!(!c.compare_put(key, None, Some(pattern(len, 2)), far).await.unwrap(), "present → refused");
            assert_eq!(c.get(key).await.unwrap().as_deref(), Some(&first[..]), "present key untouched");
        }

        // `expected` compares the visible value; the new value is large.
        let small = c.get(b"small").await.unwrap().unwrap();
        let wrong = b"not-the-value".to_vec();
        assert!(!c.compare_put(b"small", Some(&wrong), Some(pattern(1 << 20, 3)), far).await.unwrap());
        let replacement = pattern(1 << 20, 4);
        assert!(c.compare_put(b"small", Some(&small), Some(replacement.clone()), far).await.unwrap());
        assert_eq!(c.get(b"small").await.unwrap().as_deref(), Some(&replacement[..]));
        assert!(!c.compare_put(b"absent", Some(&small), Some(pattern(10, 5)), far).await.unwrap(), "expected on an absent key");

        // Over the PS inline cap: refused at once, not retried as a routing miss.
        let t = std::time::Instant::now();
        let r = c.compare_put(b"huge", None, Some(Bytes::from(vec![0u8; (64 << 20) + 1])), far).await;
        assert!(matches!(r, Err(autumn_client::AutumnError::ValueTooLarge { .. })), "{r:?}");
        assert!(t.elapsed() < Duration::from_secs(5), "took {:?}", t.elapsed());
        assert_eq!(c.get(b"huge").await.unwrap(), None);
        let long = vec![1u8; 64 * 1024 + 1];
        assert!(matches!(
            c.compare_put(b"small", Some(&long), Some(pattern(10, 6)), far).await,
            Err(autumn_client::AutumnError::InvalidArgument(_))
        ));

        // Presence from an SST rather than the memtable.
        ps_flush(&ps, PART).await;
        assert!(!c.compare_put(b"big", None, Some(pattern(4 << 20, 6)), far).await.unwrap(), "present in an SST");

        // A deleted key is absent.
        c.delete(b"big", WriteOptions::default()).await.unwrap();
        let again = pattern(2 << 20, 7);
        assert!(c.compare_put(b"big", None, Some(again.clone()), far).await.unwrap(), "deleted → written");
        assert_eq!(c.get(b"big").await.unwrap().as_deref(), Some(&again[..]));

        // The TTL reaches the stored entry, and an expired key is absent.
        let soon = WriteOptions::default().with_ttl(Duration::from_secs(2));
        assert!(c.compare_put(b"ttl", None, Some(pattern(1 << 20, 8)), soon).await.unwrap());
        assert!(c.get(b"ttl").await.unwrap().is_some());
        compio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(c.get(b"ttl").await.unwrap(), None, "the write carried its TTL");
        assert!(c.compare_put(b"ttl", None, Some(pattern(1 << 20, 9)), far).await.unwrap(), "expired → written");

        // Present keys are reported, absent ones written.
        let vals: Vec<Bytes> = (0..6).map(|i| pattern(1 << 20, 100 + i)).collect();
        let keys: Vec<Vec<u8>> = (0..6).map(|i| format!("batch/{i}").into_bytes()).collect();
        for i in [1usize, 4] {
            c.put(&keys[i], b"already", WriteOptions::default()).await.unwrap();
        }
        let mut results = Vec::new();
        for (k, v) in keys.iter().zip(&vals) {
            results.push(c.compare_put(k, None, Some(v.clone()), far).await.unwrap());
        }
        assert_eq!(results, vec![true, false, true, true, false, true]);
        for (i, k) in keys.iter().enumerate() {
            let want: &[u8] = if results[i] { &vals[i] } else { b"already" };
            assert_eq!(c.get(k).await.unwrap().as_deref(), Some(want), "batch key {i}");
        }

        // Concurrent creators of one key: exactly one wins, and its value stays.
        let racers: Vec<Bytes> = (0..8).map(|i| pattern(1 << 20, 200 + i)).collect();
        let outcomes = futures::future::join_all(
            racers.iter().map(|v| c.compare_put(b"race", None, Some(v.clone()), far)),
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

#[test]
fn ttl_and_lease_reach_every_write() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (c, _ps, _dirs) = boot(162, 16201).await;
        let (small, large) = (pattern(100, 1), pattern(1 << 20, 2));

        // TTL: every write that carries one stores it; the control does not.
        let ttl = WriteOptions::default().with_ttl(Duration::from_secs(2));
        c.put(b"t/put-small", &small, ttl).await.unwrap();
        c.put(b"t/put-large", &large, ttl).await.unwrap();
        for r in c.put_many(&[(&b"t/many-small"[..], small.clone()), (&b"t/many-large"[..], large.clone())], ttl).await {
            r.unwrap();
        }
        assert!(c.compare_put(b"t/cmp", None, Some(large.clone()), ttl).await.unwrap());
        c.put(b"t/keep", &small, WriteOptions::default()).await.unwrap();
        let expiring: [&[u8]; 5] = [b"t/put-small", b"t/put-large", b"t/many-small", b"t/many-large", b"t/cmp"];
        for k in expiring {
            assert!(c.get(k).await.unwrap().is_some(), "{} written", String::from_utf8_lossy(k));
        }
        compio::time::sleep(Duration::from_secs(4)).await;
        for k in expiring {
            assert_eq!(c.get(k).await.unwrap(), None, "{} carried its TTL", String::from_utf8_lossy(k));
        }
        assert!(c.get(b"t/keep").await.unwrap().is_some(), "no TTL, no expiry");

        // Lease: raise inode 9's floor to epoch 5; epoch 4 is then refused by
        // every write, and none of them lands.
        let current = WriteOptions::fenced(WriteLease { inode_hint: 9, lease_epoch: 5 });
        let stale = WriteOptions::fenced(WriteLease { inode_hint: 9, lease_epoch: 4 });
        c.put(b"l/held", &small, current).await.unwrap();
        let fenced = |what: &str, r: Result<(), AutumnError>| {
            assert!(matches!(r, Err(AutumnError::Fenced(_))), "{what}: {r:?}");
        };
        fenced("put small", c.put(b"l/a", &small, stale).await);
        fenced("put large", c.put(b"l/b", &large, stale).await);
        for r in c.put_many(&[(&b"l/c"[..], small.clone()), (&b"l/d"[..], large.clone())], stale).await {
            fenced("put_many", r);
        }
        fenced("compare_put", c.compare_put(b"l/e", None, Some(small.clone()), stale).await.map(|_| ()));
        fenced("delete", c.delete(b"l/held", stale).await);
        for k in [&b"l/a"[..], b"l/b", b"l/c", b"l/d", b"l/e"] {
            assert_eq!(c.get(k).await.unwrap(), None, "{} not written", String::from_utf8_lossy(k));
        }
        assert!(c.get(b"l/held").await.unwrap().is_some(), "not deleted");
        c.delete(b"l/held", current).await.expect("the current epoch deletes");
    });
}
