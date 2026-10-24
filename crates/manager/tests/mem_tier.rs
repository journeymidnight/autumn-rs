//! The PS memory tier (`--mem-tier-bytes`, `--mem-tier-prefix`) against a live
//! in-process cluster: covered bulk puts are served from memory and gone after
//! a PS restart (never logged); uncovered, small, fenced, oversized and batched
//! writes stay durable; a durable write replaces the held value; a put behind
//! an older durable write of its key is written durably too; deletes, TTLs and
//! conditional writes see the held value; an unread burst is evicted as a
//! whole while a read burst survives.

mod support;

use std::time::Duration;

use autumn_client::{ClusterClient, WriteLease, WriteOptions};
use autumn_rpc::client::RpcClient;
use bytes::Bytes;

use support::*;

const PART: u64 = 18101;

fn val(len: usize, seed: u8) -> Bytes {
    Bytes::from(vec![seed; len])
}

async fn get(c: &ClusterClient, k: &[u8]) -> Option<Vec<u8>> {
    c.get(k).await.unwrap()
}

#[test]
fn memory_tier_holds_covered_puts_until_restart() {
    // One process per test binary: the in-process PS reads these at start.
    autumn_partition_server::params().mem_tier_bytes.set(4 << 20);
    autumn_partition_server::params().mem_tier_prefix.set(vec!["kvc/".to_string()]);

    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (n1, n2) = (pick_addr(), pick_addr());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 181).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        let (ps_stop, ps_join) = start_partition_server_stoppable(181, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
        c.set_rpc_timeout(Duration::from_secs(30));
        let none = WriteOptions::default();

        // Grouped eviction: 13 values of 300 KiB fit. Three bursts of five; a
        // read of one member of the first saves it, the unread second goes
        // whole when the third needs room.
        let page = 300 << 10;
        for (burst, seed) in [("A", 1u8), ("B", 2), ("C", 3)] {
            if burst == "C" {
                assert!(c.head(b"kvc/A2").await.unwrap().found);
            }
            for i in 0..5 {
                c.put(format!("kvc/{burst}{i}").as_bytes(), val(page, seed), none).await.unwrap();
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        for i in 0..5 {
            assert_eq!(get(&c, format!("kvc/B{i}").as_bytes()).await, None, "B{i}: the unread burst went whole");
            assert_eq!(get(&c, format!("kvc/A{i}").as_bytes()).await, Some(vec![1; page]), "A{i}: the read burst stayed");
            assert_eq!(get(&c, format!("kvc/C{i}").as_bytes()).await, Some(vec![3; page]), "C{i}");
        }

        let big = 100 << 10; // a bulk put: what the tier holds
        let fenced = WriteOptions::fenced(WriteLease { inode_hint: 5, lease_epoch: 1 });

        // A durable write of a held key replaces it (a fenced put takes the
        // durable path).
        c.put(b"kvc/m", val(big, 11), none).await.unwrap();
        c.put(b"kvc/m", val(big, 12), fenced).await.unwrap();
        assert_eq!(get(&c, b"kvc/m").await, Some(vec![12; big]), "the durable write is newer than the held value");

        // A held put behind an older durable write of its key, not yet
        // applied, is written durably too: the older write landing later must
        // not drop it (checked after the restart below).
        let (older, newer) = futures::join!(c.put(b"kvc/r", val(8 << 20, 21), fenced), c.put(b"kvc/r", val(1 << 20, 22), none));
        older.unwrap();
        newer.unwrap();
        assert_eq!(get(&c, b"kvc/r").await, Some(vec![22; 1 << 20]));

        // Conditional writes compare against the held value.
        let x = val(big, 13);
        c.put(b"kvc/c", x.clone(), none).await.unwrap();
        assert!(!c.compare_put(b"kvc/c", None, Some(Bytes::from_static(b"y")), none).await.unwrap(), "present in memory");
        assert!(c.compare_put(b"kvc/c", Some(&x[..64]), Some(Bytes::from_static(b"no")), none).await.is_ok());
        assert_eq!(get(&c, b"kvc/c").await, Some(x.to_vec()), "a mismatching expectation leaves it");

        // A delete removes a held key.
        c.put(b"kvc/d", val(big, 14), none).await.unwrap();
        c.delete(b"kvc/d", none).await.unwrap();
        assert_eq!(get(&c, b"kvc/d").await, None);

        // A TTL holds in memory too.
        c.put(b"kvc/t", val(big, 15), none.with_ttl(Duration::from_secs(2))).await.unwrap();
        assert!(get(&c, b"kvc/t").await.is_some());
        compio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(get(&c, b"kvc/t").await, None, "expired in memory");

        // Durable: a value larger than the whole budget, a small value, a
        // batch, an uncovered key.
        c.put(b"kvc/huge", val(5 << 20, 9), none).await.unwrap();
        c.put(b"kvc/small", b"inline", none).await.unwrap();
        for r in c.put_many(&[(&b"kvc/b1"[..], val(100, 4)), (&b"kvc/b2"[..], val(100, 5))], none).await {
            r.unwrap();
        }
        c.put(b"fs/x", b"durable", none).await.unwrap();
        c.put(b"kvc/held", val(big, 16), none).await.unwrap();
        assert!(get(&c, b"kvc/held").await.is_some());

        // Restart the PS: only what was written durably comes back.
        ps_stop.shutdown();
        ps_join.join().expect("join PS");
        start_partition_server(181, mgr_addr, pick_addr());
        let mut back = false;
        for _ in 0..100 {
            if matches!(c.get(b"fs/x").await, Ok(Some(_))) {
                back = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(back, "the PS came back");
        assert_eq!(get(&c, b"fs/x").await.as_deref(), Some(&b"durable"[..]));
        assert_eq!(get(&c, b"kvc/huge").await.map(|v| v.len()), Some(5 << 20), "too large for memory: durable");
        assert_eq!(get(&c, b"kvc/b1").await, Some(vec![4; 100]), "batched: durable");
        assert_eq!(get(&c, b"kvc/small").await.as_deref(), Some(&b"inline"[..]), "small: durable");
        assert_eq!(get(&c, b"kvc/m").await, Some(vec![12; big]), "fenced: durable");
        assert_eq!(get(&c, b"kvc/r").await, Some(vec![22; 1 << 20]), "behind an older durable write: durable, and newer");
        assert_eq!(get(&c, b"kvc/held").await, None, "held in memory only");
        assert_eq!(get(&c, b"kvc/C0").await, None, "held in memory only");
    });
}
