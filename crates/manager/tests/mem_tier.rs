//! The PS memory tier (`--mem-tier-bytes`, `--mem-tier-prefix`) against a live
//! in-process cluster: covered puts, small or bulk, and covered batches are
//! held in memory and written behind, so an evicted value reads back from disk
//! and every acknowledged value is there after a PS restart; uncovered, fenced
//! and oversized writes, and a batch with any such op, are not held; a durable
//! write replaces the held value; a put or batch behind an older durable write
//! of its key is written durably too; deletes, TTLs and conditional writes see
//! the held value; the least recently used goes first, and of a chain the tail
//! goes before the head (`evict_rank`, on both `MSG_PUT` and `MSG_BATCH_PUT`).
//! What is held is read off the PS's `autumn_ps_mem_tier_*` counters.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_client::{ClusterClient, WriteLease, WriteOptions};
use autumn_partition_server::PartitionServer;
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

type Render = Box<dyn Fn() -> String + Send>;

/// `start_partition_server_stoppable`, also handing back the PS's metrics.
fn start_ps(ps_id: u64, mgr_addr: SocketAddr, ps_addr: SocketAddr) -> (ShutdownFlag, std::thread::JoinHandle<()>, Render) {
    let flag = ShutdownFlag::new();
    let stop = flag.clone();
    let (tx, rx) = std::sync::mpsc::channel::<Render>();
    let handle = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let ps = PartitionServer::connect_with_advertise_and_port(ps_id, &mgr_addr.to_string(), Some(ps_addr.to_string()), ps_addr)
                .await
                .expect("connect partition server");
            ps.sync_regions_once().await.expect("sync regions");
            let m = ps.metrics();
            tx.send(Box::new(move || m.render())).unwrap();
            let shutdown = async move {
                while !stop.is_shutdown() {
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            ps.serve_until_shutdown(ps_addr, shutdown).await.expect("serve");
        });
    });
    let render = rx.recv().expect("metrics");
    (flag, handle, render)
}

fn counter(m: &Render, name: &str) -> u64 {
    let text = m();
    let line = text.lines().find(|l| l.split_whitespace().next() == Some(name)).unwrap_or_else(|| panic!("{name} missing"));
    line.split_whitespace().nth(1).unwrap().parse::<f64>().unwrap() as u64
}

/// Wait until every held value's log append has landed.
async fn settle(m: &Render) {
    for _ in 0..200 {
        if counter(m, "autumn_ps_mem_tier_dirty_bytes") == 0 {
            return;
        }
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("write-behind appends never landed");
}

/// How many values `f` put into the tier.
async fn held_by<F: std::future::Future<Output = ()>>(m: &Render, f: F) -> u64 {
    let before = counter(m, "autumn_ps_mem_tier_inserts_total");
    f.await;
    counter(m, "autumn_ps_mem_tier_inserts_total") - before
}

#[test]
fn memory_tier_holds_covered_puts_and_writes_them_behind() {
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
        let (ps_stop, ps_join, m) = start_ps(181, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        create_namespaces(mgr_addr, &["kvc", "fs"]).await;
        let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
        c.set_rpc_timeout(Duration::from_secs(30));
        let none = WriteOptions::default();

        // LRU: 13 values of 300 KiB fit. Fifteen written one after another
        // (each landed before the next, so all are evictable): the two
        // written first go, and read back from disk.
        let page = 300 << 10;
        for (burst, seed) in [("A", 1u8), ("B", 2), ("C", 3)] {
            for i in 0..5 {
                c.put(format!("kvc/{burst}{i}").as_bytes(), val(page, seed), none).await.unwrap();
                settle(&m).await;
            }
        }
        assert_eq!(counter(&m, "autumn_ps_mem_tier_evicted_entries_total"), 2);
        for i in 0..5 {
            let hits = counter(&m, "autumn_ps_mem_tier_hits_total");
            assert_eq!(get(&c, format!("kvc/A{i}").as_bytes()).await, Some(vec![1; page]), "A{i}");
            let hit = counter(&m, "autumn_ps_mem_tier_hits_total") > hits;
            assert_eq!(hit, i >= 2, "A{i}: the two least recently written went, to disk");
            assert_eq!(get(&c, format!("kvc/B{i}").as_bytes()).await, Some(vec![2; page]), "B{i}");
            assert_eq!(get(&c, format!("kvc/C{i}").as_bytes()).await, Some(vec![3; page]), "C{i}: the newest stayed");
        }

        // A held value past the bulk cut is on no extent, so a direct batch
        // declines it (no position, and not gathered into the batch reply);
        // the client reads it with its own GET.
        let ps = RpcClient::connect(ps_addr).await.unwrap();
        let ask = autumn_rpc::partition_rpc::rkyv_encode(&autumn_rpc::partition_rpc::BatchGetReq {
            part_id: PART,
            region_epoch: 0,
            direct: true,
            items: vec![autumn_rpc::partition_rpc::BatchGetItem { key: b"kvc/B0".to_vec(), offset: 0, length: 0 }],
        });
        let reply = ps.call_into_pooled(autumn_rpc::partition_rpc::MSG_BATCH_GET, ask).await.unwrap();
        assert_eq!(reply.code, autumn_rpc::partition_rpc::CODE_OK);
        let ctrl: autumn_rpc::partition_rpc::BatchGetCtrl = autumn_rpc::partition_rpc::rkyv_decode(&reply.ctrl).unwrap();
        assert_eq!(ctrl.statuses, vec![autumn_rpc::partition_rpc::BATCH_GET_DECLINED], "declined, not gathered");
        assert!(reply.buf.is_empty());
        let mut dest = vec![0u8; page];
        let mut items = [autumn_client::GetManyItem { key: b"kvc/B0", offset: 0, length: 0, dest: &mut dest }];
        assert_eq!(c.get_many_direct(&mut items).await[0].as_ref().unwrap(), &Some(page));
        assert_eq!(dest, vec![2; page], "read on its own");

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

        // Held: a batch of covered keys (MSG_BATCH_PUT), small or adding up
        // past the bulk cut.
        let n = held_by(&m, async {
            for r in c.put_many(&[(&b"kvc/b1"[..], val(100, 4)), (&b"kvc/b2"[..], val(100, 5))], none).await {
                r.unwrap();
            }
        })
        .await;
        assert_eq!(n, 2, "a covered batch is held");
        let part = 30 << 10;
        let n = held_by(&m, async {
            for r in c
                .put_many(&[(&b"kvc/bb1"[..], val(part, 6)), (&b"kvc/bb2"[..], val(part, 7)), (&b"kvc/bb3"[..], val(part, 8))], none)
                .await
            {
                r.unwrap();
            }
        })
        .await;
        assert_eq!(n, 3, "a covered bulk batch is held");
        assert_eq!(get(&c, b"kvc/b2").await, Some(vec![5; 100]));
        assert_eq!(get(&c, b"kvc/bb3").await, Some(vec![8; part]));
        // A batch behind an older durable write of one of its keys, not yet
        // applied, is written durably as a whole.
        let rb = [(&b"kvc/rb"[..], val(100, 24))];
        let (older, newer) = futures::join!(c.put(b"kvc/rb", val(8 << 20, 23), fenced), c.put_many(&rb, none));
        older.unwrap();
        for r in newer {
            r.unwrap();
        }

        // A key twice in one batch is not held (its first append landing
        // would make the entry holding the second value evictable early); the
        // later value wins.
        let dup = [(&b"kvc/dup"[..], val(100, 27)), (&b"kvc/dup"[..], val(100, 28))];
        let n = held_by(&m, async {
            for r in c.put_many(&dup, none).await {
                r.unwrap();
            }
        })
        .await;
        assert_eq!(n, 0, "a batch with a repeated key is not held");
        assert_eq!(get(&c, b"kvc/dup").await, Some(vec![28; 100]));

        // A batch larger than the whole budget is refused partway: it takes
        // back what it had put in and is written durably.
        let over_keys: Vec<Vec<u8>> = (0..80).map(|i| format!("kvc/over{i:02}").into_bytes()).collect();
        let over: Vec<(&[u8], Bytes)> = over_keys.iter().map(|k| (&k[..], val(60 << 10, 30))).collect();
        for r in c.put_many(&over, none).await {
            r.unwrap();
        }
        assert_eq!(get(&c, b"kvc/over79").await, Some(vec![30; 60 << 10]));

        // Held: a small single put (MSG_PUT); a fenced one is not.
        assert_eq!(held_by(&m, async { c.put(b"kvc/small", b"inline", none).await.unwrap() }).await, 1);
        assert_eq!(held_by(&m, async { c.put(b"kvc/sfenced", b"fenced", fenced).await.unwrap() }).await, 0);
        assert_eq!(get(&c, b"kvc/small").await.as_deref(), Some(&b"inline"[..]));

        // Not held: a value larger than the whole budget, a batch with an
        // uncovered key, an uncovered key.
        let n = held_by(&m, async {
            c.put(b"kvc/huge", val(5 << 20, 9), none).await.unwrap();
            for r in c.put_many(&[(&b"kvc/mix"[..], val(100, 25)), (&b"fs/mix"[..], val(100, 26))], none).await {
                r.unwrap();
            }
            c.put(b"fs/x", b"durable", none).await.unwrap();
        })
        .await;
        assert_eq!(n, 0);
        assert_eq!(held_by(&m, async { c.put(b"kvc/held", val(big, 16), none).await.unwrap() }).await, 1);

        // The rank reaches the tier on both writes. With the tier filled (and
        // landed), a value 1M tokens deep is worth 250 s less than anything
        // written or read in this test, so it may evict none of it: it is not
        // held. The same at rank 0 is.
        for i in 0..14 {
            c.put(format!("kvc/fill{i}").as_bytes(), val(page, 39), none).await.unwrap();
            settle(&m).await;
        }
        let deep = none.with_evict_rank(1_000_000);
        assert_eq!(held_by(&m, async { c.put(b"kvc/deep", val(page, 40), deep).await.unwrap() }).await, 0, "ranked below everything held");
        assert_eq!(held_by(&m, async { c.put(b"kvc/shallow", val(page, 41), none).await.unwrap() }).await, 1, "the same at rank 0");
        settle(&m).await;
        // Eight small values make a batch larger than any room left, so it
        // has to evict.
        let small: Vec<Vec<u8>> = (0..8).map(|i| format!("kvc/bdeep{i}").into_bytes()).collect();
        let items: Vec<(&[u8], Bytes)> = small.iter().map(|k| (k.as_slice(), val(60 << 10, 42))).collect();
        let n = held_by(&m, async {
            for r in c.put_many_ranked(&items, &[1_000_000; 8], none).await {
                r.unwrap();
            }
        })
        .await;
        assert_eq!(n, 0, "a batch ranked below everything held");
        let items: Vec<(&[u8], Bytes)> = (0..4).map(|i| (&b"kvc/bshallow"[..], val(60 << 10, 43 + i))).collect();
        let n = held_by(&m, async {
            for r in c.put_many_ranked(&items[..1], &[0], none).await {
                r.unwrap();
            }
        })
        .await;
        assert_eq!(n, 1, "the same batch at rank 0");
        settle(&m).await;
        assert_eq!(counter(&m, "autumn_ps_mem_tier_write_behind_failed_total"), 0);

        // Restart the PS: every acknowledged value comes back, held or not.
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
        assert_eq!(get(&c, b"kvc/A0").await, Some(vec![1; page]), "evicted, written behind");
        assert_eq!(get(&c, b"kvc/b1").await, Some(vec![4; 100]), "a covered batch: written behind");
        assert_eq!(get(&c, b"kvc/dup").await, Some(vec![28; 100]), "the later value");
        for k in [&b"kvc/over00"[..], b"kvc/over40", b"kvc/over79"] {
            assert_eq!(get(&c, k).await, Some(vec![30; 60 << 10]), "a batch that did not fit: durable");
        }
        assert_eq!(get(&c, b"kvc/bb2").await, Some(vec![7; part]), "a covered bulk batch: written behind");
        assert_eq!(get(&c, b"kvc/mix").await, Some(vec![25; 100]), "a batch with an uncovered key: durable");
        assert_eq!(get(&c, b"fs/mix").await, Some(vec![26; 100]));
        assert_eq!(get(&c, b"kvc/rb").await, Some(vec![24; 100]), "a batch behind an older durable write: durable, and newer");
        assert_eq!(get(&c, b"kvc/small").await.as_deref(), Some(&b"inline"[..]), "a small single put: written behind");
        assert_eq!(get(&c, b"kvc/sfenced").await.as_deref(), Some(&b"fenced"[..]), "a small fenced put: durable");
        assert_eq!(get(&c, b"kvc/m").await, Some(vec![12; big]), "fenced: durable");
        assert_eq!(get(&c, b"kvc/r").await, Some(vec![22; 1 << 20]), "behind an older durable write: durable, and newer");
        assert_eq!(get(&c, b"kvc/held").await, Some(vec![16; big]), "written behind");
        assert_eq!(get(&c, b"kvc/C0").await, Some(vec![3; page]), "written behind");
        assert_eq!(get(&c, b"kvc/deep").await.map(|v| v.len()), Some(page), "a put ranked below everything held: durable");
        assert_eq!(get(&c, b"kvc/shallow").await, Some(vec![41; page]), "the same put at rank 0: written behind");
        assert_eq!(get(&c, b"kvc/d").await, None, "deleted");
        assert_eq!(get(&c, b"kvc/c").await, Some(x.to_vec()));
        for i in 0..8 {
            let k = format!("kvc/bdeep{i}");
            assert_eq!(get(&c, k.as_bytes()).await.map(|v| v.len()), Some(60 << 10), "{k}: a batch ranked below everything held: durable");
        }
        assert_eq!(get(&c, b"kvc/bshallow").await, Some(vec![43; 60 << 10]), "the same batch at rank 0: written behind");
    });
}
