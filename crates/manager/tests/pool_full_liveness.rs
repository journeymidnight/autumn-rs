//! With the buffer pool full, a cluster keeps answering. The manager, two
//! extent nodes, the partition server and the client share one process and so
//! one 64 MiB pool (about 25.6 MiB each of `Receive` and `Reply`, half of that
//! per thread). Writers put and readers read values of 4–8 MiB (the SDK keeps
//! three large requests in flight per connection, so smaller ones never fill a
//! thread's share; log values, so each read goes extent
//! node → partition server → client, a pool buffer at every hop), far more at
//! once than the pool holds. Refusals must happen, and every put and get must
//! still complete with the right bytes, with no long stall. Red when the client
//! stops reading a refused reply past (the connection loses its place).

mod support;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_client::{ClusterClient, WriteOptions};
use autumn_rpc::client::RpcClient;
use autumn_transport::PoolUse;
use bytes::Bytes;

use support::*;

const PART: u64 = 19301;
const MIB: usize = 1 << 20;
const WRITERS: usize = 6;
const READERS: usize = 6;
const OPS: usize = 16;

/// A patterned value per size, every 4 KiB stamped with the key and round, so
/// a stale or spliced piece of another value is caught too.
fn value(key: usize, round: usize) -> Vec<u8> {
    thread_local! {
        static BASE: Vec<Vec<u8>> = (0..3).map(|k| (0..4 * MIB + k * 2 * MIB).map(|i| (i as u8).wrapping_mul(31)).collect()).collect();
    }
    let mut v = BASE.with(|b| b[key % 3].clone());
    let stamp = [(key as u64).to_le_bytes(), (round as u64).to_le_bytes()].concat();
    for page in v.chunks_mut(4096) {
        page[..16].copy_from_slice(&stamp);
    }
    v
}

/// Readers' values, apart from every writer's.
const READ_KEY: usize = 100;

/// (`Receive` refused, `Reply` refused or waited out).
fn refusals() -> (u64, u64) {
    let s = autumn_transport::regpool_snapshot();
    let (rx, reply) = (&s.uses[PoolUse::Receive as usize], &s.uses[PoolUse::Reply as usize]);
    (rx.refused, reply.refused + reply.timeouts)
}

#[test]
fn a_full_pool_refuses_and_every_request_still_completes() {
    assert!(autumn_transport::set_regpool_cap_bytes(64 * MIB), "this process sets the cap first");
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr_addr = pick_addr();
        start_manager(mgr_addr);
        let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (n1, n2) = (pick_addr(), pick_addr());
        start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
        start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 193).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        start_partition_server(193, mgr_addr, pick_addr());
        compio::time::sleep(Duration::from_millis(1500)).await;
        let c = Rc::new(ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect"));
        c.set_rpc_timeout(Duration::from_secs(30));

        // Keys the readers read, written before the load starts.
        for k in 0..READERS {
            c.put(format!("live/r{k}").as_bytes(), Bytes::from(value(READ_KEY + k, 0)), WriteOptions::default()).await.unwrap();
        }

        let before = refusals();
        let started = Instant::now();
        let done: Rc<RefCell<Vec<Instant>>> = Rc::default();
        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let (c, done) = (c.clone(), done.clone());
            tasks.push(compio::runtime::spawn(async move {
                for op in 0..OPS {
                    let key = format!("live/w{w}/{op}");
                    c.put(key.as_bytes(), Bytes::from(value(w, op)), WriteOptions::default())
                        .await
                        .unwrap_or_else(|e| panic!("put {key}: {e}"));
                    done.borrow_mut().push(Instant::now());
                }
            }));
        }
        for r in 0..READERS {
            let (c, done) = (c.clone(), done.clone());
            tasks.push(compio::runtime::spawn(async move {
                let want = value(READ_KEY + r, 0);
                for _ in 0..OPS {
                    let got = c.get(format!("live/r{r}").as_bytes()).await.unwrap_or_else(|e| panic!("get r{r}: {e}"));
                    assert!(got.as_deref() == Some(want.as_slice()), "get r{r}: wrong bytes");
                    done.borrow_mut().push(Instant::now());
                }
            }));
        }
        let all = futures::future::join_all(tasks);
        if compio::time::timeout(Duration::from_secs(120), all).await.is_err() {
            panic!("not every request completed in 120 s ({} of {})", done.borrow().len(), (WRITERS + READERS) * OPS);
        }

        let mut times = done.borrow().clone();
        times.sort();
        let gaps = times.windows(2).map(|w| w[1] - w[0]).chain(times.first().map(|t| *t - started));
        let worst = gaps.max().unwrap();
        assert!(worst < Duration::from_secs(10), "a stall of {worst:?} between completions");
        let after = refusals();
        assert!(after.0 > before.0, "no value was refused: Receive never filled, the test proves nothing");
        assert!(after.1 > before.1, "no reply was refused: Reply never filled, the test proves nothing");

        // Everything written reads back.
        for w in 0..WRITERS {
            for op in 0..OPS {
                let got = c.get(format!("live/w{w}/{op}").as_bytes()).await.expect("read back");
                assert!(got.as_deref() == Some(value(w, op).as_slice()), "w{w}/{op} read back wrong");
            }
        }
        let snap = autumn_transport::regpool_snapshot();
        eprintln!(
            "{} requests in {:?}, worst gap {worst:?}; receive {}; reply {}",
            times.len(),
            started.elapsed(),
            snap.uses[PoolUse::Receive as usize],
            snap.uses[PoolUse::Reply as usize]
        );
    });
}
