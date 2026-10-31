//! `get_many_into_raw` against a live in-process cluster: large values are
//! received straight into the caller's memory (and only there), a value longer
//! than its memory is truncated with its true length reported, a missing key
//! reads as `None` and leaves its memory alone, a batch of small values takes
//! the batched path, and the memory reads back the same through a flush.

mod support;

use std::time::Duration;

use autumn_client::{ClusterClient, RawGetItem, WriteOptions};
use autumn_rpc::client::RpcClient;
use bytes::Bytes;

use support::*;

const PART: u64 = 19101;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

/// Reads `keys` into fresh buffers of `lens`, pre-filled with 0xAB.
async fn read(c: &ClusterClient, keys: &[&[u8]], lens: &[usize]) -> (Vec<Option<usize>>, Vec<Vec<u8>>) {
    let mut bufs: Vec<Vec<u8>> = lens.iter().map(|&n| vec![0xAB; n]).collect();
    let items: Vec<RawGetItem> = keys
        .iter()
        .zip(bufs.iter_mut())
        .map(|(k, b)| RawGetItem { key: k, ptr: b.as_mut_ptr(), len: b.len() })
        .collect();
    // SAFETY: `bufs` outlives the call, which runs to completion.
    let res = unsafe { c.get_many_into_raw(&items, None).await };
    (res.into_iter().map(|r| r.expect("read")).collect(), bufs)
}

#[test]
fn values_land_in_the_callers_memory() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr_addr = pick_addr();
        start_manager(mgr_addr);
        let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (n1, n2) = (pick_addr(), pick_addr());
        start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
        start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 191).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(191, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        let ps = RpcClient::connect(ps_addr).await.unwrap();
        let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
        c.set_rpc_timeout(Duration::from_secs(30));
        let none = WriteOptions::default();

        let big = 4 << 20;
        let (a, b) = (pattern(big, 1), pattern(300 << 10, 2));
        c.put(b"raw/a", Bytes::from(a.clone()), none).await.unwrap();
        c.put(b"raw/b", Bytes::from(b.clone()), none).await.unwrap();

        // Exact fit, a missing key, a value longer than its memory.
        let keys: [&[u8]; 3] = [b"raw/a", b"raw/missing", b"raw/b"];
        let (got, bufs) = read(&c, &keys, &[big, 256 << 10, 256 << 10]).await;
        assert_eq!(got, vec![Some(big), None, Some(300 << 10)]);
        assert_eq!(bufs[0], a, "a whole value");
        assert!(bufs[1].iter().all(|&x| x == 0xAB), "a missing key leaves its memory alone");
        assert_eq!(bufs[2], b[..256 << 10], "a longer value is truncated to its memory");

        // Small values: the batched path.
        let small: Vec<Vec<u8>> = (0..8).map(|i| pattern(1000 + i, i as u8)).collect();
        let skeys: Vec<Vec<u8>> = (0..8).map(|i| format!("raw/s{i}").into_bytes()).collect();
        for (k, v) in skeys.iter().zip(&small) {
            c.put(k, Bytes::from(v.clone()), none).await.unwrap();
        }
        let srefs: Vec<&[u8]> = skeys.iter().map(|k| k.as_slice()).collect();
        let (got, bufs) = read(&c, &srefs, &vec![4096; 8]).await;
        for i in 0..8 {
            assert_eq!(got[i], Some(small[i].len()));
            assert_eq!(bufs[i][..small[i].len()], small[i][..], "small value {i}");
        }

        // Many large values at once, read back after a flush to an SST.
        let many: Vec<Vec<u8>> = (0..32).map(|i| pattern(1 << 20, 100 + i as u8)).collect();
        let mkeys: Vec<Vec<u8>> = (0..32).map(|i| format!("raw/m{i:02}").into_bytes()).collect();
        for (k, v) in mkeys.iter().zip(&many) {
            c.put(k, Bytes::from(v.clone()), none).await.unwrap();
        }
        let mrefs: Vec<&[u8]> = mkeys.iter().map(|k| k.as_slice()).collect();
        for round in ["memtable", "sst"] {
            let (got, bufs) = read(&c, &mrefs, &vec![1 << 20; 32]).await;
            for i in 0..32 {
                assert_eq!(got[i], Some(1 << 20), "{round} {i}");
                assert!(bufs[i] == many[i], "{round}: value {i} differs");
            }
            if round == "memtable" {
                ps_flush(&ps, PART).await;
            }
        }
    });
}
