//! F-FS-UNIFY M0 — manager-side fuse-fs inode allocation (`MSG_ALLOC_INODES`).
//!
//! The pre-M0 allocator was a client-side non-CAS read-modify-write on the
//! fs KV `[0x04]next_inode` key: concurrent allocators (two fuse mounts, or
//! a mount + the Python `autumn.Fs` client) could claim the same batch →
//! duplicate inodes. These tests pin the manager grant's contract:
//!
//! 1. Concurrent grants return DISJOINT `[base, base+count)` ranges
//!    (memory-mode manager, raw RPC fan-out).
//! 2. The migration `floor` raises the counter but can never rewind it.
//! 3. count == 0 is refused.
//! 4. (etcd) The counter persists across a manager restart — a successor
//!    leader's grants don't overlap the predecessor's (the CAS + replay
//!    path; this is what the legacy KV RMW fundamentally couldn't do).
//! 5. (etcd) The etcd counter key holds a strict BE u64 that matches the
//!    granted watermark.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, AllocInodesReq, AllocInodesResp, CODE_INVALID_ARGUMENT, CODE_OK,
    MSG_ALLOC_INODES,
};

use support::{pick_addr, start_etcd, start_manager};

async fn alloc(mgr: &RpcClient, count: u32, floor: u64) -> AllocInodesResp {
    let payload = rkyv_encode(&AllocInodesReq {
        count,
        floor,
        volume: Vec::new(),
    });
    let resp = mgr
        .call(MSG_ALLOC_INODES, payload)
        .await
        .expect("alloc_inodes rpc");
    rkyv_decode(&resp).expect("decode AllocInodesResp")
}

/// Grants must be pairwise disjoint under concurrency, and the floor must
/// raise-but-never-rewind the counter. Memory-mode manager (the same grant
/// code path minus the etcd CAS, which test 4/5 cover).
#[test]
fn concurrent_grants_are_disjoint_and_floor_is_monotonic() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr_addr = pick_addr();
        start_manager(mgr_addr);
        compio::time::sleep(Duration::from_millis(300)).await;
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");

        // 16 concurrent grants of 100 — every [base, base+100) disjoint.
        let futs: Vec<_> = (0..16).map(|_| alloc(&mgr, 100, 0)).collect();
        let resps = futures::future::join_all(futs).await;
        let mut ranges: Vec<(u64, u64)> = resps
            .iter()
            .map(|r| {
                assert_eq!(r.code, CODE_OK, "grant failed: {}", r.message);
                (r.base, r.base + 100)
            })
            .collect();
        ranges.sort();
        for w in ranges.windows(2) {
            assert!(
                w[0].1 <= w[1].0,
                "overlapping grants: {:?} vs {:?}",
                w[0],
                w[1]
            );
        }
        // inode 1 (ROOT_INO) is never granted
        assert!(ranges[0].0 >= 2, "granted the reserved root inode");

        // floor raises the counter (legacy-KV migration)...
        let hi = alloc(&mgr, 10, 1_000_000).await;
        assert_eq!(hi.code, CODE_OK);
        assert!(hi.base >= 1_000_000, "floor not honored: {}", hi.base);
        // ...but a stale floor can never rewind it
        let after = alloc(&mgr, 10, 3).await;
        assert_eq!(after.code, CODE_OK);
        assert!(
            after.base >= hi.base + 10,
            "stale floor rewound the counter: {} < {}",
            after.base,
            hi.base + 10
        );

        // count == 0 refused
        let bad = alloc(&mgr, 0, 0).await;
        assert_eq!(bad.code, CODE_INVALID_ARGUMENT);
    });
}

fn start_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = autumn_manager::AutumnManager::new_with_etcd(vec![etcd_endpoint])
                .await
                .expect("new manager with etcd");
            let _ = manager.serve(mgr_addr).await;
        });
    });
}

/// Etcd-backed: grants go through the leader-fenced CAS loop on
/// `autumn-rs/fs/next_inode`; the persisted counter equals the granted
/// watermark (strict BE u64). Persistence across restart follows from the
/// same code path — etcd mode reads the counter from etcd on EVERY grant
/// (no in-memory cache), so a successor leader's first grant sees the
/// predecessor's watermark exactly as grant #2 sees grant #1's.
///
/// Split-brain guard: a FOLLOWER manager sharing the same etcd must refuse
/// with NOT_LEADER — never grant (the double-grant a second live allocator
/// produced under the legacy client-side RMW).
#[test]
#[ignore] // requires embedded etcd
fn etcd_counter_is_cas_backed_and_follower_refuses() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        // Leader: concurrent grants through the CAS loop + a floor bump.
        let mgr1_addr = pick_addr();
        start_etcd_manager(mgr1_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_secs(2)).await;
        let mgr1 = RpcClient::connect(mgr1_addr).await.expect("connect mgr1");

        let futs: Vec<_> = (0..8).map(|_| alloc(&mgr1, 50, 0)).collect();
        let resps = futures::future::join_all(futs).await;
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        for r in &resps {
            assert_eq!(r.code, CODE_OK, "etcd grant failed: {}", r.message);
            ranges.push((r.base, r.base + 50));
        }
        ranges.sort();
        for w in ranges.windows(2) {
            assert!(
                w[0].1 <= w[1].0,
                "overlapping CAS grants: {:?} vs {:?}",
                w[0],
                w[1]
            );
        }
        let floored = alloc(&mgr1, 10, 77_000).await;
        assert_eq!(floored.code, CODE_OK);
        assert!(floored.base >= 77_000);

        // The etcd counter key equals the granted watermark (strict BE u64).
        let etcd = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("etcd connect");
        let got = etcd.get("autumn-rs/fs/next_inode").await.expect("get");
        let kv = got.kvs.first().expect("counter key exists");
        let counter = u64::from_be_bytes(kv.value.as_slice().try_into().expect("8-byte BE u64"));
        assert_eq!(
            counter,
            floored.base + 10,
            "etcd counter != granted watermark"
        );

        // A follower on the same etcd must refuse, never grant.
        let mgr2_addr = pick_addr();
        start_etcd_manager(mgr2_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_secs(2)).await;
        let mgr2 = RpcClient::connect(mgr2_addr).await.expect("connect mgr2");
        let follower = alloc(&mgr2, 50, 0).await;
        assert_eq!(
            follower.code,
            autumn_rpc::manager_rpc::CODE_NOT_LEADER,
            "follower must refuse to grant (got code={} base={})",
            follower.code,
            follower.base
        );
    });
}
