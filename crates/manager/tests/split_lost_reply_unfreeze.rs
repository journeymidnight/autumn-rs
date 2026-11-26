//! A split the manager committed, whose reply the partition server never got:
//! every write the PS acknowledges afterwards must still be readable once the
//! partition reopens on its new layout.
//!
//! A proxy in front of the manager (the PS's only manager address) forwards the
//! PS's `MSG_MULTI_MODIFY_SPLIT`, lets the manager commit it, and drops the
//! reply. The PS's region sync is slowed to 60 s so the window after the split
//! stays open for the test; a crash and a fresh PS then reopen the partitions.

mod support;

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::frame::{FLAG_RESPONSE, HEADER_LEN};
use autumn_rpc::manager_rpc::{self, OpSubmitReq, OpSubmitResp, MSG_MULTI_MODIFY_SPLIT, MSG_OP_SUBMIT, OP_KIND_SPLIT};
use autumn_rpc::partition_rpc;
use autumn_rpc::version_hello::Role;

use support::*;

const PART: u64 = 501;
const PS_ID: u64 = 51;

fn read_frame(r: &mut impl Read) -> Option<(u8, u8, Vec<u8>)> {
    let mut header = [0u8; HEADER_LEN];
    r.read_exact(&mut header).ok()?;
    let len = u32::from_le_bytes([header[6], header[7], header[8], header[9]]) as usize;
    let mut raw = header.to_vec();
    raw.resize(HEADER_LEN + len, 0);
    r.read_exact(&mut raw[HEADER_LEN..]).ok()?;
    Some((header[4], header[5], raw))
}

/// Front `upstream`; the first `drops` replies of `target` are swallowed after
/// the manager answered, and that connection is reset.
fn spawn_lost_reply_proxy(upstream: SocketAddr, target: u8, drops: usize) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
    let front = listener.local_addr().unwrap();
    let left = Arc::new(AtomicUsize::new(drops));
    let dropped = Arc::new(AtomicUsize::new(0));
    let dropped_out = dropped.clone();
    std::thread::spawn(move || {
        for client in listener.incoming().flatten() {
            let Ok(up) = TcpStream::connect(upstream) else {
                let _ = client.shutdown(Shutdown::Both);
                continue;
            };
            let (mut c_r, mut u_w) = (client.try_clone().unwrap(), up.try_clone().unwrap());
            std::thread::spawn(move || {
                while let Some((_, _, raw)) = read_frame(&mut c_r) {
                    if u_w.write_all(&raw).is_err() {
                        break;
                    }
                }
                let _ = u_w.shutdown(Shutdown::Both);
            });
            let (left, dropped) = (left.clone(), dropped.clone());
            std::thread::spawn(move || {
                let (mut u_r, mut c_w) = (up, client);
                while let Some((msg, flags, raw)) = read_frame(&mut u_r) {
                    let take = msg == target
                        && flags & FLAG_RESPONSE != 0
                        && left.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok();
                    if take {
                        dropped.fetch_add(1, Ordering::SeqCst);
                        let _ = c_w.shutdown(Shutdown::Both);
                        let _ = u_r.shutdown(Shutdown::Both);
                        return;
                    }
                    if c_w.write_all(&raw).is_err() {
                        break;
                    }
                }
                let _ = c_w.shutdown(Shutdown::Both);
            });
        }
    });
    (front, dropped_out)
}

async fn put_at(c: &RpcClient, part_id: u64, key: &[u8], region_epoch: u64) -> bool {
    let req = partition_rpc::PutReq {
        part_id,
        key: key.to_vec(),
        expires_at: 0,
        region_epoch,
        inode_hint: 0,
        lease_epoch: 0,
        evict_rank: 0,
    };
    match c.call_put(req, b"after-split".to_vec()).await {
        Ok(resp) => {
            let r: partition_rpc::PutResp = partition_rpc::rkyv_decode(&resp).expect("PutResp");
            r.code == partition_rpc::CODE_OK
        }
        Err(_) => false,
    }
}

fn owner(regions: &manager_rpc::GetRegionsResp, key: &[u8]) -> u64 {
    regions
        .regions
        .iter()
        .find(|(_, r)| {
            let rg = r.rg.as_ref().expect("range");
            key >= rg.start_key.as_slice() && (rg.end_key.is_empty() || key < rg.end_key.as_slice())
        })
        .map(|(id, _)| *id)
        .expect("a region holds the key")
}

struct Cluster {
    mgr_addr: SocketAddr,
    /// The manager behind the reply-dropping proxy: the PS's manager address.
    front: SocketAddr,
    dropped: Arc<AtomicUsize>,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

fn cluster(rt: &compio::runtime::Runtime) -> Cluster {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let (front, dropped) = spawn_lost_reply_proxy(mgr_addr, MSG_MULTI_MODIFY_SPLIT, 1);
    let (n1, n2) = (pick_addr(), pick_addr());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    start_extent_node(n1, d1.path().to_path_buf(), 1);
    start_extent_node(n2, d2.path().to_path_buf(), 2);
    rt.block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 40).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
    });
    Cluster { mgr_addr, front, dropped, _dirs: (d1, d2) }
}

/// Write `b…`/`w…` keys, flush, then split at `m` with its reply dropped.
/// Returns the pre-split region epoch once the manager holds the split.
async fn seed_and_split(c: &Cluster, ps_addr: SocketAddr) -> u64 {
    let (mgr_addr, dropped) = (c.mgr_addr, c.dropped.clone());
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    let router = PsRouter::new(mgr_addr, ps_addr);
    for i in 0..20u32 {
        psr_put(&router, PART, format!("b{i:03}").as_bytes(), b"before").await;
        psr_put(&router, PART, format!("w{i:03}").as_bytes(), b"before").await;
    }
    psr_flush(&router, PART).await;
    let epoch = get_regions(&mgr)
        .await
        .regions
        .iter()
        .find(|(id, _)| *id == PART)
        .map(|(_, r)| r.region_epoch)
        .unwrap();

    let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None).await.unwrap();
    let resp = admin
        .call(
            MSG_OP_SUBMIT,
            manager_rpc::rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_SPLIT,
                part_id: PART,
                at_key: Some(b"m".to_vec()),
                requested_by: "test".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("submit split");
    let r: OpSubmitResp = manager_rpc::rkyv_decode(&resp).unwrap();
    assert_eq!(r.code, manager_rpc::CODE_OK, "split refused: {}", r.message);
    let committed = poll_until_async(Duration::from_secs(30), Duration::from_millis(50), || {
        let mgr = mgr.clone();
        let dropped = dropped.clone();
        async move { dropped.load(Ordering::SeqCst) >= 1 && get_regions(&mgr).await.regions.len() == 2 }
    })
    .await;
    assert!(committed, "the split never committed behind a lost reply");
    epoch
}

#[test]
fn writes_acked_after_a_split_whose_reply_was_lost_survive_a_reopen() {
    let rt = compio::runtime::Runtime::new().unwrap();
    let c = cluster(&rt);
    let mgr_addr = c.mgr_addr;
    let ps_addr = pick_addr();
    let mut ps = start_partition_server_killable_with(PS_ID, c.front, ps_addr, &["--region-sync-interval-secs", "60"]);
    let acked = rt.block_on(async {
        let epoch = seed_and_split(&c, ps_addr).await;
        let router = PsRouter::new(mgr_addr, ps_addr);

        // A client still holding the pre-split layout writes both halves for
        // 20 s, past the PS's wait for its split outcome to be reported.
        // Whatever the PS acknowledges must survive.
        let c = router.client_for(PART).await;
        let mut todo: Vec<String> = (0..20u32).flat_map(|i| [format!("c{i:03}"), format!("x{i:03}")]).collect();
        let mut acked = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !todo.is_empty() && Instant::now() < deadline {
            let mut left = Vec::new();
            for key in todo {
                if put_at(&c, PART, key.as_bytes(), epoch).await {
                    acked.push(key);
                } else {
                    left.push(key);
                }
            }
            todo = left;
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        acked
    });
    eprintln!("acked after the split: {} writes", acked.len());

    // Crash, and reopen every partition on a fresh PS.
    ps.kill();
    let ps_addr2 = pick_addr();
    let _ps2 = start_partition_server_killable(PS_ID, mgr_addr, ps_addr2);
    rt.block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 2, "the split stands");
        let router = PsRouter::new(mgr_addr, ps_addr2);
        let read = |key: String| {
            let router = &router;
            let pid = owner(&regions, key.as_bytes());
            async move {
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    if let Ok(c) = router.try_client_for(pid).await {
                        if let Ok(r) = try_ps_get(&c, pid, key.as_bytes()).await {
                            if r.code == partition_rpc::CODE_OK || r.code == partition_rpc::CODE_NOT_FOUND {
                                return r.code == partition_rpc::CODE_OK;
                            }
                        }
                    }
                    if Instant::now() > deadline {
                        return false;
                    }
                    compio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        };
        for i in 0..20u32 {
            assert!(read(format!("b{i:03}")).await, "a key written before the split is gone");
        }
        let mut lost = Vec::new();
        for key in &acked {
            if !read(key.clone()).await {
                lost.push(key.clone());
            }
        }
        assert!(lost.is_empty(), "{} of {} writes acked after the split are gone: {lost:?}", lost.len(), acked.len());

        // The split's layout serves both halves.
        for key in ["d-new", "y-new"] {
            let pid = owner(&regions, key.as_bytes());
            let c = router.client_for(pid).await;
            let epoch = regions.regions.iter().find(|(id, _)| *id == pid).map(|(_, r)| r.region_epoch).unwrap();
            let mut ok = false;
            for _ in 0..100 {
                if put_at(&c, pid, key.as_bytes(), epoch).await {
                    ok = true;
                    break;
                }
                compio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(ok, "partition {pid} refused {key}");
            assert!(read(key.to_string()).await, "{key} written after the reopen is not readable");
        }
    });
}


/// The same split on a PS that reopens the partition itself (default region
/// sync): both halves serve, and what was written before the split is there.
#[test]
fn the_ps_reopens_a_partition_whose_split_reply_was_lost() {
    let rt = compio::runtime::Runtime::new().unwrap();
    let c = cluster(&rt);
    let ps_addr = pick_addr();
    let _ps = start_partition_server_killable(PS_ID, c.front, ps_addr);
    rt.block_on(async {
        seed_and_split(&c, ps_addr).await;
        let mgr = RpcClient::connect(c.mgr_addr).await.unwrap();
        let regions = get_regions(&mgr).await;
        let router = PsRouter::new(c.mgr_addr, ps_addr);
        for key in ["d-new", "y-new"] {
            let pid = owner(&regions, key.as_bytes());
            let epoch = regions.regions.iter().find(|(id, _)| *id == pid).map(|(_, r)| r.region_epoch).unwrap();
            let served = poll_until_async(Duration::from_secs(60), Duration::from_millis(200), || {
                let router = &router;
                async move {
                    match router.try_client_for(pid).await {
                        Ok(cl) => put_at(&cl, pid, key.as_bytes(), epoch).await,
                        Err(_) => false,
                    }
                }
            })
            .await;
            assert!(served, "partition {pid} never served {key} after the reopen");
        }
        for key in ["b000", "b019", "w000", "w019", "d-new", "y-new"] {
            let pid = owner(&regions, key.as_bytes());
            let cl = router.client_for(pid).await;
            let r = try_ps_get(&cl, pid, key.as_bytes()).await.expect("get");
            assert_eq!(r.code, partition_rpc::CODE_OK, "{key} missing after the reopen");
        }
    });
}
