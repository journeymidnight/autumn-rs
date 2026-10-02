//! A write that reached ONE of three replicas, then the writer crashed.
//!
//! The record never reached two of the three log replicas (their nodes were
//! down), so the append was never acked. The writer then crashed, and its
//! successor opened the stream while only the replica that did take the
//! record was up. One answering replica is enough to seal: it holds every
//! acked byte, and here one more, the un-acked record, which becomes part of
//! the sealed extent (data gained, never lost). Then the other two nodes come
//! back holding a SHORTER copy than the seal. The manager must bring them up to
//! the sealed length on its own, with the survivor's bytes, so the extent is
//! back to three good copies.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, CodeResp, FenceNodeReq, ReportCorruptReplicaReq,
    ReportCorruptReplicaResp, CODE_OK, MSG_FENCE_NODE, MSG_REPORT_CORRUPT_REPLICA,
};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

/// The extent's `.dat` under `dir` (hashed `{base}/{hh}/` layout), if any.
fn dat_path(dir: &std::path::Path, extent_id: u64) -> Option<std::path::PathBuf> {
    let name = format!("extent-{extent_id}.dat");
    std::fs::read_dir(dir).ok()?.flatten().find_map(|sub| {
        let p = sub.path().join(&name);
        p.is_file().then_some(p)
    })
}

/// A stoppable extent node that knows its manager, as every production node
/// does (`--manager`): repairing a short replica asks the manager for the
/// extent's seal.
fn start_node(
    addr: std::net::SocketAddr,
    dir: std::path::PathBuf,
    disk_id: u64,
    mgr: std::net::SocketAddr,
) -> (ShutdownFlag, std::thread::JoinHandle<()>) {
    let flag = ShutdownFlag::new();
    let stop = flag.clone();
    let handle = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let cfg = autumn_stream::ExtentNodeConfig::new(dir, disk_id)
                .with_manager_endpoint(mgr.to_string());
            let node = autumn_stream::ExtentNode::new(cfg).await.expect("extent node");
            compio::runtime::spawn(async move {
                if let Err(e) = node.serve(addr).await {
                    eprintln!("extent node {addr} stopped serving: {e}");
                }
            })
            .detach();
            while !stop.is_shutdown() {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    (flag, handle)
}

fn dat_len(dir: &std::path::Path, extent_id: u64) -> Option<u64> {
    dat_path(dir, extent_id).map(|p| std::fs::metadata(p).unwrap().len())
}

#[test]
fn shorter_replicas_rejoining_after_a_single_replica_seal_are_brought_up() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    const N: usize = 5;
    let dirs: Vec<_> = (0..N).map(|_| tempfile::tempdir().expect("tmpdir")).collect();
    let addrs: Vec<_> = (0..N).map(|_| pick_addr()).collect();
    let disks: Vec<u64> = (0..N)
        .map(|i| format_node(mgr_addr, addrs[i], &format!("uuid-single-wal-{i}")))
        .collect();
    let mut nodes: Vec<_> = (0..N)
        .map(|i| Some(start_node(addrs[i], dirs[i].path().to_path_buf(), disks[i], mgr_addr)))
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let stream_id = create_stream(&mgr, 3).await;
        drop(mgr);
        let owner = "single-wal-survivor/owner".to_string();

        // Record 1 reaches all three replicas and is acked.
        let first = StreamClient::connect(
            &mgr_addr.to_string(),
            owner.clone(),
            1 << 30,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("first writer");
        let rec1 = vec![0x11_u8; 4096];
        let r1 = first.append(stream_id, &rec1).await.expect("record 1");
        let extent = r1.extent_id;
        let len1 = r1.end as u64;
        let holders: Vec<usize> = (0..N).filter(|&i| dat_path(dirs[i].path(), extent).is_some()).collect();
        assert_eq!(holders.len(), 3, "RF 3 extent on {holders:?}");
        let (survivor, lost) = (holders[0], [holders[1], holders[2]]);

        // Two replicas' nodes go down; record 2 reaches only the survivor, and
        // the writer crashes before it can retry, seal or roll.
        for &i in &lost {
            let (flag, handle) = nodes[i].take().unwrap();
            flag.shutdown();
            handle.join().expect("join extent node");
        }
        let rec2 = vec![0x22_u8; 4096];
        let attempt = compio::time::timeout(Duration::from_millis(60), first.append(stream_id, &rec2)).await;
        assert!(
            !matches!(attempt, Ok(Ok(_))),
            "record 2 was acked with two of three replicas down"
        );
        drop(first);
        let sealed_len = len1 + rec2.len() as u64;
        let start = Instant::now();
        while dat_len(dirs[survivor].path(), extent) != Some(sealed_len) {
            assert!(start.elapsed() < Duration::from_secs(5), "record 2 never reached the survivor");
            compio::time::sleep(Duration::from_millis(20)).await;
        }
        for &i in &lost {
            assert_eq!(dat_len(dirs[i].path(), extent), Some(len1), "node {i} took record 2");
        }

        // Let the manager's health poll see the two nodes down, so new
        // extents avoid them.
        compio::time::sleep(Duration::from_secs(6)).await;

        // The successor opens with only the survivor up: one answer is enough.
        let second = StreamClient::connect(
            &mgr_addr.to_string(),
            owner.clone(),
            1 << 30,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("second writer");
        assert_eq!(
            second.commit_length(stream_id).await.expect("commit length"),
            sealed_len,
            "the single answering replica's length, record 2 included"
        );
        let r3 = second.append(stream_id, &[0x33_u8; 4096]).await.expect("record 3 after a single-replica seal");
        assert_ne!(r3.extent_id, extent, "record 3 must land on a fresh tail");
        second.invalidate_extent_cache(extent);
        let ex = second.get_extent_info(extent).await.expect("extent info");
        assert!(ex.sealed, "the old tail is sealed");
        assert_eq!(ex.sealed_length, sealed_len, "sealed at the survivor's length");

        // The two nodes come back, each holding the shorter copy.
        for &i in &lost {
            nodes[i] = Some(start_node(addrs[i], dirs[i].path().to_path_buf(), disks[i], mgr_addr));
        }

        // The manager brings both up to the seal, with the survivor's bytes.
        let all_bits = (1u32 << ex.replicates.len()) - 1;
        let start = Instant::now();
        loop {
            second.invalidate_extent_cache(extent);
            let ex = second.get_extent_info(extent).await.expect("extent info");
            let lens: Vec<_> = lost.iter().map(|&i| dat_len(dirs[i].path(), extent)).collect();
            if ex.avali == all_bits && lens.iter().all(|l| *l == Some(sealed_len)) {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "60 s after the two nodes returned: avali {:#b} (want {all_bits:#b}), their copies {lens:?} (want {sealed_len})",
                ex.avali
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        let want = [rec1, rec2].concat();
        for slot in 0..ex.replicates.len() {
            let (bytes, _, _) = second
                .read_committed_from_replica(extent, slot, 0, sealed_len)
                .await
                .expect("read a replica");
            assert!(bytes == want, "replica in slot {slot} differs from the survivor");
        }
    });
    drop(nodes);
}

/// A slot marked CORRUPT is never a source, for the catch-up or for anything
/// else that copies a whole extent — even when it is the only full-length copy
/// in reach. (A dark slot that is merely behind still is one.)
///
/// Extent [A, C, B] in slot order, sealed at 8192 while B was down: A and C
/// hold records 1 and 2, B only record 1. Then A's bytes rot and its partition
/// owner reports it: A is isolated (bit clear, marked corrupt), full length.
/// When B returns, the loop catches it up in place, and the node copies from
/// its peers. A source is accepted on LENGTH, and A has the full length — so a
/// walk in slot order that does not skip dark copies takes A's rot, B comes
/// back "available" holding it, and the next rebuild that reads B spreads it.
/// B must come back with C's bytes.
#[test]
fn a_catch_up_never_copies_from_a_dark_replica() {
    const PART: u64 = 77;
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    const N: usize = 5;
    let dirs: Vec<_> = (0..N).map(|_| tempfile::tempdir().expect("tmpdir")).collect();
    let addrs: Vec<_> = (0..N).map(|_| pick_addr()).collect();
    let uuid = |i: usize| format!("uuid-dark-source-{i}");
    let disks: Vec<u64> = (0..N).map(|i| format_node(mgr_addr, addrs[i], &uuid(i))).collect();
    let mut nodes: Vec<_> = (0..N)
        .map(|i| Some(start_node(addrs[i], dirs[i].path().to_path_buf(), disks[i], mgr_addr)))
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let mut node_ids = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            node_ids.push(register_node(&mgr, &addr.to_string(), &uuid(i)).await.node_id);
        }
        let log = create_stream(&mgr, 3).await;
        let row = create_stream(&mgr, 1).await;
        let meta = create_stream(&mgr, 1).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
        let owner = format!("partition/{PART}");
        let mgr_str = mgr_addr.to_string();
        let connect =
            || StreamClient::connect(&mgr_str, owner.clone(), 1 << 30, Rc::new(ConnPool::new()));

        let first = connect().await.expect("first writer");
        let rec1 = vec![0x11_u8; 4096];
        let extent = first.append(log, &rec1).await.expect("record 1").extent_id;
        let members = first.get_extent_info(extent).await.expect("extent info").replicates;
        let index_of = |id: u64| node_ids.iter().position(|n| *n == id).unwrap();
        // A first in slot order (the source a slot-order walk tries first),
        // B the one that misses the seal.
        let (a, c, b) = (index_of(members[0]), index_of(members[1]), index_of(members[2]));
        let spares: Vec<usize> = (0..N).filter(|i| ![a, b, c].contains(i)).collect();

        let (flag, handle) = nodes[b].take().unwrap();
        flag.shutdown();
        handle.join().expect("join extent node");
        let rec2 = vec![0x22_u8; 4096];
        let attempt = compio::time::timeout(Duration::from_millis(60), first.append(log, &rec2)).await;
        assert!(!matches!(attempt, Ok(Ok(_))), "record 2 was acked with a replica down");
        drop(first);
        let sealed_len = 8192u64;
        let start = Instant::now();
        while [a, c].iter().any(|&i| dat_len(dirs[i].path(), extent) != Some(sealed_len)) {
            assert!(start.elapsed() < Duration::from_secs(5), "record 2 never reached A and C");
            compio::time::sleep(Duration::from_millis(20)).await;
        }
        compio::time::sleep(Duration::from_secs(6)).await;

        // Seal with B down: A and C answer, so B's bit stays clear.
        let second = connect().await.expect("second writer");
        let r3 = second.append(log, &[0x33_u8; 4096]).await.expect("record 3");
        assert_ne!(r3.extent_id, extent);
        second.invalidate_extent_cache(extent);
        let ex = second.get_extent_info(extent).await.expect("extent info");
        assert!(ex.sealed && ex.sealed_length == sealed_len, "sealed at {}", ex.sealed_length);
        assert_eq!(ex.avali, 0b011, "A and C answered the seal, B did not");

        // No node may take a rebuild of A, so B's catch-up is the only copy
        // that happens and A stays a dark, full-length member.
        for &i in &spares {
            let (flag, handle) = nodes[i].take().unwrap();
            flag.shutdown();
            handle.join().expect("join extent node");
        }

        // A rots at full length, and its owner reports it.
        let a_path = dat_path(dirs[a].path(), extent).expect("A's copy");
        std::fs::write(&a_path, vec![0xEE_u8; sealed_len as usize]).expect("rot A");
        let resp = mgr
            .call(
                MSG_REPORT_CORRUPT_REPLICA,
                rkyv_encode(&ReportCorruptReplicaReq {
                    partition_id: PART,
                    owner_epoch: second.owner_epoch(),
                    log_stream_id: log,
                    extent_id: extent,
                    eversion: ex.eversion,
                    corrupt_node_ids: vec![node_ids[a]],
                }),
            )
            .await
            .expect("report corrupt");
        let rep: ReportCorruptReplicaResp = rkyv_decode(&resp).expect("decode report");
        assert_eq!(rep.code, CODE_OK, "report refused: {}", rep.message);

        // B returns while C — the one lit copy — is away: the only full-length
        // copy B could reach is A's rot, and it must not take it.
        let (flag, handle) = nodes[c].take().unwrap();
        flag.shutdown();
        handle.join().expect("join extent node");
        nodes[b] = Some(start_node(addrs[b], dirs[b].path().to_path_buf(), disks[b], mgr_addr));
        compio::time::sleep(Duration::from_secs(12)).await;
        second.invalidate_extent_cache(extent);
        let ex = second.get_extent_info(extent).await.expect("extent info");
        assert_eq!(
            (ex.avali & 0b100, dat_len(dirs[b].path(), extent)),
            (0, Some(4096)),
            "with C away, B was filled from A's corrupt copy"
        );

        // C returns; B is caught up from it.
        nodes[c] = Some(start_node(addrs[c], dirs[c].path().to_path_buf(), disks[c], mgr_addr));
        let start = Instant::now();
        loop {
            second.invalidate_extent_cache(extent);
            let ex = second.get_extent_info(extent).await.expect("extent info");
            if ex.avali & 0b100 != 0 && dat_len(dirs[b].path(), extent) == Some(sealed_len) {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "60 s after B returned: avali {:#b}, B's copy {:?}",
                ex.avali,
                dat_len(dirs[b].path(), extent)
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        let b_bytes = std::fs::read(dat_path(dirs[b].path(), extent).unwrap()).unwrap();
        assert!(
            b_bytes == [rec1, rec2].concat(),
            "B was caught up from the dark, rotted copy (first byte past record 1: {:#x})",
            b_bytes[4096]
        );
    });
    drop(nodes);
}

/// The one replica that answered the seal is fenced before the other two come
/// back. It holds the only full copy — record 2 reached nothing else, and the
/// seal made it part of the extent, so a partition may already have replayed
/// it. Fencing means "move the data off", and the node is alive: its slot is
/// rebuilt FROM it, and the two returning replicas are then caught up to the
/// full seal.
///
/// Two wrong answers this pins. Reading only lit members left the rebuild no
/// source at all (the fenced member is the slot being replaced) and the
/// extent wedged until someone unfenced it. Reading the dark members but never
/// the replaced slot reconciled the rebuild DOWN to their 4096 bytes —
/// dropping record 2 while the fenced node still held it — and left both dark
/// slots unable ever to reach the seal.
#[test]
fn a_fenced_sole_seal_member_is_rebuilt_from_its_own_copy() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    const N: usize = 5;
    let dirs: Vec<_> = (0..N).map(|_| tempfile::tempdir().expect("tmpdir")).collect();
    let addrs: Vec<_> = (0..N).map(|_| pick_addr()).collect();
    let uuid = |i: usize| format!("uuid-fenced-sole-{i}");
    let disks: Vec<u64> = (0..N).map(|i| format_node(mgr_addr, addrs[i], &uuid(i))).collect();
    let mut nodes: Vec<_> = (0..N)
        .map(|i| Some(start_node(addrs[i], dirs[i].path().to_path_buf(), disks[i], mgr_addr)))
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("connect mgr");
        let mut node_ids = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            node_ids.push(register_node(&mgr, &addr.to_string(), &uuid(i)).await.node_id);
        }
        let stream_id = create_stream(&mgr, 3).await;
        let mgr_str = mgr_addr.to_string();
        let owner = "fenced-sole-member/owner".to_string();
        let connect =
            || StreamClient::connect(&mgr_str, owner.clone(), 1 << 30, Rc::new(ConnPool::new()));

        let first = connect().await.expect("first writer");
        let extent = first.append(stream_id, &[0x11_u8; 4096]).await.expect("record 1").extent_id;
        let sealed_len = 8192u64;
        let holders: Vec<usize> =
            (0..N).filter(|&i| dat_path(dirs[i].path(), extent).is_some()).collect();
        let (sole, dark) = (holders[0], [holders[1], holders[2]]);
        let rec1 = vec![0x11_u8; 4096];
        let rec2 = vec![0x22_u8; 4096];
        for &i in &dark {
            let (flag, handle) = nodes[i].take().unwrap();
            flag.shutdown();
            handle.join().expect("join extent node");
        }
        let attempt =
            compio::time::timeout(Duration::from_millis(60), first.append(stream_id, &rec2)).await;
        assert!(!matches!(attempt, Ok(Ok(_))), "record 2 was acked with two replicas down");
        drop(first);
        compio::time::sleep(Duration::from_secs(6)).await;

        let second = connect().await.expect("second writer");
        second.append(stream_id, &[0x33_u8; 4096]).await.expect("record 3");
        second.invalidate_extent_cache(extent);
        let ex = second.get_extent_info(extent).await.expect("extent info");
        let sole_slot = ex.replicates.iter().position(|n| *n == node_ids[sole]).unwrap();
        assert!(ex.sealed, "the old tail is sealed");
        assert_eq!(ex.sealed_length, sealed_len, "sealed with record 2, from the sole member");
        assert_eq!(ex.avali, 1 << sole_slot, "only the sole member answered the seal");

        // Fence the sole member while the other two are still away.
        let resp = mgr
            .call(
                MSG_FENCE_NODE,
                rkyv_encode(&FenceNodeReq {
                    node_id: node_ids[sole],
                    reason: "decommission".to_string(),
                    set_by: "test".to_string(),
                    force: true,
                }),
            )
            .await
            .expect("fence");
        let f: CodeResp = rkyv_decode(&resp).expect("decode fence");
        assert_eq!(f.code, CODE_OK, "fence failed: {}", f.message);
        compio::time::sleep(Duration::from_secs(4)).await;

        for &i in &dark {
            nodes[i] = Some(start_node(addrs[i], dirs[i].path().to_path_buf(), disks[i], mgr_addr));
        }
        let all_bits = (1u32 << ex.replicates.len()) - 1;
        let start = Instant::now();
        let ex = loop {
            second.invalidate_extent_cache(extent);
            let ex = second.get_extent_info(extent).await.expect("extent info");
            if !ex.replicates.contains(&node_ids[sole]) && ex.avali == all_bits {
                break ex;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "60 s after the dark replicas returned: fenced member still in {:?}, avali \
                 {:#b} (want {all_bits:#b})",
                ex.replicates,
                ex.avali
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        };
        let want = [rec1, rec2].concat();
        for slot in 0..ex.replicates.len() {
            let (bytes, _, _) = second
                .read_committed_from_replica(extent, slot, 0, sealed_len)
                .await
                .expect("read a replica");
            assert!(
                bytes == want,
                "slot {slot} holds {} bytes, not records 1 and 2 — the fenced member's copy \
                 was not the source",
                bytes.len()
            );
        }
    });
    drop(nodes);
}
