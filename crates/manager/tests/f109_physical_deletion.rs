//! F109 — physical extent file deletion when refs → 0 (end-to-end).
//!
//! Drives `MSG_STREAM_PUNCH_HOLES` directly via `StreamClient` (the same
//! RPC the GC loop in autumn-partition-server eventually calls) and
//! asserts that the manager's `extent_delete_loop` fans out
//! `EXT_MSG_DELETE_EXTENT` to every replica, removing the `.dat` /
//! `.meta` files within the sweep window (default 2 s, retry up to 60).
//!
//! Skipping the partition-server stack here is deliberate: GC needs
//! >2.5 GiB of dead VPs to roll a log_stream extent at the default
//! `max_extent_size = 3 GiB`, which is impractical for an integration
//! test. Setting `max_extent_size = 1 KiB` on a raw `StreamClient`
//! produces multiple extents from a handful of small appends and lets
//! us drive the same manager-side mutation path that GC drives in
//! production.

mod support;

use std::collections::HashSet;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_stream::{ConnPool, StreamClient};
use support::*;

/// Walk a node's data dir (the `{base_dir}/{hash:02x}/` hashed layout)
/// and return the set of `extent-{id}.dat` file basenames.
fn list_dat_files(dir: &Path) -> HashSet<String> {
    let mut out = HashSet::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        let Ok(files) = std::fs::read_dir(&p) else {
            continue;
        };
        for f in files.flatten() {
            if let Some(name) = f.file_name().to_str() {
                if name.starts_with("extent-") && name.ends_with(".dat") {
                    out.insert(name.to_string());
                }
            }
        }
    }
    out
}

#[test]
fn f109_punched_extents_are_physically_unlinked() {
    let (mgr_addr, n1_addr, n2_addr, n1_dir, n2_dir) = setup_two_node_infra(7200);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 7200).await;
        let stream_id = create_stream(&mgr, 2).await;
        drop(mgr);

        // Tiny max_extent_size forces an extent roll on every append, so
        // a handful of writes produces enough extents to punch some
        // without emptying the stream (the manager rejects
        // punch_holes that would empty a stream).
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "f109-test/owner".to_string(),
            1024, // 1 KiB max_extent_size
            pool,
        )
        .await
        .expect("StreamClient::connect");
        // F099 worker requires explicit ResetTail; the easiest way to
        // populate the worker's tail cache is to call append once and
        // let alloc roll on the next call. We do 6 appends of 600 B
        // each → 6 rolls → ~6 extents added to the initial one = 7
        // extents in the stream. Punch 4, leave 3 (so the stream is
        // never empty).
        // Each append > max_extent_size guarantees a roll → one new
        // extent per call, so 7 appends + the initial create_stream
        // extent = 8 extents in the stream.
        let payload: Vec<u8> = vec![0xab; 1500];
        for _ in 0..7 {
            sc.append(stream_id, &payload, false).await.expect("append");
        }

        // Read the stream layout from manager. Capture the extent_ids
        // that exist so we can pick a punch set safely.
        let info = sc
            .get_stream_info(stream_id)
            .await
            .expect("stream_info");
        assert!(
            info.extent_ids.len() >= 5,
            "expected at least 5 extents, got {} (extent_ids={:?})",
            info.extent_ids.len(),
            info.extent_ids,
        );

        // Punch all but the last 2 extents. Manager refuses to empty
        // a stream entirely (Precondition); 2 leftover keeps that safe.
        let to_punch: Vec<u64> = info.extent_ids[..info.extent_ids.len() - 2].to_vec();
        assert!(!to_punch.is_empty(), "to_punch is empty");

        let before_n1 = list_dat_files(n1_dir.path());
        let before_n2 = list_dat_files(n2_dir.path());
        for eid in &to_punch {
            let basename = format!("extent-{eid}.dat");
            assert!(
                before_n1.contains(&basename) || before_n2.contains(&basename),
                "extent {eid} .dat missing on both nodes pre-punch (n1={:?}, n2={:?})",
                before_n1,
                before_n2,
            );
        }

        // Drive the manager refs → 0 path. Returns OK once etcd is
        // mirrored; the physical unlink happens asynchronously via
        // extent_delete_loop.
        let _ = sc
            .punch_holes(stream_id, to_punch.clone())
            .await
            .expect("punch_holes");

        // Wait for fanout. Sweep interval = 2 s; allow margin for
        // stream-worker drain + first sweep.
        let unlinked = poll_until(Duration::from_secs(8), Duration::from_millis(200), || {
            let after_n1 = list_dat_files(n1_dir.path());
            let after_n2 = list_dat_files(n2_dir.path());
            to_punch.iter().all(|eid| {
                let basename = format!("extent-{eid}.dat");
                !after_n1.contains(&basename) && !after_n2.contains(&basename)
            })
        })
        .await;

        let after_n1 = list_dat_files(n1_dir.path());
        let after_n2 = list_dat_files(n2_dir.path());
        assert!(
            unlinked,
            "F109: punched extents not unlinked on both replicas within 8 s.\n\
             punched={:?}\nbefore n1={:?}\nafter  n1={:?}\nbefore n2={:?}\nafter  n2={:?}",
            to_punch, before_n1, after_n1, before_n2, after_n2,
        );

        // Surviving (un-punched) extents must still be present on at
        // least one replica — the delete loop must not over-reach.
        for eid in &info.extent_ids[info.extent_ids.len() - 2..] {
            let basename = format!("extent-{eid}.dat");
            assert!(
                after_n1.contains(&basename) || after_n2.contains(&basename),
                "F109: surviving extent {eid} missing from both replicas",
            );
        }
    });
}

