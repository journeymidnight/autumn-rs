//! F109 — startup orphan reconcile.
//!
//! Verifies the second-line cleanup path: a `DelExtent` fanout that
//! exhausts its retry window (extent-node was offline for the entire
//! retry budget) leaves the orphan `.dat` file on disk; on the next
//! `ExtentNode` boot, `reconcile_orphans_with_manager` calls
//! `MSG_RECONCILE_EXTENTS` and the manager returns the orphan id;
//! the node unlinks the file.
//!
//! Strategy:
//!   1. Two-node cluster as usual.
//!   2. Open a stream + roll several extents (same trick as
//!      `f109_physical_deletion`: max_extent_size = 1 KiB).
//!   3. Manually unlink one extent on n1 to simulate "manager succeeded
//!      in deleting on n1 but n2 was offline" — actually simpler: kill
//!      the live ExtentNode on n2 (drop its handle isn't possible
//!      cleanly), so we instead pre-create an orphan file in n2's data
//!      dir for an extent that was NEVER allocated on the manager,
//!      then restart n2 and assert the orphan vanishes after reconcile.
//!
//!   Even simpler test (chosen): drop a synthetic `extent-99999.dat`
//!   directly into n2's data dir via std::fs (no ExtentNode involvement
//!   for the orphan), then start a fresh ExtentNode pointing at that
//!   dir with the manager already running. The reconcile call will
//!   discover 99999 is unknown → manager returns it as garbage → node
//!   unlinks. This exercises the same code path as the "left over after
//!   exhausted retries" scenario without needing to coordinate a node
//!   shutdown.

mod support;

use std::path::Path;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use support::*;

/// Drop a synthetic orphan extent into the *correct* hash subdir
/// (matches `DiskFS::extent_path`: `{disk}/{crc32c(eid_le)&0xFF:02x}/`).
/// Without the right subdir, ExtentNode::new's `load_extents` would
/// open the file at the wrong path, leaving the orphan untouched.
fn write_orphan_files(disk_dir: &Path, extent_id: u64) {
    let hash_byte = (crc32c::crc32c(&extent_id.to_le_bytes()) & 0xFF) as u8;
    let hash_dir = disk_dir.join(format!("{:02x}", hash_byte));
    std::fs::create_dir_all(&hash_dir).expect("mkdir hash subdir");
    let dat = hash_dir.join(format!("extent-{extent_id}.dat"));
    let meta = hash_dir.join(format!("extent-{extent_id}.meta"));
    std::fs::write(&dat, b"orphan-payload").expect("write orphan dat");
    std::fs::write(&meta, &[0u8; 40]).expect("write orphan meta");
    assert!(dat.exists());
    assert!(meta.exists());
}

/// Recursively check whether any `extent-{id}.dat` or `.meta` for the
/// given extent_id exists anywhere under `disk_dir`.
fn orphan_present(disk_dir: &Path, extent_id: u64) -> bool {
    let dat_name = format!("extent-{extent_id}.dat");
    let meta_name = format!("extent-{extent_id}.meta");
    let Ok(top) = std::fs::read_dir(disk_dir) else {
        return false;
    };
    for sub in top.flatten() {
        let p = sub.path();
        if !p.is_dir() {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&p) else {
            continue;
        };
        for f in entries.flatten() {
            let n = f.file_name();
            let ns = n.to_string_lossy();
            if ns == dat_name || ns == meta_name {
                return true;
            }
        }
    }
    false
}

#[test]
fn f109_startup_reconcile_unlinks_orphans() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    // Pre-create an orphan in a tempdir BEFORE the extent-node opens it,
    // so the orphan will be discovered by `load_extents` and then asked
    // about via `reconcile_orphans_with_manager`.
    let n_dir = tempfile::tempdir().expect("tempdir");
    let orphan_id: u64 = 0xdead_beef_0042;
    write_orphan_files(n_dir.path(), orphan_id);

    // Start the extent node with a manager_endpoint set so the reconcile
    // call has a target. Use a custom ExtentNodeConfig because the
    // support helper doesn't expose manager_endpoint config.
    let n_addr = pick_addr();
    let n_dir_path = n_dir.path().to_path_buf();
    let mgr_addr_str = mgr_addr.to_string();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let cfg = ExtentNodeConfig::new(n_dir_path, 8001)
                .with_manager_endpoint(mgr_addr_str);
            let n = ExtentNode::new(cfg).await.expect("extent node new");
            let _ = n.serve(n_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(400));

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // Register the node so the manager has a `MgrNodeInfo` for it
        // (not strictly needed for reconcile, but matches a real boot).
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let _ = mgr; // keep alive
        register_node(
            &RpcClient::connect(mgr_addr).await.expect("mgr"),
            &n_addr.to_string(),
            "uuid-f109-rec-1",
        )
        .await;

        // Reconcile happens during ExtentNode::new (BEFORE serve()), so
        // by the time we observe the dir, the orphan should already be
        // unlinked. Poll briefly to absorb any startup jitter.
        let unlinked = poll_until(
            Duration::from_secs(4),
            Duration::from_millis(100),
            || !orphan_present(n_dir.path(), orphan_id),
        )
        .await;

        assert!(
            unlinked,
            "F109 reconcile: orphan extent {orphan_id} still present in {}",
            n_dir.path().display(),
        );
    });
}
