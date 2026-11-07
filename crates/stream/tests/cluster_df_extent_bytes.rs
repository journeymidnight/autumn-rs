//! cluster-df: EN `handle_df` self-reports its real per-disk extent footprint
//! (`DiskStatus.extent_bytes` = Σ `ExtentEntry.len`). This is the data source
//! the manager sums into `physical_used` — exact filesystem truth, no
//! amplification formula. Guards the EN half of the cluster-df feature.

mod test_helpers;

use autumn_stream::extent_rpc::CODE_OK;
use test_helpers::{pick_addr, start_node, TestConn};

#[compio::test]
async fn df_reports_summed_extent_len_per_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();
    start_node(dir.path(), addr).await;
    let conn = TestConn::new(addr);

    // Empty node: extent_bytes == 0, but real statvfs total/free are non-zero.
    let df0 = conn.df(vec![], vec![]).await;
    assert!(!df0.disk_status.is_empty(), "single disk must be reported");
    let ext0: u64 = df0.disk_status.iter().map(|(_, st)| st.extent_bytes).sum();
    assert_eq!(ext0, 0, "no extents yet → 0 footprint");
    assert!(
        df0.disk_status
            .iter()
            .all(|(_, st)| st.total > 0 && st.online),
        "statvfs total must be real + disk online"
    );

    // Two extents with known lengths on the single disk.
    assert_eq!(conn.alloc_extent(101).await.code, CODE_OK);
    assert_eq!(conn.alloc_extent(102).await.code, CODE_OK);
    assert_eq!(
        conn.append(101, 1, 0, 0, vec![0u8; 4096]).await.code,
        CODE_OK
    );
    assert_eq!(
        conn.append(102, 1, 0, 0, vec![0u8; 2048]).await.code,
        CODE_OK
    );

    // df now sums entry.len across the disk's extents (4096 + 2048).
    let df = conn.df(vec![], vec![]).await;
    let ext: u64 = df.disk_status.iter().map(|(_, st)| st.extent_bytes).sum();
    assert_eq!(
        ext,
        4096 + 2048,
        "extent_bytes must equal the summed real extent file lengths"
    );

    // A further append grows the footprint by exactly the appended bytes.
    assert_eq!(
        conn.append(101, 1, 4096, 0, vec![0u8; 1000]).await.code,
        CODE_OK
    );
    let df2 = conn.df(vec![], vec![]).await;
    let ext2: u64 = df2.disk_status.iter().map(|(_, st)| st.extent_bytes).sum();
    assert_eq!(
        ext2,
        4096 + 2048 + 1000,
        "footprint tracks live extent length"
    );
}
