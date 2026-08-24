//! The upgrade precondition for deleting the commit-phase repair code.
//!
//! Conversion no longer creates `extent-{id}.ec.dat` or `extent-{id}.ec.commit`
//! — it stages to `extent-{id}.shard{i}` and the manager's layout flip is the
//! only commit point. So either file can only have come from a node that ran
//! the pre-copy-on-write binary, and it is exactly what the RETAINED repair
//! path exists to finish.
//!
//! Deleting that path is safe only once no node holds such state. These tests
//! cover the check that turns "we think it's clean" into "every node said so".

mod test_helpers;

use autumn_stream::{ExtentNode, ExtentNodeConfig};

/// Give the node a `.dat` so there is a real extent, then plant the legacy file
/// beside it.
async fn seed_with(dir: &std::path::Path, extent_id: u64, legacy_suffix: Option<&str>) {
    let addr = test_helpers::pick_addr();
    test_helpers::start_node(dir, addr).await;
    let conn = test_helpers::TestConn::new(addr);
    assert_eq!(
        conn.alloc_extent(extent_id).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, vec![0x11u8; 256]).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    let Some(suffix) = legacy_suffix else { return };
    for byte in 0u8..=255 {
        let sub = dir.join(format!("{byte:02x}"));
        if sub.join(format!("extent-{extent_id}.dat")).exists() {
            // 4 bytes: too short to be a valid `[eversion][sealed_length]`
            // marker, so the replay cannot finish it and quarantines instead —
            // which is precisely the "unresolved residue" this check is for.
            // (16 well-formed bytes would parse as VALID and be consumed.)
            let body = if suffix == "ec.commit" { vec![9u8; 4] } else { vec![9u8; 16] };
            std::fs::write(sub.join(format!("extent-{extent_id}.{suffix}")), body)
                .expect("plant legacy file");
            return;
        }
    }
    panic!("no .dat for extent {extent_id}");
}

async fn open_node(dir: &std::path::Path, refuse: bool) -> anyhow::Result<ExtentNode> {
    ExtentNode::new(
        ExtentNodeConfig::new(dir.to_path_buf(), 1).with_refuse_legacy_ec_state(refuse),
    )
    .await
}

/// A node holding old-scheme staging still STARTS by default. The repair path
/// handles it, so refusing would break an upgrade that works — the check exists
/// to make the state visible, not to block it.
#[compio::test]
async fn legacy_staging_is_reported_but_does_not_block_startup() {
    let d = tempfile::tempdir().expect("tempdir");
    seed_with(d.path(), 9300, Some("ec.dat")).await;
    assert!(
        open_node(d.path(), false).await.is_ok(),
        "the default must not refuse a state the retained repair path owns"
    );
}

/// With the flag, the same node refuses — this is how an operator establishes
/// fleet-wide that nothing holds old-scheme state.
#[compio::test]
async fn legacy_staging_refuses_startup_under_the_flag() {
    let d = tempfile::tempdir().expect("tempdir");
    seed_with(d.path(), 9301, Some("ec.dat")).await;
    let msg = match open_node(d.path(), true).await {
        Ok(_) => panic!("must refuse while legacy staging is present"),
        Err(e) => format!("{e:#}"),
    };
    assert!(msg.contains("9301"), "the refusal must name the extent: {msg}");
    assert!(
        msg.contains("legacy EC state"),
        "the refusal must say what it found: {msg}"
    );
}

/// An unresolvable `.ec.commit` marker counts too. `load_extents` quarantines
/// what its replay cannot finish and KEEPS the marker, so what survives to this
/// check is genuinely unresolved residue.
#[compio::test]
async fn an_unresolved_commit_marker_refuses_startup_under_the_flag() {
    let d = tempfile::tempdir().expect("tempdir");
    seed_with(d.path(), 9302, Some("ec.commit")).await;
    let msg = match open_node(d.path(), true).await {
        Ok(_) => panic!("must refuse while an unresolved commit marker is present"),
        Err(e) => format!("{e:#}"),
    };
    assert!(msg.contains("9302"), "the refusal must name the extent: {msg}");
}

/// The direction that decides whether this flag is usable at all: a node with
/// no old-scheme state must start CLEANLY under it. The flag is meant to be
/// turned on fleet-wide, so a false positive would be indistinguishable from
/// the state it is looking for — and would stall the very upgrade it gates.
#[compio::test]
async fn a_clean_node_starts_under_the_flag() {
    let d = tempfile::tempdir().expect("tempdir");
    seed_with(d.path(), 9303, None).await;
    // A converted extent's own files must not look legacy either.
    for byte in 0u8..=255 {
        let sub = d.path().join(format!("{byte:02x}"));
        if sub.join("extent-9303.dat").exists() {
            std::fs::write(sub.join("extent-9303.shard1"), vec![7u8; 64]).expect("plant shard");
            std::fs::write(sub.join("extent-9303.ec.prepared"), vec![0u8; 16])
                .expect("plant prepared marker");
            break;
        }
    }
    assert!(
        open_node(d.path(), true).await.is_ok(),
        "a node with only current-scheme files must start under the flag"
    );
}
