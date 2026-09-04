//! A read NAMES the payload file it wants, and the node serves that file or
//! says it does not have it.
//!
//! The alternative — inferring which file to serve — is what makes shard bytes
//! reachable through a request that meant the whole value, and vice versa. Both
//! forms can exist on one node at once, so inference has no safe answer.

mod test_helpers;

use autumn_stream::extent_rpc::{PayloadRef, CODE_OK, CODE_PAYLOAD_NOT_HERE};
use test_helpers::{pick_addr, start_node, TestConn};

/// The node holds `.dat`, so a request naming `.dat` is served from it.
#[compio::test]
async fn a_request_for_dat_is_served_from_dat() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9101;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);
    let payload = vec![0x5Au8; 1024];
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, payload.clone()).await.code,
        CODE_OK
    );

    let rd = conn
        .read_bytes_from(extent_id, 1, 0, 1024, PayloadRef::in_dat())
        .await;
    assert_eq!(rd.code, CODE_OK);
    assert_eq!(&rd.payload[..], &payload[..]);
}

/// The node holds no shard file, so a request naming one is REFUSED — with its
/// own code, so the caller knows to refresh its layout rather than retry.
///
/// The failure this rules out is silent: falling back to `.dat` would answer a
/// shard-sized read with the head of the whole value, and the caller would RS-
/// decode it as if it were a shard.
#[compio::test]
async fn a_request_for_a_shard_file_this_node_lacks_is_refused() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9102;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);
    let payload = vec![0x7Bu8; 1024];
    assert_eq!(conn.append(extent_id, 1, 0, 0, payload).await.code, CODE_OK);

    let rd = conn
        .read_bytes_from(extent_id, 1, 0, 1024, PayloadRef::shard(2))
        .await;
    assert_eq!(
        rd.code, CODE_PAYLOAD_NOT_HERE,
        "a named file this node does not hold must be refused, never substituted"
    );
    assert!(
        rd.payload.is_empty(),
        "a refusal must carry no bytes — a short read would look like a real one"
    );
}

/// The bulk read path is a separate server branch with its own response
/// framing, and it is the one the PS uses for values. It must refuse the same
/// way.
#[compio::test]
async fn the_bulk_path_refuses_a_missing_payload_file_too() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9103;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, vec![0x2Cu8; 512])
            .await
            .code,
        CODE_OK
    );

    let ok = conn.read_bytes_bulk(extent_id, 1, 0, 512).await;
    assert_eq!(ok.0, CODE_OK, "the bulk path still serves .dat");

    let refused = conn
        .read_bytes_bulk_from(extent_id, 1, 0, 512, PayloadRef::shard(1))
        .await;
    assert_eq!(refused.0, CODE_PAYLOAD_NOT_HERE);
}

/// Locate the hashed directory an extent's files live in, by finding the `.dat`
/// the node just created. Avoids re-deriving the hash layout in the test.
fn extent_dir(root: &std::path::Path, extent_id: u64) -> std::path::PathBuf {
    for byte in 0u8..=255 {
        let sub = root.join(format!("{byte:02x}"));
        if sub.join(format!("extent-{extent_id}.dat")).exists() {
            return sub;
        }
    }
    panic!("no .dat found for extent {extent_id}");
}

/// A shard file beside a live `.dat` is ADDITIVE: each request gets its own
/// file, and neither read disturbs the other. This is the property the whole
/// CoW conversion rests on — the shard can be staged without touching the
/// replica it is derived from.
#[compio::test]
async fn a_shard_file_and_dat_coexist_without_interfering() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9110;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);
    let whole = vec![0xA1u8; 2048];
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, whole.clone()).await.code,
        CODE_OK
    );

    // Plant a shard file, as the conversion will.
    let shard = vec![0xB2u8; 683];
    let dir = extent_dir(node_dir.path(), extent_id);
    std::fs::write(dir.join(format!("extent-{extent_id}.shard1")), &shard).expect("plant shard");

    // A fresh node over the same directory discovers it at startup.
    let addr2 = pick_addr();
    start_node(node_dir.path(), addr2).await;
    let conn2 = TestConn::new(addr2);

    let dat = conn2
        .read_bytes_from(extent_id, 1, 0, 2048, PayloadRef::in_dat())
        .await;
    assert_eq!(dat.code, CODE_OK);
    assert_eq!(&dat.payload[..], &whole[..], ".dat still serves the whole value");

    let sh = conn2
        .read_bytes_from(extent_id, 1, 0, 683, PayloadRef::shard(1))
        .await;
    assert_eq!(sh.code, CODE_OK);
    assert_eq!(&sh.payload[..], &shard[..], "the shard serves its own bytes");

    let absent = conn2
        .read_bytes_from(extent_id, 1, 0, 683, PayloadRef::shard(2))
        .await;
    assert_eq!(
        absent.code, CODE_PAYLOAD_NOT_HERE,
        "a shard index this node does not hold is still refused"
    );
}

/// A node whose `.dat` was already reclaimed holds ONLY a shard. Without
/// startup discovery that shard is unreachable, unaccounted and undeletable,
/// and the extent looks absent to the manager — which is how a rebuilt copy
/// becomes a blocking orphan.
#[compio::test]
async fn a_shard_only_extent_is_discovered_at_startup() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9111;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, vec![0xC3u8; 1500]).await.code,
        CODE_OK
    );

    // Post-cleanup shape: the shard remains, `.dat` is gone, `.meta` stays.
    let shard = vec![0xD4u8; 500];
    let dir = extent_dir(node_dir.path(), extent_id);
    std::fs::write(dir.join(format!("extent-{extent_id}.shard0")), &shard).expect("plant shard");
    std::fs::remove_file(dir.join(format!("extent-{extent_id}.dat"))).expect("drop .dat");

    let addr2 = pick_addr();
    start_node(node_dir.path(), addr2).await;
    let conn2 = TestConn::new(addr2);

    let sh = conn2
        .read_bytes_from(extent_id, 1, 0, 500, PayloadRef::shard(0))
        .await;
    assert_eq!(sh.code, CODE_OK, "the shard-only extent survived the restart");
    assert_eq!(&sh.payload[..], &shard[..]);

    let dat = conn2
        .read_bytes_from(extent_id, 1, 0, 500, PayloadRef::in_dat())
        .await;
    assert_eq!(
        dat.code, CODE_PAYLOAD_NOT_HERE,
        "a reclaimed .dat must be reported missing, never re-created empty"
    );
    assert!(
        !dir.join(format!("extent-{extent_id}.dat")).exists(),
        "opening the extent must not resurrect its .dat"
    );
}

/// Deleting an extent must take its shard files with it — a shard left behind
/// is invisible to every accounting path and reappears at the next restart.
#[compio::test]
async fn deleting_an_extent_removes_its_shard_files() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9112;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, vec![0xE5u8; 256]).await.code,
        CODE_OK
    );
    let dir = extent_dir(node_dir.path(), extent_id);
    for idx in [0u32, 3] {
        std::fs::write(dir.join(format!("extent-{extent_id}.shard{idx}")), vec![1u8; 64])
            .expect("plant shard");
    }

    assert_eq!(conn.delete_extent(extent_id).await.code, CODE_OK);
    for idx in [0u32, 3] {
        assert!(
            !dir.join(format!("extent-{extent_id}.shard{idx}")).exists(),
            "shard {idx} outlived its extent"
        );
    }
    assert!(!dir.join(format!("extent-{extent_id}.dat")).exists());
}

/// Shard bytes are real bytes on a real disk. If `df` does not count them, a
/// converted cluster under-reports its footprint to cluster-df and to the
/// allocation free-space gate.
#[compio::test]
async fn df_counts_shard_bytes() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9113;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, vec![0xF6u8; 1024]).await.code,
        CODE_OK
    );

    let before: u64 = conn
        .df(vec![], vec![])
        .await
        .disk_status
        .iter()
        .map(|(_, d)| d.extent_bytes)
        .sum();

    let dir = extent_dir(node_dir.path(), extent_id);
    std::fs::write(dir.join(format!("extent-{extent_id}.shard1")), vec![7u8; 4096])
        .expect("plant shard");

    let addr2 = pick_addr();
    start_node(node_dir.path(), addr2).await;
    let after: u64 = TestConn::new(addr2)
        .df(vec![], vec![])
        .await
        .disk_status
        .iter()
        .map(|(_, d)| d.extent_bytes)
        .sum();

    assert_eq!(
        after,
        before + 4096,
        "the shard file's bytes must appear in the disk footprint"
    );
}

/// A shard-only extent must be counted ONCE. `len` is the `.dat` length and
/// there is no `.dat`, so if the shard's bytes were also parked there, every
/// converted extent would inflate the footprint that cluster-df and the
/// allocation free-space gate both read.
#[compio::test]
async fn a_shard_only_extent_is_counted_once() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9114;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, vec![0x33u8; 2048]).await.code,
        CODE_OK
    );

    // Post-cleanup shape: shard only, `.dat` reclaimed.
    let dir = extent_dir(node_dir.path(), extent_id);
    std::fs::write(dir.join(format!("extent-{extent_id}.shard0")), vec![9u8; 700])
        .expect("plant shard");
    std::fs::remove_file(dir.join(format!("extent-{extent_id}.dat"))).expect("drop .dat");

    let addr2 = pick_addr();
    start_node(node_dir.path(), addr2).await;
    let bytes: u64 = TestConn::new(addr2)
        .df(vec![], vec![])
        .await
        .disk_status
        .iter()
        .map(|(_, d)| d.extent_bytes)
        .sum();

    assert_eq!(
        bytes, 700,
        "a shard-only extent must report exactly its shard's bytes"
    );
}

/// The manager makes exactly ONE `df` call per node and dials shard 0, but a
/// completion is queued by whichever shard OWNS the extent. Per-instance queues
/// meant every completion for an extent owned by shard 1..N was pushed where
/// nothing ever read — conversions could never commit and rebuilt replicas were
/// never applied, on (N-1)/N of a production EN's extents.
///
/// The queues are shared per NODE, so two shards over the same data dirs see
/// each other's reports; two different nodes must not.
#[compio::test]
async fn shards_of_one_node_share_their_completion_queue() {
    use autumn_stream::{ExtentNode, ExtentNodeConfig};

    let d = tempfile::tempdir().expect("tempdir");
    let other = tempfile::tempdir().expect("tempdir2");

    // Two shards of the SAME node: same data dir, different shard index.
    let shard0 = ExtentNode::new(
        ExtentNodeConfig::new(d.path().to_path_buf(), 1)
            .with_shard(0, 2, vec![String::new(), String::new()]),
    )
    .await
    .expect("shard 0");
    let shard1 = ExtentNode::new(
        ExtentNodeConfig::new(d.path().to_path_buf(), 1)
            .with_shard(1, 2, vec![String::new(), String::new()]),
    )
    .await
    .expect("shard 1");
    // A different node entirely.
    let elsewhere = ExtentNode::new(ExtentNodeConfig::new(other.path().to_path_buf(), 1))
        .await
        .expect("other node");

    shard1.test_push_ec_done(4242, 7);

    assert_eq!(
        elsewhere.test_take_ec_done(),
        vec![],
        "a different node must not see this node's completions — in-process test          clusters run several ENs, and the manager refuses an ec_done reported by          a node that is not the marker's coordinator"
    );
    assert_eq!(
        shard0.test_take_ec_done(),
        vec![(4242, 7)],
        "shard 0 serves the node's df, so it must drain what shard 1 completed"
    );
}

/// The verification must sit on the path production reads actually take.
///
/// `MSG_READ_BYTES` is intercepted in `handle_connection` and answered by
/// `build_read_future`; the `dispatch` arm that calls `handle_read_bytes` is
/// dead over the wire. A check placed there passes its own unit test and
/// protects nothing — so this test goes over a real socket, which is the only
/// way to tell the two apart.
#[compio::test]
async fn a_rotted_sealed_extent_is_refused_over_the_wire() {
    let d = tempfile::tempdir().expect("tempdir");
    let addr = test_helpers::pick_addr();
    let node = test_helpers::start_node(d.path(), addr).await;
    let conn = test_helpers::TestConn::new(addr);
    let eid = 8801u64;
    let content: Vec<u8> = (0..(2 * 1024 * 1024u32)).map(|i| (i % 251) as u8).collect();

    assert_eq!(
        conn.alloc_extent(eid).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert_eq!(
        conn.append(eid, 1, 0, 0, content.clone()).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    node.test_seal_durable(eid, content.len() as u64, 2)
        .await
        .expect("seal");

    // Clean: served over the wire, byte-exact.
    let r = conn.read_bytes(eid, 2, 0, content.len() as u64).await;
    assert_eq!(r.code, autumn_stream::extent_rpc::CODE_OK, "clean read must serve");
    assert_eq!(r.payload.as_ref(), &content[..], "clean bytes");

    // Rot one byte inside the first block, behind the node's back. `fs::write`
    // truncates the SAME inode, so the node's open fd sees the new bytes — no
    // cache to invalidate, which is what makes this a faithful stand-in for
    // media rot under a live process.
    let path = extent_dir(d.path(), eid).join(format!("extent-{eid}.dat"));
    let mut rotted = content.clone();
    rotted[123_456] ^= 0x01;
    std::fs::write(&path, &rotted).expect("rot");

    let Err(err) = conn.read_bytes_result(eid, 2, 0, content.len() as u64).await else {
        panic!("a rotted sealed extent was served over the wire with a success code");
    };
    assert!(
        err.contains("content checksum"),
        "refused for the wrong reason: {err}"
    );
}
