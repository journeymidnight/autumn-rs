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
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, payload).await.code,
        CODE_OK
    );

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
        conn.append(extent_id, 1, 0, 0, vec![0x2Cu8; 512]).await.code,
        CODE_OK
    );

    let ok = conn.read_bytes_bulk(extent_id, 1, 0, 512).await;
    assert_eq!(ok.0, CODE_OK, "the bulk path still serves .dat");

    let refused = conn
        .read_bytes_bulk_from(extent_id, 1, 0, 512, PayloadRef::shard(1))
        .await;
    assert_eq!(refused.0, CODE_PAYLOAD_NOT_HERE);
}
