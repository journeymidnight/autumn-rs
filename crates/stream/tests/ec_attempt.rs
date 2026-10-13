//! One EC conversion attempt (one nonce) runs at most once on its coordinator:
//! a re-send of the running attempt is RUNNING, of a failed one FAILED with the
//! target that failed (not started again), and a NEWER nonce supersedes it.

mod test_helpers;

use std::time::Duration;

use autumn_stream::extent_rpc::{
    ConvertToEcReq, CODE_OK, CODE_PRECONDITION, EC_ATTEMPT_FAILED, EC_ATTEMPT_RUNNING,
    EC_ATTEMPT_STARTED,
};
use test_helpers::{pick_addr, start_node, TestConn};

const EXTENT: u64 = 7101;

/// A coordinator holding a sealed 4 KiB extent at eversion 1.
async fn coordinator() -> (tempfile::TempDir, std::net::SocketAddr, TestConn) {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();
    let node = start_node(dir.path(), addr).await;
    let conn = TestConn::new(addr);
    assert_eq!(conn.alloc_extent(EXTENT).await.code, CODE_OK);
    assert_eq!(conn.append(EXTENT, 1, 0, 0, vec![7u8; 4096]).await.code, CODE_OK);
    node.test_seal_local(EXTENT, 4096, 1).await.expect("seal");
    (dir, addr, conn)
}

fn req(coord: std::net::SocketAddr, others: [String; 2], nonce: u64) -> ConvertToEcReq {
    ConvertToEcReq {
        extent_id: EXTENT,
        data_shards: 2,
        parity_shards: 1,
        target_addrs: vec![coord.to_string(), others[0].clone(), others[1].clone()],
        eversion: 2,
        owner_epoch: 0,
        attempt_nonce: nonce,
    }
}

/// The attempt fails on its first participant write (nothing listens there).
/// Re-sending it reports that failure, naming target 1, and does not encode
/// again; the manager cancels the marker and derives a new attempt.
#[compio::test]
async fn a_failed_attempt_names_its_target_and_is_not_started_again() {
    let (_dir, addr, conn) = coordinator().await;
    let dead = [pick_addr().to_string(), pick_addr().to_string()];
    let first = conn.convert_to_ec(&req(addr, dead.clone(), 10)).await;
    assert_eq!((first.code, first.state), (CODE_OK, EC_ATTEMPT_STARTED), "{first:?}");

    let mut answer = None;
    for _ in 0..100 {
        compio::time::sleep(Duration::from_millis(50)).await;
        let r = conn.convert_to_ec(&req(addr, dead.clone(), 10)).await;
        if r.state != EC_ATTEMPT_RUNNING {
            answer = Some(r);
            break;
        }
    }
    let r = answer.expect("the attempt ended");
    assert_eq!(r.code, CODE_OK, "{r:?}");
    assert_eq!(r.state, EC_ATTEMPT_FAILED, "{r:?}");
    assert_eq!(r.failed_target, Some(1), "the first participant failed: {r:?}");
    let again = conn.convert_to_ec(&req(addr, dead, 10)).await;
    assert_eq!(again.state, EC_ATTEMPT_FAILED, "still not started again: {again:?}");
}

/// While an attempt runs (its first participant accepts the connection and
/// never answers), a re-send of it is RUNNING, an OLDER attempt is refused, and
/// a NEWER one is started — after which the old nonce is the refused one.
#[compio::test]
async fn a_newer_attempt_supersedes_the_running_one() {
    let (_dir, addr, conn) = coordinator().await;
    let silent = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let others = [silent.local_addr().unwrap().to_string(), pick_addr().to_string()];

    let r = conn.convert_to_ec(&req(addr, others.clone(), 20)).await;
    assert_eq!(r.state, EC_ATTEMPT_STARTED, "{r:?}");
    compio::time::sleep(Duration::from_millis(100)).await;
    let r = conn.convert_to_ec(&req(addr, others.clone(), 20)).await;
    assert_eq!((r.code, r.state), (CODE_OK, EC_ATTEMPT_RUNNING), "{r:?}");
    let r = conn.convert_to_ec(&req(addr, others.clone(), 19)).await;
    assert_eq!(r.code, CODE_PRECONDITION, "an older attempt is refused: {r:?}");

    let r = conn.convert_to_ec(&req(addr, others.clone(), 21)).await;
    assert_eq!((r.code, r.state), (CODE_OK, EC_ATTEMPT_STARTED), "{r:?}");
    let r = conn.convert_to_ec(&req(addr, others.clone(), 21)).await;
    assert_eq!(r.state, EC_ATTEMPT_RUNNING, "{r:?}");
    let r = conn.convert_to_ec(&req(addr, others, 20)).await;
    assert_eq!(r.code, CODE_PRECONDITION, "the superseded attempt is refused: {r:?}");
    drop(silent);
}
