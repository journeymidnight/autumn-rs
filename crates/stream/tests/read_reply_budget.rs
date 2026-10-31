//! An extent node reads a bulk value into a buffer from the `Reply` budget,
//! waiting a bounded time for one; with the budget full for longer it answers
//! the read `Unavailable` (the PS then reads through its copy path), and once
//! there is room the read is served. A 64 MiB cap gives `Reply` about 25.6 MiB
//! process-wide; two helper threads hold all of it.
mod test_helpers;

use autumn_stream::extent_rpc::*;
use autumn_transport::PoolUse;
use std::time::{Duration, Instant};
use test_helpers::*;

const MIB: usize = 1 << 20;

/// Hold this thread's whole share of `Reply`.
fn hold_share() -> Vec<autumn_transport::PooledBuf> {
    let mut held = Vec::new();
    for size in [MIB, 64 << 10, 4 << 10] {
        while let Ok(pb) = autumn_transport::regpool_try_acquire_for(PoolUse::Reply, size) {
            held.push(pb);
        }
    }
    held
}

#[compio::test]
async fn a_read_with_no_reply_buffer_is_answered_after_a_bounded_wait() {
    assert!(autumn_transport::set_regpool_cap_bytes(64 * MIB), "this process sets the cap first");
    let _ = autumn_transport::current_or_init();
    let dir = tempfile::tempdir().unwrap();
    let addr = pick_addr();
    let _node = start_node(dir.path(), addr).await;
    let conn = TestConn::new(addr);
    assert_eq!(conn.alloc_extent(901).await.code, CODE_OK);
    let value: Vec<u8> = (0..4 * MIB).map(|i| (i as u8).wrapping_mul(7)).collect();
    assert_eq!(conn.append(901, 1, 0, 1, value.clone()).await.code, CODE_OK);

    let held: Vec<_> = (0..2).flat_map(|_| std::thread::spawn(hold_share).join().unwrap()).collect();
    let started = Instant::now();
    let refused = conn.try_read_exact(901, 1, 0, value.len() as u64).await;
    let err = refused.err().expect("no reply buffer: the read is refused");
    assert!(
        err.chain().any(|c| matches!(
            c.downcast_ref::<autumn_rpc::RpcError>(),
            Some(autumn_rpc::RpcError::Status { code: autumn_rpc::StatusCode::Unavailable, .. })
        )),
        "refused Unavailable: {err:#}"
    );
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(900) && waited < Duration::from_millis(2500), "after the 1 s wait: {waited:?}");

    drop(held);
    let (code, got) = conn.read_exact(901, 1, 0, value.len() as u64).await;
    assert_eq!(code, CODE_OK);
    assert_eq!(got, value);
}
