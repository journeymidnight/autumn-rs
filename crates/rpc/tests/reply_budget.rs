//! A bulk reply received from the stream takes its buffer from the `Reply`
//! budget. With this thread's share held, the reply is read past and the caller
//! gets `Unavailable`; the connection stays in step, so the next reply lands
//! whole. A small reply, decoded with its frame, is already in memory and is
//! taken anyway. A 64 MiB cap gives a thread about 12.8 MiB of `Reply`.
#[path = "support/protocol.rs"]
mod protocol;
#[path = "support/status_peer.rs"]
#[allow(dead_code)] // shared fixture; this suite uses part of it
mod peer;

use autumn_rpc::client::RpcClient;
use autumn_rpc::{Frame, RpcError, StatusCode};
use autumn_transport::PoolUse;
use bytes::Bytes;
use peer::*;

const MIB: usize = 1 << 20;
const BULK: u8 = 0xb0;
const SMALL: u8 = 0xb1;

fn value() -> Vec<u8> {
    (0..4 * MIB).map(|i| (i as u8).wrapping_mul(31)).collect()
}

fn small() -> Vec<u8> {
    (0..4096).map(|i| (i as u8).wrapping_mul(13)).collect()
}

#[compio::test]
async fn a_bulk_reply_with_no_buffer_is_refused_and_the_next_one_lands() {
    assert!(autumn_transport::set_regpool_cap_bytes(64 * MIB), "this process sets the cap first");
    let _ = autumn_transport::current_or_init();
    let p = Peer::start(|frame| match frame.msg_type {
        BULK => Reply::Frame(Frame::response_zc(frame.req_id, BULK, Bytes::from_static(&[0]), Bytes::from(value()))),
        SMALL => Reply::Frame(Frame::response_zc(frame.req_id, SMALL, Bytes::from_static(&[0]), Bytes::from(small()))),
        _ => respond(frame),
    })
    .await;
    let client = RpcClient::connect(p.addr.parse().unwrap()).await.unwrap();

    // The read loop runs on this thread: hold its share of `Reply`.
    let mut held = Vec::new();
    for size in [MIB, 64 << 10, 4 << 10] {
        while let Ok(pb) = autumn_transport::regpool_try_acquire_for(PoolUse::Reply, size) {
            held.push(pb);
        }
    }
    let refused = || autumn_transport::regpool_snapshot().uses[PoolUse::Reply as usize].refused;
    let before = refused();
    let reply = client.call_into_pooled(SMALL, Bytes::new()).await.expect("a decoded reply is taken");
    assert_eq!(reply.buf.filled(), small().as_slice());
    assert_eq!(refused(), before, "a reply taken anyway is not counted as refused");
    match client.call_into_pooled(BULK, Bytes::new()).await {
        Err(RpcError::Status { code: StatusCode::Unavailable, .. }) => {}
        other => panic!("a reply with no buffer must be refused Unavailable, got {:?}", other.map(|r| r.buf.len())),
    }
    assert_eq!(refused(), before + 1, "the refusal is counted once");
    drop(held);
    let reply = compio::time::timeout(std::time::Duration::from_secs(10), client.call_into_pooled(BULK, Bytes::new()))
        .await
        .expect("the connection lost its place in the stream")
        .expect("the connection goes on");
    assert_eq!(reply.buf.filled(), value().as_slice(), "the refused value was read past exactly");
}
