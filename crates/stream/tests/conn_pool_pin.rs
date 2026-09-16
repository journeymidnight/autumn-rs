//! A pipelined send must survive another caller evicting its connection.
//!
//! `ConnPool::send_vectored` / `send_prepared` hand back only a response
//! receiver, so the pool map used to hold the only `Rc<RpcClient>`. Dropping the
//! last one now CLOSES the connection (autumn-rpc holds the two task handles as
//! its teardown), and eviction is something OTHER tasks do: one worker's replica
//! timeout evicts the address another worker is mid-append on. Without a pin,
//! that append's frame is abandoned mid-`writev` and its caller reports a closed
//! connection — an append that was about to succeed, failed by an unrelated
//! caller's error. `PinnedRecv` carries the `Rc` next to the receiver.
//!
//! Ablation: make `PinnedRecv::_conn` a `std::rc::Weak<RpcClient>` (exactly the
//! pre-fix "the caller holds no strong ref" shape) and this test fails with
//! `slow request died with the eviction` — the eviction tears the connection
//! down while the reply is still in flight.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use autumn_rpc::frame::Frame;
use autumn_stream::ConnPool;
use bytes::Bytes;

/// Answers `SLOW` after a delay and never answers `STUCK` — the shape that
/// makes one caller time out (and evict) while another's reply is in flight.
const SLOW: u8 = 0xA0;
const STUCK: u8 = 0xA1;

#[compio::test]
async fn an_eviction_does_not_kill_another_callers_in_flight_request() {
    let _ = autumn_transport::current_or_init();

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().expect("accept");
        let mut answered = 0usize;
        // Two requests arrive; only SLOW is ever answered, 400 ms after it
        // lands — after the STUCK caller's 200 ms deadline has evicted.
        while answered < 1 {
            let mut hdr = [0u8; 10];
            if sock.read_exact(&mut hdr).is_err() {
                return answered;
            }
            let req_id = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let msg_type = hdr[4];
            let plen = u32::from_le_bytes(hdr[6..10].try_into().unwrap()) as usize;
            let mut payload = vec![0u8; plen];
            if sock.read_exact(&mut payload).is_err() {
                return answered;
            }
            if msg_type == SLOW {
                std::thread::sleep(Duration::from_millis(400));
                let resp = Frame::response(req_id, SLOW, Bytes::from_static(b"ok")).encode();
                if sock.write_all(&resp).is_err() {
                    // The client closed the connection out from under the
                    // reply — the failure this test exists to catch.
                    return answered;
                }
                answered += 1;
            }
        }
        answered
    });

    let pool = ConnPool::new();

    // Worker A: pipelined submit, reply still in flight.
    let slow = pool
        .send_vectored(&addr, SLOW, vec![Bytes::from_static(b"payload")])
        .await
        .expect("submit slow request");

    // Worker B: same address, never answered — its timeout evicts the pooled
    // client, which is the only strong `Rc` besides A's pin.
    let stuck = pool
        .call_timeout(
            &addr,
            STUCK,
            Bytes::from_static(b"payload"),
            Duration::from_millis(200),
        )
        .await;
    assert!(stuck.is_err(), "the fixture must leave this caller unanswered");
    assert!(
        !pool.is_healthy(&addr),
        "the timeout must evict the pooled client, or this test is not \
         reproducing the race it exists for"
    );

    // Worker A's reply must still arrive.
    let frame = compio::time::timeout(Duration::from_secs(5), slow)
        .await
        .expect("worker A must not wait forever")
        .expect("slow request died with the eviction: an unrelated caller's \
                 timeout tore down a connection with a live request on it");
    assert_eq!(&frame.payload[..], b"ok");

    assert_eq!(
        server.join().expect("server thread"),
        1,
        "the server must have completed the slow reply"
    );
}
