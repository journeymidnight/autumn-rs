//! How a read loop takes a frame value against the pool's budgets: a value
//! larger than a thread may hold (a compaction output appended whole) is
//! received unbudgeted; with the budget full a loop either refuses the frame,
//! letting its value go, or takes an unbudgeted buffer. A 64 MiB cap gives a
//! thread about 12.8 MiB of `Receive`. Binds `AUTUMN_UCX_TEST_BIND`, like the
//! transport's UCX tests.

#![cfg(feature = "ucx")]

use autumn_rpc::am_frame::{self, IncomingError, WhenFull};
use autumn_rpc::frame::{encode_am_frame, Frame, FrameDecoder};
use autumn_transport::{am, AutumnTransport, PoolUse, ReadHalf, TransportKind, UcxTransport};
use bytes::Bytes;
use compio::io::AsyncWriteExt;
use std::net::SocketAddr;
use std::time::Duration;

const MIB: usize = 1 << 20;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(13).wrapping_add(seed)).collect()
}

/// Send `value` as the value of frame `req_id` (tag `tag`) and decode the frame.
async fn send(sender: &am::Sender, writer: &mut autumn_transport::WriteHalf, req_id: u32, tag: u64, value: &[u8]) -> Frame {
    let (bufs, values) = encode_am_frame(req_id, 0x51, 0, vec![Bytes::from_static(b"k")], vec![Bytes::from(value.to_vec())], || tag);
    for (tag, v) in values {
        sender.post_frame_value(tag, v);
    }
    let wire: Vec<u8> = bufs.iter().flat_map(|b| b.iter().copied()).collect();
    writer.write_all(wire.clone()).await.0.unwrap();
    let mut decoder = FrameDecoder::new();
    decoder.feed(&wire);
    decoder.try_decode().unwrap().expect("one frame")
}

async fn take(reader: &ReadHalf, frame: Frame, when_full: WhenFull) -> Result<Frame, IncomingError> {
    let incoming = am_frame::start(reader, frame, PoolUse::Receive, when_full).unwrap();
    compio::time::timeout(Duration::from_secs(10), incoming.into_frame(reader)).await.expect("taken")
}

#[compio::test]
async fn a_read_loop_takes_values_against_the_budget() {
    autumn_transport::init_with(TransportKind::Ucx);
    assert!(autumn_transport::set_regpool_cap_bytes(64 * MIB), "this process sets the cap first");
    let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap().0 });
    let client = UcxTransport.connect(bound).await.unwrap();
    let server = accept.await.unwrap();
    let (reader, _server_w) = server.into_split();
    let (_client_r, mut writer) = client.into_split();
    let sender = writer.am_sender().unwrap();

    // Too large for the budget: received, even by a loop that refuses.
    let big = pattern(16 * MIB, 1);
    let frame = send(&sender, &mut writer, 1, 11, &big).await;
    let got = take(&reader, frame, WhenFull::Refuse).await.unwrap_or_else(|e| panic!("refused: {e}"));
    assert_eq!(&got.value[..], &big[..]);
    assert!(got.value.is_registered(), "a value received into a pool buffer keeps its memh");

    // This thread's share held: a refusing loop answers Unavailable.
    let mut held = Vec::new();
    for size in [MIB, 64 << 10] {
        while let Ok(pb) = autumn_transport::regpool_try_acquire_for(PoolUse::Receive, size) {
            held.push(pb);
        }
    }
    let small = pattern(4 * MIB, 2);
    let frame = send(&sender, &mut writer, 2, 12, &small).await;
    match take(&reader, frame, WhenFull::Refuse).await {
        Err(e) => assert_eq!(e.refusal().map(|f| f.req_id), Some(2)),
        Ok(_) => panic!("a full budget must refuse"),
    }
    // A loop whose sender bounds its values takes it unbudgeted instead.
    let frame = send(&sender, &mut writer, 3, 13, &small).await;
    let got = take(&reader, frame, WhenFull::Unbudgeted).await.unwrap_or_else(|e| panic!("refused: {e}"));
    assert_eq!(&got.value[..], &small[..]);
    drop(held);
}
