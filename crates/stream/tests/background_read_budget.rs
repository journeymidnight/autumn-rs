//! A background read (`ReadReq::with_crcs`) is received whatever the reader's
//! `Reply` budget holds: it is what the PS's copy path sends when a pooled
//! read was refused for want of a reply buffer. Over UCX its bytes must ride
//! the stream, not come as a frame value, which the reader's loop takes
//! against that budget. A 64 MiB cap gives `Reply` about 25.6 MiB, half of it
//! per thread; this test holds the reading thread's half.
//! AUTUMN_TEST_UCX_BIND=<RoCE-IP>:0 selects UCX; absent uses TCP loopback.
use autumn_rpc::frame::PreparedPayload;
use autumn_stream::extent_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig};
use autumn_transport::PoolUse;
use bytes::Bytes;
use std::time::Duration;

const MIB: usize = 1 << 20;

fn hold_share() -> Vec<autumn_transport::PooledBuf> {
    let mut held = Vec::new();
    for size in [MIB, 64 << 10, 4 << 10] {
        while let Ok(pb) = autumn_transport::regpool_try_acquire_for(PoolUse::Reply, size) {
            held.push(pb);
        }
    }
    held
}

/// An extent node on its own thread and UCX worker, so the connection crosses
/// the NIC rather than UCX's `self` transport.
fn node_on_its_own_thread(
    bind: std::net::SocketAddr,
    kind: autumn_transport::TransportKind,
    dir: std::path::PathBuf,
) -> (std::net::SocketAddr, std::sync::mpsc::Sender<()>) {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let transport = autumn_transport::init_with(kind);
            let mut listener = transport.bind(bind).await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let node = ExtentNode::new(ExtentNodeConfig::new(dir, 1)).await.unwrap();
            compio::runtime::spawn(async move {
                loop {
                    let (conn, _) = listener.accept().await.unwrap();
                    let node = node.clone();
                    compio::runtime::spawn(async move { ExtentNode::handle_connection(conn, node).await }).detach();
                }
            })
            .detach();
            while matches!(stop_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)) {
                compio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    });
    (addr_rx.recv().unwrap(), stop_tx)
}

#[compio::test]
async fn a_background_read_is_received_with_the_reply_budget_full() {
    assert!(autumn_transport::set_regpool_cap_bytes(64 * MIB), "this process sets the cap first");
    let (kind, bind) = match std::env::var("AUTUMN_TEST_UCX_BIND") {
        Ok(bind) => (autumn_transport::TransportKind::Ucx, bind),
        Err(_) => (autumn_transport::TransportKind::Tcp, "127.0.0.1:0".to_string()),
    };
    autumn_transport::init_with(kind);
    let dir = tempfile::tempdir().unwrap();
    let (addr, stop) = node_on_its_own_thread(bind.parse().unwrap(), kind, dir.path().to_path_buf());
    let addr = addr.to_string();
    let pool = ConnPool::new();
    let r = pool.call(&addr, MSG_ALLOC_EXTENT, rkyv_encode(&AllocExtentReq { extent_id: 801 })).await.unwrap();
    assert_eq!(rkyv_decode::<AllocExtentResp>(&r).unwrap().code, CODE_OK);
    let value: Vec<u8> = (0..4 * MIB).map(|i| (i as u8).wrapping_mul(13)).collect();
    let header = AppendHeader { extent_id: 801, eversion: 1, commit: 0, owner_epoch: 1, payload_crc: 0 };
    let payload: PreparedPayload = header.prepared(vec![Bytes::from(value.clone())], None);
    let appended = pool.call_prepared(&addr, MSG_APPEND, &payload).await.unwrap();
    assert_eq!(AppendResp::decode(appended).unwrap().code, CODE_OK);

    let held = hold_share();
    assert!(!held.is_empty());
    // Control: a foreground pooled read of the same bytes is refused here.
    let fg = rkyv_encode(&ReadReq::new(801, 1, 0, value.len() as u64, READ_EXACT, PayloadRef::in_dat()));
    let refused = pool.call_into_pooled(&addr, MSG_READ, fg, Duration::from_secs(10)).await;
    assert!(refused.is_err(), "the reading thread's Reply share is full");
    // The background read is received all the same, its crcs intact.
    let bg = rkyv_encode(&ReadReq::new(801, 1, 0, value.len() as u64, READ_UP_TO, PayloadRef::in_dat()).with_crcs());
    let frame = pool
        .call_frame_timeout(&addr, MSG_READ, bg, Duration::from_secs(10))
        .await
        .expect("a background read with the Reply budget full");
    assert_eq!(frame.payload[0], CODE_OK);
    let resp: ReadResp = rkyv_decode(&frame.payload[1..]).unwrap();
    check_read_block_crcs(0, frame.value.bytes(), &resp.block_crcs).unwrap();
    assert_eq!(frame.value.bytes().as_ref(), value.as_slice());
    drop(held);
    stop.send(()).unwrap();
}
