//! `MSG_READ_AM`: an extent read whose value the node sends as the reader's
//! tagged active message, straight into the reader's registered memory.
//! AUTUMN_TEST_UCX_BIND=<RoCE-IP>:0 selects UCX; over TCP a read cannot land.
use autumn_rpc::{client::RpcClient, extent_rpc::*};
use autumn_stream::{read_extent_value_landed, ConnPool, ExtentNode, ExtentNodeConfig, Landed};
use bytes::Bytes;
use std::time::Duration;

fn ucx() -> bool {
    std::env::var_os("AUTUMN_TEST_UCX_BIND").is_some()
}

/// An extent node on its own thread (its own UCX worker, so the connection
/// takes the NIC as between processes). Stop it with the sender; it answers
/// with the AM values its thread sent (with their slab's memh, without).
fn node_on_its_own_thread(
    bind: std::net::SocketAddr,
    kind: autumn_transport::TransportKind,
    dir: std::path::PathBuf,
) -> (std::net::SocketAddr, std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<(u64, u64)>) {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let (count_tx, count_rx) = std::sync::mpsc::channel();
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
            count_tx.send(autumn_transport::am::sent_counts()).unwrap();
        })
    });
    (addr_rx.recv().unwrap(), stop_tx, count_rx)
}

#[compio::test]
async fn a_landed_read_arrives_in_the_registered_memory() {
    let (kind, bind) = match std::env::var("AUTUMN_TEST_UCX_BIND") {
        Ok(bind) => (autumn_transport::TransportKind::Ucx, bind),
        Err(_) => (autumn_transport::TransportKind::Tcp, "127.0.0.1:0".to_string()),
    };
    autumn_transport::init_with(kind);
    let dir = tempfile::tempdir().unwrap();
    let (addr, stop_node, node_sent) = node_on_its_own_thread(bind.parse().unwrap(), kind, dir.path().to_path_buf());
    let client = RpcClient::connect(addr).await.unwrap();
    let allocated = client.call(MSG_ALLOC_EXTENT, rkyv_encode(&AllocExtentReq { extent_id: 801 })).await.unwrap();
    assert_eq!(rkyv_decode::<AllocExtentResp>(&allocated).unwrap().code, CODE_OK);
    let size = 4 << 20;
    let content: Vec<u8> = (0..2 * size).map(|i| (i * 31 % 251) as u8).collect();
    let header = AppendHeader { extent_id: 801, eversion: 1, commit: 0, owner_epoch: 1, payload_crc: 0 };
    let payload = header.prepared(vec![Bytes::from(content.clone())], None);
    let reply = client.send_prepared(MSG_APPEND, &payload).await.unwrap();
    let frame = compio::time::timeout(Duration::from_secs(10), reply).await.unwrap().unwrap();
    assert_eq!(AppendResp::decode(frame.payload).unwrap().code, CODE_OK);

    // A direct-reading client's connection, as the SDK opens it.
    let pool = ConnPool::with_role(autumn_rpc::version_hello::Role::Client);
    let addr = addr.to_string();
    let mut mem = vec![0u8; size];
    // SAFETY: `mem` outlives the region, which is dropped first below.
    let region = unsafe { autumn_transport::Region::register(mem.as_mut_ptr(), mem.len()) }.unwrap();
    let (in_place, landed) = (autumn_transport::am::in_place_bytes(), autumn_transport::am::landed_bytes());

    // SAFETY: `mem` is ours for the call, which runs to completion.
    let got = unsafe { read_extent_value_landed(&pool, &addr, 801, 1, size as u64, size as u64, mem.as_mut_ptr(), Some(&region)) }
        .await
        .unwrap();
    if !ucx() {
        assert!(matches!(got, Landed::NotHere), "over TCP a read cannot land");
        stop_node.send(()).unwrap();
        return;
    }
    assert!(matches!(got, Landed::In(n) if n == size));
    assert!(mem == content[size..], "the second half of the extent, in place");
    assert_eq!(autumn_transport::am::in_place_bytes() - in_place, size as u64, "received straight into the memory");
    assert_eq!(autumn_transport::am::landed_bytes(), landed, "no copy");

    // Memory outside any region: the value comes into a pool buffer, copied in.
    let mut plain = vec![0u8; size];
    // SAFETY: as above.
    let got = unsafe { read_extent_value_landed(&pool, &addr, 801, 1, 0, size as u64, plain.as_mut_ptr(), Some(&region)) }
        .await
        .unwrap();
    assert!(matches!(got, Landed::In(n) if n == size));
    assert!(plain == content[..size]);
    assert_eq!(autumn_transport::am::landed_bytes() - landed, size as u64, "copied in from a pool buffer");

    // A read past what the node holds is refused and writes nothing.
    mem.fill(0xAB);
    // SAFETY: as above.
    let err = unsafe { read_extent_value_landed(&pool, &addr, 801, 1, size as u64 + 1, size as u64, mem.as_mut_ptr(), Some(&region)) }
        .await
        .err()
        .expect("past the end");
    assert!(format!("{err:#}").contains("direct read"), "{err:#}");
    assert!(mem.iter().all(|&b| b == 0xAB));

    drop(region);
    stop_node.send(()).unwrap();
    // Both values left from the node's pool slabs, with their memh.
    assert_eq!(node_sent.recv().unwrap(), (2, 0));
}
