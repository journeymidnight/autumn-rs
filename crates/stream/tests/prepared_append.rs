//! Full-CRC prepared sends reach durable EN data.
//! AUTUMN_TEST_UCX_BIND=<RoCE-IP>:0 selects UCX; absent uses TCP loopback.
use autumn_rpc::{client::RpcClient, extent_rpc::*, frame::PreparedPayload};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use bytes::Bytes;
use std::time::Duration;

fn ucx() -> bool {
    std::env::var_os("AUTUMN_TEST_UCX_BIND").is_some()
}

/// Rendezvous receives on this thread.
fn rendezvous_count() -> u64 {
    autumn_transport::am::received_counts().1
}

/// Both tests share the process, so either may be the one that arms zerocopy.
fn arm_zerocopy_if_requested() {
    if std::env::var_os("AUTUMN_TEST_ZEROCOPY").is_some() {
        autumn_rpc::client::set_prepared_zerocopy_min_bytes(1);
    }
}

/// An extent node serving on its own thread (its own UCX worker, so the
/// connection takes the NIC as between processes rather than UCX's `self`
/// transport, which registers no memory). Stop it with the sender; it answers
/// with its thread's rendezvous receives.
fn node_on_its_own_thread(
    bind: std::net::SocketAddr,
    kind: autumn_transport::TransportKind,
    dir: std::path::PathBuf,
) -> (std::net::SocketAddr, std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<u64>) {
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
            // Also stops when the test went away without saying so.
            while matches!(stop_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)) {
                compio::time::sleep(Duration::from_millis(20)).await;
            }
            count_tx.send(rendezvous_count()).unwrap();
        })
    });
    (addr_rx.recv().unwrap(), stop_tx, count_rx)
}

#[compio::test]
async fn prepared_large_appends_preserve_content_and_offsets() {
    arm_zerocopy_if_requested();
    let (kind, bind) = match std::env::var("AUTUMN_TEST_UCX_BIND") {
        Ok(bind) => (autumn_transport::TransportKind::Ucx, bind),
        Err(_) => (
            autumn_transport::TransportKind::Tcp,
            "127.0.0.1:0".to_string(),
        ),
    };
    autumn_transport::init_with(kind);
    let dir = tempfile::tempdir().unwrap();
    let (addr, stop_node, node_rendezvous) = node_on_its_own_thread(bind.parse().unwrap(), kind, dir.path().to_path_buf());
    let client = RpcClient::connect(addr).await.unwrap();
    let allocated = client
        .call(
            MSG_ALLOC_EXTENT,
            rkyv_encode(&AllocExtentReq { extent_id: 701 }),
        )
        .await
        .unwrap();
    assert_eq!(
        rkyv_decode::<AllocExtentResp>(&allocated).unwrap().code,
        CODE_OK
    );
    let size = 2 * 1024 * 1024;
    let rndv_before = rendezvous_count();
    let mut replies = Vec::new();
    for i in 0..4 {
        let value = Bytes::from(vec![i as u8 + 1; size]);
        let mut parts = vec![AppendReq::encode_header(701, 1, (i * size) as u64, 1)];
        if i == 2 {
            // Cross the RPC writer's IOV_MAX boundary inside one CRC-protected
            // frame. Slices keep the same owned payload through each send.
            parts.extend(
                (0..size)
                    .step_by(1024)
                    .map(|start| value.slice(start..start + 1024)),
            );
        } else {
            parts.push(value);
        }
        // RF=3 at 2 MiB: the arm that shares one scan across the replicas, so
        // a real EN is what accepts the combined checksum.
        let head = parts.remove(0);
        let payload = PreparedPayload::new(head, parts, 3);
        replies.push(client.send_prepared(MSG_APPEND, &payload).await.unwrap());
    }
    for (i, reply) in replies.into_iter().enumerate() {
        let frame = compio::time::timeout(Duration::from_secs(10), reply)
            .await
            .unwrap()
            .unwrap();
        assert!(!frame.is_error());
        let appended = AppendResp::decode(frame.payload).unwrap();
        assert_eq!(appended.code, CODE_OK);
        assert_eq!(appended.offset, (i * size) as u64);
        assert_eq!(appended.end, ((i + 1) * size) as u64);
    }
    let req = ReadBytesReq::new(701, 1, 0, (4 * size) as u64, PayloadRef::in_dat());
    let reply = client
        .call_into_pooled(MSG_READ_BYTES_BULK, req.encode())
        .await
        .unwrap();
    assert_eq!(reply.code, CODE_OK);
    for (i, part) in reply.buf.as_ref().chunks(size).enumerate() {
        assert!(part.iter().all(|&b| b == i as u8 + 1));
    }
    assert_eq!(reply.buf.len(), 4 * size);
    if ucx() {
        // The read's value came as a frame value by rendezvous.
        assert_eq!(rendezvous_count() - rndv_before, 1);
    }
    // Many bulk reads at once: the node flushes their replies together, which
    // over UCX makes a stream send large enough to go zero-copy (every iovec
    // registered). A reply frame built from read-only memory then stopped the
    // connection's stream dead under UCX's default protocols.
    let reads = (0..64u64).map(|i| {
        let req = ReadBytesReq::new(701, 1, i * 65536, 65536, PayloadRef::in_dat());
        let client = client.clone();
        async move { client.call_into_pooled(MSG_READ_BYTES_BULK, req.encode()).await }
    });
    let replies = compio::time::timeout(Duration::from_secs(10), futures::future::join_all(reads))
        .await
        .expect("64 concurrent bulk reads did not all complete");
    for (i, r) in replies.into_iter().enumerate() {
        let r = r.unwrap();
        assert_eq!(r.code, CODE_OK);
        assert_eq!(r.buf.as_ref(), &reply.buf.as_ref()[i * 65536..(i + 1) * 65536]);
    }
    // ACK has passed sync_data. Read the data file independently to ensure the
    // sender/receiver did not merely agree on an incorrect in-memory view.
    let shard = crc32c::crc32c(&701u64.to_le_bytes()) & 0xff;
    let disk = std::fs::read(dir.path().join(format!("{shard:02x}/extent-701.dat"))).unwrap();
    assert_eq!(disk.as_slice(), reply.buf.as_ref());
    stop_node.send(()).unwrap();
    let node_rendezvous = node_rendezvous.recv().unwrap();
    if ucx() {
        // The single-part appends came to the node as frame values; the
        // 1 KiB-part append stayed on the stream.
        assert_eq!(node_rendezvous, 3);
    }
}

/// WAL-sized appends take the same prepared path as bulk ones: `launch_append`
/// prepares every star-replicated append, so a 4 KiB record reaches the EN
/// through `send_prepared` too — on the re-scanning arm, which the sibling test
/// above does not reach. Under AUTUMN_TEST_ZEROCOPY=1 this is also the only
/// coverage of a frame far below any deployment's zerocopy threshold going out
/// through the zerocopy writer.
#[compio::test]
async fn prepared_wal_sized_appends_are_durable() {
    arm_zerocopy_if_requested();
    let (transport, bind) = match std::env::var("AUTUMN_TEST_UCX_BIND") {
        Ok(bind) => (autumn_transport::TransportKind::Ucx, bind),
        Err(_) => (
            autumn_transport::TransportKind::Tcp,
            "127.0.0.1:0".to_string(),
        ),
    };
    let transport = autumn_transport::init_with(transport);
    let mut listener = transport.bind(bind.parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
        .await
        .unwrap();
    let server = compio::runtime::spawn(async move {
        let (conn, _) = listener.accept().await.unwrap();
        ExtentNode::handle_connection(conn, node).await
    });
    let client = RpcClient::connect(addr).await.unwrap();
    let allocated = client
        .call(
            MSG_ALLOC_EXTENT,
            rkyv_encode(&AllocExtentReq { extent_id: 702 }),
        )
        .await
        .unwrap();
    assert_eq!(
        rkyv_decode::<AllocExtentResp>(&allocated).unwrap().code,
        CODE_OK
    );
    // 0 bytes is the degenerate end of the range the size gate used to exclude.
    let sizes = [4096usize, 1, 0, 4096];
    let mut replies = Vec::new();
    let mut offset = 0u64;
    let mut want = Vec::new();
    for (i, size) in sizes.iter().enumerate() {
        let value = Bytes::from(vec![i as u8 + 1; *size]);
        want.extend_from_slice(&value);
        // Same RF=3, but far below the size that makes sharing a scan pay:
        // the re-scanning arm, against the same real EN.
        let payload = PreparedPayload::new(AppendReq::encode_header(702, 1, offset, 1), vec![value], 3);
        replies.push((offset, *size));
        offset += *size as u64;
        let reply = client.send_prepared(MSG_APPEND, &payload).await.unwrap();
        let frame = compio::time::timeout(Duration::from_secs(10), reply)
            .await
            .unwrap()
            .unwrap();
        assert!(!frame.is_error(), "append {i} of {size} B returned an error");
        let appended = AppendResp::decode(frame.payload).unwrap();
        assert_eq!(appended.code, CODE_OK);
        let (want_offset, want_size) = replies[i];
        assert_eq!(appended.offset, want_offset);
        assert_eq!(appended.end, want_offset + want_size as u64);
    }
    // ACK has passed sync_data; read the data file, not the EN's memory.
    let shard = crc32c::crc32c(&702u64.to_le_bytes()) & 0xff;
    let disk = std::fs::read(dir.path().join(format!("{shard:02x}/extent-702.dat"))).unwrap();
    assert_eq!(disk, want);
    drop(server);
}

/// An append whose payload comes as a frame value carries the payload's
/// crc32c, and the extent node checks it: a payload that does not match is
/// refused with an error frame and nothing is appended. UCX only (frame values
/// need it); under TCP this test has nothing to send.
#[compio::test]
async fn a_frame_value_append_whose_payload_misses_its_crc_is_refused() {
    use autumn_rpc::version_hello::{self, Hello, Role};
    use compio::io::{AsyncRead, AsyncWriteExt};
    let Ok(bind) = std::env::var("AUTUMN_TEST_UCX_BIND") else {
        eprintln!("frame values need UCX: set AUTUMN_TEST_UCX_BIND");
        return;
    };
    let transport = autumn_transport::init_with(autumn_transport::TransportKind::Ucx);
    let mut listener = transport.bind(bind.parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1)).await.unwrap();
    let server = compio::runtime::spawn(async move {
        loop {
            let (conn, _) = listener.accept().await.unwrap();
            let node = node.clone();
            compio::runtime::spawn(async move { ExtentNode::handle_connection(conn, node).await }).detach();
        }
    });
    let client = RpcClient::connect(addr).await.unwrap();
    let allocated = client.call(MSG_ALLOC_EXTENT, rkyv_encode(&AllocExtentReq { extent_id: 703 })).await.unwrap();
    assert_eq!(rkyv_decode::<AllocExtentResp>(&allocated).unwrap().code, CODE_OK);

    let conn = transport.connect(addr).await.unwrap();
    let (mut r, mut w) = conn.into_split();
    let n = version_hello::initiate(&mut r, &mut w, Hello::current(Role::Peer), None).await.unwrap();
    autumn_rpc::peer_auth::initiate(&mut r, &mut w, &n, autumn_rpc::peer_auth::installed()).await.unwrap();
    let value = Bytes::from(vec![0x5au8; 1 << 20]);
    let wrong = crc32c::crc32c(&value) ^ 1;
    let ctrl = vec![AppendReq::encode_header(703, 1, 0, 1), Bytes::copy_from_slice(&wrong.to_le_bytes())];
    let (bufs, am) = autumn_rpc::frame::encode_am_frame(1, MSG_APPEND, 0, ctrl, vec![value.clone()], || 77);
    let sender = w.am_sender().unwrap();
    for (tag, v) in am {
        sender.post_frame_value(tag, v);
    }
    w.write_vectored_all(bufs).await.0.unwrap();
    let mut decoder = autumn_rpc::FrameDecoder::new();
    let reply = loop {
        if let Some(f) = decoder.try_decode().unwrap() {
            break f;
        }
        let compio::BufResult(got, buf) = r.read(Vec::with_capacity(4096)).await;
        assert!(got.unwrap() > 0, "connection closed before a reply");
        decoder.feed(&buf);
    };
    assert!(reply.is_error(), "a payload that misses its crc was accepted");
    assert!(String::from_utf8_lossy(&reply.payload).contains("crc"), "{:?}", reply.payload);

    // Nothing was appended: the good payload goes in at offset 0.
    let payload = PreparedPayload::new(AppendReq::encode_header(703, 1, 0, 1), vec![value], 1);
    let frame = client.send_prepared(MSG_APPEND, &payload).await.unwrap().await.unwrap();
    assert!(!frame.is_error());
    let appended = AppendResp::decode(frame.payload).unwrap();
    assert_eq!((appended.code, appended.offset), (CODE_OK, 0));
    drop(server);
}
