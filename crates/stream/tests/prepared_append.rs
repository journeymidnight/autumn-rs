//! Full-CRC prepared sends reach durable EN data.
//! AUTUMN_TEST_UCX_BIND=<RoCE-IP>:0 selects UCX; absent uses TCP loopback.
use autumn_rpc::{client::RpcClient, extent_rpc::*, frame::PreparedPayload};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use bytes::Bytes;
use std::time::Duration;

/// Both tests share the process, so either may be the one that arms zerocopy.
fn arm_zerocopy_if_requested() {
    if std::env::var_os("AUTUMN_TEST_ZEROCOPY").is_some() {
        autumn_rpc::client::set_prepared_zerocopy_min_bytes(1);
    }
}

#[compio::test]
async fn prepared_large_appends_preserve_content_and_offsets() {
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
            rkyv_encode(&AllocExtentReq { extent_id: 701 }),
        )
        .await
        .unwrap();
    assert_eq!(
        rkyv_decode::<AllocExtentResp>(&allocated).unwrap().code,
        CODE_OK
    );
    let size = 2 * 1024 * 1024;
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
        let payload = PreparedPayload::new(parts, 3);
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
    // ACK has passed sync_data. Read the data file independently to ensure the
    // sender/receiver did not merely agree on an incorrect in-memory view.
    let shard = crc32c::crc32c(&701u64.to_le_bytes()) & 0xff;
    let disk = std::fs::read(dir.path().join(format!("{shard:02x}/extent-701.dat"))).unwrap();
    assert_eq!(disk.as_slice(), reply.buf.as_ref());
    drop(server);
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
        let payload = PreparedPayload::new(
            vec![AppendReq::encode_header(702, 1, offset, 1), value],
            3,
        );
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
