//! Full-CRC prepared sends reach durable EN data.
//! AUTUMN_TEST_UCX_BIND=<RoCE-IP>:0 selects UCX; absent uses TCP loopback.
use autumn_rpc::{client::RpcClient, extent_rpc::*, frame::PreparedPayload};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use bytes::Bytes;
use std::time::Duration;

#[compio::test]
async fn prepared_large_appends_preserve_content_and_offsets() {
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
        let payload = PreparedPayload::new(vec![
            AppendReq::encode_header(701, 1, (i * size) as u64, 1),
            value,
        ]);
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
