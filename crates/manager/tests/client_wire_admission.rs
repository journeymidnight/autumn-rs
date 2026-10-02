//! Admission through the real manager connection loop, before any business DTO.
mod support;
use autumn_rpc::version_hello::{self, Hello, Role, Service};
use autumn_rpc::manager_rpc::*;
use autumn_rpc::{Frame, FrameDecoder, RpcError, StatusCode, WIRE_VERSION, MIN_CLIENT_WIRE_VERSION};
use autumn_transport::{Conn, ReadHalf, WriteHalf};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use std::net::SocketAddr;
use support::{pick_stable_port_pair, start_manager};

async fn open(addr: SocketAddr, hello: Hello) -> Result<(ReadHalf, WriteHalf), RpcError> {
    let socket = compio::net::TcpStream::connect(addr).await?;
    let (mut rd, mut wr) = Conn::Tcp(socket).into_split();
    let negotiated = version_hello::initiate(&mut rd, &mut wr, hello, Some(Service::Manager)).await?;
    autumn_rpc::peer_auth::initiate(&mut rd, &mut wr, &negotiated, autumn_rpc::peer_auth::installed())
        .await?;
    Ok((rd, wr))
}
async fn receive(rd: &mut ReadHalf) -> Frame {
    let mut decoder = FrameDecoder::new();
    loop {
        let compio::BufResult(n, buf) = rd.read(vec![0; 4096]).await;
        let n = n.unwrap(); assert_ne!(n, 0);
        decoder.feed(&buf[..n]);
        if let Some(frame) = decoder.try_decode().unwrap() { return frame; }
    }
}
async fn call(rd: &mut ReadHalf, wr: &mut WriteHalf, op: u8, payload: Bytes) -> Frame {
    wr.write_all(Frame::request(2, op, payload).encode()).await.0.unwrap();
    receive(rd).await
}

#[test]
fn manager_checks_client_interval_and_exact_internal_wire() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_stable_port_pair()).parse().unwrap();
    start_manager(addr);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        for version in [MIN_CLIENT_WIRE_VERSION - 1, MIN_CLIENT_WIRE_VERSION,
            WIRE_VERSION - 1, WIRE_VERSION, WIRE_VERSION + 1] {
            let result = open(addr, Hello { role: Role::Client, wire_version: version, client_version: version }).await;
            if (MIN_CLIENT_WIRE_VERSION..=WIRE_VERSION).contains(&version) {
                let (mut rd, mut wr) = result.unwrap();
                let frame = call(&mut rd, &mut wr, MSG_GET_CLUSTER_ID, rkyv_encode(&GetClusterIdReq {})).await;
                assert!(!frame.is_error());
                let id: GetClusterIdResp = rkyv_decode(&frame.payload).unwrap();
                assert_eq!((id.wire_version_min, id.wire_version_max), (MIN_CLIENT_WIRE_VERSION, WIRE_VERSION));
                assert_eq!(id.cluster_version, 0, "reserved field has no latch semantics");
                let frame = call(&mut rd, &mut wr, MSG_REGISTER_PS, Bytes::from_static(b"invalid DTO")).await;
                assert!(frame.is_error());
                assert_eq!(RpcError::decode_status(&frame.payload).0, StatusCode::PermissionDenied);
            } else {
                assert!(matches!(result, Err(RpcError::VersionMismatch { .. })));
            }
            for role in [Role::Peer, Role::Admin] {
                let result = open(addr, Hello { role, wire_version: version, client_version: 0 }).await;
                if version == WIRE_VERSION { assert!(result.is_ok()); }
                else { assert!(matches!(result, Err(RpcError::VersionMismatch { .. }))); }
            }
        }
        for op in [MSG_REGISTER_PS, autumn_rpc::client_hello::MSG_CLIENT_HELLO] {
            let socket = compio::net::TcpStream::connect(addr).await.unwrap();
            let (mut rd, mut wr) = Conn::Tcp(socket).into_split();
            let frame = call(&mut rd, &mut wr, op, Bytes::from_static(b"not a bootstrap")).await;
            assert_eq!(frame.msg_type, version_hello::MSG_VERSION_HELLO);
            assert_eq!(frame.payload[6], version_hello::Verdict::Malformed as u8);
        }
        let (mut rd, mut wr) = open(addr, Hello::current(Role::Peer)).await.unwrap();
        let frame = call(&mut rd, &mut wr, MSG_CREATE_STREAM, Bytes::new()).await;
        assert_eq!(RpcError::decode_status(&frame.payload).0, StatusCode::PermissionDenied);
    });
}
