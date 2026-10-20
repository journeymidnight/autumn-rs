//! A client connection that may only send small control messages — to the
//! manager, to an extent node — is dropped on a frame header announcing more,
//! instead of the server reserving the announced body and waiting for it. A
//! peer connection is not limited.
mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_rpc::version_hello::{self, Hello, Role, Service};
use autumn_transport::{Conn, ReadHalf, WriteHalf};
use compio::io::{AsyncRead, AsyncWriteExt};
use support::{pick_addr, start_extent_node, start_manager};

async fn open(addr: SocketAddr, service: Service, role: Role) -> (ReadHalf, WriteHalf) {
    let socket = compio::net::TcpStream::connect(addr).await.unwrap();
    let (mut rd, mut wr) = Conn::Tcp(socket).into_split();
    let negotiated = version_hello::initiate(&mut rd, &mut wr, Hello::current(role), Some(service))
        .await
        .unwrap();
    autumn_rpc::peer_auth::initiate(&mut rd, &mut wr, &negotiated, autumn_rpc::peer_auth::installed())
        .await
        .unwrap();
    (rd, wr)
}

/// Sends only a header announcing a 64 MiB request; whether the server
/// closes the connection within a few seconds.
async fn dropped_on_a_large_header(addr: SocketAddr, service: Service, role: Role) -> bool {
    let (mut rd, mut wr) = open(addr, service, role).await;
    let mut head = vec![0u8; autumn_rpc::frame::HEADER_LEN];
    head[0] = 1;
    head[4] = autumn_rpc::MSG_TYPE_PING;
    head[6..10].copy_from_slice(&(64u32 << 20).to_le_bytes());
    wr.write_all(head).await.0.unwrap();
    match compio::time::timeout(Duration::from_secs(3), rd.read(vec![0u8; 64])).await {
        Ok(compio::BufResult(Ok(0), _)) | Ok(compio::BufResult(Err(_), _)) => true,
        Ok(compio::BufResult(Ok(n), _)) => panic!("unexpected {n}-byte answer"),
        Err(_) => false,
    }
}

#[test]
fn a_client_header_announcing_a_large_body_is_refused() {
    let mgr = pick_addr();
    start_manager(mgr);
    let en = pick_addr();
    let dir = tempfile::tempdir().unwrap();
    start_extent_node(en, dir.path().to_path_buf(), 1);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        assert!(dropped_on_a_large_header(mgr, Service::Manager, Role::Client).await, "manager client");
        assert!(!dropped_on_a_large_header(mgr, Service::Manager, Role::Peer).await, "manager peer");
        assert!(dropped_on_a_large_header(en, Service::ExtentNode, Role::Client).await, "extent node client");
        assert!(!dropped_on_a_large_header(en, Service::ExtentNode, Role::Peer).await, "extent node peer");
    });
}
