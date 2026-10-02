//! Bootstrap helpers for mock business-protocol peers. These peers are not
//! production admission tests; version_hello and peer_auth tests exercise the
//! real parsers.
#![allow(dead_code)]
use std::io::{Read, Write};

/// Byte 6 of the Hello control (14 + 6 in the packet) is the declared role.
const ROLE_AT: usize = 20;
const ROLE_CLIENT: u8 = 1;

/// The PEER_AUTH challenge of a server without a cluster secret: the exchange
/// ends there for a dialer that holds none.
fn open_peer_auth_challenge() -> Vec<u8> {
    let mut ctrl = b"AUPA".to_vec();
    ctrl.push(0);
    ctrl.extend_from_slice(&[0; 32]);
    let mut out = 1u32.to_le_bytes().to_vec();
    out.extend_from_slice(&[autumn_rpc::peer_auth::MSG_PEER_AUTH, 1]);
    out.extend_from_slice(&((ctrl.len() + 8) as u32).to_le_bytes());
    out.extend_from_slice(&(ctrl.len() as u32).to_le_bytes());
    out.extend_from_slice(&ctrl);
    out.extend_from_slice(&crc32c::crc32c(&out).to_le_bytes());
    out
}

fn reply(wire: u32, service: u8, verdict: u8, message: &str) -> Vec<u8> {
    let mut ctrl = b"AUPH".to_vec();
    ctrl.extend_from_slice(&1u16.to_le_bytes());
    ctrl.push(verdict);
    ctrl.push(service);
    ctrl.extend_from_slice(&wire.to_le_bytes());
    ctrl.extend_from_slice(&43u32.to_le_bytes());
    ctrl.extend_from_slice(&wire.to_le_bytes());
    ctrl.extend_from_slice(&(message.len() as u16).to_le_bytes());
    ctrl.extend_from_slice(message.as_bytes());
    let mut out = 1u32.to_le_bytes().to_vec();
    out.extend_from_slice(&[0xF0, 1]);
    out.extend_from_slice(&((ctrl.len() + 8) as u32).to_le_bytes());
    out.extend_from_slice(&(ctrl.len() as u32).to_le_bytes());
    out.extend_from_slice(&ctrl);
    out.extend_from_slice(&crc32c::crc32c(&out).to_le_bytes());
    out
}
pub fn accept_std(socket: &mut std::net::TcpStream, wire: u32, service: u8) {
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let mut req = [0; 34];
    socket.read_exact(&mut req).unwrap();
    assert_eq!(&req[14..18], b"AUPH");
    assert_eq!(req[4], 0xF0);
    socket.write_all(&reply(wire, service, 0, "")).unwrap();
    // Blocking mock: answers PEER_AUTH as a server without a secret, so it only
    // serves a test process that has none installed either.
    if req[ROLE_AT] != ROLE_CLIENT {
        socket.write_all(&open_peer_auth_challenge()).unwrap();
    }
}
pub async fn accept_tcp(
    socket: &mut compio::net::TcpStream,
    wire: u32,
    service: u8,
    verdict: u8,
    message: &str,
) {
    use compio::io::{AsyncReadExt, AsyncWriteExt};
    let compio::BufResult(result, req) = socket.read_exact(vec![0; 34]).await;
    result.unwrap();
    assert_eq!(&req[14..18], b"AUPH");
    assert_eq!(req[4], 0xF0);
    let compio::BufResult(result, _) = socket
        .write_all(reply(wire, service, verdict, message))
        .await;
    result.unwrap();
    if verdict != 0 || req[ROLE_AT] == ROLE_CLIENT {
        return;
    }
    use autumn_rpc::version_hello::{Negotiated, Role, Service};
    let negotiated = Negotiated {
        role: if req[ROLE_AT] == 2 { Role::Peer } else { Role::Admin },
        service: match service {
            1 => Service::Manager,
            2 => Service::PartitionServer,
            _ => Service::ExtentNode,
        },
        remote_wire: wire,
        min_client: autumn_rpc::MIN_CLIENT_WIRE_VERSION,
        max_client: wire,
        declared_version: wire,
    };
    let (mut rd, mut wr) = autumn_transport::Conn::Tcp(socket.clone()).into_split();
    autumn_rpc::peer_auth::accept(
        &mut rd,
        &mut wr,
        &negotiated,
        autumn_rpc::peer_auth::installed(),
        "mock",
    )
    .await
    .unwrap();
}
pub async fn initiate_tcp(
    socket: compio::net::TcpStream,
    service: autumn_rpc::version_hello::Service,
) -> compio::net::TcpStream {
    let (mut rd, mut wr) = autumn_transport::Conn::Tcp(socket.clone()).into_split();
    let negotiated = autumn_rpc::version_hello::initiate(
        &mut rd,
        &mut wr,
        autumn_rpc::version_hello::Hello::current(autumn_rpc::version_hello::Role::Peer),
        Some(service),
    )
    .await
    .unwrap();
    autumn_rpc::peer_auth::initiate(
        &mut rd,
        &mut wr,
        &negotiated,
        autumn_rpc::peer_auth::installed(),
    )
    .await
    .unwrap();
    socket
}
/// The raw request packet `initiate` would write for `hello`, so a test can
/// pipeline business frames behind it in one write.
pub fn hello_packet(hello: autumn_rpc::version_hello::Hello) -> Vec<u8> {
    let ctrl = hello.encode();
    let mut out = 1u32.to_le_bytes().to_vec();
    out.extend_from_slice(&[0xF0, 0]);
    out.extend_from_slice(&((ctrl.len() + 8) as u32).to_le_bytes());
    out.extend_from_slice(&(ctrl.len() as u32).to_le_bytes());
    out.extend_from_slice(&ctrl);
    out.extend_from_slice(&crc32c::crc32c(&out).to_le_bytes());
    out
}
