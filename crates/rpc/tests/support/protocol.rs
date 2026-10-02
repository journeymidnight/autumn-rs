//! Bootstrap helpers for mock business-protocol peers. These peers are not
//! production admission tests; version_hello tests exercise the real parser.
#![allow(dead_code)]
use std::io::{Read, Write};

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
}
pub async fn initiate_tcp(
    socket: compio::net::TcpStream,
    service: autumn_rpc::version_hello::Service,
) -> compio::net::TcpStream {
    let (mut rd, mut wr) = autumn_transport::Conn::Tcp(socket.clone()).into_split();
    autumn_rpc::version_hello::initiate(
        &mut rd,
        &mut wr,
        autumn_rpc::version_hello::Hello::current(autumn_rpc::version_hello::Role::Peer),
        Some(service),
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
