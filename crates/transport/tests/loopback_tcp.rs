mod common;

use autumn_transport::TcpTransport;

fn loopback() -> std::net::SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

#[compio::test]
async fn ping_pong() {
    common::ping_pong_at(TcpTransport, loopback()).await;
}

#[compio::test]
async fn large_payload() {
    common::large_payload_at(TcpTransport, loopback()).await;
}

#[compio::test]
async fn half_close() {
    common::half_close_at(TcpTransport, loopback()).await;
}

#[compio::test]
async fn many_concurrent_1k() {
    common::many_concurrent_at(TcpTransport, loopback(), 1000).await;
}
