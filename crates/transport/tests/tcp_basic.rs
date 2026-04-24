use autumn_transport::{AutumnTransport, TcpTransport, TransportKind};
use compio::io::{AsyncRead, AsyncWrite};
use compio::BufResult;
use std::net::SocketAddr;

#[compio::test]
async fn tcp_connect_bind_round_trip() {
    let t = TcpTransport;
    assert_eq!(t.kind(), TransportKind::Tcp);

    let mut listener = t
        .bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let server = compio::runtime::spawn(async move {
        let (conn, _peer) = listener.accept().await.expect("accept");
        let (mut r, mut w) = conn.into_split();
        let buf = vec![0u8; 5];
        let BufResult(n, b) = r.read(buf).await;
        assert_eq!(n.unwrap(), 5);
        assert_eq!(&b[..], b"hello");
        let BufResult(n, _) = w.write(b"world".to_vec()).await;
        assert_eq!(n.unwrap(), 5);
    });

    let client = t.connect(addr).await.expect("connect");
    let (mut r, mut w) = client.into_split();
    let BufResult(n, _) = w.write(b"hello".to_vec()).await;
    assert_eq!(n.unwrap(), 5);
    let buf = vec![0u8; 5];
    let BufResult(n, b) = r.read(buf).await;
    assert_eq!(n.unwrap(), 5);
    assert_eq!(&b[..], b"world");

    server.await;
}
