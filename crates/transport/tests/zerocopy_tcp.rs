use autumn_transport::{AutumnTransport, TcpTransport};
use bytes::Bytes;
use compio::io::{AsyncReadExt, AsyncWrite};
use std::time::Duration;

#[compio::test]
async fn segmented_large_sends_and_close_preserve_bytes() {
    let mut listener = TcpTransport
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let server = compio::runtime::spawn(async move {
        let (conn, _) = listener.accept().await.unwrap();
        let (mut reader, _) = conn.into_split();
        let (_, received) = reader.read_to_end(vec![]).await.unwrap();
        received
    });
    let conn = TcpTransport.connect(addr).await.unwrap();
    let (reader, mut writer) = conn.into_split();
    let mut expected = vec![];
    for size in [4096, 65536, 1048576, 8388608] {
        let parts: Vec<_> = (0..3).map(|i| Bytes::from(vec![i + 1; size / 3])).collect();
        for part in &parts {
            expected.extend_from_slice(part);
        }
        writer.write_vectored_all_zerocopy(parts).await.unwrap();
    }
    writer.shutdown().await.unwrap();
    drop(writer);
    drop(reader);
    let received = compio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, expected);
}

#[compio::test]
async fn cancelling_blocked_send_releases_owned_buffers_after_close() {
    use compio::io::AsyncRead;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct Owner {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    let mut listener = TcpTransport
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let conn = TcpTransport.connect(addr).await.unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let (mut receiver, _) = peer.into_split();
    let (reader, mut writer) = conn.into_split();
    let drops = Arc::new(AtomicUsize::new(0));
    let payload = Bytes::from_owner(Owner {
        bytes: vec![0x6a; 32 * 1024 * 1024],
        drops: drops.clone(),
    });
    let result = compio::time::timeout(
        Duration::from_millis(20),
        writer.write_vectored_all_zerocopy(vec![payload]),
    )
    .await;
    assert!(
        result.is_err(),
        "peer does not drain, so a 32 MiB send must block"
    );
    writer.shutdown().await.unwrap();
    drop(writer);
    drop(reader);
    compio::time::timeout(Duration::from_secs(10), async {
        loop {
            let (n, buf) = receiver.read(Vec::with_capacity(65536)).await.unwrap();
            assert!(buf.iter().all(|b| *b == 0x6a));
            if n == 0 {
                break;
            }
        }
        while drops.load(Ordering::SeqCst) == 0 {
            compio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
