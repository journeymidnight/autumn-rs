use super::*;
#[path = "../../rpc/tests/support/status_peer.rs"]
mod peer;
use autumn_rpc::RpcError;
use peer::*;

#[compio::test]
async fn status_errors_reuse_connections_in_every_call_shape() {
    let peer = Peer::start(respond).await;
    let pool = ConnPool::new();
    let sock = parse_addr(&peer.addr).unwrap();
    for shape in 0..4 {
        for code in STATUSES {
            let payload = Bytes::from(vec![code as u8]);
            let error = match shape {
                0 => pool.call(&peer.addr, STATUS, payload).await.unwrap_err(),
                1 => pool
                    .call_timeout(&peer.addr, STATUS, payload, Duration::from_secs(2))
                    .await
                    .unwrap_err(),
                2 => pool
                    .call_vectored(&peer.addr, STATUS, vec![payload])
                    .await
                    .unwrap_err(),
                _ => pool
                    .call_into_pooled(&peer.addr, STATUS, payload, Duration::from_secs(2))
                    .await
                    .err()
                    .unwrap(),
            };
            assert!(
                matches!(error.downcast_ref::<RpcError>(), Some(RpcError::Status { code: actual, .. }) if *actual == code)
            );
            assert!(
                pool.clients.borrow().contains_key(&sock),
                "{shape}: {code:?} evicted a healthy peer"
            );
            assert_eq!(
                pool.call(&peer.addr, ECHO, Bytes::new()).await.unwrap(),
                b"ok"[..]
            );
        }
    }
    assert_eq!(
        peer.accepts.get(),
        1,
        "all status replies and successful calls must share one TCP connection"
    );
}

#[compio::test]
async fn transport_failures_and_local_timeouts_still_evict() {
    let peer = Peer::start(respond).await;
    let pool = ConnPool::new();
    for shape in 0..4 {
        for failure in [CLOSE, BAD_CRC, HANG] {
            if failure == HANG && (shape == 0 || shape == 2) {
                continue;
            }
            let result = match shape {
                0 => pool.call(&peer.addr, failure, Bytes::new()).await,
                1 => {
                    pool.call_timeout(&peer.addr, failure, Bytes::new(), Duration::from_millis(50))
                        .await
                }
                2 => pool.call_vectored(&peer.addr, failure, vec![]).await,
                _ => pool
                    .call_into_pooled(&peer.addr, failure, Bytes::new(), Duration::from_millis(50))
                    .await
                    .map(|_| Bytes::new()),
            };
            assert!(result.is_err());
            assert!(
                !pool.is_healthy(&peer.addr),
                "{shape}: {failure} must evict"
            );
            let before = peer.accepts.get();
            assert_eq!(
                pool.call(&peer.addr, ECHO, Bytes::new()).await.unwrap(),
                b"ok"[..]
            );
            assert_eq!(peer.accepts.get(), before + 1);
        }
    }
}

#[compio::test]
async fn corrupt_bulk_prologue_evicts_instead_of_becoming_a_peer_status() {
    let peer = Peer::start(|f| {
        if f.msg_type == BAD_CRC {
            Reply::BadCrc(autumn_rpc::Frame::response_zc(
                f.req_id,
                f.msg_type,
                Bytes::from_static(&[0]),
                Bytes::from(vec![7; autumn_rpc::client::TCP_RECV_INTO_POOLED_MIN_BYTES]),
            ))
        } else {
            respond(f)
        }
    })
    .await;
    let pool = ConnPool::new();
    let error = pool
        .call_into_pooled(&peer.addr, BAD_CRC, Bytes::new(), Duration::from_secs(2))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            error.downcast_ref::<RpcError>(),
            Some(RpcError::ConnectionClosed)
        ),
        "corrupt prologue must be a transport failure: {error:?}"
    );
    assert!(!pool.is_healthy(&peer.addr));
    assert_eq!(
        pool.call(&peer.addr, ECHO, Bytes::new()).await.unwrap(),
        b"ok"[..]
    );
    assert_eq!(peer.accepts.get(), 2);
}
