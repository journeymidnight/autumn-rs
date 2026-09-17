use super::*;
#[path = "../../rpc/tests/support/status_peer.rs"]
mod peer;
use peer::*;
use std::time::Duration;

#[compio::test]
async fn manager_pool_preserves_status_connections_and_evicts_transport_failures() {
    let peer = Peer::start(respond).await;
    let pool = ConnPool::new();
    let sock = parse_addr(&peer.addr).unwrap();
    for timed in [false, true] {
        for code in STATUSES {
            let payload = Bytes::from(vec![code as u8]);
            let result = if timed {
                pool.call_timeout(&peer.addr, STATUS, payload, Duration::from_secs(2))
                    .await
            } else {
                pool.call(&peer.addr, STATUS, payload).await
            };
            assert!(
                matches!(result.unwrap_err().downcast_ref::<autumn_rpc::RpcError>(),
                Some(autumn_rpc::RpcError::Status { code: actual, .. }) if *actual == code)
            );
            assert!(pool.conns.borrow().contains_key(&sock));
            assert_eq!(
                pool.call(&peer.addr, ECHO, Bytes::new()).await.unwrap(),
                b"ok"[..]
            );
        }
    }
    assert_eq!(peer.accepts.get(), 1);
    for timed in [false, true] {
        for failure in [CLOSE, BAD_CRC, HANG] {
            if failure == HANG && !timed {
                continue;
            }
            let result = if timed {
                pool.call_timeout(&peer.addr, failure, Bytes::new(), Duration::from_millis(50))
                    .await
            } else {
                pool.call(&peer.addr, failure, Bytes::new()).await
            };
            assert!(result.is_err());
            assert!(!pool.conns.borrow().contains_key(&sock));
            let before = peer.accepts.get();
            pool.call(&peer.addr, ECHO, Bytes::new()).await.unwrap();
            assert_eq!(peer.accepts.get(), before + 1);
        }
    }
}
