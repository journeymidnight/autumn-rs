//! The manager and the extent node answer the keepalive ping as a success,
//! from their real connection loops — the thing a client's dead-peer detection
//! relies on to tell a live idle peer from a silent one.
mod support;

use autumn_rpc::client::RpcClient;
use autumn_rpc::MSG_TYPE_PING;
use bytes::Bytes;
use support::{pick_stable_port_pair, start_extent_node, start_manager};

#[test]
fn manager_and_extent_node_answer_the_keepalive_ping() {
    let mgr_addr: std::net::SocketAddr =
        format!("127.0.0.1:{}", pick_stable_port_pair()).parse().unwrap();
    let en_addr: std::net::SocketAddr =
        format!("127.0.0.1:{}", pick_stable_port_pair()).parse().unwrap();
    let dir = tempfile::tempdir().unwrap();
    start_manager(mgr_addr);
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        for addr in [mgr_addr, en_addr] {
            let c = RpcClient::connect(addr).await.expect("connect");
            let pong = c
                .call(MSG_TYPE_PING, Bytes::new())
                .await
                .unwrap_or_else(|e| panic!("{addr} must answer the ping as a success: {e}"));
            assert!(pong.is_empty(), "{addr}");
        }
    });
}
