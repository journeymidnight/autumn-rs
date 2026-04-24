//! UCX loopback suite — same shape as `loopback_tcp.rs` but binds on a
//! RoCE-attached IPv6 so the data path actually goes through `rc_mlx5`.
//!
//! ## Status
//!
//! - `ping_pong`, `large_payload`, `many_concurrent_4`: pass individually
//!   (UCX 1.16 + `rc_mlx5/mlx5_0:1`, real RoCEv2 wire). Run with
//!   `--test-threads=1` to keep the per-thread worker contention bounded.
//! - `half_close`: `#[ignore]` — `ucp_stream` is a duplex byte pipe with
//!   no FIN/EOF concept, so a half-close test cannot pass.
//!
//! ### Production caveat — close-on-drop
//!
//! `UcxConn::Drop` does NOT call `ucp_ep_close_nbx` (see endpoint.rs);
//! UCX's close synchronously cancels in-flight ops on the peer's side
//! of the EP, which races with the peer's pending recv (TCP buffers
//! bytes in the kernel; UCX RC does not). For path 1/2 in autumn-rs
//! this is fine: connection pools (autumn-rpc::pool, autumn-stream
//! conn_pool) reuse one EP per peer address for the life of the
//! process. The full close path (with `ucp_worker_flush` + await) is
//! tracked in spec §10 as a P3+ follow-up.

#![cfg(feature = "ucx")]

mod common;

use autumn_transport::UcxTransport;
use std::net::SocketAddr;

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .unwrap_or_else(|_| "[fdbb:dc62:3:3::16]:0".to_string())
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

#[compio::test]
async fn ping_pong() {
    common::ping_pong_at(UcxTransport, bind_addr()).await;
}

#[compio::test]
async fn large_payload() {
    common::large_payload_at(UcxTransport, bind_addr()).await;
}

#[compio::test]
#[ignore = "ucp_stream has no FIN/EOF semantics"]
async fn half_close() {
    common::half_close_at(UcxTransport, bind_addr()).await;
}

#[compio::test]
async fn many_concurrent_4() {
    common::many_concurrent_at(UcxTransport, bind_addr(), 4).await;
}
