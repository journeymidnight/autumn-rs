pub mod client;
pub mod conn_pool;
pub mod erasure;
pub mod extent_node;
/// F-unify: extent_rpc relocated to autumn-rpc (joins manager_rpc /
/// partition_rpc as the single wire-schema home). Re-exported here so all
/// existing `autumn_stream::extent_rpc::*` / `crate::extent_rpc::*` paths
/// keep resolving unchanged.
pub use autumn_rpc::extent_rpc;

pub use client::{read_extent_value_direct, set_append_chain_min_bytes, set_read_hedge_ms, AppendResult, StaleVpOffset, StreamClient};
pub use conn_pool::{normalize_endpoint, shard_addr_for_extent, ConnPool};
pub use extent_node::{
    render_en_metrics, set_ec_encode_stripe_bytes, set_fd_cache_cap, ExtentNode, ExtentNodeConfig,
};
