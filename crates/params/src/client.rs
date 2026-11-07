//! Client SDK parameters, shared by every binary that embeds `ClusterClient`
//! (autumn-client, autumnfs, autumn-fuse, autumn-s3, gallery). One
//! process-wide instance: `autumn_client::params()`.

use crate::MIB;

crate::params! {
    /// Every client SDK tunable.
    pub struct ClientParams("client") {
        /// Deadline of one data-plane request (retries included).
        rpc_timeout_ms: u64 = 30_000, [100, 3_600_000];
        /// Deadline of the first attempt of a request; a slow first try is
        /// retried elsewhere instead of waiting out the whole deadline.
        first_attempt_timeout_ms: u64 = 5000, [100, 3_600_000];
        /// Partition requests a batch get keeps in flight.
        batch_get_concurrency: usize = 32, [1, 4096];
        /// Partition requests a batch put keeps in flight.
        batch_put_concurrency: usize = 32, [1, 4096];
        /// Large requests (64 KiB and up) a PS connection holds unanswered;
        /// the rest wait in the client, where reads go ahead of them. Under
        /// the PS's `--conn-inflight-cap` (16), so a read finds a free
        /// admission slot; 0 = no limit.
        ps_send_window: usize = 3, [0, 4096];
        /// A minted token is renewed when less than this much of its life is
        /// left.
        token_renew_margin_secs: u64 = 300, [10, 86_400];
        /// Chunk size of a striped put when the caller names none.
        stripe_chunk_bytes: u32 = 4 * MIB as u32, [64 * 1024, 64 * MIB as u32];
        /// Buffer pool cap for the whole process (chunks, registered under UCX).
        ucx_regpool_cap_bytes: usize = 16 * crate::GIB as usize, [64 * MIB as usize, 64 * crate::GIB as usize];
    }
}

impl crate::CrossCheck for ClientParams {}
