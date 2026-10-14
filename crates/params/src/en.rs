//! autumn-extent-node parameters. One process-wide instance shared by every
//! shard: `autumn_stream::en_params()`.

use crate::{GIB, KIB, MIB};

crate::params! {
    /// Every extent-node tunable.
    pub struct EnParams("en") {
        // ── Recovery and scrub speed (runtime) ───────────────────────────
        /// Recoveries (replica rebuilds) running at once per shard.
        [runtime] recovery_parallelism: usize = 2, [1, 16];
        /// Bytes a scrub reads per second per shard (0 = unpaced): each shard
        /// runs its own scrub worker, so a node with N shards reads up to N
        /// times this.
        [runtime] scrub_bytes_per_sec: u64 = 8 * MIB, [0, u64::MAX];

        // ── EC ───────────────────────────────────────────────────────────
        /// EC conversions running at once per shard.
        ec_convert_parallelism: usize = 1, [1, 16];
        /// EC conversion stripe: peak memory is (K+M) stripes.
        ec_stripe_bytes: usize = 64 * MIB as usize, [MIB as usize, GIB as usize];
        /// EC rebuild stripe: peak memory is (K+1) stripes.
        ec_recovery_stripe_bytes: u64 = 64 * MIB, [MIB, GIB];
        /// Timeout of one EC shard-write RPC to a participant.
        ec_participant_rpc_timeout_secs: u64 = 30, [1, 3600];

        // ── Recovery ─────────────────────────────────────────────────────
        /// Attempts of one recovery task before it reports failure.
        recovery_max_retries: u32 = 10, [1, 1000];
        /// Wait between two recovery attempts.
        recovery_retry_backoff_secs: u64 = 10, [1, 3600];
        /// Timeout of one read from a recovery source.
        recovery_read_timeout_secs: u64 = 30, [1, 3600];

        // ── Disks and files ──────────────────────────────────────────────
        /// fallocate this many bytes for every new extent (0 = off).
        prealloc_bytes: u64 = 0, [0, 64 * GIB];
        /// Open file descriptors cached for sealed extents, per process
        /// (split across shards).
        fd_cache_cap: usize = 4096, [64, 1_000_000];
        /// A full disk accepts allocations again once this percent is free.
        disk_full_resume_free_percent: u64 = 5, [1, 100];
        /// Deadline of the write + fsync probe that decides a disk is faulted.
        disk_probe_timeout_ms: u64 = 2000, [100, 600_000];
        /// Period of the sweep for extent files no stream references.
        reconcile_sweep_interval_secs: u64 = 300, [10, 86_400];

        // ── Connections ──────────────────────────────────────────────────
        /// Requests in flight per client connection.
        inflight_cap: usize = 64, [1, 65_536];
        /// Per-connection read window.
        conn_read_buf_bytes: usize = 512 * KIB as usize, [4 * KIB as usize, 64 * MIB as usize];
        /// UCX registered buffer pool cap per thread.
        ucx_regpool_cap_bytes: usize = 512 * MIB as usize, [16 * MIB as usize, 64 * GIB as usize];
    }
}

impl crate::CrossCheck for EnParams {}
