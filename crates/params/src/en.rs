//! autumn-extent-node parameters. One process-wide instance shared by every
//! shard: `autumn_stream::en_params()`.

use crate::{GIB, KIB, MIB};

crate::params! {
    /// Every extent-node parameter.
    pub struct EnParams("en") {
        // ── Process ──────────────────────────────────────────────────────
        /// Data dirs formatted by `autumn-op format`; comma-separated or repeated.
        [required] data: Vec<std::path::PathBuf>;
        /// Shard 0 data port; shard i binds port + i * shard-stride.
        port: u16 = 9101;
        /// Port stride between shards.
        shard_stride: u16 = 10, [1, u16::MAX];
        /// Control port of shard 0 (default port+1000).
        control_port: Option<u16>;
        /// Manager endpoints (requires --advertise).
        manager: Option<String>;
        /// `IP:PORT` announced to the manager at startup; PORT is --port.
        advertise: Option<String>;
        /// Bind host.
        listen: String = "0.0.0.0".into();
        transport: crate::Transport;
        /// Cluster secret; peer and admin connections prove it. The server
        /// refuses to start without it.
        cluster_secret_file: Option<std::path::PathBuf>;
        /// Cores, like taskset; one shard per core (default: the mask it was
        /// started with).
        cpuset: Option<crate::CpuSet>;
        /// No O_DIRECT for append bursts of 1 MiB and more (on by default on
        /// Linux).
        [switch] no_direct_io: bool;
        /// /metrics port (default port+500, 0 = off).
        metrics_port: Option<u16>;
        /// /metrics bind host (default: --listen).
        metrics_listen: Option<String>;

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
        /// Period of the authz config fetch from the manager (one per process).
        /// A token with a key this node does not know triggers an early fetch
        /// (at most once a second).
        config_poll_secs: u64 = 30, [1, 3600];
        /// Period of the sweep for extent files no stream references.
        reconcile_sweep_interval_secs: u64 = 300, [10, 86_400];

        // ── Connections ──────────────────────────────────────────────────
        /// Requests in flight per client connection.
        inflight_cap: usize = 64, [1, 65_536];
        /// Per-connection read window.
        conn_read_buf_bytes: usize = 512 * KIB as usize, [4 * KIB as usize, 64 * MIB as usize];
        /// UCX registered buffer pool cap per thread.
        ucx_regpool_cap_bytes: usize = 2 * GIB as usize, [16 * MIB as usize, 64 * GIB as usize];
    }

    removed {
        "cpu-start" => "--cpu-start was removed; pass --cpuset <SPEC> with the cores to use",
        "shards" => "--shards was removed; pass --cpuset <SPEC> to size the EN (shard count = cpuset_len)",
        "disk-id" => "--disk-id was removed. Run `autumn-op format <DIR>...` first; the EN reads disk_id from each dir's `disk_id` sentinel file.",
    }
}

impl crate::CrossCheck for EnParams {}

impl EnParams {
    /// O_DIRECT for large append bursts: on by default, and only on Linux.
    pub fn direct_io(&self) -> bool {
        cfg!(target_os = "linux") && !self.no_direct_io.get()
    }
}
