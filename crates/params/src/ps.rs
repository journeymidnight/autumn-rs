//! autumn-ps (partition server) parameters. One process-wide instance:
//! `autumn_partition_server::params()`.

use crate::{GIB, KIB, MIB};

crate::params! {
    /// Every partition-server tunable.
    pub struct PsParams("ps") {
        // ── Background work speed (runtime) ──────────────────────────────
        /// Per-partition foreground byte rate cap (0 = unlimited).
        [runtime] fg_rate_bytes_per_sec: u64 = GIB, [0, u64::MAX];
        /// Per-partition foreground ops/s cap (0 = unlimited).
        [runtime] fg_iops_per_sec: u64 = 30_000, [0, u64::MAX];
        /// Per-partition compaction byte rate (0 = unlimited).
        [runtime] admission_compact_rate_bytes_per_sec: u64 = 256 * MIB, [0, u64::MAX];
        /// Per-partition GC byte rate in the admission controller (0 = unlimited).
        [runtime] admission_gc_rate_bytes_per_sec: u64 = 128 * MIB, [0, u64::MAX];
        /// Compaction and GC yield while foreground traffic is above this
        /// share of its cap.
        [runtime] fg_saturated_threshold: f64 = 0.8, [0.1, 1.0];
        /// Per-partition GC relocation read rate (0 = unlimited).
        [runtime] gc_rate_bytes_per_sec: u64 = 64 * MIB, [0, u64::MAX];
        /// Major compactions running at once across the PS (each holds about
        /// one output SST in memory).
        [runtime] major_compact_parallelism: usize = 4, [1, 64];
        /// GC runs at once across the PS.
        [runtime] gc_parallelism: usize = 4, [1, 64];
        /// Minor compaction: fewest tables in a window.
        [runtime] compact_min_files: usize = 3, [2, 1000];
        /// Minor compaction: most tables in a window.
        [runtime] compact_max_files: usize = 10, [2, 1000];
        /// Minor compaction: a window's newest-to-rest size ratio limit.
        [runtime] compact_ratio: f64 = 1.2, [0.01, 1000.0];
        /// Minor compaction: a window smaller than this in total is accepted
        /// without the ratio test.
        [runtime] compact_min_size_bytes: u64 = MIB, [0, u64::MAX];
        /// Minor compaction: a table this large is never merged by a minor
        /// (only a major or the head-extent reclaim rewrites it).
        [runtime] compact_large_size_bytes: u64 = 128 * MIB, [1, u64::MAX];
        /// Minor compaction: at this many mergeable tables a window is picked
        /// even when none passes the ratio.
        [runtime] compact_blocking_files: usize = 16, [2, 100_000];

        // ── Write path ───────────────────────────────────────────────────
        /// Requests in one group-commit batch.
        group_commit_cap: usize = 3072, [1, 1_000_000];
        /// Log append batches in flight per partition.
        ps_inflight_cap: usize = 8, [1, 64];
        /// SST uploads in flight per partition.
        ps_bulk_inflight_cap: usize = 2, [1, 16];
        /// Memtable flushes building at once per partition (committed in order).
        flush_inflight_cap: usize = 1, [1, 64];
        /// Immutable memtables queued before writes stall.
        max_imm_depth: usize = 4, [1, 64];
        /// Unflushed log bytes that force a memtable rotation (bounds replay).
        max_wal_gap: u64 = GIB, [128 * MIB, 64 * GIB];
        /// Size at which a stream's tail extent is sealed and a new one opened.
        max_extent_size_bytes: u64 = 16 * GIB, [GIB, 64 * GIB];
        /// Memtable rotation size = the size of a flushed SST.
        flush_mem_bytes: u64 = 256 * MIB, [4 * KIB, GIB];
        /// Requests admitted per client connection.
        conn_inflight_cap: usize = 4, [1, 4096];
        /// Per-connection read window.
        conn_read_buf_bytes: usize = 64 * KIB as usize, [4 * KIB as usize, 64 * MIB as usize];
        /// Values at least this large are received straight into a pooled
        /// buffer instead of the connection's read window.
        bulk_recv_min_bytes: usize = 64 * KIB as usize, [4 * KIB as usize, GIB as usize];
        /// Wait before retrying a failed flush.
        flush_retry_backoff_ms: u64 = 2000, [10, 600_000];

        // ── Compaction ───────────────────────────────────────────────────
        /// Data bytes at which a compaction cuts an output SST (at least twice
        /// the flush size; SST block offsets are u32).
        compact_max_sst_bytes: u64 = GIB, [4 * KIB, 3 * GIB];
        /// The head extent of the row stream is rewritten once its live tables
        /// are below this percent of its sealed bytes.
        compact_reclaim_live_percent: u64 = 30, [1, 99];
        /// Period of the head-extent reclaim probe.
        reclaim_check_interval_secs: u64 = 60, [1, 86_400];
        /// Period of the deletion-triggered major compaction check.
        deletion_compact_check_secs: u64 = 300, [1, 86_400];
        /// Tombstones a partition must hold before deletions trigger a major.
        deletion_compact_min_tombstones: u64 = 10_000, [1, u64::MAX];
        /// ... and their share of all entries, in percent.
        deletion_compact_min_percent: u64 = 30, [1, 100];
        /// Read window of compaction and split scans: one extent read per
        /// window, bypassing the block cache. Amortizes the round trip; N
        /// concurrent merge inputs hold N windows.
        scan_read_window_bytes: u32 = 8 * MIB as u32, [64 * KIB as u32, GIB as u32];
        /// Period of the open-tail and size gauge refresh.
        size_refresh_interval_secs: u64 = 30, [1, 3600];
        /// Bloom filter false-positive rate of new SSTs.
        sst_bloom_fp_rate: f64 = 0.01, [0.0001, 0.5];
        /// SST block cache per PS.
        sst_block_cache_bytes: usize = 512 * MIB as usize, [16 * MIB as usize, 256 * GIB as usize];

        // ── GC ───────────────────────────────────────────────────────────
        /// Read chunk of a GC relocation scan.
        gc_read_chunk_bytes: u32 = 8 * MIB as u32, [1, u32::MAX];
        /// Records per GC relocation append.
        gc_batch_records: usize = 256, [1, 4096];
        /// Bytes per GC relocation append.
        gc_batch_bytes: usize = 4 * MIB as usize, [64 * KIB as usize, 256 * MIB as usize];
        /// Extents rewritten per GC run.
        gc_max_extents_per_run: usize = 3, [1, 1000];
        /// Empty extents punched per GC run.
        gc_max_empty_per_run: usize = 32, [1, 100_000];
        /// Dead share above which an extent is rewritten, when the dispatch
        /// names none.
        gc_discard_ratio: f64 = 0.4, [0.0, 1.0];
        /// An extent whose GC failed is skipped for this long.
        gc_failure_cooldown_secs: u64 = 300, [1, 86_400];
        /// ... or this long, when the failure was a transient one.
        gc_failure_soft_cooldown_secs: u64 = 30, [1, 86_400];

        // ── Control plane and recovery ───────────────────────────────────
        /// Partitions opened at once (a takeover of many partitions).
        open_parallelism: usize = 64, [1, 4096];
        /// Longest backoff between attempts to open a failing partition.
        open_backoff_max_secs: u64 = 30, [1, 3600];
        /// Log bytes read per replay request when a partition opens.
        replay_chunk_bytes: u64 = 64 * MIB, [MIB, 4 * GIB];
        /// Period of the authz config fetch from the manager. A write to a
        /// namespace or a token with a key this PS does not know triggers an
        /// early fetch (at most once a second).
        config_poll_secs: u64 = 30, [1, 3600];
        /// Period of the region sync with the manager (partition assignment).
        region_sync_interval_secs: u64 = 2, [1, 60];
        /// The PS exits after failing to reach any manager for this long.
        heartbeat_failure_exit_secs: u64 = 90, [10, 86_400];
        /// The PS exits after hearing only NOT_LEADER for this long.
        heartbeat_not_leader_exit_secs: u64 = 900, [10, 86_400];
        /// Per-stage deadline of a graceful shutdown.
        shutdown_timeout_ms: u64 = 60_000, [1000, 600_000];
        /// Hedge a sealed-extent replica read after this many ms (0 = off).
        read_hedge_ms: u64 = 0, [0, 60_000];

        // ── Stream client (the PS's appends and reads to extent nodes) ───
        /// A replica that failed an append is avoided for this long.
        stream_bad_nodes_ttl_secs: u64 = 30, [1, 600];
        /// Appends in flight per stream.
        stream_inflight_cap: usize = 32, [1, 4096];
        /// Base deadline of one replica append; the deadline grows with the
        /// payload at `--stream-append-floor-bytes-per-sec`.
        stream_append_timeout_ms: u64 = 5000, [200, 60_000];
        /// Worst-case append throughput the deadline assumes.
        stream_append_floor_bytes_per_sec: u64 = 8 * MIB, [64 * KIB, GIB];
        /// Largest single read from one replica; bigger reads are chunked.
        stream_read_chunk_bytes: u64 = 256 * MIB, [KIB, GIB];
        /// Base deadline of one replica read; grows with the length at
        /// `--stream-read-floor-bytes-per-sec`.
        stream_read_timeout_ms: u64 = 3000, [100, 600_000];
        /// Worst-case read throughput the deadline assumes.
        stream_read_floor_bytes_per_sec: u64 = 4 * MIB, [64 * KIB, GIB];

        // ── Transport ────────────────────────────────────────────────────
        /// Replica appends at least this large are sent with TCP zerocopy
        /// (0 = off).
        tcp_zerocopy_min_bytes: usize = 0, [0, usize::MAX];
        /// UCX registered buffer pool cap per thread.
        ucx_regpool_cap_bytes: usize = 512 * MIB as usize, [16 * MIB as usize, 64 * GIB as usize];
        /// Log the UCX pool every this many seconds (0 = off).
        regpool_log_interval_secs: u64 = 0, [0, 86_400];
    }
}

impl crate::CrossCheck for PsParams {
    fn cross_check(&self) -> Result<(), String> {
        if self.compact_max_files.get() < self.compact_min_files.get() {
            return Err(format!(
                "compact-max-files {} is below compact-min-files {}",
                self.compact_max_files.get(),
                self.compact_min_files.get()
            ));
        }
        if self.compact_blocking_files.get() < self.compact_min_files.get() {
            return Err(format!(
                "compact-blocking-files {} is below compact-min-files {}",
                self.compact_blocking_files.get(),
                self.compact_min_files.get()
            ));
        }
        Ok(())
    }
}
