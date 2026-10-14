//! autumn-manager parameters.

use crate::{GIB, MIB};

const DAY: u64 = 86_400;

crate::params! {
    /// Every manager tunable. One instance per `AutumnManager`.
    pub struct ManagerParams("manager") {
        // ── Recovery and repair speed (runtime) ──────────────────────────
        /// Recoveries in flight across the cluster.
        [runtime] recovery_max_global: u32 = 64, [1, 100_000];
        /// Recoveries in flight reading from one source node.
        [runtime] recovery_max_per_source: u32 = 4, [1, 10_000];
        /// Recoveries in flight writing to one target node.
        [runtime] recovery_max_per_target: u32 = 2, [1, 10_000];
        /// Longest retry backoff of a failing (extent, slot) recovery; the
        /// backoff doubles per failure from 1 s up to this.
        [runtime] recovery_backoff_max_secs: u64 = 300, [1, DAY];
        /// In-place catch-ups (a lagging replica re-synced where it is) in
        /// flight across the cluster.
        [runtime] catch_up_max_inflight: usize = 8, [1, 10_000];
        /// EC conversions dispatched per dispatch round.
        [runtime] ec_dispatch_concurrency: usize = 8, [1, 10_000];
        /// How long a slot stays degraded before the repair policy proposes
        /// rebuilding it elsewhere.
        [runtime] repair_grace_secs: u64 = 600, [0, 30 * DAY];

        // ── Liveness and leadership ──────────────────────────────────────
        /// An extent node silent this long (no df answer) is Suspected.
        node_suspected_timeout_secs: u64 = 10, [1, 3600];
        /// Period of the df poll that drives node liveness and free space.
        node_health_interval_secs: u64 = 2, [1, 60];
        /// Timeout of one df RPC.
        df_timeout_secs: u64 = 5, [1, 600];
        /// A partition server silent this long (no heartbeat) is evicted and
        /// its partitions reassigned. The PS heartbeats every 2 s.
        ps_dead_timeout_secs: u64 = 10, [4, 3600];
        /// TTL of the leader lease: an upper bound on failover time. Kept
        /// alive every 2 s, so it must leave room for a few missed beats.
        leader_lease_ttl_secs: u64 = 10, [6, 600];
        /// How long a leaderless ex-leader that has not seen a successor keeps
        /// answering routing reads.
        routable_stale_ttl_secs: u64 = 900, [0, DAY];
        /// Connect timeout of the manager's connections to nodes and servers.
        connect_timeout_ms: u64 = 5000, [500, 600_000];
        /// Timeout of one etcd request (also bounds its reconnect).
        etcd_request_timeout_ms: u64 = 10_000, [1000, 600_000];

        // ── In-flight markers and sweeps ─────────────────────────────────
        /// Age after which an in-flight Delete marker is released (an EC
        /// conversion marker only gets a warning).
        inflight_stale_threshold_secs: u64 = 600, [60, 30 * DAY];
        /// Period of the stale in-flight marker sweep.
        inflight_sweep_interval_secs: u64 = 60, [1, DAY];
        /// Period of the sealed-empty extent backstop sweep.
        sealed_empty_sweep_interval_secs: u64 = 60, [1, DAY];
        /// Sealed-empty extents reclaimed per sweep.
        sealed_empty_sweep_max_per_tick: usize = 64, [1, 1_000_000];
        /// Period of the sweep for extents no stream references.
        orphan_sweep_interval_secs: u64 = 60, [1, DAY];
        /// Delete fan-out attempts (2 s apart) before the delete is moved to
        /// the persisted retry queue.
        delete_fanout_max_attempts: u32 = 60, [1, 100_000];
        /// Longest backoff of the persisted delete retry queue.
        delete_retry_backoff_max_secs: u64 = 3600, [60, 30 * DAY];
        /// Reconcile rounds a replica outside its extent's membership survives
        /// before it is deleted.
        non_member_rounds_before_gc: u32 = 3, [1, 1000];

        // ── Allocation and placement ─────────────────────────────────────
        /// Nodes whose best disk has less free space are avoided when
        /// allocating extents (0 = off).
        min_alloc_free_bytes: u64 = 256 * MIB, [0, u64::MAX];
        /// Free space a non-forced node fence requires on the remaining
        /// nodes, as a percentage of the bytes it moves.
        fence_headroom_percent: u64 = 120, [100, 1000];
        /// Sliding window of the disk-failure report quorum.
        report_disk_failure_window_secs: u64 = 60, [1, DAY];
        /// Distinct reporters needed to mark a disk faulted.
        report_disk_failure_quorum: usize = 3, [1, 1000];

        // ── Operations ───────────────────────────────────────────────────
        /// How long a submitted split waits for the PS's reply; past it the
        /// outcome is unknown, not failed.
        split_reply_timeout_secs: u64 = 60, [1, 3600];
        /// Timeout of a compaction or GC dispatch to a PS.
        maintenance_rpc_timeout_secs: u64 = 60, [1, 3600];
        /// Timeout of one EC conversion RPC.
        ec_convert_timeout_secs: u64 = 60, [1, 3600];
        /// Timeout of the first recovery dispatch to a node.
        recovery_dispatch_timeout_secs: u64 = 30, [1, 3600];
        /// Timeout of an in-place catch-up RPC.
        catch_up_timeout_secs: u64 = 30, [1, 3600];
        /// Timeout of a merge freeze RPC.
        merge_freeze_timeout_secs: u64 = 30, [1, 3600];
        /// Longest a split or merge waits for in-flight extent ops on its
        /// partitions before giving up.
        topology_blocker_wait_max_secs: u64 = 600, [1, DAY];
        /// Ops kept in the in-memory op ledger.
        op_ledger_cap: usize = 256, [16, 1_000_000];
        /// A running PS op not heard from for this long is reported unknown.
        op_running_ttl_secs: u64 = 1800, [60, 30 * DAY];
        /// Op-history records kept in etcd.
        op_log_cap: usize = 2000, [16, 1_000_000];
        /// A scrub op not heard from for this long is reported unknown.
        scrub_op_silence_secs: u64 = 7200, [60, 30 * DAY];
        /// Shortest gap between two scrubs proposed by the auto-policy.
        scrub_policy_interval_secs: u64 = 7 * DAY, [3600, 365 * DAY];
        /// Audit records older than this are deleted (0 = keep forever).
        audit_retention_days: u64 = 90, [0, 36_500];

        // ── File system leases ───────────────────────────────────────────
        /// Inode writer lease TTL. File system clients heartbeat every 5 s,
        /// so it must leave room for a couple of missed beats.
        inode_lease_ttl_secs: u32 = 30, [15, 3600];
        /// Grace given to a lease holder before a forced revoke.
        inode_revoke_grace_ms: u64 = 5000, [0, 600_000];

        // ── Data-plane authz ─────────────────────────────────────────────
        /// Lifetime of minted tokens.
        auth_token_ttl_secs: u64 = 3600, [60, 30 * DAY];
        /// Clock-skew leeway servers apply to token validity.
        auth_clock_skew_secs: u64 = 60, [0, 3600];

        // ── Policy engine (advisories the auto-policy acts on) ───────────
        /// Seconds per load bucket.
        policy_bucket_secs: i64 = 60, [1, 3600];
        /// Buckets kept per partition.
        policy_window_buckets: usize = 10, [1, 1000];
        /// Consecutive buckets a condition must hold before it is advised.
        policy_required_buckets: usize = 5, [1, 1000];
        /// Period of the policy tick.
        policy_tick_secs: i64 = 60, [1, 3600];
        /// A partition's load window is dropped when its newest bucket is
        /// this old.
        policy_stale_metrics_secs: i64 = 300, [1, DAY as i64];
        /// LSM-resident bytes above which a partition is split.
        split_lsm_hard_bytes: u64 = 50 * GIB, [1, u64::MAX];
        /// Carried bytes a partition must exceed before a rate trigger may
        /// split it.
        split_size_min_bytes: u64 = GIB, [0, u64::MAX];
        /// Sustained requests/s above which a partition is split (the
        /// single-partition ceiling is ~30K).
        split_qps_high: u32 = 15_000, [1, u32::MAX];
        /// Sustained read+write bytes/s above which a partition is split (one
        /// log stream tops out near 350 MB/s).
        split_bw_high_bytes: u64 = 175 * MIB, [1, u64::MAX];
        /// Sustained memtable-full stalls/s above which a partition is split.
        split_immfull_high: u32 = 10, [1, u32::MAX];
        /// Shortest gap between two splits of one partition.
        split_cooldown_secs: i64 = 3600, [0, 30 * DAY as i64];
        /// Carried bytes below which a pair of partitions may merge.
        merge_size_low_bytes: u64 = GIB, [0, u64::MAX];
        /// Requests/s below which a pair counts as cold. Keep it about a tenth
        /// of `--split-qps-high` (hysteresis).
        merge_qps_low: u32 = 1500, [0, u32::MAX];
        /// Read+write bytes/s below which a pair counts as cold. Keep it about
        /// a tenth of `--split-bw-high-bytes` (hysteresis).
        merge_bw_low_bytes: u64 = 175 * MIB / 10, [0, u64::MAX];
        /// Shortest gap between two merges involving one partition.
        merge_cooldown_secs: i64 = 6 * 3600, [0, 30 * DAY as i64];
        /// Sustained GC debt above which a GC is advised.
        gc_debt_high_bytes: u64 = GIB, [1, u64::MAX];
        /// Sustained pending compaction bytes above which a major compaction
        /// is advised.
        compact_pending_high_bytes: u64 = 4 * GIB, [1, u64::MAX];
        /// Shortest gap between two GC advisories for one partition.
        gc_cooldown_secs: i64 = 300, [0, 30 * DAY as i64];
        /// Shortest gap between two compaction advisories for one partition.
        compact_cooldown_secs: i64 = 300, [0, 30 * DAY as i64];
        /// Sealed extents smaller than this are not advised for EC.
        ec_min_extent_bytes: u64 = 64 * MIB, [0, u64::MAX];
        /// Per-PS partition-count spread above which a rebalance is advised
        /// (0 = never).
        rebalance_gap_threshold: usize = 2, [0, 1_000_000];
        /// Shortest gap between two rebalance advisories.
        rebalance_cooldown_secs: i64 = 120, [0, 30 * DAY as i64];
        /// Partitions one rebalance moves per tick.
        rebalance_max_moves_per_tick: u32 = 4, [1, 10_000];
        /// Hot/cold advisory: hottest / coldest requests/s ratio on one PS.
        hot_cold_ratio: u32 = 10, [2, u32::MAX];
        /// Hot/cold advisory: the hottest partition's floor, requests/s.
        hot_cold_min_hot_qps: u32 = 10_000, [0, u32::MAX];
        /// Hot/cold advisory: largest / smallest carried bytes ratio.
        hot_cold_size_ratio: u64 = 10, [2, u64::MAX];
        /// Hot/cold advisory: the largest partition's floor.
        hot_cold_min_hot_size_bytes: u64 = 25 * GIB, [0, u64::MAX];
        /// Shortest gap between two hot/cold advisories for one PS.
        hot_cold_cooldown_secs: i64 = 300, [0, 30 * DAY as i64];
    }
}

impl ManagerParams {
    /// `--policy-fast-mode`: thresholds for a short-lived dev or test cluster.
    /// Applied before the other flags, so an explicit flag still wins.
    pub fn apply_policy_fast_mode(&self) {
        self.policy_required_buckets.set(1);
        self.policy_tick_secs.set(5);
        self.policy_bucket_secs.set(5);
        self.gc_debt_high_bytes.set(MIB);
        self.compact_pending_high_bytes.set(4 * MIB);
        self.gc_cooldown_secs.set(30);
        self.compact_cooldown_secs.set(30);
        self.split_cooldown_secs.set(30);
        self.merge_cooldown_secs.set(30);
        self.sealed_empty_sweep_interval_secs.set(5);
    }
}

impl crate::CrossCheck for ManagerParams {
    fn cross_check(&self) -> Result<(), String> {
        if self.policy_required_buckets.get() > self.policy_window_buckets.get() {
            return Err(format!(
                "--policy-required-buckets {} exceeds --policy-window-buckets {}",
                self.policy_required_buckets.get(),
                self.policy_window_buckets.get()
            ));
        }
        Ok(())
    }
}
