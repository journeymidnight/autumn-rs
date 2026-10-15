//! autumn-dashboard parameters.

crate::params! {
    /// Every dashboard parameter.
    pub struct DashboardParams("dashboard") {
        // ── Process ──────────────────────────────────────────────────────
        /// Manager endpoints.
        manager: String = "127.0.0.1:9001".into();
        transport: crate::Transport;
        /// Bind host.
        listen: String = "0.0.0.0".into();
        /// Listen port.
        port: u16 = 8799;
        /// The autumn-op binary every action runs.
        autumn_op: String = "autumn-op".into();
        /// Cluster secret, handed to autumn-op.
        [required] cluster_secret_file: std::path::PathBuf;

        /// Period of the metrics poll.
        metrics_interval_secs: u64 = 10, [1, 3600];
        /// History kept in memory.
        metrics_retention_secs: u64 = 3600, [60, 30 * 86_400];
        /// Deadline of one autumn-op call.
        op_timeout_secs: u64 = 30, [1, 3600];
        /// Period of the scrape target list refresh.
        target_refresh_secs: u64 = 15, [1, 3600];
        /// Deadline of one /metrics scrape.
        scrape_timeout_secs: u64 = 3, [1, 600];
    }
}

impl crate::CrossCheck for DashboardParams {}

