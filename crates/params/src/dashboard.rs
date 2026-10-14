//! autumn-dashboard parameters.

crate::params! {
    /// Every dashboard tunable.
    pub struct DashboardParams("dashboard") {
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
