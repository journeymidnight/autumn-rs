//! autumn-s3 gateway parameters.

crate::params! {
    /// Every S3 gateway parameter.
    pub struct S3Params("s3") {
        // ── Process ──────────────────────────────────────────────────────
        /// Manager endpoints.
        [required] manager: String;
        /// Bind host.
        listen: String = "0.0.0.0".into();
        /// Listen port.
        port: u16 = 9000;
        /// Daemon identity; each thread leases as `<host>-<n>`.
        host: String = "autumn-s3".into();
        /// Credential file (`<principal>\n<hex>`, from `autumn-op
        /// principal-create`); needed when the cluster protects `fs/`.
        credential_file: Option<std::path::PathBuf>;
        /// Read extents of 64 KiB and more straight from the extent nodes
        /// (a failed direct read falls back to the PS).
        direct_read: bool = true;

        /// Accept threads (SO_REUSEPORT). The gateway is CPU-bound on socket
        /// work: an AWS-CRT client drains slowly enough that each body write
        /// fragments, and one thread saturates well below the read path. As a
        /// sidecar it should not claim every core (0 = the core count, at most
        /// 8).
        workers: usize = 0, [0, 1024];
        /// Period of the sweep that recovers dead uploads and reclaims
        /// unlinked files (0 = no periodic sweep).
        sweep_interval_secs: u64 = 30, [0, 86_400];
        /// An unused read pin on an object is kept this long, so a run of
        /// ranged GETs pays one acquire.
        pin_linger_ms: u64 = 2000, [0, 600_000];
    }
}

impl crate::CrossCheck for S3Params {}

