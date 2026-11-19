//! autumn-convert parameters. Each subcommand reads the flags it names.

crate::params! {
    /// autumn-convert parameters.
    pub struct ConvertParams("autumn-convert") {
        /// etcd endpoints (`extent-records`).
        etcd: Vec<String>;
        /// Manager endpoints (`sst`).
        manager: String = "127.0.0.1:9001".into();
        /// The cluster secret the servers were started with (`sst`).
        cluster_secret_file: Option<std::path::PathBuf>;
        /// Report what would change, write nothing.
        [switch] dry_run: bool;
        /// Partitions converted at once (`sst`).
        parallel: usize = 4, [1, 64];
        /// Only these partitions (`sst`); every partition when not given.
        part: Vec<u64>;
        /// The partition servers' `--max-extent-size-bytes` (`sst`).
        max_extent_size_bytes: u64 = 16 * crate::GIB, [crate::GIB, 64 * crate::GIB];
    }
}

impl crate::CrossCheck for ConvertParams {}
