//! autumn-convert parameters. Each subcommand reads the flags it names.

crate::params! {
    /// autumn-convert parameters.
    pub struct ConvertParams("autumn-convert") {
        /// Manager endpoints.
        manager: String = "127.0.0.1:9001".into();
        /// The cluster secret the servers were started with.
        cluster_secret_file: Option<std::path::PathBuf>;
        /// Report what would change, write nothing.
        [switch] dry_run: bool;
        /// Partitions converted at once.
        parallel: usize = 4, [1, 64];
        /// Only these partitions; every partition when not given.
        part: Vec<u64>;
        /// Partitions whose several checkpoint records are their own (a
        /// deferred cut, not a merge's sources): convert the newest, cut the rest.
        keep_newest: Vec<u64>;
        /// The partition servers' `--max-extent-size-bytes`.
        max_extent_size_bytes: u64 = 16 * crate::GIB, [crate::GIB, 64 * crate::GIB];
    }
}

impl crate::CrossCheck for ConvertParams {}
