//! autumn-fuse parameters. One process-wide instance: `autumn_fuse::params()`.

use crate::MIB;

crate::params! {
    /// Every FUSE daemon tunable.
    pub struct FuseParams("fuse") {
        /// Read I/O threads, each with its own runtime and cluster client
        /// (0 = reads stay on the dispatcher).
        read_io_threads: usize = 4, [0, 256];
        /// Memory the daemon may hold for its own readahead, MiB (0 = off;
        /// needs read-io-threads > 0).
        prefetch_mem_mb: u64 = 1024, [0, 1_048_576];
        /// Prefetch unit; blocks start at multiples of it.
        prefetch_block_bytes: u64 = 4 * MIB, [64 * 1024, 256 * MIB];
        /// How far ahead of a reader's front blocks are kept in flight.
        prefetch_window_bytes: u64 = 64 * MIB, [64 * 1024, 16 * 1024 * MIB];
        /// A READ this close to a front continues it (one kernel readahead
        /// window).
        prefetch_front_tolerance_bytes: u64 = 2 * MIB, [0, 256 * MIB];
        /// A run is sequential once it has read this much.
        prefetch_min_run_bytes: u64 = 3 * MIB, [0, 1024 * MIB];
        /// Sequential fronts tracked per file.
        prefetch_max_fronts: usize = 16, [1, 1024];
        /// A prefetched block nobody read is freed after this long.
        prefetch_idle_secs: u64 = 5, [1, 3600];
        /// Kernel attr/entry cache TTL.
        attr_ttl_secs: u64 = 30, [0, 86_400];
        /// Background requests (readahead READs) the kernel keeps in flight; the
        /// congestion threshold is 3/4 of it.
        max_background: u16 = 64, [1, 21_845];
        /// Deadline of one request handed from a FUSE thread to the runtime.
        reply_timeout_secs: u64 = 30, [1, 3600];
        /// Period of the sync of dirty files.
        dirty_sync_secs: u64 = 30, [1, 3600];
        /// How long the mount waits for the cluster at startup.
        cluster_ready_timeout_secs: u64 = 60, [1, 3600];
    }
}

impl crate::CrossCheck for FuseParams {
    fn cross_check(&self) -> Result<(), String> {
        if self.prefetch_window_bytes.get() < self.prefetch_block_bytes.get() {
            return Err(format!(
                "prefetch-window-bytes {} is below prefetch-block-bytes {}",
                self.prefetch_window_bytes.get(),
                self.prefetch_block_bytes.get()
            ));
        }
        Ok(())
    }
}
