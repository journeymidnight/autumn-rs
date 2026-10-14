//! File-system layer parameters (`autumn_fs`), shared by autumn-fuse, autumnfs
//! and autumn-s3. One process-wide instance: `autumn_fs::params()`.

crate::params! {
    /// Every file-system layer tunable.
    pub struct FsParams("fs") {
        /// Extents buffered per writing inode before a flush (RAM per writing
        /// inode = this × 8 MiB).
        write_buf_extents: usize = 8, [1, 1024];
        /// Append flushes one inode keeps in flight (each pins a copy of the
        /// write buffer).
        append_inflight_depth: usize = 2, [1, 64];
        /// Inode numbers reserved per allocation from the manager.
        inode_alloc_batch: u64 = 1000, [1, 1_000_000];
        /// Segment-map pages a session caches before clearing them (256 pages
        /// of up to 1024 segments is about 10 MiB).
        segment_page_cache_cap: usize = 256, [1, 1_000_000];
        /// Data puts a segment stream keeps in flight.
        segment_stream_inflight: usize = 8, [1, 1024];
        /// Writer-lease heartbeat period (the manager's lease TTL is 30 s by
        /// default; keep a few beats inside it).
        lease_heartbeat_secs: u64 = 5, [1, 600];
        /// A completed multipart upload's record is kept this long.
        multipart_completed_retention_secs: u64 = 3600, [60, 30 * 86_400];
    }
}

impl crate::CrossCheck for FsParams {}
