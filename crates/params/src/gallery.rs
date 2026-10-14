//! gallery example parameters.

use crate::{GIB, MIB};

crate::params! {
    /// Every gallery tunable.
    pub struct GalleryParams("gallery") {
        /// Thumbnail width in pixels.
        thumb_width: u32 = 320, [16, 8192];
        /// Thumbnail JPEG quality.
        thumb_quality: u8 = 80, [1, 100];
        /// Chunk of a source video download before transcoding.
        range_chunk_bytes: u32 = 4 * MIB as u32, [64 * 1024, 64 * MIB as u32];
        /// Target payload of one HLS segment (keep it under the PS inline cap,
        /// 64 MiB).
        hls_segment_target_bytes: u64 = 48 * MIB, [MIB, 60 * MIB];
        /// Shortest HLS segment.
        hls_time_min_secs: u64 = 2, [1, 600];
        /// Longest HLS segment (also used when the source rate is unknown).
        hls_time_max_secs: u64 = 20, [1, 600];
        /// Largest upload accepted.
        upload_limit_bytes: u64 = 64 * GIB, [MIB, 1024 * GIB];
    }
}

impl crate::CrossCheck for GalleryParams {
    fn cross_check(&self) -> Result<(), String> {
        if self.hls_time_min_secs.get() > self.hls_time_max_secs.get() {
            return Err("hls-time-min-secs exceeds hls-time-max-secs".into());
        }
        Ok(())
    }
}
