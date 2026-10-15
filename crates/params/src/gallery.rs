//! gallery example parameters.

use crate::{GIB, MIB};

crate::params! {
    /// Every gallery parameter.
    pub struct GalleryParams("gallery") {
        // ── Process ──────────────────────────────────────────────────────
        /// Manager endpoints.
        manager: String = "127.0.0.1:9001".into();
        /// Namespace scope every key lives under.
        scope: String = "gallery".into();
        /// Credential file (`<principal>\n<hex>`, from `autumn-op
        /// principal-create`); needed when authz is on.
        credential_file: Option<std::path::PathBuf>;
        /// Bind host.
        listen: String = "0.0.0.0".into();
        /// Listen port.
        port: u16 = 5001;

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

