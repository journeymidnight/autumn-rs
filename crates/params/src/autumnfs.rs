//! autumnfs CLI parameters.

use crate::MIB;

crate::params! {
    /// Every autumnfs CLI tunable.
    pub struct AutumnfsParams("autumnfs") {
        /// Fewest extents a download fetches per request.
        get_window_min_extents: usize = 8, [1, 4096];
        /// RAM ceiling of one download's reused buffers.
        get_window_max_bytes: usize = 192 * MIB as usize, [8 * MIB as usize, 64 * 1024 * MIB as usize];
        /// Puts an upload keeps in flight for a non-striped file.
        put_depth: usize = 8, [1, 1024];
        /// Puts in flight per stripe lane of a striped file (clamped to
        /// [put-depth-striped-min, put-depth-striped-max] in total).
        put_depth_per_lane: usize = 12, [1, 1024];
        /// Fewest puts in flight for a striped file.
        put_depth_striped_min: usize = 24, [1, 4096];
        /// Most puts in flight for a striped file.
        put_depth_striped_max: usize = 96, [1, 4096];
    }
}

impl crate::CrossCheck for AutumnfsParams {
    fn cross_check(&self) -> Result<(), String> {
        if self.put_depth_striped_min.get() > self.put_depth_striped_max.get() {
            return Err("put-depth-striped-min exceeds put-depth-striped-max".into());
        }
        Ok(())
    }
}
