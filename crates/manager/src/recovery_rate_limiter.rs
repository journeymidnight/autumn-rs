//! F211-H: in-memory rate limiter for the recovery dispatch loop.
//!
//! Pre-F211-H, a large node death (`fence_node` against an EN holding
//! thousands of extents) released a flood of recovery dispatches in one
//! `recovery_dispatch_loop` tick. Each dispatch fired a sustained
//! `COPY_EXTENT` / RS-decode against the source replicas and the target
//! EN — saturating both the inter-node network and the per-node IO
//! queue, and pushing client-facing read/write latency through the
//! roof. The limiter caps concurrent in-flight recoveries on three
//! axes simultaneously:
//!
//! - `max_global_concurrent` (default 64): cluster-wide ceiling. Keeps
//!   a single fence from monopolising bandwidth.
//! - `max_concurrent_per_source` (default 4): per-source-node ceiling.
//!   Without this, a single overloaded source EN serves K-shard reads
//!   for hundreds of concurrent EC-decode jobs.
//! - `max_concurrent_per_target` (default 2): per-target-node ceiling.
//!   Without this, a freshly-allocated empty target EN takes hundreds
//!   of simultaneous writes and exhausts disk IOPS.
//!
//! Counters are pure-counter — no persistence. Manager restart resets
//! them; the dispatch loop re-derives "who is recovering what" from
//! the F207 unified inflight ledger and reseeds the limiter on each
//! `acquire`. Worst case after a leader failover: the new leader runs
//! one tick at the unconstrained pre-F211-H concurrency until the next
//! reseed cycle. This is acceptable — the alternative (etcd-persisted
//! counters) would multiply manager etcd traffic per dispatch.

use std::collections::HashMap;

pub struct RecoveryRateLimiter {
    pub max_global: u32,
    pub max_per_source: u32,
    pub max_per_target: u32,
    pub global_inflight: u32,
    pub per_source: HashMap<u64, u32>,
    pub per_target: HashMap<u64, u32>,
    /// (extent_id, slot) → consecutive failure count for exponential
    /// backoff (F211-E). Cleared on a successful dispatch.
    pub backoff: HashMap<(u64, u32), BackoffState>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BackoffState {
    pub consecutive_failures: u32,
    /// Unix epoch seconds of the last failed attempt.
    pub last_attempt_at: i64,
}

impl Default for RecoveryRateLimiter {
    fn default() -> Self {
        Self::new(64, 4, 2)
    }
}

impl RecoveryRateLimiter {
    pub fn new(max_global: u32, max_per_source: u32, max_per_target: u32) -> Self {
        Self {
            max_global: max_global.max(1),
            max_per_source: max_per_source.max(1),
            max_per_target: max_per_target.max(1),
            global_inflight: 0,
            per_source: HashMap::new(),
            per_target: HashMap::new(),
            backoff: HashMap::new(),
        }
    }

    pub fn from_env() -> Self {
        let g = std::env::var("AUTUMN_MGR_RECOVERY_MAX_GLOBAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64u32);
        let s = std::env::var("AUTUMN_MGR_RECOVERY_MAX_PER_SOURCE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4u32);
        let t = std::env::var("AUTUMN_MGR_RECOVERY_MAX_PER_TARGET")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2u32);
        Self::new(g, s, t)
    }

    /// Try to reserve a dispatch slot for (source_node, target_node).
    /// Returns `true` and increments counters; `false` leaves all
    /// counters untouched.
    pub fn try_acquire(&mut self, source_node: u64, target_node: u64) -> bool {
        if self.global_inflight >= self.max_global {
            return false;
        }
        if *self.per_source.get(&source_node).unwrap_or(&0) >= self.max_per_source {
            return false;
        }
        if *self.per_target.get(&target_node).unwrap_or(&0) >= self.max_per_target {
            return false;
        }
        self.global_inflight += 1;
        *self.per_source.entry(source_node).or_insert(0) += 1;
        *self.per_target.entry(target_node).or_insert(0) += 1;
        true
    }

    /// Release a previously-acquired slot. No-op if counters are
    /// already at zero (defensive against double-release on the
    /// drain_extent_inflight_marker / apply_recovery_done path).
    pub fn release(&mut self, source_node: u64, target_node: u64) {
        if self.global_inflight > 0 {
            self.global_inflight -= 1;
        }
        if let Some(c) = self.per_source.get_mut(&source_node) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.per_source.remove(&source_node);
            }
        }
        if let Some(c) = self.per_target.get_mut(&target_node) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.per_target.remove(&target_node);
            }
        }
    }

    /// F224: clear the in-flight counters (global / per-source /
    /// per-target) WITHOUT touching backoff state. The dispatch loop
    /// calls this then reseeds from the inflight ledger each tick, so
    /// the counters always reflect actually-in-flight recoveries —
    /// `recovery-stats` reports real numbers and the per-candidate
    /// `try_acquire` gate enforces the caps. Pre-F224 `try_acquire`
    /// was never called in production, so `global` was stuck at 0 and
    /// the caps were unenforced (a big fence could still flood
    /// recoveries — the exact thing this limiter was added to prevent).
    pub fn reset_counts(&mut self) {
        self.global_inflight = 0;
        self.per_source.clear();
        self.per_target.clear();
    }

    /// F224: unconditionally count one in-flight recovery. Used only to
    /// reseed from the F207 ledger (the ledger is truth and may exceed
    /// the caps after a leader failover — reseeding must reflect
    /// reality, not clamp it; clamping is the job of `try_acquire` on
    /// NEW dispatches).
    pub fn seed_inflight(&mut self, source_node: u64, target_node: u64) {
        self.global_inflight += 1;
        *self.per_source.entry(source_node).or_insert(0) += 1;
        *self.per_target.entry(target_node).or_insert(0) += 1;
    }

    /// Record a failure for backoff. Returns the new
    /// `consecutive_failures` count.
    pub fn record_failure(&mut self, extent_id: u64, slot: u32, now_s: i64) -> u32 {
        let entry = self.backoff.entry((extent_id, slot)).or_default();
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        entry.last_attempt_at = now_s;
        entry.consecutive_failures
    }

    pub fn record_success(&mut self, extent_id: u64, slot: u32) {
        self.backoff.remove(&(extent_id, slot));
    }

    /// Returns `true` if this (extent, slot) is currently inside the
    /// backoff window — the dispatch loop should skip it this tick.
    pub fn in_backoff(&self, extent_id: u64, slot: u32, now_s: i64) -> bool {
        let Some(e) = self.backoff.get(&(extent_id, slot)) else {
            return false;
        };
        if e.consecutive_failures == 0 {
            return false;
        }
        let backoff_secs = Self::backoff_secs(e.consecutive_failures);
        now_s.saturating_sub(e.last_attempt_at) < backoff_secs
    }

    /// 2^N seconds, capped at 300 s (5 min). Caller invokes with
    /// the new `consecutive_failures` value (1..=u32::MAX).
    pub fn backoff_secs(consecutive_failures: u32) -> i64 {
        let shift = consecutive_failures.min(20);
        let secs: i64 = 1i64 << shift;
        secs.min(300)
    }

    pub fn snapshot(&self) -> (Vec<(u64, u32)>, Vec<(u64, u32)>) {
        let mut src: Vec<(u64, u32)> =
            self.per_source.iter().map(|(k, v)| (*k, *v)).collect();
        src.sort_by_key(|(id, _)| *id);
        let mut tgt: Vec<(u64, u32)> =
            self.per_target.iter().map(|(k, v)| (*k, *v)).collect();
        tgt.sort_by_key(|(id, _)| *id);
        (src, tgt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release_basic() {
        let mut l = RecoveryRateLimiter::new(10, 4, 2);
        assert!(l.try_acquire(1, 2));
        assert_eq!(l.global_inflight, 1);
        l.release(1, 2);
        assert_eq!(l.global_inflight, 0);
        assert!(l.per_source.is_empty());
        assert!(l.per_target.is_empty());
    }

    #[test]
    fn global_cap_enforced() {
        let mut l = RecoveryRateLimiter::new(2, 4, 4);
        assert!(l.try_acquire(1, 2));
        assert!(l.try_acquire(3, 4));
        assert!(!l.try_acquire(5, 6));
    }

    #[test]
    fn per_source_cap_enforced() {
        let mut l = RecoveryRateLimiter::new(10, 2, 4);
        assert!(l.try_acquire(1, 2));
        assert!(l.try_acquire(1, 3));
        // Third from source=1 must fail even though global has room.
        assert!(!l.try_acquire(1, 4));
        // A different source still works.
        assert!(l.try_acquire(7, 8));
    }

    #[test]
    fn per_target_cap_enforced() {
        let mut l = RecoveryRateLimiter::new(10, 4, 1);
        assert!(l.try_acquire(1, 2));
        // Second to target=2 must fail.
        assert!(!l.try_acquire(3, 2));
        // Different target works.
        assert!(l.try_acquire(3, 4));
    }

    #[test]
    fn backoff_increments_and_caps() {
        let mut l = RecoveryRateLimiter::default();
        assert_eq!(l.record_failure(7, 0, 100), 1);
        assert_eq!(l.record_failure(7, 0, 110), 2);
        assert_eq!(l.record_failure(7, 0, 120), 3);
        // 2^3 = 8 s, last_attempt_at = 120 → in backoff until 128.
        assert!(l.in_backoff(7, 0, 125));
        assert!(!l.in_backoff(7, 0, 200));
    }

    #[test]
    fn backoff_secs_caps_at_300() {
        assert_eq!(RecoveryRateLimiter::backoff_secs(1), 2);
        assert_eq!(RecoveryRateLimiter::backoff_secs(8), 256);
        assert_eq!(RecoveryRateLimiter::backoff_secs(9), 300);
        assert_eq!(RecoveryRateLimiter::backoff_secs(20), 300);
    }

    #[test]
    fn record_success_clears_backoff() {
        let mut l = RecoveryRateLimiter::default();
        l.record_failure(7, 0, 100);
        l.record_success(7, 0);
        assert!(!l.in_backoff(7, 0, 100));
    }
}
