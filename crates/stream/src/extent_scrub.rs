//! Pacing for the scrub (`extent_node/scrub.rs`).
//!
//! A scrub reads every byte it checks from the same disks live traffic uses,
//! so it is paced by BYTES and the pace lives on the node that does the
//! reading: the requester only names what to check, and cannot know how busy
//! this node's disks are.
//!
//! Pure, so the pacing rule is testable without a disk or a clock.

use std::time::{Duration, Instant};

/// How much a scrub may read per second, PER SHARD, unless the node is started
/// with another figure (`--scrub-bytes-per-sec`).
///
/// Per shard, not per node: each `ExtentNode` runs its own scrub worker, so a
/// node with N shards reads at up to N times this.
pub const SCRUB_DEFAULT_BYTES_PER_SEC: u64 = 8 * 1024 * 1024;

/// The most a pacer lets a reader run ahead of its rate after being idle.
///
/// An idle stretch must not bank credit for a burst against live reads — the
/// point is a ceiling on interference, not a throughput guarantee.
const MAX_IDLE_CREDIT: Duration = Duration::from_secs(1);

/// Keeps a stream of reads at `bytes_per_sec`.
#[derive(Debug)]
pub(crate) struct ScrubPacer {
    bytes_per_sec: u64,
    /// When the current run of reads started, and how many bytes it has read.
    epoch: Option<(Instant, u64)>,
}

impl ScrubPacer {
    /// `0` means unpaced.
    pub(crate) fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            epoch: None,
        }
    }

    /// Account for a read of `bytes` about to happen at `now`, returning how
    /// long to wait first so the run stays at the rate.
    ///
    /// The run restarts when the reader has fallen more than
    /// `MAX_IDLE_CREDIT` behind its schedule, so idle time is never paid back
    /// as a burst.
    pub(crate) fn pace(&mut self, now: Instant, bytes: u64) -> Duration {
        if self.bytes_per_sec == 0 {
            return Duration::ZERO;
        }
        let (start, done) = match self.epoch {
            Some((start, done)) if self.due(start, done) + MAX_IDLE_CREDIT >= now => {
                (start, done)
            }
            _ => (now, 0),
        };
        let wait = self.due(start, done).saturating_duration_since(now);
        self.epoch = Some((start, done + bytes));
        wait
    }

    /// When a run that started at `start` may have read `done` bytes.
    fn due(&self, start: Instant, done: u64) -> Instant {
        start + Duration::from_secs_f64(done as f64 / self.bytes_per_sec as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    /// At 8 MiB/s a run of 1 MiB reads waits 125 ms per read after the first.
    #[test]
    fn a_run_of_reads_is_held_to_the_rate() {
        let mut p = ScrubPacer::new(8 * MIB);
        let t0 = Instant::now();
        assert_eq!(p.pace(t0, MIB), Duration::ZERO, "the first read goes at once");
        assert_eq!(p.pace(t0, MIB), Duration::from_millis(125));
        assert_eq!(p.pace(t0 + Duration::from_millis(125), MIB), Duration::from_millis(125));
        // Reading slower than the rate costs no wait.
        assert_eq!(p.pace(t0 + Duration::from_millis(400), MIB), Duration::ZERO);
    }

    /// After an idle minute the next read is not entitled to a minute's worth
    /// of reads back to back.
    #[test]
    fn idle_time_banks_no_burst() {
        let mut p = ScrubPacer::new(8 * MIB);
        let t0 = Instant::now();
        p.pace(t0, MIB);
        let later = t0 + Duration::from_secs(60);
        assert_eq!(p.pace(later, MIB), Duration::ZERO);
        assert_eq!(p.pace(later, MIB), Duration::from_millis(125), "paced again at once");
    }

    #[test]
    fn zero_means_unpaced() {
        let mut p = ScrubPacer::new(0);
        let t0 = Instant::now();
        for _ in 0..10 {
            assert_eq!(p.pace(t0, 64 * MIB), Duration::ZERO);
        }
    }
}
