//! G11 — clock skew / wall-clock time-jump vs TTL backstops.
//!
//! REPRODUCE-FIRST probe (do NOT fix; this file only asserts the
//! current behaviour). Investigates whether a skewed / jumped wall
//! clock on a manager can subvert the inode writer-lease TTL machinery
//! (`crates/manager/src/inode_lease.rs`) — the sharpest of the four
//! hypothesised wall-clock consumers because it gates fuse
//! single-writer exclusion.
//!
//! FINDING (asserted below):
//!   * The LIVE lease TTL path (`acquire` / `heartbeat` / `tick` /
//!     `tick_plan` / `release`) takes a MONOTONIC `Instant`, NOT a
//!     wall clock. `Instant` cannot be moved by NTP / settimeofday /
//!     a VM time-jump, so the live revoke schedule is SKEW-IMMUNE by
//!     construction — there is no seam to inject a wall-clock skew
//!     into it. (Demonstrated: revoke fires purely off the monotonic
//!     delta the caller passes.)
//!   * The ONLY wall-clock crossing is the failover rehydration pair
//!     `writer_record` (persist: monotonic remaining -> absolute unix
//!     `expires_at`) and `install_persisted_writer` (replay: unix
//!     `expires_at` -> monotonic deadline on the NEW leader's clock).
//!     That path is DEFENDED by `.min(self.lease_ttl)` +
//!     `(expires_at - now_epoch).max(0)`:
//!       - a FORWARD-skewed persisted deadline (old leader's clock ran
//!         ahead, or the new leader's clock is behind) can NEVER pin a
//!         writer beyond `lease_ttl` from the new leader's monotonic
//!         now — so it cannot hold a dead writer / starve a successor.
//!       - a BACKWARD-skewed rehydration (new leader's clock jumped
//!         forward, so `now_epoch > expires_at`) clamps remaining to 0
//!         => the writer is installed already-expired and revoked on
//!         the next tick. This is EARLY revoke = the standard
//!         lease-revoke contract (identical to a writer/manager
//!         partition); the writer learns via a `NotHeld` heartbeat.
//!   * NO double-writer is reachable: the manager writer slot is a
//!     single `Option<ClientKey>`; a second client is `WriteConflict`
//!     until the first is revoked (slot cleared), so the manager's
//!     authoritative state never holds two writers at once regardless
//!     of skew.
//!
//! Everything here drives the PUBLIC `LeaseRegistry` API directly and
//! CONSTRUCTS the skewed persisted timestamp (a far-future / past unix
//! `expires_at`) to simulate a skewed peer, per the reproduce-first
//! brief.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use autumn_manager::inode_lease::LeaseRegistry;
use autumn_rpc::manager_rpc::{MgrClientId, MgrInodeLeaseRecord, LEASE_MODE_WRITE};

const TTL: Duration = Duration::from_secs(30);

fn cid(kind: u8, n: u8, host: &str) -> MgrClientId {
    MgrClientId {
        kind,
        uuid: [n; 16],
        host: host.to_string(),
    }
}

fn epoch_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn rec(ino: u64, writer: &MgrClientId, version: u64, expires_at: i64) -> MgrInodeLeaseRecord {
    MgrInodeLeaseRecord {
        ino,
        writer: writer.clone(),
        version,
        expires_at,
    }
}

/// Baseline: the LIVE TTL revoke is a pure function of the MONOTONIC
/// `Instant` delta the caller passes. There is no wall-clock seam to
/// skew — this is the skew-immunity of the live path, made concrete.
#[test]
fn live_ttl_revoke_is_monotonic_only() {
    let mut r = LeaseRegistry::with_ttl(TTL);
    let w = cid(1, 1, "writer-a");
    let t0 = Instant::now();

    let out = r.acquire(&w, 42, LEASE_MODE_WRITE, t0);
    assert!(
        matches!(out, autumn_manager::inode_lease::AcquireOutcome::Granted { .. }),
        "writer granted, got {out:?}"
    );

    // Before TTL: not revoked. The function consumes only the Instant
    // delta; a hostile wall-clock value is never read here.
    assert!(
        r.tick_plan(t0 + Duration::from_secs(29)).is_empty(),
        "writer must survive before its monotonic TTL"
    );
    // After TTL: revoked. Purely (t - deadline) on the monotonic clock.
    let plans = r.tick_plan(t0 + Duration::from_secs(31));
    assert_eq!(plans.len(), 1, "writer revoked exactly at its monotonic TTL");
    assert_eq!(plans[0].ino, 42);
}

/// FORWARD-skewed persisted deadline: an old leader whose wall clock
/// ran far ahead (or a new leader whose clock is far behind) persists
/// `expires_at = now + ~27h`. Rehydrating it MUST clamp to
/// `<= now + lease_ttl` — else a dead writer would pin the inode for
/// hours and no successor could ever acquire.
///
/// This is the load-bearing skew defense (`.min(self.lease_ttl)`).
#[test]
fn forward_skew_far_future_deadline_is_clamped_to_ttl() {
    let mut r = LeaseRegistry::with_ttl(TTL);
    let w = cid(1, 7, "skewed-writer");

    // Simulate the skewed peer directly: 100_000 s (~27.7 h) in the future.
    let skewed = rec(9, &w, 5, epoch_now() + 100_000);
    let now = Instant::now();
    r.install_persisted_writer(skewed, now);

    // If the clamp were absent, the deadline would be ~now+100000s and
    // this tick (now+31s) would NOT revoke. With the clamp it is
    // now+30s, so it revokes.
    assert!(
        r.tick_plan(now + Duration::from_secs(29)).is_empty(),
        "clamped deadline is now+TTL, so still live at now+29s"
    );
    let plans = r.tick_plan(now + Duration::from_secs(31));
    assert_eq!(
        plans.len(),
        1,
        "forward-skewed far-future deadline MUST be clamped to <= now+TTL \
         (else a dead writer pins the inode for ~27h)"
    );
    assert_eq!(plans[0].ino, 9);
}

/// BACKWARD-skewed rehydration: the new leader's wall clock jumped
/// forward so `now_epoch > persisted expires_at`. `(expires_at -
/// now_epoch).max(0) == 0` => the writer is installed already-expired
/// and revoked on the very next tick (early revoke = normal lease
/// contract). No pin, no grant-to-nobody.
#[test]
fn backward_skew_past_deadline_installs_already_expired() {
    let mut r = LeaseRegistry::with_ttl(TTL);
    let w = cid(1, 8, "stale-writer");

    // Persisted deadline 100 s in the PAST (new leader's clock ahead).
    let stale = rec(11, &w, 3, epoch_now() - 100);
    let now = Instant::now();
    r.install_persisted_writer(stale, now);

    // remaining clamps to 0 => deadline == now => revoked at now.
    let plans = r.tick_plan(now);
    assert_eq!(
        plans.len(),
        1,
        "past deadline installs already-expired => revoked next tick (early revoke)"
    );
    assert_eq!(plans[0].ino, 11);
}

/// The anti-double-writer invariant, under a skewed rehydration: the
/// manager holds a SINGLE writer slot. After a skewed
/// `install_persisted_writer` for W1, a DIFFERENT client W2's
/// non-force write acquire is `WriteConflict` until W1 is revoked; a
/// force acquire opens a grace window rather than granting a second
/// concurrent writer. The manager's authoritative state NEVER holds
/// two writers at once — no skew value changes that.
#[test]
fn skewed_rehydrate_never_yields_two_live_writers() {
    use autumn_manager::inode_lease::AcquireOutcome;

    let mut r = LeaseRegistry::with_ttl(TTL);
    let w1 = cid(1, 1, "w1");
    let w2 = cid(1, 2, "w2");

    // W1 rehydrated from a FORWARD-skewed record (far-future deadline).
    r.install_persisted_writer(rec(21, &w1, 4, epoch_now() + 100_000), Instant::now());
    let now = Instant::now();

    // W2 non-force write acquire -> WriteConflict (single-slot).
    match r.acquire(&w2, 21, LEASE_MODE_WRITE, now) {
        AcquireOutcome::WriteConflict { .. } => {}
        other => panic!("expected WriteConflict while W1 holds the slot, got {other:?}"),
    }

    // W2 force acquire -> a grace window (RevokePending), NOT an
    // immediate second grant. Two writers are never live at once.
    match r.acquire_with_force(&w2, 21, LEASE_MODE_WRITE, true, now) {
        AcquireOutcome::RevokePending { .. } => {}
        AcquireOutcome::Granted { .. } => {
            panic!("force acquire must NOT grant a 2nd concurrent writer immediately")
        }
        other => panic!("expected RevokePending, got {other:?}"),
    }

    // Only after W1 is revoked (its clamped TTL elapses) does the slot
    // free for a successor. This proves the clamp bounds the handoff
    // to <= TTL rather than the ~27h skewed deadline.
    let plans = r.tick_plan(now + Duration::from_secs(31));
    assert_eq!(plans.len(), 1, "W1 revoked within TTL, freeing the slot");
    assert_eq!(plans[0].ino, 21);
}

/// End-to-end persist -> rehydrate cycle: `writer_record` renders the
/// live monotonic remaining into an absolute unix `expires_at`, then
/// `install_persisted_writer` on a FRESH registry (new leader)
/// re-derives a monotonic deadline. The rehydrated deadline is bounded
/// by `now + lease_ttl` even though the wall-clock round-trip crossed
/// the skewable boundary. (Here with no injected skew: the healthy
/// round-trip preserves ~TTL, and the clamp is the ceiling.)
#[test]
fn writer_record_roundtrip_deadline_bounded_by_ttl() {
    let mut src = LeaseRegistry::with_ttl(TTL);
    let w = cid(1, 9, "rt-writer");
    src.acquire(&w, 33, LEASE_MODE_WRITE, Instant::now());

    let record = src
        .writer_record(33)
        .expect("writer_record for a held writer");
    assert_eq!(record.ino, 33);

    // Feed the persisted record into a new leader.
    let mut dst = LeaseRegistry::with_ttl(TTL);
    let now = Instant::now();
    dst.install_persisted_writer(record, now);

    // Bounded above by now+TTL: not live past the TTL horizon.
    let plans = dst.tick_plan(now + Duration::from_secs(31));
    assert_eq!(
        plans.len(),
        1,
        "rehydrated deadline is bounded by now+TTL across the persist/replay cycle"
    );
}
