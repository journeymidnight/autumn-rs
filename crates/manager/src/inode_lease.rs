//! F-ioring-lease-1 — JuiceFS-style inode-level lease state machine.
//!
//! Single writer + many readers per inode. Writer-close bumps the
//! `version` (close-to-open coherence marker) and pushes
//! `InvalidateInode { kind = WriterClosed, new_version }` to every
//! reader currently subscribed to the inode. Reader caches keyed by
//! `(ino, version)` drop on mismatch (plan §3.1 / §4.4 / §6).
//!
//! Only the writer lease is persisted to etcd
//! (`inode_leases/<ino>`); reader leases live in memory only. The
//! plan's rationale (§6 / §7 "lease 数量爆炸"): a reader subscribing
//! again after a manager failover invalidates all of its cache up
//! front, so losing the reader set on failover is benign.
//!
//! Time policy: the manager passes monotonic `Instant`s to mutating
//! ops so unit tests can drive the clock without `sleep`. The TTL
//! revoke loop in `lib.rs` calls `tick(now)` once per second.
//!
//! Layer rules — every change here must respect:
//! - Single-threaded compio (Rc/RefCell, !Send).
//! - F149 leader fence on every etcd write (handlers call
//!   `put_msgs_txn` / `put_and_delete_txn` which carry the fence).
//! - Plan §6 invariants (manager is the only lease decision-maker;
//!   writer Release happens AFTER flush; cache is version-tagged on
//!   the CLIENT; subscribe-drop invalidates everything).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::manager_rpc::{
    MgrClientId, MgrInodeLeaseRecord, MgrInvalidation,
    LEASE_INVAL_LEASE_REVOKED, LEASE_INVAL_WILL_REVOKE_IN, LEASE_INVAL_WRITER_CLOSED,
    LEASE_MODE_READ, LEASE_MODE_WRITE,
};

/// Default writer-lease TTL (seconds). Same magnitude as
/// `acquire_owner_lock`'s etcd lease (plan §3.1). Clients must
/// heartbeat at <= TTL / 6 (`5s` for the 30s default) to stay alive.
pub const DEFAULT_LEASE_TTL_SECS: u32 = 30;

/// F-lease-preempt: default grace window the manager gives a writer
/// to flush + voluntarily release after a peer's
/// `AcquireLease(force=true)`. Matches plan §5 Phase 3 "writer
/// revoke 协议: WillRevokeIn { 5s }". Tunable per-registry via
/// `LeaseRegistry::with_ttl_and_revoke_grace`.
pub const DEFAULT_REVOKE_GRACE: Duration = Duration::from_secs(5);

/// How often the revoke loop ticks. Picked under the default TTL so a
/// revoke fires within ~1 s of the deadline.
pub const REVOKE_TICK: Duration = Duration::from_secs(1);

/// Max events buffered per client inbox before the oldest is dropped.
/// On overflow the manager logs WARN; the client's
/// "subscribe-disconnect = invalidate everything" semantics then
/// covers the gap on the next reconnect.
pub const MAX_INBOX_EVENTS: usize = 1024;

/// Stable in-memory key for a client. Equivalent to the wire
/// `MgrClientId` but with the diagnostic `host` field stripped so two
/// processes that report different hostnames for the same UUID still
/// hash to the same entry (plan §3.2: "host 诊断用").
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientKey {
    pub kind: u8,
    pub uuid: [u8; 16],
}

impl ClientKey {
    pub fn from_wire(c: &MgrClientId) -> Self {
        ClientKey {
            kind: c.kind,
            uuid: c.uuid,
        }
    }
}

/// One inode's lease state. Single writer XOR many readers
/// concurrently; readers may coexist with the writer (reads through
/// an open file remain legal — the writer's flush-before-close
/// ordering, plan §6.2, keeps coherence intact).
#[derive(Clone, Debug)]
pub struct InodeLeaseState {
    pub ino: u64,
    pub writer: Option<ClientKey>,
    pub writer_diag_host: String,
    pub writer_expires_at: Option<Instant>,
    pub readers: BTreeMap<ClientKey, Instant>,
    pub version: u64,
    /// F-lease-preempt: deadline after which a force-acquire that's
    /// already pushed `WillRevokeIn` to the current writer will be
    /// allowed to force-revoke. `Some(t)` means "grace period
    /// running until t"; `None` means "no preempt in flight."
    /// Cleared on release / writer-revoke / TTL expiry so a future
    /// force-acquire starts a fresh grace window.
    pub pending_revoke_at: Option<Instant>,
}

impl InodeLeaseState {
    fn new(ino: u64) -> Self {
        InodeLeaseState {
            ino,
            writer: None,
            writer_diag_host: String::new(),
            writer_expires_at: None,
            readers: BTreeMap::new(),
            version: 1,
            pending_revoke_at: None,
        }
    }
}

/// Outcome of `acquire`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    /// Lease granted (refresh case included). Carries the post-call
    /// state snapshot.
    Granted {
        version: u64,
        writer_present: bool,
        ttl_secs: u32,
    },
    /// Rejected because another client holds the writer lease.
    WriteConflict {
        held_by_kind: u8,
        held_by_host: String,
    },
    /// F-lease-preempt: `force=true` against a held writer; manager
    /// started the grace window. Client should retry after
    /// `eta_ms`; on retry it either gets `Granted` (writer
    /// released voluntarily) or — past the deadline — the manager
    /// force-revokes the writer and grants.
    RevokePending {
        eta_ms: u32,
        held_by_kind: u8,
        held_by_host: String,
    },
    /// Rejected because the mode byte is invalid.
    InvalidMode,
}

/// Outcome of `release`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The writer released; `new_version` is the post-bump generation.
    WriterClosed { new_version: u64 },
    /// A reader released. Version unchanged.
    ReaderReleased,
    /// The client did not hold any lease on this inode (idempotent).
    NotHeld,
}

/// Outcome of `heartbeat`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatOutcome {
    Renewed {
        version: u64,
        writer_present: bool,
        ttl_secs: u32,
    },
    /// Client doesn't currently hold any lease on this inode. Client
    /// must drop its cache and re-acquire.
    NotHeld,
}

/// One reason an inode entry got an invalidation push.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationReason {
    /// Writer ran ReleaseLease (or wrote+closed). Readers cache must
    /// be re-validated.
    WriterClosed,
    /// Manager TTL revoked the writer (or admin took it away).
    LeaseRevoked,
    /// F-lease-preempt: pushed to the CURRENT writer when another
    /// client force-acquires. Writer should flush + voluntarily
    /// `ReleaseLease` within the grace window or the manager
    /// will force-revoke (which then pushes `LeaseRevoked`).
    WillRevokeIn,
}

impl InvalidationReason {
    pub fn wire_kind(self) -> u8 {
        match self {
            InvalidationReason::WriterClosed => LEASE_INVAL_WRITER_CLOSED,
            InvalidationReason::LeaseRevoked => LEASE_INVAL_LEASE_REVOKED,
            InvalidationReason::WillRevokeIn => LEASE_INVAL_WILL_REVOKE_IN,
        }
    }
}

/// Per-client inbox.
///
/// F-ioring-lease-3: extended with a parked `waker` so the
/// `MSG_POLL_INVALIDATIONS` handler can register a long-poll. A
/// subsequent `push_invalidation` fires the waker → the handler's
/// `recv` resolves → it returns the queued events without burning a
/// round-trip per second.
#[derive(Default, Debug)]
pub struct ClientInbox {
    pub events: VecDeque<MgrInvalidation>,
    pub overflowed: bool,
    /// Most recent long-poll waker. `None` when no client is parked
    /// (events are queued normally and the next poll drains them).
    /// At most one parked waker per client — the per-session client_id
    /// is single-threaded (one ps-conn long-poll task), so a stale
    /// parked waker would mean the previous poll's task was cancelled
    /// or its RPC connection dropped. The push path drops + replaces
    /// transparently in either case.
    pub waker: Option<futures::channel::oneshot::Sender<()>>,
}

impl ClientInbox {
    fn push(&mut self, ev: MgrInvalidation) {
        if self.events.len() >= MAX_INBOX_EVENTS {
            // Drop oldest; reader's reconnect path catches the gap.
            self.events.pop_front();
            self.overflowed = true;
        }
        self.events.push_back(ev);
        // F-ioring-lease-3: wake any parked long-poll. `send` on a
        // dropped receiver is a no-op — happens when the client's
        // RPC connection closed before the push; the next poll's
        // reconnect refreshes the waker.
        if let Some(w) = self.waker.take() {
            let _ = w.send(());
        }
    }
}

/// BUG-LEASE-4 (P1 #4, 2026-06-06) — staging area for
/// `acquire_with_force_deferred`'s invalidation pushes. The
/// production handler (`handle_acquire_lease`) does:
///
///   1. `let pushes = DeferredPushes::default();`
///   2. `let out = reg.acquire_with_force_deferred(.., &mut pushes);`
///      — applies in-memory mutation (writer slot, version bump,
///      pending_revoke_at), STAGES invalidation events in `pushes`.
///   3. Etcd persist.
///   4a. On etcd OK: `reg.flush_deferred_pushes(pushes)` — fans out
///      to client inboxes, wakes parked long-polls.
///   4b. On etcd FAIL: drop `pushes` (clients never see the events),
///      then `reg.revert_writer_acquire(ino, snapshot)` to rewind
///      the in-memory mutation INCLUDING the force-revoke's
///      version bump (BUG-LEASE-4 — the pre-fix revert restored
///      writer/host/deadline but left version bumped, so a future
///      Open got a lease_epoch that didn't correspond to any
///      persisted state).
///
/// Items are pre-rendered as (target, MgrInvalidation) tuples so the
/// flush phase doesn't need to re-walk the inode entry's readers
/// (which may have changed between stage and flush).
#[derive(Default, Debug)]
pub struct DeferredPushes {
    items: Vec<(ClientKey, MgrInvalidation)>,
}

impl DeferredPushes {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    fn stage(&mut self, target: ClientKey, ino: u64, version: u64, reason: InvalidationReason) {
        self.items.push((
            target,
            MgrInvalidation {
                ino,
                version,
                kind: reason.wire_kind(),
            },
        ));
    }
}

/// BUG-LEASE-5 (P1 #5, 2026-06-06) — plan returned by
/// `tick_plan` for the 2PC TTL-revoke loop.
///
/// `ino`: the writer-held inode whose TTL has elapsed.
/// `new_version`: the version the in-memory state WILL bump to in
///   `tick_commit_revokes`. The caller does NOT use this for any
///   etcd key (etcd delete is keyed by `ino` alone); it's exposed
///   for diagnostics + the manager log.
/// `readers`: snapshot of reader ClientKeys captured BEFORE the
///   commit phase's reader expiry sweep. Each reader gets a
///   `LeaseRevoked` push on commit even if their own lease expired
///   in the same tick — the L4 ordering invariant.
#[derive(Debug, Clone)]
pub struct RevokePlan {
    pub ino: u64,
    pub new_version: u64,
    pub readers: Vec<ClientKey>,
}

/// Manager-side lease registry. Single-threaded; wrap in
/// `Rc<RefCell<…>>` for AutumnManager.
#[derive(Default)]
pub struct LeaseRegistry {
    pub inodes: HashMap<u64, InodeLeaseState>,
    pub inboxes: HashMap<ClientKey, ClientInbox>,
    /// MONOTONIC version shadow for inodes that have ever held a
    /// writer. Persists across the (writer-release → empty → re-
    /// created) cycle so a future Open never sees an `(ino, version)`
    /// pair that an earlier reader's cache had already tagged. Without
    /// this, removing the empty inode + re-creating on the next
    /// acquire would reset `version` to 1 and break close-to-open
    /// coherence for any client whose cache still carried version=1
    /// from a different generation.
    pub last_version: HashMap<u64, u64>,
    pub lease_ttl: Duration,
    /// F-lease-preempt: how long the manager waits between pushing
    /// `WillRevokeIn` to a writer and force-revoking on a subsequent
    /// `AcquireLease(force=true)` retry.
    pub revoke_grace: Duration,
}

impl LeaseRegistry {
    pub fn with_ttl(ttl: Duration) -> Self {
        LeaseRegistry {
            inodes: HashMap::new(),
            inboxes: HashMap::new(),
            last_version: HashMap::new(),
            lease_ttl: ttl,
            revoke_grace: DEFAULT_REVOKE_GRACE,
        }
    }

    /// F-lease-preempt: registry constructor with both TTL and
    /// revoke-grace overrides. Used by tests that want a sub-second
    /// grace window to keep wall-clock test runtime low.
    pub fn with_ttl_and_revoke_grace(ttl: Duration, revoke_grace: Duration) -> Self {
        LeaseRegistry {
            inodes: HashMap::new(),
            inboxes: HashMap::new(),
            last_version: HashMap::new(),
            lease_ttl: ttl,
            revoke_grace,
        }
    }

    pub fn ttl_secs(&self) -> u32 {
        self.lease_ttl.as_secs() as u32
    }

    fn inode_or_create(&mut self, ino: u64) -> &mut InodeLeaseState {
        let seed = self.last_version.get(&ino).copied().unwrap_or(0);
        self.inodes.entry(ino).or_insert_with(|| {
            let mut s = InodeLeaseState::new(ino);
            // If the inode has ever been seen, resume from the highest
            // version we ever handed out — never go backwards.
            if seed >= s.version {
                s.version = seed;
            }
            s
        })
    }

    /// Record `version` as the high-water mark for `ino` so a future
    /// re-creation resumes monotonically. Call after every place we
    /// bump `state.version` or remove an inode entry.
    fn remember_version(&mut self, ino: u64, version: u64) {
        let slot = self.last_version.entry(ino).or_insert(0);
        if version > *slot {
            *slot = version;
        }
    }

    /// `mode = LEASE_MODE_READ` or `LEASE_MODE_WRITE`. `now` is the
    /// monotonic clock the manager owns; tests pass a synthetic
    /// `Instant`. `force = false` (this method) is the non-preempt
    /// path — see `acquire_with_force` for the F-lease-preempt
    /// variant.
    pub fn acquire(
        &mut self,
        client: &MgrClientId,
        ino: u64,
        mode: u8,
        now: Instant,
    ) -> AcquireOutcome {
        self.acquire_with_force(client, ino, mode, false, now)
    }

    /// F-lease-preempt: `acquire` with the `force` flag.
    ///
    /// - `force = false`: identical to the legacy `acquire`.
    /// - `force = true` AND no current writer (or writer is the
    ///   requester): same as `force=false` (no-op preempt).
    /// - `force = true` AND a DIFFERENT writer is currently held:
    ///   - first call: pushes `WillRevokeIn` to the current
    ///     writer, sets `pending_revoke_at = now + revoke_grace`,
    ///     returns `RevokePending { eta_ms }`.
    ///   - subsequent call BEFORE `pending_revoke_at`: returns
    ///     `RevokePending { eta_ms = remaining }`. The writer
    ///     may release voluntarily before the deadline — the
    ///     next non-force OR force acquire is then granted
    ///     normally.
    ///   - subsequent call AT/AFTER `pending_revoke_at`:
    ///     force-revokes (clears writer, bumps version, pushes
    ///     `LeaseRevoked` to readers + the deposed writer),
    ///     then grants the requester.
    /// BUG-LEASE-4 (P1 #4, coco arch review 2026-06-05) — non-deferred
    /// wrapper kept for unit tests + callers that don't need 2PC. Pushes
    /// invalidations directly into client inboxes; equivalent to
    /// `acquire_with_force_deferred` + immediate `flush_deferred_pushes`.
    ///
    /// **Production handlers (`handle_acquire_lease`) MUST use the
    /// deferred form** so an etcd-persist failure can roll back without
    /// leaving phantom LeaseRevoked / WillRevokeIn events in client
    /// inboxes that no longer correspond to a force-revoke that actually
    /// happened. See `DeferredPushes` doc.
    pub fn acquire_with_force(
        &mut self,
        client: &MgrClientId,
        ino: u64,
        mode: u8,
        force: bool,
        now: Instant,
    ) -> AcquireOutcome {
        let mut pushes = DeferredPushes::default();
        let out = self.acquire_with_force_deferred(client, ino, mode, force, now, &mut pushes);
        self.flush_deferred_pushes(pushes);
        out
    }

    /// BUG-LEASE-4 (P1 #4, 2026-06-06) — two-phase form of
    /// `acquire_with_force`. The in-memory mutation (writer slot,
    /// version bump, pending_revoke_at) is applied immediately, but
    /// the LeaseRevoked / WillRevokeIn pushes are STAGED in
    /// `pushes` rather than landing in client inboxes. The caller
    /// then:
    ///   - on etcd commit success: `flush_deferred_pushes(pushes)`
    ///     fans the staged events into the inboxes;
    ///   - on etcd persist failure: drops `pushes` (so clients
    ///     never see the events) AND calls `revert_writer_acquire`
    ///     to rewind the in-memory state — including the
    ///     force-revoke's version bump, courtesy of BUG-LEASE-4's
    ///     extended snapshot restore.
    /// This is the only correct shape for the "force-revoke +
    /// persist failure" race: pre-BUG-LEASE-4 the deposed writer
    /// and current readers saw `LeaseRevoked` events for a
    /// preemption that never actually persisted, and a subsequent
    /// leader failover replayed the OLD writer's etcd record →
    /// "phantom revoke."
    pub fn acquire_with_force_deferred(
        &mut self,
        client: &MgrClientId,
        ino: u64,
        mode: u8,
        force: bool,
        now: Instant,
        pushes: &mut DeferredPushes,
    ) -> AcquireOutcome {
        if mode != LEASE_MODE_READ && mode != LEASE_MODE_WRITE {
            return AcquireOutcome::InvalidMode;
        }
        let ttl = self.lease_ttl;
        let ttl_secs = self.ttl_secs();
        let revoke_grace = self.revoke_grace;
        let me = ClientKey::from_wire(client);

        // Preempt-handling state captured before mutating so we can
        // push invalidations after releasing the &mut borrow.
        let mut deposed_writer: Option<(ClientKey, u64)> = None;
        let mut will_revoke_push: Option<(ClientKey, u64, u32)> = None;
        // Whether to grant after the borrow ends.
        let grant_outcome: AcquireOutcome;

        {
            let state = self.inode_or_create(ino);
            match mode {
                LEASE_MODE_WRITE => {
                    let other_writer = match &state.writer {
                        Some(existing) if existing != &me => Some(existing.clone()),
                        _ => None,
                    };
                    if let Some(other) = other_writer {
                        // Another writer holds the slot.
                        if !force {
                            return AcquireOutcome::WriteConflict {
                                held_by_kind: other.kind,
                                held_by_host: state.writer_diag_host.clone(),
                            };
                        }
                        // F-lease-preempt: force-acquire path.
                        match state.pending_revoke_at {
                            None => {
                                // Start the grace window. Push
                                // WillRevokeIn to the current
                                // writer; return RevokePending.
                                let deadline = now + revoke_grace;
                                state.pending_revoke_at = Some(deadline);
                                let grace_ms = revoke_grace.as_millis() as u32;
                                will_revoke_push =
                                    Some((other.clone(), ino, grace_ms));
                                grant_outcome = AcquireOutcome::RevokePending {
                                    eta_ms: grace_ms,
                                    held_by_kind: other.kind,
                                    held_by_host: state.writer_diag_host.clone(),
                                };
                            }
                            Some(deadline) if deadline > now => {
                                // Still waiting; tell the caller
                                // how long.
                                let remaining = deadline
                                    .saturating_duration_since(now)
                                    .as_millis() as u32;
                                grant_outcome = AcquireOutcome::RevokePending {
                                    eta_ms: remaining,
                                    held_by_kind: other.kind,
                                    held_by_host: state.writer_diag_host.clone(),
                                };
                            }
                            Some(_) => {
                                // Grace expired → force-revoke.
                                state.writer = None;
                                state.writer_diag_host.clear();
                                state.writer_expires_at = None;
                                state.pending_revoke_at = None;
                                state.version = state.version.wrapping_add(1);
                                deposed_writer = Some((other, state.version));
                                // Now grant the WRITE to the new client.
                                state.writer = Some(me.clone());
                                state.writer_diag_host = client.host.clone();
                                state.writer_expires_at = Some(now + ttl);
                                grant_outcome = AcquireOutcome::Granted {
                                    version: state.version,
                                    writer_present: true,
                                    ttl_secs,
                                };
                            }
                        }
                    } else {
                        // Same writer re-acquiring OR no writer:
                        // grant (idempotent for same writer).
                        state.writer = Some(me.clone());
                        state.writer_diag_host = client.host.clone();
                        state.writer_expires_at = Some(now + ttl);
                        // A successful WRITE acquire clears any
                        // half-progressed preempt for this ino.
                        state.pending_revoke_at = None;
                        grant_outcome = AcquireOutcome::Granted {
                            version: state.version,
                            writer_present: true,
                            ttl_secs,
                        };
                    }
                }
                LEASE_MODE_READ => {
                    state.readers.insert(me.clone(), now + ttl);
                    grant_outcome = AcquireOutcome::Granted {
                        version: state.version,
                        writer_present: state.writer.is_some(),
                        ttl_secs,
                    };
                }
                _ => unreachable!(),
            }
        }

        // After the &mut borrow ends, STAGE the F-lease-preempt
        // invalidations into `pushes` (BUG-LEASE-4: not into
        // self.inboxes — the caller flushes after etcd commit, or
        // drops on etcd-fail).
        self.inboxes.entry(ClientKey::from_wire(client)).or_default();
        if let Some((target, ino_for_push, grace_ms)) = will_revoke_push {
            // The `version` field of the MgrInvalidation carries
            // the grace milliseconds — clients interpret per the
            // `LEASE_INVAL_WILL_REVOKE_IN` contract documented on
            // the constant.
            pushes.stage(
                target,
                ino_for_push,
                grace_ms as u64,
                InvalidationReason::WillRevokeIn,
            );
        }
        if let Some((deposed, new_version)) = deposed_writer {
            // Forcibly-revoked writer + every current reader gets
            // a `LeaseRevoked` push so their caches re-validate.
            let readers: Vec<ClientKey> = self
                .inodes
                .get(&ino)
                .map(|s| s.readers.keys().cloned().collect())
                .unwrap_or_default();
            pushes.stage(
                deposed,
                ino,
                new_version,
                InvalidationReason::LeaseRevoked,
            );
            for r in readers {
                pushes.stage(
                    r,
                    ino,
                    new_version,
                    InvalidationReason::LeaseRevoked,
                );
            }
            self.remember_version(ino, new_version);
        }
        if let AcquireOutcome::Granted { version, .. } = grant_outcome {
            self.remember_version(ino, version);
        }
        grant_outcome
    }

    /// BUG-LEASE-4 (P1 #4, 2026-06-06) — caller-side commit of the
    /// staged invalidations from `acquire_with_force_deferred`.
    /// Idempotent: calling with an empty bundle is a noop.
    pub fn flush_deferred_pushes(&mut self, pushes: DeferredPushes) {
        for (target, ev) in pushes.items {
            let inbox = self.inboxes.entry(target).or_default();
            inbox.push(ev);
        }
    }

    /// F-ioring-lease-5 (coco P2 #7): read-only "what would `release`
    /// do" predicate. Lets the handler order the etcd delete BEFORE
    /// the in-memory mutate so a failed etcd-delete doesn't leave a
    /// stale `inode_leases/<ino>` record (which would re-install
    /// the released writer on the next leader failover).
    ///
    /// Note: the returned `new_version` is the predicted post-release
    /// generation; the actual `release()` call may produce a
    /// different `ReleaseOutcome` if state mutated between the
    /// preview and the commit (e.g. TTL revoke). Handler treats a
    /// post-etcd `NotHeld` as benign (idempotent release).
    pub fn preview_release(&self, client: &MgrClientId, ino: u64) -> ReleaseOutcome {
        let me = ClientKey::from_wire(client);
        let Some(state) = self.inodes.get(&ino) else {
            return ReleaseOutcome::NotHeld;
        };
        if state.writer.as_ref() == Some(&me) {
            return ReleaseOutcome::WriterClosed {
                new_version: state.version.wrapping_add(1),
            };
        }
        if state.readers.contains_key(&me) {
            return ReleaseOutcome::ReaderReleased;
        }
        ReleaseOutcome::NotHeld
    }

    /// `release` returns whether a writer-close fired so the caller
    /// can persist the etcd delete + push invalidations.
    pub fn release(&mut self, client: &MgrClientId, ino: u64) -> ReleaseOutcome {
        let me = ClientKey::from_wire(client);
        let Some(state) = self.inodes.get_mut(&ino) else {
            return ReleaseOutcome::NotHeld;
        };

        if state.writer.as_ref() == Some(&me) {
            state.writer = None;
            state.writer_diag_host.clear();
            state.writer_expires_at = None;
            // F-lease-preempt: voluntary release clears any
            // pending revoke window — the next force-acquire
            // will start a fresh one if it lands on a future
            // writer.
            state.pending_revoke_at = None;
            state.version = state.version.wrapping_add(1);
            let new_version = state.version;
            // Snapshot reader set so we can push invalidations after
            // the borrow ends.
            let readers: Vec<ClientKey> = state.readers.keys().cloned().collect();
            // Avoid empty entries leaking memory.
            let inode_clean = state.writer.is_none() && state.readers.is_empty();
            let ino_copy = state.ino;
            if inode_clean {
                self.inodes.remove(&ino);
            }
            // Always record the high-water mark so a future re-
            // creation resumes from `new_version + 1` rather than 1.
            self.remember_version(ino_copy, new_version);
            for r in readers {
                self.push_invalidation(&r, ino_copy, new_version, InvalidationReason::WriterClosed);
            }
            return ReleaseOutcome::WriterClosed { new_version };
        }

        if state.readers.remove(&me).is_some() {
            let inode_clean = state.writer.is_none() && state.readers.is_empty();
            if inode_clean {
                self.inodes.remove(&ino);
            }
            return ReleaseOutcome::ReaderReleased;
        }

        ReleaseOutcome::NotHeld
    }

    pub fn heartbeat(
        &mut self,
        client: &MgrClientId,
        ino: u64,
        now: Instant,
    ) -> HeartbeatOutcome {
        let me = ClientKey::from_wire(client);
        let ttl = self.lease_ttl;
        let ttl_secs = self.ttl_secs();
        let Some(state) = self.inodes.get_mut(&ino) else {
            return HeartbeatOutcome::NotHeld;
        };

        if state.writer.as_ref() == Some(&me) {
            state.writer_expires_at = Some(now + ttl);
            return HeartbeatOutcome::Renewed {
                version: state.version,
                writer_present: true,
                ttl_secs,
            };
        }
        if let Some(deadline) = state.readers.get_mut(&me) {
            *deadline = now + ttl;
            return HeartbeatOutcome::Renewed {
                version: state.version,
                writer_present: state.writer.is_some(),
                ttl_secs,
            };
        }
        HeartbeatOutcome::NotHeld
    }

    // ── BUG-LEASE-4 (P1 #4, 2026-06-06) — deferred-push 2PC ────
    // (see `DeferredPushes` below for the public type;
    //  `acquire_with_force_deferred` / `flush_deferred_pushes` /
    //  `revert_writer_acquire` cooperate to ensure no phantom
    //  LeaseRevoked / WillRevokeIn events land in client inboxes
    //  when the manager's etcd persist fails.)

    /// Drain a client's invalidation queue. Returns the (possibly
    /// empty) batch. `overflowed=true` if any events were dropped due
    /// to overflow since the last poll — F-ioring-lease-3's poller
    /// will then surface the loss to the client which invalidates
    /// every cached inode (plan §6.4).
    pub fn drain_invalidations(&mut self, client: &MgrClientId) -> (Vec<MgrInvalidation>, bool) {
        let me = ClientKey::from_wire(client);
        let Some(inbox) = self.inboxes.get_mut(&me) else {
            return (Vec::new(), false);
        };
        let events: Vec<MgrInvalidation> = inbox.events.drain(..).collect();
        let overflowed = inbox.overflowed;
        inbox.overflowed = false;
        (events, overflowed)
    }

    /// F-ioring-lease-3: atomic drain-or-install-waker for the
    /// long-poll path. Returns the drained events PLUS, when the
    /// queue was empty, a fresh receiver the caller awaits with a
    /// timeout. A subsequent `push_invalidation` on this client
    /// fires the matching sender.
    ///
    /// At most one parked waker per client at any time — installing
    /// a new one drops the previous (its caller had to have died,
    /// since a single client_id is owned by a single in-flight
    /// long-poll task by design). The dropped sender's matching
    /// `recv` resolves to `Err(Canceled)` which the caller handles
    /// the same as a timeout (return empty events).
    pub fn drain_or_park(
        &mut self,
        client: &MgrClientId,
    ) -> (
        Vec<MgrInvalidation>,
        bool,
        Option<futures::channel::oneshot::Receiver<()>>,
    ) {
        let me = ClientKey::from_wire(client);
        let inbox = self.inboxes.entry(me).or_default();
        let events: Vec<MgrInvalidation> = inbox.events.drain(..).collect();
        let overflowed = inbox.overflowed;
        inbox.overflowed = false;
        if events.is_empty() && !overflowed {
            let (tx, rx) = futures::channel::oneshot::channel();
            // Drop any prior parked waker. `tx` drops on overwrite →
            // the older `rx.await` resolves Err(Canceled), which the
            // handler treats as "no events, retry."
            inbox.waker = Some(tx);
            (Vec::new(), false, Some(rx))
        } else {
            (events, overflowed, None)
        }
    }

    /// Drop a client entirely (long-poll disconnect, manager shutdown).
    /// The pending invalidations vanish — clients reconnecting then
    /// invalidate all their local cache per plan §6.4.
    pub fn forget_client(&mut self, client: &MgrClientId) {
        let me = ClientKey::from_wire(client);
        self.inboxes.remove(&me);
    }

    /// TTL revoke pass — BACKWARD-COMPATIBLE wrapper. Mutates state
    /// + queues invalidations + returns the (ino, new_version) pairs
    /// the caller etcd-deletes. Used by unit tests + memory-only
    /// (no-etcd) callers where etcd-failure is structurally
    /// impossible.
    ///
    /// **Production callers (`inode_lease_revoke_loop`) MUST use the
    /// 2PC pair `tick_plan` + `tick_commit_revokes` / `tick_reader_expiry`**
    /// so an etcd-delete failure rolls back without leaving phantom
    /// LeaseRevoked events in reader inboxes or a version bump
    /// without a corresponding etcd-record-delete. See BUG-LEASE-5.
    ///
    /// Ordering invariant: a writer-revoke captures the reader set
    /// BEFORE expired readers are evicted in the same tick. A reader
    /// whose lease expired on the same boundary as the writer's TTL
    /// must still be notified — its inbox lives independently of its
    /// lease entry, so the notification survives until the reader
    /// reconnects.
    pub fn tick(&mut self, now: Instant) -> Vec<(u64, u64)> {
        let plans = self.tick_plan(now);
        let revoked = self.tick_commit_revokes(&plans, now);
        self.tick_reader_expiry(now);
        revoked
    }

    /// BUG-LEASE-5 (P1 #5, 2026-06-06) — 2PC plan phase. Pure (does
    /// not mutate); enumerates writers whose TTL has expired. The
    /// caller (`inode_lease_revoke_loop`) etcd-deletes the matching
    /// `inode_leases/<ino>` records BEFORE applying the in-memory
    /// mutation via `tick_commit_revokes`. On etcd-delete failure,
    /// the in-memory state stays untouched and the next 1s tick
    /// re-discovers the same expired writers — i.e. the existing
    /// 1s revoke loop IS the natural retry, no persisted retry
    /// queue needed.
    ///
    /// Pre-fix `tick()` mutated state THEN attempted the etcd
    /// delete; on etcd-fail the in-memory writer was gone + the
    /// version was bumped + readers got LeaseRevoked events, but
    /// the etcd record stayed. Leader failover then replayed the
    /// stale record and "resurrected" the revoked writer.
    pub fn tick_plan(&self, now: Instant) -> Vec<RevokePlan> {
        let mut plans = Vec::new();
        for (&ino, state) in &self.inodes {
            if let Some(deadline) = state.writer_expires_at {
                if deadline <= now {
                    let new_version = state.version.wrapping_add(1);
                    let readers: Vec<ClientKey> = state.readers.keys().cloned().collect();
                    plans.push(RevokePlan { ino, new_version, readers });
                }
            }
        }
        plans
    }

    /// BUG-LEASE-5 (P1 #5) — 2PC commit phase for writer revokes.
    /// Applies the in-memory mutation (clear writer / bump version /
    /// queue invalidations) for each plan. **Re-validates** that
    /// each plan's writer is still expired — a concurrent heartbeat
    /// during the etcd-delete await may have refreshed the writer
    /// (rare but possible if the loop's leader check + etcd RTT
    /// overlap with a writer heartbeat). On race-skip: the etcd
    /// record was unnecessarily deleted; the next heartbeat will
    /// CAS-fail on read-not-found and the next AcquireLease
    /// re-persists. Documented but accepted residual.
    ///
    /// Returns `(ino, new_version)` pairs that ACTUALLY committed
    /// (i.e. excludes race-skipped plans).
    pub fn tick_commit_revokes(&mut self, plans: &[RevokePlan], now: Instant) -> Vec<(u64, u64)> {
        let mut committed: Vec<(u64, u64)> = Vec::new();
        let mut pending_pushes: Vec<(ClientKey, u64, u64)> = Vec::new(); // (target, ino, new_version)
        for plan in plans {
            let Some(state) = self.inodes.get_mut(&plan.ino) else {
                continue;
            };
            // Idempotency / race-skip guard.
            match state.writer_expires_at {
                Some(deadline) if deadline <= now => {}
                _ => continue,
            }
            state.writer = None;
            state.writer_diag_host.clear();
            state.writer_expires_at = None;
            state.pending_revoke_at = None;
            state.version = state.version.wrapping_add(1);
            committed.push((plan.ino, state.version));
            for r in &plan.readers {
                pending_pushes.push((r.clone(), plan.ino, state.version));
            }
        }
        for (target, ino, ver) in pending_pushes {
            self.remember_version(ino, ver);
            self.push_invalidation(&target, ino, ver, InvalidationReason::LeaseRevoked);
        }
        committed
    }

    /// BUG-LEASE-5 (P1 #5) — reader expiry sweep, SEPARATE from
    /// writer revokes. Reader expiry has no etcd write (reader
    /// leases aren't persisted), so it runs unconditionally each
    /// tick whether or not the 2PC writer-revoke loop saw etcd
    /// failures. Mirrors the original `tick()` semantics: silent
    /// drop of expired reader entries; drop empty inode entries.
    pub fn tick_reader_expiry(&mut self, now: Instant) {
        let mut to_drop_inodes: Vec<u64> = Vec::new();
        for (&ino, state) in self.inodes.iter_mut() {
            state.readers.retain(|_, deadline| *deadline > now);
            if state.writer.is_none() && state.readers.is_empty() {
                to_drop_inodes.push(ino);
            }
        }
        for ino in to_drop_inodes {
            self.inodes.remove(&ino);
        }
    }

    fn push_invalidation(
        &mut self,
        target: &ClientKey,
        ino: u64,
        version: u64,
        reason: InvalidationReason,
    ) {
        let inbox = self.inboxes.entry(target.clone()).or_default();
        inbox.push(MgrInvalidation {
            ino,
            version,
            kind: reason.wire_kind(),
        });
    }

    /// Build the persistence record for a writer-held inode.
    pub fn writer_record(&self, ino: u64) -> Option<MgrInodeLeaseRecord> {
        let s = self.inodes.get(&ino)?;
        let writer = s.writer.clone()?;
        let deadline = s.writer_expires_at?;
        let now = Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        let expires_at = epoch_seconds_now() + remaining.as_secs() as i64;
        Some(MgrInodeLeaseRecord {
            ino,
            writer: MgrClientId {
                kind: writer.kind,
                uuid: writer.uuid,
                host: s.writer_diag_host.clone(),
            },
            version: s.version,
            expires_at,
        })
    }

    /// F-ioring-lease-5 (coco P1 #6) + BUG-LEASE-4 (P1 #4): precise
    /// rollback for a writer AcquireLease whose etcd persistence
    /// failed AFTER the in-memory grant landed. Restores the writer
    /// slot AND (BUG-LEASE-4) the `version` field to `snapshot`'s
    /// shape; readers that subscribed during the etcd await are
    /// preserved (the pre-F-ioring-lease-5 rollback abused
    /// `release()`, which bumps version + pushes a phantom
    /// WriterClosed).
    ///
    /// `snapshot = None` means "no entry existed pre-acquire" → clear
    /// the writer fields; if no readers joined, drop the empty entry.
    /// `snapshot = Some(state)` means "an entry existed pre-acquire"
    /// → restore writer/host/deadline/version exactly.
    ///
    /// **BUG-LEASE-4 (2026-06-06) — version rewind.** Pre-fix this
    /// helper did NOT restore `version`, on the rationale that
    /// `acquire()` itself never bumps version. But
    /// `acquire_with_force` DOES bump on the force-revoke arm. With
    /// the 2PC refactor (`acquire_with_force_deferred`), the caller
    /// drops the staged pushes on etcd-fail so no client SEES the
    /// version bump as `LeaseRevoked.version`, but the in-memory
    /// `state.version` was still set. A future Granted-after-real-
    /// commit would return that already-bumped version to the new
    /// writer, leaving a gap with the persisted record (which still
    /// points at the OLD writer's record). Net: when the next
    /// successful Acquire bumps from N → N+2 in memory while etcd
    /// only has the old N, leader failover replays a record that
    /// claims a lower version than the live one, and any in-flight
    /// invalidation event with the live version would dangle.
    ///
    /// `last_version` shadow is INTENTIONALLY NOT REWOUND — it is a
    /// monotonic high-water mark; rewinding it would let a future
    /// re-creation pick a lower version than this revert ever
    /// observed, which breaks the L11 monotonicity invariant.
    /// Keeping `last_version` at the bumped value just means a
    /// future re-creation starts at the higher number, which is
    /// correct.
    pub fn revert_writer_acquire(&mut self, ino: u64, snapshot: Option<InodeLeaseState>) {
        let Some(entry) = self.inodes.get_mut(&ino) else {
            return;
        };
        match snapshot {
            Some(s) => {
                entry.writer = s.writer;
                entry.writer_diag_host = s.writer_diag_host;
                entry.writer_expires_at = s.writer_expires_at;
                // F-lease-preempt: restore the preempt window too
                // so an etcd-failed force-acquire doesn't leak the
                // half-progressed grace state into the next call.
                entry.pending_revoke_at = s.pending_revoke_at;
                // BUG-LEASE-4: rewind the version bump that
                // `acquire_with_force`'s force-revoke arm would
                // have applied. The `if` ensures we never lower an
                // unbumped version (paranoid no-op for non-force
                // paths).
                if entry.version > s.version {
                    entry.version = s.version;
                }
            }
            None => {
                entry.writer = None;
                entry.writer_diag_host.clear();
                entry.writer_expires_at = None;
                entry.pending_revoke_at = None;
                if entry.readers.is_empty() {
                    self.inodes.remove(&ino);
                }
            }
        }
    }

    /// On leader-promotion replay, install a writer lease from etcd.
    /// `now` is the new leader's monotonic clock; the remaining TTL is
    /// derived from `expires_at - epoch_now`.
    pub fn install_persisted_writer(&mut self, rec: MgrInodeLeaseRecord, now: Instant) {
        let now_epoch = epoch_seconds_now();
        let remaining_secs = (rec.expires_at - now_epoch).max(0) as u64;
        // Clamp to the configured TTL so a long-future deadline doesn't
        // pin a dead writer until wall-clock catches up.
        let remaining = Duration::from_secs(remaining_secs).min(self.lease_ttl);
        let key = ClientKey::from_wire(&rec.writer);
        let ino = rec.ino;
        let recorded = rec.version;
        let s = self.inode_or_create(ino);
        s.version = s.version.max(recorded);
        s.writer = Some(key);
        s.writer_diag_host = rec.writer.host.clone();
        s.writer_expires_at = Some(now + remaining);
        // Seed the shadow so any subsequent release/revoke that drops
        // the entry preserves monotonicity across failover.
        self.remember_version(ino, recorded);
    }
}

fn epoch_seconds_now() -> i64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Convenience alias so the manager can stash a single shared
/// `Rc<RefCell<LeaseRegistry>>`.
pub type SharedRegistry = Rc<RefCell<LeaseRegistry>>;

// ───────────────────── manager background loop ─────────────────────

use crate::AutumnManager;

impl AutumnManager {
    /// F-ioring-lease-1: per-second sweep of writer-TTL deadlines.
    /// Revoked writers etcd-delete the persisted record under
    /// `inode_leases/<ino>`; reader-side invalidations were already
    /// queued by `LeaseRegistry::tick`. Reader expiries are silent.
    ///
    /// Follows the F228 background-loop invariants: every await is
    /// bounded (`sleep` + the bounded etcd `put_msgs_txn` /
    /// `put_and_delete_txn` calls); the loop runs under
    /// `spawn_supervised` (panic → log + restart).
    pub(crate) async fn inode_lease_revoke_loop(self) {
        loop {
            compio::time::sleep(REVOKE_TICK).await;
            if !self.leader.get() {
                continue;
            }
            let now = Instant::now();
            // BUG-LEASE-5 (P1 #5, 2026-06-06) — 2PC TTL revoke.
            // PLAN phase: peek expired writers WITHOUT mutating.
            let plans = {
                let reg = self.inode_leases.borrow();
                reg.tick_plan(now)
            };

            if plans.is_empty() {
                // No writer-revokes this tick; still need to sweep
                // expired READER leases (no etcd write — readers
                // aren't persisted, so this never fails).
                self.inode_leases.borrow_mut().tick_reader_expiry(now);
                continue;
            }

            // ETCD phase: delete the matching writer records FIRST.
            // On failure, leave in-memory state untouched — the next
            // 1s tick re-discovers the same expired writers and
            // retries. Pre-fix the in-memory mutation happened first
            // and was UNROLLABLE on etcd-fail; leader failover then
            // replayed the stale etcd record and "resurrected" the
            // revoked writer.
            let keys: Vec<String> = plans
                .iter()
                .map(|p| format!("{}{ino}", crate::INODE_LEASES_PREFIX, ino = p.ino))
                .collect();
            if let Some(etcd) = &self.etcd {
                if let Err(e) = etcd.put_and_delete_txn(vec![], keys).await {
                    tracing::warn!(
                        error = %e,
                        planned = plans.len(),
                        "BUG-LEASE-5: revoke-loop etcd delete failed; in-memory state preserved, retry next tick"
                    );
                    // Reader expiry still runs (no etcd write).
                    self.inode_leases.borrow_mut().tick_reader_expiry(now);
                    continue;
                }
            }

            // COMMIT phase: apply the in-memory mutations + push
            // invalidations + run reader expiry. Single borrow so
            // there's no interleaving between commit + reader expiry.
            let revoked: Vec<(u64, u64)> = {
                let mut reg = self.inode_leases.borrow_mut();
                let committed = reg.tick_commit_revokes(&plans, now);
                reg.tick_reader_expiry(now);
                committed
            };
            for (ino, ver) in revoked {
                tracing::info!(
                    ino,
                    new_version = ver,
                    "BUG-LEASE-5: writer lease revoked by TTL (2PC commit)"
                );
            }
        }
    }
}

// ───────────────────────────── tests ─────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(kind: u8, n: u8, host: &str) -> MgrClientId {
        MgrClientId {
            kind,
            uuid: [n; 16],
            host: host.to_string(),
        }
    }

    fn reg() -> LeaseRegistry {
        LeaseRegistry::with_ttl(Duration::from_secs(30))
    }

    #[test]
    fn write_acquire_then_release_bumps_version_and_invalidates_readers() {
        let mut r = reg();
        let now = Instant::now();
        let w = cid(1, 1, "writer");
        let rd = cid(2, 2, "reader");

        // Reader subscribes first.
        let out = r.acquire(&rd, 42, LEASE_MODE_READ, now);
        match out {
            AcquireOutcome::Granted { version, .. } => assert_eq!(version, 1),
            other => panic!("expected Granted, got {other:?}"),
        }
        // Writer acquires.
        let out = r.acquire(&w, 42, LEASE_MODE_WRITE, now);
        assert!(matches!(out, AcquireOutcome::Granted { version: 1, writer_present: true, .. }));
        // Writer releases → version bumps + reader gets push.
        let out = r.release(&w, 42);
        assert_eq!(out, ReleaseOutcome::WriterClosed { new_version: 2 });
        let (events, _) = r.drain_invalidations(&rd);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].ino, 42);
        assert_eq!(events[0].version, 2);
        assert_eq!(events[0].kind, LEASE_INVAL_WRITER_CLOSED);
    }

    #[test]
    fn second_writer_is_refused_with_conflict_info() {
        let mut r = reg();
        let now = Instant::now();
        let w1 = cid(1, 1, "host-a");
        let w2 = cid(2, 2, "host-b");
        assert!(matches!(
            r.acquire(&w1, 7, LEASE_MODE_WRITE, now),
            AcquireOutcome::Granted { writer_present: true, .. }
        ));
        match r.acquire(&w2, 7, LEASE_MODE_WRITE, now) {
            AcquireOutcome::WriteConflict { held_by_kind, held_by_host } => {
                assert_eq!(held_by_kind, 1);
                assert_eq!(held_by_host, "host-a");
            }
            other => panic!("expected WriteConflict, got {other:?}"),
        }
    }

    #[test]
    fn same_writer_reacquire_is_idempotent() {
        let mut r = reg();
        let now = Instant::now();
        let w = cid(1, 1, "h");
        let _ = r.acquire(&w, 7, LEASE_MODE_WRITE, now);
        let out = r.acquire(&w, 7, LEASE_MODE_WRITE, now);
        assert!(matches!(out, AcquireOutcome::Granted { writer_present: true, .. }));
    }

    #[test]
    fn ttl_expiry_revokes_writer_and_invalidates_readers() {
        let mut r = reg();
        let t0 = Instant::now();
        let w = cid(1, 1, "h");
        let rd = cid(2, 2, "h");
        let _ = r.acquire(&rd, 9, LEASE_MODE_READ, t0);
        let _ = r.acquire(&w, 9, LEASE_MODE_WRITE, t0);

        // Tick well after TTL.
        let revoked = r.tick(t0 + Duration::from_secs(31));
        assert_eq!(revoked, vec![(9, 2)]);

        let (events, overflowed) = r.drain_invalidations(&rd);
        assert!(!overflowed);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, LEASE_INVAL_LEASE_REVOKED);
        assert_eq!(events[0].ino, 9);
        assert_eq!(events[0].version, 2);
    }

    #[test]
    fn heartbeat_extends_lease() {
        let mut r = reg();
        let t0 = Instant::now();
        let w = cid(1, 1, "h");
        let _ = r.acquire(&w, 1, LEASE_MODE_WRITE, t0);

        // Heartbeat right before expiry; subsequent tick must not revoke.
        let _ = r.heartbeat(&w, 1, t0 + Duration::from_secs(25));
        let revoked = r.tick(t0 + Duration::from_secs(31));
        assert!(revoked.is_empty(), "heartbeat should have extended");

        // Skip more time — TTL re-expires.
        let revoked = r.tick(t0 + Duration::from_secs(60));
        assert_eq!(revoked, vec![(1, 2)]);
    }

    #[test]
    fn heartbeat_returns_not_held_when_lease_was_revoked() {
        let mut r = reg();
        let t0 = Instant::now();
        let w = cid(1, 1, "h");
        let _ = r.acquire(&w, 1, LEASE_MODE_WRITE, t0);
        let _ = r.tick(t0 + Duration::from_secs(31));
        let out = r.heartbeat(&w, 1, t0 + Duration::from_secs(32));
        assert_eq!(out, HeartbeatOutcome::NotHeld);
    }

    #[test]
    fn reader_release_is_silent_and_does_not_bump_version() {
        let mut r = reg();
        let now = Instant::now();
        let rd = cid(2, 2, "r");
        let _ = r.acquire(&rd, 1, LEASE_MODE_READ, now);
        let out = r.release(&rd, 1);
        assert_eq!(out, ReleaseOutcome::ReaderReleased);
        // No push to anyone.
        let (events, _) = r.drain_invalidations(&rd);
        assert!(events.is_empty());
    }

    #[test]
    fn release_unknown_client_is_noop() {
        let mut r = reg();
        let c = cid(1, 9, "x");
        assert_eq!(r.release(&c, 100), ReleaseOutcome::NotHeld);
    }

    #[test]
    fn invalid_mode_is_rejected() {
        let mut r = reg();
        let c = cid(1, 1, "x");
        let out = r.acquire(&c, 1, 99, Instant::now());
        assert_eq!(out, AcquireOutcome::InvalidMode);
    }

    #[test]
    fn host_field_does_not_affect_identity() {
        let mut r = reg();
        let now = Instant::now();
        let c1 = cid(1, 7, "alpha");
        // Same kind+uuid, different host.
        let c2 = cid(1, 7, "beta");
        let _ = r.acquire(&c1, 1, LEASE_MODE_WRITE, now);
        let out = r.acquire(&c2, 1, LEASE_MODE_WRITE, now);
        // Treated as the same writer → granted, not conflict.
        assert!(matches!(
            out,
            AcquireOutcome::Granted { writer_present: true, .. }
        ));
    }

    #[test]
    fn many_readers_can_coexist_with_writer() {
        let mut r = reg();
        let now = Instant::now();
        let w = cid(1, 1, "w");
        let r1 = cid(2, 10, "r1");
        let r2 = cid(2, 11, "r2");
        let r3 = cid(2, 12, "r3");
        let _ = r.acquire(&w, 1, LEASE_MODE_WRITE, now);
        let _ = r.acquire(&r1, 1, LEASE_MODE_READ, now);
        let _ = r.acquire(&r2, 1, LEASE_MODE_READ, now);
        let _ = r.acquire(&r3, 1, LEASE_MODE_READ, now);
        let _ = r.release(&w, 1);
        for rd in [&r1, &r2, &r3] {
            let (events, _) = r.drain_invalidations(rd);
            assert_eq!(events.len(), 1, "{rd:?} missed invalidation");
            assert_eq!(events[0].version, 2);
        }
    }

    #[test]
    fn writer_record_round_trips_and_install_recovers_state() {
        let mut r = reg();
        let now = Instant::now();
        let w = cid(1, 1, "host-x");
        let _ = r.acquire(&w, 77, LEASE_MODE_WRITE, now);
        let rec = r.writer_record(77).expect("must persist");
        assert_eq!(rec.ino, 77);
        assert_eq!(rec.writer.kind, 1);
        assert_eq!(rec.version, 1);

        // Fresh registry simulates leader failover.
        let mut r2 = reg();
        r2.install_persisted_writer(rec, Instant::now());
        // Same writer can heartbeat.
        let out = r2.heartbeat(&w, 77, Instant::now());
        assert!(matches!(out, HeartbeatOutcome::Renewed { writer_present: true, .. }));
        // Different writer is still refused.
        let other = cid(1, 99, "x");
        assert!(matches!(
            r2.acquire(&other, 77, LEASE_MODE_WRITE, Instant::now()),
            AcquireOutcome::WriteConflict { .. }
        ));
    }

    #[test]
    fn revert_writer_acquire_restores_pre_state_without_bumping_version() {
        // Regression for coco P1 #6: pre-fix the etcd-write-failure
        // rollback called `release()` which bumps version + pushes
        // phantom WriterClosed to readers. Now `revert_writer_acquire`
        // restores the writer slot precisely without touching version.
        let mut r = reg();
        let now = Instant::now();
        let w = cid(1, 1, "writer-X");
        let rd = cid(2, 2, "reader-Y");

        // Reader subscribed first; writer acquires.
        let _ = r.acquire(&rd, 99, LEASE_MODE_READ, now);
        let snap = r.inodes.get(&99).cloned();
        let _ = r.acquire(&w, 99, LEASE_MODE_WRITE, now);
        let post_version = r.inodes.get(&99).unwrap().version;

        // Revert as if etcd-put-failed.
        r.revert_writer_acquire(99, snap);

        let entry = r.inodes.get(&99).expect("reader keeps the entry alive");
        assert!(entry.writer.is_none(), "writer slot cleared");
        assert_eq!(entry.version, post_version, "version untouched by revert");
        assert!(entry.readers.contains_key(&ClientKey::from_wire(&rd)));

        // No invalidation should have been pushed to the reader —
        // they didn't see any version change.
        let (events, _) = r.drain_invalidations(&rd);
        assert!(
            events.is_empty(),
            "revert MUST NOT push phantom WriterClosed; events={events:?}"
        );
    }

    #[test]
    fn revert_writer_acquire_drops_empty_entry_when_no_pre_state() {
        // Fresh inode (no readers), writer acquires, etcd fails →
        // revert with `snapshot = None` should leave the registry
        // exactly empty.
        let mut r = reg();
        let w = cid(1, 7, "w");
        assert!(!r.inodes.contains_key(&55));
        let _ = r.acquire(&w, 55, LEASE_MODE_WRITE, Instant::now());
        r.revert_writer_acquire(55, None);
        assert!(
            !r.inodes.contains_key(&55),
            "no readers ⇒ empty entry must be dropped"
        );
    }

    // ───────────── F-lease-preempt ─────────────────────────

    fn reg_with_grace(grace_ms: u64) -> LeaseRegistry {
        LeaseRegistry::with_ttl_and_revoke_grace(
            Duration::from_secs(30),
            Duration::from_millis(grace_ms),
        )
    }

    #[test]
    fn force_acquire_on_held_writer_returns_revoke_pending_and_pushes_will_revoke() {
        let mut r = reg_with_grace(5000);
        let t0 = Instant::now();
        let w1 = cid(1, 1, "first-writer");
        let w2 = cid(1, 2, "preempter");

        // Writer 1 holds.
        let _ = r.acquire(&w1, 100, LEASE_MODE_WRITE, t0);
        // Writer 2 tries force-acquire → RevokePending.
        match r.acquire_with_force(&w2, 100, LEASE_MODE_WRITE, true, t0) {
            AcquireOutcome::RevokePending { eta_ms, held_by_kind, held_by_host } => {
                assert!(eta_ms > 0 && eta_ms <= 5000);
                assert_eq!(held_by_kind, 1);
                assert_eq!(held_by_host, "first-writer");
            }
            other => panic!("expected RevokePending, got {other:?}"),
        }
        // Writer 1's inbox MUST have a WillRevokeIn push.
        let (events, _) = r.drain_invalidations(&w1);
        assert_eq!(events.len(), 1, "WillRevokeIn must be queued");
        assert_eq!(events[0].kind, LEASE_INVAL_WILL_REVOKE_IN);
        assert_eq!(events[0].ino, 100);
        // The `version` field carries the grace ms.
        assert!(events[0].version > 0 && events[0].version <= 5000);
    }

    #[test]
    fn force_acquire_retried_before_grace_expires_returns_remaining_eta() {
        let mut r = reg_with_grace(5000);
        let t0 = Instant::now();
        let w1 = cid(1, 1, "w1");
        let w2 = cid(1, 2, "w2");

        let _ = r.acquire(&w1, 1, LEASE_MODE_WRITE, t0);
        let _ = r.acquire_with_force(&w2, 1, LEASE_MODE_WRITE, true, t0);
        // 1s in → ~4s remaining.
        let t1 = t0 + Duration::from_secs(1);
        let out = r.acquire_with_force(&w2, 1, LEASE_MODE_WRITE, true, t1);
        match out {
            AcquireOutcome::RevokePending { eta_ms, .. } => {
                assert!(
                    (3500..=4500).contains(&eta_ms),
                    "second call before grace: eta_ms should be near 4000ms, got {eta_ms}"
                );
            }
            other => panic!("expected RevokePending, got {other:?}"),
        }
    }

    #[test]
    fn voluntary_release_within_grace_clears_pending_and_lets_next_acquire_succeed_cleanly() {
        let mut r = reg_with_grace(5000);
        let t0 = Instant::now();
        let w1 = cid(1, 1, "w1");
        let w2 = cid(1, 2, "w2");

        let v_before = match r.acquire(&w1, 7, LEASE_MODE_WRITE, t0) {
            AcquireOutcome::Granted { version, .. } => version,
            other => panic!("acquire: {other:?}"),
        };
        // Start the grace window.
        let _ = r.acquire_with_force(&w2, 7, LEASE_MODE_WRITE, true, t0);
        // w1 releases voluntarily.
        match r.release(&w1, 7) {
            ReleaseOutcome::WriterClosed { new_version } => {
                assert_eq!(new_version, v_before + 1);
            }
            other => panic!("release: {other:?}"),
        }
        // The pending_revoke_at was cleared by release (the inode
        // entry may have been dropped if no readers — recreated on
        // next acquire).
        let t1 = t0 + Duration::from_millis(100);
        match r.acquire_with_force(&w2, 7, LEASE_MODE_WRITE, true, t1) {
            AcquireOutcome::Granted { version, writer_present, .. } => {
                assert_eq!(version, v_before + 1, "version inherits the close-bump");
                assert!(writer_present);
            }
            other => panic!("expected Granted after voluntary release, got {other:?}"),
        }
    }

    #[test]
    fn force_acquire_after_grace_expires_force_revokes_and_grants() {
        let mut r = reg_with_grace(100); // short grace for the test
        let t0 = Instant::now();
        let w1 = cid(1, 1, "w1");
        let w2 = cid(1, 2, "w2");
        let rd = cid(2, 3, "reader");

        // Reader subscribes so the LeaseRevoked push has a target.
        let _ = r.acquire(&rd, 11, LEASE_MODE_READ, t0);
        let v_before = match r.acquire(&w1, 11, LEASE_MODE_WRITE, t0) {
            AcquireOutcome::Granted { version, .. } => version,
            other => panic!("{other:?}"),
        };

        // Start grace + immediately retry past the deadline.
        let _ = r.acquire_with_force(&w2, 11, LEASE_MODE_WRITE, true, t0);
        let t_past = t0 + Duration::from_millis(200); // > 100ms grace
        match r.acquire_with_force(&w2, 11, LEASE_MODE_WRITE, true, t_past) {
            AcquireOutcome::Granted { version, writer_present, .. } => {
                assert_eq!(
                    version,
                    v_before + 1,
                    "force-revoke bumps version"
                );
                assert!(writer_present);
            }
            other => panic!("expected Granted after grace expiry, got {other:?}"),
        }

        // The deposed w1 + the reader BOTH get LeaseRevoked.
        let (w1_events, _) = r.drain_invalidations(&w1);
        assert!(
            w1_events.iter().any(|e| e.kind == LEASE_INVAL_LEASE_REVOKED),
            "deposed writer must get LeaseRevoked: {w1_events:?}"
        );
        let (rd_events, _) = r.drain_invalidations(&rd);
        assert!(
            rd_events.iter().any(|e| e.kind == LEASE_INVAL_LEASE_REVOKED),
            "reader must get LeaseRevoked: {rd_events:?}"
        );
    }

    #[test]
    fn force_acquire_on_no_writer_is_just_a_grant() {
        let mut r = reg_with_grace(5000);
        let w = cid(1, 1, "w");
        match r.acquire_with_force(&w, 99, LEASE_MODE_WRITE, true, Instant::now()) {
            AcquireOutcome::Granted { writer_present, .. } => {
                assert!(writer_present);
            }
            other => panic!("expected Granted for fresh ino, got {other:?}"),
        }
    }

    #[test]
    fn force_acquire_by_same_writer_is_idempotent_refresh() {
        // The writer re-acquiring with force on their own ino must
        // not trigger a self-revoke. Same shape as the regular
        // refresh path.
        let mut r = reg_with_grace(5000);
        let t0 = Instant::now();
        let w = cid(1, 1, "w");
        let _ = r.acquire(&w, 1, LEASE_MODE_WRITE, t0);
        let out = r.acquire_with_force(&w, 1, LEASE_MODE_WRITE, true, t0);
        assert!(matches!(
            out,
            AcquireOutcome::Granted { writer_present: true, .. }
        ));
        // No push to self.
        let (events, _) = r.drain_invalidations(&w);
        assert!(events.is_empty(), "self-acquire MUST NOT push WillRevokeIn");
    }

    #[test]
    fn drain_or_park_returns_events_when_present() {
        let mut r = reg();
        let rd = cid(2, 1, "rd");
        // Pre-queue an event by walking through a writer-release.
        let w = cid(1, 1, "w");
        let _ = r.acquire(&rd, 1, LEASE_MODE_READ, Instant::now());
        let _ = r.acquire(&w, 1, LEASE_MODE_WRITE, Instant::now());
        let _ = r.release(&w, 1);
        let (events, overflowed, parked) = r.drain_or_park(&rd);
        assert_eq!(events.len(), 1);
        assert!(!overflowed);
        assert!(parked.is_none(), "non-empty events ⇒ no waker installed");
    }

    #[test]
    fn drain_or_park_installs_waker_and_push_wakes_it() {
        let mut r = reg();
        let rd = cid(2, 5, "rd");
        let (events, _, parked) = r.drain_or_park(&rd);
        assert!(events.is_empty());
        let mut rx = parked.expect("waker installed when queue empty");
        // Receiver pending before push.
        assert!(rx.try_recv().unwrap().is_none());
        // Push an event → waker fires synchronously.
        r.push_invalidation(
            &ClientKey::from_wire(&rd),
            7,
            42,
            InvalidationReason::WriterClosed,
        );
        assert!(matches!(rx.try_recv().unwrap(), Some(())));
        // Now drain finds the event.
        let (events, _) = r.drain_invalidations(&rd);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].ino, 7);
        assert_eq!(events[0].version, 42);
    }

    #[test]
    fn drain_or_park_replaces_stale_waker() {
        let mut r = reg();
        let rd = cid(2, 9, "rd");
        let (_, _, parked1) = r.drain_or_park(&rd);
        let mut rx1 = parked1.unwrap();
        // Second call installs a fresh waker; the first one is dropped
        // → its receiver resolves to Canceled (treated as "no events").
        let (_, _, parked2) = r.drain_or_park(&rd);
        let _rx2 = parked2.unwrap();
        let err = rx1.try_recv();
        // try_recv returns Ok(None) until the sender resolves; once
        // dropped it returns Err(Canceled).
        assert!(err.is_err(), "stale waker should be cancelled");
    }

    #[test]
    fn version_is_monotonic_across_remove_and_reacquire() {
        // Regression: pre-F-ioring-lease-2 the auto-remove of an empty
        // inode entry on release reset `version` to 1, breaking close-
        // to-open coherence for any client whose cache still carried
        // the older generation's `(ino, version)` pair.
        let mut r = reg();
        let now = Instant::now();
        let wa = cid(1, 1, "wa");
        let wb = cid(1, 2, "wb");
        let _ = r.acquire(&wa, 100, LEASE_MODE_WRITE, now);
        let out = r.release(&wa, 100);
        assert_eq!(out, ReleaseOutcome::WriterClosed { new_version: 2 });
        // Inode entry removed (no readers), so the next acquire would
        // recreate the entry. Verify it resumes from 2, not 1.
        match r.acquire(&wb, 100, LEASE_MODE_WRITE, now) {
            AcquireOutcome::Granted { version, .. } => assert_eq!(version, 2),
            other => panic!("expected Granted, got {other:?}"),
        }
        // Another close → 3, not 1.
        assert_eq!(
            r.release(&wb, 100),
            ReleaseOutcome::WriterClosed { new_version: 3 }
        );
    }

    // ─────────────── BUG-LEASE-4 (P1 #4) ────────────────────

    #[test]
    fn bug_lease_4_deferred_pushes_hold_lease_revoked_until_commit() {
        // BUG-LEASE-4 KEY ASSERTION: the force-revoke arm of
        // `acquire_with_force_deferred` STAGES LeaseRevoked events
        // in the DeferredPushes bundle — they do NOT land in client
        // inboxes until `flush_deferred_pushes` is called. This
        // lets the production handler do (1) plan + memory mutate,
        // (2) etcd persist, (3) on commit → flush, on fail → drop
        // pushes + revert memory.
        let mut r = reg_with_grace(0); // grace=0 → immediate force-revoke
        let t0 = Instant::now();
        let w_old = cid(1, 1, "deposed");
        let w_new = cid(1, 2, "preempter");
        let rd = cid(2, 3, "reader");

        // Reader subscribes; writer 1 acquires.
        let _ = r.acquire(&rd, 50, LEASE_MODE_READ, t0);
        let _ = r.acquire(&w_old, 50, LEASE_MODE_WRITE, t0);
        // Drain inboxes so the test sees only the BUG-LEASE-4 pushes.
        let _ = r.drain_invalidations(&w_old);
        let _ = r.drain_invalidations(&rd);

        // First force call: starts the grace window (grace=0 so the
        // SAME call at this Instant won't yet revoke — second call
        // crosses the deadline). Stage WillRevokeIn.
        let mut pushes1 = DeferredPushes::default();
        let _ =
            r.acquire_with_force_deferred(&w_new, 50, LEASE_MODE_WRITE, true, t0, &mut pushes1);
        // The WillRevokeIn push is STAGED; w_old's inbox empty.
        let (w_old_events_pre, _) = r.drain_invalidations(&w_old);
        assert!(
            w_old_events_pre.is_empty(),
            "WillRevokeIn must NOT be in inbox before flush; got {w_old_events_pre:?}"
        );
        // (Simulate etcd OK for the RevokePending arm — flush.)
        r.flush_deferred_pushes(pushes1);

        // Second force call AFTER grace=0: force-revokes + grants.
        let t1 = t0 + Duration::from_millis(1);
        let mut pushes2 = DeferredPushes::default();
        let out2 = r.acquire_with_force_deferred(
            &w_new, 50, LEASE_MODE_WRITE, true, t1, &mut pushes2,
        );
        assert!(matches!(out2, AcquireOutcome::Granted { .. }));
        // Two LeaseRevoked events staged (deposed + reader).
        assert_eq!(
            pushes2.len(),
            2,
            "BUG-LEASE-4: deposed writer + 1 reader → 2 LeaseRevoked pushes staged"
        );

        // BEFORE flush: NEITHER inbox has the LeaseRevoked.
        // (w_old already saw the WillRevokeIn from pushes1; drain it
        // first so we measure only the new staging.)
        let (w_old_after_will, _) = r.drain_invalidations(&w_old);
        assert!(
            w_old_after_will.iter().all(|e| e.kind != LEASE_INVAL_LEASE_REVOKED),
            "BUG-LEASE-4: w_old inbox must NOT contain LeaseRevoked before commit-flush; got {w_old_after_will:?}"
        );
        let (rd_pre, _) = r.drain_invalidations(&rd);
        assert!(
            rd_pre.iter().all(|e| e.kind != LEASE_INVAL_LEASE_REVOKED),
            "BUG-LEASE-4: reader inbox must NOT contain LeaseRevoked before commit-flush; got {rd_pre:?}"
        );

        // Now flush (simulating etcd commit success).
        r.flush_deferred_pushes(pushes2);
        let (w_old_post, _) = r.drain_invalidations(&w_old);
        let (rd_post, _) = r.drain_invalidations(&rd);
        assert!(
            w_old_post.iter().any(|e| e.kind == LEASE_INVAL_LEASE_REVOKED),
            "after flush w_old must see LeaseRevoked: {w_old_post:?}"
        );
        assert!(
            rd_post.iter().any(|e| e.kind == LEASE_INVAL_LEASE_REVOKED),
            "after flush reader must see LeaseRevoked: {rd_post:?}"
        );
    }

    #[test]
    fn bug_lease_4_revert_rewinds_version_after_force_revoke() {
        // BUG-LEASE-4 KEY ASSERTION: `revert_writer_acquire` rewinds
        // the force-revoke's version bump when the snapshot's
        // version is lower than the in-memory version. Pre-fix the
        // revert ONLY restored writer/host/deadline/pending_revoke_at
        // and left version bumped — so a future Open would get a
        // lease_epoch that didn't correspond to any persisted
        // record.
        let mut r = reg_with_grace(0);
        let t0 = Instant::now();
        let w_old = cid(1, 1, "old");
        let w_new = cid(1, 2, "new");

        // w_old acquires (version=1).
        let _ = r.acquire(&w_old, 60, LEASE_MODE_WRITE, t0);
        let snap = r.inodes.get(&60).cloned();
        assert_eq!(snap.as_ref().unwrap().version, 1);

        // First force → RevokePending (starts grace). Drop pushes.
        let mut p1 = DeferredPushes::default();
        let _ = r.acquire_with_force_deferred(&w_new, 60, LEASE_MODE_WRITE, true, t0, &mut p1);
        drop(p1);

        // Second force past grace → Granted, force-revoke bumps to 2.
        let t1 = t0 + Duration::from_millis(1);
        let mut p2 = DeferredPushes::default();
        let _ = r.acquire_with_force_deferred(&w_new, 60, LEASE_MODE_WRITE, true, t1, &mut p2);
        let post = r.inodes.get(&60).unwrap().version;
        assert_eq!(post, 2, "force-revoke bumped version 1→2");

        // Simulate etcd-fail: drop pushes + revert.
        drop(p2);
        r.revert_writer_acquire(60, snap);
        let reverted = r.inodes.get(&60).unwrap().version;
        assert_eq!(
            reverted, 1,
            "BUG-LEASE-4: revert MUST rewind force-revoke's version bump"
        );

        // ALSO: w_old still holds the writer slot (snapshot restored).
        let entry = r.inodes.get(&60).unwrap();
        assert!(entry.writer.is_some(), "writer slot restored");
        assert_eq!(
            entry.writer.as_ref().unwrap().uuid,
            [1u8; 16],
            "snap's writer restored, NOT the preempter's"
        );
    }

    #[test]
    fn bug_lease_4_last_version_shadow_not_rewound_on_revert() {
        // The shadow is monotonic HIGH-WATER. Even on revert, the
        // shadow keeps the bumped value because:
        //   (a) clients that observed an inbox draining of the
        //       staged-but-then-flushed pushes need monotonic
        //       reasoning when they re-Open the inode later, and
        //   (b) the next Granted on this inode SHOULD start at
        //       least at `shadow + 1` so a fresh writer doesn't
        //       hand out the same version twice.
        // (a) is moot in the revert-on-fail path — by definition no
        // flush happened, so no client saw a version. But (b) still
        // matters because the bump was in-memory at some point;
        // future correctness of the shadow doesn't depend on
        // "transactions" being externally visible.
        let mut r = reg_with_grace(0);
        let t0 = Instant::now();
        let w_old = cid(1, 1, "old");
        let w_new = cid(1, 2, "new");

        let _ = r.acquire(&w_old, 70, LEASE_MODE_WRITE, t0);
        let snap = r.inodes.get(&70).cloned();

        // Trigger force-revoke (bumps shadow to 2).
        let mut p1 = DeferredPushes::default();
        let _ = r.acquire_with_force_deferred(&w_new, 70, LEASE_MODE_WRITE, true, t0, &mut p1);
        drop(p1);
        let t1 = t0 + Duration::from_millis(1);
        let mut p2 = DeferredPushes::default();
        let _ = r.acquire_with_force_deferred(&w_new, 70, LEASE_MODE_WRITE, true, t1, &mut p2);
        drop(p2);
        assert_eq!(r.last_version.get(&70).copied().unwrap_or(0), 2);

        // Revert: in-memory state.version goes back to 1; shadow
        // STAYS at 2 (high-water invariant).
        r.revert_writer_acquire(70, snap);
        assert_eq!(r.inodes.get(&70).unwrap().version, 1);
        assert_eq!(
            r.last_version.get(&70).copied().unwrap_or(0),
            2,
            "shadow MUST stay at the bumped value (high-water)"
        );
    }

    #[test]
    fn bug_lease_4_revert_handles_empty_pre_snapshot_no_panic() {
        // The case where the pre-acquire snapshot is None (no entry
        // existed). Reverting should drop the entry if no readers
        // subscribed during the etcd await. This is the existing
        // F-ioring-lease-5 behavior; BUG-LEASE-4 doesn't change it,
        // but a regression test guards the helper's None arm
        // against an accidental edit.
        let mut r = reg_with_grace(0);
        let w = cid(1, 7, "w");
        assert!(!r.inodes.contains_key(&85));
        let _ = r.acquire(&w, 85, LEASE_MODE_WRITE, Instant::now());
        r.revert_writer_acquire(85, None);
        assert!(!r.inodes.contains_key(&85));
    }

    // ─────────────── BUG-LEASE-5 (P1 #5) ────────────────────

    #[test]
    fn bug_lease_5_tick_plan_does_not_mutate_state() {
        // BUG-LEASE-5 KEY ASSERTION: `tick_plan` is a pure read,
        // so an etcd-delete failure between plan and commit
        // doesn't leave the in-memory state in a half-revoked
        // shape.
        let mut r = LeaseRegistry::with_ttl(Duration::from_millis(100));
        let t0 = Instant::now();
        let w = cid(1, 1, "writer");
        let rd = cid(2, 2, "reader");
        let _ = r.acquire(&w, 1, LEASE_MODE_WRITE, t0);
        let _ = r.acquire(&rd, 1, LEASE_MODE_READ, t0);

        let t_past = t0 + Duration::from_millis(200);
        let pre_version = r.inodes.get(&1).unwrap().version;
        let pre_writer = r.inodes.get(&1).unwrap().writer.clone();

        let plans = r.tick_plan(t_past);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].ino, 1);
        assert_eq!(plans[0].new_version, pre_version + 1);
        assert_eq!(plans[0].readers.len(), 1);

        // tick_plan must NOT mutate.
        let post_version = r.inodes.get(&1).unwrap().version;
        let post_writer = r.inodes.get(&1).unwrap().writer.clone();
        assert_eq!(post_version, pre_version);
        assert_eq!(post_writer, pre_writer);
        // Reader inbox MUST NOT contain a LeaseRevoked event yet.
        let (events, _) = r.drain_invalidations(&rd);
        assert!(
            events.iter().all(|e| e.kind != LEASE_INVAL_LEASE_REVOKED),
            "tick_plan must NOT push events: {events:?}"
        );
    }

    #[test]
    fn bug_lease_5_tick_commit_applies_writer_revoke_and_pushes_lease_revoked() {
        let mut r = LeaseRegistry::with_ttl(Duration::from_millis(100));
        let t0 = Instant::now();
        let w = cid(1, 1, "writer");
        let rd1 = cid(2, 10, "reader-1");
        let rd2 = cid(2, 11, "reader-2");
        let _ = r.acquire(&w, 2, LEASE_MODE_WRITE, t0);
        let _ = r.acquire(&rd1, 2, LEASE_MODE_READ, t0);
        let _ = r.acquire(&rd2, 2, LEASE_MODE_READ, t0);

        let t_past = t0 + Duration::from_millis(200);
        let plans = r.tick_plan(t_past);
        let committed = r.tick_commit_revokes(&plans, t_past);
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0, 2);

        // Both readers see LeaseRevoked.
        let (e1, _) = r.drain_invalidations(&rd1);
        let (e2, _) = r.drain_invalidations(&rd2);
        for events in [&e1, &e2] {
            let lr: Vec<_> = events
                .iter()
                .filter(|e| e.kind == LEASE_INVAL_LEASE_REVOKED && e.ino == 2)
                .collect();
            assert_eq!(lr.len(), 1, "must push exactly one LeaseRevoked: {events:?}");
        }
    }

    #[test]
    fn bug_lease_5_tick_commit_skips_writer_who_heartbeated_during_etcd_await() {
        // Race-skip path: between tick_plan and tick_commit_revokes,
        // the writer's deadline was refreshed by a heartbeat. The
        // commit phase MUST NOT revoke (the writer is no longer
        // expired); the corresponding etcd record would have been
        // unnecessarily deleted, and the next heartbeat's CAS-fail
        // pattern recovers eventually (heartbeat re-persists are
        // currently disabled — see comments — but the in-memory
        // state stays consistent).
        let mut r = LeaseRegistry::with_ttl(Duration::from_millis(100));
        let t0 = Instant::now();
        let w = cid(1, 1, "writer");
        let _ = r.acquire(&w, 3, LEASE_MODE_WRITE, t0);

        let t_past = t0 + Duration::from_millis(200);
        let plans = r.tick_plan(t_past);
        assert_eq!(plans.len(), 1);

        // Simulate: while etcd-delete was in flight, the writer
        // heartbeated and refreshed the deadline.
        let _ = r.heartbeat(&w, 3, t_past);
        // Now the writer's deadline > t_past, so commit must skip.

        let committed = r.tick_commit_revokes(&plans, t_past);
        assert!(
            committed.is_empty(),
            "BUG-LEASE-5 race-skip: refreshed writer MUST NOT be revoked on commit; got {committed:?}"
        );
        // The writer slot is still set + version untouched.
        let entry = r.inodes.get(&3).unwrap();
        assert!(entry.writer.is_some());
        assert_eq!(entry.version, 1);
    }

    #[test]
    fn bug_lease_5_tick_reader_expiry_independent_of_writer_revokes() {
        // Reader expiry runs unconditionally each tick (no etcd
        // write). Verify it correctly drops expired readers + the
        // empty inode entry, independently of any writer-revoke
        // plan.
        let mut r = LeaseRegistry::with_ttl(Duration::from_millis(100));
        let t0 = Instant::now();
        let rd = cid(2, 1, "reader-only");
        let _ = r.acquire(&rd, 4, LEASE_MODE_READ, t0);
        assert!(r.inodes.contains_key(&4));

        let t_past = t0 + Duration::from_millis(200);
        let plans = r.tick_plan(t_past);
        assert!(plans.is_empty(), "no writer to revoke");
        r.tick_reader_expiry(t_past);

        // Reader was the only entry holder → inode dropped.
        assert!(!r.inodes.contains_key(&4));
    }

    #[test]
    fn bug_lease_5_simulated_etcd_fail_preserves_in_memory_state() {
        // The behavioral guarantee: if the loop's etcd-delete call
        // were to fail, the loop calls `tick_reader_expiry` but
        // SKIPS `tick_commit_revokes`. The expired writer stays in
        // the registry; the next tick re-discovers it.
        let mut r = LeaseRegistry::with_ttl(Duration::from_millis(100));
        let t0 = Instant::now();
        let w = cid(1, 1, "writer");
        let rd = cid(2, 2, "reader");
        let _ = r.acquire(&w, 5, LEASE_MODE_WRITE, t0);
        let _ = r.acquire(&rd, 5, LEASE_MODE_READ, t0);

        let t_past = t0 + Duration::from_millis(200);
        let plans = r.tick_plan(t_past);
        assert_eq!(plans.len(), 1);

        // Simulate etcd-delete fail by SKIPPING `tick_commit_revokes`.
        // Only reader expiry runs.
        r.tick_reader_expiry(t_past);

        // Reader expiry dropped the reader, but the writer entry
        // is still alive: writer slot present, version unchanged.
        let entry = r.inodes.get(&5).unwrap();
        assert!(
            entry.writer.is_some(),
            "BUG-LEASE-5: etcd-fail leaves writer slot intact"
        );
        assert_eq!(entry.version, 1, "no spurious version bump");

        // Next tick re-discovers it (idempotent rediscovery).
        let t_next = t_past + Duration::from_millis(1);
        let plans_next = r.tick_plan(t_next);
        assert_eq!(plans_next.len(), 1, "re-discovered next tick");

        // Now the etcd delete succeeds → commit.
        let committed = r.tick_commit_revokes(&plans_next, t_next);
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0, 5);
        assert_eq!(committed[0].1, 2, "post-commit version bumped exactly once");
    }

    #[test]
    fn bug_lease_5_tick_wrapper_remains_backward_compatible() {
        // The `tick()` wrapper still applies in one call (used by
        // memory-only tests + the in-process test helpers). The 2PC
        // refactor MUST NOT have broken its semantics.
        let mut r = LeaseRegistry::with_ttl(Duration::from_millis(100));
        let t0 = Instant::now();
        let w = cid(1, 1, "writer");
        let rd = cid(2, 2, "reader");
        let _ = r.acquire(&w, 6, LEASE_MODE_WRITE, t0);
        let _ = r.acquire(&rd, 6, LEASE_MODE_READ, t0);

        let t_past = t0 + Duration::from_millis(200);
        let revoked = r.tick(t_past);
        assert_eq!(revoked.len(), 1);
        assert_eq!(revoked[0].0, 6);
        let (e, _) = r.drain_invalidations(&rd);
        assert!(e.iter().any(|ev| ev.kind == LEASE_INVAL_LEASE_REVOKED));
    }

    #[test]
    fn inbox_overflow_drops_oldest_and_flags() {
        let mut r = reg();
        let rd = cid(2, 1, "r");
        // Ensure the inbox exists.
        let _ = r.acquire(&rd, 1, LEASE_MODE_READ, Instant::now());
        let key = ClientKey::from_wire(&rd);
        // Force-fill beyond the cap.
        for i in 0..(MAX_INBOX_EVENTS as u64 + 5) {
            r.push_invalidation(&key, 1, i, InvalidationReason::WriterClosed);
        }
        let (events, overflowed) = r.drain_invalidations(&rd);
        assert!(overflowed);
        assert_eq!(events.len(), MAX_INBOX_EVENTS);
        // Oldest dropped → first surviving version is 5.
        assert_eq!(events.first().unwrap().version, 5);
    }
}
