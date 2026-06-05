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
    MgrClientId, MgrInodeLeaseInfo, MgrInodeLeaseRecord, MgrInvalidation,
    LEASE_INVAL_LEASE_REVOKED, LEASE_INVAL_WRITER_CLOSED, LEASE_MODE_READ, LEASE_MODE_WRITE,
};

/// Default writer-lease TTL (seconds). Same magnitude as
/// `acquire_owner_lock`'s etcd lease (plan §3.1). Clients must
/// heartbeat at <= TTL / 6 (`5s` for the 30s default) to stay alive.
pub const DEFAULT_LEASE_TTL_SECS: u32 = 30;

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
}

impl InvalidationReason {
    pub fn wire_kind(self) -> u8 {
        match self {
            InvalidationReason::WriterClosed => LEASE_INVAL_WRITER_CLOSED,
            InvalidationReason::LeaseRevoked => LEASE_INVAL_LEASE_REVOKED,
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
}

impl LeaseRegistry {
    pub fn with_ttl(ttl: Duration) -> Self {
        LeaseRegistry {
            inodes: HashMap::new(),
            inboxes: HashMap::new(),
            last_version: HashMap::new(),
            lease_ttl: ttl,
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

    fn snapshot(&self, ino: u64) -> Option<MgrInodeLeaseInfo> {
        let s = self.inodes.get(&ino)?;
        Some(MgrInodeLeaseInfo {
            ino,
            version: s.version,
            writer_present: s.writer.is_some(),
            ttl_secs: self.ttl_secs(),
        })
    }

    /// `mode = LEASE_MODE_READ` or `LEASE_MODE_WRITE`. `now` is the
    /// monotonic clock the manager owns; tests pass a synthetic
    /// `Instant`.
    pub fn acquire(
        &mut self,
        client: &MgrClientId,
        ino: u64,
        mode: u8,
        now: Instant,
    ) -> AcquireOutcome {
        if mode != LEASE_MODE_READ && mode != LEASE_MODE_WRITE {
            return AcquireOutcome::InvalidMode;
        }
        let ttl = self.lease_ttl;
        let ttl_secs = self.ttl_secs();
        let me = ClientKey::from_wire(client);

        let state = self.inode_or_create(ino);
        match mode {
            LEASE_MODE_WRITE => {
                if let Some(existing) = &state.writer {
                    if existing != &me {
                        return AcquireOutcome::WriteConflict {
                            held_by_kind: existing.kind,
                            held_by_host: state.writer_diag_host.clone(),
                        };
                    }
                    // Same writer re-acquiring: refresh the deadline.
                }
                state.writer = Some(me);
                state.writer_diag_host = client.host.clone();
                state.writer_expires_at = Some(now + ttl);
            }
            LEASE_MODE_READ => {
                state.readers.insert(me, now + ttl);
            }
            _ => unreachable!(),
        }
        // Ensure an inbox exists so subsequent push targets find it.
        self.inboxes.entry(ClientKey::from_wire(client)).or_default();
        let (s_version, s_writer_present) = {
            let s = self.inodes.get(&ino).expect("just inserted");
            (s.version, s.writer.is_some())
        };
        // Seed the shadow on every acquire so a future release/revoke
        // that drops the entry resumes from this version, not 1.
        self.remember_version(ino, s_version);
        AcquireOutcome::Granted {
            version: s_version,
            writer_present: s_writer_present,
            ttl_secs,
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

    /// TTL revoke pass. Returns the list of inodes whose writer was
    /// revoked + the new `(ino, version)` so the caller (manager) can
    /// etcd-delete the persisted record; invalidation pushes are
    /// queued before this returns. Also drops expired reader leases
    /// silently.
    ///
    /// Ordering matters: a writer-revoke captures the reader set
    /// BEFORE expired readers are evicted in the same tick. A reader
    /// whose lease expired on the same boundary as the writer's TTL
    /// must still be notified — its inbox lives independently of its
    /// lease entry, so the notification survives until the reader
    /// reconnects (or until the inbox is dropped by `forget_client`).
    pub fn tick(&mut self, now: Instant) -> Vec<(u64, u64)> {
        let mut writer_revokes: Vec<(u64, u64, Vec<ClientKey>)> = Vec::new();
        let mut to_drop_inodes: Vec<u64> = Vec::new();

        for (&ino, state) in self.inodes.iter_mut() {
            // Writer expiry — capture pre-eviction reader set, then
            // bump version.
            if let Some(deadline) = state.writer_expires_at {
                if deadline <= now {
                    state.writer = None;
                    state.writer_diag_host.clear();
                    state.writer_expires_at = None;
                    state.version = state.version.wrapping_add(1);
                    let readers: Vec<ClientKey> = state.readers.keys().cloned().collect();
                    writer_revokes.push((ino, state.version, readers));
                }
            }
            // Reader expiry — silent drop, no invalidation push.
            state.readers.retain(|_, deadline| *deadline > now);
            if state.writer.is_none() && state.readers.is_empty() {
                to_drop_inodes.push(ino);
            }
        }

        for ino in to_drop_inodes {
            self.inodes.remove(&ino);
        }
        let revoked_pairs: Vec<(u64, u64)> = writer_revokes
            .iter()
            .map(|(ino, ver, _)| (*ino, *ver))
            .collect();
        for (ino, ver, readers) in writer_revokes {
            self.remember_version(ino, ver);
            for r in readers {
                self.push_invalidation(&r, ino, ver, InvalidationReason::LeaseRevoked);
            }
        }
        revoked_pairs
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

    pub fn snapshot_for(&self, ino: u64) -> Option<MgrInodeLeaseInfo> {
        self.snapshot(ino)
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

    /// F-ioring-lease-5 (coco P1 #6): precise rollback for a writer
    /// AcquireLease whose etcd persistence failed AFTER the in-memory
    /// grant landed. Restores ONLY the writer slot to `snapshot`'s
    /// shape; readers that subscribed during the etcd await are
    /// preserved (release() — the pre-fix rollback — wrongly bumped
    /// version + pushed a phantom WriterClosed to them).
    ///
    /// `snapshot = None` means "no entry existed pre-acquire" → clear
    /// the writer fields; if no readers joined, drop the empty entry.
    /// `snapshot = Some(state)` means "writer was already me, this
    /// was a refresh" → restore writer/host/deadline to the
    /// pre-acquire values exactly.
    ///
    /// Does NOT touch `version` because `acquire()` itself does not
    /// bump it; the only paths that bump version are `release()` and
    /// `tick()`'s revoke. (If a future refactor changes that, this
    /// helper MUST be updated to also revert `version`.)
    pub fn revert_writer_acquire(&mut self, ino: u64, snapshot: Option<InodeLeaseState>) {
        let Some(entry) = self.inodes.get_mut(&ino) else {
            return;
        };
        match snapshot {
            Some(s) => {
                entry.writer = s.writer;
                entry.writer_diag_host = s.writer_diag_host;
                entry.writer_expires_at = s.writer_expires_at;
            }
            None => {
                entry.writer = None;
                entry.writer_diag_host.clear();
                entry.writer_expires_at = None;
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
            let revoked: Vec<(u64, u64)> = {
                let mut reg = self.inode_leases.borrow_mut();
                reg.tick(now)
            };
            if revoked.is_empty() {
                continue;
            }
            let keys: Vec<String> = revoked
                .iter()
                .map(|(ino, _)| format!("{}{ino}", crate::INODE_LEASES_PREFIX))
                .collect();
            if let Some(etcd) = &self.etcd {
                if let Err(e) = etcd.put_and_delete_txn(vec![], keys).await {
                    tracing::warn!(
                        error = %e,
                        revoked = revoked.len(),
                        "F-ioring-lease-1: revoke-loop etcd delete failed; retry next tick"
                    );
                    continue;
                }
            }
            for (ino, ver) in revoked {
                tracing::info!(
                    ino,
                    new_version = ver,
                    "F-ioring-lease-1: writer lease revoked by TTL"
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
