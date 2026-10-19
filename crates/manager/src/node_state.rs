//! per-extent-node auto-tracked liveness: `Suspend` (not heard from this
//! term), `Online`, `Suspected` (silent past the soft timeout).
//!
//! Pure in-memory; no etcd persistence. Every leader term starts every node
//! `Suspend` (`on_term_start`) and lets the df loop prove it. A node silent for
//! the soft timeout becomes `Suspected` — measured from its last answer, or,
//! when it has none this term, from when the term (or its registration)
//! began.
//!
//! There is no automatic `Down`: fencing is an operator action
//! (`node_override/<node_id>`). How these states combine with the overrides is
//! `node_class`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeAutoState {
    /// Not heard from yet this term: just registered, or a leader just took
    /// over. Becomes `Online` on its first df, `Suspected` after the soft
    /// timeout without one.
    Suspend,
    Online,
    Suspected {
        since: Instant,
    },
}

impl NodeAutoState {
    pub fn is_online(self) -> bool {
        matches!(self, NodeAutoState::Online)
    }

    pub fn is_suspected(self) -> bool {
        matches!(self, NodeAutoState::Suspected { .. })
    }

    pub fn is_suspend(self) -> bool {
        matches!(self, NodeAutoState::Suspend)
    }
}

/// In-memory tracker. Single owner is `AutumnManager.node_states`.
pub struct NodeStateTracker {
    states: HashMap<u64, NodeAutoState>,
    last_ok: HashMap<u64, Instant>,
    /// When a node with no answer this term started waiting for one.
    unproven_since: HashMap<u64, Instant>,
    soft_timeout: Duration,
}

impl NodeStateTracker {
    pub fn new(soft_timeout: Duration) -> Self {
        Self {
            states: HashMap::new(),
            last_ok: HashMap::new(),
            unproven_since: HashMap::new(),
            soft_timeout,
        }
    }

    /// Read the configured soft timeout. Used by the health-report RPC
    /// so the operator sees the gating threshold alongside last-seen.
    pub fn soft_timeout(&self) -> Duration {
        self.soft_timeout
    }

    /// A new leader term: nothing the previous term saw counts. Every node
    /// is `Suspend` until it answers this term's df.
    pub fn on_term_start(&mut self, node_ids: impl IntoIterator<Item = u64>) {
        let now = Instant::now();
        self.states.clear();
        self.last_ok.clear();
        self.unproven_since.clear();
        for id in node_ids {
            self.states.insert(id, NodeAutoState::Suspend);
            self.unproven_since.insert(id, now);
        }
    }

    /// The node answered (df ok, or it re-registered itself).
    pub fn on_heartbeat_ok(&mut self, node_id: u64) {
        self.last_ok.insert(node_id, Instant::now());
        self.unproven_since.remove(&node_id);
        self.states.insert(node_id, NodeAutoState::Online);
    }

    /// First registration: `Suspend` until its first df.
    pub fn on_register_first(&mut self, node_id: u64) {
        self.states.insert(node_id, NodeAutoState::Suspend);
        self.unproven_since.insert(node_id, Instant::now());
    }

    /// The node did not answer. `Suspected` once it has been silent for the
    /// soft timeout; `Suspected → Suspected` keeps the original `since`.
    pub fn on_heartbeat_fail(&mut self, node_id: u64) {
        if self.states.get(&node_id).is_some_and(|s| s.is_suspected()) {
            return;
        }
        let Some(silent_since) = self
            .last_ok
            .get(&node_id)
            .or_else(|| self.unproven_since.get(&node_id))
        else {
            return;
        };
        let now = Instant::now();
        if now.duration_since(*silent_since) >= self.soft_timeout {
            self.states
                .insert(node_id, NodeAutoState::Suspected { since: now });
        }
    }

    /// Resolve current auto state. Returns `Online` for a never-seen
    /// node (defensive — register_node always touches the tracker
    /// before any heartbeat could land).
    pub fn state_of(&self, node_id: u64) -> NodeAutoState {
        self.states
            .get(&node_id)
            .copied()
            .unwrap_or(NodeAutoState::Online)
    }

    /// Seconds since the most recent successful heartbeat, or `None`
    /// if the node has not produced one this term.
    pub fn last_heartbeat_secs_ago(&self, node_id: u64) -> Option<u64> {
        self.last_ok.get(&node_id).map(|t| t.elapsed().as_secs())
    }

    /// Suspected-window age in seconds, or `None` if not Suspected.
    pub fn suspected_age_secs(&self, node_id: u64) -> Option<u64> {
        match self.states.get(&node_id) {
            Some(NodeAutoState::Suspected { since }) => Some(since.elapsed().as_secs()),
            _ => None,
        }
    }

    /// test helper: seed a node directly into the `Suspend` state.
    #[cfg(test)]
    pub(crate) fn _test_register_first(&mut self, node_id: u64) {
        self.on_register_first(node_id);
    }

    /// Snapshot: `(node_id, state, last_heartbeat_secs_ago)` for every
    /// tracked node. Used by `mgr_list_node_states`.
    pub fn snapshot(&self) -> Vec<(u64, NodeAutoState, Option<u64>)> {
        let mut out = Vec::with_capacity(self.states.len());
        for (id, st) in self.states.iter() {
            out.push((
                *id,
                *st,
                self.last_ok.get(id).map(|t| t.elapsed().as_secs()),
            ));
        }
        // Stable order by id so test output / RPC responses are
        // deterministic.
        out.sort_by_key(|(id, _, _)| *id);
        out
    }

    /// Drop tracking for a removed node (called by `mgr_remove_node`).
    pub fn drop_node(&mut self, node_id: u64) {
        self.states.remove(&node_id);
        self.last_ok.remove(&node_id);
        self.unproven_since.remove(&node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fail_within_timeout_stays_online() {
        let mut t = NodeStateTracker::new(Duration::from_secs(10));
        t.on_heartbeat_ok(7);
        // Immediate failure — within the soft window.
        t.on_heartbeat_fail(7);
        assert_eq!(t.state_of(7), NodeAutoState::Online);
    }

    #[test]
    fn fail_after_timeout_marks_suspected() {
        let mut t = NodeStateTracker::new(Duration::from_secs(0));
        t.on_heartbeat_ok(7);
        // Soft timeout = 0 → next failure flips immediately.
        t.on_heartbeat_fail(7);
        assert!(t.state_of(7).is_suspected());
        assert!(t.suspected_age_secs(7).is_some());
    }

    #[test]
    fn ok_clears_suspected() {
        let mut t = NodeStateTracker::new(Duration::from_secs(0));
        t.on_heartbeat_ok(7);
        t.on_heartbeat_fail(7);
        assert!(t.state_of(7).is_suspected());
        t.on_heartbeat_ok(7);
        assert_eq!(t.state_of(7), NodeAutoState::Online);
        assert!(t.suspected_age_secs(7).is_none());
    }

    // ── Suspend state machine ───────────────────────────────────────

    /// A node that never answers is not left `Suspend` forever: the soft
    /// timeout counts from its registration.
    #[test]
    fn a_silent_registered_node_becomes_suspected() {
        let mut t = NodeStateTracker::new(Duration::from_secs(0));
        t.on_register_first(7);
        t.on_heartbeat_fail(7);
        assert!(t.state_of(7).is_suspected(), "got {:?}", t.state_of(7));
        let mut t = NodeStateTracker::new(Duration::from_secs(3600));
        t.on_register_first(7);
        t.on_heartbeat_fail(7);
        assert!(t.state_of(7).is_suspend(), "inside the timeout it waits");
    }

    /// A new term forgets what the previous one saw: an `Online` node is
    /// `Suspend` again until it answers, and a dead one becomes `Suspected`
    /// once the timeout has passed since the term began.
    #[test]
    fn a_new_term_starts_every_node_suspend() {
        let mut t = NodeStateTracker::new(Duration::from_secs(0));
        t.on_heartbeat_ok(1);
        t.on_heartbeat_ok(2);
        t.on_term_start([1, 2]);
        assert!(t.state_of(1).is_suspend());
        assert!(t.last_heartbeat_secs_ago(1).is_none());
        t.on_heartbeat_ok(1);
        t.on_heartbeat_fail(2);
        assert!(t.state_of(1).is_online());
        assert!(t.state_of(2).is_suspected());
    }
}
