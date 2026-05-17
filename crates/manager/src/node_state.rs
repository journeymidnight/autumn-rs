//! F211-A: per-extent-node auto-tracked liveness (Online ↔ Suspected).
//!
//! Pure in-memory; no etcd persistence. Re-derived on leader failover from
//! the next `df` poll (the new leader gets a 1-tick window to observe each
//! node before its judgement settles).
//!
//! **Crucially: there is no automatic `Down` transition.** Manager only
//! soft-marks the node `Suspected` once heartbeats lapse past
//! `AUTUMN_MGR_NODE_SUSPECTED_TIMEOUT_SECS` (default 10 s). Hard fence
//! ("the operator confirms the node is dead, proceed to recovery + EC
//! abandon") is **always** an explicit operator action — `mgr_fence_node`
//! (F211-C) writes an etcd `node_override/<node_id>` row, which F211-D
//! reads to bump owner-lock revisions, and F211-E reads to gate recovery.
//!
//! This is the "manager provides facts, operator policy script decides"
//! split — see `docs/superpowers/plans/2026-05-17-f211-operator-driven-node-lifecycle.md`
//! for the design rationale and the HDFS decommission analogue.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Auto-tracked per-node state. There are deliberately only two states —
/// `Online` and `Suspected`. `Down` / `Fenced` are operator-driven and
/// persisted separately in `node_override/<id>` (F211-C).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeAutoState {
    Online,
    Suspected { since: Instant },
}

impl NodeAutoState {
    pub fn is_online(self) -> bool {
        matches!(self, NodeAutoState::Online)
    }

    pub fn is_suspected(self) -> bool {
        matches!(self, NodeAutoState::Suspected { .. })
    }
}

/// In-memory tracker. Single owner is `AutumnManager.node_states`.
pub struct NodeStateTracker {
    states: HashMap<u64, NodeAutoState>,
    last_ok: HashMap<u64, Instant>,
    soft_timeout: Duration,
}

impl Default for NodeStateTracker {
    fn default() -> Self {
        Self::new(Self::default_soft_timeout())
    }
}

impl NodeStateTracker {
    pub fn new(soft_timeout: Duration) -> Self {
        Self {
            states: HashMap::new(),
            last_ok: HashMap::new(),
            soft_timeout,
        }
    }

    /// Read the configured soft timeout. Used by the health-report RPC
    /// so the operator sees the gating threshold alongside last-seen.
    pub fn soft_timeout(&self) -> Duration {
        self.soft_timeout
    }

    /// Resolve the soft-timeout from env, defaulting to 10 s. Clamped to
    /// >= 1 to avoid pathological values that would race the df cadence.
    pub fn default_soft_timeout() -> Duration {
        let secs = std::env::var("AUTUMN_MGR_NODE_SUSPECTED_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(10)
            .max(1);
        Duration::from_secs(secs)
    }

    /// Successful heartbeat (df ok, register, etc.). Flip the node to
    /// `Online` unconditionally and refresh `last_ok`.
    pub fn on_heartbeat_ok(&mut self, node_id: u64) {
        self.last_ok.insert(node_id, Instant::now());
        self.states.insert(node_id, NodeAutoState::Online);
    }

    /// Heartbeat failure. Mark `Suspected` if (a) we already have a
    /// last_ok stamp and the soft timeout has elapsed since it, or
    /// (b) this is the first signal we've ever had for the node
    /// (defensive — registration should always touch last_ok first,
    /// but make sure a never-seen-then-failed node still surfaces).
    ///
    /// `Suspected → Suspected` is a no-op (keeps the original
    /// `since` timestamp so the operator sees "how long has it been
    /// flaky").
    pub fn on_heartbeat_fail(&mut self, node_id: u64) {
        let now = Instant::now();
        let elapsed_since_ok = self
            .last_ok
            .get(&node_id)
            .map(|t| now.duration_since(*t))
            .unwrap_or(Duration::MAX);
        let cur = self.states.get(&node_id).copied();
        match cur {
            Some(NodeAutoState::Suspected { .. }) => {}
            Some(NodeAutoState::Online) | None => {
                if elapsed_since_ok >= self.soft_timeout {
                    self.states
                        .insert(node_id, NodeAutoState::Suspected { since: now });
                }
            }
        }
    }

    /// Periodic tick — promote stale `Online` entries even without an
    /// explicit failure call (defensive against missed failure paths).
    pub fn tick(&mut self) {
        let now = Instant::now();
        let timeout = self.soft_timeout;
        let stale_ids: Vec<u64> = self
            .last_ok
            .iter()
            .filter_map(|(id, last)| {
                if now.duration_since(*last) >= timeout {
                    let cur = self.states.get(id).copied();
                    if matches!(cur, Some(NodeAutoState::Online) | None) {
                        return Some(*id);
                    }
                }
                None
            })
            .collect();
        for id in stale_ids {
            self.states
                .insert(id, NodeAutoState::Suspected { since: now });
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
    /// if the node has never produced one.
    pub fn last_heartbeat_secs_ago(&self, node_id: u64) -> Option<u64> {
        self.last_ok
            .get(&node_id)
            .map(|t| t.elapsed().as_secs())
    }

    /// Suspected-window age in seconds, or `None` if not Suspected.
    pub fn suspected_age_secs(&self, node_id: u64) -> Option<u64> {
        match self.states.get(&node_id) {
            Some(NodeAutoState::Suspected { since }) => Some(since.elapsed().as_secs()),
            _ => None,
        }
    }

    /// Snapshot: `(node_id, state, last_heartbeat_secs_ago)` for every
    /// tracked node. Used by `mgr_list_node_states` (F211-B).
    pub fn snapshot(&self) -> Vec<(u64, NodeAutoState, Option<u64>)> {
        let mut out = Vec::with_capacity(self.states.len());
        for (id, st) in self.states.iter() {
            out.push((*id, *st, self.last_ok.get(id).map(|t| t.elapsed().as_secs())));
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
    }

    #[cfg(test)]
    pub(crate) fn _test_set_last_ok(&mut self, node_id: u64, t: Instant) {
        self.last_ok.insert(node_id, t);
        self.states.insert(node_id, NodeAutoState::Online);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn online_after_ok() {
        let mut t = NodeStateTracker::new(Duration::from_secs(10));
        t.on_heartbeat_ok(7);
        assert_eq!(t.state_of(7), NodeAutoState::Online);
        assert!(t.last_heartbeat_secs_ago(7).is_some());
    }

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

    #[test]
    fn never_seen_node_returns_online_default() {
        let t = NodeStateTracker::new(Duration::from_secs(10));
        assert_eq!(t.state_of(99), NodeAutoState::Online);
        assert!(t.last_heartbeat_secs_ago(99).is_none());
    }

    #[test]
    fn tick_promotes_stale_online_to_suspected() {
        let mut t = NodeStateTracker::new(Duration::from_secs(0));
        // Backdate last_ok so the tick condition matches.
        t._test_set_last_ok(
            7,
            Instant::now()
                .checked_sub(Duration::from_secs(60))
                .unwrap_or_else(Instant::now),
        );
        t.tick();
        assert!(t.state_of(7).is_suspected());
    }

    #[test]
    fn drop_node_removes_state() {
        let mut t = NodeStateTracker::new(Duration::from_secs(10));
        t.on_heartbeat_ok(7);
        t.drop_node(7);
        // Defaults to Online (never-seen branch) — verifies the entry
        // is gone, not stale.
        assert_eq!(t.state_of(7), NodeAutoState::Online);
        assert!(t.last_heartbeat_secs_ago(7).is_none());
    }

    #[test]
    fn snapshot_is_sorted_by_id() {
        let mut t = NodeStateTracker::new(Duration::from_secs(10));
        t.on_heartbeat_ok(5);
        t.on_heartbeat_ok(1);
        t.on_heartbeat_ok(9);
        let snap = t.snapshot();
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].0, 1);
        assert_eq!(snap[1].0, 5);
        assert_eq!(snap[2].0, 9);
    }
}
