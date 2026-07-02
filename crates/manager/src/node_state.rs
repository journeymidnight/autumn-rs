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
//! split (F211 design; HDFS decommission analogue).

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Auto-tracked per-node state. F214-B added `Suspend` — a registered
/// node that has never had a successful heartbeat. Distinct from
/// `Suspected` (which means "was alive and verified, now flaky") so the
/// operator-facing health report can distinguish "format ran but EN
/// never started" from "EN was running and crashed". `Down` / `Fenced`
/// remain operator-driven and live in `node_override/<id>` (F211-C).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeAutoState {
    /// F214-B: post-register, pre-first-df. The state machine entry
    /// point. Transitions to `Online` on the first successful heartbeat
    /// (df ok), or via operator re-register. Never transitions to
    /// `Suspected` — Suspended requires a prior verified-alive baseline.
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

    /// F214-B: first-time register seed. Sets the state to `Suspend` and
    /// does **not** touch `last_ok` — we have no first heartbeat yet, so
    /// any later `on_heartbeat_fail` defensively flips us to `Suspected`
    /// only after the soft timeout (its `last_ok` lookup falls through
    /// to `Duration::MAX`). The first successful df transitions to
    /// `Online` via `on_heartbeat_ok`.
    ///
    /// Re-register (operator re-runs format / register-node against an
    /// already-known address) goes through `on_heartbeat_ok` instead —
    /// the operator's explicit re-registration counts as vouching the
    /// node alive, matching the pre-F214 F211-A behaviour.
    pub fn on_register_first(&mut self, node_id: u64) {
        self.states.insert(node_id, NodeAutoState::Suspend);
        // No last_ok insert — see doc above.
    }

    /// Heartbeat failure. Mark `Suspected` only when we already have a
    /// verified-alive baseline (`last_ok` set) AND the soft timeout has
    /// elapsed. F214-B: a `Suspend` node — registered but never
    /// verified — stays `Suspend` on failure; never auto-promoted to
    /// `Suspected` because the "was alive, now flaky" framing requires
    /// the prior verified state.
    ///
    /// `Suspected → Suspected` is a no-op (keeps the original `since`
    /// timestamp so the operator sees "how long has it been flaky").
    pub fn on_heartbeat_fail(&mut self, node_id: u64) {
        let now = Instant::now();
        let elapsed_since_ok = match self.last_ok.get(&node_id) {
            Some(t) => now.duration_since(*t),
            None => return, // never verified → stay Suspend (or whatever caller set)
        };
        let cur = self.states.get(&node_id).copied();
        match cur {
            Some(NodeAutoState::Suspected { .. }) => {}
            Some(NodeAutoState::Suspend) => {} // F214-B: no auto-promotion
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
    /// F214-B: `Suspend` nodes (no `last_ok` ever) are not affected — the
    /// filter on `self.last_ok.iter()` naturally skips them.
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
        self.last_ok.get(&node_id).map(|t| t.elapsed().as_secs())
    }

    /// Suspected-window age in seconds, or `None` if not Suspected.
    pub fn suspected_age_secs(&self, node_id: u64) -> Option<u64> {
        match self.states.get(&node_id) {
            Some(NodeAutoState::Suspected { since }) => Some(since.elapsed().as_secs()),
            _ => None,
        }
    }

    /// F214-B test helper: seed a node directly into the `Suspend` state.
    #[cfg(test)]
    pub(crate) fn _test_register_first(&mut self, node_id: u64) {
        self.on_register_first(node_id);
    }

    /// F214-B: return the set of node_ids whose state is `Online`. Used
    /// by `select_nodes` to gate fresh extent allocation on a verified-
    /// alive baseline. Pre-F214-B, allocation gated only on
    /// `disk.online`, which is a per-disk-health signal — that left a
    /// 10-20 s "Pending" window after `register-node` where the
    /// allocation could target an EN whose binary had never started.
    pub fn online_node_ids(&self) -> std::collections::HashSet<u64> {
        self.states
            .iter()
            .filter_map(|(id, st)| if st.is_online() { Some(*id) } else { None })
            .collect()
    }

    /// Snapshot: `(node_id, state, last_heartbeat_secs_ago)` for every
    /// tracked node. Used by `mgr_list_node_states` (F211-B).
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

    // ── F214-B ──────────────────────────────────────────────────────

    #[test]
    fn register_first_seeds_suspend() {
        let mut t = NodeStateTracker::new(Duration::from_secs(10));
        t.on_register_first(7);
        assert_eq!(t.state_of(7), NodeAutoState::Suspend);
        assert!(t.state_of(7).is_suspend());
        assert!(!t.state_of(7).is_online());
        assert!(t.last_heartbeat_secs_ago(7).is_none());
    }

    #[test]
    fn suspend_then_df_ok_transitions_to_online() {
        let mut t = NodeStateTracker::new(Duration::from_secs(10));
        t.on_register_first(7);
        assert!(t.state_of(7).is_suspend());
        t.on_heartbeat_ok(7);
        assert_eq!(t.state_of(7), NodeAutoState::Online);
        assert!(t.last_heartbeat_secs_ago(7).is_some());
    }

    #[test]
    fn suspend_does_not_auto_promote_to_suspected() {
        let mut t = NodeStateTracker::new(Duration::from_secs(0));
        t.on_register_first(7);
        // Even with zero soft_timeout, a failure on a Suspend node
        // must NOT promote to Suspected — Suspected semantics require
        // a prior verified-alive baseline.
        t.on_heartbeat_fail(7);
        assert!(t.state_of(7).is_suspend(), "got {:?}", t.state_of(7));
        // tick() likewise does not touch Suspend (no last_ok to iterate).
        t.tick();
        assert!(t.state_of(7).is_suspend(), "got {:?}", t.state_of(7));
    }

    #[test]
    fn re_register_skips_suspend_via_heartbeat_ok() {
        let mut t = NodeStateTracker::new(Duration::from_secs(10));
        // Simulate a node already known and verified.
        t.on_register_first(7);
        t.on_heartbeat_ok(7);
        // Operator re-runs `register-node` — handler uses on_heartbeat_ok.
        t.on_heartbeat_ok(7);
        assert_eq!(t.state_of(7), NodeAutoState::Online);
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
