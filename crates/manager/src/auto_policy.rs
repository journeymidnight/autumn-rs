//! F-DASH-IN-MGR — auto-policy pure decision helpers (M1) + controller (M2).
//!
//! M1 lands ONLY the pure functions ported verbatim from the retired Python
//! `python/dashboard/autumn_dashboard.py`: `policy_kind_str`,
//! `describe_candidate`, `candidate_to_cmd`, `cooldown_key`. The dashboard's
//! `/api/overview` advisories are rendered through them; M2's leader-fenced
//! controller (`decide_actions` + the tick loop + etcd config) will reuse the
//! SAME functions, so proving them here is the ground floor for M2.
//!
//! These operate on `PolicyCandidate` (the entries in the manager's
//! `advisory_cache`, populated by `policy_tick_loop`). They are the exact
//! kind→actuation mapping the Python controller used; keeping them byte-for-byte
//! faithful is what makes the in-manager controller a behavior-preserving
//! replacement (M2).

use std::collections::{HashMap, HashSet, VecDeque};

use autumn_rpc::manager_rpc::{
    AutoPolicyLogEntry, MgrAutoPolicyConfig, MgrAutoPolicyCooldowns, MgrAutoPolicyEntry,
    PolicyCandidate, POLICY_KIND_EC, POLICY_KIND_GC, POLICY_KIND_HOT_COLD, POLICY_KIND_MAJOR_COMPACT,
    POLICY_KIND_MERGE, POLICY_KIND_MINOR_COMPACT, POLICY_KIND_SPLIT,
};

/// Rolling action-log cap (leader-local, in-memory — not persisted).
const ACTION_LOG_CAP: usize = 100;

/// In-manager controller state: the mode + active policy + custom policies +
/// per-target cooldowns + a rolling action log. Config (mode/active/custom) is
/// etcd-persisted (`autoPolicy/config`, leader-fenced) and cooldowns to
/// `autoPolicy/cooldowns`, replayed on leader promotion so the active policy
/// survives failover. The action log is leader-local (not persisted).
pub(crate) struct AutoPolicyState {
    pub mode: AutoPolicyMode,
    pub active: String,
    /// CUSTOM policies only; presets come from `preset_policies()`.
    pub custom: Vec<MgrAutoPolicyEntry>,
    pub cooldowns: HashMap<String, i64>,
    pub log: VecDeque<AutoPolicyLogEntry>,
    /// Epoch-seconds the loop last launched an actuation decision; it only
    /// re-decides every active-policy `interval_sec`.
    pub last_tick_at: i64,
    /// True while an `autopolicy_set` is between its (validated) in-memory
    /// compute and its etcd persist — serializes concurrent config mutations so
    /// a second operator action can't interleave + lose the first (coco P1).
    pub updating: bool,
}

impl Default for AutoPolicyState {
    fn default() -> Self {
        AutoPolicyState {
            mode: AutoPolicyMode::Off,
            active: String::new(),
            custom: Vec::new(),
            cooldowns: HashMap::new(),
            log: VecDeque::new(),
            last_tick_at: 0,
            updating: false,
        }
    }
}

/// Business-safe bounds for the loop's cadence math (a hostile/corrupted
/// `u64` interval/cooldown would wrap the loop's `i64` conversion negative and
/// bypass the interval/cooldown gates — coco P1).
pub(crate) const MAX_INTERVAL_SEC: u64 = 86_400;
pub(crate) const MAX_COOLDOWN_SEC: u64 = 86_400;
pub(crate) const MAX_ACTIONS_CAP: u32 = 100;

/// Clamp a (possibly hostile / corrupted) custom policy entry to safe bounds —
/// applied on every UPSERT and on every replay of a persisted config.
pub(crate) fn sanitize_entry(e: &mut MgrAutoPolicyEntry) {
    e.switches.truncate(5);
    while e.switches.len() < 5 {
        e.switches.push(false);
    }
    e.interval_sec = e.interval_sec.clamp(2, MAX_INTERVAL_SEC);
    e.cooldown_sec = e.cooldown_sec.min(MAX_COOLDOWN_SEC);
    e.max_actions = e.max_actions.clamp(1, MAX_ACTIONS_CAP);
}

impl AutoPolicyState {
    /// Presets (compiled-in) + custom, for display / lookup.
    pub fn all_policies(&self) -> Vec<MgrAutoPolicyEntry> {
        let mut v = preset_policies();
        v.extend(self.custom.iter().cloned());
        v
    }

    /// Look up a policy by name — presets first, then custom.
    pub fn find_policy(&self, name: &str) -> Option<MgrAutoPolicyEntry> {
        preset_policies()
            .into_iter()
            .find(|p| p.name == name)
            .or_else(|| self.custom.iter().find(|p| p.name == name).cloned())
    }

    /// Push a newest-first action-log entry (capped).
    pub fn record(&mut self, ts: i64, level: &str, msg: String) {
        self.log.push_front(AutoPolicyLogEntry {
            ts,
            level: level.to_string(),
            msg,
        });
        while self.log.len() > ACTION_LOG_CAP {
            self.log.pop_back();
        }
    }

    pub fn load_config(&mut self, c: MgrAutoPolicyConfig) {
        self.mode = AutoPolicyMode::from_u8(c.mode);
        self.active = c.active;
        self.custom = c.policies;
        // Clamp on replay: a config persisted by an older/other build (or a
        // hand-edited etcd value) must not bypass the loop's cadence gates.
        for e in &mut self.custom {
            sanitize_entry(e);
        }
    }

    pub fn to_cooldowns(&self) -> MgrAutoPolicyCooldowns {
        MgrAutoPolicyCooldowns {
            entries: self.cooldowns.iter().map(|(k, v)| (k.clone(), *v)).collect(),
        }
    }

    pub fn load_cooldowns(&mut self, c: MgrAutoPolicyCooldowns) {
        self.cooldowns = c.entries.into_iter().collect();
    }
}

/// Controller lifecycle — a state machine, NOT a bool ([[feedback_state_machine_not_bool]]).
/// `Off` = nothing runs (a fresh cluster stays pure-mechanism, F203). `DryRun` =
/// the loop runs and logs "would: …" but never actuates. `Armed` = actuates, and
/// is only honored when the process was started with `--dashboard-allow-mutations`
/// (else it degrades to DryRun with a WARN). Byte values are wire/etcd-stable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AutoPolicyMode {
    Off,
    DryRun,
    Armed,
}

impl AutoPolicyMode {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            AutoPolicyMode::Off => 0,
            AutoPolicyMode::DryRun => 1,
            AutoPolicyMode::Armed => 2,
        }
    }
    pub(crate) fn from_u8(b: u8) -> Self {
        match b {
            2 => AutoPolicyMode::Armed,
            1 => AutoPolicyMode::DryRun,
            _ => AutoPolicyMode::Off,
        }
    }
}

// The 5 friendly UI switches, in order, are [split, ec, compact, gc, merge]
// (Python SWITCH_ORDER) — encoded positionally in MgrAutoPolicyEntry.switches
// and consumed by kinds_from_switches + the dashboard switches_to_dict.

/// Build a built-in preset (`builtin=true`, never persisted). Switches are
/// [split, ec, compact, gc, merge] per `SWITCH_ORDER`.
fn preset(
    name: &str,
    desc: &str,
    switches: [bool; 5],
    interval_sec: u64,
    cooldown_sec: u64,
    max_actions: u32,
) -> MgrAutoPolicyEntry {
    MgrAutoPolicyEntry {
        name: name.to_string(),
        desc: desc.to_string(),
        switches: switches.to_vec(),
        interval_sec,
        cooldown_sec,
        max_actions,
        builtin: true,
    }
}

/// The built-in presets, safest → most aggressive (Python `PRESET_POLICIES`).
pub(crate) fn preset_policies() -> Vec<MgrAutoPolicyEntry> {
    vec![
        preset("gc-only", "Reclaim space only (GC)", [false, false, false, true, false], 30, 120, 2),
        preset("maintenance", "GC + compaction, no topology change", [false, false, true, true, false], 30, 180, 2),
        preset("space-reclaim", "GC + auto-EC, space-first", [false, true, false, true, false], 20, 120, 3),
        preset("balanced", "GC + compaction + EC (recommended steady-state)", [false, true, true, true, false], 30, 240, 2),
        preset("aggressive", "Full auto: incl. split / merge topology changes", [true, true, true, true, true], 20, 180, 3),
    ]
}

/// Is `name` a built-in preset (which a custom entry may not shadow / delete)?
pub(crate) fn is_preset_name(name: &str) -> bool {
    preset_policies().iter().any(|p| p.name == name)
}

/// Expand a switch set to the actionable candidate kinds it enables (Python
/// `kinds_from_switches`). compact ⇒ major + minor. Reads the first 5 switches
/// by `SWITCH_ORDER`; a shorter Vec treats absent switches as off.
pub(crate) fn kinds_from_switches(switches: &[bool]) -> HashSet<u8> {
    let on = |i: usize| switches.get(i).copied().unwrap_or(false);
    let mut out = HashSet::new();
    if on(0) {
        out.insert(POLICY_KIND_SPLIT);
    }
    if on(1) {
        out.insert(POLICY_KIND_EC);
    }
    if on(2) {
        out.insert(POLICY_KIND_MAJOR_COMPACT);
        out.insert(POLICY_KIND_MINOR_COMPACT);
    }
    if on(3) {
        out.insert(POLICY_KIND_GC);
    }
    if on(4) {
        out.insert(POLICY_KIND_MERGE);
    }
    out
}

/// Actuation priority (Python `order`): split (relief valve — spreads load) first,
/// then cheap upkeep (gc/minor/major), then ec, then merge LAST (concentrates load
/// onto one core — [[feedback_auto_split_before_merge]]). Lower = higher priority.
fn kind_priority(kind: u8) -> u8 {
    match kind {
        POLICY_KIND_SPLIT => 0,
        POLICY_KIND_GC => 1,
        POLICY_KIND_MINOR_COMPACT => 2,
        POLICY_KIND_MAJOR_COMPACT => 3,
        POLICY_KIND_EC => 4,
        POLICY_KIND_MERGE => 5,
        _ => 9,
    }
}

/// Pure decision (Python `decide_actions`): from the advisory candidates, pick up
/// to `max_actions` to actuate this tick — filtered by `enabled` kinds, mapped to
/// a command, de-duplicated per cooldown key, and dropped if still inside their
/// per-target cooldown window. Returns `(candidate, cmd, cooldown_key)` triples in
/// priority order. Client-side cooldown is defense-in-depth on top of the
/// manager's own per-kind cooldowns + inflight flags.
pub(crate) fn decide_actions(
    candidates: &[PolicyCandidate],
    cooldowns: &HashMap<String, i64>,
    enabled: &HashSet<u8>,
    now: i64,
    cooldown_secs: i64,
    max_actions: usize,
) -> Vec<(PolicyCandidate, Vec<String>, String)> {
    let mut ordered: Vec<&PolicyCandidate> = candidates.iter().collect();
    ordered.sort_by_key(|c| kind_priority(c.kind)); // stable — ties keep order
    let mut picked: Vec<(PolicyCandidate, Vec<String>, String)> = Vec::new();
    let mut seen_keys: HashSet<String> = HashSet::new();
    for c in ordered {
        if picked.len() >= max_actions {
            break;
        }
        if !enabled.contains(&c.kind) {
            continue;
        }
        let Some(cmd) = candidate_to_cmd(c) else {
            continue;
        };
        let key = cooldown_key(c);
        if seen_keys.contains(&key) {
            continue;
        }
        let last = cooldowns.get(&key).copied().unwrap_or(0);
        if now - last < cooldown_secs {
            continue; // still cooling down from a recent actuation
        }
        seen_keys.insert(key.clone());
        picked.push((c.clone(), cmd, key));
    }
    picked
}

/// Lowercase kind string — matches `autumn-op`'s policy-candidates JSON and the
/// dashboard page (`autumn_op/main.rs` kind map).
pub(crate) fn policy_kind_str(kind: u8) -> &'static str {
    match kind {
        POLICY_KIND_SPLIT => "split",
        POLICY_KIND_MERGE => "merge",
        POLICY_KIND_GC => "gc",
        POLICY_KIND_MAJOR_COMPACT => "major",
        POLICY_KIND_HOT_COLD => "hotcold",
        POLICY_KIND_MINOR_COMPACT => "minor",
        POLICY_KIND_EC => "ec",
        _ => "?",
    }
}

/// Human-readable one-liner for a candidate (Python `describe_candidate`):
/// `"<kind> <target> <reason>"`. EC targets an extent (in `secondary_part_id`);
/// merge shows `survivor<-victim`; everything else targets `primary_part_id`.
pub(crate) fn describe_candidate(c: &PolicyCandidate) -> String {
    let target = match c.kind {
        POLICY_KIND_EC => format!("extent {}", c.secondary_part_id),
        POLICY_KIND_MERGE => format!("part {}<-{}", c.primary_part_id, c.secondary_part_id),
        _ => format!("part {}", c.primary_part_id),
    };
    format!("{:<6} {:<18} {}", policy_kind_str(c.kind), target, c.reason)
}

/// Map a candidate to the `autumn-op` actuation command, or `None` if it is
/// advisory-only / missing its target (Python `candidate_to_cmd`). EC carries
/// the extent in `secondary_part_id` (primary=0); split/gc/compact use
/// `primary_part_id`; merge = primary survivor + secondary victim; major/minor
/// both map to `compact` (the PS picks the tier). `hotcold`/unknown → `None`.
pub(crate) fn candidate_to_cmd(c: &PolicyCandidate) -> Option<Vec<String>> {
    match c.kind {
        POLICY_KIND_EC => {
            if c.secondary_part_id == 0 {
                return None;
            }
            Some(vec![
                "force-ec-convert".to_string(),
                "--extent".to_string(),
                c.secondary_part_id.to_string(),
            ])
        }
        POLICY_KIND_SPLIT => Some(vec!["split".to_string(), c.primary_part_id.to_string()]),
        POLICY_KIND_MERGE => {
            if c.secondary_part_id == 0 {
                return None;
            }
            Some(vec![
                "merge".to_string(),
                c.primary_part_id.to_string(),
                c.secondary_part_id.to_string(),
            ])
        }
        POLICY_KIND_GC => Some(vec!["gc".to_string(), c.primary_part_id.to_string()]),
        POLICY_KIND_MAJOR_COMPACT | POLICY_KIND_MINOR_COMPACT => {
            Some(vec!["compact".to_string(), c.primary_part_id.to_string()])
        }
        _ => None, // hotcold / unknown → advisory only
    }
}

/// Stable per-(kind, target) key for client-side cooldown tracking (Python
/// `cooldown_key`).
pub(crate) fn cooldown_key(c: &PolicyCandidate) -> String {
    match c.kind {
        POLICY_KIND_EC => format!("ec:{}", c.secondary_part_id),
        POLICY_KIND_MERGE => {
            format!("merge:{}:{}", c.primary_part_id, c.secondary_part_id)
        }
        _ => format!("{}:{}", policy_kind_str(c.kind), c.primary_part_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(kind: u8, prim: u64, sec: u64) -> PolicyCandidate {
        PolicyCandidate {
            kind,
            primary_part_id: prim,
            secondary_part_id: sec,
            reason: "qps high".to_string(),
            size_bytes: 0,
            req_per_sec: 0,
            imm_full_per_sec: 0,
            same_ps: false,
            last_op_at: 0,
        }
    }

    #[test]
    fn candidate_to_cmd_maps_every_actionable_kind() {
        // split/gc/major/minor use primary; ec uses secondary as the EXTENT id;
        // merge = survivor + victim.
        assert_eq!(
            candidate_to_cmd(&cand(POLICY_KIND_SPLIT, 7, 0)),
            Some(vec!["split".into(), "7".into()])
        );
        assert_eq!(
            candidate_to_cmd(&cand(POLICY_KIND_GC, 7, 0)),
            Some(vec!["gc".into(), "7".into()])
        );
        assert_eq!(
            candidate_to_cmd(&cand(POLICY_KIND_MAJOR_COMPACT, 7, 0)),
            Some(vec!["compact".into(), "7".into()])
        );
        assert_eq!(
            candidate_to_cmd(&cand(POLICY_KIND_MINOR_COMPACT, 7, 0)),
            Some(vec!["compact".into(), "7".into()])
        );
        assert_eq!(
            candidate_to_cmd(&cand(POLICY_KIND_EC, 0, 42)),
            Some(vec!["force-ec-convert".into(), "--extent".into(), "42".into()])
        );
        assert_eq!(
            candidate_to_cmd(&cand(POLICY_KIND_MERGE, 3, 4)),
            Some(vec!["merge".into(), "3".into(), "4".into()])
        );
    }

    #[test]
    fn candidate_to_cmd_none_for_advisory_only_or_missing_target() {
        assert_eq!(candidate_to_cmd(&cand(POLICY_KIND_HOT_COLD, 1, 2)), None);
        assert_eq!(candidate_to_cmd(&cand(POLICY_KIND_EC, 0, 0)), None); // no extent
        assert_eq!(candidate_to_cmd(&cand(POLICY_KIND_MERGE, 3, 0)), None); // no victim
        assert_eq!(candidate_to_cmd(&cand(99, 1, 2)), None); // unknown kind
    }

    #[test]
    fn cooldown_key_is_per_kind_target() {
        assert_eq!(cooldown_key(&cand(POLICY_KIND_SPLIT, 7, 0)), "split:7");
        assert_eq!(cooldown_key(&cand(POLICY_KIND_GC, 7, 0)), "gc:7");
        assert_eq!(cooldown_key(&cand(POLICY_KIND_EC, 0, 42)), "ec:42");
        assert_eq!(cooldown_key(&cand(POLICY_KIND_MERGE, 3, 4)), "merge:3:4");
    }

    #[test]
    fn describe_candidate_targets_extent_for_ec_and_pair_for_merge() {
        assert!(describe_candidate(&cand(POLICY_KIND_EC, 0, 42)).contains("extent 42"));
        assert!(describe_candidate(&cand(POLICY_KIND_MERGE, 3, 4)).contains("part 3<-4"));
        assert!(describe_candidate(&cand(POLICY_KIND_SPLIT, 7, 0)).contains("part 7"));
    }

    #[test]
    fn kinds_from_switches_expands_compact_to_major_and_minor() {
        // [split, ec, compact, gc, merge]
        let ks = kinds_from_switches(&[false, false, true, false, false]);
        assert!(ks.contains(&POLICY_KIND_MAJOR_COMPACT));
        assert!(ks.contains(&POLICY_KIND_MINOR_COMPACT));
        assert_eq!(ks.len(), 2);
        let all = kinds_from_switches(&[true, true, true, true, true]);
        assert_eq!(all.len(), 6); // split, ec, major, minor, gc, merge
        assert!(kinds_from_switches(&[false; 5]).is_empty());
    }

    #[test]
    fn presets_are_ordered_and_gc_only_enables_only_gc() {
        let ps = preset_policies();
        assert_eq!(ps.len(), 5);
        assert_eq!(ps[0].name, "gc-only");
        assert_eq!(ps[4].name, "aggressive");
        assert!(ps.iter().all(|p| p.builtin));
        // gc-only: only the gc switch (index 3).
        assert_eq!(ps[0].switches, vec![false, false, false, true, false]);
        assert_eq!(kinds_from_switches(&ps[0].switches), {
            let mut s = HashSet::new();
            s.insert(POLICY_KIND_GC);
            s
        });
        // aggressive turns everything on.
        assert_eq!(ps[4].switches, vec![true; 5]);
    }

    #[test]
    fn decide_actions_priority_cap_and_dedup() {
        let enabled: HashSet<u8> = [POLICY_KIND_SPLIT, POLICY_KIND_MERGE, POLICY_KIND_GC]
            .into_iter()
            .collect();
        let cds = HashMap::new();
        // merge first in input, split last — but split must be picked FIRST
        // (relief valve before merge).
        let cands = vec![
            cand(POLICY_KIND_MERGE, 3, 4),
            cand(POLICY_KIND_GC, 5, 0),
            cand(POLICY_KIND_SPLIT, 9, 0),
        ];
        let out = decide_actions(&cands, &cds, &enabled, 1000, 300, 2);
        assert_eq!(out.len(), 2, "capped at max_actions");
        assert_eq!(out[0].0.kind, POLICY_KIND_SPLIT, "split before gc before merge");
        assert_eq!(out[1].0.kind, POLICY_KIND_GC);
    }

    #[test]
    fn decide_actions_respects_enabled_filter_and_cooldown() {
        let enabled: HashSet<u8> = [POLICY_KIND_GC].into_iter().collect();
        let cands = vec![cand(POLICY_KIND_SPLIT, 1, 0), cand(POLICY_KIND_GC, 2, 0)];
        // split disabled → only gc considered.
        let out = decide_actions(&cands, &HashMap::new(), &enabled, 1000, 300, 5);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0.kind, POLICY_KIND_GC);
        // gc:2 actuated 100 s ago, cooldown 300 s → suppressed.
        let mut cds = HashMap::new();
        cds.insert("gc:2".to_string(), 900i64);
        let out2 = decide_actions(&cands, &cds, &enabled, 1000, 300, 5);
        assert!(out2.is_empty(), "still cooling down");
        // …but 400 s ago (> cooldown) → allowed.
        cds.insert("gc:2".to_string(), 600i64);
        assert_eq!(decide_actions(&cands, &cds, &enabled, 1000, 300, 5).len(), 1);
    }

    #[test]
    fn mode_roundtrips_through_u8() {
        for m in [AutoPolicyMode::Off, AutoPolicyMode::DryRun, AutoPolicyMode::Armed] {
            assert_eq!(AutoPolicyMode::from_u8(m.as_u8()), m);
        }
        assert_eq!(AutoPolicyMode::from_u8(99), AutoPolicyMode::Off); // unknown → Off
    }
}
