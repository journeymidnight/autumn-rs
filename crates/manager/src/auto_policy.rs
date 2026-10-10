//! auto-policy pure decision helpers (M1) + controller (M2).
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

use std::collections::{HashMap, HashSet};

use autumn_rpc::manager_rpc::{
    MgrAutoPolicyConfig, MgrAutoPolicyCooldowns, MgrAutoPolicyEntry,
    OpSubmitReq, PolicyCandidate, OP_KIND_COMPACT, OP_KIND_EC_CONVERT, OP_KIND_GC, OP_KIND_MERGE,
    OP_KIND_REBALANCE, OP_KIND_REPAIR, OP_KIND_SCRUB, OP_KIND_SPLIT, POLICY_KIND_EC,
    POLICY_KIND_GC, POLICY_KIND_HOT_COLD, POLICY_KIND_MAJOR_COMPACT, POLICY_KIND_MERGE,
    POLICY_KIND_MINOR_COMPACT, POLICY_KIND_REBALANCE, POLICY_KIND_REPAIR, POLICY_KIND_SCRUB,
    POLICY_KIND_SPLIT, SCRUB_POLICY_INTERVAL_SEC,
};

/// In-manager controller state: the mode + active policy + custom policies +
/// per-target cooldowns. Config (mode/active/custom) is etcd-persisted
/// (`autoPolicy/config`, leader-fenced) and cooldowns to `autoPolicy/cooldowns`,
/// replayed on leader promotion so the active policy survives failover. An
/// armed policy's actions are op-ledger ops, listed and kept in history like
/// an operator's.
pub(crate) struct AutoPolicyState {
    pub mode: AutoPolicyMode,
    pub active: String,
    /// CUSTOM policies only; presets come from `preset_policies()`.
    pub custom: Vec<MgrAutoPolicyEntry>,
    pub cooldowns: HashMap<String, i64>,
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

/// Non-configurable floor on how often the auto-policy loop may ACTUATE a
/// cluster-scoped `rebalance` (coco P1): a policy's `cooldown_sec` can be set to
/// 0, but rebalance reopens partitions, so it must never fire more than once per
/// this window regardless of config. Backstops the advisory-side
/// `rebalance_cooldown_sec` (which gates EMISSION, not re-actuation of a cached
/// candidate). Deliberately below the 120 s advisory default so it never
/// over-throttles a legitimately-armed rebalance policy.
pub(crate) const REBALANCE_MIN_ACTUATION_COOLDOWN_SEC: i64 = 60;

/// The same non-configurable floor, for COMPACTION (`compact <part>`, a major
/// compaction; minor compaction is the PS's own and never actuated). A compaction
/// rewrites every SST of the partition, and the loop actuates from a CACHED
/// candidate list rebuilt only on the 60 s policy tick — so a policy with
/// `cooldown_sec = 0` and `interval_sec = 2` would re-issue the same cached row
/// ~30 times inside one window.
///
/// Emission-side cooldowns do not cover this: `unblocking_compact` does not
/// suppress on `compact_cooldown_sec` (it keys on a FLAG, not a debt level —
/// see its doc), and even where an advisory does suppress, a row it already
/// emitted stays in the cache for the rest of the window.
///
/// 60 s, matching rebalance and deliberately below every preset's
/// `cooldown_sec` (120-240 s): it can only catch a misconfiguration, never
/// throttle a correctly configured policy.
pub(crate) const COMPACT_MIN_ACTUATION_COOLDOWN_SEC: i64 = 60;

/// Clamp a (possibly hostile / corrupted) custom policy entry to safe bounds —
/// applied on every UPSERT and on every replay of a persisted config.
pub(crate) fn sanitize_entry(e: &mut MgrAutoPolicyEntry) {
    e.switches.truncate(SWITCHES);
    while e.switches.len() < SWITCHES {
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

    /// Errs on a mode this build does not have (`1`, the removed observe
    /// mode): `migratev1_v2` converts it to Off.
    pub fn load_config(&mut self, c: MgrAutoPolicyConfig) -> Result<(), String> {
        self.mode = AutoPolicyMode::from_u8(c.mode).ok_or_else(|| {
            format!(
                "autoPolicy/config mode {} is not a mode this build has (1 was the \
                 removed observe mode); run migratev1_v2, which converts it to off",
                c.mode
            )
        })?;
        self.active = c.active;
        self.custom = c.policies;
        // Clamp on replay: a config persisted by an older/other build (or a
        // hand-edited etcd value) must not bypass the loop's cadence gates.
        for e in &mut self.custom {
            sanitize_entry(e);
        }
        Ok(())
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
/// `Off` = nothing runs (a fresh cluster stays pure-mechanism). `Armed` =
/// actuates. The mode is the whole gate — arming is per-policy, with no
/// separate process-wide flag. Byte values are wire/etcd-stable; `1` was the
/// removed observe mode and is refused, never read as another mode.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AutoPolicyMode {
    Off,
    Armed,
}

impl AutoPolicyMode {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            AutoPolicyMode::Off => 0,
            AutoPolicyMode::Armed => 2,
        }
    }
    pub(crate) fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(AutoPolicyMode::Off),
            2 => Some(AutoPolicyMode::Armed),
            _ => None,
        }
    }
}

// The friendly UI switches, in order, are [split, ec, compact, gc, merge,
// rebalance, repair, scrub] (SWITCH_ORDER) — encoded positionally in
// MgrAutoPolicyEntry.switches and consumed by kinds_from_switches + the
// dashboard switches_to_dict. Switches are only ever APPENDED; a shorter
// persisted Vec (a config written before a switch existed) reads the absent
// switches as off.

/// Number of switches (`SWITCH_ORDER`).
pub(crate) const SWITCHES: usize = 8;

/// Build a built-in preset (`builtin=true`, never persisted). Switches are
/// [split, ec, compact, gc, merge, rebalance, repair, scrub] per `SWITCH_ORDER`.
fn preset(
    name: &str,
    desc: &str,
    switches: [bool; SWITCHES],
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
    // switches = [split, ec, compact, gc, merge, rebalance, repair, scrub]
    vec![
        preset("gc-only", "Reclaim space only (GC)", [false, false, false, true, false, false, false, false], 30, 120, 2),
        preset("maintenance", "GC + compaction + extent repair + weekly scrub, no topology change", [false, false, true, true, false, false, true, true], 30, 180, 2),
        preset("space-reclaim", "GC + auto-EC, space-first", [false, true, false, true, false, false, false, false], 20, 120, 3),
        preset("balanced", "GC + compaction + EC + region rebalance + extent repair + weekly scrub (recommended steady-state)", [false, true, true, true, false, true, true, true], 30, 240, 2),
        preset("aggressive", "Full auto: incl. split / merge / rebalance topology changes, extent repair and weekly scrub", [true, true, true, true, true, true, true, true], 20, 180, 3),
    ]
}

/// Is `name` a built-in preset (which a custom entry may not shadow / delete)?
pub(crate) fn is_preset_name(name: &str) -> bool {
    preset_policies().iter().any(|p| p.name == name)
}

/// Expand a switch set to the actionable candidate kinds it enables. compact ⇒
/// major compaction. Reads the switches by `SWITCH_ORDER` ([split, ec, compact,
/// gc, merge, rebalance, repair, scrub]); a shorter Vec treats absent switches
/// as off.
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
    }
    if on(3) {
        out.insert(POLICY_KIND_GC);
    }
    if on(4) {
        out.insert(POLICY_KIND_MERGE);
    }
    if on(5) {
        out.insert(POLICY_KIND_REBALANCE);
    }
    if on(6) {
        out.insert(POLICY_KIND_REPAIR);
    }
    if on(7) {
        out.insert(POLICY_KIND_SCRUB);
    }
    out
}

/// Actuation priority (Python `order`): split (relief valve — spreads load) first,
/// then cheap upkeep (gc/major), then ec, then merge LAST (concentrates load
/// onto one core — [[feedback_auto_split_before_merge]]). Lower = higher priority.
fn kind_priority(kind: u8) -> u8 {
    match kind {
        // A copy short comes before everything: the others tune performance
        // and space, this one restores durability.
        POLICY_KIND_REPAIR => 0,
        POLICY_KIND_SPLIT => 1,
        // rebalance is a load-SPREADING action (like split — relieves a hot PS),
        // so it ranks high, ahead of upkeep; merge (concentrates) stays last.
        POLICY_KIND_REBALANCE => 2,
        POLICY_KIND_GC => 3,
        POLICY_KIND_MAJOR_COMPACT => 5,
        POLICY_KIND_EC => 6,
        POLICY_KIND_MERGE => 7,
        // Background reading, never urgent: it finds rot that repair then fixes.
        POLICY_KIND_SCRUB => 8,
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
        // REBALANCE is cluster-scoped + expensive (each actuation reopens up to
        // `rebalance_max_moves_per_tick` partitions). Its advisory is emitted at
        // most once per `rebalance_cooldown_sec`, but that candidate lingers in
        // `advisory_cache` for a whole `policy_tick` window (~60 s) while THIS
        // loop ticks every `interval_sec` — so a policy with `cooldown_sec = 0`
        // would re-actuate the SAME cached candidate every tick → partition
        // reopen storm (coco P1). Floor rebalance's actuation cooldown at a
        // NON-CONFIGURABLE minimum so a mis-set `cooldown_sec` can't bypass it.
        let effective_cooldown = match c.kind {
            POLICY_KIND_REBALANCE => cooldown_secs.max(REBALANCE_MIN_ACTUATION_COOLDOWN_SEC),
            POLICY_KIND_MAJOR_COMPACT => cooldown_secs.max(COMPACT_MIN_ACTUATION_COOLDOWN_SEC),
            // The cadence IS the cooldown: once a week, whatever the policy's
            // own cooldown says. Persisted with the others, so a failover does
            // not restart the week.
            POLICY_KIND_SCRUB => cooldown_secs.max(SCRUB_POLICY_INTERVAL_SEC as i64),
            _ => cooldown_secs,
        };
        if now - last < effective_cooldown {
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
        POLICY_KIND_REBALANCE => "rebalance",
        POLICY_KIND_REPAIR => "repair",
        POLICY_KIND_SCRUB => "scrub",
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
        POLICY_KIND_REBALANCE | POLICY_KIND_SCRUB => "cluster".to_string(),
        POLICY_KIND_REPAIR => format!("node {}", c.secondary_part_id),
        _ => format!("part {}", c.primary_part_id),
    };
    format!("{:<6} {:<18} {}", policy_kind_str(c.kind), target, c.reason)
}

/// Map a candidate to the `autumn-op` actuation command, or `None` if it is
/// advisory-only / missing its target (Python `candidate_to_cmd`). EC carries
/// the extent in `secondary_part_id` (primary=0); split/gc/compact use
/// `primary_part_id`; merge = primary survivor + secondary victim; major maps
/// to `compact`. `hotcold`/unknown → `None`.
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
        POLICY_KIND_MAJOR_COMPACT => {
            Some(vec!["compact".to_string(), c.primary_part_id.to_string()])
        }
        // Cluster-scoped; no target id. Used for the submission log + the
        // client-side cooldown key ("rebalance:0" via the default arm).
        POLICY_KIND_REBALANCE => Some(vec!["rebalance".to_string()]),
        POLICY_KIND_SCRUB => Some(vec!["scrub".to_string(), "--all".to_string()]),
        POLICY_KIND_REPAIR => {
            if c.secondary_part_id == 0 {
                return None;
            }
            Some(vec![
                "repair".to_string(),
                "--node".to_string(),
                c.secondary_part_id.to_string(),
            ])
        }
        _ => None, // hotcold / unknown → advisory only
    }
}

/// The op the controller submits for a candidate: the same ledger entry, and
/// so the same `ops list` / `ops history` rows, as the operator's command from
/// `candidate_to_cmd`. `None` exactly where that is `None`.
pub(crate) fn candidate_to_submit(c: &PolicyCandidate) -> Option<OpSubmitReq> {
    candidate_to_cmd(c)?;
    let (kind, part_id, secondary_id, extent_ids) = match c.kind {
        POLICY_KIND_SPLIT => (OP_KIND_SPLIT, c.primary_part_id, 0, vec![]),
        POLICY_KIND_MERGE => (
            OP_KIND_MERGE,
            c.primary_part_id,
            c.secondary_part_id,
            vec![],
        ),
        POLICY_KIND_GC => (OP_KIND_GC, c.primary_part_id, 0, vec![]),
        POLICY_KIND_MAJOR_COMPACT => (OP_KIND_COMPACT, c.primary_part_id, 0, vec![]),
        POLICY_KIND_EC => (
            OP_KIND_EC_CONVERT,
            0,
            c.secondary_part_id,
            vec![c.secondary_part_id],
        ),
        POLICY_KIND_REBALANCE => (OP_KIND_REBALANCE, 0, 0, vec![]),
        POLICY_KIND_SCRUB => (OP_KIND_SCRUB, 0, 0, vec![]),
        // A node's degraded slots: the node id rides in `part_id`.
        POLICY_KIND_REPAIR => (OP_KIND_REPAIR, c.secondary_part_id, 0, vec![]),
        _ => return None,
    };
    Some(OpSubmitReq {
        kind,
        part_id,
        secondary_id,
        extent_ids,
        requested_by: POLICY_REQUESTER.to_string(),
        // `force` stays false: the controller must never erase an
        // operator-declared presplit boundary by merging across it.
        ..Default::default()
    })
}

/// `requested_by` of the controller's ops.
pub(crate) const POLICY_REQUESTER: &str = "auto-policy";

/// Stable per-(kind, target) key for client-side cooldown tracking (Python
/// `cooldown_key`).
pub(crate) fn cooldown_key(c: &PolicyCandidate) -> String {
    match c.kind {
        POLICY_KIND_EC => format!("ec:{}", c.secondary_part_id),
        POLICY_KIND_MERGE => {
            format!("merge:{}:{}", c.primary_part_id, c.secondary_part_id)
        }
        // Per node — the target lives in `secondary_part_id`.
        POLICY_KIND_REPAIR => format!("repair:{}", c.secondary_part_id),
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
        // split/gc/major use primary; ec uses secondary as the EXTENT id;
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

    /// Every candidate the controller acts on becomes the ledger op of the
    /// operator command it previews, naming the same target.
    #[test]
    fn candidate_to_submit_matches_the_previewed_command() {
        let cases = [
            (cand(POLICY_KIND_SPLIT, 7, 0), OP_KIND_SPLIT, 7, 0),
            (cand(POLICY_KIND_MERGE, 3, 4), OP_KIND_MERGE, 3, 4),
            (cand(POLICY_KIND_GC, 5, 0), OP_KIND_GC, 5, 0),
            (cand(POLICY_KIND_MAJOR_COMPACT, 6, 0), OP_KIND_COMPACT, 6, 0),
            (cand(POLICY_KIND_EC, 0, 88), OP_KIND_EC_CONVERT, 0, 88),
            (cand(POLICY_KIND_REBALANCE, 0, 0), OP_KIND_REBALANCE, 0, 0),
            (cand(POLICY_KIND_SCRUB, 0, 0), OP_KIND_SCRUB, 0, 0),
            (cand(POLICY_KIND_REPAIR, 0, 9), OP_KIND_REPAIR, 9, 0),
        ];
        for (c, kind, part_id, secondary_id) in cases {
            let s = candidate_to_submit(&c).expect("actionable");
            assert_eq!(
                (s.kind, s.part_id, s.secondary_id),
                (kind, part_id, secondary_id),
                "{}",
                describe_candidate(&c)
            );
            assert_eq!(s.requested_by, POLICY_REQUESTER);
            assert!(!s.force, "the controller never forces a merge");
        }
        assert!(candidate_to_submit(&cand(POLICY_KIND_HOT_COLD, 1, 2)).is_none());
        assert!(candidate_to_submit(&cand(POLICY_KIND_EC, 0, 0)).is_none());
        assert!(candidate_to_submit(&cand(POLICY_KIND_MERGE, 3, 0)).is_none());
        assert!(candidate_to_submit(&cand(POLICY_KIND_REPAIR, 0, 0)).is_none());
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

    /// A repair advisory targets a NODE (`secondary_part_id`): its command,
    /// its cooldown key and its description all name that node — a key built
    /// from `primary_part_id` (always 0 here) would make every node share one
    /// cooldown.
    #[test]
    fn a_repair_candidate_names_its_node() {
        let c = cand(POLICY_KIND_REPAIR, 0, 5);
        assert_eq!(
            candidate_to_cmd(&c),
            Some(vec!["repair".to_string(), "--node".to_string(), "5".to_string()])
        );
        assert_eq!(cooldown_key(&c), "repair:5");
        assert_ne!(cooldown_key(&c), cooldown_key(&cand(POLICY_KIND_REPAIR, 0, 6)));
        assert!(describe_candidate(&c).contains("node 5"));
        assert_eq!(candidate_to_cmd(&cand(POLICY_KIND_REPAIR, 0, 0)), None);
    }

    #[test]
    fn repair_is_actuated_before_anything_else() {
        let cands = vec![cand(POLICY_KIND_SPLIT, 1, 0), cand(POLICY_KIND_REPAIR, 0, 5)];
        let enabled: HashSet<u8> = [POLICY_KIND_SPLIT, POLICY_KIND_REPAIR].into_iter().collect();
        let picked = decide_actions(&cands, &HashMap::new(), &enabled, 1000, 0, 1);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].0.kind, POLICY_KIND_REPAIR);
    }

    #[test]
    fn kinds_from_switches_expands_compact_to_major() {
        // [split, ec, compact, gc, merge, rebalance, repair]
        let ks = kinds_from_switches(&[false, false, true, false, false, false, false]);
        assert!(ks.contains(&POLICY_KIND_MAJOR_COMPACT));
        assert_eq!(ks.len(), 1);
        let all = kinds_from_switches(&[true; SWITCHES]);
        assert_eq!(all.len(), 8); // split, ec, major, gc, merge, rebalance, repair, scrub
        assert!(all.contains(&POLICY_KIND_REBALANCE));
        assert!(all.contains(&POLICY_KIND_REPAIR));
        assert!(all.contains(&POLICY_KIND_SCRUB));
        // A config persisted before a switch existed reads it as off.
        assert!(!kinds_from_switches(&[true; 6]).contains(&POLICY_KIND_REPAIR));
        assert!(!kinds_from_switches(&[true; 7]).contains(&POLICY_KIND_SCRUB));
        // rebalance switch (index 5) alone → just rebalance.
        let rb = kinds_from_switches(&[false, false, false, false, false, true, false]);
        assert_eq!(rb, {
            let mut s = HashSet::new();
            s.insert(POLICY_KIND_REBALANCE);
            s
        });
        assert!(kinds_from_switches(&[false; SWITCHES]).is_empty());
    }

    #[test]
    fn presets_are_ordered_and_gc_only_enables_only_gc() {
        let ps = preset_policies();
        assert_eq!(ps.len(), 5);
        assert_eq!(ps[0].name, "gc-only");
        assert_eq!(ps[4].name, "aggressive");
        assert!(ps.iter().all(|p| p.builtin));
        // gc-only: only the gc switch (index 3).
        // switches = [split,ec,compact,gc,merge,rebalance,repair,scrub]
        assert_eq!(ps[0].switches, vec![false, false, false, true, false, false, false, false]);
        assert_eq!(kinds_from_switches(&ps[0].switches), {
            let mut s = HashSet::new();
            s.insert(POLICY_KIND_GC);
            s
        });
        // aggressive turns everything on (incl. rebalance).
        assert_eq!(ps[4].switches, vec![true; SWITCHES]);
        assert!(kinds_from_switches(&ps[4].switches).contains(&POLICY_KIND_REBALANCE));
    }

    /// The weekly cadence is the actuation cooldown, whatever the policy's own
    /// `cooldown_sec` says: an armed policy that scrubbed a minute ago does not
    /// scrub again until a week has passed.
    #[test]
    fn a_scrub_is_actuated_at_most_once_a_week() {
        let enabled: HashSet<u8> = [POLICY_KIND_SCRUB].into_iter().collect();
        let c = cand(POLICY_KIND_SCRUB, 0, 0);
        assert_eq!(candidate_to_cmd(&c), Some(vec!["scrub".to_string(), "--all".to_string()]));
        let key = cooldown_key(&c);
        let week = SCRUB_POLICY_INTERVAL_SEC as i64;
        let now = 10 * week;
        let fresh = HashMap::new();
        assert_eq!(decide_actions(&[c.clone()], &fresh, &enabled, now, 60, 5).len(), 1);
        let mut cds = HashMap::new();
        cds.insert(key.clone(), now - week + 60);
        assert!(
            decide_actions(&[c.clone()], &cds, &enabled, now, 60, 5).is_empty(),
            "a policy cooldown of 60 s must not shorten the week"
        );
        cds.insert(key, now - week);
        assert_eq!(decide_actions(&[c], &cds, &enabled, now, 60, 5).len(), 1);
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

    /// A policy with `cooldown_sec = 0` must NOT let the expensive kinds
    /// re-actuate every tick — the non-configurable floors apply: cluster-scoped
    /// rebalance, and BOTH compact kinds, which actuate the identical op.
    #[test]
    fn expensive_kinds_are_floored_despite_zero_cooldown() {
        let enabled: HashSet<u8> = [POLICY_KIND_REBALANCE].into_iter().collect();
        let cands = vec![cand(POLICY_KIND_REBALANCE, 0, 0)];
        let key = cooldown_key(&cands[0]); // "rebalance:cluster"

        // Actuated 10 s ago; policy cooldown_sec=0 alone would NOT suppress, but
        // the 60 s floor must.
        let mut cds = HashMap::new();
        cds.insert(key.clone(), 990i64);
        assert!(
            decide_actions(&cands, &cds, &enabled, 1000, 0, 5).is_empty(),
            "rebalance floored despite cooldown_sec=0"
        );
        // Past the floor → allowed.
        cds.insert(key, 1000 - REBALANCE_MIN_ACTUATION_COOLDOWN_SEC - 1);
        assert_eq!(
            decide_actions(&cands, &cds, &enabled, 1000, 0, 5).len(),
            1,
            "allowed once past the floor"
        );

        // Compaction carries the same floor, and for the same reason: the loop
        // actuates from a cached candidate list, and `unblocking_compact` does
        // not suppress emission on the compact cooldown — so without it a
        // `cooldown_sec = 0` policy re-issues one every interval tick.
        let mc_enabled: HashSet<u8> = [POLICY_KIND_MAJOR_COMPACT].into_iter().collect();
        let mc = vec![cand(POLICY_KIND_MAJOR_COMPACT, 7, 0)];
        let mut mcd = HashMap::new();
        mcd.insert("major:7".to_string(), 990i64); // 10 s ago
        assert!(
            decide_actions(&mc, &mcd, &mc_enabled, 1000, 0, 5).is_empty(),
            "major compact floored despite cooldown_sec=0"
        );
        mcd.insert("major:7".to_string(), 1000 - COMPACT_MIN_ACTUATION_COOLDOWN_SEC - 1);
        assert_eq!(
            decide_actions(&mc, &mcd, &mc_enabled, 1000, 0, 5).len(),
            1,
            "allowed once past the floor"
        );

        // A kind with no floor at cooldown_sec=0 is unaffected.
        let gc_enabled: HashSet<u8> = [POLICY_KIND_GC].into_iter().collect();
        let gc = vec![cand(POLICY_KIND_GC, 7, 0)];
        let mut gcd = HashMap::new();
        gcd.insert("gc:7".to_string(), 999i64); // 1 s ago
        assert_eq!(
            decide_actions(&gc, &gcd, &gc_enabled, 1000, 0, 5).len(),
            1,
            "gc is not floored at cooldown_sec=0"
        );
    }

    #[test]
    fn mode_roundtrips_through_u8() {
        for m in [AutoPolicyMode::Off, AutoPolicyMode::Armed] {
            assert_eq!(AutoPolicyMode::from_u8(m.as_u8()), Some(m));
        }
        // 1 was the removed observe mode: refused, not read as Off or Armed.
        assert_eq!(AutoPolicyMode::from_u8(1), None);
        assert_eq!(AutoPolicyMode::from_u8(99), None);
    }

    #[test]
    fn a_persisted_observe_mode_is_refused() {
        let mut st = AutoPolicyState::default();
        let cfg = MgrAutoPolicyConfig { ver: 1, mode: 1, active: "gc-only".into(), policies: vec![] };
        let e = st.load_config(cfg).unwrap_err();
        assert!(e.contains("migratev1_v2"), "{e}");
    }
}
