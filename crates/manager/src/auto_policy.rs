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

use autumn_rpc::manager_rpc::{
    PolicyCandidate, POLICY_KIND_EC, POLICY_KIND_GC, POLICY_KIND_HOT_COLD, POLICY_KIND_MAJOR_COMPACT,
    POLICY_KIND_MERGE, POLICY_KIND_MINOR_COMPACT, POLICY_KIND_SPLIT,
};

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
}
