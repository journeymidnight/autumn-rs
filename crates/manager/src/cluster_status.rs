//! `MSG_GET_CLUSTER_STATUS`: the fleet at a glance, measured against the
//! EXPECTED members — manager and PS members, registered extent nodes — never
//! against whoever answered last. One leader, one instant.

use std::collections::{BTreeMap, BTreeSet};

use autumn_rpc::manager_rpc::*;
use autumn_rpc::HandlerResult;

use crate::persist::records::MemberRecord;
use crate::AutumnManager;

fn secs_since(now_ms: i64, then_ms: i64) -> u64 {
    if then_ms <= 0 {
        u64::MAX
    } else {
        (now_ms.saturating_sub(then_ms).max(0) / 1000) as u64
    }
}

/// Every expected manager: members, plus any present id the leader has not
/// folded in yet, plus the leader itself (`self_id` 0 = memory mode).
pub(crate) fn manager_fleet(
    self_id: u64,
    self_address: &str,
    members: &BTreeMap<u64, MemberRecord>,
    present: &BTreeMap<u64, String>,
    now_ms: i64,
) -> Vec<FleetMember> {
    let ids: BTreeSet<u64> = members
        .keys()
        .chain(present.keys())
        .copied()
        .chain(std::iter::once(self_id))
        .collect();
    ids.into_iter()
        .map(|id| {
            let (state, age_secs) = if id == self_id {
                (FLEET_MANAGER_LEADER, 0)
            } else if present.contains_key(&id) {
                (FLEET_MANAGER_STANDBY, 0)
            } else {
                let left = members.get(&id).map_or(0, |m| m.left_at_ms);
                (FLEET_MANAGER_ABSENT, secs_since(now_ms, left))
            };
            let address = if id == self_id {
                self_address.to_string()
            } else {
                present
                    .get(&id)
                    .or(members.get(&id).map(|m| &m.address))
                    .cloned()
                    .unwrap_or_default()
            };
            FleetMember {
                id,
                address,
                state,
                age_secs,
            }
        })
        .collect()
}

pub(crate) fn ps_fleet(servers: &[PsOverview], now_ms: i64) -> Vec<FleetMember> {
    servers
        .iter()
        .map(|p| {
            let (state, age_secs) = if p.evicted_at_ms > 0 {
                (FLEET_PS_EVICTED, secs_since(now_ms, p.evicted_at_ms))
            } else if p.ready() {
                (FLEET_PS_READY, p.last_heartbeat_secs_ago)
            } else if p.last_heartbeat_secs_ago < PsOverview::READY_MAX_HEARTBEAT_AGE_SECS {
                (FLEET_PS_OPENING, p.last_heartbeat_secs_ago)
            } else {
                (FLEET_PS_SILENT, p.last_heartbeat_secs_ago)
            };
            FleetMember {
                id: p.ps_id,
                address: p.address.clone(),
                state,
                age_secs,
            }
        })
        .collect()
}

/// `answered(node)`: this leader has had a `df` answer from it. Without one
/// the tracker's Online is only the replay seed, which vouches for nothing.
pub(crate) fn en_fleet(
    nodes: &[NodeStateEntry],
    answered: impl Fn(u64) -> bool,
) -> Vec<FleetMember> {
    nodes
        .iter()
        .map(|n| {
            let state = match (n.override_kind, n.auto_state) {
                (NODE_OVERRIDE_FENCED, _) => FLEET_EN_FENCED,
                (NODE_OVERRIDE_MAINTENANCE, _) => FLEET_EN_MAINTENANCE,
                (_, NODE_AUTO_STATE_ONLINE) if !answered(n.node_id) => FLEET_EN_UNKNOWN,
                (_, NODE_AUTO_STATE_ONLINE) => FLEET_EN_ONLINE,
                (_, NODE_AUTO_STATE_SUSPECTED) => FLEET_EN_SUSPECTED,
                _ => FLEET_EN_SUSPEND,
            };
            FleetMember {
                id: n.node_id,
                address: n.address.clone(),
                state,
                age_secs: n.last_heartbeat_secs_ago,
            }
        })
        .collect()
}

impl AutumnManager {
    pub(crate) async fn handle_get_cluster_status(&self) -> HandlerResult {
        let fail = |code, message| {
            Ok(rkyv_encode(&ClusterStatusResp {
                code,
                message,
                ..Default::default()
            }))
        };
        if let Err(err) = self.ensure_leader() {
            return fail(Self::err_to_code(&err), err.to_string());
        }
        // The only await: everything after it is read at one instant.
        let present = match self.read_manager_presence().await {
            Ok(p) => p,
            Err(err) => return fail(Self::err_to_code(&err), err.to_string()),
        };
        if let Err(err) = self.ensure_leader() {
            return fail(Self::err_to_code(&err), err.to_string());
        }
        let now_ms = Self::now_s_ms().1;
        let (self_id, self_address) = self
            .identity
            .as_ref()
            .map_or((0, String::new()), |i| (i.id, i.address.clone()));
        let (managers, ps) = {
            let s = self.store.inner.borrow();
            (
                manager_fleet(self_id, &self_address, &s.manager_members, &present, now_ms),
                self.ps_servers_overview(&s),
            )
        };
        let nodes = self.compute_list_node_states_resp();
        let health = crate::extent_health::summarize(self.extent_health_scan(now_ms / 1000), 0);
        let recovery_inflight = self
            .inflight
            .borrow()
            .values()
            .filter(|r| {
                matches!(
                    r.unpack(),
                    Some((_, crate::extent_inflight::ExtentOpPayload::Recovery(_)))
                )
            })
            .count() as u64;
        Ok(rkyv_encode(&ClusterStatusResp {
            code: CODE_OK,
            message: String::new(),
            sampled_at_ms: now_ms,
            managers,
            partition_servers: ps_fleet(&ps, now_ms),
            extent_nodes: en_fleet(&nodes.nodes, |id| self.has_first_hand_df(id)),
            sealed_extents: health.sealed_extents,
            clean: health.clean,
            degraded: health.degraded,
            unavailable: health.unavailable,
            recovery_inflight,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(address: &str, left_at_ms: i64) -> MemberRecord {
        MemberRecord {
            address: address.to_string(),
            joined_at_ms: 1,
            left_at_ms,
        }
    }

    /// The denominator is the membership: a stopped manager stays counted as
    /// absent, and a present id not yet folded into the members still counts.
    #[test]
    fn managers_are_counted_against_the_membership() {
        let members = BTreeMap::from([
            (1, member("m1:9001", 0)),
            (2, member("m2:9001", 0)),
            (3, member("m3:9001", 40_000)),
        ]);
        let present = BTreeMap::from([(1, "m1:9001".to_string()), (4, "m4:9001".to_string())]);
        let fleet = manager_fleet(1, "m1:9001", &members, &present, 100_000);
        let states: Vec<(u64, u8, u64)> = fleet.iter().map(|m| (m.id, m.state, m.age_secs)).collect();
        assert_eq!(
            states,
            vec![
                (1, FLEET_MANAGER_LEADER, 0),
                (2, FLEET_MANAGER_ABSENT, u64::MAX),
                (3, FLEET_MANAGER_ABSENT, 60),
                (4, FLEET_MANAGER_STANDBY, 0),
            ]
        );
        assert_eq!(fleet[3].address, "m4:9001");
    }

    #[test]
    fn memory_mode_is_one_leader() {
        let fleet = manager_fleet(0, "", &BTreeMap::new(), &BTreeMap::new(), 1);
        assert_eq!(fleet.len(), 1);
        assert_eq!(fleet[0].state, FLEET_MANAGER_LEADER);
    }

    fn ps(id: u64, hb: u64, open: Option<u32>, parts: u32, evicted_at_ms: i64) -> PsOverview {
        PsOverview {
            ps_id: id,
            last_heartbeat_secs_ago: hb,
            partition_count: parts,
            open_count: open,
            evicted_at_ms,
            ..Default::default()
        }
    }

    #[test]
    fn a_ps_is_ready_opening_silent_or_evicted() {
        let servers = [
            ps(1, 1, Some(2), 2, 0),
            ps(2, 1, Some(1), 2, 0),
            ps(3, 9, Some(2), 2, 0),
            ps(4, u64::MAX, None, 0, 70_000),
        ];
        let got: Vec<(u8, u64)> = ps_fleet(&servers, 100_000)
            .iter()
            .map(|m| (m.state, m.age_secs))
            .collect();
        assert_eq!(
            got,
            vec![
                (FLEET_PS_READY, 1),
                (FLEET_PS_OPENING, 1),
                (FLEET_PS_SILENT, 9),
                (FLEET_PS_EVICTED, 30),
            ]
        );
    }

    #[test]
    fn an_override_wins_and_an_unanswered_node_is_not_online() {
        let node = |id, auto, ovr| NodeStateEntry {
            node_id: id,
            address: String::new(),
            auto_state: auto,
            last_heartbeat_secs_ago: 2,
            suspected_age_secs: 0,
            override_kind: ovr,
            override_reason: String::new(),
            override_set_by: String::new(),
            override_set_at: 0,
            override_expire_at: 0,
            node_uuid: String::new(),
            shard_ports: Vec::new(),
        };
        let nodes = [
            node(1, NODE_AUTO_STATE_ONLINE, NODE_OVERRIDE_NONE),
            node(2, NODE_AUTO_STATE_ONLINE, NODE_OVERRIDE_FENCED),
            node(3, NODE_AUTO_STATE_SUSPECTED, NODE_OVERRIDE_MAINTENANCE),
            node(4, NODE_AUTO_STATE_SUSPECTED, NODE_OVERRIDE_NONE),
            node(5, NODE_AUTO_STATE_SUSPEND, NODE_OVERRIDE_NONE),
            node(6, NODE_AUTO_STATE_ONLINE, NODE_OVERRIDE_NONE),
        ];
        let got: Vec<u8> = en_fleet(&nodes, |id| id != 6).iter().map(|m| m.state).collect();
        assert_eq!(
            got,
            vec![
                FLEET_EN_ONLINE,
                FLEET_EN_FENCED,
                FLEET_EN_MAINTENANCE,
                FLEET_EN_SUSPECTED,
                FLEET_EN_SUSPEND,
                FLEET_EN_UNKNOWN,
            ]
        );
    }
}
