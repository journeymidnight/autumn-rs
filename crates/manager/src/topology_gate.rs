//! A split or merge waits for EC conversions and recoveries BEFORE it freezes
//! writes, and holds back other work on its partitions' extents until done.
//!
//! The split commit (`handle_multi_modify_split`) bumps `refs` and `eversion`
//! on every extent of the partition's three streams, CAS'd in one etcd txn,
//! and refuses while any of them carries a ConvertToEc or Recovery marker; the
//! merge commit refuses the same way for both partitions. Those ops run for
//! seconds to minutes; met after the freeze, a split kept writes frozen for
//! its whole freeze budget and a merge froze both sides to roll back. So the
//! manager waits first, writes still flowing, at most `BLOCKER_WAIT_MAX`.
//!
//! From before that wait until the op ends, its partitions are in
//! `topology_held`, and nothing new starts on their extents: EC conversion,
//! recovery dispatch, in-place catch-up, scrub, GC punch / truncate (any
//! partition sharing them), the sealed-empty sweep, and maintenance dispatched
//! to them. Without the recovery hold, fresh markers could keep the wait from
//! ever ending; the cap bounds how long repairs wait. An op that slips in
//! anyway (a marker acquired across its etcd await) makes the commit refuse: a
//! split's PS aborts at once instead of retrying while frozen, and a merge
//! rolls back.

use std::time::Duration;

use crate::extent_inflight::ExtentOpKind;
use crate::persist::records::PartitionRecord;
use crate::store::MetadataState;
use crate::AutumnManager;

const BLOCKER_POLL: Duration = Duration::from_millis(500);

/// Past this the op fails with its reason and releases the hold.
const BLOCKER_WAIT_MAX: Duration = Duration::from_secs(600);

/// Held from before the wait until the split's PS answers or the merge ends.
pub(crate) struct TopologyHold {
    manager: AutumnManager,
    parts: Vec<u64>,
}

impl Drop for TopologyHold {
    fn drop(&mut self) {
        let mut held = self.manager.topology_held.borrow_mut();
        for p in &self.parts {
            held.remove(p);
        }
    }
}

impl AutumnManager {
    /// The extent op a split or merge commit of `part` would be refused for.
    /// The split's PS matches `in flight on extent` to abort without retrying.
    pub(crate) fn topology_blocker(&self, s: &MetadataState, part: &PartitionRecord) -> Option<String> {
        for sid in [part.log_stream, part.row_stream, part.meta_stream] {
            let Some(stream) = s.streams.get(&sid) else {
                continue;
            };
            for &eid in &stream.extent_ids {
                match self.extent_inflight_op(eid) {
                    Some(ExtentOpKind::ConvertToEc) => {
                        return Some(format!("ec conversion in flight on extent {eid}"))
                    }
                    Some(ExtentOpKind::Recovery) => {
                        return Some(format!("recovery in flight on extent {eid}"))
                    }
                    _ => {}
                }
            }
        }
        None
    }

    /// The held partition whose streams contain `extent_id`.
    pub(crate) fn topology_holding_extent(&self, extent_id: u64) -> Option<u64> {
        let held = self.topology_held.borrow();
        if held.is_empty() {
            return None;
        }
        let s = self.store.inner.borrow();
        held.iter().copied().find(|pid| {
            s.partitions.get(pid).is_some_and(|p| {
                [p.log_stream, p.row_stream, p.meta_stream].iter().any(|sid| {
                    s.streams
                        .get(sid)
                        .is_some_and(|st| st.extent_ids.contains(&extent_id))
                })
            })
        })
    }

    /// GC's punch and truncate change `refs` and stream membership, which the
    /// commit CAS's; refused like an extent op in flight (the PS backs off on a
    /// precondition). A held partition's own `stream_id` is not refused for
    /// its own hold: its PS takes the maintenance gate before freezing, so its
    /// own GC and compaction cannot overlap the commit (any other own-stream
    /// punch, e.g. a rolled tail's reclaim, meets the commit's per-extent CAS),
    /// and refusing them during the wait only fails a compaction's truncate.
    /// Another held partition sharing the extents still refuses it.
    pub(crate) fn refuse_if_topology_holds(
        &self,
        removed: &std::collections::HashSet<u64>,
        stream_id: u64,
        op: &str,
    ) -> Result<(), autumn_common::AppError> {
        let held = self.topology_held.borrow();
        if held.is_empty() {
            return Ok(());
        }
        let s = self.store.inner.borrow();
        for pid in held.iter() {
            let Some(p) = s.partitions.get(pid) else {
                continue;
            };
            let streams = [p.log_stream, p.row_stream, p.meta_stream];
            if streams.contains(&stream_id) {
                continue;
            }
            let holds = streams.iter().any(|sid| {
                s.streams
                    .get(sid)
                    .is_some_and(|st| st.extent_ids.iter().any(|e| removed.contains(e)))
            });
            if holds {
                return Err(autumn_common::AppError::Precondition(format!(
                    "{op}: partition {pid} holding these extents is being split or merged; \
                     retry after it"
                )));
            }
        }
        Ok(())
    }

    /// Every held extent, for a pass over many extents.
    pub(crate) fn topology_held_extents(&self) -> std::collections::HashSet<u64> {
        let held = self.topology_held.borrow();
        let mut out = std::collections::HashSet::new();
        if held.is_empty() {
            return out;
        }
        let s = self.store.inner.borrow();
        for pid in held.iter() {
            if let Some(p) = s.partitions.get(pid) {
                for sid in [p.log_stream, p.row_stream, p.meta_stream] {
                    if let Some(st) = s.streams.get(&sid) {
                        out.extend(st.extent_ids.iter().copied());
                    }
                }
            }
        }
        out
    }

    /// Take the hold on `parts`, then wait until no extent op blocks `what`
    /// ("split" / "merge"). `op_id` 0 (the policy controller, which actuates
    /// inline, or a direct RPC) refuses instead of waiting. Returns whether it
    /// waited, i.e. whether the caller's metadata snapshot is stale.
    pub(crate) async fn topology_ready(
        &self,
        parts: &[u64],
        op_id: u64,
        what: &str,
    ) -> anyhow::Result<(TopologyHold, bool)> {
        self.topology_ready_within(parts, op_id, what, BLOCKER_WAIT_MAX)
            .await
    }

    async fn topology_ready_within(
        &self,
        parts: &[u64],
        op_id: u64,
        what: &str,
        max_wait: Duration,
    ) -> anyhow::Result<(TopologyHold, bool)> {
        {
            let mut held = self.topology_held.borrow_mut();
            if let Some(p) = parts.iter().find(|p| held.contains(p)) {
                anyhow::bail!("partition {p} is already being split or merged");
            }
            held.extend(parts.iter().copied());
        }
        let hold = TopologyHold {
            manager: self.clone(),
            parts: parts.to_vec(),
        };
        let started = std::time::Instant::now();
        let mut waited = false;
        loop {
            let why = {
                let s = self.store.inner.borrow();
                // A missing partition is reported by the op itself.
                parts
                    .iter()
                    .filter_map(|pid| s.partitions.get(pid))
                    .find_map(|p| self.topology_blocker(&s, p))
            };
            let Some(why) = why else {
                if waited {
                    self.ops.borrow_mut().set_message(op_id, String::new());
                }
                return Ok((hold, waited));
            };
            if op_id == 0 {
                anyhow::bail!("{why}; {what} deferred");
            }
            if !self.leader.get() {
                anyhow::bail!("lost leadership while waiting: {why}");
            }
            if started.elapsed() >= max_wait {
                anyhow::bail!(
                    "{why} for over {} s; {what} abandoned before freezing writes",
                    max_wait.as_secs()
                );
            }
            if !waited {
                tracing::info!(op_id, ?parts, reason = %why, "{what} waits before freezing writes");
            }
            self.ops
                .borrow_mut()
                .set_message(op_id, format!("waiting, writes not frozen: {why}"));
            waited = true;
            compio::time::sleep(BLOCKER_POLL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::records::StreamRecord;

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    /// Partition 5 on streams 100/101/102; extent 7 is the log stream's.
    fn manager() -> AutumnManager {
        let m = AutumnManager::new();
        m.leader.set(true);
        let mut s = m.store.inner.borrow_mut();
        for (sid, extents) in [(100u64, vec![7u64]), (101, vec![]), (102, vec![])] {
            s.streams.insert(
                sid,
                StreamRecord {
                    stream_id: sid,
                    extent_ids: extents,
                    ec_data_shard: 0,
                    ec_parity_shard: 0,
                    replicates: 3,
                },
            );
        }
        s.partitions.insert(
            5,
            PartitionRecord {
                part_id: 5,
                log_stream: 100,
                row_stream: 101,
                meta_stream: 102,
                rg: None,
            },
        );
        drop(s);
        m
    }

    #[test]
    fn the_policy_path_refuses_and_releases_the_hold() {
        let m = manager();
        m._test_mark_ec_inflight(7);
        let e = run(m.topology_ready(&[5], 0, "split")).err().expect("refused");
        assert!(e.to_string().contains("ec conversion in flight on extent 7"), "{e}");
        assert!(m.topology_held.borrow().is_empty(), "hold released on refusal");
    }

    #[test]
    fn a_second_dispatch_of_the_same_partition_is_refused() {
        let m = manager();
        let (_hold, _) = run(m.topology_ready(&[5], 0, "split")).expect("first");
        let e = run(m.topology_ready(&[5], 0, "split")).err().expect("second refused");
        assert!(e.to_string().contains("already being split or merged"), "{e}");
        assert!(m.topology_held.borrow().contains(&5), "the first hold survives");
    }

    #[test]
    fn the_wait_gives_up_after_its_cap_and_releases_the_hold() {
        let m = manager();
        let (op_id, _) = m.ops.borrow_mut().submit(
            autumn_rpc::manager_rpc::OP_KIND_SPLIT,
            5,
            0,
            vec![],
            "test".into(),
            0,
            0,
        );
        m._test_mark_ec_inflight(7);
        let e = run(m.topology_ready_within(&[5], op_id, "split", Duration::from_millis(600)))
            .err()
            .expect("gave up");
        assert!(e.to_string().contains("split abandoned before freezing writes"), "{e}");
        assert!(m.topology_held.borrow().is_empty(), "hold released");
    }

    /// While held, another partition's punch of the shared extent is refused,
    /// the held partition's own is not, and the extent is in the held set
    /// scrub skips; all clear once the hold drops.
    #[test]
    fn the_hold_refuses_a_sibling_punch_until_released() {
        let m = manager();
        // A CoW sibling's log stream sharing extent 7.
        m.store.inner.borrow_mut().streams.insert(
            200,
            StreamRecord {
                stream_id: 200,
                extent_ids: vec![7],
                ec_data_shard: 0,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        let (hold, _) = run(m.topology_ready(&[5], 0, "split")).expect("ready");
        let removed: std::collections::HashSet<u64> = [7u64].into_iter().collect();
        let e = m
            .refuse_if_topology_holds(&removed, 200, "punch_holes")
            .err()
            .expect("sibling refused");
        assert!(e.to_string().contains("partition 5"), "{e}");
        assert!(
            m.refuse_if_topology_holds(&removed, 100, "truncate").is_ok(),
            "the held partition's own stream"
        );
        assert!(m.topology_held_extents().contains(&7));
        drop(hold);
        assert!(m.refuse_if_topology_holds(&removed, 200, "punch_holes").is_ok());
        assert!(m.topology_held_extents().is_empty());
    }

    /// Two held partitions sharing an extent: each one's own punch is still
    /// refused for the other's hold, whatever order the held set iterates in.
    #[test]
    fn a_punch_is_refused_for_another_held_partition_sharing_the_extent() {
        let m = manager();
        {
            let mut s = m.store.inner.borrow_mut();
            for sid in [200u64, 201, 202] {
                s.streams.insert(
                    sid,
                    StreamRecord {
                        stream_id: sid,
                        extent_ids: if sid == 200 { vec![7] } else { vec![] },
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
            }
            s.partitions.insert(
                6,
                PartitionRecord {
                    part_id: 6,
                    log_stream: 200,
                    row_stream: 201,
                    meta_stream: 202,
                    rg: None,
                },
            );
        }
        let (_hold, _) = run(m.topology_ready(&[5, 6], 0, "merge")).expect("ready");
        let removed: std::collections::HashSet<u64> = [7u64].into_iter().collect();
        for (stream, other) in [(100u64, "partition 6"), (200, "partition 5")] {
            let e = m
                .refuse_if_topology_holds(&removed, stream, "punch_holes")
                .err()
                .expect("refused for the other hold");
            assert!(e.to_string().contains(other), "{e}");
        }
    }

    /// A slot the tick would rebuild (its node is fenced) is left alone while
    /// a split holds the extent, and dispatched once the hold drops.
    #[test]
    fn the_hold_defers_a_rebuild() {
        use crate::persist::records::{DiskRecord, ExtentRecord, NodeRecord};
        use autumn_rpc::manager_rpc::{MgrNodeOverride, OpQueryReq, NODE_OVERRIDE_FENCED, OP_KIND_RECOVERY};
        let m = manager();
        {
            let mut s = m.store.inner.borrow_mut();
            for (nid, disk) in [(1u64, 10u64), (9, 90)] {
                s.nodes.insert(
                    nid,
                    NodeRecord {
                        node_id: nid,
                        // Nothing listens: the dispatch is refused, which still
                        // shows up as an attempt in the ledger.
                        address: format!("127.0.0.1:{}", 9100 + nid),
                        disks: vec![disk],
                        ..Default::default()
                    },
                );
                s.disks.insert(
                    disk,
                    DiskRecord {
                        disk_id: disk,
                        online: true,
                        ..Default::default()
                    },
                );
            }
            s.extents.insert(
                7,
                ExtentRecord {
                    extent_id: 7,
                    sealed: true,
                    sealed_length: 4096,
                    replicates: vec![1],
                    replicate_disks: vec![10],
                    avali: 1,
                    refs: 1,
                    ..Default::default()
                },
            );
        }
        for nid in [1u64, 9] {
            m.node_states.borrow_mut().on_heartbeat_ok(nid);
            m.node_max_free.borrow_mut().insert(nid, 1 << 30);
        }
        m.node_overrides.borrow_mut().insert(
            1,
            MgrNodeOverride {
                node_id: 1,
                kind: NODE_OVERRIDE_FENCED,
                ..Default::default()
            },
        );
        let attempts = |m: &AutumnManager| {
            m.ops
                .borrow()
                .query(&OpQueryReq {
                    kind_filter: OP_KIND_RECOVERY,
                    ..Default::default()
                })
                .len()
        };
        let (hold, _) = run(m.topology_ready(&[5], 0, "split")).expect("ready");
        run(m.recovery_dispatch_tick());
        assert_eq!(attempts(&m), 0, "no rebuild while the split holds extent 7");
        drop(hold);
        run(m.recovery_dispatch_tick());
        assert!(attempts(&m) > 0, "the rebuild is dispatched once the hold drops");
    }

    /// An operator split waits with the reason on its op, then proceeds once
    /// the marker goes away, and clears the reason.
    #[test]
    fn an_operator_split_waits_for_the_marker() {
        let m = manager();
        let (op_id, _) = m.ops.borrow_mut().submit(
            autumn_rpc::manager_rpc::OP_KIND_SPLIT,
            5,
            0,
            vec![],
            "test".into(),
            0,
            0,
        );
        m.ops.borrow_mut().set_running(op_id, 0);
        m._test_mark_ec_inflight(7);
        run(async {
            let clear = {
                let m = m.clone();
                compio::runtime::spawn(async move {
                    compio::time::sleep(Duration::from_millis(1200)).await;
                    let msg = m.ops.borrow().record(op_id).unwrap().message;
                    m._test_clear_inflight(7);
                    msg
                })
            };
            let (_hold, waited) = m.topology_ready(&[5], op_id, "split").await.expect("ready");
            assert!(waited);
            let seen = clear.await.unwrap();
            assert!(
                seen.contains("waiting, writes not frozen: ec conversion in flight on extent 7"),
                "{seen}"
            );
        });
        assert_eq!(m.ops.borrow().record(op_id).unwrap().message, "");
    }
}
