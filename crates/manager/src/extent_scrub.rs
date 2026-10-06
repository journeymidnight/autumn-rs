//! Scrub orchestration: `autumn-op scrub` and the weekly `scrub` policy.
//!
//! The manager decides WHICH files on WHICH nodes and hands each node its list
//! (`MSG_SCRUB_EXTENTS`); the node reads and hashes its own files, paced by its
//! own byte budget, and reports one outcome per file on its next `df`. No
//! content crosses the network. What a node does is in the stream crate's
//! `extent_node/scrub.rs`; a rot finding is a `SCRUB_OUTCOME_ROT` outcome, and
//! goes through `isolate_rotted_slot`. One that cannot be judged when it
//! arrives — the extent moved since the task was planned — stays open in its
//! op until the extent settles, and that copy is scrubbed again
//! (`recheck_scrub_findings`).
//!
//! The manager names the file and its length because it is the one that
//! knows: the extent is sealed, its payload on that node is `.dat` (a replica)
//! or `.shard{i}` (slot `i` of a converted extent), and how many bytes that
//! file holds.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use autumn_rpc::extent_rpc::{
    PayloadLocation, ScrubDone, ScrubExtentsReq, ScrubTask, MSG_SCRUB_EXTENTS,
    SCRUB_OUTCOME_CLEAN, SCRUB_OUTCOME_DESCRIBED, SCRUB_OUTCOME_FAILED, SCRUB_OUTCOME_ROT,
    SCRUB_OUTCOME_SKIPPED,
};
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, OpSubmitReq, PolicyCandidate, OP_KIND_SCRUB, OP_STATE_FAILED,
    OP_STATE_SUCCEEDED, OP_STATE_UNKNOWN, POLICY_KIND_SCRUB, SCRUB_POLICY_INTERVAL_SEC,
};

use crate::store::MetadataState;
use crate::AutumnManager;

/// Which extents an op scrubs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScrubScope {
    Extents(Vec<u64>),
    Partition(u64),
    All,
}

/// Tasks per `MSG_SCRUB_EXTENTS` request. A task is ~40 bytes; this keeps a
/// request far below any frame limit however many extents a node holds.
const TASKS_PER_REQUEST: usize = 4096;

/// An op nothing has been heard about for this long is given up as `UNKNOWN`.
/// "Heard about" is an outcome, or a `df` from a node holding its files that
/// lists it as still queued — so a long wait behind other work keeps it alive,
/// and this fires only when every node holding its files stopped answering.
/// Without it such an op would sit RUNNING and, by attach-dedup, swallow every
/// later submit of the same scope.
pub(crate) const SCRUB_OP_SILENCE_SECS: i64 = 2 * 3600;

/// How long after dispatch a node's `df` may omit an op before its files there
/// are taken as lost. Covers a `df` already in flight when the request landed.
const SCRUB_QUEUE_GRACE_SECS: i64 = 30;

/// What the manager will ask for, and what it will not.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ScrubPlan {
    /// `(node_id, task)`, one per file.
    pub tasks: Vec<(u64, ScrubTask)>,
    /// Named extents the manager does not know.
    pub missing: Vec<u64>,
    /// Extents left alone, with the reason: open, sealed empty, an op in
    /// flight, or a pre-CoW EC layout whose shards live in `.dat`.
    pub not_scrubbed: u64,
    /// Copies left alone because their slot is dark (behind, or isolated and
    /// awaiting a rebuild) — there is nothing settled there to check.
    pub dark_slots: u64,
}

/// Plan the files to scrub. Pure: everything the decision needs is passed in.
pub(crate) fn plan_scrub(
    state: &MetadataState,
    layout: &HashMap<u64, PayloadLocation>,
    op_in_flight: &dyn Fn(u64) -> bool,
    scope: &ScrubScope,
    op_id: u64,
) -> ScrubPlan {
    let mut plan = ScrubPlan::default();
    let ids: Vec<u64> = match scope {
        ScrubScope::Extents(ids) => ids.clone(),
        ScrubScope::Partition(part_id) => {
            let mut ids = Vec::new();
            if let Some(p) = state.partitions.get(part_id) {
                for sid in [p.log_stream, p.row_stream, p.meta_stream] {
                    if let Some(s) = state.streams.get(&sid) {
                        ids.extend(s.extent_ids.iter().copied());
                    }
                }
            }
            ids
        }
        ScrubScope::All => state.extents.keys().copied().collect(),
    };
    let mut ids = ids;
    ids.sort_unstable();
    ids.dedup();
    for extent_id in ids {
        let Some(ex) = state.extents.get(&extent_id) else {
            plan.missing.push(extent_id);
            continue;
        };
        if !ex.sealed || ex.sealed_length == 0 || op_in_flight(extent_id) {
            plan.not_scrubbed += 1;
            continue;
        }
        let location = layout.get(&extent_id).copied().unwrap_or(PayloadLocation::InDat);
        let (location, length) = if ex.ec_converted {
            if location != PayloadLocation::InShardFile {
                plan.not_scrubbed += 1;
                continue;
            }
            (PayloadLocation::InShardFile, shard_len(ex.sealed_length, ex.replicates.len()))
        } else {
            (PayloadLocation::InDat, ex.sealed_length)
        };
        for (slot, node_id) in ex.replicates.iter().chain(ex.parity.iter()).enumerate() {
            if slot >= 32 || ex.avali & (1u32 << slot) == 0 {
                plan.dark_slots += 1;
                continue;
            }
            plan.tasks.push((
                *node_id,
                ScrubTask {
                    extent_id,
                    payload_location: location.as_byte(),
                    shard_index: if ex.ec_converted { slot as u32 } else { 0 },
                    length,
                    eversion: ex.eversion,
                    op_id,
                },
            ));
        }
    }
    plan
}

/// A shard's length: `ceil(sealed_length / K)`, at least 1 — the encoder's
/// `erasure::shard_size` in the stream crate, which this crate does not link.
/// The two must agree or every shard is skipped as "not the length named";
/// `scrub_on_demand.rs`'s EC case (an odd length) fails first.
fn shard_len(sealed_length: u64, data_shards: usize) -> u64 {
    sealed_length.div_ceil(data_shards.max(1) as u64).max(1)
}

/// One running scrub op's accounting.
#[derive(Debug, Default)]
pub(crate) struct ScrubOp {
    /// Files dispatched and not yet reported: `(extent, node, location, shard)`.
    pending: HashSet<(u64, u64, u8, u32)>,
    /// Files reported rotted whose finding could not be judged: the extent
    /// changed after the task was planned, so the eversion it carried no
    /// longer matches, or an op is in flight on it. Rot does not heal; once
    /// the extent settles the file is scrubbed again under its current
    /// eversion. Nothing else asks again — a finding dropped here would wait
    /// for the next scrub, a week on the policy's schedule. Two rotted copies
    /// of one extent hit this every time: the first one's isolation moves the
    /// eversion the second one was planned under.
    recheck: HashSet<(u64, u64, u8, u32)>,
    total: u64,
    /// Indexed by `SCRUB_OUTCOME_*`.
    counts: [u64; 5],
    rotted: Vec<u64>,
    last_heard_s: i64,
    /// Files still pending, per node — so a node's `df` costs a lookup, not a
    /// scan of every pending file in the cluster.
    pending_on: HashMap<u64, u64>,
    /// When each node last ACCEPTED a request of this op: the grace for its
    /// `df` to list the op runs from here, not from when planning started (a
    /// dispatch is sequential, and nodes late in it get their requests late).
    delivered_s: HashMap<u64, i64>,
    /// What planning left out, for the final message.
    note: String,
}

impl ScrubOp {
    /// Files with no outcome yet: on a node, or waiting to be checked again.
    fn open_files(&self) -> u64 {
        (self.pending.len() + self.recheck.len()) as u64
    }

    fn summary(&self) -> String {
        let [clean, described, rot, skipped, failed] = self.counts;
        let mut s = format!(
            "{} file(s): {clean} clean, {described} recorded for the first time, {rot} rotted, \
             {skipped} skipped, {failed} failed",
            self.total
        );
        if !self.rotted.is_empty() {
            s.push_str(&format!(" — rotted extents {:?}", self.rotted));
        }
        if !self.note.is_empty() {
            s.push_str("; ");
            s.push_str(&self.note);
        }
        s
    }

    fn final_state(&self) -> u8 {
        if self.counts[SCRUB_OUTCOME_FAILED as usize] > 0 {
            OP_STATE_FAILED
        } else {
            OP_STATE_SUCCEEDED
        }
    }
}

impl AutumnManager {
    /// Plan, record and dispatch a scrub op. `Ok(message)` = dispatched and
    /// RUNNING until the nodes report; `Err` = nothing was dispatched.
    pub(crate) async fn dispatch_scrub(
        &self,
        op_id: u64,
        scope: ScrubScope,
    ) -> Result<String, String> {
        let plan = {
            let state = self.store.inner.borrow();
            let layout = self.extent_payload_location.borrow();
            let in_flight = |id: u64| self.extent_inflight_op(id).is_some();
            plan_scrub(&state, &layout, &in_flight, &scope, op_id)
        };
        if !plan.missing.is_empty() && matches!(scope, ScrubScope::Extents(_)) {
            return Err(format!("unknown extent(s) {:?}", plan.missing));
        }
        // Group by the EN shard that owns each extent: an extent node runs one
        // listener per shard and a request must reach the owner.
        let mut by_addr: BTreeMap<String, Vec<(u64, ScrubTask)>> = BTreeMap::new();
        let mut unreachable = 0u64;
        for (node_id, task) in plan.tasks {
            match self.scrub_addr(node_id, task.extent_id) {
                Some(addr) => by_addr.entry(addr).or_default().push((node_id, task)),
                None => unreachable += 1,
            }
        }
        let mut note = Vec::new();
        if plan.not_scrubbed > 0 {
            note.push(format!(
                "{} extent(s) not scrubbed (open, empty, an op in flight, or a pre-CoW EC layout)",
                plan.not_scrubbed
            ));
        }
        if plan.dark_slots > 0 {
            note.push(format!("{} dark slot(s) left to recovery", plan.dark_slots));
        }
        if unreachable > 0 {
            note.push(format!("{unreachable} copy(ies) on nodes that are not online"));
        }
        let note = note.join("; ");
        let total: u64 = by_addr.values().map(|v| v.len() as u64).sum();
        if total == 0 {
            return Err(if note.is_empty() {
                "nothing to scrub".to_string()
            } else {
                format!("nothing to scrub: {note}")
            });
        }
        let (now_s, _) = Self::now_s_ms();
        {
            let mut op = ScrubOp {
                total,
                last_heard_s: now_s,
                note: note.clone(),
                ..Default::default()
            };
            for (node_id, t) in by_addr.values().flatten() {
                if op
                    .pending
                    .insert((t.extent_id, *node_id, t.payload_location, t.shard_index))
                {
                    *op.pending_on.entry(*node_id).or_insert(0) += 1;
                }
            }
            self.scrub_ops.borrow_mut().insert(op_id, op);
        }
        self.ops.borrow_mut().update_progress(op_id, 0, total);

        let nodes = by_addr.len();
        for (addr, tasks) in by_addr {
            for chunk in tasks.chunks(TASKS_PER_REQUEST) {
                self.send_scrub_tasks(op_id, &addr, chunk).await;
            }
        }
        Ok(format!("scrubbing {total} file(s) on {nodes} node shard(s)"))
    }

    /// Send one request of `op_id`'s tasks to the node shard at `addr`. A
    /// failed send fails its files now: nothing will report them.
    async fn send_scrub_tasks(&self, op_id: u64, addr: &str, chunk: &[(u64, ScrubTask)]) {
        let req = ScrubExtentsReq {
            tasks: chunk.iter().map(|(_, t)| t.clone()).collect(),
        };
        let sent = self
            .conn_pool
            .call_timeout(addr, MSG_SCRUB_EXTENTS, rkyv_encode(&req), Duration::from_secs(10))
            .await
            .map_err(|e| e.to_string())
            .and_then(|b| {
                rkyv_decode::<autumn_rpc::extent_rpc::CodeResp>(&b)
                    .map_err(|e| e.to_string())
                    .and_then(|r| {
                        if r.code == autumn_rpc::extent_rpc::CODE_OK {
                            Ok(())
                        } else {
                            Err(r.message)
                        }
                    })
            });
        match sent {
            Ok(()) => {
                let (now_s, _) = Self::now_s_ms();
                if let Some(op) = self.scrub_ops.borrow_mut().get_mut(&op_id) {
                    for (node_id, _) in chunk {
                        op.delivered_s.insert(*node_id, now_s);
                    }
                }
            }
            Err(why) => {
                tracing::warn!(op_id, addr = %addr, error = %why, "scrub dispatch failed");
                for (node_id, t) in chunk {
                    self.record_scrub_outcome(
                        *node_id,
                        &ScrubDone {
                            extent_id: t.extent_id,
                            payload_location: t.payload_location,
                            shard_index: t.shard_index,
                            op_id,
                            eversion: t.eversion,
                            outcome: SCRUB_OUTCOME_FAILED,
                            message: format!("dispatch to {addr} failed: {why}"),
                        },
                    );
                }
            }
        }
    }

    /// Where a scrub task for `extent_id` on `node_id` must be sent: the node
    /// shard that owns the extent. `None` = the node is unknown or not online.
    fn scrub_addr(&self, node_id: u64, extent_id: u64) -> Option<String> {
        let state = self.store.inner.borrow();
        let n = state.nodes.get(&node_id)?;
        if !self.node_states.borrow().state_of(node_id).is_online() {
            return None;
        }
        Some(Self::shard_addr_for_extent(&n.address, &n.shard_ports, extent_id))
    }

    /// Hold a rot finding that could not be judged for a re-check, instead of
    /// recording it. `false` = not this leader's op (a leader change, or an op
    /// already given up), or not a file it is waiting on: nothing to hold.
    pub(crate) fn defer_scrub_recheck(&self, node_id: u64, d: &ScrubDone) -> bool {
        let (now_s, _) = Self::now_s_ms();
        let mut ops = self.scrub_ops.borrow_mut();
        let Some(op) = ops.get_mut(&d.op_id) else {
            return false;
        };
        let key = (d.extent_id, node_id, d.payload_location, d.shard_index);
        if !op.pending.remove(&key) {
            return false;
        }
        if let Some(n) = op.pending_on.get_mut(&node_id) {
            *n = n.saturating_sub(1);
        }
        op.recheck.insert(key);
        op.last_heard_s = now_s;
        true
    }

    /// Scrub again the files whose rot finding is held (`ScrubOp::recheck`),
    /// under the eversion the extent holds now. A file stays held while its
    /// extent has an op in flight or its node is not Online — the cases a
    /// re-check exists to wait out. A file the extent no longer has there —
    /// its slot went dark, it was replaced, the extent is gone — is recorded
    /// with the rot it was found with: what it said is no longer about
    /// anything the manager can isolate.
    ///
    /// Synchronous, and the sends are spawned: it runs inside the `df` round,
    /// which a dead shard listener must not hold for a send timeout per file,
    /// and with no await between the in-flight check and the re-plan nothing
    /// can start an op on the extent in between.
    pub(crate) fn recheck_scrub_findings(&self) {
        let held: Vec<(u64, (u64, u64, u8, u32))> = {
            let ops = self.scrub_ops.borrow();
            ops.iter()
                .flat_map(|(op_id, op)| op.recheck.iter().map(move |k| (*op_id, *k)))
                .collect()
        };
        for (op_id, key) in held {
            let (extent_id, node_id, payload_location, shard_index) = key;
            if self.extent_inflight_op(extent_id).is_some() {
                continue;
            }
            let Some(addr) = self.scrub_addr(node_id, extent_id) else {
                continue;
            };
            let task = {
                let state = self.store.inner.borrow();
                let layout = self.extent_payload_location.borrow();
                let in_flight = |id: u64| self.extent_inflight_op(id).is_some();
                plan_scrub(&state, &layout, &in_flight, &ScrubScope::Extents(vec![extent_id]), op_id)
                    .tasks
                    .into_iter()
                    .find(|(n, t)| {
                        *n == node_id
                            && t.payload_location == payload_location
                            && t.shard_index == shard_index
                    })
                    .map(|(_, t)| t)
            };
            {
                let mut ops = self.scrub_ops.borrow_mut();
                let Some(op) = ops.get_mut(&op_id) else {
                    continue;
                };
                if !op.recheck.remove(&key) {
                    continue;
                }
                op.pending.insert(key);
                *op.pending_on.entry(node_id).or_insert(0) += 1;
            }
            match task {
                Some(task) => {
                    tracing::info!(
                        op_id,
                        extent_id,
                        node_id,
                        eversion = task.eversion,
                        "scrub: checking a rotted copy again under the extent's current eversion"
                    );
                    let mgr = self.clone();
                    compio::runtime::spawn(async move {
                        mgr.send_scrub_tasks(op_id, &addr, &[(node_id, task)]).await
                    })
                    .detach();
                }
                None => {
                    let message = "found rotted, but the copy is no longer one to isolate (its \
                                   slot went dark, or it was replaced or deleted)";
                    tracing::warn!(op_id, extent_id, node_id, "scrub: {message}");
                    self.record_scrub_outcome(
                        node_id,
                        &ScrubDone {
                            extent_id,
                            payload_location,
                            shard_index,
                            op_id,
                            eversion: 0,
                            outcome: SCRUB_OUTCOME_ROT,
                            message: message.to_string(),
                        },
                    );
                }
            }
        }
    }

    /// Apply one outcome a node reported on `df` (or a dispatch failure).
    /// Outcomes for an op this leader does not know — a leader change, or an
    /// op already given up — are dropped (a rot finding among them has already
    /// been acted on by the caller).
    pub(crate) fn record_scrub_outcome(&self, node_id: u64, d: &ScrubDone) {
        let (now_s, _) = Self::now_s_ms();
        let finished = {
            let mut ops = self.scrub_ops.borrow_mut();
            let Some(op) = ops.get_mut(&d.op_id) else {
                return;
            };
            if !op
                .pending
                .remove(&(d.extent_id, node_id, d.payload_location, d.shard_index))
            {
                return;
            }
            if let Some(n) = op.pending_on.get_mut(&node_id) {
                *n = n.saturating_sub(1);
            }
            op.last_heard_s = now_s;
            if let Some(c) = op.counts.get_mut(d.outcome as usize) {
                *c += 1;
            }
            match d.outcome {
                SCRUB_OUTCOME_ROT if !op.rotted.contains(&d.extent_id) => op.rotted.push(d.extent_id),
                SCRUB_OUTCOME_FAILED | SCRUB_OUTCOME_SKIPPED => tracing::info!(
                    op_id = d.op_id,
                    extent_id = d.extent_id,
                    node_id,
                    outcome = d.outcome,
                    "scrub: {}",
                    d.message
                ),
                SCRUB_OUTCOME_CLEAN | SCRUB_OUTCOME_DESCRIBED | SCRUB_OUTCOME_ROT => {}
                _ => {}
            }
            let done = op.total - op.open_files();
            self.ops.borrow_mut().update_progress(d.op_id, done, op.total);
            if op.open_files() == 0 {
                ops.remove(&d.op_id)
            } else {
                None
            }
        };
        if let Some(op) = finished {
            let state = op.final_state();
            let summary = op.summary();
            tracing::info!(op_id = d.op_id, "scrub finished: {summary}");
            let error = if state == OP_STATE_FAILED {
                summary.clone()
            } else {
                String::new()
            };
            self.ops
                .borrow_mut()
                .finish(d.op_id, state, error, summary, now_s);
        }
    }

    /// The weekly `scrub` advisory: one cluster-wide row once a week has passed
    /// since the policy last scrubbed, and none while any scrub is running.
    /// The cadence itself is the actuation cooldown (`decide_actions`), which
    /// is persisted, so this only keeps a row from sitting in the advisory
    /// list for the six days it could not act.
    pub(crate) fn scrub_candidates(&self, now_s: i64) -> Vec<PolicyCandidate> {
        if !self.scrub_ops.borrow().is_empty() {
            return Vec::new();
        }
        let last = self
            .auto_policy
            .borrow()
            .cooldowns
            .get("scrub:0")
            .copied()
            .unwrap_or(0);
        let since = now_s - last;
        if since < SCRUB_POLICY_INTERVAL_SEC as i64 {
            return Vec::new();
        }
        let reason = if last == 0 {
            "no scheduled scrub yet: check every sealed copy against its checksums".to_string()
        } else {
            format!(
                "last scheduled scrub {} day(s) ago: check every sealed copy against its checksums",
                since / 86_400
            )
        };
        vec![PolicyCandidate {
            kind: POLICY_KIND_SCRUB,
            primary_part_id: 0,
            secondary_part_id: 0,
            reason,
            size_bytes: 0,
            req_per_sec: 0,
            imm_full_per_sec: 0,
            same_ps: false,
            last_op_at: 0,
        }]
    }

    /// Submit a whole-cluster scrub through the op ledger, so it is listed and
    /// followed like an operator's. A scrub already running is attached to.
    pub(crate) fn submit_scrub_all(&self, requested_by: &str) -> u64 {
        let (now_s, now_ms) = Self::now_s_ms();
        let (op_id, attached) = self.ops.borrow_mut().submit(
            OP_KIND_SCRUB,
            0,
            0,
            Vec::new(),
            requested_by.to_string(),
            now_s,
            now_ms,
        );
        if !attached {
            let mgr = self.clone();
            let spec = OpSubmitReq {
                kind: OP_KIND_SCRUB,
                requested_by: requested_by.to_string(),
                ..Default::default()
            };
            compio::runtime::spawn(async move { mgr.run_submitted_op(op_id, spec).await }).detach();
        }
        op_id
    }

    /// Apply a node's `scrub_queued` (from the same `df`, AFTER its outcomes).
    ///
    /// An op listed there is alive, however long its files wait behind other
    /// work. An op with files pending on this node that the node does NOT list
    /// has lost them — the node restarted (its queue is in memory), or the
    /// report carrying their outcomes was lost — and they are failed now rather
    /// than after the silence timeout. Only once the node has accepted the
    /// op's request, and not within a grace after that: a `df` answered before
    /// the request landed, and applied late, would not list it yet.
    pub(crate) fn record_scrub_queued(&self, node_id: u64, queued: &[u64]) {
        let (now_s, _) = Self::now_s_ms();
        let mut lost: Vec<ScrubDone> = Vec::new();
        {
            let mut ops = self.scrub_ops.borrow_mut();
            for (op_id, op) in ops.iter_mut() {
                if op.pending_on.get(&node_id).copied().unwrap_or(0) == 0 {
                    continue;
                }
                if queued.contains(op_id) {
                    op.last_heard_s = now_s;
                    continue;
                }
                let Some(delivered) = op.delivered_s.get(&node_id).copied() else {
                    continue;
                };
                if now_s - delivered > SCRUB_QUEUE_GRACE_SECS {
                    let here: Vec<(u64, u64, u8, u32)> = op
                        .pending
                        .iter()
                        .filter(|(_, n, _, _)| *n == node_id)
                        .copied()
                        .collect();
                    for (extent_id, _, payload_location, shard_index) in here {
                        lost.push(ScrubDone {
                            extent_id,
                            payload_location,
                            shard_index,
                            op_id: *op_id,
                            eversion: 0,
                            outcome: SCRUB_OUTCOME_FAILED,
                            message: format!(
                                "node {node_id} no longer has it queued (it restarted, or the \
                                 report of its outcome was lost)"
                            ),
                        });
                    }
                }
            }
        }
        for d in lost {
            self.record_scrub_outcome(node_id, &d);
        }
    }

    /// Give up ops nothing has been heard about for `SCRUB_OP_SILENCE_SECS`.
    pub(crate) fn sweep_silent_scrub_ops(&self, now_s: i64) {
        let silent: Vec<(u64, ScrubOp)> = {
            let mut ops = self.scrub_ops.borrow_mut();
            let ids: Vec<u64> = ops
                .iter()
                .filter(|(_, op)| now_s - op.last_heard_s > SCRUB_OP_SILENCE_SECS)
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| ops.remove(&id).map(|op| (id, op)))
                .collect()
        };
        for (op_id, op) in silent {
            let why = format!(
                "no outcome for {} s; {} file(s) never reported (a node restarted, or its \
                 report was lost), {} rot finding(s) still held (their extent kept an op in \
                 flight, or their node stayed offline) — {}",
                SCRUB_OP_SILENCE_SECS,
                op.pending.len(),
                op.recheck.len(),
                op.summary()
            );
            tracing::warn!(op_id, "scrub given up: {why}");
            self.ops
                .borrow_mut()
                .finish(op_id, OP_STATE_UNKNOWN, why.clone(), why, now_s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::records::{ExtentRecord, PartitionRecord, StreamRecord};

    fn extent(id: u64, nodes: Vec<u64>, parity: Vec<u64>, sealed: u64, avali: u32) -> ExtentRecord {
        ExtentRecord {
            extent_id: id,
            replicates: nodes,
            parity,
            eversion: 4,
            refs: 1,
            vp_table_refs: 0,
            sealed_length: sealed,
            sealed: sealed > 0,
            avali,
            replicate_disks: vec![],
            parity_disks: vec![],
            ec_converted: false,
        }
    }

    fn state() -> MetadataState {
        let mut s = MetadataState::default();
        // Replicated, three copies, one dark.
        s.extents.insert(1, extent(1, vec![10, 11, 12], vec![], 5000, 0b101));
        // EC 2+1 over shard files.
        let mut ec = extent(2, vec![20, 21], vec![22], 10_001, 0b111);
        ec.ec_converted = true;
        s.extents.insert(2, ec);
        // Open.
        s.extents.insert(3, extent(3, vec![10, 11], vec![], 0, 0));
        // Pre-CoW EC (shards in .dat).
        let mut legacy = extent(4, vec![20, 21], vec![22], 900, 0b111);
        legacy.ec_converted = true;
        s.extents.insert(4, legacy);
        s.streams.insert(
            100,
            StreamRecord {
                stream_id: 100,
                extent_ids: vec![1, 3],
                ec_data_shard: 0,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        s.partitions.insert(
            7,
            PartitionRecord {
                part_id: 7,
                log_stream: 100,
                row_stream: 101,
                meta_stream: 102,
                rg: None,
            },
        );
        s
    }

    fn layout() -> HashMap<u64, PayloadLocation> {
        [(2u64, PayloadLocation::InShardFile)].into_iter().collect()
    }

    /// Each lit copy of a sealed extent is one task naming its own file at its
    /// own length: the sealed length for a replica, a shard's length and the
    /// slot's index for a shard. Dark copies, open extents and pre-CoW EC
    /// layouts are left out and counted.
    #[test]
    fn every_lit_copy_of_a_sealed_extent_is_one_task_for_its_own_file() {
        let plan = plan_scrub(&state(), &layout(), &|_| false, &ScrubScope::All, 9);
        let mut got: Vec<(u64, u64, u8, u32, u64)> = plan
            .tasks
            .iter()
            .map(|(n, t)| (*n, t.extent_id, t.payload_location, t.shard_index, t.length))
            .collect();
        got.sort();
        let dat = PayloadLocation::InDat.as_byte();
        let shard = PayloadLocation::InShardFile.as_byte();
        let shard_len = 5001;
        assert_eq!(
            got,
            vec![
                (10, 1, dat, 0, 5000),
                (12, 1, dat, 0, 5000),
                (20, 2, shard, 0, shard_len),
                (21, 2, shard, 1, shard_len),
                (22, 2, shard, 2, shard_len),
            ]
        );
        assert!(plan.tasks.iter().all(|(_, t)| t.op_id == 9 && t.eversion == 4));
        assert_eq!(plan.dark_slots, 1);
        assert_eq!(plan.not_scrubbed, 2, "the open extent and the pre-CoW layout");
    }

    #[test]
    fn a_partition_scope_covers_its_streams_and_named_extents_report_the_unknown() {
        let plan = plan_scrub(&state(), &layout(), &|_| false, &ScrubScope::Partition(7), 1);
        assert!(plan.tasks.iter().all(|(_, t)| t.extent_id == 1));
        assert_eq!(plan.tasks.len(), 2);

        let plan = plan_scrub(&state(), &layout(), &|_| false, &ScrubScope::Extents(vec![2, 99]), 1);
        assert_eq!(plan.missing, vec![99]);
        assert_eq!(plan.tasks.len(), 3);
    }

    fn op_record(m: &AutumnManager, op_id: u64) -> autumn_rpc::manager_rpc::OpRecord {
        m.ops
            .borrow()
            .query(&autumn_rpc::manager_rpc::OpQueryReq {
                op_id,
                ..Default::default()
            })
            .remove(0)
    }

    fn running_scrub(m: &AutumnManager, files: &[(u64, u64)]) -> u64 {
        let (op_id, _) = m.ops.borrow_mut().submit(OP_KIND_SCRUB, 0, 0, vec![], "test".into(), 0, 0);
        m.ops.borrow_mut().set_running(op_id, 0);
        let now = AutumnManager::now_s_ms().0;
        let mut op = ScrubOp {
            total: files.len() as u64,
            last_heard_s: now,
            ..Default::default()
        };
        for (extent, node) in files {
            op.pending.insert((*extent, *node, PayloadLocation::InDat.as_byte(), 0));
            *op.pending_on.entry(*node).or_insert(0) += 1;
            op.delivered_s.insert(*node, now);
        }
        m.scrub_ops.borrow_mut().insert(op_id, op);
        op_id
    }

    fn done(extent_id: u64, op_id: u64, outcome: u8) -> ScrubDone {
        ScrubDone {
            extent_id,
            payload_location: PayloadLocation::InDat.as_byte(),
            shard_index: 0,
            op_id,
            eversion: 0,
            outcome,
            message: String::new(),
        }
    }

    /// The op runs until every file dispatched has reported, counting each
    /// file once; it succeeds whatever was FOUND (rot is a result, isolated
    /// separately) and fails only if a file could not be checked.
    #[test]
    fn an_op_finishes_when_every_file_has_reported_once() {
        let m = AutumnManager::new();
        let op = running_scrub(&m, &[(1, 10), (1, 11), (2, 10)]);
        m.record_scrub_outcome(10, &done(1, op, SCRUB_OUTCOME_CLEAN));
        m.record_scrub_outcome(10, &done(1, op, SCRUB_OUTCOME_CLEAN));
        m.record_scrub_outcome(99, &done(2, op, SCRUB_OUTCOME_CLEAN));
        assert_eq!(op_record(&m, op).state, autumn_rpc::manager_rpc::OP_STATE_RUNNING);
        m.record_scrub_outcome(11, &done(1, op, SCRUB_OUTCOME_ROT));
        m.record_scrub_outcome(10, &done(2, op, SCRUB_OUTCOME_DESCRIBED));
        let r = op_record(&m, op);
        assert_eq!(r.state, OP_STATE_SUCCEEDED, "{}", r.message);
        assert!(r.message.contains("1 clean, 1 recorded for the first time, 1 rotted"), "{}", r.message);
        assert!(m.scrub_ops.borrow().is_empty());

        let op = running_scrub(&m, &[(3, 10)]);
        m.record_scrub_outcome(10, &done(3, op, SCRUB_OUTCOME_FAILED));
        assert_eq!(op_record(&m, op).state, OP_STATE_FAILED);
    }

    /// Outcomes are at-most-once: a node that restarted never reports what it
    /// had queued, and an op left RUNNING would swallow every later submit of
    /// its scope. Silence ends it as UNKNOWN.
    #[test]
    fn a_node_listing_an_op_keeps_it_alive_and_one_that_lost_it_fails_its_files() {
        let m = AutumnManager::new();
        let op = running_scrub(&m, &[(1, 10), (2, 11)]);
        let now = AutumnManager::now_s_ms().0;
        // Inside the grace, an omission says nothing.
        m.record_scrub_queued(10, &[]);
        assert_eq!(op_record(&m, op).state, autumn_rpc::manager_rpc::OP_STATE_RUNNING);
        {
            let mut ops = m.scrub_ops.borrow_mut();
            let o = ops.get_mut(&op).unwrap();
            o.delivered_s.insert(10, now - 3600);
            o.delivered_s.insert(11, now - 3600);
            o.last_heard_s = now - SCRUB_OP_SILENCE_SECS + 5;
        }
        // Node 11 still has it queued: alive, and the silence clock restarts.
        m.record_scrub_queued(11, &[op]);
        m.sweep_silent_scrub_ops(now + 10);
        assert_eq!(op_record(&m, op).state, autumn_rpc::manager_rpc::OP_STATE_RUNNING);
        // Node 10 answers without it: its file is lost, failed now.
        m.record_scrub_queued(10, &[]);
        assert_eq!(op_record(&m, op).state, autumn_rpc::manager_rpc::OP_STATE_RUNNING);
        m.record_scrub_outcome(11, &done(2, op, SCRUB_OUTCOME_CLEAN));
        let r = op_record(&m, op);
        assert_eq!(r.state, OP_STATE_FAILED, "{}", r.message);
        assert!(r.message.contains("1 failed"), "{}", r.message);
    }

    /// A rot finding that could not be judged is held, not counted: the op
    /// stays open for it, the node that no longer has it queued does not fail
    /// it, and only the re-check settles it.
    #[test]
    fn a_held_rot_finding_keeps_its_op_open_until_rechecked() {
        let m = AutumnManager::new();
        let op = running_scrub(&m, &[(1, 10), (2, 11)]);
        assert!(m.defer_scrub_recheck(10, &done(1, op, SCRUB_OUTCOME_ROT)));
        assert!(!m.defer_scrub_recheck(10, &done(1, op, SCRUB_OUTCOME_ROT)), "held once");
        assert!(!m.defer_scrub_recheck(10, &done(1, 999, SCRUB_OUTCOME_ROT)), "unknown op");
        m.record_scrub_outcome(11, &done(2, op, SCRUB_OUTCOME_CLEAN));
        {
            let mut ops = m.scrub_ops.borrow_mut();
            ops.get_mut(&op).unwrap().delivered_s.insert(10, AutumnManager::now_s_ms().0 - 3600);
        }
        m.record_scrub_queued(10, &[]);
        let r = op_record(&m, op);
        assert_eq!(r.state, autumn_rpc::manager_rpc::OP_STATE_RUNNING, "{}", r.message);
        // Node 10 is not Online: the finding waits for it rather than being
        // settled unchecked.
        m.recheck_scrub_findings();
        let r = op_record(&m, op);
        assert_eq!(r.state, autumn_rpc::manager_rpc::OP_STATE_RUNNING, "{}", r.message);
        m.store.inner.borrow_mut().nodes.insert(
            10,
            crate::persist::records::NodeRecord {
                node_id: 10,
                address: "127.0.0.1:1".into(),
                disks: vec![],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            },
        );
        m.node_states.borrow_mut().on_heartbeat_ok(10);
        // Online now, but extent 1 is unknown here, so the re-check finds
        // nothing to scrub and settles the finding as the rot it was.
        m.recheck_scrub_findings();
        let r = op_record(&m, op);
        assert_eq!(r.state, OP_STATE_SUCCEEDED, "{}", r.message);
        assert!(r.message.contains("1 clean, 0 recorded for the first time, 1 rotted"), "{}", r.message);
    }

    #[test]
    fn an_op_nothing_reports_on_is_given_up() {
        let m = AutumnManager::new();
        let op = running_scrub(&m, &[(1, 10)]);
        let now = AutumnManager::now_s_ms().0;
        m.sweep_silent_scrub_ops(now);
        assert_eq!(op_record(&m, op).state, autumn_rpc::manager_rpc::OP_STATE_RUNNING);
        m.sweep_silent_scrub_ops(now + SCRUB_OP_SILENCE_SECS + 1);
        assert_eq!(op_record(&m, op).state, OP_STATE_UNKNOWN);
        assert!(m.scrub_ops.borrow().is_empty());
    }

    /// An extent a recovery or conversion is changing is not scrubbed now.
    #[test]
    fn an_extent_with_an_op_in_flight_is_left_alone() {
        let plan = plan_scrub(&state(), &layout(), &|id| id == 1, &ScrubScope::All, 1);
        assert!(plan.tasks.iter().all(|(_, t)| t.extent_id != 1));
    }
}
