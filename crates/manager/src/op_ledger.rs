//! The manager's async op-ledger.
//!
//! A leader-local, in-memory, bounded record of every submitted long-running op
//! (split / merge / rebalance / compact / gc / forcegc / ec-convert) and its live
//! state. This is the OBSERVABILITY half — it recovers the failure reason that the
//! fire-and-forget maintenance ops used to drop into a `tracing::error!`. The
//! ORCHESTRATION crash-safety lives elsewhere (fenced split/merge txns, EC
//! inflight markers), so the ledger need not be durable: terminal outcomes are
//! ALSO appended to the etcd audit log by the caller, and a query for an
//! unknown/old id answers `UNKNOWN`, never a false `RUNNING`.
//!
//! Pure data structure — the manager passes wall-clock in (`now_s` / `now_ms`),
//! so this is unit-testable without a clock or a cluster.

use std::collections::VecDeque;

use autumn_rpc::manager_rpc::{
    OpQueryReq, OpRecord, OP_KIND_COMPACT, OP_KIND_EC_CONVERT, OP_KIND_FORCE_GC, OP_KIND_GC,
    OP_KIND_RECOVERY, OP_STATE_PENDING, OP_STATE_RUNNING, OP_STATE_SUCCEEDED, OP_STATE_UNKNOWN,
};

/// Max live+recent entries kept (newest-first). Mirrors the auto-policy action
/// log's bounded-ring pattern.
pub(crate) const OP_LEDGER_CAP: usize = 256;

/// A RUNNING PS-executed op (compact/gc/forcegc) whose terminal outcome never
/// came back (PS restarted / heartbeat missed) is flipped to UNKNOWN after this
/// long, so `ops status` stays honest instead of RUNNING forever.
pub(crate) const OP_RUNNING_TTL_SECS: i64 = 30 * 60;

#[derive(Default)]
pub(crate) struct OpLedger {
    /// newest at the front.
    entries: VecDeque<OpRecord>,
    seq: u16,
    /// Terminal records not yet written to durable history (see `op_log`).
    pending_log: Vec<OpRecord>,
}

impl OpLedger {
    /// Terminal records awaiting a write to durable history. Filled
    /// synchronously wherever an op goes terminal, drained by an async caller —
    /// so the ledger stays a plain synchronous structure and the etcd write
    /// never sits inside a `borrow_mut`.
    pub(crate) fn drain_pending_log(&mut self) -> Vec<OpRecord> {
        std::mem::take(&mut self.pending_log)
    }

    /// Queue `op_id`'s current record for history, if it is terminal.
    fn queue_terminal(&mut self, op_id: u64) {
        if let Some(e) = self.entries.iter().find(|e| e.op_id == op_id) {
            if e.state != OP_STATE_RUNNING && e.state != OP_STATE_PENDING {
                self.pending_log.push(e.clone());
            }
        }
    }

    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// op_id = time-sortable, unique across leader incarnations without an etcd
    /// counter: `(epoch_ms << 16) | seq16`. `now_ms.max(1)` keeps it non-zero
    /// (0 is the `OpQueryReq` "list" sentinel) — a no-op in production, where the
    /// clock is always far past the epoch.
    fn next_id(&mut self, now_ms: i64) -> u64 {
        let s = self.seq;
        self.seq = self.seq.wrapping_add(1);
        ((now_ms.max(1) as u64) << 16) | (s as u64)
    }

    fn find_mut(&mut self, op_id: u64) -> Option<&mut OpRecord> {
        self.entries.iter_mut().find(|e| e.op_id == op_id)
    }

    fn is_active(state: u8) -> bool {
        state == OP_STATE_PENDING || state == OP_STATE_RUNNING
    }

    /// May a PS-reported terminal outcome still be written onto this entry?
    ///
    /// Active states, obviously — but UNKNOWN too. UNKNOWN is not a verdict,
    /// it is this ledger admitting it lost track (`sweep_running_ttl`), and the
    /// PS keeps retransmitting the real outcome on every 5 s load report. If
    /// UNKNOWN were terminal the truth would be refused forever and never reach
    /// the audit log, which is where this module promises terminal outcomes
    /// live. A REAL terminal state is not superseded: a retransmit after a
    /// genuine transition stays an idempotent no-op.
    fn accepts_terminal_report(state: u8) -> bool {
        Self::is_active(state) || state == OP_STATE_UNKNOWN
    }

    /// Submit a new op, or ATTACH to an in-flight one with the same target
    /// `(kind, part_id, secondary_id)`. Returns `(op_id, attached)`; attach turns
    /// an impatient re-run into a status watch (the PS cap-1 channels + the
    /// manager's merge/EC guards remain the real double-dispatch protection).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn submit(
        &mut self,
        kind: u8,
        part_id: u64,
        secondary_id: u64,
        extent_ids: Vec<u64>,
        requested_by: String,
        now_s: i64,
        now_ms: i64,
    ) -> (u64, bool) {
        if let Some(e) = self.entries.iter().find(|e| {
            Self::is_active(e.state)
                && e.kind == kind
                && e.part_id == part_id
                && e.secondary_id == secondary_id
        }) {
            return (e.op_id, true);
        }
        let op_id = self.next_id(now_ms);
        self.entries.push_front(OpRecord {
            op_id,
            progress_done: 0,
            progress_total: 0,
            kind,
            part_id,
            secondary_id,
            extent_ids,
            state: OP_STATE_PENDING,
            error: String::new(),
            error_code: 0,
            attempts: 0,
            message: String::new(),
            requested_by,
            submitted_at: now_s,
            started_at: 0,
            finished_at: 0,
        });
        while self.entries.len() > OP_LEDGER_CAP {
            self.entries.pop_back();
        }
        (op_id, false)
    }

    pub(crate) fn set_running(&mut self, op_id: u64, now_s: i64) {
        if let Some(e) = self.find_mut(op_id) {
            if e.state == OP_STATE_PENDING {
                e.state = OP_STATE_RUNNING;
                e.started_at = now_s;
            }
        }
    }

    /// Update the human message of a still-active op without changing its state
    /// (e.g. the forcegc replay-floor advisory, which is known at dispatch time
    /// but the op stays RUNNING until the PS reports the terminal outcome).
    pub(crate) fn set_message(&mut self, op_id: u64, message: String) {
        if let Some(e) = self.find_mut(op_id) {
            if Self::is_active(e.state) {
                e.message = message;
            }
        }
    }

    /// Move a known, still-active op to a terminal state. No-op if the op is gone
    /// (evicted) or already terminal (idempotent — heartbeat outcomes retransmit).
    pub(crate) fn finish(
        &mut self,
        op_id: u64,
        state: u8,
        error: String,
        message: String,
        now_s: i64,
    ) {
        let mut went_terminal = false;
        if let Some(e) = self.find_mut(op_id) {
            if Self::is_active(e.state) {
                e.state = state;
                e.error = error;
                if !message.is_empty() {
                    e.message = message;
                }
                e.finished_at = now_s;
                if e.started_at == 0 {
                    e.started_at = now_s;
                }
                went_terminal = true;
            }
        }
        if went_terminal {
            self.queue_terminal(op_id);
        }
    }

    /// Reconcile a PS-reported terminal maintenance outcome. Returns `true` iff
    /// this call actually moved a KNOWN op that was still accepting a report
    /// (active, or UNKNOWN — see `accepts_terminal_report`) to terminal — so
    /// the caller audits exactly once. A retransmit onto an already-terminal
    /// entry, or an unknown op_id, returns `false`: an idempotent no-op.
    pub(crate) fn reconcile_outcome(
        &mut self,
        op_id: u64,
        state: u8,
        error: String,
        message: String,
        now_s: i64,
    ) -> bool {
        let mut moved = false;
        if let Some(e) = self.find_mut(op_id) {
            if Self::accepts_terminal_report(e.state) {
                e.state = state;
                e.error = error;
                if !message.is_empty() {
                    e.message = message;
                }
                e.finished_at = now_s;
                if e.started_at == 0 {
                    e.started_at = now_s;
                }
                moved = true;
            }
        }
        if moved {
            self.queue_terminal(op_id);
        }
        moved
    }

    /// Close the extent-scoped entry of `kind` whose target extent matches.
    /// Identity is exact (extent_id) and the manager orchestrates both EC and
    /// recovery, so this is authoritative, not inference.
    fn complete_by_extent(
        &mut self,
        kind: u8,
        extent_id: u64,
        state: u8,
        message: String,
        error: String,
        now_s: i64,
    ) {
        let mut closed = None;
        if let Some(e) = self.entries.iter_mut().find(|e| {
            e.kind == kind && e.secondary_id == extent_id && Self::is_active(e.state)
        }) {
            e.state = state;
            e.message = message;
            e.error = error;
            if state == OP_STATE_SUCCEEDED {
                e.error_code = 0;
            }
            e.finished_at = now_s;
            if e.started_at == 0 {
                e.started_at = now_s;
            }
            closed = Some(e.op_id);
        }
        if let Some(id) = closed {
            self.queue_terminal(id);
        }
    }

    /// Close the EC-convert entry for `extent_id` (apply-done, or abandon).
    pub(crate) fn complete_ec(
        &mut self,
        extent_id: u64,
        state: u8,
        message: String,
        error: String,
        now_s: i64,
    ) {
        self.complete_by_extent(OP_KIND_EC_CONVERT, extent_id, state, message, error, now_s);
    }

    /// Close the recovery entry for `extent_id` — called from
    /// `apply_recovery_done` (the extent layout is repaired).
    /// Close a recovery entry that ended WITHOUT completing — its marker was
    /// dropped, so nothing will report it done.
    pub(crate) fn abandon_recovery(&mut self, extent_id: u64, reason: String, now_s: i64) {
        self.complete_by_extent(
            OP_KIND_RECOVERY,
            extent_id,
            autumn_rpc::manager_rpc::OP_STATE_FAILED,
            String::new(),
            reason,
            now_s,
        );
    }

    pub(crate) fn complete_recovery(&mut self, extent_id: u64, message: String, now_s: i64) {
        self.complete_by_extent(
            OP_KIND_RECOVERY,
            extent_id,
            OP_STATE_SUCCEEDED,
            message,
            String::new(),
            now_s,
        );
    }

    /// A recovery dispatch was ACCEPTED by a target EN: create-or-refresh the
    /// extent's RUNNING entry and count the attempt. Recovery is auto-dispatched
    /// and retried, so one entry per extent accumulates attempts rather than
    /// spawning an entry per try.
    pub(crate) fn note_recovery_dispatch(
        &mut self,
        extent_id: u64,
        slot: u32,
        node_id: u64,
        now_s: i64,
        now_ms: i64,
    ) {
        let msg = format!("rebuilding slot {slot} on node {node_id}");
        if let Some(e) = self.entries.iter_mut().find(|e| {
            e.kind == OP_KIND_RECOVERY && e.secondary_id == extent_id && Self::is_active(e.state)
        }) {
            e.attempts = e.attempts.saturating_add(1);
            e.message = msg;
            e.state = OP_STATE_RUNNING;
            if e.started_at == 0 {
                e.started_at = now_s;
            }
            return;
        }
        let op_id = self.next_id(now_ms);
        self.entries.push_front(OpRecord {
            op_id,
            kind: OP_KIND_RECOVERY,
            secondary_id: extent_id,
            state: OP_STATE_RUNNING,
            message: msg,
            attempts: 1,
            requested_by: "auto-recovery".to_string(),
            submitted_at: now_s,
            started_at: now_s,
            ..Default::default()
        });
        while self.entries.len() > OP_LEDGER_CAP {
            self.entries.pop_back();
        }
    }

    /// A recovery dispatch attempt FAILED. The entry stays RUNNING (the loop
    /// retries with exponential backoff — it never gives up), carrying the last
    /// reason + code + the consecutive-failure count, so `ops status` shows
    /// "running, N attempts, last error: …" instead of hiding the churn.
    pub(crate) fn record_recovery_failure(
        &mut self,
        extent_id: u64,
        reason: String,
        error_code: u8,
        consecutive_failures: u32,
        now_s: i64,
        now_ms: i64,
    ) {
        if let Some(e) = self.entries.iter_mut().find(|e| {
            e.kind == OP_KIND_RECOVERY && e.secondary_id == extent_id && Self::is_active(e.state)
        }) {
            e.error = reason;
            e.error_code = error_code;
            e.attempts = consecutive_failures.max(e.attempts);
            return;
        }
        // First observation of this extent is a FAILURE (never got as far as an
        // accepted dispatch) — still worth listing: a repair that can't even
        // start is exactly what an operator needs to see.
        let op_id = self.next_id(now_ms);
        self.entries.push_front(OpRecord {
            op_id,
            kind: OP_KIND_RECOVERY,
            secondary_id: extent_id,
            state: OP_STATE_RUNNING,
            error: reason,
            error_code,
            attempts: consecutive_failures.max(1),
            requested_by: "auto-recovery".to_string(),
            submitted_at: now_s,
            started_at: now_s,
            ..Default::default()
        });
        while self.entries.len() > OP_LEDGER_CAP {
            self.entries.pop_back();
        }
    }

    /// On leader promotion, seed a synthetic RUNNING entry per in-flight
    /// extent-scoped marker replayed from etcd (`kind` = EC_CONVERT or RECOVERY).
    /// Both are DURABLE (the etcd marker survived; the new leader keeps working
    /// the task), so the work is genuinely still running and belongs in
    /// `ops list` — even though the original op_id died with the previous
    /// leader's in-memory ledger. Each closes normally via
    /// `complete_ec` / `complete_recovery`. compact/gc/forcegc are PS-local (NOT
    /// in etcd) and cannot be seeded — an unknown id honestly answers UNKNOWN.
    /// Idempotent across re-promotions (skips an already-tracked extent).
    pub(crate) fn seed_replay(
        &mut self,
        kind: u8,
        extent_ids: impl IntoIterator<Item = u64>,
        now_s: i64,
        now_ms: i64,
    ) {
        for extent_id in extent_ids {
            if self
                .entries
                .iter()
                .any(|e| e.kind == kind && e.secondary_id == extent_id && Self::is_active(e.state))
            {
                continue;
            }
            let op_id = self.next_id(now_ms);
            self.entries.push_front(OpRecord {
                op_id,
                kind,
                secondary_id: extent_id,
                state: OP_STATE_RUNNING,
                requested_by: "replay".to_string(),
                submitted_at: now_s,
                started_at: now_s,
                ..Default::default()
            });
        }
        while self.entries.len() > OP_LEDGER_CAP {
            self.entries.pop_back();
        }
    }

    /// Record the latest progress sample for a RUNNING op. Ignores unknown ids
    /// and terminal entries: a sample that arrives after the outcome (the PS
    /// keeps resending its ring) must not reopen a closed op or resurrect one
    /// the cap already evicted.
    pub(crate) fn update_progress(&mut self, op_id: u64, done: u64, total: u64) {
        if op_id == 0 {
            return;
        }
        if let Some(e) = self
            .entries
            .iter_mut()
            .find(|e| e.op_id == op_id && e.state == OP_STATE_RUNNING)
        {
            e.progress_done = done;
            e.progress_total = total;
        }
    }

    /// Apply a progress sample the EXTENT NODE reported for an extent-scoped
    /// op (EC conversion / recovery).
    ///
    /// Keyed by extent because the node never learns the manager's op id, while
    /// both kinds carry their extent in `secondary_id` here. Only a RUNNING
    /// entry is touched: a sample that arrives after the op closed must not
    /// re-animate a terminal record.
    pub(crate) fn update_progress_by_extent(&mut self, kind: u8, extent_id: u64, done: u64, total: u64) {
        if let Some(e) = self.entries.iter_mut().find(|e| {
            e.kind == kind && e.secondary_id == extent_id && e.state == OP_STATE_RUNNING
        }) {
            e.progress_done = done;
            e.progress_total = total;
        }
    }

    /// TTL backstop: a RUNNING PS-executed op (compact/gc/forcegc) whose terminal
    /// outcome never came back becomes UNKNOWN, keeping `ops status` honest.
    pub(crate) fn sweep_running_ttl(&mut self, now_s: i64) {
        for e in self.entries.iter_mut() {
            if e.state == OP_STATE_RUNNING
                && matches!(e.kind, OP_KIND_COMPACT | OP_KIND_GC | OP_KIND_FORCE_GC)
                && now_s - e.started_at > OP_RUNNING_TTL_SECS
            {
                e.state = OP_STATE_UNKNOWN;
                e.message = "outcome lost — PS restarted or load report missed".to_string();
                e.finished_at = now_s;
            }
        }
    }

    /// Answer a query. `op_id != 0` → one record, synthesizing `UNKNOWN` when it
    /// isn't in this leader's ledger (never a false RUNNING). `op_id == 0` → a
    /// filtered list (active-only / kind / limit), newest-first.
    pub(crate) fn query(&self, req: &OpQueryReq) -> Vec<OpRecord> {
        if req.op_id != 0 {
            return match self.entries.iter().find(|e| e.op_id == req.op_id) {
                Some(e) => vec![e.clone()],
                None => vec![OpRecord {
                    op_id: req.op_id,
                    state: OP_STATE_UNKNOWN,
                    message: "op not in this leader's ledger (leader changed?); \
                              terminal outcomes are in audit-log"
                        .to_string(),
                    ..Default::default()
                }],
            };
        }
        let mut out: Vec<OpRecord> = self
            .entries
            .iter()
            .filter(|e| !req.active_only || Self::is_active(e.state))
            .filter(|e| req.kind_filter == 0 || e.kind == req.kind_filter)
            .cloned()
            .collect();
        if req.limit != 0 && out.len() > req.limit as usize {
            out.truncate(req.limit as usize);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_rpc::manager_rpc::{
        OP_KIND_EC_CONVERT, OP_KIND_GC, OP_KIND_SPLIT, OP_STATE_FAILED, OP_STATE_SUCCEEDED,
    };

    fn q_one(led: &OpLedger, op_id: u64) -> OpRecord {
        led.query(&OpQueryReq { op_id, ..Default::default() }).remove(0)
    }

    #[test]
    fn submit_then_running_then_terminal() {
        let mut led = OpLedger::new();
        let (id, attached) = led.submit(OP_KIND_SPLIT, 7, 0, vec![], "cli".into(), 100, 100_000);
        assert!(!attached);
        assert_eq!(q_one(&led, id).state, OP_STATE_PENDING);
        led.set_running(id, 101);
        assert_eq!(q_one(&led, id).state, OP_STATE_RUNNING);
        led.finish(id, OP_STATE_SUCCEEDED, String::new(), "split part 7 dispatched".into(), 105);
        let r = q_one(&led, id);
        assert_eq!(r.state, OP_STATE_SUCCEEDED);
        assert_eq!(r.message, "split part 7 dispatched");
        assert_eq!(r.finished_at, 105);
        // finish is idempotent — a late heartbeat retransmit can't reopen it.
        led.finish(id, OP_STATE_FAILED, "boom".into(), String::new(), 200);
        assert_eq!(q_one(&led, id).state, OP_STATE_SUCCEEDED);
    }

    /// A marker can die five ways; only two of them used to close the ledger.
    /// For EC a leaked RUNNING entry is not cosmetic: `submit` attach-dedups on
    /// any ACTIVE (kind, target) and the caller returns WITHOUT actuating, so
    /// every future convert of that extent silently does nothing.
    #[test]
    fn an_abandoned_ec_op_stops_blocking_future_submits() {
        let mut led = OpLedger::new();
        let (first, attached) =
            led.submit(OP_KIND_EC_CONVERT, 0, 55, vec![55], "cli".into(), 100, 100_000);
        assert!(!attached);
        led.set_running(first, 100);

        // While it is RUNNING, a resubmit attaches and actuates nothing.
        let (again, attached) =
            led.submit(OP_KIND_EC_CONVERT, 0, 55, vec![55], "cli".into(), 101, 101_000);
        assert!(attached, "a live op should dedup");
        assert_eq!(again, first);

        // The marker is dropped for any reason — the entry must close.
        led.complete_ec(
            55,
            OP_STATE_FAILED,
            String::new(),
            "conversion abandoned: its pinned executor is no longer online".into(),
            110,
        );

        let (fresh, attached) =
            led.submit(OP_KIND_EC_CONVERT, 0, 55, vec![55], "cli".into(), 120, 120_000);
        assert!(
            !attached,
            "an abandoned conversion must not block the operator's retry forever"
        );
        assert_ne!(fresh, first);
    }

    #[test]
    fn attach_dedup_returns_same_id() {
        let mut led = OpLedger::new();
        let (id1, a1) = led.submit(OP_KIND_GC, 3, 0, vec![], "cli".into(), 1, 1000);
        let (id2, a2) = led.submit(OP_KIND_GC, 3, 0, vec![], "cli".into(), 2, 2000);
        assert!(!a1 && a2);
        assert_eq!(id1, id2);
        // a DIFFERENT target is a fresh op.
        let (id3, a3) = led.submit(OP_KIND_GC, 4, 0, vec![], "cli".into(), 3, 3000);
        assert!(!a3 && id3 != id1);
        // once terminal, a resubmit is fresh again.
        led.finish(id1, OP_STATE_SUCCEEDED, String::new(), String::new(), 4);
        let (id4, a4) = led.submit(OP_KIND_GC, 3, 0, vec![], "cli".into(), 5, 5000);
        assert!(!a4 && id4 != id1);
    }

    #[test]
    fn unknown_id_is_unknown_never_running() {
        let led = OpLedger::new();
        let r = q_one(&led, 424242);
        assert_eq!(r.state, OP_STATE_UNKNOWN);
        assert_eq!(r.op_id, 424242);
    }

    #[test]
    fn cap_evicts_oldest() {
        let mut led = OpLedger::new();
        let mut first = 0;
        for i in 0..(OP_LEDGER_CAP as u64 + 10) {
            let (id, _) = led.submit(OP_KIND_GC, i, 0, vec![], "cli".into(), i as i64, i as i64 * 10);
            if i == 0 {
                first = id;
            }
        }
        // the oldest fell out → now UNKNOWN; the ring holds exactly CAP.
        assert_eq!(q_one(&led, first).state, OP_STATE_UNKNOWN);
        assert_eq!(led.query(&OpQueryReq::default()).len(), OP_LEDGER_CAP);
    }

    #[test]
    fn ttl_flips_stale_running_maintenance_to_unknown() {
        let mut led = OpLedger::new();
        let (id, _) = led.submit(OP_KIND_GC, 1, 0, vec![], "cli".into(), 0, 0);
        led.set_running(id, 0);
        led.sweep_running_ttl(OP_RUNNING_TTL_SECS - 1); // not yet
        assert_eq!(q_one(&led, id).state, OP_STATE_RUNNING);
        led.sweep_running_ttl(OP_RUNNING_TTL_SECS + 1); // past TTL
        assert_eq!(q_one(&led, id).state, OP_STATE_UNKNOWN);
    }

    #[test]
    fn terminal_records_are_queued_for_history_exactly_once() {
        let mut led = OpLedger::new();
        let (id, _) = led.submit(OP_KIND_GC, 1, 0, vec![], "cli".into(), 0, 0);
        led.set_running(id, 0);
        assert!(
            led.drain_pending_log().is_empty(),
            "a RUNNING op has no history to write yet"
        );

        assert!(led.reconcile_outcome(id, OP_STATE_FAILED, "boom".into(), String::new(), 5));
        let queued = led.drain_pending_log();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].op_id, id);
        assert_eq!(
            queued[0].error, "boom",
            "history must carry the error text the ledger ring would later evict"
        );

        // Draining is destructive, so a second flush writes nothing again.
        assert!(led.drain_pending_log().is_empty());

        // A heartbeat retransmit of the same outcome is a no-op and must not
        // queue a duplicate history record.
        assert!(!led.reconcile_outcome(id, OP_STATE_FAILED, "boom".into(), String::new(), 6));
        assert!(led.drain_pending_log().is_empty());
    }

    #[test]
    fn extent_keyed_completions_are_queued_too() {
        let mut led = OpLedger::new();
        let (id, _) = led.submit(OP_KIND_RECOVERY, 0, 77, vec![], "auto".into(), 0, 0);
        led.set_running(id, 0);
        led.complete_recovery(77, "rebuilt".into(), 9);
        let queued = led.drain_pending_log();
        assert_eq!(queued.len(), 1, "recovery closes by extent id, not op id");
        assert_eq!(queued[0].state, OP_STATE_SUCCEEDED);
    }

    #[test]
    fn progress_tracks_a_running_op_and_ignores_late_or_unknown_samples() {
        let mut led = OpLedger::new();
        let (id, _) = led.submit(OP_KIND_GC, 1, 0, vec![], "cli".into(), 0, 0);
        led.set_running(id, 0);

        led.update_progress(id, 3 * 1024, 8 * 1024);
        let r = q_one(&led, id);
        assert_eq!((r.progress_done, r.progress_total), (3 * 1024, 8 * 1024));

        // Later sample wins — progress is a sample, not an accumulator.
        led.update_progress(id, 7 * 1024, 8 * 1024);
        assert_eq!(q_one(&led, id).progress_done, 7 * 1024);

        // The PS keeps resending its outcome ring, so a progress sample can
        // arrive AFTER the op went terminal. It must not reopen it or move the
        // counters on a closed op.
        assert!(led.reconcile_outcome(id, OP_STATE_SUCCEEDED, String::new(), String::new(), 5));
        led.update_progress(id, 8 * 1024, 8 * 1024);
        let done = q_one(&led, id);
        assert_eq!(done.state, OP_STATE_SUCCEEDED);
        assert_eq!(
            done.progress_done,
            7 * 1024,
            "a sample arriving after the outcome must not touch a terminal entry"
        );

        // Unknown / untracked ids are ignored rather than synthesizing entries.
        led.update_progress(999, 1, 2);
        led.update_progress(0, 1, 2);
        assert_eq!(q_one(&led, 999).state, OP_STATE_UNKNOWN);
    }

    /// The 30-min TTL flip to UNKNOWN must not be terminal. The PS keeps
    /// retransmitting the true outcome on every load report; refusing it would
    /// discard the real SUCCEEDED/FAILED and its reason, and the audit log —
    /// which this module promises holds terminal outcomes — would never see it.
    /// Reachable without any fault: `autumn-op compact` fanned over many
    /// partitions of one PS queues behind its concurrency permits, and the
    /// queue wait counts against the TTL because RUNNING is set at submit.
    /// EC conversion and recovery are executed by the extent node, which knows
    /// only the extent id — the manager resolves that to its own op. A sample
    /// must land on the RUNNING entry of the MATCHING kind, and must never
    /// re-animate one that already closed.
    #[test]
    fn extent_keyed_progress_lands_on_the_right_running_op() {
        let mut led = OpLedger::new();
        let (ec, _) = led.submit(OP_KIND_EC_CONVERT, 0, 55, vec![55], "cli".into(), 0, 100_000);
        led.set_running(ec, 0);
        let (rec, _) = led.submit(OP_KIND_RECOVERY, 0, 55, vec![55], "auto".into(), 0, 100_001);
        led.set_running(rec, 0);

        led.update_progress_by_extent(OP_KIND_EC_CONVERT, 55, 3, 8);
        assert_eq!(
            (q_one(&led, ec).progress_done, q_one(&led, ec).progress_total),
            (3, 8)
        );
        assert_eq!(
            (q_one(&led, rec).progress_done, q_one(&led, rec).progress_total),
            (0, 0),
            "the same extent has two ops; kind is what tells them apart"
        );

        // An extent nobody is running: dropped, not mis-filed.
        led.update_progress_by_extent(OP_KIND_EC_CONVERT, 999, 1, 2);
        assert_eq!(q_one(&led, ec).progress_done, 3);

        // A sample racing the completion must not touch a closed entry.
        led.complete_ec(55, OP_STATE_SUCCEEDED, String::new(), String::new(), 9);
        led.update_progress_by_extent(OP_KIND_EC_CONVERT, 55, 7, 8);
        let r = q_one(&led, ec);
        assert_eq!(r.state, OP_STATE_SUCCEEDED);
        assert_eq!(
            (r.progress_done, r.progress_total),
            (3, 8),
            "a terminal record keeps the last sample it had; it is not re-opened"
        );
    }

    #[test]
    fn a_late_authoritative_outcome_supersedes_unknown() {
        let mut led = OpLedger::new();
        let (id, _) = led.submit(OP_KIND_COMPACT, 7, 0, vec![], "cli".into(), 0, 100_000);
        led.set_running(id, 0);
        led.sweep_running_ttl(OP_RUNNING_TTL_SECS + 10);
        assert_eq!(q_one(&led, id).state, OP_STATE_UNKNOWN, "precondition: TTL flip");

        assert!(
            led.reconcile_outcome(
                id,
                OP_STATE_FAILED,
                "disk full".to_string(),
                String::new(),
                OP_RUNNING_TTL_SECS + 20,
            ),
            "a late PS-reported outcome must be accepted onto an UNKNOWN entry"
        );
        let r = q_one(&led, id);
        assert_eq!(r.state, OP_STATE_FAILED);
        assert_eq!(r.error, "disk full", "the failure reason must survive");

        assert!(
            !led.reconcile_outcome(
                id,
                OP_STATE_SUCCEEDED,
                String::new(),
                String::new(),
                OP_RUNNING_TTL_SECS + 30,
            ),
            "a genuine terminal state is NOT superseded by a later retransmit"
        );
        assert_eq!(q_one(&led, id).state, OP_STATE_FAILED);
    }

    #[test]
    fn reconcile_outcome_transitions_once() {
        let mut led = OpLedger::new();
        let (id, _) = led.submit(OP_KIND_GC, 1, 0, vec![], "cli".into(), 1, 1_000_000);
        led.set_running(id, 1);
        assert!(led.reconcile_outcome(id, OP_STATE_FAILED, "boom".into(), String::new(), 5));
        assert_eq!(q_one(&led, id).state, OP_STATE_FAILED);
        assert_eq!(q_one(&led, id).error, "boom");
        // heartbeat retransmit → false (already terminal), state unchanged.
        assert!(!led.reconcile_outcome(id, OP_STATE_SUCCEEDED, String::new(), String::new(), 6));
        assert_eq!(q_one(&led, id).state, OP_STATE_FAILED);
        // unknown op_id → false.
        assert!(!led.reconcile_outcome(999, OP_STATE_SUCCEEDED, String::new(), String::new(), 7));
    }

    #[test]
    fn complete_ec_closes_by_extent() {
        let mut led = OpLedger::new();
        let (id, _) = led.submit(OP_KIND_EC_CONVERT, 0, 55, vec![], "cli".into(), 0, 0);
        led.set_running(id, 0);
        led.complete_ec(55, OP_STATE_SUCCEEDED, "ec done".into(), String::new(), 10);
        assert_eq!(q_one(&led, id).state, OP_STATE_SUCCEEDED);
        // a stray complete for an unknown extent is a no-op.
        led.complete_ec(999, OP_STATE_SUCCEEDED, "x".into(), String::new(), 11);
        // abandon path: complete_ec can drive a terminal FAILED too (an EC
        // whose marker was auto-abandoned on a coordinator fence — EC is not in
        // the TTL sweep, so this is its terminal signal).
        let (id2, _) = led.submit(OP_KIND_EC_CONVERT, 0, 77, vec![], "cli".into(), 0, 0);
        led.set_running(id2, 0);
        led.complete_ec(77, OP_STATE_FAILED, String::new(), "abandoned".into(), 20);
        let r = q_one(&led, id2);
        assert_eq!(r.state, OP_STATE_FAILED);
        assert_eq!(r.error, "abandoned");
    }

    #[test]
    fn recovery_lifecycle_tracks_attempts_and_last_error() {
        let mut led = OpLedger::new();
        // dispatch accepted → RUNNING, attempts=1
        led.note_recovery_dispatch(42, 2, 7, 100, 1_000_000);
        let recs = led.query(&OpQueryReq { kind_filter: OP_KIND_RECOVERY, ..Default::default() });
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].state, OP_STATE_RUNNING);
        assert_eq!(recs[0].attempts, 1);
        assert_eq!(recs[0].secondary_id, 42);
        assert_eq!(recs[0].requested_by, "auto-recovery");
        // a failed attempt keeps it RUNNING (the loop retries) but records the
        // reason + code + consecutive count — the operator-actionable state.
        led.record_recovery_failure(42, "no healthy target".into(), 3, 4, 110, 1_100_000);
        let r = led.query(&OpQueryReq { kind_filter: OP_KIND_RECOVERY, ..Default::default() })
            .remove(0);
        assert_eq!(r.state, OP_STATE_RUNNING, "recovery retries; never terminal on one failure");
        assert_eq!(r.error, "no healthy target");
        assert_eq!(r.error_code, 3);
        assert_eq!(r.attempts, 4);
        // a re-dispatch counts another attempt on the SAME entry (one op per
        // extent, not one per try).
        led.note_recovery_dispatch(42, 2, 9, 120, 1_200_000);
        let r = led.query(&OpQueryReq { kind_filter: OP_KIND_RECOVERY, ..Default::default() })
            .remove(0);
        assert_eq!(r.attempts, 5);
        assert_eq!(
            led.query(&OpQueryReq { kind_filter: OP_KIND_RECOVERY, ..Default::default() }).len(),
            1
        );
        // apply_recovery_done → SUCCEEDED, and the stale error code is cleared.
        led.complete_recovery(42, "recovered slot onto node 9".into(), 130);
        let r = led.query(&OpQueryReq { kind_filter: OP_KIND_RECOVERY, ..Default::default() })
            .remove(0);
        assert_eq!(r.state, OP_STATE_SUCCEEDED);
        assert_eq!(r.error_code, 0);
    }

    #[test]
    fn recovery_failure_before_any_dispatch_still_listed() {
        let mut led = OpLedger::new();
        // a repair that can't even start (no candidate) must still be visible.
        led.record_recovery_failure(9, "all recovery candidates rejected".into(), 3, 1, 5, 5_000);
        let r = led.query(&OpQueryReq { kind_filter: OP_KIND_RECOVERY, ..Default::default() })
            .remove(0);
        assert_eq!(r.state, OP_STATE_RUNNING);
        assert_eq!(r.secondary_id, 9);
        assert_eq!(r.error, "all recovery candidates rejected");
    }

    #[test]
    fn seed_ec_replay_makes_inflight_ec_listable_and_closable() {
        let mut led = OpLedger::new();
        led.seed_replay(OP_KIND_EC_CONVERT, [55u64, 77], 100, 1_000_000);
        let ecs = led.query(&OpQueryReq { kind_filter: OP_KIND_EC_CONVERT, ..Default::default() });
        assert_eq!(ecs.len(), 2);
        assert!(ecs
            .iter()
            .all(|e| e.state == OP_STATE_RUNNING && e.requested_by == "replay"));
        // idempotent: re-seeding an already-tracked extent doesn't duplicate.
        led.seed_replay(OP_KIND_EC_CONVERT, [55u64], 101, 1_100_000);
        assert_eq!(
            led.query(&OpQueryReq { kind_filter: OP_KIND_EC_CONVERT, ..Default::default() }).len(),
            2
        );
        // a replayed entry closes normally when the conversion applies.
        led.complete_ec(55, OP_STATE_SUCCEEDED, "done".into(), String::new(), 200);
        let active = led.query(&OpQueryReq {
            active_only: true,
            kind_filter: OP_KIND_EC_CONVERT,
            ..Default::default()
        });
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].secondary_id, 77);
    }

    #[test]
    fn query_filters_active_kind_limit() {
        let mut led = OpLedger::new();
        let (g, _) = led.submit(OP_KIND_GC, 1, 0, vec![], "cli".into(), 0, 0);
        let (s, _) = led.submit(OP_KIND_SPLIT, 2, 0, vec![], "cli".into(), 0, 1);
        led.finish(g, OP_STATE_SUCCEEDED, String::new(), String::new(), 1);
        // active-only drops the finished gc.
        let active = led.query(&OpQueryReq { active_only: true, ..Default::default() });
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].op_id, s);
        // kind filter.
        let gcs = led.query(&OpQueryReq { kind_filter: OP_KIND_GC, ..Default::default() });
        assert_eq!(gcs.len(), 1);
        assert_eq!(gcs[0].op_id, g);
        // limit.
        assert_eq!(led.query(&OpQueryReq { limit: 1, ..Default::default() }).len(), 1);
    }
}
