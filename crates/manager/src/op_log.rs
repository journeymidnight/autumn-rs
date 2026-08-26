//! Durable terminal history for maintenance ops.
//!
//! The `OpLedger` answers "what is running right now". It is a 256-entry
//! in-memory ring that dies with the leader, which is the right shape for live
//! state and the wrong one for history: the 257th op evicts the oldest, a
//! failover drops everything, and recovery — which retries forever and never
//! goes terminal — overwrites its own `error` on every attempt. So the reason a
//! maintenance op failed had nowhere durable to live.
//!
//! The audit log does persist, but it answers a different question. Audit is
//! "who asked for what", written for every admin RPC and kept for 90 days; its
//! entry carries only `result_code: 0/1`, so the error text is discarded at the
//! call site. Widening it would mix two retention policies and two consumers in
//! one prefix. This is a separate prefix with its own rotation.
//!
//! The value is the `OpRecord` itself, so a record read back out of history
//! decodes into exactly the shape `ops status` already renders — no second
//! schema to keep in step.
//!
//! Rotation is by COUNT, not age. Op volume is driven by cluster activity
//! rather than by the clock, so a time window bounds the history badly in both
//! directions: a quiet week keeps almost nothing, and a compaction storm can
//! write more in an hour than an operator will ever page through. A count cap
//! makes the worst case predictable.

use autumn_rpc::manager_rpc::{rkyv_encode, OpRecord};

use crate::AutumnManager;

pub(crate) const OP_LOG_PREFIX: &str = "opLog/";

/// How many terminal records to keep. At ~1 KB each this is a couple of MB in
/// etcd — small next to the extent metadata already there, and deep enough to
/// cover an incident that happened while nobody was watching.
pub(crate) const OP_LOG_CAP: usize = 2000;

/// Writes between rotation sweeps. Rotation costs a prefix read, so it is
/// amortised rather than run per write; the cap is therefore a soft ceiling
/// that can be exceeded by up to this much between sweeps.
pub(crate) const OP_LOG_GC_EVERY: u32 = 200;

/// Lexically sortable key: zero-padded nanos, then a per-process sequence so
/// two records inside the same nanosecond keep a stable order.
pub(crate) fn op_log_key(ts_ns: u64, seq: u64) -> String {
    format!("{}{:024}_{:012}", OP_LOG_PREFIX, ts_ns, seq)
}

impl AutumnManager {
    /// Append a terminal op record to durable history.
    ///
    /// Best-effort, exactly like `append_audit`: the operation itself already
    /// finished, and failing it because its history could not be written would
    /// turn an observability gap into an outage.
    /// Write a batch of terminal records to durable history in ONE etcd txn.
    ///
    /// Batched rather than one round-trip per record because the caller is the
    /// PS load heartbeat: a burst of completions would otherwise put N serial
    /// etcd round-trips on the path that keeps the fleet's liveness accounting
    /// current, which is exactly where added latency hurts most.
    ///
    /// Best-effort, like `append_audit`: the ops themselves already finished,
    /// and failing them because their history could not be written would turn
    /// an observability gap into an outage.
    pub(crate) async fn append_op_log_batch(&self, recs: &[OpRecord]) {
        if recs.is_empty() {
            return;
        }
        let Some(etcd) = &self.etcd else {
            return; // memory mode: no history to keep
        };
        let ts_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let mut puts = Vec::with_capacity(recs.len());
        for rec in recs {
            let seq = self.op_log_seq.get().wrapping_add(1).max(1);
            self.op_log_seq.set(seq);
            puts.push((op_log_key(ts_ns, seq), rkyv_encode(rec).to_vec()));
        }
        if let Err(e) = etcd.put_msgs_txn(puts).await {
            tracing::warn!(
                records = recs.len(),
                error = %e,
                "failed to persist op-log records (continuing anyway)"
            );
            return;
        }

        let n = self.op_log_writes_since_gc.get() + recs.len() as u32;
        if n < OP_LOG_GC_EVERY {
            self.op_log_writes_since_gc.set(n);
            return;
        }
        self.op_log_writes_since_gc.set(0);
        self.rotate_op_log().await;
    }

    /// Move every queued terminal record into durable history.
    ///
    /// Called from async contexts that already run often — the PS load
    /// heartbeat for timeliness, the policy tick as a backstop for kinds no PS
    /// reports (recovery, ec-convert). The queue is drained under a short
    /// borrow so the etcd writes happen outside it.
    pub(crate) async fn flush_op_log(&self) {
        let pending = self.ops.borrow_mut().drain_pending_log();
        self.append_op_log_batch(&pending).await;
    }

    /// Read terminal history, NEWEST FIRST.
    ///
    /// Keys are fixed-width zero-padded, so the prefix scan is already in
    /// timestamp order and "the most recent N" is a tail slice — no sorting by
    /// a decoded field, and no dependence on etcd's return order.
    ///
    /// A record that fails to decode is skipped with a warning rather than
    /// failing the query: history is diagnostic, and one unreadable row must
    /// not deny an operator the rest of it.
    pub(crate) async fn read_op_log(
        &self,
        kind_filter: u8,
        since_unix: i64,
        limit: u32,
    ) -> Vec<OpRecord> {
        const DEFAULT_LIMIT: u32 = 50;
        let limit = if limit == 0 { DEFAULT_LIMIT } else { limit } as usize;
        let Some(etcd) = &self.etcd else {
            return Vec::new();
        };
        let c = etcd.client.clone();
        let listed = match c.get_prefix(OP_LOG_PREFIX).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "op-log read could not list the prefix");
                return Vec::new();
            }
        };
        let mut rows: Vec<(Vec<u8>, OpRecord)> = Vec::with_capacity(listed.kvs.len());
        for kv in &listed.kvs {
            match autumn_rpc::manager_rpc::rkyv_decode::<OpRecord>(&kv.value) {
                Ok(rec) => rows.push((kv.key.clone(), rec)),
                Err(e) => tracing::warn!(error = %e, "skipping an undecodable op-log record"),
            }
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows.into_iter()
            .rev() // newest first
            .map(|(_, r)| r)
            .filter(|r| kind_filter == 0 || r.kind == kind_filter)
            .filter(|r| since_unix == 0 || r.finished_at >= since_unix)
            .take(limit)
            .collect()
    }

    /// Trim the history back to `OP_LOG_CAP`, oldest first. Leader-only by
    /// construction — only a leader appends — and best-effort: a failed sweep
    /// just means the next one has more to remove.
    pub(crate) async fn rotate_op_log(&self) {
        let Some(etcd) = &self.etcd else {
            return;
        };
        let c = etcd.client.clone();
        let listed = match c.get_prefix(OP_LOG_PREFIX).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "op-log rotation could not list the prefix");
                return;
            }
        };
        if listed.kvs.len() <= OP_LOG_CAP {
            return;
        }
        // Keys are lexically ordered by timestamp, so the oldest sort first.
        let mut keys: Vec<String> = listed
            .kvs
            .iter()
            .filter_map(|kv| String::from_utf8(kv.key.clone()).ok())
            .collect();
        keys.sort();
        let drop_n = keys.len().saturating_sub(OP_LOG_CAP);
        let doomed: Vec<String> = keys.into_iter().take(drop_n).collect();
        if let Err(e) = etcd.put_and_delete_txn(Vec::new(), doomed).await {
            tracing::warn!(error = %e, dropped = drop_n, "op-log rotation could not delete");
            return;
        }
        tracing::debug!(dropped = drop_n, cap = OP_LOG_CAP, "op-log rotated");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_sort_oldest_first() {
        let a = op_log_key(1_000, 1);
        let b = op_log_key(2_000, 1);
        let c = op_log_key(2_000, 2);
        let mut v = vec![c.clone(), a.clone(), b.clone()];
        v.sort();
        assert_eq!(v, vec![a, b, c], "ts then seq, lexically");
    }

    #[test]
    fn key_is_prefixed_and_fixed_width() {
        let k = op_log_key(42, 7);
        assert!(k.starts_with(OP_LOG_PREFIX));
        // Fixed width is what makes the lexical sort match numeric order.
        assert_eq!(k.len(), OP_LOG_PREFIX.len() + 24 + 1 + 12);
    }
}
