//! F211-I: operator-action audit log.
//!
//! Append-only etcd records keyed `mgr_audit_log/<ts_ns>_<op_id>`. Each
//! entry captures `(op, node_id?, extent_id?, by, reason, result_code,
//! result_message, ts_ns)`. Every admin RPC handler wraps a call to
//! `append_audit` AFTER its primary etcd mutation (or in the response
//! path) so we have a complete operator history even when leader
//! changes hands.
//!
//! The append goes through `txn_fenced` (F149) so a deposed leader's
//! audit write fails along with everything else. GC runs every hour
//! and deletes records older than `AUTUMN_MGR_AUDIT_RETENTION_DAYS`
//! (default 90).

use autumn_rpc::manager_rpc::{rkyv_decode, rkyv_encode, MgrAuditEntry};

use crate::AutumnManager;

pub const AUDIT_PREFIX: &str = "mgr_audit_log/";

/// Compose the key for a given timestamp + sequence. Sequence is the
/// per-process counter held by the AutumnManager; collisions inside a
/// single ns are extremely unlikely but the suffix keeps order stable.
pub fn audit_key(ts_ns: u64, seq: u64) -> String {
    // 24-char zero-padded ts_ns + 12-char seq → lexically sortable.
    format!("{}{:024}_{:012}", AUDIT_PREFIX, ts_ns, seq)
}

impl AutumnManager {
    /// Append an audit entry. Best-effort — failure of the audit
    /// persistence MUST NOT cause the primary operation to surface
    /// an error to the caller (the operation already succeeded). We
    /// log at WARN and move on.
    pub(crate) async fn append_audit(&self, mut entry: MgrAuditEntry) {
        if entry.ts_ns == 0 {
            entry.ts_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
        }
        let seq = self.audit_seq.get().wrapping_add(1).max(1);
        self.audit_seq.set(seq);
        let key = audit_key(entry.ts_ns, seq);
        let value = rkyv_encode(&entry).to_vec();
        // In-memory mode (no etcd): nothing to persist, but we still
        // increment seq so unit-test ordering matches production.
        if let Some(etcd) = &self.etcd {
            if let Err(e) = etcd.put_msgs_txn(vec![(key, value)]).await {
                tracing::warn!(
                    op = entry.op,
                    node_id = entry.node_id,
                    extent_id = entry.extent_id,
                    error = %e,
                    "F211-I: failed to persist audit entry (continuing anyway)"
                );
            }
        }
    }

    /// Query handler — filters by op / node_id / time window. Reads
    /// the entire prefix and filters in-process. Production audit
    /// volume is low (operator actions are infrequent vs. data plane),
    /// so the simple scan is fine; if the volume ever justifies it,
    /// upgrade to etcd range queries with proper key encoding.
    pub(crate) async fn query_audit(
        &self,
        op_filter: u8,
        node_filter: u64,
        since_ts_s: i64,
        until_ts_s: i64,
        limit: u32,
    ) -> Vec<MgrAuditEntry> {
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Vec::new(),
        };
        let raw = {
            let c = etcd.client.borrow().clone();
            match c.get_prefix(AUDIT_PREFIX).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "F211-I: audit prefix read failed");
                    return Vec::new();
                }
            }
        };
        let mut out: Vec<MgrAuditEntry> = Vec::with_capacity(raw.kvs.len());
        for kv in &raw.kvs {
            let entry: MgrAuditEntry = match rkyv_decode(&kv.value) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "F211-I: skipping malformed audit entry");
                    continue;
                }
            };
            let entry_ts_s = (entry.ts_ns / 1_000_000_000) as i64;
            if op_filter != 0 && entry.op != op_filter {
                continue;
            }
            if node_filter != 0 && entry.node_id != node_filter {
                continue;
            }
            if since_ts_s != 0 && entry_ts_s < since_ts_s {
                continue;
            }
            if until_ts_s != 0 && entry_ts_s > until_ts_s {
                continue;
            }
            out.push(entry);
        }
        out.sort_by_key(|e| e.ts_ns);
        if limit > 0 && (limit as usize) < out.len() {
            // Return the most recent `limit` entries.
            let start = out.len() - limit as usize;
            out = out.split_off(start);
        }
        out
    }

    /// Retention GC. Reads the prefix, finds entries older than the
    /// configured retention window, and deletes them in batches.
    /// Best-effort — partial progress is fine, next tick continues.
    #[allow(dead_code)]
    pub(crate) async fn audit_retention_gc(&self) {
        let days = std::env::var("AUTUMN_MGR_AUDIT_RETENTION_DAYS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(90);
        let cutoff_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            .saturating_sub(days.saturating_mul(86_400).saturating_mul(1_000_000_000));
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return,
        };
        let raw = {
            let c = etcd.client.borrow().clone();
            match c.get_prefix(AUDIT_PREFIX).await {
                Ok(v) => v,
                Err(_) => return,
            }
        };
        let mut to_delete: Vec<String> = Vec::new();
        for kv in &raw.kvs {
            let key = match std::str::from_utf8(&kv.key) {
                Ok(s) => s.to_string(),
                Err(_) => continue,
            };
            let ts_part = key
                .strip_prefix(AUDIT_PREFIX)
                .and_then(|s| s.split('_').next())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            if ts_part > 0 && ts_part < cutoff_ns {
                to_delete.push(key);
            }
        }
        if to_delete.is_empty() {
            return;
        }
        for chunk in to_delete.chunks(64) {
            let _ = etcd.put_and_delete_txn(Vec::new(), chunk.to_vec()).await;
        }
    }
}
