//! M0 — crash-safe, multi-writer fuse-fs inode-number allocation.
//!
//! Pre-M0 every allocator (each fuse mount) did a non-CAS read-modify-write
//! on the fs KV superblock key `[0x04]next_inode`: two concurrent allocators
//! could read the same value and claim the same 1000-inode batch → duplicate
//! inodes → namespace corruption. With the Python `autumn.Fs` client joining
//! as a co-equal writer (design: `docs/fs_unify_design.md` §6, user decision
//! Q2 = option B), allocation moves into the manager — the grantor of every
//! other monotonic token (owner_epoch, lease_epoch) — as a leader-fenced
//! **etcd CAS** on `autumn-rs/fs/next_inode`.
//!
//! Concurrency model: the manager grants one at a time
//! (`AutumnManager::fs_alloc_turn`), and each grant is a CAS on the counter
//! inside the leader fence. Only the leader writes the counter, so the queue
//! is what keeps concurrent grants from conflicting: with the CAS alone, the
//! leader's own concurrent requests raced each other, and a burst of 64
//! allocators (eight 8-worker S3 gateways writing for the first time) ran
//! requests out of their CAS attempts. The CAS stays for what the queue
//! cannot see — a deposed leader still writing — and the fence inside
//! `txn_fenced` makes that grant lose the txn instead of double-granting.
//!
//! Migration: requests carry a `floor` — the legacy KV counter value read by
//! the fuse mount. The grant never returns a base below the floor, so a
//! pre-M0 filesystem's existing inodes are never re-issued. The counter only
//! ever grows (`max(cur, floor)`), so a stale floor can't rewind it.

use std::cell::Cell;

use autumn_common::AppError;

use crate::{AutumnManager, EtcdMirror};

/// Etcd key holding the next unallocated fuse-fs inode number, big-endian
/// u64. Same `autumn-rs/` namespace as `cluster_id`.
pub(crate) const FS_NEXT_INODE_KEY: &str = "autumn-rs/fs/next_inode";

/// First allocatable inode number: fuse's `ROOT_INO` (1) is preassigned to
/// the filesystem root and never allocated. Kept in sync with
/// `autumn_fs::schema::ROOT_INO` by value (an fs dep here would invert the
/// crate DAG).
pub(crate) const FS_FIRST_ALLOCATABLE_INO: u64 = 2;

/// CAS retry budget. Grants are queued in the manager, so a conflict means
/// another writer of the counter (a deposed leader); exceeding this
/// indicates something systemically wrong — fail loudly.
const MAX_CAS_ATTEMPTS: u32 = 16;

impl AutumnManager {
    /// Grant `[base, base + count)` fuse-fs inode numbers. `floor` raises the
    /// counter before granting (legacy-KV migration; 0 = none).
    pub(crate) async fn alloc_fs_inodes(
        &self,
        count: u64,
        floor: u64,
    ) -> Result<u64, AppError> {
        debug_assert!(count > 0, "handler validates count >= 1");
        let floor = floor.max(FS_FIRST_ALLOCATABLE_INO);
        match &self.etcd {
            None => Ok(alloc_from_cell(&self.fs_next_inode, count, floor)),
            Some(etcd) => {
                // Two etcd round trips (get, txn) per grant of ~1000 inodes.
                let _turn = self.fs_alloc_turn.lock().await;
                etcd.alloc_fs_inodes_cas(FS_NEXT_INODE_KEY.as_bytes(), count, floor)
                    .await
            }
        }
    }
}

/// Memory-only allocation (tests/dev — no persistence, no leader election;
/// single-threaded compio, and no await between read and write, so the
/// read-modify-write cannot interleave). 0 = nothing granted yet.
fn alloc_from_cell(next: &Cell<u64>, count: u64, floor: u64) -> u64 {
    let base = next.get().max(FS_FIRST_ALLOCATABLE_INO).max(floor);
    next.set(base + count);
    base
}

impl EtcdMirror {
    /// Leader-fenced CAS grant loop. Reads the counter, computes
    /// `base = max(cur, floor)`, and commits `base + count` back with a
    /// txn that requires BOTH the leader fence AND the counter still
    /// holding the value we read (value-CAS; the counter is strictly
    /// monotonic so ABA is impossible). Conflict → re-read and retry.
    pub(crate) async fn alloc_fs_inodes_cas(
        &self,
        key: &[u8],
        count: u64,
        floor: u64,
    ) -> Result<u64, AppError> {
        for attempt in 0..MAX_CAS_ATTEMPTS {
            if attempt > 0 {
                // Linear backoff on CAS conflict, which with grants queued
                // means another writer of the counter (a deposed leader).
                // Bounded await (3 ms × attempt ≤ 45 ms).
                compio::time::sleep(std::time::Duration::from_millis(3 * attempt as u64)).await;
            }
            let got = self
                .client
                .get(key)
                .await
                .map_err(|e| AppError::Internal(format!("alloc_fs_inodes get: {e}")))?;

            let (base, cas_cmp) = match got.kvs.first() {
                None => {
                    // First-ever grant: create the key iff it still doesn't
                    // exist (same create_revision==0 pattern as owner locks).
                    (floor, autumn_etcd::Cmp::create_revision(key, 0))
                }
                Some(kv) => {
                    let cur = decode_counter(key, &kv.value)?;
                    (
                        cur.max(floor),
                        autumn_etcd::Cmp::value(key, kv.value.clone()),
                    )
                }
            };

            let next = base.checked_add(count).ok_or_else(|| {
                AppError::Internal("alloc_fs_inodes: inode counter overflow".to_string())
            })?;
            let committed = self
                .txn_fenced(
                    vec![cas_cmp],
                    vec![autumn_etcd::Op::put(key, next.to_be_bytes())],
                    vec![],
                )
                .await?; // NotLeader bubbles → handler returns CODE_NOT_LEADER
            if committed {
                return Ok(base);
            }
            // CAS conflict: a concurrent grant landed between our read and
            // our txn. Loop re-reads the fresh counter.
        }
        Err(AppError::Internal(format!(
            "alloc_fs_inodes({}): {MAX_CAS_ATTEMPTS} CAS attempts exhausted — etcd churn?",
            String::from_utf8_lossy(key)
        )))
    }
}

/// Strict 8-byte big-endian decode. A malformed counter is corruption —
/// refuse loudly rather than guessing (a lenient default could re-issue
/// live inode numbers).
fn decode_counter(key: &[u8], v: &[u8]) -> Result<u64, AppError> {
    let bytes: [u8; 8] = v.try_into().map_err(|_| {
        AppError::Internal(format!(
            "{} holds {} bytes, want 8 (BE u64) — refusing to allocate",
            String::from_utf8_lossy(key),
            v.len()
        ))
    })?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_alloc_disjoint_and_floor() {
        let next = Cell::new(0);
        let a = alloc_from_cell(&next, 1000, 0);
        let b = alloc_from_cell(&next, 1000, 0);
        assert_eq!(a, FS_FIRST_ALLOCATABLE_INO);
        assert_eq!(b, a + 1000); // disjoint, contiguous

        // floor raises the counter (legacy migration)...
        let c = alloc_from_cell(&next, 10, 50_000);
        assert_eq!(c, 50_000);
        // ...but a stale floor can never rewind it
        let d = alloc_from_cell(&next, 10, 3);
        assert_eq!(d, 50_010);
    }

    #[test]
    fn decode_counter_strict() {
        let k = FS_NEXT_INODE_KEY.as_bytes();
        assert_eq!(decode_counter(k, &42u64.to_be_bytes()).unwrap(), 42);
        assert!(decode_counter(k, b"short").is_err());
        assert!(decode_counter(k, b"").is_err());
        assert!(decode_counter(k, &[0u8; 9]).is_err());
    }
}
