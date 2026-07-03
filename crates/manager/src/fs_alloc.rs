//! F-FS-UNIFY M0 — crash-safe, multi-writer fuse-fs inode-number allocation.
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
//! Concurrency model: the CAS loop re-reads the counter and retries on
//! conflict, so any number of concurrent `AllocInodes` grants receive
//! disjoint `[base, base+count)` ranges; the F149 leader fence inside
//! `txn_fenced` means a deposed leader's grant loses the txn instead of
//! double-granting across a leader transition.
//!
//! Migration: requests carry a `floor` — the legacy KV counter value read by
//! the fuse mount. The grant never returns a base below the floor, so a
//! pre-M0 filesystem's existing inodes are never re-issued. The counter only
//! ever grows (`max(cur, floor)`), so a stale floor can't rewind it.

use std::cell::Cell;

use autumn_common::AppError;

use crate::{AutumnManager, EtcdMirror};

/// Etcd key holding the next unallocated fuse-fs inode number (big-endian
/// u64, matching the legacy fs KV counter encoding). Same `autumn-rs/`
/// namespace as `cluster_id` / `cluster_version`.
pub(crate) const FS_NEXT_INODE_KEY: &str = "autumn-rs/fs/next_inode";

/// First allocatable inode number: fuse's `ROOT_INO` (1) is preassigned to
/// the filesystem root and never allocated. Kept in sync with
/// `autumn_fuse::schema::ROOT_INO` by value (a fuse dep here would invert
/// the crate DAG).
pub(crate) const FS_FIRST_ALLOCATABLE_INO: u64 = 2;

/// CAS retry budget. Contention is per-batch (one grant per ~1000 inodes per
/// allocator), so even pathological mount storms resolve in a few rounds;
/// exceeding this indicates something systemically wrong — fail loudly.
const MAX_CAS_ATTEMPTS: u32 = 16;

impl AutumnManager {
    /// Grant `[base, base + count)` fuse-fs inode numbers. `floor` raises
    /// the counter before granting (legacy-KV migration; 0 = none).
    pub(crate) async fn alloc_fs_inodes(&self, count: u64, floor: u64) -> Result<u64, AppError> {
        debug_assert!(count > 0, "handler validates count >= 1");
        let floor = floor.max(FS_FIRST_ALLOCATABLE_INO);
        match &self.etcd {
            None => Ok(alloc_from_cell(&self.fs_next_inode, count, floor)),
            Some(etcd) => etcd.alloc_fs_inodes_cas(count, floor).await,
        }
    }
}

/// Memory-only allocation (tests/dev — no persistence, no leader election;
/// single-threaded compio, and no await between read and write, so the
/// read-modify-write on the Cell cannot interleave).
fn alloc_from_cell(cell: &Cell<u64>, count: u64, floor: u64) -> u64 {
    let base = cell.get().max(floor);
    cell.set(base + count);
    base
}

impl EtcdMirror {
    /// Leader-fenced CAS grant loop. Reads the counter, computes
    /// `base = max(cur, floor)`, and commits `base + count` back with a
    /// txn that requires BOTH the F149 leader fence AND the counter still
    /// holding the value we read (value-CAS; the counter is strictly
    /// monotonic so ABA is impossible). Conflict → re-read and retry.
    pub(crate) async fn alloc_fs_inodes_cas(
        &self,
        count: u64,
        floor: u64,
    ) -> Result<u64, AppError> {
        let key = FS_NEXT_INODE_KEY.as_bytes();
        for attempt in 0..MAX_CAS_ATTEMPTS {
            if attempt > 0 {
                // Linear backoff on CAS conflict (coco P3): each round some
                // writer commits, so N conflicters finish in ≤ N rounds; the
                // spread just de-synchronizes their re-reads under a mount
                // storm. Bounded await (F228 1A: 3 ms × attempt ≤ 45 ms).
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
                    (
                        floor,
                        autumn_etcd::Cmp::create_revision(key, 0),
                    )
                }
                Some(kv) => {
                    let cur = decode_counter(&kv.value)?;
                    (cur.max(floor), autumn_etcd::Cmp::value(key, kv.value.clone()))
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
            "alloc_fs_inodes: {MAX_CAS_ATTEMPTS} CAS attempts exhausted — etcd churn?"
        )))
    }
}

/// Strict 8-byte big-endian decode. A malformed counter is corruption —
/// refuse loudly rather than guessing (a lenient default could re-issue
/// live inode numbers).
fn decode_counter(v: &[u8]) -> Result<u64, AppError> {
    let bytes: [u8; 8] = v.try_into().map_err(|_| {
        AppError::Internal(format!(
            "{FS_NEXT_INODE_KEY} holds {} bytes, want 8 (BE u64) — refusing to allocate",
            v.len()
        ))
    })?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_alloc_disjoint_and_floor() {
        let cell = Cell::new(FS_FIRST_ALLOCATABLE_INO);
        let a = alloc_from_cell(&cell, 1000, 0);
        let b = alloc_from_cell(&cell, 1000, 0);
        assert_eq!(a, FS_FIRST_ALLOCATABLE_INO);
        assert_eq!(b, a + 1000); // disjoint, contiguous

        // floor raises the counter (legacy migration)...
        let c = alloc_from_cell(&cell, 10, 50_000);
        assert_eq!(c, 50_000);
        // ...but a stale floor can never rewind it
        let d = alloc_from_cell(&cell, 10, 3);
        assert_eq!(d, 50_010);
    }

    #[test]
    fn decode_counter_strict() {
        assert_eq!(decode_counter(&42u64.to_be_bytes()).unwrap(), 42);
        assert!(decode_counter(b"short").is_err());
        assert!(decode_counter(b"").is_err());
        assert!(decode_counter(&[0u8; 9]).is_err());
    }
}
