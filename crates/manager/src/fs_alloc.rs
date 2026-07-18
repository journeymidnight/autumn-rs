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

use std::cell::RefCell;
use std::collections::HashMap;

use autumn_common::AppError;

use crate::{AutumnManager, EtcdMirror};

/// Etcd key holding the next unallocated fuse-fs inode number for the LEGACY
/// single global counter (empty `volume`), big-endian u64. Same `autumn-rs/`
/// namespace as `cluster_id` / `cluster_version`. F-KEY-NS SD-3 makes the
/// counter PER-VOLUME (see `fs_next_inode_key`); this stays the empty-volume
/// key so the pre-SD-3 wire (`volume = ""`) and existing tests are unchanged.
pub(crate) const FS_NEXT_INODE_KEY: &str = "autumn-rs/fs/next_inode";

/// F-KEY-NS SD-3: the etcd counter key for a fuse `volume` identity. `volume`
/// is the canonicalized `fs/{tenant}/{volume}/` prefix (ends in `/`) the fuse
/// mount sends in `AllocInodesReq.volume`, so the per-volume counter lives at
/// `autumn-rs/fs/{tenant}/{volume}/next_inode` — each volume numbers its inodes
/// independently from 2, and two volumes can never be handed the same inode.
/// Empty `volume` → the legacy global key (`FS_NEXT_INODE_KEY`).
pub(crate) fn fs_next_inode_key(volume: &[u8]) -> Vec<u8> {
    if volume.is_empty() {
        return FS_NEXT_INODE_KEY.as_bytes().to_vec();
    }
    let mut k = Vec::with_capacity(b"autumn-rs/".len() + volume.len() + b"next_inode".len());
    k.extend_from_slice(b"autumn-rs/");
    k.extend_from_slice(volume);
    k.extend_from_slice(b"next_inode");
    k
}

/// F-KEY-NS SD-3 (review P2-4): validate `AllocInodesReq.volume` before it is
/// concatenated into an etcd key (`fs_next_inode_key`). Empty = the global
/// counter (the only shape the fuse layer sends today — see the client
/// `alloc_inodes` note). A non-empty value must be the canonical
/// `ns/tenant/volume/` prefix: exactly 3 non-empty `[a-z0-9._-]+` segments plus a
/// trailing `/`. Without this, a client could forge the global key
/// (`volume="fs/"` → `autumn-rs/fs/next_inode`), churn another tenant's counter,
/// or create a non-canonical duplicate counter (missing trailing `/`) that would
/// hand out overlapping inode ranges.
pub(crate) fn valid_alloc_volume(v: &[u8]) -> bool {
    if v.is_empty() {
        return true;
    }
    let Ok(s) = std::str::from_utf8(v) else {
        return false;
    };
    // "ns/tenant/vol/" splits to ["ns", "tenant", "vol", ""].
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 4 || !parts[3].is_empty() {
        return false;
    }
    parts[..3].iter().all(|seg| {
        !seg.is_empty()
            && seg.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
            })
    })
}

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
    /// Grant `[base, base + count)` fuse-fs inode numbers for `volume` (the
    /// canonicalized `fs/{tenant}/{volume}/` prefix; empty = legacy global
    /// counter). `floor` raises the counter before granting (legacy-KV
    /// migration; 0 = none).
    pub(crate) async fn alloc_fs_inodes(
        &self,
        count: u64,
        floor: u64,
        volume: &[u8],
    ) -> Result<u64, AppError> {
        debug_assert!(count > 0, "handler validates count >= 1");
        let floor = floor.max(FS_FIRST_ALLOCATABLE_INO);
        match &self.etcd {
            None => Ok(alloc_from_map(&self.fs_next_inode, volume, count, floor)),
            Some(etcd) => {
                etcd.alloc_fs_inodes_cas(&fs_next_inode_key(volume), count, floor)
                    .await
            }
        }
    }
}

/// Memory-only allocation (tests/dev — no persistence, no leader election;
/// single-threaded compio, and no await between read and write, so the
/// read-modify-write on the map entry cannot interleave). Keyed per `volume`
/// so two volumes get disjoint counters (mirrors the per-volume etcd key).
fn alloc_from_map(
    map: &RefCell<HashMap<Vec<u8>, u64>>,
    volume: &[u8],
    count: u64,
    floor: u64,
) -> u64 {
    let mut m = map.borrow_mut();
    let cur = m
        .get(volume)
        .copied()
        .unwrap_or(FS_FIRST_ALLOCATABLE_INO);
    let base = cur.max(floor);
    m.insert(volume.to_vec(), base + count);
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
        key: &[u8],
        count: u64,
        floor: u64,
    ) -> Result<u64, AppError> {
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
                    let cur = decode_counter(key, &kv.value)?;
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
        let map = RefCell::new(HashMap::new());
        let vol = b"".as_slice(); // global counter
        let a = alloc_from_map(&map, vol, 1000, 0);
        let b = alloc_from_map(&map, vol, 1000, 0);
        assert_eq!(a, FS_FIRST_ALLOCATABLE_INO);
        assert_eq!(b, a + 1000); // disjoint, contiguous

        // floor raises the counter (legacy migration)...
        let c = alloc_from_map(&map, vol, 10, 50_000);
        assert_eq!(c, 50_000);
        // ...but a stale floor can never rewind it
        let d = alloc_from_map(&map, vol, 10, 3);
        assert_eq!(d, 50_010);
    }

    #[test]
    fn map_alloc_per_volume_isolation() {
        // F-KEY-NS SD-3: two volumes number their inodes independently.
        let map = RefCell::new(HashMap::new());
        let v1 = b"fs/t/v1/".as_slice();
        let v2 = b"fs/t/v2/".as_slice();
        let a1 = alloc_from_map(&map, v1, 1000, 0);
        let a2 = alloc_from_map(&map, v2, 1000, 0);
        // both start fresh from FS_FIRST_ALLOCATABLE_INO — dense, not shared
        assert_eq!(a1, FS_FIRST_ALLOCATABLE_INO);
        assert_eq!(a2, FS_FIRST_ALLOCATABLE_INO);
        // and advance independently
        let b1 = alloc_from_map(&map, v1, 1000, 0);
        assert_eq!(b1, a1 + 1000);
        let b2 = alloc_from_map(&map, v2, 5, 0);
        assert_eq!(b2, a2 + 1000);
    }

    #[test]
    fn valid_alloc_volume_shape() {
        assert!(valid_alloc_volume(b"")); // global counter
        assert!(valid_alloc_volume(b"fs/acme/vol0/"));
        assert!(valid_alloc_volume(b"fs/default/default/"));
        // wrong segment count / no trailing slash
        assert!(!valid_alloc_volume(b"fs/")); // would forge the global key
        assert!(!valid_alloc_volume(b"fs/acme/")); // 2 segments
        assert!(!valid_alloc_volume(b"fs/acme/vol0")); // missing trailing /
        assert!(!valid_alloc_volume(b"fs/acme/vol0/extra/")); // 4 segments
        // bad charset
        assert!(!valid_alloc_volume(b"fs/Acme/vol0/")); // uppercase
        assert!(!valid_alloc_volume(b"fs//vol0/")); // empty tenant
        assert!(!valid_alloc_volume(b"fs/ac me/vol0/")); // space
    }

    #[test]
    fn fs_next_inode_key_shape() {
        // empty volume → legacy global key (byte-identical)
        assert_eq!(fs_next_inode_key(b""), FS_NEXT_INODE_KEY.as_bytes());
        // per-volume key = autumn-rs/ ++ volume ++ next_inode
        assert_eq!(
            fs_next_inode_key(b"fs/acme/vol0/"),
            b"autumn-rs/fs/acme/vol0/next_inode".as_slice()
        );
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
