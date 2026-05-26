//! F242 — io_uring daemon WRITE side: writes into autumn-fuse files (inode +
//! F247 variable-length extents). Write-mirror of F248 `fuse_read`.
//!
//! Pre-F242 the daemon returned `ENOSYS` for `Opcode::Write` — fuse files were
//! only readable through the ring. F242 reuses the OPEN-time `OpenedExtents`
//! (inode + extent map) and lets a Write SQE write straight from the ring
//! buffer slot through the same `ClusterClient` the Read path uses, splitting
//! the source into ≤8 MiB extents (the F247 cap) and routing per extent:
//!
//! - **Append / gap** (model-file write-once hot path): `pos` lands past every
//!   existing extent (or in a hole) → fresh `put` / `put_zc` (≥64 KiB → ZC, per
//!   F235's `zc_worthwhile`; on UCX with the ring registered the value iovec
//!   transfers via RDMA). Capped at the next extent's start so the new extent
//!   can't overlap (the F247 non-overlap invariant).
//! - **Overwrite (RMW)**: `pos` falls inside an existing extent → read-modify-
//!   write that extent's value (its start key stays the same → no overlap).
//!   Costs an extra `get`; rare on the model-file workload.
//!
//! Source bytes are copied out of the ring slot into a heap `Bytes` (one
//! memcpy) before the put: the ring slot is reused by the client as soon as
//! the CQE lands, so we cannot borrow it across the async put. The wire
//! transfer from the heap `Bytes` is zero-copy (`put_zc` keeps the value as
//! its own iovec; UCX RDMA's the iovec when ≥64 KiB).
//!
//! Daemon-side cached state (`opened.extents`, `opened.size`) is updated after
//! each step. When the write extends EOF, `InodeMeta.size` is rewritten in KV
//! so a subsequent `Open` sees the new size (its `load_extent_map` uses the
//! meta's `size` to infer the last extent's length).
//!
//! Uses ONLY autumn-fuse's UNGATED `key` + `schema` modules — no `fuser`,
//! no mount layer, no `FsState` — so the daemon stays a lean io_uring binary.

use anyhow::{anyhow, Result};
use bytes::Bytes;

use autumn_client::{zc_worthwhile, ClusterClient};
use autumn_fuse::key;
use autumn_fuse::schema::{self, MAX_EXTENT};

use crate::fuse_read::OpenedExtents;

/// One step of a planned write. `Append` = no existing extent at `start` → fresh
/// KV put; `Rmw` = `start` falls inside an existing extent → read-modify-write
/// of that extent's value.
#[derive(Debug, PartialEq, Eq)]
pub enum WriteStep {
    /// Create a new extent at `start` carrying `src[src_off..src_off+len]`.
    Append {
        start: u64,
        src_off: usize,
        len: usize,
    },
    /// Splice `src[src_off..src_off+len]` into the extent at `extent_start`,
    /// at in-extent offset `in_extent_off`. `new_value_len` is the resulting
    /// value length (max of old length and `in_extent_off+len`).
    Rmw {
        extent_start: u64,
        in_extent_off: usize,
        src_off: usize,
        len: usize,
        new_value_len: usize,
    },
}

/// Plan a write covering `[off, off + src_len)` against a sorted non-overlapping
/// extent snapshot. Pure — unit-testable. Caller applies each step in order;
/// `extents` is NOT mutated by this function.
pub fn plan_write(extents: &[(u64, u32)], off: u64, src_len: usize) -> Vec<WriteStep> {
    let mut out = Vec::new();
    if src_len == 0 {
        return out;
    }
    let mut pos = off;
    let mut remaining = src_len;
    let mut src_off = 0usize;
    while remaining > 0 {
        let (floor, next) = floor_and_next(extents, pos);
        let next_start = next.unwrap_or(u64::MAX);

        match floor {
            // `pos` lies inside an existing extent → RMW that extent.
            Some((fs, fl)) if pos < fs + fl as u64 => {
                let cap = (fs + MAX_EXTENT as u64).min(next_start);
                let end = (pos + remaining as u64).min(cap);
                let n = (end - pos) as usize;
                let in_off = (pos - fs) as usize;
                let new_value_len = (in_off + n).max(fl as usize);
                out.push(WriteStep::Rmw {
                    extent_start: fs,
                    in_extent_off: in_off,
                    src_off,
                    len: n,
                    new_value_len,
                });
                pos += n as u64;
                src_off += n;
                remaining -= n;
            }
            // Fresh territory (append past EOF or a hole) → new extent.
            _ => {
                let cap = (pos + MAX_EXTENT as u64).min(next_start);
                let end = (pos + remaining as u64).min(cap);
                let n = (end - pos) as usize;
                out.push(WriteStep::Append {
                    start: pos,
                    src_off,
                    len: n,
                });
                pos += n as u64;
                src_off += n;
                remaining -= n;
            }
        }
    }
    out
}

/// `(floor, next)` for `pos` over a sorted extent map.
fn floor_and_next(ext: &[(u64, u32)], pos: u64) -> (Option<(u64, u32)>, Option<u64>) {
    let idx = ext.partition_point(|&(s, _)| s <= pos);
    let floor = if idx > 0 { Some(ext[idx - 1]) } else { None };
    let next = ext.get(idx).map(|&(s, _)| s);
    (floor, next)
}

/// Insert or update `(start, len)` in a sorted extent map (maintains the F247
/// non-overlap invariant under the assumption the caller's plan respects caps).
fn upsert(ext: &mut Vec<(u64, u32)>, start: u64, len: u32) {
    match ext.binary_search_by_key(&start, |&(s, _)| s) {
        Ok(i) => ext[i].1 = len,
        Err(i) => ext.insert(i, (start, len)),
    }
}

/// F242 — write `src` at `[off, off + src.len())` into an opened fuse file.
/// Returns the byte count written (= `src.len()` unless empty).
///
/// The caller (the daemon's `service_sqe`) ensures `src` is the ring buffer
/// slot the client wrote into for this SQE. `src` is copied into a heap `Bytes`
/// before the put — the client may reuse the slot the moment the CQE lands.
///
/// `opened` is updated in place: the extent map is upserted after each step;
/// if EOF moved forward, `opened.size` and the persistent `InodeMeta.size`
/// are both updated (one extra `inode_key` round-trip — acceptable for the
/// model-file write-once workload where bytes-per-write dominates).
pub async fn write_into(
    cluster: &ClusterClient,
    opened: &mut OpenedExtents,
    off: u64,
    src: &[u8],
) -> Result<usize> {
    if src.is_empty() {
        return Ok(0);
    }
    let steps = plan_write(&opened.extents, off, src.len());

    for step in &steps {
        match *step {
            WriteStep::Append {
                start,
                src_off,
                len,
            } => {
                let k = key::extent_key(opened.ino, start);
                if zc_worthwhile(len) {
                    let value = Bytes::copy_from_slice(&src[src_off..src_off + len]);
                    cluster
                        .put_zc(&k, value)
                        .await
                        .map_err(|e| anyhow!("put_zc: {e}"))?;
                } else {
                    cluster
                        .put(&k, &src[src_off..src_off + len])
                        .await
                        .map_err(|e| anyhow!("put: {e}"))?;
                }
                upsert(&mut opened.extents, start, len as u32);
            }
            WriteStep::Rmw {
                extent_start,
                in_extent_off,
                src_off,
                len,
                new_value_len,
            } => {
                let k = key::extent_key(opened.ino, extent_start);
                let mut val = cluster
                    .get(&k)
                    .await
                    .map_err(|e| anyhow!("rmw get: {e}"))?
                    .unwrap_or_default();
                if val.len() < new_value_len {
                    val.resize(new_value_len, 0);
                }
                val[in_extent_off..in_extent_off + len]
                    .copy_from_slice(&src[src_off..src_off + len]);
                let put_len = val.len();
                if zc_worthwhile(put_len) {
                    cluster
                        .put_zc(&k, Bytes::from(val))
                        .await
                        .map_err(|e| anyhow!("rmw put_zc: {e}"))?;
                } else {
                    cluster
                        .put(&k, &val)
                        .await
                        .map_err(|e| anyhow!("rmw put: {e}"))?;
                }
                upsert(&mut opened.extents, extent_start, put_len as u32);
            }
        }
    }

    let new_eof = off + src.len() as u64;
    if new_eof > opened.size {
        opened.size = new_eof;
        // Persist InodeMeta so a later Open's load_extent_map sees the new EOF.
        let ik = key::inode_key(opened.ino);
        let mv = cluster
            .get(&ik)
            .await
            .map_err(|e| anyhow!("inode get: {e}"))?
            .ok_or_else(|| anyhow!("ENOENT inode {} during write", opened.ino))?;
        let mut meta = schema::decode_inode_meta(&mv).map_err(|e| anyhow!("decode inode: {e}"))?;
        meta.size = new_eof;
        let new_mv = schema::encode_inode_meta(&meta);
        cluster
            .put(&ik, &new_mv)
            .await
            .map_err(|e| anyhow!("inode put: {e}"))?;
    }

    Ok(src.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_into_empty_file_one_extent() {
        let steps = plan_write(&[], 0, 1024);
        assert_eq!(
            steps,
            vec![WriteStep::Append {
                start: 0,
                src_off: 0,
                len: 1024
            }]
        );
    }

    #[test]
    fn append_exceeding_cap_splits_into_max_extents() {
        // Write 10 MiB at offset 0 into an empty file → 8 MiB + 2 MiB.
        let m = MAX_EXTENT;
        let steps = plan_write(&[], 0, m + 2 * 1024 * 1024);
        assert_eq!(steps.len(), 2);
        assert_eq!(
            steps[0],
            WriteStep::Append {
                start: 0,
                src_off: 0,
                len: m,
            }
        );
        assert_eq!(
            steps[1],
            WriteStep::Append {
                start: m as u64,
                src_off: m,
                len: 2 * 1024 * 1024,
            }
        );
    }

    #[test]
    fn append_into_hole_caps_at_next_extent() {
        // Existing extent at [8 MiB, 8 MiB + 1 MiB). Write 10 MiB at offset 0
        // should produce one new extent [0, 8 MiB) capped by next_start = 8 MiB,
        // then RMW into the existing extent (continuation).
        let m = MAX_EXTENT as u64;
        let ext = vec![(m, 1024 * 1024)];
        let steps = plan_write(&ext, 0, 10 * 1024 * 1024);
        assert_eq!(steps.len(), 2);
        assert_eq!(
            steps[0],
            WriteStep::Append {
                start: 0,
                src_off: 0,
                len: m as usize,
            }
        );
        // Continuation falls into the existing extent → RMW.
        match steps[1] {
            WriteStep::Rmw {
                extent_start,
                in_extent_off,
                src_off,
                len,
                new_value_len,
            } => {
                assert_eq!(extent_start, m);
                assert_eq!(in_extent_off, 0);
                assert_eq!(src_off, m as usize);
                assert_eq!(len, 2 * 1024 * 1024);
                // new value length = max(old 1 MiB, 0 + 2 MiB) = 2 MiB
                assert_eq!(new_value_len, 2 * 1024 * 1024);
            }
            ref other => panic!("unexpected step: {other:?}"),
        }
    }

    #[test]
    fn overwrite_inside_existing_extent_emits_rmw() {
        // Extent [0, 1 MiB). Overwrite 100 bytes at offset 500.
        let ext = vec![(0, 1024 * 1024)];
        let steps = plan_write(&ext, 500, 100);
        assert_eq!(steps.len(), 1);
        match steps[0] {
            WriteStep::Rmw {
                extent_start,
                in_extent_off,
                src_off,
                len,
                new_value_len,
            } => {
                assert_eq!(extent_start, 0);
                assert_eq!(in_extent_off, 500);
                assert_eq!(src_off, 0);
                assert_eq!(len, 100);
                // new value length = max(old 1 MiB, 600) = 1 MiB
                assert_eq!(new_value_len, 1024 * 1024);
            }
            ref other => panic!("unexpected step: {other:?}"),
        }
    }

    #[test]
    fn overwrite_at_extent_end_grows_value() {
        // Extent [0, 1000). Write 200 bytes at offset 900 → RMW that grows
        // the value to 1100 (still ≤ MAX_EXTENT and ≤ next_start = ∞).
        let ext = vec![(0, 1000)];
        let steps = plan_write(&ext, 900, 200);
        assert_eq!(steps.len(), 1);
        if let WriteStep::Rmw { new_value_len, .. } = steps[0] {
            assert_eq!(new_value_len, 1100);
        } else {
            panic!("expected RMW");
        }
    }

    #[test]
    fn empty_write_emits_no_steps() {
        let steps = plan_write(&[(0u64, 1024u32)], 0, 0);
        assert!(steps.is_empty());
    }

    #[test]
    fn upsert_inserts_and_updates() {
        let mut ext = vec![(0u64, 10u32), (100, 20u32)];
        upsert(&mut ext, 50, 5);
        assert_eq!(ext, vec![(0, 10), (50, 5), (100, 20)]);
        upsert(&mut ext, 0, 99);
        assert_eq!(ext[0], (0, 99));
    }
}
