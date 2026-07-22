//! Core data types for the FUSE filesystem layer.

use rkyv::{Archive, Deserialize, Serialize};

/// Decode InodeMeta from rkyv bytes.
pub fn decode_inode_meta(bytes: &[u8]) -> Result<InodeMeta, String> {
    if bytes.is_empty() {
        return Err("empty InodeMeta bytes".to_string());
    }
    autumn_rpc::partition_rpc::rkyv_decode(bytes).map_err(|e| format!("{:?}", e))
}

/// Encode InodeMeta to rkyv bytes.
pub fn encode_inode_meta(meta: &InodeMeta) -> Vec<u8> {
    autumn_rpc::partition_rpc::rkyv_encode(meta).to_vec()
}

/// F-FS-GEOM-DECLARED: decode the declared fs-wide stripe geometry
/// (`[0x04]stripe_geom`). Validated through `checked()` so a corrupt/truncated
/// value fails loud here rather than dividing by zero deep in the key builder.
pub fn decode_stripe_geom(bytes: &[u8]) -> Result<StripeLayout, String> {
    if bytes.is_empty() {
        return Err("empty stripe_geom bytes".to_string());
    }
    let layout: StripeLayout =
        autumn_rpc::partition_rpc::rkyv_decode(bytes).map_err(|e| format!("{:?}", e))?;
    layout.checked()?;
    Ok(layout)
}

/// Encode the declared fs-wide stripe geometry.
pub fn encode_stripe_geom(layout: &StripeLayout) -> Vec<u8> {
    autumn_rpc::partition_rpc::rkyv_encode(layout).to_vec()
}

/// Decode DirentValue from rkyv bytes.
pub fn decode_dirent(bytes: &[u8]) -> Result<DirentValue, String> {
    if bytes.is_empty() {
        return Err("empty DirentValue bytes".to_string());
    }
    autumn_rpc::partition_rpc::rkyv_decode(bytes).map_err(|e| format!("{:?}", e))
}

/// Encode DirentValue to rkyv bytes.
pub fn encode_dirent(dirent: &DirentValue) -> Vec<u8> {
    autumn_rpc::partition_rpc::rkyv_encode(dirent).to_vec()
}

/// Inode metadata stored in KV at key `[0x01][ino: u64 BE]`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct InodeMeta {
    /// File type + permissions (e.g. S_IFREG | 0o644).
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    /// Logical file size in bytes.
    pub size: u64,
    /// Hard link count.
    pub nlink: u32,
    pub atime_secs: i64,
    pub atime_nsecs: u32,
    pub mtime_secs: i64,
    pub mtime_nsecs: u32,
    pub ctime_secs: i64,
    pub ctime_nsecs: u32,
    /// Inline data for small files (≤4KB). None for directories or large files.
    pub inline_data: Option<Vec<u8>>,
    /// Symlink target path. None for non-symlinks.
    pub symlink_target: Option<Vec<u8>>,
    /// F-FS-STRIPE: `Some` iff this file's data extents are STRIPED across
    /// `lanes` partitions. `None` = the legacy single-partition layout
    /// (`[0x03][ino][off]`, all extents in one partition/log_stream). A large
    /// file (> threshold) is stamped striped at create so its extents round-robin
    /// across lane partitions (`[0x03][lane][ino][off]`) → parallel write/read
    /// beyond the single-stream ceiling. Immutable once set; the reader branches
    /// on this to pick the key layout, so old files stay correct with no migration.
    pub stripe: Option<StripeLayout>,
}

/// F-FS-STRIPE: per-file stripe geometry. An extent at logical offset `off` lives
/// on lane `(off / unit_bytes) % lanes`, whose wire key is `[0x03][lane][ino][off]`.
/// Lane partitions are pre-split at `[0x03][lane]` (static, ino-independent) and
/// placed on distinct PSs, so consecutive extents hit different PSs in parallel.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StripeLayout {
    /// Number of stripe lanes = number of pre-split lane partitions. 1..=255.
    pub lanes: u8,
    /// Stripe unit in bytes. v1 uses `MAX_EXTENT` (each extent its own lane).
    pub unit_bytes: u32,
}

impl StripeLayout {
    /// coco P3: validate the PERSISTED geometry before it feeds `stripe_lane`'s
    /// `off / unit_bytes` + `% lanes` — a corrupt/hand-written inode with
    /// `lanes == 0` or `unit_bytes == 0` would otherwise div-by-zero panic the
    /// reader/rm. Returns `(lanes, unit_bytes)` or a diagnostic error. Every path
    /// that reads `meta.stripe` from KV MUST route through this, not the raw fields.
    pub fn checked(&self) -> Result<(u8, u32), String> {
        if self.lanes < 1 || self.unit_bytes < 1 {
            return Err(format!(
                "invalid stripe layout (corrupt inode?): lanes={} unit_bytes={}",
                self.lanes, self.unit_bytes
            ));
        }
        Ok((self.lanes, self.unit_bytes))
    }
}

/// F-FS-STRIPE (coco P3): the `unit_bytes`-aligned logical offsets (0, u, 2u, …)
/// of a striped file of `size` bytes — the enumeration the reader / rm / delete
/// paths use to COMPUTE lane keys.
///
/// **`unit` MUST come from the file's PERSISTED `StripeLayout.unit_bytes`, never
/// from the `MAX_EXTENT` constant.** This used to step by the compile-time
/// constant, which silently assumed "the extent cap today == the extent cap when
/// this file was written". `MAX_EXTENT` is explicitly slated for retuning on
/// full-resource hardware, and the day it shrinks (8 MiB → 4 MiB) every existing
/// striped file would enumerate offsets (0, 4M, 8M…) that do not match its
/// written keys (0, 8M, 16M…) — and because a missing extent READS AS ZEROS
/// (sparse-file semantics, `read::execute`), half of every striped file would
/// come back zero-filled with no error. That is the same silent-misread class the
/// stripe stamp exists to prevent, so the stride is now part of the file's
/// self-describing geometry. Decoupling `unit_bytes` from the extent cap later
/// (multi-extent stripe units) needs a new persisted field + a schema bump.
///
/// Guards a corrupt huge `size`: a bare `while off < size { off += unit }` would
/// wrap `off` (infinite loop) or push billions of entries (OOM) if
/// `size ≈ u64::MAX`. Errors when the extent count exceeds a sane ceiling.
/// `unit == 0` errors rather than dividing by zero (`StripeLayout::checked()`
/// is the normal gate, but this is a pub fn taking a bare u32).
pub fn striped_extent_offsets(size: u64, unit: u32) -> Result<Vec<u64>, String> {
    const MAX_EXTENTS: u64 = 16 * 1024 * 1024; // 16M × 8 MiB = 128 TiB
    if unit == 0 {
        return Err("striped file has unit_bytes=0 (corrupt inode?)".to_string());
    }
    let unit = unit as u64;
    let count = size.div_ceil(unit);
    if count > MAX_EXTENTS {
        return Err(format!(
            "striped file size {size} too large ({count} extents > {MAX_EXTENTS} cap — corrupt inode?)"
        ));
    }
    // count ≤ 16M and unit ≤ MAX_EXTENT-class values ⇒ i * unit stays far below
    // u64::MAX; saturating_mul keeps that true even for an absurd stamped unit.
    Ok((0..count).map(|i| i.saturating_mul(unit)).collect())
}

/// Directory entry stored in KV at key `[0x02][parent_ino: u64 BE][name]`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DirentValue {
    pub child_inode: u64,
    /// File type constant: DT_REG=8, DT_DIR=4, DT_LNK=10.
    pub file_type: u8,
}

/// Maximum extent (variable-length data block) size: 8 MiB (F247).
///
/// Files are stored as **variable-length extents** keyed by their logical byte
/// offset (`[0x03][ino][logical_off BE]`), NOT fixed 256 KiB chunks. A
/// sequential (write-once) stream coalesces into extents capped at `MAX_EXTENT`;
/// the last/partial extent is shorter → "variable like Linux extents". 8 MiB
/// matches the project's large-value bench size and the UCX large-read sweet
/// spot, so the ≥64 KiB ZC path (and the future RDMA path, F243) is engaged for
/// whole-extent reads. The write buffer also flushes at this granularity.
pub const MAX_EXTENT: usize = 8 * 1024 * 1024;

/// Small file inline threshold: 4KB (matches VALUE_THROTTLE).
pub const INLINE_THRESHOLD: usize = 4096;

/// Inode allocation batch size.
pub const INODE_ALLOC_BATCH: u64 = 1000;

/// Per-inode write buffer capacity in extents. Buffer = WRITE_BUF_EXTENTS *
/// [`MAX_EXTENT`] bytes (8 × 8 MiB = 64 MiB). The buffer fills with sequential
/// fuse `write` syscalls and flushes one whole buffer's worth at a time via
/// `extent::write_region`. With WRITE_BUF_EXTENTS == 1 the flush is a single
/// `put` and the cp throughput ceiling is `MAX_EXTENT / RPC_RTT` (~270 MB/s
/// on local NVMe + TCP loopback + 3 replicas). With > 1, write_region splits
/// into multiple extents and `put_many` pipelines them at `APPEND_PIPELINE_
/// DEPTH` concurrency, so the ceiling scales to `WRITE_BUF_EXTENTS *
/// MAX_EXTENT / RPC_RTT` until the EN-side disk + replica fanout saturates.
///
/// Cost: up to `WRITE_BUF_EXTENTS * MAX_EXTENT` (64 MiB) of pre-flush RAM per
/// actively-writing inode. For sglang-style model checkpoint cp (a handful
/// of concurrent multi-GB files) this is negligible; for a workload with
/// thousands of concurrently-open dirty inodes a smaller value may be
/// appropriate.
pub const WRITE_BUF_EXTENTS: usize = 8;

/// Per-inode write buffer capacity in bytes. See [`WRITE_BUF_EXTENTS`].
pub const WRITE_BUF_CAP: usize = WRITE_BUF_EXTENTS * MAX_EXTENT;

/// Root inode number (FUSE_ROOT_ID).
pub const ROOT_INO: u64 = 1;

/// F-KEY-NS: the on-disk layout version stamped per tenant in the superblock
/// (`[0x04]schema_version`, so it lands at `fs/{tenant}/[0x04]schema_version`).
///
/// - **v1** = the pre-namespace layout (raw `0x01`–`0x04` keys under a Raw client
///   binding). Never actually stamped — v1 filesystems carry NO schema_version
///   key.
/// - **v2** = the namespaced layout: keys are RELATIVE to `fs/{tenant}/` (the
///   client prepends it), with a cluster-unique GLOBAL inode counter. (Removing
///   the short-lived SD-3 `{volume}` layer does NOT change this relative on-disk
///   format — only where the tree lives on the wire — so it stays v2.)
///
/// `meta::ensure_schema_version` stamps a fresh filesystem with this value and
/// FAILS LOUD if an existing stamp differs — a future incompatible layout (v3+)
/// then refuses to mount rather than silently corrupting data.
/// - **v3** = F-FS-STRIPE: `InodeMeta` gains the `stripe: Option<StripeLayout>`
///   field (rkyv layout change — v2 inode bytes can't decode), and large files
///   store data extents under the lane-striped key `[0x03][lane][ino][off]`.
///   Stop-world reset from v2 (no in-place migration; old files would decode
///   wrong). Small / legacy files keep `stripe=None` + the `[0x03][ino][off]` key.
///
/// BUMP this whenever the key layout / value encoding changes incompatibly.
pub const SCHEMA_VERSION: u64 = 3;

// File type constants (from libc)
pub const DT_REG: u8 = 8;
pub const DT_DIR: u8 = 4;
pub const DT_LNK: u8 = 10;

/// A single readdir entry. F-FS-UNIFY M1: lives in the fuser-free core
/// (`kind` is a `DT_*` byte, not `fuser::FileType`); the FUSE reply
/// boundary (`ops.rs`) converts via `attr::dt_to_filetype`.
pub struct ReaddirEntry {
    pub ino: u64,
    pub offset: i64,
    /// `DT_REG` / `DT_DIR` / `DT_LNK`.
    pub kind: u8,
    pub name: std::ffi::OsString,
}

/// Runtime write buffer state for a single inode (compio thread-local).
pub struct WriteBuffer {
    /// Buffer storage, capacity = [`WRITE_BUF_CAP`].
    pub buf: Vec<u8>,
    /// Starting file offset of buffered data.
    pub offset: i64,
    /// Bytes currently in buffer.
    pub len: usize,
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBuffer {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(WRITE_BUF_CAP),
            offset: 0,
            len: 0,
        }
    }
}

/// Per-inode runtime state (compio thread-local, not persisted).
pub struct InodeState {
    pub meta: InodeMeta,
    pub write_buf: Option<WriteBuffer>,
    pub dirty: bool,
    pub open_count: u32,
    /// F247 runtime extent map: sorted `(logical_start, value_len)` of this
    /// file's data extents. `None` = not yet loaded (rebuild via range-scan of
    /// the `[0x03][ino]` prefix + neighbor/file-size length inference). This is a
    /// *runtime cache only* — the persistent source of truth is the extent KV
    /// keys themselves (the implicit-key design: no extent list in InodeMeta).
    /// Invalidated (set `None`) on truncate; maintained incrementally on write.
    pub extents: Option<Vec<(u64, u32)>>,
    /// BUG-LEASE-7 (P2 #8, 2026-06-06) — the manager-side lease
    /// version that was current when `meta` / `extents` were
    /// LAST refetched from KV. On Open the dispatcher compares
    /// this against the version returned by AcquireLease; on
    /// mismatch the cached InodeState is dropped and rebuilt
    /// from `get_inode` (and a fresh extent rescan on next
    /// `ensure_extents_loaded`). Without this a second mount
    /// that re-Opens the inode after the first mount wrote +
    /// closed would happily serve from its stale cached `meta`
    /// (correct user-space lease state notwithstanding) — the
    /// close-to-open bytes invariant breaks at the FUSE layer
    /// even when the manager + kernel-cache layers are correct.
    ///
    /// `0` means "no lease seen yet" — only seeded by Open and
    /// preserved across `forget` for as long as the entry lives.
    pub cached_version: u64,
}
