//! FUSE-side type conversions: core `InodeMeta` / `DT_*` → `fuser` reply
//! types.
//!
//! F-FS-UNIFY M1: this is the ONLY place the filesystem core's data model
//! meets `fuser`. The core (`meta`/`dir`/`extent`/`read`/`write`/`state`)
//! returns plain `InodeMeta` / `DT_*` bytes and compiles without `fuser`
//! (`--no-default-features --features core`), so the PyO3 `autumn.Fs`
//! binding (M2) can reuse it verbatim; the kernel-mount glue (`dispatch`/
//! `ops`) converts here at the reply boundary.

use std::time::{SystemTime, UNIX_EPOCH};

use fuser::FileAttr;

use crate::meta::{S_IFDIR, S_IFLNK, S_IFMT};
use crate::schema::{InodeMeta, DT_DIR, DT_LNK};

/// Convert InodeMeta to fuser::FileAttr.
pub fn inode_to_attr(ino: u64, meta: &InodeMeta) -> FileAttr {
    let kind = mode_to_filetype(meta.mode);
    FileAttr {
        ino,
        size: meta.size,
        blocks: meta.size.div_ceil(512),
        atime: system_time(meta.atime_secs, meta.atime_nsecs),
        mtime: system_time(meta.mtime_secs, meta.mtime_nsecs),
        ctime: system_time(meta.ctime_secs, meta.ctime_nsecs),
        crtime: system_time(meta.ctime_secs, meta.ctime_nsecs),
        kind,
        perm: (meta.mode & 0o7777) as u16,
        nlink: meta.nlink,
        uid: meta.uid,
        gid: meta.gid,
        rdev: 0,
        // Optimal-IO hint reported via stat(2) st_blksize. Kept at 1 MiB (not
        // the 8 MiB MAX_EXTENT) so stdio/cp size their buffers sensibly while
        // still issuing large reads; FUSE readahead is configured separately.
        blksize: 1024 * 1024,
        flags: 0,
    }
}

fn system_time(secs: i64, nsecs: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + std::time::Duration::new(secs as u64, nsecs)
    } else {
        UNIX_EPOCH
    }
}

fn mode_to_filetype(mode: u32) -> fuser::FileType {
    match mode & S_IFMT {
        m if m == S_IFDIR => fuser::FileType::Directory,
        m if m == S_IFLNK => fuser::FileType::Symlink,
        _ => fuser::FileType::RegularFile,
    }
}

/// Convert a core `DT_*` dirent type byte to fuser::FileType.
pub fn dt_to_filetype(dt: u8) -> fuser::FileType {
    match dt {
        DT_DIR => fuser::FileType::Directory,
        DT_LNK => fuser::FileType::Symlink,
        _ => fuser::FileType::RegularFile,
    }
}
