//! The kernel mount over the `fs/` tree.
//!
//! The filesystem itself — layout, namespace ops, data path, sessions — is
//! `autumn-fs`. This crate is only what a kernel mount adds: the
//! `fuser::Filesystem` impl (`ops`), the fuser<->compio channel (`bridge`),
//! the dispatch loop (`dispatch`), the kernel cache invalidation thread
//! (`inval`), the read I/O pool (`read_pool`), daemon-side readahead
//! (`prefetch`), the mount's kernel readahead window (`readahead`), and the
//! ONLY place core types convert to `fuser` reply types (`attr`).

pub mod attr;
pub mod bridge;
pub mod dispatch;
pub mod inval;
pub mod ops;
pub mod prefetch;
pub mod read_pool;
pub mod readahead;

/// Re-export `fuser` so downstream test crates (autumn-manager
/// inode-lease tests) can name the bridge-handler reply types
/// (`fuser::FileAttr`) without taking a direct dep on `fuser`.
pub use fuser;
