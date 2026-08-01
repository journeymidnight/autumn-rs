// M1: the crate is split into two feature layers.
//
//   `core` — the fuser-FREE filesystem core: inode/dirent/extent layout
//   (`key`/`schema`), namespace ops (`meta`/`dir`), data path
//   (`extent`/`read`/`write`), and runtime state (`state`). Everything
//   here returns plain `InodeMeta` / `DT_*` data and compiles with
//   `--no-default-features --features core`. This is what the PyO3
//   `autumn.Fs` binding (M2) and the fsspec facade (M3) build on — one
//   namespace implementation, two front-ends.
//
//   `fuse` (default; implies `core`) — the kernel-mount glue: the
//   `fuser::Filesystem` impl (`ops`), the fuser↔compio channel
//   (`bridge`), the dispatch loop (`dispatch`), and the ONLY place core
//   types convert to `fuser` reply types (`attr`).

pub mod geom;
pub mod key;
pub mod schema;

#[cfg(feature = "core")]
pub mod dir;
#[cfg(feature = "core")]
pub mod extent;
#[cfg(feature = "core")]
pub mod lease_tasks;
#[cfg(feature = "core")]
pub mod meta;
#[cfg(feature = "core")]
pub mod read;
#[cfg(feature = "core")]
pub mod state;
#[cfg(feature = "core")]
pub mod write;

#[cfg(feature = "fuse")]
pub mod attr;
#[cfg(feature = "fuse")]
pub mod bridge;
#[cfg(feature = "fuse")]
pub mod dispatch;
#[cfg(feature = "fuse")]
pub mod ops;

/// Re-export `fuser` so downstream test crates (autumn-manager
/// inode-lease tests) can name the bridge-handler reply types
/// (`fuser::FileAttr`) without taking a direct dep on `fuser`. The
/// FUSE-callback types come and go with the `fuse` feature, same as
/// the rest of this module set.
#[cfg(feature = "fuse")]
pub use fuser;
