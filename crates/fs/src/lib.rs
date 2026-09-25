//! The `fs/` tree: autumn's filesystem, stored in the partition layer.
//!
//! Inode, dirent and extent layout (`key`/`schema`/`geom`), namespace ops
//! (`meta`/`dir`), the data path (`extent`/`read`/`write`), segmented files
//! (`segment`), whole-file publication and publishing sessions (`publish`),
//! the per-session lease tasks (`lease_tasks`) and the runtime state
//! (`state`). Everything here returns plain `InodeMeta` / `DT_*` data and
//! knows nothing about a kernel mount.
//!
//! Front-ends: the fuse mount (`autumn-fuse`, the only place core types turn
//! into `fuser` replies), the S3 gateway, the `autumnfs` CLI and the PyO3
//! `autumn.Fs` binding. One implementation, so the front-ends cannot drift.

pub mod dir;
pub mod extent;
pub mod geom;
pub mod key;
pub mod lease_tasks;
pub mod meta;
pub mod publish;
pub mod read;
pub mod schema;
pub mod segment;
pub mod state;
pub mod write;
