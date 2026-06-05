pub mod key;
pub mod schema;

#[cfg(feature = "fuse")]
pub mod bridge;
#[cfg(feature = "fuse")]
pub mod dir;
#[cfg(feature = "fuse")]
pub mod dispatch;
#[cfg(feature = "fuse")]
pub mod extent;
#[cfg(feature = "fuse")]
pub mod meta;
#[cfg(feature = "fuse")]
pub mod ops;
#[cfg(feature = "fuse")]
pub mod read;
#[cfg(feature = "fuse")]
pub mod state;
#[cfg(feature = "fuse")]
pub mod sync_task;
#[cfg(feature = "fuse")]
pub mod write;

/// Re-export `fuser` so downstream test crates (autumn-manager tests
/// for F-fuse-lease-*) can name the bridge-handler reply types
/// (`fuser::FileAttr`) without taking a direct dep on `fuser`. The
/// FUSE-callback types come and go with the `fuse` feature, same as
/// the rest of this module set.
#[cfg(feature = "fuse")]
pub use fuser;
