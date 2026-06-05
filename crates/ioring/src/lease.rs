//! Lease client helpers — re-export from `autumn-client::lease`.
//!
//! F-fuse-lease-1: the lease module was moved into `autumn-client`
//! so autumn-fuse can use it without pulling in the heavy
//! `autumn-ioring` daemon feature. This thin shim preserves the
//! `autumn_ioring::lease::*` import path for the existing daemon
//! call sites + integration tests so the move was a zero-source-
//! diff for callers.

pub use autumn_client::lease::*;
