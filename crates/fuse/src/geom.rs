//! F-FS-GEOM-DECLARED: the fs-wide DECLARED stripe geometry.
//!
//! Geometry is a policy value and `[0x04]stripe_geom` is its authoritative home.
//! It used to have none: `autumnfs` reverse-engineered the lane count from the
//! CURRENT partition split points on every large-file create. Partitions are the
//! manager's elastic placement unit — it splits, merges and rebalances them
//! autonomously — so deriving a durable per-file property from a mutable
//! placement artifact drifted silently.
//!
//! This module is UNGATED (like `key` / `schema`) so `autumnfs`, which imports
//! `autumn-fuse` with `default-features = false` to skip the fuser/libc
//! kernel-side deps, shares the exact same reader as the mount. One
//! implementation, no drift — the duplication this replaces is what let the CLI
//! hardcode the wire prefix `b"fs/"`, a byte string that has already changed
//! twice (tenant-first, then Option 3).
//!
//! What stays per-file is the STAMP (`InodeMeta.stripe`): every file remains
//! self-describing, so reads never consult cluster shape and any
//! split/merge/rebalance leaves existing files correct. Only the write-side
//! derivation moves here.

use anyhow::{anyhow, Result};

use crate::key;
use crate::schema::{self, StripeLayout};

/// Read the fs-wide declared stripe geometry.
///
/// Three outcomes, and the distinction between the last two is the whole point:
/// - `Ok(Some(geom))` — this fs is striped; new files are stamped with it.
/// - `Ok(None)` — genuinely absent ⇒ this fs is not striped. Legal and quiet.
/// - `Err` — a hard KV error. **PROPAGATES.** The detection this replaces
///   swallowed every failure into `lanes = 1`, so one transient manager blip at
///   create time produced a permanently single-partition 41 GB file whose only
///   symptom was "throughput is mysteriously bad" — the hardest class to
///   diagnose. A loud, immediate, retryable refusal is strictly better.
///
/// Callers read this ONCE per session (the mount at `ensure_root`, `autumnfs`
/// per invocation) and cache it; it is not a per-file lookup.
pub async fn read_stripe_geom(
    client: &autumn_client::ClusterClient,
) -> Result<Option<StripeLayout>> {
    let bytes = client
        .get(&key::stripe_geom_key())
        .await
        .map_err(|e| anyhow!("read declared stripe geometry: {e}"))?;
    match bytes {
        Some(b) => Ok(Some(schema::decode_stripe_geom(&b).map_err(|e| {
            anyhow!("declared stripe geometry is corrupt: {e}")
        })?)),
        None => Ok(None),
    }
}

/// Declare the fs-wide stripe geometry.
///
/// Written by `autumn-op presplit --namespace fs --lanes N` in the SAME command
/// that cuts the lane boundaries, so the declaration and the placement cannot be
/// created out of step.
pub async fn write_stripe_geom(
    client: &autumn_client::ClusterClient,
    layout: &StripeLayout,
) -> Result<()> {
    layout.checked().map_err(|e| anyhow!(e))?;
    client
        .put(&key::stripe_geom_key(), &schema::encode_stripe_geom(layout))
        .await
        .map_err(|e| anyhow!("declare stripe geometry: {e}"))
}
