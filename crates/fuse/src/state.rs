//! Filesystem state owned by the compio thread.
//!
//! Contains ClusterClient, inode cache, dirty tracking, and KV helper methods.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use anyhow::{anyhow, Context, Result};

use autumn_client::ClusterClient;

use crate::schema::{InodeState, ROOT_INO};

/// Central filesystem state, lives on the compio thread (single-threaded, no locks).
pub struct FsState {
    /// `Rc` so the spawned `read::execute` task can hold a clone and call
    /// `get_many_into` without an `&FsState` reference (F244-B).
    pub client: Rc<ClusterClient>,
    pub inodes: HashMap<u64, InodeState>,
    pub dirty_inodes: HashSet<u64>,
    pub next_inode: u64,
    pub inode_batch_end: u64,
    /// FUSE lookup refcounts (separate from open_count).
    pub lookup_count: HashMap<u64, u64>,
}

impl FsState {
    pub async fn new(manager_addr: &str) -> Result<Self> {
        let client = ClusterClient::connect(manager_addr)
            .await
            .context("connect to manager")?;
        Ok(Self {
            client: Rc::new(client),
            inodes: HashMap::new(),
            dirty_inodes: HashSet::new(),
            next_inode: ROOT_INO + 1,
            inode_batch_end: ROOT_INO + 1, // will trigger batch alloc on first use
            lookup_count: HashMap::new(),
        })
    }

    // ── KV helpers ──────────────────────────────────────────────────────────

    /// Get a value from the KV store by key.
    pub async fn kv_get(&mut self, k: &[u8]) -> Result<Vec<u8>> {
        // 2026-06-04 fix — was hand-assembling GetReq + `ps_call`, which
        // BYPASSES the SDK's `call_ps_for_key` retry+region-refresh loop.
        // On a split, the PS rejects the stale region_epoch with
        // FailedPrecondition; without retry it bubbles up as EIO to FUSE
        // and the routing cache stays stale for the rest of the process's
        // lifetime — `ls` / `cat` / `cp` all fail until autumn-fuse is
        // restarted. `client.get` is the same RPC underneath but goes
        // through the standard retry loop (MAX_PS_REFRESHES=10).
        // Same fix applied to kv_get_range / kv_put / kv_delete /
        // kv_range_keys / kv_exists below.
        match self
            .client
            .get(k)
            .await
            .map_err(|e| anyhow!("KV get: {e}"))?
        {
            Some(v) => Ok(v),
            None => Err(anyhow!("not found")),
        }
    }

    /// Get a sub-range of a value from the KV store.
    pub async fn kv_get_range(&mut self, k: &[u8], offset: u32, length: u32) -> Result<Vec<u8>> {
        match self
            .client
            .get_range(k, offset, length)
            .await
            .map_err(|e| anyhow!("KV get_range: {e}"))?
        {
            Some(v) => Ok(v),
            None => Err(anyhow!("not found")),
        }
    }

    /// Put a key-value pair into the KV store.
    ///
    /// F178: every Put is durable (no `must_sync` flag). Pre-F178 there
    /// was a `kv_put` (must_sync=false) and `kv_put_sync` (must_sync=
    /// true) split; post-F178 they collapse to one method because the
    /// extent-node fsync coalescer makes every append durable
    /// regardless. The `kv_put_sync` alias is retained as a no-op
    /// pass-through for callers that explicitly want to read as
    /// "durable Put".
    pub async fn kv_put(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        self.client
            .put(k, v)
            .await
            .map_err(|e| anyhow!("KV put: {e}"))
    }

    /// F178: alias for `kv_put`. See `kv_put` doc — no semantic difference
    /// post-F178 since every write is durable.
    pub async fn kv_put_sync(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        self.kv_put(k, v).await
    }

    /// Delete a key from the KV store.
    pub async fn kv_delete(&mut self, k: &[u8]) -> Result<()> {
        self.client
            .delete(k)
            .await
            .map_err(|e| anyhow!("KV delete: {e}"))
    }

    /// Range scan with prefix and optional start key.
    ///
    /// Returns keys only — PS `handle_range` does not populate values on the wire.
    /// Callers that need values must issue a separate `kv_get` per key.
    pub async fn kv_range_keys(
        &mut self,
        prefix: &[u8],
        start: &[u8],
        limit: u32,
    ) -> Result<Vec<Vec<u8>>> {
        let r = self
            .client
            .range(prefix, start, limit)
            .await
            .map_err(|e| anyhow!("KV range: {e}"))?;
        Ok(r.entries.into_iter().map(|e| e.key).collect())
    }

    /// Check if a key exists (uses Head RPC).
    pub async fn kv_exists(&mut self, k: &[u8]) -> Result<bool> {
        let meta = self
            .client
            .head(k)
            .await
            .map_err(|e| anyhow!("KV head: {e}"))?;
        Ok(meta.found)
    }
}
