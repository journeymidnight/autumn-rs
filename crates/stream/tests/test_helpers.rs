#![allow(dead_code)] // test/bench support toolbox — not every helper is used by every target
//! Shared test infrastructure for autumn-stream integration tests.
//!
//! Provides `TestConn` (typed RPC client over autumn-rpc) and `start_node`
//! helpers that spawn an ExtentNode on the compio runtime.

use std::net::SocketAddr;
use std::time::Duration;

use autumn_stream::conn_pool::ConnPool;
use autumn_stream::extent_rpc::*;
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use bytes::Bytes;

/// Pick a free TCP port by binding to :0 then dropping the listener.
pub fn pick_addr() -> SocketAddr {
    autumn_test_ports::addr()
}

/// Start an ExtentNode on the given address (single-disk).
/// Returns after the server is listening.
pub async fn start_node(data_dir: &std::path::Path, addr: SocketAddr) -> ExtentNode {
    let config = ExtentNodeConfig::new(data_dir.to_path_buf(), 1);
    let node = ExtentNode::new(config).await.expect("create ExtentNode");
    // The returned handle SHARES state with the served node (Rc interior), so a
    // test can drive it directly as well as over RPC.
    let served = node.clone();
    compio::runtime::spawn(async move {
        if let Err(e) = served.serve(addr).await {
            panic!("serve: {e:#}");
        }
    })
    .detach();
    compio::time::sleep(Duration::from_millis(120)).await;
    node
}

/// Start a multi-disk ExtentNode.
pub async fn start_node_multi(dirs: Vec<std::path::PathBuf>, addr: SocketAddr) {
    let config = ExtentNodeConfig::new_multi(dirs);
    let node = ExtentNode::new(config).await.expect("create ExtentNode");
    compio::runtime::spawn(async move {
        if let Err(e) = node.serve(addr).await {
            panic!("extent node serve: {e:#}");
        }
    })
    .detach();
    compio::time::sleep(Duration::from_millis(120)).await;
}

/// Typed RPC client for integration tests. Wraps ConnPool.
pub struct TestConn {
    pool: ConnPool,
    addr: String,
}

impl TestConn {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            pool: ConnPool::new(),
            addr: addr.to_string(),
        }
    }

    pub async fn convert_to_ec(&self, req: &ConvertToEcReq) -> ConvertToEcResp {
        let resp = self
            .pool
            .call(&self.addr, MSG_CONVERT_TO_EC, rkyv_encode(req))
            .await
            .expect("convert_to_ec RPC");
        rkyv_decode::<ConvertToEcResp>(&resp).expect("decode ConvertToEcResp")
    }

    pub async fn alloc_extent(&self, extent_id: u64) -> AllocExtentResp {
        let payload = rkyv_encode(&AllocExtentReq { extent_id });
        let resp = self
            .pool
            .call(&self.addr, MSG_ALLOC_EXTENT, payload)
            .await
            .expect("alloc_extent RPC");
        rkyv_decode::<AllocExtentResp>(&resp).expect("decode AllocExtentResp")
    }

    pub async fn append(
        &self,
        extent_id: u64,
        eversion: u64,
        commit: u64,
        owner_epoch: i64,
        payload: Vec<u8>,
    ) -> AppendResp {
        self.try_append(extent_id, eversion, commit, owner_epoch, payload).await.expect("append RPC")
    }

    /// `append`, with a refusal that comes as an error frame returned.
    pub async fn try_append(
        &self,
        extent_id: u64,
        eversion: u64,
        commit: u64,
        owner_epoch: i64,
        payload: Vec<u8>,
    ) -> anyhow::Result<AppendResp> {
        let req = AppendReq {
            extent_id,
            eversion,
            commit,
            owner_epoch,
            payload: Bytes::from(payload).into(),
        };
        let resp = self.pool.call_prepared(&self.addr, MSG_APPEND, &req.prepared()).await?;
        Ok(AppendResp::decode(resp).expect("decode AppendResp"))
    }

    pub async fn commit_length(&self, extent_id: u64, owner_epoch: i64) -> CommitLengthResp {
        let req = CommitLengthReq {
            extent_id,
            owner_epoch,
        };
        let resp = self
            .pool
            .call(&self.addr, MSG_COMMIT_LENGTH, req.encode())
            .await
            .expect("commit_length RPC");
        CommitLengthResp::decode(resp).expect("decode CommitLengthResp")
    }

    /// Fence-free probe. Use this instead of
    /// `commit_length(eid, 0)` when the test only wants to verify
    /// "extent exists + what's its current length" without
    /// claiming an owner lock.
    pub async fn probe_extent(&self, extent_id: u64) -> ProbeExtentResp {
        let req = ProbeExtentReq { extent_id };
        let resp = self
            .pool
            .call(&self.addr, MSG_PROBE_EXTENT, req.encode())
            .await
            .expect("probe_extent RPC");
        ProbeExtentResp::decode(resp).expect("decode ProbeExtentResp")
    }

    pub async fn read_bytes(
        &self,
        extent_id: u64,
        eversion: u64,
        offset: u64,
        length: u64,
    ) -> ReadBytesResp {
        self.read_bytes_from(extent_id, eversion, offset, length, PayloadRef::in_dat())
            .await
    }

    /// `read_bytes` that surfaces a REFUSAL instead of panicking: a read the
    /// node declines (a pread error) comes back as an error frame, not a
    /// response code, and bytes that fail their block crcs (a scrubbed block
    /// that rotted) are an error too.
    pub async fn read_bytes_result(
        &self,
        extent_id: u64,
        eversion: u64,
        offset: u64,
        length: u64,
    ) -> Result<ReadBytesResp, String> {
        self.read_scan(extent_id, eversion, offset, length, PayloadRef::in_dat()).await
    }

    /// `read_bytes` naming a specific payload file.
    pub async fn read_bytes_from(
        &self,
        extent_id: u64,
        eversion: u64,
        offset: u64,
        length: u64,
        payload: PayloadRef,
    ) -> ReadBytesResp {
        self.read_scan(extent_id, eversion, offset, length, payload).await.expect("read RPC")
    }

    /// A background-style read: up to `length` bytes (to the end when 0), with
    /// block crcs, checked.
    async fn read_scan(
        &self,
        extent_id: u64,
        eversion: u64,
        offset: u64,
        length: u64,
        payload: PayloadRef,
    ) -> Result<ReadBytesResp, String> {
        let mode = if length == 0 { READ_TO_END } else { READ_UP_TO };
        let req = rkyv_encode(&ReadReq::new(extent_id, eversion, offset, length, mode, payload).with_crcs());
        let frame = self
            .pool
            .call_frame_timeout(&self.addr, MSG_READ, req, Duration::from_secs(10))
            .await
            .map_err(|e| e.to_string())?;
        let code = frame.payload[0];
        let resp = rkyv_decode::<ReadResp>(&frame.payload[1..]).expect("decode ReadResp");
        let value = frame.value.bytes().clone();
        if code == CODE_OK {
            check_read_block_crcs(offset, &value, &resp.block_crcs)?;
        }
        Ok(ReadBytesResp { code, end: resp.end, payload: value })
    }

    /// An exact `MSG_READ` of `.dat`: `(code, bytes)`, copied out of the pooled
    /// receive buffer for assertion convenience.
    pub async fn read_exact(&self, extent_id: u64, eversion: u64, offset: u64, length: u64) -> (u8, Vec<u8>) {
        self.read_exact_from(extent_id, eversion, offset, length, PayloadRef::in_dat()).await
    }

    /// `read_exact`, the RPC's error returned instead of panicking.
    pub async fn try_read_exact(&self, extent_id: u64, eversion: u64, offset: u64, length: u64) -> anyhow::Result<(u8, Vec<u8>)> {
        let (code, _, got) = self.try_read(extent_id, eversion, offset, length, READ_EXACT, PayloadRef::in_dat()).await?;
        Ok((code, got))
    }

    /// `read_exact` naming a specific payload file.
    pub async fn read_exact_from(&self, extent_id: u64, eversion: u64, offset: u64, length: u64, payload: PayloadRef) -> (u8, Vec<u8>) {
        let (code, _, got) = self.try_read(extent_id, eversion, offset, length, READ_EXACT, payload).await.expect("read RPC");
        (code, got)
    }

    /// One `MSG_READ` of `.dat`: the code, the replica's end, and the bytes.
    pub async fn read(&self, extent_id: u64, eversion: u64, offset: u64, length: u64, mode: u8) -> (u8, u64, Vec<u8>) {
        self.try_read(extent_id, eversion, offset, length, mode, PayloadRef::in_dat()).await.expect("read RPC")
    }

    async fn try_read(
        &self,
        extent_id: u64,
        eversion: u64,
        offset: u64,
        length: u64,
        mode: u8,
        payload: PayloadRef,
    ) -> anyhow::Result<(u8, u64, Vec<u8>)> {
        let req = ReadReq::new(extent_id, eversion, offset, length, mode, payload);
        let z = self.pool.call_into_pooled(&self.addr, MSG_READ, rkyv_encode(&req), Duration::from_secs(5)).await?;
        let resp = rkyv_decode::<ReadResp>(&z.ctrl).map_err(anyhow::Error::msg)?;
        Ok((z.code, resp.end, z.buf.as_ref().to_vec()))
    }

    pub async fn df(&self, tasks: Vec<RecoveryTask>, disk_ids: Vec<u64>) -> DfResp {
        let payload = rkyv_encode(&DfReq { tasks, disk_ids });
        let resp = self
            .pool
            .call(&self.addr, MSG_DF, payload)
            .await
            .expect("df RPC");
        rkyv_decode::<DfResp>(&resp).expect("decode DfResp")
    }

    pub async fn scrub_extents(&self, tasks: Vec<ScrubTask>) -> CodeResp {
        let payload = rkyv_encode(&ScrubExtentsReq { tasks });
        let resp = self
            .pool
            .call(&self.addr, MSG_SCRUB_EXTENTS, payload)
            .await
            .expect("scrub_extents RPC");
        rkyv_decode::<CodeResp>(&resp).expect("decode CodeResp")
    }

    pub async fn delete_extent(&self, extent_id: u64) -> CodeResp {
        let payload = rkyv_encode(&DeleteExtentReq {
            extent_id,
            // Unspecified: the helper drives whichever node it is pointed at.
            node_uuid: String::new(),
        });
        let resp = self
            .pool
            .call(&self.addr, MSG_DELETE_EXTENT, payload)
            .await
            .expect("delete_extent RPC");
        rkyv_decode::<CodeResp>(&resp).expect("decode CodeResp")
    }

    pub async fn write_shard(
        &self,
        extent_id: u64,
        shard_index: u32,
        sealed_length: u64,
        eversion: u64,
        payload: Vec<u8>,
    ) -> WriteShardResp {
        self.write_shard_with_nonce(extent_id, shard_index, sealed_length, eversion, 0, payload)
            .await
    }

    /// `write_shard` stamped with a conversion-attempt nonce. Nonces are etcd
    /// revisions in production, so a LOWER one names a superseded attempt.
    #[allow(clippy::too_many_arguments)]
    pub async fn write_shard_with_nonce(
        &self,
        extent_id: u64,
        shard_index: u32,
        sealed_length: u64,
        eversion: u64,
        attempt_nonce: u64,
        payload: Vec<u8>,
    ) -> WriteShardResp {
        // added `owner_epoch`; tests use 0 (the documented
        // "no fence requested" wire-compat sentinel).
        let req = WriteShardReq {
            extent_id,
            shard_index,
            sealed_length,
            shard_len: payload.len() as u64,
            eversion,
            owner_epoch: 0,
            // whole-shard single-stripe write (offset 0) — the degenerate form.
            shard_offset: 0,
            attempt_nonce,
            payload: Bytes::from(payload),
        };
        let resp = self
            .pool
            .call_prepared(&self.addr, MSG_WRITE_SHARD, &req.prepared())
            .await
            .expect("write_shard RPC");
        WriteShardResp::decode(resp).expect("decode WriteShardResp")
    }

    /// A stripe at an explicit offset, surfacing a REFUSAL instead of panicking.
    /// The interesting answers on this path arrive as error frames rather than
    /// response codes, so the refusal text is what a caller asserts on.
    #[allow(clippy::too_many_arguments)]
    pub async fn write_shard_stripe(
        &self,
        extent_id: u64,
        shard_index: u32,
        sealed_length: u64,
        shard_len: u64,
        eversion: u64,
        attempt_nonce: u64,
        shard_offset: u64,
        payload: Vec<u8>,
    ) -> Result<WriteShardResp, String> {
        let req = WriteShardReq {
            extent_id,
            shard_index,
            sealed_length,
            shard_len,
            eversion,
            owner_epoch: 0,
            shard_offset,
            attempt_nonce,
            payload: Bytes::from(payload),
        };
        match self.pool.call_prepared(&self.addr, MSG_WRITE_SHARD, &req.prepared()).await {
            Ok(resp) => Ok(WriteShardResp::decode(resp).expect("decode WriteShardResp")),
            Err(e) => Err(e.to_string()),
        }
    }

}

/// What `read_bytes` returns: the reply code, the replica's end, the bytes.
pub struct ReadBytesResp {
    pub code: u8,
    pub end: u64,
    pub payload: Bytes,
}
