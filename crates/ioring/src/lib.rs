//! Shared-memory io_uring protocol for autumn-fuse (F180).
//!
//! # Why this crate exists
//!
//! `autumn-fuse` exposes data through a kernel-FUSE mount (`autumn-fuse
//! daemon` ↔ `/dev/fuse` ↔ user `read()`) for POSIX compatibility. The
//! kernel FUSE protocol caps single-request size at
//! `FUSE_MAX_PAGES_PER_REQ` ~128 KiB and serialises requests through
//! `fuser-0.15`'s single-threaded `/dev/fuse` reader. With those two
//! limits we measured 4 K read aggregate at ~31 k ops/s and 8 M read at
//! ~600 MB/s (F179) — far below the ~1.8 M ops/s and ~7 GB/s the
//! cluster delivers via direct `ClusterClient` calls (see
//! `perf_baseline_tcp_p16_d8_s4k.json`).
//!
//! F180 adds a **side-channel** parallel to the FUSE mount: applications
//! that opt in (typically AI training / inference processes) link this
//! crate (or its Python binding) and submit reads/writes through a
//! shared-memory ring straight to the daemon, bypassing kernel FUSE.
//! The mount stays — `ls`, `cp`, `cat`, monitoring agents continue to
//! work as before.
//!
//! Inspired by 3FS's `IoRing` (DeepSeek/3FS, `src/fuse/IoRing.h`,
//! `IoRing.cc`). The wire format is autumn-rs-specific; only the
//! architectural pattern (shared-mem ring, atomic SQE/CQE indices,
//! batched submit) is borrowed.
//!
//! # Crate scope (F180-A)
//!
//! This crate currently contains **only the wire protocol** — pure-data
//! `Sqe`/`Cqe`/`RingHeader` structs with codecs, opcode definitions,
//! and round-trip tests. Daemon-side polling and client-side submit are
//! deferred to F180-B and F180-C respectively.
//!
//! Concretely, after F180-A you can:
//!
//! - encode/decode an SQE or CQE round-trip (used by both daemon and
//!   client to read from / write to the shared ring);
//! - reason about the ring memory layout (`RING_MAGIC`,
//!   `RingHeaderLayout`, slot offset arithmetic);
//! - hash-check capability flags between client and daemon.
//!
//! You CANNOT yet:
//!
//! - open a ring (no SHM allocation, no `mmap` calls);
//! - submit an SQE (no producer-side ring writer);
//! - poll a CQE (no consumer-side ring reader).
//!
//! The atomic-index ring traversal logic will land in F180-B/C alongside
//! the daemon poller and client submit primitives.

// `ring.rs` uses `unsafe` to materialise `&AtomicU64` views over a
// shared byte slice (mmap-backed in production). All other modules
// remain pure-data; the unsafe surface is small and centralised.
#![allow(unsafe_code)]

pub mod opcode;
pub mod sqe;
pub mod cqe;
pub mod header;
pub mod buffer_pool;
pub mod ring;

pub use opcode::Opcode;
pub use sqe::{Sqe, SqeDecodeError};
pub use cqe::{Cqe, CqeDecodeError};
pub use header::{
    RingHeader, RingHeaderDecodeError, RING_MAGIC, RING_VERSION,
    DEFAULT_SQ_ENTRIES, DEFAULT_CQ_ENTRIES, DEFAULT_BUF_POOL_SIZE,
    DEFAULT_BUF_SLOT_SIZE,
};
pub use buffer_pool::{BufferPoolLayout, BufferPoolError};
pub use ring::{
    SqProducer, SqConsumer, CqProducer, CqConsumer, allocate_test_region,
};

/// All decode errors produced by this crate share this surface so a
/// daemon poll loop can match on a single error type.
#[derive(thiserror::Error, Debug)]
pub enum IoRingError {
    #[error("malformed SQE: {0}")]
    SqeDecode(#[from] SqeDecodeError),
    #[error("malformed CQE: {0}")]
    CqeDecode(#[from] CqeDecodeError),
    #[error("malformed ring header: {0}")]
    HeaderDecode(#[from] RingHeaderDecodeError),
    #[error("buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),
}
