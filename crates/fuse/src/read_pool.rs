//! Read I/O thread pool: N OS threads, each with its OWN compio runtime and its
//! OWN `ClusterClient`, executing prepared read plans off the dispatcher thread.
//!
//! # Why a pool of runtimes and not a multi-threaded one
//!
//! compio has no work-stealing scheduler — a runtime is one thread, and every
//! handle it hands out is `!Send`. So "multi-threaded" here means what it means
//! everywhere else in this repo (the partition server's P-log / P-sst pair): a
//! set of independent single-threaded runtimes, each owning its own connections,
//! fed by a channel that carries only `Send` work.
//!
//! # Why the read path can be split this way at all
//!
//! `read::prepare` is the half that needs `&mut FsState` — the inode cache, the
//! write-buffer flush, the extent map. `read::execute` needs none of it: it owns
//! a plan of `(key, sub-range, dest offset)` and a client. That split already
//! existed for the spawned reply; all this adds is that the spawn lands on
//! ANOTHER thread. State stays single-threaded and `prepare` stays the only
//! reader of it, so nothing about lease coherence or the extent map changes.
//!
//! The measurement that motivated it: with eight concurrent readers the mount
//! plateaued near 2600 MiB/s and then FELL to ~2070 at eight streams, while
//! eight `autumnfs` CLI processes reading the same files over the same loopback
//! TCP reached 5466. The daemon's compio thread was pinned at ~100% of a core
//! (per-thread CPU time: 394 jiffies against 3 for the kernel-channel reader),
//! which is the whole shape — one stream is latency-bound below saturation, four
//! approaches it, eight is past it and pays contention. Every byte crosses that
//! one thread about three times (network recv into a pooled buffer, memcpy into
//! the result, then the reply write into `/dev/fuse`).
//!
//! # What crosses the thread boundary
//!
//! `ReadJob` is `Send` without an unsafe impl: `ChunkSpec` is owned data, and
//! `fuser::ReplyData` is `Send` because `ReplySender` carries `Send + Sync` as a
//! supertrait. The client does NOT cross — each worker connects for itself, so a
//! job carries no `Rc`.
//!
//! # Cost
//!
//! N clients means N connection pools per mount (the partition server has the
//! same property with its two per-partition stream clients). It also means N
//! manager connections at mount time.

use std::cell::Cell;
use std::rc::Rc;

use futures::channel::{mpsc, oneshot};
use futures::StreamExt;

use autumn_client::ClusterClient;

use crate::bridge::REPLY_TIMEOUT;
use crate::read::{self, ChunkSpec, ReadPlan};

/// How long to wait for a worker to connect before mounting without it. The
/// mount is already live at that point — see the note in `ReadPool::new`.
const READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// One prepared read, handed to a worker thread. Carries the kernel reply
/// handle: the worker answers `/dev/fuse` itself, exactly as the dispatcher's
/// own spawned task used to.
pub struct ReadJob {
    pub chunks: Vec<ChunkSpec>,
    pub actual_size: usize,
    pub direct_read: bool,
    pub reply: fuser::ReplyData,
}

/// Handle to the worker set. Lives on the dispatcher thread (hence the plain
/// `Cell` for round-robin — never shared).
pub struct ReadPool {
    txs: Vec<mpsc::UnboundedSender<ReadJob>>,
    next: Cell<usize>,
}

impl ReadPool {
    /// Spawn `threads` workers and wait for each to connect. A worker that fails
    /// to connect is left OUT of the pool rather than failing the mount: reads
    /// still work through the dispatcher, just without the extra thread, and a
    /// mount that refuses to come up because it could not open a second
    /// connection would be a worse trade than a slower one.
    ///
    /// `threads == 0` yields an empty pool, which `submit` reports as "not
    /// taken" — the revert path, and what every non-mount front-end gets.
    pub async fn new(
        threads: usize,
        manager_addr: &str,
        credential: Option<(String, Vec<u8>)>,
    ) -> Self {
        let mut txs = Vec::with_capacity(threads);
        let mut readies = Vec::with_capacity(threads);

        for idx in 0..threads {
            let (tx, rx) = mpsc::unbounded::<ReadJob>();
            let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();
            let addr = manager_addr.to_string();
            let cred = credential.clone();
            match std::thread::Builder::new()
                .name(format!("autumn-fuse-rd{idx}"))
                .spawn(move || worker_main(idx, addr, cred, rx, ready_tx))
            {
                Ok(_handle) => {
                    // Deliberately not joined. The workers exit on their own
                    // when their receivers close (pool drop), and joining from
                    // the dispatcher during unmount would block it behind an
                    // in-flight cluster read.
                    txs.push(tx);
                    readies.push((idx, ready_rx));
                }
                Err(e) => {
                    tracing::error!(idx, error = %e, "read pool: thread spawn failed");
                }
            }
        }

        // Keep only the workers that reported a live client.
        //
        // BOUNDED, because the fuse session is already mounted by the time this
        // runs: FUSE requests are queueing in the bridge while we wait, so a
        // worker whose `connect` stalls stalls every syscall on the mount. A raw
        // TCP connect to a blackholed manager is bounded only by SYN retry
        // (minutes), which is far too long to hold a mount for an optimisation.
        // Giving up on a worker costs throughput, never correctness — the
        // dispatcher serves reads no worker takes.
        let mut live = Vec::with_capacity(txs.len());
        for (slot, (idx, ready_rx)) in readies.into_iter().enumerate() {
            match compio::time::timeout(READY_TIMEOUT, ready_rx).await {
                Ok(Ok(Ok(()))) => live.push(txs[slot].clone()),
                Ok(Ok(Err(e))) => {
                    tracing::error!(idx, error = %e, "read pool: worker failed to connect")
                }
                Ok(Err(_)) => {
                    tracing::error!(idx, "read pool: worker died before reporting ready")
                }
                Err(_) => tracing::error!(
                    idx,
                    timeout_secs = READY_TIMEOUT.as_secs(),
                    "read pool: worker did not connect in time; serving its share on the dispatcher"
                ),
            }
        }

        if threads > 0 {
            tracing::info!(
                threads = live.len(),
                requested = threads,
                "read pool ready (each worker owns its own compio runtime and cluster client)"
            );
        }
        Self {
            txs: live,
            next: Cell::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.txs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    /// Hand a job to the next worker, round-robin. Returns the job back when no
    /// worker took it, so the caller can run it on its own runtime.
    ///
    /// Round-robin rather than hashing by inode: nothing in the read path needs
    /// a file's reads to stay on one thread (each read answers its own kernel
    /// request and `prepare` already resolved everything state-dependent), and
    /// hashing would put every reader of a single large file — the model-load
    /// shape — back on one thread.
    ///
    /// A worker whose thread has died shows up as a closed sender; skip it and
    /// try the next, so one dead thread degrades throughput instead of failing
    /// every N-th read.
    pub fn submit(&self, job: ReadJob) -> Result<(), ReadJob> {
        submit_round_robin(&self.txs, &self.next, job)
    }
}

/// The routing itself, split out from `submit` so it can be tested without a
/// `fuser::ReplyData` (whose `ReplySender` bound fuser does not export, so one
/// cannot be built outside a live session).
///
/// Returning the item on failure is the load-bearing part. Dropping a job is
/// not silent — `fuser::ReplyRaw::Drop` answers EIO for a reply that was never
/// sent — but EIO on a read the cluster could have served is still a failure the
/// caller sees, and relying on that Drop would put correctness in a detail of
/// the fuser version. Hand the job back and run it locally instead.
fn submit_round_robin<T>(
    txs: &[mpsc::UnboundedSender<T>],
    next: &Cell<usize>,
    item: T,
) -> Result<(), T> {
    let n = txs.len();
    if n == 0 {
        return Err(item);
    }
    let mut item = item;
    for _ in 0..n {
        let i = next.get() % n;
        next.set(i + 1);
        match txs[i].unbounded_send(item) {
            Ok(()) => return Ok(()),
            Err(e) => item = e.into_inner(),
        }
    }
    Err(item)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_pool_hands_the_job_back() {
        let next = Cell::new(0);
        let txs: Vec<mpsc::UnboundedSender<u32>> = Vec::new();
        assert_eq!(submit_round_robin(&txs, &next, 7), Err(7));
    }

    #[test]
    fn jobs_spread_round_robin() {
        let (t0, mut r0) = mpsc::unbounded::<u32>();
        let (t1, mut r1) = mpsc::unbounded::<u32>();
        let txs = vec![t0, t1];
        let next = Cell::new(0);
        for v in 0..4u32 {
            assert!(submit_round_robin(&txs, &next, v).is_ok());
        }
        assert_eq!(r0.try_next().unwrap(), Some(0));
        assert_eq!(r0.try_next().unwrap(), Some(2));
        assert_eq!(r1.try_next().unwrap(), Some(1));
        assert_eq!(r1.try_next().unwrap(), Some(3));
    }

    #[test]
    fn a_dead_worker_is_skipped_not_hung_on() {
        // Worker 0's thread died: its receiver is gone. Every job must still
        // land on worker 1 — not vanish into the closed channel every other
        // turn, which would leave those FUSE reads unanswered forever.
        let (t0, r0) = mpsc::unbounded::<u32>();
        let (t1, mut r1) = mpsc::unbounded::<u32>();
        drop(r0);
        let txs = vec![t0, t1];
        let next = Cell::new(0);
        for v in 0..3u32 {
            assert!(submit_round_robin(&txs, &next, v).is_ok());
        }
        assert_eq!(r1.try_next().unwrap(), Some(0));
        assert_eq!(r1.try_next().unwrap(), Some(1));
        assert_eq!(r1.try_next().unwrap(), Some(2));
    }

    #[test]
    fn all_workers_dead_returns_the_job_for_local_execution() {
        let (t0, r0) = mpsc::unbounded::<u32>();
        let (t1, r1) = mpsc::unbounded::<u32>();
        drop(r0);
        drop(r1);
        let txs = vec![t0, t1];
        let next = Cell::new(0);
        assert_eq!(submit_round_robin(&txs, &next, 9), Err(9));
    }
}

fn worker_main(
    idx: usize,
    manager_addr: String,
    credential: Option<(String, Vec<u8>)>,
    mut rx: mpsc::UnboundedReceiver<ReadJob>,
    ready_tx: oneshot::Sender<Result<(), String>>,
) {
    let rt = match compio::runtime::RuntimeBuilder::new().build() {
        Ok(rt) => rt,
        Err(e) => {
            let _ = ready_tx.send(Err(format!("runtime init: {e}")));
            return;
        }
    };
    rt.block_on(async move {
        // Same scope as the dispatcher's own client: the whole `fs/` namespace,
        // so the relative keys a plan carries resolve identically.
        let connected = match credential {
            Some((principal, secret)) => {
                ClusterClient::connect_with_credential(&manager_addr, "fs", principal, secret).await
            }
            None => ClusterClient::connect(&manager_addr, "fs").await,
        };
        let client = match connected {
            Ok(c) => Rc::new(c),
            Err(e) => {
                let _ = ready_tx.send(Err(format!("{e:#}")));
                return;
            }
        };
        if ready_tx.send(Ok(())).is_err() {
            // The pool gave up on us before we finished connecting.
            return;
        }

        while let Some(job) = rx.next().await {
            let client = client.clone();
            // SPAWN, never await inline: a worker that ran one job at a time
            // would cap concurrency at the thread count, which is far below what
            // a single runtime already delivers. The point of the pool is more
            // CPU for the same concurrency, not less concurrency.
            compio::runtime::spawn(async move {
                let plan = ReadPlan {
                    inline_result: None,
                    actual_size: job.actual_size,
                    client,
                    chunks: job.chunks,
                    direct_read: job.direct_read,
                };
                // BOUNDED, for the same reason the dispatcher's version is: this
                // is the one FUSE op answered off the request loop, so it is the
                // one that can leave the kernel waiting forever.
                match compio::time::timeout(REPLY_TIMEOUT, read::execute(plan)).await {
                    Ok(Ok(data)) => job.reply.data(&data),
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "fuse read execute failed");
                        job.reply.error(libc::EIO);
                    }
                    Err(_) => {
                        tracing::warn!(
                            timeout_secs = REPLY_TIMEOUT.as_secs(),
                            "fuse read timed out — replying EIO"
                        );
                        job.reply.error(libc::EIO);
                    }
                }
            })
            .detach();
        }
        tracing::info!(idx, "read pool worker exiting (channel closed)");
    });
}
