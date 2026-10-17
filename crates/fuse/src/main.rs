//! autumn-fuse: Mount autumn-rs KV store as a POSIX filesystem.
//!
//! Architecture:
//! - fuser threads handle FUSE callbacks and unbounded_send FsRequests on a futures mpsc channel
//! - A single compio thread owns ClusterClient and awaits rx.next() on the event loop
//! - A timeout on rx.next (`--dirty-sync-secs`) drives the periodic dirty-inode flush without busy-polling

use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use tracing_subscriber::EnvFilter;

use autumn_fuse::bridge::{FsRequest, FuseBridge};
use autumn_fuse::dispatch;
use autumn_fuse::ops::AutumnFs;
use autumn_fs::state::FsState;
use autumn_fs::write;
use autumn_params::{Positional, Section};

const USAGE: &str = "usage: autumn-fuse --mountpoint <DIR> [options]\nMount autumn-rs KV store as a POSIX filesystem.";

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let p = autumn_fuse::params();
    autumn_params::parse_or_exit(
        USAGE,
        &[p as &dyn Section, autumn_fs::params(), autumn_client::params()],
        Positional::Refuse,
    );
    let host = p.host.get().unwrap_or_else(autumn_fs::state::system_hostname);
    if !autumn_transport::set_regpool_cap_bytes(autumn_client::params().ucx_regpool_cap_bytes.get()) {
        tracing::warn!("regpool cap already set (ignored — first-call-wins)");
    }

    // Select the transport process-wide before the compio thread connects.
    autumn_transport::init_with(p.transport.get().into());

    let mountpoint = p.mountpoint.get();

    // read the authz credential up front (fail-loud on a
    // bad path) so the compio thread just carries (principal, bytes). The
    // principal identity travels IN the file (§8.5).
    let credential: Option<(String, Vec<u8>)> = match &p.credential_file.get() {
        Some(path) => {
            let (principal, secret) =
                autumn_client::read_credential_file(path).unwrap_or_else(|e| {
                    eprintln!("{e:#}");
                    std::process::exit(2);
                });
            if principal.is_empty() {
                eprintln!(
                    "--credential-file {}: missing principal name (expected '<principal>\\n<hex>')",
                    path.display()
                );
                std::process::exit(2);
            }
            Some((principal, secret))
        }
        None => None,
    };

    tracing::info!(
        manager = %p.manager.get(),
        mountpoint = %mountpoint.display(),
        authz = credential.is_some(),
        "starting autumn-fuse"
    );

    // Create the bridge channel
    // The pool's workers each connect for themselves, so they need their own copy.
    let pool_credential = credential.clone();

    let bridge = FuseBridge::new();
    let tx = bridge.tx.clone();
    let mut rx = bridge.rx;

    // Build the fuse Session UP FRONT so we can
    // grab a `Notifier` BEFORE the compio thread starts. The
    // notifier goes to the invalidation thread (`inval.rs`) so
    // the invalidation poll loop can drop the kernel's attribute
    // + page cache on per-ino `WriterClosed` / `LeaseRevoked`
    // events. Without this, a reader app on host B continues to
    // serve from the kernel page cache after host A's writer
    // closes — close-to-open coherence breaks at the kernel
    // boundary even though the user-space lease state is correct.
    let mut options = vec![
        fuser::MountOption::FSName("autumn-fuse".to_string()),
        fuser::MountOption::DefaultPermissions,
        // AutoUnmount is not optional, and its absence took down five
        // Kubernetes nodes.
        //
        // Without it the ONLY unmount path is `Mount::drop` on a graceful
        // return from main. SIGKILL (`kubectl delete pod --force
        // --grace-period=0`, an OOM kill, a node evicting us), SIGTERM with no
        // handler, and any abort all skip Drop — and the kernel mount survives
        // with NO server behind it. From then on, every `stat()` that crosses
        // that path blocks in uninterruptible sleep forever, because nothing is
        // left to answer the kernel's FUSE request. A container runtime walks
        // and stats mount points on every sandbox create and teardown, so the
        // first of its threads to touch the corpse wedges, and the node stops
        // being able to start ANY container while every already-running process
        // keeps working — which is exactly what we saw, on five nodes, one at a
        // time, over two hours, fixable only by rebooting.
        //
        // With AutoUnmount, fusermount3 holds the mount and drops it as soon as
        // our /dev/fuse fd closes, for ANY reason including SIGKILL. fuser adds
        // AllowOther implicitly here (it needs allow_root or allow_other for
        // fusermount's auto_unmount) and enforces the ACL in userspace itself —
        // see fuser 0.15 session.rs. Both the daemon and its consumer run as
        // root in our images, so that costs us nothing.
        fuser::MountOption::AutoUnmount,
        // max_read=8 MiB so a userspace pread(8 MiB) arrives as one FUSE read
        // instead of 64 × 128 KiB (kernel default). One large FUSE read fans
        // out across the file's variable-length extents (≤ 8 MiB each)
        // via `get_many_into` — each whole-extent get is bulk-eligible (≥ 64 KiB).
        fuser::MountOption::CUSTOM("max_read=8388608".to_string()),
    ];
    if p.allow_other.get() {
        options.push(fuser::MountOption::AllowOther);
    }

    let shutdown_tx = tx.clone();
    let fs = AutumnFs::new(tx);
    tracing::info!(mountpoint = %mountpoint.display(), "mounting filesystem");
    let mut session = fuser::Session::new(fs, &mountpoint, &options)?;
    let notifier = session.notifier();
    // Before any thread exists: every thread inherits the mask, so only
    // `shutdown_on_signal`'s `sigwait` ever takes SIGTERM / SIGINT. (The mount
    // creates none; its fusermount3 helper blocks every signal on its own.)
    let term_signals = block_term_signals().context("block SIGTERM/SIGINT")?;
    // Never on the compio thread: the notify blocks on pages whose reads
    // only that thread can answer (see `inval.rs`).
    let (inval_tx, inval_results, inval_gate) =
        autumn_fuse::inval::spawn(move |ino| notifier.inval_inode(ino, 0, 0))
            .context("spawn invalidation thread")?;
    // Never sent on: the sender drops when the compio thread ends, however it ends.
    let (compio_done_tx, compio_done_rx) = std::sync::mpsc::channel::<()>();

    // Start the compio thread
    let manager_addr = p.manager.get();
    let compio_handle = std::thread::Builder::new()
        .name("autumn-fuse-compio".to_string())
        .spawn(move || {
            let _compio_done = compio_done_tx;
            compio::runtime::Runtime::new().unwrap().block_on(async {
                // Connect to cluster (scoped to `fs/`); with an authz
                // credential when `--credential-file` was given.
                let connect = async {
                    match credential {
                        Some((principal, secret)) => {
                            FsState::new_with_host_credential(
                                &manager_addr,
                                host,
                                &principal,
                                secret,
                            )
                            .await
                        }
                        None => FsState::new_with_host(&manager_addr, host).await,
                    }
                };
                let mut state = match connect.await {
                    Ok(s) => s,
                    Err(e) => {
                        // `{:#}` (anyhow's alternate Display) walks the cause
                        // chain. Plain `%e` prints only the outermost context,
                        // which turns every connect failure into the same
                        // uninformative line — a wire-version mismatch, a
                        // rejected credential and an unreachable manager all
                        // looked identical, and one of them cost a long
                        // misdirected authz investigation.
                        tracing::error!(error = %format_args!("{e:#}"), "failed to connect to cluster");
                        return;
                    }
                };
                state.direct_read = p.direct_read.get();
                // Only the mount: every non-write request goes through
                // `dispatch::handle_request`, which drains an in-flight flush
                // before the handler runs. Nothing else has that funnel.
                state.pipelined_writes = true;
                if p.direct_read.get() {
                    tracing::info!(
                        "direct-read ON (default): ≥64 KiB reads go EN-direct, bypassing the PS \
                         (falls back to proxy per read if ENs unreachable). EC-converted extents \
                         are read the same way, by fetching the data shards the range covers \
                         straight from their nodes; a shard whose node will not answer needs \
                         reconstruction, which only the PS can do, so those reads fall back."
                    );
                } else {
                    tracing::info!("direct-read OFF (--direct-read false): all reads via PS proxy");
                }
                tracing::info!("connected to cluster");

                // Bug #1 fix (2026-06-06) — ride out the fresh-bootstrap
                // window where manager has assigned partitions but the
                // PS process hasn't yet bound each partition's listener +
                // called `RegisterPartitionAddr`. Without this, fuse's
                // init_root kv_put hits 10× mis-route → `ps_call after
                // 10 refreshes: key not found`. The 60 s default budget is generous
                // enough for a hot-restart-on-loaded-cluster (a few
                // hundred MiB of WAL replay per partition); 250 ms poll
                // is cheap.
                let ready_timeout_secs = autumn_fuse::params().cluster_ready_timeout_secs.get();
                if let Err(e) = state
                    .client
                    .wait_for_cluster_ready(
                        std::time::Duration::from_secs(ready_timeout_secs),
                        std::time::Duration::from_millis(250),
                    )
                    .await
                {
                    tracing::error!(
                        error = %format_args!("{e:#}"),
                        "cluster did not become ready in {ready_timeout_secs}s"
                    );
                    return;
                }
                tracing::info!("cluster ready (all partition listeners reachable)");

                // After readiness, so the workers' own `connect` sees the same
                // regions the dispatcher just waited for.
                let fuse_params = autumn_fuse::params();
                let read_pool = autumn_fuse::read_pool::ReadPool::new(
                    fuse_params.read_io_threads.get(),
                    &manager_addr,
                    pool_credential,
                    fuse_params.prefetch_mem_mb.get() << 20,
                )
                .await;
                let read_pool = (!read_pool.is_empty()).then_some(read_pool);

                // The invalidator the poll loop calls per
                // WriterClosed/LeaseRevoked event queues the ino for the
                // invalidation thread, whose `inval_inode(ino, 0, 0)` drops
                // both attribute and the full data range — kernel re-fetches
                // via our dispatcher on the next read. Its outcomes land in
                // `state.notify_inval_failed` (BUG-LEASE-6, fail-closed): a
                // failed ino is reloaded and re-notified on its next Open.
                let invalidator = autumn_fuse::inval::invalidator(inval_tx);
                compio::runtime::spawn(autumn_fuse::inval::record_results(
                    inval_results,
                    state.notify_inval_failed.clone(),
                ))
                .detach();

                // Spawn per-mount lease heartbeat +
                // invalidation poll loops. They share the compio
                // runtime and reference state.held_leases /
                // state.invalidations via Rc<RefCell<…>>.
                dispatch::spawn_lease_background_tasks(&state, Some(invalidator));

                let sync_interval =
                    Duration::from_secs(autumn_fuse::params().dirty_sync_secs.get());
                let mut last_sync = std::time::Instant::now();

                loop {
                    let remaining = (last_sync + sync_interval)
                        .saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        periodic_sync(&mut state).await;
                        last_sync = std::time::Instant::now();
                        continue;
                    }

                    match compio::time::timeout(remaining, rx.next()).await {
                        Ok(Some(req)) => {
                            if !dispatch::handle_request(&mut state, req, read_pool.as_ref())
                                .await
                            {
                                tracing::info!("received Destroy, shutting down");
                                break;
                            }
                        }
                        Ok(None) => {
                            tracing::info!("bridge channel closed, shutting down");
                            break;
                        }
                        Err(_) => {
                            periodic_sync(&mut state).await;
                            last_sync = std::time::Instant::now();
                        }
                    }
                }
            });
        })
        .context("spawn compio thread")?;

    let unmounter = session.unmount_callable();
    std::thread::Builder::new()
        .name("autumn-fuse-signal".to_string())
        .spawn(move || {
            shutdown_on_signal(term_signals, inval_gate, shutdown_tx, compio_done_rx, unmounter)
        })
        .context("spawn signal thread")?;

    // Run the fuse session loop on the main thread. Blocks until
    // unmount.
    let served = session.run();

    tracing::info!("filesystem unmounted");

    // The dispatch loop stops on Destroy or once every sender is gone, and
    // three senders outlive this point: `bridge.tx`, the session's, and the
    // signal thread's (parked in `sigwait`). The kernel sends no FUSE_DESTROY
    // for a plain `umount`, so without this the join below never returned
    // and the daemon stayed up, syncing every 30 s, with nothing mounted.
    // Sent before `served` is judged, so a session error still flushes.
    if bridge.tx.unbounded_send(FsRequest::Destroy).is_err() {
        tracing::info!("dispatcher already stopped");
    }
    // Wait for compio thread to finish
    let _ = compio_handle.join();
    served?;

    Ok(())
}

/// SIGTERM (what Kubernetes sends first) and SIGINT, blocked in the calling
/// thread and so in every thread spawned after it.
fn block_term_signals() -> std::io::Result<libc::sigset_t> {
    // SAFETY: plain libc calls on a local sigset_t.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::sigaddset(&mut set, libc::SIGINT);
        match libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) {
            0 => Ok(set),
            rc => Err(std::io::Error::from_raw_os_error(rc)),
        }
    }
}

/// Exit on SIGTERM / SIGINT without leaving a zombie behind the mount.
///
/// The default action kills every thread at once, and a thread that is inside
/// `inval_inode` waiting on a readahead page cannot die: it waits on a read only
/// the dispatcher could answer, and the `/dev/fuse` fd whose release would
/// abort the connection outlives it (see `inval.rs`). So the order is:
///   1. close the invalidation gate — waits out the notify in flight while the
///      session loop and the dispatcher still answer its read;
///   2. Destroy the dispatcher, which flushes dirty inodes, and wait for it;
///   3. unmount (lazy), and exit. Exit closes `/dev/fuse`, which aborts the
///      connection for any process still holding a file open on it.
fn shutdown_on_signal(
    signals: libc::sigset_t,
    inval_gate: autumn_fuse::inval::InvalGate,
    shutdown_tx: futures::channel::mpsc::UnboundedSender<FsRequest>,
    compio_done: std::sync::mpsc::Receiver<()>,
    mut unmounter: fuser::SessionUnmounter,
) {
    let mut signal: libc::c_int = 0;
    // SAFETY: `signals` is a valid, initialised set; `signal` is a valid out-pointer.
    let rc = unsafe { libc::sigwait(&signals, &mut signal) };
    if rc != 0 {
        tracing::error!(error = %std::io::Error::from_raw_os_error(rc), "sigwait failed; SIGTERM will not unmount");
        return;
    }
    tracing::info!(signal, "shutting down: draining kernel invalidations");
    inval_gate.close();
    tracing::info!("flushing dirty inodes");
    if shutdown_tx.unbounded_send(FsRequest::Destroy).is_err() {
        tracing::warn!("dispatcher already gone; nothing to flush");
    }
    compio_done.recv().unwrap_err();
    tracing::info!("unmounting");
    if let Err(e) = unmounter.unmount() {
        tracing::error!(error = %e, "unmount failed");
    }
    std::process::exit(0);
}

async fn periodic_sync(state: &mut FsState) {
    // Unreachable inodes whose data waited for another client to let go.
    // Cheap when there are none: one empty range scan.
    if let Err(e) = autumn_fs::extent::sweep_unlink_tombstones(state).await {
        tracing::warn!(error = %e, "periodic sync: tombstone sweep failed");
    }
    if let Err(e) = autumn_fs::segment::sweep_garbage(state).await {
        tracing::warn!(error = %e, "periodic sync: segment garbage sweep failed");
    }
    let dirty: Vec<u64> = state.dirty_inodes.iter().copied().collect();
    if dirty.is_empty() {
        return;
    }
    tracing::debug!(count = dirty.len(), "periodic sync: flushing dirty inodes");
    for ino in &dirty {
        // Best-effort: this only logs. Consuming the sticky record here would
        // let the application's next fsync answer success over a hole.
        if let Err(e) = write::flush_inode(state, *ino, write::FlushReport::BestEffort).await {
            tracing::warn!(ino, error = %e, "periodic sync: flush failed");
        }
    }
}
