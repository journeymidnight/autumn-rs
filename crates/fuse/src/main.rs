//! autumn-fuse: Mount autumn-rs KV store as a POSIX filesystem.
//!
//! Architecture:
//! - fuser threads handle FUSE callbacks and unbounded_send FsRequests on a futures mpsc channel
//! - A single compio thread owns ClusterClient and awaits rx.next() on the event loop
//! - A 30s timeout on rx.next drives the periodic dirty-inode flush without busy-polling

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use tracing_subscriber::EnvFilter;

use autumn_fuse::bridge::FuseBridge;
use autumn_fuse::dispatch;
use autumn_fuse::ops::AutumnFs;
use autumn_fuse::state::FsState;
use autumn_fuse::write;

#[derive(Parser)]
#[command(
    name = "autumn-fuse",
    about = "Mount autumn-rs KV store as a POSIX filesystem"
)]
struct Args {
    /// Manager address (host:port)
    #[arg(long, default_value = "127.0.0.1:9001")]
    manager: String,

    /// Mount point
    #[arg(long)]
    mountpoint: PathBuf,

    /// F-KEY-NS: tenant this mount's data lives under. Every KV key is scoped to
    /// `fs/{tenant}/`, so two tenants never share inodes/dirents/extents. Must
    /// match `[a-z0-9._-]+`.
    #[arg(long, default_value = "default")]
    tenant: String,

    /// Allow other users to access the mount
    #[arg(long, default_value = "false")]
    allow_other: bool,

    /// Transport backend: `tcp` (default) or `ucx`. Must match the cluster's
    /// transport (the ClusterClient talks to the PS over it).
    #[arg(long, default_value = "tcp")]
    transport: String,

    /// F-DIRECT-MANY — read whole extents (≥ 64 KiB) STRAIGHT from an extent
    /// node, bypassing the PS on the large-value data path (`get_many_direct`).
    /// A cross-host throughput win for large-file (model) serving: the PS NIC
    /// egress leaves the read path. Size-gated per read (< 64 KiB stays on the
    /// PS proxy), and SAFE even when ENs are unreachable — each large read
    /// falls back to the PS proxy (the client warns once). DEFAULT ON; disable
    /// with `--direct-read false` on a topology that keeps EN data ports on a
    /// PS-only subnet (fallback works but wastes one redirect RTT per extent).
    #[arg(long, default_value = "true")]
    direct_read: bool,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Select the transport process-wide before the compio thread connects.
    let tk = autumn_transport::parse_transport_flag(&args.transport).unwrap_or_else(|bad| {
        eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
        std::process::exit(2);
    });
    autumn_transport::init_with(tk);

    let mountpoint = args.mountpoint.clone();

    tracing::info!(
        manager = %args.manager,
        mountpoint = %mountpoint.display(),
        "starting autumn-fuse"
    );

    // Create the bridge channel
    let bridge = FuseBridge::new();
    let tx = bridge.tx.clone();
    let mut rx = bridge.rx;

    // F-fuse-lease-2: build the fuse Session UP FRONT so we can
    // grab a `Notifier` BEFORE the compio thread starts. The
    // notifier is `Clone + Send`; we ship one clone to the compio
    // thread (wrapped in the `InodeInvalidator` Rc-Fn closure) so
    // the invalidation poll loop can drop the kernel's attribute
    // + page cache on per-ino `WriterClosed` / `LeaseRevoked`
    // events. Without this, a reader app on host B continues to
    // serve from the kernel page cache after host A's writer
    // closes — close-to-open coherence breaks at the kernel
    // boundary even though the user-space lease state is correct.
    let mut options = vec![
        fuser::MountOption::FSName("autumn-fuse".to_string()),
        fuser::MountOption::DefaultPermissions,
        // max_read=8 MiB so a userspace pread(8 MiB) arrives as one FUSE read
        // instead of 64 × 128 KiB (kernel default). One large FUSE read fans
        // out across the file's variable-length extents (F247, ≤ 8 MiB each)
        // via `get_many_into` — each whole-extent get is ZC-eligible (≥ 64 KiB).
        fuser::MountOption::CUSTOM("max_read=8388608".to_string()),
    ];
    if args.allow_other {
        options.push(fuser::MountOption::AllowOther);
    }

    let fs = AutumnFs::new(tx);
    tracing::info!(mountpoint = %mountpoint.display(), "mounting filesystem");
    let mut session = fuser::Session::new(fs, &mountpoint, &options)?;
    let notifier = session.notifier();

    // Start the compio thread
    let manager_addr = args.manager.clone();
    let compio_handle = std::thread::Builder::new()
        .name("autumn-fuse-compio".to_string())
        .spawn(move || {
            // Move `notifier` into this thread; the
            // `InodeInvalidator` Rc-Fn closure constructed below
            // wraps it inside the compio runtime (which is
            // single-threaded so the Rc trait object never
            // crosses threads after this point).
            let notifier = notifier;
            compio::runtime::Runtime::new().unwrap().block_on(async {
                // Connect to cluster (scoped to `fs/{tenant}/`)
                let mut state = match FsState::new(&manager_addr, &args.tenant).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to connect to cluster");
                        return;
                    }
                };
                state.direct_read = args.direct_read;
                if args.direct_read {
                    tracing::info!("direct-read ON (default): ≥64 KiB reads go EN-direct, bypassing the PS (falls back to proxy per read if ENs unreachable)");
                } else {
                    tracing::info!("direct-read OFF (--direct-read false): all reads via PS proxy");
                }
                tracing::info!("connected to cluster");

                // Bug #1 fix (2026-06-06) — ride out the fresh-bootstrap
                // window where manager has assigned partitions but the
                // PS process hasn't yet bound each F099-K listener +
                // called `RegisterPartitionAddr`. Without this, fuse's
                // init_root kv_put hits 10× mis-route → `ps_call after
                // 10 refreshes: key not found`. 60 s budget is generous
                // enough for a hot-restart-on-loaded-cluster (a few
                // hundred MiB of WAL replay per partition); 250 ms poll
                // is cheap.
                if let Err(e) = state
                    .client
                    .wait_for_cluster_ready(
                        std::time::Duration::from_secs(60),
                        std::time::Duration::from_millis(250),
                    )
                    .await
                {
                    tracing::error!(error = %e, "cluster did not become ready in 60s");
                    return;
                }
                tracing::info!("cluster ready (all partition listeners reachable)");

                // F-fuse-lease-2: build the invalidator that the
                // poll loop calls per WriterClosed/LeaseRevoked
                // event. `inval_inode(ino, 0, 0)` drops both
                // attribute and the full data range — kernel
                // re-fetches via our dispatcher on the next read.
                //
                // BUG-LEASE-6 (P2 #7, 2026-06-06) — fail-closed
                // tracking. On `inval_inode` error, record the ino
                // in `state.notify_inval_failed` so the Open/Read
                // arms can force a fresh `get_inode` reload + retry
                // the kernel notify on the next syscall. On
                // success, REMOVE the entry — every Open-triggered
                // retry runs this closure too, so a successful
                // retry naturally clears the sticky flag.
                let notify_failed_h = state.notify_inval_failed.clone();
                let invalidator: dispatch::InodeInvalidator =
                    std::rc::Rc::new(move |ino: u64| {
                        match notifier.inval_inode(ino, 0, 0) {
                            Ok(()) => {
                                notify_failed_h.borrow_mut().remove(&ino);
                            }
                            Err(e) => {
                                notify_failed_h.borrow_mut().insert(ino);
                                tracing::warn!(
                                    ino,
                                    error = %e,
                                    "BUG-LEASE-6: notify_inval_inode failed; marked sticky for retry on next Open"
                                );
                            }
                        }
                    });

                // F-fuse-lease-1: spawn per-mount lease heartbeat +
                // invalidation poll loops. They share the compio
                // runtime and reference state.held_leases /
                // state.invalidations via Rc<RefCell<…>>.
                dispatch::spawn_lease_background_tasks(&state, Some(invalidator));

                let sync_interval = Duration::from_secs(30);
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
                            if !dispatch::handle_request(&mut state, req).await {
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

    // Run the fuse session loop on the main thread. Blocks until
    // unmount.
    session.run()?;

    tracing::info!("filesystem unmounted");

    // Wait for compio thread to finish
    let _ = compio_handle.join();

    Ok(())
}

async fn periodic_sync(state: &mut FsState) {
    let dirty: Vec<u64> = state.dirty_inodes.iter().copied().collect();
    if dirty.is_empty() {
        return;
    }
    tracing::debug!(count = dirty.len(), "periodic sync: flushing dirty inodes");
    for ino in &dirty {
        if let Err(e) = write::flush_inode(state, *ino).await {
            tracing::warn!(ino, error = %e, "periodic sync: flush failed");
        }
    }
}
