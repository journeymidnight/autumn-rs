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

    /// path to a file holding this mount's authz credential
    /// (`<principal>\n<hex>`, from `autumn-op principal-create`). REQUIRED when the
    /// cluster protects the `fs/` namespace; omit on an authz-off cluster. The
    /// mount connects via `connect_with_credential` (principal read from the file)
    /// and FAILS FAST if the credential doesn't cover `fs/`. (The tenant segment
    /// is gone — a mount covers the WHOLE `fs/` namespace.)
    #[arg(long)]
    credential_file: Option<PathBuf>,

    /// Allow other users to access the mount
    #[arg(long, default_value = "false")]
    allow_other: bool,

    /// Transport backend: `tcp` (default) or `ucx`. Must match the cluster's
    /// transport (the ClusterClient talks to the PS over it).
    #[arg(long, default_value = "tcp")]
    transport: String,

    /// read whole extents (≥ 64 KiB) STRAIGHT from an extent
    /// node, bypassing the PS on the large-value data path (`get_many_direct`).
    /// A cross-host throughput win for large-file (model) serving: the PS NIC
    /// egress leaves the read path. Size-gated per read (< 64 KiB stays on the
    /// PS proxy), and SAFE even when ENs are unreachable — each large read
    /// falls back to the PS proxy (the client warns once). DEFAULT ON; disable
    /// with `--direct-read false` on a topology that keeps EN data ports on a
    /// PS-only subnet (fallback works but wastes one redirect RTT per extent).
    // `action = Set` is what makes `--direct-read false` actually parse. With
    // clap 4's derive, a plain `bool` field becomes a valueless SetTrue flag and
    // `default_value = "true"` is inert — so the form this flag's own help text
    // and startup log both tell operators to use was rejected with "unexpected
    // argument 'false'", and there was NO way to turn direct reads off. That
    // matters more than a usability wart: the size-gated EN-direct path is the
    // one an operator needs to disable when it misbehaves, and the escape hatch
    // was documented but absent.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
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

    // read the authz credential up front (fail-loud on a
    // bad path) so the compio thread just carries (principal, bytes). The
    // principal identity travels IN the file (§8.5).
    let credential: Option<(String, Vec<u8>)> = match &args.credential_file {
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
        manager = %args.manager,
        mountpoint = %mountpoint.display(),
        authz = credential.is_some(),
        "starting autumn-fuse"
    );

    // Create the bridge channel
    let bridge = FuseBridge::new();
    let tx = bridge.tx.clone();
    let mut rx = bridge.rx;

    // Build the fuse Session UP FRONT so we can
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
                // Connect to cluster (scoped to `fs/{tenant}/`); with an authz
                // credential when `--credential-file` was given.
                let connect = async {
                    match credential {
                        Some((principal, secret)) => {
                            FsState::new_with_credential(
                                &manager_addr,
                                &principal,
                                secret,
                            )
                            .await
                        }
                        None => FsState::new(&manager_addr).await,
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
                state.direct_read = args.direct_read;
                if args.direct_read {
                    tracing::info!(
                        "direct-read ON (default): ≥64 KiB reads go EN-direct, bypassing the PS \
                         (falls back to proxy per read if ENs unreachable). NOTE: an extent that \
                         has been converted to EC is always served through the PS instead — it \
                         holds RS shards, not the value — so on a cluster with EC armed this \
                         applies to fewer and fewer reads as extents convert. Bootstrap arms EC from \
                         four extent nodes up (3+1 at four, 4+1 at five or more), so that is the \
                         common case, not a corner one."
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
                    tracing::error!(error = %format_args!("{e:#}"), "cluster did not become ready in 60s");
                    return;
                }
                tracing::info!("cluster ready (all partition listeners reachable)");

                // Build the invalidator that the
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

                // Spawn per-mount lease heartbeat +
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
