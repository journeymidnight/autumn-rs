//! Per-session lease background tasks — heartbeat + invalidation poll.
//!
//! F-FS-UNIFY M4: extracted from the fuse-gated `dispatch.rs` into the
//! `fuser`-free core so BOTH front-ends share one implementation: the
//! `autumn-fuse` mount (passing a real kernel `InodeInvalidator`) and the PyO3
//! `autumn.Fs` binding (passing `None` — headless, no kernel page cache to
//! evict). The tasks operate purely on the `Rc<RefCell<…>>` fields of
//! `FsState` (`held_leases` / `invalidations` / `client` / `client_id`), never
//! on `&mut FsState`, so they run concurrently with the request/dispatch loop
//! on the single-threaded compio runtime (brief, non-await-crossing borrows).
//!
//! `dispatch.rs` re-exports these (`pub use`) so its existing unit tests +
//! `main.rs` references resolve unchanged.

use crate::state::FsState;

/// F-fuse-lease-2: callback the per-session invalidation poll loop runs for
/// every per-ino `WriterClosed` / `LeaseRevoked` event. In the fuse mount this
/// is `notifier.inval_inode(ino, 0, 0)` against the live `fuser::Session`'s
/// `Notifier`, dropping the kernel's attribute + page cache for the ino so the
/// next syscall reaches the dispatcher.
///
/// Boxed as a trait object on `Rc` because the compio runtime is
/// single-threaded — `Rc` is enough; no `Send` needed — and the callback is
/// invoked many times per event batch (so we don't move-out).
///
/// `None` in the PyO3 binding + tests skips kernel-side eviction (the
/// user-space lease bookkeeping still updates correctly).
pub type InodeInvalidator = std::rc::Rc<dyn Fn(u64)>;

/// R2-P0 #3 — mark every held lease that just got a `LEASE_REVOKED` event as
/// revoked. Returns the newly-revoked inos (the caller best-effort releases
/// them). A `revoked` entry is KEPT in the map so `check_write_allowed` can
/// fast-fail a stale writer's next op (the client-side half of the fence).
pub fn evict_revoked_held_leases(
    events: &[autumn_rpc::manager_rpc::MgrInvalidation],
    held_leases: &mut std::collections::HashMap<u64, crate::state::FuseLease>,
) -> Vec<u64> {
    let mut newly_revoked: Vec<u64> = Vec::new();
    for ev in events {
        if ev.kind == autumn_rpc::manager_rpc::LEASE_INVAL_LEASE_REVOKED && ev.ino != 0 {
            if let Some(slot) = held_leases.get_mut(&ev.ino) {
                if !slot.revoked {
                    slot.revoked = true;
                    newly_revoked.push(ev.ino);
                }
            }
        }
    }
    newly_revoked
}

/// F-fuse-lease-2 — pure-fn extracted from the invalidation poll loop so the
/// "per-event ⇒ per-ino invalidator call" contract can be unit-tested without
/// booting a real cluster. For every event in the batch, runs the kernel-cache
/// eviction callback iff the event has a non-zero ino. `ino == 0` is the
/// manager-side overflow sentinel and is filtered here — the wholesale-clear
/// branch in `spawn_lease_background_tasks` handles it separately.
pub fn invalidate_kernel_cache_for_events(
    events: &[autumn_rpc::manager_rpc::MgrInvalidation],
    invalidator: &InodeInvalidator,
) {
    for ev in events {
        if ev.ino != 0 {
            invalidator(ev.ino);
        }
    }
}

/// F-fuse-lease-1 + F-fuse-lease-2: per-session background tasks. Spawn ONCE on
/// the compio runtime right after `FsState::new` (before the dispatch / job
/// loop). Two tasks: a lease heartbeat loop + an invalidation poll loop:
///
/// - heartbeat: TTL/6 = 5s tick; renews every held lease;
///   `HeartbeatResult::NotHeld` drops the entry (open fds surface EIO on their
///   next op — explicit failure beats silently serving stale state).
/// - invalidation poll: persistent long-poll; per-event update
///   `invalidations[ino]` via `apply_invalidation`; per-ino call
///   `invalidator(ino)` (when supplied) so the kernel's attribute + page cache
///   is dropped — without this, a reader may keep serving from the page cache
///   after a writer closes, breaking close-to-open coherence at the kernel
///   boundary even though the user-space lease state is correct. Overflow
///   sentinel or transport error → wholesale invalidate + best-effort
///   `ReleaseLease`.
pub fn spawn_lease_background_tasks(state: &FsState, invalidator: Option<InodeInvalidator>) {
    use std::time::Duration;
    // BUG-LEASE-6 (P2 #7) — stash the invalidator on FsState so the Open arm
    // can retry the kernel notify when `notify_inval_failed` contains the ino.
    // Stored only when the fuse mount supplied a real Notifier-backed closure;
    // the binding + tests pass None, in which case the Open arm short-circuits
    // to "drop the cached InodeState" (no kernel retry to attempt).
    if let Some(ref inv) = invalidator {
        *state.kernel_invalidator.borrow_mut() = Some(inv.clone());
    }
    let cluster_h = state.client.clone();
    let id_h = state.client_id.clone();
    let held_h = state.held_leases.clone();
    compio::runtime::spawn(async move {
        loop {
            compio::time::sleep(Duration::from_secs(5)).await;
            let inos: Vec<u64> = held_h.borrow().keys().copied().collect();
            if inos.is_empty() {
                continue;
            }
            for ino in inos {
                match autumn_client::lease::heartbeat(&cluster_h, &id_h, ino).await {
                    Ok(autumn_client::lease::HeartbeatResult::Renewed(info)) => {
                        if let Some(slot) = held_h.borrow_mut().get_mut(&ino) {
                            slot.lease_epoch = info.version;
                        }
                    }
                    Ok(autumn_client::lease::HeartbeatResult::NotHeld) => {
                        tracing::warn!(
                            ino,
                            "F-fuse-lease-1: heartbeat NotHeld; dropping local lease entry"
                        );
                        held_h.borrow_mut().remove(&ino);
                    }
                    Err(e) => {
                        tracing::warn!(ino, error = %e, "F-fuse-lease-1: heartbeat transient");
                    }
                }
            }
        }
    })
    .detach();

    let cluster_p = state.client.clone();
    let id_p = state.client_id.clone();
    let held_p = state.held_leases.clone();
    let inv_p = state.invalidations.clone();
    let invalidator_p = invalidator;
    compio::runtime::spawn(async move {
        loop {
            match autumn_client::lease::poll_invalidations(&cluster_p, &id_p).await {
                Ok(events) => {
                    let wholesale = {
                        let mut inv = inv_p.borrow_mut();
                        autumn_client::lease::apply_invalidation(&events, &mut inv)
                    };
                    for ev in &events {
                        tracing::info!(
                            ino = ev.ino,
                            version = ev.version,
                            kind = ev.kind,
                            "F-fuse-lease-2: invalidation"
                        );
                    }
                    // Drop the kernel's attribute + page cache for each non-zero
                    // ino (ino=0 is the overflow sentinel, handled below).
                    if let Some(inv) = &invalidator_p {
                        invalidate_kernel_cache_for_events(&events, inv);
                    }
                    // BUG-LEASE-3: per-event immediate eviction on LeaseRevoked.
                    let evicted_inos = {
                        let mut held_mut = held_p.borrow_mut();
                        evict_revoked_held_leases(&events, &mut held_mut)
                    };
                    for ino in &evicted_inos {
                        if let Err(e) =
                            autumn_client::lease::release(&cluster_p, &id_p, *ino).await
                        {
                            tracing::warn!(
                                ino = *ino,
                                error = %e,
                                "BUG-LEASE-3: best-effort release after eviction"
                            );
                        }
                    }
                    if wholesale {
                        tracing::warn!(
                            "F-fuse-lease-1: overflow sentinel; wholesale invalidating session"
                        );
                        let drained: Vec<u64> = held_p.borrow().keys().copied().collect();
                        held_p.borrow_mut().clear();
                        inv_p.borrow_mut().clear();
                        if let Some(inv) = &invalidator_p {
                            for ino in &drained {
                                inv(*ino);
                            }
                        }
                        for ino in drained {
                            if let Err(e) =
                                autumn_client::lease::release(&cluster_p, &id_p, ino).await
                            {
                                tracing::warn!(ino, error = %e, "best-effort release after overflow");
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "F-fuse-lease-1: poll failed; invalidating session cache + retry"
                    );
                    let drained: Vec<u64> = held_p.borrow().keys().copied().collect();
                    held_p.borrow_mut().clear();
                    inv_p.borrow_mut().clear();
                    if let Some(inv) = &invalidator_p {
                        for ino in &drained {
                            inv(*ino);
                        }
                    }
                    for ino in drained {
                        let _ = autumn_client::lease::release(&cluster_p, &id_p, ino).await;
                    }
                    compio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })
    .detach();
}
