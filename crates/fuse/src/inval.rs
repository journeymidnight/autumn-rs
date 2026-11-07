//! Kernel cache invalidation, sent from a thread of its own.
//!
//! `Notifier::inval_inode` is a synchronous write(2) to `/dev/fuse`, and the
//! kernel serves it by locking every cached page of the inode
//! (`invalidate_inode_pages2_range`). A page under readahead stays locked until
//! its FUSE_READ is answered — and every FUSE_READ is prepared by the dispatch
//! thread. Issued from that thread, the write waits on a page that waits on the
//! same thread: the reader sits in D state, and the mount never answers another
//! request (FUSE has no timeout). `scripts/fuse_inval_deadlock.sh` wedges an
//! unfixed mount on the first or second event, with or without the read pool.
//!
//! So the dispatch runtime only queues the ino. This thread does the write —
//! blocking there is harmless, because the dispatcher stays free to answer the
//! read that holds the page — and sends each outcome back, in order, for
//! `record_results` to apply to the per-mount sticky-failure set on the
//! dispatch runtime.
//!
//! Nothing may wait for an invalidation to land before answering a request:
//! awaiting it inside a handler holds the dispatch loop just as the blocking
//! write held the thread.
//!
//! Exiting while a notify waits on a readahead page leaves a zombie: the
//! threads that would answer that read die, the waiting one is uninterruptible,
//! and the `/dev/fuse` fd — whose release aborts the connection and would free
//! the page — is only released once every thread has exited. The mount then
//! keeps no server behind it until the connection is aborted by hand
//! (`echo 1 > /sys/fs/fuse/connections/<minor>/abort`). So shutdown closes the
//! [`InvalGate`] first: it returns once the notify in flight, if any, has been
//! answered — the dispatcher is still serving — and no notify starts after it.
//! A SIGKILL skips that step and still leaves the zombie, when it lands while
//! page-cache pages are under readahead (a private mmap or a create fd, the
//! same workload that used to deadlock outright).

use std::cell::RefCell;
use std::collections::HashSet;
use std::io;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex, PoisonError};

use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::StreamExt;

use crate::dispatch::InodeInvalidator;

/// One notify's outcome, reported back to the dispatch runtime.
pub type InvalResult = (u64, io::Result<()>);

/// Shuts the invalidation thread for process exit. The thread holds the lock
/// across each notify, so taking it waits out the one in flight.
#[derive(Clone)]
pub struct InvalGate(Arc<Mutex<bool>>);

impl InvalGate {
    /// Returns once no notify is in flight; none starts afterwards. Inos still
    /// queued are dropped — nothing is left to serve the pages they would drop.
    pub fn close(&self) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = true;
    }
}

/// Start the invalidation thread around `notify` (the mount passes
/// `|ino| notifier.inval_inode(ino, 0, 0)`). Returns the sender the dispatch
/// runtime queues inos on, the stream of outcomes, and the gate shutdown
/// closes before exiting.
///
/// The thread ends when every sender is gone, the outcome stream is dropped or
/// the gate is closed. Nobody joins it.
pub fn spawn<F>(
    mut notify: F,
) -> io::Result<(mpsc::Sender<u64>, UnboundedReceiver<InvalResult>, InvalGate)>
where
    F: FnMut(u64) -> io::Result<()> + Send + 'static,
{
    let (ino_tx, ino_rx) = mpsc::channel::<u64>();
    let (result_tx, result_rx) = unbounded::<InvalResult>();
    let gate = InvalGate(Arc::new(Mutex::new(false)));
    let closed = gate.0.clone();
    std::thread::Builder::new()
        .name("autumn-fuse-inval".to_string())
        .spawn(move || {
            for ino in ino_rx {
                let closed = closed.lock().unwrap_or_else(PoisonError::into_inner);
                if *closed {
                    break;
                }
                let result = notify(ino);
                drop(closed);
                if result_tx.unbounded_send((ino, result)).is_err() {
                    break;
                }
            }
        })?;
    Ok((ino_tx, result_rx, gate))
}

/// The `InodeInvalidator` the lease tasks and the Open arm call: queue the ino
/// and return.
pub fn invalidator(ino_tx: mpsc::Sender<u64>) -> InodeInvalidator {
    Rc::new(move |ino: u64| {
        if ino_tx.send(ino).is_err() {
            tracing::warn!(ino, "invalidation thread is gone; kernel cache not dropped");
        }
    })
}

/// Apply each outcome to the sticky-failure set: a failed notify marks the ino,
/// so its next Open reloads the inode and queues the notify again; a success
/// clears the mark. Runs on the dispatch runtime until the thread stops.
pub async fn record_results(
    mut results: UnboundedReceiver<InvalResult>,
    failed: Rc<RefCell<HashSet<u64>>>,
) {
    while let Some((ino, result)) = results.next().await {
        record_result(&mut failed.borrow_mut(), ino, result);
    }
}

fn record_result(failed: &mut HashSet<u64>, ino: u64, result: io::Result<()>) {
    match result {
        Ok(()) => {
            if failed.remove(&ino) {
                tracing::info!(ino, "BUG-LEASE-6: notify_inval_inode retry succeeded");
            }
        }
        Err(e) => {
            failed.insert(ino);
            tracing::warn!(
                ino,
                error = %e,
                "BUG-LEASE-6: notify_inval_inode failed; marked sticky for retry on next Open"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The invalidator must return before the notify completes. The notify
    /// here holds until the test releases it, which the test does only after
    /// the invalidator call has returned — issued inline, the call would sit
    /// out the whole release timeout and the notify would report it.
    #[test]
    fn the_invalidator_does_not_wait_for_the_notify() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (ino_tx, mut results, _gate) = spawn(move |_ino| {
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| io::Error::other("the invalidator waited for the notify"))
        })
        .unwrap();
        let invalidate = invalidator(ino_tx);

        invalidate(7);
        release_tx.send(()).unwrap();

        let (ino, result) = futures::executor::block_on(results.next()).unwrap();
        assert_eq!(ino, 7);
        result.unwrap();
    }

    /// Outcomes come back in queue order, one per queued ino, each carrying
    /// the notify's own result.
    #[test]
    fn outcomes_come_back_in_order() {
        let (ino_tx, results, _gate) = spawn(|ino| {
            if ino % 2 == 0 {
                Err(io::Error::from_raw_os_error(libc::ENOENT))
            } else {
                Ok(())
            }
        })
        .unwrap();
        let invalidate = invalidator(ino_tx);
        for ino in 1..=4 {
            invalidate(ino);
        }
        drop(invalidate);

        let got: Vec<(u64, bool)> = futures::executor::block_on(results.collect::<Vec<_>>())
            .into_iter()
            .map(|(ino, r)| (ino, r.is_ok()))
            .collect();
        assert_eq!(got, vec![(1, true), (2, false), (3, true), (4, false)]);
    }

    /// Closing the gate waits for the notify in flight, and no notify runs
    /// after it — so the process can exit with no thread inside the kernel.
    #[test]
    fn closing_the_gate_waits_out_the_notify_in_flight_and_stops_the_rest() {
        let (entered_tx, entered_rx) = mpsc::channel::<u64>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (ino_tx, results, gate) = spawn(move |ino| {
            entered_tx.send(ino).unwrap();
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| io::Error::other("never released"))
        })
        .unwrap();
        let invalidate = invalidator(ino_tx);

        invalidate(1);
        assert_eq!(entered_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
        let (closed_tx, closed_rx) = mpsc::channel::<()>();
        std::thread::spawn(move || {
            gate.close();
            closed_tx.send(()).unwrap();
        });
        assert!(
            closed_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "close returned while a notify was still in flight"
        );
        release_tx.send(()).unwrap();
        closed_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        invalidate(2);
        drop(invalidate);
        let got: Vec<u64> = futures::executor::block_on(results.collect::<Vec<_>>())
            .into_iter()
            .map(|(ino, r)| {
                r.unwrap();
                ino
            })
            .collect();
        assert_eq!(got, vec![1], "a notify ran after the gate closed");
        assert!(entered_rx.try_recv().is_err());
    }

    #[test]
    fn a_failure_marks_the_ino_and_a_later_success_clears_it() {
        let mut failed = HashSet::new();
        record_result(&mut failed, 42, Err(io::Error::from_raw_os_error(libc::EINVAL)));
        record_result(&mut failed, 99, Ok(()));
        assert!(failed.contains(&42));
        assert!(!failed.contains(&99), "a failure on one ino must not mark another");

        record_result(&mut failed, 42, Ok(()));
        assert!(failed.is_empty());
    }
}
