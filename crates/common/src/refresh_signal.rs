//! Ask a background poll loop for an early fetch, from any thread.
//!
//! The loop owns the signal through an `Arc` and waits on it next to its
//! timer; a request wakes it at once. Requests that arrive while it is
//! already fetching collapse into one more fetch. Nothing is lost if the loop
//! restarts: the request is a flag, not a queued message.

use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use futures::task::AtomicWaker;

#[derive(Default)]
pub struct RefreshSignal {
    wanted: AtomicBool,
    waker: AtomicWaker,
}

impl RefreshSignal {
    pub fn request(&self) {
        self.wanted.store(true, Ordering::Release);
        self.waker.wake();
    }

    /// Resolves once a request is pending, and consumes it.
    pub async fn wait(&self) {
        poll_fn(|cx| {
            self.waker.register(cx.waker());
            if self.wanted.swap(false, Ordering::AcqRel) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

/// Milliseconds since this process started (monotonic).
pub fn mono_ms() -> u64 {
    static START: std::sync::LazyLock<std::time::Instant> =
        std::sync::LazyLock::new(std::time::Instant::now);
    START.elapsed().as_millis() as u64 + 1
}

/// What a request that met an unknown name (a namespace, a signing key)
/// should be told.
#[derive(Debug, PartialEq, Eq)]
pub enum Miss {
    /// The config may just be older than the name: fetch, and let the client
    /// retry.
    Retry,
    /// A config fetched after this name's first miss still lacks it.
    Final,
}

/// Remembers when each unknown name was first met, so that its retry is
/// answered from a config fetched AFTER that — not from one that merely
/// happens to be recent. Bounded: past `CAP` names, or after `TTL_MS`, a name
/// starts over.
#[derive(Default)]
pub struct MissLedger {
    first_miss_ms: std::sync::Mutex<std::collections::HashMap<u64, u64>>,
}

impl MissLedger {
    const CAP: usize = 1024;
    const TTL_MS: u64 = 60_000;

    /// `name` = a hash of what was not found; `installed_at_ms` = `mono_ms()`
    /// of the config the request was checked against.
    pub fn on_miss(&self, name: u64, installed_at_ms: u64) -> Miss {
        let now = mono_ms();
        let mut m = self
            .first_miss_ms
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(&first) = m.get(&name) {
            if installed_at_ms > first {
                m.remove(&name);
                return Miss::Final;
            }
            return Miss::Retry;
        }
        m.retain(|_, t| now.saturating_sub(*t) < Self::TTL_MS);
        if m.len() >= Self::CAP {
            return Miss::Final;
        }
        m.insert(name, now);
        Miss::Retry
    }
}

/// A stable hash of `bytes` for `MissLedger`.
pub fn name_hash(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn a_request_from_another_thread_wakes_the_waiter() {
        let sig = Arc::new(RefreshSignal::default());
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let s = sig.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                s.request();
            });
            compio::time::timeout(Duration::from_secs(5), sig.wait())
                .await
                .expect("woken");
            // Consumed: a second wait needs a second request.
            assert!(compio::time::timeout(Duration::from_millis(100), sig.wait()).await.is_err());
            sig.request();
            sig.request();
            compio::time::timeout(Duration::from_secs(1), sig.wait()).await.expect("pending");
        });
    }

    /// A name is retried until a config installed after its first miss has
    /// been checked; a recent install from before the miss does not count.
    #[test]
    fn a_name_is_final_only_against_a_config_fetched_after_its_first_miss() {
        let ledger = MissLedger::default();
        let installed_before = mono_ms();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let ns = name_hash(b"fresh/");
        assert_eq!(ledger.on_miss(ns, installed_before), Miss::Retry);
        assert_eq!(ledger.on_miss(ns, installed_before), Miss::Retry);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let installed_after = mono_ms();
        assert_eq!(ledger.on_miss(ns, installed_after), Miss::Final);
        // Another name met right after that install still gets its retry.
        assert_eq!(ledger.on_miss(name_hash(b"other/"), installed_after), Miss::Retry);
    }
}
