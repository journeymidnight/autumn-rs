//! Transient `alloc_extent` conflict messages: the BUILDERS (manager side)
//! and the CLASSIFIER (stream-client side) in one place.
//!
//! Over the wire these rejections are plain `CODE_PRECONDITION`, which is
//! also what deterministic business-rule refusals use ("stream cannot be
//! empty after punch holes", admin-token checks). The client must tell them
//! apart: a transient conflict self-heals by re-pulling a fresh snapshot or
//! waiting out an in-flight op and retrying, while a deterministic
//! precondition must fail fast. Distinguishing them by a new wire code would
//! perturb `WIRE_FINGERPRINT` and force a stop-world version bump, so the
//! signal rides in the message text.
//!
//! That makes the text load-bearing, so no caller writes it: the manager
//! calls a builder here, the stream client calls [`is_transient_alloc_conflict_message`]
//! here, and `builders_pair_with_classifier` runs every builder's real output
//! through the classifier. A reword cannot detach the matcher, because there
//! is no second place to reword.
//!
//! Sibling classifier for owner-epoch fences: [`crate::store::is_owner_epoch_fence_message`].
//!
//! Renaming a token is WIRE-visible for a mixed-version cluster (an old
//! client matching a new manager's text): treat it like a wire-schema edit —
//! same-commit stop-world only.

/// Marker for "your view of the metadata is stale; re-read and retry".
pub const FRESH_SNAPSHOT_TOKEN: &str = "fresh snapshot";

/// Marker for "another op holds this extent; retry once it finishes".
pub const INFLIGHT_DEFER_TOKEN: &str = "until it completes";

/// etcd value-CAS lost a race against a concurrent stream mutation.
pub fn cas_conflict_message() -> String {
    format!("stream changed concurrently (CAS conflict); retry with a {FRESH_SNAPSHOT_TOKEN}")
}

/// The tail extent is busy with an EC conversion / recovery / GC op.
pub fn inflight_defer_message(extent_id: u64, op: &str) -> String {
    format!("extent {extent_id} has in-flight {op}; defer alloc_extent {INFLIGHT_DEFER_TOKEN}")
}

/// Verify-at-apply saw the stream's membership move under the allocation.
pub fn membership_changed_message(stream_id: u64) -> String {
    format!(
        "stream {stream_id} membership changed during alloc_extent; \
         retry with {FRESH_SNAPSHOT_TOKEN}"
    )
}

/// Verify-at-apply saw the tail extent's eversion move under the allocation.
pub fn eversion_changed_message(extent_id: u64, expected: u64, live: u64) -> String {
    format!(
        "extent {extent_id} eversion changed during alloc_extent \
         ({expected} -> {live}); retry with {FRESH_SNAPSHOT_TOKEN}"
    )
}

/// Is this `CODE_PRECONDITION` message a transient conflict the caller should
/// retry, rather than a deterministic refusal it must surface?
///
/// Callers pair this with the code check — see `StreamError::is_transient_conflict`.
pub fn is_transient_alloc_conflict_message(msg: &str) -> bool {
    msg.contains(FRESH_SNAPSHOT_TOKEN) || msg.contains(INFLIGHT_DEFER_TOKEN)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The point of the module: every REAL builder output classifies as
    /// transient. Literals are deliberately absent — a test that matched its
    /// own literals would keep passing after a producer reword, which is the
    /// exact failure this pairing exists to catch.
    #[test]
    fn builders_pair_with_classifier() {
        for m in [
            cas_conflict_message(),
            inflight_defer_message(7, "EcConvert"),
            membership_changed_message(42),
            eversion_changed_message(7, 3, 4),
        ] {
            assert!(
                is_transient_alloc_conflict_message(&m),
                "builder output must classify as transient: {m}"
            );
        }
    }

    /// Deterministic refusals that share `CODE_PRECONDITION` must NOT be
    /// swept in — misclassifying one turns a hard error into a retry loop.
    #[test]
    fn deterministic_preconditions_are_not_transient() {
        for m in [
            "stream cannot be empty after punch holes",
            "owner_key=ps-1 owner_epoch mismatch, expected 4, got 3",
            "admin token invalid",
            "extent 7 is already sealed",
        ] {
            assert!(
                !is_transient_alloc_conflict_message(m),
                "must stay fail-fast: {m}"
            );
        }
    }
}
