# Readability Review Ledger (loop task, started 2026-07-01)

Task: review ALL code and make the CODE LOGIC easy to understand
(behavior-preserving, perf-neutral; no coco). Iterate until every chunk is `done`.

Focus (user directive 2026-07-01): simplify the logic itself — NOT comments.
Rules per chunk:
- Read every file in the chunk fully.
- Targets: deeply nested control flow, duplicated branches, overly clever
  expressions, functions doing several unrelated things, misleading names,
  dead/vestigial code paths. Restructure so the code carries the meaning.
- NO comment-only edits. Comments only where a constraint can't be expressed
  in code.
- Every refactor must be provably behavior-preserving and perf-neutral
  (hot paths: keep allocation/lock/syscall counts identical).
- After edits: full `cargo test -p <crate> --lib` green (plus
  `cargo build --workspace` on cross-crate ripple).
- Commit per chunk; push to main.

| # | Chunk | Lines | Status | Commit(s) |
|---|-------|-------|--------|-----------|
| 1 | crates/common | 601 | done (no changes needed — logic already clear) | |
| 2 | crates/etcd | 1329 | done (no changes needed — logic already clear) | |
| 3 | crates/rpc | 6579 | done | f833d1d (client.rs read_loop dedup), f89802c (bench dedup) |
| 4 | crates/transport (+ucx-sys-mini) | 4140 | done | 0fe3e72 (endpoint/lib/listener dedup), 63dcc34 (regpool dead branch + bench/test dedup) |
| 5 | crates/client | 3966 | done | c66d555 (NOT_LEADER loop ×4, fail_slots ×8, GetStream ctor, lease_call ×4) |
| 6 | crates/stream — server side (extent node) | ~9000 | done | 9bc0408 (read_plan/committed_length_value/wrong_shard_err dedup + clippy sweep; conn_pool+erasure clean) |
| 7 | crates/stream — client side (StreamClient) | ~9000 | todo | |
| 8 | crates/partition-server — core write/read path | ~11000 | todo | |
| 9 | crates/partition-server — flush/compact/GC/split | ~11400 | todo | |
| 10 | crates/manager — core (lib, stream mgmt) | ~13000 | todo | |
| 11 | crates/manager — partition mgmt + authz + lease | ~13000 | todo | |
| 12 | crates/manager — rest (recovery, EC, tools) | ~13000 | todo | |
| 13 | crates/server (binaries) | 8170 | todo | |
| 14 | crates/fuse | 4731 | todo | |
| 15 | crates/autumn-memory | 2334 | todo | |
| 16 | examples/gallery | 1837 | todo | |
| 17 | python/ (bindings + memory + adapters) | 5762 | todo | |

Status values: todo | in_progress | done

## Findings log (cross-chunk issues found while reviewing)
- Wire-fingerprinted files (rpc: manager_rpc/partition_rpc/extent_rpc/frame/
  cap_token): edits force a wire-version registry decision, so low-value
  cleanups are deferred until those files are next touched for real work:
  - cap_token.rs:140+183 — signer & verifier both hand-build
    `CAP_DOMAIN ‖ claims_bytes`; factor a shared `signing_input()` to make
    the "single source of truth" claim structural.
  - extent_rpc.rs:470 — `rkyv_encode/rkyv_decode` byte-identical duplicates of
    manager_rpc.rs:204 (partition_rpc already re-exports; extent_rpc should too).
  - partition_rpc.rs:331 — `parse_put_zc_meta` dead `.ok()?` on infallible
    fixed-slice try_into after the length guard.
- rpc CLAUDE.md drift (doc-only, not fixed in this logic pass): mentions
  `writer_task_handles_2048_concurrent_vectored` test that no longer exists;
  says MAX_PAYLOAD_LEN=512MB but code says u32::MAX.
- Pre-existing dead code flagged by cargo check (chunk 8/9 material):
  partition-server `decode_records_with_offsets`, `lookup_in_sst`,
  `invalidate_extent`/`stats` methods; server bin unused imports
  `MergeIterator`/`TableIterator`; fields `server_owner_key`/`server_revision`
  never read.
- transport regpool.rs:141 — clippy manual_range_contains (fixed in 0fe3e72).
- transport skipped-as-not-worth-it: regpool warn-once latch duplicated at 2
  cold sites (borderline); pre-existing ucx-build clippy nits (vec_box on the
  deliberate stable-address Slot pool, u32→u32 FFI casts) left alone.
- extent_node.rs DEFERRED (needs a dedicated session + chaos validation, per
  the note-24 reverted-refactor history — do NOT do these in a casual pass):
  - The append validation protocol (corrupt_meta gate → eversion refresh →
    seal check → P0-B durable fence + recheck → commit reconcile + F146/F147-B
    recheck) exists TWICE: build_append_future (hot, ~2200-2360) and
    handle_append (single-op/test-only reference, ~5330-5490). Guard fixes
    have had to land twice (F146 vs F147-B). Extraction shape: an
    `append_fence_and_reconcile(extent, owner_epoch, commit) -> AppendGate`
    helper; callers map to their response forms. handle_append is NOT
    production-reachable (process_frames_backpressured handles MSG_APPEND
    before dispatch()), so the hot copy is the only perf-sensitive one.
  - handle_convert_to_ec is ~420 lines with 3 clean seams (setup+lock /
    stripe-encode-fanout / phase-2 commit loop).
- stream conn_pool.rs: `is_healthy` only checks pool presence, not
  `is_closed()` — arguably misleading name (public API; left alone).
