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
| 3 | crates/rpc | 6579 | in_progress | |
| 4 | crates/transport (+ucx-sys-mini) | 4140 | todo | |
| 5 | crates/client | 3966 | todo | |
| 6 | crates/stream — server side (extent node) | ~9000 | todo | |
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
- (none yet)
