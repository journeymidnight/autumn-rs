# autumn-rs Architecture Guide

## Project shape

1. The bottom layer is the `stream layer`: distributed file storage and recovery.
2. The top layer is the `partition layer`: table management and ordered KV storage.
3. IMPORTANT: every code change must account for performance.
4. Each crate keeps its own `CLAUDE.md` as the architecture summary and the
   record of why the code is shaped the way it is. When you change code, update
   that crate's `CLAUDE.md` in the same change.

## Long-task execution rules

1. Before starting any task, read `claude-progress.txt` and `feature_list.md`
   at the repository root.
2. Before writing any code, output two lists for this task:
   - features/tasks already implemented
   - features/tasks not yet implemented
3. Coding starts only after those lists are out.
4. When the code is done, update the task status in `claude-progress.txt`. Only
   two values are permitted:
   - `completed`
   - `not_completed`
5. If the task was interrupted, blocked, or failed verification, the status
   must be written as `not_completed`.
6. Only when the feature, its tests, and its verification are all finished does
   the status become `completed`.
7. Long-task context is carried by three external memories:
   - `feature_list.md`: the feature list, acceptance criteria, completion state
   - `claude-progress.txt`: current progress, blockers, next steps
   - `git`: every intermediate result must be revertible and traceable
8. `feature_list.md` is the requirements ledger. Once a task has started, the
   requirement text, the acceptance steps, and the test criteria must not be
   rewritten; only the completion field (`passes` or its equivalent) may change.
9. Every session must end with a clean handoff:
   - commit this stage's code
   - update the status in `claude-progress.txt` and `feature_list.md`
   - leave the working tree in a state the next session can pick up directly
     (no destructive half-finished state)
10. Every feature moves through the same sequence:
    - define the feature (goal / boundary / acceptance)
    - implement it
    - run the tests and verify
    - update `docs/ops.md` (manual test and operations steps); if user-visible
      usage changed, update `README.md` too
    - commit — that commit is the feature's completion point
11. `docs/ops.md` must stay current: the manual verification steps have to remain
    executable. `README.md` stays user-facing (intro / features / usage; the
    pitch is all-in-one storage for AI architectures) and is not a dumping
    ground for verification steps.
12. When `claude-progress.txt` and `feature_list.md` grow too long, prune them.
    Keep them tidy.
13. Never write feature numbers of the `Fxxx` form in comments, in commit
    descriptions, or — above all — in the code itself.
14. A commit message must NEVER contain a
    `Claude-Session: https://claude.ai/code/session_...` line. Session links are
    internal, they expire, and they mean nothing to whoever reads `git log`
    later. `Co-Authored-By: Claude ...` may stay. (Do not rewrite the history
    that already carries one, and do not force-push over it.)
15. **Every time code is written, dispatch a fable subagent for an independent
    review** — after your own tests pass and before you write the commit. Use a
    NEW subagent each time; never continue an earlier reviewer, because one
    carrying its own previous context defends what it already concluded instead
    of re-deriving. This is not optional. It is here because it has paid off:
    a reviewer caught a self-introduced high-severity regression — after the
    bulk-read status moved from a `FLAG_ERROR` frame to a code inside `ctrl`,
    the upper layer's `Err(PreconditionFailed)`-triggered refresh-and-fallback
    became dead code, so a whole key group failed outright after split/merge —
    while the full unit suite and a byte-for-byte e2e run were **both green**.
    The same round also caught a missed writev segmentation point and doc
    comments that had been welded together.
    - Give the prompt everything: which files changed, the intent, the numbers
      already measured, and an instruction to separate what it VERIFIED in the
      code from what it INFERRED.
    - **Treat its inferences as hypotheses**, not findings: it once inferred a
      change would be slower on UCX; measurement showed +61%.
    - For every high-severity finding, after fixing it add a regression test
      that demonstrably goes red without the fix (ablation).
16. When writing code and fixing bugs, find the ROOT CAUSE. *Do not scatter
    defensive hardening*; that only adds junk. Instrumentation is fine — a log
    line that reports state changes nothing. A timeout, a retry tick, or a
    rollback does, and those stay out of the tree until the cause is proven. If
    a diagnosis is later disproven, revert the speculative fix rather than
    keeping it because it "seems harmless".
17. **Only the user edits this schema-level `CLAUDE.md`, by hand.** An agent
    must never modify it on its own initiative — not to add a rule it thinks is
    missing, not to tidy the wording, and not to restore something that looks
    like an accidental deletion. A change here that appears to be a mistake is
    the maintainer's deliberate edit until the maintainer says otherwise. The
    only exception is an explicit instruction from the user to change this file.

## `claude-progress.txt` conventions

1. Location: `claude-progress.txt` at the repository root.
2. The file must contain a `TaskStatus` field.
3. `TaskStatus` is either `completed` or `not_completed`. No other value is
   permitted.
4. Suggested structure:
```txt
Date: 2026-03-16
TaskStatus: not_completed
Task scope: ...
Current summary: ...
Main gaps: ...
Next steps: ...
```
