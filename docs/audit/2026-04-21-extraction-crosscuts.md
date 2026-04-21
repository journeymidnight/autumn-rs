# autumn-rs extraction — cross-cut inventory (2026-04-21)

This document enumerates everything that ties autumn-rs to the parent `autumn`
repo, so we can partition files cleanly for the `git filter-repo` split.

## 1. Inbound references (parent → autumn-rs)

`grep -rn "autumn-rs" --exclude-dir=autumn-rs <parent root>` total hits: **459**

Grouped by file:

### `./feature_list.md` (35 hits) — move to autumn-rs

All 35 references are paths like `autumn-rs/crates/...` in the Evidence fields
of Rust-era feature entries (F001–F099 series). The entire file is Rust-era
content (header says "autumn go→rust feature list", last updated 2026-04-07).
**Action:** move to `autumn-rs/feature_list.md`; leave a one-line stub at root
pointing to the new location.

### `./CLAUDE.md` (4 hits) — keep in parent with trimming

References describe: "新代码在 `autumn-rs`", "autumn-rs项目都增加了CLAUDE.md",
"`autumn-rs/README.md`" (×2). These are project-level meta-instructions for
working in the mixed repo. After extraction, trim these lines from the parent
copy; the `autumn-rs/CLAUDE.md` becomes the standalone instructions for the
extracted repo.

### `./AGENTS.md` (3 hits) — keep in parent with trimming

Same pattern as CLAUDE.md: references "新代码在 `autumn-rs`" and
"`autumn-rs/README.md`". After extraction, trim autumn-rs references from the
parent copy; they become stale.

### `./progress.md` (1 hit) — move to autumn-rs

The single reference points at `autumn-rs/...` path context. The file tracks
the current task state for the Rust rewrite. **Action:** move to
`autumn-rs/progress.md` (or keep as `claude-progress.txt` per CLAUDE.md
convention); stub parent with a redirect.

### `./.gitignore` (1 hit) — update parent after extraction

The parent `.gitignore` has the line `autumn-rs/target/` to exclude the Rust
build output. After extraction this line becomes meaningless in the parent repo.
**Action:** remove `autumn-rs/target/` from parent `.gitignore` post-extraction.

### `./docs/superpowers/plans/2026-04-21-autumn-rs-audit-and-extraction.md` (154 hits) — move to autumn-rs

This is the extraction task plan itself. All references are to
`autumn-rs/docs/audit/` and `autumn-rs/...` paths. Entirely Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/plans/` (or
`autumn-rs/docs/audit/`).

### `./docs/superpowers/plans/2026-04-18-perf-r1-plan.md` (80 hits) — move to autumn-rs

Perf plan for the Rust partition scale-out work (R1 series). All paths reference
`autumn-rs/crates/...`. Entirely Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/plans/`.

### `./docs/superpowers/plans/2026-04-18-perf-r3-plan.md` (62 hits) — move to autumn-rs

Perf plan for R3 multiplex pipeline. All paths reference `autumn-rs/crates/...`.
Entirely Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/plans/`.

### `./docs/superpowers/plans/2026-04-18-perf-r2-plan.md` (59 hits) — move to autumn-rs

Perf plan for R2 profile-then-optimize. All paths reference `autumn-rs/crates/...`.
Entirely Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/plans/`.

### `./docs/superpowers/specs/2026-04-18-perf-r1-partition-scale-out-design.md` (20 hits) — move to autumn-rs

Design spec for R1 scale-out. All references are to `autumn-rs/crates/...`.
**Action:** move to `autumn-rs/docs/superpowers/specs/`.

### `./docs/superpowers/specs/2026-04-20-perf-f099-n-ceiling.md` (8 hits) — move to autumn-rs

Diagnosis spec for the F099-N throughput ceiling. Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/specs/`.

### `./docs/superpowers/specs/2026-04-20-perf-f099k-n4-diagnosis.md` (8 hits) — move to autumn-rs

Diagnosis spec for F099k N=4 behaviour. Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/specs/`.

### `./docs/superpowers/specs/2026-04-20-perf-r4-ceiling-diagnosis.md` (7 hits) — move to autumn-rs

Diagnosis spec for R4 ceiling. Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/specs/`.

### `./docs/superpowers/specs/2026-04-20-perf-f099-h-kernel-rtt.md` (6 hits) — move to autumn-rs

Kernel RTT spec for F099-H. Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/specs/`.

### `./docs/superpowers/specs/2026-04-18-perf-r3-multiplex-pipeline-design.md` (5 hits) — move to autumn-rs

Design spec for R3 multiplex pipeline. Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/specs/`.

### `./docs/superpowers/specs/2026-04-18-perf-r2-profile-then-optimize-design.md` (4 hits) — move to autumn-rs

Design spec for R2 profiling. Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/specs/`.

### `./docs/superpowers/diagnosis/2026-04-18-perf-r2-flamegraph-analysis.md` (1 hit) — move to autumn-rs

Flamegraph analysis for R2 perf work. Rust-era.
**Action:** move to `autumn-rs/docs/superpowers/diagnosis/`.

### `./.git` (1 hit — internal git object, not a text file)

This is the git index entry for `autumn-rs/` itself. Not an actionable reference.

---

Note: `docs/superpowers/specs/2026-04-18-perf-r4-sqcq-pipeline-design.md` has
**0** hits for "autumn-rs" in the inbound scan (it exists in the specs dir but
was not counted above). It is still Rust-era and should be moved with the other
specs.

## 2. Outbound references (autumn-rs → parent)

`grep -rn '\.\./\.\.' autumn-rs` total hits: **0**
`grep -rn "/data/dongmao_dev/autumn[^-/]" autumn-rs` total hits: **0**

No `../../` or absolute-path references found. autumn-rs is fully self-contained
at the filesystem level.

Verdict: **self-contained**. No outbound blockers.

## 3. Shared-file partition plan

| File / dir | Current location | Action for extraction |
|---|---|---|
| `CLAUDE.md` (root) | Exists (2565 bytes). Contains Go→Rust project goal (长任务执行规则), references to `autumn-rs/README.md` and `autumn-rs/CLAUDE.md`. Pure meta-instructions, no Go code. | Keep in parent but remove the four `autumn-rs`-specific lines (goal item 2 & 6, long-task rule 10 & 11 referencing `autumn-rs/README.md`). The extracted repo already has `autumn-rs/CLAUDE.md` per sub-crate convention. |
| `AGENTS.md` (root) | Exists. Duplicate of CLAUDE.md but using `progress.md` instead of `claude-progress.txt`. References `autumn-rs` in goal items 2 and in rule 10. | Keep in parent; trim the three `autumn-rs` references after extraction. |
| `feature_list.md` (root) | Exists (612 lines, ~117 KB). Header: "autumn go→rust feature list", last updated 2026-04-07. All 88 named features (F001–F099 series) are Rust-era; there are no Go-only feature entries. | Move to `autumn-rs/feature_list.md`. Replace root copy with a one-line stub: "Feature list moved to autumn-rs/feature_list.md (now a separate repo)." |
| `claude-progress.txt` (root) | Exists (5102 bytes). Current entry date 2026-04-21, TaskStatus: completed. Tracks Rust rewrite tasks exclusively. Branch field references Rust branches. | Move to `autumn-rs/claude-progress.txt`. Seed a fresh root stub (or drop if parent becomes Go-only and no longer needs the harness). |
| `progress.md` (root) | Exists (3111 bytes). Date 2026-04-02, TaskStatus: completed. Tracks Rust batching work. Older sibling of claude-progress.txt (AGENTS.md still points here). | Move to `autumn-rs/progress.md`. Parent keeps a redirect stub or drops the file once all agents are updated to `claude-progress.txt`. |
| `docs/superpowers/specs/` (root) | Exists. 8 files, all dated 2026-04-18 or 2026-04-20. All Rust-era (R1–R4 / F099 series). No Go-era spec files found. | Move all 8 to `autumn-rs/docs/superpowers/specs/` via `git mv`. Remove the now-empty dir from parent. |
| `docs/superpowers/plans/` (root) | Exists. 4 files: 3 perf-r series (2026-04-18) + the audit-and-extraction plan (2026-04-21). All Rust-era. | Move all 4 to `autumn-rs/docs/superpowers/plans/` via `git mv`. |
| `docs/superpowers/diagnosis/` (root) | Exists. 1 file: `2026-04-18-perf-r2-flamegraph-analysis.md`. Rust-era. | Move to `autumn-rs/docs/superpowers/diagnosis/` via `git mv`. |
| `LICENSE` (root) | Exists. Apache License Version 2.0. | Copy to `autumn-rs/LICENSE` (Apache-2.0 matches the Rust crate `license = "Apache-2.0"` field in Cargo.toml). Keep original in parent. |
| Parent `.gitignore` | Exists. Contains `*.json` and `*.toml` global exclusions, and `autumn-rs/target/` exclusion. No `!perf_baseline.json` override at root level — that override lives in `autumn-rs/.gitignore`. | After extraction: remove `autumn-rs/target/` line from parent `.gitignore`. The `autumn-rs/.gitignore` (containing `!perf_baseline.json`) will travel with `autumn-rs/` naturally under `filter-repo`. |
| `autumn-rs/.gitignore` | Exists (`!perf_baseline.json` override). Unblocks tracking of perf baseline JSON inside autumn-rs/ despite the root `*.json` exclusion. | No action needed; it travels with the subdirectory during `filter-repo`. After extraction it becomes the root `.gitignore` for the new repo — verify it is sufficient standalone. |
| Parent `.github/` | Exists. Single workflow: `.github/workflows/main.yml`. Go-only: uses `go-version: [1.16.x]`, runs `make test` and `docker push journeymidnight/autumn:latest`. No Rust or autumn-rs references. | Keep in parent as-is. Port a separate `rust.yml` workflow to `autumn-rs/.github/workflows/` for the extracted repo (cargo test, cargo build). |
| Parent `README.md` | Exists. Go-oriented description: "Distributed Object Store", Windows Azure Storage reference, Apache 2.0 license badge. No mention of Rust or autumn-rs. | Keep in parent. Add a short redirect banner at the top: "The Rust rewrite lives at [new repo URL]." |
| Parent `Makefile` | Exists. Go-only: builds `extent-node`, `autumn-manager`, `autumn-ps`, `autumn-client` via `make -C cmd/$$dir/`. Zero references to `autumn-rs`. | No change needed. |
| Parent `go.mod` / `go.sum` | Exists. `module github.com/journeymidnight/autumn`, `go 1.16`. Go-only, no Rust content. | No change needed. |

## 4. Build / script references

`grep -rn "autumn-rs" --include="*.sh" --include="*.py" --exclude-dir=autumn-rs .` total hits: **0**

No shell scripts or Python scripts in the parent reference `autumn-rs`. The
`Makefile` also has zero references (verified above). No build/script blockers.

## 5. Blocker checklist before filter-repo

- [x] Every outbound reference resolved or explicitly accepted — zero `../../` and zero absolute-path refs found; autumn-rs is fully self-contained.
- [x] Every inbound reference is either (a) moved with the file, (b) updated to point at new repo URL, or (c) explicitly left in parent as historical — all 459 hits classified above; all are in files that will either move (`feature_list.md`, `claude-progress.txt`, `progress.md`, all `docs/superpowers/` files) or be trimmed (`CLAUDE.md`, `AGENTS.md`, `.gitignore`). None block `filter-repo`.
- [ ] LICENSE decision made — parent has Apache-2.0 `LICENSE`. Action required (Task 8): copy to `autumn-rs/LICENSE` before extraction.
- [x] `.gitignore` strategy decided — the `autumn-rs/.gitignore` (`!perf_baseline.json` override) travels naturally with `filter-repo --subdirectory-filter autumn-rs`. After extraction, verify it functions as root `.gitignore`. The parent `.gitignore` `autumn-rs/target/` line should be removed in cleanup (Task 11).
- [ ] Parent `.github/` workflow — the single Go workflow travels with the parent. A Rust-specific `rust.yml` needs to be added to `autumn-rs/.github/workflows/` before or after extraction (Task 8 or new task).

## 6. Extraction strategy summary

`git filter-repo --subdirectory-filter autumn-rs` will preserve approximately
**206 commits** that touch `autumn-rs/` (measured via `git log --oneline --
autumn-rs | wc -l` on the `compio` branch). There are zero outbound references
(`../../` or absolute paths) from autumn-rs into the parent, so no source edits
are required before running `filter-repo`. Thirteen shared files must be
migrated first (Task 8): `feature_list.md`, `claude-progress.txt`, `progress.md`,
all 13 `docs/superpowers/` files (8 specs + 4 plans + 1 diagnosis), and a copy
of `LICENSE`; the two agent-instruction files (`CLAUDE.md`, `AGENTS.md`) need
minor trimming in the parent post-extraction rather than migration. No inbound
references block the `filter-repo` run itself; all parent-side cleanup
(`CLAUDE.md` trim, `.gitignore` line removal, README banner) can happen in
Task 11 after the new repo is live.

## filter-repo dry run — 2026-04-21

**Source:** `/data/dongmao_dev/autumn/.claude/worktrees/autumn-rs-extraction` @ `a0387ba extract: migrate Rust-era feature list, docs, and long-task rules under autumn-rs/`
**Output:** `/tmp/autumn-rs-split`
**Commits before filter-repo:** 456 (full branch history on `autumn-rs-extraction`)
**Commits after filter-repo:** 210 (commits touching `autumn-rs/`)
**First commit preserved:** `ae335bc rust: add stream append benchmark and client connection reuse`
**Parent-only files leaked:** none (`test ! -e go.mod && test ! -e autumn_clientv1 && test ! -e manager && test ! -e node && test ! -e partition_server` → `CLEAN`)
**Build:** pass (`Finished dev profile` in 12.79s on warm cache)
**Unit tests:** 144 passed / 0 failed / 0 ignored (across 7 crates: 0+2+3+5+84+10+40)
**Integration tests:** 213 passed / 0 failed / 8 ignored (across all workspace crates, --test-threads=1)
**perf_check --shm vs baseline (`perf_baseline_shm.json`):**
  - write ops/s: 38,021 (baseline 41,389, Δ−8.1%) — within 80% gate
  - read ops/s: 53,043 (baseline 51,014, Δ+4.0%) — above baseline
  - verdict: `perf-check OK` — within tolerance (gate is 80% of baseline)

**Verdict:** filter-repo split is clean; ready for Task 10 (push to new remote).
