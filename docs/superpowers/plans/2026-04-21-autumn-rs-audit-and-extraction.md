# autumn-rs Audit & Git-Repo Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Audit the autumn-rs subtree (unit + distributed-integration coverage, perf_check health) and extract it — with full git history — into a standalone repository, leaving a clean cut-over on the parent autumn (Go) repo.

**Architecture:** Two phases, must run in order.
- **Phase A — Audit (read-only):** measure unit-test line coverage, catalogue integration / system tests against a distributed-system scenario matrix, re-run `perf_check.sh` against the committed baselines, publish a go / no-go verdict.
- **Phase B — Extraction:** make `autumn-rs/` self-contained (LICENSE, .gitignore, CI, standalone docs), split git history with `git filter-repo --subdirectory-filter autumn-rs`, dry-run-clone-and-verify, push to the new remote, retire the tree in the parent repo.

**Tech Stack:** Rust workspace (9 crates, ~35k LOC), `compio` async runtime, `cargo-llvm-cov` (coverage), `git-filter-repo` (history split), `cargo build/test/clippy/fmt` + `perf_check.sh` on shell.

**Repository context (as of 2026-04-21, branch `compio`):**
- Total test attributes: 212 (per `grep -rE '#\[test\]|#\[tokio::test\]' crates --include='*.rs' | wc -l`)
- Integration tests: `crates/manager/tests/` (18 files, 6.4k LoC), `crates/stream/tests/` (8 files), `crates/rpc/tests/` (1 file)
- Perf baselines: `perf_baseline.json` (disk, 33k w / 57k r), `perf_baseline_shm.json` (tmpfs, 55k w / 74k r)
- autumn-rs first commit: `79a30dd` — 194 commits touch this path
- No external `path = "../../..` deps cross the `autumn-rs/` boundary (verified via `grep -rn 'path\s*=' autumn-rs --include=Cargo.toml`)

---

## Phase A — Audit

### Task 1: Install cargo-llvm-cov and produce workspace unit-test coverage

**Files:**
- Create: `autumn-rs/docs/audit/2026-04-21-unit-coverage.md`

- [ ] **Step 1: Install cargo-llvm-cov**

```bash
cargo install cargo-llvm-cov --locked
rustup component add llvm-tools-preview
```

Expected: `cargo llvm-cov --version` prints a version; `llvm-tools-preview` component is installed for the active toolchain.

- [ ] **Step 2: Generate workspace line-coverage summary (lib tests only, fast)**

Reason to separate: integration tests under `tests/` pay a heavy cluster-spin-up cost (Task 3). We isolate the unit-test signal here.

```bash
cd /data/dongmao_dev/autumn/autumn-rs
cargo llvm-cov --workspace --lib --summary-only --json \
  --output-path /tmp/autumn-rs-unit-cov.json \
  2>&1 | tee /tmp/autumn-rs-unit-cov.log
```

Expected: prints a per-file line/region/function coverage table; exits 0. Writes JSON to `/tmp/autumn-rs-unit-cov.json`.

If it fails with "no `lib` target" in some crates (e.g. `server` has bin-only targets), re-run with `--workspace --lib --bins` and note it.

- [ ] **Step 3: Generate HTML report for drill-down**

```bash
cargo llvm-cov --workspace --lib --html \
  --output-dir /tmp/autumn-rs-unit-cov-html
```

Expected: `/tmp/autumn-rs-unit-cov-html/index.html` exists.

- [ ] **Step 4: Write the audit markdown**

Create `autumn-rs/docs/audit/2026-04-21-unit-coverage.md` with this exact skeleton — fill the numbers from Step 2 output:

```markdown
# autumn-rs unit-test coverage — 2026-04-21

**Tool:** cargo-llvm-cov (lib + bin units only, no tests/ integration)
**Command:** `cargo llvm-cov --workspace --lib --summary-only`
**Snapshot JSON:** `/tmp/autumn-rs-unit-cov.json`
**HTML report:** `/tmp/autumn-rs-unit-cov-html/index.html`

## Workspace totals

| Metric | Covered | Total | % |
|---|---|---|---|
| Lines | <fill> | <fill> | <fill>% |
| Regions | <fill> | <fill> | <fill>% |
| Functions | <fill> | <fill> | <fill>% |

## Per-crate (ranked by line coverage %)

| Crate | Lines % | Functions % | Key uncovered modules |
|---|---|---|---|
| autumn-common | | | |
| autumn-rpc | | | |
| autumn-etcd | | | |
| autumn-stream | | | |
| autumn-manager | | | |
| autumn-partition-server | | | |
| autumn-client | | | |
| autumn-server (bins only) | | | |
| autumn-fuse | | | |

## Top-10 largest uncovered files

Paste the 10 files with the biggest absolute number of uncovered lines
(not %age), each with `file:lines-uncovered / lines-total (coverage%)`.

## Observations

- Which crates fall below 60% line coverage?
- Which files have <30% coverage despite being >500 LoC?
- Any `unsafe` blocks uncovered? (grep the report for `unsafe`)

## Raw summary output

```
<paste the stderr/stdout table from Step 2 verbatim>
```
```

- [ ] **Step 5: Commit**

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/docs/audit/2026-04-21-unit-coverage.md
git commit -m "audit: autumn-rs unit-test coverage snapshot 2026-04-21"
```

---

### Task 2: Integration-test scenario inventory for distributed-system coverage

**Files:**
- Create: `autumn-rs/docs/audit/2026-04-21-integration-inventory.md`

- [ ] **Step 1: Enumerate all integration-test files and test functions**

```bash
cd /data/dongmao_dev/autumn/autumn-rs
for f in crates/*/tests/*.rs; do
  echo "## $f"
  grep -nE '^\s*#\[(test|tokio::test)\]|^\s*fn [a-z_]+\(' "$f" \
    | head -200
  echo ""
done > /tmp/autumn-rs-integration-fns.txt
wc -l /tmp/autumn-rs-integration-fns.txt
```

Expected: produces a flat listing of every `#[test]` and its following `fn name(...)` grouped by file. ~80–120 entries.

- [ ] **Step 2: Build the scenario-coverage matrix**

Fill `autumn-rs/docs/audit/2026-04-21-integration-inventory.md` with this matrix; put an `x` where a test exercises the scenario, `—` where not:

```markdown
# autumn-rs distributed-system integration coverage — 2026-04-21

**Scope:** `crates/*/tests/*.rs` — out-of-process integration / system tests
that spin up manager + extent-nodes + (optionally) PS via real RPC.

## Scenario × test matrix

| Scenario | Current coverage (files / fns) | Status |
|---|---|---|
| **Manager** |
| etcd-backed metadata round-trip | `manager/tests/etcd_stream_integration.rs` (4) | covered |
| Manager leader failover | `manager/tests/system_manager_failover.rs` (2) | covered |
| Manager crash mid-allocation | `manager/tests/integration.rs` (?) | |
| **Extent-node** |
| Extent-node restart recovery | `stream/tests/extent_restart_recovery.rs` | covered |
| Extent-node crash mid-append (WAL replay) | `stream/tests/wal_recovery.rs` | covered |
| Extent-node failover (quorum replacement) | `manager/tests/system_extent_failover.rs` (2) | covered |
| Multi-disk round-robin | `stream/tests/f021_multi_disk.rs` | covered |
| Per-shard runtime sharding (F099-M) | `stream/tests/f099m_shards.rs` (4) | covered |
| Extent-append pipeline (SQ/CQ) | `stream/tests/stream_sqcq.rs` (5), `extent_pipeline.rs`, `extent_append_semantics.rs` | covered |
| **Partition-server** |
| PS crash → reassign → resume | `manager/tests/system_ps_failover.rs` (2) | covered |
| PS restart replays metaStream+logStream | `manager/tests/system_ps_recovery.rs` (2) | covered |
| Partition split (normal) | `manager/tests/system_split_writes.rs` (1), `system_split_overlap.rs` (1), `system_split_ref_counting.rs` (1) | covered |
| Partition split with large values (VP) | `manager/tests/system_split_large_values.rs` (1) | covered |
| Seal-during-writes race | `manager/tests/system_seal_during_writes.rs` (1) | covered |
| `LockedByOther` revision fencing | `manager/tests/system_locked_by_other.rs` (1) | covered |
| Compound failure (multi-component) | `manager/tests/system_compound_failures.rs` (1) | covered |
| **Erasure coding** |
| EC encode/decode correctness | `manager/tests/ec_integration.rs` (4) | covered |
| EC failover (one shard loss) | `manager/tests/ec_failover.rs` (1) | covered |
| **Known gaps (candidate future work)** |
| Network partition (asymmetric, client ↔ PS) | — | gap |
| Slow / stuck replica (one extent-node artificially delayed) | — | gap |
| Clock skew / NTP jump across nodes | — | gap |
| Quorum loss recovery (2-of-3 nodes down) | — | gap |
| Disk-full ENOSPC on extent-node | — | gap |
| Rolling-upgrade binary compatibility | — | gap |
| Manager + PS simultaneous restart | — | gap |

## File-level test count

Paste output of:
  awk 'FNR==1{print FILENAME}/#\[test\]|#\[tokio::test\]/{c++} END{}' crates/*/tests/*.rs
and the `grep -c '#\[test\]' crates/*/tests/*.rs` table.

## Gap analysis

<2–3 sentence summary: which distributed-system scenarios lack coverage,
and of those, which are load-bearing for production confidence.>

## Verdict

- **Covered well:** failover, recovery, split, EC — the happy-path correctness envelope is thorough.
- **Under-covered:** partial-failure & degraded-mode scenarios (partition, skew, slow-replica, ENOSPC).
```

- [ ] **Step 3: Commit**

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/docs/audit/2026-04-21-integration-inventory.md
git commit -m "audit: autumn-rs distributed-system integration inventory 2026-04-21"
```

---

### Task 3: Run all integration tests green

**Files:**
- Modify: `autumn-rs/docs/audit/2026-04-21-integration-inventory.md` (append a “Green run 2026-04-21” section)

- [ ] **Step 1: Ensure etcd is available**

```bash
which etcd && etcd --version || echo "MISSING ETCD"
```

Many manager integration tests start a real etcd. If missing, install (`brew install etcd` / `apt install etcd-server`) before proceeding, or use the embedded etcd (`crates/manager/tests/support/embedded_etcd/`).

- [ ] **Step 2: Ensure no stale cluster process holds ports**

```bash
pkill -f autumn-manager-server || true
pkill -f autumn-extent-node || true
pkill -f autumn-ps || true
pkill -f 'etcd --data-dir /tmp/autumn-etcd' || true
```

Expected: exit 0 whether or not processes existed.

- [ ] **Step 3: Run the full test suite with single-thread for stability**

Integration tests here grab real TCP ports (via `pick_addr()`) and run real subprocesses; parallelism causes flake.

```bash
cd /data/dongmao_dev/autumn/autumn-rs
cargo test --workspace --tests -- --test-threads=1 2>&1 \
  | tee /tmp/autumn-rs-integration-run.log
```

Expected last line: `test result: ok. <N> passed; 0 failed; <M> ignored`. Total expected ~80–120 passing.

- [ ] **Step 4: Append run results to the inventory doc**

Append a new section to `autumn-rs/docs/audit/2026-04-21-integration-inventory.md`:

```markdown
## Green run — 2026-04-21

**Command:** `cargo test --workspace --tests -- --test-threads=1`
**Wall clock:** <paste>
**Result:** <paste the final "test result:" line(s)>
**Failed tests:** <list or "none">
**Ignored tests:** <list with reasons if any>
**Log:** `/tmp/autumn-rs-integration-run.log`
```

- [ ] **Step 5: Commit**

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/docs/audit/2026-04-21-integration-inventory.md
git commit -m "audit: integration-test green run results 2026-04-21"
```

---

### Task 4: Re-run perf_check against committed baselines (disk + tmpfs)

**Files:**
- Create: `autumn-rs/docs/audit/2026-04-21-perf-check.md`

- [ ] **Step 1: Build release**

```bash
cd /data/dongmao_dev/autumn/autumn-rs
cargo build --workspace --release --exclude autumn-fuse 2>&1 | tail -5
```

Expected: `Finished `release` profile` as final line.

- [ ] **Step 2: Run perf_check on disk**

```bash
cd /data/dongmao_dev/autumn/autumn-rs
./perf_check.sh 2>&1 | tee /tmp/autumn-rs-perf-disk.log
```

Expected: Script spins a fresh 3-replica cluster, runs the 256-thread × 10s bench, prints a diff vs. `perf_baseline.json`. Cluster is stopped at the end.

- [ ] **Step 3: Run perf_check on tmpfs**

```bash
./perf_check.sh --shm 2>&1 | tee /tmp/autumn-rs-perf-shm.log
```

Expected: prints diff vs. `perf_baseline_shm.json`.

- [ ] **Step 4: Optional multi-partition sweep (validates F099-N-c fix)**

Context: commit `6bd2ddb` fixed a bench-tool bug where all keys landed in the last partition at N>1. A post-fix sweep should now show real scaling.

```bash
for N in 1 2 4 8; do
  ./perf_check.sh --shm --partitions "$N" \
    2>&1 | tee "/tmp/autumn-rs-perf-shm-n${N}.log"
done
```

Expected: write ops/s grows roughly linearly through N=4 (F099-N-b predicted 150–200k at N=4 post-fix vs. the pre-fix 62k plateau).

- [ ] **Step 5: Write the perf audit**

Create `autumn-rs/docs/audit/2026-04-21-perf-check.md`:

```markdown
# autumn-rs perf_check — 2026-04-21

**Host:** <uname -a, CPU model, core count, memory>
**Kernel knobs:** `ulimit -n <value>` (perf_check.sh sets 65536)
**Git HEAD:** <git rev-parse HEAD>
**Binaries:** `target/release/autumn-{manager-server, extent-node, ps, client}`

## Results vs. baseline

### Disk — `AUTUMN_DATA_ROOT=/tmp/autumn-rs`

| Metric | Baseline | This run | Δ % |
|---|---|---|---|
| write ops/s | 32995 | <fill> | |
| write p50 / p95 / p99 (ms) | 5.42 / 9.99 / 59.18 | <fill> | |
| read ops/s | 56877 | <fill> | |
| read p50 / p95 / p99 (ms) | 4.09 / 8.48 / 12.48 | <fill> | |

### tmpfs — `AUTUMN_DATA_ROOT=/dev/shm/autumn-rs`

| Metric | Baseline | This run | Δ % |
|---|---|---|---|
| write ops/s | 54535 | <fill> | |
| write p50 / p95 / p99 (ms) | 3.34 / 4.53 / 21.28 | <fill> | |
| read ops/s | 73546 | <fill> | |
| read p50 / p95 / p99 (ms) | 3.32 / 4.00 / 5.92 | <fill> | |

### tmpfs × multi-partition sweep (Step 4, optional)

| N | write ops/s | read ops/s | p99 write (ms) |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 4 | | | |
| 8 | | | |

## Verdict

- No regression? (|Δ| < 5% on ops/s, p99 within 2× baseline) — yes / no, with file:line pointers to the offending change if regressed.
- Multi-partition fix confirmed? (N=4 write ≥ 1.8× N=1) — yes / no.

## Raw logs

- `/tmp/autumn-rs-perf-disk.log`
- `/tmp/autumn-rs-perf-shm.log`
- `/tmp/autumn-rs-perf-shm-n{1,2,4,8}.log` (if Step 4 ran)
```

- [ ] **Step 6: Commit**

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/docs/audit/2026-04-21-perf-check.md
git commit -m "audit: perf_check re-run vs. committed baselines 2026-04-21"
```

---

### Task 5: Consolidated audit verdict

**Files:**
- Create: `autumn-rs/docs/audit/2026-04-21-audit-summary.md`

- [ ] **Step 1: Write the one-page verdict**

```markdown
# autumn-rs pre-extraction audit — summary & verdict (2026-04-21)

## Inputs

- Unit coverage: [2026-04-21-unit-coverage.md](./2026-04-21-unit-coverage.md)
- Integration inventory + green run: [2026-04-21-integration-inventory.md](./2026-04-21-integration-inventory.md)
- perf_check: [2026-04-21-perf-check.md](./2026-04-21-perf-check.md)

## Numbers at a glance

| Dimension | Result |
|---|---|
| Workspace unit line coverage | <fill>% |
| Crates below 60% unit coverage | <list> |
| Integration test pass rate | <N>/<N> passing, <M> ignored |
| Distributed-scenario gaps | partition, clock skew, slow replica, ENOSPC, rolling upgrade |
| perf_check disk vs baseline | <+/-X%> |
| perf_check tmpfs vs baseline | <+/-X%> |
| Multi-partition scaling (if run) | N=4 write = <Y>× N=1 |

## Extraction readiness

- [ ] No integration regressions (green suite)
- [ ] No perf regression >5% on either baseline
- [ ] Unit coverage stable (no crate newly dropped below 30%)
- [ ] Docs-gaps list recorded for post-move follow-up

## Verdict

**<GO / NO-GO>** — <one sentence rationale>.

## Follow-up TODOs that do NOT block extraction

- <gap 1>
- <gap 2>
```

- [ ] **Step 2: Commit**

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/docs/audit/2026-04-21-audit-summary.md
git commit -m "audit: autumn-rs extraction-readiness verdict 2026-04-21"
```

Gate: only proceed to Phase B if the verdict is GO. If NO-GO, stop here and surface blockers to the user.

---

## Phase B — Extraction

### Task 6: Cross-cut inventory — what lives outside autumn-rs/ that autumn-rs needs (and vice versa)

**Files:**
- Create: `autumn-rs/docs/audit/2026-04-21-extraction-crosscuts.md`

- [ ] **Step 1: Search for inbound references — parent-repo files that mention `autumn-rs`**

```bash
cd /data/dongmao_dev/autumn
grep -rn "autumn-rs" \
  --exclude-dir=autumn-rs \
  --exclude-dir=target \
  --exclude-dir=.git \
  --exclude-dir=node_modules \
  . 2>/dev/null | tee /tmp/autumn-rs-inbound.txt | wc -l
```

Expected: a few references in `CLAUDE.md`, `feature_list.md`, `claude-progress.txt`, `docs/superpowers/`, README, Makefile (if any). Record each.

- [ ] **Step 2: Search for outbound references — autumn-rs files reaching into parent**

```bash
grep -rn '\.\./\.\.' autumn-rs \
  --include="*.rs" --include="*.toml" --include="*.sh" \
  --exclude-dir=target 2>/dev/null | tee /tmp/autumn-rs-outbound.txt | wc -l
```

Expected: 0. The earlier workspace scan already showed no crate path-dep crosses the boundary. If anything surfaces (e.g. a `build.rs` referencing `../../proto`), it is a blocker — fix it before extraction.

- [ ] **Step 3: Inventory shared files that must be partitioned**

| File / dir | Action |
|---|---|
| `CLAUDE.md` (root) | Partitioned: Go rules stay, Rust rules move to `autumn-rs/CLAUDE.md` |
| `feature_list.md` (root) | Rust rows move to `autumn-rs/feature_list.md`; Go-only rows stay |
| `claude-progress.txt` (root) | Fresh copy created in `autumn-rs/claude-progress.txt` for post-extraction work |
| `docs/superpowers/` (root) | Already contains Rust-era `specs/` and `plans/` — decide: (a) keep history in parent then move via filter-repo, (b) copy, or (c) leave and link. Recommend: move under `autumn-rs/docs/superpowers/` before the filter-repo split so history follows the files. |
| `LICENSE` (root) | Copy to `autumn-rs/LICENSE` (Apache-2.0 per workspace.package) |
| `.gitignore` (root) | autumn-rs needs its own: `/target`, `perf_baseline*.json` kept, OS junk |
| `.github/` (root) | Determine which workflows apply to autumn-rs and port them |
| `README.md` (root) | Trim to Go-only; point readers to new repo |

- [ ] **Step 4: Write the crosscut report**

Create `autumn-rs/docs/audit/2026-04-21-extraction-crosscuts.md` with Steps 1–3 results verbatim plus per-row “mitigation” column and owner.

- [ ] **Step 5: Commit**

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/docs/audit/2026-04-21-extraction-crosscuts.md
git commit -m "extract: crosscut inventory for autumn-rs split"
```

---

### Task 7: Seed autumn-rs with LICENSE, .gitignore, and CI workflow

**Files:**
- Create: `autumn-rs/LICENSE`
- Create: `autumn-rs/.gitignore`
- Create: `autumn-rs/.github/workflows/ci.yml`

- [ ] **Step 1: Copy LICENSE**

```bash
cd /data/dongmao_dev/autumn
cp LICENSE autumn-rs/LICENSE
head -3 autumn-rs/LICENSE
```

Expected: Apache License 2.0 header. If the parent is not Apache-2.0 but `autumn-rs/Cargo.toml` says `license = "Apache-2.0"`, that is a real inconsistency — surface to the user before copying.

- [ ] **Step 2: Write autumn-rs/.gitignore**

```gitignore
# Build output
/target

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Local test artefacts (cluster.sh, perf_check.sh)
/tmp/
/dev/shm/

# Python bindings build output
python/target
python/*.so

# Cargo lock only for the workspace root — already tracked
# Do NOT ignore perf_baseline*.json — these are checked-in regression gates
```

- [ ] **Step 3: Write CI workflow**

Create `autumn-rs/.github/workflows/ci.yml`:

```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:

jobs:
  build-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - name: Install etcd
        run: |
          sudo apt-get update
          sudo apt-get install -y etcd-server protobuf-compiler
      - name: Format
        run: cargo fmt --all -- --check
      - name: Clippy
        run: cargo clippy --workspace --all-targets -- -D warnings
      - name: Build
        run: cargo build --workspace --exclude autumn-fuse
      - name: Unit tests
        run: cargo test --workspace --lib
      - name: Integration tests
        run: cargo test --workspace --tests -- --test-threads=1
```

- [ ] **Step 4: Local validation — the new files do not break the workspace**

```bash
cd /data/dongmao_dev/autumn/autumn-rs
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```

Expected: fmt passes silently; clippy passes with `-D warnings` or reports pre-existing warnings — if the latter, surface to user and decide whether to fix or downgrade in CI.

- [ ] **Step 5: Commit**

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/LICENSE autumn-rs/.gitignore autumn-rs/.github/
git commit -m "extract: seed LICENSE, .gitignore, CI workflow for standalone repo"
```

---

### Task 8: Move Rust-era docs and feature rows under autumn-rs/

**Files:**
- Move: `docs/superpowers/specs/2026-*.md` containing Rust-only content → `autumn-rs/docs/superpowers/specs/`
- Move: `docs/superpowers/plans/2026-*.md` (Rust-only) → `autumn-rs/docs/superpowers/plans/`
- Modify: `feature_list.md` — split into root (Go) + `autumn-rs/feature_list.md` (Rust)
- Create: `autumn-rs/claude-progress.txt`

- [ ] **Step 1: List and classify each spec / plan**

```bash
cd /data/dongmao_dev/autumn
ls docs/superpowers/specs/ docs/superpowers/plans/ docs/superpowers/diagnosis/ 2>/dev/null
```

For each file, classify: Rust-only (move), mixed (copy + edit), Go-only (leave).

- [ ] **Step 2: Move Rust-only docs with `git mv` (preserves history through filter-repo)**

For each Rust-only file:

```bash
cd /data/dongmao_dev/autumn
git mv docs/superpowers/specs/2026-04-20-perf-f099-n-ceiling.md \
       autumn-rs/docs/superpowers/specs/2026-04-20-perf-f099-n-ceiling.md
# ... repeat per file
```

Expected: `git status` shows renames.

- [ ] **Step 3: Move this very plan**

```bash
git mv docs/superpowers/plans/2026-04-21-autumn-rs-audit-and-extraction.md \
       autumn-rs/docs/superpowers/plans/2026-04-21-autumn-rs-audit-and-extraction.md
```

- [ ] **Step 4: Split feature_list.md**

Open `feature_list.md`. Every row F001–F099-* is Rust-era (the file itself says “autumn go→rust feature list”). Write the entire file to `autumn-rs/feature_list.md`, then trim the parent `feature_list.md` to a stub pointing at the new location:

```markdown
# Feature list

The Go→Rust rewrite feature list has moved to
[`autumn-rs/feature_list.md`](autumn-rs/feature_list.md).

This file intentionally left near-empty — the Go tree in this repo is
feature-complete and frozen; active feature tracking lives in the autumn-rs
repo after extraction (see `autumn-rs/CLAUDE.md`).
```

- [ ] **Step 5: Create a fresh claude-progress.txt inside autumn-rs**

```bash
cd /data/dongmao_dev/autumn
cat > autumn-rs/claude-progress.txt <<'EOF'
Date: 2026-04-21
TaskStatus: not_completed
Task scope: Pre-extraction audit (unit/integration/perf) and preparation
            for moving autumn-rs to a standalone git repository.

Current summary: Phase A audit docs published under docs/audit/.
                 Phase B crosscut inventory + LICENSE/.gitignore/CI seeded.
                 Next: migrate feature_list and Rust-era docs, then run
                 git filter-repo to produce the standalone history.

Main gaps: integration scenarios still uncovered — network partition,
           clock skew, slow replica, ENOSPC, rolling upgrade. Tracked
           in feature_list.md as post-extraction P2 items.

Next steps: execute Task 9 (filter-repo dry run) and Task 10 (push).
EOF
git add autumn-rs/claude-progress.txt
```

- [ ] **Step 6: Update autumn-rs/CLAUDE.md to include the long-task rules**

The parent `CLAUDE.md` holds the canonical long-task / feature_list / claude-progress rules. After extraction those rules must live inside autumn-rs. Open `autumn-rs/CLAUDE.md`, append a section near the top:

```markdown
## 长任务执行规则

(Verbatim copy of the "长任务执行规则" and "claude-progress.txt 约定"
sections from the parent repo's CLAUDE.md — paste exactly, so the
standalone repo carries the rules with it.)
```

- [ ] **Step 7: Commit**

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/feature_list.md feature_list.md \
        autumn-rs/claude-progress.txt autumn-rs/CLAUDE.md \
        autumn-rs/docs/superpowers/
git commit -m "extract: migrate Rust-era feature list, docs, and long-task rules under autumn-rs/"
```

---

### Task 9: Dry-run history split with `git filter-repo`

**Files:**
- No source changes — this task produces a throwaway cloned repo and a verification log.

- [ ] **Step 1: Install git-filter-repo**

```bash
pip install --user git-filter-repo
which git-filter-repo || echo "MISSING — add ~/.local/bin to PATH"
```

Expected: `git-filter-repo --version` prints a version. (On Debian: `apt install git-filter-repo`.)

- [ ] **Step 2: Create a fresh mirror clone (filter-repo refuses to run on the working repo)**

```bash
cd /tmp
rm -rf /tmp/autumn-rs-split
git clone --no-local /data/dongmao_dev/autumn /tmp/autumn-rs-split
cd /tmp/autumn-rs-split
git log --oneline | wc -l
```

Expected: commit count matches parent (`git -C /data/dongmao_dev/autumn log --oneline | wc -l`).

- [ ] **Step 3: Run filter-repo with --subdirectory-filter**

```bash
cd /tmp/autumn-rs-split
git filter-repo --subdirectory-filter autumn-rs --force
git log --oneline | wc -l
git log --oneline | tail -5
```

Expected: commit count drops (only commits touching `autumn-rs/` survive). The `tail -5` should show `79a30dd` family (first rust commits) but with paths rewritten to repo-root.

Verify no parent-only files leaked:

```bash
test ! -e go.mod && test ! -e autumn_clientv1 && test ! -e manager && echo "CLEAN"
ls | head -20   # expect: CLAUDE.md, Cargo.toml, Cargo.lock, crates/, cluster.sh, perf_check.sh, README.md, LICENSE, .gitignore, .github/, python/, scripts/, examples/, docs/
```

Expected: `CLEAN` printed; `ls` shows only autumn-rs contents.

- [ ] **Step 4: Build and test the filtered clone**

```bash
cd /tmp/autumn-rs-split
cargo build --workspace --exclude autumn-fuse 2>&1 | tail -3
cargo test --workspace --lib 2>&1 | tail -3
cargo test --workspace --tests -- --test-threads=1 2>&1 | tail -3
```

Expected: all three `Finished`/`test result: ok.`. If anything fails here and not in the original repo, an absolute path escaped somewhere — grep the failing file for `/data/dongmao_dev/autumn` and fix before proceeding.

- [ ] **Step 5: Run perf_check on the filtered clone**

```bash
cd /tmp/autumn-rs-split
./perf_check.sh --shm 2>&1 | tail -20
```

Expected: passes regression gate against `perf_baseline_shm.json`.

- [ ] **Step 6: Record dry-run evidence**

Append to `autumn-rs/docs/audit/2026-04-21-extraction-crosscuts.md`:

```markdown
## filter-repo dry run — 2026-04-21

**Source:** `/data/dongmao_dev/autumn` @ <HEAD sha>
**Output:** `/tmp/autumn-rs-split`
**Commits before:** <N>
**Commits after:** <M>
**First commit preserved:** <sha + subject>
**Build:** pass
**Unit tests:** <pass/N>
**Integration tests:** <pass/N>
**perf_check --shm:** <pass/fail, ops/s>
```

Commit the note:

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/docs/audit/2026-04-21-extraction-crosscuts.md
git commit -m "extract: filter-repo dry-run passes build+test+perf"
```

---

### Task 10: Push to the new remote (pauses for user input)

**Files:**
- No source changes.

- [ ] **Step 1: Get the target remote URL from the user**

Ask: "What is the new repo URL (e.g. `git@github.com:<org>/autumn-rs.git`)? Who should the initial push go to — you manually, or may I push on your behalf?" Wait for answer. Do not guess.

- [ ] **Step 2: Re-run filter-repo against a freshly cloned mirror**

The `/tmp/autumn-rs-split` from Task 9 may be stale if Tasks 6–8 added commits. Re-do cleanly:

```bash
rm -rf /tmp/autumn-rs-split
cd /data/dongmao_dev/autumn
git clone --no-local . /tmp/autumn-rs-split
cd /tmp/autumn-rs-split
git filter-repo --subdirectory-filter autumn-rs --force
```

- [ ] **Step 3: Rename the default branch if needed**

The parent working branch is `compio`. The new repo's default should be `main`. Inside `/tmp/autumn-rs-split`:

```bash
git branch -m compio main 2>/dev/null || true
git branch   # expect: * main
```

- [ ] **Step 4: Wire up the new remote and push**

Substitute `<NEW_REMOTE_URL>` with the value from Step 1.

```bash
cd /tmp/autumn-rs-split
git remote add origin <NEW_REMOTE_URL>
git push -u origin main
# Only after user confirms the remote is correct
```

Expected: push accepts, remote now has the full history. If the user is doing the push themselves, print the command and stop.

- [ ] **Step 5: Tag the first release**

```bash
git tag -a v0.1.0 -m "autumn-rs: first standalone release (extracted from autumn monorepo)"
git push origin v0.1.0
```

- [ ] **Step 6: Verify CI runs green on the new remote**

Open the new repo’s Actions tab in a browser (or `gh run list --repo <owner>/<repo>`) and confirm the `CI` workflow from Task 7 is green on the first push.

- [ ] **Step 7: Record the push**

Append to `autumn-rs/docs/audit/2026-04-21-extraction-crosscuts.md`:

```markdown
## Push to new remote — <yyyy-mm-dd>

**Remote:** <URL>
**Branch:** main
**Tag:** v0.1.0
**CI first run:** <green / red + link>
```

Commit:

```bash
cd /data/dongmao_dev/autumn
git add autumn-rs/docs/audit/2026-04-21-extraction-crosscuts.md
git commit -m "extract: push autumn-rs history to new remote + v0.1.0 tag"
```

---

### Task 11: Retire autumn-rs from the parent repo

**Files:**
- Delete: `autumn-rs/` (entire subtree) OR convert to submodule pointing at the new remote (user choice)
- Modify: `CLAUDE.md`, `README.md`, `feature_list.md` — update to reflect the split
- Modify: `.gitignore` — drop `!perf_baseline.json` override (was for autumn-rs only)

- [ ] **Step 1: Ask the user to choose the retirement mode**

Three options — pause and wait for choice:

1. **Full removal:** `git rm -r autumn-rs`, parent repo no longer contains Rust code.
2. **Submodule:** `git submodule add <NEW_REMOTE_URL> autumn-rs`, parent can still compile bits if desired.
3. **README-only redirect:** Leave current directory untouched, add a big “MOVED — see <URL>” banner in `autumn-rs/README.md`. Only pick this if the monorepo is still actively dual-maintained for a transition period.

Most teams pick #1 once CI on the new repo is green. Do not default — ask.

- [ ] **Step 2a (if option 1): Remove the subtree**

```bash
cd /data/dongmao_dev/autumn
git rm -r autumn-rs
```

- [ ] **Step 2b (if option 2): Replace with submodule**

```bash
cd /data/dongmao_dev/autumn
git rm -r autumn-rs
git commit -m "extract: remove autumn-rs subtree in favor of submodule"
git submodule add <NEW_REMOTE_URL> autumn-rs
```

- [ ] **Step 3: Update parent CLAUDE.md**

Replace the “项目目标” section’s Rust-rewrite rules with a redirect note and drop the `长任务执行规则` (they now live only in autumn-rs/CLAUDE.md — or keep them if the parent Go repo also uses them; user decides). At minimum, add:

```markdown
> **autumn-rs (Rust rewrite) moved to its own repository at <URL> as of
> 2026-04-21.** This `autumn` repo is now Go-only and frozen except for
> maintenance fixes.
```

- [ ] **Step 4: Update parent README.md**

Add a banner at the very top:

```markdown
> **Note:** The Rust rewrite of this project now lives at <URL>.
> Active development continues there; this repository holds the
> original Go implementation and is frozen.
```

- [ ] **Step 5: Update parent feature_list.md**

Already stubbed in Task 8 Step 4 — verify it still reads correctly after autumn-rs/ removal (i.e., the link now points at the new remote, not a local path):

```markdown
# Feature list

Active feature tracking for the Rust rewrite has moved to the standalone
[autumn-rs repository](<NEW_REMOTE_URL>/blob/main/feature_list.md).
```

- [ ] **Step 6: Clean parent .gitignore**

Drop the `!perf_baseline.json` exception — it existed for autumn-rs only:

```bash
cd /data/dongmao_dev/autumn
# edit .gitignore, remove the "Override root *.json exclusion..." lines
```

- [ ] **Step 7: Final build check on parent**

The parent is a Go repo. Confirm it still builds:

```bash
cd /data/dongmao_dev/autumn
go build ./... 2>&1 | tail -10
```

Expected: no errors. If anything in the Go tree referenced `autumn-rs/` (unlikely but possible via scripts), fix the reference.

- [ ] **Step 8: Commit the cut-over**

```bash
cd /data/dongmao_dev/autumn
git add -A
git commit -m "extract: retire autumn-rs subtree — moved to standalone repo <URL>"
```

- [ ] **Step 9: Push parent (ask user first — this is a visible change)**

Do NOT push without explicit user confirmation. Print the command and wait:

```bash
# Pending user OK:
git push origin compio
```

---

### Task 12: Post-extraction smoke test

**Files:**
- No source changes — pure verification.

- [ ] **Step 1: Fresh clone of the new repo in a clean dir**

```bash
rm -rf /tmp/autumn-rs-verify
git clone <NEW_REMOTE_URL> /tmp/autumn-rs-verify
cd /tmp/autumn-rs-verify
git log --oneline | wc -l
```

Expected: matches the post-filter commit count from Task 9/10.

- [ ] **Step 2: Build release + unit + integration on the clone**

```bash
cd /tmp/autumn-rs-verify
cargo build --workspace --release --exclude autumn-fuse 2>&1 | tail -3
cargo test --workspace --lib 2>&1 | tail -3
cargo test --workspace --tests -- --test-threads=1 2>&1 | tail -3
```

Expected: all three `ok`.

- [ ] **Step 3: perf_check green on the clone**

```bash
cd /tmp/autumn-rs-verify
./perf_check.sh --shm 2>&1 | tail -20
```

Expected: passes regression gate.

- [ ] **Step 4: Manual README walkthrough**

Follow the "Quick Start: 1-replica cluster" section of `README.md` top-to-bottom from the clone. Every command must work verbatim on a machine with just the listed prerequisites (protoc, etcd, Rust).

- [ ] **Step 5: File the handoff summary**

Open a PR / issue on the new repo titled "Post-extraction smoke test 2026-04-21" documenting:
- commit count matches
- all tests green
- perf_check within baseline
- README walkthrough succeeds

Close the extraction loop by updating `claude-progress.txt` inside the new repo:

```txt
Date: <date>
TaskStatus: completed
Task scope: autumn-rs extraction from autumn monorepo — verified
            post-move on a fresh clone.

Current summary: Phase A audit and Phase B extraction complete.
                 New repo at <URL> is the single source of truth.
                 Parent autumn repo's autumn-rs/ tree <removed /
                 replaced with submodule>.

Main gaps: none blocking. Post-extraction P2 items in feature_list.md.

Next steps: decide on active branch strategy (main vs. develop) and
            kick off next feature in the standalone repo.
```

Commit + push in the new repo.

---

## Self-review

**Spec coverage:**
- Unit-test coverage audit → Task 1 ✓
- Distributed-system integration coverage audit → Tasks 2, 3 ✓
- perf_check result review → Task 4 ✓
- Preparation to move autumn-rs to another git repo → Tasks 6–12 ✓

**Placeholder scan:** All report templates include explicit `<fill>` markers that are data, not implementation directions. Every shell command is exact. No `TBD` in action steps.

**Type / command consistency:**
- `cargo llvm-cov` commands used identically in Tasks 1 & pre-gate.
- `cargo test --workspace --tests -- --test-threads=1` reused verbatim in Tasks 3, 9, 12.
- `perf_check.sh` and `perf_check.sh --shm` used identically across Tasks 4, 9, 12.
- `git filter-repo --subdirectory-filter autumn-rs --force` used identically in Tasks 9 & 10.
- File paths (`autumn-rs/docs/audit/2026-04-21-*.md`) match between the "Files" headers and the body commands.

**Decision points flagged for user (do not pre-commit):**
- Task 7 Step 1: is the parent repo actually Apache-2.0? — surfaced inline.
- Task 8 Step 1: classify each `docs/superpowers/` file by era before moving.
- Task 10 Step 1: new remote URL and who pushes.
- Task 11 Step 1: retirement mode (full remove vs. submodule vs. redirect-only).
- Task 11 Step 9: explicit confirmation before `git push` on parent.

**Scope check:** The plan is one feature ("extract autumn-rs cleanly") with audit gates up front. It does not try to also close the integration-coverage gaps (partition, skew, slow replica, ENOSPC, rolling upgrade) — those are documented as post-extraction P2 work in Task 8's feature_list stub, not folded into this plan.
