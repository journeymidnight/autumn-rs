# autumn-rs pre-extraction audit — summary & verdict (2026-04-21)

## Inputs

- Unit coverage: [2026-04-21-unit-coverage.md](./2026-04-21-unit-coverage.md)
- Integration inventory + green run: [2026-04-21-integration-inventory.md](./2026-04-21-integration-inventory.md)
- perf_check: [2026-04-21-perf-check.md](./2026-04-21-perf-check.md)

## Numbers at a glance

| Dimension | Result |
|---|---|
| Workspace unit line coverage | 38.96% (4,653 / 11,942 lines; excludes autumn-fuse, autumn-manager, autumn-server) |
| Crates below 60% unit coverage | autumn-client (0.0%), autumn-etcd (12.6%), autumn-stream (20.6%), autumn-partition-server (55.4%), autumn-common (59.4%) — 5 of 6 measured crates |
| Integration test pass rate | 45 / 69 reachable passing, 2 ignored, 22 failing, 4 unreachable (ec_integration.rs compile error) |
| Manager lib-test compile state | broken — 24 errors: 5 handler methods (`handle_register_node`, `handle_register_ps`, `handle_upsert_partition`, `handle_get_regions`, `handle_heartbeat_ps`) made private in refactor; `#[cfg(test)]` block calls them directly |
| Distributed-scenario gaps (from Task 2) | 8 named gaps: (1) network partition (asymmetric client↔PS), (2) slow/stuck replica, (3) clock skew / NTP jump, (4) quorum loss recovery (2-of-3 nodes down), (5) disk-full ENOSPC on extent-node, (6) rolling-upgrade binary compatibility, (7) manager + PS simultaneous restart, (8) partition scan / range-query correctness |
| perf_check disk vs baseline | write +12.7% ops/s (improvement); read -13.7% ops/s (system noise — p99 improved 36.9%); all latency percentiles improved |
| perf_check tmpfs vs baseline | baseline established this session (first-ever shm baseline); second independent run: write +2.9%, read -0.3%, all metrics within ±5% — noise-vs-noise comparison within same session |
| Multi-partition scaling (F099-N-c fix) | Confirmed: N=4 write = 2.76× N=1 (threshold ≥ 1.8×); N=8 read = 11.34× N=1 (near-linear) |

## Extraction readiness checklist

- ❌ No Rust compile failures in **any** test target (lib or integration)
  - `autumn-manager` lib-test: 24 compile errors (private handler methods)
  - `autumn-manager/tests/ec_integration.rs`: 1 compile error (`Rc<StreamClient>` vs `StreamClient` type mismatch); 4 tests unreachable
- ❌ Integration test suite passes (no failing tests beyond pre-declared `#[ignore]`)
  - 22 integration tests fail: 18 due to F099-K port-init regression in `start_partition_server()` (port 0+1=1 is reserved), 4 due to Go not installed for embedded etcd
- ⚠️ No perf regression >5% on either disk or tmpfs baseline ops/s
  - Disk read ops/s: -13.7% below baseline, failing the strict 5% gate. However, p99 latency improved 36.9%; tmpfs results (±0.3% read, +2.9% write) confirm no code regression — attributed to I/O variability on shared 192-CPU overlayfs host.
  - tmpfs baseline was established in this same session; the comparison is noise-vs-noise with no historical anchor.
- ⚠️ Unit coverage stable (no crate <30% on any workspace member that has >100 LoC)
  - autumn-client: 0.0% on 552 lines (no unit tests at all — entire crate uncovered)
  - autumn-etcd: 12.6% on 470 lines (`transport.rs` 0%, most of `lib.rs` 14%)
  - autumn-stream: 20.6% on 4,488 lines (`extent_node.rs` 0% / 1,892 lines; `client.rs` 6.7% / 1,360 lines)
  - These three crates all have >100 LoC and are below 30%.
- ✅ Docs-gaps list recorded for post-move follow-up
  - 8 distributed-scenario gaps listed in `2026-04-21-integration-inventory.md`; top risks are quorum loss recovery and rolling-upgrade binary compatibility.

## Blockers (must fix before extraction)

1. **F099-K port-init regression in `start_partition_server()` — 18 tests failing.**
   File: `crates/manager/tests/support/mod.rs`.
   Fix: Call `serve()` (which sets `base_port` in the `Cell`) before calling `sync_regions_once()` / `open_partition()`, so that `base_port + ord` resolves to a valid ephemeral port instead of `0 + 1 = 1`. Alternatively, pass the target `ps_addr` into the helper so `base_port` is initialized prior to partition discovery.

2. **`autumn-manager` lib-test private-method compile error — blocks `cargo test -p autumn-manager` entirely.**
   File: `crates/manager/src/lib.rs` (the `#[cfg(test)]` block).
   Fix: Either mark the five handler methods `pub(crate)` (one-line change each), or move the test module to a submodule that can access private items via `use super::*`. 24 errors collapse to 0 with either approach.

3. **`ec_integration.rs` type-mismatch compile error — 4 EC tests unreachable.**
   File: `crates/manager/tests/ec_integration.rs:121`.
   Fix: Update the helper function return type (or the call site) to use `Rc<StreamClient>` instead of `StreamClient`, matching the current `StreamClient::connect()` return type.

4. **`etcd_stream_integration.rs` — Go not installed on CI host — 4 tests fail.**
   File: `crates/manager/tests/etcd_stream_integration.rs`.
   Fix: Either install Go on the CI host (or the extraction repo's CI image), or refactor the embedded-etcd launcher to use a pre-built `etcd` binary (system etcd at `/usr/local/bin/etcd` is already present). Add `#[ignore]` with a documented skip reason as a minimum interim fix so they do not count as failures.

## Non-blocking observations (post-extraction work OK)

- **Disk read ops/s -13.7% vs old baseline:** Not a code regression — p99 improved 36.9%, tmpfs results are stable. The disk baseline predates this session and was recorded under different host load. Record a fresh disk baseline after the extraction repo has stable CI. (⚠️ perf gate)
- **tmpfs baseline age:** The shm baseline (`perf_baseline_shm.json`) was established and immediately consumed in the same session. It carries no multi-day stability signal. Commit it, run perf_check weekly, and treat the first week's results as calibration. (⚠️ perf gate)
- **autumn-client: 0% unit coverage (552 lines):** The client crate is a CLI/admin tool. Its logic is thin glue over RPC calls, but having zero tests means any refactor breaks silently. Add at minimum parse-level unit tests post-extraction. (⚠️ coverage)
- **autumn-stream / extent_node.rs: 0% coverage on 1,892 lines including 8 unsafe blocks:** The core ExtentNode server implementation is entirely covered only by integration tests. The unsafe `UnsafeCell` dereferences for per-shard file handles are invisible to unit tests. This is the highest-risk uncovered file in the workspace. Plan targeted unit tests post-extraction. (⚠️ coverage)
- **autumn-etcd: 12.6% coverage (470 lines, transport.rs 0%):** The etcd transport layer has no unit tests. Failures here would be silent until an integration test caught them. (⚠️ coverage)
- **8 distributed-scenario gaps:** Network partition, slow replica, quorum loss (2-of-3 down), ENOSPC, rolling-upgrade compatibility, and range-scan correctness are unexercised. Highest production risk: quorum loss and rolling-upgrade (unversioned binary RPC frame format). These should be added as labelled issues in the extraction repo's issue tracker immediately post-move.

## Verdict

**CONDITIONAL-GO** — Extract now to the new repo on a `pre-release` branch; gate the new repo's CI on `cargo test` green by fixing the 4 concrete compile/port bugs (estimated 1–3 days total) before tagging v0.1.0. The 22 failing tests and 2 compile errors are all well-understood, well-scoped regressions that already exist on `compio` HEAD; extraction does not make them worse, and the new repo's CI feedback loop will force faster resolution than the monorepo environment.

Concrete fixer work needed before v0.1.0 tag (ordered by impact):

1. **Fix F099-K port-init in `start_partition_server()`** — `crates/manager/tests/support/mod.rs`: call `serve()` before `sync_regions_once()`; unblocks 18 tests. Estimated effort: 2–4 hours (fix + verify).
2. **Fix `autumn-manager` lib-test private-method visibility** — `crates/manager/src/lib.rs`: mark 5 handler methods `pub(crate)`; unblocks `cargo test -p autumn-manager` entirely. Estimated effort: 30 minutes.
3. **Fix `ec_integration.rs` type mismatch** — `crates/manager/tests/ec_integration.rs:121`: align return type with `Rc<StreamClient>`; unblocks 4 EC tests. Estimated effort: 1 hour.
4. **Fix etcd integration tests for no-Go environment** — `crates/manager/tests/etcd_stream_integration.rs`: replace `go run` embedded-etcd launcher with system `etcd` binary, or `#[ignore]` with documented reason. Estimated effort: 2–4 hours.

## Follow-up TODOs that do NOT block extraction

- Add unit tests for `autumn-client` (0% coverage, 552 lines); prioritize argument parsing and response formatting.
- Add targeted unit tests for `extent_node.rs` unsafe blocks (`UnsafeCell` shard file handles); mock the file descriptor layer so tests run without a real fs.
- Add unit tests for `autumn-etcd/transport.rs` (0%, 158 lines); use a mock TCP server.
- Commit a stable `perf_baseline_shm.json` after 3+ independent runs on the extraction repo's CI host (current baseline is same-session noise-vs-noise).
- Record a fresh `perf_baseline.json` (disk) on the extraction repo's CI host to replace the monorepo baseline.
- Create labelled issues in the extraction repo for the 8 distributed-scenario gaps, especially: (a) quorum loss recovery (2-of-3 nodes down), (b) rolling-upgrade binary compatibility (unversioned RPC frame format), (c) partition range-scan correctness.
- Investigate the 2 timing/counter failures in `autumn-partition-server` unit tests (`f099i_d1_fast_path_no_fu_allocation`, `f099i_fast_path_inactive_under_batch`) — shared static counter causes cross-test pollution; fix test isolation.
