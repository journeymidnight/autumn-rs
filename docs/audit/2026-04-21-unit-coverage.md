# autumn-rs unit-test coverage — 2026-04-21

**Tool:** cargo-llvm-cov 0.8.5 (lib + bin units only, no tests/ integration)

**Commands (run from `autumn-rs/`):**

```
# Step 1 — JSON summary (produces the raw table below)
cargo llvm-cov --workspace --lib \
  --exclude autumn-fuse --exclude autumn-manager \
  --no-fail-fast \
  --json --output-path /tmp/autumn-rs-unit-cov.json

# Step 2 — HTML report
cargo llvm-cov --workspace --lib \
  --exclude autumn-fuse --exclude autumn-manager \
  --no-fail-fast \
  --html --output-dir /tmp/autumn-rs-unit-cov-html
```

**Snapshot JSON:** `/tmp/autumn-rs-unit-cov.json`
**HTML report:** `/tmp/autumn-rs-unit-cov-html/html/index.html`
(cargo-llvm-cov places HTML files in an `html/` subdirectory inside `--output-dir`)

**Exclusions and workarounds:**
- `autumn-fuse` excluded: libfuse3/libfuse not installed on this system (pkg-config cannot find `fuse3.pc` or `fuse.pc`).
- `autumn-manager` excluded from `--lib` test compilation: tests call private methods (`handle_register_node`, `handle_register_ps`, `handle_upsert_partition`, `handle_get_regions`, `handle_heartbeat_ps`) that are no longer `pub`. This is a test-visibility bug — the handlers were likely made private in a refactor without updating the test module. The library itself compiles fine; only the `#[cfg(test)]` block fails. The `autumn-manager` lib is therefore measured at 0% coverage in this snapshot.
- `autumn-partition-server` had 2 failing tests (`f099i_d1_fast_path_no_fu_allocation`, `f099i_fast_path_inactive_under_batch`) but coverage data was still collected. The failures appear to be a timing/counter race in fast-path detection tests.

## Workspace totals

(Excludes autumn-fuse and autumn-manager from the measurement; autumn-server is bin-only and has no lib target.)

| Metric | Covered | Total | % |
|---|---|---|---|
| Lines | 4,653 | 11,942 | 38.96% |
| Regions | 8,758 | 20,218 | 43.32% |
| Functions | 465 | 1,300 | 35.77% |

## Per-crate (ranked by line coverage %)

| Crate | Lines % | Functions % | Key uncovered modules |
|---|---|---|---|
| autumn-client | 0.0% (0/552) | 0.0% (0/103) | entire crate — no unit tests at all |
| autumn-etcd | 12.6% (59/470) | 10.5% (8/76) | `transport.rs` (0%), most of `lib.rs` (14%) |
| autumn-stream | 20.6% (926/4488) | 18.8% (98/522) | `extent_node.rs` (0%), `extent_rpc.rs` (0%), `conn_pool.rs` (0%), `client.rs` (6.7%) |
| autumn-partition-server | 55.4% (3075/5548) | 58.0% (276/476) | `rpc_handlers.rs` (0%), `background.rs` (20%) |
| autumn-common | 59.4% (38/64) | 55.6% (5/9) | `metrics.rs` (0%) |
| autumn-rpc | 67.7% (555/820) | 68.4% (78/114) | `pool.rs` (0%), `error.rs` (19%), `partition_rpc.rs` (33%) |
| | | | |
| **— excluded / unmeasured —** | | | |
| autumn-manager | N/A (excluded) | N/A | test-visibility compile error — private handler methods called from tests |
| autumn-fuse | N/A (excluded) | N/A | libfuse3 not installed on this host |
| autumn-server | N/A (no lib target) | N/A | bin-only crate; no lib to measure |

## Top-10 largest uncovered files

Files with the biggest absolute number of uncovered lines:

1. `stream/src/extent_node.rs`: 1892 uncovered / 1892 total (0.0%)
2. `partition-server/src/lib.rs`: 1273 uncovered / 3001 total (57.6%)
3. `stream/src/client.rs`: 1269 uncovered / 1360 total (6.7%)
4. `partition-server/src/background.rs`: 814 uncovered / 1017 total (20.0%)
5. `client/src/lib.rs`: 552 uncovered / 552 total (0.0%)
6. `partition-server/src/rpc_handlers.rs`: 278 uncovered / 278 total (0.0%)
7. `etcd/src/lib.rs`: 249 uncovered / 290 total (14.1%)
8. `stream/src/extent_rpc.rs`: 221 uncovered / 221 total (0.0%)
9. `etcd/src/transport.rs`: 158 uncovered / 158 total (0.0%)
10. `stream/src/conn_pool.rs`: 111 uncovered / 111 total (0.0%)

## Observations

- **Crates below 60% line coverage:** autumn-client (0%), autumn-etcd (12.6%), autumn-stream (20.6%), autumn-partition-server (55.4%), autumn-common (59.4%). That is 5 of the 6 measured crates below 60%.

- **Files with <30% coverage and >500 LoC:**
  - `stream/src/extent_node.rs` (1892 lines, 0%): the entire ExtentNode server implementation has zero test coverage. This is the core of the stream layer. It is exclusively exercised by integration/end-to-end tests (or not yet tested). Any `unsafe` bugs here are completely invisible to unit tests.
  - `stream/src/client.rs` (1360 lines, 6.7%): stream client logic almost entirely uncovered.
  - `partition-server/src/lib.rs` (3001 lines, 57.6%): large file but only the SSTable and memtable paths are covered; the RPC dispatch, compaction scheduler, and WAL replay paths are not.
  - `partition-server/src/background.rs` (1017 lines, 20.0%): compaction background tasks, pipeline scheduler, flush loop — all largely untested.

- **Unsafe blocks uncovered:** There are 20 `unsafe` usages in the codebase (excluding comments and `#[allow]` attributes). The majority (8 occurrences) are in `stream/src/extent_node.rs`, which is at 0% coverage — these `unsafe` UnsafeCell dereferences for per-shard file handles are **completely uncovered** by unit tests. Two more `unsafe { rkyv::from_bytes_unchecked }` calls are in `stream/src/extent_rpc.rs` (0% coverage) and `rpc/src/manager_rpc.rs` (48% coverage). The `unsafe` blocks in `manager/src/lib.rs` (raw pointer coercion for connection pooling) are also untested due to the manager test-visibility bug. In total, the majority of `unsafe` code in this workspace is uncovered.

- **Test failures to fix before next snapshot:**
  - `autumn-partition-server::f099i_d1_fast_path_no_fu_allocation` — assertion on fast-path counter (11 != 10); likely a test isolation issue with shared static counter.
  - `autumn-partition-server::f099i_fast_path_inactive_under_batch` — mutex poison from the above failure.
  - `autumn-manager` — private method access in `#[cfg(test)]` block; needs either `pub(crate)` visibility or test restructuring.

## Raw summary output

```
Filename                                     Regions    Missed Regions     Cover   Functions  Missed Functions  Executed       Lines      Missed Lines     Cover    Branches   Missed Branches     Cover
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
client/src/lib.rs                                866               866     0.00%         103               103     0.00%         552               552     0.00%           0                 0         -
common/src/metrics.rs                             17                17     0.00%           3                 3     0.00%          16                16     0.00%           0                 0         -
common/src/store.rs                               77                 6    92.21%           6                 1    83.33%          48                10    79.17%           0                 0         -
etcd/src/lib.rs                                  386               312    19.17%          50                44    12.00%         290               249    14.14%           0                 0         -
etcd/src/proto.rs                                 44                 7    84.09%           3                 1    66.67%          22                 4    81.82%           0                 0         -
etcd/src/transport.rs                            212               212     0.00%          23                23     0.00%         158               158     0.00%           0                 0         -
partition-server/src/background.rs              1910              1509    20.99%          77                56    27.27%        1017               814    19.96%           0                 0         -
partition-server/src/lib.rs                     5074              1899    62.57%         246               106    56.91%        3001              1273    57.58%           0                 0         -
partition-server/src/rpc_handlers.rs             591               591     0.00%          33                33     0.00%         278               278     0.00%           0                 0         -
partition-server/src/sstable/bloom.rs            162                 4    97.53%          10                 0   100.00%          88                 2    97.73%           0                 0         -
partition-server/src/sstable/builder.rs          408                 4    99.02%          18                 0   100.00%         217                 3    98.62%           0                 0         -
partition-server/src/sstable/format.rs           512                81    84.18%          13                 1    92.31%         247                44    82.19%           0                 0         -
partition-server/src/sstable/iterator.rs        1081                26    97.59%          65                 0   100.00%         585                27    95.38%           0                 0         -
partition-server/src/sstable/reader.rs           176                37    78.98%          14                 4    71.43%         115                32    72.17%           0                 0         -
rpc/src/client.rs                                693               178    74.31%          56                11    80.36%         393               109    72.26%           0                 0         -
rpc/src/error.rs                                  42                29    30.95%           4                 3    25.00%          31                25    19.35%           0                 0         -
rpc/src/frame.rs                                 309                17    94.50%          20                 2    90.00%         166                12    92.77%           0                 0         -
rpc/src/manager_rpc.rs                            42                21    50.00%           5                 3    40.00%          31                16    48.39%           0                 0         -
rpc/src/partition_rpc.rs                          36                29    19.44%           1                 0   100.00%          12                 8    33.33%           0                 0         -
rpc/src/pool.rs                                  100               100     0.00%          15                15     0.00%          69                69     0.00%           0                 0         -
rpc/src/server.rs                                199                45    77.39%          13                 2    84.62%         118                26    77.97%           0                 0         -
stream/src/client.rs                            1920              1788     6.88%         154               146     5.19%        1360              1269     6.69%           0                 0         -
stream/src/conn_pool.rs                          156               156     0.00%          20                20     0.00%         111               111     0.00%           0                 0         -
stream/src/erasure.rs                            545                34    93.76%          45                10    77.78%         269                33    87.73%           0                 0         -
stream/src/extent_node.rs                       3069              3069     0.00%         219               219     0.00%        1892              1892     0.00%           0                 0         -
stream/src/extent_rpc.rs                         352               352     0.00%          26                26     0.00%         221               221     0.00%           0                 0         -
stream/src/wal.rs                               1239                71    94.27%          58                 3    94.83%         635                36    94.33%           0                 0         -
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTAL                                          20218             11460    43.32%        1300               835    35.77%       11942              7289    38.96%           0                 0         -
```
