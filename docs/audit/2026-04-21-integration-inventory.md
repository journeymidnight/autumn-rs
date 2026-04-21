# autumn-rs distributed-system integration coverage — 2026-04-21

**Scope:** `crates/*/tests/*.rs` — out-of-process integration / system tests
that spin up manager + extent-nodes + (optionally) PS via real RPC.

> **Note on test annotation style:** The stream crate uses `#[compio::test]`
> (async, runs in a compio runtime) in addition to the standard `#[test]`.
> Both are counted as integration tests throughout this document.

---

## Scenario × test matrix

| Scenario | Current coverage (files / fns) | Status |
|---|---|---|
| **Manager** | | |
| etcd-backed metadata round-trip | `manager/tests/etcd_stream_integration.rs` (4) — `stream_manager_with_real_etcd`, `etcd_replay_owner_lock_allows_check_commit_length_without_reacquire`, `etcd_replicated_append_and_recovery_flow`, `etcd_election_and_replay_on_second_manager` | covered |
| Manager leader failover | `manager/tests/system_manager_failover.rs` (2) — `manager_failover_preserves_streams_and_partitions`, `manager_crash_during_split_state_consistent` | covered |
| Manager crash mid-allocation | `manager/tests/integration.rs` — `stream_manager_alloc_and_truncate_flow` (exercises alloc path; no mid-alloc crash injection) | partial — no fault-injection |
| **Extent-node** | | |
| Extent-node restart recovery | `stream/tests/extent_restart_recovery.rs` (3) — `restart_preserves_commit_length`, `restart_preserves_meta_fields`, `restart_extent_remains_writable` | covered |
| Extent-node crash mid-append (WAL replay) | `stream/tests/wal_recovery.rs` (3) — `wal_replay_recovers_truncated_extent`, `large_write_bypasses_wal`, `wal_replay_multiple_appends` | covered |
| Extent-node failover (quorum replacement) | `manager/tests/system_extent_failover.rs` (2) — `extent_node_unreachable_stream_client_retries_on_new_extent`, `alloc_extent_falls_back_on_dead_node`; `manager/tests/system_extent_recovery.rs` (2) — `extent_recovery_replaces_dead_node`, `recovery_dispatch_skips_healthy_sealed_extents` | covered |
| Multi-disk round-robin | `stream/tests/f021_multi_disk.rs` (3) — `f021_multi_disk_alloc`, `f021_multi_disk_load_extents`, `f021_df_reports_per_disk_stats` | covered |
| Per-shard runtime sharding (F099-M) | `stream/tests/f099m_shards.rs` (4) — `f099m_shards_serve_disjoint_extents`, `f099m_register_node_reports_shard_ports`, `f099m_client_routes_by_extent_id_modulo`, `f099m_recovery_per_shard` | covered |
| Extent-append pipeline (SQ/CQ) | `stream/tests/stream_sqcq.rs` (5) — `concurrent_append_preserves_order_within_stream`, `worker_handles_back_pressure`, `cq_advances_commit_on_out_of_order_completion`, `sq_continues_submitting_while_cq_drains`, `parallel_fanout_fires_3_replicas_concurrently`; `stream/tests/extent_pipeline.rs` (5) — `concurrent_appends_preserve_offset_order_per_extent`, `appends_to_different_extents_run_concurrently`, `seal_rejects_subsequent_appends`, `pwritev_batch_still_coalesced`, `cq_flushes_fast_ops_while_slow_op_runs`; `stream/tests/extent_append_semantics.rs` (2) — `append_rejects_stale_revision`, `append_with_mid_byte_commit_truncates_and_succeeds` | covered |
| **Partition-server** | | |
| PS crash → reassign → resume | `manager/tests/system_ps_failover.rs` (2) — `ps_crash_partition_reassigned_to_new_ps`, `ps_heartbeat_timeout_triggers_reassignment` | covered |
| PS restart replays metaStream+logStream | `manager/tests/system_ps_recovery.rs` (2) — `ps_crash_unflushed_data_recoverable`, `sequential_ps_crash_data_accumulates`; also `manager/tests/integration.rs` — `partition_server_recovery_replays_table_and_wal`, `f030_recovery_from_meta_and_row_streams`, `f031_recovery_replays_log_stream` | covered |
| Partition split (normal) | `manager/tests/system_split_writes.rs` (1) — `split_preserves_all_data`; `manager/tests/system_split_overlap.rs` (1) — `split_overlap_compaction_enables_second_split`; `manager/tests/system_split_ref_counting.rs` (1) — `split_ref_counting_shared_extents_freed_after_both_gc`; `manager/tests/integration.rs` — `partition_server_put_get_and_split_flow`, `f037_overlap_detected_after_split_and_cleared_by_compaction` | covered |
| Partition split with large values (VP) | `manager/tests/system_split_large_values.rs` (1) — `split_with_large_values_preserves_vp_resolution` | covered |
| Seal-during-writes race | `manager/tests/system_seal_during_writes.rs` (1) — `seal_during_active_writes_client_retries` | covered |
| `LockedByOther` revision fencing | `manager/tests/system_locked_by_other.rs` (1) — `owner_lock_fencing_rejects_stale_revision` | covered |
| Compound failure (multi-component) | `manager/tests/system_compound_failures.rs` (1) — `split_then_ps_crash_data_survives` | covered |
| PS flush → SSTable on rowStream | `manager/tests/integration.rs` (2) — `f030_flush_writes_sst_to_row_stream`, `f030_recovery_from_meta_and_row_streams` | covered |
| PS compaction (major) | `manager/tests/integration.rs` (3) — `f029_compaction_merges_small_tables`, `f031_compaction_preserves_value_pointers`, `f037_overlap_detected_after_split_and_cleared_by_compaction` | covered |
| Value-pointer (VP) large-value separation | `manager/tests/integration.rs` (2) — `f031_large_value_stored_in_log_stream`, `f031_recovery_replays_log_stream` | covered |
| GC reclaims log-stream extents | `manager/tests/integration.rs` (1) — `f033_gc_reclaims_log_stream_extents` | covered |
| **Erasure coding** | | |
| EC encode/decode correctness | `manager/tests/ec_integration.rs` (4) — `ec_policy_stream_write_read_roundtrip`, `ec_policy_stream_multiple_appends`, `ec_policy_stream_large_payload`, `replication_stream_works` | covered |
| EC failover (one shard loss) | `manager/tests/ec_failover.rs` (1) — `ec_2_1_failover_and_recovery` | covered |
| **RPC framework** | | |
| RPC round-trip correctness / connection pool | `rpc/tests/round_trip.rs` (8) — `unary_echo`, `unary_error`, `concurrent_requests`, `large_payload`, `connection_pool_basic`, `multiple_clients_same_server`, `stateful_handler`, `writer_task_handles_2048_concurrent_vectored` | covered |
| **Throughput benchmark (labelled #[test])** | | |
| Stream-append throughput | `manager/tests/append_benchmark.rs` (1) — `benchmark_append_stream_throughput` | covered (perf, not correctness) |
| **Known gaps (candidate future work)** | | |
| Network partition (asymmetric, client ↔ PS) | — | gap |
| Slow / stuck replica (one extent-node artificially delayed) | — | gap |
| Clock skew / NTP jump across nodes | — | gap |
| Quorum loss recovery (2-of-3 nodes down) | — | gap |
| Disk-full ENOSPC on extent-node | — | gap |
| Rolling-upgrade binary compatibility | — | gap |
| Manager + PS simultaneous restart | — | gap |
| Partition scan / range-query correctness | — | gap |
| Heartbeat storm (many PS reconnect simultaneously) | — | gap |

---

## File-level test count

```
crates/manager/tests/integration.rs 13
crates/rpc/tests/round_trip.rs 8
crates/stream/tests/stream_sqcq.rs 5
crates/stream/tests/extent_pipeline.rs 5
crates/stream/tests/f099m_shards.rs 4
crates/manager/tests/etcd_stream_integration.rs 4
crates/manager/tests/ec_integration.rs 4
crates/stream/tests/wal_recovery.rs 3
crates/stream/tests/f021_multi_disk.rs 3
crates/stream/tests/extent_restart_recovery.rs 3
crates/stream/tests/extent_append_semantics.rs 2
crates/manager/tests/system_ps_recovery.rs 2
crates/manager/tests/system_ps_failover.rs 2
crates/manager/tests/system_manager_failover.rs 2
crates/manager/tests/system_extent_recovery.rs 2
crates/manager/tests/system_extent_failover.rs 2
crates/manager/tests/system_split_writes.rs 1
crates/manager/tests/system_split_ref_counting.rs 1
crates/manager/tests/system_split_overlap.rs 1
crates/manager/tests/system_split_large_values.rs 1
crates/manager/tests/system_seal_during_writes.rs 1
crates/manager/tests/system_locked_by_other.rs 1
crates/manager/tests/system_compound_failures.rs 1
crates/manager/tests/ec_failover.rs 1
crates/manager/tests/append_benchmark.rs 1
crates/stream/tests/test_helpers.rs 0
```

**Total: 73 test functions across 25 files (1 helper-only file with 0 tests).**

---

## Gap analysis

The biggest uncovered distributed-system scenarios are **network-level faults** (asymmetric partition, slow/stuck replica, simultaneous multi-component restart) and **resource exhaustion** (ENOSPC on extent-node). These are the scenarios most likely to produce split-brain or data-loss in production because they cannot be triggered by process-kill alone — they require either tc-netem, a custom fault-injection layer, or intercepted system calls. Of these, **quorum loss recovery (2-of-3 nodes down)** and **network partition between client and PS** carry the highest production risk because the system makes no explicit quorum-loss guarantee in the current test suite; if two extent-node replicas are simultaneously unreachable, the manager recovery path would need to rebuild two shards from one surviving replica, a path never exercised in CI. **Rolling-upgrade compatibility** is also notable because the binary RPC frame format is unversioned; a mismatch between an old extent-node and a new StreamClient would produce silent frame corruption rather than a clean error.

---

## Verdict

- **Covered well:** etcd-backed leader election and failover; extent-node restart and WAL replay; partition-server crash/recovery (single-node); partition split (normal, VP, overlap, ref-counting); seal-during-writes; LockedByOther fencing; EC encode/decode and single-shard failover; multi-disk allocation; per-shard (F099-M) routing; SQ/CQ pipeline correctness; PS flush/compaction/GC lifecycle; RPC framework correctness under concurrency.
- **Under-covered:** network-partition scenarios (asymmetric drop, packet delay); multi-node simultaneous failure (quorum loss); disk-full ENOSPC; rolling-upgrade binary compatibility; concurrent PS reconnect (heartbeat storm); partition range-scan correctness.
