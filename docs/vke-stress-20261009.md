# VKE rollout and fault test, 2026-10-09

Tested storage server image: `7446c62b7abf7af9eb809c7a1a4a83c0c6151819`.
Volc CP run #134 (`affed4bd48514678a8cf82ad2d364127`) built the configured
main branch successfully in 2m45s. Checkout SHA and image tag were checked
in the build log. All core workloads were updated; the dashboard was also
checked in Chromium against the live API.

During the fixed-image test, upstream added `0379ae1b`, affecting
`autumn-op`'s open-extent size display. After review, CP #135
(`c79a2c414b8c4d1e95c8603d28fa4b78`, 2m42s) built that revision. After the
fault window ended and logs were captured, dashboard and toolbox were updated
to `0379ae1b916815474e1cfd3e054e285d3ebf558e`. Storage server code is unchanged
between these revisions.

Evidence for this session is in
`/private/tmp/autumn-ff29ad67-stress-20261009/`. The directory retains the
initial revision in its name; filenames distinguish the later tested images.

## Review and display fix

The initial five-commit review covered `e236ecdc`, `823f83b6`, `ff6ccec8`,
`a92a2f4f`, and `ff29ad67`. The stale merge-freeze cleanup issue found in that
review was fixed upstream by `91163fa5`; all seven merge-freeze unit tests
passed. The subsequently pulled allocation fix `edba2cd3` captures the initial
stream membership and uses stream-value and owner-revision comparisons at
commit. Its seven `alloc_extent_races` tests passed.

The dashboard's single-line ellipsis hid differing suffixes of neighboring
range endpoints. `7446c62b` shows inclusive `start` and exclusive `end`
separately, wraps complete escaped keys, and sizes the virtual rows from those
same wrapped lines. Range continuity was intact in the reported snapshot.

Validation: `render_check`, `tabs_smoke`, and `policy_controls` passed. Chromium
checked captured long KVC boundaries, resizing, and scrolling to the final row.
The deployed page passed a second check against all 30 live partitions at
1564px and 1100px widths: complete endpoints, no clipping or row overlap, and
no browser errors. Screenshot: `range-live-7446.png`.

The final dashboard on `0379ae1b` passed the same live range check again
(`range-live-0379.png`). Its open-extent regression target compiled locally
but could not run on macOS because it reads Linux `/proc/self/status`.
An equivalent isolated test used the CP-built Linux binaries: one manager,
one eight-shard EN, one PS, and eight 4 KiB writes. Open extent 4 belongs to
shard 2. The old `7446c62b` operator reported 0 B; the new operator reported
33,080 B in both `info --part` and `info --full`. All fixture processes were
stopped afterward. Evidence: `op-shard-linux.py`, `op-shard-linux.log`, and
`op-shard-0379-*.json`.

## Confirmed bug: standby promotion evicts healthy partition servers

`replay_from_etcd` seeds `ps_last_heartbeat` using `or_insert(now)`. A standby
already has entries from startup, and does not receive the leader's PS
heartbeats. Promotion therefore keeps old timestamps. `mark_serving` resets
them when the listener initially binds, but does not run again on promotion.
The first liveness tick after promotion can immediately evict all live PSes.

Evidence on the deployed image, UTC:

- During rollout, manager 1 began re-election at 22:59:10.855; all three PSes
  were declared timed out at 22:59:11.363. All 30 partitions landed on PS 2.
- During the subsequent fault test, manager 2 began re-election at
  23:13:15.752; all three PSes were declared timed out at 23:13:17.388.
  The distribution changed from 10/10/11 (a merge was completing) to all
  30 partitions on PS 1. PS pod UIDs and restart counts were unchanged.
- The second fault promoted manager 1 at about 23:20:40; it evicted all
  three PSes at 23:20:41.202, concentrating all 30 partitions on PS 2.
  Again, the PS pod UIDs and restart counts were unchanged.

The existing ignored `ps_members_etcd` test passed. A diagnostic variant
started the standby while the old leader continued receiving PS heartbeats
for another 15 seconds, stopped the old manager runtime, and waited three
seconds after promotion before heartbeating the successor. This is inside
the promised ten-second grace. It failed with `ps 1 not registered`.
Changing only `or_insert(now)` to `insert(id, now)` made the same probe pass.

The probe and candidate were diagnostic only and were removed from the worktree.
The deployed image still has this bug. It is tracked as
`BUG-MANAGER-STANDBY-HEARTBEAT-GRACE` in `feature_list.md`. Evidence:
`standby_heartbeat_probe.rs`, `standby-heartbeat-probe.log`,
`standby-heartbeat-candidate.log`, and `standby-heartbeat-candidate.patch`.

## Performance and integrity workload

4 KiB `perf-check`: 32 threads, pipeline depth 16, bulk 64, 20 seconds each
for write and read, client pinned to physical cores `0,2,4,6`.

| Server image | Write MiB/s | Read MiB/s | Write p99 | Read p99 |
| --- | ---: | ---: | ---: | ---: |
| `91163fa5` | 631.08 | 1981.45 | 0.86 ms | 0.38 ms |
| `7446c62b` | 581.77 | 1835.04 | 1.09 ms | 0.39 ms |

These are separate post-rollout measurements with different cache and placement
history, not a controlled performance comparison. Raw output and baseline JSON
are saved as `perf4k-<revision>.*`.

The fault workload uses the scope `bench/perf/50000000-ff29stress`:

- 800,000 deterministic 512 B values, 32 threads, depth 16, mixed reads/writes.
- 2,048 versioned 1 MiB values, eight workers, at most 128 writes/s, pinned to
  physical cores `8,10,12,14`. Acknowledged versions from the previous run
  are retained. Ambiguous writes retry the identical payload before reading.

After rollout and before fault injection, all 802,048 keys passed full
readback with zero errors, missing keys, or mismatches.

The fixed-image fault window ran from 23:06:47 to 23:26:49 UTC. Confirmed
actions: two EN outages (`autumn-en-6`, `autumn-en-9`), two manager failovers
(1→2, 2→1), three successful splits, three successful merges, six successful
compactions, and two post-failover rebalances. One additional split failed
because its freeze window elapsed with an EN absent. A later split succeeded
while a different EN was absent. Peak degraded extents: 147; observed
unavailable extents: zero. The EN counts are validated against pod removal and
subsequent five-online health samples; the controller's immediate counter
missed both because the last heartbeat was still fresh at the first sample.

| Workload | Successful reads | Successful writes | Request errors | Missing | Mismatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| 512 B, 1200.17 s | 26,571,834 | 25,866,510 | 726,809 | 0 | 0 |
| 1 MiB, 1210.53 s including startup/wait | 123,461 | 123,461 | 453 | 0 | 0 |

After both workloads stopped and the final diagnostic rollout completed,
full readback passed again: 800,000 small values in 7.707 s and 2,048 latest
versioned large values in 5.004 s, with zero request errors, missing keys, or
mismatches. The entire partition boundary chain was continuous, and the test
parent 3374 was restored to `[bench/perf/40000000, bench/perf/70000000)`.
There were 30 partitions, distributed 10/10/10 across the three PSes.
Both EN deployments were restored to one replica; both managers were present.
The original `aggressive` policy was restored to Armed with mutations allowed.
Test data was retained. Evidence: `final-verify-*.log`, `final-info.json`,
`final-status.json`, `final-policy.json`, and `validated-action-summary.json`.

Transient errors are counted separately from integrity failures. Ordinary
`put` passes the embedded merge-freeze `CODE_UNAVAILABLE` through as a
`ServerError`; `put_bulk` explicitly refreshes and retries this code. The
workload therefore exposes freeze refusals to the application even when all
acknowledged values remain intact. The first split with one EN absent failed
explicitly after its freeze window elapsed; after EN restoration a later
split and merge succeeded. The first merge committed before the manager
stop, so it does not prove a crash during an in-flight merge.
