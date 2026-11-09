# WAL replay self-heal loop

## Goal

When log_stream replay hits a corrupt record in the middle of the stream, it **fails loud**
(the WAL-FAILSTOP invariant, see `crates/partition-server/CLAUDE.md`) instead of silently
skipping it and losing data. But failing loud alone is **loud-but-stuck**: reads use
deterministic replica selection, and bad bytes are not a stream-layer read error, so the
same bad replica is read again and again → the partition can never open. In practice the
corruption is usually bit-rot on a single disk, and the other two replicas are fine.

The self-heal loop turns loud-but-stuck into: **replay finds a bad replica → re-read across
replicas to repair that window → synchronously isolate the bad replica (serve only on
success)**.

## Path

Everything lives in `recover_partition` (`crates/partition-server/src/lib.rs`) +
`StreamClient` (`crates/stream/src/client.rs`) + the manager's
`handle_report_corrupt_replica` (`crates/manager/src/rpc_handlers.rs`):

1. Each committed window runs through `decode_records_chunk`. Any one of three corruption
   signals triggers self-heal: a short read (the serving replica is truncated), a CRC /
   inner-length mismatch on a complete record (`Err`), or leftover bytes after the final
   chunk is decoded (`consumed < buf.len()`).
2. `self_heal_replay_chunk`: for a **sealed replicated** extent, re-read **the same
   committed window** from every eligible replica with
   `StreamClient::read_committed_from_replica(eid, idx, off, len)` (no failover; returns
   the node_id that served it). The pure function `select_clean_replica_chunk` picks the
   first one that decodes cleanly and collects the bad node_ids.
3. **Content corruption** in the OPEN tail → `StreamClient::seal_and_roll_tail`
   (seal-over-reachable seals the current tail + rolls a new tail onto healthy nodes);
   in the same pass, re-fetch the now-sealed ExtentInfo and go through the sealed
   cross-replica isolation of step 2 (no reopen needed).
4. EC extent / all replicas bad / a **truncated** (short) open tail / seal-and-roll
   failure → no self-heal, fail loud.
5. After the chunk loop ends and **before the partition serves**: for each bad extent,
   `invalidate_extent_cache` → re-fetch eversion → `report_corrupt_replica`
   (`MSG_REPORT_CORRUPT_REPLICA`). The manager clears the bad slot's `avali` bit + bumps
   eversion (etcd-first). Rejected (stale owner/eversion, would isolate the last replica,
   extent still open, already EC) → `Err` → partition open fails loud → region_sync
   retries → detect again and report again.
6. The read path's `eligible_replica_slots` filters by `avali` (consumed by
   `replicated_read_order`).

## Invariants (load-bearing)

> The numbers are referenced directly by code comments (`I1`/`I2`/`I4`/`I5`/`I7`); do not
> renumber them.

- **I1 isolate before serving**: once replay confirms a replica is bad → durable isolation
  (removing the bad slot from the read set) must succeed before the partition serves.
  Isolation fails → stay fail-loud; never start serving while the bad replica is still
  readable.
- **I2 the read path isolates by avali (sealed replicated only)**: failover reads on a
  sealed replicated extent skip slots whose `avali` bit is 0. **Open extents are not
  filtered** (avali=0 is the normal "not sealed" state, not "bad"; open consistency is
  governed by the commit-min protocol). **EC extents do not filter the addr list** (that
  would break the shard↔addr index alignment; a missing shard is handled by EC
  reconstruction). Defense: 0 usable after filtering → fall back to reading all + the
  caller logs it (no availability regression).
- **I3 bad replica in the open tail = seal-and-roll**: the open tail has no fixed length,
  so a direct failover read is ambiguous (commit-min changes with the read set). Instead,
  seal-over-reachable seals the current tail + rolls a new tail. The ACKed prefix exists
  on every committed member → min over healthy replicas ≥ ACK, so no acknowledged data is
  lost; bytes beyond the healthy committed length are un-ACKed speculative writes, and
  sealing them off is correct. New writes land in the new tail. An extent that is already
  sealed is not sealed again; it is isolated directly + handled by its fixed
  `sealed_length`.
- **I4 fencing**: the report carries `owner_epoch + partition_id + log_stream_id +
  extent_id + eversion`; the manager checks four layers before touching layout —
  ① the reporter must hold the partition's current owner_epoch; ② the log_stream must
  belong to that partition; ③ the extent must be a member of that log_stream; ④ the
  eversion must match the current one. Any mismatch → `CODE_PRECONDITION`, so a stale PS
  cannot change layout and cannot isolate another partition's replicas.
- **I5 etcd-first**: clearing avali / bumping eversion first goes to etcd via
  `persist_extent`; only on success is memory written, and before writing memory a
  verify-at-apply runs (insert only if the live eversion still equals the reported
  snapshot, otherwise reject, so a stale clone never overwrites a concurrent mutator).
- **I7 clean-source selection**: during the cross-replica re-read, a slot with `avali=0`
  is not a candidate clean source (it is already isolated; untrustworthy even if it
  decodes cleanly this time); **a read error ≠ corruption** (node unreachable / timeout
  is transient, does not enter the bad-replica set, and an unreachable node is never
  isolated); the verdict uses WAL V1 CRC decoding, not just length checks; if no
  reachable replica is clean → fail loud, never "longest wins".
- **I8 the re-read window aligns with carry**: the cross-replica re-read starts at
  `buf_base = cur_off - carry.len()` and reads `carry.len() + got` bytes; after switching
  replicas, consumed / carry / record_extent_off are recomputed (otherwise a bad carry
  would misjudge a healthy replica, and VP offsets would be misaligned). The request is
  for the **full window** (`want_full`), not the possibly truncated `buf.len()` — a
  truncated serving replica must not shrink the window asked of a clean replica.
- **I9 guarantee boundary**: strong integrity applies only to V1 WAL (with CRC). V0
  legacy has no CRC; a bit-flip that does not cross a length boundary makes `decode_one`
  return `Ok` outright, so the self-heal report can only mark it `legacy_unchecked` and
  does not claim 2^-32.

## Limits (known gaps in the current implementation)

- **Isolation is not permanent**: the manager's `recovery_dispatch_loop` treats
  `avali & bit == 0` as "this replica needs repair" and sends `EXT_MSG_RE_AVALI` to that
  node; but a bit-rotted replica has its full length, so the EN's `handle_re_avali`
  returns `CODE_OK` straight away when `local_len >= sealed_length`, and the manager then
  calls `mark_extent_available`, setting the `avali` bit again. So for a replica with
  **content corruption** (as opposed to missing length), isolation holds only for the
  read window of this open and is later undone by the recovery loop. Actually reviving a
  bad replica needs a forced overwrite (staging + WAL-CRC check + atomic rename) — not
  implemented; `re_avali` judges "latest" by length, which is useless against bit-rot.
- **No local quarantine on the EN**: the bad node neither marks the data as corrupt
  locally nor refuses `build_read_future` (a different shape from META-FAILCLOSED's
  `corrupt_meta`). Isolation relies entirely on the manager-side `avali` + eversion
  evicting client caches.
- **VP reads do not check WAL CRC**: `resolve_value` does no per-record CRC, so I1's
  "isolate before serving" is the only guarantee that a user GET of a large value does
  not read bad bytes.
- **The etcd write for the report is a blind put** (`extents/<id>`, not a value-CAS), the
  same class of residue as `apply_recovery_done` / `apply_ec_conversion_done` /
  split-seal (manager CLAUDE.md note 33, on hold pending a reproduction); closed off by
  verify-at-apply.
- The initial serving read does not return a node_id: when the bit-rot re-read is still
  bad it can be attributed; a timeout cannot (per I7, unreachable replicas are not
  isolated).

## Rejected alternatives

- Per-record CRC in the stream layer + automatic failover on read: intrudes on the hot
  path, crosses layers, does not apply to EC → keep it in the partition layer.
- Repair directly with `re_avali`: it judges "latest" whenever the local length suffices →
  useless against bit-rot.
- Fail loud only and rely on manual fencing: unacceptable availability.
- Best-effort report + serve without isolation: becomes "available but returns bad
  data", worse than failing loud.
