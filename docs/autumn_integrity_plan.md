# At-rest integrity for stream-layer content: scrub

## What is protected, and by what

Every layer that owns a byte format checksums it. The stream layer owns none —
it stores opaque bytes — so its content is checked by a **scrub**, on demand,
on the node that holds it.

| bytes | checksum | checked by | reference |
|---|---|---|---|
| RPC frame head | CRC32C over `[header][ctrl_len][ctrl]` | decode | `crates/rpc/src/frame.rs` |
| RPC frame bulk value | none, by design ("bulk value integrity is the transport's job") | — | `frame.rs` |
| partition WAL record | CRC32C including the value | replay | `crates/partition-server/src/wal_record.rs` |
| partition SST block | CRC32C | every block read | `crates/partition-server/src/sstable/format.rs` |
| stream `.meta` | CRC32C over its 48 metadata bytes | `parse_meta` | `crates/stream/src/extent_node.rs` |
| sealed `.dat` (replica) | CRC32C per 1 MiB block, `extent-{id}.ck` | scrub only | `crates/stream/src/extent_node/scrub.rs` |
| sealed `.shard{i}` (EC) | CRC32C per 1 MiB block, `extent-{id}.shard{i}.ck` | scrub only | same |

## The rule: the scrub shares nothing with the hot path

Appends, reads, seals, EC conversion and repairs never compute, write or
consult a checksum. The scrub is the only writer and the only reader of the
sidecars. Every other choice follows from that:

- **No write path pays for integrity.** Content is hashed when it is scrubbed,
  not when it is written.
- **No read is refused on a checksum.** A copy leaves service only when the
  manager isolates its slot, after a scrub reported it.
- **Trust-on-first-use.** A copy's first scrub records what it holds. Bytes
  already damaged before then are recorded as they are; a later scrub finds
  only what changed after the first. Scrubbing early is what keeps that window
  small — which is what the weekly policy is for.
- **Recovery and conversion read unverified.** A rebuild copies, and a
  conversion encodes, whatever its sources hold. A rotted source no scrub has
  found yet is propagated. Once a scrub finds it the slot is dark, and no
  repair reads a dark slot: a replica copy chooses sources by the corrupt
  bitmap, and an EC rebuild skips dark slots (`ec_rebuild_source_slots`) —
  Reed-Solomon would turn one wrong input into a wrong output for every byte
  it spans. Clients still read a dark EC shard until its rebuild lands; it is
  the only copy of its bytes.

## Who asks, who reads

```
autumn-op scrub EXT... | --part P | --all        (or the weekly `scrub` policy)
        │  MSG_OP_SUBMIT (OP_KIND_SCRUB)
        ▼
manager: plan — every LIT copy of every SEALED extent in scope, naming
         the file (.dat, or .shard{i} for slot i) and its length
        │  MSG_SCRUB_EXTENTS, to the EN shard that owns each extent
        ▼
EN: queue → one worker per shard, paced by --scrub-bytes-per-sec
    per file: checksums recorded for exactly this length?
              yes → compare every block      → CLEAN or ROT
              no  → hash and record them     → DESCRIBED
        │  DfResp.scrub_done (every outcome, ROT among them)
        ▼
manager: op progress / finish; isolate_rotted_slot → recovery rebuilds the slot
```

**The manager names the file and its length** because it is the one that
knows: the extent is sealed, its payload on that node is `.dat` (a replica) or
`.shard{i}` (slot `i` of a converted extent), and how many bytes that file
holds (`sealed_length`, or `ceil(sealed_length / K)` for a shard). The node
never asks, and a sidecar describing another length is about other bytes and
is replaced.

**No content crosses the network.** The request carries extent ids, file
names and lengths; the reply is one outcome per file.

**Pacing lives on the node.** Only the node that reads knows how busy its
disks are. `--scrub-bytes-per-sec` (default 8 MiB/s per EN shard, so N shards
read up to N times that) bounds it, and an idle stretch banks no burst.

**What is left alone.** Open and sealed-empty extents (no settled content),
extents with a recovery or EC conversion in flight (their files are being
rewritten), dark slots (behind and being caught up, or isolated and awaiting a
rebuild), and pre-CoW EC layouts whose shards live in `.dat`. On the node: a
copy shorter than the length named (a replica that missed the seal), a file it
does not hold, a quarantined `.meta`.

## Findings and repair

A block that differs from its checksum, or that can no longer be read in full
(a described file that lost its tail mismatches nothing), is ROT. The outcome
carries the eversion the task was planned under, and the manager runs the
decision every first-hand report goes through (`compute_corrupt_isolation`):
never darken the last available replica — on an EC extent, never leave fewer
than K shards serving — and do not judge a finding while an op is in flight on
the extent or after its eversion moved. Such a finding is not dropped: the op
holds it, and once the extent has nothing in flight the manager scrubs that
copy again under the current eversion. Two rotted copies of one extent always
take this path — the first one's isolation moves the eversion the second was
planned under — and nothing else would ask again until the next scrub.
Accepted limits: when a spare node exists the first copy's rebuild can start
before the second finding is re-checked, and copy from that still-lit rotted
copy — the rebuilt copy has no checksums, so the next scrub records the damage
as its content; and a rot finding refused because it is the last available
copy is settled without a corrupt mark. A darkened slot carries a
corrupt mark, so the recovery loop rebuilds it on another node. The rebuilt
copy has no checksums; the next scrub records them.

A scrub never acts on content that changed while it ran. Every writer that
replaces a payload file out of band — a recovery writeback, a peer copy, a
shard rebuild, a discard, a delete — drops that file's sidecar first and bumps
the entry's content generation; a scrub that sees the generation move drops
its result instead of reporting or recording what it read.

## The op

`OP_KIND_SCRUB` runs until every file dispatched has reported. It SUCCEEDS
whatever was found (rot is a result, acted on separately) and FAILS only if a
file could not be checked. A file two ops ask for is read once and reported to
both.

Outcomes are at-most-once on `df`, and a node's queue is in memory, so each
`df` also lists the ops a node still has queued (`DfResp.scrub_queued`). An op
listed there is alive however long its files wait behind others; an op with
files on a node that answers `df` WITHOUT listing it (after a 30 s grace) has
lost them — the node restarted, or the report was lost — and those files are
FAILED at once. An op nothing is heard about for two hours (every node holding
its files stopped answering) ends as UNKNOWN instead of holding its scope,
which attach-dedup would otherwise turn every later submit of into a no-op.

## The weekly policy

The auto-policy `scrub` switch (on in `maintenance`, `balanced` and
`aggressive`) submits `scrub --all` at most once per
`SCRUB_POLICY_INTERVAL_SEC` (7 days). The interval is that kind's actuation
cooldown whatever the policy's own cooldown says, and cooldowns are persisted,
so a leader change does not restart the week. The advisory row appears only
once a week has passed and no scrub is running.

## Non-goals

- **A lying peer.** This finds media rot and silent mis-writes; a node that
  hashes bytes it already corrupted is a Byzantine problem.
- **Cross-copy comparison.** Each copy is checked against its own record, not
  against the other copies. Comparing replicas' records (only the records
  travel) is a separate, deferred step; for EC it would be an RS consistency
  check, which reads K shards across the network.
- **Open extents.** Their content is still changing; the WAL record CRC covers
  the partition layer's use of them.

## Tests

- `crates/stream/src/extent_node/scrub.rs` — record then compare, rot, a file
  that lost its tail, shard files, the skip cases (not held, wrong length, a
  replica behind the seal, an op in flight), content replaced mid-scrub,
  forgetting on repair, sidecars removed on delete and discard, the queued
  request path.
- `crates/stream/tests/payload_location.rs` — the request path over a socket,
  both outcomes and the finding on `df`; the read path still serves the rotted
  copy.
- `crates/manager/src/extent_scrub.rs` — the plan (lit copies of sealed
  extents, the file and length of each, scopes) and op accounting (finish once
  every file has reported, give up on silence).
- `crates/manager/tests/scrub_on_demand.rs` — a replica and an EC shard: the
  scrub records, a second is clean, rot with nobody reading, a scrub finds it,
  the slot is isolated, a spare joins and the copy is rebuilt byte-exact, and
  the next scrub records the rebuilt copy.
- `crates/manager/tests/system_chaos.rs` `corrupt` — rot a recorded replica
  under fault injection; a scrub at the end of the round must report it.
