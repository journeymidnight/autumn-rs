# At-rest integrity: scrub — design

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
| `.dat` as it is written | CRC32C per whole 1 MiB block, `extent-{id}.ck.open`, recorded as appends become durable | the first scrub | `crates/stream/src/extent_cksum.rs` (`WriteCk`, `open_slots`) |
| sealed `.dat` (replica) | CRC32C per 1 MiB block, `extent-{id}.ck` | scrub; the EC coordinator, on the copy it encodes; the reader of a background `MSG_READ` | `crates/stream/src/extent_node/scrub.rs` |
| sealed `.shard{i}` (EC) | CRC32C per 1 MiB block, `extent-{id}.shard{i}.ck`, written as the shard is staged or rebuilt | scrub; the reader of a background `MSG_READ` | same |

## The rule: content is described as it is written, checked by the scrub

The crcs a payload was checked against on its way in are kept as its baseline.
An append's payload crc is checked by the extent node in one pass that cuts
the payload at the 1 MiB block boundaries of the file it lands in
(`pieces_of_parts`); once the burst is durable, the per-extent owner extends
the open block's crc with those pieces (`WriteCk`, `crc32c_combine`: one
GF(2) multiplication per set bit of the length, no second pass) and, after
replying, writes each completed block's slot to `extent-{id}.ck.open` — a
fixed position per block, self-checked, no fsync. A slot a crash loses leaves
that block to the scrub's first record; a burst whose fsync failed records
nothing; a truncate cuts the slots from the block it cuts into, durably, before
it shrinks the file. EC shards are described as they are staged and as they are
rebuilt. A replica rebuilt by recovery or refilled by a peer copy (re_avali,
the EC coordinator's refill) is described by the block crcs it was checked
against on the way in. The scrub finalizes: it checks a `.dat`'s whole blocks against their
write-time crcs, writes `.ck` and drops `.ck.open`.

Foreground reads and seals never compute or consult a checksum. Besides the scrub and the EC coordinator, a background read
(`MSG_READ` with `with_crcs`: recovery, peer copy, EC rebuild, replay)
consults them: its block crcs for whole scrubbed blocks come from the sidecar,
so a block that rotted since the scrub fails the reader's check, and the other
pieces are computed over the bytes sent. Every other choice follows from that:

- **The write path pays no extra pass.** The append's one scan both checks
  its crc and yields the block pieces; recording them costs a few GF(2)
  multiplications per append and a slot write per MiB, no fsync.
- **No read is refused on a checksum.** A copy leaves service only when the
  manager isolates its slot, after a scrub reported it.
- **Trust-on-first-use only where nothing was recorded.** A block written
  whole and recorded is checked against what was written, so rot before the
  first scrub is found. A block with no write-time record — the block a
  restart or a truncate left mid-way, one whose slot a crash lost, a file
  written before this build — is recorded by its first scrub as it is, and
  only later changes are found. Scrubbing early keeps that window small, which
  is what the weekly policy is for.
- **Recovery checks against the source's record; conversion checks too.** A
  rebuild's reads carry block crcs, taken from the source's `.ck` where a scrub
  recorded one, so a scrubbed source that rotted since fails the read and the
  next source is tried; a source no scrub has described yet is copied as it
  is (only its transit is checked) — but the other copies survive it. A conversion is different:
  once its layout flips, reconcile drops every replica, and the encoded bytes,
  parity included, are the only version left. So the coordinator checks the
  `.dat` it encodes against its `.ck` as it reads it (`PieceCheck`: pieces read
  shard by shard, combined per block with `crc32c_combine` — no second pass).
  A mismatch fails the attempt; the re-dispatch is answered
  `CODE_CONTENT_CORRUPT`, and the manager releases the marker and isolates that
  copy (`release_rotted_ec_attempt`), so it is rebuilt before any conversion
  (`acquire_extent_inflight` refuses an extent with a corrupt-marked slot). A
  copy with no `.ck` is encoded unchecked: rot before the first scrub cannot be
  told from content. Once a scrub finds rot the slot is dark, and no
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

## Reads are not left waiting for the scrub

The partition server checks every SST data block's own CRC32C as it decodes
it. A block that fails is fetched again from every other
copy — each lit replica, or for an EC extent one reconstruction per covering
data shard with that shard left out and its node not asked — and the first
copy that decodes is served. A replica of a sealed extent whose copy failed
while another passed is reported (`MSG_REPORT_CORRUPT_REPLICA`, row stream)
and isolated like a WAL replay's finding; an EC culprit is only logged, and
its node's scrub reports it. A healthy read pays nothing extra.

## Where the CRCs are, and who computes them

1. **On disk: the formats carry their own CRCs, computed by the server.** Every
   SST data block and MetaBlock has a CRC32C written by the PS's SST builder;
   every V1 WAL record has a CRC32C over its length and payload, value
   included, written by the PS when it encodes the record. The extent node's
   `.meta` has its own, and the scrub's `.ck` sidecar describes a payload file
   per 1 MiB block. Nothing here changes per request.
2. **On the wire: every frame's header and ctrl are CRC'd; a value tail is
   not.** `rpc/src/frame.rs` computes the CRC over `header ++ ctrl_len ++ ctrl`
   on every encode path and verifies it on every decode path before ctrl is
   used. The value RPCs (`PUT`, `BATCH_PUT`, `COMPARE_WRITE`, `GET`,
   `BATCH_GET`, `READ`) carry their value as a raw tail.
3. **`MSG_APPEND` carries its payload's crc32c in ctrl**, under the frame
   CRC, and the payload as the frame's value; the extent node recomputes the
   crc over what it received before writing. It cannot check a WAL record or
   an SST block itself, so this is the only in-transit check it has.
4. **Client writes carry no CRC.** The PS computes the WAL record CRC on the
   bytes it received.
5. **Reads: SST blocks are verified as they are decoded (and read around when
   they fail). Large values are not verified on read; the scrub is their only
   check.** A value pointer's bytes sit inside a V1 WAL record whose CRC
   covers them, but a read fetches only the value — mostly straight from an
   extent node into the caller's buffer (a position from `MSG_GET` + `MSG_READ`),
   otherwise through the PS (`resolve_value`) — and nothing checks that record
   CRC. It is not checked, for two reasons: most reads are direct
   reads, where a check costs a CRC pass over every value on the client (about
   1.2 ms for 8 MiB, comparable to the transfer on RDMA, and one CRC per
   record cannot be checked while the value streams in); and the scrub covers
   sealed log extents. The cost: a rotted copy of a value, or
   one damaged in transit, reaches the reader undetected — rot until a scrub
   of its sealed log extent isolates it (up to the scrub interval; rot before
   the first scrub is recorded as the content, and an open tail is not
   scrubbed).

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
before the second finding is re-checked — a replica rebuild then refuses the
still-lit rotted copy as a source (its reads fail against its own `.ck`); an EC
shard rebuild reconstructs from what the shards hold, and its output has no
checksums, so the next scrub records whatever it got — and a rot finding
refused because it is the last available copy is settled without a corrupt
mark. A darkened slot carries a corrupt mark, so the recovery loop rebuilds it
on another node. A rebuilt replica comes with the checksums its copy was
checked against; a rebuilt EC shard comes with the crcs of the bytes it
reconstructed.

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
`--scrub-policy-interval-secs` (default 7 days). The interval is that kind's actuation
cooldown whatever the policy's own cooldown says, and cooldowns are persisted,
so a leader change does not restart the week. The advisory row appears only
once the interval has passed and no scrub is running.

## Non-goals

- **A lying peer.** This finds media rot and silent mis-writes; a node that
  hashes bytes it already corrupted is a Byzantine problem.
- **Cross-copy comparison.** Each copy is checked against its own record, not
  against the other copies. For EC that would be an RS consistency check,
  which reads K shards across the network.
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
  the next scrub finds the rebuilt replica clean against the checksums its
  recovery wrote.
- `crates/manager/tests/system_chaos.rs` `corrupt` — rot a recorded replica
  under fault injection; a scrub at the end of the round must report it.
