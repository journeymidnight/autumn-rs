# At-rest integrity for stream-layer content

## What is protected, and by what

Every layer that owns a byte format checksums it. The stream layer owns none —
it stores opaque bytes — so its content is described by sidecars written beside
the files that hold it.

| bytes | checksum | verified at | reference |
|---|---|---|---|
| RPC frame head | CRC32C over `[header][ctrl_len][ctrl]` | decode | `crates/rpc/src/frame.rs:143` |
| RPC frame bulk value | **none, by design** — "bulk value integrity is the transport's job" | — | `frame.rs` (its own round-trip test flips a value byte and asserts the frame still decodes) |
| partition WAL record | CRC32C **including the value** | replay | `crates/partition-server/src/wal_record.rs:220` |
| partition SST block | CRC32C, compared on read | every block read | `crates/partition-server/src/sstable/format.rs:134`, `:254` |
| stream `.meta` | CRC32C over the 48 metadata bytes | `parse_meta` | `crates/stream/src/extent_node.rs:4212` |
| stream `.dat` content | CRC32C per 1 MiB block, `extent-{id}.ck` | whole-block reads, recovery source reads, before EC encode, scrub | `crates/stream/src/extent_cksum.rs` |
| stream `.shard{i}` content | CRC32C per 1 MiB block, `extent-{id}.shard{i}.ck` | whole-block reads, EC rebuild source reads, scrub | `describe_shard_stripe` in `extent_node.rs` |
| background scrub | per-shard byte-paced sweep | continuously | `crates/stream/src/extent_scrub.rs` |

Without the sidecars two things hold, and the second is the serious one.

**A stream-layer consumer is protected only if it brings its own checksum.**
The partition layer does, for what it writes. A consumer that hands raw bytes to
`StreamClient` — anything reading and writing extents directly — has nothing.

**The repair paths run BELOW the layer that holds the checksums, so they
propagate corruption faithfully.** Recovery's verify-after-fetch compares
fetched length against `sealed_length` and checks that eversion did not advance;
neither changes under a bit flip, so a rebuilt replica is byte-identical to the
corrupt source. EC conversion's coordinator reads its local `.dat` and encodes
parity from it, which makes the corrupt bytes canonical across the stripe. The
partition layer can still detect its own WAL damage at replay, but by then the
damage has been replicated and encoded.

Read-side replica selection is a deterministic SplitMix64 over
`(extent_id, offset)` (`crates/stream/src/client.rs:575`), so a corrupt replica
is chosen **consistently** for the affected offsets rather than intermittently.
The reproduction harness observes 25 of 64 sub-ranges landing on it.

## Invariants this design establishes

1. **A sealed extent's content is self-describing.** Its bytes can be checked
   against a checksum written when it sealed, by any holder, with no peer.
2. **No repair path may promote unverified bytes.** Recovery must not rebuild
   from a source that fails verification, and EC conversion must not encode
   parity from one.
3. **Detection is not conditional on someone reading.** A replica that rots
   while idle is found, isolated, and rebuilt.
4. **Absence of a checksum is not corruption.** An extent sealed before this
   exists verifies as "unknown", never as "bad" — otherwise deploying it would
   condemn every existing extent.
5. **Verification never fails a read closed.** A failed check routes around the
   bad replica using the isolation path that already exists; it does not deny
   the caller data another replica can serve.

## Format: the `.ck` sidecar

`extent-{id}.ck`, beside `.dat` / `.meta` / `.shard{i}`, written when the extent
seals.

```
magic          8   "EXTCKS\0\x01"
extent_id      8   u64 LE   — anti-reuse, same guard as .meta
sealed_length  8   u64 LE   — the content these checksums describe
block_bytes    4   u32 LE
block_count    4   u32 LE
blocks       4×N   u32 LE   — CRC32C per block, in order
trailer        4   u32 LE   — CRC32C over everything above
```

**A sidecar, not a `.meta` field.** `.meta` is a fixed-size record with an atomic
write, a CRC, and a V0/V1/V2 parse chain; making it variable-length complicates
all four. Sidecars are already the idiom here (`.shard{i}`, `.ec.prepared`), and
a new one must be registered in `remove_extent_files`, whose path list is
explicit.

**Absent is legal.** No `.ck` means the extent predates this and verifies as
unknown. That is what makes the change deployable with no migration, satisfying
the stop-the-world rule trivially.

**Per block, not per extent.** A whole-extent checksum can only be verified by a
whole-extent read, which is useless for a sub-range and forces a scrub to
re-read everything to report anything. Blocks let a read verify exactly the
blocks it fully covers, let a scrub report *which* region rotted, and let a
multi-GiB extent be hashed in bounded steps. `block_bytes` is 1 MiB: the sidecar
costs 4 KiB per GiB, and one block is a unit of I/O the existing chunked pread
already deals in.

**Stale is treated as absent.** If `.ck` disagrees with `.meta` on
`sealed_length` it describes different content — a crash between the two writes —
and it verifies as unknown with a warning, not as corruption.

## Where verification happens

`apply_extent_meta_durable` is the durable seal applier and the natural writer:
idempotent, retry-safe, already skipping a short replica mid-repair (so it never
checksums a partial `.dat`), and it fsyncs `.dat` before persisting the seal — so
hashing after that fsync reads durable content. The sidecar is written before
`.meta`, so a crash between them leaves an extent that reloads unsealed and is
re-sealed on the manager's next contact.

**It is not sufficient on its own, because there is no seal event on an extent
node.** The manager seals in its own metadata; a replicated extent's holder
learns about it only when something else brings it — an append refresh,
`re_avali`, a copy, or the reconcile. A tail that rolls and is never touched
again may hold no sidecar for a long time. So the scrub is not only the detector
of last resort, it is also what BACKFILLS a missing sidecar, and the two roles
are the same walk.

Backfill is trust-on-first-use: an extent that rotted before it was ever hashed
gets its damage recorded as truth. That is unavoidable for content already at
rest with no prior digest, and it is still strictly better than no checksum,
which protects nothing at any time. The residual would close by hashing on
several replicas and comparing, which is a cross-node mechanism this does not
build.

**A repeat apply must never re-hash.** The applier runs on every manager
contact, so re-hashing would bless post-seal rot into a fresh checksum on the
next contact and the corrupt bytes would verify forever after. An existing
sidecar that already describes this `sealed_length` is left alone.

Verification belongs in `build_read_future`, not `handle_read_bytes`.
`MSG_READ_BYTES` and `MSG_READ_BYTES_BULK` are intercepted in
`handle_connection` and answered by the batched read future; the `dispatch` arm
that reaches `handle_read_bytes` is dead over the wire, so a check placed there
passes its own unit test and protects nothing. Any test for this must read over
a socket.

Built:

| point | what it does on mismatch |
|---|---|
| **read of whole blocks** on a sealed `.dat` | fail the read; the client's existing rotation serves another replica |
| **recovery source read** | the same — `read_bytes_chunk` sends `MSG_READ_BYTES` to the source, which lands in that same batched path, and its 256 MiB chunks cover whole blocks |

| **EC conversion, before encoding** | refuse; do not turn corrupt bytes into parity — the layout flip makes whatever it read canonical for the whole stripe. Runs after the coordinator syncs the seal (the checksums are unreadable until this node knows the extent is sealed) AND after its peer-copy (a short `.dat` is a normal, repairable state here, and checking first reads past EOF and calls it damage). A read failure is answered as `Unavailable`, separately from a mismatch: naming the wrong fault sends the operator after the wrong thing |
| **scrub** | report it on `DfResp`; the manager clears this replica's `avali` bit and the corrupt bitmap makes recovery rebuild it |

The scrub also BACKFILLS, a block at a time across ticks. Demanding a whole
extent per tick does not work: the default extent is 16 GiB and the budget is a
few MiB per second, so the tick's budget is spent, nothing is read, and the same
thing repeats forever. Partial hashes accumulate on the extent entry until the
extent is covered, then land as one sidecar.

Two refusals bound what may be described, and both exist because this node's
own sidecar is what later condemns this node's own bytes:

- **Only durable content.** A replica shorter than `sealed_length` — a copy
  mid-repair, or one legitimately sealed above its own length — is not
  described at all. Hashing what it holds now and the rest after the repair
  would persist a description of a file that never existed, after which the
  copy that was just made healthy fails its own checksum forever.
- **A rebuild restarts the description.** Installing durable bytes out of band
  (peer copy, recovery) clears any half-built accumulator, the cached sidecar
  and the verify cursor, so nothing survives from the content that was
  replaced.

The scrub's own reads are the only ones under its budget. A seal observed on a
control path (append refresh, `re_avali`, reconcile) hashes the whole extent
right there instead — once per extent, on the thing that noticed, which is
where that cost belongs; the scrub asks for the seal but describes the content
itself, block by block, so reaching that writer from inside the sweep cannot
turn a budgeted sweep into a 16 GiB hash.

An extent the scrub cannot describe yet — an open tail, or one sealed empty —
costs one manager probe and is then not asked about again for ~5 minutes. Open
tails are permanent candidates that can never be described, so without a
per-extent cooldown a busy node turns a hardening sweep into steady load on the
single manager.

Sub-block reads are deliberately **not** verified. Verifying a 4 KiB read would
require reading and hashing its whole 1 MiB block — 256× amplification on the
hot path. The scrub covers those bytes on its own schedule instead. Whether a
read covers a whole block is decided by arithmetic BEFORE the sidecar is
consulted, so a sub-block read pays nothing, and the decoded sidecar is cached
on the extent entry so a read never costs a filesystem probe. The cache holds
"there is none" as deliberately as it holds the checksums — but the seal marks
an extent sealed in memory BEFORE it writes the sidecar, so a read landing in
that window would otherwise cache that absence permanently. Writing the sidecar
refreshes the cache for exactly that reason.

**Every writer that installs durable bytes must say so.** The checksum gate
reads the coalescer's fsync watermark, which only the append paths maintain. A
peer copy or a recovery rebuild fsyncs and installs the file without touching
it, so a repaired replica would read as permanently un-synced and be denied a
checksum — the copy most in need of one. `note_durable_install` is the single
definition both repair paths use.

**Only durable bytes are hashed.** The append prologue advances the extent's
reserved length before its write is submitted, so "length covers the seal" does
not mean the disk holds those bytes; the coalescer's fsync high-water does. A
checksum taken over an in-flight write would describe bytes that never existed,
and it would be kept — turning every whole-block read of a HEALTHY replica into
a refusal. A false positive on a healthy replica is worse than the rot this
detects, so that case fails closed and the seal proceeds without a sidecar.

## Isolation and repair reuse what exists

The isolation and repair machinery is reused whole; only the way evidence
ARRIVES is new. `MSG_REPORT_CORRUPT_REPLICA` (0x4C) already clears the slot's `avali` bit and records it in the manager's
`extentCorrupt/<id>` bitmap, and `recovery_dispatch_loop` **force-dispatches a
marked slot regardless of the recovery gate** (`crates/manager/src/recovery.rs:1140`) — because a clear `avali` bit
cannot say *why* a slot is not serving, and a rotted full-length replica passes
`re_avali`'s `local_len >= sealed_length` test. The partition layer's WAL
self-heal is already a caller (`crates/partition-server/src/lib.rs:8991`), so
this design adds a second evidence source to a path that is proven.

**That RPC is PS-shaped and an extent node cannot use it.** It CAS-validates the
reporter against `partition/<id>`'s owner epoch, scopes the report by
`log_stream_id`, and its contract is that the reporter "confirmed at least one
OTHER replica decodes clean" (`crates/rpc/src/manager_rpc.rs:1744-1762`). An
extent node knows none of those: it is a byte store, it holds no partition
epoch, and finding its own block bad tells it nothing about its peers.

The evidence travels on `DfResp` instead — the at-most-once EN→manager heartbeat
that already carries `done_tasks` and `ec_done`, which the manager already
drains and acts on (`crates/rpc/src/extent_rpc.rs:947-968`). A node reporting
ITSELF needs no fencing, and that is what makes the simpler entry point sound: a
PS accusing another node must prove it is the owner, whereas a node saying "my
own copy is bad" can only ever cost itself. It is a wire change, so it carries a
fingerprint and a `WIRE_VERSION` bump.

Two guards carry over unchanged and are not optional: a report must not isolate
the LAST available replica, and it must not act on an extent whose eversion
moved since the scrub read it.

On an EC extent a slot is one shard, and "last" means K: isolation refuses when
fewer than K shards would remain available, because below K nothing can be
reconstructed and the damaged shard's range would go from wrong to gone.
`handle_report_corrupt_replica` — the partition server's report — still refuses
EC extents: its evidence is the same bytes read clean from another copy, and an
EC extent has no other copy of any byte. Only the shard's own node can tell.

## EC shards

A shard file gets its own sidecar, `extent-{id}.shard{i}.ck`, in the `.ck`
format with the length field holding the shard's length.

**Described as it is written, not read back.** A conversion streams shards far
faster than the scrub's few MiB per second could describe them afterwards, so
a backfill would make "no description yet" the normal state of every recently
converted shard. Each staged stripe is hashed from the bytes in hand
(`ExtentChecksums::append`, off the event loop) and the sidecar is persisted
after the stripe is durable, so it always describes exactly the bytes staged so
far; stripe and block boundaries need not agree. Stripe 0 starts a new
description (it truncates the file); a stripe that does not continue the
recorded description leaves no sidecar at all, and the scrub describes the
finished shard instead. A rebuilt shard is described the same way, from the
bytes the reconstruct produced, once they are durable.

**Verified where a shard is read whole.** A whole-block shard read is refused
on mismatch, like `.dat`. That covers a client's direct shard read (it then
reconstructs the shard from the others) and every source read of an EC rebuild,
whose stripes cover whole blocks — a rotted peer is refused and the rebuild
reads the next one.

**Scrubbed, and reported only once it is live.** The scrub takes an extent's
shard file when the node holds exactly one (more is reconcile residue, and
nothing on the node can say which index is live), verifies a block per pass,
and backfills a shard with no sidecar once the manager confirms the layout
publishes it at exactly a shard's length. A mismatch is REPORTED only after the
manager confirms the layout is committed to shard files: before that the shard
is staging — an abandoned attempt simply deletes it — and this node's slot is a
replica whose `.dat` may be fine. Reporting would isolate that. The converse
holds for `.dat`: once the extent is converted, rot in the `.dat` a holder keeps
until the next reconcile is about residue, and reporting it would isolate the
node's shard; it is dropped.

**An isolated shard never serves.** The client treats a sealed EC slot with its
`avali` bit dark as gone: reads of its range are reconstructed, it is never an
input to another shard's reconstruct (one wrong input makes every byte the RS
decode spans wrong), and the direct-read descriptor declines while a data shard
is isolated. Sub-block reads are not checksummed, so without this a rotted shard
would keep answering them until its rebuild finished.

Shards of a pre-CoW conversion live in `.dat` and are not covered.

## Non-goals

- **The append hot path is not checksummed per frame.** Content is hashed once,
  at seal.
- **Open extents are not covered.** Their content is still changing; the WAL
  record CRC covers the partition layer's own use of them, and a tail that has
  not sealed has not yet been replicated as authoritative.
- **This does not detect a lying peer.** It detects media rot and silent
  mis-writes. A node that computes a checksum over bytes it has already
  corrupted is a Byzantine problem this does not address.
- **RS reconstruction is not made self-checking.** Its inputs are checked where
  they are read whole (rebuild stripes); a client's sub-block reconstruct is not,
  and relies on isolation to keep a known-bad shard out of it.

## Acceptance

`crates/manager/tests/silent_corruption_rot.rs` began as a reproduction whose
legs passed because corruption went undetected; each is now the opposite
assertion. Every leg first waits for the replicas to describe the sealed
content, then rots one — rot before any description is the trust-on-first-use
window, not detection.

- **(a) read** — a whole-block read of the rotted replica is refused by its
  node, client reads return the original, and once the scrub has isolated the
  slot even 4 KiB reads do.
- **(b) recovery** — a rebuild whose only full-length source is the rotted
  replica finishes byte-exact or not at all.
- **(c) EC** — the rotted coordinator is isolated, and an EC read-back, if the
  extent ever converts, is the original.
- **(d) scrub** — `scrub_isolates_rot.rs`: with no read at all, the rotted
  replica's `avali` bit is cleared.
- **(e) EC shard** — `ec_shard_rot.rs`: a shard described as staged is rotted;
  its node refuses the block, the client reconstructs it, the scrub isolates
  the slot, a sub-block read while it is dark is the original, and recovery
  rebuilds the shard byte-exact on the spare.

Ablations, each red at the step it guards: no read check (a, b); no pre-encode
check (c); no shard read check, no staging description, no shard scrub, the
manager refusing EC isolation, or a client that reads a dark shard (e). The
rebuild's own description is not discriminated by (e) — the scrub's backfill
describes the new shard within a tick — and is pinned by
`a_durable_shard_is_recorded_at_its_length`.
