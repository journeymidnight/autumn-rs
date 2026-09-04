# At-rest integrity for stream-layer content

## What is protected today, and what is not

Every layer that owns a byte format checksums it. The stream layer does not own
one — it stores opaque bytes — and that is where the hole is.

| bytes | checksum | verified at | reference |
|---|---|---|---|
| RPC frame head | CRC32C over `[header][ctrl_len][ctrl]` | decode | `crates/rpc/src/frame.rs:143` |
| RPC frame bulk value | **none, by design** — "bulk value integrity is the transport's job" | — | `frame.rs` (its own round-trip test flips a value byte and asserts the frame still decodes) |
| partition WAL record | CRC32C **including the value** | replay | `crates/partition-server/src/wal_record.rs:220` |
| partition SST block | CRC32C, compared on read | every block read | `crates/partition-server/src/sstable/format.rs:134`, `:254` |
| stream `.meta` | CRC32C over the 48 metadata bytes | `parse_meta` | `crates/stream/src/extent_node.rs:4212` |
| stream `.dat` content | **none** | — | — |
| stream `.shard{i}` content | **none** | — | — |
| background scrub | **does not exist** | — | no match for `scrub` under `crates/stream/` or `crates/manager/` |

Two consequences follow, and the second is the serious one.

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

Planned:

| point | what it will do on mismatch |
|---|---|
| **EC conversion, before encoding** | refuse; do not turn corrupt bytes into parity. The coordinator today reads its local `.dat` unverified |
| **scrub** | clear this replica's `avali` bit and report it on `DfResp` |

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

`handle_report_corrupt_replica` refuses EC-converted extents
(`crates/manager/src/rpc_handlers.rs:1148`), on the
grounds that no shard-content checksum exists and a reader's failed shard read is
more likely congestion than rot. This design supplies exactly the missing
evidence, so that refusal is what must change for EC coverage — the bitmap and
the gate bypass are already slot-generic over `replicates ++ parity`.

## Non-goals

- **The append hot path is not checksummed per frame.** Content is hashed once,
  at seal.
- **Open extents are not covered.** Their content is still changing; the WAL
  record CRC covers the partition layer's own use of them, and a tail that has
  not sealed has not yet been replicated as authoritative.
- **This does not detect a lying peer.** It detects media rot and silent
  mis-writes. A node that computes a checksum over bytes it has already
  corrupted is a Byzantine problem this does not address.
- **RS reconstruction is not made self-checking.** Verifying a reconstruct needs
  shard checksums, which arrive with EC coverage; reconstruct itself stays as is.

## Acceptance

The reproduction harness `crates/manager/tests/silent_corruption_rot.rs` has
three legs that pass today **because** corruption goes undetected. Each flips to
a correctness assertion:

- **(a) read** — a client reading a flipped single replica gets correct bytes
  from another replica, or an error; never the corrupt bytes with `CODE_OK`.
- **(b) recovery** — rebuilding from sources including the corrupt one produces
  byte-exact content, or refuses; it does not launder.
- **(c) EC** — conversion over a corrupt replica reports an error instead of
  encoding corrupt parity.
- **(d) scrub** — with no external read at all, the corrupt replica's `avali`
  bit is cleared and the slot is rebuilt.

Each leg must be shown to fail without the corresponding change.

Increment 1 (the sidecar format, the seal-time write, and the read check) flips
none of them, for a reason worth stating: with no seal event on an extent node,
that flow never writes a sidecar, so there is nothing to verify against. The
scrub is what makes the legs reachable, and it is also what makes them fail.
