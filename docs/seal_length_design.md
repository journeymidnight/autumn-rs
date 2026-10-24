# Sealed length vs. the copies on the extent nodes — design

A sealed extent has one length, `sealed_length`, recorded by the manager. The
copies on the extent nodes do **not** all have that length: a copy can be
**longer** or **shorter** than `sealed_length`, and both are normal states the
system is built for. This document states why, what each case means, and which
code keeps it safe. It follows the stream layer of Windows Azure Storage
(SOSP 2011, §4.3.1).

---

## 1. What an append guarantees

An append is acknowledged to the writer only after **every** replica has
written (and fsynced) it (`client.rs::apply_completion`). So the acknowledged
prefix of an extent is present on every replica. A replica can hold more than
that prefix: an append that reached some replicas and failed on another leaves
bytes the writer never saw acknowledged.

Each append's header carries `commit`, the acknowledged length up to the
previous append; a replica truncates anything past it before writing. That
keeps an **open** extent's replicas converging on the next append, but a sealed
extent gets no next append, so whatever divergence it had at the seal stays.

## 2. The two ways an extent is sealed

| path | when | `sealed_length` | `avali` bits |
|---|---|---|---|
| **authoritative** (`seal_commit = Some(c)`) | the writer that owns the stream seals its own tail: rolling over, or rolling past a failed append (`SealCommit` handshake → `alloc_new_extent`) | exactly `c`, the writer's acknowledged length; no replica is asked | all set: every replica acknowledged `c`, so every replica holds at least `c` |
| **probe** (`seal_commit = None`, `MSG_SEAL_TAIL`) | an owner without a commit of its own: **every partition open** seals its open tails this way, after the takeover fence and before replay; also a roll with no live writer (`seal_and_roll_tail`) and an open tail whose commit cannot be read from every replica (`ensure_tail_initialised`) | the **minimum over the replicas that answer** (`compute_commit_seal`), catching-up replicas excluded; one answer is enough | set for the replicas that answered; a replica that did not answer is left unset |

Both are in `crates/manager/src/rpc_handlers.rs` (`handle_stream_alloc_extent`,
`handle_seal_tail`, `probe_commit_seal`, `compute_commit_seal`).

### Why a partition seals its tails when it opens

Replay must read a length that never changes afterwards. An open tail has no
such length: replay would read up to what the replicas it reaches hold, while
the next append computes its commit over **all** replicas. A replica missing at
open and back before the first write then has the extra record truncated off
the log, after clients may have read it. Sealing first fixes the length in the
manager; the first write after the open allocates a fresh tail, as rolling over
does. The seal comes after the takeover fence, so the previous owner cannot
append past it.

Every open tail is sealed, even when every replica answers with the same
length. Agreement at the probe proves nothing about the next moment: an append
from the previous owner that passed an extent node's fence check before the
fence landed still writes its bytes (only its acknowledgement is refused), so
a replica can grow right after answering. A sealed length is immune — readers
clamp to it. The cost is one extent per stream per open after writes; a tail
left sealed by an earlier open (no writes since) is reused as it is.

### Why the probe does not wait for every replica

A probe seal happens exactly because something is down. If it waited for every
replica, an extent node that never comes back would keep the partition from
ever opening.

### Why it does not seal at the acknowledged watermark either

The replicas only learn the acknowledged length from the **next** append's
`commit`. The last batch acknowledged to a client has no next append if the
server dies right after acknowledging it, so a watermark known to the replicas
can sit below acknowledged data, and sealing there would lose it. Pushing the
watermark to the replicas before acknowledging costs another round trip on
every write.

So the probe seals at the minimum over the replicas that answer. That minimum
is never below the acknowledged length (every replica holds the acknowledged
prefix), and it may be above it.

## 3. A copy against `sealed_length`

| the copy is | how it happens | what it means |
|---|---|---|
| **equal** | the usual case | — |
| **longer** | the replica received bytes past the seal: an append that never completed on every replica, under either path | the bytes past `sealed_length` are never read. The node keeps them (it logs `BUG2 UNDER-SEAL` when a seal lands below its file length) and every reader clamps to `sealed_length`: replay reads through `read_committed_bytes_from_extent`, and value pointers only name acknowledged bytes |
| **shorter** | **only** a replica that did not answer a **probe** seal, when the replicas that did answer held more than it — an append that reached them but not it | its `avali` bit is unset, so reads skip it (`eligible_replica_slots`), and it is caught up in place when its node answers again (§4) |

A shorter copy cannot come from the authoritative path: every replica
acknowledged `c`.

Measured on a local 4-node cluster (3 replicas), killing an extent node and the
partition server together under write load, then restarting the partition
server while the node was still down: of 10 copies left out of a probe seal, 9
already held `sealed_length` and one was 132,854 bytes short of a
2,569,063,352-byte seal. Restarting extent nodes alone while the partition
server kept running produced no copy left out of a seal (every seal took the
authoritative path).

## 4. Catching a copy up: `re_avali`

The manager's recovery tick (`recovery.rs`, every 2 s on the leader) sends
`re_avali` to a replicated, sealed extent's copy whose `avali` bit is unset,
once its node and disk answer, no other operation holds the extent, and the
slot is not backing off. The extent node:

- answers at once when its copy already holds `sealed_length` (a length check
  only; see the open bug below);
- otherwise copies the WHOLE extent from a peer into a temporary file and
  renames it over its own only once the full `sealed_length` has landed, so a
  copy that cannot be completed never loses what it had.

It does not fetch only the missing tail. Only the bytes up to the last commit
acknowledged to the writer are the same on every copy; past it, a copy left out
may hold an append the others never got, at an offset where the others hold a
different one that the seal kept. Appending the tail to it would make a copy of
the right length with wrong bytes, served as healthy. The extent node does not
receive the acknowledged commit, so it cannot tell where to start. Each outcome
is logged ("nothing copied" / "copied the whole extent … elapsed_ms").

The same blind spot sits under every length-only judgement past the
acknowledged commit, and is open (BUG-SEAL-DIVERGENT-UNACKED-BYTES, not
reproduced): a probe seal lights every member that answers by length alone, and
`re_avali`'s "already holds the sealed length" is a length check too, so a
member holding a different append at the same offsets could be served.

On success the manager sets the bit. This is not a rebuild: the copy stays on
its node and slot, nothing enters the operation ledger. A copy that is full
length but wrong is a different case (corrupt, rebuilt elsewhere;
`extent_corrupt.rs`).

## 5. A write whose outcome is unknown

Because a probe seal can keep bytes the writer never saw acknowledged, a write
that failed or timed out may still be there after the partition reopens. Its
outcome is **unknown**, not failed: the system guarantees only that once the
seal is recorded, the answer never changes.

So a partition never answers a write as failed while its records may sit on
an open tail. When the stream client gives up on a log append it had already
sent (the alloc after an append error failed, or the roll limit was hit; the
error carries `APPEND_OUTCOME_UNKNOWN`), the tail is still open, and the
partition poisons itself and reopens without serving anything more; the open
seals the tail before replay, which settles the batch. The batch's writers are
answered `OutcomeUnknown` (status 9, batch code `CODE_OUTCOME_UNKNOWN`), or lose
the connection when the partition's thread exits; writes queued but never sent
are answered `Unavailable` ("not applied"). A failure before anything was sent
is answered `Unavailable` and the partition keeps serving.

The SDK follows HBase's client. It retries a put, a delete or a batch op on
both answers — sending one again leaves the same key in the same state, whether
or not the first attempt survived — and returns `AutumnError::OutcomeUnknown`
when the write fails in the end after an attempt that may have been applied
(the server said so, or the request was sent and no answer came back); every
other write error means not applied. A conditional write (`compare_put`) is not
idempotent — a retry can report a conflict caused by its
own first attempt — so it returns `OutcomeUnknown` at its first such attempt.

HDFS block recovery and HBase behave the same way: block recovery syncs a
block to the minimum length among the replicas it can reach, and HBase replays
every edit it finds in the log, including ones whose client saw an error.
HBase's nonces exist for the operations that are not idempotent.
