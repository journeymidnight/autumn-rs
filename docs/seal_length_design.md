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
| **probe** (`seal_commit = None`) | a new owner takes the tail over without a commit of its own: the partition reopened after its server died, or the open tail's commit cannot be read from every replica (`ensure_tail_initialised`) | the **minimum over the replicas that answer** (`compute_commit_seal`), catching-up replicas excluded; one answer is enough | set for the replicas that answered; a replica that did not answer is left unset |

`handle_check_commit_length` (a partition reopening, before replay) uses the
same minimum over the replicas that answer to decide where replay ends.

Both are in `crates/manager/src/rpc_handlers.rs` (`handle_stream_alloc_extent`,
`compute_commit_seal`, `handle_check_commit_length`).

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

- answers at once when its copy already holds `sealed_length`;
- otherwise copies the extent from a peer into a temporary file and renames it
  over its own only once the full `sealed_length` has landed, so a copy that
  cannot be completed never loses what it had.

On success the manager sets the bit. This is not a rebuild: the copy stays on
its node and slot, nothing enters the operation ledger. A copy that is full
length but wrong is a different case (corrupt, rebuilt elsewhere;
`extent_corrupt.rs`).

## 5. A write whose outcome is unknown

Because a probe seal can keep bytes the writer never saw acknowledged, a write
that failed or timed out may still be there after the partition reopens. Its
outcome is **unknown**, not failed: the system guarantees only that once the
seal is recorded, the answer never changes. The partition server answers a
write whose log append failed with an `Internal` status; the SDK maps it to
`ServerError` and retries it; sending a put or delete again leaves the same
key in the same state, whether or not the first attempt survived.

HDFS block recovery and HBase behave the same way: block recovery syncs a
block to the minimum length among the replicas it can reach, and HBase replays
every edit it finds in the log, including ones whose client saw an error.
HBase's nonces exist for the operations that are not idempotent.
