# autumn-fs multi-client consistency: inode lease — design

**Code**: `crates/manager/src/inode_lease.rs` (lease registry, TTL revoke) ·
`crates/manager/src/rpc_handlers.rs` (4 handlers + long-poll) ·
`crates/client/src/lease.rs` (SDK) ·
`crates/fs/src/lease_tasks.rs` (per-session heartbeat / poll / eviction) ·
`crates/partition-server/src/lib.rs` (`FenceFloors`, PS-side write fencing)

## 1. Semantics

The same fs tree can be opened by several frontends at once: the kernel mount `autumn-fuse`, the CLI `autumnfs`,
PyO3 `autumn.Fs`, and the `autumn-s3` gateway. All of them are lease clients of the manager. The manager
does not distinguish client families (`kind` is only a diagnostic label), so several kinds of frontend running against the same ino are covered by the same
lease / invalidation machinery.

| Scenario | Semantics |
|------|------|
| Several clients read the same file concurrently | Safe: read leases can have multiple holders |
| Several clients write the same file concurrently | Single writer: another client's write request returns Conflict (the mount maps it to EBUSY) |
| writer writes → reader elsewhere reads | close-to-open: visible as soon as the writer releases |
| Delete a file still open elsewhere | Data reclamation waits until there are no other holders (EXCLUSIVE) |
| Overwritten during an S3 GET | The GET reads the old content to the end (STABLE + REPLACE) |

Non-goals: POSIX byte-range locks (`fcntl(F_SETLK)`); merging multiple writers to the same file; atomic
rename (`dir::rename` is several non-transactional KV operations, and leases do not change that).

## 2. Why in the manager

manager + etcd is already the cluster metadata service; inode leases reuse it (the same source as the stream layer's
`acquire_owner_lock`) instead of introducing a second metadata system. Lease state lives only in manager
memory and in etcd `inode_leases/<ino>`; no keys are added to the data KV, and the fs key schema does not change because of leases.

## 3. State

One `InodeLeaseState` per ino: at most one **writer slot** (`writer` + `writer_kind`),
`readers`, `stable` (readers pinning the content), a monotonic `version`, and the force-acquire
`pending_revoke_at`.

- **Identity** `MgrClientId { kind, uuid, host }`: the lease key is `(kind, uuid)`; `uuid` is
  generated once per `FsState` (`DaemonClientId::new_fuse`) and does not change within a session. A fuse
  mount and a PyO3 `autumn.Fs` are each one client; autumn-s3 has one
  `FsState` per worker thread, so one gateway process is several clients. Every frontend reports kind FUSE; `host`
  is only for diagnostics and the manager never compares it.
- **version** starts at 1 and is incremented when the writer releases or is revoked. The `last_version` shadow value keeps
  the high-water mark across entry deletion / re-creation, so a re-acquire never reissues an `(ino, version)` that a stale cache
  still holds.
- **Persistence**: only `WRITE` writers write to etcd (leader-fenced). release and TTL revoke
  delete from etcd first, then change memory; acquire changes memory first, then writes etcd, and on etcd failure
  `revert_writer_acquire` rolls it back. Readers,
  STABLE, REPLACE and EXCLUSIVE are memory-only — losing the reader set on failover is benign (a client
  invalidates its whole cache on reconnect); replayed writer records are restored as `WRITE`.
- **Timing**: TTL `--inode-lease-ttl-secs` (default 30 s); clients renew all held leases every
  `--lease-heartbeat-secs` (default 5 s) (fuse, PyO3 and
  autumn-s3 run this background task; the `autumnfs` CLI is a one-shot command and does not renew); force-acquire grace
  `--inode-revoke-grace-ms` (default 5 s); the revoke loop runs every 1 s; each client inbox is capped at
  1024 events.

## 4. Modes

| Mode | Purpose | Coexistence with holdings of **other clients** |
|---|---|---|
| `READ` (1) | Read | Rejected only by EXCLUSIVE |
| `WRITE` (2) | In-place modification | Takes the writer slot; rejected by another's writer or STABLE |
| `STABLE` (3) | S3 GET pins the content | Rejected by another's WRITE / EXCLUSIVE; coexists with REPLACE |
| `REPLACE` (4) | S3 overwrite: swap the inode out from under the name | Takes the writer slot; excludes only other writers, readers and STABLE continue |
| `EXCLUSIVE` (5) | Reclaim inode data (after unlink) | Takes the writer slot; rejected by any holding of another client, and rejects every new lease while held |

The same client can hold read and write at the same time (ffmpeg faststart: a write fd + a read fd); conflicts only consider **other** clients.

Reclamation path: `reclaim_unreachable` / `reclaim_live` take EXCLUSIVE; if it cannot be obtained, the tombstone is left for
the periodic scan; deletion happens only once it is obtained, fenced by that lease's epoch. `reclaim_unreachable` re-acquires between
phases to re-confirm exclusivity.

## 5. Protocol (manager RPC `0x46`–`0x49`)

**Acquire (`MSG_ACQUIRE_LEASE`)** `{ client, ino, mode, force }` →
`{ code, lease: { version, writer_present, ttl_secs } }`. Conflicts (internally `WriteConflict`
/ `HolderConflict`) are both `CODE_PRECONDITION` on the wire. `force` only applies to "WRITE taking over WRITE": the first one pushes `WillRevokeIn` to
the current holder, opens a grace window, and returns `CODE_REVOKE_PENDING` (`eta_ms` is carried in `lease.ttl_secs`); if the writer releases voluntarily within the window,
the next acquire is granted directly; a force-acquire after the window expires revokes forcibly (version+1, pushes `LeaseRevoked`)
and grants. REPLACE / EXCLUSIVE holders are never taken over; STABLE readers are never taken over. Takeover pushes are staged first and
delivered only after the etcd commit; if the commit fails, the staged pushes are dropped and `revert_writer_acquire` rolls memory back — clients
never see a revoke that did not happen.

**Release (`MSG_RELEASE_LEASE`)** Writer release: clears the writer slot, version+1, pushes `WriterClosed` to all
readers of that ino (the close-to-open trigger). Reader / STABLE release does not change version;
releasing something not held returns NotHeld (idempotent).

**Heartbeat (`MSG_HEARTBEAT_LEASE`)** Renewal returns `Renewed { version, … }`; NotHeld
means it has been revoked or has expired, and the client drops the local lease and its cache.

**PollInvalidations (`MSG_POLL_INVALIDATIONS`)** The RPC is req/resp, so pushes are implemented as a server-side
long-poll: drain the inbox first, and if it is empty, park for up to 10 s; `push` wakes the parked waker, so "writer
close → reader sees it" takes milliseconds. Each inbox has at most one waker; a displaced poll returns with no events.

Event `MgrInvalidation { ino, version, kind }`:

| kind | Value | Meaning |
|---|---|---|
| `WRITER_CLOSED` | 1 | Writer released, version already incremented |
| `LEASE_REVOKED` | 2 | Forcibly revoked by the manager (TTL expiry / takeover) |
| `META_CHANGED` | 3 | `ino == 0` is the inbox-overflow sentinel: the client invalidates everything |
| `WILL_REVOKE_IN` | 4 | Pushed to the current writer: someone force-acquired; the `version` field is the grace period in milliseconds |

## 6. Failures and recovery

| Case | Handling |
|------|------|
| Writer TTL expires | Two phases: delete the etcd record first, then revoke in memory (version+1) and push to readers |
| Reader / STABLE TTL expires | Removed silently |
| manager failover | The new leader reinstalls writers from etcd (`install_persisted_writer`, deadline clamped to the TTL) |
| Poll failure / overflow sentinel | The client drops all held leases and its cache |

`tick` handles writer revokes before evicting expired readers, so a reader that expires right at the writer's TTL boundary still
receives the push.

## 7. Invariants

1. **The manager is the only lease decision maker**; the write path acquires first, and conflicts are returned synchronously.
2. **The writer flushes dirty buffers before releasing**; the manager increments version before pushing — when a reader receives
   the event the new data is already in KV, and the event is paired with the new version.
3. **Client caches are tagged by version**; an invalidation is a veto: a cache-stale read reloads the
   extent before serving, otherwise it returns EIO, and never serves bytes from before the close.
4. **A broken poll channel = invalidate all caches**; the overflow sentinel takes the same path.
5. **Renewal failure = self-revoke**; writes on a revoked lease fail fast.
6. **The write lease is obtained before the first Put**. Every write carries `(inode_hint = ino, lease_epoch =
   version)`; the PS keeps a fence floor per ino: `lease_epoch < floor` is rejected with `CODE_FENCED`,
   a higher epoch raises the floor and writes an `OP_FENCE_BUMP` WAL record before the ACK (restored by checkpoint +
   replay). Anonymous writes without a lease (`WriteLease::ANON`, `lease_epoch = 0`) do not take part in fencing,
   so a single frontend works as before.
