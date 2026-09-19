# Client Wire Compatibility

An upgrade stops and restarts the manager, partition servers and extent nodes
together. It does not stop the processes that hold an embedded client — an
inference pod, a mounted fuse daemon, an s3 gateway, a python wheel inside
somebody else's image. Those reconnect to the upgraded cluster on their own
schedule, so the cluster serves a client older than itself.

---

## 1. One version integer, two constants

`crates/rpc/src/lib.rs` carries TWO constants over ONE numbering space:

| Constant | Meaning |
|---|---|
| `WIRE_VERSION` | the schema this binary speaks |
| `MIN_CLIENT_WIRE_VERSION` | the oldest CLIENT it serves |

They are the two ends of the client window `[MIN_CLIENT_WIRE_VERSION,
WIRE_VERSION]` — the same shape a MongoDB server reports to a driver as
`minWireVersion`/`maxWireVersion`. A cluster peer is not a point in that window;
it must speak `WIRE_VERSION` exactly (§1.1), which is what keeps the internal
protocol stop-the-world.

There is no separate constant for the cluster floor. "The oldest peer I
interoperate with" pinned equal to the version I speak carries no information,
and a `MIN`/`MAX` pair whose `MIN` answers a different question than its `MAX` is
what made the earlier three-constant spelling unreadable.

`MIN_CLIENT_WIRE_VERSION` moves ONLY when a change breaks the client-facing
surface, which makes it the one constant a reviewer has to look at to answer
"does this force every embedded image to be rebuilt". Raising `WIRE_VERSION`
alone — the common case — leaves every client inside the window untouched.

The etcd `cluster_version` key is unrelated: it is the operator-bumped feature
gate for persisted formats, and its code compares against `WIRE_VERSION` only.
`MIN_CLIENT_WIRE_VERSION` is a property of a binary, not of a cluster, because
stop-the-world already makes every server agree. The pair is the same split
Kafka draws between `inter.broker.protocol.version` and the per-API versions a
client negotiates, and MongoDB between `featureCompatibilityVersion` and the
wire version.

### 1.1 The two checks are different questions

`GetClusterIdResp` is frozen (§4), so its FIELD names outlive the constants they
carry:

| Frozen field | Carries |
|---|---|
| `wire_version_max` | `WIRE_VERSION` |
| `wire_version_min` | `MIN_CLIENT_WIRE_VERSION` |

The floor goes in the `wire_version_min` slot because the floor is what a client
needs, and a client already deployed reads that field with code that cannot be
changed. **The constant names and the field names are deliberately not the same
words; both sides carry a comment saying so.**

Reporting a floor there is what rules out an interval-overlap test for cluster
peers, and it fails in the dangerous direction: overlap accepts whenever
`max(LOCAL_MIN, remote_min) <= min(LOCAL_MAX, remote_max)`, so a stale PS at 44
meeting a cluster reporting `[44, 45]` overlaps and is ADMITTED — and this
handshake is the only thing enforcing stop-the-world.

**INVARIANT: a cluster peer requires equality, a client requires membership.**

- PS and EN, checking the manager (`crates/partition-server/src/lib.rs`,
  `crates/server/src/bin/extent_node.rs`): admit iff
  `resp.wire_version_max == WIRE_VERSION`.
- A client, checking the cluster: admit iff
  `resp.wire_version_min <= WIRE_VERSION(client) <= resp.wire_version_max`.

Overlap is the wrong shape for both, so there is no shared helper: the two
questions are two functions, and each names in its refusal which of the two
fixes applies.

Without this the window cannot open at all. A cluster at `WIRE_VERSION = 45`
with the floor at 44, answering `[45, 45]` to an in-window client at 44, makes
that client compute `lo = 45 > hi = 44` and **refuse itself** at connect.

## 2. The client-facing surface

The window covers what an embedded client encodes or decodes:

- `partition_rpc`'s data plane — Put / Get / Delete / Head / Range, the three
  batch families and their bulk forms, `MSG_GET_REDIRECT(_MANY)`, `AUTH_HELLO`.
  (Server-side multipart upload is retired: `MSG_PUT_BEGIN`/`CHUNK`/`COMMIT`/
  `ABORT` are reserved opcodes with no handlers, and stream writes are ordinary
  Puts under a striped key.);
- `manager_rpc`'s runtime subset — `GetRegions` and the `MgrRegionInfo` /
  `MgrRange` / `MgrPsDetail` it carries, `MintToken`, `AllocInodes`, the inode
  lease and invalidation messages, `ClusterDf` (a mounted fuse daemon answers
  `statfs` from it, `crates/fuse/src/dispatch.rs`);
- `cap_token.rs` in full;
- `extent_rpc`'s READ subset — `ReadBytesReq` and `PayloadRef`, reached by
  `read_extent_value_direct` (`crates/stream/src/client.rs`) because
  `--direct-read` is on by default.

EC conversion, recovery, `WriteShard`, `df`, reconcile, split/merge, the
op-ledger and the dashboard are outside it: they are manager↔PS↔EN traffic, or
they belong to `autumn-op`.

`autumn-op`'s messages are compiled into the same `autumn-client` crate that an
embedded client links, so "belongs to autumn-op" is not a property the linker
can see. The boundary is therefore a `const` set of client-surface msg_types
that the admission rule in §5 reads, and a test asserts that set against the
msg_types the SDK's data-plane entry points emit — so adding a data-plane
message without classifying it fails, rather than silently landing outside the
window.

### 2.1 `frame.rs` is frozen, not windowed

The frame layer cannot be in the window, because a frame change cannot be
negotiated: a peer whose frame format differs fails the CRC before anything is
decoded, and both servers tear the connection down at that point
(`crates/partition-server/src/lib.rs`, `crates/manager/src/rpc_handlers.rs`).
A refusal message cannot be delivered, and the hello cannot be reached. So for
as long as a window is promised, `frame.rs` is frozen — reshaping it is a
stop-the-world event for clients too, and the window does not soften it.

## 3. The negotiated version belongs to the connection

It governs ADMISSION (§5) and what the server may send unprompted; it never
decides how received bytes are read. That is the msg_type's job (§7), so a frame
stays self-describing and nothing depends on per-connection state being right.

A request carries no version field. Two reasons, and the first is decisive:
rkyv has no optional fields, so adding `version: u32` to every client-facing
request struct rewrites the layout of the whole surface at once — the largest
possible incompatible change, made in the name of future compatibility. The
second is that the value is constant for a connection's life, so per-frame bytes
buy nothing. The 10-byte frame header has no room either (`req_id` 4,
`msg_type` 1, `flags` 1, `payload_len` 4; flags has four spare bits, which
cannot express an interval, and §2.1 forbids reshaping the header anyway).

The two directions need different carriers:

- **server → client** already has one: `GetClusterIdResp`, checked at
  `ClusterClient::connect`. The client keeps the number
  (`ClusterClient::negotiated_cluster_wire`); nothing branches on it until §7.
- **client → server** has none. `MSG_AUTH_HELLO` cannot serve: it carries no
  version, and a client with no credential never sends it at all
  (`crates/client/src/lib.rs` sends it only when a credential is configured), so
  an authz-disabled cluster — fuse, kvcache, every dev cluster — sees it never.

## 4. `MSG_CLIENT_HELLO`

A new msg_type, sent once per connection before any other frame, on every
manager and PS connection a client opens. Being a new msg_type it is additive:
it changes no existing struct.

It is **hand-coded fixed-layout binary**, like `ReadBytesReq` and the other
extent hot-path codecs, NOT rkyv — `[magic: u32][client_wire_version: u32]`,
answered with `[code: u8][server_wire_version: u32][min_client_wire_version: u32]`.
Its own field names match the constants, because unlike `GetClusterIdResp` it is
new and nothing already deployed reads it. The
negotiation channel is the one message whose cross-version decode cannot be
allowed to go wrong, and rkyv's archived root sits at the END of its buffer
(`root_position = size - size_of::<T>()`), so a decoder reading a longer peer's
struct reads its SUFFIX: a two-`u64` struct decoding a three-`u64` one returns
`Ok` with the fields shifted, and an added `u32` that fits tail padding
round-trips `Ok` in both directions with the new field reading zero. Only
`Vec`/`String` shapes fail loudly, and a negotiation message must not depend on
its own shape for that.

**The introduction commit moves no version and changes no `Archive` type.** It
adds one msg_type and the constant. This is what makes §5 inert on arrival: a
client at the introduction version and one built the day before are byte-identical
on every other message. It also settles a rule the tree has followed until now —
that a pure opcode addition is still a `MIN = MAX` bump. From this commit a new
msg_type is not a bump, because an old peer that never sends it cannot be
affected by its existence.

**INVARIANT: the negotiation messages are frozen, and a test holds them there.**
`GetClusterIdReq`/`GetClusterIdResp` and the hello are pinned by golden encoded
bytes — a fixed instance, asserted byte for byte; rkyv's writer zeroes padding
before resolving, so the encoding is deterministic. A comment declaring them
frozen has already failed once: `GetClusterIdResp` lost its `wire_fingerprint`
field in a commit that left `WIRE_VERSION` unchanged on both sides, which no
per-bump review can see. **The test's failure message prescribes the action —
"this is a client-facing break: raise `MIN_CLIENT_WIRE_VERSION`" — and never "update the
recorded bytes".** The deleted schema fingerprint failed exactly there: each
false alarm taught the reflex of refreshing the recorded value, which is how a
real change gets waved through. A golden vector's one false-alarm mode is an
rkyv upgrade that changes the archived format, and that IS a client-wire break,
so the prescribed action is still right.

Neither this test nor anything else catches a change that leaves the bytes alone
and moves what a field MEANS. That stays a review obligation (§7).

## 5. Admission is scoped to the client surface, not to the connection

A server admits a client iff its version falls INSIDE the window:

```
MIN_CLIENT_WIRE_VERSION <= client_wire_version <= WIRE_VERSION
```

Both bounds refuse, for different reasons. Below the floor, the server no longer
keeps the behavior that client needs (§7). Above `WIRE_VERSION`, the server
cannot speak what the client will send — and that direction is not hypothetical
here: images are built from `main`, so a wheel routinely runs ahead of a cluster
that has not been upgraded yet (the shape recorded as BUG-WIRE36-UNDEPLOYED).
A ceiling refusal names the cluster's version, because the fix is to deploy or
to rebuild, and the operator needs to know which.

A connection that sends no hello is treated as the version in which the hello
was introduced. Once `MIN_CLIENT_WIRE_VERSION` rises above that version, such a
connection is refused — **but only for the client-surface msg_types of §2.**

The scoping is not a refinement; without it the first floor move is a cluster
outage. The manager and PS listeners that serve clients also serve internal
peers, and that peer traffic is silent: PS→manager and EN→manager go
through `autumn_stream::ConnPool` straight to `RpcClient::connect` with no
handshake, and manager→PS drives `MSG_SPLIT_PART`,
`MSG_MAINTENANCE`, `MSG_MERGE_FREEZE` and `MSG_ROLL_TAILS` the same way. Nothing
in a frame says which role sent it. A connection-scoped refusal would therefore
reject `register_ps`, heartbeats, `register_node`, reconcile, split, merge-freeze
and roll-tails the moment the floor moved. Scoping by msg_type also leaves the
89 test files that open a raw `RpcClient` to a PS or manager working unchanged.

The one peer that does handshake is the extent node's startup identity check
(`verify_manager_cluster_id` / `register_with_manager`), which is a
`ClusterClient`. It runs at `WIRE_VERSION`, so it is always admitted, and the
two messages it sends are un-gated anyway.

`MSG_GET_CLUSTER_ID` and the hello are exempt: they are how a peer finds out
what it is talking to.

**Placement.** On the PS the check sits where the `MSG_AUTH_HELLO` arm sits —
inside `authz_gate`, **above** its `if !authz.gate_active() { return None }`
early return. Below that line it would never run on an authz-off cluster, which
is most of them. The manager has no equivalent slot: `handle_connection` decodes
sequentially and spawns one detached task per frame, so the per-connection value
is captured in the decode loop before the spawn.

**A new client meeting an old server.** The PS answers an unknown msg_type as a
misrouted frame, not as "unknown": `extract_part_id` returns `0` for anything it
does not know and partition ids start at 1, so the hello comes back `NotFound`,
indistinguishable from a real misroute. The manager answers `InvalidArgument`.
A client treats either reply to the hello as "server predates the hello", and
then falls back to the §1.1 membership check, which refuses it because its own
`MAX` exceeds the server's.

**INVARIANT: the server decides admission.** The client's startup self-check
stays as an early, better-worded failure, but it cannot be the only gate: it is
skipped when the transport fails fetching `GetClusterIdResp` (an `if let Ok`
around the fetch). The manager and the partition server each validate an
incoming client; the extent node has no version code and acquires none (§8).

**`MSG_GET_REGIONS` is the one message the msg_type boundary cannot cut**, and
it was not anticipated above. An SDK routes with it and so does every partition
server's `sync_regions_once`, so it is on BOTH surfaces at once. A PS sends no
hello, so gating it would refuse region sync fleet-wide the moment the floor
rose — the same outage this scoping exists to prevent, arriving through the set
instead of through the connection. It is therefore left un-gated. What that
leaves is a below-floor client able to fetch routing: every data-plane message
it then sends is refused, which is where the damage would be. Closing it
properly means giving a cluster peer a way to identify itself, which is a
different change from this one.

The OPERATOR surface is uncovered by the same kind of decision but for a
different reason. `MSG_STATUS`, stream/extent info, `namespace_*`, `tenant_*`,
the op-ledger, autopolicy and `MSG_MULTI_MODIFY_*` are `autumn-op`'s messages,
and `autumn-op` ships WITH the cluster at the same commit — it is never out of
window in practice, so maintaining a second window for it buys nothing. What
that concedes is that a STALE `autumn-op` still decodes rkyv admin structs
cross-version, which §4 establishes is only sometimes loud. The trade is
recorded here rather than left to be discovered.

## 6. Where the number is kept

**Client** — one `Cell<u32>` on `ClusterClient`, set from `GetClusterIdResp`.
One value, not one per peer: stop-the-world means every server reports the same
`MAX`. The hello is sent from the two functions that OPEN connections —
`mgr_client()` and `get_ps_client()` — not from `connect()`, because
`rotate_manager` and the `mgr_call` error arm drop a manager connection and
`mgr_client()` silently reopens it.

**Server** — one `u32` per connection, beside the bound principal. The PS
already carries per-connection state of exactly this shape
(`let mut principal: Option<BoundPrincipal>`, threaded `&mut` into `authz_gate`
from both the frame dispatch and the bulk-write drain), including the discipline
that makes it safe: it is mutated synchronously in the gate, before any await in
the calling dispatch, so the borrow never spans an await.

## 7. Serving two forms

**A new form of a client-facing message takes a NEW msg_type. The old form keeps
its opcode and its struct, untouched, until the floor passes it.** The server
serves both; the old struct is deleted when `MIN_CLIENT_WIRE_VERSION` rises past
the version that introduced its successor. Each retained form names that version
where it is defined, so the window's width is a count of forms somebody is
maintaining rather than a number chosen in the abstract.

**The discriminator is in the bytes, never in connection state.** A frame's
msg_type sits in the header and is read before any decode, and a response echoes
its request's msg_type, so both directions are self-describing. The alternative
— one opcode whose meaning depends on the version recorded for that connection —
makes a frame decodable two ways with nothing in it to say which, and rkyv
mis-decodes SILENTLY (§4). A hello that was missed, or a per-connection value
recorded wrongly, would then be a silent misread rather than a refusal. This is
also what keeps §3 honest: the connection's version decides ADMISSION and what
the server may send UNPROMPTED (a lease invalidation pushed to a fuse client);
it never decides how received bytes are interpreted.

New opcodes are cheap here precisely because §4 stopped counting them as bumps.

**`one_definition_only!` does not extend to this surface, and the reason is the
window itself.** That guard (`crates/rpc/src/extent_rpc.rs`) is an identity
function per message that compiles only while two modules name the SAME type, and
it exists because mirrored copies of one message — the manager encoding through
its own `ExtDfReq` while the node decoded through `extent_rpc`'s — mis-decoded
silently when a field landed on one side. What made that lethal was not that two
definitions existed but that **nothing decided which one the bytes were**. The
cluster-internal schema keeps the guard at full strength: stop-the-world means
two versions of an internal message are never live at once, so a second
definition there is always a mirror. On the client surface two versions ARE live
by construction, so the invariant is restated rather than inherited:

**INVARIANT: one definition per (message, version), and every definition is
reachable only through its msg_type.** Two forms of a message are legal; two
definitions that the same bytes could land in are not.

This is the mechanism no encoding can supply. The changes that break a client
and are not fixable by any framing — a key layout whose meaning changed, a
descriptor field that re-interprets its neighbour, a per-item status that used to
fail a whole batch — are behavioral. A tagged encoding makes the additive subset
WORSE: `GetRedirectResp`'s `ec_data_shards` is a discriminator that changes what
`replica_addrs` means, so rkyv's layout shift is what makes an old client fail
loudly, while a decoder that skipped the unknown field would succeed and then
read EC shard bytes as a value.

## 8. The extent-node edge

A client reads directly from an extent node with no handshake, and the EN has no
version concept. It does not acquire one. The descriptor that authorises the
read comes from the PS, the PS knows the connection's version, and a client
below the floor for the direct-read form is DECLINED the descriptor and proxies
instead.

That reuses paths that already exist and are exercised. The batched form
declines per item with `CODE_PRECONDITION` and the client proxies only those
items (`redirect_item_action`); the single-key form declines by inlining the
value with `extent_id: 0`. Either way the proxy read is authoritative. The
direct-read wire form therefore stays single-form and the EN decoder is
untouched, where the alternative is teaching the EN's hand-rolled fixed-width
codecs to accept two widths.

**INVARIANT: a client holds no extent-node address across calls.** This is what
makes the edge closable from the PS side alone. It holds today — the direct-read
helpers take the address from the caller, and every caller passes
`resp.replica_addrs` out of a descriptor obtained in that same call, with no
descriptor cache anywhere in the client crate. A descriptor cache would reopen
the edge and force a version onto the EN.

## 9. Five schemas, five homes

Every schema in the tree is one party writing for another. The parties, not the
medium, are what decide the discipline — a format written for a FUTURE self
evolves under a magic and a version it carries itself; a format written for a
LIVE peer evolves under a handshake. §9.1 is why those cannot be the same rule.

| Schema | Written for | Medium | Self-versioned |
|---|---|---|---|
| SST / WAL record / checkpoint | a future PS (or one taking over) | an extent — the EN carries the bytes without interpreting them | `MAGIC "AU7B"` + `FORMAT_VERSION` |
| `.meta` / `.ck` | a future EN, and an EN rebuilding from a peer | EN local disk | `EXTMETA\0` / `\x01` / `\x02`, all three still parsed |
| manager records | a future manager | etcd | **nothing — it borrows `WIRE_VERSION`** |
| cluster wire | a live peer | network | `WIRE_VERSION`, equality |
| client wire | a live client | network | the window |

The partition server writes NOTHING to a local filesystem; its durable state is
manager records plus stream contents. So an SST format change is a PS-to-future-PS
matter that the extent node never sees, which is why `FORMAT_VERSION` is already
independent of `WIRE_VERSION` — and correctly so.

Three of the five already carry their own magic and version and owe nothing to
the wire. The manager's records are the exception, and only because they are
defined as rkyv structs inside the wire schema files: a value no other component
decodes therefore costs a wire bump, which under `MIN == MAX` restarts the
partition servers and the extent nodes too. Fixing that is not a new discipline
to invent — it is the one `.meta` already uses. It is tracked separately from
this design.

### 9.1 Why a wire change and a stored-format change are not the same event

Both are "the bytes changed shape", and stop-the-world is the answer to exactly
one of them. The difference is what happens to the OLD bytes at the moment
everything restarts.

**Wire bytes are in flight, and they die.** A request's bytes exist for the
length of that request. Stop the world and no byte of the old shape exists
anywhere afterwards, so the compatibility requirement is SIMULTANEITY — every
live peer agreeing at one instant — and a restart satisfies it completely. That
is why `WIRE_VERSION` equality is a sufficient rule and needs no migration, no
old parser, and no format stamp.

**Stored bytes survive the restart.** Everything etcd and the extent files hold
is still there when the new binary comes up, written by the binary that just
died. The requirement is not simultaneity but RANGE OVER TIME: the new reader
must handle every version ever written and not since rewritten. Stopping the
world achieves nothing here — there is no instant at which the old data stops
existing.

| | wire change | stored-format change |
|---|---|---|
| Who must agree | every live peer, at one instant | the writer and every future reader |
| Does stop-the-world settle it | **yes, completely** | **no** — the old bytes are still there afterwards |
| What it needs | a handshake plus a simultaneous swap | the new binary parses every old version (`.meta` still reads V0 and V1), or a migration plus a format stamp |
| Rolling back | safe — stop again, put the old binaries back, nothing persists | **unsafe** — the old binary meets bytes from the future; this is what `parse_cluster_version` refuses to start on |
| When it is got wrong | the handshake refuses: loud, at startup, before anything is served | the old bytes decode as something else, and with rkyv that is only SOMETIMES loud (§4) — the damage is to data already on disk |

The last row is the one that decides how much care each deserves. A wire mistake
costs a refused connection. A stored-format mistake is a data-corruption event,
and it is discovered later, by a reader that had no way to know the bytes were
not written for it.

This is also why a wire bump must never be treated as cover for a persisted
change. The manager's records get one today by accident (§9), and that accident
runs in the harmless direction — a bump the persisted change did not need. The
reverse, letting the stop-the-world window stand in for a migration, is the
trade that loses data, and no version equality check can catch it.

### 9.2 `cluster_version` is the rollback latch, and it gates stored formats only

It follows from §9.1. A wire change is settled by the restart, so after the swap
there is nothing a gate could still decide. A stored-format change is not
settled, and what it needs is a point in time after which it is safe to START
WRITING a shape the previous binary cannot read. That point is "every member runs
the new binary and we are not going back", which is precisely what an operator
asserts by bumping — and why `autumn-op upgrade-version` prints that rollback is
no longer possible.

So `cluster_version >= N` answers one question, "may I write the new format yet",
and never "which form is on the wire". The cap at the binary's own wire version
REUSES that numbering to keep the interlock a single comparison; it does not make
this a wire version. The interlock is the latch's other side: a manager refuses
leadership when the persisted value exceeds what its binary knows, so a
rolled-back binary cannot come up against data written past its horizon.

Nothing gates on it today, which is the intended state — the mechanism exists and
carries no resident evolution code until a persisted change needs it.

A type that is BOTH stored and sent needs both mechanisms at once, and that is
the §9 accident rather than a design: after the split the stored half gates on
`cluster_version` and the sent half on version equality, each under the rule that
fits it.

Types that straddle two homes are the residue of the same accident:
`PayloadLocation` is a wire enum (`crates/rpc/src/extent_rpc.rs`) whose
`as_byte()` value is persisted at `.meta` byte 41, and its `from_byte`'s
"unknown decodes to `InDat`, never an error" is a PERSISTENCE decision living on
a wire type. Bounded today by stop-the-world and by the no-rollback rule, and
listed here because the byte's meaning belongs to the format that stores it.

## 10. What stays outside

- **Rolling upgrade of the cluster.** The manager/PS/EN protocol keeps
  `MIN == MAX`, now enforced by exact-`MAX` equality (§1.1) rather than by
  interval overlap.
- **The encoding.** rkyv stays. A tagged encoding over the client-facing subset
  buys one clean case out of nine measured field additions, and converts a loud
  failure into a silent misread in the case that matters (§7).
- **Persistent formats.** etcd values, SSTs, `.meta` and the WAL evolve under
  the stop-the-world rules and the `cluster_version` gate.
