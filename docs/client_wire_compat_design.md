# Client Wire Compatibility

An upgrade stops and restarts the manager, partition servers and extent nodes
together. It does not stop the processes that hold an embedded client — an
inference pod, a mounted fuse daemon, an s3 gateway, a python wheel inside
somebody else's image. Those reconnect to the upgraded cluster on their own
schedule, so the cluster serves a client older than itself.

---

## 1. One version integer, two intervals

`crates/rpc/src/lib.rs` carries three constants over ONE numbering space:

| Constant | Meaning |
|---|---|
| `WIRE_VERSION_MAX` | the schema this binary speaks |
| `WIRE_VERSION_MIN` | the oldest CLUSTER peer it interoperates with — equal to `MAX` |
| `CLIENT_WIRE_MIN` | the oldest CLIENT it serves |

`WIRE_VERSION_MIN == WIRE_VERSION_MAX` keeps the cluster-internal protocol
stop-the-world: manager, PS and EN binaries swap in one window, so they never
face a peer of another version.

`CLIENT_WIRE_MIN` is the floor of the client window
`[CLIENT_WIRE_MIN, WIRE_VERSION_MAX]`. It moves ONLY when a change breaks the
client-facing surface, which makes it the one constant a reviewer has to look at
to answer "does this force every embedded image to be rebuilt". Raising
`WIRE_VERSION_MAX` alone — the common case — leaves every client inside the
window untouched.

The etcd `cluster_version` key is unrelated: it is the operator-bumped feature
gate for persisted formats, and its code compares against `WIRE_VERSION_MAX`
only. `CLIENT_WIRE_MIN` is a property of a binary, not of a cluster, because
stop-the-world already makes every server agree.

### 1.1 The two checks are different questions

`GetClusterIdResp` is frozen (§4) and has one `wire_version_min` slot, so that
slot carries **`CLIENT_WIRE_MIN`** — the floor is what a client needs, and a
client already deployed today reads this field with code that cannot be changed.

That makes the existing `wire_compat_check` interval-overlap test wrong for
cluster peers, and the error is silent in the dangerous direction. It computes
`lo = max(LOCAL_MIN, remote_min)`, `hi = min(LOCAL_MAX, remote_max)`, and
accepts when `lo <= hi`. A stale PS at 44 meeting a cluster reporting `[44, 45]`
overlaps and is ADMITTED — the handshake was the only thing enforcing
stop-the-world, and reporting a floor dissolves it.

**INVARIANT: a cluster peer requires exact equality, a client requires
membership.**

- PS and EN, checking the manager (`crates/partition-server/src/lib.rs`,
  `crates/server/src/bin/extent_node.rs`): admit iff
  `resp.wire_version_max == WIRE_VERSION_MAX`.
- A client, checking the cluster: admit iff
  `resp.wire_version_min <= own MAX <= resp.wire_version_max`.

Overlap is the wrong shape for both and must not survive as a shared helper;
its doc comment and the tests that encode the old relation
(`crates/rpc/src/lib.rs`) change with it.

Without this the window cannot open at all. A cluster at `MAX = 45` with
`CLIENT_WIRE_MIN = 44`, answering `[45, 45]` to an in-window client at 44, makes
that client compute `lo = 45 > hi = 44` and **refuse itself** at connect.

## 2. The client-facing surface

The window covers what an embedded client encodes or decodes:

- `partition_rpc`'s data plane — Put / Get / Delete / Head / Range,
  `MSG_PUT_BEGIN` and the stream ops, the three batch families and their bulk
  forms, `MSG_GET_REDIRECT(_MANY)`, `AUTH_HELLO`;
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

## 3. The version belongs to the connection

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
  `ClusterClient::connect`. What is missing is that the client discards the
  number instead of keeping it.
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
answered with `[code: u8][server_wire_max: u32][client_wire_min: u32]`. The
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
field in a commit that left `WIRE_VERSION_MAX` unchanged on both sides, which no
per-bump review can see. **The test's failure message prescribes the action —
"this is a client-facing break: raise `CLIENT_WIRE_MIN`" — and never "update the
recorded bytes".** The deleted schema fingerprint failed exactly there: each
false alarm taught the reflex of refreshing the recorded value, which is how a
real change gets waved through. A golden vector's one false-alarm mode is an
rkyv upgrade that changes the archived format, and that IS a client-wire break,
so the prescribed action is still right.

Neither this test nor anything else catches a change that leaves the bytes alone
and moves what a field MEANS. That stays a review obligation (§7).

## 5. Admission is scoped to the client surface, not to the connection

A connection that sends no hello is treated as the version in which the hello
was introduced. Once `CLIENT_WIRE_MIN` rises above that version, such a
connection is refused — **but only for the client-surface msg_types of §2.**

The scoping is not a refinement; without it the first floor move is a cluster
outage. The manager and PS listeners that serve clients also serve internal
peers, and those peers are silent by construction: PS→manager and EN→manager go
through `autumn_stream::ConnPool` straight to `RpcClient::connect` with no
handshake of any kind, and manager→PS drives `MSG_SPLIT_PART`,
`MSG_MAINTENANCE`, `MSG_MERGE_FREEZE` and `MSG_ROLL_TAILS` the same way. Nothing
in a frame says which role sent it. A connection-scoped refusal would therefore
reject `register_ps`, heartbeats, `register_node`, reconcile, split, merge-freeze
and roll-tails the moment the floor moved. Scoping by msg_type also leaves the
89 test files that open a raw `RpcClient` to a PS or manager working unchanged.

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
around the fetch), and today no server validates an incoming client at all —
`AUTH_HELLO` carries no version and the extent node has no version code.

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

## 7. Gating a behavior

A call site that must serve two forms branches on the connection's version and
keeps the old form until the floor passes it. Each such branch names the version
that deletes it, so the window's width is a count of behaviors somebody is
maintaining rather than a number chosen in the abstract.

This is the mechanism the encoding cannot supply. The changes that break a
client and are not fixable by any framing — a key layout whose meaning changed,
a descriptor field that re-interprets its neighbour, a per-item status that used
to fail a whole batch — are behavioral, and a branch on the negotiated version is
the only thing that serves both. A tagged encoding makes the additive subset
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

## 9. What stays outside

- **Rolling upgrade of the cluster.** The manager/PS/EN protocol keeps
  `MIN == MAX`, now enforced by exact-`MAX` equality (§1.1) rather than by
  interval overlap.
- **The encoding.** rkyv stays. A tagged encoding over the client-facing subset
  buys one clean case out of nine measured field additions, and converts a loud
  failure into a silent misread in the case that matters (§7).
- **Persistent formats.** etcd values, SSTs, `.meta` and the WAL evolve under
  the stop-the-world rules and the `cluster_version` gate.
