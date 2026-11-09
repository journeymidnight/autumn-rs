# rkyv, unified Hello, and the rolling upgrade plan

Status: RPC admission, removal of the old version mechanism, and the operations procedure are implemented.

## The three changes implemented here

1. **Explicit version check at the RPC layer**: every connection completes Hello before any business decode; internal/admin connections use a WIRE_VERSION equality check, clients use the existing [MIN_CLIENT_WIRE_VERSION, WIRE_VERSION] range check. A version mismatch explicitly returns both sides' versions and the rejection reason, distinct from transport errors, identity errors and business decode errors; we do not wait for a decode failure and then guess whether an upgrade is in progress.
2. **Remove cluster_version and the cluster bump mechanism**: remove the manager's etcd version latch, the startup/replay checks, the query and bump RPCs, and autumn-op's cluster-version and upgrade-version commands. cluster_version is no longer used to judge persistence compatibility or to restrict rollback; persistence changes are handled per release. Historical fields in frozen responses keep their existing encoding and remain only as reserved placeholders after retirement; old opcodes are not reused; cluster_id and ownership generation are kept.
3. **Update the general upgrade procedure in docs/ops.md**: record the original policy and switch it to Off, stop new management tasks, wait for already-dispatched compaction, EC, GC, split/merge, rebalance and similar tasks to finish, and check recovery and PS-local background tasks; then drain, replace, verify recovery, and finally restore the original policy. The first rollout of unified Hello uses the decided stopworld; afterwards, provided persistence is compatible and the existing fencing/recovery mechanisms guarantee that ACKed data is not lost, a rolling upgrade that allows RPC failures, timeouts, unknown results and brief unavailability is permitted, and this applies to wire changes as well.

The rest of this document gives the implementation and acceptance details of these three changes; rkyv, the existing client compatibility range, and per-change persistence analysis are retained.

## 1. Decisions and scope

1. RPC keeps rkyv; no migration to Protobuf, no new proto/protoc.
2. Every business RPC connection first completes a stable bootstrap Hello. Internal peers and admin require equal WIRE_VERSION; clients are admitted by the client_wire_version compatibility rules.
3. Connection admission and request admission both run at mandatory entry points. Passing the client compatibility window does not mean a connection may call internal or admin RPCs.
4. **The first rollout of unified Hello uses stopworld**: manager, PS, EN, SDK, autumn-op and other background callers are updated together, and old connections are closed. This migration is not required to be compatible with old connections that have no Hello; the existing WIRE_VERSION and MIN_CLIENT_WIRE_VERSION and the client range admission mechanism are kept.
5. **A WIRE_VERSION change can also be a rolling upgrade**: processes with different wires may coexist temporarily during instance-by-instance replacement, but cross-wire internal RPCs are always rejected at the Hello stage and no business is decoded or executed. Cross-wire interoperation is not supported and no dual codec is introduced; the mixed phase allows RPC failures, timeouts and temporary business unavailability, and eventually converges on a single wire. Whether persistence permits a rolling upgrade is decided separately for each specific change.
6. Persistent format changes are rare; when one happens, the affected readers/writers, format and recovery semantics are analyzed for that change, and the conversion and downtime scope are decided separately. This plan does not build a unified persistence read/write entry point and does not introduce a unified cluster_version/storage epoch gate.

This document adopts neither Protobuf nor a unified storage epoch. The first handshake mechanism migration runs as the decided stopworld; once the mechanism is in place, a wire change by itself does not require stopworld. Persistence changes are still handled case by case and may require stopping writes or stopworld when necessary.

## 2. Responsibilities of each version

| Item | Responsibility | Change rule |
|---|---|---|
| WIRE_VERSION | Hard boundary for layout, encoding and protocol semantics of internal and admin RPCs; also the current upper bound of the client protocol | Bump when layout or semantics change incompatibly; not bumped on every release |
| client_wire_version | Protocol version declared by a client connection | Judged by the client compatibility rules; cannot replace internal peer exact-match |
| MIN_CLIENT_WIRE_VERSION | Lower bound of the client protocol the current implementation truly supports | Keep the existing lower bound; adjust only when the client compatibility range is actually tightened, never raised automatically by a server release or an internal wire bump |
| bootstrap protocol identifier/version | Fixed handshake format ahead of the business version check | Frozen independently of business DTOs; unknown bootstrap versions are rejected without attempting business decode |
| Existing format versions (if any) | Basis for interpreting a specific persistent format | When that format changes, handled by that change's migration plan; no requirement to retrofit versions or envelopes uniformly |

rkyv does not offer Protobuf-style field-addition compatibility: even adding an Option field can change the archived layout, which has to be assessed and may require a wire bump. Ordinary optimizations, fixes and releases that do not change the protocol do not bump. When the client surface changes, the real old entry points/types must be kept, or the compatibility range must be explicitly tightened; compatibility cannot be claimed by an integer window alone.

Currently WIRE_VERSION = 54 and MIN_CLIENT_WIRE_VERSION = 43. Adding Hello does not automatically bump the wire or raise the client lower bound; when the business protocol really changes, the corresponding change rule applies.

Clients use the existing range check at connect: the server offers [MIN_CLIENT_WIRE_VERSION, WIRE_VERSION], and the client's declared client_wire_version must fall within it. The current SDK declares its own WIRE_VERSION. This expresses that the protocol the client uses intersects the set the server supports; internal peer admission is not changed to range overlap. Internal members use only WIRE_VERSION exact-match.

"No mixed wire internally" means no usable business RPC connection may be established between different wires; it does not require every process in a deployment to be on a single wire at all times. During an upgrade, manager, PS and EN processes with different wires may exist; they communicate only with peers on the same wire. Cross-wire registration or business requests are rejected, nodes whose dependencies are not met keep waiting, and everything eventually converges. Different builds/commits on the same wire are not rejected because of source differences, and the client compatibility range plays no part in internal peer exact-match.

Unified Hello is an additional condition for connection admission. The first stopworld updates the handshake implementation of old SDKs without using that to automatically tighten their supported business protocol range; the client entry points, types and behavior actually supported within the range are kept. The range declaration cannot replace the required Hello, nor can it on its own prove that a historical binary with a non-updated handshake can connect.

Existing frozen encodings such as MSG_CLIENT_HELLO and GetClusterIdResp are not directly extended with fields, given new meanings, or reused as new role encodings. The new bootstrap uses its own explicit identifier; the old Hello cannot turn an unchecked connection into a usable business connection. Keeping historical fields does not mean continuing to accept the old handshake.

Remove the reads and writes of the existing etcd cluster_version latch, the startup/replay checks, the query/bump RPCs, and the cluster-version and upgrade-version commands (cluster bump). Historical fields in frozen encodings such as GetClusterIdResp remain as placeholders and no longer carry valid version state; retired opcodes keep their numbers and are not reused. Leftover etcd keys are no longer read and do not affect behavior; cleaning them up can be scheduled separately. cluster_id/ownership generation must not be removed by mistake, and removing the old latch does not by default mean an old build can be safely rolled back to.

## 3. Connection paths covered

- The manager, PS and EN listeners all first complete `version_hello::accept`, then complete `peer_auth::accept` for Peer / Admin connections (cluster secret, see `cluster_secret_design.md`), and only then create the business `FrameDecoder`.
- `RpcClient::connect` / `from_conn` complete the two-step handshake (VERSION_HELLO, plus PEER_AUTH for Peer / Admin) before starting the business reader/writer. The default is peer; the SDK uses client, autumn-op uses admin, and EN identity checks and registration explicitly use peer.
- A `ConnPool`'s role is fixed at construction; a failed handshake never enters the pool. The SDK's direct-read pool to EN independently uses client.
- The manager, PS and EN run the role × opcode check before dispatch or before merging batched appends. The role is only a declaration; Peer / Admin is proven by PEER_AUTH. Capability (CLIENT_AUTH), cluster_id and ownership checks still run.
- PS acquiring the owner lock and registering, and EN verifying the manager's identity and registering, wait and retry on an explicit wire mismatch or a transient transport failure, without skipping the version check. Identity, authorization and storage errors still return errors.

## 4. Unified admission for connections and requests

### 4.1 The frozen bootstrap boundary

The unified version handshake is named VERSION_HELLO, with the constant MSG_VERSION_HELLO = 0xF0 (shared by manager, PS and EN). It uses the new magic AUPH, and bootstrap_version starts at 1; the frozen encoding of the existing MSG_CLIENT_HELLO = 0x5F is not modified. VERSION_HELLO is parsed with its own independently frozen fixed format and does not depend on rkyv or the business FrameDecoder.

CLIENT_AUTH = 0x55 is the existing SDK → PS authentication message and still uses the existing rkyv encoding: request ClientAuthReq { token: Vec<u8> }, response ClientAuthResp { code: u8, message: String }. A client connection first completes VERSION_HELLO, then runs CLIENT_AUTH according to the existing auth configuration (PS; with authz on, also EN direct-read connections). Peer / Admin connections run PEER_AUTH after VERSION_HELLO (MSG_PEER_AUTH = 0xF1, same frozen frame format as VERSION_HELLO) to prove they hold the cluster secret. The role in the version Hello is only a protocol declaration and grants no identity or permission.

The bootstrap uses a fixed binary format containing the protocol identifier/version, connection role, and declared wire/client versions. Requests, success responses and rejection responses must not depend on mutable rkyv DTOs. The response explicitly carries the target service type, the server wire, the client compatibility range and the failure reason, and the initiating side must check it too, rather than relying on a one-way decision by the listener.

The frozen scope covers the bootstrap's outer framing, field widths, byte order, length limits and error results, not just the Hello payload. The handshake must not depend on a FrameDecoder that may change with the business wire in order to read the version. The fixed format of the current implementation is as follows (all integers little-endian):

Outer: `req_id:u32=1 | opcode:u8=0xF0 | flags:u8 | payload_len:u32 | ctrl_len:u32 | ctrl | crc32c:u32`.
`flags` is 0 for requests and 1 for responses; `payload_len=ctrl_len+8`; the CRC covers header, ctrl_len and ctrl, with no value tail.
This framing is implemented separately and does not change with the business Frame/FrameDecoder. A request is 34 bytes in total; a response is 40–296 bytes in total.

| ctrl | Fields, in send order |
|---|---|
| Request, 16 bytes | `magic[4]=AUPH, bootstrap_version:u16=1, role:u8, reserved:u8=0, wire_version:u32, client_version:u32` |
| Response, 22+n bytes | `magic[4]=AUPH, bootstrap_version:u16=1, verdict:u8, service:u8, wire_version:u32, min_client:u32, max_client:u32, message_len:u16, message[n]` |

Roles: client=1, peer=2, admin=3; target services: manager=1, PS=2, EN=3.
Verdicts: success=0, wire mismatch=1, client mismatch=2, malformed=3, bootstrap mismatch=4.
For peer/admin, client_version must be 0; a client is judged against the range by client_version. The reason message is at most 256 bytes.
The handshake, and RpcClient's connect+handshake, are capped at 5 seconds; a shorter caller budget takes precedence. Fixed bytes of request/success response, role/version boundaries and abnormal framing are covered by tests.

The handshake uses its own length limit and timeout; the length is checked before reading or allocating the body. Unknown protocol, invalid role, malformed, no Hello, unobtainable version or timeout are all rejected and the connection closed; the version is never guessed from a decode failure, and there is no fallback to an unchecked mode.

cluster_id/auth/ownership checks still run independently; the role in Hello is a protocol declaration, not proof of identity or authorization. Only after the required version and identity checks complete does a connection become a checked connection usable for business.

### 4.2 Role and request admission matrix

| Connection role | Connection version rule | Allowed requests |
|---|---|---|
| client | MIN_CLIENT_WIRE_VERSION ≤ client_wire_version ≤ server WIRE_VERSION | Requests explicitly listed in the client compatibility surface; the matching codec/behavior is chosen by the declared version |
| peer (manager/PS/EN) | Local and remote WIRE_VERSION are equal | Internal requests allowed for that peer type, plus explicitly allowed shared requests |
| admin (autumn-op, etc.) | Local and remote WIRE_VERSION are equal | Requests explicitly listed in the admin surface, plus explicitly allowed shared requests; authorization still follows the existing rules |

A checked connection records the local and remote service types, role, version and applicable compatibility rule. A connection's role cannot change once established; a repeated Hello must not reset the admission state, and after a rejection the connection cannot try another role to get into business.

The unified dispatch entry checks role × opcode × declared version before decoding the business body. A client connection cannot call registration, internal mutations or admin requests; a peer connection does not automatically get admin rights. A shared opcode must explicitly register the roles allowed to use it and its layout/semantic compatibility range; shared requests cannot simply all be treated as internal requests that reject old clients.

EN registration and the startup identity check explicitly use peer mode; autumn-op explicitly uses admin mode. The ordinary SDK stays in client mode. When one library serves several callers, they are distinguished explicitly through construction entry points and types; the role is never inferred from the library or process name.

Requests, responses, direct node connections, pooled connections, batch, streaming, background tasks and admin paths all go through this entry. Raw business codecs and raw connection dispatch are confined to infrastructure modules; business send/receive accept only checked connections. A client's historical codec is chosen from the checked declared version; a business handler must not guess the version on its own.

### 4.3 SDK → EN direct-read

Direct-read is a separate client connection: the first connect must Hello; the EN admits it by client_wire_version and the compatibility rules for the corresponding client read requests. The check is not skipped because the request bypasses the PS, because it reads bytes, or because Hello was already done with the manager/PS.

Reconnects, pool replacement, and switching replica/EN address all redo the handshake. A pool's reuse key includes at least address, role and version/identity context; client, peer and admin connections to the same address are never mixed. With authz on, the EN serves direct reads only on connections whose CLIENT_AUTH bound a valid principal (identity only, no key-range check, see `data_plane_authz_design.md` §10); the extent eversion check still runs.

The first release does not substitute the old SDK's proxy fallback for updating the SDK, nor does it automatically raise MIN_CLIENT_WIRE_VERSION because of it. Clients within the supported window from now on must also support unified Hello; a new server must keep the direct-read requests it actually uses and the related token/response encodings, or explicitly tighten the range for a specific incompatible change.

### 4.4 Connection lifecycle

Connection states are: unchecked → handshaking → checked → closed; rejection, timeout and transport errors go to closed. A connection may be put into the usable pool or send business only after Hello succeeds; business frames cannot be queued during the handshake.

Every new connection, reconnect after disconnect, and pool replacement redoes Hello. Versions are compared once at connection establishment; normal messages do not check the version or etcd per message; framing/length/codec and request role admission are still checked per message.

Replacing a process normally closes the old sockets, and the new process must not inherit the old connections' checked state. A mismatched connection is not kept as usable; a buffer already encoded in the old layout must not be sent on a new connection. A retry's encoding and sending must go through the new connection's check and codec selection.

## 5. Mismatch, startup and request recovery

An internal wire mismatch means the current peer cannot communicate; the connection is closed and explicitly rejected. It can be an expected transient during a wire rolling upgrade; a node may stay in a started-but-dependencies-not-ready waiting state and reconnect with backoff until the peer is also upgraded to the same wire. Cross-wire registration and business must not be let through; retrying must not be treated as cross-wire compatibility.

The waiting state must be observable through local diagnostics or the management platform, not only through rejected business RPCs. Errors record both sides' versions, roles, client range and the reason; the startup wait log records node/target, wire, error chain and the next retry interval. Startup reconnect currently uses 1–5 second backoff; the deployment system owns the overall upgrade timeout, and jitter and dedicated wait metrics can be added later as scale requires. A node's long wait and the total upgrade timeout are kept separate; a permanent mismatch must be reported, never waited on silently forever.

Transport failures, temporary unreachability during a node restart, and version rejections are handled separately, and new connections always redo Hello. Identity and authorization failures and invalid protocol are classified separately and must not be masked by the wire-wait logic. The PS best-effort wire skip is removed; if the check cannot complete, the connection stays unchecked, rather than registering first and verifying later. Check the fixed retry limits in EN registration, startup checks and background workers so that an expected mismatch wait does not degrade into repeated exit-and-restart.

A ConnPool timed call covers connect, Hello, send and response with a single budget; SDK connection initialization is bounded by the first attempt's budget. A node's long-term reconnect is separate from a single call. The SDK's existing routing retry still uses a per-attempt budget and a finite count; no overall deadline per operation was added in this change, so it cannot be promised that a whole SDK operation finishes within a single RPC timeout. Future attempts that were cancelled must not be re-queued; a business request already sent may still execute.

No business request has executed before a Hello rejection; after a business request is sent, a disconnect may still mean it executed. For each write entry point, record the handling rules for "not sent, retryable", "sent and deduplicable" and "sent but result unknown". Existing idempotency/dedup capabilities are reused; append, management operations or timed-out requests must not be blindly replayed because of the version check; entry points without a dedup guarantee return result-unknown or follow the existing query/recovery procedure.

What may be lost is an unfinished RPC message or response, surfacing as an explicit failure, timeout or unknown result; an ACKed persistent write must not be lost. During the upgrade, node death detection, ownership transfer and background recovery must still obey the existing fencing and data protection rules. A Hello rejection proves only that cross-wire business was not decoded or executed; it cannot on its own prove the data safety of every control flow.

## 6. Deployment

### 6.1 First rollout of unified Hello: stopworld

This deployment covers the role admission migration and SDK → EN Hello, and uses the decided first stopworld; it is not to be equated with a wire rolling upgrade after the mechanism is in place. Before release, list every caller, including SDK/wheel, FUSE, S3 gateway, autumn-op, scheduled jobs, migration tools and other background processes, and assign responsibility for stopping, updating and restoring each.

The execution steps are written into docs/ops.md as an actionable checklist:

1. Confirm the new builds of servers, SDK and tools are all ready, and that the existing client compatibility range and the rollout recovery plan have passed acceptance; if persist changes are involved, also complete acceptance of that migration.
2. Stop new business and background management writes, drain accepted requests, record operations with undetermined results; stop all old callers and service instances, confirm the old sockets are closed, and prevent old processes from being restarted automatically.
3. If this release also involves persistent layout or semantic changes, follow the stop-write/fencing, backup and conversion order determined separately for this change under section 7. Stopworld by itself does not prove data compatibility.
4. Deploy all new servers, SDK and tools. Start them in the dependency order validated in rehearsal, and complete Hello, identity checks, registration and recovery with traffic closed.
5. Check leader, partitions, replicas, reads/writes and EN direct-read; confirm that no old caller with a non-updated handshake reconnects and that the existing client range matches the actually supported set; then open business and background tasks.

If the rollout fails, keep business closed and run the validated recovery plan. With persistence changes, binaries cannot simply be rolled back and conversion cannot be skipped; even if no incompatible data was written, whether a full rollback is possible must be confirmed from the compatibility matrix and the actual state.

Leftover processes of old SDKs/tools must be rejected and observable; a rejection must not be read as a reason to temporarily relax the check. The rules for keeping the existing frozen Hello/response encodings do not grant old processes business admission.

### 6.2 Choosing how to upgrade later

| Release change | Deployment |
|---|---|
| Same wire, no incompatible persistence change | Normal drain, instance-by-instance restart and Ready wait |
| Wire change, no persist change or that change's persist plan allows mixed running | Replace instance by instance or by dependency group; temporary mismatch, RPC failures and business unavailability are allowed; eventually converges on the target wire |
| Persistence does not allow old and new writers to coexist | Stop the relevant writers, fence and convert according to this change's format analysis; stopworld if necessary |
| Incompatible change to the frozen bootstrap boundary | Draw up a separate handshake mechanism migration plan and caller update scope; the ordinary wire rolling procedure cannot be applied directly |

A general upgrade first records the policy name and mode, runs auto-policy deactivate and confirms Off, stops new manual/external background management tasks, waits for already-dispatched compaction, EC, GC, split/merge, rebalance and similar tasks to finish and checks their results, and also checks recovery and PS-local background tasks. Policy Off does not mean all background tasks are paused. After confirmation, perform the replacement, and once recovery is verified restore the original policy name and mode; see docs/ops.md for details. Pausing the policy and waiting for tasks reduces interference during the upgrade; it does not replace persistence compatibility analysis, drain, ownership fencing or recovery guarantees.

Later wire changes do not automatically raise MIN_CLIENT_WIRE_VERSION. While the compatible implementation of the client surface is kept, clients within the range can keep working; admin tools are updated with the internal wire. Whether an internal protocol upgrade requires updating the SDK depends on that change's client surface and handshake changes; not every internal wire bump requires rebuilding clients.

Version rejection stops different layouts from decoding each other, but retry does not make two sides on different wires interoperate. A same-wire rolling restart must meet the necessary dependencies and the replica/EC service budget; a wire-change rolling upgrade explicitly accepts unavailability caused by temporarily insufficient dependencies and cannot promise zero interruption or that every request succeeds automatically. The duration of unavailability depends on the actual dependencies and deployment speed; no fixed upper bound is assumed.

### 6.3 Progress conditions and final recovery

A same-wire rolling upgrade follows drain → restart → Hello/registration/recovery → Ready → next instance. Before each replacement, check the remaining instances and the replica/EC service budget; stop advancing while a new instance is not Ready, set per-instance and overall timeouts, and handle failures per this change's recovery plan.

With a wire change, the deployer cannot wait unconditionally for business Ready: a new instance may have to connect to a manager/EN that has not been upgraded yet before it can become Ready, and a deployer that waits for it to be Ready before upgrading the next instance will get stuck. Distinguish "process started, local initialization done, waiting because of a known wire mismatch" from "overall business Ready", and either advance subsequent instances under conditions established in rehearsal or coordinate replacing a dependency group. Identity, storage or other initialization errors must not be ignored as an expected mismatch.

Drain, stop and replace instances one at a time, close old connections, and let new instances redo Hello. If a drain or shutdown step cannot complete normally because of a peer mismatch, use the timeout and recovery steps validated in that rehearsal; do not force ahead on the assumption that committed writes are necessarily safe. Set a total upgrade timeout, failure reporting and a recovery plan; prevent old processes from being restarted unexpectedly and delaying version convergence.

The concrete replacement order is validated against the real topology; it cannot be assumed that manager → PS → EN always works. The rehearsal observes node death detection triggered by heartbeat interruption, partition ownership migration, replica recovery and GC behavior, and settles whether the related control tasks must be paused during maintenance. The manager's election path through etcd is not protected by Hello, so it must still be verified that leadership, shared state and ownership fencing stay correct across the version switch; processes on different wires coexisting in a deployment is not treated as an error.

After all processes are updated, check leader, registration, ownership, partitions, replicas/EC and business recovery, and verify that ACKed writes are readable and nothing was wrongly deleted, before declaring the upgrade complete.

Each release states whether the wire and the client surface changed, and whether persistent layout/semantic changes are involved. A wire change by itself does not require stopworld; when persistence changes are involved, first complete that change's upgrade analysis, then decide the allowed replacement method and stop-write scope. Hello protects only RPC, not on-disk data or shared etcd state.

### 6.4 Narrowing the scope and duration of interruption

Under the current constraints, narrowing interruption depends first on deployment preparation and the actual dependency topology. Global wire exact-match provides no bridge between old and new services; a shared manager and ENs shared across partitions widen the blast radius, so the number of affected partitions cannot be inferred from the number of processes alone. Stream append currently waits for all replicas to succeed, so the upgrade cannot be planned on the assumption that "keeping a majority of replicas is enough to keep writing".

Before release, complete binary/image distribution and config and data compatibility checks, and record the partition → PS → EN replica dependencies of active streams, the read dependencies of sealed data, and the manager dependencies. State which partitions' reads, writes and control requests each batch will affect; where a node is shared by several partitions, take the actual affected set, and do not promise per-partition isolation.

With a wire change, after drain, ownership and startup conditions have been verified, switch closely related PS/EN in a coordinated way, using bounded concurrency to shorten the time dependencies are on mismatched versions. The whole release must not become a long serial process where each instance waits for full business Ready, nor can raising concurrency alone guarantee business correctness. During preparation, old and new processes must not write the same data directory or hold the same partition ownership at the same time.

Manager leadership switching is scheduled separately: finish preparation first, then change the leader in a coordinated switch window, so that a new manager is not elected early and cuts off large numbers of old-wire nodes. The existing etcd automatic election has no upgrade version threshold; to start a new manager standby early, a control that prevents early campaigning must be added and validated, and it cannot be assumed that it naturally stays standby after starting. The old leader also cannot provide cross-wire control service to upgraded nodes.

Determine a bounded maintenance scope before the upgrade, so that expected heartbeat interruptions do not trigger needless reassignment, recovery, rebalance or GC that prolongs business recovery. The existing EN maintenance and PS heartbeat death detection are checked separately; EN maintenance is not treated as upgrade protection for PS. Ownership fencing and handling of real failures outside the maintenance scope are kept, with an explicit recovery strategy after timeout.

Version rejection must return quickly and be kept separate from request timeouts. When it complies with the existing committed-prefix/EC rules, the read path picks an available replica as soon as possible instead of exhausting the full timeout on each rejecting endpoint; background connection backoff must not directly become a long foreground request wait. Once a business path recovers, its service can resume without artificially waiting for all unrelated nodes to update, but shared dependencies may block several partitions together.

Acceptance measures separately the number of unreadable partitions, the number of unwritable partitions, the longest continuous interruption, the time to first partition recovery, and the time to full recovery. If a wire change must also be strictly limited to affecting a single partition, service isolation of the shared manager and EN across versions has to be solved separately; the current deployment optimizations alone cannot provide that guarantee.

## 7. Persistence changes are handled case by case

The project's persistent formats change rarely, and each upgrade involving such a change is handled on its own. This plan only lists what must be stated when a change happens; it does not build a unified read/write entry point, a generic versioned codec, an all-format envelope, or an anti-bypass framework for persistence. A global cluster_version number is not used to prove data compatibility.

### 7.1 Triggers and scope of analysis

When a release has no persistent layout or semantic change, record "no persist change" explicitly; the Hello work does not require refactoring existing storage paths, retrofitting format versions or scheduling conversions.

When persist changes are involved, analyze only the affected formats and related paths. Based on what the change needs, decide whether to use an existing format version, add a local marker, keep a historical reader or run a one-off conversion; no unified mechanism is presupposed. A format number by itself does not prove semantic compatibility.

The scope of analysis can include the affected manager etcd records, SST/entry, WAL/value pointer, checkpoint/TableLocations, EN metadata, FS metadata or control records. Related formats are analyzed together, and the actual readers and writers such as recovery, GC, compaction, repair and FUSE/SDK are checked; this does not require the release to complete a project-wide persistence catalogue overhaul.

Opaque application values are not part of Autumn's schema; eversion/ownership generation is not a format version.

### 7.2 Per-change statement

1. State the specific layout or semantics changed, the affected readers/writers, and the actual read/write compatibility of old and new builds with the related data.
2. Decide whether old and new processes can run together for this change; if not, state which writers must stop, the drain/fencing scope and whether stopworld is needed; do not rely on Hello retry.
3. If conversion is needed, give this change's converter, execution order, service start/stop conditions and required resources; if not, state why.
4. Verify historical data reads and recovery, the conversion result and the consistency of related references for the affected formats. When offset/VP/checkpoint/ownership are involved, verify the corresponding relationships; where old and new formats are distinguished, a failed check must not be mistaken for a bad tail or missing data and then overwrite state.
5. State how to resume or recover after a failure, and whether direct binary rollback is allowed; when a backup or reverse conversion is needed, write out the concrete steps.
6. Write the executable upgrade and acceptance steps for that change into docs/ops.md or that change's migration document.

## 8. Preventing omissions, and performance

- Module visibility of the unified connection type, RPC dispatch and codecs, plus CI architecture checks, prevent bypassing Hello; audit direct uses of transport, rpc_oneshot, bulk, admin and cross-shard paths.
- Fixed-byte tests cover bootstrap request/success/rejection/outer framing. A change to the bootstrap format must go through a separate migration decision, not just an update to the tests' expected values. Persistence tests are arranged with each specific change, verifying the affected historical data, conversion and recovery; no unified layout testing framework is required.
- Wire bumps for RPC business layout changes are enforced jointly by sample tests and review; the whole-file source fingerprint is not restored. Semantic changes with unchanged encoding cannot all be detected automatically; review checks the upgrade analysis and acceptance evidence.
- The RPC version comparison is folded into Hello; no per-request version lookup or global lock is introduced. Persistence checks keep their existing paths and are adjusted along with specific format changes when needed.
- The rkyv performance path is kept. Verify that a normal connection handshakes only once, pool reuse adds no handshake RTT, and client → EN direct-read bandwidth/latency and PS load show no unexpected regression.

## 9. Implementation phases and exit criteria

Phases advance by dependency. The first stopworld deployment must wait for the first four phases to complete; later same-wire and wire-change rolling upgrades are accepted separately. A wire change adds no global downtime requirement; persistence restrictions are handled per specific upgrade.

| Phase | Deliverables | Exit criteria |
|---|---|---|
| 1. Path and protocol audit | Connection path table, opcode admission matrix, exact bootstrap byte specification, existing client compatibility range | Every production path classified; construction entry points for peer/admin/client and the frozen scope are explicit |
| 2. Hello and request admission | Checked connection type, listener state machine, changes to every connection pool and to the SDK/tools | No Hello, malformed, wire mismatch and wrong role are all rejected before business decode; covers direct-read and reconnect |
| 3. Startup and recovery | Waiting/Ready states, error classification, mismatch backoff reconnect, absolute deadline, write retry rules | Mismatched business rejected and the wait observable; node waiting not unexpectedly terminated by a finite retry; timed-out/cancelled requests not replayed |
| 4. First release preparation | Audit and retirement of old gate dependencies, ops stopworld checklist, build list for all callers; statement of whether persist changes are involved | If persist changes are involved, that migration and recovery pass acceptance; the actually supported set matches the existing client range; first rollout and failure recovery rehearsals pass |
| 5. First deployment | Run stopworld per section 6.1 | Traffic opens only after all servers/callers are updated, old connections are closed, and registration and business have recovered |
| 6. Later upgrade procedure | Real rolling rehearsals of same wire with different builds and of different wires; cross-wire RPC rejection tests | Same wire advances after Ready; a wire change can advance in the expected waiting state; manager failover, upgrade timeout, final business recovery and ACKed data verification pass |

The first release does not schedule retrofitting all-format envelopes or building a unified persistence entry point. The actual dependencies of the old gate are closed out in phase 4; if this release really changes a persist format, section 7's analysis and handling are completed separately. Every later persist change draws up its own upgrade steps and cannot reuse the conclusions of the first stopworld.

### 9.1 Required acceptance scenarios

1. Hello success/rejection fixed bytes, unknown bootstrap version, invalid length, timeout, no Hello, wrong service type and repeated Hello.
2. In both directions of an internal/admin wire mismatch, no business body is rkyv-decoded; a client using internal/admin opcodes is rejected before body decode.
3. EN registration uses peer and autumn-op uses admin; every ConnPool, rpc_oneshot, direct node connection, cross-shard, bulk, streaming and background path completes the handshake.
4. SDK → EN direct-read succeeds at supported client versions; Hello is redone after EN restart, replica switch and pool replacement; connections to the same address with different roles are not mixed.
5. The first stopworld uses real SDKs/tools with non-updated handshakes to verify that leftover callers are rejected, and the updated SDK to verify direct-read; real clients supporting unified Hello verify that the lower bound, interior and upper bound of the existing range work and that anything outside the range is rejected, without proving compatibility by faking version integers.
6. Cross-wire registration/business RPCs are rejected and cannot gain admission through retry or best-effort skip; a node can stay in an observable waiting state, and the deployer can advance subsequent instances under the agreed conditions; restart and reconnect redo Hello; a single request finishes within its own deadline and is not retried after cancellation.
7. Writes that executed but whose responses were lost are handled per each entry point's idempotency/dedup rules; operations without dedup are not blindly replayed; after recovery, ACKed data and ownership are verified.
8. The first Hello stopworld, later different-wire rolling upgrades, and same-wire rolling upgrades are rehearsed separately. The different-wire rehearsal uses two real builds that both support unified Hello, covering both switch directions, manager failover, advancing while not Ready, control tasks after heartbeat interruption, and final recovery; it reports request failures, timeouts, unknown results and the scope of business interruption, and verifies that no ACKed persistent write is lost.

If this release involves persist changes, the compatibility, conversion, recovery and rollback of the affected data are accepted separately per that change's statement; these acceptances are not extra retrofit tasks for a Hello release with no persist change.

RPC explicit admission and the removal of cluster_version/cluster bump are implemented. The old one-off SST/FS conversion tools and the old persistence-switch comments have been deleted; the existing SST/FS format rejection checks remain. Cleanup of leftover etcd keys and conversion of historical data are out of scope for this implementation. The release rehearsals above and acceptance against real historical clients are completed by the specific release and cannot be replaced by version integers declared in handshake tests.
