# Cluster membership authentication (cluster secret)

The whole cluster shares one secret. The manager, PS and EN must be given it at startup;
a connection that declares itself a cluster member (Peer) or an operator tool (Admin)
must prove at connect time that it holds it. Clients do not hold it: a client's identity
is proven by the data-plane capability token, see `data_plane_authz_design.md`.

## 1. Threat model

- **Defends against**: any process that can reach a cluster port but does not hold the
  secret, impersonating a cluster member or an operator tool — sending `APPEND` /
  `DELETE_EXTENT` / `FENCE_EXTENT` to an EN, sending `TRUNCATE` / `ACQUIRE_OWNER_LOCK` /
  reading `GET_AUTHZ_CONFIG` on the manager, or running operator actions
  (fence / remove / merge / namespace / principal / bootstrap / split / gc …).
- **Does not defend against**: an attacker who can read or rewrite the traffic itself (no
  encryption, no channel binding, no TLS); a compromised cluster member that therefore
  holds the secret (PS / EN and operator tools hold the same secret, so compromising any
  of them is equivalent to operator privileges).

## 2. What each of the three layers covers

| Layer | Answers | Where |
|---|---|---|
| VERSION_HELLO (`version_hello.rs`) | Whether the two sides' versions can interoperate; which interface the connection speaks (Client / Peer / Admin) | The first frame of every connection |
| PEER_AUTH (`peer_auth.rs`) | Whether a connection declaring Peer / Admin holds the cluster secret | Right after VERSION_HELLO, Peer / Admin only |
| CLIENT_AUTH (capability token) | Which principal the client is, which keys it may read and write | Business frames on client connections (PS; also on EN when authz is enabled) |

The identity declared in VERSION_HELLO only determines the version rules and the
interface scope; it is not a credential by itself. PEER_AUTH makes the Peer / Admin
declarations require proof.

## 3. Handshake

After VERSION_HELLO succeeds, both sides are still on the raw byte stream (the business
reader has not started yet) and exchange, in the same delimited frame format as
VERSION_HELLO (opcode `0xF1`, magic `AUPA`):

```text
server → challenge  "AUPA" | mode u8 (0 open, 1 required) | server_nonce[32]
client → proof      "AUPA" | client_nonce[32] | client_mac[32]
server → result     "AUPA" | verdict u8 (0 ok, 1 refused) | server_mac[32]
mac = HMAC-SHA256(secret, "autumn-rs peer-auth v1" | side | service | role | server_nonce | client_nonce)
```

- **Mutual**: the server verifies the client's mac, and the client also verifies the
  server's mac. A listener impersonating a member's address cannot get past the client.
- **The secret is never sent**: only nonces and macs are on the wire. Every connection
  gets a fresh server_nonce, so a recorded proof cannot be replayed onto another
  connection.
- **Done once, at connect time**: pooled connections are reused for a long time, and
  there is no added cost per request on the data path.
- **Client connections skip it**: both sides know the identity from the VERSION_HELLO
  result, and a Client connection exchanges no bytes.
- **open mode**: a server with no secret installed replies `mode = 0`, and the exchange
  ends there. Only in-process tests do this (the server binary refuses to start without
  a secret). A client that holds a secret refuses an open server, because that is exactly
  how an impersonator would answer.
- **Refusals leave a trace**: when the server refuses, it logs a WARN with the peer
  address, the declared identity and the service:
  `PEER_AUTH refused a connection holding a different cluster secret`, and
  `PEER_AUTH: connection gave no cluster-secret proof` (the peer has no secret; the
  connection is dropped).

## 4. After the initiator is refused

A refusal is a definite conclusion: the two sides hold different secrets. The secret is
read only once at startup, rotation requires a full cluster shutdown, and retrying will
not change the outcome. What to do depends on who refused whom
(`peer_auth::on_dial_failure`, applied in `RpcClient::from_conn_as` to every Peer / Admin
connection). "Who" is the address this process dialed, not the service the peer claims
to be in VERSION_HELLO: otherwise an outsider occupying some EN's address could claim to
be the manager and make every process that connects to it exit.

- **Refused by this process's manager** (the dialed address is in `--manager`, registered
  by PS / EN at startup with `designate_managers`): the manager is the authority on
  membership, and being refused by it means this process is not a member of this
  cluster. A server process (secret installed with `install_for_server`) logs an ERROR
  and exits with status 1.
- **Refused by any other address** (an EN, a PS, or an outsider occupying a member's
  address): the other side is wrong. Log an ERROR and return the error as is; the caller
  treats it as an unreachable node and the process keeps running. If it is actually this
  process that is misconfigured, it is refused the next time it contacts the manager and
  exits per the previous item. The manager does not register manager addresses itself,
  so it never exits on a refusal.
- In-process tests and autumn-op do not exit; they just get the error.
- Only a refusal (`PermissionDenied`) takes this path; a timeout or a dropped connection
  is not a refusal and is returned as usual.

## 5. The gate for operator actions

The opcodes that only Admin connections may send to the manager are the table in
`manager_rpc::is_admin_mgr_msg` (fence / remove / maintenance / EC / create-stream /
upsert-partition / merge / op-submit / principal / namespace / set-presplit / GET and SET
of auto-policy); a Peer connection sending these is refused by `check_opcode`. An Admin
connection must pass PEER_AUTH first, so this table is the entire gate, and request
bodies no longer carry any token. Split / maintenance on the PS are not part of the
client interface; only Peer / Admin connections can send them, likewise gated by
PEER_AUTH.

The split / flush / gc the manager drives itself (auto-policy, the flush before a merge)
are sent by the manager connecting to the PS as a Peer, through the same authentication
path as the operator tools.

## 6. Secret distribution

- **manager / PS / EN**: `--cluster-secret-file <PATH>`, required; missing it exits
  (exit 2). The file content, with leading and trailing whitespace stripped, must be at
  least 32 bytes. `autumn-op gen-cluster-secret` prints 64 hex characters. A file is used
  rather than a command-line argument so the secret never appears in `ps` /
  `/proc/<pid>/cmdline`.
- **autumn-op**: global `--cluster-secret-file` (before or after the subcommand). It
  connects as Admin, so read-only commands need it too; commands that do not connect to
  the cluster (`gen-signing-key`, `gen-cluster-secret`) do not.
- **autumn-dashboard**: requires `--cluster-secret-file` and passes the path on to every
  autumn-op invocation.
- **Clients (SDK / fuse / kvcache / S3 gateway / autumn-client)**: do not hold it.
- The rs code does not read environment variables: the env → flag translation is done in
  `scripts/cluster.sh` / the deployment layer (`scripts/cluster.sh` keeps the secret in
  `$DATA_ROOT/cluster.secret`, generated on first start).

The secret is process-global (`peer_auth::install`, the same shape as transport
selection): a process belongs to exactly one cluster, and all Peer / Admin connections
prove the same secret.

## 7. Rotation

A process recognizes only one secret, so rotation requires a full cluster shutdown:
generate a new file, distribute it to every node and operator machine, and restart
everything. There is no dual-secret transition period.

## 8. Performance

Each Peer / Admin connection costs 1 extra RTT at setup (the challenge is sent back to
back with the VERSION_HELLO response, and proof / result take one more round trip) and
two HMAC-SHA256 computations. Zero on the data path.
