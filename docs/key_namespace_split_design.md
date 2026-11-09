# Application Key Namespaces + Split Rules

This document rests on two rules:

1. **Every write must belong to a registered namespace**, and it must belong
   **explicitly** — forgetting to specify one is an error (fail-loud); there is
   no default fallback.
2. **Per-app split rules live in the policy / deployment layer and do not leak
   into the partition layer** — PS / manager split, merge and routing look only
   at byte ranges and never know which prefix belongs to which application.

---

## 1. Key layout

### 1.1 One keyspace, organized by namespace

The deployment assumes a **trusted internal network** (for example the RoCE
network an sglang/vLLM inference cluster runs on). There are no mutually
untrusting tenants; it is **one shared resource**, and different components
simply have different access rights. So the keyspace has no ownership segment;
the first segment is the namespace:

```
wire key = {ns}/[relative]
```

`{ns}` is a single path segment (`[a-z0-9._-]+`); Layer-A looks at the key's
**first segment**. "Ownership" is carried by the namespace itself, and
"permission" is carried by the principal's grants (§4).

| namespace | Relative key shape | Location |
|---|---|---|
| `fs/` | Raw typed bytes: `[0x01][ino BE]` (inode meta), `[0x02][parent BE][name]` (dirent), `[0x03][lane][ino BE][off BE]` (stripe data), `[0x04][field]` (superblock, including `stripe_geom` and the `rmtomb/` unlink tombstones) | `crates/fuse/src/key.rs` |
| `kvc/` | `{h}/{model}[_{fingerprint}][_{tp}][_pp{pp}]/…/{sha256-hex}/{layer}`, `h` = first character of the hash | `autumn_kvcache/_keys.py` (the autumn_kvcache repo) |
| Others (`mem/`, `bench/`, `gallery/`, user-created) | Defined by the application | `autumn-client` |

There are no built-in namespaces: after bootstrap the registry is empty, and
every application's namespace is created by the operator with
`autumn-op namespace-create` (`docs/ops.md`, "Enabling fs / kvc"); `fs` and
`kvc` are no exception. `cluster.sh` creates `fs` and `kvc` by default on first
start.

### 1.2 Path-segment vs binary namespaces

The granularity of a grant depends on the namespace's relative key shape, and
implementation and operations **must treat the two differently**:

| | Path-segment ns | Binary ns |
|---|---|---|
| Example | `mem/{app}/…`, `gallery/…` | fuse `fs/[0x01][ino]…` |
| Start of relative key | ASCII segment (`agent7/…`) | Fixed-length binary record |
| Sub-prefix grant inside the ns | **Meaningful** (`mem/app7/` really carves out a sub-space) | **Meaningless** (the `/models/` in `fs/models/` is a dirent name inside a `[0x02]` record, not a key prefix → it matches zero keys, and `validate_credential_scope` rejects it at connect time anyway) |
| Means of isolation | Grant different sub-prefixes | **Only the whole ns can be granted (`fs/`); for multiple mutually isolated trees → create multiple namespaces** (`fsA`/`fsB`, each registered separately) |

**The trade-off for binary namespaces (stated explicitly)**: `fs/` is one global
tree and the only unit of authorization. Any writer holding an `fs/` grant can
forge `fs/[0x04]rmtomb/<any ino>` (the sweep at mount time **unconditionally
deletes the data** of the inode a tombstone points to). **This is accepted under
the trusted-network assumption**: multiple writers in one tree are expected to
trust each other; for hard isolation, create multiple namespaces.

### 1.3 Client-side binding (`NamespaceBinding`)

`crates/client/src/lib.rs`. Every `ClusterClient` instance carries a binding,
so an "unowned write" cannot be expressed at the type level:

```rust
pub enum NamespaceBinding {
    Scoped { prefix: Vec<u8> },  // always ends with `/`: `fs/`, `mem/agent7/`
    Raw,                         // admin / cross-ns tools
}

ClusterClient::connect(mgr, "fs")                    // scope is required
ClusterClient::connect_with_credential(mgr, "mem/agent7", cred)
ClusterClient::connect_raw(mgr)                      // explicit Raw
client.rescope("kvc") / client.raw()                 // switch scope view, shared connection pool
```

- **Scoped always prepends**: `bind_key` = `prefix ++ user_key`; the scope is
  **locked in at construction** rather than "checked after the fact" — a scoped
  client **cannot reach** anything outside its own keyspace. The built-in key
  builders (fuse / kvcache) emit **relative keys** (without the ns segment); the
  prefix belongs to the binding.
- **Three-way range clamping**: `bind_prefix` prepends the prefix, the cursor
  seed is never below the lower bound, and `upper_cap()` supplies the upper
  bound = the prefix's trailing `/` (0x2f) replaced by its successor `0` (0x30),
  so a limit-driven scan can never walk out of its ns; on return `strip()`
  removes the prefix, so the caller still sees its own relative keys.
- **`Raw` bypasses only the client-side clamping, not the PS-side checks** —
  Layer-A / Layer-B are enforced on the server as usual. The escape hatch only
  solves "a tool needs to operate on multiple namespaces", not "bypass the
  protection".
- **Scope segment validation**: `is_valid_scope_segment` enforces
  `[a-z0-9._-]+` and non-empty at connect time — otherwise a segment containing
  `/` or an empty one could forge a nested / aliased scope
  (`acme/sub` + `mem` → `acme/sub/mem/`, or `//mem`).

**Which segment belongs to the binding and which to the builder is declared once
per app** (in that app's CLAUDE.md). Counter-example: with grant = `mem/app7/`
and an app builder that also emits `app7/…` itself, the result is a
double-segment key `mem/app7/app7/…`.

### 1.4 Chunk keys for striped large values

`make_chunk_key(user_key, upload_id, idx)` = `CHUNK_KEY_PREFIX ++ len(user_key) ++
user_key ++ upload_id ++ idx` (`upload_id` is 16 random bytes per upload, so
overwrite and abort only touch this upload's chunks). It produces a
**relative key**, which the binding then prefixes →
`{ns}/ ++ \xff\xfe… ++ user_key ++ upload_id ++ idx`. This way the stripe body
lands inside the ns's range: Layer-A admits it, Layer-B covers it, and the
presplit ranges include it. The cost is that a range scan within the ns sees its
own chunk keys (`\xff\xfe` sorts at the tail of the ns), which the caller has to
filter out.

---

## 2. Namespace registry

### 2.1 Data model

etcd `namespace/<name>` → `persist::NamespaceRecord` (format version 2; its wire
twin is `MgrNamespace` in `crates/rpc/src/manager_rpc.rs`, with the same fields):

| Field | Meaning |
|---|---|
| `name` | Single path segment, `[a-z0-9._-]+` |
| `prefix` | `name + "/"` — the byte prefix matched by Layer-A / authz / presplit |
| `presplit: Vec<Vec<u8>>` | Split points the operator has declared (the sacred boundaries of §6.3) |
| `created_at` | For diagnostics |

The manager keeps an in-memory shadow `namespaces: HashMap<String, MgrNamespace>`,
replayed from `get_prefix("namespace/")` when a leader takes office; **a decode
failure is fail-loud**.

### 2.2 Lifecycle

All leader-only, accepted only on Admin connections (`cluster_secret_design.md`
§4), etcd-first, with the whole critical section (existence + disjointness check
→ etcd write → in-memory apply) serialized by `namespace_admin_lock`, so two
concurrent creates cannot both pass validation and then commit in a conflicting
order.

```
autumn-op namespace-create --name <NS> [--parts <N>]  # after registering, split into N partitions (§6.2)
autumn-op namespace-delete --name <NS> [--force]
autumn-op namespace-list [--json]        # read-only
```

- **Prefix disjointness rule**: creation of a new name `X` is rejected when
  `X/` and any existing namespace prefix are `starts_with` of one another, which
  keeps all namespace ranges pairwise disjoint — only then is prefix matching in
  Layer-A / authz / presplit unambiguous.
- **Delete**: the non-empty check is done **client-side** by `autumn-op` (the
  manager has no KV data-plane client); `--force` skips it. The handler only
  removes the etcd registry row.

### 2.3 Propagation window

Every `--config-poll-secs` (30 s) the PS fetches the full list of namespace
prefixes via `MSG_GET_AUTHZ_CONFIG` (`GetAuthzConfigResp.namespaces`). When the
first write to a new ns after `namespace-create` meets a stale list, the PS
immediately re-fetches and returns `Unavailable`; by the time the SDK retries,
the write is admitted. Only a ns that is still absent from the re-fetched list
gets the terminal `NamespaceUnknown`.

---

## 3. Two layers of checks

### 3.1 Layer-A — existence (no token required)

`partition_server::authz::check_layer_a`. The keys of **put-class** requests
(`MSG_PUT` / `MSG_BATCH_PUT` / `MSG_COMPARE_WRITE`) **must** fall under the
prefix of some registered namespace; otherwise they are rejected with
`StatusCode::NamespaceUnknown` (=8).

- **Pure byte `starts_with` against the registered list, no token, and anonymous
  connections are checked too**.
- **Always on, independent of the signing key**: an empty registry rejects every
  client write. A cold PS that has not yet polled its config treats the first
  miss as a possibly stale config: it re-fetches and returns `Unavailable`, and
  the SDK retry is judged against the new config.
- **Applies to Client connections only**: namespaces are the application
  contract; Admin (`autumn-op`) and Peer (intra-cluster) connections have proven
  the cluster secret and are not checked.
- **Applies to writes only, not deletes/reads**: a delete cannot create "unowned
  data" (the purpose of Layer-A is to prevent pollution), and isolation of reads
  / ranges belongs to Layer-B. This narrowing also lets cleanup tools batch-delete
  legacy bare keys directly, without opening a hole in Layer-A.
- `NamespaceUnknown` is a **terminal** error on the client side (refreshing
  routes cannot create a namespace), in the same class as `PermissionDenied`;
  it is not retried.

### 3.2 Layer-B — authorization (capability token required)

See `data_plane_authz_design.md` for the design. In short: the manager acts as a
KDC and signs short-TTL tokens with an Ed25519 private key; the PS holds the
public key, verifies signatures locally and gates on the token's
`allowed_prefixes`. **Once authz is enabled it is protect-everything**: every
key and every range needs a token.

### 3.3 Why two layers

Merging them into "a namespace always comes with a token" would kill the dev /
test UX (every cluster.sh test cluster would first have to generate keys and
issue credentials), whereas the existence check holds without any key
infrastructure. **The namespace is an identity concept; authz is an enforcement
switch**: a dev cluster can have namespaces without tokens; a prod cluster runs
both layers.

---

## 4. Principals and grants

- A **principal** is the only identity:
  `autumn-op principal-create <name> --grant <prefix> [--grant …]` →
  the manager stores `{name, credential_hash, allowed_prefixes}` and returns a
  long-lived credential. `autumn-op principal-list` lists them read-only
  (credential hashes never leave the manager).
- A grant is usually one ns (`fs/`); a path-segment ns can be subdivided further
  into sub-prefixes (`mem/agent7/`), a binary ns cannot (§1.2).
- **Least privilege is the default; no all-ns master key is created**: a master
  key would make Layer-B always true, and one buggy kvcache process could delete
  fs data. A turnkey deployment creates **one key per family**:

```bash
autumn-op principal-create --principal fs  --grant fs/       > fs.cred   # fuse / autumnfs
autumn-op principal-create --principal kvc --grant kvc/      > kvc.cred  # kvcache loader
autumn-op principal-create --principal app --grant mem/app/  > app.cred
```

- **The data plane presents only the credential**; there is no `--principal`
  flag: the principal name travels with the credential file (two-line format
  `<name>\n<hex>`), and the scope comes from `--namespace <ns>`
  (`autumn-client`'s `--namespace` / `--scope`) or from the app's own ns.
- **Layer-A is orthogonal to grants**: however broad a grant is, a write must
  still land under a **registered ns**. "Full permission" does not mean you can
  write anywhere under unregistered prefixes.

---

## 5. Three measures of partition size

The same partition carries three mutually contradictory numbers at once. **All
three are true; they just do not measure the same thing**:

| Measure | Definition | Code location |
|---|---|---|
| ① `PartitionLoad.size_bytes` | **LSM-resident bytes** = Σ SST `len` + active/imm memtable bytes. Values > 4 KiB go through ValuePointer, so SSTs/memtables hold only a ~24–40 B pointer; **the value bodies (in log_stream) are not counted at all** | `partition-server/src/lib.rs::lsm_resident_bytes`; writer in `background.rs` (30 s refresh) |
| ② Per-partition `live_size` in `autumn-op info` | The **byte sum of all deduplicated extents** of the partition's three streams (log/row/meta): sealed ones use `sealed_length`, the open tail is probed from the EN on the spot. **Includes not-yet-GC'd dead bytes** and extents shared with a sibling after a CoW split (double-counted) | `autumn_op/main.rs::run_info` |
| ③ `live_size` in the overview | Periodic rollup of ②: the manager's sealed sum + `open_tail_bytes` reported in PS heartbeats | manager `compute_cluster_overview_resp` |

**The real amount of live data lies between ① and ②**. For workloads like fuse /
kvcache, where almost all bytes are behind ValuePointers, ① is a shadow tens of
times smaller.

**The two measures answer two different questions; using either in the wrong
direction is wrong once each.**

- **The merge veto** and **the floor under the rate triggers** use ② (carried
  bytes). Using ① as the merge criterion would mark a partition carrying tens of
  GB as a merge candidate — after merging into one partition, all those bytes
  would sit on a single log_stream.
- **The hard size trigger for split uses ① (LSM-resident)**. A split cuts a key
  range, and only the LSM follows the key range: large values sit in the
  log_stream, and after a CoW split both children **point at the same extents**,
  which cannot be separated without a major compaction. The cost of using ② as a
  split trigger is concrete: a partition with 0 iops, 0 B/s, 0 MiB LSM and
  73 GiB carried was recommended for split in every window, and the only effect
  a split has on it is to buy a compaction bill.

Carried bytes are given by `manager::policy::effective_size_bytes`:

```
est_live = Σ sealed_length(three streams, deduplicated)   # manager state
         + open_tail_bytes                      # reported by PS
         − gc_debt_bytes                        # reported by PS (dead bytes before the replay floor)
         − open_tail_dead_bytes                 # reported by PS (dead bytes at/after the floor, including the open tail)

effective = max(size_bytes, est_live)
```

- A pure manager-side computation: **zero wire changes, zero PS hot-path cost**
  (all four inputs are already reported).
- All arithmetic saturates: the three PS gauges each refresh on their own
  cadence, and an extent that was just punched can briefly make
  debt > sealed + open_tail; that clamps to 0 instead of wrapping, and the
  degenerate value falls back to ① through `max`.
- **Max is taken on both sides**: either measure saying "big" can veto a merge.
  (The hard size trigger for split does not consume it — see the two questions
  above; it is used only for the floor under the rate triggers.)
- Known errors (all overestimates, so the direction is safe): CoW-shared extents
  are counted once in each child; compaction garbage in row_stream is not
  subtracted; `open_tail_bytes` is 0 before the PS's first probe (the
  underestimate is covered by `max`).
- **Known gap**: `sealed_sum` is a single current snapshot applied to every
  bucket of the sliding window, so the size dimension **does not take part** in
  the "all N buckets fire" debounce — a single step in sealed reaches the size
  trigger within one tick. Acceptable today (it is only executed when a policy
  with split/merge enabled is running; otherwise it only affects the
  `policy-candidates` advisory; sealed is a slow variable; split/merge both have
  cooldowns). **It must be fixed before arming size-based auto-exec**: either
  move sealed off the hot path into per-bucket history, or take the size
  dimension out of the debounce entirely.

**Split's three triggers, each matching a bottleneck a split can actually
relieve**: request rate (one partition = one P-log thread, ~30K ops/s →
`SPLIT_QPS_HIGH`), byte rate (one partition = one log_stream, ~350 MB/s →
`SPLIT_BW_HIGH`), and LSM size (compaction/memtable work, which is exactly the
part one cut really halves → `SPLIT_LSM_HARD`, measure ①). Carried bytes are not
a fourth.

Thresholds (`crates/manager/src/policy.rs`): `SPLIT_LSM_HARD = 50 GiB` (measure ①),
`SPLIT_SIZE_MIN = 1 GiB` (measure ②, the floor under the rate triggers),
`SPLIT_QPS_HIGH = 15K`, `SPLIT_BW_HIGH = 175 MiB/s`, `MERGE_SIZE_LOW = 1 GiB`,
`MERGE_QPS_LOW = 1.5K`, `MERGE_BW_LOW = 17.5 MiB/s` (summed over the whole pair),
`HOT_COLD_MIN_HOT_SIZE_BYTES = 25 GiB` (measure ②, a separate constant).

---

## 6. Splitting

### 6.1 Runtime split: app-agnostic

`handle_split_part` (`partition-server/src/rpc_handlers.rs`): flush memtable →
pick the split point → `multi_modify_split` CoW-copies the three streams. Two
ways to pick the point:

- **median (`at_key = None`)**: take the **median of the deduplicated user keys**
  in the LSM. Requires ≥ 2 deduplicated keys and `has_overlap == 0`.
- **explicit (`at_key = Some(k)`)**: the PS checks that `k` falls strictly inside
  the partition's `(start_key, end_key)` and then **adopts it byte for byte**,
  skipping median selection and the `>= 2 keys` gate — so **an empty partition
  can be split too**, which is exactly what an after-the-fact presplit needs.

`mid_key` is a **complete arbitrary-byte user key**; there is no "can only cut at
byte K" restriction: the split point can be as deep as needed. sha256-hex is
uniform → the median by key count ≈ the median of the hash space; the median of
`[0x03][lane][ino][off]` can fall inside a file — **a runtime split can cut a
single large file, which no presplit can do**.

**Median by byte volume (not done)**: median-by-key-count is approximately
byte-balanced for fuse (extents ≤ 8 MiB); fixing it would require a per-key
cumulative VP byte histogram on the SST side, and after `effective_size_bytes` +
explicit split points no real scenario is left that needs it.

CLI (the user-facing side talks only in scopes, never hand-assembled prefix bytes):

```
autumn-op split <PARTID>                                             # median
autumn-op split <PARTID> --namespace <NS[/SUB]> [--at <SUFFIX> | --at-hex <HEX>]
autumn-op split <PARTID> --at-raw-hex <HEX>                          # admin escape hatch
```

`--namespace` takes a **scope**: the namespace itself (`kvc`) or a sub-scope
within it (`kvc/acme`, `bench/perf`), each segment `[a-z0-9._-]+`, the same
convention as `autumn-client --namespace`. `SplitPoint::resolve_at_key` lowers
the CLI intent to a bare wire key = `{scope}/ ++ suffix`;
**an empty suffix = cut exactly on the prefix boundary**. The wire layer
(`MSG_SPLIT_PART`) **only understands bare byte keys**; the partition layer
still does not know about namespaces.

### 6.2 presplit: split along the namespace's own dimension at creation

`bootstrap --presplit` **is retired** (it split at absolute points at bootstrap
time, when no real keys existed yet, so the points landed in no-man's-land and
achieved nothing; passing it now fails loudly and prints the replacement
command). A presplit is anchored to **the namespace + its own dimension** and
runs while the keyspace is still empty: `namespace-create --name <NS> [--parts <N>]`
splits right after registering; `presplit --parts <N>` adds splits after the
fact.

`namespace-create` also first cuts at the start of the namespace, `{ns}/`, and
records it as a declared boundary: every namespace starts in its own partition,
the start of the next namespace is its end, and merge never combines two
namespaces into one partition. Only the start is cut, not the end, because
another cut at `{ns}0` would leave a permanently empty gap partition between
adjacent namespaces. The start is cut only when a namespace is created, and
`presplit` cuts only by rule; namespaces created before this are left as they
are.

```
autumn-op namespace-create --name <NS> [--parts <N>]    # omitted = 1 partition
autumn-op presplit --namespace <NS[/SUB]> --parts <N>
```

There is a single parameter, `--parts N` (create N partitions), and **the first
segment decides how to split**: `fs` splits on its 24 stripe lane boundaries
(N divides 24); `kvc` splits on the hash buckets `kvc/<hex>` (N ≤ 16); other
namespaces split hex evenly under the scope. The whole scope is the split-point
prefix, and the declared split points are recorded on the registry row of the
namespace named by the first segment. `fs` is one tree and `kvc` splits only by
bucket, so both reject sub-scopes. Example:
`presplit --namespace bench/perf --parts 8` (this is what cluster.sh /
perf_check.sh use). A namespace without `--parts` has a single partition and is
later split under load only if the auto-policy allows splits (`aggressive`; the
deployment default `balanced` does not split).

`presplit_suffixes(rule)` produces **relative** split-point suffixes;
`cmd_presplit` prepends `{scope}/` to get absolute split points and calls the
explicit split of §6.1 for each point. The rules are in `PresplitRule`:
`FsLanes` / `Kvc` / `Hex`. Without `--parts`, fs and kvc still use their own
rules: nothing is split, but the lane / bucket boundaries are recorded, and later
splits snap to them.

A `kvc` key is `kvc/{h}/{model_scope}/{pool}/{hash}…`, where `h` is the first
character of the content hash (sha256 hexdigest), written by the kvcache adapter
ahead of the model identifier. So splitting by bucket needs no knowledge of
models and can be done at deployment time, and every model's writes spread
evenly over all buckets. The cost: one model's keys are scattered over 16
buckets, so deleting/listing by model has to scan 16 prefixes, and per-model
sub-prefix grants are not possible.

The two parameters of `FsLanes` are two independent decisions: **lanes = key
layout** (how many stripes a file is cut into), **parts = how many partitions
are actually created**. The split points are `[0x03][k · lanes/parts]`, so each
partition owns **exactly `lanes/parts` consecutive lanes**; `parts` must
**divide** `lanes`, otherwise partitions would own uneven lane counts and every
file would be skewed, so it is an error rather than rounded.

Creating fs (with or without `--parts`) **declares the stripe geometry (24
lanes) in the same command that cuts the boundaries**: it is written only after
all split points have been cut, and if the write fails the command fails; if the
cuts did not all complete it is not written, and nothing is left behind in the
new namespace.

Every child partition produced by a split shares its parent's SSTs; as long as
any of them contains keys outside its range (has_overlap), the PS refuses to
split it again until a major compaction, and the policy compacts only the
partitions it is about to split / merge, so new small partitions never get one.
So when the PS rejects a cut with `PreconditionFailed`, presplit first
major-compacts that partition through the op ledger, waits for it to finish,
and cuts again (the same as the policy's unblocking compaction); if any split
point is still not cut, the command fails.

Split points are cut **from high to low**, which is a speed choice: the
partition a new namespace lands in holds only the previous namespace's data,
all below the split points, so when cutting from high to low every cut after the
first lands on the left partition, which is already open and still splittable;
there is no need to compact or to wait for the right child to open in the next
region sync (if the first cut lands on a right child that still shares the SSTs
below, it still has to be compacted once) (fs `--parts` + kvc 16 + mem 4 takes
about 1–3.5 s, versus 7–30 s low to high).

### 6.3 Sacred boundaries: operator-declared split points cannot be merged away

After `autumn-op presplit` finishes cutting, it records the actual split points
back into `MgrNamespace.presplit` via `MSG_NAMESPACE_SET_PRESPLIT`. From these
the manager provides three **deliberately generic** predicates (the manager never
knows what a "lane" is):

- `sacred_boundary_owner(key)` — whether this key is a boundary declared by some
  ns. A merge must not cross it (only `--force` can).
- `declared_split_point_within(part_id)` — which **declared but not yet cut**
  point a split should snap to: of the declared points inside the partition's
  range, the one **closest to the middle**, so the split halves the lane span
  instead of shaving one off an end.
- `sacred_boundaries()` — the cluster-wide set of declared boundaries; the policy
  engine uses it to skip merge candidates before they are advertised.

The single rule "operator-declared boundaries are sacred" covers fs lane
boundaries, kvc hash buckets and the even split points of other namespaces at
once.

### 6.4 presplit and runtime split are complementary

- presplit spreads load "between files / between buckets"; what it cannot do:
  the inside of one hot bucket, or the inside of a single large fs file.
- Runtime split is the safety net itself, and it only sees large-value workloads
  through the rate triggers of §5.
- Both depend on the measures of §5 — otherwise a bad initial split would never
  get corrected.

---

## 7. Layering principle

**A namespace is a concept of the SDK + manager metadata layer and never leaks
down into the partition / stream layers.** PS / manager split, merge and routing
keep looking only at byte ranges; there is no per-namespace META table or
separate region space (the HBase analogy stops at the API; the storage layer is
still a single global range space).

Therefore we **do not** do "the partition layer recognizes at runtime which
application a key belongs to and splits by application-declared rules":

- Mechanically unnecessary: split-point selection is already data-driven (the
  median follows the real key distribution), and the app identity does not
  change the answer to "where to cut";
- Architecturally constrained: the manager is pure mechanism, policy lives in
  the controller (`auto_policy.rs`), and pushing app rules into the PS would be
  a step backwards;
- More brittle to evolve: from the day the partition layer knows about `kvc/`,
  every revision of `_keys.py` would require a matching Rust change — a
  cross-language coupling that buys no capability median + explicit split points
  cannot already provide.

The policy / controller layer (and humans) do need "which application is this
partition serving" to choose thresholds and interpret advisories (a TTL-style
cache like `kvc/` may allow more aggressive merges, `fs/` should be more
conservative). That identification only needs the intersection of the
partition's `[start_key, end_key)` with the namespace registry — a pure function
of a dozen or so lines on the manager side. **This is the real dependency between
the two claims: the prefix is a prerequisite for per-app rules in the policy
layer, not for the split code in the partition layer.**

If real per-namespace placement is ever needed (separate replication policy /
separate EC profile), that is a different system design; do not slide into it
from here.
