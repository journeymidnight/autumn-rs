# F-DASH-IN-MGR — fold the web dashboard + auto-policy controller into `autumn-manager`

**Status:** plan (approved decisions folded in 2026-07-04) — not yet started.
**Produced by:** Fable planning sub-agent, grounded against real symbols (all verified to exist).

---

## 1. Goal & scope

Move the entire `python/dashboard/` surface into the `autumn-manager` binary:

1. **Embedded web dashboard** — the existing 29 KB single-page UI served over a small
   hand-rolled HTTP/1.1 listener on the manager's compio runtime, HTML embedded via
   `include_str!`, backed by **in-process** data (no `autumn-op` subprocess, no rkyv-over-TCP
   round-trip to itself).
2. **Auto-policy controller** — the Python `AutoPolicy` tick-loop (poll advisories → decide →
   actuate split/merge/gc/compact/force-ec) as a **leader-fenced, etcd-persisted, default-OFF**
   manager background task.

**Why:** autumn-rs is "all-in-one AI storage". A standalone Python dashboard is architecturally
fragmented and — critically — hosts the auto-policy controller in a killable web process: stop the
dashboard and auto-split/merge/gc/compact/ec stops with it. Standing directive
(`feedback_orchestrator_must_be_crash_safe`): *编排/policy 循环必须进 leader-fenced manager,不进
CLI/SDK*. One process, crash-safe, leader-owned.

**Non-goals:** no change to advisory *emission* (the `policy.rs` mechanism layer is untouched);
no TLS/auth on the dashboard port (same posture as the existing `--metrics-port`); no redesign of
the HTML UI (byte-preserving port); no PS-local maintenance scheduler.

## 2. Decisions (user, 2026-07-04)

| # | Decision | Consequence |
|---|----------|-------------|
| Rollout | **On by default, bind the manager `--listen` host** | Deploy layer (cluster.sh / entrypoint.sh / autumn-deploy) defaults `AUTUMN_DASHBOARD=1 → --dashboard-port 8799`; `--dashboard-listen` defaults to the manager's listen host (reachable cluster-wide). Default state is a **read-only** viewer (mutations still gated, see below). |
| Arming | **One flag for both** | A single `--dashboard-allow-mutations` gates BOTH the controller leaving DryRun AND the manual `/api/action` buttons. No separate `--auto-policy-allow`. |
| Console CLI | **Retire `python/dashboard/` fully** | Delete all 5 files in M4. Decision-logic tests port to Rust first. Headless viewing stays on `autumn-op df/info/policy-candidates` + the new `auto-policy` subcommands. |
| Headless ctl | **Include `autumn-op auto-policy` subcommands** | Add `auto-policy status|activate|deactivate` (small new manager RPCs) so chaos/perf/deploy scripts toggle the controller without a browser. |

## 3. Architecture — HTTP serving

> **DECISION UPDATE (2026-07-04, user): use `examples/gallery`'s stack, NOT hand-rolled.**
> After M0 first shipped hand-rolled, the user chose to align the dashboard with
> gallery — `axum` served by the compio-native `cyper_axum::serve` over a
> `compio::net::TcpListener`, with `send_wrapper::SendWrapper` bridging axum's
> `Send` bound over the `!Send` manager (gallery's exact idiom:
> `SendWrapper::new((client.clone(), …))` + `SendWrapper::new(async …)`).
> Rationale: one HTTP-serving pattern across the repo. Trade-off accepted: +3 deps
> (`axum`/`cyper-axum`/`send_wrapper`) on the core crate + `SendWrapper` on
> stateful handlers. In exchange, request parsing (incl. a hostile
> `Content-Length` → hyper 431, no panic) is handled by hyper/axum and
> `DefaultBodyLimit` caps bodies. Residual: no active Slowloris cut-off
> (`cyper_axum::serve` exposes no header-read timeout) — same as gallery,
> compensated by read-only-default + network-ACL guidance. The hand-rolled
> analysis below is kept for the record.

### (superseded) hand-rolled compio HTTP/1.1

Precedent: `crates/common/src/metrics_http.rs` — *"hand-rolled HTTP/1.1 is ~40 lines, no new
dependencies"*, already wired into the manager binary. **But** it runs on its own `std::net`
OS thread, and the manager store is `Rc<RefCell<MetadataState>>` (`!Send`) — an OS-thread server
can only serve pre-rendered strings, which is fine for `/metrics` but not for the dashboard's
request-driven `/api/partition/<id>` + POST `/api/action`.

**Approach:** new `crates/manager/src/dashboard.rs` — an accept loop on a compio `TcpListener`
(mirrors `AutumnManager::serve()` in `rpc_handlers.rs:34`, incl. the F257 accept-error tolerance:
log + 100 ms backoff, never `?` out of the loop). Each accepted connection is handled in a
detached `compio::runtime::spawn`, so handlers get direct `Rc<AutumnManager>` access with zero
channels/snapshots, and a stalled browser can't head-of-line-block the RPC plane. Guards:
`Connection: close`, 64 KiB request cap, read/write timeouts.

Rejected: axum + `cyper-axum` (used by `examples/memory-browser`, and it *is* compio-native so no
runtime mixing) — but the manager's dep list is deliberately lean (rkyv-only, no serde), the surface
is 1 page + 7 endpoints, and pulling the axum tree in buys nothing at this scale.

New dep: `serde_json` only (already used by `crates/server`; `serde` is a workspace dep). JSON
responses are built with `serde_json::json!` — no derives on the rkyv wire structs.

**Where spawned / flags:** `AutumnManager::start_dashboard(listen_host, port, allow_mutations)`,
wrapped in the existing `spawn_supervised` panic isolation (`lib.rs:1081`, F228), called from
`crates/server/src/bin/manager.rs` main next to the metrics block. Flags `--dashboard-port <P>`,
`--dashboard-listen <HOST>` (default = `--listen`), `--dashboard-allow-mutations` — all mirroring
`--metrics-port`/`--metrics-listen` parsing (`manager.rs:142`). No env reads in Rust
(`feedback_no_env_in_rs`).

## 4. Architecture — the policy loop (leader-fenced, opt-in, in-process)

> **INVARIANT (leader-only).** The auto-policy loop runs on the etcd **leader and nowhere else**.
> Every tick begins with `if !self.leader.get() { continue; }` — no candidate read, no decision, no
> actuation, not even DryRun logging of an actuation on a follower. On demotion the loop goes idle
> on the next tick; a straggler in-flight actuation is additionally refused by the F149 leader fence
> carried on the manager ops themselves. Followers MAY serve the read-only dashboard endpoints
> (replica state), but the loop and every mutating/config-writing endpoint are leader-gated.

**F203 reconciliation (must be written into `crates/manager/CLAUDE.md`).** F203 made the manager
pure *mechanism*: `policy_tick_loop` (`lib.rs:1201`) only fills `advisory_cache` via
`recompute_advisory_cache`; dispatch was deleted and moved to the external Python controller.
Folding the loop back in does **not** revert F203's layering:

- Advisory emission stays a separable mechanism layer that never self-dispatches.
- The controller is a **distinct** module (`auto_policy.rs`), **default-OFF** — a fresh cluster is
  byte-for-byte pure-mechanism until an operator selects AND enables a policy.
- Lifecycle is a **state machine, not a bool** (`feedback_state_machine_not_bool`):
  `Off → DryRun → Armed`. `Off` = nothing runs. `DryRun` = loop runs and logs "would: …" (the
  Python `--apply`-absent behavior). `Armed` = actuates, and is only honored when
  `--dashboard-allow-mutations` is present (else it degrades to DryRun with a WARN each transition).

What changes is the *host process* (a crash-safe etcd leader instead of a killable Python
webserver), not the mechanism/policy boundary.

**Loop shape** — new `crates/manager/src/auto_policy.rs`, one `spawn_supervised("auto_policy", …)`
in `start_runtime_tasks` (`lib.rs:1106`):

- Coarse ~1 s tick; `if !self.leader.get() { continue; }` — the same `leader: Rc<Cell<bool>>`
  gate (`lib.rs:87`) every leader-only loop uses. Demotion stops actuation next tick; promotion
  resumes after `replay_from_etcd` reloads config.
- Every `policy.interval` seconds: read `advisory_cache` **directly** (the exact data
  `handle_get_policy_candidates` serves at `rpc_handlers.rs:3724` — no RPC, no subprocess), run the
  ported pure `decide_actions`, actuate up to `max_actions`.
- **Ported pure logic** (from `python/dashboard/autumn_dashboard.py`, unit-tested in Rust):
  - `decide_actions` (py:273) — filter by enabled kinds → priority order
    `{split:0, gc:1, minor:2, major:3, ec:4, merge:5}` → per-tick target dedup → cooldown → cap.
  - `candidate_to_cmd` (py:237) — ec carries the extent in `secondary_part_id`; merge = primary
    survivor / secondary victim; major+minor both → compact.
  - `cooldown_key` (py:263), `describe_candidate` (py:222).
- **In-process actuation** (no subprocess, no self-RPC):
  - split → `auto_dispatch_split` (`lib.rs:1307`, sends `MSG_SPLIT_PART` to the owning PS).
  - merge → extract the **F185 freeze-drain** body of `handle_merge_partitions`
    (`rpc_handlers.rs:3461`, the frozen-for-merge path at 3382) into
    `pub(crate) async fn do_merge_partitions(...)` — NOT the F184 flush-based `auto_dispatch_merge`
    (`lib.rs:1359`), which has the ~5 % loss window F185 closed. Both the RPC handler and the loop
    call the extracted fn.
  - gc / compact → PS RPC `MSG_MAINTENANCE` (`MAINTENANCE_AUTO_GC` / `MAINTENANCE_COMPACT`) via
    `conn_pool.call_timeout` — the pattern already in `auto_dispatch_merge`'s flush closure
    (`lib.rs:1381`).
  - ec → extract `handle_force_ec_convert`'s body (`rpc_handlers.rs:3797`) into an inner fn taking
    `extent_id` (already idempotent, F198).
- Refusals (Precondition / inflight / NotLeader) → logged to the action log, retried next tick.
  Safe because the underlying ops are crash-safe + idempotent-on-retry (F149 fence, F207 inflight
  ledger, F185 merge freeze).

## 5. Config & persistence (etcd, leader-fenced)

New rkyv structs in `crates/rpc/src/manager_rpc.rs` (precedent: `MgrTenantAccount`,
`MgrEcDispatchInflight`), written via the F149-fenced `EtcdMirror::put_msgs_txn` (`lib.rs:243`) so
**only the leader mutates controller config**, loaded in `replay_from_etcd` (`lib.rs:1976`) so it
survives restart + failover:

- `autoPolicy/config` → `MgrAutoPolicyConfig { ver, mode: u8, active: String, policies: Vec<MgrAutoPolicyEntry> }`
  - `mode`: `0=Off, 1=DryRun, 2=Armed` (Armed honored only with `--dashboard-allow-mutations`).
  - `MgrAutoPolicyEntry { name, desc, switches: [bool;5] (split/ec/compact/gc/merge), interval_sec,
    cooldown_sec, max_actions, builtin }`.
  - The 5 presets (`gc-only`, `maintenance`, `space-reclaim`, `balanced`, `aggressive`) are
    compiled-in constants, **not** persisted; only custom entries + the `(mode, active)` pair go to etcd.
- `autoPolicy/cooldowns` → `MgrAutoPolicyCooldowns { entries: Vec<(String, i64)> }` — per-target
  last-actuation stamps, best-effort. Loss on failover is bounded (the load-bearing guard is the
  advisory layer's own server-side cooldowns + `*_inflight` flags; the client-side cooldown is
  defense-in-depth).
- In-memory on `AutumnManager`: `Rc<RefCell<AutoPolicyState>>` (config mirror + rolling action log,
  last 100 entries, served by `/api/policies` + `auto-policy status`). Log is leader-local only.

## 6. New RPCs + `autumn-op` subcommands

Add to `crates/rpc/src/manager_rpc.rs` (bumps the wire fingerprint — MIN=MAX same-commit deploy
discipline, as F-FS-UNIFY M0 did with WIRE v11):

- `MSG_AUTOPOLICY_GET` — returns `MgrAutoPolicyConfig` + the action log. Routes to leader
  (`mgr_call_leader`) since the log is leader-local.
- `MSG_AUTOPOLICY_SET` — a tagged request `{ SetMode{mode} | SetActive{name} | Upsert{entry} |
  Delete{name} }`. Leader-fenced write to `autoPolicy/config`.

`autumn-op` (`crates/server/src/bin/autumn_op/main.rs`) gains `auto-policy status | activate <name>
[--arm] | deactivate`. The HTTP `/api/policies/*` handlers and these RPC handlers share the same
`pub(crate)` core fns (single source of truth for the state transitions).

## 7. HTML embedding

Copy `python/dashboard/autumn_dashboard_web.html` → `crates/manager/src/dashboard_web.html`;
`const DASHBOARD_HTML: &str = include_str!("dashboard_web.html");` served at `GET /`. **No JS
endpoint-path changes** — the page fetches only relative paths (`/api/overview`,
`/api/partition/<id>`, `/api/policies{,/upsert,/activate,/delete}`, `/api/action`). The Rust
endpoints reproduce the exact JSON contracts:

- `/api/overview` → `build_overview` shape (`autumn_dashboard_web.py:120`): `ts, df{…}, nodes[],
  partitions[], ps_roll[], part_count, ps_count, total_*_per_sec, advisories[{kind, primary_part_id,
  secondary_part_id, reason, desc, cmd, key}], errors[]`. Field names must match `autumn-op`'s JSON
  shaping (the HTML consumes those names). Sourced in-process from extracted pure helpers around
  `handle_cluster_df` (`rpc_handlers.rs:1423`), `handle_get_cluster_overview` (4112),
  `handle_list_node_states` (4537), + `advisory_cache`.
- `/api/partition/<id>` → `partition_detail` shape (`autumn_dashboard_web.py:189`):
  `handle_get_partition_detail` (`rpc_handlers.rs:4076`) + topology/extents read from `store.inner`.
- The only permissible HTML edit (cosmetic, M4): footer text mentioning `--allow-mutations` →
  `--dashboard-allow-mutations`.

## 8. Safety gates

- **`--dashboard-allow-mutations`** (default absent = read-only viewer): without it, POST
  `/api/action` and any Armed transition return the read-only error string (port of
  `AutoPolicy.activate`, `autumn_dashboard_web.py:346`). `/api/overview`, `/api/partition/*`,
  `/api/policies` GET always work.
- **Backend verb whitelist** (port of `validate_action` + `ALLOWED_VERBS`,
  `autumn_dashboard_web.py:60/395`): `/api/action` keeps the `{cmd:["split","7"]}` contract; Rust
  validates verb ∈ {split, merge, gc, forcegc, compact, force-ec-convert} and every remaining token
  is an integer (or literal `--extent`) before dispatching to the same in-process actuation fns as
  the loop.
- **Controller default-OFF**: fresh etcd has no `autoPolicy/config` → Off, no active policy.

## 9. Milestones (each independently committable + testable)

- **M0 — HTTP layer + embedded page (read-only skeleton).** `dashboard.rs` (compio accept loop,
  request parse, router, `include_str!` HTML, `/healthz`); `--dashboard-port/--dashboard-listen`
  flags; cluster.sh `AUTUMN_DASHBOARD=1` default-on threading. `/api/overview` returns df +
  partitions (first extracted helper); other endpoints stub.
  *Acceptance:* `cluster.sh start`; `curl :8799/` serves the page, `/api/overview` returns valid
  JSON with real capacity + partitions; RPC plane unaffected (`perf_check` unchanged); HTTP-parser
  unit test.
- **M1 — full in-process data parity.** Refactor `handle_cluster_df` /
  `handle_get_cluster_overview` / `handle_get_partition_detail` / `handle_list_node_states` into
  `compute_*` pure fns (RPC handlers become encode-wrappers); implement `/api/partition/<id>` incl.
  extents, node-row merge, ps_roll, advisories with `desc/cmd/key`.
  *Acceptance:* browser side-by-side vs the Python page on the same live cluster shows identical
  numbers; `cargo test -p autumn-manager` green.
- **M2 — controller (DryRun cap) + etcd config + new RPCs + `autumn-op` subcommands.**
  `auto_policy.rs` (ported `decide_actions`/`candidate_to_cmd`/`cooldown_key` + unit tests ported
  from `test_autumn_dashboard.py`); `MgrAutoPolicyConfig` + `autoPolicy/` keys + replay;
  `spawn_supervised` leader-gated loop; `MSG_AUTOPOLICY_GET/SET` + wire bump; `autumn-op auto-policy
  status|activate|deactivate`; HTTP `/api/policies` GET/upsert/activate/delete. Mode capped at DryRun.
  *Acceptance:* activate `gc-only` under write load → action log shows "would: gc part N"; `kill -9`
  the leader → new leader resumes the SAME active policy + mode from etcd; follower never ticks;
  `autumn-op auto-policy status` reflects state.
- **M3 — mutations + arming.** `--dashboard-allow-mutations` gates loop-Armed + `/api/action`;
  in-process actuation (split / extracted `do_merge_partitions` F185 path / gc / compact / ec);
  POST `/api/action` live.
  *Acceptance:* fast-mode cluster, `aggressive` Armed → a real auto-split + auto-gc land (manager
  log + `info`); manual UI compact works; without the flag both paths refuse; leader-failover during
  Armed operation resumes actuating on the new leader.
- **M4 — retire Python + docs.** Delete `python/dashboard/` (5 files); fold `DASHBOARD.md` ops
  content into `docs/ops.md`; update `README.md`, `crates/manager/CLAUDE.md` (F203 note),
  `crates/server/CLAUDE.md` (manager flags), deploy k8s/baremetal port exposure, `feature_list.md`,
  `claude-progress.txt`.
  *Acceptance:* `grep -r autumn_dashboard` finds only history/archive; `docs/ops.md` manual steps
  executable against a fresh cluster.

## 10. Files touched

**New:** `crates/manager/src/dashboard.rs`, `crates/manager/src/dashboard_web.html`,
`crates/manager/src/auto_policy.rs`.

**Modified:** `crates/manager/src/lib.rs` (`AutoPolicyState`, `start_dashboard`,
`spawn_supervised("auto_policy")`, `replay_from_etcd` loads `autoPolicy/`, `do_merge_partitions`
extraction) · `crates/manager/src/rpc_handlers.rs` (`compute_*` extractions; force-ec inner fn;
`MSG_AUTOPOLICY_*` handlers) · `crates/rpc/src/manager_rpc.rs` (`MgrAutoPolicy*` structs +
`MSG_AUTOPOLICY_GET/SET` + fingerprint bump) · `crates/manager/Cargo.toml` (`serde_json`) ·
`crates/server/src/bin/manager.rs` (3 flags) · `crates/server/src/bin/autumn_op/main.rs`
(`auto-policy` subcommands) · `cluster.sh`, `deploy/docker/entrypoint.sh`,
`deploy/baremetal/autumn-deploy`, `deploy/k8s/manager.yaml` + `configmap.yaml` (env→flag + port
exposure) · `crates/manager/CLAUDE.md`, `crates/server/CLAUDE.md`, `docs/ops.md`, `README.md`,
`feature_list.md`, `claude-progress.txt`.

**Deleted (M4):** `python/dashboard/{autumn_dashboard.py, autumn_dashboard_web.py,
autumn_dashboard_web.html, test_autumn_dashboard.py, DASHBOARD.md}`.

## 11. Risks & residuals

- **Network-reachable mutating surface (from Decision: bind manager host + one flag).** Default is
  a read-only viewer, safe. But arming with `--dashboard-allow-mutations` exposes BOTH the auto-loop
  AND a mutating browser UI on the manager's listen host, unauthenticated. Mitigation: the storage
  network is already a trusted plane; document pairing arming with network ACLs, and note a
  `--dashboard-listen 127.0.0.1` override for tunnel-only arming. On k8s the port rides the
  leader-gated manager Service. **Flag this prominently in `docs/ops.md`.**
- **Hand-rolled HTTP correctness** — bounded by `Connection: close`, 64 KiB cap, timeouts, per-conn
  detached task; parser handles exactly GET/POST + Content-Length.
- **F203 optics** — mitigated by §4 (opt-in, default-OFF, mechanism untouched); write into
  `crates/manager/CLAUDE.md` so a future session doesn't "fix" it back.
- **Leader failover mid-actuation** — split/ec/merge already fenced/idempotent (F149/F198/F185 30 s
  freeze TTL/F207 ledger); residual is a lost `autoPolicy/cooldowns` stamp → one possible early
  re-actuation, bounded by server-side per-kind cooldowns + inflight flags.
- **Wire-version bump** — `MSG_AUTOPOLICY_*` bumps the fingerprint; MIN=MAX same-commit deploy
  (whole-cluster stop/start, `feedback_stopworld_restart_primary`), no rolling.

## 12. `feature_list.md` entry (to add at task start)

```markdown
### F-DASH-IN-MGR — web dashboard + auto-policy controller folded into autumn-manager
- **Trigger**: "all-in-one AI storage" — the standalone Python dashboard (`python/dashboard/`) is
  architecturally fragmented AND hosts the auto-policy controller in a killable web process: stop the
  dashboard and auto-split/merge/gc/compact/ec stops with it. Standing directive: 编排/policy 循环必须进
  leader-fenced manager,不进 CLI/SDK. Fold both into `autumn-manager` (HTML embedded via `include_str!`),
  one process, crash-safe, leader-owned. F203 reconciliation: advisory emission stays pure mechanism; the
  in-manager controller is leader-fenced, etcd-config-driven, and DEFAULT-OFF (Off→DryRun→Armed state
  machine; armed only when an operator selects+enables a policy AND `--dashboard-allow-mutations` is set),
  so a fresh cluster remains pure-mechanism.
- **Scope**: `crates/manager/src/dashboard.rs` (hand-rolled HTTP/1.1 on a compio listener —
  metrics_http precedent, NOT axum; `include_str!` of `dashboard_web.html`; JSON endpoints byte-compatible
  with the Python contracts; verb-whitelisted `/api/action`) + `crates/manager/src/auto_policy.rs` (ported
  decide_actions/candidate_to_cmd/cooldown_key; in-process actuation: auto_dispatch_split, extracted
  do_merge_partitions (F185 freeze path), PS MSG_MAINTENANCE gc/compact, force-ec inner fn) + etcd
  `autoPolicy/{config,cooldowns}` (rkyv, leader-fenced put_msgs_txn, replay_from_etcd) + `MSG_AUTOPOLICY_*`
  RPCs + `autumn-op auto-policy status|activate|deactivate` + flags
  `--dashboard-port/--dashboard-listen/--dashboard-allow-mutations` (env→flag in
  cluster.sh/entrypoint.sh/autumn-deploy per [[feedback_no_env_in_rs]], default-on bound to manager host) +
  retire `python/dashboard/` (5 files; decision tests ported to Rust; DASHBOARD.md → docs/ops.md).
- **Acceptance**: (a) `cluster.sh start` (AUTUMN_DASHBOARD=1 default) → browser dashboard serves from the
  manager with data parity vs the Python page on the same cluster; (b) fast-mode cluster, preset `gc-only`
  activated + armed → real auto-gc actuations logged; `kill -9` the leader → new leader resumes the SAME
  active policy from etcd and keeps actuating (the Python version provably cannot survive this); (c)
  default-OFF: fresh cluster shows zero controller activity until armed; without `--dashboard-allow-mutations`
  every mutation path refuses; (d) manager RPC-plane perf unchanged (perf_check within threshold); (e)
  `cargo test -p autumn-manager` green incl. ported decision-logic tests; (f) `autumn-op auto-policy status`
  reflects live state.
- **Status**: `passes: false`
```
