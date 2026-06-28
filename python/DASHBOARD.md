# autumn-rs cluster dashboard + auto-policy controller

`python/autumn_dashboard.py` — a live cluster dashboard and an external
auto-policy controller. Like `node_policy.py`, it **shells out to the
`autumn-op --json` Rust binary** instead of re-implementing the rkyv wire codec
in Python, so the wire schema lives in exactly one place
(`crates/rpc/src/manager_rpc.rs`) and upgrades are just "rebuild the binary".

It needs `autumn-op` on `PATH` (or `AUTUMN_OP_BIN=/path/to/autumn-op`) and a
running manager (`--manager HOST:PORT`, or `AUTUMN_MANAGER`, default
`127.0.0.1:9001`). Pure stdlib — no pip installs.

## Dashboard

```bash
# live, auto-refreshing every 2s (Ctrl-C to exit)
python3 python/autumn_dashboard.py dashboard

# one-shot snapshot (good for logs / cron / piping)
python3 python/autumn_dashboard.py --manager host:9001 dashboard --once

# skip the per-partition detail RPCs (topology + capacity only) on huge clusters
python3 python/autumn_dashboard.py dashboard --no-detail
```

Shows, per refresh:

- **Cluster capacity** — raw used/total/free, physical vs logical (sealed)
  bytes, the empirical amplification factor — from `autumn-op df`
  (`MSG_CLUSTER_DF`).
- **Node health** — per-node online flag, free/total, on-disk extent bytes.
- **Per-partition table** — sorted by IOPS (hottest first): `req_per_sec`
  (IOPS), `p99_us` latency, `size_bytes`, `gc_debt_bytes`,
  `pending_compaction_bytes`, gc/compact inflight flags, sealed-log-extent
  count — from `autumn-op info --part P --detail` (`MSG_GET_PARTITION_DETAIL`).
- **Policy advisories** — pending split/merge/gc/compact/ec/hotcold candidates
  from `autumn-op policy-candidates` (`MSG_GET_POLICY_CANDIDATES`).

```
═══ autumn-rs cluster dashboard ═══  00:00:01
capacity: raw 512.0G/1.0T (50%) used  free 512.0G  physical 256.0G  logical 128.0G  amp 2.00x
  nodes: n1[on 512.0G/1.0T ext=256.0G]

   PART   PS     IOPS   p99us     SIZE   GCdebt  COMPACT  INFL SEALEXT
   9001    1    20000     500     1.0G     1.0M       0B    --       3
   9002    2      100     500     1.0G     1.0M       0B    --       3

policy advisories (2):
  split  part 9001          qps high
  ec     extent 50          sealed 128M
```

## Auto-policy controller

The manager is **pure mechanism** (F203): it only *emits* advisories from its
30-minute windowed per-partition load metrics; it does **not** self-dispatch
split/merge/ec. This controller is the external policy loop that *decides* and
*actuates* — exactly the role F203 carved out.

```bash
# DRY-RUN by default — prints what it WOULD do, touches nothing
python3 python/autumn_dashboard.py control

# actually actuate, every 30s, EC + maintenance only (conservative)
python3 python/autumn_dashboard.py control --apply --enable ec,gc,major,minor

# full auto: split/merge/ec/gc/compact, 2 actions/tick, 5-min per-target cooldown
python3 python/autumn_dashboard.py control --apply --interval 30 --max-actions 2 --cooldown 300

# one cycle then exit (cron / k8s CronJob)
python3 python/autumn_dashboard.py control --apply --once
```

Each tick it polls `policy-candidates` and, for the enabled kinds, actuates:

| advisory kind | autumn-op actuation                         |
|---------------|---------------------------------------------|
| `split`       | `split <primary_part_id>`                   |
| `merge`       | `merge <primary_part_id> <secondary_part_id>` (survivor ← victim) |
| `ec`          | `force-ec-convert --extent <secondary_part_id>` |
| `gc`          | `gc <primary_part_id>`                       |
| `major`/`minor` | `compact <primary_part_id>`               |
| `hotcold`     | advisory only (placement hint, no mutation) |

Safety:

- **`--apply` required to mutate** — default is a printed dry-run.
- **`--enable`** whitelists which kinds may be actuated.
- **`--max-actions` per tick** rate-caps mutations.
- **Per-`(kind, target)` cooldown** (`--cooldown`, default 300s) suppresses
  re-issuing the same action while it's likely still settling — on top of the
  manager's own cooldowns.
- **Priority order** `split → gc → minor → major → ec → merge`: split is the
  relief valve (it *spreads* load), merge is last because it *concentrates*
  load onto one core (the thread-per-core model — auto-split before auto-merge).
- **A manager refusal is benign** — if the manager rejects an action
  (already in flight / precondition), it's logged and retried next tick. The
  controller carries no orchestration state of its own; the manager's ops are
  already crash-safe + idempotent-on-retry (F149 leader fence, F207 inflight
  ledger, F185 merge freeze), so this stays a thin, restartable poller.

## Tests

`python/test_autumn_dashboard.py` unit-tests all the pure logic (candidate →
command mapping incl. EC's extent-in-`secondary`, decide priority/cooldown/cap,
dry-run vs apply, refusal handling, gather + render) against a fully mocked
`autumn-op`, so no live cluster is needed:

```bash
python3 python/test_autumn_dashboard.py        # 14 tests, stdlib-only runner
# or, if pytest is available:
python3 -m pytest python/test_autumn_dashboard.py
```
