# autumn-dashboard

The autumn-rs web dashboard as a **standalone app** — a small
[`cyper-axum`](https://crates.io/crates/cyper-axum) server that serves the
single-page UI (`static/index.html`) and proxies every `/api/*` call to the
`autumn-op` CLI (`--json`). It holds no cluster state and makes no direct manager
RPC: the wire schema stays in exactly one place (`autumn-op`).

The leader-fenced **auto-policy controller** is NOT here — it stays inside
`autumn-manager` (crash-safe, leader-owned). This app is presentation only; its
policy panel drives the controller through `autumn-op auto-policy …`.

## Run

```bash
# autumn-op must be on PATH (or pass --autumn-op /path/to/autumn-op)
autumn-dashboard \
  --manager 127.0.0.1:9001 \
  --admin-token-file /etc/autumn/admin.token \
  --port 8799            # then open http://<host>:8799
```

The **admin token is required** (the dashboard is token-gated) and is forwarded
to every `autumn-op` call — read-only views ignore it, mutations (the per-target
Apply buttons and auto-policy activate/deactivate) use it.

| Flag | Default | |
|------|---------|--|
| `--manager H:P` | `127.0.0.1:9001` | manager address |
| `--admin-token TOK` / `--admin-token-file FILE` | — (**required**) | admin secret |
| `--port N` | `8799` | listen port |
| `--listen H` | `0.0.0.0` | bind host |
| `--transport tcp\|ucx` | `tcp` | must match the manager |
| `--autumn-op PATH` | `autumn-op` | the CLI binary |

## Endpoints → `autumn-op`

| Route | Runs |
|-------|------|
| `GET /api/overview` | `autumn-op overview` (df + nodes with per-disk rows + partitions + ps_servers + amplification + advisories) |
| `GET /api/partition/{id}` | `autumn-op info --part {id} --detail` |
| `POST /api/action` | maps `{action, part_id, …}` → `split` / `gc` / `compact` / `merge` / `force-ec-convert` / `rebalance` |
| `GET /api/policies` | `autumn-op auto-policy status` (reshaped to the page's schema) |
| `GET /api/ops` | `autumn-op ops list --active` + `ops history` → `{live, history, history_error}` |
| `POST /api/policies/activate` | `autumn-op auto-policy activate <name> [--arm]` / `deactivate` |
| `POST /api/policies/upsert` | `autumn-op auto-policy upsert <name> --switches … --interval … …` |
| `POST /api/policies/delete` | `autumn-op auto-policy delete <name>` |

The controller panel is **use** (select → DryRun / observe) → **Arm** (actuate) →
**Stop** (Off), and the custom-policy editor (create/edit/delete) is fully wired.

## Navigating the page

**Six tabs**, because the questions an operator arrives with are different
questions and each wants the whole width. The **vital signs** (topology /
capacity / throughput / controller) stay above the tabs — they are read first on
every one. The tab is in the URL hash (`#nodes`), so a view is linkable.

| Tab | Answers |
|-----|---------|
| **Overview** | the keyspace ribbon (−∞ → +∞, one segment per partition, colored by owning PS), a fleet health roll-up, space + amplification, the top advisories, and what is running |
| **Partitions** | which partition — PS-scoped list + the lazy detail drawer |
| **Servers** | which partition server — every REGISTERED PS with its heartbeat, load and partitions |
| **Nodes** | which disk — every extent node with a per-disk table (capacity, online, faulted) |
| **Policy** | what the controller would do — advisories with their full reasoning, and the policy editor |
| **Logs** | what just happened — running ops, durable outcomes, and the auto-policy action log |

Built for many partitions: the Partitions tab is **partition-server-first** (pick
a PS card and the list shows *only that server's* partitions; **All servers**
restores the full list, virtual-scrolled), and per-partition detail — extents +
load metrics — is fetched lazily when a row is opened.

**Each `/api/*` call spawns an `autumn-op` subprocess**, so the poll fetches only
what the visible tab renders: `/api/overview` always (the vitals and every tab's
data come from it), `/api/policies` on Overview + Policy, `/api/ops` on Overview
+ Logs.

### What the Servers and Nodes tabs show that nothing else could

Both exist for facts a roll-up cannot carry:

- **A PS serving nothing, or one that has gone silent.** A server list derived
  from the partitions can only show a PS that currently owns something, so those
  are exactly the two states it cannot express. `ps_servers` comes from the
  manager's registry plus its heartbeat map. A PS with no heartbeat entry
  renders as **unknown**, not dead (defensive — replay and registration both
  seed one).
- **Which disk.** A node with disks `[empty, full, full, full]` rolls up as
  half-free. Each disk is in one of three states: **online**; **faulted** — the
  node's own verdict on its own disk, which is what drives a rebuild; and **not
  reported** — the node answered but did not mention a disk the registry assigns
  to it, usually because the EN was started without that data directory. A node
  that did not answer at all shows no disk rows and says its disk state is
  unknown — an unreachable machine is not N missing disks.

### The precondition the drawer warns about

A CoW split's children share the parent's SSTs, which carry keys outside each
child's own range, and the partition server **refuses `split` until a major
compaction rewrites them**. The drawer says so before the Split button is
clicked, and the auto-policy advises that compaction *in place of* the split — so
the Policy tab shows "major compaction before split", not a split that
would be refused once per window forever.

## Security posture

Same as `--metrics-port`: no per-request auth/TLS on the dashboard port itself —
pair exposure with network ACLs. The admin token gates *mutations* against the
manager, not access to the page.

## Maintenance-ops panel

`/api/ops` returns two lists, kept apart because they answer different
questions and have different lifetimes. `live` is the leader's in-memory
ledger — a bounded ring that dies with the leader, and the only place a running
op's progress exists. `history` is the etcd-backed log, and the only place a
terminal op's failure reason survives. An op missing from `live` is therefore
not necessarily gone; it is in `history`.

A manager started without `--etcd` persists no history at all. That comes back
as `history_error` rather than an empty list, and the panel says so — an empty
list would read as "nothing failed".

**Progress counts ride the wire RAW** — the wire carries facts and the consumer
derives the ratio — so the consumer owes them a unit. `fmtProgress` supplies it
per kind; without it a 16 GiB rebuild renders as `11895046144 / 17179981824`.
Byte sizes everywhere on the page use IEC suffixes (`GiB`, `MiB`), because the
divisor is 1024 and a bare `G` names a different quantity.

## Tests

```bash
# 1. API contract against a REAL isolated cluster (own etcd, own ports, a node
#    with TWO disks). Asserts /api/ops' two lists + progress counts + error
#    text, /api/overview's ps_servers and per-disk rows, and the partition
#    detail's has_overlap. Every one of these crosses the rkyv wire, the
#    manager compose and the autumn-op subprocess — a missing key renders as a
#    silently blank panel, which no unit test would notice.
cargo build --workspace
bash examples/dashboard/tests/api_contract.sh

# 2. Render check — no cluster, no browser. LIFTS the page's own functions out
#    of index.html at run time (a copy would drift and pass while the page was
#    broken) and asserts the verdicts: percentage AND magnitude IN THE UNIT THE
#    KIND MEASURES (bytes for gc/forcegc/ec-convert/recovery, SST data blocks
#    for compact, phases for split/merge), the bar width, the right target per
#    op kind, a failed row's reason, the heartbeat classification, the three
#    disk states, and an advisory's full reasoning.
node examples/dashboard/tests/render_check.js

# 3. Tabs smoke — runs the page's own init and tab switching under a minimal DOM
#    stub that REFUSES any element id the markup does not declare, then asserts
#    each pane rendered what it exists to show. Catches a pane that throws, or a
#    renamed container, which (2) cannot see.
node examples/dashboard/tests/tabs_smoke.js
```

Measured live (1 GiB extent, EC 3+1): `/api/ops` carried
`ec-convert running 18.6% → 37.2% → 55.8% → 74.4%`, then the op moved to
`history` as `succeeded 100%`.
