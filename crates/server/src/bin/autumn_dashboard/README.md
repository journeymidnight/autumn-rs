# autumn-dashboard

The autumn-rs web dashboard is a **server component** in `autumn-server`.
It runs as its own process: a small
[`cyper-axum`](https://crates.io/crates/cyper-axum) server that serves the
single-page UI (`static/index.html`) and proxies every `/api/*` call to the
`autumn-op` CLI (`--json`). It holds no cluster state and makes no direct manager
RPC: the wire schema stays in exactly one place (`autumn-op`).

The leader-fenced **auto-policy controller** is NOT here — it stays inside
`autumn-manager` (crash-safe, leader-owned). This component exposes operator
controls; its
policy panel drives the controller through `autumn-op auto-policy …`.

## Run

```bash
cargo build -p autumn-server --bin autumn-dashboard --bin autumn-op
# autumn-op must be on PATH (or pass --autumn-op /path/to/autumn-op)
autumn-dashboard \
  --manager 127.0.0.1:9001 \
  --cluster-secret-file /etc/autumn/cluster.secret \
  --port 8799            # then open http://<host>:8799
```

The **cluster secret file is required**; its path is forwarded to every
`autumn-op` call. autumn-op connects as an operator, which the manager refuses
without the secret — read-only views included.

| Flag | Default | |
|------|---------|--|
| `--manager H:P` | `127.0.0.1:9001` | manager address |
| `--cluster-secret-file FILE` | — (**required**) | the cluster secret (`docs/cluster_secret_design.md`) |
| `--port N` | `8799` | listen port |
| `--listen H` | `0.0.0.0` | bind host |
| `--transport tcp\|ucx` | `tcp` | must match the manager |
| `--autumn-op PATH` | `autumn-op` | the CLI binary |

## Endpoints → `autumn-op`

| Route | Runs |
|-------|------|
| `GET /api/overview` | `autumn-op overview` (df + nodes with per-disk rows + partitions + ps_servers + amplification + advisories + extent health) |
| `GET /api/partition/{id}` | `autumn-op info --part {id} --detail` |
| `POST /api/action` | maps `{action, part_id, …}` → `split` / `gc` / `compact` / `merge` / `force-ec-convert` / `rebalance` / `repair <extent>` / `repair --node <id>` |
| `GET /api/policies` | `autumn-op auto-policy status` (reshaped to the page's schema) |
| `GET /api/ops` | `autumn-op ops list --active` + `ops history` → `{live, history, history_error}` |
| `POST /api/policies/activate` | `autumn-op auto-policy activate <name> [--arm]` / `deactivate` |
| `POST /api/policies/upsert` | `autumn-op auto-policy upsert <name> --switches … --interval … …` |
| `POST /api/policies/delete` | `autumn-op auto-policy delete <name>` |

Each policy has a **Start** button: confirm the named policy to select it and
run its enabled actions immediately. **Observe** is an optional preview that
logs proposed actions without executing them; it is not a prerequisite for
Start. **Stop** stops the current controller. Only one policy runs at a time;
starting or observing another replaces the current selection. The page shows
**Running / Observing / Stopped** and disables the current mode's button.

The custom-policy editor supports create/replace/delete. Policy names must be
nonblank and must not start with `-` (they are CLI positional arguments).
`switches` must be a boolean object using the six listed names; omitted switches
are off. Optional `interval` is an integer >= 2, `cooldown` an unsigned integer,
and `max_actions` an integer in 1..=100. Invalid requests return HTTP 400.
Manager refusals return HTTP 502 with the CLI output; the page displays the
reason and retains failed edits. Failed status reads display **unknown**.

## Navigating the page

**Six tabs**, because the questions an operator arrives with are different
questions and each wants the whole width. The **vital signs** (topology /
capacity / throughput / controller) stay above the tabs — they are read first on
every one. The tab is in the URL hash (`#nodes`), so a view is linkable.

| Tab | Answers |
|-----|---------|
| **Overview** | the keyspace ribbon (−∞ → +∞, one segment per partition, colored by owning PS), a fleet health roll-up with extent alerts (unavailable / degraded / no redundancy left / rebuilding, naming the worst extents, with a Repair button for the worst readable one), space + amplification, the top advisories, and what is running |
| **Partitions** | which partition — PS-scoped list + the lazy detail drawer |
| **Servers** | which partition server — every REGISTERED PS with its heartbeat, load and partitions |
| **Nodes** | which disk — every extent node with a per-disk table (capacity, online, faulted) |
| **Policy** | what the controller would do — advisories with their full reasoning, and the policy editor |
| **Logs** | what just happened — running ops, durable outcomes, and the auto-policy action log |

Built for many partitions: the Partitions tab is **partition-server-first** (pick
a PS card and the list shows *only that server's* partitions; **All servers**
restores the full list, virtual-scrolled), and per-partition detail — extents +
load metrics — is fetched lazily when a row is opened.

The Policy tab calls its first panel **Operational advisories** because those
rows are diagnoses, not controller policies. In particular, `hotcold` means
partitions on one PS have stayed at least 10x apart in request rate or carried
size across five one-minute samples (with a 10,000 QPS or 25 GiB hot-side
floor). It is information-only: the row names the PS, ratio, and busy/quiet or
large/small partitions, but never dispatches an operation by itself.

**Each `/api/*` call spawns an `autumn-op` subprocess**, so the poll fetches only
what the visible tab renders (concurrent identical reads in one page share a
single request): `/api/overview` always (the vitals and every tab's
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

The dashboard HTTP port has **no per-request authentication or TLS**. Anyone
who can reach it can read cluster state and submit mutations with the cluster
secret the dashboard holds. The VKE overlay includes an APIG Ingress for all paths; ClusterIP
does not make that route private. Use network access controls or a loopback
bind and tunnel. HTTP authentication remains outside this migration, following
the existing access contract. See [the review](../../../../../docs/dashboard_review.md)
for this boundary and the existing non-atomic CLI policy activation.

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
cargo build -p autumn-server --bins
bash crates/server/src/bin/autumn_dashboard/tests/api_contract.sh

# 2. Render check — no cluster, no browser. LIFTS the page's own functions out
#    of index.html at run time (a copy would drift and pass while the page was
#    broken) and asserts the verdicts: percentage AND magnitude IN THE UNIT THE
#    KIND MEASURES (bytes for gc/forcegc/ec-convert/recovery, SST data blocks
#    for compact, phases for split/merge), the bar width, the right target per
#    op kind, a failed row's reason, the heartbeat classification, the three
#    disk states, and an advisory's full reasoning.
node crates/server/src/bin/autumn_dashboard/tests/render_check.js

# 3. Tabs smoke — runs the page's own init and tab switching under a minimal DOM
#    stub that REFUSES any element id the markup does not declare, then asserts
#    each pane rendered what it exists to show. Catches a pane that throws, or a
#    renamed container, which (2) cannot see.
node crates/server/src/bin/autumn_dashboard/tests/tabs_smoke.js

# 4. Failed policy mutations/status, unusual names, and overlapping refreshes.
node crates/server/src/bin/autumn_dashboard/tests/policy_controls.js
```

Measured live (1 GiB extent, EC 3+1): `/api/ops` carried
`ec-convert running 18.6% → 37.2% → 55.8% → 74.4%`, then the op moved to
`history` as `succeeded 100%`.
