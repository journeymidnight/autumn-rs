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
| `GET /api/overview` | `autumn-op overview` (df + nodes + partitions + amplification + advisories) |
| `GET /api/partition/{id}` | `autumn-op info --part {id} --detail` |
| `POST /api/action` | maps `{action, part_id, …}` → `split` / `gc` / `compact` / `merge` / `force-ec-convert` / `rebalance` |
| `GET /api/policies` | `autumn-op auto-policy status` (reshaped to the page's schema) |
| `GET /api/ops` | `autumn-op ops list --active` + `ops history` → `{live, history, history_error}` |
| `POST /api/policies/activate` | `autumn-op auto-policy activate <name> [--arm]` / `deactivate` |
| `POST /api/policies/upsert` | `autumn-op auto-policy upsert <name> --switches … --interval … …` |
| `POST /api/policies/delete` | `autumn-op auto-policy delete <name>` |

The controller panel is **use** (select → DryRun / observe) → **Arm** (actuate) →
**Stop** (Off), and the custom-policy editor (create/edit/delete) is fully wired.

## Navigating the page (built for many partitions)

The layout is **partition-server-first** so it stays legible when a cluster has
thousands of partitions:

1. **Vital signs** (topology / capacity / throughput / controller) read first.
2. The **keyspace ribbon** (−∞ → +∞) shows every partition as a segment colored
   by its owning PS — click any segment to scope.
3. **Partition servers** are the primary drill-in: pick a PS card and the list
   below shows *only that server's* partitions (dozens/hundreds, not the whole
   keyspace). **All servers** restores the full list, virtual-scrolled.
4. Selecting a partition opens the **detail drawer** — load metrics + per-extent
   distribution, fetched lazily on expand.
5. **Extent nodes** (storage layer) sit at the bottom; click one for its detail.

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

Contract test (isolated cluster with its own etcd, asserts the shape and that a
record carries the progress counts and the error text):

```bash
cargo build --workspace
bash examples/dashboard/tests/ops_contract.sh
```
