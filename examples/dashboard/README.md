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
| `GET /api/policies` | `autumn-op auto-policy status` |
| `POST /api/policies/activate` | `autumn-op auto-policy activate <name> [--arm]` / `deactivate` |

The custom-policy editor (`/api/policies/upsert`, `/delete`) returns `501` until
`autumn-op auto-policy upsert/delete` land (follow-up); preset
activate/deactivate works today.

## Security posture

Same as `--metrics-port`: no per-request auth/TLS on the dashboard port itself —
pair exposure with network ACLs. The admin token gates *mutations* against the
manager, not access to the page.
