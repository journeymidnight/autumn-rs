#!/usr/bin/env bash
# F-AUTHZ-1 cross-tenant e2e: bring up an ISOLATED authz-ENABLED cluster from
# this tree's debug binaries, create two tenants, and run the #[ignore]
# cross-tenant isolation test. Does NOT touch any other cluster (distinct ports
# / data dirs). Memory-only manager (no etcd) — authz works without etcd.
#
#   cargo build --workspace
#   bash crates/autumn-memory/tests/run_authz_e2e.sh
#
# Env overrides: AM_PORT_BASE (default 19300), AM_WORK (default /tmp/am-authz-e2e).
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AM_WORK:-/tmp/am-authz-e2e}"
PB="${AM_PORT_BASE:-19300}"
MGR="127.0.0.1:$((PB + 1))"
EN_PORT=$((PB + 101))
PS_PORT=$((PB + 201))
ADMIN_TOKEN="authz-e2e-admin-secret"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "[authz-e2e] FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT

wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

# 1) generate an Ed25519 signing keyfile (kid 1) — stdout is the keyfile line.
"$BIN/autumn-op" gen-signing-key --kid 1 >"$WORK/signing.key" 2>/dev/null \
  || { echo "[authz-e2e] FAIL gen-signing-key"; exit 1; }
echo "[authz-e2e] signing key: $(cat "$WORK/signing.key" | cut -c1-8)…"

# 2) manager (memory-only) WITH authz enabled.
echo "[authz-e2e] manager (authz-enabled) on $MGR"
"$BIN/autumn-manager-server" --port "$((PB + 1))" --listen 127.0.0.1 \
  --auth-signing-key-file "$WORK/signing.key" \
  --admin-token "$ADMIN_TOKEN" \
  --auth-protected-prefix "mem/" \
  >"$WORK/mgr.log" 2>&1 &
PIDS+=($!); wait_port "$((PB + 1))" 20 || { echo "[authz-e2e] FAIL manager"; tail -8 "$WORK/mgr.log"; exit 1; }

# 3) EN + bootstrap + PS.
echo "[authz-e2e] format + launch EN0 on $EN_PORT"
"$BIN/autumn-op" --manager "$MGR" format --listen ":$EN_PORT" --advertise "127.0.0.1:$EN_PORT" "$WORK/en0" \
  >"$WORK/format.log" 2>&1 || { echo "[authz-e2e] FAIL format"; cat "$WORK/format.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port "$EN_PORT" --manager "$MGR" --cpuset 0 --listen 127.0.0.1 \
  >"$WORK/en0.log" 2>&1 &
PIDS+=($!); wait_port "$EN_PORT" 20 || { echo "[authz-e2e] FAIL EN"; tail -8 "$WORK/en0.log"; exit 1; }
sleep 3

echo "[authz-e2e] bootstrap (replication 1+0)"
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 \
  >"$WORK/bootstrap.log" 2>&1 || { echo "[authz-e2e] FAIL bootstrap"; cat "$WORK/bootstrap.log"; exit 1; }

echo "[authz-e2e] PS psid 1 on $PS_PORT"
"$BIN/autumn-ps" --psid 1 --port "$PS_PORT" --manager "$MGR" --data "$WORK/ps1" \
  --listen 127.0.0.1 --advertise "127.0.0.1:$PS_PORT" >"$WORK/ps1.log" 2>&1 &
PIDS+=($!); wait_port "$PS_PORT" 20 || { echo "[authz-e2e] FAIL PS"; tail -8 "$WORK/ps1.log"; exit 1; }
sleep 4

# 4) create two tenants; capture each credential (hex) from stdout.
create_tenant() { # $1 = tenant, $2 = prefix
  "$BIN/autumn-op" --manager "$MGR" tenant-create --tenant "$1" --prefix "$2" --admin-token "$ADMIN_TOKEN" 2>/dev/null \
    | awk '/^credential:/{print $2}'
}
ACME_CRED="$(create_tenant acme mem/acme/)"
OTHER_CRED="$(create_tenant other mem/other/)"
[ -n "$ACME_CRED" ] && [ -n "$OTHER_CRED" ] || { echo "[authz-e2e] FAIL tenant-create (empty cred)"; tail -20 "$WORK/mgr.log"; exit 1; }
echo "[authz-e2e] tenants created (acme=${ACME_CRED:0:8}… other=${OTHER_CRED:0:8}…)"

# 5) run the cross-tenant isolation test.
echo "[authz-e2e] running tests/authz_e2e.rs"
AUTUMN_AUTHZ_E2E_MANAGER="$MGR" \
AUTUMN_AUTHZ_E2E_ACME_CRED="$ACME_CRED" \
AUTUMN_AUTHZ_E2E_OTHER_CRED="$OTHER_CRED" \
  cargo test -p autumn-memory --test authz_e2e -- --ignored --nocapture >"$WORK/authz_e2e.log" 2>&1
RC=$?
echo "===== authz-e2e exit: $RC ====="; tail -12 "$WORK/authz_e2e.log"
exit $RC
