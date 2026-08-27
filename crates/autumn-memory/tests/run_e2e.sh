#!/usr/bin/env bash
# Phase-1 manual verification for autumn-memory: spin up an ISOLATED minimal
# cluster (loopback, alt ports, memory-only manager = no etcd) from this tree's
# debug binaries, run the #[ignore] e2e, and tear everything down. Does NOT
# touch any other cluster (its ports / data dirs are distinct).
#
#   cargo build --workspace          # build the debug binaries first
#   bash crates/autumn-memory/tests/run_e2e.sh
#
# Env overrides: AM_PORT_BASE (default 19000), AM_WORK (default /tmp/am-e2e).
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AM_WORK:-/tmp/am-e2e}"
PB="${AM_PORT_BASE:-19000}"
MGR="127.0.0.1:$((PB + 1))"        # 19001
EN_PORT=$((PB + 101))              # 19101
PS_PORT=$((PB + 201))              # 19201
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "[e2e] FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT

wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[e2e] manager (memory-only) on $MGR"
"$BIN/autumn-manager-server" --port "$((PB + 1))" --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 &
PIDS+=($!); wait_port "$((PB + 1))" 20 || { echo "[e2e] FAIL manager"; tail -8 "$WORK/mgr.log"; exit 1; }

echo "[e2e] format + launch EN0 on $EN_PORT (cpuset 0 = single shard)"
"$BIN/autumn-op" --manager "$MGR" format "$WORK/en0" \
  >"$WORK/format.log" 2>&1 || { echo "[e2e] FAIL format"; cat "$WORK/format.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port "$EN_PORT" --manager "$MGR" --cpuset 0 --advertise "127.0.0.1:$EN_PORT" --listen 127.0.0.1 \
  >"$WORK/en0.log" 2>&1 &
PIDS+=($!); wait_port "$EN_PORT" 20 || { echo "[e2e] FAIL EN"; tail -8 "$WORK/en0.log"; exit 1; }
sleep 3  # register + first df -> Online

echo "[e2e] bootstrap (replication 1+0)"
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 \
  >"$WORK/bootstrap.log" 2>&1 || { echo "[e2e] FAIL bootstrap"; cat "$WORK/bootstrap.log"; exit 1; }

echo "[e2e] PS psid 1 on $PS_PORT"
"$BIN/autumn-ps" --psid 1 --port "$PS_PORT" --manager "$MGR" --data "$WORK/ps1" \
  --listen 127.0.0.1 --advertise "127.0.0.1:$PS_PORT" >"$WORK/ps1.log" 2>&1 &
PIDS+=($!); wait_port "$PS_PORT" 20 || { echo "[e2e] FAIL PS"; tail -8 "$WORK/ps1.log"; exit 1; }
sleep 4  # register + open partition

echo "[e2e] running tests/e2e.rs"
AUTUMN_MEMORY_E2E_MANAGER="$MGR" cargo test -p autumn-memory --test e2e --test scan_boundary -- --ignored --nocapture \
  >"$WORK/e2e.log" 2>&1
RC=$?
echo "===== e2e exit: $RC ====="; tail -8 "$WORK/e2e.log"
exit $RC
