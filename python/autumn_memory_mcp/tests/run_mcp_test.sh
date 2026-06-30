#!/usr/bin/env bash
# Self-contained test for the autumn-memory MCP server: build+install the autumn
# wheel + autumn_memory + autumn_memory_mcp (and the `mcp` SDK) into a throwaway
# venv, bring up an ISOLATED minimal cluster, drive the MCP server through a real
# MCP client over the in-memory transport (tests/test_mcp_inproc.py), tear down.
#
#   cargo build --workspace
#   bash python/autumn_memory_mcp/tests/run_mcp_test.sh
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AM_WORK:-/tmp/am-mcp}"
VENV="${AM_VENV:-/tmp/am-mcp-venv}"
MGR="127.0.0.1:19001"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[mcp] venv + autumn wheel + autumn_memory + autumn_memory_mcp + mcp SDK"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin 2>&1 | tail -1
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo FAIL maturin; exit 1; }
pip install -q -e python/autumn_memory 2>&1 | tail -1
pip install -q -e python/autumn_memory_mcp 2>&1 | tail -1   # pulls in mcp + anyio

echo "[mcp] cluster bring-up"
"$BIN/autumn-manager-server" --port 19001 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19001 20 || { echo FAIL mgr; tail -6 "$WORK/mgr.log"; exit 1; }
"$BIN/autumn-op" --manager "$MGR" format --listen :19101 --advertise 127.0.0.1:19101 "$WORK/en0" >"$WORK/fmt.log" 2>&1 || { echo FAIL fmt; cat "$WORK/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port 19101 --manager "$MGR" --cpuset 0 --listen 127.0.0.1 >"$WORK/en0.log" 2>&1 & PIDS+=($!)
wait_port 19101 20 || { echo FAIL en; tail -6 "$WORK/en0.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19201 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19201 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19201 20 || { echo FAIL ps; tail -6 "$WORK/ps1.log"; exit 1; }
sleep 4

echo "[mcp] in-process MCP client test"
AUTUMN_MEMORY_MANAGER="$MGR" python python/autumn_memory_mcp/tests/test_mcp_inproc.py
RC=$?; echo "===== mcp-test exit: $RC ====="; exit $RC
