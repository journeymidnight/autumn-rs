#!/usr/bin/env bash
# Self-contained test for the LangGraph AutumnStore: build+install the autumn
# wheel + autumn_memory + autumn_memory_langgraph (and langgraph) into a
# throwaway venv, bring up an ISOLATED minimal cluster, drive the BaseStore
# surface (tests/test_store_inproc.py), tear down.
#
#   cargo build --workspace
#   bash python/autumn_memory_langgraph/tests/run_store_test.sh
# pipefail so a failing maturin/pip in a `… | tail` pipeline is not masked.
set -uo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AM_WORK:-/tmp/am-lg}"
VENV="${AM_VENV:-/tmp/am-lg-venv}"
MGR="127.0.0.1:19001"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[lg] venv + autumn wheel + autumn_memory + autumn_memory_langgraph + langgraph"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin 2>&1 | tail -1
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo FAIL maturin; exit 1; }
pip install -q -e python/autumn_memory 2>&1 | tail -1 || { echo FAIL pip autumn_memory; exit 1; }
pip install -q -e python/autumn_memory_langgraph 2>&1 | tail -1 || { echo FAIL pip langgraph; exit 1; }  # pulls in langgraph + anyio

echo "[lg] cluster bring-up"
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

echo "[lg] BaseStore functional test"
AUTUMN_MEMORY_MANAGER="$MGR" python python/autumn_memory_langgraph/tests/test_store_inproc.py
RC=$?; echo "===== lg-store-test exit: $RC ====="; exit $RC
