#!/usr/bin/env bash
# Self-contained test for the autumn Hermes MemoryProvider: build+install the
# autumn wheel + autumn_memory into a throwaway venv, bring up an ISOLATED
# minimal cluster, and drive the provider against the REAL Hermes ABC (cloned at
# HERMES_AGENT_PATH), then tear down.
#
#   git clone https://github.com/NousResearch/hermes-agent /data/dongmao_dev/hermes-agent
#   cargo build --workspace
#   bash python/hermes_memory_autumn/tests/run_hermes_test.sh
set -uo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AM_WORK:-/tmp/am-hermes}"
VENV="${AM_VENV:-/tmp/am-hermes-venv}"
MGR="127.0.0.1:19001"
export HERMES_AGENT_PATH="${HERMES_AGENT_PATH:-/data/dongmao_dev/hermes-agent}"
case "$VENV" in /tmp/*) ;; *) echo "refusing to rm -rf AM_VENV=$VENV (must be under /tmp/)"; exit 2;; esac
case "$WORK" in /tmp/*) ;; *) echo "refusing to rm -rf AM_WORK=$WORK (must be under /tmp/)"; exit 2;; esac
[ -f "$HERMES_AGENT_PATH/agent/memory_provider.py" ] || { echo "FAIL: Hermes not at $HERMES_AGENT_PATH (git clone NousResearch/hermes-agent there)"; exit 2; }
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[hermes] venv + autumn wheel + autumn_memory (Hermes ABC from $HERMES_AGENT_PATH)"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin 2>&1 | tail -1 || { echo FAIL maturin-install; exit 1; }
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo FAIL maturin; exit 1; }
pip install -q -e python/autumn_memory 2>&1 | tail -1 || { echo FAIL pip autumn_memory; exit 1; }

echo "[hermes] cluster bring-up"
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

echo "[hermes] provider test (real MemoryProvider ABC)"
AUTUMN_MEMORY_MANAGER="$MGR" python python/hermes_memory_autumn/tests/test_provider_inproc.py
RC=$?; echo "===== hermes-test exit: $RC ====="; exit $RC
