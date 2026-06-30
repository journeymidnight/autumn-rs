#!/usr/bin/env bash
# Recall-latency bench (plan §13): bring up an isolated cluster and measure the
# lexical recall (prefetch) P50/P99 at single-agent scale. Reuses an existing
# venv with autumn + autumn_memory (build one via run_hermes_test.sh) or builds.
#
#   BENCH_N=2000 BENCH_Q=300 bash python/autumn_memory/tests/run_recall_bench.sh
set -uo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN="${AUTUMN_BIN:-target/debug}"  # AUTUMN_BIN=target/release for a fair perf number
WORK="${AM_WORK:-/tmp/am-bench}"
VENV="${AM_VENV:-/tmp/am-hermes-venv}"
MGR="127.0.0.1:19001"
# robust rm-rf guard: must be /tmp/<non-empty> and contain no `..` (so `/tmp`,
# `/tmp/`, `/tmp/../x` are all rejected, not just string-prefix-matched).
safe_tmp() { case "$1" in /tmp/?*) case "$1" in *..*) return 1;; *) return 0;; esac;; *) return 1;; esac; }
safe_tmp "$WORK" || { echo "refusing rm -rf AM_WORK=$WORK (must be /tmp/<name>, no ..)"; exit 2; }
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — cargo build --workspace"; exit 2; }
done
if [ ! -x "$VENV/bin/python" ] || ! "$VENV/bin/python" -c "import autumn_memory" 2>/dev/null; then
  safe_tmp "$VENV" || { echo "refusing to rm/build at AM_VENV=$VENV (must be /tmp/<name>, no ..)"; exit 2; }
  echo "[bench] building venv at $VENV"
  rm -rf "$VENV"; python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q maturin 2>&1 | tail -1 || { echo FAIL maturin-install; exit 1; }
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
  ( cd python && maturin develop 2>&1 | tail -2 ) || { echo FAIL maturin; exit 1; }
  pip install -q -e python/autumn_memory 2>&1 | tail -1 || { echo FAIL pip; exit 1; }
fi
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[bench] cluster bring-up"
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

echo "[bench] recall bench (N=${BENCH_N:-2000} Q=${BENCH_Q:-300})"
AUTUMN_MEMORY_MANAGER="$MGR" "$VENV/bin/python" python/autumn_memory/tests/bench_recall.py
RC=$?; echo "===== recall-bench exit: $RC ====="; exit $RC
